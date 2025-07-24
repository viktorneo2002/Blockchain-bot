use anchor_client::{Client, Cluster};
use anchor_lang::prelude::*;
use anyhow::{anyhow, Result};
use dashmap::DashMap;
use phoenix::program::{
    MarketHeader, MarketSizeParams, OrderPacket, 
    CancelMultipleOrdersByIdParams, CancelOrderParams,
    DepositParams, WithdrawParams, PlaceLimitOrderParams,
};
use phoenix::state::{
    Market, OrderId, Side, TraderState as PhoenixTraderState,
    markets::{FIFOOrderId, FIFORestingOrder, RestingOrder},
};
use phoenix::quantities::{BaseAtomsPerBaseUnit, QuoteAtomsPerQuoteUnit, WrapperU64};
use rand::Rng;
use serde::{Deserialize, Serialize};
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig, RpcSendTransactionConfig},
    rpc_response::{Response, RpcKeyedAccount},
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use solana_transaction_status::UiTransactionEncoding;
use spl_associated_token_account::get_associated_token_address;
use std::{
    collections::{HashMap, VecDeque},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{mpsc, oneshot, RwLock, Semaphore},
    time::{interval, sleep, timeout},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use futures_util::{SinkExt, StreamExt};

const PHOENIX_PROGRAM_ID: &str = "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketMakerConfig {
    pub market: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub trader: Keypair,
    pub rpc_url: String,
    pub ws_url: String,
    pub backup_rpc_urls: Vec<String>,
    pub target_inventory_base: u64,
    pub max_inventory_base: u64,
    pub min_order_size_base: u64,
    pub max_order_size_base: u64,
    pub max_orders_per_side: usize,
    pub min_spread_bps: u64,
    pub max_spread_bps: u64,
    pub order_refresh_ms: u64,
    pub inventory_risk_ratio: f64,
    pub quote_size_ratio: f64,
    pub compute_units: u32,
    pub priority_fee_lamports: u64,
    pub max_retry_attempts: u32,
    pub volatility_window: usize,
    pub order_lifetime_ms: u64,
    pub rate_limit_per_second: u32,
    pub circuit_breaker_threshold: u32,
}

impl Default for MarketMakerConfig {
    fn default() -> Self {
        Self {
            market: Pubkey::default(),
            base_mint: Pubkey::default(),
            quote_mint: Pubkey::default(),
            trader: Keypair::new(),
            rpc_url: String::from("https://api.mainnet-beta.solana.com"),
            ws_url: String::from("wss://api.mainnet-beta.solana.com"),
            backup_rpc_urls: vec![],
            target_inventory_base: 1000,
            max_inventory_base: 2000,
            min_order_size_base: 10,
            max_order_size_base: 100,
            max_orders_per_side: 8,
            min_spread_bps: 10,
            max_spread_bps: 100,
            order_refresh_ms: 500,
            inventory_risk_ratio: 0.3,
            quote_size_ratio: 0.02,
            compute_units: 400_000,
            priority_fee_lamports: 10_000,
            max_retry_attempts: 3,
            volatility_window: 100,
            order_lifetime_ms: 30_000,
            rate_limit_per_second: 10,
            circuit_breaker_threshold: 10,
        }
    }
}

#[derive(Clone, Debug)]
struct MarketData {
    bid: Option<f64>,
    ask: Option<f64>,
    mid: Option<f64>,
    spread: f64,
    last_update: Instant,
    base_atoms_per_unit: u64,
    quote_atoms_per_unit: u64,
    tick_size: u64,
    raw_base_units_per_base_unit: u32,
}

#[derive(Clone, Debug)]
struct OrderInfo {
    order_id: OrderId,
    sequence_number: u64,
    side: Side,
    price_in_ticks: u64,
    size_in_base_lots: u64,
    placed_at: Instant,
    client_order_id: u128,
}

#[derive(Clone, Debug)]
struct Fill {
    order_id: OrderId,
    side: Side,
    price: f64,
    size: f64,
    timestamp: u64,
    fee: f64,
}

#[derive(Debug)]
pub struct PhoenixMarketMaker {
    config: Arc<MarketMakerConfig>,
    rpc_pool: Arc<RpcPool>,
    market_data: Arc<RwLock<MarketData>>,
    trader_state: Arc<RwLock<PhoenixTraderState>>,
    active_orders: Arc<DashMap<OrderId, OrderInfo>>,
    fills: Arc<RwLock<VecDeque<Fill>>>,
    price_history: Arc<RwLock<VecDeque<f64>>>,
    metrics: Arc<Metrics>,
    rate_limiter: Arc<Semaphore>,
    circuit_breaker: Arc<CircuitBreaker>,
    shutdown_signal: Arc<AtomicBool>,
    ws_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

struct RpcPool {
    clients: Vec<Arc<RpcClient>>,
    current_index: AtomicU64,
}

impl RpcPool {
    fn new(urls: Vec<String>) -> Self {
        let clients = urls
            .into_iter()
            .map(|url| {
                Arc::new(RpcClient::new_with_commitment(
                    url,
                    CommitmentConfig::confirmed(),
                ))
            })
            .collect();
        
        Self {
            clients,
            current_index: AtomicU64::new(0),
        }
    }

    fn get(&self) -> Arc<RpcClient> {
        let index = self.current_index.fetch_add(1, Ordering::Relaxed) as usize % self.clients.len();
        self.clients[index].clone()
    }
}

#[derive(Debug)]
struct Metrics {
    orders_placed: AtomicU64,
    orders_cancelled: AtomicU64,
    orders_filled: AtomicU64,
    total_volume: AtomicU64,
    errors: AtomicU64,
    last_update: RwLock<Instant>,
}

impl Metrics {
    fn new() -> Self {
        Self {
            orders_placed: AtomicU64::new(0),
            orders_cancelled: AtomicU64::new(0),
            orders_filled: AtomicU64::new(0),
            total_volume: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            last_update: RwLock::new(Instant::now()),
        }
    }
}

struct CircuitBreaker {
    failure_count: AtomicU64,
    threshold: u64,
    last_reset: RwLock<Instant>,
    is_open: AtomicBool,
}

impl CircuitBreaker {
    fn new(threshold: u64) -> Self {
        Self {
            failure_count: AtomicU64::new(0),
            threshold,
            last_reset: RwLock::new(Instant::now()),
            is_open: AtomicBool::new(false),
        }
    }

    async fn record_success(&self) {
        self.failure_count.store(0, Ordering::Relaxed);
        self.is_open.store(false, Ordering::Relaxed);
        *self.last_reset.write().await = Instant::now();
    }

    async fn record_failure(&self) {
        let count = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
        if count >= self.threshold {
            self.is_open.store(true, Ordering::Relaxed);
        }
    }

    fn is_open(&self) -> bool {
        self.is_open.load(Ordering::Relaxed)
    }
}

impl PhoenixMarketMaker {
    pub async fn new(config: MarketMakerConfig) -> Result<Self> {
        let mut all_urls = vec![config.rpc_url.clone()];
        all_urls.extend(config.backup_rpc_urls.clone());
        
        let rpc_pool = Arc::new(RpcPool::new(all_urls));
        
        let market_data = Arc::new(RwLock::new(MarketData {
            bid: None,
            ask: None,
            mid: None,
            spread: 0.0,
            last_update: Instant::now(),
            base_atoms_per_unit: 1_000_000,
            quote_atoms_per_unit: 1_000_000,
            tick_size: 1,
            raw_base_units_per_base_unit: 1,
        }));

        let rate_limiter = Arc::new(Semaphore::new(config.rate_limit_per_second as usize));
        let circuit_breaker = Arc::new(CircuitBreaker::new(config.circuit_breaker_threshold as u64));

        Ok(Self {
            config: Arc::new(config),
            rpc_pool,
            market_data,
            trader_state: Arc::new(RwLock::new(PhoenixTraderState::new())),
            active_orders: Arc::new(DashMap::new()),
            fills: Arc::new(RwLock::new(VecDeque::with_capacity(1000))),
            price_history: Arc::new(RwLock::new(VecDeque::with_capacity(config.volatility_window))),
            metrics: Arc::new(Metrics::new()),
            rate_limiter,
            circuit_breaker,
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            ws_handle: Arc::new(RwLock::new(None)),
        })
    }

    pub async fn run(&self) -> Result<()> {
        self.initialize().await?;
        
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        
        let ws_handle = self.start_websocket_listener().await?;
        *self.ws_handle.write().await = Some(ws_handle);
        
        let mut interval = interval(Duration::from_millis(self.config.order_refresh_ms));
        
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if self.shutdown_signal.load(Ordering::Relaxed) {
                        break;
                    }
                    
                    if self.circuit_breaker.is_open() {
                        eprintln!("Circuit breaker is open, skipping cycle");
                        sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                    
                    let _permit = match self.rate_limiter.try_acquire() {
                        Ok(permit) => permit,
                        Err(_) => continue,
                    };
                    
                    if let Err(e) = self.market_making_cycle().await {
                        eprintln!("Market making cycle error: {:?}", e);
                        self.metrics.errors.fetch_add(1, Ordering::Relaxed);
                        self.circuit_breaker.record_failure().await;
                    } else {
                        self.circuit_breaker.record_success().await;
                    }
                }
                _ = &mut shutdown_rx => {
                    println!("Received shutdown signal");
                    break;
                }
            }
        }
        
        self.shutdown().await
    }

    async fn initialize(&self) -> Result<()> {
        println!("Initializing Phoenix Market Maker...");
        
        self.setup_token_accounts().await?;
        self.initialize_seat().await?;
        
        let market_account = self.get_market_account().await?;
        let market_header = MarketHeader::try_deserialize(&mut &market_account.data[..])?;
        
        let mut market_data = self.market_data.write().await;
        market_data.base_atoms_per_unit = market_header.base_atoms_per_base_unit.as_u64();
        market_data.quote_atoms_per_unit = market_header.quote_atoms_per_quote_unit.as_u64();
        market_data.tick_size = market_header.tick_size_in_quote_atoms_per_base_unit.as_u64();
        market_data.raw_base_units_per_base_unit = market_header.raw_base_units_per_base_unit;
        
        println!("Initialization complete");
        Ok(())
    }

    async fn market_making_cycle(&self) -> Result<()> {
        self.update_market_data().await?;
        self.update_trader_state().await?;
        self.process_fills().await?;
        
        let (cancel_ixs, place_ixs) = self.generate_orders().await?;
        
                if !cancel_ixs.is_empty() || !place_ixs.is_empty() {
            self.execute_instructions(cancel_ixs, place_ixs).await?;
        }
        
        Ok(())
    }

    async fn start_websocket_listener(&self) -> Result<tokio::task::JoinHandle<()>> {
        let ws_url = self.config.ws_url.clone();
        let market = self.config.market;
        let market_data = self.market_data.clone();
        let active_orders = self.active_orders.clone();
        let fills = self.fills.clone();
        let shutdown_signal = self.shutdown_signal.clone();
        
        let handle = tokio::spawn(async move {
            loop {
                if shutdown_signal.load(Ordering::Relaxed) {
                    break;
                }
                
                match Self::websocket_connect_and_listen(
                    ws_url.clone(),
                    market,
                    market_data.clone(),
                    active_orders.clone(),
                    fills.clone(),
                    shutdown_signal.clone(),
                ).await {
                    Ok(_) => {
                        println!("Websocket connection closed normally");
                    }
                    Err(e) => {
                        eprintln!("Websocket error: {:?}, reconnecting in 5s", e);
                        sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        });
        
        Ok(handle)
    }

    async fn websocket_connect_and_listen(
        ws_url: String,
        market: Pubkey,
        market_data: Arc<RwLock<MarketData>>,
        active_orders: Arc<DashMap<OrderId, OrderInfo>>,
        fills: Arc<RwLock<VecDeque<Fill>>>,
        shutdown_signal: Arc<AtomicBool>,
    ) -> Result<()> {
        let (ws_stream, _) = connect_async(&ws_url).await?;
        let (mut write, mut read) = ws_stream.split();
        
        let subscribe_msg = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "accountSubscribe",
            "params": [
                market.to_string(),
                {
                    "encoding": "base64",
                    "commitment": "confirmed"
                }
            ]
        });
        
        write.send(Message::Text(subscribe_msg.to_string())).await?;
        
        while let Some(msg) = read.next().await {
            if shutdown_signal.load(Ordering::Relaxed) {
                break;
            }
            
            match msg? {
                Message::Text(text) => {
                    if let Ok(response) = serde_json::from_str::<serde_json::Value>(&text) {
                        if let Some(params) = response.get("params") {
                            if let Some(result) = params.get("result") {
                                if let Some(value) = result.get("value") {
                                    if let Some(data) = value.get("data").and_then(|d| d.as_array()) {
                                        if let Some(data_str) = data.get(0).and_then(|s| s.as_str()) {
                                            if let Ok(account_data) = base64::decode(data_str) {
                                                let _ = Self::process_market_update(
                                                    account_data,
                                                    market_data.clone(),
                                                    active_orders.clone(),
                                                    fills.clone(),
                                                ).await;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
        
        Ok(())
    }

    async fn process_market_update(
        account_data: Vec<u8>,
        market_data: Arc<RwLock<MarketData>>,
        active_orders: Arc<DashMap<OrderId, OrderInfo>>,
        fills: Arc<RwLock<VecDeque<Fill>>>,
    ) -> Result<()> {
        let market = Market::load(&account_data)?;
        let ladder = market.get_book_levels(10);
        
        let mut data = market_data.write().await;
        
        if let Some(best_bid) = ladder.bids.first() {
            data.bid = Some(best_bid.price_in_ticks as f64 * data.tick_size as f64 / data.quote_atoms_per_unit as f64);
        }
        
        if let Some(best_ask) = ladder.asks.first() {
            data.ask = Some(best_ask.price_in_ticks as f64 * data.tick_size as f64 / data.quote_atoms_per_unit as f64);
        }
        
        if let (Some(bid), Some(ask)) = (data.bid, data.ask) {
            data.mid = Some((bid + ask) / 2.0);
            data.spread = ask - bid;
        }
        
        data.last_update = Instant::now();
        
        Ok(())
    }

    async fn update_market_data(&self) -> Result<()> {
        let market_account = self.get_market_account().await?;
        let market = Market::load(&market_account.data)?;
        let ladder = market.get_book_levels(10);
        
        let mut market_data = self.market_data.write().await;
        
        if let Some(best_bid) = ladder.bids.first() {
            market_data.bid = Some(
                best_bid.price_in_ticks as f64 * market_data.tick_size as f64 
                / market_data.quote_atoms_per_unit as f64
            );
        }
        
        if let Some(best_ask) = ladder.asks.first() {
            market_data.ask = Some(
                best_ask.price_in_ticks as f64 * market_data.tick_size as f64 
                / market_data.quote_atoms_per_unit as f64
            );
        }
        
        if let (Some(bid), Some(ask)) = (market_data.bid, market_data.ask) {
            market_data.mid = Some((bid + ask) / 2.0);
            market_data.spread = ask - bid;
            
            let mut price_history = self.price_history.write().await;
            price_history.push_back(market_data.mid.unwrap());
            if price_history.len() > self.config.volatility_window {
                price_history.pop_front();
            }
        }
        
        market_data.last_update = Instant::now();
        
        Ok(())
    }

    async fn update_trader_state(&self) -> Result<()> {
        let trader_pubkey = self.config.trader.pubkey();
        let (trader_state_pubkey, _) = Pubkey::find_program_address(
            &[b"trader", self.config.market.as_ref(), trader_pubkey.as_ref()],
            &Pubkey::from_str(PHOENIX_PROGRAM_ID)?,
        );
        
        let client = self.rpc_pool.get();
        if let Ok(account) = client.get_account(&trader_state_pubkey).await {
            let state = PhoenixTraderState::try_deserialize(&mut &account.data[..])?;
            *self.trader_state.write().await = state;
        }
        
        Ok(())
    }

    async fn process_fills(&self) -> Result<()> {
        let market_account = self.get_market_account().await?;
        let market = Market::load(&market_account.data)?;
        
        let trader_pubkey = self.config.trader.pubkey();
        let events = market.get_events_for_trader(&trader_pubkey);
        
        let mut fills = self.fills.write().await;
        let active_orders = self.active_orders.clone();
        
        for event in events {
            if let Some(order_info) = active_orders.get(&event.order_id) {
                let fill = Fill {
                    order_id: event.order_id,
                    side: order_info.side,
                    price: event.price_in_ticks as f64 * self.market_data.read().await.tick_size as f64 
                        / self.market_data.read().await.quote_atoms_per_unit as f64,
                    size: event.base_lots_filled as f64 / self.market_data.read().await.base_atoms_per_unit as f64,
                    timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
                    fee: event.fee_in_quote_atoms as f64 / self.market_data.read().await.quote_atoms_per_unit as f64,
                };
                
                fills.push_back(fill);
                if fills.len() > 1000 {
                    fills.pop_front();
                }
                
                self.metrics.orders_filled.fetch_add(1, Ordering::Relaxed);
                self.metrics.total_volume.fetch_add(
                    (event.base_lots_filled * event.price_in_ticks) as u64,
                    Ordering::Relaxed
                );
                
                if event.base_lots_filled >= order_info.size_in_base_lots {
                    active_orders.remove(&event.order_id);
                }
            }
        }
        
        Ok(())
    }

    async fn generate_orders(&self) -> Result<(Vec<Instruction>, Vec<Instruction>)> {
        let market_data = self.market_data.read().await.clone();
        let trader_state = self.trader_state.read().await.clone();
        
        if market_data.bid.is_none() || market_data.ask.is_none() {
            return Ok((vec![], vec![]));
        }
        
        if market_data.last_update.elapsed() > Duration::from_secs(5) {
            return Err(anyhow!("Market data is stale"));
        }
        
        let volatility = self.calculate_volatility().await;
        let inventory_skew = self.calculate_inventory_skew(&trader_state);
        let spread_adjustment = self.calculate_spread_adjustment(volatility, inventory_skew);
        
        let mut cancel_ixs = Vec::new();
        let active_orders = self.active_orders.clone();
        
        let mut orders_to_cancel = Vec::new();
        for entry in active_orders.iter() {
            if self.should_cancel_order(&entry.value(), &market_data, spread_adjustment) {
                orders_to_cancel.push(entry.key().clone());
            }
        }
        
        if !orders_to_cancel.is_empty() {
            for chunk in orders_to_cancel.chunks(8) {
                let params = CancelMultipleOrdersByIdParams {
                    orders: chunk.to_vec(),
                };
                
                let ix = phoenix::program::create_cancel_multiple_orders_by_id_instruction(
                    &self.config.market,
                    &self.config.trader.pubkey(),
                    &params,
                )?;
                cancel_ixs.push(ix);
            }
            
            for order_id in &orders_to_cancel {
                active_orders.remove(order_id);
            }
            
            self.metrics.orders_cancelled.fetch_add(orders_to_cancel.len() as u64, Ordering::Relaxed);
        }
        
        let bid_count = active_orders.iter().filter(|e| e.value().side == Side::Bid).count();
        let ask_count = active_orders.iter().filter(|e| e.value().side == Side::Ask).count();
        
        let mut place_ixs = Vec::new();
        
        if bid_count < self.config.max_orders_per_side {
            let bid_levels = self.calculate_bid_levels(&market_data, spread_adjustment, inventory_skew);
            for (price, size) in bid_levels.into_iter().take(self.config.max_orders_per_side - bid_count) {
                let client_order_id = rand::thread_rng().gen::<u128>();
                let params = PlaceLimitOrderParams {
                    side: Side::Bid,
                    price_in_ticks: price,
                    num_base_lots: size,
                    client_order_id,
                    reduce_only: false,
                    post_only: true,
                    immediate_or_cancel: false,
                    reject_post_only: false,
                    use_only_deposited_funds: true,
                    last_valid_slot: None,
                    last_valid_unix_timestamp_in_seconds: None,
                };
                
                let ix = self.create_place_order_instruction(&params)?;
                place_ixs.push(ix);
                
                let order_info = OrderInfo {
                    order_id: OrderId::new_from_untyped(client_order_id),
                    sequence_number: 0,
                    side: Side::Bid,
                    price_in_ticks: price,
                    size_in_base_lots: size,
                    placed_at: Instant::now(),
                    client_order_id,
                };
                
                active_orders.insert(order_info.order_id, order_info);
            }
        }
        
        if ask_count < self.config.max_orders_per_side {
            let ask_levels = self.calculate_ask_levels(&market_data, spread_adjustment, inventory_skew);
            for (price, size) in ask_levels.into_iter().take(self.config.max_orders_per_side - ask_count) {
                let client_order_id = rand::thread_rng().gen::<u128>();
                let params = PlaceLimitOrderParams {
                    side: Side::Ask,
                    price_in_ticks: price,
                    num_base_lots: size,
                    client_order_id,
                    reduce_only: false,
                    post_only: true,
                    immediate_or_cancel: false,
                    reject_post_only: false,
                    use_only_deposited_funds: true,
                    last_valid_slot: None,
                    last_valid_unix_timestamp_in_seconds: None,
                };
                
                                let ix = self.create_place_order_instruction(&params)?;
                place_ixs.push(ix);
                
                let order_info = OrderInfo {
                    order_id: OrderId::new_from_untyped(client_order_id),
                    sequence_number: 0,
                    side: Side::Ask,
                    price_in_ticks: price,
                    size_in_base_lots: size,
                    placed_at: Instant::now(),
                    client_order_id,
                };
                
                active_orders.insert(order_info.order_id, order_info);
            }
        }
        
        self.metrics.orders_placed.fetch_add(place_ixs.len() as u64, Ordering::Relaxed);
        
        Ok((cancel_ixs, place_ixs))
    }

    fn create_place_order_instruction(&self, params: &PlaceLimitOrderParams) -> Result<Instruction> {
        let base_account = get_associated_token_address(&self.config.trader.pubkey(), &self.config.base_mint);
        let quote_account = get_associated_token_address(&self.config.trader.pubkey(), &self.config.quote_mint);
        
        let (seat, _) = Pubkey::find_program_address(
            &[b"seat", self.config.market.as_ref(), self.config.trader.pubkey().as_ref()],
            &Pubkey::from_str(PHOENIX_PROGRAM_ID)?,
        );
        
        phoenix::program::create_new_order_instruction(
            &self.config.market,
            &self.config.trader.pubkey(),
            &base_account,
            &quote_account,
            &seat,
            params,
        )
    }

    async fn calculate_volatility(&self) -> f64 {
        let price_history = self.price_history.read().await;
        if price_history.len() < 2 {
            return 0.01;
        }
        
        let prices: Vec<f64> = price_history.iter().cloned().collect();
        let mean = prices.iter().sum::<f64>() / prices.len() as f64;
        let variance = prices.iter().map(|p| (p - mean).powi(2)).sum::<f64>() / prices.len() as f64;
        
        (variance.sqrt() / mean).min(0.1)
    }

    fn calculate_inventory_skew(&self, trader_state: &PhoenixTraderState) -> f64 {
        let base_free = trader_state.base_units_free.as_u64() as f64;
        let base_locked = trader_state.base_units_locked.as_u64() as f64;
        let total_base = base_free + base_locked;
        
        if self.config.target_inventory_base == 0 {
            return 0.0;
        }
        
        ((total_base - self.config.target_inventory_base as f64) / self.config.target_inventory_base as f64)
            .max(-1.0)
            .min(1.0)
    }

    fn calculate_spread_adjustment(&self, volatility: f64, inventory_skew: f64) -> f64 {
        let vol_adjustment = 1.0 + volatility * 10.0;
        let inventory_adjustment = 1.0 + inventory_skew.abs() * 0.5;
        
        (vol_adjustment * inventory_adjustment).min(3.0)
    }

    fn should_cancel_order(&self, order: &OrderInfo, market_data: &MarketData, spread_adjustment: f64) -> bool {
        if order.placed_at.elapsed() > Duration::from_millis(self.config.order_lifetime_ms) {
            return true;
        }
        
        let mid = match market_data.mid {
            Some(m) => m,
            None => return false,
        };
        
        let order_price = order.price_in_ticks as f64 * market_data.tick_size as f64 
            / market_data.quote_atoms_per_unit as f64;
        let distance_from_mid = ((order_price - mid) / mid).abs();
        
        let threshold = match order.side {
            Side::Bid => self.config.min_spread_bps as f64 / 10000.0 * spread_adjustment,
            Side::Ask => self.config.min_spread_bps as f64 / 10000.0 * spread_adjustment,
        };
        
        distance_from_mid > threshold * 2.0
    }

    fn calculate_bid_levels(&self, market_data: &MarketData, spread_adjustment: f64, inventory_skew: f64) -> Vec<(u64, u64)> {
        let mut levels = Vec::new();
        let mid = match market_data.mid {
            Some(m) => m,
            None => return levels,
        };
        
        let base_spread_bps = (self.config.min_spread_bps as f64 + 
            (self.config.max_spread_bps - self.config.min_spread_bps) as f64 * 
            (spread_adjustment - 1.0).min(1.0)) as u64;
        
        let bid_adjustment = if inventory_skew > 0.0 {
            1.0 + inventory_skew * 0.2
        } else {
            1.0 - inventory_skew * 0.1
        };
        
        for i in 0..3 {
            let spread_bps = base_spread_bps + (i as u64 * 5);
            let price = mid * (1.0 - spread_bps as f64 / 10000.0 * bid_adjustment);
            let price_atoms = (price * market_data.quote_atoms_per_unit as f64) as u64;
            let price_ticks = (price_atoms / market_data.tick_size) * market_data.tick_size;
            
            if price_ticks == 0 {
                continue;
            }
            
            let base_size = self.calculate_order_size(inventory_skew, i);
            let size_lots = base_size / market_data.raw_base_units_per_base_unit as u64;
            
            if size_lots > 0 && base_size >= self.config.min_order_size_base {
                levels.push((price_ticks / market_data.tick_size, size_lots));
            }
        }
        
        levels
    }

    fn calculate_ask_levels(&self, market_data: &MarketData, spread_adjustment: f64, inventory_skew: f64) -> Vec<(u64, u64)> {
        let mut levels = Vec::new();
        let mid = match market_data.mid {
            Some(m) => m,
            None => return levels,
        };
        
        let base_spread_bps = (self.config.min_spread_bps as f64 + 
            (self.config.max_spread_bps - self.config.min_spread_bps) as f64 * 
            (spread_adjustment - 1.0).min(1.0)) as u64;
        
        let ask_adjustment = if inventory_skew < 0.0 {
            1.0 - inventory_skew * 0.2
        } else {
            1.0 + inventory_skew * 0.1
        };
        
        for i in 0..3 {
            let spread_bps = base_spread_bps + (i as u64 * 5);
            let price = mid * (1.0 + spread_bps as f64 / 10000.0 * ask_adjustment);
            let price_atoms = (price * market_data.quote_atoms_per_unit as f64) as u64;
            let price_ticks = ((price_atoms + market_data.tick_size - 1) / market_data.tick_size) * market_data.tick_size;
            
            let base_size = self.calculate_order_size(-inventory_skew, i);
            let size_lots = base_size / market_data.raw_base_units_per_base_unit as u64;
            
            if size_lots > 0 && base_size >= self.config.min_order_size_base {
                levels.push((price_ticks / market_data.tick_size, size_lots));
            }
        }
        
        levels
    }

    fn calculate_order_size(&self, inventory_skew: f64, level: usize) -> u64 {
        let base_size = (self.config.max_inventory_base as f64 * self.config.quote_size_ratio) as u64;
        let level_decay = 0.7_f64.powi(level as i32);
        let inventory_adjustment = 1.0 - inventory_skew.abs() * self.config.inventory_risk_ratio;
        
        let size = (base_size as f64 * level_decay * inventory_adjustment.max(0.3)) as u64;
        size.clamp(self.config.min_order_size_base, self.config.max_order_size_base)
    }

    async fn execute_instructions(&self, cancel_ixs: Vec<Instruction>, place_ixs: Vec<Instruction>) -> Result<()> {
        let mut all_ixs = Vec::new();
        
        all_ixs.push(ComputeBudgetInstruction::set_compute_unit_limit(self.config.compute_units));
        all_ixs.push(ComputeBudgetInstruction::set_compute_unit_price(self.config.priority_fee_lamports));
        
        all_ixs.extend(cancel_ixs);
        all_ixs.extend(place_ixs);
        
        if all_ixs.len() <= 2 {
            return Ok(());
        }
        
        let mut retry_count = 0;
        let client = self.rpc_pool.get();
        
        while retry_count < self.config.max_retry_attempts {
            match self.send_transaction(&client, all_ixs.clone()).await {
                Ok(signature) => {
                    println!("Transaction sent: {}", signature);
                    return Ok(());
                }
                Err(e) => {
                    eprintln!("Transaction failed (attempt {}): {:?}", retry_count + 1, e);
                    retry_count += 1;
                    if retry_count < self.config.max_retry_attempts {
                        sleep(Duration::from_millis(100 * (1 << retry_count))).await;
                    }
                }
            }
        }
        
        Err(anyhow!("Failed to execute transaction after {} attempts", self.config.max_retry_attempts))
    }

    async fn send_transaction(&self, client: &RpcClient, instructions: Vec<Instruction>) -> Result<solana_sdk::signature::Signature> {
        let recent_blockhash = timeout(
            Duration::from_secs(5),
            client.get_latest_blockhash()
        ).await??;
        
        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.config.trader.pubkey()),
            &[&self.config.trader],
            recent_blockhash,
        );
        
        let config = RpcSendTransactionConfig {
            skip_preflight: false,
            preflight_commitment: Some(CommitmentLevel::Processed),
            encoding: Some(UiTransactionEncoding::Base64),
            max_retries: Some(0),
            min_context_slot: None,
        };
        
        let signature = timeout(
            Duration::from_secs(10),
            client.send_transaction_with_config(&transaction, config)
        ).await??;
        
        Ok(signature)
    }

    async fn get_market_account(&self) -> Result<solana_sdk::account::Account> {
        let client = self.rpc_pool.get();
        timeout(
            Duration::from_secs(5),
            client.get_account(&self.config.market)
        ).await?
        .map_err(|e| anyhow!("Failed to get market account: {}", e))
    }

    async fn initialize_seat(&self) -> Result<()> {
        let (seat_pubkey, _) = Pubkey::find_program_address(
            &[b"seat", self.config.market.as_ref(), self.config.trader.pubkey().as_ref()],
            &Pubkey::from_str(PHOENIX_PROGRAM_ID)?,
        );
        
        let client = self.rpc_pool.get();
        if client.get_account(&seat_pubkey).await.is_err() {
            let ix = phoenix::program::create_request_seat_instruction(
                &self.config.market,
                &self.config.trader.pubkey(),
            )?;
            
            self.execute_instructions(vec![], vec![ix]).await?;
            println!("Seat initialized for trader: {}", self.config.trader.pubkey());
        }
        
        Ok(())
    }

    async fn setup_token_accounts(&self) -> Result<()> {
        let base_ata = get_associated_token_address(&self.config.trader.pubkey(), &self.config.base_mint);
        let quote_ata = get_associated_token_address(&self.config.trader.pubkey(), &self.config.quote_mint);
        
        let client = self.rpc_pool.get();
        for (ata, mint) in [(base_ata, self.config.base_mint), (quote_ata, self.config.quote_mint)] {
            if client.get_account(&ata).await.is_err() {
                let ix = spl_associated_token_account::instruction::create_associated_token_account(
                    &self.config.trader.pubkey(),
                    &self.config.trader.pubkey(),
                    &mint,
                    &spl_token::id(),
                );
                
                self.execute_instructions(vec![], vec![ix]).await?;
                println!("Created ATA: {} for mint: {}", ata, mint);
            }
        }
        
        Ok(())
    }

    pub async fn emergency_cancel_all(&self) -> Result<()> {
        let active_orders: Vec<OrderId> = self.active_orders.iter().map(|e| e.key().clone()).collect();
        
        if active_orders.is_empty() {
            return Ok(());
        }
        
        let mut cancel_ixs = Vec::new();
        for chunk in active_orders.chunks(8) {
            let params = CancelMultipleOrdersByIdParams {
                orders: chunk.to_vec(),
            };
            
                        let ix = phoenix::program::create_cancel_multiple_orders_by_id_instruction(
                &self.config.market,
                &self.config.trader.pubkey(),
                &params,
            )?;
            cancel_ixs.push(ix);
        }
        
        self.execute_instructions(cancel_ixs, vec![]).await?;
        self.active_orders.clear();
        
        println!("Emergency cancelled {} orders", active_orders.len());
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        println!("Initiating graceful shutdown...");
        self.shutdown_signal.store(true, Ordering::Relaxed);
        
        // Cancel all active orders
        if let Err(e) = self.emergency_cancel_all().await {
            eprintln!("Error cancelling orders during shutdown: {:?}", e);
        }
        
        // Stop websocket listener
        if let Some(handle) = self.ws_handle.write().await.take() {
            handle.abort();
        }
        
        // Log final metrics
        self.log_metrics().await;
        
        println!("Shutdown complete");
        Ok(())
    }

    async fn log_metrics(&self) {
        let orders_placed = self.metrics.orders_placed.load(Ordering::Relaxed);
        let orders_cancelled = self.metrics.orders_cancelled.load(Ordering::Relaxed);
        let orders_filled = self.metrics.orders_filled.load(Ordering::Relaxed);
        let total_volume = self.metrics.total_volume.load(Ordering::Relaxed);
        let errors = self.metrics.errors.load(Ordering::Relaxed);
        
        println!("=== Market Maker Metrics ===");
        println!("Orders Placed: {}", orders_placed);
        println!("Orders Cancelled: {}", orders_cancelled);
        println!("Orders Filled: {}", orders_filled);
        println!("Total Volume: {}", total_volume);
        println!("Errors: {}", errors);
        println!("========================");
    }

    pub async fn deposit_to_market(&self, base_amount: u64, quote_amount: u64) -> Result<()> {
        let mut ixs = Vec::new();
        
        if base_amount > 0 {
            let base_account = get_associated_token_address(&self.config.trader.pubkey(), &self.config.base_mint);
            let params = DepositParams {
                quote_lots: 0,
                base_lots: base_amount / self.market_data.read().await.raw_base_units_per_base_unit as u64,
            };
            
            let ix = phoenix::program::create_deposit_funds_instruction(
                &self.config.market,
                &self.config.trader.pubkey(),
                &base_account,
                &params,
            )?;
            ixs.push(ix);
        }
        
        if quote_amount > 0 {
            let quote_account = get_associated_token_address(&self.config.trader.pubkey(), &self.config.quote_mint);
            let params = DepositParams {
                quote_lots: quote_amount / self.market_data.read().await.tick_size,
                base_lots: 0,
            };
            
            let ix = phoenix::program::create_deposit_funds_instruction(
                &self.config.market,
                &self.config.trader.pubkey(),
                &quote_account,
                &params,
            )?;
            ixs.push(ix);
        }
        
        if !ixs.is_empty() {
            self.execute_instructions(vec![], ixs).await?;
            println!("Deposited - Base: {}, Quote: {}", base_amount, quote_amount);
        }
        
        Ok(())
    }

    pub async fn withdraw_from_market(&self, base_amount: Option<u64>, quote_amount: Option<u64>) -> Result<()> {
        let mut ixs = Vec::new();
        
        if let Some(amount) = base_amount {
            let base_account = get_associated_token_address(&self.config.trader.pubkey(), &self.config.base_mint);
            let params = WithdrawParams {
                quote_lots_to_withdraw: None,
                base_lots_to_withdraw: Some(amount / self.market_data.read().await.raw_base_units_per_base_unit as u64),
            };
            
            let ix = phoenix::program::create_withdraw_funds_instruction(
                &self.config.market,
                &self.config.trader.pubkey(),
                &base_account,
                &params,
            )?;
            ixs.push(ix);
        }
        
        if let Some(amount) = quote_amount {
            let quote_account = get_associated_token_address(&self.config.trader.pubkey(), &self.config.quote_mint);
            let params = WithdrawParams {
                quote_lots_to_withdraw: Some(amount / self.market_data.read().await.tick_size),
                base_lots_to_withdraw: None,
            };
            
            let ix = phoenix::program::create_withdraw_funds_instruction(
                &self.config.market,
                &self.config.trader.pubkey(),
                &quote_account,
                &params,
            )?;
            ixs.push(ix);
        }
        
        if !ixs.is_empty() {
            self.execute_instructions(vec![], ixs).await?;
            println!("Withdrew - Base: {:?}, Quote: {:?}", base_amount, quote_amount);
        }
        
        Ok(())
    }

    pub async fn get_market_stats(&self) -> MarketStats {
        let market_data = self.market_data.read().await.clone();
        let trader_state = self.trader_state.read().await.clone();
        let active_orders = self.active_orders.len();
        let fills = self.fills.read().await.clone();
        
        let base_inventory = (trader_state.base_units_free.as_u64() + trader_state.base_units_locked.as_u64()) as f64 
            / market_data.base_atoms_per_unit as f64;
        let quote_inventory = (trader_state.quote_units_free.as_u64() + trader_state.quote_units_locked.as_u64()) as f64 
            / market_data.quote_atoms_per_unit as f64;
        
        let recent_pnl = fills.iter()
            .take(100)
            .map(|f| match f.side {
                Side::Bid => -f.price * f.size - f.fee,
                Side::Ask => f.price * f.size - f.fee,
            })
            .sum();
        
        MarketStats {
            bid: market_data.bid,
            ask: market_data.ask,
            spread: market_data.spread,
            mid: market_data.mid,
            base_inventory,
            quote_inventory,
            active_orders,
            volatility: self.calculate_volatility().await,
            recent_pnl,
            health_status: self.get_health_status(),
        }
    }

    pub fn get_health_status(&self) -> HealthStatus {
        let is_healthy = !self.circuit_breaker.is_open() && 
                        !self.shutdown_signal.load(Ordering::Relaxed);
        
        HealthStatus {
            is_healthy,
            circuit_breaker_open: self.circuit_breaker.is_open(),
            last_error_count: self.circuit_breaker.failure_count.load(Ordering::Relaxed),
            uptime_seconds: self.metrics.last_update.try_read()
                .map(|t| t.elapsed().as_secs())
                .unwrap_or(0),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketStats {
    pub bid: Option<f64>,
    pub ask: Option<f64>,
    pub spread: f64,
    pub mid: Option<f64>,
    pub base_inventory: f64,
    pub quote_inventory: f64,
    pub active_orders: usize,
    pub volatility: f64,
    pub recent_pnl: f64,
    pub health_status: HealthStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub is_healthy: bool,
    pub circuit_breaker_open: bool,
    pub last_error_count: u64,
    pub uptime_seconds: u64,
}

impl PhoenixTraderState {
    fn new() -> Self {
        Self {
            base_units_free: WrapperU64::default(),
            base_units_locked: WrapperU64::default(),
            quote_units_free: WrapperU64::default(),
            quote_units_locked: WrapperU64::default(),
            padding: [0; 256],
        }
    }
}

pub async fn run_market_maker(config: MarketMakerConfig) -> Result<()> {
    // Validate configuration
    if config.max_inventory_base <= config.target_inventory_base {
        return Err(anyhow!("max_inventory_base must be greater than target_inventory_base"));
    }
    
    if config.min_order_size_base >= config.max_order_size_base {
        return Err(anyhow!("min_order_size_base must be less than max_order_size_base"));
    }
    
    if config.min_spread_bps >= config.max_spread_bps {
        return Err(anyhow!("min_spread_bps must be less than max_spread_bps"));
    }
    
    println!("Starting Phoenix CLOB Market Maker");
    println!("Market: {}", config.market);
    println!("Trader: {}", config.trader.pubkey());
    println!("Target Inventory: {} base units", config.target_inventory_base);
    
    let market_maker = PhoenixMarketMaker::new(config).await?;
    
    // Set up signal handler for graceful shutdown
    let mm = market_maker.clone();
    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                println!("\nReceived interrupt signal");
                if let Err(e) = mm.shutdown().await {
                    eprintln!("Error during shutdown: {:?}", e);
                }
            }
            Err(e) => eprintln!("Error setting up signal handler: {:?}", e),
        }
    });
    
    market_maker.run().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;

    #[test]
    fn test_inventory_skew_calculation() {
        let config = MarketMakerConfig {
            target_inventory_base: 1000,
            ..Default::default()
        };
        
        let mm = PhoenixMarketMaker {
            config: Arc::new(config),
            rpc_pool: Arc::new(RpcPool::new(vec!["http://localhost:8899".to_string()])),
            market_data: Arc::new(RwLock::new(MarketData {
                bid: Some(100.0),
                ask: Some(101.0),
                mid: Some(100.5),
                spread: 1.0,
                last_update: Instant::now(),
                base_atoms_per_unit: 1_000_000,
                quote_atoms_per_unit: 1_000_000,
                tick_size: 1,
                raw_base_units_per_base_unit: 1,
            })),
            trader_state: Arc::new(RwLock::new(PhoenixTraderState::new())),
            active_orders: Arc::new(DashMap::new()),
            fills: Arc::new(RwLock::new(VecDeque::new())),
            price_history: Arc::new(RwLock::new(VecDeque::new())),
            metrics: Arc::new(Metrics::new()),
            rate_limiter: Arc::new(Semaphore::new(10)),
            circuit_breaker: Arc::new(CircuitBreaker::new(10)),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            ws_handle: Arc::new(RwLock::new(None)),
        };
        
        let mut trader_state = PhoenixTraderState::new();
        trader_state.base_units_free = WrapperU64::from(500_000_000); // 500 base units
        
        let skew = mm.calculate_inventory_skew(&trader_state);
        assert!((skew - (-0.5)).abs() < 0.01);
    }

    #[test]
    fn test_spread_adjustment() {
        let config = MarketMakerConfig::default();
        let mm = PhoenixMarketMaker {
            config: Arc::new(config),
            rpc_pool: Arc::new(RpcPool::new(vec!["http://localhost:8899".to_string()])),
            market_data: Arc::new(RwLock::new(MarketData {
                bid: Some(100.0),
                ask: Some(101.0),
                mid: Some(100.5),
                spread: 1.0,
                last_update: Instant::now(),
                base_atoms_per_unit: 1_000_000,
                quote_atoms_per_unit: 1_000_000,
                tick_size: 1,
                raw_base_units_per_base_unit: 1,
            })),
            trader_state: Arc::new(RwLock::new(PhoenixTraderState::new())),
            active_orders: Arc::new(DashMap::new()),
            fills: Arc::new(RwLock::new(VecDeque::new())),
            price_history: Arc::new(RwLock::new(VecDeque::new())),
            metrics: Arc::new(Metrics::new()),
            rate_limiter: Arc::new(Semaphore::new(10)),
            circuit_breaker: Arc::new(CircuitBreaker::new(10)),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            ws_handle: Arc::new(RwLock::new(None)),
        };
        
        let adjustment = mm.calculate_spread_adjustment(0.05, 0.5);
        assert!(adjustment > 1.0);
        assert!(adjustment <= 3.0);
    }
}

