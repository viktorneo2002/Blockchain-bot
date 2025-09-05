use anyhow::{anyhow, Result};
use borsh::{BorshDeserialize, BorshSerialize};
use dashmap::DashMap;
use mango::state::{MangoAccount, PerpMarket};
use phoenix::program::MarketHeader;
use serum_dex::state::{gen_vault_signer_key, Market, MarketState};
use solana_account_decoder::UiAccountEncoding;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig, RpcSendTransactionConfig},
    rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
};
use solana_tpu_client::tpu_client::TpuClient;
use solana_program::{
    clock::Slot,
    program_pack::Pack,
    pubkey::Pubkey,
};
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    instruction::Instruction,
    message::v0::Message as V0Message,
    signature::{Keypair, Signature},
    transaction::VersionedTransaction,
    address_lookup_table_account::AddressLookupTableAccount,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use std::sync::Arc as StdArc;
use tokio::{
    sync::{mpsc, RwLock},
    time::interval,
};
use futures::{stream, StreamExt, TryStreamExt};
use tracing::warn;

const SERUM_DEX_V3: &str = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin";
const PHOENIX_V1: &str = "PhoeNiXZ8ByJGLkxNfZRnkUfjvmuYqLR89jjFHGqdXY";
const MANGO_V4: &str = "4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg";
const MIN_LIQUIDITY_USD: f64 = 1000.0;
const MAX_SPREAD_BPS: u16 = 100;
const MIN_DEPTH_LEVELS: usize = 5;
const OPPORTUNITY_THRESHOLD_BPS: u16 = 10;
const MAX_SLIPPAGE_BPS: u16 = 50;
const ORDERBOOK_REFRESH_MS: u64 = 100;
const CACHE_TTL_MS: u64 = 500;
// Slot-based staleness thresholds (circuit breaker)
const MAX_SLOT_LAG_SOFT: u64 = 8;   // apply confidence penalty
const MAX_SLOT_LAG_HARD: u64 = 16;  // drop opportunities entirely

#[derive(Debug, Clone)]
pub struct OrderbookSnapshot {
    pub market: Pubkey,
    pub protocol: DexProtocol,
    pub bids: StdArc<[Order]>,
    pub asks: StdArc<[Order]>,
    pub timestamp: u64,
    pub slot: Slot,
    pub spread_bps: u16,
    pub mid_price: f64,
    pub bid_liquidity_usd: f64,
    pub ask_liquidity_usd: f64,
    pub imbalance_ratio: f64,
}

impl OrderbookSnapshot {
    fn new(
        market: Pubkey,
        protocol: DexProtocol,
        mut bids: Vec<Order>,
        mut asks: Vec<Order>,
        slot: Slot,
    ) -> Self {
        // Filter invalid orders
        bids.retain(|o| o.price.is_finite() && o.size.is_finite() && o.price > 0.0 && o.size > 0.0);
        asks.retain(|o| o.price.is_finite() && o.size.is_finite() && o.price > 0.0 && o.size > 0.0);

        // Sort by price levels
        bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());
        asks.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());

        // Compute spread and mid
        let (spread_bps, mid_price) = if bids.is_empty() || asks.is_empty() {
            (u16::MAX, 0.0)
        } else {
            let mid = 0.5 * (bids[0].price + asks[0].price);
            let spread_bps = (((asks[0].price - bids[0].price) / mid) * 10000.0)
                .max(0.0)
                .round() as u16;
            (spread_bps, mid)
        };

        // Liquidity and imbalance
        let bid_liquidity_usd: f64 = bids
            .iter()
            .take(MIN_DEPTH_LEVELS)
            .map(|o| o.price * o.size)
            .sum();
        let ask_liquidity_usd: f64 = asks
            .iter()
            .take(MIN_DEPTH_LEVELS)
            .map(|o| o.price * o.size)
            .sum();
        let imbalance_ratio = if bid_liquidity_usd > 0.0 {
            ask_liquidity_usd / bid_liquidity_usd
        } else {
            0.0
        };

        Self {
            market,
            protocol,
            bids: bids.into(),
            asks: asks.into(),
            timestamp: chrono::Utc::now().timestamp_millis() as u64,
            slot,
            spread_bps,
            mid_price,
            bid_liquidity_usd,
            ask_liquidity_usd,
            imbalance_ratio,
        }

        fn calculate_confidence_simple(&self, snap: &OrderbookSnapshot, current_slot: Slot) -> f64 {
            // --- Slot freshness penalty ---
            let lag = current_slot.saturating_sub(snap.slot);
            let slot_penalty = if lag >= MAX_SLOT_LAG_HARD {
                0.0
            } else if lag >= MAX_SLOT_LAG_SOFT {
                // Smooth decay between soft and hard
                let range = (MAX_SLOT_LAG_HARD - MAX_SLOT_LAG_SOFT) as f64;
                let decay = 0.5 * (1.0 - ((lag - MAX_SLOT_LAG_SOFT) as f64 / range).clamp(0.0, 1.0));
                0.5 + decay
            } else {
                1.0
            };
        
            // --- Liquidity score ---
            let total_liq = snap.bid_liquidity_usd + snap.ask_liquidity_usd;
            let liq_score = (total_liq / MIN_LIQUIDITY_USD).clamp(0.0, 2.0); // cap at 2×
        
            // --- Spread quality ---
            let spread_score = if snap.spread_bps == u16::MAX {
                0.0
            } else {
                let raw = (MAX_SPREAD_BPS.saturating_sub(snap.spread_bps) as f64) / (MAX_SPREAD_BPS as f64);
                raw.clamp(0.0, 1.0)
            };
        
            // --- Depth quality ---
            let depth_levels = (snap.bids.len() + snap.asks.len()) as f64;
            let depth_score = (depth_levels / (2.0 * MIN_DEPTH_LEVELS as f64)).clamp(0.0, 1.0);
        
            // --- Imbalance moderation ---
            let imb = snap.imbalance_ratio;
            let imb_penalty = if imb < 0.2 || imb > 5.0 {
                0.6
            } else if imb < 0.5 || imb > 2.0 {
                0.85
            } else {
                1.0
            };
        
            // --- Top-of-book curvature ---
            let curvature_score = {
                let bid_slope = if snap.bids.len() >= 2 {
                    let p0 = snap.bids[0].price.max(1e-12);
                    let p1 = snap.bids[1].price.max(1e-12);
                    ((p0 - p1) / p0).abs()
                } else { 0.0 };
                let ask_slope = if snap.asks.len() >= 2 {
                    let p0 = snap.asks[0].price.max(1e-12);
                    let p1 = snap.asks[1].price.max(1e-12);
                    ((p1 - p0) / p0).abs()
                } else { 0.0 };
                // Lower slope = better (thicker book near top)
                (1.0 - ((bid_slope + ask_slope) / 0.05).clamp(0.0, 1.0)).clamp(0.0, 1.0)
            };
        
            // --- Weighted blend ---
            let base_score =
                0.35 * liq_score.clamp(0.0, 1.0) +
                0.25 * spread_score +
                0.20 * depth_score +
                0.20 * curvature_score;
        
            let confidence = slot_penalty * imb_penalty * base_score;
            confidence.clamp(0.0, 1.0)
        }

    // ---------------- Leader schedule awareness -----------------
    pub async fn set_ghosted_validators(&self, validators: &[Pubkey]) {
        let mut set = self.ghosted_validators.write().await;
        set.clear();
        for v in validators { set.insert(*v); }
    }

    async fn leader_schedule_updater(self: Arc<Self>) {
        let mut tick = interval(Duration::from_secs(10));
        while self.running.load(Ordering::Acquire) {
            tick.tick().await;
            // Fetch epoch info and leader schedule (current epoch)
            let epoch_info = match self.rpc.get_epoch_info().await { Ok(e) => e, Err(_) => continue };
            let slots_in_epoch = epoch_info.slots_in_epoch;
            let epoch = epoch_info.epoch;
            match self.rpc.get_leader_schedule(None).await {
                Ok(Some(map)) => {
                    // map: validator identity -> list of slot indices in epoch
                    let mut index_to_leader: HashMap<u64, Pubkey> = HashMap::new();
                    for (validator, slots) in map {
                        if let Ok(pk) = Pubkey::from_str(&validator) {
                            for idx in slots { index_to_leader.insert(idx as u64, pk); }
                        }
                    }
                    let mut guard = self.leader_schedule.write().await;
                    *guard = Some(LeaderSchedule { epoch, slots_in_epoch, index_to_leader });
                }
                _ => { /* skip on RPC error */ }
            }
        }
    }

    fn leader_for_slot_sync(&self, absolute_slot: u64) -> Option<Pubkey> {
        // Try to compute leader from cached schedule
        if let Ok(guard) = self.leader_schedule.try_read() {
            if let Some(sched) = guard.as_ref() {
                let idx = absolute_slot % sched.slots_in_epoch;
                return sched.index_to_leader.get(&idx).cloned();
            }
        }
        None
    }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DexProtocol {
    SerumV3,
    Phoenix,
    MangoV4,
}

#[derive(Debug, Clone)]
pub struct Order {
    pub price: f64,
    pub size: f64,
    pub owner: Option<Pubkey>,
}

#[derive(Debug, Clone)]
pub struct LiquidityOpportunity {
    pub market: Pubkey,
    pub protocol: DexProtocol,
    pub opportunity_type: OpportunityType,
    pub entry_price: f64,
    pub exit_price: f64,
    pub size: f64,
    pub profit_bps: u16,
    pub confidence: f64,
    pub expires_at: u64,
}

#[derive(Debug, Clone)]
pub enum OpportunityType {
    SpreadCapture,
    LiquidityImbalance,
    LargePriceMovement,
    CrossDexArbitrage,
}

#[derive(Debug, Clone)]
struct LeaderSchedule {
    epoch: u64,
    slots_in_epoch: u64,
    index_to_leader: std::collections::HashMap<u64, Pubkey>,
}

pub struct OrderbookLiquidityDetector {
    rpc: Arc<RpcClient>,
    tpu: Arc<RwLock<Option<Arc<TpuClient>>>>,
    markets: Arc<DashMap<Pubkey, MarketInfo>>,
    orderbooks: Arc<DashMap<Pubkey, OrderbookSnapshot>>,
    opportunities: Arc<RwLock<VecDeque<LiquidityOpportunity>>>,
    price_feeds: Arc<DashMap<Pubkey, f64>>,
    running: Arc<AtomicBool>,
    last_update: Arc<AtomicU64>,
    leader_schedule: Arc<RwLock<Option<LeaderSchedule>>>,
    ghosted_validators: Arc<RwLock<HashSet<Pubkey>>>,
    // LSTM-like predictor hooks
    execution_confidence: Arc<DashMap<Pubkey, f64>>,     // execution success probability per market
    vol_history: Arc<DashMap<Pubkey, VecDeque<f64>>>,    // rolling mid-price history per market
    // Frequency sketch for hot markets
    hit_frequency: Arc<DashMap<Pubkey, u64>>,
}

#[derive(Clone)]
struct MarketInfo {
    pubkey: Pubkey,
    protocol: DexProtocol,
    base_mint: Pubkey,
    quote_mint: Pubkey,
    base_decimals: u8,
    quote_decimals: u8,
    tick_size: f64,
    lot_size: f64,
}

impl OrderbookLiquidityDetector {
    pub async fn new(rpc_url: &str) -> Result<Self> {
        let rpc = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::processed(),
        ));

        Ok(Self {
            rpc,
            tpu: Arc::new(RwLock::new(None)),
            markets: Arc::new(DashMap::new()),
            orderbooks: Arc::new(DashMap::new()),
            opportunities: Arc::new(RwLock::new(VecDeque::with_capacity(1000))),
            price_feeds: Arc::new(DashMap::new()),
            running: Arc::new(AtomicBool::new(false)),
            last_update: Arc::new(AtomicU64::new(0)),
            leader_schedule: Arc::new(RwLock::new(None)),
            ghosted_validators: Arc::new(RwLock::new(HashSet::new())),
            execution_confidence: Arc::new(DashMap::new()),
            vol_history: Arc::new(DashMap::new()),
            hit_frequency: Arc::new(DashMap::new()),
        })
    }

    // Batched helper for getMultipleAccounts to reduce RPC round-trips and avoid URL limits
    async fn get_multiple_accounts(
        &self,
        keys: &[Pubkey],
    ) -> Result<Vec<Option<Account>>> {
        const BATCH: usize = 100; // conservative to avoid RPC caps
        let mut out: Vec<Option<Account>> = Vec::with_capacity(keys.len());
        for chunk in keys.chunks(BATCH) {
            let res = self.rpc.get_multiple_accounts(chunk).await?;
            out.extend(res);
        }
        Ok(out)
    }

    // ---------------- Transaction Building Integration (v0 + ALTs) -----------------
    async fn build_versioned_tx_with_alt(
        &self,
        payer: &Pubkey,
        instructions: Vec<Instruction>,
        alt_addresses: Vec<Pubkey>,
    ) -> Result<VersionedTransaction> {
        let recent_blockhash = self.rpc.get_latest_blockhash().await?;

        // Resolve ALTs (non-fatal on failure)
        let mut alts: Vec<AddressLookupTableAccount> = Vec::new();
        for alt in alt_addresses {
            match self.rpc.get_address_lookup_table(&alt).await {
                Ok(resp) => {
                    if let Some(data) = resp.value { alts.push(data); }
                }
                Err(e) => {
                    warn!("ALT fetch failed {}: {}", alt, e);
                }
            }
        }

        let v0 = V0Message::try_compile(
            payer,
            &instructions,
            &alts,
            recent_blockhash,
        )?;
        let tx = VersionedTransaction::try_new(v0, &[self.get_payer_signer()?])?;
        Ok(tx)
    }

    // Note: This detector is not an executor, so it doesn't own a wallet signer.
    // Provide a hook to supply a signer reference if embedded usage passes it in another way.
    // Default implementation returns an error; override/wire at integration points.
    fn get_payer_signer(&self) -> Result<&Keypair> {
        Err(anyhow!("OrderbookLiquidityDetector has no wallet signer; provide one via integration"))
    }

    async fn send_versioned_with_retries(
        &self,
        mut tx: VersionedTransaction,
        max_retries: u8,
    ) -> Result<Signature> {
        let mut attempt: u8 = 0;
        loop {
            let bytes = bincode::serialize(&tx)?;
            let cfg = RpcSendTransactionConfig {
                skip_preflight: true,
                preflight_commitment: Some(CommitmentConfig::confirmed().commitment),
                ..Default::default()
            };
            match self.rpc.send_raw_transaction_with_config(&bytes, cfg.clone()).await {
                Ok(sig) => return Ok(sig),
                Err(e) => {
                    attempt = attempt.saturating_add(1);
                    if attempt > max_retries { return Err(anyhow!("send v0 failed after retries: {e}")); }
                    // Refresh blockhash and re-sign if v0
                    let new_bh = self.rpc.get_latest_blockhash().await?;
                    let msg = match &mut tx.message {
                        solana_sdk::transaction::VersionedMessage::V0(ref mut inner) => {
                            inner.recent_blockhash = new_bh;
                            Some(inner.clone())
                        }
                        _ => None,
                    };
                    if let Some(v0) = msg {
                        tx = VersionedTransaction::try_new(v0, &[self.get_payer_signer()?])?;
                    }
                    tokio::time::sleep(Duration::from_millis(120)).await;
                }
            }
        }
    }

    fn pack_jito_bundle(&self, txs: &[VersionedTransaction]) -> Result<Vec<Vec<u8>>> {
        let mut out = Vec::with_capacity(txs.len());
        for tx in txs { out.push(bincode::serialize(tx)?); }
        Ok(out)
    }

    // ---------------- TPU / QUIC Direct Send -----------------
    pub async fn init_tpu_client(&self, ws_url: &str) -> Result<()> {
        // Create TPU client and store
        match TpuClient::new(self.rpc.clone(), ws_url, CommitmentConfig::processed()) {
            Ok(client) => {
                let mut guard = self.tpu.write().await;
                *guard = Some(Arc::new(client));
                Ok(())
            }
            Err(e) => Err(anyhow!("TPU client init failed: {e}")),
        }
    }

    pub async fn try_send_via_tpu_with_fallback(&self, tx: &VersionedTransaction, max_retries: u8) -> Result<Signature> {
        // Prefer TPU if available
        {
            let guard = self.tpu.read().await;
            if let Some(ref tpu) = *guard {
                if tpu.try_send_transaction(tx).is_ok() {
                    // We don't get a signature back; reconstruct from tx
                    let sig = tx.signatures.get(0).cloned().ok_or_else(|| anyhow!("missing signature"))?;
                    return Ok(sig);
                }
            }
        }
        // Fallback to RPC send with retries
        self.send_versioned_with_retries(tx.clone(), max_retries).await
    }

    pub async fn start(&self) -> Result<()> {
        self.running.store(true, Ordering::Release);
        
        let markets = self.discover_markets().await?;
        for market in markets {
            self.markets.insert(market.pubkey, market);
        }

        let (tx, mut rx) = mpsc::channel::<OrderbookUpdate>(2048);
        
        tokio::spawn(self.clone().orderbook_updater(tx.clone()));
        tokio::spawn(self.clone().opportunity_scanner());
        tokio::spawn(self.clone().price_feed_updater());
        tokio::spawn(self.clone().cache_cleaner());
        tokio::spawn(self.clone().leader_schedule_updater());

        while let Some(update) = rx.recv().await {
            // Capture current slot once per update batch iteration
            let current_slot = self.rpc.get_slot().await.unwrap_or(0);
            self.process_orderbook_update(update, current_slot).await?;
        }

        Ok(())
    }

    async fn discover_markets(&self) -> Result<Vec<MarketInfo>> {
        let mut markets = Vec::new();

        // Discover Serum markets
        let serum_markets = self.discover_serum_markets().await?;
        markets.extend(serum_markets);

        // Discover Phoenix markets
        let phoenix_markets = self.discover_phoenix_markets().await?;
        markets.extend(phoenix_markets);

        // Discover Mango markets
        let mango_markets = self.discover_mango_markets().await?;
        markets.extend(mango_markets);

        Ok(markets)
    }

    async fn discover_serum_markets(&self) -> Result<Vec<MarketInfo>> {
        let program_id = Pubkey::from_str(SERUM_DEX_V3)?;
        let config = RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::DataSize(
                std::mem::size_of::<MarketState>() as u64
            )]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                commitment: Some(CommitmentConfig::confirmed()),
                ..Default::default()
            },
            ..Default::default()
        };

        let accounts = self.rpc.get_program_accounts_with_config(&program_id, config).await?;
        let mut markets = Vec::new();

        for (pubkey, account) in accounts {
            if let Ok(market) = Market::load(&account, &program_id, false) {
                markets.push(MarketInfo {
                    pubkey,
                    protocol: DexProtocol::SerumV3,
                    base_mint: market.coin_mint,
                    quote_mint: market.pc_mint,
                    base_decimals: market.coin_decimals,
                    quote_decimals: market.pc_decimals,
                    tick_size: market.pc_lot_size as f64 / 10f64.powi(market.pc_decimals as i32),
                    lot_size: market.coin_lot_size as f64 / 10f64.powi(market.coin_decimals as i32),
                });
            }
        }

        Ok(markets)
    }

    async fn discover_phoenix_markets(&self) -> Result<Vec<MarketInfo>> {
        let program_id = Pubkey::from_str(PHOENIX_V1)?;
        let config = RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::Memcmp(Memcmp {
                offset: 0,
                bytes: MemcmpEncodedBytes::Bytes(vec![1]), // Phoenix market discriminator
                encoding: None,
            })]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                commitment: Some(CommitmentConfig::confirmed()),
                ..Default::default()
            },
            ..Default::default()
        };

        let accounts = self.rpc.get_program_accounts_with_config(&program_id, config).await?;
        let mut markets = Vec::new();

        for (pubkey, account) in accounts {
            if account.data.len() >= std::mem::size_of::<MarketHeader>() {
                let header = MarketHeader::load(&account.data)?;
                markets.push(MarketInfo {
                    pubkey,
                    protocol: DexProtocol::Phoenix,
                    base_mint: header.base_params.mint_key,
                    quote_mint: header.quote_params.mint_key,
                    base_decimals: header.base_params.decimals,
                    quote_decimals: header.quote_params.decimals,
                    tick_size: header.get_tick_size_in_quote_atoms_per_base_unit() as f64,
                    lot_size: header.get_base_lot_size() as f64,
                });
            }
        }

        Ok(markets)
    }

    async fn discover_mango_markets(&self) -> Result<Vec<MarketInfo>> {
        let program_id = Pubkey::from_str(MANGO_V4)?;
        let accounts = self.rpc.get_program_accounts(&program_id).await?;
        let mut markets = Vec::new();

        for (pubkey, account) in accounts {
            if account.data.len() >= 8 && &account.data[0..8] == b"PerpMkt\0" {
                if let Ok(perp_market) = PerpMarket::try_from_slice(&account.data[8..]) {
                    markets.push(MarketInfo {
                        pubkey,
                        protocol: DexProtocol::MangoV4,
                        base_mint: perp_market.base_mint,
                        quote_mint: perp_market.quote_mint,
                        base_decimals: perp_market.base_decimals,
                        quote_decimals: perp_market.quote_decimals,
                        tick_size: perp_market.quote_lot_size as f64 / 10f64.powi(perp_market.quote_decimals as i32),
                        lot_size: perp_market.base_lot_size as f64 / 10f64.powi(perp_market.base_decimals as i32),
                    });
                }
            }
        }

        Ok(markets)
    }

    async fn orderbook_updater(self: Arc<Self>, tx: mpsc::Sender<OrderbookUpdate>) {
        let mut tick = interval(Duration::from_millis(ORDERBOOK_REFRESH_MS));
        const MAX_CONCURRENCY: usize = 64;

        while self.running.load(Ordering::Acquire) {
            tick.tick().await;

            // Snapshot market list to avoid holding iter guard during awaits
            let markets: Vec<MarketInfo> = self
                .markets
                .iter()
                .map(|e| e.value().clone())
                .collect();

            // Bounded-concurrency async fetching with error-tolerant pipeline
            let _ = stream::iter(markets)
                .map(|mi| {
                    let this = self.clone();
                    async move {
                        this.fetch_orderbook_async(&mi)
                            .await
                            .map(|snapshot| OrderbookUpdate {
                                market: mi.pubkey,
                                snapshot,
                            })
                    }
                })
                .buffer_unordered(MAX_CONCURRENCY)
                .try_for_each(|update| {
                    let tx = tx.clone();
                    async move { tx.send(update).await.map_err(|e| anyhow!("send update: {e}")) }
                })
                .await;
            // Do not crash loop on errors
        }
    }

    async fn fetch_orderbook_async(&self, market: &MarketInfo) -> Result<OrderbookSnapshot> {
        match market.protocol {
            DexProtocol::SerumV3 => self.fetch_serum_orderbook(market).await,
            DexProtocol::Phoenix => self.fetch_phoenix_orderbook(market).await,
            DexProtocol::MangoV4 => self.fetch_mango_orderbook(market).await,
        }
    }

    async fn fetch_serum_orderbook(&self, market_info: &MarketInfo) -> Result<OrderbookSnapshot> {
        let program_id = Pubkey::from_str(SERUM_DEX_V3)?;
        let account = self.rpc.get_account(&market_info.pubkey).await?;
        let market = Market::load(&account, &program_id, false)
            .map_err(|e| anyhow!("serum market load {}: {e}", market_info.pubkey))?;

        let keys = vec![market.bids, market.asks];
        let slot = self.rpc.get_slot().await.unwrap_or(0);
        let res = self.get_multiple_accounts(&keys).await?;
        let bids_acc = res.get(0).and_then(|o| o.as_ref()).ok_or_else(|| anyhow!("missing serum bids"))?;
        let asks_acc = res.get(1).and_then(|o| o.as_ref()).ok_or_else(|| anyhow!("missing serum asks"))?;

        let mut bids = Vec::new();
        let mut asks = Vec::new();

        let bid_slab = market.load_bids_mut(&bids_acc.data)?;
        for order_node in bid_slab.iter() {
            bids.push(Order {
                price: (order_node.price() as f64) * market_info.tick_size,
                size: (order_node.quantity() as f64) * market_info.lot_size,
                owner: Some(order_node.owner()),
            });
        }
        let ask_slab = market.load_asks_mut(&asks_acc.data)?;
        for order_node in ask_slab.iter() {
            asks.push(Order {
                price: (order_node.price() as f64) * market_info.tick_size,
                size: (order_node.quantity() as f64) * market_info.lot_size,
                owner: Some(order_node.owner()),
            });
        }

        Ok(OrderbookSnapshot::new(
            market_info.pubkey,
            market_info.protocol,
            bids,
            asks,
            slot,
        ))
    }

    async fn fetch_phoenix_orderbook(&self, market_info: &MarketInfo) -> Result<OrderbookSnapshot> {
        let account = self.rpc.get_account(&market_info.pubkey).await?;
        let market_data = &account.data;
        
        let header = MarketHeader::load(market_data)?;
        let base_lot_size = header.get_base_lot_size();
        let quote_lot_size = header.get_quote_lot_size();
        
        let mut bids = Vec::new();
        let mut asks = Vec::new();

        let ladder_offset = std::mem::size_of::<MarketHeader>();
        if market_data.len() > ladder_offset {
            let ladder_data = &market_data[ladder_offset..];
            
            // Parse Phoenix orderbook ladder structure
            let mut offset = 0;
            while offset + 32 <= ladder_data.len() {
                let price_in_ticks = u64::from_le_bytes(ladder_data[offset..offset+8].try_into()?);
                let size_in_base_lots = u64::from_le_bytes(ladder_data[offset+8..offset+16].try_into()?);
                let side = ladder_data[offset+16];
                
                if price_in_ticks > 0 && size_in_base_lots > 0 {
                    let price = (price_in_ticks as f64 * quote_lot_size as f64) / 
                               (base_lot_size as f64 * 10f64.powi(market_info.quote_decimals as i32 - market_info.base_decimals as i32));
                    let size = size_in_base_lots as f64 * base_lot_size as f64 / 10f64.powi(market_info.base_decimals as i32);
                    
                    let order = Order {
                        price,
                        size,
                        owner: None,
                    };
                    
                    if side == 0 {
                        bids.push(order);
                    } else {
                        asks.push(order);
                    }
                }
                offset += 32;
            }
        }

        let slot = self.rpc.get_slot().await.unwrap_or(0);
        Ok(OrderbookSnapshot::new(
            market_info.pubkey,
            market_info.protocol,
            bids,
            asks,
            slot,
        ))
    }

    async fn fetch_serum_orderbook(&self, market_info: &MarketInfo) -> Result<OrderbookSnapshot> {
        let program_id = Pubkey::from_str(SERUM_DEX_V3)?;
        let account = self.rpc.get_account(&market_info.pubkey).await?;
        let market = Market::load(&account, &program_id, false)
            .map_err(|e| anyhow!("serum market load {}: {e}", market_info.pubkey))?;

        let keys = vec![market.bids, market.asks];
        let slot = self.rpc.get_slot().await.unwrap_or(0);
        let res = self.get_multiple_accounts(&keys).await?;
        let bids_acc = res.get(0).and_then(|o| o.as_ref()).ok_or_else(|| anyhow!("missing serum bids"))?;
        let asks_acc = res.get(1).and_then(|o| o.as_ref()).ok_or_else(|| anyhow!("missing serum asks"))?;

        let mut bids = Vec::new();
        let mut asks = Vec::new();

        let bid_slab = market.load_bids_mut(&bids_acc.data)?;
        for node in bid_slab.iter() {
            bids.push(Order {
                price: (node.price() as f64) * market_info.tick_size,
                size: (node.quantity() as f64) * market_info.lot_size,
                owner: Some(node.owner()),
            });
        }
        let ask_slab = market.load_asks_mut(&asks_acc.data)?;
        for node in ask_slab.iter() {
            asks.push(Order {
                price: (node.price() as f64) * market_info.tick_size,
                size: (node.quantity() as f64) * market_info.lot_size,
                owner: Some(node.owner()),
            });
        }

        Ok(OrderbookSnapshot::new(
            market_info.pubkey,
            market_info.protocol,
            bids,
            asks,
            slot,
        ))
    }

    fn parse_mango_book_side(&self, data: &[u8], is_bids: bool) -> Vec<(i64, i64)> {
        let mut orders = Vec::new();
        let node_size = 88; // Mango BookSideNode size
        
        for chunk in data.chunks_exact(node_size) {
            if chunk.len() >= 24 {
                let price = i64::from_le_bytes(chunk[0..8].try_into().unwrap_or([0u8; 8]));
                let quantity = i64::from_le_bytes(chunk[8..16].try_into().unwrap_or([0u8; 8]));
                
                if price > 0 && quantity > 0 {
                    orders.push((price, quantity));
                }
            }
        }
        
        if is_bids {
            orders.sort_by(|a, b| b.0.cmp(&a.0));
        } else {
            orders.sort_by(|a, b| a.0.cmp(&b.0));
        }
        
        orders
    }

    async fn fetch_mango_orderbook(&self, market_info: &MarketInfo) -> Result<OrderbookSnapshot> {
        let account = self.rpc.get_account(&market_info.pubkey).await?;
        let perp_market = PerpMarket::try_from_slice(&account.data[8..])?;

        // Batched getMultipleAccounts for bids/asks
        let ba = self.get_multiple_accounts(&[perp_market.bids, perp_market.asks]).await?;
        let bids_account = ba.get(0).and_then(|o| o.clone()).ok_or_else(|| anyhow!("missing Mango bids account"))?;
        let asks_account = ba.get(1).and_then(|o| o.clone()).ok_or_else(|| anyhow!("missing Mango asks account"))?;

        let mut bids = Vec::new();
        let mut asks = Vec::new();

        // Parse Mango orderbook nodes (placeholder approach; TODO: exact crit-bit parsing)
        let bid_nodes = self.parse_mango_book_side(&bids_account.data, true);
        for (price, size) in bid_nodes {
            bids.push(Order {
                price: price as f64 / 10f64.powi(perp_market.quote_decimals as i32 - perp_market.base_decimals as i32),
                size: size as f64 / 10f64.powi(perp_market.base_decimals as i32),
                owner: None,
            });
        }
        let ask_nodes = self.parse_mango_book_side(&asks_account.data, false);
        for (price, size) in ask_nodes {
            asks.push(Order {
                price: price as f64 / 10f64.powi(perp_market.quote_decimals as i32 - perp_market.base_decimals as i32),
                size: size as f64 / 10f64.powi(perp_market.base_decimals as i32),
                owner: None,
            });
        }

        let slot = self.rpc.get_slot().await.unwrap_or(0);
        Ok(OrderbookSnapshot::new(
            market_info.pubkey,
            market_info.protocol,
            bids,
            asks,
            slot,
        ))
    }

    fn calculate_spread_and_mid(&self, bids: &[Order], asks: &[Order]) -> (u16, f64) {
        if bids.is_empty() || asks.is_empty() {
            return (u16::MAX, 0.0);
        }
        let best_bid = bids[0].price;
        let best_ask = asks[0].price;
        if !(best_bid.is_finite() && best_ask.is_finite()) || best_bid <= 0.0 || best_ask <= 0.0 {
            return (u16::MAX, 0.0);
        }
        let mid = 0.5 * (best_bid + best_ask);
        if !mid.is_finite() || mid <= 0.0 {
            return (u16::MAX, 0.0);
        }
        let spread_bps = (((best_ask - best_bid) / mid) * 10000.0).max(0.0).round() as u16;
        (spread_bps, mid)
    }

    fn calculate_liquidity_usd(&self, orders: &[Order], reference_price: f64) -> f64 {
        orders.iter()
            .take(MIN_DEPTH_LEVELS)
            .map(|order| order.size * order.price)
            .sum()
    }

    async fn process_orderbook_update(&self, update: OrderbookUpdate, current_slot: Slot) -> Result<()> {
        let old_snapshot = self.orderbooks.get(&update.market);
        self.orderbooks.insert(update.market, update.snapshot.clone());
        self.last_update.store(chrono::Utc::now().timestamp_millis() as u64, Ordering::Release);

        if let Some(old) = old_snapshot {
            // Circuit breaker: if snapshot is too stale in slots relative to current slot, skip detection
            let lag = current_slot.saturating_sub(update.snapshot.slot);
            if lag < MAX_SLOT_LAG_HARD {
                self.detect_opportunities(&old, &update.snapshot, current_slot).await?;
            }
        }

        // Update rolling volatility and execution confidence for this market
        self.update_volatility(update.market, update.snapshot.mid_price);
        let conf = self.predict_execution_success(&update.snapshot, current_slot);
        self.execution_confidence.insert(update.market, conf);

        // Frequency sketch: increment market hotness
        self.hit_frequency
            .entry(update.market)
            .and_modify(|v| *v = v.saturating_add(1))
            .or_insert(1);

        Ok(())
    }

    async fn detect_opportunities(&self, old: &OrderbookSnapshot, new: &OrderbookSnapshot, current_slot: Slot) -> Result<()> {
        // Basic sanity and circuit breakers
        if new.bids.is_empty() || new.asks.is_empty() {
            return Ok(());
        }
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        if new.spread_bps == u16::MAX || new.mid_price <= 0.0 {
            return Ok(());
        }
        if (new.bid_liquidity_usd + new.ask_liquidity_usd) < MIN_LIQUIDITY_USD {
            return Ok(());
        }

        let conf = self.calculate_confidence_with_slot(new, current_slot);
        if conf < 0.2 {
            return Ok(());
        }

        let mut out: Vec<LiquidityOpportunity> = Vec::new();

        // Parameters
        let target_usd = new.bid_liquidity_usd.min(new.ask_liquidity_usd).min(25_000.0);
        let target_base = (target_usd / new.mid_price).max(0.001);
        let fees = self.default_taker_fees(new.protocol);

        // Spread capture on same book (depth- and fee-aware)
        if (new.spread_bps as u32) > (OPPORTUNITY_THRESHOLD_BPS as u32) && new.spread_bps <= MAX_SPREAD_BPS {
            if let Some((bps_after, filled, buy_vwap, sell_vwap)) = self.profit_after_cost_bps(
                new.asks.as_ref(),
                new.bids.as_ref(),
                target_base,
                fees,
                fees,
            ) {
                if bps_after > (OPPORTUNITY_THRESHOLD_BPS as f64) && filled > 0.0 {
                    out.push(LiquidityOpportunity {
                        market: new.market,
                        protocol: new.protocol,
                        opportunity_type: OpportunityType::SpreadCapture,
                        entry_price: buy_vwap,
                        exit_price: sell_vwap,
                        size: filled,
                        profit_bps: (bps_after.max(0.0)) as u16,
                        confidence: conf,
                        expires_at: now_ms + 1500,
                    });
                }
            }
        }

        // Liquidity imbalance + momentum check
        let old_mid = old.mid_price;
        if old_mid > 0.0 && new.mid_price > 0.0 {
            let change = ((new.mid_price - old_mid) / old_mid).abs();
            if (new.imbalance_ratio < 0.5 || new.imbalance_ratio > 2.0) && change > 0.001 {
                let sign = if new.imbalance_ratio < 1.0 { 1.0 } else { -1.0 };
                let tgt = (target_base * 0.5).max(0.001);
                let entry = new.mid_price;
                let exit = entry * (1.0 + 0.002 * sign);
                let profit_bps = (((exit - entry) / entry) * 10000.0) as u16;
                if profit_bps > OPPORTUNITY_THRESHOLD_BPS {
                    out.push(LiquidityOpportunity {
                        market: new.market,
                        protocol: new.protocol,
                        opportunity_type: OpportunityType::LiquidityImbalance,
                        entry_price: entry,
                        exit_price: exit,
                        size: tgt,
                        profit_bps,
                        confidence: conf * 0.9,
                        expires_at: now_ms + 1200,
                    });
                }
            }
        }

        // Large movement event
        if old.mid_price > 0.0 {
            let pc = ((new.mid_price - old.mid_price) / old.mid_price).abs();
            if pc > 0.005 {
                out.push(LiquidityOpportunity {
                    market: new.market,
                    protocol: new.protocol,
                    opportunity_type: OpportunityType::LargePriceMovement,
                    entry_price: new.mid_price,
                    exit_price: old.mid_price,
                    size: (target_base * 0.25).max(0.001),
                    profit_bps: (pc * 10000.0) as u16,
                    confidence: conf * 0.8,
                    expires_at: now_ms + 1000,
                });
            }
        }

        if !out.is_empty() {
            let mut opps = self.opportunities.write().await;
            for opp in out {
                if opps.len() >= 2048 {
                    opps.pop_front();
                }
                opps.push_back(opp);
            }
        }

        // Execution hook: attempt to build and submit if profitable
        if new.spread_bps >= OPPORTUNITY_THRESHOLD_BPS {
            if let Ok(tx) = self.build_profitable_tx(new, new.spread_bps).await {
                let _ = self.submit_bundle(tx, current_slot).await;
            }
        }

        Ok(())
    }

    fn calculate_confidence_with_slot(&self, snapshot: &OrderbookSnapshot, current_slot: Slot) -> f64 {
        // Liquidity-quality component
        let mut liq = 1.0;
        if snapshot.spread_bps > MAX_SPREAD_BPS { liq *= 0.5; }
        let total_liq = snapshot.bid_liquidity_usd + snapshot.ask_liquidity_usd;
        if total_liq < MIN_LIQUIDITY_USD * 2.0 { liq *= 0.7; }
        let count = snapshot.bids.len() + snapshot.asks.len();
        if count < MIN_DEPTH_LEVELS * 2 { liq *= 0.85; }
        if snapshot.imbalance_ratio < 0.2 || snapshot.imbalance_ratio > 5.0 { liq *= 0.6; }

        // Slot-trust component
        let mut slot_trust = 1.0;
        if current_slot > snapshot.slot {
            let lag = current_slot - snapshot.slot;
            if lag >= MAX_SLOT_LAG_HARD { slot_trust *= 0.2; }
            else if lag >= MAX_SLOT_LAG_SOFT { slot_trust *= 0.7; }
        }
        // Leader schedule penalty: look ahead one slot (approx landing slot)
        if let Some(leader) = self.leader_for_slot_sync(current_slot.saturating_add(1)) {
            if let Ok(set) = self.ghosted_validators.try_read() {
                if set.contains(&leader) { slot_trust *= 0.6; }
            }
        }

        // Blend
        let blended = 0.6 * liq + 0.4 * slot_trust;
        blended.clamp(0.1, 1.0)
    }

    // Strict Pyth price parsing with header/body validation per classic layout
    fn parse_pyth_price(data: &[u8]) -> Option<(f64, i32, u32, u64)> {
        // Minimum size up to agg_status + pad
        if data.len() < (4 * 4 + 4 * 4 + 8 * 9 + 32 + 32 + 8 + 8 + 8 + 4 + 4) {
            return None;
        }
        // Header: magic, ver, atype, size
        let magic = u32::from_le_bytes(data.get(0..4)?.try_into().ok()?);
        let ver = u32::from_le_bytes(data.get(4..8)?.try_into().ok()?);
        let atype = u32::from_le_bytes(data.get(8..12)?.try_into().ok()?);
        if magic != 0xa1b2c3d4 || atype != 3 || ver < 2 {
            return None;
        }
        // Offsets after 16-byte header
        let expo = i32::from_le_bytes(data.get(16 + 4..16 + 8)?.try_into().ok()?);
        let valid_slot = u64::from_le_bytes(data.get(16 + 24..16 + 32)?.try_into().ok()?);
        // Compute agg_price offset: 136 bytes after header
        let agg_price_off = 16 + 136;
        let agg_price = i64::from_le_bytes(data.get(agg_price_off..agg_price_off + 8)?.try_into().ok()?);
        let agg_conf = u64::from_le_bytes(data.get(agg_price_off + 8..agg_price_off + 16)?.try_into().ok()?);
        let agg_status = u32::from_le_bytes(data.get(agg_price_off + 16..agg_price_off + 20)?.try_into().ok()?);
        if agg_status != 1 || agg_price == 0 || agg_conf == 0 {
            return None;
        }
        let price = (agg_price as f64) * 10f64.powi(expo);
        if !price.is_finite() || price <= 0.0 {
            return None;
        }
        Some((price, expo, agg_status, valid_slot))
    }

    async fn opportunity_scanner(self: Arc<Self>) {
        let mut interval = interval(Duration::from_millis(100));
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            // Capture slot once per scan tick
            let current_slot = self.rpc.get_slot().await.unwrap_or(0);

            let now = chrono::Utc::now().timestamp_millis() as u64;
            let mut opportunities = self.opportunities.write().await;
            
            // Remove expired opportunities
            opportunities.retain(|opp| opp.expires_at > now);
            
            // Cross-DEX arbitrage detection
            let markets: Vec<(Pubkey, MarketInfo)> = self.markets.iter()
                .map(|entry| (*entry.key(), entry.value().clone()))
                .collect();
            
            for i in 0..markets.len() {
                for j in i+1..markets.len() {
                    if markets[i].1.base_mint == markets[j].1.base_mint && 
                       markets[i].1.quote_mint == markets[j].1.quote_mint {
                        
                        if let (Some(ob1), Some(ob2)) = (
                            self.orderbooks.get(&markets[i].0),
                            self.orderbooks.get(&markets[j].0)
                        ) {
                            if let Some(arb) = self.detect_cross_dex_arbitrage_with_slot(&ob1, &ob2, current_slot) {
                                if opportunities.len() >= 1000 {
                                    opportunities.pop_front();
                                }
                                opportunities.push_back(arb);
                            }
                        }
                    }
                }
            }
        }
    }

    fn detect_cross_dex_arbitrage_with_slot(&self, ob1: &OrderbookSnapshot, ob2: &OrderbookSnapshot, current_slot: Slot) -> Option<LiquidityOpportunity> {
        if ob1.bids.is_empty() || ob1.asks.is_empty() || ob2.bids.is_empty() || ob2.asks.is_empty() {
            return None;
        }

        // Circuit breaker: drop arbitrage if either book is too stale
        let lag1 = current_slot.saturating_sub(ob1.slot);
        let lag2 = current_slot.saturating_sub(ob2.slot);
        if lag1 >= MAX_SLOT_LAG_HARD || lag2 >= MAX_SLOT_LAG_HARD {
            return None;
        }

        // Determine a target size using shared liquidity and mid; cap notional to avoid huge sweeps
        let mid = 0.5 * (ob1.asks[0].price + ob2.bids[0].price);
        if !mid.is_finite() || mid <= 0.0 { return None; }
        let shared_usd = ob1.bid_liquidity_usd.min(ob2.ask_liquidity_usd).min(5_000.0);
        let target_base = (shared_usd / mid).max(0.0);
        if target_base <= 0.0 { return None; }

        let f1 = self.default_taker_fees(ob1.protocol);
        let f2 = self.default_taker_fees(ob2.protocol);

        // Direction 1: buy ob1 asks, sell ob2 bids
        let dir1 = self.profit_after_cost_bps(ob1.asks.as_ref(), ob2.bids.as_ref(), target_base, f1, f2)
            .map(|(bps, filled, buy_vwap, sell_vwap)| (bps, filled, buy_vwap, sell_vwap, ob1.market, ob1.protocol));
        // Direction 2: buy ob2 asks, sell ob1 bids
        let dir2 = self.profit_after_cost_bps(ob2.asks.as_ref(), ob1.bids.as_ref(), target_base, f2, f1)
            .map(|(bps, filled, buy_vwap, sell_vwap)| (bps, filled, buy_vwap, sell_vwap, ob2.market, ob2.protocol));

        let best = match (dir1, dir2) {
            (Some(a), Some(b)) => if a.0 >= b.0 { Some(a) } else { Some(b) },
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            _ => None,
        }?;

        let profit_bps = best.0 as u16;
        if profit_bps <= OPPORTUNITY_THRESHOLD_BPS { return None; }

        Some(LiquidityOpportunity {
            market: best.4,
            protocol: best.5,
            opportunity_type: OpportunityType::CrossDexArbitrage,
            entry_price: best.2,
            exit_price: best.3,
            size: best.1,
            profit_bps: profit_bps.saturating_sub(MAX_SLIPPAGE_BPS),
            confidence: (self.calculate_confidence_with_slot(ob1, current_slot) + self.calculate_confidence_with_slot(ob2, current_slot)) / 2.0,
            expires_at: chrono::Utc::now().timestamp_millis() as u64 + 1000,
        })
    }

    #[inline]
    fn simulate_take_asks(&self, asks: &[Order], mut base: f64) -> Option<(f64, f64)> {
        if base <= 0.0 { return None; }
        let mut cost = 0.0;
        let mut filled = 0.0;
        for lvl in asks {
            if base <= 0.0 { break; }
            let take = base.min(lvl.size);
            cost += take * lvl.price;
            filled += take;
            base -= take;
        }
        if filled > 0.0 && cost.is_finite() { Some((cost / filled, filled)) } else { None }
    }

    #[inline]
    fn simulate_take_bids(&self, bids: &[Order], mut base: f64) -> Option<(f64, f64)> {
        if base <= 0.0 { return None; }
        let mut revenue = 0.0;
        let mut filled = 0.0;
        for lvl in bids {
            if base <= 0.0 { break; }
            let take = base.min(lvl.size);
            revenue += take * lvl.price;
            filled += take;
            base -= take;
        }
        if filled > 0.0 && revenue.is_finite() { Some((revenue / filled, filled)) } else { None }
    }

    fn profit_after_cost_bps(
        &self,
        buy_asks: &[Order],
        sell_bids: &[Order],
        target_base: f64,
        fees_buy: Fees,
        fees_sell: Fees,
    ) -> Option<(f64 /*bps*/, f64 /*filled_base*/, f64 /*buy_vwap*/, f64 /*sell_vwap*/)> {
        let (buy_vwap, f1) = self.simulate_take_asks(buy_asks, target_base)?;
        let (sell_vwap, f2) = self.simulate_take_bids(sell_bids, target_base)?;
        let filled = f1.min(f2);
        if filled <= 0.0 { return None; }
        let buy_cost = buy_vwap * (1.0 + fees_buy.taker_bps / 10000.0);
        let sell_rev = sell_vwap * (1.0 - fees_sell.taker_bps / 10000.0);
        let pnl_ratio = (sell_rev - buy_cost) / buy_cost;
        Some(((pnl_ratio * 10000.0), filled, buy_vwap, sell_vwap))
    }

    #[inline]
    fn default_taker_fees(&self, protocol: DexProtocol) -> Fees {
        match protocol {
            DexProtocol::SerumV3 => Fees { taker_bps: 5.0 },   // approximate default
            DexProtocol::Phoenix => Fees { taker_bps: 0.0 },    // Phoenix often maker/taker-free in atoms; adjust if needed
            DexProtocol::MangoV4 => Fees { taker_bps: 5.0 },    // derivative-like, approximate
        }
    }

    async fn price_feed_updater(self: Arc<Self>) {
        let mut tick = interval(Duration::from_secs(5));
        let _pyth_program = Pubkey::from_str("FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH").unwrap();
        let price_accounts = vec![
            Pubkey::from_str("H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG").unwrap(), // SOL/USD
            Pubkey::from_str("GVXRSBjFk6e6J3NbVPXohDJetcTjaeeuykUpbQF8UoMU").unwrap(), // BTC/USD
            Pubkey::from_str("JBu1AL4obBcCMqKBBxhpWCNUt136ijcuMZLFvTP7iWdB").unwrap(), // ETH/USD
            Pubkey::from_str("Gnt27xtC473ZT2Mw5u8wZ68Z3gULkSTb5DuxJy7eJotD").unwrap(), // USDC/USD
        ];

        while self.running.load(Ordering::Acquire) {
            tick.tick().await;

            let current_slot = self.rpc.get_slot().await.unwrap_or(0);
            match self.get_multiple_accounts(&price_accounts).await {
                Ok(accs) => {
                    for (i, acc) in accs.into_iter().enumerate() {
                        let Some(account) = acc else { continue; };
                        if let Some((price, _expo, _status, valid_slot)) = Self::parse_pyth_price(&account.data) {
                            // freshness gate: <= 10 slots lag
                            if current_slot > 0 && valid_slot > 0 && current_slot.saturating_sub(valid_slot) <= 10 {
                                self.price_feeds.insert(price_accounts[i], price);
                            }
                        }
                    }
                }
                Err(_) => {
                    // transient RPC error; skip this tick
                }
            }
        }
    }

    async fn cache_cleaner(self: Arc<Self>) {
        let mut tick = interval(Duration::from_millis(CACHE_TTL_MS));
        while self.running.load(Ordering::Acquire) {
            tick.tick().await;
            let now = chrono::Utc::now().timestamp_millis() as u64;
            let stale_threshold = now.saturating_sub(CACHE_TTL_MS);
            let current_slot = self.rpc.get_slot().await.unwrap_or(0);

            // Orderbooks TTL (slot-based primary, time-based fallback)
            let mut candidates: Vec<(Pubkey, f64, u64)> = Vec::new();
            let mut all_freq: Vec<(Pubkey, u64)> = Vec::new();
            for entry in self.orderbooks.iter() {
                let k = *entry.key();
                let snap = entry.value();
                let slot_stale = if current_slot > 0 && snap.slot > 0 {
                    current_slot.saturating_sub(snap.slot) >= MAX_SLOT_LAG_HARD
                } else { false };
                let time_stale = snap.timestamp < stale_threshold;
                if slot_stale || time_stale {
                    let liq = snap.bid_liquidity_usd + snap.ask_liquidity_usd;
                    let freq = self.hit_frequency.get(&k).map(|v| *v.value()).unwrap_or(0);
                    candidates.push((k, liq, freq));
                }
                // collect overall freq for hot pinning
                let f = self.hit_frequency.get(&k).map(|v| *v.value()).unwrap_or(0);
                all_freq.push((k, f));
            }

            // Pin top-K hot markets
            const PIN_TOP_K: usize = 50;
            all_freq.sort_by_key(|(_, f)| std::cmp::Reverse(*f));
            let pinned: HashSet<Pubkey> = all_freq.into_iter().take(PIN_TOP_K).map(|(k, _)| k).collect();

            // Sort stale candidates: lowest freq then lowest liquidity first
            candidates.sort_by(|a, b| a.2.cmp(&b.2).then(a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)));
            for (k, _liq, _f) in candidates {
                if pinned.contains(&k) { continue; }
                let _ = self.orderbooks.remove(&k);
            }

            // Opportunities TTL + compaction
            let mut opps = self.opportunities.write().await;
            opps.retain(|o| o.expires_at > now);
            if opps.len() > 4096 {
                let drop_n = opps.len() - 4096;
                for _ in 0..drop_n { opps.pop_front(); }
            }
        }
    }

    pub async fn get_opportunities(&self, limit: usize) -> Vec<LiquidityOpportunity> {
        let opportunities = self.opportunities.read().await;
        opportunities.iter()
            .rev()
            .take(limit)
            .cloned()
            .collect()
    }

    pub async fn get_orderbook(&self, market: &Pubkey) -> Option<OrderbookSnapshot> {
        self.orderbooks.get(market).map(|entry| entry.value().clone())
    }

    pub fn get_markets(&self) -> Vec<(Pubkey, DexProtocol)> {
        self.markets.iter()
            .map(|entry| (entry.key().clone(), entry.value().protocol))
            .collect()
    }

    pub async fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub fn last_update_ms(&self) -> u64 {
        self.last_update.load(Ordering::Acquire)
    }

impl Clone for OrderbookLiquidityDetector {
    fn clone(&self) -> Self {
        Self {
            rpc: Arc::clone(&self.rpc),
            markets: Arc::clone(&self.markets),
            orderbooks: Arc::clone(&self.orderbooks),
            opportunities: Arc::clone(&self.opportunities),
            price_feeds: Arc::clone(&self.price_feeds),
            running: Arc::clone(&self.running),
            last_update: Arc::clone(&self.last_update),
        }
    }
}

#[derive(Debug)]
struct OrderbookUpdate {
    market: Pubkey,
    snapshot: OrderbookSnapshot,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_detector_initialization() {
        let detector = OrderbookLiquidityDetector::new("https://api.mainnet-beta.solana.com")
            .await
            .unwrap();
        assert!(!detector.is_running());
    }

    #[tokio::test]
    async fn test_spread_calculation() {
        let detector = OrderbookLiquidityDetector::new("https://api.mainnet-beta.solana.com")
            .await
            .unwrap();
        
        let bids = vec![
            Order { price: 99.5, size: 10.0, owner: None },
            Order { price: 99.0, size: 20.0, owner: None },
        ];
        
        let asks = vec![
            Order { price: 100.5, size: 10.0, owner: None },
            Order { price: 101.0, size: 20.0, owner: None },
        ];
        
        let (spread_bps, mid_price) = detector.calculate_spread_and_mid(&bids, &asks);
        assert_eq!(spread_bps, 100); // 1% spread = 100 bps
        assert_eq!(mid_price, 100.0);
    }

    #[tokio::test]
    async fn test_liquidity_calculation() {
        let detector = OrderbookLiquidityDetector::new("https://api.mainnet-beta.solana.com")
            .await
            .unwrap();
        
        let orders = vec![
            Order { price: 100.0, size: 10.0, owner: None },
            Order { price: 99.0, size: 20.0, owner: None },
            Order { price: 98.0, size: 30.0, owner: None },
        ];
        
        let liquidity = detector.calculate_liquidity_usd(&orders, 100.0);
        assert_eq!(liquidity, 5940.0); // 100*10 + 99*20 + 98*30
    }

    #[tokio::test]
    async fn simulate_take_monotonicity() {
        let d = OrderbookLiquidityDetector::new("https://api.mainnet-beta.solana.com")
            .await
            .unwrap();
        let asks = vec![
            Order { price: 100.0, size: 1.0, owner: None },
            Order { price: 100.1, size: 1.0, owner: None },
            Order { price: 100.2, size: 1.0, owner: None },
        ];
        let (vwap1, f1) = d.simulate_take_asks(&asks, 0.5).unwrap();
        let (vwap2, f2) = d.simulate_take_asks(&asks, 2.0).unwrap();
        assert!(f2 >= f1);
        assert!(vwap2 >= vwap1);
    }

    #[tokio::test]
    async fn profit_after_cost_signals_only_with_depth() {
        let d = OrderbookLiquidityDetector::new("https://api.mainnet-beta.solana.com")
            .await
            .unwrap();
        let asks = vec![
            Order { price: 100.0, size: 1.0, owner: None },
            Order { price: 100.2, size: 1.0, owner: None },
        ];
        let bids = vec![
            Order { price: 100.4, size: 1.0, owner: None },
            Order { price: 100.3, size: 1.0, owner: None },
        ];
        let fees = Fees { taker_bps: 5.0 };
        let res = d.profit_after_cost_bps(&asks, &bids, 1.5, fees, fees).unwrap();
        assert!(res.0 > 0.0);
        assert!(res.1 > 0.0);
    }

    #[tokio::test]
    async fn pyth_parser_rejects_invalid() {
        // Clearly invalid small buffer
        let bad = vec![0u8; 64];
        assert!(OrderbookLiquidityDetector::parse_pyth_price(&bad).is_none());
    }
}
}
