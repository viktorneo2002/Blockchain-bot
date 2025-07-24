use std::sync::{Arc, Mutex};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, mpsc, broadcast};
use tokio::time::interval;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcTransactionConfig, RpcBlockConfig};
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    transaction::Transaction,
    instruction::CompiledInstruction,
    commitment_config::CommitmentConfig,
};
use solana_transaction_status::{
    UiTransactionEncoding,
    EncodedConfirmedTransactionWithStatusMeta,
};
use serde::{Serialize, Deserialize};
use dashmap::DashMap;
use futures::stream::{StreamExt, FuturesUnordered};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use serde_json::{json, Value};
use borsh::{BorshDeserialize, BorshSerialize};
use rayon::prelude::*;
use crossbeam::channel;
use lru::LruCache;
use std::str::FromStr;
use bytemuck::{Pod, Zeroable};

const RAYDIUM_V4: Pubkey = solana_sdk::pubkey!("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8");
const ORCA_WHIRLPOOL: Pubkey = solana_sdk::pubkey!("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc");
const JUPITER_V6: Pubkey = solana_sdk::pubkey!("JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4");
const OPENBOOK_V3: Pubkey = solana_sdk::pubkey!("srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX");
const PUMP_FUN: Pubkey = solana_sdk::pubkey!("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");
const PYTH_ORACLE: Pubkey = solana_sdk::pubkey!("FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH");
const SOLEND_PROGRAM: Pubkey = solana_sdk::pubkey!("So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo");

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct RaydiumPoolState {
    status: u64,
    nonce: u64,
    max_order: u64,
    depth: u64,
    base_decimal: u64,
    quote_decimal: u64,
    state: u64,
    reset_flag: u64,
    min_size: u64,
    vol_max_cut_ratio: u64,
    amount_wave: u64,
    coin_lot_size: u64,
    pc_lot_size: u64,
    min_price_multiplier: u64,
    max_price_multiplier: u64,
    sys_decimal_value: u64,
    fees: RaydiumFees,
    out_put: RaydiumOutPutData,
    token_coin: Pubkey,
    token_pc: Pubkey,
    coin_mint: Pubkey,
    pc_mint: Pubkey,
    lp_mint: Pubkey,
    open_orders: Pubkey,
    market: Pubkey,
    market_program: Pubkey,
    target_orders: Pubkey,
    withdraw_queue: Pubkey,
    token_temp_lp: Pubkey,
    amm_owner: Pubkey,
    lp_amount: u64,
    client_order_id: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct RaydiumFees {
    min_separate_numerator: u64,
    min_separate_denominator: u64,
    trade_fee_numerator: u64,
    trade_fee_denominator: u64,
    pnl_numerator: u64,
    pnl_denominator: u64,
    swap_fee_numerator: u64,
    swap_fee_denominator: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct RaydiumOutPutData {
    need_take_pnl_coin: u64,
    need_take_pnl_pc: u64,
    total_pnl_pc: u64,
    total_pnl_coin: u64,
    pool_total_deposit_pc: u128,
    pool_total_deposit_coin: u128,
    swap_coin_in_amount: u128,
    swap_pc_out_amount: u128,
    swap_coin2_pc_fee: u64,
    swap_pc_in_amount: u128,
    swap_coin_out_amount: u128,
    swap_pc2_coin_fee: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct PythPriceAccount {
    magic: u32,
    version: u32,
    account_type: u32,
    size: u32,
    price_type: u32,
    exponent: i32,
    num_components: u32,
    last_slot: u64,
    valid_slot: u64,
    twap: i64,
    agg: PriceInfo,
}

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct PriceInfo {
    price: i64,
    conf: u64,
    status: u32,
    corp_act: u32,
    pub_slot: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarvestedIntent {
    pub signature: String,
    pub timestamp: u64,
    pub intent_type: IntentType,
    pub program_id: Pubkey,
    pub accounts: Vec<Pubkey>,
    pub data: Vec<u8>,
    pub compute_units: u64,
    pub priority_fee: u64,
    pub estimated_value: f64,
    pub confidence_score: f64,
    pub arbitrage_paths: Vec<ArbitragePath>,
    pub liquidation_opportunity: Option<LiquidationData>,
    pub sandwich_target: Option<SandwichData>,
    pub mev_type: MEVType,
    pub slot: u64,
    pub block_time: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IntentType {
    Swap { input_mint: Pubkey, output_mint: Pubkey, amount: u64, min_out: u64 },
    AddLiquidity { pool: Pubkey, amounts: Vec<u64> },
    RemoveLiquidity { pool: Pubkey, lp_amount: u64 },
    OpenPosition { market: Pubkey, side: bool, size: u64, leverage: u8 },
    ClosePosition { market: Pubkey, position_id: Pubkey },
    Liquidate { account: Pubkey, debt_mint: Pubkey, collateral_mint: Pubkey },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MEVType {
    Arbitrage { profit_bps: u16, hops: u8 },
    Sandwich { victim_loss: f64, attack_profit: f64 },
    Liquidation { ltv_ratio: f64, bonus_rate: f64 },
    JIT { pool: Pubkey, blocks_ahead: u8 },
    BackRun { parent_profit: f64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArbitragePath {
    pub dexes: Vec<String>,
    pub pools: Vec<Pubkey>,
    pub route: Vec<Pubkey>,
    pub amounts: Vec<u64>,
    pub expected_profit: f64,
    pub gas_estimate: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidationData {
    pub account: Pubkey,
    pub collateral_value: f64,
    pub debt_value: f64,
    pub health_ratio: f64,
    pub max_liquidation: u64,
    pub profit_estimate: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandwichData {
    pub victim_tx: String,
    pub victim_amount: u64,
    pub front_run_amount: u64,
    pub back_run_amount: u64,
    pub expected_profit: f64,
    pub pool_tvl: f64,
}

pub struct PreExecutionHarvester {
    rpc_client: Arc<RpcClient>,
    mempool_cache: Arc<DashMap<String, (Transaction, Instant)>>,
    price_cache: Arc<DashMap<Pubkey, (f64, u64, Instant)>>,
    pool_states: Arc<DashMap<Pubkey, PoolInfo>>,
    intent_sender: mpsc::Sender<HarvestedIntent>,
    metrics: Arc<Metrics>,
    ws_client: Arc<RwLock<Option<tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>>>>,
    dedup_cache: Arc<Mutex<LruCache<String, ()>>>,
}

#[derive(Clone)]
struct PoolInfo {
    dex: String,
    token_a: Pubkey,
    token_b: Pubkey,
    reserve_a: u64,
    reserve_b: u64,
    fee_bps: u16,
    volume_24h: f64,
    tvl: f64,
    last_update: Instant,
}

#[derive(Default)]
struct Metrics {
    intents_processed: Arc<Mutex<u64>>,
    mev_opportunities: Arc<Mutex<HashMap<String, u64>>>,
    total_profit: Arc<Mutex<f64>>,
    processing_times: Arc<Mutex<VecDeque<u128>>>,
}

impl PreExecutionHarvester {
    pub async fn new(
        rpc_url: String,
        intent_sender: mpsc::Sender<HarvestedIntent>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url,
            CommitmentConfig::processed(),
        ));
        
        Ok(Self {
            rpc_client,
            mempool_cache: Arc::new(DashMap::with_capacity(100_000)),
            price_cache: Arc::new(DashMap::new()),
            pool_states: Arc::new(DashMap::new()),
            intent_sender,
            metrics: Arc::new(Metrics::default()),
            ws_client: Arc::new(RwLock::new(None)),
            dedup_cache: Arc::new(Mutex::new(LruCache::new(std::num::NonZeroUsize::new(50_000).unwrap()))),
        })
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.initialize_pools().await?;
        
        tokio::try_join!(
            self.spawn_websocket_listener(),
            self.spawn_block_scanner(),
            self.spawn_intent_analyzer(),
            self.spawn_price_updater(),
            self.spawn_pool_updater(),
            self.spawn_metrics_reporter(),
        )?;
        
        Ok(())
    }

    async fn initialize_pools(&self) -> Result<(), Box<dyn std::error::Error>> {
        let known_pools = vec![
            (solana_sdk::pubkey!("58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2"), "raydium"),
            (solana_sdk::pubkey!("7XawhbbxtsRcQA8KTkHT9f9nc6d69UwqCDh6U5EEbEmX"), "raydium"),
            (solana_sdk::pubkey!("FpCMFDFGYotvufJ7HrFHsWEiiuxkRLvscrGhiAhbM1Qi"), "orca"),
            (solana_sdk::pubkey!("2AEWSvUds1wsufnsDPCXjFsJCMJH5SNNm7fSF4kxys9a"), "orca"),
        ];
        
        for (pool_pubkey, dex) in known_pools {
            if let Ok(account) = self.rpc_client.get_account(&pool_pubkey).await {
                self.parse_pool_account(&pool_pubkey, &account.data, dex).await;
            }
        }
        
        Ok(())
    }

    async fn parse_pool_account(&self, pool_pubkey: &Pubkey, data: &[u8], dex: &str) {
        match dex {
            "raydium" => {
                if let Ok(pool_state) = bytemuck::try_from_bytes::<RaydiumPoolState>(data) {
                    if pool_state.status == 1 {
                        let coin_balance = self.rpc_client
                            .get_token_account_balance(&pool_state.token_coin)
                            .await
                            .ok()
                            .and_then(|b| b.amount.parse::<u64>().ok())
                            .unwrap_or(0);
                            
                        let pc_balance = self.rpc_client
                            .get_token_account_balance(&pool_state.token_pc)
                            .await
                            .ok()
                            .and_then(|b| b.amount.parse::<u64>().ok())
                            .unwrap_or(0);
                        
                        self.pool_states.insert(*pool_pubkey, PoolInfo {
                            dex: dex.to_string(),
                            token_a: pool_state.coin_mint,
                            token_b: pool_state.pc_mint,
                            reserve_a: coin_balance,
                            reserve_b: pc_balance,
                            fee_bps: (pool_state.fees.trade_fee_numerator * 10000 / pool_state.fees.trade_fee_denominator) as u16,
                            volume_24h: 0.0,
                            tvl: 0.0,
                            last_update: Instant::now(),
                        });
                    }
                }
            }
            "orca" => {
                if data.len() >= 324 {
                    let token_a = Pubkey::new(&data[65..97]);
                    let token_b = Pubkey::new(&data[97..129]);
                    let sqrt_price = u128::from_le_bytes(data[253..269].try_into().unwrap_or([0; 16]));
                    let liquidity = u128::from_le_bytes(data[269..285].try_into().unwrap_or([0; 16]));
                    
                    self.pool_states.insert(*pool_pubkey, PoolInfo {
                        dex: dex.to_string(),
                        token_a,
                        token_b,
                        reserve_a: liquidity as u64,
                        reserve_b: liquidity as u64,
                        fee_bps: 30,
                        volume_24h: 0.0,
                        tvl: 0.0,
                        last_update: Instant::now(),
                    });
                }
            }
            _ => {}
        }
    }

    async fn spawn_websocket_listener(&self) -> Result<(), Box<dyn std::error::Error>> {
        let ws_url = "wss://api.mainnet-beta.solana.com";
        let (ws_stream, _) = connect_async(ws_url).await?;
        *self.ws_client.write().await = Some(ws_stream);
        
        let ws_client = self.ws_client.clone();
        let mempool_cache = self.mempool_cache.clone();
        
        tokio::spawn(async move {
            loop {
                let mut ws = ws_client.write().await;
                if let Some(stream) = ws.as_mut() {
                    let (mut write, mut read) = stream.split();
                    
                    let subscribe_msg = json!({
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "transactionSubscribe",
                        "params": [
                            {
                                "vote": false,
                                "failed": false,
                                "accountInclude": [
                                    RAYDIUM_V4.to_string(),
                                    ORCA_WHIRLPOOL.to_string(),
                                    JUPITER_V6.to_string(),
                                    OPENBOOK_V3.to_string(),
                                    PUMP_FUN.to_string(),
                                    SOLEND_PROGRAM.to_string(),
                                ]
                            },
                            {
                                "commitment": "processed",
                                "encoding": "base64",
                                "maxSupportedTransactionVersion": 0
                            }
                        ]
                    });
                    
                    let _ = write.send(Message::Text(subscribe_msg.to_string())).await;
                    
                    while let Some(msg) = read.next().await {
                        if let Ok(Message::Text(text)) = msg {
                            if let Ok(json_msg) = serde_json::from_str::<Value>(&text) {
                                if let Some(result) = json_msg.get("params").and_then(|p| p.get("result")) {
                                    if let Some(tx_str) = result.get("transaction").and_then(|t| t.get(0)).and_then(|t| t.as_str()) {
                                        if let Ok(tx_bytes) = base64::decode(tx_str) {
                                            if let Ok(tx) = bincode::deserialize::<Transaction>(&tx_bytes) {
                                                let sig = tx.signatures[0].to_string();
                                                mempool_cache.insert(sig.clone(), (tx, Instant::now()));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        
        Ok(())
    }

    async fn spawn_block_scanner(&self) -> Result<(), Box<dyn std::error::Error>> {
        let rpc_client = self.rpc_client.clone();
        let mempool_cache = self.mempool_cache.clone();
        
        tokio::spawn(async move {
            let mut last_slot = 0u64;
            
            loop {
                if let Ok(slot) = rpc_client.get_slot().await {
                    if slot > last_slot {
                        let config = RpcBlockConfig {
                            encoding: Some(UiTransactionEncoding::Base64),
                            max_supported_transaction_version: Some(0),
                            rewards: Some(false),
                            commitment: Some(CommitmentConfig::confirmed()),
                            ..Default::default()
                        };
                        
                        if let Ok(block) = rpc_client.get_block_with_config(slot, config).await {
                            if let Some(transactions) = block.transactions {
                                for tx_with_meta in transactions {
                                    if let Some(tx) = tx_with_meta.transaction.decode() {
                                        let sig = tx.signatures[0].to_string();
                                        mempool_cache.insert(sig.clone(), (tx, Instant::now()));
                                    }
                                }
                            }
                        }
                        last_slot = slot;
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
        
        Ok(())
    }

    async fn spawn_intent_analyzer(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mempool_cache = self.mempool_cache.clone();
        let pool_states = self.pool_states.clone();
        let price_cache = self.price_cache.clone();
        let intent_sender = self.intent_sender.clone();
        let metrics = self.metrics.clone();
        let dedup_cache = self.dedup_cache.clone();
        
        let (tx, rx) = channel::bounded::<(String, Transaction, Instant)>(10000);
        
        for _ in 0..num_cpus::get() {
            let rx = rx.clone();
            let pool_states = pool_states.clone();
            let price_cache = price_cache.clone();
            let intent_sender = intent_sender.clone();
            let metrics = metrics.clone();
            let dedup_cache = dedup_cache.clone();
            
            std::thread::spawn(move || {
                while let Ok((sig, transaction, timestamp)) = rx.recv() {
                    let start = Instant::now();
                    
                    {
                        let mut dedup = dedup_cache.lock().unwrap();
                        if dedup.get(&sig).is_some() {
                            continue;
                        }
                        dedup.put(sig.clone(), ());
                    }
                    
                    if let Some(intent) = Self::analyze_transaction(
                        &sig,
                        &transaction,
                        timestamp,
                        &pool_states,
                        &price_cache,
                    ) {
                        if intent.confidence_score > 0.7 && intent.estimated_value > 10.0 {
                            let _ = intent_sender.blocking_send(intent);
                            *metrics.intents_processed.lock().unwrap() += 1;
                            
                            match &intent.mev_type {
                                MEVType::Arbitrage { .. } => {
                                    metrics.mev_opportunities.lock().unwrap()
                                        .entry("arbitrage".to_string())
                                        .and_modify(|e| *e += 1)
                                        .or_insert(1);
                                }
                                MEVType::Sandwich { .. } => {
                                    metrics.mev_opportunities.lock().unwrap()
                                        .entry("sandwich".to_string())
                                        .and_modify(|e| *e += 1)
                                        .or_insert(1);
                                }
                                MEVType::Liquidation { .. } => {
                                    metrics.mev_opportunities.lock().unwrap()
                                        .entry("liquidation".to_string())
                                        .and_modify(|e| *e += 1)
                                        .or_insert(1);
                                }
                                _ => {}
                            }
                            
                            *metrics.total_profit.lock().unwrap() += intent.estimated_value;
                        }
                    }
                    
                    let processing_time = start.elapsed().as_nanos();
                    let mut times = metrics.processing_times.lock().unwrap();
                    times.push_back(processing_time);
                    if times.len() > 10000 {
                        times.pop_front();
                    }
                }
            });
        }
        
        tokio::spawn(async move {
            loop {
                let expired_sigs: Vec<_> = mempool_cache.iter()
                    .filter(|entry| entry.value().1.elapsed() > Duration::from_secs(30))
                    .map(|entry| entry.key().clone())
                    .collect();
                
                for sig in expired_sigs {
                    mempool_cache.remove(&sig);
                }
                
                let batch: Vec<_> = mempool_cache.iter()
                    .take(100)
                    .map(|entry| (entry.key().clone(), entry.value().0.clone(), entry.value().1))
                    .collect();
                
                for (sig, tx, timestamp) in batch {
                    let _ = tx.try_send((sig, tx, timestamp));
                }
                
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
        });
        
        Ok(())
    }

    fn analyze_transaction(
        sig: &str,
        transaction: &Transaction,
        timestamp: Instant,
        pool_states: &Arc<DashMap<Pubkey, PoolInfo>>,
        price_cache: &Arc<DashMap<Pubkey, (f64, u64, Instant)>>,
    ) -> Option<HarvestedIntent> {
        let message = &transaction.message;
        let (priority_fee, compute_units) = Self::extract_compute_budget(&message);
        
        for instruction in message.instructions.iter() {
            let program_id = message.account_keys[instruction.program_id_index as usize];
            
            if let Some(intent_type) = Self::parse_instruction(&instruction, &message.account_keys, &program_id) {
                let accounts: Vec<Pubkey> = instruction.accounts.iter()
                    .map(|&idx| message.account_keys[idx as usize])
                    .collect();
                
                let (mev_type, arbitrage_paths, liquidation_opportunity, sandwich_target, estimated_value, confidence_score) = 
                    Self::detect_mev_opportunity(
                        &intent_type,
                        &accounts,
                        pool_states,
                        price_cache,
                        priority_fee,
                    );
                
                return Some(HarvestedIntent {
                    signature: sig.to_string(),
                    timestamp: timestamp.elapsed().as_secs(),
                    intent_type,
                    program_id,
                    accounts,
                    data: instruction.data.clone(),
                    compute_units,
                    priority_fee,
                    estimated_value,
                    confidence_score,
                    arbitrage_paths,
                    liquidation_opportunity,
                    sandwich_target,
                    mev_type,
                    slot: 0,
                    block_time: 0,
                });
            }
        }
        
        None
    }

    fn extract_compute_budget(message: &solana_sdk::message::Message) -> (u64, u64) {
        let mut priority_fee = 0u64;
        let mut compute_units = 200_000u64;
        
        for instruction in &message.instructions {
            if message.account_keys[instruction.program_id_index as usize] == solana_sdk::compute_budget::id() {
                match instruction.data.first() {
                    Some(&2) if instruction.data.len() >= 5 => {
                        compute_units = u32::from_le_bytes(instruction.data[1..5].try_into().unwrap()) as u64;
                    }
                    Some(&3) if instruction.data.len() >= 9 => {
                        priority_fee = u64::from_le_bytes(instruction.data[1..9].try_into().unwrap());
                    }
                    _ => {}
                }
            }
        }
        
        (priority_fee, compute_units)
    }

    fn parse_instruction(
        instruction: &CompiledInstruction,
        account_keys: &[Pubkey],
        program_id: &Pubkey,
    ) -> Option<IntentType> {
        match *program_id {
            RAYDIUM_V4 => Self::parse_raydium_swap(instruction, account_keys),
            ORCA_WHIRLPOOL => Self::parse_orca_swap(instruction, account_keys),
            JUPITER_V6 => Self::parse_jupiter_swap(instruction, account_keys),
            PUMP_FUN => Self::parse_pump_swap(instruction, account_keys),
            SOLEND_PROGRAM => Self::parse_solend_liquidation(instruction, account_keys),
            _ => None,
        }
    }

    fn parse_raydium_swap(instruction: &CompiledInstruction, account_keys: &[Pubkey]) -> Option<IntentType> {
        if instruction.data.len() >= 17 && instruction.data[0] == 9 {
            let amount_in = u64::from_le_bytes(instruction.data[1..9].try_into().ok()?);
            let min_out = u64::from_le_bytes(instruction.data[9..17].try_into().ok()?);
            
            if instruction.accounts.len() >= 17 {
                return Some(IntentType::Swap {
                    input_mint: account_keys[instruction.accounts[15] as usize],
                    output_mint: account_keys[instruction.accounts[16] as usize],
                    amount: amount_in,
                    min_out,
                });
            }
        }
        None
    }

    fn parse_orca_swap(instruction: &CompiledInstruction, account_keys: &[Pubkey]) -> Option<IntentType> {
        if instruction.data.len() >= 24 && instruction.accounts.len() >= 11 {
            let discriminator = &instruction.data[0..8];
            if discriminator == &[248, 198, 158, 145, 225, 117, 135, 200] {
                let amount = u64::from_le_bytes(instruction.data[8..16].try_into().ok()?);
                let min_out = u64::from_le_bytes(instruction.data[16..24].try_into().ok()?);
                
                return Some(IntentType::Swap {
                    input_mint: account_keys[instruction.accounts[2] as usize],
                    output_mint: account_keys[instruction.accounts[3] as usize],
                    amount,
                    min_out,
                });
            }
        }
        None
    }

    fn parse_jupiter_swap(instruction: &CompiledInstruction, account_keys: &[Pubkey]) -> Option<IntentType> {
        if instruction.data.len() >= 56 && instruction.accounts.len() >= 6 {
            let discriminator = &instruction.data[0..8];
            if discriminator == &[229, 23, 203, 151, 122, 227, 173, 42] {
                let amount = u64::from_le_bytes(instruction.data[16..24].try_into().ok()?);
                let min_out = u64::from_le_bytes(instruction.data[40..48].try_into().ok()?);
                
                return Some(IntentType::Swap {
                    input_mint: account_keys[instruction.accounts[3] as usize],
                    output_mint: account_keys[instruction.accounts[4] as usize],
                    amount,
                    min_out,
                });
            }
        }
        None
    }

    fn parse_pump_swap(instruction: &CompiledInstruction, account_keys: &[Pubkey]) -> Option<IntentType> {
        if instruction.data.len() >= 24 && instruction.accounts.len() >= 8 {
            let discriminator = u64::from_le_bytes(instruction.data[0..8].try_into().ok()?);
            if discriminator == 16927863322537952870u64 {
                let amount = u64::from_le_bytes(instruction.data[8..16].try_into().ok()?);
                let min_out = u64::from_le_bytes(instruction.data[16..24].try_into().ok()?);
                
                return Some(IntentType::Swap {
                    input_mint: account_keys[instruction.accounts[3] as usize],
                    output_mint: account_keys[instruction.accounts[4] as usize],
                    amount,
                    min_out,
                });
            }
        }
        None
    }

    fn parse_solend_liquidation(instruction: &CompiledInstruction, account_keys: &[Pubkey]) -> Option<IntentType> {
        if instruction.data.len() >= 1 && instruction.data[0] == 16 && instruction.accounts.len() >= 13 {
            return Some(IntentType::Liquidate {
                account: account_keys[instruction.accounts[1] as usize],
                debt_mint: account_keys[instruction.accounts[10] as usize],
                collateral_mint: account_keys[instruction.accounts[11] as usize],
            });
        }
        None
    }

    fn detect_mev_opportunity(
        intent_type: &IntentType,
        accounts: &[Pubkey],
        pool_states: &Arc<DashMap<Pubkey, PoolInfo>>,
        price_cache: &Arc<DashMap<Pubkey, (f64, u64, Instant)>>,
        priority_fee: u64,
    ) -> (MEVType, Vec<ArbitragePath>, Option<LiquidationData>, Option<SandwichData>, f64, f64) {
        match intent_type {
            IntentType::Swap { input_mint, output_mint, amount, min_out } => {
                Self::analyze_swap_mev(input_mint, output_mint, *amount, *min_out, pool_states, price_cache, priority_fee)
            }
            IntentType::Liquidate { account, debt_mint, collateral_mint } => {
                Self::analyze_liquidation_mev(account, debt_mint, collateral_mint, price_cache)
            }
            _ => (MEVType::BackRun { parent_profit: 0.0 }, vec![], None, None, 0.0, 0.0),
        }
    }

    fn analyze_swap_mev(
        input_mint: &Pubkey,
        output_mint: &Pubkey,
        amount: u64,
        min_out: u64,
        pool_states: &Arc<DashMap<Pubkey, PoolInfo>>,
        price_cache: &Arc<DashMap<Pubkey, (f64, u64, Instant)>>,
        priority_fee: u64,
    ) -> (MEVType, Vec<ArbitragePath>, Option<LiquidationData>, Option<SandwichData>, f64, f64) {
        let mut arbitrage_paths = vec![];
        let mut best_profit = 0.0;
        let mut confidence = 0.5;
        
        let input_pools: Vec<_> = pool_states.iter()
            .filter(|p| (p.token_a == *input_mint || p.token_b == *input_mint) && 
                       (p.token_a == *output_mint || p.token_b == *output_mint))
            .collect();
        
        let (input_price, output_price) = match (
            price_cache.get(input_mint).map(|p| p.0),
            price_cache.get(output_mint).map(|p| p.0)
        ) {
            (Some(ip), Some(op)) => (ip, op),
            _ => return (MEVType::BackRun { parent_profit: 0.0 }, vec![], None, None, 0.0, 0.0),
        };
        
        for pool1 in &input_pools {
            for pool2 in &input_pools {
                if pool1.key() != pool2.key() && pool1.dex != pool2.dex {
                    let output1 = Self::calculate_output(
                        amount,
                        pool1.reserve_a,
                        pool1.reserve_b,
                        pool1.fee_bps,
                        pool1.token_a == *input_mint,
                    );
                    
                    let output2 = Self::calculate_output(
                        output1,
                        pool2.reserve_a,
                        pool2.reserve_b,
                        pool2.fee_bps,
                        pool2.token_a == *output_mint,
                    );
                    
                    if output2 > amount {
                        let profit_tokens = output2 - amount;
                        let profit_usd = (profit_tokens as f64 / 1e9) * input_price;
                        let gas_cost_usd = (priority_fee as f64 / 1e6) * 0.000025 * 150.0;
                        let net_profit = profit_usd - gas_cost_usd;
                        
                        if net_profit > 20.0 && net_profit > best_profit {
                            best_profit = net_profit;
                            confidence = 0.7 + (net_profit.min(500.0) / 2000.0);
                            
                            arbitrage_paths.push(ArbitragePath {
                                dexes: vec![pool1.dex.clone(), pool2.dex.clone()],
                                pools: vec![*pool1.key(), *pool2.key()],
                                route: vec![*input_mint, *output_mint, *input_mint],
                                amounts: vec![amount, output1, output2],
                                expected_profit: net_profit,
                                gas_estimate: 300_000,
                            });
                        }
                    }
                }
            }
        }
        
        let slippage_bps = if min_out > 0 {
            ((amount - min_out) as f64 / amount as f64 * 10000.0) as u16
        } else {
            0
        };
        
        let sandwich_data = if amount > 1_000_000_000 && slippage_bps > 50 {
            Self::detect_sandwich(amount, min_out, &input_pools, input_price)
        } else {
            None
        };
        
        let mev_type = if !arbitrage_paths.is_empty() {
            MEVType::Arbitrage { 
                profit_bps: (best_profit / (amount as f64 / 1e9 * input_price) * 10000.0) as u16,
                hops: 2,
            }
        } else if sandwich_data.is_some() {
            MEVType::Sandwich { 
                victim_loss: sandwich_data.as_ref().unwrap().expected_profit * 0.4,
                attack_profit: sandwich_data.as_ref().unwrap().expected_profit * 0.6,
            }
        } else {
            MEVType::BackRun { parent_profit: 0.0 }
        };
        
        (mev_type, arbitrage_paths, None, sandwich_data, best_profit, confidence)
    }

    fn calculate_output(amount_in: u64, reserve_in: u64, reserve_out: u64, fee_bps: u16, is_a_to_b: bool) -> u64 {
        if amount_in == 0 || reserve_in == 0 || reserve_out == 0 {
            return 0;
        }
        
        let fee_multiplier = 10000u64 - fee_bps as u64;
        let amount_in_with_fee = (amount_in as u128 * fee_multiplier as u128) / 10000u128;
        let numerator = amount_in_with_fee * reserve_out as u128;
        let denominator = reserve_in as u128 + amount_in_with_fee;
        
        (numerator / denominator) as u64
    }

    fn detect_sandwich(
        victim_amount: u64,
        min_out: u64,
        pools: &[dashmap::mapref::multiple::RefMulti<Pubkey, PoolInfo>],
        token_price: f64,
    ) -> Option<SandwichData> {
        if let Some(target_pool) = pools.first() {
            let pool = target_pool.value();
            let pool_tvl = (pool.reserve_a as f64 + pool.reserve_b as f64) / 1e9 * token_price;
            let impact = (victim_amount as f64) / (pool.reserve_a.min(pool.reserve_b) as f64);
            
            if impact > 0.01 && pool_tvl > 100_000.0 {
                let optimal_front = ((victim_amount as f64) * impact.sqrt() * 1.5) as u64;
                let front_output = Self::calculate_output(optimal_front, pool.reserve_a, pool.reserve_b, pool.fee_bps, true);
                
                let new_reserve_a = pool.reserve_a + optimal_front;
                let new_reserve_b = pool.reserve_b - front_output;
                let victim_output = Self::calculate_output(victim_amount, new_reserve_a, new_reserve_b, pool.fee_bps, true);
                
                let final_reserve_a = new_reserve_a + victim_amount;
                let final_reserve_b = new_reserve_b - victim_output;
                let back_output = Self::calculate_output(front_output, final_reserve_b, final_reserve_a, pool.fee_bps, false);
                
                if back_output > optimal_front {
                    let profit = back_output - optimal_front;
                    let profit_usd = (profit as f64 / 1e9) * token_price;
                    
                    if profit_usd > 50.0 {
                        return Some(SandwichData {
                            victim_tx: String::new(),
                            victim_amount,
                            front_run_amount: optimal_front,
                            back_run_amount: front_output,
                            expected_profit: profit_usd,
                            pool_tvl,
                        });
                    }
                }
            }
        }
        None
    }

    fn analyze_liquidation_mev(
        account: &Pubkey,
        debt_mint: &Pubkey,
        collateral_mint: &Pubkey,
        price_cache: &Arc<DashMap<Pubkey, (f64, u64, Instant)>>,
    ) -> (MEVType, Vec<ArbitragePath>, Option<LiquidationData>, Option<SandwichData>, f64, f64) {
        let (debt_price, collateral_price) = match (
            price_cache.get(debt_mint).map(|p| p.0),
            price_cache.get(collateral_mint).map(|p| p.0)
        ) {
            (Some(d), Some(c)) => (d, c),
            _ => return (MEVType::BackRun { parent_profit: 0.0 }, vec![], None, None, 0.0, 0.0),
        };
        
        let mock_debt_amount = 50_000_000_000u64;
        let mock_collateral_amount = 40_000_000_000u64;
        
        let debt_value = (mock_debt_amount as f64 / 1e9) * debt_price;
        let collateral_value = (mock_collateral_amount as f64 / 1e9) * collateral_price;
        let health_ratio = collateral_value * 0.85 / debt_value;
        
        if health_ratio < 1.02 {
            let max_liquidation = (debt_value * 0.5) as u64;
            let liquidation_bonus = 0.05;
            let profit_estimate = debt_value * liquidation_bonus;
            
            let liquidation_data = LiquidationData {
                account: *account,
                collateral_value,
                debt_value,
                health_ratio,
                max_liquidation,
                profit_estimate,
            };
            
            let confidence = 0.85 + (1.02 - health_ratio).min(0.1);
            
            (
                MEVType::Liquidation { 
                    ltv_ratio: debt_value / collateral_value,
                    bonus_rate: liquidation_bonus,
                },
                vec![],
                Some(liquidation_data),
                None,
                profit_estimate,
                confidence,
            )
        } else {
            (MEVType::BackRun { parent_profit: 0.0 }, vec![], None, None, 0.0, 0.0)
        }
    }

    async fn spawn_price_updater(&self) -> Result<(), Box<dyn std::error::Error>> {
        let rpc_client = self.rpc_client.clone();
        let price_cache = self.price_cache.clone();
        
        let price_accounts = vec![
            (solana_sdk::pubkey!("GVXRSBjFk6e6J3NbVPXohDJetcTjaeeuykUpbQF8UoMU"), "SOL"),
            (solana_sdk::pubkey!("JBu1AL4obBcCMqKBBxhpWCNUt136ijcuMZLFvTP7iWdB"), "ETH"),
            (solana_sdk::pubkey!("Gnt27xtC473ZT2Mw5u8wZ68Z3gULkSTb5DuxJy7eJotD"), "USDC"),
            (solana_sdk::pubkey!("H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG"), "USDT"),
            (solana_sdk::pubkey!("Dprgkn2LTqZBmnH1MB2cVMLd7uPxX6SaVFEUTWuiKq3b"), "BTC"),
        ];
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(250));
            
            loop {
                interval.tick().await;
                
                let mut futures = FuturesUnordered::new();
                
                for (price_account, _symbol) in &price_accounts {
                    let rpc = rpc_client.clone();
                    let cache = price_cache.clone();
                    let account = *price_account;
                    
                    futures.push(async move {
                        if let Ok(account_data) = rpc.get_account_data(&account).await {
                            if account_data.len() >= std::mem::size_of::<PythPriceAccount>() {
                                if let Ok(price_data) = bytemuck::try_from_bytes::<PythPriceAccount>(&account_data) {
                                    if price_data.agg.status == 1 {
                                        let price = (price_data.agg.price as f64) * 10f64.powi(price_data.exponent);
                                        let confidence = price_data.agg.conf as f64 * 10f64.powi(price_data.exponent);
                                        
                                        if price > 0.0 && confidence / price < 0.02 {
                                            cache.insert(account, (price, price_data.agg.pub_slot, Instant::now()));
                                        }
                                    }
                                }
                            }
                        }
                    });
                }
                
                while futures.next().await.is_some() {}
                
                let expired: Vec<_> = price_cache.iter()
                    .filter(|entry| entry.value().2.elapsed() > Duration::from_secs(60))
                    .map(|entry| *entry.key())
                    .collect();
                
                for key in expired {
                    price_cache.remove(&key);
                }
            }
        });
        
        Ok(())
    }

    async fn spawn_pool_updater(&self) -> Result<(), Box<dyn std::error::Error>> {
        let rpc_client = self.rpc_client.clone();
        let pool_states = self.pool_states.clone();
        let price_cache = self.price_cache.clone();
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(500));
            
            loop {
                interval.tick().await;
                
                let pools_to_update: Vec<_> = pool_states.iter()
                    .filter(|p| p.value().last_update.elapsed() > Duration::from_millis(400))
                    .map(|p| (*p.key(), p.value().clone()))
                    .collect();
                
                let mut futures = FuturesUnordered::new();
                
                for (pool_key, pool_info) in pools_to_update {
                    let rpc = rpc_client.clone();
                    let pools = pool_states.clone();
                    let prices = price_cache.clone();
                    
                    futures.push(async move {
                        if let Ok(account) = rpc.get_account(&pool_key).await {
                            match pool_info.dex.as_str() {
                                "raydium" => {
                                    if let Ok(pool_state) = bytemuck::try_from_bytes::<RaydiumPoolState>(&account.data) {
                                        if pool_state.status == 1 {
                                            if let Ok(coin_balance) = rpc.get_token_account_balance(&pool_state.token_coin).await {
                                                if let Ok(pc_balance) = rpc.get_token_account_balance(&pool_state.token_pc).await {
                                                    if let Some(mut pool) = pools.get_mut(&pool_key) {
                                                        pool.reserve_a = coin_balance.amount.parse().unwrap_or(0);
                                                        pool.reserve_b = pc_balance.amount.parse().unwrap_or(0);
                                                        
                                                        if let (Some(price_a), Some(price_b)) = (
                                                            prices.get(&pool.token_a).map(|p| p.0),
                                                            prices.get(&pool.token_b).map(|p| p.0)
                                                        ) {
                                                            pool.tvl = (pool.reserve_a as f64 / 1e9 * price_a) + 
                                                                      (pool.reserve_b as f64 / 1e9 * price_b);
                                                        }
                                                        
                                                        pool.last_update = Instant::now();
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                                "orca" => {
                                    if account.data.len() >= 324 {
                                        let liquidity = u128::from_le_bytes(account.data[269..285].try_into().unwrap_or([0; 16]));
                                        let sqrt_price = u128::from_le_bytes(account.data[253..269].try_into().unwrap_or([0; 16]));
                                        
                                        if let Some(mut pool) = pools.get_mut(&pool_key) {
                                            let price_ratio = (sqrt_price as f64 / 2u128.pow(64) as f64).powi(2);
                                            pool.reserve_a = (liquidity as f64 / (1.0 + price_ratio).sqrt()) as u64;
                                            pool.reserve_b = (pool.reserve_a as f64 * price_ratio) as u64;
                                            pool.last_update = Instant::now();
                                        }
                                    }
                                }
                                _ => {}
                            }
                        }
                    });
                }
                
                while futures.next().await.is_some() {}
            }
        });
        
        Ok(())
    }

    async fn spawn_metrics_reporter(&self) -> Result<(), Box<dyn std::error::Error>> {
        let metrics = self.metrics.clone();
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(10));
            let start_time = Instant::now();
            
            loop {
                interval.tick().await;
                
                let intents = *metrics.intents_processed.lock().unwrap();
                let mev_opps = metrics.mev_opportunities.lock().unwrap().clone();
                let profit = *metrics.total_profit.lock().unwrap();
                let times = metrics.processing_times.lock().unwrap();
                
                let (avg_time, p99_time) = if !times.is_empty() {
                    let mut sorted: Vec<_> = times.iter().cloned().collect();
                    sorted.sort_unstable();
                    let avg = sorted.iter().sum::<u128>() / sorted.len() as u128;
                    let p99_idx = (sorted.len() as f64 * 0.99) as usize;
                    (avg, sorted.get(p99_idx).cloned().unwrap_or(0))
                } else {
                    (0, 0)
                };
                
                let runtime = start_time.elapsed().as_secs();
                let tps = if runtime > 0 { intents / runtime } else { 0 };
                
                println!("\n╔═══════════════════════════════════════════════════════════════╗");
                println!("║               PRE-EXECUTION INTENT HARVESTER                   ║");
                println!("╠═══════════════════════════════════════════════════════════════╣");
                println!("║ Runtime: {:>10}s | TPS: {:>10} | Profit: ${:>10.2} ║", runtime, tps, profit);
                println!("║ Total Intents: {:>15} | Avg Process: {:>10} ns     ║", intents, avg_time);
                println!("║ P99 Latency: {:>12} ns | Pool States: {:>10}      ║", p99_time, metrics.intents_processed.lock().unwrap());
                println!("╟───────────────────────────────────────────────────────────────╢");
                println!("║ MEV Opportunities:                                            ║");
                for (mev_type, count) in mev_opps {
                    println!("║   {:.<20} {:>37} ║", mev_type, count);
                }
                println!("╚═══════════════════════════════════════════════════════════════╝");
            }
        });
        
        Ok(())
    }
}
