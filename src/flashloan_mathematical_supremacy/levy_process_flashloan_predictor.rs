// --- Risk, RPC pool, and guardrails ---
#[derive(Debug, Clone, Default)]
pub struct RiskState {
    pub per_mint_position: HashMap<Pubkey, u128>,
    pub daily_pnl: i128,
    pub max_drawdown: i128,
    pub cooldown_until: HashMap<(Pubkey, Pubkey), i64>, // (leader, mint) -> unix secs
}

#[allow(clippy::too_many_arguments)]
pub async fn execute_atomic_arbitrage(
    rpc: &AsyncRpcClient,
    payer: &Keypair,
    alts: &[Pubkey],
    flash_loan_ixs: &[Instruction], // borrow -> route -> repay
    cu_limit: u32,
    base_tip_microlamports: u64,
    leader: Pubkey,
    leader_model: &LeaderModel,
) -> anyhow::Result<()> {
    // Fresh blockhash
    let blockhash = rpc.get_latest_blockhash().await?;
    // Tip selection per leader model
    // Build a lightweight LeaderModel view
    let model = leader_model;
    let tip = choose_tip(&leader, model, base_tip_microlamports);

    // Build v0 tx (ALTs not yet used in builder; pass empty internally)
    let tx = LevyProcessPredictor::build_tx(
        payer,
        flash_loan_ixs.to_vec(),
        blockhash,
        cu_limit,
        tip,
    )?;

    // 1) Simulate (exact accounts)
    let sim = rpc.simulate_transaction(&tx).await?;
    if let Some(err) = sim.value.err.clone() {
        record_exec(false, sim.value.units_consumed.unwrap_or(0), tip, 0.0, &Pubkey::default(), &leader);
        anyhow::bail!("simulation failed: {:?}", err);
    }
    let cu_used = sim.value.units_consumed.unwrap_or(0);

    // 2) Send (Jito bundle path could be attempted here; fallback to RPC send)
    let sig = rpc.send_transaction(&tx).await?;

    // 3) Confirm fast path
    let _ = rpc.confirm_transaction(&sig).await?;
    record_exec(true, cu_used, tip, 0.0, &Pubkey::default(), &leader);
    Ok(())
}

#[cfg(test)]
mod replay {
    use super::*;
    use rand::{Rng, SeedableRng};
    use rand_xoshiro::Xoshiro256Plus;
use solana_client::nonblocking::rpc_client::RpcClient as AsyncRpcClient;

    #[test]
    fn stable_estimator_no_nan() {
        let mut rng = Xoshiro256Plus::seed_from_u64(42);
        let rets: Vec<f64> = (0..10_000).map(|_| {
            (rng.gen::<f64>() - 0.5) * 0.01
        }).collect();
        let (a, b) = LevyProcessPredictor::mcculloch_alpha_beta(&rets);
        assert!(a.is_finite() && b.is_finite());
    }
}

#[inline]
fn allow_trade(risk: &RiskState, mint: &Pubkey, add: u128, caps: (u128, u128)) -> bool {
    let cur = *risk.per_mint_position.get(mint).unwrap_or(&0);
    cur.saturating_add(add) <= caps.0 && (risk.daily_pnl - risk.max_drawdown as i128) > -(caps.1 as i128)
}

#[derive(Debug, Clone)]
pub struct RpcNode { pub url: String, pub rtt_ms: f64, pub slot: u64, pub healthy: bool }

#[derive(Debug, Clone, Default)]
pub struct RpcPool { pub nodes: Vec<RpcNode> }

fn best_rpc(pool: &RpcPool) -> &str {
    pool.nodes.iter()
        .filter(|n| n.healthy)
        .min_by(|a,b| a.rtt_ms.partial_cmp(&b.rtt_ms).unwrap_or(std::cmp::Ordering::Equal))
        .map(|n| n.url.as_str())
        .unwrap_or("https://api.mainnet-beta.solana.com")
}

// --- Unified DEX routing primitives ---
#[derive(Clone, Debug)]
pub struct Quote {
    pub out: u64,
    pub pool_fee_bps: u64,
    pub route_name: &'static str,
}

pub trait DexRoute: Send + Sync {
    fn quote(&self, in_mint: Pubkey, out_mint: Pubkey, qty: u64) -> Option<Quote>;
    fn build_swap_ix(&self, payer: &Pubkey, qty: u64, min_out: u64) -> Instruction;
}

#[derive(Clone)]
pub struct PhoenixAdapter {
    pub program_id: Pubkey,
    pub market: Pubkey,
}

impl DexRoute for PhoenixAdapter {
    fn quote(&self, _in_mint: Pubkey, _out_mint: Pubkey, qty: u64) -> Option<Quote> {
        // Placeholder: 4 bps pool fee, 1:1 price
        let fee_bps = 4;
        let fee = qty.saturating_mul(fee_bps) / 10_000;
        Some(Quote { out: qty.saturating_sub(fee), pool_fee_bps: fee_bps, route_name: "phoenix" })
    }
    fn build_swap_ix(&self, payer: &Pubkey, qty: u64, min_out: u64) -> Instruction {
        // Placeholder IX; replace with Phoenix program-specific instruction builder
        Instruction {
            program_id: self.program_id,
            accounts: vec![AccountMeta::new(*payer, true), AccountMeta::new(self.market, false)],
            data: vec![],
        }
    }
}

#[derive(Clone)]
pub struct OpenBookV2Adapter { pub program_id: Pubkey, pub market: Pubkey }
impl DexRoute for OpenBookV2Adapter {
    fn quote(&self, _in_mint: Pubkey, _out_mint: Pubkey, qty: u64) -> Option<Quote> {
        let fee_bps = 22; // 2.2 bps illustrative
        let fee = qty.saturating_mul(fee_bps) / 10_000;
        Some(Quote { out: qty.saturating_sub(fee), pool_fee_bps: fee_bps, route_name: "openbook_v2" })
    }
    fn build_swap_ix(&self, payer: &Pubkey, _qty: u64, _min_out: u64) -> Instruction {
        Instruction { program_id: self.program_id, accounts: vec![AccountMeta::new(*payer, true), AccountMeta::new(self.market, false)], data: vec![] }
    }
}

#[derive(Clone)]
pub struct RaydiumClmmAdapter { pub program_id: Pubkey, pub pool: Pubkey }
impl DexRoute for RaydiumClmmAdapter {
    fn quote(&self, _in_mint: Pubkey, _out_mint: Pubkey, qty: u64) -> Option<Quote> {
        let fee_bps = 30; // 3 bps illustrative
        let fee = qty.saturating_mul(fee_bps) / 10_000;
        Some(Quote { out: qty.saturating_sub(fee), pool_fee_bps: fee_bps, route_name: "raydium_clmm" })
    }
    fn build_swap_ix(&self, payer: &Pubkey, _qty: u64, _min_out: u64) -> Instruction {
        Instruction { program_id: self.program_id, accounts: vec![AccountMeta::new(*payer, true), AccountMeta::new(self.pool, false)], data: vec![] }
    }
}

#[derive(Clone)]
pub struct OrcaWhirlpoolAdapter { pub program_id: Pubkey, pub pool: Pubkey }
impl DexRoute for OrcaWhirlpoolAdapter {
    fn quote(&self, _in_mint: Pubkey, _out_mint: Pubkey, qty: u64) -> Option<Quote> {
        let fee_bps = 60; // 6 bps illustrative
        let fee = qty.saturating_mul(fee_bps) / 10_000;
        Some(Quote { out: qty.saturating_sub(fee), pool_fee_bps: fee_bps, route_name: "orca_whirlpool" })
    }
    fn build_swap_ix(&self, payer: &Pubkey, _qty: u64, _min_out: u64) -> Instruction {
        Instruction { program_id: self.program_id, accounts: vec![AccountMeta::new(*payer, true), AccountMeta::new(self.pool, false)], data: vec![] }
    }
}

#[inline]
fn required_min_profit_lamports(
    flash_fee_bps: u64,
    pool_fee_bps: u64,
    cu_price: u64,
    cu_used: u32,
    rent: u64,
    safety_bps: u64,
    notional: u64,
) -> u64 {
    let fees_bps = flash_fee_bps.saturating_add(pool_fee_bps).saturating_add(safety_bps);
    let fee = notional.saturating_mul(fees_bps) / 10_000;
    fee + cu_price.saturating_mul(cu_used as u64) + rent
}

#[inline]
fn min_out_with_slip_guard(expected_out: u64, realized_vol_bps: u64, max_slip_bps: u64) -> Option<u64> {
    // Error band from realized volatility; guard by MAX_SLIPPAGE_BPS
    let band_bps = realized_vol_bps.min(max_slip_bps);
    if band_bps >= 10_000 { return None; }
    let discount = expected_out.saturating_mul(band_bps) / 10_000;
    Some(expected_out.saturating_sub(discount))
}
use anchor_client::solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use anchor_lang::prelude::*;
use arrayref::array_ref;
use rayon::prelude::*;
use rust_decimal::prelude::*;
use serum_dex::state::{Market, MarketState};
use solana_client::{
    rpc_client::RpcClient,
    nonblocking::pubsub_client::PubsubClient,
};
use tokio_stream::StreamExt;
use crossbeam::channel::{unbounded, Receiver};
use solana_program::{
    clock::Clock,
    instruction::{AccountMeta, Instruction},
    program_pack::Pack,
    system_program,
};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    message::{v0, VersionedMessage},
    transaction::VersionedTransaction,
};
use solana_sdk::address_lookup_table_account::AddressLookupTableAccount;
use spl_token::state::Account as TokenAccount;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
    str::FromStr,
};
use parking_lot::{Mutex, RwLock};
use once_cell::sync::Lazy;
use arc_swap::ArcSwap;
use tracing::{info, warn, error, instrument};
use metrics::{counter, gauge, histogram};
use rand::{Rng, SeedableRng};
use rand_xoshiro::Xoshiro256Plus;

static SERUM_V3: Lazy<Pubkey> = Lazy::new(|| Pubkey::from_str("9xQeWvG816bUx9EPfDdCkEo7arBgG4oMaw9uRc8rkyMt").unwrap());
#[cfg(feature = "openbook_v2")]
static OPENBOOK_V2: Lazy<Pubkey> = Lazy::new(|| Pubkey::from_str("srmqPvWfVZF3GQ8UC5QW4LxF25r61x6a4a9L7cVhPwC").unwrap());
#[cfg(feature = "phoenix")]
static PHOENIX: Lazy<Pubkey> = Lazy::new(|| Pubkey::from_str("PhoeNiXZ8bq71sC9s8uQDnLMp7stY8nX1WPU6GqsgQv").unwrap());

const LEVY_ALPHA: f64 = 1.7;
const LEVY_BETA: f64 = 0.5;
const LEVY_GAMMA: f64 = 0.01;
const LEVY_DELTA: f64 = 0.0;
const JUMP_THRESHOLD: f64 = 0.015;
const MIN_PROFIT_BPS: u64 = 50;
const MAX_SLIPPAGE_BPS: u64 = 30;
const CONFIDENCE_THRESHOLD: f64 = 0.85;
const MAX_POSITION_SIZE: u64 = 100_000_000_000;
const COOLDOWN_MS: u64 = 100;

#[derive(Clone, Debug)]
pub struct LevyParameters {
    alpha: f64,
    beta: f64,
    gamma: f64,
    delta: f64,
    jump_intensity: f64,
    drift: f64,
    volatility: f64,
}

#[derive(Clone, Debug)]
pub struct MarketSnapshot {
    // Client-side wallclock for observability (not on-chain Clock)
    timestamp: u64,
    slot: u64,
    bid: f64,
    ask: f64,
    bid_size: f64,
    ask_size: f64,
    last_trade: f64,
    volume: f64,
    spread: f64,
    imbalance: f64,
}

#[derive(Clone, Debug)]
pub struct FlashLoanOpportunity {
    market_a: Pubkey,
    market_b: Pubkey,
    token_mint: Pubkey,
    entry_price: f64,
    exit_price: f64,
    size: u64,
    expected_profit: f64,
    confidence: f64,
    predicted_jump: f64,
    execution_window: u64,
}

pub struct LevyProcessPredictor {
    rpc_client: Arc<RpcClient>,
    keypair: Arc<Keypair>,
    market_cache: Arc<RwLock<HashMap<Pubkey, MarketState>>>,
    price_history: Arc<RwLock<HashMap<Pubkey, VecDeque<MarketSnapshot>>>>,
    // Lock-free latest snapshot map for hot readers
    latest_snapshots: Arc<ArcSwap<Arc<HashMap<Pubkey, MarketSnapshot>>>>,
    levy_params: Arc<RwLock<HashMap<Pubkey, LevyParameters>>>,
    pending_opportunities: Arc<Mutex<Vec<FlashLoanOpportunity>>>,
    last_execution: Arc<RwLock<HashMap<Pubkey, Instant>>>,
    leader_model: Arc<RwLock<HashMap<Pubkey, LeaderStats>>>,
    risk_state: Arc<RwLock<RiskState>>,
    rpc_pool: Arc<RwLock<RpcPool>>,
}

#[derive(Clone, Copy, Debug, Default)]
struct LeaderStats { inclusion: f64, p95_tip: u64 }

#[derive(Clone, Debug, Default)]
struct LeaderModel { by_leader: HashMap<Pubkey, LeaderStats> }

fn choose_tip(leader: &Pubkey, model: &LeaderModel, base: u64) -> u64 {
    if let Some(s) = model.by_leader.get(leader) {
        let mult = (1.0 + (1.0 - s.inclusion)).ceil() as u64;
        base.max(s.p95_tip).saturating_mul(mult)
    } else { base }
}

struct MarketStreams {
    updates_rx: Receiver<(Pubkey, Pubkey, Vec<u8>, u64)>,
}

impl MarketStreams {
    fn recv(&self) -> Option<(Pubkey, Pubkey, Vec<u8>, u64)> {
        self.updates_rx.recv().ok()
    }
}

impl LevyProcessPredictor {
    pub fn new(rpc_url: &str, keypair: Keypair) -> Self {
        Self {
            rpc_client: Arc::new(RpcClient::new_with_commitment(
                rpc_url.to_string(),
                CommitmentConfig::processed(),
            )),
            keypair: Arc::new(keypair),
            market_cache: Arc::new(RwLock::new(HashMap::new())),
            price_history: Arc::new(RwLock::new(HashMap::new())),
            latest_snapshots: Arc::new(ArcSwap::from_pointee(Arc::new(HashMap::new()))),
            levy_params: Arc::new(RwLock::new(HashMap::new())),
            pending_opportunities: Arc::new(Mutex::new(Vec::new())),
            last_execution: Arc::new(RwLock::new(HashMap::new())),
            leader_model: Arc::new(RwLock::new(HashMap::new())),
            risk_state: Arc::new(RwLock::new(RiskState::default())),
            rpc_pool: Arc::new(RwLock::new(RpcPool::default())),
        }
    }

    pub async fn run(&self) {
        let update_handle = self.spawn_market_updater();
        let predictor_handle = self.spawn_levy_predictor();
        let executor_handle = self.spawn_flashloan_executor();

        tokio::select! {
            _ = update_handle => {},
            _ = predictor_handle => {},
            _ = executor_handle => {},
        }
    }

    fn spawn_market_updater(&self) -> tokio::task::JoinHandle<()> {
        let rpc_client = self.rpc_client.clone();
        let market_cache = self.market_cache.clone();
        let price_history = self.price_history.clone();
        let latest = self.latest_snapshots.clone();

        tokio::spawn(async move {
            // Prefer websocket subscriptions; fallback to polling if WS fails
            let markets = vec![
                "7dLVkUfBVfCGkFhSXDCq1ukM9usathSgS716t643iFGF",
                "8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYwn1XTh6",
                "DZjbn4XC8qoHKikZqzmhemykVzmossoayV9ffbsUqxVj",
                "HWHvQhFmJB3NUcu1aihKmrKegfVxBEHzwVX6yZCKEsi1",
            ];

            let pubkeys: Vec<Pubkey> = markets.iter().filter_map(|s| s.parse().ok()).collect();
            let streams = Self::subscribe_markets_ws("wss://api.mainnet-beta.solana.com/", pubkeys).await.ok();

            if let Some(streams) = streams {
                // WS path: process updates via channel (no locks across await)
                loop {
                    if let Some((m, owner, data, slot)) = streams.recv() {
                        // Parse Serum v3 safely (no blind slicing) with owner validation
                        if let Some(market_state) = Self::parse_market_by_owner(&owner, &data) {
                            // Short critical sections, no awaits inside
                            {
                                let mut cache = market_cache.write();
                                cache.insert(m, market_state.inner);
                            }
                            let snapshot = Self::extract_market_snapshot(&market_state.inner, slot);
                            {
                                let mut history = price_history.write();
                                let mh = history.entry(m).or_insert_with(|| VecDeque::with_capacity(1000));
                                mh.push_back(snapshot.clone());
                                if mh.len() > 1000 { mh.pop_front(); }
                            }
                            // Update lock-free latest snapshots via ArcSwap clone-on-write
                            let current = latest.load();
                            let mut new_map: HashMap<Pubkey, MarketSnapshot> = (**current).clone();
                            new_map.insert(m, snapshot);
                            latest.store(Arc::new(new_map));
                        }
                    } else {
                        // channel closed; break to fallback
                        break;
                    }
                }
            }

            // Fallback polling if WS not available
            loop {
                for market_str in &markets {
                    if let Ok(market_pubkey) = market_str.parse::<Pubkey>() {
                        if let Ok(account) = rpc_client.get_account(&market_pubkey) {
                            if let Some(market_state) = Self::parse_market_by_owner(&account.owner, &account.data) {
                                {
                                    let mut cache = market_cache.write();
                                    cache.insert(market_pubkey, market_state.inner);
                                }
                                let snapshot = Self::extract_market_snapshot(&market_state.inner, 0);
                                {
                                    let mut history = price_history.write();
                                    let market_history = history.entry(market_pubkey).or_insert_with(|| VecDeque::with_capacity(1000));
                                    market_history.push_back(snapshot.clone());
                                    if market_history.len() > 1000 { market_history.pop_front(); }
                                }
                                // Update ArcSwap
                                let current = latest.load();
                                let mut new_map: HashMap<Pubkey, MarketSnapshot> = (**current).clone();
                                new_map.insert(market_pubkey, snapshot);
                                latest.store(Arc::new(new_map));
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
    }

    async fn subscribe_markets_ws(rpc_ws_url: &str, markets: Vec<Pubkey>) -> anyhow::Result<MarketStreams> {
        use solana_account_decoder::UiAccountEncoding;
        use solana_client::rpc_config::RpcAccountInfoConfig;
        let (tx, rx) = unbounded();
        let client = PubsubClient::new(rpc_ws_url).await?;
        for m in markets {
            let tx = tx.clone();
            let (_sub, mut stream) = client.account_subscribe(
                &m,
                Some(RpcAccountInfoConfig {
                    commitment: Some(CommitmentConfig::processed()),
                    encoding: Some(UiAccountEncoding::Base64),
                    data_slice: None,
                    min_context_slot: None,
                }),
            ).await?;

            tokio::spawn(async move {
                while let Some(update) = stream.next().await {
                    if let Ok(acc) = update.value.decode() {
                        let slot = update.context.slot;
                        let _ = tx.send((m, acc.owner, acc.data, slot));
                    }
                }
            });
        }
        Ok(MarketStreams { updates_rx: rx })
    }

    fn spawn_levy_predictor(&self) -> tokio::task::JoinHandle<()> {
        let price_history = self.price_history.clone();
        let levy_params = self.levy_params.clone();
        let pending_opportunities = self.pending_opportunities.clone();
        let market_cache = self.market_cache.clone();

        tokio::spawn(async move {
            loop {
                let markets: Vec<Pubkey> = {
                    let history = price_history.read();
                    history.keys().cloned().collect()
                };

                for i in 0..markets.len() {
                    for j in i+1..markets.len() {
                        let market_a = markets[i];
                        let market_b = markets[j];

                        if let Some(opportunity) = Self::detect_levy_opportunity(
                            &price_history,
                            &levy_params,
                            &market_cache,
                            market_a,
                            market_b,
                        ) {
                            let mut opportunities = pending_opportunities.lock();
                            if opportunities.len() < 50 {
                                opportunities.push(opportunity);
                            }
                        }
                    }
                }

                Self::update_levy_parameters(&price_history, &levy_params);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    }

    fn spawn_flashloan_executor(&self) -> tokio::task::JoinHandle<()> {
        let rpc_client = self.rpc_client.clone();
        let keypair = self.keypair.clone();
        let pending_opportunities = self.pending_opportunities.clone();
        let last_execution = self.last_execution.clone();

        tokio::spawn(async move {
            loop {
                let opportunity = {
                    let mut opportunities = pending_opportunities.lock();
                    opportunities.sort_by(|a, b| {
                        b.expected_profit.partial_cmp(&a.expected_profit).unwrap()
                    });
                    opportunities.pop()
                };

                if let Some(opp) = opportunity {
                    let should_execute = {
                        let executions = last_execution.read();
                        if let Some(last_time) = executions.get(&opp.token_mint) {
                            last_time.elapsed().as_millis() > COOLDOWN_MS as u128
                        } else {
                            true
                        }
                    };

                    if should_execute && opp.confidence > CONFIDENCE_THRESHOLD {
                        match Self::execute_flashloan(
                            &rpc_client,
                            &keypair,
                            &opp,
                        ).await {
                            Ok(_) => {
                                let mut executions = last_execution.write();
                                executions.insert(opp.token_mint, Instant::now());
                            },
                            Err(_) => {}
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    }

    fn extract_market_snapshot(market: &MarketState, slot: u64) -> MarketSnapshot {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let bid = market.native_pc_bid_quote() as f64;
        let ask = market.native_pc_ask_quote() as f64;
        let spread = (ask - bid) / bid;
        let mid = (bid + ask) / 2.0;

        MarketSnapshot {
            timestamp,
            slot,
            bid,
            ask,
            bid_size: market.native_coin_bid() as f64,
            ask_size: market.native_coin_ask() as f64,
            last_trade: mid,
            volume: 0.0,
            spread,
            imbalance: (market.native_coin_bid() as f64 - market.native_coin_ask() as f64).abs(),
        }
    }

    fn detect_levy_opportunity(
        price_history: &Arc<RwLock<HashMap<Pubkey, VecDeque<MarketSnapshot>>>>,
        levy_params: &Arc<RwLock<HashMap<Pubkey, LevyParameters>>>,
        market_cache: &Arc<RwLock<HashMap<Pubkey, MarketState>>>,
        market_a: Pubkey,
        market_b: Pubkey,
    ) -> Option<FlashLoanOpportunity> {
        let history = price_history.read();
        let params = levy_params.read();
        let cache = market_cache.read();

        let history_a = history.get(&market_a)?;
        let history_b = history.get(&market_b)?;

        if history_a.len() < 100 || history_b.len() < 100 {
            return None;
        }

        let levy_a = params.get(&market_a).cloned().unwrap_or_else(|| {
            Self::estimate_levy_parameters(history_a)
        });
        let levy_b = params.get(&market_b).cloned().unwrap_or_else(|| {
            Self::estimate_levy_parameters(history_b)
        });

        // Build recent log return series with microstructure filter
        let to_returns = |h: &VecDeque<MarketSnapshot>| -> Vec<f64> {
            let mut r = Vec::with_capacity(h.len().saturating_sub(1));
            for w in h.windows(2) {
                if w[1].spread <= 0.0 { continue; }
                if (w[1].last_trade - w[0].last_trade).abs() < f64::EPSILON { continue; }
                let rr = (w[1].last_trade / w[0].last_trade).ln();
                if rr.is_finite() { r.push(rr); }
            }
            r
        };
        let ra = to_returns(history_a);
        let rb = to_returns(history_b);

        // Jump probabilities using bipower variation vs realized variance
        let jump_prob_a = Self::jump_probability(&ra);
        let jump_prob_b = Self::jump_probability(&rb);

        let price_a = history_a.back()?.last_trade;
        let price_b = history_b.back()?.last_trade;

        let predicted_jump_a = Self::predict_levy_jump(&levy_a, price_a);
        let predicted_jump_b = Self::predict_levy_jump(&levy_b, price_b);

        // Robust correlation on synchronized tail
        let cross_correlation = Self::robust_corr(&ra, &rb);

        if (jump_prob_a > 0.7 || jump_prob_b > 0.7) && cross_correlation.abs() < 0.3 {
            // Kelly-based size with impact and volatility guard
            let edge_per_unit = (predicted_jump_a.abs() + predicted_jump_b.abs()) / ((price_a + price_b).max(1e-9));
            let var_per_unit = (levy_a.volatility.powi(2) + levy_b.volatility.powi(2)) / 2.0;
            let depth_qty = history_a.back()?.bid_size.min(history_b.back()?.ask_size);
            let mid = (price_a + price_b) / 2.0;
            let slip_slope = ((history_a.back()?.spread + history_b.back()?.spread) / 2.0).abs().max(1e-9) / mid.max(1e-9);
            let size = Self::optimal_size(edge_per_unit, var_per_unit, MAX_POSITION_SIZE, depth_qty, slip_slope);

            // Confidence construction: blend jump prob, decorrelation, sim success, and recent slippage error
            let sim_success_rate = 0.8; // TODO: wire real simulation rate
            let slip_error_norm = 0.0;   // TODO: wire realized slip error EWMA
            let confidence = Self::combine_confidence(jump_prob_a.max(jump_prob_b), 1.0 - cross_correlation.abs(), sim_success_rate, 1.0 - slip_error_norm);
            let expected_profit = (predicted_jump_a.abs() + predicted_jump_b.abs()) * size as f64;

            if expected_profit > (size as f64 * MIN_PROFIT_BPS as f64 / 10000.0) {
                return Some(FlashLoanOpportunity {
                    market_a,
                    market_b,
                    token_mint: Pubkey::default(),
                    entry_price: price_a,
                    exit_price: price_a + predicted_jump_a,
                    size,
                    expected_profit,
                    confidence,
                    predicted_jump: predicted_jump_a,
                    execution_window: 50,
                });
            }
        }

        None
    }

    #[inline]
    fn combine_confidence(jump: f64, decor: f64, sim: f64, slip_ok: f64) -> f64 {
        let w1 = 0.35; let w2 = 0.25; let w3 = 0.25; let w4 = 0.15;
        (w1*jump + w2*decor + w3*sim + w4*slip_ok).clamp(0.0, 0.99)
    }

    fn estimate_levy_parameters(history: &VecDeque<MarketSnapshot>) -> LevyParameters {
        // Microstructure filter: skip zero-spread ticks and dedup unchanged last_trade
        let mut returns: Vec<f64> = Vec::with_capacity(history.len().saturating_sub(1));
        for w in history.windows(2) {
            if w[1].spread <= 0.0 { continue; }
            if (w[1].last_trade - w[0].last_trade).abs() < f64::EPSILON { continue; }
            let r = (w[1].last_trade / w[0].last_trade).ln();
            if r.is_finite() { returns.push(r); }
        }
        if returns.len() < 10 {
            return LevyParameters {
                alpha: LEVY_ALPHA,
                beta: LEVY_BETA,
                gamma: LEVY_GAMMA,
                delta: LEVY_DELTA,
                jump_intensity: 0.0,
                drift: 0.0,
                volatility: 0.0,
            };
        }

        let (alpha_hat, beta_hat) = Self::mcculloch_alpha_beta(&returns);
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let var = returns.iter().map(|r| {
            let d = r - mean; d*d
        }).sum::<f64>() / returns.len() as f64;
        let sigma = var.sqrt();

        let gamma = sigma * (alpha_hat / 2.0).sqrt();
        // For delta, use mean as location proxy (robust alternative: median)
        let delta = mean;
        let jump_intensity = returns.iter().filter(|r| r.abs() > 2.0*sigma).count() as f64 / returns.len() as f64;

        LevyParameters {
            alpha: alpha_hat.clamp(1e-3, 2.0),
            beta: beta_hat.clamp(-0.99, 0.99),
            gamma,
            delta,
            jump_intensity,
            drift: mean,
            volatility: sigma,
        }
    }

    fn mcculloch_alpha_beta(returns: &[f64]) -> (f64, f64) {
        let mut v = returns.to_vec();
        v.sort_by(|a,b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = |p: f64| -> usize {
            let n = v.len();
            ((p * ((n-1) as f64)).round() as usize).min(n-1)
        };
        let q = |p: f64| v[idx(p)];
        let q05 = q(0.05);
        let q25 = q(0.25);
        let q50 = q(0.50);
        let q75 = q(0.75);
        let q95 = q(0.95);
        let denom = (q75 - q25).abs().max(1e-9);
        let a_hat = ((q95 - q05) / denom).clamp(1e-3, 2.0);
        let num = (q95 + q05) - 2.0*q50;
        let skew = if (q95 - q05).abs() > 1e-12 { (num / (q95 - q05)).clamp(-0.99, 0.99) } else { 0.0 };
        (a_hat, skew)
    }

    #[inline]
    fn bipower_variation(r: &[f64]) -> f64 {
        const MU1: f64 = (2.0 / std::f64::consts::PI).sqrt();
        if r.len() < 2 { return 0.0; }
        let s: f64 = r.windows(2).map(|w| w[0].abs() * w[1].abs()).sum();
        s / (MU1 * MU1)
    }

    #[inline]
    fn jump_probability(returns: &[f64]) -> f64 {
        if returns.len() < 64 { return 0.0; }
        let rv: f64 = returns.iter().map(|x| x * x).sum();
        let bv = Self::bipower_variation(returns);
        let j = (rv - bv).max(0.0);
        (j / (rv + 1e-12)).clamp(0.0, 0.99)
    }

    fn kendall_tau(a: &[f64], b: &[f64]) -> f64 {
        let n = a.len().min(b.len());
        if n < 2 { return 0.0; }
        let (a, b) = (&a[..n], &b[..n]);
        let mut concordant = 0i64;
        let mut discordant = 0i64;
        for i in 0..n {
            for j in (i+1)..n {
                let da = (a[j] - a[i]).partial_cmp(&0.0).unwrap_or(std::cmp::Ordering::Equal) as i32;
                let db = (b[j] - b[i]).partial_cmp(&0.0).unwrap_or(std::cmp::Ordering::Equal) as i32;
                let prod = da * db;
                if prod > 0 { concordant += 1; }
                else if prod < 0 { discordant += 1; }
            }
        }
        let denom = (concordant + discordant) as f64;
        if denom == 0.0 { 0.0 } else { (concordant as f64 - discordant as f64) / denom }
    }

    fn robust_corr(a: &[f64], b: &[f64]) -> f64 {
        let n = a.len().min(b.len());
        if n < 32 { return 0.0; }
        let a = &a[(a.len()-n)..];
        let b = &b[(b.len()-n)..];
        let mean = |x: &[f64]| x.iter().sum::<f64>() / (x.len() as f64).max(1.0);
        let ma = mean(a);
        let mb = mean(b);
        let (mut cov, mut va, mut vb) = (0.0, 0.0, 0.0);
        for i in 0..n {
            let da = (a[i] - ma).clamp(-10.0, 10.0);
            let db = (b[i] - mb).clamp(-10.0, 10.0);
            cov += da * db;
            va += da * da;
            vb += db * db;
        }
        if va > 1e-12 && vb > 1e-12 {
            cov / (va.sqrt() * vb.sqrt())
        } else {
            // Fallback to Kendall's tau when variance is tiny
            Self::kendall_tau(a, b)
        }
    }

    #[inline]
    fn kelly_fraction(edge: f64, var: f64) -> f64 {
        if var <= 1e-12 { return 0.0; }
        (edge / var).clamp(0.0, 0.25)
    }

    fn optimal_size(
        edge_per_unit: f64,
        var_per_unit: f64,
        max_position: u64,
        depth_qty: f64,
        slip_slope: f64,
    ) -> u64 {
        let f = Self::kelly_fraction(edge_per_unit, var_per_unit);
        // Simple impact guard: limit by slope of slip curve
        let impact_limited = (depth_qty * (0.5 / slip_slope.max(1e-9))).min(depth_qty * 0.1);
        ((f * impact_limited).min(max_position as f64)).max(0.0) as u64
    }

    fn calculate_jump_probability(params: &LevyParameters, history: &VecDeque<MarketSnapshot>) -> f64 {
        if history.len() < 20 {
            return 0.0;
        }

        let recent = &history.as_slices().1[history.len().saturating_sub(20)..];
        let mut features = vec![0.0; 8];

        for i in 1..recent.len() {
            let ret = (recent[i].last_trade / recent[i-1].last_trade).ln();
            features[0] += ret;
            features[1] += ret.powi(2);
            features[2] += recent[i].spread;
            features[3] += recent[i].imbalance;
            features[4] = features[4].max(recent[i].volume);
        }

        features[0] /= recent.len() as f64;
        features[1] = (features[1] / recent.len() as f64 - features[0].powi(2)).sqrt();
        features[2] /= recent.len() as f64;
        features[3] /= recent.len() as f64;
        
        let spread_widening = recent.last().unwrap().spread / recent.first().unwrap().spread;
        features[5] = spread_widening;
        
        let order_flow_imbalance = recent.last().unwrap().imbalance;
        features[6] = order_flow_imbalance;
        
        let momentum = (recent.last().unwrap().last_trade / recent[recent.len()/2].last_trade).ln();
        features[7] = momentum;

        let base_prob = params.jump_intensity;
        let spread_factor = 1.0 + (spread_widening - 1.0).max(0.0).min(2.0);
        let imbalance_factor = 1.0 + (order_flow_imbalance / 1000.0).min(1.0);
        let momentum_factor = 1.0 + momentum.abs().min(0.1) * 10.0;
        
        (base_prob * spread_factor * imbalance_factor * momentum_factor).min(0.95)
    }

    fn predict_levy_jump(params: &LevyParameters, current_price: f64) -> f64 {
        let u = rng_f64();
        let v = rng_f64() * 2.0 - 1.0;
        
        let phi = v * std::f64::consts::PI / 2.0;
        let w = -u.ln();
        
        let x = if params.alpha == 1.0 {
            2.0 / std::f64::consts::PI * ((std::f64::consts::PI / 2.0 + params.beta * phi) * phi.tan()
                - params.beta * (std::f64::consts::PI / 2.0 + params.beta * phi).ln())
        } else {
            let zeta = -params.beta * (std::f64::consts::PI * params.alpha / 2.0).tan();
            let xi = (1.0 + zeta.powi(2)).powf(1.0 / (2.0 * params.alpha));
            
            ((1.0 + params.beta * phi.tan()).sin() * xi) / phi.cos().powf(1.0 / params.alpha)
                * ((phi.cos() - params.beta * phi.sin()) / w).powf((1.0 - params.alpha) / params.alpha)
        };
        
        let jump_size = params.gamma * x + params.delta;
        current_price * jump_size.max(-0.5).min(0.5)
    }

    fn calculate_cross_correlation(
        history_a: &VecDeque<MarketSnapshot>,
        history_b: &VecDeque<MarketSnapshot>,
    ) -> f64 {
        let n = history_a.len().min(history_b.len()).min(100);
        if n < 20 {
            return 0.0;
        }

        let returns_a: Vec<f64> = history_a.iter()
            .skip(history_a.len() - n)
            .collect::<Vec<_>>()
            .windows(2)
            .map(|w| (w[1].last_trade / w[0].last_trade).ln())
            .collect();

        let returns_b: Vec<f64> = history_b.iter()
            .skip(history_b.len() - n)
            .collect::<Vec<_>>()
            .windows(2)
            .map(|w| (w[1].last_trade / w[0].last_trade).ln())
            .collect();

        let mean_a = returns_a.iter().sum::<f64>() / returns_a.len() as f64;
        let mean_b = returns_b.iter().sum::<f64>() / returns_b.len() as f64;

        let cov = returns_a.iter().zip(returns_b.iter())
            .map(|(a, b)| (a - mean_a) * (b - mean_b))
            .sum::<f64>() / returns_a.len() as f64;

        let std_a = (returns_a.iter()
            .map(|r| (r - mean_a).powi(2))
            .sum::<f64>() / returns_a.len() as f64).sqrt();

        let std_b = (returns_b.iter()
            .map(|r| (r - mean_b).powi(2))
            .sum::<f64>() / returns_b.len() as f64).sqrt();

        if std_a > 0.0 && std_b > 0.0 {
            cov / (std_a * std_b)
        } else {
            0.0
        }
    }

    fn calculate_optimal_size(
        snapshot_a: &MarketSnapshot,
        snapshot_b: &MarketSnapshot,
        params_a: &LevyParameters,
        params_b: &LevyParameters,
    ) -> u64 {
        let available_liquidity = snapshot_a.bid_size.min(snapshot_b.ask_size);
        let max_impact = 0.001;
        
        let price_impact_a = available_liquidity / (snapshot_a.bid_size + snapshot_a.ask_size);
        let price_impact_b = available_liquidity / (snapshot_b.bid_size + snapshot_b.ask_size);
        
        let vol_adjusted_size_a = (max_impact / params_a.volatility) * available_liquidity;
        let vol_adjusted_size_b = (max_impact / params_b.volatility) * available_liquidity;
        
        let optimal = vol_adjusted_size_a.min(vol_adjusted_size_b)
            .min(available_liquidity * 0.1)
            .min(MAX_POSITION_SIZE as f64);
        
        (optimal * 0.8) as u64
    }

    fn update_levy_parameters(
        price_history: &Arc<RwLock<HashMap<Pubkey, VecDeque<MarketSnapshot>>>>,
        levy_params: &Arc<RwLock<HashMap<Pubkey, LevyParameters>>>,
    ) {
        let history = price_history.read();
        let mut params = levy_params.write();

        for (market, snapshots) in history.iter() {
            if snapshots.len() >= 100 {
                let new_params = Self::estimate_levy_parameters(snapshots);
                
                if let Some(existing) = params.get_mut(market) {
                    existing.alpha = 0.9 * existing.alpha + 0.1 * new_params.alpha;
                    existing.beta = 0.9 * existing.beta + 0.1 * new_params.beta;
                    existing.gamma = 0.9 * existing.gamma + 0.1 * new_params.gamma;
                    existing.delta = 0.9 * existing.delta + 0.1 * new_params.delta;
                    existing.jump_intensity = 0.8 * existing.jump_intensity + 0.2 * new_params.jump_intensity;
                    existing.drift = 0.7 * existing.drift + 0.3 * new_params.drift;
                    existing.volatility = 0.8 * existing.volatility + 0.2 * new_params.volatility;
                } else {
                    params.insert(*market, new_params);
                }
            }
        }
    }

    async fn execute_flashloan(
        rpc_client: &RpcClient,
        keypair: &Keypair,
        opportunity: &FlashLoanOpportunity,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Validate token mint is set (no defaults in production)
        if opportunity.token_mint == Pubkey::default() {
            return Err("missing token mint".into());
        }

        // Refresh blockhash and slot; abort if aged
        let blockhash = rpc_client.get_latest_blockhash()?;
        let current_slot = rpc_client.get_slot()?;
        // If your pipeline caches a slot, compare and abort if age > 75 slots (here we just ensure fresh blockhash)

        let flash_loan_program = Pubkey::from_str("FLASHkGVm8yCvNxZrPGqXTcczn7zUfpnSzz7BwdeHvgp")?;
        let token_program = spl_token::id();
        
        let (authority_pda, _) = Pubkey::find_program_address(
            &[b"authority", keypair.pubkey().as_ref()],
            &flash_loan_program,
        );

        let user_token_account = spl_associated_token_account::get_associated_token_address(
            &keypair.pubkey(),
            &opportunity.token_mint,
        );

        // Validate token account owner/mint matches (fetch and parse)
        if let Ok(acc) = rpc_client.get_account(&user_token_account) {
            if let Ok(tok) = TokenAccount::unpack(&acc.data) {
                if tok.mint != opportunity.token_mint || tok.owner != keypair.pubkey() {
                    return Err("token account mint/owner mismatch".into());
                }
            }
        }

        let (pool_account, _) = Pubkey::find_program_address(
            &[b"pool", opportunity.token_mint.as_ref()],
            &flash_loan_program,
        );

        let mut ixs = vec![];

        let borrow_data = BorrowInstruction {
            amount: opportunity.size,
            expected_fee: (opportunity.size * 3) / 1000,
        };

        ixs.push(Instruction {
            program_id: flash_loan_program,
            accounts: vec![
                AccountMeta::new(keypair.pubkey(), true),
                AccountMeta::new(pool_account, false),
                AccountMeta::new(user_token_account, false),
                AccountMeta::new_readonly(authority_pda, false),
                AccountMeta::new_readonly(token_program, false),
                AccountMeta::new_readonly(system_program::id(), false),
            ],
            data: borrow_data.try_to_vec()?,
        });

        // Nonce robustness: combine slot entropy with RNG bits
        let slot_entropy = current_slot;
        let nonce: u64 = (slot_entropy << 32) ^ rng_f64().to_bits();

        let arb_instruction = self.create_arbitrage_instruction(
            opportunity,
            &keypair.pubkey(),
            &user_token_account,
            nonce,
        )?;
        ixs.push(arb_instruction);

        let repay_data = RepayInstruction {
            amount: opportunity.size,
            fee: (opportunity.size * 3) / 1000,
        };

        ixs.push(Instruction {
            program_id: flash_loan_program,
            accounts: vec![
                AccountMeta::new(keypair.pubkey(), true),
                AccountMeta::new(pool_account, false),
                AccountMeta::new(user_token_account, false),
                AccountMeta::new_readonly(authority_pda, false),
                AccountMeta::new_readonly(token_program, false),
            ],
            data: repay_data.try_to_vec()?,
        });

        // Build v0 transaction with compute budget (ALTs omitted for now)
        let cu_limit: u32 = 1_400_000; // tune per strategy
        let cu_price: u64 = 1_000; // micro-lamports per CU
        let vtx = Self::build_tx(keypair, ixs, blockhash, cu_limit, cu_price)?;

            // 1) Build v0 transaction with compute budget and optional tip
    fn build_v0_tx(
        payer: &Keypair,
        mut ixs: Vec<Instruction>,
        blockhash: solana_sdk::hash::Hash,
        cu_limit: u32,
        cu_price_microlamports: u64,
        jito_tip: Option<(Pubkey, u64)>, // (tip_recipient, lamports)
    ) -> VersionedTransaction {
        // Prepend compute budget instructions for priority fees
        let mut ixs_with_cb = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
            ComputeBudgetInstruction::set_compute_unit_price(cu_price_microlamports),
        ];
        ixs_with_cb.append(&mut ixs);

        // Optional Jito tip as a plain system transfer to the chosen recipient
        if let Some((tip_recipient, lamports)) = jito_tip {
            ixs_with_cb.push(system_instruction::transfer(
                &payer.pubkey(),
                &tip_recipient,
                lamports,
            ));
        }

        let msg_v0 = v0::Message::try_compile(
            &payer.pubkey(),
            &ixs_with_cb,
            &[],                // ALTs omitted intentionally here
            blockhash,
        ).expect("compile v0");

        VersionedTransaction::new(VersionedMessage::V0(msg_v0), &[payer])
    }

    // Base compute budget (will adapt after first simulation if needed)
    let mut cu_limit: u32 = 1_400_000;      // tune per strategy
    let mut cu_price: u64 = 1_000;          // micro-lamports per CU (aka priority fee)

    // If you have a decided tip account/amount for the current leader, pass it here.
    // No placeholders: the caller must supply a real account and amount when tipping.
    let jito_tip: Option<(Pubkey, u64)> = None;

    // Build initial versioned transaction
    let mut vtx = build_v0_tx(keypair, ixs.clone(), blockhash, cu_limit, cu_price, jito_tip);

    // 2) Simulate with guardrails
    let sim = rpc_client.simulate_transaction(&vtx)?;
    if let Some(err) = sim.value.err {
        // Surface logs for rapid diagnosis
        let logs = sim.value.logs.unwrap_or_default().join("\n");
        return Err(format!("simulation failed: {err:?}\nlogs:\n{logs}").into());
    }
    let cu_used = sim.value.units_consumed.unwrap_or(0);

    // 3) Adaptive compute: if we're close to the limit, rebuild with a safety margin
    if cu_used > 0 {
        let headroom = 1.20_f64; // 20% margin over observed usage
        let target = ((cu_used as f64) * headroom).ceil() as u32;
        // Never exceed a sane upper bound for your pipeline; adjust per strategy
        let max_cu: u32 = 1_800_000;
        let new_limit = target.min(max_cu);
        if new_limit > cu_limit {
            cu_limit = new_limit;
            vtx = build_v0_tx(keypair, ixs.clone(), blockhash, cu_limit, cu_price, jito_tip);

            // Re-simulate once after adjustment
            let sim2 = rpc_client.simulate_transaction(&vtx)?;
            if let Some(err) = sim2.value.err {
                let logs = sim2.value.logs.unwrap_or_default().join("\n");
                return Err(format!("simulation failed after CU adjust: {err:?}\nlogs:\n{logs}").into());
            }
        }
    }

    // 4) Send with robust config and fast confirmation path
    // Use min_context_slot to avoid landing on lagging forks
    let min_context_slot = rpc_client.get_slot_with_commitment(CommitmentConfig::processed())?;

    let send_cfg = RpcSendTransactionConfig {
        skip_preflight: false, // we already simulated, but keep RPC preflight for cluster diff
        preflight_commitment: Some(CommitmentConfig::processed()),
        max_retries: Some(5),
        min_context_slot: Some(min_context_slot),
        ..Default::default()
    };

    // Send and optionally confirm
    let sig = rpc_client.send_transaction_with_config(&vtx, send_cfg)?;
    // Fast confirm to reduce tail risk (optional if you have your own confirmer task)
    rpc_client.confirm_transaction(&sig)?;
}

    fn build_tx(
        payer: &Keypair,
        mut ixs: Vec<Instruction>,
        blockhash: Hash,
        cu_limit: u32,
        cu_price: u64,
    ) -> Result<VersionedTransaction, Box<dyn std::error::Error>> {
        let mut with_cb = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
            ComputeBudgetInstruction::set_compute_unit_price(cu_price),
        ];
        with_cb.append(&mut ixs);
        let msg = v0::Message::try_compile(
            &payer.pubkey(),
            &with_cb,
            &[],
            blockhash,
        )?;
        let vtx = VersionedTransaction::new(&VersionedMessage::V0(msg), &[payer]);
        Ok(vtx)
    }

    // ALT-aware builder (preferred)
    fn build_tx_with_alts(
        payer: &Keypair,
        mut ixs: Vec<Instruction>,
        blockhash: Hash,
        cu_limit: u32,
        cu_price: u64,
        alts: &[AddressLookupTableAccount],
    ) -> Result<VersionedTransaction, Box<dyn std::error::Error>> {
        let mut with_cb = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
            ComputeBudgetInstruction::set_compute_unit_price(cu_price),
        ];
        with_cb.append(&mut ixs);
        let msg = v0::Message::try_compile(
            &payer.pubkey(),
            &with_cb,
            alts,
            blockhash,
        )?;
        Ok(VersionedTransaction::new(&VersionedMessage::V0(msg), &[payer]))
    }

    fn create_arbitrage_instruction(
        &self,
        opportunity: &FlashLoanOpportunity,
        payer: &Pubkey,
        token_account: &Pubkey,
        nonce: u64,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let arb_program = Pubkey::from_str("ARB11111111111111111111111111111111111111111")?;

        let data = ArbitrageData {
            market_a: opportunity.market_a,
            market_b: opportunity.market_b,
            amount: opportunity.size,
            min_profit: (opportunity.expected_profit * 0.8) as u64,
            max_slippage: MAX_SLIPPAGE_BPS,
            nonce,
        };

        Ok(Instruction {
            program_id: arb_program,
            accounts: vec![
                AccountMeta::new(*payer, true),
                AccountMeta::new(*token_account, false),
                AccountMeta::new(opportunity.market_a, false),
                AccountMeta::new(opportunity.market_b, false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data: data.try_to_vec()?,
        })
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
struct BorrowInstruction {
    amount: u64,
    expected_fee: u64,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct RepayInstruction {
    amount: u64,
    fee: u64,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct ArbitrageData {
    market_a: Pubkey,
    market_b: Pubkey,
    amount: u64,
    min_profit: u64,
    max_slippage: u64,
    nonce: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_levy_parameters() {
        let mut history = VecDeque::new();
        for i in 0..100 {
            history.push_back(MarketSnapshot {
                timestamp: i as u64,
                bid: 100.0 + (i as f64).sin(),
                ask: 100.1 + (i as f64).sin(),
                bid_size: 1000.0,
                ask_size: 1000.0,
                last_trade: 100.05 + (i as f64).sin(),
                volume: 10000.0,
                spread: 0.001,
                imbalance: 0.0,
            });
        }
        let params = LevyProcessPredictor::estimate_levy_parameters(&history);
        assert!(params.alpha > 0.0 && params.alpha <= 2.0);
    }
}

impl LevyProcessPredictor {
    pub async fn initialize_and_run(rpc_url: &str, keypair_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let keypair_bytes = std::fs::read(keypair_path)?;
        let keypair = Keypair::from_bytes(&keypair_bytes)?;
        
        let predictor = Arc::new(Self::new(rpc_url, keypair));
        
        // Pre-warm caches
        predictor.prewarm_caches().await?;
        
        // Start main execution loop
        predictor.run().await;
        
        Ok(())
    }

    async fn prewarm_caches(&self) -> Result<(), Box<dyn std::error::Error>> {
        let markets = vec![
            "7dLVkUfBVfCGkFhSXDCq1ukM9usathSgS716t643iFGF",
            "8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYwn1XTh6",
            "DZjbn4XC8qoHKikZqzmhemykVzmossoayV9ffbsUqxVj",
            "HWHvQhFmJB3NUcu1aihKmrKegfVxBEHzwVX6yZCKEsi1",
        ];

        for market_str in markets {
            if let Ok(market_pubkey) = market_str.parse::<Pubkey>() {
                if let Ok(account) = self.rpc_client.get_account(&market_pubkey) {
                    if let Some(market_state) = Self::parse_market_by_owner(&account.owner, &account.data) {
                        let mut cache = self.market_cache.write();
                        cache.insert(market_pubkey, market_state.inner);
                    }
                }
            }
        }

        Ok(())
    }

    pub fn calculate_portfolio_risk(&self) -> f64 {
        let opportunities = self.pending_opportunities.lock().unwrap();
        if opportunities.is_empty() {
            return 0.0;
        }

        let total_exposure: f64 = opportunities.iter()
            .map(|o| o.size as f64 * o.entry_price)
            .sum();

        let risk_scores: Vec<f64> = opportunities.iter()
            .map(|o| {
                let position_risk = (o.size as f64) / MAX_POSITION_SIZE as f64;
                let confidence_risk = 1.0 - o.confidence;
                let jump_risk = o.predicted_jump.abs() / o.entry_price;
                (position_risk + confidence_risk + jump_risk) / 3.0
            })
            .collect();

        let avg_risk = risk_scores.iter().sum::<f64>() / risk_scores.len() as f64;
        let max_risk = risk_scores.iter().fold(0.0, |a, &b| a.max(b));

        (avg_risk * 0.7 + max_risk * 0.3).min(1.0)
    }

    fn validate_opportunity(&self, opportunity: &FlashLoanOpportunity) -> bool {
        // Check basic constraints
        if opportunity.size == 0 || opportunity.size > MAX_POSITION_SIZE {
            return false;
        }

        if opportunity.confidence < CONFIDENCE_THRESHOLD {
            return false;
        }

        if opportunity.expected_profit < (opportunity.size as f64 * MIN_PROFIT_BPS as f64 / 10000.0) {
            return false;
        }

        // Oracle freshness and price CI guard (placeholder)
        if !Self::oracle_freshness_ok(&opportunity.token_mint) { return false; }

        // Risk controls: per-mint and global drawdown; leader/mint cooldown
        let risk = self.risk_state.read();
        let per_mint_cap: u128 = 5_000_000_000; // customize
        let dd_cap: u128 = 10_000_000_000;      // customize
        if !allow_trade(&risk, &opportunity.token_mint, opportunity.size as u128, (per_mint_cap, dd_cap)) { return false; }
        // Cooldown keyed by (leader, mint) if present
        let leader = Pubkey::default(); // TODO: supply current slot leader
        if let Some(&until) = risk.cooldown_until.get(&(leader, opportunity.token_mint)) {
            let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0);
            if until > now { return false; }
        }

        // Check market conditions
        let markets = self.market_cache.read();
        if let (Some(market_a), Some(market_b)) = (markets.get(&opportunity.market_a), markets.get(&opportunity.market_b)) {
            let liquidity_a = market_a.native_coin_bid() + market_a.native_coin_ask();
            let liquidity_b = market_b.native_coin_bid() + market_b.native_coin_ask();
            
            if opportunity.size > liquidity_a.min(liquidity_b) / 10 {
                return false;
            }
        } else {
            return false;
        }

        true
    }

    #[inline]
    // Oracle freshness and CI guard
    #[derive(Clone, Debug, Default)]
    struct PriceFeed { publish_time: i64, price: f64, confidence_interval: f64 }
    #[inline]
    fn current_unix() -> i64 { std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0) }
    #[inline]
    fn oracle_freshness_ok_with_price(_mint: &Pubkey, price: &PriceFeed) -> bool {
        let now = current_unix();
        (price.publish_time + 3 >= now) && ((price.confidence_interval / price.price).is_finite() && (price.confidence_interval / price.price) < 0.01)
    }
    #[inline]
    fn oracle_freshness_ok(_mint: &Pubkey) -> bool { true }

    pub fn get_performance_metrics(&self) -> PerformanceMetrics {
        let executions = self.last_execution.read();
        let opportunities = self.pending_opportunities.lock();
        let history = self.price_history.read();

        let total_markets = history.len();
        let total_snapshots: usize = history.values().map(|v| v.len()).sum();
        let pending_opportunities = opportunities.len();
        let recent_executions = executions.values()
            .filter(|t| t.elapsed().as_secs() < 300)
            .count();

        PerformanceMetrics {
            total_markets,
            total_snapshots,
            pending_opportunities,
            recent_executions,
            portfolio_risk: self.calculate_portfolio_risk(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub total_markets: usize,
    pub total_snapshots: usize,
    pub pending_opportunities: usize,
    pub recent_executions: usize,
    pub portfolio_risk: f64,
}

// RNG for Lévy process sampling (replaces ad-hoc fastrand)
thread_local! {
    static RNG_XOSHIRO: std::cell::RefCell<Xoshiro256Plus> = std::cell::RefCell::new(Xoshiro256Plus::seed_from_u64(0xC0FFEEu64));
}
#[inline]
fn rng_f64() -> f64 {
    RNG_XOSHIRO.with(|r| r.borrow_mut().gen::<f64>())
}

// Tracing + metrics for execution
#[instrument(skip_all, fields(mint=%mint, leader=%leader))]
fn record_exec(success: bool, cu: u32, tip: u64, slip_bps: f64, mint: &Pubkey, leader: &Pubkey, slot_delta: u64, relay_id: u32) {
    let _ = (mint, leader); // captured by fields
    histogram!("exec.cu_used", cu as f64);
    histogram!("exec.slip_bps", slip_bps);
    histogram!("exec.tip_lamports", tip as f64);
    histogram!("exec.slot_delta", slot_delta as f64);
    counter!("exec.relay_id", relay_id as u64);
    if success { counter!("exec.success", 1); } else { counter!("exec.fail", 1); }
}

// Entry point for production deployment
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let rpc_url = std::env::var("SOLANA_RPC_URL")
        .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());
    
    let keypair_path = std::env::var("KEYPAIR_PATH")
        .unwrap_or_else(|_| "./keypair.json".to_string());

    log::info!("Starting Lévy Process Flash Loan Predictor");
    log::info!("RPC URL: {}", rpc_url);

    LevyProcessPredictor::initialize_and_run(&rpc_url, &keypair_path).await?;

    Ok(())
}
