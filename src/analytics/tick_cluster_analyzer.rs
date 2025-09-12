use anchor_lang::prelude::*;
use arrayref::array_ref;
use borsh::{BorshDeserialize, BorshSerialize};
use parking_lot::RwLock; // faster, no poisoning semantics
use rayon::prelude::*;   // for parallel parsing
use smallvec::SmallVec;  // small vector for cluster building
use futures::future::select_ok; // legacy hedging (kept for any other usage)
use futures::{FutureExt, stream::FuturesUnordered, StreamExt};
use dashmap::DashMap; // TTL caches
use ahash::RandomState; // fast hashing
use hashbrown::HashMap as FastHashMap; // faster HashMap
use xxhash_rust::xxh3::xxh3_64; // fast content fingerprint
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::{
    account::Account,
    clock::Clock,
    pubkey::Pubkey,
    signature::Keypair,
};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, VecDeque},
    fmt,
    sync::Arc,
    time::{Duration, Instant},
};
use std::convert::TryInto;
use tokio::sync::mpsc;

// Fast map aliases used across the module
type MapI32U128 = FastHashMap<i32, u128, RandomState>;
type MapPkAnalysis = FastHashMap<Pubkey, ClusterAnalysis, RandomState>;
type MapPkU64 = FastHashMap<Pubkey, u64, RandomState>;

#[derive(Debug)]
pub enum ClusterAnalysisError {
    MissingData(String),
    RpcError(String),
}

impl fmt::Display for ClusterAnalysisError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ClusterAnalysisError::MissingData(msg) => write!(f, "Missing data: {}", msg),
            ClusterAnalysisError::RpcError(msg) => write!(f, "RPC error: {}", msg),
        }
    }
}

// Telemetry-ranked hedging + circuit breaker (PIECE R3)
#[derive(Clone)]
struct EndpointStat {
    ewma_ms: f64,
    fail_ewma: f64,
    cb_open_until: Option<Instant>,
}

impl EndpointStat {
    #[inline] fn new() -> Self { Self { ewma_ms: 40.0, fail_ewma: 0.0, cb_open_until: None } }
    #[inline] fn score(&self) -> f64 {
        let cb_penalty = match self.cb_open_until { Some(until) if Instant::now() < until => 5_000.0, _ => 0.0 };
        let lat = self.ewma_ms.min(1_200.0);
        lat * (1.0 + 1.7 * self.fail_ewma) + cb_penalty
    }
    #[inline] fn observe_ok(&mut self, ms: f64) {
        let a = 0.18; let ms_clipped = ms.min(2_000.0);
        self.ewma_ms = (1.0 - a) * self.ewma_ms + a * ms_clipped; self.fail_ewma = (1.0 - 0.10) * self.fail_ewma; self.cb_open_until = None;
    }
    #[inline] fn observe_err(&mut self) {
        self.fail_ewma = (1.0 - 0.05) * self.fail_ewma + 0.05; if self.fail_ewma > 0.52 { self.cb_open_until = Some(Instant::now() + Duration::from_millis(900)); }
    }
}

struct RpcFanout {
    clients: SmallVec<[Arc<RpcClient>; 3]>,
    stats: Arc<RwLock<Vec<EndpointStat>>>,
    per_req_timeout_ms: u64,
    hedge_stagger_ms: u64,
    commitment: CommitmentConfig,
    quorum: usize,
}

impl RpcFanout {
    #[inline]
    fn new_single(endpoint: &str, per_req_timeout_ms: u64) -> Self {
        Self {
            clients: smallvec::smallvec![Arc::new(RpcClient::new_with_commitment(endpoint.to_string(), CommitmentConfig::processed()))],
            stats: Arc::new(RwLock::new(vec![EndpointStat::new()])),
            per_req_timeout_ms,
            hedge_stagger_ms: 25,
            commitment: CommitmentConfig::processed(),
            quorum: 1,
        }
    }

    #[inline]
    fn new_multi(endpoints: &[&str], per_req_timeout_ms: u64) -> Self {
        let clients = endpoints
            .iter()
            .map(|e| Arc::new(RpcClient::new_with_commitment((*e).to_string(), CommitmentConfig::processed())))
            .collect::<SmallVec<[Arc<RpcClient>; 3]>>();
        let quorum = (clients.len().saturating_add(1)) / 2;
        let stats = vec![EndpointStat::new(); clients.len()];
        Self { clients, stats: Arc::new(RwLock::new(stats)), per_req_timeout_ms, hedge_stagger_ms: 20, commitment: CommitmentConfig::processed(), quorum }
    }

    #[inline] fn with_commitment(mut self, c: CommitmentConfig) -> Self { self.commitment = c; self }
    #[inline] fn with_stagger(mut self, ms: u64) -> Self { self.hedge_stagger_ms = ms; self }
    #[inline] fn with_quorum(mut self, q: usize) -> Self { self.quorum = q.max(1); self }

    #[inline]
    fn ranked_indices(&self) -> SmallVec<[usize; 8]> {
        let stats = self.stats.read();
        let mut idx: SmallVec<[usize; 8]> = (0..self.clients.len()).collect();
        idx.sort_unstable_by(|&a, &b| stats[a].score().partial_cmp(&stats[b].score()).unwrap_or(Ordering::Equal));
        idx
    }

    async fn get_account_fast(&self, key: &Pubkey) -> Result<Account, ClusterAnalysisError> {
        let order = self.ranked_indices();
        let mut tasks = FuturesUnordered::new();
        for (i, idx) in order.iter().enumerate() {
            let k = *key; let c = Arc::clone(&self.clients[*idx]); let delay = self.hedge_stagger_ms.saturating_mul(i as u64); let stats = Arc::clone(&self.stats); let per_req = 250u64.max(self.per_req_timeout_ms);
            tasks.push(async move {
                if delay > 0 { tokio::time::sleep(Duration::from_millis(delay)).await; }
                let t0 = tokio::time::Instant::now();
                let res = tokio::time::timeout(Duration::from_millis(per_req), c.get_account(&k)).await;
                let ms = t0.elapsed().as_millis() as f64;
                match res {
                    Err(_) => { stats.write()[*idx].observe_err(); Err(ClusterAnalysisError::RpcError("timeout get_account".into())) }
                    Ok(Err(e)) => { stats.write()[*idx].observe_err(); Err(ClusterAnalysisError::RpcError(format!("get_account: {e}"))) }
                    Ok(Ok(acc)) => { stats.write()[*idx].observe_ok(ms); Ok::<(usize, Account), ClusterAnalysisError>((*idx, acc)) }
                }
            });
        }
        let mut tallies: FastHashMap<u64, (Account, usize), RandomState> = FastHashMap::with_hasher(RandomState::new());
        let mut best_latency: Option<(f64, Account)> = None;
        while let Some(res) = tasks.next().await {
            if let Ok((idx, acc)) = res {
                let h = hash_account(&acc);
                let e = tallies.entry(h).or_insert_with(|| (acc.clone(), 0));
                e.1 += 1;
                let ew = self.stats.read()[idx].ewma_ms;
                match &mut best_latency { Some((best,_)) if ew < *best => best_latency = Some((ew, acc.clone())), None => best_latency = Some((ew, acc.clone())), _ => {} }
                if e.1 >= self.quorum { return Ok(e.0.clone()); }
            }
        }
        if let Some((_, acc)) = best_latency { return Ok(acc); }
        Err(ClusterAnalysisError::RpcError("fanout get_account: no quorum".into()))
    }

    async fn get_multiple_accounts_fast(&self, addrs: &[Pubkey]) -> Result<Vec<Option<Account>>, ClusterAnalysisError> {
        const CHUNK: usize = 100; if addrs.is_empty() { return Ok(vec![]); }
        let order = self.ranked_indices(); let mut out = vec![None; addrs.len()];
        for (chunk_idx, chunk) in addrs.chunks(CHUNK).enumerate() {
            let rel_len = chunk.len();
            let mut tallies: Vec<FastHashMap<u64, (Account, usize), RandomState>> = (0..rel_len).map(|_| FastHashMap::with_hasher(RandomState::new())).collect();
            let mut decided = vec![false; rel_len];
            let mut tasks = FuturesUnordered::new();
            for (i, idx) in order.iter().enumerate() {
                let c = Arc::clone(&self.clients[*idx]); let addrs_vec = chunk.to_vec(); let delay = self.hedge_stagger_ms.saturating_mul(i as u64); let stats = Arc::clone(&self.stats); let per_req = 250u64.max(self.per_req_timeout_ms);
                tasks.push(async move {
                    if delay > 0 { tokio::time::sleep(Duration::from_millis(delay)).await; }
                    let t0 = tokio::time::Instant::now();
                    let res = tokio::time::timeout(Duration::from_millis(per_req), c.get_multiple_accounts(&addrs_vec)).await;
                    let ms = t0.elapsed().as_millis() as f64;
                    match res {
                        Err(_) => { stats.write()[*idx].observe_err(); Err(ClusterAnalysisError::RpcError("timeout get_multiple_accounts".into())) }
                        Ok(Err(e)) => { stats.write()[*idx].observe_err(); Err(ClusterAnalysisError::RpcError(format!("get_multiple_accounts: {e}"))) }
                        Ok(Ok(vec_accs)) => { stats.write()[*idx].observe_ok(ms); Ok::<(usize, Vec<Option<Account>>), ClusterAnalysisError>((*idx, vec_accs)) }
                    }
                });
            }
            while let Some(res) = tasks.next().await {
                if let Ok((_idx, vec_accs)) = res { if vec_accs.len() != rel_len { continue; }
                    for (i, opt) in vec_accs.into_iter().enumerate() {
                        if decided[i] { continue; }
                        if let Some(acc) = opt { let h = hash_account(&acc); let e = tallies[i].entry(h).or_insert_with(|| (acc.clone(), 0)); e.1 += 1; if e.1 >= self.quorum { let base = chunk_idx * CHUNK; out[base + i] = Some(e.0.clone()); decided[i] = true; } }
                    }
                    if decided.iter().all(|&d| d) { break; }
                }
            }
            let base = chunk_idx * CHUNK; for i in 0..rel_len { if out[base + i].is_none() { if let Some((_h, (acc, _))) = tallies[i].iter().max_by_key(|(_, v)| v.1) { out[base + i] = Some(acc.clone()); } } }
        }
        Ok(out)
    }
}

// TTLs for caches
const BITMAP_TTL_MS: u64 = 800;
const TICKARR_TTL_MS: u64 = 800;

#[inline]
fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// Actionability gate
impl TickClusterAnalyzer {
    #[inline]
    fn is_cluster_actionable(&self, c: &TickCluster) -> bool {
        let n = c.ticks.len(); if n < 6 { return false; }
        let center = c.center_tick; let mut central: u128 = 0;
        for t in &c.ticks { if (t.tick_index - center).abs() <= 2 { central = central.saturating_add(t.liquidity_gross); } }
        let central_share = (central as f64) / (c.total_liquidity as f64 + 1e-9);
        let hhi = self.hhi_concentration(&c.ticks, c.total_liquidity);
        let vol = c.volatility_score.max(self.robust_volatility_score(&c.ticks));
        (central_share >= 0.52) && (hhi >= 0.18) && (vol <= 0.45)
    }
}

// Fee-true arbitrage net PnL
impl TickClusterAnalyzer {
    #[inline]
    fn arb_net_profit_estimate(
        &self,
        swap_amount: u64,
        price_diff_frac: f64,
        pool: &PoolState,
        tip_budget: u64,
        liq_L: f64,
    ) -> i128 {
        let fee_rate = (pool.fee_rate as f64) / (BASIS_POINT_MAX as f64);
        let proto_rate = (pool.protocol_fee_rate as f64) / (BASIS_POINT_MAX as f64);
        let impact = self.estimate_price_impact_at(swap_amount, liq_L, pool.sqrt_price_x64);
        let gross = (swap_amount as f64) * (price_diff_frac - impact).max(0.0);
        let fees = (swap_amount as f64) * (fee_rate + proto_rate);
        let net = gross - fees - (tip_budget as f64);
        (net.floor() as i128).max(0)
    }
}

impl std::error::Error for ClusterAnalysisError {}

const TICK_WINDOW_SIZE: usize = 256;
const CLUSTER_MIN_SIZE: usize = 3;
const CLUSTER_MAX_DISTANCE: u64 = 5;
const VOLUME_THRESHOLD: u64 = 1_000_000_000; // 1000 USDC
const TICK_SPACING: u16 = 64;
const BASIS_POINT_MAX: u64 = 10000;
const SQRT_PRICE_LIMIT_X64: u128 = 79228162514264337593543950336;
const MIN_SQRT_PRICE_X64: u128 = 4295128739;
const MAX_SQRT_PRICE_X64: u128 = 1461446703485210103287273052203988822378723970342;
// Deterministic power-of-two as f64 (exact representable)
const TWO_POW_64_F64: f64 = 18446744073709551616.0; // 2^64

// Off-chain unix timestamp seconds
#[inline]
fn unix_ts_s() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0)
}

// RF0: Quorum content fingerprint for Accounts
#[inline]
fn hash_account(acc: &Account) -> u64 {
    use xxhash_rust::xxh3::xxh3_64;
    let mut buf = Vec::with_capacity(32 + 8 + 8 + 16);
    buf.extend_from_slice(acc.owner.as_ref());
    buf.extend_from_slice(&acc.lamports.to_le_bytes());
    buf.extend_from_slice(&(acc.data.len() as u64).to_le_bytes());
    let d = xxh3_64(&acc.data);
    buf.extend_from_slice(&d.to_le_bytes());
    xxh3_64(&buf)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, BorshSerialize, BorshDeserialize)]
pub struct TickData {
    pub tick_index: i32,
    pub sqrt_price_x64: u128,
    pub liquidity_net: i128,
    pub liquidity_gross: u128,
    pub fee_growth_outside_a: u128,
    pub fee_growth_outside_b: u128,
    pub timestamp: u64,
    pub block_height: u64,
}

#[derive(Debug, Clone)]
pub struct TickCluster {
    pub center_tick: i32,
    pub ticks: Vec<TickData>,
    pub total_liquidity: u128,
    pub price_range: (u128, u128),
    pub volume_24h: u64,
    pub concentration_score: f64,
    pub volatility_score: f64,
    pub opportunity_score: f64,
}

#[derive(Debug, Clone)]
pub struct ClusterAnalysis {
    pub cluster_id: u64,
    pub timestamp: u64,
    pub clusters: Vec<TickCluster>,
    pub dominant_cluster: Option<TickCluster>,
    pub liquidity_distribution: MapI32U128,
    pub price_impact_map: BTreeMap<i32, f64>,
    pub mev_opportunities: Vec<MevOpportunity>,
}

#[derive(Debug, Clone)]
pub struct MevOpportunity {
    pub opportunity_type: OpportunityType,
    pub tick_range: (i32, i32),
    pub expected_profit: u64,
    pub risk_score: f64,
    pub execution_probability: f64,
    pub gas_estimate: u64,
    pub priority_fee: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpportunityType {
    Arbitrage,
    Liquidation,
    JitLiquidity,
    BackRun,
    Sandwich,
}

 

pub struct TickClusterAnalyzer {
    rpc: Arc<RpcFanout>,
    tick_history: Arc<RwLock<VecDeque<TickData>>>,
    cluster_cache: Arc<RwLock<MapPkAnalysis>>,
    price_oracle: Arc<RwLock<MapPkU64>>,
    bitmap_cache: DashMap<Pubkey, (Vec<u8>, u64)>,
    tick_array_cache: DashMap<(Pubkey, i32), (Vec<TickData>, u64)>,
    tick_spacing_override: DashMap<Pubkey, u16>,
    default_tick_spacing: u16,
    tip_state: RwLock<TipEwma>,
    liquidity_threshold: u128,
    analysis_interval: Duration,
}

impl TickClusterAnalyzer {
    pub fn new(rpc_endpoint: &str) -> Self {
        Self {
            rpc: Arc::new(RpcFanout::new_single(rpc_endpoint, 250)),
            tick_history: Arc::new(RwLock::new(VecDeque::with_capacity(TICK_WINDOW_SIZE))),
            cluster_cache: Arc::new(RwLock::new(FastHashMap::with_hasher(RandomState::new()))),
            price_oracle: Arc::new(RwLock::new(FastHashMap::with_hasher(RandomState::new()))),
            bitmap_cache: DashMap::new(),
            tick_array_cache: DashMap::new(),
            tick_spacing_override: DashMap::new(),
            default_tick_spacing: TICK_SPACING,
            tip_state: RwLock::new(TipEwma::new(0.2)),
            liquidity_threshold: 50_000_000_000_000, // 50k USDC equivalent
            analysis_interval: Duration::from_millis(100),
        }
    }

    pub fn new_multi(endpoints: &[&str]) -> Self {
        Self {
            rpc: Arc::new(RpcFanout::new_multi(endpoints, 250)),
            tick_history: Arc::new(RwLock::new(VecDeque::with_capacity(TICK_WINDOW_SIZE))),
            cluster_cache: Arc::new(RwLock::new(FastHashMap::with_hasher(RandomState::new()))),
            price_oracle: Arc::new(RwLock::new(FastHashMap::with_hasher(RandomState::new()))),
            bitmap_cache: DashMap::new(),
            tick_array_cache: DashMap::new(),
            tick_spacing_override: DashMap::new(),
            default_tick_spacing: TICK_SPACING,
            tip_state: RwLock::new(TipEwma::new(0.2)),
            liquidity_threshold: 50_000_000_000_000, // 50k USDC equivalent
            analysis_interval: Duration::from_millis(100),
        }
    }

    pub async fn analyze_pool_ticks(&self, pool_address: &Pubkey) -> Result<ClusterAnalysis, ClusterAnalysisError> {
        let account = self
            .rpc
            .get_account_fast(pool_address)
            .await?;
        let pool_data = self
            .decode_pool_state(&account)
            .map_err(|e| ClusterAnalysisError::MissingData(format!("decode_pool_state: {e}")))?;

        // Fetch current bitmap and get previous snapshot (for diff)
        let (tick_array_bitmap, prev_bitmap_opt) = self
            .fetch_bitmap_with_prev(pool_address)
            .await?;
        let active_ticks = self
            .fetch_active_ticks_batched_diff(
                pool_address,
                &tick_array_bitmap,
                prev_bitmap_opt.as_deref(),
            )
            .await?;

        let (clusters, liquidity_distribution, price_impact_map) =
            self.analyze_views(pool_address, &active_ticks, &pool_data);

        let mev_opportunities = self.detect_mev_opportunities(&clusters, &pool_data);

        let ts = unix_ts_s();

        let analysis = ClusterAnalysis {
            cluster_id: self.generate_cluster_id_for(pool_address, &clusters, ts),
            timestamp: ts,
            dominant_cluster: self.find_dominant_cluster(&clusters),
            clusters,
            liquidity_distribution,
            price_impact_map,
            mev_opportunities,
        };

        self.cluster_cache.write().insert(*pool_address, analysis.clone());

        Ok(analysis)
    }

    fn identify_clusters(&self, ticks: &[TickData]) -> Vec<TickCluster> {
        if ticks.is_empty() { return Vec::new(); }
        let ticks = self.denoise_ticks(ticks);
        if ticks.is_empty() { return Vec::new(); }
        let mut sorted = ticks.to_vec();
        sorted.sort_unstable_by_key(|t| t.tick_index);

        let mut out = Vec::with_capacity(16);
        let mut i = 0;
        while i < sorted.len() {
            let mut cluster: SmallVec<[TickData; 32]> = SmallVec::new();
            cluster.push(sorted[i]);
            let mut j = i + 1;

            while j < sorted.len()
                && (sorted[j].tick_index - sorted[j - 1].tick_index) as u64 <= CLUSTER_MAX_DISTANCE
            {
                cluster.push(sorted[j]);
                j += 1;
            }

            if cluster.len() >= CLUSTER_MIN_SIZE {
                let built = self.build_cluster_smallvec(&cluster);
                if built.total_liquidity >= self.liquidity_threshold {
                    out.push(built);
                }
            }
            i = j;
        }
        out
    }

    fn build_cluster_smallvec(&self, ticks: &SmallVec<[TickData; 32]>) -> TickCluster {
        let mid = ticks.len() / 2;
        let center_tick = ticks[mid].tick_index;
        let total_liquidity: u128 = ticks.iter().map(|t| t.liquidity_gross).sum();

        let price_range = (ticks.first().unwrap().sqrt_price_x64, ticks.last().unwrap().sqrt_price_x64);
        let volume_24h = self.calculate_cluster_volume(ticks);
        let concentration_score = self.calculate_concentration_score(ticks, total_liquidity);
        let volatility_score = self.robust_volatility_score(ticks);
        let opportunity_score = self.opportunity_score_calibrated(
            concentration_score,
            ticks,
            volatility_score,
            volume_24h,
            total_liquidity,
        );

        TickCluster {
            center_tick,
            ticks: ticks.to_vec(),
            total_liquidity,
            price_range,
            volume_24h,
            concentration_score,
            volatility_score,
            opportunity_score,
        }
    }

    // Denoise ticks (PIECE C2): keep ticks that are at least EPS of local window max
    #[inline]
    fn denoise_ticks(&self, ticks: &[TickData]) -> SmallVec<[TickData; 256]> {
        if ticks.len() < 5 {
            return SmallVec::from_slice(ticks);
        }
        const EPS: f64 = 0.035;
        let mut out: SmallVec<[TickData; 256]> = SmallVec::new();
        for i in 0..ticks.len() {
            let liq = ticks[i].liquidity_gross as f64;
            let start = i.saturating_sub(2);
            let end = (i + 2).min(ticks.len() - 1);
            let mut lmax: u128 = 0;
            for j in start..=end {
                if ticks[j].liquidity_gross > lmax {
                    lmax = ticks[j].liquidity_gross;
                }
            }
            let lmaxf = lmax as f64;
            if lmaxf == 0.0 || (liq / lmaxf) >= EPS {
                out.push(ticks[i]);
            }
        }
        out
    }

    fn calculate_concentration_score(&self, ticks: &[TickData], total_liquidity: u128) -> f64 {
        if ticks.is_empty() || total_liquidity == 0 {
            return 0.0;
        }
        
        let mut gini_sum = 0.0;
        let n = ticks.len() as f64;
        
        for (i, tick) in ticks.iter().enumerate() {
            let liquidity_ratio = tick.liquidity_gross as f64 / total_liquidity as f64;
            gini_sum += (2.0 * (i as f64 + 1.0) - n - 1.0) * liquidity_ratio;
        }
        
        1.0 - (gini_sum / n).abs()
    }

    fn calculate_volatility_score(&self, ticks: &[TickData]) -> f64 {
        if ticks.len() < 2 {
            return 0.0;
        }
        
        let prices: Vec<f64> = ticks.iter()
            .map(|t| self.sqrt_price_to_price(t.sqrt_price_x64))
            .collect();
        
        let mean = prices.iter().sum::<f64>() / prices.len() as f64;
        let variance = prices.iter()
            .map(|p| (p - mean).powi(2))
            .sum::<f64>() / prices.len() as f64;
        
        variance.sqrt() / mean
    }

    // Robust volatility (MAD-based) and HHI concentration (PIECE P)
    #[inline]
    fn median_sorted(&self, v: &mut [f64]) -> f64 {
        v.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
        let n = v.len();
        if n == 0 { return 0.0; }
        if n % 2 == 1 { v[n/2] } else { 0.5 * (v[n/2 - 1] + v[n/2]) }
    }

    #[inline]
    fn hhi_concentration(&self, ticks: &[TickData], total_liquidity: u128) -> f64 {
        if total_liquidity == 0 { return 1.0; }
        let denom = total_liquidity as f64;
        let mut sum = 0.0;
        for t in ticks {
            let s = (t.liquidity_gross as f64) / denom;
            sum += s * s;
        }
        sum.clamp(0.0, 1.0)
    }

    #[inline]
    fn robust_volatility_score(&self, ticks: &[TickData]) -> f64 {
        if ticks.len() < 3 { return 0.0; }
        let mut px: Vec<f64> = ticks.iter().map(|t| self.sqrt_price_to_price(t.sqrt_price_x64)).collect();
        let med = {
            let mut tmp = px.clone();
            self.median_sorted(&mut tmp)
        };
        if med <= 0.0 { return 0.0; }
        let mut dev: Vec<f64> = px.iter().map(|p| (p - med).abs()).collect();
        let mad = self.median_sorted(&mut dev);
        let vol = (1.4826 * mad) / med; // Gaussian-consistent MAD
        vol.clamp(0.0, 1.0)
    }

    #[inline]
    fn opportunity_score_calibrated(
        &self,
        concentration_gini: f64,
        ticks: &[TickData],
        vol_robust: f64,
        volume: u64,
        liquidity: u128,
    ) -> f64 {
        let hhi = self.hhi_concentration(ticks, liquidity);
        let hhi_penalty = (1.0 - hhi).clamp(0.0, 1.0);
        let v_score = ((volume as f64) + 1.0).ln() / 25.0;
        let l_score = ((liquidity as f64) + 1.0).ln() / 35.0;
        let vol_adj = vol_robust.min(0.5) * 2.0;
        let raw = 0.28*concentration_gini + 0.22*hhi_penalty + 0.25*vol_adj + 0.15*v_score + 0.10*l_score;
        let s = 1.0 / (1.0 + (-3.0 * (raw - 0.5)).exp());
        s.clamp(0.0, 1.0)
    }

    fn calculate_opportunity_score(&self, concentration: f64, volatility: f64, volume: u64, liquidity: u128) -> f64 {
        let volume_score = (volume as f64).ln() / 25.0;
        let liquidity_score = (liquidity as f64).ln() / 35.0;
        let volatility_adjusted = volatility.min(0.5) * 2.0;
        
        (concentration * 0.3 + volatility_adjusted * 0.3 + volume_score * 0.2 + liquidity_score * 0.2)
            .min(1.0)
            .max(0.0)
    }

    fn detect_mev_opportunities(&self, clusters: &[TickCluster], pool_data: &PoolState) -> Vec<MevOpportunity> {
        let mut opportunities = Vec::new();
        
        for cluster in clusters {
            if cluster.opportunity_score > 0.7 && self.is_cluster_actionable(cluster) && self.gate_by_oracle(pool_data, cluster) {
                if let Some(arb_opp) = self.check_arbitrage_opportunity(cluster, pool_data) {
                    opportunities.push(arb_opp);
                }
                
                if cluster.volatility_score > 0.3 {
                    if let Some(sandwich_opp) = self.check_sandwich_opportunity(cluster, pool_data) {
                        opportunities.push(sandwich_opp);
                    }
                }
                
                if cluster.concentration_score > 0.8 {
                    if let Some(jit_opp) = self.check_jit_liquidity_opportunity(cluster, pool_data) {
                        opportunities.push(jit_opp);
                    }
                }
            }
        }

        opportunities.sort_by(|a, b| {
            let ascore = self.net_pnl_score(a.expected_profit, a.risk_score, a.execution_probability, a.priority_fee);
            let bscore = self.net_pnl_score(b.expected_profit, b.risk_score, b.execution_probability, b.priority_fee);
            ord_f64_desc(ascore, bscore)
        });

        opportunities
    }

    fn check_arbitrage_opportunity(&self, c: &TickCluster, pool: &PoolState) -> Option<MevOpportunity> {
        let current_price = self.sqrt_price_to_price(pool.sqrt_price_x64);
        let cluster_price = self.sqrt_price_to_price(c.price_range.0);
        let price_diff = (current_price - cluster_price).abs() / current_price;

        if price_diff <= 0.0015 { return None; }

        let swap_amount = self.calculate_optimal_swap_amount(c, pool);
        let mult = self.tip_state.read().tip_multiplier();
        let tip = ((swap_amount as f64) * 0.0025 * mult) as u64; // ~25bps scaled by congestion
        let tip = tip.clamp(3_000, 700_000);
        let net = self.arb_net_profit_estimate(swap_amount, price_diff, pool, tip, c.total_liquidity as f64);
        if net <= 0 { return None; }

        let start_tick = c.ticks.first().map(|t| t.tick_index).unwrap_or(c.center_tick - 1);
        let end_tick = c.ticks.last().map(|t| t.tick_index).unwrap_or(c.center_tick + 1);

        Some(MevOpportunity {
            opportunity_type: OpportunityType::Arbitrage,
            tick_range: (start_tick, end_tick),
            expected_profit: net as u64,
            risk_score: (0.28 + c.volatility_score * 0.45).min(0.95),
            execution_probability: (0.88 - c.volatility_score * 0.22).max(0.35),
            gas_estimate: 300_000,
            priority_fee: tip,
        })
    }

    fn check_sandwich_opportunity(&self, cluster: &TickCluster, pool_data: &PoolState) -> Option<MevOpportunity> {
        if cluster.volume_24h < VOLUME_THRESHOLD * 10 {
            return None;
        }
        
        let liquidity_depth = cluster.total_liquidity as f64;
        let price_impact = self.estimate_price_impact_at(VOLUME_THRESHOLD, liquidity_depth, pool_data.sqrt_price_x64);
        
        if price_impact > 0.001 && price_impact < 0.01 {
            let expected_profit = (VOLUME_THRESHOLD as f64 * price_impact * 0.5) as u64;
            let priority_fee = self.jito_tip_budget(expected_profit);
            return Some(MevOpportunity {
                opportunity_type: OpportunityType::Sandwich,
                tick_range: (cluster.center_tick - 10, cluster.center_tick + 10),
                expected_profit,
                risk_score: 0.6 + price_impact * 10.0,
                execution_probability: 0.7 - price_impact * 5.0,
                gas_estimate: 600_000,
                priority_fee,
            });
        }
        
        None
    }

    fn check_jit_liquidity_opportunity(&self, cluster: &TickCluster, pool_data: &PoolState) -> Option<MevOpportunity> {
        let tick_spacing_multiplier = (TICK_SPACING as f64 / 64.0).max(1.0);
        let concentration_threshold = 0.85 * tick_spacing_multiplier;
        
        if cluster.concentration_score > concentration_threshold {
            let liquidity_to_add = self.calculate_jit_liquidity_amount(cluster, pool_data);
            let fee_earnings = self.estimate_jit_fee_earnings(liquidity_to_add, cluster.volume_24h, pool_data);
            
            if fee_earnings > 1_000_000 {
                let priority_fee = self.jito_tip_budget(fee_earnings);
                return Some(MevOpportunity {
                    opportunity_type: OpportunityType::JitLiquidity,
                    tick_range: (cluster.center_tick - 2, cluster.center_tick + 2),
                    expected_profit: fee_earnings,
                    risk_score: 0.4 + (1.0 - cluster.concentration_score) * 0.6,
                    execution_probability: 0.9 - cluster.volatility_score * 0.3,
                    gas_estimate: 400_000,
                    priority_fee,
                });
            }
        }
        
        None
    }

    fn calculate_optimal_swap_amount(&self, cluster: &TickCluster, pool_data: &PoolState) -> u64 {
        let available_liquidity = cluster.total_liquidity as f64;
        let sqrt_k = available_liquidity.sqrt();
        let price_ratio = self.sqrt_price_to_price(cluster.price_range.1) / self.sqrt_price_to_price(cluster.price_range.0);
        
        let optimal_amount = (sqrt_k * price_ratio.sqrt() * 0.3) as u64;
        optimal_amount.min(pool_data.token_a_amount / 10).max(VOLUME_THRESHOLD)
    }

    fn calculate_jit_liquidity_amount(&self, cluster: &TickCluster, pool_data: &PoolState) -> u128 {
        let current_liquidity = cluster.total_liquidity;
        let target_share = 0.15;
        let jit_liquidity = (current_liquidity as f64 * target_share / (1.0 - target_share)) as u128;
        
        jit_liquidity.min(pool_data.liquidity / 5)
    }

    fn estimate_jit_fee_earnings(&self, liquidity: u128, volume: u64, pool_data: &PoolState) -> u64 {
        let fee_rate = pool_data.fee_rate as f64 / BASIS_POINT_MAX as f64;
        let liquidity_share = liquidity as f64 / (pool_data.liquidity + liquidity) as f64;
        let expected_volume_share = volume as f64 * liquidity_share * 0.8;
        
        (expected_volume_share * fee_rate) as u64
    }

    fn estimate_price_impact(&self, amount_in: u64, liquidity_L: f64) -> f64 {
        if liquidity_L <= 0.0 {
            return 0.5; // worst-case cap
        }
        let dp_over_p = (amount_in as f64) / liquidity_L;
        dp_over_p.clamp(0.0, 0.05) // cap at 5%
    }

    // Exact CLMM impact approximation at current sqrtP (PIECE M2)
    #[inline]
    fn estimate_price_impact_at(&self, amount_in: u64, liquidity_L: f64, sqrt_price_x64: u128) -> f64 {
        if liquidity_L <= 0.0 { return 0.05; }
        let s = (sqrt_price_x64 as f64) / TWO_POW_64_F64; if !(s.is_finite()) || s <= 0.0 { return 0.05; }
        let dx = amount_in as f64;
        let k = dx * s / liquidity_L;
        let denom = (1.0 + k).max(1e-9);
        let s_prime = s / denom;
        let ratio = s_prime / s;
        let impact = 1.0 - (ratio * ratio);
        impact.clamp(0.0, 0.05)
    }

    fn calculate_liquidity_distribution_for(&self, pool: &Pubkey, ticks: &[TickData]) -> MapI32U128 {
        let spacing = self.tick_spacing_for(pool);
        let mut distribution: MapI32U128 = FastHashMap::with_hasher(RandomState::new());
        for tick in ticks {
            let bucket = (tick.tick_index / spacing) * spacing;
            *distribution.entry(bucket).or_insert(0) += tick.liquidity_gross;
        }
        distribution
    }

    fn calculate_price_impact(&self, clusters: &[TickCluster], _pool: &PoolState) -> BTreeMap<i32, f64> {
        let mut m = BTreeMap::new();
        for c in clusters {
            let l = c.total_liquidity as f64;
            let base = self.estimate_price_impact(VOLUME_THRESHOLD, l);
            for offset in -10..=10 {
                let tick = c.center_tick + offset;
                let decay = 1.0 / (1.0 + (offset as f64 * 0.35).powi(2));
                m.insert(tick, (base * decay).clamp(0.0, 0.05));
            }
        }
        m
    }

    // Parallelize independent views (clusters + distribution), then price impact
    fn analyze_views(
        &self,
        pool: &Pubkey,
        active_ticks: &[TickData],
        pool_data: &PoolState,
    ) -> (Vec<TickCluster>, MapI32U128, BTreeMap<i32, f64>) {
        let (clusters, dist) = rayon::join(
            || self.identify_clusters(active_ticks),
            || self.calculate_liquidity_distribution_for(pool, active_ticks),
        );
        let impact = self.calculate_price_impact(&clusters, pool_data);
        (clusters, dist, impact)
    }

    fn find_dominant_cluster(&self, clusters: &[TickCluster]) -> Option<TickCluster> {
        clusters
            .iter()
            .max_by(|a, b| {
                let a_q = to_qbps(a.opportunity_score);
                let b_q = to_qbps(b.opportunity_score);
                let ascore = (a.total_liquidity as f64) * a.opportunity_score;
                let bscore = (b.total_liquidity as f64) * b.opportunity_score;
                ord_by_q_then_float(a_q, b_q, ascore, bscore)
            })
            .cloned()
    }

    fn calculate_cluster_volume(&self, ticks: &[TickData]) -> u64 {
        let fee_growth_sum: u128 = ticks.iter()
            .map(|t| t.fee_growth_outside_a + t.fee_growth_outside_b)
            .sum();
        
        (fee_growth_sum / 1000) as u64
    }

    async fn fetch_bitmap_with_prev(&self, pool: &Pubkey) -> Result<(Vec<u8>, Option<Vec<u8>>), ClusterAnalysisError> {
        let now = now_ms();
        if let Some(e) = self.bitmap_cache.get(pool) {
            if now - e.value().1 <= BITMAP_TTL_MS {
                return Ok((e.value().0.clone(), None));
            }
        }
        let addr = self.derive_tick_array_bitmap_address(pool);
        let acc = self.rpc.get_account_fast(&addr).await?;
        let prev = self.bitmap_cache.insert(*pool, (acc.data.clone(), now)).map(|old| old.0);
        Ok((acc.data, prev))
    }

    #[inline]
    fn iter_set_bits<'a>(&'a self, bitmap: &'a [u8]) -> impl Iterator<Item = i32> + 'a {
        bitmap.iter().enumerate().flat_map(|(byte_idx, &byte)| {
            let mut b = byte;
            let mut v: SmallVec<[i32; 8]> = SmallVec::new();
            while b != 0 {
                let tz = b.trailing_zeros() as u8;
                v.push((byte_idx * 8 + tz as usize) as i32);
                b &= b - 1;
            }
            v.into_iter()
        })
    }

    async fn fetch_active_ticks_batched_diff(
        &self,
        pool: &Pubkey,
        curr_bitmap: &[u8],
        prev_bitmap: Option<&[u8]>,
    ) -> Result<Vec<TickData>, ClusterAnalysisError> {
        let now = now_ms();
        let mut to_fetch: Vec<(Pubkey, i32)> = Vec::with_capacity(64);

        if let Some(prev) = prev_bitmap {
            let len = curr_bitmap.len().min(prev.len());
            let words = len / 8;
            for w in 0..words {
                let c = u64::from_le_bytes(*array_ref![curr_bitmap, w*8, 8]);
                let p = u64::from_le_bytes(*array_ref![prev,        w*8, 8]);
                let mut changed = c ^ p;
                while changed != 0 {
                    let bit = changed.trailing_zeros() as usize;
                    let global_bit = w * 64 + bit;
                    let cur_set = ((c >> bit) & 1) == 1;
                    if !cur_set {
                        let idx = global_bit as i32;
                        self.tick_array_cache.remove(&(*pool, idx));
                    }
                    changed &= changed - 1;
                }
            }
            for i in (words*8)..len {
                let c = curr_bitmap[i];
                let p = prev[i];
                let mut changed = (c ^ p) as u8;
                while changed != 0 {
                    let tz = changed.trailing_zeros() as u8;
                    let bit = tz as usize;
                    let cur_set = (c >> bit) & 1 == 1;
                    if !cur_set {
                        let idx = (i * 8 + bit) as i32;
                        self.tick_array_cache.remove(&(*pool, idx));
                    }
                    changed &= changed - 1;
                }
            }
        }

        for idx in self.iter_set_bits(curr_bitmap) {
            let key = (*pool, idx);
            let stale = self
                .tick_array_cache
                .get(&key)
                .map(|v| now - v.value().1 > TICKARR_TTL_MS)
                .unwrap_or(true);
            if stale {
                to_fetch.push((self.derive_tick_array_address(pool, idx), idx));
            }
        }

        if !to_fetch.is_empty() {
            let addrs: Vec<Pubkey> = to_fetch.iter().map(|(a, _)| *a).collect();
            let accounts = self.rpc.get_multiple_accounts_fast(&addrs).await?;
            let ts = unix_ts_s();

            for ((_, idx), opt) in to_fetch.into_iter().zip(accounts.into_iter()) {
                let ticks = opt
                    .map(|acc| self.parse_tick_array(&acc.data, ts).unwrap_or_default())
                    .unwrap_or_default();
                self.tick_array_cache.insert((*pool, idx), (ticks, now));
            }
        }

        let mut out = Vec::with_capacity(512);
        for idx in self.iter_set_bits(curr_bitmap) {
            if let Some(entry) = self.tick_array_cache.get(&(*pool, idx)) {
                out.extend_from_slice(&entry.value().0);
            }
        }
        Ok(out)
    }

    #[inline]
    fn parse_tick_array(&self, data: &[u8], timestamp: u64) -> Result<Vec<TickData>, ClusterAnalysisError> {
        const HEADER_SIZE: usize = 8;
        const TICK_SIZE: usize = 88;
        if data.len() < HEADER_SIZE { return Ok(Vec::new()); }
        let payload = &data[HEADER_SIZE..];
        let mut out = Vec::with_capacity(payload.len() / TICK_SIZE);
        for chunk in payload.chunks_exact(TICK_SIZE) {
            let tick_index = i32::from_le_bytes(*array_ref![chunk, 0, 4]);
            let sqrt_price_x64 = u128::from_le_bytes(*array_ref![chunk, 4, 16]);
            let liquidity_net =  i128::from_le_bytes(*array_ref![chunk, 20, 16]);
            let liquidity_gross = u128::from_le_bytes(*array_ref![chunk, 36, 16]);
            let fee_out_a = u128::from_le_bytes(*array_ref![chunk, 52, 16]);
            let fee_out_b = u128::from_le_bytes(*array_ref![chunk, 68, 16]);
            if liquidity_gross != 0 {
                out.push(TickData {
                    tick_index,
                    sqrt_price_x64,
                    liquidity_net,
                    liquidity_gross,
                    fee_growth_outside_a: fee_out_a,
                    fee_growth_outside_b: fee_out_b,
                    timestamp,
                    block_height: 0,
                });
            }
        }
        Ok(out)
    }

    fn decode_pool_state(&self, account: &Account) -> Result<PoolState, Box<dyn std::error::Error>> {
        // SAFE1: owner + size guard
        if account.owner != orca_whirlpool::ID {
            return Err("Invalid owner for pool account".into());
        }
        if account.data.len() < 184 {
            return Err("Invalid pool account data".into());
        }
        Ok(PoolState {
            bump: account.data[0],
            token_mint_a: Pubkey::new_from_array(*array_ref![account.data, 1, 32]),
            token_mint_b: Pubkey::new_from_array(*array_ref![account.data, 33, 32]),
            token_vault_a: Pubkey::new_from_array(*array_ref![account.data, 65, 32]),
            token_vault_b: Pubkey::new_from_array(*array_ref![account.data, 97, 32]),
            fee_rate: u16::from_le_bytes(*array_ref![account.data, 129, 2]),
            protocol_fee_rate: u16::from_le_bytes(*array_ref![account.data, 131, 2]),
            liquidity: u128::from_le_bytes(*array_ref![account.data, 133, 16]),
            sqrt_price_x64: u128::from_le_bytes(*array_ref![account.data, 149, 16]),
            tick_current: i32::from_le_bytes(*array_ref![account.data, 165, 4]),
            token_a_amount: u64::from_le_bytes(*array_ref![account.data, 169, 8]),
            token_b_amount: u64::from_le_bytes(*array_ref![account.data, 177, 8]),
        })
    }

    fn derive_tick_array_bitmap_address(&self, pool_address: &Pubkey) -> Pubkey {
        let (address, _) = Pubkey::find_program_address(
            &[b"tick_array_bitmap", pool_address.as_ref()],
            &orca_whirlpool::ID,
        );
        address
    }

    fn derive_tick_array_address(&self, pool_address: &Pubkey, tick_array_idx: i32) -> Pubkey {
        let (address, _) = Pubkey::find_program_address(
            &[
                b"tick_array",
                pool_address.as_ref(),
                &tick_array_idx.to_le_bytes(),
            ],
            &orca_whirlpool::ID,
        );
        address
    }

    fn sqrt_price_to_price(&self, sqrt_price_x64: u128) -> f64 {
        // price = (sqrt_price_x64 / 2^64)^2, clamp to sane domain
        let s = (sqrt_price_x64 as f64) / TWO_POW_64_F64;
        let p = s * s;
        p.clamp(1e-24, 1e24)
    }

    #[inline]
    fn generate_cluster_id_for(&self, pool: &Pubkey, clusters: &[TickCluster], ts: u64) -> u64 {
        let mut acc: u64 = 0;
        for c in clusters {
            let first = c.ticks.first().map(|t| t.tick_index as i64).unwrap_or_default();
            let last = c.ticks.last().map(|t| t.tick_index as i64).unwrap_or_default();
            let bytes = [
                pool.to_bytes().as_slice(),
                &c.center_tick.to_le_bytes(),
                &(c.total_liquidity as u64).to_le_bytes(),
                &(first as i64).to_le_bytes(),
                &(last as i64).to_le_bytes(),
                &ts.to_le_bytes(),
            ]
            .concat();
            acc ^= xxh3_64(&bytes);
        }
        acc
    }

    pub async fn update_price_oracle(&self, token_mint: &Pubkey, price: u64) {
        self.price_oracle.write().insert(*token_mint, price);
    }

    pub fn get_cached_analysis(&self, pool_address: &Pubkey) -> Option<ClusterAnalysis> {
        self.cluster_cache.read().get(pool_address).cloned()
    }

    pub async fn start_continuous_analysis(&self, pool_addresses: Vec<Pubkey>) -> mpsc::Receiver<ClusterAnalysis> {
        let (tx, rx) = mpsc::channel(256);
        let analyzer = Arc::new(self.clone());
        let period = analyzer.analysis_interval;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                let txc = tx.clone();
                let futs = futures::stream::iter(pool_addresses.iter().cloned().map(|pool| {
                    let analyzer = analyzer.clone();
                    async move { analyzer.analyze_pool_ticks(&pool).await.ok() }
                }))
                .buffer_unordered(8)
                .collect::<Vec<_>>();
                let results = futs.await;
                for r in results.into_iter().flatten() {
                    let _ = txc.send(r).await;
                }
            }
        });
        rx
    }
}

#[derive(Debug, Clone)]
struct PoolState {
    bump: u8,
    token_mint_a: Pubkey,
    token_mint_b: Pubkey,
    token_vault_a: Pubkey,
    token_vault_b: Pubkey,
    fee_rate: u16,
    protocol_fee_rate: u16,
    liquidity: u128,
    sqrt_price_x64: u128,
    tick_current: i32,
    token_a_amount: u64,
    token_b_amount: u64,
}

impl Clone for TickClusterAnalyzer {
    fn clone(&self) -> Self {
        Self {
            rpc: Arc::clone(&self.rpc),
            tick_history: Arc::clone(&self.tick_history),
            cluster_cache: Arc::clone(&self.cluster_cache),
            price_oracle: Arc::clone(&self.price_oracle),
            bitmap_cache: self.bitmap_cache.clone(),
            tick_array_cache: self.tick_array_cache.clone(),
            tick_spacing_override: self.tick_spacing_override.clone(),
            default_tick_spacing: self.default_tick_spacing,
            tip_state: RwLock::new(self.tip_state.read().clone()),
            liquidity_threshold: self.liquidity_threshold,
            analysis_interval: self.analysis_interval,
        }
    }
}

mod orca_whirlpool {
    use super::*;
    pub const ID: Pubkey = solana_sdk::pubkey!("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc");
}

// NaN-safe ordering helper for descending order
#[inline]
fn ord_f64_desc(a: f64, b: f64) -> Ordering {
    b.partial_cmp(&a).unwrap_or(Ordering::Equal)
}

// Fixed-point quantization for deterministic ordering (0..=10000 qbps)
#[inline]
fn to_qbps(v: f64) -> u16 {
    let x = (v.clamp(0.0, 1.0) * 10000.0).round() as i64;
    x.clamp(0, 10000) as u16
}

// Order by fixed-point first, then float as tiebreaker
#[inline]
fn ord_by_q_then_float(q_a: u16, q_b: u16, fa: f64, fb: f64) -> Ordering {
    match q_b.cmp(&q_a) {
        Ordering::Equal => ord_f64_desc(fa, fb),
        o => o,
    }
}

// Net PnL scoring function (fee- and tip-aware)
impl TickClusterAnalyzer {
    #[inline]
    fn net_pnl_score(&self, expected_profit: u64, risk: f64, exec_p: f64, tip_budget: u64) -> f64 {
        let ep = expected_profit.saturating_sub(tip_budget) as f64;
        let denom = 1.0 + risk.max(0.0);
        (ep * exec_p) / denom
    }

    #[inline]
    fn jito_tip_budget(&self, ep: u64) -> u64 {
        // ~0.8% of expected profit, floored and capped
        ep.saturating_mul(8)
            .saturating_div(1000)
            .clamp(3_000, 500_000)
    }

    // TIP1: Execution feedback loop to adapt tip EWMA from the execution layer
    #[inline]
    pub fn record_bundle_outcome(&self, success: bool) {
        self.tip_state.write().observe(success);
    }

    // Per-pool tick spacing override API
    pub fn set_tick_spacing(&self, pool: Pubkey, spacing: u16) {
        self.tick_spacing_override.insert(pool, spacing.max(1));
    }

    #[inline]
    fn tick_spacing_for(&self, pool: &Pubkey) -> i32 {
        self
            .tick_spacing_override
            .get(pool)
            .map(|v| *v.value() as i32)
            .unwrap_or(self.default_tick_spacing as i32)
    }

    // Oracle-aware sanity gates
    #[inline]
    fn oracle_deviation_frac(&self, pool: &PoolState) -> Option<f64> {
        let o = self.price_oracle.read();
        let pa = o.get(&pool.token_mint_a)?;
        let pb = o.get(&pool.token_mint_b)?;
        if *pb == 0 {
            return None;
        }
        let oracle_px = (*pa as f64) / (*pb as f64);
        let pool_px = self.sqrt_price_to_price(pool.sqrt_price_x64);
        let dev = ((pool_px - oracle_px).abs() / oracle_px).clamp(0.0, 1.0);
        Some(dev)
    }

    #[inline]
    fn gate_by_oracle(&self, pool: &PoolState, c: &TickCluster) -> bool {
        if let Some(dev) = self.oracle_deviation_frac(pool) {
            let vol = c.volume_24h.max(1) as f64;
            let tol = (0.08 + (vol.ln() / 20.0)).clamp(0.08, 0.20);
            if dev > tol { return false; }
        }
        true
    }
}

// Adaptive Jito tip EWMA (congestion-aware)
#[derive(Default, Clone)]
struct TipEwma {
    alpha: f64,
    s: f64,
}

impl TipEwma {
    fn new(alpha: f64) -> Self {
        Self { alpha, s: 0.7 }
    }
    fn observe(&mut self, success: bool) {
        let x = if success { 1.0 } else { 0.0 };
        self.s = self.alpha * x + (1.0 - self.alpha) * self.s;
    }
    fn tip_multiplier(&self) -> f64 {
        (2.2 - 1.4 * self.s).clamp(0.8, 2.2)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_tick_cluster_identification() {
        let analyzer = TickClusterAnalyzer::new("https://api.mainnet-beta.solana.com");
        
        let test_ticks = vec![
            TickData {
                tick_index: 100,
                sqrt_price_x64: 1000000000000000000,
                liquidity_net: 1000000,
                liquidity_gross: 1000000,
                fee_growth_outside_a: 0,
                fee_growth_outside_b: 0,
                timestamp: 1000,
                block_height: 1000,
            },
            TickData {
                tick_index: 102,
                sqrt_price_x64: 1000100000000000000,
                liquidity_net: 2000000,
                liquidity_gross: 2000000,
                fee_growth_outside_a: 100,
                fee_growth_outside_b: 100,
                timestamp: 1001,
                block_height: 1001,
            },
            TickData {
                tick_index: 104,
                sqrt_price_x64: 1000200000000000000,
                liquidity_net: 3000000,
                liquidity_gross: 3000000,
                fee_growth_outside_a: 200,
                fee_growth_outside_b: 200,
                timestamp: 1002,
                block_height: 1002,
            },
        ];
        
        let clusters = analyzer.identify_clusters(&test_ticks);
        assert!(!clusters.is_empty());
    }
    
    #[test]
    fn test_price_conversion() {
        let analyzer = TickClusterAnalyzer::new("https://api.mainnet-beta.solana.com");
        let sqrt_price_x64: u128 = 79228162514264337593543950336;
        let price = analyzer.sqrt_price_to_price(sqrt_price_x64);
        assert!(price > 0.0);
    }
}

