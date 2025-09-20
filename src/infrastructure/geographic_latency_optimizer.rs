// === ADD: extra imports used by shapers/diversity/helpers ===
use tokio::sync::{mpsc, oneshot, RwLock, Mutex};
use rand::Rng;
use futures::{future::AbortHandle, future::Abortable, FutureExt};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use std::sync::Arc;
use tokio::time::{timeout, interval};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcSendTransactionConfig, RpcSignatureStatusConfig};
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signature::Signature;
use solana_sdk::clock::Slot;
use serde::{Deserialize, Serialize};
use futures::future::join_all;
use rand::seq::SliceRandom;
use statrs::statistics::{Statistics, OrderStatistics};
use futures::future::select_ok;
// === ADD: imports for compute budget ===
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::instruction::Instruction;

#[derive(Copy, Clone, Debug)]
pub enum OpClass { Landing, Sim, Read }

// === ADD: helper: OpClass -> index ===
#[inline]
fn class_ix(op: OpClass) -> usize {
    match op {
        OpClass::Landing => 0,
        OpClass::Sim     => 1,
        OpClass::Read    => 2,
    }
}

// === REPLACE: calc_timeout_budget_ms (use p99_fast) ===
fn calc_timeout_budget_ms(op: OpClass, m: Option<&EndpointMetrics>) -> u64 {
    let base = m.map(|mm| {
        let p99 = mm.p99_fast();
        if p99 > 0.0 { p99 } else { 120.0 }
    }).unwrap_or(120.0);

    let mult = match op {
        OpClass::Landing => 0.9,
        OpClass::Sim     => 1.15,
        OpClass::Read    => 1.05,
    };

    let b = (base * mult) + 10.0;
    let b = b.clamp(40.0, 180.0);
    b as u64
}

// === ADD: robust_mad_clip + provider_penalty ===
fn robust_mad_clip(samples: &[f64]) -> (f64, f64) {
    if samples.is_empty() { return (0.0, 0.0); }
    let mut v = samples.to_vec();
    v.sort_by(|a,b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let med = v[v.len()/2];
    let mut devs: Vec<f64> = v.iter().map(|x| (x - med).abs()).collect();
    devs.sort_by(|a,b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let mad = devs[devs.len()/2];
    (med, mad)
}

// === REPLACE: provider penalty helper (canonical) ===
#[inline]
fn provider_penalty(sr: f64) -> f64 {
    // Slightly convex penalty when provider average SR drops.
    // 1.0 at 100% SR; ~0.92 at 80% SR; floors gently.
    (0.88 + 0.12 * sr.clamp(0.0, 1.0)).powf(2.0)
}

// === REPLACE: RPC error classifier (expanded) ===
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum ErrorClass {
    RateLimit,
    Duplicate,
    BlockhashStale,
    NodeBehind,
    Unavailable,
    TimeoutLike,
    BlockCostLimit,
    AccountInUse,
    WouldExceedVoteCost,
    SimulationFailure,
    Other,
}

#[inline]
fn classify_rpc_error(s: &str) -> ErrorClass {
    let t = s.to_lowercase();
    if t.contains("429") || t.contains("rate limit") || t.contains("too many request") { return ErrorClass::RateLimit; }
    if t.contains("already processed") || t.contains("duplicate") { return ErrorClass::Duplicate; }
    if t.contains("blockhash not found") || t.contains("blockhash not present") { return ErrorClass::BlockhashStale; }
    if t.contains("node is behind") || t.contains("slot not available") || t.contains("blockstore") { return ErrorClass::NodeBehind; }
    if t.contains("service unavailable") || t.contains("unavailable") || t.contains("overloaded") { return ErrorClass::Unavailable; }
    if t.contains("timeout") || t.contains("timed out") { return ErrorClass::TimeoutLike; }
    if t.contains("would exceed max block cost limit") { return ErrorClass::BlockCostLimit; }
    if t.contains("account in use") { return ErrorClass::AccountInUse; }
    if t.contains("would exceed max vote cost") { return ErrorClass::WouldExceedVoteCost; }
    if t.contains("simulation failed") { return ErrorClass::SimulationFailure; }
    ErrorClass::Other
}

// === ADD: P² quantile estimator (p50/p99) ===
#[derive(Clone, Debug)]
struct P2Estimator {
    q: f64,
    init: Vec<f64>,
    n: [i32; 5],
    np: [f64; 5],
    dn: [f64; 5],
    y: [f64; 5],
    ready: bool,
}

impl P2Estimator {
    fn new(q: f64) -> Self {
        Self { q: q.clamp(0.01, 0.99), init: Vec::with_capacity(5),
               n: [0;5], np: [0.0;5], dn: [0.0;5], y: [0.0;5], ready: false }
    }
    #[inline] fn estimate(&self) -> Option<f64> { if self.ready { Some(self.y[2]) } else { None } }
    fn observe(&mut self, x: f64) {
        if !self.ready {
            self.init.push(x);
            if self.init.len() < 5 { return; }
            self.init.sort_by(|a,b| a.partial_cmp(b).unwrap());
            self.y.copy_from_slice(&[self.init[0], self.init[1], self.init[2], self.init[3], self.init[4]]);
            self.n = [1, 2, 3, 4, 5];
            self.np = [1.0,
                       1.0 + 2.0 * self.q,
                       1.0 + 4.0 * self.q,
                       3.0 + 2.0 * self.q,
                       5.0];
            self.dn = [0.0, self.q/2.0, self.q, (1.0+self.q)/2.0, 1.0];
            self.ready = true;
            return;
        }

        // locate cell k
        let mut k = if x < self.y[0] {
            self.y[0] = x; 0
        } else if x >= self.y[4] {
            self.y[4] = x; 3
        } else {
            let mut kk = 0;
            for i in 0..4 { if x >= self.y[i] && x < self.y[i+1] { kk = i; break; } }
            kk
        };

        // update positions
        for i in (k+1)..5 { self.n[i] += 1; }
        for i in 0..5 { self.np[i] += self.dn[i]; }

        // adjust heights y[1..3]
        for i in 1..4 {
            let d = self.np[i] - (self.n[i] as f64);
            if (d >= 1.0 && self.n[i+1] - self.n[i] > 1) || (d <= -1.0 && self.n[i-1] - self.n[i] < -1) {
                let di = d.signum() as i32;
                let yip = self.y[i] + (di as f64) * (
                    (self.y[i+1] - self.y[i]) / ((self.n[i+1]-self.n[i]) as f64) * ((self.n[i]-self.n[i-1]+di) as f64) +
                    (self.y[i] - self.y[i-1]) / ((self.n[i]-self.n[i-1]) as f64) * ((self.n[i+1]-self.n[i]-di) as f64)
                ) / ((self.n[i+1]-self.n[i-1]) as f64);

                // if parabolic step leaves [y[i-1], y[i+1]], do linear
                if yip > self.y[i-1] && yip < self.y[i+1] {
                    self.y[i] = yip;
                } else {
                    self.y[i] += (di as f64) * (self.y[(i as i32 + di) as usize] - self.y[i]) /
                                 ((self.n[(i as i32 + di) as usize] - self.n[i]) as f64);
                }
                self.n[i] += di;
            }
        }
    }
}

// === REPLACE: MultiBucket priority lanes (with base + scaling) ===
#[derive(Debug)]
struct MultiBucket {
    base_landing: u64,
    base_general: u64,
    landing: TokenBucket,
    general: TokenBucket,
}
impl MultiBucket {
    fn new(total_rps: u64) -> Self {
        let total = total_rps.max(2);
        let landing_rps = ((total as f64) * 0.7).round() as u64;
        let landing_rps = landing_rps.max(1);
        let general_rps = (total - landing_rps).max(1);
        Self {
            base_landing: landing_rps,
            base_general: general_rps,
            landing: TokenBucket::new(landing_rps),
            general: TokenBucket::new(general_rps),
        }
    }
    fn try_acquire(&mut self, cls: OpClass) -> bool {
        match cls {
            OpClass::Landing => self.landing.try_acquire(),
            OpClass::Sim | OpClass::Read => self.general.try_acquire(),
        }
    }
    fn set_scale(&mut self, scale: f64) {
        let s = scale.clamp(0.2, 1.0);
        self.landing.set_rates((self.base_landing as f64) * s, (self.base_landing as f64) * s);
        self.general.set_rates((self.base_general as f64) * s, (self.base_general as f64) * s);
    }
    fn reset(&mut self) {
        self.landing.set_rates(self.base_landing as f64, self.base_landing as f64);
        self.general.set_rates(self.base_general as f64, self.base_general as f64);
    }
    fn after_recover(&mut self) { self.set_scale(0.5); }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeographicEndpoint {
    pub url: String,
    pub region: String,
    pub provider: String,
    pub weight: f64,
    pub priority: u8,
    pub max_requests_per_second: u64,
}

// === REPLACE: TokenBucket (impl with scaling) ===
#[derive(Debug)]
struct TokenBucket {
    capacity: f64,
    tokens: f64,
    refill_per_sec: f64,
    last_refill: std::time::Instant,
}

impl TokenBucket {
    fn new(rps: u64) -> Self {
        let cap = rps.max(1) as f64;
        Self { capacity: cap, tokens: cap, refill_per_sec: cap, last_refill: std::time::Instant::now() }
    }
    fn try_acquire(&mut self) -> bool {
        let now = std::time::Instant::now();
        let dt = now.duration_since(self.last_refill).as_secs_f64();
        if dt > 0.0 {
            self.tokens = (self.tokens + dt * self.refill_per_sec).min(self.capacity);
            self.last_refill = now;
        }
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else { false }
    }
    fn set_rates(&mut self, capacity: f64, refill_per_sec: f64) {
        self.capacity = capacity.max(1.0);
        self.refill_per_sec = refill_per_sec.max(1.0);
        if self.tokens > self.capacity { self.tokens = self.capacity; }
    }
}

// === REPLACE: struct EndpointMetrics (now includes P²) ===
#[derive(Debug)]
struct EndpointMetrics {
    latency_samples: std::collections::VecDeque<f64>,
    ewma_p50: f64,
    ewma_p99: f64,
    success_count: std::sync::atomic::AtomicU64,
    failure_count: std::sync::atomic::AtomicU64,
    window_outcomes: std::collections::VecDeque<u8>,

    last_success: std::sync::Mutex<Option<std::time::Instant>>,
    last_failure: std::sync::Mutex<Option<std::time::Instant>>,
    breaker_open_until: std::sync::Mutex<Option<std::time::Instant>>,
    half_open_probe_in_flight: std::sync::atomic::AtomicBool,

    current_requests: std::sync::atomic::AtomicU64,

    inflight_limit: std::sync::atomic::AtomicU64,
    inflight_cap: u64,

    slot_latencies: std::collections::VecDeque<(solana_sdk::clock::Slot, std::time::Duration)>,
    buckets: std::sync::Mutex<MultiBucket>,
    rate_backoff_until: std::sync::Mutex<Option<std::time::Instant>>,
    lag_strikes: std::sync::atomic::AtomicU64,
    timeout_strikes: std::sync::atomic::AtomicU64,

    // === ADD: streaming quantiles
    p2_p50: P2Estimator,
    p2_p99: P2Estimator,
}

// === REPLACE: impl EndpointMetrics::new_with_rps (now includes P²) ===
impl EndpointMetrics {
    fn new_with_rps(rps: u64) -> Self {
        let cap = (rps.max(8)) as u64;
        let start = (cap / 2).max(8);
        Self {
            latency_samples: VecDeque::with_capacity(1024),
            ewma_p50: 0.0,
            ewma_p99: 0.0,
            success_count: AtomicU64::new(0),
            failure_count: AtomicU64::new(0),
            window_outcomes: VecDeque::with_capacity(512),
            last_success: std::sync::Mutex::new(None),
            last_failure: std::sync::Mutex::new(None),
            breaker_open_until: std::sync::Mutex::new(None),
            half_open_probe_in_flight: AtomicBool::new(false),
            current_requests: AtomicU64::new(0),
            inflight_limit: std::sync::atomic::AtomicU64::new(start),
            inflight_cap: cap,
            slot_latencies: VecDeque::with_capacity(512),
            buckets: std::sync::Mutex::new(MultiBucket::new(rps)),
            rate_backoff_until: std::sync::Mutex::new(None),
            lag_strikes: AtomicU64::new(0),
            timeout_strikes: AtomicU64::new(0),
            p2_p50: P2Estimator::new(0.50),
            p2_p99: P2Estimator::new(0.99),
        }
    }
    fn add_latency_sample(&mut self, latency_ms: f64) {
        if self.latency_samples.len() >= 1024 { self.latency_samples.pop_front(); }
        let capped = latency_ms.min(500.0);
        self.latency_samples.push_back(capped);

        // EWMA (stable under burst)
        let a_p50 = 0.25_f64; let a_p99 = 0.15_f64;
        self.ewma_p50 = if self.ewma_p50 == 0.0 { capped } else { a_p50 * capped + (1.0 - a_p50) * self.ewma_p50 };
        self.ewma_p99 = if self.ewma_p99 == 0.0 { capped } else { a_p99 * capped + (1.0 - a_p99) * self.ewma_p99 };

        // P² streaming
        self.p2_p50.observe(capped);
        self.p2_p99.observe(capped);
    }
    fn record_outcome(&mut self, ok: bool) {
        if self.window_outcomes.len() >= 512 { self.window_outcomes.pop_front(); }
        self.window_outcomes.push_back(if ok { 1 } else { 0 });
        if ok { self.success_count.fetch_add(1, Ordering::Relaxed); }
        else { self.failure_count.fetch_add(1, Ordering::Relaxed); }
    }
    fn rolling_success_rate(&self) -> f64 {
        if self.window_outcomes.is_empty() { return self.get_success_rate(); }
        let s: u64 = self.window_outcomes.iter().map(|&x| x as u64).sum();
        s as f64 / (self.window_outcomes.len() as f64)
    }
    fn get_percentile(&self, percentile: f64) -> Option<f64> { // legacy fallback
        if self.latency_samples.is_empty() { return None; }
        let mut sorted: Vec<f64> = self.latency_samples.iter().cloned().collect();
        sorted.sort_by(|a,b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let index = ((percentile / 100.0) * (sorted.len() - 1) as f64) as usize;
        sorted.get(index).cloned()
    }

    #[inline] fn p50_fast(&self) -> f64 {
        if self.ewma_p50 > 0.0 { self.ewma_p50 }
        else { self.p2_p50.estimate().or_else(|| self.get_percentile(50.0)).unwrap_or(100.0) }
    }
    #[inline] fn p99_fast(&self) -> f64 {
        if self.ewma_p99 > 0.0 { self.ewma_p99 }
        else { self.p2_p99.estimate().or_else(|| self.get_percentile(99.0)).unwrap_or(160.0) }
    }
    fn get_success_rate(&self) -> f64 {
        let s = self.success_count.load(Ordering::Relaxed) as f64;
        let f = self.failure_count.load(Ordering::Relaxed) as f64;
        let tot = s + f;
        if tot == 0.0 { 0.0 } else { s / tot }
    }
    fn breaker_is_open(&self) -> bool {
        if let Some(until) = *self.breaker_open_until.lock().unwrap() {
            return std::time::Instant::now() < until;
        }
        false
    }
    fn open_breaker_for(&self, dur: Duration) {
        *self.breaker_open_until.lock().unwrap() = Some(std::time::Instant::now() + dur);
        self.half_open_probe_in_flight.store(false, Ordering::Relaxed);
    }
    fn maybe_allow_half_open_probe(&self) -> bool {
        let mut guard = self.breaker_open_until.lock().unwrap();
        let now = std::time::Instant::now();
        if guard.map(|t| now >= t).unwrap_or(false) {
            if !self.half_open_probe_in_flight.swap(true, Ordering::Relaxed) {
                *guard = None;
                return true;
            }
        }
        false
    }
    fn try_acquire_for(&self, cls: OpClass) -> bool {
        self.maybe_restore_caps();
        if self.current_requests.load(Ordering::Relaxed) >= self.inflight_limit.load(Ordering::Relaxed) { return false; }
        let mut b = self.buckets.lock().unwrap();
        b.try_acquire(cls)
    }
    fn ramp_after_recover(&self) {
        self.buckets.lock().unwrap().after_recover();
        // reset AIMD window to safe baseline
        let base = (self.inflight_cap / 2).max(8);
        self.inflight_limit.store(base, Ordering::Relaxed);
    }
    fn apply_rate_limit_backoff(&self, _dur: Duration, scale: f64) {
        *self.rate_backoff_until.lock().unwrap() = Some(std::time::Instant::now() + Duration::from_secs(5));
        let mut b = self.buckets.lock().unwrap();
        b.set_scale(scale);
        // also curb inflight window slightly
        let cur = self.inflight_limit.load(Ordering::Relaxed);
        let dec = ((cur as f64) * 0.8).max(4.0) as u64;
        self.inflight_limit.store(dec, Ordering::Relaxed);
    }
    fn maybe_restore_caps(&self) {
        let mut guard = self.rate_backoff_until.lock().unwrap();
        if let Some(until) = *guard {
            if std::time::Instant::now() >= until {
                let mut b = self.buckets.lock().unwrap();
                b.reset();
                *guard = None;
            }
        }
    }
    // AIMD hooks
    fn aimd_on_success(&self) {
        let cur = self.inflight_limit.load(Ordering::Relaxed);
        let cap = self.inflight_cap;
        if cur < cap { self.inflight_limit.fetch_add(1, Ordering::Relaxed); }
    }
    fn aimd_on_timeout(&self) {
        let cur = self.inflight_limit.load(Ordering::Relaxed);
        let dec = ((cur as f64) * 0.7).max(4.0) as u64;
        self.inflight_limit.store(dec, Ordering::Relaxed);
    }
    fn aimd_on_error(&self) {
        let cur = self.inflight_limit.load(Ordering::Relaxed);
        let dec = ((cur as f64) * 0.9).max(4.0) as u64;
        self.inflight_limit.store(dec, Ordering::Relaxed);
    }
}

// === ADD: provider bucket wrapper ===
#[derive(Debug)]
struct ProviderBucket {
    base_rps: u64,
    bucket: TokenBucket,
}

impl ProviderBucket {
    fn new(base_rps: u64) -> Self {
        let r = base_rps.max(1);
        Self { base_rps: r, bucket: TokenBucket::new(r) }
    }
    fn set_scale(&mut self, scale: f64) {
        let s = scale.clamp(0.2, 1.0);
        let rate = (self.base_rps as f64) * s;
        self.bucket.set_rates(rate, rate);
    }
    fn try_acquire(&mut self) -> bool { self.bucket.try_acquire() }
}

// === REPLACE: struct GeographicLatencyOptimizer (adds region_shapers) ===
pub struct GeographicLatencyOptimizer {
    endpoints: Arc<RwLock<HashMap<String, GeographicEndpoint>>>,
    metrics:   Arc<RwLock<HashMap<String, EndpointMetrics>>>,
    rpc_clients: Arc<RwLock<HashMap<String, Arc<RpcClient>>>>,

    primary_region: String,
    fallback_regions: Vec<String>,

    latency_threshold_ms: f64,
    circuit_breaker_threshold: f64,
    circuit_breaker_timeout: Duration,
    sample_window: Duration,
    health_check_interval: Duration,

    // Provider incident controls
    provider_cooldown: Arc<RwLock<HashMap<String, Instant>>>,
    provider_backoff:  Arc<RwLock<HashMap<String, u32>>>,

    // Provider-wide shapers
    provider_shapers: Arc<RwLock<HashMap<String, std::sync::Mutex<ProviderBucket>>>>,

    // === ADD: region-wide shapers ===
    region_shapers: Arc<RwLock<HashMap<String, std::sync::Mutex<ProviderBucket>>>>,

    // === ADD: region cooldown & backoff ===
    region_cooldown: Arc<RwLock<HashMap<String, Instant>>>,
    region_backoff:  Arc<RwLock<HashMap<String, u32>>>,

    // === REPLACE: per-class fast winners: 0=Landing, 1=Sim, 2=Read ===
    last_fast_winners: Arc<RwLock<Vec<Option<(String, Instant)>>>>,

    // Batched signature status
    sig_req_tx: mpsc::UnboundedSender<(Signature, oneshot::Sender<bool>)>,

    // Confirmed signature cache
    confirmed_sig_cache: Arc<RwLock<HashMap<Signature, Instant>>>,

    // === ADD: send coalescer to dedupe concurrent identical sends ===
    send_coalesce: Arc<Mutex<HashMap<Signature, Vec<oneshot::Sender<Result<(), String>>>>>>,
}

impl GeographicLatencyOptimizer {
    // === REPLACE: GeographicLatencyOptimizer::new (adds region_shapers init) ===
    pub async fn new(
        endpoints: Vec<GeographicEndpoint>,
        primary_region: String,
        fallback_regions: Vec<String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut endpoint_map = HashMap::new();
        let mut metrics_map  = HashMap::new();
        let mut rpc_clients_map = HashMap::new();
        let mut prov_sum: HashMap<String, u64> = HashMap::new();
        let mut region_sum: HashMap<String, u64> = HashMap::new();

        for endpoint in endpoints {
            let client = Arc::new(RpcClient::new_with_commitment(
                endpoint.url.clone(),
                CommitmentConfig::confirmed(),
            ));
            *prov_sum.entry(endpoint.provider.clone()).or_insert(0) += endpoint.max_requests_per_second;
            *region_sum.entry(endpoint.region.clone()).or_insert(0) += endpoint.max_requests_per_second;
            endpoint_map.insert(endpoint.url.clone(), endpoint.clone());
            metrics_map.insert(endpoint.url.clone(), EndpointMetrics::new_with_rps(endpoint.max_requests_per_second));
            rpc_clients_map.insert(endpoint.url.clone(), client);
        }

        let (sig_req_tx, sig_req_rx) = mpsc::unbounded_channel::<(Signature, oneshot::Sender<bool>)>();

        let provider_shapers = prov_sum.into_iter()
            .map(|(p, rps)| (p, std::sync::Mutex::new(ProviderBucket::new(rps))))
            .collect::<HashMap<_, _>>();

        let region_shapers = region_sum.into_iter()
            .map(|(r, rps)| (r, std::sync::Mutex::new(ProviderBucket::new(rps))))
            .collect::<HashMap<_, _>>();

        let optimizer = Self {
            endpoints: Arc::new(RwLock::new(endpoint_map)),
            metrics:   Arc::new(RwLock::new(metrics_map)),
            rpc_clients: Arc::new(RwLock::new(rpc_clients_map)),
            primary_region,
            fallback_regions,
            latency_threshold_ms: 50.0,
            circuit_breaker_threshold: 0.5,
            circuit_breaker_timeout: Duration::from_secs(15),
            sample_window: Duration::from_secs(300),
            health_check_interval: Duration::from_secs(10),
            provider_cooldown: Arc::new(RwLock::new(HashMap::new())),
            provider_backoff:  Arc::new(RwLock::new(HashMap::new())),
            provider_shapers:  Arc::new(RwLock::new(provider_shapers)),
            region_shapers:    Arc::new(RwLock::new(region_shapers)),
            region_cooldown:   Arc::new(RwLock::new(HashMap::new())),
            region_backoff:    Arc::new(RwLock::new(HashMap::new())),
            // per-class vector length = 3, all None
            last_fast_winners: Arc::new(RwLock::new(vec![None, None, None])),
            sig_req_tx,
            confirmed_sig_cache: Arc::new(RwLock::new(HashMap::new())),
            send_coalesce: Arc::new(Mutex::new(HashMap::new())),
        };

        optimizer.start_health_monitoring();
        optimizer.start_latency_monitoring();
        optimizer.start_capacity_controller();
        optimizer.start_sig_status_batcher(sig_req_rx);
        optimizer.start_signature_cache_sweeper(Duration::from_secs(60), Duration::from_secs(180));

        Ok(optimizer)
    }

// === ADD: capacity controller (adaptive scaling every ~300ms) ===
impl GeographicLatencyOptimizer {
    fn start_capacity_controller(&self) {
        let metrics = Arc::clone(&self.metrics);
        tokio::spawn(async move {
            let mut itv = tokio::time::interval(Duration::from_millis(300));
            loop {
                itv.tick().await;
                // 0–3ms jitter to desync controllers across processes
                let j = (rand::random::<u64>() % 4);
                if j > 0 { tokio::time::sleep(Duration::from_millis(j)).await; }

                let mut mw = metrics.write().await;
                for (_url, m) in mw.iter_mut() {
                    let sr = m.rolling_success_rate();
                    let p99 = if m.ewma_p99 > 0.0 { m.ewma_p99 } else { m.get_percentile(99.0).unwrap_or(180.0) };
                    let lat_pen = (120.0 / (120.0 + p99.max(1.0))).clamp(0.3, 1.0);
                    let target_scale = (0.35 + 0.65 * sr).clamp(0.2, 1.0) * lat_pen;

                    m.buckets.lock().unwrap().set_scale(target_scale);

                    let tgt = ((m.inflight_cap as f64) * target_scale).max(4.0) as u64;
                    let cur = m.inflight_limit.load(Ordering::Relaxed);
                    if tgt > cur { m.inflight_limit.fetch_add(1, Ordering::Relaxed); }
                    else if tgt + 1 < cur { m.inflight_limit.store(cur - 1, Ordering::Relaxed); }
                }
            }
        });
    }
}

// === ADD: helper: reverse-lookup URL for a client ===
impl GeographicLatencyOptimizer {
    async fn url_of_client(&self, client: &Arc<RpcClient>) -> Option<String> {
        let clients = self.rpc_clients.read().await;
        for (url, c) in clients.iter() {
            if Arc::ptr_eq(c, client) {
                return Some(url.clone());
            }
        }
        None
    }
}

impl GeographicLatencyOptimizer {
    // === ADD: exponential provider cooldown helpers ===
    async fn cooldown_provider_for(&self, provider: &str, dur: Duration) {
        let mut cd = self.provider_cooldown.write().await;
        cd.insert(provider.to_string(), Instant::now() + dur);
    }

    async fn cooldown_provider_escalate(&self, provider: &str) {
        let mut levels = self.provider_backoff.write().await;
        let lvl = levels.entry(provider.to_string()).or_insert(0);
        *lvl = (*lvl + 1).min(6); // cap
        let secs = match *lvl {
            0 => 4, 1 => 8, 2 => 12, 3 => 20, 4 => 30, 5 => 45, _ => 60
        };
        drop(levels);
        // scale provider shaper (heavier backoff → lower scale)
        if let Some(m) = self.provider_shapers.write().await.get_mut(provider) {
            let mut g = m.lock().unwrap();
            let scale = match secs { 4=>0.9,8=>0.8,12=>0.7,20=>0.6,30=>0.5,45=>0.4,_=>0.35 };
            g.set_scale(scale);
        }
        self.cooldown_provider_for(provider, Duration::from_secs(secs)).await;
    }

    async fn cooldown_provider_decay(&self, provider: &str) {
        let mut levels = self.provider_backoff.write().await;
        if let Some(v) = levels.get_mut(provider) {
            if *v > 0 { *v -= 1; }
        }
        // relax shaper toward 1.0
        if let Some(m) = self.provider_shapers.write().await.get_mut(provider) {
            let mut g = m.lock().unwrap();
            g.set_scale(1.0);
        }
    }

    // === ADD: provider shaper acquire ===
    async fn provider_try_acquire(&self, provider: &str) -> bool {
        if let Some(m) = self.provider_shapers.write().await.get_mut(provider) {
            return m.lock().unwrap().try_acquire();
        }
        true
    }

    // === ADD: region cooldown/backoff helpers (mirror provider logic) ===
    async fn region_cooldown_for(&self, region: &str, dur: Duration) {
        let mut cd = self.region_cooldown.write().await;
        cd.insert(region.to_string(), Instant::now() + dur);
    }

    async fn region_cooldown_escalate(&self, region: &str) {
        let mut levels = self.region_backoff.write().await;
        let lvl = levels.entry(region.to_string()).or_insert(0);
        *lvl = (*lvl + 1).min(6);
        let secs = match *lvl { 0=>4,1=>8,2=>12,3=>18,4=>26,5=>40,_=>55 };
        drop(levels);
        if let Some(m) = self.region_shapers.write().await.get_mut(region) {
            let mut g = m.lock().unwrap();
            let scale = match secs { 4=>0.9,8=>0.8,12=>0.7,18=>0.6,26=>0.5,40=>0.45,_=>0.4 };
            g.set_scale(scale);
        }
        self.region_cooldown_for(region, Duration::from_secs(secs)).await;
    }

    async fn region_cooldown_decay(&self, region: &str) {
        let mut levels = self.region_backoff.write().await;
        if let Some(v) = levels.get_mut(region) {
            if *v > 0 { *v -= 1; }
        }
        if let Some(m) = self.region_shapers.write().await.get_mut(region) {
            let mut g = m.lock().unwrap();
            g.set_scale(1.0);
        }
    }

    // === ADD: region shaper helpers + diversity selector ===
    async fn region_try_acquire(&self, region: &str) -> bool {
        if let Some(m) = self.region_shapers.write().await.get_mut(region) {
            return m.lock().unwrap().try_acquire();
        }
        true
    }

    // === REPLACE: select_diverse_candidates (correlation-aware) ===
    pub async fn select_diverse_candidates(&self, want: usize) -> Vec<Arc<RpcClient>> {
        use std::collections::HashSet;
        if want == 0 { return Vec::new(); }

        let endpoints = self.endpoints.read().await;
        let metrics   = self.metrics.read().await;
        let clients   = self.rpc_clients.read().await;

        // global tip
        let mut global_max: Slot = 0;
        for (_u, m) in metrics.iter() {
            if let Some(mx) = m.slot_latencies.iter().map(|(s, _)| *s).max() {
                if mx > global_max { global_max = mx; }
            }
        }

        // pre-score
        let mut items: Vec<(&String, f64)> = Vec::new();
        for (url, ep) in endpoints.iter() {
            if let Some(m) = metrics.get(url) {
                if m.breaker_is_open() || m.slot_latencies.len() < 8 { continue; }
                let max_slot_here = m.slot_latencies.iter().map(|(s, _)| *s).max().unwrap_or(0);
                let lead_delta = global_max.saturating_sub(max_slot_here);
                if lead_delta > 6 { continue; }

                let p50 = m.p50_fast();
                let sr  = m.rolling_success_rate().max(m.get_success_rate());
                let latency = 1.0 / (1.0 + p50 / 28.0);
                let reliability = sr.powf(2.2);
                let capacity = {
                    let cur = m.current_requests.load(Ordering::Relaxed) as f64;
                    1.0 - (cur / (ep.max_requests_per_second as f64)).clamp(0.0, 0.95)
                };
                let lead_boost = if lead_delta <= 1 { 1.2 } else if lead_delta <= 3 { 1.08 } else { 1.02 };
                let score = (latency * 0.36 + reliability * 0.36 + capacity * 0.28) * lead_boost;
                items.push((url, score));
            }
        }
        items.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // greedy pick with provider/region + correlation penalty
        let mut picked: Vec<String> = Vec::new();
        let mut seen_prov = HashSet::new();
        let mut seen_reg  = HashSet::new();

        for (url, base_score) in items {
            if picked.len() >= want { break; }
            let ep = endpoints.get(url).unwrap();
            let mut score = *base_score;

            // diversity boosts
            if !seen_prov.contains(&ep.provider) { score *= 1.06; }
            if !seen_reg.contains(&ep.region) { score *= 1.04; }

            // correlation penalty against already-picked
            let mut corr_pen = 1.0;
            for sel in picked.iter() {
                if let (Some(m1), Some(m2)) = (metrics.get(url.as_str()), metrics.get(sel.as_str())) {
                    let sim = outcome_similarity(&m1.window_outcomes, &m2.window_outcomes);
                    // penalize high similarity: if sim=0.8 → penalty ~ 0.84 (aggressive)
                    corr_pen *= (1.0 - 0.2 * sim).clamp(0.80, 1.0);
                }
            }
            score *= corr_pen;

            // accept if still competitive
            if score.is_finite() && score > 0.0 {
                picked.push(url.clone());
                seen_prov.insert(ep.provider.clone());
                seen_reg.insert(ep.region.clone());
            }
        }

        let mut out = Vec::new();
        for u in picked {
            if let Some(c) = clients.get(&u) { out.push(Arc::clone(c)); }
        }
        out
    }

// === ADD: similarity helper (0..1), where 1.0 == identical outcomes ===
#[inline]
fn outcome_similarity(a: &std::collections::VecDeque<u8>, b: &std::collections::VecDeque<u8>) -> f64 {
    if a.is_empty() || b.is_empty() { return 0.0; }
    let n = a.len().min(b.len()).min(128);
    let mut same = 0usize;
    for i in 0..n {
        if a[a.len()-1-i] == b[b.len()-1-i] { same += 1; }
    }
    (same as f64) / (n as f64)
}

    // === REPLACE: get_optimal_client_for (add region cooldown check) ===
    pub async fn get_optimal_client_for(&self, cls: OpClass) -> Result<Arc<RpcClient>, Box<dyn std::error::Error>> {
        let endpoints = self.endpoints.read().await;
        let metrics   = self.metrics.read().await;
        let clients   = self.rpc_clients.read().await;
        let provider_cooldowns = self.provider_cooldown.read().await;
        let region_cooldowns   = self.region_cooldown.read().await;
        // FAST-LANE: reuse last class-specific winner if fresh & healthy
        if let Some(Some((ref url, ts))) = self.last_fast_winners.read().await.get(class_ix(cls)).cloned() {
            if ts.elapsed() <= Duration::from_millis(1000) {
                if let (Some(ep), Some(m), Some(c)) = (endpoints.get(url), metrics.get(url), clients.get(url)) {
                    if !provider_cooldowns.get(ep.provider.as_str()).map(|&u| Instant::now() < u).unwrap_or(false) &&
                       !region_cooldowns.get(ep.region.as_str()).map(|&u| Instant::now() < u).unwrap_or(false) &&
                       !m.breaker_is_open() && m.try_acquire_for(cls) && self.provider_try_acquire(&ep.provider).await && self.region_try_acquire(&ep.region).await {
                        return Ok(Arc::clone(c));
                    }
                }
            }
        }

        let last_win = self.last_fast_winners.read().await.get(class_ix(cls)).cloned().unwrap_or(None);

        // global tip
        let mut global_max: Slot = 0;
        for (_u, m) in metrics.iter() {
            if let Some((s, _)) = m.slot_latencies.iter().max_by_key(|(s, _)| *s) {
                if *s > global_max { global_max = *s; }
            }
        }

        // provider SR
        let mut accum: HashMap<&str, (f64,u64)> = HashMap::new();
        for (url, ep) in endpoints.iter() {
            if let Some(m) = metrics.get(url) {
                let e = accum.entry(ep.provider.as_str()).or_insert((0.0,0));
                e.0 += m.rolling_success_rate(); e.1 += 1;
            }
        }
        let mut prov_sr: HashMap<&str, f64> = HashMap::new();
        for (p,(sum,cnt)) in accum { prov_sr.insert(p, (sum/(cnt as f64)).clamp(0.0,1.0)); }

        let mut candidates: Vec<(&String, &GeographicEndpoint, f64)> = Vec::new();
        for (url, ep) in endpoints.iter() {
            if provider_cooldowns.get(ep.provider.as_str()).map(|&u| Instant::now() < u).unwrap_or(false) { continue; }
            if region_cooldowns.get(ep.region.as_str()).map(|&u| Instant::now() < u).unwrap_or(false) { continue; }
            if let Some(m) = metrics.get(url) {
                if m.breaker_is_open() && !m.maybe_allow_half_open_probe() { continue; }
                if m.rolling_success_rate() < (1.0 - self.circuit_breaker_threshold) { continue; }
                if !m.try_acquire_for(cls) { continue; }
                // provider/region shapers
                if !self.provider_try_acquire(&ep.provider).await { continue; }
                if !self.region_try_acquire(&ep.region).await { continue; }

                let p50 = if m.ewma_p50 > 0.0 { m.ewma_p50 } else { m.get_percentile(50.0).unwrap_or(f64::MAX) };
                let p99 = if m.ewma_p99 > 0.0 { m.ewma_p99 } else { m.get_percentile(99.0).unwrap_or(f64::MAX) };
                if p99.is_finite() && p99 > 220.0 { continue; }

                let sr  = m.rolling_success_rate().max(m.get_success_rate());
                let current_load = m.current_requests.load(Ordering::Relaxed) as f64;
                let max_load = ep.max_requests_per_second as f64;
                let load_factor = (1.0 - (current_load / max_load)).clamp(0.05, 0.95);

                let mut score = calculate_endpoint_score(
                    p50, p99, sr, load_factor, ep.priority, &ep.region, &self.primary_region,
                );

                let samples: Vec<f64> = m.latency_samples.iter().cloned().collect();
                if !samples.is_empty() {
                    let (med, mad) = robust_mad_clip(&samples);
                    score *= (1.0 / (1.0 + (mad / (med + 1.0)))).clamp(0.7, 1.05);
                }

                if let Some(psr) = prov_sr.get(ep.provider.as_str()) { score *= provider_penalty(*psr); }
                if let Some((max_here, _)) = m.slot_latencies.iter().max_by_key(|(s, _)| *s) {
                    let gap = global_max.saturating_sub(*max_here) as f64;
                    if gap > 0.0 { score *= (1.0 - (0.03 * gap).min(0.30)); }
                }
                if let Some((ref last_url, ts)) = last_win {
                    if last_url == url && ts.elapsed() <= Duration::from_millis(800) { score *= 1.05; }
                }

                candidates.push((url, ep, score));
            }
        }

        candidates.sort_by(|a,b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        if candidates.is_empty() { return Err("No available endpoints".into()); }

        let pick_url = if candidates.len() >= 2 {
            let best=candidates[0].2; let second=candidates[1].2;
            let close = second >= best * 0.92;
            let mut rng = rand::thread_rng();
            if close && rng.gen_bool(0.05) { candidates[1].0 } else { candidates[0].0 }
        } else { candidates[0].0 };

        if let Some(client) = clients.get(*pick_url) { return Ok(Arc::clone(client)); }
        Err("No available endpoints".into())
    }

    pub async fn get_optimal_client(&self) -> Result<Arc<RpcClient>, Box<dyn std::error::Error>> {
        self.get_optimal_client_for(OpClass::Read).await
    }

    // === REPLACE: get_regional_clients ===
    pub async fn get_regional_clients(&self, region: &str) -> Vec<Arc<RpcClient>> {
        let endpoints = self.endpoints.read().await;
        let metrics = self.metrics.read().await;
        let clients = self.rpc_clients.read().await;
        let mut out = Vec::new();

        for (url, endpoint) in endpoints.iter() {
            if endpoint.region == region {
                if let Some(m) = metrics.get(url) {
                    if !m.breaker_is_open() {
                        if let Some(c) = clients.get(url) {
                            out.push(Arc::clone(c));
                        }
                    }
                }
            }
        }
        out
    }

    // === REPLACE: hedged_execute (p50-aware, per-endpoint delays) ===
    pub async fn hedged_execute<F, T>(
        &self,
        operation: F,
        fanout: usize,
        initial_delay_ms: u64,
        min_stagger_ms: u64,
    ) -> Result<T, Box<dyn std::error::Error>>
    where
        F: Fn(Arc<RpcClient>) -> futures::future::BoxFuture<'static, Result<T, Box<dyn std::error::Error>>> + Clone + Send + Sync + 'static,
        T: Send + 'static,
    {
        let chosen = self.select_diverse_candidates(fanout.max(1)).await;
        if chosen.is_empty() { return Err("no clients".into()); }

        let ms_read = self.metrics.read().await;
        let mut scored: Vec<(Arc<RpcClient>, f64, u64)> = Vec::new(); // (client, p50, budget)

        for c in chosen.into_iter() {
            let url = self.url_of_client(&c).await.unwrap_or_default();
            let p50 = ms_read.get(&url).map(|m| if m.ewma_p50 > 0.0 { m.ewma_p50 } else { m.get_percentile(50.0).unwrap_or(60.0) }).unwrap_or(60.0);
            let budget = ms_read.get(&url).map(|m| calc_timeout_budget_ms(OpClass::Landing, Some(m))).unwrap_or(120);
            scored.push((c, p50, budget));
        }
        drop(ms_read);

        // best p50 first (lowest delay), tail gets slightly earlier offsets
        scored.sort_by(|a,b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

        let mut aborts: Vec<AbortHandle> = Vec::with_capacity(scored.len());
        let mut futs = Vec::with_capacity(scored.len());
        let mut rng = rand::thread_rng();

        for (rank, (client, _p50, budget_ms)) in scored.into_iter().enumerate() {
            // delay grows slowly with rank, bounded by p50-aware stagger
            let base = (min_stagger_ms as f64 * (rank as f64).sqrt()).round() as u64;
            let jitter = rng.gen_range(0..3);
            let delay = initial_delay_ms + base + jitter;

            let op = operation.clone();
            let (ah, reg) = AbortHandle::new_pair();
            aborts.push(ah);
            let fut = async move {
                if delay > 0 { tokio::time::sleep(Duration::from_millis(delay)).await; }
                timeout(Duration::from_millis(budget_ms), op(client)).await
                    .map_err(|_| "hedged timeout".into())?
            };
            futs.push(Abortable::new(fut, reg));
        }

        let res = futures::future::select_ok(futs).await;
        for ah in aborts { ah.abort(); }
        match res {
            Ok((ok, _)) => ok,
            Err(e) => Err(format!("all hedged ops failed: {}", e).into()),
        }
    }

    // === REPLACE: execute_with_optimal_latency ===
    pub async fn execute_with_optimal_latency<F, T>(
        &self,
        operation: F,
    ) -> Result<T, Box<dyn std::error::Error>>
    where
        F: Fn(Arc<RpcClient>) -> futures::future::BoxFuture<'static, Result<T, Box<dyn std::error::Error>>> + Clone,
        T: Send + 'static,
    {
        self.execute_classed_with_optimal_latency(OpClass::Read, operation).await
    }

    // === REPLACE: execute_classed_with_optimal_latency (fixed locks + strikes) ===
    pub async fn execute_classed_with_optimal_latency<F, T>(
        &self,
        op_class: OpClass,
        operation: F,
    ) -> Result<T, Box<dyn std::error::Error>>
    where
        F: Fn(Arc<RpcClient>) -> futures::future::BoxFuture<'static, Result<T, Box<dyn std::error::Error>>> + Clone,
        T: Send + 'static,
    {
        let client = self.get_optimal_client_for(op_class).await?;
        let url = self.url_of_client(&client).await.unwrap_or_default();

        let ms_read = self.metrics.read().await;
        let budget_ms = ms_read.get(&url).map(|m| calc_timeout_budget_ms(op_class, Some(m))).unwrap_or(100);
        drop(ms_read);

        {
            let mut mw = self.metrics.write().await;
            if let Some(m) = mw.get_mut(&url) {
                m.current_requests.fetch_add(1, Ordering::Relaxed);
            }
        }

        let start = Instant::now();
        let result = tokio::time::timeout(Duration::from_millis(budget_ms), operation(client.clone())).await;
        let latency_ms = start.elapsed().as_secs_f64() * 1000.0;

        let mut success_winner = false;
        let mut want_provider_escalate = false;
        let mut want_region_escalate = false;

        let mut mw = self.metrics.write().await;
        if let Some(m) = mw.get_mut(&url) {
            m.add_latency_sample(latency_ms);
            match &result {
                Ok(Ok(_)) => {
                    m.record_outcome(true);
                    m.aimd_on_success();
                    m.timeout_strikes.store(0, Ordering::Relaxed);
                    *m.last_success.lock().unwrap() = Some(Instant::now());
                    if m.half_open_probe_in_flight.swap(false, Ordering::Relaxed) {
                        *m.breaker_open_until.lock().unwrap() = None;
                        m.ramp_after_recover();
                    }
                    success_winner = true;
                }
                Ok(Err(e)) => {
                    m.record_outcome(false);
                    m.aimd_on_error();
                    *m.last_failure.lock().unwrap() = Some(Instant::now());
                    m.timeout_strikes.store(0, Ordering::Relaxed);
                    match classify_rpc_error(&e.to_string()) {
                        ErrorClass::RateLimit => { m.apply_rate_limit_backoff(Duration::from_secs(5), 0.5); want_provider_escalate = true; }
                        ErrorClass::Duplicate => { success_winner = true; } // soft-ok
                        ErrorClass::NodeBehind => { m.open_breaker_for(Duration::from_secs(30)); want_region_escalate = true; }
                        ErrorClass::Unavailable => { want_provider_escalate = true; }
                        _ => {}
                    }
                    if m.rolling_success_rate() < (1.0 - self.circuit_breaker_threshold) {
                        m.open_breaker_for(self.circuit_breaker_timeout);
                    }
                }
                Err(_) => {
                    m.record_outcome(false);
                    m.aimd_on_timeout();
                    *m.last_failure.lock().unwrap() = Some(Instant::now());
                    let strikes = m.timeout_strikes.fetch_add(1, Ordering::Relaxed) + 1;
                    if strikes >= 2 {
                        m.open_breaker_for(Duration::from_secs(30));
                        m.timeout_strikes.store(0, Ordering::Relaxed);
                    }
                    if m.rolling_success_rate() < (1.0 - self.circuit_breaker_threshold) {
                        m.open_breaker_for(self.circuit_breaker_timeout);
                    }
                }
            }
            m.current_requests.fetch_sub(1, Ordering::Relaxed);
        }
        drop(mw);

        if success_winner {
            let ci = class_ix(op_class);
            let mut lw = self.last_fast_winners.write().await;
            if lw.len() <= ci { lw.resize(ci+1, None); }
            lw[ci] = Some((url.clone(), Instant::now()));
            if let Some(ep) = self.endpoints.read().await.get(&url) {
                self.cooldown_provider_decay(&ep.provider).await;
                self.region_cooldown_decay(&ep.region).await;
            }
        } else if let Some(ep) = self.endpoints.read().await.get(&url) {
            if want_provider_escalate { self.cooldown_provider_escalate(&ep.provider).await; }
            if want_region_escalate   { self.region_cooldown_escalate(&ep.region).await; }
        }

        match result {
            Ok(Ok(v)) => Ok(v),
            Ok(Err(e)) => Err(e),
            Err(_) => Err("Request timeout".into()),
        }
    }

    pub async fn hedged_execute_classed<F, T>(
        &self,
        op_class: OpClass,
        operation: F,
        max_fanout: usize,
        initial_delay_ms: u64,
        base_stagger_ms: u64,
    ) -> Result<T, Box<dyn std::error::Error>>
    where
        F: Fn(Arc<RpcClient>) -> futures::future::BoxFuture<'static, Result<T, Box<dyn std::error::Error>>> + Clone + Send + Sync + 'static,
        T: Send + 'static,
    {
        // Choose fanout adaptively using success-rate dispersion
        let endpoints = self.endpoints.read().await;
        let metrics = self.metrics.read().await;
        let clients = self.rpc_clients.read().await;

        // Score top endpoints via landing optimizer first
        let top = self.optimize_for_transaction_landing().await;
        let mut chosen: Vec<Arc<RpcClient>> = if top.is_empty() {
            vec![self.get_optimal_client().await?]
        } else {
            top
        };

        // Compute dispersion: low rolling SR -> widen fanout
        let mut avg_sr = 0.0;
        let mut cnt = 0.0;
        for c in chosen.iter() {
            if let Some(url) = self.url_of_client(c).await {
                if let Some(m) = metrics.get(&url) {
                    avg_sr += m.rolling_success_rate();
                    cnt += 1.0;
                }
            }
        }
        let mean_sr = if cnt > 0.0 { avg_sr / cnt } else { 0.8 };
        let want_fanout = if mean_sr >= 0.92 { 1 } else if mean_sr >= 0.85 { 2 } else { 3 }.min(max_fanout.max(1));
        chosen.truncate(want_fanout.max(1));

        // Launch hedged futures with per-branch budget
        let mut futs = Vec::with_capacity(chosen.len());
        for (i, client) in chosen.into_iter().enumerate() {
            let url = self.url_of_client(&client).await.unwrap_or_default();
            let m_opt = metrics.get(&url);
            let budget_ms = calc_timeout_budget_ms(op_class, m_opt);
            let delay = initial_delay_ms + (i as u64) * (if budget_ms >= 140 { (base_stagger_ms/2).max(6) } else { base_stagger_ms });

            let op = operation.clone();
            let fut = async move {
                if delay > 0 { tokio::time::sleep(Duration::from_millis(delay)).await; }
                tokio::time::timeout(Duration::from_millis(budget_ms), op(client)).await
                    .map_err(|_| "hedged timeout".into())?
            };
            futs.push(Box::pin(fut));
        }
        drop(endpoints); drop(metrics); drop(clients);

        let (ok, _rest) = futures::future::select_ok(futs).await
            .map_err(|e| format!("all hedged ops failed: {}", e))?;
        ok
    }

    // === REPLACE: parallel_query ===
    pub async fn parallel_query<F, T>(
        &self,
        operation: F,
        regions: Vec<String>,
    ) -> Result<T, Box<dyn std::error::Error>>
    where
        F: Fn(Arc<RpcClient>) -> futures::future::BoxFuture<'static, Result<T, Box<dyn std::error::Error>>> + Clone + Send + Sync + 'static,
        T: Send + 'static,
    {
        let mut aborts: Vec<AbortHandle> = Vec::new();
        let mut futs = Vec::new();

        for region in regions {
            let clients = self.get_regional_clients(&region).await;
            if let Some(client) = clients.first() {
                let op = operation.clone();
                let c = client.clone();
                let (ah, reg) = AbortHandle::new_pair();
                aborts.push(ah);
                let f = async move {
                    tokio::time::timeout(Duration::from_millis(100), op(c)).await
                        .map_err(|_| "parallel timeout".into())?
                };
                futs.push(Abortable::new(f, reg));
            }
        }

        let res = futures::future::select_ok(futs).await;
        for ah in aborts { ah.abort(); }
        match res {
            Ok((ok, _)) => ok,
            Err(e) => Err(format!("all parallel queries failed: {}", e).into()),
        }
    }

    pub async fn parallel_query_classed<F, T>(
        &self,
        op_class: OpClass,
        operation: F,
        regions: Vec<String>,
    ) -> Result<T, Box<dyn std::error::Error>>
    where
        F: Fn(Arc<RpcClient>) -> futures::future::BoxFuture<'static, Result<T, Box<dyn std::error::Error>>> + Clone + Send + Sync + 'static,
        T: Send + 'static,
    {
        let metrics = self.metrics.read().await;

        let mut futs = Vec::new();
        for region in regions {
            let clients = self.get_regional_clients(&region).await;
            if let Some(client) = clients.first() {
                let url = self.url_of_client(&client).await.unwrap_or_default();
                let m_opt = metrics.get(&url);
                let budget_ms = calc_timeout_budget_ms(op_class, m_opt);

                let op = operation.clone();
                let c = client.clone();
                let f = async move {
                    tokio::time::timeout(Duration::from_millis(budget_ms), op(c)).await
                        .map_err(|_| "parallel timeout".into())?
                };
                futs.push(Box::pin(f));
            }
        }
        drop(metrics);

        let (ok, _rest) = futures::future::select_ok(futs).await
            .map_err(|e| format!("all parallel queries failed: {}", e))?;
        ok
    }

    async fn record_request_outcome(&self, url: &str, success: bool, latency: f64) {
        let mut metrics = self.metrics.write().await;
        
        if let Some(metric) = metrics.get_mut(url) {
            metric.add_latency_sample(latency);
            
            if success {
                metric.success_count.fetch_add(1, Ordering::Relaxed);
                *metric.last_success.lock().unwrap() = Some(Instant::now());
            } else {
                metric.failure_count.fetch_add(1, Ordering::Relaxed);
                *metric.last_failure.lock().unwrap() = Some(Instant::now());
            }
            
            metric.current_requests.fetch_sub(1, Ordering::Relaxed);
        }
    }

    async fn handle_endpoint_failure(&self, url: &str) {
        let ms = self.metrics.read().await;
        if let Some(m) = ms.get(url) {
            // Use rolling window rather than lifetime success rate
            let roll_sr = m.rolling_success_rate();
            if roll_sr < (1.0 - self.circuit_breaker_threshold) {
                m.open_breaker_for(self.circuit_breaker_timeout);
            }
        }
    }

    fn start_health_monitoring(&self) {
        let endpoints = Arc::clone(&self.endpoints);
        let metrics = Arc::clone(&self.metrics);
        let clients = Arc::clone(&self.rpc_clients);
        let interval_duration = self.health_check_interval;

        tokio::spawn(async move {
            let mut itv = interval(interval_duration);
            loop {
                itv.tick().await;

                // Small jitter to avoid alignment with other jobs
                let jitter_ms = (rand::random::<u64>() % 10) + 1;
                tokio::time::sleep(Duration::from_millis(jitter_ms)).await;

                let endpoints_read = endpoints.read().await;
                let clients_read = clients.read().await;

                for (url, _) in endpoints_read.iter() {
                    if let Some(client) = clients_read.get(url) {
                        let client_clone = Arc::clone(client);
                        let metrics_clone = Arc::clone(&metrics);
                        let url_clone = url.clone();

                        tokio::spawn(async move {
                            let started = Instant::now();
                            // Prefer get_health (faster path); fallback to get_slot
                            let res_health = timeout(Duration::from_millis(40), client_clone.get_health()).await;
                            let ok = match res_health {
                                Ok(Ok(_)) => true,
                                _ => match timeout(Duration::from_millis(60), client_clone.get_slot()).await {
                                    Ok(Ok(_)) => true,
                                    _ => false,
                                },
                            };
                            let lat_ms = started.elapsed().as_secs_f64() * 1000.0;

                            let mut mw = metrics_clone.write().await;
                            if let Some(m) = mw.get_mut(&url_clone) {
                                m.add_latency_sample(lat_ms);
                                m.record_outcome(ok);
                                if ok {
                                    *m.last_success.lock().unwrap() = Some(Instant::now());
                                } else {
                                    *m.last_failure.lock().unwrap() = Some(Instant::now());
                                }
                            }
                        });
                    }
                }
            }
        });
    }

    // === REPLACE: start_latency_monitoring (adds regional incident detector) ===
    fn start_latency_monitoring(&self) {
        let metrics = Arc::clone(&self.metrics);
        let clients = Arc::clone(&self.rpc_clients);
        let endpoints = Arc::clone(&self.endpoints);
        let region_cooldown_clone = Arc::clone(&self.region_cooldown);
        let region_backoff_clone  = Arc::clone(&self.region_backoff);

        tokio::spawn(async move {
            let mut itv = interval(Duration::from_secs(5));
            loop {
                itv.tick().await;

                let clients_read = clients.read().await;
                let mut slot_latencies: HashMap<String, Vec<(Slot, Duration)>> = HashMap::new();

                for (url, client) in clients_read.iter() {
                    let client_clone = Arc::clone(client);
                    let url_clone = url.clone();
                    let start = Instant::now();
                    if let Ok(Ok(slot)) = timeout(Duration::from_millis(100), client_clone.get_slot()).await {
                        let latency = start.elapsed();
                        slot_latencies.entry(url_clone).or_insert_with(Vec::new).push((slot, latency));
                    }
                }

                // update per-endpoint slot latency deques
                let mut global_max: Slot = 0;
                for (_u, v) in slot_latencies.iter() {
                    for (s, _) in v.iter() { if *s > global_max { global_max = *s; } }
                }

                // region lag tallies
                let eps = endpoints.read().await;
                let mut region_lag_counts: HashMap<String, usize> = HashMap::new();
                let mut region_total: HashMap<String, usize> = HashMap::new();

                let mut mw = metrics.write().await;
                for (url, latencies) in slot_latencies {
                    if let Some(m) = mw.get_mut(&url) {
                        for (slot, latency) in latencies {
                            if m.slot_latencies.len() >= 512 { m.slot_latencies.pop_front(); }
                            m.slot_latencies.push_back((slot, latency));
                        }
                        if let Some((max_here, _)) = m.slot_latencies.iter().max_by_key(|(s, _)| *s) {
                            let gap = global_max.saturating_sub(*max_here);
                            if let Some(ep) = eps.get(&url) {
                                *region_total.entry(ep.region.clone()).or_insert(0) += 1;
                                if gap > 8 {
                                    *region_lag_counts.entry(ep.region.clone()).or_insert(0) += 1;
                                    let _ = m.lag_strikes.fetch_add(1, Ordering::Relaxed);
                                    if m.lag_strikes.load(Ordering::Relaxed) >= 3 && !m.breaker_is_open() {
                                        m.open_breaker_for(Duration::from_secs(45));
                                    }
                                } else {
                                    m.lag_strikes.store(0, Ordering::Relaxed);
                                }
                            }
                        }
                    }
                }
                drop(mw);

                // regional escalation/decay decision
                for (region, total) in region_total.iter() {
                    let lag = *region_lag_counts.get(region).unwrap_or(&0);
                    let frac = (lag as f64) / (*total as f64);
                    if frac >= 0.5 {
                        // escalate region
                        let mut rb = region_backoff_clone.write().await; drop(rb);
                        // call helper via a short-lived self-style block
                    }
                }
                // We can't call methods on self here, so perform cooldown map updates directly:
                {
                    let mut cd = region_cooldown_clone.write().await;
                    for (region, total) in region_total.iter() {
                        let lag = *region_lag_counts.get(region).unwrap_or(&0);
                        let frac = (lag as f64) / (*total as f64);
                        if frac >= 0.5 {
                            // extend cooldown window (idempotent)
                            cd.insert(region.clone(), Instant::now() + Duration::from_secs(18));
                        } else {
                            // let decay happen naturally when pickers succeed
                        }
                    }
                }
            }
        });
    }

    // === REPLACE: get_best_endpoint_for_slot ===
    pub async fn get_best_endpoint_for_slot(&self, target_slot: Slot) -> Option<String> {
        let metrics = self.metrics.read().await;
        let endpoints = self.endpoints.read().await;

        let mut best_endpoint = None;
        let mut best_score = f64::MIN;

        for (url, metric) in metrics.iter() {
            if metric.breaker_is_open() { continue; }

            let recent_slots: Vec<Slot> = metric.slot_latencies.iter().map(|(slot, _)| *slot).collect();
            if recent_slots.is_empty() { continue; }
            let max_slot = *recent_slots.iter().max().unwrap();
            let slot_freshness = if max_slot > target_slot {
                1.0
            } else {
                1.0 - ((target_slot - max_slot) as f64 / 100.0).min(0.9)
            };
            let avg_latency = recent_slots.len() as f64;
            let avg_latency_ms = metric.slot_latencies
                .iter().map(|(_, d)| d.as_secs_f64() * 1000.0).collect::<Vec<f64>>().mean();

            if let Some(endpoint) = endpoints.get(url) {
                let latency_score = 1.0 / (1.0 + avg_latency_ms / 10.0);
                let success_rate = metric.get_success_rate();
                let current_load = metric.current_requests.load(Ordering::Relaxed) as f64;
                let capacity_score = 1.0 - (current_load / endpoint.max_requests_per_second as f64).min(0.95);
                let score = slot_freshness * 0.4 + latency_score * 0.3 + success_rate * 0.2 + capacity_score * 0.1;
                let _ = avg_latency; // avoid unused var warning
                if score > best_score { best_score = score; best_endpoint = Some(url.clone()); }
            }
        }
        best_endpoint
    }

    pub async fn get_latency_stats(&self, url: &str) -> Option<LatencyStats> {
        let metrics = self.metrics.read().await;
        
        if let Some(metric) = metrics.get(url) {
            let samples: Vec<f64> = metric.latency_samples.iter().cloned().collect();
            
            if samples.is_empty() {
                return None;
            }
            
            Some(LatencyStats {
                p50: metric.get_percentile(50.0).unwrap_or(0.0),
                p90: metric.get_percentile(90.0).unwrap_or(0.0),
                p95: metric.get_percentile(95.0).unwrap_or(0.0),
                p99: metric.get_percentile(99.0).unwrap_or(0.0),
                mean: samples.mean(),
                std_dev: samples.std_dev(),
                min: samples.min(),
                max: samples.max(),
                success_rate: metric.get_success_rate(),
                sample_count: samples.len(),
            })
        } else {
            None
        }
    }

    pub async fn adjust_weights_based_on_performance(&self) {
        let metrics = self.metrics.read().await;
        let mut endpoints = self.endpoints.write().await;
        
        for (url, endpoint) in endpoints.iter_mut() {
            if let Some(metric) = metrics.get(url) {
                let p50 = metric.get_percentile(50.0).unwrap_or(100.0);
                let success_rate = metric.get_success_rate();
                
                let performance_score = (1.0 / (1.0 + p50 / 50.0)) * success_rate;
                
                endpoint.weight = endpoint.weight * 0.9 + performance_score * 0.1;
                endpoint.weight = endpoint.weight.max(0.1).min(1.0);
            }
        }
    }

    pub async fn get_region_performance(&self) -> HashMap<String, RegionPerformance> {
        let endpoints = self.endpoints.read().await;
        let metrics = self.metrics.read().await;
        
        let mut region_stats: HashMap<String, Vec<f64>> = HashMap::new();
        let mut region_success: HashMap<String, (u64, u64)> = HashMap::new();
        
        for (url, endpoint) in endpoints.iter() {
            if let Some(metric) = metrics.get(url) {
                let samples: Vec<f64> = metric.latency_samples.iter().cloned().collect();
                region_stats.entry(endpoint.region.clone()).or_insert_with(Vec::new).extend(samples);
                
                let success = metric.success_count.load(Ordering::Relaxed);
                let failure = metric.failure_count.load(Ordering::Relaxed);
                let (s, f) = region_success.entry(endpoint.region.clone()).or_insert((0, 0));
                *s += success;
                *f += failure;
            }
        }
        
        let mut performance_map = HashMap::new();
        
        for (region, latencies) in region_stats {
            if let Some((success, failure)) = region_success.get(&region) {
                let success_rate = if success + failure > 0 {
                    *success as f64 / (*success + *failure) as f64
                } else {
                    0.0
                };
                
                performance_map.insert(region.clone(), RegionPerformance {
                    region,
                    avg_latency: latencies.mean(),
                    p99_latency: calculate_percentile(&latencies, 99.0),
                    success_rate,
                    sample_count: latencies.len(),
                });
            }
        }
        
        performance_map
    }

    pub async fn failover_to_best_region(&self) -> Result<String, Box<dyn std::error::Error>> {
        let region_perf = self.get_region_performance().await;
        
        let mut regions: Vec<(&String, &RegionPerformance)> = region_perf.iter().collect();
        regions.sort_by(|a, b| {
            let score_a = a.1.success_rate * (1.0 / (1.0 + a.1.avg_latency / 50.0));
            let score_b = b.1.success_rate * (1.0 / (1.0 + b.1.avg_latency / 50.0));
            score_b.partial_cmp(&score_a).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        if let Some((best_region, _)) = regions.first() {
            Ok((*best_region).clone())
        } else {
            Err("No available regions".into())
        }
    }

    pub async fn optimize_for_transaction_landing(&self) -> Vec<Arc<RpcClient>> {
        let endpoints = self.endpoints.read().await;
        let metrics = self.metrics.read().await;
        let clients = self.rpc_clients.read().await;

        // global tip
        let mut global_max: Slot = 0;
        for (_url, m) in metrics.iter() {
            if let Some(mx) = m.slot_latencies.iter().map(|(s, _)| *s).max() {
                if mx > global_max { global_max = mx; }
            }
        }

        let mut items: Vec<(Arc<RpcClient>, f64, String, String)> = Vec::new(); // client, score, provider, region

        for (url, endpoint) in endpoints.iter() {
            if let Some(m) = metrics.get(url) {
                if m.breaker_is_open() || m.slot_latencies.len() < 8 { continue; }
                let slots: Vec<Slot> = m.slot_latencies.iter().map(|(s, _)| *s).collect();
                let max_slot_here = *slots.iter().max().unwrap_or(&0);
                let lead_delta = global_max.saturating_sub(max_slot_here);
                if lead_delta > 6 { continue; } // too far behind

                let slot_variance = calculate_slot_variance(&slots);
                let p50 = if m.ewma_p50 > 0.0 { m.ewma_p50 } else { m.get_percentile(50.0).unwrap_or(100.0) };
                let sr  = m.rolling_success_rate().max(m.get_success_rate());

                let consistency = 1.0 / (1.0 + slot_variance);
                let latency = 1.0 / (1.0 + p50 / 28.0);
                let reliability = sr.powf(2.2);
                let capacity = {
                    let cur = m.current_requests.load(Ordering::Relaxed) as f64;
                    1.0 - (cur / (endpoint.max_requests_per_second as f64)).clamp(0.0, 0.95)
                };
                let lead_boost = match lead_delta {
                    0 | 1 => 1.22,
                    2 => 1.12,
                    3 => 1.06,
                    4..=6 => 1.03,
                    _ => 1.0,
                };

                let composite = (consistency * 0.31 + latency * 0.31 + reliability * 0.24 + capacity * 0.14) * lead_boost;

                if let Some(c) = clients.get(url) {
                    items.push((Arc::clone(c), composite, endpoint.provider.clone(), endpoint.region.clone()));
                }
            }
        }

        items.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        // Greedy diversity pick: aim 3 with distinct providers first
        use std::collections::HashSet;
        let mut picked: Vec<Arc<RpcClient>> = Vec::new();
        let mut seen_provider = HashSet::new();
        let mut seen_region = HashSet::new();

        for (c, _s, prov, reg) in items.iter() {
            if picked.len() >= 3 { break; }
            if seen_provider.insert(prov.clone()) && seen_region.insert(reg.clone()) {
                picked.push(c.clone());
            }
        }

        // fill if needed
        if picked.len() < 3 {
            for (c, _s, _prov, _reg) in items.into_iter() {
                if picked.len() >= 3 { break; }
                if !picked.iter().any(|p| Arc::ptr_eq(p, &c)) {
                    picked.push(c);
                }
            }
        }

        picked
    }

    // === REPLACE: reset_circuit_breaker (reset AIMD baseline too) ===
    pub async fn reset_circuit_breaker(&self, url: &str) {
        let metrics = self.metrics.read().await;
        if let Some(metric) = metrics.get(url) {
            *metric.breaker_open_until.lock().unwrap() = None;
            metric.half_open_probe_in_flight.store(false, Ordering::Relaxed);
            metric.success_count.store(0, Ordering::Relaxed);
            metric.failure_count.store(0, Ordering::Relaxed);
            metric.lag_strikes.store(0, Ordering::Relaxed);
            let base = (metric.inflight_cap / 2).max(8);
            metric.inflight_limit.store(base, Ordering::Relaxed);
            metric.buckets.lock().unwrap().after_recover();
        }
    }

    pub async fn get_endpoint_status(&self) -> Vec<EndpointStatus> {
        let endpoints = self.endpoints.read().await;
        let metrics = self.metrics.read().await;
        let mut status_list = Vec::new();

        for (url, endpoint) in endpoints.iter() {
            if let Some(m) = metrics.get(url) {
                let status = EndpointStatus {
                    url: url.clone(),
                    region: endpoint.region.clone(),
                    is_healthy: !m.breaker_is_open(),
                    current_load: m.current_requests.load(Ordering::Relaxed),
                    success_rate: m.rolling_success_rate(),
                    avg_latency: m.latency_samples.iter().cloned().collect::<Vec<f64>>().mean(),
                    p99_latency: m.get_percentile(99.0).unwrap_or(m.ewma_p99),
                };
                status_list.push(status);
            }
        }
        status_list
    }

    pub async fn quorum_get_latest_blockhash(
        &self,
        k: usize,
    ) -> Result<solana_sdk::hash::Hash, Box<dyn std::error::Error>> {
        use std::collections::HashMap;

        let candidates = self.optimize_for_transaction_landing().await;
        let mut chosen = candidates.into_iter().take(k.max(1)).collect::<Vec<_>>();
        if chosen.is_empty() {
            chosen.push(self.get_optimal_client().await?);
        }

        let mut futs = Vec::new();
        for c in chosen.into_iter() {
            futs.push(async move {
                let h = tokio::time::timeout(Duration::from_millis(120), c.get_latest_blockhash()).await?;
                h.map_err(|e| e.into())
            });
        }

        let results = futures::future::join_all(futs).await.into_iter().filter_map(|r| r.ok()).collect::<Vec<_>>();
        if results.is_empty() { return Err("quorum: no results".into()); }

        // Count equal hashes and pick majority; else fall back to first
        let mut freq: HashMap<String, (usize, solana_sdk::hash::Hash)> = HashMap::new();
        for h in results {
            let s = format!("{:?}", h);
            let e = freq.entry(s).or_insert((0, h));
            e.0 += 1;
        }
        let mut vec = freq.into_iter().collect::<Vec<_>>();
        vec.sort_by(|a, b| b.1.0.cmp(&a.1.0));
        Ok(vec.first().unwrap().1.1)
    }

    pub async fn try_execute_read_shed<F, T>(
        &self,
        system_load_threshold: f64,
        operation: F,
    ) -> Result<T, Box<dyn std::error::Error>>
    where
        F: Fn(Arc<RpcClient>) -> futures::future::BoxFuture<'static, Result<T, Box<dyn std::error::Error>>> + Clone,
        T: Send + 'static,
    {
        // Estimate system load as sum(current_requests / max_rps) across endpoints
        let endpoints = self.endpoints.read().await;
        let metrics = self.metrics.read().await;

        let mut load_sum = 0.0;
        let mut denom = 0.0;
        for (url, ep) in endpoints.iter() {
            if let Some(m) = metrics.get(url) {
                let cur = m.current_requests.load(Ordering::Relaxed) as f64;
                let cap = (ep.max_requests_per_second as f64).max(1.0);
                load_sum += (cur / cap).min(1.0);
                denom += 1.0;
            }
        }
        drop(endpoints); drop(metrics);

        let sys_load = if denom > 0.0 { load_sum / denom } else { 0.0 };
        if sys_load >= system_load_threshold {
            return Err("shed: system hot, dropping low-priority read".into());
        }
        self.execute_with_optimal_latency(operation).await
    }

    // === REPLACE: conditional hedging for landing ===
    pub async fn conditional_hedged_execute_classed<F, T>(
        &self,
        operation: F,
        max_fanout: usize,
        trigger_frac: f64,
        base_stagger_ms: u64,
    ) -> Result<T, Box<dyn std::error::Error>>
    where
        F: Fn(Arc<RpcClient>) -> futures::future::BoxFuture<'static, Result<T, Box<dyn std::error::Error>>> + Clone + Send + Sync + 'static,
        T: Send + 'static,
    {
        let mut top = self.optimize_for_transaction_landing().await;
        if top.is_empty() {
            top.push(self.get_optimal_client_for(OpClass::Landing).await?);
        }
        let primary = top.remove(0);

        let url = self.url_of_client(&primary).await.unwrap_or_default();
        let ms_read = self.metrics.read().await;
        let m_opt = ms_read.get(&url);
        let budget_ms = calc_timeout_budget_ms(OpClass::Landing, m_opt);
        drop(ms_read);

        let backups = self.select_diverse_candidates(max_fanout.saturating_sub(1)).await;

        let (p_abort, p_reg) = AbortHandle::new_pair();
        let op1 = operation.clone();
        let fut_primary = Abortable::new(async move {
            tokio::time::timeout(Duration::from_millis(budget_ms), op1(primary)).await
                .map_err(|_| "primary timeout".into())?
        }, p_reg);

        let op_b = operation.clone();
        let fut_backups = async move {
            let trigger_ms = ((budget_ms as f64) * trigger_frac).clamp(6.0, (budget_ms as f64) * 0.6) as u64;
            tokio::time::sleep(Duration::from_millis(trigger_ms)).await;

            let mut futs = Vec::new();
            for (i, c) in backups.into_iter().enumerate() {
                let (ah, reg) = AbortHandle::new_pair();
                let op = op_b.clone();
                let delay = (i as u64) * base_stagger_ms.max(6);
                let f = async move {
                    if delay > 0 { tokio::time::sleep(Duration::from_millis(delay)).await; }
                    tokio::time::timeout(Duration::from_millis((budget_ms/2).max(60)), op(c)).await
                        .map_err(|_| "backup timeout".into())?
                };
                futs.push(Abortable::new(f, reg));
                // drop ah: Abortable cancels on select_ok completion
                let _ = ah;
            }
            futures::future::select_ok(futs).await
        };

        let res = futures::future::select(Box::pin(fut_primary), Box::pin(fut_backups)).await;
        match res {
            futures::future::Either::Left((Ok(v), _)) => Ok(v),
            futures::future::Either::Left((Err(_), r)) => {
                match r.await { Ok((v, _)) => Ok(v), Err(e) => Err(format!("all hedged failed: {}", e).into()) }
            }
            futures::future::Either::Right((Ok((v, _)), _)) => { p_abort.abort(); Ok(v) }
            futures::future::Either::Right((Err(e), _)) => { p_abort.abort(); Err(format!("all hedged failed: {}", e).into()) }
        }
    }

    // === ADD: compute provider rolling SR (O(n), cheap) ===
    async fn compute_provider_sr(&self, provider: &str) -> f64 {
        let endpoints = self.endpoints.read().await;
        let metrics = self.metrics.read().await;

        let mut ok: f64 = 0.0;
        let mut tot: f64 = 0.0;
        for (url, ep) in endpoints.iter() {
            if ep.provider == provider {
                if let Some(m) = metrics.get(url) {
                    let sr = m.rolling_success_rate();
                    ok += sr;
                    tot += 1.0;
                }
            }
        }
        if tot == 0.0 { 1.0 } else { (ok / tot).clamp(0.0, 1.0) }
    }


    // === ADD: confirm_signature_hedged ===
    pub async fn confirm_signature_hedged(
        &self,
        sig: Signature,
        max_wait_ms: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        use futures::future::{AbortHandle, Abortable};

        let deadline = Instant::now() + Duration::from_millis(max_wait_ms);
        let mut sleep_ms = 20u64;

        loop {
            if Instant::now() >= deadline { return Err("confirm timeout".into()); }

            // Fire hedged status queries across diverse top endpoints
            let candidates = self.select_diverse_candidates(3).await;
            if candidates.is_empty() {
                // fallback single
                let c = self.get_optimal_client_for(OpClass::Read).await?;
                let st = tokio::time::timeout(Duration::from_millis(80), c.get_signature_statuses(&[sig])).await??;
                if st.value.get(0).and_then(|o| o.as_ref()).is_some() { return Ok(()); }
            } else {
                let mut aborts: Vec<AbortHandle> = Vec::new();
                let mut futs = Vec::new();
                for c in candidates.into_iter() {
                    let (ah, reg) = AbortHandle::new_pair();
                    aborts.push(ah);
                    let f = async move {
                        tokio::time::timeout(Duration::from_millis(80), c.get_signature_statuses(&[sig])).await
                            .map_err(|_| "status timeout".into())?
                    };
                    futs.push(Abortable::new(f, reg));
                }
                match futures::future::select_ok(futs).await {
                    Ok((ok, _)) => {
                        for ah in aborts { ah.abort(); }
                        if ok.value.get(0).and_then(|o| o.as_ref()).is_some() { return Ok(()); }
                    }
                    Err(_) => { for ah in aborts { ah.abort(); } }
                }
            }

            tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
            sleep_ms = (sleep_ms + 10).min(60);
        }
    }
}

#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub p50: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
    pub mean: f64,
    pub std_dev: f64,
    pub min: f64,
    pub max: f64,
    pub success_rate: f64,
    pub sample_count: usize,
}

#[derive(Debug, Clone)]
pub struct RegionPerformance {
    pub region: String,
    pub avg_latency: f64,
    pub p99_latency: f64,
    pub success_rate: f64,
    pub sample_count: usize,
}

#[derive(Debug, Clone)]
pub struct EndpointStatus {
    pub url: String,
    pub region: String,
    pub is_healthy: bool,
    pub current_load: u64,
    pub success_rate: f64,
    pub avg_latency: f64,
    pub p99_latency: f64,
}

// === REPLACE: blockhash coalescer ===
#[derive(Debug)]
pub struct BlockhashCoalescer {
    cache: RwLock<Option<(solana_sdk::hash::Hash, Instant)>>,
    in_flight: tokio::sync::Mutex<Option<oneshot::Receiver<solana_sdk::hash::Hash>>>,
}
impl BlockhashCoalescer {
    pub fn new() -> Self {
        Self { cache: RwLock::new(None), in_flight: tokio::sync::Mutex::new(None) }
    }
    pub async fn get(&self, opt: &GeographicLatencyOptimizer, ttl_ms: u64) -> Result<solana_sdk::hash::Hash, Box<dyn std::error::Error>> {
        if let Some((h, t)) = *self.cache.read().await {
            if t.elapsed().as_millis() as u64 <= ttl_ms { return Ok(h); }
        }
        {
            let mut infl = self.in_flight.lock().await;
            if let Some(rx) = infl.take() {
                drop(infl);
                let h = rx.await?;
                return Ok(h);
            } else {
                let (tx, rx) = oneshot::channel::<solana_sdk::hash::Hash>();
                *infl = Some(rx);
                drop(infl);
                let h = opt.quorum_get_latest_blockhash(3).await?;
                let _ = tx.send(h);
                let mut w = self.cache.write().await;
                *w = Some((h, Instant::now()));
                let mut infl2 = self.in_flight.lock().await;
                *infl2 = None;
                return Ok(h);
            }
        }
    }
}

// === REPLACE: calculate_endpoint_score ===
fn calculate_endpoint_score(
    p50: f64,
    p99: f64,
    success_rate: f64,
    load_factor: f64,
    priority: u8,
    endpoint_region: &str,
    primary_region: &str,
) -> f64 {
    let adj_p99 = p99.min(p50 * 6.0 + 60.0);
    let latency_term = 1.0 / (1.0 + (p50 / 18.0) + (adj_p99 / 85.0));
    let reliability = (success_rate.clamp(0.0, 1.0)).powf(3.5);
    let capacity = load_factor.clamp(0.05, 1.0).powf(0.6);
    let priority_boost = 1.0 + (priority as f64 * 0.06);
    let region_boost = if endpoint_region == primary_region { 1.35 } else { 1.0 };
    (latency_term * 0.42 + reliability * 0.40 + capacity * 0.18) * priority_boost * region_boost
}

fn calculate_percentile(samples: &[f64], percentile: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    
    let index = ((percentile / 100.0) * (sorted.len() - 1) as f64) as usize;
    sorted.get(index).copied().unwrap_or(0.0)
}

fn calculate_slot_variance(slots: &[Slot]) -> f64 {
    if slots.len() < 2 {
        return 0.0;
    }
    
    let diffs: Vec<f64> = slots.windows(2)
        .map(|w| (w[1] as i64 - w[0] as i64).abs() as f64)
        .collect();
    
    if diffs.is_empty() {
        return 0.0;
    }
    
    let mean_diff = diffs.iter().sum::<f64>() / diffs.len() as f64;
    let variance = diffs.iter()
        .map(|d| (d - mean_diff).powi(2))
        .sum::<f64>() / diffs.len() as f64;
    
    variance.sqrt()
}

// === REPLACE: start_sig_status_batcher (std::sync::Mutex locks fixed) ===
impl GeographicLatencyOptimizer {
    fn start_sig_status_batcher(&self, mut rx: mpsc::UnboundedReceiver<(Signature, oneshot::Sender<bool>)>) {
        let endpoints = Arc::clone(&self.endpoints);
        let metrics   = Arc::clone(&self.metrics);
        let clients   = Arc::clone(&self.rpc_clients);
        let cache     = Arc::clone(&self.confirmed_sig_cache);

        tokio::spawn(async move {
            const FLUSH_MS: u64 = 4;
            const MAX_BATCH: usize = 64;
            let mut pending: HashMap<Signature, Vec<oneshot::Sender<bool>>> = HashMap::new();

            loop {
                let item = match rx.recv().await { Some(x) => x, None => break };
                let (sig, tx) = item;

                if cache.read().await.get(&sig).is_some() {
                    let _ = tx.send(true);
                } else {
                    pending.entry(sig).or_default().push(tx);
                }

                let deadline = Instant::now() + Duration::from_millis(FLUSH_MS);
                while pending.len() < MAX_BATCH && Instant::now() < deadline {
                    match rx.try_recv() {
                        Ok((s, t)) => {
                            if cache.read().await.get(&s).is_some() {
                                let _ = t.send(true);
                            } else {
                                pending.entry(s).or_default().push(t);
                            }
                        }
                        Err(_) => tokio::time::sleep(Duration::from_millis(1)).await,
                    }
                }
                if pending.is_empty() { continue; }

                // pick best read endpoint
                let (chosen_url, budget_ms) = {
                    let eps = endpoints.read().await;
                    let ms  = metrics.read().await;

                    let mut global_max: Slot = 0;
                    for (_u, m) in ms.iter() {
                        if let Some((s,_)) = m.slot_latencies.iter().max_by_key(|(s,_)| *s) {
                            if *s > global_max { global_max = *s; }
                        }
                    }

                    let mut best: Option<(String, f64)> = None;
                    for (url, ep) in eps.iter() {
                        if let Some(m) = ms.get(url) {
                            if m.breaker_is_open() { continue; }
                            let p50 = if m.ewma_p50 > 0.0 { m.ewma_p50 } else { m.get_percentile(50.0).unwrap_or(100.0) };
                            let p99 = if m.ewma_p99 > 0.0 { m.ewma_p99 } else { m.get_percentile(99.0).unwrap_or(160.0) };
                            let sr  = m.rolling_success_rate().max(m.get_success_rate());
                            let cur = m.current_requests.load(Ordering::Relaxed) as f64;
                            let max = (ep.max_requests_per_second as f64).max(1.0);
                            let load_factor = (1.0 - (cur / max)).clamp(0.05, 0.95);

                            let mut score = calculate_endpoint_score(p50, p99, sr, load_factor, ep.priority, &ep.region, "");
                            if let Some((max_here, _)) = m.slot_latencies.iter().max_by_key(|(s, _)| *s) {
                                let gap = global_max.saturating_sub(*max_here) as f64;
                                if gap > 0.0 { score *= (1.0 - (0.03 * gap).min(0.30)); }
                            }
                            match best {
                                None => best = Some((url.clone(), score)),
                                Some((_u, bs)) if score > bs => best = Some((url.clone(), score)),
                                _ => {}
                            }
                        }
                    }

                    match best {
                        Some((u, _)) => {
                            let msr = metrics.read().await;
                            let budget = msr.get(&u).map(|m| calc_timeout_budget_ms(OpClass::Read, Some(m))).unwrap_or(90);
                            (u, budget)
                        }
                        None => { ("".to_string(), 90) }
                    }
                };

                let client_opt = { let cl = clients.read().await; cl.get(&chosen_url).cloned() };
                let order: Vec<Signature> = pending.keys().cloned().collect();

                if let Some(client) = client_opt {
                    let res = tokio::time::timeout(Duration::from_millis(budget_ms), client.get_signature_statuses(&order)).await;
                    match res {
                        Ok(Ok(resp)) => {
                            let vals = resp.value;
                            for (idx, sig) in order.iter().enumerate() {
                                let ok = vals.get(idx).and_then(|o| o.as_ref()).is_some();
                                if ok { cache.write().await.insert(*sig, Instant::now()); }
                                if let Some(waiters) = pending.remove(sig) {
                                    for tx in waiters { let _ = tx.send(ok); }
                                }
                            }
                        }
                        _ => {
                            for (_sig, waiters) in pending.drain() {
                                for tx in waiters { let _ = tx.send(false); }
                            }
                        }
                    }
                } else {
                    for (_sig, waiters) in pending.drain() {
                        for tx in waiters { let _ = tx.send(false); }
                    }
                }
            }
        });
    }

// === REPLACE: confirm_signature_smart (adds cache check-first) ===
    pub async fn confirm_signature_smart(
        &self,
        sig: Signature,
        max_wait_ms: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.confirmed_sig_cache.read().await.get(&sig).is_some() {
            return Ok(());
        }
        let (tx, rx) = oneshot::channel::<bool>();
        if self.sig_req_tx.send((sig, tx)).is_ok() {
            if let Ok(Ok(true)) = tokio::time::timeout(Duration::from_millis(max_wait_ms.min(120)), rx).await {
                // fill cache to speed up future checks
                self.confirmed_sig_cache.write().await.insert(sig, Instant::now());
                return Ok(());
            }
        }
        self.confirm_signature_hedged(sig, max_wait_ms).await
    }

// === ADD: confirm_signature_hedged (diverse, abortable) ===
    pub async fn confirm_signature_hedged(
        &self,
        sig: Signature,
        max_wait_ms: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let fan = 3usize;
        let clients = self.select_diverse_candidates(fan).await;
        if clients.is_empty() {
            return Err("no clients for confirm".into());
        }

        let mut aborts: Vec<AbortHandle> = Vec::with_capacity(clients.len());
        let mut futs = Vec::with_capacity(clients.len());
        for (i, c) in clients.into_iter().enumerate() {
            let (ah, reg) = AbortHandle::new_pair();
            aborts.push(ah);
            let delay = (i as u64) * 8; // 8ms stagger
            let f = async move {
                if delay > 0 { tokio::time::sleep(Duration::from_millis(delay)).await; }
                let cfg = RpcSignatureStatusConfig {
                    search_transaction_history: true,
                };
                let resp = c.get_signature_statuses_with_config(&[sig], cfg).await?;
                if let Some(Some(_)) = resp.value.get(0) { Ok(()) } else { Err("not yet".into()) }
            };
            futs.push(Abortable::new(timeout(Duration::from_millis(max_wait_ms), f).map(|r| {
                r.map_err(|_| "timeout".into()).and_then(|x| x)
            }), reg));
        }

        let res = futures::future::select_ok(futs).await;
        for ah in aborts { ah.abort(); }
        match res {
            Ok((_ok, _)) => Ok(()),
            Err(e) => Err(format!("confirm failed: {}", e).into()),
        }
    }

// === REPLACE: start_signature_cache_sweeper (dynamic TTL via performance samples) ===
    fn start_signature_cache_sweeper(&self, sweep_every: Duration, _ttl_ignored: Duration) {
        let cache = Arc::clone(&self.confirmed_sig_cache);
        let clients = Arc::clone(&self.rpc_clients);
        tokio::spawn(async move {
            let mut itv = tokio::time::interval(sweep_every);
            loop {
                itv.tick().await;

                // compute TTL ~= 4 slots based on recent sample; clamp [60s, 240s]
                let ttl = {
                    let any = {
                        let rd = clients.read().await;
                        rd.values().next().cloned()
                    };
                    if let Some(c) = any {
                        if let Ok(Ok(samples)) = timeout(Duration::from_millis(120), c.get_recent_performance_samples(Some(1))).await {
                            if let Some(s) = samples.first() {
                                let slot_ms = if s.num_slots > 0 { ((s.sample_period_secs as f64)*1000.0 / (s.num_slots as f64)).clamp(200.0, 1000.0) } else { 400.0 };
                                let ttl_ms = (slot_ms * 4.0).round() as u64;
                                Duration::from_millis(ttl_ms).clamp(Duration::from_secs(60), Duration::from_secs(240))
                            } else { Duration::from_secs(120) }
                        } else { Duration::from_secs(120) }
                    } else { Duration::from_secs(120) }
                };

                let now = Instant::now();
                let mut wr = cache.write().await;
                wr.retain(|_, ts| now.duration_since(*ts) <= ttl);
            }
        });
    }

// === ADD: quorum_get_latest_blockhash (read-hedged quorum=3) ===
    pub async fn quorum_get_latest_blockhash(&self, quorum: usize)
        -> Result<solana_sdk::hash::Hash, Box<dyn std::error::Error>>
    {
        let fans = self.select_diverse_candidates(quorum.max(1)).await;
        if fans.is_empty() { return Err("no clients".into()); }

        let mut aborts: Vec<AbortHandle> = Vec::with_capacity(fans.len());
        let mut futs = Vec::with_capacity(fans.len());
        for (i, c) in fans.into_iter().enumerate() {
            let (ah, reg) = AbortHandle::new_pair();
            aborts.push(ah);
            let delay = (i as u64) * 6;
            let f = async move {
                if delay > 0 { tokio::time::sleep(Duration::from_millis(delay)).await; }
                let bh = c.get_latest_blockhash().await?;
                Ok::<_, Box<dyn std::error::Error>>(bh)
            };
            futs.push(Abortable::new(timeout(Duration::from_millis(120), f)
                .map(|r| r.map_err(|_| "timeout".into()).and_then(|x| x)), reg));
        }

        // first success wins
        let res = futures::future::select_ok(futs).await;
        for ah in aborts { ah.abort(); }
        match res {
            Ok((bh, _)) => Ok(bh),
            Err(e) => Err(format!("blockhash quorum failed: {}", e).into()),
        }
    }

// === REPLACE: calibrate_hedge_params_from_metrics (include perf samples) ===
    async fn calibrate_hedge_params_from_metrics(&self) -> (usize, f64, u64) {
        // metrics snapshot
        let ms = self.metrics.read().await;
        if ms.is_empty() { return (2, 0.40, 10); }
        let mut p99s = Vec::with_capacity(ms.len());
        let mut max_slots = Vec::with_capacity(ms.len());
        for (_u, m) in ms.iter() {
            let p = if m.ewma_p99 > 0.0 { m.ewma_p99 } else { m.get_percentile(99.0).unwrap_or(120.0) };
            p99s.push(p);
            if let Some((s, _)) = m.slot_latencies.iter().max_by_key(|(s, _)| *s) { max_slots.push(*s); }
        }
        p99s.sort_by(|a,b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let med_p99 = p99s[p99s.len()/2];
        let global_tip = max_slots.into_iter().max().unwrap_or(0);
        let lagged = ms.iter().filter(|(_u, m)|
            m.slot_latencies.iter().max_by_key(|(s, _)| *s)
             .map(|(s, _)| global_tip.saturating_sub(*s) > 4).unwrap_or(false)
        ).count();
        drop(ms);

        // perf sample (best effort)
        let mut slot_ms = 400.0f64; // default
        if let Ok(c) = self.get_optimal_client_for(OpClass::Read).await {
            if let Ok(Ok(samples)) = timeout(Duration::from_millis(120), c.get_recent_performance_samples(Some(1))).await {
                if let Some(s) = samples.first() {
                    if s.num_slots > 0 {
                        slot_ms = ((s.sample_period_secs as f64)*1000.0 / (s.num_slots as f64)).clamp(200.0, 1000.0);
                    }
                }
            }
        }

        // heuristics tweaked by slot speed & lag share
        let lag_share = (lagged as f64) / (self.endpoints.read().await.len().max(1) as f64);
        if med_p99 > 150.0 || lag_share >= 0.40 || slot_ms > 500.0 {
            (3, 0.28, 8)   // congested: earlier backups, tight stagger
        } else if med_p99 > 110.0 || lag_share >= 0.20 {
            (3, 0.34, 10)
        } else {
            (2, 0.46, 12)  // smooth network
        }
    }

// === ADD: try_execute_read_shed ===
    /// Probabilistically shed reads under load; returns Err quickly if shed.
    pub async fn try_execute_read_shed<F, T>(
        &self,
        keep_frac: f64,
        operation: F,
    ) -> Result<T, Box<dyn std::error::Error>>
    where
        F: Fn(Arc<RpcClient>) -> futures::future::BoxFuture<'static, Result<T, Box<dyn std::error::Error>>> + Clone,
        T: Send + 'static,
    {
        // Fast random shed
        let mut rng = rand::thread_rng();
        if rng.gen::<f64>() > keep_frac.clamp(0.0, 1.0) {
            return Err("shed".into());
        }
        self.execute_classed_with_optimal_latency(OpClass::Read, operation).await
    }

// === ADD: price micro-jitter (basis points) ===
    #[inline]
    pub fn jitter_cu_price(price: u64, bps: u32) -> u64 {
        let bps = bps.min(200); // cap ±2%
        let r: u32 = rand::random::<u32>();
        let sign = if (r & 1) == 0 { 1i64 } else { -1i64 };
        let delta = ((price as u128) * (bps as u128) / 10_000u128) as u64;
        let newp = (price as i64) + sign * (delta as i64);
        newp.clamp(200, 10_000) as u64
    }

// === ADD: fee helpers (metrics-driven) ===
    /// Recommend microlamports-per-CU from live metrics.
    /// Heuristic: med p99 + lag share → CU price. Tuned to avoid overpay.
    pub async fn recommend_cu_price_from_metrics(&self) -> u64 {
        let ms = self.metrics.read().await;
        if ms.is_empty() { return 400; } // safe floor

        let mut p99s: Vec<f64> = Vec::with_capacity(ms.len());
        let mut max_slots: Vec<Slot> = Vec::with_capacity(ms.len());
        for (_u, m) in ms.iter() {
            let p = if m.ewma_p99 > 0.0 { m.ewma_p99 } else { m.get_percentile(99.0).unwrap_or(120.0) };
            p99s.push(p);
            if let Some((s, _)) = m.slot_latencies.iter().max_by_key(|(s, _)| *s) {
                max_slots.push(*s);
            }
        }
        p99s.sort_by(|a,b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let med_p99 = p99s[p99s.len()/2];
        let tip = max_slots.iter().copied().max().unwrap_or(0);
        let lagged = ms.iter().filter(|(_u, m)|
            m.slot_latencies.iter().max_by_key(|(s, _)| *s)
             .map(|(s, _)| tip.saturating_sub(*s) > 4).unwrap_or(false)
        ).count() as f64;
        let lag_frac = if ms.len() > 0 { lagged / (ms.len() as f64) } else { 0.0 };

        // Map med p99 + lag to price
        let base = if med_p99 <= 90.0 { 300 } else if med_p99 <= 130.0 { 700 } else { 1400 };
        let bump = (lag_frac * 1200.0).round() as u64; // up to +1200 when many are lagging
        (base + bump).min(5_000) // cap for sanity
    }

    /// Build ComputeBudget ixs to prepend before signing a tx message.
    pub fn compute_budget_ixs(cu_price_microlamports: u64, cu_limit: Option<u32>) -> Vec<Instruction> {
        let mut v = Vec::with_capacity(2);
        v.push(ComputeBudgetInstruction::set_compute_unit_price(cu_price_microlamports));
        if let Some(limit) = cu_limit {
            v.push(ComputeBudgetInstruction::set_compute_unit_limit(limit));
        }
        v
    }

    // Convenience: fully automatic send + confirm based on current conditions
    pub async fn send_transaction_landed_auto(
        &self,
        tx: solana_sdk::transaction::Transaction,
        confirm_wait_ms: u64,
    ) -> Result<solana_sdk::signature::Signature, Box<dyn std::error::Error>> {
        let (fanout, trig, stagger) = self.calibrate_hedge_params_from_metrics().await;
        self.send_transaction_landed_hedged(tx, fanout, trig, stagger, confirm_wait_ms).await
    }

// === REPLACE: send_transaction_landed_hedged (adds rescue confirm-on-error) ===
    pub async fn send_transaction_landed_hedged(
        &self,
        tx: solana_sdk::transaction::Transaction,
        send_fanout: usize,
        trigger_frac: f64,
        stagger_ms: u64,
        confirm_wait_ms: u64,
    ) -> Result<solana_sdk::signature::Signature, Box<dyn std::error::Error>> {
        use solana_client::rpc_config::RpcSendTransactionConfig;

        let sig = tx.signatures[0];
        let tx_arc = Arc::new(tx);

        let sent = self.conditional_hedged_execute_classed(
            {
                let txc = tx_arc.clone();
                move |c: Arc<RpcClient>| {
                    let t = txc.clone();
                    Box::pin(async move {
                        let cfg = RpcSendTransactionConfig {
                            skip_preflight: true,
                            max_retries: Some(0),
                            ..Default::default()
                        };
                        match c.send_transaction_with_config(&t, cfg).await {
                            Ok(s) => Ok(s),
                            Err(e) => {
                                // treat provider "already processed" as soft-ok
                                let msg = e.to_string().to_lowercase();
                                if msg.contains("already processed") || msg.contains("duplicate") {
                                    Ok(sig)
                                } else {
                                    Err(e.into())
                                }
                            }
                        }
                    })
                }
            },
            send_fanout,
            trigger_frac,
            stagger_ms,
        ).await;

        match sent {
            Ok(_) => {
                self.confirm_signature_smart(sig, confirm_wait_ms).await?;
                Ok(sig)
            }
            Err(e) => {
                // Rescue path: it might have already landed elsewhere
                if self.confirm_signature_smart(sig, confirm_wait_ms.min(600)).await.is_ok() {
                    return Ok(sig);
                }
                Err(e)
            }
        }
    }

// === ADD: send + confirm with coalescing ===
    pub async fn send_and_confirm_coalesced(
        &self,
        tx: solana_sdk::transaction::Transaction,
        confirm_wait_ms: u64,
    ) -> Result<solana_sdk::signature::Signature, Box<dyn std::error::Error>> {
        use solana_client::rpc_config::RpcSendTransactionConfig;
        let sig = tx.signatures[0];
        let (tx_done, rx_done) = oneshot::channel::<Result<(), String>>();

        {
            let mut map = self.send_coalesce.lock().await;
            if let Some(waiters) = map.get_mut(&sig) {
                waiters.push(tx_done);
                drop(map);
                // follower: await leader's outcome
                rx_done.await.map_err(|e| e.to_string())??;
                // short-circuit confirm (likely cached)
                self.confirm_signature_smart(sig, confirm_wait_ms).await?;
                return Ok(sig);
            } else {
                map.insert(sig, vec![tx_done]);
            }
        }

        // leader path: actually send (diverse hedged) and notify all waiters
        let tx_arc = Arc::new(tx);
        let fans = self.select_diverse_candidates(3).await;
        let mut aborts: Vec<AbortHandle> = Vec::with_capacity(fans.len());
        let mut futs = Vec::with_capacity(fans.len());

        for (i, c) in fans.into_iter().enumerate() {
            let (ah, reg) = AbortHandle::new_pair(); aborts.push(ah);
            let t = tx_arc.clone();
            let delay = (i as u64) * 10;
            let fut = async move {
                if delay > 0 { tokio::time::sleep(Duration::from_millis(delay)).await; }
                let cfg = RpcSendTransactionConfig { skip_preflight: true, max_retries: Some(0), ..Default::default() };
                c.send_transaction_with_config(&t, cfg).await.map(|_| ()).map_err(|e| e.into())
            };
            // 140–180ms budget based on our dynamic budget calc
            let budget = 160_u64;
            futs.push(Abortable::new(timeout(Duration::from_millis(budget), fut)
                .map(|r| r.map_err(|_| "timeout".into()).and_then(|x| x)), reg));
        }

        // winner notifies all waiters
        let result = futures::future::select_ok(futs).await;
        for ah in aborts { ah.abort(); }

        let outcome: Result<(), String> = match result {
            Ok((_ok, _)) => Ok(()),
            Err(e) => Err(format!("send failed: {}", e)),
        };

        let waiters = {
            let mut map = self.send_coalesce.lock().await;
            map.remove(&sig).unwrap_or_default()
        };
        for w in waiters { let _ = w.send(outcome.clone()); }

        outcome.map_err(|e| e.into())?;
        self.confirm_signature_smart(sig, confirm_wait_ms).await?;
        Ok(sig)
    }

// === ADD: helper: edge-aligned coalesced landing ===
    pub async fn send_aligned_and_coalesced(
        &self,
        mut tx: solana_sdk::transaction::Transaction,
        payer: &solana_sdk::pubkey::Pubkey,
        program_ixs: Vec<solana_sdk::instruction::Instruction>,
        cu_limit: Option<u32>,
        max_wait_ms: u64,
    ) -> Result<solana_sdk::signature::Signature, Box<dyn std::error::Error>> {
        // 1) slot align
        self.wait_slot_edge(120, 6).await;

        // 2) hybrid fee + jitter, prepend compute budget and re-sign if needed
        let base = self.recommend_cu_price_hybrid(&[*payer], 0.90, 5_000).await;
        let price = GeographicLatencyOptimizer::jitter_cu_price(base, 75);
        let mut ixs = GeographicLatencyOptimizer::compute_budget_ixs(price, cu_limit);
        ixs.extend(program_ixs);

        // rebuild message using original signers
        let msg = solana_sdk::message::Message::new(&ixs, Some(payer));
        tx.message = msg;

        // 3) coalesced send + smart confirm
        self.send_and_confirm_coalesced(tx, max_wait_ms).await
    }

    // === ADD: wait_slot_edge (align to near slot boundary with guard) ===
    pub async fn wait_slot_edge(&self, max_wait_ms: u64, guard_ms: u64) {
        // Try best client; if it fails, just sleep a minimal jitter and return.
        let c = match self.get_optimal_client_for(OpClass::Read).await {
            Ok(c) => c,
            Err(_) => { tokio::time::sleep(Duration::from_millis(5)).await; return; }
        };
        let start = Instant::now();
        let mut last = c.get_slot().await.ok();
        loop {
            if start.elapsed().as_millis() as u64 >= max_wait_ms { break; }
            tokio::time::sleep(Duration::from_millis(1)).await;
            match c.get_slot().await {
                Ok(s) => {
                    if let Some(prev) = last {
                        if s > prev {
                            // we just crossed the boundary; wait a tiny guard then exit
                            tokio::time::sleep(Duration::from_millis(guard_ms.min(20))).await;
                            break;
                        }
                    }
                    last = Some(s);
                }
                Err(_) => break,
            }
        }
    }

    // === ADD: send_aligned_and_coalesced_signed (safe, signed flow) ===
    pub async fn send_aligned_and_coalesced_signed(
        &self,
        payer: &solana_sdk::pubkey::Pubkey,
        signers: &[&dyn solana_sdk::signature::Signer],
        program_ixs: Vec<solana_sdk::instruction::Instruction>,
        cu_limit: Option<u32>,
        max_wait_ms: u64,
    ) -> Result<solana_sdk::signature::Signature, Box<dyn std::error::Error>> {
        // 1) slot align
        self.wait_slot_edge(120, 6).await;

        // 2) dynamic CU price with subtle jitter
        let base = self.recommend_cu_price_from_metrics().await;
        let price = GeographicLatencyOptimizer::jitter_cu_price(base, 75);
        let mut ixs = GeographicLatencyOptimizer::compute_budget_ixs(price, cu_limit);
        ixs.extend(program_ixs);

        // 3) quorum blockhash + sign
        let bh = self.quorum_get_latest_blockhash(3).await?;
        let msg = solana_sdk::message::Message::new(&ixs, Some(payer));
        let tx = solana_sdk::transaction::Transaction::new(signers, msg, bh);

        // 4) coalesced send + confirm
        self.send_and_confirm_coalesced(tx, max_wait_ms).await
    }

    // === ADD: send_with_auto_renew_blockhash_signed ===
    pub async fn send_with_auto_renew_blockhash_signed(
        &self,
        payer: &solana_sdk::pubkey::Pubkey,
        signers: &[&dyn solana_sdk::signature::Signer],
        program_ixs: Vec<solana_sdk::instruction::Instruction>,
        cu_limit: Option<u32>,
        confirm_wait_ms: u64,
        max_attempts: usize,
    ) -> Result<solana_sdk::signature::Signature, Box<dyn std::error::Error>> {
        use solana_sdk::{message::Message, transaction::Transaction};

        let base = self.recommend_cu_price_from_metrics().await;
        let mut last_err: Option<String> = None;

        for attempt in 0..max_attempts.max(1) {
            if attempt > 0 {
                // small guard: next slot edge for fresh write-lock window
                self.wait_slot_edge(100, 5).await;
            }

            // escalate tip gently on retries (1.00, 1.12, 1.28, …)
            let mult = 1.0 + (attempt as f64) * 0.12;
            let price = GeographicLatencyOptimizer::jitter_cu_price(((base as f64) * mult) as u64, 60);
            let mut ixs = GeographicLatencyOptimizer::compute_budget_ixs(price, cu_limit);
            ixs.extend(program_ixs.clone());

            // fresh blockhash per attempt
            let bh = self.quorum_get_latest_blockhash(3).await?;
            let msg = Message::new(&ixs, Some(payer));
            let tx  = Transaction::new(signers, msg, bh);

            match self.send_and_confirm_coalesced(tx, confirm_wait_ms).await {
                Ok(sig) => return Ok(sig),
                Err(e) => {
                    let es = e.to_string();
                    match classify_rpc_error(&es) {
                        ErrorClass::BlockhashStale => {
                            // immediate re-loop with fresh blockhash; no sleep
                        }
                        ErrorClass::AccountInUse => {
                            // slight delay into next micro-window inside the slot
                            tokio::time::sleep(std::time::Duration::from_millis(6)).await;
                        }
                        ErrorClass::BlockCostLimit | ErrorClass::WouldExceedVoteCost => {
                            // wait slot edge then escalate (handled by next attempt)
                            self.wait_slot_edge(120, 6).await;
                        }
                        _ => {}
                    }
                    last_err = Some(es);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| "send_with_auto_renew: failed".into()).into())
    }

    // === ADD: send_price_ladder_until_confirm_signed ===
    pub async fn send_price_ladder_until_confirm_signed(
        &self,
        payer: &solana_sdk::pubkey::Pubkey,
        signers: &[&dyn solana_sdk::signature::Signer],
        program_ixs: Vec<solana_sdk::instruction::Instruction>,
        cu_limit: Option<u32>,
        confirm_wait_ms: u64,
        steps: &[f64],   // e.g. &[1.00, 1.15, 1.35]
    ) -> Result<solana_sdk::signature::Signature, Box<dyn std::error::Error>> {
        use solana_sdk::{message::Message, transaction::Transaction};
        if steps.is_empty() { return Err("ladder: empty steps".into()); }

        let base = self.recommend_cu_price_from_metrics().await;

        for (i, mult) in steps.iter().enumerate() {
            if i > 0 { self.wait_slot_edge(100, 5).await; }

            let price = GeographicLatencyOptimizer::jitter_cu_price(((base as f64) * *mult) as u64, 50);
            let mut ixs = GeographicLatencyOptimizer::compute_budget_ixs(price, cu_limit);
            ixs.extend(program_ixs.clone());

            let bh = self.quorum_get_latest_blockhash(3).await?;
            let msg = Message::new(&ixs, Some(payer));
            let tx  = Transaction::new(signers, msg, bh);

            match self.send_and_confirm_coalesced(tx, confirm_wait_ms).await {
                Ok(sig) => return Ok(sig),
                Err(e) => {
                    let es = e.to_string();
                    match classify_rpc_error(&es) {
                        ErrorClass::BlockhashStale => { /* immediate retry with next step uses fresh bh */ }
                        ErrorClass::AccountInUse => { tokio::time::sleep(std::time::Duration::from_millis(6)).await; }
                        ErrorClass::BlockCostLimit | ErrorClass::WouldExceedVoteCost => {
                            self.wait_slot_edge(120, 6).await;
                        }
                        _ => {}
                    }
                    // proceed to next step
                }
            }
        }
        Err("ladder: all steps exhausted".into())
    }
}

// === ADD: invalidate() to BlockhashCoalescer ===
impl BlockhashCoalescer {
    pub async fn invalidate(&self) {
        let mut w = self.cache.write().await;
        *w = None;
        let mut infl = self.in_flight.lock().await;
        *infl = None;
    }

    /// If cached entry is older than (warm_frac * ttl_ms), refresh in-line and return Ok(()).
    /// If empty cache, fetch once.
    pub async fn warm(
        &self,
        opt: &GeographicLatencyOptimizer,
        ttl_ms: u64,
        warm_frac: f64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some((_h, t)) = *self.cache.read().await {
            let age = t.elapsed().as_millis() as u64;
            let warm_at = ((ttl_ms as f64) * warm_frac.clamp(0.2, 0.95)) as u64;
            if age >= warm_at {
                let _ = self.get(opt, ttl_ms).await?;
            }
            Ok(())
        } else {
            let _ = self.get(opt, ttl_ms).await?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_geographic_optimizer_initialization() {
        let endpoints = vec![
            GeographicEndpoint {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                region: "us-west".to_string(),
                provider: "solana".to_string(),
                weight: 1.0,
                priority: 10,
                max_requests_per_second: 100,
            },
            GeographicEndpoint {
                url: "https://solana-api.projectserum.com".to_string(),
                region: "us-east".to_string(),
                provider: "serum".to_string(),
                weight: 0.9,
                priority: 8,
                max_requests_per_second: 80,
            },
        ];

        let optimizer = GeographicLatencyOptimizer::new(
            endpoints,
            "us-west".to_string(),
            vec!["us-east".to_string()],
        ).await;

        assert!(optimizer.is_ok());
    }

    #[tokio::test]
    async fn test_endpoint_selection() {
        let endpoints = vec![
            GeographicEndpoint {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                region: "us-west".to_string(),
                provider: "solana".to_string(),
                weight: 1.0,
                priority: 10,
                max_requests_per_second: 100,
            },
        ];

        let optimizer = GeographicLatencyOptimizer::new(
            endpoints,
            "us-west".to_string(),
            vec![],
        ).await.unwrap();

        let client = optimizer.get_optimal_client().await;
        assert!(client.is_ok());
    }
}
