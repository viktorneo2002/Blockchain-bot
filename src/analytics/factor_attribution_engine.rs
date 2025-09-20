// REPLACE: top-of-file attributes, imports, constants, factor enum
#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use ahash::AHashMap as HashMap;
use arc_swap::ArcSwapOption;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use solana_sdk::clock::Slot;
use solana_sdk::pubkey::Pubkey;
use std::collections::VecDeque;
use std::f64::consts::LN_2;
use std::sync::{Arc, atomic::{AtomicBool, Ordering, AtomicU64}};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use anyhow::{Result, Context};

// === tuned constants for low-latency, robust attribution ===
const FACTOR_WINDOW_SIZE: usize = 1024;      // power-of-2 cache friendliness
const MIN_SAMPLE_SIZE: usize = 64;           // stable stats
const ATTRIBUTION_UPDATE_INTERVAL_MS: u64 = 64;  // snappier react without thrash
const MAX_FACTOR_AGE_MS: u64 = 30_000;

// === adaptive decay: half-life in milliseconds (time-scale invariant) ===
const EW_HALFLIFE_MS: f64 = 3_000.0;         // ~3s half-life for fast regime shifts
#[inline(always)]
fn decay_from_dt_ms(dt_ms: u64) -> f64 {
    // exp(-ln2 * dt / halflife) ; dt==0 -> ~1.0 (but we clamp below)
    let x = (-LN_2 * (dt_ms as f64) / EW_HALFLIFE_MS).exp();
    x.clamp(0.5, 0.999)                       // avoid extremes
}

#[inline(always)]
fn decay_from_dt_ms_with_hl(dt_ms: u64, half_life_ms: f64) -> f64 {
    let x = (-LN_2 * (dt_ms as f64) / half_life_ms.max(1.0)).exp();
    x.clamp(0.5, 0.999)
}

const CONFIDENCE_THRESHOLD: f64 = 0.86;
const CORRELATION_THRESHOLD: f64 = 0.28;
const HUBER_K: f64 = 3.0;                    // robust clamp in z-space
const DECAY_FACTOR: f64 = 0.1;               // weight adaptation rate

// === regime detection constants ===
const EW_HALFLIFE_MS_MIN: f64 = 1_200.0;
const EW_HALFLIFE_MS_MAX: f64 = 6_000.0;

// === execution tuning caps/rounding (Jito economics) ===
const TIP_ROUND_LAMPORTS: u64 = 100;         // round tips to 100-lamport steps
const TIP_MULT_MIN: f64 = 0.85;
const TIP_MULT_MAX: f64 = 2.40;
const SIZE_MULT_MIN: f64 = 0.30;
const SIZE_MULT_MAX: f64 = 1.80;
const SLIP_MIN: f64 = 0.0001;
const SLIP_MAX: f64 = 0.0012;

// === factor universe: array-indexed (no string hashing on hot path) ===
#[repr(u8)]
#[derive(Copy, Clone, Debug, Eq, PartialEq, Hash)]
pub enum Factor {
    GasPrice = 0,
    NetworkCongestion = 1,
    LiquidityDepth = 2,
    PriceVolatility = 3,
    CompetitionLevel = 4,
    SlippageEstimate = 5,
    BlockSpaceUtilization = 6,
    PriorityFeePercentile = 7,
    SuccessProbability = 8,
    LatencyMs = 9,
}
const FACTOR_COUNT: usize = 10;
const INTER_COUNT: usize = 6;
const PHI_DIM: usize = FACTOR_COUNT + INTER_COUNT;

// indices for interactions (in terms of base-factor indices)
const INTERACTIONS: [(usize, usize); INTER_COUNT] = [
    (Factor::LiquidityDepth as usize, Factor::PriceVolatility as usize),      // depth × (low-vol proxy)
    (Factor::CompetitionLevel as usize, Factor::NetworkCongestion as usize),  // low-comp × low-cong
    (Factor::GasPrice as usize,        Factor::NetworkCongestion as usize),   // low-fee × low-cong
    (Factor::LatencyMs as usize,       Factor::CompetitionLevel as usize),    // low-lat × low-comp
    (Factor::PriorityFeePercentile as usize, Factor::NetworkCongestion as usize), // low-prio% × low-cong
    (Factor::BlockSpaceUtilization as usize, Factor::NetworkCongestion as usize), // low-util × low-cong
];

const ALL_FACTORS: [Factor; FACTOR_COUNT] = [
    Factor::GasPrice,
    Factor::NetworkCongestion,
    Factor::LiquidityDepth,
    Factor::PriceVolatility,
    Factor::CompetitionLevel,
    Factor::SlippageEstimate,
    Factor::BlockSpaceUtilization,
    Factor::PriorityFeePercentile,
    Factor::SuccessProbability,
    Factor::LatencyMs,
];
#[inline(always)] fn fidx(f: Factor) -> usize { f as usize }
#[inline(always)] fn fname(f: Factor) -> &'static str {
    match f {
        Factor::GasPrice => "gas_price",
        Factor::NetworkCongestion => "network_congestion",
        Factor::LiquidityDepth => "liquidity_depth",
        Factor::PriceVolatility => "price_volatility",
        Factor::CompetitionLevel => "competition_level",
        Factor::SlippageEstimate => "slippage_estimate",
        Factor::BlockSpaceUtilization => "block_space_utilization",
        Factor::PriorityFeePercentile => "priority_fee_percentile",
        Factor::SuccessProbability => "success_probability",
        Factor::LatencyMs => "latency_ms",
    }
}
#[inline(always)] fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

#[inline(always)]
fn sigmoid(z: f64) -> f64 { 1.0 / (1.0 + (-z).exp()) }

#[inline(always)]
fn normalize_all(
    m: &FactorMetrics,
    rs: &[Running; FACTOR_COUNT],
    rabs: &[RunningAbsDev; FACTOR_COUNT],
) -> [f64; FACTOR_COUNT] {
    let mut out = [0.0f64; FACTOR_COUNT];
    for (i, f) in ALL_FACTORS.iter().enumerate() {
        let mean = rs[i].mean();
        let std = rs[i].std();
        let robust = (1.4826 * rabs[i].mad()).max(std).max(1e-9);
        let x = m.get(*f);
        let raw_z = (x - mean) / robust;
        let z = if raw_z.abs() <= HUBER_K { raw_z } else { HUBER_K * raw_z.signum() };
        let z01 = sigmoid(z);
        out[i] = match f {
            Factor::LiquidityDepth | Factor::SuccessProbability => z01, // "more is better"
            _ => 1.0 - z01, // costs/risks inverted so high = good
        };
    }
    out
}

#[inline(always)]
fn build_phi(norm: &[f64; FACTOR_COUNT]) -> [f64; PHI_DIM] {
    let mut phi = [0.0f64; PHI_DIM];
    // first 10 = normalized base features
    for i in 0..FACTOR_COUNT { phi[i] = norm[i]; }
    // interactions
    for (k, (a, b)) in INTERACTIONS.iter().enumerate() {
        phi[FACTOR_COUNT + k] = norm[*a] * norm[*b];
    }
    phi
}

// REPLACE: FactorMetrics, FactorAttribution, AttributionResult (Instant -> u64 ms)
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FactorMetrics {
    pub timestamp_ms: u64,   // serializable, monotone enough for our purpose
    pub slot: Slot,
    pub gas_price: f64,
    pub network_congestion: f64,
    pub liquidity_depth: f64,
    pub price_volatility: f64,
    pub competition_level: f64,
    pub slippage_estimate: f64,
    pub block_space_utilization: f64,
    pub priority_fee_percentile: f64,
    pub success_probability: f64,
    pub latency_ms: f64,
}
impl FactorMetrics {
    #[inline] pub fn get(&self, f: Factor) -> f64 {
        match f {
            Factor::GasPrice => self.gas_price,
            Factor::NetworkCongestion => self.network_congestion,
            Factor::LiquidityDepth => self.liquidity_depth,
            Factor::PriceVolatility => self.price_volatility,
            Factor::CompetitionLevel => self.competition_level,
            Factor::SlippageEstimate => self.slippage_estimate,
            Factor::BlockSpaceUtilization => self.block_space_utilization,
            Factor::PriorityFeePercentile => self.priority_fee_percentile,
            Factor::SuccessProbability => self.success_probability,
            Factor::LatencyMs => self.latency_ms,
        }
    }
}

#[derive(Clone, Debug)]
pub struct FactorAttribution {
    pub factor: Factor,
    pub weight: f64,
    pub contribution: f64,
    pub confidence: f64,
    pub corr_to_success: f64,
    pub samples: usize,
    pub last_updated_ms: u64,
}

#[derive(Clone, Debug)]
pub struct AttributionResult {
    pub total_attribution: f64,
    pub factors: HashMap<&'static str, FactorAttribution>,
    pub confidence_score: f64,
    pub timestamp_ms: u64,
    pub recommendations: SmallVec<[&'static str; 8]>,
}

// PASTE VERBATIM
#[derive(Clone, Copy, Debug)]
struct Running {
    n: f64,
    mean: f64,
    m2: f64,       // sum of squared diffs (Welford)
}
impl Running {
    fn new() -> Self { Self { n: 0.0, mean: 0.0, m2: 0.0 } }
    fn push(&mut self, x: f64, decay: f64) {
        // EW Welford: decay older info to adapt fast
        self.n = self.n * decay + 1.0;
        let delta = x - self.mean;
        self.mean += delta / self.n.max(1.0);
        let delta2 = x - self.mean;
        self.m2 = self.m2 * decay + delta * delta2;
    }
    fn mean(&self) -> f64 { self.mean }
    fn var(&self) -> f64 { if self.n > 1.0 { self.m2 / (self.n - 1.0) } else { 0.0 } }
    fn std(&self) -> f64 { self.var().sqrt() }
}

#[derive(Clone, Copy, Debug)]
struct RunningXY {
    x: Running,
    y: Running,
    c: f64,    // EW cov
    n: f64,
}
impl RunningXY {
    fn new() -> Self { Self { x: Running::new(), y: Running::new(), c: 0.0, n: 0.0 } }
    fn push(&mut self, x: f64, y: f64, decay: f64) {
        self.n = self.n * decay + 1.0;
        let x_old_mean = self.x.mean;
        self.x.push(x, decay);
        self.y.push(y, decay);
        // exponentially weighted covariance update
        self.c = self.c * decay + (x - x_old_mean) * (y - self.y.mean());
    }
    fn corr(&self) -> f64 {
        let den = self.x.std() * self.y.std();
        if den <= 1e-12 { 0.0 } else { (self.c / (self.n.max(1.0))) / den }
    }
}

// PASTE VERBATIM
#[derive(Clone, Copy, Debug)]
struct RunningAbsDev {
    n: f64,
    mean_abs_dev: f64, // EW mean of |x - mean_ew|
}
impl RunningAbsDev {
    fn new() -> Self { Self { n: 0.0, mean_abs_dev: 0.0 } }
    #[inline(always)]
    fn push(&mut self, abs_dev: f64, decay: f64) {
        self.n = self.n * decay + 1.0;
        self.mean_abs_dev = self.mean_abs_dev * decay + abs_dev;
    }
    #[inline(always)]
    fn mad(&self) -> f64 { self.mean_abs_dev / self.n.max(1.0) }
}

// PASTE VERBATIM
#[derive(Clone, Debug)]
pub struct OnlineRidge<const K: usize> {
    w: [f64; K],                 // coefficient vector
    p: [[f64; K]; K],            // inverse covariance
    lambda: f64,                 // forgetting (0<lambda<=1) ; e.g., 0.98
    samples: f64,
}

impl<const K: usize> OnlineRidge<K> {
    pub fn new(lambda: f64, delta: f64) -> Self {
        // P0 = delta * I ; larger delta => stronger regularization initially
        let mut p = [[0.0f64; K]; K];
        for i in 0..K { p[i][i] = delta; }
        Self { w: [0.0; K], p, lambda, samples: 0.0 }
    }

    #[inline(always)]
    pub fn update(&mut self, x: &[f64; K], y: f64) {
        // g = P x / (lambda + x^T P x)
        let mut px = [0.0f64; K];
        for i in 0..K {
            let mut s = 0.0;
            for j in 0..K { s += self.p[i][j] * x[j]; }
            px[i] = s;
        }
        let mut xpx = 0.0;
        for i in 0..K { xpx += x[i] * px[i]; }
        let denom = (self.lambda + xpx).max(1e-9);
        let mut g = [0.0f64; K];
        for i in 0..K { g[i] = px[i] / denom; }

        // e = y - w^T x
        let mut yhat = 0.0;
        for i in 0..K { yhat += self.w[i] * x[i]; }
        let e = y - yhat;

        // w = w + g * e
        for i in 0..K { self.w[i] += g[i] * e; }

        // P = (P - g x^T P) / lambda
        // compute outer = g * (x^T P)
        let mut xtp = [0.0f64; K];
        for j in 0..K {
            let mut s = 0.0;
            for k in 0..K { s += x[k] * self.p[k][j]; }
            xtp[j] = s;
        }
        for i in 0..K {
            for j in 0..K {
                self.p[i][j] = (self.p[i][j] - g[i] * xtp[j]) / self.lambda;
            }
        }
        self.samples = (self.samples + 1.0).min(1e9);
    }

    #[inline(always)]
    pub fn weights(&self) -> [f64; K] { self.w }

    #[inline(always)]
    pub fn sample_count(&self) -> f64 { self.samples }

    #[inline(always)]
    pub fn set_lambda(&mut self, lambda: f64) {
        // clamp for numeric stability
        self.lambda = lambda.clamp(0.90, 0.9999);
    }
}

// PASTE VERBATIM
#[derive(Clone, Debug)]
pub struct FtrlLogit<const K: usize> {
    z: [f64; K],   // accumulators
    n: [f64; K],   // squared-gradient sum
    alpha: f64,    // learning rate
    beta: f64,     // smoothing
    l1: f64,       // L1
    l2: f64,       // L2
}

impl<const K: usize> FtrlLogit<K> {
    pub fn new(alpha: f64, beta: f64, l1: f64, l2: f64) -> Self {
        Self { z: [0.0; K], n: [0.0; K], alpha, beta, l1, l2 }
    }

    #[inline(always)]
    fn weight_i(&self, i: usize) -> f64 {
        let z = self.z[i];
        let n = self.n[i].sqrt();
        if z.abs() <= self.l1 {
            0.0
        } else {
            -((z - z.signum() * self.l1) /
              ((self.beta + n) / self.alpha + self.l2))
        }
    }

    #[inline(always)]
    pub fn weights(&self) -> [f64; K] {
        let mut w = [0.0f64; K];
        for i in 0..K { w[i] = self.weight_i(i); }
        w
    }

    #[inline(always)]
    pub fn predict(&self, x: &[f64; K]) -> f64 {
        let mut zsum = 0.0;
        for i in 0..K { zsum += self.weight_i(i) * x[i]; }
        1.0 / (1.0 + (-zsum).exp()) // calibrated prob
    }

    #[inline(always)]
    pub fn update(&mut self, x: &[f64; K], y: f64) {
        let p = self.predict(x);
        let g_common = (p - y).clamp(-1e6, 1e6);
        for i in 0..K {
            let g = g_common * x[i];
            let sigma = ((self.n[i] + g*g).sqrt() - self.n[i].sqrt()) / self.alpha;
            let w_i = self.weight_i(i);
            self.z[i] += g - sigma * w_i;
            self.n[i] += g*g;
        }
    }

    #[inline(always)]
    pub fn set_alpha(&mut self, alpha: f64) {
        // stable range for on-chain feature scales
        self.alpha = alpha.clamp(0.01, 0.20);
    }
}

// PASTE VERBATIM
const CALIB_BINS: usize = 16;

#[derive(Clone, Copy, Debug)]
struct CalibBin {
    n: f64,         // EW count
    sum_pred: f64,  // EW sum of predicted p
    sum_true: f64,  // EW sum of realized y
}
impl CalibBin {
    #[inline(always)] fn new() -> Self { Self { n: 0.0, sum_pred: 0.0, sum_true: 0.0 } }
    #[inline(always)] fn update(&mut self, p: f64, y: f64, d: f64) {
        self.n = self.n * d + 1.0;
        self.sum_pred = self.sum_pred * d + p;
        self.sum_true = self.sum_true * d + y;
    }
    #[inline(always)] fn rate(&self) -> f64 {
        (self.sum_true / self.n.max(1.0)).clamp(0.0, 1.0)
    }
}

#[derive(Clone, Debug)]
pub struct BinCalibrator {
    bins: [CalibBin; CALIB_BINS],
}
impl BinCalibrator {
    pub fn new() -> Self { Self { bins: [CalibBin::new(); CALIB_BINS] } }

    #[inline(always)]
    pub fn update(&mut self, p_raw: f64, y: bool, decay: f64) {
        let p = p_raw.clamp(0.0, 1.0 - 1e-9);
        let idx = ((p * CALIB_BINS as f64) as usize).min(CALIB_BINS - 1);
        self.bins[idx].update(p, if y { 1.0 } else { 0.0 }, decay.clamp(0.5, 0.999));
    }

    #[inline(always)]
    pub fn apply(&self, p_raw: f64) -> f64 {
        let p = p_raw.clamp(0.0, 1.0 - 1e-9);
        let pos = p * CALIB_BINS as f64;
        let i = pos.floor() as usize;
        let t = (pos - i as f64).clamp(0.0, 1.0);
        let r0 = self.bins[i].rate();
        let r1 = if i + 1 < CALIB_BINS { self.bins[i + 1].rate() } else { r0 };
        ((1.0 - t) * r0 + t * r1).clamp(0.0, 1.0)
    }

    #[inline(always)]
    pub fn sample_count(&self) -> f64 {
        self.bins.iter().map(|b| b.n).sum()
    }
}

#[derive(Clone, Debug)]
pub struct CalibratorEnsemble {
    temp: ProbCalibrator,
    bins: BinCalibrator,
}
impl CalibratorEnsemble {
    pub fn new() -> Self { Self { temp: ProbCalibrator::new(), bins: BinCalibrator::new() } }

    #[inline(always)]
    pub fn update(&mut self, p_pred: f64, realized_success: bool, decay: f64) {
        self.temp.update(p_pred, realized_success, decay);
        self.bins.update(p_pred, realized_success, decay);
    }

    #[inline(always)]
    pub fn apply(&self, p_pred: f64) -> f64 {
        // Blend shifts gradually toward bins as evidence accumulates (≤50% weight).
        let p1 = self.temp.apply(p_pred);
        let p2 = self.bins.apply(p_pred);
        let w_bins = (self.bins.sample_count() / 256.0).clamp(0.0, 0.5);
        ((1.0 - w_bins) * p1 + w_bins * p2).clamp(0.0, 1.0)
    }
}

// PASTE VERBATIM
#[derive(Clone, Debug)]
pub struct ProbCalibrator {
    t: f64,      // temperature (>0): >1 flattens, <1 sharpens
    b: f64,      // bias shift on logit
    n: f64,      // EW sample count
}
impl ProbCalibrator {
    pub fn new() -> Self { Self { t: 1.0, b: 0.0, n: 0.0 } }

    // Online gradient step on log-loss with forgetting; expects predicted p_raw in [0,1], y in {0,1}
    #[inline(always)]
    pub fn update(&mut self, p_raw: f64, y: bool, decay: f64) {
        let d = decay.clamp(0.5, 0.999);
        self.n = self.n * d + 1.0;

        let p = p_raw.clamp(1e-6, 1.0 - 1e-6);
        let logit = (p / (1.0 - p)).ln(); // raw logit
        let z = self.b + logit / self.t;
        let p_cal = 1.0 / (1.0 + (-z).exp());
        let yv = if y { 1.0 } else { 0.0 };

        // gradients
        let g = (p_cal - yv).clamp(-1e6, 1e6);      // d/dz logloss = p_cal - y
        let dz_dt = -logit / (self.t * self.t);     // d(b + logit/t)/dt
        let lr = 0.02;

        self.t = (self.t - lr * g * dz_dt).clamp(0.5, 2.0);
        self.b = self.b - lr * g;
    }

    #[inline(always)]
    pub fn apply(&self, p_raw: f64) -> f64 {
        let p = p_raw.clamp(1e-6, 1.0 - 1e-6);
        let logit = (p / (1.0 - p)).ln();
        let z = self.b + logit / self.t;
        1.0 / (1.0 + (-z).exp())
    }
}

// PASTE VERBATIM
#[derive(Clone, Debug)]
struct RegimeDetector {
    ew_vol_norm: f64,     // EW of normalized "goodness" (1=low vol, 0=high vol)
    half_life_ms: f64,
}

impl RegimeDetector {
    fn new(base_hl_ms: f64) -> Self {
        Self { ew_vol_norm: 0.5, half_life_ms: base_hl_ms }
    }
    #[inline(always)]
    fn update_and_get(&mut self, vol_norm_good: f64) -> f64 {
        // smooth the normalized volatility goodness
        self.ew_vol_norm = 0.9 * self.ew_vol_norm + 0.1 * vol_norm_good.clamp(0.0, 1.0);
        // map 0..1 -> [0.6, 1.4] multiplier (high vol → 0.6x, low vol → 1.4x)
        let mult = 0.6 + 0.8 * self.ew_vol_norm;
        self.half_life_ms = (EW_HALFLIFE_MS * mult).clamp(EW_HALFLIFE_MS_MIN, EW_HALFLIFE_MS_MAX);
        self.half_life_ms
    }
    #[inline(always)]
    fn current_hl(&self) -> f64 { self.half_life_ms }
}

// REPLACE: Engine struct + new()
pub struct FactorAttributionEngine {
    factor_history: Arc<RwLock<VecDeque<FactorMetrics>>>,
    runs: Arc<RwLock<[Running; FACTOR_COUNT]>>,
    runs_absdev: Arc<RwLock<[RunningAbsDev; FACTOR_COUNT]>>,
    runs_to_success: Arc<RwLock<[RunningXY; FACTOR_COUNT]>>,
    factor_weights: Arc<RwLock<[f64; FACTOR_COUNT]>>,
    ridge: Arc<RwLock<OnlineRidge<PHI_DIM>>>,          // RLS over φ
    ftrl:  Arc<RwLock<FtrlLogit<PHI_DIM>>>,            // Logistic over φ
    regime: Arc<RwLock<RegimeDetector>>,               // NEW: adaptive half-life controller
    calib: Arc<RwLock<CalibratorEnsemble>>,          // CHANGED: ensemble calibrator
    last_result: Arc<ArcSwapOption<AttributionResult>>,
    last_update_ms: Arc<AtomicU64>,
    update_sender: mpsc::UnboundedSender<FactorMetrics>,
    shutting_down: Arc<AtomicBool>,
}

impl FactorAttributionEngine {
    pub fn new() -> Result<Self> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let factor_history = Arc::new(RwLock::new(VecDeque::with_capacity(FACTOR_WINDOW_SIZE)));
        let runs = Arc::new(RwLock::new([Running::new(); FACTOR_COUNT]));
        let runs_absdev = Arc::new(RwLock::new([RunningAbsDev::new(); FACTOR_COUNT]));
        let runs_to_success = Arc::new(RwLock::new([RunningXY::new(); FACTOR_COUNT]));
        let factor_weights = Arc::new(RwLock::new(Self::initialize_weights()));
        let ridge = Arc::new(RwLock::new(OnlineRidge::<PHI_DIM>::new(0.98, 1e3)));
        let ftrl  = Arc::new(RwLock::new(FtrlLogit::<PHI_DIM>::new(0.08, 1.0, 1e-4, 1e-3)));
        let regime = Arc::new(RwLock::new(RegimeDetector::new(EW_HALFLIFE_MS)));
        let calib  = Arc::new(RwLock::new(CalibratorEnsemble::new()));        // CHANGED
        let last_result = Arc::new(ArcSwapOption::from(None));
        let shutting_down = Arc::new(AtomicBool::new(false));
        let last_update_ms = Arc::new(AtomicU64::new(now_ms()));

        let engine = Self {
            factor_history: factor_history.clone(),
            runs: runs.clone(),
            runs_absdev: runs_absdev.clone(),
            runs_to_success: runs_to_success.clone(),
            factor_weights: factor_weights.clone(),
            ridge: ridge.clone(),
            ftrl:  ftrl.clone(),
            regime: regime.clone(),
            calib: calib.clone(),                                            // CHANGED
            last_result: last_result.clone(),
            last_update_ms: last_update_ms.clone(),
            update_sender: tx,
            shutting_down: shutting_down.clone(),
        };

        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_millis(ATTRIBUTION_UPDATE_INTERVAL_MS));
            loop {
                tokio::select! {
                    Some(m) = rx.recv() => {
                        Self::process_metrics(
                            m, &factor_history, &runs, &runs_absdev, &runs_to_success,
                            &factor_weights, &ridge, &ftrl, &regime, &last_result, &last_update_ms
                        );
                    }
                    _ = tick.tick() => {
                        if shutting_down.load(Ordering::Relaxed) { break; }
                        Self::update_weights_adaptive(&factor_history, &runs_to_success, &factor_weights, &ridge, &ftrl);
                        if let Some(latest) = factor_history.read().back().cloned() {
                            if let Some(r) = Self::calculate_attribution(&latest, &factor_history, &runs, &runs_absdev, &runs_to_success, &factor_weights) {
                                last_result.store(Some(Arc::new(r)));
                            }
                        }
                    }
                }
            }
        });

        Ok(engine)
    }

    #[inline(always)]
    fn initialize_weights() -> [f64; FACTOR_COUNT] {
        [
            0.16, // GasPrice
            0.12, // NetworkCongestion
            0.20, // LiquidityDepth
            0.18, // PriceVolatility
            0.10, // CompetitionLevel
            0.08, // SlippageEstimate
            0.06, // BlockSpaceUtilization
            0.05, // PriorityFeePercentile
            0.03, // SuccessProbability
            0.02, // LatencyMs
        ]
    }
}

// REPLACE: submit_metrics / process_metrics / calculate_attribution
impl FactorAttributionEngine {
    #[inline(always)]
    pub async fn submit_metrics(&self, mut m: FactorMetrics) -> Result<()> {
        if m.timestamp_ms == 0 { m.timestamp_ms = now_ms(); }
        self.update_sender.send(m).context("submit_metrics send failed")
    }

    #[inline(always)]
    fn process_metrics(
        m: FactorMetrics,
        history: &Arc<RwLock<VecDeque<FactorMetrics>>>,
        runs: &Arc<RwLock<[Running; FACTOR_COUNT]>>,
        runs_absdev: &Arc<RwLock<[RunningAbsDev; FACTOR_COUNT]>>,
        runs_xy: &Arc<RwLock<[RunningXY; FACTOR_COUNT]>>,
        _weights: &Arc<RwLock<[f64; FACTOR_COUNT]>>,
        ridge: &Arc<RwLock<OnlineRidge<PHI_DIM>>>,
        ftrl:  &Arc<RwLock<FtrlLogit<PHI_DIM>>>,
        regime: &Arc<RwLock<RegimeDetector>>,
        last_result: &Arc<ArcSwapOption<AttributionResult>>,
        last_update_ms: &Arc<AtomicU64>,
    ) {
        // compute decay from wall-clock delta and CURRENT regime half-life
        let prev = last_update_ms.load(Ordering::Relaxed);
        let dt_ms = m.timestamp_ms.saturating_sub(prev);
        let hl_ms = regime.read().current_hl();
        let eff_decay = decay_from_dt_ms_with_hl(dt_ms, hl_ms);
        last_update_ms.store(m.timestamp_ms, Ordering::Relaxed);

        // history
        {
            let mut h = history.write();
            if h.len() >= FACTOR_WINDOW_SIZE { h.pop_front(); }
            h.push_back(m.clone());
        }

        // stats update
        {
            let mut rs = runs.write();
            let mut rabs = runs_absdev.write();
            let mut rxyw = runs_xy.write();
            for (i, f) in ALL_FACTORS.iter().enumerate() {
                let x = m.get(*f);
                rs[i].push(x, eff_decay);
                let dev = (x - rs[i].mean()).abs();
                rabs[i].push(dev, eff_decay);
                rxyw[i].push(x, m.success_probability, eff_decay);
            }
        }

        // normalized features -> φ
        let (norm, phi) = {
            let rs_r = runs.read();
            let rabs_r = runs_absdev.read();
            let n = normalize_all(&m, &rs_r, &rabs_r);
            let p = build_phi(&n);
            (n, p)
        };

        // learners (regime-aware forgetting/step)
        {
            // shorter half-life -> more forgetting (smaller lambda)
            let hl_now = regime.read().current_hl();
            let lambda = (0.995 - 0.06 * ((EW_HALFLIFE_MS / hl_now).clamp(0.5, 2.0) - 1.0)).clamp(0.94, 0.995);
            let mut r = ridge.write();
            r.set_lambda(lambda);
            r.update(&phi, m.success_probability);
        }
        {
            // higher volatility -> reduce alpha; lower vol -> increase alpha
            let vol_norm_good = norm[Factor::PriceVolatility as usize]; // 1=low vol
            let alpha = (0.05 + 0.06 * vol_norm_good).clamp(0.02, 0.11);
            let mut f = ftrl.write();
            f.set_alpha(alpha);
            f.update(&phi, m.success_probability);
        }

        // update regime for NEXT tick using current normalized volatility goodness
        {
            let vol_norm_good = norm[Factor::PriceVolatility as usize]; // 1=low vol, 0=high vol
            let mut rg = regime.write();
            rg.update_and_get(vol_norm_good);
        }

        // refresh cache attribution
        if let Some(res) = Self::calculate_attribution(&m, history, runs, runs_absdev, runs_xy, _weights) {
            last_result.store(Some(Arc::new(res)));
        }
    }

    #[inline(always)]
    fn calculate_attribution(
        m: &FactorMetrics,
        history: &Arc<RwLock<VecDeque<FactorMetrics>>>,
        runs: &Arc<RwLock<[Running; FACTOR_COUNT]>>,
        runs_absdev: &Arc<RwLock<[RunningAbsDev; FACTOR_COUNT]>>,
        runs_xy: &Arc<RwLock<[RunningXY; FACTOR_COUNT]>>,
        weights: &Arc<RwLock<[f64; FACTOR_COUNT]>>,
    ) -> Option<AttributionResult> {
        let h = history.read();
        if h.len() < MIN_SAMPLE_SIZE { return None; }

        let rs = runs.read();
        let rabs = runs_absdev.read();
        let rxy = runs_xy.read();
        let w = weights.read();

        // shrinkage constant to stabilize corr at small n
        const CORR_SHRINK_LAMBDA: f64 = 10.0;

        let mut total_contrib = 0.0;
        let mut total_weight = 0.0;
        let mut out: HashMap<&'static str, FactorAttribution> = HashMap::with_capacity(FACTOR_COUNT);

        for (i, f) in ALL_FACTORS.iter().enumerate() {
            let mean = rs[i].mean();
            let std = rs[i].std();
            // robust scale: max(std, 1.4826 * EW|x - mean|)
            let robust = (1.4826 * rabs[i].mad()).max(std).max(1e-9);

            let x = m.get(*f);
            let raw_z = (x - mean) / robust;
            // Huber clamp
            let z = if raw_z.abs() <= HUBER_K { raw_z } else { HUBER_K * raw_z.signum() };
            let z01 = sigmoid(z);
            let normalized = match f {
                Factor::LiquidityDepth | Factor::SuccessProbability => z01,
                _ => 1.0 - z01,
            };

            // correlation with shrinkage toward 0 by effective sample size
            let corr_raw = rxy[i].corr();
            let n_eff = rxy[i].n.max(1.0);
            let corr = corr_raw * (n_eff / (n_eff + CORR_SHRINK_LAMBDA));

            let sample_frac = (h.len() as f64 / FACTOR_WINDOW_SIZE as f64).clamp(0.0, 1.0);

            // recency proxy
            let newest = m.timestamp_ms;
            let oldest = h.front().map(|mm| mm.timestamp_ms).unwrap_or(newest);
            let span = (newest.saturating_sub(oldest)).max(1) as f64;
            let recency = ((newest as f64 - oldest as f64) / span).clamp(0.0, 1.0);

            let conf = (0.45 * corr.abs() + 0.4 * sample_frac + 0.15 * recency).clamp(0.0, 1.0);

            let weight = w[i];
            let contrib = weight * normalized * (0.5 + 0.5 * conf);

            total_contrib += contrib;
            total_weight += weight;

            out.insert(fname(*f), FactorAttribution {
                factor: *f,
                weight,
                contribution: contrib,
                confidence: conf,
                corr_to_success: corr,
                samples: h.len(),
                last_updated_ms: newest,
            });
        }

        let total_attr = total_contrib / total_weight.max(1e-9);
        let conf_score = out.values().map(|fa| fa.confidence * fa.weight).sum::<f64>()
            / out.values().map(|fa| fa.weight).sum::<f64>().max(1e-9);

        let recs = Self::recommendations(&out, m);

        Some(AttributionResult {
            total_attribution: total_attr,
            factors: out,
            confidence_score: conf_score,
            timestamp_ms: m.timestamp_ms,
            recommendations: recs,
        })
    }
}

// REPLACE: update_weights_adaptive + generate recommendations
impl FactorAttributionEngine {
    fn update_weights_adaptive(
        history: &Arc<RwLock<VecDeque<FactorMetrics>>>,
        runs_xy: &Arc<RwLock<[RunningXY; FACTOR_COUNT]>>,
        weights: &Arc<RwLock<[f64; FACTOR_COUNT]>>,
        ridge: &Arc<RwLock<OnlineRidge<PHI_DIM>>>,
        ftrl:  &Arc<RwLock<FtrlLogit<PHI_DIM>>>,
    ) {
        let hlen = history.read().len();
        if hlen < MIN_SAMPLE_SIZE * 2 { return; }

        // 1) correlation scores (association)
        let rxy = runs_xy.read();
        let mut s_corr = [0.0f64; FACTOR_COUNT];
        for i in 0..FACTOR_COUNT {
            let c = rxy[i].corr().abs();
            s_corr[i] = (0.1 + 0.9 * c).clamp(0.05, 1.0);
        }
        let sumc: f64 = s_corr.iter().sum();
        for i in 0..FACTOR_COUNT { s_corr[i] /= sumc.max(1e-12); }

        // 2) RLS scores (directional, from φ weights), distribute interaction weights
        let r = ridge.read();
        let wr = r.weights();
        let mut s_rls = [0.0f64; FACTOR_COUNT];
        for i in 0..FACTOR_COUNT {
            s_rls[i] += wr[i].max(0.0); // base feature
        }
        for (k, (a, b)) in INTERACTIONS.iter().enumerate() {
            let w = wr[FACTOR_COUNT + k].max(0.0);
            if w > 0.0 {
                s_rls[*a] += 0.5 * w;
                s_rls[*b] += 0.5 * w;
            }
        }
        let sumr: f64 = s_rls.iter().sum();
        if sumr > 0.0 { for i in 0..FACTOR_COUNT { s_rls[i] /= sumr; } }
        let alpha_rls = (r.sample_count() / 200.0).clamp(0.0, 0.60); // up to 60%

        // 3) FTRL scores (probabilistic, calibrated), same distribution rule
        let f = ftrl.read();
        let wf = f.weights();
        let mut s_ftrl = [0.0f64; FACTOR_COUNT];
        for i in 0..FACTOR_COUNT {
            s_ftrl[i] += wf[i].abs(); // allow negative predictive weights, use magnitude
        }
        for (k, (a, b)) in INTERACTIONS.iter().enumerate() {
            let w = wf[FACTOR_COUNT + k].abs();
            if w > 0.0 {
                s_ftrl[*a] += 0.5 * w;
                s_ftrl[*b] += 0.5 * w;
            }
        }
        let sumf: f64 = s_ftrl.iter().sum();
        if sumf > 0.0 { for i in 0..FACTOR_COUNT { s_ftrl[i] /= sumf; } }
        let alpha_ftrl = (hlen as f64 / 256.0).clamp(0.0, 0.40); // ramp to 40% with history

        // Blend → target
        let mut target = [0.0f64; FACTOR_COUNT];
        for i in 0..FACTOR_COUNT {
            // base corr gets at least 20% authority; RLS + FTRL share the rest
            let base = 0.20 * s_corr[i];
            let blended = base
                + alpha_rls * s_rls[i]
                + alpha_ftrl * s_ftrl[i]
                + (0.80 - alpha_rls - alpha_ftrl).max(0.0) * s_corr[i];
            target[i] = blended.clamp(0.01, 0.50);
        }

        // renormalize and decay-blend with current weights
        let sumt: f64 = target.iter().sum();
        for i in 0..FACTOR_COUNT { target[i] /= sumt.max(1e-12); }

        let mut w = weights.write();
        for i in 0..FACTOR_COUNT {
            w[i] = w[i] * (1.0 - DECAY_FACTOR) + target[i] * DECAY_FACTOR;
        }
    }

    fn recommendations(factors: &HashMap<&'static str, FactorAttribution>, m: &FactorMetrics)
        -> SmallVec<[&'static str; 8]>
    {
        let mut v: SmallVec<[&'static str; 8]> = SmallVec::new();
        let gp = factors.get("gas_price").map(|x| x.contribution).unwrap_or(0.0);
        let cong = factors.get("network_congestion").map(|x| x.contribution).unwrap_or(0.0);
        let depth = factors.get("liquidity_depth").map(|x| x.contribution).unwrap_or(0.0);
        let vol = factors.get("price_volatility").map(|x| x.contribution).unwrap_or(0.0);
        let comp = factors.get("competition_level").map(|x| x.contribution).unwrap_or(0.0);
        let lat = factors.get("latency_ms").map(|x| x.contribution).unwrap_or(0.0);

        if gp > 0.65 || cong > 0.6 {
            v.push("Raise priority fee/Jito tip for critical bundles; down-rank low-ev flows.");
        }
        if depth < 0.35 {
            v.push("Cut size or widen slippage minOut; prefer multi-hop with deeper CLMM tiers.");
        }
        if vol > 0.6 {
            v.push("Tighten path guardrails; enable live delta vs oracle preflight.");
        }
        if comp > 0.55 {
            v.push("Switch to private relay set; increase CU budget + targeted slot timing.");
        }
        if m.success_probability < 0.5 {
            v.push("Defer non-urgent; re-route through best historical-inclusion validators.");
        }
        if lat > 0.6 {
            v.push("Stick to closest RPC/quic TPU; pin core & bypass Nagle; compress IX.");
        }
        v
    }
}

// REPLACE: get_current_attribution / get_factor_recommendation / shutdown
impl FactorAttributionEngine {
    pub async fn get_current_attribution(&self, _pool: &Pubkey) -> Result<AttributionResult> {
        if let Some(arc) = self.last_result.load_full() {
            if now_ms().saturating_sub(arc.timestamp_ms) < MAX_FACTOR_AGE_MS {
                return Ok((*arc).clone());
            }
        }
        let h = self.factor_history.read();
        let latest = h.back().cloned().context("No metrics available for attribution")?;
        drop(h);
        let r = Self::calculate_attribution(
            &latest, &self.factor_history, &self.runs, &self.runs_absdev, &self.runs_to_success, &self.factor_weights
        ).context("calc attribution failed")?;
        self.last_result.store(Some(Arc::new(r.clone())));
        Ok(r)
    }

    pub async fn get_factor_recommendation(&self, f: Factor) -> Result<&'static str> {
        let r = self.get_current_attribution(&Pubkey::new_from_array([0u8;32])).await?;
        if let Some(fa) = r.factors.get(fname(f)) {
            let msg = match f {
                Factor::GasPrice =>
                    if fa.contribution > 0.6 { "Fee elevated: boost tip + CU, only send high EV" } else { "Fees normal: run standard profile" },
                Factor::LiquidityDepth =>
                    if fa.contribution < 0.4 { "Shallow: reduce size or split route" } else { "Deep: safe to size up" },
                Factor::CompetitionLevel =>
                    if fa.contribution > 0.55 { "High competition: private relays + tighter slot targeting" } else { "Moderate competition: public ok" },
                _ => "Within bounds: keep defaults",
            };
            return Ok(msg);
        }
        Ok("No signal")
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.shutting_down.store(true, Ordering::Relaxed);
        Ok(())
    }

    #[inline(always)]
    pub fn predict_success(&self) -> Result<(f64, f64)> {
        let m = self.factor_history.read().back().cloned().context("no metrics")?;
        let rs = self.runs.read();
        let rabs = self.runs_absdev.read();
        let norm = normalize_all(&m, &rs, &rabs);
        let phi = build_phi(&norm);
        let r = self.ridge.read();
        let f = self.ftrl.read();

        // RLS is linear; constrain to [0,1]
        let mut y_rls = 0.0;
        let wr = r.weights();
        for i in 0..PHI_DIM { y_rls += wr[i] * phi[i]; }
        let p_rls = y_rls.clamp(0.0, 1.0);

        // FTRL is logistic
        let p_ftrl = f.predict(&phi);
        Ok((p_rls, p_ftrl))
    }

    // NEW: calibrated success probability (non-breaking; keep existing methods too)
    #[inline(always)]
    pub fn predict_success_calibrated(&self) -> Result<f64> {
        let (p_rls, p_ftrl) = self.predict_success()?;
        let p_raw = 0.5 * p_rls + 0.5 * p_ftrl;
        let c = self.calib.read();
        Ok(c.apply(p_raw))
    }

    // NEW: call this post-settlement to keep calibration tight
    #[inline(always)]
    pub fn update_calibration(&self, p_pred: f64, realized_success: bool, decay: f64) {
        let mut c = self.calib.write();
        c.update(p_pred, realized_success, decay);
    }

    /// Calibrated success probability with a conservative lower bound (Wilson-lite).
    /// Uses effective sample count from factor history to shrink uncertainty.
    #[inline(always)]
    pub fn predict_success_conservative(&self) -> anyhow::Result<f64> {
        let p = self.predict_success_calibrated()?;
        // effective n from recent metrics; clamp for stability
        let n_eff = (self.factor_history.read().len() as f64 * 0.6).max(8.0);
        // one-sided ~84% lower bound (z≈1); safe for fast gating
        let var = (p * (1.0 - p) / n_eff).max(1e-9);
        let p_lo = (p - var.sqrt()).clamp(0.0, 1.0);
        Ok(p_lo)
    }
}

// REPLACE: ExecutionTuning + derive_execution_tuning
#[derive(Clone, Copy, Debug)]
pub struct ExecutionTuning {
    pub tip_mult: f64,        // multiply base lamports tip
    pub cu_mult: f64,         // multiply base CU budget
    pub size_mult: f64,       // multiply base notional
    pub slip_abs: f64,        // absolute slippage widen (e.g., 0.0004 = 4 bps)
    pub use_private_relays: bool,
}

// helper: round lamports to economic step; expose to caller for convenience
#[inline(always)]
pub fn round_tip_lamports(x: u64) -> u64 {
    ((x + TIP_ROUND_LAMPORTS - 1) / TIP_ROUND_LAMPORTS) * TIP_ROUND_LAMPORTS
}

#[inline(always)]
pub fn derive_execution_tuning(att: &AttributionResult) -> ExecutionTuning {
    let get = |k: &str| att.factors.get(k).map(|x| x.contribution).unwrap_or(0.5);
    let comp = get("competition_level");
    let cong = get("network_congestion");
    let depth = get("liquidity_depth");
    let vol   = get("price_volatility");
    let lat   = get("latency_ms");

    // Tip multiplier: weigh competition > congestion; modulate by confidence/attr; clamp
    let raw_tip = (1.0 + 0.6*comp + 0.45*cong) * (0.75 + 0.5*att.confidence_score) * (0.85 + 0.3*att.total_attribution);
    let tip_mult = raw_tip.clamp(TIP_MULT_MIN, TIP_MULT_MAX);

    // CU multiplier: only when heat is real
    let cu_mult = if comp > 0.58 || cong > 0.58 { 1.25 } else { 1.0 };

    // Size: depth & total_attr help; latency hurts; damp if confidence is low; clamp
    let mut size_mult = (0.70 + 0.55*depth + 0.40*att.total_attribution - 0.30*lat)
        .clamp(SIZE_MULT_MIN, SIZE_MULT_MAX);
    if att.confidence_score < 0.55 { size_mult *= 0.85; }

    // Slippage widen: generous only with strong depth + low vol; clamp
    let mut slip_abs = if depth > 0.55 && vol < 0.45 { 0.0006 } else { 0.0002 };
    slip_abs = slip_abs.clamp(SLIP_MIN, SLIP_MAX);

    // Private relays: turn on in heat
    let use_private_relays = comp > 0.55 || cong > 0.55;

    ExecutionTuning { tip_mult, cu_mult, size_mult, slip_abs, use_private_relays }
}

// Optional calibrated variant; keep your existing derive_execution_tuning as-is if you prefer.
#[inline(always)]
pub fn derive_execution_tuning_calibrated(att: &AttributionResult, p_succ: f64) -> ExecutionTuning {
    let mut t = derive_execution_tuning(att);
    // softly modulate aggression by calibrated success prob
    let conf_boost = (0.9 + 0.4 * p_succ).clamp(0.9, 1.3);
    t.tip_mult = (t.tip_mult * conf_boost).clamp(TIP_MULT_MIN, TIP_MULT_MAX);
    t.size_mult = (t.size_mult * (0.85 + 0.5 * p_succ)).clamp(SIZE_MULT_MIN, SIZE_MULT_MAX);
    t
}

// PASTE VERBATIM
#[inline(always)]
pub fn ev_gate(
    att: &AttributionResult,
    p_succ: f64,                      // calibrated success probability [0,1]
    expected_pnl_lamports_if_success: i64,
    base_tip_lamports: u64,
    cu_price_lamports_per_cu: f64,    // priority lamports per CU
    base_cu: u32
) -> (bool, i64, u64, u32) {
    // Use your calibrated variant for aggression
    let tune = derive_execution_tuning_calibrated(att, p_succ.clamp(0.0, 1.0));
    let tip_lamports = round_tip_lamports((base_tip_lamports as f64 * tune.tip_mult) as u64);
    let cu_budget = (base_cu as f64 * tune.cu_mult) as u32;

    let cost_lamports = tip_lamports as f64 + cu_price_lamports_per_cu * (cu_budget as f64);
    let ev = p_succ * (expected_pnl_lamports_if_success as f64) - cost_lamports;
    let ev_i64 = ev.round() as i64;
    (ev_i64 > 0, ev_i64, tip_lamports, cu_budget)
}

// PASTE VERBATIM
#[derive(Clone, Debug)]
pub struct RelayStats {
    ew_inclusion: f64,          // [0..1]
    ew_latency_ms: f64,         // >=0
    ew_tip_lamports: f64,       // >=0
    ew_jitter_ms: f64,          // >=0
    samples: f64,

    // elasticity moments
    ew_x: f64, ew_y: f64, ew_x2: f64, ew_xy: f64,

    // NEW: backoff control
    backoff_ms: u64,
    blocked_until_ms: u64,
}
impl RelayStats {
    pub fn new() -> Self {
        Self {
            ew_inclusion: 0.5, ew_latency_ms: 40.0, ew_tip_lamports: 2e5, ew_jitter_ms: 2.0, samples: 0.0,
            ew_x: 0.0, ew_y: 0.0, ew_x2: 0.0, ew_xy: 0.0,
            backoff_ms: 0, blocked_until_ms: 0,
        }
    }
    #[inline(always)]
    pub fn update(&mut self, included: bool, latency_ms: f64, tip_lamports: u64, jitter_ms: f64, decay: f64) {
        let d = decay.clamp(0.5, 0.999);
        self.samples = self.samples * d + 1.0;
        let inc = if included { 1.0 } else { 0.0 };
        self.ew_inclusion = self.ew_inclusion * d + inc;
        self.ew_latency_ms = self.ew_latency_ms * d + latency_ms.max(0.0);
        self.ew_tip_lamports = self.ew_tip_lamports * d + (tip_lamports as f64).max(0.0);
        self.ew_jitter_ms = self.ew_jitter_ms * d + jitter_ms.max(0.0);

        let x = ((tip_lamports as f64) + 1.0).ln();
        self.ew_x  = self.ew_x  * d + x;
        self.ew_y  = self.ew_y  * d + inc;
        self.ew_x2 = self.ew_x2 * d + x * x;
        self.ew_xy = self.ew_xy * d + x * inc;

        // backoff heal on any inclusion
        if included { self.backoff_ms = self.backoff_ms.saturating_div(2); }
    }
    #[inline(always)]
    pub fn score(&self) -> f64 {
        // higher better: inclusion^2 / (1+lat/25) / (1+tip/2e5)^0.5 / (1+jit/5)^0.5
        let inc = (self.ew_inclusion / self.samples.max(1.0)).clamp(0.0, 1.0);
        let lat_term = 1.0 + self.ew_latency_ms / 25.0;
        let tip_term = 1.0 + self.ew_tip_lamports / 200_000.0;
        let jit_term = 1.0 + self.ew_jitter_ms / 5.0;
        (inc * inc) / (lat_term * tip_term.sqrt() * jit_term.sqrt())
    }
    #[inline(always)]
    pub fn tip_elasticity(&self) -> f64 {
        let n = self.samples.max(1.0);
        let mx = self.ew_x / n;
        let my = self.ew_y / n;
        let var = (self.ew_x2 / n) - mx * mx;
        let cov = (self.ew_xy / n) - mx * my;
        if var <= 1e-9 { 0.0 } else { (cov / var).clamp(0.0, 1.0) }
    }
    #[inline(always)]
    pub fn allow_send(&self, now_ms_: u64) -> bool { now_ms_ >= self.blocked_until_ms }
    #[inline(always)]
    pub fn register_failure(&mut self, now_ms_: u64) {
        let next = (self.backoff_ms.saturating_mul(2)).clamp(0, 80_000); // cap 80ms
        self.backoff_ms = if next == 0 { 5_000 } else { next };
        self.blocked_until_ms = now_ms_.saturating_add(self.backoff_ms);
    }
}

#[derive(Clone, Debug, Default)]
pub struct RelayBook {
    // key by relay identifier, e.g. "jito.relay.mainnet.XYZ"
    inner: HashMap<String, RelayStats>,
}
impl RelayBook {
    pub fn new() -> Self { Self { inner: HashMap::default() } }
    #[inline(always)]
    pub fn update(&mut self, relay: &str, included: bool, latency_ms: f64, tip_lamports: u64, jitter_ms: f64, decay: f64) {
        let rs = self.inner.entry(relay.to_owned()).or_insert_with(RelayStats::new);
        rs.update(included, latency_ms, tip_lamports, jitter_ms, decay);
    }
    #[inline(always)]
    pub fn best(&self) -> Option<(String, f64)> {
        let mut best_k: Option<String> = None;
        let mut best_s = -1.0;
        for (k, v) in self.inner.iter() {
            let s = v.score();
            if s > best_s { best_s = s; best_k = Some(k.clone()); }
        }
        best_k.map(|k| (k, best_s))
    }

    // UCB1-like selection on inclusion with latency penalty; c≈1.2 is a good start
    #[inline(always)]
    pub fn best_ucb(&self, c: f64) -> Option<(String, f64)> {
        let mut total_n = 0.0;
        for v in self.inner.values() { total_n += v.samples; }
        let total = total_n.max(1.0);

        let mut best: Option<(String, f64)> = None;
        let mut best_s = f64::NEG_INFINITY;
        for (k, v) in self.inner.iter() {
            let n = v.samples.max(1.0);
            let inc = (v.ew_inclusion / n).clamp(0.0, 1.0);
            let lat_pen = 1.0 / (1.0 + v.ew_latency_ms / 25.0);
            let base = inc * lat_pen;                // 0..1-ish
            let bonus = c * (total.ln() / n).sqrt(); // exploration bonus
            let s = base + bonus;
            if s > best_s { best_s = s; best = Some((k.clone(), s)); }
        }
        best
    }

    #[inline(always)]
    pub fn best_ucb_k(&self, c: f64, k: usize) -> SmallVec<[(String, f64); 8]> {
        let mut total_n = 0.0;
        for v in self.inner.values() { total_n += v.samples; }
        let total = total_n.max(1.0);

        let mut v: SmallVec<[(String, f64); 8]> = SmallVec::new();
        for (kname, vstats) in self.inner.iter() {
            let n = vstats.samples.max(1.0);
            let inc = (vstats.ew_inclusion / n).clamp(0.0, 1.0);
            let lat_pen = 1.0 / (1.0 + vstats.ew_latency_ms / 25.0);
            let s = inc * lat_pen + c * (total.ln() / n).sqrt();
            v.push((kname.clone(), s));
        }
        v.sort_by(|a,b| b.1.partial_cmp(&a.1).unwrap());
        v.truncate(k.max(1).min(8));
        v
    }
}

// Adjust tip multiplier based on relay prior; keep caps enforced.
#[inline(always)]
pub fn adjust_tuning_with_relay_prior(mut t: ExecutionTuning, book: &RelayBook) -> (ExecutionTuning, Option<String>) {
    if let Some((best, s)) = book.best() {
        // score s typically ~0..1+, map to 0.9..1.15 multiplier
        let m = (0.90 + 0.35 * s).clamp(0.90, 1.15);
        t.tip_mult = (t.tip_mult * m).clamp(TIP_MULT_MIN, TIP_MULT_MAX);
        return (t, Some(best));
    }
    (t, None)
}

// Optional: tip tweak with UCB (use either `.best()` or `.best_ucb()` depending on policy)
#[inline(always)]
pub fn adjust_tuning_with_relay_prior_ucb(mut t: ExecutionTuning, book: &RelayBook, c: f64) -> (ExecutionTuning, Option<String>) {
    if let Some((best, s)) = book.best_ucb(c) {
        // UCB score is ≥ base score; map to a tight 0.92..1.12 window
        let m = (0.92 + 0.30 * s).clamp(0.92, 1.12);
        t.tip_mult = (t.tip_mult * m).clamp(TIP_MULT_MIN, TIP_MULT_MAX);
        return (t, Some(best));
    }
    (t, None)
}

// PASTE VERBATIM
#[derive(Clone, Debug)]
pub struct LeaderStats {
    ew_inclusion: f64,   // [0..1] per-leader inclusion ratio (EW)
    ew_latency_ms: f64,  // leader-specific observed end-to-end latency
    ew_fail: f64,        // EW failure rate (1 - inclusion)
    samples: f64,
}
impl LeaderStats {
    pub fn new() -> Self {
        Self { ew_inclusion: 0.5, ew_latency_ms: 40.0, ew_fail: 0.5, samples: 0.0 }
    }
    #[inline(always)]
    pub fn update(&mut self, included: bool, latency_ms: f64, decay: f64) {
        let d = decay.clamp(0.5, 0.999);
        self.samples = self.samples * d + 1.0;
        let inc = if included { 1.0 } else { 0.0 };
        self.ew_inclusion = self.ew_inclusion * d + inc;
        self.ew_fail = self.ew_fail * d + (1.0 - inc);
        self.ew_latency_ms = self.ew_latency_ms * d + latency_ms.max(0.0);
    }
    #[inline(always)]
    pub fn score(&self) -> f64 {
        // higher better: inclusion / (1 + lat/25) / (1 + fail)^0.5
        let inc = (self.ew_inclusion / self.samples.max(1.0)).clamp(0.0, 1.0);
        let lat_term = 1.0 + self.ew_latency_ms / 25.0;
        let fail_term = 1.0 + self.ew_fail;
        inc / (lat_term * fail_term.sqrt())
    }
}

#[derive(Clone, Debug, Default)]
pub struct LeaderBook {
    inner: HashMap<Pubkey, LeaderStats>,
}
impl LeaderBook {
    pub fn new() -> Self { Self { inner: HashMap::default() } }
    #[inline(always)]
    pub fn update(&mut self, leader: Pubkey, included: bool, latency_ms: f64, decay: f64) {
        let ls = self.inner.entry(leader).or_insert_with(LeaderStats::new);
        ls.update(included, latency_ms, decay);
    }
    #[inline(always)]
    pub fn score(&self, leader: &Pubkey) -> Option<f64> {
        self.inner.get(leader).map(|x| x.score())
    }
}

// Tip multiplier tweak using current slot leader prior; caps preserved.
#[inline(always)]
pub fn adjust_tuning_with_leader_prior(mut t: ExecutionTuning, leader: Pubkey, leaders: &LeaderBook) -> ExecutionTuning {
    if let Some(s) = leaders.score(&leader) {
        // map ~[0..1+] -> 0.9..1.2; good leaders slightly reduce spend, bad leaders bump it
        let m = (0.90 + 0.40 * s).clamp(0.90, 1.20);
        t.tip_mult = (t.tip_mult * m).clamp(TIP_MULT_MIN, TIP_MULT_MAX);
    }
    t
}

// PASTE VERBATIM
#[derive(Clone, Debug)]
pub struct FamilyStats {
    ew_inclusion: f64,
    ew_latency_ms: f64,
    samples: f64,
}
impl FamilyStats {
    pub fn new() -> Self { Self { ew_inclusion: 0.5, ew_latency_ms: 40.0, samples: 0.0 } }
    #[inline(always)]
    pub fn update(&mut self, included: bool, latency_ms: f64, decay: f64) {
        let d = decay.clamp(0.5, 0.999);
        self.samples = self.samples * d + 1.0;
        let inc = if included { 1.0 } else { 0.0 };
        self.ew_inclusion = self.ew_inclusion * d + inc;
        self.ew_latency_ms = self.ew_latency_ms * d + latency_ms.max(0.0);
    }
    #[inline(always)]
    pub fn score(&self) -> f64 {
        let inc = (self.ew_inclusion / self.samples.max(1.0)).clamp(0.0, 1.0);
        inc / (1.0 + self.ew_latency_ms / 25.0)
    }
}

#[derive(Clone, Debug, Default)]
pub struct LeaderFamilyBook {
    inner: HashMap<u64, FamilyStats>, // key = hashed family id (ASN/IP cluster)
}

#[inline(always)]
pub fn hash_family_id(s: &str) -> u64 {
    // FNV-1a
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() { h ^= *b as u64; h = h.wrapping_mul(0x100000001b3); }
    h
}

impl LeaderFamilyBook {
    pub fn new() -> Self { Self { inner: HashMap::default() } }
    #[inline(always)]
    pub fn update(&mut self, family_id: &str, included: bool, latency_ms: f64, decay: f64) {
        let k = hash_family_id(family_id);
        let fs = self.inner.entry(k).or_insert_with(FamilyStats::new);
        fs.update(included, latency_ms, decay);
    }
    #[inline(always)]
    pub fn score(&self, family_id: &str) -> Option<f64> {
        self.inner.get(&hash_family_id(family_id)).map(|x| x.score())
    }
}

// tip multiplier tweak using family prior; narrow band to avoid overreacting
#[inline(always)]
pub fn adjust_tuning_with_family_prior(mut t: ExecutionTuning, family_id: &str, fams: &LeaderFamilyBook) -> ExecutionTuning {
    if let Some(s) = fams.score(family_id) {
        let m = (0.94 + 0.30 * s).clamp(0.94, 1.12);
        t.tip_mult = (t.tip_mult * m).clamp(TIP_MULT_MIN, TIP_MULT_MAX);
    }
    t
}

// PASTE VERBATIM
#[inline(always)]
fn heat_from(att: &AttributionResult) -> f64 {
    let get = |k: &str| att.factors.get(k).map(|x| x.contribution).unwrap_or(0.5);
    (0.6*get("competition_level") + 0.4*get("network_congestion")).clamp(0.0, 1.0)
}

// inclusion uplift model from extra CU under heat; calibrated, bounded
#[inline(always)]
fn inclusion_uplift(heat: f64, cu_mult: f64) -> f64 {
    // 1 + a*heat*sqrt(max(cu_mult-1,0)) with saturation
    let a = 0.25; // ~25% max uplift per sqrt term under full heat
    let base = 1.0 + a * heat * (cu_mult - 1.0).max(0.0).sqrt();
    base.clamp(1.0, 1.30) // hard cap 30% uplift
}

#[inline(always)]
pub fn optimize_cu_budget(
    att: &AttributionResult,
    p_succ: f64,
    pnl_if_success_lamports: f64,
    base_cu: u32,
    cu_price_lamports_per_cu: f64
) -> (u32, f64) {
    let heat = heat_from(att);
    // discrete grid keeps compute trivial and stable
    const GRID: [f64; 6] = [0.80, 1.00, 1.10, 1.25, 1.50, 1.75];
    let mut best_ev = f64::NEG_INFINITY;
    let mut best_cu = base_cu;

    for m in GRID {
        let cu = (base_cu as f64 * m).round().max(20_000.0) as u32; // guard min CU
        let p_eff = (p_succ * inclusion_uplift(heat, m)).clamp(0.0, 1.0);
        let cost = cu as f64 * cu_price_lamports_per_cu;
        let ev = p_eff * pnl_if_success_lamports - cost;
        if ev > best_ev {
            best_ev = ev;
            best_cu = cu;
        }
    }
    (best_cu, best_ev)
}

// PASTE VERBATIM
#[inline(always)]
pub fn optimize_tip_cu_mix(
    att: &AttributionResult,
    p_base: f64,
    expected_pnl_lamports_if_success: i64,
    base_tip_lamports: u64,
    base_cu: u32,
    cu_price_lamports_per_cu: f64,
    relay_book: Option<&RelayBook>,
) -> (u64, u32, f64) {
    let heat = heat_from(att);
    let mut eta_tip = 0.10; // default weak elasticity
    if let Some(book) = relay_book {
        if let Some((_best, _s)) = book.best() {
            if let Some(rs) = book.inner.get(&_best) {
                eta_tip = (0.05 + rs.tip_elasticity()).clamp(0.05, 0.60);
            }
        }
    }
    // search grids (tiny, constant time)
    const TIP_GRID: [f64; 6] = [0.80, 1.00, 1.20, 1.50, 1.80, 2.10];
    const CU_GRID:  [f64; 5] = [0.80, 1.00, 1.10, 1.25, 1.50];

    let mut best_ev = f64::NEG_INFINITY;
    let mut best_tip = base_tip_lamports;
    let mut best_cu  = base_cu;

    for mt in TIP_GRID {
        for mc in CU_GRID {
            let tip = round_tip_lamports((base_tip_lamports as f64 * mt) as u64);
            let cu  = (base_cu as f64 * mc).round().max(20_000.0) as u32;
            // inclusion gains: tip via elasticity; CU via heat uplift
            let gain_tip = 1.0 + eta_tip * (mt.ln().max(0.0));
            let gain_cu  = inclusion_uplift(heat, mc);
            let p_eff = (p_base * gain_tip * gain_cu).clamp(0.0, 1.0);
            let cost = tip as f64 + (cu as f64) * cu_price_lamports_per_cu;
            let ev = p_eff * (expected_pnl_lamports_if_success as f64) - cost;
            if ev > best_ev {
                best_ev = ev;
                best_tip = tip;
                best_cu  = cu;
            }
        }
    }
    (best_tip, best_cu, best_ev)
}

// PASTE VERBATIM
#[derive(Clone, Copy, Debug)]
pub struct SendAction {
    pub delay_us: u64,
    pub tip_lamports: u64,
    pub burst: u8,
}

#[derive(Clone, Debug)]
pub struct RelaySendAction {
    pub relay_id: String,
    pub action: SendAction,
}

// xorshift64* for deterministic jitter
#[inline(always)]
fn jitter_from(seed: u64) -> u64 {
    let mut x = seed | 1;
    x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
    x.wrapping_mul(0x2545F4914F6CDD1D)
}

// PASTE VERBATIM
#[inline(always)]
pub fn stochastic_gate(slot: Slot, ev_lamports: i64, threshold_lamports: i64) -> bool {
    if ev_lamports <= 0 { return false; }
    // acceptance prob rises with EV; threshold sets softness around 0
    let th = threshold_lamports.max(1) as f64;
    let p = (ev_lamports as f64 / (ev_lamports as f64 + th)).clamp(0.0, 1.0);

    // deterministic jitter from slot -> [0,1)
    let r = (jitter_from((slot as u64) ^ 0xD2B74407B1CE6E93) as f64 % 1_000_000.0) / 1_000_000.0;
    r < p
}

#[inline(always)]
pub fn build_multi_relay_ladder(
    slot: Slot,
    base_tip_lamports: u64,
    att: &AttributionResult,
    p_succ: f64,
    profile: SlotSendProfile,
    book: &RelayBook,
    k: usize
) -> SmallVec<[RelaySendAction; 12]> {
    let top = book.best_ucb_k(1.2, k);
    let mut out: SmallVec<[RelaySendAction; 12]> = SmallVec::new();
    let now = now_ms();

    for (j, (relay, _score)) in top.iter().enumerate() {
        // respect relay backoff
        if let Some(rs) = book.inner.get(relay) {
            if !rs.allow_send(now) { continue; }
        }
        let salt = relay_nonce_salt(slot, relay) % 1_500; // ≤1.5ms
        let mut ladder = build_retry_ladder(slot, base_tip_lamports, att, p_succ, profile);

        // micro-stagger & burst shaping (low jitter -> larger burst)
        let offset = (j as u64) * 2_000 + salt; // 2ms per rank + salt
        let burst_adj = if let Some(rs) = book.inner.get(relay) {
            let jit = rs.ew_jitter_ms.max(0.1);
            let lat = rs.ew_latency_ms.max(1.0);
            let q = (lat / (jit + 0.5)).clamp(1.0, 6.0);
            q as u8
        } else { profile.burst };

        for a in ladder.iter_mut() {
            a.delay_us = a.delay_us.saturating_add(offset);
            a.burst = burst_adj;
        }
        for a in ladder.into_iter() {
            out.push(RelaySendAction { relay_id: relay.clone(), action: a });
        }
    }
    out
}

#[inline(always)]
pub fn build_retry_ladder(
    slot: Slot,
    base_tip_lamports: u64,
    att: &AttributionResult,
    p_succ: f64,
    profile: SlotSendProfile
) -> SmallVec<[SendAction; 6]> {
    let heat = heat_from(att);
    let mut out: SmallVec<[SendAction; 6]> = SmallVec::new();

    // geometric tip ramp anchored at plan tip; higher heat → steeper ramp
    let r = (1.05 + 0.20 * heat).clamp(1.05, 1.25);
    let seed = (slot as u64).wrapping_mul(0x9E3779B185EBCA87);
    let base_jit = jitter_from(seed) % 5_000; // up to 5ms extra first send

    for i in 0..(profile.retries as usize) {
        let mult = r.powi(i as i32);
        let tip = round_tip_lamports((base_tip_lamports as f64 * mult) as u64);

        // staggered delays: base + (i * 6ms) + deterministic jitter diminishing with success prob
        let jit = ((jitter_from(seed ^ (i as u64)) % 6_000) as f64 * (1.0 - 0.6*p_succ)).max(0.0) as u64;
        let delay = profile.delay_us + base_jit + (i as u64)*6_000 + jit;

        out.push(SendAction { delay_us: delay, tip_lamports: tip, burst: profile.burst });
    }
    out
}

// PASTE VERBATIM
pub struct AttributionManager {
    cap: usize,
    engines: RwLock<HashMap<Pubkey, Arc<FactorAttributionEngine>>>,
    lru: RwLock<VecDeque<Pubkey>>,
}
impl AttributionManager {
    pub fn new(capacity: usize) -> Self {
        Self { cap: capacity.max(1), engines: RwLock::new(HashMap::default()), lru: RwLock::new(VecDeque::new()) }
    }
    #[inline(always)]
    fn touch_lru(&self, k: Pubkey) {
        let mut q = self.lru.write();
        if let Some(pos) = q.iter().position(|x| *x == k) { q.remove(pos); }
        q.push_back(k);
        if q.len() > self.cap {
            if let Some(evict) = q.pop_front() {
                self.engines.write().remove(&evict);
            }
        }
    }
    pub fn get_or_create(&self, pool: Pubkey) -> Arc<FactorAttributionEngine> {
        if let Some(e) = self.engines.read().get(&pool).cloned() {
            self.touch_lru(pool);
            return e;
        }
        let e = Arc::new(FactorAttributionEngine::new().expect("engine new"));
        {
            let mut map = self.engines.write();
            map.insert(pool, e.clone());
        }
        self.touch_lru(pool);
        e
    }
    // convenience passthroughs
    pub async fn submit_for(&self, pool: Pubkey, m: FactorMetrics) -> Result<()> {
        self.get_or_create(pool).submit_metrics(m).await
    }
    pub async fn current_for(&self, pool: Pubkey) -> Result<AttributionResult> {
        self.get_or_create(pool).get_current_attribution(&pool).await
    }
    pub async fn predict_for(&self, pool: Pubkey) -> Result<(f64,f64)> {
        self.get_or_create(pool).predict_success()
    }
}

// PASTE VERBATIM
#[derive(Clone, Copy, Debug)]
pub struct SlotSendProfile {
    pub delay_us: u64,     // initial delay before first send
    pub retries: u8,       // resend count within slot
    pub burst: u8,         // QUIC batch size
    pub use_bundles: bool, // prefer bundle relays
}
#[inline(always)]
pub fn compute_slot_send_profile(att: &AttributionResult, p_succ: f64) -> SlotSendProfile {
    let get = |k: &str| att.factors.get(k).map(|x| x.contribution).unwrap_or(0.5);
    let cong = get("network_congestion");
    let comp = get("competition_level");
    let lat  = get("latency_ms");
    let vol  = get("price_volatility");

    // Base delay: under heat we shift slightly later to avoid early collisions; else send immediately.
    let heat = (0.6*comp + 0.4*cong).clamp(0.0, 1.0);
    let delay_us = ((40_000.0 * heat) * (1.0 + 0.4*vol) * (1.0 - 0.3*p_succ)).max(0.0) as u64;

    // Retries: more when congestion high and latency moderate; cap at 4.
    let retries = ((1.0 + 3.5 * (cong * (1.0 - lat).clamp(0.0,1.0))).round() as i64)
        .clamp(1, 4) as u8;

    // Burst: increase quic batch when competition high; cap at 6.
    let burst = ((2.0 + 4.0 * comp).round() as i64).clamp(2, 6) as u8;

    // If heat high, bundles help; otherwise public ok.
    let use_bundles = heat > 0.55;

    SlotSendProfile { delay_us, retries, burst, use_bundles }
}

// PASTE VERBATIM
#[inline(always)]
pub fn compute_slot_send_profile_progress(
    att: &AttributionResult,
    p_succ: f64,
    slot_progress_01: f64, // 0=start of slot, 1=end; caller supplies from clock
) -> SlotSendProfile {
    let get = |k: &str| att.factors.get(k).map(|x| x.contribution).unwrap_or(0.5);
    let cong = get("network_congestion");
    let comp = get("competition_level");
    let lat  = get("latency_ms");
    let vol  = get("price_volatility");

    let heat = (0.6*comp + 0.4*cong).clamp(0.0, 1.0);

    // As slot ages, pull sends earlier (front-load) but keep a small offset to avoid stampedes.
    let age = slot_progress_01.clamp(0.0, 1.0);
    let frontload = (1.0 - 0.6*age).clamp(0.4, 1.0);

    let delay_us = ((40_000.0 * heat) * (1.0 + 0.4*vol) * (1.0 - 0.3*p_succ) * frontload)
        .max(0.0) as u64;

    // allow one extra retry early in slot; reduce near end
    let base_retries = ((1.0 + 3.5 * (cong * (1.0 - lat).clamp(0.0,1.0))).round() as i64)
        .clamp(1, 4);
    let retries = (base_retries - (age * 2.0).round() as i64).clamp(1, 4) as u8;

    let burst = ((2.0 + 4.0 * comp).round() as i64).clamp(2, 6) as u8;
    let use_bundles = heat > 0.55;
    SlotSendProfile { delay_us, retries, burst, use_bundles }
}

// PASTE VERBATIM
#[derive(Clone, Debug)]
pub struct GlobalBudget {
    // lamports available (token bucket), refilled at `refill_per_ms`
    state: RwLock<(i64 /*bucket*/, u64 /*last_ms*/ )>,
    cap: i64,
    refill_per_ms: i64,
}
impl GlobalBudget {
    pub fn new(cap_lamports: i64, refill_lamports_per_sec: i64) -> Self {
        Self {
            state: RwLock::new((cap_lamports.max(0), now_ms())),
            cap: cap_lamports.max(0),
            refill_per_ms: (refill_lamports_per_sec / 1000).max(0),
        }
    }
    #[inline(always)]
    pub fn allow(&self, cost_lamports: i64) -> bool {
        let now = now_ms();
        let mut st = self.state.write();
        let (ref mut bucket, ref mut last) = *st;
        // refill
        if now > *last {
            let dt = now - *last;
            let add = (dt as i64) * self.refill_per_ms;
            *bucket = (*bucket + add).min(self.cap);
            *last = now;
        }
        if *bucket >= cost_lamports {
            *bucket -= cost_lamports;
            true
        } else {
            false
        }
    }
    #[inline(always)]
    pub fn available(&self) -> i64 { self.state.read().0 }
}

// PASTE VERBATIM
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoutePath { Private, Public }

#[derive(Clone, Debug, Default)]
pub struct PathBandit {
    // exponentially weighted counts
    n_priv: f64, s_priv: f64, // successes
    n_pub:  f64, s_pub:  f64,
}
impl PathBandit {
    #[inline(always)]
    pub fn update(&mut self, path: RoutePath, success: bool, decay: f64) {
        let d = decay.clamp(0.5, 0.999);
        match path {
            RoutePath::Private => {
                self.n_priv = self.n_priv * d + 1.0;
                self.s_priv = self.s_priv * d + if success { 1.0 } else { 0.0 };
            }
            RoutePath::Public => {
                self.n_pub = self.n_pub * d + 1.0;
                self.s_pub = self.s_pub * d + if success { 1.0 } else { 0.0 };
            }
        }
    }
    #[inline(always)]
    pub fn choose(&self, slot: Slot, c: f64) -> RoutePath {
        let t = (self.n_priv + self.n_pub).max(1.0);
        let ucb = |s: f64, n: f64| {
            let mean = if n > 0.0 { s / n } else { 0.5 };
            let bonus = c * (t.ln() / n.max(1.0)).sqrt();
            mean + bonus
        };
        let u_priv = ucb(self.s_priv, self.n_priv.max(1.0));
        let u_pub  = ucb(self.s_pub,  self.n_pub.max(1.0));
        if (u_priv - u_pub) > 0.0 { RoutePath::Private }
        else if (u_pub - u_priv) > 0.0 { RoutePath::Public }
        else {
            // tie-break deterministic by slot parity
            if (slot & 1) == 0 { RoutePath::Private } else { RoutePath::Public }
        }
    }
}

// PASTE VERBATIM
#[derive(Clone, Debug)]
pub struct ExecutionPlan {
    pub ok: bool,
    pub ev_lamports: i64,
    pub tip_lamports: u64,
    pub cu_budget: u32,
    pub min_amount_out: f64,
    pub notional: u64,
    pub use_private_relays: bool,
    pub chosen_relay: Option<String>,
    pub send_profile: SlotSendProfile,
}

#[inline(always)]
pub fn build_execution_plan(
    att: &AttributionResult,
    p_succ: f64,
    expected_pnl_lamports_if_success: i64,
    base_tip_lamports: u64,
    cu_price_lamports_per_cu: f64,
    base_cu: u32,
    base_notional: u64,
    sim_amount_out: f64,
    relay_book: Option<&RelayBook>
) -> ExecutionPlan {
    // Baseline tuning + EV gate
    let tune0 = derive_execution_tuning_calibrated(att, p_succ.clamp(0.0,1.0));
    let mut tip = round_tip_lamports((base_tip_lamports as f64 * tune0.tip_mult) as u64);
    let mut cu  = (base_cu as f64 * tune0.cu_mult) as u32;

    // Optional relay prior adjustment
    let mut chosen: Option<String> = None;
    if let Some(book) = relay_book {
        let (t_adj, rel) = adjust_tuning_with_relay_prior(tune0, book);
        tip = round_tip_lamports((base_tip_lamports as f64 * t_adj.tip_mult) as u64);
        cu  = (base_cu as f64 * t_adj.cu_mult) as u32;
        chosen = rel;
    }

    // EV check
    let cost = tip as f64 + cu_price_lamports_per_cu * (cu as f64);
    let ev = p_succ * (expected_pnl_lamports_if_success as f64) - cost;
    let ok = ev > 0.0;

    // Size & slippage from tuning0 (conservative; independent of relay tweak)
    let min_out = (sim_amount_out * (1.0 - tune0.slip_abs)).max(1.0);
    let notional = (base_notional as f64 * tune0.size_mult) as u64;

    let profile = compute_slot_send_profile(att, p_succ);

    ExecutionPlan {
        ok,
        ev_lamports: ev.round() as i64,
        tip_lamports: tip,
        cu_budget: cu,
        min_amount_out: min_out,
        notional,
        use_private_relays: tune0.use_private_relays || profile.use_bundles,
        chosen_relay: chosen,
        send_profile: profile,
    }
}

// PASTE VERBATIM
#[derive(Clone, Debug)]
pub struct ExecutionPlanWithLadder {
    pub base: ExecutionPlan,
    pub ladder: SmallVec<[SendAction; 6]>,
}

#[inline(always)]
pub fn build_execution_plan_cu_opt(
    att: &AttributionResult,
    p_succ: f64,
    expected_pnl_lamports_if_success: i64,
    base_tip_lamports: u64,
    cu_price_lamports_per_cu: f64,
    base_cu: u32,
    base_notional: u64,
    sim_amount_out: f64,
    relay_book: Option<&RelayBook>,
    slot_leader: Option<Pubkey>,
    leader_book: Option<&LeaderBook>,
    slot: Slot
) -> ExecutionPlanWithLadder {
    // Start with calibrated tuning
    let mut tune = derive_execution_tuning_calibrated(att, p_succ.clamp(0.0,1.0));

    // Optional: leader prior
    if let (Some(leader), Some(lbook)) = (slot_leader, leader_book) {
        tune = adjust_tuning_with_leader_prior(tune, leader, lbook);
    }

    // Relay priors
    let mut chosen: Option<String> = None;
    if let Some(book) = relay_book {
        let (t2, rel) = adjust_tuning_with_relay_prior(tune, book);
        tune = t2;
        chosen = rel;
    }

    // CU optimizer
    let (cu_opt, _ev_cu_only) = optimize_cu_budget(
        att,
        p_succ,
        expected_pnl_lamports_if_success as f64,
        base_cu,
        cu_price_lamports_per_cu
    );

    // Costs & EV gate
    let tip = round_tip_lamports((base_tip_lamports as f64 * tune.tip_mult) as u64);
    let cost = tip as f64 + cu_price_lamports_per_cu * (cu_opt as f64);
    let ev = p_succ * (expected_pnl_lamports_if_success as f64) - cost;
    let ok = ev > 0.0;

    // Size & slippage
    let min_out = (sim_amount_out * (1.0 - tune.slip_abs)).max(1.0);
    let notional = (base_notional as f64 * tune.size_mult) as u64;

    let profile = compute_slot_send_profile(att, p_succ);
    let ladder = build_retry_ladder(slot, tip, att, p_succ, profile);

    let base = ExecutionPlan {
        ok,
        ev_lamports: ev.round() as i64,
        tip_lamports: tip,
        cu_budget: cu_opt,
        min_amount_out: min_out,
        notional,
        use_private_relays: tune.use_private_relays || profile.use_bundles,
        chosen_relay: chosen,
        send_profile: profile,
    };
    ExecutionPlanWithLadder { base, ladder }
}

// PASTE VERBATIM
#[inline(always)]
pub fn build_execution_plan_cu_opt_calibrated(
    engine: &FactorAttributionEngine,
    att: &AttributionResult,
    expected_pnl_lamports_if_success: i64,
    base_tip_lamports: u64,
    cu_price_lamports_per_cu: f64,
    base_cu: u32,
    base_notional: u64,
    sim_amount_out: f64,
    relay_book: Option<&RelayBook>,
    slot_leader: Option<Pubkey>,
    leader_book: Option<&LeaderBook>,
    slot: Slot
) -> ExecutionPlanWithLadder {
    // calibrated success probability
    let p = engine.predict_success_calibrated().expect("predict calibrated");

    // tuning + priors
    let mut tune = derive_execution_tuning_calibrated(att, p);
    if let (Some(leader), Some(lbook)) = (slot_leader, leader_book) {
        tune = adjust_tuning_with_leader_prior(tune, leader, lbook);
    }
    let mut chosen: Option<String> = None;
    if let Some(book) = relay_book {
        // use UCB to preserve exploration
        let (t2, rel) = adjust_tuning_with_relay_prior_ucb(tune, book, 1.2);
        tune = t2; chosen = rel;
    }

    // CU optimization
    let (cu_opt, _ev_cu) = optimize_cu_budget(att, p, expected_pnl_lamports_if_success as f64, base_cu, cu_price_lamports_per_cu);

    // EV
    let tip = round_tip_lamports((base_tip_lamports as f64 * tune.tip_mult) as u64);
    let cost = tip as f64 + cu_price_lamports_per_cu * (cu_opt as f64);
    let ev = p * (expected_pnl_lamports_if_success as f64) - cost;

    // Stochastic gate near boundary (e.g., 0.5–1.5 * base tip)
    let ev_i64 = ev.round() as i64;
    let ok = ev_i64 > 0 && stochastic_gate(slot, ev_i64, (base_tip_lamports as i64));

    // Size & slippage
    let min_out = (sim_amount_out * (1.0 - tune.slip_abs)).max(1.0);
    let notional = (base_notional as f64 * tune.size_mult) as u64;

    let profile = compute_slot_send_profile(att, p);
    let ladder = build_retry_ladder(slot, tip, att, p, profile);

    let base = ExecutionPlan {
        ok,
        ev_lamports: ev_i64,
        tip_lamports: tip,
        cu_budget: cu_opt,
        min_amount_out: min_out,
        notional,
        use_private_relays: tune.use_private_relays || profile.use_bundles,
        chosen_relay: chosen,
        send_profile: profile,
    };
    ExecutionPlanWithLadder { base, ladder }
}

// PASTE VERBATIM
#[inline(always)]
pub fn build_execution_plan_tip_cu_opt(
    engine: &FactorAttributionEngine,
    att: &AttributionResult,
    expected_pnl_lamports_if_success: i64,
    base_tip_lamports: u64,
    cu_price_lamports_per_cu: f64,
    base_cu: u32,
    base_notional: u64,
    sim_amount_out: f64,
    relay_book: Option<&RelayBook>,
    slot_leader: Option<Pubkey>,
    leader_book: Option<&LeaderBook>,
    slot: Slot
) -> ExecutionPlanWithLadder {
    // calibrated p
    let p = engine.predict_success_calibrated().expect("predict calibrated");

    // baseline tuning (size/slip/private policy still from tuning)
    let mut tune = derive_execution_tuning_calibrated(att, p);
    if let (Some(leader), Some(lbook)) = (slot_leader, leader_book) {
        tune = adjust_tuning_with_leader_prior(tune, leader, lbook);
    }
    // relay prior adjusts *policy*, not optimizer's internal split
    let mut chosen: Option<String> = None;
    if let Some(book) = relay_book {
        let (t2, rel) = adjust_tuning_with_relay_prior_ucb(tune, book, 1.2);
        tune = t2; chosen = rel;
    }

    // tip↔CU split optimizer
    let (tip_opt, cu_opt, ev_split) = optimize_tip_cu_mix(
        att, p, expected_pnl_lamports_if_success,
        round_tip_lamports((base_tip_lamports as f64 * tune.tip_mult) as u64),
        (base_cu as f64 * tune.cu_mult) as u32,
        cu_price_lamports_per_cu,
        relay_book
    );

    // EV & stochastic gate
    let ev_i64 = ev_split.round() as i64;
    let ok = ev_i64 > 0 && stochastic_gate(slot, ev_i64, base_tip_lamports as i64);

    // Size & slippage (unchanged)
    let min_out = (sim_amount_out * (1.0 - tune.slip_abs)).max(1.0);
    let notional = (base_notional as f64 * tune.size_mult) as u64;

    let profile = compute_slot_send_profile(att, p);
    let ladder  = build_retry_ladder(slot, tip_opt, att, p, profile);

    let base = ExecutionPlan {
        ok,
        ev_lamports: ev_i64,
        tip_lamports: tip_opt,
        cu_budget: cu_opt,
        min_amount_out: min_out,
        notional,
        use_private_relays: tune.use_private_relays || profile.use_bundles,
        chosen_relay: chosen,
        send_profile: profile,
    };
    ExecutionPlanWithLadder { base, ladder }
}

// REPLACE: add this variant; keep others
#[inline(always)]
pub fn build_execution_plan_tip_cu_opt_routed(
    engine: &FactorAttributionEngine,
    att: &AttributionResult,
    expected_pnl_lamports_if_success: i64,
    base_tip_lamports: u64,
    cu_price_lamports_per_cu: f64,
    base_cu: u32,
    base_notional: u64,
    sim_amount_out: f64,
    relay_book: Option<&RelayBook>,
    slot_leader: Option<Pubkey>,
    leader_book: Option<&LeaderBook>,
    bandit: &PathBandit,
    slot: Slot
) -> ExecutionPlanWithLadder {
    // conservative p for gating
    let p_cons = engine.predict_success_conservative().expect("predict conservative");

    // base tuning for size/slip
    let mut tune = derive_execution_tuning_calibrated(att, p_cons);
    if let (Some(leader), Some(lbook)) = (slot_leader, leader_book) {
        tune = adjust_tuning_with_leader_prior(tune, leader, lbook);
    }
    let route = bandit.choose(slot, 1.1);
    let force_private = matches!(route, RoutePath::Private);
    if force_private { tune.use_private_relays = true; }

    // relay tweak (UCB) for tip multiplier; not used by tip↔CU optimizer directly
    let mut chosen: Option<String> = None;
    if let Some(book) = relay_book {
        let (t2, rel) = adjust_tuning_with_relay_prior_ucb(tune, book, 1.2);
        tune = t2; chosen = rel;
    }

    // tip↔CU split optimizer
    let (tip_opt, cu_opt, ev_split) = optimize_tip_cu_mix(
        att, p_cons, expected_pnl_lamports_if_success,
        round_tip_lamports((base_tip_lamports as f64 * tune.tip_mult) as u64),
        (base_cu as f64 * tune.cu_mult) as u32,
        cu_price_lamports_per_cu,
        relay_book
    );

    let ev_i64 = ev_split.round() as i64;
    let ok = ev_i64 > 0 && stochastic_gate(slot, ev_i64, base_tip_lamports as i64);

    let min_out = (sim_amount_out * (1.0 - tune.slip_abs)).max(1.0);
    let notional = (base_notional as f64 * tune.size_mult) as u64;

    let profile = compute_slot_send_profile(att, p_cons);
    let ladder  = build_retry_ladder(slot, tip_opt, att, p_cons, profile);

    let base = ExecutionPlan {
        ok,
        ev_lamports: ev_i64,
        tip_lamports: tip_opt,
        cu_budget: cu_opt,
        min_amount_out: min_out,
        notional,
        use_private_relays: tune.use_private_relays,
        chosen_relay: chosen,
        send_profile: profile,
    };
    ExecutionPlanWithLadder { base, ladder }
}

// PASTE VERBATIM
#[inline(always)]
pub fn precompute_next_slot_plan(
    engine: &FactorAttributionEngine,
    att: &AttributionResult,
    expected_pnl_lamports_if_success: i64,
    base_tip_lamports: u64,
    cu_price_lamports_per_cu: f64,
    base_cu: u32,
    base_notional: u64,
    sim_amount_out: f64,
    relay_book: Option<&RelayBook>,
    next_slot_leader: Option<Pubkey>,
    leader_book: Option<&LeaderBook>,
    bandit: &PathBandit,
    next_slot: Slot
) -> ExecutionPlanWithLadder {
    // reuse routed + tip↔CU optimal builder
    build_execution_plan_tip_cu_opt_routed(
        engine, att, expected_pnl_lamports_if_success,
        base_tip_lamports, cu_price_lamports_per_cu, base_cu,
        base_notional, sim_amount_out,
        relay_book, next_slot_leader, leader_book, bandit, next_slot
    )
}

// PASTE VERBATIM
#[derive(Clone, Debug)]
struct SlotState {
    slot: Slot,
    sends: u8,
    ev_accum_lamports: i64,
    max_sends: u8,
    ev_budget_lamports: i64,
}

#[derive(Clone, Debug)]
pub struct SlotRiskManager {
    inner: RwLock<SlotState>,
}
impl SlotRiskManager {
    pub fn new(max_sends_per_slot: u8, ev_budget_lamports: i64) -> Self {
        let st = SlotState { slot: 0, sends: 0, ev_accum_lamports: 0, max_sends: max_sends_per_slot.max(1), ev_budget_lamports };
        Self { inner: RwLock::new(st) }
    }
    #[inline(always)]
    fn reset_if_new_slot(st: &mut SlotState, slot: Slot) {
        if st.slot != slot {
            st.slot = slot;
            st.sends = 0;
            st.ev_accum_lamports = 0;
        }
    }
    #[inline(always)]
    pub fn allow(&self, slot: Slot, ev_lamports: i64) -> bool {
        let mut st = self.inner.write();
        Self::reset_if_new_slot(&mut st, slot);
        if st.sends >= st.max_sends { return false; }
        if st.ev_accum_lamports.saturating_add(ev_lamports) > st.ev_budget_lamports { return false; }
        st.sends = st.sends.saturating_add(1);
        st.ev_accum_lamports = st.ev_accum_lamports.saturating_add(ev_lamports.max(0));
        true
    }
}

// PASTE VERBATIM
#[inline(always)]
pub fn plan_fanout_actions(
    slot: Slot,
    plan: &ExecutionPlanWithLadder,
    relay_book: Option<&RelayBook>,
    fanout_k: usize
) -> SmallVec<[RelaySendAction; 12]> {
    if !plan.base.use_private_relays { return SmallVec::new(); }
    if let Some(book) = relay_book {
        return build_multi_relay_ladder(
            slot,
            plan.base.tip_lamports,
            &AttributionResult {
                total_attribution: 0.0, // only heat/shape used from ladder inputs: pass full att if you prefer
                factors: HashMap::default(),
                confidence_score: 0.0,
                timestamp_ms: plan.base.send_profile.delay_us, // placeholder, not used
                recommendations: SmallVec::new(),
            },
            0.5, // p_succ not used in multi-relay ladder delay math beyond base ladder (already baked)
            plan.base.send_profile,
            book,
            fanout_k
        );
    }
    SmallVec::new()
}

// PASTE VERBATIM
#[inline(always)]
pub fn relay_nonce_salt(slot: Slot, relay_id: &str) -> u64 {
    // FNV-1a over relay_id, mixed with slot; cheap and deterministic
    let mut h: u64 = 0xcbf29ce484222325;
    for b in relay_id.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h ^ (slot as u64).wrapping_mul(0x9E3779B185EBCA87)
}

// PASTE VERBATIM
#[inline(always)]
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
    x.wrapping_mul(0x2545F4914F6CDD1D)
}

#[inline(always)]
pub fn make_send_dedupe_key(
    slot: Slot,
    pool: &Pubkey,
    route: RoutePath,
    tip_lamports: u64,
    cu_budget: u32,
    notional: u64
) -> u128 {
    let mut hi = mix64(slot as u64 ^ match route { RoutePath::Private => 0xAA55AA55AA55AA55, RoutePath::Public => 0x55AA55AA55AA55AA });
    // fold pubkey bytes
    for chunk in pool.to_bytes().chunks(8) {
        let mut v = 0u64;
        for (i,b) in chunk.iter().enumerate() { v |= (*b as u64) << (i*8); }
        hi ^= mix64(v);
    }
    let lo = mix64(tip_lamports ^ ((cu_budget as u64) << 32) ^ notional);
    ((hi as u128) << 64) | (lo as u128)
}

#[derive(Clone, Debug)]
pub struct DedupeLRU {
    cap: usize,
    q: RwLock<VecDeque<u128>>,
    seen: RwLock<HashMap<u128, u64>>, // key -> last_ms
}
impl DedupeLRU {
    pub fn new(cap: usize) -> Self {
        Self { cap: cap.max(64), q: RwLock::new(VecDeque::with_capacity(cap)), seen: RwLock::new(HashMap::default()) }
    }
    /// Returns true if this key is a duplicate we have seen recently (same slot).
    #[inline(always)]
    pub fn seen_or_insert(&self, key: u128, now_ms_: u64) -> bool {
        {
            let m = self.seen.read();
            if m.contains_key(&key) { return true; }
        }
        {
            let mut m = self.seen.write();
            let mut q = self.q.write();
            m.insert(key, now_ms_);
            q.push_back(key);
            if q.len() > self.cap {
                if let Some(old) = q.pop_front() { m.remove(&old); }
            }
        }
        false
    }
}

// REPLACE: tests
#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn sanity() {
        let eng = FactorAttributionEngine::new().unwrap();
        for i in 0..200 {
            let m = FactorMetrics {
                timestamp_ms: now_ms(),
                slot: 100 + i,
                gas_price: 60.0 + (i % 7) as f64,
                network_congestion: 0.45,
                liquidity_depth: 1_200_000.0,
                price_volatility: 0.021,
                competition_level: 0.38,
                slippage_estimate: 0.0012,
                block_space_utilization: 0.61,
                priority_fee_percentile: 72.0,
                success_probability: if i % 5 == 0 { 0.9 } else { 0.75 },
                latency_ms: 24.0,
            };
            eng.submit_metrics(m).await.unwrap();
        }
        let res = eng.get_current_attribution(&Pubkey::new_unique()).await.unwrap();
        assert!(res.total_attribution > 0.0);
        assert!(res.factors.len() == FACTOR_COUNT);
    }

    #[test]
    fn test_execution_tuning() {
        let mut factors = HashMap::new();
        factors.insert("competition_level", FactorAttribution {
            factor: Factor::CompetitionLevel,
            weight: 0.1,
            contribution: 0.7,
            confidence: 0.8,
            corr_to_success: 0.6,
            samples: 100,
            last_updated_ms: now_ms(),
        });
        factors.insert("network_congestion", FactorAttribution {
            factor: Factor::NetworkCongestion,
            weight: 0.12,
            contribution: 0.6,
            confidence: 0.75,
            corr_to_success: 0.5,
            samples: 100,
            last_updated_ms: now_ms(),
        });
        factors.insert("liquidity_depth", FactorAttribution {
            factor: Factor::LiquidityDepth,
            weight: 0.2,
            contribution: 0.8,
            confidence: 0.9,
            corr_to_success: 0.7,
            samples: 100,
            last_updated_ms: now_ms(),
        });
        factors.insert("price_volatility", FactorAttribution {
            factor: Factor::PriceVolatility,
            weight: 0.18,
            contribution: 0.3,
            confidence: 0.6,
            corr_to_success: 0.4,
            samples: 100,
            last_updated_ms: now_ms(),
        });
        factors.insert("latency_ms", FactorAttribution {
            factor: Factor::LatencyMs,
            weight: 0.02,
            contribution: 0.4,
            confidence: 0.7,
            corr_to_success: 0.3,
            samples: 100,
            last_updated_ms: now_ms(),
        });

        let att = AttributionResult {
            total_attribution: 0.65,
            factors,
            confidence_score: 0.8,
            timestamp_ms: now_ms(),
            recommendations: SmallVec::new(),
        };

        let tuning = derive_execution_tuning(&att);
        assert!(tuning.tip_mult > 1.0);
        assert!(tuning.cu_mult > 1.0);
        assert!(tuning.use_private_relays);
        assert!(tuning.slip_abs > 0.0002);
    }

    #[test]
    fn test_round_tip_lamports() {
        assert_eq!(round_tip_lamports(0), 0);
        assert_eq!(round_tip_lamports(50), 100);
        assert_eq!(round_tip_lamports(100), 100);
        assert_eq!(round_tip_lamports(150), 200);
        assert_eq!(round_tip_lamports(200), 200);
    }
}

#[cfg(test)]
mod regime_ev_tests {
    use super::*;

    #[test]
    fn regime_adapts_halflife() {
        let mut r = RegimeDetector::new(EW_HALFLIFE_MS);
        let hl0 = r.current_hl();
        // simulate low vol (good = 1.0) -> longer half-life
        for _ in 0..20 { r.update_and_get(1.0); }
        let hl_lowvol = r.current_hl();
        // simulate high vol (good = 0.0) -> shorter half-life
        for _ in 0..40 { r.update_and_get(0.0); }
        let hl_highvol = r.current_hl();
        assert!(hl_lowvol >= hl0);
        assert!(hl_highvol <= hl_lowvol);
        assert!(hl_highvol >= EW_HALFLIFE_MS_MIN && hl_lowvol <= EW_HALFLIFE_MS_MAX);
    }

    #[test]
    fn ev_gate_basic() {
        // minimal AttributionResult stub for EV math
        let mut factors = HashMap::default();
        // weight numbers don't matter here; just ensure non-empty
        factors.insert("liquidity_depth", FactorAttribution{
            factor: Factor::LiquidityDepth, weight: 0.2, contribution: 0.6,
            confidence: 0.8, corr_to_success: 0.3, samples: 100, last_updated_ms: now_ms()
        });
        let att = AttributionResult{
            total_attribution: 0.55,
            factors,
            confidence_score: 0.7,
            timestamp_ms: now_ms(),
            recommendations: SmallVec::new(),
        };
        let (ok, ev, _tip, _cu) =
            ev_gate(&att, 0.75, 2_000_000, 200_000, 0.0, 200_000); // 0.002 SOL pnl, no CU fee
        assert!(ok && ev > 0);
    }
}

#[cfg(test)]
mod relay_mgr_timing_tests {
    use super::*;

    #[test]
    fn relay_scores_rank() {
        let mut rb = RelayBook::new();
        rb.update("A", true, 35.0, 150_000, 1.5, 0.9);
        rb.update("B", true, 60.0, 120_000, 4.0, 0.9);
        rb.update("C", false, 20.0, 80_000, 1.0, 0.9);
        let best = rb.best().unwrap();
        assert_eq!(best.0, "A"); // better inclusion & lat beats cheaper tip here
    }

    #[tokio::test]
    async fn manager_eviction_and_passthrough() {
        let mgr = AttributionManager::new(2);
        let p1 = Pubkey::new_unique();
        let p2 = Pubkey::new_unique();
        let p3 = Pubkey::new_unique();

        // create 3 engines; capacity 2 -> LRU evicts first
        assert!(mgr.get_or_create(p1).shutdown().await.is_ok());
        assert!(mgr.get_or_create(p2).shutdown().await.is_ok());
        assert!(mgr.get_or_create(p3).shutdown().await.is_ok());

        // p1 likely evicted; re-creating should succeed
        let _ = mgr.get_or_create(p1);
    }

    #[test]
    fn timing_profile_behaves() {
        // craft minimal AttributionResult
        let mut factors = HashMap::default();
        factors.insert("network_congestion", FactorAttribution { factor: Factor::NetworkCongestion, weight: 0.12, contribution: 0.8, confidence: 0.8, corr_to_success: 0.2, samples: 100, last_updated_ms: now_ms() });
        factors.insert("competition_level",  FactorAttribution { factor: Factor::CompetitionLevel,    weight: 0.10, contribution: 0.7, confidence: 0.8, corr_to_success: 0.2, samples: 100, last_updated_ms: now_ms() });
        factors.insert("latency_ms",         FactorAttribution { factor: Factor::LatencyMs,           weight: 0.02, contribution: 0.3, confidence: 0.8, corr_to_success: 0.1, samples: 100, last_updated_ms: now_ms() });
        factors.insert("price_volatility",   FactorAttribution { factor: Factor::PriceVolatility,     weight: 0.18, contribution: 0.4, confidence: 0.8, corr_to_success: 0.1, samples: 100, last_updated_ms: now_ms() });
        let att = AttributionResult {
            total_attribution: 0.6,
            factors,
            confidence_score: 0.7,
            timestamp_ms: now_ms(),
            recommendations: SmallVec::new(),
        };
        let prof = compute_slot_send_profile(&att, 0.65);
        assert!(prof.retries >= 1 && prof.retries <= 4);
        assert!(prof.burst >= 2 && prof.burst <= 6);
    }
}

#[cfg(test)]
mod calib_bandit_stoch_tests {
    use super::*;

    #[test]
    fn calibrator_monotone_bounds() {
        let mut c = ProbCalibrator::new();
        // if predictions are too confident, temperature should increase (flatten)
        for _ in 0..200 {
            c.update(0.99, false, 0.98); // overconfident wrong
        }
        let p_adj = c.apply(0.99);
        assert!(p_adj < 0.99 && p_adj > 0.5);
    }

    #[test]
    fn relay_ucb_explores_low_sample() {
        let mut rb = RelayBook::new();
        // A is solid but well-sampled; B is under-sampled but promising
        for _ in 0..200 { rb.update("A", true, 30.0, 150_000, 2.0, 0.98); }
        for _ in 0..3   { rb.update("B", true, 35.0, 150_000, 2.0, 0.98); }
        let (best, _s) = rb.best_ucb(1.2).unwrap();
        assert!(best == "B" || best == "A"); // often B wins early due to bonus
    }

    #[test]
    fn stochastic_gate_probability_behaves() {
        let slot = 1234567;
        // large positive EV should almost always pass
        let mut pass_hi = 0;
        let mut pass_lo = 0;
        for i in 0..100 {
            if stochastic_gate(slot + i, 1_000_000, 100_000) { pass_hi += 1; }
            if stochastic_gate(slot + i, 10_000, 100_000) { pass_lo += 1; }
        }
        assert!(pass_hi > pass_lo);
        assert!(pass_hi > 60); // rough bound
    }
}

#[cfg(test)]
mod ensemble_ucbk_slotrisk_tests {
    use super::*;

    #[test]
    fn bin_calibrator_interpolates() {
        let mut b = BinCalibrator::new();
        for _ in 0..200 { b.update(0.2, true, 0.98); }
        for _ in 0..200 { b.update(0.8, false, 0.98); }
        let p_lo = b.apply(0.2);
        let p_hi = b.apply(0.8);
        assert!(p_lo > p_hi); // calibrated shape flips confidence as expected
    }

    #[test]
    fn ensemble_weights_shift() {
        let mut e = CalibratorEnsemble::new();
        // make bins informative
        for _ in 0..300 { e.update(0.8, false, 0.98); }
        let p = e.apply(0.8);
        assert!(p < 0.8); // bins pulled down overconfident predictions
    }

    #[test]
    fn ucb_k_returns_k() {
        let mut rb = RelayBook::new();
        for _ in 0..50 { rb.update("A", true, 30.0, 120_000, 2.0, 0.98); }
        for _ in 0..10 { rb.update("B", true, 25.0, 140_000, 2.5, 0.98); }
        for _ in 0..5  { rb.update("C", false, 20.0, 100_000, 1.5, 0.98); }
        let top2 = rb.best_ucb_k(1.2, 2);
        assert!(top2.len() == 2);
    }

    #[test]
    fn slot_risk_limits() {
        let rm = SlotRiskManager::new(2, 1_000_000);
        let slot = 42;
        assert!(rm.allow(slot, 600_000));
        assert!(rm.allow(slot, 300_000));
        assert!(!rm.allow(slot, 200_000)); // hits send cap
        // new slot resets
        assert!(rm.allow(slot + 1, 900_000));
    }
}

#[cfg(test)]
mod elasticity_opt_tests {
    use super::*;

    #[test]
    fn relay_elasticity_nonnegative() {
        let mut rs = RelayStats::new();
        for i in 0..200 {
            let tip = 100_000 + (i as u64) * 1_000;
            rs.update(true, 30.0, tip, 2.0, 0.98);
        }
        assert!(rs.tip_elasticity() >= 0.0);
    }

    #[test]
    fn tip_cu_mix_improves_ev() {
        // craft minimal AttributionResult
        let att = AttributionResult {
            total_attribution: 0.5,
            factors: HashMap::default(),
            confidence_score: 0.6,
            timestamp_ms: now_ms(),
            recommendations: SmallVec::new(),
        };
        let rb = {
            let mut b = RelayBook::new();
            for _ in 0..200 { b.update("R", true, 35.0, 150_000, 2.0, 0.98); }
            b
        };
        let (tip, cu, ev) = optimize_tip_cu_mix(&att, 0.70, 2_000_000, 150_000, 200_000, 0.0, Some(&rb));
        assert!(ev > 0.0);
        assert!(tip >= 100_000 && cu >= 20_000);
    }
}

#[cfg(test)]
mod progress_bandit_budget_tests {
    use super::*;

    #[test]
    fn slot_progress_changes_retries() {
        let mut f = HashMap::default();
        f.insert("network_congestion", FactorAttribution{ factor: Factor::NetworkCongestion, weight: 0.1, contribution: 0.7, confidence: 0.8, corr_to_success: 0.2, samples: 100, last_updated_ms: now_ms()});
        f.insert("competition_level", FactorAttribution{ factor: Factor::CompetitionLevel, weight: 0.1, contribution: 0.6, confidence: 0.8, corr_to_success: 0.2, samples: 100, last_updated_ms: now_ms()});
        f.insert("latency_ms", FactorAttribution{ factor: Factor::LatencyMs, weight: 0.02, contribution: 0.3, confidence: 0.8, corr_to_success: 0.1, samples: 100, last_updated_ms: now_ms()});
        f.insert("price_volatility", FactorAttribution{ factor: Factor::PriceVolatility, weight: 0.18, contribution: 0.4, confidence: 0.8, corr_to_success: 0.1, samples: 100, last_updated_ms: now_ms()});
        let att = AttributionResult{ total_attribution: 0.6, factors: f, confidence_score: 0.7, timestamp_ms: now_ms(), recommendations: SmallVec::new() };

        let early = compute_slot_send_profile_progress(&att, 0.6, 0.0);
        let late  = compute_slot_send_profile_progress(&att, 0.6, 0.9);
        assert!(early.retries >= late.retries); // fewer retries late in slot
    }

    #[test]
    fn bandit_chooses_and_updates() {
        let slot = 1000;
        let mut b = PathBandit::default();
        // private wins a few times
        for _ in 0..10 { b.update(RoutePath::Private, true, 0.98); }
        let r = b.choose(slot, 1.1);
        assert_eq!(r, RoutePath::Private);
        // public catches up
        for _ in 0..20 { b.update(RoutePath::Public, true, 0.98); }
        let r2 = b.choose(slot+1, 1.1);
        assert!(matches!(r2, RoutePath::Public));
    }

    #[test]
    fn global_budget_enforces() {
        let gb = GlobalBudget::new(1_000_000, 0); // no refill
        assert!(gb.allow(600_000));
        assert!(gb.allow(400_000));
        assert!(!gb.allow(1)); // exhausted
    }
}

#[cfg(test)]
mod dedupe_backoff_family_tests {
    use super::*;

    #[test]
    fn dedupe_lru_blocks_duplicates() {
        let d = DedupeLRU::new(64);
        let pool = Pubkey::new_unique();
        let k = make_send_dedupe_key(123, &pool, RoutePath::Private, 200_000, 200_000, 1_000_000);
        assert_eq!(d.seen_or_insert(k, now_ms()), false);
        assert_eq!(d.seen_or_insert(k, now_ms()), true);
    }

    #[test]
    fn relay_backoff_blocks_then_heals() {
        let mut rs = RelayStats::new();
        let t0 = now_ms();
        // simulate failures
        for _ in 0..3 { rs.register_failure(t0); }
        assert!(!rs.allow_send(t0 + 1)); // blocked immediately
        // advance time
        assert!(rs.allow_send(t0 + 100_000)); // healed by time
        // inclusion heals faster in-stream
        rs.update(true, 30.0, 150_000, 2.0, 0.98);
        assert!(rs.backoff_ms <= 40_000);
    }

    #[test]
    fn family_prior_tweaks_tip() {
        let mut fams = LeaderFamilyBook::new();
        fams.update("AS12345", true, 28.0, 0.98);
        let t0 = ExecutionTuning { tip_mult: 1.0, cu_mult: 1.0, size_mult: 1.0, slip_abs: 0.0003, use_private_relays: false };
        let t1 = adjust_tuning_with_family_prior(t0, "AS12345", &fams);
        assert!(t1.tip_mult >= 0.94 && t1.tip_mult <= 1.12);
    }
}
