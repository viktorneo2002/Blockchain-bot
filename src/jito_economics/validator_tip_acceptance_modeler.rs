use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH, Instant};
use solana_sdk::pubkey::Pubkey;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock as AsyncRwLock;
use anyhow::{Result, anyhow};
use chrono::{Utc, Timelike};
use dashmap::DashMap;
use ahash::RandomState as FastHash;
use arc_swap::ArcSwap;
use once_cell::sync::Lazy;
use prometheus::{register_histogram, register_histogram_vec, register_gauge, register_gauge_vec, register_int_counter_vec, Histogram, HistogramVec, IntCounterVec, Gauge, GaugeVec};
use futures::stream::{FuturesUnordered, StreamExt};
use tokio::task::JoinSet;
use tokio::sync::Semaphore;
use std::sync::atomic::{AtomicU64, Ordering};

// PIECE E21: lock-free network snapshot
#[repr(align(64))]
#[derive(Debug)]
struct NetworkAtomics {
    slot: AtomicU64,
    tps_bits: AtomicU64,
    congestion_bits: AtomicU64,
    avg_fee: AtomicU64,
    hour_idx: AtomicU64,
}

#[inline(always)]
fn wilson_upper_bound_from_ew(ew_accept: f64, ew_total: f64, z: f64) -> f64 {
    let n = ew_total.max(1e-9);
    let p = (ew_accept / n).clamp(0.0, 1.0);
    let z2 = z * z;
    let denom = 1.0 + z2 / n;
    let center = (p + z2 / (2.0 * n)) / denom;
    let margin = (z / denom) * ((p * (1.0 - p) / n + z2 / (4.0 * n * n)).max(0.0)).sqrt();
    (center + margin).clamp(0.0, 1.0)
}

// E36: lock-free monotonic guard shadow state
#[repr(align(64))]
struct MonoState {
    last_tip: AtomicU64,
    last_p_bits: AtomicU64,
}
impl MonoState {
    fn new() -> Self {
        Self { last_tip: AtomicU64::new(0), last_p_bits: AtomicU64::new(0f64.to_bits()) }
    }
    #[inline(always)]
    fn p_load(&self) -> f64 { f64::from_bits(self.last_p_bits.load(Ordering::Relaxed)) }
}

// E29: Wilson lower bound from exponentially-decayed counts
#[inline(always)]
fn wilson_lower_bound_from_ew(ew_accept: f64, ew_total: f64, z: f64) -> f64 {
    let n = ew_total.max(1e-9);
    let p = (ew_accept / n).clamp(0.0, 1.0);
    let z2 = z * z;
    let denom = 1.0 + z2 / n;
    let center = p + z2 / (2.0 * n);
    let rad = (p * (1.0 - p) / n + z2 / (4.0 * n * n)).max(0.0).sqrt();
    ((center - z * rad) / denom).clamp(0.0, 1.0)
}
impl NetworkAtomics {
    fn new() -> Self {
        Self {
            slot: AtomicU64::new(0),
            tps_bits: AtomicU64::new(0f64.to_bits()),
            congestion_bits: AtomicU64::new((0.5f64).to_bits()),
            avg_fee: AtomicU64::new(1_000),
            hour_idx: AtomicU64::new(0),
        }
    }
    #[inline(always)]
    fn read(&self) -> NetSnap {
        NetSnap {
            current_slot: self.slot.load(Ordering::Relaxed),
            current_tps: f64::from_bits(self.tps_bits.load(Ordering::Relaxed)),
            congestion_level: f64::from_bits(self.congestion_bits.load(Ordering::Relaxed)),
            average_priority_fee: self.avg_fee.load(Ordering::Relaxed),
            hour_idx: (self.hour_idx.load(Ordering::Relaxed) as u8) % 24,
        }
    }
    #[inline(always)]
    fn write_from(&self, nc: &NetworkConditions) {
        self.slot.store(nc.current_slot, Ordering::Relaxed);
        self.tps_bits.store(nc.current_tps.to_bits(), Ordering::Relaxed);
        self.congestion_bits.store(nc.congestion_level.to_bits(), Ordering::Relaxed);
        self.avg_fee.store(nc.average_priority_fee, Ordering::Relaxed);
        let h = chrono::Utc::now().hour() as u64;
        self.hour_idx.store(h % 24, Ordering::Relaxed);
    }
}

#[derive(Clone, Copy, Debug)]
struct NetSnap {
    current_slot: u64,
    current_tps: f64,
    congestion_level: f64,
    average_priority_fee: u64,
    hour_idx: u8,
}

const SLIDING_WINDOW_SIZE: usize = 1000;
const MIN_SAMPLE_SIZE: usize = 50;
const DECAY_FACTOR: f64 = 0.95;
const BASE_TIP_LAMPORTS: u64 = 10_000;
const MAX_TIP_LAMPORTS: u64 = 50_000_000;
const CONFIDENCE_THRESHOLD: f64 = 0.85;
const OUTLIER_THRESHOLD: f64 = 3.0;
const PRIORITY_FEE_PERCENTILE: f64 = 0.75;
// PIECE T11: EWMA smoothing factor for congestion
const CONGESTION_ALPHA: f64 = 0.20;
// PIECE E17: cap allocator concurrency
const ALLOC_MAX_CONCURRENCY: usize = 32;
// PIECE E23: metrics sampling rate (lg2). 0 = every event
const METRIC_SAMPLE_LG2: u32 = 0;
// E48: expected validators for initial map capacity
const EXPECTED_VALIDATORS: usize = 3000;
// E40: tip ramp limiter
const TIP_UP_MAX_MULT: f64 = 1.40;
const TIP_DN_MAX_MULT: f64 = 0.70;
const TIP_MIN_STEP:   u64  = 500;
// P25: time-aware EWMA half-life (seconds) tuned and stable
const EW_HALF_LIFE_SECS: f64 = 1800.0; // 30 minutes
#[inline(always)]
fn ew_decay(dt_secs: f64) -> f64 {
    (-std::f64::consts::LN_2 * (dt_secs.max(0.0) / EW_HALF_LIFE_SECS)).exp()
}

// E73: screen to top-K validators before water-filling
const WF_SCREEN_K: usize = 128;

// PIECE E24: cos(2π·hour/24) LUT
const COS_HOUR: [f64; 24] = [
    1.0000000000000000,  0.9659258262890683,  0.8660254037844386,  0.7071067811865476,
    0.5000000000000001,  0.2588190451025208,  0.0000000000000001, -0.2588190451025207,
   -0.5000000000000000, -0.7071067811865475, -0.8660254037844386, -0.9659258262890683,
   -1.0000000000000000, -0.9659258262890684, -0.8660254037844387, -0.7071067811865477,
   -0.5000000000000004, -0.2588190451025215, -0.0000000000000003,  0.2588190451025202,
    0.4999999999999993,  0.7071067811865470,  0.8660254037844384,  0.9659258262890682,
];

#[inline(always)]
fn hour_cos_from_snap(s: &NetSnap) -> f64 {
    unsafe { *COS_HOUR.get_unchecked((s.hour_idx as usize) & 23) }
}

// E58: fast ln(1+tip) in hot path band
#[inline(always)]
fn ln1p_tip_fast(tip: u64) -> f64 {
    let x = tip as f64;
    if (1_000.0..=10_000_000.0).contains(&x) {
        let u = x / (x + 1.0);
        let n0 =  1.0;
        let n1 = -0.4999999979131145;
        let n2 =  0.3333333205741615;
        let d1 = -0.9999999868940157;
        let d2 =  0.6666665972481562;
        let u2 = u * u;
        let num = u * (n0 + u2 * (n1 + u2 * n2));
        let den = 1.0 + u2 * (d1 + u2 * d2);
        num / den
    } else {
        (x + 1.0).ln()
    }
}

// E32: cooperative yield + deadline guard for long loops
#[inline(always)]
async fn coop_or_deadline(deadline: Instant) -> bool {
    if Instant::now() >= deadline { return false; }
    tokio::task::yield_now().await;
    true
}

#[inline(always)]
fn should_sample_metrics(v: &Pubkey, slot: u64) -> bool {
    if METRIC_SAMPLE_LG2 == 0 { return true; }
    let b = v.to_bytes();
    let lo = u64::from_le_bytes([b[0],b[1],b[2],b[3],b[4],b[5],b[6],b[7]]);
    let key = (lo ^ slot).wrapping_mul(0x9E3779B97F4A7C15);
    (key & ((1u64 << METRIC_SAMPLE_LG2) - 1)) == 0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TipAcceptanceMetrics {
    pub validator: Pubkey,
    pub accepted_tips: VecDeque<u64>,
    pub rejected_tips: VecDeque<u64>,
    pub outcomes: VecDeque<bool>, // true=accepted, false=rejected
    pub timestamp_secs: VecDeque<i64>, // epoch seconds
    pub success_rate: f64,
    pub min_accepted_tip: u64,
    pub median_accepted_tip: u64,
    pub percentile_95_tip: u64,
    pub last_update_secs: i64,
    pub bundle_inclusion_times: VecDeque<Duration>,
    pub network_congestion_factor: f64,
    // E26: exponentially-decayed counts for O(1) success-rate update
    pub ew_accept: f64,
    pub ew_total: f64,
    pub last_ew_ts: i64,
}

// PIECE E14: cold-path escalation helper to keep hot path lean
#[cold]
fn escalate_reject_penalty(ds: &mut DegradationState, streak: u32) {
    let target = if streak >= 6 { 0.70 } else if streak >= 3 { 0.82 } else { 0.90 };
    ds.penalty = (ds.penalty.min(target) + target) * 0.5;
    ds.last_update = now_secs();
}

// PIECE E19: adaptive tip quantum aligner to reduce exact ties
#[inline(always)]
fn align_tip_lamports_adaptive(t: u64, mq: &MarketQuantiles) -> u64 {
    let p25 = mq.percentile_25().unwrap_or(t);
    let p75 = mq.percentile_75().unwrap_or(t);
    let iqr = p75.saturating_sub(p25).max(1);
    let q = (iqr / 1024).clamp(8, 128).max(8);
    (t / q) * q
}

// PIECE T8: Per-validator quantile sketch (median, p95, min) using P²
#[derive(Clone)]
struct ValidatorQuantiles {
    q50: P2Quantile,
    q95: P2Quantile,
    min_seen: u64,
    count: usize,
}
impl ValidatorQuantiles {
    fn new() -> Self {
        Self { q50: P2Quantile::new(0.50), q95: P2Quantile::new(0.95), min_seen: u64::MAX, count: 0 }
    }
    #[inline(always)]
    fn insert(&mut self, tip: u64) {
        let x = tip as f64;
        self.q50.insert(x); self.q95.insert(x);
        self.count += 1;
        if tip < self.min_seen { self.min_seen = tip; }
    }
    #[inline(always)] fn median(&self) -> Option<u64> { self.q50.estimate() }
    #[inline(always)] fn p95(&self)    -> Option<u64> { self.q95.estimate() }
}

// PIECE T2: degradation state to throttle misbehaving validators
#[derive(Debug, Clone, Copy)]
struct DegradationState {
    penalty: f64,    // [0.50 .. 1.00]
    last_update: i64,
}
impl Default for DegradationState {
    fn default() -> Self { Self { penalty: 1.0, last_update: now_secs() } }
}

// --- Prometheus metrics ---
static TIP_RECOMMENDATION: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!("tip_recommendation_lamports", "Recommended tip").unwrap()
});
static TIP_RECOMMENDATION_BY_VALIDATOR: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!("tip_recommendation_by_validator", "Recommended tip per validator", &["validator"]).unwrap()
});
static ACCEPT_REJECT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!("tip_outcome_total", "Tip outcomes", &["validator","outcome"]).unwrap()
});
static ACCEPT_PROB: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!("accept_probability", "Predicted acceptance probability").unwrap()
});
static ACCEPT_PROB_BY_VALIDATOR: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!("accept_probability_by_validator", "Predicted acceptance probability per validator", &["validator"]).unwrap()
});
static INCLUSION_MS: Lazy<Histogram> = Lazy::new(|| {
    register_histogram!("bundle_inclusion_ms", "Observed bundle inclusion times (ms)").unwrap()
});
static CONGESTION_GAUGE: Lazy<Gauge> = Lazy::new(|| {
    register_gauge!("network_congestion", "Current network congestion [0,1]").unwrap()
});
static EV_EFFICIENCY: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!("ev_efficiency", "Expected value efficiency (EV/Tip)", &["validator"]).unwrap()
});
static VALIDATOR_COOLDOWN: Lazy<GaugeVec> = Lazy::new(|| {
    register_gauge_vec!("validator_cooldown_seconds", "Cooldown seconds remaining per validator", &["validator"]).unwrap()
});
// PIECE T1: zero-waste label cache
// PIECE E28/E31: switch caches to AHash and Arc<str> labels
type FastDashMap<K, V> = DashMap<K, V, FastHash>;
const MAX_LABEL_CACHE: usize = 4096;
static LABEL_CACHE: Lazy<DashMap<Pubkey, std::sync::Arc<str>, FastHash>> =
    Lazy::new(|| DashMap::with_hasher(FastHash::new()));

#[inline]
fn observe_tip(amt: u64) {
    if METRIC_SAMPLE_LG2 == 0 {
        TIP_RECOMMENDATION.observe(amt as f64);
    } else {
        if (amt & ((1u64 << METRIC_SAMPLE_LG2) - 1)) == 0 { TIP_RECOMMENDATION.observe(amt as f64); }
    }
}
#[inline]
fn observe_outcome(v: &Pubkey, accepted: bool) {
    let slot = 0u64; // optionally thread NetSnap here if needed
    if !should_sample_metrics(v, slot) { return; }
    let v_label = label_for_cached(v);
    ACCEPT_REJECT.with_label_values(&[&*v_label, if accepted { "accept" } else { "reject" }]).inc();
}
#[inline]
fn observe_accept_prob(p: f64) {
    if METRIC_SAMPLE_LG2 == 0 { ACCEPT_PROB.observe(p.clamp(0.0, 1.0)); }
}
#[inline]
fn observe_inclusion(dur: Duration) {
    if METRIC_SAMPLE_LG2 == 0 {
        let ms = dur.as_secs_f64() * 1000.0;
        if ms.is_finite() { INCLUSION_MS.observe(ms); }
    }
}

#[inline(always)]
fn label_for_cached(v: &Pubkey) -> std::sync::Arc<str> {
    if let Some(s) = LABEL_CACHE.get(v) { return std::sync::Arc::clone(s.value()); }
    let s = v.to_string();
    let arc: std::sync::Arc<str> = std::sync::Arc::<str>::from(s);
    if LABEL_CACHE.len() < MAX_LABEL_CACHE {
        LABEL_CACHE.insert(*v, std::sync::Arc::clone(&arc));
    }
    arc
}

#[inline(always)]
fn now_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

// PIECE E9: compute-budget guard
#[derive(Clone, Copy)]
struct ComputeBudget { deadline: Instant }
impl ComputeBudget {
    #[inline(always)] fn new(ms: u64) -> Self { Self { deadline: Instant::now() + Duration::from_millis(ms) } }
    #[inline(always)] fn time_ok(&self) -> bool { Instant::now() < self.deadline }
}

// PIECE S1: SplitMix64 + per-slot salt deterministic jitter
#[inline(always)]
fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}
#[inline(always)]
fn fast_jitter_lamports_salted(v: &Pubkey, slot: u64, salt: u64) -> u64 {
    let b = v.to_bytes();
    let lo = u64::from_le_bytes([
        b[0]^b[8], b[1]^b[9], b[2]^b[10], b[3]^b[11],
        b[4]^b[12], b[5]^b[13], b[6]^b[14], b[7]^b[15],
    ]);
    let r = splitmix64(lo ^ slot ^ salt);
    (r >> 32) & 0x3FF // 0..=1023
}

#[inline]
fn sigmoid(x: f64) -> f64 { 1.0 / (1.0 + (-x).exp()) }

// PIECE E25: fast, saturated sigmoid
#[inline(always)]
fn sigmoid_fast(z: f64) -> f64 {
    if z > 18.0 { 1.0 } else if z < -18.0 { 0.0 } else { 1.0 / (1.0 + (-z).exp()) }
}

#[inline]
fn wilson_lower_bound(successes: usize, trials: usize, z: f64) -> f64 {
    if trials == 0 { return 0.0; }
    let n = trials as f64;
    let phat = successes as f64 / n;
    let denom = 1.0 + z.powi(2) / n;
    let centre = phat + z.powi(2) / (2.0 * n);
    let adj = z * ((phat * (1.0 - phat) + z.powi(2) / (4.0 * n)) / n).sqrt();
    ((centre - adj) / denom).clamp(0.0, 1.0)
}

fn safe_percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() { return BASE_TIP_LAMPORTS; }
    let p = p.clamp(0.0, 1.0);
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn median(sorted: &[u64]) -> u64 {
    if sorted.is_empty() { return BASE_TIP_LAMPORTS; }
    let n = sorted.len();
    if n % 2 == 1 { sorted[n/2] } else { ((sorted[n/2 - 1] as u128 + sorted[n/2] as u128)/2) as u64 }
}

fn winsorize(sorted: &mut [u64], p: f64) {
    if sorted.is_empty() { return; }
    let low = safe_percentile(sorted, p);
    let high = safe_percentile(sorted, 1.0 - p);
    for v in sorted.iter_mut() {
        if *v < low { *v = low; } else if *v > high { *v = high; }
    }
}

impl ValidatorTipAcceptanceModeler {
    // E85: NaN/Inf sanitizer and clamping helper
    #[inline(always)]
    fn sane01(x: f64, lo: f64, hi: f64) -> f64 {
        if !x.is_finite() { return ((lo + hi) * 0.5).clamp(lo, hi); }
        x.clamp(lo, hi)
    }
    // E68 (basic): global temperature helper (can be extended with AdaGrad). Returns neutral 1.0 by default.
    #[inline(always)]
    fn tau_global(&self) -> f64 { 1.0 }

    // E60: EV derivative upper bound for monotonic culls
    #[inline(always)]
    fn ev_slope_upper_bound(&self, profile: &ValidatorProfile, bundle_value: u64, t: u64) -> f64 {
        let bt = profile.logistic_beta_tip.abs();
        (bundle_value as f64) * 0.25_f64 * bt / (1.0 + t as f64) - 1.0
    }

    // E61: Analytic inverse tips for target probabilities (seeds)
    #[inline(always)]
    fn tip_for_target_p(&self, profile: &ValidatorProfile, snap: &NetSnap, p_target: f64) -> Option<u64> {
        let bt = profile.logistic_beta_tip;
        if bt.abs() < 1e-9 { return None; }
        let tau = profile.temp_log.exp().clamp(0.5, 2.0);
        let x_cong = snap.congestion_level.clamp(0.0, 1.0);
        let x_hour = hour_cos_from_snap(snap);
        let z_target = (p_target / (1.0 - p_target)).ln() * tau;
        let rhs = z_target - profile.logistic_beta0
            - profile.logistic_beta_cong * x_cong
            - profile.logistic_beta_hour * x_hour;
        let x_tip = rhs / bt;
        let t = x_tip.exp() - 1.0;
        if t.is_finite() && t > 0.0 { Some(t.round() as u64) } else { None }
    }

    // E62: deadline-aware iteration budget for golden-section
    #[inline(always)]
    fn time_left_ms(deadline: Instant) -> u64 {
        deadline.saturating_duration_since(Instant::now()).as_millis() as u64
    }
    #[inline(always)]
    fn pick_golden_iters(deadline: Instant) -> u32 {
        match Self::time_left_ms(deadline) {
            t if t >= 5 => 17,
            t if t >= 3 => 12,
            t if t >= 2 => 10,
            _           => 8,
        }
    }

    // F1/E72: Analytic argmax seed for per-λ using logistic model + 1-2 Newton steps
    #[inline(always)]
    fn argmax_tip_for_lambda_analytic_seed(
        &self,
        profile: &ValidatorProfile,
        snap: &NetSnap,
        bundle_value: u64,
        lambda: f64,
        lo: u64,
        hi: u64,
    ) -> u64 {
        let bt = profile.logistic_beta_tip;
        if bt.abs() < 1e-12 || lambda <= 0.0 { return lo; }
        let x_cong = snap.congestion_level.clamp(0.0, 1.0);
        let x_hour = hour_cos_from_snap(snap);
        let tau_v  = profile.temp_log.exp().clamp(0.5, 2.0);
        let tau    = (tau_v * self.tau_global()).clamp(0.5, 2.0);
        let b0 = profile.logistic_beta0
            + profile.logistic_beta_cong * x_cong
            + profile.logistic_beta_hour * x_hour;
        let mut t = lo.max(BASE_TIP_LAMPORTS).min(hi) as f64;
        for _ in 0..2 {
            let x_tip = ln1p_tip_fast(t as u64);
            let z  = b0 + bt * x_tip;
            let s  = sigmoid_fast(z / tau);
            let sp = s * (1.0 - s);
            let f  = (bundle_value as f64) * sp * (bt / (tau * (1.0 + t))) - lambda;
            let zt  = bt / (1.0 + t);
            let spp = sp * (1.0 - 2.0 * s) * (zt / tau);
            let df  = (bundle_value as f64) * (spp * (bt / (1.0 + t)) - sp * (bt / (tau * (1.0 + t) * (1.0 + t))));
            if df.abs() < 1e-18 { break; }
            let t_new = (t - f / df).clamp(lo as f64, hi as f64);
            if (t_new - t).abs() <= 1.0 { t = t_new; break; }
            t = t_new;
        }
        t.round() as u64
    }

    // F1/E72: Quick argmax for λ using analytic seed + tiny polish
    #[inline(always)]
    async fn argmax_tip_for_lambda_quick(
        &self,
        v: &Pubkey,
        bundle_value: u64,
        lambda: f64,
        lo: u64,
        hi: u64,
        deadline: Instant,
    ) -> anyhow::Result<u64> {
        let snap = self.network_atomics.read();
        if let Some(pf) = self.validator_profiles.get(v) {
            let seed = self.argmax_tip_for_lambda_analytic_seed(pf.value(), &snap, bundle_value, lambda, lo, hi);
            let neigh = [
                seed.saturating_sub(seed/16 + 500).clamp(lo, hi),
                seed,
                (seed + seed/16 + 500).min(hi),
            ];
            let mut best_t = seed;
            let mut best_obj = f64::NEG_INFINITY;
            for &t in &neigh {
                if Instant::now() >= deadline { break; }
                let p = self.get_p_fast_or_async(v, t).await?;
                let obj = (bundle_value as f64) * p - lambda * (t as f64);
                if obj > best_obj { best_obj = obj; best_t = t; }
            }
            if Self::time_left_ms(deadline) >= 2 {
                let a = best_t.saturating_mul(7)/10;
                let b = ((best_t as f64)*1.3).round() as u64;
                let (t_best, _ev) = self.golden_refine_int(v, bundle_value, a.clamp(lo,hi), b.clamp(lo+1,hi), 6).await?;
                return Ok(t_best);
            }
            return Ok(best_t);
        }
        Ok(lo)
    }

    // F2: desired allocator concurrency accessor
    #[inline(always)]
    fn desired_allocator_concurrency(&self) -> usize { ALLOC_MAX_CONCURRENCY }

    // F4: shared degradation penalty applier
    #[inline(always)]
    fn apply_degradation_penalty(&self, v: &Pubkey, p: f64) -> f64 {
        if let Some(d) = self.degradation.get(v) {
            let age = (now_secs() - d.value().last_update).max(0) as f64;
            let decay = (1.0 - (age / 20.0).min(1.0) * 0.2).max(0.8);
            let pen = (d.value().penalty * decay).clamp(0.5, 1.0);
            return (p * pen).clamp(0.01, 0.99);
        }
        p.clamp(0.01, 0.99)
    }

    // F5/E73: screen validators to top-K by cheap marginal
    pub fn screen_validators_for_bundle(&self, validators: &[Pubkey]) -> Vec<Pubkey> {
        let snap = self.network_atomics.read();
        let mut scored: Vec<(Pubkey, f64)> = Vec::with_capacity(validators.len());
        for v in validators {
            if let Some(pf) = self.validator_profiles.get(v) {
                let pr  = pf.value();
                let lt  = pr.last_recommended_tip.max(BASE_TIP_LAMPORTS);
                let x_t = ln1p_tip_fast(lt);
                let z   = pr.logistic_beta0
                        + pr.logistic_beta_tip  * x_t
                        + pr.logistic_beta_cong * snap.congestion_level.clamp(0.0,1.0)
                        + pr.logistic_beta_hour * hour_cos_from_snap(&snap);
                let tau = (pr.temp_log.exp().clamp(0.5,2.0) * self.tau_global()).clamp(0.5,2.0);
                let s   = sigmoid_fast(z / tau);
                let sp  = s * (1.0 - s);
                let marginal = sp * (pr.logistic_beta_tip.abs() / (tau * (1.0 + lt as f64))) - (1.0 / (lt as f64));
                scored.push((*v, marginal));
            }
        }
        let k = WF_SCREEN_K.min(scored.len());
        if k == 0 { return Vec::new(); }
        scored.select_nth_unstable_by(k-1, |a,b| b.1.partial_cmp(&a.1).unwrap());
        scored.truncate(k);
        scored.sort_unstable_by(|a,b| b.1.partial_cmp(&a.1).unwrap());
        scored.into_iter().map(|(v,_)| v).collect()
    }

    // E63: quick reject of dead validators this slot
    #[inline(always)]
    fn is_no_hope_now(&self, v: &Pubkey, p_lb: f64, deg_penalty: f64) -> bool {
        (p_lb * deg_penalty).clamp(0.0, 1.0) < 0.05
    }
    // E39: lockless fast-path predictor (sync) with async fallback helper
    #[inline(always)]
    fn predict_acceptance_probability_fast(&self, validator: &Pubkey, tip_amount: u64) -> Option<f64> {
        let snap = self.network_atomics.read();
        if let Some(p) = self.pt_slot_cache.get(&(*validator, tip_amount, snap.current_slot)) {
            return Some(*p.value() as f64);
        }
        let vp = self.validator_profiles.get(validator)?;
        let prof = vp.value();

        // Logistic from atomic snapshot
        let mut p = Self::logistic_predict_from_snap(prof, tip_amount, &snap);
        let network_adjustment = 1.0 - (snap.congestion_level - 0.5).abs() * 0.2;
        let profile_adjustment = 1.0;
        p = Self::sane01(p * network_adjustment * profile_adjustment, 0.01, 0.99);

        // Monotonic guard (lock-free shadow)
        p = self.apply_monotone_floor_slot(validator, tip_amount, p);

        // Degradation penalty (cheap read)
        if let Some(d) = self.degradation.get(validator) {
            let age = (now_secs() - d.last_update).max(0) as f64;
            let decay = (1.0 - (age / 20.0).min(1.0) * 0.2).max(0.8);
            let pen = (d.penalty * decay).clamp(0.5, 1.0);
            p = (p * pen).clamp(0.01, 0.99);
        }

        // write slot cache and update shadow
        self.pt_slot_cache.insert((*validator, tip_amount, snap.current_slot), p as f32);
        let ms = self.monotonic_shadow
            .entry(*validator)
            .or_insert_with(|| std::sync::Arc::new(MonoState::new()));
        let msr = ms.value();
        msr.last_tip.store(tip_amount, Ordering::Relaxed);
        msr.last_p_bits.store(p.to_bits(), Ordering::Relaxed);
        Some(p)
    }

    #[inline(always)]
    async fn get_p_fast_or_async(&self, v: &Pubkey, t: u64) -> anyhow::Result<f64> {
        if let Some(p) = self.predict_acceptance_probability_fast(v, t) { return Ok(p); }
        self.predict_acceptance_probability(v, t).await
    }
    #[inline(always)]
    fn apply_tie_breaker_jitter(&self, base: u64, v: &Pubkey, network: &NetworkConditions) -> u64 {
        // S1: use salted SplitMix64 jitter
        let salt = self.jitter_salt();
        let jitter = fast_jitter_lamports_salted(v, network.current_slot, salt);
        let cap = (base / 1000).max(1).min(1023);
        base.saturating_add(jitter.min(cap))
    }
    // S1: per-slot salt derived from atomics snapshot
    #[inline(always)]
    fn jitter_salt(&self) -> u64 {
        let s = self.network_atomics.read();
        s.current_slot.rotate_left(13) ^ (s.average_priority_fee as u64).rotate_right(7)
    }
    // S1: smarter jitter using market IQR cap
    #[inline(always)]
    fn apply_tie_breaker_jitter_smart(
        &self,
        base: u64,
        v: &Pubkey,
        network: &NetworkConditions,
        mq: &MarketQuantiles,
    ) -> u64 {
        let salt = self.jitter_salt();
        let raw = fast_jitter_lamports_salted(v, network.current_slot, salt);
        let p25 = mq.percentile_25().unwrap_or(base);
        let p75 = mq.percentile_75().unwrap_or(base);
        let iqr = p75.saturating_sub(p25).max(1);
        let cap_by_base = (base / 1000).max(1).min(1023);
        let cap_by_iqr  = (iqr / 2048).max(1).min(1023);
        let cap = cap_by_base.min(cap_by_iqr);
        base.saturating_add(raw.min(cap))
    }

    // S2: one-shot seal helper for per-slot tip/p caches and shadow
    #[inline(always)]
    fn seal_tip_slot(&self, validator: &Pubkey, tip: u64, p_cache: f64, slot: u64) {
        self.pt_slot_cache.insert((*validator, tip, slot), p_cache as f32);
        self.tip_slot_cache.insert((*validator, slot), tip);
        let ms = self
            .monotonic_shadow
            .entry(*validator)
            .or_insert_with(|| std::sync::Arc::new(MonoState::new()));
        let msr = ms.value();
        msr.last_tip.store(tip, Ordering::Relaxed);
        msr.last_p_bits.store(p_cache.to_bits(), Ordering::Relaxed);
    }

    // S4: EV Newton warm-start (2 steps) for coarse sweep seeds
    #[inline(always)]
    fn ev_newton_warmstart(
        &self,
        profile: Option<&ValidatorProfile>,
        snap: &NetworkConditions,
        bundle_value: u64,
        lo: u64,
    ) -> u64 {
        let Some(pr) = profile else { return lo.max(BASE_TIP_LAMPORTS) };
        let bt = pr.logistic_beta_tip;
        if bt.abs() < 1e-9 { return lo.max(BASE_TIP_LAMPORTS); }
        let tau = pr.temp_log.exp().clamp(0.5, 2.0);
        let x_cong = snap.congestion_level.clamp(0.0, 1.0);
        let hour = chrono::Utc::now().hour() as f64;
        let x_hour = (hour / 24.0) * 2.0 * std::f64::consts::PI;
        let b0 = pr.logistic_beta0 + pr.logistic_beta_cong * x_cong + pr.logistic_beta_hour * x_hour.cos();
        let mut t = lo.max(BASE_TIP_LAMPORTS) as f64;
        for _ in 0..2 {
            let x_tip = (t).ln_1p();
            let z  = b0 + bt * x_tip;
            let s  = sigmoid_fast(z / tau);
            let sp = s * (1.0 - s);
            let dpdt = sp * (bt / (tau * (1.0 + t)));
            let f = (bundle_value as f64) * dpdt - 1.0;
            let zt  = bt / (1.0 + t);
            let spp = sp * (1.0 - 2.0 * s) * (zt / tau);
            let d2 = (bundle_value as f64) * (spp * (bt / (1.0 + t)) - sp * (bt / (tau * (1.0 + t) * (1.0 + t))));
            if d2.abs() < 1e-18 { break; }
            let t_new = (t - f / d2).max(BASE_TIP_LAMPORTS as f64);
            if (t_new - t).abs() <= 1.0 { t = t_new; break; }
            t = t_new;
        }
        t.round() as u64
    }
    fn leader_phase_multiplier(&self, network: &NetworkConditions, validator: &Pubkey) -> f64 {
        let cur = network.current_slot;
        for s in cur..(cur + 2) {
            if network.slot_leader_schedule.get(&s) == Some(validator) { return 1.10; }
        }
        1.0
    }

    #[inline(always)]
    fn slot_phase_multiplier_from_metrics(
        metrics_opt: Option<&TipAcceptanceMetrics>,
        fallback_slot: u64,
    ) -> f64 {
        if let Some(m) = metrics_opt {
            if !m.bundle_inclusion_times.is_empty() {
                let sum_ms: f64 = m.bundle_inclusion_times.iter().map(|d| d.as_secs_f64() * 1000.0).sum();
                let avg_ms = sum_ms / (m.bundle_inclusion_times.len() as f64);
                return if avg_ms <= 300.0 { 0.95 } else if avg_ms <= 450.0 { 1.00 } else { 1.08 };
            }
        }
        match (fallback_slot & 0b11) {
            0 => 1.02,
            1 => 1.00,
            2 => 1.00,
            _ => 1.06,
        }
    }
    fn update_validator_statistics_inplace(&self, metrics: &mut TipAcceptanceMetrics) {
        // E26/E41/E47: Bayesian posterior from time-decayed counts with cold-start prior
        let mut alpha = 1.0 + metrics.ew_accept.max(0.0);
        let mut beta  = 1.0 + (metrics.ew_total - metrics.ew_accept).max(0.0);
        if metrics.ew_total < 10.0 {
            // E47: small Beta(6,4) prior to stabilize early decisions (~60%)
            alpha += 6.0; beta += 4.0;
        }
        let denom = (alpha + beta).max(f64::MIN_POSITIVE);
        metrics.success_rate = (alpha / denom).clamp(0.0, 1.0);

        // PIECE T8: prefer per-validator quantile sketch if available
        if let Some(q) = self.validator_tip_quants.get(&metrics.validator) {
            let qv = q.value();
            if qv.count >= MIN_SAMPLE_SIZE {
                metrics.min_accepted_tip = metrics.min_accepted_tip.min(qv.min_seen);
                if let Some(med) = qv.median() { metrics.median_accepted_tip = med; }
                if let Some(p95) = qv.p95() { metrics.percentile_95_tip = p95; }
                metrics.last_update_secs = now_secs();
                return;
            }
        }

        if !metrics.accepted_tips.is_empty() {
            let mut v: Vec<u64> = metrics.accepted_tips.iter().copied().collect();
            v.sort_unstable();
            // Robustify against outliers
            winsorize(&mut v, 0.01);
            metrics.min_accepted_tip = v[0];
            metrics.median_accepted_tip = median(&v);
            metrics.percentile_95_tip = safe_percentile(&v, 0.95);
        }
        metrics.last_update_secs = now_secs();
    }
}

#[derive(Debug, Clone)]
pub struct ValidatorProfile {
    pub pubkey: Pubkey,
    pub historical_acceptance_rate: f64,
    pub tip_sensitivity: f64,
    pub peak_hours: Vec<u8>,
    pub reliability_score: f64,
    pub recent_performance: VecDeque<f64>,
    // Online logistic calibration parameters: sigmoid(beta0 + b_tip*log1p(tip) + b_cong*cong + b_hour*hour)
    pub logistic_beta0: f64,
    pub logistic_beta_tip: f64,
    pub logistic_beta_cong: f64,
    pub logistic_beta_hour: f64,
    // PIECE T9: AdaGrad accumulators
    pub g2_beta0: f64,
    pub g2_beta_tip: f64,
    pub g2_beta_cong: f64,
    pub g2_beta_hour: f64,
    // PIECE E16: drift detection
    pub logloss_ewma: f64,
    pub cusum: f64,
    // PIECE E34: temperature scaling
    pub temp_log: f64,
    pub g2_temp: f64,
    // PIECE E4: ROI governor EWMA trackers
    pub spent_ewma: f64,
    pub ev_ewma: f64,
    // Online tip dynamics
    pub tip_elasticity_coef: f64,
    pub streak_rejects: u32,
    pub last_recommended_tip: u64,
    pub last_outcome: Option<bool>,
    pub last_tip_submitted: Option<u64>,
    pub cooldown_until_secs: i64,
    pub last_ev_lost: f64,
    pub last_bundle_value: u64,
    pub last_p_accept: f64,
    pub logistic_updates: u64,
}

#[derive(Debug, Clone)]
pub struct NetworkConditions {
    pub current_tps: f64,
    pub average_priority_fee: u64,
    pub slot_leader_schedule: HashMap<u64, Pubkey>,
    pub recent_block_rewards: VecDeque<u64>,
    pub congestion_level: f64,
    pub current_slot: u64,
}

pub struct ValidatorTipAcceptanceModeler {
    validator_metrics: Arc<FastDashMap<Pubkey, TipAcceptanceMetrics>>,
    validator_profiles: Arc<FastDashMap<Pubkey, ValidatorProfile>>,
    network_conditions: Arc<AsyncRwLock<NetworkConditions>>,
    degradation: Arc<FastDashMap<Pubkey, DegradationState>>,
    // PIECE T8: per-validator quantile sketches
    validator_tip_quants: Arc<FastDashMap<Pubkey, ValidatorQuantiles>>,
    // PIECE E13: per-slot rec cache
    tip_slot_cache: Arc<FastDashMap<(Pubkey, u64), u64>>,
    // PIECE E20: slot-scoped p(t) memo
    pt_slot_cache: Arc<FastDashMap<(Pubkey, u64, u64), f32>>,
    // PIECE E21: atomics snapshot for hot reads
    network_atomics: Arc<NetworkAtomics>,
    // E36: lock-free monotonic guard state per validator
    monotonic_shadow: Arc<FastDashMap<Pubkey, std::sync::Arc<MonoState>>>,
    // E37: allocator EWMA latency (us)
    alloc_ewma_us: AtomicU64,
    // E51: lock-free market quantiles via ArcSwap RCU
    market_quants_rcu: Arc<ArcSwap<Arc<MarketQuantiles>>>,
    // E52: CAS slot epoch for one-shot clears
    slot_epoch: AtomicU64,
    // E65: latency guard switch until slot
    latency_guard_until_slot: AtomicU64,
    model_parameters: ModelParameters,
}

#[derive(Debug, Clone)]
struct ModelParameters {
    base_multiplier: f64,
    congestion_multiplier: f64,
    validator_reputation_weight: f64,
    time_decay_weight: f64,
    competitive_factor: f64,
}

impl Default for ModelParameters {
    fn default() -> Self {
        Self {
            base_multiplier: 1.15,
            congestion_multiplier: 1.25,
            validator_reputation_weight: 0.3,
            time_decay_weight: 0.2,
            competitive_factor: 1.1,
        }
    }
}

impl ValidatorTipAcceptanceModeler {
    pub fn new() -> Self {
        Self {
            validator_metrics: Arc::new(FastDashMap::with_capacity_and_hasher(EXPECTED_VALIDATORS, FastHash::new())),
            validator_profiles: Arc::new(FastDashMap::with_capacity_and_hasher(EXPECTED_VALIDATORS, FastHash::new())),
            network_conditions: Arc::new(AsyncRwLock::new(NetworkConditions {
                current_tps: 2000.0,
                average_priority_fee: 1_000,
                slot_leader_schedule: HashMap::new(),
                recent_block_rewards: VecDeque::with_capacity(100),
                congestion_level: 0.5,
                current_slot: 0,
            })),
            degradation: Arc::new(FastDashMap::with_capacity_and_hasher(EXPECTED_VALIDATORS, FastHash::new())),
            validator_tip_quants: Arc::new(FastDashMap::with_capacity_and_hasher(EXPECTED_VALIDATORS, FastHash::new())),
            tip_slot_cache: Arc::new(FastDashMap::with_capacity_and_hasher(EXPECTED_VALIDATORS * 4, FastHash::new())),
            pt_slot_cache: Arc::new(FastDashMap::with_capacity_and_hasher(EXPECTED_VALIDATORS * 8, FastHash::new())),
            network_atomics: Arc::new(NetworkAtomics::new()),
            monotonic_shadow: Arc::new(FastDashMap::with_capacity_and_hasher(EXPECTED_VALIDATORS, FastHash::new())),
            alloc_ewma_us: AtomicU64::new(1500),
            market_quants_rcu: Arc::new(ArcSwap::from(Arc::new(MarketQuantiles::new()))),
            slot_epoch: AtomicU64::new(0),
            latency_guard_until_slot: AtomicU64::new(0),
            model_parameters: ModelParameters::default(),
        }
    }

    pub async fn calculate_optimal_tip(
        &self,
        validator: &Pubkey,
        bundle_value: u64,
        urgency_factor: f64,
    ) -> Result<u64> {
        // E52: one-shot per-slot clear if needed
        let snap0 = self.network_atomics.read();
        self.on_new_slot_if_needed(snap0.current_slot);
        // E65: latency guard – fail-open to baseline for one slot
        let guard = self.latency_guard_until_slot.load(Ordering::Relaxed);
        if snap0.current_slot <= guard {
            let mut final_tip = BASE_TIP_LAMPORTS;
            if let Some(p) = self.validator_profiles.get(validator) {
                final_tip = final_tip.max(p.value().last_recommended_tip);
            }
            // apply jitter/alignment and seal
            let network = self.network_conditions.read().await.clone();
            let mq = self.market_quants_rcu.load();
            final_tip = self.apply_tie_breaker_jitter_smart(final_tip, validator, &network, &mq);
            final_tip = align_tip_lamports_adaptive(final_tip, &mq);
            // S2: seal caches and monotone shadow
            {
                let p_cache = self.predict_acceptance_probability_fast(validator, final_tip).unwrap_or(0.5);
                self.seal_tip_slot(validator, final_tip, p_cache, snap0.current_slot);
            }
            return Ok(final_tip);
        }
        // Fetch current views
        let metrics = &self.validator_metrics;
        let profiles = &self.validator_profiles;
        let network = self.network_conditions.read().await.clone();

        // Per-slot memoization handled by E52 on_new_slot_if_needed(); optional read-through cache below
        if let Some(cached) = self.tip_slot_cache.get(&(*validator, network.current_slot)) {
            return Ok(*cached.value());
        }

        let validator_metrics = metrics.get(validator).map(|r| r.value().clone());
        let validator_profile = profiles.get(validator).map(|r| r.value().clone());

        // Early cooldown guard: if validator is cooling down, avoid spending budget
        if let Some(profile) = validator_profile.as_ref() {
            if profile.cooldown_until_secs > now_secs() {
                let conservative = BASE_TIP_LAMPORTS;
                let v_label = label_for_cached(validator);
                TIP_RECOMMENDATION_BY_VALIDATOR.with_label_values(&[&v_label]).observe(conservative as f64);
                EV_EFFICIENCY.with_label_values(&[&v_label]).set(0.0);
                return Ok(conservative);
            }
        }

        // --- S13: early no-hope bailout ---
        if let Some(m) = validator_metrics.as_ref() {
            let z = Self::wilson_z_for_n(m.ew_total.max(1.0));
            let p_lb = wilson_lower_bound_from_ew(m.ew_accept, m.ew_total, z);
            let deg  = self.current_deg_penalty(validator);
            if self.is_no_hope_now(validator, p_lb, deg) {
                let mq = self.market_quants_rcu.load();
                let mut t = BASE_TIP_LAMPORTS.max(mq.percentile_25().unwrap_or(BASE_TIP_LAMPORTS));
                t = self.apply_tie_breaker_jitter_smart(t, validator, &network, &mq);
                t = align_tip_lamports_adaptive(t, &mq);
                let snap_slot = self.network_atomics.read().current_slot;
                let p_cache = self.predict_acceptance_probability_fast(validator, t).unwrap_or(0.25);
                self.seal_tip_slot(validator, t, p_cache, snap_slot);
                return Ok(t);
            }
        }
        // --- end S13 ---

        // 1) Base tip
        let base_tip = self.calculate_base_tip(
            validator_metrics.as_ref(),
            validator_profile.as_ref(),
            &network,
            bundle_value,
        );

        // 2) Dynamic adjustments (congestion, UTC hour/leader-phase, urgency)
        let dynamic_tip = self.apply_dynamic_adjustments(
            base_tip,
            urgency_factor,
            &network,
            validator_profile.as_ref(),
        );

        // 3) Market-aware competition: at least market p75
        let competitive_tip = {
            let mq = self.market_quants_rcu.load();
            // Volatility-coupled percentile: blend between p50 and p95 depending on vol ratio
            let p50 = mq.percentile_50().unwrap_or(BASE_TIP_LAMPORTS);
            let p75 = mq.percentile_75().unwrap_or(BASE_TIP_LAMPORTS);
            let p95 = mq.percentile_95().unwrap_or(p75);
            let vol_ratio = if mq.mean() > 0.0 { (mq.std_dev() / mq.mean()).clamp(0.0, 1.0) } else { 0.0 };
            // Blend: low vol→closer to p50/p75; high vol→closer to p95
            let target = if vol_ratio < 0.5 {
                let t = vol_ratio * 2.0; // [0,1]
                (p50 as f64 * (1.0 - t) + p75 as f64 * t) as u64
            } else {
                let t = (vol_ratio - 0.5) * 2.0; // [0,1]
                (p75 as f64 * (1.0 - t) + p95 as f64 * t) as u64
            };
            dynamic_tip.max(target)
        };

        // 4) Per-validator competition overlay (async, non-blocking)
        let competitive_tip = self.apply_competitive_analysis_async(competitive_tip, validator_metrics.as_ref()).await;

        // 5) Coarse-to-fine EV optimization around competitive_tip
        let mut candidate = self.optimize_tip_ev(validator, bundle_value, competitive_tip / 2, competitive_tip.saturating_mul(2).min(MAX_TIP_LAMPORTS)).await?;
        let mut p_accept = self.predict_acceptance_probability(validator, candidate).await?;
        // S17: enforce monotone floor envelope this slot
        p_accept = self.apply_monotone_floor_slot(validator, candidate, p_accept);

        // E29: Bayesian EV guardrail using Wilson lower bound on success
        if let Some(mref) = self.validator_metrics.get(validator) {
            let m = mref.value();
            let p_lb = wilson_lower_bound_from_ew(m.ew_accept, m.ew_total, 1.2815515655446004);
            let ev_lb = (bundle_value as f64) * p_lb - (candidate as f64);
            let tol = (bundle_value as f64) * 0.0025; // 0.25%
            if ev_lb < -tol {
                candidate = BASE_TIP_LAMPORTS;
            }
        }

        // 6) EV guardrails and finalize
        let mut final_tip = self.apply_safety_bounds_curvature(validator, candidate, bundle_value, p_accept);

        // PIECE E4: ROI governor clamp
        if let Some(mut pe) = self.validator_profiles.get_mut(validator) {
            let pr = pe.value_mut();
            let roi = if pr.spent_ewma > 0.0 { pr.ev_ewma / pr.spent_ewma } else { 10.0 };
            let roi_mult = if roi < 1.0 { 0.85 } else if roi < 1.5 { 0.93 } else { 1.0 };
            final_tip = ((final_tip as f64) * roi_mult).round() as u64;
        }

        // E40: tip ramp limiter + hysteresis (apply before jitter/alignment)
        if let Some(mut pe) = self.validator_profiles.get_mut(validator) {
            let pr = pe.value_mut();
            let last = pr.last_recommended_tip.max(BASE_TIP_LAMPORTS);
            let up_cap = ((last as f64) * TIP_UP_MAX_MULT).round() as u64;
            let dn_cap = ((last as f64) * TIP_DN_MAX_MULT).round() as u64;
            let deadband = (last / 64).max(TIP_MIN_STEP);
            let bounded = if final_tip > last.saturating_add(deadband) {
                final_tip.min(up_cap)
            } else if final_tip + deadband < last {
                final_tip.max(dn_cap)
            } else {
                last
            };
            final_tip = bounded;
            pr.last_recommended_tip = final_tip;
        }

        // S18: fuse small moves before jitter/alignment
        final_tip = self.fuse_small_moves(validator, final_tip);

        // PIECE E18/E19: smart jitter and adaptive alignment using market quantiles
        let mq = self.market_quants_rcu.load();
        final_tip = self.apply_tie_breaker_jitter_smart(final_tip, validator, &network, &mq);
        final_tip = align_tip_lamports_adaptive(final_tip, &mq);

        // Persist and observe
        if let Some(mut pe) = self.validator_profiles.get_mut(validator) {
            let pr = pe.value_mut();
            pr.last_recommended_tip = final_tip;
            pr.last_bundle_value = bundle_value;
            pr.last_p_accept = p_accept;
        }
        observe_tip(final_tip);
        let v_label = label_for_cached(validator);
        ACCEPT_PROB_BY_VALIDATOR.with_label_values(&[&*v_label]).observe(p_accept);
        TIP_RECOMMENDATION_BY_VALIDATOR.with_label_values(&[&*v_label]).observe(final_tip as f64);
        let ev = (bundle_value as f64) * p_accept;
        let eff = if final_tip > 0 { ev / (final_tip as f64) } else { 0.0 };
        if eff.is_finite() { EV_EFFICIENCY.with_label_values(&[&*v_label]).set(eff); }
        // S2: one-shot seal per slot
        {
            let snap2 = self.network_atomics.read();
            let p_cache = self.predict_acceptance_probability_fast(validator, final_tip).unwrap_or(p_accept);
            self.seal_tip_slot(validator, final_tip, p_cache, snap2.current_slot);
        }
        Ok(final_tip)
    }

    // E45: integer golden refine in lamports
    #[inline(always)]
    async fn golden_refine_int(
        &self,
        v: &Pubkey,
        bundle_value: u64,
        mut a: u64,
        mut b: u64,
        max_iters: u32,
    ) -> anyhow::Result<(u64, f64)> {
        let phi_num: u128 = 618033988; // scaled 1e9
        let phi_den: u128 = 1000000000u128;
        let mut cache: HashMap<u64, f64> = HashMap::new();
        let mut eval = |t: u64| async {
            if let Some(&p) = cache.get(&t) { return Ok::<f64, anyhow::Error>(p); }
            let p = self.get_p_fast_or_async(v, t).await?;
            cache.insert(t, p);
            Ok::<f64, anyhow::Error>(p)
        };
        let mut left = a;
        let mut right = b;
        let mut c = left + (((right - left) as u128 * (phi_den - phi_num)) / phi_den) as u64;
        let mut d = left + (((right - left) as u128 *  phi_num) / phi_den) as u64;
        if c == d { d = d.saturating_add(1).min(right); }
        let mut fc = { let p = eval(c).await?; (bundle_value as f64) * p - (c as f64) };
        let mut fd = { let p = eval(d).await?; (bundle_value as f64) * p - (d as f64) };
        let mut it = 0;
        while right.saturating_sub(left) > 1 && it < max_iters {
            if fc > fd {
                right = d; d = c; fd = fc;
                c = left + (((right - left) as u128 * (phi_den - phi_num)) / phi_den) as u64;
                if c == d { c = c.saturating_sub(1).max(left); }
                fc = { let p = eval(c).await?; (bundle_value as f64) * p - (c as f64) };
            } else {
                left = c; c = d; fc = fd;
                d = left + (((right - left) as u128 *  phi_num) / phi_den) as u64;
                if c == d { d = d.saturating_add(1).min(right); }
                fd = { let p = eval(d).await?; (bundle_value as f64) * p - (d as f64) };
            }
            it += 1;
        }
        let (t_best, ev_best) = if fc >= fd { (c, fc) } else { (d, fd) };
        Ok((t_best, ev_best))
    }

    // PIECE P3: curvature-aware bounded EV optimizer (coarse parallel + guided bracket + safety finalize)
    pub async fn optimize_tip_ev(
        &self,
        validator: &Pubkey,
        bundle_value: u64,
        lo: u64,
        hi: u64,
    ) -> Result<u64> {
        let lo = lo.max(BASE_TIP_LAMPORTS);
        let hi = hi.max(lo + 1).min(MAX_TIP_LAMPORTS);
        let budget = ComputeBudget::new(4);

        // per-call memoization of p(t)
        let mut p_cache: HashMap<u64, f64> = HashMap::new();
        let mut get_p = |tip: u64| async {
            if let Some(&p) = p_cache.get(&tip) { return Ok::<f64, anyhow::Error>(p); }
            let p = self.get_p_fast_or_async(validator, tip).await?;
            p_cache.insert(tip, p);
            Ok::<f64, anyhow::Error>(p)
        };

        // Analytic warm-start
        let prof_opt = self.validator_profiles.get(validator).map(|e| e.value().clone());
        let net_now = self.network_conditions.read().await.clone();
        let warm = self.ev_newton_warmstart(prof_opt.as_ref(), &net_now, bundle_value, lo).clamp(lo, hi);

        // 8-point coarse grid
        let geo = |r: f64| -> u64 { (lo as f64 * (1.0 - r) + hi as f64 * r * r).round() as u64 };
        let cands: [u64; 8] = [warm, lo, hi, geo(0.15), geo(0.33), geo(0.50), geo(0.67), geo(0.85)];

        // Parallel coarse sweep with tiny deadline
        let deadline = Instant::now() + Duration::from_millis(3);
        let (mut best_t, mut best_ev) = if cands.len() >= 4 && deadline > Instant::now() {
            self.best_candidate_parallel(validator, bundle_value, &cands, deadline).await?
        } else {
            let mut bt = cands[0];
            let mut bev = { let p = get_p(bt).await?; (bundle_value as f64) * p - (bt as f64) };
            for &t0 in &cands[1..] {
                if !budget.time_ok() { break; }
                let t = t0.clamp(lo, hi);
                if t == bt { continue; }
                let p = get_p(t).await?;
                let ev = (bundle_value as f64) * p - (t as f64);
                if ev > bev { bev = ev; bt = t; }
            }
            (bt, bev)
        };

        // Derivative-guided bracketing
        let mut a = best_t.saturating_mul(7).saturating_div(10);
        let mut b = (best_t as f64 * 1.35).round() as u64;
        if let Some(pf) = self.validator_profiles.get(validator) {
            let pr = pf.value();
            let snap = self.network_atomics.read();
            let x_tip = ln1p_tip_fast(best_t);
            let z = pr.logistic_beta0
                + pr.logistic_beta_tip  * x_tip
                + pr.logistic_beta_cong * snap.congestion_level.clamp(0.0, 1.0)
                + pr.logistic_beta_hour * hour_cos_from_snap(&snap);
            let tau = pr.temp_log.exp().clamp(0.5, 2.0);
            let s = sigmoid_fast(z / tau);
            let dpdt = s * (1.0 - s) * (pr.logistic_beta_tip / (tau * (1.0 + best_t as f64)));
            if dpdt > 0.0 {
                a = best_t.saturating_mul(9).saturating_div(10);
                b = ((best_t as f64) * 1.45).round() as u64;
            } else {
                a = (best_t as f64 * 0.55).round() as u64;
                b = ((best_t as f64) * 1.15).round() as u64;
            }
        }
        a = a.clamp(lo, hi);
        b = b.clamp(a + 1, hi);

        // Stage 2: integer golden refine
        let (t_best, ev_best) = self.golden_refine_int(validator, bundle_value, a, b, 17).await?;

        // Finalize with monotone floor + curvature-aware safety
        let mut p_final = get_p(t_best).await?;
        p_final = self.apply_monotone_floor_slot(validator, t_best, p_final);
        let tip_final = self.apply_safety_bounds_curvature(validator, t_best, bundle_value, p_final);

        // Baseline EV cross-check
        let p_base = self.predict_acceptance_probability(validator, BASE_TIP_LAMPORTS).await?;
        let ev_base = (bundle_value as f64) * p_base - (BASE_TIP_LAMPORTS as f64);
        if ev_best < ev_base { return Ok(BASE_TIP_LAMPORTS.max(tip_final.min(hi))); }
        Ok(tip_final.min(hi))
    }

    // PIECE P4: market-aware, Wilson-aware base tip
    fn calculate_base_tip(
        &self,
        metrics: Option<&TipAcceptanceMetrics>,
        profile: Option<&ValidatorProfile>,
        network: &NetworkConditions,
        bundle_value: u64,
    ) -> u64 {
        let mq = self.market_quants_rcu.load();
        let m_p50 = mq.percentile_50().unwrap_or(BASE_TIP_LAMPORTS);
        let m_p75 = mq.percentile_75().unwrap_or(m_p50);
        let m_p95 = mq.percentile_95().unwrap_or(m_p75);
        let vol = if mq.mean() > 0.0 { (mq.std_dev() / mq.mean()).clamp(0.0, 1.0) } else { 0.0 };
        let market_target = if vol < 0.4 {
            ((m_p50 as f64) * (1.0 - 2.5*vol) + (m_p75 as f64) * (2.5*vol)) as u64
        } else {
            let t = ((vol - 0.4) / 0.6).clamp(0.0, 1.0);
            ((m_p75 as f64) * (1.0 - t) + (m_p95 as f64) * t) as u64
        };

        let (v_anchor, p_lcb) = if let Some(m) = metrics {
            let n = m.ew_total.max(1.0);
            let z = if n >= 200.0 { 1.96 } else if n >= 50.0 { 1.645 } else { 1.2815515655446004 };
            let lb = wilson_lower_bound_from_ew(m.ew_accept, m.ew_total, z);
            let base = if m.accepted_tips.len() >= MIN_SAMPLE_SIZE { m.percentile_95_tip } else { m.median_accepted_tip.max(BASE_TIP_LAMPORTS) };
            (base, lb)
        } else {
            (BASE_TIP_LAMPORTS, 0.6)
        };

        if let Some(p) = profile {
            if p.cooldown_until_secs > now_secs() {
                return BASE_TIP_LAMPORTS.max(m_p50);
            }
        }

        let network_base = (network.average_priority_fee as f64 * network.congestion_level * self.model_parameters.congestion_multiplier) as u64;
        let value_based_tip = (bundle_value as f64 * 0.001) as u64;
        let succ_mult = if p_lcb < 0.55 { 1.10 } else if p_lcb < 0.65 { 1.05 } else if p_lcb > 0.85 { 0.95 } else { 1.0 };
        let raw = (v_anchor.max(market_target).max(network_base).max(value_based_tip)) as f64
            * self.model_parameters.base_multiplier * succ_mult;
        let rep_mult = if let Some(p) = profile { 1.0 + (p.reliability_score * self.model_parameters.validator_reputation_weight) } else { 1.0 };
        (raw * rep_mult) as u64
    }

    // PIECE P5: hour/phase polish + urgency
    fn apply_dynamic_adjustments(
        &self,
        base_tip: u64,
        urgency_factor: f64,
        network: &NetworkConditions,
        profile: Option<&ValidatorProfile>,
    ) -> u64 {
        let congestion_adjustment = 1.0 + (network.congestion_level - 0.5) * 0.5;
        let snap = self.network_atomics.read();
        let hour_mult = 1.0 + 0.10 * hour_cos_from_snap(&snap);
        let peak_mult = profile
            .map(|p| {
                let h = chrono::Utc::now().hour() as u8;
                if p.peak_hours.binary_search(&h).is_ok() { 1.12 } else { 1.0 }
            })
            .unwrap_or(1.0);
        let leader_mult = profile.map(|p| self.leader_phase_multiplier(network, &p.pubkey)).unwrap_or(1.0);
        let slot_phase_mult = {
            if let Some(p) = profile {
                let m = self.validator_metrics.get(&p.pubkey);
                Self::slot_phase_multiplier_from_metrics(m.as_deref(), network.current_slot)
            } else {
                Self::slot_phase_multiplier_from_metrics(None, network.current_slot)
            }
        };
        let urgency_multiplier = 1.0 + (urgency_factor - 1.0).clamp(0.0, 2.0) * 0.3;
        ((base_tip as f64) * congestion_adjustment * hour_mult * peak_mult * leader_mult * slot_phase_mult * urgency_multiplier) as u64
    }

    // PIECE P6: volatility/Wilson-aware competition adjustment
    async fn apply_competitive_analysis_async(
        &self,
        tip: u64,
        metrics: Option<&TipAcceptanceMetrics>,
    ) -> u64 {
        let mq = self.market_quants_rcu.load();
        if mq.count < MIN_SAMPLE_SIZE {
            return (tip as f64 * self.model_parameters.competitive_factor) as u64;
        }
        let p01 = mq.percentile_01().unwrap_or(tip);
        let p75 = mq.percentile_75().unwrap_or(tip);
        let p90 = mq.percentile_95().unwrap_or(p75);
        let vol = if mq.mean() > 0.0 { (mq.std_dev() / mq.mean()).clamp(0.0, 1.0) } else { 0.0 };
        let target = if vol < 0.4 { p75 } else { let t = ((vol - 0.4) / 0.6).clamp(0.0, 1.0); ((p75 as f64) * (1.0 - t) + (p90 as f64) * t) as u64 }.max(p01);
        let mut comp = tip.max(target);
        if let Some(m) = metrics {
            let z = if m.ew_total >= 200.0 { 1.96 } else if m.ew_total >= 50.0 { 1.645 } else { 1.2815515655446004 };
            let p_lb = wilson_lower_bound_from_ew(m.ew_accept, m.ew_total, z);
            if p_lb < 0.65 { comp = ((comp as f64) * 1.08) as u64; }
            if m.success_rate < 0.7 { comp = ((comp as f64) * 1.05) as u64; }
        } else {
            comp = ((comp as f64) * self.model_parameters.competitive_factor) as u64;
        }
        comp
    }

    fn apply_safety_bounds(&self, tip: u64, bundle_value: u64, p_accept: f64) -> u64 {
        // EV guardrail: cap tip at 25% of EV = value * p_accept
        let max_tip_ev = (bundle_value as f64 * 0.25 * p_accept.clamp(0.0, 1.0)).floor() as u64;
        let max_reasonable_tip = (bundle_value as f64 * 0.5) as u64;
        tip.min(max_tip_ev).min(max_reasonable_tip).min(MAX_TIP_LAMPORTS).max(BASE_TIP_LAMPORTS)
    }

    // PIECE P7: focal learning + degradation + cooldown tail
    pub async fn record_tip_result(
        &self,
        validator: &Pubkey,
        tip_amount: u64,
        accepted: bool,
        inclusion_time: Option<Duration>,
    ) -> Result<()> {
        {
            let mut entry = self.validator_metrics.entry(*validator).or_insert_with(|| TipAcceptanceMetrics {
                validator: *validator,
                accepted_tips: VecDeque::with_capacity(SLIDING_WINDOW_SIZE),
                rejected_tips: VecDeque::with_capacity(SLIDING_WINDOW_SIZE),
                outcomes: VecDeque::with_capacity(SLIDING_WINDOW_SIZE),
                timestamp_secs: VecDeque::with_capacity(SLIDING_WINDOW_SIZE),
                success_rate: 0.0,
                min_accepted_tip: u64::MAX,
                median_accepted_tip: BASE_TIP_LAMPORTS,
                percentile_95_tip: BASE_TIP_LAMPORTS,
                last_update_secs: now_secs(),
                bundle_inclusion_times: VecDeque::with_capacity(100),
                network_congestion_factor: 0.5,
                ew_accept: 0.0,
                ew_total: 0.0,
                last_ew_ts: now_secs(),
            });

            {
                let m = entry.value_mut();
                if accepted {
                    m.accepted_tips.push_back(tip_amount);
                    if m.accepted_tips.len() > SLIDING_WINDOW_SIZE { m.accepted_tips.pop_front(); }
                    if let Some(t) = inclusion_time {
                        m.bundle_inclusion_times.push_back(t);
                        if m.bundle_inclusion_times.len() > 100 { m.bundle_inclusion_times.pop_front(); }
                    }
                } else {
                    m.rejected_tips.push_back(tip_amount);
                    if m.rejected_tips.len() > SLIDING_WINDOW_SIZE { m.rejected_tips.pop_front(); }
                }
                m.outcomes.push_back(accepted);
                if m.outcomes.len() > SLIDING_WINDOW_SIZE { m.outcomes.pop_front(); }
                m.timestamp_secs.push_back(now_secs());
                if m.timestamp_secs.len() > SLIDING_WINDOW_SIZE { m.timestamp_secs.pop_front(); }
                let now = now_secs();
                let dt = (now - m.last_ew_ts).max(0) as f64;
                let a = ew_decay(dt);
                let y = if accepted { 1.0 } else { 0.0 };
                m.ew_accept = m.ew_accept * a + y;
                m.ew_total  = m.ew_total  * a + 1.0;
                m.last_ew_ts = now;
                self.update_validator_statistics_inplace(m);
            }
        }

        // update global market distribution (accepts only)
        self.update_global_distribution(tip_amount, accepted).await?;

        // Online logistic calibration + governance
        let network = self.network_conditions.read().await;
        if let Some(mut entry) = self.validator_profiles.get_mut(validator) {
            let p = entry.value_mut();

            // Focal logistic + drift guard
            Self::logistic_update_focal(p, tip_amount, accepted, &network);
            Self::apply_drift_guard(p);

            // Streaks + per-validator quantiles
            if accepted {
                p.streak_rejects = 0;
                let mut q = self.validator_tip_quants
                    .entry(*validator)
                    .or_insert_with(|| ValidatorQuantiles::new());
                q.value_mut().insert(tip_amount);
            } else {
                p.streak_rejects = p.streak_rejects.saturating_add(1);
            }

            // ROI governor EWMAs
            let realized_ev = if accepted { p.last_bundle_value as f64 } else { 0.0 };
            p.spent_ewma = 0.9 * p.spent_ewma + 0.1 * (tip_amount as f64);
            p.ev_ewma    = 0.9 * p.ev_ewma    + 0.1 * realized_ev;

            // Degradation penalty update
            if !accepted {
                let entry_d = self.degradation.entry(*validator).or_default();
                let mut ds = *entry_d.value();
                escalate_reject_penalty(&mut ds, p.streak_rejects);
                self.degradation.insert(*validator, ds);
            } else {
                let entry_d = self.degradation.entry(*validator).or_default();
                let mut ds = *entry_d.value();
                ds.penalty = (ds.penalty * 1.02).min(1.0);
                ds.last_update = now_secs();
                self.degradation.insert(*validator, ds);
            }

            // Risk-aware cooldown
            if !accepted && p.streak_rejects >= 3 {
                let cd = self.compute_cooldown_secs(p, tip_amount, accepted, network.congestion_level);
                p.cooldown_until_secs = now_secs() + cd;
                VALIDATOR_COOLDOWN.with_label_values(&[&*label_for_cached(validator)]).set(cd as f64);
            }

            p.last_outcome = Some(accepted);
            p.last_tip_submitted = Some(tip_amount);
        }

        observe_outcome(validator, accepted);
        if let Some(d) = inclusion_time { observe_inclusion(d); }
        Ok(())
    }

    fn update_validator_statistics(metrics: &mut TipAcceptanceMetrics) {
        let total_attempts = metrics.accepted_tips.len() + metrics.rejected_tips.len();
        if total_attempts > 0 {
            metrics.success_rate = metrics.accepted_tips.len() as f64 / total_attempts as f64;
        }

        if !metrics.accepted_tips.is_empty() {
            let mut accepted_vec: Vec<u64> = metrics.accepted_tips.iter().cloned().collect();
            accepted_vec.sort_unstable();
            
            metrics.min_accepted_tip = *accepted_vec.first().unwrap_or(&BASE_TIP_LAMPORTS);
            let mid = accepted_vec.len() / 2;
            metrics.median_accepted_tip = accepted_vec.get(mid).copied().unwrap_or(BASE_TIP_LAMPORTS);
            let p95_idx = ((accepted_vec.len() as f64 * 0.95).floor() as usize).min(accepted_vec.len().saturating_sub(1));
            metrics.percentile_95_tip = accepted_vec.get(p95_idx).copied().unwrap_or(metrics.median_accepted_tip);
        }
        metrics.last_update_secs = now_secs();
    }

    async fn update_global_distribution(&self, tip_amount: u64, accepted: bool) -> Result<()> {
        if !accepted { return Ok(()); }
        // E51: clone-modify-swap RCU writer
        let curr = self.market_quants_rcu.load_full();
        let mut next = (*curr).clone();
        next.robust_insert(tip_amount);
        self.market_quants_rcu.store(Arc::new(next));
        Ok(())
    }

    pub async fn update_network_conditions(
        &self,
        tps: f64,
        avg_priority_fee: u64,
        congestion_level: f64,
    ) -> Result<()> {
        let mut network = self.network_conditions.write().await;
        network.current_tps = tps;
        network.average_priority_fee = avg_priority_fee;
        // PIECE T11: EWMA smoothing to avoid flapping
        let new_c = congestion_level.clamp(0.0, 1.0);
        let smoothed = (1.0 - CONGESTION_ALPHA) * network.congestion_level + CONGESTION_ALPHA * new_c;
        network.congestion_level = smoothed.clamp(0.0, 1.0);
        CONGESTION_GAUGE.set(network.congestion_level);
        // PIECE E21: update atomics snapshot for hot reads
        self.network_atomics.write_from(&network);
        
        Ok(())
    }

    pub async fn get_validator_recommendation(
        &self,
        validator: &Pubkey,
    ) -> Result<TipRecommendation> {
        let metrics = &self.validator_metrics;
        let profiles = &self.validator_profiles;
        let network = self.network_conditions.read().await;
        
        let confidence = self.calculate_confidence(
            metrics.get(validator).map(|r| r.value()),
            profiles.get(validator).map(|r| r.value()),
        );
        
        let optimal_tip = self.calculate_optimal_tip(
            validator,
            100_000_000,
            1.0,
        ).await?;
        
        Ok(TipRecommendation {
            validator: *validator,
            recommended_tip: optimal_tip,
            confidence_score: confidence,
            expected_success_rate: metrics.get(validator)
                .map(|r| r.value().success_rate)
                .unwrap_or(0.5),
            network_adjusted: true,
        })
    }

    fn calculate_confidence(
        &self,
        metrics: Option<&TipAcceptanceMetrics>,
        profile: Option<&ValidatorProfile>,
    ) -> f64 {
        let data_confidence = if let Some(m) = metrics {
            let trials = m.outcomes.len();
            let succ = m.outcomes.iter().filter(|&&x| x).count();
            let w = wilson_lower_bound(succ, trials, 1.96);
            let recency = m.timestamp_secs.back().copied().map(|last| {
                let age = (now_secs() - last).max(0) as f64;
                (-age / 3600.0).exp()
            }).unwrap_or(0.0);
            0.7 * w * recency
        } else { 0.0 };

        let profile_confidence = profile.map(|p| 0.3 * p.reliability_score).unwrap_or(0.0);
        (data_confidence + profile_confidence).min(1.0)
    }

    pub async fn update_validator_profile(
        &self,
        validator: &Pubkey,
        slot_performance: f64,
    ) -> Result<()> {
        let mut entry = self.validator_profiles.entry(*validator).or_insert_with(|| ValidatorProfile {
            pubkey: *validator,
            historical_acceptance_rate: 0.5,
            tip_sensitivity: 1.0,
            peak_hours: vec![14, 15, 16, 17, 18, 19, 20, 21],
            reliability_score: 0.5,
            recent_performance: VecDeque::with_capacity(50),
            logistic_beta0: 0.0,
            logistic_beta_tip: 0.0,
            logistic_beta_cong: 0.0,
            logistic_beta_hour: 0.0,
            g2_beta0: 0.0,
            g2_beta_tip: 0.0,
            g2_beta_cong: 0.0,
            g2_beta_hour: 0.0,
            temp_log: 0.0,
            g2_temp: 0.0,
            spent_ewma: 0.0,
            ev_ewma: 0.0,
            logloss_ewma: 0.0,
            cusum: 0.0,
            tip_elasticity_coef: 0.0,
            streak_rejects: 0,
            last_recommended_tip: BASE_TIP_LAMPORTS,
            last_outcome: None,
            last_tip_submitted: None,
            cooldown_until_secs: 0,
            last_ev_lost: 0.0,
            last_bundle_value: 0,
            last_p_accept: 0.0,
            logistic_updates: 0,
        });
        {
            let profile = entry.value_mut();
            profile.recent_performance.push_back(slot_performance);
            if profile.recent_performance.len() > 50 {
                profile.recent_performance.pop_front();
            }
            if profile.recent_performance.len() >= 10 {
                let avg_performance: f64 = profile.recent_performance.iter().sum::<f64>() / profile.recent_performance.len() as f64;
                profile.reliability_score = (profile.reliability_score * 0.7 + avg_performance * 0.3).min(1.0).max(0.0);
            }
            if let Some(m) = self.validator_metrics.get(validator) {
                profile.historical_acceptance_rate = m.value().success_rate;
                if m.value().accepted_tips.len() >= MIN_SAMPLE_SIZE && m.value().rejected_tips.len() >= 10 {
                    let accepted_mean = m.value().accepted_tips.iter().sum::<u64>() as f64 / m.value().accepted_tips.len() as f64;
                    let rejected_mean = m.value().rejected_tips.iter().sum::<u64>() as f64 / m.value().rejected_tips.len() as f64;
                    if rejected_mean > 0.0 {
                        profile.tip_sensitivity = (accepted_mean / rejected_mean).min(5.0).max(0.2);
                    }
                }
            }
        }
        
        Ok(())
    }

    // PIECE P8: Beta prior + logistic blend with EW total weighting
    pub async fn predict_acceptance_probability(
        &self,
        validator: &Pubkey,
        tip_amount: u64,
    ) -> Result<f64> {
        let snap = self.network_atomics.read();
        let cur_slot = snap.current_slot;
        if let Some(p) = self.pt_slot_cache.get(&(*validator, tip_amount, cur_slot)) {
            return Ok(*p.value() as f64);
        }
        let vm = self.validator_metrics.get(validator);
        let vp = self.validator_profiles.get(validator);

        // decayed Beta posterior with mild prior
        let (base_p, ew_total) = if let Some(m) = vm.as_ref() {
            let m = m.value();
            let a = 6.0 + m.ew_accept.max(0.0);
            let b = 4.0 + (m.ew_total - m.ew_accept).max(0.0);
            ((a / (a + b)).clamp(0.01, 0.99), m.ew_total.max(0.0))
        } else { (0.6, 0.0) };

        // logistic from snapshot
        let logi_p = if let Some(p) = vp.as_ref() { Self::logistic_predict_from_snap(p.value(), tip_amount, &snap) } else { base_p };

        // weight grows with EW total
        let w_log = (1.0 - (-ew_total / 50.0).exp()).clamp(0.0, 0.85);
        let mut p = w_log * logi_p + (1.0 - w_log) * base_p;

        // monotone guard vs last tip
        if let Some(vpr) = vp.as_ref() {
            let pr = vpr.value();
            if let Some(last_tip) = pr.last_tip_submitted {
                if tip_amount > last_tip && pr.last_p_accept > 0.0 {
                    let floor = (pr.last_p_accept * 0.98).clamp(0.01, 0.995);
                    if p < floor { p = floor; }
                }
            }
        }

        // degradation penalty
        p = self.apply_degradation_penalty(validator, p);
        self.pt_slot_cache.insert((*validator, tip_amount, cur_slot), p as f32);
        Ok(p)
    }

    fn logistic_features(tip: u64, network: &NetworkConditions) -> (f64, f64, f64) {
        // Diminishing returns for higher tips, bounded congestion, cyclic hour embedding (cosine)
        let x_tip = (tip as f64).ln_1p();
        let x_cong = network.congestion_level.clamp(0.0, 1.0);
        let hour = chrono::Utc::now().hour() as f64;
        let x_hour = (hour / 24.0) * 2.0 * std::f64::consts::PI;
        (x_tip, x_cong, x_hour.cos())
    }

    fn logistic_predict(profile: &ValidatorProfile, tip: u64, network: &NetworkConditions) -> f64 {
        let (x_tip, x_cong, hour) = Self::logistic_features(tip, network);
        let z = profile.logistic_beta0 + profile.logistic_beta_tip * x_tip + profile.logistic_beta_cong * x_cong + profile.logistic_beta_hour * hour;
        sigmoid(z)
    }

    #[inline(always)]
    fn logistic_predict_from_snap(profile: &ValidatorProfile, tip: u64, snap: &NetSnap) -> f64 {
        let x_tip = ln1p_tip_fast(tip);
        let x_cong = snap.congestion_level.clamp(0.0, 1.0);
        let x_hour_cos = hour_cos_from_snap(snap);
        let z = profile.logistic_beta0
            + profile.logistic_beta_tip * x_tip
            + profile.logistic_beta_cong * x_cong
            + profile.logistic_beta_hour * x_hour_cos;
        // PIECE E34: apply temperature scaling τ
        let mut tau = profile.temp_log.exp();
        tau = tau.clamp(0.5, 2.0);
        sigmoid_fast(z / tau)
    }

    // S3: focal-loss variant of logistic update (hard-sample focus)
    fn logistic_update_focal(profile: &mut ValidatorProfile, tip: u64, accepted: bool, network: &NetworkConditions) {
        let x_tip  = (tip as f64).ln_1p();
        let x_cong = network.congestion_level.clamp(0.0, 1.0);
        let hour   = chrono::Utc::now().hour() as f64;
        let hour_c = ((hour / 24.0) * 2.0 * std::f64::consts::PI).cos();

        let z = profile.logistic_beta0
              + profile.logistic_beta_tip  * x_tip
              + profile.logistic_beta_cong * x_cong
              + profile.logistic_beta_hour * hour_c;

        let mut tau = profile.temp_log.exp().clamp(0.5, 2.0);
        let a = z / tau;
        let p_hat = sigmoid_fast(a);
        let y = if accepted { 1.0 } else { 0.0 };
        let pt = if accepted { p_hat } else { 1.0 - p_hat };
        let gamma = 1.5;
        let w = (1.0 - pt).powf(gamma).clamp(0.1, 5.0);

        let err = (w * (y - p_hat)).clamp(-0.5, 0.5);
        let lambda = 1e-4;

        let g0 = err - lambda * profile.logistic_beta0;
        let gt = err * x_tip  - lambda * profile.logistic_beta_tip;
        let gc = err * x_cong - lambda * profile.logistic_beta_cong;
        let gh = err * hour_c - lambda * profile.logistic_beta_hour;

        profile.g2_beta0 += g0 * g0;
        profile.g2_beta_tip += gt * gt;
        profile.g2_beta_cong += gc * gc;
        profile.g2_beta_hour += gh * gh;

        let eps = 1e-8;
        let lr0 = 0.08;
        profile.logistic_beta0    += lr0 * g0 / (profile.g2_beta0.sqrt() + eps);
        profile.logistic_beta_tip += lr0 * gt / (profile.g2_beta_tip.sqrt() + eps);
        profile.logistic_beta_cong+= lr0 * gc / (profile.g2_beta_cong.sqrt() + eps);
        profile.logistic_beta_hour+= lr0 * gh / (profile.g2_beta_hour.sqrt() + eps);

        let g_temp = ((p_hat - y) * (z / tau)).clamp(-1.0, 1.0);
        profile.g2_temp += g_temp * g_temp;
        let lr_t = 0.03;
        profile.temp_log -= lr_t * g_temp / (profile.g2_temp.sqrt() + eps);
        let log_lo = (0.5f64).ln();
        let log_hi = (2.0f64).ln();
        profile.temp_log = profile.temp_log.clamp(log_lo, log_hi);

        profile.logistic_updates = profile.logistic_updates.saturating_add(1);

        let eps_ll = 1e-12;
        let logloss = if y > 0.5 { -(p_hat.max(eps_ll)).ln() } else { -(1.0 - p_hat).max(eps_ll).ln() };
        profile.logloss_ewma = 0.9 * profile.logloss_ewma + 0.1 * logloss;
        let excess = (logloss - profile.logloss_ewma).max(0.0);
        profile.cusum = (0.9 * profile.cusum + excess).min(10.0);
    }

    // E52: one-shot per-slot clears via CAS epoch
    #[inline(always)]
    fn on_new_slot_if_needed(&self, cur_slot: u64) {
        let prev = self.slot_epoch.load(Ordering::Relaxed);
        if prev == cur_slot { return; }
        if self.slot_epoch.compare_exchange(prev, cur_slot, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
            self.tip_slot_cache.clear();
            self.pt_slot_cache.clear();
            // optional latency sentinel
            let ew_us = self.alloc_ewma_us.load(Ordering::Relaxed);
            if ew_us > 4_000 {
                self.latency_guard_until_slot.store(cur_slot, Ordering::Relaxed);
            }
        }
    }

    fn logistic_update(profile: &mut ValidatorProfile, tip: u64, accepted: bool, network: &NetworkConditions) {
        let (x_tip, x_cong, x_hour) = Self::logistic_features(tip, network);
        let z = profile.logistic_beta0
            + profile.logistic_beta_tip * x_tip
            + profile.logistic_beta_cong * x_cong
            + profile.logistic_beta_hour * x_hour;
        // With temperature scaling
        let mut tau = profile.temp_log.exp();
        tau = tau.clamp(0.5, 2.0);
        let a = z / tau;
        let p_hat = sigmoid_fast(a);
        let y = if accepted { 1.0 } else { 0.0 };

        // Gradient with clipping and L2 regularization
        let err = (y - p_hat).clamp(-0.5, 0.5);
        let lambda = 1e-4;
        let g0 = err - lambda * profile.logistic_beta0;
        let gt = err * x_tip - lambda * profile.logistic_beta_tip;
        let gc = err * x_cong - lambda * profile.logistic_beta_cong;
        let gh = err * x_hour - lambda * profile.logistic_beta_hour;

        // AdaGrad accumulators
        profile.g2_beta0 += g0 * g0;
        profile.g2_beta_tip += gt * gt;
        profile.g2_beta_cong += gc * gc;
        profile.g2_beta_hour += gh * gh;

        let eps = 1e-8;
        let lr0 = 0.08;
        profile.logistic_beta0    += lr0 * g0 / (profile.g2_beta0.sqrt() + eps);
        profile.logistic_beta_tip += lr0 * gt / (profile.g2_beta_tip.sqrt() + eps);
        profile.logistic_beta_cong+= lr0 * gc / (profile.g2_beta_cong.sqrt() + eps);
        profile.logistic_beta_hour+= lr0 * gh / (profile.g2_beta_hour.sqrt() + eps);

        // PIECE E34: AdaGrad for temperature (optimize log τ)
        let g_temp = ((p_hat - y) * (z / tau)).clamp(-1.0, 1.0);
        profile.g2_temp += g_temp * g_temp;
        let lr_t = 0.03;
        profile.temp_log -= lr_t * g_temp / (profile.g2_temp.sqrt() + eps);
        let log_lo = (0.5f64).ln();
        let log_hi = (2.0f64).ln();
        profile.temp_log = profile.temp_log.clamp(log_lo, log_hi);

        profile.logistic_updates = profile.logistic_updates.saturating_add(1);
        // PIECE E16: online log-loss CUSUM drift detector
        let eps_ll = 1e-12;
        let logloss = if y > 0.5 { -(p_hat.max(eps_ll)).ln() } else { -(1.0 - p_hat).max(eps_ll).ln() };
        profile.logloss_ewma = 0.9 * profile.logloss_ewma + 0.1 * logloss;
        let excess = (logloss - profile.logloss_ewma).max(0.0);
        profile.cusum = (0.9 * profile.cusum + excess).min(10.0);
    }

    // PIECE E16: drift guard
    #[inline(always)]
    fn apply_drift_guard(profile: &mut ValidatorProfile) {
        if profile.cusum > 2.5 {
            profile.logistic_beta0    *= 0.9;
            profile.logistic_beta_tip *= 0.9;
            profile.logistic_beta_cong*= 0.9;
            profile.logistic_beta_hour*= 0.9;
            profile.g2_beta0 = 0.0; profile.g2_beta_tip = 0.0;
            profile.g2_beta_cong = 0.0; profile.g2_beta_hour = 0.0;
            profile.cusum *= 0.25;
        }
    }

    // S6: risk-aware cooldown seconds
    #[inline(always)]
    fn compute_cooldown_secs(&self, profile: &ValidatorProfile, tip_amount: u64, accepted: bool, congestion: f64) -> i64 {
        if accepted { return 0; }
        let streak = profile.streak_rejects as f64;
        let base = 6.0 + 2.0 * streak;
        let ev_proxy = (profile.last_bundle_value as f64) * profile.last_p_accept.max(0.05);
        let ev_scale = 1.0 + (ev_proxy / (tip_amount as f64 + 1.0)).ln_1p().clamp(0.0, 2.0);
        let cong_scale = (0.7 + 0.6 * congestion.clamp(0.0, 1.0)).clamp(0.7, 1.3);
        (base * ev_scale * cong_scale).round() as i64
    }

    pub async fn get_market_insights(&self) -> Result<MarketInsights> {
        let network = self.network_conditions.read().await;
        let metrics = &self.validator_metrics;
        let mq = self.market_quants_rcu.load();
        let market_stats = if mq.count >= MIN_SAMPLE_SIZE {
            MarketStatistics {
                median_tip: mq.percentile_50().unwrap_or(0),
                percentile_25: mq.percentile_25().unwrap_or(0),
                percentile_75: mq.percentile_75().unwrap_or(0),
                percentile_95: mq.percentile_95().unwrap_or(0),
                average_tip: mq.mean() as u64,
                std_deviation: mq.std_dev() as u64,
            }
        } else {
            MarketStatistics::default()
        };

        let top_validators: Vec<(Pubkey, f64)> = metrics.iter()
            .filter(|m| m.value().success_rate > 0.8 && m.value().accepted_tips.len() >= MIN_SAMPLE_SIZE)
            .map(|m| (*m.key(), m.value().success_rate))
            .collect();
        
        Ok(MarketInsights {
            current_market_stats: market_stats,
            network_congestion: network.congestion_level,
            recommended_base_tip: market_stats.percentile_75,
            high_performance_validators: top_validators,
            market_volatility: if market_stats.average_tip > 0 { market_stats.std_deviation as f64 / market_stats.average_tip as f64 } else { 0.0 },
        })
    }

    /// PIECE E1': Water-filling global EV optimizer via bisection on lambda + JoinSet + deadline
    pub async fn optimize_bundle_tips(
        &self,
        validators: &[Pubkey],
        total_budget: u64,
        bundle_value: u64,
    ) -> Result<HashMap<Pubkey, u64>> {
        let t0_wall = Instant::now();
        if validators.is_empty() || total_budget < BASE_TIP_LAMPORTS {
            return Ok(HashMap::new());
        }

        // E73: screen validators to top-K using cheap marginal heuristic
        let shortlist = self.screen_validators_for_bundle(validators);
        let vset: Vec<Pubkey> = if shortlist.is_empty() { validators.to_vec() } else { shortlist };

        let lo = BASE_TIP_LAMPORTS;
        let hi = (MAX_TIP_LAMPORTS).min(total_budget.saturating_mul(2));
        let deadline = Instant::now() + Duration::from_millis(8);

        let mut lam_lo = 0.0f64;
        let mut lam_hi = 1.0f64;

        // Exponential search for lam_hi
        let sem = Arc::new(tokio::sync::Semaphore::new(self.desired_allocator_concurrency()));
        for _ in 0..10 {
            let mut js = JoinSet::new();
            for v in &vset {
                let v2 = *v;
                let s = sem.clone();
                js.spawn(async move {
                    let _permit = s.acquire().await.map_err(|e| anyhow!("semaphore acquire failed: {}", e))?;
                    let t = self.argmax_tip_for_lambda_quick(&v2, bundle_value, lam_hi, lo, hi, deadline).await?;
                    Ok::<u64, anyhow::Error>(t)
                });
            }
            let mut sum_t = 0u64;
            while let Some(res) = js.join_next().await {
                let t = res??;
                sum_t = sum_t.saturating_add(t);
            }
            if sum_t <= total_budget || Instant::now() >= deadline { break; }
            lam_hi *= 2.0;
            if lam_hi > 1e9 { break; }
        }

        // Bisection with cap
        let mut best_map: HashMap<Pubkey, u64> = HashMap::new();
        for _ in 0..16 {
            if Instant::now() >= deadline { break; }
            let lam_mid = 0.5 * (lam_lo + lam_hi);

            let mut js = JoinSet::new();
            for v in &vset {
                let v2 = *v;
                let s = sem.clone();
                js.spawn(async move {
                    let _permit = s.acquire().await.map_err(|e| anyhow!("semaphore acquire failed: {}", e))?;
                    let t = self.argmax_tip_for_lambda_quick(&v2, bundle_value, lam_mid, lo, hi, deadline).await?;
                    Ok::<u64, anyhow::Error>(t)
                });
            }

            let mut sum_t = 0u64;
            let mut tmp_map: HashMap<Pubkey, u64> = HashMap::with_capacity(vset.len());
            for v in &vset {
                if let Some(res) = js.join_next().await {
                    let t = res??;
                    tmp_map.insert(*v, t);
                    sum_t = sum_t.saturating_add(t);
                }
            }

            if sum_t > total_budget {
                lam_lo = lam_mid;
            } else {
                lam_hi = lam_mid;
                best_map = tmp_map;
            }

            if (lam_hi - lam_lo) < 1e-6 { break; }
        }

        // E27: marginal-based slack redistribution (≤1 eval per validator)
        let used = best_map.values().copied().sum::<u64>();
        let mut slack = total_budget.saturating_sub(used);
        if slack >= (BASE_TIP_LAMPORTS / 4) && !best_map.is_empty() {
            // Compute marginal EV gains for a tiny delta per validator
            let mut marginals: Vec<(Pubkey, f64, u64)> = Vec::with_capacity(best_map.len());
            for (v, &t0) in best_map.iter() {
                let delta = (t0 / 64).max(1_000);
                let t1 = (t0.saturating_add(delta)).min(MAX_TIP_LAMPORTS);
                let p0 = self.predict_acceptance_probability(v, t0).await?;
                let p1 = self.predict_acceptance_probability(v, t1).await?;
                let ev0 = (bundle_value as f64) * p0 - (t0 as f64);
                let ev1 = (bundle_value as f64) * p1 - (t1 as f64);
                let gain = (ev1 - ev0).max(0.0);
                marginals.push((*v, gain, delta));
            }
            let wsum = marginals.iter().map(|(_, g, _)| *g).sum::<f64>().max(1e-9);
            for (v, g, _d) in marginals {
                if slack == 0 { break; }
                let share = ((g / wsum) * (slack as f64)).round() as u64;
                let add = share.max(1).min(slack);
                if let Some(t) = best_map.get_mut(&v) {
                    *t = t.saturating_add(add).min(MAX_TIP_LAMPORTS);
                    slack = slack.saturating_sub(add);
                }
            }
            while slack > 0 {
                for v in &vset {
                    if slack == 0 { break; }
                    if let Some(t) = best_map.get_mut(v) {
                        *t = t.saturating_add(1).min(MAX_TIP_LAMPORTS);
                        slack -= 1;
                    }
                }
            }
        }
        // update EWMA allocator wall time
        let us = t0_wall.elapsed().as_micros() as u64;
        let prev = self.alloc_ewma_us.load(Ordering::Relaxed);
        let newv = ((prev * 7) + us) / 8;
        self.alloc_ewma_us.store(newv, Ordering::Relaxed);
        Ok(best_map)
    }

    // PIECE E1': argmax with JoinSet deadline and local memoization
    #[inline(always)]
    async fn argmax_tip_for_lambda_js(
        &self,
        v: &Pubkey,
        bundle_value: u64,
        lambda: f64,
        lo: u64,
        hi: u64,
        deadline: Instant,
    ) -> Result<(u64, f64)> {
        let lo = lo.max(BASE_TIP_LAMPORTS);
        let hi = hi.min(MAX_TIP_LAMPORTS).max(lo + 1);

        // local memo
        let mut p_cache: HashMap<u64, f64> = HashMap::new();
        let mut get_p = |tip: u64| async {
            if let Some(&p) = p_cache.get(&tip) { return Ok::<f64, anyhow::Error>(p); }
            let p = self.get_p_fast_or_async(v, tip).await?;
            p_cache.insert(tip, p);
            Ok::<f64, anyhow::Error>(p)
        };

        const PHI: f64 = 1.6180339887498948;
        let (mut left, mut right) = (lo as f64, hi as f64);
        let mut c = right - (right - left) / PHI;
        let mut d = left + (right - left) / PHI;

        let mut fc = {
            let t = c.round() as u64;
            let p = get_p(t).await?;
            (bundle_value as f64) * p - lambda * (t as f64)
        };
        let mut fd = {
            let t = d.round() as u64;
            let p = get_p(t).await?;
            (bundle_value as f64) * p - lambda * (t as f64)
        };

        let mut iters = 0usize;
        while (right - left) > 1.0 && iters < 17 {
            if Instant::now() >= deadline { break; }
            if fc > fd {
                right = d; d = c; fd = fc;
                c = right - (right - left) / PHI;
                let t = c.round() as u64;
                let p = get_p(t).await?;
                fc = (bundle_value as f64) * p - lambda * (t as f64);
            } else {
                left = c; c = d; fc = fd;
                d = left + (right - left) / PHI;
                let t = d.round() as u64;
                let p = get_p(t).await?;
                fd = (bundle_value as f64) * p - lambda * (t as f64);
            }
            iters += 1;
        }

        let (t_c, t_d) = (c.round() as u64, d.round() as u64);
        let (t_best, _) = if fc >= fd { (t_c, fc) } else { (t_d, fd) };

        let p_final = get_p(t_best).await?;
        let safe_t = self.apply_safety_bounds(t_best, bundle_value, p_final);
        Ok((safe_t, p_final))
    }

    // PIECE E2: concurrency-limited batch predictor
    pub async fn predict_acceptance_probability_many(
        &self,
        pairs: &[(Pubkey, u64)],
        max_concurrency: usize,
    ) -> Result<Vec<(Pubkey, u64, f64)>> {
        let cap = max_concurrency.max(1);
        let mut out = Vec::with_capacity(pairs.len());
        let mut idx = 0usize;
        while idx < pairs.len() {
            let end = (idx + cap).min(pairs.len());
            let mut futs = FuturesUnordered::new();
            for i in idx..end {
                let (v, t) = pairs[i];
                let this = self;
                futs.push(async move {
                    let p = this.predict_acceptance_probability(&v, t).await;
                    (v, t, p)
                });
            }
            while let Some((v, t, p_res)) = futs.next().await {
                let p = p_res?;
                out.push((v, t, p));
            }
            idx = end;
        }
        Ok(out)
    }

    pub async fn analyze_validator_patterns(
        &self,
        validator: &Pubkey,
    ) -> Result<ValidatorPattern> {
        // Fetch metrics and optional profile
        let vm_entry = self.validator_metrics.get(validator).ok_or_else(|| anyhow!("No metrics found for validator"))?;
        let profile = self.validator_profiles.get(validator).map(|e| e.value().clone());
        let vm = vm_entry.value();

        // Acceptance trend from last 20 outcomes with fixed denominator
        let acceptance_trend = if vm.outcomes.len() >= 20 {
            let recent_true = vm.outcomes.iter().rev().take(20).filter(|&&x| x).count() as f64;
            let recent_rate = recent_true / 20.0;
            if recent_rate > vm.success_rate + 0.1 {
                AcceptanceTrend::Improving
            } else if recent_rate < vm.success_rate - 0.1 {
                AcceptanceTrend::Declining
            } else {
                AcceptanceTrend::Stable
            }
        } else {
            AcceptanceTrend::Unknown
        };

        let tip_elasticity = profile.as_ref().map(|p| p.tip_sensitivity).unwrap_or(1.0);

        let avg_inclusion_time = if !vm.bundle_inclusion_times.is_empty() {
            let total: Duration = vm.bundle_inclusion_times.iter().copied().sum();
            total / (vm.bundle_inclusion_times.len() as u32)
        } else {
            Duration::from_millis(400)
        };

        Ok(ValidatorPattern {
            validator: *validator,
            acceptance_trend,
            tip_elasticity,
            average_inclusion_time: avg_inclusion_time,
            reliability_score: profile.as_ref().map(|p| p.reliability_score).unwrap_or(0.5),
            peak_performance_hours: profile.as_ref().map(|p| p.peak_hours.clone()).unwrap_or_default(),
        })
    }
}

// --- Online quantile estimation (Jain–Chlamtac P²) and running stats ---
#[derive(Clone)]
struct P2Quantile {
    p: f64,
    n: [i64; 5],
    q: [f64; 5],
    np: [f64; 5],
    dn: [f64; 5],
    count: usize,
}

impl P2Quantile {
    fn new(p: f64) -> Self {
        assert!((0.0..=1.0).contains(&p));
        Self { p, n: [0; 5], q: [0.0; 5], np: [0.0; 5], dn: [0.0; 5], count: 0 }
    }

    fn insert(&mut self, x: f64) {
        self.count += 1;
        if self.count <= 5 {
            self.q[self.count - 1] = x;
            if self.count == 5 {
                self.q.sort_by(|a, b| a.partial_cmp(b).unwrap());
                self.n = [1, 2, 3, 4, 5];
                self.np = [1.0, 1.0 + 2.0 * self.p, 1.0 + 4.0 * self.p, 3.0 + 2.0 * self.p, 5.0];
                self.dn = [0.0, self.p / 2.0, self.p, (1.0 + self.p) / 2.0, 1.0];
            }
            return;
        }

        let mut k = if x < self.q[0] {
            self.q[0] = x; 0
        } else if x >= self.q[4] {
            self.q[4] = x; 3
        } else {
            let mut k = 0;
            for i in 0..4 {
                if x >= self.q[i] && x < self.q[i + 1] { k = i; break; }
            }
            k
        };
        for i in (k + 1)..5 { self.n[i] += 1; }
        for i in 0..5 { self.np[i] += self.dn[i]; }
        for i in 1..4 {
            let d = self.np[i] - self.n[i] as f64;
            if (d >= 1.0 && self.n[i + 1] - self.n[i] > 1) || (d <= -1.0 && self.n[i - 1] - self.n[i] < -1) {
                let dsign = d.signum() as i64;
                let qi = self.q[i];
                let qip = self.q[i + 1];
                let qim = self.q[i - 1];
                let nip = self.n[i + 1] as f64;
                let ni = self.n[i] as f64;
                let nim = self.n[i - 1] as f64;
                let mut qn = qi + dsign as f64 * ((qip - qi) / (nip - ni) * (ni - nim + dsign as f64) + (qi - qim) / (ni - nim) * (nip - ni - dsign as f64)) / (nip - nim);
                if qn <= qim || qn >= qip {
                    qn = qi + dsign as f64 * (self.q[(i as i64 + dsign) as usize] - qi) / (self.n[(i as i64 + dsign) as usize] - self.n[i]) as f64;
                }
                self.q[i] = qn;
                self.n[i] += dsign;
            }
        }
    }

    fn estimate(&self) -> Option<u64> {
        if self.count < 5 { None } else { Some(self.q[2].max(0.0) as u64) }
    }
}

#[derive(Clone)]
struct MarketQuantiles {
    p01: P2Quantile,
    p25: P2Quantile,
    p50: P2Quantile,
    p75: P2Quantile,
    p95: P2Quantile,
    p99: P2Quantile,
    mean: f64,
    m2: f64,
    count: usize,
}

impl Default for MarketQuantiles {
    fn default() -> Self { Self::new() }
}

impl MarketQuantiles {
    fn new() -> Self {
        Self {
            p01: P2Quantile::new(0.01),
            p25: P2Quantile::new(0.25),
            p50: P2Quantile::new(0.50),
            p75: P2Quantile::new(0.75),
            p95: P2Quantile::new(0.95),
            p99: P2Quantile::new(0.99),
            mean: 0.0,
            m2: 0.0,
            count: 0,
        }
    }
    #[inline(always)]
    fn robust_insert(&mut self, tip: u64) {
        // If not enough samples, just insert
        if self.count < 20 {
            self.insert(tip);
            return;
        }
        let lo = self.percentile_01().unwrap_or(tip);
        let hi = self.percentile_99().unwrap_or(tip);
        // clamp with a widened band to be conservative
        let lo2 = lo.saturating_div(2);
        let hi2 = hi.saturating_mul(2).min(MAX_TIP_LAMPORTS);
        let x = tip.clamp(lo2, hi2);
        self.insert(x);
    }
    fn insert(&mut self, tip: u64) {
        let x = tip as f64;
        self.p01.insert(x);
        self.p25.insert(x);
        self.p50.insert(x);
        self.p75.insert(x);
        self.p95.insert(x);
        self.p99.insert(x);
        self.count += 1;
        // Welford
        let delta = x - self.mean;
        self.mean += delta / self.count as f64;
        self.m2 += delta * (x - self.mean);
    }
    fn percentile_01(&self) -> Option<u64> { self.p01.estimate() }
    fn percentile_25(&self) -> Option<u64> { self.p25.estimate() }
    fn percentile_50(&self) -> Option<u64> { self.p50.estimate() }
    fn percentile_75(&self) -> Option<u64> { self.p75.estimate() }
    fn percentile_95(&self) -> Option<u64> { self.p95.estimate() }
    fn percentile_99(&self) -> Option<u64> { self.p99.estimate() }
    fn mean(&self) -> f64 { self.mean }
    fn std_dev(&self) -> f64 { if self.count > 1 { (self.m2 / (self.count as f64 - 1.0)).sqrt() } else { 0.0 } }
}

#[derive(Debug, Clone)]
pub struct TipRecommendation {
    pub validator: Pubkey,
    pub recommended_tip: u64,
    pub confidence_score: f64,
    pub expected_success_rate: f64,
    pub network_adjusted: bool,
}

#[derive(Debug, Clone)]
pub struct MarketInsights {
    pub current_market_stats: MarketStatistics,
    pub network_congestion: f64,
    pub recommended_base_tip: u64,
    pub high_performance_validators: Vec<(Pubkey, f64)>,
    pub market_volatility: f64,
}

#[derive(Debug, Clone, Default)]
pub struct MarketStatistics {
    pub median_tip: u64,
    pub percentile_25: u64,
    pub percentile_75: u64,
    pub percentile_95: u64,
    pub average_tip: u64,
    pub std_deviation: u64,
}

#[derive(Debug, Clone)]
pub struct ValidatorPattern {
    pub validator: Pubkey,
    pub acceptance_trend: AcceptanceTrend,
    pub tip_elasticity: f64,
    pub average_inclusion_time: Duration,
    pub reliability_score: f64,
    pub peak_performance_hours: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AcceptanceTrend {
    Improving,
    Stable,
    Declining,
    Unknown,
}

fn calculate_std_dev(values: &[u64]) -> u64 {
    if values.is_empty() {
        return 0;
    }
    
    let mean = values.iter().sum::<u64>() / values.len() as u64;
    let variance = values.iter()
        .map(|&x| {
            let diff = if x > mean { x - mean } else { mean - x };
            diff * diff
        })
        .sum::<u64>() / values.len() as u64;
    
    (variance as f64).sqrt() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_tip_calculation() {
        let modeler = ValidatorTipAcceptanceModeler::new();
        let validator = Pubkey::new_unique();
        
        let tip = modeler.calculate_optimal_tip(&validator, 1_000_000_000, 1.0).await.unwrap();
        assert!(tip >= BASE_TIP_LAMPORTS);
        assert!(tip <= MAX_TIP_LAMPORTS);
    }

    #[tokio::test]
    async fn test_acceptance_probability() {
        let modeler = ValidatorTipAcceptanceModeler::new();
        let validator = Pubkey::new_unique();
        
        let prob = modeler.predict_acceptance_probability(&validator, 100_000).await.unwrap();
        assert!(prob >= 0.0 && prob <= 1.0);
    }
}
