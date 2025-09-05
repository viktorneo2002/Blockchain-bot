use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use solana_sdk::pubkey::Pubkey;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock as AsyncRwLock;
use anyhow::{Result, anyhow};
use chrono::Timelike;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use prometheus::{register_histogram, register_histogram_vec, register_gauge, register_gauge_vec, register_int_counter_vec, Histogram, HistogramVec, IntCounterVec, Gauge, GaugeVec};

const SLIDING_WINDOW_SIZE: usize = 1000;
const MIN_SAMPLE_SIZE: usize = 50;
const DECAY_FACTOR: f64 = 0.95;
const BASE_TIP_LAMPORTS: u64 = 10_000;
const MAX_TIP_LAMPORTS: u64 = 50_000_000;
const CONFIDENCE_THRESHOLD: f64 = 0.85;
const OUTLIER_THRESHOLD: f64 = 3.0;
const PRIORITY_FEE_PERCENTILE: f64 = 0.75;

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

#[inline]
fn observe_tip(amt: u64) { TIP_RECOMMENDATION.observe(amt as f64); }
#[inline]
fn observe_outcome(v: &Pubkey, accepted: bool) {
    let v_label = v.to_string();
    ACCEPT_REJECT.with_label_values(&[&v_label, if accepted { "accept" } else { "reject" }]).inc();
}
#[inline]
fn observe_accept_prob(p: f64) { ACCEPT_PROB.observe(p.clamp(0.0, 1.0)); }
#[inline]
fn observe_inclusion(dur: Duration) { INCLUSION_MS.observe(dur.as_secs_f64() * 1000.0); }

#[inline]
fn label_for(v: &Pubkey) -> String { v.to_string() }

#[inline]
fn now_secs() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs() as i64).unwrap_or(0)
}

#[inline]
fn sigmoid(x: f64) -> f64 { 1.0 / (1.0 + (-x).exp()) }

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
    fn leader_phase_multiplier(&self, network: &NetworkConditions, validator: &Pubkey) -> f64 {
        let cur = network.current_slot;
        for s in cur..(cur + 2) {
            if network.slot_leader_schedule.get(&s) == Some(validator) { return 1.10; }
        }
        1.0
    }

    fn slot_phase_multiplier(current_slot: u64) -> f64 {
        match current_slot % 4 {
            0 => 1.05,
            1 | 2 => 1.0,
            _ => 1.15,
        }
    }
    fn update_validator_statistics_inplace(metrics: &mut TipAcceptanceMetrics) {
        let total_attempts = metrics.outcomes.len();
        if total_attempts > 0 {
            // Exponential decay weighting for recency
            let mut weighted_accepts = 0.0;
            let mut weighted_total = 0.0;
            for (i, outcome) in metrics.outcomes.iter().enumerate() {
                let w = DECAY_FACTOR.powi((total_attempts - i - 1) as i32);
                weighted_total += w;
                if *outcome { weighted_accepts += w; }
            }

            // Bayesian update with Beta(1,1) prior (uniform)
            let alpha = 1.0 + weighted_accepts;
            let beta = 1.0 + (weighted_total - weighted_accepts).max(0.0);
            let denom = alpha + beta;
            metrics.success_rate = if denom > 0.0 { alpha / denom } else { 0.5 };
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
    validator_metrics: Arc<DashMap<Pubkey, TipAcceptanceMetrics>>,
    validator_profiles: Arc<DashMap<Pubkey, ValidatorProfile>>,
    network_conditions: Arc<AsyncRwLock<NetworkConditions>>,
    market_quants: Arc<AsyncRwLock<MarketQuantiles>>,
    degradation: Arc<DashMap<Pubkey, DegradationState>>,
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
            validator_metrics: Arc::new(DashMap::new()),
            validator_profiles: Arc::new(DashMap::new()),
            network_conditions: Arc::new(AsyncRwLock::new(NetworkConditions {
                current_tps: 2000.0,
                average_priority_fee: 1_000,
                slot_leader_schedule: HashMap::new(),
                recent_block_rewards: VecDeque::with_capacity(100),
                congestion_level: 0.5,
                current_slot: 0,
            })),
            market_quants: Arc::new(AsyncRwLock::new(MarketQuantiles::new())),
            degradation: Arc::new(DashMap::new()),
            model_parameters: ModelParameters::default(),
        }
    }

    pub async fn calculate_optimal_tip(
        &self,
        validator: &Pubkey,
        bundle_value: u64,
        urgency_factor: f64,
    ) -> Result<u64> {
        // Fetch current views
        let metrics = &self.validator_metrics;
        let profiles = &self.validator_profiles;
        let network = self.network_conditions.read().await.clone();

        let validator_metrics = metrics.get(validator).map(|r| r.value().clone());
        let validator_profile = profiles.get(validator).map(|r| r.value().clone());

        // Early cooldown guard: if validator is cooling down, avoid spending budget
        if let Some(profile) = validator_profile.as_ref() {
            if profile.cooldown_until_secs > now_secs() {
                let conservative = BASE_TIP_LAMPORTS;
                let v_label = label_for(validator);
                TIP_RECOMMENDATION_BY_VALIDATOR.with_label_values(&[&v_label]).observe(conservative as f64);
                EV_EFFICIENCY.with_label_values(&[&v_label]).set(0.0);
                return Ok(conservative);
            }
        }

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
            let mq = self.market_quants.read().await;
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
        let candidate = self.optimize_tip_ev(validator, bundle_value, competitive_tip / 2, competitive_tip.saturating_mul(2).min(MAX_TIP_LAMPORTS)).await?;
        let p_accept = self.predict_acceptance_probability(validator, candidate).await?;

        // 6) EV guardrails and finalize
        let final_tip = self.apply_safety_bounds(candidate, bundle_value, p_accept);

        // Persist and observe
        if let Some(mut pe) = self.validator_profiles.get_mut(validator) {
            let pr = pe.value_mut();
            pr.last_recommended_tip = final_tip;
            pr.last_bundle_value = bundle_value;
            pr.last_p_accept = p_accept;
        }
        observe_tip(final_tip);
        let v_label = label_for(validator);
        ACCEPT_PROB_BY_VALIDATOR.with_label_values(&[&v_label]).observe(p_accept);
        TIP_RECOMMENDATION_BY_VALIDATOR.with_label_values(&[&v_label]).observe(final_tip as f64);
        let ev = (bundle_value as f64) * p_accept;
        let eff = if final_tip > 0 { ev / (final_tip as f64) } else { 0.0 };
        EV_EFFICIENCY.with_label_values(&[&v_label]).set(eff);
        Ok(final_tip)
    }

    fn calculate_base_tip(
        &self,
        metrics: Option<&TipAcceptanceMetrics>,
        profile: Option<&ValidatorProfile>,
        network: &NetworkConditions,
        bundle_value: u64,
    ) -> u64 {
        let network_base = (network.average_priority_fee as f64 * 
                           network.congestion_level * 
                           self.model_parameters.congestion_multiplier) as u64;
        
        let validator_base = if let Some(m) = metrics {
            if m.accepted_tips.len() >= MIN_SAMPLE_SIZE {
                m.percentile_95_tip
            } else {
                m.median_accepted_tip.max(BASE_TIP_LAMPORTS)
            }
        } else {
            network_base.max(BASE_TIP_LAMPORTS)
        };
        
        let reputation_factor = if let Some(p) = profile {
            1.0 + (p.reliability_score * self.model_parameters.validator_reputation_weight)
        } else {
            1.0
        };
        
        let value_based_tip = (bundle_value as f64 * 0.001) as u64;
        
        ((validator_base.max(value_based_tip) as f64) * 
         reputation_factor * 
         self.model_parameters.base_multiplier) as u64
    }

    fn apply_dynamic_adjustments(
        &self,
        base_tip: u64,
        urgency_factor: f64,
        network: &NetworkConditions,
        profile: Option<&ValidatorProfile>,
    ) -> u64 {
        let congestion_adjustment = 1.0 + (network.congestion_level - 0.5) * 0.5;

        let hour = chrono::Utc::now().hour() as u8;
        let time_sensitivity = profile
            .map(|p| if p.peak_hours.binary_search(&hour).is_ok() { 1.2 } else { 1.0 })
            .unwrap_or(1.0);

        let leader_mult = profile.map(|p| self.leader_phase_multiplier(network, &p.pubkey)).unwrap_or(1.0);
        let slot_phase_mult = Self::slot_phase_multiplier(network.current_slot);
        let urgency_multiplier = 1.0 + (urgency_factor - 1.0).clamp(0.0, 2.0) * 0.3;

        ((base_tip as f64)
            * congestion_adjustment
            * time_sensitivity
            * leader_mult
            * slot_phase_mult
            * urgency_multiplier) as u64
    }

    async fn apply_competitive_analysis_async(
        &self,
        tip: u64,
        metrics: Option<&TipAcceptanceMetrics>,
    ) -> u64 {
        let mq_guard = self.market_quants.read().await;
        if mq_guard.count < MIN_SAMPLE_SIZE {
            return (tip as f64 * self.model_parameters.competitive_factor) as u64;
        }
        // Clamp competition level to central 98% to ignore outliers
        let p01 = mq_guard.percentile_01().unwrap_or(tip);
        let p99 = mq_guard.percentile_99().unwrap_or(tip);
        let mut p75 = mq_guard.percentile_75().unwrap_or(tip);
        if p75 < p01 { p75 = p01; }
        if p75 > p99 { p75 = p99; }
        let competitive_tip = tip.max(p75);
        
        if let Some(m) = metrics {
            if m.success_rate < 0.7 {
                ((competitive_tip as f64) * 1.15) as u64
            } else {
                competitive_tip
            }
        } else {
            ((competitive_tip as f64) * self.model_parameters.competitive_factor) as u64
        }
    }

    fn apply_safety_bounds(&self, tip: u64, bundle_value: u64, p_accept: f64) -> u64 {
        // EV guardrail: cap tip at 25% of EV = value * p_accept
        let max_tip_ev = (bundle_value as f64 * 0.25 * p_accept.clamp(0.0, 1.0)).floor() as u64;
        let max_reasonable_tip = (bundle_value as f64 * 0.5) as u64;
        tip.min(max_tip_ev).min(max_reasonable_tip).min(MAX_TIP_LAMPORTS).max(BASE_TIP_LAMPORTS)
    }

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
                Self::update_validator_statistics_inplace(m);
            }
        }

        // Update global dist after releasing validator lock
        self.update_global_distribution(tip_amount, accepted).await?;

        // Online logistic calibration update (after releasing metrics map)
        let network = self.network_conditions.read().await;
        if let Some(mut entry) = self.validator_profiles.get_mut(validator) {
            let p = entry.value_mut();
            Self::logistic_update(p, tip_amount, accepted, &network);
            // streak logic and elasticity update
            if accepted { p.streak_rejects = 0; } else { p.streak_rejects = p.streak_rejects.saturating_add(1); }
            if let Some(prev_outcome) = p.last_outcome {
                if !prev_outcome && accepted {
                    if let Some(prev_tip) = p.last_tip_submitted { if tip_amount > prev_tip {
                        let inc = (tip_amount as f64 / prev_tip.max(1) as f64) - 1.0;
                        p.tip_elasticity_coef = 0.9 * p.tip_elasticity_coef + 0.1 * inc.clamp(0.0, 0.5);
                    }}
                }
            }
            // Risk-aware cooldown after missed inclusion
            if !accepted && p.streak_rejects >= 3 {
                // Approximate EV lost if not provided elsewhere: use tip amount as a conservative proxy
                let ev_lost = tip_amount as f64; // Replace with bundle_value * p_accept if available
                p.last_ev_lost = ev_lost;
                let base = 10.0; // seconds
                let streak_scale = 1.0 + 0.2 * (p.streak_rejects as f64);
                let cong = self.network_conditions.read().await.congestion_level.clamp(0.0, 1.0);
                let cong_scale = 0.5 + cong; // [0.5, 1.5]
                let ev_scale = 1.0 + ev_lost.ln_1p() / 10.0; // grows slowly
                let cooldown = (base * streak_scale * cong_scale * ev_scale).round() as i64;
                p.cooldown_until_secs = now_secs() + cooldown;
                VALIDATOR_COOLDOWN.with_label_values(&[&validator.to_string()]).set(cooldown as f64);
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
        if accepted {
            let mut mq = self.market_quants.write().await;
            mq.insert(tip_amount);
        }
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
        network.congestion_level = congestion_level.min(1.0).max(0.0);
        CONGESTION_GAUGE.set(network.congestion_level);
        
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

    pub async fn predict_acceptance_probability(
        &self,
        validator: &Pubkey,
        tip_amount: u64,
    ) -> Result<f64> {
        let metrics = &self.validator_metrics;
        let profiles = &self.validator_profiles;
        let network = self.network_conditions.read().await;
        
        let validator_metrics = metrics.get(validator);
        let validator_profile = profiles.get(validator);
        
        let base_probability = if let Some(m) = validator_metrics {
            if m.accepted_tips.is_empty() {
                0.5
            } else if tip_amount < m.min_accepted_tip {
                0.1 * (tip_amount as f64 / m.min_accepted_tip as f64)
            } else if tip_amount >= m.percentile_95_tip {
                0.95.min(m.success_rate + 0.15)
            } else {
                let normalized_tip = (tip_amount - m.min_accepted_tip) as f64 
                    / (m.percentile_95_tip - m.min_accepted_tip).max(1) as f64;
                m.success_rate * (0.5 + 0.5 * normalized_tip)
            }
        } else {
            0.5
        };
        
        let network_adjustment = 1.0 - (network.congestion_level - 0.5).abs() * 0.2;
        let profile_adjustment = if let Some(p) = validator_profile { 1.0 + (p.reliability_score - 0.5) * 0.3 } else { 1.0 };

        // Logistic prediction based on online calibrated parameters
        let logistic_p = if let Some(p) = validator_profile { Self::logistic_predict(p.value(), tip_amount, &network) } else { base_probability };
        // Blend logistic and base depending on sample size
        let trials = validator_metrics.map(|m| m.outcomes.len()).unwrap_or(0);
        let w_log = if trials >= MIN_SAMPLE_SIZE { 0.6 } else { 0.3 };
        let blended = w_log * logistic_p + (1.0 - w_log) * base_probability;

        let final_probability = (blended * network_adjustment * profile_adjustment)
            .clamp(0.01, 0.99);
        Ok(final_probability)
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

    fn logistic_update(profile: &mut ValidatorProfile, tip: u64, accepted: bool, network: &NetworkConditions) {
        let (x_tip, x_cong, x_hour) = Self::logistic_features(tip, network);
        let z = profile.logistic_beta0
            + profile.logistic_beta_tip * x_tip
            + profile.logistic_beta_cong * x_cong
            + profile.logistic_beta_hour * x_hour;
        let p_hat = sigmoid(z);
        let y = if accepted { 1.0 } else { 0.0 };
        let err = y - p_hat;
        // Decaying learning rate for online Bayesian-flavored adaptation
        profile.logistic_updates = profile.logistic_updates.saturating_add(1);
        let lr0 = 0.05;
        let lr = lr0 / (1.0 + (profile.logistic_updates as f64).sqrt() * 0.1);
        profile.logistic_beta0 += lr * err;
        profile.logistic_beta_tip += lr * err * x_tip;
        profile.logistic_beta_cong += lr * err * x_cong;
        profile.logistic_beta_hour += lr * err * x_hour;
    }

    pub async fn get_market_insights(&self) -> Result<MarketInsights> {
        let network = self.network_conditions.read().await;
        let metrics = &self.validator_metrics;
        let mq = self.market_quants.read().await;
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

    pub async fn optimize_bundle_tips(
        &self,
        validators: &[Pubkey],
        total_budget: u64,
        bundle_value: u64,
    ) -> Result<HashMap<Pubkey, u64>> {
        let mut allocations = HashMap::new();
        let mut remaining_budget = total_budget;
        
        let mut validator_scores: Vec<(Pubkey, f64, u64)> = Vec::new();
        
        for validator in validators {
            let optimal_tip = self.calculate_optimal_tip(
                validator,
                bundle_value,
                1.0,
            ).await?;
            
            let acceptance_prob = self.predict_acceptance_probability(
                validator,
                optimal_tip,
            ).await?;
            
            let score = acceptance_prob * (1.0 / (optimal_tip as f64 + 1.0).ln());
            validator_scores.push((*validator, score, optimal_tip));
        }
        
        // Cross-validator arbitrage tilt: prefer validators that are competitively priced vs the group
        if !validator_scores.is_empty() {
            let mut tips: Vec<u64> = validator_scores.iter().map(|(_,_,t)| *t).collect();
            tips.sort_unstable();
            let mid = tips[tips.len()/2];
            let alpha = 0.2; // mild tilt
            for (.., score, tip) in validator_scores.iter_mut() {
                let rel = if *tip > 0 { (mid as f64) / (*tip as f64) } else { 1.0 };
                *score *= rel.powf(alpha);
            }
        }

        validator_scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        
        for (validator, _, optimal_tip) in validator_scores {
            if remaining_budget >= optimal_tip {
                allocations.insert(validator, optimal_tip);
                remaining_budget -= optimal_tip;
            } else if remaining_budget >= BASE_TIP_LAMPORTS {
                allocations.insert(validator, remaining_budget);
                break;
            }
        }
        
        Ok(allocations)
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
    p25: P2Quantile,
    p50: P2Quantile,
    p75: P2Quantile,
    p95: P2Quantile,
    mean: f64,
    m2: f64,
    count: usize,
}

impl Default for MarketQuantiles {
    fn default() -> Self { Self::new() }
}

impl MarketQuantiles {
    fn new() -> Self {
        Self { p25: P2Quantile::new(0.25), p50: P2Quantile::new(0.50), p75: P2Quantile::new(0.75), p95: P2Quantile::new(0.95), mean: 0.0, m2: 0.0, count: 0 }
    }
    fn insert(&mut self, tip: u64) {
        let x = tip as f64;
        self.p25.insert(x); self.p50.insert(x); self.p75.insert(x); self.p95.insert(x);
        self.count += 1;
        // Welford
        let delta = x - self.mean;
        self.mean += delta / self.count as f64;
        self.m2 += delta * (x - self.mean);
    }
    fn percentile_25(&self) -> Option<u64> { self.p25.estimate() }
    fn percentile_50(&self) -> Option<u64> { self.p50.estimate() }
    fn percentile_75(&self) -> Option<u64> { self.p75.estimate() }
    fn percentile_95(&self) -> Option<u64> { self.p95.estimate() }
    // Approximate 1% and 99% with extra P2s (additionally tracked via p25/p50 etc.); for now derive from p25/p95 ranges conservatively
    fn percentile_01(&self) -> Option<u64> { Some((self.percentile_25().unwrap_or(0) as f64 * 0.5) as u64) }
    fn percentile_99(&self) -> Option<u64> { Some((self.percentile_95().unwrap_or(0) as f64 * 1.2) as u64) }
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
