use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bincode;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use solana_sdk::clock::Slot;
use solana_sdk::pubkey::Pubkey;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::interval;

// chrono bits (robust timestamp handling)
use chrono::{DateTime, Datelike, NaiveDateTime, Timelike, Utc};

// fast hash for HFT-grade maps
use ahash::RandomState;

// Handy alias so we pick AHash transparently everywhere
type DMap<K, V> = DashMap<K, V, RandomState>;

// serde defaults for backward-compatible loads
fn default_alpha() -> f64 { 2.0 }
fn default_beta() -> f64 { 2.0 }
fn default_ewma_latency() -> f64 { 200.0 }

// ---------- Tunables (HFT-safe) ----------
const PREFERENCE_WINDOW_SIZE: usize = 10_000;
const MIN_SAMPLE_SIZE: usize = 50;

// Time-coalesced recomputation cadence (don’t recompute per event).
const PREFERENCE_UPDATE_INTERVAL: Duration = Duration::from_millis(400);

// Exponential decay via half-life (hours) => consistent across machine restarts
const DECAY_HALFLIFE_HOURS: f64 = 24.0; // halve weight each 24h
const MAX_VALIDATORS_TRACKED: usize = 500;

// Robust stats knobs
const OUTLIER_Z_THRESHOLD: f64 = 3.0; // for z/MAD filtering
const MAD_EPS: f64 = 1e-9;

// Confidence gating
const CONFIDENCE_THRESHOLD: f64 = 0.75;

// Slot history cap (~2 days on Solana ~ 432k slots)
const SLOT_HISTORY_SIZE: usize = 432_000;

// UCB exploration (upper-confidence bonus to avoid overfitting one validator)
const UCB_BONUS_SCALE: f64 = 0.10; // ~10% exploratory bias

// Latency scales used in scoring/logistic (soft knee)
const LAT_MS_SOFT_RELIABILITY: f64 = 50.0;
const LAT_MS_SOFT_SELECTION:  f64 = 80.0;

// --- numerics helpers (no NaNs) ---
#[inline(always)]
fn safe_logit(p: f64) -> f64 {
    let q = p.clamp(1e-6, 1.0 - 1e-6);
    (q / (1.0 - q)).ln()
}

#[inline(always)]
fn safe_ln(x: f64) -> f64 {
    (x.max(1e-9)).ln()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorMetrics {
    pub pubkey: Pubkey,
    pub total_slots_processed: u64,
    pub mev_inclusion_rate: f64,
    pub avg_latency_ms: f64,
    pub fee_preference: FeePreference,
    pub ordering_preference: OrderingPreference,
    pub temporal_patterns: TemporalPatterns,
    pub reliability_score: f64,
    pub last_updated: u64,
    pub confidence_score: f64,
    // NEW: Bayesian counts for inclusion (for UCB + calibration)
    #[serde(default = "default_alpha")]
    pub alpha_included: f64,
    #[serde(default = "default_beta")]
    pub beta_excluded: f64,
    // NEW: EWMA counters (for continuous decay)
    #[serde(default = "default_ewma_latency")]
    pub ewma_latency_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeePreference {
    pub min_priority_fee: u64,
    pub avg_accepted_fee: f64,
    pub fee_stdev: f64,
    pub high_fee_affinity: f64,
    // NEW: robust quantiles for optimal bidding
    pub p50: f64,
    pub p75: f64,
    pub p90: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderingPreference {
    pub frontrun_tendency: f64,
    pub backrun_tendency: f64,
    pub sandwich_success_rate: f64,
    pub bundle_acceptance_rate: f64,
    pub ordering_consistency: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalPatterns {
    pub hourly_activity: [f64; 24],
    pub peak_hours: Vec<u8>,
    pub low_activity_hours: Vec<u8>,
    pub weekly_pattern: [f64; 7],
}

#[derive(Debug, Clone)]
pub struct TransactionOutcome {
    pub validator: Pubkey,
    pub slot: Slot,
    pub included: bool,
    pub latency_ms: u64,
    pub priority_fee: u64,
    pub transaction_type: TransactionType,
    pub position_in_block: Option<usize>,
    pub block_size: Option<usize>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionType {
    Arbitrage,
    Liquidation,
    Sandwich,
    Frontrun,
    Backrun,
    Standard,
}

pub struct ValidatorPreferenceLearner {
    metrics: Arc<DMap<Pubkey, ValidatorMetrics>>,
    transaction_history: Arc<DMap<Pubkey, VecDeque<TransactionOutcome>>>,
    slot_performance: Arc<DMap<Slot, SlotMetrics>>,
    // mark validators needing recompute
    updated_validators: Arc<DMap<Pubkey, u64>>,
    update_sender: Sender<PreferenceUpdate>,
    persistence_path: String,
}

#[derive(Debug, Clone)]
struct SlotMetrics {
    validator: Pubkey,
    total_transactions: u64,
    mev_transactions: u64,
    avg_priority_fee: f64,
    timestamp: u64,
}

#[derive(Debug)]
enum PreferenceUpdate {
    TransactionOutcome(TransactionOutcome),
    SlotComplete(Slot, SlotMetrics),
    PersistState,
}

impl ValidatorPreferenceLearner {
    pub fn new(persistence_path: String) -> Self {
        let (tx, rx) = channel(10_000);
        let metrics = Arc::new(DMap::with_hasher(RandomState::new()));
        let transaction_history = Arc::new(DMap::with_hasher(RandomState::new()));
        let slot_performance = Arc::new(DMap::with_hasher(RandomState::new()));
        let updated_validators = Arc::new(DMap::with_hasher(RandomState::new()));
        
        let learner = Self {
            metrics: metrics.clone(),
            transaction_history: transaction_history.clone(),
            slot_performance: slot_performance.clone(),
            updated_validators: updated_validators.clone(),
            update_sender: tx,
            persistence_path: persistence_path.clone(),
        };

        learner.load_state();
        learner.spawn_update_processor(rx);
        learner.spawn_decay_processor();
        learner.spawn_persistence_task();
        learner.spawn_coalesced_recompute();
        
        learner
    }

    pub async fn record_transaction_outcome(&self, outcome: TransactionOutcome) -> Result<(), String> {
        self.update_sender
            .send(PreferenceUpdate::TransactionOutcome(outcome))
            .await
            .map_err(|e| format!("Failed to send update: {}", e))
    }

    pub fn get_validator_preference(&self, validator: &Pubkey) -> Option<ValidatorMetrics> {
        self.metrics.get(validator).map(|entry| entry.clone())
    }

    pub fn get_top_validators_for_type(&self, tx_type: TransactionType, limit: usize) -> Vec<(Pubkey, f64)> {
        let tnow = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as f64;
        let mut v: Vec<(Pubkey, f64)> = self.metrics
            .iter()
            .filter_map(|e| {
                let m = e.value();
                if m.confidence_score < CONFIDENCE_THRESHOLD { return None; }

                let base = match tx_type {
                    TransactionType::Arbitrage => m.mev_inclusion_rate * m.reliability_score
                        * (1.0 / (1.0 + m.ewma_latency_ms / LAT_MS_SOFT_SELECTION)),
                    TransactionType::Liquidation => m.mev_inclusion_rate * m.reliability_score * 1.25,
                    TransactionType::Sandwich => m.ordering_preference.sandwich_success_rate * m.reliability_score,
                    TransactionType::Frontrun => m.ordering_preference.frontrun_tendency * m.reliability_score,
                    TransactionType::Backrun => m.ordering_preference.backrun_tendency * m.reliability_score,
                    TransactionType::Standard => m.reliability_score,
                };

                // UCB exploration bonus using Beta posterior variance ≈ ab/[(a+b)^2 (a+b+1)]
                let a = m.alpha_included.max(1.0);
                let b = m.beta_excluded.max(1.0);
                let post_var = (a * b) / (((a + b).powi(2)) * (a + b + 1.0));
                // age bonus (fresher -> a tiny boost)
                let age_hours = ((tnow as u64).saturating_sub(m.last_updated) as f64) / 3600.0;
                let freshness = (0.5f64).powf(age_hours / 2.0); // 2h half-life
                let bonus = UCB_BONUS_SCALE * (post_var.sqrt()) * (0.5 + 0.5 * freshness);

                Some((*e.key(), (base * (1.0 + bonus)).clamp(0.0, 10.0)))
            })
            .collect();

        v.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        v.truncate(limit);
        v
    }

    pub fn get_optimal_fee_for_validator(&self, validator: &Pubkey, urgency: f64) -> u64 {
        // urgency in [0, 2] — 0 = chill (p50), 1 = normal (p75), 2 = aggressive (p90+stdev)
        let u = urgency.clamp(0.0, 2.0);
        self.metrics.get(validator).map(|m| {
            let f = &m.fee_preference;
            let base = if u < 0.5 {
                f.p50
            } else if u < 1.5 {
                f.p75
            } else {
                f.p90 + f.fee_stdev
            };
            // safety pad 8% (reduce rejections when relay load spikes)
            ((base * 1.08).max(f.min_priority_fee as f64)) as u64
        }).unwrap_or(5_000)
    }

    pub fn get_best_submission_time(&self, validator: &Pubkey) -> Option<u8> {
        self.metrics.get(validator).and_then(|metrics| {
            metrics.temporal_patterns.peak_hours.first().copied()
        })
    }

    fn spawn_update_processor(&self, mut rx: Receiver<PreferenceUpdate>) {
        let history = self.transaction_history.clone();
        let slot_performance = self.slot_performance.clone();
        let updated = self.updated_validators.clone();

        tokio::spawn(async move {
            while let Some(update) = rx.recv().await {
                match update {
                    PreferenceUpdate::TransactionOutcome(outcome) => {
                        // Append outcome (bounded window)
                        let mut dq = history.entry(outcome.validator).or_insert_with(VecDeque::new);
                        if dq.len() >= PREFERENCE_WINDOW_SIZE {
                            dq.pop_front();
                        }
                        dq.push_back(outcome.clone());
                        // Mark validator dirty with last-seen ts
                        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                        updated.insert(outcome.validator, now);
                    }
                    PreferenceUpdate::SlotComplete(slot, sm) => {
                        slot_performance.insert(slot, sm);
                        // NB: cleanup handled elsewhere
                    }
                    PreferenceUpdate::PersistState => { /* handled by persistence task */ }
                }
            }
        });
    }

    fn spawn_coalesced_recompute(&self) {
        let metrics = self.metrics.clone();
        let history = self.transaction_history.clone();
        let updated = self.updated_validators.clone();

        tokio::spawn(async move {
            let mut tick = interval(PREFERENCE_UPDATE_INTERVAL);
            loop {
                tick.tick().await;

                // collect dirty set to recompute
                let dirty: Vec<Pubkey> = updated.iter().map(|e| *e.key()).collect();
                if dirty.is_empty() { continue; }

                for vid in dirty {
                    // detach history copy to avoid holding map locks
                    let maybe_hist = history.get(&vid).map(|r| r.clone());
                    if let Some(outcomes) = maybe_hist {
                        if outcomes.len() >= MIN_SAMPLE_SIZE {
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

                            // --- Inclusion (Bayesian) ---
                            let (included, total) = outcomes.iter().fold((0usize, 0usize), |acc, o| {
                                (acc.0 + (o.included as usize), acc.1 + 1)
                            });
                            // Jeffreys prior (α=0.5, β=0.5) or a slightly stronger α=2,β=2 for stability
                            let (alpha, beta) = (2.0, 2.0);
                            let posterior_mean = (included as f64 + alpha) / (total as f64 + alpha + beta);

                            // --- Latency (EWMA) ---
                            let lat_inc: Vec<f64> = outcomes
                                .iter()
                                .filter(|o| o.included)
                                .map(|o| o.latency_ms as f64)
                                .collect();
                            let avg_latency = if lat_inc.is_empty() {
                                1000.0
                            } else {
                                lat_inc.iter().sum::<f64>() / lat_inc.len() as f64
                            };

                            // --- Fees (robust, MAD & quantiles) ---
                            let fees_inc: Vec<f64> = outcomes
                                .iter()
                                .filter(|o| o.included)
                                .map(|o| o.priority_fee as f64)
                                .collect();
                            let fee_pref = Self::robust_fee_preference(&fees_inc);

                            // --- Ordering prefs ---
                            let ordering_pref = Self::calculate_ordering_preference(&outcomes);

                            // --- Temporal patterns ---
                            let temporal = Self::calculate_temporal_patterns(&outcomes);

                            // --- Reliability (time-weighted success & latency) ---
                            let reliability = Self::calculate_reliability_score(&outcomes);

                            // --- Confidence ---
                            let confidence = Self::calculate_confidence_score(outcomes.len(), reliability);

                            // --- EWMA latency continuation ---
                            let mut ewma_lat = avg_latency;
                            if let Some(mut cur) = metrics.get_mut(&vid) {
                                let v = cur.value_mut();
                                // half-life to alpha conversion for latency smoothing per recompute pull
                                // choose hl=5 samples → alpha = 1 - 0.5^(1/5)
                                let alpha_lat = 1.0 - (0.5f64).powf(1.0 / 5.0);
                                ewma_lat = alpha_lat * avg_latency + (1.0 - alpha_lat) * v.ewma_latency_ms.max(1.0);
                            }

                            let updated_metrics = ValidatorMetrics {
                                pubkey: vid,
                                total_slots_processed: outcomes.iter().map(|o| o.slot).collect::<HashSet<_>>().len() as u64,
                                mev_inclusion_rate: posterior_mean,
                                avg_latency_ms: avg_latency,
                                ewma_latency_ms: ewma_lat,
                                fee_preference: fee_pref,
                                ordering_preference: ordering_pref,
                                temporal_patterns: temporal,
                                reliability_score: reliability,
                                last_updated: now,
                                confidence_score: confidence,
                                alpha_included: included as f64 + alpha,
                                beta_excluded: (total - included) as f64 + beta,
                            };
                            metrics.insert(vid, updated_metrics);
                        }
                    }
                    // clear dirty mark
                    updated.remove(&vid);
                }
            }
        });
    }

    // Robust fee preference using MAD and quantiles
    #[inline]
    fn robust_fee_preference(fees: &[f64]) -> FeePreference {
        if fees.is_empty() {
            return FeePreference {
                min_priority_fee: 1000,
                avg_accepted_fee: 5_000.0,
                fee_stdev: 1_000.0,
                high_fee_affinity: 0.5,
                p50: 5_000.0,
                p75: 6_500.0,
                p90: 9_000.0,
            };
        }

        // helper: nth-quantile via select_nth_unstable_by (O(n)), safe for f64
        let nth = |buf: &mut [f64], p: f64| -> f64 {
            if buf.is_empty() { return 0.0; }
            let len = buf.len();
            let idx = ((p * (len as f64 - 1.0)).round() as usize).min(len - 1);
            let (val, _, _) = buf.select_nth_unstable_by(idx, |a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            *val
        };

        // Median & MAD (robust scale)
        let median = {
            let mut tmp = fees.to_vec();
            nth(&mut tmp, 0.50)
        };
        let mut abs_dev: Vec<f64> = fees.iter().map(|&x| (x - median).abs()).collect();
        let mad = nth(&mut abs_dev, 0.50).max(MAD_EPS);
        let z = |x: f64| (x - median) / (1.4826 * mad);

        // filter outliers
        let mut clean: Vec<f64> = fees.iter().copied().filter(|&x| z(x).abs() <= OUTLIER_Z_THRESHOLD).collect();
        if clean.is_empty() { clean = fees.to_vec(); }

        // mean / stdev on cleaned
        let mean = clean.iter().sum::<f64>() / clean.len() as f64;
        let stdev = (clean.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / clean.len().max(1) as f64).sqrt();

        // quantiles on cleaned
        let p50 = { let mut b = clean.clone(); nth(&mut b, 0.50) };
        let p75 = { let mut b = clean.clone(); nth(&mut b, 0.75) };
        let p90 = { let mut b = clean.clone(); nth(&mut b, 0.90) };

        let high_thr = mean + stdev;
        let high_aff = clean.iter().filter(|&&x| x > high_thr).count() as f64 / clean.len().max(1) as f64;

        FeePreference {
            min_priority_fee: clean.iter().cloned().fold(f64::INFINITY, f64::min) as u64,
            avg_accepted_fee: mean,
            fee_stdev: stdev,
            high_fee_affinity: high_aff,
            p50, p75, p90,
        }
    }

    // keep existing ordering preference function

    fn calculate_ordering_preference(outcomes: &VecDeque<TransactionOutcome>) -> OrderingPreference {
        let mut frontrun_success = 0.0;
        let mut backrun_success = 0.0;
        let mut sandwich_success = 0.0;
        let mut bundle_total = 0.0;
        let mut bundle_accepted = 0.0;
        
        for outcome in outcomes {
            match outcome.transaction_type {
                TransactionType::Frontrun => {
                    if outcome.included {
                        if let Some(pos) = outcome.position_in_block {
                            if let Some(size) = outcome.block_size {
                                if pos < size / 3 {
                                    frontrun_success += 1.0;
                                }
                            }
                        }
                    }
                },
                TransactionType::Backrun => {
                    if outcome.included {
                        backrun_success += 1.0;
                    }
                },
                TransactionType::Sandwich => {
                    bundle_total += 1.0;
                    if outcome.included {
                        sandwich_success += 1.0;
                        bundle_accepted += 1.0;
                    }
                },
                _ => {}
            }
        }
        
        let total_mev = outcomes.iter()
            .filter(|o| matches!(o.transaction_type, 
                TransactionType::Arbitrage | TransactionType::Liquidation | 
                TransactionType::Sandwich | TransactionType::Frontrun | TransactionType::Backrun))
            .count() as f64;
        
                OrderingPreference {
            frontrun_tendency: if total_mev > 0.0 { frontrun_success / total_mev } else { 0.0 },
            backrun_tendency: if total_mev > 0.0 { backrun_success / total_mev } else { 0.0 },
            sandwich_success_rate: if bundle_total > 0.0 { sandwich_success / bundle_total } else { 0.0 },
            bundle_acceptance_rate: if bundle_total > 0.0 { bundle_accepted / bundle_total } else { 0.0 },
            ordering_consistency: Self::calculate_ordering_consistency(outcomes),
        }
    }

    fn calculate_ordering_consistency(outcomes: &VecDeque<TransactionOutcome>) -> f64 {
        let positions: Vec<f64> = outcomes.iter()
            .filter_map(|o| {
                if o.included {
                    o.position_in_block.and_then(|pos| {
                        o.block_size.map(|size| pos as f64 / size as f64)
                    })
                } else {
                    None
                }
            })
            .collect();
        
        if positions.len() < 2 {
            return 0.5;
        }
        
        let mean = positions.iter().sum::<f64>() / positions.len() as f64;
        let variance = positions.iter()
            .map(|p| (p - mean).powi(2))
            .sum::<f64>() / positions.len() as f64;
        
        1.0 / (1.0 + variance.sqrt())
    }

    fn calculate_temporal_patterns(outcomes: &VecDeque<TransactionOutcome>) -> TemporalPatterns {
        let mut hourly_activity = [0.0; 24];
        let mut hourly_counts = [0u32; 24];
        let mut weekly_activity = [0.0; 7];
        let mut weekly_counts = [0u32; 7];
        
        for o in outcomes {
            if !o.included { continue; }
            if let Some(ndt) = NaiveDateTime::from_timestamp_opt(o.timestamp as i64, 0) {
                let dt: DateTime<Utc> = DateTime::<Utc>::from_utc(ndt, Utc);
                let hour = dt.hour() as usize;
                let weekday = dt.weekday().num_days_from_monday() as usize;

                hourly_activity[hour] += 1.0;
                hourly_counts[hour] += 1;
                weekly_activity[weekday] += 1.0;
                weekly_counts[weekday] += 1;
            }
        }
        
        for i in 0..24 {
            if hourly_counts[i] > 0 {
                hourly_activity[i] /= hourly_counts[i] as f64;
            }
        }
        
        for i in 0..7 {
            if weekly_counts[i] > 0 {
                weekly_activity[i] /= weekly_counts[i] as f64;
            }
        }
        
        let avg_hourly = hourly_activity.iter().sum::<f64>() / 24.0;
        let threshold_high = avg_hourly * 1.2;
        let threshold_low = avg_hourly * 0.8;
        
        let peak_hours: Vec<u8> = (0..24)
            .filter(|&h| hourly_activity[h] > threshold_high)
            .map(|h| h as u8)
            .collect();
        
        let low_activity_hours: Vec<u8> = (0..24)
            .filter(|&h| hourly_activity[h] < threshold_low)
            .map(|h| h as u8)
            .collect();
        
        TemporalPatterns {
            hourly_activity,
            peak_hours,
            low_activity_hours,
            weekly_pattern: weekly_activity,
        }
    }

    fn calculate_reliability_score(outcomes: &VecDeque<TransactionOutcome>) -> f64 {
        if outcomes.is_empty() { return 0.0; }

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        // half-life 24h -> per-hour decay factor d = 0.5^(age_hours/hl)
        let hl = DECAY_HALFLIFE_HOURS.max(1.0);

        let mut num = 0.0;
        let mut den = 0.0;
        for (idx, o) in outcomes.iter().enumerate() {
            let age_hours = ((now - o.timestamp) as f64 / 3600.0).max(0.0);
            let time_w = (0.5f64).powf(age_hours / hl);
            // slight recency emphasis by index
            let recency_w = (idx as f64 + 1.0) / outcomes.len() as f64;
            let w = time_w * recency_w;

            let s = if o.included {
                // latency penalty: faster is better
                1.0 / (1.0 + (o.latency_ms as f64 / 50.0))
            } else { 0.0 };

            num += s * w;
            den += w;
        }
        if den > 0.0 { num / den } else { 0.0 }
    }

    fn calculate_confidence_score(sample_size: usize, reliability: f64) -> f64 {
        // Smooth growth with sample size; balanced by reliability
        let n = sample_size as f64;
        let size_term = 1.0 - (-n / 300.0).exp(); // ~63% by 300 samples
        let r = reliability.clamp(0.0, 1.0);
        (0.5 * size_term + 0.5 * r).clamp(0.0, 1.0)
    }

    fn spawn_decay_processor(&self) {
        let metrics = self.metrics.clone();
        
        tokio::spawn(async move {
            let mut decay_interval = interval(Duration::from_secs(300));
            loop {
                decay_interval.tick().await;

                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                let mut evict: Vec<Pubkey> = Vec::new();

                for mut entry in metrics.iter_mut() {
                    let v = entry.value_mut();
                    let age_hours = ((now - v.last_updated) as f64 / 3600.0).max(0.0);
                    let decay = (0.5f64).powf(age_hours / DECAY_HALFLIFE_HOURS.max(1.0));

                    v.reliability_score *= decay;
                    v.confidence_score *= decay;
                    v.mev_inclusion_rate *= decay;

                    if v.confidence_score < 0.10 || age_hours > 168.0 {
                        evict.push(*entry.key());
                    }
                }

                for k in evict { metrics.remove(&k); }

                if metrics.len() > MAX_VALIDATORS_TRACKED {
                    let mut scores: Vec<(Pubkey, f64)> =
                        metrics.iter().map(|e| (*e.key(), e.value().confidence_score)).collect();
                    scores.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                    let remove_count = metrics.len() - MAX_VALIDATORS_TRACKED;
                    for (k, _) in scores.into_iter().take(remove_count) {
                        metrics.remove(&k);
                    }
                }
            }
        });
    }

    fn spawn_persistence_task(&self) {
        let metrics = self.metrics.clone();
        let path = self.persistence_path.clone();
        
        tokio::spawn(async move {
            let mut persist_interval = interval(Duration::from_secs(60));
            
            loop {
                persist_interval.tick().await;
                
                let snapshot: Vec<ValidatorMetrics> = metrics.iter()
                    .map(|e| e.value().clone())
                    .collect();
                
                if let Ok(encoded) = bincode::serialize(&snapshot) {
                    let tmp = format!("{}.tmp", path);
                    if tokio::fs::write(&tmp, &encoded).await.is_ok() {
                        let _ = tokio::fs::rename(&tmp, &path).await;
                    }
                }
            }
        });
    }

    fn load_state(&self) {
        if let Ok(data) = std::fs::read(&self.persistence_path) {
            if let Ok(mut snapshot): Result<Vec<ValidatorMetrics>, _> = bincode::deserialize(&data) {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                for mut m in snapshot.drain(..) {
                    let age_hours = ((now - m.last_updated) as f64 / 3600.0).max(0.0);
                    if age_hours < 168.0 {
                        let decay = (0.5f64).powf(age_hours / DECAY_HALFLIFE_HOURS.max(1.0));
                        m.reliability_score *= decay;
                        m.confidence_score *= decay;
                        self.metrics.insert(m.pubkey, m);
                    }
                }
            }
        }
    }

    fn cleanup_old_slots(slot_performance: &Arc<DMap<Slot, SlotMetrics>>, current_slot: Slot) {
        if current_slot > SLOT_HISTORY_SIZE as u64 {
            let cutoff = current_slot - SLOT_HISTORY_SIZE as u64;
            slot_performance.retain(|s, _| *s > cutoff);
        }
    }

    pub fn get_validator_statistics(&self) -> HashMap<Pubkey, ValidatorStats> {
        self.metrics.iter()
            .map(|entry| {
                let metrics = entry.value();
                let stats = ValidatorStats {
                    total_slots: metrics.total_slots_processed,
                    inclusion_rate: metrics.mev_inclusion_rate,
                    avg_latency: metrics.avg_latency_ms,
                    reliability: metrics.reliability_score,
                    confidence: metrics.confidence_score,
                    last_seen: metrics.last_updated,
                };
                (entry.key().clone(), stats)
            })
            .collect()
    }

    pub fn should_use_validator(&self, validator: &Pubkey, min_confidence: f64) -> bool {
        self.metrics.get(validator)
            .map(|m| m.confidence_score >= min_confidence && m.reliability_score >= 0.5)
            .unwrap_or(false)
    }

    pub fn get_validator_risk_score(&self, validator: &Pubkey) -> f64 {
        self.metrics.get(validator)
            .map(|m| {
                let inclusion_risk = 1.0 - m.mev_inclusion_rate;
                let latency_risk = m.avg_latency_ms / 100.0;
                let confidence_risk = 1.0 - m.confidence_score;
                (inclusion_risk + latency_risk + confidence_risk) / 3.0
            })
            .unwrap_or(1.0)
    }

    pub fn predict_inclusion_probability(&self, validator: &Pubkey, tx_type: TransactionType, priority_fee: u64) -> f64 {
        self.metrics.get(validator).map(|m| {
            let base = m.mev_inclusion_rate.clamp(0.001, 0.999);
            let logit0 = safe_logit(base);

            // odds multipliers enter log-odds additively
            let type_mult = match tx_type {
                TransactionType::Liquidation => 1.20,
                TransactionType::Arbitrage  => 1.10,
                TransactionType::Sandwich   => (m.ordering_preference.sandwich_success_rate / 0.5).clamp(0.5, 2.0),
                TransactionType::Frontrun   => (m.ordering_preference.frontrun_tendency * 2.0).clamp(0.5, 2.0),
                TransactionType::Backrun    => (m.ordering_preference.backrun_tendency  * 2.0).clamp(0.5, 2.0),
                TransactionType::Standard   => 1.0,
            };
            let lat_mult = 1.0 / (1.0 + m.ewma_latency_ms / LAT_MS_SOFT_SELECTION);
            let conf_mult = m.confidence_score.clamp(0.25, 1.0);

            // fee pressure: sigmoid around p75 with stdev scale → map to [-K, +K] bump in logit
            let f = &m.fee_preference;
            let scale = f.fee_stdev.max(1.0);
            let x = (priority_fee as f64 - f.p75) / scale;
            let fee_sig = 1.0 / (1.0 + (-x).exp()); // (0,1)
            let k_fee = 0.75; // strength of fee leverage in log-odds
            let fee_bump = k_fee * (2.0 * fee_sig - 1.0); // [-k, +k]

            let logit = logit0 + safe_ln(type_mult) + safe_ln(lat_mult) + safe_ln(conf_mult) + fee_bump;
            (1.0 / (1.0 + (-logit).exp())).clamp(0.0, 1.0)
        }).unwrap_or(0.0)
    }
}

#[derive(Debug, Clone)]
pub struct ValidatorStats {
    pub total_slots: u64,
    pub inclusion_rate: f64,
    pub avg_latency: f64,
    pub reliability: f64,
    pub confidence: f64,
    pub last_seen: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_validator_preference_learning() {
        let learner = ValidatorPreferenceLearner::new("/tmp/test_validator_prefs.bin".to_string());
        let v = Pubkey::new_unique();

        for i in 0..100usize {
            let o = TransactionOutcome {
                validator: v,
                slot: 1000 + i as u64,
                included: i % 10 != 0,
                latency_ms: 20 + (i % 5) as u64 * 10,
                priority_fee: 1000 + (i % 20) as u64 * 500,
                transaction_type: TransactionType::Arbitrage,
                position_in_block: Some((i % 50) as usize),
                block_size: Some(100),
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            };
            learner.record_transaction_outcome(o).await.unwrap();
        }

        // allow at least one coalesced recompute
        tokio::time::sleep(PREFERENCE_UPDATE_INTERVAL * 2).await;

        let m = learner.get_validator_preference(&v).expect("metrics");
        assert!(m.mev_inclusion_rate > 0.80);
        assert!(m.fee_preference.p50 > 0.0);
    }
}

