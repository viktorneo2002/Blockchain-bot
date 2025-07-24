use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::clock::Slot;
use tokio::sync::mpsc::{channel, Sender, Receiver};
use tokio::time::interval;
use serde::{Deserialize, Serialize};
use bincode;
use dashmap::DashMap;

const PREFERENCE_WINDOW_SIZE: usize = 10000;
const DECAY_FACTOR: f64 = 0.995;
const MIN_SAMPLE_SIZE: usize = 50;
const PREFERENCE_UPDATE_INTERVAL: Duration = Duration::from_secs(5);
const MAX_VALIDATORS_TRACKED: usize = 500;
const OUTLIER_THRESHOLD: f64 = 3.0;
const CONFIDENCE_THRESHOLD: f64 = 0.75;
const SLOT_HISTORY_SIZE: usize = 432000; // ~2 days worth of slots

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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeePreference {
    pub min_priority_fee: u64,
    pub avg_accepted_fee: f64,
    pub fee_variance: f64,
    pub high_fee_affinity: f64,
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
    metrics: Arc<DashMap<Pubkey, ValidatorMetrics>>,
    transaction_history: Arc<DashMap<Pubkey, VecDeque<TransactionOutcome>>>,
    slot_performance: Arc<DashMap<Slot, SlotMetrics>>,
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
        let (tx, rx) = channel(10000);
        let metrics = Arc::new(DashMap::new());
        let transaction_history = Arc::new(DashMap::new());
        let slot_performance = Arc::new(DashMap::new());
        
        let learner = Self {
            metrics: metrics.clone(),
            transaction_history: transaction_history.clone(),
            slot_performance: slot_performance.clone(),
            update_sender: tx,
            persistence_path: persistence_path.clone(),
        };

        learner.load_state();
        learner.spawn_update_processor(rx);
        learner.spawn_decay_processor();
        learner.spawn_persistence_task();
        
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
        let mut validators: Vec<(Pubkey, f64)> = self.metrics
            .iter()
            .filter_map(|entry| {
                let metrics = entry.value();
                if metrics.confidence_score < CONFIDENCE_THRESHOLD {
                    return None;
                }
                
                let score = match tx_type {
                    TransactionType::Arbitrage => {
                        metrics.mev_inclusion_rate * metrics.reliability_score
                            * (1.0 / (1.0 + metrics.avg_latency_ms / 100.0))
                    },
                    TransactionType::Liquidation => {
                        metrics.mev_inclusion_rate * metrics.reliability_score * 1.2
                    },
                    TransactionType::Sandwich => {
                        metrics.ordering_preference.sandwich_success_rate * metrics.reliability_score
                    },
                    TransactionType::Frontrun => {
                        metrics.ordering_preference.frontrun_tendency * metrics.reliability_score
                    },
                    TransactionType::Backrun => {
                        metrics.ordering_preference.backrun_tendency * metrics.reliability_score
                    },
                    TransactionType::Standard => metrics.reliability_score,
                };
                
                Some((entry.key().clone(), score))
            })
            .collect();
        
        validators.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        validators.truncate(limit);
        validators
    }

    pub fn get_optimal_fee_for_validator(&self, validator: &Pubkey, urgency: f64) -> u64 {
        self.metrics.get(validator).map(|metrics| {
            let base_fee = metrics.fee_preference.avg_accepted_fee;
            let variance = metrics.fee_preference.fee_variance;
            let adjustment = variance * urgency.min(2.0);
            ((base_fee + adjustment) * 1.1) as u64
        }).unwrap_or(5000)
    }

    pub fn get_best_submission_time(&self, validator: &Pubkey) -> Option<u8> {
        self.metrics.get(validator).and_then(|metrics| {
            metrics.temporal_patterns.peak_hours.first().copied()
        })
    }

    fn spawn_update_processor(&self, mut rx: Receiver<PreferenceUpdate>) {
        let metrics = self.metrics.clone();
        let transaction_history = self.transaction_history.clone();
        let slot_performance = self.slot_performance.clone();
        
        tokio::spawn(async move {
            while let Some(update) = rx.recv().await {
                match update {
                    PreferenceUpdate::TransactionOutcome(outcome) => {
                        Self::process_transaction_outcome(
                            &metrics,
                            &transaction_history,
                            outcome
                        );
                    },
                    PreferenceUpdate::SlotComplete(slot, slot_metrics) => {
                        slot_performance.insert(slot, slot_metrics);
                        Self::cleanup_old_slots(&slot_performance, slot);
                    },
                    PreferenceUpdate::PersistState => {
                        // Handled by persistence task
                    }
                }
            }
        });
    }

    fn process_transaction_outcome(
        metrics: &Arc<DashMap<Pubkey, ValidatorMetrics>>,
        history: &Arc<DashMap<Pubkey, VecDeque<TransactionOutcome>>>,
        outcome: TransactionOutcome,
    ) {
        let mut entry = history.entry(outcome.validator).or_insert_with(VecDeque::new);
        if entry.len() >= PREFERENCE_WINDOW_SIZE {
            entry.pop_front();
        }
        entry.push_back(outcome.clone());
        drop(entry);
        
        Self::update_validator_metrics(metrics, history, &outcome.validator);
    }

    fn update_validator_metrics(
        metrics: &Arc<DashMap<Pubkey, ValidatorMetrics>>,
        history: &Arc<DashMap<Pubkey, VecDeque<TransactionOutcome>>>,
        validator: &Pubkey,
    ) {
        let Some(outcomes) = history.get(validator) else { return };
        let outcomes = outcomes.clone();
        drop(outcomes);
        
        if outcomes.len() < MIN_SAMPLE_SIZE {
            return;
        }
        
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let total_txs = outcomes.len() as f64;
        let included_txs = outcomes.iter().filter(|o| o.included).count() as f64;
        let mev_inclusion_rate = included_txs / total_txs;
        
        let avg_latency = outcomes.iter()
            .filter(|o| o.included)
            .map(|o| o.latency_ms as f64)
            .sum::<f64>() / included_txs.max(1.0);
        
        let fee_preference = Self::calculate_fee_preference(&outcomes);
        let ordering_preference = Self::calculate_ordering_preference(&outcomes);
        let temporal_patterns = Self::calculate_temporal_patterns(&outcomes);
        let reliability_score = Self::calculate_reliability_score(&outcomes);
        let confidence_score = Self::calculate_confidence_score(outcomes.len(), reliability_score);
        
        let updated_metrics = ValidatorMetrics {
            pubkey: *validator,
            total_slots_processed: outcomes.iter().map(|o| o.slot).collect::<std::collections::HashSet<_>>().len() as u64,
            mev_inclusion_rate,
            avg_latency_ms: avg_latency,
            fee_preference,
            ordering_preference,
            temporal_patterns,
            reliability_score,
            last_updated: now,
            confidence_score,
        };
        
        metrics.insert(*validator, updated_metrics);
    }

    fn calculate_fee_preference(outcomes: &VecDeque<TransactionOutcome>) -> FeePreference {
        let fees: Vec<f64> = outcomes.iter()
            .filter(|o| o.included)
            .map(|o| o.priority_fee as f64)
            .collect();
        
        if fees.is_empty() {
            return FeePreference {
                min_priority_fee: 1000,
                avg_accepted_fee: 5000.0,
                fee_variance: 1000.0,
                high_fee_affinity: 0.5,
            };
        }
        
        let min_fee = fees.iter().cloned().fold(f64::INFINITY, f64::min) as u64;
        let avg_fee = fees.iter().sum::<f64>() / fees.len() as f64;
        let variance = fees.iter()
            .map(|f| (f - avg_fee).powi(2))
            .sum::<f64>() / fees.len() as f64;
        
        let high_fee_threshold = avg_fee + variance.sqrt();
        let high_fee_affinity = fees.iter()
            .filter(|&&f| f > high_fee_threshold)
            .count() as f64 / fees.len() as f64;
        
        FeePreference {
            min_priority_fee: min_fee,
            avg_accepted_fee: avg_fee,
            fee_variance: variance.sqrt(),
            high_fee_affinity,
        }
    }

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
        
        for outcome in outcomes {
            if outcome.included {
                let datetime = chrono::DateTime::<chrono::Utc>::from_timestamp(outcome.timestamp as i64, 0)
                    .unwrap_or_else(chrono::Utc::now);
                let hour = datetime.hour() as usize;
                let weekday = datetime.weekday().num_days_from_monday() as usize;
                
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
        if outcomes.is_empty() {
            return 0.0;
        }
        
        let mut time_weighted_score = 0.0;
        let mut weight_sum = 0.0;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        
        for (idx, outcome) in outcomes.iter().enumerate() {
            let age_hours = (now - outcome.timestamp) as f64 / 3600.0;
            let time_weight = (-age_hours / 24.0).exp();
            let recency_weight = (idx as f64 + 1.0) / outcomes.len() as f64;
            let combined_weight = time_weight * recency_weight;
            
            let score = if outcome.included {
                let latency_factor = 1.0 / (1.0 + outcome.latency_ms as f64 / 50.0);
                latency_factor
            } else {
                0.0
            };
            
            time_weighted_score += score * combined_weight;
            weight_sum += combined_weight;
        }
        
        if weight_sum > 0.0 {
            time_weighted_score / weight_sum
        } else {
            0.0
        }
    }

    fn calculate_confidence_score(sample_size: usize, reliability: f64) -> f64 {
        let size_factor = (sample_size as f64 / MIN_SAMPLE_SIZE as f64).min(2.0);
        let reliability_factor = reliability.powf(0.5);
        (size_factor * reliability_factor / 2.0).min(1.0)
    }

    fn spawn_decay_processor(&self) {
        let metrics = self.metrics.clone();
        
        tokio::spawn(async move {
            let mut decay_interval = interval(Duration::from_secs(300));
            
            loop {
                decay_interval.tick().await;
                
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                let mut to_remove = Vec::new();
                
                for mut entry in metrics.iter_mut() {
                    let age_hours = (now - entry.last_updated) as f64 / 3600.0;
                    let decay = DECAY_FACTOR.powf(age_hours);
                    
                    entry.reliability_score *= decay;
                    entry.confidence_score *= decay;
                    entry.mev_inclusion_rate *= decay;
                    
                    if entry.confidence_score < 0.1 || age_hours > 168.0 {
                        to_remove.push(entry.key().clone());
                    }
                }
                
                for key in to_remove {
                    metrics.remove(&key);
                }
                
                if metrics.len() > MAX_VALIDATORS_TRACKED {
                    let mut scores: Vec<(Pubkey, f64)> = metrics.iter()
                        .map(|e| (e.key().clone(), e.value().confidence_score))
                        .collect();
                    scores.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
                    
                    let remove_count = metrics.len() - MAX_VALIDATORS_TRACKED;
                    for (key, _) in scores.into_iter().take(remove_count) {
                        metrics.remove(&key);
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
                    let temp_path = format!("{}.tmp", path);
                    if tokio::fs::write(&temp_path, &encoded).await.is_ok() {
                        let _ = tokio::fs::rename(&temp_path, &path).await;
                    }
                }
            }
        });
    }

    fn load_state(&self) {
        if let Ok(data) = std::fs::read(&self.persistence_path) {
            if let Ok(snapshot): Result<Vec<ValidatorMetrics>, _> = bincode::deserialize(&data) {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                
                for mut metric in snapshot {
                    let age_hours = (now - metric.last_updated) as f64 / 3600.0;
                    if age_hours < 168.0 {
                        let decay = DECAY_FACTOR.powf(age_hours);
                        metric.reliability_score *= decay;
                        metric.confidence_score *= decay;
                        self.metrics.insert(metric.pubkey, metric);
                    }
                }
            }
        }
    }

    fn cleanup_old_slots(slot_performance: &Arc<DashMap<Slot, SlotMetrics>>, current_slot: Slot) {
        if current_slot > SLOT_HISTORY_SIZE as u64 {
            let cutoff_slot = current_slot - SLOT_HISTORY_SIZE as u64;
            slot_performance.retain(|slot, _| *slot > cutoff_slot);
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
        self.metrics.get(validator)
            .map(|m| {
                let base_prob = m.mev_inclusion_rate;
                let fee_factor = if priority_fee >= m.fee_preference.avg_accepted_fee as u64 {
                    1.0 + (priority_fee as f64 / m.fee_preference.avg_accepted_fee - 1.0).min(0.5)
                } else {
                    priority_fee as f64 / m.fee_preference.avg_accepted_fee
                };
                
                let type_factor = match tx_type {
                    TransactionType::Liquidation => 1.2,
                    TransactionType::Arbitrage => 1.1,
                    TransactionType::Sandwich => m.ordering_preference.sandwich_success_rate / 0.5,
                    TransactionType::Frontrun => m.ordering_preference.frontrun_tendency * 2.0,
                    TransactionType::Backrun => m.ordering_preference.backrun_tendency * 2.0,
                    TransactionType::Standard => 1.0,
                };
                
                (base_prob * fee_factor * type_factor * m.confidence_score).min(1.0)
            })
            .unwrap_or(0.0)
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
        let validator = Pubkey::new_unique();
        
        for i in 0..100 {
            let outcome = TransactionOutcome {
                validator,
                slot: 1000 + i,
                included: i % 10 != 0,
                latency_ms: 20 + (i % 5) * 10,
                priority_fee: 1000 + (i % 20) * 500,
                transaction_type: TransactionType::Arbitrage,
                position_in_block: Some((i % 50) as usize),
                block_size: Some(100),
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            };
            
            learner.record_transaction_outcome(outcome).await.unwrap();
        }
        
        tokio::time::sleep(Duration::from_secs(1)).await;
        
        let metrics = learner.get_validator_preference(&validator);
        assert!(metrics.is_some());
        assert!(metrics.unwrap().mev_inclusion_rate > 0.8);
    }
}

