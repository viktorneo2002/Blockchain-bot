use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    clock::Slot,
    commitment_config::CommitmentLevel,
};
use tokio::sync::RwLock as TokioRwLock;
use statistical::{mean, standard_deviation};

const SLOT_DURATION_MS: u64 = 400;
const MAX_SAMPLES_PER_COMPETITOR: usize = 1000;
const LATENCY_DECAY_FACTOR: f64 = 0.95;
const MIN_SAMPLES_FOR_ESTIMATION: usize = 5;
const OUTLIER_THRESHOLD_MULTIPLIER: f64 = 3.0;
const CACHE_TTL_SECONDS: u64 = 300;
const MAX_COMPETITORS_TRACKED: usize = 500;
const SLOT_HISTORY_DEPTH: usize = 64;
const CONFIRMATION_TIMEOUT_MS: u64 = 5000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencySample {
    pub timestamp: u64,
    pub slot: Slot,
    pub latency_ms: u64,
    pub commitment_level: CommitmentLevel,
    pub tx_signature: Signature,
    pub bundle_size: usize,
    pub priority_fee: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompetitorProfile {
    pub pubkey: Pubkey,
    pub samples: VecDeque<LatencySample>,
    pub avg_latency_ms: f64,
    pub std_dev_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub success_rate: f64,
    pub last_seen_slot: Slot,
    pub total_transactions: u64,
    pub successful_transactions: u64,
    pub weighted_avg_latency_ms: f64,
}

#[derive(Debug, Clone)]
pub struct SlotTiming {
    pub slot: Slot,
    pub start_time: Instant,
    pub leader: Pubkey,
    pub estimated_end_time: Instant,
}

#[derive(Debug, Clone)]
pub struct PendingTransaction {
    pub signature: Signature,
    pub sender: Pubkey,
    pub submission_time: Instant,
    pub slot_submitted: Slot,
    pub priority_fee: u64,
    pub bundle_size: usize,
}

pub struct CompetitorLatencyEstimator {
    competitors: Arc<DashMap<Pubkey, CompetitorProfile>>,
    pending_txs: Arc<DashMap<Signature, PendingTransaction>>,
    slot_timings: Arc<TokioRwLock<VecDeque<SlotTiming>>>,
    current_slot: Arc<RwLock<Slot>>,
    cluster_avg_latency: Arc<RwLock<f64>>,
    network_congestion_factor: Arc<RwLock<f64>>,
}

impl CompetitorLatencyEstimator {
    pub fn new() -> Self {
        Self {
            competitors: Arc::new(DashMap::new()),
            pending_txs: Arc::new(DashMap::new()),
            slot_timings: Arc::new(TokioRwLock::new(VecDeque::with_capacity(SLOT_HISTORY_DEPTH))),
            current_slot: Arc::new(RwLock::new(0)),
            cluster_avg_latency: Arc::new(RwLock::new(50.0)),
            network_congestion_factor: Arc::new(RwLock::new(1.0)),
        }
    }

    pub async fn update_slot_timing(&self, slot: Slot, leader: Pubkey) {
        let mut timings = self.slot_timings.write().await;
        let now = Instant::now();
        
        timings.push_back(SlotTiming {
            slot,
            start_time: now,
            leader,
            estimated_end_time: now + Duration::from_millis(SLOT_DURATION_MS),
        });

        if timings.len() > SLOT_HISTORY_DEPTH {
            timings.pop_front();
        }

        *self.current_slot.write().unwrap() = slot;
        self.cleanup_stale_pending_txs(slot).await;
    }

    pub async fn record_transaction_submission(
        &self,
        signature: Signature,
        sender: Pubkey,
        priority_fee: u64,
        bundle_size: usize,
    ) {
        let current_slot = *self.current_slot.read().unwrap();
        
        self.pending_txs.insert(
            signature,
            PendingTransaction {
                signature,
                sender,
                submission_time: Instant::now(),
                slot_submitted: current_slot,
                priority_fee,
                bundle_size,
            },
        );
    }

    pub async fn record_transaction_confirmation(
        &self,
        signature: Signature,
        confirmation_slot: Slot,
        commitment_level: CommitmentLevel,
    ) -> Option<u64> {
        if let Some((_, pending_tx)) = self.pending_txs.remove(&signature) {
            let latency_ms = pending_tx.submission_time.elapsed().as_millis() as u64;
            
            if latency_ms > CONFIRMATION_TIMEOUT_MS {
                return None;
            }

            let sample = LatencySample {
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64,
                slot: confirmation_slot,
                latency_ms,
                commitment_level,
                tx_signature: signature,
                bundle_size: pending_tx.bundle_size,
                priority_fee: pending_tx.priority_fee,
            };

            self.update_competitor_profile(pending_tx.sender, sample).await;
            self.update_cluster_metrics(latency_ms).await;
            
            Some(latency_ms)
        } else {
            None
        }
    }

    async fn update_competitor_profile(&self, competitor: Pubkey, sample: LatencySample) {
        self.competitors
            .entry(competitor)
            .and_modify(|profile| {
                profile.samples.push_back(sample.clone());
                
                if profile.samples.len() > MAX_SAMPLES_PER_COMPETITOR {
                    profile.samples.pop_front();
                }

                profile.total_transactions += 1;
                profile.successful_transactions += 1;
                profile.last_seen_slot = sample.slot;

                self.recalculate_statistics(profile);
            })
            .or_insert_with(|| {
                let mut profile = CompetitorProfile {
                    pubkey: competitor,
                    samples: VecDeque::new(),
                    avg_latency_ms: sample.latency_ms as f64,
                    std_dev_ms: 0.0,
                    p95_latency_ms: sample.latency_ms as f64,
                    p99_latency_ms: sample.latency_ms as f64,
                    success_rate: 1.0,
                    last_seen_slot: sample.slot,
                    total_transactions: 1,
                    successful_transactions: 1,
                    weighted_avg_latency_ms: sample.latency_ms as f64,
                };
                profile.samples.push_back(sample);
                profile
            });

        self.enforce_competitor_limit().await;
    }

    fn recalculate_statistics(&self, profile: &mut CompetitorProfile) {
        if profile.samples.len() < MIN_SAMPLES_FOR_ESTIMATION {
            return;
        }

        let mut latencies: Vec<f64> = profile.samples
            .iter()
            .map(|s| s.latency_ms as f64)
            .collect();

        let mean_latency = mean(&latencies);
        let std_dev = standard_deviation(&latencies, Some(mean_latency));

        let outlier_threshold = mean_latency + (OUTLIER_THRESHOLD_MULTIPLIER * std_dev);
        latencies.retain(|&l| l <= outlier_threshold);

        if !latencies.is_empty() {
            latencies.sort_by(|a, b| a.partial_cmp(b).unwrap());
            
            profile.avg_latency_ms = mean(&latencies);
            profile.std_dev_ms = standard_deviation(&latencies, Some(profile.avg_latency_ms));
            
            let p95_idx = ((latencies.len() as f64 * 0.95) as usize).min(latencies.len() - 1);
            let p99_idx = ((latencies.len() as f64 * 0.99) as usize).min(latencies.len() - 1);
            
            profile.p95_latency_ms = latencies[p95_idx];
            profile.p99_latency_ms = latencies[p99_idx];

            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let mut weighted_sum = 0.0;
            let mut weight_total = 0.0;

            for sample in profile.samples.iter().rev().take(50) {
                let age_ms = now.saturating_sub(sample.timestamp) as f64;
                let time_weight = (-age_ms / 60000.0).exp();
                let priority_weight = (sample.priority_fee as f64 / 1_000_000.0).sqrt();
                let weight = time_weight * (1.0 + priority_weight * 0.1);
                
                weighted_sum += sample.latency_ms as f64 * weight;
                weight_total += weight;
            }

            if weight_total > 0.0 {
                profile.weighted_avg_latency_ms = weighted_sum / weight_total;
            }

            profile.success_rate = profile.successful_transactions as f64 / profile.total_transactions as f64;
        }
    }

    async fn update_cluster_metrics(&self, latency_ms: u64) {
        let mut cluster_avg = self.cluster_avg_latency.write().unwrap();
        *cluster_avg = (*cluster_avg * 0.99) + (latency_ms as f64 * 0.01);

        let congestion_factor = (latency_ms as f64 / 50.0).min(3.0).max(0.5);
        let mut network_congestion = self.network_congestion_factor.write().unwrap();
        *network_congestion = (*network_congestion * 0.95) + (congestion_factor * 0.05);
    }

    pub async fn estimate_competitor_latency(&self, competitor: Pubkey) -> Option<LatencyEstimate> {
        self.competitors.get(&competitor).map(|entry| {
            let profile = entry.value();
            
            if profile.samples.len() < MIN_SAMPLES_FOR_ESTIMATION {
                return LatencyEstimate {
                    expected_ms: *self.cluster_avg_latency.read().unwrap(),
                    confidence: 0.1,
                    p95_ms: *self.cluster_avg_latency.read().unwrap() * 2.0,
                    p99_ms: *self.cluster_avg_latency.read().unwrap() * 3.0,
                    slot_adjusted_ms: *self.cluster_avg_latency.read().unwrap(),
                };
            }

            let network_factor = *self.network_congestion_factor.read().unwrap();
            let slot_position_factor = self.calculate_slot_position_factor().unwrap_or(1.0);
            
            let base_latency = profile.weighted_avg_latency_ms;
            let adjusted_latency = base_latency * network_factor * slot_position_factor;

            let confidence = calculate_confidence(
                profile.samples.len(),
                profile.std_dev_ms,
                profile.success_rate,
            );

            LatencyEstimate {
                expected_ms: adjusted_latency,
                confidence,
                p95_ms: profile.p95_latency_ms * network_factor,
                p99_ms: profile.p99_latency_ms * network_factor,
                slot_adjusted_ms: adjusted_latency,
            }
        })
    }

    pub async fn get_top_competitors(&self, limit: usize) -> Vec<CompetitorProfile> {
        let mut competitors: Vec<_> = self.competitors
            .iter()
            .map(|entry| entry.value().clone())
            .collect();

        competitors.sort_by(|a, b| {
            let a_score = calculate_competitor_score(a);
            let b_score = calculate_competitor_score(b);
            b_score.partial_cmp(&a_score).unwrap()
        });

        competitors.into_iter().take(limit).collect()
    }

    pub async fn predict_competitor_action_window(
        &self,
        competitor: Pubkey,
        target_slot: Slot,
    ) -> Option<ActionWindow> {
        let profile = self.competitors.get(&competitor)?;
        let current_slot = *self.current_slot.read().unwrap();
        
        if target_slot <= current_slot {
            return None;
        }

        let slot_distance = target_slot - current_slot;
        let time_to_slot_ms = slot_distance as u64 * SLOT_DURATION_MS;
        
        let latency_est = self.estimate_competitor_latency(competitor).await?;
        
        let earliest_action = time_to_slot_ms.saturating_sub(latency_est.p99_ms as u64);
        let likely_action = time_to_slot_ms.saturating_sub(latency_est.expected_ms as u64);
        let latest_action = time_to_slot_ms.saturating_sub(latency_est.expected_ms as u64 / 2);

        Some(ActionWindow {
            earliest_ms: earliest_action,
            likely_ms: likely_action,
            latest_ms: latest_action,
            confidence: latency_est.confidence,
        })
    }

    fn calculate_slot_position_factor(&self) -> Option<f64> {
        let current_slot = *self.current_slot.read().unwrap();
        let slot_progress = (current_slot % 4) as f64 / 4.0;
        
        Some(0.8 + (slot_progress * 0.4))
    }

        async fn cleanup_stale_pending_txs(&self, current_slot: Slot) {
        let stale_threshold = current_slot.saturating_sub(32);
        let mut stale_sigs = Vec::new();
        
        for entry in self.pending_txs.iter() {
            if entry.value().slot_submitted < stale_threshold {
                stale_sigs.push(*entry.key());
            }
        }
        
        for sig in stale_sigs {
            if let Some((_, pending_tx)) = self.pending_txs.remove(&sig) {
                self.competitors
                    .entry(pending_tx.sender)
                    .and_modify(|profile| {
                        profile.total_transactions += 1;
                        profile.success_rate = profile.successful_transactions as f64 
                            / profile.total_transactions as f64;
                    });
            }
        }
    }

    async fn enforce_competitor_limit(&self) {
        if self.competitors.len() > MAX_COMPETITORS_TRACKED {
            let current_slot = *self.current_slot.read().unwrap();
            let mut removal_candidates: Vec<(Pubkey, u64)> = self.competitors
                .iter()
                .map(|entry| {
                    let profile = entry.value();
                    let age = current_slot.saturating_sub(profile.last_seen_slot);
                    (*entry.key(), age)
                })
                .collect();
            
            removal_candidates.sort_by_key(|&(_, age)| std::cmp::Reverse(age));
            
            for (pubkey, _) in removal_candidates.iter().take(50) {
                self.competitors.remove(pubkey);
            }
        }
    }

    pub async fn get_network_congestion_estimate(&self) -> NetworkCongestionEstimate {
        let congestion_factor = *self.network_congestion_factor.read().unwrap();
        let cluster_avg = *self.cluster_avg_latency.read().unwrap();
        
        let level = match congestion_factor {
            f if f < 0.8 => CongestionLevel::Low,
            f if f < 1.2 => CongestionLevel::Normal,
            f if f < 1.8 => CongestionLevel::High,
            _ => CongestionLevel::Critical,
        };
        
        NetworkCongestionEstimate {
            level,
            factor: congestion_factor,
            avg_latency_ms: cluster_avg,
            recommended_priority_multiplier: calculate_priority_multiplier(congestion_factor),
        }
    }

    pub async fn estimate_bundle_latency(
        &self,
        competitor: Pubkey,
        bundle_size: usize,
        priority_fee: u64,
    ) -> Option<BundleLatencyEstimate> {
        let base_estimate = self.estimate_competitor_latency(competitor).await?;
        let profile = self.competitors.get(&competitor)?;
        
        let size_factor = (1.0 + (bundle_size as f64 * 0.05)).min(2.0);
        let priority_factor = (1.0 - (priority_fee as f64 / 10_000_000_000.0).min(0.3)).max(0.7);
        
        let similar_bundles: Vec<&LatencySample> = profile.samples
            .iter()
            .filter(|s| {
                let size_diff = (s.bundle_size as i32 - bundle_size as i32).abs();
                let fee_ratio = s.priority_fee as f64 / priority_fee.max(1) as f64;
                size_diff <= 2 && fee_ratio > 0.5 && fee_ratio < 2.0
            })
            .collect();
        
        let adjusted_latency = if similar_bundles.len() >= 3 {
            let bundle_avg = similar_bundles
                .iter()
                .map(|s| s.latency_ms as f64)
                .sum::<f64>() / similar_bundles.len() as f64;
            bundle_avg * priority_factor
        } else {
            base_estimate.expected_ms * size_factor * priority_factor
        };
        
        Some(BundleLatencyEstimate {
            expected_ms: adjusted_latency,
            confidence: base_estimate.confidence * if similar_bundles.len() >= 3 { 1.2 } else { 0.8 },
            size_impact_factor: size_factor,
            priority_impact_factor: priority_factor,
            sample_count: similar_bundles.len(),
        })
    }

    pub async fn calculate_optimal_submission_timing(
        &self,
        target_slot: Slot,
        competitors: &[Pubkey],
    ) -> OptimalTimingRecommendation {
        let current_slot = *self.current_slot.read().unwrap();
        let congestion = self.get_network_congestion_estimate().await;
        
        let mut competitor_windows = Vec::new();
        for competitor in competitors {
            if let Some(window) = self.predict_competitor_action_window(*competitor, target_slot).await {
                competitor_windows.push(window);
            }
        }
        
        competitor_windows.sort_by_key(|w| w.likely_ms as i64);
        
        let slots_until_target = target_slot.saturating_sub(current_slot);
        let time_until_slot_ms = slots_until_target as u64 * SLOT_DURATION_MS;
        
        let (submit_at_ms, strategy) = if competitor_windows.is_empty() {
            let base_timing = time_until_slot_ms.saturating_sub(80);
            (base_timing, SubmissionStrategy::Default)
        } else if competitor_windows.len() == 1 {
            let window = &competitor_windows[0];
            let timing = window.likely_ms.saturating_sub(20);
            (timing, SubmissionStrategy::FrontRun)
        } else {
            let earliest = competitor_windows.first().unwrap();
            let timing = earliest.likely_ms.saturating_sub(15);
            (timing, SubmissionStrategy::Competitive)
        };
        
        let priority_fee_recommendation = calculate_optimal_priority_fee(
            &competitor_windows,
            congestion.factor,
        );
        
        OptimalTimingRecommendation {
            submit_at_ms,
            confidence: calculate_timing_confidence(&competitor_windows),
            strategy,
            priority_fee_gwei: priority_fee_recommendation,
            fallback_timing_ms: time_until_slot_ms.saturating_sub(150),
        }
    }

    pub fn get_competitor_stats(&self, competitor: &Pubkey) -> Option<CompetitorStats> {
        self.competitors.get(competitor).map(|entry| {
            let profile = entry.value();
            CompetitorStats {
                total_txs: profile.total_transactions,
                success_rate: profile.success_rate,
                avg_latency_ms: profile.avg_latency_ms,
                std_dev_ms: profile.std_dev_ms,
                last_seen_slot: profile.last_seen_slot,
                sample_count: profile.samples.len(),
            }
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyEstimate {
    pub expected_ms: f64,
    pub confidence: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub slot_adjusted_ms: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionWindow {
    pub earliest_ms: u64,
    pub likely_ms: u64,
    pub latest_ms: u64,
    pub confidence: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CongestionLevel {
    Low,
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone)]
pub struct NetworkCongestionEstimate {
    pub level: CongestionLevel,
    pub factor: f64,
    pub avg_latency_ms: f64,
    pub recommended_priority_multiplier: f64,
}

#[derive(Debug, Clone)]
pub struct BundleLatencyEstimate {
    pub expected_ms: f64,
    pub confidence: f64,
    pub size_impact_factor: f64,
    pub priority_impact_factor: f64,
    pub sample_count: usize,
}

#[derive(Debug, Clone, Copy)]
pub enum SubmissionStrategy {
    Default,
    FrontRun,
    Competitive,
}

#[derive(Debug, Clone)]
pub struct OptimalTimingRecommendation {
    pub submit_at_ms: u64,
    pub confidence: f64,
    pub strategy: SubmissionStrategy,
    pub priority_fee_gwei: u64,
    pub fallback_timing_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompetitorStats {
    pub total_txs: u64,
    pub success_rate: f64,
    pub avg_latency_ms: f64,
    pub std_dev_ms: f64,
    pub last_seen_slot: Slot,
    pub sample_count: usize,
}

fn calculate_confidence(sample_count: usize, std_dev: f64, success_rate: f64) -> f64 {
    let sample_factor = (sample_count as f64 / 50.0).min(1.0);
    let consistency_factor = 1.0 / (1.0 + std_dev / 20.0);
    let success_factor = success_rate.powf(0.5);
    
    (sample_factor * 0.4 + consistency_factor * 0.4 + success_factor * 0.2).min(1.0)
}

fn calculate_competitor_score(profile: &CompetitorProfile) -> f64 {
    let recency_score = 1.0 / (1.0 + profile.last_seen_slot as f64 / 10000.0);
    let performance_score = 1.0 / (1.0 + profile.avg_latency_ms / 100.0);
    let reliability_score = profile.success_rate;
    let volume_score = (profile.total_transactions as f64 / 1000.0).min(1.0);
    
    recency_score * 0.2 + performance_score * 0.4 + reliability_score * 0.3 + volume_score * 0.1
}

fn calculate_priority_multiplier(congestion_factor: f64) -> f64 {
    match congestion_factor {
        f if f < 0.8 => 1.0,
        f if f < 1.2 => 1.5,
        f if f < 1.8 => 2.5,
        _ => 4.0,
    }
}

fn calculate_optimal_priority_fee(windows: &[ActionWindow], congestion: f64) -> u64 {
    let base_fee = 50_000;
    let congestion_multiplier = (congestion * 2.0).max(1.0);
    let competition_multiplier = (windows.len() as f64 / 3.0).max(1.0).min(5.0);
    
    (base_fee as f64 * congestion_multiplier * competition_multiplier) as u64
}

fn calculate_timing_confidence(windows: &[ActionWindow]) -> f64 {
    if windows.is_empty() {
        return 0.5;
    }
    
    let avg_confidence = windows.iter().map(|w| w.confidence).sum::<f64>() / windows.len() as f64;
    let consistency_factor = if windows.len() > 1 {
        let timings: Vec<f64> = windows.iter().map(|w| w.likely_ms as f64).collect();
        let timing_std_dev = standard_deviation(&timings, None);
        1.0 / (1.0 + timing_std_dev / 50.0)
    } else {
        0.8
    };
    
    (avg_confidence * 0.7 + consistency_factor * 0.3).min(0.95)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_latency_estimation() {
        let estimator = CompetitorLatencyEstimator::new();
        let competitor = Pubkey::new_unique();
        let sig = Signature::new_unique();
        
        estimator.record_transaction_submission(sig, competitor, 100_000, 1).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        let latency = estimator.record_transaction_confirmation(sig, 12345, CommitmentLevel::Confirmed).await;
        assert!(latency.is_some());
        
        let estimate = estimator.estimate_competitor_latency(competitor).await;
        assert!(estimate.is_some());
    }

    #[tokio::test]
    async fn test_congestion_estimation() {
        let estimator = CompetitorLatencyEstimator::new();
        let congestion = estimator.get_network_congestion_estimate().await;
        assert_eq!(congestion.level, CongestionLevel::Normal);
    }
}

