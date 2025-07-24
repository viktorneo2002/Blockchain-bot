use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock as AsyncRwLock;
use anyhow::{Result, anyhow};

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
    pub timestamp_history: VecDeque<Instant>,
    pub success_rate: f64,
    pub min_accepted_tip: u64,
    pub median_accepted_tip: u64,
    pub percentile_95_tip: u64,
    pub last_update: Instant,
    pub bundle_inclusion_times: VecDeque<Duration>,
    pub network_congestion_factor: f64,
}

#[derive(Debug, Clone)]
pub struct ValidatorProfile {
    pub pubkey: Pubkey,
    pub historical_acceptance_rate: f64,
    pub tip_sensitivity: f64,
    pub peak_hours: Vec<u8>,
    pub reliability_score: f64,
    pub recent_performance: VecDeque<f64>,
}

#[derive(Debug, Clone)]
pub struct NetworkConditions {
    pub current_tps: f64,
    pub average_priority_fee: u64,
    pub slot_leader_schedule: HashMap<u64, Pubkey>,
    pub recent_block_rewards: VecDeque<u64>,
    pub congestion_level: f64,
}

pub struct ValidatorTipAcceptanceModeler {
    validator_metrics: Arc<AsyncRwLock<HashMap<Pubkey, TipAcceptanceMetrics>>>,
    validator_profiles: Arc<AsyncRwLock<HashMap<Pubkey, ValidatorProfile>>>,
    network_conditions: Arc<AsyncRwLock<NetworkConditions>>,
    global_tip_distribution: Arc<RwLock<VecDeque<u64>>>,
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
            validator_metrics: Arc::new(AsyncRwLock::new(HashMap::new())),
            validator_profiles: Arc::new(AsyncRwLock::new(HashMap::new())),
            network_conditions: Arc::new(AsyncRwLock::new(NetworkConditions {
                current_tps: 2000.0,
                average_priority_fee: 1_000,
                slot_leader_schedule: HashMap::new(),
                recent_block_rewards: VecDeque::with_capacity(100),
                congestion_level: 0.5,
            })),
            global_tip_distribution: Arc::new(RwLock::new(VecDeque::with_capacity(SLIDING_WINDOW_SIZE))),
            model_parameters: ModelParameters::default(),
        }
    }

    pub async fn calculate_optimal_tip(
        &self,
        validator: &Pubkey,
        bundle_value: u64,
        urgency_factor: f64,
    ) -> Result<u64> {
        let metrics = self.validator_metrics.read().await;
        let profiles = self.validator_profiles.read().await;
        let network = self.network_conditions.read().await;
        
        let validator_metrics = metrics.get(validator);
        let validator_profile = profiles.get(validator);
        
        let base_tip = self.calculate_base_tip(
            validator_metrics,
            validator_profile,
            &network,
            bundle_value,
        );
        
        let adjusted_tip = self.apply_dynamic_adjustments(
            base_tip,
            urgency_factor,
            &network,
            validator_profile,
        );
        
        let competitive_tip = self.apply_competitive_analysis(
            adjusted_tip,
            validator_metrics,
        );
        
        let final_tip = self.apply_safety_bounds(competitive_tip, bundle_value);
        
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
        
        let time_sensitivity = if let Some(p) = profile {
            let current_hour = chrono::Local::now().hour() as u8;
            if p.peak_hours.contains(&current_hour) {
                1.2
            } else {
                1.0
            }
        } else {
            1.0
        };
        
        let urgency_multiplier = 1.0 + (urgency_factor - 1.0).min(2.0) * 0.3;
        
        ((base_tip as f64) * 
         congestion_adjustment * 
         time_sensitivity * 
         urgency_multiplier) as u64
    }

    fn apply_competitive_analysis(
        &self,
        tip: u64,
        metrics: Option<&TipAcceptanceMetrics>,
    ) -> u64 {
        let global_distribution = self.global_tip_distribution.read().unwrap();
        
        if global_distribution.len() < MIN_SAMPLE_SIZE {
            return (tip as f64 * self.model_parameters.competitive_factor) as u64;
        }
        
        let mut sorted_tips: Vec<u64> = global_distribution.iter().cloned().collect();
        sorted_tips.sort_unstable();
        
        let percentile_75_idx = (sorted_tips.len() as f64 * 0.75) as usize;
        let market_percentile_75 = sorted_tips[percentile_75_idx];
        
        let competitive_tip = tip.max(market_percentile_75);
        
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

    fn apply_safety_bounds(&self, tip: u64, bundle_value: u64) -> u64 {
        let max_reasonable_tip = (bundle_value as f64 * 0.5) as u64;
        tip.min(max_reasonable_tip).min(MAX_TIP_LAMPORTS).max(BASE_TIP_LAMPORTS)
    }

    pub async fn record_tip_result(
        &self,
        validator: &Pubkey,
        tip_amount: u64,
        accepted: bool,
        inclusion_time: Option<Duration>,
    ) -> Result<()> {
        let mut metrics = self.validator_metrics.write().await;
        let entry = metrics.entry(*validator).or_insert_with(|| TipAcceptanceMetrics {
            validator: *validator,
            accepted_tips: VecDeque::with_capacity(SLIDING_WINDOW_SIZE),
            rejected_tips: VecDeque::with_capacity(SLIDING_WINDOW_SIZE),
            timestamp_history: VecDeque::with_capacity(SLIDING_WINDOW_SIZE),
            success_rate: 0.0,
            min_accepted_tip: u64::MAX,
            median_accepted_tip: BASE_TIP_LAMPORTS,
            percentile_95_tip: BASE_TIP_LAMPORTS,
            last_update: Instant::now(),
            bundle_inclusion_times: VecDeque::with_capacity(100),
            network_congestion_factor: 0.5,
        });

        if accepted {
            entry.accepted_tips.push_back(tip_amount);
            if entry.accepted_tips.len() > SLIDING_WINDOW_SIZE {
                entry.accepted_tips.pop_front();
            }
            
            if let Some(time) = inclusion_time {
                entry.bundle_inclusion_times.push_back(time);
                if entry.bundle_inclusion_times.len() > 100 {
                    entry.bundle_inclusion_times.pop_front();
                }
            }
        } else {
            entry.rejected_tips.push_back(tip_amount);
            if entry.rejected_tips.len() > SLIDING_WINDOW_SIZE {
                entry.rejected_tips.pop_front();
            }
        }

        entry.timestamp_history.push_back(Instant::now());
        if entry.timestamp_history.len() > SLIDING_WINDOW_SIZE {
            entry.timestamp_history.pop_front();
        }

        self.update_validator_statistics(entry).await;
        self.update_global_distribution(tip_amount, accepted).await;

        Ok(())
    }

    async fn update_validator_statistics(&self, metrics: &mut TipAcceptanceMetrics) {
        let total_attempts = metrics.accepted_tips.len() + metrics.rejected_tips.len();
        if total_attempts > 0 {
            metrics.success_rate = metrics.accepted_tips.len() as f64 / total_attempts as f64;
        }

        if !metrics.accepted_tips.is_empty() {
            let mut accepted_vec: Vec<u64> = metrics.accepted_tips.iter().cloned().collect();
            accepted_vec.sort_unstable();
            
            metrics.min_accepted_tip = *accepted_vec.first().unwrap();
            metrics.median_accepted_tip = accepted_vec[accepted_vec.len() / 2];
            
            let percentile_95_idx = (accepted_vec.len() as f64 * 0.95) as usize;
            metrics.percentile_95_tip = accepted_vec[percentile_95_idx.min(accepted_vec.len() - 1)];
        }

        metrics.last_update = Instant::now();
    }

    async fn update_global_distribution(&self, tip_amount: u64, accepted: bool) {
        if accepted {
            let mut global_dist = self.global_tip_distribution.write().unwrap();
            global_dist.push_back(tip_amount);
            if global_dist.len() > SLIDING_WINDOW_SIZE {
                global_dist.pop_front();
            }
        }
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
        
        Ok(())
    }

    pub async fn get_validator_recommendation(
        &self,
        validator: &Pubkey,
    ) -> Result<TipRecommendation> {
        let metrics = self.validator_metrics.read().await;
        let profiles = self.validator_profiles.read().await;
        let network = self.network_conditions.read().await;
        
        let confidence = self.calculate_confidence(
            metrics.get(validator),
            profiles.get(validator),
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
                .map(|m| m.success_rate)
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
            let sample_size = m.accepted_tips.len() + m.rejected_tips.len();
            let recency_factor = if let Some(last) = m.timestamp_history.back() {
                let age = last.elapsed().as_secs() as f64;
                (-age / 3600.0).exp()
            } else {
                0.0
            };
            
            let size_factor = (sample_size as f64 / MIN_SAMPLE_SIZE as f64).min(1.0);
            size_factor * recency_factor * 0.7
        } else {
            0.0
        };
        
        let profile_confidence = if let Some(p) = profile {
            p.reliability_score * 0.3
        } else {
            0.0
        };
        
        (data_confidence + profile_confidence).min(1.0)
    }

    pub async fn update_validator_profile(
        &self,
        validator: &Pubkey,
        slot_performance: f64,
    ) -> Result<()> {
        let mut profiles = self.validator_profiles.write().await;
        let profile = profiles.entry(*validator).or_insert_with(|| ValidatorProfile {
            pubkey: *validator,
            historical_acceptance_rate: 0.5,
            tip_sensitivity: 1.0,
            peak_hours: vec![14, 15, 16, 17, 18, 19, 20, 21],
            reliability_score: 0.5,
            recent_performance: VecDeque::with_capacity(50),
        });
        
        profile.recent_performance.push_back(slot_performance);
        if profile.recent_performance.len() > 50 {
            profile.recent_performance.pop_front();
        }
        
        if profile.recent_performance.len() >= 10 {
            let avg_performance: f64 = profile.recent_performance.iter().sum::<f64>() 
                / profile.recent_performance.len() as f64;
            profile.reliability_score = (profile.reliability_score * 0.7 + avg_performance * 0.3)
                .min(1.0).max(0.0);
        }
        
        let metrics = self.validator_metrics.read().await;
        if let Some(m) = metrics.get(validator) {
            profile.historical_acceptance_rate = m.success_rate;
            
            if m.accepted_tips.len() >= MIN_SAMPLE_SIZE && m.rejected_tips.len() >= 10 {
                let accepted_mean = m.accepted_tips.iter().sum::<u64>() as f64 
                    / m.accepted_tips.len() as f64;
                let rejected_mean = m.rejected_tips.iter().sum::<u64>() as f64 
                    / m.rejected_tips.len() as f64;
                
                if rejected_mean > 0.0 {
                    profile.tip_sensitivity = (accepted_mean / rejected_mean).min(5.0).max(0.2);
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
        let metrics = self.validator_metrics.read().await;
        let profiles = self.validator_profiles.read().await;
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
        
        let profile_adjustment = if let Some(p) = validator_profile {
            1.0 + (p.reliability_score - 0.5) * 0.3
        } else {
            1.0
        };
        
        let final_probability = (base_probability * network_adjustment * profile_adjustment)
            .min(0.99)
            .max(0.01);
        
        Ok(final_probability)
    }

    pub async fn get_market_insights(&self) -> Result<MarketInsights> {
        let global_dist = self.global_tip_distribution.read().unwrap();
        let network = self.network_conditions.read().await;
        let metrics = self.validator_metrics.read().await;
        
        let mut all_tips: Vec<u64> = global_dist.iter().cloned().collect();
        all_tips.sort_unstable();
        
        let market_stats = if all_tips.len() >= MIN_SAMPLE_SIZE {
            let median_idx = all_tips.len() / 2;
            let p25_idx = all_tips.len() / 4;
            let p75_idx = (all_tips.len() * 3) / 4;
            let p95_idx = (all_tips.len() * 95) / 100;
            
            MarketStatistics {
                median_tip: all_tips[median_idx],
                percentile_25: all_tips[p25_idx],
                percentile_75: all_tips[p75_idx],
                percentile_95: all_tips[p95_idx],
                average_tip: all_tips.iter().sum::<u64>() / all_tips.len() as u64,
                std_deviation: calculate_std_dev(&all_tips),
            }
        } else {
            MarketStatistics::default()
        };
        
        let top_validators: Vec<(Pubkey, f64)> = metrics.iter()
            .filter(|(_, m)| m.success_rate > 0.8 && m.accepted_tips.len() >= MIN_SAMPLE_SIZE)
            .map(|(k, m)| (*k, m.success_rate))
            .collect();
        
        Ok(MarketInsights {
            current_market_stats: market_stats,
            network_congestion: network.congestion_level,
            recommended_base_tip: market_stats.percentile_75,
            high_performance_validators: top_validators,
            market_volatility: market_stats.std_deviation as f64 / market_stats.average_tip as f64,
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
        let metrics = self.validator_metrics.read().await;
        let profile = self.validator_profiles.read().await;
        
        let validator_metrics = metrics.get(validator)
            .ok_or_else(|| anyhow!("No metrics found for validator"))?;
        
        let acceptance_trend = if validator_metrics.timestamp_history.len() >= 20 {
            let recent_20: Vec<bool> = validator_metrics.timestamp_history.iter()
                .rev()
                .take(20)
                .zip(validator_metrics.accepted_tips.iter().rev().take(20))
                .map(|(_, _)| true)
                .collect();
            
            let recent_rate = recent_20.iter().filter(|&&x| x).count() as f64 / 20.0;
            if recent_rate > validator_metrics.success_rate + 0.1 {
                AcceptanceTrend::Improving
            } else if recent_rate < validator_metrics.success_rate - 0.1 {
                AcceptanceTrend::Declining
            } else {
                AcceptanceTrend::Stable
            }
        } else {
            AcceptanceTrend::Unknown
        };
        
        let tip_elasticity = if let Some(p) = profile.get(validator) {
            p.tip_sensitivity
        } else {
            1.0
        };
        
        let avg_inclusion_time = if !validator_metrics.bundle_inclusion_times.is_empty() {
            let total: Duration = validator_metrics.bundle_inclusion_times.iter().sum();
            total / validator_metrics.bundle_inclusion_times.len() as u32
        } else {
            Duration::from_millis(400)
        };
        
        Ok(ValidatorPattern {
            validator: *validator,
            acceptance_trend,
            tip_elasticity,
            average_inclusion_time: avg_inclusion_time,
            reliability_score: profile.get(validator).map(|p| p.reliability_score).unwrap_or(0.5),
            peak_performance_hours: profile.get(validator)
                .map(|p| p.peak_hours.clone())
                .unwrap_or_default(),
        })
    }
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
