use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    clock::Slot,
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::Signature,
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;
use serde::{Deserialize, Serialize};

const MAX_HISTORICAL_SLOTS: usize = 432000; // ~2 days of slots
const FEE_PERCENTILES: [f64; 5] = [0.5, 0.75, 0.9, 0.95, 0.99];
const DECAY_FACTOR: f64 = 0.95;
const MIN_SAMPLE_SIZE: usize = 100;
const OUTLIER_THRESHOLD: f64 = 3.0;
const CONFIDENCE_THRESHOLD: f64 = 0.85;
const MAX_FEE_HISTORY: usize = 10000;
const SLOT_DURATION_MS: u64 = 400;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotFeeMetrics {
    pub slot: Slot,
    pub leader: Pubkey,
    pub timestamp: u64,
    pub total_fees: u64,
    pub transaction_count: u32,
    pub avg_fee_per_tx: u64,
    pub median_fee: u64,
    pub percentiles: HashMap<String, u64>,
    pub max_fee: u64,
    pub min_fee: u64,
    pub success_rate: f64,
    pub compute_unit_price_avg: u64,
    pub priority_fee_avg: u64,
}

#[derive(Debug, Clone)]
pub struct LeaderFeeProfile {
    pub pubkey: Pubkey,
    pub historical_fees: VecDeque<SlotFeeMetrics>,
    pub avg_fee_30min: u64,
    pub avg_fee_1hr: u64,
    pub avg_fee_24hr: u64,
    pub fee_volatility: f64,
    pub reliability_score: f64,
    pub last_updated: Instant,
    pub total_slots_processed: u64,
    pub optimal_fee_estimate: u64,
    pub confidence_score: f64,
}

#[derive(Debug, Clone)]
pub struct FeeRecommendation {
    pub base_fee: u64,
    pub priority_fee: u64,
    pub compute_unit_price: u64,
    pub confidence: f64,
    pub risk_level: RiskLevel,
    pub expected_success_rate: f64,
    pub percentile_target: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RiskLevel {
    Conservative,
    Moderate,
    Aggressive,
    UltraAggressive,
}

pub struct SlotLeaderFeeProfiler {
    rpc_client: Arc<RpcClient>,
    leader_profiles: Arc<RwLock<HashMap<Pubkey, LeaderFeeProfile>>>,
    slot_metrics: Arc<RwLock<VecDeque<SlotFeeMetrics>>>,
    current_slot: Arc<RwLock<Slot>>,
    update_channel: mpsc::Sender<SlotFeeMetrics>,
    shutdown_signal: Arc<RwLock<bool>>,
}

impl SlotLeaderFeeProfiler {
    pub fn new(rpc_endpoint: &str) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_timeout(
            rpc_endpoint.to_string(),
            Duration::from_secs(30),
        ));

        let (tx, mut rx) = mpsc::channel::<SlotFeeMetrics>(1000);
        let leader_profiles = Arc::new(RwLock::new(HashMap::new()));
        let slot_metrics = Arc::new(RwLock::new(VecDeque::with_capacity(MAX_HISTORICAL_SLOTS)));
        
        let profiles_clone = leader_profiles.clone();
        let metrics_clone = slot_metrics.clone();
        
        tokio::spawn(async move {
            while let Some(metrics) = rx.recv().await {
                Self::update_leader_profile(&profiles_clone, &metrics_clone, metrics);
            }
        });

        Self {
            rpc_client,
            leader_profiles,
            slot_metrics,
            current_slot: Arc::new(RwLock::new(0)),
            update_channel: tx,
            shutdown_signal: Arc::new(RwLock::new(false)),
        }
    }

    pub async fn start_monitoring(&self) {
        let rpc_client = self.rpc_client.clone();
        let update_channel = self.update_channel.clone();
        let current_slot = self.current_slot.clone();
        let shutdown_signal = self.shutdown_signal.clone();

        tokio::spawn(async move {
            let mut last_processed_slot = 0;
            
            loop {
                if *shutdown_signal.read().unwrap() {
                    break;
                }

                match rpc_client.get_slot_with_commitment(CommitmentConfig::confirmed()) {
                    Ok(slot) => {
                        *current_slot.write().unwrap() = slot;
                        
                        if slot > last_processed_slot {
                            for processing_slot in (last_processed_slot + 1)..=slot {
                                if let Ok(Some(block)) = rpc_client.get_block_with_config(
                                    processing_slot,
                                    solana_client::rpc_config::RpcBlockConfig {
                                        encoding: Some(UiTransactionEncoding::Base64),
                                        transaction_details: Some(
                                            solana_transaction_status::TransactionDetails::Full
                                        ),
                                        rewards: Some(true),
                                        commitment: Some(CommitmentConfig::confirmed()),
                                        max_supported_transaction_version: Some(0),
                                    },
                                ) {
                                    if let Ok(leader) = rpc_client.get_slot_leader(processing_slot) {
                                        let metrics = Self::analyze_block_fees(&block, processing_slot, leader);
                                        let _ = update_channel.send(metrics).await;
                                    }
                                }
                            }
                            last_processed_slot = slot;
                        }
                    }
                    Err(_) => {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
                
                tokio::time::sleep(Duration::from_millis(SLOT_DURATION_MS / 2)).await;
            }
        });
    }

    fn analyze_block_fees(
        block: &solana_transaction_status::EncodedConfirmedBlock,
        slot: Slot,
        leader: Pubkey,
    ) -> SlotFeeMetrics {
        let mut fees = Vec::new();
        let mut total_fees = 0u64;
        let mut successful_txs = 0u32;
        let mut total_compute_units = 0u64;
        let mut total_priority_fees = 0u64;

        if let Some(transactions) = &block.transactions {
            for tx_with_meta in transactions {
                if let Some(meta) = &tx_with_meta.meta {
                    let fee = meta.fee;
                    fees.push(fee);
                    total_fees += fee;
                    
                    if meta.err.is_none() {
                        successful_txs += 1;
                    }

                    if let Some(compute_units_consumed) = meta.compute_units_consumed {
                        total_compute_units += compute_units_consumed;
                    }

                    if let Some(inner_instructions) = &meta.inner_instructions {
                        for inner in inner_instructions {
                            total_priority_fees += inner.instructions.len() as u64 * 5000;
                        }
                    }
                }
            }
        }

        fees.sort_unstable();
        let transaction_count = fees.len() as u32;
        let success_rate = if transaction_count > 0 {
            successful_txs as f64 / transaction_count as f64
        } else {
            0.0
        };

        let avg_fee_per_tx = if transaction_count > 0 {
            total_fees / transaction_count as u64
        } else {
            0
        };

        let median_fee = if !fees.is_empty() {
            fees[fees.len() / 2]
        } else {
            0
        };

        let mut percentiles = HashMap::new();
        for &p in &FEE_PERCENTILES {
            let idx = ((fees.len() as f64 * p) as usize).min(fees.len().saturating_sub(1));
            percentiles.insert(format!("p{}", (p * 100.0) as u8), fees.get(idx).copied().unwrap_or(0));
        }

        let compute_unit_price_avg = if total_compute_units > 0 {
            total_fees * 1_000_000 / total_compute_units
        } else {
            0
        };

        let priority_fee_avg = if transaction_count > 0 {
            total_priority_fees / transaction_count as u64
        } else {
            0
        };

        SlotFeeMetrics {
            slot,
            leader,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
            total_fees,
            transaction_count,
            avg_fee_per_tx,
            median_fee,
            percentiles,
            max_fee: fees.last().copied().unwrap_or(0),
            min_fee: fees.first().copied().unwrap_or(0),
            success_rate,
            compute_unit_price_avg,
            priority_fee_avg,
        }
    }

    fn update_leader_profile(
        profiles: &Arc<RwLock<HashMap<Pubkey, LeaderFeeProfile>>>,
        all_metrics: &Arc<RwLock<VecDeque<SlotFeeMetrics>>>,
        new_metrics: SlotFeeMetrics,
    ) {
        let mut profiles_guard = profiles.write().unwrap();
        let mut metrics_guard = all_metrics.write().unwrap();
        
        metrics_guard.push_back(new_metrics.clone());
        if metrics_guard.len() > MAX_HISTORICAL_SLOTS {
            metrics_guard.pop_front();
        }

        let profile = profiles_guard
            .entry(new_metrics.leader)
            .or_insert_with(|| LeaderFeeProfile {
                pubkey: new_metrics.leader,
                historical_fees: VecDeque::with_capacity(MAX_FEE_HISTORY),
                avg_fee_30min: 0,
                avg_fee_1hr: 0,
                avg_fee_24hr: 0,
                fee_volatility: 0.0,
                reliability_score: 1.0,
                last_updated: Instant::now(),
                total_slots_processed: 0,
                optimal_fee_estimate: 0,
                confidence_score: 0.0,
            });

        profile.historical_fees.push_back(new_metrics.clone());
        if profile.historical_fees.len() > MAX_FEE_HISTORY {
            profile.historical_fees.pop_front();
        }

        profile.total_slots_processed += 1;
        profile.last_updated = Instant::now();

        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        
        let fees_30min: Vec<u64> = profile.historical_fees.iter()
            .filter(|m| now - m.timestamp <= 1800)
            .map(|m| m.avg_fee_per_tx)
            .collect();
        
        let fees_1hr: Vec<u64> = profile.historical_fees.iter()
            .filter(|m| now - m.timestamp <= 3600)
            .map(|m| m.avg_fee_per_tx)
            .collect();
        
        let fees_24hr: Vec<u64> = profile.historical_fees.iter()
            .filter(|m| now - m.timestamp <= 86400)
            .map(|m| m.avg_fee_per_tx)
            .collect();

        profile.avg_fee_30min = Self::calculate_weighted_average(&fees_30min);
        profile.avg_fee_1hr = Self::calculate_weighted_average(&fees_1hr);
        profile.avg_fee_24hr = Self::calculate_weighted_average(&fees_24hr);
        
        profile.fee_volatility = Self::calculate_volatility(&fees_1hr);
        profile.reliability_score = Self::calculate_reliability_score(profile);
        profile.optimal_fee_estimate = Self::calculate_optimal_fee(profile);
        profile.confidence_score = Self::calculate_confidence_score(profile);
    }

    fn calculate_weighted_average(fees: &[u64]) -> u64 {
        if fees.is_empty() {
            return 0;
        }
        
        let mut weighted_sum = 0.0;
        let mut weight_sum = 0.0;
        
        for (i, &fee) in fees.iter().enumerate() {
            let weight = DECAY_FACTOR.powi(i as i32);
            weighted_sum += fee as f64 * weight;
            weight_sum += weight;
        }
        
        (weighted_sum / weight_sum) as u64
    }

    fn calculate_volatility(fees: &[u64]) -> f64 {
        if fees.len() < 2 {
            return 0.0;
        }
        
        let mean = fees.iter().sum::<u64>() as f64 / fees.len() as f64;
        let variance = fees.iter()
            .map(|&f| {
                let diff = f as f64 - mean;
                diff * diff
            })
            .sum::<f64>() / fees.len() as f64;
        
        variance.sqrt() / mean.max(1.0)
    }

    fn calculate_reliability_score(profile: &LeaderFeeProfile) -> f64 {
        let base_score = profile.historical_fees.iter()
            .map(|m| m.success_rate)
                        .sum::<f64>() / profile.historical_fees.len().max(1) as f64;
        
        let consistency_factor = 1.0 - profile.fee_volatility.min(1.0);
        let volume_factor = (profile.total_slots_processed as f64 / 1000.0).min(1.0);
        
        base_score * 0.5 + consistency_factor * 0.3 + volume_factor * 0.2
    }

    fn calculate_optimal_fee(profile: &LeaderFeeProfile) -> u64 {
        if profile.historical_fees.len() < MIN_SAMPLE_SIZE {
            return profile.avg_fee_1hr;
        }

        let mut recent_fees: Vec<u64> = profile.historical_fees
            .iter()
            .rev()
            .take(200)
            .map(|m| m.avg_fee_per_tx)
            .collect();
        
        recent_fees.sort_unstable();
        let q1_idx = recent_fees.len() / 4;
        let q3_idx = 3 * recent_fees.len() / 4;
        let q1 = recent_fees[q1_idx] as f64;
        let q3 = recent_fees[q3_idx] as f64;
        let iqr = q3 - q1;
        
        let lower_bound = q1 - OUTLIER_THRESHOLD * iqr;
        let upper_bound = q3 + OUTLIER_THRESHOLD * iqr;
        
        let filtered_fees: Vec<u64> = recent_fees
            .into_iter()
            .filter(|&f| f as f64 >= lower_bound && f as f64 <= upper_bound)
            .collect();
        
        if filtered_fees.is_empty() {
            return profile.avg_fee_30min;
        }
        
        let target_idx = (filtered_fees.len() as f64 * 0.85) as usize;
        filtered_fees[target_idx.min(filtered_fees.len() - 1)]
    }

    fn calculate_confidence_score(profile: &LeaderFeeProfile) -> f64 {
        let sample_size_score = (profile.historical_fees.len() as f64 / MIN_SAMPLE_SIZE as f64).min(1.0);
        let recency_score = if profile.last_updated.elapsed().as_secs() < 300 { 1.0 } else { 0.5 };
        let volatility_score = 1.0 / (1.0 + profile.fee_volatility);
        
        (sample_size_score * 0.4 + recency_score * 0.3 + volatility_score * 0.3).min(1.0)
    }

    pub async fn get_fee_recommendation(
        &self,
        leader: &Pubkey,
        risk_level: RiskLevel,
    ) -> Result<FeeRecommendation, String> {
        let profiles = self.leader_profiles.read().unwrap();
        let profile = profiles.get(leader)
            .ok_or_else(|| format!("No profile found for leader {}", leader))?;
        
        if profile.confidence_score < CONFIDENCE_THRESHOLD && profile.historical_fees.len() < MIN_SAMPLE_SIZE {
            return self.get_network_wide_recommendation(risk_level).await;
        }
        
        let percentile_target = match risk_level {
            RiskLevel::Conservative => 0.75,
            RiskLevel::Moderate => 0.85,
            RiskLevel::Aggressive => 0.95,
            RiskLevel::UltraAggressive => 0.99,
        };
        
        let base_fee = self.calculate_base_fee(profile, percentile_target);
        let priority_fee = self.calculate_priority_fee(profile, risk_level);
        let compute_unit_price = self.calculate_compute_unit_price(profile, risk_level);
        
        let expected_success_rate = self.estimate_success_rate(profile, base_fee);
        
        Ok(FeeRecommendation {
            base_fee,
            priority_fee,
            compute_unit_price,
            confidence: profile.confidence_score,
            risk_level,
            expected_success_rate,
            percentile_target,
        })
    }

    fn calculate_base_fee(&self, profile: &LeaderFeeProfile, percentile: f64) -> u64 {
        let mut recent_fees: Vec<u64> = profile.historical_fees
            .iter()
            .rev()
            .take(500)
            .flat_map(|m| {
                let p_key = format!("p{}", (percentile * 100.0) as u8);
                m.percentiles.get(&p_key).copied()
            })
            .collect();
        
        if recent_fees.is_empty() {
            return profile.optimal_fee_estimate;
        }
        
        recent_fees.sort_unstable();
        let idx = ((recent_fees.len() as f64 * percentile) as usize).min(recent_fees.len() - 1);
        recent_fees[idx]
    }

    fn calculate_priority_fee(&self, profile: &LeaderFeeProfile, risk_level: RiskLevel) -> u64 {
        let base_priority = profile.historical_fees
            .iter()
            .rev()
            .take(100)
            .map(|m| m.priority_fee_avg)
            .sum::<u64>() / 100u64.max(profile.historical_fees.len() as u64).min(100);
        
        match risk_level {
            RiskLevel::Conservative => base_priority,
            RiskLevel::Moderate => (base_priority as f64 * 1.2) as u64,
            RiskLevel::Aggressive => (base_priority as f64 * 1.5) as u64,
            RiskLevel::UltraAggressive => (base_priority as f64 * 2.0) as u64,
        }
    }

    fn calculate_compute_unit_price(&self, profile: &LeaderFeeProfile, risk_level: RiskLevel) -> u64 {
        let base_price = profile.historical_fees
            .iter()
            .rev()
            .take(100)
            .map(|m| m.compute_unit_price_avg)
            .filter(|&p| p > 0)
            .sum::<u64>() / 100u64.max(profile.historical_fees.len() as u64).min(100).max(1);
        
        match risk_level {
            RiskLevel::Conservative => base_price,
            RiskLevel::Moderate => (base_price as f64 * 1.15) as u64,
            RiskLevel::Aggressive => (base_price as f64 * 1.3) as u64,
            RiskLevel::UltraAggressive => (base_price as f64 * 1.6) as u64,
        }
    }

    fn estimate_success_rate(&self, profile: &LeaderFeeProfile, proposed_fee: u64) -> f64 {
        let historical_success: Vec<(u64, f64)> = profile.historical_fees
            .iter()
            .map(|m| (m.avg_fee_per_tx, m.success_rate))
            .collect();
        
        if historical_success.len() < 10 {
            return profile.reliability_score;
        }
        
        let mut weighted_success = 0.0;
        let mut weight_sum = 0.0;
        
        for (fee, success_rate) in &historical_success {
            let fee_diff = (*fee as f64 - proposed_fee as f64).abs();
            let weight = 1.0 / (1.0 + fee_diff / 1_000_000.0);
            weighted_success += success_rate * weight;
            weight_sum += weight;
        }
        
        (weighted_success / weight_sum).max(0.0).min(1.0)
    }

    async fn get_network_wide_recommendation(&self, risk_level: RiskLevel) -> Result<FeeRecommendation, String> {
        let metrics = self.slot_metrics.read().unwrap();
        
        if metrics.is_empty() {
            return Err("No network metrics available".to_string());
        }
        
        let mut all_fees: Vec<u64> = metrics
            .iter()
            .rev()
            .take(1000)
            .map(|m| m.avg_fee_per_tx)
            .collect();
        
        all_fees.sort_unstable();
        
        let percentile_target = match risk_level {
            RiskLevel::Conservative => 0.75,
            RiskLevel::Moderate => 0.85,
            RiskLevel::Aggressive => 0.95,
            RiskLevel::UltraAggressive => 0.99,
        };
        
        let idx = ((all_fees.len() as f64 * percentile_target) as usize).min(all_fees.len() - 1);
        let base_fee = all_fees[idx];
        
        let priority_fee = match risk_level {
            RiskLevel::Conservative => 10_000,
            RiskLevel::Moderate => 25_000,
            RiskLevel::Aggressive => 50_000,
            RiskLevel::UltraAggressive => 100_000,
        };
        
        let compute_unit_price = match risk_level {
            RiskLevel::Conservative => 100,
            RiskLevel::Moderate => 200,
            RiskLevel::Aggressive => 500,
            RiskLevel::UltraAggressive => 1000,
        };
        
        Ok(FeeRecommendation {
            base_fee,
            priority_fee,
            compute_unit_price,
            confidence: 0.5,
            risk_level,
            expected_success_rate: 0.7,
            percentile_target,
        })
    }

    pub async fn get_upcoming_leaders(&self, slots_ahead: u64) -> Result<Vec<(Slot, Pubkey)>, String> {
        let current_slot = *self.current_slot.read().unwrap();
        let mut leaders = Vec::new();
        
        for i in 1..=slots_ahead {
            let slot = current_slot + i;
            match self.rpc_client.get_slot_leader(slot) {
                Ok(leader) => leaders.push((slot, leader)),
                Err(e) => return Err(format!("Failed to get slot leader: {}", e)),
            }
        }
        
        Ok(leaders)
    }

    pub async fn get_leader_metrics(&self, leader: &Pubkey) -> Option<LeaderFeeProfile> {
        self.leader_profiles.read().unwrap().get(leader).cloned()
    }

    pub fn get_recent_slot_metrics(&self, count: usize) -> Vec<SlotFeeMetrics> {
        let metrics = self.slot_metrics.read().unwrap();
        metrics.iter().rev().take(count).cloned().collect()
    }

    pub async fn analyze_fee_trends(&self, window_minutes: u64) -> HashMap<String, f64> {
        let metrics = self.slot_metrics.read().unwrap();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let window_start = now - (window_minutes * 60);
        
        let recent_metrics: Vec<&SlotFeeMetrics> = metrics
            .iter()
            .filter(|m| m.timestamp >= window_start)
            .collect();
        
        if recent_metrics.is_empty() {
            return HashMap::new();
        }
        
        let mut trends = HashMap::new();
        
        let avg_fee = recent_metrics.iter().map(|m| m.avg_fee_per_tx).sum::<u64>() as f64 / recent_metrics.len() as f64;
        let avg_success_rate = recent_metrics.iter().map(|m| m.success_rate).sum::<f64>() / recent_metrics.len() as f64;
        let avg_tx_count = recent_metrics.iter().map(|m| m.transaction_count).sum::<u32>() as f64 / recent_metrics.len() as f64;
        
        trends.insert("avg_fee".to_string(), avg_fee);
        trends.insert("avg_success_rate".to_string(), avg_success_rate);
        trends.insert("avg_tx_count".to_string(), avg_tx_count);
        
        let fee_volatility = Self::calculate_volatility(
            &recent_metrics.iter().map(|m| m.avg_fee_per_tx).collect::<Vec<_>>()
        );
        trends.insert("fee_volatility".to_string(), fee_volatility);
        
        trends
    }

    pub fn shutdown(&self) {
        *self.shutdown_signal.write().unwrap() = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fee_calculation() {
        let fees = vec![100, 200, 300, 400, 500];
        let avg = SlotLeaderFeeProfiler::calculate_weighted_average(&fees);
        assert!(avg > 0);
    }

    #[test]
    fn test_volatility_calculation() {
        let fees = vec![100, 200, 300, 400, 500];
        let volatility = SlotLeaderFeeProfiler::calculate_volatility(&fees);
        assert!(volatility >= 0.0);
    }
}

