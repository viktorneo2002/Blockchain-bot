use solana_client::{
    rpc_client::RpcClient,
    rpc_config::{RpcSimulateTransactionConfig, RpcSendTransactionConfig},
};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::Keypair,
    transaction::Transaction,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tokio::sync::RwLock as AsyncRwLock;
use rand::Rng;

const DEFAULT_COMPUTE_UNITS: u32 = 200_000;
const MAX_COMPUTE_UNITS: u32 = 1_400_000;
const MIN_COMPUTE_UNITS: u32 = 50_000;
const PRIORITY_FEE_PERCENTILE: f64 = 0.75;
const SIMULATION_COMMITMENT: CommitmentConfig = CommitmentConfig::processed();
const CONGESTION_THRESHOLD: f64 = 0.85;
const MAX_PRIORITY_FEE_LAMPORTS: u64 = 50_000_000;
const MIN_PRIORITY_FEE_LAMPORTS: u64 = 1_000;
const FEE_CACHE_DURATION_MS: u64 = 1000;
const HISTORY_WINDOW_SIZE: usize = 100;
const OUTLIER_THRESHOLD: f64 = 3.0;
const BASE_RETRY_DELAY_MS: u64 = 50;
const MAX_RETRY_DELAY_MS: u64 = 2000;

#[derive(Debug, Clone)]
pub struct ComputeUnitMetrics {
    pub compute_units_consumed: u32,
    pub priority_fee: u64,
    pub success: bool,
    pub slot: u64,
    pub timestamp: Instant,
    pub congestion_score: f64,
}

#[derive(Debug, Clone)]
pub struct OptimizationConfig {
    pub aggressive_mode: bool,
    pub max_priority_fee_multiplier: f64,
    pub compute_unit_buffer_ratio: f64,
    pub dynamic_adjustment_enabled: bool,
    pub congestion_response_multiplier: f64,
}

impl Default for OptimizationConfig {
    fn default() -> Self {
        Self {
            aggressive_mode: true,
            max_priority_fee_multiplier: 5.0,
            compute_unit_buffer_ratio: 1.15,
            dynamic_adjustment_enabled: true,
            congestion_response_multiplier: 2.5,
        }
    }
}

pub struct ComputeUnitOptimizer {
    rpc_client: Arc<RpcClient>,
    metrics_history: Arc<RwLock<VecDeque<ComputeUnitMetrics>>>,
    fee_cache: Arc<AsyncRwLock<HashMap<String, (u64, Instant)>>>,
    config: OptimizationConfig,
    network_stats: Arc<RwLock<NetworkStats>>,
    instruction_complexity_map: Arc<RwLock<HashMap<String, u32>>>,
}

#[derive(Debug, Clone)]
struct NetworkStats {
    avg_success_rate: f64,
    avg_compute_units: u32,
    avg_priority_fee: u64,
    congestion_level: f64,
    last_update: Instant,
}

impl Default for NetworkStats {
    fn default() -> Self {
        Self {
            avg_success_rate: 0.5,
            avg_compute_units: DEFAULT_COMPUTE_UNITS,
            avg_priority_fee: 10_000,
            congestion_level: 0.5,
            last_update: Instant::now(),
        }
    }
}

impl ComputeUnitOptimizer {
    pub fn new(rpc_client: Arc<RpcClient>, config: OptimizationConfig) -> Self {
        Self {
            rpc_client,
            metrics_history: Arc::new(RwLock::new(VecDeque::with_capacity(HISTORY_WINDOW_SIZE))),
            fee_cache: Arc::new(AsyncRwLock::new(HashMap::new())),
            config,
            network_stats: Arc::new(RwLock::new(NetworkStats::default())),
            instruction_complexity_map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn optimize_transaction(
        &self,
        transaction: &mut Transaction,
        payer: &Keypair,
        recent_blockhash: solana_sdk::hash::Hash,
    ) -> Result<(u32, u64), Box<dyn std::error::Error + Send + Sync>> {
        let instructions = transaction.message.instructions.clone();
        
        let (compute_units, priority_fee) = self.calculate_optimal_parameters(&instructions).await?;
        
        let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(compute_units);
        let priority_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(
            priority_fee.saturating_div(compute_units as u64),
        );
        
        let mut new_instructions = vec![compute_budget_ix, priority_fee_ix];
        new_instructions.extend(instructions);
        
        *transaction = Transaction::new_signed_with_payer(
            &new_instructions,
            Some(&payer.pubkey()),
            &[payer],
            recent_blockhash,
        );
        
        Ok((compute_units, priority_fee))
    }

    async fn calculate_optimal_parameters(
        &self,
        instructions: &[Instruction],
    ) -> Result<(u32, u64), Box<dyn std::error::Error + Send + Sync>> {
        let base_compute_units = self.estimate_compute_units(instructions).await?;
        let network_stats = self.network_stats.read().unwrap().clone();
        
        let congestion_adjusted_units = if network_stats.congestion_level > CONGESTION_THRESHOLD {
            (base_compute_units as f64 * (1.0 + (network_stats.congestion_level - CONGESTION_THRESHOLD) * 2.0))
                .min(MAX_COMPUTE_UNITS as f64) as u32
        } else {
            base_compute_units
        };
        
        let buffered_units = (congestion_adjusted_units as f64 * self.config.compute_unit_buffer_ratio)
            .min(MAX_COMPUTE_UNITS as f64) as u32;
        
        let priority_fee = self.calculate_priority_fee(buffered_units, &network_stats).await?;
        
        Ok((buffered_units, priority_fee))
    }

    async fn estimate_compute_units(
        &self,
        instructions: &[Instruction],
    ) -> Result<u32, Box<dyn std::error::Error + Send + Sync>> {
        let mut total_complexity = 0u32;
        let complexity_map = self.instruction_complexity_map.read().unwrap();
        
        for instruction in instructions {
            let program_id = instruction.program_id.to_string();
            
            if let Some(&cached_complexity) = complexity_map.get(&program_id) {
                total_complexity = total_complexity.saturating_add(cached_complexity);
            } else {
                let estimated = self.estimate_instruction_complexity(instruction);
                total_complexity = total_complexity.saturating_add(estimated);
            }
        }
        
        let historical_adjustment = self.get_historical_adjustment();
        let adjusted_units = (total_complexity as f64 * historical_adjustment)
            .max(MIN_COMPUTE_UNITS as f64)
            .min(MAX_COMPUTE_UNITS as f64) as u32;
        
        Ok(adjusted_units)
    }

    fn estimate_instruction_complexity(&self, instruction: &Instruction) -> u32 {
        let base_cost = 5000;
        let account_cost = instruction.accounts.len() as u32 * 1000;
        let data_cost = (instruction.data.len() as u32).saturating_mul(100);
        
        let program_specific_cost = match instruction.program_id.to_string().as_str() {
            "11111111111111111111111111111111" => 1000,
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" => 3000,
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" => 25000,
            "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc" => 20000,
            "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4" => 30000,
            _ => 10000,
        };
        
        base_cost + account_cost + data_cost + program_specific_cost
    }

    async fn calculate_priority_fee(
        &self,
        compute_units: u32,
        network_stats: &NetworkStats,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let cache_key = format!("priority_fee_{}", compute_units);
        
        {
            let fee_cache = self.fee_cache.read().await;
            if let Some((cached_fee, timestamp)) = fee_cache.get(&cache_key) {
                if timestamp.elapsed().as_millis() < FEE_CACHE_DURATION_MS as u128 {
                    return Ok(*cached_fee);
                }
            }
        }
        
        let recent_fees = self.get_recent_priority_fees();
        let percentile_fee = self.calculate_percentile_fee(&recent_fees, PRIORITY_FEE_PERCENTILE);
        
        let congestion_multiplier = if network_stats.congestion_level > CONGESTION_THRESHOLD {
            1.0 + (network_stats.congestion_level - CONGESTION_THRESHOLD) * self.config.congestion_response_multiplier
        } else {
            1.0
        };
        
        let success_rate_multiplier = if network_stats.avg_success_rate < 0.3 {
            2.0
        } else if network_stats.avg_success_rate < 0.5 {
            1.5
        } else {
            1.0
        };
        
        let base_fee = if self.config.aggressive_mode {
            percentile_fee.max(network_stats.avg_priority_fee)
        } else {
            percentile_fee
        };
        
        let adjusted_fee = (base_fee as f64 * congestion_multiplier * success_rate_multiplier)
            .min(MAX_PRIORITY_FEE_LAMPORTS as f64)
            .max(MIN_PRIORITY_FEE_LAMPORTS as f64) as u64;
        
        {
            let mut fee_cache = self.fee_cache.write().await;
            fee_cache.insert(cache_key, (adjusted_fee, Instant::now()));
        }
        
        Ok(adjusted_fee)
    }

    fn get_recent_priority_fees(&self) -> Vec<u64> {
        let history = self.metrics_history.read().unwrap();
        history.iter()
            .filter(|m| m.timestamp.elapsed().as_secs() < 60)
            .map(|m| m.priority_fee)
            .collect()
    }

    fn calculate_percentile_fee(&self, fees: &[u64], percentile: f64) -> u64 {
        if fees.is_empty() {
            return 10_000;
        }
        
        let mut sorted_fees = fees.to_vec();
        sorted_fees.sort_unstable();
        
        let index = ((sorted_fees.len() as f64 - 1.0) * percentile) as usize;
        sorted_fees[index]
    }

    fn get_historical_adjustment(&self) -> f64 {
        let history = self.metrics_history.read().unwrap();
        if history.len() < 10 {
            return 1.0;
        }
        
        let recent_metrics: Vec<_> = history.iter()
            .filter(|m| m.timestamp.elapsed().as_secs() < 300)
            .collect();
        
        if recent_metrics.is_empty() {
            return 1.0;
        }
        
        let success_rate = recent_metrics.iter()
            .filter(|m| m.success)
            .count() as f64 / recent_metrics.len() as f64;
        
        if success_rate < 0.5 {
            1.2
        } else if success_rate < 0.7 {
            1.1
        } else if success_rate > 0.9 {
            0.95
        } else {
            1.0
        }
    }

    pub async fn simulate_and_adjust(
        &self,
        transaction: &Transaction,
    ) -> Result<(u32, bool), Box<dyn std::error::Error + Send + Sync>> {
        let config = RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: true,
            commitment: Some(SIMULATION_COMMITMENT),
            ..Default::default()
        };
        
        match self.rpc_client.simulate_transaction_with_config(transaction, config) {
            Ok(result) => {
                if let Some(err) = result.value.err {
                    Ok((DEFAULT_COMPUTE_UNITS, false))
                } else {
                    let units_consumed = result.value.units_consumed.unwrap_or(DEFAULT_COMPUTE_UNITS as u64) as u32;
                    Ok((units_consumed, true))
                }
            }
            Err(_) => Ok((DEFAULT_COMPUTE_UNITS, false))
        }
    }

    pub async fn record_transaction_result(
        &self,
        compute_units: u32,
        priority_fee: u64,
        success: bool,
        slot: u64,
    ) {
                let metrics = ComputeUnitMetrics {
            compute_units_consumed: compute_units,
            priority_fee,
            success,
            slot,
            timestamp: Instant::now(),
            congestion_score: self.calculate_congestion_score(),
        };
        
        {
            let mut history = self.metrics_history.write().unwrap();
            if history.len() >= HISTORY_WINDOW_SIZE {
                history.pop_front();
            }
            history.push_back(metrics.clone());
        }
        
        self.update_network_stats().await;
        self.update_instruction_complexity_cache(compute_units).await;
    }

    fn calculate_congestion_score(&self) -> f64 {
        let history = self.metrics_history.read().unwrap();
        if history.len() < 10 {
            return 0.5;
        }
        
        let recent_window = 20;
        let recent_metrics: Vec<_> = history.iter()
            .rev()
            .take(recent_window)
            .collect();
        
        let failure_rate = recent_metrics.iter()
            .filter(|m| !m.success)
            .count() as f64 / recent_metrics.len() as f64;
        
        let avg_priority_fee = recent_metrics.iter()
            .map(|m| m.priority_fee)
            .sum::<u64>() as f64 / recent_metrics.len() as f64;
        
        let normalized_fee = (avg_priority_fee / MAX_PRIORITY_FEE_LAMPORTS as f64).min(1.0);
        
        (failure_rate * 0.7 + normalized_fee * 0.3).min(1.0)
    }

    async fn update_network_stats(&self) {
        let history = self.metrics_history.read().unwrap();
        if history.is_empty() {
            return;
        }
        
        let recent_metrics: Vec<_> = history.iter()
            .filter(|m| m.timestamp.elapsed().as_secs() < 120)
            .collect();
        
        if recent_metrics.len() < 5 {
            return;
        }
        
        let success_count = recent_metrics.iter().filter(|m| m.success).count();
        let avg_success_rate = success_count as f64 / recent_metrics.len() as f64;
        
        let compute_units_without_outliers = self.remove_outliers(
            &recent_metrics.iter().map(|m| m.compute_units_consumed).collect::<Vec<_>>()
        );
        let avg_compute_units = compute_units_without_outliers.iter().sum::<u32>() / 
            compute_units_without_outliers.len().max(1) as u32;
        
        let priority_fees_without_outliers = self.remove_outliers_u64(
            &recent_metrics.iter().map(|m| m.priority_fee).collect::<Vec<_>>()
        );
        let avg_priority_fee = priority_fees_without_outliers.iter().sum::<u64>() / 
            priority_fees_without_outliers.len().max(1) as u64;
        
        let congestion_level = recent_metrics.iter()
            .map(|m| m.congestion_score)
            .sum::<f64>() / recent_metrics.len() as f64;
        
        let mut stats = self.network_stats.write().unwrap();
        *stats = NetworkStats {
            avg_success_rate,
            avg_compute_units,
            avg_priority_fee,
            congestion_level,
            last_update: Instant::now(),
        };
    }

    fn remove_outliers(&self, values: &[u32]) -> Vec<u32> {
        if values.len() < 3 {
            return values.to_vec();
        }
        
        let mean = values.iter().sum::<u32>() as f64 / values.len() as f64;
        let variance = values.iter()
            .map(|&v| (v as f64 - mean).powi(2))
            .sum::<f64>() / values.len() as f64;
        let std_dev = variance.sqrt();
        
        values.iter()
            .filter(|&&v| (v as f64 - mean).abs() <= OUTLIER_THRESHOLD * std_dev)
            .cloned()
            .collect()
    }

    fn remove_outliers_u64(&self, values: &[u64]) -> Vec<u64> {
        if values.len() < 3 {
            return values.to_vec();
        }
        
        let mean = values.iter().sum::<u64>() as f64 / values.len() as f64;
        let variance = values.iter()
            .map(|&v| (v as f64 - mean).powi(2))
            .sum::<f64>() / values.len() as f64;
        let std_dev = variance.sqrt();
        
        values.iter()
            .filter(|&&v| (v as f64 - mean).abs() <= OUTLIER_THRESHOLD * std_dev)
            .cloned()
            .collect()
    }

    async fn update_instruction_complexity_cache(&self, actual_units: u32) {
        let mut complexity_map = self.instruction_complexity_map.write().unwrap();
        
        if complexity_map.len() > 1000 {
            complexity_map.clear();
        }
    }

    pub async fn get_retry_strategy(&self, attempt: u32) -> (u64, u64) {
        let network_stats = self.network_stats.read().unwrap();
        let congestion = network_stats.congestion_level;
        
        let base_delay = if congestion > 0.8 {
            BASE_RETRY_DELAY_MS * 2
        } else {
            BASE_RETRY_DELAY_MS
        };
        
        let exponential_delay = base_delay * (2u64.pow(attempt.min(5)));
        let jittered_delay = exponential_delay + (rand::random::<u64>() % (exponential_delay / 2));
        let final_delay = jittered_delay.min(MAX_RETRY_DELAY_MS);
        
        let priority_multiplier = if congestion > 0.8 {
            1.5 + (0.3 * attempt as f64)
        } else {
            1.0 + (0.2 * attempt as f64)
        };
        
        (final_delay, (priority_multiplier * 100.0) as u64)
    }

    pub async fn should_retry(&self, error_msg: &str, attempt: u32) -> bool {
        if attempt >= 5 {
            return false;
        }
        
        let retryable_errors = [
            "BlockhashNotFound",
            "AlreadyProcessed",
            "InsufficientFunds",
            "AccountInUse",
            "TooManyAccountLocks",
            "BlockheightExceeded",
        ];
        
        retryable_errors.iter().any(|&e| error_msg.contains(e))
    }

    pub async fn get_dynamic_priority_fee_for_slot(
        &self,
        target_slot: u64,
    ) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
        let history = self.metrics_history.read().unwrap();
        
        let slot_window = 5;
        let relevant_metrics: Vec<_> = history.iter()
            .filter(|m| m.slot >= target_slot.saturating_sub(slot_window) && m.slot <= target_slot)
            .collect();
        
        if relevant_metrics.is_empty() {
            return Ok(self.network_stats.read().unwrap().avg_priority_fee);
        }
        
        let successful_fees: Vec<u64> = relevant_metrics.iter()
            .filter(|m| m.success)
            .map(|m| m.priority_fee)
            .collect();
        
        if successful_fees.is_empty() {
            let all_fees: Vec<u64> = relevant_metrics.iter()
                .map(|m| m.priority_fee)
                .collect();
            Ok(self.calculate_percentile_fee(&all_fees, 0.9))
        } else {
            Ok(self.calculate_percentile_fee(&successful_fees, PRIORITY_FEE_PERCENTILE))
        }
    }

    pub fn get_optimization_metrics(&self) -> OptimizationMetrics {
        let history = self.metrics_history.read().unwrap();
        let network_stats = self.network_stats.read().unwrap();
        
        let recent_metrics: Vec<_> = history.iter()
            .filter(|m| m.timestamp.elapsed().as_secs() < 300)
            .collect();
        
        let total_attempts = recent_metrics.len();
        let successful_attempts = recent_metrics.iter().filter(|m| m.success).count();
        let success_rate = if total_attempts > 0 {
            successful_attempts as f64 / total_attempts as f64
        } else {
            0.0
        };
        
        let avg_compute_units = if !recent_metrics.is_empty() {
            recent_metrics.iter().map(|m| m.compute_units_consumed).sum::<u32>() / recent_metrics.len() as u32
        } else {
            DEFAULT_COMPUTE_UNITS
        };
        
        let avg_priority_fee = if !recent_metrics.is_empty() {
            recent_metrics.iter().map(|m| m.priority_fee).sum::<u64>() / recent_metrics.len() as u64
        } else {
            MIN_PRIORITY_FEE_LAMPORTS
        };
        
        OptimizationMetrics {
            success_rate,
            avg_compute_units,
            avg_priority_fee,
            congestion_level: network_stats.congestion_level,
            total_attempts: total_attempts as u64,
            successful_attempts: successful_attempts as u64,
        }
    }

    pub async fn emergency_mode_fee(&self) -> u64 {
        let network_stats = self.network_stats.read().unwrap();
        let base_emergency_fee = network_stats.avg_priority_fee * 3;
        
        base_emergency_fee.min(MAX_PRIORITY_FEE_LAMPORTS)
    }

    pub fn clear_old_metrics(&self) {
        let mut history = self.metrics_history.write().unwrap();
        let cutoff_time = Instant::now() - Duration::from_secs(3600);
        
        history.retain(|m| m.timestamp > cutoff_time);
    }
}

#[derive(Debug, Clone)]
pub struct OptimizationMetrics {
    pub success_rate: f64,
    pub avg_compute_units: u32,
    pub avg_priority_fee: u64,
    pub congestion_level: f64,
    pub total_attempts: u64,
    pub successful_attempts: u64,
}

impl ComputeUnitOptimizer {
    pub async fn adaptive_compute_unit_adjustment(
        &self,
        base_units: u32,
        program_id: &Pubkey,
    ) -> u32 {
        let program_str = program_id.to_string();
        let complexity_map = self.instruction_complexity_map.read().unwrap();
        
        let historical_avg = complexity_map.get(&program_str).copied().unwrap_or(base_units);
        let network_stats = self.network_stats.read().unwrap();
        
        let adjustment_factor = match network_stats.congestion_level {
            c if c > 0.9 => 1.3,
            c if c > 0.7 => 1.15,
            c if c < 0.3 => 0.9,
            _ => 1.0,
        };
        
        let adjusted = (historical_avg as f64 * adjustment_factor) as u32;
        adjusted.clamp(MIN_COMPUTE_UNITS, MAX_COMPUTE_UNITS)
    }

    pub fn get_slot_based_multiplier(&self, current_slot: u64) -> f64 {
        let slot_in_epoch = current_slot % 432_000;
        let epoch_progress = slot_in_epoch as f64 / 432_000.0;
        
        if epoch_progress < 0.1 || epoch_progress > 0.9 {
            1.2
        } else {
            1.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_outlier_removal() {
        let optimizer = ComputeUnitOptimizer::new(
            Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string())),
            OptimizationConfig::default()
        );
        
        let values = vec![100, 105, 110, 1000, 115, 120];
        let filtered = optimizer.remove_outliers(&values);
        assert!(!filtered.contains(&1000));
    }

    #[test]
    fn test_congestion_score_calculation() {
        let optimizer = ComputeUnitOptimizer::new(
            Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string())),
            OptimizationConfig::default()
        );
        
        let score = optimizer.calculate_congestion_score();
        assert!(score >= 0.0 && score <= 1.0);
    }
}
