use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    message::Message,
    pubkey::Pubkey,
    signature::Keypair,
    transaction::Transaction,
};
use solana_transaction_status::{
    option_serializer::OptionSerializer, UiTransactionEncoding,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

const LAMPORTS_PER_SOL: u64 = 1_000_000_000;
const MAX_COMPUTE_UNITS: u32 = 1_400_000;
const BASE_COMPUTE_UNITS: u32 = 150_000;
const SIGNATURE_COST: u64 = 5_000;
const MAX_TRANSACTION_SIZE: usize = 1232;
const PRIORITY_FEE_PERCENTILE: f64 = 0.95;
const CONGESTION_THRESHOLD: f64 = 0.85;
const FEE_CACHE_TTL: Duration = Duration::from_secs(2);
const COST_HISTORY_SIZE: usize = 1000;
const MIN_PRIORITY_FEE: u64 = 1000;
const MAX_PRIORITY_FEE: u64 = 500_000;
const NETWORK_SAMPLE_SIZE: usize = 150;

#[derive(Debug, Clone)]
pub struct TransactionCostMetrics {
    pub compute_units: u32,
    pub priority_fee_per_cu: u64,
    pub total_fee: u64,
    pub transaction_size: usize,
    pub estimated_slot_latency: u64,
    pub network_congestion_score: f64,
    pub success_probability: f64,
    pub roi_threshold: f64,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
struct FeeCache {
    priority_fees: Vec<u64>,
    last_update: Instant,
    avg_compute_units: u32,
    network_congestion: f64,
}

#[derive(Debug, Clone)]
struct CostHistory {
    successful_costs: VecDeque<TransactionCostMetrics>,
    failed_costs: VecDeque<TransactionCostMetrics>,
    avg_success_fee: f64,
    avg_failure_fee: f64,
}

pub struct TransactionCostAnalyzer {
    rpc_client: Arc<RpcClient>,
    fee_cache: Arc<RwLock<FeeCache>>,
    cost_history: Arc<RwLock<CostHistory>>,
    recent_blockhashes: Arc<RwLock<VecDeque<(u64, String)>>>,
    slot_timing: Arc<RwLock<HashMap<u64, u64>>>,
}

impl TransactionCostAnalyzer {
    pub fn new(rpc_endpoint: &str) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_endpoint.to_string(),
            CommitmentConfig::confirmed(),
        ));

        Self {
            rpc_client,
            fee_cache: Arc::new(RwLock::new(FeeCache {
                priority_fees: Vec::new(),
                last_update: Instant::now(),
                avg_compute_units: BASE_COMPUTE_UNITS,
                network_congestion: 0.0,
            })),
            cost_history: Arc::new(RwLock::new(CostHistory {
                successful_costs: VecDeque::with_capacity(COST_HISTORY_SIZE),
                failed_costs: VecDeque::with_capacity(COST_HISTORY_SIZE),
                avg_success_fee: 0.0,
                avg_failure_fee: 0.0,
            })),
            recent_blockhashes: Arc::new(RwLock::new(VecDeque::with_capacity(32))),
            slot_timing: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn analyze_transaction_cost(
        &self,
        instructions: &[Instruction],
        payer: &Pubkey,
        signers_count: usize,
        expected_profit: u64,
    ) -> Result<TransactionCostMetrics, Box<dyn std::error::Error>> {
        let compute_units = self.estimate_compute_units(instructions)?;
        let priority_fee_per_cu = self.calculate_optimal_priority_fee()?;
        let transaction_size = self.calculate_transaction_size(instructions, signers_count);
        let network_congestion = self.get_network_congestion()?;
        let slot_latency = self.estimate_slot_latency()?;
        
        let base_fee = SIGNATURE_COST * signers_count as u64;
        let priority_fee = (compute_units as u64 * priority_fee_per_cu) / 1_000_000;
        let total_fee = base_fee + priority_fee;
        
        let success_probability = self.calculate_success_probability(
            priority_fee_per_cu,
            network_congestion,
            transaction_size,
        );
        
        let roi_threshold = if expected_profit > 0 {
            (expected_profit as f64 - total_fee as f64) / expected_profit as f64
        } else {
            0.0
        };

        Ok(TransactionCostMetrics {
            compute_units,
            priority_fee_per_cu,
            total_fee,
            transaction_size,
            estimated_slot_latency: slot_latency,
            network_congestion_score: network_congestion,
            success_probability,
            roi_threshold,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
        })
    }

    fn estimate_compute_units(&self, instructions: &[Instruction]) -> Result<u32, Box<dyn std::error::Error>> {
        let mut total_units = BASE_COMPUTE_UNITS;
        
        for instruction in instructions {
            let program_id = &instruction.program_id;
            let data_len = instruction.data.len();
            let accounts_len = instruction.accounts.len();
            
            let instruction_units = match program_id.to_string().as_str() {
                "11111111111111111111111111111111" => 150 + (accounts_len as u32 * 50),
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" => {
                    2_000 + (data_len as u32 * 10) + (accounts_len as u32 * 100)
                }
                "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL" => {
                    3_000 + (accounts_len as u32 * 150)
                }
                "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8" => {
                    match data_len {
                        1..=8 => 15_000,
                        9..=32 => 25_000,
                        33..=64 => 35_000,
                        _ => 50_000,
                    }
                }
                "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc" => {
                    20_000 + (accounts_len as u32 * 200)
                }
                "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4" => {
                    30_000 + (accounts_len as u32 * 250)
                }
                _ => {
                    5_000 + (data_len as u32 * 15) + (accounts_len as u32 * 200)
                }
            };
            
            total_units += instruction_units;
        }
        
        total_units = total_units.min(MAX_COMPUTE_UNITS);
        
        if let Ok(mut cache) = self.fee_cache.write() {
            cache.avg_compute_units = (cache.avg_compute_units * 9 + total_units) / 10;
        }
        
        Ok(total_units)
    }

    fn calculate_optimal_priority_fee(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let mut cache = self.fee_cache.write().unwrap();
        
        if cache.last_update.elapsed() < FEE_CACHE_TTL && !cache.priority_fees.is_empty() {
            let idx = (cache.priority_fees.len() as f64 * PRIORITY_FEE_PERCENTILE) as usize;
            return Ok(cache.priority_fees[idx.min(cache.priority_fees.len() - 1)]);
        }
        
        let recent_fees = self.fetch_recent_priority_fees()?;
        if recent_fees.is_empty() {
            return Ok(MIN_PRIORITY_FEE);
        }
        
        let mut sorted_fees = recent_fees.clone();
        sorted_fees.sort_unstable();
        
        let percentile_idx = (sorted_fees.len() as f64 * PRIORITY_FEE_PERCENTILE) as usize;
        let base_fee = sorted_fees[percentile_idx.min(sorted_fees.len() - 1)];
        
        let congestion_multiplier = 1.0 + (cache.network_congestion * 0.5);
        let history_multiplier = self.get_historical_success_multiplier();
        
        let optimal_fee = (base_fee as f64 * congestion_multiplier * history_multiplier) as u64;
        let final_fee = optimal_fee.clamp(MIN_PRIORITY_FEE, MAX_PRIORITY_FEE);
        
        cache.priority_fees = sorted_fees;
        cache.last_update = Instant::now();
        
        Ok(final_fee)
    }

    fn fetch_recent_priority_fees(&self) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
        let recent_blocks = self.rpc_client.get_blocks(
            self.rpc_client.get_slot()?.saturating_sub(NETWORK_SAMPLE_SIZE as u64),
            Some(self.rpc_client.get_slot()?),
        )?;
        
        let mut priority_fees = Vec::new();
        
        for &slot in recent_blocks.iter().rev().take(20) {
            if let Ok(block) = self.rpc_client.get_block(slot) {
                for tx in block.transactions {
                    if let Some(meta) = tx.meta {
                        if meta.err.is_none() {
                            if let Some(fee) = meta.fee {
                                if let OptionSerializer::Some(inner_instructions) = meta.inner_instructions {
                                    for inner in inner_instructions {
                                        for instruction in inner.instructions {
                                            if let Ok(decoded) = bs58::decode(&instruction.data).into_vec() {
                                                if decoded.len() >= 8 && decoded[0] == 3 {
                                                    let priority_fee = u64::from_le_bytes([
                                                        decoded[1], decoded[2], decoded[3], decoded[4],
                                                        decoded[5], decoded[6], decoded[7], decoded[8],
                                                    ]);
                                                    priority_fees.push(priority_fee);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        
        if priority_fees.is_empty() {
            priority_fees.push(MIN_PRIORITY_FEE);
        }
        
        Ok(priority_fees)
    }

    fn calculate_transaction_size(&self, instructions: &[Instruction], signers_count: usize) -> usize {
        let mut size = 0;
        
        size += 1 + 64 * signers_count;
        size += 32;
        size += 1;
        
        let mut unique_accounts = std::collections::HashSet::new();
        for instruction in instructions {
            unique_accounts.insert(instruction.program_id);
            for account in &instruction.accounts {
                unique_accounts.insert(account.pubkey);
            }
        }
        
        size += 1 + unique_accounts.len() * 32;
        size += 1;
        
        for instruction in instructions {
            size += 1;
            size += 1 + instruction.accounts.len();
            size += 1 + instruction.data.len();
        }
        
        size.min(MAX_TRANSACTION_SIZE)
    }

    fn get_network_congestion(&self) -> Result<f64, Box<dyn std::error::Error>> {
        let current_slot = self.rpc_client.get_slot()?;
        let slot_timing = self.slot_timing.read().unwrap();
        
        let recent_slots: Vec<_> = slot_timing
            .iter()
            .filter(|(slot, _)| current_slot.saturating_sub(**slot) < 100)
            .map(|(_, time)| *time)
            .collect();
        
        if recent_slots.len() < 10 {
            return Ok(0.5);
        }
        
        let avg_time = recent_slots.iter().sum::<u64>() / recent_slots.len() as u64;
        let expected_time = 400;
        
        let congestion = ((avg_time as f64 / expected_time as f64) - 1.0)
            .max(0.0)
            .min(1.0);
        
        if let Ok(mut cache) = self.fee_cache.write() {
            cache.network_congestion = congestion;
        }
        
        Ok(congestion)
    }

    fn estimate_slot_latency(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let current_slot = self.rpc_client.get_slot()?;
        let mut slot_timing = self.slot_timing.write().unwrap();
        
                slot_timing.insert(current_slot, SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64);
        
        if slot_timing.len() > 200 {
            let oldest = slot_timing.keys().min().copied().unwrap_or(0);
            slot_timing.remove(&oldest);
        }
        
        let recent_latencies: Vec<u64> = slot_timing
            .iter()
            .filter(|(slot, _)| current_slot.saturating_sub(**slot) < 32)
            .map(|(_, time)| *time)
            .collect();
        
        if recent_latencies.len() < 2 {
            return Ok(400);
        }
        
        let mut deltas = Vec::new();
        for window in recent_latencies.windows(2) {
            deltas.push(window[1].saturating_sub(window[0]));
        }
        
        let avg_latency = if deltas.is_empty() {
            400
        } else {
            deltas.iter().sum::<u64>() / deltas.len() as u64
        };
        
        Ok(avg_latency.clamp(350, 600))
    }

    fn calculate_success_probability(
        &self,
        priority_fee: u64,
        congestion: f64,
        tx_size: usize,
    ) -> f64 {
        let fee_score = (priority_fee as f64 / MAX_PRIORITY_FEE as f64).min(1.0);
        let congestion_penalty = 1.0 - (congestion * 0.6);
        let size_penalty = 1.0 - (tx_size as f64 / MAX_TRANSACTION_SIZE as f64) * 0.3;
        
        let history = self.cost_history.read().unwrap();
        let historical_success_rate = if history.successful_costs.is_empty() {
            0.7
        } else {
            let total = history.successful_costs.len() + history.failed_costs.len();
            history.successful_costs.len() as f64 / total as f64
        };
        
        let base_probability = fee_score * 0.4 + 
                              congestion_penalty * 0.3 + 
                              size_penalty * 0.2 + 
                              historical_success_rate * 0.1;
        
        base_probability.clamp(0.1, 0.95)
    }

    fn get_historical_success_multiplier(&self) -> f64 {
        let history = self.cost_history.read().unwrap();
        
        if history.successful_costs.len() < 10 {
            return 1.0;
        }
        
        let recent_success: Vec<_> = history.successful_costs
            .iter()
            .rev()
            .take(50)
            .map(|m| m.priority_fee_per_cu)
            .collect();
        
        let recent_failed: Vec<_> = history.failed_costs
            .iter()
            .rev()
            .take(50)
            .map(|m| m.priority_fee_per_cu)
            .collect();
        
        if recent_success.is_empty() || recent_failed.is_empty() {
            return 1.0;
        }
        
        let avg_success_fee = recent_success.iter().sum::<u64>() / recent_success.len() as u64;
        let avg_failed_fee = recent_failed.iter().sum::<u64>() / recent_failed.len() as u64;
        
        if avg_failed_fee > avg_success_fee {
            1.1 + ((avg_failed_fee - avg_success_fee) as f64 / avg_success_fee as f64).min(0.3)
        } else {
            1.0
        }
    }

    pub fn update_transaction_result(&self, metrics: TransactionCostMetrics, success: bool) {
        let mut history = self.cost_history.write().unwrap();
        
        if success {
            history.successful_costs.push_back(metrics);
            if history.successful_costs.len() > COST_HISTORY_SIZE {
                history.successful_costs.pop_front();
            }
            
            history.avg_success_fee = history.successful_costs
                .iter()
                .map(|m| m.total_fee as f64)
                .sum::<f64>() / history.successful_costs.len() as f64;
        } else {
            history.failed_costs.push_back(metrics);
            if history.failed_costs.len() > COST_HISTORY_SIZE {
                history.failed_costs.pop_front();
            }
            
            history.avg_failure_fee = history.failed_costs
                .iter()
                .map(|m| m.total_fee as f64)
                .sum::<f64>() / history.failed_costs.len() as f64;
        }
    }

    pub fn get_dynamic_compute_budget_instructions(&self, metrics: &TransactionCostMetrics) -> Vec<Instruction> {
        vec![
            ComputeBudgetInstruction::set_compute_unit_limit(metrics.compute_units),
            ComputeBudgetInstruction::set_compute_unit_price(metrics.priority_fee_per_cu),
        ]
    }

    pub fn should_execute_transaction(&self, metrics: &TransactionCostMetrics, min_profit: u64) -> bool {
        if metrics.roi_threshold < 0.2 {
            return false;
        }
        
        if metrics.success_probability < 0.3 {
            return false;
        }
        
        if metrics.network_congestion_score > CONGESTION_THRESHOLD && metrics.priority_fee_per_cu < 50_000 {
            return false;
        }
        
        let expected_value = (min_profit as f64 * metrics.success_probability) - metrics.total_fee as f64;
        
        expected_value > 0.0
    }

    pub fn get_adaptive_priority_fee(&self, urgency_factor: f64) -> Result<u64, Box<dyn std::error::Error>> {
        let base_fee = self.calculate_optimal_priority_fee()?;
        let congestion = self.get_network_congestion()?;
        
        let urgency_multiplier = 1.0 + (urgency_factor * 0.5);
        let congestion_boost = if congestion > 0.7 { 1.2 } else { 1.0 };
        
        let history = self.cost_history.read().unwrap();
        let recent_success_rate = if history.successful_costs.len() >= 10 {
            let recent_10 = history.successful_costs.iter().rev().take(10).count();
            recent_10 as f64 / 10.0
        } else {
            0.7
        };
        
        let success_adjustment = if recent_success_rate < 0.5 { 1.3 } else { 1.0 };
        
        let adaptive_fee = (base_fee as f64 * urgency_multiplier * congestion_boost * success_adjustment) as u64;
        
        Ok(adaptive_fee.clamp(MIN_PRIORITY_FEE, MAX_PRIORITY_FEE))
    }

    pub fn estimate_confirmation_time(&self, priority_fee: u64) -> u64 {
        let cache = self.fee_cache.read().unwrap();
        
        if cache.priority_fees.is_empty() {
            return 2000;
        }
        
        let position = cache.priority_fees
            .iter()
            .position(|&fee| fee >= priority_fee)
            .unwrap_or(cache.priority_fees.len());
        
        let percentile = position as f64 / cache.priority_fees.len() as f64;
        
        let base_time = 400;
        let max_additional_time = 3600;
        
        (base_time as f64 + (1.0 - percentile) * max_additional_time as f64) as u64
    }

    pub fn get_cost_efficiency_score(&self, metrics: &TransactionCostMetrics) -> f64 {
        let fee_efficiency = 1.0 - (metrics.total_fee as f64 / (metrics.roi_threshold * 1_000_000.0 + 1.0)).min(1.0);
        let compute_efficiency = 1.0 - (metrics.compute_units as f64 / MAX_COMPUTE_UNITS as f64);
        let size_efficiency = 1.0 - (metrics.transaction_size as f64 / MAX_TRANSACTION_SIZE as f64);
        let timing_efficiency = 1.0 - (metrics.estimated_slot_latency as f64 / 1000.0).min(1.0);
        
        (fee_efficiency * 0.4 + 
         compute_efficiency * 0.2 + 
         size_efficiency * 0.2 + 
         timing_efficiency * 0.2) * metrics.success_probability
    }

    pub fn optimize_instruction_order(&self, instructions: Vec<Instruction>) -> Vec<Instruction> {
        let mut optimized = instructions;
        
        optimized.sort_by_key(|inst| {
            let complexity = inst.data.len() + inst.accounts.len() * 10;
            
            match inst.program_id.to_string().as_str() {
                "ComputeBudget111111111111111111111111111111" => 0,
                "11111111111111111111111111111111" => 1,
                _ => 2 + complexity / 100,
            }
        });
        
        optimized
    }

    pub fn batch_cost_analysis(
        &self,
        transaction_groups: Vec<Vec<Instruction>>,
        payer: &Pubkey,
        expected_profits: Vec<u64>,
    ) -> Result<Vec<(TransactionCostMetrics, f64)>, Box<dyn std::error::Error>> {
        let mut results = Vec::new();
        
        for (instructions, expected_profit) in transaction_groups.iter().zip(expected_profits.iter()) {
            let metrics = self.analyze_transaction_cost(instructions, payer, 1, *expected_profit)?;
            let efficiency = self.get_cost_efficiency_score(&metrics);
            results.push((metrics, efficiency));
        }
        
        results.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        
        Ok(results)
    }

    pub fn get_network_stats(&self) -> (f64, u64, f64) {
        let cache = self.fee_cache.read().unwrap();
        let history = self.cost_history.read().unwrap();
        
        (
            cache.network_congestion,
            cache.avg_compute_units as u64,
            history.avg_success_fee,
        )
    }

    pub fn update_blockhash_cache(&self, slot: u64, blockhash: String) {
        let mut cache = self.recent_blockhashes.write().unwrap();
        cache.push_back((slot, blockhash));
        
        if cache.len() > 32 {
            cache.pop_front();
        }
    }

    pub fn estimate_total_cost_with_retries(
        &self,
        metrics: &TransactionCostMetrics,
        max_retries: u32,
    ) -> u64 {
        let single_attempt_cost = metrics.total_fee;
        let failure_probability = 1.0 - metrics.success_probability;
        
        let mut expected_attempts = 0.0;
        for i in 0..max_retries {
            expected_attempts += (i + 1) as f64 * metrics.success_probability * failure_probability.powi(i as i32);
        }
        expected_attempts += (max_retries + 1) as f64 * failure_probability.powi(max_retries as i32);
        
        (single_attempt_cost as f64 * expected_attempts) as u64
    }

    pub fn calculate_mev_adjusted_fee(
        &self,
        base_metrics: &TransactionCostMetrics,
        competitor_count: usize,
        opportunity_value: u64,
    ) -> u64 {
        let competition_factor = 1.0 + (competitor_count as f64 * 0.15).min(2.0);
        let value_factor = (opportunity_value as f64 / LAMPORTS_PER_SOL as f64).sqrt().max(1.0);
        
        let adjusted_fee = (base_metrics.priority_fee_per_cu as f64 * competition_factor * value_factor) as u64;
        
        adjusted_fee.clamp(base_metrics.priority_fee_per_cu, MAX_PRIORITY_FEE)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::system_instruction;

    #[test]
    fn test_transaction_cost_analysis() {
        let analyzer = TransactionCostAnalyzer::new("https://api.mainnet-beta.solana.com");
        let instructions = vec![
            system_instruction::transfer(&Pubkey::new_unique(), &Pubkey::new_unique(), 1000000),
        ];
        
        let result = analyzer.analyze_transaction_cost(&instructions, &Pubkey::new_unique(), 1, 10000000);
        assert!(result.is_ok());
        
        let metrics = result.unwrap();
        assert!(metrics.compute_units > 0);
        assert!(metrics.total_fee > 0);
        assert!(metrics.success_probability > 0.0 && metrics.success_probability <= 1.0);
    }
}

