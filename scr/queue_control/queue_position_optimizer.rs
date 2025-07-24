use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    clock::Slot,
    commitment_config::{CommitmentConfig, CommitmentLevel},
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
use tokio::sync::mpsc;

const SLOT_DURATION_MS: u64 = 400;
const LEADER_SCHEDULE_LOOKAHEAD: u64 = 12;
const MAX_PRIORITY_FEE_LAMPORTS: u64 = 50_000_000;
const MIN_PRIORITY_FEE_LAMPORTS: u64 = 10_000;
const COMPUTE_UNIT_LIMIT: u32 = 1_400_000;
const PRIORITY_FEE_PERCENTILE: f64 = 0.95;
const CONGESTION_THRESHOLD: f64 = 0.85;
const SLOT_TIMING_BUFFER_MS: u64 = 50;
const MAX_RETRIES: u32 = 3;
const POSITION_DECAY_FACTOR: f64 = 0.95;
const NETWORK_LATENCY_MS: u64 = 20;
const JITO_TIP_PERCENTAGE: f64 = 0.001;
const MIN_JITO_TIP_LAMPORTS: u64 = 10_000;

#[derive(Clone, Debug)]
pub struct QueuePosition {
    slot: Slot,
    position: u64,
    priority_fee: u64,
    compute_units: u32,
    timestamp: Instant,
    success_rate: f64,
}

#[derive(Clone, Debug)]
pub struct LeaderInfo {
    pubkey: Pubkey,
    slots: Vec<Slot>,
    historical_success_rate: f64,
    avg_priority_fee: u64,
    network_stake_percentage: f64,
}

#[derive(Clone, Debug)]
pub struct NetworkMetrics {
    tps: f64,
    avg_slot_time: Duration,
    congestion_level: f64,
    recent_priority_fees: VecDeque<u64>,
    leader_performance: HashMap<Pubkey, LeaderInfo>,
}

#[derive(Clone, Debug)]
pub struct OptimizationParams {
    target_position: u64,
    max_priority_fee: u64,
    min_priority_fee: u64,
    compute_budget: u32,
    retry_strategy: RetryStrategy,
    use_jito_bundles: bool,
    jito_tip_lamports: u64,
}

#[derive(Clone, Debug)]
pub enum RetryStrategy {
    Exponential { base_fee_multiplier: f64 },
    Linear { fee_increment: u64 },
    Adaptive { success_threshold: f64 },
}

pub struct QueuePositionOptimizer {
    rpc_client: Arc<RpcClient>,
    network_metrics: Arc<RwLock<NetworkMetrics>>,
    position_history: Arc<RwLock<VecDeque<QueuePosition>>>,
    optimization_params: Arc<RwLock<OptimizationParams>>,
    leader_schedule: Arc<RwLock<HashMap<Slot, Pubkey>>>,
    metrics_sender: mpsc::Sender<NetworkMetrics>,
}

impl QueuePositionOptimizer {
    pub fn new(
        rpc_endpoint: &str,
        metrics_sender: mpsc::Sender<NetworkMetrics>,
    ) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_endpoint.to_string(),
            CommitmentConfig {
                commitment: CommitmentLevel::Processed,
            },
        ));

        Self {
            rpc_client,
            network_metrics: Arc::new(RwLock::new(NetworkMetrics {
                tps: 2500.0,
                avg_slot_time: Duration::from_millis(SLOT_DURATION_MS),
                congestion_level: 0.5,
                recent_priority_fees: VecDeque::with_capacity(100),
                leader_performance: HashMap::new(),
            })),
            position_history: Arc::new(RwLock::new(VecDeque::with_capacity(1000))),
            optimization_params: Arc::new(RwLock::new(OptimizationParams {
                target_position: 1,
                max_priority_fee: MAX_PRIORITY_FEE_LAMPORTS,
                min_priority_fee: MIN_PRIORITY_FEE_LAMPORTS,
                compute_budget: COMPUTE_UNIT_LIMIT,
                retry_strategy: RetryStrategy::Adaptive { success_threshold: 0.7 },
                use_jito_bundles: true,
                jito_tip_lamports: MIN_JITO_TIP_LAMPORTS,
            })),
            leader_schedule: Arc::new(RwLock::new(HashMap::new())),
            metrics_sender,
        }
    }

    pub async fn optimize_transaction_position(
        &self,
        transaction: &Transaction,
        target_slot: Slot,
        profit_estimate: u64,
    ) -> Result<OptimizedTransaction, Box<dyn std::error::Error>> {
        let current_slot = self.get_current_slot().await?;
        let slot_delta = target_slot.saturating_sub(current_slot);
        
        if slot_delta > LEADER_SCHEDULE_LOOKAHEAD {
            return Err("Target slot too far in future".into());
        }

        let leader = self.get_slot_leader(target_slot).await?;
        let network_metrics = self.network_metrics.read().unwrap().clone();
        let optimization_params = self.optimization_params.read().unwrap().clone();

        let priority_fee = self.calculate_optimal_priority_fee(
            &network_metrics,
            &leader,
            profit_estimate,
            slot_delta,
        );

        let compute_units = self.calculate_optimal_compute_units(
            transaction,
            &network_metrics,
        );

        let timing_offset = self.calculate_slot_timing_offset(
            target_slot,
            current_slot,
            &network_metrics,
        );

        let jito_tip = if optimization_params.use_jito_bundles {
            self.calculate_jito_tip(profit_estimate, priority_fee)
        } else {
            0
        };

        Ok(OptimizedTransaction {
            priority_fee,
            compute_units,
            timing_offset,
            retry_count: 0,
            jito_tip,
            target_slot,
            expected_position: self.estimate_queue_position(priority_fee, &network_metrics),
        })
    }

    pub async fn update_network_metrics(&self) -> Result<(), Box<dyn std::error::Error>> {
        let slot = self.get_current_slot().await?;
        let performance_samples = self.rpc_client.get_recent_performance_samples(Some(60))?;
        
        let mut metrics = self.network_metrics.write().unwrap();
        
        if !performance_samples.is_empty() {
            let recent_samples = &performance_samples[..performance_samples.len().min(10)];
            let avg_tps = recent_samples.iter()
                .map(|s| s.num_transactions as f64 / s.sample_period_secs as f64)
                .sum::<f64>() / recent_samples.len() as f64;
            
            metrics.tps = avg_tps;
            
            let avg_slot_ms = recent_samples.iter()
                .map(|s| (s.sample_period_secs * 1000) as u64 / s.num_slots)
                .sum::<u64>() / recent_samples.len() as u64;
            
            metrics.avg_slot_time = Duration::from_millis(avg_slot_ms);
        }

        let recent_fees = self.fetch_recent_priority_fees().await?;
        metrics.recent_priority_fees.extend(recent_fees.iter().cloned());
        while metrics.recent_priority_fees.len() > 100 {
            metrics.recent_priority_fees.pop_front();
        }

        metrics.congestion_level = self.calculate_congestion_level(&metrics);

        let _ = self.metrics_sender.try_send(metrics.clone());

        Ok(())
    }

    pub async fn adjust_for_failed_transaction(
        &self,
        opt_tx: &mut OptimizedTransaction,
        error: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        opt_tx.retry_count += 1;
        
        if opt_tx.retry_count > MAX_RETRIES {
            return Err("Max retries exceeded".into());
        }

        let params = self.optimization_params.read().unwrap().clone();
        
        match params.retry_strategy {
            RetryStrategy::Exponential { base_fee_multiplier } => {
                opt_tx.priority_fee = (opt_tx.priority_fee as f64 * 
                    base_fee_multiplier.powf(opt_tx.retry_count as f64)) as u64;
            }
            RetryStrategy::Linear { fee_increment } => {
                opt_tx.priority_fee += fee_increment * opt_tx.retry_count as u64;
            }
            RetryStrategy::Adaptive { success_threshold } => {
                let history = self.position_history.read().unwrap();
                let recent_success_rate = self.calculate_recent_success_rate(&history);
                
                if recent_success_rate < success_threshold {
                    let fee_multiplier = 1.0 + (success_threshold - recent_success_rate);
                    opt_tx.priority_fee = (opt_tx.priority_fee as f64 * fee_multiplier) as u64;
                }
            }
        }

        opt_tx.priority_fee = opt_tx.priority_fee.min(params.max_priority_fee);

        if error.contains("compute") || error.contains("exceeded") {
            opt_tx.compute_units = (opt_tx.compute_units as f64 * 1.2) as u32;
            opt_tx.compute_units = opt_tx.compute_units.min(COMPUTE_UNIT_LIMIT);
        }

        Ok(())
    }

    pub fn build_optimized_instructions(
        &self,
        opt_tx: &OptimizedTransaction,
    ) -> Vec<Instruction> {
        let mut instructions = Vec::new();

        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(
            opt_tx.compute_units,
        ));

        let priority_fee_microlamports = (opt_tx.priority_fee * 1_000_000) / opt_tx.compute_units as u64;
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(
            priority_fee_microlamports,
        ));

        instructions
    }

    async fn get_current_slot(&self) -> Result<Slot, Box<dyn std::error::Error>> {
        Ok(self.rpc_client.get_slot()?)
    }

    async fn get_slot_leader(&self, slot: Slot) -> Result<LeaderInfo, Box<dyn std::error::Error>> {
        let mut schedule = self.leader_schedule.write().unwrap();
        
        if !schedule.contains_key(&slot) {
            self.update_leader_schedule().await?;
        }

        let leader_pubkey = schedule.get(&slot)
            .ok_or("Leader not found for slot")?
            .clone();

        let metrics = self.network_metrics.read().unwrap();
        let leader_info = metrics.leader_performance.get(&leader_pubkey)
            .cloned()
            .unwrap_or_else(|| LeaderInfo {
                pubkey: leader_pubkey,
                slots: vec![slot],
                historical_success_rate: 0.7,
                avg_priority_fee: MIN_PRIORITY_FEE_LAMPORTS * 10,
                network_stake_percentage: 0.01,
            });

        Ok(leader_info)
    }

    async fn update_leader_schedule(&self) -> Result<(), Box<dyn std::error::Error>> {
        let current_slot = self.get_current_slot().await?;
        let epoch_info = self.rpc_client.get_epoch_info()?;
        let slot_index = (current_slot - epoch_info.absolute_slot) as usize;
        let leader_schedule = self.rpc_client.get_leader_schedule(Some(current_slot))?
            .ok_or("Failed to get leader schedule")?;

        let mut schedule = self.leader_schedule.write().unwrap();
        schedule.clear();

        for (leader_str, slots) in leader_schedule.iter() {
            let leader_pubkey = leader_str.parse::<Pubkey>()?;
            for &leader_slot_index in slots.iter() {
                if leader_slot_index >= slot_index {
                    let slot = epoch_info.absolute_slot + leader_slot_index as u64;
                    schedule.insert(slot, leader_pubkey);
                }
            }
        }

        Ok(())
    }

    fn calculate_optimal_priority_fee(
        &self,
        metrics: &NetworkMetrics,
        leader: &LeaderInfo,
        profit_estimate: u64,
        slot_delta: u64,
    ) -> u64 {
        let base_fee = if metrics.recent_priority_fees.is_empty() {
            MIN_PRIORITY_FEE_LAMPORTS * 10
        } else {
            let mut fees: Vec<u64> = metrics.recent_priority_fees.iter().cloned().collect();
            fees.sort_unstable();
            let percentile_index = (fees.len() as f64 * PRIORITY_FEE_PERCENTILE) as usize;
            fees[percentile_index.min(fees.len() - 1)]
        };

        let congestion_multiplier = 1.0 + metrics.congestion_level.powf(2.0);
        let leader_multiplier = 1.0 + (1.0 - leader.historical_success_rate) * 0.5;
        let urgency_multiplier = (1.0 / (slot_delta as f64 + 1.0)).sqrt() + 1.0;
                let profit_multiplier = ((profit_estimate as f64 / 1_000_000_000.0).ln() + 1.0).max(1.0);

        let calculated_fee = (base_fee as f64 
            * congestion_multiplier 
            * leader_multiplier 
            * urgency_multiplier
            * profit_multiplier) as u64;

        calculated_fee
            .max(self.optimization_params.read().unwrap().min_priority_fee)
            .min(self.optimization_params.read().unwrap().max_priority_fee)
    }

    fn calculate_optimal_compute_units(
        &self,
        transaction: &Transaction,
        metrics: &NetworkMetrics,
    ) -> u32 {
        let base_compute = (transaction.message.instructions.len() as u32 * 200_000)
            .max(400_000);

        let congestion_buffer = if metrics.congestion_level > CONGESTION_THRESHOLD {
            (base_compute as f64 * 0.2) as u32
        } else {
            (base_compute as f64 * 0.1) as u32
        };

        (base_compute + congestion_buffer).min(COMPUTE_UNIT_LIMIT)
    }

    fn calculate_slot_timing_offset(
        &self,
        target_slot: Slot,
        current_slot: Slot,
        metrics: &NetworkMetrics,
    ) -> Duration {
        let slots_until_target = target_slot.saturating_sub(current_slot);
        let base_offset = metrics.avg_slot_time * slots_until_target as u32;
        
        let jitter_reduction = Duration::from_millis(
            (SLOT_TIMING_BUFFER_MS as f64 * (1.0 - metrics.congestion_level)) as u64
        );

        base_offset.saturating_sub(Duration::from_millis(NETWORK_LATENCY_MS + SLOT_TIMING_BUFFER_MS))
            .saturating_add(jitter_reduction)
    }

    fn calculate_jito_tip(&self, profit_estimate: u64, priority_fee: u64) -> u64 {
        let percentage_tip = (profit_estimate as f64 * JITO_TIP_PERCENTAGE) as u64;
        let dynamic_tip = priority_fee / 10;
        
        percentage_tip
            .max(dynamic_tip)
            .max(MIN_JITO_TIP_LAMPORTS)
            .min(profit_estimate / 100)
    }

    fn estimate_queue_position(&self, priority_fee: u64, metrics: &NetworkMetrics) -> u64 {
        if metrics.recent_priority_fees.is_empty() {
            return 10;
        }

        let mut sorted_fees: Vec<u64> = metrics.recent_priority_fees.iter().cloned().collect();
        sorted_fees.sort_unstable_by(|a, b| b.cmp(a));

        let position = sorted_fees.iter().position(|&fee| priority_fee >= fee).unwrap_or(sorted_fees.len());
        let normalized_position = (position as f64 / sorted_fees.len() as f64 * 100.0) as u64;

        normalized_position.max(1)
    }

    async fn fetch_recent_priority_fees(&self) -> Result<Vec<u64>, Box<dyn std::error::Error>> {
        let recent_fees = self.rpc_client
            .get_recent_prioritization_fees(&[])?
            .into_iter()
            .map(|fee| fee.prioritization_fee)
            .filter(|&fee| fee > 0)
            .collect();

        Ok(recent_fees)
    }

    fn calculate_congestion_level(&self, metrics: &NetworkMetrics) -> f64 {
        let tps_ratio = (metrics.tps / 3000.0).min(1.0);
        let slot_time_ratio = (SLOT_DURATION_MS as f64 / metrics.avg_slot_time.as_millis() as f64).min(1.0);
        
        let fee_pressure = if !metrics.recent_priority_fees.is_empty() {
            let avg_fee = metrics.recent_priority_fees.iter().sum::<u64>() / metrics.recent_priority_fees.len() as u64;
            (avg_fee as f64 / MAX_PRIORITY_FEE_LAMPORTS as f64).min(1.0)
        } else {
            0.5
        };

        (tps_ratio * 0.4 + slot_time_ratio * 0.3 + fee_pressure * 0.3).min(1.0)
    }

    fn calculate_recent_success_rate(&self, history: &VecDeque<QueuePosition>) -> f64 {
        if history.is_empty() {
            return 0.7;
        }

        let recent_window = 50.min(history.len());
        let recent_positions: Vec<&QueuePosition> = history.iter().take(recent_window).collect();
        
        let success_count = recent_positions.iter()
            .filter(|pos| pos.position <= 10)
            .count();

        success_count as f64 / recent_window as f64
    }

    pub async fn record_transaction_result(
        &self,
        opt_tx: &OptimizedTransaction,
        success: bool,
        actual_slot: Slot,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let position = if success {
            self.estimate_queue_position(opt_tx.priority_fee, &self.network_metrics.read().unwrap())
        } else {
            u64::MAX
        };

        let queue_position = QueuePosition {
            slot: actual_slot,
            position,
            priority_fee: opt_tx.priority_fee,
            compute_units: opt_tx.compute_units,
            timestamp: Instant::now(),
            success_rate: if success { 1.0 } else { 0.0 },
        };

        let mut history = self.position_history.write().unwrap();
        history.push_front(queue_position);
        
        while history.len() > 1000 {
            history.pop_back();
        }

        self.update_leader_performance(actual_slot, success, opt_tx.priority_fee).await?;

        Ok(())
    }

    async fn update_leader_performance(
        &self,
        slot: Slot,
        success: bool,
        priority_fee: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let leader_pubkey = self.leader_schedule.read().unwrap()
            .get(&slot)
            .cloned();

        if let Some(leader) = leader_pubkey {
            let mut metrics = self.network_metrics.write().unwrap();
            
            let leader_info = metrics.leader_performance
                .entry(leader)
                .or_insert_with(|| LeaderInfo {
                    pubkey: leader,
                    slots: Vec::new(),
                    historical_success_rate: 0.7,
                    avg_priority_fee: priority_fee,
                    network_stake_percentage: 0.01,
                });

            leader_info.slots.push(slot);
            if leader_info.slots.len() > 100 {
                leader_info.slots.remove(0);
            }

            let success_weight = if success { 1.0 } else { 0.0 };
            leader_info.historical_success_rate = 
                leader_info.historical_success_rate * 0.95 + success_weight * 0.05;

            leader_info.avg_priority_fee = 
                (leader_info.avg_priority_fee as f64 * 0.9 + priority_fee as f64 * 0.1) as u64;
        }

        Ok(())
    }

    pub async fn get_optimal_submission_time(
        &self,
        target_slot: Slot,
    ) -> Result<Instant, Box<dyn std::error::Error>> {
        let current_slot = self.get_current_slot().await?;
        let metrics = self.network_metrics.read().unwrap();
        
        let slots_until_target = target_slot.saturating_sub(current_slot);
        let time_until_slot = metrics.avg_slot_time * slots_until_target as u32;
        
        let network_latency_buffer = Duration::from_millis(NETWORK_LATENCY_MS);
        let congestion_buffer = Duration::from_millis(
            (SLOT_TIMING_BUFFER_MS as f64 * metrics.congestion_level) as u64
        );
        
        let optimal_submission_offset = time_until_slot
            .saturating_sub(network_latency_buffer)
            .saturating_sub(congestion_buffer);

        Ok(Instant::now() + optimal_submission_offset)
    }

    pub fn apply_dynamic_adjustments(
        &self,
        opt_tx: &mut OptimizedTransaction,
        market_conditions: &MarketConditions,
    ) {
        let params = self.optimization_params.read().unwrap();
        
        if market_conditions.volatility > 0.8 {
            opt_tx.priority_fee = (opt_tx.priority_fee as f64 * 1.3) as u64;
        }

        if market_conditions.opportunity_count > 10 {
            opt_tx.priority_fee = (opt_tx.priority_fee as f64 * 1.2) as u64;
        }

        if market_conditions.competitor_activity > 0.7 {
            opt_tx.priority_fee = (opt_tx.priority_fee as f64 * 1.15) as u64;
            if params.use_jito_bundles {
                opt_tx.jito_tip = (opt_tx.jito_tip as f64 * 1.5) as u64;
            }
        }

        opt_tx.priority_fee = opt_tx.priority_fee
            .max(params.min_priority_fee)
            .min(params.max_priority_fee);
    }

    pub async fn preflight_optimization_check(
        &self,
        opt_tx: &OptimizedTransaction,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        let current_slot = self.get_current_slot().await?;
        
        if opt_tx.target_slot <= current_slot {
            return Ok(false);
        }

        let params = self.optimization_params.read().unwrap();
        
        if opt_tx.priority_fee > params.max_priority_fee {
            return Ok(false);
        }

        if opt_tx.compute_units > COMPUTE_UNIT_LIMIT {
            return Ok(false);
        }

        let metrics = self.network_metrics.read().unwrap();
        if metrics.congestion_level > 0.95 && opt_tx.expected_position > 50 {
            return Ok(false);
        }

        Ok(true)
    }
}

#[derive(Clone, Debug)]
pub struct OptimizedTransaction {
    pub priority_fee: u64,
    pub compute_units: u32,
    pub timing_offset: Duration,
    pub retry_count: u32,
    pub jito_tip: u64,
    pub target_slot: Slot,
    pub expected_position: u64,
}

#[derive(Clone, Debug)]
pub struct MarketConditions {
    pub volatility: f64,
    pub opportunity_count: u32,
    pub competitor_activity: f64,
}

impl Default for MarketConditions {
    fn default() -> Self {
        Self {
            volatility: 0.5,
            opportunity_count: 5,
            competitor_activity: 0.5,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;

    #[tokio::test]
    async fn test_optimizer_initialization() {
        let (tx, _rx) = mpsc::channel(100);
        let optimizer = QueuePositionOptimizer::new("https://api.mainnet-beta.solana.com", tx);
        assert!(optimizer.optimization_params.read().unwrap().use_jito_bundles);
    }
}

