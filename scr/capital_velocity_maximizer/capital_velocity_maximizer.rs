use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::Transaction,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::RwLock;

const MAX_CAPITAL_PER_TRADE: u64 = 1_000_000_000_000; // 1000 SOL in lamports
const MIN_PROFIT_THRESHOLD_BPS: u64 = 15; // 0.15% minimum profit
const MAX_SLIPPAGE_BPS: u64 = 50; // 0.5% max slippage
const PRIORITY_FEE_LAMPORTS: u64 = 50_000;
const MAX_COMPUTE_UNITS: u32 = 1_400_000;
const CAPITAL_RECYCLE_TIME_MS: u64 = 100;
const MAX_CONCURRENT_TRADES: usize = 10;
const VELOCITY_WINDOW_MS: u64 = 60_000;

#[derive(Clone, Debug)]
pub struct CapitalAllocation {
    pub amount: u64,
    pub locked_until: Instant,
    pub opportunity_id: String,
    pub expected_return: u64,
}

#[derive(Clone, Debug)]
pub struct VelocityMetrics {
    pub trades_executed: u64,
    pub capital_turned_over: u64,
    pub profits_generated: u64,
    pub window_start: Instant,
}

#[derive(Clone, Debug)]
pub struct OpportunityScore {
    pub profit_bps: u64,
    pub execution_probability: f64,
    pub capital_efficiency: f64,
    pub time_sensitivity: f64,
}

pub struct CapitalVelocityMaximizer {
    rpc_client: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    available_capital: Arc<RwLock<u64>>,
    allocated_capital: Arc<Mutex<HashMap<String, CapitalAllocation>>>,
    velocity_metrics: Arc<RwLock<VelocityMetrics>>,
    pending_recycles: Arc<Mutex<VecDeque<(String, Instant)>>>,
}

impl CapitalVelocityMaximizer {
    pub fn new(rpc_url: &str, wallet: Keypair, initial_capital: u64) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));

        Self {
            rpc_client,
            wallet: Arc::new(wallet),
            available_capital: Arc::new(RwLock::new(initial_capital)),
            allocated_capital: Arc::new(Mutex::new(HashMap::new())),
            velocity_metrics: Arc::new(RwLock::new(VelocityMetrics {
                trades_executed: 0,
                capital_turned_over: 0,
                profits_generated: 0,
                window_start: Instant::now(),
            })),
            pending_recycles: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    pub async fn allocate_capital(
        &self,
        opportunity_id: String,
        requested_amount: u64,
        expected_return: u64,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let mut available = self.available_capital.write().await;
        
        let allocation_amount = self.calculate_optimal_allocation(
            *available,
            requested_amount,
            expected_return,
        ).await?;

        if allocation_amount == 0 {
            return Ok(0);
        }

        *available = available.saturating_sub(allocation_amount);
        
        let allocation = CapitalAllocation {
            amount: allocation_amount,
            locked_until: Instant::now() + Duration::from_millis(CAPITAL_RECYCLE_TIME_MS),
            opportunity_id: opportunity_id.clone(),
            expected_return,
        };

        let mut allocations = self.allocated_capital.lock().unwrap();
        allocations.insert(opportunity_id.clone(), allocation);

        let mut pending = self.pending_recycles.lock().unwrap();
        pending.push_back((opportunity_id, Instant::now() + Duration::from_millis(CAPITAL_RECYCLE_TIME_MS)));

        Ok(allocation_amount)
    }

    async fn calculate_optimal_allocation(
        &self,
        available: u64,
        requested: u64,
        expected_return: u64,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let profit_bps = expected_return.saturating_mul(10000) / requested.max(1);
        
        if profit_bps < MIN_PROFIT_THRESHOLD_BPS {
            return Ok(0);
        }

        let velocity_multiplier = self.get_velocity_multiplier().await;
        let risk_adjusted_size = (requested as f64 * velocity_multiplier) as u64;
        
        let final_allocation = risk_adjusted_size
            .min(available)
            .min(MAX_CAPITAL_PER_TRADE)
            .min(requested);

        Ok(final_allocation)
    }

    async fn get_velocity_multiplier(&self) -> f64 {
        let metrics = self.velocity_metrics.read().await;
        let elapsed = metrics.window_start.elapsed().as_millis() as u64;
        
        if elapsed == 0 || metrics.trades_executed == 0 {
            return 1.0;
        }

        let velocity = (metrics.capital_turned_over as f64) / (elapsed as f64 / 1000.0);
        let efficiency = (metrics.profits_generated as f64) / (metrics.capital_turned_over.max(1) as f64);
        
        let multiplier = 0.5 + (velocity / 1_000_000_000.0).min(1.0) * 0.3 + efficiency.min(0.01) * 20.0;
        multiplier.clamp(0.5, 1.5)
    }

    pub async fn execute_opportunity(
        &self,
        opportunity_id: String,
        instructions: Vec<Instruction>,
        expected_profit: u64,
    ) -> Result<Signature, Box<dyn std::error::Error>> {
        let allocation = {
            let allocations = self.allocated_capital.lock().unwrap();
            allocations.get(&opportunity_id).cloned()
        };

        let allocation = allocation.ok_or("No capital allocated for opportunity")?;

        let mut final_instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(MAX_COMPUTE_UNITS),
            ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS),
        ];
        final_instructions.extend(instructions);

        let recent_blockhash = self.rpc_client.get_latest_blockhash()?;
        let transaction = Transaction::new_signed_with_payer(
            &final_instructions,
            Some(&self.wallet.pubkey()),
            &[&*self.wallet],
            recent_blockhash,
        );

        let signature = self.send_transaction_with_retry(transaction).await?;
        
        self.update_velocity_metrics(allocation.amount, expected_profit).await;
        
        Ok(signature)
    }

    async fn send_transaction_with_retry(
        &self,
        transaction: Transaction,
    ) -> Result<Signature, Box<dyn std::error::Error>> {
        let max_retries = 3;
        let mut last_error = None;

        for attempt in 0..max_retries {
            match self.rpc_client.send_and_confirm_transaction_with_spinner(&transaction) {
                Ok(signature) => return Ok(signature),
                Err(e) => {
                    last_error = Some(e);
                    if attempt < max_retries - 1 {
                        tokio::time::sleep(Duration::from_millis(10 * (attempt as u64 + 1))).await;
                    }
                }
            }
        }

        Err(Box::new(last_error.unwrap()))
    }

    async fn update_velocity_metrics(&self, capital_used: u64, profit: u64) {
        let mut metrics = self.velocity_metrics.write().await;
        
        let elapsed = metrics.window_start.elapsed().as_millis() as u64;
        if elapsed > VELOCITY_WINDOW_MS {
            *metrics = VelocityMetrics {
                trades_executed: 1,
                capital_turned_over: capital_used,
                profits_generated: profit,
                window_start: Instant::now(),
            };
        } else {
            metrics.trades_executed += 1;
            metrics.capital_turned_over += capital_used;
            metrics.profits_generated += profit;
        }
    }

    pub async fn recycle_capital(&self) -> Result<(), Box<dyn std::error::Error>> {
        let now = Instant::now();
        let mut to_recycle = Vec::new();

        {
            let mut pending = self.pending_recycles.lock().unwrap();
            while let Some((id, unlock_time)) = pending.front() {
                if *unlock_time <= now {
                    to_recycle.push(pending.pop_front().unwrap().0);
                } else {
                    break;
                }
            }
        }

        for opportunity_id in to_recycle {
            if let Some(allocation) = self.allocated_capital.lock().unwrap().remove(&opportunity_id) {
                let mut available = self.available_capital.write().await;
                *available += allocation.amount;
            }
        }

        Ok(())
    }

    pub fn score_opportunity(
        &self,
        profit_bps: u64,
        execution_window_ms: u64,
        success_rate: f64,
        capital_required: u64,
    ) -> OpportunityScore {
        let time_sensitivity = 1.0 / (1.0 + (execution_window_ms as f64 / 1000.0));
        let capital_efficiency = (profit_bps as f64 / 10000.0) / (capital_required as f64 / 1_000_000_000.0);
        
        OpportunityScore {
            profit_bps,
            execution_probability: success_rate,
            capital_efficiency,
            time_sensitivity,
        }
    }

    pub fn calculate_composite_score(&self, score: &OpportunityScore) -> f64 {
        let profit_weight = 0.35;
        let probability_weight = 0.30;
        let efficiency_weight = 0.20;
        let time_weight = 0.15;

        (score.profit_bps as f64 / 100.0) * profit_weight +
        score.execution_probability * probability_weight +
        score.capital_efficiency * efficiency_weight +
        score.time_sensitivity * time_weight
    }

    pub async fn get_current_velocity(&self) -> f64 {
        let metrics = self.velocity_metrics.read().await;
        let elapsed_seconds = metrics.window_start.elapsed().as_secs_f64();
        
        if elapsed_seconds == 0.0 {
            return 0.0;
        }

        metrics.capital_turned_over as f64 / elapsed_seconds
    }

    pub async fn get_available_capital(&self) -> u64 {
        *self.available_capital.read().await
    }

    pub async fn get_total_allocated(&self) -> u64 {
        let allocations = self.allocated_capital.lock().unwrap();
        allocations.values().map(|a| a.amount).sum()
    }

    pub async fn optimize_capital_distribution(
        &self,
        opportunities: Vec<(String, u64, u64, f64)>,
    ) -> HashMap<String, u64> {
        let available = self.get_available_capital().await;
        let mut distribution = HashMap::new();
        
        if available == 0 || opportunities.is_empty() {
            return distribution;
        }

        let mut sorted_opportunities = opportunities;
        sorted_opportunities.sort_by(|a, b| {
            let score_a = self.calculate_opportunity_value(a.2, a.1, a.3);
            let score_b = self.calculate_opportunity_value(b.2, b.1, b.3);
            score_b.partial_cmp(&score_a).unwrap()
        });

        let mut remaining_capital = available;
        let max_opportunities = MAX_CONCURRENT_TRADES.min(sorted_opportunities.len());

        for (id, required, profit, success_rate) in sorted_opportunities.into_iter().take(max_opportunities) {
            if remaining_capital == 0 {
                break;
            }

            let allocation = self.calculate_risk_adjusted_allocation(
                remaining_capital,
                required,
                profit,
                success_rate,
            );

            if allocation > 0 {
                distribution.insert(id, allocation);
                remaining_capital = remaining_capital.saturating_sub(allocation);
            }
        }

        distribution
    }

    fn calculate_opportunity_value(&self, profit: u64, required: u64, success_rate: f64) -> f64 {
        let profit_ratio = profit as f64 / required.max(1) as f64;
        profit_ratio * success_rate * (1.0 - (required as f64 / MAX_CAPITAL_PER_TRADE as f64))
    }

    fn calculate_risk_adjusted_allocation(
        &self,
        available: u64,
        required: u64,
        profit: u64,
        success_rate: f64,
    ) -> u64 {
        let profit_bps = profit.saturating_mul(10000) / required.max(1);
        
        if profit_bps < MIN_PROFIT_THRESHOLD_BPS {
            return 0;
        }

                let base_allocation = required.min(available).min(MAX_CAPITAL_PER_TRADE);
        
        let risk_factor = success_rate.powf(2.0);
        let size_factor = 1.0 - (base_allocation as f64 / MAX_CAPITAL_PER_TRADE as f64).powf(0.5);
        let profit_factor = (profit_bps as f64 / 1000.0).min(1.0);
        
        let final_multiplier = risk_factor * size_factor * profit_factor;
        (base_allocation as f64 * final_multiplier) as u64
    }

    pub async fn emergency_capital_recall(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let mut total_recalled = 0u64;
        
        let allocations_to_recall: Vec<String> = {
            let allocations = self.allocated_capital.lock().unwrap();
            allocations.keys().cloned().collect()
        };

        for opportunity_id in allocations_to_recall {
            if let Some(allocation) = self.allocated_capital.lock().unwrap().remove(&opportunity_id) {
                total_recalled += allocation.amount;
            }
        }

        if total_recalled > 0 {
            let mut available = self.available_capital.write().await;
            *available += total_recalled;
        }

        self.pending_recycles.lock().unwrap().clear();
        
        Ok(total_recalled)
    }

    pub async fn rebalance_allocations(&self) -> Result<(), Box<dyn std::error::Error>> {
        let current_allocations = {
            let allocations = self.allocated_capital.lock().unwrap();
            allocations.clone()
        };

        let total_allocated: u64 = current_allocations.values().map(|a| a.amount).sum();
        let available = self.get_available_capital().await;
        let total_capital = total_allocated + available;

        if total_capital == 0 {
            return Ok(());
        }

        let target_utilization = 0.85;
        let target_allocated = (total_capital as f64 * target_utilization) as u64;

        if total_allocated > target_allocated {
            let reduction_ratio = target_allocated as f64 / total_allocated as f64;
            
            for (id, allocation) in current_allocations.iter() {
                let new_amount = (allocation.amount as f64 * reduction_ratio) as u64;
                let freed_capital = allocation.amount - new_amount;
                
                if freed_capital > 0 {
                    let mut allocations = self.allocated_capital.lock().unwrap();
                    if let Some(alloc) = allocations.get_mut(id) {
                        alloc.amount = new_amount;
                    }
                    
                    let mut available = self.available_capital.write().await;
                    *available += freed_capital;
                }
            }
        }

        Ok(())
    }

    pub fn calculate_slippage_adjusted_profit(
        &self,
        expected_output: u64,
        input_amount: u64,
        pool_liquidity: u64,
    ) -> u64 {
        let price_impact = (input_amount as f64 / pool_liquidity as f64).min(0.1);
        let slippage_factor = 1.0 - price_impact - (MAX_SLIPPAGE_BPS as f64 / 10000.0);
        
        let adjusted_output = (expected_output as f64 * slippage_factor.max(0.0)) as u64;
        adjusted_output.saturating_sub(input_amount)
    }

    pub async fn get_capital_efficiency(&self) -> f64 {
        let metrics = self.velocity_metrics.read().await;
        let total_allocated = self.get_total_allocated().await;
        let available = self.get_available_capital().await;
        let total_capital = total_allocated + available;

        if total_capital == 0 || metrics.capital_turned_over == 0 {
            return 0.0;
        }

        let utilization = total_allocated as f64 / total_capital as f64;
        let profit_margin = metrics.profits_generated as f64 / metrics.capital_turned_over as f64;
        
        utilization * profit_margin * 100.0
    }

    pub async fn should_execute_opportunity(
        &self,
        profit_bps: u64,
        capital_required: u64,
        execution_cost: u64,
    ) -> bool {
        if profit_bps < MIN_PROFIT_THRESHOLD_BPS {
            return false;
        }

        let available = self.get_available_capital().await;
        if available < capital_required {
            return false;
        }

        let net_profit = (capital_required * profit_bps / 10000).saturating_sub(execution_cost);
        if net_profit == 0 {
            return false;
        }

        let allocated = self.get_total_allocated().await;
        let total_capital = available + allocated;
        let utilization = allocated as f64 / total_capital.max(1) as f64;

        if utilization > 0.95 && profit_bps < MIN_PROFIT_THRESHOLD_BPS * 2 {
            return false;
        }

        true
    }

    pub fn estimate_execution_cost(&self, num_accounts: usize, compute_units: u32) -> u64 {
        let base_fee = 5000;
        let priority_fee = PRIORITY_FEE_LAMPORTS;
        let account_fee = num_accounts as u64 * 1000;
        
        base_fee + priority_fee + account_fee
    }

    pub async fn get_performance_metrics(&self) -> (f64, f64, f64, u64) {
        let metrics = self.velocity_metrics.read().await;
        let elapsed_seconds = metrics.window_start.elapsed().as_secs_f64().max(1.0);
        
        let trades_per_second = metrics.trades_executed as f64 / elapsed_seconds;
        let capital_velocity = metrics.capital_turned_over as f64 / elapsed_seconds;
        let profit_rate = metrics.profits_generated as f64 / elapsed_seconds;
        
        (trades_per_second, capital_velocity, profit_rate, metrics.trades_executed)
    }

    pub async fn adaptive_priority_fee(&self, base_priority: u64, network_congestion: f64) -> u64 {
        let velocity = self.get_current_velocity().await;
        let efficiency = self.get_capital_efficiency().await;
        
        let velocity_multiplier = (velocity / 1_000_000_000.0).min(2.0).max(0.5);
        let efficiency_multiplier = (efficiency / 10.0).min(1.5).max(0.8);
        let congestion_multiplier = (1.0 + network_congestion).min(3.0);
        
        let adjusted_fee = (base_priority as f64 * velocity_multiplier * efficiency_multiplier * congestion_multiplier) as u64;
        adjusted_fee.min(1_000_000).max(10_000)
    }

    pub fn validate_opportunity_params(
        &self,
        input_mint: &Pubkey,
        output_mint: &Pubkey,
        input_amount: u64,
        min_output: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if input_mint == output_mint {
            return Err("Input and output mints cannot be the same".into());
        }

        if input_amount == 0 {
            return Err("Input amount cannot be zero".into());
        }

        if min_output == 0 {
            return Err("Minimum output cannot be zero".into());
        }

        if input_amount > MAX_CAPITAL_PER_TRADE {
            return Err("Input amount exceeds maximum capital per trade".into());
        }

        Ok(())
    }

    pub async fn batch_allocate_capital(
        &self,
        opportunities: Vec<(String, u64, u64)>,
    ) -> Result<HashMap<String, u64>, Box<dyn std::error::Error>> {
        let mut allocations = HashMap::new();
        let distribution = self.optimize_capital_distribution(
            opportunities.iter()
                .map(|(id, req, ret)| (id.clone(), *req, *ret, 0.85))
                .collect()
        ).await;

        for (id, amount) in distribution {
            if amount > 0 {
                match self.allocate_capital(id.clone(), amount, amount * 11 / 10).await {
                    Ok(allocated) => {
                        allocations.insert(id, allocated);
                    }
                    Err(_) => continue,
                }
            }
        }

        Ok(allocations)
    }

    pub async fn auto_recycle_loop(&self) {
        loop {
            if let Err(e) = self.recycle_capital().await {
                eprintln!("Capital recycle error: {}", e);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    pub async fn monitor_capital_health(&self) -> Result<(), Box<dyn std::error::Error>> {
        let available = self.get_available_capital().await;
        let allocated = self.get_total_allocated().await;
        let total = available + allocated;

        if total == 0 {
            return Err("No capital available".into());
        }

        let utilization = allocated as f64 / total as f64;
        
        if utilization > 0.98 {
            self.rebalance_allocations().await?;
        }

        if available < total / 10 {
            let elapsed = Instant::now();
            loop {
                self.recycle_capital().await?;
                let new_available = self.get_available_capital().await;
                if new_available >= total / 5 || elapsed.elapsed() > Duration::from_millis(500) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }

        Ok(())
    }
}

