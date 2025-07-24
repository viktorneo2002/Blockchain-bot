use std::collections::{HashMap, BTreeMap};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use solana_sdk::pubkey::Pubkey;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use rayon::prelude::*;

const MIN_ALLOCATION_LAMPORTS: u64 = 1_000_000;
const MAX_POSITION_PERCENTAGE: f64 = 0.15;
const KELLY_FRACTION: f64 = 0.25;
const GAS_RESERVE_MULTIPLIER: f64 = 1.5;
const CONFIDENCE_DECAY_RATE: f64 = 0.95;
const MIN_SHARPE_RATIO: f64 = 1.5;
const RISK_FREE_RATE: f64 = 0.02;
const LOOKBACK_PERIODS: usize = 100;
const MAX_CORRELATION: f64 = 0.7;
const VAR_CONFIDENCE_LEVEL: f64 = 0.95;
const LIQUIDITY_DISCOUNT_FACTOR: f64 = 0.98;
const MAX_SLIPPAGE_BPS: u64 = 50;
const OPPORTUNITY_TIMEOUT_MS: u64 = 300;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpportunityMetrics {
    pub expected_profit: u64,
    pub probability: f64,
    pub gas_cost: u64,
    pub slippage_estimate: u64,
    pub execution_time_ms: u64,
    pub liquidity_score: f64,
    pub market_impact: f64,
    pub confidence: f64,
    pub timestamp: Instant,
}

#[derive(Debug, Clone)]
pub struct HistoricalPerformance {
    pub returns: Vec<f64>,
    pub timestamps: Vec<Instant>,
    pub success_rate: f64,
    pub avg_execution_time: f64,
    pub volatility: f64,
    pub max_drawdown: f64,
}

#[derive(Debug, Clone)]
pub struct RiskMetrics {
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub value_at_risk: f64,
    pub conditional_var: f64,
    pub max_drawdown: f64,
    pub beta: f64,
    pub correlation_matrix: HashMap<String, f64>,
}

#[derive(Debug, Clone)]
pub struct AllocationDecision {
    pub amount: u64,
    pub confidence: f64,
    pub risk_adjusted_return: f64,
    pub position_size_ratio: f64,
    pub gas_budget: u64,
    pub stop_loss: u64,
    pub take_profit: u64,
}

pub struct CapitalAllocationOptimizer {
    total_capital: Arc<RwLock<u64>>,
    allocated_capital: Arc<RwLock<HashMap<Pubkey, u64>>>,
    performance_history: Arc<RwLock<HashMap<String, HistoricalPerformance>>>,
    risk_metrics: Arc<RwLock<RiskMetrics>>,
    opportunity_queue: Arc<RwLock<BTreeMap<u64, OpportunityMetrics>>>,
    gas_reserve: Arc<RwLock<u64>>,
    min_liquidity_buffer: u64,
    max_concurrent_positions: usize,
    risk_parameters: RiskParameters,
}

#[derive(Debug, Clone)]
struct RiskParameters {
    max_leverage: f64,
    max_var_percentage: f64,
    min_profit_threshold: u64,
    max_correlation_exposure: f64,
    rebalance_threshold: f64,
    emergency_stop_loss: f64,
}

impl Default for RiskParameters {
    fn default() -> Self {
        Self {
            max_leverage: 1.0,
            max_var_percentage: 0.05,
            min_profit_threshold: 100_000,
            max_correlation_exposure: 0.3,
            rebalance_threshold: 0.1,
            emergency_stop_loss: 0.2,
        }
    }
}

impl CapitalAllocationOptimizer {
    pub fn new(initial_capital: u64, max_concurrent: usize) -> Self {
        let gas_reserve = (initial_capital as f64 * 0.02) as u64;
        let min_liquidity = (initial_capital as f64 * 0.1) as u64;
        
        Self {
            total_capital: Arc::new(RwLock::new(initial_capital)),
            allocated_capital: Arc::new(RwLock::new(HashMap::new())),
            performance_history: Arc::new(RwLock::new(HashMap::new())),
            risk_metrics: Arc::new(RwLock::new(RiskMetrics {
                sharpe_ratio: 0.0,
                sortino_ratio: 0.0,
                value_at_risk: 0.0,
                conditional_var: 0.0,
                max_drawdown: 0.0,
                beta: 1.0,
                correlation_matrix: HashMap::new(),
            })),
            opportunity_queue: Arc::new(RwLock::new(BTreeMap::new())),
            gas_reserve: Arc::new(RwLock::new(gas_reserve)),
            min_liquidity_buffer: min_liquidity,
            max_concurrent_positions: max_concurrent,
            risk_parameters: RiskParameters::default(),
        }
    }

    pub fn calculate_optimal_allocation(
        &self,
        opportunity_id: &Pubkey,
        metrics: &OpportunityMetrics,
    ) -> Result<AllocationDecision, String> {
        let total_capital = *self.total_capital.read().unwrap();
        let allocated = self.get_total_allocated();
        let available_capital = total_capital.saturating_sub(allocated + self.min_liquidity_buffer);
        
        if available_capital < MIN_ALLOCATION_LAMPORTS {
            return Err("Insufficient available capital".to_string());
        }

        let kelly_size = self.calculate_kelly_position(metrics, available_capital);
        let risk_adjusted_size = self.apply_risk_constraints(kelly_size, metrics);
        let final_size = self.apply_liquidity_constraints(risk_adjusted_size, metrics);
        
        let gas_budget = self.calculate_gas_budget(metrics);
        let (stop_loss, take_profit) = self.calculate_risk_levels(final_size, metrics);
        
        if final_size < MIN_ALLOCATION_LAMPORTS || final_size < gas_budget * 2 {
            return Err("Position size too small after constraints".to_string());
        }

        let risk_adjusted_return = self.calculate_risk_adjusted_return(metrics, final_size);
        
        Ok(AllocationDecision {
            amount: final_size,
            confidence: metrics.confidence * self.get_strategy_confidence(&opportunity_id.to_string()),
            risk_adjusted_return,
            position_size_ratio: final_size as f64 / available_capital as f64,
            gas_budget,
            stop_loss,
            take_profit,
        })
    }

    fn calculate_kelly_position(&self, metrics: &OpportunityMetrics, available: u64) -> u64 {
        let win_prob = metrics.probability;
        let loss_prob = 1.0 - win_prob;
        
        let net_profit = metrics.expected_profit.saturating_sub(metrics.gas_cost + metrics.slippage_estimate);
        if net_profit == 0 {
            return 0;
        }
        
        let win_loss_ratio = net_profit as f64 / available as f64;
        let kelly_fraction = (win_prob * win_loss_ratio - loss_prob) / win_loss_ratio;
        
        let adjusted_kelly = kelly_fraction * KELLY_FRACTION * metrics.liquidity_score;
        let position_size = (available as f64 * adjusted_kelly.max(0.0).min(MAX_POSITION_PERCENTAGE)) as u64;
        
        position_size
    }

    fn apply_risk_constraints(&self, position_size: u64, metrics: &OpportunityMetrics) -> u64 {
        let risk_metrics = self.risk_metrics.read().unwrap();
        let total_capital = *self.total_capital.read().unwrap();
        
        let var_constraint = (total_capital as f64 * self.risk_parameters.max_var_percentage) as u64;
        let correlation_constraint = self.calculate_correlation_constraint(metrics);
        let drawdown_constraint = self.calculate_drawdown_constraint();
        
        let risk_adjusted = position_size
            .min(var_constraint)
            .min(correlation_constraint)
            .min(drawdown_constraint);
            
        let volatility_scalar = 1.0 / (1.0 + metrics.market_impact * 2.0);
        (risk_adjusted as f64 * volatility_scalar) as u64
    }

    fn apply_liquidity_constraints(&self, position_size: u64, metrics: &OpportunityMetrics) -> u64 {
        let liquidity_adjusted = (position_size as f64 * metrics.liquidity_score * LIQUIDITY_DISCOUNT_FACTOR) as u64;
        let slippage_impact = 1.0 - (metrics.slippage_estimate as f64 / metrics.expected_profit.max(1) as f64);
        
        (liquidity_adjusted as f64 * slippage_impact.max(0.5)) as u64
    }

    fn calculate_gas_budget(&self, metrics: &OpportunityMetrics) -> u64 {
        let base_gas = metrics.gas_cost;
        let gas_reserve = *self.gas_reserve.read().unwrap();
        
        let priority_multiplier = 1.0 + (metrics.confidence - 0.5).max(0.0);
        let timed_multiplier = if metrics.execution_time_ms < 100 { 1.2 } else { 1.0 };
        
        let total_gas = (base_gas as f64 * GAS_RESERVE_MULTIPLIER * priority_multiplier * timed_multiplier) as u64;
        total_gas.min(gas_reserve / 10)
    }

    fn calculate_risk_levels(&self, position_size: u64, metrics: &OpportunityMetrics) -> (u64, u64) {
        let expected_profit_ratio = metrics.expected_profit as f64 / position_size.max(1) as f64;
        let volatility_factor = 1.0 + metrics.market_impact;
        
        let stop_loss_percentage = 0.02 * volatility_factor * (2.0 - metrics.confidence);
        let take_profit_percentage = expected_profit_ratio * metrics.probability * 0.8;
        
        let stop_loss = (position_size as f64 * (1.0 - stop_loss_percentage)) as u64;
        let take_profit = (position_size as f64 * (1.0 + take_profit_percentage)) as u64;
        
        (stop_loss, take_profit)
    }

    fn calculate_risk_adjusted_return(&self, metrics: &OpportunityMetrics, position_size: u64) -> f64 {
        let net_return = (metrics.expected_profit.saturating_sub(metrics.gas_cost + metrics.slippage_estimate)) as f64;
        let return_rate = net_return / position_size.max(1) as f64;
        
        let time_factor = 365.0 * 24.0 * 60.0 * 60.0 * 1000.0 / metrics.execution_time_ms.max(1) as f64;
        let annualized_return = return_rate * time_factor;
        
        let risk_metrics = self.risk_metrics.read().unwrap();
        let volatility = self.estimate_position_volatility(metrics);
        
        (annualized_return - RISK_FREE_RATE) / volatility.max(0.01)
    }

    fn calculate_correlation_constraint(&self, metrics: &OpportunityMetrics) -> u64 {
        let total_capital = *self.total_capital.read().unwrap();
        let allocated = self.allocated_capital.read().unwrap();
        
        if allocated.is_empty() {
            return total_capital;
        }
        
        let avg_correlation = self.estimate_correlation_impact(metrics);
        let correlation_penalty = 1.0 - (avg_correlation * self.risk_parameters.max_correlation_exposure);
        
        (total_capital as f64 * correlation_penalty.max(0.1)) as u64
    }

    fn calculate_drawdown_constraint(&self) -> u64 {
        let total_capital = *self.total_capital.read().unwrap();
        let risk_metrics = self.risk_metrics.read().unwrap();
        
        let drawdown_buffer = 1.0 - risk_metrics.max_drawdown - 0.1;
        (total_capital as f64 * drawdown_buffer.max(0.2)) as u64
    }

    fn estimate_position_volatility(&self, metrics: &OpportunityMetrics) -> f64 {
        let base_volatility = 0.02;
        let liquidity_impact = (1.0 - metrics.liquidity_score) * 0.01;
        let market_impact = metrics.market_impact * 0.02;
        let time_impact = (metrics.execution_time_ms as f64 / 1000.0).sqrt() * 0.001;
        
        base_volatility + liquidity_impact + market_impact + time_impact
    }

    fn estimate_correlation_impact(&self, metrics: &OpportunityMetrics) -> f64 {
        let risk_metrics = self.risk_metrics.read().unwrap();
        
        if risk_metrics.correlation_matrix.is_empty() {
            return 0.0;
        }
        
        let correlations: Vec<f64> = risk_metrics.correlation_matrix.values().copied().collect();
        correlations.iter().sum::<f64>() / correlations.len().max(1) as f64
    }

    fn get_strategy_confidence(&self, strategy_id: &str) -> f64 {
        let history = self.performance_history.read().unwrap();
        
        history.get(strategy_id)
            .map(|h| {
                                let recency_weight = (-h.timestamps.len() as f64 / LOOKBACK_PERIODS as f64).exp();
                let base_confidence = h.success_rate * (1.0 - h.volatility.min(1.0));
                let sharpe_bonus = (h.returns.iter().sum::<f64>() / h.returns.len().max(1) as f64 - RISK_FREE_RATE) 
                    / h.volatility.max(0.01);
                
                (base_confidence + sharpe_bonus.max(0.0).min(1.0) * 0.2) * recency_weight * CONFIDENCE_DECAY_RATE
            })
            .unwrap_or(0.5)
    }

    pub fn allocate_capital(&self, opportunity_id: Pubkey, amount: u64) -> Result<(), String> {
        let total = *self.total_capital.read().unwrap();
        let mut allocated = self.allocated_capital.write().unwrap();
        
        let current_allocated: u64 = allocated.values().sum();
        let new_total = current_allocated + amount;
        
        if new_total > total - self.min_liquidity_buffer {
            return Err("Allocation would exceed available capital".to_string());
        }
        
        if allocated.len() >= self.max_concurrent_positions {
            return Err("Maximum concurrent positions reached".to_string());
        }
        
        allocated.insert(opportunity_id, amount);
        self.update_gas_reserve(amount);
        
        Ok(())
    }

    pub fn release_capital(&self, opportunity_id: &Pubkey, profit_loss: i64) -> Result<u64, String> {
        let mut allocated = self.allocated_capital.write().unwrap();
        let amount = allocated.remove(opportunity_id)
            .ok_or_else(|| "No allocation found for opportunity".to_string())?;
        
        let mut total = self.total_capital.write().unwrap();
        let new_total = (*total as i64 + profit_loss).max(0) as u64;
        *total = new_total;
        
        self.update_performance_metrics(opportunity_id, profit_loss, amount);
        self.rebalance_if_needed();
        
        Ok(amount)
    }

    fn update_gas_reserve(&self, allocated_amount: u64) {
        let mut gas_reserve = self.gas_reserve.write().unwrap();
        let gas_usage_estimate = (allocated_amount as f64 * 0.002) as u64;
        *gas_reserve = gas_reserve.saturating_sub(gas_usage_estimate);
        
        let total = *self.total_capital.read().unwrap();
        let min_gas = (total as f64 * 0.01) as u64;
        if *gas_reserve < min_gas {
            *gas_reserve = min_gas;
        }
    }

    fn update_performance_metrics(&self, opportunity_id: &Pubkey, profit_loss: i64, position_size: u64) {
        let mut history = self.performance_history.write().unwrap();
        let strategy_id = opportunity_id.to_string();
        
        let return_rate = profit_loss as f64 / position_size.max(1) as f64;
        
        let perf = history.entry(strategy_id).or_insert_with(|| HistoricalPerformance {
            returns: Vec::with_capacity(LOOKBACK_PERIODS),
            timestamps: Vec::with_capacity(LOOKBACK_PERIODS),
            success_rate: 0.5,
            avg_execution_time: 100.0,
            volatility: 0.02,
            max_drawdown: 0.0,
        });
        
        perf.returns.push(return_rate);
        perf.timestamps.push(Instant::now());
        
        if perf.returns.len() > LOOKBACK_PERIODS {
            perf.returns.remove(0);
            perf.timestamps.remove(0);
        }
        
        self.recalculate_performance_stats(perf);
        self.update_risk_metrics();
    }

    fn recalculate_performance_stats(&self, perf: &mut HistoricalPerformance) {
        if perf.returns.is_empty() {
            return;
        }
        
        let successes = perf.returns.iter().filter(|&&r| r > 0.0).count();
        perf.success_rate = successes as f64 / perf.returns.len() as f64;
        
        let mean = perf.returns.iter().sum::<f64>() / perf.returns.len() as f64;
        let variance = perf.returns.iter()
            .map(|&r| (r - mean).powi(2))
            .sum::<f64>() / perf.returns.len().max(1) as f64;
        perf.volatility = variance.sqrt();
        
        let mut cumulative = 0.0;
        let mut peak = 0.0;
        let mut max_dd = 0.0;
        
        for &ret in &perf.returns {
            cumulative += ret;
            peak = peak.max(cumulative);
            let drawdown = (peak - cumulative) / peak.max(1.0);
            max_dd = max_dd.max(drawdown);
        }
        
        perf.max_drawdown = max_dd;
    }

    fn update_risk_metrics(&self) {
        let history = self.performance_history.read().unwrap();
        let mut risk_metrics = self.risk_metrics.write().unwrap();
        
        let all_returns: Vec<f64> = history.values()
            .flat_map(|h| h.returns.clone())
            .collect();
        
        if all_returns.len() < 10 {
            return;
        }
        
        let mean_return = all_returns.iter().sum::<f64>() / all_returns.len() as f64;
        let volatility = self.calculate_portfolio_volatility(&all_returns, mean_return);
        
        risk_metrics.sharpe_ratio = (mean_return - RISK_FREE_RATE / 252.0) / volatility.max(0.001);
        risk_metrics.sortino_ratio = self.calculate_sortino_ratio(&all_returns, mean_return);
        risk_metrics.value_at_risk = self.calculate_var(&all_returns);
        risk_metrics.conditional_var = self.calculate_cvar(&all_returns, risk_metrics.value_at_risk);
        risk_metrics.max_drawdown = history.values()
            .map(|h| h.max_drawdown)
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .unwrap_or(0.0);
    }

    fn calculate_portfolio_volatility(&self, returns: &[f64], mean: f64) -> f64 {
        if returns.is_empty() {
            return 0.02;
        }
        
        let variance = returns.iter()
            .map(|&r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;
        
        variance.sqrt()
    }

    fn calculate_sortino_ratio(&self, returns: &[f64], mean: f64) -> f64 {
        let downside_returns: Vec<f64> = returns.iter()
            .filter(|&&r| r < 0.0)
            .copied()
            .collect();
        
        if downside_returns.is_empty() {
            return 3.0;
        }
        
        let downside_variance = downside_returns.iter()
            .map(|&r| r.powi(2))
            .sum::<f64>() / downside_returns.len() as f64;
        
        (mean - RISK_FREE_RATE / 252.0) / downside_variance.sqrt().max(0.001)
    }

    fn calculate_var(&self, returns: &[f64]) -> f64 {
        let mut sorted_returns = returns.to_vec();
        sorted_returns.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let var_index = ((1.0 - VAR_CONFIDENCE_LEVEL) * sorted_returns.len() as f64) as usize;
        sorted_returns.get(var_index).copied().unwrap_or(0.0).abs()
    }

    fn calculate_cvar(&self, returns: &[f64], var: f64) -> f64 {
        let tail_losses: Vec<f64> = returns.iter()
            .filter(|&&r| r <= -var)
            .map(|&r| r.abs())
            .collect();
        
        if tail_losses.is_empty() {
            return var;
        }
        
        tail_losses.iter().sum::<f64>() / tail_losses.len() as f64
    }

    fn rebalance_if_needed(&self) {
        let total = *self.total_capital.read().unwrap();
        let allocated = self.allocated_capital.read().unwrap();
        let total_allocated: u64 = allocated.values().sum();
        
        let allocation_ratio = total_allocated as f64 / total.max(1) as f64;
        
        if allocation_ratio < 0.3 || allocation_ratio > 0.8 {
            self.rebalance_positions();
        }
    }

    fn rebalance_positions(&self) {
        let total = *self.total_capital.read().unwrap();
        let risk_metrics = self.risk_metrics.read().unwrap();
        
        if risk_metrics.sharpe_ratio < MIN_SHARPE_RATIO {
            self.reduce_position_sizes();
        } else if risk_metrics.max_drawdown > self.risk_parameters.emergency_stop_loss {
            self.emergency_deleverage();
        }
    }

    fn reduce_position_sizes(&self) {
        let allocated = self.allocated_capital.read().unwrap();
        let reduction_factor = 0.8;
        
        let new_allocations: HashMap<Pubkey, u64> = allocated.iter()
            .map(|(k, &v)| (*k, (v as f64 * reduction_factor) as u64))
            .collect();
        
        drop(allocated);
        let mut allocated = self.allocated_capital.write().unwrap();
        *allocated = new_allocations;
    }

    fn emergency_deleverage(&self) {
        let mut allocated = self.allocated_capital.write().unwrap();
        allocated.clear();
        
        let mut gas_reserve = self.gas_reserve.write().unwrap();
        let total = *self.total_capital.read().unwrap();
        *gas_reserve = (total as f64 * 0.05) as u64;
    }

    pub fn get_allocation_status(&self) -> AllocationStatus {
        let total = *self.total_capital.read().unwrap();
        let allocated = self.allocated_capital.read().unwrap();
        let total_allocated: u64 = allocated.values().sum();
        let gas_reserve = *self.gas_reserve.read().unwrap();
        
        AllocationStatus {
            total_capital: total,
            allocated_capital: total_allocated,
            available_capital: total.saturating_sub(total_allocated + self.min_liquidity_buffer),
            active_positions: allocated.len(),
            gas_reserve,
            utilization_rate: total_allocated as f64 / total.max(1) as f64,
        }
    }

    pub fn optimize_concurrent_allocations(&self, opportunities: Vec<(Pubkey, OpportunityMetrics)>) -> Vec<(Pubkey, AllocationDecision)> {
        let mut scored_opportunities: Vec<(Pubkey, OpportunityMetrics, f64)> = opportunities
            .into_par_iter()
            .map(|(id, metrics)| {
                let score = self.score_opportunity(&metrics);
                (id, metrics, score)
            })
            .collect();
        
        scored_opportunities.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        
        let mut allocations = Vec::new();
        let mut remaining_capital = self.get_allocation_status().available_capital;
        
        for (id, metrics, _) in scored_opportunities.iter().take(self.max_concurrent_positions) {
            if let Ok(decision) = self.calculate_optimal_allocation(id, metrics) {
                if decision.amount <= remaining_capital {
                    remaining_capital = remaining_capital.saturating_sub(decision.amount);
                    allocations.push((*id, decision));
                }
            }
        }
        
        allocations
    }

    fn score_opportunity(&self, metrics: &OpportunityMetrics) -> f64 {
        let profit_score = (metrics.expected_profit as f64 / 1_000_000.0).ln().max(0.0);
        let probability_score = metrics.probability * 2.0;
        let liquidity_score = metrics.liquidity_score * 1.5;
        let time_score = 1.0 / (1.0 + metrics.execution_time_ms as f64 / 100.0);
        let confidence_score = metrics.confidence * 2.0;
        
        let gas_efficiency = metrics.expected_profit as f64 / metrics.gas_cost.max(1) as f64;
        let gas_score = gas_efficiency.ln().max(0.0) / 10.0;
        
        profit_score + probability_score + liquidity_score + time_score + confidence_score + gas_score
    }

    pub fn get_total_allocated(&self) -> u64 {
        self.allocated_capital.read().unwrap().values().sum()
    }

    pub fn update_capital(&self, new_total: u64) {
        let mut total = self.total_capital.write().unwrap();
        *total = new_total;
        
        self.min_liquidity_buffer = (new_total as f64 * 0.1) as u64;
        let mut gas_reserve = self.gas_reserve.write().unwrap();
        *gas_reserve = (new_total as f64 * 0.02) as u64;
    }
}

#[derive(Debug, Clone)]
pub struct AllocationStatus {
    pub total_capital: u64,
    pub allocated_capital: u64,
    pub available_capital: u64,
    pub active_positions: usize,
    pub gas_reserve: u64,
    pub utilization_rate: f64,
}

impl CapitalAllocationOptimizer {
    pub fn get_risk_metrics(&self) -> RiskMetrics {
        self.risk_metrics.read().unwrap().clone()
    }

    pub fn should_take_opportunity(&self, metrics: &OpportunityMetrics) -> bool {
        if metrics.timestamp.elapsed() > Duration::from_millis(OPPORTUNITY_TIMEOUT_MS) {
            return false;
        }

        let min_profit = self.risk_parameters.min_profit_threshold;
        let net_profit = metrics.expected_profit.saturating_sub(metrics.gas_cost + metrics.slippage_estimate);
        
        if net_profit < min_profit {
            return false;
        }

        let risk_metrics = self.risk_metrics.read().unwrap();
        if risk_metrics.max_drawdown > self.risk_parameters.emergency_stop_loss * 0.8 {
            return false;
        }

        let slippage_bps = (metrics.slippage_estimate * 10000) / metrics.expected_profit.max(1);
        if slippage_bps > MAX_SLIPPAGE_BPS {
            return false;
        }

        let required_sharpe = MIN_SHARPE_RATIO * (1.0 + risk_metrics.max_drawdown);
        let opportunity_sharpe = self.estimate_opportunity_sharpe(metrics);
        
        opportunity_sharpe >= required_sharpe && metrics.confidence >= 0.6
    }

    fn estimate_opportunity_sharpe(&self, metrics: &OpportunityMetrics) -> f64 {
        let return_rate = (metrics.expected_profit as f64 - metrics.gas_cost as f64) / MIN_ALLOCATION_LAMPORTS as f64;
        let time_factor = 365.0 * 24.0 * 60.0 * 60.0 * 1000.0 / metrics.execution_time_ms.max(1) as f64;
        let annualized_return = return_rate * time_factor * metrics.probability;
        
        let volatility = self.estimate_position_volatility(metrics);
        (annualized_return - RISK_FREE_RATE) / volatility.max(0.01)
    }

    pub fn update_opportunity_metrics(&self, opportunity_id: Pubkey, metrics: OpportunityMetrics) {
        let mut queue = self.opportunity_queue.write().unwrap();
        let priority = (metrics.expected_profit as f64 * metrics.probability * metrics.confidence) as u64;
        queue.insert(priority, metrics);
        
        while queue.len() > 100 {
            if let Some(first_key) = queue.keys().next().cloned() {
                queue.remove(&first_key);
            }
        }
    }

    pub fn get_portfolio_health(&self) -> PortfolioHealth {
        let status = self.get_allocation_status();
        let risk_metrics = self.risk_metrics.read().unwrap();
        
        let health_score = self.calculate_health_score(&status, &risk_metrics);
        let risk_level = self.classify_risk_level(&risk_metrics);
        
        PortfolioHealth {
            score: health_score,
            risk_level,
            recommendations: self.generate_recommendations(&status, &risk_metrics),
            warnings: self.check_warnings(&status, &risk_metrics),
        }
    }

    fn calculate_health_score(&self, status: &AllocationStatus, risk_metrics: &RiskMetrics) -> f64 {
        let utilization_score = if status.utilization_rate < 0.3 {
            status.utilization_rate * 3.33
        } else if status.utilization_rate > 0.8 {
            1.0 - (status.utilization_rate - 0.8) * 5.0
        } else {
            1.0
        };

        let sharpe_score = (risk_metrics.sharpe_ratio / 3.0).min(1.0).max(0.0);
        let drawdown_score = (1.0 - risk_metrics.max_drawdown).max(0.0);
        let var_score = (1.0 - risk_metrics.value_at_risk * 10.0).max(0.0);
        
        (utilization_score * 0.2 + sharpe_score * 0.4 + drawdown_score * 0.25 + var_score * 0.15) * 100.0
    }

    fn classify_risk_level(&self, risk_metrics: &RiskMetrics) -> RiskLevel {
        if risk_metrics.max_drawdown > 0.15 || risk_metrics.value_at_risk > 0.1 {
            RiskLevel::High
        } else if risk_metrics.sharpe_ratio < 1.0 || risk_metrics.max_drawdown > 0.08 {
            RiskLevel::Medium
        } else {
            RiskLevel::Low
        }
    }

    fn generate_recommendations(&self, status: &AllocationStatus, risk_metrics: &RiskMetrics) -> Vec<String> {
        let mut recommendations = Vec::new();

        if status.utilization_rate < 0.3 {
            recommendations.push("Consider increasing position sizes to improve capital efficiency".to_string());
        } else if status.utilization_rate > 0.8 {
            recommendations.push("High capital utilization - consider reducing positions for flexibility".to_string());
        }

        if risk_metrics.sharpe_ratio < MIN_SHARPE_RATIO {
            recommendations.push("Risk-adjusted returns below target - review strategy selection".to_string());
        }

        if risk_metrics.max_drawdown > 0.1 {
            recommendations.push("Significant drawdown detected - implement tighter risk controls".to_string());
        }

        if status.gas_reserve < status.total_capital / 50 {
            recommendations.push("Gas reserve running low - allocate more funds for transaction costs".to_string());
        }

        recommendations
    }

    fn check_warnings(&self, status: &AllocationStatus, risk_metrics: &RiskMetrics) -> Vec<String> {
        let mut warnings = Vec::new();

        if risk_metrics.max_drawdown > self.risk_parameters.emergency_stop_loss * 0.9 {
            warnings.push("CRITICAL: Approaching emergency stop loss threshold".to_string());
        }

        if risk_metrics.value_at_risk > self.risk_parameters.max_var_percentage * 0.9 {
            warnings.push("WARNING: VaR approaching maximum allowed threshold".to_string());
        }

        if status.available_capital < self.min_liquidity_buffer * 1.1 {
            warnings.push("WARNING: Low available capital - liquidity constraints imminent".to_string());
        }

        let correlation_avg: f64 = risk_metrics.correlation_matrix.values().sum::<f64>() 
            / risk_metrics.correlation_matrix.len().max(1) as f64;
        if correlation_avg > MAX_CORRELATION {
            warnings.push("WARNING: High portfolio correlation - diversification needed".to_string());
        }

        warnings
    }

    pub fn emergency_shutdown(&self) {
        let mut allocated = self.allocated_capital.write().unwrap();
        let positions: Vec<Pubkey> = allocated.keys().cloned().collect();
        allocated.clear();

        let mut queue = self.opportunity_queue.write().unwrap();
        queue.clear();

        let total = *self.total_capital.read().unwrap();
        let mut gas_reserve = self.gas_reserve.write().unwrap();
        *gas_reserve = total / 10;
    }

    pub fn adjust_risk_parameters(&mut self, new_params: RiskParameters) {
        self.risk_parameters = new_params;
        self.rebalance_positions();
    }

    pub fn export_metrics(&self) -> MetricsExport {
        let risk_metrics = self.risk_metrics.read().unwrap();
        let status = self.get_allocation_status();
        let history = self.performance_history.read().unwrap();

        MetricsExport {
            timestamp: Instant::now(),
            total_capital: status.total_capital,
            allocated_capital: status.allocated_capital,
            active_positions: status.active_positions,
            sharpe_ratio: risk_metrics.sharpe_ratio,
            sortino_ratio: risk_metrics.sortino_ratio,
            max_drawdown: risk_metrics.max_drawdown,
            value_at_risk: risk_metrics.value_at_risk,
            total_opportunities_evaluated: self.opportunity_queue.read().unwrap().len(),
            average_success_rate: history.values()
                .map(|h| h.success_rate)
                .sum::<f64>() / history.len().max(1) as f64,
        }
    }
}

#[derive(Debug, Clone)]
pub enum RiskLevel {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone)]
pub struct PortfolioHealth {
    pub score: f64,
    pub risk_level: RiskLevel,
    pub recommendations: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsExport {
    pub timestamp: Instant,
    pub total_capital: u64,
    pub allocated_capital: u64,
    pub active_positions: usize,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub max_drawdown: f64,
    pub value_at_risk: f64,
    pub total_opportunities_evaluated: usize,
    pub average_success_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_capital_allocation() {
        let optimizer = CapitalAllocationOptimizer::new(10_000_000_000, 5);
        let opportunity_id = Pubkey::new_unique();
        
        let metrics = OpportunityMetrics {
            expected_profit: 1_000_000,
            probability: 0.75,
            gas_cost: 50_000,
            slippage_estimate: 20_000,
            execution_time_ms: 50,
            liquidity_score: 0.9,
            market_impact: 0.02,
            confidence: 0.8,
            timestamp: Instant::now(),
        };

        let decision = optimizer.calculate_optimal_allocation(&opportunity_id, &metrics).unwrap();
        assert!(decision.amount > MIN_ALLOCATION_LAMPORTS);
        assert!(decision.amount <= optimizer.get_allocation_status().available_capital);
    }

    #[test]
    fn test_risk_constraints() {
        let optimizer = CapitalAllocationOptimizer::new(1_000_000_000, 3);
        assert!(optimizer.get_allocation_status().gas_reserve > 0);
        assert_eq!(optimizer.get_total_allocated(), 0);
    }
}

