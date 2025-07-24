use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use nalgebra::{DMatrix, DVector};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;

const MAX_STRATEGIES: usize = 32;
const MIN_ALLOCATION: Decimal = dec!(0.001);
const MAX_ALLOCATION: Decimal = dec!(0.95);
const CONVERGENCE_THRESHOLD: f64 = 1e-8;
const MAX_ITERATIONS: usize = 1000;
const RISK_LOOKBACK_PERIODS: usize = 168;
const CORRELATION_DECAY_FACTOR: f64 = 0.94;
const VOL_FLOOR: f64 = 0.0001;
const REBALANCE_THRESHOLD: Decimal = dec!(0.02);
const EMERGENCY_RISK_MULTIPLIER: Decimal = dec!(2.5);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyMetrics {
    pub strategy_id: String,
    pub pubkey: Pubkey,
    pub returns: Vec<f64>,
    pub volumes: Vec<f64>,
    pub success_rate: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub avg_slippage: f64,
    pub gas_efficiency: f64,
    pub last_update: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskMetrics {
    pub volatility: f64,
    pub downside_volatility: f64,
    pub value_at_risk: f64,
    pub conditional_value_at_risk: f64,
    pub max_loss: f64,
    pub correlation_matrix: DMatrix<f64>,
    pub covariance_matrix: DMatrix<f64>,
}

#[derive(Debug, Clone)]
pub struct AllocationResult {
    pub allocations: HashMap<String, Decimal>,
    pub risk_contributions: HashMap<String, f64>,
    pub portfolio_volatility: f64,
    pub expected_return: f64,
    pub sharpe_ratio: f64,
    pub diversification_ratio: f64,
    pub timestamp: Instant,
}

pub struct RiskParityAllocator {
    strategies: Arc<RwLock<HashMap<String, StrategyMetrics>>>,
    risk_metrics: Arc<RwLock<RiskMetrics>>,
    current_allocation: Arc<RwLock<AllocationResult>>,
    risk_budget: HashMap<String, f64>,
    max_leverage: Decimal,
    risk_free_rate: f64,
    rebalance_frequency: Duration,
    last_rebalance: Instant,
}

impl RiskParityAllocator {
    pub fn new(max_leverage: Decimal, risk_free_rate: f64) -> Self {
        Self {
            strategies: Arc::new(RwLock::new(HashMap::new())),
            risk_metrics: Arc::new(RwLock::new(RiskMetrics {
                volatility: 0.0,
                downside_volatility: 0.0,
                value_at_risk: 0.0,
                conditional_value_at_risk: 0.0,
                max_loss: 0.0,
                correlation_matrix: DMatrix::identity(1, 1),
                covariance_matrix: DMatrix::identity(1, 1),
            })),
            current_allocation: Arc::new(RwLock::new(AllocationResult {
                allocations: HashMap::new(),
                risk_contributions: HashMap::new(),
                portfolio_volatility: 0.0,
                expected_return: 0.0,
                sharpe_ratio: 0.0,
                diversification_ratio: 1.0,
                timestamp: Instant::now(),
            })),
            risk_budget: HashMap::new(),
            max_leverage,
            risk_free_rate,
            rebalance_frequency: Duration::from_secs(300),
            last_rebalance: Instant::now(),
        }
    }

    pub fn add_strategy(&mut self, strategy: StrategyMetrics) -> Result<(), String> {
        let mut strategies = self.strategies.write().map_err(|_| "Lock error")?;
        
        if strategies.len() >= MAX_STRATEGIES {
            return Err("Maximum strategies reached".to_string());
        }

        let strategy_id = strategy.strategy_id.clone();
        strategies.insert(strategy_id.clone(), strategy);
        
        let equal_risk = 1.0 / (strategies.len() as f64);
        self.risk_budget.insert(strategy_id, equal_risk);
        
        Ok(())
    }

    pub fn update_metrics(&self, strategy_id: &str, returns: Vec<f64>, volumes: Vec<f64>) -> Result<(), String> {
        let mut strategies = self.strategies.write().map_err(|_| "Lock error")?;
        
        if let Some(strategy) = strategies.get_mut(strategy_id) {
            strategy.returns = returns;
            strategy.volumes = volumes;
            strategy.last_update = Instant::now();
            
            let sharpe = self.calculate_sharpe_ratio(&strategy.returns, self.risk_free_rate);
            strategy.sharpe_ratio = sharpe;
            
            let max_dd = self.calculate_max_drawdown(&strategy.returns);
            strategy.max_drawdown = max_dd;
        }
        
        drop(strategies);
        self.update_risk_metrics()?;
        
        Ok(())
    }

    fn update_risk_metrics(&self) -> Result<(), String> {
        let strategies = self.strategies.read().map_err(|_| "Lock error")?;
        
        if strategies.is_empty() {
            return Ok(());
        }

        let strategy_ids: Vec<String> = strategies.keys().cloned().collect();
        let n = strategy_ids.len();
        
        let mut returns_matrix = DMatrix::zeros(RISK_LOOKBACK_PERIODS, n);
        
        for (j, id) in strategy_ids.iter().enumerate() {
            if let Some(strategy) = strategies.get(id) {
                let returns_len = strategy.returns.len().min(RISK_LOOKBACK_PERIODS);
                for i in 0..returns_len {
                    returns_matrix[(i, j)] = strategy.returns[strategy.returns.len() - returns_len + i];
                }
            }
        }

        let cov_matrix = self.calculate_covariance_matrix(&returns_matrix);
        let corr_matrix = self.calculate_correlation_matrix(&cov_matrix);
        
        let mut volatilities = vec![0.0; n];
        for i in 0..n {
            volatilities[i] = cov_matrix[(i, i)].sqrt().max(VOL_FLOOR);
        }

        let portfolio_metrics = self.calculate_portfolio_metrics(&strategies, &strategy_ids, &cov_matrix);
        
        let mut risk_metrics = self.risk_metrics.write().map_err(|_| "Lock error")?;
        risk_metrics.covariance_matrix = cov_matrix;
        risk_metrics.correlation_matrix = corr_matrix;
        risk_metrics.volatility = portfolio_metrics.0;
        risk_metrics.value_at_risk = portfolio_metrics.1;
        risk_metrics.conditional_value_at_risk = portfolio_metrics.2;
        
        Ok(())
    }

    pub fn calculate_optimal_allocation(&self) -> Result<AllocationResult, String> {
        let strategies = self.strategies.read().map_err(|_| "Lock error")?;
        let risk_metrics = self.risk_metrics.read().map_err(|_| "Lock error")?;
        
        if strategies.is_empty() {
            return Err("No strategies available".to_string());
        }

        let strategy_ids: Vec<String> = strategies.keys().cloned().collect();
        let n = strategy_ids.len();
        
        let cov_matrix = &risk_metrics.covariance_matrix;
        if cov_matrix.nrows() != n || cov_matrix.ncols() != n {
            return Err("Covariance matrix dimension mismatch".to_string());
        }

        let mut weights = DVector::from_element(n, 1.0 / n as f64);
        let mut prev_weights = weights.clone();
        
        for iteration in 0..MAX_ITERATIONS {
            let risk_contributions = self.calculate_risk_contributions(&weights, cov_matrix);
            let total_risk = risk_contributions.iter().sum::<f64>();
            
            let mut gradient = DVector::zeros(n);
            for i in 0..n {
                let target_risk = self.risk_budget.get(&strategy_ids[i]).unwrap_or(&(1.0 / n as f64)) * total_risk;
                gradient[i] = risk_contributions[i] - target_risk;
            }
            
            let step_size = self.calculate_adaptive_step_size(iteration);
            weights = &weights - step_size * &gradient;
            
            weights = self.apply_constraints(&weights, &strategies, &strategy_ids);
            
            let convergence = (&weights - &prev_weights).norm() / weights.norm();
            if convergence < CONVERGENCE_THRESHOLD {
                break;
            }
            
            prev_weights = weights.clone();
        }

        let mut allocations = HashMap::new();
        let mut risk_contributions_map = HashMap::new();
        
        let risk_contributions = self.calculate_risk_contributions(&weights, cov_matrix);
        let total_risk = risk_contributions.iter().sum::<f64>();
        
        for (i, id) in strategy_ids.iter().enumerate() {
            let weight_decimal = Decimal::from_f64(weights[i]).unwrap_or(Decimal::ZERO);
            allocations.insert(id.clone(), weight_decimal);
            risk_contributions_map.insert(id.clone(), risk_contributions[i] / total_risk);
        }

        let portfolio_stats = self.calculate_portfolio_statistics(&allocations, &strategies, cov_matrix);
        
        Ok(AllocationResult {
            allocations,
            risk_contributions: risk_contributions_map,
            portfolio_volatility: portfolio_stats.0,
            expected_return: portfolio_stats.1,
            sharpe_ratio: portfolio_stats.2,
            diversification_ratio: portfolio_stats.3,
            timestamp: Instant::now(),
        })
    }

    fn calculate_risk_contributions(&self, weights: &DVector<f64>, cov_matrix: &DMatrix<f64>) -> Vec<f64> {
        let portfolio_var = weights.dot(&(cov_matrix * weights));
        let portfolio_vol = portfolio_var.sqrt();
        
        let marginal_contributions = cov_matrix * weights;
        
        weights.iter()
            .zip(marginal_contributions.iter())
            .map(|(w, mc)| w * mc / portfolio_vol)
            .collect()
    }

    fn calculate_adaptive_step_size(&self, iteration: usize) -> f64 {
        let base_step = 0.1;
        let decay_rate = 0.99;
        base_step * decay_rate.powi(iteration as i32)
    }

    fn apply_constraints(&self, weights: &DVector<f64>, strategies: &HashMap<String, StrategyMetrics>, strategy_ids: &[String]) -> DVector<f64> {
        let mut constrained_weights = weights.clone();
        
        for i in 0..constrained_weights.len() {
            constrained_weights[i] = constrained_weights[i].max(MIN_ALLOCATION.to_f64().unwrap());
            constrained_weights[i] = constrained_weights[i].min(MAX_ALLOCATION.to_f64().unwrap());
            
            if let Some(strategy) = strategies.get(&strategy_ids[i]) {
                if strategy.sharpe_ratio < 0.0 {
                    constrained_weights[i] *= 0.5;
                }
                
                if strategy.max_drawdown > 0.2 {
                    constrained_weights[i] *= 0.7;
                }
                
                if strategy.success_rate < 0.4 {
                    constrained_weights[i] *= 0.6;
                }
            }
        }
        
        let sum = constrained_weights.sum();
        if sum > 0.0 {
            constrained_weights /= sum;
        }
        
        let leverage = constrained_weights.iter().map(|w| w.abs()).sum::<f64>();
        if leverage > self.max_leverage.to_f64().unwrap() {
            constrained_weights *= self.max_leverage.to_f64().unwrap() / leverage;
        }
        
        constrained_weights
    }

    fn calculate_covariance_matrix(&self, returns: &DMatrix<f64>) -> DMatrix<f64> {
        let n_assets = returns.ncols();
        let n_periods = returns.nrows();
        
        let mut weights = DVector::zeros(n_periods);
        for i in 0..n_periods {
            weights[i] = CORRELATION_DECAY_FACTOR.powi((n_periods - i - 1) as i32);
        }
        weights /= weights.sum();
        
        let mut means = DVector::zeros(n_assets);
        for j in 0..n_assets {
            for i in 0..n_periods {
                means[j] += weights[i] * returns[(i, j)];
            }
        }
        
        let mut cov = DMatrix::zeros(n_assets, n_assets);
        for i in 0..n_assets {
            for j in 0..n_assets {
                for k in 0..n_periods {
                    cov[(i, j)] += weights[k] * (returns[(k, i)] - means[i]) * (returns[(k, j)] - means[j]);
                }
            }
        }
        
        cov
    }

    fn calculate_correlation_matrix(&self, cov_matrix: &DMatrix<f64>) -> DMatrix<f64> {
        let n = cov_matrix.nrows();
        let mut corr = DMatrix::zeros(n, n);
        
        for i in 0..n {
            for j in 0..n {
                                let vol_i = cov_matrix[(i, i)].sqrt();
                let vol_j = cov_matrix[(j, j)].sqrt();
                
                if vol_i > VOL_FLOOR && vol_j > VOL_FLOOR {
                    corr[(i, j)] = cov_matrix[(i, j)] / (vol_i * vol_j);
                    corr[(i, j)] = corr[(i, j)].max(-1.0).min(1.0);
                } else {
                    corr[(i, j)] = if i == j { 1.0 } else { 0.0 };
                }
            }
        }
        
        corr
    }

    fn calculate_portfolio_metrics(&self, strategies: &HashMap<String, StrategyMetrics>, strategy_ids: &[String], cov_matrix: &DMatrix<f64>) -> (f64, f64, f64) {
        let current_alloc = self.current_allocation.read().unwrap();
        let n = strategy_ids.len();
        
        if current_alloc.allocations.is_empty() || n == 0 {
            return (0.0, 0.0, 0.0);
        }
        
        let mut weights = DVector::zeros(n);
        for (i, id) in strategy_ids.iter().enumerate() {
            weights[i] = current_alloc.allocations.get(id)
                .and_then(|d| d.to_f64())
                .unwrap_or(0.0);
        }
        
        let portfolio_variance = weights.transpose() * cov_matrix * &weights;
        let portfolio_vol = portfolio_variance[(0, 0)].sqrt();
        
        let mut returns_vec = Vec::new();
        for id in strategy_ids {
            if let Some(strategy) = strategies.get(id) {
                returns_vec.extend(&strategy.returns);
            }
        }
        
        returns_vec.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let var_percentile = (returns_vec.len() as f64 * 0.05) as usize;
        let var = if var_percentile < returns_vec.len() {
            -returns_vec[var_percentile]
        } else {
            0.0
        };
        
        let cvar = if var_percentile > 0 {
            -returns_vec[..var_percentile].iter().sum::<f64>() / var_percentile as f64
        } else {
            var
        };
        
        (portfolio_vol, var, cvar)
    }

    fn calculate_portfolio_statistics(&self, allocations: &HashMap<String, Decimal>, strategies: &HashMap<String, StrategyMetrics>, cov_matrix: &DMatrix<f64>) -> (f64, f64, f64, f64) {
        let strategy_ids: Vec<String> = allocations.keys().cloned().collect();
        let n = strategy_ids.len();
        
        if n == 0 {
            return (0.0, 0.0, 0.0, 1.0);
        }
        
        let mut weights = DVector::zeros(n);
        let mut expected_returns = DVector::zeros(n);
        let mut individual_vols = Vec::with_capacity(n);
        
        for (i, id) in strategy_ids.iter().enumerate() {
            weights[i] = allocations.get(id)
                .and_then(|d| d.to_f64())
                .unwrap_or(0.0);
            
            if let Some(strategy) = strategies.get(id) {
                let avg_return = if !strategy.returns.is_empty() {
                    strategy.returns.iter().sum::<f64>() / strategy.returns.len() as f64
                } else {
                    0.0
                };
                
                expected_returns[i] = avg_return * strategy.success_rate * strategy.gas_efficiency;
                
                let vol = if i < cov_matrix.nrows() {
                    cov_matrix[(i, i)].sqrt()
                } else {
                    0.0
                };
                individual_vols.push(vol);
            }
        }
        
        let portfolio_return = weights.dot(&expected_returns);
        
        let portfolio_variance = if n <= cov_matrix.nrows() && n <= cov_matrix.ncols() {
            let sub_cov = cov_matrix.view((0, 0), (n, n));
            weights.transpose() * sub_cov * &weights
        } else {
            0.0
        };
        
        let portfolio_vol = portfolio_variance.sqrt();
        
        let sharpe_ratio = if portfolio_vol > VOL_FLOOR {
            (portfolio_return - self.risk_free_rate) / portfolio_vol
        } else {
            0.0
        };
        
        let weighted_individual_vol: f64 = weights.iter()
            .zip(individual_vols.iter())
            .map(|(w, v)| w.abs() * v)
            .sum();
        
        let diversification_ratio = if portfolio_vol > VOL_FLOOR {
            weighted_individual_vol / portfolio_vol
        } else {
            1.0
        };
        
        (portfolio_vol, portfolio_return, sharpe_ratio, diversification_ratio)
    }

    fn calculate_sharpe_ratio(&self, returns: &[f64], risk_free_rate: f64) -> f64 {
        if returns.len() < 2 {
            return 0.0;
        }
        
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / (returns.len() - 1) as f64;
        
        let std_dev = variance.sqrt();
        
        if std_dev > VOL_FLOOR {
            (mean_return - risk_free_rate) / std_dev
        } else {
            0.0
        }
    }

    fn calculate_max_drawdown(&self, returns: &[f64]) -> f64 {
        if returns.is_empty() {
            return 0.0;
        }
        
        let mut cumulative_returns = vec![1.0];
        for r in returns {
            cumulative_returns.push(cumulative_returns.last().unwrap() * (1.0 + r));
        }
        
        let mut max_drawdown = 0.0;
        let mut peak = cumulative_returns[0];
        
        for &value in &cumulative_returns[1..] {
            if value > peak {
                peak = value;
            }
            let drawdown = (peak - value) / peak;
            if drawdown > max_drawdown {
                max_drawdown = drawdown;
            }
        }
        
        max_drawdown
    }

    pub fn should_rebalance(&self) -> bool {
        if self.last_rebalance.elapsed() < self.rebalance_frequency {
            return false;
        }
        
        let current_alloc = match self.current_allocation.read() {
            Ok(alloc) => alloc,
            Err(_) => return false,
        };
        
        if current_alloc.allocations.is_empty() {
            return true;
        }
        
        match self.calculate_optimal_allocation() {
            Ok(new_alloc) => {
                let mut max_deviation = Decimal::ZERO;
                
                for (strategy_id, current_weight) in &current_alloc.allocations {
                    let new_weight = new_alloc.allocations.get(strategy_id)
                        .cloned()
                        .unwrap_or(Decimal::ZERO);
                    
                    let deviation = (new_weight - current_weight).abs();
                    if deviation > max_deviation {
                        max_deviation = deviation;
                    }
                }
                
                max_deviation > REBALANCE_THRESHOLD
            }
            Err(_) => false,
        }
    }

    pub fn execute_rebalance(&self) -> Result<AllocationResult, String> {
        let new_allocation = self.calculate_optimal_allocation()?;
        
        let risk_metrics = self.risk_metrics.read().map_err(|_| "Lock error")?;
        let emergency_mode = risk_metrics.volatility > 0.4 || 
                          risk_metrics.value_at_risk > 0.15 ||
                          risk_metrics.max_loss > 0.25;
        
        let final_allocation = if emergency_mode {
            self.apply_emergency_risk_management(new_allocation)
        } else {
            new_allocation
        };
        
        let mut current_alloc = self.current_allocation.write().map_err(|_| "Lock error")?;
        *current_alloc = final_allocation.clone();
        
        Ok(final_allocation)
    }

    fn apply_emergency_risk_management(&self, mut allocation: AllocationResult) -> AllocationResult {
        let risk_reduction_factor = 1.0 / EMERGENCY_RISK_MULTIPLIER.to_f64().unwrap();
        
        for (_, weight) in allocation.allocations.iter_mut() {
            *weight = (*weight * Decimal::from_f64(risk_reduction_factor).unwrap())
                .max(MIN_ALLOCATION);
        }
        
        let total_weight: Decimal = allocation.allocations.values().sum();
        if total_weight > Decimal::ZERO && total_weight < Decimal::ONE {
            let cash_weight = Decimal::ONE - total_weight;
            allocation.allocations.insert("CASH_RESERVE".to_string(), cash_weight);
        }
        
        allocation.portfolio_volatility *= risk_reduction_factor;
        
        allocation
    }

    pub fn get_current_allocation(&self) -> Result<AllocationResult, String> {
        let allocation = self.current_allocation.read().map_err(|_| "Lock error")?;
        Ok(allocation.clone())
    }

    pub fn get_strategy_performance(&self, strategy_id: &str) -> Result<StrategyMetrics, String> {
        let strategies = self.strategies.read().map_err(|_| "Lock error")?;
        strategies.get(strategy_id)
            .cloned()
            .ok_or_else(|| format!("Strategy {} not found", strategy_id))
    }

    pub fn remove_strategy(&mut self, strategy_id: &str) -> Result<(), String> {
        let mut strategies = self.strategies.write().map_err(|_| "Lock error")?;
        strategies.remove(strategy_id);
        self.risk_budget.remove(strategy_id);
        
        if !strategies.is_empty() {
            let equal_risk = 1.0 / strategies.len() as f64;
            for (id, _) in self.risk_budget.iter_mut() {
                *_ = equal_risk;
            }
        }
        
        Ok(())
    }

    pub fn update_risk_budget(&mut self, strategy_id: &str, risk_budget: f64) -> Result<(), String> {
        if risk_budget <= 0.0 || risk_budget >= 1.0 {
            return Err("Risk budget must be between 0 and 1".to_string());
        }
        
        self.risk_budget.insert(strategy_id.to_string(), risk_budget);
        
        let total_budget: f64 = self.risk_budget.values().sum();
        if (total_budget - 1.0).abs() > 1e-6 {
            for (_, budget) in self.risk_budget.iter_mut() {
                *budget /= total_budget;
            }
        }
        
        Ok(())
    }

    pub fn get_risk_metrics(&self) -> Result<RiskMetrics, String> {
        let metrics = self.risk_metrics.read().map_err(|_| "Lock error")?;
        Ok(metrics.clone())
    }

    pub fn set_rebalance_frequency(&mut self, duration: Duration) {
        self.rebalance_frequency = duration;
    }

    pub fn emergency_shutdown(&self) -> Result<AllocationResult, String> {
        let mut allocation = AllocationResult {
            allocations: HashMap::new(),
            risk_contributions: HashMap::new(),
            portfolio_volatility: 0.0,
            expected_return: 0.0,
            sharpe_ratio: 0.0,
            diversification_ratio: 1.0,
            timestamp: Instant::now(),
        };
        
        allocation.allocations.insert("CASH_RESERVE".to_string(), Decimal::ONE);
        
        let mut current_alloc = self.current_allocation.write().map_err(|_| "Lock error")?;
        *current_alloc = allocation.clone();
        
        Ok(allocation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_risk_parity_allocator() {
        let mut allocator = RiskParityAllocator::new(dec!(2.0), 0.02);
        
        let strategy = StrategyMetrics {
            strategy_id: "test_strategy".to_string(),
            pubkey: Pubkey::new_unique(),
            returns: vec![0.01, 0.02, -0.01, 0.03, 0.01],
            volumes: vec![1000.0, 1500.0, 800.0, 2000.0, 1200.0],
            success_rate: 0.7,
            sharpe_ratio: 1.5,
            max_drawdown: 0.1,
            avg_slippage: 0.001,
            gas_efficiency: 0.95,
            last_update: Instant::now(),
        };
        
        assert!(allocator.add_strategy(strategy).is_ok());
        assert!(allocator.calculate_optimal_allocation().is_ok());
    }
}

