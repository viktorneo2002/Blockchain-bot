use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;

const RISK_FREE_RATE: Decimal = dec!(0.0001); // 0.01% hourly
const MIN_OBSERVATIONS: usize = 30;
const MAX_OBSERVATIONS: usize = 1000;
const CONFIDENCE_LEVEL: Decimal = dec!(1.96); // 95% confidence
const MAX_POSITION_FRACTION: Decimal = dec!(0.25);
const MIN_SHARPE_THRESHOLD: Decimal = dec!(0.5);
const VOLATILITY_WINDOW: usize = 20;
const SHARPE_WINDOW: usize = 100;
const MAX_DRAWDOWN_TOLERANCE: Decimal = dec!(0.15);
const POSITION_SCALE_FACTOR: Decimal = dec!(0.7);
const MIN_TRADE_SIZE: Decimal = dec!(0.001);
const SHARPE_DECAY_FACTOR: Decimal = dec!(0.995);
const CORRELATION_WINDOW: usize = 50;
const MAX_CORRELATION: Decimal = dec!(0.7);
const DYNAMIC_ADJUSTMENT_RATE: Decimal = dec!(0.05);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeResult {
    pub strategy_id: String,
    pub timestamp: u64,
    pub pnl: Decimal,
    pub capital_deployed: Decimal,
    pub gas_cost: Decimal,
    pub slippage: Decimal,
}

#[derive(Debug, Clone)]
pub struct StrategyMetrics {
    returns: VecDeque<Decimal>,
    timestamps: VecDeque<u64>,
    cumulative_return: Decimal,
    peak_value: Decimal,
    current_drawdown: Decimal,
    sharpe_ratio: Decimal,
    volatility: Decimal,
    win_rate: Decimal,
    total_trades: u64,
    winning_trades: u64,
    avg_win: Decimal,
    avg_loss: Decimal,
    last_update: u64,
    ewma_return: Decimal,
    ewma_variance: Decimal,
}

#[derive(Debug, Clone)]
pub struct CorrelationMatrix {
    correlations: HashMap<(String, String), Decimal>,
    last_update: u64,
}

pub struct SharpeRatioMaximizer {
    strategy_metrics: Arc<RwLock<HashMap<String, StrategyMetrics>>>,
    correlation_matrix: Arc<RwLock<CorrelationMatrix>>,
    total_capital: Arc<RwLock<Decimal>>,
    allocated_capital: Arc<RwLock<HashMap<String, Decimal>>>,
    risk_budget: Arc<RwLock<Decimal>>,
    max_var: Decimal,
    lookback_period: Duration,
}

impl StrategyMetrics {
    fn new() -> Self {
        Self {
            returns: VecDeque::with_capacity(MAX_OBSERVATIONS),
            timestamps: VecDeque::with_capacity(MAX_OBSERVATIONS),
            cumulative_return: Decimal::ONE,
            peak_value: Decimal::ONE,
            current_drawdown: Decimal::ZERO,
            sharpe_ratio: Decimal::ZERO,
            volatility: Decimal::ZERO,
            win_rate: Decimal::ZERO,
            total_trades: 0,
            winning_trades: 0,
            avg_win: Decimal::ZERO,
            avg_loss: Decimal::ZERO,
            last_update: 0,
            ewma_return: Decimal::ZERO,
            ewma_variance: Decimal::ZERO,
        }
    }

    fn update(&mut self, result: &TradeResult) {
        let return_rate = if result.capital_deployed > Decimal::ZERO {
            (result.pnl - result.gas_cost) / result.capital_deployed
        } else {
            return;
        };

        self.returns.push_back(return_rate);
        self.timestamps.push_back(result.timestamp);

        if self.returns.len() > MAX_OBSERVATIONS {
            self.returns.pop_front();
            self.timestamps.pop_front();
        }

        self.cumulative_return *= Decimal::ONE + return_rate;
        
        if self.cumulative_return > self.peak_value {
            self.peak_value = self.cumulative_return;
        }
        
        self.current_drawdown = (self.peak_value - self.cumulative_return) / self.peak_value;

        self.total_trades += 1;
        if result.pnl > result.gas_cost {
            self.winning_trades += 1;
            let win_amount = result.pnl - result.gas_cost;
            self.avg_win = (self.avg_win * Decimal::from(self.winning_trades - 1) + win_amount) 
                / Decimal::from(self.winning_trades);
        } else {
            let loss_amount = result.gas_cost - result.pnl;
            let losing_trades = self.total_trades - self.winning_trades;
            self.avg_loss = (self.avg_loss * Decimal::from(losing_trades - 1) + loss_amount)
                / Decimal::from(losing_trades);
        }

        self.win_rate = Decimal::from(self.winning_trades) / Decimal::from(self.total_trades);

        let alpha = dec!(0.06);
        self.ewma_return = alpha * return_rate + (Decimal::ONE - alpha) * self.ewma_return;
        let deviation = return_rate - self.ewma_return;
        self.ewma_variance = alpha * deviation * deviation + (Decimal::ONE - alpha) * self.ewma_variance;

        self.calculate_metrics();
        self.last_update = result.timestamp;
    }

    fn calculate_metrics(&mut self) {
        if self.returns.len() < MIN_OBSERVATIONS {
            return;
        }

        let n = Decimal::from(self.returns.len());
        let mean_return = self.returns.iter().sum::<Decimal>() / n;

        let variance = self.returns.iter()
            .map(|r| (*r - mean_return).powi(2))
            .sum::<Decimal>() / (n - Decimal::ONE);

        self.volatility = variance.sqrt();

        if self.volatility > Decimal::ZERO {
            let excess_return = mean_return - RISK_FREE_RATE;
            self.sharpe_ratio = excess_return / self.volatility * Decimal::from(24);
        }
    }

    fn get_kelly_fraction(&self) -> Decimal {
        if self.avg_loss == Decimal::ZERO || self.win_rate == Decimal::ZERO {
            return Decimal::ZERO;
        }

        let loss_rate = Decimal::ONE - self.win_rate;
        let win_loss_ratio = self.avg_win / self.avg_loss;
        
        let kelly = (self.win_rate * win_loss_ratio - loss_rate) / win_loss_ratio;
        
        kelly.max(Decimal::ZERO).min(MAX_POSITION_FRACTION)
    }

    fn get_confidence_adjusted_sharpe(&self) -> Decimal {
        if self.returns.len() < MIN_OBSERVATIONS {
            return Decimal::ZERO;
        }

        let n = Decimal::from(self.returns.len());
        let se = Decimal::ONE / n.sqrt();
        let confidence_adjustment = CONFIDENCE_LEVEL * se;
        
        (self.sharpe_ratio - confidence_adjustment).max(Decimal::ZERO)
    }
}

impl SharpeRatioMaximizer {
    pub fn new(total_capital: Decimal, max_var: Decimal) -> Self {
        Self {
            strategy_metrics: Arc::new(RwLock::new(HashMap::new())),
            correlation_matrix: Arc::new(RwLock::new(CorrelationMatrix {
                correlations: HashMap::new(),
                last_update: 0,
            })),
            total_capital: Arc::new(RwLock::new(total_capital)),
            allocated_capital: Arc::new(RwLock::new(HashMap::new())),
            risk_budget: Arc::new(RwLock::new(total_capital * max_var)),
            max_var,
            lookback_period: Duration::from_secs(3600 * 24),
        }
    }

    pub fn record_trade(&self, result: TradeResult) -> Result<(), Box<dyn std::error::Error>> {
        let mut metrics = self.strategy_metrics.write().unwrap();
        let strategy_metrics = metrics.entry(result.strategy_id.clone())
            .or_insert_with(StrategyMetrics::new);
        
        strategy_metrics.update(&result);
        
        drop(metrics);
        
        self.update_correlations();
        self.rebalance_allocations()?;
        
        Ok(())
    }

    pub fn get_position_size(&self, strategy_id: &str, opportunity_size: Decimal) -> Decimal {
        let metrics = self.strategy_metrics.read().unwrap();
        let allocated = self.allocated_capital.read().unwrap();
        
        let available_capital = allocated.get(strategy_id).copied().unwrap_or(Decimal::ZERO);
        
        if available_capital <= MIN_TRADE_SIZE {
            return Decimal::ZERO;
        }

        if let Some(strategy_metrics) = metrics.get(strategy_id) {
            if strategy_metrics.current_drawdown > MAX_DRAWDOWN_TOLERANCE {
                return Decimal::ZERO;
            }

            let confidence_sharpe = strategy_metrics.get_confidence_adjusted_sharpe();
            if confidence_sharpe < MIN_SHARPE_THRESHOLD {
                return MIN_TRADE_SIZE.min(available_capital);
            }

            let kelly_fraction = strategy_metrics.get_kelly_fraction();
            let sharpe_adjustment = (confidence_sharpe / dec!(3.0)).min(Decimal::ONE);
            let volatility_adjustment = (dec!(0.02) / (strategy_metrics.volatility + dec!(0.001))).min(Decimal::ONE);
            
            let position_fraction = kelly_fraction * sharpe_adjustment * volatility_adjustment * POSITION_SCALE_FACTOR;
            let max_position = available_capital * position_fraction;
            
            max_position.min(opportunity_size).max(MIN_TRADE_SIZE)
        } else {
            MIN_TRADE_SIZE.min(available_capital)
        }
    }

    fn update_correlations(&self) {
        let mut metrics = self.strategy_metrics.read().unwrap();
        let strategies: Vec<String> = metrics.keys().cloned().collect();
        
        if strategies.len() < 2 {
            return;
        }

        let mut correlations = self.correlation_matrix.write().unwrap();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

        for i in 0..strategies.len() {
            for j in i+1..strategies.len() {
                let strat1 = &strategies[i];
                let strat2 = &strategies[j];
                
                if let (Some(metrics1), Some(metrics2)) = (metrics.get(strat1), metrics.get(strat2)) {
                    if metrics1.returns.len() >= CORRELATION_WINDOW && metrics2.returns.len() >= CORRELATION_WINDOW {
                        let corr = self.calculate_correlation(&metrics1.returns, &metrics2.returns);
                        correlations.correlations.insert((strat1.clone(), strat2.clone()), corr);
                        correlations.correlations.insert((strat2.clone(), strat1.clone()), corr);
                    }
                }
            }
        }
        
        correlations.last_update = now;
    }

    fn calculate_correlation(&self, returns1: &VecDeque<Decimal>, returns2: &VecDeque<Decimal>) -> Decimal {
        let n = returns1.len().min(returns2.len()).min(CORRELATION_WINDOW);
        if n < 10 {
            return Decimal::ZERO;
        }

        let r1: Vec<Decimal> = returns1.iter().rev().take(n).copied().collect();
        let r2: Vec<Decimal> = returns2.iter().rev().take(n).copied().collect();

        let mean1 = r1.iter().sum::<Decimal>() / Decimal::from(n);
        let mean2 = r2.iter().sum::<Decimal>() / Decimal::from(n);

        let mut cov = Decimal::ZERO;
        let mut var1 = Decimal::ZERO;
        let mut var2 = Decimal::ZERO;

        for i in 0..n {
            let dev1 = r1[i] - mean1;
            let dev2 = r2[i] - mean2;
            cov += dev1 * dev2;
            var1 += dev1 * dev1;
            var2 += dev2 * dev2;
        }

        if var1 > Decimal::ZERO && var2 > Decimal::ZERO {
            cov / (var1.sqrt() * var2.sqrt())
        } else {
            Decimal::ZERO
        }
    }

    fn rebalance_allocations(&self) -> Result<(), Box<dyn std::error::Error>> {
        let metrics = self.strategy_metrics.read().unwrap();
        let total_capital = *self.total_capital.read().unwrap();
        let correlations = self.correlation_matrix.read().unwrap();
        
        let mut strategy_scores: HashMap<String, Decimal> = HashMap::new();
        let mut total_score = Decimal::ZERO;

        for (strategy_id, strategy_metrics) in metrics.iter() {
            if strategy_metrics.returns.len() < MIN_OBSERVATIONS {
                continue;
            }

            let confidence_sharpe = strategy_metrics.get_confidence_adjusted_sharpe();
            if confidence_sharpe < MIN_SHARPE_THRESHOLD || strategy_metrics.current_drawdown > MAX_DRAWDOWN_TOLERANCE {
                continue;
            }

                        let recency_weight = SHARPE_DECAY_FACTOR.powi(
                ((SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() - strategy_metrics.last_update) / 3600) as i32
            );

            let kelly_fraction = strategy_metrics.get_kelly_fraction();
            let volatility_penalty = (dec!(1.0) / (dec!(1.0) + strategy_metrics.volatility * dec!(10.0)));
            let win_rate_bonus = strategy_metrics.win_rate.powf(dec!(2.0));
            
            let mut correlation_penalty = Decimal::ONE;
            for (other_strategy, other_metrics) in metrics.iter() {
                if other_strategy != strategy_id && strategy_scores.contains_key(other_strategy) {
                    let key = (strategy_id.clone(), other_strategy.clone());
                    if let Some(&corr) = correlations.correlations.get(&key) {
                        correlation_penalty *= (Decimal::ONE - corr.abs() * dec!(0.5));
                    }
                }
            }

            let score = confidence_sharpe * kelly_fraction * volatility_penalty * 
                       win_rate_bonus * recency_weight * correlation_penalty;
            
            if score > Decimal::ZERO {
                strategy_scores.insert(strategy_id.clone(), score);
                total_score += score;
            }
        }

        drop(metrics);
        drop(correlations);

        let mut new_allocations = HashMap::new();
        let risk_budget = *self.risk_budget.read().unwrap();

        if total_score > Decimal::ZERO {
            for (strategy_id, score) in strategy_scores {
                let weight = score / total_score;
                let target_allocation = total_capital * weight * dec!(0.95);
                
                let metrics = self.strategy_metrics.read().unwrap();
                if let Some(strategy_metrics) = metrics.get(&strategy_id) {
                    let var_contribution = target_allocation * strategy_metrics.volatility;
                    if var_contribution <= risk_budget * weight {
                        new_allocations.insert(strategy_id, target_allocation);
                    } else {
                        let risk_adjusted_allocation = (risk_budget * weight) / strategy_metrics.volatility;
                        new_allocations.insert(strategy_id, risk_adjusted_allocation);
                    }
                }
            }
        }

        let current_allocations = self.allocated_capital.read().unwrap();
        let mut updated_allocations = current_allocations.clone();
        
        for (strategy_id, target_allocation) in new_allocations {
            let current = updated_allocations.get(&strategy_id).copied().unwrap_or(Decimal::ZERO);
            let adjustment = (target_allocation - current) * DYNAMIC_ADJUSTMENT_RATE;
            let new_allocation = (current + adjustment).max(Decimal::ZERO);
            updated_allocations.insert(strategy_id, new_allocation);
        }

        let total_allocated: Decimal = updated_allocations.values().sum();
        if total_allocated > total_capital {
            let scale_factor = total_capital / total_allocated;
            for allocation in updated_allocations.values_mut() {
                *allocation *= scale_factor;
            }
        }

        *self.allocated_capital.write().unwrap() = updated_allocations;
        
        Ok(())
    }

    pub fn get_strategy_allocation(&self, strategy_id: &str) -> Decimal {
        self.allocated_capital.read().unwrap()
            .get(strategy_id)
            .copied()
            .unwrap_or(Decimal::ZERO)
    }

    pub fn get_portfolio_metrics(&self) -> PortfolioMetrics {
        let metrics = self.strategy_metrics.read().unwrap();
        let allocations = self.allocated_capital.read().unwrap();
        let total_capital = *self.total_capital.read().unwrap();
        
        let mut portfolio_return = Decimal::ZERO;
        let mut portfolio_variance = Decimal::ZERO;
        let mut active_strategies = 0;
        let mut total_trades = 0u64;
        let mut total_winning_trades = 0u64;
        
        let weights: HashMap<String, Decimal> = allocations.iter()
            .map(|(id, alloc)| (id.clone(), *alloc / total_capital))
            .collect();
        
        for (strategy_id, strategy_metrics) in metrics.iter() {
            if let Some(&weight) = weights.get(strategy_id) {
                if weight > Decimal::ZERO && strategy_metrics.returns.len() >= MIN_OBSERVATIONS {
                    let mean_return = strategy_metrics.returns.iter().sum::<Decimal>() 
                        / Decimal::from(strategy_metrics.returns.len());
                    portfolio_return += weight * mean_return;
                    portfolio_variance += weight * weight * strategy_metrics.volatility * strategy_metrics.volatility;
                    active_strategies += 1;
                    total_trades += strategy_metrics.total_trades;
                    total_winning_trades += strategy_metrics.winning_trades;
                }
            }
        }
        
        let correlations = self.correlation_matrix.read().unwrap();
        for (strategy1, weight1) in &weights {
            for (strategy2, weight2) in &weights {
                if strategy1 < strategy2 {
                    let key = (strategy1.clone(), strategy2.clone());
                    if let Some(&correlation) = correlations.correlations.get(&key) {
                        if let (Some(metrics1), Some(metrics2)) = (metrics.get(strategy1), metrics.get(strategy2)) {
                            portfolio_variance += dec!(2.0) * weight1 * weight2 * 
                                correlation * metrics1.volatility * metrics2.volatility;
                        }
                    }
                }
            }
        }
        
        let portfolio_volatility = portfolio_variance.sqrt();
        let portfolio_sharpe = if portfolio_volatility > Decimal::ZERO {
            (portfolio_return - RISK_FREE_RATE) / portfolio_volatility * Decimal::from(24)
        } else {
            Decimal::ZERO
        };
        
        PortfolioMetrics {
            total_capital,
            allocated_capital: allocations.values().sum(),
            portfolio_return,
            portfolio_volatility,
            portfolio_sharpe,
            active_strategies,
            total_trades,
            win_rate: if total_trades > 0 {
                Decimal::from(total_winning_trades) / Decimal::from(total_trades)
            } else {
                Decimal::ZERO
            },
            value_at_risk: self.calculate_var(&metrics, &weights),
        }
    }

    fn calculate_var(&self, metrics: &HashMap<String, StrategyMetrics>, weights: &HashMap<String, Decimal>) -> Decimal {
        let mut portfolio_returns = Vec::new();
        let min_length = metrics.values()
            .map(|m| m.returns.len())
            .min()
            .unwrap_or(0);
        
        if min_length < MIN_OBSERVATIONS {
            return Decimal::ZERO;
        }
        
        for i in 0..min_length.min(SHARPE_WINDOW) {
            let mut portfolio_return = Decimal::ZERO;
            for (strategy_id, strategy_metrics) in metrics.iter() {
                if let Some(&weight) = weights.get(strategy_id) {
                    if let Some(&ret) = strategy_metrics.returns.get(i) {
                        portfolio_return += weight * ret;
                    }
                }
            }
            portfolio_returns.push(portfolio_return);
        }
        
        portfolio_returns.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let var_index = ((portfolio_returns.len() as f64) * 0.05) as usize;
        
        portfolio_returns.get(var_index).copied().unwrap_or(Decimal::ZERO).abs()
    }

    pub fn should_trade(&self, strategy_id: &str, expected_return: Decimal, expected_cost: Decimal) -> bool {
        let metrics = self.strategy_metrics.read().unwrap();
        
        if let Some(strategy_metrics) = metrics.get(strategy_id) {
            if strategy_metrics.current_drawdown > MAX_DRAWDOWN_TOLERANCE {
                return false;
            }
            
            let net_expected_return = expected_return - expected_cost;
            if net_expected_return <= Decimal::ZERO {
                return false;
            }
            
            let confidence_sharpe = strategy_metrics.get_confidence_adjusted_sharpe();
            if confidence_sharpe < MIN_SHARPE_THRESHOLD && strategy_metrics.returns.len() >= MIN_OBSERVATIONS {
                return false;
            }
            
            let required_return = strategy_metrics.volatility * dec!(0.5) + expected_cost * dec!(1.5);
            if expected_return < required_return {
                return false;
            }
            
            let allocations = self.allocated_capital.read().unwrap();
            let allocated = allocations.get(strategy_id).copied().unwrap_or(Decimal::ZERO);
            
            allocated >= MIN_TRADE_SIZE
        } else {
            expected_return > expected_cost * dec!(3.0)
        }
    }

    pub fn update_capital(&self, new_capital: Decimal) -> Result<(), Box<dyn std::error::Error>> {
        if new_capital <= Decimal::ZERO {
            return Err("Capital must be positive".into());
        }
        
        let old_capital = *self.total_capital.read().unwrap();
        let scale_factor = new_capital / old_capital;
        
        *self.total_capital.write().unwrap() = new_capital;
        *self.risk_budget.write().unwrap() = new_capital * self.max_var;
        
        let mut allocations = self.allocated_capital.write().unwrap();
        for allocation in allocations.values_mut() {
            *allocation *= scale_factor;
        }
        
        Ok(())
    }

    pub fn emergency_stop(&self, strategy_id: &str) {
        let mut allocations = self.allocated_capital.write().unwrap();
        allocations.insert(strategy_id.to_string(), Decimal::ZERO);
    }

    pub fn get_risk_metrics(&self, strategy_id: &str) -> Option<RiskMetrics> {
        let metrics = self.strategy_metrics.read().unwrap();
        
        metrics.get(strategy_id).map(|m| {
            let sortino_ratio = self.calculate_sortino_ratio(&m.returns);
            let max_consecutive_losses = self.calculate_max_consecutive_losses(&m.returns);
            let calmar_ratio = if m.current_drawdown > Decimal::ZERO {
                m.sharpe_ratio / m.current_drawdown
            } else {
                m.sharpe_ratio
            };
            
            RiskMetrics {
                volatility: m.volatility,
                downside_volatility: self.calculate_downside_volatility(&m.returns),
                sharpe_ratio: m.sharpe_ratio,
                sortino_ratio,
                calmar_ratio,
                max_drawdown: m.current_drawdown,
                value_at_risk: self.calculate_strategy_var(&m.returns),
                conditional_var: self.calculate_cvar(&m.returns),
                win_rate: m.win_rate,
                profit_factor: if m.avg_loss > Decimal::ZERO {
                    (m.avg_win * m.win_rate) / (m.avg_loss * (Decimal::ONE - m.win_rate))
                } else {
                    Decimal::MAX
                },
                max_consecutive_losses,
            }
        })
    }

    fn calculate_sortino_ratio(&self, returns: &VecDeque<Decimal>) -> Decimal {
        if returns.len() < MIN_OBSERVATIONS {
            return Decimal::ZERO;
        }
        
        let mean_return = returns.iter().sum::<Decimal>() / Decimal::from(returns.len());
        let downside_returns: Vec<Decimal> = returns.iter()
            .filter(|&&r| r < RISK_FREE_RATE)
            .map(|&r| (r - RISK_FREE_RATE).powi(2))
            .collect();
        
        if downside_returns.is_empty() {
            return Decimal::MAX;
        }
        
        let downside_variance = downside_returns.iter().sum::<Decimal>() / Decimal::from(downside_returns.len());
        let downside_volatility = downside_variance.sqrt();
        
        if downside_volatility > Decimal::ZERO {
            (mean_return - RISK_FREE_RATE) / downside_volatility * Decimal::from(24)
        } else {
            Decimal::MAX
        }
    }

    fn calculate_downside_volatility(&self, returns: &VecDeque<Decimal>) -> Decimal {
        let downside_returns: Vec<Decimal> = returns.iter()
            .filter(|&&r| r < Decimal::ZERO)
            .map(|&r| r * r)
            .collect();
        
        if downside_returns.is_empty() {
            return Decimal::ZERO;
        }
        
        let variance = downside_returns.iter().sum::<Decimal>() / Decimal::from(downside_returns.len());
        variance.sqrt()
    }

    fn calculate_max_consecutive_losses(&self, returns: &VecDeque<Decimal>) -> u32 {
        let mut max_losses = 0u32;
        let mut current_losses = 0u32;
        
        for &ret in returns {
            if ret < Decimal::ZERO {
                current_losses += 1;
                max_losses = max_losses.max(current_losses);
            } else {
                current_losses = 0;
            }
        }
        
        max_losses
    }

    fn calculate_strategy_var(&self, returns: &VecDeque<Decimal>) -> Decimal {
        if returns.len() < MIN_OBSERVATIONS {
            return Decimal::ZERO;
        }
        
        let mut sorted_returns: Vec<Decimal> = returns.iter().copied().collect();
        sorted_returns.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let var_index = ((sorted_returns.len() as f64) * 0.05) as usize;
        sorted_returns.get(var_index).copied().unwrap_or(Decimal::ZERO).abs()
    }

    fn calculate_cvar(&self, returns: &VecDeque<Decimal>) -> Decimal {
        if returns.len() < MIN_OBSERVATIONS {
            return Decimal::ZERO;
        }
        
                let mut sorted_returns: Vec<Decimal> = returns.iter().copied().collect();
        sorted_returns.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let var_index = ((sorted_returns.len() as f64) * 0.05) as usize;
        let worst_returns: Vec<Decimal> = sorted_returns.iter()
            .take(var_index.max(1))
            .copied()
            .collect();
        
        if worst_returns.is_empty() {
            Decimal::ZERO
        } else {
            worst_returns.iter().sum::<Decimal>() / Decimal::from(worst_returns.len())
        }.abs()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioMetrics {
    pub total_capital: Decimal,
    pub allocated_capital: Decimal,
    pub portfolio_return: Decimal,
    pub portfolio_volatility: Decimal,
    pub portfolio_sharpe: Decimal,
    pub active_strategies: usize,
    pub total_trades: u64,
    pub win_rate: Decimal,
    pub value_at_risk: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskMetrics {
    pub volatility: Decimal,
    pub downside_volatility: Decimal,
    pub sharpe_ratio: Decimal,
    pub sortino_ratio: Decimal,
    pub calmar_ratio: Decimal,
    pub max_drawdown: Decimal,
    pub value_at_risk: Decimal,
    pub conditional_var: Decimal,
    pub win_rate: Decimal,
    pub profit_factor: Decimal,
    pub max_consecutive_losses: u32,
}

impl SharpeRatioMaximizer {
    pub fn get_optimal_strategies(&self, n: usize) -> Vec<(String, Decimal)> {
        let metrics = self.strategy_metrics.read().unwrap();
        let mut strategy_rankings: Vec<(String, Decimal)> = Vec::new();
        
        for (strategy_id, strategy_metrics) in metrics.iter() {
            if strategy_metrics.returns.len() >= MIN_OBSERVATIONS {
                let score = strategy_metrics.get_confidence_adjusted_sharpe() * 
                           strategy_metrics.win_rate * 
                           (Decimal::ONE - strategy_metrics.current_drawdown);
                
                if score > Decimal::ZERO {
                    strategy_rankings.push((strategy_id.clone(), score));
                }
            }
        }
        
        strategy_rankings.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        strategy_rankings.truncate(n);
        strategy_rankings
    }

    pub fn calculate_portfolio_efficiency(&self) -> Decimal {
        let portfolio_metrics = self.get_portfolio_metrics();
        let metrics = self.strategy_metrics.read().unwrap();
        
        if metrics.is_empty() {
            return Decimal::ZERO;
        }
        
        let best_individual_sharpe = metrics.values()
            .filter(|m| m.returns.len() >= MIN_OBSERVATIONS)
            .map(|m| m.sharpe_ratio)
            .max()
            .unwrap_or(Decimal::ZERO);
        
        if best_individual_sharpe > Decimal::ZERO {
            portfolio_metrics.portfolio_sharpe / best_individual_sharpe
        } else {
            Decimal::ZERO
        }
    }

    pub fn get_strategy_recommendation(&self, strategy_id: &str, market_volatility: Decimal) -> StrategyRecommendation {
        let metrics = self.strategy_metrics.read().unwrap();
        let allocations = self.allocated_capital.read().unwrap();
        
        let current_allocation = allocations.get(strategy_id).copied().unwrap_or(Decimal::ZERO);
        
        if let Some(strategy_metrics) = metrics.get(strategy_id) {
            let confidence_sharpe = strategy_metrics.get_confidence_adjusted_sharpe();
            let volatility_ratio = strategy_metrics.volatility / market_volatility.max(dec!(0.001));
            
            let action = if strategy_metrics.current_drawdown > MAX_DRAWDOWN_TOLERANCE {
                Action::Exit
            } else if confidence_sharpe < MIN_SHARPE_THRESHOLD {
                Action::Reduce
            } else if confidence_sharpe > dec!(2.0) && volatility_ratio < dec!(1.5) {
                Action::Increase
            } else {
                Action::Hold
            };
            
            let recommended_size = match action {
                Action::Exit => Decimal::ZERO,
                Action::Reduce => current_allocation * dec!(0.5),
                Action::Increase => {
                    let kelly = strategy_metrics.get_kelly_fraction();
                    let target = self.total_capital.read().unwrap().clone() * kelly * POSITION_SCALE_FACTOR;
                    current_allocation + (target - current_allocation) * DYNAMIC_ADJUSTMENT_RATE
                },
                Action::Hold => current_allocation,
            };
            
            StrategyRecommendation {
                strategy_id: strategy_id.to_string(),
                action,
                current_allocation,
                recommended_allocation: recommended_size,
                confidence: confidence_sharpe / dec!(3.0),
                risk_score: strategy_metrics.volatility * (Decimal::ONE + strategy_metrics.current_drawdown),
            }
        } else {
            StrategyRecommendation {
                strategy_id: strategy_id.to_string(),
                action: Action::Hold,
                current_allocation,
                recommended_allocation: MIN_TRADE_SIZE,
                confidence: Decimal::ZERO,
                risk_score: Decimal::ONE,
            }
        }
    }

    pub fn optimize_portfolio_weights(&self) -> HashMap<String, Decimal> {
        let metrics = self.strategy_metrics.read().unwrap();
        let total_capital = *self.total_capital.read().unwrap();
        let mut optimized_weights = HashMap::new();
        
        let eligible_strategies: Vec<(String, &StrategyMetrics)> = metrics.iter()
            .filter(|(_, m)| {
                m.returns.len() >= MIN_OBSERVATIONS &&
                m.get_confidence_adjusted_sharpe() >= MIN_SHARPE_THRESHOLD &&
                m.current_drawdown <= MAX_DRAWDOWN_TOLERANCE
            })
            .map(|(id, m)| (id.clone(), m))
            .collect();
        
        if eligible_strategies.is_empty() {
            return optimized_weights;
        }
        
        let correlations = self.correlation_matrix.read().unwrap();
        let mut weights: Vec<Decimal> = vec![Decimal::ONE / Decimal::from(eligible_strategies.len()); eligible_strategies.len()];
        
        for _ in 0..100 {
            let mut gradients = vec![Decimal::ZERO; eligible_strategies.len()];
            
            for i in 0..eligible_strategies.len() {
                let (_, metrics_i) = &eligible_strategies[i];
                let mean_i = metrics_i.ewma_return;
                let vol_i = metrics_i.volatility;
                
                gradients[i] = mean_i - RISK_FREE_RATE;
                
                for j in 0..eligible_strategies.len() {
                    let (id_j, metrics_j) = &eligible_strategies[j];
                    let vol_j = metrics_j.volatility;
                    
                    let correlation = if i == j {
                        Decimal::ONE
                    } else {
                        let key = (eligible_strategies[i].0.clone(), id_j.clone());
                        correlations.correlations.get(&key).copied().unwrap_or(Decimal::ZERO)
                    };
                    
                    gradients[i] -= weights[j] * correlation * vol_i * vol_j;
                }
            }
            
            let learning_rate = dec!(0.01);
            let mut new_weights = weights.clone();
            
            for i in 0..weights.len() {
                new_weights[i] += learning_rate * gradients[i];
                new_weights[i] = new_weights[i].max(Decimal::ZERO).min(MAX_POSITION_FRACTION);
            }
            
            let total_weight: Decimal = new_weights.iter().sum();
            if total_weight > Decimal::ZERO {
                for w in &mut new_weights {
                    *w /= total_weight;
                }
            }
            
            let diff: Decimal = weights.iter().zip(&new_weights)
                .map(|(w1, w2)| (*w1 - *w2).abs())
                .sum();
            
            weights = new_weights;
            
            if diff < dec!(0.0001) {
                break;
            }
        }
        
        for (i, (strategy_id, _)) in eligible_strategies.iter().enumerate() {
            let allocation = total_capital * weights[i] * dec!(0.95);
            optimized_weights.insert(strategy_id.clone(), allocation);
        }
        
        optimized_weights
    }

    pub fn health_check(&self) -> SystemHealth {
        let metrics = self.strategy_metrics.read().unwrap();
        let allocations = self.allocated_capital.read().unwrap();
        let total_capital = *self.total_capital.read().unwrap();
        
        let active_strategies = metrics.iter()
            .filter(|(_, m)| m.returns.len() >= MIN_OBSERVATIONS)
            .count();
        
        let allocated_capital: Decimal = allocations.values().sum();
        let utilization = allocated_capital / total_capital;
        
        let avg_sharpe = if active_strategies > 0 {
            metrics.values()
                .filter(|m| m.returns.len() >= MIN_OBSERVATIONS)
                .map(|m| m.sharpe_ratio)
                .sum::<Decimal>() / Decimal::from(active_strategies)
        } else {
            Decimal::ZERO
        };
        
        let max_drawdown = metrics.values()
            .map(|m| m.current_drawdown)
            .max()
            .unwrap_or(Decimal::ZERO);
        
        let status = if max_drawdown > MAX_DRAWDOWN_TOLERANCE * dec!(1.5) {
            HealthStatus::Critical
        } else if avg_sharpe < MIN_SHARPE_THRESHOLD || utilization < dec!(0.2) {
            HealthStatus::Warning
        } else {
            HealthStatus::Healthy
        };
        
        SystemHealth {
            status,
            active_strategies,
            total_capital,
            allocated_capital,
            utilization,
            average_sharpe: avg_sharpe,
            max_drawdown,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Action {
    Increase,
    Hold,
    Reduce,
    Exit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyRecommendation {
    pub strategy_id: String,
    pub action: Action,
    pub current_allocation: Decimal,
    pub recommended_allocation: Decimal,
    pub confidence: Decimal,
    pub risk_score: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemHealth {
    pub status: HealthStatus,
    pub active_strategies: usize,
    pub total_capital: Decimal,
    pub allocated_capital: Decimal,
    pub utilization: Decimal,
    pub average_sharpe: Decimal,
    pub max_drawdown: Decimal,
    pub timestamp: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sharpe_ratio_maximizer_initialization() {
        let maximizer = SharpeRatioMaximizer::new(dec!(100000), dec!(0.02));
        assert_eq!(*maximizer.total_capital.read().unwrap(), dec!(100000));
        assert_eq!(*maximizer.risk_budget.read().unwrap(), dec!(2000));
    }

    #[test]
    fn test_position_sizing() {
        let maximizer = SharpeRatioMaximizer::new(dec!(100000), dec!(0.02));
        let size = maximizer.get_position_size("test_strategy", dec!(10000));
        assert!(size >= Decimal::ZERO);
    }

    #[test]
    fn test_trade_recording() {
        let maximizer = SharpeRatioMaximizer::new(dec!(100000), dec!(0.02));
        let result = TradeResult {
            strategy_id: "test_strategy".to_string(),
            timestamp: 1000000,
            pnl: dec!(100),
            capital_deployed: dec!(1000),
            gas_cost: dec!(10),
            slippage: dec!(5),
        };
        
        assert!(maximizer.record_trade(result).is_ok());
    }
}

