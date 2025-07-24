use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, RwLock as TokioRwLock};
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use solana_client::nonblocking::rpc_client::RpcClient;
use raydium_amm::state::AmmInfo;
use anchor_lang::prelude::*;

const MAX_STRATEGIES: usize = 32;
const MIN_STRATEGY_ALLOCATION: f64 = 0.01;
const MAX_STRATEGY_ALLOCATION: f64 = 0.35;
const REBALANCE_INTERVAL_MS: u64 = 30000;
const PERFORMANCE_WINDOW_SIZE: usize = 1000;
const MIN_SHARPE_RATIO: f64 = 0.5;
const MAX_DRAWDOWN_PERCENT: f64 = 0.15;
const KELLY_FRACTION: f64 = 0.25;
const CORRELATION_THRESHOLD: f64 = 0.7;
const MIN_STRATEGY_CAPITAL: u64 = 1_000_000;
const RISK_FREE_RATE: f64 = 0.02;
const CONFIDENCE_DECAY_FACTOR: f64 = 0.995;
const MIN_TRADES_FOR_STATS: usize = 50;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyPerformance {
    pub total_pnl: Decimal,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub win_rate: f64,
    pub avg_win: Decimal,
    pub avg_loss: Decimal,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub max_drawdown: f64,
    pub current_drawdown: f64,
    pub trades_count: u64,
    pub winning_trades: u64,
    pub losing_trades: u64,
    pub last_update: Instant,
    pub returns_history: VecDeque<f64>,
    pub cumulative_returns: Vec<f64>,
    pub peak_value: Decimal,
    pub trough_value: Decimal,
    pub confidence_score: f64,
    pub risk_adjusted_return: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyAllocation {
    pub strategy_id: String,
    pub allocated_capital: u64,
    pub target_allocation: f64,
    pub current_allocation: f64,
    pub min_allocation: f64,
    pub max_allocation: f64,
    pub priority_score: f64,
    pub risk_score: f64,
    pub last_rebalance: Instant,
    pub position_count: u32,
    pub active: bool,
}

#[derive(Debug, Clone)]
pub struct PortfolioState {
    pub total_capital: u64,
    pub available_capital: u64,
    pub deployed_capital: u64,
    pub total_pnl: Decimal,
    pub strategies: HashMap<String, StrategyAllocation>,
    pub performance_metrics: HashMap<String, StrategyPerformance>,
    pub correlation_matrix: HashMap<(String, String), f64>,
    pub last_rebalance: Instant,
    pub portfolio_sharpe: f64,
    pub portfolio_volatility: f64,
    pub target_volatility: f64,
}

#[derive(Debug, Clone)]
pub struct RiskParameters {
    pub max_portfolio_leverage: f64,
    pub max_correlation_exposure: f64,
    pub max_single_strategy_loss: f64,
    pub vol_target: f64,
    pub min_liquidity_ratio: f64,
    pub max_concentration: f64,
    pub stress_test_scenarios: Vec<StressScenario>,
}

#[derive(Debug, Clone)]
pub struct StressScenario {
    pub name: String,
    pub market_impact: f64,
    pub correlation_shock: f64,
    pub liquidity_multiplier: f64,
}

pub struct MultiStrategyPortfolioManager {
    state: Arc<RwLock<PortfolioState>>,
    risk_params: Arc<RwLock<RiskParameters>>,
    performance_tracker: Arc<Mutex<PerformanceTracker>>,
    rebalancer: Arc<TokioRwLock<PortfolioRebalancer>>,
    risk_engine: Arc<RiskEngine>,
    allocation_optimizer: Arc<AllocationOptimizer>,
    rpc_client: Arc<RpcClient>,
}

struct PerformanceTracker {
    trade_history: HashMap<String, VecDeque<TradeResult>>,
    pnl_snapshots: VecDeque<PnLSnapshot>,
    strategy_correlations: HashMap<(String, String), VecDeque<f64>>,
}

struct PortfolioRebalancer {
    target_allocations: HashMap<String, f64>,
    rebalance_queue: VecDeque<RebalanceAction>,
    last_optimization: Instant,
}

struct RiskEngine {
    var_calculator: ValueAtRiskCalculator,
    stress_tester: StressTester,
    exposure_monitor: ExposureMonitor,
}

struct AllocationOptimizer {
    optimization_method: OptimizationMethod,
    constraints: Vec<AllocationConstraint>,
}

#[derive(Debug, Clone)]
struct TradeResult {
    pub timestamp: Instant,
    pub pnl: Decimal,
    pub strategy_id: String,
    pub capital_used: u64,
    pub success: bool,
}

#[derive(Debug, Clone)]
struct PnLSnapshot {
    pub timestamp: Instant,
    pub total_pnl: Decimal,
    pub strategy_pnls: HashMap<String, Decimal>,
}

#[derive(Debug, Clone)]
struct RebalanceAction {
    pub strategy_id: String,
    pub current_allocation: f64,
    pub target_allocation: f64,
    pub capital_delta: i64,
}

#[derive(Debug, Clone)]
enum OptimizationMethod {
    MeanVariance,
    RiskParity,
    MaxSharpe,
    MinCorrelation,
}

#[derive(Debug, Clone)]
struct AllocationConstraint {
    pub constraint_type: ConstraintType,
    pub value: f64,
}

#[derive(Debug, Clone)]
enum ConstraintType {
    MinAllocation,
    MaxAllocation,
    MaxDrawdown,
    MinSharpe,
}

struct ValueAtRiskCalculator {
    confidence_level: f64,
    lookback_period: usize,
}

struct StressTester {
    scenarios: Vec<StressScenario>,
}

struct ExposureMonitor {
    exposure_limits: HashMap<String, f64>,
}

impl MultiStrategyPortfolioManager {
    pub async fn new(
        initial_capital: u64,
        rpc_client: Arc<RpcClient>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let state = Arc::new(RwLock::new(PortfolioState {
            total_capital: initial_capital,
            available_capital: initial_capital,
            deployed_capital: 0,
            total_pnl: Decimal::ZERO,
            strategies: HashMap::new(),
            performance_metrics: HashMap::new(),
            correlation_matrix: HashMap::new(),
            last_rebalance: Instant::now(),
            portfolio_sharpe: 0.0,
            portfolio_volatility: 0.0,
            target_volatility: 0.15,
        }));

        let risk_params = Arc::new(RwLock::new(RiskParameters {
            max_portfolio_leverage: 2.0,
            max_correlation_exposure: 0.6,
            max_single_strategy_loss: 0.1,
            vol_target: 0.15,
            min_liquidity_ratio: 0.3,
            max_concentration: 0.35,
            stress_test_scenarios: vec![
                StressScenario {
                    name: "Market Crash".to_string(),
                    market_impact: -0.25,
                    correlation_shock: 0.3,
                    liquidity_multiplier: 0.5,
                },
                StressScenario {
                    name: "Flash Crash".to_string(),
                    market_impact: -0.15,
                    correlation_shock: 0.5,
                    liquidity_multiplier: 0.3,
                },
            ],
        }));

        let performance_tracker = Arc::new(Mutex::new(PerformanceTracker {
            trade_history: HashMap::new(),
            pnl_snapshots: VecDeque::with_capacity(10000),
            strategy_correlations: HashMap::new(),
        }));

        let rebalancer = Arc::new(TokioRwLock::new(PortfolioRebalancer {
            target_allocations: HashMap::new(),
            rebalance_queue: VecDeque::new(),
            last_optimization: Instant::now(),
        }));

        let risk_engine = Arc::new(RiskEngine {
            var_calculator: ValueAtRiskCalculator {
                confidence_level: 0.95,
                lookback_period: 500,
            },
            stress_tester: StressTester {
                scenarios: risk_params.read().unwrap().stress_test_scenarios.clone(),
            },
            exposure_monitor: ExposureMonitor {
                exposure_limits: HashMap::new(),
            },
        });

        let allocation_optimizer = Arc::new(AllocationOptimizer {
            optimization_method: OptimizationMethod::MaxSharpe,
            constraints: vec![
                AllocationConstraint {
                    constraint_type: ConstraintType::MinAllocation,
                    value: MIN_STRATEGY_ALLOCATION,
                },
                AllocationConstraint {
                    constraint_type: ConstraintType::MaxAllocation,
                    value: MAX_STRATEGY_ALLOCATION,
                },
            ],
        });

        Ok(Self {
            state,
            risk_params,
            performance_tracker,
            rebalancer,
            risk_engine,
            allocation_optimizer,
            rpc_client,
        })
    }

    pub async fn register_strategy(
        &self,
        strategy_id: String,
        initial_allocation: f64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut state = self.state.write().unwrap();
        
        if state.strategies.len() >= MAX_STRATEGIES {
            return Err("Maximum strategies reached".into());
        }

        if initial_allocation < MIN_STRATEGY_ALLOCATION || initial_allocation > MAX_STRATEGY_ALLOCATION {
            return Err("Invalid allocation percentage".into());
        }

        let allocation = StrategyAllocation {
            strategy_id: strategy_id.clone(),
            allocated_capital: (state.total_capital as f64 * initial_allocation) as u64,
            target_allocation: initial_allocation,
            current_allocation: initial_allocation,
            min_allocation: MIN_STRATEGY_ALLOCATION,
            max_allocation: MAX_STRATEGY_ALLOCATION,
            priority_score: 1.0,
            risk_score: 0.5,
            last_rebalance: Instant::now(),
            position_count: 0,
            active: true,
        };

        state.strategies.insert(strategy_id.clone(), allocation);
        
        state.performance_metrics.insert(strategy_id.clone(), StrategyPerformance {
            total_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            win_rate: 0.0,
            avg_win: Decimal::ZERO,
            avg_loss: Decimal::ZERO,
            sharpe_ratio: 0.0,
            sortino_ratio: 0.0,
            max_drawdown: 0.0,
            current_drawdown: 0.0,
            trades_count: 0,
            winning_trades: 0,
            losing_trades: 0,
            last_update: Instant::now(),
            returns_history: VecDeque::with_capacity(PERFORMANCE_WINDOW_SIZE),
            cumulative_returns: Vec::new(),
            peak_value: Decimal::ZERO,
            trough_value: Decimal::ZERO,
            confidence_score: 0.5,
            risk_adjusted_return: 0.0,
        });

        Ok(())
    }

    pub async fn allocate_capital(
        &self,
        strategy_id: &str,
        requested_amount: u64,
    ) -> Result<u64, Box<dyn std::error::Error>> {
        let mut state = self.state.write().unwrap();
        
        let strategy = state.strategies.get_mut(strategy_id)
            .ok_or("Strategy not found")?;

        if !strategy.active {
            return Err("Strategy is inactive".into());
        }

        let max_allowed = (state.total_capital as f64 * strategy.max_allocation) as u64;
        let current_allocated = strategy.allocated_capital;
        
        let available_for_strategy = max_allowed.saturating_sub(current_allocated);
        let allocation = requested_amount.min(available_for_strategy).min(state.available_capital);

        if allocation < MIN_STRATEGY_CAPITAL {
            return Err("Insufficient capital available".into());
        }

        strategy.allocated_capital += allocation;
        state.available_capital -= allocation;
        state.deployed_capital += allocation;

        strategy.current_allocation = strategy.allocated_capital as f64 / state.total_capital as f64;

        Ok(allocation)
    }

    pub async fn release_capital(
        &self,
        strategy_id: &str,
        amount: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut state = self.state.write().unwrap();
        
        let strategy = state.strategies.get_mut(strategy_id)
            .ok_or("Strategy not found")?;

        let release_amount = amount.min(strategy.allocated_capital);
        
        strategy.allocated_capital -= release_amount;
        state.available_capital += release_amount;
        state.deployed_capital -= release_amount;

                strategy.current_allocation = strategy.allocated_capital as f64 / state.total_capital as f64;

        Ok(())
    }

    pub async fn update_performance(
        &self,
        strategy_id: &str,
        trade_result: TradeResult,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut state = self.state.write().unwrap();
        let mut tracker = self.performance_tracker.lock().await;

        let perf = state.performance_metrics.get_mut(strategy_id)
            .ok_or("Strategy not found")?;

        // Update PnL
        perf.total_pnl += trade_result.pnl;
        perf.realized_pnl += trade_result.pnl;
        
        // Update trade statistics
        perf.trades_count += 1;
        if trade_result.pnl > Decimal::ZERO {
            perf.winning_trades += 1;
            let win_sum = perf.avg_win * Decimal::from(perf.winning_trades - 1);
            perf.avg_win = (win_sum + trade_result.pnl) / Decimal::from(perf.winning_trades);
        } else if trade_result.pnl < Decimal::ZERO {
            perf.losing_trades += 1;
            let loss_sum = perf.avg_loss * Decimal::from(perf.losing_trades - 1);
            perf.avg_loss = (loss_sum + trade_result.pnl.abs()) / Decimal::from(perf.losing_trades);
        }

        perf.win_rate = if perf.trades_count > 0 {
            perf.winning_trades as f64 / perf.trades_count as f64
        } else {
            0.0
        };

        // Update returns history
        let return_pct = if trade_result.capital_used > 0 {
            trade_result.pnl.to_f64().unwrap_or(0.0) / trade_result.capital_used as f64
        } else {
            0.0
        };
        
        perf.returns_history.push_back(return_pct);
        if perf.returns_history.len() > PERFORMANCE_WINDOW_SIZE {
            perf.returns_history.pop_front();
        }

        // Update drawdown
        let current_value = perf.total_pnl;
        if current_value > perf.peak_value {
            perf.peak_value = current_value;
            perf.trough_value = current_value;
        } else if current_value < perf.trough_value {
            perf.trough_value = current_value;
        }

        if perf.peak_value > Decimal::ZERO {
            let drawdown = (perf.peak_value - current_value) / perf.peak_value;
            perf.current_drawdown = drawdown.to_f64().unwrap_or(0.0);
            perf.max_drawdown = perf.max_drawdown.max(perf.current_drawdown);
        }

        // Calculate Sharpe ratio
        if perf.returns_history.len() >= MIN_TRADES_FOR_STATS {
            let returns: Vec<f64> = perf.returns_history.iter().copied().collect();
            let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
            let variance = returns.iter()
                .map(|r| (r - mean_return).powi(2))
                .sum::<f64>() / returns.len() as f64;
            let std_dev = variance.sqrt();
            
            if std_dev > 0.0 {
                perf.sharpe_ratio = (mean_return - RISK_FREE_RATE / 365.0) / std_dev * (252.0_f64).sqrt();
                
                // Calculate Sortino ratio
                let downside_returns: Vec<f64> = returns.iter()
                    .filter(|&&r| r < 0.0)
                    .copied()
                    .collect();
                
                if !downside_returns.is_empty() {
                    let downside_variance = downside_returns.iter()
                        .map(|r| r.powi(2))
                        .sum::<f64>() / downside_returns.len() as f64;
                    let downside_dev = downside_variance.sqrt();
                    
                    if downside_dev > 0.0 {
                        perf.sortino_ratio = (mean_return - RISK_FREE_RATE / 365.0) / downside_dev * (252.0_f64).sqrt();
                    }
                }
            }
        }

        // Update confidence score
        let success_rate = if perf.trades_count > 0 {
            perf.winning_trades as f64 / perf.trades_count as f64
        } else {
            0.5
        };
        
        let sharpe_factor = (perf.sharpe_ratio / 3.0).min(1.0).max(0.0);
        let drawdown_factor = (1.0 - perf.current_drawdown / MAX_DRAWDOWN_PERCENT).max(0.0);
        
        perf.confidence_score = perf.confidence_score * CONFIDENCE_DECAY_FACTOR 
            + (1.0 - CONFIDENCE_DECAY_FACTOR) * (success_rate * 0.4 + sharpe_factor * 0.4 + drawdown_factor * 0.2);

        // Update risk-adjusted return
        if std::cmp::max(perf.winning_trades, perf.losing_trades) > 0 {
            let kelly_criterion = if perf.avg_loss > Decimal::ZERO {
                let p = perf.win_rate;
                let b = perf.avg_win / perf.avg_loss;
                (p * b.to_f64().unwrap_or(0.0) - (1.0 - p)) / b.to_f64().unwrap_or(1.0)
            } else {
                0.0
            };
            
            perf.risk_adjusted_return = kelly_criterion.max(0.0).min(1.0) * KELLY_FRACTION;
        }

        perf.last_update = Instant::now();

        // Update trade history
        tracker.trade_history.entry(strategy_id.to_string())
            .or_insert_with(|| VecDeque::with_capacity(PERFORMANCE_WINDOW_SIZE))
            .push_back(trade_result);

        // Update portfolio PnL
        state.total_pnl = state.performance_metrics.values()
            .map(|p| p.total_pnl)
            .sum();

        drop(tracker);
        drop(state);

        // Check if rebalancing needed
        if self.should_rebalance().await {
            self.trigger_rebalance().await?;
        }

        Ok(())
    }

    async fn should_rebalance(&self) -> bool {
        let state = self.state.read().unwrap();
        let elapsed = state.last_rebalance.elapsed().as_millis() as u64;
        
        if elapsed < REBALANCE_INTERVAL_MS {
            return false;
        }

        // Check allocation drift
        for (_, strategy) in &state.strategies {
            let drift = (strategy.current_allocation - strategy.target_allocation).abs();
            if drift > 0.05 {
                return true;
            }
        }

        // Check risk metrics
        let risk_params = self.risk_params.read().unwrap();
        if state.portfolio_volatility > risk_params.vol_target * 1.2 {
            return true;
        }

        false
    }

    async fn trigger_rebalance(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut rebalancer = self.rebalancer.write().await;
        let state = self.state.read().unwrap();
        
        // Calculate optimal allocations
        let optimal_allocations = self.calculate_optimal_allocations(&state).await?;
        
        // Generate rebalance actions
        let mut actions = VecDeque::new();
        for (strategy_id, target_alloc) in optimal_allocations {
            if let Some(strategy) = state.strategies.get(&strategy_id) {
                let capital_delta = (target_alloc * state.total_capital as f64) as i64 
                    - strategy.allocated_capital as i64;
                
                if capital_delta.abs() > MIN_STRATEGY_CAPITAL as i64 {
                    actions.push_back(RebalanceAction {
                        strategy_id: strategy_id.clone(),
                        current_allocation: strategy.current_allocation,
                        target_allocation: target_alloc,
                        capital_delta,
                    });
                }
            }
        }

        rebalancer.rebalance_queue = actions;
        rebalancer.target_allocations = optimal_allocations;
        rebalancer.last_optimization = Instant::now();

        drop(rebalancer);
        drop(state);

        self.execute_rebalance().await?;

        Ok(())
    }

    async fn calculate_optimal_allocations(
        &self,
        state: &PortfolioState,
    ) -> Result<HashMap<String, f64>, Box<dyn std::error::Error>> {
        let mut allocations = HashMap::new();
        let active_strategies: Vec<_> = state.strategies.iter()
            .filter(|(_, s)| s.active)
            .collect();

        if active_strategies.is_empty() {
            return Ok(allocations);
        }

        // Collect performance metrics
        let mut returns_matrix = Vec::new();
        let mut strategy_ids = Vec::new();
        
        for (id, _) in &active_strategies {
            if let Some(perf) = state.performance_metrics.get(*id) {
                if perf.returns_history.len() >= MIN_TRADES_FOR_STATS {
                    returns_matrix.push(perf.returns_history.iter().copied().collect::<Vec<_>>());
                    strategy_ids.push((*id).clone());
                }
            }
        }

        if strategy_ids.is_empty() {
            // Equal weight if no performance data
            let equal_weight = 1.0 / active_strategies.len() as f64;
            for (id, _) in active_strategies {
                allocations.insert(id.clone(), equal_weight);
            }
            return Ok(allocations);
        }

        // Calculate correlation matrix
        let correlation_matrix = self.calculate_correlation_matrix(&returns_matrix);

        // Apply optimization method
        match &self.allocation_optimizer.optimization_method {
            OptimizationMethod::MaxSharpe => {
                allocations = self.optimize_max_sharpe(
                    &strategy_ids,
                    &returns_matrix,
                    &correlation_matrix,
                    state,
                ).await?;
            },
            OptimizationMethod::RiskParity => {
                allocations = self.optimize_risk_parity(
                    &strategy_ids,
                    &returns_matrix,
                    &correlation_matrix,
                ).await?;
            },
            _ => {
                // Default to equal weight
                let weight = 1.0 / strategy_ids.len() as f64;
                for id in strategy_ids {
                    allocations.insert(id, weight);
                }
            }
        }

        // Apply constraints
        self.apply_allocation_constraints(&mut allocations, state).await?;

        Ok(allocations)
    }

    fn calculate_correlation_matrix(&self, returns_matrix: &[Vec<f64>]) -> Vec<Vec<f64>> {
        let n = returns_matrix.len();
        let mut correlation = vec![vec![0.0; n]; n];

        for i in 0..n {
            for j in 0..n {
                if i == j {
                    correlation[i][j] = 1.0;
                } else {
                    let corr = self.calculate_correlation(&returns_matrix[i], &returns_matrix[j]);
                    correlation[i][j] = corr;
                    correlation[j][i] = corr;
                }
            }
        }

        correlation
    }

    fn calculate_correlation(&self, returns1: &[f64], returns2: &[f64]) -> f64 {
        let n = returns1.len().min(returns2.len());
        if n < 2 {
            return 0.0;
        }

        let mean1: f64 = returns1.iter().take(n).sum::<f64>() / n as f64;
        let mean2: f64 = returns2.iter().take(n).sum::<f64>() / n as f64;

        let mut cov = 0.0;
        let mut var1 = 0.0;
        let mut var2 = 0.0;

        for i in 0..n {
            let diff1 = returns1[i] - mean1;
            let diff2 = returns2[i] - mean2;
            cov += diff1 * diff2;
            var1 += diff1 * diff1;
            var2 += diff2 * diff2;
        }

        if var1 > 0.0 && var2 > 0.0 {
            cov / (var1.sqrt() * var2.sqrt())
        } else {
            0.0
        }
    }

    async fn optimize_max_sharpe(
        &self,
        strategy_ids: &[String],
        returns_matrix: &[Vec<f64>],
        correlation_matrix: &[Vec<f64>],
        state: &PortfolioState,
    ) -> Result<HashMap<String, f64>, Box<dyn std::error::Error>> {
        let mut allocations = HashMap::new();
        let n = strategy_ids.len();

        // Calculate expected returns and standard deviations
        let mut expected_returns = vec![0.0; n];
        let mut std_devs = vec![0.0; n];

        for i in 0..n {
            let returns = &returns_matrix[i];
            expected_returns[i] = returns.iter().sum::<f64>() / returns.len() as f64;
            let variance = returns.iter()
                .map(|r| (r - expected_returns[i]).powi(2))
                .sum::<f64>() / returns.len() as f64;
            std_devs[i] = variance.sqrt();
        }

        // Use simplified mean-variance optimization
        let mut weights = vec![1.0 / n as f64; n];
        let mut best_sharpe = -1.0;
        let mut best_weights = weights.clone();

        // Gradient ascent for Sharpe ratio
        let learning_rate = 0.01;
        let iterations = 100;

        for _ in 0..iterations {
            let portfolio_return: f64 = weights.iter()
                .zip(expected_returns.iter())
                .map(|(w, r)| w * r)
                .sum();

                        let mut portfolio_variance = 0.0;
            for i in 0..n {
                for j in 0..n {
                    portfolio_variance += weights[i] * weights[j] * std_devs[i] * std_devs[j] * correlation_matrix[i][j];
                }
            }

            let portfolio_std = portfolio_variance.sqrt();
            let sharpe = if portfolio_std > 0.0 {
                (portfolio_return - RISK_FREE_RATE / 365.0) / portfolio_std * (252.0_f64).sqrt()
            } else {
                0.0
            };

            if sharpe > best_sharpe {
                best_sharpe = sharpe;
                best_weights = weights.clone();
            }

            // Calculate gradients
            let mut gradients = vec![0.0; n];
            for i in 0..n {
                let mut grad_return = expected_returns[i];
                let mut grad_risk = 0.0;
                
                for j in 0..n {
                    grad_risk += 2.0 * weights[j] * std_devs[i] * std_devs[j] * correlation_matrix[i][j];
                }
                
                if portfolio_std > 0.0 {
                    gradients[i] = (grad_return * portfolio_std - portfolio_return * grad_risk / (2.0 * portfolio_std)) 
                        / (portfolio_std * portfolio_std) * (252.0_f64).sqrt();
                }
            }

            // Update weights
            for i in 0..n {
                weights[i] += learning_rate * gradients[i];
                weights[i] = weights[i].max(MIN_STRATEGY_ALLOCATION).min(MAX_STRATEGY_ALLOCATION);
            }

            // Normalize weights
            let sum: f64 = weights.iter().sum();
            if sum > 0.0 {
                for w in &mut weights {
                    *w /= sum;
                }
            }
        }

        // Apply performance-based adjustments
        for i in 0..n {
            if let Some(perf) = state.performance_metrics.get(&strategy_ids[i]) {
                let confidence_multiplier = 0.5 + perf.confidence_score * 0.5;
                best_weights[i] *= confidence_multiplier;
            }
        }

        // Normalize final weights
        let sum: f64 = best_weights.iter().sum();
        if sum > 0.0 {
            for w in &mut best_weights {
                *w /= sum;
            }
        }

        for (i, id) in strategy_ids.iter().enumerate() {
            allocations.insert(id.clone(), best_weights[i]);
        }

        Ok(allocations)
    }

    async fn optimize_risk_parity(
        &self,
        strategy_ids: &[String],
        returns_matrix: &[Vec<f64>],
        correlation_matrix: &[Vec<f64>],
    ) -> Result<HashMap<String, f64>, Box<dyn std::error::Error>> {
        let mut allocations = HashMap::new();
        let n = strategy_ids.len();

        // Calculate volatilities
        let mut volatilities = vec![0.0; n];
        for i in 0..n {
            let variance = returns_matrix[i].iter()
                .map(|r| r * r)
                .sum::<f64>() / returns_matrix[i].len() as f64;
            volatilities[i] = variance.sqrt();
        }

        // Risk parity: weight inversely proportional to volatility
        let mut weights = vec![0.0; n];
        let inv_vol_sum: f64 = volatilities.iter()
            .map(|&v| if v > 0.0 { 1.0 / v } else { 0.0 })
            .sum();

        if inv_vol_sum > 0.0 {
            for i in 0..n {
                weights[i] = if volatilities[i] > 0.0 {
                    (1.0 / volatilities[i]) / inv_vol_sum
                } else {
                    0.0
                };
            }
        }

        for (i, id) in strategy_ids.iter().enumerate() {
            allocations.insert(id.clone(), weights[i]);
        }

        Ok(allocations)
    }

    async fn apply_allocation_constraints(
        &self,
        allocations: &mut HashMap<String, f64>,
        state: &PortfolioState,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Apply min/max constraints
        for (id, allocation) in allocations.iter_mut() {
            if let Some(strategy) = state.strategies.get(id) {
                *allocation = allocation.max(strategy.min_allocation).min(strategy.max_allocation);
            }
        }

        // Check concentration risk
        let risk_params = self.risk_params.read().unwrap();
        let max_single = allocations.values().cloned().fold(0.0, f64::max);
        if max_single > risk_params.max_concentration {
            let scale = risk_params.max_concentration / max_single;
            for allocation in allocations.values_mut() {
                *allocation *= scale;
            }
        }

        // Check correlation exposure
        let mut high_corr_exposure = 0.0;
        let strategy_ids: Vec<_> = allocations.keys().cloned().collect();
        
        for i in 0..strategy_ids.len() {
            for j in i+1..strategy_ids.len() {
                let key = (strategy_ids[i].clone(), strategy_ids[j].clone());
                if let Some(&corr) = state.correlation_matrix.get(&key) {
                    if corr > CORRELATION_THRESHOLD {
                        high_corr_exposure += allocations[&strategy_ids[i]] * allocations[&strategy_ids[j]] * corr;
                    }
                }
            }
        }

        if high_corr_exposure > risk_params.max_correlation_exposure {
            // Reduce allocations to highly correlated strategies
            for (id, allocation) in allocations.iter_mut() {
                let mut corr_penalty = 1.0;
                for (other_id, _) in allocations.iter() {
                    if id != other_id {
                        let key = if id < other_id {
                            (id.clone(), other_id.clone())
                        } else {
                            (other_id.clone(), id.clone())
                        };
                        
                        if let Some(&corr) = state.correlation_matrix.get(&key) {
                            if corr > CORRELATION_THRESHOLD {
                                corr_penalty *= 1.0 - (corr - CORRELATION_THRESHOLD) * 0.5;
                            }
                        }
                    }
                }
                *allocation *= corr_penalty;
            }
        }

        // Normalize allocations
        let sum: f64 = allocations.values().sum();
        if sum > 0.0 && (sum - 1.0).abs() > 1e-6 {
            for allocation in allocations.values_mut() {
                *allocation /= sum;
            }
        }

        Ok(())
    }

    async fn execute_rebalance(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut rebalancer = self.rebalancer.write().await;
        let mut state = self.state.write().unwrap();

        while let Some(action) = rebalancer.rebalance_queue.pop_front() {
            if let Some(strategy) = state.strategies.get_mut(&action.strategy_id) {
                if action.capital_delta > 0 {
                    // Need to allocate more capital
                    let allocation = action.capital_delta as u64;
                    if state.available_capital >= allocation {
                        strategy.allocated_capital += allocation;
                        state.available_capital -= allocation;
                        state.deployed_capital += allocation;
                    }
                } else {
                    // Need to release capital
                    let release = (-action.capital_delta) as u64;
                    let actual_release = release.min(strategy.allocated_capital);
                    strategy.allocated_capital -= actual_release;
                    state.available_capital += actual_release;
                    state.deployed_capital -= actual_release;
                }

                strategy.current_allocation = strategy.allocated_capital as f64 / state.total_capital as f64;
                strategy.target_allocation = action.target_allocation;
                strategy.last_rebalance = Instant::now();
            }
        }

        state.last_rebalance = Instant::now();
        
        // Update portfolio metrics
        self.update_portfolio_metrics(&mut state).await?;

        Ok(())
    }

    async fn update_portfolio_metrics(
        &self,
        state: &mut PortfolioState,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Calculate portfolio returns
        let mut portfolio_returns = Vec::new();
        let mut weights = Vec::new();

        for (id, strategy) in &state.strategies {
            if let Some(perf) = state.performance_metrics.get(id) {
                if !perf.returns_history.is_empty() {
                    weights.push(strategy.current_allocation);
                    portfolio_returns.push(perf.returns_history.clone());
                }
            }
        }

        if !weights.is_empty() {
            // Calculate portfolio volatility
            let min_len = portfolio_returns.iter()
                .map(|r| r.len())
                .min()
                .unwrap_or(0);

            if min_len > 0 {
                let mut portfolio_variance = 0.0;
                
                for t in 0..min_len {
                    let mut period_return = 0.0;
                    for (i, returns) in portfolio_returns.iter().enumerate() {
                        period_return += weights[i] * returns[t];
                    }
                    portfolio_variance += period_return * period_return;
                }

                state.portfolio_volatility = (portfolio_variance / min_len as f64).sqrt() * (252.0_f64).sqrt();

                // Calculate portfolio Sharpe ratio
                let mean_return: f64 = (0..min_len)
                    .map(|t| {
                        portfolio_returns.iter()
                            .enumerate()
                            .map(|(i, r)| weights[i] * r[t])
                            .sum::<f64>()
                    })
                    .sum::<f64>() / min_len as f64;

                if state.portfolio_volatility > 0.0 {
                    state.portfolio_sharpe = (mean_return - RISK_FREE_RATE / 365.0) 
                        / (state.portfolio_volatility / (252.0_f64).sqrt());
                }
            }
        }

        // Update correlation matrix
        let strategy_ids: Vec<_> = state.strategies.keys().cloned().collect();
        for i in 0..strategy_ids.len() {
            for j in i+1..strategy_ids.len() {
                if let (Some(perf1), Some(perf2)) = (
                    state.performance_metrics.get(&strategy_ids[i]),
                    state.performance_metrics.get(&strategy_ids[j])
                ) {
                    if perf1.returns_history.len() >= MIN_TRADES_FOR_STATS 
                        && perf2.returns_history.len() >= MIN_TRADES_FOR_STATS {
                        let corr = self.calculate_correlation(
                            &perf1.returns_history.iter().cloned().collect::<Vec<_>>(),
                            &perf2.returns_history.iter().cloned().collect::<Vec<_>>()
                        );
                        let key = (strategy_ids[i].clone(), strategy_ids[j].clone());
                        state.correlation_matrix.insert(key, corr);
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn get_strategy_allocation(&self, strategy_id: &str) -> Option<u64> {
        let state = self.state.read().unwrap();
        state.strategies.get(strategy_id).map(|s| s.allocated_capital)
    }

    pub async fn get_portfolio_metrics(&self) -> PortfolioMetrics {
        let state = self.state.read().unwrap();
        PortfolioMetrics {
            total_capital: state.total_capital,
            available_capital: state.available_capital,
            deployed_capital: state.deployed_capital,
            total_pnl: state.total_pnl,
            portfolio_sharpe: state.portfolio_sharpe,
            portfolio_volatility: state.portfolio_volatility,
            strategy_count: state.strategies.len(),
            active_strategies: state.strategies.values().filter(|s| s.active).count(),
        }
    }

    pub async fn emergency_liquidation(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut state = self.state.write().unwrap();
        
        // Release all allocated capital
        for (_, strategy) in state.strategies.iter_mut() {
            state.available_capital += strategy.allocated_capital;
            state.deployed_capital -= strategy.allocated_capital;
            strategy.allocated_capital = 0;
            strategy.current_allocation = 0.0;
            strategy.active = false;
        }

        Ok(())
    }

    pub async fn update_risk_parameters(
        &self,
        new_params: RiskParameters,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut params = self.risk_params.write().unwrap();
        *params = new_params;
        drop(params);
        
        // Trigger rebalance with new risk parameters
        self.trigger_rebalance().await?;
        
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct PortfolioMetrics {
    pub total_capital: u64,
    pub available_capital: u64,
    pub deployed_capital: u64,
    pub total_pnl: Decimal,
    pub portfolio_sharpe: f64,
    pub portfolio_volatility: f64,
    pub strategy_count: usize,
    pub active_strategies: usize,
}

impl Default for StrategyPerformance {
    fn default() -> Self {
        Self {
            total_pnl: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            win_rate: 0.0,
            avg_win: Decimal::ZERO,
            avg_loss: Decimal::ZERO,
            sharpe_ratio: 0.0,
            sortino_ratio: 0.0,
            max_drawdown: 0.0,
            current_drawdown: 0.0,
            trades_count: 0,
            winning_trades: 0,
            losing_trades: 0,
            last_update: Instant::now(),
            returns_history: VecDeque::with_capacity(PERFORMANCE_WINDOW_SIZE),
                        cumulative_returns: Vec::new(),
            peak_value: Decimal::ZERO,
            trough_value: Decimal::ZERO,
            confidence_score: 0.5,
            risk_adjusted_return: 0.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_client::rpc_client::RpcClient;

    #[tokio::test]
    async fn test_portfolio_manager_initialization() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let manager = MultiStrategyPortfolioManager::new(100_000_000_000, rpc_client).await.unwrap();
        
        let metrics = manager.get_portfolio_metrics().await;
        assert_eq!(metrics.total_capital, 100_000_000_000);
        assert_eq!(metrics.available_capital, 100_000_000_000);
        assert_eq!(metrics.deployed_capital, 0);
    }

    #[tokio::test]
    async fn test_strategy_registration() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let manager = MultiStrategyPortfolioManager::new(100_000_000_000, rpc_client).await.unwrap();
        
        manager.register_strategy("arb_strategy_1".to_string(), 0.25).await.unwrap();
        manager.register_strategy("sandwich_strategy_1".to_string(), 0.20).await.unwrap();
        
        let metrics = manager.get_portfolio_metrics().await;
        assert_eq!(metrics.strategy_count, 2);
        assert_eq!(metrics.active_strategies, 2);
    }

    #[tokio::test]
    async fn test_capital_allocation() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let manager = MultiStrategyPortfolioManager::new(100_000_000_000, rpc_client).await.unwrap();
        
        manager.register_strategy("test_strategy".to_string(), 0.30).await.unwrap();
        
        let allocated = manager.allocate_capital("test_strategy", 10_000_000_000).await.unwrap();
        assert!(allocated > 0);
        assert!(allocated <= 30_000_000_000); // 30% of total capital
        
        let allocation = manager.get_strategy_allocation("test_strategy").await.unwrap();
        assert_eq!(allocation, allocated);
    }

    #[tokio::test]
    async fn test_performance_update() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let manager = MultiStrategyPortfolioManager::new(100_000_000_000, rpc_client).await.unwrap();
        
        manager.register_strategy("test_strategy".to_string(), 0.25).await.unwrap();
        
        let trade_result = TradeResult {
            timestamp: Instant::now(),
            pnl: Decimal::from_str("1000.50").unwrap(),
            strategy_id: "test_strategy".to_string(),
            capital_used: 10_000_000,
            success: true,
        };
        
        manager.update_performance("test_strategy", trade_result).await.unwrap();
        
        let state = manager.state.read().unwrap();
        let perf = state.performance_metrics.get("test_strategy").unwrap();
        assert_eq!(perf.trades_count, 1);
        assert_eq!(perf.winning_trades, 1);
        assert_eq!(perf.total_pnl, Decimal::from_str("1000.50").unwrap());
    }

    #[tokio::test]
    async fn test_risk_parameters_update() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let manager = MultiStrategyPortfolioManager::new(100_000_000_000, rpc_client).await.unwrap();
        
        let new_params = RiskParameters {
            max_portfolio_leverage: 1.5,
            max_correlation_exposure: 0.5,
            max_single_strategy_loss: 0.08,
            vol_target: 0.12,
            min_liquidity_ratio: 0.4,
            max_concentration: 0.30,
            stress_test_scenarios: vec![
                StressScenario {
                    name: "Test Scenario".to_string(),
                    market_impact: -0.20,
                    correlation_shock: 0.4,
                    liquidity_multiplier: 0.6,
                },
            ],
        };
        
        manager.update_risk_parameters(new_params).await.unwrap();
        
        let params = manager.risk_params.read().unwrap();
        assert_eq!(params.max_portfolio_leverage, 1.5);
        assert_eq!(params.vol_target, 0.12);
    }

    #[tokio::test]
    async fn test_emergency_liquidation() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let manager = MultiStrategyPortfolioManager::new(100_000_000_000, rpc_client).await.unwrap();
        
        manager.register_strategy("strategy1".to_string(), 0.30).await.unwrap();
        manager.register_strategy("strategy2".to_string(), 0.25).await.unwrap();
        
        manager.allocate_capital("strategy1", 20_000_000_000).await.unwrap();
        manager.allocate_capital("strategy2", 15_000_000_000).await.unwrap();
        
        manager.emergency_liquidation().await.unwrap();
        
        let metrics = manager.get_portfolio_metrics().await;
        assert_eq!(metrics.deployed_capital, 0);
        assert_eq!(metrics.available_capital, metrics.total_capital);
        assert_eq!(metrics.active_strategies, 0);
    }

    #[tokio::test]
    async fn test_correlation_calculation() {
        let manager = MultiStrategyPortfolioManager::new(
            100_000_000_000,
            Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()))
        ).await.unwrap();
        
        let returns1 = vec![0.01, -0.02, 0.03, -0.01, 0.02];
        let returns2 = vec![0.02, -0.01, 0.025, -0.015, 0.03];
        
        let correlation = manager.calculate_correlation(&returns1, &returns2);
        assert!(correlation > 0.8); // Should be highly correlated
        assert!(correlation <= 1.0);
    }

    #[tokio::test]
    async fn test_portfolio_rebalancing() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let manager = MultiStrategyPortfolioManager::new(100_000_000_000, rpc_client).await.unwrap();
        
        // Register multiple strategies
        manager.register_strategy("arb_1".to_string(), 0.25).await.unwrap();
        manager.register_strategy("sandwich_1".to_string(), 0.25).await.unwrap();
        manager.register_strategy("liquidation_1".to_string(), 0.25).await.unwrap();
        manager.register_strategy("jit_1".to_string(), 0.25).await.unwrap();
        
        // Simulate performance for each strategy
        for i in 0..100 {
            let strategies = vec!["arb_1", "sandwich_1", "liquidation_1", "jit_1"];
            for (idx, strategy) in strategies.iter().enumerate() {
                let pnl = if (i + idx) % 3 == 0 {
                    Decimal::from_str(&format!("{}", 100.0 + idx as f64 * 50.0)).unwrap()
                } else if (i + idx) % 5 == 0 {
                    Decimal::from_str(&format!("-{}", 50.0 + idx as f64 * 20.0)).unwrap()
                } else {
                    Decimal::from_str(&format!("{}", 20.0 + idx as f64 * 10.0)).unwrap()
                };
                
                let trade_result = TradeResult {
                    timestamp: Instant::now(),
                    pnl,
                    strategy_id: strategy.to_string(),
                    capital_used: 1_000_000,
                    success: pnl > Decimal::ZERO,
                };
                
                manager.update_performance(strategy, trade_result).await.unwrap();
            }
        }
        
        // Force rebalance
        manager.trigger_rebalance().await.unwrap();
        
        let state = manager.state.read().unwrap();
        
        // Verify allocations have been adjusted based on performance
        let mut total_allocation = 0.0;
        for (_, strategy) in &state.strategies {
            total_allocation += strategy.current_allocation;
            assert!(strategy.current_allocation >= MIN_STRATEGY_ALLOCATION);
            assert!(strategy.current_allocation <= MAX_STRATEGY_ALLOCATION);
        }
        assert!((total_allocation - 1.0).abs() < 0.01); // Should sum to approximately 1.0
    }

    #[tokio::test]
    async fn test_max_strategies_limit() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let manager = MultiStrategyPortfolioManager::new(100_000_000_000, rpc_client).await.unwrap();
        
        // Register maximum number of strategies
        for i in 0..MAX_STRATEGIES {
            let result = manager.register_strategy(
                format!("strategy_{}", i),
                1.0 / MAX_STRATEGIES as f64
            ).await;
            assert!(result.is_ok());
        }
        
        // Try to register one more
        let result = manager.register_strategy(
            "overflow_strategy".to_string(),
            0.01
        ).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_capital_release() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let manager = MultiStrategyPortfolioManager::new(100_000_000_000, rpc_client).await.unwrap();
        
        manager.register_strategy("test_strategy".to_string(), 0.30).await.unwrap();
        
        let initial_available = manager.get_portfolio_metrics().await.available_capital;
        
        manager.allocate_capital("test_strategy", 10_000_000_000).await.unwrap();
        
        let after_allocation = manager.get_portfolio_metrics().await.available_capital;
        assert_eq!(after_allocation, initial_available - 10_000_000_000);
        
        manager.release_capital("test_strategy", 5_000_000_000).await.unwrap();
        
        let after_release = manager.get_portfolio_metrics().await.available_capital;
        assert_eq!(after_release, after_allocation + 5_000_000_000);
    }
}

