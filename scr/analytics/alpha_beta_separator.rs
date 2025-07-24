use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    clock::Clock,
    instruction::Instruction,
};
use solana_client::rpc_client::RpcClient;
use std::{
    sync::{Arc, RwLock},
    collections::{HashMap, VecDeque},
    time::{Duration, Instant},
};
use serde::{Deserialize, Serialize};
use rayon::prelude::*;
use statistical::{mean, standard_deviation};

const ALPHA_THRESHOLD: f64 = 0.015;
const BETA_CORRELATION_THRESHOLD: f64 = 0.7;
const RISK_FREE_RATE: f64 = 0.0001;
const MARKET_IMPACT_COEFFICIENT: f64 = 0.0003;
const CONFIDENCE_INTERVAL: f64 = 0.95;
const MAX_POSITION_SIZE: u64 = 1_000_000_000_000;
const ROLLING_WINDOW_SIZE: usize = 1000;
const DECAY_FACTOR: f64 = 0.995;
const MIN_LIQUIDITY_THRESHOLD: u64 = 100_000_000;
const MAX_SLIPPAGE_BPS: u16 = 25;
const VOLATILITY_LOOKBACK: usize = 240;
const CORRELATION_WINDOW: usize = 120;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlphaBetaMetrics {
    pub alpha: f64,
    pub beta: f64,
    pub sharpe_ratio: f64,
    pub sortino_ratio: f64,
    pub max_drawdown: f64,
    pub volatility: f64,
    pub skewness: f64,
    pub kurtosis: f64,
    pub var_95: f64,
    pub cvar_95: f64,
    pub correlation_to_market: f64,
    pub information_ratio: f64,
    pub timestamp: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingOpportunity {
    pub id: [u8; 32],
    pub entry_price: f64,
    pub exit_price: f64,
    pub size: u64,
    pub market: Pubkey,
    pub token_a: Pubkey,
    pub token_b: Pubkey,
    pub pool_liquidity: u64,
    pub expected_profit: f64,
    pub confidence: f64,
    pub market_impact: f64,
    pub execution_risk: f64,
    pub timestamp: i64,
    pub priority_fee: u64,
    pub compute_units: u32,
}

#[derive(Debug, Clone)]
pub struct MarketBenchmark {
    pub returns: VecDeque<f64>,
    pub volatility: f64,
    pub cumulative_return: f64,
    pub last_update: Instant,
}

#[derive(Debug, Clone)]
pub struct StrategyClassification {
    pub strategy_type: StrategyType,
    pub alpha_component: f64,
    pub beta_component: f64,
    pub risk_adjusted_score: f64,
    pub execution_priority: u8,
    pub position_sizing: f64,
    pub stop_loss: f64,
    pub take_profit: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StrategyType {
    PureAlpha,
    AlphaDominant,
    MarketNeutral,
    BetaDominant,
    PureBeta,
    Arbitrage,
    MomentumAlpha,
    MeanReversionAlpha,
}

pub struct AlphaBetaSeparator {
    market_benchmark: Arc<RwLock<MarketBenchmark>>,
    strategy_returns: Arc<RwLock<HashMap<[u8; 32], VecDeque<f64>>>>,
    performance_cache: Arc<RwLock<HashMap<[u8; 32], AlphaBetaMetrics>>>,
    risk_parameters: Arc<RwLock<RiskParameters>>,
    execution_stats: Arc<RwLock<ExecutionStatistics>>,
}

#[derive(Debug, Clone)]
struct RiskParameters {
    max_leverage: f64,
    max_concentration: f64,
    max_correlated_exposure: f64,
    dynamic_risk_limit: f64,
    stress_test_scenarios: Vec<StressScenario>,
}

#[derive(Debug, Clone)]
struct StressScenario {
    market_shock: f64,
    liquidity_reduction: f64,
    correlation_increase: f64,
    probability: f64,
}

#[derive(Debug, Clone)]
struct ExecutionStatistics {
    success_rate: f64,
    average_slippage: f64,
    average_latency: Duration,
    failed_opportunities: u64,
    total_opportunities: u64,
}

impl AlphaBetaSeparator {
    pub fn new() -> Self {
        let market_benchmark = MarketBenchmark {
            returns: VecDeque::with_capacity(ROLLING_WINDOW_SIZE),
            volatility: 0.02,
            cumulative_return: 1.0,
            last_update: Instant::now(),
        };

        let risk_parameters = RiskParameters {
            max_leverage: 3.0,
            max_concentration: 0.25,
            max_correlated_exposure: 0.4,
            dynamic_risk_limit: 0.1,
            stress_test_scenarios: vec![
                StressScenario {
                    market_shock: -0.1,
                    liquidity_reduction: 0.5,
                    correlation_increase: 0.3,
                    probability: 0.01,
                },
                StressScenario {
                    market_shock: -0.05,
                    liquidity_reduction: 0.3,
                    correlation_increase: 0.2,
                    probability: 0.05,
                },
                StressScenario {
                    market_shock: -0.02,
                    liquidity_reduction: 0.1,
                    correlation_increase: 0.1,
                    probability: 0.15,
                },
            ],
        };

        Self {
            market_benchmark: Arc::new(RwLock::new(market_benchmark)),
            strategy_returns: Arc::new(RwLock::new(HashMap::new())),
            performance_cache: Arc::new(RwLock::new(HashMap::new())),
            risk_parameters: Arc::new(RwLock::new(risk_parameters)),
            execution_stats: Arc::new(RwLock::new(ExecutionStatistics {
                success_rate: 1.0,
                average_slippage: 0.0,
                average_latency: Duration::from_millis(50),
                failed_opportunities: 0,
                total_opportunities: 0,
            })),
        }
    }

    pub fn classify_opportunity(&self, opportunity: &TradingOpportunity) -> Result<StrategyClassification, Box<dyn std::error::Error>> {
        let market_data = self.market_benchmark.read().unwrap();
        let mut strategy_returns = self.strategy_returns.write().unwrap();
        
        let expected_return = (opportunity.exit_price - opportunity.entry_price) / opportunity.entry_price;
        let risk_adjusted_return = expected_return - (opportunity.execution_risk * opportunity.market_impact);
        
        let returns_window = strategy_returns
            .entry(opportunity.id)
            .or_insert_with(|| VecDeque::with_capacity(ROLLING_WINDOW_SIZE));
        
        returns_window.push_back(risk_adjusted_return);
        if returns_window.len() > ROLLING_WINDOW_SIZE {
            returns_window.pop_front();
        }
        
        let alpha_beta = self.calculate_alpha_beta(
            &returns_window.iter().copied().collect::<Vec<_>>(),
            &market_data.returns.iter().copied().collect::<Vec<_>>(),
        )?;
        
        let risk_metrics = self.calculate_risk_metrics(&returns_window.iter().copied().collect::<Vec<_>>())?;
        
        let strategy_type = self.determine_strategy_type(
            alpha_beta.alpha,
            alpha_beta.beta,
            alpha_beta.correlation_to_market,
            risk_metrics.sharpe_ratio,
        );
        
        let risk_score = self.calculate_risk_adjusted_score(
            &alpha_beta,
            &risk_metrics,
            opportunity,
        )?;
        
        let position_sizing = self.calculate_optimal_position_size(
            opportunity,
            &alpha_beta,
            &risk_metrics,
        )?;
        
        let (stop_loss, take_profit) = self.calculate_risk_limits(
            opportunity.entry_price,
            &risk_metrics,
            &strategy_type,
        );
        
        let execution_priority = self.calculate_execution_priority(
            &strategy_type,
            risk_score,
            opportunity.confidence,
        );
        
        self.update_performance_cache(opportunity.id, alpha_beta)?;
        
        Ok(StrategyClassification {
            strategy_type,
            alpha_component: alpha_beta.alpha,
            beta_component: alpha_beta.beta,
            risk_adjusted_score: risk_score,
            execution_priority,
            position_sizing,
            stop_loss,
            take_profit,
        })
    }

    fn calculate_alpha_beta(
        &self,
        strategy_returns: &[f64],
        market_returns: &[f64],
    ) -> Result<AlphaBetaMetrics, Box<dyn std::error::Error>> {
        if strategy_returns.len() < 20 || market_returns.len() < 20 {
            return Ok(AlphaBetaMetrics {
                alpha: 0.0,
                beta: 1.0,
                sharpe_ratio: 0.0,
                sortino_ratio: 0.0,
                max_drawdown: 0.0,
                volatility: 0.02,
                skewness: 0.0,
                kurtosis: 3.0,
                var_95: 0.0,
                cvar_95: 0.0,
                correlation_to_market: 0.0,
                information_ratio: 0.0,
                timestamp: Clock::get()?.unix_timestamp,
            });
        }

        let n = strategy_returns.len().min(market_returns.len());
        let strategy_slice = &strategy_returns[strategy_returns.len().saturating_sub(n)..];
        let market_slice = &market_returns[market_returns.len().saturating_sub(n)..];
        
        let strategy_mean = mean(strategy_slice);
        let market_mean = mean(market_slice);
        
        let mut covariance = 0.0;
        let mut market_variance = 0.0;
        
        for i in 0..n {
            let strategy_dev = strategy_slice[i] - strategy_mean;
            let market_dev = market_slice[i] - market_mean;
            covariance += strategy_dev * market_dev;
            market_variance += market_dev * market_dev;
        }
        
        covariance /= n as f64;
        market_variance /= n as f64;
        
        let beta = if market_variance > 1e-10 {
            covariance / market_variance
        } else {
            1.0
        };
        
        let alpha = strategy_mean - (beta * market_mean) - RISK_FREE_RATE;
        
        let volatility = standard_deviation(strategy_slice);
        let sharpe_ratio = if volatility > 1e-10 {
            (strategy_mean - RISK_FREE_RATE) / volatility
        } else {
            0.0
        };
        
        let downside_returns: Vec<f64> = strategy_slice
            .iter()
            .filter(|&&r| r < RISK_FREE_RATE)
            .copied()
            .collect();
        
        let downside_volatility = if !downside_returns.is_empty() {
            standard_deviation(&downside_returns)
        } else {
            volatility
        };
        
        let sortino_ratio = if downside_volatility > 1e-10 {
            (strategy_mean - RISK_FREE_RATE) / downside_volatility
        } else {
            sharpe_ratio
        };
        
        let correlation = if volatility > 1e-10 && standard_deviation(market_slice) > 1e-10 {
            covariance / (volatility * standard_deviation(market_slice))
        } else {
            0.0
        };
        
        let max_drawdown = self.calculate_max_drawdown(strategy_slice);
        let (skewness, kurtosis) = self.calculate_higher_moments(strategy_slice);
        let (var_95, cvar_95) = self.calculate_var_cvar(strategy_slice, CONFIDENCE_INTERVAL);
        
        let tracking_error = standard_deviation(
            &strategy_slice.iter()
                .zip(market_slice.iter())
                .map(|(s, m)| s - m)
                .collect::<Vec<_>>()
        );
        
        let information_ratio = if tracking_error > 1e-10 {
            alpha / tracking_error
        } else {
            0.0
        };
        
        Ok(AlphaBetaMetrics {
            alpha,
            beta,
            sharpe_ratio,
            sortino_ratio,
            max_drawdown,
            volatility,
            skewness,
            kurtosis,
            var_95,
            cvar_95,
            correlation_to_market: correlation,
            information_ratio,
            timestamp: Clock::get()?.unix_timestamp,
        })
    }

    fn calculate_risk_metrics(&self, returns: &[f64]) -> Result<AlphaBetaMetrics, Box<dyn std::error::Error>> {
        let market_data = self.market_benchmark.read().unwrap();
        let market_returns: Vec<f64> = market_data.returns.iter().copied().collect();
        self.calculate_alpha_beta(returns, &market_returns)
    }

    fn determine_strategy_type(
        &self,
        alpha: f64,
        beta: f64,
        correlation: f64,
        sharpe_ratio: f64,
    ) -> StrategyType {
        if alpha.abs() < 0.001 && beta.abs() < 0.1 {
            StrategyType::Arbitrage
                } else if alpha > ALPHA_THRESHOLD && beta.abs() < 0.3 {
            StrategyType::PureAlpha
        } else if alpha > ALPHA_THRESHOLD * 0.5 && beta < 0.5 {
            StrategyType::AlphaDominant
        } else if beta.abs() < 0.2 && correlation.abs() < 0.3 {
            StrategyType::MarketNeutral
        } else if alpha > 0.0 && sharpe_ratio > 1.5 && beta > 0.8 {
            StrategyType::MomentumAlpha
        } else if alpha > 0.0 && sharpe_ratio > 1.0 && beta < -0.3 {
            StrategyType::MeanReversionAlpha
        } else if beta > 0.7 && alpha < ALPHA_THRESHOLD * 0.3 {
            StrategyType::BetaDominant
        } else {
            StrategyType::PureBeta
        }
    }

    fn calculate_risk_adjusted_score(
        &self,
        metrics: &AlphaBetaMetrics,
        risk_metrics: &AlphaBetaMetrics,
        opportunity: &TradingOpportunity,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let risk_params = self.risk_parameters.read().unwrap();
        let exec_stats = self.execution_stats.read().unwrap();
        
        let alpha_score = metrics.alpha * 100.0;
        let sharpe_score = metrics.sharpe_ratio.max(0.0) * 25.0;
        let sortino_score = metrics.sortino_ratio.max(0.0) * 20.0;
        let drawdown_penalty = metrics.max_drawdown.abs() * 50.0;
        let var_penalty = metrics.var_95.abs() * 30.0;
        
        let liquidity_score = (opportunity.pool_liquidity as f64 / MIN_LIQUIDITY_THRESHOLD as f64)
            .min(2.0)
            .ln()
            .max(0.0) * 10.0;
        
        let execution_score = exec_stats.success_rate * 20.0
            - (exec_stats.average_slippage * 1000.0)
            - (exec_stats.average_latency.as_millis() as f64 / 100.0);
        
        let confidence_weight = opportunity.confidence.powi(2);
        let market_impact_penalty = opportunity.market_impact * 200.0;
        let execution_risk_penalty = opportunity.execution_risk * 150.0;
        
        let stress_penalty = risk_params.stress_test_scenarios.iter()
            .map(|scenario| {
                let potential_loss = scenario.market_shock * metrics.beta
                    + scenario.liquidity_reduction * 0.02
                    + scenario.correlation_increase * metrics.correlation_to_market.abs() * 0.05;
                potential_loss.abs() * scenario.probability * 100.0
            })
            .sum::<f64>();
        
        let base_score = alpha_score + sharpe_score + sortino_score + liquidity_score + execution_score
            - drawdown_penalty - var_penalty - market_impact_penalty - execution_risk_penalty - stress_penalty;
        
        let final_score = base_score * confidence_weight * (1.0 - opportunity.execution_risk);
        
        Ok(final_score.max(-100.0).min(100.0))
    }

    fn calculate_optimal_position_size(
        &self,
        opportunity: &TradingOpportunity,
        metrics: &AlphaBetaMetrics,
        risk_metrics: &AlphaBetaMetrics,
    ) -> Result<f64, Box<dyn std::error::Error>> {
        let risk_params = self.risk_parameters.read().unwrap();
        
        let kelly_criterion = if metrics.volatility > 1e-10 {
            let edge = metrics.alpha + (metrics.beta * 0.005);
            let odds = opportunity.expected_profit / opportunity.size as f64;
            (edge * odds) / metrics.volatility.powi(2)
        } else {
            0.0
        };
        
        let kelly_fraction = kelly_criterion.max(0.0).min(0.25) * 0.4;
        
        let volatility_adjustment = (0.02 / metrics.volatility.max(0.01)).min(2.0);
        let sharpe_adjustment = (metrics.sharpe_ratio / 2.0).max(0.1).min(1.5);
        let liquidity_constraint = (opportunity.pool_liquidity as f64 * 0.02) / opportunity.size as f64;
        
        let var_constraint = if metrics.var_95.abs() > 1e-10 {
            risk_params.dynamic_risk_limit / metrics.var_95.abs()
        } else {
            1.0
        };
        
        let correlation_adjustment = 1.0 - (metrics.correlation_to_market.abs() * 0.3);
        let drawdown_constraint = if metrics.max_drawdown.abs() > 0.05 {
            0.05 / metrics.max_drawdown.abs()
        } else {
            1.0
        };
        
        let market_impact_constraint = 1.0 / (1.0 + opportunity.market_impact * 10.0);
        let execution_risk_adjustment = 1.0 - opportunity.execution_risk;
        
        let base_position = kelly_fraction
            * volatility_adjustment
            * sharpe_adjustment
            * correlation_adjustment
            * execution_risk_adjustment;
        
        let constrained_position = base_position
            .min(liquidity_constraint)
            .min(var_constraint)
            .min(drawdown_constraint)
            .min(market_impact_constraint)
            .min(risk_params.max_concentration);
        
        let final_position = constrained_position
            .max(0.001)
            .min(1.0)
            * (MAX_POSITION_SIZE as f64 / opportunity.size as f64);
        
        Ok(final_position)
    }

    fn calculate_risk_limits(
        &self,
        entry_price: f64,
        metrics: &AlphaBetaMetrics,
        strategy_type: &StrategyType,
    ) -> (f64, f64) {
        let base_stop_loss = match strategy_type {
            StrategyType::PureAlpha => 0.015,
            StrategyType::AlphaDominant => 0.02,
            StrategyType::MarketNeutral => 0.01,
            StrategyType::BetaDominant => 0.025,
            StrategyType::PureBeta => 0.03,
            StrategyType::Arbitrage => 0.005,
            StrategyType::MomentumAlpha => 0.02,
            StrategyType::MeanReversionAlpha => 0.025,
        };
        
        let volatility_adjusted_stop = base_stop_loss * (1.0 + metrics.volatility * 2.0);
        let var_adjusted_stop = metrics.var_95.abs().max(volatility_adjusted_stop);
        
        let stop_loss_price = entry_price * (1.0 - var_adjusted_stop.min(0.05));
        
        let risk_reward_ratio = match strategy_type {
            StrategyType::PureAlpha => 3.0,
            StrategyType::AlphaDominant => 2.5,
            StrategyType::MarketNeutral => 2.0,
            StrategyType::BetaDominant => 1.5,
            StrategyType::PureBeta => 1.2,
            StrategyType::Arbitrage => 4.0,
            StrategyType::MomentumAlpha => 2.5,
            StrategyType::MeanReversionAlpha => 2.0,
        };
        
        let expected_profit = var_adjusted_stop * risk_reward_ratio;
        let sharpe_adjusted_profit = expected_profit * (1.0 + metrics.sharpe_ratio.max(0.0) * 0.2);
        
        let take_profit_price = entry_price * (1.0 + sharpe_adjusted_profit.min(0.1));
        
        (stop_loss_price, take_profit_price)
    }

    fn calculate_execution_priority(
        &self,
        strategy_type: &StrategyType,
        risk_score: f64,
        confidence: f64,
    ) -> u8 {
        let type_priority = match strategy_type {
            StrategyType::Arbitrage => 100,
            StrategyType::PureAlpha => 90,
            StrategyType::AlphaDominant => 80,
            StrategyType::MomentumAlpha => 75,
            StrategyType::MeanReversionAlpha => 70,
            StrategyType::MarketNeutral => 60,
            StrategyType::BetaDominant => 40,
            StrategyType::PureBeta => 20,
        };
        
        let score_component = ((risk_score + 100.0) / 200.0 * 50.0) as u8;
        let confidence_component = (confidence * 50.0) as u8;
        
        let raw_priority = type_priority + score_component + confidence_component;
        raw_priority.min(255)
    }

    fn calculate_max_drawdown(&self, returns: &[f64]) -> f64 {
        if returns.is_empty() {
            return 0.0;
        }
        
        let mut cumulative = 1.0;
        let mut peak = 1.0;
        let mut max_drawdown = 0.0;
        
        for &ret in returns {
            cumulative *= 1.0 + ret;
            if cumulative > peak {
                peak = cumulative;
            }
            let drawdown = (peak - cumulative) / peak;
            if drawdown > max_drawdown {
                max_drawdown = drawdown;
            }
        }
        
        max_drawdown
    }

    fn calculate_higher_moments(&self, returns: &[f64]) -> (f64, f64) {
        if returns.len() < 4 {
            return (0.0, 3.0);
        }
        
        let mean = mean(returns);
        let std_dev = standard_deviation(returns);
        
        if std_dev < 1e-10 {
            return (0.0, 3.0);
        }
        
        let n = returns.len() as f64;
        let mut sum_cubed = 0.0;
        let mut sum_fourth = 0.0;
        
        for &ret in returns {
            let deviation = (ret - mean) / std_dev;
            sum_cubed += deviation.powi(3);
            sum_fourth += deviation.powi(4);
        }
        
        let skewness = sum_cubed / n;
        let kurtosis = sum_fourth / n;
        
        (skewness, kurtosis)
    }

    fn calculate_var_cvar(&self, returns: &[f64], confidence: f64) -> (f64, f64) {
        if returns.is_empty() {
            return (0.0, 0.0);
        }
        
        let mut sorted_returns = returns.to_vec();
        sorted_returns.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        let var_index = ((1.0 - confidence) * sorted_returns.len() as f64) as usize;
        let var = sorted_returns.get(var_index).copied().unwrap_or(0.0);
        
        let cvar_returns: Vec<f64> = sorted_returns.iter()
            .take(var_index.max(1))
            .copied()
            .collect();
        
        let cvar = if !cvar_returns.is_empty() {
            mean(&cvar_returns)
        } else {
            var
        };
        
        (var, cvar)
    }

    fn update_performance_cache(
        &self,
        id: [u8; 32],
        metrics: AlphaBetaMetrics,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut cache = self.performance_cache.write().unwrap();
        cache.insert(id, metrics);
        
        if cache.len() > 10000 {
            let mut entries: Vec<_> = cache.iter().map(|(k, v)| (*k, v.timestamp)).collect();
            entries.sort_by_key(|&(_, timestamp)| timestamp);
            
            for (key, _) in entries.iter().take(1000) {
                cache.remove(key);
            }
        }
        
        Ok(())
    }

    pub fn update_market_benchmark(&self, market_return: f64) -> Result<(), Box<dyn std::error::Error>> {
        let mut benchmark = self.market_benchmark.write().unwrap();
        
        benchmark.returns.push_back(market_return);
        if benchmark.returns.len() > ROLLING_WINDOW_SIZE {
            benchmark.returns.pop_front();
        }
        
        if benchmark.returns.len() >= 20 {
            let returns_vec: Vec<f64> = benchmark.returns.iter().copied().collect();
            benchmark.volatility = standard_deviation(&returns_vec);
            benchmark.cumulative_return *= 1.0 + market_return;
        }
        
        benchmark.last_update = Instant::now();
        Ok(())
    }

    pub fn update_execution_stats(
        &self,
        success: bool,
        slippage: f64,
        latency: Duration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut stats = self.execution_stats.write().unwrap();
        
        stats.total_opportunities += 1;
        if !success {
            stats.failed_opportunities += 1;
        }
        
        let decay = DECAY_FACTOR;
        stats.success_rate = stats.success_rate * decay + (if success { 1.0 } else { 0.0 }) * (1.0 - decay);
        stats.average_slippage = stats.average_slippage * decay + slippage * (1.0 - decay);
        
        let current_latency_ms = latency.as_millis() as f64;
        let avg_latency_ms = stats.average_latency.as_millis() as f64;
        let new_avg_ms = avg_latency_ms * decay + current_latency_ms * (1.0 - decay);
        stats.average_latency = Duration::from_millis(new_avg_ms as u64);
        
        Ok(())
    }

    pub fn get_strategy_performance(&self, strategy_id: [u8; 32]) -> Option<AlphaBetaMetrics> {
                self.performance_cache.read().unwrap().get(&strategy_id).cloned()
    }

    pub fn get_active_strategies(&self) -> Vec<([u8; 32], StrategyType, f64)> {
        let cache = self.performance_cache.read().unwrap();
        let mut strategies = Vec::new();
        
        for (id, metrics) in cache.iter() {
            let strategy_type = self.determine_strategy_type(
                metrics.alpha,
                metrics.beta,
                metrics.correlation_to_market,
                metrics.sharpe_ratio,
            );
            strategies.push((*id, strategy_type, metrics.sharpe_ratio));
        }
        
        strategies.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));
        strategies
    }

    pub fn perform_risk_analysis(&self, positions: &[(TradingOpportunity, f64)]) -> Result<RiskAnalysis, Box<dyn std::error::Error>> {
        let mut total_var = 0.0;
        let mut total_cvar = 0.0;
        let mut correlation_matrix = vec![vec![0.0; positions.len()]; positions.len()];
        let mut position_weights = Vec::new();
        let mut total_weight = 0.0;
        
        for (opportunity, size) in positions {
            position_weights.push(*size);
            total_weight += *size;
        }
        
        if total_weight > 0.0 {
            for weight in position_weights.iter_mut() {
                *weight /= total_weight;
            }
        }
        
        let strategy_returns = self.strategy_returns.read().unwrap();
        
        for i in 0..positions.len() {
            let (opp_i, _) = &positions[i];
            if let Some(returns_i) = strategy_returns.get(&opp_i.id) {
                let returns_vec_i: Vec<f64> = returns_i.iter().copied().collect();
                let (var_i, cvar_i) = self.calculate_var_cvar(&returns_vec_i, CONFIDENCE_INTERVAL);
                
                total_var += var_i * position_weights[i].powi(2);
                total_cvar += cvar_i * position_weights[i].powi(2);
                
                for j in 0..positions.len() {
                    if i != j {
                        let (opp_j, _) = &positions[j];
                        if let Some(returns_j) = strategy_returns.get(&opp_j.id) {
                            let returns_vec_j: Vec<f64> = returns_j.iter().copied().collect();
                            let correlation = self.calculate_correlation(&returns_vec_i, &returns_vec_j);
                            correlation_matrix[i][j] = correlation;
                            
                            let std_i = standard_deviation(&returns_vec_i);
                            let std_j = standard_deviation(&returns_vec_j);
                            total_var += 2.0 * position_weights[i] * position_weights[j] * correlation * std_i * std_j;
                        }
                    }
                }
            }
        }
        
        let portfolio_var = total_var.sqrt();
        let portfolio_cvar = total_cvar.sqrt();
        
        let risk_params = self.risk_parameters.read().unwrap();
        let stress_test_results: Vec<f64> = risk_params.stress_test_scenarios.iter()
            .map(|scenario| {
                let mut stress_loss = 0.0;
                for (i, (opportunity, size)) in positions.iter().enumerate() {
                    if let Some(metrics) = self.performance_cache.read().unwrap().get(&opportunity.id) {
                        let position_loss = scenario.market_shock * metrics.beta * position_weights[i]
                            + scenario.liquidity_reduction * 0.01 * position_weights[i]
                            + scenario.correlation_increase * metrics.correlation_to_market.abs() * 0.02 * position_weights[i];
                        stress_loss += position_loss;
                    }
                }
                stress_loss
            })
            .collect();
        
        let max_correlation = correlation_matrix.iter()
            .flat_map(|row| row.iter())
            .filter(|&&c| c < 1.0)
            .fold(0.0, |max, &c| max.max(c.abs()));
        
        Ok(RiskAnalysis {
            portfolio_var,
            portfolio_cvar,
            max_correlation,
            stress_test_results,
            risk_utilization: portfolio_var / risk_params.dynamic_risk_limit,
            diversification_ratio: self.calculate_diversification_ratio(&correlation_matrix, &position_weights),
        })
    }

    fn calculate_correlation(&self, returns_a: &[f64], returns_b: &[f64]) -> f64 {
        if returns_a.len() < 20 || returns_b.len() < 20 {
            return 0.0;
        }
        
        let n = returns_a.len().min(returns_b.len());
        let slice_a = &returns_a[returns_a.len().saturating_sub(n)..];
        let slice_b = &returns_b[returns_b.len().saturating_sub(n)..];
        
        let mean_a = mean(slice_a);
        let mean_b = mean(slice_b);
        let std_a = standard_deviation(slice_a);
        let std_b = standard_deviation(slice_b);
        
        if std_a < 1e-10 || std_b < 1e-10 {
            return 0.0;
        }
        
        let mut covariance = 0.0;
        for i in 0..n {
            covariance += (slice_a[i] - mean_a) * (slice_b[i] - mean_b);
        }
        covariance /= n as f64;
        
        covariance / (std_a * std_b)
    }

    fn calculate_diversification_ratio(&self, correlation_matrix: &[Vec<f64>], weights: &[f64]) -> f64 {
        let n = weights.len();
        if n == 0 {
            return 1.0;
        }
        
        let mut weighted_avg_correlation = 0.0;
        let mut weight_sum = 0.0;
        
        for i in 0..n {
            for j in 0..n {
                if i != j {
                    weighted_avg_correlation += weights[i] * weights[j] * correlation_matrix[i][j].abs();
                    weight_sum += weights[i] * weights[j];
                }
            }
        }
        
        if weight_sum > 0.0 {
            1.0 / (1.0 + weighted_avg_correlation / weight_sum)
        } else {
            1.0
        }
    }

    pub fn optimize_portfolio(
        &self,
        opportunities: Vec<TradingOpportunity>,
        capital: u64,
    ) -> Result<Vec<(TradingOpportunity, u64)>, Box<dyn std::error::Error>> {
        let mut classified_opps = Vec::new();
        
        for opp in opportunities {
            match self.classify_opportunity(&opp) {
                Ok(classification) => {
                    classified_opps.push((opp, classification));
                }
                Err(_) => continue,
            }
        }
        
        classified_opps.sort_by(|a, b| {
            b.1.execution_priority.cmp(&a.1.execution_priority)
                .then(b.1.risk_adjusted_score.partial_cmp(&a.1.risk_adjusted_score)
                    .unwrap_or(std::cmp::Ordering::Equal))
        });
        
        let mut portfolio = Vec::new();
        let mut remaining_capital = capital as f64;
        let mut total_risk = 0.0;
        let risk_params = self.risk_parameters.read().unwrap();
        
        for (opp, classification) in classified_opps {
            if remaining_capital < MIN_LIQUIDITY_THRESHOLD as f64 {
                break;
            }
            
            let position_size = (classification.position_sizing * remaining_capital)
                .min(opp.pool_liquidity as f64 * 0.02)
                .min(remaining_capital * risk_params.max_concentration)
                .min(MAX_POSITION_SIZE as f64);
            
            if position_size < MIN_LIQUIDITY_THRESHOLD as f64 {
                continue;
            }
            
            let position_risk = if let Some(metrics) = self.performance_cache.read().unwrap().get(&opp.id) {
                metrics.var_95.abs() * position_size / capital as f64
            } else {
                0.02 * position_size / capital as f64
            };
            
            if total_risk + position_risk > risk_params.dynamic_risk_limit {
                continue;
            }
            
            let mut add_position = true;
            if !portfolio.is_empty() {
                let positions_with_size: Vec<(TradingOpportunity, f64)> = portfolio.iter()
                    .map(|(o, s): &(TradingOpportunity, u64)| (o.clone(), *s as f64))
                    .chain(std::iter::once((opp.clone(), position_size)))
                    .collect();
                
                if let Ok(risk_analysis) = self.perform_risk_analysis(&positions_with_size) {
                    if risk_analysis.max_correlation > 0.8 || risk_analysis.risk_utilization > 0.95 {
                        add_position = false;
                    }
                }
            }
            
            if add_position {
                portfolio.push((opp, position_size as u64));
                remaining_capital -= position_size;
                total_risk += position_risk;
            }
        }
        
        Ok(portfolio)
    }

    pub fn generate_execution_instructions(
        &self,
        opportunity: &TradingOpportunity,
        classification: &StrategyClassification,
        slippage_tolerance: u16,
    ) -> Result<ExecutionPlan, Box<dyn std::error::Error>> {
        let adjusted_slippage = slippage_tolerance.min(MAX_SLIPPAGE_BPS);
        
        let compute_budget = match classification.strategy_type {
            StrategyType::Arbitrage => 1_400_000,
            StrategyType::PureAlpha => 1_200_000,
            StrategyType::AlphaDominant => 1_000_000,
            _ => 800_000,
        };
        
        let priority_fee = self.calculate_priority_fee(
            classification.execution_priority,
            opportunity.expected_profit,
            opportunity.priority_fee,
        );
        
        Ok(ExecutionPlan {
            opportunity_id: opportunity.id,
            entry_price: opportunity.entry_price,
            position_size: (classification.position_sizing * opportunity.size as f64) as u64,
            stop_loss: classification.stop_loss,
            take_profit: classification.take_profit,
            max_slippage_bps: adjusted_slippage,
            priority_fee,
            compute_budget,
            execution_deadline: Clock::get()?.unix_timestamp + 5,
            strategy_type: classification.strategy_type.clone(),
        })
    }

    fn calculate_priority_fee(&self, priority: u8, expected_profit: f64, base_fee: u64) -> u64 {
        let priority_multiplier = 1.0 + (priority as f64 / 255.0) * 2.0;
        let profit_based_fee = (expected_profit * 0.001 * 1e9) as u64;
        let dynamic_fee = base_fee as f64 * priority_multiplier;
        
        dynamic_fee.max(profit_based_fee as f64).min(50_000_000) as u64
    }
}

#[derive(Debug, Clone)]
pub struct RiskAnalysis {
    pub portfolio_var: f64,
    pub portfolio_cvar: f64,
    pub max_correlation: f64,
    pub stress_test_results: Vec<f64>,
    pub risk_utilization: f64,
    pub diversification_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    pub opportunity_id: [u8; 32],
    pub entry_price: f64,
    pub position_size: u64,
    pub stop_loss: f64,
    pub take_profit: f64,
    pub max_slippage_bps: u16,
    pub priority_fee: u64,
    pub compute_budget: u32,
    pub execution_deadline: i64,
    pub strategy_type: StrategyType,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alpha_beta_separator() {
        let separator = AlphaBetaSeparator::new();
        assert!(separator.update_market_benchmark(0.01).is_ok());
    }
}

