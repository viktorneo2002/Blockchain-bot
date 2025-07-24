use solana_sdk::{
    clock::Clock,
    pubkey::Pubkey,
    signature::Signature,
};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::mpsc;
use serde::{Deserialize, Serialize};

const PERFORMANCE_WINDOW_SIZE: usize = 10000;
const DECAY_FACTOR: f64 = 0.995;
const MIN_SAMPLE_SIZE: usize = 100;
const OUTLIER_THRESHOLD: f64 = 3.0;
const LATENCY_BUCKETS: [u64; 10] = [1, 5, 10, 25, 50, 100, 250, 500, 1000, 5000];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub timestamp: u64,
    pub slot: u64,
    pub strategy_type: StrategyType,
    pub gross_pnl: i64,
    pub net_pnl: i64,
    pub priority_fees: u64,
    pub slippage_cost: i64,
    pub opportunity_cost: i64,
    pub execution_latency_us: u64,
    pub mempool_latency_us: u64,
    pub success: bool,
    pub failure_reason: Option<FailureReason>,
    pub competition_level: f64,
    pub market_impact: f64,
    pub realized_edge: f64,
    pub theoretical_edge: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum StrategyType {
    Arbitrage,
    Liquidation,
    JitLiquidity,
    Sandwich,
    BackRun,
    FrontRun,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FailureReason {
    InsufficientProfit,
    SlippageExceeded,
    CompetitorWon,
    NetworkCongestion,
    SimulationFailed,
    TransactionReverted,
    StaleData,
    RiskLimitExceeded,
}

#[derive(Debug, Clone)]
pub struct DecomposedPerformance {
    pub total_pnl: i64,
    pub strategy_attribution: HashMap<StrategyType, StrategyAttribution>,
    pub cost_breakdown: CostBreakdown,
    pub timing_analysis: TimingAnalysis,
    pub market_conditions: MarketConditions,
    pub risk_metrics: RiskMetrics,
}

#[derive(Debug, Clone)]
pub struct StrategyAttribution {
    pub gross_pnl: i64,
    pub net_pnl: i64,
    pub opportunity_count: u64,
    pub execution_count: u64,
    pub success_rate: f64,
    pub avg_profit_per_trade: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub win_rate: f64,
    pub profit_factor: f64,
}

#[derive(Debug, Clone)]
pub struct CostBreakdown {
    pub total_priority_fees: u64,
    pub avg_priority_fee: f64,
    pub total_slippage: i64,
    pub avg_slippage_bps: f64,
    pub opportunity_cost: i64,
    pub market_impact_cost: i64,
}

#[derive(Debug, Clone)]
pub struct TimingAnalysis {
    pub avg_execution_latency: f64,
    pub p50_execution_latency: u64,
    pub p95_execution_latency: u64,
    pub p99_execution_latency: u64,
    pub latency_distribution: HashMap<u64, u64>,
    pub optimal_timing_rate: f64,
}

#[derive(Debug, Clone)]
pub struct MarketConditions {
    pub avg_competition_level: f64,
    pub volatility_regime: VolatilityRegime,
    pub liquidity_depth: f64,
    pub trend_strength: f64,
    pub market_efficiency: f64,
}

#[derive(Debug, Clone, Copy)]
pub enum VolatilityRegime {
    Low,
    Medium,
    High,
    Extreme,
}

#[derive(Debug, Clone)]
pub struct RiskMetrics {
    pub value_at_risk: f64,
    pub conditional_value_at_risk: f64,
    pub max_position_size: u64,
    pub correlation_risk: f64,
    pub tail_risk: f64,
}

pub struct StrategyPerformanceDecomposer {
    metrics_buffer: Arc<RwLock<VecDeque<PerformanceMetrics>>>,
    strategy_stats: Arc<RwLock<HashMap<StrategyType, StrategyStatistics>>>,
    market_state: Arc<RwLock<MarketState>>,
    risk_engine: Arc<RwLock<RiskEngine>>,
}

struct StrategyStatistics {
    total_trades: u64,
    successful_trades: u64,
    total_gross_pnl: i64,
    total_net_pnl: i64,
    total_fees: u64,
    pnl_history: VecDeque<i64>,
    latency_histogram: HashMap<u64, u64>,
    rolling_sharpe: f64,
    max_drawdown: f64,
    current_drawdown: f64,
    peak_pnl: i64,
}

struct MarketState {
    volatility_window: VecDeque<f64>,
    competition_scores: VecDeque<f64>,
    liquidity_measures: VecDeque<f64>,
    price_momentum: f64,
    efficiency_score: f64,
}

struct RiskEngine {
    position_limits: HashMap<Pubkey, u64>,
    var_calculator: VaRCalculator,
    correlation_matrix: HashMap<(Pubkey, Pubkey), f64>,
    tail_risk_estimator: TailRiskEstimator,
}

struct VaRCalculator {
    confidence_level: f64,
    lookback_period: usize,
    returns_history: VecDeque<f64>,
}

struct TailRiskEstimator {
    extreme_losses: VecDeque<f64>,
    threshold_percentile: f64,
}

impl StrategyPerformanceDecomposer {
    pub fn new() -> Self {
        Self {
            metrics_buffer: Arc::new(RwLock::new(VecDeque::with_capacity(PERFORMANCE_WINDOW_SIZE))),
            strategy_stats: Arc::new(RwLock::new(HashMap::new())),
            market_state: Arc::new(RwLock::new(MarketState::new())),
            risk_engine: Arc::new(RwLock::new(RiskEngine::new())),
        }
    }

    pub fn record_performance(&self, metrics: PerformanceMetrics) -> Result<(), String> {
        let mut buffer = self.metrics_buffer.write().map_err(|e| e.to_string())?;
        
        if buffer.len() >= PERFORMANCE_WINDOW_SIZE {
            buffer.pop_front();
        }
        
        buffer.push_back(metrics.clone());
        drop(buffer);

        self.update_strategy_statistics(&metrics)?;
        self.update_market_state(&metrics)?;
        self.update_risk_metrics(&metrics)?;

        Ok(())
    }

    pub fn decompose_performance(&self, window_slots: u64) -> Result<DecomposedPerformance, String> {
        let buffer = self.metrics_buffer.read().map_err(|e| e.to_string())?;
        let current_slot = buffer.back().map(|m| m.slot).unwrap_or(0);
        let cutoff_slot = current_slot.saturating_sub(window_slots);
        
        let window_metrics: Vec<_> = buffer.iter()
            .filter(|m| m.slot >= cutoff_slot)
            .cloned()
            .collect();
        
        drop(buffer);

        if window_metrics.len() < MIN_SAMPLE_SIZE {
            return Err("Insufficient data for decomposition".to_string());
        }

        let total_pnl = self.calculate_total_pnl(&window_metrics);
        let strategy_attribution = self.calculate_strategy_attribution(&window_metrics)?;
        let cost_breakdown = self.calculate_cost_breakdown(&window_metrics);
        let timing_analysis = self.calculate_timing_analysis(&window_metrics);
        let market_conditions = self.analyze_market_conditions(&window_metrics)?;
        let risk_metrics = self.calculate_risk_metrics(&window_metrics)?;

        Ok(DecomposedPerformance {
            total_pnl,
            strategy_attribution,
            cost_breakdown,
            timing_analysis,
            market_conditions,
            risk_metrics,
        })
    }

    fn update_strategy_statistics(&self, metrics: &PerformanceMetrics) -> Result<(), String> {
        let mut stats = self.strategy_stats.write().map_err(|e| e.to_string())?;
        
        let strategy_stat = stats.entry(metrics.strategy_type).or_insert_with(|| {
            StrategyStatistics {
                total_trades: 0,
                successful_trades: 0,
                total_gross_pnl: 0,
                total_net_pnl: 0,
                total_fees: 0,
                pnl_history: VecDeque::with_capacity(1000),
                latency_histogram: HashMap::new(),
                rolling_sharpe: 0.0,
                max_drawdown: 0.0,
                current_drawdown: 0.0,
                peak_pnl: 0,
            }
        });

        strategy_stat.total_trades += 1;
        if metrics.success {
            strategy_stat.successful_trades += 1;
        }
        
        strategy_stat.total_gross_pnl += metrics.gross_pnl;
        strategy_stat.total_net_pnl += metrics.net_pnl;
        strategy_stat.total_fees += metrics.priority_fees;

        if strategy_stat.pnl_history.len() >= 1000 {
            strategy_stat.pnl_history.pop_front();
        }
        strategy_stat.pnl_history.push_back(metrics.net_pnl);

        let latency_bucket = self.get_latency_bucket(metrics.execution_latency_us);
        *strategy_stat.latency_histogram.entry(latency_bucket).or_insert(0) += 1;

        self.update_drawdown(strategy_stat);
        self.update_rolling_sharpe(strategy_stat);

        Ok(())
    }

    fn update_market_state(&self, metrics: &PerformanceMetrics) -> Result<(), String> {
        let mut state = self.market_state.write().map_err(|e| e.to_string())?;
        
        if state.volatility_window.len() >= 1000 {
            state.volatility_window.pop_front();
        }
        state.volatility_window.push_back(metrics.market_impact.abs());

        if state.competition_scores.len() >= 1000 {
            state.competition_scores.pop_front();
        }
        state.competition_scores.push_back(metrics.competition_level);

        let liquidity_proxy = if metrics.slippage_cost != 0 {
            (metrics.gross_pnl as f64) / (metrics.slippage_cost.abs() as f64)
        } else {
            100.0
        };
        
        if state.liquidity_measures.len() >= 1000 {
            state.liquidity_measures.pop_front();
        }
        state.liquidity_measures.push_back(liquidity_proxy);

        state.price_momentum = self.calculate_momentum(&state.volatility_window);
        state.efficiency_score = self.calculate_efficiency_score(metrics, &state.competition_scores);

        Ok(())
    }

    fn update_risk_metrics(&self, metrics: &PerformanceMetrics) -> Result<(), String> {
        let mut engine = self.risk_engine.write().map_err(|e| e.to_string())?;
        
        let return_pct = if metrics.theoretical_edge != 0.0 {
            metrics.net_pnl as f64 / metrics.theoretical_edge
        } else {
            0.0
        };

        if engine.var_calculator.returns_history.len() >= engine.var_calculator.lookback_period {
            engine.var_calculator.returns_history.pop_front();
        }
        engine.var_calculator.returns_history.push_back(return_pct);

        if return_pct < -2.0 {
            if engine.tail_risk_estimator.extreme_losses.len() >= 100 {
                engine.tail_risk_estimator.extreme_losses.pop_front();
            }
            engine.tail_risk_estimator.extreme_losses.push_back(return_pct);
        }

        Ok(())
    }

    fn calculate_total_pnl(&self, metrics: &[PerformanceMetrics]) -> i64 {
        metrics.iter().map(|m| m.net_pnl).sum()
    }

    fn calculate_strategy_attribution(&self, metrics: &[PerformanceMetrics]) -> Result<HashMap<StrategyType, StrategyAttribution>, String> {
        let mut attributions = HashMap::new();
        let stats = self.strategy_stats.read().map_err(|e| e.to_string())?;

        for (strategy_type, stat) in stats.iter() {
            let strategy_metrics: Vec<_> = metrics.iter()
                .filter(|m| m.strategy_type == *strategy_type)
                .collect();

            if strategy_metrics.is_empty() {
                continue;
            }

            let gross_pnl: i64 = strategy_metrics.iter().map(|m| m.gross_pnl).sum();
            let net_pnl: i64 = strategy_metrics.iter().map(|m| m.net_pnl).sum();
            let opportunity_count = strategy_metrics.len() as u64;
            let execution_count = strategy_metrics.iter().filter(|m| m.success).count() as u64;
                        let success_rate = execution_count as f64 / opportunity_count as f64;
            
            let profits: Vec<i64> = strategy_metrics.iter()
                .filter(|m| m.net_pnl > 0)
                .map(|m| m.net_pnl)
                .collect();
            
            let losses: Vec<i64> = strategy_metrics.iter()
                .filter(|m| m.net_pnl < 0)
                .map(|m| m.net_pnl.abs())
                .collect();
            
            let avg_profit_per_trade = net_pnl as f64 / opportunity_count as f64;
            let win_rate = profits.len() as f64 / opportunity_count as f64;
            
            let avg_win = if !profits.is_empty() {
                profits.iter().sum::<i64>() as f64 / profits.len() as f64
            } else {
                0.0
            };
            
            let avg_loss = if !losses.is_empty() {
                losses.iter().sum::<i64>() as f64 / losses.len() as f64
            } else {
                0.0
            };
            
            let profit_factor = if avg_loss > 0.0 {
                (avg_win * profits.len() as f64) / (avg_loss * losses.len() as f64)
            } else {
                f64::INFINITY
            };

            attributions.insert(*strategy_type, StrategyAttribution {
                gross_pnl,
                net_pnl,
                opportunity_count,
                execution_count,
                success_rate,
                avg_profit_per_trade,
                sharpe_ratio: stat.rolling_sharpe,
                max_drawdown: stat.max_drawdown,
                win_rate,
                profit_factor,
            });
        }

        Ok(attributions)
    }

    fn calculate_cost_breakdown(&self, metrics: &[PerformanceMetrics]) -> CostBreakdown {
        let total_priority_fees: u64 = metrics.iter().map(|m| m.priority_fees).sum();
        let total_slippage: i64 = metrics.iter().map(|m| m.slippage_cost).sum();
        let opportunity_cost: i64 = metrics.iter().map(|m| m.opportunity_cost).sum();
        let market_impact_cost: i64 = metrics.iter()
            .map(|m| (m.market_impact * m.gross_pnl as f64) as i64)
            .sum();
        
        let avg_priority_fee = total_priority_fees as f64 / metrics.len() as f64;
        
        let total_volume: f64 = metrics.iter()
            .map(|m| m.theoretical_edge.abs())
            .sum();
        
        let avg_slippage_bps = if total_volume > 0.0 {
            (total_slippage.abs() as f64 / total_volume) * 10000.0
        } else {
            0.0
        };

        CostBreakdown {
            total_priority_fees,
            avg_priority_fee,
            total_slippage,
            avg_slippage_bps,
            opportunity_cost,
            market_impact_cost,
        }
    }

    fn calculate_timing_analysis(&self, metrics: &[PerformanceMetrics]) -> TimingAnalysis {
        let mut latencies: Vec<u64> = metrics.iter()
            .map(|m| m.execution_latency_us)
            .collect();
        latencies.sort_unstable();

        let avg_execution_latency = latencies.iter().sum::<u64>() as f64 / latencies.len() as f64;
        let p50_execution_latency = latencies[latencies.len() / 2];
        let p95_execution_latency = latencies[latencies.len() * 95 / 100];
        let p99_execution_latency = latencies[latencies.len() * 99 / 100];

        let mut latency_distribution = HashMap::new();
        for latency in &latencies {
            let bucket = self.get_latency_bucket(*latency);
            *latency_distribution.entry(bucket).or_insert(0) += 1;
        }

        let optimal_timings = metrics.iter()
            .filter(|m| m.success && m.net_pnl > 0 && m.execution_latency_us < p50_execution_latency)
            .count();
        
        let optimal_timing_rate = optimal_timings as f64 / metrics.len() as f64;

        TimingAnalysis {
            avg_execution_latency,
            p50_execution_latency,
            p95_execution_latency,
            p99_execution_latency,
            latency_distribution,
            optimal_timing_rate,
        }
    }

    fn analyze_market_conditions(&self, metrics: &[PerformanceMetrics]) -> Result<MarketConditions, String> {
        let state = self.market_state.read().map_err(|e| e.to_string())?;
        
        let avg_competition_level = if !state.competition_scores.is_empty() {
            state.competition_scores.iter().sum::<f64>() / state.competition_scores.len() as f64
        } else {
            metrics.iter().map(|m| m.competition_level).sum::<f64>() / metrics.len() as f64
        };

        let volatility = self.calculate_volatility(&state.volatility_window);
        let volatility_regime = match volatility {
            v if v < 0.005 => VolatilityRegime::Low,
            v if v < 0.015 => VolatilityRegime::Medium,
            v if v < 0.03 => VolatilityRegime::High,
            _ => VolatilityRegime::Extreme,
        };

        let liquidity_depth = if !state.liquidity_measures.is_empty() {
            let sum: f64 = state.liquidity_measures.iter().sum();
            sum / state.liquidity_measures.len() as f64
        } else {
            50.0
        };

        Ok(MarketConditions {
            avg_competition_level,
            volatility_regime,
            liquidity_depth,
            trend_strength: state.price_momentum,
            market_efficiency: state.efficiency_score,
        })
    }

    fn calculate_risk_metrics(&self, metrics: &[PerformanceMetrics]) -> Result<RiskMetrics, String> {
        let engine = self.risk_engine.read().map_err(|e| e.to_string())?;
        
        let value_at_risk = engine.var_calculator.calculate_var();
        let conditional_value_at_risk = engine.var_calculator.calculate_cvar();
        
        let max_position_size = metrics.iter()
            .map(|m| (m.theoretical_edge.abs() * 1e9) as u64)
            .max()
            .unwrap_or(0);
        
        let correlation_risk = self.calculate_correlation_risk(&engine.correlation_matrix);
        let tail_risk = engine.tail_risk_estimator.estimate_tail_risk();

        Ok(RiskMetrics {
            value_at_risk,
            conditional_value_at_risk,
            max_position_size,
            correlation_risk,
            tail_risk,
        })
    }

    fn get_latency_bucket(&self, latency_us: u64) -> u64 {
        for &bucket in &LATENCY_BUCKETS {
            if latency_us <= bucket {
                return bucket;
            }
        }
        LATENCY_BUCKETS[LATENCY_BUCKETS.len() - 1]
    }

    fn update_drawdown(&self, stats: &mut StrategyStatistics) {
        if stats.total_net_pnl > stats.peak_pnl {
            stats.peak_pnl = stats.total_net_pnl;
            stats.current_drawdown = 0.0;
        } else {
            stats.current_drawdown = (stats.peak_pnl - stats.total_net_pnl) as f64 / stats.peak_pnl.abs() as f64;
            if stats.current_drawdown > stats.max_drawdown {
                stats.max_drawdown = stats.current_drawdown;
            }
        }
    }

    fn update_rolling_sharpe(&self, stats: &mut StrategyStatistics) {
        if stats.pnl_history.len() < 30 {
            return;
        }

        let returns: Vec<f64> = stats.pnl_history.iter()
            .map(|&pnl| pnl as f64)
            .collect();
        
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|&r| (r - mean_return).powi(2))
            .sum::<f64>() / returns.len() as f64;
        
        let std_dev = variance.sqrt();
        
        if std_dev > 0.0 {
            stats.rolling_sharpe = (mean_return / std_dev) * (252.0_f64).sqrt();
        }
    }

    fn calculate_momentum(&self, window: &VecDeque<f64>) -> f64 {
        if window.len() < 20 {
            return 0.0;
        }

        let recent: Vec<f64> = window.iter().rev().take(10).cloned().collect();
        let past: Vec<f64> = window.iter().rev().skip(10).take(10).cloned().collect();
        
        let recent_avg = recent.iter().sum::<f64>() / recent.len() as f64;
        let past_avg = past.iter().sum::<f64>() / past.len() as f64;
        
        if past_avg > 0.0 {
            (recent_avg - past_avg) / past_avg
        } else {
            0.0
        }
    }

    fn calculate_efficiency_score(&self, metrics: &PerformanceMetrics, competition_scores: &VecDeque<f64>) -> f64 {
        let realized_ratio = if metrics.theoretical_edge != 0.0 {
            metrics.realized_edge / metrics.theoretical_edge
        } else {
            0.0
        };
        
        let avg_competition = if !competition_scores.is_empty() {
            competition_scores.iter().sum::<f64>() / competition_scores.len() as f64
        } else {
            0.5
        };
        
        let efficiency = realized_ratio * (1.0 - avg_competition);
        efficiency.max(0.0).min(1.0)
    }

    fn calculate_volatility(&self, window: &VecDeque<f64>) -> f64 {
        if window.len() < 2 {
            return 0.0;
        }

        let mean = window.iter().sum::<f64>() / window.len() as f64;
        let variance = window.iter()
            .map(|&x| (x - mean).powi(2))
            .sum::<f64>() / window.len() as f64;
        
        variance.sqrt()
    }

    fn calculate_correlation_risk(&self, correlation_matrix: &HashMap<(Pubkey, Pubkey), f64>) -> f64 {
        if correlation_matrix.is_empty() {
            return 0.0;
        }

        let max_correlation = correlation_matrix.values()
            .map(|&c| c.abs())
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or(0.0);
        
        max_correlation
    }
}

impl MarketState {
    fn new() -> Self {
        Self {
            volatility_window: VecDeque::with_capacity(1000),
            competition_scores: VecDeque::with_capacity(1000),
            liquidity_measures: VecDeque::with_capacity(1000),
            price_momentum: 0.0,
            efficiency_score: 0.5,
        }
    }
}

impl RiskEngine {
    fn new() -> Self {
        Self {
            position_limits: HashMap::new(),
            var_calculator: VaRCalculator::new(0.95, 100),
            correlation_matrix: HashMap::new(),
            tail_risk_estimator: TailRiskEstimator::new(0.05),
        }
    }
}

impl VaRCalculator {
    fn new(confidence_level: f64, lookback_period: usize) -> Self {
        Self {
            confidence_level,
            lookback_period,
            returns_history: VecDeque::with_capacity(lookback_period),
        }
    }

    fn calculate_var(&self) -> f64 {
        if self.returns_history.len() < 20 {
            return 0.0;
        }

        let mut sorted_returns: Vec<f64> = self.returns_history.iter().cloned().collect();
        sorted_returns.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let index = ((1.0 - self.confidence_level) * sorted_returns.len() as f64) as usize;
        sorted_returns[index].abs()
    }

    fn calculate_cvar(&self) -> f64 {
        if self.returns_history.len() < 20 {
            return 0.0;
        }

        let mut sorted_returns: Vec<f64> = self.returns_history.iter().cloned().collect();
        sorted_returns.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let index = ((1.0 - self.confidence_level) * sorted_returns.len() as f64) as usize;
        let tail_returns: Vec<f64> = sorted_returns[..=index].to_vec();
        
        let sum: f64 = tail_returns.iter().sum();
        (sum / tail_returns.len() as f64).abs()
    }
}

impl TailRiskEstimator {
    fn new(threshold_percentile: f64) -> Self {
        Self {
            extreme_losses: VecDeque::with_capacity(100),
            threshold_percentile,
        }
    }

    fn estimate_tail_risk(&self) -> f64 {
        if self.extreme_losses.is_empty() {
            return 0.0;
        }

                let mean_extreme_loss: f64 = self.extreme_losses.iter().sum::<f64>() / self.extreme_losses.len() as f64;
        mean_extreme_loss.abs()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_performance_decomposer_initialization() {
        let decomposer = StrategyPerformanceDecomposer::new();
        assert!(decomposer.metrics_buffer.read().unwrap().is_empty());
        assert!(decomposer.strategy_stats.read().unwrap().is_empty());
    }

    #[test]
    fn test_record_performance() {
        let decomposer = StrategyPerformanceDecomposer::new();
        let metrics = PerformanceMetrics {
            timestamp: 1700000000,
            slot: 250000000,
            strategy_type: StrategyType::Arbitrage,
            gross_pnl: 1000000,
            net_pnl: 950000,
            priority_fees: 50000,
            slippage_cost: -20000,
            opportunity_cost: -10000,
            execution_latency_us: 15000,
            mempool_latency_us: 5000,
            success: true,
            failure_reason: None,
            competition_level: 0.3,
            market_impact: 0.001,
            realized_edge: 0.95,
            theoretical_edge: 1.0,
        };

        assert!(decomposer.record_performance(metrics).is_ok());
        assert_eq!(decomposer.metrics_buffer.read().unwrap().len(), 1);
    }

    #[test]
    fn test_latency_bucket_calculation() {
        let decomposer = StrategyPerformanceDecomposer::new();
        assert_eq!(decomposer.get_latency_bucket(0), 1);
        assert_eq!(decomposer.get_latency_bucket(3), 5);
        assert_eq!(decomposer.get_latency_bucket(15), 25);
        assert_eq!(decomposer.get_latency_bucket(100), 100);
        assert_eq!(decomposer.get_latency_bucket(10000), 5000);
    }
}

