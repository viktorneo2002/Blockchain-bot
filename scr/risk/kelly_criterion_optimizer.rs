use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::native_token::LAMPORTS_PER_SOL;
use serde::{Deserialize, Serialize};
use dashmap::DashMap;

const MAX_KELLY_FRACTION: f64 = 0.25;
const MIN_KELLY_FRACTION: f64 = 0.001;
const CONFIDENCE_THRESHOLD: f64 = 0.95;
const MIN_SAMPLE_SIZE: usize = 100;
const MAX_HISTORY_SIZE: usize = 10000;
const DECAY_FACTOR: f64 = 0.995;
const VOLATILITY_ADJUSTMENT_FACTOR: f64 = 0.85;
const MAX_DRAWDOWN_LIMIT: f64 = 0.20;
const SHARPE_RATIO_TARGET: f64 = 3.0;
const MIN_WIN_RATE: f64 = 0.51;
const RISK_FREE_RATE: f64 = 0.05 / 365.0 / 24.0 / 3600.0;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeOutcome {
    pub timestamp: u64,
    pub profit_lamports: i64,
    pub risk_lamports: u64,
    pub opportunity_type: OpportunityType,
    pub confidence_score: f64,
    pub slippage_bps: u16,
    pub gas_cost_lamports: u64,
    pub execution_time_ms: u64,
    pub market_volatility: f64,
    pub competition_level: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OpportunityType {
    Arbitrage,
    Liquidation,
    JitLiquidity,
    Sandwich,
    FlashLoan,
}

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub win_rate: f64,
    pub profit_factor: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub avg_profit: f64,
    pub avg_loss: f64,
    pub total_trades: u64,
    pub winning_trades: u64,
    pub losing_trades: u64,
    pub current_streak: i32,
    pub volatility: f64,
    pub sortino_ratio: f64,
    pub calmar_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct KellyParameters {
    pub kelly_fraction: f64,
    pub adjusted_fraction: f64,
    pub confidence_level: f64,
    pub expected_value: f64,
    pub variance: f64,
    pub max_position_size: u64,
    pub min_position_size: u64,
    pub risk_adjustment_factor: f64,
}

pub struct KellyCriterionOptimizer {
    trade_history: Arc<RwLock<VecDeque<TradeOutcome>>>,
    opportunity_metrics: Arc<DashMap<OpportunityType, OpportunityMetrics>>,
    global_metrics: Arc<RwLock<GlobalMetrics>>,
    risk_parameters: Arc<RwLock<RiskParameters>>,
    market_conditions: Arc<RwLock<MarketConditions>>,
}

struct OpportunityMetrics {
    outcomes: VecDeque<TradeOutcome>,
    win_rate: f64,
    profit_factor: f64,
    avg_profit: f64,
    avg_loss: f64,
    volatility: f64,
    last_updated: Instant,
    confidence_score: f64,
    execution_success_rate: f64,
}

struct GlobalMetrics {
    total_capital: u64,
    available_capital: u64,
    current_exposure: u64,
    peak_capital: u64,
    trough_capital: u64,
    last_peak_time: Instant,
    cumulative_pnl: i64,
    daily_pnl: i64,
    hourly_pnl: i64,
    position_count: u32,
}

#[derive(Clone)]
struct RiskParameters {
    max_single_position_pct: f64,
    max_total_exposure_pct: f64,
    max_correlated_exposure_pct: f64,
    stop_loss_pct: f64,
    trailing_stop_pct: f64,
    volatility_scalar: f64,
    regime_adjustment: f64,
}

struct MarketConditions {
    volatility_index: f64,
    liquidity_score: f64,
    competition_index: f64,
    network_congestion: f64,
    regime_type: MarketRegime,
    trend_strength: f64,
    correlation_matrix: Vec<Vec<f64>>,
}

#[derive(Debug, Clone, Copy)]
enum MarketRegime {
    Bull,
    Bear,
    Sideways,
    HighVolatility,
    LowVolatility,
}

impl KellyCriterionOptimizer {
    pub fn new(initial_capital: u64) -> Self {
        Self {
            trade_history: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_HISTORY_SIZE))),
            opportunity_metrics: Arc::new(DashMap::new()),
            global_metrics: Arc::new(RwLock::new(GlobalMetrics {
                total_capital: initial_capital,
                available_capital: initial_capital,
                current_exposure: 0,
                peak_capital: initial_capital,
                trough_capital: initial_capital,
                last_peak_time: Instant::now(),
                cumulative_pnl: 0,
                daily_pnl: 0,
                hourly_pnl: 0,
                position_count: 0,
            })),
            risk_parameters: Arc::new(RwLock::new(RiskParameters {
                max_single_position_pct: 0.10,
                max_total_exposure_pct: 0.40,
                max_correlated_exposure_pct: 0.25,
                stop_loss_pct: 0.02,
                trailing_stop_pct: 0.015,
                volatility_scalar: 1.0,
                regime_adjustment: 1.0,
            })),
            market_conditions: Arc::new(RwLock::new(MarketConditions {
                volatility_index: 0.5,
                liquidity_score: 0.8,
                competition_index: 0.6,
                network_congestion: 0.3,
                regime_type: MarketRegime::Sideways,
                trend_strength: 0.0,
                correlation_matrix: vec![vec![1.0]],
            })),
        }
    }

    pub fn calculate_optimal_position(
        &self,
        opportunity_type: OpportunityType,
        expected_profit: u64,
        risk_amount: u64,
        confidence_score: f64,
        market_data: &MarketData,
    ) -> Result<KellyParameters, OptimizationError> {
        let metrics = self.get_opportunity_metrics(opportunity_type)?;
        let global = self.global_metrics.read().unwrap();
        let risk_params = self.risk_parameters.read().unwrap();
        let market = self.market_conditions.read().unwrap();

        if metrics.outcomes.len() < MIN_SAMPLE_SIZE {
            return Ok(self.conservative_position(&global, risk_amount));
        }

        let win_rate = self.calculate_bayesian_win_rate(&metrics, confidence_score);
        let profit_factor = self.calculate_profit_factor(&metrics);
        
        if win_rate < MIN_WIN_RATE || profit_factor < 1.1 {
            return Ok(KellyParameters {
                kelly_fraction: 0.0,
                adjusted_fraction: 0.0,
                confidence_level: 0.0,
                expected_value: 0.0,
                variance: 0.0,
                max_position_size: 0,
                min_position_size: 0,
                risk_adjustment_factor: 0.0,
            });
        }

        let b = (expected_profit as f64) / (risk_amount as f64);
        let p = win_rate;
        let q = 1.0 - p;
        
        let kelly_fraction = (b * p - q) / b;
        let kelly_fraction = kelly_fraction.max(0.0).min(MAX_KELLY_FRACTION);

        let variance = self.calculate_outcome_variance(&metrics);
        let volatility_adjustment = (-variance.sqrt() * VOLATILITY_ADJUSTMENT_FACTOR).exp();
        
        let regime_adjustment = self.get_regime_adjustment(&market.regime_type);
        let drawdown_adjustment = self.calculate_drawdown_adjustment(&global);
        let correlation_adjustment = self.calculate_correlation_adjustment(opportunity_type);
        
        let adjusted_fraction = kelly_fraction
            * volatility_adjustment
            * regime_adjustment
            * drawdown_adjustment
            * correlation_adjustment
            * confidence_score
            * market.liquidity_score
            * (1.0 - market.competition_index * 0.5)
            * (1.0 - market.network_congestion * 0.3);

        let adjusted_fraction = adjusted_fraction.max(MIN_KELLY_FRACTION).min(MAX_KELLY_FRACTION);

        let max_position_size = self.calculate_max_position_size(
            &global,
            &risk_params,
            adjusted_fraction,
            risk_amount,
        );

        let min_position_size = self.calculate_min_position_size(
            expected_profit,
            market_data.gas_estimate,
            market_data.priority_fee,
        );

        Ok(KellyParameters {
            kelly_fraction,
            adjusted_fraction,
            confidence_level: self.calculate_confidence_level(&metrics, win_rate, profit_factor),
            expected_value: p * (expected_profit as f64) - q * (risk_amount as f64),
            variance,
            max_position_size,
            min_position_size,
            risk_adjustment_factor: volatility_adjustment * regime_adjustment,
        })
    }

    pub fn record_trade_outcome(&self, outcome: TradeOutcome) -> Result<(), OptimizationError> {
        let mut history = self.trade_history.write().unwrap();
        if history.len() >= MAX_HISTORY_SIZE {
            history.pop_front();
        }
        history.push_back(outcome.clone());
        drop(history);

        self.update_opportunity_metrics(outcome.opportunity_type, &outcome)?;
        self.update_global_metrics(&outcome)?;
        self.update_market_conditions(&outcome)?;
        
        Ok(())
    }

    pub fn get_performance_metrics(&self) -> PerformanceMetrics {
        let history = self.trade_history.read().unwrap();
        if history.is_empty() {
            return PerformanceMetrics {
                win_rate: 0.0,
                profit_factor: 0.0,
                sharpe_ratio: 0.0,
                max_drawdown: 0.0,
                avg_profit: 0.0,
                avg_loss: 0.0,
                total_trades: 0,
                winning_trades: 0,
                losing_trades: 0,
                current_streak: 0,
                volatility: 0.0,
                sortino_ratio: 0.0,
                calmar_ratio: 0.0,
            };
        }

        let mut profits = Vec::new();
        let mut losses = Vec::new();
        let mut returns = Vec::new();
        let mut current_streak = 0;
        let mut peak = 0i64;
        let mut max_drawdown = 0f64;
        let mut cumulative = 0i64;

        for outcome in history.iter() {
            let net_profit = outcome.profit_lamports - outcome.gas_cost_lamports as i64;
            returns.push(net_profit as f64 / outcome.risk_lamports as f64);
            
            if net_profit > 0 {
                profits.push(net_profit as f64);
                current_streak = if current_streak >= 0 { current_streak + 1 } else { 1 };
            } else {
                losses.push(net_profit.abs() as f64);
                current_streak = if current_streak <= 0 { current_streak - 1 } else { -1 };
            }

            cumulative += net_profit;
            peak = peak.max(cumulative);
            let drawdown = if peak > 0 {
                (peak - cumulative) as f64 / peak as f64
            } else {
                0.0
            };
            max_drawdown = max_drawdown.max(drawdown);
        }

        let winning_trades = profits.len() as u64;
        let losing_trades = losses.len() as u64;
        let total_trades = winning_trades + losing_trades;
        let win_rate = if total_trades > 0 {
            winning_trades as f64 / total_trades as f64
        } else {
            0.0
        };

        let avg_profit = if !profits.is_empty() {
            profits.iter().sum::<f64>() / profits.len() as f64
        } else {
            0.0
        };

        let avg_loss = if !losses.is_empty() {
            losses.iter().sum::<f64>() / losses.len() as f64
        } else {
            0.0
        };

        let profit_factor = if avg_loss > 0.0 {
            (avg_profit * winning_trades as f64) / (avg_loss * losing_trades as f64)
        } else {
            f64::INFINITY
        };

        let (sharpe_ratio, sortino_ratio, volatility) = self.calculate_risk_metrics(&returns);
        let calmar_ratio = if max_drawdown > 0.0 {
                    let (sharpe_ratio, sortino_ratio, volatility) = self.calculate_risk_metrics(&returns);
        let calmar_ratio = if max_drawdown > 0.0 {
            (cumulative as f64 / LAMPORTS_PER_SOL as f64) / max_drawdown
        } else {
            0.0
        };

        PerformanceMetrics {
            win_rate,
            profit_factor,
            sharpe_ratio,
            max_drawdown,
            avg_profit,
            avg_loss,
            total_trades,
            winning_trades,
            losing_trades,
            current_streak,
            volatility,
            sortino_ratio,
            calmar_ratio,
        }
    }

    fn calculate_bayesian_win_rate(&self, metrics: &OpportunityMetrics, confidence_score: f64) -> f64 {
        let alpha = metrics.outcomes.iter().filter(|o| o.profit_lamports > o.gas_cost_lamports as i64).count() as f64 + 1.0;
        let beta = metrics.outcomes.iter().filter(|o| o.profit_lamports <= o.gas_cost_lamports as i64).count() as f64 + 1.0;
        
        let base_win_rate = alpha / (alpha + beta);
        let confidence_weight = (metrics.outcomes.len() as f64 / MIN_SAMPLE_SIZE as f64).min(1.0);
        
        base_win_rate * confidence_weight * confidence_score + (1.0 - confidence_weight) * 0.5
    }

    fn calculate_profit_factor(&self, metrics: &OpportunityMetrics) -> f64 {
        let mut gross_profit = 0.0;
        let mut gross_loss = 0.0;
        
        for outcome in &metrics.outcomes {
            let net_pnl = outcome.profit_lamports - outcome.gas_cost_lamports as i64;
            if net_pnl > 0 {
                gross_profit += net_pnl as f64;
            } else {
                gross_loss += net_pnl.abs() as f64;
            }
        }
        
        if gross_loss > 0.0 {
            gross_profit / gross_loss
        } else if gross_profit > 0.0 {
            f64::INFINITY
        } else {
            0.0
        }
    }

    fn calculate_outcome_variance(&self, metrics: &OpportunityMetrics) -> f64 {
        if metrics.outcomes.len() < 2 {
            return 1.0;
        }
        
        let returns: Vec<f64> = metrics.outcomes.iter()
            .map(|o| (o.profit_lamports - o.gas_cost_lamports as i64) as f64 / o.risk_lamports as f64)
            .collect();
        
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / (returns.len() - 1) as f64;
        
        variance.max(0.0001)
    }

    fn get_regime_adjustment(&self, regime: &MarketRegime) -> f64 {
        match regime {
            MarketRegime::Bull => 1.15,
            MarketRegime::Bear => 0.65,
            MarketRegime::Sideways => 0.90,
            MarketRegime::HighVolatility => 0.50,
            MarketRegime::LowVolatility => 1.10,
        }
    }

    fn calculate_drawdown_adjustment(&self, global: &GlobalMetrics) -> f64 {
        let current_drawdown = if global.peak_capital > 0 {
            (global.peak_capital as f64 - global.total_capital as f64) / global.peak_capital as f64
        } else {
            0.0
        };
        
        if current_drawdown > MAX_DRAWDOWN_LIMIT * 0.8 {
            0.25
        } else if current_drawdown > MAX_DRAWDOWN_LIMIT * 0.6 {
            0.50
        } else if current_drawdown > MAX_DRAWDOWN_LIMIT * 0.4 {
            0.75
        } else {
            1.0
        }
    }

    fn calculate_correlation_adjustment(&self, opportunity_type: OpportunityType) -> f64 {
        let active_positions = self.get_active_position_types();
        let correlation = match opportunity_type {
            OpportunityType::Arbitrage => {
                if active_positions.contains(&OpportunityType::Arbitrage) { 0.8 } else { 1.0 }
            },
            OpportunityType::Liquidation => {
                if active_positions.contains(&OpportunityType::Sandwich) { 0.6 } else { 1.0 }
            },
            OpportunityType::JitLiquidity => {
                if active_positions.contains(&OpportunityType::Arbitrage) { 0.7 } else { 1.0 }
            },
            OpportunityType::Sandwich => {
                if active_positions.contains(&OpportunityType::Liquidation) { 0.6 } else { 1.0 }
            },
            OpportunityType::FlashLoan => {
                if active_positions.len() > 2 { 0.5 } else { 1.0 }
            },
        };
        
        correlation
    }

    fn calculate_max_position_size(
        &self,
        global: &GlobalMetrics,
        risk_params: &RiskParameters,
        adjusted_fraction: f64,
        risk_amount: u64,
    ) -> u64 {
        let kelly_based_size = (global.available_capital as f64 * adjusted_fraction) as u64;
        let risk_based_size = (global.total_capital as f64 * risk_params.max_single_position_pct) as u64;
        let exposure_limit = (global.total_capital as f64 * risk_params.max_total_exposure_pct 
            - global.current_exposure as f64).max(0.0) as u64;
        
        kelly_based_size.min(risk_based_size).min(exposure_limit).min(global.available_capital)
    }

    fn calculate_min_position_size(
        &self,
        expected_profit: u64,
        gas_estimate: u64,
        priority_fee: u64,
    ) -> u64 {
        let total_cost = gas_estimate + priority_fee;
        let min_profit_multiple = 3.0;
        
        ((total_cost as f64 * min_profit_multiple / (expected_profit as f64 / 100.0)).ceil() as u64)
            .max(LAMPORTS_PER_SOL / 100)
    }

    fn conservative_position(&self, global: &GlobalMetrics, risk_amount: u64) -> KellyParameters {
        let conservative_fraction = 0.01;
        let max_position = (global.available_capital as f64 * conservative_fraction) as u64;
        
        KellyParameters {
            kelly_fraction: conservative_fraction,
            adjusted_fraction: conservative_fraction,
            confidence_level: 0.5,
            expected_value: 0.0,
            variance: 1.0,
            max_position_size: max_position.min(risk_amount * 2),
            min_position_size: LAMPORTS_PER_SOL / 100,
            risk_adjustment_factor: 0.5,
        }
    }

    fn get_opportunity_metrics(&self, opportunity_type: OpportunityType) -> Result<OpportunityMetrics, OptimizationError> {
        self.opportunity_metrics
            .get(&opportunity_type)
            .map(|entry| entry.clone())
            .ok_or(OptimizationError::NoHistoricalData)
    }

    fn update_opportunity_metrics(&self, opportunity_type: OpportunityType, outcome: &TradeOutcome) -> Result<(), OptimizationError> {
        self.opportunity_metrics.entry(opportunity_type).and_modify(|metrics| {
            if metrics.outcomes.len() >= MAX_HISTORY_SIZE / 5 {
                metrics.outcomes.pop_front();
            }
            metrics.outcomes.push_back(outcome.clone());
            
            let recent_outcomes: Vec<&TradeOutcome> = metrics.outcomes.iter()
                .rev()
                .take(MIN_SAMPLE_SIZE)
                .collect();
            
            let wins = recent_outcomes.iter()
                .filter(|o| o.profit_lamports > o.gas_cost_lamports as i64)
                .count();
            
            metrics.win_rate = wins as f64 / recent_outcomes.len().max(1) as f64;
            
            let mut profits = 0.0;
            let mut losses = 0.0;
            let mut profit_count = 0;
            let mut loss_count = 0;
            
            for o in &recent_outcomes {
                let net = o.profit_lamports - o.gas_cost_lamports as i64;
                if net > 0 {
                    profits += net as f64;
                    profit_count += 1;
                } else {
                    losses += net.abs() as f64;
                    loss_count += 1;
                }
            }
            
            metrics.avg_profit = if profit_count > 0 { profits / profit_count as f64 } else { 0.0 };
            metrics.avg_loss = if loss_count > 0 { losses / loss_count as f64 } else { 0.0 };
            metrics.profit_factor = if losses > 0.0 { profits / losses } else { f64::INFINITY };
            
            let returns: Vec<f64> = recent_outcomes.iter()
                .map(|o| (o.profit_lamports - o.gas_cost_lamports as i64) as f64 / o.risk_lamports as f64)
                .collect();
            
            if returns.len() > 1 {
                let mean = returns.iter().sum::<f64>() / returns.len() as f64;
                metrics.volatility = (returns.iter()
                    .map(|r| (r - mean).powi(2))
                    .sum::<f64>() / (returns.len() - 1) as f64)
                    .sqrt();
            }
            
            metrics.confidence_score = (metrics.win_rate * 2.0 - 1.0).max(0.0) 
                * (1.0 / (1.0 + metrics.volatility));
            metrics.execution_success_rate = recent_outcomes.iter()
                .filter(|o| o.execution_time_ms < 100)
                .count() as f64 / recent_outcomes.len() as f64;
            
            metrics.last_updated = Instant::now();
        }).or_insert_with(|| {
            let mut outcomes = VecDeque::with_capacity(MAX_HISTORY_SIZE / 5);
            outcomes.push_back(outcome.clone());
            
            OpportunityMetrics {
                outcomes,
                win_rate: if outcome.profit_lamports > outcome.gas_cost_lamports as i64 { 1.0 } else { 0.0 },
                profit_factor: 1.0,
                avg_profit: outcome.profit_lamports.max(0) as f64,
                avg_loss: outcome.profit_lamports.min(0).abs() as f64,
                volatility: 0.5,
                last_updated: Instant::now(),
                confidence_score: 0.5,
                execution_success_rate: 1.0,
            }
        });
        
        Ok(())
    }

    fn update_global_metrics(&self, outcome: &TradeOutcome) -> Result<(), OptimizationError> {
        let mut global = self.global_metrics.write().unwrap();
        
        let net_pnl = outcome.profit_lamports - outcome.gas_cost_lamports as i64;
        global.cumulative_pnl += net_pnl;
        global.daily_pnl += net_pnl;
        global.hourly_pnl += net_pnl;
        
        global.total_capital = (global.total_capital as i64 + net_pnl).max(0) as u64;
        
        if global.total_capital > global.peak_capital {
            global.peak_capital = global.total_capital;
            global.last_peak_time = Instant::now();
        }
        
        global.trough_capital = global.trough_capital.min(global.total_capital);
        
        if outcome.profit_lamports > 0 {
            global.current_exposure = global.current_exposure.saturating_sub(outcome.risk_lamports);
            global.available_capital = global.available_capital.saturating_add(outcome.risk_lamports);
        }
        
        Ok(())
    }

    fn update_market_conditions(&self, outcome: &TradeOutcome) -> Result<(), OptimizationError> {
        let mut market = self.market_conditions.write().unwrap();
        
        market.volatility_index = market.volatility_index * 0.99 + outcome.market_volatility * 0.01;
        market.competition_index = market.competition_index * 0.99 + outcome.competition_level * 0.01;
        
        if outcome.execution_time_ms > 200 || outcome.gas_cost_lamports > LAMPORTS_PER_SOL / 100 {
            market.network_congestion = (market.network_congestion * 0.95 + 0.05).min(1.0);
        } else {
            market.network_congestion = (market.network_congestion * 0.95).max(0.0);
        }
        
        market.regime_type = self.detect_market_regime(&market);
        
        Ok(())
    }

    fn detect_market_regime(&self, market: &MarketConditions) -> MarketRegime {
        if market.volatility_index > 0.8 {
            MarketRegime::HighVolatility
        } else if market.volatility_index < 0.2 {
            MarketRegime::LowVolatility
        } else if market.trend_strength > 0.6 {
            MarketRegime::Bull
        } else if market.trend_strength < -0.6 {
            MarketRegime::Bear
        } else {
            MarketRegime::Sideways
        }
    }

    fn calculate_risk_metrics(&self, returns: &[f64]) -> (f64, f64, f64) {
        if returns.len() < 2 {
            return (0.0, 0.0, 0.0);
        }
        
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
                        .sum::<f64>() / (returns.len() - 1) as f64;
        
        let volatility = variance.sqrt();
        let sharpe_ratio = if volatility > 0.0 {
            (mean - RISK_FREE_RATE) / volatility
        } else {
            0.0
        };
        
        let downside_returns: Vec<f64> = returns.iter()
            .filter(|&&r| r < 0.0)
            .copied()
            .collect();
        
        let sortino_ratio = if !downside_returns.is_empty() {
            let downside_variance = downside_returns.iter()
                .map(|r| r.powi(2))
                .sum::<f64>() / downside_returns.len() as f64;
            let downside_deviation = downside_variance.sqrt();
            
            if downside_deviation > 0.0 {
                (mean - RISK_FREE_RATE) / downside_deviation
            } else {
                0.0
            }
        } else {
            f64::INFINITY
        };
        
        (sharpe_ratio, sortino_ratio, volatility)
    }

    fn calculate_confidence_level(&self, metrics: &OpportunityMetrics, win_rate: f64, profit_factor: f64) -> f64 {
        let sample_confidence = (metrics.outcomes.len() as f64 / MIN_SAMPLE_SIZE as f64).min(1.0);
        let performance_confidence = ((win_rate - 0.5) * 2.0).max(0.0) * (profit_factor / 2.0).min(1.0);
        let execution_confidence = metrics.execution_success_rate;
        let recency_factor = 0.95_f64.powf(metrics.last_updated.elapsed().as_secs() as f64 / 3600.0);
        
        (sample_confidence * 0.3 + performance_confidence * 0.4 + execution_confidence * 0.2 + recency_factor * 0.1)
            .min(CONFIDENCE_THRESHOLD)
    }

    fn get_active_position_types(&self) -> Vec<OpportunityType> {
        let mut active_types = Vec::new();
        for entry in self.opportunity_metrics.iter() {
            if entry.value().last_updated.elapsed() < Duration::from_secs(300) {
                active_types.push(*entry.key());
            }
        }
        active_types
    }

    pub fn update_capital(&self, new_capital: u64) -> Result<(), OptimizationError> {
        let mut global = self.global_metrics.write().unwrap();
        global.total_capital = new_capital;
        global.available_capital = new_capital.saturating_sub(global.current_exposure);
        
        if new_capital > global.peak_capital {
            global.peak_capital = new_capital;
            global.last_peak_time = Instant::now();
        }
        
        Ok(())
    }

    pub fn add_exposure(&self, amount: u64) -> Result<(), OptimizationError> {
        let mut global = self.global_metrics.write().unwrap();
        
        if amount > global.available_capital {
            return Err(OptimizationError::InsufficientCapital);
        }
        
        global.current_exposure = global.current_exposure.saturating_add(amount);
        global.available_capital = global.available_capital.saturating_sub(amount);
        global.position_count += 1;
        
        Ok(())
    }

    pub fn remove_exposure(&self, amount: u64) -> Result<(), OptimizationError> {
        let mut global = self.global_metrics.write().unwrap();
        
        global.current_exposure = global.current_exposure.saturating_sub(amount);
        global.available_capital = global.available_capital.saturating_add(amount);
        global.position_count = global.position_count.saturating_sub(1);
        
        Ok(())
    }

    pub fn reset_daily_metrics(&self) {
        let mut global = self.global_metrics.write().unwrap();
        global.daily_pnl = 0;
    }

    pub fn reset_hourly_metrics(&self) {
        let mut global = self.global_metrics.write().unwrap();
        global.hourly_pnl = 0;
    }

    pub fn update_risk_parameters(&self, params: RiskParameters) -> Result<(), OptimizationError> {
        let mut risk_params = self.risk_parameters.write().unwrap();
        
        if params.max_single_position_pct > 0.25 || params.max_single_position_pct < 0.001 {
            return Err(OptimizationError::InvalidParameters);
        }
        
        if params.max_total_exposure_pct > 0.8 || params.max_total_exposure_pct < 0.1 {
            return Err(OptimizationError::InvalidParameters);
        }
        
        *risk_params = params;
        Ok(())
    }

    pub fn get_current_exposure(&self) -> u64 {
        self.global_metrics.read().unwrap().current_exposure
    }

    pub fn get_available_capital(&self) -> u64 {
        self.global_metrics.read().unwrap().available_capital
    }

    pub fn get_total_capital(&self) -> u64 {
        self.global_metrics.read().unwrap().total_capital
    }

    pub fn should_halt_trading(&self) -> bool {
        let global = self.global_metrics.read().unwrap();
        let metrics = self.get_performance_metrics();
        
        let drawdown = if global.peak_capital > 0 {
            (global.peak_capital as f64 - global.total_capital as f64) / global.peak_capital as f64
        } else {
            0.0
        };
        
        drawdown > MAX_DRAWDOWN_LIMIT || 
        global.available_capital < LAMPORTS_PER_SOL ||
        (metrics.current_streak < -5 && metrics.losing_trades > 5) ||
        global.hourly_pnl < -(global.total_capital as i64 / 20)
    }

    pub fn get_opportunity_confidence(&self, opportunity_type: OpportunityType) -> f64 {
        self.opportunity_metrics
            .get(&opportunity_type)
            .map(|entry| entry.confidence_score)
            .unwrap_or(0.0)
    }

    pub fn cleanup_old_data(&self) {
        let cutoff_time = Instant::now() - Duration::from_secs(86400);
        
        self.opportunity_metrics.retain(|_, metrics| {
            metrics.last_updated > cutoff_time
        });
        
        let mut history = self.trade_history.write().unwrap();
        let current_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        history.retain(|outcome| {
            current_timestamp - outcome.timestamp < 86400
        });
    }
}

#[derive(Debug, Clone)]
pub struct MarketData {
    pub gas_estimate: u64,
    pub priority_fee: u64,
    pub current_slot: u64,
    pub recent_blockhash: [u8; 32],
}

#[derive(Debug, thiserror::Error)]
pub enum OptimizationError {
    #[error("No historical data available")]
    NoHistoricalData,
    
    #[error("Insufficient capital")]
    InsufficientCapital,
    
    #[error("Invalid parameters")]
    InvalidParameters,
    
    #[error("Risk limit exceeded")]
    RiskLimitExceeded,
}

impl Default for RiskParameters {
    fn default() -> Self {
        Self {
            max_single_position_pct: 0.10,
            max_total_exposure_pct: 0.40,
            max_correlated_exposure_pct: 0.25,
            stop_loss_pct: 0.02,
            trailing_stop_pct: 0.015,
            volatility_scalar: 1.0,
            regime_adjustment: 1.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kelly_calculation() {
        let optimizer = KellyCriterionOptimizer::new(100 * LAMPORTS_PER_SOL);
        
        for _ in 0..150 {
            let outcome = TradeOutcome {
                timestamp: 0,
                profit_lamports: 1_000_000,
                risk_lamports: 10_000_000,
                opportunity_type: OpportunityType::Arbitrage,
                confidence_score: 0.8,
                slippage_bps: 10,
                gas_cost_lamports: 100_000,
                execution_time_ms: 50,
                market_volatility: 0.3,
                competition_level: 0.4,
            };
            optimizer.record_trade_outcome(outcome).unwrap();
        }
        
        let market_data = MarketData {
            gas_estimate: 100_000,
            priority_fee: 10_000,
            current_slot: 1000,
            recent_blockhash: [0; 32],
        };
        
        let params = optimizer.calculate_optimal_position(
            OpportunityType::Arbitrage,
            2_000_000,
            10_000_000,
            0.9,
            &market_data,
        ).unwrap();
        
        assert!(params.kelly_fraction > 0.0);
        assert!(params.kelly_fraction <= MAX_KELLY_FRACTION);
        assert!(params.adjusted_fraction <= params.kelly_fraction);
    }
}

