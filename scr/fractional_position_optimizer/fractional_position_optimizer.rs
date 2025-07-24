use anchor_lang::prelude::*;
use solana_sdk::{
    pubkey::Pubkey,
    clock::Clock,
    sysvar::clock,
};
use std::{
    collections::VecDeque,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use arrayref::{array_ref, array_refs};
use borsh::{BorshDeserialize, BorshSerialize};
use fixed::types::I80F48;

const KELLY_FRACTION: f64 = 0.25;
const MIN_POSITION_SIZE: u64 = 100_000;
const MAX_POSITION_RATIO: f64 = 0.15;
const VOLATILITY_WINDOW: usize = 120;
const CONFIDENCE_THRESHOLD: f64 = 0.68;
const SLIPPAGE_BUFFER: f64 = 0.003;
const GAS_SAFETY_MULTIPLIER: f64 = 1.5;
const DECAY_FACTOR: f64 = 0.995;
const MIN_LIQUIDITY_RATIO: f64 = 0.02;
const MAX_LEVERAGE: f64 = 3.0;
const RISK_FREE_RATE: f64 = 0.02;
const POSITION_SCALE_FACTOR: f64 = 0.8;

#[derive(Debug, Clone, Copy, BorshSerialize, BorshDeserialize)]
pub struct PositionMetrics {
    pub win_rate: f64,
    pub avg_profit: f64,
    pub avg_loss: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub current_streak: i32,
    pub total_trades: u64,
    pub last_update: i64,
}

#[derive(Debug, Clone)]
pub struct MarketConditions {
    pub volatility: f64,
    pub liquidity_depth: u64,
    pub spread_bps: f64,
    pub priority_fee: u64,
    pub network_congestion: f64,
    pub oracle_confidence: f64,
    pub recent_slippage: f64,
}

#[derive(Debug, Clone)]
pub struct OptimizedPosition {
    pub size: u64,
    pub leverage: f64,
    pub stop_loss: f64,
    pub take_profit: f64,
    pub confidence: f64,
    pub expected_value: f64,
    pub risk_adjusted_size: u64,
    pub max_gas_price: u64,
}

pub struct FractionalPositionOptimizer {
    capital_base: Arc<RwLock<u64>>,
    position_metrics: Arc<RwLock<PositionMetrics>>,
    volatility_buffer: Arc<RwLock<VecDeque<f64>>>,
    recent_trades: Arc<RwLock<VecDeque<TradeResult>>>,
    risk_parameters: RiskParameters,
    performance_tracker: PerformanceTracker,
}

#[derive(Debug, Clone)]
struct RiskParameters {
    max_position_pct: f64,
    vol_adjustment_factor: f64,
    streak_penalty: f64,
    congestion_discount: f64,
    confidence_weight: f64,
}

#[derive(Debug, Clone)]
struct TradeResult {
    pnl: f64,
    size: u64,
    timestamp: i64,
    slippage: f64,
    gas_cost: u64,
}

struct PerformanceTracker {
    rolling_sharpe: f64,
    rolling_sortino: f64,
    calmar_ratio: f64,
    omega_ratio: f64,
    tail_ratio: f64,
}

impl FractionalPositionOptimizer {
    pub fn new(initial_capital: u64) -> Self {
        Self {
            capital_base: Arc::new(RwLock::new(initial_capital)),
            position_metrics: Arc::new(RwLock::new(PositionMetrics {
                win_rate: 0.5,
                avg_profit: 0.0,
                avg_loss: 0.0,
                sharpe_ratio: 0.0,
                max_drawdown: 0.0,
                current_streak: 0,
                total_trades: 0,
                last_update: 0,
            })),
            volatility_buffer: Arc::new(RwLock::new(VecDeque::with_capacity(VOLATILITY_WINDOW))),
            recent_trades: Arc::new(RwLock::new(VecDeque::with_capacity(1000))),
            risk_parameters: RiskParameters {
                max_position_pct: MAX_POSITION_RATIO,
                vol_adjustment_factor: 2.0,
                streak_penalty: 0.05,
                congestion_discount: 0.3,
                confidence_weight: 1.5,
            },
            performance_tracker: PerformanceTracker {
                rolling_sharpe: 0.0,
                rolling_sortino: 0.0,
                calmar_ratio: 0.0,
                omega_ratio: 1.0,
                tail_ratio: 1.0,
            },
        }
    }

    pub fn calculate_optimal_position(
        &self,
        opportunity_value: u64,
        market_conditions: &MarketConditions,
        timestamp: i64,
    ) -> OptimizedPosition {
        let capital = *self.capital_base.read().unwrap();
        let metrics = self.position_metrics.read().unwrap().clone();
        
        let kelly_size = self.calculate_kelly_position(&metrics, opportunity_value);
        let vol_adjusted_size = self.adjust_for_volatility(kelly_size, market_conditions);
        let risk_adjusted_size = self.apply_risk_constraints(vol_adjusted_size, capital, &metrics);
        
        let liquidity_constrained = self.constrain_by_liquidity(
            risk_adjusted_size,
            market_conditions.liquidity_depth,
        );
        
        let network_adjusted = self.adjust_for_network_conditions(
            liquidity_constrained,
            market_conditions,
        );
        
        let final_size = self.apply_dynamic_scaling(
            network_adjusted,
            &metrics,
            market_conditions,
            timestamp,
        );
        
        let leverage = self.calculate_optimal_leverage(&metrics, market_conditions);
        let stops = self.calculate_stop_levels(&metrics, market_conditions, final_size);
        
        OptimizedPosition {
            size: final_size,
            leverage,
            stop_loss: stops.0,
            take_profit: stops.1,
            confidence: self.calculate_position_confidence(&metrics, market_conditions),
            expected_value: self.calculate_expected_value(final_size, &metrics, market_conditions),
            risk_adjusted_size: final_size,
            max_gas_price: self.calculate_max_gas(final_size, market_conditions),
        }
    }

    fn calculate_kelly_position(&self, metrics: &PositionMetrics, opportunity_value: u64) -> u64 {
        if metrics.total_trades < 20 || metrics.avg_loss == 0.0 {
            return (opportunity_value as f64 * 0.01) as u64;
        }
        
        let win_prob = metrics.win_rate.max(0.0).min(1.0);
        let loss_prob = 1.0 - win_prob;
        
        let avg_win = metrics.avg_profit.max(0.0);
        let avg_loss = metrics.avg_loss.abs().max(0.001);
        
        let odds = avg_win / avg_loss;
        let kelly_pct = (win_prob * odds - loss_prob) / odds;
        
        let adjusted_kelly = kelly_pct * KELLY_FRACTION * self.streak_adjustment(metrics);
        let size = (opportunity_value as f64 * adjusted_kelly.max(0.0).min(0.25)) as u64;
        
        size.max(MIN_POSITION_SIZE)
    }

    fn streak_adjustment(&self, metrics: &PositionMetrics) -> f64 {
        let streak_factor = match metrics.current_streak {
            s if s >= 5 => 1.2,
            s if s >= 3 => 1.1,
            s if s <= -5 => 0.5,
            s if s <= -3 => 0.7,
            _ => 1.0,
        };
        
        let sharpe_factor = if metrics.sharpe_ratio > 2.0 {
            1.15
        } else if metrics.sharpe_ratio > 1.0 {
            1.05
        } else if metrics.sharpe_ratio < 0.0 {
            0.8
        } else {
            1.0
        };
        
        streak_factor * sharpe_factor
    }

    fn adjust_for_volatility(&self, base_size: u64, market: &MarketConditions) -> u64 {
        let vol_buffer = self.volatility_buffer.read().unwrap();
        let recent_vol = if vol_buffer.len() >= 20 {
            let sum: f64 = vol_buffer.iter().take(20).sum();
            sum / 20.0
        } else {
            market.volatility
        };
        
        let vol_ratio = recent_vol / 0.02;
        let vol_multiplier = if vol_ratio > 2.0 {
            0.5
        } else if vol_ratio > 1.5 {
            0.7
        } else if vol_ratio < 0.5 {
            1.3
        } else {
            1.0
        };
        
        (base_size as f64 * vol_multiplier) as u64
    }

    fn apply_risk_constraints(&self, size: u64, capital: u64, metrics: &PositionMetrics) -> u64 {
        let max_position = (capital as f64 * self.risk_parameters.max_position_pct) as u64;
        
        let drawdown_adjusted = if metrics.max_drawdown > 0.1 {
            (max_position as f64 * (1.0 - metrics.max_drawdown * 2.0).max(0.3)) as u64
        } else {
            max_position
        };
        
        let risk_capacity = self.calculate_risk_capacity(capital, metrics);
        let risk_limited = size.min(risk_capacity);
        
        risk_limited.min(drawdown_adjusted)
    }

    fn calculate_risk_capacity(&self, capital: u64, metrics: &PositionMetrics) -> u64 {
        let var_95 = self.calculate_value_at_risk(capital, metrics, 0.95);
        let cvar_95 = var_95 * 1.2;
        
        let risk_budget = (capital as f64 * 0.02) as u64;
        let var_constrained = (capital as f64 - cvar_95).max(0.0) as u64;
        
        risk_budget.min(var_constrained)
    }

    fn calculate_value_at_risk(&self, capital: u64, metrics: &PositionMetrics, confidence: f64) -> f64 {
        let trades = self.recent_trades.read().unwrap();
        if trades.len() < 30 {
            return capital as f64 * 0.05;
        }
        
        let mut returns: Vec<f64> = trades.iter()
            .map(|t| t.pnl / t.size as f64)
            .collect();
        returns.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let index = ((1.0 - confidence) * returns.len() as f64) as usize;
        let var = returns[index].abs() * capital as f64;
        
        var
    }

    fn constrain_by_liquidity(&self, size: u64, liquidity: u64) -> u64 {
        let max_impact_size = (liquidity as f64 * MIN_LIQUIDITY_RATIO) as u64;
        size.min(max_impact_size)
    }

    fn adjust_for_network_conditions(&self, size: u64, market: &MarketConditions) -> u64 {
        let congestion_factor = 1.0 - (market.network_congestion * self.risk_parameters.congestion_discount);
        let priority_factor = if market.priority_fee > 50000 {
            0.8
        } else if market.priority_fee > 25000 {
            0.9
        } else {
            1.0
        };
        
        let confidence_factor = market.oracle_confidence.powf(self.risk_parameters.confidence_weight);
        
        let adjustment = congestion_factor * priority_factor * confidence_factor;
        (size as f64 * adjustment.max(0.3).min(1.0)) as u64
    }

    fn apply_dynamic_scaling(
        &self,
        size: u64,
        metrics: &PositionMetrics,
        market: &MarketConditions,
        timestamp: i64,
    ) -> u64 {
        let time_decay = if metrics.last_update > 0 {
            let elapsed = (timestamp - metrics.last_update) as f64 / 3600.0;
            DECAY_FACTOR.powf(elapsed)
        } else {
            1.0
        };
        
        let performance_scale = self.calculate_performance_scale(metrics);
        let market_scale = self.calculate_market_scale(market);
        
        let total_scale = time_decay * performance_scale * market_scale * POSITION_SCALE_FACTOR;
        (size as f64 * total_scale.max(0.1).min(1.5)) as u64
    }

    fn calculate_performance_scale(&self, metrics: &PositionMetrics) -> f64 {
        let sharpe_scale = (metrics.sharpe_ratio / 2.0).max(0.0).min(1.5);
        let win_rate_scale = (metrics.win_rate * 2.0).max(0.5).min(1.2);
        let drawdown_scale = (1.0 - metrics.max_drawdown).max(0.3);
        
        (sharpe_scale * win_rate_scale * drawdown_scale).powf(0.333)
    }

        fn calculate_market_scale(&self, market: &MarketConditions) -> f64 {
        let spread_scale = (10.0 / (market.spread_bps + 1.0)).min(1.0);
        let vol_scale = (0.02 / market.volatility.max(0.001)).min(1.2).max(0.5);
        let liquidity_scale = ((market.liquidity_depth as f64).ln() / 20.0).min(1.0).max(0.3);
        let slippage_scale = (1.0 - market.recent_slippage * 10.0).max(0.5);
        
        (spread_scale * vol_scale * liquidity_scale * slippage_scale).powf(0.25)
    }

    fn calculate_optimal_leverage(&self, metrics: &PositionMetrics, market: &MarketConditions) -> f64 {
        let base_leverage = if metrics.sharpe_ratio > 1.5 {
            2.0
        } else if metrics.sharpe_ratio > 1.0 {
            1.5
        } else if metrics.sharpe_ratio > 0.5 {
            1.2
        } else {
            1.0
        };
        
        let vol_adjustment = (0.02 / market.volatility.max(0.01)).min(1.5).max(0.5);
        let win_rate_adjustment = (metrics.win_rate * 2.0).min(1.3).max(0.7);
        let drawdown_penalty = (1.0 - metrics.max_drawdown * 2.0).max(0.5);
        
        let adjusted_leverage = base_leverage * vol_adjustment * win_rate_adjustment * drawdown_penalty;
        adjusted_leverage.min(MAX_LEVERAGE).max(1.0)
    }

    fn calculate_stop_levels(
        &self,
        metrics: &PositionMetrics,
        market: &MarketConditions,
        position_size: u64,
    ) -> (f64, f64) {
        let atr_estimate = market.volatility * 2.0;
        let spread_cost = market.spread_bps / 10000.0;
        
        let avg_loss_pct = if metrics.avg_loss != 0.0 {
            (metrics.avg_loss / position_size as f64).abs()
        } else {
            0.02
        };
        
        let stop_loss_distance = (atr_estimate * 1.5 + spread_cost)
            .max(avg_loss_pct * 0.7)
            .min(0.05);
        
        let risk_reward_ratio = if metrics.avg_profit > 0.0 && metrics.avg_loss < 0.0 {
            (metrics.avg_profit / metrics.avg_loss.abs()).max(1.5)
        } else {
            2.0
        };
        
        let take_profit_distance = stop_loss_distance * risk_reward_ratio;
        
        let slippage_adjusted_stop = stop_loss_distance * (1.0 + market.recent_slippage);
        let slippage_adjusted_tp = take_profit_distance * (1.0 - market.recent_slippage * 0.5);
        
        (slippage_adjusted_stop, slippage_adjusted_tp)
    }

    fn calculate_position_confidence(&self, metrics: &PositionMetrics, market: &MarketConditions) -> f64 {
        let win_rate_confidence = if metrics.total_trades > 100 {
            metrics.win_rate * 0.9 + 0.1
        } else if metrics.total_trades > 50 {
            metrics.win_rate * 0.7 + 0.15
        } else {
            0.5
        };
        
        let sharpe_confidence = (metrics.sharpe_ratio / 3.0).min(1.0).max(0.0);
        let oracle_confidence = market.oracle_confidence;
        
        let recency_weight = if metrics.total_trades > 0 {
            let recent_trades = self.recent_trades.read().unwrap();
            let recent_count = recent_trades.iter().take(20).count();
            (recent_count as f64 / 20.0).min(1.0)
        } else {
            0.0
        };
        
        let market_confidence = 1.0 - (market.network_congestion * 0.5 + market.volatility * 2.0).min(0.7);
        
        let weighted_confidence = win_rate_confidence * 0.3
            + sharpe_confidence * 0.25
            + oracle_confidence * 0.2
            + recency_weight * 0.15
            + market_confidence * 0.1;
        
        weighted_confidence.max(CONFIDENCE_THRESHOLD * 0.5).min(0.95)
    }

    fn calculate_expected_value(
        &self,
        size: u64,
        metrics: &PositionMetrics,
        market: &MarketConditions,
    ) -> f64 {
        let win_prob = metrics.win_rate;
        let loss_prob = 1.0 - win_prob;
        
        let expected_profit = if metrics.avg_profit > 0.0 {
            metrics.avg_profit * (1.0 - market.recent_slippage)
        } else {
            size as f64 * 0.01
        };
        
        let expected_loss = if metrics.avg_loss < 0.0 {
            metrics.avg_loss * (1.0 + market.recent_slippage)
        } else {
            -(size as f64 * 0.02)
        };
        
        let gas_cost = market.priority_fee as f64 * GAS_SAFETY_MULTIPLIER;
        let spread_cost = size as f64 * (market.spread_bps / 10000.0);
        
        let gross_ev = win_prob * expected_profit + loss_prob * expected_loss;
        let net_ev = gross_ev - gas_cost - spread_cost;
        
        net_ev * (1.0 - market.network_congestion * 0.2)
    }

    fn calculate_max_gas(&self, position_size: u64, market: &MarketConditions) -> u64 {
        let base_gas = market.priority_fee;
        let size_ratio = (position_size as f64 / 1_000_000.0).min(10.0);
        
        let urgency_multiplier = if market.network_congestion > 0.8 {
            2.5
        } else if market.network_congestion > 0.6 {
            2.0
        } else if market.network_congestion > 0.4 {
            1.5
        } else {
            1.2
        };
        
        let volatility_multiplier = (market.volatility * 50.0).max(1.0).min(2.0);
        
        let max_gas = (base_gas as f64 * urgency_multiplier * volatility_multiplier * size_ratio.sqrt()) as u64;
        
        let position_pct = position_size as f64 * 0.001;
        max_gas.min((position_pct * 0.01) as u64).max(5000)
    }

    pub fn update_trade_result(&self, result: TradeResult) {
        let mut trades = self.recent_trades.write().unwrap();
        trades.push_front(result.clone());
        if trades.len() > 1000 {
            trades.pop_back();
        }
        drop(trades);
        
        self.update_metrics(result);
        self.update_performance_tracker();
    }

    fn update_metrics(&self, result: TradeResult) {
        let mut metrics = self.position_metrics.write().unwrap();
        
        metrics.total_trades += 1;
        metrics.last_update = result.timestamp;
        
        let is_win = result.pnl > 0.0;
        
        if is_win {
            metrics.current_streak = metrics.current_streak.max(0) + 1;
            let alpha = 2.0 / (metrics.total_trades as f64 + 1.0).min(100.0);
            metrics.avg_profit = metrics.avg_profit * (1.0 - alpha) + result.pnl * alpha;
        } else {
            metrics.current_streak = metrics.current_streak.min(0) - 1;
            let alpha = 2.0 / (metrics.total_trades as f64 + 1.0).min(100.0);
            metrics.avg_loss = metrics.avg_loss * (1.0 - alpha) + result.pnl * alpha;
        }
        
        let win_count = self.recent_trades.read().unwrap()
            .iter()
            .take(100)
            .filter(|t| t.pnl > 0.0)
            .count();
        metrics.win_rate = win_count as f64 / 100.0.min(metrics.total_trades as f64);
        
        self.update_sharpe_ratio(&mut metrics);
        self.update_max_drawdown(&mut metrics);
    }

    fn update_sharpe_ratio(&self, metrics: &mut PositionMetrics) {
        let trades = self.recent_trades.read().unwrap();
        if trades.len() < 30 {
            return;
        }
        
        let returns: Vec<f64> = trades.iter()
            .take(252)
            .map(|t| t.pnl / t.size as f64)
            .collect();
        
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / returns.len() as f64;
        let std_dev = variance.sqrt();
        
        if std_dev > 0.0 {
            let annualized_return = mean_return * 252.0;
            let annualized_std = std_dev * 252.0_f64.sqrt();
            metrics.sharpe_ratio = (annualized_return - RISK_FREE_RATE) / annualized_std;
        }
    }

    fn update_max_drawdown(&self, metrics: &mut PositionMetrics) {
        let trades = self.recent_trades.read().unwrap();
        if trades.is_empty() {
            return;
        }
        
        let mut cumulative = 0.0;
        let mut peak = 0.0;
        let mut max_dd = 0.0;
        
        for trade in trades.iter().take(500) {
            cumulative += trade.pnl;
            if cumulative > peak {
                peak = cumulative;
            }
            let drawdown = if peak > 0.0 {
                (peak - cumulative) / peak
            } else {
                0.0
            };
            max_dd = max_dd.max(drawdown);
        }
        
        metrics.max_drawdown = max_dd;
    }

    fn update_performance_tracker(&self) {
        let trades = self.recent_trades.read().unwrap();
        if trades.len() < 50 {
            return;
        }
        
        let returns: Vec<f64> = trades.iter()
            .take(252)
            .map(|t| t.pnl / t.size as f64)
            .collect();
        
        self.performance_tracker.rolling_sharpe = self.calculate_rolling_sharpe(&returns);
        self.performance_tracker.rolling_sortino = self.calculate_sortino_ratio(&returns);
        self.performance_tracker.calmar_ratio = self.calculate_calmar_ratio(&returns);
        self.performance_tracker.omega_ratio = self.calculate_omega_ratio(&returns);
        self.performance_tracker.tail_ratio = self.calculate_tail_ratio(&returns);
    }

    fn calculate_rolling_sharpe(&self, returns: &[f64]) -> f64 {
        if returns.len() < 30 {
            return 0.0;
        }
        
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let std_dev = (returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64)
            .sqrt();
        
        if std_dev > 0.0 {
            (mean - RISK_FREE_RATE / 252.0) / std_dev * 252.0_f64.sqrt()
        } else {
            0.0
        }
    }

    fn calculate_sortino_ratio(&self, returns: &[f64]) -> f64 {
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let downside_returns: Vec<f64> = returns.iter()
            .filter(|&&r| r < 0.0)
            .copied()
            .collect();
        
        if downside_returns.is_empty() {
            return 3.0;
        }
        
        let downside_dev = (downside_returns.iter()
            .map(|r| r.powi(2))
            .sum::<f64>() / downside_returns.len() as f64)
            .sqrt();
        
        if downside_dev > 0.0 {
            (mean - RISK_FREE_RATE / 252.0) / downside_dev * 252.0_f64.sqrt()
        } else {
            3.0
        }
    }

    fn calculate_calmar_ratio(&self, returns: &[f64]) -> f64 {
        let annual_return = returns.iter().sum::<f64>() / returns.len() as f64 * 252.0;
        let metrics = self.position_metrics.read().unwrap();
        
        if metrics.max_drawdown > 0.0 {
            annual_return / metrics.max_drawdown
        } else {
            annual_return / 0.01
        }
    }

    fn calculate_omega_ratio(&self, returns: &[f64]) -> f64 {
        let threshold = RISK_FREE_RATE / 252.0;
        let gains: f64 = returns.iter()
            .filter(|&&r| r > threshold)
            .map(|&r| r - threshold)
            .sum();
        let losses: f64 = returns.iter()
            .filter(|&&r| r <= threshold)
            .map(|&r| threshold - r)
            .sum();
        
                if losses > 0.0 {
            gains / losses
        } else {
            gains / 0.001
        }
    }

    fn calculate_tail_ratio(&self, returns: &[f64]) -> f64 {
        let mut sorted_returns = returns.to_vec();
        sorted_returns.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let percentile_5 = sorted_returns.len() / 20;
        let percentile_95 = sorted_returns.len() * 19 / 20;
        
        if percentile_5 > 0 && percentile_95 < sorted_returns.len() {
            let left_tail = sorted_returns[..percentile_5].iter().sum::<f64>() / percentile_5 as f64;
            let right_tail = sorted_returns[percentile_95..].iter().sum::<f64>() / (sorted_returns.len() - percentile_95) as f64;
            
            if left_tail < 0.0 {
                right_tail / left_tail.abs()
            } else {
                right_tail / 0.001
            }
        } else {
            1.0
        }
    }

    pub fn update_volatility(&self, price_change: f64) {
        let mut vol_buffer = self.volatility_buffer.write().unwrap();
        vol_buffer.push_front(price_change.abs());
        if vol_buffer.len() > VOLATILITY_WINDOW {
            vol_buffer.pop_back();
        }
    }

    pub fn update_capital(&self, new_capital: u64) {
        let mut capital = self.capital_base.write().unwrap();
        *capital = new_capital;
    }

    pub fn get_current_metrics(&self) -> PositionMetrics {
        self.position_metrics.read().unwrap().clone()
    }

    pub fn get_performance_stats(&self) -> PerformanceStats {
        PerformanceStats {
            sharpe: self.performance_tracker.rolling_sharpe,
            sortino: self.performance_tracker.rolling_sortino,
            calmar: self.performance_tracker.calmar_ratio,
            omega: self.performance_tracker.omega_ratio,
            tail: self.performance_tracker.tail_ratio,
            total_trades: self.position_metrics.read().unwrap().total_trades,
            win_rate: self.position_metrics.read().unwrap().win_rate,
            avg_profit: self.position_metrics.read().unwrap().avg_profit,
            avg_loss: self.position_metrics.read().unwrap().avg_loss,
            max_drawdown: self.position_metrics.read().unwrap().max_drawdown,
        }
    }

    pub fn should_trade(&self, confidence: f64, expected_value: f64) -> bool {
        if confidence < CONFIDENCE_THRESHOLD {
            return false;
        }
        
        let metrics = self.position_metrics.read().unwrap();
        
        if metrics.current_streak <= -5 {
            return false;
        }
        
        if metrics.max_drawdown > 0.2 && metrics.current_streak < 0 {
            return false;
        }
        
        if expected_value <= 0.0 {
            return false;
        }
        
        let capital = *self.capital_base.read().unwrap();
        if capital < MIN_POSITION_SIZE * 10 {
            return false;
        }
        
        true
    }

    pub fn emergency_stop_check(&self) -> bool {
        let metrics = self.position_metrics.read().unwrap();
        
        if metrics.max_drawdown > 0.3 {
            return true;
        }
        
        if metrics.current_streak <= -8 {
            return true;
        }
        
        if metrics.sharpe_ratio < -1.0 && metrics.total_trades > 50 {
            return true;
        }
        
        let recent_trades = self.recent_trades.read().unwrap();
        if recent_trades.len() >= 10 {
            let recent_losses = recent_trades.iter()
                .take(10)
                .filter(|t| t.pnl < 0.0)
                .count();
            if recent_losses >= 8 {
                return true;
            }
        }
        
        false
    }

    pub fn get_risk_adjusted_size(&self, base_size: u64, urgency: f64) -> u64 {
        let metrics = self.position_metrics.read().unwrap();
        let capital = *self.capital_base.read().unwrap();
        
        let urgency_factor = urgency.max(0.5).min(2.0);
        let confidence_factor = if metrics.total_trades < 50 {
            0.5
        } else {
            1.0
        };
        
        let streak_factor = match metrics.current_streak {
            s if s >= 3 => 1.1,
            s if s <= -3 => 0.7,
            _ => 1.0,
        };
        
        let sharpe_factor = if metrics.sharpe_ratio > 1.5 {
            1.1
        } else if metrics.sharpe_ratio < 0.5 {
            0.8
        } else {
            1.0
        };
        
        let adjusted = (base_size as f64 * urgency_factor * confidence_factor * streak_factor * sharpe_factor) as u64;
        adjusted.min((capital as f64 * MAX_POSITION_RATIO) as u64).max(MIN_POSITION_SIZE)
    }

    pub fn calculate_adaptive_stops(&self, entry_price: f64, position_size: u64, side: TradeSide) -> (f64, f64) {
        let metrics = self.position_metrics.read().unwrap();
        let vol_buffer = self.volatility_buffer.read().unwrap();
        
        let current_vol = if vol_buffer.len() >= 10 {
            vol_buffer.iter().take(10).sum::<f64>() / 10.0
        } else {
            0.02
        };
        
        let base_stop_distance = current_vol * 2.0;
        let streak_adjustment = if metrics.current_streak < -2 {
            0.7
        } else if metrics.current_streak > 2 {
            1.3
        } else {
            1.0
        };
        
        let stop_distance = base_stop_distance * streak_adjustment;
        let profit_distance = stop_distance * 2.5;
        
        match side {
            TradeSide::Buy => (
                entry_price * (1.0 - stop_distance),
                entry_price * (1.0 + profit_distance)
            ),
            TradeSide::Sell => (
                entry_price * (1.0 + stop_distance),
                entry_price * (1.0 - profit_distance)
            ),
        }
    }

    pub fn optimize_for_market_conditions(&self, base_position: OptimizedPosition, market_state: MarketState) -> OptimizedPosition {
        let mut optimized = base_position;
        
        match market_state {
            MarketState::Trending => {
                optimized.size = (optimized.size as f64 * 1.2) as u64;
                optimized.leverage = (optimized.leverage * 1.1).min(MAX_LEVERAGE);
                optimized.take_profit *= 1.5;
            },
            MarketState::Ranging => {
                optimized.size = (optimized.size as f64 * 0.8) as u64;
                optimized.leverage = (optimized.leverage * 0.9).max(1.0);
                optimized.stop_loss *= 0.8;
                optimized.take_profit *= 0.7;
            },
            MarketState::Volatile => {
                optimized.size = (optimized.size as f64 * 0.6) as u64;
                optimized.leverage = 1.0;
                optimized.stop_loss *= 1.5;
                optimized.max_gas_price = (optimized.max_gas_price as f64 * 1.5) as u64;
            },
            MarketState::Illiquid => {
                optimized.size = (optimized.size as f64 * 0.4) as u64;
                optimized.leverage = 1.0;
                optimized.confidence *= 0.7;
            },
        }
        
        optimized
    }

    pub fn calculate_portfolio_heat(&self) -> f64 {
        let capital = *self.capital_base.read().unwrap();
        let metrics = self.position_metrics.read().unwrap();
        let recent_trades = self.recent_trades.read().unwrap();
        
        let open_risk = recent_trades.iter()
            .take(5)
            .map(|t| t.size as f64 / capital as f64)
            .sum::<f64>();
        
        let drawdown_heat = metrics.max_drawdown * 2.0;
        let streak_heat = if metrics.current_streak < 0 {
            metrics.current_streak.abs() as f64 * 0.05
        } else {
            0.0
        };
        
        (open_risk + drawdown_heat + streak_heat).min(1.0)
    }

    pub fn adjust_for_correlation(&self, position: OptimizedPosition, correlation: f64) -> OptimizedPosition {
        let mut adjusted = position;
        let correlation_factor = 1.0 - correlation.abs() * 0.5;
        
        adjusted.size = (adjusted.size as f64 * correlation_factor) as u64;
        adjusted.confidence *= correlation_factor;
        
        if correlation.abs() > 0.7 {
            adjusted.leverage = (adjusted.leverage * 0.8).max(1.0);
        }
        
        adjusted
    }

    pub fn calculate_time_decay_factor(&self, opportunity_age_ms: u64) -> f64 {
        let age_seconds = opportunity_age_ms as f64 / 1000.0;
        
        if age_seconds < 0.1 {
            1.0
        } else if age_seconds < 0.5 {
            0.95
        } else if age_seconds < 1.0 {
            0.8
        } else if age_seconds < 2.0 {
            0.6
        } else {
            0.3
        }
    }

    pub fn finalize_position(&self, mut position: OptimizedPosition) -> OptimizedPosition {
        let capital = *self.capital_base.read().unwrap();
        let metrics = self.position_metrics.read().unwrap();
        
        position.size = position.size.min((capital as f64 * 0.95) as u64);
        position.size = ((position.size / 1000) * 1000).max(MIN_POSITION_SIZE);
        
        position.stop_loss = (position.stop_loss * 10000.0).round() / 10000.0;
        position.take_profit = (position.take_profit * 10000.0).round() / 10000.0;
        
        if metrics.total_trades < 20 {
            position.size = (position.size as f64 * 0.5) as u64;
            position.leverage = 1.0;
        }
        
        position
    }
}

#[derive(Debug, Clone, Copy)]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy)]
pub enum MarketState {
    Trending,
    Ranging,
    Volatile,
    Illiquid,
}

#[derive(Debug, Clone)]
pub struct PerformanceStats {
    pub sharpe: f64,
    pub sortino: f64,
    pub calmar: f64,
    pub omega: f64,
    pub tail: f64,
    pub total_trades: u64,
    pub win_rate: f64,
    pub avg_profit: f64,
    pub avg_loss: f64,
    pub max_drawdown: f64,
}

impl Default for FractionalPositionOptimizer {
    fn default() -> Self {
        Self::new(1_000_000_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_optimizer_initialization() {
        let optimizer = FractionalPositionOptimizer::new(1_000_000_000);
        assert_eq!(*optimizer.capital_base.read().unwrap(), 1_000_000_000);
    }

    #[test]
    fn test_optimal_position_calculation() {
        let optimizer = FractionalPositionOptimizer::new(1_000_000_000);
        let market = MarketConditions {
            volatility: 0.02,
            liquidity_depth: 10_000_000,
            spread_bps: 5.0,
            priority_fee: 10_000,
            network_congestion: 0.3,
            oracle_confidence: 0.95,
            recent_slippage: 0.001,
        };
        
        let position = optimizer.calculate_optimal_position(1_000_000, &market, 1000);
        assert!(position.size >= MIN_POSITION_SIZE);
        assert!(position.leverage >= 1.0 && position.leverage <= MAX_LEVERAGE);
    }
}

