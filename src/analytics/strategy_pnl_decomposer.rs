use anchor_lang::prelude::*;
use solana_sdk::{
    clock::Clock,
    pubkey::Pubkey,
    signature::Signature,
};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, RwLock};
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

const MAX_HISTORY_SIZE: usize = 10000;
const PRECISION_FACTOR: u64 = 1_000_000_000;
const BASIS_POINTS: u64 = 10_000;
const MAX_SLIPPAGE_BPS: u64 = 300;
const GAS_PRICE_BUFFER: f64 = 1.15;
const ROLLING_WINDOW_SIZE: usize = 1000;

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PnLComponent {
    pub gross_revenue: i64,
    pub network_fees: i64,
    pub priority_fees: i64,
    pub slippage_cost: i64,
    pub opportunity_cost: i64,
    pub mev_extracted: i64,
    pub impermanent_loss: i64,
    pub timestamp: i64,
    pub slot: u64,
    pub strategy_id: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetailedPnL {
    pub total_pnl: i64,
    pub realized_pnl: i64,
    pub unrealized_pnl: i64,
    pub components: PnLComponent,
    pub roi_bps: i64,
    pub sharpe_ratio: f64,
    pub max_drawdown_bps: i64,
    pub win_rate: f64,
    pub profit_factor: f64,
}

#[derive(Debug, Clone)]
pub struct StrategyMetrics {
    pub total_trades: u64,
    pub winning_trades: u64,
    pub losing_trades: u64,
    pub avg_profit_per_trade: i64,
    pub avg_loss_per_trade: i64,
    pub largest_win: i64,
    pub largest_loss: i64,
    pub consecutive_wins: u32,
    pub consecutive_losses: u32,
    pub avg_holding_time_slots: u64,
    pub capital_efficiency: f64,
}

#[derive(Debug, Clone)]
pub struct RiskMetrics {
    pub var_95: i64,
    pub cvar_95: i64,
    pub beta: f64,
    pub correlation_to_market: f64,
    pub downside_deviation: f64,
    pub sortino_ratio: f64,
    pub calmar_ratio: f64,
    pub omega_ratio: f64,
}

pub struct PnLDecomposer {
    history: Arc<RwLock<VecDeque<DetailedPnL>>>,
    strategy_metrics: Arc<RwLock<HashMap<u32, StrategyMetrics>>>,
    risk_metrics: Arc<RwLock<RiskMetrics>>,
    capital_deployed: Arc<RwLock<u64>>,
    high_water_mark: Arc<RwLock<i64>>,
    rolling_returns: Arc<RwLock<VecDeque<f64>>>,
}

impl PnLDecomposer {
    pub fn new(initial_capital: u64) -> Self {
        Self {
            history: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_HISTORY_SIZE))),
            strategy_metrics: Arc::new(RwLock::new(HashMap::new())),
            risk_metrics: Arc::new(RwLock::new(RiskMetrics {
                var_95: 0,
                cvar_95: 0,
                beta: 0.0,
                correlation_to_market: 0.0,
                downside_deviation: 0.0,
                sortino_ratio: 0.0,
                calmar_ratio: 0.0,
                omega_ratio: 1.0,
            })),
            capital_deployed: Arc::new(RwLock::new(initial_capital)),
            high_water_mark: Arc::new(RwLock::new(0)),
            rolling_returns: Arc::new(RwLock::new(VecDeque::with_capacity(ROLLING_WINDOW_SIZE))),
        }
    }

    pub fn decompose_transaction_pnl(
        &self,
        tx_signature: &Signature,
        pre_balance: u64,
        post_balance: u64,
        token_delta: i64,
        token_price: u64,
        gas_used: u64,
        priority_fee: u64,
        strategy_id: u32,
        slot: u64,
        timestamp: i64,
    ) -> DetailedPnL {
        let gross_revenue = self.calculate_gross_revenue(token_delta, token_price);
        let network_fees = self.calculate_network_fees(gas_used);
        let priority_fees = priority_fee as i64;
        let slippage_cost = self.calculate_slippage_cost(token_delta, token_price);
        let opportunity_cost = self.calculate_opportunity_cost(pre_balance, slot);
        let mev_extracted = self.calculate_mev_value(gross_revenue, network_fees, priority_fees);
        let impermanent_loss = self.calculate_impermanent_loss(token_delta, token_price, timestamp);

        let total_pnl = gross_revenue - network_fees - priority_fees - slippage_cost - opportunity_cost - impermanent_loss;
        let realized_pnl = if post_balance > pre_balance {
            (post_balance - pre_balance) as i64
        } else {
            -((pre_balance - post_balance) as i64)
        };
        let unrealized_pnl = total_pnl - realized_pnl;

        let capital = *self.capital_deployed.read().unwrap();
        let roi_bps = if capital > 0 {
            (total_pnl * BASIS_POINTS as i64) / capital as i64
        } else {
            0
        };

        let components = PnLComponent {
            gross_revenue,
            network_fees,
            priority_fees,
            slippage_cost,
            opportunity_cost,
            mev_extracted,
            impermanent_loss,
            timestamp,
            slot,
            strategy_id,
        };

        let sharpe_ratio = self.calculate_sharpe_ratio();
        let max_drawdown_bps = self.calculate_max_drawdown();
        let (win_rate, profit_factor) = self.calculate_win_metrics();

        DetailedPnL {
            total_pnl,
            realized_pnl,
            unrealized_pnl,
            components,
            roi_bps,
            sharpe_ratio,
            max_drawdown_bps,
            win_rate,
            profit_factor,
        }
    }

    fn calculate_gross_revenue(&self, token_delta: i64, token_price: u64) -> i64 {
        let price_normalized = token_price as i64;
        (token_delta.saturating_mul(price_normalized)) / PRECISION_FACTOR as i64
    }

    fn calculate_network_fees(&self, gas_used: u64) -> i64 {
        let base_fee = 5000u64;
        let compute_unit_price = 1u64;
        let total_fee = base_fee + (gas_used * compute_unit_price);
        (total_fee as f64 * GAS_PRICE_BUFFER) as i64
    }

    fn calculate_slippage_cost(&self, token_delta: i64, token_price: u64) -> i64 {
        let expected_price = token_price;
        let actual_price = self.get_actual_execution_price(token_delta.abs() as u64, token_price);
        let price_impact = if actual_price > expected_price {
            actual_price - expected_price
        } else {
            0
        };
        
        let slippage_bps = (price_impact * BASIS_POINTS) / expected_price;
        if slippage_bps > MAX_SLIPPAGE_BPS {
            (token_delta.abs() * MAX_SLIPPAGE_BPS as i64 * token_price as i64) / (BASIS_POINTS as i64 * PRECISION_FACTOR as i64)
        } else {
            (token_delta.abs() * slippage_bps as i64 * token_price as i64) / (BASIS_POINTS as i64 * PRECISION_FACTOR as i64)
        }
    }

    fn get_actual_execution_price(&self, size: u64, base_price: u64) -> u64 {
        let impact_factor = 1.0 + (size as f64 / 1_000_000.0) * 0.001;
        (base_price as f64 * impact_factor) as u64
    }

    fn calculate_opportunity_cost(&self, capital: u64, slot: u64) -> i64 {
        let risk_free_rate = 0.05f64;
        let slot_duration_ms = 400;
        let ms_per_year = 365.25 * 24.0 * 60.0 * 60.0 * 1000.0;
        let time_fraction = slot_duration_ms as f64 / ms_per_year;
        
        ((capital as f64 * risk_free_rate * time_fraction) as i64).max(0)
    }

    fn calculate_mev_value(&self, gross_revenue: i64, network_fees: i64, priority_fees: i64) -> i64 {
        let net_before_mev = gross_revenue - network_fees - priority_fees;
        if net_before_mev > 0 {
            net_before_mev
        } else {
            0
        }
    }

    fn calculate_impermanent_loss(&self, token_delta: i64, token_price: u64, timestamp: i64) -> i64 {
        if token_delta == 0 {
            return 0;
        }

        let price_change_factor = self.get_price_volatility(timestamp);
        let il_factor = 2.0 * price_change_factor.sqrt() / (1.0 + price_change_factor) - 1.0;
        
        let position_value = (token_delta.abs() as f64 * token_price as f64) / PRECISION_FACTOR as f64;
        (position_value * il_factor.abs()) as i64
    }

    fn get_price_volatility(&self, timestamp: i64) -> f64 {
        let base_volatility = 0.02;
        let time_of_day_factor = ((timestamp % 86400) as f64 / 86400.0 * 2.0 * std::f64::consts::PI).sin();
        1.0 + base_volatility * (1.0 + time_of_day_factor * 0.5)
    }

    fn calculate_sharpe_ratio(&self) -> f64 {
        let history = self.history.read().unwrap();
        if history.len() < 30 {
            return 0.0;
        }

        let returns: Vec<f64> = history.iter()
            .map(|pnl| pnl.roi_bps as f64 / BASIS_POINTS as f64)
            .collect();

        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / returns.len() as f64;
        
        let std_dev = variance.sqrt();
        if std_dev > 0.0 {
            let risk_free_rate = 0.05 / 365.25 / 24.0 / 60.0 / 60.0 * 0.4;
            (mean_return - risk_free_rate) / std_dev * (252.0f64).sqrt()
        } else {
            0.0
        }
    }

    fn calculate_max_drawdown(&self) -> i64 {
        let history = self.history.read().unwrap();
        if history.is_empty() {
            return 0;
        }

        let mut peak = 0i64;
        let mut max_dd = 0i64;
        let mut cumulative_pnl = 0i64;

        for pnl in history.iter() {
            cumulative_pnl += pnl.total_pnl;
            if cumulative_pnl > peak {
                peak = cumulative_pnl;
            }
            let drawdown = peak - cumulative_pnl;
            if drawdown > max_dd {
                max_dd = drawdown;
            }
        }

        let capital = *self.capital_deployed.read().unwrap() as i64;
        if capital > 0 {
            (max_dd * BASIS_POINTS as i64) / capital
        } else {
            0
        }
    }

    fn calculate_win_metrics(&self) -> (f64, f64) {
        let history = self.history.read().unwrap();
        if history.is_empty() {
            return (0.0, 1.0);
        }

        let mut wins = 0u64;
        let mut total_profit = 0i64;
        let mut total_loss = 0i64;

        for pnl in history.iter() {
            if pnl.total_pnl > 0 {
                wins += 1;
                total_profit += pnl.total_pnl;
            } else if pnl.total_pnl < 0 {
                total_loss += pnl.total_pnl.abs();
            }
        }

        let win_rate = wins as f64 / history.len() as f64;
        let profit_factor = if total_loss > 0 {
            total_profit as f64 / total_loss as f64
        } else {
            f64::MAX
        };

        (win_rate, profit_factor)
    }

        pub fn update_metrics(&self, pnl: DetailedPnL) {
        let mut history = self.history.write().unwrap();
        if history.len() >= MAX_HISTORY_SIZE {
            history.pop_front();
        }
        
        let strategy_id = pnl.components.strategy_id;
        let total_pnl = pnl.total_pnl;
        
        history.push_back(pnl.clone());
        drop(history);

        let mut strategy_metrics = self.strategy_metrics.write().unwrap();
        let metrics = strategy_metrics.entry(strategy_id).or_insert(StrategyMetrics {
            total_trades: 0,
            winning_trades: 0,
            losing_trades: 0,
            avg_profit_per_trade: 0,
            avg_loss_per_trade: 0,
            largest_win: 0,
            largest_loss: 0,
            consecutive_wins: 0,
            consecutive_losses: 0,
            avg_holding_time_slots: 0,
            capital_efficiency: 0.0,
        });

        metrics.total_trades += 1;
        
        if total_pnl > 0 {
            metrics.winning_trades += 1;
            metrics.consecutive_wins += 1;
            metrics.consecutive_losses = 0;
            
            if total_pnl > metrics.largest_win {
                metrics.largest_win = total_pnl;
            }
            
            let prev_avg = metrics.avg_profit_per_trade;
            metrics.avg_profit_per_trade = 
                (prev_avg * (metrics.winning_trades - 1) as i64 + total_pnl) / metrics.winning_trades as i64;
        } else if total_pnl < 0 {
            metrics.losing_trades += 1;
            metrics.consecutive_losses += 1;
            metrics.consecutive_wins = 0;
            
            if total_pnl < metrics.largest_loss {
                metrics.largest_loss = total_pnl;
            }
            
            let prev_avg = metrics.avg_loss_per_trade;
            metrics.avg_loss_per_trade = 
                (prev_avg * (metrics.losing_trades - 1) as i64 + total_pnl.abs()) / metrics.losing_trades as i64;
        }

        let capital = *self.capital_deployed.read().unwrap() as i64;
        metrics.capital_efficiency = if capital > 0 {
            (metrics.avg_profit_per_trade as f64 * metrics.winning_trades as f64) / capital as f64
        } else {
            0.0
        };

        drop(strategy_metrics);

        self.update_risk_metrics();
        self.update_rolling_returns(pnl.roi_bps);
        self.update_high_water_mark(total_pnl);
    }

    fn update_risk_metrics(&self) {
        let history = self.history.read().unwrap();
        if history.len() < 100 {
            return;
        }

        let returns: Vec<f64> = history.iter()
            .map(|p| p.roi_bps as f64 / BASIS_POINTS as f64)
            .collect();

        let sorted_returns = {
            let mut sorted = returns.clone();
            sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            sorted
        };

        let var_95_idx = (sorted_returns.len() as f64 * 0.05) as usize;
        let var_95 = sorted_returns[var_95_idx] * (*self.capital_deployed.read().unwrap() as f64);

        let cvar_95 = sorted_returns[..var_95_idx].iter()
            .sum::<f64>() / var_95_idx as f64 * (*self.capital_deployed.read().unwrap() as f64);

        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let downside_returns: Vec<f64> = returns.iter()
            .filter(|&&r| r < 0.0)
            .cloned()
            .collect();

        let downside_deviation = if !downside_returns.is_empty() {
            let downside_mean = downside_returns.iter().sum::<f64>() / downside_returns.len() as f64;
            (downside_returns.iter()
                .map(|r| (r - downside_mean).powi(2))
                .sum::<f64>() / downside_returns.len() as f64).sqrt()
        } else {
            0.0
        };

        let risk_free_rate = 0.05 / 365.25;
        let sortino_ratio = if downside_deviation > 0.0 {
            (mean_return - risk_free_rate) / downside_deviation * (252.0f64).sqrt()
        } else {
            0.0
        };

        let max_dd = self.calculate_max_drawdown() as f64 / BASIS_POINTS as f64;
        let calmar_ratio = if max_dd > 0.0 {
            mean_return * 252.0 / max_dd
        } else {
            0.0
        };

        let positive_returns = returns.iter().filter(|&&r| r > risk_free_rate).count() as f64;
        let negative_returns = returns.iter().filter(|&&r| r <= risk_free_rate).count() as f64;
        let omega_ratio = if negative_returns > 0.0 {
            positive_returns / negative_returns
        } else {
            positive_returns
        };

        let mut risk_metrics = self.risk_metrics.write().unwrap();
        risk_metrics.var_95 = var_95 as i64;
        risk_metrics.cvar_95 = cvar_95 as i64;
        risk_metrics.downside_deviation = downside_deviation;
        risk_metrics.sortino_ratio = sortino_ratio;
        risk_metrics.calmar_ratio = calmar_ratio;
        risk_metrics.omega_ratio = omega_ratio;
        risk_metrics.beta = self.calculate_beta(&returns);
        risk_metrics.correlation_to_market = self.calculate_market_correlation(&returns);
    }

    fn update_rolling_returns(&self, roi_bps: i64) {
        let mut rolling_returns = self.rolling_returns.write().unwrap();
        if rolling_returns.len() >= ROLLING_WINDOW_SIZE {
            rolling_returns.pop_front();
        }
        rolling_returns.push_back(roi_bps as f64 / BASIS_POINTS as f64);
    }

    fn update_high_water_mark(&self, pnl: i64) {
        let mut hwm = self.high_water_mark.write().unwrap();
        let cumulative_pnl = self.get_cumulative_pnl();
        if cumulative_pnl > *hwm {
            *hwm = cumulative_pnl;
        }
    }

    fn calculate_beta(&self, returns: &[f64]) -> f64 {
        if returns.len() < 30 {
            return 1.0;
        }

        let market_return = 0.0001;
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        
        let covariance = returns.iter()
            .map(|r| (r - mean_return) * (market_return - market_return))
            .sum::<f64>() / returns.len() as f64;

        let market_variance = 0.0001;
        if market_variance > 0.0 {
            covariance / market_variance
        } else {
            1.0
        }
    }

    fn calculate_market_correlation(&self, returns: &[f64]) -> f64 {
        if returns.len() < 30 {
            return 0.0;
        }

        let simulated_market_vol = 0.02;
        let return_vol = returns.iter()
            .map(|r| r.powi(2))
            .sum::<f64>() / returns.len() as f64;

        if return_vol > 0.0 {
            (return_vol.sqrt() / simulated_market_vol).min(1.0).max(-1.0)
        } else {
            0.0
        }
    }

    pub fn get_cumulative_pnl(&self) -> i64 {
        self.history.read().unwrap()
            .iter()
            .map(|p| p.total_pnl)
            .sum()
    }

    pub fn get_strategy_performance(&self, strategy_id: u32) -> Option<StrategyPerformance> {
        let metrics = self.strategy_metrics.read().unwrap();
        let strategy_metric = metrics.get(&strategy_id)?;
        
        let history = self.history.read().unwrap();
        let strategy_pnls: Vec<&DetailedPnL> = history.iter()
            .filter(|p| p.components.strategy_id == strategy_id)
            .collect();

        if strategy_pnls.is_empty() {
            return None;
        }

        let total_pnl: i64 = strategy_pnls.iter().map(|p| p.total_pnl).sum();
        let avg_pnl = total_pnl / strategy_pnls.len() as i64;
        let best_trade = strategy_pnls.iter().map(|p| p.total_pnl).max().unwrap_or(0);
        let worst_trade = strategy_pnls.iter().map(|p| p.total_pnl).min().unwrap_or(0);

        Some(StrategyPerformance {
            strategy_id,
            total_pnl,
            avg_pnl,
            total_trades: strategy_metric.total_trades,
            win_rate: strategy_metric.winning_trades as f64 / strategy_metric.total_trades as f64,
            profit_factor: if strategy_metric.avg_loss_per_trade > 0 {
                strategy_metric.avg_profit_per_trade as f64 / strategy_metric.avg_loss_per_trade as f64
            } else {
                f64::MAX
            },
            best_trade,
            worst_trade,
            consecutive_wins: strategy_metric.consecutive_wins,
            consecutive_losses: strategy_metric.consecutive_losses,
        })
    }

    pub fn get_current_risk_metrics(&self) -> RiskMetrics {
        self.risk_metrics.read().unwrap().clone()
    }

    pub fn get_detailed_breakdown(&self, last_n: usize) -> Vec<DetailedPnL> {
        let history = self.history.read().unwrap();
        let start_idx = history.len().saturating_sub(last_n);
        history.iter().skip(start_idx).cloned().collect()
    }

    pub fn calculate_kelly_criterion(&self, strategy_id: u32) -> f64 {
        let metrics = self.strategy_metrics.read().unwrap();
        if let Some(strategy) = metrics.get(&strategy_id) {
            let win_rate = strategy.winning_trades as f64 / strategy.total_trades.max(1) as f64;
            let avg_win = strategy.avg_profit_per_trade as f64;
            let avg_loss = strategy.avg_loss_per_trade as f64;
            
            if avg_loss > 0.0 {
                let b = avg_win / avg_loss;
                let kelly = (b * win_rate - (1.0 - win_rate)) / b;
                kelly.max(0.0).min(0.25)
            } else {
                0.0
            }
        } else {
            0.0
        }
    }

    pub fn reset_metrics(&self) {
        self.history.write().unwrap().clear();
        self.strategy_metrics.write().unwrap().clear();
        self.rolling_returns.write().unwrap().clear();
        *self.high_water_mark.write().unwrap() = 0;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StrategyPerformance {
    pub strategy_id: u32,
    pub total_pnl: i64,
    pub avg_pnl: i64,
    pub total_trades: u64,
    pub win_rate: f64,
    pub profit_factor: f64,
    pub best_trade: i64,
    pub worst_trade: i64,
    pub consecutive_wins: u32,
    pub consecutive_losses: u32,
}

impl Default for PnLDecomposer {
    fn default() -> Self {
        Self::new(1_000_000_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pnl_decomposition() {
        let decomposer = PnLDecomposer::new(1_000_000_000);
        let sig = Signature::default();
        
        let pnl = decomposer.decompose_transaction_pnl(
            &sig,
            1_000_000_000,
            1_001_000_000,
            1000,
            1_000_000,
            100_000,
            5_000,
            1,
            100,
            1234567890,
        );

        assert!(pnl.total_pnl != 0);
        assert!(pnl.components.network_fees > 0);
    }
}

