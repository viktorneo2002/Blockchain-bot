use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use solana_sdk::pubkey::Pubkey;
use serde::{Deserialize, Serialize};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;

const MAX_HISTORY_SIZE: usize = 10000;
const MIN_SAMPLE_SIZE: usize = 100;
const DEFAULT_KELLY_FRACTION: Decimal = dec!(0.25);
const MAX_KELLY_FRACTION: Decimal = dec!(0.40);
const MIN_POSITION_SIZE: u64 = 100000; // 0.0001 SOL
const MAX_POSITION_RATIO: Decimal = dec!(0.15);
const CONFIDENCE_DECAY_FACTOR: Decimal = dec!(0.995);
const VOLATILITY_WINDOW: usize = 50;
const SHARPE_WINDOW: usize = 200;
const MIN_SHARPE_RATIO: Decimal = dec!(1.5);
const DRAWDOWN_THRESHOLD: Decimal = dec!(0.10);
const RECOVERY_MULTIPLIER: Decimal = dec!(0.5);
const GAS_RESERVE_MULTIPLIER: Decimal = dec!(1.15);
const SLIPPAGE_BUFFER: Decimal = dec!(0.003);
const MAX_CONSECUTIVE_LOSSES: u32 = 5;
const RISK_FREE_RATE: Decimal = dec!(0.00001); // Per opportunity

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpportunityResult {
    pub timestamp: u64,
    pub profit_lamports: i64,
    pub capital_deployed: u64,
    pub gas_used: u64,
    pub opportunity_type: OpportunityType,
    pub success: bool,
    pub slippage: Decimal,
    pub priority_fee: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub enum OpportunityType {
    Arbitrage,
    Liquidation,
    JitLiquidity,
    Sandwich,
}

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub win_rate: Decimal,
    pub avg_win: Decimal,
    pub avg_loss: Decimal,
    pub sharpe_ratio: Decimal,
    pub max_drawdown: Decimal,
    pub current_drawdown: Decimal,
    pub volatility: Decimal,
    pub consecutive_losses: u32,
    pub total_opportunities: u64,
    pub profitable_opportunities: u64,
}

#[derive(Debug, Clone)]
pub struct KellyParameters {
    pub kelly_fraction: Decimal,
    pub position_size_ratio: Decimal,
    pub confidence_score: Decimal,
    pub risk_adjustment: Decimal,
    pub max_position_lamports: u64,
    pub min_position_lamports: u64,
}

pub struct FractionalKellyAdjuster {
    history: Arc<RwLock<VecDeque<OpportunityResult>>>,
    performance_by_type: Arc<RwLock<std::collections::HashMap<OpportunityType, PerformanceMetrics>>>,
    total_capital: Arc<RwLock<u64>>,
    high_water_mark: Arc<RwLock<u64>>,
    last_update: Arc<RwLock<u64>>,
    adaptive_params: Arc<RwLock<AdaptiveParameters>>,
}

#[derive(Debug, Clone)]
struct AdaptiveParameters {
    base_kelly_fraction: Decimal,
    volatility_threshold: Decimal,
    sharpe_target: Decimal,
    recovery_mode: bool,
    last_adjustment: u64,
}

impl FractionalKellyAdjuster {
    pub fn new(initial_capital: u64) -> Self {
        Self {
            history: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_HISTORY_SIZE))),
            performance_by_type: Arc::new(RwLock::new(std::collections::HashMap::new())),
            total_capital: Arc::new(RwLock::new(initial_capital)),
            high_water_mark: Arc::new(RwLock::new(initial_capital)),
            last_update: Arc::new(RwLock::new(Self::current_timestamp())),
            adaptive_params: Arc::new(RwLock::new(AdaptiveParameters {
                base_kelly_fraction: DEFAULT_KELLY_FRACTION,
                volatility_threshold: dec!(0.05),
                sharpe_target: dec!(2.0),
                recovery_mode: false,
                last_adjustment: Self::current_timestamp(),
            })),
        }
    }

    pub fn calculate_position_size(
        &self,
        opportunity_type: OpportunityType,
        expected_profit: u64,
        confidence: Decimal,
        current_gas_price: u64,
    ) -> Result<u64, &'static str> {
        let history = self.history.read().map_err(|_| "Failed to acquire read lock")?;
        
        if history.len() < MIN_SAMPLE_SIZE {
            return Ok(self.calculate_conservative_position());
        }

        let metrics = self.calculate_metrics_for_type(opportunity_type)?;
        let kelly_params = self.compute_kelly_parameters(&metrics, confidence)?;
        
        let total_capital = *self.total_capital.read().map_err(|_| "Failed to read capital")?;
        let position_size = self.apply_position_sizing(
            total_capital,
            kelly_params,
            expected_profit,
            current_gas_price,
        )?;

        Ok(position_size)
    }

    pub fn record_opportunity_result(&self, result: OpportunityResult) -> Result<(), &'static str> {
        let mut history = self.history.write().map_err(|_| "Failed to acquire write lock")?;
        
        if history.len() >= MAX_HISTORY_SIZE {
            history.pop_front();
        }
        
        history.push_back(result.clone());
        drop(history);

        self.update_capital(result.profit_lamports)?;
        self.update_performance_metrics(result.opportunity_type)?;
        self.check_adaptive_adjustments()?;

        Ok(())
    }

    fn calculate_metrics_for_type(&self, opportunity_type: OpportunityType) -> Result<PerformanceMetrics, &'static str> {
        let history = self.history.read().map_err(|_| "Failed to acquire read lock")?;
        
        let type_results: Vec<&OpportunityResult> = history
            .iter()
            .filter(|r| r.opportunity_type == opportunity_type)
            .collect();

        if type_results.len() < 10 {
            return Ok(self.default_metrics());
        }

        let profitable = type_results.iter().filter(|r| r.success && r.profit_lamports > 0).count() as u64;
        let total = type_results.len() as u64;
        let win_rate = Decimal::from(profitable) / Decimal::from(total);

        let wins: Vec<Decimal> = type_results
            .iter()
            .filter(|r| r.success && r.profit_lamports > 0)
            .map(|r| Decimal::from(r.profit_lamports) / Decimal::from(r.capital_deployed))
            .collect();

        let losses: Vec<Decimal> = type_results
            .iter()
            .filter(|r| !r.success || r.profit_lamports <= 0)
            .map(|r| {
                let loss = if r.profit_lamports < 0 {
                    Decimal::from(-r.profit_lamports)
                } else {
                    Decimal::from(r.gas_used)
                };
                loss / Decimal::from(r.capital_deployed)
            })
            .collect();

        let avg_win = if wins.is_empty() {
            Decimal::ZERO
        } else {
            wins.iter().sum::<Decimal>() / Decimal::from(wins.len())
        };

        let avg_loss = if losses.is_empty() {
            Decimal::ZERO
        } else {
            losses.iter().sum::<Decimal>() / Decimal::from(losses.len())
        };

        let returns = self.calculate_returns(&type_results)?;
        let volatility = self.calculate_volatility(&returns);
        let sharpe_ratio = self.calculate_sharpe_ratio(&returns, volatility);
        let (max_drawdown, current_drawdown) = self.calculate_drawdowns(&returns);
        let consecutive_losses = self.count_consecutive_losses(&type_results);

        Ok(PerformanceMetrics {
            win_rate,
            avg_win,
            avg_loss,
            sharpe_ratio,
            max_drawdown,
            current_drawdown,
            volatility,
            consecutive_losses,
            total_opportunities: total,
            profitable_opportunities: profitable,
        })
    }

    fn compute_kelly_parameters(
        &self,
        metrics: &PerformanceMetrics,
        base_confidence: Decimal,
    ) -> Result<KellyParameters, &'static str> {
        let adaptive = self.adaptive_params.read().map_err(|_| "Failed to read adaptive params")?;
        
        let kelly_optimal = if metrics.avg_loss > Decimal::ZERO {
            (metrics.win_rate * metrics.avg_win - (Decimal::ONE - metrics.win_rate) * metrics.avg_loss) / metrics.avg_win
        } else {
            Decimal::ZERO
        };

        let kelly_fraction = if kelly_optimal <= Decimal::ZERO {
            dec!(0.01)
        } else {
            kelly_optimal.min(MAX_KELLY_FRACTION) * adaptive.base_kelly_fraction
        };

        let volatility_adjustment = if metrics.volatility > adaptive.volatility_threshold {
            Decimal::ONE - (metrics.volatility - adaptive.volatility_threshold).min(dec!(0.5))
        } else {
            Decimal::ONE
        };

        let sharpe_adjustment = if metrics.sharpe_ratio < MIN_SHARPE_RATIO {
            metrics.sharpe_ratio / MIN_SHARPE_RATIO
        } else {
            Decimal::ONE + (metrics.sharpe_ratio - MIN_SHARPE_RATIO) * dec!(0.1)
        }.min(dec!(1.5));

        let drawdown_adjustment = if metrics.current_drawdown > DRAWDOWN_THRESHOLD {
            RECOVERY_MULTIPLIER
        } else {
            Decimal::ONE - metrics.current_drawdown / dec!(2)
        };

        let consecutive_loss_adjustment = Decimal::ONE / (Decimal::ONE + Decimal::from(metrics.consecutive_losses) * dec!(0.2));

        let confidence_score = base_confidence * 
            Decimal::from(metrics.total_opportunities).min(dec!(1000)) / dec!(1000);

        let risk_adjustment = volatility_adjustment
            .min(sharpe_adjustment)
            .min(drawdown_adjustment)
            .min(consecutive_loss_adjustment);

        let adjusted_kelly = kelly_fraction * risk_adjustment * confidence_score;
        let position_size_ratio = adjusted_kelly.min(MAX_POSITION_RATIO);

        let total_capital = *self.total_capital.read().map_err(|_| "Failed to read capital")?;
        let max_position = (Decimal::from(total_capital) * position_size_ratio)
            .to_u64()
            .ok_or("Failed to convert position size")?;

        Ok(KellyParameters {
            kelly_fraction: adjusted_kelly,
            position_size_ratio,
            confidence_score,
            risk_adjustment,
            max_position_lamports: max_position,
            min_position_lamports: MIN_POSITION_SIZE,
        })
    }

    fn apply_position_sizing(
        &self,
        total_capital: u64,
        params: KellyParameters,
        expected_profit: u64,
        gas_estimate: u64,
    ) -> Result<u64, &'static str> {
        let gas_adjusted = Decimal::from(gas_estimate) * GAS_RESERVE_MULTIPLIER;
        let slippage_reserve = Decimal::from(expected_profit) * SLIPPAGE_BUFFER;
        let total_reserve = (gas_adjusted + slippage_reserve)
            .to_u64()
            .ok_or("Failed to calculate reserves")?;

        let available_capital = total_capital.saturating_sub(total_reserve);
        let position_size = params.max_position_lamports.min(available_capital);

        if position_size < params.min_position_lamports {
            return Ok(0);
        }

        let expected_return = Decimal::from(expected_profit) / Decimal::from(position_size + gas_estimate);
        if expected_return < RISK_FREE_RATE * dec!(2) {
            return Ok(0);
        }

        Ok(position_size)
    }

    fn calculate_returns(&self, results: &[&OpportunityResult]) -> Result<Vec<Decimal>, &'static str> {
        let mut returns = Vec::with_capacity(results.len());
        
        for result in results {
            let return_rate = if result.success {
                Decimal::from(result.profit_lamports) / Decimal::from(result.capital_deployed + result.gas_used)
            } else {
                -Decimal::from(result.gas_used) / Decimal::from(result.capital_deployed + result.gas_used)
            };
            returns.push(return_rate);
        }

        Ok(returns)
    }

    fn calculate_volatility(&self, returns: &[Decimal]) -> Decimal {
        if returns.len() < VOLATILITY_WINDOW {
            return dec!(0.1);
        }

        let recent_returns: Vec<Decimal> = returns.iter()
            .rev()
            .take(VOLATILITY_WINDOW)
            .copied()
            .collect();

        let mean = recent_returns.iter().sum::<Decimal>() / Decimal::from(recent_returns.len());
        let variance = recent_returns.iter()
            .map(|r| (*r - mean).powi(2))
            .sum::<Decimal>() / Decimal::from(recent_returns.len());

        variance.sqrt().unwrap_or(dec!(0.1))
    }

        fn calculate_sharpe_ratio(&self, returns: &[Decimal], volatility: Decimal) -> Decimal {
        if returns.len() < SHARPE_WINDOW || volatility == Decimal::ZERO {
            return Decimal::ZERO;
        }

        let recent_returns: Vec<Decimal> = returns.iter()
            .rev()
            .take(SHARPE_WINDOW)
            .copied()
            .collect();

        let mean_return = recent_returns.iter().sum::<Decimal>() / Decimal::from(recent_returns.len());
        let excess_return = mean_return - RISK_FREE_RATE;

        if volatility > Decimal::ZERO {
            excess_return / volatility
        } else {
            Decimal::ZERO
        }
    }

    fn calculate_drawdowns(&self, returns: &[Decimal]) -> (Decimal, Decimal) {
        if returns.is_empty() {
            return (Decimal::ZERO, Decimal::ZERO);
        }

        let mut cumulative_return = Decimal::ONE;
        let mut peak = Decimal::ONE;
        let mut max_drawdown = Decimal::ZERO;
        let mut current_value = Decimal::ONE;

        for &ret in returns {
            cumulative_return *= (Decimal::ONE + ret);
            current_value = cumulative_return;
            
            if current_value > peak {
                peak = current_value;
            }
            
            let drawdown = if peak > Decimal::ZERO {
                (peak - current_value) / peak
            } else {
                Decimal::ZERO
            };
            
            max_drawdown = max_drawdown.max(drawdown);
        }

        let current_drawdown = if peak > Decimal::ZERO {
            (peak - current_value) / peak
        } else {
            Decimal::ZERO
        };

        (max_drawdown, current_drawdown)
    }

    fn count_consecutive_losses(&self, results: &[&OpportunityResult]) -> u32 {
        let mut consecutive = 0u32;
        let mut max_consecutive = 0u32;

        for result in results.iter().rev() {
            if !result.success || result.profit_lamports <= 0 {
                consecutive += 1;
                max_consecutive = max_consecutive.max(consecutive);
            } else {
                break;
            }
        }

        consecutive
    }

    fn update_capital(&self, profit_lamports: i64) -> Result<(), &'static str> {
        let mut capital = self.total_capital.write().map_err(|_| "Failed to acquire capital lock")?;
        let mut hwm = self.high_water_mark.write().map_err(|_| "Failed to acquire HWM lock")?;

        if profit_lamports > 0 {
            *capital = capital.saturating_add(profit_lamports as u64);
        } else {
            *capital = capital.saturating_sub((-profit_lamports) as u64);
        }

        if *capital > *hwm {
            *hwm = *capital;
        }

        Ok(())
    }

    fn update_performance_metrics(&self, opportunity_type: OpportunityType) -> Result<(), &'static str> {
        let metrics = self.calculate_metrics_for_type(opportunity_type)?;
        let mut perf_map = self.performance_by_type.write().map_err(|_| "Failed to acquire perf lock")?;
        perf_map.insert(opportunity_type, metrics);
        Ok(())
    }

    fn check_adaptive_adjustments(&self) -> Result<(), &'static str> {
        let mut adaptive = self.adaptive_params.write().map_err(|_| "Failed to acquire adaptive lock")?;
        let current_time = Self::current_timestamp();
        
        if current_time - adaptive.last_adjustment < 300_000 {
            return Ok(());
        }

        let capital = *self.total_capital.read().map_err(|_| "Failed to read capital")?;
        let hwm = *self.high_water_mark.read().map_err(|_| "Failed to read HWM")?;
        
        let drawdown_ratio = if hwm > 0 {
            Decimal::ONE - Decimal::from(capital) / Decimal::from(hwm)
        } else {
            Decimal::ZERO
        };

        if drawdown_ratio > DRAWDOWN_THRESHOLD {
            adaptive.recovery_mode = true;
            adaptive.base_kelly_fraction = DEFAULT_KELLY_FRACTION * RECOVERY_MULTIPLIER;
        } else if adaptive.recovery_mode && drawdown_ratio < dec!(0.05) {
            adaptive.recovery_mode = false;
            adaptive.base_kelly_fraction = DEFAULT_KELLY_FRACTION;
        }

        let perf_map = self.performance_by_type.read().map_err(|_| "Failed to read perf")?;
        let avg_sharpe = if !perf_map.is_empty() {
            perf_map.values().map(|m| m.sharpe_ratio).sum::<Decimal>() / Decimal::from(perf_map.len())
        } else {
            Decimal::ZERO
        };

        if avg_sharpe > adaptive.sharpe_target {
            adaptive.base_kelly_fraction = (adaptive.base_kelly_fraction * dec!(1.05)).min(MAX_KELLY_FRACTION);
        } else if avg_sharpe < MIN_SHARPE_RATIO {
            adaptive.base_kelly_fraction = (adaptive.base_kelly_fraction * dec!(0.95)).max(dec!(0.10));
        }

        adaptive.last_adjustment = current_time;
        Ok(())
    }

    fn calculate_conservative_position(&self) -> u64 {
        let capital = self.total_capital.read().unwrap_or_else(|e| e.into_inner());
        (*capital as f64 * 0.01) as u64
    }

    fn default_metrics(&self) -> PerformanceMetrics {
        PerformanceMetrics {
            win_rate: dec!(0.5),
            avg_win: dec!(0.002),
            avg_loss: dec!(0.001),
            sharpe_ratio: Decimal::ZERO,
            max_drawdown: Decimal::ZERO,
            current_drawdown: Decimal::ZERO,
            volatility: dec!(0.1),
            consecutive_losses: 0,
            total_opportunities: 0,
            profitable_opportunities: 0,
        }
    }

    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    pub fn get_current_parameters(&self) -> Result<KellyParameters, &'static str> {
        let adaptive = self.adaptive_params.read().map_err(|_| "Failed to read adaptive params")?;
        let capital = *self.total_capital.read().map_err(|_| "Failed to read capital")?;
        
        Ok(KellyParameters {
            kelly_fraction: adaptive.base_kelly_fraction,
            position_size_ratio: MAX_POSITION_RATIO,
            confidence_score: Decimal::ONE,
            risk_adjustment: if adaptive.recovery_mode { RECOVERY_MULTIPLIER } else { Decimal::ONE },
            max_position_lamports: (Decimal::from(capital) * MAX_POSITION_RATIO).to_u64().unwrap_or(0),
            min_position_lamports: MIN_POSITION_SIZE,
        })
    }

    pub fn get_performance_summary(&self) -> Result<PerformanceSummary, &'static str> {
        let capital = *self.total_capital.read().map_err(|_| "Failed to read capital")?;
        let hwm = *self.high_water_mark.read().map_err(|_| "Failed to read HWM")?;
        let perf_map = self.performance_by_type.read().map_err(|_| "Failed to read performance")?;
        
        let total_opportunities: u64 = perf_map.values().map(|m| m.total_opportunities).sum();
        let total_profitable: u64 = perf_map.values().map(|m| m.profitable_opportunities).sum();
        
        let overall_win_rate = if total_opportunities > 0 {
            Decimal::from(total_profitable) / Decimal::from(total_opportunities)
        } else {
            Decimal::ZERO
        };

        Ok(PerformanceSummary {
            current_capital: capital,
            high_water_mark: hwm,
            overall_win_rate,
            total_opportunities,
            performance_by_type: perf_map.clone(),
        })
    }

    pub fn should_take_opportunity(
        &self,
        opportunity_type: OpportunityType,
        expected_profit: u64,
        required_capital: u64,
        confidence: Decimal,
        gas_estimate: u64,
    ) -> Result<bool, &'static str> {
        let position_size = self.calculate_position_size(
            opportunity_type,
            expected_profit,
            confidence,
            gas_estimate,
        )?;

        if position_size == 0 {
            return Ok(false);
        }

        if position_size < required_capital {
            return Ok(false);
        }

        let expected_return = Decimal::from(expected_profit) / Decimal::from(required_capital + gas_estimate);
        let perf_map = self.performance_by_type.read().map_err(|_| "Failed to read performance")?;
        
        if let Some(metrics) = perf_map.get(&opportunity_type) {
            if metrics.consecutive_losses >= MAX_CONSECUTIVE_LOSSES {
                return Ok(expected_return > metrics.avg_win * dec!(1.5));
            }
            
            let min_return = metrics.avg_win * confidence * dec!(0.8);
            return Ok(expected_return >= min_return);
        }

        Ok(expected_return > RISK_FREE_RATE * dec!(3))
    }

    pub fn reset_metrics(&self) -> Result<(), &'static str> {
        let mut history = self.history.write().map_err(|_| "Failed to acquire history lock")?;
        let mut perf_map = self.performance_by_type.write().map_err(|_| "Failed to acquire perf lock")?;
        
        history.clear();
        perf_map.clear();
        
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceSummary {
    pub current_capital: u64,
    pub high_water_mark: u64,
    pub overall_win_rate: Decimal,
    pub total_opportunities: u64,
    pub performance_by_type: std::collections::HashMap<OpportunityType, PerformanceMetrics>,
}

impl Default for FractionalKellyAdjuster {
    fn default() -> Self {
        Self::new(1_000_000_000) // 1 SOL default
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kelly_calculation() {
        let adjuster = FractionalKellyAdjuster::new(10_000_000_000);
        
        let result = OpportunityResult {
            timestamp: FractionalKellyAdjuster::current_timestamp(),
            profit_lamports: 1_000_000,
            capital_deployed: 100_000_000,
            gas_used: 5_000,
            opportunity_type: OpportunityType::Arbitrage,
            success: true,
            slippage: dec!(0.001),
            priority_fee: 1_000,
        };

        for _ in 0..MIN_SAMPLE_SIZE {
            adjuster.record_opportunity_result(result.clone()).unwrap();
        }

        let position_size = adjuster.calculate_position_size(
            OpportunityType::Arbitrage,
            1_000_000,
            dec!(0.95),
            5_000,
        ).unwrap();

        assert!(position_size > MIN_POSITION_SIZE);
        assert!(position_size < 10_000_000_000);
    }
}

