use std::sync::{Arc, RwLock};
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use solana_sdk::pubkey::Pubkey;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;

const LAMPORTS_PER_SOL: u64 = 1_000_000_000;
const MIN_ORDER_SIZE_LAMPORTS: u64 = 10_000_000; // 0.01 SOL
const MAX_ORDER_SIZE_LAMPORTS: u64 = 100_000_000_000; // 100 SOL
const RISK_FREE_RATE: Decimal = dec!(0.045); // 4.5% annual
const VOLATILITY_WINDOW: usize = 100;
const SUCCESS_RATE_WINDOW: usize = 1000;
const MIN_CONFIDENCE_THRESHOLD: f64 = 0.65;
const MAX_POSITION_RISK: f64 = 0.25; // 25% of capital
const SLIPPAGE_BUFFER: f64 = 0.003; // 0.3%
const PRIORITY_FEE_MULTIPLIER: f64 = 1.15;
const COMPUTE_UNIT_BUFFER: u32 = 50_000;
const BASE_COMPUTE_UNITS: u32 = 200_000;
const MAX_RETRY_ATTEMPTS: u8 = 3;
const VOLATILITY_DECAY: f64 = 0.95;
const CONFIDENCE_BOOST_FACTOR: f64 = 1.25;
const MIN_PROFIT_THRESHOLD: u64 = 100_000; // 0.0001 SOL
const KELLY_FRACTION: f64 = 0.25; // Conservative Kelly

#[derive(Debug, Clone)]
pub struct OrderMetrics {
    pub timestamp: Instant,
    pub size_lamports: u64,
    pub success: bool,
    pub profit_lamports: i64,
    pub gas_cost_lamports: u64,
    pub slippage_bps: u16,
    pub compute_units_used: u32,
    pub priority_fee: u64,
}

#[derive(Debug, Clone)]
pub struct MarketConditions {
    pub volatility: f64,
    pub liquidity_depth: u64,
    pub competitor_count: u32,
    pub network_congestion: f64,
    pub recent_gas_prices: VecDeque<u64>,
}

#[derive(Debug)]
pub struct RiskParameters {
    pub max_drawdown: f64,
    pub sharpe_ratio_target: f64,
    pub var_limit: f64,
    pub concentration_limit: f64,
}

pub struct AdaptiveOrderSizer {
    capital_lamports: Arc<RwLock<u64>>,
    order_history: Arc<RwLock<VecDeque<OrderMetrics>>>,
    market_conditions: Arc<RwLock<MarketConditions>>,
    risk_params: RiskParameters,
    volatility_ema: Arc<RwLock<f64>>,
    success_rate_ema: Arc<RwLock<f64>>,
    last_update: Arc<RwLock<Instant>>,
}

impl AdaptiveOrderSizer {
    pub fn new(initial_capital: u64) -> Self {
        Self {
            capital_lamports: Arc::new(RwLock::new(initial_capital)),
            order_history: Arc::new(RwLock::new(VecDeque::with_capacity(SUCCESS_RATE_WINDOW))),
            market_conditions: Arc::new(RwLock::new(MarketConditions {
                volatility: 0.02,
                liquidity_depth: 1_000_000_000_000,
                competitor_count: 10,
                network_congestion: 0.5,
                recent_gas_prices: VecDeque::with_capacity(50),
            })),
            risk_params: RiskParameters {
                max_drawdown: 0.15,
                sharpe_ratio_target: 2.0,
                var_limit: 0.05,
                concentration_limit: 0.30,
            },
            volatility_ema: Arc::new(RwLock::new(0.02)),
            success_rate_ema: Arc::new(RwLock::new(0.7)),
            last_update: Arc::new(RwLock::new(Instant::now())),
        }
    }

    pub fn calculate_order_size(
        &self,
        opportunity_value: u64,
        confidence_score: f64,
        token_mint: &Pubkey,
        liquidity_available: u64,
    ) -> Result<u64, &'static str> {
        if opportunity_value < MIN_PROFIT_THRESHOLD {
            return Err("Opportunity below minimum threshold");
        }

        let capital = *self.capital_lamports.read().map_err(|_| "Lock poisoned")?;
        if capital < MIN_ORDER_SIZE_LAMPORTS {
            return Err("Insufficient capital");
        }

        let market_conditions = self.market_conditions.read().map_err(|_| "Lock poisoned")?;
        let success_rate = *self.success_rate_ema.read().map_err(|_| "Lock poisoned")?;
        let volatility = *self.volatility_ema.read().map_err(|_| "Lock poisoned")?;

        let adjusted_confidence = self.adjust_confidence_for_market(
            confidence_score,
            &market_conditions,
            volatility,
        );

        if adjusted_confidence < MIN_CONFIDENCE_THRESHOLD {
            return Err("Confidence below threshold");
        }

        let kelly_size = self.calculate_kelly_criterion(
            opportunity_value,
            capital,
            adjusted_confidence,
            success_rate,
        );

        let risk_adjusted_size = self.apply_risk_constraints(
            kelly_size,
            capital,
            volatility,
            &market_conditions,
        );

        let liquidity_constrained = (risk_adjusted_size.min(liquidity_available) as f64 * 0.95) as u64;

        let gas_adjusted_size = self.adjust_for_gas_costs(
            liquidity_constrained,
            opportunity_value,
            &market_conditions,
        );

        let final_size = gas_adjusted_size
            .max(MIN_ORDER_SIZE_LAMPORTS)
            .min(MAX_ORDER_SIZE_LAMPORTS)
            .min(capital);

        self.validate_order_size(final_size, capital, opportunity_value)?;

        Ok(final_size)
    }

    fn calculate_kelly_criterion(
        &self,
        opportunity_value: u64,
        capital: u64,
        confidence: f64,
        success_rate: f64,
    ) -> u64 {
        let win_probability = confidence * success_rate;
        let loss_probability = 1.0 - win_probability;
        
        let expected_return = (opportunity_value as f64) / (capital as f64);
        let loss_fraction = 0.02; // Expected 2% loss on failure

        let kelly_fraction = (win_probability * expected_return - loss_probability * loss_fraction) 
            / expected_return;

        let conservative_kelly = kelly_fraction.max(0.0).min(1.0) * KELLY_FRACTION;
        
        (capital as f64 * conservative_kelly) as u64
    }

    fn adjust_confidence_for_market(
        &self,
        base_confidence: f64,
        market_conditions: &MarketConditions,
        volatility: f64,
    ) -> f64 {
        let volatility_penalty = 1.0 - (volatility - 0.01).max(0.0).min(0.1) * 2.0;
        let congestion_penalty = 1.0 - market_conditions.network_congestion * 0.3;
        let competition_penalty = 1.0 - (market_conditions.competitor_count as f64 / 100.0).min(0.5);
        
        let depth_bonus = (market_conditions.liquidity_depth as f64 / 1e12).min(1.5);
        
        base_confidence * volatility_penalty * congestion_penalty * competition_penalty * depth_bonus
    }

    fn apply_risk_constraints(
        &self,
        proposed_size: u64,
        capital: u64,
        volatility: f64,
        market_conditions: &MarketConditions,
    ) -> u64 {
        let max_position = (capital as f64 * MAX_POSITION_RISK) as u64;
        
        let var_constraint = self.calculate_var_constraint(capital, volatility);
        let concentration_limit = (capital as f64 * self.risk_params.concentration_limit) as u64;
        
        let stress_multiplier = self.calculate_stress_multiplier(market_conditions, volatility);
        
        proposed_size
            .min(max_position)
            .min(var_constraint)
            .min(concentration_limit)
            .min((proposed_size as f64 * stress_multiplier) as u64)
    }

    fn calculate_var_constraint(&self, capital: u64, volatility: f64) -> u64 {
        let z_score = 2.33; // 99% confidence
        let daily_var = volatility * z_score;
        
        ((capital as f64 * self.risk_params.var_limit) / daily_var) as u64
    }

    fn calculate_stress_multiplier(
        &self,
        market_conditions: &MarketConditions,
        volatility: f64,
    ) -> f64 {
        let base_multiplier = 1.0;
        
        let volatility_stress = if volatility > 0.05 {
            0.5
        } else if volatility > 0.03 {
            0.75
        } else {
            1.0
        };
        
        let congestion_stress = 1.0 - market_conditions.network_congestion * 0.5;
        let competition_stress = 1.0 - (market_conditions.competitor_count as f64 / 50.0).min(0.5);
        
        base_multiplier * volatility_stress * congestion_stress * competition_stress
    }

    fn adjust_for_gas_costs(
        &self,
        size: u64,
        opportunity_value: u64,
        market_conditions: &MarketConditions,
    ) -> u64 {
        let estimated_gas = self.estimate_transaction_cost(market_conditions);
        let min_profit_after_gas = opportunity_value.saturating_sub(estimated_gas * 2);
        
        if min_profit_after_gas < MIN_PROFIT_THRESHOLD {
            return 0;
        }
        
        let gas_impact = (estimated_gas as f64) / (size as f64);
        if gas_impact > 0.01 { // Gas > 1% of position
            ((size as f64) * (1.0 - gas_impact * 2.0)) as u64
        } else {
            size
        }
    }

    fn estimate_transaction_cost(&self, market_conditions: &MarketConditions) -> u64 {
        let base_fee = 5000; // 0.000005 SOL
        
        let priority_fee = if market_conditions.recent_gas_prices.is_empty() {
            10000
        } else {
            let avg_gas: u64 = market_conditions.recent_gas_prices.iter().sum::<u64>() 
                / market_conditions.recent_gas_prices.len() as u64;
            (avg_gas as f64 * PRIORITY_FEE_MULTIPLIER) as u64
        };
        
        let compute_cost = ((BASE_COMPUTE_UNITS + COMPUTE_UNIT_BUFFER) as f64 * 0.000001) as u64;
        
        base_fee + priority_fee + compute_cost
    }

    fn validate_order_size(
        &self,
        size: u64,
        capital: u64,
        opportunity_value: u64,
    ) -> Result<(), &'static str> {
        if size == 0 {
            return Err("Order size is zero");
        }
        
        if size > capital {
            return Err("Order exceeds available capital");
        }
        
        let estimated_return = (opportunity_value as f64 / size as f64) - 1.0;
        if estimated_return < 0.001 { // Less than 0.1% return
            return Err("Insufficient expected return");
        }
        
        Ok(())
    }

    pub fn update_order_result(&self, metrics: OrderMetrics) -> Result<(), &'static str> {
        let mut history = self.order_history.write().map_err(|_| "Lock poisoned")?;
        
        if history.len() >= SUCCESS_RATE_WINDOW {
            history.pop_front();
        }
        history.push_back(metrics.clone());
        
        self.update_success_rate(&history)?;
        self.update_volatility(&history)?;
        self.update_capital(metrics.profit_lamports)?;
        
        Ok(())
    }

    fn update_success_rate(&self, history: &VecDeque<OrderMetrics>) -> Result<(), &'static str> {
        if history.is_empty() {
            return Ok(());
        }
        
        let recent_count = history.len().min(100);
        let recent_success = history.iter()
            .rev()
            .take(recent_count)
            .filter(|m| m.success)
            .count() as f64;
        
        let new_rate = recent_success / recent_count as f64;
        let mut success_rate = self.success_rate_ema.write().map_err(|_| "Lock poisoned")?;
        
        *success_rate = *success_rate * 0.95 + new_rate * 0.05;
        
        Ok(())
    }

    fn update_volatility(&self, history: &VecDeque<OrderMetrics>) -> Result<(), &'static str> {
        if history.len() < 2 {
            return Ok(());
        }
        
        let returns: Vec<f64> = history.windows(2)
            .filter_map(|w| {
                                let r1 = w[0].profit_lamports as f64 / w[0].size_lamports as f64;
                let r2 = w[1].profit_lamports as f64 / w[1].size_lamports as f64;
                Some(r2 - r1)
            })
            .collect();
        
        if returns.is_empty() {
            return Ok(());
        }
        
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;
        
        let new_vol = variance.sqrt();
        let mut volatility = self.volatility_ema.write().map_err(|_| "Lock poisoned")?;
        
        *volatility = *volatility * VOLATILITY_DECAY + new_vol * (1.0 - VOLATILITY_DECAY);
        
        Ok(())
    }

    fn update_capital(&self, profit_lamports: i64) -> Result<(), &'static str> {
        let mut capital = self.capital_lamports.write().map_err(|_| "Lock poisoned")?;
        
        if profit_lamports < 0 && profit_lamports.abs() as u64 > *capital {
            *capital = MIN_ORDER_SIZE_LAMPORTS;
        } else {
            *capital = (*capital as i64 + profit_lamports).max(0) as u64;
        }
        
        Ok(())
    }

    pub fn update_market_conditions(
        &self,
        volatility_update: Option<f64>,
        liquidity_update: Option<u64>,
        competitor_update: Option<u32>,
        congestion_update: Option<f64>,
        gas_price: Option<u64>,
    ) -> Result<(), &'static str> {
        let mut conditions = self.market_conditions.write().map_err(|_| "Lock poisoned")?;
        
        if let Some(vol) = volatility_update {
            conditions.volatility = vol.max(0.001).min(1.0);
        }
        
        if let Some(liq) = liquidity_update {
            conditions.liquidity_depth = liq.max(MIN_ORDER_SIZE_LAMPORTS);
        }
        
        if let Some(comp) = competitor_update {
            conditions.competitor_count = comp.min(1000);
        }
        
        if let Some(cong) = congestion_update {
            conditions.network_congestion = cong.max(0.0).min(1.0);
        }
        
        if let Some(gas) = gas_price {
            if conditions.recent_gas_prices.len() >= 50 {
                conditions.recent_gas_prices.pop_front();
            }
            conditions.recent_gas_prices.push_back(gas);
        }
        
        *self.last_update.write().map_err(|_| "Lock poisoned")? = Instant::now();
        
        Ok(())
    }

    pub fn get_dynamic_priority_fee(&self) -> Result<u64, &'static str> {
        let conditions = self.market_conditions.read().map_err(|_| "Lock poisoned")?;
        
        if conditions.recent_gas_prices.is_empty() {
            return Ok(10_000); // Default 0.00001 SOL
        }
        
        let recent_10: Vec<u64> = conditions.recent_gas_prices.iter()
            .rev()
            .take(10)
            .copied()
            .collect();
        
        let p75 = self.calculate_percentile(&recent_10, 0.75);
        let congestion_multiplier = 1.0 + conditions.network_congestion;
        let competition_multiplier = 1.0 + (conditions.competitor_count as f64 / 50.0).min(1.0);
        
        let dynamic_fee = (p75 as f64 * congestion_multiplier * competition_multiplier) as u64;
        
        Ok(dynamic_fee.max(5_000).min(1_000_000)) // 0.000005 to 0.001 SOL
    }

    fn calculate_percentile(&self, values: &[u64], percentile: f64) -> u64 {
        if values.is_empty() {
            return 0;
        }
        
        let mut sorted = values.to_vec();
        sorted.sort_unstable();
        
        let index = ((sorted.len() - 1) as f64 * percentile) as usize;
        sorted[index]
    }

    pub fn should_skip_opportunity(
        &self,
        opportunity_value: u64,
        required_capital: u64,
    ) -> bool {
        let Ok(capital) = self.capital_lamports.read() else { return true };
        let Ok(conditions) = self.market_conditions.read() else { return true };
        let Ok(success_rate) = self.success_rate_ema.read() else { return true };
        
        if *capital < required_capital {
            return true;
        }
        
        let estimated_gas = self.estimate_transaction_cost(&conditions);
        let net_profit = opportunity_value.saturating_sub(estimated_gas);
        
        if net_profit < MIN_PROFIT_THRESHOLD {
            return true;
        }
        
        let profit_ratio = net_profit as f64 / required_capital as f64;
        let risk_adjusted_threshold = 0.001 * (1.0 + conditions.volatility * 5.0);
        
        if profit_ratio < risk_adjusted_threshold {
            return true;
        }
        
        if *success_rate < 0.4 && profit_ratio < 0.005 {
            return true;
        }
        
        false
    }

    pub fn get_compute_units_recommendation(&self) -> u32 {
        let Ok(conditions) = self.market_conditions.read() else {
            return BASE_COMPUTE_UNITS;
        };
        
        let congestion_factor = 1.0 + conditions.network_congestion * 0.5;
        let competition_factor = 1.0 + (conditions.competitor_count as f64 / 100.0).min(0.5);
        
        let recommended = (BASE_COMPUTE_UNITS as f64 * congestion_factor * competition_factor) as u32;
        
        recommended.min(1_400_000) // Solana max
    }

    pub fn get_retry_recommendation(&self) -> u8 {
        let Ok(conditions) = self.market_conditions.read() else {
            return 1;
        };
        
        if conditions.network_congestion > 0.8 {
            return 1; // Don't retry in extreme congestion
        }
        
        if conditions.network_congestion > 0.6 {
            return 2;
        }
        
        MAX_RETRY_ATTEMPTS
    }

    pub fn calculate_slippage_tolerance(
        &self,
        order_size: u64,
        liquidity: u64,
    ) -> f64 {
        let size_impact = (order_size as f64 / liquidity as f64).min(1.0);
        let base_slippage = SLIPPAGE_BUFFER;
        
        let Ok(volatility) = self.volatility_ema.read() else {
            return base_slippage * 2.0;
        };
        
        let volatility_adjustment = 1.0 + *volatility * 10.0;
        let size_adjustment = 1.0 + size_impact * 2.0;
        
        (base_slippage * volatility_adjustment * size_adjustment).min(0.02) // Max 2%
    }

    pub fn get_position_health(&self) -> Result<PositionHealth, &'static str> {
        let capital = *self.capital_lamports.read().map_err(|_| "Lock poisoned")?;
        let history = self.order_history.read().map_err(|_| "Lock poisoned")?;
        let success_rate = *self.success_rate_ema.read().map_err(|_| "Lock poisoned")?;
        let volatility = *self.volatility_ema.read().map_err(|_| "Lock poisoned")?;
        
        let recent_pnl = history.iter()
            .rev()
            .take(50)
            .map(|m| m.profit_lamports)
            .sum::<i64>();
        
        let max_drawdown = self.calculate_max_drawdown(&history);
        let sharpe_ratio = self.calculate_sharpe_ratio(&history);
        
        Ok(PositionHealth {
            capital_lamports: capital,
            recent_pnl_lamports: recent_pnl,
            success_rate,
            volatility,
            max_drawdown,
            sharpe_ratio,
            orders_count: history.len(),
        })
    }

    fn calculate_max_drawdown(&self, history: &VecDeque<OrderMetrics>) -> f64 {
        if history.is_empty() {
            return 0.0;
        }
        
        let mut peak = 0i64;
        let mut max_dd = 0.0;
        let mut cumulative = 0i64;
        
        for metric in history.iter() {
            cumulative += metric.profit_lamports;
            peak = peak.max(cumulative);
            
            if peak > 0 {
                let drawdown = (peak - cumulative) as f64 / peak as f64;
                max_dd = max_dd.max(drawdown);
            }
        }
        
        max_dd
    }

    fn calculate_sharpe_ratio(&self, history: &VecDeque<OrderMetrics>) -> f64 {
        if history.len() < 30 {
            return 0.0;
        }
        
        let returns: Vec<f64> = history.iter()
            .map(|m| m.profit_lamports as f64 / m.size_lamports as f64)
            .collect();
        
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / returns.len() as f64;
        
        let std_dev = variance.sqrt();
        
        if std_dev > 0.0 {
            let annualized_return = mean_return * 365.0;
            let annualized_std = std_dev * (365.0_f64).sqrt();
            (annualized_return - RISK_FREE_RATE.to_f64().unwrap_or(0.045)) / annualized_std
        } else {
            0.0
        }
    }
}

#[derive(Debug, Clone)]
pub struct PositionHealth {
    pub capital_lamports: u64,
    pub recent_pnl_lamports: i64,
    pub success_rate: f64,
    pub volatility: f64,
    pub max_drawdown: f64,
    pub sharpe_ratio: f64,
    pub orders_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_sizing_safety() {
        let sizer = AdaptiveOrderSizer::new(1_000_000_000); // 1 SOL
        
        let size = sizer.calculate_order_size(
            10_000_000, // 0.01 SOL opportunity
            0.8,
            &Pubkey::default(),
            100_000_000_000, // 100 SOL liquidity
        );
        
        assert!(size.is_ok());
        let size_value = size.unwrap();
        assert!(size_value >= MIN_ORDER_SIZE_LAMPORTS);
        assert!(size_value <= 1_000_000_000);
    }

    #[test]
    fn test_risk_constraints() {
        let sizer = AdaptiveOrderSizer::new(10_000_000_000); // 10 SOL
        
        let size = sizer.calculate_order_size(
            1_000_000_000, // 1 SOL opportunity
            0.95,
            &Pubkey::default(),
            1_000_000_000_000,
        );
        
        assert!(size.is_ok());
        let size_value = size.unwrap();
        assert!(size_value <= 10_000_000_000 * 0.25); // Max 25% position
    }
}

