use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use borsh::{BorshDeserialize, BorshSerialize};
use solana_sdk::pubkey::Pubkey;
use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};

const MAX_POSITION_RATIO: Decimal = dec!(0.15);
const MIN_POSITION_RATIO: Decimal = dec!(0.001);
const VAR_CONFIDENCE: f64 = 0.99;
const RISK_FREE_RATE: Decimal = dec!(0.05);
const MAX_CORRELATION: f64 = 0.95;
const MIN_LIQUIDITY_RATIO: Decimal = dec!(0.1);
const INVENTORY_UPDATE_INTERVAL: Duration = Duration::from_millis(100);
const RISK_CALC_INTERVAL: Duration = Duration::from_secs(1);
const MAX_LEVERAGE: Decimal = dec!(3.0);
const BASE_CAPITAL: u64 = 1_000_000_000_000;
const VOLATILITY_WINDOW: usize = 120;
const CORRELATION_WINDOW: usize = 60;
const STRESS_TEST_SCENARIOS: usize = 1000;
const MAX_DRAWDOWN: Decimal = dec!(0.25);
const SHARPE_TARGET: Decimal = dec!(2.5);

#[derive(Clone, Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct TokenMetrics {
    pub mint: Pubkey,
    pub balance: u64,
    pub value_usd: Decimal,
    pub volatility: f64,
    pub liquidity_score: Decimal,
    pub price_history: Vec<Decimal>,
    pub volume_24h: u64,
    pub market_depth: Decimal,
    pub spread_bps: Decimal,
    pub last_update: Instant,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RiskMetrics {
    pub total_exposure: Decimal,
    pub var_95: Decimal,
    pub var_99: Decimal,
    pub sharpe_ratio: Decimal,
    pub sortino_ratio: Decimal,
    pub max_drawdown: Decimal,
    pub current_leverage: Decimal,
    pub concentration_risk: Decimal,
    pub liquidity_risk: Decimal,
    pub correlation_risk: f64,
}

#[derive(Clone, Debug)]
pub struct PositionLimits {
    pub max_position_size: u64,
    pub min_position_size: u64,
    pub max_exposure: Decimal,
    pub stop_loss: Decimal,
    pub take_profit: Decimal,
    pub max_slippage_bps: u32,
    pub timeout_ms: u64,
}

#[derive(Clone, Debug)]
pub struct HedgeRecommendation {
    pub token: Pubkey,
    pub amount: u64,
    pub direction: TradeDirection,
    pub urgency: f64,
    pub expected_pnl: Decimal,
}

#[derive(Clone, Debug, PartialEq)]
pub enum TradeDirection {
    Buy,
    Sell,
}

pub struct InventoryRiskOptimizer {
    inventory: Arc<RwLock<HashMap<Pubkey, TokenMetrics>>>,
    risk_metrics: Arc<RwLock<RiskMetrics>>,
    capital_base: u64,
    risk_params: RiskParameters,
    correlation_matrix: Arc<RwLock<HashMap<(Pubkey, Pubkey), f64>>>,
    last_risk_calc: Arc<RwLock<Instant>>,
    performance_history: Arc<RwLock<Vec<Decimal>>>,
}

#[derive(Clone, Debug)]
struct RiskParameters {
    max_var: Decimal,
    target_sharpe: Decimal,
    max_concentration: Decimal,
    min_liquidity: Decimal,
    rebalance_threshold: Decimal,
    stress_multiplier: Decimal,
}

impl InventoryRiskOptimizer {
    pub fn new(capital_base: u64) -> Self {
        Self {
            inventory: Arc::new(RwLock::new(HashMap::new())),
            risk_metrics: Arc::new(RwLock::new(RiskMetrics {
                total_exposure: Decimal::ZERO,
                var_95: Decimal::ZERO,
                var_99: Decimal::ZERO,
                sharpe_ratio: Decimal::ZERO,
                sortino_ratio: Decimal::ZERO,
                max_drawdown: Decimal::ZERO,
                current_leverage: Decimal::ONE,
                concentration_risk: Decimal::ZERO,
                liquidity_risk: Decimal::ZERO,
                correlation_risk: 0.0,
            })),
            capital_base,
            risk_params: RiskParameters {
                max_var: dec!(0.05),
                target_sharpe: SHARPE_TARGET,
                max_concentration: dec!(0.3),
                min_liquidity: MIN_LIQUIDITY_RATIO,
                rebalance_threshold: dec!(0.02),
                stress_multiplier: dec!(2.5),
            },
            correlation_matrix: Arc::new(RwLock::new(HashMap::new())),
            last_risk_calc: Arc::new(RwLock::new(Instant::now())),
            performance_history: Arc::new(RwLock::new(Vec::with_capacity(1000))),
        }
    }

    pub fn update_inventory(&self, mint: Pubkey, balance: u64, price_usd: Decimal, volume_24h: u64) {
        let mut inventory = self.inventory.write().unwrap();
        
        if let Some(metrics) = inventory.get_mut(&mint) {
            metrics.balance = balance;
            metrics.value_usd = Decimal::from(balance) * price_usd / dec!(1_000_000_000);
            metrics.volume_24h = volume_24h;
            metrics.price_history.push(price_usd);
            if metrics.price_history.len() > VOLATILITY_WINDOW {
                metrics.price_history.remove(0);
            }
            metrics.volatility = self.calculate_volatility(&metrics.price_history);
            metrics.last_update = Instant::now();
        } else {
            let mut price_history = Vec::with_capacity(VOLATILITY_WINDOW);
            price_history.push(price_usd);
            
            inventory.insert(mint, TokenMetrics {
                mint,
                balance,
                value_usd: Decimal::from(balance) * price_usd / dec!(1_000_000_000),
                volatility: 0.0,
                liquidity_score: self.calculate_liquidity_score(volume_24h, price_usd),
                price_history,
                volume_24h,
                market_depth: Decimal::from(volume_24h) * price_usd / dec!(1_000_000_000),
                spread_bps: dec!(5),
                last_update: Instant::now(),
            });
        }
        
        drop(inventory);
        self.update_risk_metrics();
    }

    pub fn get_position_limits(&self, mint: &Pubkey, strategy_type: &str) -> PositionLimits {
        let inventory = self.inventory.read().unwrap();
        let risk_metrics = self.risk_metrics.read().unwrap();
        
        let token_metrics = inventory.get(mint);
        let base_size = self.capital_base / 100;
        
        let risk_multiplier = if risk_metrics.current_leverage > dec!(2.5) {
            dec!(0.5)
        } else if risk_metrics.current_leverage > dec!(2.0) {
            dec!(0.7)
        } else {
            dec!(1.0)
        };
        
        let volatility_adjustment = if let Some(metrics) = token_metrics {
            let vol_factor = 0.2 / (metrics.volatility + 0.01);
            Decimal::from_f64(vol_factor).unwrap_or(dec!(1.0))
        } else {
            dec!(0.5)
        };
        
        let strategy_multiplier = match strategy_type {
            "arbitrage" => dec!(1.5),
            "liquidation" => dec!(2.0),
            "sandwich" => dec!(0.8),
            "jit" => dec!(1.2),
            _ => dec!(1.0),
        };
        
        let max_position = (Decimal::from(base_size) * risk_multiplier * 
                           volatility_adjustment * strategy_multiplier)
                           .min(Decimal::from(self.capital_base) * MAX_POSITION_RATIO)
                           .to_u64().unwrap_or(base_size);
        
        let min_position = (Decimal::from(base_size) * MIN_POSITION_RATIO)
                          .to_u64().unwrap_or(base_size / 1000);
        
        let max_exposure = if let Some(metrics) = token_metrics {
            (metrics.value_usd * dec!(0.3)).min(Decimal::from(self.capital_base) * dec!(0.1))
        } else {
            Decimal::from(self.capital_base) * dec!(0.05)
        };
        
        let stop_loss = dec!(0.02) + (risk_metrics.var_99 * dec!(0.5));
        let take_profit = dec!(0.03) + (Decimal::from_f64(token_metrics
            .map(|m| m.volatility).unwrap_or(0.01)).unwrap_or(dec!(0.01)) * dec!(2));
        
        PositionLimits {
            max_position_size: max_position,
            min_position_size: min_position,
            max_exposure,
            stop_loss,
            take_profit,
            max_slippage_bps: 50,
            timeout_ms: 500,
        }
    }

    pub fn calculate_optimal_position_size(&self, mint: &Pubkey, opportunity_score: Decimal) -> u64 {
        let inventory = self.inventory.read().unwrap();
        let risk_metrics = self.risk_metrics.read().unwrap();
        
        let token_metrics = match inventory.get(mint) {
            Some(m) => m,
            None => return 0,
        };
        
        let kelly_fraction = self.calculate_kelly_criterion(
            opportunity_score,
            Decimal::from_f64(token_metrics.volatility).unwrap_or(dec!(0.1))
        );
        
        let risk_adjusted_fraction = kelly_fraction * 
            (dec!(1) - risk_metrics.current_leverage / MAX_LEVERAGE) *
            (dec!(1) - risk_metrics.concentration_risk);
        
        let liquidity_constraint = token_metrics.liquidity_score.min(dec!(1));
        let final_fraction = risk_adjusted_fraction * liquidity_constraint;
        
        let position_size = (Decimal::from(self.capital_base) * final_fraction)
            .to_u64().unwrap_or(0);
        
        let limits = self.get_position_limits(mint, "dynamic");
        position_size.max(limits.min_position_size).min(limits.max_position_size)
    }

    pub fn get_hedge_recommendations(&self) -> Vec<HedgeRecommendation> {
        let inventory = self.inventory.read().unwrap();
        let risk_metrics = self.risk_metrics.read().unwrap();
        let mut recommendations = Vec::new();
        
        if risk_metrics.concentration_risk > self.risk_params.max_concentration {
            for (mint, metrics) in inventory.iter() {
                let position_ratio = metrics.value_usd / Decimal::from(self.capital_base);
                
                if position_ratio > self.risk_params.max_concentration {
                    let excess = metrics.value_usd - 
                        (Decimal::from(self.capital_base) * self.risk_params.max_concentration);
                    let hedge_amount = (excess / metrics.value_usd * Decimal::from(metrics.balance))
                        .to_u64().unwrap_or(0);
                    
                    recommendations.push(HedgeRecommendation {
                        token: *mint,
                        amount: hedge_amount,
                        direction: TradeDirection::Sell,
                        urgency: (position_ratio / self.risk_params.max_concentration)
                            .to_f64().unwrap_or(1.0),
                        expected_pnl: -excess * dec!(0.001),
                    });
                }
            }
        }
        
        if risk_metrics.var_99 > self.risk_params.max_var {
            let reduction_factor = self.risk_params.max_var / risk_metrics.var_99;
            
            for (mint, metrics) in inventory.iter() {
                if metrics.volatility > 0.5 {
                    let hedge_amount = (Decimal::from(metrics.balance) * 
                        (dec!(1) - reduction_factor)).to_u64().unwrap_or(0);
                    
                    if hedge_amount > 0 {
                        recommendations.push(HedgeRecommendation {
                            token: *mint,
                            amount: hedge_amount,
                            direction: TradeDirection::Sell,
                            urgency: metrics.volatility / 0.5,
                            expected_pnl: Decimal::ZERO,
                        });
                    }
                }
            }
        }
        
        recommendations.sort_by(|a, b| b.urgency.partial_cmp(&a.urgency).unwrap());
        recommendations
    }

    pub fn should_take_position(&self, mint: &Pubkey, size: u64, direction: &TradeDirection) -> bool {
        let inventory = self.inventory.read().unwrap();
        let risk_metrics = self.risk_metrics.read().unwrap();
        
        if risk_metrics.current_leverage >= MAX_LEVERAGE {
            return false;
        }
        
        if risk_metrics.max_drawdown >= MAX_DRAWDOWN {
            return false;
        }
        
                let proposed_exposure = Decimal::from(size) / dec!(1_000_000_000);
        let new_total_exposure = risk_metrics.total_exposure + proposed_exposure;
        
        if new_total_exposure > Decimal::from(self.capital_base) * MAX_LEVERAGE {
            return false;
        }
        
        if let Some(token_metrics) = inventory.get(mint) {
            let new_position_value = match direction {
                TradeDirection::Buy => token_metrics.value_usd + proposed_exposure,
                TradeDirection::Sell => token_metrics.value_usd - proposed_exposure,
            };
            
            let new_concentration = new_position_value / Decimal::from(self.capital_base);
            if new_concentration > self.risk_params.max_concentration {
                return false;
            }
            
            let liquidity_check = proposed_exposure <= token_metrics.market_depth * dec!(0.02);
            if !liquidity_check {
                return false;
            }
            
            let spread_cost = proposed_exposure * token_metrics.spread_bps / dec!(10000);
            let min_expected_profit = spread_cost * dec!(3);
            
            true
        } else {
            proposed_exposure < Decimal::from(self.capital_base) * dec!(0.01)
        }
    }

    pub fn get_capital_allocation(&self) -> HashMap<String, Decimal> {
        let risk_metrics = self.risk_metrics.read().unwrap();
        let mut allocations = HashMap::new();
        
        let risk_budget = Decimal::from(self.capital_base) * 
            (dec!(1) - risk_metrics.current_leverage / MAX_LEVERAGE);
        
        let sharpe_adjustment = (risk_metrics.sharpe_ratio / SHARPE_TARGET)
            .max(dec!(0.5))
            .min(dec!(1.5));
        
        allocations.insert("arbitrage".to_string(), 
            risk_budget * dec!(0.4) * sharpe_adjustment);
        allocations.insert("liquidation".to_string(), 
            risk_budget * dec!(0.3) * (dec!(2) - sharpe_adjustment));
        allocations.insert("sandwich".to_string(), 
            risk_budget * dec!(0.2) * sharpe_adjustment.min(dec!(1)));
        allocations.insert("jit".to_string(), 
            risk_budget * dec!(0.1));
        
        allocations
    }

    pub fn update_performance(&self, pnl: Decimal) {
        let mut history = self.performance_history.write().unwrap();
        history.push(pnl);
        if history.len() > 1000 {
            history.remove(0);
        }
        drop(history);
        
        self.update_risk_metrics();
    }

    fn update_risk_metrics(&self) {
        let now = Instant::now();
        let mut last_calc = self.last_risk_calc.write().unwrap();
        
        if now.duration_since(*last_calc) < RISK_CALC_INTERVAL {
            return;
        }
        *last_calc = now;
        drop(last_calc);
        
        let inventory = self.inventory.read().unwrap();
        let mut risk_metrics = self.risk_metrics.write().unwrap();
        
        let total_value: Decimal = inventory.values()
            .map(|m| m.value_usd)
            .sum();
        
        risk_metrics.total_exposure = total_value;
        risk_metrics.current_leverage = total_value / Decimal::from(self.capital_base);
        
        let mut position_values: Vec<(Pubkey, Decimal, f64)> = inventory.iter()
            .map(|(k, v)| (*k, v.value_usd, v.volatility))
            .collect();
        
        risk_metrics.var_95 = self.calculate_var(&position_values, 0.95);
        risk_metrics.var_99 = self.calculate_var(&position_values, 0.99);
        
        let max_position = position_values.iter()
            .map(|(_, v, _)| *v)
            .max()
            .unwrap_or(Decimal::ZERO);
        risk_metrics.concentration_risk = max_position / total_value.max(dec!(1));
        
        let total_liquidity: Decimal = inventory.values()
            .map(|m| m.liquidity_score * m.value_usd)
            .sum();
        risk_metrics.liquidity_risk = dec!(1) - (total_liquidity / total_value.max(dec!(1))).min(dec!(1));
        
        let performance = self.performance_history.read().unwrap();
        if performance.len() > 30 {
            risk_metrics.sharpe_ratio = self.calculate_sharpe_ratio(&performance);
            risk_metrics.sortino_ratio = self.calculate_sortino_ratio(&performance);
            risk_metrics.max_drawdown = self.calculate_max_drawdown(&performance);
        }
        
        risk_metrics.correlation_risk = self.calculate_portfolio_correlation(&position_values);
    }

    fn calculate_var(&self, positions: &[(Pubkey, Decimal, f64)], confidence: f64) -> Decimal {
        if positions.is_empty() {
            return Decimal::ZERO;
        }
        
        let correlations = self.correlation_matrix.read().unwrap();
        let mut portfolio_variance = 0.0;
        
        for (i, (mint1, value1, vol1)) in positions.iter().enumerate() {
            let weight1 = value1.to_f64().unwrap_or(0.0) / self.capital_base as f64;
            portfolio_variance += weight1 * weight1 * vol1 * vol1;
            
            for (j, (mint2, value2, vol2)) in positions.iter().enumerate().skip(i + 1) {
                let weight2 = value2.to_f64().unwrap_or(0.0) / self.capital_base as f64;
                let correlation = correlations.get(&(*mint1, *mint2))
                    .or_else(|| correlations.get(&(*mint2, *mint1)))
                    .copied()
                    .unwrap_or(0.3);
                
                portfolio_variance += 2.0 * weight1 * weight2 * vol1 * vol2 * correlation;
            }
        }
        
        let portfolio_volatility = portfolio_variance.sqrt();
        let z_score = match confidence {
            c if c >= 0.99 => 2.326,
            c if c >= 0.95 => 1.645,
            _ => 1.282,
        };
        
        Decimal::from_f64(portfolio_volatility * z_score * (self.capital_base as f64))
            .unwrap_or(Decimal::ZERO)
    }

    fn calculate_kelly_criterion(&self, win_rate: Decimal, volatility: Decimal) -> Decimal {
        let b = dec!(1);
        let p = win_rate;
        let q = dec!(1) - p;
        
        let kelly = (p * b - q) / (b * volatility.max(dec!(0.01)));
        kelly.max(Decimal::ZERO).min(dec!(0.25))
    }

    fn calculate_volatility(&self, prices: &[Decimal]) -> f64 {
        if prices.len() < 2 {
            return 0.01;
        }
        
        let returns: Vec<f64> = prices.windows(2)
            .map(|w| ((w[1] / w[0]) - dec!(1)).to_f64().unwrap_or(0.0))
            .collect();
        
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / (returns.len() - 1) as f64;
        
        (variance.sqrt() * (365.25_f64).sqrt()).min(2.0).max(0.001)
    }

    fn calculate_liquidity_score(&self, volume_24h: u64, price: Decimal) -> Decimal {
        let volume_usd = Decimal::from(volume_24h) * price / dec!(1_000_000_000);
        let capital_decimal = Decimal::from(self.capital_base) / dec!(1_000_000_000);
        
        (volume_usd / capital_decimal / dec!(100))
            .min(dec!(1))
            .max(dec!(0.01))
    }

    fn calculate_sharpe_ratio(&self, returns: &[Decimal]) -> Decimal {
        if returns.is_empty() {
            return Decimal::ZERO;
        }
        
        let mean_return = returns.iter().sum::<Decimal>() / Decimal::from(returns.len());
        let variance = returns.iter()
            .map(|r| (*r - mean_return).powi(2))
            .sum::<Decimal>() / Decimal::from(returns.len() - 1);
        
        let std_dev = variance.sqrt().unwrap_or(dec!(0.01));
        let excess_return = mean_return - (RISK_FREE_RATE / dec!(365));
        
        (excess_return / std_dev * dec!(365).sqrt().unwrap_or(dec!(19.1)))
            .max(dec!(-3))
            .min(dec!(5))
    }

    fn calculate_sortino_ratio(&self, returns: &[Decimal]) -> Decimal {
        if returns.is_empty() {
            return Decimal::ZERO;
        }
        
        let mean_return = returns.iter().sum::<Decimal>() / Decimal::from(returns.len());
        let downside_returns: Vec<Decimal> = returns.iter()
            .filter(|&&r| r < Decimal::ZERO)
            .copied()
            .collect();
        
        if downside_returns.is_empty() {
            return dec!(5);
        }
        
        let downside_variance = downside_returns.iter()
            .map(|r| r.powi(2))
            .sum::<Decimal>() / Decimal::from(downside_returns.len());
        
        let downside_deviation = downside_variance.sqrt().unwrap_or(dec!(0.01));
        let excess_return = mean_return - (RISK_FREE_RATE / dec!(365));
        
        (excess_return / downside_deviation * dec!(365).sqrt().unwrap_or(dec!(19.1)))
            .max(dec!(-3))
            .min(dec!(5))
    }

    fn calculate_max_drawdown(&self, returns: &[Decimal]) -> Decimal {
        if returns.is_empty() {
            return Decimal::ZERO;
        }
        
        let mut cumulative = Decimal::ONE;
        let mut peak = Decimal::ONE;
        let mut max_dd = Decimal::ZERO;
        
        for ret in returns {
            cumulative *= dec!(1) + ret;
            if cumulative > peak {
                peak = cumulative;
            }
            let drawdown = (peak - cumulative) / peak;
            if drawdown > max_dd {
                max_dd = drawdown;
            }
        }
        
        max_dd
    }

    fn calculate_portfolio_correlation(&self, positions: &[(Pubkey, Decimal, f64)]) -> f64 {
        if positions.len() < 2 {
            return 0.0;
        }
        
        let correlations = self.correlation_matrix.read().unwrap();
        let total_value = positions.iter().map(|(_, v, _)| v).sum::<Decimal>();
        
        let mut weighted_correlation = 0.0;
        let mut total_weight = 0.0;
        
        for (i, (mint1, value1, _)) in positions.iter().enumerate() {
            for (j, (mint2, value2, _)) in positions.iter().enumerate().skip(i + 1) {
                let weight = (value1 * value2 / total_value.powi(2)).to_f64().unwrap_or(0.0);
                let corr = correlations.get(&(*mint1, *mint2))
                    .or_else(|| correlations.get(&(*mint2, *mint1)))
                    .copied()
                    .unwrap_or(0.3);
                
                weighted_correlation += weight * corr;
                total_weight += weight;
            }
        }
        
        if total_weight > 0.0 {
            (weighted_correlation / total_weight).min(MAX_CORRELATION).max(-MAX_CORRELATION)
        } else {
            0.0
        }
    }

    pub fn update_correlation(&self, mint1: Pubkey, mint2: Pubkey, correlation: f64) {
        let mut matrix = self.correlation_matrix.write().unwrap();
        matrix.insert((mint1, mint2), correlation.min(MAX_CORRELATION).max(-MAX_CORRELATION));
    }

    pub fn get_risk_adjusted_size(&self, base_size: u64, mint: &Pubkey) -> u64 {
        let risk_metrics = self.risk_metrics.read().unwrap();
        let inventory = self.inventory.read().unwrap();
        
        let risk_multiplier = (dec!(1) - risk_metrics.var_99 / self.risk_params.max_var)
            .max(dec!(0.1))
            .min(dec!(1));
        
        let concentration_multiplier = (dec!(1) - risk_metrics.concentration_risk)
            .max(dec!(0.2))
            .min(dec!(1));
        
        let token_specific_multiplier = if let Some(metrics) = inventory.get(mint) {
            (dec!(1) / (dec!(1) + Decimal::from_f64(metrics.volatility).unwrap_or(dec!(0.1))))
                .max(dec!(0.3))
                .min(dec!(1))
        } else {
            dec!(0.5)
        };
        
        (Decimal::from(base_size) * risk_multiplier * concentration_multiplier * 
         token_specific_multiplier).to_u64().unwrap_or(base_size / 2)
    }

    pub fn emergency_risk_check(&self) -> bool {
        let risk_metrics = self.risk_metrics.read().unwrap();
        
        risk_metrics.current_leverage < MAX_LEVERAGE * dec!(0.9) &&
        risk_metrics.var_99 < self.risk_params.max_var * dec!(0.9) &&
        risk_metrics.max_drawdown < MAX_DRAWDOWN * dec!(0.9) &&
        risk_metrics.liquidity_risk < dec!(0.5)
    }
}
        risk_metrics.current_leverage < MAX_LEVERAGE * dec!(0.9) &&
        risk_metrics.var_99 < self.risk_params.max_var * dec!(0.9) &&
        risk_metrics.max_drawdown < MAX_DRAWDOWN * dec!(0.9) &&
        risk_metrics.liquidity_risk < dec!(0.5)
    }

    pub fn stress_test_portfolio(&self) -> Vec<(String, Decimal)> {
        let inventory = self.inventory.read().unwrap();
        let mut stress_results = Vec::new();
        
        // Market crash scenario
        let crash_loss = inventory.values()
            .map(|m| m.value_usd * dec!(0.3) * Decimal::from_f64(m.volatility * 2.0).unwrap_or(dec!(1)))
            .sum::<Decimal>();
        stress_results.push(("market_crash_30%".to_string(), -crash_loss));
        
        // Liquidity crisis
        let liquidity_impact = inventory.values()
            .map(|m| m.value_usd * (dec!(1) - m.liquidity_score) * dec!(0.15))
            .sum::<Decimal>();
        stress_results.push(("liquidity_crisis".to_string(), -liquidity_impact));
        
        // Correlation breakdown
        let correlation_loss = self.simulate_correlation_stress();
        stress_results.push(("correlation_stress".to_string(), correlation_loss));
        
        // Flash crash recovery
        let flash_crash_pnl = inventory.values()
            .map(|m| {
                let down = m.value_usd * dec!(0.5);
                let recovery = down * m.liquidity_score * dec!(0.7);
                recovery - down
            })
            .sum::<Decimal>();
        stress_results.push(("flash_crash_recovery".to_string(), flash_crash_pnl));
        
        // Regulatory shock
        let regulatory_impact = -Decimal::from(self.capital_base) * dec!(0.1);
        stress_results.push(("regulatory_shock".to_string(), regulatory_impact));
        
        stress_results
    }

    fn simulate_correlation_stress(&self) -> Decimal {
        let inventory = self.inventory.read().unwrap();
        let positions: Vec<(Pubkey, Decimal, f64)> = inventory.iter()
            .map(|(k, v)| (*k, v.value_usd, v.volatility))
            .collect();
        
        if positions.len() < 2 {
            return Decimal::ZERO;
        }
        
        let mut max_loss = Decimal::ZERO;
        
        for _ in 0..100 {
            let scenario_loss = positions.iter()
                .map(|(_, value, vol)| {
                    let shock = Decimal::from_f64(vol * 3.0 * rand::random::<f64>())
                        .unwrap_or(dec!(0.1));
                    value * shock * dec!(0.5)
                })
                .sum::<Decimal>();
            
            if scenario_loss > max_loss {
                max_loss = scenario_loss;
            }
        }
        
        -max_loss
    }

    pub fn rebalance_recommendations(&self) -> Vec<(Pubkey, i64, String)> {
        let inventory = self.inventory.read().unwrap();
        let allocations = self.get_capital_allocation();
        let mut recommendations = Vec::new();
        
        let total_value = inventory.values()
            .map(|m| m.value_usd)
            .sum::<Decimal>();
        
        if total_value == Decimal::ZERO {
            return recommendations;
        }
        
        for (mint, metrics) in inventory.iter() {
            let current_weight = metrics.value_usd / total_value;
            let target_weight = self.calculate_target_weight(mint, &metrics);
            let weight_diff = target_weight - current_weight;
            
            if weight_diff.abs() > self.risk_params.rebalance_threshold {
                let rebalance_value = weight_diff * total_value;
                let rebalance_amount = (rebalance_value / metrics.value_usd * 
                    Decimal::from(metrics.balance)).to_i64().unwrap_or(0);
                
                let reason = if weight_diff > Decimal::ZERO {
                    format!("Underweight by {:.2}%", (weight_diff * dec!(100)).to_f64().unwrap_or(0.0))
                } else {
                    format!("Overweight by {:.2}%", (weight_diff.abs() * dec!(100)).to_f64().unwrap_or(0.0))
                };
                
                recommendations.push((*mint, rebalance_amount, reason));
            }
        }
        
        recommendations.sort_by_key(|(_, amount, _)| amount.abs());
        recommendations.reverse();
        recommendations
    }

    fn calculate_target_weight(&self, mint: &Pubkey, metrics: &TokenMetrics) -> Decimal {
        let base_weight = dec!(1) / dec!(10);
        
        let vol_adjustment = dec!(1) / (dec!(1) + Decimal::from_f64(metrics.volatility).unwrap_or(dec!(0.1)));
        let liquidity_adjustment = metrics.liquidity_score.min(dec!(1));
        let spread_adjustment = dec!(1) - (metrics.spread_bps / dec!(1000)).min(dec!(0.5));
        
        let risk_score = vol_adjustment * liquidity_adjustment * spread_adjustment;
        let normalized_weight = base_weight * risk_score;
        
        normalized_weight.min(self.risk_params.max_concentration).max(dec!(0.01))
    }

    pub fn get_execution_params(&self, mint: &Pubkey, size: u64) -> ExecutionParameters {
        let inventory = self.inventory.read().unwrap();
        let risk_metrics = self.risk_metrics.read().unwrap();
        
        let urgency_factor = risk_metrics.current_leverage / MAX_LEVERAGE;
        let volatility_factor = inventory.get(mint)
            .map(|m| Decimal::from_f64(m.volatility).unwrap_or(dec!(0.1)))
            .unwrap_or(dec!(0.1));
        
        let max_slippage_bps = (50.0 * (1.0 + urgency_factor.to_f64().unwrap_or(0.0) + 
            volatility_factor.to_f64().unwrap_or(0.0))) as u32;
        
        let chunk_size = if let Some(metrics) = inventory.get(mint) {
            let liquidity_constraint = (metrics.market_depth * dec!(0.01))
                .to_u64().unwrap_or(size / 10);
            size.min(liquidity_constraint).max(size / 20)
        } else {
            size / 10
        };
        
        let time_limit_ms = if urgency_factor > dec!(0.8) {
            200
        } else if urgency_factor > dec!(0.5) {
            500
        } else {
            1000
        };
        
        ExecutionParameters {
            max_slippage_bps: max_slippage_bps.min(200),
            chunk_size,
            chunk_delay_ms: 50,
            time_limit_ms,
            retry_attempts: 3,
            gas_price_multiplier: 1.0 + urgency_factor.to_f64().unwrap_or(0.0) * 0.5,
        }
    }

    pub fn calculate_portfolio_metrics(&self) -> PortfolioMetrics {
        let inventory = self.inventory.read().unwrap();
        let risk_metrics = self.risk_metrics.read().unwrap();
        let performance = self.performance_history.read().unwrap();
        
        let total_value = inventory.values()
            .map(|m| m.value_usd)
            .sum::<Decimal>();
        
        let position_count = inventory.len();
        let avg_position_size = if position_count > 0 {
            total_value / Decimal::from(position_count)
        } else {
            Decimal::ZERO
        };
        
        let total_volume_24h = inventory.values()
            .map(|m| m.volume_24h)
            .sum::<u64>();
        
        let win_rate = if !performance.is_empty() {
            let wins = performance.iter().filter(|&&p| p > Decimal::ZERO).count();
            Decimal::from(wins) / Decimal::from(performance.len())
        } else {
            Decimal::ZERO
        };
        
        let avg_win = if !performance.is_empty() {
            let wins: Vec<&Decimal> = performance.iter().filter(|&&p| p > Decimal::ZERO).collect();
            if !wins.is_empty() {
                wins.iter().copied().sum::<Decimal>() / Decimal::from(wins.len())
            } else {
                Decimal::ZERO
            }
        } else {
            Decimal::ZERO
        };
        
        let avg_loss = if !performance.is_empty() {
            let losses: Vec<&Decimal> = performance.iter().filter(|&&p| p < Decimal::ZERO).collect();
            if !losses.is_empty() {
                losses.iter().copied().sum::<Decimal>() / Decimal::from(losses.len())
            } else {
                Decimal::ZERO
            }
        } else {
            Decimal::ZERO
        };
        
        let profit_factor = if avg_loss != Decimal::ZERO {
            (avg_win * win_rate) / (avg_loss.abs() * (dec!(1) - win_rate))
        } else {
            Decimal::ZERO
        };
        
        PortfolioMetrics {
            total_value,
            position_count,
            avg_position_size,
            total_volume_24h,
            win_rate,
            profit_factor,
            risk_reward_ratio: if avg_loss != Decimal::ZERO { avg_win / avg_loss.abs() } else { Decimal::ZERO },
            sharpe_ratio: risk_metrics.sharpe_ratio,
            sortino_ratio: risk_metrics.sortino_ratio,
            max_drawdown: risk_metrics.max_drawdown,
            current_leverage: risk_metrics.current_leverage,
            var_99: risk_metrics.var_99,
        }
    }
}

#[derive(Clone, Debug)]
pub struct ExecutionParameters {
    pub max_slippage_bps: u32,
    pub chunk_size: u64,
    pub chunk_delay_ms: u64,
    pub time_limit_ms: u64,
    pub retry_attempts: u8,
    pub gas_price_multiplier: f64,
}

#[derive(Clone, Debug)]
pub struct PortfolioMetrics {
    pub total_value: Decimal,
    pub position_count: usize,
    pub avg_position_size: Decimal,
    pub total_volume_24h: u64,
    pub win_rate: Decimal,
    pub profit_factor: Decimal,
    pub risk_reward_ratio: Decimal,
    pub sharpe_ratio: Decimal,
    pub sortino_ratio: Decimal,
    pub max_drawdown: Decimal,
    pub current_leverage: Decimal,
    pub var_99: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_risk_optimizer_initialization() {
        let optimizer = InventoryRiskOptimizer::new(BASE_CAPITAL);
        assert!(optimizer.emergency_risk_check());
    }

    #[test]
    fn test_position_limits() {
        let optimizer = InventoryRiskOptimizer::new(BASE_CAPITAL);
        let mint = Pubkey::new_unique();
        let limits = optimizer.get_position_limits(&mint, "arbitrage");
        assert!(limits.max_position_size > limits.min_position_size);
        assert!(limits.stop_loss > Decimal::ZERO);
        assert!(limits.take_profit > limits.stop_loss);
    }
}

