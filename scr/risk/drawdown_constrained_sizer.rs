use std::sync::{Arc, RwLock};
use std::collections::VecDeque;
use std::time::{Duration, Instant};
use solana_sdk::pubkey::Pubkey;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;

const MAX_DRAWDOWN_THRESHOLD: Decimal = dec!(0.20); // 20% max drawdown
const EMERGENCY_DRAWDOWN: Decimal = dec!(0.30); // 30% emergency stop
const MIN_POSITION_SIZE: u64 = 100_000; // 0.0001 SOL minimum
const MAX_POSITION_MULTIPLIER: Decimal = dec!(0.25); // Max 25% of portfolio
const VOLATILITY_WINDOW: usize = 100;
const KELLY_SAFETY_FACTOR: Decimal = dec!(0.25); // 25% Kelly for safety
const BASE_RISK_FACTOR: Decimal = dec!(0.02); // 2% base risk
const DRAWDOWN_RECOVERY_FACTOR: Decimal = dec!(0.85); // 85% confidence for recovery
const MIN_EDGE_THRESHOLD: Decimal = dec!(0.001); // 0.1% minimum edge
const COMPUTE_UNIT_BUFFER: u32 = 200_000; // Buffer for CU estimation
const PRIORITY_FEE_MULTIPLIER: Decimal = dec!(1.5); // 1.5x priority fee buffer

#[derive(Debug, Clone)]
pub struct PortfolioMetrics {
    pub current_value: Decimal,
    pub peak_value: Decimal,
    pub drawdown: Decimal,
    pub timestamp: Instant,
    pub win_rate: Decimal,
    pub avg_win: Decimal,
    pub avg_loss: Decimal,
    pub sharpe_ratio: Decimal,
    pub volatility: Decimal,
}

#[derive(Debug, Clone)]
pub struct TradeMetrics {
    pub profit_loss: Decimal,
    pub timestamp: Instant,
    pub size: u64,
    pub edge: Decimal,
    pub success: bool,
}

#[derive(Debug, Clone)]
pub struct RiskParameters {
    pub max_position_size: u64,
    pub min_position_size: u64,
    pub max_drawdown: Decimal,
    pub position_limit: u32,
    pub daily_loss_limit: Decimal,
    pub volatility_scalar: Decimal,
    pub confidence_threshold: Decimal,
}

impl Default for RiskParameters {
    fn default() -> Self {
        Self {
            max_position_size: 1_000_000_000_000, // 1000 SOL
            min_position_size: MIN_POSITION_SIZE,
            max_drawdown: MAX_DRAWDOWN_THRESHOLD,
            position_limit: 10,
            daily_loss_limit: dec!(0.05), // 5% daily loss limit
            volatility_scalar: dec!(2.0),
            confidence_threshold: dec!(0.65),
        }
    }
}

pub struct DrawdownConstrainedSizer {
    portfolio_metrics: Arc<RwLock<PortfolioMetrics>>,
    trade_history: Arc<RwLock<VecDeque<TradeMetrics>>>,
    risk_params: Arc<RwLock<RiskParameters>>,
    volatility_buffer: Arc<RwLock<VecDeque<Decimal>>>,
    daily_pnl: Arc<RwLock<Decimal>>,
    last_reset: Arc<RwLock<Instant>>,
    active_positions: Arc<RwLock<u32>>,
    emergency_mode: Arc<RwLock<bool>>,
}

impl DrawdownConstrainedSizer {
    pub fn new(initial_capital: Decimal) -> Self {
        let portfolio_metrics = PortfolioMetrics {
            current_value: initial_capital,
            peak_value: initial_capital,
            drawdown: Decimal::ZERO,
            timestamp: Instant::now(),
            win_rate: dec!(0.5),
            avg_win: Decimal::ZERO,
            avg_loss: Decimal::ZERO,
            sharpe_ratio: Decimal::ZERO,
            volatility: dec!(0.02),
        };

        Self {
            portfolio_metrics: Arc::new(RwLock::new(portfolio_metrics)),
            trade_history: Arc::new(RwLock::new(VecDeque::with_capacity(1000))),
            risk_params: Arc::new(RwLock::new(RiskParameters::default())),
            volatility_buffer: Arc::new(RwLock::new(VecDeque::with_capacity(VOLATILITY_WINDOW))),
            daily_pnl: Arc::new(RwLock::new(Decimal::ZERO)),
            last_reset: Arc::new(RwLock::new(Instant::now())),
            active_positions: Arc::new(RwLock::new(0)),
            emergency_mode: Arc::new(RwLock::new(false)),
        }
    }

    pub fn calculate_position_size(
        &self,
        opportunity_edge: Decimal,
        confidence: Decimal,
        market_volatility: Decimal,
        priority_fee: u64,
        compute_units: u32,
    ) -> Result<u64, &'static str> {
        // Check emergency mode
        if *self.emergency_mode.read().unwrap() {
            return Ok(0);
        }

        // Validate inputs
        if opportunity_edge < MIN_EDGE_THRESHOLD || confidence <= Decimal::ZERO || confidence > Decimal::ONE {
            return Ok(0);
        }

        // Check daily reset
        self.check_daily_reset();

        // Get current metrics
        let metrics = self.portfolio_metrics.read().unwrap();
        let risk_params = self.risk_params.read().unwrap();
        let daily_pnl = *self.daily_pnl.read().unwrap();
        let active_positions = *self.active_positions.read().unwrap();

        // Check position limits
        if active_positions >= risk_params.position_limit {
            return Ok(0);
        }

        // Check daily loss limit
        if daily_pnl < -risk_params.daily_loss_limit * metrics.current_value {
            return Ok(0);
        }

        // Calculate drawdown-adjusted size
        let base_size = self.calculate_kelly_size(
            opportunity_edge,
            confidence,
            metrics.win_rate,
            metrics.avg_win,
            metrics.avg_loss,
            metrics.current_value,
        );

        // Apply drawdown constraints
        let drawdown_multiplier = self.calculate_drawdown_multiplier(metrics.drawdown);
        
        // Apply volatility adjustment
        let volatility_adjustment = self.calculate_volatility_adjustment(
            market_volatility,
            metrics.volatility,
        );

        // Calculate final size
        let mut position_size = (base_size * drawdown_multiplier * volatility_adjustment)
            .round()
            .to_u64()
            .unwrap_or(0);

        // Apply transaction cost adjustment
        position_size = self.adjust_for_transaction_costs(
            position_size,
            priority_fee,
            compute_units,
            opportunity_edge,
        );

        // Apply risk limits
        position_size = position_size
            .min(risk_params.max_position_size)
            .max(if position_size > 0 { risk_params.min_position_size } else { 0 });

        // Final portfolio constraint
        let max_allowed = (metrics.current_value * MAX_POSITION_MULTIPLIER)
            .round()
            .to_u64()
            .unwrap_or(0);
        
        position_size = position_size.min(max_allowed);

        // Update active positions if taking position
        if position_size > 0 {
            *self.active_positions.write().unwrap() += 1;
        }

        Ok(position_size)
    }

    fn calculate_kelly_size(
        &self,
        edge: Decimal,
        confidence: Decimal,
        win_rate: Decimal,
        avg_win: Decimal,
        avg_loss: Decimal,
        portfolio_value: Decimal,
    ) -> Decimal {
        // Adjusted Kelly Criterion with confidence weighting
        let effective_win_rate = win_rate * confidence + (Decimal::ONE - confidence) * dec!(0.5);
        
        let b = if avg_loss > Decimal::ZERO {
            avg_win / avg_loss
        } else {
            dec!(1.5) // Default odds
        };

        let kelly_fraction = if b > Decimal::ZERO {
            (effective_win_rate * b - (Decimal::ONE - effective_win_rate)) / b
        } else {
            Decimal::ZERO
        };

        // Apply safety factor and edge adjustment
        let adjusted_kelly = kelly_fraction * KELLY_SAFETY_FACTOR * (Decimal::ONE + edge);
        
        // Base position size
        let base_size = portfolio_value * adjusted_kelly.max(Decimal::ZERO).min(dec!(0.10));
        
        base_size * confidence
    }

    fn calculate_drawdown_multiplier(&self, current_drawdown: Decimal) -> Decimal {
        if current_drawdown >= EMERGENCY_DRAWDOWN {
            *self.emergency_mode.write().unwrap() = true;
            return Decimal::ZERO;
        }

        if current_drawdown >= MAX_DRAWDOWN_THRESHOLD {
            return dec!(0.1); // Minimal sizing at max drawdown
        }

        // Progressive reduction based on drawdown
        let drawdown_ratio = current_drawdown / MAX_DRAWDOWN_THRESHOLD;
        let multiplier = Decimal::ONE - (drawdown_ratio * DRAWDOWN_RECOVERY_FACTOR);
        
        multiplier.max(dec!(0.1))
    }

    fn calculate_volatility_adjustment(
        &self,
        market_volatility: Decimal,
        portfolio_volatility: Decimal,
    ) -> Decimal {
        let vol_ratio = if portfolio_volatility > Decimal::ZERO {
            market_volatility / portfolio_volatility
        } else {
            Decimal::ONE
        };

        // Inverse volatility sizing
        let adjustment = Decimal::ONE / (Decimal::ONE + vol_ratio);
        
        adjustment.max(dec!(0.5)).min(dec!(1.5))
    }

    fn adjust_for_transaction_costs(
        &self,
        position_size: u64,
        priority_fee: u64,
        compute_units: u32,
        edge: Decimal,
    ) -> u64 {
        let total_cu = compute_units + COMPUTE_UNIT_BUFFER;
        let estimated_fee = (priority_fee as i64 * total_cu as i64) / 1_000_000;
        let total_cost = Decimal::from(estimated_fee) * PRIORITY_FEE_MULTIPLIER;
        
        let min_profitable_size = (total_cost / edge)
            .round()
            .to_u64()
            .unwrap_or(u64::MAX);

        if position_size < min_profitable_size {
            0
        } else {
            position_size
        }
    }

    pub fn update_portfolio_value(&self, new_value: Decimal) {
        let mut metrics = self.portfolio_metrics.write().unwrap();
        
        metrics.current_value = new_value;
        metrics.timestamp = Instant::now();
        
        // Update peak and drawdown
        if new_value > metrics.peak_value {
            metrics.peak_value = new_value;
            metrics.drawdown = Decimal::ZERO;
            *self.emergency_mode.write().unwrap() = false;
        } else {
            metrics.drawdown = (metrics.peak_value - new_value) / metrics.peak_value;
        }

        // Update volatility buffer
        let mut vol_buffer = self.volatility_buffer.write().unwrap();
        let returns = if metrics.current_value > Decimal::ZERO {
            (new_value - metrics.current_value) / metrics.current_value
        } else {
            Decimal::ZERO
        };
        
        vol_buffer.push_back(returns);
        if vol_buffer.len() > VOLATILITY_WINDOW {
            vol_buffer.pop_front();
        }

        // Calculate rolling volatility
        if vol_buffer.len() >= 20 {
            let mean = vol_buffer.iter().sum::<Decimal>() / Decimal::from(vol_buffer.len());
            let variance = vol_buffer.iter()
                .map(|r| (*r - mean).powi(2))
                .sum::<Decimal>() / Decimal::from(vol_buffer.len());
            
            metrics.volatility = variance.sqrt();
        }
    }

    pub fn record_trade(&self, trade: TradeMetrics) {
        let mut history = self.trade_history.write().unwrap();
        let mut metrics = self.portfolio_metrics.write().unwrap();
        let mut daily_pnl = self.daily_pnl.write().unwrap();

        // Update daily PnL
        *daily_pnl += trade.profit_loss;

        // Add to history
        history.push_back(trade.clone());
        if history.len() > 1000 {
            history.pop_front();
        }

        // Update metrics
        let recent_trades: Vec<_> = history.iter()
            .filter(|t| t.timestamp.elapsed() < Duration::from_secs(86400))
            .cloned()
            .collect();

        if !recent_trades.is_empty() {
            let wins: Vec<_> = recent_trades.iter()
                .filter(|t| t.success)
                .collect();
            
            let losses: Vec<_> = recent_trades.iter()
                .filter(|t| !t.success)
                .collect();

            metrics.win_rate = Decimal::from(wins.len()) / Decimal::from(recent_trades.len());
            
            if !wins.is_empty() {
                metrics.avg_win = wins.iter()
                    .map(|t| t.profit_loss.abs())
                    .sum::<Decimal>() / Decimal::from(wins.len());
            }

            if !losses.is_empty() {
                metrics.avg_loss = losses.iter()
                    .map(|t| t.profit_loss.abs())
                    .sum::<Decimal>() / Decimal::from(losses.len());
            }

                        // Update Sharpe ratio
            self.calculate_sharpe_ratio(&recent_trades, &mut metrics);
        }

        // Release position slot if trade closed
        if trade.profit_loss != Decimal::ZERO {
            let mut active = self.active_positions.write().unwrap();
            if *active > 0 {
                *active -= 1;
            }
        }
    }

    fn calculate_sharpe_ratio(&self, trades: &[TradeMetrics], metrics: &mut PortfolioMetrics) {
        if trades.len() < 10 {
            return;
        }

        let returns: Vec<Decimal> = trades.iter()
            .map(|t| t.profit_loss / Decimal::from(t.size))
            .collect();

        let mean_return = returns.iter().sum::<Decimal>() / Decimal::from(returns.len());
        let std_dev = {
            let variance = returns.iter()
                .map(|r| (*r - mean_return).powi(2))
                .sum::<Decimal>() / Decimal::from(returns.len());
            variance.sqrt()
        };

        if std_dev > Decimal::ZERO {
            // Annualized Sharpe (assuming ~1000 trades per day on Solana)
            metrics.sharpe_ratio = (mean_return * dec!(1000) * dec!(365)).checked_div(std_dev)
                .unwrap_or(Decimal::ZERO)
                .round_dp(4);
        }
    }

    fn check_daily_reset(&self) {
        let mut last_reset = self.last_reset.write().unwrap();
        
        if last_reset.elapsed() >= Duration::from_secs(86400) {
            *self.daily_pnl.write().unwrap() = Decimal::ZERO;
            *self.active_positions.write().unwrap() = 0;
            *last_reset = Instant::now();
            
            // Clear old trades from history
            let mut history = self.trade_history.write().unwrap();
            let cutoff = Instant::now() - Duration::from_secs(86400 * 7); // Keep 7 days
            history.retain(|t| t.timestamp > cutoff);
        }
    }

    pub fn get_risk_adjusted_size(
        &self,
        base_opportunity: Decimal,
        market_conditions: &MarketConditions,
    ) -> Result<u64, &'static str> {
        // Advanced risk adjustment based on market conditions
        let volatility_premium = self.calculate_volatility_premium(market_conditions.implied_vol);
        let liquidity_discount = self.calculate_liquidity_discount(market_conditions.depth);
        let momentum_factor = self.calculate_momentum_factor(market_conditions.trend_strength);
        
        let adjusted_edge = base_opportunity * volatility_premium * liquidity_discount * momentum_factor;
        let confidence = self.calculate_confidence_score(market_conditions);
        
        self.calculate_position_size(
            adjusted_edge,
            confidence,
            market_conditions.realized_vol,
            market_conditions.priority_fee,
            market_conditions.compute_units,
        )
    }

    fn calculate_volatility_premium(&self, implied_vol: Decimal) -> Decimal {
        let metrics = self.portfolio_metrics.read().unwrap();
        let vol_ratio = implied_vol / metrics.volatility.max(dec!(0.01));
        
        // Higher IV = higher premium opportunity
        if vol_ratio > dec!(1.5) {
            dec!(1.2)
        } else if vol_ratio > dec!(1.2) {
            dec!(1.1)
        } else if vol_ratio < dec!(0.8) {
            dec!(0.9)
        } else {
            Decimal::ONE
        }
    }

    fn calculate_liquidity_discount(&self, depth: Decimal) -> Decimal {
        // Adjust for market depth
        let depth_millions = depth / dec!(1_000_000);
        
        if depth_millions < dec!(0.1) {
            dec!(0.5) // Heavy discount for thin markets
        } else if depth_millions < dec!(0.5) {
            dec!(0.75)
        } else if depth_millions < dec!(1.0) {
            dec!(0.9)
        } else {
            Decimal::ONE
        }
    }

    fn calculate_momentum_factor(&self, trend_strength: Decimal) -> Decimal {
        // Momentum alignment bonus
        let abs_trend = trend_strength.abs();
        
        if abs_trend > dec!(0.7) {
            dec!(1.15) // Strong trend bonus
        } else if abs_trend > dec!(0.4) {
            dec!(1.05)
        } else if abs_trend < dec!(0.1) {
            dec!(0.95) // Choppy market penalty
        } else {
            Decimal::ONE
        }
    }

    fn calculate_confidence_score(&self, conditions: &MarketConditions) -> Decimal {
        let metrics = self.portfolio_metrics.read().unwrap();
        
        // Multi-factor confidence scoring
        let mut confidence = dec!(0.5); // Base confidence
        
        // Win rate contribution
        if metrics.win_rate > dec!(0.6) {
            confidence += dec!(0.2);
        } else if metrics.win_rate > dec!(0.55) {
            confidence += dec!(0.1);
        }
        
        // Sharpe ratio contribution
        if metrics.sharpe_ratio > dec!(2.0) {
            confidence += dec!(0.15);
        } else if metrics.sharpe_ratio > dec!(1.0) {
            confidence += dec!(0.05);
        }
        
        // Market conditions contribution
        if conditions.signals_aligned {
            confidence += dec!(0.1);
        }
        
        if conditions.low_competition {
            confidence += dec!(0.05);
        }
        
        confidence.min(dec!(0.95))
    }

    pub fn emergency_shutdown(&self) {
        *self.emergency_mode.write().unwrap() = true;
        *self.active_positions.write().unwrap() = 0;
    }

    pub fn resume_trading(&self) -> Result<(), &'static str> {
        let metrics = self.portfolio_metrics.read().unwrap();
        
        // Only resume if drawdown recovered
        if metrics.drawdown < MAX_DRAWDOWN_THRESHOLD * dec!(0.8) {
            *self.emergency_mode.write().unwrap() = false;
            Ok(())
        } else {
            Err("Drawdown still too high to resume")
        }
    }

    pub fn update_risk_parameters(&self, new_params: RiskParameters) {
        *self.risk_params.write().unwrap() = new_params;
    }

    pub fn get_current_metrics(&self) -> PortfolioMetrics {
        self.portfolio_metrics.read().unwrap().clone()
    }

    pub fn get_active_exposure(&self) -> u64 {
        let active = *self.active_positions.read().unwrap();
        let metrics = self.portfolio_metrics.read().unwrap();
        let avg_position = (metrics.current_value * dec!(0.05))
            .round()
            .to_u64()
            .unwrap_or(0);
        
        active as u64 * avg_position
    }

    pub fn should_take_position(&self, edge: Decimal, priority: TradePriority) -> bool {
        if *self.emergency_mode.read().unwrap() {
            return false;
        }

        let metrics = self.portfolio_metrics.read().unwrap();
        let risk_params = self.risk_params.read().unwrap();
        
        match priority {
            TradePriority::Critical => {
                edge > MIN_EDGE_THRESHOLD * dec!(2.0) && 
                metrics.drawdown < risk_params.max_drawdown
            },
            TradePriority::High => {
                edge > MIN_EDGE_THRESHOLD * dec!(1.5) && 
                metrics.drawdown < risk_params.max_drawdown * dec!(0.8)
            },
            TradePriority::Normal => {
                edge > MIN_EDGE_THRESHOLD && 
                metrics.drawdown < risk_params.max_drawdown * dec!(0.6)
            },
            TradePriority::Low => {
                edge > MIN_EDGE_THRESHOLD * dec!(0.5) && 
                metrics.drawdown < risk_params.max_drawdown * dec!(0.4)
            },
        }
    }

    pub fn optimize_for_gas(&self, base_size: u64, gas_estimate: u64) -> u64 {
        // Optimize position size for Solana gas economics
        let gas_impact = Decimal::from(gas_estimate) / Decimal::from(base_size.max(1));
        
        if gas_impact > dec!(0.001) { // >0.1% gas cost
            let optimized = base_size * 95 / 100; // Reduce by 5%
            optimized.max(MIN_POSITION_SIZE)
        } else {
            base_size
        }
    }
}

#[derive(Debug, Clone)]
pub struct MarketConditions {
    pub implied_vol: Decimal,
    pub realized_vol: Decimal,
    pub depth: Decimal,
    pub trend_strength: Decimal,
    pub signals_aligned: bool,
    pub low_competition: bool,
    pub priority_fee: u64,
    pub compute_units: u32,
}

#[derive(Debug, Clone, Copy)]
pub enum TradePriority {
    Critical,
    High,
    Normal,
    Low,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_sizing_with_drawdown() {
        let sizer = DrawdownConstrainedSizer::new(dec!(1_000_000));
        
        // Test normal conditions
        let size = sizer.calculate_position_size(
            dec!(0.01),
            dec!(0.8),
            dec!(0.02),
            1000,
            140_000,
        ).unwrap();
        
        assert!(size > 0);
        assert!(size <= 250_000_000_000); // Max 25% of portfolio
        
        // Test with drawdown
        sizer.update_portfolio_value(dec!(800_000)); // 20% drawdown
        let constrained_size = sizer.calculate_position_size(
            dec!(0.01),
            dec!(0.8),
            dec!(0.02),
            1000,
            140_000,
        ).unwrap();
        
        assert!(constrained_size < size); // Should be smaller with drawdown
    }

    #[test]
    fn test_emergency_shutdown() {
        let sizer = DrawdownConstrainedSizer::new(dec!(1_000_000));
        sizer.update_portfolio_value(dec!(700_000)); // 30% drawdown triggers emergency
        
        let size = sizer.calculate_position_size(
            dec!(0.05),
            dec!(0.9),
            dec!(0.01),
            1000,
            140_000,
        ).unwrap();
        
        assert_eq!(size, 0); // Should return 0 in emergency mode
    }

    #[test]
    fn test_trade_metrics_update() {
        let sizer = DrawdownConstrainedSizer::new(dec!(1_000_000));
        
        // Record winning trade
        sizer.record_trade(TradeMetrics {
            profit_loss: dec!(1000),
            timestamp: Instant::now(),
            size: 100_000_000,
            edge: dec!(0.01),
            success: true,
        });
        
        let metrics = sizer.get_current_metrics();
        assert!(metrics.win_rate > dec!(0));
    }
}

