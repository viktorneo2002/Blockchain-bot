//! Avellaneda-Stoikov Market Making Engine
//! 
//! Production-ready implementation of the Avellaneda-Stoikov (2008) market making model
//! with robust risk management, proper P&L accounting, and mainnet-grade reliability.

use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

// ===== Configuration Constants =====

/// Maximum price history points to maintain
const MAX_PRICE_HISTORY: usize = 1000;

/// Window size for volatility estimation
const VOLATILITY_WINDOW: usize = 100;

/// Default risk aversion parameter (γ)
const GAMMA_DEFAULT: Decimal = Decimal::from_parts(1, 0, 0, false, 1); // 0.1

/// Default order arrival intensity decay parameter (κ)
const KAPPA_DEFAULT: Decimal = Decimal::from_parts(15, 0, 0, false, 1); // 1.5

/// Default adverse selection parameter (η)
const ETA_DEFAULT: Decimal = Decimal::from_parts(1, 0, 0, false, 2); // 0.01

/// Minimum spread in basis points
const MIN_SPREAD_BPS: Decimal = Decimal::from_parts(5, 0, 0, false, 0); // 5.0

/// Maximum spread in basis points
const MAX_SPREAD_BPS: Decimal = Decimal::from_parts(200, 0, 0, false, 0); // 200.0

/// Target inventory level (neutral position)
const INVENTORY_TARGET: Decimal = Decimal::ZERO;

/// Maximum inventory ratio before emergency measures
const MAX_INVENTORY_RATIO: Decimal = Decimal::from_parts(8, 0, 0, false, 1); // 0.8

/// Volatility floor (minimum volatility)
const VOLATILITY_FLOOR: Decimal = Decimal::from_parts(1, 0, 0, false, 4); // 0.0001

/// Volatility ceiling (maximum volatility)
const VOLATILITY_CEILING: Decimal = Decimal::from_parts(5, 0, 0, false, 1); // 0.5

/// Maker fee in basis points (negative = rebate)
const MAKER_FEE_BPS: Decimal = Decimal::from_parts(2, 0, 0, true, 0); // -2.0

/// Taker fee in basis points
const TAKER_FEE_BPS: Decimal = Decimal::from_parts(5, 0, 0, false, 0); // 5.0

/// Minimum profit requirement in basis points
const MIN_PROFIT_BPS: Decimal = Decimal::from_parts(3, 0, 0, false, 0); // 3.0

/// Maximum relative oracle confidence (price uncertainty)
const MAX_ORACLE_CONFIDENCE_RATIO: Decimal = Decimal::from_parts(2, 0, 0, false, 2); // 0.02

/// Maximum price deviation from oracle
const MAX_PRICE_DEVIATION: Decimal = Decimal::from_parts(5, 0, 0, false, 2); // 0.05

/// EWMA smoothing factor for volatility
const EWMA_ALPHA: Decimal = Decimal::from_parts(1, 0, 0, false, 1); // 0.1

/// Quote validity duration in milliseconds
const QUOTE_VALIDITY_MS: u64 = 100;

/// Maximum drawdown before halt (as fraction of max position)
const MAX_DRAWDOWN_RATIO: Decimal = Decimal::from_parts(1, 0, 0, false, 1); // 0.1

// ===== Error Types =====

#[derive(Error, Debug)]
pub enum EngineError {
    #[error("Invalid parameter: {0}")]
    InvalidParameter(String),
    
    #[error("Oracle confidence too low: {confidence} > {threshold}")]
    LowOracleConfidence { confidence: Decimal, threshold: Decimal },
    
    #[error("Price deviation exceeds threshold: {deviation}")]
    ExcessivePriceDeviation { deviation: Decimal },
    
    #[error("Position limit breached: {position} > {limit}")]
    PositionLimitBreached { position: Decimal, limit: Decimal },
    
    #[error("Daily loss limit reached: {loss} > {limit}")]
    DailyLossLimitReached { loss: Decimal, limit: Decimal },
    
    #[error("Order size below minimum: {size} < {min}")]
    OrderSizeTooSmall { size: Decimal, min: Decimal },
    
    #[error("Trading halted: {reason}")]
    TradingHalted { reason: String },
    
    #[error("Invalid quote: {reason}")]
    InvalidQuote { reason: String },
    
    #[error("Stale quote: age {age_ms}ms > {max_ms}ms")]
    StaleQuote { age_ms: u64, max_ms: u64 },
    
    #[error("Math error: {0}")]
    MathError(String),
}

// ===== Time Source Abstraction =====

/// Trait for providing timestamps (allows mocking in tests)
pub trait TimeSource: Send + Sync {
    /// Get current timestamp in microseconds since epoch
    fn now_micros(&self) -> i64;
    
    /// Get current timestamp in milliseconds since epoch
    fn now_millis(&self) -> i64 {
        self.now_micros() / 1000
    }
}

/// System time source (production)
pub struct SystemTimeSource;

impl TimeSource for SystemTimeSource {
    fn now_micros(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64
    }
}

// ===== Market Configuration =====

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketConfig {
    pub symbol: String,
    pub base_decimals: u8,
    pub quote_decimals: u8,
    pub tick_size: Decimal,
    pub lot_size: Decimal,
    pub min_order_size: Decimal,
    pub max_order_size: Decimal,
    pub maker_fee_rate: Decimal,
    pub taker_fee_rate: Decimal,
}

impl MarketConfig {
    pub fn round_to_tick(&self, price: Decimal) -> Decimal {
        if self.tick_size == Decimal::ZERO {
            return price;
        }
        (price / self.tick_size).round() * self.tick_size
    }
    
    pub fn round_to_lot(&self, size: Decimal) -> Decimal {
        if self.lot_size == Decimal::ZERO {
            return size;
        }
        (size / self.lot_size).round() * self.lot_size
    }
}

// ===== Main Engine =====

    pub config: MarketConfig,
    pub gamma: Decimal,              // Risk aversion parameter
    pub kappa: Decimal,              // Order arrival intensity decay
    pub eta: Decimal,                // Adverse selection parameter
    pub sigma: Decimal,              // Current volatility estimate
    
    // Position tracking
    pub position: Arc<RwLock<PositionTracker>>,
    
    // Market data
    pub price_history: Arc<RwLock<VecDeque<PricePoint>>>,
    pub volatility_estimator: Arc<RwLock<VolatilityEstimator>>,
    
    // Risk management
    pub risk_manager: Arc<RwLock<RiskManager>>,
    
    // Time management
    pub time_source: Arc<dyn TimeSource>,
    pub session_start: i64,          // Session start time in micros
    pub session_end: i64,            // Session end time in micros
    pub last_update: i64,            // Last update time in micros
    
    // Pricing state
    pub reservation_price: Decimal,
    pub optimal_spread: Decimal,
    pub bid_price: Decimal,
    pub ask_price: Decimal,
    pub mid_price: Decimal,
    pub oracle_price: Decimal,
    pub oracle_confidence: Decimal,
    
    // Order sizing
    pub order_size_bid: Decimal,
    pub order_size_ask: Decimal,
    
    // Operational state
    pub active: bool,
    pub last_quote_time: i64,
    pub metrics: Arc<RwLock<EngineMetrics>>,
}

// ===== Position Tracking =====

#[derive(Clone, Debug)]
pub struct PositionTracker {
    pub inventory: Decimal,           // Current inventory (signed)
    pub target_inventory: Decimal,    // Target inventory level
    pub max_inventory: Decimal,       // Maximum allowed inventory
    pub min_inventory: Decimal,       // Minimum allowed inventory (negative)
    
    // Cost basis tracking
    pub position_cost: Decimal,       // Total cost of current position
    pub avg_entry_price: Decimal,     // Average entry price
    
    // P&L tracking
    pub realized_pnl: Decimal,        // Realized P&L from closed positions
    pub unrealized_pnl: Decimal,      // Unrealized P&L on open position
    pub fees_paid: Decimal,           // Total fees paid
    
    // Volume tracking
    pub total_buy_volume: Decimal,
    pub total_sell_volume: Decimal,
    pub trade_count: u64,
}

impl PositionTracker {
    pub fn new(max_inventory: Decimal) -> Self {
        Self {
            inventory: Decimal::ZERO,
            target_inventory: INVENTORY_TARGET,
            max_inventory,
            min_inventory: -max_inventory,
            position_cost: Decimal::ZERO,
            avg_entry_price: Decimal::ZERO,
            realized_pnl: Decimal::ZERO,
            unrealized_pnl: Decimal::ZERO,
            fees_paid: Decimal::ZERO,
            total_buy_volume: Decimal::ZERO,
            total_sell_volume: Decimal::ZERO,
            trade_count: 0,
        }
    }
    
    pub fn update_position_buy(
        &mut self,
        quantity: Decimal,
        price: Decimal,
        fee: Decimal,
    ) {
        self.position_cost += quantity * price;
        self.inventory += quantity;
        self.fees_paid += fee;
        self.total_buy_volume += quantity;
        self.trade_count += 1;
        
        if self.inventory != Decimal::ZERO {
            self.avg_entry_price = self.position_cost / self.inventory;
        }
    }
    
    pub fn update_position_sell(
        &mut self,
        quantity: Decimal,
        price: Decimal,
        fee: Decimal,
    ) {
        // Calculate realized P&L on this portion
        if self.inventory > Decimal::ZERO && self.avg_entry_price > Decimal::ZERO {
            let realized = quantity * (price - self.avg_entry_price);
            self.realized_pnl += realized;
        }
        
        // Update position
        let remaining_ratio = (self.inventory - quantity) / self.inventory;
        self.position_cost *= remaining_ratio;
        self.inventory -= quantity;
        self.fees_paid += fee;
        self.total_sell_volume += quantity;
        self.trade_count += 1;
        
        if self.inventory != Decimal::ZERO {
            self.avg_entry_price = self.position_cost / self.inventory;
        } else {
            self.avg_entry_price = Decimal::ZERO;
            self.position_cost = Decimal::ZERO;
        }
    }
    
    pub fn update_unrealized_pnl(&mut self, current_price: Decimal) {
        if self.inventory != Decimal::ZERO && self.avg_entry_price > Decimal::ZERO {
            self.unrealized_pnl = self.inventory * (current_price - self.avg_entry_price);
        } else {
            self.unrealized_pnl = Decimal::ZERO;
        }
    }
    
    pub fn total_pnl(&self) -> Decimal {
        self.realized_pnl + self.unrealized_pnl - self.fees_paid
    }
    
    pub fn inventory_ratio(&self) -> Decimal {
        if self.max_inventory == Decimal::ZERO {
            return Decimal::ZERO;
        }
        self.inventory / self.max_inventory
    }
}

// ===== Market Data =====

#[derive(Clone, Debug)]
pub struct PricePoint {
    pub timestamp: i64,      // Microseconds since epoch
    pub price: Decimal,
    pub volume: Decimal,
    pub spread: Decimal,
}

// ===== Volatility Estimation =====

#[derive(Clone, Debug)]
pub struct VolatilityEstimator {
    pub realized_vol: Decimal,        // Realized volatility (annualized)
    pub ewma_vol: Decimal,            // EWMA volatility estimate
    pub returns: VecDeque<(i64, Decimal)>,  // (timestamp, log_return)
    pub ewma_alpha: Decimal,          // EWMA smoothing factor
}

impl VolatilityEstimator {
    pub fn new() -> Self {
        Self {
            realized_vol: Decimal::from_str("0.3").unwrap(),  // 30% annualized default
            ewma_vol: Decimal::from_str("0.3").unwrap(),
            returns: VecDeque::with_capacity(VOLATILITY_WINDOW),
            ewma_alpha: EWMA_ALPHA,
        }
    }
    
    pub fn update(&mut self, timestamp: i64, price: Decimal, prev_price: Decimal) {
        if price <= Decimal::ZERO || prev_price <= Decimal::ZERO {
            return;
        }
        
        // Calculate log return
        let price_f64 = price.to_f64().unwrap_or(0.0);
        let prev_f64 = prev_price.to_f64().unwrap_or(1.0);
        let log_return = (price_f64 / prev_f64).ln();
        let log_return_dec = Decimal::from_f64(log_return).unwrap_or(Decimal::ZERO);
        
        // Add to returns history
        self.returns.push_back((timestamp, log_return_dec));
        if self.returns.len() > VOLATILITY_WINDOW {
            self.returns.pop_front();
        }
        
        // Update EWMA volatility
        let abs_return = log_return_dec.abs();
        self.ewma_vol = self.ewma_alpha * abs_return + 
                        (Decimal::ONE - self.ewma_alpha) * self.ewma_vol;
        
        // Calculate realized volatility if we have enough data
        if self.returns.len() >= 20 {
            self.calculate_realized_volatility();
        }
        
        // Apply bounds
        self.realized_vol = self.realized_vol.max(VOLATILITY_FLOOR).min(VOLATILITY_CEILING);
        self.ewma_vol = self.ewma_vol.max(VOLATILITY_FLOOR).min(VOLATILITY_CEILING);
    }
    
    fn calculate_realized_volatility(&mut self) {
        if self.returns.len() < 2 {
            return;
        }
        
        // Calculate time-weighted variance
        let returns_vec: Vec<Decimal> = self.returns.iter().map(|(_, r)| *r).collect();
        let mean = returns_vec.iter().sum::<Decimal>() / Decimal::from(returns_vec.len());
        
        let variance = returns_vec.iter()
            .map(|r| (*r - mean) * (*r - mean))
            .sum::<Decimal>() / Decimal::from(returns_vec.len() - 1);
        
        // Get time span in days
        if let (Some(first), Some(last)) = (self.returns.front(), self.returns.back()) {
            let time_span_micros = last.0 - first.0;
            let time_span_days = Decimal::from(time_span_micros) / 
                                 Decimal::from(86_400_000_000i64);  // micros per day
            
            if time_span_days > Decimal::ZERO {
                // Annualize (assuming 365 days)
                let samples_per_year = Decimal::from(365) * Decimal::from(self.returns.len()) / time_span_days;
                let variance_f64 = variance.to_f64().unwrap_or(0.0);
                let samples_f64 = samples_per_year.to_f64().unwrap_or(365.0);
                let annualized_vol = (variance_f64 * samples_f64).sqrt();
                self.realized_vol = Decimal::from_f64(annualized_vol).unwrap_or(self.realized_vol);
            }
        }
    }
    
    pub fn get_current_volatility(&self) -> Decimal {
        // Weighted average of realized and EWMA
        Decimal::from_str("0.7").unwrap() * self.realized_vol + 
        Decimal::from_str("0.3").unwrap() * self.ewma_vol
    }
}

// ===== Risk Management =====

#[derive(Clone, Debug)]
pub struct RiskManager {
    pub max_position_value: Decimal,     // Maximum position value in quote
    pub max_order_size: Decimal,         // Maximum order size
    pub daily_loss_limit: Decimal,       // Daily loss limit
    pub current_daily_pnl: Decimal,      // Current daily P&L
    pub session_start_pnl: Decimal,      // P&L at session start
    pub max_drawdown: Decimal,           // Maximum drawdown allowed
    pub peak_pnl: Decimal,               // Peak P&L for drawdown calculation
    
    // Risk state
    pub position_limits_breached: bool,
    pub trading_halted: bool,
    pub halt_reason: String,
    pub last_risk_check: i64,
    
    // Rate limiting
    pub loss_window: VecDeque<(i64, Decimal)>,  // (timestamp, loss)
    pub max_loss_per_minute: Decimal,
}

impl RiskManager {
    pub fn new(max_position_value: Decimal) -> Self {
        Self {
            max_position_value,
            max_order_size: max_position_value * Decimal::from_str("0.1").unwrap(),
            daily_loss_limit: max_position_value * Decimal::from_str("0.02").unwrap(),
            current_daily_pnl: Decimal::ZERO,
            session_start_pnl: Decimal::ZERO,
            max_drawdown: max_position_value * MAX_DRAWDOWN_RATIO,
            peak_pnl: Decimal::ZERO,
            position_limits_breached: false,
            trading_halted: false,
            halt_reason: String::new(),
            last_risk_check: 0,
            loss_window: VecDeque::new(),
            max_loss_per_minute: max_position_value * Decimal::from_str("0.005").unwrap(),
        }
    }
    
    pub fn check_risk_limits(
        &mut self,
        position: &PositionTracker,
        current_price: Decimal,
        timestamp: i64,
    ) -> Result<(), EngineError> {
        self.last_risk_check = timestamp;
        
        // Update daily P&L
        self.current_daily_pnl = position.total_pnl() - self.session_start_pnl;
        
        // Check daily loss limit
        if self.current_daily_pnl < -self.daily_loss_limit {
            self.trading_halted = true;
            self.halt_reason = "Daily loss limit breached".to_string();
            return Err(EngineError::DailyLossLimitReached {
                loss: -self.current_daily_pnl,
                limit: self.daily_loss_limit,
            });
        }
        
        // Check position limits
        let position_value = position.inventory.abs() * current_price;
        if position_value > self.max_position_value {
            self.position_limits_breached = true;
            return Err(EngineError::PositionLimitBreached {
                position: position_value,
                limit: self.max_position_value,
            });
        } else {
            self.position_limits_breached = false;
        }
        
        // Check drawdown
        let current_pnl = position.total_pnl();
        if current_pnl > self.peak_pnl {
            self.peak_pnl = current_pnl;
        }
        let drawdown = self.peak_pnl - current_pnl;
        if drawdown > self.max_drawdown {
            self.trading_halted = true;
            self.halt_reason = format!("Maximum drawdown exceeded: {}", drawdown);
            return Err(EngineError::TradingHalted {
                reason: self.halt_reason.clone(),
            });
        }
        
        // Check rate of loss
        self.update_loss_window(timestamp, current_pnl);
        if self.is_losing_too_fast() {
            self.trading_halted = true;
            self.halt_reason = "Losing too quickly".to_string();
            return Err(EngineError::TradingHalted {
                reason: self.halt_reason.clone(),
            });
        }
        
        Ok(())
    }
    
    fn update_loss_window(&mut self, timestamp: i64, current_pnl: Decimal) {
        // Add current P&L to window
        self.loss_window.push_back((timestamp, current_pnl));
        
        // Remove old entries (older than 1 minute)
        let cutoff = timestamp - 60_000_000;  // 60 seconds in microseconds
        while let Some((t, _)) = self.loss_window.front() {
            if *t < cutoff {
                self.loss_window.pop_front();
            } else {
                break;
            }
        }
    }
    
    fn is_losing_too_fast(&self) -> bool {
        if self.loss_window.len() < 2 {
            return false;
        }
        
        if let (Some((_, start_pnl)), Some((_, end_pnl))) = 
            (self.loss_window.front(), self.loss_window.back()) {
            let loss = end_pnl - start_pnl;
            return loss < -self.max_loss_per_minute;
        }
        
        false
    }
    
    pub fn reset_daily_metrics(&mut self, current_pnl: Decimal) {
        self.session_start_pnl = current_pnl;
        self.current_daily_pnl = Decimal::ZERO;
        self.peak_pnl = current_pnl;
        self.trading_halted = false;
        self.halt_reason.clear();
    }
}

// ===== Engine Metrics =====

#[derive(Clone, Debug, Default)]
pub struct EngineMetrics {
    pub quotes_generated: u64,
    pub orders_placed: u64,
    pub orders_filled: u64,
    pub orders_cancelled: u64,
    pub bid_fills: u64,
    pub ask_fills: u64,
    pub total_volume: Decimal,
    pub avg_spread_bps: Decimal,
    pub time_at_target_inventory: u64,
    pub max_inventory_reached: Decimal,
    pub min_inventory_reached: Decimal,
}

// ===== Quote Generation =====

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Quote {
    pub bid_price: Decimal,
    pub bid_size: Decimal,
    pub ask_price: Decimal,
    pub ask_size: Decimal,
    pub mid_price: Decimal,
    pub spread_bps: Decimal,
    pub timestamp: i64,
    pub valid_until: i64,
}

impl Quote {
    pub fn is_valid(&self, current_time: i64) -> bool {
        current_time <= self.valid_until && 
        self.bid_price > Decimal::ZERO &&
        self.ask_price > self.bid_price &&
        self.bid_size > Decimal::ZERO &&
        self.ask_size > Decimal::ZERO
    }
}

// ===== Engine Implementation =====

impl ASMMEngine {
    pub fn new(
        config: MarketConfig,
        max_position_value: Decimal,
        time_source: Arc<dyn TimeSource>,
        session_duration_hours: i64,
    ) -> Self {
        let now = time_source.now_micros();
        let session_end = now + (session_duration_hours * 3600 * 1_000_000);
        
        let max_inventory = max_position_value / Decimal::from(1000);  // Rough estimate
        let position = Arc::new(RwLock::new(PositionTracker::new(max_inventory)));
        let volatility_estimator = Arc::new(RwLock::new(VolatilityEstimator {
            realized_vol: 0.01,
            garch_vol: 0.01,
            ewma_vol: 0.01,
            returns: VecDeque::with_capacity(VOLATILITY_WINDOW),
            squared_returns: VecDeque::with_capacity(VOLATILITY_WINDOW),
            alpha: 0.05,
            beta: 0.94,
            omega: 0.000001,
        }));
        let risk_manager = Arc::new(RwLock::new(RiskManager::new(max_position_value)));
        let metrics = Arc::new(RwLock::new(EngineMetrics::default()));
        
        Self {
            config,
            gamma: GAMMA_DEFAULT,
            kappa: KAPPA_DEFAULT,
            eta: ETA_DEFAULT,
            sigma: Decimal::from_str("0.3").unwrap(),  // 30% annualized volatility default
            position,
            price_history: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_PRICE_HISTORY))),
            volatility_estimator,
            risk_manager,
            time_source,
            session_start: now,
            session_end,
            last_update: now,
            reservation_price: Decimal::ZERO,
            optimal_spread: Decimal::ZERO,
            bid_price: Decimal::ZERO,
            ask_price: Decimal::ZERO,
            mid_price: Decimal::ZERO,
            oracle_price: Decimal::ZERO,
            oracle_confidence: Decimal::ZERO,
            order_size_bid: Decimal::ZERO,
            order_size_ask: Decimal::ZERO,
            active: true,
            last_quote_time: 0,
            metrics,
        }
    }

    /// Update market data from oracle price feed
    pub async fn update_market_data(
        &mut self,
        oracle_price: Decimal,
        oracle_confidence: Decimal,
    ) -> Result<(), EngineError> {
        let current_time = self.time_source.now_micros();
        
        // Validate oracle inputs
        if oracle_price <= Decimal::ZERO {
            return Err(EngineError::InvalidParameter(
                "Oracle price must be positive".to_string()
            ));
        }
        
        // Check relative confidence
        let relative_confidence = oracle_confidence / oracle_price;
        if relative_confidence > MAX_ORACLE_CONFIDENCE_RATIO {
            return Err(EngineError::LowOracleConfidence {
                confidence: relative_confidence,
                threshold: MAX_ORACLE_CONFIDENCE_RATIO,
            });
        }
        
        // Check price deviation from last known price
        if self.mid_price > Decimal::ZERO {
            let deviation = ((oracle_price - self.mid_price) / self.mid_price).abs();
            if deviation > MAX_PRICE_DEVIATION {
                warn!("Large price deviation: {} vs {}", oracle_price, self.mid_price);
                // Don't reject, but log for monitoring
            }
        }
        
        // Update price data
        let prev_price = self.oracle_price;
        self.oracle_price = oracle_price;
        self.oracle_confidence = oracle_confidence;
        self.mid_price = oracle_price;
        
        // Store price point
        let price_point = PricePoint {
            timestamp: current_time,
            price: oracle_price,
            volume: Decimal::ZERO,
            spread: self.optimal_spread,
        };
        
        {
            let mut price_history = self.price_history.write().await;
            price_history.push_back(price_point);
            if price_history.len() > MAX_PRICE_HISTORY {
                price_history.pop_front();
            }
        }
        
        // Update volatility estimate
        if prev_price > Decimal::ZERO {
            let mut vol_estimator = self.volatility_estimator.write().await;
            vol_estimator.update(current_time, oracle_price, prev_price);
            self.sigma = vol_estimator.get_current_volatility();
        }
        
        // Update position unrealized P&L
        {
            let mut position = self.position.write().await;
            position.update_unrealized_pnl(oracle_price);
        }
        
        self.last_update = current_time;
        
        Ok(())
    }

    /// Calculate reservation price using canonical Avellaneda-Stoikov formula
    /// r = S - q * γ * σ² * (T - t)
    fn calculate_reservation_price(&mut self) -> Result<(), EngineError> {
        let now = self.time_source.now_micros();
        
        // Calculate time to close in days
        let time_to_close_micros = (self.session_end - now).max(0);
        let time_to_close_days = Decimal::from(time_to_close_micros) / 
                                 Decimal::from(86_400_000_000i64);  // micros per day
        
        // Get current inventory
        let inventory = self.position.try_read()
            .map_err(|_| EngineError::MathError("Failed to read position".to_string()))?
            .inventory;
        
        // Calculate inventory adjustment
        // Adjustment = q * γ * σ² * (T - t)
        let inventory_adjustment = inventory * self.gamma * self.sigma * self.sigma * time_to_close_days;
        
        // Reservation price = mid_price - inventory_adjustment
        self.reservation_price = self.mid_price - inventory_adjustment;
        
        // Ensure reservation price is reasonable
        let min_price = self.mid_price * Decimal::from_str("0.95").unwrap();
        let max_price = self.mid_price * Decimal::from_str("1.05").unwrap();
        self.reservation_price = self.reservation_price.max(min_price).min(max_price);
        
        debug!("Reservation price: {}, Mid: {}, Inventory: {}, Time to close: {} days",
               self.reservation_price, self.mid_price, inventory, time_to_close_days);
        
        Ok(())
    }

    /// Calculate optimal quotes using canonical Avellaneda-Stoikov formula
    /// Total spread = γ * σ² * (T - t) + (2/γ) * ln(1 + γ/κ)
    fn calculate_optimal_quotes(&mut self) -> Result<(), EngineError> {
        let now = self.time_source.now_micros();
        
        // Calculate time to close in days
        let time_to_close_micros = (self.session_end - now).max(0);
        let time_to_close_days = Decimal::from(time_to_close_micros) / 
                                 Decimal::from(86_400_000_000i64);
        
        // Canonical Avellaneda-Stoikov spread formula
        // spread = γ * σ² * (T - t) + (2/γ) * ln(1 + γ/κ)
        let variance_term = self.gamma * self.sigma * self.sigma * time_to_close_days;
        
        // Calculate liquidity term: (2/γ) * ln(1 + γ/κ)
        let liquidity_ratio = self.gamma / self.kappa;
        let ln_arg = Decimal::ONE + liquidity_ratio;
        let ln_value = if ln_arg > Decimal::ZERO {
            let ln_f64 = ln_arg.to_f64().unwrap_or(1.0).ln();
            Decimal::from_f64(ln_f64).unwrap_or(Decimal::ZERO)
        } else {
            Decimal::ZERO
        };
        let liquidity_term = (Decimal::TWO / self.gamma) * ln_value;
        
        // Total optimal spread
        self.optimal_spread = variance_term + liquidity_term;
        
        // Apply spread bounds
        let min_spread = MIN_SPREAD_BPS / Decimal::from(10000) * self.mid_price;
        let max_spread = MAX_SPREAD_BPS / Decimal::from(10000) * self.mid_price;
        self.optimal_spread = self.optimal_spread.max(min_spread).min(max_spread);
        
        // Calculate half-spread
        let half_spread = self.optimal_spread / Decimal::TWO;
        
        // Set bid and ask prices
        let raw_bid = self.reservation_price - half_spread;
        let raw_ask = self.reservation_price + half_spread;
        
        // Round to tick size
        self.bid_price = self.config.round_to_tick(raw_bid);
        self.ask_price = self.config.round_to_tick(raw_ask);
        
        // Ensure minimum profitable spread (accounting for fees)
        let fee_adjusted_spread = (TAKER_FEE_BPS - MAKER_FEE_BPS + MIN_PROFIT_BPS) / 
                                  Decimal::from(10000) * self.mid_price;
        
        if self.ask_price - self.bid_price < fee_adjusted_spread {
            let adjustment = (fee_adjusted_spread - (self.ask_price - self.bid_price)) / Decimal::TWO;
            self.bid_price = self.config.round_to_tick(self.bid_price - adjustment);
            self.ask_price = self.config.round_to_tick(self.ask_price + adjustment);
        }
        
        // Ensure ask > bid
        if self.ask_price <= self.bid_price {
            return Err(EngineError::InvalidQuote {
                reason: format!("Ask {} <= Bid {}", self.ask_price, self.bid_price),
            });
        }
        
        // Apply safety bounds
        let min_bid = self.mid_price * Decimal::from_str("0.9").unwrap();
        let max_ask = self.mid_price * Decimal::from_str("1.1").unwrap();
        self.bid_price = self.bid_price.max(min_bid);
        self.ask_price = self.ask_price.min(max_ask);
        
        debug!("Quotes - Bid: {}, Ask: {}, Spread: {} bps",
               self.bid_price, self.ask_price,
               (self.ask_price - self.bid_price) / self.mid_price * Decimal::from(10000));
        
        Ok(())
    }

    /// Calculate order sizes with inventory skew and risk limits
    async fn calculate_order_sizes(&mut self) -> Result<(), EngineError> {
        let position = self.position.read().await;
        let risk_manager = self.risk_manager.read().await;
        
        // Base order size calculation
        let max_order = risk_manager.max_order_size;
        let base_size = max_order * Decimal::from_str("0.05").unwrap();
        
        // Volatility adjustment - reduce size in high volatility
        let vol_factor = Decimal::ONE / (Decimal::ONE + self.sigma * Decimal::from(10));
        let vol_adjustment = vol_factor.max(Decimal::from_str("0.2").unwrap());
        
        // Inventory skew - adjust sizes based on current position
        let inventory_ratio = position.inventory_ratio();
        let skew_factor = Decimal::from_str("0.5").unwrap();
        
        // Increase bid size when short, decrease when long
        let bid_adjustment = Decimal::ONE - inventory_ratio * skew_factor;
        // Decrease ask size when short, increase when long
        let ask_adjustment = Decimal::ONE + inventory_ratio * skew_factor;
        
        // Calculate raw sizes
        let raw_bid_size = base_size * vol_adjustment * bid_adjustment;
        let raw_ask_size = base_size * vol_adjustment * ask_adjustment;
        
        // Round to lot size
        self.order_size_bid = self.config.round_to_lot(raw_bid_size);
        self.order_size_ask = self.config.round_to_lot(raw_ask_size);
        
        // Apply position limits
        let remaining_buy_capacity = position.max_inventory - position.inventory;
        let remaining_sell_capacity = position.inventory - position.min_inventory;
        
        self.order_size_bid = self.order_size_bid
            .min(remaining_buy_capacity)
            .min(risk_manager.max_order_size)
            .max(Decimal::ZERO);
            
        self.order_size_ask = self.order_size_ask
            .min(remaining_sell_capacity)
            .min(risk_manager.max_order_size)
            .max(Decimal::ZERO);
        
        // Zero out if at limits
        if position.inventory >= position.max_inventory * MAX_INVENTORY_RATIO {
            self.order_size_bid = Decimal::ZERO;
        }
        if position.inventory <= position.min_inventory * MAX_INVENTORY_RATIO {
            self.order_size_ask = Decimal::ZERO;
        }
        
        // Ensure minimum order size or zero
        if self.order_size_bid > Decimal::ZERO && self.order_size_bid < self.config.min_order_size {
            self.order_size_bid = Decimal::ZERO;
        }
        if self.order_size_ask > Decimal::ZERO && self.order_size_ask < self.config.min_order_size {
            self.order_size_ask = Decimal::ZERO;
        }
        
        Ok(())
    }
    
    /// Execute a trade and update position tracking
    pub async fn execute_trade(
        &mut self,
        side: OrderSide,
        quantity: Decimal,
        price: Decimal,
    ) -> Result<(), EngineError> {
        // Validate inputs
        if quantity <= Decimal::ZERO {
            return Err(EngineError::InvalidParameter(
                "Quantity must be positive".to_string()
            ));
        }
        if price <= Decimal::ZERO {
            return Err(EngineError::InvalidParameter(
                "Price must be positive".to_string()
            ));
        }
        
        // Round to lot size
        let adjusted_quantity = self.config.round_to_lot(quantity);
        if adjusted_quantity < self.config.min_order_size {
            return Err(EngineError::OrderSizeTooSmall {
                size: adjusted_quantity,
                min: self.config.min_order_size,
            });
        }
        
        // Calculate fees
        let fee_rate = match side {
            OrderSide::Buy => self.config.maker_fee_rate,
            OrderSide::Sell => self.config.maker_fee_rate,
        };
        let fee = adjusted_quantity * price * fee_rate.abs() / Decimal::from(10000);
        
        // Update position
        {
            let mut position = self.position.write().await;
            
            match side {
                OrderSide::Buy => {
                    // Check position limits
                    if position.inventory + adjusted_quantity > position.max_inventory {
                        return Err(EngineError::PositionLimitBreached {
                            position: position.inventory + adjusted_quantity,
                            limit: position.max_inventory,
                        });
                    }
                    position.update_position_buy(adjusted_quantity, price, fee);
                }
                OrderSide::Sell => {
                    // Check position limits
                    if position.inventory - adjusted_quantity < position.min_inventory {
                        return Err(EngineError::PositionLimitBreached {
                            position: position.inventory - adjusted_quantity,
                            limit: position.min_inventory,
                        });
                    }
                    position.update_position_sell(adjusted_quantity, price, fee);
                }
            }
            
            // Update unrealized P&L
            position.update_unrealized_pnl(self.mid_price);
        }
        
        // Update metrics
        {
            let mut metrics = self.metrics.write().await;
            metrics.orders_filled += 1;
            metrics.total_volume += adjusted_quantity;
            match side {
                OrderSide::Buy => metrics.bid_fills += 1,
                OrderSide::Sell => metrics.ask_fills += 1,
            }
        }
        
        // Check risk limits
        {
            let position = self.position.read().await;
            let mut risk_manager = self.risk_manager.write().await;
            risk_manager.check_risk_limits(&position, self.mid_price, self.time_source.now_micros())?;
        }
        
        // Recalculate quotes
        self.calculate_reservation_price()?;
        self.calculate_optimal_quotes()?;
        self.calculate_order_sizes().await?;
        
        info!("Trade executed - Side: {:?}, Qty: {}, Price: {}, Fee: {}",
              side, adjusted_quantity, price, fee);
        
        Ok(())
    }
    
    /// Generate a quote with current market conditions
    pub async fn get_quote(&mut self) -> Result<Quote, EngineError> {
        // Check if engine is active
        if !self.active {
            return Err(EngineError::TradingHalted {
                reason: "Engine is not active".to_string(),
            });
        }
        
        // Check risk manager status
        {
            let risk_manager = self.risk_manager.read().await;
            if risk_manager.trading_halted {
                return Err(EngineError::TradingHalted {
                    reason: risk_manager.halt_reason.clone(),
                });
            }
        }
        
        // Calculate fresh quotes
        self.calculate_reservation_price()?;
        self.calculate_optimal_quotes()?;
        self.calculate_order_sizes().await?;
        
        // Update metrics
        {
            let mut metrics = self.metrics.write().await;
            metrics.quotes_generated += 1;
            
            // Update average spread tracking
            let spread_bps = (self.ask_price - self.bid_price) / self.mid_price * Decimal::from(10000);
            if metrics.quotes_generated == 1 {
                metrics.avg_spread_bps = spread_bps;
            } else {
                let weight = Decimal::from_str("0.99").unwrap();
                metrics.avg_spread_bps = metrics.avg_spread_bps * weight + spread_bps * (Decimal::ONE - weight);
            }
        }
        
        let now = self.time_source.now_micros();
        self.last_quote_time = now;
        
        let quote = Quote {
            bid_price: self.bid_price,
            bid_size: self.order_size_bid,
            ask_price: self.ask_price,
            ask_size: self.order_size_ask,
            mid_price: self.mid_price,
            spread_bps: (self.ask_price - self.bid_price) / self.mid_price * Decimal::from(10000),
            timestamp: now,
            valid_until: now + (QUOTE_VALIDITY_MS as i64 * 1000),
        };
        
        // Validate quote before returning
        if !quote.is_valid(now) {
            return Err(EngineError::InvalidQuote {
                reason: "Generated quote failed validation".to_string(),
            });
        }
        
        Ok(quote)
    }
    
    /// Emergency position close
    pub async fn emergency_close_position(&mut self, market_price: Decimal) -> Result<(), EngineError> {
        let mut position = self.position.write().await;
        
        if position.inventory == Decimal::ZERO {
            info!("No position to close");
            return Ok(());
        }
        
        let side = if position.inventory > Decimal::ZERO {
            OrderSide::Sell
        } else {
            OrderSide::Buy
        };
        
        let quantity = position.inventory.abs();
        
        // Calculate emergency fee (likely taker fee)
        let fee = quantity * market_price * TAKER_FEE_BPS.abs() / Decimal::from(10000);
        
        // Update position
        match side {
            OrderSide::Buy => position.update_position_buy(quantity, market_price, fee),
            OrderSide::Sell => position.update_position_sell(quantity, market_price, fee),
        }
        
        warn!("Emergency close executed - Side: {:?}, Qty: {}, Price: {}",
              side, quantity, market_price);
        
        // Halt trading after emergency close
        self.active = false;
        let mut risk_manager = self.risk_manager.write().await;
        risk_manager.trading_halted = true;
        risk_manager.halt_reason = "Emergency position close executed".to_string();
        
        Ok(())
    }
    
    /// Update engine parameters
    pub async fn update_parameters(
        &mut self,
        gamma: Option<Decimal>,
        kappa: Option<Decimal>,
        eta: Option<Decimal>,
    ) -> Result<(), EngineError> {
        if let Some(g) = gamma {
            if g <= Decimal::ZERO || g > Decimal::ONE {
                return Err(EngineError::InvalidParameter(
                    "Gamma must be between 0 and 1".to_string()
                ));
            }
            self.gamma = g;
        }
        
        if let Some(k) = kappa {
            if k <= Decimal::ZERO || k > Decimal::from(10) {
                return Err(EngineError::InvalidParameter(
                    "Kappa must be between 0 and 10".to_string()
                ));
            }
            self.kappa = k;
        }
        
        if let Some(e) = eta {
            if e <= Decimal::ZERO || e > Decimal::ONE {
                return Err(EngineError::InvalidParameter(
                    "Eta must be between 0 and 1".to_string()
                ));
            }
            self.eta = e;
        }
        
        // Recalculate quotes with new parameters
        self.calculate_reservation_price()?;
        self.calculate_optimal_quotes()?;
        
        Ok(())
    }
    
    /// Reset session metrics
    pub async fn reset_session(&mut self) {
        let position = self.position.read().await;
        let mut risk_manager = self.risk_manager.write().await;
        
        risk_manager.reset_daily_metrics(position.total_pnl());
        
        let now = self.time_source.now_micros();
        self.session_start = now;
        self.session_end = now + (24 * 3600 * 1_000_000);  // 24 hours
        
        info!("Session reset at {}", now);
    }
    
    /// Get current engine status
    pub async fn get_status(&self) -> EngineStatus {
        let position = self.position.read().await;
        let risk_manager = self.risk_manager.read().await;
        let metrics = self.metrics.read().await;
        
        EngineStatus {
            active: self.active,
            trading_halted: risk_manager.trading_halted,
            halt_reason: risk_manager.halt_reason.clone(),
            position: position.inventory,
            total_pnl: position.total_pnl(),
            realized_pnl: position.realized_pnl,
            unrealized_pnl: position.unrealized_pnl,
            fees_paid: position.fees_paid,
            current_volatility: self.sigma,
            quotes_generated: metrics.quotes_generated,
            orders_filled: metrics.orders_filled,
            total_volume: metrics.total_volume,
        }
    }
}

// ===== Supporting Types =====

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EngineStatus {
    pub active: bool,
    pub trading_halted: bool,
    pub halt_reason: String,
    pub position: Decimal,
    pub total_pnl: Decimal,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub fees_paid: Decimal,
    pub current_volatility: Decimal,
    pub quotes_generated: u64,
    pub orders_filled: u64,
    pub total_volume: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;
    
    struct MockTimeSource {
        current_time: std::sync::Mutex<i64>,
    }
    
    impl MockTimeSource {
        fn new(time: i64) -> Self {
            Self {
                current_time: std::sync::Mutex::new(time),
            }
        }
        
        fn advance(&self, micros: i64) {
            let mut time = self.current_time.lock().unwrap();
            *time += micros;
        }
    }
    
    impl TimeSource for MockTimeSource {
        fn now_micros(&self) -> i64 {
            *self.current_time.lock().unwrap()
        }
    }
    
    #[tokio::test]
    async fn test_canonical_spread_formula() {
        let config = MarketConfig {
            symbol: "TEST".to_string(),
            base_decimals: 8,
            quote_decimals: 2,
            tick_size: Decimal::from_str("0.01").unwrap(),
            lot_size: Decimal::from_str("0.01").unwrap(),
            min_order_size: Decimal::from_str("0.1").unwrap(),
            max_order_size: Decimal::from(1000),
            maker_fee_rate: MAKER_FEE_BPS,
            taker_fee_rate: TAKER_FEE_BPS,
        };
        
        let time_source = Arc::new(MockTimeSource::new(1_000_000_000));
        let mut engine = ASMMEngine::new(
            config,
            Decimal::from(100_000),
            time_source.clone(),
            24,
        );
        
        // Set known values for testing
        engine.mid_price = Decimal::from(100);
        engine.gamma = Decimal::from_str("0.1").unwrap();
        engine.kappa = Decimal::from_str("1.5").unwrap();
        engine.sigma = Decimal::from_str("0.3").unwrap();  // 30% annualized vol
        
        // Calculate quotes
        engine.calculate_reservation_price().unwrap();
        engine.calculate_optimal_quotes().unwrap();
        
        // Verify spread is positive and reasonable
        assert!(engine.optimal_spread > Decimal::ZERO);
        assert!(engine.ask_price > engine.bid_price);
        
        // Verify spread components
        let time_to_close_days = Decimal::from(24 * 3600) / Decimal::from(86400);  // ~1 day
        let variance_term = engine.gamma * engine.sigma * engine.sigma * time_to_close_days;
        assert!(variance_term > Decimal::ZERO);
    }
    
    #[tokio::test]
    async fn test_position_tracking() {
        let mut tracker = PositionTracker::new(Decimal::from(100));
        
        // Buy 10 @ 100
        tracker.update_position_buy(
            Decimal::from(10),
            Decimal::from(100),
            Decimal::from_str("0.2").unwrap(),
        );
        
        assert_eq!(tracker.inventory, Decimal::from(10));
        assert_eq!(tracker.position_cost, Decimal::from(1000));
        assert_eq!(tracker.avg_entry_price, Decimal::from(100));
        
        // Sell 5 @ 105
        tracker.update_position_sell(
            Decimal::from(5),
            Decimal::from(105),
            Decimal::from_str("0.1").unwrap(),
        );
        
        assert_eq!(tracker.inventory, Decimal::from(5));
        assert_eq!(tracker.realized_pnl, Decimal::from(25));  // 5 * (105 - 100)
        
        // Update unrealized P&L
        tracker.update_unrealized_pnl(Decimal::from(110));
        assert_eq!(tracker.unrealized_pnl, Decimal::from(50));  // 5 * (110 - 100)
    }
    
    #[tokio::test]
    async fn test_risk_limits() {
        let mut risk_manager = RiskManager::new(Decimal::from(10_000));
        let mut position = PositionTracker::new(Decimal::from(100));
        
        // Simulate loss
        position.realized_pnl = Decimal::from(-150);
        
        // Should not breach daily loss limit yet
        let result = risk_manager.check_risk_limits(
            &position,
            Decimal::from(100),
            1_000_000,
        );
        assert!(result.is_ok());
        
        // Simulate larger loss
        position.realized_pnl = Decimal::from(-250);
        
        // Should breach daily loss limit (2% of 10,000 = 200)
        let result = risk_manager.check_risk_limits(
            &position,
            Decimal::from(100),
            2_000_000,
        );
        assert!(result.is_err());
        assert!(
            risk_manager.trading_halted = true;
            self.active = false;
            return Err("Daily loss limit reached".into());
        }
        
        let position_value = self.inventory.abs() * self.mid_price;
        let max_allowed_position = risk_manager.max_position_size * self.mid_price;
        
        if position_value > max_allowed_position {
            self.order_size_bid = 0.0;
            self.order_size_ask = 0.0;
            return Err("Position size limit exceeded".into());
        }
        
        risk_manager.last_risk_check = Clock::get()?.unix_timestamp;
        
        Ok(())
    }

    pub async fn execute_trade(
        &mut self,
        side: OrderSide,
        quantity: f64,
        price: f64,
        timestamp: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let adjusted_quantity = (quantity / LOT_SIZE).round() * LOT_SIZE;
        
        if adjusted_quantity < MIN_ORDER_SIZE {
            return Err("Order size below minimum".into());
        }
        
        match side {
            OrderSide::Buy => {
                if self.inventory + adjusted_quantity > self.max_inventory {
                    return Err("Trade would exceed max inventory".into());
                }
                self.inventory += adjusted_quantity;
                self.realized_pnl -= adjusted_quantity * price * (1.0 + MAKER_FEE_BPS / 10000.0);
            },
            OrderSide::Sell => {
                if self.inventory - adjusted_quantity < self.min_inventory {
                    return Err("Trade would exceed min inventory".into());
                }
                self.inventory -= adjusted_quantity;
                self.realized_pnl += adjusted_quantity * price * (1.0 - MAKER_FEE_BPS / 10000.0);
            }
        }
        
        self.total_volume += adjusted_quantity;
        self.trade_count += 1;
        
        self.update_unrealized_pnl();
        
        let mut risk_manager = self.risk_manager.write().await;
        risk_manager.current_daily_pnl = self.realized_pnl + self.unrealized_pnl;
        
        self.calculate_reservation_price(timestamp).await?;
        self.calculate_optimal_quotes(timestamp).await?;
        
        Ok(())
    }

    fn update_unrealized_pnl(&mut self) {
        self.unrealized_pnl = self.inventory * (self.mid_price - self.calculate_average_cost());
    }

    fn calculate_average_cost(&self) -> f64 {
        if self.inventory.abs() < f64::EPSILON {
            return self.mid_price;
        }
        
        let notional = self.realized_pnl + self.inventory * self.mid_price;
        notional / self.inventory
    }

    pub async fn get_quote(&self) -> Result<Quote, Box<dyn std::error::Error>> {
        if !self.active {
            return Err("Engine inactive".into());
        }
        
        let risk_manager = self.risk_manager.read().await;
        if risk_manager.trading_halted {
            return Err("Trading halted due to risk limits".into());
        }
        
        Ok(Quote {
            bid_price: self.bid_price,
            bid_size: self.order_size_bid,
            ask_price: self.ask_price,
            ask_size: self.order_size_ask,
            timestamp: self.last_update,
            spread: self.ask_price - self.bid_price,
            mid_price: self.mid_price,
        })
    }

    pub async fn update_parameters(
        &mut self,
        gamma: Option<f64>,
        kappa: Option<f64>,
        eta: Option<f64>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(g) = gamma {
            if g <= 0.0 || g > 1.0 {
                return Err("Invalid gamma parameter".into());
            }
            self.gamma = g;
        }
        
        if let Some(k) = kappa {
            if k <= 0.0 || k > 10.0 {
                return Err("Invalid kappa parameter".into());
            }
            self.kappa = k;
        }
        
        if let Some(e) = eta {
            if e <= 0.0 || e > 1.0 {
                return Err("Invalid eta parameter".into());
            }
            self.eta = e;
        }
        
        self.calculate_optimal_quotes(Clock::get()?.unix_timestamp).await?;
        
        Ok(())
    }

    pub async fn reset_daily_metrics(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut risk_manager = self.risk_manager.write().await;
        risk_manager.current_daily_pnl = 0.0;
        risk_manager.trading_halted = false;
        
        self.realized_pnl = 0.0;
        self.trade_count = 0;
        self.total_volume = 0.0;
        
        Ok(())
    }

    pub fn get_inventory_ratio(&self) -> f64 {
        self.inventory / self.max_inventory
    }

    pub fn get_spread_bps(&self) -> f64 {
        if self.mid_price > 0.0 {
            (self.ask_price - self.bid_price) / self.mid_price * 10000.0
        } else {
            0.0
        }
    }

    pub async fn emergency_close_position(&mut self, market_price: f64) -> Result<f64, Box<dyn std::error::Error>> {
        if self.inventory.abs() < f64::EPSILON {
            return Ok(0.0);
        }
        
        let fee_multiplier = if self.inventory > 0.0 {
            1.0 - TAKER_FEE_BPS / 10000.0
        } else {
            1.0 + TAKER_FEE_BPS / 10000.0
        };
        
        let close_pnl = self.inventory.abs() * market_price * fee_multiplier;
        
        if self.inventory > 0.0 {
            self.realized_pnl += close_pnl;
        } else {
            self.realized_pnl -= close_pnl;
        }
        
        self.inventory = 0.0;
        self.unrealized_pnl = 0.0;
        self.active = false;
        
        Ok(close_pnl)
    }

    pub fn calculate_sharpe_ratio(&self) -> f64 {
        let price_history = futures::executor::block_on(self.price_history.read());
        
        if price_history.len() < 2 {
            return 0.0;
        }
        
        let returns: Vec<f64> = price_history.windows(2)
            .map(|w| (w[1].price / w[0].price).ln())
            .collect();
        
        if returns.is_empty() {
            return 0.0;
        }
        
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / returns.len() as f64;
        
        if variance > 0.0 {
            mean_return / variance.sqrt() * (365.0_f64).sqrt()
        } else {
            0.0
        }
    }

    pub fn get_risk_metrics(&self) -> RiskMetrics {
        RiskMetrics {
            inventory_ratio: self.get_inventory_ratio(),
            spread_bps: self.get_spread_bps(),
            volatility: self.sigma,
            sharpe_ratio: self.calculate_sharpe_ratio(),
            total_pnl: self.realized_pnl + self.unrealized_pnl,
            realized_pnl: self.realized_pnl,
            unrealized_pnl: self.unrealized_pnl,
            trade_count: self.trade_count,
            total_volume: self.total_volume,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Clone, Debug)]
pub struct Quote {
    pub bid_price: f64,
    pub bid_size: f64,
    pub ask_price: f64,
    pub ask_size: f64,
    pub timestamp: i64,
    pub spread: f64,
    pub mid_price: f64,
}

#[derive(Clone, Debug)]
pub struct RiskMetrics {
    pub inventory_ratio: f64,
    pub spread_bps: f64,
    pub volatility: f64,
    pub sharpe_ratio: f64,
    pub total_pnl: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub trade_count: u64,
    pub total_volume: f64,
}

impl VolatilityEstimator {
    pub fn update_garch_params(&mut self, returns: &[f64]) {
        if returns.len() < 20 {
            return;
        }
        
        let mean_return = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean_return).powi(2))
            .sum::<f64>() / returns.len() as f64;
        
        self.omega = variance * 0.01;
        self.alpha = 0.05;
        self.beta = 0.94;
        
        let persistence = self.alpha + self.beta;
        if persistence >= 1.0 {
            self.beta = 0.99 - self.alpha;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_engine_initialization() {
        let engine = ASMMEngine::new(
            "SOL/USDC".to_string(),
            9,
            6,
            1000.0,
        );
        
        assert_eq!(engine.inventory, 0.0);
        assert_eq!(engine.gamma, GAMMA_DEFAULT);
        assert!(engine.active);
    }

    #[tokio::test]
    async fn test_quote_generation() {
        let mut engine = ASMMEngine::new(
            "SOL/USDC".to_string(),
            9,
            6,
            1000.0,
        );
        
        engine.update_market_data(100.0, 0.01, 1000).await.unwrap();
        
        let quote = engine.get_quote().await.unwrap();
        assert!(quote.bid_price < quote.ask_price);
        assert!(quote.bid_size > 0.0);
        assert!(quote.ask_size > 0.0);
    }
}

