//! Risk management and circuit breakers for the Ornstein-Uhlenbeck mean reversion bot

use solana_sdk::pubkey::Pubkey;
use std::time::{Instant, Duration};
use std::collections::HashMap;
use thiserror::Error;

/// Configuration for risk management parameters
#[derive(Debug, Clone)]
pub struct RiskConfig {
    /// Maximum position size as a fraction of NAV (0.0 to 1.0)
    pub max_position_size: f64,
    /// Maximum daily loss as a fraction of NAV (0.0 to 1.0)
    pub max_daily_loss: f64,
    /// Maximum hourly loss as a fraction of NAV (0.0 to 1.0)
    pub max_hourly_loss: f64,
    /// Maximum number of trades per hour
    pub max_hourly_trades: u64,
    /// Maximum slippage allowed per trade as a fraction (0.0 to 1.0)
    pub max_slippage: f64,
    /// Minimum time between trades to prevent rapid trading
    pub min_trade_interval: Duration,
}

impl Default for RiskConfig {
    fn default() -> Self {
        Self {
            max_position_size: 0.33,  // 33% of NAV
            max_daily_loss: 0.05,     // 5% of NAV
            max_hourly_loss: 0.02,    // 2% of NAV
            max_hourly_trades: 50,
            max_slippage: 0.005,      // 0.5%
            min_trade_interval: Duration::from_secs(10), // 10 seconds
        }
    }
}

/// Tracks trading activity for risk management
#[derive(Debug)]
pub struct RiskManager {
    config: RiskConfig,
    last_trade_time: Instant,
    hourly_trade_count: u64,
    hourly_loss: f64,
    daily_loss: f64,
    last_reset: Instant,
    position_sizes: HashMap<Pubkey, f64>, // token mint -> position size in USD
}

#[derive(Error, Debug)]
pub enum RiskError {
    #[error("Position size {0} exceeds maximum allowed {1}")]
    PositionSizeExceeded(f64, f64),
    
    #[error("Daily loss limit exceeded: {0} > {1}")]
    DailyLossLimitExceeded(f64, f64),
    
    #[error("Hourly loss limit exceeded: {0} > {1}")]
    HourlyLossLimitExceeded(f64, f64),
    
    #[error("Maximum hourly trades exceeded: {0} > {1}")]
    MaxTradesExceeded(u64, u64),
    
    #[error("Trade too soon after previous trade")]
    TradeTooSoon,
    
    #[error("Slippage {0} exceeds maximum allowed {1}")]
    SlippageExceeded(f64, f64),
}

impl RiskManager {
    /// Creates a new RiskManager with default configuration
    pub fn new() -> Self {
        Self {
            config: RiskConfig::default(),
            last_trade_time: Instant::now() - Duration::from_secs(3600), // Start with a valid time
            hourly_trade_count: 0,
            hourly_loss: 0.0,
            daily_loss: 0.0,
            last_reset: Instant::now(),
            position_sizes: HashMap::new(),
        }
    }
    
    /// Creates a new RiskManager with custom configuration
    pub fn with_config(config: RiskConfig) -> Self {
        Self {
            config,
            ..Self::new()
        }
    }
    
    /// Validates a potential trade against risk parameters
    pub fn validate_trade(
        &mut self,
        token_mint: &Pubkey,
        position_size: f64,
        nav: f64,
        estimated_slippage: f64,
    ) -> Result<(), RiskError> {
        self.check_reset();
        
        // Check position size limit
        let position_fraction = position_size / nav;
        if position_fraction > self.config.max_position_size {
            return Err(RiskError::PositionSizeExceeded(
                position_fraction,
                self.config.max_position_size,
            ));
        }
        
        // Check trade frequency
        if self.hourly_trade_count >= self.config.max_hourly_trades {
            return Err(RiskError::MaxTradesExceeded(
                self.hourly_trade_count,
                self.config.max_hourly_trades,
            ));
        }
        
        // Check minimum time between trades
        let time_since_last = self.last_trade_time.elapsed();
        if time_since_last < self.config.min_trade_interval {
            return Err(RiskError::TradeTooSoon);
        }
        
        // Check slippage
        if estimated_slippage > self.config.max_slippage {
            return Err(RiskError::SlippageExceeded(
                estimated_slippage,
                self.config.max_slippage,
            ));
        }
        
        // If we get here, the trade is valid
        self.hourly_trade_count += 1;
        self.last_trade_time = Instant::now();
        
        // Update position size
        self.position_sizes
            .entry(*token_mint)
            .and_modify(|e| *e += position_size)
            .or_insert(position_size);
            
        Ok(())
    }
    
    /// Records a realized loss
    pub fn record_loss(&mut self, loss_amount: f64) {
        self.hourly_loss += loss_amount;
        self.daily_loss += loss_amount;
    }
    
    /// Checks if we need to reset hourly/daily counters
    fn check_reset(&mut self) {
        let now = Instant::now();
        
        // Reset hourly counters if more than an hour has passed
        if now.duration_since(self.last_reset) >= Duration::from_secs(3600) {
            self.hourly_trade_count = 0;
            self.hourly_loss = 0.0;
            self.last_reset = now;
            
            // Reset daily counters if more than 24 hours have passed
            if now.duration_since(self.last_reset) >= Duration::from_secs(24 * 3600) {
                self.daily_loss = 0.0;
            }
        }
    }
    
    /// Checks if the current loss limits are being approached
    pub fn check_loss_limits(&self, nav: f64) -> Result<(), RiskError> {
        if self.hourly_loss > self.config.max_hourly_loss * nav {
            return Err(RiskError::HourlyLossLimitExceeded(
                self.hourly_loss,
                self.config.max_hourly_loss * nav,
            ));
        }
        
        if self.daily_loss > self.config.max_daily_loss * nav {
            return Err(RiskError::DailyLossLimitExceeded(
                self.daily_loss,
                self.config.max_daily_loss * nav,
            ));
        }
        
        Ok(())
    }
    
    /// Gets the current position size for a token
    pub fn get_position_size(&self, token_mint: &Pubkey) -> f64 {
        self.position_sizes.get(token_mint).copied().unwrap_or(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::new_rand;
    
    #[test]
    fn test_risk_limits() {
        let mut rm = RiskManager::new();
        let token = new_rand();
        let nav = 100_000.0; // $100k NAV
        
        // Test position size limit
        let result = rm.validate_trade(&token, 50_000.0, nav, 0.001);
        assert!(matches!(result, Err(RiskError::PositionSizeExceeded(0.5, 0.33))));
        
        // Test valid trade
        assert!(rm.validate_trade(&token, 10_000.0, nav, 0.001).is_ok());
        
        // Test trade too soon
        let result = rm.validate_trade(&token, 5_000.0, nav, 0.001);
        assert!(matches!(result, Err(RiskError::TradeTooSoon)));
        
        // Test slippage
        let result = rm.validate_trade(&token, 5_000.0, nav, 0.01);
        assert!(matches!(result, Err(RiskError::SlippageExceeded(0.01, 0.005))));
    }
    
    #[test]
    fn test_loss_limits() {
        let mut rm = RiskManager::new();
        let nav = 100_000.0;
        
        // Record a loss that's under the hourly limit but over the daily limit
        rm.record_loss(3_000.0); // 3% of NAV
        assert!(rm.check_loss_limits(nav).is_ok());
        
        // Record more losses to exceed hourly limit
        rm.record_loss(2_000.0); // Now at 5% of NAV
        assert!(matches!(
            rm.check_loss_limits(nav),
            Err(RiskError::HourlyLossLimitExceeded(5_000.0, 2_000.0))
        ));
    }
}
