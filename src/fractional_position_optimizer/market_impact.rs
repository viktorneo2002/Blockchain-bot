//! Market impact models and regime classification for trading strategies
//! 
//! This module provides tools for estimating trade impact and classifying market regimes
//! to inform trading decisions and risk management.

use std::cmp::Ordering;

/// Market regime classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MarketRegime {
    /// Strong directional movement with high momentum
    Trending,
    /// Sideways price action with mean-reverting behavior
    Ranging,
    /// High volatility with large price swings
    Volatile,
    /// Low liquidity with wide spreads
    Illiquid,
    /// Abnormal price action around news/events
    EventDriven,
}

impl Default for MarketRegime {
    fn default() -> Self {
        Self::Ranging
    }
}

/// Calculate CPMM (Constant Product Market Maker) price impact for a trade
/// 
/// # Arguments
/// * `size` - Size of the trade in base asset units
/// * `base_reserve` - Current base asset reserve in the pool
/// * `quote_reserve` - Current quote asset reserve in the pool
/// 
/// # Returns
/// Price impact as a fraction (e.g., 0.01 for 1% impact)
/// 
/// # Example
/// ```
/// use your_crate::market_impact::cpmm_expected_impact;
/// 
/// // For a pool with 1000 ETH and 3,000,000 USDC
/// let impact = cpmm_expected_impact(10.0, 1000.0, 3_000_000.0);
/// ```
pub fn cpmm_expected_impact(size: f64, base_reserve: f64, quote_reserve: f64) -> f64 {
    if base_reserve <= 0.0 || quote_reserve <= 0.0 {
        return 0.0;
    }
    size / (base_reserve + size)
}

/// Calculate order book impact for a given trade size
/// 
/// # Arguments
/// * `size` - Size of the trade in base asset units
/// * `levels` - Vector of (price, available_quantity) tuples, sorted by price
/// 
/// # Returns
/// Volume-weighted average price impact as a fraction of mid price
/// 
/// # Example
/// ```
/// use your_crate::market_impact::ob_impact;
/// 
/// let orderbook = vec![(100.0, 5.0), (101.0, 3.0), (102.0, 2.0)];
/// let impact = ob_impact(8.0, &orderbook);
/// ```
pub fn ob_impact(size: f64, levels: &[(f64, f64)]) -> f64 {
    if size <= 0.0 || levels.is_empty() {
        return 0.0;
    }
    
    let mut remain = size;
    let mut total_cost = 0.0;
    
    for (price, qty) in levels {
        let take = remain.min(*qty);
        total_cost += take * price;
        remain -= take;
        
        if remain <= 0.0 {
            break;
        }
    }
    
    if size > 0.0 {
        let vwap = total_cost / size;
        let mid_price = levels.first().map_or(0.0, |(p, _)| *p);
        
        if mid_price > 0.0 {
            (vwap / mid_price) - 1.0
        } else {
            0.0
        }
    } else {
        0.0
    }
}

/// Check if trade impact is within acceptable bounds
/// 
/// # Arguments
/// * `size` - Trade size in base asset units
/// * `base_reserve` - Base asset reserve in the pool
/// * `quote_reserve` - Quote asset reserve in the pool
/// * `max_slippage` - Maximum allowed slippage as a fraction (e.g., 0.01 for 1%)
/// 
/// # Returns
/// `Ok(impact)` if within bounds, `Err(impact)` if exceeds bounds
/// 
/// # Example
/// ```
/// use your_crate::market_impact::check_impact_bounds;
/// 
/// let result = check_impact_bounds(10.0, 1000.0, 3_000_000.0, 0.01);
/// assert!(result.is_ok());
/// ```
pub fn check_impact_bounds(
    size: f64,
    base_reserve: f64,
    quote_reserve: f64,
    max_slippage: f64,
) -> Result<f64, f64> {
    let impact = cpmm_expected_impact(size, base_reserve, quote_reserve);
    
    if impact <= max_slippage {
        Ok(impact)
    } else {
        Err(impact)
    }
}

/// Classify market regime based on recent price action and liquidity
/// 
/// # Arguments
/// * `returns` - Array of recent returns (e.g., log returns)
/// * `spreads` - Array of recent bid-ask spreads as fractions of mid price
/// * `volatility_window` - Window size for volatility calculation
/// 
/// # Returns
/// `MarketRegime` classification
/// 
/// # Example
/// ```
/// use your_crate::market_impact::{classify_regime, MarketRegime};
/// 
/// let returns = vec![0.01, -0.005, 0.008, -0.002, 0.015];
/// let spreads = vec![0.0005, 0.0006, 0.0004, 0.0007, 0.0005];
/// let regime = classify_regime(&returns, &spreads, 0.94);
/// ```
pub fn classify_regime(returns: &[f64], spreads: &[f64], decay: f64) -> MarketRegime {
    if returns.is_empty() || spreads.is_empty() {
        return MarketRegime::default();
    }
    
    // Calculate EWMA volatility
    let vol = ewma_vol(returns, decay);
    
    // Calculate average spread
    let avg_spread = spreads.iter().sum::<f64>() / spreads.len() as f64;
    
    // Simple regime classification - can be enhanced with more sophisticated logic
    match () {
        _ if avg_spread > 0.005 => MarketRegime::Illiquid,
        _ if vol > 0.05 => MarketRegime::Volatile,
        _ if is_trending(returns) => MarketRegime::Trending,
        _ => MarketRegime::Ranging,
    }
}

/// Calculate Exponentially Weighted Moving Average (EWMA) volatility
fn ewma_vol(returns: &[f64], lambda: f64) -> f64 {
    if returns.is_empty() || lambda <= 0.0 || lambda >= 1.0 {
        return 0.0;
    }
    
    let mut variance = 0.0;
    let mut weight_sum = 0.0;
    let mut weight = 1.0;
    
    for &r in returns.iter().rev() {
        variance += weight * r.powi(2);
        weight_sum += weight;
        weight *= lambda;
        
        if weight < 1e-6 {
            break;
        }
    }
    
    if weight_sum > 0.0 {
        (variance / weight_sum).sqrt()
    } else {
        0.0
    }
}

/// Simple trend detection using linear regression
fn is_trending(returns: &[f64]) -> bool {
    if returns.len() < 2 {
        return false;
    }
    
    // Simple trend detection: more than 60% of returns have the same sign
    let positive = returns.iter().filter(|&&r| r > 0.0).count();
    let ratio = positive as f64 / returns.len() as f64;
    
    ratio > 0.6 || ratio < 0.4
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;
    
    #[test]
    fn test_cpmm_impact() {
        // Test basic impact calculation
        let impact = cpmm_expected_impact(10.0, 1000.0, 3_000_000.0);
        assert_relative_eq!(impact, 0.00990099, epsilon = 1e-6);
        
        // Test edge cases
        assert_eq!(cpmm_expected_impact(0.0, 1000.0, 3_000_000.0), 0.0);
        assert_eq!(cpmm_expected_impact(10.0, 0.0, 3_000_000.0), 0.0);
    }
    
    #[test]
    fn test_ob_impact() {
        let orderbook = vec![(100.0, 5.0), (101.0, 3.0), (102.0, 2.0)];
        
        // Test partial fill of first level
        assert_relative_eq!(ob_impact(3.0, &orderbook), 0.0, epsilon = 1e-6);
        
        // Test fill across multiple levels
        let impact = ob_impact(8.0, &orderbook);
        let expected_vwap = (5.0 * 100.0 + 3.0 * 101.0) / 8.0;
        let expected_impact = (expected_vwap / 100.0) - 1.0;
        assert_relative_eq!(impact, expected_impact, epsilon = 1e-6);
        
        // Test edge cases
        assert_eq!(ob_impact(0.0, &orderbook), 0.0);
        assert_eq!(ob_impact(100.0, &orderbook), 0.0); // Returns 0.0 when mid price is 0.0
    }
    
    #[test]
    fn test_check_impact_bounds() {
        // Test within bounds
        let result = check_impact_bounds(10.0, 1000.0, 3_000_000.0, 0.01);
        assert!(result.is_ok());
        
        // Test exceeds bounds
        let result = check_impact_bounds(100.0, 100.0, 300_000.0, 0.01);
        assert!(result.is_err());
    }
    
    #[test]
    fn test_classify_regime() {
        // Test volatile regime
        let returns = vec![0.1, -0.08, 0.12, -0.1, 0.15];
        let spreads = vec![0.001, 0.0012, 0.0011, 0.0013, 0.0010];
        assert_eq!(classify_regime(&returns, &spreads, 0.94), MarketRegime::Volatile);
        
        // Test illiquid regime
        let returns = vec![0.01, -0.005, 0.008, -0.002, 0.015];
        let spreads = vec![0.01, 0.012, 0.015, 0.011, 0.013];
        assert_eq!(classify_regime(&returns, &spreads, 0.94), MarketRegime::Illiquid);
        
        // Test trending regime
        let returns = vec![0.01, 0.02, 0.015, 0.018, 0.01];
        let spreads = vec![0.001, 0.0009, 0.0011, 0.0008, 0.0010];
        assert_eq!(classify_regime(&returns, &spreads, 0.94), MarketRegime::Trending);
        
        // Test ranging regime (default)
        let returns = vec![0.01, -0.01, 0.01, -0.01, 0.01];
        let spreads = vec![0.001, 0.001, 0.001, 0.001, 0.001];
        assert_eq!(classify_regime(&returns, &spreads, 0.94), MarketRegime::Ranging);
    }
}
