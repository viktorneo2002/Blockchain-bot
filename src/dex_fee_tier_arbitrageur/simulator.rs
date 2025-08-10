use anyhow::{anyhow, Result};
use crate::config::Q64;

/// Convert fee_rate (hundredths of basis point) to fraction
/// Example: fee_rate = 3000 -> 0.003 (0.3%)
#[inline]
pub fn fee_fraction_from_hundredths_bps(fee_rate: u16) -> f64 {
    (fee_rate as f64) / 1_000_000.0
}

/// Convert sqrt_price Q64.64 to f64 price (for logging only)
/// Do not use for swap calculations - use integer math
#[inline]
pub fn sqrt_q64_to_price_f64(sqrt_q64: u128) -> f64 {
    let s = (sqrt_q64 as f64) / (2f64.powi(64));
    s * s
}

/// Convert price to sqrt_price Q64.64
#[inline]
pub fn price_to_sqrt_q64(price: f64) -> u128 {
    (price.sqrt() * (2f64.powi(64))) as u128
}

/// Safe checked multiplication and division
#[inline]
pub fn checked_mul_div(a: u128, b: u128, d: u128) -> Option<u128> {
    if d == 0 {
        return None;
    }
    
    // Check for overflow in multiplication
    match a.checked_mul(b) {
        Some(product) => product.checked_div(d),
        None => {
            // Try to reduce before multiplication
            let gcd = gcd(b, d);
            let b_reduced = b / gcd;
            let d_reduced = d / gcd;
            a.checked_mul(b_reduced)?.checked_div(d_reduced)
        }
    }
}

fn gcd(mut a: u128, mut b: u128) -> u128 {
    while b != 0 {
        let temp = b;
        b = a % b;
        a = temp;
    }
    a
}

/// Apply Raydium trade fee to input amount
/// amount_after_fee = amount_in * (denominator - numerator) / denominator
#[inline]
pub fn apply_raydium_trade_fee(amount_in: u128, numerator: u128, denominator: u128) -> u128 {
    if denominator == 0 || numerator >= denominator {
        return 0;
    }
    
    checked_mul_div(
        amount_in,
        denominator.saturating_sub(numerator),
        denominator,
    ).unwrap_or(0)
}

/// Simulate Orca Whirlpool swap
/// This is a simplified version - production should use the exact tick-based math
/// from the Orca SDK or replicate the on-chain calculation exactly
pub fn simulate_orca_swap(
    sqrt_price: u128,
    liquidity: u128,
    fee_rate: u16,
    amount_in: u128,
    a_to_b: bool,
) -> Result<u128> {
    if liquidity == 0 || sqrt_price == 0 {
        return Err(anyhow!("Invalid pool state: zero liquidity or price"));
    }
    
    // Apply fee (hundredths of basis point)
    let fee_fraction = fee_fraction_from_hundredths_bps(fee_rate);
    let amount_after_fee = ((amount_in as f64) * (1.0 - fee_fraction)) as u128;
    
    // Simplified constant product approximation
    // WARNING: This is not exact - use Orca SDK for production
    let price = sqrt_q64_to_price_f64(sqrt_price);
    
    let amount_out = if a_to_b {
        // A -> B: output = input * price
        ((amount_after_fee as f64) * price) as u128
    } else {
        // B -> A: output = input / price
        ((amount_after_fee as f64) / price) as u128
    };
    
    // Apply slippage safety margin (round down)
    let amount_out_safe = amount_out.saturating_mul(997).saturating_div(1000);
    
    Ok(amount_out_safe)
}

/// Simulate Raydium v4 swap (constant product AMM)
/// Validated against raydium-amm/program/src/math.rs
pub fn simulate_raydium_swap(
    reserve_in: u128,
    reserve_out: u128,
    amount_in: u128,
    fee_numerator: u64,
    fee_denominator: u64,
) -> Result<u128> {
    if reserve_in == 0 || reserve_out == 0 {
        return Err(anyhow!("Invalid reserves: zero liquidity"));
    }
    
    if fee_denominator == 0 {
        return Err(anyhow!("Invalid fee: zero denominator"));
    }
    
    // Apply trade fee first (as per Raydium math.rs)
    let amount_after_fee = apply_raydium_trade_fee(
        amount_in,
        fee_numerator as u128,
        fee_denominator as u128,
    );
    
    if amount_after_fee == 0 {
        return Ok(0);
    }
    
    // Constant product formula: out = (reserve_out * amount_after_fee) / (reserve_in + amount_after_fee)
    // Using safe checked math to avoid overflow
    let numerator = checked_mul_div(
        reserve_out,
        amount_after_fee,
        reserve_in.saturating_add(amount_after_fee),
    );
    
    match numerator {
        Some(amount_out) => {
            // Apply safety margin for slippage (round down)
            let amount_out_safe = amount_out.saturating_mul(997).saturating_div(1000);
            Ok(amount_out_safe)
        }
        None => Err(anyhow!("Overflow in swap calculation")),
    }
}

/// Calculate minimum amount out with slippage tolerance
pub fn calculate_min_amount_out(expected_out: u128, slippage_bps: u64) -> u128 {
    let slippage_factor = 10000u128.saturating_sub(slippage_bps as u128);
    expected_out.saturating_mul(slippage_factor).saturating_div(10000)
}

/// Estimate arbitrage profit between two pools
pub fn estimate_arbitrage_profit(
    amount_in: u128,
    amount_intermediate: u128,
    amount_final: u128,
    gas_cost: u128,
    tip_cost: u128,
) -> i128 {
    let total_cost = amount_in.saturating_add(gas_cost).saturating_add(tip_cost);
    
    if amount_final > total_cost {
        (amount_final - total_cost) as i128
    } else {
        -((total_cost - amount_final) as i128)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fee_conversion() {
        // 30 bps = 3000 hundredths of bps = 0.003
        assert_eq!(fee_fraction_from_hundredths_bps(3000), 0.003);
        
        // 1 bps = 100 hundredths of bps = 0.0001
        assert_eq!(fee_fraction_from_hundredths_bps(100), 0.0001);
        
        // 0.05% = 5 bps = 500 hundredths of bps
        assert_eq!(fee_fraction_from_hundredths_bps(500), 0.0005);
    }

    #[test]
    fn test_raydium_fee_application() {
        // 0.25% fee (25/10000)
        let amount_in = 1_000_000u128;
        let fee_num = 25u128;
        let fee_den = 10000u128;
        
        let after_fee = apply_raydium_trade_fee(amount_in, fee_num, fee_den);
        assert_eq!(after_fee, 997_500); // 1M * (10000-25)/10000
    }

    #[test]
    fn test_raydium_swap() {
        let reserve_in = 1_000_000u128;
        let reserve_out = 2_000_000u128;
        let amount_in = 10_000u128;
        let fee_num = 25u64;
        let fee_den = 10000u64;
        
        let result = simulate_raydium_swap(
            reserve_in,
            reserve_out,
            amount_in,
            fee_num,
            fee_den,
        );
        
        assert!(result.is_ok());
        let amount_out = result.unwrap();
        
        // After fee: 10000 * 9975/10000 = 9975
        // Out: 2000000 * 9975 / (1000000 + 9975) ≈ 19752
        // With safety margin: 19752 * 997/1000 ≈ 19692
        assert!(amount_out > 19000 && amount_out < 20000);
    }

    #[test]
    fn test_checked_mul_div() {
        assert_eq!(checked_mul_div(10, 20, 5), Some(40));
        assert_eq!(checked_mul_div(u128::MAX / 2, 2, 2), Some(u128::MAX / 2));
        assert_eq!(checked_mul_div(100, 200, 0), None); // Division by zero
    }

    #[test]
    fn test_min_amount_out() {
        // 1% slippage
        assert_eq!(calculate_min_amount_out(100_000, 100), 99_000);
        
        // 0.3% slippage
        assert_eq!(calculate_min_amount_out(100_000, 30), 99_700);
    }
}
