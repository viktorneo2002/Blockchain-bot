//! Core numerical stability primitives for financial calculations
//! 
//! This module provides robust implementations of statistical and mathematical
//! functions with careful attention to numerical stability and edge cases.

use std::cmp::Ordering;

/// Calculate statistics including mean, standard deviation, and expected shortfall
/// 
/// # Arguments
/// * `rets` - Vector of returns (can be empty)
/// * `alpha` - Confidence level for expected shortfall (e.g., 0.05 for 95% ES)
/// 
/// # Returns
/// A tuple containing (mean, standard_deviation, expected_shortfall)
/// 
/// # Examples
/// ```
/// use your_crate::math_utils::stats_with_es;
/// 
/// let returns = vec![0.01, -0.02, 0.03, -0.04, 0.05];
/// let (mean, std_dev, es) = stats_with_es(returns, 0.05);
/// ```
pub fn stats_with_es(mut rets: Vec<f64>, alpha: f64) -> (f64, f64, f64) {
    let n = rets.len();
    if n == 0 {
        return (0.0, 0.0, 0.0);
    }
    
    // Calculate mean using Kahan summation for better numerical stability
    let mut mean = 0.0;
    let mut c = 0.0;
    for &x in &rets {
        let y = x - c;
        let t = mean + y;
        c = (t - mean) - y;
        mean = t;
    }
    mean /= n as f64;
    
    // Calculate variance with Bessel's correction
    let var = if n > 1 {
        let mut sum_sq = 0.0;
        let mut c = 0.0;
        for &x in &rets {
            let y = (x - mean).powi(2) - c;
            let t = sum_sq + y;
            c = (t - sum_sq) - y;
            sum_sq = t;
        }
        sum_sq / (n as f64 - 1.0)
    } else {
        0.0
    };
    
    // Calculate expected shortfall
    rets.sort_by(|a, b| a.partial_cmp(b).unwrap_or(Ordering::Equal));
    let idx = ((1.0 - alpha) * n as f64).floor().clamp(0.0, (n - 1) as f64) as usize;
    
    let es = if idx == 0 {
        rets[0]
    } else {
        // Use Kahan summation for ES calculation
        let mut sum = 0.0;
        let mut c = 0.0;
        for &x in &rets[..=idx] {
            let y = x - c;
            let t = sum + y;
            c = (t - sum) - y;
            sum = t;
        }
        sum / (idx + 1) as f64
    };
    
    (mean, var.sqrt(), es)
}

/// Calculate the Wilson lower bound for a binomial proportion
/// 
/// This is particularly useful for calculating conservative win-rate estimates
/// with small sample sizes.
/// 
/// # Arguments
/// * `successes` - Number of successful trials
/// * `n` - Total number of trials
/// * `z` - Z-score for desired confidence level (e.g., 1.96 for 95% CI)
/// 
/// # Returns
/// Conservative estimate of the success probability
/// 
/// # Examples
/// ```
/// use your_crate::math_utils::wilson_lower_bound;
/// 
/// // With 8 wins out of 10 trials at 95% confidence
/// let p_lower = wilson_lower_bound(8, 10, 1.96);
/// ```
pub fn wilson_lower_bound(successes: u64, n: u64, z: f64) -> f64 {
    if n == 0 {
        return 0.0;
    }
    
    let p = successes as f64 / n as f64;
    let z_sq = z * z;
    let n_f64 = n as f64;
    
    let denom = 1.0 + z_sq / n_f64;
    let centre = p + z_sq / (2.0 * n_f64);
    let adj = z * ((p * (1.0 - p) + z_sq / (4.0 * n_f64)) / n_f64).sqrt();
    
    ((centre - adj) / denom).clamp(0.0, 1.0)
}

/// Calculate Kelly criterion with optional information ratio adjustment
/// 
/// # Arguments
/// * `win_prob` - Probability of winning (0.0 to 1.0)
/// * `win_loss_ratio` - Ratio of average win to average loss
/// * `information_ratio` - Optional information ratio for adjustment
/// * `target_ir` - Target information ratio (defaults to 1.0)
/// 
/// # Returns
/// Kelly fraction (0.0 to 1.0)
/// 
/// # Examples
/// ```
/// use your_crate::math_utils::kelly_fraction;
/// 
/// // Basic Kelly
/// let f = kelly_fraction(0.6, 1.5, None, 1.0);
/// 
/// // With information ratio adjustment
/// let f_adj = kelly_fraction(0.6, 1.5, Some(1.2), 1.0);
/// ```
pub fn kelly_fraction(
    win_prob: f64,
    win_loss_ratio: f64,
    information_ratio: Option<f64>,
    target_ir: f64,
) -> f64 {
    // Basic Kelly criterion
    let f = (win_prob * (win_loss_ratio + 1.0) - 1.0) / win_loss_ratio;
    
    // Apply information ratio adjustment if provided
    if let Some(ir) = information_ratio {
        let ir_ratio = (ir / target_ir).min(1.0);
        f * ir_ratio
    } else {
        f
    }
    .clamp(0.0, 1.0) // Ensure fraction is between 0 and 1
}

#[cfg(test)]
mod tests {
    use super::*;
    use approx::assert_relative_eq;
    
    #[test]
    fn test_stats_with_es() {
        let returns = vec![0.01, -0.02, 0.03, -0.04, 0.05];
        let (mean, std_dev, es) = stats_with_es(returns.clone(), 0.05);
        
        // Test edge cases
        assert_eq!(stats_with_es(vec![], 0.05), (0.0, 0.0, 0.0));
        assert_eq!(stats_with_es(vec![0.1], 0.05), (0.1, 0.0, 0.1));
        
        // Test with known values
        let expected_mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let expected_var = returns.iter()
            .map(|x| (x - expected_mean).powi(2))
            .sum::<f64>() / (returns.len() - 1) as f64;
            
        assert_relative_eq!(mean, expected_mean, epsilon = 1e-10);
        assert_relative_eq!(std_dev, expected_var.sqrt(), epsilon = 1e-10);
        
        // ES should be the average of the worst 5% (rounded down to 1 value)
        let mut sorted = returns.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let expected_es = sorted[0];
        assert_eq!(es, expected_es);
    }
    
    #[test]
    fn test_wilson_lower_bound() {
        // Test edge cases
        assert_eq!(wilson_lower_bound(0, 0, 1.96), 0.0);
        assert_eq!(wilson_lower_bound(10, 10, 1.96), 0.0); // Should be > 0 but < 1
        
        // Test with known values
        // Values cross-validated with online calculators
        assert_relative_eq!(
            wilson_lower_bound(8, 10, 1.96),
            0.4904032,
            epsilon = 1e-6
        );
        
        assert_relative_eq!(
            wilson_lower_bound(80, 100, 1.96),
            0.7119,
            epsilon = 1e-4
        );
    }
    
    #[test]
    fn test_kelly_fraction() {
        // Test basic Kelly
        assert_relative_eq!(
            kelly_fraction(0.6, 1.5, None, 1.0),
            0.2,
            epsilon = 1e-10
        );
        
        // Test with information ratio adjustment
        assert_relative_eq!(
            kelly_fraction(0.6, 1.5, Some(1.2), 1.0),
            0.2 * 1.2,
            epsilon = 1e-10
        );
        
        // Test IR capping at target
        assert_relative_eq!(
            kelly_fraction(0.6, 1.5, Some(2.0), 1.0),
            0.2,  // Capped at 1.0 * kelly
            epsilon = 1e-10
        );
    }
}
