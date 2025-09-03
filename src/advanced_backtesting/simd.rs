use simdeez::prelude::*;
use rust_decimal::prelude::*;
use rayon::prelude::*;
use rand_pcg::Pcg64Mcg;
use rand::Rng;
use crate::advanced_backtesting::monte_carlo_shared::SimulationConfig;

const FIXED_POINT_SCALE: u64 = 1_000_000_000;

pub fn run_simd_mc(config: &SimulationConfig) -> Result<Decimal, &'static str> {
    // Convert fixed-point inputs to Decimal
    let initial_price = convert_from_fixed(config.initial_price);
    let volatility = convert_from_fixed(config.volatility);
    let drift = convert_from_fixed(config.drift);
    let dt = convert_from_fixed(config.dt);
    let strike_price = convert_from_fixed(config.strike_price);

    let results: Vec<Decimal> = (0..config.num_paths)
        .into_par_iter()
        .map(|i| {
            let mut rng = Pcg64Mcg::new(i as u64);
            let mut price = initial_price;
            
            for _ in 0..config.time_steps {
                let z: f64 = rng.gen_range(-1.0..1.0);
                let drift_term = drift * dt;
                let diffusion_term = volatility * Decimal::from_f64(z).unwrap() * dt.sqrt();
                price *= (Decimal::ONE + drift_term + diffusion_term).max(Decimal::ZERO);
            }
            
            if config.is_call_option {
                (price - strike_price).max(Decimal::ZERO)
            } else {
                (strike_price - price).max(Decimal::ZERO)
            }
        })
        .collect();

    Ok(results.iter().sum::<Decimal>() / Decimal::from(config.num_paths))
}

fn convert_from_fixed(v: u64) -> Decimal {
    Decimal::from(v) / Decimal::from(FIXED_POINT_SCALE)
}

fn convert_to_fixed(d: Decimal) -> u64 {
    (d * Decimal::from(FIXED_POINT_SCALE))
        .to_u64()
        .unwrap_or_else(|| panic!("Fixed point conversion overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    
    #[test]
    fn test_simd_mc() {
        let config = SimulationConfig {
            num_paths: 10_000,
            time_steps: 365,
            initial_price: convert_to_fixed(dec!(100.00)),
            volatility: convert_to_fixed(dec!(0.20)),
            drift: convert_to_fixed(dec!(0.05)),
            dt: convert_to_fixed(dec!(0.001)),
            strike_price: convert_to_fixed(dec!(105.00)),
            is_call_option: true,
        };
        
        let result = run_simd_mc(&config).unwrap();
        assert!(result > Decimal::ZERO);
    }
}
