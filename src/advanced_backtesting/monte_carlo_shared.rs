#![no_std]

use borsh::{BorshDeserialize, BorshSerialize};
use solana_program::{
    account_info::AccountInfo,
    entrypoint::ProgramResult,
    program_error::ProgramError,
    pubkey::Pubkey,
};
use rust_decimal::Decimal;

const FIXED_POINT_SCALE: u64 = 1_000_000_000;
const MAX_SIMULATION_PATHS: usize = 1000;
const MAX_TIME_STEPS: usize = 100;

#[derive(BorshSerialize, BorshDeserialize, Clone, Copy)]
pub struct SimulationConfig {
    pub num_paths: u32,
    pub time_steps: u32,
    pub initial_price: u64,
    pub volatility: u64,
    pub drift: u64,
    pub dt: u64,
    pub strike_price: u64,
    pub is_call_option: bool,
}

#[derive(BorshSerialize, BorshDeserialize, Clone, Copy)]
pub struct SimulationResults {
    pub expected_value: Decimal,
    pub variance: Decimal,
    pub percentile_5: Decimal,
    pub percentile_95: Decimal,
    pub max_profit: Decimal,
    pub max_loss: Decimal,
    pub sharpe_ratio: Decimal,
    pub paths_completed: u32,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub struct SimulationState {
    pub config: SimulationConfig,
    pub results: SimulationResults,
    pub current_path: u32,
    pub random_seed: u64,
}

pub fn process_simulation(
    _program_id: &Pubkey,
    accounts: &[AccountInfo],
    instruction_data: &[u8],
) -> ProgramResult {
    if accounts.is_empty() {
        return Err(ProgramError::NotEnoughAccountKeys);
    }

    let config = SimulationConfig::try_from_slice(instruction_data)
        .map_err(|_| ProgramError::InvalidInstructionData)?;

    let results = run_monte_carlo_simulation(&config)?;

    let state = SimulationState {
        config,
        results,
        current_path: config.num_paths,
        random_seed: 0,
    };

    let serialized = borsh::to_vec(&state)
        .map_err(|_| ProgramError::InvalidAccountData)?;

    let state_account = &accounts[0];
    let mut data = state_account.try_borrow_mut_data()?;
    
    if data.len() < serialized.len() {
        return Err(ProgramError::AccountDataTooSmall);
    }

    data[..serialized.len()].copy_from_slice(&serialized);

    Ok(())
}

fn run_monte_carlo_simulation(config: &SimulationConfig) -> Result<SimulationResults, ProgramError> {
    let initial_price = convert_from_fixed(config.initial_price);
    let volatility = convert_from_fixed(config.volatility);
    let drift = convert_from_fixed(config.drift);
    let dt = convert_from_fixed(config.dt);
    let strike_price = convert_from_fixed(config.strike_price);

    let mut rng = rand::rngs::StdRng::from_entropy();
    let mut payoffs = Vec::with_capacity(config.num_paths as usize);
    
    for _ in 0..config.num_paths {
        let mut price = initial_price;
        
        for _ in 0..config.time_steps {
            let z: f64 = rng.gen_range(-1.0..1.0);
            let drift_term = drift * dt;
            let diffusion_term = volatility * Decimal::from_f64(z).unwrap() * dt.sqrt();
            price *= (Decimal::ONE + drift_term + diffusion_term).max(Decimal::ZERO);
        }
        
        let payoff = if config.is_call_option {
            (price - strike_price).max(Decimal::ZERO)
        } else {
            (strike_price - price).max(Decimal::ZERO)
        };
        
        payoffs.push(payoff);
    }
    
    let expected_value = payoffs.iter().sum::<Decimal>() / Decimal::from(config.num_paths);
    let variance = payoffs.iter()
        .map(|x| (*x - expected_value).powu(2))
        .sum::<Decimal>() / Decimal::from(config.num_paths);
    
    let min_payoff = payoffs.iter().min().copied().unwrap_or(Decimal::ZERO);
    let max_payoff = payoffs.iter().max().copied().unwrap_or(Decimal::ZERO);
    
    let mut sorted_payoffs = payoffs.clone();
    sorted_payoffs.sort_by(|a, b| a.partial_cmp(b).unwrap());
    
    let percentile_5 = sorted_payoffs[(config.num_paths as f64 * 0.05) as usize];
    let percentile_95 = sorted_payoffs[(config.num_paths as f64 * 0.95) as usize];
    
    let std_dev = variance.sqrt();
    let sharpe_ratio = if std_dev == Decimal::ZERO {
        Decimal::ZERO
    } else {
        expected_value / std_dev
    };
    
    Ok(SimulationResults {
        expected_value,
        variance,
        percentile_5,
        percentile_95,
        max_profit: max_payoff,
        max_loss: min_payoff,
        sharpe_ratio,
        paths_completed: config.num_paths,
    })
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

    #[test]
    fn test_multiply_fixed_point() {
        let a = FIXED_POINT_SCALE; // 1.0
        let b = 2 * FIXED_POINT_SCALE; // 2.0
        let result = convert_to_fixed(convert_from_fixed(a) * convert_from_fixed(b));
        assert_eq!(result, 2 * FIXED_POINT_SCALE);
    }

    #[test]
    fn test_sqrt_fixed_point() {
        let x = 4 * FIXED_POINT_SCALE; // 4.0
        let result = convert_to_fixed(convert_from_fixed(x).sqrt());
        assert!((result as i64 - 2 * FIXED_POINT_SCALE as i64).abs() < 1000);
    }

    #[test]
    fn test_option_payoff_call() {
        let final_price = 110 * FIXED_POINT_SCALE;
        let strike_price = 100 * FIXED_POINT_SCALE;
        let payoff = if true {
            (convert_from_fixed(final_price) - convert_from_fixed(strike_price)).max(Decimal::ZERO)
        } else {
            (convert_from_fixed(strike_price) - convert_from_fixed(final_price)).max(Decimal::ZERO)
        };
        assert_eq!(payoff, 10 * convert_from_fixed(FIXED_POINT_SCALE));
    }

    #[test]
    fn test_option_payoff_put() {
        let final_price = 90 * FIXED_POINT_SCALE;
        let strike_price = 100 * FIXED_POINT_SCALE;
        let payoff = if true {
            (convert_from_fixed(final_price) - convert_from_fixed(strike_price)).max(Decimal::ZERO)
        } else {
            (convert_from_fixed(strike_price) - convert_from_fixed(final_price)).max(Decimal::ZERO)
        };
        assert_eq!(payoff, 10 * convert_from_fixed(FIXED_POINT_SCALE));
    }
}
