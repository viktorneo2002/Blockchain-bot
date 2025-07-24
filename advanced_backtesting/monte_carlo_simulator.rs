#![no_std]

use borsh::{BorshDeserialize, BorshSerialize};
use solana_program::{
    account_info::AccountInfo,
    entrypoint::ProgramResult,
    program_error::ProgramError,
    pubkey::Pubkey,
};

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
    pub expected_value: u64,
    pub variance: u64,
    pub percentile_5: u64,
    pub percentile_95: u64,
    pub max_profit: u64,
    pub max_loss: u64,
    pub sharpe_ratio: u64,
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
    let num_paths = config.num_paths.min(MAX_SIMULATION_PATHS as u32);
    let time_steps = config.time_steps.min(MAX_TIME_STEPS as u32);

    let mut sum_payoffs: u64 = 0;
    let mut sum_squared_payoffs: u64 = 0;
    let mut max_profit: u64 = 0;
    let mut max_loss: u64 = u64::MAX;
    let mut payoffs: [u64; MAX_SIMULATION_PATHS] = [0; MAX_SIMULATION_PATHS];

    let mut rng_state = config.initial_price ^ config.volatility;

    for path_idx in 0..num_paths {
        let payoff = simulate_single_path(
            config,
            time_steps,
            &mut rng_state,
        )?;

        payoffs[path_idx as usize] = payoff;
        sum_payoffs = sum_payoffs.saturating_add(payoff);
        sum_squared_payoffs = sum_squared_payoffs.saturating_add(
            multiply_fixed_point(payoff, payoff)?
        );

        if payoff > max_profit {
            max_profit = payoff;
        }
        if payoff < max_loss {
            max_loss = payoff;
        }
    }

    let expected_value = sum_payoffs / (num_paths as u64);
    
    let variance = calculate_variance(
        sum_squared_payoffs,
        sum_payoffs,
        num_paths,
    )?;

    let (percentile_5, percentile_95) = calculate_percentiles(
        &payoffs[..num_paths as usize],
        num_paths,
    )?;

    let sharpe_ratio = calculate_sharpe_ratio(expected_value, variance)?;

    Ok(SimulationResults {
        expected_value,
        variance,
        percentile_5,
        percentile_95,
        max_profit,
        max_loss,
        sharpe_ratio,
        paths_completed: num_paths,
    })
}

fn simulate_single_path(
    config: &SimulationConfig,
    time_steps: u32,
    rng_state: &mut u64,
) -> Result<u64, ProgramError> {
    let mut price = config.initial_price;
    let sqrt_dt = sqrt_fixed_point(config.dt)?;

    for _ in 0..time_steps {
        let random_normal = generate_normal_random(rng_state)?;
        
        let drift_component = multiply_fixed_point(
            config.drift,
            config.dt,
        )?;
        
        let diffusion_component = multiply_fixed_point(
            multiply_fixed_point(config.volatility, sqrt_dt)?,
            random_normal,
        )?;
        
        let price_change = drift_component.saturating_add(diffusion_component);
        price = multiply_fixed_point(
            price,
            FIXED_POINT_SCALE.saturating_add(price_change),
        )?;
    }

    calculate_option_payoff(price, config.strike_price, config.is_call_option)
}

fn calculate_option_payoff(
    final_price: u64,
    strike_price: u64,
    is_call: bool,
) -> Result<u64, ProgramError> {
    if is_call {
        Ok(final_price.saturating_sub(strike_price))
    } else {
        Ok(strike_price.saturating_sub(final_price))
    }
}

fn generate_normal_random(state: &mut u64) -> Result<u64, ProgramError> {
    *state = state.wrapping_mul(1664525).wrapping_add(1013904223);
    let u1 = (*state % FIXED_POINT_SCALE) + 1;
    
    *state = state.wrapping_mul(1664525).wrapping_add(1013904223);
    let u2 = (*state % FIXED_POINT_SCALE) + 1;
    
    let ln_u1 = ln_approximation(u1)?;
    let sqrt_component = sqrt_fixed_point(
        multiply_fixed_point(2 * FIXED_POINT_SCALE, ln_u1)?
    )?;
    
    let angle = multiply_fixed_point(2 * 314159265, u2)?;
    let cos_angle = cos_approximation(angle)?;
    
    multiply_fixed_point(sqrt_component, cos_angle)
}

fn calculate_variance(
    sum_squared: u64,
    sum: u64,
    n: u32,
) -> Result<u64, ProgramError> {
    let mean_squared = sum_squared / (n as u64);
    let mean = sum / (n as u64);
    let squared_mean = multiply_fixed_point(mean, mean)?;
    
    Ok(mean_squared.saturating_sub(squared_mean))
}

fn calculate_percentiles(
    payoffs: &[u64],
    n: u32,
) -> Result<(u64, u64), ProgramError> {
    let mut sorted: [u64; MAX_SIMULATION_PATHS] = [0; MAX_SIMULATION_PATHS];
    sorted[..n as usize].copy_from_slice(&payoffs[..n as usize]);
    
    simple_sort(&mut sorted[..n as usize]);
    
    let idx_5 = ((n as u64) * 5 / 100) as usize;
    let idx_95 = ((n as u64) * 95 / 100) as usize;
    
    Ok((sorted[idx_5], sorted[idx_95]))
}

fn calculate_sharpe_ratio(
    expected_return: u64,
    variance: u64,
) -> Result<u64, ProgramError> {
    if variance == 0 {
        return Ok(0);
    }
    
    let std_dev = sqrt_fixed_point(variance)?;
    
    multiply_fixed_point(
        expected_return,
        FIXED_POINT_SCALE,
    ).map(|v| v / std_dev)
}

fn multiply_fixed_point(a: u64, b: u64) -> Result<u64, ProgramError> {
    let product = (a as u128) * (b as u128);
    let scaled = product / (FIXED_POINT_SCALE as u128);
    
    if scaled > u64::MAX as u128 {
        return Err(ProgramError::ArithmeticOverflow);
    }
    
    Ok(scaled as u64)
}

fn sqrt_fixed_point(x: u64) -> Result<u64, ProgramError> {
    if x == 0 {
        return Ok(0);
    }
    
    let mut result = x;
    let mut last = 0;
    
    while result != last {
        last = result;
        result = (result + multiply_fixed_point(x, FIXED_POINT_SCALE)? / result) / 2;
    }
    
    Ok(result)
}

fn ln_approximation(x: u64) -> Result<u64, ProgramError> {
    if x == 0 {
        return Err(ProgramError::ArithmeticOverflow);
    }
    
    let normalized = if x > FIXED_POINT_SCALE {
        multiply_fixed_point(x, FIXED_POINT_SCALE / 2)?
    } else {
        x
    };
    
    let x_minus_one = normalized.saturating_sub(FIXED_POINT_SCALE);
    let x_plus_one = normalized.saturating_add(FIXED_POINT_SCALE);
    
    let ratio = multiply_fixed_point(x_minus_one, FIXED_POINT_SCALE)?
        .checked_div(x_plus_one)
        .ok_or(ProgramError::ArithmeticOverflow)?;
    
    let ratio_squared = multiply_fixed_point(ratio, ratio)?;
    
    let term1 = ratio;
    let term2 = multiply_fixed_point(ratio_squared, ratio)? / 3;
    let term3 = multiply_fixed_point(
        multiply_fixed_point(ratio_squared, ratio_squared)?,
        ratio
    )? / 5;
    
    Ok(multiply_fixed_point(
        2 * FIXED_POINT_SCALE,
        term1.saturating_add(term2).saturating_add(term3)
    )?)
}

fn cos_approximation(x: u64) -> Result<u64, ProgramError> {
    let pi = 314159265;
    let two_pi = 2 * pi;
    
    let normalized = x % two_pi;
    
    let x_squared = multiply_fixed_point(normalized, normalized)?;
    
    let term1 = FIXED_POINT_SCALE;
    let term2 = x_squared / 2;
    let term3 = multiply_fixed_point(x_squared, x_squared)? / 24;
    
    Ok(term1.saturating_sub(term2).saturating_add(term3))
}

fn simple_sort(arr: &mut [u64]) {
    let len = arr.len();
    for i in 0..len {
        for j in 0..len - 1 - i {
            if arr[j] > arr[j + 1] {
                let temp = arr[j];
                arr[j] = arr[j + 1];
                arr[j + 1] = temp;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_multiply_fixed_point() {
        let a = FIXED_POINT_SCALE; // 1.0
        let b = 2 * FIXED_POINT_SCALE; // 2.0
        let result = multiply_fixed_point(a, b).unwrap();
        assert_eq!(result, 2 * FIXED_POINT_SCALE);
    }

    #[test]
    fn test_sqrt_fixed_point() {
        let x = 4 * FIXED_POINT_SCALE; // 4.0
        let result = sqrt_fixed_point(x).unwrap();
        assert!((result as i64 - 2 * FIXED_POINT_SCALE as i64).abs() < 1000);
    }

    #[test]
    fn test_option_payoff_call() {
        let final_price = 110 * FIXED_POINT_SCALE;
        let strike_price = 100 * FIXED_POINT_SCALE;
        let payoff = calculate_option_payoff(final_price, strike_price, true).unwrap();
        assert_eq!(payoff, 10 * FIXED_POINT_SCALE);
    }

    #[test]
    fn test_option_payoff_put() {
        let final_price = 90 * FIXED_POINT_SCALE;
        let strike_price = 100 * FIXED_POINT_SCALE;
        let payoff = calculate_option_payoff(final_price, strike_price, false).unwrap();
        assert_eq!(payoff, 10 * FIXED_POINT_SCALE);
    }
}
