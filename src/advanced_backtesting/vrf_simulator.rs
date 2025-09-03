use anchor_lang::prelude::*;
use switchboard_v2::VrfAccountData;
use decimal::Decimal;

declare_id!("VrFSim1111111111111111111111111111111111111");

const FIXED_POINT_SCALE: u64 = 1_000_000_000_000_000_000;

#[program]
pub mod vrf_simulator {
    use super::*;
    use rand_pcg::Pcg64Mcg;
    use rand::SeedableRng;

    pub fn init_request(ctx: Context<InitRequest>) -> Result<()> {
        // Invoke VRF to request randomness
        let vrf = &mut ctx.accounts.vrf;
        vrf.request_randomness()?;
        Ok(())
    }

    pub fn callback(ctx: Context<Callback>, randomness: [u8; 32]) -> Result<()> {
        // Convert first 16 bytes into u128 seed
        let seed = u128::from_le_bytes(randomness[..16].try_into().unwrap());
        let mut rng = Pcg64Mcg::new(seed);

        // Convert fixed-point inputs to Decimal for calculations
        let price = convert_from_fixed(ctx.accounts.config.price);
        let strike = convert_from_fixed(ctx.accounts.config.strike);

        // Perform light Monte Carlo simulation
        let result = run_light_simulation(price, strike, &mut rng)?;

        // Store fixed-point result
        let state = &mut ctx.accounts.state;
        state.result = convert_to_fixed(result);
        
        Ok(())
    }

    fn run_light_simulation(
        price: Decimal,
        strike: Decimal,
        rng: &mut Pcg64Mcg
    ) -> Result<Decimal, ProgramError> {
        // Simplified simulation for on-chain use
        let mut final_price = price;
        
        for _ in 0..10 { // Fewer steps for compute budget
            let z: f64 = rng.gen_range(-1.0..1.0);
            let drift_term = Decimal::new(5, 4) * Decimal::new(1, 3); // Fixed params
            let diffusion_term = Decimal::new(20, 4) * Decimal::from_f64(z).unwrap() * Decimal::new(1, 3).sqrt();
            final_price *= (Decimal::ONE + drift_term + diffusion_term).max(Decimal::ZERO);
        }
        
        Ok((final_price - strike).max(Decimal::ZERO))
    }

    fn convert_from_fixed(v: u64) -> Decimal {
        Decimal::from(v) / Decimal::from(FIXED_POINT_SCALE)
    }

    fn convert_to_fixed(d: Decimal) -> u64 {
        (d * Decimal::from(FIXED_POINT_SCALE))
            .to_u64()
            .unwrap_or_else(|| panic!("Fixed point conversion overflow"))
    }
}

#[derive(Accounts)]
pub struct InitRequest<'info> {
    #[account(mut)]
    pub vrf: Account<'info, VrfAccountData>,
}

#[derive(Accounts)]
pub struct Callback<'info> {
    pub config: Account<'info, SimulationConfigAccount>,
    #[account(mut)]
    pub state: Account<'info, SimulationStateAccount>,
}

#[account]
pub struct SimulationConfigAccount {
    pub price: u64,
    pub strike: u64,
}

#[account]
pub struct SimulationStateAccount {
    pub result: u64,
}
