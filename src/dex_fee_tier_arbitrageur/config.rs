use serde::{Deserialize, Serialize};
use solana_sdk::native_token::LAMPORTS_PER_SOL;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub compute_units: u32,
    pub priority_fee_microlamports: u64,
    pub max_slippage_bps: u64,
    pub min_profit_lamports: u64,
    pub jito_tip_lamports: u64,
    pub max_retries: u8,
    pub min_liquidity: u128,
    pub pool_refresh_interval_ms: u64,
    pub max_concurrent_simulations: usize,
    pub circuit_breaker_loss_per_minute: u64,
    pub max_test_amount_lamports: u64,
    pub adaptive_test_amount: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            compute_units: 1_400_000,
            priority_fee_microlamports: 25_000,
            max_slippage_bps: 30,
            min_profit_lamports: 100_000,
            jito_tip_lamports: 10_000,
            max_retries: 2,
            min_liquidity: 1_000_000,
            pool_refresh_interval_ms: 100,
            max_concurrent_simulations: 10,
            circuit_breaker_loss_per_minute: LAMPORTS_PER_SOL,
            max_test_amount_lamports: LAMPORTS_PER_SOL / 10,
            adaptive_test_amount: true,
        }
    }
}

pub const ORCA_WHIRLPOOL_PROGRAM: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
pub const RAYDIUM_V4_PROGRAM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
pub const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub const ASSOCIATED_TOKEN_PROGRAM_ID: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
pub const JITO_TIP_PROGRAM: &str = "T1pyyaTNZsKv2WcRAB8oVnk93mLJw2XzjtVYqCsaHqt";

// Orca constants
pub const WHIRLPOOL_DISCRIM: [u8; 8] = [247, 198, 158, 145, 225, 117, 135, 72];
pub const TICKS_PER_ARRAY: i32 = 88;

// Math constants
pub const Q64: u128 = 1u128 << 64;
pub const Q96: u128 = 1u128 << 96;
pub const FEE_RATE_MUL: u128 = 1_000_000; // for hundredths of bps
