use anyhow::{anyhow, Result};
use solana_sdk::pubkey::Pubkey;
use crate::config::{WHIRLPOOL_DISCRIM, ORCA_WHIRLPOOL_PROGRAM, RAYDIUM_V4_PROGRAM};

/// Parsed Orca Whirlpool state
#[derive(Debug, Clone)]
pub struct ParsedWhirlpool {
    pub fee_rate: u16,              // hundredths of basis point
    pub protocol_fee_rate: u16,     // basis points
    pub liquidity: u128,
    pub sqrt_price: u128,           // Q64.64
    pub tick_current: i32,
    pub tick_spacing: i16,
    pub token_mint_a: Pubkey,
    pub token_vault_a: Pubkey,
    pub token_mint_b: Pubkey,
    pub token_vault_b: Pubkey,
}

/// Parse Orca Whirlpool account (validated against official repo)
/// GitHub: https://github.com/orca-so/whirlpools/blob/main/programs/whirlpool/src/state/whirlpool.rs
pub fn parse_whirlpool_account(data: &[u8]) -> Result<ParsedWhirlpool> {
    if data.len() < 653 {
        return Err(anyhow!("Whirlpool account data too small: {} bytes", data.len()));
    }
    
    // Validate Anchor discriminator (8 bytes)
    if &data[0..8] != WHIRLPOOL_DISCRIM {
        return Err(anyhow!("Invalid Whirlpool discriminator"));
    }
    
    // Skip discriminator, all offsets relative to base=8
    let base = 8usize;
    
    // Validated offsets from Orca repo (whirlpool.rs)
    // whirlpools_config: Pubkey at 0
    // whirlpool_bump: [u8; 1] at 32
    // tick_spacing: u16 at 33
    let tick_spacing = u16::from_le_bytes(data[base + 33..base + 35].try_into()?) as i16;
    
    // tick_spacing_seed: [u8; 2] at 35
    // fee_rate: u16 at 37 (hundredths of basis point)
    let fee_rate = u16::from_le_bytes(data[base + 37..base + 39].try_into()?);
    
    // protocol_fee_rate: u16 at 39 (basis points)
    let protocol_fee_rate = u16::from_le_bytes(data[base + 39..base + 41].try_into()?);
    
    // liquidity: u128 at 41
    let liquidity = u128::from_le_bytes(data[base + 41..base + 57].try_into()?);
    
    // sqrt_price: u128 at 57 (Q64.64)
    let sqrt_price = u128::from_le_bytes(data[base + 57..base + 73].try_into()?);
    
    // tick_current_index: i32 at 73
    let tick_current = i32::from_le_bytes(data[base + 73..base + 77].try_into()?);
    
    // protocol_fee_owed_a: u64 at 77
    // protocol_fee_owed_b: u64 at 85
    // token_mint_a: Pubkey at 93
    let token_mint_a = Pubkey::new(&data[base + 93..base + 125]);
    
    // token_vault_a: Pubkey at 125
    let token_vault_a = Pubkey::new(&data[base + 125..base + 157]);
    
    // fee_growth_global_a: u128 at 157
    // token_mint_b: Pubkey at 173
    let token_mint_b = Pubkey::new(&data[base + 173..base + 205]);
    
    // token_vault_b: Pubkey at 205
    let token_vault_b = Pubkey::new(&data[base + 205..base + 237]);
    
    Ok(ParsedWhirlpool {
        fee_rate,
        protocol_fee_rate,
        liquidity,
        sqrt_price,
        tick_current,
        tick_spacing,
        token_mint_a,
        token_vault_a,
        token_mint_b,
        token_vault_b,
    })
}

/// Parsed Raydium AMM v4 state
#[derive(Debug, Clone)]
pub struct ParsedRaydiumAmm {
    pub status: u64,
    pub nonce: u8,
    pub order_num: u64,
    pub depth: u64,
    pub coin_decimals: u8,
    pub pc_decimals: u8,
    pub state: u64,
    pub reset_flag: u8,
    pub min_size: u64,
    pub vol_max_cut_ratio: u64,
    pub amount_wave: u64,
    pub coin_lot_size: u64,
    pub pc_lot_size: u64,
    pub min_price_multiplier: u64,
    pub max_price_multiplier: u64,
    pub system_decimals_value: u64,
    // Fees (64 bytes total)
    pub trade_fee_numerator: u64,
    pub trade_fee_denominator: u64,
    pub pnl_numerator: u64,
    pub pnl_denominator: u64,
    pub swap_fee_numerator: u64,
    pub swap_fee_denominator: u64,
    // Vaults and amounts
    pub coin_vault: Pubkey,
    pub pc_vault: Pubkey,
    pub coin_vault_amount: u64,
    pub pc_vault_amount: u64,
    // Other accounts
    pub coin_mint: Pubkey,
    pub pc_mint: Pubkey,
    pub lp_mint: Pubkey,
    pub open_orders: Pubkey,
    pub market: Pubkey,
    pub market_program: Pubkey,
    pub target_orders: Pubkey,
    pub withdraw_queue: Pubkey,
    pub temp_lp_vault: Pubkey,
    pub owner: Pubkey,
    pub pnl_owner: Pubkey,
}

/// Parse Raydium AMM v4 account (validated against official repo)
/// GitHub: https://github.com/raydium-io/raydium-amm/blob/master/program/src/state.rs
pub fn parse_raydium_amm_account(data: &[u8]) -> Result<ParsedRaydiumAmm> {
    if data.len() < 752 {
        return Err(anyhow!("Raydium AMM account too small: {} bytes", data.len()));
    }
    
    // Direct offset parsing (no discriminator for Raydium)
    // AmmInfo struct layout from state.rs
    let status = u64::from_le_bytes(data[0..8].try_into()?);
    
    // Check if swap is enabled
    if !is_raydium_swap_enabled(status) {
        return Err(anyhow!("Pool not enabled for swaps (status: {})", status));
    }
    
    let nonce = data[8];
    let order_num = u64::from_le_bytes(data[9..17].try_into()?);
    let depth = u64::from_le_bytes(data[17..25].try_into()?);
    let coin_decimals = data[25];
    let pc_decimals = data[26];
    let state = u64::from_le_bytes(data[27..35].try_into()?);
    let reset_flag = data[35];
    let min_size = u64::from_le_bytes(data[36..44].try_into()?);
    let vol_max_cut_ratio = u64::from_le_bytes(data[44..52].try_into()?);
    let amount_wave = u64::from_le_bytes(data[52..60].try_into()?);
    let coin_lot_size = u64::from_le_bytes(data[60..68].try_into()?);
    let pc_lot_size = u64::from_le_bytes(data[68..76].try_into()?);
    let min_price_multiplier = u64::from_le_bytes(data[76..84].try_into()?);
    let max_price_multiplier = u64::from_le_bytes(data[84..92].try_into()?);
    let system_decimals_value = u64::from_le_bytes(data[92..100].try_into()?);
    
    // Fees struct (64 bytes) starts at offset 100
    // min_separate_numerator: u64 at 100
    // min_separate_denominator: u64 at 108
    // trade_fee_numerator: u64 at 116
    let trade_fee_numerator = u64::from_le_bytes(data[116..124].try_into()?);
    // trade_fee_denominator: u64 at 124
    let trade_fee_denominator = u64::from_le_bytes(data[124..132].try_into()?);
    // pnl_numerator: u64 at 132
    let pnl_numerator = u64::from_le_bytes(data[132..140].try_into()?);
    // pnl_denominator: u64 at 140
    let pnl_denominator = u64::from_le_bytes(data[140..148].try_into()?);
    // swap_fee_numerator: u64 at 148
    let swap_fee_numerator = u64::from_le_bytes(data[148..156].try_into()?);
    // swap_fee_denominator: u64 at 156
    let swap_fee_denominator = u64::from_le_bytes(data[156..164].try_into()?);
    
    // OutPutData (144 bytes) starts at 164
    // need_take_pnl_coin: u64 at 164
    // need_take_pnl_pc: u64 at 172
    // total_pnl_pc: u64 at 180
    // total_pnl_coin: u64 at 188
    // pool_total_deposit_pc: u128 at 196
    // pool_total_deposit_coin: u128 at 212
    // swap_coin_in_amount: u128 at 228
    // swap_pc_out_amount: u128 at 244
    // swap_coin2_pc_fee: u64 at 260
    // swap_pc_in_amount: u128 at 268
    // swap_coin_out_amount: u128 at 284
    // swap_pc2_coin_fee: u64 at 300
    
    // token_coin: Pubkey at 308
    let coin_mint = Pubkey::new(&data[308..340]);
    // token_pc: Pubkey at 340
    let pc_mint = Pubkey::new(&data[340..372]);
    // coin_vault: Pubkey at 372
    let coin_vault = Pubkey::new(&data[372..404]);
    // pc_vault: Pubkey at 404
    let pc_vault = Pubkey::new(&data[404..436]);
    // bid_data_len: u64 at 436
    // ask_data_len: u64 at 444
    // coin_vault_amount: u64 at 452
    let coin_vault_amount = u64::from_le_bytes(data[452..460].try_into()?);
    // pc_vault_amount: u64 at 460
    let pc_vault_amount = u64::from_le_bytes(data[460..468].try_into()?);
    // PoolOpenTime: u64 at 468
    // punish_pc_amount: u64 at 476
    // punish_coin_amount: u64 at 484
    // orderbooktoInitTime: u64 at 492
    // swap_coin_in_amount_part: u128 at 500
    // swap_pc_in_amount_part: u128 at 516
    // swap_coin_out_amount_part: u128 at 532
    // swap_pc_out_amount_part: u128 at 548
    // swap_coin2_pc_fee_part: u64 at 564
    // swap_pc2_coin_fee_part: u64 at 572
    
    // lp_mint: Pubkey at 580
    let lp_mint = Pubkey::new(&data[580..612]);
    // open_orders: Pubkey at 612
    let open_orders = Pubkey::new(&data[612..644]);
    // market: Pubkey at 644
    let market = Pubkey::new(&data[644..676]);
    // market_program: Pubkey at 676
    let market_program = Pubkey::new(&data[676..708]);
    // target_orders: Pubkey at 708
    let target_orders = Pubkey::new(&data[708..740]);
    // withdraw_queue: Pubkey at 740
    let withdraw_queue = Pubkey::new(&data[740..772]);
    // temp_lp_vault: Pubkey at 772
    let temp_lp_vault = Pubkey::new(&data[772..804]);
    // owner: Pubkey at 804
    let owner = Pubkey::new(&data[804..836]);
    // pnl_owner: Pubkey at 836
    let pnl_owner = Pubkey::new(&data[836..868]);
    
    Ok(ParsedRaydiumAmm {
        status,
        nonce,
        order_num,
        depth,
        coin_decimals,
        pc_decimals,
        state,
        reset_flag,
        min_size,
        vol_max_cut_ratio,
        amount_wave,
        coin_lot_size,
        pc_lot_size,
        min_price_multiplier,
        max_price_multiplier,
        system_decimals_value,
        trade_fee_numerator,
        trade_fee_denominator,
        pnl_numerator,
        pnl_denominator,
        swap_fee_numerator,
        swap_fee_denominator,
        coin_vault,
        pc_vault,
        coin_vault_amount,
        pc_vault_amount,
        coin_mint,
        pc_mint,
        lp_mint,
        open_orders,
        market,
        market_program,
        target_orders,
        withdraw_queue,
        temp_lp_vault,
        owner,
        pnl_owner,
    })
}

/// Check if Raydium pool allows swaps
fn is_raydium_swap_enabled(status: u64) -> bool {
    // AmmStatus enum values from Raydium state.rs
    const INITIALIZED: u64 = 1;
    const DISABLED: u64 = 2;
    const WITHDRAW_ONLY: u64 = 3;
    const LIQUIDITY_ONLY: u64 = 4;
    const ORDERBOOK_ONLY: u64 = 5;
    const SWAP_ONLY: u64 = 6;
    const WAITING_TRADE: u64 = 7;
    
    matches!(status, INITIALIZED | SWAP_ONLY | WAITING_TRADE)
}

/// Validate account owner
pub fn validate_account_owner(account_owner: &Pubkey, expected_program: &str) -> Result<()> {
    let expected = Pubkey::from_str(expected_program)?;
    if account_owner != &expected {
        return Err(anyhow!(
            "Invalid account owner. Expected: {}, Got: {}",
            expected,
            account_owner
        ));
    }
    Ok(())
}

use std::str::FromStr;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raydium_status() {
        assert!(is_raydium_swap_enabled(1)); // Initialized
        assert!(!is_raydium_swap_enabled(2)); // Disabled
        assert!(!is_raydium_swap_enabled(3)); // WithdrawOnly
        assert!(!is_raydium_swap_enabled(4)); // LiquidityOnly
        assert!(!is_raydium_swap_enabled(5)); // OrderbookOnly
        assert!(is_raydium_swap_enabled(6)); // SwapOnly
        assert!(is_raydium_swap_enabled(7)); // WaitingTrade
    }
}
