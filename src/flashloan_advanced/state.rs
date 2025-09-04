use anchor_lang::prelude::*;

pub const MAX_SWAP_ACCOUNTS: usize = 12;
pub const MAX_SWAP_DATA: usize = 1024;
pub const CURRENT_VERSION: u8 = 1;
pub const MAX_PROTOCOLS: usize = 8;

#[derive(AnchorSerialize, AnchorDeserialize, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolKind {
    Solend,
    PortFinance,
    Larix,
    Kamino,
    Custom,
}

/// Program-wide versioned state
#[account]
pub struct ProgramState {
    pub version: u8,
}

impl ProgramState {
    pub const LEN: usize = 1;
}

/// Aggregator main account (PDA: ["aggregator"])
#[account]
pub struct Aggregator {
    pub authority: Pubkey,
    pub bump: u8,
    pub token_account_bump: u8,
    pub slippage_bps: u16,
    pub min_profit_bps: u16,
    pub total_volume: u128,
    pub total_profit: u128,
    pub total_flash_loans: u64,
    pub is_paused: bool,
    pub protocol_count: u8,
    pub last_exec_slot: u64,
    pub exec_count_this_slot: u16,
    pub max_execs_per_slot: u16,
}

impl Aggregator {
    pub const LEN: usize = 32 + 1 + 1 + 2 + 2 + 16 + 16 + 8 + 1 + 1 + 8 + 2 + 2;
}

/// Protocol registration entry (PDA: ["protocol", reserve_pubkey])
#[account]
pub struct ProtocolInfo {
    pub aggregator: Pubkey,
    pub protocol_kind: ProtocolKind,
    pub program_id: Pubkey,
    pub reserve_address: Pubkey,
    pub collateral_mint: Pubkey,
    pub liquidity_address: Pubkey,
    pub fee_bps: u16,
    pub is_active: bool,
    pub last_update: i64,
    pub total_borrowed: u64,
    pub bump: u8,
    pub success_count: u32,
    pub failure_count: u32,
}

impl ProtocolInfo {
    pub const LEN: usize = 32 + 1 + 32 + 32 + 32 + 32 + 2 + 1 + 8 + 8 + 1 + 4 + 4;
}

#[account]
pub struct Config {
    pub min_profit_bps: u16,
    pub slippage_bps: u16,
    pub disable_threshold_pct: u8,
    pub last_update: i64,
}

impl Config { 
    pub const LEN: usize = 2 + 2 + 1 + 8; 
}

/// Compact typed struct for off-chain -> on-chain route data
#[derive(AnchorSerialize, AnchorDeserialize, Clone, Debug)]
pub struct RouteData {
    pub dex_program: Pubkey,
    pub swap_accounts: [Pubkey; MAX_SWAP_ACCOUNTS],
    pub swap_accounts_len: u8,
    pub swap_data: Vec<u8>,
    pub expected_amount_out: u64,
    pub min_amount_out: u64,
}

impl RouteData {
    pub fn validate(&self) -> Result<()> {
        require!(self.dex_program != Pubkey::default(), ErrorCode::InvalidProgramId);
        require!(self.swap_accounts_len > 0, ErrorCode::InvalidParameter);
        require!(self.swap_accounts_len as usize <= MAX_SWAP_ACCOUNTS, ErrorCode::RouteDataTooLarge);
        require!(!self.swap_data.is_empty(), ErrorCode::InvalidInstructionData);
        require!(self.expected_amount_out >= self.min_amount_out, ErrorCode::SlippageExceeded);
        Ok(())
    }
}

#[account(zero_copy)]
#[repr(packed)]
pub struct RateLimiter {
    pub last_slot: u64,
    pub count_this_slot: u8,
}
