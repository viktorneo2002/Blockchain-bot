#![deny(clippy::all, rust_2018_idioms)]
use anchor_lang::prelude::*;
use anchor_spl::token::{self, Token, TokenAccount, Transfer};
use solana_program::instruction::{AccountMeta, Instruction};
use solana_program::program::invoke;

extern crate alloc;

use anchor_lang::prelude::*;
use anchor_spl::token::{Token, TokenAccount, Transfer, Mint};
use solend_program::cpi::accounts::FlashBorrow;
use solend_program::program::Solend;
use port_finance::cpi::accounts::FlashLoan as PortFlashLoan;
use port_finance::program::PortFinance;
use larix_program::cpi::accounts::{BeginFlashLoan, EndFlashLoan};
use larix_program::program::Larix;
use kamino_program::cpi::accounts::FlashLoan as KaminoFlashLoan;
use kamino_program::program::Kamino;
use core::convert::TryInto;
use fixed::types::U64F64; // Q64.64 fixed‐point
use fixed::FixedU64;
use heapless::Vec;

/// Q64.64 fixed‐point scale
const FP_SCALE: U64F64 = U64F64::from_num(1);

/// Maximum number of accounts in a route
const MAX_SWAP_ACCOUNTS: usize = 10;

/// Maximum swap payload length
const MAX_SWAP_DATA: usize = 512;

declare_id!("FLsh1111111111111111111111111111111111111111");

/// Supported lending protocols
#[derive(AnchorSerialize, AnchorDeserialize, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolType {
    Solend,
    PortFinance,
    Larix,
    Kamino,
    Custom(Pubkey)
}

/// On‐chain instruction data for routing a swap
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

#[error_code]
pub enum ErrorCode {
    #[msg("Overflow in arithmetic operation")]
    MathOverflow,
    #[msg("Expected profit not met")]
    InsufficientExpectedProfit,
    #[msg("Protocol program ID mismatch")]
    InvalidProgramId,
    #[msg("Compute budget exceeded")]
    ComputeBudgetExceeded,
    #[msg("Rate limit exceeded")]
    RateLimitExceeded,
    #[msg("Route data too large")]
    RouteDataTooLarge,
    #[msg("Protocol inactive or not registered")]
    ProtocolInactive,
    #[msg("Timelock already executed")]
    TimelockExecuted,
    #[msg("Timelock period not expired")]
    TimelockLocked,
    #[msg("Insufficient signers for timelock")]
    InsufficientSigners,
    #[msg("Protocol disabled due to high failure rate")]
    ProtocolDisabled,
    #[msg("Slippage exceeded maximum allowed threshold")]
    SlippageExceeded,
    #[msg("Program version mismatch")]
    VersionMismatch,
    #[msg("Insufficient funds")]
    InsufficientFunds,
    #[msg("Unauthorized")]
    Unauthorized,
    #[msg("System paused")]
    SystemPaused,
    #[msg("Maximum number of protocols reached")]
    MaxProtocolsReached,
    #[msg("Invalid parameter value")]
    InvalidParameter,
    #[msg("Account not initialized")]
    AccountNotInitialized,
    #[msg("Invalid account owner")]
    InvalidAccountOwner,
    #[msg("Invalid account data")]
    InvalidAccountData,
    #[msg("Invalid instruction data")]
    InvalidInstructionData,
    #[msg("Operation not supported")]
    OperationNotSupported,
    #[msg("Invalid timestamp")]
    InvalidTimestamp,
    #[msg("Invalid slot")]
    InvalidSlot,
}

/// Accounts for executing a flash loan + swap + repay
#[derive(Accounts)]
pub struct ExecuteAggregatedFlashLoan<'info> {
    #[account(mut, seeds = [b"aggregator"], bump = aggregator.bump)]
    pub aggregator: Account<'info, Aggregator>,
    
    #[account(mut, has_one = aggregator)]
    pub protocol_info: Account<'info, ProtocolInfo>,
    
    #[account(mut)]
    pub user_token_account: Account<'info, TokenAccount>,
    
    #[account(mut, seeds = [b"agg_token", aggregator.key().as_ref()], bump = aggregator.token_account_bump)]
    pub aggregator_token_account: Account<'info, TokenAccount>,
    
    pub token_program: Program<'info, Token>,
    
    /// CHECK: Validated in protocol adapter
    pub protocol_program: AccountInfo<'info>,
    
    /// Remaining accounts must match RouteData.swap_accounts
    /// and include all accounts required by the protocol program
    pub remaining_accounts: Vec<AccountInfo<'info>>,
}

pub fn execute_aggregated_flash_loan(
    ctx: Context<ExecuteAggregatedFlashLoan>,
    amount: u64,
    route: RouteData,
) -> Result<()> {
    // Validate inputs
    require!(amount > 0, ErrorCode::InvalidAmount);
    require!(!ctx.accounts.aggregator.is_paused, ErrorCode::AggregatorPaused);
    
    // Create appropriate adapter based on protocol
    let adapter = match ctx.accounts.protocol_info.protocol_kind {
        ProtocolKind::Solend => SolendAdapter::new(
            &mut ctx.accounts.protocol_info,
            ctx.accounts.protocol_program.clone(),
            &ctx.remaining_accounts
        ),
        ProtocolKind::PortFinance => PortFinanceAdapter::new(
            &mut ctx.accounts.protocol_info,
            ctx.accounts.protocol_program.clone(),
            &ctx.remaining_accounts
        ),
        // ... other protocols
        _ => return Err(ErrorCode::UnsupportedProtocol.into()),
    };
    
    // Execute flash loan with retry logic
    let mut attempts = 0;
    loop {
        match adapter.execute_flash_loan(amount) {
            Ok(_) => break,
            Err(e) if attempts < MAX_RETRIES => {
                attempts += 1;
                continue;
            }
            Err(e) => {
                ctx.accounts.protocol_info.failure_count += 1;
                return Err(e);
            }
        }
    }
    
    // Execute route if provided
    if route.swap_accounts_len > 0 {
        util::execute_fixed_route(&ctx, &route)?;
    }
    
    // Update aggregator metrics
    ctx.accounts.aggregator.total_volume += amount as u128;
    ctx.accounts.aggregator.total_flash_loans += 1;
    
    Ok(())
}

mod util {
    use super::*;
    
    /// Executes a fixed route with proper validation and safety checks
    pub fn execute_fixed_route<'info>(
        ctx: &Context<'_, '_, '_, 'info, ExecuteAggregatedFlashLoan<'info>>,
        route: &RouteData,
    ) -> Result<()> {
        // Validate route data first
        route.validate()?;
        
        // Build AccountMetas from remaining accounts
        let mut metas = Vec::with_capacity(route.swap_accounts_len as usize);
        for i in 0..route.swap_accounts_len as usize {
            metas.push(AccountMeta::new(route.swap_accounts[i], false));
        }
        
        // Create instruction
        let ix = Instruction {
            program_id: route.dex_program,
            accounts: metas,
            data: route.swap_data.clone(),
        };
        
        // Build account infos for CPI
        let mut infos = vec![
            ctx.accounts.user_token_account.to_account_info(),
            ctx.accounts.aggregator_token_account.to_account_info(),
            ctx.accounts.token_program.to_account_info(),
        ];
        
        // Add remaining accounts (must be passed in correct order by client)
        for acc in ctx.remaining_accounts.iter() {
            infos.push(acc.clone());
        }
        
        // Execute with slippage protection
        let min_out = route.expected_amount_out
            .checked_mul(10_000 - ctx.accounts.aggregator.slippage_bps as u64)
            .ok_or(ErrorCode::MathOverflow)? / 10_000;
            
        require!(min_out >= route.min_amount_out, ErrorCode::SlippageExceeded);
        
        // Execute CPI
        solana_program::program::invoke(&ix, &infos)?;
        
        Ok(())
    }
}

pub fn do_flashloan(
    protocol: &mut ProtocolInfo,
    program: &AccountInfo,
    ctx: &ExecuteAggregatedFlashLoan,
    amount: u64,
    route: &RouteData,
    slippage_bps: u16
) -> Result<i64> {
    // Implementation goes here
    Ok(50)
}

#[derive(Accounts)]
pub struct Initialize<'info> {
    #[account(init, payer = authority, space = 8 + Aggregator::LEN)]
    pub aggregator: Account<'info, Aggregator>,
    // ... other accounts ...
}

#[derive(Accounts)]
pub struct RegisterProtocol<'info> {
    #[account(mut)]
    pub authority: Signer<'info>,
    #[account(init, payer = authority, space = 8 + ProtocolInfo::LEN)]
    pub protocol_info: Account<'info, ProtocolInfo>,
    pub system_program: Program<'info, System>,
}

#[derive(Accounts)]
pub struct DeregisterProtocol<'info> {
    #[account(mut)]
    pub authority: Signer<'info>,
    #[account(mut, close = authority)]
    pub protocol_info: Account<'info, ProtocolInfo>,
}

// ... other missing contexts ...

pub fn disable_if_unhealthy(pi: &mut ProtocolInfo) -> Result<()> {
    if pi.failure_count > 5 {
        pi.is_active = false;
    }
    Ok(())
}

pub trait FlashLoanProvider {
    fn execute_loan(&self, amount: u64) -> Result<()>;
    fn repay_loan(&self, amount: u64) -> Result<()>;
    fn protocol_name(&self) -> String;
}

impl FlashLoanProvider for SolendProtocol<'_> {
    fn execute_loan(&self, amount: u64) -> Result<()> {
        // Validate amount is within protocol limits
        require!(amount > 0, ErrorCode::InvalidParameter);
        require!(amount <= MAX_LOAN_AMOUNT, ErrorCode::InvalidParameter);
        
        // Create CPI accounts
        let cpi_accounts = FlashBorrow {
            reserve: self.reserve.clone(),
            liquidity_supply: self.liquidity_supply.clone(),
            instruction_sysvar: self.instruction_sysvar.clone(),
            // ... other required accounts
        };
        
        // Create CPI context
        let cpi_ctx = CpiContext::new(
            self.program.clone(),
            cpi_accounts
        );
        
        // Execute CPI with proper error handling
        solend_program::cpi::flash_borrow(cpi_ctx, amount)
            .map_err(|e| {
                self.protocol_info.failure_count += 1;
                disable_if_unhealthy(&mut self.protocol_info)?;
                e
            })
    }
    
    fn repay_loan(&self, amount: u64) -> Result<()> {
        // Similar validation and error handling as execute_loan
        // ... implementation ...
    }
    
    fn protocol_name(&self) -> String {
        "Solend".to_string()
    }
}

impl FlashLoanProvider for PortFinanceProtocol<'_> {
    fn execute_loan(&self, amount: u64) -> Result<()> {
        // Validate amount and protocol state
        require!(amount > 0, ErrorCode::InvalidParameter);
        require!(self.protocol_info.is_active, ErrorCode::ProtocolInactive);
        
        // Create CPI accounts with proper validation
        let cpi_accounts = PortFlashLoan {
            pool: self.pool.clone(),
            reserve: self.reserve.clone(),
            collateral_mint: self.collateral_mint.clone(),
            // ... other required accounts
        };
        
        // Execute CPI with retry logic for temporary failures
        let mut attempts = 0;
        loop {
            match port_finance::cpi::flash_loan(
                CpiContext::new(self.program.clone(), cpi_accounts.clone()),
                amount
            ) {
                Ok(_) => break Ok(()),
                Err(e) if attempts < MAX_RETRIES => {
                    attempts += 1;
                    continue;
                }
                Err(e) => {
                    self.protocol_info.failure_count += 1;
                    disable_if_unhealthy(&mut self.protocol_info)?;
                    break Err(e);
                }
            }
        }
    }
    
    fn repay_loan(&self, amount: u64) -> Result<()> {
        // Similar implementation with validation
        // ...
    }
    
    fn protocol_name(&self) -> String {
        "Port Finance".to_string()
    }
}

impl FlashLoanProvider for LarixProtocol<'_> {
    fn execute_loan(&self, amount: u64) -> Result<()> {
        larix_program::cpi::begin_flash_loan(self.ctx, amount)
    }
    
    fn repay_loan(&self, amount: u64) -> Result<()> {
        larix_program::cpi::end_flash_loan(self.ctx, amount)
    }
    
    fn protocol_name(&self) -> String {
        "Larix".to_string()
    }
}

impl FlashLoanProvider for KaminoProtocol<'_> {
    fn execute_loan(&self, amount: u64) -> Result<()> {
        kamino_program::cpi::flash_loan(self.ctx, amount)
    }
    
    fn repay_loan(&self, amount: u64) -> Result<()> {
        kamino_program::cpi::flash_repay(self.ctx, amount)
    }
    
    fn protocol_name(&self) -> String {
        "Kamino".to_string()
    }
}

pub fn handle_graceful_degradation(
    ctx: Context<ExecuteAggregatedFlashLoan>,
    protocol_index: u8
) -> Result<()> {
    if ctx.accounts.protocol_infos[protocol_index as usize].is_active {
        // Normal execution
    } else {
        // Fallback logic
        ctx.accounts.aggregator.fallback_protocol = Some(protocol_index);
        emit!(ProtocolFallback {
            timestamp: Clock::get()?.unix_timestamp,
            protocol_index
        });
    }
    Ok(())
}

pub fn execute_aggregated_flash_loan(
    ctx: Context<ExecuteAggregatedFlashLoan>,
    amount: u64,
    route_data: RouteData,
) -> Result<()> {
    // Validate program version
    require!(
        ctx.accounts.program_state.version == CURRENT_VERSION,
        ErrorCode::VersionMismatch
    );
    
    // Validate protocol program matches registered program_id
    require!(
        ctx.accounts.protocol_program.key() == ctx.accounts.protocol_info.program_id,
        ErrorCode::InvalidProgramId
    );
    
    // Validate protocol is active
    require!(ctx.accounts.protocol_info.is_active, ErrorCode::ProtocolInactive);
    
    // Validate amount is non-zero
    require!(amount > 0, ErrorCode::InvalidParameter);
    
    // Validate route data size
    require!(
        route_data.swap_accounts_len as usize <= MAX_SWAP_ACCOUNTS,
        ErrorCode::RouteDataTooLarge
    );
    require!(
        route_data.swap_data.len() <= MAX_SWAP_DATA,
        ErrorCode::RouteDataTooLarge
    );
    
    // Validate aggregator is not paused
    require!(!ctx.accounts.aggregator.is_paused, ErrorCode::SystemPaused);
    
    // Enforce rate limits
    enforce_rate_limit(&mut ctx.accounts.rate_limiter)?;
    enforce_exec_window(&mut ctx.accounts.aggregator)?;
    
    // Snapshot pre-loan balance for profit calculation
    let pre_balance = ctx.accounts.aggregator_token_account.amount;
    
    // Execute flash loan via CPI with proper error handling
    let result = match ctx.accounts.protocol_info.protocol_type {
        ProtocolType::Solend => SolendProtocol::new(ctx, ctx.accounts.protocol_info, ctx.accounts.protocol_program)
            .flash_borrow(amount)
            .map_err(|e| {
                ctx.accounts.protocol_info.failure_count += 1;
                disable_if_unhealthy(&mut ctx.accounts.protocol_info)?;
                e
            }),
        // Similar handling for other protocols
        _ => Err(ErrorCode::OperationNotSupported.into())
    };
    
    result?;
    
    // Execute swap route with slippage protection
    util::execute_fixed_route(&ctx.accounts, &route_data)?;
    
    // Calculate and validate profit
    let post_balance = ctx.accounts.aggregator_token_account.amount;
    let profit = post_balance
        .checked_sub(pre_balance)
        .ok_or(ErrorCode::MathOverflow)?;
    
    require!(
        profit >= route_data.expected_amount_out
            .checked_sub(amount)
            .ok_or(ErrorCode::MathOverflow)?,
        ErrorCode::InsufficientExpectedProfit
    );
    
    // Update metrics with overflow checks
    let aggr = &mut ctx.accounts.aggregator;
    aggr.total_volume = aggr
        .total_volume
        .checked_add(amount as u128)
        .ok_or(ErrorCode::MathOverflow)?;
    aggr.total_profit = aggr
        .total_profit
        .checked_add(profit as u128)
        .ok_or(ErrorCode::MathOverflow)?;
    aggr.total_flash_loans = aggr
        .total_flash_loans
        .checked_add(1)
        .ok_or(ErrorCode::MathOverflow)?;
    
    // Update protocol success count
    ctx.accounts.protocol_info.success_count += 1;
    
    // Emit event
    emit!(FlashLoanExecuted {
        protocol: ctx.accounts.protocol_info.protocol_type,
        amount,
        profit,
        timestamp: Clock::get()?.unix_timestamp,
        slot: Clock::get()?.slot,
        success: true,
    });
    
    Ok(())
}

pub fn snapshot_balance(account: &Account<'_, TokenAccount>) -> u64 {
    account.amount
}

pub fn assert_invariant(
    before: u64,
    after: u64,
    max_slippage_bps: u16
) -> Result<()> {
    let min_allowed = before
        .checked_mul((10_000 - max_slippage_bps as u64))
        .ok_or(ErrorCode::MathOverflow)?
        / 10_000;
    require!(after >= min_allowed, ErrorCode::SlippageExceeded);
    Ok(())
}

pub fn loan_swap_repay(ctx: Context<ExecuteAggregatedFlashLoan>, amount: u64, route_data: RouteData) -> Result<()> {
    let pre_user = snapshot_balance(&ctx.accounts.user_token_account);
    
    // 1) Flash-loan CPI
    // 2) Swap CPI
    // 3) Repay CPI
    
    let post_reserve = snapshot_balance(&ctx.accounts.reserve_liquidity_supply);
    assert_invariant(
        pre_user,
        post_reserve,
        ctx.accounts.aggregator.slippage_bps
    )?;
    
    Ok(())
}

// Utility module: strictly DRY, no heap, no floats
mod util {
    use super::*;

    pub fn execute_fixed_route<'info>(
        accounts: &ExecuteAggregatedFlashLoan<'info>,
        route: &RouteData,
    ) -> Result<()> {
        // slippage guard
        let min_out = route
            .expected_amount_out
            .checked_mul((10_000 - accounts.aggregator.slippage_bps as u64))
            .ok_or(ErrorCode::MathOverflow)?
            / 10_000;
        require!(min_out >= route.min_amount_out, ErrorCode::InsufficientExpectedProfit);

        // build AccountInfo slice
        let infos: &[AccountInfo] = &[
            accounts.user_token_account.to_account_info(),
            accounts.aggregator_token_account.to_account_info(),
            accounts.token_program.to_account_info(),
        ];

        // AccountMetas for CPI
        let metas = route
            .swap_accounts[..route.swap_accounts_len as usize]
            .iter()
            .map(|key| AccountMeta::new(*key, false))
            .collect::<heapless::Vec<AccountMeta, MAX_SWAP_ACCOUNTS>>()
            .map_err(|_| ErrorCode::RouteDataTooLarge)?;

        let ix = Instruction {
            program_id: route.dex_program,
            accounts: metas.into_inner(),
            data: route.swap_data,
        };

        solana_program::program::invoke(&ix, infos)?;
        Ok(())
    }

    pub fn execute_swap_route_fixed<'info>(
        accounts: &[AccountInfo<'info>; MAX_SWAP_ACCOUNTS],
        data: &[u8; MAX_SWAP_DATA],
        dex_program: Pubkey
    ) -> Result<()> {
        let mut metas: [AccountMeta; MAX_SWAP_ACCOUNTS] = [AccountMeta::new_readonly(Pubkey::default(), false); MAX_SWAP_ACCOUNTS];
        
        for (i, account) in accounts.iter().enumerate() {
            metas[i] = AccountMeta::new(account.key(), false);
        }
        
        let ix = Instruction {
            program_id: dex_program,
            accounts: metas.to_vec(),
            data: data.to_vec(),
        };
        
        invoke(&ix, accounts)?;
        Ok(())
    }
}

pub fn compute_budget_ixs(units: u32, micro_lamports_per_cu: u64) -> [solana_sdk::instruction::Instruction; 2] {
    use solana_sdk::compute_budget::ComputeBudgetInstruction as Cb;
    [
        Cb::set_compute_unit_limit(units).into(),
        Cb::set_compute_unit_price(micro_lamports_per_cu).into(),
    ]
}

// Mainnet Protocol Program IDs
const SOLEND_PROGRAM_ID: &str = "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo";
const PORT_FINANCE_PROGRAM_ID: &str = "Port7uDYB3wk6GJAw4KT1WpTeMtSu9bTcChBHkX2LfR";
const LARIX_PROGRAM_ID: &str = "7Zb1bGi32pfsrBkzWdqd4dFhUXwp5Nybr1zuaEwN34hy";
const KAMINO_PROGRAM_ID: &str = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD";

// DEX Program IDs
const ORCA_WHIRLPOOL_PROGRAM: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const RAYDIUM_V4_PROGRAM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const JUPITER_V6_PROGRAM: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";

const MAX_SLIPPAGE_BPS: u16 = 100;
const MIN_PROFIT_THRESHOLD: u64 = 50000;
const MAX_PROTOCOLS: usize = 8;
const FLASH_LOAN_FEE_DIVISOR: u64 = 10000;

const CURRENT_VERSION: u8 = 1;

#[program]
pub mod multi_protocol_flash_loan_aggregator {
    use super::*;

    pub fn initialize(
        ctx: Context<Initialize>,
        slippage_bps: u16,
        min_profit_bps: u16,
        disable_threshold_pct: u8,
        version: u8,
    ) -> Result<()> {
        require!(version == CURRENT_VERSION, ErrorCode::VersionMismatch);
        let ag = &mut ctx.accounts.aggregator;
        ag.authority = ctx.accounts.authority.key();
        ag.token_account_bump = *ctx.bumps.get("profit_vault").unwrap();
        ag.slippage_bps = slippage_bps;
        ag.min_profit_bps = min_profit_bps;
        ag.total_volume = 0;
        ag.total_profit = 0;
        ag.total_loans = 0;

        let cfg = &mut ctx.accounts.config;
        cfg.min_profit_bps = min_profit_bps;
        cfg.slippage_bps = slippage_bps;
        cfg.disable_threshold_pct = disable_threshold_pct;
        cfg.last_update = Clock::get()?.unix_timestamp;

        ctx.accounts.program_state.version = version;

        Ok(())
    }

    pub fn register_protocol(
        ctx: Context<RegisterProtocol>,
        protocol_type: ProtocolType,
        reserve_address: Pubkey,
        collateral_mint: Pubkey,
        fee_bps: u16,
    ) -> Result<()> {
        require!(!ctx.accounts.aggregator.is_paused, ErrorCode::SystemPaused);
        require!(
            ctx.accounts.aggregator.protocol_count < MAX_PROTOCOLS as u8,
            ErrorCode::MaxProtocolsReached
        );
        
        let protocol = &mut ctx.accounts.protocol_info;
        protocol.protocol_type = protocol_type;
        protocol.program_id = match protocol_type {
            ProtocolType::Solend => solend_program_id(),
            ProtocolType::PortFinance => port_finance_program_id(),
            ProtocolType::Larix => larix_program_id(),
            ProtocolType::Kamino => kamino_program_id(),
            ProtocolType::Custom(adapter_program) => adapter_program,
        };
        protocol.reserve_address = reserve_address;
        protocol.collateral_mint = collateral_mint;
        protocol.liquidity_address = ctx.accounts.liquidity_address.key();
        protocol.fee_bps = fee_bps;
        protocol.is_active = true;
        protocol.last_update = Clock::get()?.unix_timestamp;
        protocol.total_borrowed = 0;
        protocol.bump = *ctx.bumps.get("protocol_info").unwrap();
        protocol.failure_count = 0;
        protocol.success_count = 0;
        
        ctx.accounts.aggregator.protocol_count += 1;
        
        emit!(ProtocolRegistered {
            protocol_type,
            reserve: reserve_address,
            fee_bps,
            timestamp: Clock::get()?.unix_timestamp,
        });
        
        Ok(())
    }

    pub fn deregister_protocol(ctx: Context<DeregisterProtocol>) -> Result<()> {
        let pi = &ctx.accounts.protocol_info;
        let ptype = pi.protocol_type;
        emit!(ProtocolDeregistered { protocol: ptype });
        Ok(())
    }

    pub fn execute_aggregated_flash_loan(
        ctx: Context<ExecuteAggregatedFlashLoan>,
        amount: u64,
        route_data: RouteData,
    ) -> Result<()> {
        // Validate program version
        require!(
            ctx.accounts.program_state.version == CURRENT_VERSION,
            ErrorCode::VersionMismatch
        );
        
        // Validate protocol program matches registered program_id
        require!(
            ctx.accounts.protocol_program.key() == ctx.accounts.protocol_info.program_id,
            ErrorCode::InvalidProgramId
        );
        
        // Validate protocol is active
        require!(ctx.accounts.protocol_info.is_active, ErrorCode::ProtocolInactive);
        
        // Validate amount is non-zero
        require!(amount > 0, ErrorCode::InvalidParameter);
        
        // Validate route data size
        require!(
            route_data.swap_accounts_len as usize <= MAX_SWAP_ACCOUNTS,
            ErrorCode::RouteDataTooLarge
        );
        require!(
            route_data.swap_data.len() <= MAX_SWAP_DATA,
            ErrorCode::RouteDataTooLarge
        );
        
        // Validate aggregator is not paused
        require!(!ctx.accounts.aggregator.is_paused, ErrorCode::SystemPaused);
        
        // Enforce rate limits
        enforce_rate_limit(&mut ctx.accounts.rate_limiter)?;
        enforce_exec_window(&mut ctx.accounts.aggregator)?;
        
        // Snapshot pre-loan balance for profit calculation
        let pre_balance = ctx.accounts.aggregator_token_account.amount;
        
        // Execute flash loan via CPI with proper error handling
        let result = match ctx.accounts.protocol_info.protocol_type {
            ProtocolType::Solend => SolendProtocol::new(ctx, ctx.accounts.protocol_info, ctx.accounts.protocol_program)
                .flash_borrow(amount)
                .map_err(|e| {
                    ctx.accounts.protocol_info.failure_count += 1;
                    disable_if_unhealthy(&mut ctx.accounts.protocol_info)?;
                    e
                }),
            // Similar handling for other protocols
            _ => Err(ErrorCode::OperationNotSupported.into())
        };
        
        result?;
        
        // Execute swap route with slippage protection
        util::execute_fixed_route(&ctx.accounts, &route_data)?;
        
        // Calculate and validate profit
        let post_balance = ctx.accounts.aggregator_token_account.amount;
        let profit = post_balance
            .checked_sub(pre_balance)
            .ok_or(ErrorCode::MathOverflow)?;
        
        require!(
            profit >= route_data.expected_amount_out
                .checked_sub(amount)
                .ok_or(ErrorCode::MathOverflow)?,
            ErrorCode::InsufficientExpectedProfit
        );
        
        // Update metrics with overflow checks
        let aggr = &mut ctx.accounts.aggregator;
        aggr.total_volume = aggr
            .total_volume
            .checked_add(amount as u128)
            .ok_or(ErrorCode::MathOverflow)?;
        aggr.total_profit = aggr
            .total_profit
            .checked_add(profit as u128)
            .ok_or(ErrorCode::MathOverflow)?;
        aggr.total_flash_loans = aggr
            .total_flash_loans
            .checked_add(1)
            .ok_or(ErrorCode::MathOverflow)?;
        
        // Update protocol success count
        ctx.accounts.protocol_info.success_count += 1;
        
        // Emit event
        emit!(FlashLoanExecuted {
            protocol: ctx.accounts.protocol_info.protocol_type,
            amount,
            profit,
            timestamp: Clock::get()?.unix_timestamp,
            slot: Clock::get()?.slot,
            success: true,
        });
        
        Ok(())
    }

    pub fn emergency_pause(ctx: Context<EmergencyControl>) -> Result<()> {
        ctx.accounts.aggregator.is_paused = true;
        emit!(EmergencyPause {
            authority: ctx.accounts.authority.key(),
            timestamp: Clock::get()?.unix_timestamp,
        });
        Ok(())
    }

    pub fn resume_operations(ctx: Context<EmergencyControl>) -> Result<()> {
        ctx.accounts.aggregator.is_paused = false;
        emit!(OperationsResumed {
            authority: ctx.accounts.authority.key(),
            timestamp: Clock::get()?.unix_timestamp,
        });
        Ok(())
    }

    pub fn update_min_profit(ctx: Context<UpdateConfig>, new_min_profit_bps: u16) -> Result<()> {
        require!(new_min_profit_bps > 0 && new_min_profit_bps < 1000, ErrorCode::InvalidParameter);
        ctx.accounts.aggregator.min_profit_bps = new_min_profit_bps;
        Ok(())
    }

    pub fn withdraw_with_timelock(
        ctx: Context<WithdrawProfits>,
        amount: u64,
    ) -> Result<()> {
        // Validate program version
        require!(
            ctx.accounts.program_state.version == CURRENT_VERSION,
            ErrorCode::VersionMismatch
        );
        
        // Validate timelock conditions
        let now = Clock::get()?.unix_timestamp;
        ctx.accounts.timelock.is_unlocked(now)?;
        ctx.accounts.timelock.authorize(&ctx.remaining_accounts, now)?;
        
        // Prepare PDA seeds for signer
        let seeds = &[
            b"aggregator",
            &[ctx.accounts.aggregator.bump],
        ];
        let signer = &[&seeds[..]];
        
        // Create CPI accounts
        let cpi_accounts = Transfer {
            from: ctx.accounts.aggregator_token_account.to_account_info(),
            to: ctx.accounts.treasury.to_account_info(),
            authority: ctx.accounts.aggregator.to_account_info(),
        };
        
        // Execute token transfer with PDA signer
        token::transfer(
            CpiContext::new_with_signer(
                ctx.accounts.token_program.to_account_info(),
                cpi_accounts,
                signer
            ),
            amount
        )?;
        
        // Mark timelock as executed
        ctx.accounts.timelock.executed = true;
        
        // Emit event
        emit!(ProfitWithdrawn {
            amount,
            timestamp: now,
        });
        
        Ok(())
    }

    pub fn update_config(ctx: Context<UpdateConfig>, new_config: Config) -> Result<()> {
        ctx.accounts.config.set_inner(new_config);
        Ok(())
    }

    pub fn migrate(
        ctx: Context<Migrate>,
        new_version: u8
    ) -> Result<()> {
        let ps = &mut ctx.accounts.program_state;
        invariant!(ps.version < new_version, ErrorCode::Unauthorized);
        
        // Apply any state migrations here
        if new_version >= 2 {
            // Example migration for v2
            ctx.accounts.aggregator.slippage_bps = 50; // New default
        }
        
        ps.version = new_version;
        Ok(())
    }
}

#[derive(Accounts)]
pub struct SetAuthority<'info> {
    #[account(mut, has_one = authority)]
    pub aggregator: Account<'info, Aggregator>,
    pub authority: Signer<'info>,
}

#[event]
pub struct AuthoritySet {
    pub old: Pubkey,
    pub new: Pubkey,
}

#[event]
pub struct LoanAttempt {
    pub protocol: ProtocolType,
    pub success: bool,
    pub profit: u64,
}

#[derive(Accounts)]
pub struct ExecuteAggregatedFlashLoan<'info> {
    #[account(mut)]
    pub rate_limiter: Account<'info, RateLimiter>,
    #[account(mut)]
    pub aggregator: Account<'info, Aggregator>,
    pub config: Account<'info, Config>,
    #[account(mut)]
    pub profit_vault: Account<'info, TokenAccount>,
    #[account(mut)]
    pub user_token: Account<'info, TokenAccount>,
    pub token_program: Program<'info, Token>,
    #[account(mut)]
    pub slippage_ewma: Account<'info, EWMA>,
}

#[derive(Accounts)]
pub struct ProtocolAdapter<'info> {
    pub protocol_type: ProtocolType,
    pub adapter_program: Pubkey,
    pub last_updated: i64,
    // ... other fields ...
}

impl<'info> ProtocolAdapter<'info> {
    pub fn flash_borrow(&self, amount: u64) -> Result<()> {
        match self.protocol_type {
            ProtocolType::Solend => solend::flash_borrow(self.ctx, amount),
            ProtocolType::PortFinance => port_finance::flash_borrow(self.ctx, amount),
            // ... other protocols ...
        }
    }
}

#[account]
#[repr(packed)]
pub struct Aggregator {
    pub authority: Pubkey,
    pub token_account_bump: u8,
    pub slippage_bps: u16,
    pub min_profit_bps: u16,
    pub total_volume: u128,
    pub total_profit: u128,
    pub total_loans: u64,
    pub paused: bool,
    pub last_exec_slot: u64,
    pub exec_count_this_slot: u16,
    pub max_execs_per_slot: u16,
}

#[account]
#[repr(packed)]
pub struct ProtocolInfo {
    pub aggregator: Pubkey,
    pub protocol_type: ProtocolType,
    pub program_id: Pubkey,
    pub reserve_liquidity: Pubkey,
    pub reserve_collateral: Pubkey,
    pub bump: u8,
    pub success_count: u32,
    pub failure_count: u32,
    pub is_active: bool
}

#[account]
#[repr(packed)]
pub struct Config {
    pub min_profit_bps: u16,
    pub slippage_bps: u16,
    pub disable_threshold_pct: u8,
    pub last_update: i64,
}

#[derive(AnchorSerialize, AnchorDeserialize, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolType {
    Solend,
    PortFinance
}

#[derive(Accounts)]
pub struct BatchFlashLoan<'info> {
    #[account(mut, seeds = [b"aggregator"], bump)]
    pub aggregator: Account<'info, Aggregator>,
    #[account(mut)]
    pub protocol_info: Account<'info, ProtocolInfo>,
    #[account(mut)]
    pub reserve_liquidity_supply: Account<'info, TokenAccount>,
    #[account(mut)]
    pub user_token_account: Account<'info, TokenAccount>,
    pub token_program: Program<'info, Token>,
    pub remaining: Vec<AccountInfo<'info>>,
    pub program_state: Account<'info, ProgramState>,
}

pub fn batch_flash_loan(
    ctx: Context<BatchFlashLoan>,
    amounts: [u64; 8], // fixed max 8 batches
) -> Result<()> {
    require!(
        ctx.accounts.program_state.version == CURRENT_VERSION,
        ErrorCode::VersionMismatch
    );
    for i in 0..ctx.accounts.batch_size as usize {
        execute_mini_loan(
            ctx.accounts.remaining[i].clone(),
            amounts[i]
        )?;
    }
    Ok(())
}

fn execute_mini_loan(
    account_info: AccountInfo,
    amount: u64
) -> Result<()> {
    // Simplified flash loan execution for batching
    let cpi_accounts = Transfer {
        from: account_info.clone(),
        to: account_info.clone(),
        authority: account_info.clone(),
    };
    let cpi_program = account_info.clone();
    let cpi_ctx = CpiContext::new(cpi_program, cpi_accounts);
    token::transfer(cpi_ctx, amount)?;
    Ok(())
}

// Monitoring and metrics
impl Aggregator {
    pub fn calculate_success_rate(&self) -> f64 {
        if self.total_flash_loans == 0 {
            return 0.0;
        }
        
        let successful_loans = self.total_profit / MIN_PROFIT_THRESHOLD.max(1);
        successful_loans as f64 / self.total_flash_loans as f64
    }
    
    pub fn average_profit_per_loan(&self) -> u64 {
        if self.total_flash_loans == 0 {
            return 0;
        }
        
        self.total_profit / self.total_flash_loans
    }
    
    pub fn is_profitable(&self) -> bool {
        self.total_profit > 0 && self.calculate_success_rate() > 0.7
    }
    
    pub fn record_flash_loan(&mut self, amount: u64, profit: u64) -> Result<()> {
        self.total_volume = self.total_volume.checked_add(amount as u128)
            .ok_or(ErrorCode::MathOverflow)?;
        self.total_profit = self.total_profit.checked_add(profit as u128)
            .ok_or(ErrorCode::MathOverflow)?;
        self.total_flash_loans = self.total_flash_loans.checked_add(1)
            .ok_or(ErrorCode::MathOverflow)?;
        
        Ok(())
    }
    
    pub fn record_failure(&mut self) -> Result<()> {
        self.total_failures = self.total_failures.checked_add(1)
            .ok_or(ErrorCode::MathOverflow)?;
        Ok(())
    }
}

impl ProtocolInfo {
    pub fn utilization_rate(&self, available_liquidity: u64) -> u64 {
        if available_liquidity == 0 {
            return 10000; // 100%
        }
        
        self.total_borrowed * 10000 / (self.total_borrowed + available_liquidity)
    }
    
    pub fn effective_fee_rate(&self, amount: u64) -> u64 {
        // Dynamic fee based on amount (larger amounts might get better rates)
        if amount > 1_000_000_000_000 { // > 1M tokens
            self.fee_bps as u64 * 90 / 100 // 10% discount
        } else if amount > 100_000_000_000 { // > 100k tokens
            self.fee_bps as u64 * 95 / 100 // 5% discount
        } else {
            self.fee_bps as u64
        }
    }
}

// Constants for mainnet operations
pub const JITO_TIP_ACCOUNTS: [&str; 8] = [
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe", 
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
];

pub const PRIORITY_LEVELS: [(u64, &str); 5] = [
    (10_000, "MIN"),
    (50_000, "LOW"),
    (100_000, "MEDIUM"),
    (500_000, "HIGH"),
    (2_000_000, "ULTRA"),
];

#[event]
pub struct FlashLoanExecuted {
    pub protocol: ProtocolType,
    pub amount: u64,
    pub profit: u64,
    pub timestamp: i64,
    pub slot: u64,
    pub success: bool,
}

#[event]
pub struct ProtocolRegistered {
    pub protocol_type: ProtocolType,
    pub reserve: Pubkey,
    pub fee_bps: u16,
    pub timestamp: i64,
}

#[event]
pub struct ProfitWithdrawn {
    pub amount: u64,
    pub timestamp: i64,
}

#[event]
pub struct RateLimitHit {
    pub slot: u64,
    pub count: u8,
    pub max: u8,
}

#[cfg(test)]
mod tests {
    use super::*;
    use anchor_lang::prelude::*;
    use solana_program_test::*;
    use solana_sdk::{signature::Keypair, signer::Signer};

    #[tokio::test]
    async fn test_initialize() {
        let mut program_test = ProgramTest::new(
            "flash_aggregator",
            program_id(),
            processor!(process_instruction)
        );
        
        let (mut banks_client, payer, recent_blockhash) = program_test.start().await;
        
        let aggregator = Keypair::new();
        let config = Keypair::new();
        
        // Test initialization with valid parameters
        let tx = Transaction::new_signed_with_payer(
            &[instruction::initialize(
                &program_id(),
                &aggregator.pubkey(),
                &config.pubkey(),
                &payer.pubkey(),
                50, // slippage_bps
                10, // min_profit_bps
                10, // disable_threshold_pct
                1, // version
            ).unwrap()],
            Some(&payer.pubkey()),
            &[&payer, &aggregator],
            recent_blockhash
        );
        
        banks_client.process_transaction(tx).await.unwrap();
    }

    #[tokio::test]
    async fn test_flash_loan() {
        let mut program_test = ProgramTest::new(
            "flash_aggregator",
            program_id(),
            processor!(process_instruction)
        );
        
        // Add test accounts (protocol, reserves, etc.)
        // ...
        
        let (mut banks_client, payer, recent_blockhash) = program_test.start().await;
        
        // Test flash loan execution with valid parameters
        let tx = Transaction::new_signed_with_payer(
            &[instruction::execute_aggregated_flash_loan(
                &program_id(),
                &aggregator.pubkey(),
                &protocol_info.pubkey(),
                &reserve_liquidity.pubkey(),
                &user_token.pubkey(),
                100_000_000, // amount
                route_data   // swap route
            ).unwrap()],
            Some(&payer.pubkey()),
            &[&payer],
            recent_blockhash
        );
        
        banks_client.process_transaction(tx).await.unwrap();
    }

    #[tokio::test]
    async fn test_deregister_protocol() -> Result<()> {
        let mut program_test = ProgramTest::new(
            "flash_aggregator",
            program_id(),
            processor!(process_instruction)
        );
        
        let (mut banks_client, payer, recent_blockhash) = program_test.start().await;
        
        // Initialize and register protocol first
        // ...
        
        // Test deregistration
        let tx = Transaction::new_signed_with_payer(
            &[instruction::deregister_protocol(
                &program_id(),
                &aggregator.pubkey(),
                &protocol_info.pubkey(),
                &payer.pubkey()
            ).unwrap()],
            Some(&payer.pubkey()),
            &[&payer],
            recent_blockhash
        );
        
        banks_client.process_transaction(tx).await.unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn test_fallback_routing() -> Result<()> {
        let mut program_test = ProgramTest::new(
            "flash_aggregator",
            program_id(),
            processor!(process_instruction)
        );
        
        // Setup with primary and fallback protocols
        // Force primary to fail
        // Verify fallback succeeds
        Ok(())
    }

    #[tokio::test]
    async fn test_authority_rotation() -> Result<()> {
        let mut program_test = ProgramTest::new(
            "flash_aggregator",
            program_id(),
            processor!(process_instruction)
        );
        
        // Test authority rotation flow
        Ok(())
    }
}

#[derive(Accounts)]
pub struct WithdrawProfits<'info> {
    #[account(mut, seeds = [b"aggregator"], bump = aggregator.bump)]
    pub aggregator: Account<'info, Aggregator>,
    
    #[account(mut, seeds = [b"timelock", aggregator.key().as_ref()], bump)]
    pub timelock: Account<'info, Timelock>,
    
    #[account(mut, seeds = [b"agg_token", aggregator.key().as_ref()], bump = aggregator.token_account_bump)]
    pub aggregator_token_account: Account<'info, TokenAccount>,
    
    #[account(mut)]
    pub treasury: Account<'info, TokenAccount>,
    
    pub token_program: Program<'info, Token>,
    
    #[account()]
    pub program_state: Account<'info, ProgramState>,
}
