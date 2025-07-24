use anchor_lang::prelude::*;
use anchor_lang::solana_program::{
    clock::Clock,
    instruction::{AccountMeta, Instruction},
    program::{invoke, invoke_signed},
    pubkey::Pubkey,
    sysvar,
};
use anchor_spl::token::{self, Token, TokenAccount, Transfer, Mint};
use arrayref::{array_ref, array_refs};
use solana_program::program_pack::Pack;
use std::mem::size_of;

declare_id!("FLsh1111111111111111111111111111111111111111");

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

#[program]
pub mod multi_protocol_flash_loan_aggregator {
    use super::*;

    pub fn initialize(ctx: Context<Initialize>) -> Result<()> {
        let aggregator = &mut ctx.accounts.aggregator;
        aggregator.authority = ctx.accounts.authority.key();
        aggregator.treasury = ctx.accounts.treasury.key();
        aggregator.bump = *ctx.bumps.get("aggregator").unwrap();
        aggregator.total_volume = 0;
        aggregator.total_profit = 0;
        aggregator.total_flash_loans = 0;
        aggregator.is_paused = false;
        aggregator.min_profit_bps = 10;
        aggregator.protocol_count = 0;
        
        emit!(AggregatorInitialized {
            authority: aggregator.authority,
            treasury: aggregator.treasury,
            timestamp: Clock::get()?.unix_timestamp,
        });
        
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
        };
        protocol.reserve_address = reserve_address;
        protocol.collateral_mint = collateral_mint;
        protocol.liquidity_address = ctx.accounts.liquidity_address.key();
        protocol.fee_bps = fee_bps;
        protocol.is_active = true;
        protocol.last_update = Clock::get()?.unix_timestamp;
        protocol.total_borrowed = 0;
        protocol.bump = *ctx.bumps.get("protocol_info").unwrap();
        
        ctx.accounts.aggregator.protocol_count += 1;
        
        emit!(ProtocolRegistered {
            protocol_type,
            reserve: reserve_address,
            fee_bps,
            timestamp: Clock::get()?.unix_timestamp,
        });
        
        Ok(())
    }

    pub fn execute_aggregated_flash_loan(
        ctx: Context<ExecuteAggregatedFlashLoan>,
        amount: u64,
        route_data: RouteData,
    ) -> Result<()> {
        require!(!ctx.accounts.aggregator.is_paused, ErrorCode::SystemPaused);
        require!(amount > MIN_PROFIT_THRESHOLD, ErrorCode::AmountTooLow);
        
        let clock = Clock::get()?;
        let aggregator = &mut ctx.accounts.aggregator;
        
        // Find best protocol
        let protocol = &ctx.accounts.protocol_info;
        require!(protocol.is_active, ErrorCode::ProtocolInactive);
        
        // Calculate fees
        let fee_amount = amount
            .checked_mul(protocol.fee_bps as u64)
            .ok_or(ErrorCode::MathOverflow)?
            .checked_div(FLASH_LOAN_FEE_DIVISOR)
            .ok_or(ErrorCode::MathOverflow)?;
            
        let total_repay = amount
            .checked_add(fee_amount)
            .ok_or(ErrorCode::MathOverflow)?;
        
        // Verify profitability
        let expected_profit = route_data.expected_profit;
        require!(
            expected_profit >= amount * aggregator.min_profit_bps as u64 / 10000,
            ErrorCode::InsufficientExpectedProfit
        );
        
        // Execute flash loan
        match protocol.protocol_type {
            ProtocolType::Solend => {
                execute_solend_flash_loan(&ctx, amount, total_repay, &route_data)?;
            },
            ProtocolType::PortFinance => {
                execute_port_finance_flash_loan(&ctx, amount, total_repay, &route_data)?;
            },
            ProtocolType::Larix => {
                execute_larix_flash_loan(&ctx, amount, total_repay, &route_data)?;
            },
            ProtocolType::Kamino => {
                execute_kamino_flash_loan(&ctx, amount, total_repay, &route_data)?;
            },
        }
        
        // Verify profit
        let final_balance = ctx.accounts.user_token_account.amount;
        let actual_profit = final_balance
            .checked_sub(total_repay)
            .ok_or(ErrorCode::FlashLoanUnprofitable)?;
            
        require!(
            actual_profit >= expected_profit * 95 / 100,
            ErrorCode::ProfitBelowExpectation
        );
        
        // Update metrics
        aggregator.total_volume = aggregator.total_volume
            .checked_add(amount)
            .ok_or(ErrorCode::MathOverflow)?;
        aggregator.total_profit = aggregator.total_profit
            .checked_add(actual_profit)
            .ok_or(ErrorCode::MathOverflow)?;
        aggregator.total_flash_loans += 1;
        
        let protocol_info = &mut ctx.accounts.protocol_info;
        protocol_info.total_borrowed = protocol_info.total_borrowed
            .checked_add(amount)
            .ok_or(ErrorCode::MathOverflow)?;
        protocol_info.last_update = clock.unix_timestamp;
        
        emit!(FlashLoanExecuted {
            protocol: protocol.protocol_type,
            amount,
            fee: fee_amount,
            profit: actual_profit,
            timestamp: clock.unix_timestamp,
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

    pub fn withdraw_profits(ctx: Context<WithdrawProfits>, amount: u64) -> Result<()> {
        let seeds = &[
            b"aggregator",
            &[ctx.accounts.aggregator.bump],
        ];
        let signer = &[&seeds[..]];
        
        let cpi_accounts = Transfer {
            from: ctx.accounts.aggregator_token_account.to_account_info(),
            to: ctx.accounts.treasury_token_account.to_account_info(),
            authority: ctx.accounts.aggregator.to_account_info(),
        };
        let cpi_program = ctx.accounts.token_program.to_account_info();
        let cpi_ctx = CpiContext::new_with_signer(cpi_program, cpi_accounts, signer);
        
        token::transfer(cpi_ctx, amount)?;
        
        Ok(())
    }
}

#[derive(Accounts)]
pub struct Initialize<'info> {
    #[account(
        init,
        payer = authority,
        space = 8 + size_of::<Aggregator>(),
        seeds = [b"aggregator"],
        bump
    )]
    pub aggregator: Account<'info, Aggregator>,
    /// CHECK: Treasury address
    pub treasury: AccountInfo<'info>,
    #[account(mut)]
    pub authority: Signer<'info>,
    pub system_program: Program<'info, System>,
}

#[derive(Accounts)]
pub struct RegisterProtocol<'info> {
    #[account(mut, seeds = [b"aggregator"], bump = aggregator.bump)]
    pub aggregator: Account<'info, Aggregator>,
    #[account(
        init,
        payer = authority,
        space = 8 + size_of::<ProtocolInfo>(),
        seeds = [b"protocol", reserve_address.key().as_ref()],
        bump
    )]
    pub protocol_info: Account<'info, ProtocolInfo>,
    /// CHECK: Protocol reserve address
    pub reserve_address: AccountInfo<'info>,
    /// CHECK: Liquidity supply address
    pub liquidity_address: AccountInfo<'info>,
    #[account(mut, constraint = authority.key() == aggregator.authority)]
    pub authority: Signer<'info>,
    pub system_program: Program<'info, System>,
}

#[derive(Accounts)]
pub struct ExecuteAggregatedFlashLoan<'info> {
    #[account(mut, seeds = [b"aggregator"], bump = aggregator.bump)]
    pub aggregator: Account<'info, Aggregator>,
    #[account(
        mut,
        seeds = [b"protocol", protocol_info.reserve_address.as_ref()],
        bump = protocol_info.bump
    )]
    pub protocol_info: Account<'info, ProtocolInfo>,
    #[account(mut)]
    pub user: Signer<'info>,
    #[account(mut)]
    pub user_token_account: Account<'info, TokenAccount>,
    #[account(mut)]
    pub aggregator_token_account: Account<'info, TokenAccount>,
    /// CHECK: Protocol reserve
    #[account(mut)]
    pub reserve: AccountInfo<'info>,
    /// CHECK: Reserve liquidity supply
    #[account(mut)]
    pub reserve_liquidity_supply: AccountInfo<'info>,
    /// CHECK: Reserve collateral mint
    #[account(mut)]
    pub reserve_collateral_mint: AccountInfo<'info>,
    /// CHECK: Protocol program
    pub protocol_program: AccountInfo<'info>,
    pub token_program: Program<'info, Token>,
    pub system_program: Program<'info, System>,
    /// CHECK: Clock sysvar
    pub clock: AccountInfo<'info>,
}

#[derive(Accounts)]
pub struct EmergencyControl<'info> {
    #[account(mut, seeds = [b"aggregator"], bump = aggregator.bump)]
    pub aggregator: Account<'info, Aggregator>,
    #[account(constraint = authority.key() == aggregator.authority)]
    pub authority: Signer<'info>,
}

#[derive(Accounts)]
pub struct UpdateConfig<'info> {
    #[account(mut, seeds = [b"aggregator"], bump = aggregator.bump)]
    pub aggregator: Account<'info, Aggregator>,
    #[account(constraint = authority.key() == aggregator.authority)]
    pub authority: Signer<'info>,
}

#[derive(Accounts)]
pub struct WithdrawProfits<'info> {
    #[account(mut, seeds = [b"aggregator"], bump = aggregator.bump)]
    pub aggregator: Account<'info, Aggregator>,
    #[account(mut)]
    pub aggregator_token_account: Account<'info, TokenAccount>,
    #[account(mut)]
    pub treasury_token_account: Account<'info, TokenAccount>,
    #[account(constraint = authority.key() == aggregator.authority)]
    pub authority: Signer<'info>,
    pub token_program: Program<'info, Token>,
}

#[account]
pub struct Aggregator {
    pub authority: Pubkey,
    pub treasury: Pubkey,
    pub bump: u8,
    pub total_volume: u64,
    pub total_profit: u64,
    pub total_flash_loans: u64,
    pub is_paused: bool,
    pub min_profit_bps: u16,
    pub protocol_count: u8,
}

#[account]
pub struct ProtocolInfo {
    pub protocol_type: ProtocolType,
    pub program_id: Pubkey,
    pub reserve_address: Pubkey,
    pub collateral_mint: Pubkey,
    pub liquidity_address: Pubkey,
    pub fee_bps: u16,
    pub is_active: bool,
    pub last_update: i64,
    pub total_borrowed: u64,
    pub bump: u8,
}

#[derive(AnchorSerialize, AnchorDeserialize, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolType {
    Solend,
    PortFinance,
    Larix,
    Kamino,
}

#[derive(AnchorSerialize, AnchorDeserialize, Clone)]
pub struct RouteData {
    pub dex_program: Pubkey,
    pub swap_accounts: Vec<Pubkey>,
    pub swap_data: Vec<u8>,
    pub expected_profit: u64,
    pub max_slippage_bps: u16,
}

#[event]
pub struct AggregatorInitialized {
    pub authority: Pubkey,
    pub treasury: Pubkey,
    pub timestamp: i64,
}

#[event]
pub struct ProtocolRegistered {
    pub protocol_type: ProtocolType,
    pub reserve: Pubkey,
    pub fee_bps: u16,
    pub timestamp: i64,
}

#[event]
pub struct FlashLoanExecuted {
    pub protocol: ProtocolType,
    pub amount: u64,
    pub fee: u64,
    pub profit: u64,
    pub timestamp: i64,
}

#[event]
pub struct EmergencyPause {
    pub authority: Pubkey,
    pub timestamp: i64,
}

#[event]
pub struct OperationsResumed {
    pub authority: Pubkey,
    pub timestamp: i64,
}

#[error_code]
pub enum ErrorCode {
    #[msg("System is currently paused")]
    SystemPaused,
    #[msg("Maximum number of protocols reached")]
    MaxProtocolsReached,
    #[msg("Amount too low")]
    AmountTooLow,
    #[msg("Protocol is inactive")]
    ProtocolInactive,
    #[msg("Math overflow")]
    MathOverflow,
    #[msg("Insufficient expected profit")]
    InsufficientExpectedProfit,
    #[msg("Flash loan resulted in loss")]
    FlashLoanUnprofitable,
    #[msg("Actual profit below expectation")]
    ProfitBelowExpectation,
    #[msg("Invalid parameter")]
    InvalidParameter,
    #[msg("Invalid instruction data")]
    InvalidInstructionData,
    #[msg("Slippage exceeded")]
    SlippageExceeded,
}

// Helper functions
fn solend_program_id() -> Pubkey {
    SOLEND_PROGRAM_ID.parse::<Pubkey>().unwrap()
}

fn port_finance_program_id() -> Pubkey {
    PORT_FINANCE_PROGRAM_ID.parse::<Pubkey>().unwrap()
}

fn larix_program_id() -> Pubkey {
    LARIX_PROGRAM_ID.parse::<Pubkey>().unwrap()
}

fn kamino_program_id() -> Pubkey {
    KAMINO_PROGRAM_ID.parse::<Pubkey>().unwrap()
}

fn execute_solend_flash_loan<'info>(
    ctx: &Context<ExecuteAggregatedFlashLoan<'info>>,
    amount: u64,
    total_repay: u64,
    route_data: &RouteData,
) -> Result<()> {
    // Solend flash loan instruction data
    let mut data = vec![0xca, 0x32, 0x5c, 0xe7]; // Flash borrow discriminator
    data.extend_from_slice(&amount.to_le_bytes());
    
    let flash_borrow_accounts = vec![
        AccountMeta::new(ctx.accounts.reserve.key(), false),
        AccountMeta::new(ctx.accounts.reserve_liquidity_supply.key(), false),
        AccountMeta::new(ctx.accounts.user_token_account.key(), false),
        AccountMeta::new_readonly(ctx.accounts.reserve_collateral_mint.key(), false),
        AccountMeta::new(ctx.accounts.user.key(), true),
        AccountMeta::new_readonly(ctx.accounts.clock.key(), false),
        AccountMeta::new_readonly(ctx.accounts.token_program.key(), false),
    ];
    
    let flash_borrow_ix = Instruction {
        program_id: ctx.accounts.protocol_program.key(),
        accounts: flash_borrow_accounts,
        data,
    };
    
    invoke(&flash_borrow_ix, &[
        ctx.accounts.reserve.to_account_info(),
        ctx.accounts.reserve_liquidity_supply.to_account_info(),
        ctx.accounts.user_token_account.to_account_info(),
        ctx.accounts.reserve_collateral_mint.to_account_info(),
        ctx.accounts.user.to_account_info(),
        ctx.accounts.clock.to_account_info(),
        ctx.accounts.token_program.to_account_info(),
    ])?;
    
    // Execute arbitrage
    execute_swap_route(ctx, route_data)?;
    
    // Repay flash loan
    let mut repay_data = vec![0xd7, 0x15, 0x4b, 0x3a]; // Flash repay discriminator
    repay_data.extend_from_slice(&total_repay.to_le_bytes());
    
    let flash_repay_accounts = vec![
        AccountMeta::new(ctx.accounts.reserve.key(), false),
        AccountMeta::new(ctx.accounts.reserve_liquidity_supply.key(), false),
        AccountMeta::new(ctx.accounts.user_token_account.key(), false),
        AccountMeta::new(ctx.accounts.reserve_collateral_mint.key(), false),
        AccountMeta::new(ctx.accounts.user.key(), true),
        AccountMeta::new_readonly(ctx.accounts.clock.key(), false),
        AccountMeta::new_readonly(ctx.accounts.token_program.key(), false),
    ];
    
    let flash_repay_ix = Instruction {
        program_id: ctx.accounts.protocol_program.key(),
        accounts: flash_repay_accounts,
        data: repay_data,
    };
    
    invoke(&flash_repay_ix, &[
        ctx.accounts.reserve.to_account_info(),
        ctx.accounts.reserve_liquidity_supply.to_account_info(),
        ctx.accounts.user_token_account.to_account_info(),
        ctx.accounts.reserve_collateral_mint.to_account_info(),
        ctx.accounts.user.to_account_info(),
        ctx.accounts.clock.to_account_info(),
        ctx.accounts.token_program.to_account_info(),
    ])?;
    
    Ok(())
}

fn execute_port_finance_flash_loan<'info>(
    ctx: &Context<ExecuteAggregatedFlashLoan<'info>>,
    amount: u64,
    total_repay: u64,
    route_data: &RouteData,
) -> Result<()> {
    // Port Finance uses a single instruction for flash loan
    let mut data = vec![0x5a, 0x61, 0x75, 0x72]; // Port flash loan discriminator
    data.extend_from_slice(&amount.to_le_bytes());
    data.extend_from_slice(&1u64.to_le_bytes()); // Number of instructions to execute
    
    let accounts = vec![
        AccountMeta::new(ctx.accounts.reserve.key(), false),
        AccountMeta::new(ctx.accounts.user_token_account.key(), false),
        AccountMeta::new(ctx.accounts.user.key(), true),
        AccountMeta::new_readonly(ctx.accounts.token_program.key(), false),
        AccountMeta::new_readonly(sysvar::instructions::id(), false),
    ];
    
    let flash_loan_ix = Instruction {
        program_id: ctx.accounts.protocol_program.key(),
        accounts,
        data,
    };
    
    invoke(&flash_loan_ix, &[
        ctx.accounts.reserve.to_account_info(),
        ctx.accounts.user_token_account.to_account_info(),
        ctx.accounts.user.to_account_info(),
        ctx.accounts.token_program.to_account_info(),
    ])?;
    
    execute_swap_route(ctx, route_data)?;
    
    Ok(())
}

fn execute_larix_flash_loan<'info>(
    ctx: &Context<ExecuteAggregatedFlashLoan<'info>>,
    amount: u64,
    total_repay: u64,
    route_data: &RouteData,
) -> Result<()> {
    // Larix flash loan begin
    let mut begin_data = vec![0xe8, 0x9f, 0x4a, 0x21]; // Begin flash loan
    begin_data.extend_from_slice(&amount.to_le_bytes());
    
    let begin_accounts = vec![
        AccountMeta::new(ctx.accounts.reserve.key(), false),
        AccountMeta::new(ctx.accounts.reserve_liquidity_supply.key(), false),
        AccountMeta::new(ctx.accounts.user_token_account.key(), false),
        AccountMeta::new(ctx.accounts.user.key(), true),
        AccountMeta::new_readonly(ctx.accounts.token_program.key(), false),
    ];
    
    let begin_ix = Instruction {
        program_id: ctx.accounts.protocol_program.key(),
        accounts: begin_accounts,
        data: begin_data,
    };
    
    invoke(&begin_ix, &[
        ctx.accounts.reserve.to_account_info(),
        ctx.accounts.reserve_liquidity_supply.to_account_info(),
        ctx.accounts.user_token_account.to_account_info(),
        ctx.accounts.user.to_account_info(),
        ctx.accounts.token_program.to_account_info(),
    ])?;
    
    execute_swap_route(ctx, route_data)?;
    
    // Larix flash loan end
    let mut end_data = vec![0x12, 0x34, 0x56, 0x78]; // End flash loan
    end_data.extend_from_slice(&total_repay.to_le_bytes());
    
    let end_accounts = vec![
        AccountMeta::new(ctx.accounts.reserve.key(), false),
        AccountMeta::new(ctx.accounts.reserve_liquidity_supply.key(), false),
        AccountMeta::new(ctx.accounts.user_token_account.key(), false),
        AccountMeta::new(ctx.accounts.user.key(), true),
        AccountMeta::new_readonly(ctx.accounts.token_program.key(), false),
    ];
    
    let end_ix = Instruction {
        program_id: ctx.accounts.protocol_program.key(),
        accounts: end_accounts,
        data: end_data,
    };
    
    invoke(&end_ix, &[
        ctx.accounts.reserve.to_account_info(),
        ctx.accounts.reserve_liquidity_supply.to_account_info(),
        ctx.accounts.user_token_account.to_account_info(),
        ctx.accounts.user.to_account_info(),
        ctx.accounts.token_program.to_account_info(),
    ])?;
    
    Ok(())
}

fn execute_kamino_flash_loan<'info>(
    ctx: &Context<ExecuteAggregatedFlashLoan<'info>>,
    amount: u64,
    total_repay: u64,
    route_data: &RouteData,
) -> Result<()> {
    // Kamino flash loan instruction
    let mut data = vec![0xf0, 0x1d, 0xca, 0xfe]; // Kamino flash loan discriminator
    data.extend_from_slice(&amount.to_le_bytes());
    
    let accounts = vec![
        AccountMeta::new(ctx.accounts.reserve.key(), false),
        AccountMeta::new(ctx.accounts.reserve_liquidity_supply.key(), false),
        AccountMeta::new(ctx.accounts.reserve_collateral_mint.key(), false),
        AccountMeta::new(ctx.accounts.user_token_account.key(), false),
        AccountMeta::new(ctx.accounts.user.key(), true),
        AccountMeta::new_readonly(sysvar::instructions::id(), false),
        AccountMeta::new_readonly(ctx.accounts.token_program.key(), false),
    ];
    
    let flash_loan_ix = Instruction {
        program_id: ctx.accounts.protocol_program.key(),
        accounts,
        data,
    };
    
    invoke(&flash_loan_ix, &[
        ctx.accounts.reserve.to_account_info(),
        ctx.accounts.reserve_liquidity_supply.to_account_info(),
        ctx.accounts.reserve_collateral_mint.to_account_info(),
        ctx.accounts.user_token_account.to_account_info(),
        ctx.accounts.user.to_account_info(),
        ctx.accounts.token_program.to_account_info(),
    ])?;
    
    execute_swap_route(ctx, route_data)?;
    
    Ok(())
}

fn execute_swap_route<'info>(
    ctx: &Context<ExecuteAggregatedFlashLoan<'info>>,
    route_data: &RouteData,
) -> Result<()> {
    let swap_accounts: Vec<AccountMeta> = route_data.swap_accounts
        .iter()
        .enumerate()
        .map(|(i, pubkey)| {
            if i < 2 {
                AccountMeta::new(*pubkey, false)
            } else {
                AccountMeta::new_readonly(*pubkey, false)
            }
        })
        .collect();
    
    let swap_ix = Instruction {
        program_id: route_data.dex_program,
        accounts: swap_accounts,
        data: route_data.swap_data.clone(),
    };
    
    let account_infos: Vec<AccountInfo> = vec![
        ctx.accounts.user_token_account.to_account_info(),
        ctx.accounts.aggregator_token_account.to_account_info(),
        ctx.accounts.user.to_account_info(),
        ctx.accounts.token_program.to_account_info(),
    ];
    
    invoke(&swap_ix, &account_infos)?;
    
    Ok(())
}

// Price and profitability calculations
pub fn calculate_minimum_profit_threshold(
    amount: u64,
    fee_bps: u16,
    gas_estimate: u64,
) -> u64 {
    let protocol_fee = amount * fee_bps as u64 / 10000;
    let safety_margin = amount / 1000; // 0.1% safety margin
    protocol_fee + gas_estimate + safety_margin
}

pub fn estimate_transaction_cost(
    priority_fee: u64,
    compute_units: u32,
) -> u64 {
    const BASE_FEE: u64 = 5000;
    const SIGNATURE_FEE: u64 = 5000;
    let compute_fee = (compute_units as u64 * priority_fee) / 1_000_000;
    BASE_FEE + SIGNATURE_FEE + compute_fee
}

// MEV Protection
pub fn calculate_jito_tip(
    expected_profit: u64,
    network_congestion: f64,
) -> u64 {
    const MIN_TIP: u64 = 10_000;
    const MAX_TIP_PERCENTAGE: u64 = 50; // 50% of profit
    
    let base_tip = (expected_profit * 10 / 100).max(MIN_TIP);
    let congestion_multiplier = (1.0 + network_congestion * 2.0).min(3.0);
    let adjusted_tip = (base_tip as f64 * congestion_multiplier) as u64;
    
    adjusted_tip.min(expected_profit * MAX_TIP_PERCENTAGE / 100)
}

// Optimal routing
pub fn select_optimal_route(
    available_routes: &[RouteOption],
    amount: u64,
    max_slippage_bps: u16,
) -> Option<RouteOption> {
    available_routes
        .iter()
        .filter(|route| {
            route.min_amount_out >= amount * (10000 - max_slippage_bps as u64) / 10000
        })
        .max_by_key(|route| route.expected_amount_out)
        .cloned()
}

#[derive(Clone)]
pub struct RouteOption {
    pub dex_program: Pubkey,
    pub pool_address: Pubkey,
    pub expected_amount_out: u64,
    pub min_amount_out: u64,
    pub swap_fee_bps: u16,
}

// Slippage protection
pub fn calculate_minimum_amount_out(
    amount_in: u64,
    expected_price: u64,
    max_slippage_bps: u16,
) -> Result<u64> {
    let expected_out = amount_in
        .checked_mul(expected_price)
        .ok_or(ErrorCode::MathOverflow)?
        .checked_div(1_000_000_000)
        .ok_or(ErrorCode::MathOverflow)?;
        
    let slippage_amount = expected_out
        .checked_mul(max_slippage_bps as u64)
        .ok_or(ErrorCode::MathOverflow)?
        .checked_div(10000)
        .ok_or(ErrorCode::MathOverflow)?;
        
    expected_out
        .checked_sub(slippage_amount)
        .ok_or(ErrorCode::MathOverflow)
}

// Protocol health checks
pub fn is_protocol_healthy(
    protocol: &ProtocolInfo,
    current_timestamp: i64,
) -> bool {
    if !protocol.is_active {
        return false;
    }
    
    // Check if protocol was updated recently (within 24 hours)
    let time_since_update = current_timestamp - protocol.last_update;
    if time_since_update > 86400 {
        return false;
    }
    
    // Check if protocol has reasonable fees
    if protocol.fee_bps > 100 { // More than 1%
        return false;
    }
    
    true
}

// Priority fee calculation for competitive execution
pub fn calculate_priority_fee(
    base_priority: u64,
    expected_profit: u64,
    recent_slot_times: &[u64],
) -> u64 {
    const MAX_PRIORITY_FEE: u64 = 2_000_000;
    const MIN_PRIORITY_FEE: u64 = 10_000;
    
    // Calculate average slot time
    let avg_slot_time = if recent_slot_times.len() > 0 {
        recent_slot_times.iter().sum::<u64>() / recent_slot_times.len() as u64
    } else {
        400 // Default 400ms
    };
    
    // Higher fee for faster slots (more competition)
    let competition_multiplier = if avg_slot_time < 400 {
        2.0
    } else if avg_slot_time < 600 {
        1.5
    } else {
        1.0
    };
    
    let dynamic_fee = (base_priority as f64 * competition_multiplier) as u64;
    let profit_based_fee = expected_profit / 100; // 1% of expected profit
    
    (dynamic_fee + profit_based_fee)
        .max(MIN_PRIORITY_FEE)
        .min(MAX_PRIORITY_FEE)
}

// Compute budget optimization
pub fn optimize_compute_budget(
    instruction_count: usize,
    has_complex_math: bool,
) -> u32 {
    const BASE_COMPUTE: u32 = 200_000;
    const PER_INSTRUCTION: u32 = 30_000;
    const COMPLEX_MATH_BUFFER: u32 = 100_000;
    const MAX_COMPUTE: u32 = 1_400_000;
    
    let mut compute_units = BASE_COMPUTE + (instruction_count as u32 * PER_INSTRUCTION);
    
    if has_complex_math {
        compute_units += COMPLEX_MATH_BUFFER;
    }
    
    compute_units.min(MAX_COMPUTE)
}

// Network congestion detection
pub fn estimate_network_congestion(
    recent_block_times: &[i64],
    failed_tx_count: u32,
    total_tx_count: u32,
) -> f64 {
    let avg_block_time = if recent_block_times.len() > 1 {
        let sum: i64 = recent_block_times.windows(2)
            .map(|w| w[1] - w[0])
            .sum();
        sum as f64 / (recent_block_times.len() - 1) as f64
    } else {
        400.0
    };
    
    let time_congestion = (avg_block_time - 400.0).max(0.0) / 400.0;
    let failure_rate = if total_tx_count > 0 {
        failed_tx_count as f64 / total_tx_count as f64
    } else {
        0.0
    };
    
    (time_congestion * 0.7 + failure_rate * 0.3).min(1.0)
}

// Risk management
pub fn calculate_position_size(
    available_capital: u64,
    risk_percentage: u8,
    expected_profit_bps: u16,
) -> u64 {
    let risk_amount = available_capital * risk_percentage as u64 / 100;
    let profit_factor = expected_profit_bps as u64 / 10;
    
    // Higher expected profit allows larger position
    let size_multiplier = (profit_factor as f64 / 10.0).min(3.0).max(0.5);
    (risk_amount as f64 * size_multiplier) as u64
}

// Helper to build Jito bundle
pub fn build_jito_bundle_message(
    transactions: Vec<Vec<u8>>,
    tip_amount: u64,
    tip_account: Pubkey,
) -> Vec<u8> {
    let mut bundle = vec![];
    
    // Bundle header
    bundle.extend_from_slice(&(transactions.len() as u32).to_le_bytes());
    bundle.extend_from_slice(&tip_amount.to_le_bytes());
    bundle.extend_from_slice(tip_account.as_ref());
    
    // Transactions
    for tx in transactions {
        bundle.extend_from_slice(&(tx.len() as u32).to_le_bytes());
        bundle.extend_from_slice(&tx);
    }
    
    bundle
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

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_minimum_profit_calculation() {
        let min_profit = calculate_minimum_profit_threshold(1_000_000_000, 30, 50_000);
        assert!(min_profit >= 3_050_000);
    }
    
    #[test]
    fn test_priority_fee_calculation() {
        let fee = calculate_priority_fee(50_000, 1_000_000, &[400, 380, 420]);
        assert!(fee >= 10_000 && fee <= 2_000_000);
    }
    
    #[test]
    fn test_slippage_calculation() {
        let min_out = calculate_minimum_amount_out(1_000_000, 2_000_000_000, 50).unwrap();
        assert_eq!(min_out, 1_900_000); // 2M - 5% slippage
    }
}
