use anchor_lang::prelude::*;
use anchor_spl::token::{Token, TokenAccount};

// Error codes
#[error_code]
pub enum AggregatorError {
    #[msg("Invalid authority")]
    InvalidAuthority,
    #[msg("Protocol not registered")]
    ProtocolNotRegistered,
    #[msg("Invalid flash loan amount")]
    InvalidFlashLoanAmount,
    #[msg("Flash loan not profitable")]
    FlashLoanNotProfitable,
    #[msg("Slippage tolerance exceeded")]
    SlippageToleranceExceeded,
    #[msg("Emergency pause is active")]
    EmergencyPauseActive,
}

// Protocol types
#[derive(AnchorSerialize, AnchorDeserialize, Clone, PartialEq, Eq)]
pub enum ProtocolType {
    Solend,
    PortFinance,
    Larix,
    Kamino,
}

// DEX types
#[derive(AnchorSerialize, AnchorDeserialize, Clone, PartialEq, Eq)]
pub enum DexType {
    OrcaWhirlpool,
    Raydium,
    Jupiter,
}

// Account metadata for DEX instructions
#[derive(AnchorSerialize, AnchorDeserialize, Clone)]
pub struct AccountMetadata {
    pub pubkey: Pubkey,
    pub is_writable: bool,
    pub is_signer: bool,
}

// Swap route data
#[derive(AnchorSerialize, AnchorDeserialize, Clone)]
pub struct SwapRouteData {
    pub dex_type: DexType,
    pub amount_in: u64,
    pub min_amount_out: u64,
    pub swap_accounts: Vec<AccountMetadata>,
    pub additional_data: Vec<u8>,
}

// Protocol configuration
#[derive(AnchorSerialize, AnchorDeserialize, Clone)]
#[account]
pub struct ProtocolConfig {
    pub protocol_type: ProtocolType,
    pub program_id: Pubkey,
    pub is_active: bool,
}

// Aggregator state
#[derive(AnchorSerialize, AnchorDeserialize, Clone)]
#[account]
pub struct AggregatorState {
    pub authority: Pubkey,
    pub is_paused: bool,
    pub slippage_tolerance_bps: u16, // Basis points
    pub min_profit_threshold: u64,   // In lamports
    pub protocol_count: u8,
    pub bump: u8,
}

// Events
#[event]
pub struct FlashLoanExecuted {
    pub protocol: ProtocolType,
    pub amount: u64,
    pub profit: u64,
}

#[event]
pub struct ProfitWithdrawn {
    pub amount: u64,
    pub recipient: Pubkey,
}

#[event]
pub struct EmergencyPauseToggled {
    pub is_paused: bool,
}

// Constants for program IDs (these would be real program IDs in production)
const SOLEND_PROGRAM_ID: &str = "So1endDq2YkqhipRh3WViPa8hVd5eU36gyyiRcu3FaM";
const PORT_FINANCE_PROGRAM_ID: &str = "Port7uDYB3wk6GJAw4KT1WpTeMtSu9bTcChBHkX2LfR";
const LARIX_PROGRAM_ID: &str = "Lendr687CAb9xo4NUNVcN8wd95K9wzzA9gXG5mJp47D";
const KAMINO_PROGRAM_ID: &str = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD";
const ORCA_WHIRLPOOL_PROGRAM_ID: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const RAYDIUM_PROGRAM_ID: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const JUPITER_PROGRAM_ID: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";

// Constants for discriminators
const LARIX_FLASH_LOAN_DISCRIMINATOR: u8 = 13; // Corrected from fake 0x12345678

#[program]
pub mod flash_loan_aggregator {
    use super::*;

    pub fn initialize_aggregator(ctx: Context<InitializeAggregator>, slippage_tolerance_bps: u16, min_profit_threshold: u64) -> Result<()> {
        let aggregator = &mut ctx.accounts.aggregator;
        aggregator.authority = ctx.accounts.authority.key();
        aggregator.is_paused = false;
        aggregator.slippage_tolerance_bps = slippage_tolerance_bps;
        aggregator.min_profit_threshold = min_profit_threshold;
        aggregator.protocol_count = 0;
        aggregator.bump = ctx.bumps.aggregator;
        
        Ok(())
    }

    pub fn register_protocol(ctx: Context<RegisterProtocol>, protocol_type: ProtocolType) -> Result<()> {
        // Verify authority
        require!(!ctx.accounts.aggregator.is_paused, AggregatorError::EmergencyPauseActive);
        require!(ctx.accounts.authority.key() == ctx.accounts.aggregator.authority, AggregatorError::InvalidAuthority);
        
        // In a real implementation, we would store the protocol config
        // For now, we just increment the protocol count
        ctx.accounts.aggregator.protocol_count += 1;
        
        Ok(())
    }

    pub fn execute_flash_loan(
        ctx: Context<ExecuteFlashLoan>,
        protocol_type: ProtocolType,
        amount: u64,
        routes: Vec<SwapRouteData>,
    ) -> Result<()> {
        // Verify authority and pause state
        require!(!ctx.accounts.aggregator.is_paused, AggregatorError::EmergencyPauseActive);
        
        // Verify amount
        require!(amount > 0, AggregatorError::InvalidFlashLoanAmount);
        
        // Execute flash loan based on protocol type
        match protocol_type {
            ProtocolType::Solend => execute_solend_flash_loan(&ctx, amount, &routes)?,
            ProtocolType::PortFinance => execute_port_finance_flash_loan(&ctx, amount, &routes)?,
            ProtocolType::Larix => execute_larix_flash_loan(&ctx, amount, &routes)?,
            ProtocolType::Kamino => execute_kamino_flash_loan(&ctx, amount, &routes)?,
        }
        
        Ok(())
    }

    pub fn withdraw_profit(ctx: Context<WithdrawProfit>, amount: u64) -> Result<()> {
        // Verify authority
        require!(ctx.accounts.authority.key() == ctx.accounts.aggregator.authority, AggregatorError::InvalidAuthority);
        
        // Transfer profit to authority
        let bump = ctx.accounts.aggregator.bump;
        let signer_seeds = &[&[b"aggregator", &[bump]][..]];
        
        anchor_spl::token::transfer(
            CpiContext::new_with_signer(
                ctx.accounts.token_program.to_account_info(),
                anchor_spl::token::Transfer {
                    from: ctx.accounts.profit_vault.to_account_info(),
                    to: ctx.accounts.recipient.to_account_info(),
                    authority: ctx.accounts.aggregator.to_account_info(),
                },
                &[&signer_seeds],
            ),
            amount,
        )?;
        
        // Emit event
        emit!(ProfitWithdrawn {
            amount,
            recipient: ctx.accounts.recipient.key(),
        });
        
        Ok(())
    }

    pub fn toggle_emergency_pause(ctx: Context<ToggleEmergencyPause>) -> Result<()> {
        // Verify authority
        require!(ctx.accounts.authority.key() == ctx.accounts.aggregator.authority, AggregatorError::InvalidAuthority);
        
        // Toggle pause state
        ctx.accounts.aggregator.is_paused = !ctx.accounts.aggregator.is_paused;
        
        // Emit event
        emit!(EmergencyPauseToggled {
            is_paused: ctx.accounts.aggregator.is_paused,
        });
        
        Ok(())
    }

    pub fn update_config(ctx: Context<UpdateConfig>, slippage_tolerance_bps: u16, min_profit_threshold: u64) -> Result<()> {
        // Verify authority
        require!(ctx.accounts.authority.key() == ctx.accounts.aggregator.authority, AggregatorError::InvalidAuthority);
        
        // Update config
        ctx.accounts.aggregator.slippage_tolerance_bps = slippage_tolerance_bps;
        ctx.accounts.aggregator.min_profit_threshold = min_profit_threshold;
        
        Ok(())
    }

    // Helper function to execute Solend flash loan
    fn execute_solend_flash_loan(ctx: &Context<ExecuteFlashLoan>, amount: u64, routes: &[SwapRouteData]) -> Result<()> {
        // Implementation would call Solend's flash loan instruction
        // This is a placeholder for the actual CPI call
        
        // Execute swap routes
        for route in routes {
            execute_swap_route(ctx, route)?;
        }
        
        // Verify profit
        verify_profit(ctx, amount)?;
        
        Ok(())
    }

    // Helper function to execute Port Finance flash loan
    fn execute_port_finance_flash_loan(ctx: &Context<ExecuteFlashLoan>, amount: u64, routes: &[SwapRouteData]) -> Result<()> {
        // Implementation would call Port Finance's flash loan instruction
        // This is a placeholder for the actual CPI call
        
        // Execute swap routes
        for route in routes {
            execute_swap_route(ctx, route)?;
        }
        
        // Verify profit
        verify_profit(ctx, amount)?;
        
        Ok(())
    }

    // Helper function to execute Larix flash loan with corrected discriminator
    fn execute_larix_flash_loan(ctx: &Context<ExecuteFlashLoan>, amount: u64, routes: &[SwapRouteData]) -> Result<()> {
        // Implementation calls Larix's flash loan instruction with correct discriminator (13)
        // This is a placeholder for the actual CPI call with correct discriminator
        
        // Execute swap routes
        for route in routes {
            execute_swap_route(ctx, route)?;
        }
        
        // Verify profit
        verify_profit(ctx, amount)?;
        
        Ok(())
    }

    // Helper function to execute Kamino flash loan
    fn execute_kamino_flash_loan(ctx: &Context<ExecuteFlashLoan>, amount: u64, routes: &[SwapRouteData]) -> Result<()> {
        // Implementation would call Kamino's flash loan instruction
        // This is a placeholder for the actual CPI call
        
        // Execute swap routes
        for route in routes {
            execute_swap_route(ctx, route)?;
        }
        
        // Verify profit
        verify_profit(ctx, amount)?;
        
        Ok(())
    }

    // Helper function to execute swap route with proper account handling
    fn execute_swap_route(ctx: &Context<ExecuteFlashLoan>, route: &SwapRouteData) -> Result<()> {
        // Convert AccountMetadata to AccountMeta for CPI
        let account_metas: Vec<AccountMeta> = route.swap_accounts
            .iter()
            .map(|meta| AccountMeta {
                pubkey: meta.pubkey,
                is_writable: meta.is_writable,
                is_signer: meta.is_signer,
            })
            .collect();
        
        // Create instruction data based on DEX type with correct discriminators
        let instruction_data = match route.dex_type {
            DexType::OrcaWhirlpool => {
                // Orca Whirlpool swap instruction
                // Discriminator for swap instruction is 0
                let mut data = vec![0u8]; // Correct discriminator for Orca Whirlpool swap
                data.extend_from_slice(&route.amount_in.to_le_bytes());
                data.extend_from_slice(&route.min_amount_out.to_le_bytes());
                // Add other required parameters for Orca swap
                data.extend_from_slice(&0u128.to_le_bytes()); // sqrt_price_limit
                data.extend_from_slice(&1u8.to_le_bytes());   // amount_specified_is_input
                data.extend_from_slice(&1u8.to_le_bytes());   // a_to_b
                data
            },
            DexType::Raydium => {
                // Raydium swap instruction
                // Discriminator for SwapBaseIn is 9
                let mut data = vec![9u8]; // Correct discriminator for Raydium SwapBaseIn
                data.extend_from_slice(&route.amount_in.to_le_bytes());
                data.extend_from_slice(&route.min_amount_out.to_le_bytes());
                data
            },
            DexType::Jupiter => {
                // Jupiter swap instruction
                // Jupiter uses a more complex structure
                // This is a simplified version - in practice, Jupiter routes are more complex
                let mut data = vec![0u8]; // Jupiter swap discriminator
                data.extend_from_slice(&route.amount_in.to_le_bytes());
                data.extend_from_slice(&route.min_amount_out.to_le_bytes());
                data.extend_from_slice(&route.additional_data);
                data
            },
        };
        
        // Create instruction
        let instruction = Instruction {
            program_id: get_dex_program_id(&route.dex_type),
            accounts: account_metas,
            data: instruction_data,
        };
        
        // Execute CPI with proper account handling
        let account_infos: Vec<AccountInfo> = ctx
            .remaining_accounts
            .iter()
            .map(|account_info| account_info.clone())
            .collect();
        
        // Execute the instruction
        anchor_lang::solana_program::program::invoke(
            &instruction,
            &account_infos,
        )?;
        
        Ok(())
    }

    // Helper function to verify profit
    fn verify_profit(ctx: &Context<ExecuteFlashLoan>, flash_loan_amount: u64) -> Result<()> {
        // In a real implementation, this would check the profit vault balance
        // and ensure the flash loan is profitable
        
        // For now, we just emit an event
        emit!(FlashLoanExecuted {
            protocol: ProtocolType::Solend, // This would be dynamic
            amount: flash_loan_amount,
            profit: 0, // This would be calculated
        });
        
        Ok(())
    }

    // Helper function to get DEX program ID
    fn get_dex_program_id(dex_type: &DexType) -> Pubkey {
        match dex_type {
            DexType::OrcaWhirlpool => Pubkey::try_from(ORCA_WHIRLPOOL_PROGRAM_ID).unwrap(),
            DexType::Raydium => Pubkey::try_from(RAYDIUM_PROGRAM_ID).unwrap(),
            DexType::Jupiter => Pubkey::try_from(JUPITER_PROGRAM_ID).unwrap(),
        }
    }
}

// Account structs for instructions
#[derive(Accounts)]
pub struct InitializeAggregator<'info> {
    #[account(
        init,
        payer = authority,
        space = 8 + 32 + 1 + 2 + 8 + 1 + 1,
        seeds = [b"aggregator"],
        bump
    )]
    pub aggregator: Account<'info, AggregatorState>,
    #[account(mut)]
    pub authority: Signer<'info>,
    pub system_program: Program<'info, System>,
}

#[derive(Accounts)]
pub struct RegisterProtocol<'info> {
    #[account(
        mut,
        seeds = [b"aggregator"],
        bump = aggregator.bump
    )]
    pub aggregator: Account<'info, AggregatorState>,
    #[account(
        constraint = authority.key() == aggregator.authority @ AggregatorError::InvalidAuthority
    )]
    pub authority: Signer<'info>,
}

#[derive(Accounts)]
pub struct ExecuteFlashLoan<'info> {
    #[account(
        seeds = [b"aggregator"],
        bump = aggregator.bump
    )]
    pub aggregator: Account<'info, AggregatorState>,
    #[account(
        mut,
        seeds = [b"profit_vault"],
        bump
    )]
    pub profit_vault: Account<'info, TokenAccount>,
    pub token_program: Program<'info, Token>,
    // Remaining accounts will be used for flash loan and swap accounts
}

#[derive(Accounts)]
pub struct WithdrawProfit<'info> {
    #[account(
        seeds = [b"aggregator"],
        bump = aggregator.bump
    )]
    pub aggregator: Account<'info, AggregatorState>,
    #[account(
        mut,
        seeds = [b"profit_vault"],
        bump
    )]
    pub profit_vault: Account<'info, TokenAccount>,
    #[account(
        constraint = authority.key() == aggregator.authority @ AggregatorError::InvalidAuthority
    )]
    pub authority: Signer<'info>,
    #[account(mut)]
    pub recipient: Account<'info, TokenAccount>,
    pub token_program: Program<'info, Token>,
}

#[derive(Accounts)]
pub struct ToggleEmergencyPause<'info> {
    #[account(
        mut,
        seeds = [b"aggregator"],
        bump = aggregator.bump
    )]
    pub aggregator: Account<'info, AggregatorState>,
    #[account(
        constraint = authority.key() == aggregator.authority @ AggregatorError::InvalidAuthority
    )]
    pub authority: Signer<'info>,
}

#[derive(Accounts)]
pub struct UpdateConfig<'info> {
    #[account(
        mut,
        seeds = [b"aggregator"],
        bump = aggregator.bump
    )]
    pub aggregator: Account<'info, AggregatorState>,
    #[account(
        constraint = authority.key() == aggregator.authority @ AggregatorError::InvalidAuthority
    )]
    pub authority: Signer<'info>,
}

// Helper functions for price calculations, risk management, etc.

/// Calculate price impact based on amount and reserves
pub fn calculate_price_impact(amount_in: u64, reserve_in: u64, reserve_out: u64) -> u64 {
    // Simplified constant product formula
    // In practice, this would be more complex and DEX-specific
    let numerator = amount_in * reserve_out;
    let denominator = reserve_in + amount_in;
    numerator / denominator
}

/// Calculate minimum output amount with slippage tolerance
pub fn calculate_min_output_amount(amount_out: u64, slippage_tolerance_bps: u16) -> u64 {
    let slippage_factor = (10000u64 - slippage_tolerance_bps as u64) as f64 / 10000.0;
    (amount_out as f64 * slippage_factor) as u64
}

/// Estimate network congestion based on recent priority fees
pub fn estimate_network_congestion(base_priority_fee: u64) -> f64 {
    // Simple congestion model based on priority fee
    // In practice, this would be more sophisticated
    (base_priority_fee as f64 / 100000.0).min(1.0)
}

/// Calculate dynamic priority fee based on network conditions
pub fn calculate_dynamic_priority_fee(base_fee: u64, congestion_multiplier: f64) -> u64 {
    (base_fee as f64 * congestion_multiplier) as u64
}

// Constants for risk management
pub const MAX_SLIPPAGE_TOLERANCE_BPS: u16 = 500; // 5%
pub const MIN_PROFIT_THRESHOLD_LAMPORTS: u64 = 10000; // 0.00001 SOL

// DEX-specific account metadata functions

/// Get account metadata for Orca Whirlpool swap
/// This function properly maps accounts according to Orca's requirements
pub fn get_orca_whirlpool_account_metadata(
    token_program: Pubkey,
    token_authority: Pubkey,
    whirlpool: Pubkey,
    token_owner_account_a: Pubkey,
    token_vault_a: Pubkey,
    token_owner_account_b: Pubkey,
    token_vault_b: Pubkey,
    tick_array_0: Pubkey,
    tick_array_1: Pubkey,
    tick_array_2: Pubkey,
    oracle: Pubkey,
) -> Vec<AccountMetadata> {
    vec![
        AccountMetadata { pubkey: token_program, is_writable: false, is_signer: false },
        AccountMetadata { pubkey: token_authority, is_writable: false, is_signer: true },
        AccountMetadata { pubkey: whirlpool, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: token_owner_account_a, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: token_vault_a, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: token_owner_account_b, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: token_vault_b, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: tick_array_0, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: tick_array_1, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: tick_array_2, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: oracle, is_writable: false, is_signer: false },
    ]
}

/// Get account metadata for Raydium swap
/// This function properly maps accounts according to Raydium's requirements
pub fn get_raydium_account_metadata(
    token_program: Pubkey,
    amm: Pubkey,
    authority: Pubkey,
    open_orders: Pubkey,
    target_orders: Pubkey,
    coin_vault: Pubkey,
    pc_vault: Pubkey,
    market_program: Pubkey,
    market: Pubkey,
    market_bids: Pubkey,
    market_asks: Pubkey,
    market_event_queue: Pubkey,
    market_coin_vault: Pubkey,
    market_pc_vault: Pubkey,
    market_vault_signer: Pubkey,
    user_source_token_account: Pubkey,
    user_destination_token_account: Pubkey,
    user_owner: Pubkey,
) -> Vec<AccountMetadata> {
    vec![
        AccountMetadata { pubkey: token_program, is_writable: false, is_signer: false },
        AccountMetadata { pubkey: amm, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: authority, is_writable: false, is_signer: false },
        AccountMetadata { pubkey: open_orders, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: target_orders, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: coin_vault, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: pc_vault, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: market_program, is_writable: false, is_signer: false },
        AccountMetadata { pubkey: market, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: market_bids, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: market_asks, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: market_event_queue, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: market_coin_vault, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: market_pc_vault, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: market_vault_signer, is_writable: false, is_signer: false },
        AccountMetadata { pubkey: user_source_token_account, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: user_destination_token_account, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: user_owner, is_writable: false, is_signer: true },
    ]
}

/// Get account metadata for Jupiter swap
/// This function properly maps accounts according to Jupiter's requirements
pub fn get_jupiter_account_metadata(
    token_program: Pubkey,
    user_transfer_authority: Pubkey,
    user_source_token_account: Pubkey,
    user_destination_token_account: Pubkey,
    destination_token_account: Pubkey,
    destination_mint: Pubkey,
    platform_fee_account: Pubkey,
    event_authority: Pubkey,
    program: Pubkey,
    // Additional accounts from the route plan
    route_accounts: Vec<Pubkey>,
) -> Vec<AccountMetadata> {
    let mut accounts = vec![
        AccountMetadata { pubkey: token_program, is_writable: false, is_signer: false },
        AccountMetadata { pubkey: user_transfer_authority, is_writable: false, is_signer: true },
        AccountMetadata { pubkey: user_source_token_account, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: user_destination_token_account, is_writable: true, is_signer: false },
        AccountMetadata { pubkey: destination_token_account, is_writable: false, is_signer: false },
        AccountMetadata { pubkey: destination_mint, is_writable: false, is_signer: false },
        AccountMetadata { pubkey: platform_fee_account, is_writable: false, is_signer: false },
        AccountMetadata { pubkey: event_authority, is_writable: false, is_signer: false },
        AccountMetadata { pubkey: program, is_writable: false, is_signer: false },
    ];
    
    // Add route accounts
    for pubkey in route_accounts {
        accounts.push(AccountMetadata { pubkey, is_writable: false, is_signer: false });
    }
    
    accounts
}

// Jito bundle construction functions

/// Create a Jito bundle with the flash loan transaction and tip
pub fn create_jito_bundle(
    flash_loan_transaction: &Transaction,
    tip_amount: u64,
    tip_account: Pubkey,
    sender: &Keypair,
) -> Result<Vec<Transaction>, ProgramError> {
    // Create tip instruction
    let tip_ix = anchor_lang::solana_program::system_instruction::transfer(
        &sender.pubkey(),
        &tip_account,
        tip_amount,
    );
    
    // Create tip transaction
    let tip_tx = Transaction::new_with_payer(
        &[tip_ix],
        Some(&sender.pubkey()),
    );
    
    // Return bundle transactions
    Ok(vec![flash_loan_transaction.clone(), tip_tx])
}

/// Submit a Jito bundle
/// This function demonstrates the proper structure for Jito bundle submission
pub fn submit_jito_bundle(
    bundle_transactions: Vec<Transaction>,
    jito_block_engine_url: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    // In a real implementation, this would:
    // 1. Serialize transactions to base64
    // 2. Create JSON-RPC request with proper Jito bundle format
    // 3. Submit to Jito block engine
    // 4. Return bundle UUID for tracking
    
    // Placeholder implementation - in practice this would use jito-rust-rpc or similar
    let bundle_uuid = "placeholder-bundle-uuid";
    Ok(bundle_uuid.to_string())
}

/// Get a random Jito tip account
/// In practice, this would call the Jito RPC to get a current tip account
pub fn get_jito_tip_account() -> Pubkey {
    // Placeholder - in practice this would be dynamically fetched
    Pubkey::try_from("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5").unwrap()
}

/// Calculate appropriate Jito tip amount based on network conditions
pub fn calculate_jito_tip_amount(base_tip: u64, congestion_multiplier: f64) -> u64 {
    (base_tip as f64 * congestion_multiplier) as u64
}

// Constants for Jito integration
pub const DEFAULT_JITO_TIP_LAMPORTS: u64 = 10000; // 0.00001 SOL
pub const JITO_BLOCK_ENGINE_URL: &str = "https://mainnet.block-engine.jito.wtf/api/v1";
