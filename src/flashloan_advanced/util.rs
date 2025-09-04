use anchor_lang::prelude::*;
use crate::state::{RouteData, MAX_SWAP_ACCOUNTS};

pub fn execute_fixed_route<'info>(
    ctx: &Context<'_, '_, '_, 'info, ExecuteAggregatedFlashLoan<'info>>,
    route: &RouteData,
) -> Result<()> {
    // Validate route
    route.validate()?;
    let len = route.swap_accounts_len as usize;
    require!(len > 0 && len <= MAX_SWAP_ACCOUNTS, ErrorCode::RouteDataTooLarge);

    // Build metas for instruction
    let metas: Vec<AccountMeta> = route.swap_accounts[..len]
        .iter()
        .map(|k| AccountMeta::new(*k, false))
        .collect();

    let ix = Instruction {
        program_id: route.dex_program,
        accounts: metas.clone(),
        data: route.swap_data.clone(),
    };

    // Build account infos slice
    let mut infos: Vec<AccountInfo> = Vec::new();
    infos.push(ctx.accounts.user_token_account.to_account_info());
    infos.push(ctx.accounts.aggregator_token_account.to_account_info());
    infos.push(ctx.accounts.token_program.to_account_info());

    // Match each swap pubkey to remaining_accounts
    for pk in route.swap_accounts[..len].iter() {
        let found = ctx
            .remaining_accounts
            .iter()
            .find(|a| a.key() == pk)
            .ok_or(ErrorCode::InvalidAccountData)?;
        
        // Verify account owner if needed
        // require!(found.owner == &expected_owner, ErrorCode::InvalidAccountOwner);
        infos.push(found.clone());
    }

    // Slippage guard
    let min_out = route
        .expected_amount_out
        .checked_mul((10_000 - ctx.accounts.aggregator.slippage_bps as u64))
        .ok_or(ErrorCode::MathOverflow)? / 10_000;
    require!(min_out >= route.min_amount_out, ErrorCode::SlippageExceeded);

    // Execute CPI
    solana_program::program::invoke(&ix, &infos)?;

    Ok(())
}
