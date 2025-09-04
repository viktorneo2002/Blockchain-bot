use anchor_lang::prelude::*;
use solend_program::cpi;
use solend_program::cpi::accounts::FlashBorrow as SolendFlashBorrow;
use crate::state::ProtocolInfo;
use crate::errors::ErrorCode;

pub struct SolendAdapter<'a, 'info> {
    pub protocol_info: &'a mut Account<'info, ProtocolInfo>,
    pub program: AccountInfo<'info>,
    pub accounts: &'a [AccountInfo<'info>],
}

impl<'a, 'info> SolendAdapter<'a, 'info> {
    pub fn new(
        protocol_info: &'a mut Account<'info, ProtocolInfo>,
        program: AccountInfo<'info>,
        accounts: &'a [AccountInfo<'info>],
    ) -> Self {
        Self { protocol_info, program, accounts }
    }
}

impl<'a, 'info> crate::adapters::FlashLoanAdapter<'info> for SolendAdapter<'a, 'info> {
    fn program_id(&self) -> Pubkey { self.protocol_info.program_id }

    fn validate(&self) -> Result<()> {
        require!(self.program.key() == &self.protocol_info.program_id, ErrorCode::InvalidProgramId);
        require!(self.protocol_info.is_active, ErrorCode::ProtocolInactive);
        Ok(())
    }

    fn execute_flash_loan(&self, amount: u64) -> Result<()> {
        self.validate()?;
        require!(amount > 0 && amount <= MAX_LOAN_AMOUNT, ErrorCode::InvalidParameter);

        let cpi_accounts = SolendFlashBorrow {
            reserve: self.accounts[0].clone(),
            liquidity_supply: self.accounts[1].clone(),
        };

        let cpi_ctx = CpiContext::new(self.program.clone(), cpi_accounts);
        solend_program::cpi::flash_borrow(cpi_ctx, amount).map_err(|e| e.into())
    }

    fn repay_flash_loan(&self, _amount: u64) -> Result<()> {
        Ok(())
    }
}
