use anchor_lang::prelude::*;
use solend_program::cpi;
use solend_program::cpi::accounts::FlashBorrow as SolendFlashBorrow;
use crate::state::ProtocolInfo;
use crate::errors::ErrorCode;
use async_trait::async_trait;
use solana_sdk::{instruction::Instruction, pubkey::Pubkey};
use solend_program::state::Obligation;

use crate::{
    adapters::adapter::{ProtocolAdapter, LiquidationOpportunity},
    errors::OptimizerError,
    optimizer::AccountHealth,
    config::ProtocolConfig,
};

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

pub struct SolendAdapterV2 {
    program_id: Pubkey,
    config: ProtocolConfig,
}

impl SolendAdapterV2 {
    pub fn new(program_id: Pubkey, config: ProtocolConfig) -> Self {
        Self { program_id, config }
    }
}

#[async_trait]
impl ProtocolAdapter for SolendAdapterV2 {
    fn name(&self) -> &str {
        "solend"
    }

    fn program_id(&self) -> Pubkey {
        self.program_id
    }

    async fn scan_accounts(&self) -> Result<Vec<Pubkey>, OptimizerError> {
        // Implementation would scan Solend obligations
        Ok(vec![])
    }

    async fn parse_health(&self, acct_data: &[u8]) -> Option<AccountHealth> {
        let obligation = Obligation::deserialize(acct_data).ok()?;
        Some(AccountHealth {
            health_factor: obligation.health(),
            value_at_risk: obligation.unhealthy_borrow_value(),
        })
    }

    async fn build_liquidation_ix(
        &self,
        opp: &LiquidationOpportunity,
    ) -> Result<Vec<Instruction>, OptimizerError> {
        // Build Solend liquidation instructions
        Ok(vec![])
    }
}
