pub mod MultiProtocolFlashLoanAggregator;
pub mod MultiProtocolFlashLoanSanitizer;
pub mod mev_protection_tests;
use anchor_lang::prelude::*;
use crate::state::ProtocolInfo;

/// Adapter trait each protocol implements
pub trait FlashLoanAdapter<'info> {
    fn program_id(&self) -> Pubkey;
    fn validate_inputs(&self) -> Result<()>;
    fn execute_flash_loan(&self, amount: u64) -> Result<()>;
    fn repay_flash_loan(&self, amount: u64) -> Result<()>;
}

#[error_code]
pub enum AdapterError {
    #[msg("Invalid program ID")]
    InvalidProgramId,
    #[msg("Protocol inactive")]
    ProtocolInactive,
    #[msg("Invalid account data")]
    InvalidAccountData,
}
