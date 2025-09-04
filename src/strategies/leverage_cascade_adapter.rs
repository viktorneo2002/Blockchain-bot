use async_trait::async_trait;
use solana_sdk::{instruction::Instruction, pubkey::Pubkey};
use crate::{
    errors::OptimizerError,
    optimizer::AccountHealth,
    config::ProtocolConfig,
};

#[async_trait]
pub trait ProtocolAdapter: Send + Sync {
    fn name(&self) -> &str;
    fn program_id(&self) -> Pubkey;
    
    async fn scan_accounts(&self) -> Result<Vec<Pubkey>, OptimizerError>;
    
    async fn parse_health(&self, acct_data: &[u8]) -> Option<AccountHealth>;
    
    async fn build_liquidation_ix(
        &self,
        opp: &super::LiquidationOpportunity,
    ) -> Result<Vec<Instruction>, OptimizerError>;
}

#[derive(Debug)]
pub struct LiquidationOpportunity {
    pub account: Pubkey,
    pub health: AccountHealth,
    pub protocol: String,
}
