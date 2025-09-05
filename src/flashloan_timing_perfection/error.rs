use solana_sdk::{signature::Signature, pubkey::Pubkey};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExecError {
    #[error("simulation failed: {0}")]
    Simulation(String),
    
    #[error("rpc error: {0}")]
    Rpc(String),
    
    #[error("confirmation timeout: sig={0}")]
    ConfirmTimeout(Signature),
    
    #[error("blockhash expired")]
    BlockhashExpired,
    
    #[error("account in use: {0}")]
    AccountInUse(Pubkey),
    
    #[error("priority fee too low")]
    FeeTooLow,
    
    #[error("profit guard tripped")]
    ProfitGuard,
    
    #[error("conflict batch error: {0}")]
    Conflict(String),
    
    #[error("invalid account data: {0}")]
    InvalidAccountData(String),
    
    #[error("insufficient liquidity")]
    InsufficientLiquidity,
    
    #[error("invalid pool state: {0}")]
    InvalidPoolState(String),
    
    #[error("math overflow")]
    MathOverflow,
    
    #[error("invalid configuration: {0}")]
    ConfigError(String),
    
    #[error("rate limited: {0}")]
    RateLimited(String),
    
    #[error("other: {0}")]
    Other(String),
}

impl ExecError {
    /// Returns true if the error is retryable
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::BlockhashExpired
            | Self::AccountInUse(_)
            | Self::FeeTooLow
            | Self::Rpc(_)
            | Self::RateLimited(_)
            | Self::Conflict(_) => true,
            _ => false,
        }
    }
}

/// Type alias for Result using ExecError
pub type Result<T> = std::result::Result<T, ExecError>;
