use thiserror::Error;

#[derive(Error, Debug)]
pub enum FlashLoanError {
    #[error("Client error: {0}")]
    ClientError(#[from] solana_client::client_error::ClientError),
    
    #[error("Program error: {0}")]
    ProgramError(#[from] solana_sdk::program_error::ProgramError),
    
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    
    #[error("Anchor error: {0}")]
    AnchorError(#[from] anchor_lang::error::Error),
    
    #[error("{0}")]
    Custom(String),
}

impl From<&str> for FlashLoanError {
    fn from(err: &str) -> Self {
        FlashLoanError::Custom(err.to_string())
    }
}

impl From<String> for FlashLoanError {
    fn from(err: String) -> Self {
        FlashLoanError::Custom(err)
    }
}
