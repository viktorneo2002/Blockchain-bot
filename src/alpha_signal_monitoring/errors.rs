use thiserror::Error;

#[derive(Error, Debug)]
pub enum AlphaDecayError {
    #[error("RPC error: {0}")]
    Rpc(#[from] solana_client::client_error::ClientError),
    
    #[error("Redis error: {0}")]
    Redis(#[from] redis::RedisError),
    
    #[error("DEX error: {0}")]
    Dex(String),
    
    #[error("Other: {0}")]
    Other(String),
}
