use solana_client::rpc_client::RpcClient;
use solana_sdk::{pubkey::Pubkey, program_pack::Pack};
solana_sdk::declare_id!("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
use spl_token::state::Mint;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum TokenError {
    #[error("RPC error: {0}")]
    RpcError(#[from] solana_client::client_error::ClientError),
    
    #[error("Invalid account data")]
    InvalidAccountData,
    
    #[error("Token mint not found")]
    MintNotFound,
}

/// Fetches token decimals from on-chain data
pub async fn get_token_decimals(
    client: &RpcClient,
    mint: &Pubkey,
) -> Result<u8, TokenError> {
    let account = client.get_account(mint).await?;
    
    if account.owner != spl_token::id() {
        return Err(TokenError::MintNotFound);
    }
    
    let mint_account = Mint::unpack(&account.data)
        .map_err(|_| TokenError::InvalidAccountData)?;
    
    Ok(mint_account.decimals)
}

/// Converts USD amount to token amount using oracle price and token decimals
pub fn usd_to_token_amount(
    usd_amount: f64,
    token_price_usd: f64,
    decimals: u8,
) -> Result<u64, std::num::TryFromIntError> {
    let token_amount = (usd_amount / token_price_usd) * 10f64.powi(decimals as i32);
    token_amount.round() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signer::keypair::Keypair;
    use spl_token::state::Mint;
    
    #[test]
    fn test_usd_to_token_amount() {
        // Test with 6 decimals (USDC-like)
        let amount = usd_to_token_amount(100.0, 1.0, 6).unwrap();
        assert_eq!(amount, 100_000_000); // 100 * 10^6
        
        // Test with 9 decimals (SOL-like)
        let amount = usd_to_token_amount(100.0, 100.0, 9).unwrap();
        assert_eq!(amount, 1_000_000_000); // 1 * 10^9
    }
}
