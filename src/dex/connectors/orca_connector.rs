use anchor_lang::prelude::*;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::Keypair,
};
use std::{
    collections::HashMap,
    sync::Arc,
};
use tokio::sync::RwLock;
use anyhow::Result;
use crate::analytics::SpreadObservation;

// Orca Whirlpool program ID
const WHIRLPOOL_PROGRAM_ID: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";

#[derive(Clone, Debug)]
pub struct WhirlpoolState {
    pub liquidity: u128,
    pub sqrt_price_x64: u128,
    pub tick_current_index: i32,
    pub token_mint_a: Pubkey,
    pub token_mint_b: Pubkey,
    pub fee_rate: u16,
}

pub struct OrcaConnector {
    rpc_client: Arc<RpcClient>,
    pool_cache: Arc<RwLock<HashMap<Pubkey, WhirlpoolState>>>,
}

impl OrcaConnector {
    pub fn new(rpc_url: &str) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::processed(),
        ));

        Self {
            rpc_client,
            pool_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn refresh_pool_state(&self, pool_address: &Pubkey) -> Result<WhirlpoolState> {
        let account = self.rpc_client.get_account(pool_address).await?;
        let pool_state = self.deserialize_whirlpool(&account.data)?;
        
        let mut cache = self.pool_cache.write().await;
        cache.insert(*pool_address, pool_state.clone());
        
        Ok(pool_state)
    }

    pub async fn get_current_price(&self, pool_address: &Pubkey) -> Result<f64> {
        let pool_state = self.get_pool_state(pool_address).await?;
        let price = self.calculate_price_from_sqrt_price(pool_state.sqrt_price_x64);
        Ok(price)
    }

    pub async fn get_spread_observation(&self, pool_address: &Pubkey) -> Result<SpreadObservation> {
        let pool_state = self.get_pool_state(pool_address).await?;
        let price = self.calculate_price_from_sqrt_price(pool_state.sqrt_price_x64);
        
        // Calculate bid/ask prices based on fee rate
        let fee_factor = pool_state.fee_rate as f64 / 1_000_000.0;
        let bid_price = price * (1.0 - fee_factor);
        let ask_price = price * (1.0 + fee_factor);
        let mid_price = (bid_price + ask_price) / 2.0;
        let spread = ask_price - bid_price;
        
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(std::time::Duration::from_secs(0))
            .as_millis() as u64;
        
        Ok(SpreadObservation {
            timestamp_ms,
            bid_price,
            ask_price,
            mid_price,
            spread,
            volume_imbalance: 0.0, // Would need to fetch volume data separately
            liquidity_depth: pool_state.liquidity as f64,
        })
    }

    fn calculate_price_from_sqrt_price(&self, sqrt_price_x64: u128) -> f64 {
        // Convert Q64.64 fixed point to f64
        // Price = sqrt_price^2, but we need to account for the Q64.64 fixed point format
        let sqrt_price = sqrt_price_x64 as f64 / (2f64.powi(64));
        // Price = sqrt_price^2
        sqrt_price * sqrt_price
    }

    async fn get_pool_state(&self, pool_address: &Pubkey) -> Result<WhirlpoolState> {
        let cache = self.pool_cache.read().await;
        if let Some(pool) = cache.get(pool_address) {
            return Ok(pool.clone());
        }
        drop(cache);
        
        self.refresh_pool_state(pool_address).await
    }

    fn deserialize_whirlpool(&self, data: &[u8]) -> Result<WhirlpoolState> {
        if data.len() < 653 {  // 8 (discriminator) + 645 (struct size)
            anyhow::bail!("Whirlpool account data too short");
        }
        
        // Skip the first 8 bytes (Anchor discriminator)
        let data = &data[8..];
        
        // Parse the Whirlpool struct based on actual layout from Orca Whirlpool program
        // whirlpools_config: Pubkey (32 bytes) [0..32]
        // whirlpool_bump: [u8; 1] (1 byte) [32..33]
        // tick_spacing: u16 (2 bytes) [33..35]
        // fee_tier_index_seed: [u8; 2] (2 bytes) [35..37]
        // fee_rate: u16 (2 bytes) [37..39]
        // protocol_fee_rate: u16 (2 bytes) [39..41]
        // liquidity: u128 (16 bytes) [41..57]
        // sqrt_price: u128 (16 bytes) [57..73]
        // tick_current_index: i32 (4 bytes) [73..77]
        // protocol_fee_owed_a: u64 (8 bytes) [77..85]
        // protocol_fee_owed_b: u64 (8 bytes) [85..93]
        // token_mint_a: Pubkey (32 bytes) [93..125]
        // token_vault_a: Pubkey (32 bytes) [125..157]
        // fee_growth_global_a: u128 (16 bytes) [157..173]
        // token_mint_b: Pubkey (32 bytes) [173..205]
        // token_vault_b: Pubkey (32 bytes) [205..237]
        // fee_growth_global_b: u128 (16 bytes) [237..253]
        // reward_last_updated_timestamp: u64 (8 bytes) [253..261]
        // reward_infos: [WhirlpoolRewardInfo; 3] (384 bytes) [261..645]
        
        let liquidity = u128::from_le_bytes([
            data[41], data[42], data[43], data[44], data[45], data[46], data[47], data[48],
            data[49], data[50], data[51], data[52], data[53], data[54], data[55], data[56]
        ]);
        
        let sqrt_price_x64 = u128::from_le_bytes([
            data[57], data[58], data[59], data[60], data[61], data[62], data[63], data[64],
            data[65], data[66], data[67], data[68], data[69], data[70], data[71], data[72]
        ]);
        
        let tick_current_index = i32::from_le_bytes([data[73], data[74], data[75], data[76]]);
        
        let token_mint_a = Pubkey::new(&data[93..125]);
        let token_mint_b = Pubkey::new(&data[173..205]);
        
        let fee_rate = u16::from_le_bytes([data[37], data[38]]);
        
        Ok(WhirlpoolState {
            liquidity,
            sqrt_price_x64,
            tick_current_index,
            token_mint_a,
            token_mint_b,
            fee_rate,
        })
    }
}
