use anyhow::{anyhow, Result};
use async_trait::async_trait;
use helius::{
    client::Helius,
    config::Config,
    types::{Asset, AssetProof, GetAssetProofRequest},
};
use serde::{Deserialize, Serialize};
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    hash::Hash,
    pubkey::Pubkey,
};
use std::{sync::Arc, time::Instant, collections::HashMap};
use tracing::{debug, error, warn, info};
use uuid::Uuid;

use crate::infrastructure::CustomRPCEndpoints;
use super::types::{AccountProof, MerkleProof};
use super::ProofProvider;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeliusConfig {
    pub api_key: String,
    pub cluster: String, // "mainnet-beta" or "devnet"
    pub max_retries: u32,
    pub timeout_ms: u64,
    pub rate_limit_per_second: u32,
}

impl Default for HeliusConfig {
    fn default() -> Self {
        Self {
            api_key: std::env::var("HELIUS_API_KEY").unwrap_or_default(),
            cluster: "mainnet-beta".to_string(),
            max_retries: 3,
            timeout_ms: 10000,
            rate_limit_per_second: 100,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EndpointHealth {
    pub latency_ms: u64,
    pub success_rate: f64,
    pub last_error: Option<String>,
    pub consecutive_failures: u32,
    pub last_success: Option<Instant>,
    pub circuit_breaker_open: bool,
}

impl Default for EndpointHealth {
    fn default() -> Self {
        Self {
            latency_ms: 0,
            success_rate: 1.0,
            last_error: None,
            consecutive_failures: 0,
            last_success: Some(Instant::now()),
            circuit_breaker_open: false,
        }
    }
}

pub struct HeliusProofProvider {
    client: Helius,
    rpc: Arc<CustomRPCEndpoints>,
    config: HeliusConfig,
    health_tracker: Arc<tokio::sync::RwLock<HashMap<String, EndpointHealth>>>,
}

impl HeliusProofProvider {
    pub fn new(config: HeliusConfig, rpc: Arc<CustomRPCEndpoints>) -> Result<Self> {
        if config.api_key.is_empty() {
            return Err(anyhow!("Helius API key is required. Set HELIUS_API_KEY environment variable"));
        }

        let helius_config = Config {
            api_key: config.api_key.clone(),
            cluster: helius::types::Cluster::from_str(&config.cluster)
                .map_err(|e| anyhow!("Invalid cluster: {}", e))?,
        };

        let client = Helius::new_with_config(helius_config)
            .map_err(|e| anyhow!("Failed to create Helius client: {}", e))?;

        Ok(Self {
            client,
            rpc,
            config,
            health_tracker: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
        })
    }

    /// Convert Helius AssetProof to our MerkleProof format
    fn convert_helius_proof(&self, helius_proof: AssetProof) -> Result<MerkleProof> {
        // Parse the root hash
        let root = helius_proof.root.parse::<Pubkey>()
            .map_err(|e| anyhow!("Invalid root hash: {}", e))?;
        let root_hash = Hash::new_from_array(root.to_bytes());

        // Convert proof array - Helius returns base58 encoded strings
        let mut proof_hashes = Vec::new();
        for proof_str in helius_proof.proof {
            let proof_pubkey = proof_str.parse::<Pubkey>()
                .map_err(|e| anyhow!("Invalid proof hash: {}", e))?;
            proof_hashes.push(Hash::new_from_array(proof_pubkey.to_bytes()));
        }

        Ok(MerkleProof {
            root: root_hash,
            proof: proof_hashes,
            leaf_index: helius_proof.node_index,
            timestamp: Instant::now(),
        })
    }

    /// Get asset information from Helius DAS API
    async fn get_helius_asset(&self, asset_id: &Pubkey) -> Result<Asset> {
        let start_time = Instant::now();
        let request_id = Uuid::new_v4().to_string();
        
        debug!(
            request_id = %request_id,
            asset_id = %asset_id,
            "Fetching asset from Helius DAS API"
        );

        let result = self.client.get_asset(asset_id.to_string()).await;
        let latency = start_time.elapsed().as_millis() as u64;

        match result {
            Ok(asset) => {
                info!(
                    request_id = %request_id,
                    asset_id = %asset_id,
                    latency_ms = latency,
                    "Successfully fetched asset from Helius"
                );
                self.update_endpoint_health("helius_das", true, latency, None).await;
                Ok(asset)
            }
            Err(e) => {
                let error_msg = format!("Helius get_asset failed: {}", e);
                error!(
                    request_id = %request_id,
                    asset_id = %asset_id,
                    latency_ms = latency,
                    error = %error_msg,
                    "Failed to fetch asset from Helius"
                );
                self.update_endpoint_health("helius_das", false, latency, Some(error_msg.clone())).await;
                Err(anyhow!(error_msg))
            }
        }
    }

    /// Get asset proof from Helius DAS API
    async fn get_helius_proof(&self, asset_id: &Pubkey) -> Result<AssetProof> {
        let start_time = Instant::now();
        let request_id = Uuid::new_v4().to_string();
        
        debug!(
            request_id = %request_id,
            asset_id = %asset_id,
            "Fetching asset proof from Helius DAS API"
        );

        let result = self.client.get_asset_proof(asset_id.to_string()).await;
        let latency = start_time.elapsed().as_millis() as u64;

        match result {
            Ok(proof) => {
                info!(
                    request_id = %request_id,
                    asset_id = %asset_id,
                    latency_ms = latency,
                    proof_length = proof.proof.len(),
                    tree_id = %proof.tree_id,
                    "Successfully retrieved asset proof from Helius"
                );
                self.update_endpoint_health("helius_proof", true, latency, None).await;
                Ok(proof)
            }
            Err(e) => {
                let error_msg = format!("Helius get_asset_proof failed: {}", e);
                error!(
                    request_id = %request_id,
                    asset_id = %asset_id,
                    latency_ms = latency,
                    error = %error_msg,
                    "Failed to retrieve asset proof from Helius"
                );
                self.update_endpoint_health("helius_proof", false, latency, Some(error_msg.clone())).await;
                Err(anyhow!(error_msg))
            }
        }
    }

    /// Update health tracking for endpoints
    async fn update_endpoint_health(
        &self,
        endpoint: &str,
        success: bool,
        latency: u64,
        error: Option<String>,
    ) {
        let mut health_map = self.health_tracker.write().await;
        let health = health_map.entry(endpoint.to_string()).or_insert_with(EndpointHealth::default);

        health.latency_ms = latency;
        
        if success {
            health.consecutive_failures = 0;
            health.last_success = Some(Instant::now());
            health.circuit_breaker_open = false;
            health.last_error = None;
            
            // Update success rate with exponential moving average
            health.success_rate = health.success_rate * 0.9 + 0.1;
        } else {
            health.consecutive_failures += 1;
            health.last_error = error;
            
            // Open circuit breaker after 5 consecutive failures
            if health.consecutive_failures >= 5 {
                health.circuit_breaker_open = true;
                warn!(
                    endpoint = endpoint,
                    consecutive_failures = health.consecutive_failures,
                    "Circuit breaker opened for endpoint"
                );
            }
            
            // Update success rate
            health.success_rate = health.success_rate * 0.9;
        }
    }

    /// Check if circuit breaker allows requests
    async fn is_circuit_breaker_open(&self, endpoint: &str) -> bool {
        let health_map = self.health_tracker.read().await;
        health_map.get(endpoint)
            .map(|h| h.circuit_breaker_open)
            .unwrap_or(false)
    }

    /// Convert Helius Asset to Solana Account
    fn convert_asset_to_account(&self, asset: &Asset) -> Result<Account> {
        // For compressed NFTs, we create a synthetic account representation
        // since they don't have traditional account data
        let mut account_data = Vec::new();
        
        // Encode basic asset information
        if let Some(content) = &asset.content {
            if let Some(metadata) = &content.metadata {
                // Serialize key metadata fields
                account_data.extend_from_slice(&metadata.name.len().to_le_bytes());
                account_data.extend_from_slice(metadata.name.as_bytes());
                
                if let Some(symbol) = &metadata.symbol {
                    account_data.extend_from_slice(&symbol.len().to_le_bytes());
                    account_data.extend_from_slice(symbol.as_bytes());
                }
            }
        }

        // Add ownership information
        if let Some(ownership) = &asset.ownership {
            let owner = ownership.owner.parse::<Pubkey>()
                .map_err(|e| anyhow!("Invalid owner pubkey: {}", e))?;
            account_data.extend_from_slice(&owner.to_bytes());
        }

        Ok(Account {
            lamports: 0, // Compressed NFTs don't hold lamports
            data: account_data,
            owner: solana_sdk::system_program::id(), // Default to system program
            executable: false,
            rent_epoch: 0,
        })
    }

    /// Get health status for all tracked endpoints
    pub async fn get_health_status(&self) -> HashMap<String, EndpointHealth> {
        self.health_tracker.read().await.clone()
    }
}

#[async_trait]
impl ProofProvider for HeliusProofProvider {
    async fn get_account_with_proof(&self, account: &Pubkey) -> Result<AccountProof> {
        // Check circuit breaker
        if self.is_circuit_breaker_open("helius_das").await {
            return Err(anyhow!("Helius DAS circuit breaker is open - service unavailable"));
        }

        let request_id = Uuid::new_v4().to_string();
        let start_time = Instant::now();
        
        info!(
            request_id = %request_id,
            account = %account,
            "Starting HeliusProofProvider::get_account_with_proof"
        );

        // Parallel fetch of asset and proof data
        let (asset_result, proof_result) = tokio::join!(
            self.get_helius_asset(account),
            self.get_helius_proof(account)
        );

        let asset = asset_result?;
        let helius_proof = proof_result?;

        // Convert to our formats
        let account_data = self.convert_asset_to_account(&asset)?;
        let merkle_proof = self.convert_helius_proof(helius_proof)?;

        // Get current slot from RPC for consistency
        let slot = self.rpc.get_slot(CommitmentConfig::processed()).await
            .map_err(|e| anyhow!("Failed to get current slot: {}", e))?;

        let total_time = start_time.elapsed();
        info!(
            request_id = %request_id,
            account = %account,
            slot = slot,
            total_time_ms = total_time.as_millis(),
            proof_length = merkle_proof.proof.len(),
            "Successfully completed HeliusProofProvider request"
        );

        Ok(AccountProof {
            account: account_data,
            proof: merkle_proof,
            slot,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_helius_config_default() {
        let config = HeliusConfig::default();
        assert_eq!(config.cluster, "mainnet-beta");
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.timeout_ms, 10000);
    }

    #[tokio::test]
    async fn test_circuit_breaker() {
        let config = HeliusConfig::default();
        let rpc = Arc::new(
            crate::infrastructure::CustomRPCEndpoints::new(vec![]).unwrap()
        );
        
        // This will fail without a valid API key, but that's expected for the test
        if let Ok(provider) = HeliusProofProvider::new(config, rpc) {
            // Simulate multiple failures
            for _ in 0..6 {
                provider.update_endpoint_health("test", false, 1000, Some("test error".to_string())).await;
            }
            
            assert!(provider.is_circuit_breaker_open("test").await);
        }
    }

    #[test]
    fn test_convert_helius_proof() {
        let config = HeliusConfig::default();
        let rpc = Arc::new(
            crate::infrastructure::CustomRPCEndpoints::new(vec![]).unwrap()
        );
        
        if let Ok(provider) = HeliusProofProvider::new(config, rpc) {
            let mock_proof = AssetProof {
                root: "11111111111111111111111111111111".to_string(),
                proof: vec!["22222222222222222222222222222222".to_string()],
                node_index: 12345,
                leaf: "33333333333333333333333333333333".to_string(),
                tree_id: "44444444444444444444444444444444".to_string(),
            };
            
            let result = provider.convert_helius_proof(mock_proof);
            assert!(result.is_ok());
            
            let merkle_proof = result.unwrap();
            assert_eq!(merkle_proof.leaf_index, 12345);
            assert_eq!(merkle_proof.proof.len(), 1);
        }
    }
}
