use anyhow::{anyhow, Result};
use async_trait::async_trait;
use blake3;
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    hash::Hash,
    pubkey::Pubkey,
};
use std::{sync::Arc, time::Instant};

use crate::infrastructure::CustomRPCEndpoints;

use super::types::{AccountProof, MerkleProof};

#[async_trait]
pub trait ProofProvider: Send + Sync {
    async fn get_account_with_proof(&self, account: &Pubkey) -> Result<AccountProof>;
}

pub struct SimulatedProofProvider {
    rpc: Arc<CustomRPCEndpoints>,
}

impl SimulatedProofProvider {
    pub fn new(rpc: Arc<CustomRPCEndpoints>) -> Self {
        Self { rpc }
    }

    fn calculate_leaf_index(&self, account: &Pubkey, slot: u64) -> u64 {
        let mut hasher = blake3::Hasher::new();
        hasher.update(account.as_ref());
        hasher.update(&slot.to_le_bytes());
        let hash = hasher.finalize();
        u64::from_le_bytes(hash.as_bytes()[0..8].try_into().unwrap())
    }

    fn calculate_tree_size(&self, block_height: u64) -> u64 {
        let base_size = 1u64 << 20;
        let growth_factor = block_height / 1_000_000;
        base_size.saturating_mul(1 + growth_factor)
    }

    fn calculate_proof_path(&self, leaf_index: u64, tree_size: u64) -> Vec<(u64, u64)> {
        let mut path = Vec::new();
        let mut current_index = leaf_index;
        let mut level_size = tree_size;

        while level_size > 1 {
            let sibling_index = if current_index % 2 == 0 {
                current_index + 1
            } else {
                current_index - 1
            };

            if sibling_index < level_size {
                path.push((sibling_index, level_size));
            }

            current_index /= 2;
            level_size = (level_size + 1) / 2;
        }

        path
    }

    fn compute_node_hash(&self, node_index: u64, level_size: u64, seed: &Hash) -> Hash {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&node_index.to_le_bytes());
        hasher.update(&level_size.to_le_bytes());
        hasher.update(&seed.to_bytes());
        let hash_bytes = hasher.finalize();
        Hash::new_from_array(hash_bytes.into())
    }

    #[allow(dead_code)]
    fn calculate_root_hash(&self, account: &Pubkey, proof: &[Hash], leaf_index: u64) -> Hash {
        let mut current_hash = {
            let mut hasher = blake3::Hasher::new();
            hasher.update(account.as_ref());
            hasher.finalize()
        };

        let mut index = leaf_index;
        for sibling_hash in proof {
            let mut hasher = blake3::Hasher::new();
            if index % 2 == 0 {
                hasher.update(current_hash.as_bytes());
                hasher.update(&sibling_hash.to_bytes());
            } else {
                hasher.update(&sibling_hash.to_bytes());
                hasher.update(current_hash.as_bytes());
            }
            current_hash = hasher.finalize();
            index /= 2;
        }

        Hash::new_from_array(current_hash.into())
    }
}

#[async_trait]
impl ProofProvider for SimulatedProofProvider {
    async fn get_account_with_proof(&self, account: &Pubkey) -> Result<AccountProof> {
        let commitment = CommitmentConfig::processed();

        let account_data = self
            .rpc
            .get_account_info(account, commitment)
            .await?
            .ok_or_else(|| anyhow!("Account not found"))?;

        // Use network state for slot, block height, and blockhash seed
        let slot = self.rpc.get_slot(commitment).await?;
        let block_height = self.rpc.get_block_height(commitment).await?;
        let (seed, _last_valid_block_height) = self.rpc.get_latest_blockhash(commitment).await?;

        let raw_leaf_index = self.calculate_leaf_index(account, slot);
        let tree_size = self.calculate_tree_size(block_height);
        let leaf_index = raw_leaf_index % tree_size;
        let proof_path = self.calculate_proof_path(leaf_index, tree_size);

        let mut proof_hashes = Vec::with_capacity(proof_path.len());
        for (node_index, level_size) in proof_path {
            let hash = self.compute_node_hash(node_index, level_size, &seed);
            proof_hashes.push(hash);
        }

        let root = self.calculate_root_hash(account, &proof_hashes, leaf_index);

        Ok(AccountProof {
            account: account_data,
            proof: MerkleProof {
                root,
                proof: proof_hashes,
                leaf_index,
                timestamp: Instant::now(),
            },
            slot,
        })
    }
}

pub struct RealProofProvider {
    rpc: Arc<CustomRPCEndpoints>,
}

impl RealProofProvider {
    pub fn new(rpc: Arc<CustomRPCEndpoints>) -> Self {
        Self { rpc }
    }
}

#[async_trait]
impl ProofProvider for RealProofProvider {
    async fn get_account_with_proof(&self, _account: &Pubkey) -> Result<AccountProof> {
        Err(anyhow!(
            "RealProofProvider not yet implemented. Integrate DAS/Bubblegum proof APIs."
        ))
    }
}
