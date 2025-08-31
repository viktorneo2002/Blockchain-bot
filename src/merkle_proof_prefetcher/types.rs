use solana_sdk::{
    account::Account,
    hash::Hash,
};
use std::time::Instant;

#[derive(Debug, Clone)]
pub struct MerkleProof {
    pub root: Hash,
    pub proof: Vec<Hash>,
    pub leaf_index: u64,
    pub timestamp: Instant,
}

#[derive(Debug, Clone)]
pub struct AccountProof {
    pub account: Account,
    pub proof: MerkleProof,
    pub slot: u64,
}
