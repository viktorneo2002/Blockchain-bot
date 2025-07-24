use anyhow::{anyhow, Result};
use dashmap::DashMap;
use futures::future::join_all;
use lru::LruCache;
use parking_lot::RwLock;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, RpcFilterType},
};
use solana_sdk::{
    account::Account,
    commitment_config::{CommitmentConfig, CommitmentLevel},
    hash::Hash,
    pubkey::Pubkey,
    signature::Signature,
};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, RwLock as TokioRwLock, Semaphore},
    time::{interval, sleep},
};

const MAX_CACHE_SIZE: usize = 50000;
const PREFETCH_BATCH_SIZE: usize = 100;
const MAX_CONCURRENT_REQUESTS: usize = 50;
const CACHE_TTL_SECONDS: u64 = 30;
const PROOF_RETRY_ATTEMPTS: u32 = 3;
const RPC_TIMEOUT: Duration = Duration::from_millis(500);
const PREFETCH_INTERVAL_MS: u64 = 50;
const MAX_PROOF_DEPTH: usize = 32;

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

#[derive(Debug)]
struct CachedProof {
    proof: AccountProof,
    last_accessed: Instant,
    access_count: u64,
}

pub struct MerkleProofPrefetcher {
    rpc_clients: Vec<Arc<RpcClient>>,
    proof_cache: Arc<DashMap<Pubkey, CachedProof>>,
    lru_cache: Arc<RwLock<LruCache<Pubkey, ()>>>,
    prefetch_queue: Arc<TokioRwLock<HashSet<Pubkey>>>,
    active_prefetches: Arc<DashMap<Pubkey, Instant>>,
    request_semaphore: Arc<Semaphore>,
    stats: Arc<PrefetcherStats>,
    shutdown: Arc<AtomicBool>,
}

#[derive(Debug, Default)]
struct PrefetcherStats {
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    prefetch_requests: AtomicU64,
    failed_requests: AtomicU64,
    evictions: AtomicU64,
}

impl MerkleProofPrefetcher {
    pub fn new(rpc_endpoints: Vec<String>) -> Result<Self> {
        if rpc_endpoints.is_empty() {
            return Err(anyhow!("No RPC endpoints provided"));
        }

        let rpc_clients = rpc_endpoints
            .into_iter()
            .map(|url| {
                Arc::new(RpcClient::new_with_timeout_and_commitment(
                    url,
                    RPC_TIMEOUT,
                    CommitmentConfig {
                        commitment: CommitmentLevel::Processed,
                    },
                ))
            })
            .collect();

        Ok(Self {
            rpc_clients,
            proof_cache: Arc::new(DashMap::new()),
            lru_cache: Arc::new(RwLock::new(LruCache::new(
                std::num::NonZeroUsize::new(MAX_CACHE_SIZE).unwrap(),
            ))),
            prefetch_queue: Arc::new(TokioRwLock::new(HashSet::new())),
            active_prefetches: Arc::new(DashMap::new()),
            request_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS)),
            stats: Arc::new(PrefetcherStats::default()),
            shutdown: Arc::new(AtomicBool::new(false)),
        })
    }

    pub async fn start(&self) -> Result<()> {
        let prefetcher = self.clone_self();
        tokio::spawn(async move {
            prefetcher.run_prefetch_loop().await;
        });

        let cleaner = self.clone_self();
        tokio::spawn(async move {
            cleaner.run_cache_cleaner().await;
        });

        Ok(())
    }

    pub async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    pub async fn get_account_with_proof(&self, account: &Pubkey) -> Result<AccountProof> {
        if let Some(cached) = self.get_from_cache(account) {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(cached.proof);
        }

        self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
        self.prefetch_queue.write().await.insert(*account);
        
        let proof = self.fetch_proof_with_retry(account).await?;
        self.update_cache(account, proof.clone());
        
        Ok(proof)
    }

    pub async fn prefetch_accounts(&self, accounts: Vec<Pubkey>) -> Result<()> {
        let mut queue = self.prefetch_queue.write().await;
        for account in accounts {
            if !self.proof_cache.contains_key(&account) {
                queue.insert(account);
            }
        }
        Ok(())
    }

    async fn run_prefetch_loop(&self) {
        let mut interval = interval(Duration::from_millis(PREFETCH_INTERVAL_MS));
        
        while !self.shutdown.load(Ordering::Relaxed) {
            interval.tick().await;
            
            let accounts = self.get_prefetch_batch().await;
            if accounts.is_empty() {
                continue;
            }

            let futures = accounts
                .into_iter()
                .map(|account| self.prefetch_single_account(account))
                .collect::<Vec<_>>();

            join_all(futures).await;
        }
    }

    async fn run_cache_cleaner(&self) {
        let mut interval = interval(Duration::from_secs(5));
        
        while !self.shutdown.load(Ordering::Relaxed) {
            interval.tick().await;
            
            let now = Instant::now();
            let mut to_remove = Vec::new();
            
            for entry in self.proof_cache.iter() {
                let age = now.duration_since(entry.value().last_accessed);
                if age > Duration::from_secs(CACHE_TTL_SECONDS) {
                    to_remove.push(*entry.key());
                }
            }
            
            for key in to_remove {
                self.proof_cache.remove(&key);
                self.lru_cache.write().pop(&key);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    async fn get_prefetch_batch(&self) -> Vec<Pubkey> {
        let mut queue = self.prefetch_queue.write().await;
        let now = Instant::now();
        
        let mut batch = Vec::new();
        let mut to_remove = Vec::new();
        
        for account in queue.iter() {
            if batch.len() >= PREFETCH_BATCH_SIZE {
                break;
            }
            
            if let Some(start_time) = self.active_prefetches.get(account) {
                if now.duration_since(*start_time) > Duration::from_secs(5) {
                    self.active_prefetches.remove(account);
                } else {
                    continue;
                }
            }
            
            batch.push(*account);
            to_remove.push(*account);
        }
        
        for account in to_remove {
            queue.remove(&account);
            self.active_prefetches.insert(account, now);
        }
        
        batch
    }

    async fn prefetch_single_account(&self, account: Pubkey) {
        self.stats.prefetch_requests.fetch_add(1, Ordering::Relaxed);
        
        match self.fetch_proof_with_retry(&account).await {
            Ok(proof) => {
                self.update_cache(&account, proof);
            }
            Err(_) => {
                self.stats.failed_requests.fetch_add(1, Ordering::Relaxed);
            }
        }
        
        self.active_prefetches.remove(&account);
    }

    async fn fetch_proof_with_retry(&self, account: &Pubkey) -> Result<AccountProof> {
        let _permit = self.request_semaphore.acquire().await?;
        
        for attempt in 0..PROOF_RETRY_ATTEMPTS {
            let client_index = (attempt as usize) % self.rpc_clients.len();
            let client = &self.rpc_clients[client_index];
            
            match self.fetch_single_proof(client, account).await {
                Ok(proof) => return Ok(proof),
                Err(e) => {
                    if attempt == PROOF_RETRY_ATTEMPTS - 1 {
                        return Err(e);
                    }
                    sleep(Duration::from_millis(10 * (attempt + 1) as u64)).await;
                }
            }
        }
        
        Err(anyhow!("Failed to fetch proof after retries"))
    }

    async fn fetch_single_proof(
        &self,
        client: &Arc<RpcClient>,
        account: &Pubkey,
    ) -> Result<AccountProof> {
        let config = RpcAccountInfoConfig {
            encoding: None,
            data_slice: None,
            commitment: Some(CommitmentConfig::processed()),
            min_context_slot: None,
        };

        let response = client
            .get_account_with_commitment(account, config.commitment.unwrap())
            .await?;

        let account_data = response
            .value
            .ok_or_else(|| anyhow!("Account not found"))?;

        let slot = response.context.slot;
        let block_height = client.get_block_height().await?;
        
        let proof = self.generate_merkle_proof(account, slot, block_height).await?;

        Ok(AccountProof {
            account: account_data,
            proof,
            slot,
        })
    }

    async fn generate_merkle_proof(
        &self,
        account: &Pubkey,
        slot: u64,
        block_height: u64,
    ) -> Result<MerkleProof> {
        let leaf_index = self.calculate_leaf_index(account, slot);
        let tree_size = self.calculate_tree_size(block_height);
        let proof_path = self.calculate_proof_path(leaf_index, tree_size);
        
        let mut proof_hashes = Vec::with_capacity(proof_path.len());
        let client = &self.rpc_clients[0];
        
        for node_info in proof_path {
            let hash = self.fetch_node_hash(client, node_info.0, node_info.1).await?;
            proof_hashes.push(hash);
        }

        let root = self.calculate_root_hash(account, &proof_hashes, leaf_index);

        Ok(MerkleProof {
            root,
            proof: proof_hashes,
            leaf_index,
            timestamp: Instant::now(),
        })
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
        let growth_factor = block_height / 1000000;
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

    async fn fetch_node_hash(
        &self,
        client: &Arc<RpcClient>,
        node_index: u64,
        level_size: u64,
    ) -> Result<Hash> {
        let slot = client.get_slot().await?;
        let block = client.get_block(slot).await?;
        
        let mut hasher = blake3::Hasher::new();
        hasher.update(&node_index.to_le_bytes());
        hasher.update(&level_size.to_le_bytes());
        hasher.update(&block.blockhash.to_bytes());
        
        let hash_bytes = hasher.finalize();
        Ok(Hash::new_from_array(hash_bytes.into()))
    }

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

    fn get_from_cache(&self, account: &Pubkey) -> Option<CachedProof> {
        if let Some(mut entry) = self.proof_cache.get_mut(account) {
            entry.last_accessed = Instant::now();
            entry.access_count += 1;
            
            self.lru_cache.write().put(*account, ());
            
            return Some(entry.clone());
        }
        None
    }

    fn update_cache(&self, account: &Pubkey, proof: AccountProof) {
        let cached = CachedProof {
            proof,
            last_accessed: Instant::now(),
            access_count: 1,
        };
        
        self.proof_cache.insert(*account, cached);
        self.lru_cache.write().put(*account, ());
        
        if self.proof_cache.len() > MAX_CACHE_SIZE {
            self.evict_lru();
        }
    }

    fn evict_lru(&self) {
        let mut lru = self.lru_cache.write();
        
        while self.proof_cache.len() > MAX_CACHE_SIZE * 9 / 10 {
            if let Some((key, _)) = lru.pop_lru() {
                self.proof_cache.remove(&key);
                self.stats.evictions.fetch_add(1, Ordering::Relaxed);
            } else {
                break;
            }
        }
    }

    fn clone_self(&self) -> Self {
        Self {
            rpc_clients: self.rpc_clients.clone(),
            proof_cache: self.proof_cache.clone(),
            lru_cache: self.lru_cache.clone(),
            prefetch_queue: self.prefetch_queue.clone(),
            active_prefetches: self.active_prefetches.clone(),
            request_semaphore: self.request_semaphore.clone(),
            stats: self.stats.clone(),
            shutdown: self.shutdown.clone(),
        }
    }

    pub async fn batch_get_proofs(&self, accounts: &[Pubkey]) -> Result<Vec<AccountProof>> {
        let mut results = Vec::with_capacity(accounts.len());
        let mut missing_accounts = Vec::new();
        let mut missing_indices = Vec::new();

        for (idx, account) in accounts.iter().enumerate() {
            if let Some(cached) = self.get_from_cache(account) {
                results.push(Some(cached.proof));
                self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            } else {
                results.push(None);
                missing_accounts.push(*account);
                missing_indices.push(idx);
                self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
            }
        }

        if !missing_accounts.is_empty() {
            let futures = missing_accounts
                .iter()
                .map(|account| self.fetch_proof_with_retry(account))
                .collect::<Vec<_>>();

            let fetched_proofs = join_all(futures).await;

            for (i, proof_result) in fetched_proofs.into_iter().enumerate() {
                match proof_result {
                    Ok(proof) => {
                        let account = &missing_accounts[i];
                        self.update_cache(account, proof.clone());
                        results[missing_indices[i]] = Some(proof);
                    }
                    Err(_) => {
                        self.stats.failed_requests.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }

        results
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| anyhow!("Failed to fetch some proofs"))
    }

    pub async fn warm_cache(&self, hot_accounts: Vec<Pubkey>) -> Result<()> {
        const WARM_BATCH_SIZE: usize = 200;
        
        for chunk in hot_accounts.chunks(WARM_BATCH_SIZE) {
            self.batch_get_proofs(chunk).await?;
            sleep(Duration::from_millis(10)).await;
        }
        
        Ok(())
    }

    pub fn get_stats(&self) -> PrefetcherStatsSnapshot {
        PrefetcherStatsSnapshot {
            cache_hits: self.stats.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.stats.cache_misses.load(Ordering::Relaxed),
            prefetch_requests: self.stats.prefetch_requests.load(Ordering::Relaxed),
            failed_requests: self.stats.failed_requests.load(Ordering::Relaxed),
            evictions: self.stats.evictions.load(Ordering::Relaxed),
            cache_size: self.proof_cache.len(),
            queue_size: 0,
        }
    }

    pub async fn verify_proof(&self, account: &Pubkey, proof: &AccountProof) -> Result<bool> {
        let calculated_root = self.calculate_root_hash(account, &proof.proof.proof, proof.proof.leaf_index);
        
        if calculated_root != proof.proof.root {
            return Ok(false);
        }

        let client = &self.rpc_clients[0];
        let current_slot = client.get_slot().await?;
        
        if current_slot.saturating_sub(proof.slot) > 150 {
            return Ok(false);
        }

        Ok(true)
    }

    pub async fn optimize_cache(&self) {
        let mut access_stats: HashMap<Pubkey, (u64, Instant)> = HashMap::new();
        
        for entry in self.proof_cache.iter() {
            access_stats.insert(
                *entry.key(),
                (entry.value().access_count, entry.value().last_accessed),
            );
        }

        let mut sorted_accounts: Vec<(Pubkey, u64, Instant)> = access_stats
            .into_iter()
            .map(|(k, (count, time))| (k, count, time))
            .collect();

        sorted_accounts.sort_by(|a, b| {
            let score_a = a.1 as f64 / (Instant::now().duration_since(a.2).as_secs() as f64 + 1.0);
            let score_b = b.1 as f64 / (Instant::now().duration_since(b.2).as_secs() as f64 + 1.0);
            score_b.partial_cmp(&score_a).unwrap()
        });

        let keep_count = (MAX_CACHE_SIZE * 8 / 10).min(sorted_accounts.len());
        let to_keep: HashSet<Pubkey> = sorted_accounts
            .into_iter()
            .take(keep_count)
            .map(|(k, _, _)| k)
            .collect();

        let to_remove: Vec<Pubkey> = self
            .proof_cache
            .iter()
            .filter(|entry| !to_keep.contains(entry.key()))
            .map(|entry| *entry.key())
            .collect();

        for key in to_remove {
            self.proof_cache.remove(&key);
            self.lru_cache.write().pop(&key);
            self.stats.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub async fn handle_slot_update(&self, new_slot: u64) -> Result<()> {
        let stale_threshold = new_slot.saturating_sub(32);
        let mut to_remove = Vec::new();

        for entry in self.proof_cache.iter() {
            if entry.value().proof.slot < stale_threshold {
                to_remove.push(*entry.key());
            }
        }

        for key in to_remove {
            self.proof_cache.remove(&key);
            self.lru_cache.write().pop(&key);
        }

        Ok(())
    }

    pub async fn prioritize_accounts(&self, priority_accounts: Vec<Pubkey>) -> Result<()> {
        let mut queue = self.prefetch_queue.write().await;
        
        for account in priority_accounts.iter() {
            queue.remove(account);
        }
        
        drop(queue);
        
        for account in priority_accounts.iter() {
            if !self.proof_cache.contains_key(account) {
                if let Ok(proof) = self.fetch_proof_with_retry(account).await {
                    self.update_cache(account, proof);
                }
            }
        }
        
        Ok(())
    }

    pub fn clear_cache(&self) {
        self.proof_cache.clear();
        self.lru_cache.write().clear();
    }
}

#[derive(Debug, Clone)]
pub struct PrefetcherStatsSnapshot {
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub prefetch_requests: u64,
    pub failed_requests: u64,
    pub evictions: u64,
    pub cache_size: usize,
    pub queue_size: usize,
}

impl PrefetcherStatsSnapshot {
    pub fn hit_rate(&self) -> f64 {
        let total = self.cache_hits + self.cache_misses;
        if total == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total as f64
        }
    }

    pub fn success_rate(&self) -> f64 {
        let total = self.prefetch_requests;
        if total == 0 {
            1.0
        } else {
            (total - self.failed_requests) as f64 / total as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_merkle_proof_prefetcher() {
        let endpoints = vec!["https://api.mainnet-beta.solana.com".to_string()];
        let prefetcher = MerkleProofPrefetcher::new(endpoints).unwrap();
        
        let test_account = Pubkey::new_unique();
        prefetcher.prefetch_accounts(vec![test_account]).await.unwrap();
        
        let stats = prefetcher.get_stats();
        assert_eq!(stats.cache_size, 0);
    }
}

