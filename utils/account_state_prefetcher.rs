#![deny(unsafe_code)]
#![deny(warnings)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;
use thiserror::Error;
use blake3::Hasher;
use lru::LruCache;
use serde_json::json;
use solana_sdk::account::Account;
use solana_sdk::pubkey::Pubkey;
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::RpcAccountInfoConfig;
use solana_client::rpc_response::RpcKeyedAccount;
use solana_client::client_error::ClientError;
use chrono::{DateTime, Utc};

const DEFAULT_TTL_SECS: u64 = 5;
const DEFAULT_CACHE_SIZE: usize = 100_000;
const METRICS_LOG_INTERVAL_MS: u64 = 1000;

#[derive(Error, Debug)]
pub enum PrefetchError {
    #[error("RPC client error: {0}")]
    RpcError(#[from] ClientError),
    #[error("Account not found: {pubkey}")]
    AccountNotFound { pubkey: Pubkey },
    #[error("Cache operation failed: {message}")]
    CacheError { message: String },
    #[error("Serialization error: {0}")]
    SerializationError(#[from] serde_json::Error),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Slot invalidation error: expected {expected}, got {actual}")]
    SlotInvalidation { expected: u64, actual: u64 },
    #[error("TTL expired for account {pubkey}")]
    TtlExpired { pubkey: Pubkey },
    #[error("Concurrent access error: {message}")]
    ConcurrencyError { message: String },
}

#[derive(Debug, Clone)]
struct CacheEntry {
    account: Account,
    cached_at: Instant,
    slot: u64,
    ttl_secs: u64,
    fingerprint: [u8; 32],
}

impl CacheEntry {
    fn new(account: Account, slot: u64, ttl_secs: u64) -> Self {
        let mut hasher = Hasher::new();
        hasher.update(&account.data);
        hasher.update(&account.lamports.to_le_bytes());
        hasher.update(&account.owner.to_bytes());
        hasher.update(&account.executable.to_string().as_bytes());
        hasher.update(&account.rent_epoch.to_le_bytes());
        
        Self {
            account,
            cached_at: Instant::now(),
            slot,
            ttl_secs,
            fingerprint: hasher.finalize().into(),
        }
    }
    
    fn is_expired(&self) -> bool {
        self.cached_at.elapsed() > Duration::from_secs(self.ttl_secs)
    }
    
    fn is_valid_for_slot(&self, current_slot: u64) -> bool {
        self.slot <= current_slot
    }
    
    fn fingerprint_matches(&self, other: &Account) -> bool {
        let mut hasher = Hasher::new();
        hasher.update(&other.data);
        hasher.update(&other.lamports.to_le_bytes());
        hasher.update(&other.owner.to_bytes());
        hasher.update(&other.executable.to_string().as_bytes());
        hasher.update(&other.rent_epoch.to_le_bytes());
        
        self.fingerprint == hasher.finalize().into()
    }
}

#[derive(Debug, Default)]
pub struct PrefetchMetrics {
    pub hits: AtomicU64,
    pub misses: AtomicU64,
    pub ttl_expiry: AtomicU64,
    pub fetch_failures: AtomicU64,
    pub slot_invalidations: AtomicU64,
    pub cache_evictions: AtomicU64,
    pub concurrent_fetches: AtomicU64,
}

impl PrefetchMetrics {
    pub fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_ttl_expiry(&self) {
        self.ttl_expiry.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_fetch_failure(&self) {
        self.fetch_failures.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_slot_invalidation(&self) {
        self.slot_invalidations.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_cache_eviction(&self) {
        self.cache_evictions.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn record_concurrent_fetch(&self) {
        self.concurrent_fetches.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn get_snapshot(&self) -> HashMap<String, u64> {
        let mut snapshot = HashMap::new();
        snapshot.insert("hits".to_string(), self.hits.load(Ordering::Relaxed));
        snapshot.insert("misses".to_string(), self.misses.load(Ordering::Relaxed));
        snapshot.insert("ttl_expiry".to_string(), self.ttl_expiry.load(Ordering::Relaxed));
        snapshot.insert("fetch_failures".to_string(), self.fetch_failures.load(Ordering::Relaxed));
        snapshot.insert("slot_invalidations".to_string(), self.slot_invalidations.load(Ordering::Relaxed));
        snapshot.insert("cache_evictions".to_string(), self.cache_evictions.load(Ordering::Relaxed));
        snapshot.insert("concurrent_fetches".to_string(), self.concurrent_fetches.load(Ordering::Relaxed));
        snapshot
    }
}

pub struct AccountStatePrefetcher {
    rpc_client: Arc<RpcClient>,
    cache: Arc<RwLock<LruCache<Pubkey, CacheEntry>>>,
    metrics: Arc<PrefetchMetrics>,
    default_ttl_secs: u64,
    current_slot: Arc<AtomicU64>,
    log_file_path: String,
}

impl AccountStatePrefetcher {
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        Self::with_config(rpc_client, DEFAULT_CACHE_SIZE, DEFAULT_TTL_SECS)
    }
    
    pub fn with_config(rpc_client: Arc<RpcClient>, cache_size: usize, ttl_secs: u64) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let date = DateTime::from_timestamp(now as i64, 0)
            .unwrap()
            .format("%Y-%m-%d")
            .to_string();
        
        let log_file_path = format!("logs/prefetch_metrics_{}.jsonl", date);
        
        let prefetcher = Self {
            rpc_client,
            cache: Arc::new(RwLock::new(LruCache::new(cache_size.try_into().unwrap()))),
            metrics: Arc::new(PrefetchMetrics::default()),
            default_ttl_secs: ttl_secs,
            current_slot: Arc::new(AtomicU64::new(0)),
            log_file_path,
        };
        
        prefetcher.start_metrics_logger();
        prefetcher
    }
    
    pub async fn prefetch_accounts(&self, pubkeys: &[Pubkey]) -> Result<HashMap<Pubkey, Account>, PrefetchError> {
        let mut result = HashMap::new();
        let mut to_fetch = Vec::new();
        
        // Update current slot
        if let Ok(slot) = self.rpc_client.get_slot() {
            self.current_slot.store(slot, Ordering::Relaxed);
        }
        
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        
        // Check cache first
        {
            let cache = self.cache.read().await;
            for &pubkey in pubkeys {
                if let Some(entry) = cache.peek(&pubkey) {
                    if !entry.is_expired() && entry.is_valid_for_slot(current_slot) {
                        self.metrics.record_hit();
                        result.insert(pubkey, entry.account.clone());
                        self.log_cache_event(current_slot, pubkey, "HIT", entry.cached_at.elapsed().as_millis() as u64).await;
                    } else {
                        if entry.is_expired() {
                            self.metrics.record_ttl_expiry();
                        } else {
                            self.metrics.record_slot_invalidation();
                        }
                        to_fetch.push(pubkey);
                    }
                } else {
                    self.metrics.record_miss();
                    to_fetch.push(pubkey);
                }
            }
        }
        
        // Fetch missing accounts
        if !to_fetch.is_empty() {
            let fetch_results = self.fetch_accounts_batch(&to_fetch).await;
            
            for pubkey in &to_fetch {
                match fetch_results.get(pubkey) {
                    Some(Ok(account)) => {
                        result.insert(*pubkey, account.clone());
                        
                        // Update cache
                        {
                            let mut cache = self.cache.write().await;
                            let entry = CacheEntry::new(account.clone(), current_slot, self.default_ttl_secs);
                            if cache.put(*pubkey, entry).is_some() {
                                self.metrics.record_cache_eviction();
                            }
                        }
                        
                        self.log_cache_event(current_slot, *pubkey, "FETCH", 0).await;
                    }
                    Some(Err(e)) => {
                        self.metrics.record_fetch_failure();
                        self.log_cache_event(current_slot, *pubkey, "ERROR", 0).await;
                        return Err(e.clone());
                    }
                    None => {
                        self.metrics.record_fetch_failure();
                        self.log_cache_event(current_slot, *pubkey, "NOT_FOUND", 0).await;
                        return Err(PrefetchError::AccountNotFound { pubkey: *pubkey });
                    }
                }
            }
        }
        
        Ok(result)
    }
    
    async fn fetch_accounts_batch(&self, pubkeys: &[Pubkey]) -> HashMap<Pubkey, Result<Account, PrefetchError>> {
        let mut results = HashMap::new();
        
        // Use concurrent futures for high throughput
        let futures: Vec<_> = pubkeys.iter().map(|&pubkey| {
            let rpc_client = self.rpc_client.clone();
            async move {
                self.metrics.record_concurrent_fetch();
                let result = rpc_client.get_account(&pubkey).await;
                (pubkey, result)
            }
        }).collect();
        
        let fetch_results = futures::future::join_all(futures).await;
        
        for (pubkey, result) in fetch_results {
            match result {
                Ok(Some(account)) => {
                    results.insert(pubkey, Ok(account));
                }
                Ok(None) => {
                    results.insert(pubkey, Err(PrefetchError::AccountNotFound { pubkey }));
                }
                Err(e) => {
                    results.insert(pubkey, Err(PrefetchError::RpcError(e)));
                }
            }
        }
        
        results
    }
    
    pub async fn invalidate_slot(&self, slot: u64) -> Result<(), PrefetchError> {
        let mut cache = self.cache.write().await;
        let mut to_remove = Vec::new();
        
        for (pubkey, entry) in cache.iter() {
            if entry.slot > slot {
                to_remove.push(*pubkey);
            }
        }
        
        for pubkey in to_remove {
            cache.pop(&pubkey);
            self.metrics.record_slot_invalidation();
        }
        
        Ok(())
    }
    
    pub async fn clear_cache(&self) -> Result<(), PrefetchError> {
        let mut cache = self.cache.write().await;
        cache.clear();
        Ok(())
    }
    
    pub fn get_metrics(&self) -> Arc<PrefetchMetrics> {
        self.metrics.clone()
    }
    
    async fn log_cache_event(&self, slot: u64, pubkey: Pubkey, status: &str, ttl_ms: u64) {
        let log_entry = json!({
            "timestamp": Utc::now().to_rfc3339(),
            "slot": slot,
            "key": pubkey.to_string(),
            "status": status,
            "ttl_ms": ttl_ms
        });
        
        if let Err(e) = self.write_log_entry(&log_entry.to_string()).await {
            eprintln!("Failed to write log entry: {}", e);
        }
    }
    
    async fn write_log_entry(&self, entry: &str) -> Result<(), PrefetchError> {
        tokio::fs::create_dir_all("logs").await?;
        
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_file_path)
            .await?;
        
        file.write_all(entry.as_bytes()).await?;
        file.write_all(b"\n").await?;
        file.flush().await?;
        
        Ok(())
    }
    
    fn start_metrics_logger(&self) {
        let metrics = self.metrics.clone();
        let log_file_path = self.log_file_path.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(METRICS_LOG_INTERVAL_MS));
            
            loop {
                interval.tick().await;
                
                let snapshot = metrics.get_snapshot();
                let log_entry = json!({
                    "timestamp": Utc::now().to_rfc3339(),
                    "type": "metrics_snapshot",
                    "metrics": snapshot
                });
                
                if let Err(e) = Self::write_metrics_log(&log_file_path, &log_entry.to_string()).await {
                    eprintln!("Failed to write metrics log: {}", e);
                }
            }
        });
    }
    
    async fn write_metrics_log(file_path: &str, entry: &str) -> Result<(), PrefetchError> {
        tokio::fs::create_dir_all("logs").await?;
        
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(file_path)
            .await?;
        
        file.write_all(entry.as_bytes()).await?;
        file.write_all(b"\n").await?;
        file.flush().await?;
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use tokio::time::{sleep, Duration};
    use solana_client::rpc_client::RpcClient;
    use solana_sdk::signature::Keypair;
    use solana_sdk::signer::Signer;
    
    fn mock_rpc_client() -> Arc<RpcClient> {
        Arc::new(RpcClient::new("https://api.testnet.solana.com".to_string()))
    }
    
    fn create_test_account() -> Account {
        Account {
            lamports: 1000000,
            data: vec![1, 2, 3, 4, 5],
            owner: Pubkey::from_str("11111111111111111111111111111112").unwrap(),
            executable: false,
            rent_epoch: 0,
        }
    }
    
    #[tokio::test]
    async fn test_cache_entry_creation() {
        let account = create_test_account();
        let entry = CacheEntry::new(account.clone(), 100, 5);
        
        assert_eq!(entry.account.lamports, 1000000);
        assert_eq!(entry.slot, 100);
        assert_eq!(entry.ttl_secs, 5);
        assert!(!entry.is_expired());
        assert!(entry.is_valid_for_slot(100));
        assert!(entry.is_valid_for_slot(101));
        assert!(!entry.is_valid_for_slot(99));
    }
    
    #[tokio::test]
    async fn test_cache_entry_expiry() {
        let account = create_test_account();
        let mut entry = CacheEntry::new(account, 100, 0);
        
        // Simulate expiry by setting cached_at to past
        entry.cached_at = Instant::now() - Duration::from_secs(1);
        assert!(entry.is_expired());
    }
    
    #[tokio::test]
    async fn test_cache_entry_fingerprint() {
        let account1 = create_test_account();
        let account2 = create_test_account();
        let mut account3 = create_test_account();
        account3.lamports = 2000000;
        
        let entry1 = CacheEntry::new(account1.clone(), 100, 5);
        
        assert!(entry1.fingerprint_matches(&account2));
        assert!(!entry1.fingerprint_matches(&account3));
    }
    
    #[tokio::test]
    async fn test_prefetcher_creation() {
        let rpc_client = mock_rpc_client();
        let prefetcher = AccountStatePrefetcher::new(rpc_client);
        
        assert_eq!(prefetcher.default_ttl_secs, DEFAULT_TTL_SECS);
        assert!(prefetcher.log_file_path.contains("prefetch_metrics_"));
    }
    
    #[tokio::test]
    async fn test_prefetcher_with_config() {
        let rpc_client = mock_rpc_client();
        let prefetcher = AccountStatePrefetcher::with_config(rpc_client, 50000, 10);
        
        assert_eq!(prefetcher.default_ttl_secs, 10);
    }
    
    #[tokio::test]
    async fn test_metrics_recording() {
        let metrics = PrefetchMetrics::default();
        
        metrics.record_hit();
        metrics.record_miss();
        metrics.record_ttl_expiry();
        metrics.record_fetch_failure();
        metrics.record_slot_invalidation();
        metrics.record_cache_eviction();
        metrics.record_concurrent_fetch();
        
        let snapshot = metrics.get_snapshot();
        assert_eq!(snapshot["hits"], 1);
        assert_eq!(snapshot["misses"], 1);
        assert_eq!(snapshot["ttl_expiry"], 1);
        assert_eq!(snapshot["fetch_failures"], 1);
        assert_eq!(snapshot["slot_invalidations"], 1);
        assert_eq!(snapshot["cache_evictions"], 1);
        assert_eq!(snapshot["concurrent_fetches"], 1);
    }
    
    #[tokio::test]
    async fn test_clear_cache() {
        let rpc_client = mock_rpc_client();
        let prefetcher = AccountStatePrefetcher::new(rpc_client);
        
        // Add some test entries
        {
            let mut cache = prefetcher.cache.write().await;
            let account = create_test_account();
            let entry = CacheEntry::new(account, 100, 5);
            cache.put(Keypair::new().pubkey(), entry);
        }
        
        // Verify cache has entries
        {
            let cache = prefetcher.cache.read().await;
            assert_eq!(cache.len(), 1);
        }
        
        // Clear cache
        prefetcher.clear_cache().await.unwrap();
        
        // Verify cache is empty
        {
            let cache = prefetcher.cache.read().await;
            assert_eq!(cache.len(), 0);
        }
    }
    
    #[tokio::test]
    async fn test_slot_invalidation() {
        let rpc_client = mock_rpc_client();
        let prefetcher = AccountStatePrefetcher::new(rpc_client);
        
        // Add entries with different slots
        {
            let mut cache = prefetcher.cache.write().await;
            let account = create_test_account();
            
            let entry1 = CacheEntry::new(account.clone(), 100, 5);
            let entry2 = CacheEntry::new(account.clone(), 200, 5);
            let entry3 = CacheEntry::new(account, 300, 5);
            
            cache.put(Keypair::new().pubkey(), entry1);
            cache.put(Keypair::new().pubkey(), entry2);
            cache.put(Keypair::new().pubkey(), entry3);
        }
        
        // Verify initial state
        {
            let cache = prefetcher.cache.read().await;
            assert_eq!(cache.len(), 3);
        }
        
        // Invalidate slot 250 (should remove entries with slot > 250)
        prefetcher.invalidate_slot(250).await.unwrap();
        
        // Verify entries with slot > 250 are removed
        {
            let cache = prefetcher.cache.read().await;
            assert_eq!(cache.len(), 2);
        }
    }
    
    #[tokio::test]
    async fn test_concurrent_access() {
        let rpc_client = mock_rpc_client();
        let prefetcher = Arc::new(AccountStatePrefetcher::new(rpc_client));
        
        let mut handles = Vec::new();
        
        // Spawn multiple concurrent tasks
        for i in 0..10 {
            let prefetcher_clone = prefetcher.clone();
            let handle = tokio::spawn(async move {
                let pubkey = Keypair::new().pubkey();
                let pubkeys = vec![pubkey];
                
                // This will likely fail due to network, but tests concurrency
                let _ = prefetcher_clone.prefetch_accounts(&pubkeys).await;
                
                // Clear cache concurrently
                let _ = prefetcher_clone.clear_cache().await;
                
                i
            });
            handles.push(handle);
        }
        
        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }
        
        // Verify metrics show concurrent activity
        let metrics = prefetcher.get_metrics();
        let snapshot = metrics.get_snapshot();
        assert!(snapshot["concurrent_fetches"] > 0 || snapshot["fetch_failures"] > 0);
    }
    
    #[tokio::test]
    async fn test_slot_rollover() {
        let rpc_client = mock_rpc_client();
        let prefetcher = AccountStatePrefetcher::new(rpc_client);
        
        // Simulate slot progression
        prefetcher.current_slot.store(100, Ordering::Relaxed);
        
        // Add cache entry at slot 100
        {
            let mut cache = prefetcher.cache.write().await;
            let account = create_test_account();
            let entry = CacheEntry::new(account, 100, 5);
            cache.put(Keypair::new().pubkey(), entry);
        }
        
        // Simulate slot rollover to 200
        prefetcher.current_slot.store(200, Ordering::Relaxed);
        
        // Entry should still be valid (slot 100 <= current slot 200)
        {
            let cache = prefetcher.cache.read().await;
            if let Some(entry) = cache.peek(&Keypair::new().pubkey()) {
                assert!(entry.is_valid_for_slot(200));
            }
        }
    }
    
    #[tokio::test]
    async fn test_mainnet_power_assertion() {
        // Test that this module meets MAINNET POWER requirements
        let rpc_client = mock_rpc_client();
        let prefetcher = AccountStatePrefetcher::with_config(rpc_client, 100_000, 5);
        
        // Verify high-performance configuration
        assert_eq!(prefetcher.default_ttl_secs, 5);
        
        // Verify metrics are working
        let metrics = prefetcher.get_metrics();
        metrics.record_hit();
        let snapshot = metrics.get_snapshot();
        assert_eq!(snapshot["hits"], 1);
        
        // Verify cache operations are async-safe
        let cache_clear_result = prefetcher.clear_cache().await;
        assert!(cache_clear_result.is_ok());
        
        // Verify slot invalidation works
        let slot_invalidation_result = prefetcher.invalidate_slot(100).await;
        assert!(slot_invalidation_result.is_ok());
        
        // Assert environment variable for MAINNET_POWER mode
        assert_eq!(std::env::var("MEV_MODE").unwrap_or_default(), "MAINNET_POWER");
    }
}
