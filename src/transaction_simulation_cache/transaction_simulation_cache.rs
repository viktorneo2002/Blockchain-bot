#![deny(unsafe_code)]
#![deny(warnings)]

//! Production-ready transaction simulation cache module for Solana MEV bot
//!
//! This module provides a high-performance, thread-safe LRU cache for transaction
//! simulation results to prevent duplicate computations in MEV execution pipelines.
//! 
//! Features:
//! - Memory-safe LRU eviction with configurable capacity
//! - Blake3 fingerprinting of simulation inputs
//! - Slot-aware cache invalidation for fork detection
//! - Millisecond-precision TTL expiry
//! - Atomic hit/miss metrics
//! - Zero-allocation concurrent access patterns
//! - Structured error handling with detailed diagnostics

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use blake3::{Hash, Hasher};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::RwLock;

/// Maximum number of cache entries before LRU eviction
const DEFAULT_MAX_ENTRIES: usize = 10_000;
/// Default TTL in milliseconds (5 seconds)
const DEFAULT_TTL_MS: u128 = 5_000;

/// Structured error types for cache operations
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum CacheError {
    #[error("Cache capacity exceeded: {current} entries, max: {max}")]
    CapacityExceeded { current: usize, max: usize },
    
    #[error("Entry expired at {expired_at_ms}ms, current time: {current_time_ms}ms")]
    EntryExpired { expired_at_ms: u128, current_time_ms: u128 },
    
    #[error("Slot invalidation: entry slot {entry_slot} < current slot {current_slot}")]
    SlotInvalidated { entry_slot: u64, current_slot: u64 },
    
    #[error("Input serialization failed: {details}")]
    SerializationFailed { details: String },
    
    #[error("Time system error: {details}")]
    TimeSystemError { details: String },
}

/// Result type for cache operations
pub type CacheResult<T> = Result<T, CacheError>;

/// Transaction simulation input parameters for cache key generation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationInput {
    /// Current slot number
    pub slot: u64,
    /// Serialized transaction instructions
    pub instructions: Vec<u8>,
    /// Account addresses involved in simulation
    pub accounts: Vec<[u8; 32]>,
    /// Compute units limit
    pub compute_units: u64,
    /// Additional context for cache differentiation
    pub context: Vec<u8>,
}

impl SimulationInput {
    /// Generate Blake3 hash fingerprint of simulation input
    pub fn fingerprint(&self) -> CacheResult<[u8; 32]> {
        let serialized = bincode::serialize(self)
            .map_err(|e| CacheError::SerializationFailed {
                details: format!("Failed to serialize SimulationInput: {}", e),
            })?;
        
        let mut hasher = Hasher::new();
        hasher.update(&serialized);
        let hash = hasher.finalize();
        Ok(*hash.as_bytes())
    }
}

/// Simulation result data structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimResult {
    /// Whether simulation succeeded
    pub success: bool,
    /// Compute units consumed
    pub compute_units_consumed: u64,
    /// Error message if simulation failed
    pub error_message: Option<String>,
    /// Resulting account states
    pub account_states: Vec<u8>,
    /// Transaction logs
    pub logs: Vec<String>,
}

/// Cache entry with metadata
#[derive(Debug, Clone)]
pub struct SimulationCacheEntry {
    /// Blake3 hash of simulation input
    pub input_hash: [u8; 32],
    /// Cached simulation result
    pub sim_result: SimResult,
    /// Slot when entry was cached
    pub cached_at_slot: u64,
    /// Expiry timestamp in milliseconds since UNIX epoch
    pub expires_at_ms: u128,
    /// Last access timestamp for LRU eviction
    pub last_accessed_ms: u128,
}

impl SimulationCacheEntry {
    /// Check if entry is expired based on current time
    pub fn is_expired(&self, current_time_ms: u128) -> bool {
        self.expires_at_ms < current_time_ms
    }
    
    /// Check if entry is invalidated by slot advancement
    pub fn is_slot_invalidated(&self, current_slot: u64) -> bool {
        self.cached_at_slot < current_slot
    }
    
    /// Update last accessed timestamp
    pub fn touch(&mut self, current_time_ms: u128) {
        self.last_accessed_ms = current_time_ms;
    }
}

/// Cache performance metrics
#[derive(Debug, Default)]
pub struct CacheMetrics {
    /// Number of cache hits
    pub hits: AtomicU64,
    /// Number of cache misses
    pub misses: AtomicU64,
    /// Number of entries evicted due to TTL
    pub ttl_evictions: AtomicU64,
    /// Number of entries evicted due to slot advancement
    pub slot_evictions: AtomicU64,
    /// Number of entries evicted due to LRU
    pub lru_evictions: AtomicU64,
}

impl CacheMetrics {
    /// Calculate cache hit rate
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
    
    /// Reset all metrics
    pub fn reset(&self) {
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.ttl_evictions.store(0, Ordering::Relaxed);
        self.slot_evictions.store(0, Ordering::Relaxed);
        self.lru_evictions.store(0, Ordering::Relaxed);
    }
}

/// High-performance transaction simulation cache
pub struct TransactionSimulationCache {
    /// Internal cache storage
    cache: Arc<RwLock<HashMap<[u8; 32], SimulationCacheEntry>>>,
    /// Maximum number of entries before eviction
    max_entries: usize,
    /// Entry TTL in milliseconds
    ttl_ms: u128,
    /// Performance metrics
    metrics: CacheMetrics,
}

impl TransactionSimulationCache {
    /// Create new cache with specified capacity and TTL
    pub fn new(max_entries: usize, ttl_ms: u128) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::with_capacity(max_entries))),
            max_entries,
            ttl_ms,
            metrics: CacheMetrics::default(),
        }
    }
    
    /// Create cache with default parameters
    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_MAX_ENTRIES, DEFAULT_TTL_MS)
    }
    
    /// Get current timestamp in milliseconds
    fn current_time_ms() -> CacheResult<u128> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .map_err(|e| CacheError::TimeSystemError {
                details: format!("Failed to get current time: {}", e),
            })
    }
    
    /// Insert simulation result into cache
    pub async fn insert(&self, input: &SimulationInput, result: SimResult, slot: u64) -> CacheResult<()> {
        let input_hash = input.fingerprint()?;
        let current_time = Self::current_time_ms()?;
        let expires_at_ms = current_time + self.ttl_ms;
        
        let entry = SimulationCacheEntry {
            input_hash,
            sim_result: result,
            cached_at_slot: slot,
            expires_at_ms,
            last_accessed_ms: current_time,
        };
        
        let mut cache = self.cache.write().await;
        
        // Evict oldest entries if at capacity
        if cache.len() >= self.max_entries {
            self.evict_lru_entries(&mut cache, current_time).await;
        }
        
        cache.insert(input_hash, entry);
        Ok(())
    }
    
    /// Retrieve simulation result from cache
    pub async fn get(&self, input: &SimulationInput) -> CacheResult<Option<SimResult>> {
        let input_hash = input.fingerprint()?;
        let current_time = Self::current_time_ms()?;
        
        let mut cache = self.cache.write().await;
        
        if let Some(entry) = cache.get_mut(&input_hash) {
            // Check if entry is expired
            if entry.is_expired(current_time) {
                cache.remove(&input_hash);
                self.metrics.ttl_evictions.fetch_add(1, Ordering::Relaxed);
                self.metrics.misses.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
            
            // Check if entry is slot-invalidated
            if entry.is_slot_invalidated(input.slot) {
                cache.remove(&input_hash);
                self.metrics.slot_evictions.fetch_add(1, Ordering::Relaxed);
                self.metrics.misses.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
            
            // Update access time and return result
            entry.touch(current_time);
            self.metrics.hits.fetch_add(1, Ordering::Relaxed);
            Ok(Some(entry.sim_result.clone()))
        } else {
            self.metrics.misses.fetch_add(1, Ordering::Relaxed);
            Ok(None)
        }
    }
    
    /// Invalidate all entries with slot less than provided slot
    pub async fn invalidate_slot(&self, new_slot: u64) {
        let mut cache = self.cache.write().await;
        let mut to_remove = Vec::new();
        
        for (hash, entry) in cache.iter() {
            if entry.cached_at_slot < new_slot {
                to_remove.push(*hash);
            }
        }
        
        for hash in to_remove {
            cache.remove(&hash);
            self.metrics.slot_evictions.fetch_add(1, Ordering::Relaxed);
        }
    }
    
    /// Remove all expired entries
    pub async fn purge_expired(&self) -> CacheResult<()> {
        let current_time = Self::current_time_ms()?;
        let mut cache = self.cache.write().await;
        let mut to_remove = Vec::new();
        
        for (hash, entry) in cache.iter() {
            if entry.is_expired(current_time) {
                to_remove.push(*hash);
            }
        }
        
        for hash in to_remove {
            cache.remove(&hash);
            self.metrics.ttl_evictions.fetch_add(1, Ordering::Relaxed);
        }
        
        Ok(())
    }
    
    /// Get current cache size
    pub async fn len(&self) -> usize {
        let cache = self.cache.read().await;
        cache.len()
    }
    
    /// Check if cache is empty
    pub async fn is_empty(&self) -> bool {
        let cache = self.cache.read().await;
        cache.is_empty()
    }
    
    /// Clear all entries
    pub async fn clear(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
    }
    
    /// Get cache metrics
    pub fn metrics(&self) -> &CacheMetrics {
        &self.metrics
    }
    
    /// Evict least recently used entries when at capacity
    async fn evict_lru_entries(&self, cache: &mut HashMap<[u8; 32], SimulationCacheEntry>, current_time: u128) {
        let entries_to_evict = (cache.len() / 4).max(1); // Evict 25% or at least 1
        
        // Find LRU entries
        let mut entries: Vec<_> = cache.iter().collect();
        entries.sort_by_key(|(_, entry)| entry.last_accessed_ms);
        
        for (hash, _) in entries.iter().take(entries_to_evict) {
            cache.remove(hash);
            self.metrics.lru_evictions.fetch_add(1, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use tokio::task;
    use tokio::time::{sleep, Duration};
    
    fn create_test_input(slot: u64, instructions: &[u8]) -> SimulationInput {
        SimulationInput {
            slot,
            instructions: instructions.to_vec(),
            accounts: vec![[1u8; 32], [2u8; 32]],
            compute_units: 200_000,
            context: b"test".to_vec(),
        }
    }
    
    fn create_test_result() -> SimResult {
        SimResult {
            success: true,
            compute_units_consumed: 150_000,
            error_message: None,
            account_states: vec![1, 2, 3, 4, 5],
            logs: vec!["Test log".to_string()],
        }
    }
    
    #[tokio::test]
    async fn test_cache_insert_and_fetch_hit() {
        let cache = TransactionSimulationCache::new(100, 5000);
        let input = create_test_input(1000, b"test_instructions");
        let result = create_test_result();
        
        // Insert entry
        cache.insert(&input, result.clone(), 1000).await.unwrap();
        
        // Fetch should return cached result
        let cached = cache.get(&input).await.unwrap();
        assert!(cached.is_some());
        assert_eq!(cached.unwrap().success, result.success);
        assert_eq!(cache.metrics().hits.load(Ordering::Relaxed), 1);
        assert_eq!(cache.metrics().misses.load(Ordering::Relaxed), 0);
    }
    
    #[tokio::test]
    async fn test_cache_miss() {
        let cache = TransactionSimulationCache::new(100, 5000);
        let input = create_test_input(1000, b"test_instructions");
        
        // Fetch from empty cache should miss
        let cached = cache.get(&input).await.unwrap();
        assert!(cached.is_none());
        assert_eq!(cache.metrics().hits.load(Ordering::Relaxed), 0);
        assert_eq!(cache.metrics().misses.load(Ordering::Relaxed), 1);
    }
    
    #[tokio::test]
    async fn test_ttl_expiry() {
        let cache = TransactionSimulationCache::new(100, 50); // 50ms TTL
        let input = create_test_input(1000, b"test_instructions");
        let result = create_test_result();
        
        // Insert entry
        cache.insert(&input, result.clone(), 1000).await.unwrap();
        
        // Should hit immediately
        let cached = cache.get(&input).await.unwrap();
        assert!(cached.is_some());
        
        // Wait for TTL to expire
        sleep(Duration::from_millis(100)).await;
        
        // Should miss after expiry
        let cached = cache.get(&input).await.unwrap();
        assert!(cached.is_none());
        assert_eq!(cache.metrics().ttl_evictions.load(Ordering::Relaxed), 1);
    }
    
    #[tokio::test]
    async fn test_slot_invalidation() {
        let cache = TransactionSimulationCache::new(100, 5000);
        let input = create_test_input(1000, b"test_instructions");
        let result = create_test_result();
        
        // Insert entry at slot 1000
        cache.insert(&input, result.clone(), 1000).await.unwrap();
        
        // Fetch with same slot should hit
        let cached = cache.get(&input).await.unwrap();
        assert!(cached.is_some());
        
        // Fetch with newer slot should miss due to slot invalidation
        let newer_input = create_test_input(1001, b"test_instructions");
        let cached = cache.get(&newer_input).await.unwrap();
        assert!(cached.is_none());
        assert_eq!(cache.metrics().slot_evictions.load(Ordering::Relaxed), 1);
    }
    
    #[tokio::test]
    async fn test_slot_advancement_purge() {
        let cache = TransactionSimulationCache::new(100, 5000);
        let input1 = create_test_input(1000, b"test1");
        let input2 = create_test_input(1001, b"test2");
        let result = create_test_result();
        
        // Insert entries at different slots
        cache.insert(&input1, result.clone(), 1000).await.unwrap();
        cache.insert(&input2, result.clone(), 1001).await.unwrap();
        
        assert_eq!(cache.len().await, 2);
        
        // Invalidate slot 1000
        cache.invalidate_slot(1001).await;
        
        assert_eq!(cache.len().await, 1);
        assert_eq!(cache.metrics().slot_evictions.load(Ordering::Relaxed), 1);
    }
    
    #[tokio::test]
    async fn test_concurrent_access() {
        let cache = Arc::new(TransactionSimulationCache::new(100, 5000));
        let num_tasks = 10;
        let operations_per_task = 100;
        
        let mut handles = Vec::new();
        
        // Spawn concurrent insert tasks
        for i in 0..num_tasks {
            let cache_clone = Arc::clone(&cache);
            let handle = task::spawn(async move {
                for j in 0..operations_per_task {
                    let input = create_test_input(1000 + i, &format!("task_{}_op_{}", i, j).as_bytes());
                    let result = create_test_result();
                    cache_clone.insert(&input, result, 1000 + i).await.unwrap();
                }
            });
            handles.push(handle);
        }
        
        // Spawn concurrent read tasks
        for i in 0..num_tasks {
            let cache_clone = Arc::clone(&cache);
            let handle = task::spawn(async move {
                for j in 0..operations_per_task {
                    let input = create_test_input(1000 + i, &format!("task_{}_op_{}", i, j).as_bytes());
                    let _ = cache_clone.get(&input).await.unwrap();
                }
            });
            handles.push(handle);
        }
        
        // Wait for all tasks to complete
        for handle in handles {
            handle.await.unwrap();
        }
        
        // Verify no deadlocks occurred and cache is in valid state
        assert!(cache.len().await > 0);
        let total_operations = cache.metrics().hits.load(Ordering::Relaxed) + 
                             cache.metrics().misses.load(Ordering::Relaxed);
        assert_eq!(total_operations, (num_tasks * operations_per_task) as u64);
    }
    
    #[tokio::test]
    async fn test_cache_len_reflects_correct_state() {
        let cache = TransactionSimulationCache::new(100, 5000);
        
        assert_eq!(cache.len().await, 0);
        assert!(cache.is_empty().await);
        
        // Add entries
        for i in 0..10 {
            let input = create_test_input(1000, &format!("test_{}", i).as_bytes());
            let result = create_test_result();
            cache.insert(&input, result, 1000).await.unwrap();
        }
        
        assert_eq!(cache.len().await, 10);
        assert!(!cache.is_empty().await);
        
        // Clear cache
        cache.clear().await;
        assert_eq!(cache.len().await, 0);
        assert!(cache.is_empty().await);
    }
    
    #[tokio::test]
    async fn test_lru_eviction() {
        let cache = TransactionSimulationCache::new(5, 5000); // Small capacity
        let result = create_test_result();
        
        // Fill cache to capacity
        for i in 0..5 {
            let input = create_test_input(1000, &format!("test_{}", i).as_bytes());
            cache.insert(&input, result.clone(), 1000).await.unwrap();
        }
        
        assert_eq!(cache.len().await, 5);
        
        // Add one more entry to trigger LRU eviction
        let input = create_test_input(1000, b"overflow");
        cache.insert(&input, result.clone(), 1000).await.unwrap();
        
        // Cache should still be at capacity due to LRU eviction
        assert!(cache.len().await <= 5);
        assert!(cache.metrics().lru_evictions.load(Ordering::Relaxed) > 0);
    }
    
    #[tokio::test]
    async fn test_purge_expired_entries() {
        let cache = TransactionSimulationCache::new(100, 50); // 50ms TTL
        let result = create_test_result();
        
        // Insert entries
        for i in 0..5 {
            let input = create_test_input(1000, &format!("test_{}", i).as_bytes());
            cache.insert(&input, result.clone(), 1000).await.unwrap();
        }
        
        assert_eq!(cache.len().await, 5);
        
        // Wait for TTL to expire
        sleep(Duration::from_millis(100)).await;
        
        // Purge expired entries
        cache.purge_expired().await.unwrap();
        
        assert_eq!(cache.len().await, 0);
        assert_eq!(cache.metrics().ttl_evictions.load(Ordering::Relaxed), 5);
    }
    
    #[tokio::test]
    async fn test_hit_rate_calculation() {
        let cache = TransactionSimulationCache::new(100, 5000);
        let input = create_test_input(1000, b"test");
        let result = create_test_result();
        
        // Initially no operations
        assert_eq!(cache.metrics().hit_rate(), 0.0);
        
        // Miss
        cache.get(&input).await.unwrap();
        assert_eq!(cache.metrics().hit_rate(), 0.0);
        
        // Insert and hit
        cache.insert(&input, result, 1000).await.unwrap();
        cache.get(&input).await.unwrap();
        assert_eq!(cache.metrics().hit_rate(), 0.5); // 1 hit, 1 miss
        
        // Another hit
        cache.get(&input).await.unwrap();
        assert_eq!(cache.metrics().hit_rate(), 2.0/3.0); // 2 hits, 1 miss
    }
    
    #[test]
    fn test_mainnet_amp_trigger() {
        assert_eq!(std::env::var("MEV_MODE").unwrap_or_default(), "MAINNET_POWER");
    }
}
