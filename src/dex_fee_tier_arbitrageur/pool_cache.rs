use dashmap::DashMap;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Protocol type for pool
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Protocol {
    OrcaWhirlpool,
    RaydiumV4,
}

/// Cached pool state
#[derive(Debug, Clone)]
pub struct PoolState {
    pub address: Pubkey,
    pub protocol: Protocol,
    pub token_a: Pubkey,
    pub token_b: Pubkey,
    
    // Common fields
    pub liquidity: u128,
    pub last_update_slot: u64,
    pub last_update_timestamp: u64,
    
    // Orca-specific
    pub fee_rate: u16,           // hundredths of bps
    pub sqrt_price: u128,         // Q64.64
    pub tick_current: i32,
    pub tick_spacing: i32,
    
    // Raydium-specific
    pub reserve_a: u128,
    pub reserve_b: u128,
    pub trade_fee_numerator: u64,
    pub trade_fee_denominator: u64,
    
    // Vault addresses
    pub token_vault_a: Pubkey,
    pub token_vault_b: Pubkey,
    
    // Additional metadata
    pub open_orders: Option<Pubkey>,  // Raydium
    pub market: Option<Pubkey>,       // Raydium
}

impl PoolState {
    /// Get current timestamp
    pub fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }
    
    /// Check if pool data is stale
    pub fn is_stale(&self, max_age_ms: u64) -> bool {
        let now = Self::current_timestamp();
        let age_ms = (now - self.last_update_timestamp) * 1000;
        age_ms > max_age_ms
    }
    
    /// Calculate pool value (simplified)
    pub fn calculate_value(&self) -> u128 {
        match self.protocol {
            Protocol::OrcaWhirlpool => self.liquidity,
            Protocol::RaydiumV4 => self.reserve_a.saturating_add(self.reserve_b),
        }
    }
}

/// Thread-safe pool cache using DashMap
pub struct PoolCache {
    /// All pools by address
    pools: Arc<DashMap<Pubkey, Arc<PoolState>>>,
    
    /// Index by token pair (sorted)
    by_token_pair: Arc<DashMap<(Pubkey, Pubkey), Vec<Pubkey>>>,
    
    /// Index by protocol
    by_protocol: Arc<DashMap<Protocol, Vec<Pubkey>>>,
    
    /// Track last update time
    last_full_update: Arc<DashMap<String, u64>>,
}

impl PoolCache {
    pub fn new() -> Self {
        Self {
            pools: Arc::new(DashMap::new()),
            by_token_pair: Arc::new(DashMap::new()),
            by_protocol: Arc::new(DashMap::new()),
            last_full_update: Arc::new(DashMap::new()),
        }
    }
    
    /// Update or insert a pool
    pub fn update(&self, pool: PoolState) {
        let pool_arc = Arc::new(pool.clone());
        let address = pool.address;
        
        // Update main cache
        self.pools.insert(address, pool_arc.clone());
        
        // Update token pair index (sorted for consistency)
        let pair = Self::normalize_pair(pool.token_a, pool.token_b);
        self.by_token_pair
            .entry(pair)
            .or_insert_with(Vec::new)
            .push(address);
        
        // Update protocol index
        self.by_protocol
            .entry(pool.protocol)
            .or_insert_with(Vec::new)
            .push(address);
        
        // Deduplicate indices periodically
        if self.pools.len() % 100 == 0 {
            self.deduplicate_indices();
        }
    }
    
    /// Batch update multiple pools
    pub fn batch_update(&self, pools: Vec<PoolState>) {
        for pool in pools {
            self.update(pool);
        }
        self.deduplicate_indices();
    }
    
    /// Get a pool by address
    pub fn get(&self, address: &Pubkey) -> Option<Arc<PoolState>> {
        self.pools.get(address).map(|entry| entry.clone())
    }
    
    /// Find pools by token pair
    pub fn find_by_tokens(&self, token_a: &Pubkey, token_b: &Pubkey) -> Vec<Arc<PoolState>> {
        let pair = Self::normalize_pair(*token_a, *token_b);
        
        self.by_token_pair
            .get(&pair)
            .map(|addresses| {
                addresses
                    .iter()
                    .filter_map(|addr| self.get(addr))
                    .collect()
            })
            .unwrap_or_default()
    }
    
    /// Find pools by protocol
    pub fn find_by_protocol(&self, protocol: Protocol) -> Vec<Arc<PoolState>> {
        self.by_protocol
            .get(&protocol)
            .map(|addresses| {
                addresses
                    .iter()
                    .filter_map(|addr| self.get(addr))
                    .collect()
            })
            .unwrap_or_default()
    }
    
    /// Get all pools
    pub fn get_all(&self) -> Vec<Arc<PoolState>> {
        self.pools
            .iter()
            .map(|entry| entry.value().clone())
            .collect()
    }
    
    /// Get pools needing refresh
    pub fn get_stale_pools(&self, max_age_ms: u64) -> Vec<Arc<PoolState>> {
        self.pools
            .iter()
            .map(|entry| entry.value().clone())
            .filter(|pool| pool.is_stale(max_age_ms))
            .collect()
    }
    
    /// Remove a pool
    pub fn remove(&self, address: &Pubkey) -> Option<Arc<PoolState>> {
        if let Some((_, pool)) = self.pools.remove(address) {
            // Clean up indices
            let pair = Self::normalize_pair(pool.token_a, pool.token_b);
            
            if let Some(mut addresses) = self.by_token_pair.get_mut(&pair) {
                addresses.retain(|addr| addr != address);
            }
            
            if let Some(mut addresses) = self.by_protocol.get_mut(&pool.protocol) {
                addresses.retain(|addr| addr != address);
            }
            
            Some(pool)
        } else {
            None
        }
    }
    
    /// Clear all pools
    pub fn clear(&self) {
        self.pools.clear();
        self.by_token_pair.clear();
        self.by_protocol.clear();
        self.last_full_update.clear();
    }
    
    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let total_pools = self.pools.len();
        let mut orca_count = 0;
        let mut raydium_count = 0;
        let mut total_liquidity = 0u128;
        
        for entry in self.pools.iter() {
            let pool = entry.value();
            match pool.protocol {
                Protocol::OrcaWhirlpool => orca_count += 1,
                Protocol::RaydiumV4 => raydium_count += 1,
            }
            total_liquidity = total_liquidity.saturating_add(pool.calculate_value());
        }
        
        CacheStats {
            total_pools,
            orca_count,
            raydium_count,
            unique_token_pairs: self.by_token_pair.len(),
            total_liquidity,
        }
    }
    
    /// Normalize token pair (smaller pubkey first)
    fn normalize_pair(token_a: Pubkey, token_b: Pubkey) -> (Pubkey, Pubkey) {
        if token_a < token_b {
            (token_a, token_b)
        } else {
            (token_b, token_a)
        }
    }
    
    /// Remove duplicate addresses from indices
    fn deduplicate_indices(&self) {
        // Deduplicate token pair index
        for mut entry in self.by_token_pair.iter_mut() {
            let addresses = entry.value_mut();
            addresses.sort();
            addresses.dedup();
        }
        
        // Deduplicate protocol index
        for mut entry in self.by_protocol.iter_mut() {
            let addresses = entry.value_mut();
            addresses.sort();
            addresses.dedup();
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub total_pools: usize,
    pub orca_count: usize,
    pub raydium_count: usize,
    pub unique_token_pairs: usize,
    pub total_liquidity: u128,
}

impl Default for PoolCache {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    fn create_test_pool(address: Pubkey, protocol: Protocol) -> PoolState {
        PoolState {
            address,
            protocol,
            token_a: Pubkey::new_unique(),
            token_b: Pubkey::new_unique(),
            liquidity: 1_000_000,
            last_update_slot: 100,
            last_update_timestamp: PoolState::current_timestamp(),
            fee_rate: 30,
            sqrt_price: 1 << 64,
            tick_current: 0,
            tick_spacing: 64,
            reserve_a: 500_000,
            reserve_b: 500_000,
            trade_fee_numerator: 25,
            trade_fee_denominator: 10000,
            token_vault_a: Pubkey::new_unique(),
            token_vault_b: Pubkey::new_unique(),
            open_orders: None,
            market: None,
        }
    }
    
    #[test]
    fn test_pool_cache_operations() {
        let cache = PoolCache::new();
        
        // Create test pools
        let pool1 = create_test_pool(Pubkey::new_unique(), Protocol::OrcaWhirlpool);
        let pool2 = create_test_pool(Pubkey::new_unique(), Protocol::RaydiumV4);
        
        // Insert pools
        cache.update(pool1.clone());
        cache.update(pool2.clone());
        
        // Test get
        assert!(cache.get(&pool1.address).is_some());
        assert!(cache.get(&pool2.address).is_some());
        
        // Test find by protocol
        let orca_pools = cache.find_by_protocol(Protocol::OrcaWhirlpool);
        assert_eq!(orca_pools.len(), 1);
        
        let raydium_pools = cache.find_by_protocol(Protocol::RaydiumV4);
        assert_eq!(raydium_pools.len(), 1);
        
        // Test stats
        let stats = cache.stats();
        assert_eq!(stats.total_pools, 2);
        assert_eq!(stats.orca_count, 1);
        assert_eq!(stats.raydium_count, 1);
        
        // Test remove
        cache.remove(&pool1.address);
        assert!(cache.get(&pool1.address).is_none());
        
        // Test clear
        cache.clear();
        assert_eq!(cache.get_all().len(), 0);
    }
    
    #[test]
    fn test_staleness_check() {
        let mut pool = create_test_pool(Pubkey::new_unique(), Protocol::OrcaWhirlpool);
        
        // Fresh pool
        assert!(!pool.is_stale(1000));
        
        // Make it stale
        pool.last_update_timestamp = PoolState::current_timestamp() - 10;
        assert!(pool.is_stale(5000));
    }
}
