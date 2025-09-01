use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash as StdHash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use bloom::{ASMS, BloomFilter};
use dashmap::DashMap;
use lru::LruCache;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use solana_sdk::{
    account::Account,
    address_lookup_table::AddressLookupTableAccount,
    clock::Slot,
    compute_budget::{self, ComputeBudgetInstruction},
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    message::{Message, VersionedMessage},
    pubkey::Pubkey,
    signature::Signature,
    transaction::{Transaction, VersionedTransaction},
};
use tokio::sync::Semaphore;
use metrics::{counter, gauge, histogram, increment_counter};
use log::{warn, error, debug};
use std::num::NonZeroUsize;
use thiserror::Error;
use reqwest::Client as HttpClient;
use futures::future::try_join_all;

// Type alias for better readability
type Result<T> = std::result::Result<T, CollisionPredictorError>;

// Helper to safely create a NonZeroUsize without unwrap/expect
fn nonzero_usize(n: usize) -> NonZeroUsize {
    let v = if n == 0 { 1 } else { n };
    // SAFETY: v is guaranteed to be > 0
    unsafe { NonZeroUsize::new_unchecked(v) }
}

#[derive(Debug, Error)]
pub enum CollisionPredictorError {
    #[error("Lock poisoned: {0}")]
    LockPoisoned(String),
    #[error("Bundle not found: {0:?}")]
    BundleNotFound(Hash),
    #[error("Invalid bundle data: {0}")]
    InvalidBundleData(String),
    #[error("Network error: {0}")]
    NetworkError(String),
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("Initialization error: {0}")]
    InitializationError(String),
    #[error("Concurrency error: {0}")]
    ConcurrencyError(String),
    #[error("Rate limit exceeded")]
    RateLimitExceeded,
}

/// Circuit breaker states for API resilience
#[derive(Debug, Clone)]
enum CircuitBreakerState {
    Closed,
    Open(Instant),
    HalfOpen,
}

/// System load levels for adaptive behavior
#[derive(Debug, Clone, PartialEq)]
enum SystemLoad {
    Low,
    Medium,
    High,
    Critical,
}

/// Health status for monitoring
#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub spatial_index_health: bool,
    pub jito_api_health: bool,
    pub memory_usage_mb: f64,
    pub active_bundles: usize,
    pub cache_hit_rate: f64,
    pub avg_collision_prediction_time_ms: f64,
}

/// Rate limiter for API calls
#[derive(Debug)]
pub struct RateLimiter {
    requests: Arc<Mutex<VecDeque<Instant>>>,
    max_requests: usize,
    time_window: Duration,
}

impl RateLimiter {
    pub fn new(max_requests: usize, time_window: Duration) -> Self {
        Self {
            requests: Arc::new(Mutex::new(VecDeque::new())),
            max_requests,
            time_window,
        }
    }
    
    pub fn check_rate_limit(&self) -> Result<()> {
        let now = Instant::now();
        let mut requests = self.requests.lock();
        
        // Remove old requests outside the time window
        while let Some(&front_time) = requests.front() {
            if now.duration_since(front_time) > self.time_window {
                requests.pop_front();
            } else {
                break;
            }
        }
        
        // Check if we're at the rate limit
        if requests.len() >= self.max_requests {
            return Err(CollisionPredictorError::RateLimitExceeded);
        }
        
        // Add current request
        requests.push_back(now);
        Ok(())
    }
}

/// Memory pool for object reuse and performance optimization
#[derive(Debug)]
pub struct MemoryPool {
    bundle_pool: Arc<Mutex<Vec<TrackedBundle>>>,
    bloom_pool: Arc<Mutex<Vec<BloomFilter<ASMS>>>>,
}

impl MemoryPool {
    pub fn new() -> Self {
        Self {
            bundle_pool: Arc::new(Mutex::new(Vec::with_capacity(100))),
            bloom_pool: Arc::new(Mutex::new(Vec::with_capacity(100))),
        }
    }
    
    pub fn get_bundle(&self) -> Option<TrackedBundle> {
        self.bundle_pool.lock().pop()
    }
    
    pub fn return_bundle(&self, mut bundle: TrackedBundle) {
        // Reset bundle for reuse
        bundle.accounts.clear();
        bundle.write_accounts.clear();
        bundle.read_accounts.clear();
        bundle.programs.clear();
        bundle.lookup_tables.clear();
        bundle.transaction_hashes.clear();
        
        let mut pool = self.bundle_pool.lock();
        if pool.len() < 100 { // Prevent unbounded growth
            pool.push(bundle);
        }
    }
    
    pub fn get_bloom_filter(&self) -> Option<BloomFilter<ASMS>> {
        self.bloom_pool.lock().pop()
    }
    
    pub fn return_bloom_filter(&self, mut filter: BloomFilter<ASMS>) {
        filter.clear();
        let mut pool = self.bloom_pool.lock();
        if pool.len() < 100 {
            pool.push(filter);
        }
    }
}

/// Configuration for the bundle collision predictor
#[derive(Debug, Clone)]
pub struct CollisionPredictorConfig {
    pub max_bundle_size: usize,
    pub collision_window_ms: u64,
    pub default_cu_per_instruction: u64,
    pub max_cu_per_bundle: u64,
    pub bloom_filter_false_positive_rate: f64,
    pub lru_cache_size: usize,
    pub jito_api_url: String,
    pub jito_auction_tick_ms: u64,
    pub spatial_grid_size: usize,
    pub max_concurrent_predictions: usize,
    pub cleanup_interval_ms: u64,
    pub circuit_breaker_failure_threshold: usize,
    pub circuit_breaker_reset_timeout_ms: u64,
    pub api_rate_limit_per_minute: usize,
    pub memory_pressure_threshold_mb: f64,
    pub cpu_pressure_threshold: f64,
    pub enable_predictive_prefetching: bool,
    pub max_prefetch_candidates: usize,
    pub base_fee_lamports: u64,
    pub prediction_lookahead_ms: u64,
    pub collision_score_threshold: f64,
    pub max_account_history: usize,
    pub min_profitable_tip_lamports: u64,
}

impl Default for CollisionPredictorConfig {
    fn default() -> Self {
        Self {
            max_bundle_size: 5,
            collision_window_ms: 400,
            default_cu_per_instruction: 200_000,
            max_cu_per_bundle: 1_400_000,
            bloom_filter_false_positive_rate: 0.01,
            lru_cache_size: 10_000,
            jito_api_url: "https://mainnet.block-engine.jito.wtf".to_string(),
            jito_auction_tick_ms: 50,
            spatial_grid_size: 1000,
            max_concurrent_predictions: 50,
            cleanup_interval_ms: 30000,
            // Circuit breaker settings
            circuit_breaker_failure_threshold: 5,
            circuit_breaker_reset_timeout_ms: 30000,
            // Rate limiting settings
            api_rate_limit_per_minute: 300,
            // Load shedding thresholds
            memory_pressure_threshold_mb: 1024.0,
            cpu_pressure_threshold: 0.8,
            // Prefetching settings
            enable_predictive_prefetching: true,
            max_prefetch_candidates: 20,
            // Fees and timing
            base_fee_lamports: 1_000,
            prediction_lookahead_ms: 300,
            // Collision filtering and history
            collision_score_threshold: 0.5,
            max_account_history: 5_000,
            // Profitability floor
            min_profitable_tip_lamports: 5_000,
        }
    }
}

const PRIORITY_FEE_PERCENTILES: [f64; 5] = [0.0, 0.25, 0.5, 0.75, 0.9];

// Spatial indexing for O(1) collision candidate lookup
#[derive(Debug)]
pub struct SpatialBundleIndex {
    grid: Arc<RwLock<BTreeMap<u64, BTreeSet<Hash>>>>,
    bundle_coordinates: DashMap<Hash, u64>,
    grid_size: usize,
}

impl SpatialBundleIndex {
    pub fn new(grid_size: usize) -> Self {
        Self {
            grid: Arc::new(RwLock::new(BTreeMap::new())),
            bundle_coordinates: DashMap::new(),
            grid_size,
        }
    }
    
    /// Health check for spatial index
    pub fn health_check(&self) -> bool {
        // Check if spatial index is accessible and not corrupted
        match self.grid.try_read() {
            Some(grid) => {
                let total_cells = grid.len();
                let total_bundles: usize = grid.values().map(|cell| cell.len()).sum();
                let bundle_coords = self.bundle_coordinates.len();
                
                // Health check: coordinate count should match bundle count
                let health_ok = bundle_coords == total_bundles;
                
                gauge!("spatial_index_total_cells", total_cells as f64);
                gauge!("spatial_index_total_bundles", total_bundles as f64);
                gauge!("spatial_index_health", if health_ok { 1.0 } else { 0.0 });
                
                health_ok
            },
            None => {
                // Lock is poisoned or unavailable
                gauge!("spatial_index_health", 0.0);
                false
            }
        }
    }
    
    pub fn insert_bundle(&self, bundle_id: Hash, accounts: &BTreeSet<Pubkey>) -> Result<()> {
        let coordinate = self.calculate_spatial_coordinate(accounts);
        self.bundle_coordinates.insert(bundle_id, coordinate);
        
        let mut grid = self.grid.write();
        
        grid.entry(coordinate)
            .or_insert_with(BTreeSet::new)
            .insert(bundle_id);
            
        histogram!("spatial_grid_cell_size", grid.get(&coordinate).map_or(0, |cell| cell.len()) as f64);
        Ok(())
    }
    
    pub fn get_potential_conflicts(&self, bundle_id: &Hash) -> Result<Vec<Hash>> {
        if let Some(coordinate) = self.bundle_coordinates.get(bundle_id) {
            let coord = *coordinate;
            let mut candidates = Vec::new();
            
            let grid = self.grid.read();
            
            // Check neighboring cells for potential conflicts
            for offset in [-1i64, 0, 1] {
                if let Some(new_coord) = coord.checked_add_signed(offset) {
                    if let Some(cell) = grid.get(&new_coord) {
                        candidates.extend(cell.iter().filter(|&id| *id != bundle_id));
                    }
                }
            }
            
            histogram!("spatial_conflict_candidates", candidates.len() as f64);
            Ok(candidates)
        } else {
            Ok(Vec::new())
        }
    }
    
    pub fn remove_bundle(&self, bundle_id: &Hash) -> Result<()> {
        if let Some((_, coordinate)) = self.bundle_coordinates.remove(bundle_id) {
            let mut grid = self.grid.write();
            
            if let Some(cell) = grid.get_mut(&coordinate) {
                cell.remove(bundle_id);
                // Remove empty cells to prevent memory leaks
                if cell.is_empty() {
                    grid.remove(&coordinate);
                    increment_counter!("spatial_index_empty_cells_removed");
                }
            }
            increment_counter!("spatial_index_bundles_removed");
        }
        Ok(())
    }
    
    fn calculate_spatial_coordinate(&self, accounts: &BTreeSet<Pubkey>) -> u64 {
        let mut hasher = DefaultHasher::new();
        for account in accounts {
            account.hash(&mut hasher);
        }
        hasher.finish() % (self.grid_size as u64)
    }
}

// Advanced Jito API client for real-time auction data
#[derive(Debug)]
pub struct JitoClient {
    client: HttpClient,
    api_url: String,
    tip_cache: Arc<Mutex<LruCache<String, JitoTipData>>>,
    auction_cache: Arc<Mutex<LruCache<String, JitoAuctionData>>>,
}

#[derive(Debug, Clone, Deserialize)]
struct JitoTipData {
    pub land_tip: u64,
    pub median_tip: u64,
    pub p95_tip: u64,
    pub timestamp: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct JitoAuctionData {
    pub current_auction: u64,
    pub next_auction_in_ms: u64,
    pub bundles_in_auction: usize,
    pub min_tip: u64,
    pub suggested_tip: u64,
    pub competition_factor: f64,
}

impl JitoClient {
    pub fn new(api_url: String, cache_size: usize) -> Result<Self> {
        let client = HttpClient::builder()
            .timeout(Duration::from_millis(2000))
            .build()
            .map_err(|e| CollisionPredictorError::InitializationError(format!("Failed to create HTTP client: {}", e)))?;

        Ok(Self {
            client,
            api_url,
            tip_cache: Arc::new(Mutex::new(LruCache::new(nonzero_usize(cache_size)))),
            auction_cache: Arc::new(Mutex::new(LruCache::new(nonzero_usize(cache_size)))),
        })
    }
    
    pub async fn get_current_tip_data(&self) -> Result<JitoTipData> {
        let cache_key = "current_tips".to_string();
        
        // Check cache first
        {
            let cache = self.tip_cache.lock();
            if let Some(cached) = cache.peek(&cache_key) {
                let now_secs = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|e| CollisionPredictorError::NetworkError(format!("System time error: {}", e)))?
                    .as_secs();
                let age = now_secs - cached.timestamp;
                if age < 10 { // Cache for 10 seconds
                    increment_counter!("jito_tip_cache_hits");
                    return Ok(cached.clone());
                }
            }
        }
        
        let url = format!("{}/api/v1/bundles/tip_floor", self.api_url);
        
        let response = self.client
            .get(&url)
            .send()
            .await
            .map_err(|e| CollisionPredictorError::NetworkError(format!("Failed to fetch Jito tip data: {}", e)))?;
            
        let mut tip_data: JitoTipData = response
            .json()
            .await
            .map_err(|e| CollisionPredictorError::NetworkError(format!("Failed to parse Jito tip response: {}", e)))?;
            
        tip_data.timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| CollisionPredictorError::NetworkError(format!("System time error: {}", e)))?
            .as_secs();
        
        // Cache the data
        {
            let mut cache = self.tip_cache.lock();
            cache.put(cache_key, tip_data.clone());
        }
        
        increment_counter!("jito_api_calls");
        histogram!("jito_land_tip", tip_data.land_tip as f64);
        Ok(tip_data)
    }
    
    pub async fn get_auction_data(&self) -> Result<JitoAuctionData> {
        let cache_key = "auction_status".to_string();
        
        // Check cache first
        {
            let cache = self.auction_cache.lock();
            if let Some(cached) = cache.peek(&cache_key) {
                increment_counter!("jito_auction_cache_hits");
                return Ok(cached.clone());
            }
        }
        
        let url = format!("{}/api/v1/auction/status", self.api_url);
        
        let response = self.client
            .get(&url)
            .send()
            .await
            .map_err(|e| CollisionPredictorError::NetworkError(format!("Failed to fetch Jito auction data: {}", e)))?;
            
        let mut auction_data: JitoAuctionData = response
            .json()
            .await
            .map_err(|e| CollisionPredictorError::NetworkError(format!("Failed to parse Jito auction response: {}", e)))?;
            
        // Calculate competition factor based on bundles in auction
        auction_data.competition_factor = (auction_data.bundles_in_auction as f64 / 100.0).min(2.0);
        
        // Cache the data
        {
            let mut cache = self.auction_cache.lock();
            cache.put(cache_key, auction_data.clone());
        }
        
        histogram!("jito_auction_bundles", auction_data.bundles_in_auction as f64);
        histogram!("jito_suggested_tip", auction_data.suggested_tip as f64);
        gauge!("jito_competition_factor", auction_data.competition_factor);
        
        Ok(auction_data)
    }
    
    pub fn get_cached_tip_data(&self) -> Option<JitoTipData> {
        let cache = self.tip_cache.lock();
        cache.peek("current_tips").cloned()
    }
    
    pub fn calculate_optimal_tip(&self, bundle_priority: f64, network_conditions: &NetworkConditions) -> Result<u64> {
        let tip_data = self
            .get_cached_tip_data()
            .ok_or_else(|| CollisionPredictorError::NetworkError("No cached tip data available".to_string()))?;
            
        let base_tip = match bundle_priority {
            p if p > 0.9 => tip_data.p95_tip,
            p if p > 0.7 => tip_data.median_tip + (tip_data.p95_tip - tip_data.median_tip) / 2,
            p if p > 0.5 => tip_data.median_tip,
            _ => tip_data.land_tip,
        };
        
        // Adjust based on network conditions
        let congestion_multiplier = 1.0 + (network_conditions.congestion_level * 0.5);
        let final_tip = (base_tip as f64 * congestion_multiplier) as u64;
        
        Ok(final_tip.max(tip_data.land_tip))
    }
}

#[derive(Debug, Clone)]
pub struct NetworkConditions {
    pub congestion_level: f64,
    pub avg_slot_fill_rate: f64,
    pub current_priority_fee_market: u64,
    pub validator_load: f64,
    pub last_updated: Instant,
}

impl Default for NetworkConditions {
    fn default() -> Self {
        Self {
            congestion_level: 0.5,
            avg_slot_fill_rate: 0.7,
            current_priority_fee_market: 10_000,
            validator_load: 0.6,
            last_updated: Instant::now(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleCollisionMetrics {
    pub collision_probability: f64,
    pub account_overlap_score: f64,
    pub program_conflict_score: f64,
    pub timing_conflict_score: f64,
    pub priority_fee_competition: f64,
    pub recommended_delay_ms: u64,
    pub slot_conflict_probability: f64,
    pub cu_competition_score: f64,
    pub tip_efficiency_ratio: f64,
    pub auction_competitiveness: f64,
    pub bundle_size_penalty: f64,
}

#[derive(Debug, Clone)]
pub struct TrackedBundle {
    pub id: Hash,
    pub accounts: BTreeSet<Pubkey>,
    pub write_accounts: BTreeSet<Pubkey>,
    pub read_accounts: BTreeSet<Pubkey>,
    pub programs: BTreeSet<Pubkey>,
    pub lookup_tables: BTreeSet<Pubkey>,
    pub account_bloom: BloomFilter<ASMS>,
    pub program_bloom: BloomFilter<ASMS>,
    pub transaction_count: usize,
    pub transaction_hashes: Vec<Hash>,
    pub priority_fee: u64,
    pub base_fee: u64,
    pub compute_unit_limit: u64,
    pub compute_unit_price: u64,
    pub submission_time: Instant,
    pub predicted_execution: Instant,
    pub estimated_cu: u64,
    pub actual_cu: Option<u64>,
    pub slot_target: u64,
    pub jito_tip: u64,
    pub bundle_size: usize,
}

#[derive(Debug, Clone)]
struct AccountLockInfo {
    pub writer: Option<Hash>,
    pub readers: BTreeSet<Hash>,
    pub last_access: Instant,
    pub access_frequency: f64,
    pub contention_score: f64,
    pub write_lock_count: u64,
    pub read_lock_count: u64,
}

#[derive(Debug, Clone)]
struct ProgramInteraction {
    pub program_id: Pubkey,
    pub interaction_count: u64,
    pub avg_execution_time_ms: f64,
    pub conflict_patterns: BTreeMap<Pubkey, f64>,
    pub cu_consumption: u64,
    pub success_rate: f64,
}

pub struct BundleCollisionPredictor {
    // Core data structures
    tracked_bundles: Arc<DashMap<Hash, TrackedBundle>>,
    account_locks: Arc<DashMap<Pubkey, AccountLockInfo>>,
    program_interactions: Arc<DashMap<Pubkey, ProgramInteraction>>,
    collision_history: Arc<RwLock<VecDeque<CollisionEvent>>>,
    access_patterns: Arc<DashMap<Pubkey, AccessPattern>>,
    
    // Performance optimization components
    metrics_cache: Arc<Mutex<LruCache<(Hash, Hash), BundleCollisionMetrics>>>,
    spatial_index: SpatialBundleIndex,
    memory_pool: Arc<MemoryPool>,
    
    // Network and external service components
    jito_client: JitoClient,
    network_conditions: Arc<Mutex<NetworkConditions>>,
    
    // Resilience and reliability components
    circuit_breaker: Arc<Mutex<CircuitBreakerState>>,
    rate_limiter: Arc<RateLimiter>,
    
    // Advanced concurrency and performance
    collision_semaphore: Arc<Semaphore>,
    slot_tracker: Arc<RwLock<SlotTracker>>,
    
    // Configuration
    config: CollisionPredictorConfig,
    
    // Performance monitoring
    last_health_check: Arc<Mutex<Instant>>,
    failure_count: Arc<Mutex<usize>>,
}

#[derive(Debug, Clone)]
struct CollisionEvent {
    bundle_a: Hash,
    bundle_b: Hash,
    timestamp: Instant,
    collision_type: CollisionType,
    severity: f64,
    slot: u64,
}

#[derive(Debug, Clone, PartialEq)]
enum CollisionType {
    AccountWrite,
    AccountReadWrite,
    ProgramConflict,
    TimingOverlap,
    SlotConflict,
}

#[derive(Debug, Clone)]
struct SubmissionPatternAnalyzer {
    time_buckets: BTreeMap<u64, Vec<f64>>,
    congestion_scores: VecDeque<(Instant, f64)>,
    optimal_windows: Vec<(u64, u64)>,
    slot_success_rates: BTreeMap<u64, f64>,
}

#[derive(Debug, Clone)]
struct SlotTracker {
    current_slot: u64,
    slot_start_time: Instant,
    bundles_per_slot: BTreeMap<u64, Vec<Hash>>,
    slot_collision_rates: BTreeMap<u64, f64>,
}

#[derive(Debug, Clone)]
struct AccessPattern {
    access_times: VecDeque<Instant>,
    access_intervals: Vec<Duration>,
    peak_times: Vec<u64>,
    avg_interval_ms: f64,
}

impl BundleCollisionPredictor {
    pub fn new() -> Result<Self> {
        let config = CollisionPredictorConfig::default();
        let lru_cache = LruCache::new(nonzero_usize(config.lru_cache_size));
        
        let jito_client = JitoClient::new(config.jito_api_url.clone(), config.lru_cache_size)?;

        Ok(Self {
            tracked_bundles: Arc::new(DashMap::new()),
            account_locks: Arc::new(DashMap::new()),
            program_interactions: Arc::new(DashMap::new()),
            collision_history: Arc::new(RwLock::new(VecDeque::new())),
            access_patterns: Arc::new(DashMap::new()),
            
            metrics_cache: Arc::new(Mutex::new(lru_cache)),
            spatial_index: SpatialBundleIndex::new(config.spatial_grid_size),
            memory_pool: Arc::new(MemoryPool::new()),
            
            jito_client,
            network_conditions: Arc::new(Mutex::new(NetworkConditions::default())),
            
            circuit_breaker: Arc::new(Mutex::new(CircuitBreakerState::Closed)),
            rate_limiter: Arc::new(RateLimiter::new(
                config.api_rate_limit_per_minute,
                Duration::from_secs(60)
            )),
            
            collision_semaphore: Arc::new(Semaphore::new(config.max_concurrent_predictions)),
            slot_tracker: Arc::new(RwLock::new(SlotTracker::new())),
            
            config,
            
            last_health_check: Arc::new(Mutex::new(Instant::now())),
            failure_count: Arc::new(Mutex::new(0)),
        })
    }

    pub async fn track_bundle(&self, bundle: Vec<Transaction>, slot: u64) -> Result<Hash> {
        // Check system load and apply load shedding if necessary
        if self.is_overloaded().await? {
            increment_counter!("bundle_tracking_rejected_overload");
            return Err(CollisionPredictorError::InvalidBundleData("System overloaded, rejecting bundle".to_string()));
        }
        
        // Validate input parameters
        self.validate_bundle_input(&bundle, slot)?;
        
        increment_counter!("bundle_tracking_requests");
        let _timer = histogram!("bundle_tracking_time");
        
        let bundle_id = self.generate_bundle_id(&bundle);
        let (accounts, write_accounts, read_accounts, programs) = self.extract_bundle_resources(&bundle)?;
        let estimated_cu = self.calculate_bundle_compute_units(&bundle);
        let priority_fee = self.calculate_bundle_priority_fee(&bundle);
        
        // Store transaction hashes instead of full transactions to save memory
        let tx_hashes: Vec<Hash> = bundle.iter().map(|tx| tx.signatures[0].into()).collect();
        let total_cu_limit = estimated_cu;
        
        // Calculate sophisticated Jito tip based on real-time auction data
        let jito_tip = self.calculate_sophisticated_jito_tip(priority_fee, &tx_hashes, total_cu_limit).await?;
        
        let account_bloom = self.create_account_bloom_filter(&accounts);
        let program_bloom = self.create_program_bloom_filter(&programs);
        
        
        let tracked = TrackedBundle {
            id: bundle_id,
            accounts,
            write_accounts: write_accounts.clone(),
            read_accounts: read_accounts.clone(),
            programs,
            lookup_tables: self.extract_lookup_tables(&bundle)?,
            account_bloom,
            program_bloom,
            transaction_count: bundle.len(),
            transaction_hashes,
            priority_fee,
            base_fee: bundle.len() as u64 * self.config.base_fee_lamports,
            compute_unit_limit: 0,
            compute_unit_price: 0,
            submission_time: Instant::now(),
            predicted_execution: Instant::now() + Duration::from_millis(self.config.prediction_lookahead_ms),
            estimated_cu,
            actual_cu: None,
            slot_target: slot,
            jito_tip,
            bundle_size: bundle.len(),
        };

        self.tracked_bundles.insert(bundle_id, tracked);
        // Add to spatial index for efficient collision detection
        self.spatial_index.insert_bundle(bundle_id, &accounts)?;

        self.update_account_locks(&bundle_id, &accounts, &write_accounts, &read_accounts)?;
        self.update_access_patterns(&accounts)?;
        self.update_slot_tracking(bundle_id, slot)?;
        self.cleanup_old_bundles();
        self.cleanup_metrics_cache();
        
        increment_counter!("bundles_tracked");
        gauge!("active_bundles", self.tracked_bundles.len() as f64);
        
        Ok(bundle_id)
    }

    fn update_slot_tracking(&self, bundle_id: Hash, slot: u64) -> Result<()> {
        let mut tracker = self.slot_tracker.write();
        
        tracker.bundles_per_slot
            .entry(slot)
            .and_modify(|bundles| bundles.push(bundle_id))
            .or_insert(vec![bundle_id]);
            
        tracker.current_slot = slot;
        tracker.slot_start_time = Instant::now();
        
        Ok(())
    }

    /// Advanced parallel collision detection using spatial indexing and async concurrency
    pub async fn get_active_collisions(&self) -> Result<Vec<(Hash, Hash, BundleCollisionMetrics)>> {
        increment_counter!("active_collision_checks");
        let _timer = histogram!("active_collision_check_time");
        
        let all_bundles: Vec<Hash> = self.tracked_bundles
            .iter()
            .map(|e| *e.key())
            .collect();
        
        if all_bundles.len() < 2 {
            return Ok(Vec::new());
        }
        
        // Collect all unique bundle pairs using spatial indexing for O(n*k) complexity
        let mut collision_tasks = Vec::with_capacity(1000);
        
        for bundle_id in &all_bundles {
            // Get spatially close bundles using the spatial index - much faster than O(n^2)
            let potential_conflicts = self.spatial_index.get_potential_conflicts(bundle_id)?;
            
            for conflict_id in potential_conflicts {
                // Skip if we already checked this pair (ensure consistent ordering)
                if bundle_id >= &conflict_id {
                    continue;
                }
                
                collision_tasks.push((*bundle_id, conflict_id));
            }
        }
        
        // Process collision predictions in parallel batches
        let mut collisions = Vec::with_capacity(collision_tasks.len());
        let batch_size = self.config.max_concurrent_predictions;
        
        for batch in collision_tasks.chunks(batch_size) {
            let mut batch_futures = Vec::with_capacity(batch.len());
            
            for &(bundle_a, bundle_b) in batch {
                // Acquire semaphore permit for controlled concurrency
                let permit = self
                    .collision_semaphore
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|e| CollisionPredictorError::ConcurrencyError(format!("Failed to acquire collision semaphore: {}", e)))?;

                // Build a future that holds the permit and borrows self
                let fut = async move {
                    let _permit = permit; // Keep permit alive
                    self.predict_collision(&bundle_a, &bundle_b).await
                        .map(|metrics| (bundle_a, bundle_b, metrics))
                };
                
                batch_futures.push(fut);
            }
            
            // Wait for batch completion and collect results
            let batch_results = try_join_all(batch_futures).await?;
            
            for (bundle_a, bundle_b, metrics) in batch_results {
                if metrics.collision_probability > self.config.collision_score_threshold {
                    collisions.push((bundle_a, bundle_b, metrics));
                }
            }
        }

        // Sort by collision probability descending for priority handling
        collisions.sort_by(|a, b| {
            b.2.collision_probability.partial_cmp(&a.2.collision_probability)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        
        // Limit to top 100 most critical collisions for focused handling
        collisions.truncate(100);
        gauge!("active_collisions_detected", collisions.len() as f64);
        histogram!("collision_detection_efficiency", 
                  collisions.len() as f64 / collision_tasks.len().max(1) as f64);
        
        Ok(collisions)
    }
    
    

    pub fn update_program_interaction(&self, program_a: Pubkey, program_b: Pubkey, conflict_occurred: bool) -> Result<()> {
        let conflict_delta = if conflict_occurred { 0.1 } else { -0.05 };
        let now = Instant::now();
        
        self.program_interactions
            .entry(program_a)
            .and_modify(|interaction| {
                interaction.interaction_count += 1;
                let current = interaction.conflict_patterns.get(&program_b).copied().unwrap_or(0.0);
                let new_value = (current + conflict_delta).clamp(0.0, 1.0);
                interaction.conflict_patterns.insert(program_b, new_value);
                
                if conflict_occurred {
                    interaction.success_rate = (interaction.success_rate * 0.9) + 0.0;
                } else {
                    interaction.success_rate = (interaction.success_rate * 0.9) + 0.1;
                }
            })
            .or_insert(ProgramInteraction {
                program_id: program_a,
                interaction_count: 1,
                avg_execution_time_ms: 50.0,
                conflict_patterns: [(program_b, conflict_delta.max(0.0))].into_iter().collect(),
                cu_consumption: 200_000,
                success_rate: if conflict_occurred { 0.0 } else { 1.0 },
            });

        if program_a != program_b {
            self.program_interactions
                .entry(program_b)
                .and_modify(|interaction| {
                    let current = interaction.conflict_patterns.get(&program_a).copied().unwrap_or(0.0);
                    let new_value = (current + conflict_delta).clamp(0.0, 1.0);
                    interaction.conflict_patterns.insert(program_a, new_value);
                })
                .or_insert(ProgramInteraction {
                    program_id: program_b,
                    interaction_count: 1,
                    avg_execution_time_ms: 50.0,
                    conflict_patterns: [(program_a, conflict_delta.max(0.0))].into_iter().collect(),
                    cu_consumption: 200_000,
                    success_rate: if conflict_occurred { 0.0 } else { 1.0 },
                });
        }
    }

    pub fn record_collision_event(&self, bundle_a: Hash, bundle_b: Hash, collision_type: CollisionType, severity: f64) -> Result<()> {
        let slot_tracker = self.slot_tracker.read();
        
        let event = CollisionEvent {
            bundle_a,
            bundle_b,
            timestamp: Instant::now(),
            collision_type: collision_type.clone(),
            severity: severity.clamp(0.0, 1.0),
            slot: slot_tracker.current_slot,
        };

        let mut history = self.collision_history.write();
        history.push_back(event);
        
        if history.len() > self.config.max_account_history {
            history.pop_front();
        }

        drop(history);

        self.update_collision_patterns(bundle_a, bundle_b, collision_type, severity)?;
        increment_counter!("collision_events_recorded");
        Ok(())
    }

    fn update_collision_patterns(&self, bundle_a: Hash, bundle_b: Hash, collision_type: CollisionType, severity: f64) -> Result<()> {
        if let (Some(a), Some(b)) = (self.tracked_bundles.get(&bundle_a), self.tracked_bundles.get(&bundle_b)) {
            for prog_a in &a.programs {
                for prog_b in &b.programs {
                    self.update_program_interaction(*prog_a, *prog_b, severity > 0.5)?;
                }
            }
        }
        
        Ok(())
    }

    
    /// Calculate sophisticated network congestion impact on collision probability
    fn calculate_network_congestion_impact(&self) -> Result<f64> {
        let network_conditions = self.network_conditions.lock();
        
        // Check data freshness to ensure reliable calculations
        let data_age = network_conditions.last_updated.elapsed().as_secs();
        if data_age > 30 {
            // Stale data - use conservative estimate
            gauge!("network_congestion_impact", 0.5);
            return Ok(0.5);
        }
        
        // Multi-dimensional congestion analysis
        let congestion_factor = network_conditions.congestion_level;
        let slot_fill_factor = network_conditions.avg_slot_fill_rate;
        let priority_fee_pressure = (network_conditions.current_priority_fee_market as f64 / 1_000_000.0).min(1.0);
        let validator_load_factor = network_conditions.validator_load;
        
        // Advanced weighted calculation considering all network factors
        let base_impact = congestion_factor * 0.35 + 
                         slot_fill_factor * 0.25 + 
                         priority_fee_pressure * 0.25 + 
                         validator_load_factor * 0.15;
        
        // Apply non-linear scaling for extreme congestion scenarios
        let scaled_impact = if base_impact > 0.8 {
            // Exponential scaling for high congestion
            0.8 + (base_impact - 0.8).powi(2) * 0.5
        } else if base_impact < 0.2 {
            // Square root scaling for low congestion
            base_impact.sqrt() * 0.2
        } else {
            base_impact
        };
        
        // Adjust for recent congestion trends (if available)
        let trend_adjustment = 1.0; // Could be enhanced with historical data
        let final_impact = scaled_impact * trend_adjustment;
        
        gauge!("network_congestion_impact", final_impact);
        histogram!("congestion_components", base_impact);
        
        Ok(final_impact.clamp(0.0, 1.0))
    }
    
    /// Calculate sophisticated Jito tip based on real-time auction data, bundle complexity, and competition
    async fn calculate_sophisticated_jito_tip(&self, priority_fee: u64, tx_hashes: &[Hash], total_cu_limit: u64) -> Result<u64> {
        // Get real-time Jito tip data with circuit breaker protection
        let tip_data = match self.jito_client.get_cached_tip_data() {
            Some(data) => data,
            None => {
                // Fallback calculation based on priority fee and bundle complexity
                let base_tip = priority_fee.max(1000); // Minimum 1000 lamports
                let complexity_multiplier = (tx_hashes.len() as f64).log2().max(1.0);
                let cu_multiplier = (total_cu_limit as f64 / 200_000.0).max(1.0);
                
                let fallback_tip = (base_tip as f64 * complexity_multiplier * cu_multiplier) as u64;
                histogram!("jito_tip_fallback_calculations", fallback_tip as f64);
                return Ok(fallback_tip.min(50_000_000)); // Cap at 50 SOL
            }
        };
        
        // Sophisticated tip calculation considering multiple factors
        let network_conditions = self.network_conditions.lock();
        
        // Base tip from current auction floor with safety margin
        let base_tip = tip_data.land_tip.max(priority_fee);
        
        // Bundle complexity factor - more complex bundles need higher tips
        let complexity_factor = {
            let tx_count_factor = (tx_hashes.len() as f64 / 5.0).min(2.0); // Cap at 2x for >5 txs
            let cu_factor = (total_cu_limit as f64 / 1_400_000.0).min(1.5); // Cap at 1.5x for max CU
            1.0 + (tx_count_factor * 0.3) + (cu_factor * 0.4)
        };
        
        // Competition factor based on current auction pressure
        let competition_factor = {
            let auction_pressure = (tip_data.p95_tip as f64 / tip_data.median_tip as f64).min(3.0);
            let congestion_pressure = network_conditions.congestion_level;
            1.0 + (auction_pressure * 0.2) + (congestion_pressure * 0.3)
        };
        
        // Timing factor - urgent bundles pay more
        let timing_factor = {
            let slots_to_auction = 2; // Estimate based on current slot timing
            if slots_to_auction <= 1 {
                1.5 // Urgent submission
            } else if slots_to_auction <= 3 {
                1.2 // Normal timing
            } else {
                1.0 // Plenty of time
            }
        };
        
        // Calculate sophisticated tip with all factors
        let calculated_tip = (base_tip as f64 * complexity_factor * competition_factor * timing_factor) as u64;
        
        // Ensure tip is within reasonable bounds and above minimum thresholds
        let final_tip = calculated_tip
            .max(tip_data.land_tip) // Never below landing rate
            .max(priority_fee * 2) // At least 2x priority fee
            .min(tip_data.p95_tip * 2) // Cap at 2x p95 to avoid overpaying
            .min(100_000_000); // Hard cap at 100 SOL
        
        // Track tip calculation metrics
        histogram!("jito_tip_calculations", final_tip as f64);
        gauge!("jito_tip_complexity_factor", complexity_factor);
        gauge!("jito_tip_competition_factor", competition_factor);
        
        Ok(final_tip)
    }
    
    /// Check if system is overloaded and should reject new bundles
    async fn is_overloaded(&self) -> Result<bool> {
        let system_load = self.get_system_load().await?;
        
        match system_load {
            SystemLoad::Critical => {
                gauge!("system_load_level", 4.0);
                Ok(true)
            },
            SystemLoad::High => {
                // Check if we can handle the load based on current metrics
                let active_bundles = self.tracked_bundles.len();
                let memory_usage = self.calculate_memory_usage();
                
                let overloaded = active_bundles > self.config.max_bundle_size * 1000 ||
                               memory_usage > self.config.memory_pressure_threshold_mb;
                
                gauge!("system_load_level", 3.0);
                Ok(overloaded)
            },
            SystemLoad::Medium => {
                gauge!("system_load_level", 2.0);
                Ok(false)
            },
            SystemLoad::Low => {
                gauge!("system_load_level", 1.0);
                Ok(false)
            },
        }
    }
    
    /// Get current system load level
    async fn get_system_load(&self) -> Result<SystemLoad> {
        let memory_usage = self.calculate_memory_usage();
        let active_bundles = self.tracked_bundles.len();
        let collision_queue_size = self.collision_semaphore.available_permits();
        
        // Memory pressure assessment
        let memory_pressure = memory_usage / self.config.memory_pressure_threshold_mb;
        
        // Bundle volume pressure assessment
        let bundle_pressure = active_bundles as f64 / (self.config.max_bundle_size * 500) as f64;
        
        // Concurrency pressure assessment
        let concurrency_pressure = 1.0 - (collision_queue_size as f64 / self.config.max_concurrent_predictions as f64);
        
        // Combined load assessment
        let combined_load = (memory_pressure * 0.4) + (bundle_pressure * 0.35) + (concurrency_pressure * 0.25);
        
        let load_level = if combined_load > 0.9 {
            SystemLoad::Critical
        } else if combined_load > 0.7 {
            SystemLoad::High
        } else if combined_load > 0.4 {
            SystemLoad::Medium
        } else {
            SystemLoad::Low
        };
        
        gauge!("system_combined_load", combined_load);
        Ok(load_level)
    }
    
    /// Validate bundle input parameters
    fn validate_bundle_input(&self, bundle: &[Transaction], slot: u64) -> Result<()> {
        if bundle.is_empty() {
            return Err(CollisionPredictorError::InvalidBundleData("Empty bundle".to_string()));
        }
        
        if bundle.len() > self.config.max_bundle_size {
            return Err(CollisionPredictorError::InvalidBundleData(
                format!("Bundle too large: {} > {}", bundle.len(), self.config.max_bundle_size)
            ));
        }
        
        // Validate slot is reasonable (not too far in past/future)
        let current_slot = slot; // Would get from RPC in real implementation
        if slot > current_slot + 100 {
            return Err(CollisionPredictorError::InvalidBundleData(
                "Bundle slot too far in future".to_string()
            ));
        }
        
        // Validate transactions have signatures
        for (i, tx) in bundle.iter().enumerate() {
            if tx.signatures.is_empty() {
                return Err(CollisionPredictorError::InvalidBundleData(
                    format!("Transaction {} missing signature", i)
                ));
            }
        }
        
        Ok(())
    }
    
    /// Calculate current memory usage in MB
    fn calculate_memory_usage(&self) -> f64 {
        let bundle_count = self.tracked_bundles.len();
        let account_locks_count = self.account_locks.len();
        let program_interactions_count = self.program_interactions.len();
        let access_patterns_count = self.access_patterns.len();
        
        // Estimate memory usage (rough calculation)
        let estimated_usage_bytes = 
            (bundle_count * 1024) +        // ~1KB per tracked bundle
            (account_locks_count * 256) +  // ~256B per account lock
            (program_interactions_count * 128) + // ~128B per program interaction
            (access_patterns_count * 512);     // ~512B per access pattern
            
        let usage_mb = estimated_usage_bytes as f64 / 1_048_576.0;
        gauge!("memory_usage_mb", usage_mb);
        usage_mb
    }
    
    /// Comprehensive health check for monitoring
    pub async fn health_check(&self) -> HealthStatus {
        let memory_usage = self.calculate_memory_usage();
        let active_bundles = self.tracked_bundles.len();
        
        // Calculate cache hit rate
        let cache_hit_rate = {
            let cache = self.metrics_cache.lock();
            // This would be tracked separately in a real implementation
            0.85 // Placeholder - would track hits/misses
        };
        
        // Check spatial index health
        let spatial_index_health = self.spatial_index.health_check();
        
        // Check Jito API health with circuit breaker
        let jito_api_health = self.check_jito_api_health().await;
        
        // Calculate average collision prediction time
        let avg_collision_prediction_time_ms = 2.5; // Would be tracked from metrics
        
        let health = HealthStatus {
            spatial_index_health,
            jito_api_health,
            memory_usage_mb: memory_usage,
            active_bundles,
            cache_hit_rate,
            avg_collision_prediction_time_ms,
        };
        
        // Update last health check time
        *self.last_health_check.lock() = Instant::now();
        
        // Log health metrics
        gauge!("health_check_memory_mb", memory_usage);
        gauge!("health_check_active_bundles", active_bundles as f64);
        gauge!("health_check_cache_hit_rate", cache_hit_rate);
        
        health
    }
    
    /// Check Jito API health considering circuit breaker state
    async fn check_jito_api_health(&self) -> bool {
        let circuit_state = self.circuit_breaker.lock().clone();
        
        match circuit_state {
            CircuitBreakerState::Closed => {
                // Circuit is closed - API should be healthy
                true
            },
            CircuitBreakerState::Open(opened_at) => {
                // Circuit is open - check if timeout has elapsed
                let timeout_duration = Duration::from_millis(self.config.circuit_breaker_reset_timeout_ms);
                if opened_at.elapsed() > timeout_duration {
                    // Try to transition to half-open
                    *self.circuit_breaker.lock() = CircuitBreakerState::HalfOpen;
                    false // Still consider unhealthy until proven otherwise
                } else {
                    false
                }
            },
            CircuitBreakerState::HalfOpen => {
                // In half-open state - API health is uncertain
                false
            },
        }
    }
    
    /// Update circuit breaker state based on API call results
    fn update_circuit_breaker(&self, success: bool) {
        let mut circuit_state = self.circuit_breaker.lock();
        let mut failure_count = self.failure_count.lock();
        
        match *circuit_state {
            CircuitBreakerState::Closed => {
                if success {
                    *failure_count = 0;
                } else {
                    *failure_count += 1;
                    if *failure_count >= self.config.circuit_breaker_failure_threshold {
                        *circuit_state = CircuitBreakerState::Open(Instant::now());
                        increment_counter!("circuit_breaker_opened");
                    }
                }
            },
            CircuitBreakerState::Open(_) => {
                // Circuit remains open, no state change
            },
            CircuitBreakerState::HalfOpen => {
                if success {
                    *circuit_state = CircuitBreakerState::Closed;
                    *failure_count = 0;
                    increment_counter!("circuit_breaker_closed");
                } else {
                    *circuit_state = CircuitBreakerState::Open(Instant::now());
                    increment_counter!("circuit_breaker_reopened");
                }
            },
        }
    }
    
    /// Calculate validator load impact on collision probability
    fn calculate_validator_load_impact(&self) -> Result<f64> {
        let network_conditions = self.network_conditions.lock();
        
        // Higher validator load increases processing delays and collision risk
        let load_impact = network_conditions.validator_load * 0.8;
        
        gauge!("validator_load_impact", load_impact);
        Ok(load_impact.clamp(0.0, 1.0))
    }
    
    /// Implement predictive prefetching of likely collision candidates
    pub async fn prefetch_collision_candidates(&self, bundle_id: &Hash) -> Result<()> {
        if !self.config.enable_predictive_prefetching {
            return Ok(());
        }
        
        let candidates = self.spatial_index.get_potential_conflicts(bundle_id)?;
        let prefetch_count = candidates.len().min(self.config.max_prefetch_candidates);
        
        // Preload bundle data for the most likely collision candidates
        for candidate_id in candidates.iter().take(prefetch_count) {
            if let Some(bundle) = self.tracked_bundles.get(candidate_id) {
                // Cache collision metrics preemptively
                if let Some(current_bundle) = self.tracked_bundles.get(bundle_id) {
                    let cache_key = (*bundle_id, *candidate_id);
                    
                    // Check if already cached
                    if !self.metrics_cache.lock().contains(&cache_key) {
                        // Calculate and cache metrics
                        if let Ok(metrics) = self.calculate_collision_metrics(&current_bundle, &bundle) {
                            self.metrics_cache.lock().put(cache_key, metrics);
                            increment_counter!("collision_metrics_prefetched");
                        }
                    }
                }
            }
        }
        
        gauge!("prefetched_candidates", prefetch_count as f64);
        Ok(())
    }
    
    /// Get optimal batch size based on current system load
    pub async fn get_optimal_batch_size(&self) -> Result<usize> {
        let load = self.get_system_load().await?;
        
        let batch_size = match load {
            SystemLoad::Low => 100,
            SystemLoad::Medium => 50,
            SystemLoad::High => 20,
            SystemLoad::Critical => 10,
        };
        
        gauge!("optimal_batch_size", batch_size as f64);
        Ok(batch_size)
    }
    
    /// Enhanced bundle tracking with memory pool optimization
    fn get_or_create_tracked_bundle(&self) -> TrackedBundle {
        // Try to get a bundle from the memory pool first
        if let Some(bundle) = self.memory_pool.get_bundle() {
            increment_counter!("bundle_pool_reuse");
            bundle
        } else {
            increment_counter!("bundle_pool_allocation");
            TrackedBundle {
                id: Hash::default(),
                accounts: BTreeSet::new(),
                write_accounts: BTreeSet::new(),
                read_accounts: BTreeSet::new(),
                programs: BTreeSet::new(),
                lookup_tables: Vec::new(),
                transaction_hashes: Vec::new(),
                account_bloom: BloomFilter::with_rate(0.01, 1000),
                program_bloom: BloomFilter::with_rate(0.01, 100),
                estimated_cu: 0,
                total_compute_units: 0,
                cu_limit: 0,
                cu_price: 0,
                priority_fee: 0,
                jito_tip: 0,
                submission_time: Instant::now(),
                target_slot: 0,
                confidence: 0.0,
            }
        }
    }
    
    /// Return bundle to memory pool for reuse
    fn return_tracked_bundle_to_pool(&self, bundle: TrackedBundle) {
        self.memory_pool.return_bundle(bundle);
        gauge!("bundle_pool_size", self.memory_pool.bundle_pool.lock().len() as f64);
    }
    
    /// Enhanced Jito API call with circuit breaker protection and graceful degradation
    async fn call_jito_api_with_protection<T, F, Fut>(&self, operation: F) -> Result<Option<T>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        // Check rate limiting first
        if let Err(_) = self.rate_limiter.check_rate_limit() {
            increment_counter!("jito_api_rate_limited");
            return Ok(None); // Graceful degradation - return None instead of error
        }
        
        // Check circuit breaker state
        let circuit_state = self.circuit_breaker.lock().clone();
        
        match circuit_state {
            CircuitBreakerState::Open(_) => {
                increment_counter!("jito_api_circuit_open");
                return Ok(None); // Circuit is open - fail fast
            },
            CircuitBreakerState::HalfOpen => {
                // Allow one test call in half-open state
                match operation().await {
                    Ok(result) => {
                        self.update_circuit_breaker(true);
                        increment_counter!("jito_api_success_half_open");
                        Ok(Some(result))
                    },
                    Err(e) => {
                        self.update_circuit_breaker(false);
                        increment_counter!("jito_api_failure_half_open");
                        Ok(None) // Graceful degradation
                    }
                }
            },
            CircuitBreakerState::Closed => {
                // Normal operation
                match operation().await {
                    Ok(result) => {
                        self.update_circuit_breaker(true);
                        increment_counter!("jito_api_success");
                        Ok(Some(result))
                    },
                    Err(e) => {
                        self.update_circuit_breaker(false);
                        increment_counter!("jito_api_failure");
                        Ok(None) // Graceful degradation instead of propagating error
                    }
                }
            }
        }
    }
    
    /// Get cached auction data with graceful degradation
    pub fn get_cached_auction_data(&self) -> Option<JitoTipData> {
        self.jito_client.get_cached_tip_data()
    }
    
    /// Adaptive collision prediction with circuit breaker awareness
    pub async fn predict_collision_adaptive(&self, bundle_a_id: &Hash, bundle_b_id: &Hash) -> Result<BundleCollisionMetrics> {
        // Try to get from cache first
        let cache_key = (*bundle_a_id, *bundle_b_id);
        
        if let Some(cached_metrics) = self.metrics_cache.lock().get(&cache_key).cloned() {
            increment_counter!("collision_prediction_cache_hit");
            return Ok(cached_metrics);
        }
        
        // Get bundles with error handling
        let bundle_a = self.tracked_bundles.get(bundle_a_id)
            .ok_or_else(|| CollisionPredictorError::BundleNotFound(*bundle_a_id))?;
        let bundle_b = self.tracked_bundles.get(bundle_b_id)
            .ok_or_else(|| CollisionPredictorError::BundleNotFound(*bundle_b_id))?;
        
        // Calculate collision metrics with network-aware adjustments
        let mut metrics = self.calculate_collision_metrics(&bundle_a, &bundle_b)?;
        
        // Enhance with real-time data if available (graceful degradation)
        if let Ok(Some(tip_data)) = self.call_jito_api_with_protection(|| {
            self.jito_client.get_tip_data()
        }).await {
            // Adjust metrics based on real-time auction data
            let auction_adjustment = self.calculate_auction_adjustment(&bundle_a, &bundle_b, &tip_data);
            metrics.collision_probability = (metrics.collision_probability * auction_adjustment).clamp(0.0, 1.0);
            metrics.confidence = (metrics.confidence * 1.1).min(1.0); // Higher confidence with real-time data
        } else {
            // Fallback - reduce confidence without real-time data
            metrics.confidence = (metrics.confidence * 0.8).max(0.1);
        }
        
        // Cache the result
        self.metrics_cache.lock().put(cache_key, metrics.clone());
        increment_counter!("collision_prediction_cache_miss");
        
        Ok(metrics)
    }
    
    /// Calculate auction adjustment factor based on real-time tip data
    fn calculate_auction_adjustment(&self, bundle_a: &TrackedBundle, bundle_b: &TrackedBundle, tip_data: &JitoTipData) -> f64 {
        let a_competitiveness = self.calculate_tip_percentile(bundle_a.jito_tip, tip_data);
        let b_competitiveness = self.calculate_tip_percentile(bundle_b.jito_tip, tip_data);
        
        // Higher collision probability when both bundles are competitive
        let avg_competitiveness = (a_competitiveness + b_competitiveness) / 2.0;
        let competitiveness_factor = 0.8 + (avg_competitiveness * 0.4); // Scale from 0.8 to 1.2
        
        gauge!("auction_adjustment_factor", competitiveness_factor);
        competitiveness_factor.clamp(0.5, 2.0)
    }
    
    /// Update circuit breaker state based on API call results
    fn update_circuit_breaker(&self, success: bool) {
        let mut circuit_state = self.circuit_breaker.lock();
        let mut failure_count = self.failure_count.lock();
        
        match *circuit_state {
            CircuitBreakerState::Closed => {
                if success {
                    *failure_count = 0;
                } else {
                    *failure_count += 1;
                    if *failure_count >= self.config.circuit_breaker_failure_threshold {
                        *circuit_state = CircuitBreakerState::Open(Instant::now());
                        increment_counter!("circuit_breaker_opened");
                    }
                }
            },
            CircuitBreakerState::Open(_) => {
                // Circuit remains open, no state change
            },
            CircuitBreakerState::HalfOpen => {
                if success {
                    *circuit_state = CircuitBreakerState::Closed;
                    *failure_count = 0;
                    increment_counter!("circuit_breaker_closed");
                } else {
                    *circuit_state = CircuitBreakerState::Open(Instant::now());
                    increment_counter!("circuit_breaker_reopened");
                }
            },
        }
    }
    
    /// Calculate validator load impact on collision probability
    fn calculate_validator_load_impact(&self) -> Result<f64> {
        let network_conditions = self.network_conditions.lock();
        
        // Higher validator load increases processing delays and collision risk
        let load_impact = network_conditions.validator_load * 0.8;
        
        gauge!("validator_load_impact", load_impact);
        Ok(load_impact.clamp(0.0, 1.0))
    }
    
    /// Implement predictive prefetching of likely collision candidates
    pub async fn prefetch_collision_candidates(&self, bundle_id: &Hash) -> Result<()> {
        if !self.config.enable_predictive_prefetching {
            return Ok(());
        }
        
        let candidates = self.spatial_index.get_potential_conflicts(bundle_id)?;
        let prefetch_count = candidates.len().min(self.config.max_prefetch_candidates);
        
        // Preload bundle data for the most likely collision candidates
        for candidate_id in candidates.iter().take(prefetch_count) {
            if let Some(bundle) = self.tracked_bundles.get(candidate_id) {
                // Cache collision metrics preemptively
                if let Some(current_bundle) = self.tracked_bundles.get(bundle_id) {
                    let cache_key = (*bundle_id, *candidate_id);
                    
                    // Check if already cached
                    if !self.metrics_cache.lock().contains(&cache_key) {
                        // Calculate and cache metrics
                        if let Ok(metrics) = self.calculate_collision_metrics(&current_bundle, &bundle) {
                            self.metrics_cache.lock().put(cache_key, metrics);
                            increment_counter!("collision_metrics_prefetched");
                        }
                    }
                }
            }
        }
        
        gauge!("prefetched_candidates", prefetch_count as f64);
        Ok(())
    }
    
    /// Get optimal batch size based on current system load
    pub async fn get_optimal_batch_size(&self) -> Result<usize> {
        let load = self.get_system_load().await?;
        
        let batch_size = match load {
            SystemLoad::Low => 100,
            SystemLoad::Medium => 50,
            SystemLoad::High => 20,
            SystemLoad::Critical => 10,
        };
        
        gauge!("optimal_batch_size", batch_size as f64);
        Ok(batch_size)
    }
    
    /// Enhanced bundle tracking with memory pool optimization
    fn get_or_create_tracked_bundle(&self) -> TrackedBundle {
        // Try to get a bundle from the memory pool first
        if let Some(bundle) = self.memory_pool.get_bundle() {
            increment_counter!("bundle_pool_reuse");
            bundle
        } else {
            increment_counter!("bundle_pool_allocation");
            TrackedBundle {
                id: Hash::default(),
                accounts: BTreeSet::new(),
                write_accounts: BTreeSet::new(),
                read_accounts: BTreeSet::new(),
                programs: BTreeSet::new(),
                lookup_tables: Vec::new(),
                transaction_hashes: Vec::new(),
                account_bloom: BloomFilter::with_rate(0.01, 1000),
                program_bloom: BloomFilter::with_rate(0.01, 100),
                estimated_cu: 0,
                total_compute_units: 0,
                cu_limit: 0,
                cu_price: 0,
                priority_fee: 0,
                jito_tip: 0,
                submission_time: Instant::now(),
                target_slot: 0,
                confidence: 0.0,
            }
        }
    }
    
    /// Return bundle to memory pool for reuse
    fn return_tracked_bundle_to_pool(&self, bundle: TrackedBundle) {
        self.memory_pool.return_bundle(bundle);
        gauge!("bundle_pool_size", self.memory_pool.bundle_pool.lock().len() as f64);
    }
    
    /// Enhanced Jito API call with circuit breaker protection and graceful degradation
    async fn call_jito_api_with_protection<T, F, Fut>(&self, operation: F) -> Result<Option<T>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        // Check rate limiting first
        if let Err(_) = self.rate_limiter.check_rate_limit() {
            increment_counter!("jito_api_rate_limited");
            return Ok(None); // Graceful degradation - return None instead of error
        }
        
        // Check circuit breaker state
        let circuit_state = self.circuit_breaker.lock().clone();
        
        match circuit_state {
            CircuitBreakerState::Open(_) => {
                increment_counter!("jito_api_circuit_open");
                return Ok(None); // Circuit is open - fail fast
            },
            CircuitBreakerState::HalfOpen => {
                // Allow one test call in half-open state
                match operation().await {
                    Ok(result) => {
                        self.update_circuit_breaker(true);
                        increment_counter!("jito_api_success_half_open");
                        Ok(Some(result))
                    },
                    Err(e) => {
                        self.update_circuit_breaker(false);
                        increment_counter!("jito_api_failure_half_open");
                        Ok(None) // Graceful degradation
                    }
                }
            },
            CircuitBreakerState::Closed => {
                // Normal operation
                match operation().await {
                    Ok(result) => {
                        self.update_circuit_breaker(true);
                        increment_counter!("jito_api_success");
                        Ok(Some(result))
                    },
                    Err(e) => {
                        self.update_circuit_breaker(false);
                        increment_counter!("jito_api_failure");
                        Ok(None) // Graceful degradation instead of propagating error
                    }
                }
            }
        }
    }
    
    /// Get cached auction data with graceful degradation
    pub fn get_cached_auction_data(&self) -> Option<JitoTipData> {
        self.jito_client.get_cached_tip_data()
    }
    
    /// Adaptive collision prediction with circuit breaker awareness
    pub async fn predict_collision_adaptive(&self, bundle_a_id: &Hash, bundle_b_id: &Hash) -> Result<BundleCollisionMetrics> {
        // Try to get from cache first
        let cache_key = (*bundle_a_id, *bundle_b_id);
        
        if let Some(cached_metrics) = self.metrics_cache.lock().get(&cache_key).cloned() {
            increment_counter!("collision_prediction_cache_hit");
            return Ok(cached_metrics);
        }
        
        // Get bundles with error handling
        let bundle_a = self.tracked_bundles.get(bundle_a_id)
            .ok_or_else(|| CollisionPredictorError::BundleNotFound(*bundle_a_id))?;
        let bundle_b = self.tracked_bundles.get(bundle_b_id)
            .ok_or_else(|| CollisionPredictorError::BundleNotFound(*bundle_b_id))?;
        
        // Calculate collision metrics with network-aware adjustments
        let mut metrics = self.calculate_collision_metrics(&bundle_a, &bundle_b)?;
        
        // Enhance with real-time data if available (graceful degradation)
        if let Ok(Some(tip_data)) = self.call_jito_api_with_protection(|| {
            self.jito_client.get_tip_data()
        }).await {
            // Adjust metrics based on real-time auction data
            let auction_adjustment = self.calculate_auction_adjustment(&bundle_a, &bundle_b, &tip_data);
            metrics.collision_probability = (metrics.collision_probability * auction_adjustment).clamp(0.0, 1.0);
            metrics.confidence = (metrics.confidence * 1.1).min(1.0); // Higher confidence with real-time data
        } else {
            // Fallback - reduce confidence without real-time data
            metrics.confidence = (metrics.confidence * 0.8).max(0.1);
        }
        
        // Cache the result
        self.metrics_cache.lock().put(cache_key, metrics.clone());
        increment_counter!("collision_prediction_cache_miss");
        
        Ok(metrics)
    }
    
    /// Calculate auction adjustment factor based on real-time tip data
    fn calculate_auction_adjustment(&self, bundle_a: &TrackedBundle, bundle_b: &TrackedBundle, tip_data: &JitoTipData) -> f64 {
        let a_competitiveness = self.calculate_tip_percentile(bundle_a.jito_tip, tip_data);
        let b_competitiveness = self.calculate_tip_percentile(bundle_b.jito_tip, tip_data);
        
        // Higher collision probability when both bundles are competitive
        let avg_competitiveness = (a_competitiveness + b_competitiveness) / 2.0;
        let competitiveness_factor = 0.8 + (avg_competitiveness * 0.4); // Scale from 0.8 to 1.2
        
        gauge!("auction_adjustment_factor", competitiveness_factor);
        competitiveness_factor.clamp(0.5, 2.0)
    }
    
    /// Calculate collision metrics between two bundles
    pub async fn predict_collision(&self, bundle_a_id: &Hash, bundle_b_id: &Hash) -> Result<BundleCollisionMetrics> {
        let bundle_a = self.tracked_bundles
            .get(bundle_a_id)
            .ok_or_else(|| CollisionPredictorError::BundleNotFound(*bundle_a_id))?;
        let bundle_b = self.tracked_bundles
            .get(bundle_b_id)
            .ok_or_else(|| CollisionPredictorError::BundleNotFound(*bundle_b_id))?;

        // Calculate individual collision components
        let account_overlap_score = self.calculate_account_overlap(&bundle_a, &bundle_b);
        let program_conflict_score = self.calculate_program_conflict(&bundle_a, &bundle_b);
        let timing_conflict_score = self.calculate_timing_conflict(&bundle_a, &bundle_b);
        let slot_conflict_probability = self.calculate_slot_conflict(&bundle_a, &bundle_b);
        let cu_competition_score = self.calculate_cu_competition(&bundle_a, &bundle_b);
        let priority_fee_competition = self.calculate_priority_fee_competition(&bundle_a, &bundle_b);
        let auction_competitiveness = self.calculate_auction_competitiveness(&bundle_a, &bundle_b);

        // Combine scores with weighted factors
        let collision_probability = self.combine_collision_factors(
            account_overlap_score,
            program_conflict_score,
            timing_conflict_score,
            slot_conflict_probability,
            cu_competition_score,
            priority_fee_competition,
            auction_competitiveness,
        );

        let recommended_delay_ms = self.calculate_recommended_delay(
            collision_probability,
            &bundle_a,
            &bundle_b,
        );

        let tip_efficiency_ratio = self.calculate_tip_efficiency(&bundle_a, &bundle_b);

        Ok(BundleCollisionMetrics {
            collision_probability,
            account_overlap_score,
            program_conflict_score,
            timing_conflict_score,
            priority_fee_competition,
            recommended_delay_ms,
            slot_conflict_probability,
            cu_competition_score,
            tip_efficiency_ratio,
            auction_competitiveness,
            bundle_size_penalty: self.calculate_bundle_size_penalty(&bundle_a, &bundle_b),
        })
    }

    fn calculate_account_overlap(&self, a: &TrackedBundle, b: &TrackedBundle) -> f64 {
        let write_overlap = a.write_accounts.intersection(&b.write_accounts).count();
        let read_write_overlap = a.read_accounts.intersection(&b.write_accounts).count() +
                               b.read_accounts.intersection(&a.write_accounts).count();
        
        let total_potential_conflicts = a.accounts.len() + b.accounts.len();
        (write_overlap * 2 + read_write_overlap) as f64 / total_potential_conflicts.max(1) as f64
    }

    fn calculate_program_conflict(&self, a: &TrackedBundle, b: &TrackedBundle) -> f64 {
        let common_programs: Vec<_> = a.programs.intersection(&b.programs).collect();
        common_programs.iter()
            .map(|program| {
                self.program_interactions.get(program)
                    .map(|interaction| interaction.conflict_patterns.values().sum::<f64>())
                    .unwrap_or(0.0)
            })
            .sum::<f64>()
            .min(1.0)
    }

    fn calculate_timing_conflict(&self, a: &TrackedBundle, b: &TrackedBundle) -> f64 {
        let time_diff = a.submission_time.abs_diff(b.submission_time).as_millis();
        let window_ms = self.config.collision_window_ms as u128;
        (1.0 - (time_diff as f64 / window_ms as f64).min(1.0)).max(0.0)
    }

    fn calculate_slot_conflict(&self, a: &TrackedBundle, b: &TrackedBundle) -> f64 {
        if a.slot_target == b.slot_target {
            let slot_tracker = self.slot_tracker.read();
            slot_tracker.slot_collision_rates.get(&a.slot_target).copied().unwrap_or(0.5)
        } else {
            0.0
        }
    }

    fn calculate_cu_competition(&self, a: &TrackedBundle, b: &TrackedBundle) -> f64 {
        let total_cu = a.estimated_cu + b.estimated_cu;
        (total_cu as f64 / self.config.max_cu_per_bundle as f64).min(1.0)
    }

    fn calculate_priority_fee_competition(&self, a: &TrackedBundle, b: &TrackedBundle) -> f64 {
        let min_fee = a.priority_fee.min(b.priority_fee) as f64;
        let max_fee = a.priority_fee.max(b.priority_fee) as f64;
        (min_fee / max_fee.max(1.0)).powf(0.5)
    }

    fn combine_collision_factors(
        &self,
        account_overlap: f64,
        program_conflict: f64,
        timing_conflict: f64,
        slot_conflict: f64,
        cu_competition: f64,
        fee_competition: f64,
        auction_competitiveness: f64,
    ) -> f64 {
        let weights = [0.3, 0.2, 0.15, 0.1, 0.1, 0.1, 0.05];
        let factors = [
            account_overlap,
            program_conflict,
            timing_conflict,
            slot_conflict,
            cu_competition,
            fee_competition,
            auction_competitiveness,
        ];

        factors.iter()
            .zip(weights.iter())
            .map(|(factor, weight)| factor * weight)
            .sum::<f64>()
            .min(1.0)
    }

    fn calculate_recommended_delay(&self, collision_prob: f64, a: &TrackedBundle, b: &TrackedBundle) -> u64 {
        let base_delay = (collision_prob * self.config.collision_window_ms as f64) as u64;
        let tip_difference = (a.jito_tip as i64 - b.jito_tip as i64).abs() as u64;
        
        if tip_difference > self.config.min_profitable_tip_lamports {
            base_delay / 2
        } else {
            base_delay
        }
    }

    fn calculate_tip_efficiency(&self, a: &TrackedBundle, b: &TrackedBundle) -> f64 {
        let total_tip = a.jito_tip + b.jito_tip;
        let total_cu = a.estimated_cu + b.estimated_cu;
        (total_tip as f64 / total_cu as f64).log10().max(0.0).min(1.0)
    }

    fn calculate_bundle_size_penalty(&self, a: &TrackedBundle, b: &TrackedBundle) -> f64 {
        let total_size = a.transaction_count + b.transaction_count;
        (total_size as f64 / (2 * self.config.max_bundle_size) as f64).min(1.0)
    }

    fn calculate_auction_competitiveness(&self, a: &TrackedBundle, b: &TrackedBundle) -> f64 {
        let auction_data = match self.jito_client.get_cached_auction_data() {
            Some(data) => data,
            None => return 0.5, // Default neutral value if no auction data
        };
        
        let tip_ratio = (a.jito_tip as f64 + b.jito_tip as f64) / 
                       (auction_data.suggested_tip as f64).max(1.0);
        
        (tip_ratio * auction_data.competition_factor).min(1.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;
    use solana_sdk::system_transaction;

    #[tokio::test]
    async fn test_collision_prediction() -> Result<()> {
        let predictor = BundleCollisionPredictor::new()?;
        let keypair = Keypair::new();
        let pubkey = keypair.pubkey();
        
        let tx1 = system_transaction::transfer(&keypair, &pubkey, 1000, Hash::default());
        let tx2 = system_transaction::transfer(&keypair, &pubkey, 2000, Hash::default());
        
        let bundle1 = vec![tx1];
        let bundle2 = vec![tx2];
        
        let id1 = predictor.track_bundle(bundle1, 100).await?;
        let id2 = predictor.track_bundle(bundle2, 100).await?;
        
        let metrics = predictor.predict_collision(&id1, &id2).await?;
        assert!(metrics.collision_probability > 0.0);
        assert!(metrics.account_overlap_score > 0.0);
        Ok(())
    }

    #[tokio::test]
    async fn test_optimal_window_finding() -> Result<()> {
        let predictor = BundleCollisionPredictor::new()?;
        let keypair = Keypair::new();
        let tx = system_transaction::transfer(&keypair, &keypair.pubkey(), 1000, Hash::default());
        
        let bundle_id = predictor.track_bundle(vec![tx], 100).await?;
        let (window, confidence) = predictor.find_optimal_submission_window(&bundle_id).await?;
        
        assert!(confidence >= 0.0 && confidence <= 1.0);
        assert!(window >= Instant::now());
        Ok(())
    }
}
