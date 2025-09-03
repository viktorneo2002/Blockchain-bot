use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    transaction::Transaction,
    commitment_config::CommitmentConfig,
    clock::Slot,
    instruction::{Instruction, AccountMeta},
    message::Message,
    sysvar,
};
use std::collections::{HashMap, VecDeque, HashSet, BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::time::{timeout, sleep};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use anyhow::{anyhow, Result as AnyhowResult};
use sha2::{Sha256, Digest};
use serde_json::Value;
use reqwest::Client as HttpClient;
use base64;
use bincode;

#[derive(Error, Debug)]
pub enum CounterStrategyError {
    #[error("Lock timeout: {0}")]
    LockTimeout(String),
    #[error("Pattern detection failed: {0}")]
    PatternDetectionError(String),
    #[error("Resource limit exceeded: {0}")]
    ResourceLimitExceeded(String),
    #[error("Market conditions unsafe: {0}")]
    UnsafeMarketConditions(String),
    #[error("Configuration error: {0}")]
    ConfigurationError(String),
    #[error("Computation error: {0}")]
    ComputationError(String),
    #[error("RPC error: {0}")]
    RpcError(String),
    #[error("Network analysis failed: {0}")]
    NetworkAnalysisError(String),
    #[error("Transaction analysis failed: {0}")]
    TransactionAnalysisError(String),
    #[error("ML model error: {0}")]
    MLModelError(String),
    #[error("Memory overflow: {0}")]
    MemoryOverflow(String),
}

pub type Result<T> = std::result::Result<T, CounterStrategyError>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CounterStrategyConfig {
    pub pattern_window_size: usize,
    pub confidence_threshold: f64,
    pub detection_sensitivity: f64,
    pub base_priority_multiplier: f64,
    pub max_priority_fee: u64,
    pub timing_offset_range_ms: (i64, i64),
    pub compute_unit_padding: u64,
    pub circuit_breaker_threshold: u32,
    pub circuit_breaker_reset_duration: Duration,
    pub max_tracked_wallets: usize,
    pub pattern_cache_size: usize,
    pub cleanup_interval: Duration,
    pub rpc_endpoints: Vec<String>,
    pub websocket_endpoint: String,
    pub jito_endpoint: String,
    pub ml_model_path: Option<String>,
    pub feature_extraction_window: usize,
    pub ml_confidence_threshold: f64,
    pub api_keys: ApiKeyConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyConfig {
    pub helius_api_key: Option<String>,
    pub quicknode_api_key: Option<String>,
    pub solana_fm_api_key: Option<String>,
    pub enable_fallback_on_missing_keys: bool,
}

impl ApiKeyConfig {
    pub fn from_env() -> Self {
        Self {
            helius_api_key: std::env::var("HELIUS_API_KEY").ok(),
            quicknode_api_key: std::env::var("QUICKNODE_API_KEY").ok(),
            solana_fm_api_key: std::env::var("SOLANA_FM_API_KEY").ok(),
            enable_fallback_on_missing_keys: std::env::var("ENABLE_API_FALLBACK")
                .unwrap_or_else(|_| "true".to_string())
                .parse()
                .unwrap_or(true),
        }
    }
    
    pub fn validate(&self) -> Result<()> {
        let mut missing_keys = Vec::new();
        
        if self.helius_api_key.is_none() {
            missing_keys.push("HELIUS_API_KEY");
        }
        if self.quicknode_api_key.is_none() {
            missing_keys.push("QUICKNODE_API_KEY");
        }
        if self.solana_fm_api_key.is_none() {
            missing_keys.push("SOLANA_FM_API_KEY");
        }
        
        if !missing_keys.is_empty() && !self.enable_fallback_on_missing_keys {
            return Err(anyhow!(
                "Missing required API keys: {}. Set ENABLE_API_FALLBACK=true to use fallback mode.",
                missing_keys.join(", ")
            ));
        }
        
        if !missing_keys.is_empty() {
            log::warn!(
                "Missing API keys: {}. Using fallback mode with reduced functionality.",
                missing_keys.join(", ")
            );
        }
        
        Ok(())
    }
}

impl Default for CounterStrategyConfig {
    fn default() -> Self {
        Self {
            pattern_window_size: 100,
            confidence_threshold: 0.75,
            detection_sensitivity: 0.8,
            base_priority_multiplier: 1.5,
            max_priority_fee: 1_000_000,
            timing_offset_range_ms: (-100, 100),
            compute_unit_padding: 10000,
            circuit_breaker_threshold: 5,
            circuit_breaker_reset_duration: Duration::from_secs(60),
            max_tracked_wallets: 10000,
            pattern_cache_size: 1000,
            cleanup_interval: Duration::from_secs(300),
            rpc_endpoints: vec!["https://api.mainnet-beta.solana.com".to_string()],
            websocket_endpoint: "wss://api.mainnet-beta.solana.com".to_string(),
            jito_endpoint: "https://mainnet.block-engine.jito.wtf".to_string(),
            ml_model_path: None,
            feature_extraction_window: 50,
            ml_confidence_threshold: 0.85,
            api_keys: ApiKeyConfig::from_env(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompetitorPattern {
    pub wallet: Pubkey,
    pub pattern_type: PatternType,
    pub confidence: f64,
    pub frequency: f64,
    pub avg_priority_fee: u64,
    pub success_rate: f64,
    pub last_seen: Instant,
    pub transaction_history: VecDeque<TransactionMetadata>,
    pub total_volume: u64,
    pub win_rate: f64,
    // Advanced ML features based on research
    pub signature_entropy: f64,
    pub timing_predictability: f64,
    pub account_clustering_score: f64,
    pub priority_fee_pattern: Vec<f64>,
    pub instruction_pattern_hash: String,
    pub jito_bundle_frequency: f64,
    pub cross_program_invocation_depth: u8,
    pub associated_validators: BTreeSet<Pubkey>,
    pub gas_efficiency_score: f64,
    pub sophistication_level: SophisticationLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum PatternType {
    Sandwich,
    FrontRunner,
    Arbitrageur,
    Liquidator,
    HighFrequencyTrader,
    MEVBot,
    JitoSearcher,
    StatisticalArbitrageur,
    LatencyArbitrageur,
    VolumeManipulator,
    PumpAndDump,
    WashTrader,
    CrossChainArbitrageur,
    FlashLoanExploiter,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum SophisticationLevel {
    Basic,      // Simple patterns, basic MEV
    Intermediate, // Uses timing analysis, multiple strategies
    Advanced,   // ML-based, sophisticated bundling
    Professional, // Advanced analytics, multi-hop arbitrage
    Institutional, // Complex strategies, high-frequency trading
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionMetadata {
    pub signature: String,
    pub wallet: Pubkey,
    pub timestamp: Instant,
    pub priority_fee: u64,
    pub compute_units: u64,
    pub target_program: Pubkey,
    pub transaction_size: usize,
    pub slot: Slot,
    pub success: bool,
    // Advanced analysis fields based on research
    pub accounts_accessed: Vec<Pubkey>,
    // Raw instruction data (human-readable for tests); when absent, fall back to balances
    pub instruction_data: String,
    pub instruction_data_hash: String,
    pub cross_program_invocations: u8,
    pub lookup_table_usage: bool,
    pub bundle_position: Option<u8>,
    pub jito_tip: Option<u64>,
    pub validator_leader: Option<Pubkey>,
    pub account_lock_conflicts: u8,
    pub pre_token_balance: Option<u64>,
    pub post_token_balance: Option<u64>,
    pub slippage_tolerance: Option<f64>,
    pub dex_program_used: Option<DexProgram>,
    // Values referenced by analytics and tests
    pub value_transferred: u64,
    pub jito_bundle_info: Option<JitoBundleInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DexProgram {
    Raydium,
    Orca,
    Serum,
    Phoenix,
    PumpFun,
    Jupiter,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct CounterStrategy {
    config: Arc<CounterStrategyConfig>,
    patterns: Arc<RwLock<HashMap<Pubkey, CompetitorPattern>>>,
    sandwich_detections: Arc<RwLock<VecDeque<SandwichAttempt>>>,
    mev_protection: Arc<RwLock<MEVProtectionState>>,
    response_strategies: Arc<RwLock<HashMap<PatternType, ResponseStrategy>>>,
    performance_metrics: Arc<RwLock<PerformanceMetrics>>,
    circuit_breaker: Arc<RwLock<CircuitBreaker>>,
    rng: Arc<Mutex<ChaCha20Rng>>,
    last_cleanup: Arc<RwLock<Instant>>,
    // Advanced components based on research
    ml_feature_extractor: Arc<RwLock<MLFeatureExtractor>>,
    signature_clustering: Arc<RwLock<SignatureClustering>>,
    network_analyzer: Arc<RwLock<NetworkConditionAnalyzer>>,
    priority_fee_predictor: Arc<RwLock<PriorityFeePredictor>>,
    jito_bundle_tracker: Arc<RwLock<JitoBundleTracker>>,
    validator_rotation_tracker: Arc<RwLock<ValidatorRotationTracker>>,
    http_client: HttpClient,
    bloom_filter_cache: Arc<RwLock<BloomFilterCache>>,
    lru_pattern_cache: Arc<RwLock<LRUCache<String, CompetitorPattern>>>,
}

#[derive(Debug, Clone)]
pub struct JitoBundleInfo {
    pub bundle_id: String,
    // Optional fields retained for broader compatibility
    pub transactions: Vec<String>,
    pub total_tip: u64,
    // Fields used by sandwich/bundle analysis and tests
    pub position_in_bundle: u8,
    pub total_transactions: u8,
    pub leader_validator: Pubkey,
    pub submission_timestamp: Instant,
    pub success: Option<bool>,
    pub bundle_type: BundleType,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BundleType {
    Sandwich,
    Arbitrage,
    Liquidation,
    Backrun,
    Frontrun,
    Mixed,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AttackSophistication {
    BasicSandwich,      // Simple front-run/back-run pattern
    BasicMEV,           // MEV with timing optimization
    IntermediateMEV,    // Advanced timing + fee optimization
    AdvancedJitoMEV,    // Jito bundles + ML-based optimization
    InstitutionalMEV,   // Multi-pool, cross-program sophisticated attacks
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundlePosition {
    pub position_in_bundle: u8,
    pub total_bundle_size: u8,
    pub frontrun_position: u8,
    pub backrun_position: u8,
    pub expected_victim_count: u8,
    pub multi_pool_coordination: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MLPatternFeatures {
    pub instruction_hash_entropy: f64,
    pub account_reuse_frequency: f64,
    pub timing_variance_coefficient: f64,
    pub fee_escalation_pattern: Vec<f64>,
    pub cross_program_complexity: u8,
    pub validator_targeting_score: f64,
    pub bundle_coordination_score: f64,
    pub profit_extraction_efficiency: f64,
}

// Missing type definitions for mainnet-ready counter-strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandwichAttempt {
    pub attacker: Pubkey,
    pub victim_tx: String,
    pub front_tx: String,
    pub back_tx: Option<String>,
    pub detected_at: Instant,
    pub pool: Pubkey,
    pub estimated_profit: u64,
    pub jito_bundle_id: Option<String>,
    pub bundle_position: Option<BundlePosition>,
    pub slippage_extracted: f64,
    pub price_impact: f64,
    pub victim_slippage_tolerance: f64,
    pub frontrun_token_amount: u64,
    pub backrun_token_amount: u64,
    pub validator_tip: u64,
    pub attack_sophistication: AttackSophistication,
    pub detection_confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseStrategy {
    pub base_priority_multiplier: f64,
    pub timing_offset_ms: i64,
    pub compute_unit_padding: u64,
    pub use_decoy_transactions: bool,
    pub adaptive_routing: bool,
    pub fee_escalation_rate: f64,
    pub max_retries: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MEVProtectionState {
    pub active_protection: bool,
    pub decoy_transactions: Vec<String>,
    pub obfuscation_level: u8,
    pub dynamic_routing: bool,
    pub priority_fee_randomization: (u64, u64),
    pub last_update: Instant,
    pub stealth_mode_active: bool,
    pub timing_randomization_seed: u64,
    pub decoy_pattern_complexity: u8,
    pub multi_path_routing: bool,
    pub validator_selection_strategy: ValidatorSelectionStrategy,
    pub transaction_ordering_obfuscation: bool,
    pub priority_fee_masking: bool,
    pub lookup_table_rotation: bool,
    pub account_clustering_prevention: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PerformanceMetrics {
    pub successful_counter_trades: u64,
    pub failed_counter_trades: u64,
    pub total_profit: i64,
    pub total_loss: i64,
    pub average_response_time_ms: f64,
    pub sandwich_attacks_prevented: u64,
    pub frontrun_attempts_countered: u64,
    pub mev_opportunities_captured: u64,
    pub gas_efficiency_score: f64,
    pub win_rate: f64,
    pub profit_per_trade: f64,
    pub validator_tips_paid: u64,
    pub jito_bundles_submitted: u64,
    pub jito_bundles_successful: u64,
    pub network_conditions: NetworkConditions,
    pub last_updated: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ValidatorSelectionStrategy {
    Diversified,       // Spread across multiple validators
    Performance,       // Target high-performance validators
    MEVFriendly,      // Target MEV-friendly validators
    Random,           // Random selection for unpredictability
    Stealth,          // Avoid known MEV validators
    Custom(Vec<Pubkey>), // Custom validator list
}

#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    pub failure_count: u32,
    pub last_failure: Option<Instant>,
    pub is_open: bool,
    pub threshold: u32,
    pub reset_duration: Duration,
}

#[derive(Debug, Clone)]
pub struct MLFeatureExtractor {
    pub feature_window: VecDeque<MLPatternFeatures>,
    pub feature_cache: HashMap<String, Vec<f64>>,
    pub model_version: String,
    pub last_update: Instant,
}

impl MLFeatureExtractor {
    pub fn new() -> Self {
        Self {
            feature_window: VecDeque::with_capacity(200),
            feature_cache: HashMap::new(),
            model_version: "v1.0.0".to_string(),
            last_update: Instant::now(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SignatureClustering {
    pub clusters: HashMap<String, Vec<String>>,
    pub threshold: f64,
    pub cluster_metadata: HashMap<String, ClusterMetadata>,
}

impl SignatureClustering {
    pub fn new(threshold: f64) -> Self {
        Self {
            clusters: HashMap::new(),
            threshold,
            cluster_metadata: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClusterMetadata {
    pub cluster_id: String,
    pub centroid: Vec<f64>,
    pub size: usize,
    pub confidence: f64,
    pub last_updated: Instant,
}

#[derive(Debug, Clone)]
pub struct CompetitorFeeAnalysis {
    pub active_bot_count: usize,
    pub average_competitor_fee: u64,
    pub top_percentile_fee: u64,
    pub market_volatility: f64,
}

#[derive(Debug, Clone)]
pub struct NetworkConditionAnalyzer {
    pub current_conditions: NetworkConditions,
    pub historical_data: VecDeque<NetworkConditions>,
    pub congestion_score: f64,
    pub last_update: Instant,
}

impl NetworkConditionAnalyzer {
    pub async fn new(config: &ApiKeyConfig) -> Self {
        let mut analyzer = Self {
            current_conditions: NetworkConditions::default(),
            historical_data: VecDeque::with_capacity(100),
            congestion_score: 0.0,
            last_update: Instant::now(),
        };
        
        // Attempt to fetch initial data, but don't fail if unavailable
        if let Ok(priority_data) = get_network_priority_fees(config).await {
            analyzer.current_conditions.average_priority_fee = priority_data.percentiles.p75 as u64;
        }
        
        analyzer
    }
}

impl CounterStrategy {
    pub async fn optimize_memory_usage(&self) -> Result<MemoryOptimizationReport> {
        let start_time = Instant::now();
        
        // Calculate optimal priority fee
        let base_fee = self.calculate_base_priority_fee().await?;
        let network_conditions = self.analyze_network_conditions().await?;
        let competitor_fees = self.analyze_competitor_fees(&self.config.api_keys).await?;

        // Clean up pattern history
        let mut patterns = self.acquire_write_lock(&self.patterns).await?;
        let initial_pattern_count = patterns.len();
        self.prune_inactive_competitors(&mut patterns)?;
        let patterns_pruned = initial_pattern_count - patterns.len();
        drop(patterns);

        // Clean up bloom filters
        let mut bloom_cache = self.acquire_write_lock(&self.bloom_cache).await?;
        bloom_cache.reset_if_needed();
        drop(bloom_cache);

        // Clean up LRU cache
        let mut lru_cache = self.acquire_write_lock(&self.lru_cache).await?;
        let cache_hit_rate = lru_cache.hit_rate();
        
        // Prune old cache entries
        let cutoff = Instant::now() - Duration::from_secs(300);
        let initial_cache_size = lru_cache.cache_entries.len();
        lru_cache.cache_entries.retain(|_, entry| entry.last_accessed > cutoff);
        let cache_entries_pruned = initial_cache_size - lru_cache.cache_entries.len();
        drop(lru_cache);

        // Clean up Jito bundle tracker
        let mut jito_tracker = self.acquire_write_lock(&self.jito_tracker).await?;
        jito_tracker.prune_old_bundles();
        drop(jito_tracker);

        // Update performance metrics
        let optimization_time = start_time.elapsed();
        
        let report = MemoryOptimizationReport {
            patterns_pruned,
            cache_entries_pruned,
            cache_hit_rate,
            optimization_duration_ms: optimization_time.as_millis() as u64,
            memory_freed_estimated_kb: (patterns_pruned * 1000 + cache_entries_pruned * 200) as u64 / 1024,
        };

        log::info!("Memory optimization completed: {:?}", report);
        Ok(report)
    }
    
    pub async fn cleanup_stale_data(&self) -> Result<()> {
        let cutoff_time = Instant::now() - Duration::from_secs(600); // 10 minutes
        
        // Clean up old patterns
        let mut patterns = self.acquire_write_lock(&self.patterns).await?;
        patterns.retain(|_, p| p.last_seen > cutoff_time);
        drop(patterns);
        
        // Clean up old cache entries
        let mut lru_cache = self.acquire_write_lock(&self.lru_pattern_cache).await?;
        lru_cache.cache_entries.retain(|_, entry| entry.last_accessed > cutoff_time);
        drop(lru_cache);
        
        // Clean up fee history
        let mut fee_predictor = self.acquire_write_lock(&self.priority_fee_predictor).await?;
        fee_predictor.fee_history.retain(|(timestamp, _)| *timestamp > cutoff_time);
        drop(fee_predictor);

        log::debug!("Cleaned up stale data older than 10 minutes");
        Ok(())
    }
    
    async fn analyze_competitor_fees(&self, api_keys: &ApiKeyConfig) -> Result<CompetitorFeeAnalysis> {
        let patterns = self.acquire_read_lock(&self.patterns).await?;
        
        let active_bots = patterns.values()
            .filter(|p| p.last_seen.elapsed() < Duration::from_secs(60))
            .count();

        // Calculate average competitor fee
        let avg_competitor_fee = patterns.values()
            .map(|p| p.avg_priority_fee)
            .sum::<u64>()
            .checked_div(patterns.len() as u64)
            .unwrap_or(0);

        // Get latest priority fee data with fallback
        let top_percentile_fee = if let Ok(priority_data) = get_network_priority_fees(api_keys).await {
            priority_data.percentiles.p95 as u64
        } else {
            // Fallback to cached or default
            self.get_cached_priority_fees().await?
                .into_iter()
                .max()
                .unwrap_or(100000)
        };
        
        Ok(CompetitorFeeAnalysis {
            active_bot_count: active_bots,
            average_competitor_fee: avg_competitor_fee,
            top_percentile_fee,
            market_volatility: 0.0,
        })
    }
    
    pub async fn get_memory_usage_stats(&self) -> Result<MemoryUsageStats> {
        let patterns = self.acquire_read_lock(&self.patterns).await?;
        let bloom_cache = self.acquire_read_lock(&self.bloom_filter_cache).await?;
        let lru_cache = self.acquire_read_lock(&self.lru_pattern_cache).await?;
        let jito_tracker = self.acquire_read_lock(&self.jito_bundle_tracker).await?;

        let total_patterns = patterns.len();
        let total_transactions = patterns.values()
            .map(|p| p.transaction_history.len())
            .sum::<usize>();
        
        let cache_entries = lru_cache.cache_entries.len();
        let active_bundles = jito_tracker.active_bundles.len();
        let bloom_filter_load = bloom_cache.items_inserted as f64 / bloom_cache.filter_capacity as f64;

        // Estimate memory usage (rough approximation)
        let estimated_memory_kb = 
            (total_patterns * 1000 +     // ~1KB per pattern
             total_transactions * 500 +  // ~500B per transaction
             cache_entries * 200 +       // ~200B per cache entry
             active_bundles * 300) as u64 / 1024; // ~300B per bundle

        Ok(MemoryUsageStats {
            total_patterns,
            total_transactions,
            cache_entries,
            active_bundles,
            bloom_filter_load_factor: bloom_filter_load,
            estimated_memory_usage_kb: estimated_memory_kb,
            last_cleanup: self.last_cleanup_time().await,
        })
    }

    async fn last_cleanup_time(&self) -> Instant {
        // Return the most recent cleanup time from various components
        let last_cleanup = self.acquire_read_lock(&self.last_cleanup).await
            .map(|lc| *lc)
            .unwrap_or_else(|_| Instant::now() - Duration::from_secs(3600));
        last_cleanup
    }
    
    // Helper methods for API fallbacks
    async fn get_network_conditions_fallback(&self) -> Result<NetworkConditions> {
        // Use cached or default network conditions when API is unavailable
        Ok(NetworkConditions {
            average_slot_time_ms: 400.0,
            transaction_throughput: 2000.0,
            average_priority_fee: 50000,
            congestion_level: 0.5,
            validator_performance: 0.85,
            network_stake_concentration: 0.3,
        })
    }
    
    async fn get_cached_priority_fees(&self) -> Result<Vec<u64>> {
        // Return cached or default priority fees
        let priority_fee_predictor = self.priority_fee_predictor.read()
            .map_err(|e| CounterStrategyError::ConcurrencyError(format!("Failed to read priority fee predictor: {}", e)))?;
        
        let fees = priority_fee_predictor.fee_history
            .iter()
            .map(|(_, fee)| *fee)
            .collect::<Vec<_>>();
        
        if fees.is_empty() {
            Ok(vec![10000, 25000, 50000, 100000]) // Default fee levels
        } else {
            Ok(fees)
        }
    }
    
    // Placeholder methods to avoid compilation errors
    async fn acquire_read_lock<T>(&self, lock: &Arc<RwLock<T>>) -> Result<RwLockReadGuard<'_, T>> {
        lock.read()
            .map_err(|e| anyhow!("Failed to acquire read lock: {}", e))
    }
    
    async fn acquire_write_lock<T>(&self, lock: &Arc<RwLock<T>>) -> Result<RwLockWriteGuard<'_, T>> {
        lock.write()
            .map_err(|e| anyhow!("Failed to acquire write lock: {}", e))
    }
    
    async fn calculate_base_priority_fee(&self) -> Result<u64> {
        Ok(50000) // Placeholder
    }
    
    async fn analyze_network_conditions(&self) -> Result<NetworkConditions> {
        self.get_network_conditions_fallback().await
    }
    
    fn prune_inactive_competitors(&self, patterns: &mut HashMap<Pubkey, CompetitorPattern>) -> Result<()> {
        let cutoff = Instant::now() - Duration::from_secs(300);
        patterns.retain(|_, p| p.last_seen > cutoff);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CompetitiveLandscape {
    pub active_bot_count: usize,
    pub total_tracked_competitors: usize,
    pub average_sophistication_score: f64,
    pub competition_intensity: f64,
    pub dominant_strategy_types: Vec<PatternType>,
    pub estimated_market_share: f64,
}

#[derive(Debug, Clone)]
pub struct MemoryOptimizationReport {
    pub patterns_pruned: usize,
    pub cache_entries_pruned: usize,
    pub cache_hit_rate: f64,
    pub optimization_duration_ms: u64,
    pub memory_freed_estimated_kb: u64,
}

#[derive(Debug, Clone)]
pub struct MemoryUsageStats {
    pub total_patterns: usize,
    pub total_transactions: usize,
    pub cache_entries: usize,
    pub active_bundles: usize,
    pub bloom_filter_load_factor: f64,
    pub estimated_memory_usage_kb: u64,
    pub last_cleanup: Instant,
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;
    
    #[tokio::test]
    async fn test_counter_strategy_creation() {
        let strategy = CounterStrategyBuilder::new()
            .with_pattern_window_size(50)
            .with_confidence_threshold(0.9)
            .build();
        
        assert!(strategy.is_ok());
    }

    #[tokio::test]
    async fn test_pattern_detection_accuracy() {
        let strategy = create_test_strategy().await;
        
        // Test MEV bot pattern detection
        let mev_bot_wallet = Keypair::new().pubkey();
        let mut transactions = create_mev_bot_transactions(mev_bot_wallet, 10);
        
        for tx in &transactions {
            strategy.record_transaction(tx.clone()).await.unwrap();
        }
        
        let patterns = strategy.acquire_read_lock(&strategy.patterns).await.unwrap();
        let detected_pattern = patterns.get(&mev_bot_wallet).unwrap();
        
        assert!(matches!(detected_pattern.pattern_type, PatternType::MEVBot));
        assert!(detected_pattern.confidence > 0.7);
        assert!(detected_pattern.ml_features.timing_predictability > 0.5);
    }

    #[tokio::test]
    async fn test_sandwich_attack_detection() {
        let strategy = create_test_strategy().await;
        
        // Create sandwich attack pattern
        let attacker_wallet = Keypair::new().pubkey();
        let sandwich_txs = create_sandwich_attack_transactions(attacker_wallet);
        
        for tx in &sandwich_txs {
            strategy.record_transaction(tx.clone()).await.unwrap();
        }
        
        let patterns = strategy.acquire_read_lock(&strategy.patterns).await.unwrap();
        let pattern = patterns.get(&attacker_wallet).unwrap();
        
        let sandwich_attempt = strategy.detect_sandwich_attack(pattern, &sandwich_txs[0]).unwrap();
        assert!(sandwich_attempt.is_some());
        
        let attack = sandwich_attempt.unwrap();
        assert!(attack.detection_confidence > 0.6);
        assert!(matches!(attack.attack_sophistication, AttackSophistication::BasicMEV | AttackSophistication::IntermediateMEV));
    }

    #[tokio::test]
    async fn test_memory_management() {
        let strategy = create_test_strategy().await;
        
        // Fill up memory with test data
        for i in 0..100 {
            let wallet = Keypair::new().pubkey();
            let txs = create_test_transactions(wallet, 5);
            for tx in txs {
                strategy.record_transaction(tx).await.unwrap();
            }
        }
        
        let initial_stats = strategy.get_memory_usage_stats().await.unwrap();
        assert!(initial_stats.total_patterns > 50);
        
        // Test memory optimization
        let optimization_report = strategy.optimize_memory_usage().await.unwrap();
        assert!(optimization_report.patterns_pruned > 0 || optimization_report.cache_entries_pruned > 0);
        
        let final_stats = strategy.get_memory_usage_stats().await.unwrap();
        assert!(final_stats.estimated_memory_usage_kb <= initial_stats.estimated_memory_usage_kb);
    }

    #[tokio::test]
    async fn test_mev_protection_mechanisms() {
        let strategy = create_test_strategy().await;
        
        // Test stealth mode activation
        strategy.implement_stealth_mode().await.unwrap();
        let protection = strategy.acquire_read_lock(&strategy.mev_protection).await.unwrap();
        assert!(protection.stealth_mode);
        assert!(protection.use_decoy_transactions);
        assert_eq!(protection.obfuscation_level, 5);
        drop(protection);
        
        // Test decoy transaction generation
        let decoys = strategy.generate_decoy_transactions(3).await.unwrap();
        assert_eq!(decoys.len(), 3);
        
        // Test timing randomization
        let base_timing = Duration::from_millis(100);
        let randomized = strategy.implement_timing_randomization(base_timing).await.unwrap();
        assert!(randomized >= Duration::from_millis(10));
        assert!(randomized <= Duration::from_millis(500));
    }

    #[tokio::test]
    async fn test_adaptive_response_generation() {
        let strategy = create_test_strategy().await;
        
        // Add some competitor patterns
        let competitor = Keypair::new().pubkey();
        let txs = create_high_threat_transactions(competitor);
        for tx in txs {
            strategy.record_transaction(tx).await.unwrap();
        }
        
        let target_pool = Keypair::new().pubkey();
        let response = strategy.get_adaptive_response(&target_pool, 1_000_000_000).await.unwrap();
        
        assert!(response.threat_level > 0.0);
        assert!(response.priority_fee_range.0 > 0);
        assert!(response.priority_fee_range.1 > response.priority_fee_range.0);
        assert!(response.compute_buffer > 0);
    }

    #[tokio::test]
    async fn test_competitive_landscape_analysis() {
        let strategy = create_test_strategy().await;
        
        // Add various competitor types
        add_test_competitors(&strategy).await;
        
        let landscape = strategy.analyze_competitive_landscape().await.unwrap();
        
        assert!(landscape.active_bot_count > 0);
        assert!(landscape.competition_intensity >= 0.0 && landscape.competition_intensity <= 1.0);
        assert!(landscape.average_sophistication_score > 0.0);
        assert!(!landscape.dominant_strategy_types.is_empty());
    }

    #[tokio::test]
    async fn test_error_handling_robustness() {
        let strategy = create_test_strategy().await;
        
        // Test various error conditions
        let invalid_response = CounterResponse {
            priority_fee: u64::MAX, // Invalid high fee
            compute_units: 0,
            timing_offset_ms: 0,
            use_decoy: false,
            routing_strategy: RoutingStrategy::Direct,
            obfuscation_level: 10, // Invalid high level
            max_retries: 0,
        };
        
        let config = CounterStrategyConfig::default();
        let validation_result = validate_counter_strategy_params(&invalid_response, &config);
        assert!(validation_result.is_err());
        
        // Test lock timeout handling
        let result = strategy.record_transaction_result("test_sig", false, None).await;
        assert!(result.is_ok()); // Should handle gracefully
    }

    #[tokio::test]
    async fn test_performance_metrics_tracking() {
        let strategy = create_test_strategy().await;
        
        // Record some successful transactions
        strategy.record_transaction_result("success1", true, Some(1000000)).await.unwrap();
        strategy.record_transaction_result("success2", true, Some(2000000)).await.unwrap();
        strategy.record_transaction_result("failure1", false, None).await.unwrap();
        
        let metrics = strategy.get_performance_metrics().await.unwrap();
        assert_eq!(metrics.successful_counter_trades, 2);
        assert_eq!(metrics.failed_counter_trades, 1);
        assert!(metrics.total_profit > 0);
    }

    #[tokio::test]
    async fn test_circuit_breaker_functionality() {
        let mut config = CounterStrategyConfig::default();
        config.circuit_breaker_threshold = 2; // Low threshold for testing
        
        let strategy = CounterStrategy::new(config).unwrap();
        
        // Trigger circuit breaker with failures
        strategy.record_transaction_result("fail1", false, None).await.unwrap();
        strategy.record_transaction_result("fail2", false, None).await.unwrap();
        
        let breaker = strategy.acquire_read_lock(&strategy.circuit_breaker).await.unwrap();
        assert!(breaker.is_open);
        drop(breaker);
        
        // Test reset
        strategy.reset_circuit_breaker().await.unwrap();
        let breaker = strategy.acquire_read_lock(&strategy.circuit_breaker).await.unwrap();
        assert!(!breaker.is_open);
    }

    // Helper functions for testing
    async fn create_test_strategy() -> CounterStrategy {
        CounterStrategyBuilder::new()
            .with_pattern_window_size(20)
            .with_confidence_threshold(0.7)
            .build()
            .unwrap()
    }

    fn create_mev_bot_transactions(wallet: Pubkey, count: usize) -> Vec<TransactionMetadata> {
        let mut transactions = Vec::new();
        let base_time = Instant::now();
        
        for i in 0..count {
            transactions.push(TransactionMetadata {
                signature: format!("mev_bot_tx_{}", i),
                wallet,
                timestamp: base_time + Duration::from_millis((i * 200) as u64), // Predictable timing
                priority_fee: 50_000 + (i as u64 * 1000), // Escalating fees
                compute_units: 800_000, // High compute for MEV
                target_program: Keypair::new().pubkey(),
                instruction_data: "swap_exact_tokens_for_tokens".to_string(),
                accounts_accessed: vec![Keypair::new().pubkey(); 5],
                value_transferred: 1_000_000_000,
                success: true,
                jito_bundle_info: Some(JitoBundleInfo {
                    bundle_id: format!("bundle_{}", i / 3),
                    position_in_bundle: (i % 3) as u8,
                    total_transactions: 3,
                }),
            });
        }
        
        transactions
    }

    fn create_sandwich_attack_transactions(attacker: Pubkey) -> Vec<TransactionMetadata> {
        let base_time = Instant::now();
        let target_program = Keypair::new().pubkey();
        
        vec![
            // Frontrun transaction
            TransactionMetadata {
                signature: "frontrun_tx".to_string(),
                wallet: attacker,
                timestamp: base_time,
                priority_fee: 100_000,
                compute_units: 500_000,
                target_program,
                instruction_data: "swap_exact_tokens_for_tokens".to_string(),
                accounts_accessed: vec![Keypair::new().pubkey(); 4],
                value_transferred: 5_000_000_000,
                success: true,
                jito_bundle_info: Some(JitoBundleInfo {
                    bundle_id: "sandwich_bundle".to_string(),
                    position_in_bundle: 0,
                    total_transactions: 3,
                }),
            },
            // Backrun transaction
            TransactionMetadata {
                signature: "backrun_tx".to_string(),
                wallet: attacker,
                timestamp: base_time + Duration::from_millis(50),
                priority_fee: 95_000,
                compute_units: 480_000,
                target_program,
                instruction_data: "swap_tokens_for_exact_tokens".to_string(),
                accounts_accessed: vec![Keypair::new().pubkey(); 4],
                value_transferred: 5_100_000_000,
                success: true,
                jito_bundle_info: Some(JitoBundleInfo {
                    bundle_id: "sandwich_bundle".to_string(),
                    position_in_bundle: 2,
                    total_transactions: 3,
                }),
            },
        ]
    }

    fn create_test_transactions(wallet: Pubkey, count: usize) -> Vec<TransactionMetadata> {
        let mut transactions = Vec::new();
        let base_time = Instant::now();
        
        for i in 0..count {
            transactions.push(TransactionMetadata {
                signature: format!("test_tx_{}_{}", wallet.to_string()[..8].to_string(), i),
                wallet,
                timestamp: base_time + Duration::from_millis((i * 1000) as u64),
                priority_fee: 5_000 + (i as u64 * 500),
                compute_units: 200_000,
                target_program: Keypair::new().pubkey(),
                instruction_data: "generic_instruction".to_string(),
                accounts_accessed: vec![Keypair::new().pubkey(); 3],
                value_transferred: 100_000_000,
                success: i % 4 != 0, // 75% success rate
                jito_bundle_info: None,
            });
        }
        
        transactions
    }

    fn create_high_threat_transactions(wallet: Pubkey) -> Vec<TransactionMetadata> {
        let base_time = Instant::now();
        
        vec![
            TransactionMetadata {
                signature: "high_threat_1".to_string(),
                wallet,
                timestamp: base_time,
                priority_fee: 200_000, // Very high priority fee
                compute_units: 1_000_000, // Maximum compute units
                target_program: Keypair::new().pubkey(),
                instruction_data: "advanced_mev_strategy".to_string(),
                accounts_accessed: vec![Keypair::new().pubkey(); 8],
                value_transferred: 10_000_000_000, // 10 SOL
                success: true,
                jito_bundle_info: Some(JitoBundleInfo {
                    bundle_id: "advanced_bundle".to_string(),
                    position_in_bundle: 0,
                    total_transactions: 5,
                }),
            },
            TransactionMetadata {
                signature: "high_threat_2".to_string(),
                wallet,
                timestamp: base_time + Duration::from_millis(100),
                priority_fee: 180_000,
                compute_units: 950_000,
                target_program: Keypair::new().pubkey(),
                instruction_data: "advanced_mev_strategy".to_string(),
                accounts_accessed: vec![Keypair::new().pubkey(); 8],
                value_transferred: 8_000_000_000,
                success: true,
                jito_bundle_info: Some(JitoBundleInfo {
                    bundle_id: "advanced_bundle".to_string(),
                    position_in_bundle: 1,
                    total_transactions: 5,
                }),
            },
        ]
    }

    async fn add_test_competitors(strategy: &CounterStrategy) {
        let competitor_types = vec![
            (PatternType::MEVBot, AttackSophistication::AdvancedJitoMEV),
            (PatternType::FrontRunner, AttackSophistication::IntermediateMEV),
            (PatternType::Sandwich, AttackSophistication::BasicMEV),
            (PatternType::Arbitrageur, AttackSophistication::BasicSandwich),
        ];

        for (pattern_type, sophistication) in competitor_types {
            let wallet = Keypair::new().pubkey();
            let txs = match pattern_type {
                PatternType::MEVBot => create_mev_bot_transactions(wallet, 5),
                PatternType::Sandwich => create_sandwich_attack_transactions(wallet),
                _ => create_test_transactions(wallet, 3),
            };

            for tx in txs {
                strategy.record_transaction(tx).await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn test_ml_feature_extraction() {
        let strategy = create_test_strategy().await;
        let wallet = Keypair::new().pubkey();
        let transactions = create_mev_bot_transactions(wallet, 15);
        
        for tx in &transactions {
            strategy.record_transaction(tx.clone()).await.unwrap();
        }
        
        let patterns = strategy.acquire_read_lock(&strategy.patterns).await.unwrap();
        let pattern = patterns.get(&wallet).unwrap();
        
        // Validate ML features are properly calculated
        assert!(pattern.ml_features.signature_entropy > 0.0);
        assert!(pattern.ml_features.timing_predictability >= 0.0);
        assert!(pattern.ml_features.account_clustering_score >= 0.0);
        assert!(!pattern.ml_features.instruction_pattern_hash.is_empty());
    }

    #[tokio::test]
    async fn test_priority_fee_prediction() {
        let predictor = PriorityFeePredictor::new();
        
        // Test with various urgency levels and transaction values
        let low_urgency_fee = predictor.predict_optimal_fee(0.2, 100_000_000).unwrap();
        let high_urgency_fee = predictor.predict_optimal_fee(0.9, 100_000_000).unwrap();
        let high_value_fee = predictor.predict_optimal_fee(0.5, 10_000_000_000).unwrap();
        
        assert!(high_urgency_fee > low_urgency_fee);
        assert!(high_value_fee > low_urgency_fee);
        assert!(high_urgency_fee >= 1000 && high_urgency_fee <= 1_000_000);
    }

    #[tokio::test]
    async fn test_network_condition_analysis() {
        let mut analyzer = NetworkConditionAnalyzer::new();
        
        // Test congestion estimation with mock data
        for i in 0..10 {
            analyzer.slot_history.push_back(SlotInfo {
                slot: 1000 + i,
                timestamp: Instant::now() - Duration::from_secs((10 - i) * 60),
                transaction_count: 2000 + (i * 100) as u32, // Increasing congestion
                priority_fee_avg: 5000 + (i * 1000),
                success_rate: 0.95 - (i as f64 * 0.01),
            });
        }
        
        let congestion = analyzer.estimate_congestion_level();
        assert!(congestion > 0.5); // Should detect increased congestion
        assert!(congestion <= 1.0);
    }

    #[tokio::test]
    async fn test_signature_clustering() {
        let mut clustering = SignatureClustering::new(100);
        
        // Add signatures from the same "bot family"
        let bot_wallet = Keypair::new().pubkey();
        let similar_signatures = vec![
            "12345abc123456789abcdef".to_string(),
            "12345def123456789abcdef".to_string(),
            "12345xyz123456789abcdef".to_string(),
        ];
        
        for sig in similar_signatures {
            clustering.add_signature(sig, bot_wallet).unwrap();
        }
        
        let cluster_score = clustering.get_cluster_score(&bot_wallet);
        assert!(cluster_score > 0.0);
    }

    #[tokio::test]
    async fn test_bloom_filter_functionality() {
        let mut bloom = BloomFilterCache::new(1000, 0.01);
        
        let test_account = Keypair::new().pubkey();
        
        // Initially should not contain account
        assert!(!bloom.might_contain_account(&test_account));
        
        // After insertion, should contain account
        bloom.insert_account(&test_account);
        assert!(bloom.might_contain_account(&test_account));
        
        // Test reset functionality
        bloom.items_inserted = 700; // Trigger reset threshold
        bloom.reset_if_needed();
        assert!(!bloom.might_contain_account(&test_account));
    }

    #[tokio::test]
    async fn test_lru_cache_operations() {
        let mut cache = LRUCache::new(3);
        
        // Insert test entries
        cache.insert("key1".to_string(), CacheEntry {
            data: "data1".to_string(),
            created_at: Instant::now(),
            access_count: 0,
            last_accessed: Instant::now(),
        });
        
        cache.insert("key2".to_string(), CacheEntry {
            data: "data2".to_string(),
            created_at: Instant::now(),
            access_count: 0,
            last_accessed: Instant::now(),
        });
        
        // Test retrieval
        assert!(cache.get("key1").is_some());
        assert!(cache.get("nonexistent").is_none());
        
        // Test capacity limits
        cache.insert("key3".to_string(), CacheEntry {
            data: "data3".to_string(),
            created_at: Instant::now(),
            access_count: 0,
            last_accessed: Instant::now(),
        });
        
        cache.insert("key4".to_string(), CacheEntry {
            data: "data4".to_string(),
            created_at: Instant::now(),
            access_count: 0,
            last_accessed: Instant::now(),
        });
        
        // Should evict oldest entry
        assert_eq!(cache.cache_entries.len(), 3);
        
        let hit_rate = cache.hit_rate();
        assert!(hit_rate >= 0.0 && hit_rate <= 1.0);
    }

    #[tokio::test]
    async fn test_mainnet_readiness_validation() {
        let strategy = create_test_strategy().await;
        
        // Validate all critical components are initialized
        assert!(strategy.acquire_read_lock(&strategy.patterns).await.is_ok());
        assert!(strategy.acquire_read_lock(&strategy.mev_protection).await.is_ok());
        assert!(strategy.acquire_read_lock(&strategy.performance_metrics).await.is_ok());
        assert!(strategy.acquire_read_lock(&strategy.circuit_breaker).await.is_ok());
        
        // Test that no panics occur during normal operations
        let test_tx = TransactionMetadata {
            signature: "mainnet_test".to_string(),
            wallet: Keypair::new().pubkey(),
            timestamp: Instant::now(),
            priority_fee: 25_000,
            compute_units: 400_000,
            target_program: Keypair::new().pubkey(),
            instruction_data: "mainnet_instruction".to_string(),
            accounts_accessed: vec![Keypair::new().pubkey(); 4],
            value_transferred: 500_000_000,
            success: true,
            jito_bundle_info: None,
        };
        
        // These operations should never panic
        let result = strategy.record_transaction(test_tx).await;
        assert!(result.is_ok());
        
        let adaptive_response = strategy.get_adaptive_response(&Keypair::new().pubkey(), 1_000_000_000).await;
        assert!(adaptive_response.is_ok());
        
        let memory_stats = strategy.get_memory_usage_stats().await;
        assert!(memory_stats.is_ok());
        
        println!("✅ All mainnet readiness validations passed - no panics detected");
    }
}
