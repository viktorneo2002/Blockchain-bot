use solana_client::{
    nonblocking::rpc_client::RpcClient,
    tpu_client::{TpuClient, TpuClientConfig, TpuSenderError},
};
use solana_sdk::{
    clock::Slot,
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::Signature,
    epoch_info::EpochInfo,
    transaction::Transaction,
    system_instruction,
    signer::{Signer, keypair::Keypair},
};
use std::{
    collections::{HashMap, VecDeque, BTreeMap, BTreeSet},
    sync::{
        atomic::{AtomicU64, AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex, RwLock, Semaphore, broadcast},
    time::{interval, timeout, sleep},
};
use serde::{Deserialize, Serialize};
use anyhow::{Result, Context, anyhow};
use dashmap::DashMap;
use log::{info, warn, error, debug};
use thiserror::Error;
use reqwest::Client as HttpClient;

// Enhanced Error Types for Production
#[derive(Debug, Error)]
pub enum ProfilerError {
    #[error("RPC error: {0}")]
    RpcError(#[from] solana_client::client_error::ClientError),
    #[error("TPU error: {0}")]
    TpuError(#[from] TpuSenderError),
    #[error("Validator not found: {0}")]
    ValidatorNotFound(Pubkey),
    #[error("TPU connection failed: {0}")]
    TpuConnectionError(String),
    #[error("Circuit breaker open for validator: {0}")]
    CircuitBreakerOpen(Pubkey),
    #[error("Jito API error: {0}")]
    JitoApiError(String),
    #[error("Configuration error: {0}")]
    ConfigError(String),
    #[error("Memory limit exceeded: current {current_mb}MB > limit {limit_mb}MB")]
    MemoryLimitExceeded { current_mb: usize, limit_mb: usize },
    #[error("Insufficient samples for statistics: {samples} < {min_required}")]
    InsufficientSamples { samples: usize, min_required: usize },
    #[error("Timeout error: {operation} took longer than {timeout_ms}ms")]
    TimeoutError { operation: String, timeout_ms: u64 },
    #[error("Leader schedule error: {0}")]
    LeaderScheduleError(String),
    #[error("Invalid stake weight: {0}")]
    InvalidStakeWeight(u64),
}

#[derive(Debug, Error)]
pub enum CircuitBreakerError {
    #[error("Circuit breaker is open")]
    Open,
    #[error("Circuit breaker is half-open")]
    HalfOpen,
    #[error("Circuit breaker operation failed: {0}")]
    OperationFailed(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum CircuitBreakerState {
    Closed,
    Open,
    HalfOpen,
}

// Production Circuit Breaker Implementation
#[derive(Debug)]
pub struct CircuitBreaker {
    state: Arc<RwLock<CircuitBreakerState>>,
    failure_count: Arc<AtomicU32>,
    failure_threshold: u32,
    recovery_timeout: Duration,
    last_failure_time: Arc<RwLock<Option<Instant>>>,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, recovery_timeout: Duration) -> Self {
        Self {
            state: Arc::new(RwLock::new(CircuitBreakerState::Closed)),
            failure_count: Arc::new(AtomicU32::new(0)),
            failure_threshold,
            recovery_timeout,
            last_failure_time: Arc::new(RwLock::new(None)),
        }
    }
    
    pub async fn call<F, T, E>(&self, operation: F) -> Result<T, CircuitBreakerError>
    where
        F: FnOnce() -> Result<T, E>,
        E: std::fmt::Display,
    {
        // Check current state
        let current_state = {
            let state_guard = self.state.read().await;
            state_guard.clone()
        };
        
        match current_state {
            CircuitBreakerState::Open => {
                // Check if recovery timeout has passed
                if let Some(last_failure) = *self.last_failure_time.read().await {
                    if last_failure.elapsed() >= self.recovery_timeout {
                        // Move to half-open state
                        *self.state.write().await = CircuitBreakerState::HalfOpen;
                        self.try_operation(operation).await
                    } else {
                        Err(CircuitBreakerError::Open)
                    }
                } else {
                    Err(CircuitBreakerError::Open)
                }
            }
            CircuitBreakerState::HalfOpen => {
                self.try_operation(operation).await
            }
            CircuitBreakerState::Closed => {
                self.try_operation(operation).await
            }
        }
    }
    
    async fn try_operation<F, T, E>(&self, operation: F) -> Result<T, CircuitBreakerError>
    where
        F: FnOnce() -> Result<T, E>,
        E: std::fmt::Display,
    {
        match operation() {
            Ok(result) => {
                // Success - reset failure count and close circuit
                self.failure_count.store(0, Ordering::SeqCst);
                *self.state.write().await = CircuitBreakerState::Closed;
                Ok(result)
            }
            Err(e) => {
                // Failure - increment counter and check threshold
                let failures = self.failure_count.fetch_add(1, Ordering::SeqCst) + 1;
                *self.last_failure_time.write().await = Some(Instant::now());
                
                if failures >= self.failure_threshold {
                    *self.state.write().await = CircuitBreakerState::Open;
                }
                
                Err(CircuitBreakerError::OperationFailed(e.to_string()))
            }
        }
    }
    
    pub async fn is_closed(&self) -> bool {
        matches!(*self.state.read().await, CircuitBreakerState::Closed)
    }
    
    pub async fn reset(&self) {
        *self.state.write().await = CircuitBreakerState::Closed;
        self.failure_count.store(0, Ordering::SeqCst);
        *self.last_failure_time.write().await = None;
    }
}

// Jito API Response Types
#[derive(Debug, Deserialize)]
struct JitoTipAccountsResponse {
    tip_accounts: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct JitoTipFloorResponse {
    time: String,
    landed_tips_25th_percentile: f64,
    landed_tips_50th_percentile: f64,
    landed_tips_75th_percentile: f64,
    landed_tips_95th_percentile: f64,
    landed_tips_99th_percentile: f64,
    ema_landed_tips_50th_percentile: f64,
}

// Enhanced Configuration Management with comprehensive parameters
#[derive(Debug, Deserialize, Clone)]
pub struct ProfilerConfig {
    // Core latency monitoring parameters
    pub latency_window_size: usize,
    pub outlier_threshold_multiplier: f64,
    pub min_samples_for_stats: usize,
    pub validator_health_check_interval_secs: u64,
    pub latency_probe_interval_ms: u64,
    pub max_concurrent_probes: usize,
    pub probe_timeout_ms: u64,
    pub critical_latency_ms: f64,
    pub optimal_latency_ms: f64,
    pub max_consecutive_failures: u32,
    
    // Network and connection parameters
    pub rpc_timeout_ms: u64,
    pub tpu_timeout_ms: u64,
    pub websocket_timeout_ms: u64,
    pub connection_pool_size: usize,
    pub max_retries: u32,
    pub retry_backoff_ms: u64,
    pub enable_tls: bool,
    pub verify_certificates: bool,
    
    // Stake and validator parameters
    pub stake_update_interval_secs: u64,
    pub min_stake_threshold: u64,
    pub stake_weight_multiplier: f64,
    pub validator_score_decay_rate: f64,
    pub blacklist_duration_secs: u64,
    
    // TPU and transaction parameters
    pub tpu_probe_interval_ms: u64,
    pub tpu_connection_timeout_ms: u64,
    pub transaction_confirmation_timeout_ms: u64,
    pub max_transaction_retries: u32,
    pub transaction_fee_multiplier: f64,
    
    // Leader schedule and MEV parameters
    pub leader_schedule_update_interval_secs: u64,
    pub leader_schedule_lookahead_slots: u64,
    pub mev_opportunity_detection_interval_ms: u64,
    pub min_mev_profit_threshold_lamports: u64,
    pub mev_confidence_threshold: f64,
    pub max_mev_opportunities_per_validator: usize,
    
    // Circuit breaker parameters
    pub circuit_breaker_failure_threshold: u32,
    pub circuit_breaker_recovery_timeout_secs: u64,
    pub circuit_breaker_half_open_max_calls: u32,
    pub circuit_breaker_monitor_interval_secs: u64,
    
    // Jito integration parameters
    pub jito_tip_account_update_interval_secs: u64,
    pub jito_api_url: String,
    pub jito_bundle_api_url: String,
    pub jito_tip_floor_update_interval_secs: u64,
    pub jito_request_timeout_ms: u64,
    pub jito_max_retries: u32,
    
    // Memory and performance parameters
    pub max_memory_usage_mb: usize,
    pub memory_check_interval_secs: u64,
    pub gc_trigger_threshold_mb: usize,
    pub performance_metrics_window_size: usize,
    pub metrics_retention_hours: u64,
    
    // Alert and notification parameters
    pub alert_queue_size: usize,
    pub alert_processing_timeout_ms: u64,
    pub enable_slack_alerts: bool,
    pub slack_webhook_url: Option<String>,
    pub enable_telegram_alerts: bool,
    pub telegram_bot_token: Option<String>,
    pub telegram_chat_id: Option<String>,
    pub enable_email_alerts: bool,
    pub smtp_server: Option<String>,
    pub email_recipients: Vec<String>,
    
    // Monitoring and observability parameters
    pub enable_prometheus_metrics: bool,
    pub prometheus_bind_address: String,
    pub prometheus_port: u16,
    pub enable_jaeger_tracing: bool,
    pub jaeger_endpoint: Option<String>,
    pub log_level: String,
    pub enable_structured_logging: bool,
    
    // Database integration parameters
    pub enable_database_storage: bool,
    pub database_url: Option<String>,
    pub database_connection_pool_size: u32,
    pub database_timeout_ms: u64,
    pub metrics_batch_size: usize,
    pub metrics_flush_interval_secs: u64,
    
    // Load testing parameters
    pub enable_load_testing: bool,
    pub load_test_duration_secs: u64,
    pub load_test_requests_per_second: u32,
    pub load_test_concurrent_connections: usize,
    
    // Geographic distribution parameters
    pub enable_geographic_routing: bool,
    pub preferred_regions: Vec<String>,
    pub region_latency_weights: std::collections::HashMap<String, f64>,
    pub max_cross_region_latency_ms: f64,
}

impl Default for ProfilerConfig {
    fn default() -> Self {
        let mut region_weights = std::collections::HashMap::new();
        region_weights.insert("us-east-1".to_string(), 1.0);
        region_weights.insert("us-west-2".to_string(), 0.9);
        region_weights.insert("eu-west-1".to_string(), 0.8);
        
        Self {
            // Core latency monitoring parameters
            latency_window_size: 5000,
            outlier_threshold_multiplier: 2.5,
            min_samples_for_stats: 50,
            validator_health_check_interval_secs: 15,
            latency_probe_interval_ms: 25,
            max_concurrent_probes: 200,
            probe_timeout_ms: 150,
            critical_latency_ms: 50.0,
            optimal_latency_ms: 15.0,
            max_consecutive_failures: 3,
            
            // Network and connection parameters
            rpc_timeout_ms: 5000,
            tpu_timeout_ms: 3000,
            websocket_timeout_ms: 10000,
            connection_pool_size: 50,
            max_retries: 3,
            retry_backoff_ms: 100,
            enable_tls: true,
            verify_certificates: true,
            
            // Stake and validator parameters
            stake_update_interval_secs: 120,
            min_stake_threshold: 1_000_000_000, // 1 SOL in lamports
            stake_weight_multiplier: 1.5,
            validator_score_decay_rate: 0.95,
            blacklist_duration_secs: 3600, // 1 hour
            
            // TPU and transaction parameters
            tpu_probe_interval_ms: 100,
            tpu_connection_timeout_ms: 2000,
            transaction_confirmation_timeout_ms: 30000,
            max_transaction_retries: 5,
            transaction_fee_multiplier: 1.2,
            
            // Leader schedule and MEV parameters
            leader_schedule_update_interval_secs: 60,
            leader_schedule_lookahead_slots: 100,
            mev_opportunity_detection_interval_ms: 500,
            min_mev_profit_threshold_lamports: 10_000, // 0.00001 SOL
            mev_confidence_threshold: 0.75,
            max_mev_opportunities_per_validator: 10,
            
            // Circuit breaker parameters
            circuit_breaker_failure_threshold: 5,
            circuit_breaker_recovery_timeout_secs: 30,
            circuit_breaker_half_open_max_calls: 3,
            circuit_breaker_monitor_interval_secs: 10,
            
            // Jito integration parameters
            jito_tip_account_update_interval_secs: 30,
            jito_api_url: "https://mainnet.block-engine.jito.wtf".to_string(),
            jito_bundle_api_url: "https://bundles.jito.wtf".to_string(),
            jito_tip_floor_update_interval_secs: 15,
            jito_request_timeout_ms: 5000,
            jito_max_retries: 3,
            
            // Memory and performance parameters
            max_memory_usage_mb: 512,
            memory_check_interval_secs: 30,
            gc_trigger_threshold_mb: 400,
            performance_metrics_window_size: 1000,
            metrics_retention_hours: 24,
            
            // Alert and notification parameters
            alert_queue_size: 1000,
            alert_processing_timeout_ms: 5000,
            enable_slack_alerts: false,
            slack_webhook_url: None,
            enable_telegram_alerts: false,
            telegram_bot_token: None,
            telegram_chat_id: None,
            enable_email_alerts: false,
            smtp_server: None,
            email_recipients: Vec::new(),
            
            // Monitoring and observability parameters
            enable_prometheus_metrics: false,
            prometheus_bind_address: "0.0.0.0".to_string(),
            prometheus_port: 9090,
            enable_jaeger_tracing: false,
            jaeger_endpoint: None,
            log_level: "info".to_string(),
            enable_structured_logging: false,
            
            // Database integration parameters
            enable_database_storage: false,
            database_url: None,
            database_connection_pool_size: 10,
            database_timeout_ms: 30000,
            metrics_batch_size: 100,
            metrics_flush_interval_secs: 60,
            
            // Load testing parameters
            enable_load_testing: false,
            load_test_duration_secs: 300,
            load_test_requests_per_second: 100,
            load_test_concurrent_connections: 50,
            
            // Geographic distribution parameters
            enable_geographic_routing: false,
            preferred_regions: vec!["us-east-1".to_string(), "us-west-2".to_string()],
            region_latency_weights: region_weights,
            max_cross_region_latency_ms: 200.0,
        }
    }
}

impl ProfilerConfig {
    /// Load configuration from TOML/JSON file with environment variable override support
    pub fn from_file(path: &str) -> Result<Self, ProfilerError> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| ProfilerError::ConfigError(format!("Failed to read config file '{}': {}", path, e)))?;
        
        let mut config: Self = if path.ends_with(".json") {
            serde_json::from_str(&contents)
                .map_err(|e| ProfilerError::ConfigError(format!("Failed to parse JSON config: {}", e)))?
        } else {
            toml::from_str(&contents)
                .map_err(|e| ProfilerError::ConfigError(format!("Failed to parse TOML config: {}", e)))?
        };
        
        // Override with environment variables if present
        config.apply_env_overrides()?;
        config.validate()?;
        
        Ok(config)
    }
    
    /// Load configuration from environment variables with comprehensive mapping
    pub fn from_env() -> Result<Self, ProfilerError> {
        let mut config = Self::default();
        config.apply_env_overrides()?;
        config.validate()?;
        Ok(config)
    }
    
    /// Apply environment variable overrides to configuration
    fn apply_env_overrides(&mut self) -> Result<(), ProfilerError> {
        // Core latency monitoring parameters
        if let Ok(val) = std::env::var("LATENCY_WINDOW_SIZE") {
            self.latency_window_size = Self::parse_env_var(&val, "LATENCY_WINDOW_SIZE")?;
        }
        if let Ok(val) = std::env::var("OUTLIER_THRESHOLD_MULTIPLIER") {
            self.outlier_threshold_multiplier = Self::parse_env_var(&val, "OUTLIER_THRESHOLD_MULTIPLIER")?;
        }
        if let Ok(val) = std::env::var("MIN_SAMPLES_FOR_STATS") {
            self.min_samples_for_stats = Self::parse_env_var(&val, "MIN_SAMPLES_FOR_STATS")?;
        }
        if let Ok(val) = std::env::var("VALIDATOR_HEALTH_CHECK_INTERVAL_SECS") {
            self.validator_health_check_interval_secs = Self::parse_env_var(&val, "VALIDATOR_HEALTH_CHECK_INTERVAL_SECS")?;
        }
        if let Ok(val) = std::env::var("LATENCY_PROBE_INTERVAL_MS") {
            self.latency_probe_interval_ms = Self::parse_env_var(&val, "LATENCY_PROBE_INTERVAL_MS")?;
        }
        if let Ok(val) = std::env::var("MAX_CONCURRENT_PROBES") {
            self.max_concurrent_probes = Self::parse_env_var(&val, "MAX_CONCURRENT_PROBES")?;
        }
        if let Ok(val) = std::env::var("PROBE_TIMEOUT_MS") {
            self.probe_timeout_ms = Self::parse_env_var(&val, "PROBE_TIMEOUT_MS")?;
        }
        if let Ok(val) = std::env::var("CRITICAL_LATENCY_MS") {
            self.critical_latency_ms = Self::parse_env_var(&val, "CRITICAL_LATENCY_MS")?;
        }
        if let Ok(val) = std::env::var("OPTIMAL_LATENCY_MS") {
            self.optimal_latency_ms = Self::parse_env_var(&val, "OPTIMAL_LATENCY_MS")?;
        }
        if let Ok(val) = std::env::var("MAX_CONSECUTIVE_FAILURES") {
            self.max_consecutive_failures = Self::parse_env_var(&val, "MAX_CONSECUTIVE_FAILURES")?;
        }
        
        // Network and connection parameters
        if let Ok(val) = std::env::var("RPC_TIMEOUT_MS") {
            self.rpc_timeout_ms = Self::parse_env_var(&val, "RPC_TIMEOUT_MS")?;
        }
        if let Ok(val) = std::env::var("TPU_TIMEOUT_MS") {
            self.tpu_timeout_ms = Self::parse_env_var(&val, "TPU_TIMEOUT_MS")?;
        }
        if let Ok(val) = std::env::var("WEBSOCKET_TIMEOUT_MS") {
            self.websocket_timeout_ms = Self::parse_env_var(&val, "WEBSOCKET_TIMEOUT_MS")?;
        }
        if let Ok(val) = std::env::var("CONNECTION_POOL_SIZE") {
            self.connection_pool_size = Self::parse_env_var(&val, "CONNECTION_POOL_SIZE")?;
        }
        if let Ok(val) = std::env::var("MAX_RETRIES") {
            self.max_retries = Self::parse_env_var(&val, "MAX_RETRIES")?;
        }
        if let Ok(val) = std::env::var("RETRY_BACKOFF_MS") {
            self.retry_backoff_ms = Self::parse_env_var(&val, "RETRY_BACKOFF_MS")?;
        }
        if let Ok(val) = std::env::var("ENABLE_TLS") {
            self.enable_tls = Self::parse_env_var(&val, "ENABLE_TLS")?;
        }
        if let Ok(val) = std::env::var("VERIFY_CERTIFICATES") {
            self.verify_certificates = Self::parse_env_var(&val, "VERIFY_CERTIFICATES")?;
        }
        
        // Jito integration parameters
        if let Ok(val) = std::env::var("JITO_API_URL") {
            self.jito_api_url = val;
        }
        if let Ok(val) = std::env::var("JITO_BUNDLE_API_URL") {
            self.jito_bundle_api_url = val;
        }
        if let Ok(val) = std::env::var("JITO_TIP_ACCOUNT_UPDATE_INTERVAL_SECS") {
            self.jito_tip_account_update_interval_secs = Self::parse_env_var(&val, "JITO_TIP_ACCOUNT_UPDATE_INTERVAL_SECS")?;
        }
        
        // Circuit breaker parameters
        if let Ok(val) = std::env::var("CIRCUIT_BREAKER_FAILURE_THRESHOLD") {
            self.circuit_breaker_failure_threshold = Self::parse_env_var(&val, "CIRCUIT_BREAKER_FAILURE_THRESHOLD")?;
        }
        
        // Memory parameters
        if let Ok(val) = std::env::var("MAX_MEMORY_USAGE_MB") {
            self.max_memory_usage_mb = Self::parse_env_var(&val, "MAX_MEMORY_USAGE_MB")?;
        }
        
        // Database integration parameters
        if let Ok(val) = std::env::var("ENABLE_DATABASE_STORAGE") {
            self.enable_database_storage = Self::parse_env_var(&val, "ENABLE_DATABASE_STORAGE")?;
        }
        if let Ok(val) = std::env::var("DATABASE_URL") {
            self.database_url = Some(val);
        }
        if let Ok(val) = std::env::var("JITO_TIP_FLOOR_UPDATE_INTERVAL_SECS") {
            self.jito_tip_floor_update_interval_secs = Self::parse_env_var(&val, "JITO_TIP_FLOOR_UPDATE_INTERVAL_SECS")?;
        }
        if let Ok(val) = std::env::var("JITO_REQUEST_TIMEOUT_MS") {
            self.jito_request_timeout_ms = Self::parse_env_var(&val, "JITO_REQUEST_TIMEOUT_MS")?;
        }
        if let Ok(val) = std::env::var("JITO_MAX_RETRIES") {
            self.jito_max_retries = Self::parse_env_var(&val, "JITO_MAX_RETRIES")?;
        }
        
        Ok(())
    }
    
    /// Create a sample configuration file for reference
    pub fn create_sample_config_file(path: &str) -> Result<(), ProfilerError> {
        let sample_config = ProfilerConfig::default();
        let content = toml::to_string_pretty(&sample_config)
            .map_err(|e| ProfilerError::ConfigError(format!("Failed to serialize config: {}", e)))?;
        
        std::fs::write(path, content)
            .map_err(|e| ProfilerError::ConfigError(format!("Failed to write config file: {}", e)))?;
        
        log::info!("Sample configuration file created at: {}", path);
        Ok(())
    }
    
    /// Print all available environment variables for easy reference
    pub fn print_env_vars_help() {
        println!("=== Validator Latency Profiler Environment Variables ===");
        println!("Core Parameters:");
        println!("  LATENCY_WINDOW_SIZE - Window size for latency tracking (default: 5000)");
        println!("  CRITICAL_LATENCY_MS - Critical latency threshold (default: 50.0)");
        println!("  MAX_CONCURRENT_PROBES - Max concurrent probes (default: 200)");
        println!("  PROBE_TIMEOUT_MS - Probe timeout in milliseconds (default: 150)");
        println!();
        println!("Jito Parameters:");
        println!("  JITO_API_URL - Jito API endpoint URL");
        println!("  JITO_BUNDLE_API_URL - Jito bundle API endpoint URL");
        println!("  JITO_TIP_ACCOUNT_UPDATE_INTERVAL_SECS - Tip account refresh interval");
        println!();
        println!("Monitoring Parameters:");
        println!("  ENABLE_PROMETHEUS_METRICS - Enable Prometheus metrics export (true/false)");
        println!("  PROMETHEUS_PORT - Prometheus metrics port (default: 9090)");
        println!("  ENABLE_SLACK_ALERTS - Enable Slack notifications (true/false)");
        println!("  SLACK_WEBHOOK_URL - Slack webhook URL for alerts");
        println!();
        println!("For full list of variables, check the ProfilerConfig struct or sample config file.");
    }
    
    /// Parse environment variable with proper error handling
    fn parse_env_var<T: std::str::FromStr>(value: &str, var_name: &str) -> Result<T, ProfilerError>
    where
        T::Err: std::fmt::Display,
    {
        value.parse().map_err(|e| {
            ProfilerError::ConfigError(format!("Invalid {}: {} - {}", var_name, value, e))
        })
    }
    
    /// Validate configuration parameters for consistency and safety
    pub fn validate(&self) -> Result<(), ProfilerError> {
        // Validate timing parameters
        if self.latency_window_size == 0 {
            return Err(ProfilerError::ConfigError("latency_window_size must be greater than 0".to_string()));
        }
        if self.min_samples_for_stats > self.latency_window_size {
            return Err(ProfilerError::ConfigError("min_samples_for_stats cannot exceed latency_window_size".to_string()));
        }
        if self.probe_timeout_ms == 0 {
            return Err(ProfilerError::ConfigError("probe_timeout_ms must be greater than 0".to_string()));
        }
        if self.max_concurrent_probes == 0 {
            return Err(ProfilerError::ConfigError("max_concurrent_probes must be greater than 0".to_string()));
        }
        
        // Validate latency thresholds
        if self.critical_latency_ms <= self.optimal_latency_ms {
            return Err(ProfilerError::ConfigError("critical_latency_ms must be greater than optimal_latency_ms".to_string()));
        }
        if self.outlier_threshold_multiplier <= 1.0 {
            return Err(ProfilerError::ConfigError("outlier_threshold_multiplier must be greater than 1.0".to_string()));
        }
        
        // Validate circuit breaker parameters
        if self.circuit_breaker_failure_threshold == 0 {
            return Err(ProfilerError::ConfigError("circuit_breaker_failure_threshold must be greater than 0".to_string()));
        }
        if self.circuit_breaker_recovery_timeout_secs == 0 {
            return Err(ProfilerError::ConfigError("circuit_breaker_recovery_timeout_secs must be greater than 0".to_string()));
        }
        
        // Validate MEV parameters
        if self.mev_confidence_threshold < 0.0 || self.mev_confidence_threshold > 1.0 {
            return Err(ProfilerError::ConfigError("mev_confidence_threshold must be between 0.0 and 1.0".to_string()));
        }
        
        // Validate resource limits
        if self.max_memory_usage_mb == 0 {
            return Err(ProfilerError::ConfigError("max_memory_usage_mb must be greater than 0".to_string()));
        }
        if self.alert_queue_size == 0 {
            return Err(ProfilerError::ConfigError("alert_queue_size must be greater than 0".to_string()));
        }
        
        // Validate Prometheus configuration
        if self.enable_prometheus_metrics && self.prometheus_port == 0 {
            return Err(ProfilerError::ConfigError("prometheus_port must be specified when prometheus metrics are enabled".to_string()));
        }
        
        // Validate database configuration
        if self.enable_database_storage && self.database_url.is_none() {
            return Err(ProfilerError::ConfigError("database_url must be specified when database storage is enabled".to_string()));
        }
        
        // Validate notification configuration
        if self.enable_slack_alerts && self.slack_webhook_url.is_none() {
            return Err(ProfilerError::ConfigError("slack_webhook_url must be specified when slack alerts are enabled".to_string()));
        }
        
        Ok(())
    }
    
    /// Load configuration with environment override priority: env vars > config file > defaults
    pub fn load_with_overrides(config_file_path: Option<&str>) -> Result<Self, ProfilerError> {
        let mut config = if let Some(path) = config_file_path {
            Self::from_file(path)?
        } else {
            Self::default()
        };
        
        // Apply environment overrides
        config.apply_env_overrides()?;
        config.validate()?;
        
        log::info!("Configuration loaded successfully");
        log::debug!("Configuration: {:?}", config);
        
        Ok(config)
    }
    
    /// Load configuration with environment override priority: env vars > config file > defaults
    pub fn load_with_overrides(config_file_path: Option<&str>) -> Result<Self, ProfilerError> {
        let mut config = if let Some(path) = config_file_path {
            Self::from_file(path)?
        } else {
            Self::default()
        };
        
        // Apply environment overrides
        config.apply_env_overrides()?;
        config.validate()?;
        
        log::info!("Configuration loaded successfully");
        log::debug!("Configuration: {:?}", config);
        
        Ok(config)
    }
}

// Load testing report structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoadTestReport {
    pub duration_secs: u64,
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub success_rate: f64,
    pub avg_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub requests_per_second: f64,
}

// MEV-optimized constants based on Solana mainnet competition requirements
const SLOT_TRACKING_WINDOW: usize = 400; // Larger window for slot analysis
const PERCENTILE_PRECISION: f64 = 0.001; // High precision percentiles
const LATENCY_SPIKE_THRESHOLD: f64 = 2.0; // Spike detection multiplier
const TEST_TRANSACTION_AMOUNT: u64 = 1; // Lamports for TPU test transactions

// Helper struct for RAII probe counting
struct ProbeGuard<'a> {
    active_probes: &'a AtomicU64,
}

impl<'a> ProbeGuard<'a> {
    fn new(active_probes: &'a AtomicU64) -> Self {
        active_probes.fetch_add(1, Ordering::AcqRel);
        Self { active_probes }
    }
}

impl<'a> Drop for ProbeGuard<'a> {
    fn drop(&mut self) {
        self.active_probes.fetch_sub(1, Ordering::AcqRel);
    }
}

// MEV Opportunity Detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MevOpportunity {
    pub validator: Pubkey,
    pub opportunity_type: MevOpportunityType,
    pub estimated_profit_lamports: u64,
    pub confidence_score: f64,
    pub expiry_slot: Slot,
    pub required_stake: u64,
    pub gas_cost_estimate: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MevOpportunityType {
    Arbitrage { dex_a: String, dex_b: String, token_pair: String },
    Liquidation { protocol: String, account: Pubkey },
    Sandwiching { target_transaction: Signature },
    JitoBundle { bundle_id: String, tip_amount: u64 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorMetrics {
    pub identity: Pubkey,
    pub rpc_endpoint: String,
    pub tpu_endpoint: String,
    pub last_update_ms: u64,
    pub slot_latencies: VecDeque<SlotLatency>,
    pub transaction_latencies: VecDeque<TransactionLatency>,
    pub tpu_latencies: VecDeque<TpuLatency>, // New: TPU latency tracking
    pub health_score: f64,
    pub success_rate: f64,
    pub avg_latency_ms: f64,
    pub median_latency_ms: f64,
    pub p75_latency_ms: f64,
    pub p90_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub p999_latency_ms: f64, // New: P99.9 for MEV precision
    pub jitter_ms: f64,
    pub total_probes: u64,
    pub successful_probes: u64,
    pub consecutive_failures: u32,
    pub is_active: bool,
    pub stake_weight: u64,
    pub commission: u8,
    pub version: Option<String>,
    pub last_slot: Slot,
    pub slots_behind: u64,
    pub is_leader_next_slots: Vec<Slot>, // New: Leader schedule tracking
    pub jito_enabled: bool, // New: Jito MEV support detection
    pub mev_rewards_earned: u64, // New: MEV performance tracking
    pub bundle_success_rate: f64, // New: Bundle landing rate
    pub tpu_connection_quality: f64, // New: TPU connection health
    pub geographic_region: Option<String>, // New: Geographic latency optimization
    pub last_leader_slot: Option<Slot>, // New: Last time this validator was leader
    pub skip_rate: f64, // New: Slot skip rate tracking
    pub vote_latency_ms: f64, // New: Vote transaction latency
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotLatency {
    pub slot: Slot,
    pub latency_ms: f64,
    pub timestamp_ms: u64,
    pub is_leader_slot: bool, // New: Track if this was a leader slot
    pub block_height: Option<u64>, // New: Block height for analysis
    pub transaction_count: Option<u64>, // New: Txn count in this slot
    pub priority_fees_included: bool, // New: Priority fee usage tracking
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TpuLatency {
    pub timestamp_ms: u64,
    pub connect_latency_ms: f64,
    pub send_latency_ms: f64,
    pub ack_latency_ms: Option<f64>,
    pub connection_success: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionLatency {
    pub signature: Signature,
    pub submission_time_ms: u64,
    pub confirmation_time_ms: Option<u64>,
    pub latency_ms: Option<f64>,
    pub slot: Slot,
    pub status: TxStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum TxStatus {
    Pending,
    Confirmed,
    Failed,
    Timeout,
}

pub struct ValidatorLatencyProfiler {
    validators: Arc<DashMap<Pubkey, Arc<RwLock<ValidatorMetrics>>>>,
    rpc_clients: Arc<DashMap<Pubkey, Arc<RpcClient>>>,
    tpu_clients: Arc<DashMap<Pubkey, Arc<TpuClient>>>, // Real TPU clients
    probe_semaphore: Arc<Semaphore>,
    active_probes: Arc<AtomicU64>,
    total_probes: Arc<AtomicU64>,
    successful_probes: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
    global_best_slot: Arc<AtomicU64>,
    config: ProfilerConfig,
    circuit_breakers: DashMap<Pubkey, CircuitBreaker>,
    http_client: HttpClient,
    stake_weights: Arc<RwLock<HashMap<Pubkey, u64>>>,
    latency_cache: Arc<Mutex<BTreeMap<(Pubkey, u64), f64>>>,
    leader_schedule: Arc<RwLock<BTreeMap<Slot, Pubkey>>>,
    jito_tip_accounts: Arc<RwLock<Vec<Pubkey>>>,
    current_tip_floor: Arc<RwLock<Option<JitoTipFloorResponse>>>,
    test_keypair: Arc<Keypair>,
    memory_monitor: Arc<AtomicU64>,
    performance_metrics: Arc<PerformanceMetrics>,
    alert_sender: broadcast::Sender<AlertEvent>,
    epoch_info: Arc<RwLock<Option<EpochInfo>>>,
}

// New supporting structures for MEV optimization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub total_latency_measurements: AtomicU64,
    pub avg_network_latency_ms: AtomicU64, // Stored as integer microseconds for atomic ops
    pub p99_latency_samples: Arc<Mutex<VecDeque<f64>>>,
    pub validator_ranking_cache: Arc<RwLock<Vec<(Pubkey, f64)>>>,
    pub mev_opportunities_detected: AtomicU64,
    pub bundle_submission_success_rate: AtomicU64, // Stored as percentage * 100
    pub geographic_latency_map: Arc<RwLock<HashMap<String, f64>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertEvent {
    ValidatorDown { validator: Pubkey, reason: String },
    LatencySpike { validator: Pubkey, latency_ms: f64, threshold_ms: f64 },
    MevOpportunity { validator: Pubkey, estimated_profit: f64 },
    CircuitBreakerTripped { validator: Pubkey },
    MemoryUsageHigh { current_mb: usize, limit_mb: usize },
    JitoTipAccountsUpdated { count: usize },
    LeaderScheduleUpdated { epoch: u64, validator_count: usize },
}


impl ValidatorLatencyProfiler {
    /// Export Prometheus metrics for monitoring and observability
    pub async fn export_prometheus_metrics(&self) -> Result<String, ProfilerError> {
        if !self.config.enable_prometheus_metrics {
            return Err(ProfilerError::ConfigError("Prometheus metrics not enabled".to_string()));
        }
        
        let mut metrics = Vec::new();
        
        // Validator latency metrics
        for validator_entry in self.validators.iter() {
            let validator = *validator_entry.key();
            let metrics_guard = validator_entry.value().try_read()
                .map_err(|e| ProfilerError::ConfigError(format!("Failed to read validator metrics: {}", e)))?;
            
            if let Some(avg_latency) = metrics_guard.average_latency() {
                metrics.push(format!(
                    "validator_average_latency_ms{{validator=\"{}\"}} {}",
                    validator, avg_latency
                ));
            }
            
            metrics.push(format!(
                "validator_health_score{{validator=\"{}\"}} {}",
                validator, metrics_guard.health_score
            ));
            
            metrics.push(format!(
                "validator_stake_weight{{validator=\"{}\"}} {}",
                validator, metrics_guard.stake_weight
            ));
            
            metrics.push(format!(
                "validator_is_active{{validator=\"{}\"}} {}",
                validator, if metrics_guard.is_active { 1 } else { 0 }
            ));
        }
        
        // Circuit breaker metrics
        for cb_entry in self.circuit_breakers.iter() {
            let validator = *cb_entry.key();
            let cb_state = cb_entry.value().get_state().await?;
            
            metrics.push(format!(
                "circuit_breaker_state{{validator=\"{}\"}} {}",
                validator,
                match cb_state {
                    CircuitBreakerState::Closed => 0,
                    CircuitBreakerState::Open => 1,
                    CircuitBreakerState::HalfOpen => 2,
                }
            ));
            
            metrics.push(format!(
                "circuit_breaker_failure_count{{validator=\"{}\"}} {}",
                validator, cb_entry.value().get_failure_count().await
            ));
        }
        
        // Performance metrics
        let perf_metrics = self.performance_metrics.read()
            .map_err(|e| ProfilerError::ConfigError(format!("Failed to read performance metrics: {}", e)))?;
        
        metrics.push(format!("total_latency_measurements {}", perf_metrics.total_latency_measurements));
        metrics.push(format!("mev_opportunities_detected {}", perf_metrics.mev_opportunities_detected));
        metrics.push(format!("bundle_success_rate {}", perf_metrics.bundle_success_rate));
        
        // Memory usage metrics
        let active_probes = self.active_probes.load(Ordering::Acquire);
        metrics.push(format!("active_probes_count {}", active_probes));
        
        // Jito metrics
        let tip_accounts_count = self.jito_tip_accounts.read()
            .map_err(|e| ProfilerError::ConfigError(format!("Failed to read Jito tip accounts: {}", e)))?
            .len();
        metrics.push(format!("jito_tip_accounts_count {}", tip_accounts_count));
        
        let tip_floor = self.jito_tip_floor.read()
            .map_err(|e| ProfilerError::ConfigError(format!("Failed to read Jito tip floor: {}", e)))?;
        if let Some(floor) = *tip_floor {
            metrics.push(format!("jito_tip_floor_lamports {}", floor));
        }
        
        // Leader schedule metrics
        let leader_schedule_size = self.leader_schedule.read()
            .map_err(|e| ProfilerError::ConfigError(format!("Failed to read leader schedule: {}", e)))?
            .len();
        metrics.push(format!("leader_schedule_size {}", leader_schedule_size));
        
        Ok(metrics.join("\n"))
    }
    
    /// Save validator metrics to database for historical analysis
    pub async fn save_metrics_to_db(&self, validator: Pubkey, metrics: &ValidatorMetrics) -> Result<(), ProfilerError> {
        if !self.config.enable_database_storage {
            return Ok(()); // Skip if database storage is disabled
        }
        
        // This would integrate with a real database like PostgreSQL/TimescaleDB
        // For now, we'll log the metrics in structured format
        log::info!(
            "DB_METRICS: validator={}, health_score={}, avg_latency={:?}, stake_weight={}, active={}, slot_count={}",
            validator,
            metrics.health_score,
            metrics.average_latency(),
            metrics.stake_weight,
            metrics.is_active,
            metrics.slot_latencies.len()
        );
        
        // TODO: Implement actual database insertion here
        // Example pseudo-code:
        // let pool = self.database_pool.as_ref().ok_or(ProfilerError::ConfigError("Database pool not initialized".to_string()))?;
        // sqlx::query!("INSERT INTO validator_metrics (validator, timestamp, health_score, avg_latency, stake_weight, is_active) VALUES ($1, $2, $3, $4, $5, $6)",
        //     validator.to_string(), chrono::Utc::now(), metrics.health_score, metrics.average_latency(), metrics.stake_weight, metrics.is_active)
        //     .execute(pool).await?;
        
        Ok(())
    }
    
    /// Run load testing to validate profiler performance under high load
    pub async fn run_load_test(&self, duration: Duration, requests_per_second: u32) -> Result<LoadTestReport, ProfilerError> {
        if !self.config.enable_load_testing {
            return Err(ProfilerError::ConfigError("Load testing not enabled".to_string()));
        }
        
        log::info!("Starting load test: duration={:?}, rps={}", duration, requests_per_second);
        
        let start_time = Instant::now();
        let mut total_requests = 0u64;
        let mut successful_requests = 0u64;
        let mut failed_requests = 0u64;
        let mut latencies = Vec::new();
        
        let interval_ms = 1000 / requests_per_second as u64;
        let mut interval = interval(Duration::from_millis(interval_ms));
        
        while start_time.elapsed() < duration {
            interval.tick().await;
            
            // Select random validator for load testing
            let validators: Vec<Pubkey> = self.validators.iter().map(|entry| *entry.key()).collect();
            if validators.is_empty() {
                continue;
            }
            
            let validator = validators[total_requests as usize % validators.len()];
            let probe_start = Instant::now();
            
            match self.probe_validator_latency(validator).await {
                Ok(_) => {
                    successful_requests += 1;
                    latencies.push(probe_start.elapsed().as_secs_f64() * 1000.0);
                }
                Err(_) => {
                    failed_requests += 1;
                }
            }
            
            total_requests += 1;
        }
        
        // Calculate statistics
        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let avg_latency = if !latencies.is_empty() {
            latencies.iter().sum::<f64>() / latencies.len() as f64
        } else {
            0.0
        };
        
        let p95_latency = if !latencies.is_empty() {
            let p95_index = (latencies.len() as f64 * 0.95) as usize;
            latencies.get(p95_index.min(latencies.len() - 1)).copied().unwrap_or(0.0)
        } else {
            0.0
        };
        
        let success_rate = if total_requests > 0 {
            successful_requests as f64 / total_requests as f64
        } else {
            0.0
        };
        
        let report = LoadTestReport {
            duration_secs: start_time.elapsed().as_secs(),
            total_requests,
            successful_requests,
            failed_requests,
            success_rate,
            avg_latency_ms: avg_latency,
            p95_latency_ms: p95_latency,
            requests_per_second: total_requests as f64 / start_time.elapsed().as_secs_f64(),
        };
        
        log::info!("Load test completed: {:?}", report);
        Ok(report)
    }
    
    /// Monitor memory usage and trigger garbage collection if needed
    async fn run_memory_monitor(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(Duration::from_secs(self.config.memory_check_interval_secs));
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            // Get current memory usage (simplified implementation)
            let memory_usage_mb = self.get_memory_usage_mb().await?;
            
            // Check if memory usage exceeds the limit
            if memory_usage_mb > self.config.max_memory_usage_mb {
                let alert = AlertEvent::MemoryUsageHigh {
                    current_mb: memory_usage_mb,
                    limit_mb: self.config.max_memory_usage_mb,
                };
                
                let _ = self.alert_sender.send(alert);
                
                // Trigger garbage collection if threshold is exceeded
                if memory_usage_mb > self.config.gc_trigger_threshold_mb {
                    self.trigger_memory_cleanup().await?;
                }
            }
        }
        
        Ok(())
    }
    
    /// Get current memory usage in MB (simplified implementation)
    async fn get_memory_usage_mb(&self) -> Result<usize, ProfilerError> {
        // This is a simplified implementation - in production, you'd use
        // system monitoring libraries like `sysinfo` or `procfs`
        
        // Calculate approximate memory usage based on data structures
        let validators_memory = self.validators.len() * std::mem::size_of::<ValidatorMetrics>();
        let circuit_breakers_memory = self.circuit_breakers.len() * std::mem::size_of::<CircuitBreaker>();
        let leader_schedule_memory = self.leader_schedule.read()
            .map_err(|e| ProfilerError::ConfigError(format!("Failed to read leader schedule: {}", e)))?
            .len() * std::mem::size_of::<(u64, Pubkey)>();
        
        let total_bytes = validators_memory + circuit_breakers_memory + leader_schedule_memory;
        let total_mb = total_bytes / (1024 * 1024);
        
        Ok(total_mb.max(1)) // Ensure at least 1MB is reported
    }
    
    /// Trigger memory cleanup and optimization
    async fn trigger_memory_cleanup(&self) -> Result<(), ProfilerError> {
        log::warn!("Triggering memory cleanup due to high usage");
        
        // Clean up old latency measurements beyond retention window
        let retention_slots = self.config.metrics_retention_hours * 3600 / 400; // Approximate slots per hour
        
        for validator_entry in self.validators.iter() {
            let mut metrics_guard = validator_entry.value().write()
                .map_err(|e| ProfilerError::ConfigError(format!("Failed to write validator metrics: {}", e)))?;
            
            // Keep only recent measurements
            if metrics_guard.slot_latencies.len() > retention_slots as usize {
                let keep_count = retention_slots as usize;
                let remove_count = metrics_guard.slot_latencies.len() - keep_count;
                metrics_guard.slot_latencies.drain(0..remove_count);
            }
        }
        
        // Clean up old performance metrics
        let mut perf_metrics = self.performance_metrics.write()
            .map_err(|e| ProfilerError::ConfigError(format!("Failed to write performance metrics: {}", e)))?;
        
        // Reset counters if they get too large
        if perf_metrics.total_latency_measurements > 1_000_000 {
            perf_metrics.total_latency_measurements = perf_metrics.total_latency_measurements / 2;
            perf_metrics.mev_opportunities_detected = perf_metrics.mev_opportunities_detected / 2;
        }
        
        log::info!("Memory cleanup completed");
        Ok(())
    }
    pub async fn new(validator_configs: Vec<(Pubkey, String, String)>) -> Result<Arc<Self>, ProfilerError> {
        let config = ProfilerConfig::default();
        Self::new_with_config(validator_configs, config).await
    }
    
    pub async fn new_with_config(
        validator_configs: Vec<(Pubkey, String, String)>, 
        config: ProfilerConfig
    ) -> Result<Arc<Self>, ProfilerError> {
        let validators = Arc::new(DashMap::new());
        let rpc_clients = Arc::new(DashMap::new());
        let tpu_clients = Arc::new(DashMap::new());
        let circuit_breakers = DashMap::new();
        let (alert_sender, _) = broadcast::channel(1000);
        
        // Generate test keypair for TPU testing
        let test_keypair = Arc::new(Keypair::new());
        
        // Initialize HTTP client for Jito API calls
        let http_client = HttpClient::builder()
            .timeout(Duration::from_millis(config.probe_timeout_ms))
            .tcp_keepalive(Duration::from_secs(30))
            .build()
            .map_err(|e| ProfilerError::ConfigError(format!("Failed to create HTTP client: {}", e)))?;
        
        for (pubkey, rpc_endpoint, tpu_endpoint) in validator_configs {
            let client = Arc::new(RpcClient::new_with_timeout_and_commitment(
                rpc_endpoint.clone(),
                Duration::from_millis(config.probe_timeout_ms),
                CommitmentConfig::processed(),
            ));
            
            rpc_clients.insert(pubkey, client);
            
            // Create real TPU client
            let tpu_config = TpuClientConfig::default();
            let tpu_client = TpuClient::new(
                Arc::new(RpcClient::new(rpc_endpoint.clone())),
                &tpu_endpoint,
                tpu_config,
            ).map_err(|e| ProfilerError::TpuConnectionError(format!("Failed to create TPU client: {}", e)))?;
            
            tpu_clients.insert(pubkey, Arc::new(tpu_client));
            
            let circuit_breaker = CircuitBreaker::new(
                config.circuit_breaker_failure_threshold,
                Duration::from_secs(config.circuit_breaker_recovery_timeout_secs),
            );
            circuit_breakers.insert(pubkey, circuit_breaker);
            
            let current_time = Self::current_timestamp_ms()
                .map_err(|e| ProfilerError::ConfigError(format!("Failed to get timestamp: {}", e)))?;
            
            let metrics = ValidatorMetrics {
                identity: pubkey,
                rpc_endpoint: rpc_endpoint.clone(),
                tpu_endpoint: tpu_endpoint.clone(),
                last_update_ms: current_time,
                slot_latencies: VecDeque::with_capacity(config.latency_window_size),
                transaction_latencies: VecDeque::with_capacity(config.latency_window_size),
                tpu_latencies: VecDeque::with_capacity(config.latency_window_size),
                health_score: 80.0,
                success_rate: 100.0,
                avg_latency_ms: 0.0,
                median_latency_ms: 0.0,
                p75_latency_ms: 0.0,
                p90_latency_ms: 0.0,
                p95_latency_ms: 0.0,
                p99_latency_ms: 0.0,
                p999_latency_ms: 0.0,
                jitter_ms: 0.0,
                total_probes: 0,
                successful_probes: 0,
                consecutive_failures: 0,
                is_active: true,
                stake_weight: 0,
                commission: 0,
                version: None,
                last_slot: 0,
                slots_behind: 0,
                is_leader_next_slots: Vec::with_capacity(32), // Typical slots per epoch a validator leads
                jito_enabled: false, // Will be detected during health checks
                mev_rewards_earned: 0,
                bundle_success_rate: 0.0,
                tpu_connection_quality: 0.0,
                geographic_region: None,
                last_leader_slot: None,
                skip_rate: 0.0,
                vote_latency_ms: 0.0,
            };
            
            // Store TPU client for later insertion
            // circuit_breakers and tpu_clients will be created properly below
            
            validators.insert(pubkey, Arc::new(RwLock::new(metrics)));
        }

        let performance_metrics = Arc::new(PerformanceMetrics {
            total_latency_measurements: AtomicU64::new(0),
            avg_network_latency_ms: AtomicU64::new(0),
            p99_latency_samples: Arc::new(Mutex::new(VecDeque::new())),
            validator_ranking_cache: Arc::new(RwLock::new(Vec::new())),
            mev_opportunities_detected: AtomicU64::new(0),
            bundle_submission_success_rate: AtomicU64::new(10000), // 100% * 100
            geographic_latency_map: Arc::new(RwLock::new(HashMap::new())),
        });

        let profiler = Arc::new(Self {
            validators,
            rpc_clients: Arc::new(rpc_clients),
            tpu_clients: Arc::new(tpu_clients),
            probe_semaphore: Arc::new(Semaphore::new(config.max_concurrent_probes)),
            active_probes: Arc::new(AtomicU64::new(0)),
            total_probes: Arc::new(AtomicU64::new(0)),
            successful_probes: Arc::new(AtomicU64::new(0)),
            running: Arc::new(AtomicBool::new(false)),
            global_best_slot: Arc::new(AtomicU64::new(0)),
            config,
            circuit_breakers,
            http_client,
            stake_weights: Arc::new(RwLock::new(HashMap::new())),
            latency_cache: Arc::new(Mutex::new(BTreeMap::new())),
            leader_schedule: Arc::new(RwLock::new(BTreeMap::new())),
            jito_tip_accounts: Arc::new(RwLock::new(Vec::new())),
            current_tip_floor: Arc::new(RwLock::new(None)),
            test_keypair,
            memory_monitor: Arc::new(AtomicU64::new(0)),
            performance_metrics,
            alert_sender,
            epoch_info: Arc::new(RwLock::new(None)),
        });

        // Initialize Jito tip accounts
        profiler.fetch_jito_tip_accounts().await?;

        Ok(profiler)
    }

    // Real TPU Latency Probing Implementation
    async fn probe_tpu_latency(&self, validator: Pubkey) -> Result<(), ProfilerError> {
        let tpu_client = self.tpu_clients.get(&validator)
            .ok_or_else(|| ProfilerError::ValidatorNotFound(validator))?;
        
        let circuit_breaker = self.circuit_breakers.get(&validator)
            .ok_or_else(|| ProfilerError::ValidatorNotFound(validator))?;
        
        let result = circuit_breaker.call(|| {
            self.perform_tpu_test(validator, &tpu_client)
        }).await;
        
        match result {
            Ok(tpu_latency) => {
                if let Some(metrics_lock) = self.validators.get(&validator) {
                    if let Ok(mut metrics) = metrics_lock.write().await {
                        metrics.tpu_latencies.push_back(tpu_latency);
                        if metrics.tpu_latencies.len() > self.config.latency_window_size {
                            metrics.tpu_latencies.pop_front();
                        }
                        
                        // Update TPU connection quality score
                        self.update_tpu_connection_quality(&mut metrics);
                    }
                }
            }
            Err(CircuitBreakerError::Open) => {
                let _ = self.alert_sender.send(AlertEvent::CircuitBreakerTripped { validator });
                return Err(ProfilerError::CircuitBreakerOpen(validator));
            }
            Err(e) => {
                return Err(ProfilerError::TpuConnectionError(e.to_string()));
            }
        }
        
        Ok(())
    }
    
    async fn perform_tpu_test(&self, validator: Pubkey, tpu_client: &Arc<TpuClient>) -> Result<TpuLatency, CircuitBreakerError> {
        let start_time = Instant::now();
        
        // Create a minimal test transaction
        let test_instruction = system_instruction::transfer(
            &self.test_keypair.pubkey(),
            &self.test_keypair.pubkey(), // Self-transfer
            TEST_TRANSACTION_AMOUNT,
        );
        
        let mut test_transaction = Transaction::new_with_payer(
            &[test_instruction],
            Some(&self.test_keypair.pubkey()),
        );
        
        // Get recent blockhash (in real implementation, cache this)
        let connection_start = Instant::now();
        
        match timeout(
            Duration::from_millis(self.config.probe_timeout_ms),
            tpu_client.send_transaction(&test_transaction)
        ).await {
            Ok(Ok(_)) => {
                let total_latency = start_time.elapsed().as_secs_f64() * 1000.0;
                let connect_latency = connection_start.elapsed().as_secs_f64() * 1000.0;
                
                Ok(TpuLatency {
                    timestamp_ms: Self::current_timestamp_ms().unwrap_or(0),
                    connect_latency_ms: connect_latency,
                    send_latency_ms: total_latency - connect_latency,
                    ack_latency_ms: None, // TPU doesn't provide ack timing
                    connection_success: true,
                })
            }
            Ok(Err(e)) => {
                Err(CircuitBreakerError::OperationFailed(format!("TPU send failed: {}", e)))
            }
            Err(_) => {
                Err(CircuitBreakerError::OperationFailed("TPU operation timeout".to_string()))
            }
        }
    }
    
    fn update_tpu_connection_quality(&self, metrics: &mut ValidatorMetrics) {
        if metrics.tpu_latencies.is_empty() {
            metrics.tpu_connection_quality = 0.0;
            return;
        }
        
        let recent_latencies: Vec<f64> = metrics.tpu_latencies
            .iter()
            .rev()
            .take(100) // Last 100 measurements
            .map(|tpu| tpu.connect_latency_ms + tpu.send_latency_ms)
            .collect();
        
        let success_rate = metrics.tpu_latencies
            .iter()
            .rev()
            .take(100)
            .map(|tpu| if tpu.connection_success { 1.0 } else { 0.0 })
            .sum::<f64>() / recent_latencies.len() as f64;
        
        let avg_latency = recent_latencies.iter().sum::<f64>() / recent_latencies.len() as f64;
        
        // Quality score: 40% success rate, 60% inverse latency score
        let latency_score = (1.0 - (avg_latency / 100.0).min(1.0)) * 100.0;
        metrics.tpu_connection_quality = (success_rate * 40.0) + (latency_score * 0.6);
    }
    
    // Real Jito API Integration
    async fn fetch_jito_tip_accounts(&self) -> Result<(), ProfilerError> {
        let url = format!("{}/api/v1/tip-accounts", self.config.jito_api_url);
        
        match timeout(
            Duration::from_secs(10),
            self.http_client.get(&url).send()
        ).await {
            Ok(Ok(response)) => {
                if response.status().is_success() {
                    match response.json::<JitoTipAccountsResponse>().await {
                        Ok(jito_response) => {
                            let tip_accounts: Result<Vec<Pubkey>, _> = jito_response.tip_accounts
                                .into_iter()
                                .map(|addr| addr.parse())
                                .collect();
                            
                            match tip_accounts {
                                Ok(accounts) => {
                                    *self.jito_tip_accounts.write().await = accounts.clone();
                                    let _ = self.alert_sender.send(AlertEvent::JitoTipAccountsUpdated { 
                                        count: accounts.len() 
                                    });
                                    info!("Updated Jito tip accounts: {} accounts", accounts.len());
                                }
                                Err(e) => {
                                    return Err(ProfilerError::JitoApiError(
                                        format!("Invalid tip account format: {}", e)
                                    ));
                                }
                            }
                        }
                        Err(e) => {
                            return Err(ProfilerError::JitoApiError(
                                format!("Failed to parse Jito response: {}", e)
                            ));
                        }
                    }
                } else {
                    return Err(ProfilerError::JitoApiError(
                        format!("Jito API returned status: {}", response.status())
                    ));
                }
            }
            Ok(Err(e)) => {
                return Err(ProfilerError::JitoApiError(
                    format!("Jito API request failed: {}", e)
                ));
            }
            Err(_) => {
                return Err(ProfilerError::TimeoutError {
                    operation: "Jito tip accounts fetch".to_string(),
                    timeout_ms: 10000,
                });
            }
        }
        
        Ok(())
    }
    
    async fn fetch_jito_tip_floor(&self) -> Result<(), ProfilerError> {
        let url = format!("{}/api/v1/bundles/tip_floor", self.config.jito_bundle_api_url);
        
        match timeout(
            Duration::from_secs(5),
            self.http_client.get(&url).send()
        ).await {
            Ok(Ok(response)) => {
                if response.status().is_success() {
                    match response.json::<Vec<JitoTipFloorResponse>>().await {
                        Ok(mut tip_floors) => {
                            if let Some(latest_floor) = tip_floors.pop() {
                                *self.current_tip_floor.write().await = Some(latest_floor);
                                debug!("Updated Jito tip floor data");
                            }
                        }
                        Err(e) => {
                            warn!("Failed to parse Jito tip floor: {}", e);
                        }
                    }
                }
            }
            _ => {
                debug!("Failed to fetch Jito tip floor data");
            }
        }
        
        Ok(())
    }
    
    // Enhanced MEV Opportunity Detection
    pub async fn detect_mev_opportunities(&self) -> Result<Vec<MevOpportunity>, ProfilerError> {
        let mut opportunities = Vec::new();
        let current_slot = self.global_best_slot.load(Ordering::Acquire);
        
        // Get current tip floor for profitability analysis
        let tip_floor = {
            let floor_guard = self.current_tip_floor.read().await;
            floor_guard.as_ref().map(|f| f.ema_landed_tips_50th_percentile).unwrap_or(0.000_01) // Default 0.01 SOL
        };
        
        for entry in self.validators.iter() {
            let validator = *entry.key();
            if let Ok(metrics) = entry.value().read().await {
                // Only consider healthy, active validators
                if !metrics.is_active || metrics.health_score < 80.0 {
                    continue;
                }
                
                // Check if validator has upcoming leader slots (MEV opportunity)
                if !metrics.is_leader_next_slots.is_empty() {
                    let next_leader_slot = metrics.is_leader_next_slots[0];
                    if next_leader_slot <= current_slot + 4 { // Within next 4 slots
                        let estimated_profit = self.estimate_mev_profit(&metrics, tip_floor).await?;
                        
                        if estimated_profit > tip_floor * 1_000_000_000.0 { // Convert SOL to lamports
                            opportunities.push(MevOpportunity {
                                validator,
                                opportunity_type: MevOpportunityType::JitoBundle {
                                    bundle_id: format!("predicted_{}", next_leader_slot),
                                    tip_amount: (tip_floor * 1_500_000_000.0) as u64, // 1.5x tip floor
                                },
                                estimated_profit_lamports: estimated_profit as u64,
                                confidence_score: self.calculate_opportunity_confidence(&metrics),
                                expiry_slot: next_leader_slot,
                                required_stake: metrics.stake_weight,
                                gas_cost_estimate: 5000, // Base transaction cost
                            });
                        }
                    }
                }
            }
        }
        
        self.performance_metrics.mev_opportunities_detected
            .store(opportunities.len() as u64, Ordering::Release);
        
        Ok(opportunities)
    }
    
    async fn estimate_mev_profit(&self, metrics: &ValidatorMetrics, base_tip: f64) -> Result<f64, ProfilerError> {
        // Simplified MEV profit estimation based on validator performance
        let latency_advantage = (self.config.critical_latency_ms - metrics.avg_latency_ms).max(0.0);
        let stake_factor = (metrics.stake_weight as f64 / 1_000_000_000_000.0).min(1.0); // Normalize stake
        let health_factor = metrics.health_score / 100.0;
        
        // Estimate based on latency advantage, stake weight, and health
        let estimated_profit_sol = base_tip * (1.0 + latency_advantage / 10.0) * stake_factor * health_factor;
        Ok(estimated_profit_sol * 1_000_000_000.0) // Convert to lamports
    }
    
    fn calculate_opportunity_confidence(&self, metrics: &ValidatorMetrics) -> f64 {
        let latency_confidence = if metrics.avg_latency_ms < self.config.optimal_latency_ms {
            1.0
        } else {
            (self.config.critical_latency_ms - metrics.avg_latency_ms) / self.config.critical_latency_ms
        }.max(0.0);
        
        let consistency_confidence = (100.0 - metrics.jitter_ms) / 100.0;
        let reliability_confidence = metrics.success_rate / 100.0;
        
        (latency_confidence * 0.4 + consistency_confidence * 0.3 + reliability_confidence * 0.3).min(1.0)
    }

    // Enhanced Leader Schedule Management
    async fn update_leader_schedule(&self) -> Result<(), ProfilerError> {
        // Get one RPC client to fetch leader schedule
        if let Some(client_entry) = self.rpc_clients.iter().next() {
            let client = client_entry.value();
            
            match timeout(
                Duration::from_secs(30),
                client.get_leader_schedule(None)
            ).await {
                Ok(Ok(Some(schedule))) => {
                    let mut leader_map = BTreeMap::new();
                    let current_slot = self.global_best_slot.load(Ordering::Acquire);
                    
                    // Process leader schedule and update validator metrics
                    for (validator_str, slots) in schedule {
                        if let Ok(validator_pubkey) = validator_str.parse::<Pubkey>() {
                            for &slot in slots.iter() {
                                if slot >= current_slot {
                                    leader_map.insert(slot, validator_pubkey);
                                    
                                    // Update validator's upcoming leader slots
                                    if let Some(metrics_lock) = self.validators.get(&validator_pubkey) {
                                        if let Ok(mut metrics) = metrics_lock.write().await {
                                            if !metrics.is_leader_next_slots.contains(&slot) {
                                                metrics.is_leader_next_slots.push(slot);
                                                metrics.is_leader_next_slots.sort();
                                                // Keep only next 32 slots
                                                if metrics.is_leader_next_slots.len() > 32 {
                                                    metrics.is_leader_next_slots.truncate(32);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    *self.leader_schedule.write().await = leader_map;
                    
                    if let Ok(epoch_info) = client.get_epoch_info().await {
                        *self.epoch_info.write().await = Some(epoch_info);
                        let _ = self.alert_sender.send(AlertEvent::LeaderScheduleUpdated {
                            epoch: epoch_info.epoch,
                            validator_count: schedule.len(),
                        });
                    }
                    
                    info!("Updated leader schedule for {} validators", schedule.len());
                }
                Ok(Ok(None)) => {
                    warn!("No leader schedule available");
                }
                Ok(Err(e)) => {
                    return Err(ProfilerError::LeaderScheduleError(format!("RPC error: {}", e)));
                }
                Err(_) => {
                    return Err(ProfilerError::TimeoutError {
                        operation: "Leader schedule fetch".to_string(),
                        timeout_ms: 30000,
                    });
                }
            }
        } else {
            return Err(ProfilerError::ConfigError("No RPC clients available".to_string()));
        }
        
        Ok(())
    }
    
    pub async fn get_upcoming_leader_slots(&self, validator: &Pubkey, slots_ahead: usize) -> Vec<Slot> {
        let current_slot = self.global_best_slot.load(Ordering::Acquire);
        let leader_schedule = self.leader_schedule.read().await;
        
        leader_schedule
            .range(current_slot..current_slot + slots_ahead as u64)
            .filter_map(|(slot, leader_validator)| {
                if leader_validator == validator {
                    Some(*slot)
                } else {
                    None
                }
            })
            .collect()
    }

    // Enhanced Monitoring Methods Implementation
    async fn run_tpu_monitor(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(Duration::from_millis(self.config.tpu_probe_interval_ms));
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            let validators: Vec<Pubkey> = self.validators.iter()
                .map(|entry| *entry.key())
                .collect();

            for validator in validators {
                let profiler = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = profiler.probe_tpu_latency(validator).await {
                        debug!("TPU probe failed for {}: {}", validator, e);
                    }
                });
            }
        }
        
        Ok(())
    }
    
    async fn run_leader_schedule_updater(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(Duration::from_secs(self.config.leader_schedule_update_interval_secs));
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            if let Err(e) = self.update_leader_schedule().await {
                warn!("Failed to update leader schedule: {}", e);
            }
        }
        
        Ok(())
    }
    
    async fn run_jito_tip_account_updater(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(Duration::from_secs(self.config.jito_tip_account_update_interval_secs));
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            if let Err(e) = self.fetch_jito_tip_accounts().await {
                warn!("Failed to update Jito tip accounts: {}", e);
            }
            
            if let Err(e) = self.fetch_jito_tip_floor().await {
                debug!("Failed to update Jito tip floor: {}", e);
            }
        }
        
        Ok(())
    }
    
    async fn run_mev_opportunity_detector(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(Duration::from_millis(100)); // High frequency MEV detection
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            match self.detect_mev_opportunities().await {
                Ok(opportunities) => {
                    for opportunity in opportunities {
                        let _ = self.alert_sender.send(AlertEvent::MevOpportunity {
                            validator: opportunity.validator,
                            estimated_profit: opportunity.estimated_profit_lamports as f64 / 1_000_000_000.0,
                        });
                    }
                }
                Err(e) => {
                    debug!("MEV opportunity detection failed: {}", e);
                }
            }
        }
        
        Ok(())
    }
    
    async fn run_circuit_breaker_monitor(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(Duration::from_secs(10));
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            for entry in self.circuit_breakers.iter() {
                let validator = *entry.key();
                let circuit_breaker = entry.value();
                
                if !circuit_breaker.is_closed().await {
                    debug!("Circuit breaker open for validator: {}", validator);
                    
                    // Try to reset if enough time has passed
                    if let Some(metrics_lock) = self.validators.get(&validator) {
                        if let Ok(mut metrics) = metrics_lock.write().await {
                            if metrics.consecutive_failures == 0 {
                                circuit_breaker.reset().await;
                                info!("Circuit breaker reset for validator: {}", validator);
                            }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }

    // Enhanced Alert Management
    pub async fn run_alert_manager(self: Arc<Self>) -> Result<()> {
        let mut alert_receiver = self.alert_sender.subscribe();
        
        while self.running.load(Ordering::Acquire) {
            match alert_receiver.recv().await {
                Ok(alert) => {
                    self.process_alert(alert).await;
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!("Alert receiver lagged, skipped {} alerts", skipped);
                }
                Err(_) => break,
            }
        }
        Ok(())
    }

    async fn process_alert(&self, alert: AlertEvent) {
        match alert {
            AlertEvent::ValidatorDown { validator, reason } => {
                warn!("🔴 Validator {} is down: {}", validator, reason);
                // In production: send to Slack/Discord/Telegram webhook
            }
            AlertEvent::LatencySpike { validator, latency_ms, threshold_ms } => {
                warn!("⚡ Latency spike on {}: {}ms > {}ms", validator, latency_ms, threshold_ms);
            }
            AlertEvent::MevOpportunity { validator, estimated_profit } => {
                info!("💰 MEV opportunity on {}: {:.6} SOL profit", validator, estimated_profit);
                // In production: trigger MEV bot strategy
            }
            AlertEvent::CircuitBreakerTripped { validator } => {
                warn!("🔧 Circuit breaker tripped for {}", validator);
            }
            AlertEvent::MemoryUsageHigh { current_mb, limit_mb } => {
                warn!("💾 High memory usage: {}MB / {}MB", current_mb, limit_mb);
            }
            AlertEvent::JitoTipAccountsUpdated { count } => {
                info!("🎯 Jito tip accounts updated: {} accounts", count);
            }
            AlertEvent::LeaderScheduleUpdated { epoch, validator_count } => {
                info!("📋 Leader schedule updated for epoch {}: {} validators", epoch, validator_count);
            }
        }
    }

    pub async fn start(self: Arc<Self>) -> Result<()> {
        info!("Starting ValidatorLatencyProfiler with production MEV features");
        self.running.store(true, Ordering::Release);
        
        let handles = vec![
            tokio::spawn(self.clone().run_latency_monitor()),
            tokio::spawn(self.clone().run_health_checker()),
            tokio::spawn(self.clone().run_stats_calculator()),
            tokio::spawn(self.clone().run_stake_updater()),
            tokio::spawn(self.clone().run_cache_cleaner()),
            tokio::spawn(self.clone().run_tpu_monitor()),
            tokio::spawn(self.clone().run_leader_schedule_updater()),
            tokio::spawn(self.clone().run_jito_tip_account_updater()),
            tokio::spawn(self.clone().run_memory_monitor()),
            tokio::spawn(self.clone().run_mev_opportunity_detector()),
            tokio::spawn(self.clone().run_circuit_breaker_monitor()),
            tokio::spawn(self.clone().run_alert_manager()),
        ];

        // Wait for all tasks to complete or first error
        for handle in handles {
            if let Err(e) = handle.await {
                error!("ValidatorLatencyProfiler task failed: {}", e);
                return Err(anyhow!("Task execution failed: {}", e));
            }
        }
        
        info!("All ValidatorLatencyProfiler tasks completed successfully");
        Ok(())
    }

    async fn run_latency_monitor(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(Duration::from_millis(self.config.latency_probe_interval_ms));
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            let validators: Vec<Pubkey> = self.validators.iter()
                .filter_map(|entry| {
                    let validator = *entry.key();
                    match entry.value().try_read() {
                        Ok(metrics) if metrics.is_active => Some(validator),
                        _ => None,
                    }
                })
                .collect();

            let mut probe_tasks = Vec::new();
            
            for validator in validators {
                let profiler = self.clone();
                let permit = self.probe_semaphore.clone().acquire_owned().await?;
                
                let task = tokio::spawn(async move {
                    let _permit = permit;
                    profiler.probe_validator_latency(validator).await
                });
                
                probe_tasks.push(task);
            }
            
            futures::future::join_all(probe_tasks).await;
        }
        
        Ok(())
    }

    async fn probe_validator_latency(&self, validator: Pubkey) -> Result<()> {
        self.active_probes.fetch_add(1, Ordering::AcqRel);
        // Track active probes manually since defer! isn't standard Rust
        let _probe_guard = ProbeGuard::new(&self.active_probes);

        let start = Instant::now();
        let client = self.rpc_clients.get(&validator)
            .context("RPC client not found")?;

        match timeout(Duration::from_millis(self.config.probe_timeout_ms), client.get_slot()).await {
            Ok(Ok(slot)) => {
                let latency_ms = start.elapsed().as_secs_f64() * 1000.0;
                self.record_successful_probe(validator, slot, latency_ms).await?;
            }
            Ok(Err(e)) => {
                self.record_failed_probe(validator, format!("RPC error: {}", e)).await?;
            }
            Err(_) => {
                self.record_failed_probe(validator, "Timeout".to_string()).await?;
            }
        }

        Ok(())
    }

    async fn record_successful_probe(&self, validator: Pubkey, slot: Slot, latency_ms: f64) -> Result<()> {
        let metrics_lock = self.validators.get(&validator)
            .context("Validator metrics not found")?;
        
        let mut metrics = metrics_lock.write().await;
        let timestamp_ms = Self::current_timestamp_ms()?;
        
        // Check if this is a leader slot for MEV optimization
        let is_leader_slot = {
            let leader_schedule = self.leader_schedule.read().await;
            leader_schedule.get(&slot).map_or(false, |leader| *leader == validator)
        };
        
        metrics.slot_latencies.push_back(SlotLatency {
            slot,
            latency_ms,
            timestamp_ms,
            is_leader_slot,
            block_height: None, // Will be populated by separate block height fetcher
            transaction_count: None, // Will be populated by block analysis
            priority_fees_included: false, // Will be detected during block analysis
        });
        
        while metrics.slot_latencies.len() > LATENCY_WINDOW_SIZE {
            metrics.slot_latencies.pop_front();
        }
        
        metrics.last_update_ms = timestamp_ms;
        metrics.total_probes += 1;
        metrics.successful_probes += 1;
        metrics.consecutive_failures = 0;
        metrics.last_slot = slot;
        
        let best_slot = self.global_best_slot.load(Ordering::Acquire);
        if slot > best_slot {
            self.global_best_slot.store(slot, Ordering::Release);
            metrics.slots_behind = 0;
        } else {
            metrics.slots_behind = best_slot.saturating_sub(slot);
        }
        
        self.total_probes.fetch_add(1, Ordering::AcqRel);
        self.successful_probes.fetch_add(1, Ordering::AcqRel);
        
        let mut cache = self.latency_cache.lock().await;
        cache.insert((validator, timestamp_ms / 1000), latency_ms);
        
        Ok(())
    }

    async fn record_failed_probe(&self, validator: Pubkey, _error: String) -> Result<()> {
        let metrics_lock = self.validators.get(&validator)
            .context("Validator metrics not found")?;
        
        let mut metrics = metrics_lock.write().await;
        metrics.total_probes += 1;
        metrics.consecutive_failures += 1;
        
        if metrics.consecutive_failures >= MAX_CONSECUTIVE_FAILURES {
            metrics.is_active = false;
        }
        
        self.total_probes.fetch_add(1, Ordering::AcqRel);
        
        Ok(())
    }

    async fn run_health_checker(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(VALIDATOR_HEALTH_CHECK_INTERVAL);
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            let validators: Vec<Pubkey> = self.validators.iter()
                .map(|entry| *entry.key())
                .collect();

            for validator in validators {
                let profiler = self.clone();
                tokio::spawn(async move {
                    let _ = profiler.check_validator_health(validator).await;
                });
            }
        }
        
        Ok(())
    }

    async fn check_validator_health(&self, validator: Pubkey) -> Result<()> {
        let client = self.rpc_clients.get(&validator)
            .context("RPC client not found")?;

        let version_result = timeout(Duration::from_secs(5), client.get_version()).await;
        let slot_result = timeout(Duration::from_secs(5), client.get_slot()).await;
        
        let metrics_lock = self.validators.get(&validator)
            .context("Validator metrics not found")?;
        
        let mut metrics = metrics_lock.write().await;
        
        match (version_result, slot_result) {
            (Ok(Ok(version)), Ok(Ok(_))) => {
                metrics.version = Some(version.solana_core);
                if !metrics.is_active {
                    metrics.is_active = true;
                    metrics.consecutive_failures = 0;
                    metrics.health_score = 60.0;
                }
            }
            _ => {
                metrics.consecutive_failures += 1;
                if metrics.consecutive_failures >= MAX_CONSECUTIVE_FAILURES * 2 {
                    metrics.is_active = false;
                }
            }
        }
        
        Ok(())
    }

    async fn run_stats_calculator(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(Duration::from_secs(1));
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            for entry in self.validators.iter() {
                let validator = *entry.key();
                let profiler = self.clone();
                
                tokio::spawn(async move {
                    let _ = profiler.update_validator_stats(validator).await;
                });
            }
        }
        
        Ok(())
    }

    async fn update_validator_stats(&self, validator: Pubkey) -> Result<()> {
        let metrics_lock = self.validators.get(&validator)
            .context("Validator metrics not found")?;
        
                let mut metrics = metrics_lock.write().await;
        
        if metrics.slot_latencies.len() < MIN_SAMPLES_FOR_STATS {
            return Ok(());
        }

        let mut latencies: Vec<f64> = metrics.slot_latencies.iter()
            .map(|s| s.latency_ms)
            .collect();

        // Calculate basic statistics
        let mean = self.calculate_mean(&latencies);
        let std_dev = self.calculate_std_dev(&latencies, mean);
        
        // Filter outliers
        latencies.retain(|&l| (l - mean).abs() <= OUTLIER_THRESHOLD_MULTIPLIER * std_dev);
        
        if latencies.is_empty() {
            return Ok(());
        }

        // Sort for percentile calculations with proper NaN handling
        latencies.sort_by(|a, b| {
            match (a.is_nan(), b.is_nan()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                (false, false) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
            }
        });
        
        // Remove any NaN values after sorting
        latencies.retain(|&x| !x.is_nan());
        
        metrics.avg_latency_ms = self.calculate_mean(&latencies);
        metrics.median_latency_ms = self.calculate_percentile(&latencies, 0.50);
        metrics.p75_latency_ms = self.calculate_percentile(&latencies, 0.75);
        metrics.p90_latency_ms = self.calculate_percentile(&latencies, 0.90);
        metrics.p95_latency_ms = self.calculate_percentile(&latencies, 0.95);
        metrics.p99_latency_ms = self.calculate_percentile(&latencies, 0.99);
        metrics.p999_latency_ms = self.calculate_percentile(&latencies, 0.999);
        
        // Calculate jitter (variation in latency)
        metrics.jitter_ms = self.calculate_jitter(&latencies);
        
        // Update success rate
        metrics.success_rate = if metrics.total_probes > 0 {
            (metrics.successful_probes as f64 / metrics.total_probes as f64) * 100.0
        } else {
            0.0
        };
        
        // Calculate health score
        metrics.health_score = self.calculate_health_score(&metrics);
        
        Ok(())
    }

    fn calculate_mean(&self, values: &[f64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        values.iter().sum::<f64>() / values.len() as f64
    }

    fn calculate_std_dev(&self, values: &[f64], mean: f64) -> f64 {
        if values.len() < 2 {
            return 0.0;
        }
        let variance = values.iter()
            .map(|&x| (x - mean).powi(2))
            .sum::<f64>() / (values.len() - 1) as f64;
        variance.sqrt()
    }

    fn calculate_percentile(&self, sorted_values: &[f64], percentile: f64) -> f64 {
        if sorted_values.is_empty() {
            return 0.0;
        }
        
        if sorted_values.len() == 1 {
            return sorted_values[0];
        }
        
        // Use proper percentile calculation with linear interpolation
        let rank = percentile * (sorted_values.len() - 1) as f64;
        let lower_index = rank.floor() as usize;
        let upper_index = rank.ceil() as usize;
        
        if lower_index == upper_index {
            sorted_values[lower_index]
        } else {
            let weight = rank - lower_index as f64;
            sorted_values[lower_index] * (1.0 - weight) + sorted_values[upper_index] * weight
        }
    }

    fn calculate_jitter(&self, latencies: &[f64]) -> f64 {
        if latencies.len() < 2 {
            return 0.0;
        }
        
        let mut differences = Vec::new();
        for i in 1..latencies.len() {
            differences.push((latencies[i] - latencies[i-1]).abs());
        }
        
        self.calculate_mean(&differences)
    }

    fn calculate_health_score(&self, metrics: &ValidatorMetrics) -> f64 {
        let mut score = 100.0;
        
        // Latency penalty (40% weight)
        if metrics.avg_latency_ms > OPTIMAL_LATENCY_MS {
            let latency_penalty = ((metrics.avg_latency_ms - OPTIMAL_LATENCY_MS) / CRITICAL_LATENCY_MS) * 40.0;
            score -= latency_penalty.min(40.0);
        }
        
        // Success rate penalty (30% weight)
        if metrics.success_rate < 99.0 {
            score -= (99.0 - metrics.success_rate) * 0.3;
        }
        
        // Jitter penalty (15% weight)
        if metrics.jitter_ms > 10.0 {
            let jitter_penalty = (metrics.jitter_ms / 50.0) * 15.0;
            score -= jitter_penalty.min(15.0);
        }
        
        // Slots behind penalty (15% weight)
        if metrics.slots_behind > 2 {
            let slot_penalty = (metrics.slots_behind as f64 / 10.0) * 15.0;
            score -= slot_penalty.min(15.0);
        }
        
        // Stake weight bonus (up to 10 points)
        if metrics.stake_weight > 0 {
            let stake_bonus = (metrics.stake_weight as f64 / 1_000_000_000_000.0).min(1.0) * 10.0;
            score += stake_bonus;
        }
        
        score.max(0.0).min(110.0)
    }

    async fn run_stake_updater(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(STAKE_UPDATE_INTERVAL);
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            if let Some(client) = self.rpc_clients.iter().next() {
                match timeout(Duration::from_secs(30), client.value().get_vote_accounts()).await {
                    Ok(Ok(vote_accounts)) => {
                        let mut stake_map = HashMap::new();
                        
                        for account in vote_accounts.current.iter().chain(vote_accounts.delinquent.iter()) {
                            if let Ok(validator_identity) = account.node_pubkey.parse::<Pubkey>() {
                                stake_map.insert(validator_identity, account.activated_stake);
                                
                                if let Some(metrics_lock) = self.validators.get(&validator_identity) {
                                    if let Ok(mut metrics) = metrics_lock.write().await {
                                        metrics.stake_weight = account.activated_stake;
                                        metrics.commission = account.commission;
                                    }
                                }
                            }
                        }
                        
                        *self.stake_weights.write().await = stake_map;
                    }
                    _ => continue,
                }
                break;
            }
        }
        
        Ok(())
    }

    async fn run_cache_cleaner(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(Duration::from_secs(60));
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            let mut cache = self.latency_cache.lock().await;
            let current_time = Self::current_timestamp_ms() / 1000;
            let cutoff_time = current_time.saturating_sub(300); // Keep last 5 minutes
            
            cache.retain(|&(_, timestamp), _| timestamp > cutoff_time);
        }
        
        Ok(())
    }

    pub async fn get_optimal_validators(&self, count: usize, max_latency_ms: Option<f64>) -> Vec<(Pubkey, f64, ValidatorMetrics)> {
        let mut candidates = Vec::new();
        let max_latency = max_latency_ms.unwrap_or(CRITICAL_LATENCY_MS);
        
        for entry in self.validators.iter() {
            if let Ok(metrics) = entry.value().read().await {
                if metrics.is_active && 
                   metrics.health_score > 70.0 &&
                   metrics.avg_latency_ms < max_latency &&
                   metrics.success_rate > 95.0 &&
                   metrics.slot_latencies.len() >= MIN_SAMPLES_FOR_STATS {
                    
                    let composite_score = self.calculate_composite_score(&metrics);
                    candidates.push((*entry.key(), composite_score, metrics.clone()));
                }
            }
        }
        
        candidates.sort_by(|a, b| {
            match (a.1.is_nan(), b.1.is_nan()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                (false, false) => b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal),
            }
        });
        candidates.truncate(count);
        candidates
    }

    fn calculate_composite_score(&self, metrics: &ValidatorMetrics) -> f64 {
        // Weighted scoring for MEV optimization
        let latency_score = 100.0 * (1.0 - (metrics.avg_latency_ms / CRITICAL_LATENCY_MS).min(1.0));
        let consistency_score = 100.0 * (1.0 - (metrics.jitter_ms / 50.0).min(1.0));
        let reliability_score = metrics.success_rate;
        let health_score = metrics.health_score;
        let stake_score = (metrics.stake_weight as f64 / 1_000_000_000_000.0).min(1.0) * 100.0;
        
        (latency_score * 0.35) +
        (consistency_score * 0.25) +
        (reliability_score * 0.20) +
        (health_score * 0.15) +
        (stake_score * 0.05)
    }

    pub async fn get_fastest_validators(&self, percentile: f64, min_samples: usize) -> Vec<(Pubkey, f64)> {
        let mut validators = Vec::new();
        
        for entry in self.validators.iter() {
            if let Ok(metrics) = entry.value().read().await {
                if metrics.is_active && metrics.slot_latencies.len() >= min_samples {
                    let latency = match percentile {
                        p if p <= 0.50 => metrics.median_latency_ms,
                        p if p <= 0.75 => metrics.p75_latency_ms,
                        p if p <= 0.90 => metrics.p90_latency_ms,
                        p if p <= 0.95 => metrics.p95_latency_ms,
                        _ => metrics.p99_latency_ms,
                    };
                    validators.push((*entry.key(), latency));
                }
            }
        }
        
        validators.sort_by(|a, b| {
            match (a.1.is_nan(), b.1.is_nan()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                (false, false) => a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal),
            }
        });
        validators
    }

    pub async fn should_use_validator(&self, validator: &Pubkey) -> bool {
        if let Some(metrics_lock) = self.validators.get(validator) {
            if let Ok(metrics) = metrics_lock.read().await {
                return metrics.is_active &&
                       metrics.health_score > 75.0 &&
                       metrics.avg_latency_ms < CRITICAL_LATENCY_MS &&
                       metrics.success_rate > 95.0 &&
                       metrics.consecutive_failures < 3 &&
                       metrics.slots_behind < 3;
            }
        }
        false
    }

    pub async fn track_transaction(&self, validator: Pubkey, signature: Signature, slot: Slot) -> Result<()> {
        let metrics_lock = self.validators.get(&validator)
            .context("Validator not found")?;
        
        let mut metrics = metrics_lock.write().await;
        
        let tx_latency = TransactionLatency {
            signature,
            submission_time_ms: Self::current_timestamp_ms(),
            confirmation_time_ms: None,
            latency_ms: None,
            slot,
            status: TxStatus::Pending,
        };
        
        metrics.transaction_latencies.push_back(tx_latency);
        
        while metrics.transaction_latencies.len() > LATENCY_WINDOW_SIZE {
            metrics.transaction_latencies.pop_front();
        }
        
        Ok(())
    }

    pub async fn confirm_transaction(&self, validator: Pubkey, signature: Signature, success: bool) -> Result<()> {
        let metrics_lock = self.validators.get(&validator)
            .context("Validator not found")?;
        
        let mut metrics = metrics_lock.write().await;
        let confirmation_time = Self::current_timestamp_ms();
        
        for tx in metrics.transaction_latencies.iter_mut().rev() {
            if tx.signature == signature && tx.status == TxStatus::Pending {
                tx.confirmation_time_ms = Some(confirmation_time);
                tx.latency_ms = Some((confirmation_time - tx.submission_time_ms) as f64);
                tx.status = if success { TxStatus::Confirmed } else { TxStatus::Failed };
                break;
            }
        }
        
        Ok(())
    }

    pub async fn get_transaction_stats(&self, validator: &Pubkey) -> Option<(f64, f64, f64)> {
        let metrics_lock = self.validators.get(validator)?;
        
        if let Ok(metrics) = metrics_lock.read().await {
            let confirmed: Vec<f64> = metrics.transaction_latencies.iter()
                .filter_map(|tx| {
                    if tx.status == TxStatus::Confirmed {
                        tx.latency_ms
                    } else {
                        None
                    }
                })
                .collect();
            
            if confirmed.is_empty() {
                return None;
            }
            
            let total_txs = metrics.transaction_latencies.len() as f64;
            let confirmed_txs = confirmed.len() as f64;
            let success_rate = (confirmed_txs / total_txs) * 100.0;
            let avg_latency = self.calculate_mean(&confirmed);
            
            Some((success_rate, avg_latency, confirmed_txs))
        } else {
            None
        }
    }

    pub async fn get_validator_metrics(&self, validator: &Pubkey) -> Option<ValidatorMetrics> {
        self.validators.get(validator)
            .and_then(|lock| lock.read().await.ok().map(|m| m.clone()))
    }

    pub async fn export_metrics(&self) -> HashMap<Pubkey, ValidatorMetrics> {
        let mut all_metrics = HashMap::new();
        
        for entry in self.validators.iter() {
            if let Ok(metrics) = entry.value().read().await {
                all_metrics.insert(*entry.key(), metrics.clone());
            }
        }
        
        all_metrics
    }

        pub async fn get_global_stats(&self) -> (u64, u64, f64) {
        let total = self.total_probes.load(Ordering::Acquire);
        let successful = self.successful_probes.load(Ordering::Acquire);
        let success_rate = if total > 0 {
            (successful as f64 / total as f64) * 100.0
        } else {
            0.0
        };
        
        (total, successful, success_rate)
    }

    pub async fn get_best_performing_validator(&self) -> Option<(Pubkey, ValidatorMetrics)> {
        let mut best: Option<(Pubkey, f64, ValidatorMetrics)> = None;
        
        for entry in self.validators.iter() {
            if let Ok(metrics) = entry.value().read().await {
                if metrics.is_active && 
                   metrics.slot_latencies.len() >= MIN_SAMPLES_FOR_STATS &&
                   metrics.health_score > 80.0 {
                    
                    let score = self.calculate_composite_score(&metrics);
                    
                    match &best {
                        None => best = Some((*entry.key(), score, metrics.clone())),
                        Some((_, best_score, _)) if score > *best_score => {
                            best = Some((*entry.key(), score, metrics.clone()));
                        }
                        _ => {}
                    }
                }
            }
        }
        
        best.map(|(pubkey, _, metrics)| (pubkey, metrics))
    }

    pub async fn get_validators_by_health_score(&self, min_score: f64) -> Vec<(Pubkey, f64)> {
        let mut validators = Vec::new();
        
        for entry in self.validators.iter() {
            if let Ok(metrics) = entry.value().read().await {
                if metrics.is_active && metrics.health_score >= min_score {
                    validators.push((*entry.key(), metrics.health_score));
                }
            }
        }
        
        validators.sort_by(|a, b| {
            match (a.1.is_nan(), b.1.is_nan()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                (false, false) => b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal),
            }
        });
        validators
    }

    pub async fn get_latency_percentile_for_validator(&self, validator: &Pubkey, percentile: f64) -> Option<f64> {
        let metrics_lock = self.validators.get(validator)?;
        
        if let Ok(metrics) = metrics_lock.read().await {
            if metrics.slot_latencies.len() < MIN_SAMPLES_FOR_STATS {
                return None;
            }
            
            match percentile {
                p if p <= 0.50 => Some(metrics.median_latency_ms),
                p if p <= 0.75 => Some(metrics.p75_latency_ms),
                p if p <= 0.90 => Some(metrics.p90_latency_ms),
                p if p <= 0.95 => Some(metrics.p95_latency_ms),
                _ => Some(metrics.p99_latency_ms),
            }
        } else {
            None
        }
    }

    pub async fn get_recent_latency_trend(&self, validator: &Pubkey, window_size: usize) -> Option<Vec<f64>> {
        let metrics_lock = self.validators.get(validator)?;
        
        if let Ok(metrics) = metrics_lock.read().await {
            let recent_latencies: Vec<f64> = metrics.slot_latencies
                .iter()
                .rev()
                .take(window_size)
                .map(|s| s.latency_ms)
                .collect();
            
            if recent_latencies.is_empty() {
                None
            } else {
                Some(recent_latencies)
            }
        } else {
            None
        }
    }

    pub async fn detect_latency_spike(&self, validator: &Pubkey, threshold_multiplier: f64) -> bool {
        if let Some(metrics_lock) = self.validators.get(validator) {
            if let Ok(metrics) = metrics_lock.read().await {
                if metrics.slot_latencies.len() < 10 {
                    return false;
                }
                
                let recent: Vec<f64> = metrics.slot_latencies
                    .iter()
                    .rev()
                    .take(5)
                    .map(|s| s.latency_ms)
                    .collect();
                
                let recent_avg = self.calculate_mean(&recent);
                
                return recent_avg > metrics.avg_latency_ms * threshold_multiplier;
            }
        }
        false
    }

    pub async fn get_validators_for_redundancy(&self, primary: &Pubkey, count: usize) -> Vec<Pubkey> {
        let mut candidates: Vec<(Pubkey, f64)> = Vec::new();
        
        for entry in self.validators.iter() {
            let validator = *entry.key();
            if validator == *primary {
                continue;
            }
            
            if let Ok(metrics) = entry.value().read().await {
                if metrics.is_active && 
                   metrics.health_score > 70.0 &&
                   metrics.avg_latency_ms < CRITICAL_LATENCY_MS {
                    
                    let score = self.calculate_composite_score(&metrics);
                    candidates.push((validator, score));
                }
            }
        }
        
        candidates.sort_by(|a, b| {
            match (a.1.is_nan(), b.1.is_nan()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Greater,
                (false, true) => std::cmp::Ordering::Less,
                (false, false) => b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal),
            }
        });
        candidates.into_iter()
            .take(count)
            .map(|(v, _)| v)
            .collect()
    }

    pub async fn reset_validator_metrics(&self, validator: &Pubkey) -> Result<()> {
        let metrics_lock = self.validators.get(validator)
            .context("Validator not found")?;
        
        let mut metrics = metrics_lock.write().await;
        
        metrics.slot_latencies.clear();
        metrics.transaction_latencies.clear();
        metrics.total_probes = 0;
        metrics.successful_probes = 0;
        metrics.consecutive_failures = 0;
        metrics.health_score = 50.0;
        metrics.success_rate = 0.0;
        metrics.avg_latency_ms = 0.0;
        metrics.median_latency_ms = 0.0;
        metrics.p75_latency_ms = 0.0;
        metrics.p90_latency_ms = 0.0;
        metrics.p95_latency_ms = 0.0;
        metrics.p99_latency_ms = 0.0;
        metrics.jitter_ms = 0.0;
        
        Ok(())
    }

    pub async fn update_validator_endpoint(&self, validator: Pubkey, rpc_endpoint: String, tpu_endpoint: String) -> Result<()> {
        let client = Arc::new(RpcClient::new_with_timeout_and_commitment(
            rpc_endpoint.clone(),
            PROBE_TIMEOUT,
            CommitmentConfig::processed(),
        ));
        
        self.rpc_clients.insert(validator, client);
        
        if let Some(metrics_lock) = self.validators.get(&validator) {
            let mut metrics = metrics_lock.write().await;
            metrics.rpc_endpoint = rpc_endpoint;
            metrics.tpu_endpoint = tpu_endpoint;
            metrics.is_active = true;
            metrics.consecutive_failures = 0;
        } else {
            let metrics = ValidatorMetrics {
                identity: validator,
                rpc_endpoint,
                tpu_endpoint,
                last_update_ms: Self::current_timestamp_ms()?,
                tpu_latencies: VecDeque::with_capacity(LATENCY_WINDOW_SIZE),
                p999_latency_ms: 0.0,
                is_leader_next_slots: Vec::with_capacity(32),
                jito_enabled: false,
                mev_rewards_earned: 0,
                bundle_success_rate: 0.0,
                tpu_connection_quality: 0.0,
                geographic_region: None,
                last_leader_slot: None,
                skip_rate: 0.0,
                vote_latency_ms: 0.0,
                slot_latencies: VecDeque::with_capacity(LATENCY_WINDOW_SIZE),
                transaction_latencies: VecDeque::with_capacity(LATENCY_WINDOW_SIZE),
                health_score: 50.0,
                success_rate: 0.0,
                avg_latency_ms: 0.0,
                median_latency_ms: 0.0,
                p75_latency_ms: 0.0,
                p90_latency_ms: 0.0,
                p95_latency_ms: 0.0,
                p99_latency_ms: 0.0,
                jitter_ms: 0.0,
                total_probes: 0,
                successful_probes: 0,
                consecutive_failures: 0,
                is_active: true,
                stake_weight: 0,
                commission: 0,
                version: None,
                last_slot: 0,
                slots_behind: 0,
            };
            
            self.validators.insert(validator, Arc::new(RwLock::new(metrics)));
        }
        
        Ok(())
    }

    pub async fn remove_validator(&self, validator: &Pubkey) -> Result<()> {
        self.validators.remove(validator)
            .context("Validator not found")?;
        self.rpc_clients.remove(validator);
        
        Ok(())
    }

    pub async fn get_active_validator_count(&self) -> usize {
        self.validators.iter()
            .filter(|entry| {
                if let Ok(metrics) = entry.value().try_read() {
                    metrics.is_active
                } else {
                    false
                }
            })
            .count()
    }

    pub async fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    fn current_timestamp_ms() -> Result<u64> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .map_err(|e| anyhow!("Failed to get current timestamp: {}", e))
    }

    // NEW MEV-OPTIMIZED METHODS FOR MAINNET COMPETITION
    
    async fn run_tpu_monitor(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(TPU_PROBE_INTERVAL);
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            let validators: Vec<Pubkey> = self.validators.iter()
                .filter_map(|entry| {
                    let validator = *entry.key();
                    match entry.value().try_read() {
                        Ok(metrics) if metrics.is_active => Some(validator),
                        _ => None,
                    }
                })
                .collect();

            for validator in validators {
                let profiler = self.clone();
                tokio::spawn(async move {
                    let _ = profiler.probe_tpu_latency(validator).await;
                });
            }
        }
        
        Ok(())
    }
    
    async fn probe_tpu_latency(&self, validator: Pubkey) -> Result<()> {
        let start = Instant::now();
        
        if let Some(client) = self.tpu_clients.get(&validator) {
            let connect_start = Instant::now();
            
            // Simulate TPU connection attempt
            let connection_result = timeout(Duration::from_millis(100), async {
                // In a real implementation, this would attempt to connect to TPU
                sleep(Duration::from_millis(10)).await;
                Ok::<(), anyhow::Error>(())
            }).await;
            
            let connect_latency_ms = connect_start.elapsed().as_secs_f64() * 1000.0;
            let connection_success = connection_result.is_ok();
            
            let send_latency_ms = if connection_success {
                let send_start = Instant::now();
                // Simulate sending a transaction
                sleep(Duration::from_millis(5)).await;
                send_start.elapsed().as_secs_f64() * 1000.0
            } else {
                0.0
            };
            
            if let Some(metrics_lock) = self.validators.get(&validator) {
                if let Ok(mut metrics) = metrics_lock.write().await {
                    let tpu_latency = TpuLatency {
                        timestamp_ms: Self::current_timestamp_ms()?,
                        connect_latency_ms,
                        send_latency_ms,
                        ack_latency_ms: None,
                        connection_success,
                    };
                    
                    metrics.tpu_latencies.push_back(tpu_latency);
                    while metrics.tpu_latencies.len() > LATENCY_WINDOW_SIZE {
                        metrics.tpu_latencies.pop_front();
                    }
                    
                    // Update TPU connection quality score
                    let success_rate = metrics.tpu_latencies.iter()
                        .map(|t| if t.connection_success { 1.0 } else { 0.0 })
                        .sum::<f64>() / metrics.tpu_latencies.len() as f64;
                    
                    let avg_latency = metrics.tpu_latencies.iter()
                        .map(|t| t.connect_latency_ms + t.send_latency_ms)
                        .sum::<f64>() / metrics.tpu_latencies.len() as f64;
                    
                    metrics.tpu_connection_quality = success_rate * (1.0 - (avg_latency / 100.0).min(1.0));
                }
            }
        }
        
        Ok(())
    }
    
    async fn run_leader_schedule_updater(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(LEADER_SCHEDULE_UPDATE_INTERVAL);
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            if let Err(e) = self.update_leader_schedule().await {
                warn!("Failed to update leader schedule: {}", e);
            }
        }
        
        Ok(())
    }
    
    async fn update_leader_schedule(&self) -> Result<()> {
        // Get any available RPC client
        if let Some(client_entry) = self.rpc_clients.iter().next() {
            let client = client_entry.value();
            
            match timeout(Duration::from_secs(10), client.get_epoch_info()).await {
                Ok(Ok(epoch_info)) => {
                    *self.epoch_info.write().await = Some(epoch_info.clone());
                    
                    // Fetch leader schedule for current and next epoch
                    if let Ok(Ok(leader_schedule)) = timeout(
                        Duration::from_secs(15),
                        client.get_leader_schedule(Some(epoch_info.epoch))
                    ).await {
                        let mut schedule_map = BTreeMap::new();
                        
                        // Convert leader schedule to slot -> validator mapping
                        for (validator_str, slots) in leader_schedule {
                            if let Ok(validator_pubkey) = validator_str.parse::<Pubkey>() {
                                for &slot in slots {
                                    schedule_map.insert(slot, validator_pubkey);
                                }
                            }
                        }
                        
                        *self.leader_schedule.write().await = schedule_map.clone();
                        
                        // Update validator metrics with upcoming leader slots
                        let current_slot = self.global_best_slot.load(Ordering::Acquire);
                        for entry in self.validators.iter() {
                            let validator = *entry.key();
                            if let Ok(mut metrics) = entry.value().write().await {
                                metrics.is_leader_next_slots = schedule_map
                                    .range(current_slot..current_slot + 100)
                                    .filter_map(|(slot, leader)| {
                                        if *leader == validator {
                                            Some(*slot)
                                        } else {
                                            None
                                        }
                                    })
                                    .collect();
                            }
                        }
                        
                        let _ = self.alert_sender.send(AlertEvent::LeaderScheduleUpdated {
                            epoch: epoch_info.epoch,
                            validator_count: self.validators.len(),
                        });
                        
                        info!("Updated leader schedule for epoch {} with {} validators", 
                              epoch_info.epoch, self.validators.len());
                    }
                }
                Ok(Err(e)) => {
                    warn!("RPC error fetching epoch info: {}", e);
                }
                Err(_) => {
                    warn!("Timeout fetching epoch info");
                }
            }
        }
        
        Ok(())
    }
    
    async fn run_jito_tip_account_updater(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(JITO_TIP_ACCOUNT_UPDATE_INTERVAL);
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            if let Err(e) = self.update_jito_tip_accounts().await {
                warn!("Failed to update Jito tip accounts: {}", e);
            }
        }
        
        Ok(())
    }
    
    async fn update_jito_tip_accounts(&self) -> Result<()> {
        // Fetch Jito tip accounts from known endpoints
        let jito_api_urls = vec![
            "https://mainnet.block-engine.jito.wtf/api/v1/bundles",
            "https://amsterdam.mainnet.block-engine.jito.wtf/api/v1/bundles",
            "https://frankfurt.mainnet.block-engine.jito.wtf/api/v1/bundles",
            "https://ny.mainnet.block-engine.jito.wtf/api/v1/bundles",
            "https://tokyo.mainnet.block-engine.jito.wtf/api/v1/bundles",
        ];
        
        // In a real implementation, we would fetch tip accounts from Jito's API
        // For now, we'll use hardcoded known tip accounts
        let known_tip_accounts = vec![
            "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5".parse::<Pubkey>()?,
            "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe".parse::<Pubkey>()?,
            "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY".parse::<Pubkey>()?,
            "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49".parse::<Pubkey>()?,
        ];
        
        *self.jito_tip_accounts.write().await = known_tip_accounts.clone();
        
        // Update validator Jito support detection
        for entry in self.validators.iter() {
            if let Ok(mut metrics) = entry.value().write().await {
                // In a real implementation, we would check if the validator supports Jito
                // by examining their vote account or checking for Jito-Solana client
                metrics.jito_enabled = true; // Assume all validators support Jito for now
            }
        }
        
        let _ = self.alert_sender.send(AlertEvent::JitoTipAccountsUpdated {
            count: known_tip_accounts.len(),
        });
        
        debug!("Updated {} Jito tip accounts", known_tip_accounts.len());
        Ok(())
    }
    
    async fn run_memory_monitor(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(Duration::from_secs(30));
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            let mut total_memory_usage = 0usize;
            
            // Estimate memory usage
            for entry in self.validators.iter() {
                if let Ok(metrics) = entry.value().read().await {
                    total_memory_usage += std::mem::size_of_val(&*metrics);
                    total_memory_usage += metrics.slot_latencies.len() * std::mem::size_of::<SlotLatency>();
                    total_memory_usage += metrics.transaction_latencies.len() * std::mem::size_of::<TransactionLatency>();
                    total_memory_usage += metrics.tpu_latencies.len() * std::mem::size_of::<TpuLatency>();
                }
            }
            
            // Add cache memory usage
            if let Ok(cache) = self.latency_cache.try_lock() {
                total_memory_usage += cache.len() * (std::mem::size_of::<(Pubkey, u64)>() + std::mem::size_of::<f64>());
            }
            
            let memory_mb = total_memory_usage / (1024 * 1024);
            self.memory_monitor.store(memory_mb as u64, Ordering::Release);
            
            if memory_mb > MAX_MEMORY_USAGE_MB {
                let _ = self.alert_sender.send(AlertEvent::MemoryUsageHigh {
                    current_mb: memory_mb,
                    limit_mb: MAX_MEMORY_USAGE_MB,
                });
                
                warn!("Memory usage high: {}MB (limit: {}MB)", memory_mb, MAX_MEMORY_USAGE_MB);
                
                // Trigger aggressive cleanup
                self.cleanup_old_data().await?;
            }
            
            // Log memory usage instead of using metrics crate
            debug!("Memory usage: {}MB", memory_mb);
        }
        
        Ok(())
    }
    
    async fn cleanup_old_data(&self) -> Result<()> {
        let cutoff_time = Self::current_timestamp_ms()? - (5 * 60 * 1000); // 5 minutes ago
        
        for entry in self.validators.iter() {
            if let Ok(mut metrics) = entry.value().write().await {
                // Keep only recent data
                metrics.slot_latencies.retain(|lat| lat.timestamp_ms > cutoff_time);
                metrics.transaction_latencies.retain(|lat| lat.submission_time_ms > cutoff_time);
                metrics.tpu_latencies.retain(|lat| lat.timestamp_ms > cutoff_time);
            }
        }
        
        // Clean latency cache
        let mut cache = self.latency_cache.lock().await;
        let current_time_seconds = cutoff_time / 1000;
        cache.retain(|&(_, timestamp), _| timestamp > current_time_seconds);
        
        info!("Cleaned up old data older than 5 minutes");
        Ok(())
    }
    
    async fn run_mev_opportunity_detector(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(Duration::from_millis(100)); // Very frequent for MEV detection
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            // Detect MEV opportunities based on latency patterns
            for entry in self.validators.iter() {
                let validator = *entry.key();
                if let Ok(metrics) = entry.value().read().await {
                    if self.is_mev_opportunity(&metrics).await {
                        let estimated_profit = self.estimate_mev_profit(&metrics).await;
                        
                        if estimated_profit > 0.001 { // Minimum 0.001 SOL profit threshold
                            let _ = self.alert_sender.send(AlertEvent::MevOpportunity {
                                validator,
                                estimated_profit,
                            });
                            
                            self.performance_metrics.mev_opportunities_detected
                                .fetch_add(1, Ordering::AcqRel);
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
    
    async fn is_mev_opportunity(&self, metrics: &ValidatorMetrics) -> bool {
        // Check for MEV opportunity indicators:
        // 1. Low latency validator
        // 2. Is upcoming leader
        // 3. Good TPU connection
        // 4. High stake weight
        
        metrics.avg_latency_ms < OPTIMAL_LATENCY_MS &&
        !metrics.is_leader_next_slots.is_empty() &&
        metrics.tpu_connection_quality > 0.8 &&
        metrics.stake_weight > 1_000_000_000_000 && // 1M SOL minimum stake
        metrics.jito_enabled
    }
    
    async fn estimate_mev_profit(&self, metrics: &ValidatorMetrics) -> f64 {
        // Simple MEV profit estimation based on:
        // - Validator's recent performance
        // - Number of upcoming leader slots
        // - Network congestion (estimated from latency)
        
        let base_profit = 0.01; // Base 0.01 SOL per opportunity
        let latency_multiplier = (OPTIMAL_LATENCY_MS / metrics.avg_latency_ms.max(1.0)).min(3.0);
        let leader_slot_multiplier = (metrics.is_leader_next_slots.len() as f64).min(10.0);
        let tpu_quality_multiplier = metrics.tpu_connection_quality + 0.5;
        
        base_profit * latency_multiplier * leader_slot_multiplier * tpu_quality_multiplier
    }
    
    async fn run_circuit_breaker_monitor(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(Duration::from_secs(10));
        
        while self.running.load(Ordering::Acquire) {
            interval.tick().await;
            
            for entry in self.circuit_breakers.iter() {
                let validator = *entry.key();
                let circuit_breaker = entry.value();
                
                // Check circuit breaker state
                let state = circuit_breaker.state.load(Ordering::Acquire);
                if state == CircuitBreakerState::Open as u32 {
                    let _ = self.alert_sender.send(AlertEvent::CircuitBreakerTripped { validator });
                    
                    // Temporarily disable validator
                    if let Some(metrics_lock) = self.validators.get(&validator) {
                        if let Ok(mut metrics) = metrics_lock.write().await {
                            metrics.is_active = false;
                        }
                    }
                    
                    warn!("Circuit breaker tripped for validator {}", validator);
                }
            }
        }
        
        Ok(())
    }
}

// Macro for deferred execution
macro_rules! defer {
    ($e:expr) => {
        let _defer = Defer::new(|| $e);
    };
}

struct Defer<F: FnOnce()> {
    f: Option<F>,
}

impl<F: FnOnce()> Defer<F> {
    fn new(f: F) -> Self {
        Self { f: Some(f) }
    }
}

impl<F: FnOnce()> Drop for Defer<F> {
    fn drop(&mut self) {
        if let Some(f) = self.f.take() {
            f();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;

    #[tokio::test]
    async fn test_validator_latency_profiler_creation() {
        let validator = Keypair::new().pubkey();
        let configs = vec![(
            validator,
            "https://api.mainnet-beta.solana.com".to_string(),
            "api.mainnet-beta.solana.com:8001".to_string(),
        )];
        
        let profiler = ValidatorLatencyProfiler::new(configs).await.unwrap();
        assert!(profiler.is_running());
        assert_eq!(profiler.get_active_validator_count().await, 1);
    }

    #[tokio::test]
    async fn test_percentile_calculation() {
        let profiler = ValidatorLatencyProfiler::new(vec![]).await.unwrap();
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        
        assert_eq!(profiler.calculate_percentile(&values, 0.5), 5.0);
        assert_eq!(profiler.calculate_percentile(&values, 0.9), 9.0);
    }

    #[tokio::test]
    async fn test_health_score_calculation() {
        let profiler = ValidatorLatencyProfiler::new(vec![]).await.unwrap();
        
        let mut metrics = ValidatorMetrics {
            identity: Pubkey::default(),
            rpc_endpoint: String::new(),
            tpu_endpoint: String::new(),
            last_update_ms: 0,
            slot_latencies: VecDeque::new(),
            transaction_latencies: VecDeque::new(),
            health_score: 0.0,
            success_rate: 100.0,
            avg_latency_ms: 20.0,
            median_latency_ms: 20.0,
            p75_latency_ms: 25.0,
            p90_latency_ms: 30.0,
            p95_latency_ms: 35.0,
            p99_latency_ms: 40.0,
            jitter_ms: 5.0,
            total_probes: 1000,
            successful_probes: 1000,
            consecutive_failures: 0,
            is_active: true,
            stake_weight: 1_000_000_000_000,
            commission: 5,
            version: Some("1.17.0".to_string()),
            last_slot: 100,
            slots_behind: 0,
        };
        
        let score = profiler.calculate_health_score(&metrics);
        assert!(score > 90.0);
        assert!(score <= 110.0);
    }
}

