use serde::{Deserialize, Serialize};
use anyhow::{Result, anyhow};
use std::path::Path;
use std::time::Duration;
use solana_sdk::pubkey::Pubkey;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MevBotConfig {
    pub rpc: RpcConfig,
    pub database: DatabaseConfig,
    pub calculator: CalculatorConfig,
    pub metrics: MetricsConfig,
    pub api: ApiConfig,
    pub circuit_breaker: CircuitBreakerConfig,
    pub rate_limiting: RateLimitingConfig,
    pub security: SecurityConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcConfig {
    pub primary_url: String,
    pub fallback_urls: Vec<String>,
    pub timeout_seconds: u64,
    pub max_retries: u32,
    pub health_check_interval_seconds: u64,
    pub connection_pool_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub url: String,
    pub max_connections: u32,
    pub min_connections: u32,
    pub connection_timeout_seconds: u64,
    pub idle_timeout_seconds: u64,
    pub max_lifetime_seconds: u64,
    pub batch_size: usize,
    pub flush_interval_seconds: u64,
    pub retention_hours: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalculatorConfig {
    pub max_history_entries: usize,
    pub update_interval_seconds: u64,
    pub min_statistical_samples: usize,
    pub outlier_threshold_sigma: f64,
    pub jito_min_tip_lamports: u64,
    pub max_tip_lamports: u64,
    pub confidence_multiplier: f64,
    pub slot_position_penalty: f64,
    pub congestion_multiplier_high: f64,
    pub congestion_multiplier_medium: f64,
    pub validator_performance_weight: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub port: u16,
    pub path: String,
    pub collection_interval_seconds: u64,
    pub alert_thresholds: AlertThresholdsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertThresholdsConfig {
    pub min_success_rate: f64,
    pub max_failure_rate: f64,
    pub max_tip_spike_ratio: f64,
    pub max_validator_rejection_rate: f64,
    pub max_response_time_ms: f64,
    pub max_error_rate: f64,
    pub alert_cooldown_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub cors_origins: Vec<String>,
    pub request_timeout_seconds: u64,
    pub max_request_size_bytes: usize,
    pub authentication: AuthConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    pub enabled: bool,
    pub api_key_header: String,
    pub valid_api_keys: Vec<String>,
    pub jwt_secret: Option<String>,
    pub token_expiry_hours: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    pub failure_threshold: u32,
    pub timeout_seconds: u64,
    pub max_timeout_seconds: u64,
    pub half_open_max_calls: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitingConfig {
    pub enabled: bool,
    pub requests_per_minute: u32,
    pub burst_size: u32,
    pub validator_specific_limits: bool,
    pub cleanup_interval_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub input_validation: InputValidationConfig,
    pub sanitization: SanitizationConfig,
    pub encryption: EncryptionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputValidationConfig {
    pub max_bundle_id_length: usize,
    pub allowed_bundle_id_chars: String,
    pub max_tip_lamports: u64,
    pub min_tip_lamports: u64,
    pub validate_pubkeys: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SanitizationConfig {
    pub strip_whitespace: bool,
    pub normalize_case: bool,
    pub max_string_length: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionConfig {
    pub encrypt_sensitive_data: bool,
    pub encryption_key_path: Option<String>,
    pub hash_bundle_ids: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub json_format: bool,
    pub include_file_line: bool,
    pub include_thread_id: bool,
    pub log_to_file: Option<String>,
    pub log_rotation: LogRotationConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRotationConfig {
    pub enabled: bool,
    pub max_file_size_mb: u64,
    pub max_files: u32,
    pub compress_old_files: bool,
}

impl Default for MevBotConfig {
    fn default() -> Self {
        Self {
            rpc: RpcConfig::default(),
            database: DatabaseConfig::default(),
            calculator: CalculatorConfig::default(),
            metrics: MetricsConfig::default(),
            api: ApiConfig::default(),
            circuit_breaker: CircuitBreakerConfig::default(),
            rate_limiting: RateLimitingConfig::default(),
            security: SecurityConfig::default(),
            logging: LoggingConfig::default(),
        }
    }
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            primary_url: "https://api.mainnet-beta.solana.com".to_string(),
            fallback_urls: vec![
                "https://solana-api.projectserum.com".to_string(),
                "https://api.mainnet-beta.solana.com".to_string(),
            ],
            timeout_seconds: 30,
            max_retries: 3,
            health_check_interval_seconds: 60,
            connection_pool_size: 10,
        }
    }
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: "postgresql://localhost:5432/mev_bot".to_string(),
            max_connections: 20,
            min_connections: 5,
            connection_timeout_seconds: 30,
            idle_timeout_seconds: 300,
            max_lifetime_seconds: 3600,
            batch_size: 100,
            flush_interval_seconds: 10,
            retention_hours: 168, // 1 week
        }
    }
}

impl Default for CalculatorConfig {
    fn default() -> Self {
        Self {
            max_history_entries: 50_000,
            update_interval_seconds: 300, // 5 minutes
            min_statistical_samples: 30,
            outlier_threshold_sigma: 3.0,
            jito_min_tip_lamports: 1_000,
            max_tip_lamports: 100_000_000, // 0.1 SOL
            confidence_multiplier: 1.2,
            slot_position_penalty: 0.1,
            congestion_multiplier_high: 2.0,
            congestion_multiplier_medium: 1.5,
            validator_performance_weight: 0.3,
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 9090,
            path: "/metrics".to_string(),
            collection_interval_seconds: 60,
            alert_thresholds: AlertThresholdsConfig::default(),
        }
    }
}

impl Default for AlertThresholdsConfig {
    fn default() -> Self {
        Self {
            min_success_rate: 0.7,
            max_failure_rate: 0.3,
            max_tip_spike_ratio: 3.0,
            max_validator_rejection_rate: 0.2,
            max_response_time_ms: 2000.0,
            max_error_rate: 0.05,
            alert_cooldown_seconds: 300,
        }
    }
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            host: "127.0.0.1".to_string(),
            port: 8080,
            cors_origins: vec!["http://localhost:3000".to_string()],
            request_timeout_seconds: 30,
            max_request_size_bytes: 1_048_576, // 1MB
            authentication: AuthConfig::default(),
        }
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            api_key_header: "X-API-Key".to_string(),
            valid_api_keys: Vec::new(), // Must be configured
            jwt_secret: None, // Must be configured if using JWT
            token_expiry_hours: 24,
        }
    }
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            timeout_seconds: 60,
            max_timeout_seconds: 300,
            half_open_max_calls: 3,
        }
    }
}

impl Default for RateLimitingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            requests_per_minute: 1000,
            burst_size: 100,
            validator_specific_limits: true,
            cleanup_interval_seconds: 60,
        }
    }
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            input_validation: InputValidationConfig::default(),
            sanitization: SanitizationConfig::default(),
            encryption: EncryptionConfig::default(),
        }
    }
}

impl Default for InputValidationConfig {
    fn default() -> Self {
        Self {
            max_bundle_id_length: 64,
            allowed_bundle_id_chars: "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_".to_string(),
            max_tip_lamports: 1_000_000_000, // 1 SOL
            min_tip_lamports: 1_000,          // Jito minimum
            validate_pubkeys: true,
        }
    }
}

impl Default for SanitizationConfig {
    fn default() -> Self {
        Self {
            strip_whitespace: true,
            normalize_case: false,
            max_string_length: 1024,
        }
    }
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            encrypt_sensitive_data: false,
            encryption_key_path: None,
            hash_bundle_ids: false,
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            json_format: true,
            include_file_line: true,
            include_thread_id: true,
            log_to_file: Some("mev_bot.log".to_string()),
            log_rotation: LogRotationConfig::default(),
        }
    }
}

impl Default for LogRotationConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_file_size_mb: 100,
            max_files: 10,
            compress_old_files: true,
        }
    }
}

impl MevBotConfig {
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow!("Failed to read config file: {}", e))?;
        
        toml::from_str(&content)
            .map_err(|e| anyhow!("Failed to parse config: {}", e))
    }

    pub fn from_env() -> Result<Self> {
        let mut config = Self::default();
        
        // Override with environment variables
        if let Ok(url) = std::env::var("SOLANA_RPC_URL") {
            config.rpc.primary_url = url;
        }
        
        if let Ok(db_url) = std::env::var("DATABASE_URL") {
            config.database.url = db_url;
        }
        
        if let Ok(log_level) = std::env::var("LOG_LEVEL") {
            config.logging.level = log_level;
        }
        
        if let Ok(api_port) = std::env::var("API_PORT") {
            config.api.port = api_port.parse()
                .map_err(|e| anyhow!("Invalid API_PORT: {}", e))?;
        }
        
        if let Ok(metrics_port) = std::env::var("METRICS_PORT") {
            config.metrics.port = metrics_port.parse()
                .map_err(|e| anyhow!("Invalid METRICS_PORT: {}", e))?;
        }

        // API keys from environment
        if let Ok(api_keys) = std::env::var("API_KEYS") {
            config.api.authentication.valid_api_keys = api_keys
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
        }

        if let Ok(jwt_secret) = std::env::var("JWT_SECRET") {
            config.api.authentication.jwt_secret = Some(jwt_secret);
        }

        Ok(config)
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let content = toml::to_string_pretty(self)
            .map_err(|e| anyhow!("Failed to serialize config: {}", e))?;
        
        std::fs::write(path, content)
            .map_err(|e| anyhow!("Failed to write config file: {}", e))
    }

    pub fn validate(&self) -> Result<()> {
        // Validate RPC configuration
        if self.rpc.primary_url.is_empty() {
            return Err(anyhow!("Primary RPC URL cannot be empty"));
        }
        
        if self.rpc.timeout_seconds == 0 || self.rpc.timeout_seconds > 300 {
            return Err(anyhow!("RPC timeout must be between 1-300 seconds"));
        }

        // Validate database configuration
        if self.database.url.is_empty() {
            return Err(anyhow!("Database URL cannot be empty"));
        }
        
        if self.database.max_connections < self.database.min_connections {
            return Err(anyhow!("Max connections must be >= min connections"));
        }

        // Validate calculator configuration
        if self.calculator.jito_min_tip_lamports >= self.calculator.max_tip_lamports {
            return Err(anyhow!("Max tip must be greater than min tip"));
        }
        
        if self.calculator.outlier_threshold_sigma <= 0.0 {
            return Err(anyhow!("Outlier threshold sigma must be positive"));
        }

        // Validate metrics configuration
        if self.metrics.enabled && (self.metrics.port == 0 || self.metrics.port == self.api.port) {
            return Err(anyhow!("Metrics port must be valid and different from API port"));
        }

        // Validate API configuration
        if self.api.enabled && self.api.port == 0 {
            return Err(anyhow!("API port must be valid when API is enabled"));
        }

        // Validate authentication if enabled
        if self.api.authentication.enabled && self.api.authentication.valid_api_keys.is_empty() && self.api.authentication.jwt_secret.is_none() {
            return Err(anyhow!("At least one authentication method must be configured"));
        }

        // Validate circuit breaker configuration
        if self.circuit_breaker.failure_threshold == 0 {
            return Err(anyhow!("Circuit breaker failure threshold must be positive"));
        }

        Ok(())
    }

    // Get timeout duration for components
    pub fn rpc_timeout(&self) -> Duration {
        Duration::from_secs(self.rpc.timeout_seconds)
    }

    pub fn database_connection_timeout(&self) -> Duration {
        Duration::from_secs(self.database.connection_timeout_seconds)
    }

    pub fn api_request_timeout(&self) -> Duration {
        Duration::from_secs(self.api.request_timeout_seconds)
    }

    pub fn circuit_breaker_timeout(&self) -> Duration {
        Duration::from_secs(self.circuit_breaker.timeout_seconds)
    }

    // Helper methods for type conversions
    pub fn get_fallback_rpc_urls(&self) -> Vec<String> {
        let mut urls = vec![self.rpc.primary_url.clone()];
        urls.extend(self.rpc.fallback_urls.clone());
        urls.dedup();
        urls
    }

    pub fn is_api_key_valid(&self, api_key: &str) -> bool {
        if !self.api.authentication.enabled {
            return true;
        }
        
        self.api.authentication.valid_api_keys.contains(&api_key.to_string())
    }
}

// Configuration builder for programmatic setup
pub struct ConfigBuilder {
    config: MevBotConfig,
}

impl ConfigBuilder {
    pub fn new() -> Self {
        Self {
            config: MevBotConfig::default(),
        }
    }

    pub fn with_rpc_url(mut self, url: String) -> Self {
        self.config.rpc.primary_url = url;
        self
    }

    pub fn with_database_url(mut self, url: String) -> Self {
        self.config.database.url = url;
        self
    }

    pub fn with_api_port(mut self, port: u16) -> Self {
        self.config.api.port = port;
        self
    }

    pub fn with_metrics_port(mut self, port: u16) -> Self {
        self.config.metrics.port = port;
        self
    }

    pub fn with_log_level(mut self, level: String) -> Self {
        self.config.logging.level = level;
        self
    }

    pub fn with_api_keys(mut self, keys: Vec<String>) -> Self {
        self.config.api.authentication.valid_api_keys = keys;
        self
    }

    pub fn disable_api(mut self) -> Self {
        self.config.api.enabled = false;
        self
    }

    pub fn disable_metrics(mut self) -> Self {
        self.config.metrics.enabled = false;
        self
    }

    pub fn build(self) -> Result<MevBotConfig> {
        self.config.validate()?;
        Ok(self.config)
    }
}

// Environment-specific configurations
impl MevBotConfig {
    pub fn development() -> Self {
        let mut config = Self::default();
        config.database.url = "postgresql://localhost:5432/mev_bot_dev".to_string();
        config.logging.level = "debug".to_string();
        config.api.authentication.enabled = false;
        config.calculator.max_history_entries = 10_000; // Smaller for dev
        config.metrics.collection_interval_seconds = 30; // More frequent for dev
        config
    }

    pub fn testing() -> Self {
        let mut config = Self::default();
        config.database.url = "postgresql://localhost:5432/mev_bot_test".to_string();
        config.logging.level = "warn".to_string();
        config.api.enabled = false;
        config.metrics.enabled = false;
        config.calculator.max_history_entries = 1_000; // Small for tests
        config.database.batch_size = 10; // Small batches for tests
        config
    }

    pub fn production() -> Self {
        let mut config = Self::default();
        config.logging.level = "info".to_string();
        config.api.authentication.enabled = true;
        config.security.input_validation.validate_pubkeys = true;
        config.security.encryption.encrypt_sensitive_data = true;
        config.circuit_breaker.failure_threshold = 3; // More sensitive in prod
        config.rate_limiting.enabled = true;
        config.database.retention_hours = 720; // 30 days in production
        config
    }
}

// Configuration hot-reloading support
pub struct ConfigWatcher {
    current_config: Arc<tokio::sync::RwLock<MevBotConfig>>,
    file_path: std::path::PathBuf,
    last_modified: std::time::SystemTime,
}

impl ConfigWatcher {
    pub fn new<P: AsRef<Path>>(config_path: P) -> Result<Self> {
        let path = config_path.as_ref().to_path_buf();
        let config = MevBotConfig::from_file(&path)?;
        
        let metadata = std::fs::metadata(&path)
            .map_err(|e| anyhow!("Failed to get file metadata: {}", e))?;
        
        Ok(Self {
            current_config: Arc::new(tokio::sync::RwLock::new(config)),
            file_path: path,
            last_modified: metadata.modified()
                .map_err(|e| anyhow!("Failed to get modification time: {}", e))?,
        })
    }

    pub async fn get_config(&self) -> MevBotConfig {
        self.current_config.read().await.clone()
    }

    pub async fn check_for_updates(&mut self) -> Result<bool> {
        let metadata = std::fs::metadata(&self.file_path)
            .map_err(|e| anyhow!("Failed to get file metadata: {}", e))?;
        
        let modified = metadata.modified()
            .map_err(|e| anyhow!("Failed to get modification time: {}", e))?;

        if modified > self.last_modified {
            let new_config = MevBotConfig::from_file(&self.file_path)?;
            new_config.validate()?;
            
            {
                let mut config = self.current_config.write().await;
                *config = new_config;
            }
            
            self.last_modified = modified;
            return Ok(true);
        }

        Ok(false)
    }

    pub fn config_handle(&self) -> Arc<tokio::sync::RwLock<MevBotConfig>> {
        self.current_config.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_validation() {
        let config = MevBotConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_builder() {
        let config = ConfigBuilder::new()
            .with_rpc_url("https://custom-rpc.com".to_string())
            .with_api_port(8081)
            .with_log_level("debug".to_string())
            .build();
        
        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.rpc.primary_url, "https://custom-rpc.com");
        assert_eq!(config.api.port, 8081);
        assert_eq!(config.logging.level, "debug");
    }

    #[test]
    fn test_environment_configs() {
        let dev_config = MevBotConfig::development();
        assert_eq!(dev_config.logging.level, "debug");
        assert!(!dev_config.api.authentication.enabled);

        let prod_config = MevBotConfig::production();
        assert_eq!(prod_config.logging.level, "info");
        assert!(prod_config.api.authentication.enabled);
        assert!(prod_config.security.input_validation.validate_pubkeys);
    }

    #[test]
    fn test_invalid_config_validation() {
        let mut config = MevBotConfig::default();
        config.calculator.jito_min_tip_lamports = 1_000_000;
        config.calculator.max_tip_lamports = 500_000; // Less than min
        
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_api_key_validation() {
        let mut config = MevBotConfig::default();
        config.api.authentication.valid_api_keys = vec!["test_key_123".to_string()];
        
        assert!(config.is_api_key_valid("test_key_123"));
        assert!(!config.is_api_key_valid("invalid_key"));
    }
}
