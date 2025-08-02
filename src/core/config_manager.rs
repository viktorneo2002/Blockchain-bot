use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use std::fs;
use std::env;
use anyhow::{Result, Context};
use tokio::sync::watch;
use notify::{Watcher, RecursiveMode, watcher};
use std::sync::mpsc::channel;
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub network: NetworkConfig,
    pub trading: TradingConfig,
    pub risk: RiskConfig,
    pub fees: FeeConfig,
    pub monitoring: MonitoringConfig,
    pub execution: ExecutionConfig,
    pub mev: MevConfig,
    pub pools: PoolConfig,
    pub tokens: TokenConfig,
    pub advanced: AdvancedConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub primary_rpc: String,
    pub backup_rpcs: Vec<String>,
    pub ws_endpoint: String,
    pub commitment_level: String,
    pub max_retries: u32,
    pub retry_delay_ms: u64,
    pub request_timeout_ms: u64,
    pub preflight_commitment: String,
    pub skip_preflight: bool,
    pub encoding: String,
    pub max_concurrent_requests: usize,
    pub rate_limit_per_second: u32,
    pub connection_pool_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradingConfig {
    pub wallet_keypair_path: String,
    pub min_profit_bps: u64,
    pub max_position_size_sol: f64,
    pub max_slippage_bps: u64,
    pub enable_jito_bundles: bool,
    pub jito_tip_amount_lamports: u64,
    pub jito_block_engine_url: String,
    pub priority_fee_lamports: u64,
    pub compute_unit_limit: u32,
    pub compute_unit_price_micro_lamports: u64,
    pub max_accounts_per_tx: usize,
    pub enable_partial_fills: bool,
    pub min_order_size_sol: f64,
    pub use_versioned_transactions: bool,
    pub enable_cross_program_invocation: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskConfig {
    pub max_daily_loss_sol: f64,
    pub max_position_count: usize,
    pub max_exposure_sol: f64,
    pub stop_loss_percentage: f64,
    pub take_profit_percentage: f64,
    pub max_gas_per_tx_sol: f64,
    pub circuit_breaker_threshold: f64,
    pub cooldown_period_ms: u64,
    pub max_consecutive_failures: u32,
    pub enable_portfolio_hedging: bool,
    pub correlation_threshold: f64,
    pub var_confidence_level: f64,
    pub max_drawdown_percentage: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeeConfig {
    pub raydium_fee_bps: u64,
    pub orca_fee_bps: u64,
    pub serum_fee_bps: u64,
    pub jupiter_fee_bps: u64,
    pub base_priority_fee: u64,
    pub dynamic_fee_enabled: bool,
    pub fee_percentile_target: u8,
    pub max_priority_fee_lamports: u64,
    pub fee_cache_duration_ms: u64,
    pub custom_fee_markets: HashMap<String, u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    pub enable_metrics: bool,
    pub metrics_port: u16,
    pub log_level: String,
    pub enable_telegram_alerts: bool,
    pub telegram_bot_token: String,
    pub telegram_chat_id: String,
    pub alert_min_profit_sol: f64,
    pub enable_discord_webhook: bool,
    pub discord_webhook_url: String,
    pub performance_sample_size: usize,
    pub latency_threshold_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionConfig {
    pub parallel_execution: bool,
    pub max_parallel_trades: usize,
    pub bundle_size: usize,
    pub pre_execution_simulation: bool,
    pub post_execution_verification: bool,
    pub enable_flashloans: bool,
    pub flashloan_provider: String,
    pub routing_algorithm: String,
    pub path_finding_depth: usize,
    pub enable_sandwich_protection: bool,
    pub front_run_protection_delay_ms: u64,
    pub enable_atomic_arbitrage: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MevConfig {
    pub enable_mev_protection: bool,
    pub private_mempool_enabled: bool,
    pub block_builder_tips: HashMap<String, u64>,
    pub searcher_reputation_threshold: f64,
    pub bundle_auction_timeout_ms: u64,
    pub enable_backrunning: bool,
    pub frontrun_detection_enabled: bool,
    pub sandwich_detection_threshold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    pub min_liquidity_usd: f64,
    pub min_volume_24h_usd: f64,
    pub max_price_impact_percentage: f64,
    pub blacklisted_pools: Vec<String>,
    pub whitelisted_pools: Vec<String>,
    pub auto_discovery_enabled: bool,
    pub pool_refresh_interval_ms: u64,
    pub depth_levels_to_track: usize,
    pub enable_concentrated_liquidity: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenConfig {
    pub supported_tokens: Vec<TokenInfo>,
    pub stablecoin_addresses: Vec<String>,
    pub min_token_liquidity_usd: f64,
    pub token_metadata_cache_duration_ms: u64,
    pub enable_token_screening: bool,
    pub rugpull_detection_enabled: bool,
    pub honeypot_detection_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenInfo {
    pub symbol: String,
    pub address: String,
    pub decimals: u8,
    pub coingecko_id: Option<String>,
    pub max_position_size: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdvancedConfig {
    pub enable_machine_learning: bool,
    pub ml_model_path: String,
    pub feature_engineering_enabled: bool,
    pub market_making_enabled: bool,
    pub market_making_spread_bps: u64,
    pub inventory_target_ratio: f64,
    pub enable_statistical_arbitrage: bool,
    pub correlation_window_size: usize,
    pub zscore_entry_threshold: f64,
    pub zscore_exit_threshold: f64,
    pub enable_triangular_arbitrage: bool,
    pub gas_optimization_enabled: bool,
    pub transaction_compression_enabled: bool,
    pub enable_dark_pool_routing: bool,
    pub quote_stuffing_protection: bool,
}

pub struct ConfigManager {
    config: Arc<RwLock<Config>>,
    config_path: String,
    watcher_tx: Option<watch::Sender<Config>>,
    hot_reload_enabled: bool,
}

impl ConfigManager {
    pub fn new(config_path: String) -> Result<Self> {
        let config = Self::load_config(&config_path)?;
        
        Ok(ConfigManager {
            config: Arc::new(RwLock::new(config)),
            config_path,
            watcher_tx: None,
            hot_reload_enabled: false,
        })
    }

    pub fn with_hot_reload(config_path: String) -> Result<(Self, watch::Receiver<Config>)> {
        let config = Self::load_config(&config_path)?;
        let (tx, rx) = watch::channel(config.clone());
        
        let mut manager = ConfigManager {
            config: Arc::new(RwLock::new(config)),
            config_path: config_path.clone(),
            watcher_tx: Some(tx),
            hot_reload_enabled: true,
        };

        manager.start_file_watcher()?;
        Ok((manager, rx))
    }

    fn load_config(path: &str) -> Result<Config> {
        let config_str = fs::read_to_string(path)
            .context("Failed to read config file")?;
        
        let mut config: Config = toml::from_str(&config_str)
            .context("Failed to parse TOML config")?;
        
        config = Self::apply_env_overrides(config)?;
        Self::validate_config(&config)?;
        
        Ok(config)
    }

    fn apply_env_overrides(mut config: Config) -> Result<Config> {
        if let Ok(rpc) = env::var("SOLANA_RPC_URL") {
            config.network.primary_rpc = rpc;
        }
        
        if let Ok(keypair) = env::var("WALLET_KEYPAIR_PATH") {
            config.trading.wallet_keypair_path = keypair;
        }
        
        if let Ok(tip) = env::var("JITO_TIP_LAMPORTS") {
            config.trading.jito_tip_amount_lamports = tip.parse()?;
        }
        
        if let Ok(token) = env::var("TELEGRAM_BOT_TOKEN") {
            config.monitoring.telegram_bot_token = token;
        }
        
        if let Ok(chat_id) = env::var("TELEGRAM_CHAT_ID") {
            config.monitoring.telegram_chat_id = chat_id;
        }
        
        Ok(config)
    }

    fn validate_config(config: &Config) -> Result<()> {
        if config.network.primary_rpc.is_empty() {
            anyhow::bail!("Primary RPC URL cannot be empty");
        }
        
        if config.trading.wallet_keypair_path.is_empty() {
            anyhow::bail!("Wallet keypair path cannot be empty");
        }
        
        if config.trading.min_profit_bps == 0 {
            anyhow::bail!("Minimum profit must be greater than 0");
        }
        
        if config.trading.max_slippage_bps > 1000 {
            anyhow::bail!("Maximum slippage cannot exceed 10%");
        }
        
        if config.risk.max_daily_loss_sol <= 0.0 {
            anyhow::bail!("Max daily loss must be positive");
        }
        
        if config.fees.fee_percentile_target > 100 {
            anyhow::bail!("Fee percentile must be between 0 and 100");
        }
        
        if config.execution.bundle_size == 0 {
            anyhow::bail!("Bundle size must be at least 1");
        }
        
        if config.advanced.market_making_spread_bps == 0 && config.advanced.market_making_enabled {
            anyhow::bail!("Market making spread must be greater than 0 when enabled");
        }
        
        for token in &config.tokens.supported_tokens {
            Pubkey::from_str(&token.address)
                .context(format!("Invalid token address for {}", token.symbol))?;
        }
        
        Ok(())
    }

    fn start_file_watcher(&mut self) -> Result<()> {
        let (tx, rx) = channel();
        let mut watcher = watcher(tx, Duration::from_millis(500))?;
        watcher.watch(&self.config_path, RecursiveMode::NonRecursive)?;
        
        let config_path = self.config_path.clone();
        let config_arc = self.config.clone();
        let watcher_tx = self.watcher_tx.clone();
        
        std::thread::spawn(move || {
            loop {
                match rx.recv() {
                    Ok(event) => {
                        if let notify::DebouncedEvent::Write(_) = event {
                            if let Ok(new_config) = Self::load_config(&config_path) {
                                if let Ok(mut config) = config_arc.write() {
                                    *config = new_config.clone();
                                    if let Some(ref tx) = watcher_tx {
                                        let _ = tx.send(new_config);
                                    }
                                }
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        });
        
        Ok(())
    }

    pub fn get_config(&self) -> Config {
        self.config.read().map_err(|e| anyhow::anyhow!("Failed to acquire read lock on config: {}", e))?.clone()
    }

    pub fn update_config<F>(&self, updater: F) -> Result<()>
    where
        F: FnOnce(&mut Config),
    {
        let mut config = self.config.write().map_err(|e| anyhow::anyhow!("Failed to acquire write lock on config: {}", e))?;
        updater(&mut config);
        Self::validate_config(&config)?;
        
        if let Some(ref tx) = self.watcher_tx {
            let _ = tx.send(config.clone());
        }
        
        Ok(())
    }

    pub fn get_rpc_endpoints(&self) -> Vec<String> {
        let config = self.config.read().unwrap();
        let mut endpoints = vec![config.network.primary_rpc.clone()];
        endpoints.extend(config.network.backup_rpcs.clone());
        endpoints
    }

    pub fn should_use_jito(&self) -> bool {
        self.config.read().map_err(|e| anyhow::anyhow!("Failed to acquire read lock on config: {}", e))?.trading.enable_jito_bundles
    }

    pub fn get_priority_fee(&self) -> u64 {
        self.config.read().map_err(|e| anyhow::anyhow!("Failed to acquire read lock on config: {}", e))?.trading.priority_fee_lamports
    }

    pub fn get_compute_budget(&self) -> (u32, u64) {
        let config = self.config.read().unwrap();
        (config.trading.compute_unit_limit, config.trading.compute_unit_price_micro_lamports)
    }

    pub fn get_max_position_size(&self) -> f64 {
        self.config.read().map_err(|e| anyhow::anyhow!("Failed to acquire read lock on config: {}", e))?.trading.max_position_size_sol
    }

    pub fn get_min_profit_bps(&self) -> u64 {
        self.config.read().map_err(|e| anyhow::anyhow!("Failed to acquire read lock on config: {}", e))?.trading.min_profit_bps
    }

    pub fn get_risk_parameters(&self) -> RiskParameters {
        let config = self.config.read().unwrap();
        RiskParameters {
            max_daily_loss: config.risk.max_daily_loss_sol,
            max_position_count: config.risk.max_position_count,
            max_exposure: config.risk.max_exposure_sol,
            stop_loss_percentage: config.risk.stop_loss_percentage,
            take_profit_percentage: config.risk.take_profit_percentage,
            circuit_breaker_threshold: config.risk.circuit_breaker_threshold,
            max_consecutive_failures: config.risk.max_consecutive_failures,
        }
    }

    pub fn get_fee_for_dex(&self, dex_name: &str) -> u64 {
        let config = self.config.read().unwrap();
        match dex_name.to_lowercase().as_str() {
            "raydium" => config.fees.raydium_fee_bps,
            "orca" => config.fees.orca_fee_bps,
            "serum" => config.fees.serum_fee_bps,
            "jupiter" => config.fees.jupiter_fee_bps,
            _ => {
                config.fees.custom_fee_markets
                    .get(dex_name)
                    .copied()
                    .unwrap_or(30)
            }
        }
    }

    pub fn is_pool_whitelisted(&self, pool_address: &str) -> bool {
        let config = self.config.read().unwrap();
        if config.pools.whitelisted_pools.is_empty() {
            !config.pools.blacklisted_pools.contains(&pool_address.to_string())
        } else {
            config.pools.whitelisted_pools.contains(&pool_address.to_string())
        }
    }

    pub fn is_token_supported(&self, token_address: &str) -> bool {
        let config = self.config.read().unwrap();
        config.tokens.supported_tokens
            .iter()
            .any(|t| t.address == token_address)
    }

    pub fn get_token_info(&self, token_address: &str) -> Option<TokenInfo> {
        let config = self.config.read().unwrap();
        config.tokens.supported_tokens
            .iter()
            .find(|t| t.address == token_address)
            .cloned()
    }

    pub fn should_enable_mev_protection(&self) -> bool {
        self.config.read().map_err(|e| anyhow::anyhow!("Failed to acquire read lock on config: {}", e))?.mev.enable_mev_protection
    }

    pub fn get_jito_config(&self) -> JitoConfig {
        let config = self.config.read().unwrap();
        JitoConfig {
            enabled: config.trading.enable_jito_bundles,
            tip_amount: config.trading.jito_tip_amount_lamports,
            block_engine_url: config.trading.jito_block_engine_url.clone(),
        }
    }

    pub fn get_execution_params(&self) -> ExecutionParameters {
        let config = self.config.read().unwrap();
        ExecutionParameters {
            parallel_execution: config.execution.parallel_execution,
            max_parallel_trades: config.execution.max_parallel_trades,
            bundle_size: config.execution.bundle_size,
            pre_execution_simulation: config.execution.pre_execution_simulation,
            routing_algorithm: config.execution.routing_algorithm.clone(),
            path_finding_depth: config.execution.path_finding_depth,
        }
    }

    pub fn get_monitoring_config(&self) -> MonitoringSettings {
        let config = self.config.read().unwrap();
        MonitoringSettings {
            metrics_enabled: config.monitoring.enable_metrics,
            metrics_port: config.monitoring.metrics_port,
            telegram_enabled: config.monitoring.enable_telegram_alerts,
            alert_threshold: config.monitoring.alert_min_profit_sol,
            latency_threshold: config.monitoring.latency_threshold_ms,
        }
    }

    pub fn calculate_dynamic_priority_fee(&self, network_congestion: f64) -> u64 {
        let config = self.config.read().unwrap();
        if !config.fees.dynamic_fee_enabled {
            return config.fees.base_priority_fee;
        }

        let base = config.fees.base_priority_fee as f64;
        let multiplier = 1.0 + (network_congestion * 2.0);
        let dynamic_fee = (base * multiplier) as u64;
        
        dynamic_fee.min(config.fees.max_priority_fee_lamports)
    }

    pub fn get_advanced_settings(&self) -> AdvancedSettings {
        let config = self.config.read().unwrap();
        AdvancedSettings {
            ml_enabled: config.advanced.enable_machine_learning,
            market_making_enabled: config.advanced.market_making_enabled,
            statistical_arb_enabled: config.advanced.enable_statistical_arbitrage,
            triangular_arb_enabled: config.advanced.enable_triangular_arbitrage,
            gas_optimization: config.advanced.gas_optimization_enabled,
            dark_pool_routing: config.advanced.enable_dark_pool_routing,
        }
    }

    pub fn validate_trade_parameters(&self, size_sol: f64, expected_profit_bps: u64) -> Result<()> {
        let config = self.config.read().unwrap();
        
        if size_sol > config.trading.max_position_size_sol {
            anyhow::bail!("Trade size {} SOL exceeds maximum position size {} SOL", 
                size_sol, config.trading.max_position_size_sol);
        }
        
        if size_sol < config.trading.min_order_size_sol {
            anyhow::bail!("Trade size {} SOL below minimum order size {} SOL", 
                size_sol, config.trading.min_order_size_sol);
        }
        
        if expected_profit_bps < config.trading.min_profit_bps {
            anyhow::bail!("Expected profit {} bps below minimum threshold {} bps", 
                expected_profit_bps, config.trading.min_profit_bps);
        }
        
        Ok(())
    }

    pub fn get_network_timeout(&self) -> Duration {
        let config = self.config.read().unwrap();
        Duration::from_millis(config.network.request_timeout_ms)
    }

    pub fn should_retry(&self, attempt: u32) -> bool {
        let config = self.config.read().unwrap();
        attempt < config.network.max_retries
    }

    pub fn get_retry_delay(&self, attempt: u32) -> Duration {
        let config = self.config.read().unwrap();
        let base_delay = config.network.retry_delay_ms;
        let exponential_delay = base_delay * (2_u64.pow(attempt.min(5)));
        Duration::from_millis(exponential_delay.min(30000))
    }

    pub fn update_pool_blacklist(&self, pool_address: String, add: bool) -> Result<()> {
        self.update_config(|config| {
            if add {
                if !config.pools.blacklisted_pools.contains(&pool_address) {
                    config.pools.blacklisted_pools.push(pool_address);
                }
            } else {
                config.pools.blacklisted_pools.retain(|p| p != &pool_address);
            }
        })
    }

    pub fn update_priority_fee(&self, new_fee: u64) -> Result<()> {
        self.update_config(|config| {
            config.trading.priority_fee_lamports = new_fee;
        })
    }

    pub fn enable_circuit_breaker(&self) -> Result<()> {
        self.update_config(|config| {
            config.risk.circuit_breaker_threshold = 0.0;
        })
    }

    pub fn get_pool_criteria(&self) -> PoolCriteria {
        let config = self.config.read().unwrap();
        PoolCriteria {
            min_liquidity_usd: config.pools.min_liquidity_usd,
            min_volume_24h_usd: config.pools.min_volume_24h_usd,
            max_price_impact: config.pools.max_price_impact_percentage,
            auto_discovery: config.pools.auto_discovery_enabled,
        }
    }

    pub fn persist_config(&self) -> Result<()> {
        let config = self.config.read().unwrap();
        let toml_string = toml::to_string_pretty(&*config)?;
        fs::write(&self.config_path, toml_string)?;
        Ok(())
    }

    pub fn export_config(&self, path: &str) -> Result<()> {
        let config = self.config.read().unwrap();
        let json_string = serde_json::to_string_pretty(&*config)?;
        fs::write(path, json_string)?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RiskParameters {
    pub max_daily_loss: f64,
    pub max_position_count: usize,
    pub max_exposure: f64,
    pub stop_loss_percentage: f64,
    pub take_profit_percentage: f64,
    pub circuit_breaker_threshold: f64,
    pub max_consecutive_failures: u32,
}

#[derive(Debug, Clone)]
pub struct JitoConfig {
    pub enabled: bool,
    pub tip_amount: u64,
    pub block_engine_url: String,
}

#[derive(Debug, Clone)]
pub struct ExecutionParameters {
    pub parallel_execution: bool,
    pub max_parallel_trades: usize,
    pub bundle_size: usize,
    pub pre_execution_simulation: bool,
    pub routing_algorithm: String,
    pub path_finding_depth: usize,
}

#[derive(Debug, Clone)]
pub struct MonitoringSettings {
    pub metrics_enabled: bool,
    pub metrics_port: u16,
    pub telegram_enabled: bool,
    pub alert_threshold: f64,
    pub latency_threshold: u64,
}

#[derive(Debug, Clone)]
pub struct AdvancedSettings {
    pub ml_enabled: bool,
    pub market_making_enabled: bool,
    pub statistical_arb_enabled: bool,
    pub triangular_arb_enabled: bool,
    pub gas_optimization: bool,
    pub dark_pool_routing: bool,
}

#[derive(Debug, Clone)]
pub struct PoolCriteria {
    pub min_liquidity_usd: f64,
    pub min_volume_24h_usd: f64,
    pub max_price_impact: f64,
    pub auto_discovery: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            network: NetworkConfig {
                primary_rpc: "https://api.mainnet-beta.solana.com".to_string(),
                backup_rpcs: vec![
                    "https://solana-api.projectserum.com".to_string(),
                    "https://rpc.ankr.com/solana".to_string(),
                ],
                ws_endpoint: "wss://api.mainnet-beta.solana.com".to_string(),
                commitment_level: "confirmed".to_string(),
                max_retries: 3,
                retry_delay_ms: 100,
                request_timeout_ms: 5000,
                preflight_commitment: "processed".to_string(),
                skip_preflight: true,
                encoding: "base64".to_string(),
                max_concurrent_requests: 100,
                rate_limit_per_second: 50,
                connection_pool_size: 10,
            },
            trading: TradingConfig {
                wallet_keypair_path: "./wallet.json".to_string(),
                min_profit_bps: 30,
                max_position_size_sol: 100.0,
                max_slippage_bps: 50,
                enable_jito_bundles: true,
                jito_tip_amount_lamports: 10000,
                jito_block_engine_url: "https://mainnet.block-engine.jito.wtf".to_string(),
                priority_fee_lamports: 50000,
                compute_unit_limit: 1400000,
                compute_unit_price_micro_lamports: 100,
                max_accounts_per_tx: 64,
                enable_partial_fills: true,
                min_order_size_sol: 0.1,
                use_versioned_transactions: true,
                enable_cross_program_invocation: true,
            },
            risk: RiskConfig {
                max_daily_loss_sol: 50.0,
                max_position_count: 10,
                max_exposure_sol: 500.0,
                stop_loss_percentage: 2.0,
                take_profit_percentage: 5.0,
                max_gas_per_tx_sol: 0.01,
                circuit_breaker_threshold: 10.0,
                cooldown_period_ms: 5000,
                max_consecutive_failures: 5,
                enable_portfolio_hedging: true,
                correlation_threshold: 0.7,
                var_confidence_level: 0.95,
                max_drawdown_percentage: 15.0,
            },
            fees: FeeConfig {
                raydium_fee_bps: 25,
                orca_fee_bps: 30,
                serum_fee_bps: 40,
                jupiter_fee_bps: 20,
                base_priority_fee: 50000,
                dynamic_fee_enabled: true,
                fee_percentile_target: 75,
                max_priority_fee_lamports: 1000000,
                fee_cache_duration_ms: 1000,
                custom_fee_markets: HashMap::new(),
            },
monitoring: MonitoringConfig {
                enable_metrics: true,
                metrics_port: 9090,
                log_level: "info".to_string(),
                enable_telegram_alerts: false,
                telegram_bot_token: String::new(),
                telegram_chat_id: String::new(),
                alert_min_profit_sol: 1.0,
                enable_discord_webhook: false,
                discord_webhook_url: String::new(),
                performance_sample_size: 1000,
                latency_threshold_ms: 100,
            },
            execution: ExecutionConfig {
                parallel_execution: true,
                max_parallel_trades: 5,
                bundle_size: 4,
                pre_execution_simulation: true,
                post_execution_verification: true,
                enable_flashloans: true,
                flashloan_provider: "solend".to_string(),
                routing_algorithm: "smart_router_v2".to_string(),
                path_finding_depth: 3,
                enable_sandwich_protection: true,
                front_run_protection_delay_ms: 50,
                enable_atomic_arbitrage: true,
            },
            mev: MevConfig {
                enable_mev_protection: true,
                private_mempool_enabled: true,
                block_builder_tips: HashMap::from([
                    ("jito".to_string(), 10000),
                    ("flashbots".to_string(), 15000),
                ]),
                searcher_reputation_threshold: 0.8,
                bundle_auction_timeout_ms: 200,
                enable_backrunning: true,
                frontrun_detection_enabled: true,
                sandwich_detection_threshold: 0.85,
            },
            pools: PoolConfig {
                min_liquidity_usd: 50000.0,
                min_volume_24h_usd: 100000.0,
                max_price_impact_percentage: 1.0,
                blacklisted_pools: vec![],
                whitelisted_pools: vec![],
                auto_discovery_enabled: true,
                pool_refresh_interval_ms: 60000,
                depth_levels_to_track: 10,
                enable_concentrated_liquidity: true,
            },
            tokens: TokenConfig {
                supported_tokens: vec![
                    TokenInfo {
                        symbol: "SOL".to_string(),
                        address: "So11111111111111111111111111111111111111112".to_string(),
                        decimals: 9,
                        coingecko_id: Some("solana".to_string()),
                        max_position_size: Some(1000.0),
                    },
                    TokenInfo {
                        symbol: "USDC".to_string(),
                        address: "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string(),
                        decimals: 6,
                        coingecko_id: Some("usd-coin".to_string()),
                        max_position_size: Some(100000.0),
                    },
                    TokenInfo {
                        symbol: "USDT".to_string(),
                        address: "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB".to_string(),
                        decimals: 6,
                        coingecko_id: Some("tether".to_string()),
                        max_position_size: Some(100000.0),
                    },
                ],
                stablecoin_addresses: vec![
                    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string(),
                    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB".to_string(),
                ],
                min_token_liquidity_usd: 10000.0,
                token_metadata_cache_duration_ms: 3600000,
                enable_token_screening: true,
                rugpull_detection_enabled: true,
                honeypot_detection_enabled: true,
            },
            advanced: AdvancedConfig {
                enable_machine_learning: false,
                ml_model_path: "./models/price_prediction.onnx".to_string(),
                feature_engineering_enabled: true,
                market_making_enabled: false,
                market_making_spread_bps: 10,
                inventory_target_ratio: 0.5,
                enable_statistical_arbitrage: true,
                correlation_window_size: 100,
                zscore_entry_threshold: 2.0,
                zscore_exit_threshold: 0.5,
                enable_triangular_arbitrage: true,
                gas_optimization_enabled: true,
                transaction_compression_enabled: true,
                enable_dark_pool_routing: false,
                quote_stuffing_protection: true,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;
    use std::io::Write;

    #[test]
    fn test_config_loading() {
        let config = Config::default();
        let mut temp_file = NamedTempFile::new().unwrap();
        let toml_content = toml::to_string(&config).unwrap();
        write!(temp_file, "{}", toml_content).unwrap();
        
        let manager = ConfigManager::new(temp_file.path().to_str().unwrap().to_string()).unwrap();
        let loaded_config = manager.get_config();
        
        assert_eq!(loaded_config.trading.min_profit_bps, 30);
        assert_eq!(loaded_config.network.primary_rpc, "https://api.mainnet-beta.solana.com");
    }

    #[test]
    fn test_config_validation() {
        let mut config = Config::default();
        config.trading.min_profit_bps = 0;
        
        let result = ConfigManager::validate_config(&config);
        assert!(result.is_err());
    }

    #[test]
    fn test_env_overrides() {
        env::set_var("SOLANA_RPC_URL", "https://custom.rpc.com");
        let config = Config::default();
        let overridden = ConfigManager::apply_env_overrides(config).unwrap();
        
        assert_eq!(overridden.network.primary_rpc, "https://custom.rpc.com");
        env::remove_var("SOLANA_RPC_URL");
    }

    #[test]
    fn test_dynamic_priority_fee() {
        let config = Config::default();
        let mut temp_file = NamedTempFile::new().unwrap();
        let toml_content = toml::to_string(&config).unwrap();
        write!(temp_file, "{}", toml_content).unwrap();
        
        let manager = ConfigManager::new(temp_file.path().to_str().unwrap().to_string()).unwrap();
        
        let fee_low = manager.calculate_dynamic_priority_fee(0.1);
        let fee_high = manager.calculate_dynamic_priority_fee(0.9);
        
        assert!(fee_high > fee_low);
        assert!(fee_high <= config.fees.max_priority_fee_lamports);
    }

    #[test]
    fn test_trade_validation() {
        let config = Config::default();
        let mut temp_file = NamedTempFile::new().unwrap();
        let toml_content = toml::to_string(&config).unwrap();
        write!(temp_file, "{}", toml_content).unwrap();
        
        let manager = ConfigManager::new(temp_file.path().to_str().unwrap().to_string()).unwrap();
        
        assert!(manager.validate_trade_parameters(50.0, 50).is_ok());
        assert!(manager.validate_trade_parameters(200.0, 50).is_err());
        assert!(manager.validate_trade_parameters(50.0, 10).is_err());
    }
}
