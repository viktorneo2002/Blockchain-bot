use anyhow::Result;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::Keypair,
};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

mod config;
mod executor;
mod pool_cache;
mod pool_parser;
mod rpc_pool;
mod safety;
mod scanner;
mod simulator;

use config::Config;
use executor::Executor;
use pool_cache::{PoolCache, PoolState, Protocol};
use pool_parser::{parse_orca_whirlpool_account, parse_raydium_amm_account};
use rpc_pool::RpcPool;
use safety::{SafetyMonitor, SafetyConfig};
use scanner::{ArbitrageOpportunity, Scanner};

/// Main arbitrageur application
pub struct Arbitrageur {
    config: Arc<Config>,
    rpc_pool: Arc<RpcPool>,
    pool_cache: Arc<PoolCache>,
    safety_monitor: Arc<SafetyMonitor>,
    wallet: Arc<Keypair>,
}

impl Arbitrageur {
    /// Create new arbitrageur instance
    pub async fn new(config_path: Option<&str>) -> Result<Self> {
        // Load configuration
        let config = if let Some(path) = config_path {
            Config::from_file(path)?
        } else {
            Config::default()
        };
        let config = Arc::new(config);
        
        // Initialize RPC pool
        let rpc_pool = Arc::new(RpcPool::new(
            config.rpc_endpoints.clone(),
            config.rpc_concurrency_limit,
        ));
        
        // Initialize pool cache
        let pool_cache = Arc::new(PoolCache::new());
        
        // Initialize safety monitor
        let safety_config = SafetyConfig {
            max_loss_per_window: config.max_loss_per_window,
            max_consecutive_failures: config.max_consecutive_failures,
            max_gas_without_profit: config.max_gas_without_profit,
            min_tx_interval_ms: config.min_tx_interval_ms,
            max_tx_per_minute: config.max_tx_per_minute,
            max_slippage_bps: config.max_slippage_bps,
            min_pool_liquidity: config.min_liquidity_threshold,
            max_position_size: config.max_position_size,
            circuit_breaker_enabled: config.circuit_breaker_enabled,
            auto_recovery_enabled: config.auto_recovery_enabled,
            cooldown_period_seconds: config.cooldown_period_seconds,
            ..Default::default()
        };
        let safety_monitor = Arc::new(SafetyMonitor::new(safety_config));
        
        // Load wallet
        let wallet = Arc::new(load_wallet(&config.wallet_path)?);
        
        Ok(Self {
            config,
            rpc_pool,
            pool_cache,
            safety_monitor,
            wallet,
        })
    }
    
    /// Run the arbitrageur
    pub async fn run(self) -> Result<()> {
        info!("Starting DEX Fee Tier Arbitrageur");
        info!("Wallet: {}", self.wallet.pubkey());
        info!("RPC endpoints: {:?}", self.config.rpc_endpoints);
        
        // Create channel for opportunities
        let (opportunity_tx, opportunity_rx) = mpsc::channel::<ArbitrageOpportunity>(100);
        
        // Start executor
        let executor = Executor::new(
            self.config.clone(),
            self.rpc_pool.clone(),
            self.pool_cache.clone(),
            self.wallet.clone(),
            opportunity_rx,
        );
        
        let executor_handle = tokio::spawn(async move {
            if let Err(e) = executor.run().await {
                error!("Executor failed: {}", e);
            }
        });
        
        // Start scanner
        let scanner = Scanner::new(
            self.config.clone(),
            self.pool_cache.clone(),
            opportunity_tx,
        );
        
        let scanner_handle = tokio::spawn(async move {
            if let Err(e) = scanner.run().await {
                error!("Scanner failed: {}", e);
            }
        });
        
        // Start pool updater
        let updater_handle = tokio::spawn({
            let arbitrageur = Arc::new(self);
            async move {
                if let Err(e) = arbitrageur.run_pool_updater().await {
                    error!("Pool updater failed: {}", e);
                }
            }
        });
        
        // Wait for tasks
        tokio::try_join!(executor_handle, scanner_handle, updater_handle)?;
        
        Ok(())
    }
    
    /// Continuously update pool states
    async fn run_pool_updater(&self) -> Result<()> {
        info!("Starting pool state updater");
        
        let mut interval = tokio::time::interval(
            tokio::time::Duration::from_millis(self.config.pool_refresh_interval_ms)
        );
        
        loop {
            interval.tick().await;
            
            if let Err(e) = self.update_pools().await {
                warn!("Failed to update pools: {}", e);
            }
        }
    }
    
    /// Update all pools or stale pools
    async fn update_pools(&self) -> Result<()> {
        // Get pools to update
        let pools_to_update = if self.pool_cache.get_all().is_empty() {
            // Initial load: fetch all configured pools
            info!("Initial pool load");
            self.fetch_initial_pools().await?
        } else {
            // Update stale pools
            self.pool_cache.get_stale_pools(self.config.max_pool_age_ms)
        };
        
        if pools_to_update.is_empty() {
            return Ok(());
        }
        
        info!("Updating {} pools", pools_to_update.len());
        
        // Fetch pool accounts in batches
        let batch_size = 100;
        for chunk in pools_to_update.chunks(batch_size) {
            let addresses: Vec<Pubkey> = chunk.iter().map(|p| p.address).collect();
            
            match self.fetch_and_parse_pools(&addresses).await {
                Ok(updated_pools) => {
                    self.pool_cache.batch_update(updated_pools);
                }
                Err(e) => {
                    warn!("Failed to update pool batch: {}", e);
                }
            }
        }
        
        // Log cache stats
        let stats = self.pool_cache.stats();
        info!(
            "Pool cache: {} total, {} Orca, {} Raydium, {} unique pairs",
            stats.total_pools, stats.orca_count, stats.raydium_count, stats.unique_token_pairs
        );
        
        Ok(())
    }
    
    /// Fetch initial pool list
    async fn fetch_initial_pools(&self) -> Result<Vec<Arc<PoolState>>> {
        let mut initial_pools = Vec::new();
        
        // Add configured Orca pools
        for pool_address in &self.config.orca_pool_addresses {
            initial_pools.push(Arc::new(PoolState {
                address: *pool_address,
                protocol: Protocol::OrcaWhirlpool,
                token_a: Pubkey::default(),
                token_b: Pubkey::default(),
                liquidity: 0,
                last_update_slot: 0,
                last_update_timestamp: 0,
                fee_rate: 0,
                sqrt_price: 0,
                tick_current: 0,
                tick_spacing: 0,
                reserve_a: 0,
                reserve_b: 0,
                trade_fee_numerator: 0,
                trade_fee_denominator: 1,
                token_vault_a: Pubkey::default(),
                token_vault_b: Pubkey::default(),
                open_orders: None,
                market: None,
            }));
        }
        
        // Add configured Raydium pools
        for pool_address in &self.config.raydium_pool_addresses {
            initial_pools.push(Arc::new(PoolState {
                address: *pool_address,
                protocol: Protocol::RaydiumV4,
                token_a: Pubkey::default(),
                token_b: Pubkey::default(),
                liquidity: 0,
                last_update_slot: 0,
                last_update_timestamp: 0,
                fee_rate: 0,
                sqrt_price: 0,
                tick_current: 0,
                tick_spacing: 0,
                reserve_a: 0,
                reserve_b: 0,
                trade_fee_numerator: 0,
                trade_fee_denominator: 1,
                token_vault_a: Pubkey::default(),
                token_vault_b: Pubkey::default(),
                open_orders: None,
                market: None,
            }));
        }
        
        Ok(initial_pools)
    }
    
    /// Fetch and parse pool accounts
    async fn fetch_and_parse_pools(&self, addresses: &[Pubkey]) -> Result<Vec<PoolState>> {
        let rpc = self.rpc_pool.get_client().await;
        
        // Fetch accounts
        let accounts = rpc.get_multiple_accounts(addresses).await?;
        
        let mut parsed_pools = Vec::new();
        
        for (i, account_opt) in accounts.iter().enumerate() {
            if let Some(account) = account_opt {
                let address = addresses[i];
                
                // Determine protocol by program owner
                let pool_state = if account.owner == config::ORCA_WHIRLPOOL_PROGRAM {
                    // Parse Orca Whirlpool
                    match parse_orca_whirlpool_account(&account.data) {
                        Ok(parsed) => {
                            let slot = rpc.get_slot().await.unwrap_or(0);
                            
                            PoolState {
                                address,
                                protocol: Protocol::OrcaWhirlpool,
                                token_a: parsed.token_mint_a,
                                token_b: parsed.token_mint_b,
                                liquidity: parsed.liquidity,
                                last_update_slot: slot,
                                last_update_timestamp: PoolState::current_timestamp(),
                                fee_rate: parsed.fee_rate,
                                sqrt_price: parsed.sqrt_price,
                                tick_current: parsed.tick_current,
                                tick_spacing: parsed.tick_spacing as i32,
                                reserve_a: 0, // Not used for Orca
                                reserve_b: 0,
                                trade_fee_numerator: 0,
                                trade_fee_denominator: 1,
                                token_vault_a: parsed.token_vault_a,
                                token_vault_b: parsed.token_vault_b,
                                open_orders: None,
                                market: None,
                            }
                        }
                        Err(e) => {
                            warn!("Failed to parse Orca pool {}: {}", address, e);
                            continue;
                        }
                    }
                } else if account.owner == config::RAYDIUM_V4_PROGRAM {
                    // Parse Raydium AMM
                    match parse_raydium_amm_account(&account.data) {
                        Ok(parsed) => {
                            let slot = rpc.get_slot().await.unwrap_or(0);
                            
                            PoolState {
                                address,
                                protocol: Protocol::RaydiumV4,
                                token_a: parsed.coin_mint,
                                token_b: parsed.pc_mint,
                                liquidity: parsed.coin_vault_amount + parsed.pc_vault_amount,
                                last_update_slot: slot,
                                last_update_timestamp: PoolState::current_timestamp(),
                                fee_rate: 0, // Not used for Raydium
                                sqrt_price: 0,
                                tick_current: 0,
                                tick_spacing: 0,
                                reserve_a: parsed.coin_vault_amount as u128,
                                reserve_b: parsed.pc_vault_amount as u128,
                                trade_fee_numerator: parsed.trade_fee_numerator,
                                trade_fee_denominator: parsed.trade_fee_denominator,
                                token_vault_a: parsed.coin_vault,
                                token_vault_b: parsed.pc_vault,
                                open_orders: Some(parsed.open_orders),
                                market: Some(parsed.market),
                            }
                        }
                        Err(e) => {
                            warn!("Failed to parse Raydium pool {}: {}", address, e);
                            continue;
                        }
                    }
                } else {
                    warn!("Unknown pool owner for {}: {}", address, account.owner);
                    continue;
                };
                
                parsed_pools.push(pool_state);
            }
        }
        
        Ok(parsed_pools)
    }
}

/// Load wallet from file
fn load_wallet(path: &str) -> Result<Keypair> {
    let wallet_bytes = std::fs::read(path)?;
    let wallet = Keypair::from_bytes(&wallet_bytes)?;
    Ok(wallet)
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("dex_fee_tier_arbitrageur=info".parse()?)
        )
        .init();
    
    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let config_path = args.get(1).map(|s| s.as_str());
    
    // Create and run arbitrageur
    let arbitrageur = Arbitrageur::new(config_path).await?;
    arbitrageur.run().await?;
    
    Ok(())
}
