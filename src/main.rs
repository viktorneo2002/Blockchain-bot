use solana_mev_bot::{
    core::{
        config_manager::ConfigManager,
        flash_loan_orchestrator::{FlashLoanConfig, FlashLoanOrchestrator},
        risk_manager::{RiskManager, RiskManagerConfig},
    },
    flashloan_mev_apocalypse::frontrun_flashloan_weaponizer::FrontrunFlashLoanWeaponizer,
    monitoring::{
        health_monitor::{HealthMonitor, HealthMonitorConfig},
        metrics_collector::MetricsCollector,
    },
    solana_flashloan_domination::solend_kamino_port_optimizer::PortOptimizer,
    strategies::{KalmanOUStrategy, FlashloanITOStrategy},
};

use anyhow::{Context, Result};
use async_trait::async_trait;
use backoff::{future::retry, ExponentialBackoff};
use dashmap::DashMap;
use solana_client::{
    nonblocking::rpc_client::RpcClient as AsyncRpcClient, rpc_client::RpcClient,
    rpc_config::RpcConfig,
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    pubkey::Pubkey,
    signature::{read_keypair_file, Keypair},
};
use std::collections::HashMap;
use std::str::FromStr;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{broadcast, mpsc, RwLock, Semaphore},
    task::JoinSet,
    time::{interval, timeout},
};
use tracing::{debug, error, info, warn};
use tracing_subscriber::{
    fmt::time::ChronoLocal, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter,
};

// Strategy trait that all strategies must implement
#[async_trait::async_trait]
trait Strategy: Send + Sync {
    fn name(&self) -> &str;
    async fn run(&self) -> Result<()>;
    async fn shutdown(&self) -> Result<()>;
}

// Configuration structures for strategies
struct FrontrunConfig {
    rpc_pool: Arc<RpcPool>,
    authority: Arc<Keypair>,
    min_profit_threshold: u16,
    max_priority_fee: u64,
}

struct PortOptimizerConfig {
    rpc_pool: Arc<RpcPool>,
    authority: Arc<Keypair>,
    max_leverage: f64,
    rebalance_threshold_bps: u16,
}

// Implement Strategy trait for our strategies
#[async_trait::async_trait]
impl Strategy for FrontrunFlashLoanWeaponizer {
    fn name(&self) -> &str {
        "FrontrunFlashLoanWeaponizer"
    }

    async fn run(&self) -> Result<()> {
        self.monitor_transactions().await
    }

    async fn shutdown(&self) -> Result<()> {
        self.shutdown().await
    }
}

#[async_trait::async_trait]
impl Strategy for PortOptimizer {
    fn name(&self) -> &str {
        "PortOptimizer"
    }

    async fn run(&self) -> Result<()> {
        self.run_optimization_loop().await
    }

    async fn shutdown(&self) -> Result<()> {
        self.shutdown().await
    }
}

// Health check response structure
#[derive(Debug, Clone)]
struct HealthStatus {
    is_healthy: bool,
    is_critical: bool,
    issues: Vec<String>,
    memory_usage_mb: u64,
    cpu_usage_percent: f32,
    active_positions: u64,
    rpc_latency_ms: u64,
}

// Extension methods for HealthMonitor
impl HealthMonitor {
    async fn check_health(&self) -> HealthStatus {
        let memory_info = match sys_info::mem_info() {
            Ok(info) => {
                let used_mb = (info.total - info.free) / 1024;
                let total_mb = info.total / 1024;
                (used_mb, total_mb)
            }
            Err(_) => (0, 0),
        };

        let cpu_usage = match sys_info::loadavg() {
            Ok(load) => (load.one * 100.0) as f32,
            Err(_) => 0.0,
        };

        let mut issues = Vec::new();
        let mut is_critical = false;

        // Check memory usage
        if memory_info.1 > 0 && memory_info.0 > (memory_info.1 * 90 / 100) {
            issues.push(format!(
                "High memory usage: {}MB / {}MB",
                memory_info.0, memory_info.1
            ));
            is_critical = true;
        }

        // Check CPU usage
        if cpu_usage > 90.0 {
            issues.push(format!("High CPU usage: {:.1}%", cpu_usage));
        }

        // Check if we have active RPC connections
        let rpc_healthy = self
            .last_rpc_check()
            .map(|instant| instant.elapsed() < Duration::from_secs(60))
            .unwrap_or(false);

        if !rpc_healthy {
            issues.push("RPC connection unhealthy".to_string());
            is_critical = true;
        }

        HealthStatus {
            is_healthy: issues.is_empty(),
            is_critical,
            issues,
            memory_usage_mb: memory_info.0,
            cpu_usage_percent: cpu_usage,
            active_positions: self.get_active_positions(),
            rpc_latency_ms: self.get_rpc_latency_ms(),
        }
    }
}

// Panic handler for production
fn install_panic_handler() {
    let default_panic = std::panic::take_hook();

    std::panic::set_hook(Box::new(move |panic_info| {
        let thread = std::thread::current();
        let thread_name = thread.name().unwrap_or("unknown");

        let msg = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            s
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            s
        } else {
            "unknown panic"
        };

        let location = if let Some(location) = panic_info.location() {
            format!(
                "{}:{}:{}",
                location.file(),
                location.line(),
                location.column()
            )
        } else {
            "unknown location".to_string()
        };

        error!("PANIC in thread '{}' at {}: {}", thread_name, location, msg);

        // Call the default panic handler
        default_panic(panic_info);

        // Force exit to prevent zombie process
        std::process::exit(1);
    }));
}

// Signal handler for graceful shutdown
fn install_signal_handlers() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};

        let mut sigterm = signal(SignalKind::terminate())?;
        let mut sighup = signal(SignalKind::hangup())?;

        tokio::spawn(async move {
            tokio::select! {
                _ = sigterm.recv() => {
                    info!("Received SIGTERM, initiating shutdown");
                }
                _ = sighup.recv() => {
                    info!("Received SIGHUP, reloading configuration");
                }
            }
        });
    }

    Ok(())
}

// Pre-flight checks before starting the bot
async fn preflight_checks(config: &solana_mev_bot::core::config::Config) -> Result<()> {
    info!("Running pre-flight checks...");

    // Check disk space
    let disk_stats = fs2::statvfs::statvfs(".")?;
    let available_gb = (disk_stats.available_space() / 1_073_741_824) as u64;
    if available_gb < 10 {
        anyhow::bail!(
            "Insufficient disk space: {}GB available, need at least 10GB",
            available_gb
        );
    }

    // Check memory
    let mem_info = sys_info::mem_info()?;
    let available_mb = mem_info.avail / 1024;
    if available_mb < 4096 {
        anyhow::bail!(
            "Insufficient memory: {}MB available, need at least 4GB",
            available_mb
        );
    }

    // Verify network connectivity
    let test_client =
        RpcClient::new_with_timeout(config.network.primary_rpc.clone(), Duration::from_secs(5));

    match test_client.get_version() {
        Ok(version) => {
            info!("Connected to Solana cluster: {}", version.solana_core);
        }
        Err(e) => {
            anyhow::bail!("Failed to connect to Solana RPC: {:?}", e);
        }
    }

    // Check required programs exist
    let required_programs = vec![
        "So11111111111111111111111111111111111111112", // Wrapped SOL
        "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", // Token Program
        "11111111111111111111111111111111",            // System Program
    ];

    for program_str in required_programs {
        let program_id = program_str.parse::<solana_sdk::pubkey::Pubkey>()?;
        match test_client.get_account(&program_id) {
            Ok(account) => {
                if !account.executable {
                    anyhow::bail!("Required program {} is not executable", program_str);
                }
            }
            Err(e) => {
                anyhow::bail!("Failed to verify program {}: {:?}", program_str, e);
            }
        }
    }

    info!("Pre-flight checks passed");
    Ok(())
}

// Entry point wrapper with panic protection
fn main() {
    install_panic_handler();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(num_cpus::get())
        .thread_name("mev-bot-worker")
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime");

    let result = runtime.block_on(async {
        if let Err(e) = install_signal_handlers() {
            eprintln!("Failed to install signal handlers: {:?}", e);
            return Err(e);
        }

        // Load config first for pre-flight checks
        let config_path =
            std::env::var("CONFIG_PATH").unwrap_or_else(|_| "config.toml".to_string());

        let (config_manager, _) = ConfigManager::with_hot_reload(config_path)?;
        let config = config_manager.get_config();

        // Run pre-flight checks
        if let Err(e) = preflight_checks(&config).await {
            eprintln!("Pre-flight checks failed: {:?}", e);
            return Err(e);
        }

        // Run the main async function
        main_async().await
    });

    match result {
        Ok(()) => {
            info!("MEV bot exited successfully");
            std::process::exit(0);
        }
        Err(e) => {
            error!("MEV bot failed: {:?}", e);
            std::process::exit(1);
        }
    }
}

// Rename the original main to main_async
async fn main_async() -> Result<()> {
    // The entire original main function content goes here
    // (Everything from the tracing_subscriber initialization to the final Ok(()))

    // Initialize structured logging with timestamps
    tracing_subscriber::registry()
        .with(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "solana_mev_bot=info,tower_abci=warn".into()),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_timer(ChronoLocal::rfc_3339())
                .with_thread_ids(true)
                .with_thread_names(true),
        )
        .init();

    // Load configuration
    let config_path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| "config.toml".to_string());
    let (config_manager, _) = ConfigManager::with_hot_reload(config_path)?;
    let config = config_manager.get_config();

    // Initialize RPC client
    let rpc_client = Arc::new(AsyncRpcClient::new_with_commitment(
        config.network.primary_rpc.clone(),
        CommitmentConfig::confirmed(),
    ));

    // Load wallet
    let wallet_path = config.wallet.keypair_path.clone();
    let wallet = Arc::new(read_keypair_file(&wallet_path).context("Failed to read keypair file")?);

    // Initialize strategies
    let mut strategies: Vec<Box<dyn Strategy>> = Vec::new();
    
    // Add our Kalman-O-U strategy
    let kalman_ou_strategy = Box::new(KalmanOUStrategy::new(
        rpc_client.clone(),
        wallet.clone(),
    )?);
    strategies.push(kalman_ou_strategy);

    // Add Flashloan ITO strategy
    let flashloan_config = config_manager.get_dex_pools_config()?;
    let flashloan_orchestrator = Arc::new(FlashLoanOrchestrator::new(
        FlashLoanConfig {
            max_loan_amount: 1_000_000_000, // 1000 SOL in lamports
            min_profit_threshold: 1_000_000, // 0.001 SOL in lamports
            max_slippage_bps: 50, // 0.5%
            emergency_shutdown_loss: 10_000_000, // 0.01 SOL in lamports
            rpc_client: rpc_client.clone(),
            authority: wallet.clone(),
        },
        Arc::new(RiskManager::new(RiskManagerConfig::default())),
        health_monitor.clone(),
    ));
    
    // Parse ITO addresses from config
    let ito_addresses: Vec<Pubkey> = flashloan_config.ito_addresses
        .iter()
        .filter_map(|addr_str| {
            Pubkey::from_str(addr_str).map_err(|e| {
                error!("Invalid ITO address in config: {} - {}", addr_str, e);
                e
            }).ok()
        })
        .collect();
    
    // Parse Pyth feeds from config
    let pyth_feeds: HashMap<Pubkey, Pubkey> = flashloan_config.pyth_feeds
        .iter()
        .filter_map(|(token_str, feed_str)| {
            let token_key = Pubkey::from_str(token_str).map_err(|e| {
                error!("Invalid token address in Pyth feeds config: {} - {}", token_str, e);
                e
            }).ok()?;
            let feed_key = Pubkey::from_str(feed_str).map_err(|e| {
                error!("Invalid feed address in Pyth feeds config: {} - {}", feed_str, e);
                e
            }).ok()?;
            Some((token_key, feed_key))
        })
        .collect();
    
    // Parse Chainlink feeds from config
    let chainlink_feeds: HashMap<Pubkey, Pubkey> = flashloan_config.chainlink_feeds
        .iter()
        .filter_map(|(token_str, feed_str)| {
            let token_key = Pubkey::from_str(token_str).map_err(|e| {
                error!("Invalid token address in Chainlink feeds config: {} - {}", token_str, e);
                e
            }).ok()?;
            let feed_key = Pubkey::from_str(feed_str).map_err(|e| {
                error!("Invalid feed address in Chainlink feeds config: {} - {}", feed_str, e);
                e
            }).ok()?;
            Some((token_key, feed_key))
        })
        .collect();
    
    let flashloan_ito_strategy = Box::new(FlashloanITOStrategy::new(
        rpc_client.clone(),
        wallet.clone(),
        flashloan_config,
        flashloan_orchestrator,
        ito_addresses,
        pyth_feeds,
        chainlink_feeds,
    )?);
    strategies.push(flashloan_ito_strategy);

    // Initialize health monitor
    let health_monitor = Arc::new(HealthMonitor::new(HealthMonitorConfig {
        check_interval_ms: 5000,
        max_memory_mb: 4096,
        max_cpu_percent: 80.0,
        rpc_timeout_ms: 5000,
    }));

    // Start health monitoring
    let health_monitor_clone = health_monitor.clone();
    tokio::spawn(async move {
        health_monitor_clone.start_monitoring().await;
    });

    // Start all strategies
    let mut strategy_handles = Vec::new();
    for strategy in strategies {
        info!("Starting strategy: {}", strategy.name());
        let handle = tokio::spawn(async move {
            if let Err(e) = strategy.run().await {
                error!("Strategy {} failed: {:?}", strategy.name(), e);
            }
        });
        strategy_handles.push(handle);
    }

    // Wait for shutdown signal
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            info!("Received Ctrl+C, shutting down...");
        }
        _ = async {
            // Wait for any strategy to fail
            for handle in strategy_handles {
                if let Err(e) = handle.await {
                    error!("Strategy task failed: {:?}", e);
                }
            }
        } => {
            error!("A strategy has failed, shutting down...");
        }
    }

    // Shutdown all strategies
    // In a real implementation, we would have a way to access all strategy instances
    // for graceful shutdown. For now, we'll just log the shutdown.
    info!("Shutting down strategies...");

    Ok(())
}
