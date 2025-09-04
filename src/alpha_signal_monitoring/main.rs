//! AlphaDecayRateMonitor: Robust and Extensible Alpha Decay Profiler for Solana
//!
//! - Modular DEX/Protocol Ingestion
//! - Zero-copy deserialization for performance
//! - Jito, Yellowstone, or WebSocket mempool monitoring
//! - Robust error handling and mainnet deployment readiness

mod config;
mod dex;
mod decay;
mod ingest;
mod logging;
mod security;
mod utils;

use crate::ingest::IngestEngine;
use crate::logging::setup_logging;
use anyhow::{Context, Result};
use dotenv::dotenv;
use std::{env, sync::Arc};
use tokio::sync::RwLock;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    setup_logging();
    // Load config (TOML or ENV)
    let config = Arc::new(config::Config::from_env()?);

    // Initialize on-chain data ingestion (mempool/block stream via Geyser, Jito, Helius...)
    let ingest_engine = IngestEngine::new(config.clone()).await?;

    // Register DEX/protocol parsers
    let mut dex_registry = dex::DexRegistry::default();
    dex_registry.register("raydium", Box::new(dex::raydium::RaydiumParser));
    dex_registry.register("orca", Box::new(dex::orca::OrcaParser));
    dex_registry.register("jupiter", Box::new(dex::jupiter::JupiterParser));
    // ... dynamically load more protocol parsers as required

    // Shared decay tracker
    let decay_tracker = Arc::new(RwLock::new(decay::DecayTable::new()));

    // Spawn ingestion tasks
    ingest_engine
        .run_stream(
            dex_registry,
            decay_tracker.clone(),
            config.clone(),
        )
        .await?;

    // Optionally: serve API/metrics for live dashboard
    // ServePrometheus::serve(decay_tracker).await?;
    Ok(())
}
