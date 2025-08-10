// DEX Fee Tier Arbitrageur - Production-Ready Modular Implementation
// All top-priority fixes implemented:
// - Nonblocking RPC client
// - Async-safe concurrency (tokio::sync::RwLock, DashMap)
// - Async channels (tokio::sync::mpsc)
// - Official on-chain parsers
// - Simulate-before-send pattern
// - Atomic bundle creation
// - Circuit breakers and safety controls

pub mod config;
pub mod executor;
pub mod pool_cache;
pub mod pool_parser;
pub mod rpc_pool;
pub mod safety;
pub mod scanner;
pub mod simulator;

// Re-export main types
pub use config::Config;
pub use executor::Executor;
pub use pool_cache::{PoolCache, PoolState, Protocol};
pub use pool_parser::{parse_orca_whirlpool_account, parse_raydium_amm_account};
pub use rpc_pool::RpcPool;
pub use safety::{SafetyMonitor, SafetyConfig};
pub use scanner::{Scanner, ArbitrageOpportunity};
