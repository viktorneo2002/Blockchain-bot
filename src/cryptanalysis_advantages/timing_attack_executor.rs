use anyhow::{Context, Result};
use dashmap::DashMap;
use solana_client::{
    nonblocking::{
        rpc_client::RpcClient,
        tpu_client::TpuClient,
        pubsub_client::PubsubClient,
    },
    rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig, RpcProgramAccountsConfig},
    rpc_filter::{RpcFilterType, Memcmp, MemcmpEncodedBytes, MemcmpEncoding},
    rpc_response::RpcSimulateTransactionResult,
    rpc_request::RpcRequest,
    client_error::Result as ClientResult,
};
use solana_account_decoder::parse_token::UiTokenAmount;
use solana_sdk::signature::Signature;
use solana_sdk::transaction::VersionedTransaction;
use solana_tpu_client::tpu_connection::TpuConnection;
use jito_protos::{
    bundle::Bundle,
    searcher::{
        searcher_service_client::SearcherServiceClient,
        SendBundleRequest,
    },
};
use tonic::Request;
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    message::{v0::{Message as MessageV0, MessageAddressTableLookup}, Message, MessageHeader},
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    system_instruction,
    sysvar::recent_blockhash,
    transaction::{Transaction, TransactionError, VersionedTransaction, VersionedMessage},
    address_lookup_table::state::LookupTableMeta,
    address_lookup_table_account::AddressLookupTableAccount,
};
use spl_token;
use solana_transaction_status::UiTransactionEncoding;
use std::{
    collections::{HashMap, VecDeque, BTreeMap},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    cmp::Reverse,
    iter,
};

/// Represents a fee level with its corresponding priority fee and compute unit price
#[derive(Debug, Clone, Copy)]
struct FeeLevel {
    percentile: f64,  // Target percentile (e.g., 0.5 for p50)
    compute_unit_price: u64,  // In microlamports per CU
}

/// Tracks historical timing data for slot-relative execution
#[derive(Debug, Default)]
struct SlotTimingStats {
    // Slot number to timing data mapping
    slot_timings: BTreeMap<u64, Vec<f64>>,
    // Maximum number of slots to keep in history
    max_history: usize,
}

impl SlotTimingStats {
    fn new(max_history: usize) -> Self {
        Self {
            slot_timings: BTreeMap::new(),
            max_history,
        }
    }

    fn record_timing(&mut self, slot: u64, timing_ms: f64) {
        self.slot_timings
            .entry(slot)
            .or_default()
            .push(timing_ms);
        
        // Trim old slots if we exceed max history
        if self.slot_timings.len() > self.max_history {
            if let Some(&oldest_slot) = self.slot_timings.keys().next() {
                self.slot_timings.remove(&oldest_slot);
            }
        }
    }

    fn get_percentile(&self, slot: u64, percentile: f64) -> Option<f64> {
        self.slot_timings
            .get(&slot)
            .and_then(|timings| {
                if timings.is_empty() {
                    return None;
                }
                let mut sorted = timings.clone();
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let idx = (percentile * (sorted.len() - 1) as f64).round() as usize;
                sorted.get(idx).copied()
            })
    }
}

/// Represents a dynamic compute unit price
#[derive(Debug, Clone, Copy)]
struct DynamicComputeUnitPrice {
    base_price: u64,  // Base price in microlamports per CU
    multiplier: f64,  // Multiplier to apply based on network conditions
}

/// Represents a dynamic fee level
#[derive(Debug, Clone, Copy)]
struct DynamicFeeLevel {
    percentile: f64,  // Target percentile (e.g., 0.5 for p50)
    compute_unit_price: DynamicComputeUnitPrice,  // Dynamic compute unit price
}

/// Enum for fee management strategies
#[derive(Debug, Clone, Copy)]
enum FeeManagementStrategy {
    Fixed,  // Use a fixed fee level
    Dynamic,  // Use a dynamic fee level based on network conditions
}

/// Enum for compute unit management strategies
#[derive(Debug, Clone, Copy)]
enum ComputeUnitManagementStrategy {
    Fixed,  // Use a fixed compute unit limit
    Dynamic,  // Use a dynamic compute unit limit based on network conditions
}

use thiserror::Error;
use tokio::{
    sync::{Mutex, RwLock, Semaphore, SemaphorePermit},
    time::{interval, sleep, timeout, Sleep},
};
use tracing::{debug, error, info, instrument, trace, warn};

#[derive(Error, Debug)]
pub enum TimingError {
    #[error("RPC error: {0}")]
    RpcError(String),
    
    #[error("Transaction simulation failed: {0}")]
    SimulationError(String),
    
    #[error("Rate limit exceeded: {0} trades/min")]
    RateLimitExceeded,
    
    #[error("Fee limit exceeded: {0} > {1} lamports")]
    FeeLimitExceeded(u64, u64),
    
    #[error("Position size exceeded: {0} > {1} lamports")]
    PositionSizeExceeded(u64, u64),
    
    #[error("Daily budget exceeded")]
    DailyBudgetExceeded,
    
    #[error("Program not whitelisted: {0}")]
    ProgramNotWhitelisted(Pubkey),
    #[error("Transaction execution timed out")]
    Timeout,
    #[error("Transaction confirmation failed: {0}")]
    ConfirmationFailed(String),
    #[error("Resource limit exceeded: {0}")]
    ResourceLimitExceeded(String),
}

type TimingResult<T> = std::result::Result<T, TimingError>;

const MAX_RETRIES: u8 = 5;
const SLOT_DURATION_MS: u64 = 400;
const NETWORK_LATENCY_BUFFER_MS: u64 = 50;
const MAX_CONCURRENT_EXECUTIONS: usize = 128;
const TIMING_PRECISION_NS: u64 = 100_000;
const MAX_TIMING_HISTORY: usize = 1000;
const PRIORITY_FEE_PERCENTILE: f64 = 0.95;
const MIN_LAMPORTS_PER_SIGNATURE: u64 = 5000;
const MAX_COMPUTE_UNITS: u32 = 1_400_000;

#[derive(Debug, Clone)]
pub struct TimingConfig {
    pub pre_execution_offset_ms: u64,
    pub execution_window_ms: u64,
    pub retry_delay_ms: u64,
    pub max_concurrent_attempts: usize,
    pub priority_fee_lamports: u64,
    pub compute_unit_limit: u32,
    pub use_adaptive_timing: bool,
    pub network_latency_compensation: bool,
    pub safety_config: SafetyConfig,
}

impl Default for TimingConfig {
    fn default() -> Self {
        Self {
            pre_execution_offset_ms: 10,
            execution_window_ms: 100,
            retry_delay_ms: 20,
            max_concurrent_attempts: 3,
            priority_fee_lamports: 100_000,
            compute_unit_limit: 400_000,
            use_adaptive_timing: true,
            network_latency_compensation: true,
            safety_config: SafetyConfig::default(),
        }
    }
}

#[derive(Debug, Clone)]
struct TimingMetrics {
    slot_number: u64,
    execution_time_ms: u64,
    network_latency_ms: u64,
    success: bool,
    priority_fee: u64,
    timestamp: u64,
}

#[derive(Debug, Clone)]
struct SlotTiming {
    slot_start_time: Instant,
    estimated_end_time: Instant,
    leader_schedule: Option<Pubkey>,
    network_conditions: NetworkConditions,
}

#[derive(Debug, Clone, Default)]
struct NetworkConditions {
    average_latency_ms: f64,
    latency_variance: f64,
    packet_loss_rate: f64,
    congestion_factor: f64,
}

/// High-performance executor for timing-sensitive Solana transactions
///
/// Handles transaction timing, retries, and confirmation with adaptive
/// strategies based on network conditions.
pub struct BroadcastPaths {
    rpc_client: Arc<RpcClient>,
    tpu_client: Option<Arc<TpuClient>>,
    jito_client: Option<Arc<SearcherServiceClient<tonic::transport::Channel>>>,
}

/// High-performance executor for timing-sensitive Solana transactions
///
/// Handles transaction timing, retries, and confirmation with adaptive
/// strategies based on network conditions.
pub struct LogsSubscription {
    pubsub_client: Option<PubsubClient>,
    signature_tracker: Arc<Mutex<HashMap<Signature, oneshot::Sender<()>>>>,
}

#[derive(Debug, Clone)]
pub struct SafetyConfig {
    pub max_fee_per_tx: u64,          // Maximum fee in lamports per transaction
    pub max_daily_loss: f64,          // Maximum allowed daily loss in SOL
    pub max_position_size: u64,       // Maximum position size in lamports
    pub max_trades_per_minute: u32,   // Rate limiting
    pub max_concurrent_trades: u32,   // Maximum concurrent trades
    pub whitelisted_programs: Vec<Pubkey>, // Allowed program IDs
}

impl Default for SafetyConfig {
    fn default() -> Self {
        Self {
            max_fee_per_tx: 100_000,
            max_daily_loss: 100.0,
            max_position_size: 100_000_000,
            max_trades_per_minute: 10,
            max_concurrent_trades: 5,
            whitelisted_programs: vec![],
        }
    }
}

#[derive(Debug, Default)]
pub struct PnLState {
    pub total_profit: f64,            // In SOL
    pub total_loss: f64,              // In SOL
    pub daily_budget: f64,            // In SOL
    pub daily_spent: f64,             // In SOL
    pub last_reset: Instant,          // Last budget reset time
    pub trade_count: u64,             // Total trades executed
    pub recent_trades: VecDeque<Instant>, // For rate limiting
    pub current_positions: HashMap<Pubkey, u64>, // Token mint -> position size
}

impl PnLState {
    pub fn new(daily_budget: f64) -> Self {
        Self {
            total_profit: 0.0,
            total_loss: 0.0,
            daily_budget,
            daily_spent: 0.0,
            last_reset: Instant::now(),
            trade_count: 0,
            recent_trades: VecDeque::with_capacity(1000),
            current_positions: HashMap::new(),
        }
    }
    
    /// Check if we can execute a new trade with the given parameters
    pub fn can_execute_trade(
        &mut self, 
        safety: &SafetyConfig,
        tx_fee: u64,
        position_size: u64,
        token_mint: Option<&Pubkey>,
    ) -> Result<(), TimingError> {
        let now = Instant::now();
        
        // Clean up old trades (older than 1 minute)
        let one_min_ago = now - Duration::from_secs(60);
        while let Some(&time) = self.recent_trades.front() {
            if time >= one_min_ago {
                break;
            }
            self.recent_trades.pop_front();
        }
        
        // Check rate limits
        if self.recent_trades.len() >= safety.max_trades_per_minute as usize {
            return Err(TimingError::RateLimitExceeded);
        }
        
        // Check fee limits
        if tx_fee > safety.max_fee_per_tx {
            return Err(TimingError::FeeLimitExceeded(tx_fee, safety.max_fee_per_tx));
        }
        
        // Check position size
        if position_size > safety.max_position_size {
            return Err(TimingError::PositionSizeExceeded(position_size, safety.max_position_size));
        }
        
        // Check daily budget
        self.check_daily_budget()?;
        
        // Update trade tracking
        self.recent_trades.push_back(now);
        self.trade_count += 1;
        
        Ok(())
    }
    
    /// Update PnL after a trade
    pub fn update_pnl(&mut self, pnl: f64, fee: u64, is_profit: bool) {
        let fee_sol = lamports_to_sol(fee);
        
        if is_profit {
            self.total_profit += pnl - fee_sol;
        } else {
            self.total_loss += pnl + fee_sol;
            self.daily_spent += pnl + fee_sol;
        }
    }
    
    /// Check and reset daily budget if needed
    fn check_daily_budget(&mut self) -> Result<(), TimingError> {
        // Reset daily budget if more than 24 hours have passed
        if self.last_reset.elapsed() > Duration::from_secs(24 * 60 * 60) {
            self.daily_spent = 0.0;
            self.last_reset = Instant::now();
        }
        
        if self.daily_spent >= self.daily_budget {
            return Err(TimingError::DailyBudgetExceeded);
        }
        
        Ok(())
    }
}

/// Convert lamports to SOL
fn lamports_to_sol(lamports: u64) -> f64 {
    lamports as f64 / 1_000_000_000.0
}

pub struct TimingAttackExecutor {
    broadcast_paths: BroadcastPaths,
    logs_subscription: Arc<LogsSubscription>,
    keypair: Arc<Keypair>,
    config: Arc<RwLock<TimingConfig>>,
    timing_history: Arc<Mutex<VecDeque<TimingMetrics>>>,
    pnl_state: Arc<Mutex<PnLState>>,
    slot_tracker: Arc<RwLock<HashMap<u64, SlotTiming>>>,
    execution_semaphore: Arc<Semaphore>,
    active_executions: Arc<DashMap<Signature, ExecutionState>>,
    network_monitor: Arc<NetworkMonitor>,
    shutdown: Arc<AtomicBool>,
    current_slot: Arc<AtomicU64>,
    metrics: Arc<Metrics>,
}

/// Thread-safe metrics collector for the executor
#[derive(Default, Debug)]
struct Metrics {
    total_executions: AtomicU64,
    successful_executions: AtomicU64,
    failed_executions: AtomicU64,
    average_execution_time: AtomicU64,
    last_error: RwLock<Option<String>>,
}

#[derive(Debug, Clone)]
struct ExecutionState {
    signature: Signature,
    start_time: Instant,
    attempts: u8,
    status: ExecutionStatus,
}

#[derive(Debug, Clone, PartialEq)]
enum ExecutionStatus {
    Pending,
    Simulating,
    Executing,
    Confirmed,
    Failed(String),
}

struct NetworkMonitor {
    latency_samples: Arc<Mutex<VecDeque<u64>>>,
    conditions: Arc<RwLock<NetworkConditions>>,
}

impl NetworkMonitor {
    fn new() -> Self {
        Self {
            latency_samples: Arc::new(Mutex::new(VecDeque::with_capacity(100))),
            conditions: Arc::new(RwLock::new(NetworkConditions::default())),
        }
    }

    async fn record_latency(&self, latency_ms: u64) {
        let mut samples = self.latency_samples.lock().await;
        if samples.len() >= 100 {
            samples.pop_front();
        }
        samples.push_back(latency_ms);

        if samples.len() >= 10 {
            let avg = samples.iter().sum::<u64>() as f64 / samples.len() as f64;
            let variance = samples.iter()
                .map(|&x| (x as f64 - avg).powi(2))
                .sum::<f64>() / samples.len() as f64;

            let mut conditions = self.conditions.write().await;
            conditions.average_latency_ms = avg;
            conditions.latency_variance = variance.sqrt();
            conditions.congestion_factor = (avg / 50.0).min(2.0);
        }
    }

    async fn get_conditions(&self) -> NetworkConditions {
        self.conditions.read().await.clone()
    }
}

impl TimingAttackExecutor {
    /// Create a new TimingAttackExecutor with the specified configuration
    ///
    /// # Arguments
    /// * `rpc_url` - RPC endpoint URL
    /// * `ws_url` - WebSocket URL for pubsub (for logs subscription)
    /// * `keypair` - Keypair for signing transactions
    /// * `config` - Timing and safety configuration
    ///
    /// # Returns
    /// A new instance of TimingAttackExecutor
    ///
    /// # Example
    /// ```no_run
    /// use solana_sdk::signer::keypair::Keypair;
    /// use timing_attack_executor::{TimingAttackExecutor, TimingConfig};
    ///
    /// let keypair = Keypair::new();
    /// let config = TimingConfig::default();
    /// let executor = TimingAttackExecutor::new(
    ///     "https://api.mainnet-beta.solana.com".to_string(),
    ///     "wss://api.mainnet-beta.solana.com".to_string(),
    ///     keypair,
    ///     config
    /// ).await?;
    /// ```
    /// Simulate transaction to estimate compute units with 10% margin
    #[instrument(skip(self, instructions))]
    async fn estimate_compute_units(
        &self,
        instructions: &[Instruction],
    ) -> TimingResult<u64> {
        // Create a simulation request with the latest blockhash
        let (recent_blockhash, _) = self.get_fresh_blockhash().await?;
        
        // Add compute budget instructions for simulation
        let mut sim_instructions = instructions.to_vec();
        sim_instructions.insert(0, ComputeBudgetInstruction::set_compute_unit_limit(1_400_000)); // Max allowed
        sim_instructions.insert(1, ComputeBudgetInstruction::set_compute_unit_price(0)); // No priority fee for simulation

        // Build and sign the transaction for simulation
        let message = Message::new(&sim_instructions, Some(&self.keypair.pubkey()));
        let tx = Transaction::new_signed_with_payer(
            &sim_instructions,
            Some(&self.keypair.pubkey()),
            &[&*self.keypair],
            recent_blockhash,
        );

        // Run simulation
        let simulation = self.broadcast_paths.rpc_client
            .simulate_transaction_with_config(
                &tx,
                RpcSimulateTransactionConfig {
                    sig_verify: false,
                    replace_recent_blockhash: true,
                    commitment: Some(CommitmentConfig::confirmed()),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| TimingError::SimulationError(e.to_string()))?;

        // Extract compute units used from simulation
        let compute_units_used = simulation
            .value
            .units_consumed
            .ok_or_else(|| TimingError::SimulationError("No compute units consumed in simulation".into()))?;

        // Add 10% margin and round up to nearest 1000
        let compute_units = ((compute_units_used as f64 * 1.1).ceil() as u64 + 999) / 1000 * 1000;
        
        // Ensure we don't exceed the maximum allowed compute units
        Ok(compute_units.min(1_400_000))
    }
    
    /// Get optimal compute unit price based on network conditions and desired percentile
    #[instrument(skip(self))]
    async fn get_optimal_compute_unit_price(&self, target_percentile: f64) -> TimingResult<u64> {
        // Get current slot and leader schedule
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        let leader_schedule = self.get_leader_schedule(current_slot).await?;
        
        // Get fee history (simplified - in practice, you'd query an RPC for this)
        let fee_history = self.get_fee_history(100).await?; // Last 100 slots
        
        // Calculate base fee from history
        let base_fee = fee_history.iter()
            .map(|(_, fee)| *fee)
            .filter(|&f| f > 0)
            .min()
            .unwrap_or(1);
            
        // Apply multiplier based on target percentile and network conditions
        let multiplier = match target_percentile {
            p if p < 0.5 => 1.0,  // p50 or lower
            p if p < 0.75 => 1.5,  // p75
            p if p < 0.9 => 2.0,   // p90
            _ => 3.0,              // p95 or higher
        };
        
        // Calculate final price with some randomness to avoid front-running
        let mut rng = rand::thread_rng();
        let random_factor = 0.9 + rng.gen_range(0.0..0.2); // 90-110%
        let price = (base_fee as f64 * multiplier * random_factor).ceil() as u64;
        
        debug!("Optimal compute unit price: {} (base: {}, multiplier: {:.2})", 
               price, base_fee, multiplier);
        
        Ok(price)
    }
    
    /// Get leader schedule for a given slot
    #[instrument(skip(self))]
    async fn get_leader_schedule(&self, slot: u64) -> TimingResult<Pubkey> {
        // In a real implementation, this would query the RPC for the leader schedule
        // For now, return a dummy value
        Ok(Pubkey::new_unique())
    }
    
    /// Get fee history for recent slots
    #[instrument(skip(self))]
    async fn get_fee_history(&self, num_slots: usize) -> TimingResult<Vec<(u64, u64)>> {
        // In a real implementation, this would query the RPC for fee history
        // For now, return dummy data
        Ok(vec![(0, 1); num_slots])
    }
    pub async fn new(
        rpc_url: String,
        keypair: Keypair,
        config: TimingConfig,
    ) -> Result<Self> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url,
            CommitmentConfig::confirmed(),
        ));

        let executor = Self {
            broadcast_paths: BroadcastPaths {
                rpc_client,
                tpu_client: None,
                jito_client: None,
            },
            logs_subscription: Arc::new(LogsSubscription {
                pubsub_client: None,
                signature_tracker: Arc::new(Mutex::new(HashMap::new())),
            }),
            keypair: Arc::new(keypair),
            config: Arc::new(RwLock::new(config)),
            timing_history: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_TIMING_HISTORY))),
            pnl_state: Arc::new(Mutex::new(PnLState::new(config.safety_config.max_daily_loss))),
            slot_tracker: Arc::new(RwLock::new(HashMap::new())),
            execution_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_EXECUTIONS)),
            active_executions: Arc::new(DashMap::new()),
            network_monitor: Arc::new(NetworkMonitor::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
            current_slot: Arc::new(AtomicU64::new(0)),
            metrics: Arc::new(Metrics::default()),
        };

        executor.start_monitoring().await;
        Ok(executor)
    }

    async fn start_monitoring(&self) {
        let rpc_client = self.broadcast_paths.rpc_client.clone();
        let slot_tracker = self.slot_tracker.clone();
        let network_monitor = self.network_monitor.clone();
        let current_slot = self.current_slot.clone();
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(50));

            while !shutdown.load(Ordering::Relaxed) {
                interval.tick().await;

                let start = Instant::now();
                if let Ok(slot) = rpc_client.get_slot().await {
                    let latency = start.elapsed().as_millis() as u64;
                    network_monitor.record_latency(latency).await;

                    let old_slot = current_slot.swap(slot, Ordering::SeqCst);
                    if slot > old_slot {
                        let mut tracker = slot_tracker.write().await;
                        tracker.insert(slot, SlotTiming {
                            slot_start_time: Instant::now(),
                            estimated_end_time: Instant::now() + Duration::from_millis(SLOT_DURATION_MS),
                            leader_schedule: None,
                            network_conditions: network_monitor.get_conditions().await,
                        });

                        tracker.retain(|&k, _| k > slot.saturating_sub(10));
                    }
                }
            }
        });
    }

    /// Execute a transaction with precise timing control
    ///
    /// # Arguments
    /// * `instructions` - List of instructions to include in the transaction
    /// * `target_slot_offset` - Target slot offset for execution
    /// * `priority_fee` - Optional priority fee in lamports
    /// * `retry_config` - Custom retry configuration
    #[instrument(skip(self, instructions))]
    pub async fn execute_timed_transaction(
        &self,
        instructions: Vec<Instruction>,
        target_slot_offset: u64,
        priority_fee: Option<u64>,
        estimated_pnl: f64,  // Estimated PnL in SOL
        position_size: u64,  // Position size in lamports
        token_mint: Option<Pubkey>,  // Optional token mint for position tracking
    ) -> TimingResult<Signature> {
        // 1. Safety checks
        let fee = self.estimate_fee(&instructions, priority_fee).await?;
        
        {
            let mut pnl_state = self.pnl_state.lock().await;
            pnl_state.can_execute_trade(
                &self.config.read().await.safety_config,
                fee,
                position_size,
                token_mint.as_ref(),
            )?;
        }
        
        // 2. Execute the trade
        let start_time = Instant::now();
        let result = self.execute_timed_transaction_inner(
            instructions,
            target_slot_offset,
            priority_fee,
        ).await;
        
        // 3. Update PnL
        if let Ok(signature) = &result {
            let is_profit = estimated_pnl >= 0.0;
            let mut pnl_state = self.pnl_state.lock().await;
            pnl_state.update_pnl(estimated_pnl.abs(), fee, is_profit);
            
            // Update position tracking if token mint is provided
            if let Some(mint) = token_mint {
                let entry = pnl_state.current_positions.entry(*mint).or_insert(0);
                *entry = entry.saturating_add_signed(
                    if is_profit { position_size as i64 } else { -(position_size as i64) }
                );
            }
        }
        
        result
    }
    
    /// Internal method for executing a transaction with timing
    async fn execute_timed_transaction_inner(
        &self,
        instructions: Vec<Instruction>,
        target_slot_offset: u64,
        priority_fee: Option<u64>,
    ) -> TimingResult<Signature> {
        let _permit = self.execution_semaphore
            .acquire()
            .await
            .map_err(|_| TimingError::ResourceLimitExceeded("Max concurrent executions reached".into()))?;
        
        let retry_config = RetryConfig {

        let retry_config = retry_config.unwrap_or_else(|| RetryConfig {
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(5),
            ..Default::default()
        });

        let mut attempt = 0;
        let mut backoff = retry_config.initial_backoff;

        while attempt <= retry_config.max_retries {
            match self.try_execute_timed(
                &instructions,
                target_slot_offset,
                priority_fee,
                attempt,
            )
            .await
            {
                Ok(signature) => {
                    self.metrics.successful_executions.fetch_add(1, Ordering::Relaxed);
                    return Ok(signature);
                }
                Err(e) => {
                    if attempt == retry_config.max_retries {
                        self.metrics.failed_executions.fetch_add(1, Ordering::Relaxed);
                        return Err(e);
                    }

                    warn!("Attempt {}/{} failed: {}", attempt + 1, retry_config.max_retries, e);

                    // Use exponential backoff with jitter
                    let jitter = rand::random::<u64>() % 100; // 0-100ms jitter
                    let sleep_time = backoff + Duration::from_millis(jitter);

                    tokio::time::sleep(sleep_time).await;
                    backoff = (backoff * 2).min(retry_config.max_backoff);
                    attempt += 1;
                }
            }
        }

        Err(TimingError::ResourceLimitExceeded("Max retries exceeded".into()))
    }

    /// Internal method to attempt a single execution
    async fn try_execute_timed(
        &self,
        instructions: &[Instruction],
        target_slot_offset: u64,
        priority_fee: Option<u64>,
        attempt: u32,
    ) -> TimingResult<Signature> {
        // 1. Simulate first to catch errors early
        let simulation = self.simulate_transaction(instructions.to_vec())
            .await
            .map_err(|e| TimingError::SimulationFailed(e.to_string()))?;

        // 2. Calculate optimal timing based on network conditions
        let target_slot = self.current_slot.load(Ordering::Relaxed) + target_slot_offset;
        let execution_time = self.calculate_optimal_timing(target_slot).await?;

        // 3. Wait for the optimal moment
        self.wait_for_optimal_moment(execution_time).await?;

        // 4. Send the transaction with precise timing
        let signature = self.send_transaction_with_timing(instructions, attempt, priority_fee).await?;

        // 5. Monitor confirmation with timeout
        let confirmed = self.confirm_transaction_fast(signature).await?;

        if !confirmed {
            return Err(TimingError::ConfirmationFailed("Transaction not confirmed".into()));
        }

        // 6. Record metrics for adaptive timing
        self.record_timing_success(signature, attempt as u8).await;

        Ok(signature)
    }

    /// Send a transaction with precise timing, dynamic compute units, and optimal fees
    #[instrument(skip(self, instructions))]
    async fn send_transaction_with_timing(
        &self,
        instructions: &[Instruction],
        attempt: u8,
        priority_fee: Option<u64>,
    ) -> TimingResult<Signature> {
        // Estimate compute units with 10% margin
        let compute_units = self.estimate_compute_units(instructions).await?;
        
        // Get optimal compute unit price based on target percentile
        let target_percentile = match attempt {
            0 => 0.5,  // Start with p50
            1 => 0.75, // Then try p75
            _ => 0.9,  // Finally p90
        };
        
        let compute_unit_price = self.get_optimal_compute_unit_price(target_percentile).await?;
        let start_time = Instant::now();
        let mut modified_instructions = instructions.to_vec();

        // Add unique identifier to prevent duplicate signatures
        let unique_id = self.generate_unique_id().await?;
        let memo_ix = self.create_unique_memo_instruction(unique_id);
        modified_instructions.push(memo_ix);

        // Add compute budget instructions with dynamic values
        modified_instructions.insert(0, ComputeBudgetInstruction::set_compute_unit_price(compute_unit_price));
        modified_instructions.insert(0, ComputeBudgetInstruction::set_compute_unit_limit(compute_units));
        
        // Add priority fee if specified (in addition to dynamic pricing)
        if let Some(fee) = priority_fee {
            modified_instructions.insert(0, ComputeBudgetInstruction::set_compute_unit_price(fee));
        }

        // Get fresh blockhash for each attempt to avoid reuse
        let (blockhash, _) = self.get_fresh_blockhash().await?;

        // Create address lookup tables for frequently used accounts
        let lookup_tables = self.get_or_create_alt(&modified_instructions).await?;

        // Build versioned message with address lookup tables
        let message = MessageV0::new(
            modified_instructions,
            Some(&self.keypair.pubkey()),
            &[&*self.keypair],
            blockhash,
            lookup_tables,
        ).map_err(|e| TimingError::RpcError(format!("Failed to create MessageV0: {}", e)))?;

        // Create versioned transaction
        let tx = VersionedTransaction::try_new(
            VersionedMessage::V0(message),
            &[&*self.keypair],
        ).map_err(|e| TimingError::RpcError(format!("Failed to create VersionedTransaction: {}", e)))?;

        // Send the transaction with versioned transaction support
        let signature = self.broadcast_paths.rpc_client
            .send_transaction_with_config(
                &tx,
                RpcSendTransactionConfig {
                    skip_preflight: false,
                    preflight_commitment: Some(CommitmentLevel::Confirmed),
                    encoding: Some(UiTransactionEncoding::Base64),
                    max_retries: Some(0), // We handle retries ourselves with fresh blockhashes
                    min_context_slot: None,
                },
            )
            .await
            .map_err(|e| match e.to_string().to_lowercase() {
                // Check for blockhash-related errors
                e if e.contains("blockhash not found") || e.contains("blockhash expired") => 
                    TimingError::BlockhashExpired,
                e => TimingError::RpcError(e)
            })?;

        let elapsed = start_time.elapsed();
        debug!("Transaction {} sent in {:?} (attempt {})", signature, elapsed, attempt + 1);

        Ok(signature)
    }
    
    /// Generate a unique ID for transaction deduplication
    async fn generate_unique_id(&self) -> TimingResult<u64> {
        use std::sync::atomic::Ordering;
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        
        let counter = COUNTER.fetch_add(1, Ordering::Relaxed);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| TimingError::SystemTimeError)?
            .as_micros() as u64;
            
        Ok(timestamp.wrapping_add(counter))
    }
    
    /// Create a memo instruction with unique identifier
    fn create_unique_memo_instruction(&self, unique_id: u64) -> Instruction {
        use solana_program::memo::id as memo_program_id;
        
        let memo = format!("tx_{:x}", unique_id);
        Instruction {
            program_id: memo_program_id(),
            accounts: vec![],
            data: memo.into_bytes(),
        }
    }
    
    /// Get a fresh blockhash with commitment
    async fn get_fresh_blockhash(&self) -> TimingResult<(Hash, u64)> {
        self.broadcast_paths.rpc_client
            .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed().commitment)
            .await
            .map_err(|e| TimingError::RpcError(e.to_string()))
    }
    
    /// Get or create address lookup tables for the given instructions
    async fn get_or_create_alt(
        &self,
        instructions: &[Instruction],
    ) -> TimingResult<Vec<AddressLookupTableAccount>> {
        // Extract all account keys from instructions
        let mut account_keys = Vec::new();
        for ix in instructions {
            account_keys.extend(ix.accounts.iter().map(|a| a.pubkey));
            if let Some(pda) = ix.program_id.find_program_address(&[], &spl_token::id()).0 {
                account_keys.push(pda);
            }
        }
        
        // Deduplicate and sort for consistent ordering
        account_keys.sort();
        account_keys.dedup();
        
        // For now, return empty vector - in production, you'd want to:
        // 1. Check if we already have a suitable ALT
        // 2. If not, create a new one with create_lookup_table
        // 3. Add the accounts to it with extend_lookup_table
        // 4. Return the ALT account
        Ok(Vec::new())
    }

    /// Subscribe to transaction logs for instant inclusion detection
    #[instrument(skip(self))]
    async fn subscribe_to_logs(&self, signature: Signature) -> TimingResult<oneshot::Receiver<()>> {
        let (tx, rx) = oneshot::channel();
        {
            let mut tracker = self.logs_subscription.signature_tracker.lock().await;
            tracker.insert(signature, tx);
        }
        
        // If not already subscribed, start the subscription
        if let Some(pubsub) = &self.logs_subscription.pubsub_client {
            if !self.logs_subscription.signature_tracker.lock().await.is_empty() {
                let tracker = self.logs_subscription.signature_tracker.clone();
                let pubsub = pubsub.clone();
                
                tokio::spawn(async move {
                    let result = pubsub.logs_subscribe(
                        RpcTransactionLogsFilter::Mentions(vec![signature.to_string()]),
                        RpcTransactionLogsConfig {
                            commitment: Some(CommitmentConfig::confirmed()),
                        },
                    ).await;
                    
                    if let Ok((mut stream, _)) = result {
                        while let Some(log) = stream.next().await {
                            if log.value.err.is_none() {
                                if let Some(tx) = tracker.lock().await.remove(&signature) {
                                    let _ = tx.send(());
                                    break;
                                }
                            }
                        }
                    }
                });
            }
        }
        
        Ok(rx)
    }
    
    /// Confirm transaction with logs subscription for instant inclusion
    #[instrument(skip(self))]
    async fn confirm_transaction_fast(&self, signature: Signature) -> TimingResult<bool> {
        const CONFIRMATION_TIMEOUT: Duration = Duration::from_secs(10);
        const POLL_INTERVAL: Duration = Duration::from_millis(100);

        let start = Instant::now();
        let mut attempts = 0;
        
        // Subscribe to logs for instant confirmation
        let log_subscription = self.subscribe_to_logs(signature).await?;

        loop {
            if start.elapsed() > CONFIRMATION_TIMEOUT {
                return Err(TimingError::Timeout);
            }

            match self.rpc_client
                .get_signature_status(&signature)
                .await
                .map_err(|e| TimingError::RpcError(e.to_string()))?
            {
                Some(Ok(_)) => {
                    debug!("Transaction {} confirmed after {:?} ({} attempts)", signature, start.elapsed(), attempts);
                    return Ok(true);
                }
                Some(Err(err)) => {
                    return Err(TimingError::ConfirmationFailed(format!("Transaction failed: {:?}", err)));
                }
                None => {
                    // Still pending
                    attempts += 1;
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
            }
        }
    }

    async fn calculate_optimal_timing(&self, target_slot: u64) -> TimingResult<Instant> {
        let slot_tracker = self.slot_tracker.read().await;
        let network_conditions = self.network_monitor.get_conditions().await;
        let timing_history = self.timing_history.lock().await;

        let base_timing = if let Some(slot_timing) = slot_tracker.get(&target_slot) {
            slot_timing.slot_start_time
        } else {
            let current_slot = self.current_slot.load(Ordering::Relaxed);
            let slots_ahead = target_slot.saturating_sub(current_slot);
            Instant::now() + Duration::from_millis(slots_ahead * SLOT_DURATION_MS)
        };

        let mut timing_adjustment = 0i64;

        if !timing_history.is_empty() {
            let recent_timings: Vec<_> = timing_history
                .iter()
                .rev()
                .take(50)
                .filter(|m| m.success)
                .collect();

            if !recent_timings.is_empty() {
                let avg_execution_time = recent_timings
                    .iter()
                    .map(|m| m.execution_time_ms as i64)
                    .sum::<i64>() / recent_timings.len() as i64;

                timing_adjustment -= avg_execution_time / 2;
            }
        }

        timing_adjustment -= network_conditions.average_latency_ms as i64;
        timing_adjustment -= (network_conditions.latency_variance * 1.5) as i64;
        timing_adjustment -= (network_conditions.congestion_factor * 10.0) as i64;

        let target_time = base_timing + Duration::from_millis(timing_adjustment as u64);

        self.wait_for_optimal_moment(target_time).await?;

        Ok(target_time)
    }

    async fn wait_for_optimal_moment(&self, target_time: Instant) -> TimingResult<()> {
        let now = Instant::now();
        if now < target_time {
            let sleep_time = target_time - now;
            tokio::time::sleep(sleep_time).await;
                    break;
                }
            }
            
            Ok(false)
        };
        
        match timeout(timeout_duration, confirmation_task).await {
            Ok(result) => result,
            Err(_) => Ok(false),
        }
    }

    async fn record_timing_success(&self, signature: Signature, attempts: u8) {
        if let Some((_, state)) = self.active_executions.remove(&signature) {
            let execution_time = state.start_time.elapsed().as_millis() as u64;
            let network_latency = self.network_monitor.get_conditions().await.average_latency_ms as u64;
            
            let metric = TimingMetrics {
                slot_number: self.current_slot.load(Ordering::Relaxed),
                execution_time_ms: execution_time,
                network_latency_ms: network_latency,
                success: true,
                priority_fee: self.config.read().await.priority_fee_lamports,
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            };
            
            let mut history = self.timing_history.lock().await;
            if history.len() >= MAX_TIMING_HISTORY {
                history.pop_front();
            }
            history.push_back(metric);
            
            if attempts == 0 && self.config.read().await.use_adaptive_timing {
                self.update_timing_parameters().await;
            }
        }
    }

    async fn update_timing_parameters(&self) {
        let history = self.timing_history.lock().await;
        if history.len() < 50 {
            return;
        }
        
        let recent_metrics: Vec<_> = history
            .iter()
            .rev()
            .take(100)
            .filter(|m| m.success)
            .collect();
        
        if recent_metrics.len() < 20 {
            return;
        }
        
        let avg_execution_time = recent_metrics
            .iter()
            .map(|m| m.execution_time_ms as f64)
            .sum::<f64>() / recent_metrics.len() as f64;
        
        let avg_network_latency = recent_metrics
            .iter()
            .map(|m| m.network_latency_ms as f64)
            .sum::<f64>() / recent_metrics.len() as f64;
        
        let mut sorted_fees: Vec<_> = recent_metrics
            .iter()
            .map(|m| m.priority_fee)
            .collect();
        sorted_fees.sort_unstable();
        
        let percentile_index = (sorted_fees.len() as f64 * PRIORITY_FEE_PERCENTILE) as usize;
        let recommended_fee = sorted_fees[percentile_index.min(sorted_fees.len() - 1)];
        
        let mut config = self.config.write().await;
        
        config.pre_execution_offset_ms = ((avg_execution_time + avg_network_latency) * 0.6) as u64;
        config.pre_execution_offset_ms = config.pre_execution_offset_ms.clamp(5, 50);
        
        if config.priority_fee_lamports < recommended_fee {
            config.priority_fee_lamports = (config.priority_fee_lamports + recommended_fee) / 2;
        }
        
        config.retry_delay_ms = (avg_network_latency * 0.4) as u64;
        config.retry_delay_ms = config.retry_delay_ms.clamp(10, 100);
    }

    pub async fn execute_batch_timed(
        &self,
        transaction_batches: Vec<Vec<Instruction>>,
        target_slot_offset: u64,
    ) -> Result<Vec<Result<Signature>>> {
        let config = self.config.read().await;
        let max_concurrent = config.max_concurrent_attempts;
        
        let mut tasks = Vec::new();
        let semaphore = Arc::new(Semaphore::new(max_concurrent));
        
        for instructions in transaction_batches {
            let executor = self.clone();
            let sem = semaphore.clone();
            
            let task = tokio::spawn(async move {
                let _permit = sem.acquire().await?;
                executor.execute_timed_transaction(instructions, target_slot_offset).await
            });
            
            tasks.push(task);
        }
        
        let results = join_all(tasks).await;
        
        Ok(results
            .into_iter()
            .map(|r| r.unwrap_or_else(|e| Err(anyhow::anyhow!("Task error: {}", e))))
            .collect())
    }

    pub async fn simulate_transaction(
        &self,
        instructions: Vec<Instruction>,
    ) -> Result<RpcSimulateTransactionResult> {
        let recent_blockhash = self.rpc_client
            .get_latest_blockhash()
            .await
            .context("Failed to get recent blockhash")?;
        
        let message = Message::new_with_blockhash(
            &instructions,
            Some(&self.keypair.pubkey()),
            &recent_blockhash,
        );
        
        let mut transaction = Transaction::new_unsigned(message);
        transaction.sign(&[self.keypair.as_ref()], recent_blockhash);
        
        let config = RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: true,
            commitment: Some(CommitmentConfig::processed()),
            encoding: Some(UiTransactionEncoding::Base64),
            accounts: None,
            min_context_slot: None,
            inner_instructions: false,
        };
        
        self.rpc_client
            .simulate_transaction_with_config(&transaction, config)
            .await
            .context("Failed to simulate transaction")
    }

    pub async fn update_config(&self, new_config: TimingConfig) {
        *self.config.write().await = new_config;
    }

    pub async fn get_timing_stats(&self) -> TimingStats {
        let history = self.timing_history.lock().await;
        let total = history.len();
        
        if total == 0 {
            return TimingStats::default();
        }
        
        let successful = history.iter().filter(|m| m.success).count();
        let total_execution_time: u64 = history.iter().map(|m| m.execution_time_ms).sum();
        let total_latency: u64 = history.iter().map(|m| m.network_latency_ms).sum();
        
        TimingStats {
            total_executions: total,
            successful_executions: successful,
            success_rate: successful as f64 / total as f64,
            average_execution_time_ms: total_execution_time as f64 / total as f64,
            average_network_latency_ms: total_latency as f64 / total as f64,
        }
    }

    pub async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        
        let active_sigs: Vec<_> = self.active_executions
            .iter()
            .map(|entry| *entry.key())
            .collect();
        
        for sig in active_sigs {
            self.active_executions.remove(&sig);
        }
    }

    pub async fn optimize_for_leader(&self, leader: Pubkey) -> Result<()> {
        let mut slot_tracker = self.slot_tracker.write().await;
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        
        for i in 0..4 {
            let slot = current_slot + i;
            if let Some(timing) = slot_tracker.get_mut(&slot) {
                timing.leader_schedule = Some(leader);
            }
        }
        
        Ok(())
    }

    pub async fn execute_with_custom_timing(
        &self,
        instructions: Vec<Instruction>,
        custom_timing: Instant,
    ) -> Result<Signature> {
        let _permit = self.execution_semaphore.acquire().await?;
        
        if custom_timing > Instant::now() {
            self.wait_for_optimal_moment(custom_timing).await?;
        }
        
        let config = self.config.read().await.clone();
        
        let mut instructions_with_budget = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(config.compute_unit_limit),
            ComputeBudgetInstruction::set_compute_unit_price(
                (config.priority_fee_lamports * 1_000_000) / config.compute_unit_limit as u64
            ),
        ];
        instructions_with_budget.extend(instructions);
        
        self.send_transaction_with_timing(&instructions_with_budget, 0).await
    }
}

impl Clone for TimingAttackExecutor {
    fn clone(&self) -> Self {
        Self {
            rpc_client: self.rpc_client.clone(),
            keypair: self.keypair.clone(),
            config: self.config.clone(),
            timing_history: self.timing_history.clone(),
            slot_tracker: self.slot_tracker.clone(),
            execution_semaphore: self.execution_semaphore.clone(),
            active_executions: self.active_executions.clone(),
            network_monitor: self.network_monitor.clone(),
            shutdown: self.shutdown.clone(),
            current_slot: self.current_slot.clone(),
        }
    }
}

#[derive(Debug, Default)]
pub struct TimingStats {
    pub total_executions: usize,
    pub successful_executions: usize,
    pub success_rate: f64,
    pub average_execution_time_ms: f64,
    pub average_network_latency_ms: f64,
}
