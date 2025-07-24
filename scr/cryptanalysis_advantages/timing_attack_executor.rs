use anyhow::{Result, Context};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig},
    rpc_response::RpcSimulateTransactionResult,
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    message::Message,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::{Transaction, VersionedTransaction},
};
use solana_transaction_status::UiTransactionEncoding;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex, RwLock, Semaphore},
    time::{interval, sleep, timeout},
};
use dashmap::DashMap;
use futures::future::join_all;

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

pub struct TimingAttackExecutor {
    rpc_client: Arc<RpcClient>,
    keypair: Arc<Keypair>,
    config: Arc<RwLock<TimingConfig>>,
    timing_history: Arc<Mutex<VecDeque<TimingMetrics>>>,
    slot_tracker: Arc<RwLock<HashMap<u64, SlotTiming>>>,
    execution_semaphore: Arc<Semaphore>,
    active_executions: Arc<DashMap<Signature, ExecutionState>>,
    network_monitor: Arc<NetworkMonitor>,
    shutdown: Arc<AtomicBool>,
    current_slot: Arc<AtomicU64>,
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
            rpc_client,
            keypair: Arc::new(keypair),
            config: Arc::new(RwLock::new(config)),
            timing_history: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_TIMING_HISTORY))),
            slot_tracker: Arc::new(RwLock::new(HashMap::new())),
            execution_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_EXECUTIONS)),
            active_executions: Arc::new(DashMap::new()),
            network_monitor: Arc::new(NetworkMonitor::new()),
            shutdown: Arc::new(AtomicBool::new(false)),
            current_slot: Arc::new(AtomicU64::new(0)),
        };

        executor.start_monitoring().await;
        Ok(executor)
    }

    async fn start_monitoring(&self) {
        let rpc_client = self.rpc_client.clone();
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

    pub async fn execute_timed_transaction(
        &self,
        instructions: Vec<Instruction>,
        target_slot_offset: u64,
    ) -> Result<Signature> {
        let _permit = self.execution_semaphore.acquire().await?;
        let config = self.config.read().await.clone();
        
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        let target_slot = current_slot + target_slot_offset;
        
        let optimal_timing = self.calculate_optimal_timing(target_slot).await?;
        
        if config.use_adaptive_timing {
            self.wait_for_optimal_moment(optimal_timing).await?;
        }
        
        let mut instructions_with_budget = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(config.compute_unit_limit),
            ComputeBudgetInstruction::set_compute_unit_price(
                (config.priority_fee_lamports * 1_000_000) / config.compute_unit_limit as u64
            ),
        ];
        instructions_with_budget.extend(instructions);
        
        let mut attempts = 0;
        let mut last_error = None;
        
        while attempts < MAX_RETRIES {
            let signature = self.send_transaction_with_timing(
                &instructions_with_budget,
                attempts,
            ).await?;
            
            self.active_executions.insert(
                signature,
                ExecutionState {
                    signature,
                    start_time: Instant::now(),
                    attempts,
                    status: ExecutionStatus::Executing,
                },
            );
            
            match self.confirm_transaction_fast(signature).await {
                Ok(confirmed) => {
                    if confirmed {
                        self.record_timing_success(signature, attempts).await;
                        return Ok(signature);
                    }
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                    self.active_executions.alter(&signature, |_, mut state| {
                        state.status = ExecutionStatus::Failed(e.to_string());
                        state
                    });
                }
            }
            
            attempts += 1;
            if attempts < MAX_RETRIES {
                sleep(Duration::from_millis(config.retry_delay_ms)).await;
            }
        }
        
        Err(anyhow::anyhow!("Failed after {} attempts: {}", 
            MAX_RETRIES, 
            last_error.unwrap_or_else(|| "Unknown error".to_string())
        ))
    }

    async fn calculate_optimal_timing(&self, target_slot: u64) -> Result<Instant> {
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
        
        let config = self.config.read().await;
        timing_adjustment -= config.pre_execution_offset_ms as i64;
        
        let optimal_time = base_timing - Duration::from_millis(timing_adjustment.abs() as u64);
        
        Ok(optimal_time)
    }

    async fn wait_for_optimal_moment(&self, target_time: Instant) -> Result<()> {
        let now = Instant::now();
        if target_time > now {
            let wait_duration = target_time - now;
            
            if wait_duration > Duration::from_secs(10) {
                return Err(anyhow::anyhow!("Wait time too long: {:?}", wait_duration));
            }
            
            let millis_to_wait = wait_duration.as_millis() as u64;
            let high_precision_wait = millis_to_wait.saturating_sub(1);
            
            if high_precision_wait > 0 {
                sleep(Duration::from_millis(high_precision_wait)).await;
            }
            
            while Instant::now() < target_time {
                std::hint::spin_loop();
            }
        }
        
        Ok(())
    }

    async fn send_transaction_with_timing(
        &self,
        instructions: &[Instruction],
        attempt: u8,
    ) -> Result<Signature> {
        let recent_blockhash = self.rpc_client
            .get_latest_blockhash()
            .await
            .context("Failed to get recent blockhash")?;
        
        let message = Message::new_with_blockhash(
            instructions,
            Some(&self.keypair.pubkey()),
            &recent_blockhash,
        );
        
        let mut transaction = Transaction::new_unsigned(message);
        transaction.sign(&[self.keypair.as_ref()], recent_blockhash);
        
        let config = self.config.read().await;
        let send_config = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Processed),
            encoding: Some(UiTransactionEncoding::Base64),
            max_retries: Some(0),
            min_context_slot: None,
        };
        
        let signature = if attempt > 0 && config.network_latency_compensation {
            let adjusted_config = RpcSendTransactionConfig {
                skip_preflight: true,
                preflight_commitment: Some(CommitmentLevel::Processed),
                encoding: Some(UiTransactionEncoding::Base64),
                max_retries: Some(1),
                min_context_slot: Some(self.current_slot.load(Ordering::Relaxed)),
            };
            
            self.rpc_client
                .send_transaction_with_config(&transaction, adjusted_config)
                .await
                .context("Failed to send transaction")?
        } else {
            self.rpc_client
                .send_transaction_with_config(&transaction, send_config)
                .await
                .context("Failed to send transaction")?
        };
        
        Ok(signature)
    }

    async fn confirm_transaction_fast(&self, signature: Signature) -> Result<bool> {
        let start_time = Instant::now();
        let config = self.config.read().await;
        let timeout_duration = Duration::from_millis(config.execution_window_ms);
        
        let confirmation_task = async {
            let mut interval = interval(Duration::from_millis(10));
            let mut checks = 0;
            
            loop {
                if checks > 0 {
                    interval.tick().await;
                }
                checks += 1;
                
                match self.rpc_client.get_signature_statuses(&[signature]).await {
                    Ok(response) => {
                        if let Some(status) = response.value[0].as_ref() {
                            if status.confirmations.is_some() || status.err.is_some() {
                                return Ok(status.err.is_none());
                            }
                        }
                    }
                    Err(_) if checks < 3 => continue,
                    Err(e) => return Err(anyhow::anyhow!("RPC error: {}", e)),
                }
                
                if start_time.elapsed() > timeout_duration {
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
