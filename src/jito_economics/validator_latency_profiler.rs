use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    clock::Slot,
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::Signature,
};
use std::{
    collections::{HashMap, VecDeque, BTreeMap},
    sync::{
        atomic::{AtomicU64, AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex, RwLock, Semaphore},
    time::{interval, timeout},
};
use serde::{Deserialize, Serialize};
use anyhow::{Result, Context};
use parking_lot::FairMutex;

const LATENCY_WINDOW_SIZE: usize = 2000;
const OUTLIER_THRESHOLD_MULTIPLIER: f64 = 3.0;
const MIN_SAMPLES_FOR_STATS: usize = 100;
const VALIDATOR_HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(30);
const LATENCY_PROBE_INTERVAL: Duration = Duration::from_millis(50);
const MAX_CONCURRENT_PROBES: usize = 100;
const PROBE_TIMEOUT: Duration = Duration::from_millis(300);
const SLOT_TRACKING_WINDOW: usize = 200;
const CRITICAL_LATENCY_MS: f64 = 100.0;
const OPTIMAL_LATENCY_MS: f64 = 25.0;
const MAX_CONSECUTIVE_FAILURES: u32 = 5;
const STAKE_UPDATE_INTERVAL: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorMetrics {
    pub identity: Pubkey,
    pub rpc_endpoint: String,
    pub tpu_endpoint: String,
    pub last_update_ms: u64,
    pub slot_latencies: VecDeque<SlotLatency>,
    pub transaction_latencies: VecDeque<TransactionLatency>,
    pub health_score: f64,
    pub success_rate: f64,
    pub avg_latency_ms: f64,
    pub median_latency_ms: f64,
    pub p75_latency_ms: f64,
    pub p90_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotLatency {
    pub slot: Slot,
    pub latency_ms: f64,
    pub timestamp_ms: u64,
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
    probe_semaphore: Arc<Semaphore>,
    active_probes: Arc<AtomicU64>,
    total_probes: Arc<AtomicU64>,
    successful_probes: Arc<AtomicU64>,
    running: Arc<AtomicBool>,
    stake_weights: Arc<RwLock<HashMap<Pubkey, u64>>>,
    global_best_slot: Arc<AtomicU64>,
    latency_cache: Arc<Mutex<BTreeMap<(Pubkey, u64), f64>>>,
}

use dashmap::DashMap;

impl ValidatorLatencyProfiler {
    pub async fn new(validator_configs: Vec<(Pubkey, String, String)>) -> Result<Arc<Self>> {
        let validators = Arc::new(DashMap::new());
        let rpc_clients = Arc::new(DashMap::new());
        
        for (pubkey, rpc_endpoint, tpu_endpoint) in validator_configs {
            let client = Arc::new(RpcClient::new_with_timeout_and_commitment(
                rpc_endpoint.clone(),
                PROBE_TIMEOUT,
                CommitmentConfig::processed(),
            ));
            
            rpc_clients.insert(pubkey, client);
            
            let metrics = ValidatorMetrics {
                identity: pubkey,
                rpc_endpoint,
                tpu_endpoint,
                last_update_ms: Self::current_timestamp_ms(),
                slot_latencies: VecDeque::with_capacity(LATENCY_WINDOW_SIZE),
                transaction_latencies: VecDeque::with_capacity(LATENCY_WINDOW_SIZE),
                health_score: 80.0,
                success_rate: 100.0,
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
            
            validators.insert(pubkey, Arc::new(RwLock::new(metrics)));
        }

        Ok(Arc::new(Self {
            validators,
            rpc_clients,
            probe_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_PROBES)),
            active_probes: Arc::new(AtomicU64::new(0)),
            total_probes: Arc::new(AtomicU64::new(0)),
            successful_probes: Arc::new(AtomicU64::new(0)),
            running: Arc::new(AtomicBool::new(true)),
            stake_weights: Arc::new(RwLock::new(HashMap::new())),
            global_best_slot: Arc::new(AtomicU64::new(0)),
            latency_cache: Arc::new(Mutex::new(BTreeMap::new())),
        }))
    }

    pub async fn start(self: Arc<Self>) -> Result<()> {
        let handles = vec![
            tokio::spawn(self.clone().run_latency_monitor()),
            tokio::spawn(self.clone().run_health_checker()),
            tokio::spawn(self.clone().run_stats_calculator()),
            tokio::spawn(self.clone().run_stake_updater()),
            tokio::spawn(self.clone().run_cache_cleaner()),
        ];

        futures::future::try_join_all(handles).await?;
        Ok(())
    }

    async fn run_latency_monitor(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(LATENCY_PROBE_INTERVAL);
        
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
        defer!({
            self.active_probes.fetch_sub(1, Ordering::AcqRel);
        });

        let start = Instant::now();
        let client = self.rpc_clients.get(&validator)
            .context("RPC client not found")?;

        match timeout(PROBE_TIMEOUT, client.get_slot()).await {
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
        let timestamp_ms = Self::current_timestamp_ms();
        
        metrics.slot_latencies.push_back(SlotLatency {
            slot,
            latency_ms,
            timestamp_ms,
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

        // Sort for percentile calculations
        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        
        metrics.avg_latency_ms = self.calculate_mean(&latencies);
        metrics.median_latency_ms = self.calculate_percentile(&latencies, 0.50);
        metrics.p75_latency_ms = self.calculate_percentile(&latencies, 0.75);
        metrics.p90_latency_ms = self.calculate_percentile(&latencies, 0.90);
        metrics.p95_latency_ms = self.calculate_percentile(&latencies, 0.95);
        metrics.p99_latency_ms = self.calculate_percentile(&latencies, 0.99);
        
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
        
        let index = (percentile * (sorted_values.len() - 1) as f64).round() as usize;
        sorted_values[index.min(sorted_values.len() - 1)]
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
        
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
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
        
        validators.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
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
        
        validators.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
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
        
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
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
                last_update_ms: Self::current_timestamp_ms(),
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

    fn current_timestamp_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
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

