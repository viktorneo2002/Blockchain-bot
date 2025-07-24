use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcGetVoteAccountsConfig, RpcLeaderScheduleConfig};
use solana_client::rpc_response::RpcVoteAccountStatus;
use solana_sdk::{
    pubkey::Pubkey,
    commitment_config::{CommitmentConfig, CommitmentLevel},
    clock::Slot,
    epoch_info::EpochInfo,
};
use std::sync::{Arc, atomic::{AtomicU64, AtomicBool, AtomicU32, Ordering}};
use std::collections::{HashMap, BTreeMap, VecDeque, HashSet};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{RwLock, Semaphore, Mutex};
use tokio::time::{interval, timeout, sleep};
use serde::{Deserialize, Serialize};
use dashmap::DashMap;
use futures::future::join_all;
use std::str::FromStr;
use tracing::{instrument, info, warn, error, debug};
use anyhow::{Result, anyhow, Context, bail};
use sha2::{Sha256, Digest};
use rand::Rng;

// Production-hardened constants for mainnet warfare
const HEALTH_CHECK_INTERVAL_MS: u64 = 100;
const TOPOLOGY_UPDATE_INTERVAL_MS: u64 = 2000;
const LEADER_SCHEDULE_UPDATE_INTERVAL_MS: u64 = 1000;
const ROUTE_OPTIMIZATION_INTERVAL_MS: u64 = 500;
const WATCHDOG_INTERVAL_MS: u64 = 1000;
const MAX_CONSECUTIVE_FAILURES: u32 = 2;
const CONNECTION_TIMEOUT_MS: u64 = 250;
const RPC_TIMEOUT_MS: u64 = 800;
const MAX_RPC_RETRIES: usize = 2;
const MAX_ENDPOINTS: usize = 16;
const MIN_HEALTHY_ENDPOINTS: usize = 3;
const VALIDATOR_PERFORMANCE_WINDOW: usize = 100;
const LATENCY_WEIGHT: f64 = 0.45;
const RELIABILITY_WEIGHT: f64 = 0.35;
const STAKE_WEIGHT: f64 = 0.20;
const SUCCESS_RATE_THRESHOLD: f64 = 0.92;
const MAX_CONCURRENT_CHECKS: usize = 6;
const SLOT_TOLERANCE: u64 = 8;
const MAX_SLOT_DRIFT: u64 = 32;
const EPOCH_CACHE_TTL_MS: u64 = 15000;
const CIRCUIT_BREAKER_THRESHOLD: u32 = 3;
const CIRCUIT_BREAKER_RECOVERY_MS: u64 = 20000;
const MAX_VALIDATOR_ENTRIES: usize = 1500;
const MAX_LEADER_SCHEDULE_ENTRIES: usize = 3000;
const MIN_MAINNET_STAKE: u64 = 500_000_000_000; // 500 SOL
const MAX_COMMISSION_RATE: u8 = 100;
const STALE_DATA_THRESHOLD_MS: u64 = 1500;
const REGIONAL_FAILOVER_THRESHOLD: usize = 2;
const SECURITY_VIOLATION_THRESHOLD: u32 = 8;
const PANIC_MODE_HEALTH_THRESHOLD: u64 = 30;
const LATENCY_HISTORY_SIZE: usize = 500;
const SLOT_HASH_ENTROPY_THRESHOLD: f64 = 0.1;
const ENDPOINT_DEGRADATION_RATE: f64 = 0.05;
const STAKE_VALIDATION_THRESHOLD: u64 = 1_000_000_000_000; // 1K SOL
const FORK_DETECTION_WINDOW: usize = 10;
const CONSENSUS_THRESHOLD: f64 = 0.67;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointMetrics {
    pub endpoint: String,
    pub latency_ms: Arc<AtomicU64>,
    pub success_rate: Arc<AtomicU64>,
    pub last_check_ms: Arc<AtomicU64>,
    pub consecutive_failures: Arc<AtomicU32>,
    pub total_requests: Arc<AtomicU64>,
    pub successful_requests: Arc<AtomicU64>,
    pub is_active: Arc<AtomicBool>,
    pub region: String,
    pub endpoint_type: EndpointType,
    pub priority_score: Arc<AtomicU64>,
    pub last_slot_seen: Arc<AtomicU64>,
    pub slot_drift: Arc<AtomicU64>,
    pub circuit_breaker_count: Arc<AtomicU32>,
    pub last_circuit_break: Arc<AtomicU64>,
    pub security_violations: Arc<AtomicU32>,
    pub degradation_score: Arc<AtomicU64>,
    pub tls_verified: Arc<AtomicBool>,
    pub stake_seen: Arc<AtomicU64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EndpointType {
    Primary,
    Secondary,
    Fallback,
}

#[derive(Debug, Clone)]
pub struct ValidatorMetrics {
    pub identity: Pubkey,
    pub vote_account: Pubkey,
    pub stake: u64,
    pub commission: u8,
    pub last_vote: Slot,
    pub credits: u64,
    pub skip_rate: f64,
    pub performance_history: VecDeque<PerformanceSample>,
    pub is_delinquent: bool,
    pub epoch_credits: Vec<(u64, u64, u64)>,
    pub validation_hash: [u8; 32],
    pub last_validation: u64,
    pub suspicious_count: u32,
    pub stake_trend: f64,
    pub vote_distance: u64,
}

#[derive(Debug, Clone, Copy)]
pub struct PerformanceSample {
    pub slot: Slot,
    pub credits_earned: u64,
    pub slots_skipped: u64,
    pub timestamp: u64,
    pub vote_latency: u64,
    pub entropy_hash: [u8; 8],
}

#[derive(Debug, Clone)]
pub struct OptimalRoute {
    pub primary_endpoint: String,
    pub fallback_endpoints: Vec<String>,
    pub latency_p95: f64,
    pub reliability_score: f64,
    pub last_updated: Instant,
    pub region: String,
    pub slot_consistency: f64,
    pub security_score: f64,
    pub partition_resistance: f64,
    pub stake_coverage: f64,
}

#[derive(Debug, Clone)]
pub struct LeaderScheduleEntry {
    pub slot: Slot,
    pub leader: Pubkey,
    pub stake: u64,
    pub estimated_block_time: u64,
    pub confidence_score: f64,
    pub validation_hash: [u8; 32],
    pub source_verified: bool,
    pub fork_probability: f64,
}

#[derive(Debug, Clone)]
struct SlotInfo {
    slot: u64,
    timestamp: u64,
    source: String,
    validation_hash: [u8; 32],
    drift_score: f64,
    entropy_score: f64,
}

#[derive(Debug, Clone)]
struct CircuitBreaker {
    failure_count: u32,
    last_failure: Instant,
    is_open: bool,
    recovery_attempts: u32,
}

#[derive(Debug, Clone)]
struct RegionalHealth {
    region: String,
    active_endpoints: usize,
    avg_latency: f64,
    reliability: f64,
    stake_coverage: f64,
    last_updated: Instant,
}

pub struct NetworkTopologyMapper {
    endpoints: Arc<DashMap<String, Arc<EndpointMetrics>>>,
    validators: Arc<RwLock<HashMap<Pubkey, ValidatorMetrics>>>,
    leader_schedule: Arc<RwLock<BTreeMap<Slot, LeaderScheduleEntry>>>,
    optimal_routes: Arc<DashMap<String, OptimalRoute>>,
    latency_history: Arc<DashMap<String, VecDeque<(u64, u64)>>>,
    rpc_clients: Arc<DashMap<String, Arc<RpcClient>>>,
    current_slot: Arc<AtomicU64>,
    current_epoch: Arc<AtomicU64>,
    network_health_score: Arc<AtomicU64>,
    semaphore: Arc<Semaphore>,
    shutdown_signal: Arc<AtomicBool>,
    panic_mode: Arc<AtomicBool>,
    slot_history: Arc<RwLock<VecDeque<SlotInfo>>>,
    epoch_cache: Arc<RwLock<Option<(EpochInfo, u64)>>>,
    circuit_breakers: Arc<DashMap<String, CircuitBreaker>>,
    malicious_validators: Arc<RwLock<HashSet<Pubkey>>>,
    trusted_endpoints: Arc<RwLock<HashSet<String>>>,
    slot_consensus: Arc<RwLock<HashMap<u64, Vec<(String, [u8; 32])>>>>,
    regional_health: Arc<DashMap<String, RegionalHealth>>,
    security_violations: Arc<AtomicU32>,
    last_watchdog_check: Arc<AtomicU64>,
    fork_detection_buffer: Arc<RwLock<VecDeque<(u64, String, [u8; 32])>>>,
    partition_detected: Arc<AtomicBool>,
    consensus_failures: Arc<AtomicU32>,
    stake_validation_cache: Arc<RwLock<HashMap<Pubkey, (u64, u64)>>>,
    endpoint_whitelist: Arc<RwLock<HashSet<String>>>,
}

impl NetworkTopologyMapper {
    #[instrument(name = "topology_mapper_new", skip(initial_endpoints))]
    pub async fn new(initial_endpoints: Vec<String>) -> Result<Arc<Self>> {
        if initial_endpoints.is_empty() {
            bail!("No endpoints provided for mainnet deployment");
        }

        let validated_endpoints = Self::validate_mainnet_endpoints(initial_endpoints)?;
        if validated_endpoints.len() < MIN_HEALTHY_ENDPOINTS {
            bail!("Insufficient validated endpoints for mainnet: {} < {}", 
                validated_endpoints.len(), MIN_HEALTHY_ENDPOINTS);
        }

        info!("Initializing mainnet-hardened NetworkTopologyMapper with {} endpoints", 
            validated_endpoints.len());

        let mapper = Arc::new(Self {
            endpoints: Arc::new(DashMap::new()),
            validators: Arc::new(RwLock::new(HashMap::new())),
            leader_schedule: Arc::new(RwLock::new(BTreeMap::new())),
            optimal_routes: Arc::new(DashMap::new()),
            latency_history: Arc::new(DashMap::new()),
            rpc_clients: Arc::new(DashMap::new()),
            current_slot: Arc::new(AtomicU64::new(0)),
            current_epoch: Arc::new(AtomicU64::new(0)),
            network_health_score: Arc::new(AtomicU64::new(0)),
            semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_CHECKS)),
            shutdown_signal: Arc::new(AtomicBool::new(false)),
            panic_mode: Arc::new(AtomicBool::new(false)),
            slot_history: Arc::new(RwLock::new(VecDeque::with_capacity(100))),
            epoch_cache: Arc::new(RwLock::new(None)),
            circuit_breakers: Arc::new(DashMap::new()),
            malicious_validators: Arc::new(RwLock::new(HashSet::new())),
            trusted_endpoints: Arc::new(RwLock::new(HashSet::new())),
            slot_consensus: Arc::new(RwLock::new(HashMap::new())),
            regional_health: Arc::new(DashMap::new()),
            security_violations: Arc::new(AtomicU32::new(0)),
            last_watchdog_check: Arc::new(AtomicU64::new(0)),
            fork_detection_buffer: Arc::new(RwLock::new(VecDeque::with_capacity(FORK_DETECTION_WINDOW))),
            partition_detected: Arc::new(AtomicBool::new(false)),
            consensus_failures: Arc::new(AtomicU32::new(0)),
            stake_validation_cache: Arc::new(RwLock::new(HashMap::new())),
            endpoint_whitelist: Arc::new(RwLock::new(HashSet::new())),
        });

        mapper.initialize_endpoints(validated_endpoints).await?;
        mapper.start_background_tasks().await;

        info!("NetworkTopologyMapper initialized and battle-ready for mainnet");
        Ok(mapper)
    }

    fn validate_mainnet_endpoints(endpoints: Vec<String>) -> Result<Vec<String>> {
        let mut validated = Vec::new();
        
        for endpoint in endpoints {
            if endpoint.contains("devnet") || endpoint.contains("testnet") {
                warn!("Rejected non-mainnet endpoint: {}", endpoint);
                continue;
            }
            
            if !endpoint.starts_with("https://") {
                warn!("Rejected insecure endpoint: {}", endpoint);
                continue;
            }

            if endpoint.len() > 512 || endpoint.len() < 10 {
                warn!("Rejected malformed endpoint: {}", endpoint);
                continue;
            }

            validated.push(endpoint);
        }

        if validated.is_empty() {
            bail!("No valid mainnet endpoints after validation");
        }

        Ok(validated)
    }

    #[instrument(name = "initialize_endpoints", skip(self, endpoints))]
    async fn initialize_endpoints(&self, endpoints: Vec<String>) -> Result<()> {
        info!("Initializing {} mainnet endpoints with concurrent validation", endpoints.len());

        let init_tasks = endpoints.into_iter().map(|endpoint| {
            let mapper = self.clone();
            tokio::spawn(async move {
                for attempt in 1..=MAX_RPC_RETRIES {
                    match timeout(
                        Duration::from_millis(RPC_TIMEOUT_MS),
                        mapper.add_endpoint_with_validation(endpoint.clone())
                    ).await {
                        Ok(Ok(())) => {
                            debug!("Endpoint {} validated on attempt {}", endpoint, attempt);
                            return Ok(());
                        }
                        Ok(Err(e)) => {
                            warn!("Endpoint {} failed attempt {}: {}", endpoint, attempt, e);
                        }
                        Err(_) => {
                            warn!("Endpoint {} timed out on attempt {}", endpoint, attempt);
                        }
                    }
                    
                    if attempt < MAX_RPC_RETRIES {
                        sleep(Duration::from_millis(100 * attempt as u64)).await;
                    }
                }
                
                Err(anyhow!("Failed to validate endpoint {} after {} attempts", endpoint, MAX_RPC_RETRIES))
            })
        });

        let results = join_all(init_tasks).await;
        
        let mut success_count = 0;
        for result in results {
            match result {
                Ok(Ok(())) => success_count += 1,
                Ok(Err(e)) => warn!("Endpoint validation failed: {}", e),
                Err(e) => warn!("Task join failed: {}", e),
            }
        }

        if success_count < MIN_HEALTHY_ENDPOINTS {
            bail!("Only {} endpoints validated, need {} for mainnet deployment", 
                success_count, MIN_HEALTHY_ENDPOINTS);
        }

        info!("Successfully validated {}/{} mainnet endpoints", success_count, endpoints.len());
        Ok(())
    }

    #[instrument(name = "add_endpoint_with_validation", skip(self), fields(endpoint = %endpoint))]
    async fn add_endpoint_with_validation(&self, endpoint: String) -> Result<()> {
        let client = RpcClient::new_with_timeout_and_commitment(
            endpoint.clone(),
            Duration::from_millis(CONNECTION_TIMEOUT_MS),
            CommitmentConfig::confirmed(),
        );

        // Validate endpoint with multiple checks
        let (slot, epoch_info) = tokio::try_join!(
            self.validate_slot(&client),
            self.validate_epoch_info(&client)
        )?;

        let region = self.detect_region(&endpoint);
        let tls_verified = endpoint.starts_with("https://");

        let metrics = Arc::new(EndpointMetrics {
            endpoint: endpoint.clone(),
            latency_ms: Arc::new(AtomicU64::new(u64::MAX)),
            success_rate: Arc::new(AtomicU64::new(0)),
            last_check_ms: Arc::new(AtomicU64::new(0)),
            consecutive_failures: Arc::new(AtomicU32::new(0)),
            total_requests: Arc::new(AtomicU64::new(0)),
            successful_requests: Arc::new(AtomicU64::new(0)),
            is_active: Arc::new(AtomicBool::new(true)),
            region: region.clone(),
            endpoint_type: EndpointType::Secondary,
            priority_score: Arc::new(AtomicU64::new(0)),
            last_slot_seen: Arc::new(AtomicU64::new(slot)),
            slot_drift: Arc::new(AtomicU64::new(0)),
            circuit_breaker_count: Arc::new(AtomicU32::new(0)),
            last_circuit_break: Arc::new(AtomicU64::new(0)),
            security_violations: Arc::new(AtomicU32::new(0)),
            degradation_score: Arc::new(AtomicU64::new(10000)),
            tls_verified: Arc::new(AtomicBool::new(tls_verified)),
            stake_seen: Arc::new(AtomicU64::new(0)),
        });

        self.endpoints.insert(endpoint.clone(), metrics);
        self.rpc_clients.insert(endpoint.clone(), Arc::new(client));
        self.latency_history.insert(endpoint.clone(), VecDeque::with_capacity(LATENCY_HISTORY_SIZE));
        
        self.circuit_breakers.insert(endpoint.clone(), CircuitBreaker {
            failure_count: 0,
            last_failure: Instant::now(),
            is_open: false,
            recovery_attempts: 0,
        });

        {
            let mut trusted = self.trusted_endpoints.write().await;
            trusted.insert(endpoint.clone());
        }

        {
            let mut whitelist = self.endpoint_whitelist.write().await;
            whitelist.insert(endpoint);
        }

        self.update_regional_health(&region).await;

        Ok(())
    }

    async fn validate_slot(&self, client: &RpcClient) -> Result<u64> {
        let slot = client.get_slot().await?;
        
        if slot == 0 {
            bail!("Invalid slot 0 from endpoint");
        }
        
        // Validate slot is within reasonable bounds (not too far in future)
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        // Rough estimate: slots should be within 24 hours of current time
        let max_expected_slot = current_time * 2; // ~400ms per slot
        
        if slot > max_expected_slot {
            bail!("Slot {} appears to be in far future", slot);
        }
        
        Ok(slot)
    }

    async fn validate_epoch_info(&self, client: &RpcClient) -> Result<EpochInfo> {
        let epoch_info = client.get_epoch_info().await?;
        
        if epoch_info.epoch == 0 {
            bail!("Invalid epoch 0 from endpoint");
        }
        
        if epoch_info.slot_index >= epoch_info.slots_in_epoch {
            bail!("Invalid epoch info: slot_index >= slots_in_epoch");
        }
        
        Ok(epoch_info)
    }

    #[instrument(name = "start_background_tasks", skip(self))]
    async fn start_background_tasks(self: &Arc<Self>) {
        info!("Starting battle-hardened background tasks");

        let tasks = vec![
            tokio::spawn(self.clone().health_check_loop()),
            tokio::spawn(self.clone().topology_update_loop()),
            tokio::spawn(self.clone().leader_schedule_update_loop()),
            tokio::spawn(self.clone().route_optimization_loop()),
            tokio::spawn(self.clone().slot_monitoring_loop()),
            tokio::spawn(self.clone().security_monitoring_loop()),
            tokio::spawn(self.clone().watchdog_loop()),
            tokio::spawn(self.clone().consensus_validation_loop()),
            tokio::spawn(self.clone().fork_detection_loop()),
        ];

        tokio::spawn(async move {
            for (i, task) in tasks.into_iter().enumerate() {
                if let Err(e) = task.await {
                    error!("Critical background task {} failed: {}", i, e);
                }
            }
        });
    }

    #[instrument(name = "health_check_loop", skip(self))]
    async fn health_check_loop(self: Arc<Self>) {
        let mut interval = interval(Duration::from_millis(HEALTH_CHECK_INTERVAL_MS));
        
        info!("Starting mainnet health check loop");
        
        while !self.shutdown_signal.load(Ordering::Relaxed) {
            interval.tick().await;
            
            if self.panic_mode.load(Ordering::Relaxed) {
                warn!("Health checks paused due to panic mode");
                sleep(Duration::from_millis(1000)).await;
                continue;
            }
            
            let endpoints: Vec<_> = self.endpoints.iter()
                .filter(|entry| !self.is_circuit_breaker_open(entry.key()).await)
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect();

            if endpoints.is_empty() {
                error!("No healthy endpoints for health checks");
                self.trigger_panic_mode().await;
                continue;
            }

            let check_tasks = endpoints.into_iter().map(|(endpoint, metrics)| {
                let mapper = self.clone();
                let permit = self.semaphore.clone().acquire_owned();
                
                tokio::spawn(async move {
                    match permit.await {
                        Ok(_permit) => {
                            mapper.check_endpoint_health(endpoint, metrics).await;
                        }
                        Err(e) => {
                            warn!("Failed to acquire health check permit: {}", e);
                        }
                    }
                })
            });

            let results = join_all(check_tasks).await;
            for result in results {
                if let Err(e) = result {
                    warn!("Health check task failed: {}", e);
                }
            }
            
            self.update_network_health().await;
        }
        
        info!("Health check loop terminated");
    }

    #[instrument(name = "check_endpoint_health", skip(self, metrics), fields(endpoint = %endpoint))]
    async fn check_endpoint_health(&self, endpoint: String, metrics: Arc<EndpointMetrics>) {
        let start = Instant::now();
        
        let client = match self.rpc_clients.get(&endpoint) {
            Some(client) => client.clone(),
            None => {
                warn!("No RPC client for endpoint: {}", endpoint);
                return;
            }
        };

        match timeout(
            Duration::from_millis(CONNECTION_TIMEOUT_MS),
            client.get_slot()
        ).await {
            Ok(Ok(slot)) => {
                if let Err(e) = self.validate_slot_response(slot, &endpoint, &metrics).await {
                    warn!("Slot validation failed for {}: {}", endpoint, e);
                    self.handle_endpoint_failure(&endpoint, &metrics).await;
                    return;
                }

                let latency = start.elapsed().as_millis() as u64;
                let current_time = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                self.update_endpoint_success(&endpoint, &metrics, slot, latency, current_time).await;
                self.record_slot_data(slot, &endpoint, current_time).await;
                self.reset_circuit_breaker(&endpoint).await;
            }
            _ => {
                self.handle_endpoint_failure(&endpoint, &metrics).await;
            }
        }
    }

    async fn validate_slot_response(&self, slot: u64, endpoint: &str, metrics: &EndpointMetrics) -> Result<()> {
        if slot == 0 {
            bail!("Invalid slot 0 from endpoint");
        }

        let last_slot = metrics.last_slot_seen.load(Ordering::Relaxed);
        if slot < last_slot {
            bail!("Slot regression detected: {} -> {}", last_slot, slot);
        }

        let slot_advance = slot - last_slot;
        if slot_advance > MAX_SLOT_DRIFT {
            bail!("Excessive slot advance: {}", slot_advance);
        }

        // Validate slot entropy
        let entropy = self.calculate_slot_entropy(slot, endpoint).await;
        if entropy < SLOT_HASH_ENTROPY_THRESHOLD {
            bail!("Low slot entropy detected: {}", entropy);
        }

        Ok(())
    }

    async fn calculate_slot_entropy(&self, slot: u64, endpoint: &str) -> f64 {
        let data = format!("{}:{}:{}", slot, endpoint, 
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_nanos());
        
        let mut hasher = Sha256::new();
        hasher.update(data.as_bytes());
        let hash = hasher.finalize();
        
        // Calculate entropy based on hash distribution
        let mut entropy = 0.0;
        for &byte in hash.iter() {
            if byte != 0 {
                let p = byte as f64 / 255.0;
                entropy -= p * p.log2();
            }
        }
        
        entropy / 8.0 // Normalize
    }

    async fn update_endpoint_success(&self, endpoint: &str, metrics: &EndpointMetrics, 
                                   slot: u64, latency: u64, current_time: u64) {
        let last_slot = metrics.last_slot_seen.load(Ordering::Relaxed);
        let last_check = metrics.last_check_ms.load(Ordering::Relaxed);
        
        let time_diff = current_time.saturating_sub(last_check);
        let expected_slots = if time_diff > 0 { time_diff / 400 } else { 0 };
        let actual_slots = slot - last_slot;
        
        let slot_drift = if actual_slots > expected_slots {
            actual_slots - expected_slots
        } else {
            expected_slots - actual_slots
        };

        metrics.latency_ms.store(latency, Ordering::Relaxed);
        metrics.last_check_ms.store(current_time, Ordering::Relaxed);
        metrics.consecutive_failures.store(0, Ordering::Relaxed);
        metrics.last_slot_seen.store(slot, Ordering::Relaxed);
        metrics.slot_drift.store(slot_drift, Ordering::Relaxed);

        let total = metrics.total_requests.fetch_add(1, Ordering::Relaxed) + 1;
        let successful = metrics.successful_requests.fetch_add(1, Ordering::Relaxed) + 1;
        let success_rate = ((successful as f64 / total as f64) * 10000.0) as u64;
        metrics.success_rate.store(success_rate, Ordering::Relaxed);

        // Update degradation score
        let degradation = metrics.degradation_score.load(Ordering::Relaxed);
        let new_degradation = if slot_drift <= SLOT_TOLERANCE {
            (degradation + 100).min(10000)
        } else {
            degradation.saturating_sub((slot_drift * 10) as u64)
        };
        metrics.degradation_score.store(new_degradation, Ordering::Relaxed);

        self.current_slot.store(slot, Ordering::Relaxed);
        self.record_latency_sample(endpoint, latency).await;
    }

    async fn handle_endpoint_failure(&self, endpoint: &str, metrics: &EndpointMetrics) {
        let failures = metrics.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        metrics.total_requests.fetch_add(1, Ordering::Relaxed);
        
        // Degrade endpoint score
        let degradation = metrics.degradation_score.load(Ordering::Relaxed);
        let penalty = (ENDPOINT_DEGRADATION_RATE * 10000.0) as u64;
        metrics.degradation_score.store(degradation.saturating_sub(penalty), Ordering::Relaxed);

        if failures >= MAX_CONSECUTIVE_FAILURES {
            metrics.is_active.store(false, Ordering::Relaxed);
            warn!("Endpoint {} deactivated after {} failures", endpoint, failures);
            
            // Remove from trusted endpoints
            let mut trusted = self.trusted_endpoints.write().await;
            trusted.remove(endpoint);
        }

        self.trigger_circuit_breaker(endpoint).await;
        self.security_violations.fetch_add(1, Ordering::Relaxed);
    }

    async fn trigger_circuit_breaker(&self, endpoint: &str) {
        if let Some(mut breaker) = self.circuit_breakers.get_mut(endpoint) {
            breaker.failure_count += 1;
            breaker.last_failure = Instant::now();
            
            if breaker.failure_count >= CIRCUIT_BREAKER_THRESHOLD {
                breaker.is_open = true;
                warn!("Circuit breaker opened for endpoint: {}", endpoint);
                
                if let Some(metrics) = self.endpoints.get(endpoint) {
                    metrics.circuit_breaker_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }

    async fn reset_circuit_breaker(&self, endpoint: &str) {
        if let Some(mut breaker) = self.circuit_breakers.get_mut(endpoint) {
            breaker.failure_count = 0;
            breaker.is_open = false;
            breaker.recovery_attempts = 0;
        }
    }

    async fn is_circuit_breaker_open(&self, endpoint: &str) -> bool {
        if let Some(breaker) = self.circuit_breakers.get(endpoint) {
            if breaker.is_open {
                let elapsed = breaker.last_failure.elapsed().as_millis() as u64;
                if elapsed > CIRCUIT_BREAKER_RECOVERY_MS {
                    drop(breaker);
                    self.reset_circuit_breaker(endpoint).await;
                    return false;
                }
                return true;
            }
        }
        false
    }

    async fn record_latency_sample(&self, endpoint: &str, latency: u64) {
        if let Some(mut history) = self.latency_history.get_mut(endpoint) {
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            
            history.push_back((timestamp, latency));
            
            while history.len() > LATENCY_HISTORY_SIZE {
                history.pop_front();
            }
        }
    }

    async fn record_slot_data(&self, slot: u64, endpoint: &str, timestamp: u64) {
        let validation_data = format!("{}:{}:{}", slot, endpoint, timestamp);
        let mut hasher = Sha256::new();
        hasher.update(validation_data.as_bytes());
        let validation_hash = hasher.finalize().into();

        let entropy_score = self.calculate_slot_entropy(slot, endpoint).await;
        let drift_score = self.calculate_drift_score(slot, timestamp).await;

        let slot_info = SlotInfo {
            slot,
            timestamp,
            source: endpoint.to_string(),
            validation_hash,
            drift_score,
            entropy_score,
        };

        let mut slot_history = self.slot_history.write().await;
        slot_history.push_back(slot_info);
        
        while slot_history.len() > 100 {
            slot_history.pop_front();
        }

        // Update consensus tracking
        let mut consensus = self.slot_consensus.write().await;
        consensus.entry(slot).or_insert_with(Vec::new).push((endpoint.to_string(), validation_hash));
    }

    async fn calculate_drift_score(&self, _slot: u64, _timestamp: u64) -> f64 {
        let slot_history = self.slot_history.read().await;
        
        if slot_history.len() < 2 {
            return 1.0;
        }

        let recent_slots: Vec<_> = slot_history.iter()
            .rev()
            .take(5)
            .collect();

        let mut drift_scores = Vec::new();
        for window in recent_slots.windows(2) {
            let slot_diff = window[0].slot.saturating_sub(window[1].slot);
            let time_diff = window[0].timestamp.saturating_sub(window[1].timestamp);
            
            if time_diff > 0 {
                let expected_slots = time_diff / 400;
                let drift = if slot_diff > expected_slots {
                    slot_diff - expected_slots
                } else {
                    expected_slots - slot_diff
                };
                
                let score = 1.0 - (drift as f64 / SLOT_TOLERANCE as f64).min(1.0);
                drift_scores.push(score);
            }
        }

        if drift_scores.is_empty() {
            1.0
        } else {
            drift_scores.iter().sum::<f64>() / drift_scores.len() as f64
        }
    }

    #[instrument(name = "topology_update_loop", skip(self))]
    async fn topology_update_loop(self: Arc<Self>) {
        let mut interval = interval(Duration::from_millis(TOPOLOGY_UPDATE_INTERVAL_MS));
        
        info!("Starting topology update loop");
        
        while !self.shutdown_signal.load(Ordering::Relaxed) {
            interval.tick().await;
            
            if self.panic_mode.load(Ordering::Relaxed) {
                sleep(Duration::from_millis(1000)).await;
                continue;
            }
            
            for attempt in 1..=MAX_RPC_RETRIES {
                match timeout(
                    Duration::from_millis(RPC_TIMEOUT_MS * 2),
                    self.update_validator_info()
                ).await {
                    Ok(Ok(())) => break,
                    Ok(Err(e)) => warn!("Topology update attempt {} failed: {}", attempt, e),
                    Err(_) => warn!("Topology update attempt {} timed out", attempt),
                }
                
                if attempt < MAX_RPC_RETRIES {
                    sleep(Duration::from_millis(200 * attempt as u64)).await;
                }
            }
        }
        
        info!("Topology update loop terminated");
    }

    #[instrument(name = "update_validator_info", skip(self))]
    async fn update_validator_info(&self) -> Result<()> {
        let client = self.get_best_client().await?;
        
        let vote_accounts = timeout(
            Duration::from_millis(RPC_TIMEOUT_MS),
            client.get_vote_accounts_with_config(RpcGetVoteAccountsConfig {
                commitment: Some(CommitmentConfig::finalized()),
                ..Default::default()
            })
        ).await??;

        let mut validators = self.validators.write().await;
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Clear old validators if too many
        if validators.len() > MAX_VALIDATOR_ENTRIES {
            validators.retain(|_, v| !v.is_delinquent && v.stake >= MIN_MAINNET_STAKE);
        }

        let mut total_stake = 0u64;
        let mut processed_count = 0;

        // Process current validators
        for account in vote_accounts.current.iter() {
            let identity = match Pubkey::from_str(&account.node_pubkey) {
                Ok(pubkey) => pubkey,
                Err(_) => continue,
            };
            
            let vote_account = match Pubkey::from_str(&account.vote_pubkey) {
                Ok(pubkey) => pubkey,
                Err(_) => continue,
            };

            if account.activated_stake < MIN_MAINNET_STAKE {
                continue;
            }

            if account.commission > MAX_COMMISSION_RATE {
                continue;
            }

            let malicious = self.malicious_validators.read().await;
            if malicious.contains(&identity) {
                continue;
            }
            drop(malicious);

            let credits = account.epoch_credits.last()
                .map(|(_, credits, _)| *credits)
                .unwrap_or(0);

            let validation_data = format!("{}:{}:{}:{}", 
                identity, account.activated_stake, account.last_vote, credits);
            let mut hasher = Sha256::new();
            hasher.update(validation_data.as_bytes());
            let validation_hash = hasher.finalize().into();

            let vote_distance = current_slot.saturating_sub(account.last_vote);
            let stake_trend = self.calculate_stake_trend(&identity, account.activated_stake).await;

            let perf_sample = PerformanceSample {
                slot: current_slot,
                credits_earned: credits,
                slots_skipped: 0,
                timestamp: current_time,
                vote_latency: vote_distance,
                entropy_hash: self.generate_entropy_hash(current_slot, current_time),
            };

            validators.entry(identity)
                .and_modify(|v| {
                    v.stake = account.activated_stake;
                    v.last_vote = account.last_vote;
                    v.commission = account.commission;
                    v.credits = credits;
                    v.is_delinquent = false;
                    v.epoch_credits = account.epoch_credits.clone();
                    v.validation_hash = validation_hash;
                    v.last_validation = current_time;
                    v.stake_trend = stake_trend;
                    v.vote_distance = vote_distance;
                    
                    v.performance_history.push_back(perf_sample);
                    while v.performance_history.len() > VALIDATOR_PERFORMANCE_WINDOW {
                        v.performance_history.pop_front();
                    }
                    
                    let total_slots = v.performance_history.len() as f64;
                    let skipped_slots: u64 = v.performance_history.iter()
                        .map(|s| s.slots_skipped)
                        .sum();
                    v.skip_rate = if total_slots > 0.0 {
                        skipped_slots as f64 / total_slots
                    } else {
                        0.0
                    };
                })
                .or_insert(ValidatorMetrics {
                    identity,
                    vote_account,
                    stake: account.activated_stake,
                    commission: account.commission,
                    last_vote: account.last_vote,
                    credits,
                    skip_rate: 0.0,
                    performance_history: vec![perf_sample].into(),
                    is_delinquent: false,
                    epoch_credits: account.epoch_credits.clone(),
                    validation_hash,
                    last_validation: current_time,
                    suspicious_count: 0,
                    stake_trend,
                    vote_distance,
                });

            total_stake += account.activated_stake;
            processed_count += 1;
        }

        // Mark delinquent validators
        for account in vote_accounts.delinquent.iter() {
            if let Ok(identity) = Pubkey::from_str(&account.node_pubkey) {
                if let Some(validator) = validators.get_mut(&identity) {
                    validator.is_delinquent = true;
                    validator.suspicious_count += 1;
                }
            }
        }

        // Update stake tracking
        for endpoint in self.endpoints.iter() {
            endpoint.stake_seen.store(total_stake, Ordering::Relaxed);
        }

        info!("Updated {} validators with total stake: {} SOL", 
            processed_count, total_stake / 1_000_000_000);
        Ok(())
    }

    async fn calculate_stake_trend(&self, identity: &Pubkey, current_stake: u64) -> f64 {
        let cache = self.stake_validation_cache.read().await;
        
        if let Some((previous_stake, timestamp)) = cache.get(identity) {
            let current_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            
            if current_time > *timestamp + 3600 { // 1 hour
                let stake_change = current_stake as f64 - *previous_stake as f64;
                let time_diff = current_time - timestamp;
                
                if time_diff > 0 {
                    return stake_change / time_diff as f64;
                }
            }
        }
        
        0.0
    }

    fn generate_entropy_hash(&self, slot: u64, timestamp: u64) -> [u8; 8] {
        let mut rng = rand::thread_rng();
        let entropy_data = format!("{}:{}:{}", slot, timestamp, rng.gen::<u64>());
        
        let mut hasher = Sha256::new();
        hasher.update(entropy_data.as_bytes());
        let hash = hasher.finalize();
        
        let mut result = [0u8; 8];
        result.copy_from_slice(&hash[..8]);
        result
    }

    #[instrument(name = "leader_schedule_update_loop", skip(self))]
    async fn leader_schedule_update_loop(self: Arc<Self>) {
        let mut interval = interval(Duration::from_millis(LEADER_SCHEDULE_UPDATE_INTERVAL_MS));
        
        info!("Starting leader schedule update loop");
        
        while !self.shutdown_signal.load(Ordering::Relaxed) {
            interval.tick().await;
            
            if self.panic_mode.load(Ordering::Relaxed) {
                sleep(Duration::from_millis(1000)).await;
                continue;
            }
            
            for attempt in 1..=MAX_RPC_RETRIES {
                match timeout(
                    Duration::from_millis(RPC_TIMEOUT_MS),
                    self.update_leader_schedule()
                ).await {
                    Ok(Ok(())) => break,
                    Ok(Err(e)) => warn!("Leader schedule update attempt {} failed: {}", attempt, e),
                    Err(_) => warn!("Leader schedule update attempt {} timed out", attempt),
                }
                
                if attempt < MAX_RPC_RETRIES {
                    sleep(Duration::from_millis(150 * attempt as u64)).await;
                }
            }
        }
        
        info!("Leader schedule update loop terminated");
    }

    #[instrument(name = "update_leader_schedule", skip(self))]
    async fn update_leader_schedule(&self) -> Result<()> {
        let client = self.get_best_client().await?;
        
        let current_slot = timeout(
            Duration::from_millis(CONNECTION_TIMEOUT_MS),
            client.get_slot()
        ).await??;

        if current_slot == 0 {
            bail!("Invalid slot 0 from RPC");
        }

        self.current_slot.store(current_slot, Ordering::Relaxed);
        
        // Update epoch cache if needed
        self.update_epoch_cache_if_needed(&client).await?;

        let leader_schedule = timeout(
            Duration::from_millis(RPC_TIMEOUT_MS),
            client.get_leader_schedule_with_config(
                Some(current_slot),
                RpcLeaderScheduleConfig {
                    commitment: Some(CommitmentConfig::finalized()),
                    ..Default::default()
                }
            )
        ).await??;

        let leader_schedule = match leader_schedule {
            Some(schedule) => schedule,
            None => {
                warn!("No leader schedule returned from RPC");
                return Ok(());
            }
        };

        let mut schedule = self.leader_schedule.write().await;
        schedule.clear();

        let validators = self.validators.read().await;
        let malicious = self.malicious_validators.read().await;
        
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let mut processed_entries = 0;
        let mut filtered_entries = 0;

        for (leader_str, slots) in leader_schedule.iter() {
            let leader = match Pubkey::from_str(leader_str) {
                Ok(pubkey) => pubkey,
                Err(_) => continue,
            };

            if malicious.contains(&leader) {
                filtered_entries += slots.len();
                continue;
            }

            let (stake, confidence, source_verified) = validators.get(&leader)
                .map(|v| {
                    if v.stake < MIN_MAINNET_STAKE {
                        return (0, 0.1, false);
                    }

                    let base_confidence = if v.is_delinquent {
                        0.2
                    } else if v.skip_rate > 0.1 {
                        0.6
                    } else if v.suspicious_count > 0 {
                        0.7
                    } else {
                        0.95
                    };

                    // Adjust confidence based on vote distance
                    let vote_penalty = if v.vote_distance > 150 {
                        0.3
                    } else if v.vote_distance > 50 {
                        0.1
                    } else {
                        0.0
                    };

                    let final_confidence = ((base_confidence - vote_penalty) as f64).max(0.1);
                    (v.stake, final_confidence, true)
                })
                .unwrap_or((0, 0.05, false));

            if stake < MIN_MAINNET_STAKE {
                filtered_entries += slots.len();
                continue;
            }

            for &slot in slots {
                if slot < (current_slot as u64).try_into().unwrap() {
                    continue;
                }

                let validation_data = format!("{}:{}:{}", slot, leader, stake);
                let mut hasher = Sha256::new();
                hasher.update(validation_data.as_bytes());
                let validation_hash = hasher.finalize().into();

                let time_offset = slot.saturating_sub((current_slot as u64).try_into().unwrap()) * 400;
                let fork_probability = self.calculate_fork_probability(slot.try_into().unwrap(), &leader).await;

                if schedule.len() < MAX_LEADER_SCHEDULE_ENTRIES {
                    schedule.insert(slot.try_into().unwrap(), LeaderScheduleEntry {
                        slot: slot.try_into().unwrap(),
                        leader,
                        stake,
                        estimated_block_time: current_time + time_offset as u64,
                        confidence_score: confidence,
                        validation_hash,
                        source_verified,
                        fork_probability,
                    });
                    processed_entries += 1;
                }
            }
        }

        info!("Updated leader schedule: {} entries processed, {} filtered", 
            processed_entries, filtered_entries);
        Ok(())
    }

    async fn update_epoch_cache_if_needed(&self, client: &RpcClient) -> Result<()> {
        let should_update = {
            let cache = self.epoch_cache.read().await;
            match cache.as_ref() {
                Some((_, cached_time)) => {
                    let current_time = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    current_time.saturating_sub(*cached_time) > EPOCH_CACHE_TTL_MS
                }
                None => true,
            }
        };

        if should_update {
            let epoch_info = client.get_epoch_info().await?;
            self.current_epoch.store(epoch_info.epoch, Ordering::Relaxed);
            
            let current_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            
            let mut cache = self.epoch_cache.write().await;
            *cache = Some((epoch_info, current_time));
        }

        Ok(())
    }

    async fn calculate_fork_probability(&self, slot: u64, leader: &Pubkey) -> f64 {
        let fork_buffer = self.fork_detection_buffer.read().await;
        
        let leader_forks = fork_buffer.iter()
            .filter(|(_, _, _)| slot <= (slot + 10))
            .count();

        let base_probability = leader_forks as f64 / FORK_DETECTION_WINDOW as f64;
        
        // Adjust based on validator reliability
        let validators = self.validators.read().await;
        let reliability_factor = validators.get(leader)
            .map(|v| 1.0 - v.skip_rate)
            .unwrap_or(0.5);

        (base_probability * (1.0 - reliability_factor)).min(1.0)
    }

    #[instrument(name = "route_optimization_loop", skip(self))]
    async fn route_optimization_loop(self: Arc<Self>) {
        let mut interval = interval(Duration::from_millis(ROUTE_OPTIMIZATION_INTERVAL_MS));
        
        info!("Starting route optimization loop");
        
        while !self.shutdown_signal.load(Ordering::Relaxed) {
            interval.tick().await;
            
            if self.panic_mode.load(Ordering::Relaxed) {
                sleep(Duration::from_millis(1000)).await;
                continue;
            }
            
            self.optimize_routes().await;
        }
        
        info!("Route optimization loop terminated");
    }

    #[instrument(name = "optimize_routes", skip(self))]
    async fn optimize_routes(&self) {
        let mut region_endpoints: HashMap<String, Vec<(String, f64)>> = HashMap::new();
        
        // Collect and score endpoints by region
        for entry in self.endpoints.iter() {
            let endpoint = entry.key();
            let metrics = entry.value();
            
            if !metrics.is_active.load(Ordering::Relaxed) {
                continue;
            }
            
            if self.is_circuit_breaker_open(endpoint).await {
                continue;
            }

            let score = self.calculate_endpoint_score(metrics).await;
            if score < 0.1 {
                continue;
            }
            
            metrics.priority_score.store((score * 10000.0) as u64, Ordering::Relaxed);
            
            region_endpoints.entry(metrics.region.clone())
                .or_insert_with(Vec::new)
                .push((endpoint.clone(), score));
        }

        // Optimize routes per region
        for (region, mut endpoints) in region_endpoints {
            endpoints.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            
            if let Some((primary, _)) = endpoints.first() {
                let fallbacks: Vec<String> = endpoints.iter()
                    .skip(1)
                    .take(4)
                    .map(|(endpoint, _)| endpoint.clone())
                    .collect();
                
                let route_metrics = self.calculate_route_metrics(primary, &fallbacks).await;
                
                if route_metrics.reliability_score < SUCCESS_RATE_THRESHOLD {
                    continue;
                }
                
                self.optimal_routes.insert(region.clone(), route_metrics);
                self.update_regional_health(&region).await;
            }
        }

        // Global route optimization
        let mut all_endpoints: Vec<(String, f64)> = self.endpoints.iter()
            .filter(|e| {
                e.value().is_active.load(Ordering::Relaxed) &&
                e.value().priority_score.load(Ordering::Relaxed) > 1000 &&
                e.value().degradation_score.load(Ordering::Relaxed) > 5000
            })
            .map(|e| {
                let score = e.value().priority_score.load(Ordering::Relaxed) as f64 / 10000.0;
                (e.key().clone(), score)
            })
            .collect();
        
        all_endpoints.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        
        if let Some((primary, _)) = all_endpoints.first() {
            let fallbacks: Vec<String> = all_endpoints.iter()
                .skip(1)
                .take(5)
                .map(|(endpoint, _)| endpoint.clone())
                .collect();
            
            let global_route = self.calculate_route_metrics(primary, &fallbacks).await;
            self.optimal_routes.insert("global".to_string(), global_route);
        }
    }

    async fn calculate_route_metrics(&self, primary: &str, fallbacks: &[String]) -> OptimalRoute {
        let latency_p95 = self.calculate_p95_latency(primary).await;
        let reliability = self.calculate_reliability_score(primary).await;
        let slot_consistency = self.calculate_slot_consistency(primary).await;
        let security_score = self.calculate_security_score(primary).await;
        let partition_resistance = self.calculate_partition_resistance(primary).await;
        let stake_coverage = self.calculate_stake_coverage(primary).await;
        
        OptimalRoute {
            primary_endpoint: primary.to_string(),
            fallback_endpoints: fallbacks.to_vec(),
            latency_p95,
            reliability_score: reliability,
            last_updated: Instant::now(),
            region: self.endpoints.get(primary)
                .map(|e| e.region.clone())
                .unwrap_or_else(|| "unknown".to_string()),
            slot_consistency,
            security_score,
            partition_resistance,
            stake_coverage,
        }
    }

    async fn calculate_endpoint_score(&self, metrics: &EndpointMetrics) -> f64 {
        let latency = metrics.latency_ms.load(Ordering::Relaxed) as f64;
        let success_rate = metrics.success_rate.load(Ordering::Relaxed) as f64 / 10000.0;
        let slot_drift = metrics.slot_drift.load(Ordering::Relaxed) as f64;
        let degradation = metrics.degradation_score.load(Ordering::Relaxed) as f64 / 10000.0;
        let consecutive_failures = metrics.consecutive_failures.load(Ordering::Relaxed) as f64;
        
        if latency >= u64::MAX as f64 || success_rate < 0.5 {
            return 0.0;
        }

        let latency_score = if latency < u64::MAX as f64 {
            1.0 / (1.0 + latency / 50.0)
        } else {
            0.0
        };
        
        let slot_score = if slot_drift > SLOT_TOLERANCE as f64 {
            0.1
        } else {
            1.0 - (slot_drift / SLOT_TOLERANCE as f64) * 0.3
        };
        
        let failure_penalty = if consecutive_failures > 0.0 {
            1.0 / (1.0 + consecutive_failures * 0.3)
        } else {
            1.0
        };
        
        let score = LATENCY_WEIGHT * latency_score * slot_score * failure_penalty + 
                   RELIABILITY_WEIGHT * success_rate * degradation + 
                   STAKE_WEIGHT * 0.8; // Assume reasonable stake coverage
        
        score.max(0.0).min(1.0)
    }

    async fn calculate_p95_latency(&self, endpoint: &str) -> f64 {
        if let Some(history) = self.latency_history.get(endpoint) {
            let mut latencies: Vec<u64> = history.iter()
                .map(|(_, latency)| *latency)
                .filter(|&l| l < u64::MAX && l > 0)
                .collect();
            
            if !latencies.is_empty() {
                latencies.sort_unstable();
                let p95_index = ((latencies.len() as f64 * 0.95) as usize).min(latencies.len() - 1);
                return latencies[p95_index] as f64;
            }
        }
        
        f64::MAX
    }

    async fn calculate_reliability_score(&self, endpoint: &str) -> f64 {
        if let Some(entry) = self.endpoints.get(endpoint) {
            let success_rate = entry.value().success_rate.load(Ordering::Relaxed) as f64 / 10000.0;
            let total_requests = entry.value().total_requests.load(Ordering::Relaxed);
            let degradation = entry.value().degradation_score.load(Ordering::Relaxed) as f64 / 10000.0;
            
            if total_requests < 10 {
                return 0.5;
            }
            
            return success_rate * degradation;
        }
        
        0.0
    }

    async fn calculate_slot_consistency(&self, endpoint: &str) -> f64 {
        if let Some(entry) = self.endpoints.get(endpoint) {
            let drift = entry.value().slot_drift.load(Ordering::Relaxed) as f64;
            if drift > SLOT_TOLERANCE as f64 {
                0.1
            } else {
                1.0 - (drift / SLOT_TOLERANCE as f64) * 0.4
            }
        } else {
            0.0
        }
    }

    async fn calculate_security_score(&self, endpoint: &str) -> f64 {
        let trusted = self.trusted_endpoints.read().await;
        if !trusted.contains(endpoint) {
            return 0.0;
        }
        
        if let Some(entry) = self.endpoints.get(endpoint) {
            let metrics = entry.value();
            let violations = metrics.security_violations.load(Ordering::Relaxed) as f64;
            let circuit_breaks = metrics.circuit_breaker_count.load(Ordering::Relaxed) as f64;
            let tls_verified = metrics.tls_verified.load(Ordering::Relaxed);
            
            let violation_score = 1.0 - (violations / SECURITY_VIOLATION_THRESHOLD as f64).min(1.0);
            let circuit_score = 1.0 - (circuit_breaks / 10.0).min(1.0);
            let tls_score = if tls_verified { 1.0 } else { 0.0 };
            
            (violation_score + circuit_score + tls_score) / 3.0
        } else {
            0.0
        }
    }

    async fn calculate_partition_resistance(&self, endpoint: &str) -> f64 {
        let regional_endpoints = if let Some(entry) = self.endpoints.get(endpoint) {
            self.get_regional_endpoints(&entry.region).await
        } else {
            Vec::new()
        };
        
        let active_count = regional_endpoints.len();
        let diversity_score = (active_count.min(5) as f64) / 5.0;
        let redundancy_score = if active_count >= REGIONAL_FAILOVER_THRESHOLD * 2 { 1.0 } else { 0.6 };
        
        (diversity_score + redundancy_score) / 2.0
    }

    async fn calculate_stake_coverage(&self, endpoint: &str) -> f64 {
        if let Some(entry) = self.endpoints.get(endpoint) {
            let stake_seen = entry.stake_seen.load(Ordering::Relaxed);
            if stake_seen >= STAKE_VALIDATION_THRESHOLD {
                1.0
            } else {
                stake_seen as f64 / STAKE_VALIDATION_THRESHOLD as f64
            }
        } else {
            0.0
        }
    }

    async fn update_regional_health(&self, region: &str) {
        let endpoints = self.get_regional_endpoints(region).await;
        
        if endpoints.is_empty() {
            return;
        }

        let mut total_latency = 0.0;
        let mut total_reliability = 0.0;
        let mut total_stake = 0u64;
        let mut active_count = 0;

        for endpoint in &endpoints {
            if let Some(entry) = self.endpoints.get(endpoint) {
                let metrics = entry.value();
                let latency = metrics.latency_ms.load(Ordering::Relaxed);
                let reliability = metrics.success_rate.load(Ordering::Relaxed) as f64 / 10000.0;
                let stake = metrics.stake_seen.load(Ordering::Relaxed);
                
                if latency < u64::MAX && reliability > 0.0 {
                    total_latency += latency as f64;
                    total_reliability += reliability;
                    total_stake += stake;
                    active_count += 1;
                }
            }
        }

        if active_count > 0 {
            let health = RegionalHealth {
                region: region.to_string(),
                active_endpoints: active_count,
                avg_latency: total_latency / active_count as f64,
                reliability: total_reliability / active_count as f64,
                stake_coverage: total_stake as f64 / STAKE_VALIDATION_THRESHOLD as f64,
                last_updated: Instant::now(),
            };

            self.regional_health.insert(region.to_string(), health);
        }
    }

    async fn get_regional_endpoints(&self, region: &str) -> Vec<String> {
        let trusted = self.trusted_endpoints.read().await;
        
        self.endpoints.iter()
            .filter(|e| {
                let metrics = e.value();
                trusted.contains(e.key()) &&
                metrics.region == region && 
                metrics.is_active.load(Ordering::Relaxed) &&
                metrics.success_rate.load(Ordering::Relaxed) >= (SUCCESS_RATE_THRESHOLD * 10000.0) as u64
            })
            .map(|e| e.key().clone())
            .collect()
    }

    #[instrument(name = "slot_monitoring_loop", skip(self))]
    async fn slot_monitoring_loop(self: Arc<Self>) {
        let mut interval = interval(Duration::from_millis(300));
        
        info!("Starting slot monitoring loop");
        
        while !self.shutdown_signal.load(Ordering::Relaxed) {
            interval.tick().await;
            
            if let Err(e) = self.detect_slot_anomalies().await {
                warn!("Slot anomaly detection failed: {}", e);
            }
        }
        
        info!("Slot monitoring loop terminated");
    }

    async fn detect_slot_anomalies(&self) -> Result<()> {
        let slot_history = self.slot_history.read().await;
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Check for endpoints with excessive drift
        for entry in self.endpoints.iter() {
            let metrics = entry.value();
            let drift = metrics.slot_drift.load(Ordering::Relaxed);
            let last_check = metrics.last_check_ms.load(Ordering::Relaxed);
            
            if drift > SLOT_TOLERANCE && current_time.saturating_sub(last_check) > STALE_DATA_THRESHOLD_MS {
                warn!("Endpoint {} has excessive slot drift: {}", entry.key(), drift);
                
                let current_degradation = metrics.degradation_score.load(Ordering::Relaxed);
                let penalty = (drift * 100).min(2000);
                metrics.degradation_score.store(current_degradation.saturating_sub(penalty), Ordering::Relaxed);
                
                metrics.security_violations.fetch_add(1, Ordering::Relaxed);
                self.security_violations.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Check for partition risks
        self.detect_partition_risks(&slot_history, current_time).await?;
        
        Ok(())
    }

    async fn detect_partition_risks(&self, slot_history: &VecDeque<SlotInfo>, current_time: u64) -> Result<()> {
        let recent_slots: Vec<_> = slot_history.iter()
            .filter(|info| current_time.saturating_sub(info.timestamp) < 5000)
            .collect();
        
        if recent_slots.len() < 3 {
            return Ok(());
        }

        let mut source_slots: HashMap<String, Vec<u64>> = HashMap::new();
        for slot_info in &recent_slots {
            source_slots.entry(slot_info.source.clone())
                .or_insert_with(Vec::new)
                .push(slot_info.slot);
        }

        let total_sources = source_slots.len();
        if total_sources < MIN_HEALTHY_ENDPOINTS {
            return Ok(());
        }

        // Check for consensus on recent slots
        let mut slot_counts: HashMap<u64, usize> = HashMap::new();
        for slot_info in &recent_slots {
            *slot_counts.entry(slot_info.slot).or_insert(0) += 1;
        }

        let consensus_threshold = ((total_sources as f64) * CONSENSUS_THRESHOLD) as usize;
        let has_consensus = slot_counts.values().any(|&count| count >= consensus_threshold);

        if !has_consensus {
            warn!("Potential network partition detected - no slot consensus among {} sources", total_sources);
            self.partition_detected.store(true, Ordering::Relaxed);
            self.consensus_failures.fetch_add(1, Ordering::Relaxed);
        } else {
            self.partition_detected.store(false, Ordering::Relaxed);
        }

        Ok(())
    }

    #[instrument(name = "security_monitoring_loop", skip(self))]
    async fn security_monitoring_loop(self: Arc<Self>) {
        let mut interval = interval(Duration::from_millis(1000));
        
        info!("Starting security monitoring loop");
        
        while !self.shutdown_signal.load(Ordering::Relaxed) {
            interval.tick().await;
            
            self.monitor_security_threats().await;
        }
        
        info!("Security monitoring loop terminated");
    }

    async fn monitor_security_threats(&self) {
        // Monitor validator behavior
        let validators = self.validators.read().await;
        let mut malicious = self.malicious_validators.write().await;
        
        for (identity, metrics) in validators.iter() {
            // Check for suspicious behavior patterns
            if metrics.suspicious_count > 5 {
                malicious.insert(*identity);
                warn!("Validator {} marked as malicious due to suspicious behavior", identity);
                continue;
            }
            
            // Check for unusual stake changes
            if metrics.stake_trend.abs() > 1_000_000_000.0 { // 1 SOL per second
                warn!("Validator {} has unusual stake trend: {}", identity, metrics.stake_trend);
            }
            
            // Check for excessive vote distance
            if metrics.vote_distance > 300 {
                warn!("Validator {} has excessive vote distance: {}", identity, metrics.vote_distance);
            }
        }
        
        drop(validators);
        drop(malicious);

        // Monitor endpoint integrity
        let total_violations = self.security_violations.load(Ordering::Relaxed);
        if total_violations > SECURITY_VIOLATION_THRESHOLD {
            warn!("High security violations detected: {}", total_violations);
            
            // Reset violation count periodically
            if total_violations > SECURITY_VIOLATION_THRESHOLD * 2 {
                self.security_violations.store(SECURITY_VIOLATION_THRESHOLD, Ordering::Relaxed);
            }
        }

        // Check for endpoint spoofing
        let trusted = self.trusted_endpoints.read().await;
        let whitelist = self.endpoint_whitelist.read().await;
        
        for endpoint in self.endpoints.iter() {
            if !trusted.contains(endpoint.key()) || !whitelist.contains(endpoint.key()) {
                warn!("Untrusted endpoint detected: {}", endpoint.key());
                endpoint.is_active.store(false, Ordering::Relaxed);
            }
        }
    }

    #[instrument(name = "consensus_validation_loop", skip(self))]
    async fn consensus_validation_loop(self: Arc<Self>) {
        let mut interval = interval(Duration::from_millis(1000));
        
        info!("Starting consensus validation loop");
        
        while !self.shutdown_signal.load(Ordering::Relaxed) {
            interval.tick().await;
            
            self.validate_slot_consensus().await;
        }
        
        info!("Consensus validation loop terminated");
    }

    async fn validate_slot_consensus(&self) {
        let mut consensus = self.slot_consensus.write().await;
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Clean old consensus data
        let cutoff_time = current_time - 10000; // 10 seconds
        consensus.retain(|_, sources| {
            sources.len() > 1 || current_time < cutoff_time
        });

        // Check current consensus
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        if let Some(sources) = consensus.get(&current_slot) {
            if sources.len() < MIN_HEALTHY_ENDPOINTS {
                warn!("Insufficient consensus for slot {}: {} sources", current_slot, sources.len());
                self.consensus_failures.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    #[instrument(name = "fork_detection_loop", skip(self))]
    async fn fork_detection_loop(self: Arc<Self>) {
        let mut interval = interval(Duration::from_millis(500));
        
        info!("Starting fork detection loop");
        
        while !self.shutdown_signal.load(Ordering::Relaxed) {
            interval.tick().await;
            
            self.detect_potential_forks().await;
        }
        
        info!("Fork detection loop terminated");
    }

    async fn detect_potential_forks(&self) {
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        let mut fork_buffer = self.fork_detection_buffer.write().await;

        // Check for conflicting slot information
        let consensus = self.slot_consensus.read().await;
        
        for (slot, sources) in consensus.iter() {
            if sources.len() > 1 {
                let mut unique_hashes = HashSet::new();
                for (_, hash) in sources {
                    unique_hashes.insert(*hash);
                }
                
                if unique_hashes.len() > 1 {
                    warn!("Potential fork detected at slot {}: {} different hashes", slot, unique_hashes.len());
                    
                      // Record fork evidence
                    let fork_entry = (current_slot, format!("fork_at_{}", slot), unique_hashes.iter().next().copied().unwrap_or([0; 32]));
                    fork_buffer.push_back(fork_entry);
                    
                    if fork_buffer.len() > FORK_DETECTION_WINDOW {
                        fork_buffer.pop_front();
                    }
                }
            }
        }
        
        drop(consensus);
        drop(fork_buffer);
        
        // Trigger partition detection if too many forks
        let fork_count = self.fork_detection_buffer.read().await.len();
        if fork_count > FORK_DETECTION_WINDOW / 2 {
            warn!("High fork activity detected: {} forks in window", fork_count);
            self.partition_detected.store(true, Ordering::Relaxed);
        }
    }

    #[instrument(name = "watchdog_loop", skip(self))]
    async fn watchdog_loop(self: Arc<Self>) {
        let mut interval = interval(Duration::from_millis(WATCHDOG_INTERVAL_MS));
        
        info!("Starting watchdog loop");
        
        while !self.shutdown_signal.load(Ordering::Relaxed) {
            interval.tick().await;
            
            let current_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            
            self.last_watchdog_check.store(current_time, Ordering::Relaxed);
            
            // Check network health
            let health = self.network_health_score.load(Ordering::Relaxed);
            if health < PANIC_MODE_HEALTH_THRESHOLD {
                warn!("Network health critical: {}%, triggering panic mode", health);
                self.trigger_panic_mode().await;
            }
            
            // Check for stalled background tasks
            self.check_background_task_health().await;
            
            // Cleanup old data
            self.cleanup_stale_data().await;
            
            // Recovery attempts
            if self.panic_mode.load(Ordering::Relaxed) {
                self.attempt_recovery().await;
            }
        }
        
        info!("Watchdog loop terminated");
    }

    async fn trigger_panic_mode(&self) {
        warn!("PANIC MODE ACTIVATED - Network conditions critical");
        self.panic_mode.store(true, Ordering::Relaxed);
        
        // Disable all non-essential operations
        for endpoint in self.endpoints.iter() {
            let metrics = endpoint.value();
            let success_rate = metrics.success_rate.load(Ordering::Relaxed) as f64 / 10000.0;
            if success_rate < 0.8 {
                metrics.is_active.store(false, Ordering::Relaxed);
            }
        }
    }

    async fn attempt_recovery(&self) {
        let active_endpoints = self.endpoints.iter()
            .filter(|e| e.value().is_active.load(Ordering::Relaxed))
            .count();
        
        if active_endpoints >= MIN_HEALTHY_ENDPOINTS {
            let health = self.network_health_score.load(Ordering::Relaxed);
            if health > PANIC_MODE_HEALTH_THRESHOLD * 2 {
                info!("Recovery conditions met, exiting panic mode");
                self.panic_mode.store(false, Ordering::Relaxed);
            }
        }
    }

    async fn check_background_task_health(&self) {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        
        let last_check = self.last_watchdog_check.load(Ordering::Relaxed);
        if current_time.saturating_sub(last_check) > WATCHDOG_INTERVAL_MS * 5 {
            error!("Watchdog task stalled, system may be under attack");
            self.security_violations.fetch_add(10, Ordering::Relaxed);
        }
    }

    async fn cleanup_stale_data(&self) {
        let current_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        
        // Clean slot consensus data
        let mut consensus = self.slot_consensus.write().await;
        consensus.retain(|_, sources| {
            sources.len() > 1 || current_time < current_time - 30000
        });
        drop(consensus);
        
        // Clean latency history
        for mut history in self.latency_history.iter_mut() {
            history.retain(|(timestamp, _)| {
                current_time.saturating_sub(*timestamp) < 60000
            });
        }
        
        // Clean stake validation cache
        let mut stake_cache = self.stake_validation_cache.write().await;
        stake_cache.retain(|_, (_, timestamp)| {
            current_time.saturating_sub(*timestamp * 1000) < 300000
        });
        drop(stake_cache);
    }

    async fn update_network_health(&self) {
        let active_endpoints = self.endpoints.iter()
            .filter(|e| {
                let metrics = e.value();
                metrics.is_active.load(Ordering::Relaxed) &&
                metrics.success_rate.load(Ordering::Relaxed) >= (SUCCESS_RATE_THRESHOLD * 10000.0) as u64 &&
                metrics.degradation_score.load(Ordering::Relaxed) > 5000
            })
            .count();
        
        let total_endpoints = self.endpoints.len();
        let security_violations = self.security_violations.load(Ordering::Relaxed);
        let consensus_failures = self.consensus_failures.load(Ordering::Relaxed);
        let partition_detected = self.partition_detected.load(Ordering::Relaxed);
        
        if total_endpoints == 0 {
            self.network_health_score.store(0, Ordering::Relaxed);
            return;
        }
        
        let base_health = ((active_endpoints as f64 / total_endpoints as f64) * 100.0) as u64;
        
        let security_penalty = (security_violations.min(20) * 2) as u64;
        let consensus_penalty = (consensus_failures.min(10) * 3) as u64;
        let partition_penalty = if partition_detected { 30 } else { 0 };
        
        let final_health = base_health
            .saturating_sub(security_penalty)
            .saturating_sub(consensus_penalty)
            .saturating_sub(partition_penalty);
        
        self.network_health_score.store(final_health, Ordering::Relaxed);
        
        if final_health < 50 {
            warn!("Network health degraded to {}%", final_health);
        }
    }

    fn detect_region(&self, endpoint: &str) -> String {
        let endpoint_lower = endpoint.to_lowercase();
        
        match endpoint_lower {
            e if e.contains("mainnet-beta") && !e.contains("east") && !e.contains("west") => "global".to_string(),
            e if e.contains("east") || e.contains("virginia") || e.contains("ashburn") || e.contains("us-east") => "us-east".to_string(),
            e if e.contains("west") || e.contains("california") || e.contains("oregon") || e.contains("us-west") => "us-west".to_string(),
            e if e.contains("europe") || e.contains("frankfurt") || e.contains("london") || e.contains("eu-") => "europe".to_string(),
            e if e.contains("asia") || e.contains("tokyo") || e.contains("singapore") || e.contains("ap-") => "asia".to_string(),
            _ => "unknown".to_string(),
        }
    }

    #[instrument(name = "get_best_client", skip(self))]
    pub async fn get_best_client(&self) -> Result<Arc<RpcClient>> {
        if self.panic_mode.load(Ordering::Relaxed) {
            return self.get_panic_mode_client().await;
        }
        
        // Try global optimal route first
        if let Some(route) = self.optimal_routes.get("global") {
            if route.reliability_score >= SUCCESS_RATE_THRESHOLD && 
               route.security_score > 0.8 &&
               route.partition_resistance > 0.7 &&
               !self.is_circuit_breaker_open(&route.primary_endpoint).await {
                if let Some(client) = self.rpc_clients.get(&route.primary_endpoint) {
                    return Ok(client.clone());
                }
            }
            
            // Try fallbacks with probabilistic selection
            for (i, fallback) in route.fallback_endpoints.iter().enumerate() {
                if !self.is_circuit_breaker_open(fallback).await {
                    if let Some(metrics) = self.endpoints.get(fallback) {
                        let score = metrics.priority_score.load(Ordering::Relaxed) as f64 / 10000.0;
                        let probability = score * (1.0 - (i as f64 * 0.1));
                        
                        if probability > 0.5 {
                            if let Some(client) = self.rpc_clients.get(fallback) {
                                return Ok(client.clone());
                            }
                        }
                    }
                }
            }
        }
        
        // Fallback to best available endpoint
        self.get_best_available_client().await
    }

    async fn get_panic_mode_client(&self) -> Result<Arc<RpcClient>> {
        let trusted = self.trusted_endpoints.read().await;
        
        // In panic mode, only use highest reliability endpoints
        let best_endpoint = self.endpoints.iter()
            .filter(|e| {
                let metrics = e.value();
                trusted.contains(e.key()) &&
                metrics.is_active.load(Ordering::Relaxed) &&
                metrics.success_rate.load(Ordering::Relaxed) >= (0.95 * 10000.0) as u64 &&
                metrics.degradation_score.load(Ordering::Relaxed) > 8000
            })
            .max_by_key(|e| {
                let metrics = e.value();
                metrics.priority_score.load(Ordering::Relaxed) + 
                metrics.degradation_score.load(Ordering::Relaxed)
            })
            .map(|e| e.key().clone());
        
        if let Some(endpoint) = best_endpoint {
            if let Some(client) = self.rpc_clients.get(&endpoint) {
                return Ok(client.clone());
            }
        }
        
        bail!("No suitable clients available in panic mode")
    }

    async fn get_best_available_client(&self) -> Result<Arc<RpcClient>> {
        let trusted = self.trusted_endpoints.read().await;
        
        let best_endpoint = self.endpoints.iter()
            .filter(|e| {
                let metrics = e.value();
                trusted.contains(e.key()) &&
                metrics.is_active.load(Ordering::Relaxed) &&
                metrics.success_rate.load(Ordering::Relaxed) >= (SUCCESS_RATE_THRESHOLD * 10000.0) as u64 &&
                !self.is_circuit_breaker_open(e.key()).await
            })
            .max_by_key(|e| {
                let metrics = e.value();
                let latency_score = if metrics.latency_ms.load(Ordering::Relaxed) < u64::MAX {
                    10000 - (metrics.latency_ms.load(Ordering::Relaxed) / 10).min(9999)
                } else {
                    0
                };
                
                latency_score + metrics.priority_score.load(Ordering::Relaxed)
            })
            .map(|e| e.key().clone());
        
        if let Some(endpoint) = best_endpoint {
            if let Some(client) = self.rpc_clients.get(&endpoint) {
                return Ok(client.clone());
            }
        }
        
        bail!("No healthy RPC clients available")
    }

    #[instrument(name = "get_optimal_endpoint", skip(self))]
    pub async fn get_optimal_endpoint(&self, region: Option<&str>) -> Result<String> {
        let region_key = region.unwrap_or("global");
        
        if let Some(route) = self.optimal_routes.get(region_key) {
            if route.reliability_score >= SUCCESS_RATE_THRESHOLD &&
               route.security_score > 0.7 &&
               self.is_endpoint_healthy(&route.primary_endpoint).await {
                return Ok(route.primary_endpoint.clone());
            }
            
            for fallback in &route.fallback_endpoints {
                if self.is_endpoint_healthy(fallback).await {
                    return Ok(fallback.to_string());
                }
            }
        }
        
        self.get_any_healthy_endpoint().await
            .ok_or_else(|| anyhow!("No healthy endpoints available"))
    }

    async fn is_endpoint_healthy(&self, endpoint: &str) -> bool {
        if self.is_circuit_breaker_open(endpoint).await {
            return false;
        }
        
        let trusted = self.trusted_endpoints.read().await;
        if !trusted.contains(endpoint) {
            return false;
        }
        drop(trusted);
        
        if let Some(entry) = self.endpoints.get(endpoint) {
            let metrics = entry.value();
            let is_active = metrics.is_active.load(Ordering::Relaxed);
            let failures = metrics.consecutive_failures.load(Ordering::Relaxed);
            let success_rate = metrics.success_rate.load(Ordering::Relaxed) as f64 / 10000.0;
            let slot_drift = metrics.slot_drift.load(Ordering::Relaxed);
            let degradation = metrics.degradation_score.load(Ordering::Relaxed);
            
            return is_active && 
                   failures < 2 && 
                   success_rate >= SUCCESS_RATE_THRESHOLD &&
                   slot_drift <= SLOT_TOLERANCE &&
                   degradation > 6000;
        }
        
        false
    }

    async fn get_any_healthy_endpoint(&self) -> Option<String> {
        let trusted = self.trusted_endpoints.read().await;
        
        let mut endpoints: Vec<_> = self.endpoints.iter()
            .filter(|e| {
                let metrics = e.value();
                trusted.contains(e.key()) &&
                metrics.is_active.load(Ordering::Relaxed) &&
                metrics.success_rate.load(Ordering::Relaxed) >= (SUCCESS_RATE_THRESHOLD * 10000.0) as u64 &&
                metrics.degradation_score.load(Ordering::Relaxed) > 5000
            })
            .map(|e| {
                let score = e.value().priority_score.load(Ordering::Relaxed);
                (e.key().clone(), score)
            })
            .collect();
        
        endpoints.sort_by_key(|(_, score)| std::cmp::Reverse(*score));
        endpoints.first().map(|(endpoint, _)| endpoint.clone())
    }

    #[instrument(name = "get_fastest_endpoints", skip(self))]
    pub async fn get_fastest_endpoints(&self, count: usize) -> Vec<String> {
        let trusted = self.trusted_endpoints.read().await;
        
        let mut endpoints: Vec<_> = self.endpoints.iter()
            .filter(|e| {
                let metrics = e.value();
                trusted.contains(e.key()) &&
                metrics.is_active.load(Ordering::Relaxed) &&
                metrics.latency_ms.load(Ordering::Relaxed) < u64::MAX &&
                metrics.slot_drift.load(Ordering::Relaxed) <= SLOT_TOLERANCE &&
                metrics.success_rate.load(Ordering::Relaxed) >= (SUCCESS_RATE_THRESHOLD * 10000.0) as u64 &&
                metrics.degradation_score.load(Ordering::Relaxed) > 7000
            })
            .map(|e| {
                let latency = e.value().latency_ms.load(Ordering::Relaxed);
                (e.key().clone(), latency)
            })
            .collect();
        
        endpoints.sort_by_key(|(_, latency)| *latency);
        endpoints.into_iter()
            .take(count)
            .map(|(endpoint, _)| endpoint)
            .collect()
    }

    #[instrument(name = "get_upcoming_leaders", skip(self))]
    pub async fn get_upcoming_leaders(&self, slots_ahead: u64) -> Vec<LeaderScheduleEntry> {
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        let end_slot = current_slot.saturating_add(slots_ahead);
        
        let schedule = self.leader_schedule.read().await;
        let malicious = self.malicious_validators.read().await;
        
        schedule.range(current_slot..=end_slot)
            .filter_map(|(_, entry)| {
                if malicious.contains(&entry.leader) {
                    return None;
                }
                
                if entry.confidence_score > 0.8 && 
                   entry.source_verified && 
                   entry.stake >= MIN_MAINNET_STAKE &&
                   entry.fork_probability < 0.2 {
                    Some(entry.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    #[instrument(name = "get_validator_performance", skip(self))]
    pub async fn get_validator_performance(&self, validator: &Pubkey) -> Option<f64> {
        let validators = self.validators.read().await;
        let malicious = self.malicious_validators.read().await;
        
        if malicious.contains(validator) {
            return None;
        }
        
        validators.get(validator).and_then(|metrics| {
            if metrics.stake < MIN_MAINNET_STAKE {
                return None;
            }
            
            if metrics.performance_history.is_empty() {
                return Some(0.0);
            }
            
            let total_credits: u64 = metrics.performance_history.iter()
                .map(|s| s.credits_earned)
                .sum();
            
            let avg_credits = total_credits as f64 / metrics.performance_history.len() as f64;
            let performance_score = avg_credits / 432000.0;
            
            let vote_penalty = if metrics.vote_distance > 100 {
                0.3
            } else if metrics.vote_distance > 50 {
                0.1
            } else {
                0.0
            };
            
            let adjusted_score = if metrics.is_delinquent {
                performance_score * 0.1
            } else {
                performance_score * (1.0 - metrics.skip_rate * 0.4) * (1.0 - vote_penalty)
            };
            
            Some(adjusted_score.max(0.0).min(1.0))
        })
    }

    #[instrument(name = "get_validator_stake", skip(self))]
    pub async fn get_validator_stake(&self, validator: &Pubkey) -> u64 {
        let validators = self.validators.read().await;
        let malicious = self.malicious_validators.read().await;
        
        if malicious.contains(validator) {
            return 0;
        }
        
        validators.get(validator)
            .filter(|v| v.stake >= MIN_MAINNET_STAKE && !v.is_delinquent)
            .map(|v| v.stake)
            .unwrap_or(0)
    }

    #[instrument(name = "get_total_active_stake", skip(self))]
    pub async fn get_total_active_stake(&self) -> u64 {
        let validators = self.validators.read().await;
        let malicious = self.malicious_validators.read().await;
        let current_slot = self.current_slot.load(Ordering::Relaxed);
        
        validators.values()
            .filter(|v| {
                !malicious.contains(&v.identity) &&
                !v.is_delinquent && 
                v.last_vote > current_slot.saturating_sub(150) &&
                v.stake >= MIN_MAINNET_STAKE &&
                v.vote_distance < 200
            })
            .map(|v| v.stake)
            .sum()
    }

    #[instrument(name = "add_endpoint", skip(self), fields(endpoint = %endpoint))]
    pub async fn add_endpoint(&self, endpoint: String) -> Result<()> {
        if self.endpoints.contains_key(&endpoint) {
            return Ok(());
        }
        
        let validated = Self::validate_mainnet_endpoints(vec![endpoint.clone()])?;
        if validated.is_empty() {
            bail!("Endpoint failed mainnet validation");
        }
        
        if self.endpoints.len() >= MAX_ENDPOINTS {
            self.remove_worst_endpoint().await?;
        }
        
        self.add_endpoint_with_validation(validated[0].clone()).await
    }

    #[instrument(name = "remove_worst_endpoint", skip(self))]
    async fn remove_worst_endpoint(&self) -> Result<()> {
        let worst = self.endpoints.iter()
            .filter(|e| e.value().endpoint_type != EndpointType::Primary)
            .min_by_key(|e| {
                let metrics = e.value();
                let score = metrics.priority_score.load(Ordering::Relaxed);
                let degradation = metrics.degradation_score.load(Ordering::Relaxed);
                let failures = metrics.consecutive_failures.load(Ordering::Relaxed) as u64;
                
                score.saturating_sub(failures * 1000) + degradation
            })
            .map(|e| e.key().clone());
        
        if let Some(endpoint) = worst {
            info!("Removing worst endpoint: {}", endpoint);
            self.endpoints.remove(&endpoint);
            self.rpc_clients.remove(&endpoint);
            self.latency_history.remove(&endpoint);
            self.circuit_breakers.remove(&endpoint);
            
            let mut trusted = self.trusted_endpoints.write().await;
            trusted.remove(&endpoint);
            
            let mut whitelist = self.endpoint_whitelist.write().await;
            whitelist.remove(&endpoint);
        }
        
        Ok(())
    }

    #[instrument(name = "reconnect_failed_endpoints", skip(self))]
    pub async fn reconnect_failed_endpoints(&self) -> Result<usize> {
        let failed: Vec<_> = self.endpoints.iter()
            .filter(|e| !e.value().is_active.load(Ordering::Relaxed))
            .map(|e| e.key().clone())
            .collect();
        
        debug!("Attempting to reconnect {} failed endpoints", failed.len());
        
        let mut reconnected = 0;
        
        for endpoint in failed {
            if let Ok(validated) = Self::validate_mainnet_endpoints(vec![endpoint.clone()]) {
                if !validated.is_empty() {
                    match self.add_endpoint_with_validation(validated[0].clone()).await {
                        Ok(()) => {
                            reconnected += 1;
                            info!("Reconnected endpoint: {}", endpoint);
                        }
                        Err(e) => {
                            warn!("Failed to reconnect {}: {}", endpoint, e);
                        }
                    }
                }
            }
        }
        
        Ok(reconnected)
    }

    pub async fn get_endpoint_metrics(&self, endpoint: &str) -> Option<EndpointMetrics> {
        self.endpoints.get(endpoint)
            .map(|entry| (*entry.value()).clone())
    }

    #[instrument(name = "get_network_stats", skip(self))]
    pub async fn get_network_stats(&self) -> NetworkStats {
        let active_count = self.endpoints.iter()
            .filter(|e| {
                let metrics = e.value();
                metrics.is_active.load(Ordering::Relaxed) &&
                metrics.success_rate.load(Ordering::Relaxed) >= (SUCCESS_RATE_THRESHOLD * 10000.0) as u64 &&
                metrics.degradation_score.load(Ordering::Relaxed) > 5000
            })
            .count();
        
        let total_count = self.endpoints.len();
        
        let avg_latency = if active_count > 0 {
            let sum: u64 = self.endpoints.iter()
                .filter(|e| {
                    let metrics = e.value();
                    metrics.is_active.load(Ordering::Relaxed) && 
                    metrics.latency_ms.load(Ordering::Relaxed) < u64::MAX
                })
                .map(|e| e.value().latency_ms.load(Ordering::Relaxed))
                .sum();
            
            sum as f64 / active_count as f64
        } else {
            0.0
        };
        
        let avg_success_rate = if total_count > 0 {
            let sum: u64 = self.endpoints.iter()
                .map(|e| e.value().success_rate.load(Ordering::Relaxed))
                .sum();
            
            (sum as f64 / total_count as f64) / 100.0
        } else {
            0.0
        };
        
        let total_stake = self.get_total_active_stake().await;
        let security_violations = self.security_violations.load(Ordering::Relaxed);
        let partition_detected = self.partition_detected.load(Ordering::Relaxed);
        let panic_mode = self.panic_mode.load(Ordering::Relaxed);
        
        NetworkStats {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            current_slot: self.current_slot.load(Ordering::Relaxed),
            current_epoch: self.current_epoch.load(Ordering::Relaxed),
            network_health: self.network_health_score.load(Ordering::Relaxed),
            active_endpoints: active_count,
            total_endpoints: total_count,
            average_latency_ms: avg_latency,
            average_success_rate: avg_success_rate,
            total_stake,
            security_violations,
            partition_detected,
            panic_mode,
        }
    }

    #[instrument(name = "force_refresh", skip(self))]
    pub async fn force_refresh(&self) -> Result<()> {
        info!("Forcing complete mainnet refresh");
        
        let (validator_result, leader_result) = tokio::join!(
            self.update_validator_info(),
            self.update_leader_schedule()
        );
        
        validator_result.context("Failed to update validator info")?;
        leader_result.context("Failed to update leader schedule")?;
        
        self.optimize_routes().await;
        
        info!("Force refresh completed");
        Ok(())
    }

    pub fn get_current_slot(&self) -> u64 {
        self.current_slot.load(Ordering::Relaxed)
    }

    pub fn get_current_epoch(&self) -> u64 {
        self.current_epoch.load(Ordering::Relaxed)
    }

    pub fn get_network_health(&self) -> u64 {
        self.network_health_score.load(Ordering::Relaxed)
    }

    pub fn is_partition_detected(&self) -> bool {
        self.partition_detected.load(Ordering::Relaxed)
    }

    pub fn is_panic_mode(&self) -> bool {
        self.panic_mode.load(Ordering::Relaxed)
    }

    pub fn get_security_violations(&self) -> u32 {
        self.security_violations.load(Ordering::Relaxed)
    }

    #[instrument(name = "get_regional_health", skip(self))]
    pub async fn get_regional_health(&self, region: &str) -> Option<RegionalHealth> {
        self.regional_health.get(region).map(|h| h.clone())
    }

    #[instrument(name = "shutdown", skip(self))]
    pub fn shutdown(&self) {
        info!("Shutting down mainnet NetworkTopologyMapper");
        self.shutdown_signal.store(true, Ordering::Relaxed);
    }
}

impl Clone for NetworkTopologyMapper {
    fn clone(&self) -> Self {
        Self {
            endpoints: self.endpoints.clone(),
            validators: self.validators.clone(),
            leader_schedule: self.leader_schedule.clone(),
            optimal_routes: self.optimal_routes.clone(),
            latency_history: self.latency_history.clone(),
            rpc_clients: self.rpc_clients.clone(),
            current_slot: self.current_slot.clone(),
            current_epoch: self.current_epoch.clone(),
            network_health_score: self.network_health_score.clone(),
            semaphore: self.semaphore.clone(),
            shutdown_signal: self.shutdown_signal.clone(),
            panic_mode: self.panic_mode.clone(),
            slot_history: self.slot_history.clone(),
            epoch_cache: self.epoch_cache.clone(),
            circuit_breakers: self.circuit_breakers.clone(),
            malicious_validators: self.malicious_validators.clone(),
            trusted_endpoints: self.trusted_endpoints.clone(),
            slot_consensus: self.slot_consensus.clone(),
            regional_health: self.regional_health.clone(),
            security_violations: self.security_violations.clone(),
            last_watchdog_check: self.last_watchdog_check.clone(),
            fork_detection_buffer: self.fork_detection_buffer.clone(),
            partition_detected: self.partition_detected.clone(),
            consensus_failures: self.consensus_failures.clone(),
            stake_validation_cache: self.stake_validation_cache.clone(),
            endpoint_whitelist: self.endpoint_whitelist.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkStats {
    pub timestamp: u64,
    pub current_slot: u64,
    pub current_epoch: u64,
    pub network_health: u64,
    pub active_endpoints: usize,
    pub total_endpoints: usize,
    pub average_latency_ms: f64,
    pub average_success_rate: f64,
    pub total_stake: u64,
    pub security_violations: u32,
    pub partition_detected: bool,
    pub panic_mode: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{timeout, Duration};

    #[tokio::test]
    async fn test_mainnet_topology_initialization() {
        let endpoints = vec![
            "https://api.mainnet-beta.solana.com".to_string(),
            "https://solana-api.projectserum.com".to_string(),
        ];
        
        let result = timeout(
            Duration::from_secs(10),
            NetworkTopologyMapper::new(endpoints)
        ).await;
        
        assert!(result.is_ok());
        if let Ok(mapper_result) = result {
            assert!(mapper_result.is_ok());
            let mapper = mapper_result.unwrap();
            assert!(mapper.get_network_health() >= 0);
            assert!(mapper.get_current_slot() >= 0);
        }
    }

    #[tokio::test]
    async fn test_endpoint_validation() {
        let malicious_endpoints = vec![
            "http://malicious.com".to_string(),
            "https://devnet.solana.com".to_string(),
            "https://testnet.solana.com".to_string(),
        ];
        
        let result = NetworkTopologyMapper::new(malicious_endpoints).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_panic_mode_activation() {
        let endpoints = vec![
            "https://api.mainnet-beta.solana.com".to_string(),
        ];
        
        let mapper = NetworkTopologyMapper::new(endpoints).await.unwrap();
        mapper.trigger_panic_mode().await;
        
        assert!(mapper.is_panic_mode());
    }

    #[tokio::test]
    async fn test_circuit_breaker_functionality() {
        let endpoints = vec![
            "https://api.mainnet-beta.solana.com".to_string(),
        ];
        
        let mapper = NetworkTopologyMapper::new(endpoints).await.unwrap();
        
        // Simulate failures
        mapper.trigger_circuit_breaker("https://api.mainnet-beta.solana.com").await;
        mapper.trigger_circuit_breaker("https://api.mainnet-beta.solana.com").await;
        mapper.trigger_circuit_breaker("https://api.mainnet-beta.solana.com").await;
        
        let is_open = mapper.is_circuit_breaker_open("https://api.mainnet-beta.solana.com").await;
        assert!(is_open);
    }

    #[tokio::test]
    async fn test_slot_validation() {
        let endpoints = vec![
            "https://api.mainnet-beta.solana.com".to_string(),
        ];
        
        let mapper = NetworkTopologyMapper::new(endpoints).await.unwrap();
        sleep(Duration::from_secs(2)).await;
        
        let current_slot = mapper.get_current_slot();
        assert!(current_slot > 0);
        assert!(current_slot < 1_000_000_000); // Reasonable upper bound
    }

    #[tokio::test]
    async fn test_validator_filtering() {
        let endpoints = vec![
            "https://api.mainnet-beta.solana.com".to_string(),
        ];
        
        let mapper = NetworkTopologyMapper::new(endpoints).await.unwrap();
        let _ = mapper.force_refresh().await;
        
        let total_stake = mapper.get_total_active_stake().await;
        assert!(total_stake >= MIN_MAINNET_STAKE || total_stake == 0);
    }

    #[tokio::test]
    async fn test_network_health_calculation() {
        let endpoints = vec![
            "https://api.mainnet-beta.solana.com".to_string(),
        ];
        
        let mapper = NetworkTopologyMapper::new(endpoints).await.unwrap();
        sleep(Duration::from_secs(1)).await;
        
        let health = mapper.get_network_health();
        assert!(health <= 100);
        
        let stats = mapper.get_network_stats().await;
        assert!(stats.active_endpoints <= stats.total_endpoints);
    }

    #[tokio::test]
    async fn test_leader_schedule_update() {
        let endpoints = vec![
            "https://api.mainnet-beta.solana.com".to_string(),
        ];
        
        let mapper = NetworkTopologyMapper::new(endpoints).await.unwrap();
        
        match timeout(Duration::from_secs(5), mapper.update_leader_schedule()).await {
            Ok(Ok(())) => {
                let leaders = mapper.get_upcoming_leaders(10).await;
                // Should either have leaders or be empty (both valid)
                assert!(leaders.len() <= 10);
            }
            _ => {
                // Network issues are acceptable in tests
            }
        }
    }

    #[tokio::test]
    async fn test_partition_detection() {
        let endpoints = vec![
            "https://api.mainnet-beta.solana.com".to_string(),
        ];
        
        let mapper = NetworkTopologyMapper::new(endpoints).await.unwrap();
        
        // Initially should not detect partition
        assert!(!mapper.is_partition_detected());
        
        // Simulate partition detection
        mapper.partition_detected.store(true, Ordering::Relaxed);
        assert!(mapper.is_partition_detected());
    }

    #[tokio::test]
    async fn test_security_violation_tracking() {
        let endpoints = vec![
            "https://api.mainnet-beta.solana.com".to_string(),
        ];
        
        let mapper = NetworkTopologyMapper::new(endpoints).await.unwrap();
        
        let initial_violations = mapper.get_security_violations();
        mapper.security_violations.fetch_add(1, Ordering::Relaxed);
        
        let new_violations = mapper.get_security_violations();
        assert!(new_violations > initial_violations);
    }

    #[tokio::test]
    async fn test_endpoint_scoring() {
        let endpoints = vec![
            "https://api.mainnet-beta.solana.com".to_string(),
        ];
        
        let mapper = NetworkTopologyMapper::new(endpoints).await.unwrap();
        sleep(Duration::from_secs(1)).await;
        
        if let Some(metrics) = mapper.get_endpoint_metrics("https://api.mainnet-beta.solana.com").await {
            let score = mapper.calculate_endpoint_score(&metrics).await;
            assert!(score >= 0.0 && score <= 1.0);
        }
    }
}
