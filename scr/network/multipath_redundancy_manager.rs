use dashmap::DashMap;
use futures::future::join_all;
use parking_lot::RwLock;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig},
    rpc_response::RpcSimulateTransactionResult,
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    signature::Signature,
    transaction::Transaction,
};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{mpsc, Semaphore},
    time::{interval, sleep, timeout},
};
use thiserror::Error;

const MAX_RETRIES: u32 = 3;
const BASE_BACKOFF_MS: u64 = 50;
const MAX_BACKOFF_MS: u64 = 5000;
const HEALTH_CHECK_INTERVAL_MS: u64 = 1000;
const LATENCY_WINDOW_SIZE: usize = 100;
const CIRCUIT_BREAKER_THRESHOLD: f64 = 0.5;
const CIRCUIT_BREAKER_WINDOW: Duration = Duration::from_secs(30);
const MAX_CONCURRENT_REQUESTS: usize = 50;
const ENDPOINT_TIMEOUT_MS: u64 = 2000;
const PROPAGATION_DELAY_US: u64 = 100;

#[derive(Error, Debug)]
pub enum MultipathError {
    #[error("No healthy endpoints available")]
    NoHealthyEndpoints,
    #[error("All endpoints failed")]
    AllEndpointsFailed,
    #[error("Transaction send timeout")]
    SendTimeout,
    #[error("Max retries exceeded")]
    MaxRetriesExceeded,
    #[error("Simulation failed: {0}")]
    SimulationFailed(String),
    #[error("RPC error: {0}")]
    RpcError(#[from] solana_client::client_error::ClientError),
}

#[derive(Debug, Clone)]
pub struct EndpointConfig {
    pub url: String,
    pub weight: u32,
    pub max_tps: u32,
    pub region: String,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum EndpointState {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Debug)]
struct EndpointMetrics {
    latencies: RwLock<VecDeque<u64>>,
    success_count: AtomicU64,
    failure_count: AtomicU64,
    last_failure: AtomicU64,
    current_tps: AtomicU64,
    state: RwLock<EndpointState>,
}

impl EndpointMetrics {
    fn new() -> Self {
        Self {
            latencies: RwLock::new(VecDeque::with_capacity(LATENCY_WINDOW_SIZE)),
            success_count: AtomicU64::new(0),
            failure_count: AtomicU64::new(0),
            last_failure: AtomicU64::new(0),
            current_tps: AtomicU64::new(0),
            state: RwLock::new(EndpointState::Healthy),
        }
    }

    fn record_latency(&self, latency_us: u64) {
        let mut latencies = self.latencies.write();
        if latencies.len() >= LATENCY_WINDOW_SIZE {
            latencies.pop_front();
        }
        latencies.push_back(latency_us);
    }

    fn avg_latency(&self) -> u64 {
        let latencies = self.latencies.read();
        if latencies.is_empty() {
            return u64::MAX;
        }
        latencies.iter().sum::<u64>() / latencies.len() as u64
    }

    fn p99_latency(&self) -> u64 {
        let latencies = self.latencies.read();
        if latencies.is_empty() {
            return u64::MAX;
        }
        let mut sorted: Vec<u64> = latencies.iter().copied().collect();
        sorted.sort_unstable();
        sorted[(sorted.len() as f64 * 0.99) as usize]
    }

    fn success_rate(&self) -> f64 {
        let success = self.success_count.load(Ordering::Relaxed) as f64;
        let failure = self.failure_count.load(Ordering::Relaxed) as f64;
        let total = success + failure;
        if total == 0.0 {
            1.0
        } else {
            success / total
        }
    }

    fn should_circuit_break(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let last_failure = self.last_failure.load(Ordering::Relaxed);
        
        if now - last_failure < CIRCUIT_BREAKER_WINDOW.as_millis() as u64 {
            self.success_rate() < CIRCUIT_BREAKER_THRESHOLD
        } else {
            false
        }
    }
}

struct Endpoint {
    config: EndpointConfig,
    client: Arc<RpcClient>,
    metrics: Arc<EndpointMetrics>,
    semaphore: Arc<Semaphore>,
    weight: AtomicU32,
}

impl Endpoint {
    fn new(config: EndpointConfig) -> Self {
        let client = Arc::new(RpcClient::new_with_timeout(
            config.url.clone(),
            Duration::from_millis(ENDPOINT_TIMEOUT_MS),
        ));
        let semaphore = Arc::new(Semaphore::new(config.max_tps as usize));
        let weight = AtomicU32::new(config.weight);
        
        Self {
            config,
            client,
            metrics: Arc::new(EndpointMetrics::new()),
            semaphore,
            weight,
        }
    }

    fn score(&self) -> f64 {
        let state = *self.metrics.state.read();
        if state == EndpointState::Unhealthy {
            return 0.0;
        }

        let success_rate = self.metrics.success_rate();
        let avg_latency = self.metrics.avg_latency() as f64;
        let weight = self.weight.load(Ordering::Relaxed) as f64;
        
        let latency_score = 1.0 / (1.0 + avg_latency / 1000.0);
        let state_multiplier = match state {
            EndpointState::Healthy => 1.0,
            EndpointState::Degraded => 0.5,
            EndpointState::Unhealthy => 0.0,
        };

        weight * success_rate * latency_score * state_multiplier
    }
}

pub struct MultipathRedundancyManager {
    endpoints: Arc<DashMap<String, Arc<Endpoint>>>,
    primary_commitment: CommitmentConfig,
    shutdown: Arc<AtomicBool>,
    active_count: Arc<AtomicUsize>,
}

impl MultipathRedundancyManager {
    pub fn new(configs: Vec<EndpointConfig>) -> Self {
        let endpoints = Arc::new(DashMap::new());
        
        for config in configs {
            let endpoint = Arc::new(Endpoint::new(config.clone()));
            endpoints.insert(config.url.clone(), endpoint);
        }

        let manager = Self {
            endpoints: endpoints.clone(),
            primary_commitment: CommitmentConfig::processed(),
            shutdown: Arc::new(AtomicBool::new(false)),
            active_count: Arc::new(AtomicUsize::new(0)),
        };

        let health_checker = manager.clone();
        tokio::spawn(async move {
            health_checker.health_check_loop().await;
        });

        manager
    }

    pub async fn send_transaction_multipath(
        &self,
        transaction: &Transaction,
        skip_preflight: bool,
    ) -> Result<Signature, MultipathError> {
        let endpoints = self.select_endpoints_for_send();
        if endpoints.is_empty() {
            return Err(MultipathError::NoHealthyEndpoints);
        }

        let (tx, mut rx) = mpsc::channel(endpoints.len());
        let mut handles = Vec::new();

        for (idx, endpoint) in endpoints.iter().enumerate() {
            let endpoint = endpoint.clone();
            let transaction = transaction.clone();
            let tx = tx.clone();
            let delay = PROPAGATION_DELAY_US * idx as u64;

            let handle = tokio::spawn(async move {
                if delay > 0 {
                    sleep(Duration::from_micros(delay)).await;
                }

                let start = Instant::now();
                let _permit = endpoint.semaphore.acquire().await.ok()?;

                let config = RpcSendTransactionConfig {
                    skip_preflight,
                    preflight_commitment: Some(CommitmentLevel::Processed),
                    encoding: None,
                    max_retries: Some(0),
                    min_context_slot: None,
                };

                match timeout(
                    Duration::from_millis(ENDPOINT_TIMEOUT_MS),
                    endpoint.client.send_transaction_with_config(&transaction, config),
                )
                .await
                {
                    Ok(Ok(sig)) => {
                        let latency = start.elapsed().as_micros() as u64;
                        endpoint.metrics.record_latency(latency);
                        endpoint.metrics.success_count.fetch_add(1, Ordering::Relaxed);
                        let _ = tx.send((sig, latency, endpoint.config.url.clone())).await;
                        Some(sig)
                    }
                    _ => {
                        endpoint.metrics.failure_count.fetch_add(1, Ordering::Relaxed);
                        endpoint.metrics.last_failure.store(
                            SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap()
                                .as_millis() as u64,
                            Ordering::Relaxed,
                        );
                        None
                    }
                }
            });

            handles.push(handle);
        }

        drop(tx);

        tokio::select! {
            result = rx.recv() => {
                match result {
                    Some((sig, _, _)) => Ok(sig),
                    None => Err(MultipathError::AllEndpointsFailed),
                }
            }
            _ = sleep(Duration::from_millis(ENDPOINT_TIMEOUT_MS * 2)) => {
                Err(MultipathError::SendTimeout)
            }
        }
    }

    pub async fn simulate_transaction(
        &self,
        transaction: &Transaction,
    ) -> Result<RpcSimulateTransactionResult, MultipathError> {
        let endpoints = self.select_endpoints_for_read();
        
        for _ in 0..MAX_RETRIES {
            for endpoint in &endpoints {
                let start = Instant::now();
                let _permit = match endpoint.semaphore.try_acquire() {
                    Ok(permit) => permit,
                    Err(_) => continue,
                };

                let config = RpcSimulateTransactionConfig {
                    sig_verify: true,
                    replace_recent_blockhash: false,
                    commitment: Some(self.primary_commitment),
                    encoding: None,
                    accounts: None,
                    min_context_slot: None,
                    inner_instructions: false,
                };

                match timeout(
                    Duration::from_millis(ENDPOINT_TIMEOUT_MS),
                    endpoint.client.simulate_transaction_with_config(transaction, config),
                )
                .await
                {
                    Ok(Ok(result)) => {
                        let latency = start.elapsed().as_micros() as u64;
                        endpoint.metrics.record_latency(latency);
                        endpoint.metrics.success_count.fetch_add(1, Ordering::Relaxed);
                        return Ok(result.value);
                    }
                    _ => {
                        endpoint.metrics.failure_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            
            sleep(Duration::from_millis(BASE_BACKOFF_MS)).await;
        }

        Err(MultipathError::SimulationFailed("All simulation attempts failed".to_string()))
    }

    pub async fn get_slot(&self) -> Result<u64, MultipathError> {
        let endpoints = self.select_endpoints_for_read();
        
        for endpoint in endpoints {
            let _permit = match endpoint.semaphore.try_acquire() {
                Ok(permit) => permit,
                Err(_) => continue,
            };

            match timeout(
                Duration::from_millis(500),
                endpoint.client.get_slot_with_commitment(self.primary_commitment),
            )
            .await
            {
                Ok(Ok(slot)) => return Ok(slot),
                _ => continue,
            }
        }

        Err(MultipathError::AllEndpointsFailed)
    }

        fn select_endpoints_for_send(&self) -> Vec<Arc<Endpoint>> {
        let mut endpoints: Vec<Arc<Endpoint>> = self
            .endpoints
            .iter()
            .map(|entry| entry.value().clone())
            .filter(|e| !e.metrics.should_circuit_break())
            .collect();

        endpoints.sort_by(|a, b| {
            b.score()
                .partial_cmp(&a.score())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let send_count = std::cmp::min(3, endpoints.len());
        endpoints.truncate(send_count);
        endpoints
    }

    fn select_endpoints_for_read(&self) -> Vec<Arc<Endpoint>> {
        let mut endpoints: Vec<Arc<Endpoint>> = self
            .endpoints
            .iter()
            .map(|entry| entry.value().clone())
            .filter(|e| {
                let state = *e.metrics.state.read();
                state != EndpointState::Unhealthy && !e.metrics.should_circuit_break()
            })
            .collect();

        endpoints.sort_by(|a, b| {
            a.metrics
                .avg_latency()
                .cmp(&b.metrics.avg_latency())
        });

        endpoints.truncate(2);
        endpoints
    }

    async fn health_check_loop(&self) {
        let mut interval = interval(Duration::from_millis(HEALTH_CHECK_INTERVAL_MS));
        
        while !self.shutdown.load(Ordering::Relaxed) {
            interval.tick().await;
            
            let endpoints: Vec<Arc<Endpoint>> = self
                .endpoints
                .iter()
                .map(|entry| entry.value().clone())
                .collect();

            let health_futures = endpoints.iter().map(|endpoint| {
                let endpoint = endpoint.clone();
                tokio::spawn(async move {
                    let start = Instant::now();
                    let permit = match timeout(
                        Duration::from_millis(100),
                        endpoint.semaphore.acquire(),
                    )
                    .await
                    {
                        Ok(Ok(permit)) => permit,
                        _ => return,
                    };

                    match timeout(
                        Duration::from_millis(500),
                        endpoint.client.get_health(),
                    )
                    .await
                    {
                        Ok(Ok(_)) => {
                            let latency = start.elapsed().as_micros() as u64;
                            endpoint.metrics.record_latency(latency);
                            
                            let success_rate = endpoint.metrics.success_rate();
                            let avg_latency = endpoint.metrics.avg_latency();
                            let p99_latency = endpoint.metrics.p99_latency();
                            
                            let new_state = if success_rate > 0.95 && avg_latency < 50000 {
                                EndpointState::Healthy
                            } else if success_rate > 0.7 && avg_latency < 100000 {
                                EndpointState::Degraded
                            } else {
                                EndpointState::Unhealthy
                            };
                            
                            *endpoint.metrics.state.write() = new_state;
                            
                            if p99_latency > 200000 && success_rate < 0.8 {
                                endpoint.metrics.failure_count.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        _ => {
                            endpoint.metrics.failure_count.fetch_add(1, Ordering::Relaxed);
                            endpoint.metrics.last_failure.store(
                                SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap()
                                    .as_millis() as u64,
                                Ordering::Relaxed,
                            );
                            
                            if endpoint.metrics.should_circuit_break() {
                                *endpoint.metrics.state.write() = EndpointState::Unhealthy;
                            }
                        }
                    }
                    
                    drop(permit);
                })
            });

            let _ = join_all(health_futures).await;
            self.rebalance_endpoints().await;
        }
    }

    async fn rebalance_endpoints(&self) {
        let total_healthy = self
            .endpoints
            .iter()
            .filter(|e| {
                let state = *e.value().metrics.state.read();
                state == EndpointState::Healthy
            })
            .count();

        if total_healthy == 0 {
            for entry in self.endpoints.iter() {
                let endpoint = entry.value();
                if endpoint.metrics.success_rate() > 0.3 {
                    *endpoint.metrics.state.write() = EndpointState::Degraded;
                    endpoint.metrics.success_count.store(0, Ordering::Relaxed);
                    endpoint.metrics.failure_count.store(0, Ordering::Relaxed);
                }
            }
        }
        
        self.active_count.store(total_healthy, Ordering::Relaxed);
    }

    pub async fn send_with_retry(
        &self,
        transaction: &Transaction,
        max_retries: u32,
    ) -> Result<Signature, MultipathError> {
        let mut backoff = BASE_BACKOFF_MS;
        let mut last_error = None;

        for attempt in 0..max_retries {
            match self.send_transaction_multipath(transaction, true).await {
                Ok(sig) => return Ok(sig),
                Err(e) => {
                    last_error = Some(e);
                    
                    if attempt < max_retries - 1 {
                        let jitter = (SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap()
                            .as_nanos() as u64) % (backoff / 4);
                        sleep(Duration::from_millis(backoff + jitter)).await;
                        backoff = std::cmp::min(backoff * 2, MAX_BACKOFF_MS);
                    }
                }
            }
        }

        Err(last_error.unwrap_or(MultipathError::MaxRetriesExceeded))
    }

    pub async fn broadcast_transaction(
        &self,
        transaction: &Transaction,
    ) -> Vec<(String, Result<Signature, String>)> {
        let endpoints: Vec<Arc<Endpoint>> = self
            .endpoints
            .iter()
            .map(|entry| entry.value().clone())
            .filter(|e| {
                let state = *e.metrics.state.read();
                state != EndpointState::Unhealthy
            })
            .collect();

        let mut handles = Vec::new();
        
        for endpoint in endpoints {
            let endpoint = endpoint.clone();
            let transaction = transaction.clone();
            
            let handle = tokio::spawn(async move {
                let _permit = match timeout(
                    Duration::from_millis(100),
                    endpoint.semaphore.acquire(),
                )
                .await
                {
                    Ok(Ok(permit)) => permit,
                    _ => {
                        return (
                            endpoint.config.url.clone(),
                            Err("Failed to acquire permit".to_string()),
                        )
                    }
                };

                let config = RpcSendTransactionConfig {
                    skip_preflight: true,
                    preflight_commitment: Some(CommitmentLevel::Processed),
                    encoding: None,
                    max_retries: Some(0),
                    min_context_slot: None,
                };

                match timeout(
                    Duration::from_millis(ENDPOINT_TIMEOUT_MS),
                    endpoint.client.send_transaction_with_config(&transaction, config),
                )
                .await
                {
                    Ok(Ok(sig)) => {
                        endpoint.metrics.success_count.fetch_add(1, Ordering::Relaxed);
                        (endpoint.config.url.clone(), Ok(sig))
                    }
                    Ok(Err(e)) => {
                        endpoint.metrics.failure_count.fetch_add(1, Ordering::Relaxed);
                        (endpoint.config.url.clone(), Err(e.to_string()))
                    }
                    Err(_) => {
                        endpoint.metrics.failure_count.fetch_add(1, Ordering::Relaxed);
                        (endpoint.config.url.clone(), Err("Timeout".to_string()))
                    }
                }
            });
            
            handles.push(handle);
        }

        let results = join_all(handles).await;
        results
            .into_iter()
            .filter_map(|r| r.ok())
            .collect()
    }

    pub fn get_endpoint_stats(&self) -> Vec<EndpointStats> {
        self.endpoints
            .iter()
            .map(|entry| {
                let endpoint = entry.value();
                EndpointStats {
                    url: endpoint.config.url.clone(),
                    region: endpoint.config.region.clone(),
                    state: *endpoint.metrics.state.read(),
                    success_rate: endpoint.metrics.success_rate(),
                    avg_latency_us: endpoint.metrics.avg_latency(),
                    p99_latency_us: endpoint.metrics.p99_latency(),
                    current_tps: endpoint.metrics.current_tps.load(Ordering::Relaxed),
                }
            })
            .collect()
    }

    pub fn update_endpoint_weight(&self, url: &str, weight: u32) {
        if let Some(entry) = self.endpoints.get(url) {
            entry.value().weight.store(weight, Ordering::Relaxed);
        }
    }

    pub async fn optimal_endpoint_for_region(
        &self,
        region: &str,
    ) -> Option<Arc<Endpoint>> {
        let mut candidates: Vec<Arc<Endpoint>> = self
            .endpoints
            .iter()
            .map(|entry| entry.value().clone())
            .filter(|e| {
                e.config.region == region && 
                *e.metrics.state.read() == EndpointState::Healthy &&
                !e.metrics.should_circuit_break()
            })
            .collect();

        candidates.sort_by(|a, b| {
            b.score()
                .partial_cmp(&a.score())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        candidates.into_iter().next()
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    pub fn is_healthy(&self) -> bool {
        self.active_count.load(Ordering::Relaxed) > 0
    }

    pub async fn emergency_fallback_send(
        &self,
        transaction: &Transaction,
    ) -> Result<Signature, MultipathError> {
        for entry in self.endpoints.iter() {
            let endpoint = entry.value();
            
            let config = RpcSendTransactionConfig {
                skip_preflight: true,
                preflight_commitment: Some(CommitmentLevel::Processed),
                encoding: None,
                max_retries: Some(0),
                min_context_slot: None,
            };

            match timeout(
                Duration::from_secs(5),
                endpoint.client.send_transaction_with_config(transaction, config),
            )
            .await
            {
                Ok(Ok(sig)) => return Ok(sig),
                Ok(Err(e)) => {
                    eprintln!("Emergency send failed for {}: {}", endpoint.config.url, e);
                    continue;
                }
                Err(_) => {
                    eprintln!("Emergency send timeout for {}", endpoint.config.url);
                    continue;
                }
            }
        }

        Err(MultipathError::AllEndpointsFailed)
    }

    pub async fn get_recent_blockhash(&self) -> Result<solana_sdk::hash::Hash, MultipathError> {
        let endpoints = self.select_endpoints_for_read();
        
        for endpoint in endpoints {
            let _permit = match endpoint.semaphore.try_acquire() {
                Ok(permit) => permit,
                Err(_) => continue,
            };

            match timeout(
                Duration::from_millis(500),
                endpoint.client.get_latest_blockhash_with_commitment(self.primary_commitment),
            )
            .await
            {
                Ok(Ok((blockhash, _))) => return Ok(blockhash),
                _ => continue,
            }
        }

        Err(MultipathError::AllEndpointsFailed)
    }

    pub fn get_best_endpoint(&self) -> Option<Arc<Endpoint>> {
        self.endpoints
            .iter()
            .map(|entry| entry.value().clone())
            .filter(|e| {
                let state = *e.metrics.state.read();
                state == EndpointState::Healthy && !e.metrics.should_circuit_break()
            })
            .max_by(|a, b| {
                a.score()
                    .partial_cmp(&b.score())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    }

    pub fn reset_endpoint_metrics(&self, url: &str) {
        if let Some(entry) = self.endpoints.get(url) {
            let endpoint = entry.value();
            endpoint.metrics.success_count.store(0, Ordering::Relaxed);
            endpoint.metrics.failure_count.store(0, Ordering::Relaxed);
            endpoint.metrics.latencies.write().clear();
            *endpoint.metrics.state.write() = EndpointState::Healthy;
        }
    }
}

#[derive(Debug, Clone)]
pub struct EndpointStats {
    pub url: String,
    pub region: String,
    pub state: EndpointState,
    pub success_rate: f64,
    pub avg_latency_us: u64,
    pub p99_latency_us: u64,
    pub current_tps: u64,
}

impl Clone for MultipathRedundancyManager {
    fn clone(&self) -> Self {
        Self {
            endpoints: self.endpoints.clone(),
            primary_commitment: self.primary_commitment,
            shutdown: self.shutdown.clone(),
            active_count: self.active_count.clone(),
        }
    }
}

impl Drop for MultipathRedundancyManager {
    fn drop(&mut self) {
        self.shutdown();
    }
}

#[cfg(not(test))]
pub fn create_default_endpoints() -> Vec<EndpointConfig> {
    vec![
        EndpointConfig {
                        url: "https://api.mainnet-beta.solana.com".to_string(),
            weight: 10,
            max_tps: 100,
            region: "global".to_string(),
        },
        EndpointConfig {
            url: "https://solana-api.projectserum.com".to_string(),
            weight: 8,
            max_tps: 80,
            region: "us-east".to_string(),
        },
        EndpointConfig {
            url: "https://rpc.ankr.com/solana".to_string(),
            weight: 7,
            max_tps: 70,
            region: "global".to_string(),
        },
        EndpointConfig {
            url: "https://solana.public-rpc.com".to_string(),
            weight: 6,
            max_tps: 60,
            region: "us-west".to_string(),
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::{
        signature::Keypair,
        signer::Signer,
        system_transaction,
        pubkey::Pubkey,
    };

    #[tokio::test]
    async fn test_endpoint_selection() {
        let configs = vec![
            EndpointConfig {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                weight: 10,
                max_tps: 100,
                region: "us-east".to_string(),
            },
            EndpointConfig {
                url: "https://solana-api.projectserum.com".to_string(),
                weight: 8,
                max_tps: 80,
                region: "us-west".to_string(),
            },
        ];

        let manager = MultipathRedundancyManager::new(configs);
        assert!(manager.is_healthy());
        
        let endpoints = manager.select_endpoints_for_send();
        assert!(!endpoints.is_empty());
    }

    #[tokio::test]
    async fn test_circuit_breaker() {
        let configs = vec![
            EndpointConfig {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                weight: 10,
                max_tps: 100,
                region: "us-east".to_string(),
            },
        ];

        let manager = MultipathRedundancyManager::new(configs);
        let endpoint = manager.endpoints.get("https://api.mainnet-beta.solana.com").unwrap();
        
        // Simulate failures
        for _ in 0..10 {
            endpoint.value().metrics.failure_count.fetch_add(1, Ordering::Relaxed);
        }
        endpoint.value().metrics.success_count.store(5, Ordering::Relaxed);
        
        assert!(endpoint.value().metrics.should_circuit_break());
    }

    #[tokio::test]
    async fn test_latency_tracking() {
        let configs = vec![
            EndpointConfig {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                weight: 10,
                max_tps: 100,
                region: "us-east".to_string(),
            },
        ];

        let manager = MultipathRedundancyManager::new(configs);
        let endpoint = manager.endpoints.get("https://api.mainnet-beta.solana.com").unwrap();
        
        // Record some latencies
        for i in 1..=10 {
            endpoint.value().metrics.record_latency(i * 1000);
        }
        
        let avg = endpoint.value().metrics.avg_latency();
        assert_eq!(avg, 5500); // Average of 1000 to 10000
    }

    #[tokio::test]
    async fn test_endpoint_scoring() {
        let config = EndpointConfig {
            url: "https://api.mainnet-beta.solana.com".to_string(),
            weight: 10,
            max_tps: 100,
            region: "us-east".to_string(),
        };

        let endpoint = Endpoint::new(config);
        
        // Perfect endpoint
        endpoint.metrics.success_count.store(100, Ordering::Relaxed);
        endpoint.metrics.failure_count.store(0, Ordering::Relaxed);
        endpoint.metrics.record_latency(1000); // 1ms
        
        let score = endpoint.score();
        assert!(score > 0.0);
        
        // Degraded endpoint
        endpoint.metrics.failure_count.store(30, Ordering::Relaxed);
        let degraded_score = endpoint.score();
        assert!(degraded_score < score);
    }

    #[tokio::test]
    async fn test_weight_update() {
        let configs = vec![
            EndpointConfig {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                weight: 10,
                max_tps: 100,
                region: "us-east".to_string(),
            },
        ];

        let manager = MultipathRedundancyManager::new(configs);
        manager.update_endpoint_weight("https://api.mainnet-beta.solana.com", 20);
        
        let endpoint = manager.endpoints.get("https://api.mainnet-beta.solana.com").unwrap();
        assert_eq!(endpoint.value().weight.load(Ordering::Relaxed), 20);
    }

    #[tokio::test]
    async fn test_shutdown() {
        let configs = create_default_endpoints();
        let manager = MultipathRedundancyManager::new(configs);
        
        assert!(!manager.shutdown.load(Ordering::Relaxed));
        manager.shutdown();
        assert!(manager.shutdown.load(Ordering::Relaxed));
    }
}

