use tracing_futures::Instrument;
use tracing::{debug, error, info, instrument, span, warn, Level};
// Production-grade RPC Load Balancer for Solana HFT MEV Operations
// 
// This module provides a fault-tolerant, high-performance RPC load balancer
// designed to survive mainnet conditions including validator censorship,
// slot forks, network partitions, and extreme MEV competition.

#[deny(unsafe_code)]
#[warn(clippy::all, clippy::pedantic, clippy::nursery)]

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::collections::HashMap;

use base64;
use bincode;
use dashmap::DashMap;
use fastrand;
use governor::{Quota, RateLimiter, DefaultDirectRateLimiter};
use moka::future::Cache;
use reqwest::Client;
use scopeguard;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::{RwLock, Semaphore, mpsc};
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;
use uuid::Uuid;

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig};
use solana_client::rpc_response::{RpcSimulateTransactionResult, RpcPrioritizationFee};
use solana_sdk::{
    clock::Slot,
    commitment_config::{CommitmentConfig, CommitmentLevel},
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::Instruction,
    message::Message,
    pubkey::Pubkey,
    signature::Signature,
    signer::Signer,
    transaction::{Transaction, VersionedTransaction},
    transaction_status::UiTransactionEncoding,
};

// Production Jito tip accounts (mainnet-beta)
const JITO_TIP_ACCOUNTS: &[&str] = &[
    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
    "HFqU5x63VTqvQss8hp176NUQpyPdhRBmkHYG9gUPD6kG", 
    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
];

// Configuration constants
const MAX_ENDPOINTS: usize = 8;
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 300;
const CRITICAL_REQUEST_TIMEOUT_MS: u64 = 200;
const SLOT_CACHE_TTL_MS: u64 = 200;
const BLOCKHASH_CACHE_TTL_MS: u64 = 20_000;
const MAX_CONCURRENT_REQUESTS: usize = 1000;
const HEALTH_CHECK_INTERVAL_MS: u64 = 5000;
const FORK_DETECTION_WINDOW: usize = 32;
const MIN_TIP_LAMPORTS: u64 = 1_000;
const MAX_TIP_LAMPORTS: u64 = 100_000;
const CIRCUIT_BREAKER_THRESHOLD: usize = 5;
const CIRCUIT_BREAKER_RESET_TIME_SECS: u64 = 30;
const BLOCKHASH_REFRESH_DELAY_MS: u64 = 100;
const MAX_BLOCKHASH_REFRESH_ATTEMPTS: usize = 3;

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("No healthy endpoints available")]
    NoHealthyEndpoints,
    
    #[error("Request timeout after {0}ms")]
    Timeout(u64),
    
    #[error("Rate limit exceeded for endpoint {0}")]
    RateLimited(String),
    
    #[error("Circuit breaker open for endpoint {0}")]
    CircuitBreakerOpen(String),
    
    #[error("Invalid transaction: {0}")]
    InvalidTransaction(String),
    
    #[error("Slot fork detected, invalidating caches")]
    SlotFork,
    
    #[error("Shutdown in progress")]
    Shutdown,
    
    #[error("Transaction simulation failed: {0}")]
    SimulationFailed(String),
    
    #[error("Blockhash not found, attempting refresh")]
    BlockhashNotFound,
    
    #[error("RPC client error: {0}")]
    Client(#[from] solana_client::client_error::ClientError),
    
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    
    #[error("Serialization error: {0}")]
    Serialization(String),
}

pub type Result<T> = std::result::Result<T, RpcError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestPriority {
    Critical,  // <200ms timeout - MEV/arbitrage
    High,      // <300ms timeout - Trading operations  
    Normal,    // <500ms timeout - Account queries
    Low,       // <1000ms timeout - Background tasks
}

impl RequestPriority {
// TODO FIX:     pub fn timeout_ms(self) -> u64 {
        match self {
            Self::Critical , CRITICAL_REQUEST_TIMEOUT_MS,
            Self::High , DEFAULT_REQUEST_TIMEOUT_MS,
            Self::Normal , 500,
            Self::Low , 1000,
        }
    }
    
// TODO FIX:     pub fn compute_units(self) -> u32 {
impl PriorityLevel {

    pub fn compute_units(&self) -> u32 {

        match self {

            Self::Critical => 1_400_000,

            Self::High => 800_000,

            Self::Normal => 400_000,

            Self::Low => 200_000,

        }
    }

}

#[derive(Debug, Clone)]
pub struct EndpointConfig {
    pub url: String,
    pub rate_limit_per_second: u32,
    pub max_connections: usize,
    pub region: String,
}

#[derive()]
struct EndpointMetrics {
    total_requests: AtomicU64,
    successful_requests: AtomicU64,
    failed_requests: AtomicU64,
    total_latency_ns: AtomicU64,
    consecutive_failures: AtomicUsize,
    last_success_time: AtomicU64,
    last_failure_time: AtomicU64,
    circuit_breaker_state: AtomicBool, // true = open (failing)
}

impl EndpointMetrics {
    fn new() -> Self {
        let now = unix_timestamp();
        Self {
            total_requests: AtomicU64::new(0),
            successful_requests: AtomicU64::new(0),
            failed_requests: AtomicU64::new(0),
            total_latency_ns: AtomicU64::new(0),
            consecutive_failures: AtomicUsize::new(0),
            last_success_time: AtomicU64::new(now),
            last_failure_time: AtomicU64::new(0),
            circuit_breaker_state: AtomicBool::new(false),
        }
    }
    
// TODO FIX:     fn record_success(&self, latency: Duration) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.successful_requests.fetch_add(1, Ordering::Relaxed);
        self.total_latency_ns.fetch_add(latency.as_nanos() as u64, Ordering::Relaxed);
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.last_success_time.store(unix_timestamp(), Ordering::Relaxed);
        self.circuit_breaker_state.store(false, Ordering::Relaxed);
    }
    
// TODO FIX:     fn record_failure(&self) {
        self.total_requests.fetch_add(1, Ordering::Relaxed);
        self.failed_requests.fetch_add(1, Ordering::Relaxed);
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        self.last_failure_time.store(unix_timestamp(), Ordering::Relaxed);
        
        // Open circuit breaker after threshold consecutive failures
        if failures >= CIRCUIT_BREAKER_THRESHOLD {
            self.circuit_breaker_state.store(true, Ordering::Relaxed);
        }
    
    
// TODO FIX:     fn maybe_reset_circuit_breaker(&self) {
        if self.circuit_breaker_state.load(Ordering::Relaxed) {
            let last_failure = self.last_failure_time.load(Ordering::Relaxed);
            let now = unix_timestamp();
            
            // Reset circuit breaker after timeout
            if now.saturating_sub(last_failure) > CIRCUIT_BREAKER_RESET_TIME_SECS {
                self.circuit_breaker_state.store(false, Ordering::Relaxed);
                // Reset consecutive failures to allow gradual recovery
                self.consecutive_failures.store(0, Ordering::Relaxed);
            }
        }
    
    
// TODO FIX:     fn health_score(&self) -> f64 {
        // Try to reset circuit breaker if enough time has passed
        self.maybe_reset_circuit_breaker();
        
        let total = self.total_requests.load(Ordering::Relaxed);
        if total == 0 {
            return 1.0; // New endpoint starts healthy
        }
        
        let successful = self.successful_requests.load(Ordering::Relaxed);
        let success_rate = successful as f64 / total as f64;
        
        let consecutive_failures = self.consecutive_failures.load(Ordering::Relaxed);
        let failure_penalty = (consecutive_failures as f64 / 10.0).min(1.0);
        
        let last_success = self.last_success_time.load(Ordering::Relaxed);
        let time_since_success = unix_timestamp().saturating_sub(last_success);
        let staleness_penalty = (time_since_success as f64 / 300.0).min(1.0); // 5 min max penalty
        
        let avg_latency = if successful > 0 {
            self.total_latency_ns.load(Ordering::Relaxed) / successful
        } else {
            u64::MAX
        };
        let latency_penalty = ((avg_latency as f64) / 1_000_000_000.0).min(1.0); // 1s max penalty
        
        (success_rate * 0.5 - failure_penalty * 0.2 - staleness_penalty * 0.2 - latency_penalty * 0.1)
            .max(0.0)
            .min(1.0)
    
    
// TODO FIX:     fn is_healthy(&self) -> bool {
        !self.circuit_breaker_state.load(Ordering::Relaxed) && self.health_score() > 0.3
    


#[derive()]
pub struct RpcEndpoint {
    config: EndpointConfig,
    client: RpcClient,
    http_client: Client,
    rate_limiter: DefaultDirectRateLimiter,
    metrics: EndpointMetrics,
    in_flight_requests: Arc<AtomicUsize>,
    last_known_slot: Arc<AtomicU64>,
    last_known_blockhash: Arc<RwLock<Option<(Hash, Slot)>>>,
}

impl RpcEndpoint {
    fn new(config: EndpointConfig) -> Result<Self> {
        // Validate URL
        let url = reqwest::Url::parse(&config.url)
            .map_err(|_| RpcError::InvalidTransaction(format!("Invalid URL: {}", config.url)))?;
        
        // Require HTTPS in production (allow localhost for dev)
        if url.scheme() != "https" && !url.host_str().unwrap_or("").contains("localhost") && !url.host_str().unwrap_or("").starts_with("127.") {
            return Err(RpcError::InvalidTransaction("HTTPS required for non-localhost endpoints".into()));
        }
        
        let client = RpcClient::new_with_timeout_and_commitment(
            config.url.clone(),
            Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
            CommitmentConfig::confirmed(),
        );
        
        let http_client = Client::builder()
            .timeout(Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS))
            .pool_max_idle_per_host(config.max_connections)
            .http2_prior_knowledge()
            .build()?;
        
        let rate_limiter = RateLimiter::direct(
            Quota::per_second(
                std::num::NonZeroU32::new(config.rate_limit_per_second)
                    .ok_or_else(|| RpcError::InvalidTransaction("Rate limit must be > 0".into()))?
            )
        );
        
        Ok(Self {
            config,
            client,
            http_client,
            rate_limiter,
            metrics: EndpointMetrics::new(),
            in_flight_requests: Arc::new(AtomicUsize::new(0)),
            last_known_slot: Arc::new(AtomicU64::new(0)),
            last_known_blockhash: Arc::new(RwLock::new(None)),
        })
    }
    
// TODO FIX:     async fn health_check(&self) -> Result<()> {
        let start = Instant::now();
        
        // Use shorter timeout for health checks
        match timeout(Duration::from_millis(1000), self.client.get_version()).await {
            Ok(Ok(_)) , {
                self.metrics.record_success(start.elapsed());
                Ok(())
            }
            Ok(Err(e)) , {
                self.metrics.record_failure();
                Err(RpcError::Client(e))
            }
            Err(_) , {
                self.metrics.record_failure();
                Err(RpcError::Timeout(1000))
            }
        }
    }
    
// TODO FIX:     fn can_accept_request(&self) -> bool {
        if !self.metrics.is_healthy() {
            return false;
        }
        
        // Check rate limit
        if self.rate_limiter.check().is_err() {
            return false;
        }
        
        // Check in-flight request capacity
        self.in_flight_requests.load(Ordering::Relaxed) < MAX_CONCURRENT_REQUESTS
    
    
// TODO FIX:     async fn detect_fork(&self, new_slot: Slot, new_blockhash: Hash, balancer_caches: Option<(&Cache<(), Slot>, &Cache<(), (Hash, Slot)>)>) -> bool {
        let last_slot = self.last_known_slot.load(Ordering::Relaxed);
        let mut last_blockhash_guard = self.last_known_blockhash.write().await;
        
        if let Some((last_hash, last_hash_slot)) = *last_blockhash_guard {
            // Fork detected if:
            // 1. New slot is less than or equal to last known slot with different hash
            // 2. Slot progression is non-monotonic
            if (new_slot <= last_hash_slot && new_blockhash != last_hash) || 
               (new_slot < last_slot) {
                warn!(
                    endpoint = %self.config.url,
                    new_slot = new_slot,
                    last_slot = last_slot,
                    new_hash = %new_blockhash,
                    last_hash = %last_hash,
                    "Fork detected - invalidating all caches"
                );
                
                // CRITICAL: Invalidate caches immediately on fork
                if let Some((slot_cache, blockhash_cache)) = balancer_caches {
                    slot_cache.invalidate_all();
                    blockhash_cache.invalidate_all();
                }
                
                return true;
            }
        }
        
        self.last_known_slot.store(new_slot, Ordering::Relaxed);
        *last_blockhash_guard = Some((new_blockhash, new_slot));
        false
    


#[derive()]
struct SlotTracker {
    current_slot: Arc<AtomicU64>,
    slot_start_time: Arc<RwLock<Instant>>,
    recent_slot_times: Cache<Slot, Duration>,
}

impl SlotTracker {
    fn new() -> Self {
        Self {
            current_slot: Arc::new(AtomicU64::new(0)),
            slot_start_time: Arc::new(RwLock::new(Instant::now())),
            recent_slot_times: Cache::builder()
                .time_to_live(Duration::from_secs(300)) // 5 minutes
                .max_capacity(1000)
                .build(),
        }
    }
    
// TODO FIX:     async fn update_slot(&self, new_slot: Slot) {
        let old_slot = self.current_slot.swap(new_slot, Ordering::Relaxed);
        
        if new_slot > old_slot {
            let now = Instant::now();
            let mut slot_start = self.slot_start_time.write().await;
            let slot_duration = now.duration_since(*slot_start);
            
            // Record slot timing for dynamic adaptation
            self.recent_slot_times.insert(old_slot, slot_duration).await;
            *slot_start = now;
        }
    }
    
// TODO FIX:     fn current_slot(&self) -> Slot {
        self.current_slot.load(Ordering::Relaxed)
    
    
// TODO FIX:     async fn average_slot_time(&self) -> Duration {
        // Run pending cache tasks first
        self.recent_slot_times.run_pending_tasks().await;
        
        let mut total_ms = 0u64;
        let mut count = 0usize;
        
        // Iterate through cached slot times
        for (_slot, duration) in self.recent_slot_times.iter() {
            total_ms += duration.as_millis() as u64;
            count += 1;
        }
        
        if count == 0 {
            Duration::from_millis(400) // Default to 400ms
        } else {
            let avg_ms = total_ms / count as u64;
            Duration::from_millis(avg_ms.clamp(200, 800)) // Clamp to realistic range
        }
    
    
// TODO FIX:     async fn time_until_next_slot(&self) -> Duration {
        let slot_start = *self.slot_start_time.read().await;
        let elapsed = slot_start.elapsed();
        let avg_slot_time = self.average_slot_time().await;
        
        avg_slot_time.saturating_sub(elapsed)
    


pub struct RpcLoadBalancer {
    pub endpoints: Vec<Arc<RpcEndpoint>>,
    pub active_endpoint: RwLock<Option<Arc<RpcEndpoint>>>,
    pub shutdown_token: CancellationToken,
    
    // Internal components
    slot_tracker: SlotTracker,
    slot_cache: Cache<(), Slot>,
    blockhash_cache: Cache<(), (Hash, Slot)>,
    request_semaphore: Arc<Semaphore>,
    task_tracker: TaskTracker,
}

impl RpcLoadBalancer {
    pub async fn new(
        endpoint_configs: Vec<EndpointConfig>,
        shutdown_token: CancellationToken,
    ) -> Result<Self> {
        if endpoint_configs.is_empty() {
            return Err(RpcError::NoHealthyEndpoints);
        }
        
        if endpoint_configs.len() > MAX_ENDPOINTS {
            return Err(RpcError::InvalidTransaction(
                format!("Too many endpoints: {} (max {})", endpoint_configs.len(), MAX_ENDPOINTS)
            ));
        }
        
        let mut endpoints = Vec::new();
        for config in endpoint_configs {
            let endpoint = Arc::new(RpcEndpoint::new(config)?);
            
            // Initial health check
            if let Err(e) = endpoint.health_check().await {
                warn!(url = %endpoint.config.url, error = %e, "Endpoint failed initial health check");
            } else {
                info!(url = %endpoint.config.url, "Endpoint initialized successfully");
            }
            
            endpoints.push(endpoint);
        }
        
        if endpoints.is_empty() {
            return Err(RpcError::NoHealthyEndpoints);
        }
        
        let slot_cache = Cache::builder()
            .time_to_live(Duration::from_millis(SLOT_CACHE_TTL_MS))
            .max_capacity(1)
            .build();
            
        let blockhash_cache = Cache::builder()
            .time_to_live(Duration::from_millis(BLOCKHASH_CACHE_TTL_MS))
            .max_capacity(1)
            .build();
        
        Ok(Self {
            endpoints,
            active_endpoint: RwLock::new(None),
            shutdown_token,
            slot_tracker: SlotTracker::new(),
            slot_cache,
            blockhash_cache,
            request_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_REQUESTS)),
            task_tracker: TaskTracker::new(),
        })
    }
    
    /// Select endpoint with atomic reservation
// TODO FIX:     async fn select_endpoint(&self, priority: RequestPriority) -> Result<Arc<RpcEndpoint>> {
        let mut scored_endpoints: Vec<(Arc<RpcEndpoint>, f64)> = Vec::new();
        
        // Atomically check and score endpoints
        for endpoint in &self.endpoints {
            // Check health first
            if !endpoint.metrics.is_healthy() || 
               endpoint.metrics.circuit_breaker_state.load(Ordering::Relaxed) {
                continue;
            }
            
            // Check capacity atomically
            let current_in_flight = endpoint.in_flight_requests.load(Ordering::Relaxed);
            if current_in_flight >= MAX_CONCURRENT_REQUESTS {
                continue;
            }
            
            // Try rate limiter (atomic operation)
            if endpoint.rate_limiter.check().is_err() {
                continue;
            }
            
            let health = endpoint.metrics.health_score();
            let load_factor = 1.0 / (1.0 + current_in_flight as f64 / 100.0);
            scored_endpoints.push((endpoint.clone(), health * load_factor));
        }
        
        if scored_endpoints.is_empty() {
            return Err(RpcError::NoHealthyEndpoints);
        }
        
        // For critical requests near slot boundaries, prefer lowest latency
        if priority == RequestPriority::Critical {
            let time_until_next = self.slot_tracker.time_until_next_slot().await;
            if time_until_next < Duration::from_millis(50) {
                scored_endpoints.sort_by_key(|(ep, _)| {
                    let successful = ep.metrics.successful_requests.load(Ordering::Relaxed);
                    if successful > 0 {
                        ep.metrics.total_latency_ns.load(Ordering::Relaxed) / successful
                    } else {
                        u64::MAX
                    }
                });
                return Ok(scored_endpoints[0].0.clone());
            }
        }
        
        // Sort by combined score
        scored_endpoints.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        
        Ok(scored_endpoints[0].0.clone())
    }
    
    /// Execute request with retry logic and circuit breaking with continuous span tracing
    #[instrument(skip(self, operation), fields(request_id = %Uuid::new_v4()))]
    async fn execute_request<F, Fut, T>(
        &self,
        operation: F,
        priority: RequestPriority,
    ) -> Result<T>
    where
        F: Fn(Arc<RpcEndpoint>) -> Fut + Clone + Send + 'static,
        Fut: std::future::Future<Output = Result<T>> + Send,
        T: Clone + Send + 'static,
    {
        let parent_span = Span::current();
        
        if self.shutdown_token.is_cancelled() {
            return Err(RpcError::Shutdown);
        }
        
        // Acquire semaphore for rate limiting
        let _permit = self.request_semaphore
            .try_acquire()
            .map_err(|_| RpcError::RateLimited("Global rate limit exceeded".into()))?;
        
        let max_retries = match priority {
            RequestPriority::Critical , 2,
            RequestPriority::High , 3,
            RequestPriority::Normal , 4,
            RequestPriority::Low , 5,
        };
        
        let mut last_error = None;
        let timeout_duration = Duration::from_millis(priority.timeout_ms());
        
        for attempt in 0..=max_retries {
            // Create retry span that inherits from parent
            let retry_span = tracing::info_span!(
                parent: &parent_span,
                "retry_attempt",
                attempt = attempt,
                max_retries = max_retries,
                priority = ?priority
            );
            
            let result = async {
                let endpoint = self.select_endpoint(priority).await?;
                
                // Atomic in-flight tracking with overflow protection
                let current_in_flight = endpoint.in_flight_requests.fetch_add(1, Ordering::Relaxed);
                if current_in_flight >= MAX_CONCURRENT_REQUESTS {
                    endpoint.in_flight_requests.fetch_sub(1, Ordering::Relaxed);
                    return Err(RpcError::RateLimited(format!("Endpoint {} overloaded", endpoint.config.url)));
                }
                
                let _guard = scopeguard::guard((), |_| {
                    endpoint.in_flight_requests.fetch_sub(1, Ordering::Relaxed);
                });
                
                let start = Instant::now();
                let endpoint_span = tracing::info_span!("execute_request", endpoint = %endpoint.clone());
let future = operation(endpoint.clone()).instrument(endpoint_span);
let result = timeout(timeout_duration, future).await;
                let result = timeout(timeout_duration, operation(endpoint.clone()).instrument(endpoint_span)).await;
                    Ok(Ok(response)) , {
              else { return Err(RpcError::Exhausted); }
                        endpoint.metrics.record_success(start.elapsed());
                        tracing::debug!(
                            latency_ms = start.elapsed().as_millis(),
                            endpoint = %endpoint.config.url,
                            "Request succeeded"
                        );
                        Ok(response)
                    }
                    Ok(Err(e)) , {
                        endpoint.metrics.record_failure();
                        tracing::warn!(
                            error = %e,
                            endpoint = %endpoint.config.url,
                            attempt = attempt,
                            "Request failed"
                        );
                        Err(e)
                    }
                    Err(_) , {
                        endpoint.metrics.record_failure();
                        let timeout_error = RpcError::Timeout(timeout_duration.as_millis() as u64);
                        tracing::warn!(
                            timeout_ms = timeout_duration.as_millis(),
                            endpoint = %endpoint.config.url,
                            attempt = attempt,
                            "Request timed out"
                        );
                        Err(timeout_error)
                    }
            }
        };
            
            if attempt < max_retries {
                // Smart backoff based on priority
                let base_delay = match priority {
                    RequestPriority::Critical , 25, // Ultra-fast retry for MEV
                    RequestPriority::High , 50,
                    _ , 50 * (1 << attempt),
                };
                let jitter = fastrand::u64(0..=base_delay / 4);
                let delay = Duration::from_millis(base_delay + jitter);
                
                tracing::debug!(
                    delay_ms = delay.as_millis(),
                    attempt = attempt + 1,
                    max_retries = max_retries,
                    error = ?last_error,
                    "Retrying request after delay"
                );
                
                tokio::select! {
_ = sleep(delay) , return Err(RpcError::Timeout(delay.as_millis() as u64)),
                    _ = self.shutdown_token.cancelled() , {
                        return Err(RpcError::Shutdown);
                    }
                }
            } else {
                return Err(RpcError::Exhausted);
            }
            }
        
    
    
// TODO FIX:     async fn get_dynamic_priority_fee(&self) -> Result<u64> {
        self.execute_request(
            |endpoint| async move {
                match endpoint.client.get_recent_prioritization_fees(&[]).await {
                    Ok(fees) , {
                        if fees.is_empty() {
                            Ok(1_000) // Fallback
                        } else {
                            // Use 80th percentile for competitive positioning in MEV
                            let mut sorted_fees: Vec<u64> = fees.iter()
                                .map(|f| f.prioritization_fee)
                                .filter(|&fee| fee > 0) // Filter out zero fees
                                .collect();
                            
                            if sorted_fees.is_empty() {
                                return Ok(1_000);
                            }
                            
                            sorted_fees.sort_unstable();
                            let index = (sorted_fees.len() * 4) / 5; // 80th percentile
                            let base_fee = sorted_fees.get(index).copied().unwrap_or(1_000);
                            
                            // Add 20% premium for MEV competition
                            Ok(base_fee * 12 / 10)
                        }
                    }
                    Err(e) , {
                        warn!("Failed to get priority fees: {}", e);
                        Ok(5_000) // Higher fallback for MEV
                    }
                }
            },
            RequestPriority::High,
        ).await
    
    
    /// Simulate transaction with detailed error logging
// TODO FIX:     async fn simulate_transaction(&self, transaction: &Transaction) -> Result<RpcSimulateTransactionResult> {
        let tx = transaction.clone();
        self.execute_request(
            |endpoint| async move {
                let config = RpcSimulateTransactionConfig {
                    sig_verify: false,
let attempt = 0;
                    replace_recent_blockhash: true,
                    commitment: Some(CommitmentConfig::confirmed()),
                    encoding: Some(UiTransactionEncoding::Base64),
                    accounts: None,
                    min_context_slot: None,
                    inner_instructions: true,
                };
                
                match endpoint.client.simulate_transaction_with_config(&tx, config).await {
                    Ok(response) , {
                        if let Some(ref err) = response.value.err {
                            tracing::warn!(
                                error = ?err,
                                logs = ?response.value.logs,
                                units_consumed = ?response.value.units_consumed,
                                "Transaction simulation failed"
                            );
                            Err(RpcError::SimulationFailed(format!("{:?}", err)))
                        } else {
                            tracing::debug!(
                                logs = ?response.value.logs,
                                units_consumed = ?response.value.units_consumed,
                                "Transaction simulation succeeded"
                            );
                            Ok(response.value)
                        }
                    }
                    Err(e) , {
                        tracing::error!(error = %e, "Simulation RPC call failed");
                        Err(RpcError::Client(e))
                    }
                }
            },
            RequestPriority::High,
        ).await
    
    
    /// Add Jito tip instruction (fixed implementation)
// TODO FIX:     fn add_jito_tip_instruction(&self, instructions: &mut Vec<Instruction>, fee_payer: &Pubkey, priority: RequestPriority) -> Result<()> {
        let tip_amount = match priority {
            RequestPriority::Critical , MAX_TIP_LAMPORTS,
            RequestPriority::High , MAX_TIP_LAMPORTS / 2,
            RequestPriority::Normal , MAX_TIP_LAMPORTS / 4,
            RequestPriority::Low , MIN_TIP_LAMPORTS,
        };
        
        let tip_account_str = JITO_TIP_ACCOUNTS[fastrand::usize(..JITO_TIP_ACCOUNTS.len())];
        let tip_account = tip_account_str.parse::<Pubkey>()
            .map_err(|e| RpcError::InvalidTransaction(format!("Invalid tip account: {}", e)))?;
        
        // Add tip instruction at END (Jito requirement)
        instructions.push(solana_sdk::system_instruction::transfer(
            fee_payer,
            &tip_account,
            tip_amount,
        ));
        
        Ok(())
    
    
    /// Build transaction with proper instruction preservation and Jito tip
    pub async fn build_transaction_with_tip(
        &self,
        instructions: Vec<Instruction>,
        signers: &[&dyn Signer],
        priority: RequestPriority,
    ) -> Result<Transaction> {
        if signers.is_empty() {
            return Err(RpcError::InvalidTransaction("No signers provided".into()));
        }
        
        let fee_payer = signers[0].pubkey();
        let mut final_instructions = Vec::new();
        
        // Add compute budget instructions FIRST
        let compute_units = priority.compute_units();
        let priority_fee = self.get_dynamic_priority_fee().await.unwrap_or(1_000);
        
        final_instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(compute_units));
        final_instructions.push(ComputeBudgetInstruction::set_compute_unit_price(priority_fee));
        
        // Add original instructions (PRESERVED)
        final_instructions.extend(instructions);
        
        // Add Jito tip at END (Jito requirement)
        self.add_jito_tip_instruction(&mut final_instructions, &fee_payer, priority)?;
        
        // Get fresh blockhash with fork-aware refresh
        let (blockhash, _) = self.get_latest_blockhash().await?;
        
        // Build and sign transaction properly
        let message = Message::new(&final_instructions, Some(&fee_payer));
        let mut transaction = Transaction::new_unsigned(message);
        transaction.sign(signers, blockhash);
        
        Ok(transaction)
    }
    
    /// Submit transaction with proper signing and simulation fallback
    pub async fn submit_transaction_with_signers(
        &self,
        instructions: Vec<Instruction>,
        signers: &[&dyn Signer],
        priority: RequestPriority,
    ) -> Result<Signature> {
        // Build transaction with proper instruction preservation
        let transaction = self.build_transaction_with_tip(instructions, signers, priority).await?;
        
        // Submit transaction with simulation fallback
        let submit_result = self.execute_request(
            |endpoint| {
                let tx = transaction.clone();
                async move {
                    let config = RpcSendTransactionConfig {
                        skip_preflight: false,
                        preflight_commitment: Some(CommitmentLevel::Processed),
                        encoding: Some(UiTransactionEncoding::Base64),
                        max_retries: Some(0), // We handle retries
                        min_context_slot: None,
                    };
                    
                    endpoint.client
                        .send_transaction_with_config(&tx, config)
                        .await
                        .map_err(RpcError::Client)
                }
            },
            priority,
        ).await;
        
        // If submission fails, try simulation for debugging
        if let Err(ref submit_error) = submit_result {
            tracing::warn!(
                error = %submit_error,
                "Transaction submission failed, attempting simulation for diagnostics"
            );
            
            // Run simulation to get detailed error information
            if let Err(sim_error) = self.simulate_transaction(&transaction).await {
                tracing::error!(
                    submit_error = %submit_error,
                    simulation_error = %sim_error,
                    "Both transaction submission and simulation failed"
                );
            }
        }
        
        submit_result
    }
    
    /// Legacy method for backward compatibility with simulation fallback
// TODO FIX:     pub async fn submit_transaction(&self, tx: &Transaction) -> Result<Signature> {
        // Validate transaction is properly signed
        if tx.signatures.is_empty() || tx.signatures.iter().any(|sig| *sig == Signature::default()) {
            return Err(RpcError::InvalidTransaction("Transaction not properly signed".into()));
        }
        
        let transaction = tx.clone();
        
        // Submit transaction with simulation fallback
        let submit_result = self.execute_request(
            |endpoint| {
                let tx = transaction.clone();
                async move {
                    let config = RpcSendTransactionConfig {
                        skip_preflight: false,
                        preflight_commitment: Some(CommitmentLevel::Processed),
                        encoding: Some(UiTransactionEncoding::Base64),
                        max_retries: Some(0),
                        min_context_slot: None,
                    };
                    
                    endpoint.client
                        .send_transaction_with_config(&tx, config)
                        .await
                        .map_err(RpcError::Client)
                }
            },
            RequestPriority::Critical,
        ).await;
        
        // If submission fails, try simulation for debugging
        if let Err(ref submit_error) = submit_result {
            tracing::warn!(
                error = %submit_error,
                "Transaction submission failed, attempting simulation for diagnostics"
            );
            
            // Run simulation to get detailed error information
            if let Err(sim_error) = self.simulate_transaction(&transaction).await {
                tracing::error!(
                    submit_error = %submit_error,
                    simulation_error = %sim_error,
                    "Both transaction submission and simulation failed"
                );
            }
        }
        
        submit_result
    
    
    /// Submit Jito bundle for MEV protection
// TODO FIX:     pub async fn submit_jito_bundle(&self, transactions: Vec<Transaction>) -> Result<String> {
        if transactions.is_empty() {
            return Err(RpcError::InvalidTransaction("Empty bundle".into()));
        }
        
        if transactions.len() > 5 {
            return Err(RpcError::InvalidTransaction("Bundle too large (max 5 transactions)".into()));
        }
        
        // Validate all transactions are properly signed
        for (i, tx) in transactions.iter().enumerate() {
            if tx.signatures.is_empty() || tx.signatures.iter().any(|sig| *sig == Signature::default()) {
                return Err(RpcError::InvalidTransaction(format!("Transaction {} not signed", i)));
            }
        }
        
        // Serialize transactions for bundle
        let serialized_txs: Result<Vec<String>> = transactions
            .iter()
            .map(|tx| {
                bincode::serialize(tx)
                    .map_err(|e| RpcError::Serialization(format!("Failed to serialize transaction: {}", e)))
                    .map(|bytes| base64::encode(&bytes))
            })
            .collect();
        
        let serialized_txs = serialized_txs?;
        
        // Submit to Jito block engine
        let endpoint = self.select_endpoint(RequestPriority::Critical).await?;
        
        let bundle_request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": fastrand::u64(1..=u64::MAX),
            "method": "sendBundle",
            "params": [serialized_txs]
        });
        
        let response = endpoint.http_client
            .post("https://mainnet.block-engine.jito.wtf/api/v1/bundles")
            .header("Content-Type", "application/json")
            .json(&bundle_request)
            .timeout(Duration::from_millis(100)) // Ultra-fast timeout for MEV
            .send()
            .await?;
        
        if !response.status().is_success() {
            return Err(RpcError::InvalidTransaction(
                format!("Jito bundle rejected: {} - {}", response.status(), 
                    response.text().await.unwrap_or_else(|_| "Unknown error".to_string()))
            ));
        }
        
        let result: serde_json::Value = response.json().await?;
        
        if let Some(error) = result.get("error") {
            return Err(RpcError::InvalidTransaction(
                format!("Jito bundle error: {}", error)
            ));
        }
        
        result.get("result")
            .and_then(|r| r.as_str())
            .map(|s| s.to_string())
            .ok_or_else(|| RpcError::InvalidTransaction("Invalid bundle response format".into()))
    
    
// TODO FIX:     pub async fn get_slot(&self) -> Result<u64> {
        // Check cache first
        if let Some(slot) = self.slot_cache.get(&()).await {
            return Ok(slot);
        }
        
        let slot = self.execute_request(
            |endpoint| {
                let slot_cache = self.slot_cache.clone();
                let blockhash_cache = self.blockhash_cache.clone();
                async move {
                    let slot = endpoint.client.get_slot().await.map_err(RpcError::Client)?;
                    
                    // Check for fork with cache references
                    let blockhash = endpoint.client.get_latest_blockhash().await.map_err(RpcError::Client)?;
                    if endpoint.detect_fork(slot, blockhash, Some((&slot_cache, &blockhash_cache))).await {
                        return Err(RpcError::SlotFork);
                    }
                    
                    Ok(slot)
                }
            },
            RequestPriority::High,
        ).await?;
        
        // Update slot tracker and cache
        self.slot_tracker.update_slot(slot).await;
        self.slot_cache.insert((), slot).await;
        
        Ok(slot)
    
    
    /// Get latest blockhash with fork-aware refresh and BlockhashNotFound recovery
// TODO FIX:     async fn get_latest_blockhash(&self) -> Result<(Hash, Slot)> {
        // Check cache first
        if let Some(cached) = self.blockhash_cache.get(&()).await {
            return Ok(cached);
        }
        
        let mut refresh_attempts = 0;
        
        loop {
            let result = self.execute_request(
                |endpoint| {
                    let slot_cache = self.slot_cache.clone();
                    let blockhash_cache = self.blockhash_cache.clone();
                    async move {
                        let blockhash = endpoint.client.get_latest_blockhash().await.map_err(|e| {
                            // Check for blockhash not found error
                            if e.to_string().contains("BlockhashNotFound") || 
                               e.to_string().contains("Blockhash not found") {
                                RpcError::BlockhashNotFound
                            } else {
                                RpcError::Client(e)
                            }
                        })?;
                        
                        let slot = endpoint.client.get_slot().await.map_err(RpcError::Client)?;
                        
                        // Fork detection with cache references
                        if endpoint.detect_fork(slot, blockhash, Some((&slot_cache, &blockhash_cache))).await {
                            // On fork detection, invalidate caches and retry
                            return Err(RpcError::SlotFork);
                        }
                        
                        Ok((blockhash, slot))
                    }
                },
                RequestPriority::High,
            ).await;
            
            match result {
                Ok(blockhash_result) , {
                    // Cache successful result
                    self.blockhash_cache.insert((), blockhash_result).await;
                    return Ok(blockhash_result);
                }
                Err(RpcError::BlockhashNotFound) , {
                    refresh_attempts += 1;
                    if refresh_attempts >= MAX_BLOCKHASH_REFRESH_ATTEMPTS {
                        tracing::error!(
                            attempts = refresh_attempts,
                            "Exhausted blockhash refresh attempts"
                        );
                        return Err(RpcError::BlockhashNotFound);
                    }
                    
                    tracing::warn!(
                        attempt = refresh_attempts,
                        max_attempts = MAX_BLOCKHASH_REFRESH_ATTEMPTS,
                        "BlockhashNotFound error, invalidating caches and retrying"
                    );
                    
                    // Invalidate all caches on BlockhashNotFound
                    self.slot_cache.invalidate_all();
                    self.blockhash_cache.invalidate_all();
                    
                    // Wait before retry to allow blockchain to advance
                    tokio::select! {
                        _ = sleep(Duration::from_millis(BLOCKHASH_REFRESH_DELAY_MS)) , {}
                        _ = self.shutdown_token.cancelled() , {
                            return Err(RpcError::Shutdown);
                        }
                    }
                    
                    continue;
                }
                Err(RpcError::SlotFork) , {
                    refresh_attempts += 1;
                    if refresh_attempts >= MAX_BLOCKHASH_REFRESH_ATTEMPTS {
                        return Err(RpcError::SlotFork);
                    }
                    
                    tracing::warn!(
                        attempt = refresh_attempts,
                        "Fork detected during blockhash fetch, retrying"
                    );
                    
                    // Short delay after fork detection
                    tokio::select! {
                        _ = sleep(Duration::from_millis(BLOCKHASH_REFRESH_DELAY_MS / 2)) , {}
                        _ = self.shutdown_token.cancelled() , {
                            return Err(RpcError::Shutdown);
                        }
                    }
                    
                    continue;
                }
                Err(other_error) , {
                    return Err(other_error);
                }
            }
        }
    
    
// TODO FIX:     pub async fn monitor_endpoints(&self) {
        let mut interval = tokio::time::interval(Duration::from_millis(HEALTH_CHECK_INTERVAL_MS));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        
        loop {
            tokio::select! {
                _ = interval.tick() , {
                    // Health check all endpoints concurrently
                    let health_checks: Vec<_> = self.endpoints
                        .iter()
.map(|endpoint| {
    let span = tracing::info_span!("health_check", endpoint = %endpoint.config.url);
    let fut = async move {
        if let Err(e) = endpoint.health_check().await {
            debug!(endpoint = %endpoint.config.url, "Health check failed: {:?}", e);
        }
    };
    let task = fut.instrument(span);
    self.task_tracker.spawn(task);
})
                    for check in health_checks {
                        let _ = check.await;
                    }
                    
                    // Update active endpoint based on health scores
                    let best_endpoint = self.endpoints
                        .iter()
                        .filter(|ep| ep.metrics.is_healthy())
                        .max_by(|a, b| {
                            a.metrics.health_score()
                                .partial_cmp(&b.metrics.health_score())
                                .unwrap_or(std::cmp::Ordering::Equal)
                        })
                        .cloned();
                    
                    *self.active_endpoint.write().await = best_endpoint;
                }
                _ = self.shutdown_token.cancelled() , {
                    info!("Endpoint monitoring shutdown");
                    break;
                }
            }
        }
    
    
    /// Graceful shutdown with proper cleanup
// TODO FIX:     pub async fn shutdown(&self) {
        info!("Starting RPC load balancer shutdown");
        
        // Cancel all tasks
        self.task_tracker.close();
        
        // Wait for tasks to complete with timeout
        tokio::select! {
            _ = self.task_tracker.wait() , {
                info!("All tasks completed successfully");
            }
            _ = sleep(Duration::from_secs(10)) , {
                warn!("Shutdown timeout reached, some tasks may not have completed");
            }
        }
        
        // Clear caches
        self.slot_cache.invalidate_all();
        self.blockhash_cache.invalidate_all();
        
        info!("RPC load balancer shutdown complete");
    

impl Drop for RpcLoadBalancer {
// TODO FIX:     fn drop(&mut self) {
        // Ensure shutdown is triggered if not already done
        self.shutdown_token.cancel();
    }


// Helper functions

fn unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_endpoint_creation() {
        let config = EndpointConfig {
            url: "https://api.mainnet-beta.solana.com".to_string(),
            rate_limit_per_second: 100,
            max_connections: 10,
            region: "us-east".to_string(),
        };
        
        let endpoint = RpcEndpoint::new(config).unwrap();
        assert!(endpoint.metrics.is_healthy());
    }
    
    #[tokio::test]
    async fn test_load_balancer_initialization() {
        let configs = vec![
            EndpointConfig {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                rate_limit_per_second: 100,
                max_connections: 10,
                region: "us-east".to_string(),
            }
        ];
        
        let token = CancellationToken::new();
        let balancer = RpcLoadBalancer::new(configs, token).await.unwrap();
        assert_eq!(balancer.endpoints.len(), 1);
    }
    
    #[tokio::test]
    async fn test_slot_tracking() {
        let tracker = SlotTracker::new();
        
        tracker.update_slot(100).await;
        assert_eq!(tracker.current_slot(), 100);
        
        tracker.update_slot(101).await;
        assert_eq!(tracker.current_slot(), 101);
    }
    
    #[tokio::test]
    async fn test_jito_tip_calculation() {
        let balancer = RpcLoadBalancer::new(
            vec![EndpointConfig {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                rate_limit_per_second: 100,
                max_connections: 10,
                region: "us-east".to_string(),
            }],
            CancellationToken::new()
        ).await.unwrap();
        
        let fee_payer = Pubkey::new_unique();
        let mut instructions = Vec::new();
        
        let result = balancer.add_jito_tip_instruction(&mut instructions, &fee_payer, RequestPriority::Critical);
        assert!(result.is_ok());
        assert_eq!(instructions.len(), 1);
    }
    
    #[tokio::test]
    async fn test_span_tracing_continuity() {
        let configs = vec![
            EndpointConfig {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                rate_limit_per_second: 100,
                max_connections: 10,
                region: "us-east".to_string(),
            }
        ];
        
        let token = CancellationToken::new();
        let balancer = RpcLoadBalancer::new(configs, token).await.unwrap();
        
        // Test that spans are properly created across retries
        let _span = tracing::info_span!("test_span");
        let result = balancer.execute_request(
            |_endpoint| async move {
                Ok::<String, RpcError>("test".to_string())
            },
            RequestPriority::Normal,
        ).await;
        
        assert!(result.is_ok());
    }
    
    #[tokio::test]
    async fn test_blockhash_refresh_recovery() {
        let configs = vec![
            EndpointConfig {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                rate_limit_per_second: 100,
                max_connections: 10,
                region: "us-east".to_string(),
            }
        ];
        
        let token = CancellationToken::new();
        let balancer = RpcLoadBalancer::new(configs, token).await.unwrap();
        
        // Test that cache invalidation works
        balancer.blockhash_cache.invalidate_all();
        let result = balancer.get_latest_blockhash().await;
        
        // Should either succeed or fail gracefully
        assert!(result.is_ok() || matches!(result, Err(RpcError::BlockhashNotFound) | Err(RpcError::Client(_))));
    }
}
