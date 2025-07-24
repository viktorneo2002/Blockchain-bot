use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, Mutex, Semaphore};
use tokio::time::{interval, timeout};
use serde::{Deserialize, Serialize};
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
use futures;

const MAX_LATENCY_SAMPLES: usize = 100;
const HEALTH_CHECK_INTERVAL: Duration = Duration::from_secs(5);
const LATENCY_UPDATE_INTERVAL: Duration = Duration::from_millis(500);
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_CONCURRENT_CHECKS: usize = 10;
const FAILURE_THRESHOLD: u32 = 3;
const SUCCESS_THRESHOLD: u32 = 2;
const LATENCY_WEIGHT: f64 = 0.7;
const SUCCESS_RATE_WEIGHT: f64 = 0.3;
const MIN_SUCCESS_RATE: f64 = 0.85;
const JITTER_RANGE: u64 = 50;
const CIRCUIT_BREAKER_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_RETRY_ATTEMPTS: u32 = 3;
const BACKOFF_BASE: u64 = 100;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeographicEndpoint {
    pub id: String,
    pub url: String,
    pub region: GeographicRegion,
    pub priority: u8,
    pub weight: f64,
    pub max_connections: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GeographicRegion {
    UsEast,
    UsWest,
    Europe,
    Asia,
    Custom(u8),
}

#[derive(Debug, Clone)]
struct EndpointMetrics {
    latency_samples: Arc<Mutex<VecDeque<u64>>>,
    success_count: Arc<AtomicU64>,
    failure_count: Arc<AtomicU64>,
    last_success: Arc<AtomicU64>,
    last_failure: Arc<AtomicU64>,
    consecutive_failures: Arc<AtomicU64>,
    circuit_breaker_state: Arc<RwLock<CircuitBreakerState>>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum CircuitBreakerState {
    Closed,
    Open(Instant),
    HalfOpen,
}

#[derive(Debug)]
struct EndpointConnection {
    client: Arc<RpcClient>,
    endpoint: GeographicEndpoint,
    metrics: EndpointMetrics,
    active_requests: Arc<AtomicUsize>,
    connection_pool: Arc<Semaphore>,
}

pub struct GeographicRelayOptimizer {
    endpoints: Arc<RwLock<HashMap<String, Arc<EndpointConnection>>>>,
    region_endpoints: Arc<RwLock<HashMap<GeographicRegion, Vec<String>>>>,
    primary_region: GeographicRegion,
    fallback_regions: Vec<GeographicRegion>,
    metrics_tx: mpsc::UnboundedSender<MetricsUpdate>,
    shutdown_tx: mpsc::Sender<()>,
}

#[derive(Debug)]
enum MetricsUpdate {
    Latency(String, u64),
    Success(String),
    Failure(String),
}

impl EndpointMetrics {
    fn new() -> Self {
        Self {
            latency_samples: Arc::new(Mutex::new(VecDeque::with_capacity(MAX_LATENCY_SAMPLES))),
            success_count: Arc::new(AtomicU64::new(0)),
            failure_count: Arc::new(AtomicU64::new(0)),
            last_success: Arc::new(AtomicU64::new(0)),
            last_failure: Arc::new(AtomicU64::new(0)),
            consecutive_failures: Arc::new(AtomicU64::new(0)),
            circuit_breaker_state: Arc::new(RwLock::new(CircuitBreakerState::Closed)),
        }
    }

    async fn add_latency_sample(&self, latency: u64) {
        let mut samples = self.latency_samples.lock().await;
        if samples.len() >= MAX_LATENCY_SAMPLES {
            samples.pop_front();
        }
        samples.push_back(latency);
    }

    async fn get_average_latency(&self) -> Option<f64> {
        let samples = self.latency_samples.lock().await;
        if samples.is_empty() {
            return None;
        }
        let sum: u64 = samples.iter().sum();
        Some(sum as f64 / samples.len() as f64)
    }

    async fn get_p99_latency(&self) -> Option<u64> {
        let mut samples = self.latency_samples.lock().await.clone();
        if samples.is_empty() {
            return None;
        }
        let mut sorted: Vec<u64> = samples.into_iter().collect();
        sorted.sort_unstable();
        let idx = ((sorted.len() as f64 * 0.99) as usize).min(sorted.len() - 1);
        Some(sorted[idx])
    }

    fn record_success(&self) {
        self.success_count.fetch_add(1, Ordering::Relaxed);
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.last_success.store(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            Ordering::Relaxed,
        );
        
        let mut state = self.circuit_breaker_state.write().unwrap();
        if matches!(*state, CircuitBreakerState::HalfOpen) {
            *state = CircuitBreakerState::Closed;
        }
    }

    fn record_failure(&self) {
        self.failure_count.fetch_add(1, Ordering::Relaxed);
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        self.last_failure.store(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            Ordering::Relaxed,
        );

        if failures >= FAILURE_THRESHOLD as u64 {
            let mut state = self.circuit_breaker_state.write().unwrap();
            *state = CircuitBreakerState::Open(Instant::now());
        }
    }

    fn get_success_rate(&self) -> f64 {
        let success = self.success_count.load(Ordering::Relaxed);
        let failure = self.failure_count.load(Ordering::Relaxed);
        let total = success + failure;
        if total == 0 {
            return 1.0;
        }
        success as f64 / total as f64
    }

    fn is_circuit_open(&self) -> bool {
        let state = self.circuit_breaker_state.read().unwrap();
        match *state {
            CircuitBreakerState::Closed => false,
            CircuitBreakerState::Open(since) => {
                if since.elapsed() > CIRCUIT_BREAKER_TIMEOUT {
                    drop(state);
                    let mut state = self.circuit_breaker_state.write().unwrap();
                    *state = CircuitBreakerState::HalfOpen;
                    false
                } else {
                    true
                }
            }
            CircuitBreakerState::HalfOpen => false,
        }
    }
}

impl GeographicRelayOptimizer {
    pub async fn new(
        endpoints: Vec<GeographicEndpoint>,
        primary_region: GeographicRegion,
    ) -> Result<Arc<Self>, Box<dyn std::error::Error>> {
        let (metrics_tx, mut metrics_rx) = mpsc::unbounded_channel();
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);

        let mut endpoint_map = HashMap::new();
        let mut region_map: HashMap<GeographicRegion, Vec<String>> = HashMap::new();
        let mut fallback_regions = Vec::new();

        for endpoint in endpoints {
            let client = Arc::new(RpcClient::new_with_timeout_and_commitment(
                endpoint.url.clone(),
                CONNECTION_TIMEOUT,
                CommitmentConfig::confirmed(),
            ));

            let connection = Arc::new(EndpointConnection {
                client,
                endpoint: endpoint.clone(),
                metrics: EndpointMetrics::new(),
                active_requests: Arc::new(AtomicUsize::new(0)),
                connection_pool: Arc::new(Semaphore::new(endpoint.max_connections)),
            });

            endpoint_map.insert(endpoint.id.clone(), connection);
            region_map
                .entry(endpoint.region)
                .or_insert_with(Vec::new)
                .push(endpoint.id.clone());

            if endpoint.region != primary_region && !fallback_regions.contains(&endpoint.region) {
                fallback_regions.push(endpoint.region);
            }
        }

        fallback_regions.sort_by_key(|r| match r {
            GeographicRegion::UsEast => 1,
            GeographicRegion::UsWest => 2,
            GeographicRegion::Europe => 3,
            GeographicRegion::Asia => 4,
            GeographicRegion::Custom(_) => 5,
        });

        let optimizer = Arc::new(Self {
            endpoints: Arc::new(RwLock::new(endpoint_map)),
            region_endpoints: Arc::new(RwLock::new(region_map)),
            primary_region,
            fallback_regions,
            metrics_tx,
            shutdown_tx,
        });

        let opt_clone = optimizer.clone();
        tokio::spawn(async move {
            let mut health_interval = interval(HEALTH_CHECK_INTERVAL);
            let mut latency_interval = interval(LATENCY_UPDATE_INTERVAL);

            loop {
                tokio::select! {
                    _ = health_interval.tick() => {
                        opt_clone.perform_health_checks().await;
                    }
                    _ = latency_interval.tick() => {
                        opt_clone.update_latency_metrics().await;
                    }
                    _ = &mut shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });

        let opt_clone = optimizer.clone();
        tokio::spawn(async move {
            while let Some(update) = metrics_rx.recv().await {
                match update {
                    MetricsUpdate::Latency(id, latency) => {
                        if let Some(conn) = opt_clone.get_connection(&id).await {
                            conn.metrics.add_latency_sample(latency).await;
                        }
                    }
                    MetricsUpdate::Success(id) => {
                        if let Some(conn) = opt_clone.get_connection(&id).await {
                            conn.metrics.record_success();
                        }
                    }
                    MetricsUpdate::Failure(id) => {
                        if let Some(conn) = opt_clone.get_connection(&id).await {
                            conn.metrics.record_failure();
                        }
                    }
                }
            }
        });

        Ok(optimizer)
    }

    async fn get_connection(&self, id: &str) -> Option<Arc<EndpointConnection>> {
        self.endpoints.read().unwrap().get(id).cloned()
    }

    async fn perform_health_checks(&self) {
        let endpoints = self.endpoints.read().unwrap().clone();
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_CHECKS));

        let mut tasks = Vec::new();
        for (id, connection) in endpoints {
            let sem = semaphore.clone();
            let conn = connection.clone();
            let metrics_tx = self.metrics_tx.clone();

            tasks.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                let start = Instant::now();

                match timeout(CONNECTION_TIMEOUT, conn.client.get_health()).await {
                    Ok(Ok(_)) => {
                        let latency = start.elapsed().as_millis() as u64;
                        let _ = metrics_tx.send(MetricsUpdate::Latency(id.clone(), latency));
                        let _ = metrics_tx.send(MetricsUpdate::Success(id));
                    }
                    _ => {
                        let _ = metrics_tx.send(MetricsUpdate::Failure(id));
                    }
                }
            }));
        }

        futures::future::join_all(tasks).await;
    }

    async fn update_latency_metrics(&self) {
        let endpoints = self.endpoints.read().unwrap().clone();
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_CHECKS));

        let mut tasks = Vec::new();
        for (id, connection) in endpoints {
            let sem = semaphore.clone();
            let conn = connection.clone();
            let metrics_tx = self.metrics_tx.clone();

            tasks.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                let start = Instant::now();

                match timeout(Duration::from_millis(500), conn.client.get_slot()).await {
                    Ok(Ok(_)) => {
                        let latency = start.elapsed().as_micros() as u64;
                        let _ = metrics_tx.send(MetricsUpdate::Latency(id, latency));
                    }
                    _ => {}
                }
            }));
        }

        futures::future::join_all(tasks).await;
    }

    pub async fn get_optimal_client(&self) -> Option<Arc<RpcClient>> {
        let mut candidates = self.get_healthy_endpoints_by_region(self.primary_region).await;

        if candidates.is_empty() {
            for region in &self.fallback_regions {
                                candidates = self.get_healthy_endpoints_by_region(*region).await;
                if !candidates.is_empty() {
                    break;
                }
            }
        }

        if candidates.is_empty() {
            return None;
        }

        let optimal = self.select_optimal_endpoint(candidates).await?;
        Some(optimal.client.clone())
    }

    async fn get_healthy_endpoints_by_region(&self, region: GeographicRegion) -> Vec<Arc<EndpointConnection>> {
        let region_endpoints = self.region_endpoints.read().unwrap();
        let endpoint_ids = match region_endpoints.get(&region) {
            Some(ids) => ids.clone(),
            None => return Vec::new(),
        };

        let endpoints = self.endpoints.read().unwrap();
        let mut healthy = Vec::new();

        for id in endpoint_ids {
            if let Some(conn) = endpoints.get(&id) {
                if !conn.metrics.is_circuit_open() && conn.metrics.get_success_rate() >= MIN_SUCCESS_RATE {
                    healthy.push(conn.clone());
                }
            }
        }

        healthy
    }

    async fn select_optimal_endpoint(&self, candidates: Vec<Arc<EndpointConnection>>) -> Option<Arc<EndpointConnection>> {
        if candidates.is_empty() {
            return None;
        }

        let mut scored_endpoints = Vec::new();

        for conn in candidates {
            let avg_latency = conn.metrics.get_average_latency().await.unwrap_or(f64::MAX);
            let success_rate = conn.metrics.get_success_rate();
            let active_requests = conn.active_requests.load(Ordering::Relaxed);
            
            let normalized_latency = 1.0 / (1.0 + avg_latency / 1000.0);
            let load_factor = 1.0 / (1.0 + active_requests as f64 / conn.endpoint.max_connections as f64);
            
            let score = (normalized_latency * LATENCY_WEIGHT + 
                        success_rate * SUCCESS_RATE_WEIGHT) * 
                        load_factor * conn.endpoint.weight;

            scored_endpoints.push((conn, score));
        }

        scored_endpoints.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let top_candidates = scored_endpoints.iter()
            .take(3)
            .filter(|(_, score)| *score > 0.0)
            .collect::<Vec<_>>();

        if top_candidates.is_empty() {
            return scored_endpoints.into_iter().next().map(|(conn, _)| conn);
        }

        let total_score: f64 = top_candidates.iter().map(|(_, score)| score).sum();
        let mut rng = thread_rng();
        let mut random_value = rng.gen_range(0.0..total_score);

        for (conn, score) in top_candidates {
            random_value -= score;
            if random_value <= 0.0 {
                return Some(conn.clone());
            }
        }

        top_candidates.first().map(|(conn, _)| conn.clone())
    }

    pub async fn execute_with_geographic_optimization<T, F, Fut>(
        &self,
        operation: F,
    ) -> Result<T, Box<dyn std::error::Error + Send + Sync>>
    where
        F: Fn(Arc<RpcClient>) -> Fut + Send + Sync,
        Fut: std::future::Future<Output = Result<T, Box<dyn std::error::Error + Send + Sync>>> + Send,
        T: Send + 'static,
    {
        let mut attempts = 0;
        let mut last_error = None;

        while attempts < MAX_RETRY_ATTEMPTS {
            let connection = match self.select_connection_with_fallback().await {
                Some(conn) => conn,
                None => {
                    return Err("No healthy endpoints available".into());
                }
            };

            let permit = match timeout(
                Duration::from_millis(100),
                connection.connection_pool.acquire()
            ).await {
                Ok(Ok(permit)) => permit,
                _ => {
                    attempts += 1;
                    continue;
                }
            };

            connection.active_requests.fetch_add(1, Ordering::Relaxed);
            let start = Instant::now();

            let result = operation(connection.client.clone()).await;

            connection.active_requests.fetch_sub(1, Ordering::Relaxed);
            drop(permit);

            match result {
                Ok(value) => {
                    let latency = start.elapsed().as_micros() as u64;
                    let _ = self.metrics_tx.send(MetricsUpdate::Latency(connection.endpoint.id.clone(), latency));
                    let _ = self.metrics_tx.send(MetricsUpdate::Success(connection.endpoint.id.clone()));
                    return Ok(value);
                }
                Err(e) => {
                    let _ = self.metrics_tx.send(MetricsUpdate::Failure(connection.endpoint.id.clone()));
                    last_error = Some(e);
                    attempts += 1;

                    if attempts < MAX_RETRY_ATTEMPTS {
                        let backoff = BACKOFF_BASE * (1 << attempts.min(4));
                        let jitter = rand::random::<u64>() % JITTER_RANGE;
                        tokio::time::sleep(Duration::from_millis(backoff + jitter)).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| "Max retry attempts exceeded".into()))
    }

    async fn select_connection_with_fallback(&self) -> Option<Arc<EndpointConnection>> {
        let mut regions = vec![self.primary_region];
        regions.extend(self.fallback_regions.iter().cloned());

        for region in regions {
            let candidates = self.get_healthy_endpoints_by_region(region).await;
            if let Some(conn) = self.select_optimal_endpoint(candidates).await {
                return Some(conn);
            }
        }

        let all_endpoints: Vec<Arc<EndpointConnection>> = self.endpoints.read().unwrap()
            .values()
            .cloned()
            .collect();

        self.select_optimal_endpoint(all_endpoints).await
    }

    pub async fn get_best_endpoint_for_region(&self, region: GeographicRegion) -> Option<GeographicEndpoint> {
        let candidates = self.get_healthy_endpoints_by_region(region).await;
        self.select_optimal_endpoint(candidates).await
            .map(|conn| conn.endpoint.clone())
    }

    pub async fn get_endpoint_metrics(&self, endpoint_id: &str) -> Option<EndpointHealthMetrics> {
        let connection = self.get_connection(endpoint_id).await?;
        
        Some(EndpointHealthMetrics {
            endpoint_id: endpoint_id.to_string(),
            average_latency: connection.metrics.get_average_latency().await,
            p99_latency: connection.metrics.get_p99_latency().await,
            success_rate: connection.metrics.get_success_rate(),
            active_requests: connection.active_requests.load(Ordering::Relaxed),
            circuit_breaker_open: connection.metrics.is_circuit_open(),
            consecutive_failures: connection.metrics.consecutive_failures.load(Ordering::Relaxed),
        })
    }

    pub async fn get_all_metrics(&self) -> HashMap<String, EndpointHealthMetrics> {
        let endpoints = self.endpoints.read().unwrap();
        let mut metrics = HashMap::new();

        for (id, _) in endpoints.iter() {
            if let Some(m) = self.get_endpoint_metrics(id).await {
                metrics.insert(id.clone(), m);
            }
        }

        metrics
    }

    pub async fn force_circuit_breaker_reset(&self, endpoint_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let connection = self.get_connection(endpoint_id).await
            .ok_or("Endpoint not found")?;

        let mut state = connection.metrics.circuit_breaker_state.write().unwrap();
        *state = CircuitBreakerState::HalfOpen;
        connection.metrics.consecutive_failures.store(0, Ordering::Relaxed);

        Ok(())
    }

    pub async fn update_endpoint_weight(&self, endpoint_id: &str, new_weight: f64) -> Result<(), Box<dyn std::error::Error>> {
        let mut endpoints = self.endpoints.write().unwrap();
        let connection = endpoints.get_mut(endpoint_id)
            .ok_or("Endpoint not found")?;

        Arc::get_mut(connection)
            .ok_or("Cannot modify endpoint in use")?
            .endpoint
            .weight = new_weight.max(0.0).min(1.0);

        Ok(())
    }

    pub async fn add_endpoint(&self, endpoint: GeographicEndpoint) -> Result<(), Box<dyn std::error::Error>> {
        let client = Arc::new(RpcClient::new_with_timeout_and_commitment(
            endpoint.url.clone(),
            CONNECTION_TIMEOUT,
            CommitmentConfig::confirmed(),
        ));

        let connection = Arc::new(EndpointConnection {
            client,
            endpoint: endpoint.clone(),
            metrics: EndpointMetrics::new(),
            active_requests: Arc::new(AtomicUsize::new(0)),
            connection_pool: Arc::new(Semaphore::new(endpoint.max_connections)),
        });

        let mut endpoints = self.endpoints.write().unwrap();
        let mut region_endpoints = self.region_endpoints.write().unwrap();

        endpoints.insert(endpoint.id.clone(), connection);
        region_endpoints
            .entry(endpoint.region)
            .or_insert_with(Vec::new)
            .push(endpoint.id.clone());

        Ok(())
    }

    pub async fn remove_endpoint(&self, endpoint_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut endpoints = self.endpoints.write().unwrap();
        let connection = endpoints.remove(endpoint_id)
            .ok_or("Endpoint not found")?;

        let mut region_endpoints = self.region_endpoints.write().unwrap();
        if let Some(region_list) = region_endpoints.get_mut(&connection.endpoint.region) {
            region_list.retain(|id| id != endpoint_id);
        }

        Ok(())
    }

    pub async fn get_region_statistics(&self, region: GeographicRegion) -> RegionStatistics {
        let endpoints = self.get_healthy_endpoints_by_region(region).await;
        let total_endpoints = self.region_endpoints.read().unwrap()
            .get(&region)
            .map(|ids| ids.len())
            .unwrap_or(0);

        let mut total_latency = 0.0;
        let mut latency_count = 0;
        let mut total_success = 0;
        let mut total_requests = 0;

        for conn in &endpoints {
            if let Some(avg) = conn.metrics.get_average_latency().await {
                total_latency += avg;
                latency_count += 1;
            }

            let success = conn.metrics.success_count.load(Ordering::Relaxed);
            let failure = conn.metrics.failure_count.load(Ordering::Relaxed);
            total_success += success;
            total_requests += success + failure;
        }

        RegionStatistics {
            region,
            healthy_endpoints: endpoints.len(),
            total_endpoints,
            average_latency: if latency_count > 0 { Some(total_latency / latency_count as f64) } else { None },
            overall_success_rate: if total_requests > 0 { total_success as f64 / total_requests as f64 } else { 1.0 },
        }
    }

    pub async fn shutdown(self: Arc<Self>) {
        let _ = self.shutdown_tx.send(()).await;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EndpointHealthMetrics {
    pub endpoint_id: String,
    pub average_latency: Option<f64>,
    pub p99_latency: Option<u64>,
    pub success_rate: f64,
    pub active_requests: usize,
    pub circuit_breaker_open: bool,
    pub consecutive_failures: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegionStatistics {
    pub region: GeographicRegion,
    pub healthy_endpoints: usize,
    pub total_endpoints: usize,
    pub average_latency: Option<f64>,
    pub overall_success_rate: f64,
}

impl Default for GeographicRegion {
    fn default() -> Self {
        GeographicRegion::UsEast
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_geographic_relay_optimizer() {
        let endpoints = vec![
            GeographicEndpoint {
                id: "us-east-1".to_string(),
                url: "https://api.mainnet-beta.solana.com".to_string(),
                region: GeographicRegion::UsEast,
                priority: 1,
                weight: 1.0,
                max_connections: 50,
            },
        ];

        let optimizer = GeographicRelayOptimizer::new(endpoints, GeographicRegion::UsEast).await.unwrap();
        assert!(optimizer.get_optimal_client().await.is_some());
    }

    #[tokio::test]
    async fn test_circuit_breaker() {
        let metrics = EndpointMetrics::new();
        
        for _ in 0..FAILURE_THRESHOLD {
            metrics.record_failure();
        }
        assert!(metrics.is_circuit_open());
        
        tokio::time::sleep(CIRCUIT_BREAKER_TIMEOUT + Duration::from_millis(100)).await;
        assert!(!metrics.is_circuit_open());
        
        metrics.record_success();
        let state = metrics.circuit_breaker_state.read().unwrap();
        assert!(matches!(*state, CircuitBreakerState::Closed));
    }

    #[tokio::test]
    async fn test_latency_tracking() {
        let metrics = EndpointMetrics::new();
        
        for i in 1..=10 {
            metrics.add_latency_sample(i * 100).await;
        }
        
        let avg = metrics.get_average_latency().await.unwrap();
        assert_eq!(avg, 550.0);
        
        let p99 = metrics.get_p99_latency().await.unwrap();
        assert_eq!(p99, 1000);
    }
}

