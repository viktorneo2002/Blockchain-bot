use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::{HashMap, VecDeque};
use tokio::sync::{RwLock, Mutex};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::clock::Slot;
use serde::{Deserialize, Serialize};
use tokio::time::{interval, timeout};
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use futures::future::join_all;
use rand::seq::SliceRandom;
use statrs::statistics::{Statistics, OrderStatistics};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeographicEndpoint {
    pub url: String,
    pub region: String,
    pub provider: String,
    pub weight: f64,
    pub priority: u8,
    pub max_requests_per_second: u64,
}

#[derive(Debug)]
struct EndpointMetrics {
    latency_samples: VecDeque<f64>,
    success_count: AtomicU64,
    failure_count: AtomicU64,
    last_success: Mutex<Option<Instant>>,
    last_failure: Mutex<Option<Instant>>,
    circuit_breaker_open: AtomicBool,
    current_requests: AtomicU64,
    slot_latencies: VecDeque<(Slot, Duration)>,
}

impl EndpointMetrics {
    fn new() -> Self {
        Self {
            latency_samples: VecDeque::with_capacity(1000),
            success_count: AtomicU64::new(0),
            failure_count: AtomicU64::new(0),
            last_success: Mutex::new(None),
            last_failure: Mutex::new(None),
            circuit_breaker_open: AtomicBool::new(false),
            current_requests: AtomicU64::new(0),
            slot_latencies: VecDeque::with_capacity(500),
        }
    }

    fn add_latency_sample(&mut self, latency: f64) {
        if self.latency_samples.len() >= 1000 {
            self.latency_samples.pop_front();
        }
        self.latency_samples.push_back(latency);
    }

    fn get_percentile(&self, percentile: f64) -> Option<f64> {
        if self.latency_samples.is_empty() {
            return None;
        }
        let mut sorted: Vec<f64> = self.latency_samples.iter().cloned().collect();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let index = ((percentile / 100.0) * (sorted.len() - 1) as f64) as usize;
        sorted.get(index).cloned()
    }

    fn get_success_rate(&self) -> f64 {
        let success = self.success_count.load(Ordering::Relaxed) as f64;
        let failure = self.failure_count.load(Ordering::Relaxed) as f64;
        let total = success + failure;
        if total == 0.0 {
            0.0
        } else {
            success / total
        }
    }
}

pub struct GeographicLatencyOptimizer {
    endpoints: Arc<RwLock<HashMap<String, GeographicEndpoint>>>,
    metrics: Arc<RwLock<HashMap<String, EndpointMetrics>>>,
    rpc_clients: Arc<RwLock<HashMap<String, Arc<RpcClient>>>>,
    primary_region: String,
    fallback_regions: Vec<String>,
    latency_threshold_ms: f64,
    circuit_breaker_threshold: f64,
    circuit_breaker_timeout: Duration,
    sample_window: Duration,
    health_check_interval: Duration,
}

impl GeographicLatencyOptimizer {
    pub async fn new(
        endpoints: Vec<GeographicEndpoint>,
        primary_region: String,
        fallback_regions: Vec<String>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut endpoint_map = HashMap::new();
        let mut metrics_map = HashMap::new();
        let mut rpc_clients_map = HashMap::new();

        for endpoint in endpoints {
            let client = Arc::new(RpcClient::new_with_commitment(
                endpoint.url.clone(),
                CommitmentConfig::confirmed(),
            ));
            
            endpoint_map.insert(endpoint.url.clone(), endpoint.clone());
            metrics_map.insert(endpoint.url.clone(), EndpointMetrics::new());
            rpc_clients_map.insert(endpoint.url.clone(), client);
        }

        let optimizer = Self {
            endpoints: Arc::new(RwLock::new(endpoint_map)),
            metrics: Arc::new(RwLock::new(metrics_map)),
            rpc_clients: Arc::new(RwLock::new(rpc_clients_map)),
            primary_region,
            fallback_regions,
            latency_threshold_ms: 50.0,
            circuit_breaker_threshold: 0.5,
            circuit_breaker_timeout: Duration::from_secs(30),
            sample_window: Duration::from_secs(300),
            health_check_interval: Duration::from_secs(10),
        };

        optimizer.start_health_monitoring();
        optimizer.start_latency_monitoring();
        
        Ok(optimizer)
    }

    pub async fn get_optimal_client(&self) -> Result<Arc<RpcClient>, Box<dyn std::error::Error>> {
        let endpoints = self.endpoints.read().await;
        let metrics = self.metrics.read().await;
        let clients = self.rpc_clients.read().await;

        let mut candidates: Vec<(&String, &GeographicEndpoint, f64)> = Vec::new();

        for (url, endpoint) in endpoints.iter() {
            if let Some(metric) = metrics.get(url) {
                if metric.circuit_breaker_open.load(Ordering::Relaxed) {
                    continue;
                }

                let success_rate = metric.get_success_rate();
                if success_rate < self.circuit_breaker_threshold {
                    continue;
                }

                let p50 = metric.get_percentile(50.0).unwrap_or(f64::MAX);
                let p99 = metric.get_percentile(99.0).unwrap_or(f64::MAX);
                let current_load = metric.current_requests.load(Ordering::Relaxed) as f64;
                let max_load = endpoint.max_requests_per_second as f64;
                let load_factor = 1.0 - (current_load / max_load).min(0.95);

                let score = calculate_endpoint_score(
                    p50,
                    p99,
                    success_rate,
                    load_factor,
                    endpoint.priority,
                    &endpoint.region,
                    &self.primary_region,
                );

                candidates.push((url, endpoint, score));
            }
        }

        candidates.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));

        if let Some((best_url, _, _)) = candidates.first() {
            if let Some(client) = clients.get(*best_url) {
                if let Some(metric) = metrics.get(*best_url) {
                    metric.current_requests.fetch_add(1, Ordering::Relaxed);
                }
                return Ok(Arc::clone(client));
            }
        }

        Err("No available endpoints".into())
    }

    pub async fn get_regional_clients(&self, region: &str) -> Vec<Arc<RpcClient>> {
        let endpoints = self.endpoints.read().await;
        let metrics = self.metrics.read().await;
        let clients = self.rpc_clients.read().await;

        let mut regional_clients = Vec::new();

        for (url, endpoint) in endpoints.iter() {
            if endpoint.region == region {
                if let Some(metric) = metrics.get(url) {
                    if !metric.circuit_breaker_open.load(Ordering::Relaxed) {
                        if let Some(client) = clients.get(url) {
                            regional_clients.push(Arc::clone(client));
                        }
                    }
                }
            }
        }

        regional_clients
    }

    pub async fn execute_with_optimal_latency<F, T>(
        &self,
        operation: F,
    ) -> Result<T, Box<dyn std::error::Error>>
    where
        F: Fn(Arc<RpcClient>) -> futures::future::BoxFuture<'static, Result<T, Box<dyn std::error::Error>>> + Clone,
        T: Send + 'static,
    {
        let start = Instant::now();
        let client = self.get_optimal_client().await?;
        let url = client.url();

        let result = timeout(Duration::from_millis(100), operation(client.clone())).await;

        let latency = start.elapsed().as_secs_f64() * 1000.0;
        self.record_request_outcome(&url, result.is_ok(), latency).await;

        match result {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(e)) => {
                self.handle_endpoint_failure(&url).await;
                Err(e)
            }
            Err(_) => {
                self.handle_endpoint_failure(&url).await;
                Err("Request timeout".into())
            }
        }
    }

    pub async fn parallel_query<F, T>(
        &self,
        operation: F,
        regions: Vec<String>,
    ) -> Result<T, Box<dyn std::error::Error>>
    where
        F: Fn(Arc<RpcClient>) -> futures::future::BoxFuture<'static, Result<T, Box<dyn std::error::Error>>> + Clone + Send + Sync,
        T: Send + 'static,
    {
        let mut tasks = Vec::new();

        for region in regions {
            let clients = self.get_regional_clients(&region).await;
            if let Some(client) = clients.first() {
                let op = operation.clone();
                let task = tokio::spawn(async move {
                    let op = op.clone();
                    timeout(Duration::from_millis(80), op(client.clone())).await
                });
                tasks.push(task);
            }
        }

        let results = join_all(tasks).await;

        for result in results {
            if let Ok(Ok(Ok(value))) = result {
                return Ok(value);
            }
        }

        Err("All parallel queries failed".into())
    }

    async fn record_request_outcome(&self, url: &str, success: bool, latency: f64) {
        let mut metrics = self.metrics.write().await;
        
        if let Some(metric) = metrics.get_mut(url) {
            metric.add_latency_sample(latency);
            
            if success {
                metric.success_count.fetch_add(1, Ordering::Relaxed);
                *metric.last_success.lock().await = Some(Instant::now());
            } else {
                metric.failure_count.fetch_add(1, Ordering::Relaxed);
                *metric.last_failure.lock().await = Some(Instant::now());
            }
            
            metric.current_requests.fetch_sub(1, Ordering::Relaxed);
        }
    }

    async fn handle_endpoint_failure(&self, url: &str) {
        let metrics = self.metrics.read().await;
        
        if let Some(metric) = metrics.get(url) {
            let failure_rate = 1.0 - metric.get_success_rate();
            
            if failure_rate > self.circuit_breaker_threshold {
                metric.circuit_breaker_open.store(true, Ordering::Relaxed);
                
                let metrics_clone = Arc::clone(&self.metrics);
                let url_clone = url.to_string();
                let timeout = self.circuit_breaker_timeout;
                
                tokio::spawn(async move {
                    tokio::time::sleep(timeout).await;
                    let metrics = metrics_clone.read().await;
                    if let Some(metric) = metrics.get(&url_clone) {
                        metric.circuit_breaker_open.store(false, Ordering::Relaxed);
                    }
                });
            }
        }
    }

    fn start_health_monitoring(&self) {
        let endpoints = Arc::clone(&self.endpoints);
        let metrics = Arc::clone(&self.metrics);
        let clients = Arc::clone(&self.rpc_clients);
        let interval_duration = self.health_check_interval;

        tokio::spawn(async move {
            let mut interval = interval(interval_duration);
            
            loop {
                interval.tick().await;
                
                let endpoints_read = endpoints.read().await;
                let clients_read = clients.read().await;
                
                for (url, _) in endpoints_read.iter() {
                    if let Some(client) = clients_read.get(url) {
                        let client_clone = Arc::clone(client);
                        let metrics_clone = Arc::clone(&metrics);
                        let url_clone = url.clone();
                        
                        tokio::spawn(async move {
                            let start = Instant::now();
                            let result = timeout(
                                Duration::from_millis(50),
                                client_clone.get_slot()
                            ).await;
                            
                            let latency = start.elapsed().as_secs_f64() * 1000.0;
                            let mut metrics_write = metrics_clone.write().await;
                            
                            if let Some(metric) = metrics_write.get_mut(&url_clone) {
                                if result.is_ok() {
                                    metric.add_latency_sample(latency);
                                    metric.success_count.fetch_add(1, Ordering::Relaxed);
                                } else {
                                    metric.failure_count.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                        });
                    }
                }
            }
        });
    }

    fn start_latency_monitoring(&self) {
        let metrics = Arc::clone(&self.metrics);
        let clients = Arc::clone(&self.rpc_clients);
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(5));
            
            loop {
                interval.tick().await;
                
                let clients_read = clients.read().await;
                let mut slot_latencies: HashMap<String, Vec<(Slot, Duration)>> = HashMap::new();
                
                for (url, client) in clients_read.iter() {
                    let client_clone = Arc::clone(client);
                    let url_clone = url.clone();
                    
                    let start = Instant::now();
                    match timeout(Duration::from_millis(100), client_clone.get_slot()).await {
                        Ok(Ok(slot)) => {
                            let latency = start.elapsed();
                            slot_latencies.entry(url_clone).or_insert_with(Vec::new).push((slot, latency));
                        }
                        _ => {}
                    }
                }
                
                let mut metrics_write = metrics.write().await;
                for (url, latencies) in slot_latencies {
                    if let Some(metric) = metrics_write.get_mut(&url) {
                        for (slot, latency) in latencies {
                            if metric.slot_latencies.len() >= 500 {
                                metric.slot_latencies.pop_front();
                            }
                            metric.slot_latencies.push_back((slot, latency));
                        }
                    }
                }
            }
        });
    }

    pub async fn get_best_endpoint_for_slot(&self, target_slot: Slot) -> Option<String> {
        let metrics = self.metrics.read().await;
        let endpoints = self.endpoints.read().await;
        
        let mut best_endpoint = None;
        let mut best_score = f64::MIN;
        
        for (url, metric) in metrics.iter() {
            if metric.circuit_breaker_open.load(Ordering::Relaxed) {
                continue;
            }
            
            let recent_slots: Vec<Slot> = metric.slot_latencies
                .iter()
                .map(|(slot, _)| *slot)
                .collect();
            
            if recent_slots.is_empty() {
                continue;
            }
            
            let max_slot = recent_slots.iter().max().copied().unwrap_or(0);
            let slot_freshness = if max_slot > target_slot {
                1.0
            } else {
                1.0 - ((target_slot - max_slot) as f64 / 100.0).min(0.9)
            };
            
            let avg_latency = metric.slot_latencies
                .iter()
                .map(|(_, duration)| duration.as_secs_f64() * 1000.0)
                .collect::<Vec<f64>>()
                .mean();
            
            if let Some(endpoint) = endpoints.get(url) {
                let latency_score = 1.0 / (1.0 + avg_latency / 10.0);
                let success_rate = metric.get_success_rate();
                let current_load = metric.current_requests.load(Ordering::Relaxed) as f64;
                let capacity_score = 1.0 - (current_load / endpoint.max_requests_per_second as f64).min(0.95);
                
                let score = slot_freshness * 0.4 
                    + latency_score * 0.3 
                    + success_rate * 0.2 
                    + capacity_score * 0.1;
                
                if score > best_score {
                    best_score = score;
                    best_endpoint = Some(url.clone());
                }
            }
        }
        
        best_endpoint
    }

    pub async fn get_latency_stats(&self, url: &str) -> Option<LatencyStats> {
        let metrics = self.metrics.read().await;
        
        if let Some(metric) = metrics.get(url) {
            let samples: Vec<f64> = metric.latency_samples.iter().cloned().collect();
            
            if samples.is_empty() {
                return None;
            }
            
            Some(LatencyStats {
                p50: metric.get_percentile(50.0).unwrap_or(0.0),
                p90: metric.get_percentile(90.0).unwrap_or(0.0),
                p95: metric.get_percentile(95.0).unwrap_or(0.0),
                p99: metric.get_percentile(99.0).unwrap_or(0.0),
                mean: samples.mean(),
                std_dev: samples.std_dev(),
                min: samples.min(),
                max: samples.max(),
                success_rate: metric.get_success_rate(),
                sample_count: samples.len(),
            })
        } else {
            None
        }
    }

    pub async fn adjust_weights_based_on_performance(&self) {
        let metrics = self.metrics.read().await;
        let mut endpoints = self.endpoints.write().await;
        
        for (url, endpoint) in endpoints.iter_mut() {
            if let Some(metric) = metrics.get(url) {
                let p50 = metric.get_percentile(50.0).unwrap_or(100.0);
                let success_rate = metric.get_success_rate();
                
                let performance_score = (1.0 / (1.0 + p50 / 50.0)) * success_rate;
                
                endpoint.weight = endpoint.weight * 0.9 + performance_score * 0.1;
                endpoint.weight = endpoint.weight.max(0.1).min(1.0);
            }
        }
    }

    pub async fn get_region_performance(&self) -> HashMap<String, RegionPerformance> {
        let endpoints = self.endpoints.read().await;
        let metrics = self.metrics.read().await;
        
        let mut region_stats: HashMap<String, Vec<f64>> = HashMap::new();
        let mut region_success: HashMap<String, (u64, u64)> = HashMap::new();
        
        for (url, endpoint) in endpoints.iter() {
            if let Some(metric) = metrics.get(url) {
                let samples: Vec<f64> = metric.latency_samples.iter().cloned().collect();
                region_stats.entry(endpoint.region.clone()).or_insert_with(Vec::new).extend(samples);
                
                let success = metric.success_count.load(Ordering::Relaxed);
                let failure = metric.failure_count.load(Ordering::Relaxed);
                let (s, f) = region_success.entry(endpoint.region.clone()).or_insert((0, 0));
                *s += success;
                *f += failure;
            }
        }
        
        let mut performance_map = HashMap::new();
        
        for (region, latencies) in region_stats {
            if let Some((success, failure)) = region_success.get(&region) {
                let success_rate = if success + failure > 0 {
                    *success as f64 / (*success + *failure) as f64
                } else {
                    0.0
                };
                
                performance_map.insert(region.clone(), RegionPerformance {
                    region,
                    avg_latency: latencies.mean(),
                    p99_latency: calculate_percentile(&latencies, 99.0),
                    success_rate,
                    sample_count: latencies.len(),
                });
            }
        }
        
        performance_map
    }

    pub async fn failover_to_best_region(&self) -> Result<String, Box<dyn std::error::Error>> {
        let region_perf = self.get_region_performance().await;
        
        let mut regions: Vec<(&String, &RegionPerformance)> = region_perf.iter().collect();
        regions.sort_by(|a, b| {
            let score_a = a.1.success_rate * (1.0 / (1.0 + a.1.avg_latency / 50.0));
            let score_b = b.1.success_rate * (1.0 / (1.0 + b.1.avg_latency / 50.0));
            score_b.partial_cmp(&score_a).unwrap_or(std::cmp::Ordering::Equal)
        });
        
        if let Some((best_region, _)) = regions.first() {
            Ok((*best_region).clone())
        } else {
            Err("No available regions".into())
        }
    }

    pub async fn optimize_for_transaction_landing(&self) -> Vec<Arc<RpcClient>> {
        let endpoints = self.endpoints.read().await;
        let metrics = self.metrics.read().await;
        let clients = self.rpc_clients.read().await;
        
        let mut scored_clients: Vec<(Arc<RpcClient>, f64)> = Vec::new();
        
        for (url, endpoint) in endpoints.iter() {
            if let Some(metric) = metrics.get(url) {
                if metric.circuit_breaker_open.load(Ordering::Relaxed) {
                    continue;
                }
                
                let slot_data: Vec<Slot> = metric.slot_latencies
                    .iter()
                    .map(|(slot, _)| *slot)
                    .collect();
                
                if slot_data.len() < 10 {
                    continue;
                }
                
                let slot_variance = calculate_slot_variance(&slot_data);
                let p50 = metric.get_percentile(50.0).unwrap_or(100.0);
                let success_rate = metric.get_success_rate();
                
                let consistency_score = 1.0 / (1.0 + slot_variance);
                let latency_score = 1.0 / (1.0 + p50 / 30.0);
                let reliability_score = success_rate.powf(2.0);
                
                let composite_score = consistency_score * 0.4 
                    + latency_score * 0.4 
                    + reliability_score * 0.2;
                
                if let Some(client) = clients.get(url) {
                    scored_clients.push((Arc::clone(client), composite_score));
                }
            }
        }
        
        scored_clients.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scored_clients.truncate(3);
        
        scored_clients.into_iter().map(|(client, _)| client).collect()
    }

    pub async fn reset_circuit_breaker(&self, url: &str) {
        let metrics = self.metrics.read().await;
        if let Some(metric) = metrics.get(url) {
            metric.circuit_breaker_open.store(false, Ordering::Relaxed);
            metric.success_count.store(0, Ordering::Relaxed);
            metric.failure_count.store(0, Ordering::Relaxed);
        }
    }

    pub async fn get_endpoint_status(&self) -> Vec<EndpointStatus> {
        let endpoints = self.endpoints.read().await;
        let metrics = self.metrics.read().await;
        let mut status_list = Vec::new();
        
        for (url, endpoint) in endpoints.iter() {
            if let Some(metric) = metrics.get(url) {
                let status = EndpointStatus {
                    url: url.clone(),
                    region: endpoint.region.clone(),
                    is_healthy: !metric.circuit_breaker_open.load(Ordering::Relaxed),
                    current_load: metric.current_requests.load(Ordering::Relaxed),
                    success_rate: metric.get_success_rate(),
                    avg_latency: metric.latency_samples.iter().cloned().collect::<Vec<f64>>().mean(),
                    p99_latency: metric.get_percentile(99.0).unwrap_or(0.0),
                };
                status_list.push(status);
            }
        }
        
        status_list
    }
}

#[derive(Debug, Clone)]
pub struct LatencyStats {
    pub p50: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
    pub mean: f64,
    pub std_dev: f64,
    pub min: f64,
    pub max: f64,
    pub success_rate: f64,
    pub sample_count: usize,
}

#[derive(Debug, Clone)]
pub struct RegionPerformance {
    pub region: String,
    pub avg_latency: f64,
    pub p99_latency: f64,
    pub success_rate: f64,
    pub sample_count: usize,
}

#[derive(Debug, Clone)]
pub struct EndpointStatus {
    pub url: String,
    pub region: String,
    pub is_healthy: bool,
    pub current_load: u64,
    pub success_rate: f64,
    pub avg_latency: f64,
    pub p99_latency: f64,
}

fn calculate_endpoint_score(
    p50: f64,
    p99: f64,
    success_rate: f64,
    load_factor: f64,
    priority: u8,
    endpoint_region: &str,
    primary_region: &str,
) -> f64 {
    let latency_score = 1.0 / (1.0 + (p50 / 20.0) + (p99 / 100.0));
    let reliability_score = success_rate.powf(3.0);
    let capacity_score = load_factor.powf(0.5);
    let priority_boost = 1.0 + (priority as f64 / 10.0);
    let region_boost = if endpoint_region == primary_region { 1.5 } else { 1.0 };
    
    (latency_score * 0.35 + reliability_score * 0.35 + capacity_score * 0.3) 
        * priority_boost 
        * region_boost
}

fn calculate_percentile(samples: &[f64], percentile: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    
    let mut sorted = samples.to_vec();
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    
    let index = ((percentile / 100.0) * (sorted.len() - 1) as f64) as usize;
    sorted.get(index).copied().unwrap_or(0.0)
}

fn calculate_slot_variance(slots: &[Slot]) -> f64 {
    if slots.len() < 2 {
        return 0.0;
    }
    
    let diffs: Vec<f64> = slots.windows(2)
        .map(|w| (w[1] as i64 - w[0] as i64).abs() as f64)
        .collect();
    
    if diffs.is_empty() {
        return 0.0;
    }
    
    let mean_diff = diffs.iter().sum::<f64>() / diffs.len() as f64;
    let variance = diffs.iter()
        .map(|d| (d - mean_diff).powi(2))
        .sum::<f64>() / diffs.len() as f64;
    
    variance.sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_geographic_optimizer_initialization() {
        let endpoints = vec![
            GeographicEndpoint {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                region: "us-west".to_string(),
                provider: "solana".to_string(),
                weight: 1.0,
                priority: 10,
                max_requests_per_second: 100,
            },
            GeographicEndpoint {
                url: "https://solana-api.projectserum.com".to_string(),
                region: "us-east".to_string(),
                provider: "serum".to_string(),
                weight: 0.9,
                priority: 8,
                max_requests_per_second: 80,
            },
        ];

        let optimizer = GeographicLatencyOptimizer::new(
            endpoints,
            "us-west".to_string(),
            vec!["us-east".to_string()],
        ).await;

        assert!(optimizer.is_ok());
    }

    #[tokio::test]
    async fn test_endpoint_selection() {
        let endpoints = vec![
            GeographicEndpoint {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                region: "us-west".to_string(),
                provider: "solana".to_string(),
                weight: 1.0,
                priority: 10,
                max_requests_per_second: 100,
            },
        ];

        let optimizer = GeographicLatencyOptimizer::new(
            endpoints,
            "us-west".to_string(),
            vec![],
        ).await.unwrap();

        let client = optimizer.get_optimal_client().await;
        assert!(client.is_ok());
    }
}
