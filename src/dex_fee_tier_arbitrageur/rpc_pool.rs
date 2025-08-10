use anyhow::{anyhow, Result};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::Signature,
    transaction::VersionedTransaction,
};
use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};
use std::time::Instant;
use tokio::{
    sync::Semaphore,
    time::{sleep, timeout, Duration},
};
use tracing::{debug, info, warn};

/// RPC endpoint metrics for health scoring
#[derive(Debug)]
pub struct RpcEndpointMetrics {
    latency_ema: AtomicU64,
    success_count: AtomicU64,
    error_count: AtomicU64,
    consecutive_errors: AtomicUsize,
}

impl RpcEndpointMetrics {
    pub fn new() -> Self {
        Self {
            latency_ema: AtomicU64::new(100),
            success_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            consecutive_errors: AtomicUsize::new(0),
        }
    }

    pub fn health_score(&self) -> f64 {
        let success = self.success_count.load(Ordering::Relaxed) as f64;
        let errors = self.error_count.load(Ordering::Relaxed) as f64;
        let latency = self.latency_ema.load(Ordering::Relaxed) as f64;
        let consecutive = self.consecutive_errors.load(Ordering::Relaxed) as f64;
        
        let success_rate = success / (success + errors + 1.0);
        let latency_factor = 100.0 / (latency + 1.0);
        let penalty = 1.0 / (1.0 + consecutive * 0.5);
        
        success_rate * latency_factor * penalty
    }

    pub fn record_success(&self, latency_ms: u64) {
        self.success_count.fetch_add(1, Ordering::Relaxed);
        self.consecutive_errors.store(0, Ordering::Relaxed);
        
        // EMA update: new = 0.7 * old + 0.3 * current
        let old = self.latency_ema.load(Ordering::Relaxed);
        let new = (old * 7 + latency_ms * 3) / 10;
        self.latency_ema.store(new, Ordering::Relaxed);
    }

    pub fn record_error(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
        self.consecutive_errors.fetch_add(1, Ordering::Relaxed);
    }
}

/// Hedged RPC pool for low-latency, fault-tolerant operations
pub struct RpcPool {
    clients: Vec<Arc<RpcClient>>,
    metrics: Vec<Arc<RpcEndpointMetrics>>,
    semaphore: Arc<Semaphore>,
}

impl RpcPool {
    pub fn new(endpoints: Vec<String>) -> Self {
        let clients: Vec<Arc<RpcClient>> = endpoints
            .into_iter()
            .map(|url| {
                Arc::new(RpcClient::new_with_commitment(
                    url,
                    CommitmentConfig::confirmed(),
                ))
            })
            .collect();
        
        let metrics = (0..clients.len())
            .map(|_| Arc::new(RpcEndpointMetrics::new()))
            .collect();
        
        Self {
            clients,
            metrics,
            semaphore: Arc::new(Semaphore::new(10)), // Max concurrent requests
        }
    }

    /// Hedged get_multiple_accounts - races multiple endpoints
    pub async fn hedged_get_multiple_accounts(
        &self,
        keys: &[Pubkey],
        timeout_ms: u64,
    ) -> Result<Vec<Option<Account>>> {
        let best = self.select_best_endpoints(2);
        if best.is_empty() {
            return Err(anyhow!("No healthy endpoints available"));
        }
        
        let mut futures = vec![];
        
        for idx in best {
            let client = self.clients[idx].clone();
            let keys = keys.to_vec();
            let metrics = self.metrics[idx].clone();
            let _permit = self.semaphore.clone().acquire_owned().await?;
            
            futures.push(tokio::spawn(async move {
                let start = Instant::now();
                let result = timeout(
                    Duration::from_millis(timeout_ms),
                    client.get_multiple_accounts(&keys)
                ).await;
                
                match result {
                    Ok(Ok(accounts)) => {
                        metrics.record_success(start.elapsed().as_millis() as u64);
                        Ok(accounts)
                    }
                    Ok(Err(e)) => {
                        metrics.record_error();
                        Err(anyhow!("RPC error: {}", e))
                    }
                    Err(_) => {
                        metrics.record_error();
                        Err(anyhow!("Request timeout"))
                    }
                }
            }));
        }
        
        // Return first successful result
        let (result, _, _) = futures::future::select_all(futures).await;
        result?
    }

    /// Get single account with hedging
    pub async fn get_account(&self, key: &Pubkey) -> Result<Account> {
        let accounts = self.hedged_get_multiple_accounts(&[*key], 500).await?;
        accounts
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| anyhow!("Account not found"))
    }

    /// Simulate transaction before sending
    pub async fn simulate_transaction(
        &self,
        tx: &VersionedTransaction,
    ) -> Result<bool> {
        let best = self.select_best_endpoints(1);
        if best.is_empty() {
            return Err(anyhow!("No healthy endpoints"));
        }
        
        let client = &self.clients[best[0]];
        let metrics = &self.metrics[best[0]];
        
        let start = Instant::now();
        match client.simulate_transaction(tx).await {
            Ok(result) => {
                metrics.record_success(start.elapsed().as_millis() as u64);
                Ok(result.value.err.is_none())
            }
            Err(e) => {
                metrics.record_error();
                Err(anyhow!("Simulation failed: {}", e))
            }
        }
    }

    /// Send transaction with retries and hedging
    pub async fn send_transaction_with_retries(
        &self,
        tx: &VersionedTransaction,
        max_retries: u8,
    ) -> Result<Signature> {
        let best = self.select_best_endpoints(2);
        if best.is_empty() {
            return Err(anyhow!("No healthy endpoints"));
        }
        
        for attempt in 0..max_retries {
            for &idx in &best {
                let client = &self.clients[idx];
                let metrics = &self.metrics[idx];
                
                match client.send_transaction(tx).await {
                    Ok(sig) => {
                        info!("Transaction sent: {}", sig);
                        metrics.record_success(100);
                        return Ok(sig);
                    }
                    Err(e) => {
                        warn!("Send attempt {} failed: {}", attempt, e);
                        metrics.record_error();
                    }
                }
            }
            
            if attempt < max_retries - 1 {
                sleep(Duration::from_millis(100 * (1 << attempt))).await;
            }
        }
        
        Err(anyhow!("Failed to send transaction after {} retries", max_retries))
    }

    /// Get latest slot
    pub async fn get_slot(&self) -> Result<u64> {
        let best = self.select_best_endpoints(1);
        if best.is_empty() {
            return Err(anyhow!("No healthy endpoints"));
        }
        
        let client = &self.clients[best[0]];
        Ok(client.get_slot().await?)
    }

    /// Get recent blockhash
    pub async fn get_latest_blockhash(&self) -> Result<solana_sdk::hash::Hash> {
        let best = self.select_best_endpoints(1);
        if best.is_empty() {
            return Err(anyhow!("No healthy endpoints"));
        }
        
        let client = &self.clients[best[0]];
        Ok(client.get_latest_blockhash().await?)
    }

    /// Select best N endpoints by health score
    fn select_best_endpoints(&self, count: usize) -> Vec<usize> {
        let mut scores: Vec<(usize, f64)> = self.metrics
            .iter()
            .enumerate()
            .map(|(i, m)| (i, m.health_score()))
            .filter(|(_, score)| *score > 0.1) // Filter out very unhealthy endpoints
            .collect();
        
        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores.into_iter()
            .take(count)
            .map(|(i, _)| i)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_score() {
        let metrics = RpcEndpointMetrics::new();
        
        // Initial score
        assert!(metrics.health_score() > 0.0);
        
        // Record successes
        for _ in 0..10 {
            metrics.record_success(50);
        }
        let good_score = metrics.health_score();
        
        // Record errors
        for _ in 0..5 {
            metrics.record_error();
        }
        let bad_score = metrics.health_score();
        
        assert!(good_score > bad_score);
    }
}
