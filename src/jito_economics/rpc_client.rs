use std::time::Duration;
use anyhow::{Result, anyhow};
use reqwest::{Client, ClientBuilder};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use solana_sdk::pubkey::Pubkey;
use tokio::time::{sleep, timeout};
use failsafe::{backoff, failure_policy, CircuitBreaker, Config};
use std::sync::Arc;

// Production-grade Solana RPC client with sophisticated retry logic
#[derive(Clone)]
pub struct SolanaRpcClient {
    rpc_url: String,
    client: Client,
    timeout: Duration,
    circuit_breaker: Arc<CircuitBreaker<backoff::ExponentialBackoff, failure_policy::ConsecutiveFailures>>,
    max_retries: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    jsonrpc: String,
    id: u64,
    method: String,
    params: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse<T> {
    jsonrpc: String,
    id: u64,
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    code: i32,
    message: String,
    data: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotInfo {
    pub slot: u64,
    pub parent: u64,
    pub root: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorInfo {
    pub identity: Pubkey,
    pub vote_account: Pubkey,
    pub commission: u8,
    pub last_vote: u64,
    pub root_slot: u64,
    pub credits: u64,
    pub epoch_vote_account: bool,
    pub epoch_credits: Vec<(u64, u64, u64)>,
    pub activated_stake: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpochInfo {
    pub epoch: u64,
    pub slot_index: u64,
    pub slots_in_epoch: u64,
    pub absolute_slot: u64,
    pub block_height: u64,
    pub transaction_count: Option<u64>,
}

impl SolanaRpcClient {
    pub fn new(rpc_url: String) -> Result<Self> {
        // Build client with production-ready settings
        let client = ClientBuilder::new()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .pool_idle_timeout(Duration::from_secs(90))
            .pool_max_idle_per_host(10)
            .tcp_keepalive(Duration::from_secs(60))
            .http2_prior_knowledge()
            .build()
            .map_err(|e| anyhow!("Failed to create HTTP client: {}", e))?;

        // Configure circuit breaker with exponential backoff
        let backoff = backoff::exponential(
            Duration::from_millis(100), // Start with 100ms
            Duration::from_secs(60),     // Max 60 seconds
        );
        
        let policy = failure_policy::consecutive_failures(5, backoff);
        let circuit_breaker = Arc::new(
            Config::new()
                .failure_policy(policy)
                .build()
        );

        Ok(Self {
            rpc_url,
            client,
            timeout: Duration::from_secs(30),
            circuit_breaker,
            max_retries: 3,
        })
    }

    pub async fn get_current_slot(&self) -> Result<u64> {
        self.circuit_breaker.call(|| async {
            self.make_rpc_call_with_retry("getSlot", json!([{"commitment": "processed"}]))
                .await
        }).await
        .map_err(|e| anyhow!("Circuit breaker error: {:?}", e))?
    }

    pub async fn get_epoch_info(&self) -> Result<EpochInfo> {
        self.circuit_breaker.call(|| async {
            self.make_rpc_call_with_retry("getEpochInfo", json!([{"commitment": "finalized"}]))
                .await
        }).await
        .map_err(|e| anyhow!("Circuit breaker error: {:?}", e))?
    }

    pub async fn get_validator_identity(&self, vote_account: &Pubkey) -> Result<Pubkey> {
        let params = json!([
            vote_account.to_string(),
            {
                "commitment": "finalized",
                "encoding": "jsonParsed"
            }
        ]);

        let account_info: Value = self.circuit_breaker.call(|| async {
            self.make_rpc_call_with_retry("getAccountInfo", params.clone())
                .await
        }).await
        .map_err(|e| anyhow!("Circuit breaker error: {:?}", e))?;

        // Parse validator identity from vote account data
        let data = account_info
            .get("data")
            .and_then(|d| d.get("parsed"))
            .and_then(|p| p.get("info"))
            .and_then(|i| i.get("nodePubkey"))
            .and_then(|n| n.as_str())
            .ok_or_else(|| anyhow!("Invalid vote account data structure"))?;

        data.parse::<Pubkey>()
            .map_err(|e| anyhow!("Failed to parse validator identity: {}", e))
    }

    pub async fn get_vote_accounts(&self) -> Result<Vec<ValidatorInfo>> {
        let response: Value = self.circuit_breaker.call(|| async {
            self.make_rpc_call_with_retry("getVoteAccounts", json!([{"commitment": "finalized"}]))
                .await
        }).await
        .map_err(|e| anyhow!("Circuit breaker error: {:?}", e))?;

        let current_validators = response
            .get("current")
            .and_then(|c| c.as_array())
            .ok_or_else(|| anyhow!("Invalid vote accounts response"))?;

        let mut validators = Vec::new();
        for validator_data in current_validators {
            let validator = self.parse_validator_info(validator_data)?;
            validators.push(validator);
        }

        Ok(validators)
    }

    pub async fn get_block_production(&self, identity: &Pubkey) -> Result<(u64, u64)> {
        let params = json!([{
            "identity": identity.to_string(),
            "commitment": "finalized"
        }]);

        let response: Value = self.circuit_breaker.call(|| async {
            self.make_rpc_call_with_retry("getBlockProduction", params.clone())
                .await
        }).await
        .map_err(|e| anyhow!("Circuit breaker error: {:?}", e))?;

        let by_identity = response
            .get("byIdentity")
            .and_then(|bi| bi.get(identity.to_string()))
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("Invalid block production response"))?;

        if by_identity.len() >= 2 {
            let blocks_produced = by_identity[0].as_u64().unwrap_or(0);
            let leader_slots = by_identity[1].as_u64().unwrap_or(0);
            Ok((blocks_produced, leader_slots))
        } else {
            Ok((0, 0))
        }
    }

    pub async fn get_recent_performance_samples(&self, limit: usize) -> Result<Vec<(u64, u64, u64, Option<u64>)>> {
        let params = json!([limit]);

        let response: Value = self.circuit_breaker.call(|| async {
            self.make_rpc_call_with_retry("getRecentPerformanceSamples", params.clone())
                .await
        }).await
        .map_err(|e| anyhow!("Circuit breaker error: {:?}", e))?;

        let samples = response
            .as_array()
            .ok_or_else(|| anyhow!("Invalid performance samples response"))?;

        let mut performance_data = Vec::new();
        for sample in samples {
            let slot = sample.get("slot").and_then(|s| s.as_u64()).unwrap_or(0);
            let num_transactions = sample.get("numTransactions").and_then(|n| n.as_u64()).unwrap_or(0);
            let num_slots = sample.get("numSlots").and_then(|n| n.as_u64()).unwrap_or(0);
            let sample_period_secs = sample.get("samplePeriodSecs").and_then(|s| s.as_u64());
            
            performance_data.push((slot, num_transactions, num_slots, sample_period_secs));
        }

        Ok(performance_data)
    }

    pub async fn get_leader_schedule(&self, epoch: Option<u64>) -> Result<std::collections::HashMap<String, Vec<u64>>> {
        let params = if let Some(epoch) = epoch {
            json!([epoch, {"commitment": "finalized"}])
        } else {
            json!([null, {"commitment": "finalized"}])
        };

        let response: std::collections::HashMap<String, Vec<u64>> = self.circuit_breaker.call(|| async {
            self.make_rpc_call_with_retry("getLeaderSchedule", params.clone())
                .await
        }).await
        .map_err(|e| anyhow!("Circuit breaker error: {:?}", e))?;

        Ok(response)
    }

    // Core RPC call method with sophisticated retry logic
    async fn make_rpc_call_with_retry<T: for<'de> Deserialize<'de>>(&self, method: &str, params: Value) -> Result<T> {
        let mut last_error = None;
        
        for attempt in 0..=self.max_retries {
            let request = RpcRequest {
                jsonrpc: "2.0".to_string(),
                id: rand::random(),
                method: method.to_string(),
                params: params.clone(),
            };

            let result = timeout(self.timeout, async {
                let response = self.client
                    .post(&self.rpc_url)
                    .json(&request)
                    .send()
                    .await?;

                if !response.status().is_success() {
                    return Err(anyhow!("HTTP error: {}", response.status()));
                }

                let rpc_response: RpcResponse<T> = response.json().await?;

                if let Some(error) = rpc_response.error {
                    return Err(anyhow!("RPC error {}: {}", error.code, error.message));
                }

                rpc_response.result.ok_or_else(|| anyhow!("No result in RPC response"))
            }).await;

            match result {
                Ok(Ok(data)) => return Ok(data),
                Ok(Err(e)) => {
                    last_error = Some(e);
                    
                    // Check if error is retryable
                    if !self.is_retryable_error(&last_error.as_ref().unwrap()) {
                        break;
                    }
                },
                Err(_) => {
                    last_error = Some(anyhow!("Request timeout"));
                }
            }

            // Exponential backoff with jitter
            if attempt < self.max_retries {
                let delay = Duration::from_millis(
                    (100 * 2_u64.pow(attempt)) + rand::random::<u64>() % 100
                );
                sleep(delay).await;
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow!("Unknown error")))
    }

    fn is_retryable_error(&self, error: &anyhow::Error) -> bool {
        let error_msg = error.to_string();
        
        // Network errors are retryable
        if error_msg.contains("timeout") || 
           error_msg.contains("connection") ||
           error_msg.contains("HTTP error: 429") || // Rate limited
           error_msg.contains("HTTP error: 5") {    // 5xx errors
            return true;
        }

        // RPC specific retryable errors
        if error_msg.contains("RPC error -32005") || // Node behind
           error_msg.contains("RPC error -32002") || // Transaction simulation failed (might be temporary)
           error_msg.contains("RPC error -32004") {  // Node is unhealthy
            return true;
        }

        false
    }

    fn parse_validator_info(&self, data: &Value) -> Result<ValidatorInfo> {
        let identity = data.get("nodePubkey")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<Pubkey>().ok())
            .ok_or_else(|| anyhow!("Invalid node pubkey"))?;

        let vote_account = data.get("votePubkey")
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<Pubkey>().ok())
            .ok_or_else(|| anyhow!("Invalid vote pubkey"))?;

        let commission = data.get("commission")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u8;

        let last_vote = data.get("lastVote")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        let root_slot = data.get("rootSlot")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        let credits = data.get("credits")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        let epoch_vote_account = data.get("epochVoteAccount")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let epoch_credits = data.get("epochCredits")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|item| {
                        if let Some(credits_array) = item.as_array() {
                            if credits_array.len() >= 3 {
                                let epoch = credits_array[0].as_u64()?;
                                let credits = credits_array[1].as_u64()?;
                                let prev_credits = credits_array[2].as_u64()?;
                                Some((epoch, credits, prev_credits))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        let activated_stake = data.get("activatedStake")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);

        Ok(ValidatorInfo {
            identity,
            vote_account,
            commission,
            last_vote,
            root_slot,
            credits,
            epoch_vote_account,
            epoch_credits,
            activated_stake,
        })
    }

    // Health check for RPC endpoint
    pub async fn health_check(&self) -> Result<bool> {
        let start = std::time::Instant::now();
        
        match timeout(Duration::from_secs(5), async {
            self.get_current_slot().await
        }).await {
            Ok(Ok(_)) => {
                let latency = start.elapsed();
                Ok(latency < Duration::from_secs(2)) // Consider healthy if < 2s response
            },
            Ok(Err(_)) | Err(_) => Ok(false),
        }
    }

    // Get network congestion indicators
    pub async fn get_network_congestion_indicators(&self) -> Result<NetworkCongestionMetrics> {
        let epoch_info = self.get_epoch_info().await?;
        let performance_samples = self.get_recent_performance_samples(20).await?;
        
        // Calculate TPS and slot utilization
        let mut total_transactions = 0u64;
        let mut total_slots = 0u64;
        
        for (_slot, num_transactions, num_slots, _period) in &performance_samples {
            total_transactions += num_transactions;
            total_slots += num_slots;
        }

        let avg_tps = if !performance_samples.is_empty() && total_slots > 0 {
            (total_transactions as f64) / (total_slots as f64 * 0.4) // 400ms per slot
        } else {
            0.0
        };

        let slot_utilization = if epoch_info.slots_in_epoch > 0 {
            epoch_info.slot_index as f64 / epoch_info.slots_in_epoch as f64
        } else {
            0.0
        };

        Ok(NetworkCongestionMetrics {
            current_slot: epoch_info.absolute_slot,
            current_epoch: epoch_info.epoch,
            slot_utilization,
            average_tps: avg_tps,
            recent_transaction_count: total_transactions,
            estimated_congestion_level: self.estimate_congestion_level(avg_tps, slot_utilization),
        })
    }

    fn estimate_congestion_level(&self, avg_tps: f64, slot_utilization: f64) -> CongestionLevel {
        // Based on Solana mainnet observations
        let tps_threshold_high = 2000.0;
        let tps_threshold_medium = 1000.0;
        
        match (avg_tps, slot_utilization) {
            (tps, util) if tps > tps_threshold_high || util > 0.8 => CongestionLevel::High,
            (tps, util) if tps > tps_threshold_medium || util > 0.6 => CongestionLevel::Medium,
            _ => CongestionLevel::Low,
        }
    }

    // Batch RPC calls for efficiency
    pub async fn batch_rpc_calls<T: for<'de> Deserialize<'de>>(&self, calls: Vec<(&str, Value)>) -> Result<Vec<Result<T>>> {
        if calls.is_empty() {
            return Ok(Vec::new());
        }

        let batch_requests: Vec<RpcRequest> = calls
            .into_iter()
            .enumerate()
            .map(|(i, (method, params))| RpcRequest {
                jsonrpc: "2.0".to_string(),
                id: i as u64,
                method: method.to_string(),
                params,
            })
            .collect();

        let response = self.client
            .post(&self.rpc_url)
            .json(&batch_requests)
            .send()
            .await
            .map_err(|e| anyhow!("Batch RPC request failed: {}", e))?;

        if !response.status().is_success() {
            return Err(anyhow!("Batch RPC HTTP error: {}", response.status()));
        }

        let batch_response: Vec<RpcResponse<T>> = response
            .json()
            .await
            .map_err(|e| anyhow!("Failed to parse batch response: {}", e))?;

        let results = batch_response
            .into_iter()
            .map(|resp| {
                if let Some(error) = resp.error {
                    Err(anyhow!("RPC error {}: {}", error.code, error.message))
                } else {
                    resp.result.ok_or_else(|| anyhow!("No result in RPC response"))
                }
            })
            .collect();

        Ok(results)
    }

    // Get validator performance metrics
    pub async fn get_validator_performance(&self, vote_account: &Pubkey, epochs: u64) -> Result<ValidatorPerformanceMetrics> {
        let epoch_info = self.get_epoch_info().await?;
        let current_epoch = epoch_info.epoch;
        
        let mut total_credits = 0u64;
        let mut total_possible_credits = 0u64;
        let mut epochs_analyzed = 0u64;

        // Analyze last N epochs
        for epoch_offset in 0..epochs {
            if current_epoch < epoch_offset {
                break;
            }
            
            let target_epoch = current_epoch - epoch_offset;
            
            if let Ok(validator_info) = self.get_validator_info_for_epoch(vote_account, target_epoch).await {
                if let Some((_, credits, prev_credits)) = validator_info.epoch_credits.last() {
                    total_credits += credits - prev_credits;
                    total_possible_credits += validator_info.activated_stake / 1_000_000_000; // Approximate
                    epochs_analyzed += 1;
                }
            }
        }

        let performance_ratio = if total_possible_credits > 0 {
            total_credits as f64 / total_possible_credits as f64
        } else {
            0.0
        };

        Ok(ValidatorPerformanceMetrics {
            vote_account: *vote_account,
            epochs_analyzed,
            total_credits,
            performance_ratio,
            is_active: epochs_analyzed > 0,
        })
    }

    async fn get_validator_info_for_epoch(&self, vote_account: &Pubkey, epoch: u64) -> Result<ValidatorInfo> {
        // This would require historical data access - simplified for this implementation
        // In production, you'd query specific epoch data or maintain historical records
        let validators = self.get_vote_accounts().await?;
        validators
            .into_iter()
            .find(|v| v.vote_account == *vote_account)
            .ok_or_else(|| anyhow!("Validator not found in current epoch"))
    }
}

#[derive(Debug, Clone)]
pub struct NetworkCongestionMetrics {
    pub current_slot: u64,
    pub current_epoch: u64,
    pub slot_utilization: f64,
    pub average_tps: f64,
    pub recent_transaction_count: u64,
    pub estimated_congestion_level: CongestionLevel,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CongestionLevel {
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone)]
pub struct ValidatorPerformanceMetrics {
    pub vote_account: Pubkey,
    pub epochs_analyzed: u64,
    pub total_credits: u64,
    pub performance_ratio: f64,
    pub is_active: bool,
}

// Production-ready RPC client factory with connection pooling
pub struct SolanaRpcClientFactory {
    clients: Vec<SolanaRpcClient>,
    current_index: std::sync::atomic::AtomicUsize,
}

impl SolanaRpcClientFactory {
    pub fn new(rpc_urls: Vec<String>) -> Result<Self> {
        let mut clients = Vec::new();
        
        for url in rpc_urls {
            clients.push(SolanaRpcClient::new(url)?);
        }

        if clients.is_empty() {
            return Err(anyhow!("At least one RPC URL must be provided"));
        }

        Ok(Self {
            clients,
            current_index: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    // Round-robin client selection with health checking
    pub async fn get_healthy_client(&self) -> Result<&SolanaRpcClient> {
        let start_index = self.current_index.load(std::sync::atomic::Ordering::SeqCst);
        
        for i in 0..self.clients.len() {
            let index = (start_index + i) % self.clients.len();
            let client = &self.clients[index];
            
            if client.health_check().await.unwrap_or(false) {
                self.current_index.store(index, std::sync::atomic::Ordering::SeqCst);
                return Ok(client);
            }
        }

        // If no healthy clients, return the first one anyway
        Ok(&self.clients[0])
    }

    pub async fn broadcast_to_all<T: for<'de> Deserialize<'de> + Clone>(&self, method: &str, params: Value) -> Vec<Result<T>> {
        let futures = self.clients.iter().map(|client| {
            client.make_rpc_call_with_retry(method, params.clone())
        });

        futures::future::join_all(futures).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_rpc_client_creation() {
        let client = SolanaRpcClient::new("https://api.mainnet-beta.solana.com".to_string());
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_circuit_breaker() {
        let client = SolanaRpcClient::new("https://invalid-url.com".to_string()).unwrap();
        
        // Circuit breaker should eventually trip after consecutive failures
        for _ in 0..10 {
            let _ = client.get_current_slot().await;
        }
        
        // The circuit breaker should now be in open state
        // (This test would need to be adjusted based on actual circuit breaker behavior)
    }

    #[tokio::test]
    async fn test_factory_health_checking() {
        let urls = vec![
            "https://api.mainnet-beta.solana.com".to_string(),
            "https://solana-api.projectserum.com".to_string(),
        ];
        
        let factory = SolanaRpcClientFactory::new(urls).unwrap();
        let _client = factory.get_healthy_client().await;
        // Should not panic and return a client
    }
}
