use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::collections::{HashMap, VecDeque};
use tokio::sync::{RwLock, Mutex, Semaphore};
use tokio::time::{sleep, timeout};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig};
use solana_client::rpc_request::RpcRequest;
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    signature::Signature,
    transaction::Transaction,
    pubkey::Pubkey,
    clock::Slot,
};
use solana_transaction_status::{TransactionStatus, UiTransactionEncoding};
use serde_json::Value;
use reqwest::{Client, Response};
use url::Url;
use futures::future::join_all;
use rand::seq::SliceRandom;
use thiserror::Error;
use anyhow::{Result, anyhow};
use solana_client::{
    client_error::{ClientError, ClientErrorKind},
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{RpcFilterType, Memcmp},
    rpc_response::{Response as RpcResponse, RpcKeyedAccount},
};
use tracing::{debug, error, warn, info};

#[derive(Debug, Clone)]
pub struct EndpointHealthStatus {
    pub url: String,
    pub is_healthy: bool,
    pub current_latency_ms: u64,
    pub average_latency_ms: u64,
    pub success_rate: f64,
    pub total_requests: u64,
    pub consecutive_failures: usize,
    pub last_failure_time: Option<Instant>,
    pub priority: u8,
    pub weight: u32,
    pub circuit_breaker_open: bool,
    pub score: f64,
}

#[derive(Debug, Clone)]
pub struct EndpointPerformanceMetrics {
    pub url: String,
    pub total_requests: u64,
    pub successful_requests: u64,
    pub failed_requests: u64,
    pub average_latency_ms: u64,
    pub uptime_percentage: f64,
    pub consecutive_failures: usize,
    pub is_currently_healthy: bool,
    pub performance_score: f64,
}

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("All endpoints failed")]
    AllEndpointsFailed,
    #[error("Request timeout")]
    RequestTimeout,
    #[error("Rate limit exceeded")]
    RateLimitExceeded,
    #[error("Invalid response")]
    InvalidResponse,
    #[error("Network error: {0}")]
    NetworkError(String),
}

#[derive(Clone, Debug)]
pub struct EndpointConfig {
    pub url: String,
    pub ws_url: String,
    pub weight: u32,
    pub max_requests_per_second: u32,
    pub timeout_ms: u64,
    pub priority: u8,
}

#[derive(Debug, Clone)]
struct EndpointStats {
    latency_ms: Arc<AtomicU64>,
    success_count: Arc<AtomicU64>,
    failure_count: Arc<AtomicU64>,
    last_failure: Arc<RwLock<Option<Instant>>>,
    consecutive_failures: Arc<AtomicUsize>,
}

struct RateLimiter {
    requests: Arc<Mutex<VecDeque<Instant>>>,
    max_requests: u32,
    window: Duration,
}

impl RateLimiter {
    fn new(max_requests: u32) -> Self {
        Self {
            requests: Arc::new(Mutex::new(VecDeque::new())),
            max_requests,
            window: Duration::from_secs(1),
        }
    }

    async fn acquire(&self) -> Result<(), RpcError> {
        let mut requests = self.requests.lock().await;
        let now = Instant::now();
        
        while let Some(&front) = requests.front() {
            if now.duration_since(front) > self.window {
                requests.pop_front();
            } else {
                break;
            }
        }

        if requests.len() >= self.max_requests as usize {
            return Err(RpcError::RateLimitExceeded);
        }

        requests.push_back(now);
        Ok(())
    }
}

pub struct Endpoint {
    config: EndpointConfig,
    client: Arc<RpcClient>,
    http_client: Arc<Client>,
    stats: EndpointStats,
    rate_limiter: RateLimiter,
    health_check_interval: Duration,
    is_healthy: Arc<RwLock<bool>>,
}

impl Endpoint {
    fn new(config: EndpointConfig) -> Self {
        let client = Arc::new(RpcClient::new_with_timeout(
            config.url.clone(),
            Duration::from_millis(config.timeout_ms),
        ));

        let http_client = Arc::new(
            Client::builder()
                .timeout(Duration::from_millis(config.timeout_ms))
                .pool_max_idle_per_host(10)
                .pool_idle_timeout(Duration::from_secs(30))
                .build()
                .unwrap()
        );

        Self {
            config: config.clone(),
            client,
            http_client,
            stats: EndpointStats {
                latency_ms: Arc::new(AtomicU64::new(0)),
                success_count: Arc::new(AtomicU64::new(0)),
                failure_count: Arc::new(AtomicU64::new(0)),
                last_failure: Arc::new(RwLock::new(None)),
                consecutive_failures: Arc::new(AtomicUsize::new(0)),
            },
            rate_limiter: RateLimiter::new(config.max_requests_per_second),
            health_check_interval: Duration::from_secs(5),
            is_healthy: Arc::new(RwLock::new(true)),
        }
    }

    async fn record_success(&self, latency: Duration) {
        self.stats.latency_ms.store(latency.as_millis() as u64, Ordering::Relaxed);
        self.stats.success_count.fetch_add(1, Ordering::Relaxed);
        self.stats.consecutive_failures.store(0, Ordering::Relaxed);
        *self.is_healthy.write().await = true;
    }

    async fn record_failure(&self) {
        self.stats.failure_count.fetch_add(1, Ordering::Relaxed);
        let failures = self.stats.consecutive_failures.fetch_add(1, Ordering::Relaxed);
        *self.stats.last_failure.write().await = Some(Instant::now());
        
        if failures > 3 {
            *self.is_healthy.write().await = false;
        }
    }

    fn get_score(&self) -> f64 {
        let latency = self.stats.latency_ms.load(Ordering::Relaxed) as f64;
        let success = self.stats.success_count.load(Ordering::Relaxed) as f64;
        let failure = self.stats.failure_count.load(Ordering::Relaxed) as f64;
        
        let success_rate = if success + failure > 0.0 {
            success / (success + failure)
        } else {
            0.5
        };

        let latency_score = 1000.0 / (latency + 1.0);
        let priority_score = self.config.priority as f64 * 10.0;
        
        (success_rate * 100.0) + latency_score + priority_score
    }

    async fn health_check(&self) -> bool {
        match timeout(
            Duration::from_millis(self.config.timeout_ms),
            self.client.get_health()
        ).await {
            Ok(Ok(_)) => true,
            _ => false,
        }
    }
}

pub struct CustomRPCEndpoints {
    endpoints: Vec<Arc<Endpoint>>,
    semaphore: Arc<Semaphore>,
    fallback_commitment: CommitmentConfig,
    request_id_counter: Arc<AtomicU64>,
}

impl CustomRPCEndpoints {
    pub fn new(configs: Vec<EndpointConfig>) -> Self {
        let endpoints: Vec<Arc<Endpoint>> = configs
            .into_iter()
            .map(|config| Arc::new(Endpoint::new(config)))
            .collect();

        for endpoint in &endpoints {
            let endpoint_clone = Arc::clone(endpoint);
            tokio::spawn(async move {
                loop {
                    sleep(endpoint_clone.health_check_interval).await;
                    let is_healthy = endpoint_clone.health_check().await;
                    *endpoint_clone.is_healthy.write().await = is_healthy;
                }
            });
        }

        Self {
            endpoints,
            semaphore: Arc::new(Semaphore::new(100)),
            fallback_commitment: CommitmentConfig::confirmed(),
            request_id_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    async fn select_endpoint(&self) -> Result<Arc<Endpoint>, RpcError> {
        let mut healthy_endpoints: Vec<_> = Vec::new();
        
        for endpoint in &self.endpoints {
            if *endpoint.is_healthy.read().await {
                healthy_endpoints.push(endpoint.clone());
            }
        }

        if healthy_endpoints.is_empty() {
            return Err(RpcError::AllEndpointsFailed);
        }

        healthy_endpoints.sort_by(|a, b| {
            b.get_score().partial_cmp(&a.get_score()).unwrap()
        });

        let top_endpoints = &healthy_endpoints[..healthy_endpoints.len().min(3)];
        let selected = top_endpoints.choose(&mut rand::thread_rng())
            .ok_or(RpcError::AllEndpointsFailed)?;

        Ok(selected.clone())
    }

    async fn execute_with_retry<T, F, Fut>(
        &self,
        operation: F,
        max_retries: usize,
    ) -> Result<T, RpcError>
    where
        F: Fn(Arc<Endpoint>) -> Fut,
        Fut: std::future::Future<Output = Result<T, RpcError>>,
    {
        let mut last_error = RpcError::AllEndpointsFailed;
        let mut retry_delay = Duration::from_millis(50);

        for attempt in 0..=max_retries {
            let endpoint = self.select_endpoint().await?;
            
            match endpoint.rate_limiter.acquire().await {
                Ok(_) => {
                    let start = Instant::now();
                    match operation(endpoint.clone()).await {
                        Ok(result) => {
                            endpoint.record_success(start.elapsed()).await;
                            return Ok(result);
                        }
                        Err(e) => {
                            endpoint.record_failure().await;
                            last_error = e;
                            
                            if attempt < max_retries {
                                sleep(retry_delay).await;
                                retry_delay = (retry_delay * 2).min(Duration::from_secs(2));
                            }
                        }
                    }
                }
                Err(e) => {
                    last_error = e;
                    sleep(Duration::from_millis(100)).await;
                }
            }
        }

        Err(last_error)
    }

    pub async fn get_slot(&self, commitment: CommitmentConfig) -> Result<Slot, RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_slot_with_commitment(commitment)
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
        }, 3).await
    }

    pub async fn get_latest_blockhash(&self, commitment: CommitmentConfig) -> Result<(solana_sdk::hash::Hash, u64), RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_latest_blockhash_with_commitment(commitment)
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
        }, 3).await
    }

    pub async fn send_transaction(&self, transaction: &Transaction) -> Result<Signature, RpcError> {
        let config = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentLevel::Processed),
            encoding: Some(UiTransactionEncoding::Base64),
            max_retries: Some(0),
            min_context_slot: None,
        };

        let futures: Vec<_> = self.endpoints
            .iter()
            .take(3)
            .map(|endpoint| {
                let tx = transaction.clone();
                let cfg = config.clone();
                let ep = endpoint.clone();
                
                async move {
                    if *ep.is_healthy.read().await && ep.rate_limiter.acquire().await.is_ok() {
                        let start = Instant::now();
                        match ep.client.send_transaction_with_config(&tx, cfg).await {
                            Ok(sig) => {
                                ep.record_success(start.elapsed()).await;
                                Some(Ok(sig))
                            }
                            Err(e) => {
                                ep.record_failure().await;
                                Some(Err(RpcError::NetworkError(e.to_string())))
                            }
                        }
                    } else {
                        None
                    }
                }
            })
            .collect();

        let results = join_all(futures).await;
        
        for result in results {
            if let Some(Ok(sig)) = result {
                return Ok(sig);
            }
        }

        Err(RpcError::AllEndpointsFailed)
    }

    pub async fn simulate_transaction(
        &self,
        transaction: &Transaction,
        commitment: CommitmentConfig,
    ) -> Result<solana_client::rpc_response::RpcSimulateTransactionResult, RpcError> {
        let config = RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: true,
            commitment: Some(commitment),
            accounts: None,
            min_context_slot: None,
            inner_instructions: false,
        };

        self.execute_with_retry(|endpoint| {
            let tx = transaction.clone();
            let cfg = config.clone();
            async move {
                endpoint.client.simulate_transaction_with_config(&tx, cfg)
                    .await
                    .map_err(|e| RpcError::NetworkError(e.to_string()))
                    .map(|r| r.value)
            }
        }, 2).await
    }

    pub async fn get_multiple_accounts(
        &self,
        pubkeys: &[Pubkey],
        commitment: CommitmentConfig,
    ) -> Result<Vec<Option<solana_sdk::account::Account>>, RpcError> {
        self.execute_with_retry(|endpoint| {
            let keys = pubkeys.to_vec();
            async move {
                endpoint.client.get_multiple_accounts_with_commitment(&keys, commitment)
                    .await
                    .map_err(|e| RpcError::NetworkError(e.to_string()))
                    .map(|r| r.value)
            }
        }, 3).await
    }

    pub async fn get_signature_status(
        &self,
        signature: &Signature,
    ) -> Result<Option<TransactionStatus>, RpcError> {
        self.execute_with_retry(|endpoint| {
            let sig = *signature;
            async move {
                endpoint.client.get_signature_status(&sig)
                    .await
                    .map_err(|e| RpcError::NetworkError(e.to_string()))
            }
        }, 2).await
    }

    pub async fn get_signature_statuses(
        &self,
        signatures: &[Signature],
    ) -> Result<Vec<Option<TransactionStatus>>, RpcError> {
        self.execute_with_retry(|endpoint| {
            let sigs = signatures.to_vec();
            async move {
                endpoint.client.get_signature_statuses(&sigs)
                    .await
                    .map_err(|e| RpcError::NetworkError(e.to_string()))
                    .map(|r| r.value)
            }
        }, 2).await
    }

    pub async fn confirm_transaction_with_spinner(
        &self,
        signature: &Signature,
        recent_blockhash: &solana_sdk::hash::Hash,
        commitment: CommitmentConfig,
    ) -> Result<(), RpcError> {
        let start = Instant::now();
        let timeout_duration = Duration::from_secs(30);
        
        while start.elapsed() < timeout_duration {
            let status = self.get_signature_status(signature).await?;
            
            if let Some(status) = status {
                if status.satisfies_commitment(commitment) {
                    if let Some(err) = status.err {
                        return Err(RpcError::NetworkError(format!("Transaction failed: {:?}", err)));
                    }
                    return Ok(());
                }
            }
            
            if self.is_blockhash_expired(recent_blockhash, commitment).await? {
                return Err(RpcError::NetworkError("Blockhash expired".to_string()));
            }
            
            sleep(Duration::from_millis(400)).await;
        }
        
        Err(RpcError::RequestTimeout)
    }

    async fn is_blockhash_expired(
        &self,
        blockhash: &solana_sdk::hash::Hash,
        commitment: CommitmentConfig,
    ) -> Result<bool, RpcError> {
        self.execute_with_retry(|endpoint| {
            let hash = *blockhash;
            async move {
                endpoint.client.is_blockhash_valid(&hash, commitment)
                    .await
                    .map_err(|e| RpcError::NetworkError(e.to_string()))
                    .map(|valid| !valid)
            }
        }, 2).await
    }

    pub async fn get_recent_prioritization_fees(
        &self,
        accounts: &[Pubkey],
    ) -> Result<Vec<u64>, RpcError> {
        let request_id = self.request_id_counter.fetch_add(1, Ordering::Relaxed);
        
        self.execute_with_retry(|endpoint| {
            let accs = accounts.to_vec();
            async move {
                let params = serde_json::json!([accs]);
                
                let request = serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "getRecentPrioritizationFees",
                    "params": params
                });

                let response = endpoint.http_client
                    .post(&endpoint.config.url)
                    .json(&request)
                    .send()
                    .await
                    .map_err(|e| RpcError::NetworkError(e.to_string()))?;

                let json: Value = response.json().await
                    .map_err(|e| RpcError::NetworkError(e.to_string()))?;

                if let Some(result) = json.get("result") {
                    if let Some(arr) = result.as_array() {
                        let fees: Vec<u64> = arr.iter()
                            .filter_map(|v| v.get("prioritizationFee").and_then(|f| f.as_u64()))
                            .collect();
                        return Ok(fees);
                    }
                }

                Err(RpcError::InvalidResponse)
            }
        }, 2).await
    }

    pub async fn get_optimal_priority_fee(
        &self,
        accounts: &[Pubkey],
        percentile: f64,
    ) -> Result<u64, RpcError> {
        let fees = self.get_recent_prioritization_fees(accounts).await?;
        
        if fees.is_empty() {
            return Ok(1000);
        }

        let mut sorted_fees = fees;
        sorted_fees.sort_unstable();
        
        let index = ((sorted_fees.len() as f64 - 1.0) * percentile / 100.0) as usize;
        Ok(sorted_fees[index].max(1000))
    }

    pub async fn get_account_info(
        &self,
        pubkey: &Pubkey,
        commitment: CommitmentConfig,
    ) -> Result<Option<solana_sdk::account::Account>, RpcError> {
        self.execute_with_retry(|endpoint| {
            let key = *pubkey;
            async move {
                endpoint.client.get_account_with_commitment(&key, commitment)
                    .await
                    .map_err(|e| RpcError::NetworkError(e.to_string()))
                    .map(|r| r.value)
            }
        }, 3).await
    }

    pub async fn get_token_accounts_by_owner(
        &self,
        owner: &Pubkey,
        token_program_id: &Pubkey,
        commitment: CommitmentConfig,
    ) -> Result<Vec<(Pubkey, solana_sdk::account::Account)>, RpcError> {
        self.execute_with_retry(|endpoint| {
            let owner_key = *owner;
            let program_id = *token_program_id;
            async move {
                let filter = solana_client::rpc_filter::RpcFilterType::TokenAccountState;
                let config = solana_client::rpc_config::RpcAccountInfoConfig {
                    encoding: Some(UiTransactionEncoding::Base64),
                    commitment: Some(commitment),
                    data_slice: None,
                    min_context_slot: None,
                };

                endpoint.client
                    .get_token_accounts_by_owner(&owner_key, 
                        solana_client::rpc_filter::TokenAccountsFilter::ProgramId(program_id))
                    .await
                    .map_err(|e| RpcError::NetworkError(e.to_string()))
                    .map(|accounts| {
                        accounts.into_iter()
                            .map(|acc| (acc.pubkey, acc.account))
                            .collect()
                    })
            }
        }, 3).await
    }

    pub async fn get_program_accounts(
        &self,
        program_id: &Pubkey,
        filters: Vec<solana_client::rpc_filter::RpcFilterType>,
        commitment: CommitmentConfig,
    ) -> Result<Vec<(Pubkey, solana_sdk::account::Account)>, RpcError> {
        self.execute_with_retry(|endpoint| {
            let pid = *program_id;
            let f = filters.clone();
            async move {
                endpoint.client
                    .get_program_accounts_with_config(
                        &pid,
                        solana_client::rpc_config::RpcProgramAccountsConfig {
                            filters: Some(f),
                            account_config: solana_client::rpc_config::RpcAccountInfoConfig {
                                encoding: Some(UiTransactionEncoding::Base64),
                                commitment: Some(commitment),
                                data_slice: None,
                                min_context_slot: None,
                            },
                            with_context: Some(false),
                        },
                    )
                    .await
                    .map_err(|e| RpcError::NetworkError(e.to_string()))
            }
        }, 3).await
    }

    pub async fn send_transaction_with_retries(
        &self,
        transaction: &Transaction,
        max_retries: usize,
    ) -> Result<Signature, RpcError> {
        let mut last_error = RpcError::AllEndpointsFailed;
        
        for attempt in 0..=max_retries {
            match self.send_transaction(transaction).await {
                Ok(sig) => {
                    let confirmation_task = self.confirm_transaction_with_spinner(
                        &sig,
                        &transaction.message.recent_blockhash,
                        CommitmentConfig::confirmed(),
                    );

                    match timeout(Duration::from_secs(20), confirmation_task).await {
                        Ok(Ok(_)) => return Ok(sig),
                        Ok(Err(e)) => last_error = e,
                        Err(_) => last_error = RpcError::RequestTimeout,
                    }
                }
                Err(e) => last_error = e,
            }

            if attempt < max_retries {
                sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await;
            }
        }

        Err(last_error)
    }

    pub async fn parallel_get_multiple_accounts(
        &self,
        pubkeys: &[Pubkey],
        commitment: CommitmentConfig,
    ) -> Result<HashMap<Pubkey, Option<solana_sdk::account::Account>>, RpcError> {
        let chunk_size = 100;
        let mut all_results = HashMap::new();
        
        for chunk in pubkeys.chunks(chunk_size) {
            let accounts = self.get_multiple_accounts(chunk, commitment).await?;
            
            for (i, account) in accounts.into_iter().enumerate() {
                if let Some(pubkey) = chunk.get(i) {
                    all_results.insert(*pubkey, account);
                }
            }
        }

        Ok(all_results)
    }

    pub async fn get_block_time(&self, slot: Slot) -> Result<i64, RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_block_time(slot)
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
        }, 2).await
    }

    pub async fn get_blocks(
        &self,
        start_slot: Slot,
        end_slot: Option<Slot>,
        commitment: CommitmentConfig,
    ) -> Result<Vec<Slot>, RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_blocks_with_commitment(start_slot, end_slot, commitment)
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
        }, 2).await
    }

    pub async fn get_vote_accounts(
        &self,
        commitment: CommitmentConfig,
    ) -> Result<solana_client::rpc_response::RpcVoteAccountStatus, RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_vote_accounts_with_commitment(commitment)
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
        }, 2).await
    }

    pub async fn request_airdrop(
        &self,
        pubkey: &Pubkey,
        lamports: u64,
        commitment: CommitmentConfig,
    ) -> Result<Signature, RpcError> {
        self.execute_with_retry(|endpoint| {
            let key = *pubkey;
            async move {
                endpoint.client.request_airdrop_with_commitment(&key, lamports, commitment)
                    .await
                    .map_err(|e| RpcError::NetworkError(e.to_string()))
            }
        }, 3).await
    }

    pub async fn get_balance(
        &self,
        pubkey: &Pubkey,
        commitment: CommitmentConfig,
    ) -> Result<u64, RpcError> {
        self.execute_with_retry(|endpoint| {
            let key = *pubkey;
            async move {
                endpoint.client.get_balance_with_commitment(&key, commitment)
                    .await
                    .map_err(|e| RpcError::NetworkError(e.to_string()))
                    .map(|r| r.value)
            }
        }, 3).await
    }

    pub async fn get_minimum_balance_for_rent_exemption(
        &self,
        data_len: usize,
        commitment: CommitmentConfig,
    ) -> Result<u64, RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_minimum_balance_for_rent_exemption(data_len)
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
        }, 2).await
    }

    pub fn get_endpoint_stats(&self) -> Vec<(String, EndpointStats)> {
        self.endpoints
            .iter()
            .map(|ep| (ep.config.url.clone(), ep.stats.clone()))
            .collect()
    }

    pub async fn batch_request<T: serde::de::DeserializeOwned>(
        &self,
        requests: Vec<(String, Value)>,
    ) -> Result<Vec<T>, RpcError> {
        let endpoint = self.select_endpoint().await?;
        
        let batch: Vec<_> = requests
            .into_iter()
            .enumerate()
            .map(|(id, (method, params))| {
                serde_json::json!({
                    "jsonrpc": "2.0",
                    "id": id,
                    "method": method,
                    "params": params
                })
            })
            .collect();

        let response = endpoint.http_client
            .post(&endpoint.config.url)
            .json(&batch)
            .send()
            .await
            .map_err(|e| RpcError::NetworkError(e.to_string()))?;

        let results: Vec<Value> = response.json().await
            .map_err(|e| RpcError::NetworkError(e.to_string()))?;

        results.into_iter()
            .map(|r| {
                serde_json::from_value(r["result"].clone())
                    .map_err(|e| RpcError::InvalidResponse)
            })
            .collect::<Result<Vec<T>, _>>()
    }

    pub async fn get_epoch_info(
        &self,
        commitment: CommitmentConfig,
    ) -> Result<solana_client::rpc_response::EpochInfo, RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_epoch_info_with_commitment(commitment)
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
        }, 2).await
    }

    pub async fn get_leader_schedule(
        &self,
        slot: Option<Slot>,
        commitment: CommitmentConfig,
    ) -> Result<Option<HashMap<String, Vec<usize>>>, RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_leader_schedule_with_commitment(slot, commitment)
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
        }, 2).await
    }

    pub async fn get_slot_leaders(
        &self,
        start_slot: Slot,
        limit: u64,
    ) -> Result<Vec<Pubkey>, RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_slot_leaders(start_slot, limit)
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
        }, 2).await
    }

    pub async fn get_transaction_count(
        &self,
        commitment: CommitmentConfig,
    ) -> Result<u64, RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_transaction_count_with_commitment(commitment)
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
        }, 2).await
    }

    pub async fn get_first_available_block(&self) -> Result<Slot, RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_first_available_block()
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
        }, 2).await
    }

    pub async fn get_genesis_hash(&self) -> Result<solana_sdk::hash::Hash, RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_genesis_hash()
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
        }, 2).await
    }

    pub async fn get_cluster_nodes(&self) -> Result<Vec<solana_client::rpc_response::RpcContactInfo>, RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_cluster_nodes()
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
        }, 2).await
    }

    pub async fn minimum_ledger_slot(&self) -> Result<Slot, RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.minimum_ledger_slot()
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
        }, 2).await
    }

    pub async fn get_supply(
        &self,
        commitment: CommitmentConfig,
    ) -> Result<solana_client::rpc_response::RpcSupply, RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_supply_with_commitment(commitment)
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
                .map(|r| r.value)
        }, 2).await
    }

    pub async fn get_largest_accounts(
        &self,
        config: Option<solana_client::rpc_config::RpcLargestAccountsConfig>,
    ) -> Result<Vec<solana_client::rpc_response::RpcAccountBalance>, RpcError> {
        self.execute_with_retry(|endpoint| {
            let cfg = config.clone();
            async move {
                endpoint.client.get_largest_accounts_with_config(cfg)
                    .await
                    .map_err(|e| RpcError::NetworkError(e.to_string()))
                    .map(|r| r.value)
            }
        }, 2).await
    }

    pub async fn get_recent_blockhash_with_commitment(
        &self,
        commitment: CommitmentConfig,
    ) -> Result<(solana_sdk::hash::Hash, solana_sdk::fee_calculator::FeeCalculator, Slot), RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_recent_blockhash_with_commitment(commitment)
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
                .map(|r| (r.value.0, r.value.1, r.context.slot))
        }, 3).await
    }

    pub async fn get_fee_for_message(
        &self,
        message: &solana_sdk::message::Message,
        commitment: CommitmentConfig,
    ) -> Result<u64, RpcError> {
        self.execute_with_retry(|endpoint| {
            let msg = message.clone();
            async move {
                endpoint.client.get_fee_for_message(&msg, commitment)
                    .await
                    .map_err(|e| RpcError::NetworkError(e.to_string()))
                    .map(|f| f.unwrap_or(5000))
            }
        }, 2).await
    }

    pub async fn send_and_confirm_transaction(
        &self,
        transaction: &Transaction,
    ) -> Result<Signature, RpcError> {
        let signature = self.send_transaction(transaction).await?;
        
        self.confirm_transaction_with_spinner(
            &signature,
            &transaction.message.recent_blockhash,
            CommitmentConfig::confirmed(),
        ).await?;

        Ok(signature)
    }

    pub async fn get_health(&self) -> Result<String, RpcError> {
        let healthy_count = futures::future::join_all(
            self.endpoints.iter().map(|ep| async move {
                if *ep.is_healthy.read().await { 1 } else { 0 }
            })
        ).await.iter().sum::<i32>();

        Ok(format!("Healthy endpoints: {}/{}", healthy_count, self.endpoints.len()))
    }

    pub async fn get_version(&self) -> Result<solana_client::rpc_response::RpcVersionInfo, RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_version()
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
        }, 2).await
    }

    pub async fn get_inflation_governor(
        &self,
        commitment: CommitmentConfig,
    ) -> Result<solana_client::rpc_response::RpcInflationGovernor, RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_inflation_governor()
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
                .map(|r| r.value)
        }, 2).await
    }

    pub async fn get_inflation_rate(&self) -> Result<solana_client::rpc_response::RpcInflationRate, RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_inflation_rate()
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
        }, 2).await
    }

    pub async fn get_inflation_reward(
        &self,
        addresses: &[Pubkey],
        epoch: Option<solana_sdk::epoch_info::Epoch>,
    ) -> Result<Vec<Option<solana_client::rpc_response::RpcInflationReward>>, RpcError> {
        self.execute_with_retry(|endpoint| {
            let addrs = addresses.to_vec();
            async move {
                endpoint.client.get_inflation_reward(&addrs, epoch)
                    .await
                    .map_err(|e| RpcError::NetworkError(e.to_string()))
            }
        }, 2).await
    }

    pub async fn get_block(
        &self,
        slot: Slot,
    ) -> Result<solana_transaction_status::EncodedConfirmedBlock, RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_block(slot)
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
        }, 2).await
    }

    pub async fn get_block_height(
        &self,
        commitment: CommitmentConfig,
    ) -> Result<u64, RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_block_height_with_commitment(commitment)
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
        }, 2).await
    }

    pub async fn get_blocks_with_limit(
        &self,
        start_slot: Slot,
        limit: usize,
        commitment: CommitmentConfig,
    ) -> Result<Vec<Slot>, RpcError> {
        self.execute_with_retry(|endpoint| async move {
            endpoint.client.get_blocks_with_limit_and_commitment(start_slot, limit, commitment)
                .await
                .map_err(|e| RpcError::NetworkError(e.to_string()))
        }, 2).await
    }

    pub async fn get_signatures_for_address(
        &self,
        address: &Pubkey,
        config: Option<solana_client::rpc_config::GetConfirmedSignaturesForAddress2Config>,
    ) -> Result<Vec<solana_client::rpc_response::RpcConfirmedTransactionStatusWithSignature>, RpcError> {
        self.execute_with_retry(|endpoint| {
            let addr = *address;
            let cfg = config.clone();
            async move {
                endpoint.client.get_signatures_for_address_with_config(&addr, cfg)
                    .await
                    .map_err(|e| RpcError::NetworkError(e.to_string()))
            }
        }, 3).await
    }

    pub async fn get_transaction(
        &self,
        signature: &Signature,
        encoding: UiTransactionEncoding,
    ) -> Result<solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta, RpcError> {
        self.execute_with_retry(|endpoint| {
            let sig = *signature;
            async move {
                endpoint.client.get_transaction(&sig, encoding)
                    .await
                    .map_err(|e| RpcError::NetworkError(e.to_string()))
            }
        }, 2).await
    }

    pub async fn shutdown(&self) {
        // Graceful shutdown - wait for ongoing requests
        for _ in 0..self.semaphore.available_permits() {
            let _ = self.semaphore.acquire().await;
        }
    }
}

impl Default for CustomRPCEndpoints {
    fn default() -> Self {
        let default_configs = vec![
            EndpointConfig {
                url: "https://api.mainnet-beta.solana.com".to_string(),
                ws_url: "wss://api.mainnet-beta.solana.com".to_string(),
                weight: 1,
                max_requests_per_second: 40,
                timeout_ms: 5000,
                priority: 5,
            },
        ];
        Self::new(default_configs)
    }
}
