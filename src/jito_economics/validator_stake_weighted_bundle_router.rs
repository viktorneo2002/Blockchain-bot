use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::{HashMap, BinaryHeap};
use std::cmp::Ordering;
use tokio::sync::{RwLock, Mutex, Semaphore};
use tokio::time::{interval, timeout};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    commitment_config::CommitmentConfig,
    clock::Slot,
};
use solana_streamer::nonblocking::quic::ConnectionCache;
use solana_bundle::BundleRequest;
use futures::future::join_all;
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use dashmap::DashMap;
use crossbeam::queue::ArrayQueue;
use parking_lot::RwLock as PLRwLock;

const MAX_VALIDATORS: usize = 32;
const STAKE_UPDATE_INTERVAL: Duration = Duration::from_secs(300);
const BUNDLE_TIMEOUT: Duration = Duration::from_millis(250);
const MAX_RETRIES: u8 = 3;
const MIN_STAKE_THRESHOLD: u64 = 1_000_000_000_000;
const CONNECTION_POOL_SIZE: usize = 16;
const LATENCY_WINDOW_SIZE: usize = 1000;
const FAILURE_PENALTY_FACTOR: f64 = 0.95;
const SUCCESS_REWARD_FACTOR: f64 = 1.02;
const MAX_CONCURRENT_BUNDLES: usize = 128;
const LEADER_SCHEDULE_CACHE_SLOTS: u64 = 432000;

#[derive(Clone, Debug)]
pub struct ValidatorInfo {
    pub pubkey: Pubkey,
    pub stake: u64,
    pub tpu_addr: String,
    pub rpc_endpoint: String,
    pub latency_ms: Arc<PLRwLock<Vec<u64>>>,
    pub success_rate: Arc<PLRwLock<f64>>,
    pub last_success: Arc<PLRwLock<Instant>>,
    pub failures: Arc<PLRwLock<u32>>,
    pub connection_cache: Arc<ConnectionCache>,
}

#[derive(Clone)]
struct ValidatorScore {
    pubkey: Pubkey,
    score: f64,
    stake: u64,
}

impl Ord for ValidatorScore {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score.partial_cmp(&other.score).unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for ValidatorScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Eq for ValidatorScore {}

impl PartialEq for ValidatorScore {
    fn eq(&self, other: &Self) -> bool {
        self.score == other.score
    }
}

pub struct ValidatorStakeWeightedBundleRouter {
    validators: Arc<DashMap<Pubkey, Arc<ValidatorInfo>>>,
    rpc_client: Arc<RpcClient>,
    leader_schedule: Arc<RwLock<HashMap<Slot, Pubkey>>>,
    current_slot: Arc<RwLock<Slot>>,
    bundle_semaphore: Arc<Semaphore>,
    submission_queue: Arc<ArrayQueue<BundleRequest>>,
    metrics: Arc<RouterMetrics>,
}

#[derive(Default)]
struct RouterMetrics {
    total_bundles: Arc<PLRwLock<u64>>,
    successful_bundles: Arc<PLRwLock<u64>>,
    failed_bundles: Arc<PLRwLock<u64>>,
    avg_latency: Arc<PLRwLock<f64>>,
}

impl ValidatorStakeWeightedBundleRouter {
    pub async fn new(rpc_url: &str) -> anyhow::Result<Self> {
        let rpc_client = Arc::new(RpcClient::new_with_timeout_and_commitment(
            rpc_url.to_string(),
            Duration::from_secs(10),
            CommitmentConfig::confirmed(),
        ));

        let router = Self {
            validators: Arc::new(DashMap::new()),
            rpc_client,
            leader_schedule: Arc::new(RwLock::new(HashMap::new())),
            current_slot: Arc::new(RwLock::new(0)),
            bundle_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_BUNDLES)),
            submission_queue: Arc::new(ArrayQueue::new(10000)),
            metrics: Arc::new(RouterMetrics::default()),
        };

        router.start_background_tasks().await;
        Ok(router)
    }

    async fn start_background_tasks(&self) {
        let router = self.clone();
        tokio::spawn(async move {
            router.update_validator_stakes_loop().await;
        });

        let router = self.clone();
        tokio::spawn(async move {
            router.update_leader_schedule_loop().await;
        });

        let router = self.clone();
        tokio::spawn(async move {
            router.process_submission_queue().await;
        });

        let router = self.clone();
        tokio::spawn(async move {
            router.update_current_slot_loop().await;
        });
    }

    async fn update_validator_stakes_loop(&self) {
        let mut interval = interval(STAKE_UPDATE_INTERVAL);
        loop {
            interval.tick().await;
            if let Err(e) = self.update_validator_stakes().await {
                tracing::error!("Failed to update validator stakes: {:?}", e);
            }
        }
    }

    async fn update_validator_stakes(&self) -> anyhow::Result<()> {
        let vote_accounts = self.rpc_client.get_vote_accounts().await?;
        
        let mut new_validators = HashMap::new();
        
        for vote_account in vote_accounts.current.iter().chain(vote_accounts.delinquent.iter()) {
            if vote_account.activated_stake < MIN_STAKE_THRESHOLD {
                continue;
            }

            let pubkey = vote_account.vote_pubkey.parse::<Pubkey>()?;
            let stake = vote_account.activated_stake;
            
            if let Some(existing) = self.validators.get(&pubkey) {
                new_validators.insert(pubkey, existing.clone());
            } else {
                let validator_info = self.create_validator_info(pubkey, stake).await?;
                new_validators.insert(pubkey, Arc::new(validator_info));
            }
        }

        self.validators.clear();
        for (pubkey, info) in new_validators {
            self.validators.insert(pubkey, info);
        }

        Ok(())
    }

    async fn create_validator_info(&self, pubkey: Pubkey, stake: u64) -> anyhow::Result<ValidatorInfo> {
        let cluster_nodes = self.rpc_client.get_cluster_nodes().await?;
        
        let node = cluster_nodes
            .iter()
            .find(|n| n.pubkey == pubkey.to_string())
            .ok_or_else(|| anyhow::anyhow!("Node not found for validator"))?;

        let tpu_addr = node.tpu.ok_or_else(|| anyhow::anyhow!("No TPU address"))?;
        let rpc_endpoint = node.rpc.unwrap_or_else(|| format!("http://{}", tpu_addr));

        let connection_cache = Arc::new(ConnectionCache::new(CONNECTION_POOL_SIZE));

        Ok(ValidatorInfo {
            pubkey,
            stake,
            tpu_addr,
            rpc_endpoint,
            latency_ms: Arc::new(PLRwLock::new(Vec::with_capacity(LATENCY_WINDOW_SIZE))),
            success_rate: Arc::new(PLRwLock::new(1.0)),
            last_success: Arc::new(PLRwLock::new(Instant::now())),
            failures: Arc::new(PLRwLock::new(0)),
            connection_cache,
        })
    }

    async fn update_leader_schedule_loop(&self) {
        let mut interval = interval(Duration::from_secs(3600));
        loop {
            interval.tick().await;
            if let Err(e) = self.update_leader_schedule().await {
                tracing::error!("Failed to update leader schedule: {:?}", e);
            }
        }
    }

    async fn update_leader_schedule(&self) -> anyhow::Result<()> {
        let current_slot = *self.current_slot.read().await;
        let leader_schedule = self.rpc_client
            .get_leader_schedule_with_commitment(
                Some(current_slot),
                CommitmentConfig::finalized(),
            )
            .await?;

        if let Some(schedule) = leader_schedule {
            let mut new_schedule = HashMap::new();
            for (validator_str, slots) in schedule {
                let validator = validator_str.parse::<Pubkey>()?;
                for &slot in slots.iter() {
                    new_schedule.insert(current_slot + slot as u64, validator);
                }
            }
            *self.leader_schedule.write().await = new_schedule;
        }

        Ok(())
    }

    async fn update_current_slot_loop(&self) {
        let mut interval = interval(Duration::from_millis(400));
        loop {
            interval.tick().await;
            if let Ok(slot) = self.rpc_client.get_slot().await {
                *self.current_slot.write().await = slot;
            }
        }
    }

    async fn process_submission_queue(&self) {
        loop {
            if let Some(bundle) = self.submission_queue.pop() {
                let router = self.clone();
                tokio::spawn(async move {
                    if let Err(e) = router.route_bundle_internal(bundle).await {
                        tracing::error!("Bundle routing failed: {:?}", e);
                    }
                });
            } else {
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
        }
    }

    pub async fn route_bundle(&self, bundle: BundleRequest) -> anyhow::Result<Signature> {
        let _permit = self.bundle_semaphore
            .acquire()
            .await
            .map_err(|_| anyhow::anyhow!("Semaphore closed"))?;

        self.submission_queue
            .push(bundle.clone())
            .map_err(|_| anyhow::anyhow!("Queue full"))?;

        self.route_bundle_internal(bundle).await
    }

    async fn route_bundle_internal(&self, bundle: BundleRequest) -> anyhow::Result<Signature> {
        let current_slot = *self.current_slot.read().await;
        let target_slots = (current_slot + 1..=current_slot + 4).collect::<Vec<_>>();
        
        let mut target_validators = Vec::new();
        let leader_schedule = self.leader_schedule.read().await;
        
        for slot in target_slots {
            if let Some(leader) = leader_schedule.get(&slot) {
                if self.validators.contains_key(leader) {
                    target_validators.push(*leader);
                }
            }
        }

        if target_validators.is_empty() {
            let validators = self.select_validators_by_stake_weight(4).await?;
            target_validators = validators.into_iter().map(|v| v.pubkey).collect();
        }

        let submission_tasks = target_validators
            .into_iter()
            .map(|validator| {
                let bundle = bundle.clone();
                let router = self.clone();
                tokio::spawn(async move {
                    router.submit_to_validator(validator, bundle).await
                })
            })
            .collect::<Vec<_>>();

        let results = join_all(submission_tasks).await;
        
        for result in results {
            if let Ok(Ok(signature)) = result {
                return Ok(signature);
            }
        }

        Err(anyhow::anyhow!("All bundle submissions failed"))
    }

    async fn select_validators_by_stake_weight(&self, count: usize) -> anyhow::Result<Vec<Arc<ValidatorInfo>>> {
        let mut scored_validators = BinaryHeap::new();
        
        for entry in self.validators.iter() {
            let validator = entry.value();
            let score = self.calculate_validator_score(validator).await;
            
            scored_validators.push(ValidatorScore {
                pubkey: validator.pubkey,
                score,
                stake: validator.stake,
            });
        }

        let mut selected = Vec::new();
        let mut total_stake = 0u64;
        
        while selected.len() < count.min(scored_validators.len()) {
            if let Some(scored) = scored_validators.pop() {
                if let Some(validator) = self.validators.get(&scored.pubkey) {
                    selected.push(validator.clone());
                    total_stake += scored.stake;
                }
            }
        }

        if selected.is_empty() {
            return Err(anyhow::anyhow!("No validators available"));
        }

        let weights: Vec<f64> = selected
            .iter()
            .map(|v| (v.stake as f64 / total_stake as f64) * self.calculate_dynamic_weight(v))
            .collect();

        let dist = WeightedIndex::new(&weights)?;
        let mut rng = thread_rng();
        
        let mut final_selection = Vec::new();
        let mut selected_indices = std::collections::HashSet::new();
        
        while final_selection.len() < count && selected_indices.len() < selected.len() {
            let idx = dist.sample(&mut rng);
            if selected_indices.insert(idx) {
                final_selection.push(selected[idx].clone());
            }
        }

        Ok(final_selection)
    }

        async fn calculate_validator_score(&self, validator: &ValidatorInfo) -> f64 {
        let stake_weight = (validator.stake as f64).log10() / 15.0;
        let success_rate = *validator.success_rate.read();
        let failures = *validator.failures.read();
        
        let latency_scores = validator.latency_ms.read();
        let avg_latency = if latency_scores.is_empty() {
            50.0
        } else {
            latency_scores.iter().sum::<u64>() as f64 / latency_scores.len() as f64
        };
        
        let latency_score = 1.0 / (1.0 + (avg_latency / 100.0).powi(2));
        let time_since_success = validator.last_success.read().elapsed().as_secs() as f64;
        let recency_score = 1.0 / (1.0 + (time_since_success / 300.0).powi(2));
        
        let failure_penalty = FAILURE_PENALTY_FACTOR.powi(failures.min(10));
        
        let base_score = stake_weight * 0.4 
            + success_rate * 0.3 
            + latency_score * 0.2 
            + recency_score * 0.1;
            
        base_score * failure_penalty
    }

    fn calculate_dynamic_weight(&self, validator: &Arc<ValidatorInfo>) -> f64 {
        let success_rate = *validator.success_rate.read();
        let failures = *validator.failures.read();
        
        let base_weight = if success_rate > 0.95 {
            SUCCESS_REWARD_FACTOR
        } else if success_rate < 0.7 {
            FAILURE_PENALTY_FACTOR
        } else {
            1.0
        };
        
        base_weight * (1.0 - (failures as f64 * 0.05).min(0.5))
    }

    async fn submit_to_validator(
        &self,
        validator_pubkey: Pubkey,
        bundle: BundleRequest,
    ) -> anyhow::Result<Signature> {
        let validator = self.validators
            .get(&validator_pubkey)
            .ok_or_else(|| anyhow::anyhow!("Validator not found"))?;
        
        let start_time = Instant::now();
        let mut last_error = None;
        
        for attempt in 0..MAX_RETRIES {
            if attempt > 0 {
                tokio::time::sleep(Duration::from_millis(10 * (1 << attempt))).await;
            }
            
            match self.submit_bundle_attempt(&validator, &bundle).await {
                Ok(signature) => {
                    let latency = start_time.elapsed().as_millis() as u64;
                    self.update_validator_metrics(&validator, true, latency).await;
                    return Ok(signature);
                }
                Err(e) => {
                    last_error = Some(e);
                    if attempt == MAX_RETRIES - 1 {
                        let latency = start_time.elapsed().as_millis() as u64;
                        self.update_validator_metrics(&validator, false, latency).await;
                    }
                }
            }
        }
        
        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Bundle submission failed")))
    }

    async fn submit_bundle_attempt(
        &self,
        validator: &Arc<ValidatorInfo>,
        bundle: &BundleRequest,
    ) -> anyhow::Result<Signature> {
        let bundle_data = self.serialize_bundle(bundle)?;
        
        let client = reqwest::Client::builder()
            .timeout(BUNDLE_TIMEOUT)
            .tcp_nodelay(true)
            .tcp_keepalive(Some(Duration::from_secs(10)))
            .pool_max_idle_per_host(CONNECTION_POOL_SIZE)
            .build()?;
        
        let response = timeout(
            BUNDLE_TIMEOUT,
            client
                .post(format!("{}/api/v1/bundles", validator.rpc_endpoint))
                .header("Content-Type", "application/json")
                .body(bundle_data)
                .send()
        ).await??;
        
        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("Bundle submission failed: {}", error_text));
        }
        
        let result: BundleSubmissionResult = response.json().await?;
        
        match result {
            BundleSubmissionResult::Success { signature } => {
                Ok(signature.parse()?)
            }
            BundleSubmissionResult::Error { message } => {
                Err(anyhow::anyhow!("Bundle rejected: {}", message))
            }
        }
    }

    fn serialize_bundle(&self, bundle: &BundleRequest) -> anyhow::Result<Vec<u8>> {
        bincode::serialize(bundle).map_err(Into::into)
    }

    async fn update_validator_metrics(
        &self,
        validator: &Arc<ValidatorInfo>,
        success: bool,
        latency_ms: u64,
    ) {
        {
            let mut latencies = validator.latency_ms.write();
            if latencies.len() >= LATENCY_WINDOW_SIZE {
                latencies.remove(0);
            }
            latencies.push(latency_ms);
        }
        
        if success {
            *validator.last_success.write() = Instant::now();
            *validator.failures.write() = 0;
            
            let mut success_rate = validator.success_rate.write();
            *success_rate = (*success_rate * 0.95) + 0.05;
            
            *self.metrics.successful_bundles.write() += 1;
        } else {
            *validator.failures.write() += 1;
            
            let mut success_rate = validator.success_rate.write();
            *success_rate = (*success_rate * 0.95);
            
            *self.metrics.failed_bundles.write() += 1;
        }
        
        *self.metrics.total_bundles.write() += 1;
        
        let total = *self.metrics.total_bundles.read();
        let successful = *self.metrics.successful_bundles.read();
        let avg_latency = self.metrics.avg_latency.write();
        *avg_latency = (*avg_latency * (total - 1) as f64 + latency_ms as f64) / total as f64;
    }

    pub async fn get_validator_stats(&self) -> HashMap<Pubkey, ValidatorStats> {
        let mut stats = HashMap::new();
        
        for entry in self.validators.iter() {
            let validator = entry.value();
            let latencies = validator.latency_ms.read();
            
            let avg_latency = if latencies.is_empty() {
                0.0
            } else {
                latencies.iter().sum::<u64>() as f64 / latencies.len() as f64
            };
            
            stats.insert(
                validator.pubkey,
                ValidatorStats {
                    stake: validator.stake,
                    success_rate: *validator.success_rate.read(),
                    avg_latency_ms: avg_latency,
                    failures: *validator.failures.read(),
                    last_success: *validator.last_success.read(),
                },
            );
        }
        
        stats
    }

    pub async fn get_metrics(&self) -> RouterMetricsSnapshot {
        RouterMetricsSnapshot {
            total_bundles: *self.metrics.total_bundles.read(),
            successful_bundles: *self.metrics.successful_bundles.read(),
            failed_bundles: *self.metrics.failed_bundles.read(),
            avg_latency_ms: *self.metrics.avg_latency.read(),
            success_rate: {
                let total = *self.metrics.total_bundles.read();
                let successful = *self.metrics.successful_bundles.read();
                if total > 0 {
                    successful as f64 / total as f64
                } else {
                    0.0
                }
            },
        }
    }

    pub async fn force_validator_update(&self) -> anyhow::Result<()> {
        self.update_validator_stakes().await?;
        self.update_leader_schedule().await?;
        Ok(())
    }

    pub async fn get_active_validator_count(&self) -> usize {
        self.validators.len()
    }

    pub async fn get_current_leader(&self) -> Option<Pubkey> {
        let current_slot = *self.current_slot.read().await;
        self.leader_schedule.read().await.get(&current_slot).copied()
    }

    pub async fn clear_validator_failures(&self, validator_pubkey: &Pubkey) {
        if let Some(validator) = self.validators.get(validator_pubkey) {
            *validator.failures.write() = 0;
            *validator.success_rate.write() = 1.0;
        }
    }

    pub async fn remove_validator(&self, validator_pubkey: &Pubkey) {
        self.validators.remove(validator_pubkey);
    }

    pub async fn prioritize_validator(&self, validator_pubkey: &Pubkey) {
        if let Some(validator) = self.validators.get(validator_pubkey) {
            *validator.success_rate.write() = 1.0;
            *validator.failures.write() = 0;
            *validator.last_success.write() = Instant::now();
        }
    }
}

impl Clone for ValidatorStakeWeightedBundleRouter {
    fn clone(&self) -> Self {
        Self {
            validators: self.validators.clone(),
            rpc_client: self.rpc_client.clone(),
            leader_schedule: self.leader_schedule.clone(),
            current_slot: self.current_slot.clone(),
            bundle_semaphore: self.bundle_semaphore.clone(),
            submission_queue: self.submission_queue.clone(),
            metrics: self.metrics.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorStats {
    pub stake: u64,
    pub success_rate: f64,
    pub avg_latency_ms: f64,
    pub failures: u32,
    pub last_success: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterMetricsSnapshot {
    pub total_bundles: u64,
    pub successful_bundles: u64,
    pub failed_bundles: u64,
    pub avg_latency_ms: f64,
    pub success_rate: f64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum BundleSubmissionResult {
    Success { signature: String },
    Error { message: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_validator_scoring() {
        let router = ValidatorStakeWeightedBundleRouter::new("https://api.mainnet-beta.solana.com")
            .await
            .unwrap();
        
        let validator = ValidatorInfo {
            pubkey: Pubkey::new_unique(),
            stake: 10_000_000_000_000,
            tpu_addr: "127.0.0.1:8001".to_string(),
            rpc_endpoint: "http://127.0.0.1:8899".to_string(),
            latency_ms: Arc::new(PLRwLock::new(vec![10, 20, 30, 40, 50])),
            success_rate: Arc::new(PLRwLock::new(0.95)),
            last_success: Arc::new(PLRwLock::new(Instant::now())),
            failures: Arc::new(PLRwLock::new(0)),
            connection_cache: Arc::new(ConnectionCache::new(1)),
        };
        
        let score = router.calculate_validator_score(&validator).await;
        assert!(score > 0.0 && score <= 1.0);
    }
}

