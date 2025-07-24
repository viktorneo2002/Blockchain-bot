use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use futures::{future::join_all, stream::FuturesUnordered, StreamExt};
use log::{debug, error, info, trace, warn};
use rand::Rng;
use solana_client::{
    nonblocking::tpu_client::TpuClient,
    rpc_client::RpcClient,
    rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig},
    tpu_client::TpuClientConfig,
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::Instruction,
    message::Message,
    packet::PACKET_DATA_SIZE,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    transaction::{Transaction, TransactionError},
};
use solana_transaction_status::{TransactionConfirmationStatus, UiTransactionEncoding};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex, RwLock, Semaphore},
    time::{interval, sleep, timeout},
};

const MAX_RETRIES: u32 = 6;
const INITIAL_RETRY_DELAY_MS: u64 = 25;
const MAX_RETRY_DELAY_MS: u64 = 1600;
const CONFIRMATION_TIMEOUT_MS: u64 = 25000;
const LEADER_REFRESH_INTERVAL_MS: u64 = 300;
const MAX_CONCURRENT_SENDS: usize = 128;
const JITO_TIP_LAMPORTS: u64 = 15_000;
const MAX_COMPUTE_UNITS: u32 = 1_400_000;
const PRIORITY_FEE_PERCENTILE: f64 = 0.98;
const PACKET_FANOUT: usize = 40;
const CONNECTION_CACHE_SIZE: usize = 512;
const MAX_BUNDLE_SIZE: usize = 5;
const SLOT_MS: u64 = 400;
const JITO_BUNDLE_TIMEOUT_MS: u64 = 5000;
const HEALTH_CHECK_INTERVAL_MS: u64 = 1000;
const MAX_RECENT_BLOCKHASHES: usize = 5;

#[derive(Clone)]
pub struct TransactionConfig {
    pub priority_fee_lamports: u64,
    pub compute_units: u32,
    pub use_jito: bool,
    pub skip_preflight: bool,
    pub max_retries: Option<u32>,
    pub require_confirmations: u8,
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self {
            priority_fee_lamports: 0,
            compute_units: 200_000,
            use_jito: true,
            skip_preflight: true,
            max_retries: Some(MAX_RETRIES),
            require_confirmations: 1,
        }
    }
}

pub struct MempoolLessInjector {
    rpc_clients: Vec<Arc<RpcClient>>,
    tpu_client: Arc<RwLock<TpuClient>>,
    leader_tracker: Arc<LeaderTracker>,
    jito_client: Option<Arc<JitoClient>>,
    metrics: Arc<Metrics>,
    send_semaphore: Arc<Semaphore>,
    network_monitor: Arc<NetworkMonitor>,
    recent_blockhashes: Arc<RwLock<VecDeque<(Hash, u64)>>>,
    shutdown: Arc<AtomicBool>,
}

struct LeaderTracker {
    current_slot: AtomicU64,
    epoch_info: RwLock<EpochInfo>,
    leader_schedule: RwLock<HashMap<u64, Pubkey>>,
    stake_weights: RwLock<HashMap<Pubkey, u64>>,
    last_update: RwLock<Instant>,
}

struct EpochInfo {
    epoch: u64,
    slot_index: u64,
    slots_per_epoch: u64,
    absolute_slot: u64,
}

struct JitoClient {
    endpoints: Vec<String>,
    auth_keypair: Arc<Keypair>,
    tip_accounts: Vec<Pubkey>,
    current_tip_index: AtomicUsize,
    client: reqwest::Client,
}

struct Metrics {
    sent_count: AtomicU64,
    confirmed_count: AtomicU64,
    failed_count: AtomicU64,
    retry_count: AtomicU64,
    jito_success_count: AtomicU64,
    avg_confirmation_ms: AtomicU64,
    total_confirmation_ms: AtomicU64,
}

struct NetworkMonitor {
    rpc_latencies: RwLock<VecDeque<Duration>>,
    confirmation_rates: RwLock<VecDeque<f64>>,
    network_quality: RwLock<NetworkQuality>,
    last_health_check: RwLock<Instant>,
}

#[derive(Debug, Clone, Copy)]
enum NetworkQuality {
    Excellent,
    Good,
    Fair,
    Poor,
    Critical,
}

impl MempoolLessInjector {
    pub async fn new(
        rpc_urls: Vec<String>,
        websocket_url: String,
        jito_config: Option<(Vec<String>, Arc<Keypair>)>,
    ) -> Result<Self> {
        if rpc_urls.is_empty() {
            return Err(anyhow!("No RPC endpoints provided"));
        }

        let rpc_clients: Vec<Arc<RpcClient>> = rpc_urls
            .into_iter()
            .map(|url| {
                Arc::new(RpcClient::new_with_timeout_and_commitment(
                    url,
                    Duration::from_secs(30),
                    CommitmentConfig::confirmed(),
                ))
            })
            .collect();

        let connection_cache = Arc::new(
            solana_client::connection_cache::ConnectionCache::new_with_client_options(
                "mempoolless_injector",
                CONNECTION_CACHE_SIZE,
                None,
                Some(Duration::from_secs(10)),
                Some(2),
            ),
        );

        let tpu_config = TpuClientConfig {
            fanout_slots: 12,
            ..Default::default()
        };

        let tpu_client = Arc::new(RwLock::new(
            TpuClient::new_with_connection_cache(
                Arc::clone(&rpc_clients[0]),
                &websocket_url,
                tpu_config,
                connection_cache,
            )
            .await
            .context("Failed to create TPU client")?,
        ));

        let leader_tracker = Arc::new(LeaderTracker {
            current_slot: AtomicU64::new(0),
            epoch_info: RwLock::new(EpochInfo {
                epoch: 0,
                slot_index: 0,
                slots_per_epoch: 432000,
                absolute_slot: 0,
            }),
            leader_schedule: RwLock::new(HashMap::new()),
            stake_weights: RwLock::new(HashMap::new()),
            last_update: RwLock::new(Instant::now()),
        });

        let jito_client = if let Some((endpoints, auth_keypair)) = jito_config {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_millis(JITO_BUNDLE_TIMEOUT_MS))
                .pool_max_idle_per_host(10)
                .build()?;

            Some(Arc::new(JitoClient {
                endpoints,
                auth_keypair,
                tip_accounts: vec![
                    "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5".parse()?,
                    "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe".parse()?,
                    "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY".parse()?,
                    "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49".parse()?,
                    "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh".parse()?,
                    "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt".parse()?,
                    "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL".parse()?,
                    "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT".parse()?,
                ],
                current_tip_index: AtomicUsize::new(0),
                client,
            }))
        } else {
            None
        };

        let metrics = Arc::new(Metrics {
            sent_count: AtomicU64::new(0),
            confirmed_count: AtomicU64::new(0),
            failed_count: AtomicU64::new(0),
            retry_count: AtomicU64::new(0),
            jito_success_count: AtomicU64::new(0),
            avg_confirmation_ms: AtomicU64::new(0),
            total_confirmation_ms: AtomicU64::new(0),
        });

        let network_monitor = Arc::new(NetworkMonitor {
            rpc_latencies: RwLock::new(VecDeque::with_capacity(100)),
            confirmation_rates: RwLock::new(VecDeque::with_capacity(100)),
            network_quality: RwLock::new(NetworkQuality::Good),
            last_health_check: RwLock::new(Instant::now()),
        });

        let injector = Self {
            rpc_clients,
            tpu_client,
            leader_tracker,
            jito_client,
            metrics,
            send_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_SENDS)),
            network_monitor,
            recent_blockhashes: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_RECENT_BLOCKHASHES))),
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        injector.start_background_tasks();
        Ok(injector)
    }

    pub async fn inject_transaction(
        &self,
        instructions: Vec<Instruction>,
        payer: &Keypair,
        signers: Vec<&Keypair>,
        config: TransactionConfig,
    ) -> Result<Signature> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(anyhow!("Injector is shutting down"));
        }

        let permit = timeout(Duration::from_secs(5), self.send_semaphore.acquire())
            .await
            .context("Timeout acquiring send permit")?
            .context("Semaphore closed")?;

        let start_time = Instant::now();
        let result = self.inject_transaction_internal(instructions, payer, signers, config).await;
        drop(permit);

        match &result {
            Ok(sig) => {
                let duration_ms = start_time.elapsed().as_millis() as u64;
                self.update_confirmation_metrics(duration_ms);
                info!("Transaction confirmed: {} in {}ms", sig, duration_ms);
            }
            Err(e) => {
                self.metrics.failed_count.fetch_add(1, Ordering::Relaxed);
                error!("Transaction failed: {}", e);
            }
        }

        result
    }

    async fn inject_transaction_internal(
        &self,
        instructions: Vec<Instruction>,
        payer: &Keypair,
        signers: Vec<&Keypair>,
        config: TransactionConfig,
    ) -> Result<Signature> {
        let blockhash = self.get_optimal_blockhash().await?;
        let priority_fee = self.calculate_dynamic_priority_fee(config.priority_fee_lamports).await?;

        let mut all_instructions = Vec::with_capacity(instructions.len() + 3);
        all_instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(config.compute_units));
        all_instructions.push(ComputeBudgetInstruction::set_compute_unit_price(priority_fee));

        if config.use_jito && self.jito_client.is_some() {
            all_instructions.push(self.create_jito_tip_instruction()?);
        }

        all_instructions.extend(instructions);

        let message = Message::new_with_blockhash(
            &all_instructions,
            Some(&payer.pubkey()),
            &blockhash,
        );

        let transaction = Transaction::new(&signers, message, blockhash);
        self.validate_transaction(&transaction)?;

        let signature = transaction.signatures[0];
        self.metrics.sent_count.fetch_add(1, Ordering::Relaxed);

        let network_quality = self.network_monitor.get_quality().await;
        let should_use_jito = config.use_jito && 
            self.jito_client.is_some() && 
            matches!(network_quality, NetworkQuality::Fair | NetworkQuality::Poor | NetworkQuality::Critical);

        if should_use_jito {
            match self.send_via_jito(transaction.clone(), &config).await {
                Ok(sig) => return Ok(sig),
                Err(e) => {
                    warn!("Jito submission failed, falling back to TPU: {}", e);
                }
            }
        }

        self.send_via_tpu(transaction, &config).await
    }

        async fn send_via_tpu(&self, transaction: Transaction, config: &TransactionConfig) -> Result<Signature> {
        let signature = transaction.signatures[0];
        let wire_transaction = bincode::serialize(&transaction)?;
        
        let mut retry_count = 0;
        let max_retries = config.max_retries.unwrap_or(MAX_RETRIES);
        let leaders = self.get_strategic_leaders(PACKET_FANOUT).await?;

        loop {
            if self.shutdown.load(Ordering::Acquire) {
                return Err(anyhow!("Shutdown requested"));
            }

            let send_futures: FuturesUnordered<_> = leaders
                .iter()
                .map(|_| {
                    let tpu_client = Arc::clone(&self.tpu_client);
                    let tx_data = wire_transaction.clone();
                    async move {
                        let client = tpu_client.read().await;
                        client.send_wire_transaction(tx_data).await
                    }
                })
                .collect();

            let mut send_results = send_futures;
            while let Some(result) = send_results.next().await {
                if let Err(e) = result {
                    trace!("TPU send error (expected): {}", e);
                }
            }

            let confirmation = self.await_confirmation(&signature, config.require_confirmations).await;
            match confirmation {
                Ok(_) => return Ok(signature),
                Err(e) if retry_count >= max_retries => return Err(e),
                Err(_) => {
                    retry_count += 1;
                    self.metrics.retry_count.fetch_add(1, Ordering::Relaxed);
                    
                    let delay = self.calculate_backoff_delay(retry_count);
                    sleep(delay).await;
                }
            }
        }
    }

    async fn send_via_jito(&self, transaction: Transaction, config: &TransactionConfig) -> Result<Signature> {
        let jito_client = self.jito_client.as_ref().ok_or_else(|| anyhow!("Jito not configured"))?;
        let signature = transaction.signatures[0];
        
        let bundle = vec![base64::encode(bincode::serialize(&transaction)?)];
        let tip_index = jito_client.current_tip_index.fetch_add(1, Ordering::AcqRel);
        let endpoint = &jito_client.endpoints[tip_index % jito_client.endpoints.len()];

        let request_body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [bundle]
        });

        let response = jito_client.client
            .post(format!("{}/api/v1/bundles", endpoint))
            .header("Content-Type", "application/json")
            .json(&request_body)
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow!("Jito error {}: {}", status, error_body));
        }

        self.metrics.jito_success_count.fetch_add(1, Ordering::Relaxed);
        
        self.await_confirmation(&signature, config.require_confirmations).await?;
        Ok(signature)
    }

    async fn await_confirmation(&self, signature: &Signature, required_confirmations: u8) -> Result<()> {
        let start = Instant::now();
        let timeout_duration = Duration::from_millis(CONFIRMATION_TIMEOUT_MS);
        
        while start.elapsed() < timeout_duration {
            match self.check_transaction_status(signature).await {
                Ok(Some(status)) => {
                    match status {
                        TransactionConfirmationStatus::Processed => {
                            if required_confirmations == 0 {
                                return Ok(());
                            }
                        }
                        TransactionConfirmationStatus::Confirmed => {
                            if required_confirmations <= 1 {
                                self.metrics.confirmed_count.fetch_add(1, Ordering::Relaxed);
                                return Ok(());
                            }
                        }
                        TransactionConfirmationStatus::Finalized => {
                            self.metrics.confirmed_count.fetch_add(1, Ordering::Relaxed);
                            return Ok(());
                        }
                    }
                }
                Ok(None) => {
                    sleep(Duration::from_millis(50)).await;
                }
                Err(TransactionError::BlockhashNotFound) => {
                    return Err(anyhow!("Blockhash expired"));
                }
                Err(e) => {
                    return Err(anyhow!("Transaction failed: {:?}", e));
                }
            }
        }
        
        Err(anyhow!("Transaction confirmation timeout"))
    }

    async fn check_transaction_status(&self, signature: &Signature) -> Result<Option<TransactionConfirmationStatus>> {
        let mut last_error = None;
        
        for client in &self.rpc_clients {
            match client.get_signature_status(signature) {
                Ok(Some(Ok(_))) => {
                    if let Ok(statuses) = client.get_signature_statuses(&[*signature]) {
                        if let Some(status) = statuses.value[0].as_ref() {
                            return Ok(Some(status.confirmation_status()));
                        }
                    }
                    return Ok(Some(TransactionConfirmationStatus::Processed));
                }
                Ok(Some(Err(err))) => return Err(err.into()),
                Ok(None) => continue,
                Err(e) => last_error = Some(e),
            }
        }
        
        if let Some(e) = last_error {
            trace!("Status check error: {}", e);
        }
        Ok(None)
    }

    async fn get_optimal_blockhash(&self) -> Result<Hash> {
        let mut blockhashes = self.recent_blockhashes.write().await;
        
        if let Some((hash, slot)) = blockhashes.front() {
            let current_slot = self.leader_tracker.current_slot.load(Ordering::Acquire);
            if current_slot.saturating_sub(*slot) < 75 {
                return Ok(*hash);
            }
        }

        let (hash, slot) = self.fetch_fresh_blockhash().await?;
        blockhashes.push_front((hash, slot));
        if blockhashes.len() > MAX_RECENT_BLOCKHASHES {
            blockhashes.pop_back();
        }
        
        Ok(hash)
    }

    async fn fetch_fresh_blockhash(&self) -> Result<(Hash, u64)> {
        let mut last_error = None;
        
        for client in &self.rpc_clients {
            match client.get_latest_blockhash_with_commitment(CommitmentConfig::finalized()) {
                Ok((blockhash, _)) => {
                    let slot = client.get_slot().unwrap_or(0);
                    return Ok((blockhash, slot));
                }
                Err(e) => last_error = Some(e),
            }
        }
        
        Err(anyhow!("Failed to fetch blockhash: {:?}", last_error))
    }

    async fn calculate_dynamic_priority_fee(&self, base_fee: u64) -> Result<u64> {
        if base_fee > 0 {
            return Ok(base_fee);
        }

        let network_quality = self.network_monitor.get_quality().await;
        let base_multiplier = match network_quality {
            NetworkQuality::Excellent => 1.0,
            NetworkQuality::Good => 1.5,
            NetworkQuality::Fair => 2.0,
            NetworkQuality::Poor => 3.0,
            NetworkQuality::Critical => 5.0,
        };

        let recent_fees = self.sample_recent_priority_fees().await?;
        if recent_fees.is_empty() {
            return Ok((5_000.0 * base_multiplier) as u64);
        }

        let mut sorted_fees = recent_fees;
        sorted_fees.sort_unstable();
        
        let percentile_index = (sorted_fees.len() as f64 * PRIORITY_FEE_PERCENTILE) as usize;
        let percentile_fee = sorted_fees.get(percentile_index.min(sorted_fees.len() - 1))
            .copied()
            .unwrap_or(5_000);

        let adjusted_fee = (percentile_fee as f64 * base_multiplier) as u64;
        Ok(adjusted_fee.clamp(1_000, 1_000_000))
    }

    async fn sample_recent_priority_fees(&self) -> Result<Vec<u64>> {
        let client = &self.rpc_clients[0];
        let current_slot = client.get_slot()?;
        
        let blocks = client.get_blocks_with_limit(
            current_slot.saturating_sub(100),
            10,
        )?;

        let mut fees = Vec::with_capacity(100);
        
        for slot in blocks.iter().rev().take(3) {
            if let Ok(block) = timeout(
                Duration::from_millis(500),
                client.get_block_with_config(*slot, Default::default())
            ).await? {
                for tx in block.transactions.iter().take(50) {
                    if let Some(meta) = &tx.meta {
                        if let (Some(pre), Some(post)) = (&meta.pre_balances, &meta.post_balances) {
                            if !pre.is_empty() && !post.is_empty() {
                                let fee = pre[0].saturating_sub(post[0]);
                                if fee > 5_000 {
                                    fees.push(fee.saturating_sub(5_000));
                                }
                            }
                        }
                    }
                }
            }
        }
        
        Ok(fees)
    }

    async fn get_strategic_leaders(&self, count: usize) -> Result<Vec<Pubkey>> {
        let current_slot = self.leader_tracker.current_slot.load(Ordering::Acquire);
        let schedule = self.leader_tracker.leader_schedule.read().await;
        let stake_weights = self.leader_tracker.stake_weights.read().await;
        
        let mut weighted_leaders = Vec::with_capacity(count);
        let slots_per_leader = 4;
        
        for i in 0..count {
            let target_slot = current_slot + (i as u64 * slots_per_leader);
            if let Some(&leader) = schedule.get(&(target_slot / slots_per_leader)) {
                let weight = stake_weights.get(&leader).copied().unwrap_or(1);
                weighted_leaders.push((leader, weight));
            }
        }

        if weighted_leaders.is_empty() {
            drop(schedule);
            drop(stake_weights);
            self.refresh_leader_schedule().await?;
            return self.get_strategic_leaders(count).await;
        }

        weighted_leaders.sort_by(|a, b| b.1.cmp(&a.1));
        Ok(weighted_leaders.into_iter().map(|(leader, _)| leader).collect())
    }

    async fn refresh_leader_schedule(&self) -> Result<()> {
        let client = &self.rpc_clients[0];
        let slot = client.get_slot()?;
        let epoch_info = client.get_epoch_info()?;
        
        self.leader_tracker.current_slot.store(slot, Ordering::Release);
        
        *self.leader_tracker.epoch_info.write().await = EpochInfo {
            epoch: epoch_info.epoch,
            slot_index: epoch_info.slot_index,
            slots_per_epoch: epoch_info.slots_in_epoch,
            absolute_slot: epoch_info.absolute_slot,
        };

        if let Ok(Some(schedule)) = client.get_leader_schedule_with_config(
            Some(slot),
            CommitmentConfig::confirmed(),
        ) {
            let mut new_schedule = HashMap::new();
            for (pubkey_str, slots) in schedule {
                let pubkey = pubkey_str.parse::<Pubkey>()?;
                for &slot in &slots {
                    new_schedule.insert(slot / 4, pubkey);
                }
            }
            *self.leader_tracker.leader_schedule.write().await = new_schedule;
        }

        if let Ok(vote_accounts) = client.get_vote_accounts() {
            let mut weights = HashMap::new();
            for account in vote_accounts.current.iter().chain(vote_accounts.delinquent.iter()) {
                if let Ok(node_pubkey) = account.node_pubkey.parse::<Pubkey>() {
                    weights.insert(node_pubkey, account.activated_stake);
                }
            }
            *self.leader_tracker.stake_weights.write().await = weights;
        }

        *self.leader_tracker.last_update.write().await = Instant::now();
        Ok(())
    }

    fn create_jito_tip_instruction(&self) -> Result<Instruction> {
        let jito = self.jito_client.as_ref().ok_or_else(|| anyhow!("Jito not initialized"))?;
        let tip_index = jito.current_tip_index.fetch_add(1, Ordering::AcqRel);
        let tip_account = jito.tip_accounts[tip_index % jito.tip_accounts.len()];
        
        Ok(solana_sdk::system_instruction::transfer(
            &jito.auth_keypair.pubkey(),
            &tip_account,
            JITO_TIP_LAMPORTS,
        ))
    }

    fn validate_transaction(&self, transaction: &Transaction) -> Result<()> {
        let size = transaction.message_data().len();
        if size > PACKET_DATA_SIZE {
            return Err(anyhow!("Transaction too large: {} bytes (max: {})", size, PACKET_DATA_SIZE));
        }

        let account_count = transaction.message.account_keys.len();
        if account_count > 64 {
            return Err(anyhow!("Too many accounts: {} (max: 64)", account_count));
        }

        Ok(())
    }

    fn calculate_backoff_delay(&self, attempt: u32) -> Duration {
        let mut rng = rand::thread_rng();
        let base = INITIAL_RETRY_DELAY_MS as f64;
                let exp_delay = base * 1.5_f64.powi(attempt.saturating_sub(1) as i32);
        let capped_delay = exp_delay.min(MAX_RETRY_DELAY_MS as f64);
        
        let jitter = capped_delay * 0.1;
        let jittered = capped_delay + (rng.gen::<f64>() - 0.5) * 2.0 * jitter;
        
        Duration::from_millis(jittered.max(INITIAL_RETRY_DELAY_MS as f64) as u64)
    }

    fn update_confirmation_metrics(&self, duration_ms: u64) {
        self.metrics.confirmed_count.fetch_add(1, Ordering::Relaxed);
        self.metrics.total_confirmation_ms.fetch_add(duration_ms, Ordering::Relaxed);
        
        let total_ms = self.metrics.total_confirmation_ms.load(Ordering::Relaxed);
        let count = self.metrics.confirmed_count.load(Ordering::Relaxed);
        if count > 0 {
            self.metrics.avg_confirmation_ms.store(total_ms / count, Ordering::Relaxed);
        }
    }

    fn start_background_tasks(&self) {
        self.start_leader_schedule_updater();
        self.start_network_health_monitor();
        self.start_blockhash_refresher();
    }

    fn start_leader_schedule_updater(&self) {
        let tracker = Arc::clone(&self.leader_tracker);
        let shutdown = Arc::clone(&self.shutdown);
        let rpc_clients = self.rpc_clients.clone();
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(LEADER_REFRESH_INTERVAL_MS));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            
            while !shutdown.load(Ordering::Acquire) {
                interval.tick().await;
                
                let needs_update = {
                    let last_update = tracker.last_update.read().await;
                    last_update.elapsed() > Duration::from_millis(LEADER_REFRESH_INTERVAL_MS)
                };
                
                if needs_update {
                    let client = &rpc_clients[0];
                    if let Ok(slot) = client.get_slot() {
                        tracker.current_slot.store(slot, Ordering::Release);
                    }
                }
            }
        });
    }

    fn start_network_health_monitor(&self) {
        let monitor = Arc::clone(&self.network_monitor);
        let shutdown = Arc::clone(&self.shutdown);
        let metrics = Arc::clone(&self.metrics);
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(HEALTH_CHECK_INTERVAL_MS));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            
            while !shutdown.load(Ordering::Acquire) {
                interval.tick().await;
                
                let confirmed = metrics.confirmed_count.load(Ordering::Relaxed) as f64;
                let sent = metrics.sent_count.load(Ordering::Relaxed) as f64;
                
                if sent > 0.0 {
                    let rate = confirmed / sent;
                    monitor.record_confirmation_rate(rate).await;
                }
                
                monitor.update_network_quality().await;
            }
        });
    }

    fn start_blockhash_refresher(&self) {
        let blockhashes = Arc::clone(&self.recent_blockhashes);
        let shutdown = Arc::clone(&self.shutdown);
        let rpc_clients = self.rpc_clients.clone();
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(5000));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            
            while !shutdown.load(Ordering::Acquire) {
                interval.tick().await;
                
                let mut hashes = blockhashes.write().await;
                hashes.retain(|(_, slot)| {
                    if let Ok(current) = rpc_clients[0].get_slot() {
                        current.saturating_sub(*slot) < 150
                    } else {
                        true
                    }
                });
            }
        });
    }

    pub async fn inject_bundle(
        &self,
        transactions: Vec<(Vec<Instruction>, &Keypair, Vec<&Keypair>)>,
        config: TransactionConfig,
    ) -> Result<Vec<Signature>> {
        if transactions.is_empty() || transactions.len() > MAX_BUNDLE_SIZE {
            return Err(anyhow!("Invalid bundle size: {} (must be 1-{})", 
                transactions.len(), MAX_BUNDLE_SIZE));
        }

        let blockhash = self.get_optimal_blockhash().await?;
        let priority_fee = self.calculate_dynamic_priority_fee(config.priority_fee_lamports).await?;
        
        let mut built_transactions = Vec::with_capacity(transactions.len());
        let mut signatures = Vec::with_capacity(transactions.len());
        
        for (instructions, payer, signers) in transactions {
            let mut all_instructions = Vec::with_capacity(instructions.len() + 3);
            all_instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(config.compute_units));
            all_instructions.push(ComputeBudgetInstruction::set_compute_unit_price(priority_fee));
            
            if config.use_jito && self.jito_client.is_some() && built_transactions.is_empty() {
                all_instructions.push(self.create_jito_tip_instruction()?);
            }
            
            all_instructions.extend(instructions);
            
            let message = Message::new_with_blockhash(
                &all_instructions,
                Some(&payer.pubkey()),
                &blockhash,
            );
            
            let transaction = Transaction::new(&signers, message, blockhash);
            self.validate_transaction(&transaction)?;
            
            signatures.push(transaction.signatures[0]);
            built_transactions.push(transaction);
        }

        if config.use_jito && self.jito_client.is_some() {
            match self.send_bundle_jito(built_transactions.clone()).await {
                Ok(()) => {
                    for sig in &signatures {
                        self.await_confirmation(sig, config.require_confirmations).await?;
                    }
                    return Ok(signatures);
                }
                Err(e) => {
                    warn!("Jito bundle failed, sending individually: {}", e);
                }
            }
        }

        let mut send_futures = Vec::with_capacity(built_transactions.len());
        for tx in built_transactions {
            send_futures.push(self.send_via_tpu(tx, &config));
        }
        
        let results = join_all(send_futures).await;
        let mut confirmed_sigs = Vec::new();
        
        for (i, result) in results.into_iter().enumerate() {
            match result {
                Ok(sig) => confirmed_sigs.push(sig),
                Err(e) => {
                    error!("Transaction {} in bundle failed: {}", i, e);
                    return Err(e);
                }
            }
        }
        
        Ok(confirmed_sigs)
    }

    async fn send_bundle_jito(&self, transactions: Vec<Transaction>) -> Result<()> {
        let jito = self.jito_client.as_ref().ok_or_else(|| anyhow!("Jito not configured"))?;
        
        let bundle: Vec<String> = transactions
            .iter()
            .map(|tx| base64::encode(bincode::serialize(tx).unwrap()))
            .collect();
        
        let tip_index = jito.current_tip_index.fetch_add(1, Ordering::AcqRel);
        let endpoint = &jito.endpoints[tip_index % jito.endpoints.len()];
        
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [bundle]
        });
        
        let response = jito.client
            .post(format!("{}/api/v1/bundles", endpoint))
            .header("Content-Type", "application/json")
            .json(&request)
            .send()
            .await?;
        
        if !response.status().is_success() {
            let error_text = response.text().await.unwrap_or_default();
            return Err(anyhow!("Jito bundle error: {}", error_text));
        }
        
        Ok(())
    }

    pub async fn get_metrics(&self) -> HashMap<String, u64> {
        let mut metrics = HashMap::new();
        
        metrics.insert("sent".to_string(), self.metrics.sent_count.load(Ordering::Relaxed));
        metrics.insert("confirmed".to_string(), self.metrics.confirmed_count.load(Ordering::Relaxed));
        metrics.insert("failed".to_string(), self.metrics.failed_count.load(Ordering::Relaxed));
        metrics.insert("retries".to_string(), self.metrics.retry_count.load(Ordering::Relaxed));
        metrics.insert("jito_success".to_string(), self.metrics.jito_success_count.load(Ordering::Relaxed));
        metrics.insert("avg_confirmation_ms".to_string(), self.metrics.avg_confirmation_ms.load(Ordering::Relaxed));
        
        let current_slot = self.leader_tracker.current_slot.load(Ordering::Acquire);
        metrics.insert("current_slot".to_string(), current_slot);
        
        let epoch_info = self.leader_tracker.epoch_info.read().await;
        metrics.insert("epoch".to_string(), epoch_info.epoch);
        metrics.insert("slot_index".to_string(), epoch_info.slot_index);
        
        metrics
    }

    pub async fn shutdown(&self) {
        info!("Shutting down mempool-less injector");
        self.shutdown.store(true, Ordering::Release);
        
        let timeout_duration = Duration::from_secs(10);
        let start = Instant::now();
        
        while self.send_semaphore.available_permits() < MAX_CONCURRENT_SENDS {
            if start.elapsed() > timeout_duration {
                warn!("Shutdown timeout - some transactions may still be in flight");
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
        
        info!("Mempool-less injector shutdown complete");
    }
}

impl NetworkMonitor {
    async fn record_rpc_latency(&self, latency: Duration) {
        let mut latencies = self.rpc_latencies.write().await;
        if latencies.len() >= 100 {
            latencies.pop_front();
        }
        latencies.push_back(latency);
    }

    async fn record_confirmation_rate(&self, rate: f64) {
        let mut rates = self.confirmation_rates.write().await;
        if rates.len() >= 100 {
            rates.pop_front();
        }
        rates.push_back(rate);
    }

    async fn update_network_quality(&self) {
        let latencies = self.rpc_latencies.read().await;
        let rates = self.confirmation_rates.read().await;
        
        if latencies.is_empty() || rates.is_empty() {
            return;
        }
        
        let avg_latency = latencies.iter().sum::<Duration>() / latencies.len() as u32;
        let avg_rate = rates.iter().sum::<f64>() / rates.len() as f64;
        
        let quality = if avg_latency < Duration::from_millis(50) && avg_rate > 0.95 {
            NetworkQuality::Excellent
        } else if avg_latency < Duration::from_millis(150) && avg_rate > 0.85 {
            NetworkQuality::Good
        } else if avg_latency < Duration::from_millis(300) && avg_rate > 0.70 {
            NetworkQuality::Fair
        } else if avg_latency < Duration::from_millis(500) && avg_rate > 0.50 {
            NetworkQuality::Poor
        } else {
            NetworkQuality::Critical
        };
        
        *self.network_quality.write().await = quality;
        *self.last_health_check.write().await = Instant::now();
    }

    async fn get_quality(&self) -> NetworkQuality {
        *self.network_quality.read().await
    }
}

impl Clone for MempoolLessInjector {
    fn clone(&self) -> Self {
        Self {
            rpc_clients: self.rpc_clients.clone(),
            tpu_client: Arc::clone(&self.tpu_client),
            leader_tracker: Arc::clone(&self.leader_tracker),
            jito_client: self.jito_client.clone(),
            metrics: Arc::clone(&self.metrics),
            send_semaphore: Arc::clone(&self.send_semaphore),
            network_monitor: Arc::clone(&self.network_monitor),
            recent_blockhashes: Arc::clone(&self.recent_blockhashes),
            shutdown: Arc::clone(&self.shutdown),
        }
    }
}

pub struct TransactionBuilder {
    instructions: Vec<Instruction>,
    compute_units: Option<u32>,
    priority_fee: Option<u64>,
}

impl TransactionBuilder {
    pub fn new() -> Self {
        Self {
            instructions: Vec::new(),
            compute_units: None,
            priority_fee: None,
        }
    }

    pub fn add_instruction(mut self, instruction: Instruction) -> Self {
        self.instructions.push(instruction);
        self
    }

    pub fn with_compute_units(mut self, units: u32) -> Self {
        self.compute_units = Some(units.min(MAX_COMPUTE_UNITS));
        self
    }

    pub fn with_priority_fee(mut self, lamports: u64) -> Self {
        self.priority_fee = Some(lamports);
        self
    }

    pub async fn build_and_send(
        self,
        injector: &MempoolLessInjector,
        payer: &Keypair,
        signers: Vec<&Keypair>,
    ) -> Result<Signature> {
        let config = TransactionConfig {
            priority_fee_lamports: self.priority_fee.unwrap_or(0),
            compute_units: self.compute_units.unwrap_or(200_000),
            use_jito: true,
                        skip_preflight: true,
            max_retries: Some(MAX_RETRIES),
            require_confirmations: 1,
        };
        
        injector.inject_transaction(self.instructions, payer, signers, config).await
    }
}

impl Drop for MempoolLessInjector {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

#[inline(always)]
pub fn estimate_transaction_compute_units(instructions: &[Instruction]) -> u32 {
    const BASE_UNITS: u32 = 1_400;
    const PER_INSTRUCTION: u32 = 12_000;
    const PER_ACCOUNT: u32 = 3_000;
    const PER_SIGNER: u32 = 1_000;
    const PER_DATA_BYTE: u32 = 10;
    
    let mut unique_accounts = std::collections::HashSet::new();
    let mut total_data_size = 0;
    let mut signer_count = 0;
    
    for ix in instructions {
        unique_accounts.insert(ix.program_id);
        for account in &ix.accounts {
            unique_accounts.insert(account.pubkey);
            if account.is_signer {
                signer_count += 1;
            }
        }
        total_data_size += ix.data.len();
    }
    
    let estimated = BASE_UNITS
        + (instructions.len() as u32 * PER_INSTRUCTION)
        + (unique_accounts.len() as u32 * PER_ACCOUNT)
        + (signer_count * PER_SIGNER)
        + (total_data_size as u32 * PER_DATA_BYTE);
    
    estimated.min(MAX_COMPUTE_UNITS)
}

pub struct OptimizedBundleBuilder {
    transactions: Vec<(Vec<Instruction>, u32, u64)>,
    total_compute_units: u32,
    max_bundle_compute: u32,
}

impl OptimizedBundleBuilder {
    pub fn new() -> Self {
        Self {
            transactions: Vec::new(),
            total_compute_units: 0,
            max_bundle_compute: MAX_COMPUTE_UNITS * 4,
        }
    }
    
    pub fn add_transaction(
        mut self, 
        instructions: Vec<Instruction>,
        compute_units: Option<u32>,
        priority_fee: u64,
    ) -> Result<Self> {
        if self.transactions.len() >= MAX_BUNDLE_SIZE {
            return Err(anyhow!("Bundle full: max {} transactions", MAX_BUNDLE_SIZE));
        }
        
        let units = compute_units.unwrap_or_else(|| estimate_transaction_compute_units(&instructions));
        
        if self.total_compute_units + units > self.max_bundle_compute {
            return Err(anyhow!("Bundle compute limit exceeded"));
        }
        
        self.total_compute_units += units;
        self.transactions.push((instructions, units, priority_fee));
        Ok(self)
    }
    
    pub fn can_add_transaction(&self, compute_units: u32) -> bool {
        self.transactions.len() < MAX_BUNDLE_SIZE &&
        self.total_compute_units + compute_units <= self.max_bundle_compute
    }
    
    pub fn build(self) -> Vec<(Vec<Instruction>, u32, u64)> {
        self.transactions
    }
}

pub async fn wait_for_confirmation_with_timeout(
    injector: &MempoolLessInjector,
    signature: &Signature,
    timeout_ms: u64,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    
    while Instant::now() < deadline {
        match injector.check_transaction_status(signature).await? {
            Some(TransactionConfirmationStatus::Confirmed) |
            Some(TransactionConfirmationStatus::Finalized) => return Ok(()),
            Some(TransactionConfirmationStatus::Processed) => {
                sleep(Duration::from_millis(25)).await;
            }
            None => {
                sleep(Duration::from_millis(50)).await;
            }
        }
    }
    
    Err(anyhow!("Confirmation timeout after {}ms", timeout_ms))
}

pub struct SmartRetryStrategy {
    max_attempts: u32,
    base_delay_ms: u64,
    max_delay_ms: u64,
    use_jitter: bool,
}

impl Default for SmartRetryStrategy {
    fn default() -> Self {
        Self {
            max_attempts: MAX_RETRIES,
            base_delay_ms: INITIAL_RETRY_DELAY_MS,
            max_delay_ms: MAX_RETRY_DELAY_MS,
            use_jitter: true,
        }
    }
}

impl SmartRetryStrategy {
    pub fn aggressive() -> Self {
        Self {
            max_attempts: 10,
            base_delay_ms: 10,
            max_delay_ms: 500,
            use_jitter: true,
        }
    }
    
    pub fn conservative() -> Self {
        Self {
            max_attempts: 3,
            base_delay_ms: 100,
            max_delay_ms: 2000,
            use_jitter: false,
        }
    }
    
    pub fn calculate_delay(&self, attempt: u32) -> Duration {
        let exponential = self.base_delay_ms as f64 * (1.5_f64).powi(attempt as i32);
        let capped = exponential.min(self.max_delay_ms as f64);
        
        let final_delay = if self.use_jitter {
            let mut rng = rand::thread_rng();
            let jitter = capped * 0.2;
            capped + (rng.gen::<f64>() - 0.5) * jitter
        } else {
            capped
        };
        
        Duration::from_millis(final_delay as u64)
    }
}

pub struct ConnectionHealthChecker {
    last_check: Mutex<Instant>,
    healthy_endpoints: Arc<DashMap<String, bool>>,
}

impl ConnectionHealthChecker {
    pub fn new() -> Self {
        Self {
            last_check: Mutex::new(Instant::now()),
            healthy_endpoints: Arc::new(DashMap::new()),
        }
    }
    
    pub async fn check_endpoint(&self, endpoint: &str) -> bool {
        if let Some(healthy) = self.healthy_endpoints.get(endpoint) {
            if self.last_check.lock().await.elapsed() < Duration::from_secs(30) {
                return *healthy;
            }
        }
        
        let client = match RpcClient::new_with_timeout(endpoint.to_string(), Duration::from_secs(5)) {
            Ok(c) => c,
            Err(_) => {
                self.healthy_endpoints.insert(endpoint.to_string(), false);
                return false;
            }
        };
        
        let healthy = client.get_health().is_ok();
        self.healthy_endpoints.insert(endpoint.to_string(), healthy);
        *self.last_check.lock().await = Instant::now();
        
        healthy
    }
    
    pub async fn get_healthy_endpoints(&self, endpoints: &[String]) -> Vec<String> {
        let mut healthy = Vec::new();
        
        for endpoint in endpoints {
            if self.check_endpoint(endpoint).await {
                healthy.push(endpoint.clone());
            }
        }
        
        if healthy.is_empty() && !endpoints.is_empty() {
            healthy.push(endpoints[0].clone());
        }
        
        healthy
    }
}

#[inline(always)]
pub fn calculate_optimal_packet_fanout(network_quality: NetworkQuality, is_high_value: bool) -> usize {
    match (network_quality, is_high_value) {
        (NetworkQuality::Excellent, false) => 20,
        (NetworkQuality::Excellent, true) => 40,
        (NetworkQuality::Good, false) => 30,
        (NetworkQuality::Good, true) => 50,
        (NetworkQuality::Fair, _) => 60,
        (NetworkQuality::Poor, _) => 80,
        (NetworkQuality::Critical, _) => 100,
    }
}

pub fn create_priority_fee_instruction(microlamports_per_cu: u64) -> Instruction {
    ComputeBudgetInstruction::set_compute_unit_price(microlamports_per_cu)
}

pub fn create_compute_limit_instruction(units: u32) -> Instruction {
    ComputeBudgetInstruction::set_compute_unit_limit(units.min(MAX_COMPUTE_UNITS))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_compute_unit_estimation() {
        let transfer = solana_sdk::system_instruction::transfer(
            &Keypair::new().pubkey(),
            &Keypair::new().pubkey(),
            1_000_000,
        );
        
        let estimated = estimate_transaction_compute_units(&[transfer]);
        assert!(estimated > 0 && estimated < MAX_COMPUTE_UNITS);
    }
    
    #[test]
    fn test_retry_strategy() {
        let strategy = SmartRetryStrategy::default();
        
        let delay1 = strategy.calculate_delay(1);
        let delay2 = strategy.calculate_delay(2);
        let delay3 = strategy.calculate_delay(3);
        
        assert!(delay1 < delay2);
        assert!(delay2 < delay3);
        assert!(delay3.as_millis() <= MAX_RETRY_DELAY_MS as u128);
    }
}

