use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use futures::{future::join_all, stream::FuturesUnordered, StreamExt};
use bincode;
use log::{debug, error, info, trace, warn};
use rand::Rng;
use solana_client::{
    nonblocking::tpu_client::TpuClient,
    rpc_client::RpcClient,
    rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig, RpcBlockConfig},
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
    system_instruction,
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

// (removed early duplicate of calculate_optimal_packet_fanout; see unified version at bottom)

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
        mut config: TransactionConfig,
    ) -> Result<Signature> {
        // === [ULTRA: PIECE C] Hedged Jito after small budget + blockhash auto-rebuild ===
        let mut build_tx = |priority_fee_micro_per_cu: u64, blockhash: Hash| -> Transaction {
            let mut all_ix = Vec::with_capacity(instructions.len() + 3);
            all_ix.push(ComputeBudgetInstruction::set_compute_unit_limit(config.compute_units));
            all_ix.push(ComputeBudgetInstruction::set_compute_unit_price(priority_fee_micro_per_cu));
            if config.use_jito && self.jito_client.is_some() {
                if let Ok(tip_ix) = self.create_jito_tip_instruction_from(&payer.pubkey()) {
                    all_ix.push(tip_ix);
                }
            }
            all_ix.extend(instructions.clone());
            let msg = Message::new_with_blockhash(&all_ix, Some(&payer.pubkey()), &blockhash);
            Transaction::new(&signers, msg, blockhash)
        };

        let mut blockhash = self.get_optimal_blockhash().await?;
        let mut cu_price = self.calculate_dynamic_priority_fee(config.priority_fee_lamports).await?; // micro-lamports/CU
        let mut tx = build_tx(cu_price, blockhash);

        self.validate_transaction(&tx)?;
        let signature = tx.signatures[0];
        self.metrics.sent_count.fetch_add(1, Ordering::Relaxed);

        let netq = self.network_monitor.get_quality().await;
        let jito_ok = config.use_jito && self.jito_client.is_some();

        // Start TPU immediately
        let tpu_send = self.send_via_tpu(tx.clone(), &config);

        // Hedge to Jito if not observed quickly under congestion
        let hedged = async {
            let budget_ms = self.network_monitor.dynamic_hedge_budget_ms(netq).await;
            match timeout(Duration::from_millis(budget_ms), self.await_confirmation(&signature, 0)).await {
                Ok(Ok(_)) => Ok(signature),
                _ if jito_ok => self.send_via_jito_multi(tx.clone(), &config).await,
                _ => Err(anyhow!("no_hedge_path")),
            }
        };

        match futures::future::select(Box::pin(tpu_send), Box::pin(hedged)).await {
            futures::future::Either::Left((Ok(sig), _))  => Ok(sig),
            futures::future::Either::Right((Ok(sig), _)) => Ok(sig),
            _ => {
                match self.send_via_tpu(tx.clone(), &config).await {
                    Ok(sig) => Ok(sig),
                    Err(e) => {
                        let s = e.to_string();
                        if s.contains("Blockhash expired") || s.contains("expired blockhash") {
                            blockhash = self.get_optimal_blockhash().await?;
                            // escalate ~18% for next attempt
                            cu_price  = ((cu_price as f64) * 1.18).round() as u64;
                            cu_price  = cu_price.clamp(100, 1_000_000);
                            tx = build_tx(cu_price, blockhash);
                            self.validate_transaction(&tx)?;
                            self.metrics.sent_count.fetch_add(1, Ordering::Relaxed);
                            self.send_via_tpu(tx, &config).await
                        } else {
                            Err(e)
                        }
                    }
        }
    };

    loop {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(anyhow!("Shutdown requested"));
        }

        let esc = if matches!(netq, NetworkQuality::Fair | NetworkQuality::Poor | NetworkQuality::Critical) {
            (attempt / 2) as usize
        } else { 0 };

        let primary_sends   = base_primary.saturating_add(esc).min(24);
        let secondary_sends = base_secondary.saturating_add(esc / 2).min(12);
        let echo_sends      = base_echo.min(4);

        // Phase A
        burst(primary_sends, Arc::clone(&wire), Arc::clone(&self.tpu_client)).await;

        // Seeded pause + early short-circuit
        sleep(Duration::from_millis(seed_ms)).await;
        if let Ok(Some(st)) = self.check_transaction_status(&signature).await {
            if matches!(st, TransactionConfirmationStatus::Processed) {
                // already in cluster — skip reinforcement
            }
        } else {
            // Phase B
            burst(secondary_sends, Arc::clone(&wire), Arc::clone(&self.tpu_client)).await;
            // Echo only if still unseen

            // Conditional echo if still unseen
            if let Ok(None) = self.check_transaction_status(&signature).await {
                if echo_sends > 0 {
                    sleep(Duration::from_millis(2 + seed_ms % 4)).await;
                    burst(echo_sends, Arc::clone(&wire), Arc::clone(&self.tpu_client)).await;
                }
            }
        }

        match self.await_confirmation(&signature, config.require_confirmations).await {
            Ok(_) => return Ok(signature),
            Err(e) if attempt >= max_retries => return Err(e),
            Err(_) => {
                attempt = attempt.saturating_add(1);
                self.metrics.retry_count.fetch_add(1, Ordering::Relaxed);
                sleep(self.calculate_backoff_delay_seeded(signature, attempt)).await;
            }
        }
    }

    // === [ULTRA+++*: PIECE D] Front-loaded + regime-aware confirmation pacing (tightened) === 
    async fn await_confirmation(&self, signature: &Signature, required_confirmations: u8) -> Result<()> {
    let start = Instant::now();
    let timeout = Duration::from_millis(CONFIRMATION_TIMEOUT_MS);
    let netq = self.network_monitor.get_quality().await;

    while start.elapsed() < timeout {
        match self.check_transaction_status(signature).await {
            Ok(Some(TransactionConfirmationStatus::Finalized)) => {
                self.metrics.confirmed_count.fetch_add(1, Ordering::Relaxed);
                return Ok(());
            }
            Ok(Some(TransactionConfirmationStatus::Confirmed)) => {
                if required_confirmations <= 1 {
                    self.metrics.confirmed_count.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
            }
            Ok(Some(TransactionConfirmationStatus::Processed)) => {
                if required_confirmations == 0 { return Ok(()); }
            }
            Ok(None) => { /* continue polling */ }
            Err(TransactionError::BlockhashNotFound) => {
                return Err(anyhow!("Blockhash expired after {}ms", start.elapsed().as_millis()));
            }
            Err(e) => {
                return Err(anyhow!("Transaction failed after {}ms: {:?}", start.elapsed().as_millis(), e));
            }
        }

        // regime-aware front-load then taper
        let e = start.elapsed().as_millis() as u64;
        let (s1, s2, s3, s4, s5) = match netq {
            NetworkQuality::Excellent => (6,  10, 14, 22, 36),
            NetworkQuality::Good      => (8,  12, 16, 24, 40),
            NetworkQuality::Fair      => (10, 14, 18, 26, 44),
            NetworkQuality::Poor      => (12, 16, 20, 28, 48),
            NetworkQuality::Critical  => (14, 18, 22, 30, 52),
        };
        let sleep_ms = if e < 200 { s1 } else if e < 350 { s2 } else if e < 500 { s3 }
                       else if e < 2_000 { s4 } else { s5 };
        sleep(Duration::from_millis(sleep_ms)).await;
    }

    Err(anyhow!("Transaction confirmation timeout after {}ms", timeout.as_millis()))
}

// === [ULTRA+++*: PIECE E] CU price (micro-lamports/CU) estimator (trim + safety rounding) ===
async fn calculate_dynamic_priority_fee(&self, base_fee_override_micro_per_cu: u64) -> Result<u64> {
    if base_fee_override_micro_per_cu > 0 {
        return Ok(base_fee_override_micro_per_cu.clamp(100, 1_000_000));
    }

    let netq = self.network_monitor.get_quality().await;
    let mult = match netq {
        NetworkQuality::Excellent => 1.00,
        NetworkQuality::Good      => 1.25,
        NetworkQuality::Fair      => 1.60,
        NetworkQuality::Poor      => 2.30,
        NetworkQuality::Critical  => 3.50,
    };

    let mut cu_prices_micro = self.sample_recent_priority_fees().await?;
    if cu_prices_micro.is_empty() {
        let cold = (5_000.0 * mult).round() as u64; // conservative baseline × regime
        return Ok(cold.clamp(100, 1_000_000));
    }

    cu_prices_micro.sort_unstable();
    let n = cu_prices_micro.len();
    let lo = n / 10;
    let hi = n.saturating_sub(n / 10).max(lo + 1);
    let trimmed = &cu_prices_micro[lo..hi];

    let pct = |p: f64| -> u64 {
        let idx = ((trimmed.len() as f64 - 1.0) * p).round() as usize;
        trimmed[idx]
    };

    let p70 = pct(0.70);
    let p85 = pct(0.85);
    let p95 = pct(0.95);

    let blend = match netq {
        NetworkQuality::Excellent | NetworkQuality::Good => (p70 as f64) * 0.7 + (p85 as f64) * 0.3,
        NetworkQuality::Fair                              => (p85 as f64) * 0.6 + (p95 as f64) * 0.4,
        NetworkQuality::Poor | NetworkQuality::Critical   => (p85 as f64) * 0.3 + (p95 as f64) * 0.7,
    };

    let mut adjusted = (blend * mult).round() as u64;
    let step = 25u64; // 25 µ-lamports/CU step
    adjusted = ((adjusted + step - 1) / step) * step; // safety rounding up

    Ok(adjusted.clamp(100, 1_000_000))
}

// === [ULTRA+++: PIECE F] Sample recent CU prices (µ-lamports/CU) with tight RPC policy ===
async fn sample_recent_priority_fees(&self) -> Result<Vec<u64>> {
    let client = &self.rpc_clients[0];

    // Fast path: direct RPC percentile fees when supported
    if let Ok(fees) = client.get_recent_prioritization_fees(None) {
        let mut out = Vec::with_capacity(fees.len());
        for f in fees.iter().take(64) {
            if f.prioritization_fee > 0 {
                out.push(f.prioritization_fee as u64);
            }
        }
        if !out.is_empty() {
            return Ok(out);
        }
    }

    let t0 = Instant::now();
    let current_slot = client.get_slot()?;
    self.network_monitor.record_rpc_latency(t0.elapsed()).await;

    // walk last ~120 slots, read at most 12 blocks, process 6 newest
    let blocks = client.get_blocks_with_limit(current_slot.saturating_sub(120), 12)?;
    let mut prices_micro_per_cu = Vec::with_capacity(192);

    let blk_cfg = solana_client::rpc_config::RpcBlockConfig {
        encoding: Some(UiTransactionEncoding::Json),
        transaction_details: Some(solana_client::rpc_config::UiTransactionDetailType::Full),
        rewards: Some(false),
        commitment: Some(CommitmentConfig::confirmed()),
        max_supported_transaction_version: Some(0),
    };

    for (i, slot) in blocks.iter().rev().take(6).enumerate() {
        let start = Instant::now();
        let per_block_timeout = Duration::from_millis(if i <= 1 { 380 } else { 300 });

        if let Ok(Ok(block)) = timeout(per_block_timeout, async {
            tokio::task::spawn_blocking({
                let c = client.clone();
                let s = *slot;
                let cfg = blk_cfg.clone();
                move || c.get_block_with_config(s, cfg)
            }).await
        }).await {
            self.network_monitor.record_rpc_latency(start.elapsed()).await;

            for tx in block.transactions.iter().take(96) {
                if let Some(meta) = &tx.meta {
                    if let (Some(cu), fee) = (meta.compute_units_consumed, meta.fee) {
                        if cu > 0 && fee > 5_000 {
                            let priority_lamports = fee.saturating_sub(5_000);
                            let micro_per_cu = (priority_lamports.saturating_mul(1_000_000)) / cu;
                            if (50..=1_000_000).contains(&micro_per_cu) {
                                prices_micro_per_cu.push(micro_per_cu);
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(prices_micro_per_cu)
}
        let current_slot = self.leader_tracker.current_slot.load(Ordering::Acquire);
        let schedule = self.leader_tracker.leader_schedule.read().await;
        let stakes   = self.leader_tracker.stake_weights.read().await;

        // Horizon sized to find strong, unique leaders without over-scanning.
        let window = (count * 6).max(count + 8) as u64;
        let mut weight_map: HashMap<Pubkey, u128> = HashMap::new();

        for s in current_slot..current_slot + window {
            if let Some(&leader) = schedule.get(&s) {
                let stake   = *stakes.get(&leader).unwrap_or(&1) as u128;
                let dist    = s.saturating_sub(current_slot) as u128;
                let recency = 1_000u128.saturating_sub(dist.min(999)); // nearer slots weighted more
                *weight_map.entry(leader).or_insert(0) = weight_map
                    .get(&leader).copied().unwrap_or(0)
                    .saturating_add(stake.saturating_mul(recency));
            }
        }

        if weight_map.is_empty() {
            drop(schedule); drop(stakes);
            self.refresh_leader_schedule().await?;
            return self.get_strategic_leaders(count).await;
        }

        // Stable tiebreaker via xorshift* on (pubkey||current_slot).
        let mut ranked: Vec<(Pubkey, u128, u64)> = weight_map.into_iter().map(|(k, w)| {
            let mut seed = {
                let mut b = [0u8; 8];
                b.copy_from_slice(&k.to_bytes()[..8]);
                u64::from_le_bytes(b) ^ (current_slot as u64)
            };
            seed ^= seed >> 12; seed ^= seed << 25; seed ^= seed >> 27;
            let tb = seed.wrapping_mul(0x2545F4914F6CDD1D);
            (k, w, tb)
        }).collect();

        ranked.sort_unstable_by(|a, b| match b.1.cmp(&a.1) {
            CmpOrd::Equal => b.2.cmp(&a.2),
            o => o,
        });

        Ok(ranked.into_iter().take(count).map(|(k, _, _)| k).collect())
    }

    // === [ULTRA++++: PIECE M] Refresh leader schedule (current + next epoch) ===
    async fn refresh_leader_schedule(&self) -> Result<()> {
        let client = &self.rpc_clients[0];
        let epoch_info = client.get_epoch_info()?;
        let slot = client.get_slot()?;

        self.leader_tracker.current_slot.store(slot, Ordering::Release);
        *self.leader_tracker.epoch_info.write().await = EpochInfo {
            epoch: epoch_info.epoch,
            slot_index: epoch_info.slot_index,
            slots_per_epoch: epoch_info.slots_in_epoch,
            absolute_slot: epoch_info.absolute_slot,
        };

        let epoch_start_abs = epoch_info.absolute_slot.saturating_sub(epoch_info.slot_index);
        let next_epoch_start_abs = epoch_start_abs.saturating_add(epoch_info.slots_in_epoch);

        let mut new_schedule = HashMap::with_capacity(epoch_info.slots_in_epoch as usize * 2 + 16);

        // Helper to fetch and merge one epoch’s schedule
        let mut merge_epoch = |epoch_abs_start: u64| -> Result<()> {
            if let Ok(Some(schedule)) = client.get_leader_schedule_with_config(
                Some(epoch_abs_start),
                CommitmentConfig::confirmed(),
            ) {
                for (pubkey_str, slots) in schedule {
                    let leader = pubkey_str.parse::<Pubkey>()?;
                    for &slot_idx in &slots {
                        let abs = epoch_abs_start + slot_idx as u64;
                        new_schedule.insert(abs, leader);
                    }
                }
            }
            Ok(())
        };

        merge_epoch(epoch_start_abs)?;
        // Look-ahead to avoid epoch rollover gap
        merge_epoch(next_epoch_start_abs)?;

        *self.leader_tracker.leader_schedule.write().await = new_schedule;

        if let Ok(vote_accounts) = client.get_vote_accounts() {
            let mut weights = HashMap::new();
            for acct in vote_accounts.current.iter().chain(vote_accounts.delinquent.iter()) {
                if let Ok(node) = acct.node_pubkey.parse::<Pubkey>() {
                    weights.insert(node, acct.activated_stake);
                }
            }
            *self.leader_tracker.stake_weights.write().await = weights;
        }

        *self.leader_tracker.last_update.write().await = Instant::now();
        Ok(())
    }

    // create_jito_tip_instruction (legacy) removed from call sites; using payer-aware version below

    // === [ULTRA+++ PIECE H] Payer-aware Jito tip (slot-aware rotation) ===
    fn create_jito_tip_instruction_from(&self, payer: &Pubkey) -> Result<Instruction> {
        let jito = self.jito_client.as_ref().ok_or_else(|| anyhow!("Jito not configured"))?;
        let slot = self.leader_tracker.current_slot.load(Ordering::Acquire) as usize;
        let idx_incr = jito.current_tip_index.fetch_add(1, Ordering::AcqRel);
        let tip_idx = (slot.wrapping_add(idx_incr)) % jito.tip_accounts.len();
        let tip_account = jito.tip_accounts[tip_idx];
        Ok(solana_sdk::system_instruction::transfer(payer, &tip_account, JITO_TIP_LAMPORTS))
    }

    // === [ULTRA+++ PIECE I] Jito multi-endpoint hedged sender ===
    async fn send_via_jito_multi(&self, transaction: Transaction, config: &TransactionConfig) -> Result<Signature> {
        let jito = self.jito_client.as_ref().ok_or_else(|| anyhow!("Jito not configured"))?;
        let signature = transaction.signatures[0];
        let wire_b64 = base64::encode(bincode::serialize(&transaction)?);

        let tip = jito.current_tip_index.fetch_add(1, Ordering::AcqRel);
        let n = jito.endpoints.len();
        if n == 0 { return Err(anyhow!("No Jito endpoints")); }
        let e1 = &jito.endpoints[tip % n];
        let e2 = &jito.endpoints[(tip + 1) % n];

        let body = serde_json::json!({
            "jsonrpc": "2.0", "id": 1, "method": "sendBundle", "params": [[wire_b64]]
        });

        let client = jito.client.clone();
        let send_req = |endpoint: &str| {
            let c = client.clone();
            let b = body.clone();
            async move {
                let resp = tokio::time::timeout(Duration::from_millis(JITO_BUNDLE_TIMEOUT_MS), async {
                    c.post(format!("{}/api/v1/bundles", endpoint))
                        .header("Content-Type", "application/json")
                        .json(&b)
                        .send()
                        .await
                }).await.map_err(|_| anyhow!("jito_timeout"))??;

                if !resp.status().is_success() {
                    let status = resp.status();
                    let text = resp.text().await.unwrap_or_default();
                    return Err(anyhow!("Jito {}: {}", status, text));
                }
                Ok::<(), anyhow::Error>(())
            }
        };

        use futures::{FutureExt, future::Either};
        let r = futures::future::select(send_req(e1).boxed(), send_req(e2).boxed()).await;
        match r {
            Either::Left((Ok(_), _)) | Either::Right((Ok(_), _)) => {
                self.await_confirmation(&signature, config.require_confirmations).await?;
                Ok(signature)
            }
            Either::Left((Err(e1e), fut2)) => match fut2.await {
                Ok(_) => { self.await_confirmation(&signature, config.require_confirmations).await?; Ok(signature) }
                Err(e2e) => Err(anyhow!("Jito failed on both: {}, {}", e1e, e2e)),
            },
            Either::Right((Err(e2e), fut1)) => match fut1.await {
                Ok(_) => { self.await_confirmation(&signature, config.require_confirmations).await?; Ok(signature) }
                Err(e1e) => Err(anyhow!("Jito failed on both: {}, {}", e2e, e1e)),
            },
        }
    }

    // === [IMPROVED: PIECE G] Strict transaction validation ===
    fn validate_transaction(&self, transaction: &Transaction) -> Result<()> {
        let size = transaction.message_data().len();
        if size > PACKET_DATA_SIZE {
            return Err(anyhow!("Transaction too large: {} bytes (max: {})", size, PACKET_DATA_SIZE));
        }
        let account_count = transaction.message.account_keys.len();
        if account_count > 64 {
            return Err(anyhow!("Too many accounts: {} (max: 64)", account_count));
        }
        if transaction.signatures[0].as_ref() == [0u8; 64] {
            return Err(anyhow!("Missing primary signature"));
        }
        Ok(())
    }

    // (moved) calculate_optimal_packet_fanout implemented at module level below

    fn calculate_backoff_delay(&self, attempt: u32) -> Duration {
        let mut rng = rand::thread_rng();
        let base = INITIAL_RETRY_DELAY_MS as f64;
                let exp_delay = base * 1.5_f64.powi(attempt.saturating_sub(1) as i32);
        let capped_delay = exp_delay.min(MAX_RETRY_DELAY_MS as f64);
        
        let jitter = capped_delay * 0.1;
        let jittered = capped_delay + (rng.gen::<f64>() - 0.5) * 2.0 * jitter;
        
        Duration::from_millis(jittered.max(INITIAL_RETRY_DELAY_MS as f64) as u64)
    }

    // Decorrelated exponential backoff seeded by signature (PIECE J)
    fn calculate_backoff_delay_seeded(&self, sig: Signature, attempt: u32) -> Duration {
        let base = INITIAL_RETRY_DELAY_MS as f64;
        let maxd = MAX_RETRY_DELAY_MS as f64;
        let exp = base * (1.7_f64).powi(attempt as i32);
        let unclamped = exp.min(maxd);
        let sb = sig.as_ref();
        let seed = ((sb[0] as u64) ^ ((sb[15] as u64) << 1) ^ ((sb[31] as u64) << 2)) % 31;
        let jitter = 0.85 + (seed as f64) / 100.0;
        let ms = ((unclamped * jitter).round() as u64).max(8);
        Duration::from_millis((ms / 2) * 2)
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

    // === [ULTRA++++: PIECE X] Proactive leader updater (epoch guard + jitter + quality) ===
    fn start_leader_schedule_updater(&self) {
        let me = self.clone();
        let shutdown = Arc::clone(&self.shutdown);

        tokio::spawn(async move {
            let mut ivl = interval(Duration::from_millis(LEADER_REFRESH_INTERVAL_MS));
            ivl.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            loop {
                if shutdown.load(Ordering::Acquire) { break; }
                ivl.tick().await;

                // Keep current slot fresh and keep it available for jitter salt
                let mut last_slot: u64 = me.leader_tracker.current_slot.load(Ordering::Acquire);
                if let Ok(s) = me.rpc_clients[0].get_slot() {
                    last_slot = s;
                    me.leader_tracker.current_slot.store(s, Ordering::Release);
                }

                let netq = me.network_monitor.get_quality().await;

                let needs_refresh = {
                    let info = me.leader_tracker.epoch_info.read().await;
                    let last = *me.leader_tracker.last_update.read().await;
                    let near_epoch_end = info.slots_per_epoch > 0 && info.slot_index + 512 >= info.slots_per_epoch;
                    let staleness = match netq {
                        NetworkQuality::Excellent | NetworkQuality::Good => Duration::from_millis(1500),
                        NetworkQuality::Fair                             => Duration::from_millis(1200),
                        NetworkQuality::Poor | NetworkQuality::Critical  => Duration::from_millis(900),
                    };
                    near_epoch_end || last.elapsed() > staleness
                };

                if needs_refresh {
                    // Small slot-salted jitter (0..80ms) to desync herding
                    let j = (last_slot ^ 0x9E3779B97F4A7C15) % 81;
                    sleep(Duration::from_millis(j)).await;
                    if let Err(e) = me.refresh_leader_schedule().await {
                        warn!("leader schedule refresh failed: {e}");
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

    // === [ULTRA++++: PIECE O] Proactive blockhash refresher (unique + processed + metric) ===
    fn start_blockhash_refresher(&self) {
        let blockhashes = Arc::clone(&self.recent_blockhashes);
        let shutdown = Arc::clone(&self.shutdown);
        let rpc = self.rpc_clients[0].clone();
        let metrics = Arc::clone(&self.metrics);

        tokio::spawn(async move {
            let mut ivl = interval(Duration::from_millis(800));
            ivl.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            while !shutdown.load(Ordering::Acquire) {
                ivl.tick().await;

                let current = rpc.get_slot().unwrap_or(0);
                let mut hashes = blockhashes.write().await;

                // prune stale (>120 slots old)
                hashes.retain(|&(_, slot)| current.saturating_sub(slot) < 120);

                // top-up when running low; keep unique; use processed commitment
                if hashes.len() < MAX_RECENT_BLOCKHASHES / 2 {
                    if let Ok((bh, _)) = rpc.get_latest_blockhash_with_commitment(CommitmentConfig::processed()) {
                        if !hashes.iter().any(|(h, _)| *h == bh) {
                            hashes.push_front((bh, current));
                            if hashes.len() > MAX_RECENT_BLOCKHASHES { hashes.pop_back(); }
                        }
                    }
                }

                // optional: heartbeat metric for visibility
                metrics.sent_count.fetch_add(0, Ordering::Relaxed);
            }
        });
    }

    // === [ULTRA++++: PIECE P] Bundle build + hedged send (per-tx CU) ===
    pub async fn inject_bundle(
        &self,
        transactions: Vec<(Vec<Instruction>, &Keypair, Vec<&Keypair>)>,
        config: TransactionConfig,
    ) -> Result<Vec<Signature>> {
        if transactions.is_empty() || transactions.len() > MAX_BUNDLE_SIZE {
            return Err(anyhow!("Invalid bundle size: {} (must be 1-{})", transactions.len(), MAX_BUNDLE_SIZE));
        }

        let blockhash = self.get_optimal_blockhash().await?;
        let cu_price_micro = self.calculate_dynamic_priority_fee(config.priority_fee_lamports).await?;

        let mut built = Vec::with_capacity(transactions.len());
        let mut sigs  = Vec::with_capacity(transactions.len());

        for (i, (ixs, payer, signers)) in transactions.into_iter().enumerate() {
            // signer normalization: ensure payer is first & unique
            let payer_pk = payer.pubkey();
            let mut all_signers: Vec<&Keypair> = Vec::with_capacity(signers.len() + 1);
            all_signers.push(payer);
            for s in signers {
                if s.pubkey() != payer_pk { all_signers.push(s); }
            }

            let est_cu = estimate_transaction_compute_units(&ixs);
            let limit  = est_cu.min(config.compute_units);

            let mut all_ix = Vec::with_capacity(ixs.len() + 3);
            all_ix.push(ComputeBudgetInstruction::set_compute_unit_limit(limit));
            all_ix.push(ComputeBudgetInstruction::set_compute_unit_price(cu_price_micro));
            if config.use_jito && self.jito_client.is_some() && i == 0 {
                if let Ok(tip) = self.create_jito_tip_instruction_from(&payer_pk) { all_ix.push(tip); }
            }
            all_ix.extend(ixs);

            let msg = Message::new_with_blockhash(&all_ix, Some(&payer_pk), &blockhash);
            let tx  = Transaction::new(&all_signers, msg, blockhash);
            self.validate_transaction(&tx)?;
            sigs.push(tx.signatures[0]);
            built.push(tx);
        }

        if config.use_jito && self.jito_client.is_some() {
            match self.send_bundle_jito_multi(built.clone()).await {
                Ok(()) => {
                    for sig in &sigs {
                        self.await_confirmation(sig, config.require_confirmations).await?;
                    }
                    return Ok(sigs);
                }
                Err(e) => warn!("Jito bundle failed, falling back to TPU: {e}"),
            }
        }

        let results = join_all(built.into_iter().map(|tx| self.send_via_tpu(tx, &config))).await;
        let mut out = Vec::with_capacity(results.len());
        for (i, r) in results.into_iter().enumerate() {
            match r { Ok(s) => out.push(s), Err(e) => { error!("bundle tx {} failed: {e}", i); return Err(e); } }
        }
        Ok(out)
    }

    // === [ULTRA++++: PIECE Q] Hedged Jito bundle sender (k-relay, early win) ===
    async fn send_bundle_jito_multi(&self, transactions: Vec<Transaction>) -> Result<()> {
        let jito = self.jito_client.as_ref().ok_or_else(|| anyhow!("Jito not configured"))?;

        let mut bundle = Vec::with_capacity(transactions.len());
        for tx in &transactions {
            let wire = bincode::serialize(tx).map_err(|e| anyhow!("serialize: {e}"))?;
            bundle.push(base64::encode(wire));
        }

        let tip = jito.current_tip_index.fetch_add(1, Ordering::AcqRel);
        let n = jito.endpoints.len().max(1);
        let k = n.min(3);
        let endpoints: Vec<&str> = (0..k).map(|i| &jito.endpoints[(tip + i) % n]).collect();

        let body = serde_json::json!({"jsonrpc":"2.0","id":1,"method":"sendBundle","params":[bundle]});
        let client = jito.client.clone();

        let send_req = |endpoint: &str| {
            let c = client.clone();
            let b = body.clone();
            async move {
                let resp = timeout(Duration::from_millis(JITO_BUNDLE_TIMEOUT_MS), async {
                    c.post(format!("{}/api/v1/bundles", endpoint))
                        .header("Content-Type", "application/json")
                        .json(&b)
                        .send()
                        .await
                }).await.map_err(|_| anyhow!("jito_timeout"))??;

                if !resp.status().is_success() {
                    let s = resp.status(); let t = resp.text().await.unwrap_or_default();
                    return Err(anyhow!("Jito {}: {}", s, t));
                }
                Ok::<(), anyhow::Error>(())
            }
        };

        let mut futs = FuturesUnordered::new();
        for ep in endpoints { futs.push(send_req(ep)); }

        let mut last_err: Option<anyhow::Error> = None;
        while let Some(res) = futs.next().await {
            match res {
                Ok(()) => return Ok(()),
                Err(e) => last_err = Some(e),
            }
        }
        Err(anyhow!(format!("Jito bundle failed on all relays: {}", last_err.unwrap_or_else(|| anyhow!("unknown")).to_string())))
    }

        // === [ULTRA+++: PIECE R] Rich metrics snapshot ===
    pub async fn get_metrics(&self) -> HashMap<String, u64> {
        let mut m = HashMap::new();

        m.insert("sent".into(), self.metrics.sent_count.load(Ordering::Relaxed));
        m.insert("confirmed".into(), self.metrics.confirmed_count.load(Ordering::Relaxed));
        m.insert("failed".into(), self.metrics.failed_count.load(Ordering::Relaxed));
        m.insert("retries".into(), self.metrics.retry_count.load(Ordering::Relaxed));
        m.insert("jito_success".into(), self.metrics.jito_success_count.load(Ordering::Relaxed));
        m.insert("avg_confirmation_ms".into(), self.metrics.avg_confirmation_ms.load(Ordering::Relaxed));

        m.insert("current_slot".into(), self.leader_tracker.current_slot.load(Ordering::Acquire));
        let epoch = self.leader_tracker.epoch_info.read().await;
        m.insert("epoch".into(), epoch.epoch);
        m.insert("slot_index".into(), epoch.slot_index);
        drop(epoch);

        // Extras: p50/p90 RPC ms, EWMA rate (x1000), quality code
        let lat = self.network_monitor.rpc_latencies.read().await;
        if !lat.is_empty() {
            let mut v: Vec<_> = lat.iter().cloned().collect();
            v.sort_unstable_by_key(|d| d.as_nanos());
            let p = |q: f64| -> u64 {
                let idx = ((v.len() as f64 - 1.0) * q).round() as usize;
                v[idx].as_millis() as u64
            };
            m.insert("rpc_p50_ms".into(), p(0.50));
            m.insert("rpc_p90_ms".into(), p(0.90));
        }
        drop(lat);

        let rates = self.network_monitor.confirmation_rates.read().await;
        if !rates.is_empty() {
            let mut ewma = 0.0;
            for &r in rates.iter() { ewma = 0.2 * r + 0.8 * ewma; }
            m.insert("confirm_rate_milli".into(), (ewma * 1000.0) as u64);
        }
        drop(rates);

        let q = self.network_monitor.get_quality().await;
        let code = match q { NetworkQuality::Excellent => 0, NetworkQuality::Good => 1, NetworkQuality::Fair => 2, NetworkQuality::Poor => 3, NetworkQuality::Critical => 4 };
        m.insert("network_quality_code".into(), code);

        m
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
    // === [ULTRA++++: PIECE S] NetworkMonitor (p90 + EWMA + hysteresis) ===
    async fn record_rpc_latency(&self, latency: Duration) {
        let mut q = self.rpc_latencies.write().await;
        if q.len() >= 256 { q.pop_front(); }
        q.push_back(latency);
    }

    async fn record_confirmation_rate(&self, rate: f64) {
        let mut q = self.confirmation_rates.write().await;
        if q.len() >= 256 { q.pop_front(); }
        q.push_back(rate.clamp(0.0, 1.0));
    }

    async fn update_network_quality(&self) {
        let lat = self.rpc_latencies.read().await;
        let rates = self.confirmation_rates.read().await;
        if lat.is_empty() || rates.is_empty() { return; }

        let mut v: Vec<_> = lat.iter().cloned().collect();
        v.sort_unstable_by_key(|d| d.as_nanos());
        let p90 = v[((v.len() as f64 - 1.0) * 0.90).round() as usize];

        // EWMA(alpha=0.2)
        let mut ewma = 0.0;
        for &r in rates.iter() { ewma = 0.2 * r + 0.8 * ewma; }

        // Base classification
        let base = if p90 <= Duration::from_millis(70)  && ewma > 0.94 { NetworkQuality::Excellent }
            else if p90 <= Duration::from_millis(110) && ewma > 0.86 { NetworkQuality::Good }
            else if p90 <= Duration::from_millis(170) && ewma > 0.72 { NetworkQuality::Fair }
            else if p90 <= Duration::from_millis(250) && ewma > 0.55 { NetworkQuality::Poor }
            else { NetworkQuality::Critical };

        // Hysteresis: don’t bounce too fast
        let last   = *self.network_quality.read().await;
        let sticky = match (last, base) {
            // upgrades require slightly stricter metrics; downgrades flow immediately
            (NetworkQuality::Critical, NetworkQuality::Poor)    if p90 <= Duration::from_millis(230) && ewma > 0.58 => base,
            (NetworkQuality::Poor,    NetworkQuality::Fair)     if p90 <= Duration::from_millis(160) && ewma > 0.75 => base,
            (NetworkQuality::Fair,    NetworkQuality::Good)     if p90 <= Duration::from_millis(100) && ewma > 0.88 => base,
            (NetworkQuality::Good,    NetworkQuality::Excellent)if p90 <= Duration::from_millis(60)  && ewma > 0.96 => base,
            // otherwise, if base worse than last, accept downgrade; else hold last
            _ if quality_worse(base, last) => base,
            _ => last,
        };

        drop(lat); drop(rates);
        *self.network_quality.write().await = sticky;
        *self.last_health_check.write().await = Instant::now();
    }

    async fn get_quality(&self) -> NetworkQuality {
        *self.network_quality.read().await
    }

    // Optional budget helper (can be used in hedging code)
    async fn dynamic_hedge_budget_ms(&self, netq: NetworkQuality) -> u64 {
        let lat = self.rpc_latencies.read().await;
        if lat.is_empty() {
            return match netq {
                NetworkQuality::Excellent => 80,  NetworkQuality::Good => 110,
                NetworkQuality::Fair      => 150, NetworkQuality::Poor => 190,
                NetworkQuality::Critical  => 210,
            };
        }
        let mut v: Vec<_> = lat.iter().cloned().collect();
        v.sort_unstable_by_key(|d| d.as_nanos());
        let p50 = v[((v.len() as f64 - 1.0) * 0.50).round() as usize];
        let p90 = v[((v.len() as f64 - 1.0) * 0.90).round() as usize];
        let base = (p50 + (p90 - p50)/2).as_millis() as u64;
        let guard = match netq { NetworkQuality::Excellent => 20, NetworkQuality::Good => 30,
                                 NetworkQuality::Fair => 40,      NetworkQuality::Poor => 50,
                                 NetworkQuality::Critical => 60 };
        let clamp = match netq { NetworkQuality::Excellent => 80, NetworkQuality::Good => 110,
                                 NetworkQuality::Fair => 150,     NetworkQuality::Poor => 190,
                                 NetworkQuality::Critical => 210 };
        base.saturating_add(guard).min(clamp).max(40)
    }
}

// Helper: ordering for hysteresis
#[inline]
fn quality_worse(a: NetworkQuality, b: NetworkQuality) -> bool {
    use NetworkQuality::*;
    let rank = |q: NetworkQuality| -> u8 { match q { Excellent=>0, Good=>1, Fair=>2, Poor=>3, Critical=>4 }};
    rank(a) > rank(b)
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

// === [ULTRA+++: PIECE U] Wait-with-timeout (front-loaded)
pub async fn wait_for_confirmation_with_timeout(
    injector: &MempoolLessInjector,
    signature: &Signature,
    timeout_ms: u64,
) -> Result<()> {
    let start = Instant::now();
    let deadline = start + Duration::from_millis(timeout_ms);

    while Instant::now() < deadline {
        match injector.check_transaction_status(signature).await? {
            Some(TransactionConfirmationStatus::Confirmed) |
            Some(TransactionConfirmationStatus::Finalized) => return Ok(()),
            Some(TransactionConfirmationStatus::Processed) => sleep(Duration::from_millis(16)).await,
            None => {
                let e = start.elapsed().as_millis() as u64;
                let ms = if e < 200 { 10 } else if e < 400 { 14 } else if e < 1500 { 24 } else { 40 };
                sleep(Duration::from_millis(ms)).await;
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
    ttl: Duration,
    states: Arc<DashMap<String, (bool, Instant)>>,
}

impl ConnectionHealthChecker {
    // === [ULTRA+++: PIECE T] ConnectionHealthChecker (TTL + parallel) ===
    pub fn new() -> Self {
        Self { ttl: Duration::from_secs(30), states: Arc::new(DashMap::new()) }
    }

    pub async fn check_endpoint(&self, endpoint: &str) -> bool {
        if let Some(entry) = self.states.get(endpoint) {
            let (ok, ts) = *entry;
            if ts.elapsed() < self.ttl { return ok; }
        }
        let client = RpcClient::new_with_timeout(endpoint.to_string(), Duration::from_secs(5));
        let ok = client.get_health().is_ok();
        self.states.insert(endpoint.to_string(), (ok, Instant::now()));
        ok
    }

    pub async fn get_healthy_endpoints(&self, endpoints: &[String]) -> Vec<String> {
        let mut futs = FuturesUnordered::new();
        for e in endpoints {
            let s = self.clone();
            let ep = e.clone();
            futs.push(async move { (ep.clone(), s.check_endpoint(&ep).await) });
        }
        let mut healthy = Vec::new();
        while let Some((ep, ok)) = futs.next().await { if ok { healthy.push(ep); } }
        if healthy.is_empty() && !endpoints.is_empty() { healthy.push(endpoints[0].clone()); }
        healthy
    }
}

impl Clone for ConnectionHealthChecker {
    fn clone(&self) -> Self { Self { ttl: self.ttl, states: Arc::clone(&self.states) } }
}

// === [ULTRA+++: PIECE V] Fanout clamp + high-value boost ===
#[inline(always)]
pub fn calculate_optimal_packet_fanout(network_quality: NetworkQuality, is_high_value: bool) -> usize {
    let base = match network_quality {
        NetworkQuality::Excellent => 12,
        NetworkQuality::Good      => 18,
        NetworkQuality::Fair      => 26,
        NetworkQuality::Poor      => 34,
        NetworkQuality::Critical  => 42,
    };
    let bonus = if is_high_value { 8 } else { 0 };
    (base + bonus).clamp(8, 80)
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

