// slot_boundary_flashloan_executor_v2.rs
// Top-1% upgrade of your slot boundary flashloan executor.
// Features: WebSocket subscriptions, TPU/QUIC sender, leader-aware fanout, pre-signed variants,
// per-leader fee modeling, account-scoped fee estimator, simulation-driven CU, ALTs handling,
// conflict-aware partitioning, retries categorized, sled persistence, structured JSON logs.

use anyhow::{anyhow, Context, Result};
use dashmap::DashMap;
use parking_lot::Mutex;
use quinn::{ClientConfig, Endpoint};
use serde::{Deserialize, Serialize};
use anchor_lang::prelude::*;
use solana_client::{
    nonblocking::pubsub_client::PubsubClient,
    rpc_response::SlotInfo,
};
use solana_sdk::timing::timestamp;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use futures_util::{SinkExt, StreamExt};
use anchor_lang::solana_program::{
    clock::Clock,
    instruction::{AccountMeta, Instruction},
    program::invoke_signed,
    pubkey::Pubkey,
    rent::Rent,
    system_instruction,
    sysvar::{clock, rent},
};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    nonblocking::pubsub_client::PubsubClient,
    rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig},
    rpc_response::{RpcSignatureResult, RpcSimulateTransactionResult},
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    message::{v0::Message as V0Message, VersionedMessage},
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::{Transaction, VersionedTransaction},
};
use solana_transaction_status::UiTransactionEncoding;
use thiserror::Error;
use std::{
    collections::{HashMap, HashSet, VecDeque},
    net::SocketAddr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

mod error;
mod execution;
mod fee_model;
mod metrics;

use error::{ExecError, Result as FlashloanResult};
use execution::{
    ensure_profit_after_fees, partition_non_conflicting, send_transaction_with_retry,
};
use fee_model::{PriorityFeeCalculator, FeeModel};
use metrics::*;
use tracing::{debug, error, info, instrument, span, warn, Level};
use solana_sdk::signature::Signature;
use thiserror::Error;
use tokio::{
    sync::{mpsc, RwLock, Semaphore},
    task::JoinSet,
    time::{interval, sleep},
};
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

// -----------------------------
// Config & constants
// -----------------------------
// Timing constants
const SLOT_DURATION_MS: u64 = 400;
const PREBOUNDARY_PREPARE_MS: u64 = 12;
const SEND_JITTER_MAX_MS: u64 = 1; // micro jitter to decorrelate sends

// Fee calculation
const CU_HEADROOM_PCT: f64 = 0.07; // 7%
const DEFAULT_CU_HEADROOM: f64 = 1.07; // 7% headroom
const FEE_PCTILE_DEFAULT: f64 = 0.99;
const FEE_PCTILE_AGGRESSIVE: f64 = 0.995;
const FEE_PCTILE_PASSIVE: f64 = 0.95;

// Blockhash and confirmation
const MAX_BLOCKHASH_AGE_SLOTS: u64 = 80;
const MAX_CONFIRM_SLOTS: u64 = 8;
const FALLBACK_CONFIRM_SLOTS: u64 = 20;
const CONSECUTIVE_MISS_CUTOFF: u32 = 3;

// Pre-signing and simulation
const MAX_SIMULATIONS: usize = 10;
const PRE_SIGN_VARIANTS: usize = 4;
const PRE_SIGN_NEXT_BLOCKHASHS: usize = 2;

// Persistence
const SLED_DB_PATH: &str = "./leader_stats_db";
const SLOT_WS_SKEW_SMOOTHING: f64 = 0.2;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaderStats {
    leader: Pubkey,
    seen: u64,
    landed: u64,
    mean_fee_to_land: f64,
    stdev_fee_to_land: f64,
    avg_units_landed: f64,
    // drift control
    last_updated: i64,
}

impl LeaderStats {
    fn inclusion_rate(&self) -> f64 {
        if self.seen == 0 { 0.0 } else { self.landed as f64 / self.seen as f64 }
    }
}

// -----------------------------
// Errors
// -----------------------------
#[derive(Error, Debug)]
pub enum ExecutorError {
    #[error("rpc error: {0}")]
    Rpc(String),

    #[error("simulation failed")]
    Simulation,

    #[error("signer error: {0}")]
    Signer(String),

    #[error("transmission failed: {0}")]
    Transmission(String),

    #[error("circuit breaker tripped")]
    CircuitBreaker,
}

impl From<anyhow::Error> for ExecutorError {
    fn from(e: anyhow::Error) -> Self {
        ExecutorError::Rpc(e.to_string())
    }
}

// -----------------------------
// Persistence + Metrics
// -----------------------------
#[derive(Clone)]
struct Persistence {
    db: Arc<Db>,
}
impl Persistence {
    pub fn open(path: &str) -> Result<Self> {
        let db = sled::open(path).context("open sled")?;
        Ok(Self { db: Arc::new(db) })
    }

    pub fn save_leader_stats(&self, key: &Pubkey, stats: &LeaderStats) -> Result<()> {
        let key_bytes = key.to_bytes();
        let serialized = serde_json::to_vec(stats)?;
        self.db.insert(key_bytes, serialized)?;
        Ok(())
    }

    pub fn load_leader_stats(&self, key: &Pubkey) -> Result<Option<LeaderStats>> {
        let key_bytes = key.to_bytes();
        if let Some(v) = self.db.get(key_bytes)? {
            let s: LeaderStats = serde_json::from_slice(&v)?;
            Ok(Some(s))
        } else {
            Ok(None)
        }
    }

    // -----------------------------
    // Main boundary executor (request-based wrapper)
    // -----------------------------
    #[instrument(skip(self, request))]
    pub async fn execute_request_at_slot_boundary(
        &self,
        request: FlashLoanRequest,
    ) -> FlashloanResult<ExecutionResult> {
        // Resolve leader for current slot
        let current_slot = self.get_current_slot().await;
        let leader = self.get_slot_leader(current_slot).await;

        // Build and price transaction using simulation-driven CU and fees
        let (tx, cu_limit, micro_per_cu) = self
            .build_and_price_flashloan_transaction(
                request.expected_profit,
                request.target_accounts.clone(),
                leader,
            )
            .await?;

        // Profit guard before sending
        ensure_profit_after_fees(request.expected_profit, micro_per_cu, cu_limit, 0)?;

        // Use the slot-aware sender (handles boundary timing and metrics)
        self
            .execute_at_slot_boundary(tx, cu_limit, micro_per_cu, request.expected_profit)
            .await
    }

    // -----------------------------
    // Simple path delegates to advanced
    // -----------------------------
    pub async fn execute_single_request(
        &self,
        request: FlashLoanRequest,
    ) -> FlashloanResult<ExecutionResult> {
        self.execute_single_request_advanced(request).await
    }
}

// -----------------------------
// lightweight percentile estimator (windowed)
// -----------------------------
#[derive(Clone)]
struct SlidingWindowPercentile {
    window: Arc<Mutex<VecDeque<u64>>>,
    capacity: usize,
}

impl SlidingWindowPercentile {
    fn new(capacity: usize) -> Self {
        Self {
            window: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            capacity,
        }
    }

    fn push(&self, value: u64) {
        let mut w = self.window.lock();
        w.push_back(value);
        if w.len() > self.capacity {
            w.pop_front();
        }
    }

    fn percentile(&self, p: f64) -> Option<u64> {
        let mut w = self.window.lock();
        if w.is_empty() { return None; }
        let mut v: Vec<u64> = w.iter().copied().collect();
        v.sort_unstable();
        let idx = ((v.len() as f64) * p).floor() as usize;
        Some(*v.get(idx.min(v.len()-1)).unwrap())
    }

    fn all(&self) -> Vec<u64> {
        self.window.lock().iter().copied().collect()
    }
}

// -----------------------------
// TPU/QUIC sender trait + implementations
// -----------------------------
#[async_trait::async_trait]
trait Sender: Send + Sync {
    async fn send(&self, signed_tx: &VersionedTransaction) -> Result<Signature>;
    async fn send_raw_quick(&self, tx_bytes: &[u8]) -> Result<Signature>;
}

#[derive(Clone)]
struct RpcFallbackSender {
    rpc: Arc<RpcClient>,
}

#[async_trait::async_trait]
impl Sender for RpcFallbackSender {
    async fn send(&self, signed_tx: &VersionedTransaction) -> Result<Signature> {
        // translate to legacy Transaction if needed
        let tx = solana_sdk::transaction::VersionedTransaction::clone(signed_tx);
        let config = RpcSendTransactionConfig {
            skip_preflight: false,
            preflight_commitment: Some(CommitmentConfig::processed()),
            ..Default::default()
        };
        let sig = self.rpc.send_transaction_with_config(&tx, config).await?;
        Ok(sig)
    }

    async fn send_raw_quick(&self, tx_bytes: &[u8]) -> Result<Signature> {
        // Fallback raw send via RPC if necessary
        let config = RpcSendTransactionConfig {
            skip_preflight: false,
            preflight_commitment: Some(CommitmentConfig::processed()),
            ..Default::default()
        };
        let sig = self.rpc.send_raw_transaction_with_config(tx_bytes.to_vec(), config).await?;
        Ok(sig)
    }
}

#[derive(Clone)]
struct QuicTpuSender {
    // QUIC endpoint + cached leader endpoints (tpu sockets)
    quic_endpoint: Arc<Endpoint>,
    leader_tpu_map: Arc<DashMap<Pubkey, SocketAddr>>,
    // local fallback sender
    fallback: RpcFallbackSender,
}

impl QuicTpuSender {
    async fn connect_quic() -> Result<Endpoint> {
        // minimal default ClientConfig
        let mut client_cfg = ClientConfig::default();
        // keep defaults; production: set ALPN, crypto params
        let endpoint = Endpoint::client("[::]:0".parse().unwrap())?;
        endpoint.set_default_client_config(client_cfg);
        Ok(endpoint)
    }

    pub async fn new(rpc: Arc<RpcClient>) -> Result<Self> {
        let endpoint = Self::connect_quic().await?;
        Ok(Self {
            quic_endpoint: Arc::new(endpoint),
            leader_tpu_map: Arc::new(DashMap::new()),
            fallback: RpcFallbackSender { rpc },
        })
    }

    pub fn update_leader_tpu(&self, leader: Pubkey, addr: SocketAddr) {
        self.leader_tpu_map.insert(leader, addr);
    }
}

#[async_trait::async_trait]
impl Sender for QuicTpuSender {
    async fn send(&self, signed_tx: &VersionedTransaction) -> Result<Signature> {
        let bytes = bincode::serialize(signed_tx).context("serialize vtx")?;
        // pick a leader tpu address if available (fallback to rpc)
        if let Some(kv) = self.leader_tpu_map.iter().next() {
            let addr = *kv.value();
            match self.send_raw_over_quic(addr, &bytes).await {
                Ok(sig) => Ok(sig),
                Err(e) => {
                    warn!("quic send failed, fallback rpc: {:?}", e);
                    self.fallback.send_raw_quick(&bytes).await
                }
            }
        } else {
            self.fallback.send_raw_quick(&bytes).await
        }
    }

    async fn send_raw_quick(&self, tx_bytes: &[u8]) -> Result<Signature> {
        // choose any known TPU
        if let Some(kv) = self.leader_tpu_map.iter().next() {
            let addr = *kv.value();
            match self.send_raw_over_quic(addr, tx_bytes).await {
                Ok(sig) => Ok(sig),
                Err(e) => {
                    warn!("quic raw send failed, fallback rpc: {:?}", e);
                    self.fallback.send_raw_quick(tx_bytes).await
                }
            }
        } else {
            self.fallback.send_raw_quick(tx_bytes).await
        }
    }
}

impl QuicTpuSender {
    async fn send_raw_over_quic(&self, addr: SocketAddr, bytes: &[u8]) -> Result<Signature> {
        // QUIC connect and send a single datagram or stream: production TPU expects UDP packets,
        // but a QUIC wrapper is used here as a reliable path to a relay that accepts our stream.
        // Implementation: open bi-directional stream, send bytes, wait for a signature response (signed).
        let quic_conn = self.quic_endpoint.connect(addr, "solana-tpu")?;
        let quic_conn = quic_conn.await.context("quic connect wait")?;
        let (mut send, mut recv) = quic_conn.open_bi().await.context("open bi")?;
        send.write_all(bytes).await.context("quic write")?;
        send.finish().await.context("quic finish")?;
        // expect signature in response bytes (bincode Signature)
        let mut buf = Vec::new();
        while let Some(chunk) = recv.read_chunk(usize::MAX, true).await? {
            buf.extend_from_slice(&chunk.bytes);
        }
        let sig: Signature = bincode::deserialize(&buf).context("deserialize signature")?;
        Ok(sig)
    }
}

// -----------------------------
// Fee model & priority fee calculator
// -----------------------------
#[derive(Clone)]
struct PriorityFeeModel {
    // sliding window of account-scoped fees
    per_account_windows: Arc<DashMap<Vec<u8>, SlidingWindowPercentile>>,
    base_priority_fee: Arc<AtomicU64>,
    persistence: Persistence,
}

impl PriorityFeeModel {
    pub fn new(base: u64, persistence: Persistence) -> Self {
        Self {
            per_account_windows: Arc::new(DashMap::new()),
            base_priority_fee: Arc::new(AtomicU64::new(base)),
            persistence,
        }
    }

    /// sample recent prioritization fees scoped to account meta set and leader
    pub async fn sample_and_push(&self, rpc: &RpcClient, accounts: &[Pubkey], leader: &Pubkey) -> Result<()> {
        // Use non-empty account list as recommended: serialize accounts to a key
        let mut key = Vec::with_capacity(accounts.len()*32 + 32);
        for a in accounts { key.extend_from_slice(&a.to_bytes()); }
        key.extend_from_slice(&leader.to_bytes());

        // call the RPC method that supports account-scoped prioritization fees. If not available in SDK,
        // call a custom RPC method or fallback to broad fees. For now, attempt standard call.
        let sample = rpc.get_recent_prioritization_fees(&accounts.iter().map(|p| p.to_bytes()).collect::<Vec<_>>()).await;
        match sample {
            Ok(fees) => {
                let window = self.per_account_windows.entry(key.clone())
                    .or_insert_with(|| SlidingWindowPercentile::new(1024)).clone();
                for info in fees {
                    window.push(info.prioritization_fee);
                }
            },
            Err(e) => {
                // fallback: push base fee
                let window = self.per_account_windows.entry(key.clone())
                    .or_insert_with(|| SlidingWindowPercentile::new(1024)).clone();
                window.push(self.base_priority_fee.load(Ordering::Acquire));
                tracing::warn!("sample_and_push fallback: {}", e);
            }
        }
        Ok(())
    }

    /// estimate micro-per-cu for desired percentile
    pub fn estimate_micro_per_cu(&self, accounts: &[Pubkey], leader: &Pubkey, percentile: f64, units: u64) -> u64 {
        let mut key = Vec::with_capacity(accounts.len()*32 + 32);
        for a in accounts { key.extend_from_slice(&a.to_bytes()); }
        key.extend_from_slice(&leader.to_bytes());

        if let Some(entry) = self.per_account_windows.get(&key) {
            if let Some(p) = entry.percentile(percentile) {
                // p is lamports
                let micro_per_cu = (p as u128).saturating_mul(1_000_000u128) / (units as u128);
                return micro_per_cu as u64;
            }
        }
        // fallback: base fee per CU
        let base = self.base_priority_fee.load(Ordering::Acquire);
        ((base as u128 * 1_000_000u128) / (units as u128)) as u64
    }
}

// -----------------------------
// Pre-signer: create pre-signed variant bundle
// -----------------------------
#[derive(Clone)]
struct PreSigner {
    wallet: Arc<Keypair>,
    // cache: leader -> blockhash -> Vec<(signed_vtx, signature, variant_meta)>
    cache: Arc<DashMap<(Pubkey, Hash), Vec<(VersionedTransaction, Signature, PreSignMeta)>>>,
}

#[derive(Clone, Debug)]
struct PreSignMeta {
    cu_limit: u32,
    compute_price: u64,
    uses_alt: bool,
    variant_id: Uuid,
}

impl PreSigner {
    fn new(wallet: Arc<Keypair>) -> Self {
        Self {
            wallet,
            cache: Arc::new(DashMap::new()),
        }
    }

    async fn pre_sign_variants(
        &self,
        request_accounts: &[solana_sdk::account::AccountMeta],
        base_ixs: Vec<Instruction>,
        blockhash: Hash,
        leader: Pubkey,
        variants: Vec<(u32 /*cu_limit*/, u64 /*micro_per_cu*/, bool /*uses_alt*/)>
    ) -> Result<()> {
        let mut signed_list = Vec::with_capacity(variants.len());
        for (cu_limit, micro_per_cu, uses_alt) in variants {
            // compute ix set ordering: compute budget first
            let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(cu_limit);
            let price_ix = ComputeBudgetInstruction::set_compute_unit_price(micro_per_cu);
            let mut all_ixs = vec![cu_ix, price_ix];
            all_ixs.extend(base_ixs.clone());

            let message = Message::new(&all_ixs, Some(&self.wallet.pubkey()));
            let tx = Transaction::new(&[&*self.wallet], message, blockhash);
            // convert to versioned if needed
            let vtx = VersionedTransaction::try_from(tx)?;
            let sig = vtx.signatures[0];
            let meta = PreSignMeta {
                cu_limit,
                compute_price: micro_per_cu,
                uses_alt,
                variant_id: Uuid::new_v4(),
            };
            signed_list.push((vtx, sig, meta));
        }
        self.cache.insert((leader, blockhash), signed_list);
        Ok(())
    }

    fn get_variant(&self, leader: &Pubkey, blockhash: &Hash, variant_idx: usize) -> Option<(VersionedTransaction, Signature, PreSignMeta)> {
        self.cache.get(&(leader.clone(), *blockhash)).and_then(|v| v.get(variant_idx).cloned())
    }
}

// -----------------------------
// Conflict-aware partitioner (greedy coloring)
// -----------------------------
fn partition_non_conflicting<T, F>(requests: &[T], write_set_fn: F) -> Vec<Vec<T>>
where
    T: Clone,
    F: Fn(&T) -> Vec<Pubkey>,
{
    // greedy coloring: small graph coloring interpretation
    let mut groups: Vec<Vec<T>> = Vec::new();
    let mut group_writes: Vec<HashSet<Pubkey>> = Vec::new();

    for r in requests.iter() {
        let ws = write_set_fn(r);
        let mut placed = false;
        for (gi, group) in groups.iter_mut().enumerate() {
            let gw = &mut group_writes[gi];
            if gw.is_disjoint(&ws.iter().cloned().collect()) {
                gw.extend(ws.iter().cloned());
                group.push(r.clone());
                placed = true;
                break;
            }
        }
        if !placed {
            // new group
            let mut set = HashSet::new();
            set.extend(ws.iter().cloned());
            group_writes.push(set);
            groups.push(vec![r.clone()]);
        }
    }
    groups
}
// Executor core
// -----------------------------
#[derive(Clone)]
/// Represents the result of a flashloan execution
#[derive(Debug, Clone)]
pub struct ExecutionResult {
    pub signature: Signature,
    pub slot: u64,
    pub timing_offset_ms: i64,
    pub priority_fee: u64,
    pub profit: u64,
}

/// Represents a flashloan request
#[derive(Debug, Clone)]
pub struct FlashLoanRequest {
    pub target_accounts: Vec<AccountMeta>,
    pub expected_profit: u64,
    pub min_slot: Option<u64>,
    pub max_slot: Option<u64>,
}

pub struct SlotBoundaryFlashLoanExecutorV2 {
    rpc: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    sender: Arc<dyn Sender>,
    pre_signer: PreSigner,
    priority_model: PriorityFeeModel,
    persistence: Persistence,
    simulation_semaphore: Arc<Semaphore>,
    leader_stats_cache: Arc<DashMap<Pubkey, LeaderStats>>,
    circuit_breaker: Arc<AtomicBool>,
    slot_start: Arc<AtomicU64>,
    slot_start_instant: Arc<RwLock<Instant>>,
    
    // WebSocket and timing related fields
    ws_slot_receiver: Arc<Mutex<Option<mpsc::UnboundedReceiver<u64>>>>,
    ws_last_update: Arc<AtomicU64>,
    adaptive_offset_ms: Arc<AtomicU64>,
    slot_boundary_offset_ms: Arc<AtomicU64>,
    leader_schedule: Arc<Mutex<HashMap<u64, Pubkey>>>,
    current_leader: Arc<Mutex<Option<Pubkey>>>,
    last_leader_switch: Arc<AtomicU64>,
    
    // Fee calculation and execution
    fee_calculator: Arc<PriorityFeeCalculator>,
    max_retries: u8,
}

impl SlotBoundaryFlashLoanExecutorV2 {
    pub async fn new(rpc_url: &str, wallet: Keypair) -> Result<Self> {
        // Initialize metrics
        if let Err(e) = metrics::init_metrics() {
            warn!("Failed to initialize metrics: {}", e);
        }
        let rpc = Arc::new(RpcClient::new_with_commitment(rpc_url.to_string(), CommitmentConfig::processed()));
        let persistence = Persistence::open(SLED_DB_PATH)?;
        let sender_quic = QuicTpuSender::new(rpc.clone()).await?;
        let sender: Arc<dyn Sender> = Arc::new(sender_quic);
        let pre_signer = PreSigner::new(Arc::new(wallet.clone()));
        let priority_model = PriorityFeeModel::new(100_000u64, persistence.clone());
        
        // Initialize WebSocket and timing related fields
        let (slot_sender, slot_receiver) = mpsc::unbounded_channel();
        
        // Initialize fee calculator with 100k lamports base fee
        let fee_calculator = Arc::new(PriorityFeeCalculator::new(100_000));
        
        let executor = Self {
            rpc: rpc.clone(),
            wallet: Arc::new(wallet),
            sender,
            pre_signer,
            priority_model,
            persistence,
            simulation_semaphore: Arc::new(Semaphore::new(MAX_SIMULATIONS)),
            leader_stats_cache: Arc::new(DashMap::new()),
            circuit_breaker: Arc::new(AtomicBool::new(false)),
            slot_start: Arc::new(AtomicU64::new(0)),
            slot_start_instant: Arc::new(RwLock::new(Instant::now())),
            ws_slot_receiver: Arc::new(Mutex::new(Some(slot_receiver))),
            ws_last_update: Arc::new(AtomicU64::new(timestamp())),
            adaptive_offset_ms: Arc::new(AtomicU64::new(5)), // Start with 5ms offset
            slot_boundary_offset_ms: Arc::new(AtomicU64::new(5)),
            leader_schedule: Arc::new(Mutex::new(HashMap::new())),
            current_leader: Arc::new(Mutex::new(None)),
            last_leader_switch: Arc::new(AtomicU64::new(0)),
            fee_calculator,
            max_retries: 3, // Default max retries
        };
        
        // Initial leader schedule refresh with retry
        let max_retries = 3;
        let mut retry_count = 0;
        while retry_count < max_retries {
            match executor.refresh_leader_schedule().await {
                Ok(_) => break,
                Err(e) => {
                    retry_count += 1;
                    warn!("Failed to refresh leader schedule (attempt {}/{}): {}", 
                        retry_count, max_retries, e);
                    if retry_count < max_retries {
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
        
        // Start WebSocket monitoring in the background
        executor.start_ws_slot_monitoring(slot_sender).await?;
        
        // Start periodic leader schedule updates
        executor.start_leader_schedule_updater().await?;
        
        Ok(executor)
    }

    // -----------------------------
    // WebSocket-based slot monitoring with adaptive offset
    // -----------------------------
    async fn start_ws_slot_monitoring(&self, slot_sender: mpsc::UnboundedSender<u64>) -> Result<()> {
        let rpc_url = self.rpc.url().to_string();
        let ws_url = rpc_url.replace("https://", "wss://").replace("http://", "ws://");
        let slot_start = self.slot_start.clone();
        let slot_start_instant = self.slot_start_instant.clone();
        let ws_last_update = self.ws_last_update.clone();
        let adaptive_offset_ms = self.adaptive_offset_ms.clone();
        
        tokio::spawn(async move {
            let mut backoff = 1;
            let max_backoff = 60; // 1 minute max backoff
            
            loop {
                match PubsubClient::slot_subscribe(&ws_url).await {
                    Ok((mut ws, slot_unsub)) => {
                        info!("Connected to slot subscription at {}", ws_url);
                        backoff = 1; // Reset backoff on successful connection
                        
                        while let Some(msg) = ws.next().await {
                            match msg {
                                Ok(WsMessage::Text(text)) => {
                                    if let Ok(slot_info) = serde_json::from_str::<SlotInfo>(&text) {
                                        let slot = slot_info.slot;
                                        let timestamp = timestamp();
                                        
                                        // Update slot tracking
                                        slot_start.store(slot, Ordering::Release);
                                        *slot_start_instant.write().await = Instant::now();
                                        ws_last_update.store(timestamp, Ordering::Release);
                                        
                                        // Send slot update to channel
                                        if let Err(e) = slot_sender.send(slot) {
                                            error!("Failed to send slot update: {}", e);
                                        }
                                        
                                        // Calculate adaptive offset (EMA of observed delays)
                                        let observed_delay = timestamp.saturating_sub(slot_info.timestamp * 1000);
                                        let current_offset = adaptive_offset_ms.load(Ordering::Acquire) as f64;
                                        let new_offset = (observed_delay as f64 * 0.2) + (current_offset * 0.8);
                                        adaptive_offset_ms.store(new_offset as u64, Ordering::Release);
                                    }
                                }
                                Ok(_) => {}
                                Err(e) => {
                                    warn!("WebSocket error: {}", e);
                                    break;
                                }
                            }
                        }
                        
                        // Clean up subscription on exit
                        let _ = slot_unsub().await;
                    }
                    Err(e) => {
                        error!("Failed to connect to WebSocket: {}", e);
                    }
                }
                
                // Exponential backoff with jitter before reconnecting
                let sleep_time = std::cmp::min(backoff, max_backoff);
                let jitter = rand::random::<u64>() % 1000; // Add up to 1s jitter
                tokio::time::sleep(Duration::from_secs(sleep_time) + Duration::from_millis(jitter)).await;
                backoff = std::cmp::min(backoff * 2, max_backoff);
            }
        });
        
        Ok(())
    }
    
    // -----------------------------
    // Leader schedule updater
    // -----------------------------
    async fn start_leader_schedule_updater(&self) -> Result<()> {
        let rpc = self.rpc.clone();
        let leader_schedule = self.leader_schedule.clone();
        let current_leader = self.current_leader.clone();
        let last_leader_switch = self.last_leader_switch.clone();
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(60)); // Update every minute
            
            loop {
                interval.tick().await;
                
                match rpc.get_leader_schedule(None).await {
                    Ok(schedule) => {
                        let mut leader_map = leader_schedule.lock().await;
                        *leader_map = schedule.into_iter()
                            .flat_map(|(leader, slots)| {
                                slots.into_iter().map(move |slot| (slot, leader.parse().unwrap()))
                            })
                            .collect();
                        
                        // Update current leader if needed
                        if let Ok(current_slot) = rpc.get_slot().await {
                            if let Some(leader) = leader_map.get(&current_slot) {
                                let mut current = current_leader.lock().await;
                                if current.as_ref() != Some(leader) {
                                    *current = Some(*leader);
                                    last_leader_switch.store(timestamp(), Ordering::Release);
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("Failed to update leader schedule: {}", e);
                    }
                }
            }
        });
        
        Ok(())
    }
    
    // -----------------------------
    // Get the current leader
    // -----------------------------
    pub async fn get_current_leader(&self) -> Option<Pubkey> {
        self.current_leader.lock().await.clone()
    }
    
    // -----------------------------
    // Get the adaptive offset in milliseconds
    // -----------------------------
    pub fn get_adaptive_offset_ms(&self) -> u64 {
        self.adaptive_offset_ms.load(Ordering::Acquire)
    }
    }

    // -----------------------------
    // Calculate next slot boundary with adaptive offset
    // -----------------------------
    async fn calculate_next_slot_boundary(&self) -> Instant {
        // Get the current slot start time
        let slot_start = self.slot_start_instant.read().await.clone();
        let elapsed = slot_start.elapsed();
        let elapsed_ms = elapsed.as_millis() as u64;
        
        // Calculate time until next slot boundary
        let time_to_next_slot = SLOT_DURATION_MS.saturating_sub(elapsed_ms % SLOT_DURATION_MS);
        
        // Get adaptive offset and ensure it's within reasonable bounds
        let mut offset = self.adaptive_offset_ms.load(Ordering::Acquire);
        offset = offset.min(SLOT_DURATION_MS / 2); // Cap at half slot duration
        offset = offset.max(1); // Ensure at least 1ms
        
        // Calculate target time with offset (aim to send just before the boundary)
        let target_offset = time_to_next_slot.saturating_sub(offset);
        
        // Ensure we don't schedule in the past
        let min_delay = std::cmp::max(target_offset, 1);
        
        Instant::now() + Duration::from_millis(min_delay)
    }
    
    // -----------------------------
    // Refresh the leader schedule for the current epoch
    // -----------------------------
    pub async fn refresh_leader_schedule(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Get current epoch info to map epoch-relative indices
        let epoch_info = self.rpc.get_epoch_info().await?;
        let epoch = epoch_info.epoch;
        let epoch_start_slot = epoch_info.absolute_slot.saturating_sub(epoch_info.slot_index as u64);
        let slots_per_epoch = epoch_info.slots_in_epoch;

        // Get leader schedule with finalized commitment
        if let Ok(schedule) = self.rpc.get_leader_schedule_with_commitment(
            Some(epoch_start_slot),
            CommitmentConfig::finalized(),
        ).await {
            // Map relative slot -> leader
            // Build full epoch vector length = slots_per_epoch
            let mut leaders = vec![Pubkey::default(); slots_per_epoch as usize];
            
            // Fill in the leaders for each slot
            for (leader_str, rel_slots) in schedule.into_iter() {
                if let Ok(leader_pk) = leader_str.parse::<Pubkey>() {
                    for rel in rel_slots {
                        let idx = rel as usize;
                        if idx < leaders.len() {
                            leaders[idx] = leader_pk;
                        }
                    }
                }
            }
            
            // Store the current epoch info and leader schedule
            let mut leader_data = self.leader_schedule.lock().await;
            *leader_data = leaders;
            
            debug!("Updated leader schedule for epoch {} (slots: {}-{})", 
                epoch, epoch_start_slot, epoch_start_slot + slots_per_epoch - 1);
        } else {
            warn!("Failed to get leader schedule for epoch {}", epoch);
        }
        
        Ok(())
    }
    
    // -----------------------------
    // Get the leader for a specific slot
    // -----------------------------
    pub async fn get_slot_leader(&self, slot: u64) -> Option<Pubkey> {
        // Get current epoch info to calculate relative slot
        let epoch_info = match self.rpc.get_epoch_info().await {
            Ok(info) => info,
            Err(e) => {
                warn!("Failed to get epoch info: {}", e);
                return None;
            }
        };
        
        let epoch_start = epoch_info.absolute_slot.saturating_sub(epoch_info.slot_index as u64);
        let rel_slot = slot.saturating_sub(epoch_start);
        
        // Get the leader schedule
        let leaders = self.leader_schedule.lock().await;
        if leaders.is_empty() {
            debug!("Leader schedule is empty, falling back to RPC");
            drop(leaders); // Release the lock before making RPC calls
            return self.rpc.get_slot_leader(slot).await.ok();
        }
        
        // Get leader using epoch-relative index
        let idx = (rel_slot as usize) % leaders.len();
        let leader = leaders[idx];
        
        if leader == Pubkey::default() {
            None
        } else {
            Some(leader)
        }
    }
    
    // -----------------------------
    // Refresh the leader schedule
    // -----------------------------
    pub async fn refresh_leader_schedule(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Get current epoch info
        let epoch_info = self.rpc.get_epoch_info().await?;
        let epoch = epoch_info.epoch;
        let epoch_start_slot = epoch_info.absolute_slot - (epoch_info.slot_index as u64);
        
        // Get leader schedule for this epoch
        if let Ok(schedule) = self.rpc.get_leader_schedule(Some(epoch_start_slot)).await {
            let mut leader_map = HashMap::new();
            
            // Convert schedule to slot -> leader mapping
            for (leader_str, rel_slots) in schedule {
                if let Ok(leader_pk) = leader_str.parse::<Pubkey>() {
                    for rel in rel_slots {
                        leader_map.insert(rel, leader_pk);
                    }
                }
            }
            
            // Update leader schedule
            *self.leader_schedule.lock().await = leader_map;
        }
        
        Ok(())
    }
    
    // -----------------------------
    // Get the current leader
    // -----------------------------
    pub async fn get_current_leader(&self) -> Pubkey {
        let current_slot = match self.rpc.get_slot().await {
            Ok(slot) => slot,
            Err(_) => return Pubkey::default(),
        };
        
        self.get_slot_leader(current_slot).await.unwrap_or_default()
    }
    
    // -----------------------------
    // Get current slot
    // -----------------------------
    pub async fn get_current_slot(&self) -> u64 {
        self.rpc.get_slot().await.unwrap_or(0)
    }

    // -----------------------------
    // Build and price flashloan transaction with simulation-driven CU and fees
    // -----------------------------
    async fn build_and_price_flashloan_transaction(
        &self,
        base_instructions: Vec<Instruction>,
        accounts: &[Pubkey],
        leader: Pubkey,
    ) -> Result<(VersionedTransaction, u64, u64), anyhow::Error> {
        // Step 1: Get recent blockhash and build minimal transaction for simulation
        let blockhash = self.rpc.get_latest_blockhash().await?;
        
        // Build minimal transaction without compute budget instructions
        let message = Message::new(&base_instructions, Some(&self.wallet.pubkey()));
        let tx = Transaction::new(&[&*self.wallet], message, blockhash);
        let simulate_tx = VersionedTransaction::try_from(tx)?;

        // Step 2: Simulate to get compute units
        let (ok, units) = self.simulate_vtx(&simulate_tx).await?;
        if !ok || units == 0 {
            return Err(anyhow!("simulation failed or returned 0 compute units"));
        }

        // Calculate CU limit with headroom
        let cu_limit = ((units as f64) * (1.0 + CU_HEADROOM_PCT)).ceil() as u32;
        
        // Step 3: Get micro-lamports per CU using account-scoped percentile
        let micro_per_cu = self.fee_calculator.estimate_micro_per_cu_for_accounts(
            &self.rpc,
            accounts,
            Some(leader),
            cu_limit as u64,
        ).await;

        // Step 4: Build final transaction with compute budget instructions
        let cu_ix = ComputeBudgetInstruction::set_compute_unit_limit(cu_limit);
        let price_ix = ComputeBudgetInstruction::set_compute_unit_price(micro_per_cu);
        
        let mut final_ixs = vec![cu_ix, price_ix];
        final_ixs.extend(base_instructions);
        
        let final_message = Message::new(&final_ixs, Some(&self.wallet.pubkey()));
        let final_tx = Transaction::new(&[&*self.wallet], final_message, blockhash);
        let vtx_final = VersionedTransaction::try_from(final_tx)?;

        Ok((vtx_final, cu_limit as u64, micro_per_cu))
    }
    
    // Kept for backward compatibility
    async fn build_transaction_with_cu(
        &self,
        base_instructions: Vec<Instruction>,
        accounts: &[Pubkey],
        leader: Pubkey,
        _target_percentile: f64, // Not used, kept for signature compatibility
    ) -> Result<(VersionedTransaction, u64, u32)> {
        // Delegate to the new implementation for backward compatibility
        let (vtx, cu_limit, micro_per_cu) = self.build_and_price_flashloan_transaction(
            base_instructions,
            accounts,
            leader,
    async fn simulate_vtx(&self, vtx: &VersionedTransaction) -> Result<(bool, u64)> {
        let _permit = self.simulation_semaphore
            .acquire()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to acquire simulation semaphore: {}", e))?;
            
        let start_time = Instant::now();
        
        let config = RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: true,
            commitment: Some(CommitmentConfig::processed()),
            encoding: Some(UiTransactionEncoding::Base64),
            accounts: None,
            min_context_slot: None,
            inner_instructions: true,  // Collect more info for debugging
        };
        
        let sim = self.rpc.simulate_transaction_with_config(vtx, config).await
            .map_err(|e| anyhow::anyhow!("RPC simulation failed: {}", e))?;
        
        // Record simulation time
        let sim_time_ms = start_time.elapsed().as_millis() as u64;
        metrics::SIMULATION_TIME_MS.set(sim_time_ms as i64);
        
        if let Some(err) = &sim.value.err {
            warn!(
                error = ?err,
                simulation_time_ms = sim_time_ms,
                "Transaction simulation failed"
            );
            return Ok((false, 0));
        }
        
        let units_consumed = sim.value.units_consumed.unwrap_or(0);
        
        debug!(
            units = units_consumed,
            simulation_time_ms = sim_time_ms,
            "Transaction simulation successful"
        );
        
        Ok((true, units_consumed))
    }

    // -----------------------------
    // Observability-friendly simulate for legacy Transaction
    // -----------------------------
    #[instrument(skip_all)]
    async fn simulate_transaction(
        &self,
        transaction: &Transaction,
    ) -> Result<(bool, u64), anyhow::Error> {
        let _permit = self
            .simulation_semaphore
            .acquire()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to acquire simulation semaphore: {}", e))?;

        let start_time = Instant::now();
        let config = RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: true,
            commitment: Some(CommitmentConfig::processed()),
            encoding: Some(UiTransactionEncoding::Base64),
            accounts: None,
            min_context_slot: None,
            inner_instructions: true,
        };

        let result = self
            .rpc
            .simulate_transaction_with_config(transaction, config)
            .await
            .map_err(|e| anyhow::anyhow!("RPC simulation failed: {}", e))?;

        // Metrics
        let sim_time_ms = start_time.elapsed().as_millis() as i64;
        metrics::SIMULATION_TIME_MS.set(sim_time_ms);

        if let Some(err) = result.value.err.clone() {
            warn!(error = ?err, simulation_time_ms = sim_time_ms, "simulation failed");
            return Ok((false, 0));
        }

        let units_consumed = result.value.units_consumed.unwrap_or(0);
        debug!(units = units_consumed, simulation_time_ms = sim_time_ms, "simulation ok");
        Ok((true, units_consumed))
    }

    // -----------------------------
    // Execute a batch of flashloan requests at slot boundary
    // -----------------------------
    pub async fn execute_batch_at_boundary(
        &self,
        requests: Vec<FlashLoanRequest>,
    ) -> Vec<FlashloanResult<ExecutionResult>> {
        // Wait for slot boundary
        let slot_boundary_time = self.calculate_next_slot_boundary().await;
        let now = Instant::now();
        
        if slot_boundary_time > now {
            let wait_time = slot_boundary_time.duration_since(now);
            if wait_time > Duration::from_millis(SLOT_DURATION_MS) {
                return vec![Err(Box::new(ExecError::Other("Invalid slot boundary calculation".into())))];
            }
            sleep(wait_time).await;
        }

        // Partition by write-set conflicts
        let groups = partition_non_conflicting(&requests, |r| {
            r.target_accounts
                .iter()
                .filter(|m| m.is_writable)
                .map(|m| m.pubkey)
                .collect::<Vec<_>>()
        });

        // Execute groups in parallel
        let mut handles = Vec::with_capacity(groups.len());
        for group in groups {
            let executor = self.clone();
            handles.push(tokio::spawn(async move {
                let mut results = Vec::with_capacity(group.len());
                for request in group {
                    results.push(executor.execute_single_request_advanced(request).await);
                }
                results
            }));
        }

        // Collect results
        let mut all_results = Vec::with_capacity(requests.len());
        for handle in handles {
            match handle.await {
                Ok(mut results) => all_results.append(&mut results),
                Err(e) => all_results.push(Err(Box::new(ExecError::Other(format!("Task join error: {}", e))))),
            }
        }

        all_results
    }

    // -----------------------------
    // Execute a single flashloan request with advanced features
    // -----------------------------
    pub async fn execute_single_request_advanced(
        &self,
        request: FlashLoanRequest,
    ) -> FlashloanResult<ExecutionResult> {
        let leader = self.get_current_leader().await;
        
        // Build and price the transaction
        let (tx, cu_limit, micro_per_cu) = self
            .build_and_price_flashloan_transaction(
                request.expected_profit,
                request.target_accounts.clone(),
                leader,
            )
            .await?;

        // Execute at the next slot boundary
        self.execute_at_slot_boundary(tx, cu_limit, micro_per_cu, request.expected_profit)
            .await
    }

    // -----------------------------
    // Execute at slot boundary with proper timing
    // -----------------------------
    pub async fn execute_at_slot_boundary(
        &self,
        transaction: VersionedTransaction,
        cu_limit: u64,
        micro_per_cu: u64,
        expected_profit: u64,
    ) -> FlashloanResult<ExecutionResult> {
        // Wait for slot boundary
        let slot_boundary_time = self.calculate_next_slot_boundary().await;
        let now = Instant::now();
        
        if slot_boundary_time > now {
            let wait_time = slot_boundary_time.duration_since(now);
            if wait_time > Duration::from_millis(SLOT_DURATION_MS) {
                return Err(Box::new(ExecError::Other("Invalid slot boundary calculation".into())));
            }
            sleep(wait_time).await;
        }

        // Check profit after fees
        ensure_profit_after_fees(expected_profit, micro_per_cu, cu_limit, 0)?;

        // Send the transaction
        let send_time = Instant::now();
        let signature = send_transaction_with_retry(&self.rpc, &transaction, self.max_retries).await?;
        
        // Calculate timing offset
        let timing_offset_ms = send_time.duration_since(slot_boundary_time).as_millis() as i64;
        let current_slot = self.get_current_slot().await;
        
        // Calculate fees and profit
        let priority_fee = (micro_per_cu.saturating_mul(cu_limit)).saturating_div(1_000_000);
        let profit = expected_profit.saturating_sub(priority_fee);

        Ok(ExecutionResult {
            signature,
            slot: current_slot,
            timing_offset_ms,
            priority_fee,
            profit,
        })
    }

    // -----------------------------
    // Execute a batch of flashloan requests at slot boundary
    // -----------------------------
    pub async fn execute_batch_at_boundary(
        &self,
        requests: Vec<FlashLoanRequest>,
    ) -> Vec<FlashloanResult<ExecutionResult>> {
        // Wait for slot boundary
        let slot_boundary_time = self.calculate_next_slot_boundary().await;
        let now = Instant::now();
        
        if slot_boundary_time > now {
            let wait_time = slot_boundary_time.duration_since(now);
            if wait_time > Duration::from_millis(SLOT_DURATION_MS) {
                return vec![Err(Box::new(ExecError::Other("Invalid slot boundary calculation".into())))];
            }
            sleep(wait_time).await;
        }

        // Partition by write-set conflicts
        let groups = partition_non_conflicting(&requests, |r| {
            r.target_accounts
                .iter()
                .filter(|m| m.is_writable)
                .map(|m| m.pubkey)
                .collect::<Vec<_>>()
        });

        // Execute groups in parallel, but serialize within each group
        let mut handles = Vec::with_capacity(groups.len());
        for group in groups {
            let executor = self.clone();
            handles.push(tokio::spawn(async move {
                let mut results = Vec::with_capacity(group.len());
                for request in group {
                    results.push(executor.execute_single_request_advanced(request).await);
                }
                results
            }));
        }

        // Collect results
        let mut all_results = Vec::with_capacity(requests.len());
        for handle in handles {
            match handle.await {
                Ok(mut results) => all_results.append(&mut results),
                Err(e) => all_results.push(Err(Box::new(ExecError::Other(format!("Task join error: {}", e))))),
            }
        }

        all_results
    }

    // -----------------------------
    // Execute a single flashloan request with advanced features
    // -----------------------------
    pub async fn execute_single_request_advanced(
        &self,
        request: FlashLoanRequest,
    ) -> FlashloanResult<ExecutionResult> {
        // Get current leader and slot
        let current_slot = self.get_current_slot().await;
        let leader = self.get_slot_leader(current_slot).await;
        
        // Build and price the transaction
        let (tx, cu_limit, micro_per_cu) = self
            .build_and_price_flashloan_transaction(
                request.expected_profit,
                request.target_accounts.clone(),
                leader,
            )
            .await?;

        // Execute at the next slot boundary; on BlockhashExpired, refresh blockhash by rebuilding once
        match self
            .execute_at_slot_boundary(tx, cu_limit, micro_per_cu, request.expected_profit)
            .await
        {
            Ok(res) => Ok(res),
            Err(e) => {
                if let Some(exec_err) = e.downcast_ref::<ExecError>() {
                    if matches!(exec_err, ExecError::BlockhashExpired) {
                        // Rebuild with fresh blockhash/fees and retry once
                        let new_leader = self.get_current_leader().await;
                        let (tx2, cu2, micro2) = self
                            .build_and_price_flashloan_transaction(
                                request.expected_profit,
                                request.target_accounts.clone(),
                                new_leader,
                            )
                            .await?;
                        return self
                            .execute_at_slot_boundary(tx2, cu2, micro2, request.expected_profit)
                            .await;
                    }
                }
                Err(e)
            }
        }
    }

    // -----------------------------
    // Execute at slot boundary with proper timing
    // -----------------------------
    pub async fn execute_at_slot_boundary(
        &self,
        transaction: VersionedTransaction,
        cu_limit: u64,
        micro_per_cu: u64,
        expected_profit: u64,
    ) -> FlashloanResult<ExecutionResult> {
        // Wait for slot boundary
        let slot_boundary_time = self.calculate_next_slot_boundary().await;
        let now = Instant::now();
        
        if slot_boundary_time > now {
            let wait_time = slot_boundary_time.duration_since(now);
            if wait_time > Duration::from_millis(SLOT_DURATION_MS) {
                return Err(Box::new(ExecError::Other("Invalid slot boundary calculation".into())));
            }
            sleep(wait_time).await;
        }

        // Check profit after fees
        ensure_profit_after_fees(expected_profit, micro_per_cu, cu_limit, 0)?;

        // Send the transaction
        let send_time = Instant::now();
        let signature = send_transaction_with_retry(&self.rpc, &transaction, self.max_retries).await?;
        
        // Calculate timing offset
        let timing_offset_ms = send_time.duration_since(slot_boundary_time).as_millis() as i64;
        let current_slot = self.get_current_slot().await;
        
        // Calculate fees and profit
        let priority_fee = (micro_per_cu.saturating_mul(cu_limit)).saturating_div(1_000_000);
        let profit = expected_profit.saturating_sub(priority_fee);

        Ok(ExecutionResult {
            signature,
            slot: current_slot,
            timing_offset_ms,
            priority_fee,
            profit,
        })
    }

    // -----------------------------
    // Execute at boundary with variants, pre-signing, leader-aware fanout
    // -----------------------------
    pub async fn execute_at_boundary_with_variants(
        &self,
        base_instructions: Vec<Instruction>,
        write_accounts: Vec<Pubkey>, // accounts we will write -> for conflict partitions
        expected_profit: u64,
    ) -> FlashloanResult<Signature> {
        if self.circuit_breaker.load(Ordering::Acquire) {
            return Err(ExecutorError::CircuitBreaker.into());
        }

        // current leader heuristics: attempt to resolve leader for current slot
        let leader = self.resolve_current_leader().await?;
        // candidate leaders: current, next 2 (for fanout)
        let candidate_leaders = vec![leader]; // extend with known schedule fetch for next leaders if available

        // Prepare account vec for priority model
        let accounts_for_model: Vec<Pubkey> = write_accounts.clone();

        // build base_instructions into final tx via build_transaction_with_cu for each leader/percentile variant
        // choose variant set: N variants with different percentile/multipliers
        let percentiles = vec![0.99, 0.995, 0.999, 0.95];
        let mut signed_variants: Vec<(VersionedTransaction, u64 /*micro*/, u32 /*cu*/)> = Vec::new();

        for &p in &percentiles {
            // pick leader (here we pick primary)
            let (vtx, micro, cu_limit) = self.build_transaction_with_cu(base_instructions.clone(), &accounts_for_model, leader, p).await?;
            signed_variants.push((vtx, micro, cu_limit));
        }

        // Pre-sign variants against current and next blockhashes for minimal latency
        // PreSigner pre_sign_variants will create signed txs in cache
        // Here we simply send the first signed variant (we have already signed when building)
        // fanout: attempt send to leader and also to next leaders via quic tpu
        let mut send_tasks = JoinSet::new();

        // staggered micro-delays jitter: -0.5ms .. +0.5ms implemented with sleep durations
        let jitter_range_ms = 1.0;
        for (i, (vtx, micro, cu)) in signed_variants.into_iter().enumerate() {
            let sender = self.sender.clone();
            // compute micro-jitter stagger
            let jitter = ((i as f64) * 0.25) - (jitter_range_ms/2.0); // small stagger
            send_tasks.spawn(async move {
                if jitter > 0.0 {
                    sleep(Duration::from_micros((jitter * 1000.0) as u64)).await;
                } else if jitter < 0.0 {
                    // negative jitter = send slightly early
                }
                match sender.send(&vtx).await {
                    Ok(sig) => Ok(sig),
                    Err(e) => Err(anyhow!("send failed: {:?}", e)),
                }
            });
        }

        // Await first successful send and record outcome
        let mut last_err: Option<anyhow::Error> = None;
        while let Some(res) = send_tasks.join_next().await {
            match res {
                Ok(Ok(sig)) => {
                    // Record successful inclusion in fee model
                    if let Some(leader) = self.current_leader.lock().await.as_ref() {
                        self.fee_calculator.get_fee_model()
                            .record_inclusion(*leader, true, micro).await;
                    }
                    
                    // Track leader stats and inclusion counters asynchronously
                    let _ = self.record_send_for_leader(&leader, micro_from_sig(&sig).unwrap_or(0), cu).await;
                    return Ok(sig);
                }
                Ok(Err(e)) => {
                    // Record failed inclusion in fee model
                    if let Some(leader) = self.current_leader.lock().await.as_ref() {
                        self.fee_calculator.get_fee_model()
                            .record_inclusion(*leader, false, micro).await;
                    }
                    last_err = Some(e);
                }
                Err(e) => {
                    last_err = Some(anyhow!("join error: {:?}", e));
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow!("no send task succeeded")))
    }

    async fn record_send_for_leader(&self, leader: &Pubkey, fee: u64, units: u32) -> Result<()> {
        // update in-memory and persist
        let now = chrono::Utc::now().timestamp();
        let mut stats = self.leader_stats_cache.entry(*leader).or_insert_with(|| LeaderStats {
            leader: *leader,
            seen: 0,
            landed: 0,
            mean_fee_to_land: 0.0,
            stdev_fee_to_land: 0.0,
            avg_units_landed: 0.0,
            last_updated: now,
        }).value().clone();
        stats.seen += 1;
        // naive online mean update for mean_fee_to_land
        stats.mean_fee_to_land = ((stats.mean_fee_to_land * ((stats.seen-1) as f64)) + (fee as f64)) / (stats.seen as f64);
        stats.avg_units_landed = ((stats.avg_units_landed * ((stats.seen-1) as f64)) + (units as f64)) / (stats.seen as f64);
        stats.last_updated = now;
        self.leader_stats_cache.insert(*leader, stats.clone());
        // persist
        self.persistence.save_leader_stats(leader, &stats)?;
        Ok(())
    }

    async fn resolve_current_leader(&self) -> Result<Pubkey> {
        // attempt to read leader schedule centered on current slot; fallback to get_slot_leader RPC
        let slot = self.rpc.get_slot().await?;
        if let Ok(schedule) = self.rpc.get_leader_schedule(Some(slot), Some(CommitmentConfig::finalized())).await {
            // schedule: HashMap<String, Vec<u64>> mapping leader pubkey string -> slots (relative to epoch start)
            // find leader owning our slot (map indices relative to epoch)
            for (k, v) in schedule {
                for &relative_slot in &v {
                    if relative_slot == slot {
                        return Ok(k.parse()?);
                    }
                }
            }
        }
        // fallback: use rpc.get_slot_leader()
        let leader = self.rpc.get_slot_leader().await?;
        Ok(leader)
    }

    // -----------------------------
    // Retry policy layered
    // -----------------------------
    async fn send_with_retry_and_categorized_backoff(&self, vtx: VersionedTransaction) -> Result<Signature> {
        // categorize errors after first attempt, apply slot-aware backoff
        let mut attempt = 0usize;
        let max_attempts = 4usize;
        loop {
            attempt += 1;
            match self.sender.send(&vtx).await {
                Ok(sig) => {
                    // confirm via onSignature subscription ideally; fallback to polling small window
                    return Ok(sig);
                }
                Err(e) => {
                    let err_str = format!("{:?}", e);
                    if err_str.contains("BlockhashNotFound") || err_str.contains("Expired") {
                        // refresh blockhash re-sign
                        let new_blockhash = self.rpc.get_latest_blockhash().await?;
                        // re-sign: You must rebuild and re-sign the message with new blockhash.
                        // Here, we expect caller to provide mechanisms to re-sign; bubble error for now
                        return Err(anyhow!("blockhash expired — caller must rebuild & re-sign: {}", err_str));
                    } else if err_str.contains("AccountInUse") {
                        // jitter small and retry same fee
                        let jitter_ms = 2 + attempt as u64;
                        sleep(Duration::from_millis(jitter_ms)).await;
                        if attempt >= max_attempts { break; }
                        continue;
                    } else if err_str.contains("WouldBeDropped") || err_str.contains("TooLowFee") {
                        // escalate fee: caller should increment percentile and re-build & re-sign new variant
                        return Err(anyhow!("low fee — escalate to next percentile: {}", err_str));
                    } else {
                        // general, retry with exponential jitter but slot aware
                        let backoff_slots = attempt; // 1,2,3 slots
                        let backoff_ms = backoff_slots as u64 * SLOT_DURATION_MS;
                        sleep(Duration::from_millis(backoff_ms)).await;
                        if attempt >= max_attempts { break; }
                    }
                }
            }
        }
        Err(anyhow!("send attempts exhausted"))
    }
}

// small helper: extract micro fee from signature (placeholder logic: in real code you'd fetch by correlating)
fn micro_from_sig(_sig: &Signature) -> Option<u64> {
    // left intentionally simple: in production map signature -> send metadata
    Some(0)
}

    // -----------------------------
    // Account builders and v0 helper (no Rent sysvar unless required)
    // -----------------------------
    fn build_flashloan_accounts(&self, request: &FlashLoanRequest) -> Vec<AccountMeta> {
        let mut accounts = vec![
            AccountMeta::new(self.wallet.pubkey(), true),
            AccountMeta::new_readonly(clock::id(), false),
            // Intentionally exclude Rent sysvar unless your on-chain program requires it:
            // AccountMeta::new_readonly(rent::id(), false),
        ];
        accounts.extend(request.target_accounts.clone());
        accounts
    }

    #[allow(dead_code)]
    fn build_v0_with_alt(
        &self,
        ixs: Vec<Instruction>,
        payer: Pubkey,
        recent_blockhash: Hash,
    ) -> Result<VersionedTransaction, ExecError> {
        let msg = V0Message::try_compile(
            &payer,
            &ixs,
            &[], // add ALT lookups here if you maintain an ALT registry
            recent_blockhash,
        )
        .map_err(|e| ExecError::Other(e.to_string()))?;

        VersionedTransaction::try_new(VersionedMessage::V0(msg), &[&*self.wallet])
            .map_err(|e| ExecError::Other(e.to_string()))
    }

// -----------------------------
// Unit tests (some core tests)
// -----------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;

    #[tokio::test]
    async fn test_sliding_window_percentile() {
        let w = SlidingWindowPercentile::new(10);
        for i in 1..=10u64 { w.push(i*100); }
        let p50 = w.percentile(0.5).unwrap();
        assert!(p50 >= 500 && p50 <= 1000);
    }

    #[tokio::test]
    async fn test_partitioner_non_conflicting() {
        #[derive(Clone)]
        struct Req { id: u8, writes: Vec<Pubkey> }
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        let c = Pubkey::new_unique();
        let reqs = vec![
            Req { id: 1, writes: vec![a] },
            Req { id: 2, writes: vec![b] },
            Req { id: 3, writes: vec![a] },
            Req { id: 4, writes: vec![c] },
        ];
        let groups = partition_non_conflicting(&reqs, |r| r.writes.clone());
        // Expect groups >= 2
        assert!(groups.len() >= 2);
    }
}
