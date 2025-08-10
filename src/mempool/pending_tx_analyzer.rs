//! Pending transaction analyzer for MEV/mempool processing on Solana mainnet.
//! 
//! This module provides:
//! - Hedged RPC pool for low-latency reads across multiple endpoints
//! - Transaction deduplication using ring-based cuckoo filters
//! - Compute budget parsing and fee estimation
//! - Address lookup table (ALT) resolution
//! - Priority queue for simulation job scheduling
//! - Pluggable simulation engines for different DEX protocols

use arc_swap::ArcSwap;
use async_trait::async_trait;
use borsh::BorshDeserialize;
use cuckoofilter::CuckooFilter;
use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use futures::future::select_all;
use moka::future::Cache as MokaCache;
use ordered_float::OrderedFloat;
use parking_lot::Mutex as SyncMutex;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use solana_address_lookup_table_program::state::AddressLookupTable as OnchainALT;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    address_lookup_table::AddressLookupTableAccount,
    account::Account,
    clock::Slot,
    commitment_config::CommitmentConfig,
    compute_budget::{self, ComputeBudgetInstruction},
    hash::Hash,
    instruction::CompiledInstruction,
    message::{v0::MessageAddressTableLookup, Message, VersionedMessage},
    pubkey::Pubkey,
    signature::Signature,
    transaction::VersionedTransaction,
};
use spl_token::state::Mint as TokenMint;
use std::{
    collections::BinaryHeap,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::{
    sync::{mpsc, Mutex, RwLock, Semaphore},
    task::JoinSet,
    time::timeout,
};
use tracing::{debug, error, info, instrument, span, warn, Level};
use xxhash_rust::xxh3::xxh3_64;

// Type aliases for clarity
type FastHashMap<K, V> = FxHashMap<K, V>;

// ===== Configuration Constants =====

// Deduplication parameters
const DEDUP_RING_COUNT: usize = 8;
const DEDUP_ITEMS_PER_RING: usize = 128_000;  // ~1M total capacity
const DEDUP_FP_RATE: f64 = 0.001;  // 0.1% false positive target
const DEDUP_NEGATIVE_CACHE_MAX: usize = 10_000;
const DEDUP_NEGATIVE_CACHE_TTL_SECS: u64 = 5;

// Queue configuration
const QUEUE_SHARDS: usize = 8;
const MAX_JOBS_PER_SHARD: usize = 500;  // 4000 total capacity
const QUEUE_EXPIRY_SLOTS: u64 = 150;
const JOB_EXPIRY_MS: u64 = 5000;
const MAX_PENDING_TX_CACHE: usize = 10_000;
const QUEUE_DROP_THRESHOLD: f64 = 0.95;  // Drop when 95% full

// ALT fetching - single source of truth
const ALT_FETCH_CONCURRENCY: usize = 4;
const ALT_BATCH_SIZE: usize = 100;
const ALT_CACHE_TTL_SECS: u64 = 600;
const ALT_NEGATIVE_CACHE_TTL_SECS: u64 = 30;
const ALT_CACHE_MAX_SIZE: u64 = 10_000;

// RPC configuration
const RPC_DEFAULT_TIMEOUT_MS: u64 = 500;
const RPC_HEDGED_MIN_ENDPOINTS: usize = 2;
const RPC_HEALTH_DECAY_FACTOR: f64 = 0.95;

// Global tiebreaker for priority queue
static TIEBREAK: AtomicU64 = AtomicU64::new(0);

// ===== Error Types =====

#[derive(Error, Debug)]
pub enum AnalyzerError {
    #[error("RPC error: {0}")]
    Rpc(String),
    
    #[error("Invalid instruction data: {0}")]
    InvalidInstruction(String),
    
    #[error("ALT resolution failed: {0}")]
    AltResolution(String),
    
    #[error("Queue full for slot {slot}")]
    QueueFull { slot: Slot },
    
    #[error("Duplicate transaction: {sig}")]
    DuplicateTransaction { sig: Signature },
    
    #[error("Timeout after {duration_ms}ms")]
    Timeout { duration_ms: u64 },
}

// ===== Metrics =====

/// Metrics for individual RPC endpoints
#[derive(Debug, Default)]
struct RpcEndpointMetrics {
    latency_ema_ms: AtomicU64,  // Exponential moving average
    success_count: AtomicU64,
    error_count: AtomicU64,
    win_count: AtomicU64,
    last_error_time: AtomicU64,  // Unix timestamp
    consecutive_errors: AtomicU64,
}

impl RpcEndpointMetrics {
    fn update_latency(&self, latency_ms: u64) {
        let old = self.latency_ema_ms.load(Ordering::Relaxed);
        let new = ((old as f64 * RPC_HEALTH_DECAY_FACTOR) + 
                   (latency_ms as f64 * (1.0 - RPC_HEALTH_DECAY_FACTOR))) as u64;
        self.latency_ema_ms.store(new, Ordering::Relaxed);
    }
    
    fn health_score(&self) -> f64 {
        let successes = self.success_count.load(Ordering::Relaxed) as f64;
        let errors = self.error_count.load(Ordering::Relaxed) as f64;
        let total = successes + errors;
        if total < 10.0 { return 0.5; }  // Not enough data
        
        let success_rate = successes / total;
        let latency_factor = 1000.0 / (self.latency_ema_ms.load(Ordering::Relaxed) as f64 + 1.0);
        let recent_errors = self.consecutive_errors.load(Ordering::Relaxed) as f64;
        let error_penalty = 1.0 / (1.0 + recent_errors * 0.1);
        
        success_rate * latency_factor * error_penalty
    }
}

/// Per-slot operational statistics
#[derive(Debug, Default)]
struct SlotOperationalStats {
    processed: AtomicU64,
    opportunities: AtomicU64,
    total_ev_lamports: AtomicU64,
    avg_latency_ms: AtomicU64,
    duplicates: AtomicU64,
    expired: AtomicU64,
}

// ===== Configuration =====

/// Runtime configuration for the analyzer
#[derive(Debug, Clone)]
pub struct RuntimeCfg {
    // Simulation parameters
    pub simulation_timeout_ms: u64,
    pub max_concurrent_sims: usize,
    pub max_retries: u32,
    
    // Economic thresholds
    pub profit_threshold_lamports: u64,
    pub min_roi: f64,
    pub max_priority_fee: u64,
    pub max_cu_estimate: u64,
    
    // Jito bundle support
    pub enable_jito_tip: bool,
    pub jito_tip_lamports: u64,
    
    // Queue management
    pub queue_expiry_slots: u64,
    pub queue_drop_threshold: f64,
}

impl Default for RuntimeCfg {
    fn default() -> Self {
        Self {
            simulation_timeout_ms: 500,
            max_concurrent_sims: 16,
            max_retries: 3,
            profit_threshold_lamports: 1_000_000,
            min_roi: 1.05,
            max_priority_fee: 1_000_000,
            max_cu_estimate: 1_400_000,
            enable_jito_tip: false,
            jito_tip_lamports: 1000,
            queue_expiry_slots: QUEUE_EXPIRY_SLOTS,
            queue_drop_threshold: QUEUE_DROP_THRESHOLD,
        }
    }
}

// ===== RPC Pool =====

/// Hedged RPC pool with adaptive endpoint selection
struct RpcPool {
    clients: Vec<Arc<RpcClient>>,
    metrics: Vec<Arc<RpcEndpointMetrics>>,
    commitment: CommitmentConfig,
    semaphore: Arc<Semaphore>,
}

impl RpcPool {
    fn new(endpoints: Vec<String>, commitment: CommitmentConfig) -> Self {
        let clients = endpoints
            .into_iter()
            .map(|url| Arc::new(RpcClient::new_with_commitment(url, commitment)))
            .collect::<Vec<_>>();
        
        let metrics = (0..clients.len())
            .map(|_| Arc::new(RpcEndpointMetrics::default()))
            .collect();
        
        let semaphore = Arc::new(Semaphore::new(RPC_HEDGED_MIN_ENDPOINTS * 2));
        
        Self { clients, metrics, commitment, semaphore }
    }
    
    /// Select best endpoints based on health scores
    fn select_best_endpoints(&self, count: usize) -> Vec<usize> {
        let mut scores: Vec<(usize, f64)> = self.metrics
            .iter()
            .enumerate()
            .map(|(i, m)| (i, m.health_score()))
            .collect();
        
        scores.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        scores.into_iter().take(count).map(|(i, _)| i).collect()
    }

    #[instrument(skip(self), fields(pubkey = %pk))]
    async fn get_account_hedged(&self, pk: &Pubkey, tmo: Duration) -> Option<Account> {
        let best_endpoints = self.select_best_endpoints(RPC_HEDGED_MIN_ENDPOINTS);
        
        let futures = best_endpoints.into_iter().map(|idx| {
            let client = Arc::clone(&self.clients[idx]);
            let metric = Arc::clone(&self.metrics[idx]);
            let pk = *pk;
            let permit = self.semaphore.clone().acquire_owned();
            
            Box::pin(async move {
                let _permit = permit.await.ok()?;
                let start = Instant::now();
                
                let res = timeout(tmo, client.get_account(&pk)).await;
                
                let elapsed = start.elapsed().as_millis() as u64;
                match res {
                    Ok(Ok(acc)) => {
                        metric.update_latency(elapsed);
                        metric.success_count.fetch_add(1, Ordering::Relaxed);
                        metric.consecutive_errors.store(0, Ordering::Relaxed);
                        Some((idx, Some(acc)))
                    },
                    Ok(Err(e)) => {
                        debug!("RPC error getting account: {}", e);
                        metric.error_count.fetch_add(1, Ordering::Relaxed);
                        metric.consecutive_errors.fetch_add(1, Ordering::Relaxed);
                        metric.last_error_time.store(
                            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                            Ordering::Relaxed
                        );
                        Some((idx, None))
                    },
                    Err(_) => {
                        metric.error_count.fetch_add(1, Ordering::Relaxed);
                        metric.consecutive_errors.fetch_add(1, Ordering::Relaxed);
                        None
                    }
                }
            })
        }).collect::<Vec<_>>();

        let ((winner_idx, result), _, _) = select_all(futures).await;
        if result.is_some() {
            let latency = start.elapsed().as_millis() as u64;
            self.metrics[winner_idx].latency_ms.store(latency, Ordering::Relaxed);
            self.metrics[winner_idx].win_count.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    #[instrument(skip(self, keys), fields(count = keys.len()))]
    async fn get_multiple_accounts_hedged(&self, keys: &[Pubkey], tmo: Duration) -> Vec<Option<Account>> {
        let best_endpoints = self.select_best_endpoints(RPC_HEDGED_MIN_ENDPOINTS);
        
        let futures = best_endpoints.into_iter().map(|idx| {
            let client = Arc::clone(&self.clients[idx]);
            let metric = Arc::clone(&self.metrics[idx]);
            let keys = keys.to_vec();
            let permit = self.semaphore.clone().acquire_owned();
            
            Box::pin(async move {
                let _permit = permit.await.ok()?;
                let start = Instant::now();
                
                let res = timeout(tmo, client.get_multiple_accounts(&keys)).await;
                
                let elapsed = start.elapsed().as_millis() as u64;
                match res {
                    Ok(Ok(accounts)) => {
                        metric.update_latency(elapsed);
                        metric.success_count.fetch_add(1, Ordering::Relaxed);
                        metric.consecutive_errors.store(0, Ordering::Relaxed);
                        Some((idx, accounts))
                    },
                    Ok(Err(e)) => {
                        debug!("RPC error getting multiple accounts: {}", e);
                        metric.error_count.fetch_add(1, Ordering::Relaxed);
                        metric.consecutive_errors.fetch_add(1, Ordering::Relaxed);
                        None
                    },
                    Err(_) => {
                        metric.error_count.fetch_add(1, Ordering::Relaxed);
                        metric.consecutive_errors.fetch_add(1, Ordering::Relaxed);
                        None
                    }
                }
            })
        }).collect::<Vec<_>>();
        
        if futures.is_empty() {
            return vec![None; keys.len()];
        }
        
        let (res, winner_idx, _remaining) = select_all(futures).await;
        
        if let Some((idx, accounts)) = res {
            self.metrics[idx].win_count.fetch_add(1, Ordering::Relaxed);
            return accounts;
        }
        
        vec![None; keys.len()]
    }

    async fn get_slot_hedged(&self, tmo: Duration) -> Option<Slot> {
        let best_endpoints = self.select_best_endpoints(RPC_HEDGED_MIN_ENDPOINTS);
        
        let futures = best_endpoints.into_iter().map(|idx| {
            let client = Arc::clone(&self.clients[idx]);
            let metric = Arc::clone(&self.metrics[idx]);
            
            Box::pin(async move {
                let start = Instant::now();
                let res = timeout(tmo, client.get_slot()).await;
                
                let elapsed = start.elapsed().as_millis() as u64;
                match res {
                    Ok(Ok(slot)) => {
                        metric.update_latency(elapsed);
                        metric.success_count.fetch_add(1, Ordering::Relaxed);
                        Some((idx, slot))
                    },
                    _ => {
                        metric.error_count.fetch_add(1, Ordering::Relaxed);
                        None
                    }
                }
            })
        }).collect::<Vec<_>>();
        
        if futures.is_empty() {
            return None;
        }
        
        let (res, _, _) = select_all(futures).await;
        res.map(|(_, slot)| slot)
    }

    async fn get_fee_for_message(&self, msg: &Message, tmo: Duration) -> Option<u64> {
        let best_endpoints = self.select_best_endpoints(RPC_HEDGED_MIN_ENDPOINTS);
        
        let futures = best_endpoints.into_iter().map(|idx| {
            let client = Arc::clone(&self.clients[idx]);
            let metric = Arc::clone(&self.metrics[idx]);
            let msg = msg.clone();
            
            Box::pin(async move {
                let start = Instant::now();
                let res = timeout(tmo, client.get_fee_for_message(&msg)).await;
                
                let elapsed = start.elapsed().as_millis() as u64;
                match res {
                    Ok(Ok(fee)) => {
                        metric.update_latency(elapsed);
                        metric.success_count.fetch_add(1, Ordering::Relaxed);
                        Some((idx, fee))
                    },
                    _ => {
                        metric.error_count.fetch_add(1, Ordering::Relaxed);
                        None
                    }
                }
            })
        }).collect::<Vec<_>>();
        
        if futures.is_empty() {
            return None;
        }
        
        let (res, _, _) = select_all(futures).await;
        res.map(|(_, fee)| fee)
    }
}

// ===== Deduplication =====

/// Ring-based cuckoo filter for transaction deduplication
struct RingCuckoo {
    rings: Vec<SyncMutex<CuckooFilter<DefaultHasher>>>,
    current_ring: AtomicUsize,
    last_reset_slot: AtomicU64,
    negative_cache: MokaCache<Signature, ()>,
    metrics: Arc<DeduplicationMetrics>,
}

#[derive(Debug, Default)]
struct DeduplicationMetrics {
    insertions: AtomicU64,
    duplicates: AtomicU64,
    ring_resets: AtomicU64,
    negative_cache_hits: AtomicU64,
    false_positives: AtomicU64,
}

impl RingCuckoo {
    fn new() -> Self {
        let mut rings = Vec::with_capacity(DEDUP_RING_COUNT);
        for _ in 0..DEDUP_RING_COUNT {
            let filter = CuckooFilter::with_capacity(DEDUP_ITEMS_PER_RING);
            rings.push(SyncMutex::new(filter));
        }
        
        let negative_cache = MokaCache::builder()
            .max_capacity(DEDUP_NEGATIVE_CACHE_MAX as u64)
            .time_to_live(Duration::from_secs(DEDUP_NEGATIVE_CACHE_TTL_SECS))
            .build();
        
        Self {
            rings,
            current_ring: AtomicUsize::new(0),
            last_reset_slot: AtomicU64::new(0),
            negative_cache,
            metrics: Arc::new(DeduplicationMetrics::default()),
        }
    }
    
    /// Try to insert a signature. Returns true if new (inserted), false if duplicate.
    async fn try_insert(&self, sig: &Signature, slot: Slot) -> bool {
        // Check negative cache first
        if self.negative_cache.get(sig).await.is_some() {
            self.metrics.negative_cache_hits.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        
        // Rotate rings if needed (every 150 slots)
        let last_reset = self.last_reset_slot.load(Ordering::Relaxed);
        if slot.saturating_sub(last_reset) > 150 {
            self.rotate_rings(slot);
        }
        
        // Check all rings for existing signature
        for ring in &self.rings {
            let filter = ring.lock();
            if filter.contains(sig) {
                self.metrics.duplicates.fetch_add(1, Ordering::Relaxed);
                return false;  // Duplicate found
            }
        }
        
        // Try to insert into current ring
        let current_idx = self.current_ring.load(Ordering::Relaxed);
        let mut filter = self.rings[current_idx].lock();
        
        match filter.add(sig) {
            Ok(_) => {
                self.metrics.insertions.fetch_add(1, Ordering::Relaxed);
                true  // Successfully inserted (new transaction)
            },
            Err(_) => {
                // Filter is full, add to negative cache
                self.negative_cache.insert(*sig, ()).await;
                self.metrics.false_positives.fetch_add(1, Ordering::Relaxed);
                false  // Couldn't insert, treat as duplicate
            }
        }
    }
    
    fn rotate_rings(&self, slot: Slot) {
        let current = self.current_ring.load(Ordering::Relaxed);
        let next = (current + 1) % DEDUP_RING_COUNT;
        
        // Clear the next ring
        let mut filter = self.rings[next].lock();
        *filter = CuckooFilter::with_capacity(DEDUP_ITEMS_PER_RING);
        drop(filter);
        
        self.current_ring.store(next, Ordering::Relaxed);
        self.last_reset_slot.store(slot, Ordering::Relaxed);
        self.metrics.ring_resets.fetch_add(1, Ordering::Relaxed);
    }
}

// ===== Core Data Types =====

/// A pending transaction ready for simulation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingTransaction {
    pub sig: Signature,
    pub tx: Arc<VersionedTransaction>,
    pub received_at_millis: u64,  // Unix timestamp in millis
    pub slot: Slot,
    pub priority_fee: u64,
    pub compute_units: u64,
    pub opportunity_type: OpportunityType,
    pub dex_program: DexProgram,
    #[serde(skip)]
    pub received_at: Option<Instant>,  // For internal timing
}

/// Opportunity types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OpportunityType {
    Arbitrage,
    Liquidation,
    Frontrun,
    Backrun,
    Sandwich,
    JitLiquidity,
    AtomicArb,
    Unknown,
}

/// MEV opportunity data
#[derive(Debug, Clone)]
pub struct MEVOpportunity {
    pub sig: Signature,
    pub opportunity_type: OpportunityType,
    pub expected_profit: i64,
    pub probability: f64,
    pub gas_cost: u64,
    pub priority_fee: u64,
    pub timestamp: Instant,
    pub dex_programs: Vec<DexProgram>,
}

/// DEX program identifiers
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DexProgram {
    RaydiumAmmV4,
    OrcaWhirlpool,
    SerumV3,
    Unknown(Pubkey),
}

/// A simulation job with priority
#[derive(Debug, Clone)]
pub struct SimJob {
    pub sig: Signature,
    pub tx: Arc<VersionedTransaction>,
    pub received_time: Instant,
    pub submission_slot: Slot,
    pub attempts: u8,
    pub priority: OrderedFloat<f64>,
    pub tiebreak: u64,  // Renamed for consistency
    pub expected_value: i64,
    pub probability_success: f64,
    pub estimated_ms: u64,
    pub est_gas_cost: u64,
    pub confidence: f64,
}

impl SimJob {
    fn is_expired(&self, current_slot: Slot) -> bool {
        current_slot.saturating_sub(self.submission_slot) > QUEUE_EXPIRY_SLOTS ||
        self.received_time.elapsed() > Duration::from_millis(JOB_EXPIRY_MS)
    }
    
    fn ev_per_ms(&self) -> f64 {
        if self.estimated_ms == 0 || self.probability_success <= 0.0 {
            return 0.0;
        }
        (self.expected_value as f64 * self.probability_success) / self.estimated_ms as f64
    }
}

impl PartialEq for SimJob {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.tiebreak == other.tiebreak
    }
}

impl Eq for SimJob {}

impl PartialOrd for SimJob {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SimJob {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Higher priority first
        match self.priority.cmp(&other.priority) {
            std::cmp::Ordering::Equal => {
                // Lower tiebreaker first (earlier transactions)
                other.tiebreak.cmp(&self.tiebreak)
            },
            other => other,
        }
    }
}

/// Sharded priority queue for simulation jobs
pub struct ShardedSimQueue {
    shards: Vec<Arc<SyncMutex<BinaryHeap<SimJob>>>>,
    sizes: Vec<AtomicUsize>,
    total_size: AtomicUsize,
    metrics: Arc<QueueMetrics>,
}

#[derive(Debug, Default)]
struct QueueMetrics {
    pushed: AtomicU64,
    popped: AtomicU64,
    expired: AtomicU64,
    dropped_full: AtomicU64,
}

impl ShardedSimQueue {
    pub fn new() -> Self {
        let num_shards = std::cmp::min(num_cpus::get(), 16);
        let mut shards = Vec::with_capacity(num_shards);
        let mut sizes = Vec::with_capacity(num_shards);
        
        for _ in 0..num_shards {
            shards.push(Arc::new(SyncMutex::new(BinaryHeap::new())));
            sizes.push(AtomicUsize::new(0));
        }
        
        Self {
            shards,
            sizes,
            total_size: AtomicUsize::new(0),
            metrics: Arc::new(QueueMetrics::default()),
        }
    }
    
    fn push(&self, sig: &Signature, job: SimJob) -> Result<(), AnalyzerError> {
        let total = self.total_size.load(Ordering::Relaxed);
        if total >= QUEUE_SHARDS * MAX_JOBS_PER_SHARD {
            self.metrics.dropped_full.fetch_add(1, Ordering::Relaxed);
            return Err(AnalyzerError::QueueFull { slot: job.submission_slot });
        }
        
        let shard_idx = (xxh3_64(sig.as_ref()) as usize) % QUEUE_SHARDS;
        let mut heap = self.shards[shard_idx].lock();
        
        if heap.len() >= MAX_JOBS_PER_SHARD {
            // Drop lowest priority job if at capacity
            if let Some(min_job) = heap.iter().min() {
                if job.priority > min_job.priority {
                    // Find and remove the minimum priority job
                    let jobs: Vec<_> = heap.drain().collect();
                    for j in jobs {
                        if j.priority != min_job.priority || j.tiebreaker != min_job.tiebreaker {
                            heap.push(j);
                        }
                    }
                    heap.push(job);
                    self.metrics.pushed.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.metrics.dropped_full.fetch_add(1, Ordering::Relaxed);
                    return Err(AnalyzerError::QueueFull { slot: job.submission_slot });
                }
            }
        } else {
            heap.push(job);
            self.sizes[shard_idx].fetch_add(1, Ordering::Relaxed);
            self.total_size.fetch_add(1, Ordering::Relaxed);
            self.metrics.pushed.fetch_add(1, Ordering::Relaxed);
        }
        
        Ok(())
    }
    
    fn pop_best(&self, current_slot: Slot) -> Option<SimJob> {
        let mut best_job: Option<SimJob> = None;
        let mut best_shard_idx = 0;
        
        // Find the best job across all shards
        for (idx, shard) in self.shards.iter().enumerate() {
            let mut heap = shard.lock();
            
            // Remove expired jobs
            let mut temp = Vec::new();
            while let Some(job) = heap.pop() {
                if !job.is_expired(current_slot) {
                    temp.push(job);
                    break;
                } else {
                    self.metrics.expired.fetch_add(1, Ordering::Relaxed);
                }
            }
            
            // Put back the non-expired job and check if it's the best
            if let Some(job) = temp.pop() {
                if best_job.as_ref().map_or(true, |b| job.priority > b.priority) {
                    if let Some(prev_best) = best_job {
                        // Put the previous best back
                        let mut prev_heap = self.shards[best_shard_idx].lock();
                        prev_heap.push(prev_best);
                    }
                    best_job = Some(job.clone());
                    best_shard_idx = idx;
                } else {
                    heap.push(job);
                }
            }
        }
        
        if best_job.is_some() {
            self.sizes[best_shard_idx].fetch_sub(1, Ordering::Relaxed);
            self.total_size.fetch_sub(1, Ordering::Relaxed);
            self.metrics.popped.fetch_add(1, Ordering::Relaxed);
        }
        
        best_job
    }
    
    fn size(&self) -> usize {
        self.total_size.load(Ordering::Relaxed)
    }
}

// ===== Circuit Breaker (cont'd) =====

/// Parse compute budget instructions from a transaction
fn parse_compute_budget(
    accounts: &[Pubkey],
    instructions: &[CompiledInstruction],
) -> Result<(u64, u64), AnalyzerError> {
    let mut compute_units = 200_000u64;  // Default
    let mut priority_fee = 0u64;
    
    let compute_budget_id = compute_budget::id();
    
    for ix in instructions {
        // Check if this instruction is for the compute budget program
        let program_id_idx = ix.program_id_index as usize;
        if program_id_idx >= accounts.len() {
            continue;
        }
        
        if accounts[program_id_idx] != compute_budget_id {
            continue;
        }
        
        // Try to deserialize the instruction
        match ComputeBudgetInstruction::try_from_slice(&ix.data) {
            Ok(ComputeBudgetInstruction::SetComputeUnitLimit(limit)) => {
                compute_units = limit as u64;
            },
            Ok(ComputeBudgetInstruction::SetComputeUnitPrice(price)) => {
                priority_fee = price;
            },
            Ok(ComputeBudgetInstruction::RequestHeapFrame(_)) => {
                // Track heap frame if needed for simulation
            },
            Ok(ComputeBudgetInstruction::SetLoadedAccountsDataSizeLimit(_)) => {
                // Track data size limit if needed
            },
            _ => {
                // Unknown or deprecated instruction
            }
        }
    }
    
    Ok((compute_units, priority_fee))
}

/// Calculate gas cost in lamports
fn gas_cost_lamports(compute_units: u64, priority_fee: u64) -> u64 {
    compute_units.saturating_mul(priority_fee).saturating_div(1_000_000)
}

/// Calculate gas cost including base fee
fn gas_cost_lamports_with_base(compute_units: u64, priority_fee: u64, signatures: u64) -> u64 {
    let priority_cost = gas_cost_lamports(compute_units, priority_fee);
    let base_fee = signatures.saturating_mul(5_000);
    base_fee.saturating_add(priority_cost)
}

/// Check if fee is acceptable given estimates
fn acceptable_fee_with_estimate(
    gas_cost: u64,
    expected_value: u64,
    min_roi: f64,
    profit_threshold: u64,
) -> bool {
    if expected_value <= gas_cost {
        return false;
    }
    
    let profit = expected_value.saturating_sub(gas_cost);
    if profit < profit_threshold {
        return false;
    }
    
    let roi = expected_value as f64 / (gas_cost as f64 + 1.0);
    roi >= min_roi
}

// ===== ALT Resolution =====

/// Address lookup table cache entry
#[derive(Clone, Debug)]
struct AltCacheEntry {
    alt: Option<AddressLookupTableAccount>,
    fetched_at: Instant,
    slot: Slot,
}

impl AltCacheEntry {
    fn is_valid(&self, ttl: Duration) -> bool {
        self.fetched_at.elapsed() < ttl
    }
}

// ===== Simulation Engines =====

/// Trait for DEX-specific simulation engines
#[async_trait]
trait SimulationEngine: Send + Sync {
    async fn simulate(
        &self,
        tx: &VersionedTransaction,
        accounts: &[Option<Account>],
    ) -> Result<SimulationResult, AnalyzerError>;
    
    fn can_handle(&self, program_id: &Pubkey) -> bool;
}

/// Result from a simulation
#[derive(Debug, Clone)]
struct SimulationResult {
    success: bool,
    profit_lamports: u64,
    gas_used: u64,
    logs: Vec<String>,
    confidence: f64,
}

/// Raydium AMM simulation engine
struct RaydiumSimulationEngine {
    program_id: Pubkey,
}

impl RaydiumSimulationEngine {
    fn new() -> Self {
        Self {
            program_id: Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap(),
        }
    }
}

#[async_trait]
impl SimulationEngine for RaydiumSimulationEngine {
    async fn simulate(
        &self,
        _tx: &VersionedTransaction,
        _accounts: &[Option<Account>],
    ) -> Result<SimulationResult, AnalyzerError> {
        // TODO: Implement actual Raydium simulation logic
        // For now, return placeholder
        Ok(SimulationResult {
            success: false,
            profit_lamports: 0,
            gas_used: 200_000,
            logs: vec![],
            confidence: 0.0,
        })
    }
    
    fn can_handle(&self, program_id: &Pubkey) -> bool {
        program_id == &self.program_id
    }
}

/// Orca Whirlpool simulation engine  
struct OrcaSimulationEngine {
    program_id: Pubkey,
}

impl OrcaSimulationEngine {
    fn new() -> Self {
        Self {
            program_id: Pubkey::from_str("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc").unwrap(),
        }
    }
}

#[async_trait]
impl SimulationEngine for OrcaSimulationEngine {
    async fn simulate(
        &self,
        _tx: &VersionedTransaction,
        _accounts: &[Option<Account>],
    ) -> Result<SimulationResult, AnalyzerError> {
        // TODO: Implement actual Orca simulation logic
        Ok(SimulationResult {
            success: false,
            profit_lamports: 0,
            gas_used: 200_000,
            logs: vec![],
            confidence: 0.0,
        })
    }
    
    fn can_handle(&self, program_id: &Pubkey) -> bool {
        program_id == &self.program_id
    }
}

// ===== Adaptive Semaphores =====

struct Semaphores {
    high_priority: Arc<Semaphore>,
    low_priority: Arc<Semaphore>,
}

impl Semaphores {
    fn new(hi_permits: usize, lo_permits: usize) -> Self {
        Self {
            high_priority: Arc::new(Semaphore::new(hi_permits)),
            low_priority: Arc::new(Semaphore::new(lo_permits)),
        }
    }
}

// ===== Pool State and Slot Stats =====

/// AMM pool state for caching
#[derive(Debug, Clone)]
pub struct AmmPoolState {
    pub pool_id: Pubkey,
    pub token_a: Pubkey,
    pub token_b: Pubkey,
    pub reserve_a: u64,
    pub reserve_b: u64,
    pub fee_numerator: u64,
    pub fee_denominator: u64,
    pub last_update_slot: Slot,
    pub protocol: DexProgram,
}

/// Slot-level statistics for circuit breaker
#[derive(Debug, Default)]
pub struct SlotStats {
    pub last_slot: Slot,
    pub successes: AtomicU64,
    pub failures: AtomicU64,
    pub total_profit: AtomicI64,
    pub total_gas: AtomicU64,
}

impl SlotStats {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn record(&mut self, slot: Slot, success: bool) {
        if slot != self.last_slot {
            // Reset for new slot
            self.last_slot = slot;
            self.successes.store(0, Ordering::Relaxed);
            self.failures.store(0, Ordering::Relaxed);
            self.total_profit.store(0, Ordering::Relaxed);
            self.total_gas.store(0, Ordering::Relaxed);
        }
        
        if success {
            self.successes.fetch_add(1, Ordering::Relaxed);
        } else {
            self.failures.fetch_add(1, Ordering::Relaxed);
        }
    }
    
    pub fn failure_rate(&self) -> f64 {
        let total = self.successes.load(Ordering::Relaxed) + self.failures.load(Ordering::Relaxed);
        if total == 0 {
            return 0.0;
        }
        self.failures.load(Ordering::Relaxed) as f64 / total as f64
    }
}

/// Check if we should back off based on slot stats
fn should_backoff(stats: &SlotStats, current_slot: Slot) -> bool {
    if stats.last_slot != current_slot {
        return false; // New slot, no backoff
    }
    
    let failures = stats.failures.load(Ordering::Relaxed);
    let successes = stats.successes.load(Ordering::Relaxed);
    let total = failures + successes;
    
    // Back off if failure rate > 50% and we have enough samples
    total >= 10 && failures > successes
}

/// Helper function for dynamic gas cost calculation
fn gas_cost_lamports_dyn(
    base_fee: u64,
    priority_fee_micro: u64,
    compute_units: u64,
    tip_lamports: u64,
) -> u64 {
    base_fee
        .saturating_add((priority_fee_micro.saturating_mul(compute_units)) / 1_000_000)
        .saturating_add(tip_lamports)
}

// ===== Helper Functions =====

/// Calculate expected value per millisecond
fn ev_per_ms(expected_value: i64, estimated_ms: u64, probability: f64) -> f64 {
    if estimated_ms == 0 {
        return 0.0;
    }
    (expected_value as f64 * probability) / estimated_ms as f64
}

/// Hash function for tiebreaking
fn xxh3_64(data: &[u8]) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    hasher.finish()
}

/// Constant product swap calculation with fees
fn cp_swap_out_with_fee(
    amount_in: u64,
    reserve_in: u64,
    reserve_out: u64,
    fee_num: u64,
    fee_denom: u64,
) -> Option<u64> {
    if amount_in == 0 || reserve_in == 0 || reserve_out == 0 || fee_denom == 0 {
        return Some(0);
    }
    
    // Apply fee: amount_in_after_fee = amount_in * (fee_denom - fee_num) / fee_denom
    let amount_in_after_fee = (amount_in as u128)
        .checked_mul((fee_denom - fee_num) as u128)?
        .checked_div(fee_denom as u128)?;
    
    // Calculate output: dy = (dx * Y) / (X + dx)
    let numerator = amount_in_after_fee.checked_mul(reserve_out as u128)?;
    let denominator = (reserve_in as u128).checked_add(amount_in_after_fee)?;
    
    let amount_out = numerator.checked_div(denominator)?;
    
    if amount_out > u64::MAX as u128 {
        return None;
    }
    
    Some(amount_out as u64)
}

// ===== Telemetry =====

#[derive(Serialize, Debug)]
struct StatSnap {
    t: u64,
    q_len: usize,
    sims: u64,
    p99_ms: u32,
    net_profit: i64,
    hit_rate: f64,
}

#[inline]
fn cu_from_logs(logs: &[String]) -> Option<u64> {
    for l in logs {
        if let Some(consumed_idx) = l.find(" consumed ") {
            let rest = &l[consumed_idx + " consumed ".len()..];
            if let Some(of_idx) = rest.find(" of ") {
                if let Ok(v) = rest[..of_idx].trim().parse::<u64>() {
                    return Some(v);
                }
            }
        }
    }
    None
}

// ROI-gated fee acceptance
#[inline]
fn acceptable_fee_with_estimate(
    price_micro: u64,
    est_cu: u64,
    tip_lamports: u64,
    base_fee_lamports: u64,
    ev: i64,
    min_roi: f64,
) -> bool {
    if ev <= 0 { return false; }
    let gas_cost = base_fee_lamports
        .saturating_add((price_micro.saturating_mul(est_cu)) / 1_000_000)
        .saturating_add(tip_lamports);
    let roi = ev as f64 / gas_cost.max(1) as f64;
    roi >= min_roi
}

// Expected value per millisecond for priority calculation
#[inline]
fn ev_per_ms(ev: i64, est_ms: u64, p_succ: f64) -> f64 {
    if est_ms == 0 || p_succ <= 0.0 { return 0.0; }
    (ev as f64 * p_succ) / est_ms as f64
}

// Simulation job for priority queue with Arc for cheap cloning
#[derive(Debug, Clone)]
struct SimJob {
    sig: Signature,
    tx: Arc<VersionedTransaction>,
    received_time: Instant,
    submission_slot: Slot,
    attempts: u32,
    priority: OrderedFloat<f64>,
    tiebreak: u64,
    est_gas_cost: u64,
    confidence: f64,
}

impl PartialEq for SimJob {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority && self.tiebreak == other.tiebreak
    }
}

impl Eq for SimJob {}

impl PartialOrd for SimJob {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SimJob {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match self.priority.cmp(&other.priority) {
            std::cmp::Ordering::Equal => other.tiebreak.cmp(&self.tiebreak),
            ord => ord,
        }
    }
}

// Pending transaction type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingTransaction {
    pub sig: Signature,
    #[serde(skip)]
    pub tx: Arc<VersionedTransaction>,
    pub received_at_millis: u64,  // Unix timestamp in millis
    pub slot: Slot,
    pub priority_fee: u64,
    pub compute_limit: u64,
}

// Sharded priority queues to reduce lock contention
struct Shard {
    heap: Mutex<BinaryHeap<SimJob>>,
}

impl Shard {
    fn new() -> Self { Self { heap: Mutex::new(BinaryHeap::new()) } }
}

struct ShardedSimQueue {
    shards: Vec<Shard>,
    rr: AtomicUsize,
    capacity_per_shard: usize,
    dropped: AtomicU64,
}

impl ShardedSimQueue {
    fn new(n: usize) -> Self {
        let n = n.max(1);
        let cap = (MAX_PENDING_TX_CACHE / n).max(1);
        Self {
            shards: (0..n).map(|_| Shard::new()).collect(),
            rr: AtomicUsize::new(0),
            capacity_per_shard: cap,
            dropped: AtomicU64::new(0),
        }
    }

    async fn push(&self, sig: &Signature, job: SimJob) {
        let idx = (xxh3_64(sig.as_ref()) as usize) % self.shards.len();
        let mut h = self.shards[idx].heap.lock().await;
        if h.len() >= self.capacity_per_shard {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            warn!(shard = idx, len = h.len(), cap = self.capacity_per_shard, "sim queue full; dropping job");
            return;
        }
        h.push(job);
    }

    async fn pop_some(&self, idx: usize) -> Option<SimJob> {
        let mut h = self.shards[idx].heap.lock().await;
        h.pop()
    }

    async fn pop_best(&self, current_slot: Slot) -> Option<SimJob> {
        let n = self.shards.len();
        if n == 0 { return None; }

        let s1 = (xxh3_64(&current_slot.to_le_bytes()) as usize) % n;
        let s2 = (s1 + 1) % n;

        let expire_slot = current_slot.saturating_sub(150);

        let job1 = {
            let mut h1 = self.shards[s1].heap.lock().await;

            while let Some(job) = h1.peek() {
                if job.submission_slot < expire_slot {
                    h1.pop();
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                } else {
                    break;
                }
            }

            h1.peek().map(|j| (j.priority, j.tiebreak, s1))
        };

        let job2 = {
            let mut h2 = self.shards[s2].heap.lock().await;

            while let Some(job) = h2.peek() {
                if job.submission_slot < expire_slot {
                    h2.pop();
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                } else {
                    break;
                }
            }

            h2.peek().map(|j| (j.priority, j.tiebreak, s2))
        };

        match (job1, job2) {
            (Some((p1, t1, idx1)), Some((p2, t2, idx2))) => {
                let selected_idx = if p1 > p2 || (p1 == p2 && t1 < t2) {
                    idx1
                } else {
                    idx2
                };

                let mut h = self.shards[selected_idx].heap.lock().await;
                h.pop()
            },
            (Some((_, _, idx)), None) | (None, Some((_, _, idx))) => {
                let mut h = self.shards[idx].heap.lock().await;
                h.pop()
            },
            (None, None) => None,
        }
    }
}

// (Duplicate simulation engine definitions removed - using consolidated version above)

// Double-precision AMM math
#[inline(always)]
fn cp_swap_out_with_fee(
    input: u128,
    rin: u128,
    rout: u128,
    fee_numerator: u128,
    fee_denominator: u128,
) -> Option<u128> {
    if rin == 0 || rout == 0 { return None; }
    if input == 0 { return Some(0); }
    if fee_denominator == 0 || fee_numerator >= fee_denominator { return None; }
    let eff = input.saturating_mul(fee_denominator.saturating_sub(fee_numerator));
    let num = eff.saturating_mul(rout);
    let den = rin.saturating_mul(fee_denominator).saturating_add(eff);
    if den == 0 { None } else { Some(num / den) }
}

#[derive(Debug, Clone, Copy)]
pub enum MsgVersion { Legacy, V0 }

#[derive(Debug)]
pub struct MEVOpportunity {
    pub tx: VersionedTransaction,
    pub profit_lamports: u64,
    pub opportunity_type: OpportunityType,
    pub confidence: f64,
    pub gas_estimate: u64,
    pub priority_fee: u64,
    pub submission_slot: Slot,
}

#[derive(Debug)]
pub struct SimulationResult {
    pub success: bool,
    pub profit: i64,
    pub gas_used: u64,
    pub logs: Vec<String>,
    pub account_changes: FastHashMap<Pubkey, (u64, u64)>,
}

#[derive(Clone)]
pub struct PendingTxAnalyzer {
    config: Arc<ArcSwap<RuntimeCfg>>,
    sim_queue: Arc<ShardedSimQueue>,
    deduper: Arc<Mutex<RingCuckoo>>,
    rpc_pool: Arc<RpcPool>,
    mint_cache: Arc<Mutex<FastHashMap<Pubkey, Option<TokenMint>>>>,
    alt_cache: Arc<Mutex<FastHashMap<Pubkey, AddressLookupTableAccount>>>,
    pool_cache: Arc<Mutex<FastHashMap<Pubkey, AmmPoolState>>>,
    op_tx: mpsc::Sender<MEVOpportunity>,
    hi_tx: mpsc::Sender<MEVOpportunity>,
    slot_stats: Arc<RwLock<SlotStats>>,
    known_dex_programs: Arc<DashMap<Pubkey, DexProgram>>,
    simulation_engines: Arc<DashMap<DexProgram, Box<dyn SimulationEngine>>>,
}

impl PendingTxAnalyzer {
    pub async fn new(endpoints: Vec<String>, runtime_cfg: RuntimeCfg) -> Self {
        let commitment = CommitmentConfig::processed();
        let pool = Arc::new(RpcPool::new(endpoints, commitment));

        let (op_tx, _op_rx) = mpsc::channel(1000);
        let (hi_tx, _hi_rx) = mpsc::channel(100);

        let known_dex_programs = Arc::new(DashMap::new());
        known_dex_programs.insert(
            Pubkey::from_str("675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8").unwrap(),
            DexProgram::RaydiumAmmV4,
        );
        known_dex_programs.insert(
            Pubkey::from_str("whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc").unwrap(),
            DexProgram::OrcaWhirlpool,
        );

        Self::new_with_pool(pool, op_tx, hi_tx, known_dex_programs, runtime_cfg)
    }

    pub fn new_with_pool(
        pool: Arc<RpcPool>,
        op_tx: mpsc::Sender<MEVOpportunity>,
        hi_tx: mpsc::Sender<MEVOpportunity>,
        known_dex_programs: Arc<DashMap<Pubkey, DexProgram>>,
        runtime_cfg: RuntimeCfg,
    ) -> Self {
        let config = Arc::new(ArcSwap::from_pointee(runtime_cfg));
        let sim_queue = Arc::new(ShardedSimQueue::new());
        let deduper = Arc::new(Mutex::new(RingCuckoo::new()));
        let mint_cache = Arc::new(Mutex::new(FastHashMap::default()));
        let alt_cache = Arc::new(Mutex::new(FastHashMap::default()));
        let pool_cache = Arc::new(Mutex::new(FastHashMap::default()));
        let slot_stats = Arc::new(RwLock::new(SlotStats::new()));
        
        // Initialize simulation engines
        let simulation_engines = Arc::new(DashMap::new());
        simulation_engines.insert(
            DexProgram::RaydiumAmmV4,
            Box::new(RaydiumSimulationEngine::new()) as Box<dyn SimulationEngine>,
        );
        simulation_engines.insert(
            DexProgram::OrcaWhirlpool,
            Box::new(OrcaSimulationEngine::new()) as Box<dyn SimulationEngine>,
        );

        Self {
            config,
            sim_queue,
            deduper,
            rpc_pool: pool,
            mint_cache,
            alt_cache,
            pool_cache,
            op_tx,
            hi_tx,
            slot_stats,
            known_dex_programs,
            simulation_engines,
        }
    }

    pub async fn queue_simulation_job(
        &self,
        sig: Signature,
        tx: &VersionedTransaction,
        slot: Slot,
    ) -> Result<(), &'static str> {
        // Check deduplication
        {
            let mut deduper = self.deduper.lock().await;
            if !deduper.try_insert(&sig, slot).await {
                return Err("duplicate transaction");
            }
        }

        let msg = match tx {
            VersionedTransaction::Legacy(t) => VersionedMessage::Legacy(t.message.clone()),
            VersionedTransaction::V0(t) => VersionedMessage::V0(t.message.clone()),
        };

        let accounts = self.resolve_message_keys_full(&msg).await;
        let ixs: Vec<CompiledInstruction> = match &msg {
            VersionedMessage::Legacy(m) => m.instructions.clone(),
            VersionedMessage::V0(m) => m.instructions.clone(),
        };

        let (price_micro, limit) = self.parse_compute_budget(&accounts, &ixs);

        let cfg = self.config.load();
        let tip = if cfg.enable_jito_tip { cfg.jito_tip_lamports } else { 0 };
        let tmo = Duration::from_millis(cfg.simulation_timeout_ms);

        if price_micro > cfg.max_priority_fee {
            debug!(price_micro, max = cfg.max_priority_fee, "priority fee exceeds max; skipping job");
            return Ok(());
        }

        let base_fee = self.rpc_pool.get_fee_for_message(&msg, tmo).await;

        let est_cu = limit.max(100_000);
        let gas = gas_cost_lamports_dyn(base_fee, price_micro, est_cu, tip);
        let ev = 0i64; // Replace with actual expected value calculation
        let est_ms = 100u64; // Replace with actual estimate
        let p_succ = 0.5f64; // Replace with actual probability
        let priority = ev_per_ms(ev, est_ms, p_succ);

        let job = SimJob {
            sig,
            tx: Arc::new(tx.clone()),
            received_time: Instant::now(),
            submission_slot: slot,
            attempts: 0,
            priority: OrderedFloat(priority),
            tiebreak: xxh3_64(sig.as_ref()),
            expected_value: ev,
            probability_success: p_succ,
            estimated_ms: est_ms,
            est_gas_cost: gas,
            confidence: 0.5,
        };

        self.sim_queue.push(&sig, job).await;
        Ok(())
    }

    fn parse_compute_budget(
        &self,
        _accounts: &[Pubkey],
        ixs: &[CompiledInstruction],
    ) -> (u64, u64) {
        let mut limit = None;
        let mut price_micro = None;

        for ix in ixs {
            if ix.data.len() >= 5 && ix.data[0] == 0x02 {
                if let Ok(bytes) = <[u8; 4]>::try_from(&ix.data[1..5]) {
                    limit = Some(u32::from_le_bytes(bytes) as u64);
                } else {
                    debug!("Invalid compute unit limit instruction");
                }
            } else if ix.data.len() >= 9 && ix.data[0] == 0x03 {
                if let Ok(bytes) = <[u8; 8]>::try_from(&ix.data[1..9]) {
                    price_micro = Some(u64::from_le_bytes(bytes));
                } else {
                    debug!("Invalid compute unit price instruction");
                }
            }
        }

        let final_limit = limit.unwrap_or(200_000).min(1_400_000);
        let final_price = price_micro.unwrap_or(0);

        (final_price, final_limit)
    }

    async fn resolve_message_keys_full(&self, msg: &VersionedMessage) -> Vec<Pubkey> {
        match msg {
            VersionedMessage::Legacy(m) => m.account_keys.clone(),
            VersionedMessage::V0(m) => {
                let mut keys = m.account_keys.clone();
                
                // Resolve ALT accounts
                let alts = self.resolve_alt_accounts(&m.address_table_lookups).await;
                
                for (lookup, alt_opt) in m.address_table_lookups.iter().zip(alts.iter()) {
                    if let Some(alt) = alt_opt {
                        // Add writable addresses
                        for idx in &lookup.writable_indexes {
                            if let Some(addr) = alt.addresses.get(*idx as usize) {
                                keys.push(*addr);
                            }
                        }
                        // Add readonly addresses  
                        for idx in &lookup.readonly_indexes {
                            if let Some(addr) = alt.addresses.get(*idx as usize) {
                                keys.push(*addr);
                            }
                        }
                    }
                }
                
                keys
            }
        }
    }
    
    async fn resolve_alt_accounts(
        &self,
        lookups: &[MessageAddressTableLookup],
    ) -> Vec<Option<AddressLookupTableAccount>> {
        if lookups.is_empty() {
            return vec![];
        }
        
        let mut results = vec![None; lookups.len()];
        let mut cache = self.alt_cache.lock().await;
        let mut missing_keys = Vec::new();
        let mut missing_indices = Vec::new();
        
        // Check cache first
        for (i, lookup) in lookups.iter().enumerate() {
            if let Some(cached) = cache.get(&lookup.account_key) {
                results[i] = Some(cached.clone());
            } else {
                missing_keys.push(lookup.account_key);
                missing_indices.push(i);
            }
        }
        
        if missing_keys.is_empty() {
            return results;
        }
        
        // Parallel fetch with concurrency control
        const ALT_FETCH_CONCURRENCY: usize = 4;
        const ALT_BATCH_SIZE: usize = 100;
        let semaphore = Arc::new(Semaphore::new(ALT_FETCH_CONCURRENCY));
        let mut futures = FuturesUnordered::new();
        
        // Batch keys for efficient fetching
        for chunk in missing_keys.chunks(ALT_BATCH_SIZE) {
            let keys = chunk.to_vec();
            let pool = self.rpc_pool.clone();
            let sem = semaphore.clone();
            
            futures.push(async move {
                let _permit = sem.acquire().await.unwrap();
                let tmo = Duration::from_millis(500);
                let accounts = pool.get_multiple_accounts_hedged(&keys, tmo).await;
                (keys, accounts)
            });
        }
        
        // Collect results
        let mut fetched = FastHashMap::default();
        while let Some((keys, accounts)) = futures.next().await {
            for (key, acc_opt) in keys.iter().zip(accounts.iter()) {
                if let Some(acc) = acc_opt {
                    // Validate owner is ALT program
                    if acc.owner != solana_address_lookup_table_program::id() {
                        warn!(
                            alt = %key,
                            owner = %acc.owner,
                            "ALT account has invalid owner"
                        );
                        fetched.insert(*key, None);
                        continue;
                    }
                    
                    // Validate data size
                    if acc.data.len() < 56 {  // Minimum ALT size
                        warn!(
                            alt = %key,
                            size = acc.data.len(),
                            "ALT account data too small"
                        );
                        fetched.insert(*key, None);
                        continue;
                    }
                    
                    // Deserialize ALT
                    let mut data_slice: &[u8] = &acc.data;
                    match OnchainALT::deserialize(&mut data_slice) {
                        Ok(onchain_alt) => {
                            let alt_account = AddressLookupTableAccount {
                                key: *key,
                                addresses: onchain_alt.addresses.to_vec(),
                            };
                            fetched.insert(*key, Some(alt_account));
                        }
                        Err(e) => {
                            debug!(
                                alt = %key,
                                error = %e,
                                "Failed to deserialize ALT"
                            );
                            fetched.insert(*key, None);
                        }
                    }
                } else {
                    fetched.insert(*key, None);
                }
            }
        }
        
        // Update results and cache
        for (idx, key) in missing_indices.iter().zip(missing_keys.iter()) {
            if let Some(alt_opt) = fetched.get(key) {
                if let Some(alt) = alt_opt {
                    results[*idx] = Some(alt.clone());
                    cache.insert(*key, alt.clone());
                }
            }
        }
        
        drop(cache);  // Release lock
        results
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mempool::bandits::ucb::BanditArm;
    use solana_sdk::{
        message::Message,
        system_instruction,
        transaction::Transaction,
    };

    async fn create_test_analyzer() -> PendingTxAnalyzer {
        let endpoints = vec!["http://127.0.0.1:8899".to_string()];
        let runtime_cfg = RuntimeCfg {
            simulation_timeout_ms: 1000,
            max_priority_fee: 1_000_000,
            jito_tip_lamports: 1000,
            enable_jito_tip: false,
            min_roi: 1.05,
            max_cu_estimate: 1_400_000,
        };
        PendingTxAnalyzer::new(endpoints, runtime_cfg).await
    }
    
    #[test]
    fn test_cp_swap_out_with_fee() {
        // 0.3% fee -> numerator=3, denominator=1000
        assert_eq!(cp_swap_out_with_fee(1000, 10_000, 20_000, 3, 1000).unwrap(), 1813);
        assert_eq!(cp_swap_out_with_fee(0, 10_000, 20_000, 3, 1000).unwrap(), 0);
    }
    
    #[test]
    fn test_slot_stats() {
        let mut stats = SlotStats::new();
        stats.last_slot = 100;
        
        // Record success in slot 100
        stats.record(100, true);
        assert!(!super::should_backoff(&stats, 100)); // Only 0 fails
        
        // Record 3 failures in same slot
        stats.record(100, false);
        stats.record(100, false);
        stats.record(100, false);
        assert!(super::should_backoff(&stats, 100)); // 3 fails, should backoff
        
        // New slot resets
        stats.record(101, false);
        assert!(!super::should_backoff(&stats, 101)); // Only 1 fail in new slot
    }
    
    #[test]
    fn test_ring_cuckoo() {
        use tokio::runtime::Runtime;
        let rt = Runtime::new().unwrap();
        
        rt.block_on(async {
            let mut deduper = RingCuckoo::new();
            let sig1 = Signature::new_unique();
            let sig2 = Signature::new_unique();
            
            // First insert should succeed
            assert!(deduper.try_insert(&sig1, 100).await);
            
            // Duplicate should fail
            assert!(!deduper.try_insert(&sig1, 100).await);
            
            // Different sig should succeed
            assert!(deduper.try_insert(&sig2, 100).await);
        });
    }
    
    #[tokio::test]
    async fn test_sharded_queue() {
        let queue = ShardedSimQueue::new();
        
        let sig1 = Signature::new_unique();
        let sig2 = Signature::new_unique();
        
        let job1 = SimJob {
            sig: sig1,
            tx: Arc::new(VersionedTransaction::default()),
            received_time: Instant::now(),
            submission_slot: 100,
            attempts: 0,
            priority: OrderedFloat(10.0),
            tiebreak: 1,
            expected_value: 100,
            probability_success: 0.9,
            estimated_ms: 10,
            est_gas_cost: 5000,
            confidence: 0.8,
        };
        
        let job2 = SimJob {
            sig: sig2,
            tx: Arc::new(VersionedTransaction::default()),
            received_time: Instant::now(),
            submission_slot: 100,
            attempts: 0,
            priority: OrderedFloat(20.0),
            tiebreak: 2,
            expected_value: 200,
            probability_success: 0.8,
            estimated_ms: 15,
            est_gas_cost: 6000,
            confidence: 0.7,
        };
        
        // Push jobs
        queue.push(&sig1, job1.clone()).await;
        queue.push(&sig2, job2.clone()).await;
        
        // Pop should return higher priority job first
        let popped = queue.pop().await;
        assert!(popped.is_some());
        assert_eq!(popped.unwrap().sig, sig2);
        
        // Next pop should return lower priority job
        let popped = queue.pop().await;
        assert!(popped.is_some());
        assert_eq!(popped.unwrap().sig, sig1);
    }
}
}
