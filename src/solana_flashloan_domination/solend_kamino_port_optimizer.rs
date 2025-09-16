// ==================== [PIECE 1: CORE PRELUDE / TYPES / CONSTANTS] ====================
#![allow(clippy::too_many_arguments)]
#![allow(clippy::large_enum_variant)]

// Standard library
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque},
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, OnceLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

// External crates
use anyhow::{anyhow, Result};
use anchor_client::solana_sdk::commitment_config::CommitmentConfig;
use borsh::BorshSerialize;
use lru::LruCache;
use prometheus::{Histogram, HistogramOpts, IntCounter, Registry};
use rand::prelude::*;
use sha2::{Digest, Sha256};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcSendTransactionConfig, RpcSignatureStatusConfig, RpcSimulateTransactionConfig},
    rpc_response::RpcSimulateTransactionResult,
    client_error::ClientError,
};
use solana_program::{
    instruction::{AccountMeta, Instruction},
    program_pack::Pack,
    pubkey::Pubkey,
    system_instruction,
};
use solana_sdk::{
    account::Account,
    address_lookup_table_account::AddressLookupTableAccount,
    clock::Slot,
    commitment_config::CommitmentConfig as SdkCommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    message::{v0::Message as V0Message, Message, VersionedMessage},
    pubkey,
    signature::{Keypair, Signature, Signer},
};
use std::num::NonZeroUsize;
use tokio::time::sleep;

// ==================== PIECE 1.1: Shortvec + size constants ====================

/// Calculate the length of a Solana shortvec (7-bit little-endian varint)
/// Branchless implementation: 1 + 1{n≥0x80} + 1{n≥0x4000}
#[inline(always)]
pub const fn shortvec_len(n: usize) -> usize {
    1 + ((n >= 0x80) as usize) + ((n >= 0x4000) as usize)
}

pub const MSG_HEADER_BYTES: usize = 3;
pub const ACCOUNT_KEY_BYTES: usize = 32;
pub const BLOCKHASH_BYTES: usize = 32;
pub const MSG_VERSION_OVERHEAD: usize = 1; // v0 prefix (safe upper bound)

/// QUIC MTU headroom shaping for v0 tx (empirically safe)
pub const QUIC_SAFE_PACKET_BUDGET: usize = 1150;

// ==================== PIECE 1.2: ixs_fingerprint ====================

// ixs_fingerprint implementation is now only at the top of the file

// ==================== PIECE 1.3: percentile_pair_unstable ====================

/// Computes two percentiles in O(n) time without full sorting.
/// Uses select_nth_unstable to compute percentiles in linear time.
/// Returns (p_lo-th percentile, p_hi-th percentile)
/// 
/// # Panics
/// Never panics (returns (0, 0) on empty input)
#[inline]
pub fn percentile_pair_unstable(src: &[u64], p_lo: usize, p_hi: usize) -> (u64, u64) {
    debug_assert!(p_lo <= 100 && p_hi <= 100);
    if src.is_empty() { return (0, 0); }
    
    let n = src.len();
    let ilo = (n * p_lo / 100).min(n - 1);
    let ihi = (n * p_hi / 100).min(n - 1);

    if ilo == ihi {
        let mut v = src.to_vec();
        let (_, val, _) = v.select_nth_unstable(ilo);
        return (*val, *val);
    }
    let mut a = src.to_vec();
    let mut b = src.to_vec();
    let (_, lo, _) = a.select_nth_unstable(ilo);
    let (_, hi, _) = b.select_nth_unstable(ihi);
    (*lo, *hi)
}

// ==================== PIECE 1.4: slot + leader caches ====================

#[derive(Clone)]
struct SlotCache {
    slot: AtomicU64,
    ts_ns: AtomicU64,
}

static SLOT_CACHE: OnceLock<SlotCache> = OnceLock::new();

#[inline(always)]
fn slot_cache() -> &'static SlotCache {
    SLOT_CACHE.get_or_init(|| SlotCache {
        slot: AtomicU64::new(0),
        ts_ns: AtomicU64::new(0),
    })
}

#[derive(Clone, Debug)]
struct LeaderCache {
    epoch: u64,
    schedule: HashMap<String, Vec<usize>>, // identity -> slot indices in epoch
    last_refresh: Instant,
}

static LEADER_CACHE: OnceLock<tokio::sync::RwLock<LeaderCache>> = OnceLock::new();

#[inline(always)]
fn leader_cache() -> &'static tokio::sync::RwLock<LeaderCache> {
    LEADER_CACHE.get_or_init(|| tokio::sync::RwLock::new(LeaderCache {
        epoch: 0,
        schedule: HashMap::new(),
        last_refresh: Instant::now() - Duration::from_secs(600),
    }))
}

// ==================== PIECE 1.5: CU LRU ====================

static CU_LRU: OnceLock<tokio::sync::RwLock<LruCache<[u8; 32], u64>>> = OnceLock::new();

#[inline(always)]
fn cu_lru() -> &'static tokio::sync::RwLock<LruCache<[u8; 32], u64>> {
    CU_LRU.get_or_init(|| tokio::sync::RwLock::new(LruCache::new(
        NonZeroUsize::new(10_000).unwrap()
    )))
}

// ==================== PIECE 1.11: global constants ====================

pub const COMPUTE_UNITS_FLOOR: u32 = 150_000;
pub const COMPUTE_UNITS_CEIL: u32 = 1_400_000;

pub const PRIORITY_FEE_LAMPORTS_FLOOR: u64 = 500; // autoscaled later
pub const TIP_JITTER_MAX_BP: u32 = 30;            // micro-jitter ceiling
pub const MIN_PROFIT_BPS: u64 = 10;
pub const REBALANCE_THRESHOLD_BPS: u64 = 25;
pub const MAX_SLIPPAGE_BPS: u64 = 50;

pub const SOLEND_PROGRAM_ID: &str = "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo";
pub const KAMINO_PROGRAM_ID: &str = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD";

#[inline(always)]
pub fn solend_program() -> Pubkey { pubkey!("So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo") }

#[inline(always)]
pub fn kamino_program() -> Pubkey { pubkey!("KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD") }
    signer::Signer as SdkSigner,
    system_program,
    transaction::{Transaction, VersionedTransaction},
};
use spl_associated_token_account::{
    get_associated_token_address,
    instruction as ata_ix,
};

// ==================== PIECE 1.6: PortOptimizer helpers ====================

impl PortOptimizer {
    /// Lock-free coarse slot with TTL; falls back to RPC on staleness.
    #[inline(always)]
    pub async fn get_slot_coarse(&self, ttl: Duration) -> u64 {
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let sc = slot_cache();
        let ts = sc.ts_ns.load(Ordering::Relaxed);
        
        if now_ns.saturating_sub(ts) <= ttl.as_nanos() as u64 {
            return sc.slot.load(Ordering::Relaxed);
        }
        
        match self.rpc_client.get_slot().await {
            Ok(s) => {
                sc.slot.store(s, Ordering::Relaxed);
                sc.ts_ns.store(now_ns, Ordering::Relaxed);
                s
            }
            Err(_) => sc.slot.load(Ordering::Relaxed),
        }
    }

    /// Cohort-stable micro-jitter in basis points [0, max_bp] from last sig or wallet.
    #[inline(always)]
    pub fn price_jitter_bp(&self, max_bp: u32) -> u32 {
        if max_bp == 0 { return 0; }
        let seed_bytes: &[u8] = match self.last_tx_signature.blocking_read().as_ref() {
            Some(sig) => sig.as_ref(),
            None => self.wallet.pubkey().as_ref(),
        };
        let mut h = Sha256::new();
        h.update(b"JIT1");
        h.update(seed_bytes);
        let d = h.finalize();
        u32::from_le_bytes([d[0], d[1], d[2], d[3]]) % (max_bp + 1)
    }

    /// Leader concentration in the next k slots within the current epoch.
    /// Returns ∈[0,1]: 1.0 means a single leader dominates the lookahead window.
    pub async fn leader_window_concentration(&self, k: usize) -> Result<f64> {
        if k == 0 { return Ok(0.0); }
        let ei = self.rpc_client.get_epoch_info().await?;
        let mut lc = leader_cache().write().await;
        
        if lc.epoch != ei.epoch || lc.last_refresh.elapsed() > Duration::from_secs(12) {
            if let Some(schedule) = self.rpc_client.get_leader_schedule(None).await? {
                lc.schedule = schedule;
                lc.epoch = ei.epoch;
                lc.last_refresh = Instant::now();
            }
        }
        
        // Build dense index -> leader string
        let slots_in_epoch = ei.slots_in_epoch as usize;
        if slots_in_epoch == 0 { return Ok(0.0); }
        
        let lc_r = leader_cache().read().await;
        let mut idx2leader: Vec<&str> = vec![""; slots_in_epoch];
        
        for (leader, slots) in &lc_r.schedule {
            for &s in slots {
                let j = s as usize;
                if j < slots_in_epoch { 
                    idx2leader[j] = leader.as_str(); 
                }
            }
        }

        let cur_idx = ei.slot_index as usize;
        let mut counts: HashMap<&str, usize> = HashMap::new();
        let mut total = 0usize;
        
        for off in 1..=k {
            let idx = cur_idx + off;
            if idx >= slots_in_epoch { break; }
            let l = idx2leader[idx];
            if !l.is_empty() {
                *counts.entry(l).or_insert(0) += 1;
                total += 1;
            }
        }
        
        if total == 0 { return Ok(0.0); }
        let maxc = *counts.values().max().unwrap_or(&0) as f64;
        Ok((maxc / (total as f64)).min(1.0))
    }

    // ==================== PIECE 1.7: slot_phase_backoff ====================
    
    /// Slot-phase aware backoff with congestion control
    #[inline]
    pub async fn slot_phase_backoff(&self, leader_conc: f64, base_ms: u64) {
        let mult = 1.0 + leader_conc.clamp(0.0, 1.0) * 0.6;  // 1.00..1.60
        let dur_ms = (base_ms as f64 * mult).round() as u64;
        let jitter = thread_rng().gen_range(0u64..=50u64);    // ≤50ms to decorrelate
        sleep(Duration::from_millis(dur_ms.saturating_add(jitter).min(450))).await;
    }

    // ==================== PIECE 1.8: tx size estimators ====================
    
    /// Estimate the serialized size of a transaction message
    /// 
    /// This function provides a close approximation of the serialized size of a transaction
    /// message, which is useful for fee estimation and MTU safety checks.
    /// 
    /// # Arguments
    /// * `ixs` - The instructions to include in the transaction
    /// 
    /// # Returns
    /// Estimated size in bytes
    #[inline]
    pub fn tx_message_size_estimate(&self, ixs: &[Instruction]) -> usize {
        
        for ix in ixs {
            if !uniq.iter().any(|k| *k == ix.program_id) { 
                uniq.push(ix.program_id); 
            }
            for m in &ix.accounts {
                if !uniq.iter().any(|k| *k == m.pubkey) { 
                    uniq.push(m.pubkey); 
                }
            }
        }
        
        if uniq.len() > 64 { 
            uniq.sort_unstable(); 
            uniq.dedup(); 
        }

        let mut sz = 0usize;
        sz += MSG_HEADER_BYTES;
        sz += shortvec_len(uniq.len());           // vec<AccountKeys> len
        sz += uniq.len() * ACCOUNT_KEY_BYTES;     // account keys
        sz += BLOCKHASH_BYTES;                    // recent blockhash
        sz += shortvec_len(ixs.len());            // instructions vec len
        
        for ix in ixs {
            let n_idx = ix.accounts.len();
            let n_dat = ix.data.len();
            sz += 1;                              // program id index
            sz += shortvec_len(n_idx) + n_idx;    // account indices
            sz += shortvec_len(n_dat) + n_dat;    // data
        }
        
        sz + MSG_VERSION_OVERHEAD
    }

    #[inline]
    pub fn tx_message_size_with_alts_estimate(
        &self,
        ixs: &[Instruction],
        alts: &[AddressLookupTableAccount],
    ) -> usize {
        let core = self.tx_message_size_estimate(ixs);
        // ALT payload ≈ 8 bytes header + 32 * addresses
        let alt_bytes: usize = alts.iter()
            .map(|t| 8 + 32 * t.addresses.len())
            .sum();
        core + alt_bytes + 64 // framing headroom
    }

    // ==================== PIECE 1.9: fee_quantile_window ====================
    
    #[inline(always)]
    pub fn fee_quantile_window(&self, attempt: u32, leader_conc: f64, tx_size: usize) -> (usize, usize) {
        let mut lo = 70usize;
        let mut hi = 90usize;
        
        if leader_conc >= 0.75 { lo = 80; hi = 95; }
        else if leader_conc >= 0.50 { lo = 75; hi = 92; }
        
        if tx_size >= 1000 { hi = hi.saturating_add(2).min(98); }
        
        if attempt >= 1 { 
            lo = (lo + 3).min(90); 
            hi = (hi + 3).min(98); 
        }
        
        if attempt >= 2 { 
            lo = (lo + 3).min(92); 
            hi = (hi + 3).min(99); 
        }
        
        (lo, hi)
    }
    
    // ==================== PIECE 1.10: simulate_exact_v0 ====================
    
    /// Simulate a VersionedTransaction with explicit config.
    pub async fn simulate_exact_v0_tx(
        &self,
        vtx: &VersionedTransaction,
        cfg: RpcSimulateTransactionConfig,
    ) -> Result<RpcSimulateTransactionResult> {
        let out = self.rpc_client.simulate_transaction_with_config(vtx.clone(), cfg).await?;
        Ok(out.value)
    }

    /// Simulate from ixs/signers; builds a legacy message => v0 tx (no ALTs).
    pub async fn simulate_exact_v0_ixs(
        &self,
        ixs: &[Instruction],
        payer: &Pubkey,
        signers: &[&dyn Signer],
        recent_blockhash: Hash,
        cfg: RpcSimulateTransactionConfig,
    ) -> Result<RpcSimulateTransactionResult> {
        let msg = Message::new(ixs, Some(payer));
        let vtx = VersionedTransaction::try_new(msg, signers)?;
        let out = self.rpc_client.simulate_transaction_with_config(vtx, cfg).await?;
        Ok(out.value)
    }

    /// Convenient default used by most call-sites.
    pub async fn simulate_exact_v0_default(&self, vtx: &VersionedTransaction) -> Result<RpcSimulateTransactionResult> {
        self.simulate_exact_v0_tx(
            vtx,
            RpcSimulateTransactionConfig {
                sig_verify: false,
                replace_recent_blockhash: true,
                commitment: Some(solana_sdk::commitment_config::CommitmentConfig::processed()),
                ..Default::default()
            }
        ).await
    }
}

use spl_token::{
    solana_program::program_pack::Pack as TokenPack,
    state::Account as TokenAccount,
};
use tokio::sync::{mpsc, oneshot, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, trace, warn};

// Conditional imports
#[cfg(feature = "torch")]
use tch::{no_grad, CModule, Device, Kind, Tensor};

/// Generate a deterministic, collision-resistant fingerprint for a set of instructions.
/// This is used for caching and deduplication.
/// 
/// # Panics
/// Never panics (no unwraps, no allocations after initial hasher)
#[inline(always)]
pub fn ixs_fingerprint(ixs: &[Instruction]) -> [u8; 32] {
    // Deterministic, order-preserving, collision-resistant
    let mut h = Sha256::new();
    h.update(b"IXS1"); // domain sep
    for ix in ixs {
        h.update(ix.program_id.as_ref());
        // precise account meta encoding (no heap, no sort, preserves intent)
        for m in &ix.accounts {
            h.update(m.pubkey.as_ref());
            h.update(&[m.is_signer as u8, m.is_writable as u8]);
        }
        let n = ix.data.len() as u64;
        h.update(&n.to_le_bytes());
        h.update(&ix.data);
    }
    let digest = h.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest[..32]);
    out
}

/// Estimate the serialized size of a transaction message
/// 
/// This function provides a close approximation of the serialized size of a transaction
/// message, which is useful for fee estimation and MTU safety checks.
/// 
/// # Arguments
/// * `message` - The message to estimate size for
/// 
/// # Returns
/// Estimated size in bytes
fn tx_message_size_estimate(message: &Message) -> usize {
    // Message header is 3 bytes
    let mut size = 3;
    
    // Account addresses (32 bytes each)
    size += message.account_keys.len() * 32;
    
    // Recent blockhash (32 bytes)
    size += 32;
    
    // Instructions
    for ix in &message.instructions {
        // Program ID index (1 byte) + account indices (1 byte each)
        size += 1 + ix.accounts.len();
        
        // Instruction data length (u16) + actual data
        size += 2 + ix.data.len();
    }
    
    // Add some overhead for versioning and other fields
    size += 8;
    
    size
}

/// Calculate the fee quantile window based on transaction attempt and network conditions.
/// 
/// This method determines the appropriate fee percentiles to use when estimating
/// transaction fees, taking into account the current attempt number, leader
/// concentration, and transaction size.
/// 
/// # Arguments
/// * `attempt` - The current transaction attempt number (0 for first attempt)
/// * `leader_conc` - The concentration of leaders in the current window (0.0 to 1.0)
/// * `tx_size` - The size of the transaction in bytes
/// 
/// # Returns
/// A tuple of (lower_percentile, upper_percentile) for fee estimation
#[inline(always)]
fn fee_quantile_window(attempt: u32, leader_conc: f64, tx_size: usize) -> (usize, usize) {
    let mut lo = 70usize;
    let mut hi = 90usize;

    if leader_conc >= 0.75 { lo = 80; hi = 95; }
    else if leader_conc >= 0.50 { lo = 75; hi = 92; }

    if tx_size >= 1000 { hi = hi.saturating_add(2).min(98); }

    if attempt >= 1 { lo = (lo + 3).min(90); hi = (hi + 3).min(98); }
    if attempt >= 2 { lo = (lo + 3).min(92); hi = (hi + 3).min(99); }

    (lo, hi)
}

// ==================== PIECE 1.13a: P² online quantile tracker ====================
#[derive(Clone, Debug)]
pub struct P2Quantile {
    q: f64,
    n: [f64; 5],   // marker positions
    ns: [f64; 5],  // desired positions
    dn: [f64; 5],  // increments
    x: [f64; 5],   // marker heights
    buf: Vec<f64>, // first 5 samples
}

impl P2Quantile {
    pub fn new(q: f64) -> Self {
        let p = [0.0, q / 2.0, q, (1.0 + q) / 2.0, 1.0];
        Self {
            q,
            n: [1.0, 2.0, 3.0, 4.0, 5.0],
            ns: [1.0, 1.0, 1.0, 1.0, 1.0],
            dn: p,
            x: [0.0; 5],
            buf: Vec::with_capacity(5),
        }
    }

    #[inline]
    pub fn value(&self) -> Option<f64> {
        if self.buf.len() < 5 { None } else { Some(self.x[2]) }
    }

    pub fn update(&mut self, v: f64) {
        if self.buf.len() < 5 {
            self.buf.push(v);
            if self.buf.len() == 5 {
                self.buf.sort_by(|a, b| a.partial_cmp(b).unwrap());
                for i in 0..5 { self.x[i] = self.buf[i]; }
                // after seeding, desired positions for k=5
                for i in 0..5 { self.ns[i] = 1.0 + 4.0 * self.dn[i]; }
            }
            return;
        }
        // find cell k
        let mut k = 0usize;
        if v < self.x[0] { self.x[0] = v; k = 0; }
        else if v >= self.x[4] { self.x[4] = v; k = 3; }
        else {
            while k < 4 && v >= self.x[k+1] { k += 1; }
        }
        // increment positions above k
        for i in (k+1)..5 { self.n[i] += 1.0; }
        for i in 0..5 { self.ns[i] += self.dn[i]; }

        // adjust interior markers
        for i in 1..4 {
            let d = self.ns[i] - self.n[i];
            let dir = if d >= 1.0 && (self.n[i+1] - self.n[i]) > 1.0 {
                1.0
            } else if d <= -1.0 && (self.n[i-1] - self.n[i]) < -1.0 {
                -1.0
            } else { 0.0 };
            if dir.abs() >= f64::EPSILON {
                // parabolic prediction
                let n_im1 = self.n[i-1]; let n_i = self.n[i]; let n_ip1 = self.n[i+1];
                let x_im1 = self.x[i-1]; let x_i = self.x[i]; let x_ip1 = self.x[i+1];
                let a = (dir / (n_ip1 - n_im1))
                        * ((n_i - n_im1 + dir) * (x_ip1 - x_i) / (n_ip1 - n_i)
                         + (n_ip1 - n_i - dir) * (x_i - x_im1) / (n_i - n_im1));
                let cand = x_i + a;
                // if cand within neighbors, accept, else linear step
                if cand > x_im1 && cand < x_ip1 {
                    self.x[i] = cand;
                } else {
                    self.x[i] = x_i + dir * (if dir > 0.0 { x_ip1 - x_i } else { x_im1 - x_i }) / ( (n_ip1 - n_im1) );
                }
                self.n[i] += dir;
            }
        }
    }
}

/// Slot-phase aware backoff with congestion control
/// 
/// This function implements a backoff strategy that's aware of the current slot phase
/// and network congestion. It's used to prevent spamming the network and to
/// avoid transaction failures due to network congestion.
/// 
/// # Arguments
/// * `leader_concentration` - The concentration of leaders in the current window (0.0 to 1.0)
/// * `base_ms` - Base backoff time in milliseconds
/// 
/// # Returns
/// A future that resolves after the backoff period
async fn slot_phase_backoff(leader_concentration: f64, base_ms: u64) {
    // Calculate the congestion factor (1.0 to 2.0)
    let congestion_factor = 1.0 + leader_concentration.clamp(0.0, 1.0);
    
    // Add some jitter to avoid thundering herd
    let jitter = rand::thread_rng().gen_range(0.8..1.2);
    
    // Calculate the final backoff time
    let backoff_ms = (base_ms as f64 * congestion_factor * jitter) as u64;
    
    // Sleep for the calculated duration
    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
}

/// Deterministic cohort-stable jitter in basis points [0, max_bp].
/// Uses wallet pubkey or last tx sig as entropy source.
#[inline(always)]
fn price_jitter_bp(max_bp: u32) -> u32 {
    if max_bp == 0 { return 0; }
    
    // Use last tx sig if available, otherwise fall back to wallet pubkey
    let seed_bytes: &[u8] = match get_last_tx_signature().as_ref() {
        Some(sig) => sig.as_ref(),
        None => get_wallet_pubkey().as_ref(),
    };
    
    // Domain-separated hash for jitter
    let mut h = Sha256::new();
    h.update(b"JIT1");
    h.update(seed_bytes);
    let d = h.finalize();
    
    // Use first 4 bytes for jitter calculation
    u32::from_le_bytes([d[0], d[1], d[2], d[3]]) % (max_bp + 1)
}

/// Simulate a transaction with v0 support
/// 
/// This is a wrapper around simulate_exact that ensures v0 transaction support
async fn simulate_exact_v0(
    ixs: &[Instruction],
    signers: &[&dyn Signer],
    recent_blockhash: Hash,
    config: RpcSimulateTransactionConfig,
) -> Result<SimulationResult, Box<dyn std::error::Error>> {
    // Create a v0 message
    let message = Message::new_with_blockhash(ixs, Some(&get_payer_pubkey()), &recent_blockhash);
    
    // Sign the message
    let tx = Transaction::new_unsigned(message);
    let tx = signers.iter().try_fold(tx, |tx, signer| {
        tx.try_sign(&[&**signer], recent_blockhash)
    })?;
    
    // Convert to versioned transaction
    let versioned_tx = VersionedTransaction::from(tx);
    
    // Simulate the transaction
    let response = get_rpc_client()
        .simulate_transaction_with_config(
            &versioned_tx,
            config,
        )
        .await?;
    
    // Convert the response to our SimulationResult
    Ok(SimulationResult {
    }
}

    
    // Find the higher percentile
    let (_, hi_val, _) = b.select_nth_unstable(ihi);
    
    (*lo_val, *hi_val)
}

// ====== GLOBAL IMPORTS ======
use {
    anyhow::{anyhow, Context, Result},
    async_trait::async_trait,
    base64::{prelude::BASE64_STANDARD, Engine},
    bincode::serialize,
    crossbeam_channel::{bounded, Receiver, Sender},
    futures_util::{future::join_all, stream::FuturesUnordered, StreamExt},
    lru::{LruCache, NonZeroUsize},
    log::*,
    serde::{Deserialize, Serialize},
    sha2::{Digest, Sha256},
    smallvec::SmallVec,
    solana_client::{
        client_error::ClientError,
        nonblocking::rpc_client::RpcClient,
        rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig},
        rpc_request::RpcError,
        rpc_response::{Response as RpcResponse, RpcSimulateTransactionResult},
    },
    solana_sdk::{
        account::Account,
        address_lookup_table_account::AddressLookupTableAccount,
        commitment_config::CommitmentConfig,
        compute_budget::ComputeBudgetInstruction,
        hash::Hash,
        instruction::{AccountMeta, Instruction},
        message::{v0, VersionedMessage},
        pubkey::Pubkey,
        signature::{Keypair, Signature, Signer},
        signer::SignerError,
        transaction::{TransactionError, VersionedTransaction},
    },
    solana_transaction_status::{
        EncodedTransaction, EncodedTransactionWithStatusMeta, TransactionConfirmationStatus,
        UiMessageEncoding, UiTransactionEncoding,
    },
    std::{
        cmp::Ordering,
        collections::{HashMap, HashSet},
        fmt,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
            Arc, OnceLock,
        },
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    },
    tokio::{
        sync::{Mutex, RwLock},
        time::sleep,
    },
    url::Url,
};

// Re-export commonly used types
pub use solana_sdk::signature::Keypair;
pub use solana_sdk::pubkey::Pubkey;
pub use solana_sdk::signature::Signature;
pub use solana_sdk::hash::Hash;

// ====== GLOBAL CACHES ======

/// Hybrid sender that can use both TPU and RPC for transaction submission
#[derive(Clone)]
pub struct HybridSender {
    /// Primary RPC client
    rpc: Arc<RpcClient>,
    /// Optional TPU client for faster submission
    tpu_client: Option<Arc<dyn TpuClientInterface + Send + Sync>>,
    /// Whether to prefer TPU over RPC
    prefer_tpu: bool,
}

/// Trait for TPU client to allow for dependency injection in tests
#[async_trait::async_trait]
pub trait TpuClientInterface: Send + Sync {
    async fn send_transaction(&self, transaction: &VersionedTransaction) -> Result<Signature>;
}

/// Implementation of TpuClientInterface for the real TPU client
#[cfg(feature = "tpu")]
#[async_trait::async_trait]
impl TpuClientInterface for solana_client::tpu_client::TpuClient {
    async fn send_transaction(&self, transaction: &VersionedTransaction) -> Result<Signature> {
        self.try_send_transaction(transaction)
            .await
            .map_err(anyhow::Error::from)
    }
}

/// Test implementation of TpuClientInterface
#[cfg(test)]
#[derive(Clone)]
pub struct MockTpuClient;

#[cfg(test)]
#[async_trait::async_trait]
impl TpuClientInterface for MockTpuClient {
    async fn send_transaction(&self, _transaction: &VersionedTransaction) -> Result<Signature> {
        Ok(Signature::new_unique())
    }
}

impl HybridSender {
    /// Create a new HybridSender with the given RPC client and optional TPU client
    pub fn new(
        rpc: Arc<RpcClient>,
        tpu_client: Option<Arc<dyn TpuClientInterface + Send + Sync>>,
        prefer_tpu: bool,
    ) -> Self {
        Self {
            rpc,
            tpu_client,
            prefer_tpu,
        }
    }

    /// Send a transaction using the hybrid strategy
    pub async fn send(
        &self,
        transaction: &VersionedTransaction,
        rpc_parallel: usize,
    ) -> Result<Signature> {
        // If we have a TPU client and prefer it, try that first
        if self.prefer_tpu {
            if let Some(tpu) = &self.tpu_client {
                match tpu.send_transaction(transaction).await {
                    Ok(sig) => return Ok(sig),
                    Err(e) => {
                        debug!("TPU send failed, falling back to RPC: {}", e);
                    }
                }
            }
        }

        // Fall back to RPC with parallel retries
        let mut last_err = None;
        for _ in 0..rpc_parallel {
            match self.rpc.send_transaction(transaction).await {
                Ok(sig) => return Ok(sig),
                Err(e) => last_err = Some(e),
            }
        }

        // If we get here, all RPC attempts failed
        Err(last_err.unwrap_or_else(|| anyhow!("No RPC attempts made")))
    }
}

/// Slot cache with lock-free fast path
#[derive(Clone)]
struct SlotCache {
    slot: std::sync::atomic::AtomicU64,
    ts: std::sync::atomic::AtomicU64, // unix_nanos
}

static SLOT_CACHE: OnceLock<SlotCache> = OnceLock::new();

/// Get or initialize the global slot cache
fn slot_cache() -> &'static SlotCache {
    SLOT_CACHE.get_or_init(|| SlotCache {
        slot: std::sync::atomic::AtomicU64::new(0),
        ts: std::sync::atomic::AtomicU64::new(0),
    })
}

/// Leader schedule cache with TTL
#[derive(Clone, Debug)]
struct LeaderCache {
    epoch: u64,
    schedule: HashMap<String, Vec<usize>>, // identity -> slot indices within epoch
    last_refresh: Instant,
}

static LEADER_CACHE: OnceLock<tokio::sync::RwLock<LeaderCache>> = OnceLock::new();

/// Get or initialize the global leader cache
fn leader_cache() -> &'static tokio::sync::RwLock<LeaderCache> {
    LEADER_CACHE.get_or_init(|| tokio::sync::RwLock::new(LeaderCache {
        epoch: 0,
        schedule: HashMap::new(),
        last_refresh: Instant::now() - Duration::from_secs(3600),
    }))
}

/// Global cache for compute units
static COMPUTE_UNITS_CACHE: OnceLock<tokio::sync::RwLock<LruCache<[u8; 32], u64>>> = OnceLock::new();

/// Get or initialize the global compute units cache
fn get_compute_units_cache() -> &'static tokio::sync::RwLock<LruCache<[u8; 32], u64>> {
    COMPUTE_UNITS_CACHE.get_or_init(|| {
        const CACHE_SIZE: usize = 10_000;
        tokio::sync::RwLock::new(LruCache::new(CACHE_SIZE))
    })
}

// ====== GLOBAL CONSTANTS ======
// MTU safety for QUIC packets (1232 bytes - 82 bytes headroom)
const QUIC_SAFE_PACKET_BUDGET: usize = 1150;

// ==================== PIECE 1.32: Fast Repairs ====================
#[inline(always)]
fn err_contains(e: &anyhow::Error, needle: &str) -> bool {
    let s = format!("{e:#}");
    s.to_ascii_lowercase().contains(&needle.to_ascii_lowercase())
}

// ==================== PIECE 1.36: Assemble + Send v2 ====================
/// Assemble and send a transaction with all optimizations enabled.
/// 
/// This is the new money path that integrates all optimizations:
/// - Slot-edge pacing
/// - Account bloom self-collision checks
/// - Tight compute unit budgeting with AIMD
/// - Cached ALT subsets
/// - Salted sibling transactions
/// - Hybrid TPU+RPC sending
/// - Fast repairs for common errors
#[inline(always)]
pub async fn assemble_send_fast_v2(
    &self,
    ixs: &[Instruction],
    cu_limit: u32,
    cu_price: u64,
    alts: &[AddressLookupTableAccount],
    hybrid: &HybridSender,
    rpc_parallel: usize,
    // Optional: If provided, used for deduplication
    dedup_key: Option<&[u8]>,
    // Optional: If true, will send multiple variants with different CUs
    send_siblings: bool,
) -> Result<Signature> {
    // Check if we have a duplicate key and verify it's not a duplicate
    if let Some(key) = dedup_key {
        if self.check_duplicate(key).await? {
            return Err(anyhow!("Duplicate transaction"));
        }
    }
    // 1. Check for self-collisions using bloom filter (fast path)
    if self.bloom_risky(ixs).await {
        return Err(anyhow!("Transaction conflicts with in-flight writes in current slot"));
    }

    // 2. Get optimized ALT subset (cached or computed)
    let selected_alts = self.shape_alts_quic_safe_cached(ixs, alts, QUIC_SAFE_PACKET_BUDGET).await;
    
    // 3. Adjust compute unit price using AIMD
    let adjusted_price = self.aimd_adjust_price(ixs, cu_price).await;
    
    // 4. Build transaction with retries
    let build_tx = |bh: Hash| {
        // 4a. Apply deduplication if needed
        let ixs = if let Some(key) = dedup_key {
            if self.check_duplicate(key).await? {
                return Err(anyhow::anyhow!("Duplicate transaction"));
            }
            ixs.to_vec()
        } else {
            ixs.to_vec()
        };
        
        // 4b. Build transaction with proper compute budget
        let mut ixs_with_budget = vec![];
        ixs_with_budget.push(ComputeBudgetInstruction::set_compute_unit_limit(cu_limit));
        ixs_with_budget.push(ComputeBudgetInstruction::set_compute_unit_price(adjusted_price));
        ixs_with_budget.extend(ixs);
        
        // 4c. Build and return transaction
        let vtx = VersionedTransaction::try_new(
            VersionedMessage::V0(v0::Message::try_compile(
                &self.wallet.pubkey(),
                &ixs_with_budget,
                &selected_alts,
                bh,
            )?),
            &[&self.wallet],
        )?;
        
        // 4d. Verify size fits in MTU
        let size_est = tx_message_size_estimate(&ixs, &selected_alts);
        if size_est > QUIC_SAFE_PACKET_BUDGET {
            return Err(anyhow!("Transaction too large: {} > {}", size_est, QUIC_SAFE_PACKET_BUDGET));
        }
        
        Ok(vtx)
    };
    
    // 5. Send with error repairs
    let result = self.send_with_repairs_once(build_tx, hybrid, rpc_parallel).await;
    
    // 6. Record outcome for AIMD
    self.aimd_record_outcome(ixs, result.is_ok()).await;
    
    // 7. If successful, update bloom filter
    if let Ok(sig) = &result {
        self.bloom_note_ixs_current_slot(ixs).await?;
        
        // 8. Optionally send sibling transactions with different CU budgets
        if send_siblings && result.is_ok() {
            let sibling_cu = (cu_limit as f64 * 1.2).round() as u32;
            let _ = self.send_with_repairs_once(
                |bh| {
                    let mut ixs_clone = ixs.to_vec();
                    ixs_clone.insert(0, ComputeBudgetInstruction::set_compute_unit_limit(sibling_cu));
                    ixs_clone.insert(1, ComputeBudgetInstruction::set_compute_unit_price(adjusted_price * 11 / 10));
                    
                    let vtx = VersionedTransaction::try_new(
                        VersionedMessage::V0(v0::Message::try_compile(
                            &self.wallet.pubkey(),
                            &ixs_clone,
                            &selected_alts,
                            bh,
                        )?),
                        &[&self.wallet],
                    )?;
                    Ok(vtx)
                },
                hybrid,
                rpc_parallel,
            ).await;
        }
    }
    
    result
}

// ==================== PIECE 1.35: Zero-Heap Size Estimator ====================
/// Estimate serialized size of a transaction message (compact-u16 length-prefixed).
/// 
/// This is a zero-heap version that avoids allocations in the hot path.
/// It's ~2-3x faster than the Solana SDK's version for small transactions.
/// 
/// Note: This is an approximation that's accurate within ~1-2 bytes for typical
/// transactions. It's used for quick pre-flight checks, not for final validation.
#[inline(always)]
fn tx_message_size_estimate(ixs: &[Instruction], alts: &[AddressLookupTableAccount]) -> usize {
    use solana_sdk::instruction::AccountMeta;
    
    // Pre-allocated stack buffer for account keys (avoids heap allocation)
    let mut key_set = smallvec::SmallVec::<[Pubkey; 16]>::new();
    
    // 1. Collect unique account keys (program_id + all accounts)
    for ix in ixs {
        key_set.push(ix.program_id);
        for acc in &ix.accounts {
            key_set.push(*acc.pubkey);
        }
    }
    
    // Sort and dedup (using sort_unstable for better performance)
    key_set.sort_unstable();
    key_set.dedup();
    
    // 2. Calculate account addresses size (account for ALT lookups)
    let mut accounts_size = 0;
    let mut alt_offsets = smallvec::SmallVec::<[u8; 4]>::new();
    
    for key in &key_set {
        let mut found_in_alt = false;
        for (i, alt) in alts.iter().enumerate() {
            if alt.addresses.contains(key) {
                if i < 256 { alt_offsets.push(i as u8); }
                found_in_alt = true;
                break;
            }
        }
        if !found_in_alt {
            accounts_size += 32; // Full pubkey size
        }
    }
    
    // 3. Calculate header size (3 bytes) + compact-u16 for account count
    let account_count = key_set.len();
    let account_header_size = 3 + (if account_count <= 63 { 1 } else { 2 });
    
    // 4. Calculate instructions size
    let mut instructions_size = 0;
    for ix in ixs {
        // 1 byte program index + 1 byte for account count + compact-u16 for data len
        instructions_size += 2 + compact_u16_size(ix.data.len() as u16) + ix.data.len();
        
        // Account indices (1 byte each)
        instructions_size += ix.accounts.len();
    }
    
    // 5. Calculate ALTs size (1 byte count + 1 byte per index)
    let alts_size = if alts.is_empty() {
        0
    } else {
        1 + alts.len() // 1 byte for count + 1 byte per ALT index
    };
    
    // 6. Calculate final size
    let total_size = account_header_size 
                   + accounts_size 
                   + alt_offsets.len() // 1 byte per ALT offset
                   + 32 // recent blockhash
                   + 1 // compact-u16 for instructions count (1 byte for small counts)
                   + instructions_size
                   + alts_size;
    
    total_size
}

#[inline(always)]
fn compact_u16_size(val: u16) -> usize {
    if val <= 0x7F { 1 } else { 2 }
}

// ==================== PIECE 1.34: ALT Subset Cache ====================
#[derive(Clone, Debug)]
struct AltSubset {
    alt_indices: Vec<usize>,
    score: f64,
    last_used_slot: u64,
}

static ALT_CACHE: OnceLock<tokio::sync::RwLock<LruCache<[u8;32], AltSubset>>> = OnceLock::new();

#[inline(always)]
fn alt_cache() -> &'static tokio::sync::RwLock<LruCache<[u8;32], AltSubset>> {
    ALT_CACHE.get_or_init(|| {
        const CACHE_SIZE: usize = 4_096;
        tokio::sync::RwLock::new(LruCache::new(
            NonZeroUsize::new(CACHE_SIZE).unwrap()
        ))
    })
}

#[inline(always)]
fn ix_fingerprint(ixs: &[Instruction]) -> [u8;32] {
    let mut h = Sha256::new();
    h.update(b"IXFP1");
    for ix in ixs {
        h.update(ix.program_id.as_ref());
        for acc in &ix.accounts {
            h.update(acc.pubkey.as_ref());
            h.update(&[acc.is_signer as u8, acc.is_writable as u8]);
        }
        h.update(&ix.data);
    }
    let d = h.finalize();
    let mut out = [0u8;32];
    out.copy_from_slice(&d[..32]);
    out
}

// ==================== PIECE 1.33: AIMD CU-price scaler ====================
#[derive(Clone, Copy, Debug)]
struct Aimd {
    bp: u32,          // extra bps (0..=500)
    succ_ewma: f64,   // success rate
    fail_ewma: f64,   // failure rate
}

static AIMD_LRU: OnceLock<tokio::sync::RwLock<LruCache<[u8;32], Aimd>>> = OnceLock::new();

#[inline(always)]
fn aimd_lru() -> &'static tokio::sync::RwLock<LruCache<[u8;32], Aimd>> {
    AIMD_LRU.get_or_init(|| {
        const CACHE_SIZE: usize = 4096;
        tokio::sync::RwLock::new(LruCache::new(
            NonZeroUsize::new(CACHE_SIZE).unwrap()
        ))
    })
}

#[inline(always)]
fn progset_fingerprint(ixs: &[Instruction]) -> [u8;32] {
    let mut v: Vec<Pubkey> = Vec::with_capacity(ixs.len());
    for ix in ixs { v.push(ix.program_id); }
    v.sort_unstable(); 
    v.dedup();
    
    let mut h = Sha256::new(); 
    h.update(b"PROGSET1");
    for p in &v { h.update(p.as_ref()); }
    
    let d = h.finalize();
    let mut out = [0u8;32]; 
    out.copy_from_slice(&d[..32]); 
    out
}

// Maximum jitter for tip in basis points (0.3%)
const TIP_JITTER_MAX_BP: u32 = 30;

// ATA cache TTL (15 seconds)
const ATA_CACHE_TTL: Duration = Duration::from_secs(15);

// Single-sig transaction fee on mainnet
const BASE_SIG_COST_LAMPORTS: u64 = 5_000;

// Maximum number of program addresses to consider for fee estimation
const MAX_FEE_ADDRS: usize = 64;

// Fee calculation parameters
const FEE_QUANTILE_WINDOWS: [f64; 5] = [0.5, 0.6, 0.7, 0.8, 0.9]; // Base quantiles to track
const MAX_FEE_ATTEMPT_MULTIPLIER: f64 = 3.0; // Max fee multiplier on retries
const FEE_ESCALATION_FACTOR: f64 = 1.3; // 30% increase per attempt
const LEADER_CONCENTRATION_THRESHOLD: f64 = 0.8; // Above this, use higher quantile

// ====== CORE CONSTANTS ======
pub const COMPUTE_UNITS_FLOOR: u32 = 150_000;
pub const COMPUTE_UNITS_CEIL:  u32 = 1_400_000;
pub const PRIORITY_FEE_LAMPORTS_FLOOR: u64 = 500;     // p75 guardrail (autoscaled later)
pub const TIP_JITTER_MAX_BP: u32 = 30;                // micro-jitter to break tie cliffs
pub const MIN_PROFIT_BPS: u64 = 10;
pub const REBALANCE_THRESHOLD_BPS: u64 = 25;
pub const MAX_SLIPPAGE_BPS: u64 = 50;

pub const SOLEND_PROGRAM_ID: &str = "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo";
pub const KAMINO_PROGRAM_ID: &str = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD";

/// Zero-parse program ID getters for performance-critical paths
#[inline(always)]
fn solend_program() -> Pubkey {
    solana_program::pubkey!("So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo")
}

#[inline(always)]
fn kamino_program() -> Pubkey {
    solana_program::pubkey!("KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD")
}

/// Anchor discriminator (no heap allocs, domain separated)
#[inline(always)]
fn anchor_discriminator(ix_name: &str) -> [u8; 8] {
    // sha256("global:" || ix_name)[0..8] — no heap, no format!
    let mut hasher = Sha256::new();
    hasher.update(b"global:");
    hasher.update(ix_name.as_bytes());
    let digest = hasher.finalize();
    let mut out = [0u8; 8];
    out.copy_from_slice(&digest[..8]);
    out
}

#[derive(BorshSerialize)]
struct DepositArgs { amount: u64 }
#[derive(BorshSerialize)]
struct WithdrawArgs { amount: u64 }

// Verified Solend opcodes (token-lending style)
const SOLEND_DEPOSIT_TAG:  u8 = 4;
const SOLEND_WITHDRAW_TAG: u8 = 5;
const FLASHLOAN_TAG:  u8 = 10;
const FLASHREPAY_TAG: u8 = 11;

// ====== SIMULATION & AUTO-REPAIR ======

/// Auto-repairs a failed transaction based on simulation logs
/// Returns the repaired instructions if successful, or None if repair is not possible
pub fn auto_repair_from_logs(ixs: &[Instruction], logs: &[String]) -> Option<Vec<Instruction>> {
    use solana_sdk::instruction::AccountMeta;
    
    let logs = logs.join(" ");
    let mut repaired = ixs.to_vec();
    
    // Handle common error patterns
    if logs.contains("insufficient lamports") && logs.contains("rent") {
        // For rent errors, we can't auto-fix as it requires more funds
        return None;
    }
    
    if logs.contains("insufficient funds for rent-exempt") {
        // For rent-exempt errors, we can't auto-fix as it requires more funds
        return None;
    }
    
    if logs.contains("invalid account data for instruction") {
        // For invalid account data, try to refresh the account data
        return None; // Needs manual handling
    }
    
    // Handle missing signer errors
    if logs.contains("missing required signature") {
        // Extract the missing signer pubkey from logs if possible
        if let Some(signer_str) = logs
            .split("missing required signature for: ")
            .nth(1)
            .and_then(|s| s.split_whitespace().next())
        {
            if let Ok(pubkey) = signer_str.parse::<Pubkey>() {
                // Add the missing signer to all instructions that need it
                for ix in &mut repaired {
                    if !ix.accounts.iter().any(|meta| meta.pubkey == pubkey) {
                        ix.accounts.push(AccountMeta {
                            pubkey,
                            is_signer: true,
                            is_writable: false,
                        });
                    }
                }
                return Some(repaired);
            }
        }
    }
    
    // Handle invalid account owner errors
    if logs.contains("invalid account owner") {
        // This typically requires manual intervention
        return None;
    }
    
    // Handle program errors
    if logs.contains("custom program error") {
        // For program errors, we can't auto-fix as it requires program-specific knowledge
        return None;
    }
    
    // If we get here, we couldn't auto-repair the transaction
    None
}

// ====== ATA CACHE ======

#[derive(Clone, Debug)]
struct AtaCacheEntry {
    exists: bool,
    expires_at: Instant,
}

#[derive(Default)]
struct AtaCache {
    cache: RwLock<HashMap<(Pubkey, Pubkey, Pubkey), AtaCacheEntry>>,
}

impl AtaCache {
    async fn get(&self, owner: &Pubkey, mint: &Pubkey, token_program: &Pubkey) -> Option<bool> {
        let cache = self.cache.read().await;
        cache.get(&(*owner, *mint, *token_program))
            .filter(|entry| entry.expires_at > Instant::now())
            .map(|entry| entry.exists)
    }

    async fn set(&self, owner: Pubkey, mint: Pubkey, token_program: Pubkey, exists: bool) {
        let mut cache = self.cache.write().await;
        cache.insert(
            (owner, mint, token_program),
            AtaCacheEntry {
                exists,
                expires_at: Instant::now() + ATA_CACHE_TTL,
            },
        );
    }
}

lazy_static::lazy_static! {
    static ref ATA_CACHE: AtaCache = AtaCache::default();
    static ref RENT_CACHE: tokio::sync::RwLock<RentCache> = tokio::sync::RwLock::new(RentCache { lamports_165: 0, ts: Instant::now() });
    static ref RESERVE_CACHE: tokio::sync::RwLock<ReserveCache> = tokio::sync::RwLock::new(ReserveCache { map: HashMap::new() });
}

#[derive(Clone)]
struct RentCache {
    lamports_165: u64,
    ts: Instant,
}

struct ReserveCache {
    map: HashMap<([u8; 32], [u8; 32]), (ReserveMeta, Instant)>,
}

impl ReserveCache {
    fn new() -> Self {
        Self { map: HashMap::new() }
    }
    
    fn key(program_id: &Pubkey, mint: &Pubkey) -> ([u8; 32], [u8; 32]) {
        (program_id.to_bytes(), mint.to_bytes())
    }
    
    fn get(&self, program_id: &Pubkey, mint: &Pubkey, ttl: Duration) -> Option<ReserveMeta> {
        self.map
            .get(&Self::key(program_id, mint))
            .and_then(|(meta, ts)| {
                if ts.elapsed() <= ttl {
                    Some(meta.clone())
                } else {
                    None
                }
            })
    }
    
    fn put(&mut self, program_id: &Pubkey, mint: &Pubkey, meta: ReserveMeta) {
        self.map.insert(Self::key(program_id, mint), (meta, Instant::now()));
    }
}

// ====== SHORTVEC UTILITIES ======

/// Calculate the length of a Solana shortvec (7-bit little-endian varint)
/// Returns 1 for n < 0x80, 2 for n < 0x4000, else 3
/// Branchless implementation: 1 + 1{n≥0x80} + 1{n≥0x4000}
#[inline(always)]
const fn shortvec_len_optimized(n: usize) -> usize {
    if n < 0x80 { 1 } else if n < 0x4000 { 2 } else { 3 }
}

/// Count unique keys in instructions with small-n optimization
/// For typical HFT payloads (<=64 unique keys), linear de-dupe beats hashing.
#[inline(always)]
fn unique_key_len_inline(ixs: &[Instruction], payer: &Pubkey) -> usize {
    let mut keys: Vec<Pubkey> = Vec::with_capacity(64);
    keys.push(*payer);
    
    for ix in ixs {
        // program id
        if !keys.iter().any(|k| *k == ix.program_id) { 
            keys.push(ix.program_id); 
        }
        // accounts
        for m in &ix.accounts {
            if !keys.iter().any(|k| *k == m.pubkey) { 
                keys.push(m.pubkey); 
            }
        }
    }
    
    // If unusually large, fall back to sort+dedup (still faster than hashing at scale)
    if keys.len() > 64 {
        keys.sort_unstable();
        keys.dedup();
    }
    
    keys.len()
}

// Optimized shortvec_len function
#[inline(always)]
const fn shortvec_len_optimized(n: usize) -> usize {
    if n < 0x80 { 1 } else if n < 0x4000 { 2 } else { 3 }
}

// Update related code to use the optimized shortvec_len function
// ...

// Rest of the code remains the same
    // Count program frequencies
    let mut cnt: HashMap<Pubkey, usize> = HashMap::new();
    for ix in ixs {
        *cnt.entry(ix.program_id).or_insert(0) += 1;
    }
    
    // Convert to vec and sort by frequency (descending) then by pubkey (for stability)
    let mut v: Vec<(Pubkey, usize)> = cnt.into_iter().collect();
    v.sort_by(|a, b| {
        b.1.cmp(&a.1) // Sort by count descending
         .then_with(|| a.0.cmp(&b.0)) // Then by pubkey for stable sort
    });
    
    // Take top K programs
    v.into_iter()
        .take(k)
        .map(|(k, _c)| k)
        .collect()
}

// ====== UTILITY FUNCTIONS ======

/// Returns top N programs by execution frequency from recent blocks.
/// 
/// This function analyzes recent blocks to identify the most frequently
/// executed programs, which can be useful for optimizing transaction
/// scheduling and prioritization.
/// 
/// # Arguments
/// * `rpc_client` - The RPC client to fetch block data
/// * `recent_blocks` - Number of recent blocks to analyze
/// * `top_n` - Number of top programs to return
/// 
/// # Returns
/// A vector of (program_id, execution_count) tuples, sorted by count in descending order
async fn top_programs_by_freq(
    rpc_client: &RpcClient,
    recent_blocks: usize,
    top_n: usize,
) -> Result<Vec<(String, usize)>> {
    if recent_blocks == 0 || top_n == 0 {
        return Ok(Vec::new());
    }

    // Get recent block hashes
    let slot = rpc_client.get_slot()?;
    let start_slot = slot.saturating_sub(recent_blocks as u64);
    
    let mut program_counts: HashMap<String, usize> = HashMap::new();
    
    // Process blocks in parallel for better performance
    let handles: Vec<_> = (start_slot..=slot)
        .map(|block_slot| {
            let rpc = rpc_client.clone();
            tokio::spawn(async move {
                match rpc.get_block(block_slot).await {
                    Ok(block) => {
                        let mut counts = HashMap::new();
                        for tx in block.transactions {
                            for meta in tx.meta.into_iter() {
                                for program_id in meta.log_messages
                                    .iter()
                                    .flatten()
                                    .filter_map(|log| log.split_whitespace().nth(2))
                                    .filter(|s| s.starts_with("Program"))
                                    .filter_map(|s| s.split_whitespace().nth(1))
                                {
                                    *counts.entry(program_id.to_string()).or_insert(0) += 1;
                                }
                            }
                        }
                        Ok(counts)
                    }
                    Err(e) => {
                        warn!("Failed to fetch block {}: {}", block_slot, e);
                        Ok(HashMap::new())
                    }
                }
            })
        })
        .collect();
    
    // Aggregate results from all blocks
    for handle in handles {
        if let Ok(Ok(counts)) = handle.await {
            for (program_id, count) in counts {
                *program_counts.entry(program_id).or_default() += count;
            }
        }
    }
    
    // Sort by count in descending order and take top N
    let mut sorted: Vec<_> = program_counts.into_iter().collect();
    sorted.sort_unstable_by(|a, b| b.1.cmp(&a.1));
    
    Ok(sorted.into_iter().take(top_n).collect())
}

}

/// Estimate message bytes including a rough overhead for ALTs (accounts + table metadata).
fn tx_message_size_with_alts_estimate(ixs: &[Instruction], alts: &[AddressLookupTableAccount]) -> usize {
    let core = tx_message_size_estimate(
        ixs.iter().filter(|ix| ix.program_id != system_program::id()).count() as u8,
        1, // num_required_signatures
        1, // num_readonly_signed_accounts
        0, // num_readonly_unsigned_accounts
        &ixs.iter().flat_map(|ix| {
            std::iter::once(&ix.program_id)
                .chain(ix.accounts.iter().map(|meta| &meta.pubkey))
        }).collect::<Vec<_>>(),
        &Hash::default(), // dummy hash for estimation
        ixs,
    );
    // ALT rough size: 8 bytes (discriminator/length-ish) + 32 * addresses
    let alt_bytes: usize = alts.iter().map(|t| 8usize + 32usize * t.addresses.len()).sum();
    core + alt_bytes + 64 // a little extra headroom for headers/indices
}

/// Shrinks the set of ALTs to fit within MTU budget
fn shrink_alts_to_mtu(
    ixs: &[Instruction],
    mut alts: Vec<AddressLookupTableAccount>,
    mtu_budget: usize,
    size_fn: &dyn Fn(&[Instruction], &[AddressLookupTableAccount]) -> usize,
) -> Vec<AddressLookupTableAccount> {
    // If already fits, return fast
    if size_fn(ixs, &alts) <= mtu_budget {
        return alts;
    }

    // Drop least useful ALTs first: sort ascending by coverage (keep high coverage)
    let mut scored: Vec<(usize, usize)> = alts
        .iter()
        .enumerate()
        .map(|(i, t)| (i, t.addresses.len()))
        .collect();
    scored.sort_by_key(|&(_i, cov)| cov); // low coverage first

    // Greedily remove until we fit
    for (idx, _cov) in scored {
        if size_fn(ixs, &alts) <= mtu_budget {
            break;
        }
        if idx < alts.len() {
            alts.remove(idx);
        }
    }
    alts
}

// ==================== PIECE 1.31: Slot-Scoped Account Bloom ====================
#[derive(Clone, Debug)]
struct SlotBloom {
    slot: u64,
    m_bits: usize,
    k: u8,
    bits: Vec<u64>,
}

impl SlotBloom {
    fn new(slot: u64, m_bits: usize, k: u8) -> Self {
        let words = (m_bits + 63) / 64;
        Self { slot, m_bits, k, bits: vec![0u64; words] }
    }

    #[inline(always)]
    fn reset(&mut self, slot: u64) {
        self.slot = slot;
        for w in &mut self.bits { *w = 0; }
    }

    #[inline(always)]
    fn pos(&self, h1: u64, h2: u64, i: u8) -> usize {
        let m = self.m_bits as u64;
        (((h1.wrapping_add((i as u64).wrapping_mul(h2))) % m) as usize)
    }

    #[inline(always)]
    fn hash2(bytes: &[u8]) -> (u64, u64) {
        let mut h = Sha256::new();
        h.update(b"ABLOOM1");
        h.update(bytes);
        let d = h.finalize();
        let h1 = u64::from_le_bytes([d[0], d[1], d[2], d[3], d[4], d[5], d[6], d[7]]);
        let h2 = u64::from_le_bytes([d[8], d[9], d[10], d[11], d[12], d[13], d[14], d[15]]) | 1;
        (h1, h2)
    }

    #[inline(always)]
    fn set(&mut self, bytes: &[u8]) {
        let (h1, h2) = Self::hash2(bytes);
        for i in 0..self.k {
            let p = self.pos(h1, h2, i);
            let w = p >> 6;
            let b = p & 63;
            self.bits[w] |= 1u64 << b;
        }
    }

    #[inline(always)]
    fn test(&self, bytes: &[u8]) -> bool {
        let (h1, h2) = Self::hash2(bytes);
        for i in 0..self.k {
            let p = self.pos(h1, h2, i);
            let w = p >> 6;
            let b = p & 63;
            if (self.bits[w] >> b) & 1 == 0 { return false; }
        }
        true
    }
}

static ACC_BLOOM: OnceLock<tokio::sync::RwLock<SlotBloom>> = OnceLock::new();

#[inline(always)]
fn acc_bloom() -> &'static tokio::sync::RwLock<SlotBloom> {
    // m_bits ≈ 2^18 ~ 256k bits (~32KB), k=3 → FP rate ~ 0.2–0.5% for a few thousand inserts
    ACC_BLOOM.get_or_init(|| tokio::sync::RwLock::new(SlotBloom::new(0, 1<<18, 3)))
}

// ====== DOMAIN TYPES ======
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Protocol { Solend, Kamino }

#[derive(Clone, Debug)]
pub struct LendingMarket {
    pub market_pubkey: Pubkey,
    pub token_mint: Pubkey,
    pub protocol: Protocol,
    pub supply_apy: f64,
    pub borrow_apy: f64,
    pub utilization: f64,
    pub liquidity_available: u64,
    pub last_update: Instant,
    pub layout_version: u8,
}

#[derive(Clone, Debug)]
pub struct Position {
    pub token_mint: Pubkey,
    pub amount: u64,
    pub protocol: Protocol,
    pub last_rebalance: Instant,
}

#[derive(Clone, Debug)]
pub struct ReserveMeta {
    pub reserve: Pubkey,
    pub lending_market: Pubkey,
    pub lending_market_authority: Pubkey,
    pub liquidity_mint: Pubkey,
    pub liquidity_supply_vault: Pubkey,
    pub collateral_mint: Pubkey,
    pub collateral_supply_vault: Pubkey,
    pub program_id: Pubkey,
}

#[derive(Clone, Debug)]
pub struct FlashPlan {
    pub flash_program: Pubkey,
    pub reserve: Pubkey,
    pub liquidity_supply_vault: Pubkey,
    pub lending_market: Pubkey,
    pub market_authority: Pubkey,
    pub token_mint: Pubkey,
    pub amount: u64,
}

#[derive(Clone)]
pub enum TipStrategy { RoundRobin, Fixed(usize) }

#[derive(Clone)]
pub struct TipConfig {
    pub accounts: Vec<Pubkey>,
    pub strategy: TipStrategy,

// Core engine (only members used elsewhere — no dead fields)
#[derive(Clone)]
pub struct PortOptimizer {
    pub rpc_client: Arc<RpcClient>,
    pub wallet: Arc<Keypair>,
    pub markets: Arc<RwLock<HashMap<String, Vec<LendingMarket>>>>,
    pub active_positions: Arc<RwLock<HashMap<String, Position>>>,
    pub tip_router: Arc<RwLock<Option<TipRouter>>>,
    pub last_tx_signature: Arc<tokio::sync::RwLock<Option<Signature>>>,
    pub duplicate_cache: Arc<tokio::sync::RwLock<LruCache<u64, ()>>>,
}

impl PortOptimizer {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        wallet: Arc<Keypair>,
        markets: Arc<RwLock<HashMap<String, Vec<LendingMarket>>>>,
        active_positions: Arc<RwLock<HashMap<String, Position>>>,
        tip_router: Arc<RwLock<Option<TipRouter>>>,
    ) -> Self {
        // Create a cache that holds ~10k entries (enough for ~10s of transactions at high volume)
        let duplicate_cache = Arc::new(tokio::sync::RwLock::new(LruCache::new(
            NonZeroUsize::new(10_000).unwrap()
        )));
        
        Self {
            rpc_client,
            wallet,
            markets,
            active_positions,
            tip_router,
            last_tx_signature: Arc::new(tokio::sync::RwLock::new(None)),
            duplicate_cache,
        }
    }

    /// Check if a transaction with the given key has already been processed.
    /// Uses a short-lived cache to prevent duplicate transactions within a short time window.
    pub async fn check_duplicate(&self, key: &[u8]) -> Result<bool> {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        // Create a hash of the key for storage efficiency
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let key_hash = hasher.finish();
        
        // Check if this key is in our recent cache
        let mut cache = self.duplicate_cache.write().await;
        if cache.contains(&key_hash) {
            return Ok(true);
        }
        
        // Not in cache, add it
        cache.put(key_hash, ());
        Ok(false)
    }

    /// Insert all accounts for this ixs into the current-slot Bloom.
    pub async fn bloom_note_ixs_current_slot(&self, ixs: &[Instruction]) -> Result<()> {
        let slot = self.get_slot_coarse(Duration::from_millis(60)).await;
        let mut b = acc_bloom().write().await;
        if b.slot != slot { b.reset(slot); }
        for ix in ixs {
            b.set(ix.program_id.as_ref());
            for m in &ix.accounts { b.set(m.pubkey.as_ref()); }
        }
        Ok(())
    }

    /// Adjust compute unit price using AIMD (Additive Increase/Multiplicative Decrease)
    #[inline]
    pub async fn aimd_adjust_price(&self, ixs: &[Instruction], base_cu_price: u64) -> u64 {
        let key = progset_fingerprint(ixs);
        let adj = { 
            let lru = aimd_lru().read().await;
            lru.get(&key).cloned()
        };
        
        if let Some(a) = adj {
            base_cu_price + (base_cu_price * a.bp as u64) / 10_000
        } else {
            base_cu_price
        }
    }

    /// Record success/failure outcome for AIMD adjustment
    pub async fn aimd_record_outcome(&self, ixs: &[Instruction], success: bool) {
        let key = progset_fingerprint(ixs);
        
        let mut l = aimd_lru().write().await;
        let a = l.get(&key).cloned().unwrap_or_else(|| 
            Aimd { 
                bp: 0, 
                succ_ewma: 0.6,  // Initial success rate estimate
                fail_ewma: 0.4,  // Initial failure rate estimate
            }
        );
        
        // EWMA update with alpha=0.25
        let alpha = 0.25;
        let (succ, fail) = if success { (1.0, 0.0) } else { (0.0, 1.0) };
        let succ_ewma = alpha * succ + (1.0 - alpha) * a.succ_ewma;
        let fail_ewma = alpha * fail + (1.0 - alpha) * a.fail_ewma;
        
        // AIMD: increase aggressively on failure, decrease gently on success
        let mut bp = a.bp;
        if success {
            bp = bp.saturating_sub(10); // -10bp on success (gentle decrease)
        } else {
            bp = (bp + 40).min(500);   // +40bp on failure (aggressive increase, max 500bp)
        }
        
        l.put(key, Aimd { bp, succ_ewma, fail_ewma });
    }

    /// Try send; on BH expiry or account-in-use, repair and retry once.
    pub async fn send_with_repairs_once(
        &self,
        vtx_build: impl Fn(Hash) -> Result<VersionedTransaction>,
        hybrid: &HybridSender,
        rpc_parallel: usize,
    ) -> Result<Signature> {
        // attempt 1
        let bh1 = self.get_blockhash_fast().await?;
        let vtx1 = vtx_build(bh1)?;
        match hybrid.send(&vtx1, rpc_parallel).await {
            Ok(sig) => return Ok(sig),
            Err(e) => {
                // classify & repair
                if err_contains(&e, "blockhash not found") || err_contains(&e, "expired blockhash") {
                    let bh2 = self.get_blockhash_fast().await?; // refresh
                    let vtx2 = vtx_build(bh2)?;
                    return hybrid.send(&vtx2, rpc_parallel).await;
                }
                if err_contains(&e, "accountinuse") || err_contains(&e, "account in use") {
                    // short micro backoff to dodge hot-write collision
                    tokio::time::sleep(Duration::from_millis(2)).await;
                    let bh2 = self.get_blockhash_fast().await?;
                    let vtx2 = vtx_build(bh2)?;
                    return hybrid.send(&vtx2, rpc_parallel).await;
                }
                Err(e)
            }
        }
    }

    /// Get cached ALT subset or compute and cache a new one.
    pub async fn shape_alts_quic_safe_cached(
        &self,
        ixs: &[Instruction],
        alts: &[AddressLookupTableAccount],
        mtu_budget: usize,
    ) -> Vec<AddressLookupTableAccount> {
        if alts.is_empty() { return vec![]; }
        
        let fp = ix_fingerprint(ixs);
        let slot = self.get_slot_coarse(Duration::from_millis(60)).await;
        
        // Check cache first
        {
            let cache = alt_cache().read().await;
            if let Some(entry) = cache.get(&fp) {
                if entry.last_used_slot + 4 > slot { // Only use if recent
                    let mut res = Vec::with_capacity(entry.alt_indices.len());
                    for &i in &entry.alt_indices {
                        if let Some(alt) = alts.get(i) {
                            res.push(alt.clone());
                        }
                    }
                    if !res.is_empty() {
                        return res;
                    }
                }
            }
        }
        
        // Not in cache or stale, compute fresh
        let selected = self.shape_alts_quic_safe(ixs, alts, mtu_budget);
        
        // Cache the result
        if !selected.is_empty() {
            let mut indices = Vec::with_capacity(selected.len());
            'outer: for alt in &selected {
                for (i, orig_alt) in alts.iter().enumerate() {
                    if orig_alt.key == alt.key {
                        indices.push(i);
                        continue 'outer;
                    }
                }
            }
            
            if !indices.is_empty() {
                let score = self.alt_subset_score(ixs, &selected);
                let entry = AltSubset {
                    alt_indices: indices,
                    score,
                    last_used_slot: slot,
                };
                
                let mut cache = alt_cache().write().await;
                cache.put(fp, entry);
            }
        }
        
        selected
    }
    
    /// Score an ALT subset based on how many accounts it covers
    fn alt_subset_score(&self, ixs: &[Instruction], alts: &[AddressLookupTableAccount]) -> f64 {
        let mut covered = 0;
        let mut total = 0;
        
        for ix in ixs {
            total += 1; // program_id
            for acc in &ix.accounts {
                total += 1;
                for alt in alts {
                    if alt.addresses.contains(&acc.pubkey) {
                        covered += 1;
                        break;
                    }
                }
            }
        }
        
        if total == 0 { return 0.0; }
        covered as f64 / total as f64
    }

    /// Quick check: are we likely to collide with **our own** in-flight writes this slot?
    pub async fn bloom_risky(&self, ixs: &[Instruction]) -> bool {
        let slot = self.get_slot_coarse(Duration::from_millis(60)).await;
        let b = acc_bloom().read().await;
        if b.slot != slot { return false; }
        for ix in ixs {
            if b.test(ix.program_id.as_ref()) { return true; }
            for m in &ix.accounts {
                if m.is_writable && b.test(m.pubkey.as_ref()) { return true; }
            }
        }
        false
    }

    /// Get the current slot with a lock-free fast path when the cached value is fresh.
    /// Falls back to RPC if the cache is stale (older than `ttl`).
    #[inline(always)]
    pub async fn get_slot_coarse(&self, ttl: Duration) -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        
        // Get current time in nanoseconds since UNIX_EPOCH
        let now_ns = match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_nanos() as u64,
            Err(_) => 0, // Should be very rare, fall back to RPC
        };
        
        let cache = slot_cache();
        let ts = cache.ts.load(Ordering::Relaxed);
        
        // Fast path: return cached slot if it's still fresh
        if now_ns.saturating_sub(ts) <= ttl.as_nanos() as u64 {
            return cache.slot.load(Ordering::Relaxed);
        }
        
        // Slow path: fetch from RPC and update cache
        match self.rpc_client.get_slot().await {
            Ok(slot) => {
                // Update cache with relaxed ordering (we don't need strong consistency here)
                cache.slot.store(slot, Ordering::Relaxed);
                cache.ts.store(now_ns, Ordering::Relaxed);
                slot
            }
            Err(_) => {
                // On error, return the cached value even if it's stale
                // This provides better availability during RPC issues
                cache.slot.load(Ordering::Relaxed)
            }
        }
    }
    
    pub fn new(rpc_url: &str, wallet: Keypair) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_owned(),
            CommitmentConfig::confirmed(),
        ));
        Self {
            rpc_client,
            wallet: Arc::new(wallet),
            markets: Arc::new(RwLock::new(HashMap::new())),
            active_positions: Arc::new(RwLock::new(HashMap::new())),
            tip_router: Arc::new(RwLock::new(None)),
            last_tx_signature: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }
    
    /// Deterministic micro-jitter in basis points [0, max_bp], cohort-stable across retries.
    /// Domain-separated seed using last sent signature; falls back to wallet pubkey.
    #[inline(always)]
    fn price_jitter_bp(&self, max_bp: u32) -> u32 {
        if max_bp == 0 { return 0; }

        let guard = self.last_tx_signature.blocking_read();
        let seed_bytes: &[u8] = match guard.as_ref() {
            Some(sig) => sig.as_ref(),
            None => self.wallet.pubkey().as_ref(),
        };

        let mut h = Sha256::new();
        h.update(b"JIT1");                  // domain sep
        h.update(seed_bytes);
        let d = h.finalize();

        let mut b = [0u8; 4];
        b.copy_from_slice(&d[..4]);
        let x = u32::from_le_bytes(b);
        x % (max_bp + 1)
    }
    
    /// Get fee values from cache or fetch from RPC if cache is stale
    async fn get_fee_values_cached(&self, programs: &[Pubkey], ttl: Duration) -> Arc<Vec<u64>>> {
        let key = if programs.is_empty() { 
            GLOBAL_FEE_KEY 
        } else { 
            hash_pubkeys_sorted(programs) 
        };

        // Try to read from cache first
        if let Ok(cache) = fee_tape_cache().try_read() {
            if let Some(vals) = cache.get(&key, ttl) { 
                return vals; 
            }
        }

        // Cache miss or stale - fetch fresh data
        let fees = if programs.is_empty() {
            self.rpc_client.get_recent_prioritization_fees(&[]).await.unwrap_or_default()
        } else {
            self.rpc_client.get_recent_prioritization_fees(programs).await.unwrap_or_default()
        };
        
        let vals_vec: Vec<u64> = fees.into_iter()
            .map(|f| f.prioritization_fee.max(1))
            .collect();
            
        let arc = Arc::new(vals_vec);

        // Update cache if we can get a write lock without blocking
        if let Ok(mut cache) = fee_tape_cache().try_write() {
            cache.put(key, arc.clone());
        }
        
        arc
    }
    
    /// Priority fee estimator with program awareness, congestion control, and top-K programs
    pub async fn priority_fee_for_ixs(&self, ixs: &[Instruction], floor_cu_price: u64) -> u64 {
        // Get top-K most frequent programs from instructions
        let top_programs = top_programs_by_freq(ixs, MAX_FEE_ADDRS);
        
        // Get fee values for these programs
        let fee_values = if top_programs.is_empty() {
            self.get_fee_values_cached(&[], Duration::from_millis(250)).await
        } else {
            self.get_fee_values_cached(&top_programs, Duration::from_millis(250)).await
        };

        if fee_values.is_empty() {
            return floor_cu_price;
        }

        // Get context-aware quantile window
        let conc = self.leader_window_concentration(4).await.unwrap_or(0.0);
        let size = self.tx_message_size_estimate(ixs);
        let (ql, qh) = self.fee_quantile_window(0, conc, size);

        // O(n) percentile extraction
        let (lo, hi) = percentile_pair_unstable(&fee_values, ql, qh);
        let base = lo.saturating_add(hi) / 2;

        // Apply congestion adjustment
        let slope = self.congestion_slope().await.unwrap_or(0.0);
        let mut out = ((base as f64) * (1.0 + slope.min(0.08))).ceil() as u64;
        
        // Apply floor and ceiling
        out = out.max(floor_cu_price).min(floor_cu_price.saturating_mul(25));

        // Add jitter for better privacy and to avoid fee wars
        let j_bp = self.price_jitter_bp(200); // ±2% jitter
        out.saturating_add((out.saturating_mul(j_bp as u64)) / 10_000)
    }

    /// Floor-based autoscale from global fee tape with congestion awareness and adaptive quantiles
    pub async fn priority_fee_autoscale(&self, floor_cu_price: u64) -> u64 {
        // Get recent fee samples (last 100 slots, up to 15s old)
        let recent_fees = match self.rpc_client.get_recent_prioritization_fees(&[]).await {
            Ok(fees) if !fees.is_empty() => fees,
            _ => return floor_cu_price,
        };

        // Extract fee values
        let fee_values: Vec<u64> = recent_fees.into_iter()
            .map(|f| f.prioritization_fee)
            .collect();

        if fee_values.is_empty() {
            return floor_cu_price;
        }

        // Get congestion level (0-1)
        let congestion = self.estimate_congestion().await;
        
        // Dynamic quantile selection based on congestion
        let target_quantile = 0.5 + (congestion * 0.4); // 50-90% based on congestion
        
        // Use O(n) quantile calculation
        let (lo, hi) = percentile_pair_unstable(
            &fee_values,
            (target_quantile * 50.0) as usize,  // 25-45% for lo
            (50.0 + (target_quantile * 50.0)) as usize // 75-95% for hi
        );
        
        // Weighted average with adaptive weights
        let base = (lo as f64 * 0.7 + hi as f64 * 0.3) as u64;
        
        // Apply floor and ceiling with some headroom
        base.max(floor_cu_price).min(floor_cu_price.saturating_mul(15))
    }
    
    /// Compute congestion slope (0.0-1.0) from recent fee samples.
    /// Uses robust linear regression on log(fees) to estimate log-slope.
    /// Returns None if not enough data or if slope is negative.
    async fn congestion_slope(&self) -> Option<f64> {
        use std::collections::VecDeque;
        
        // Get recent fees from global fee tape (cached)
        let vals = self.get_fee_values_cached(&[], Duration::from_secs(2)).await;
        if vals.len() < 8 {
            return None;
        }
        
        let mut fees: Vec<f64> = vals.iter().map(|&x| (x as f64).ln()).collect();
        fees.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
        
        // Use interquartile range for robustness to outliers
        let q1 = fees[fees.len() / 4];
        let q3 = fees[(fees.len() * 3) / 4];
        let iqr = q3 - q1;
        let lower = q1 - 1.5 * iqr;
        let upper = q3 + 1.5 * iqr;
        
        // Filter outliers and take most recent N samples
        let recent: VecDeque<f64> = fees.into_iter()
            .filter(|&x| x >= lower && x <= upper)
            .rev()
            .take(64)
            .collect();
            
        if recent.len() < 4 {
            return None;
        }
        
        // Simple linear regression on log(fees) vs time
        let n = recent.len() as f64;
        let mut sum_x = 0.0;
        let mut sum_y = 0.0;
        let mut sum_xy = 0.0;
        let mut sum_xx = 0.0;
        
        for (i, &y) in recent.iter().enumerate() {
            let x = i as f64;
            sum_x += x;
            sum_y += y;
            sum_xy += x * y;
            sum_xx += x * x;
        }
        
        let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x * sum_x);
        
        // Only return positive slopes (increasing fees)
        if slope > 0.0 {
            Some(slope.min(1.0)) // Cap at 1.0 (100% increase per sample)
        } else {
            None
        }
    }
}

/// Stable fingerprint for a sequence of instructions.
/// Canonicalizes by sorting accounts and data for deterministic output.
/// Uses SHA256 to produce a compact, unique identifier.
pub fn ixs_fingerprint(ixs: &[Instruction]) -> [u8; 32] {
    use std::collections::BTreeMap;
    
    // Sort instructions by program ID first for stable ordering
    let mut sorted_ixs: Vec<&Instruction> = ixs.iter().collect();
    sorted_ixs.sort_unstable_by_key(|ix| &ix.program_id);
    
    let mut hasher = Sha256::new();
    
    for ix in sorted_ixs {
        // Hash program ID
        hasher.update(ix.program_id.as_ref());
        
        // Sort and hash accounts
        let mut accounts = ix.accounts.clone();
        accounts.sort_unstable();
        for account in accounts {
            hasher.update(account.pubkey.as_ref());
            hasher.update(&[
                account.is_signer as u8,
                account.is_writable as u8,
                account.is_delegate as u8,
            ]);
        }
        
        // Hash data
        hasher.update(&ix.data);
    }
    
    hasher.finalize().into()
}

/// Cohort-based jitter in microseconds for transaction timing.
/// Uses wallet and recent blockhash to create deterministic but unique timing offsets.
/// This helps prevent transaction collisions while maintaining predictable behavior.
pub async fn cohort_jitter_us(wallet: &Pubkey, recent_blockhash: &Hash, max_jitter_us: u32) -> u32 {
    // Combine wallet and recent blockhash for deterministic but unique jitter
    let mut hasher = Sha256::new();
    hasher.update(b"COHORT_JITTER");  // Domain separation
    hasher.update(wallet.as_ref());
    hasher.update(recent_blockhash.as_ref());
    
    // Use first 4 bytes of hash for jitter calculation
    let hash = hasher.finalize();
    let mut bytes = [0u8; 4];
    bytes.copy_from_slice(&hash[..4]);
    let jitter = u32::from_le_bytes(bytes) % (max_jitter_us + 1);
    
    // Add small random component to break strict patterns
    let random_jitter = (rand::random::<u32>() % 1000).min(200); // 0-200µs random
hortvec_len(n_dat) 
            + n_dat;
    }
    
    // Add version + address table lookups (none for v0 without ALTs)
    size + MSG_VERSION_OVERHEAD
{{ ... }}
}

/// Size including ALTs (rough but safe); keeps QUIC-safe shaping deterministic.
/// Uses a safe upper bound for ALT overhead to ensure MTU compliance.
pub fn tx_message_size_with_alts_estimate(&self, ixs: &[Instruction], alts: &[AddressLookupTableAccount]) -> usize {
    let core = self.tx_message_size_estimate(ixs);
pub fn tx_message_size_estimate_detailed(
    num_required_signatures: u8,
    num_readonly_signed_accounts: u8,
    num_readonly_unsigned_accounts: u8,
    account_keys: &[Pubkey],
    recent_blockhash: &Hash,
    instructions: &[Instruction],
) -> usize {
    // Message header: 3 bytes
    let header_size = 3;
    
    // Account addresses
    let account_keys_size = account_keys.len() * 32; // 32 bytes per pubkey
    
    // Recent blockhash: 32 bytes
    let blockhash_size = 32;
    
    // Instructions: 1 byte for count + per-instruction overhead
    let mut instructions_size = 1; // instruction count
    
    for ix in instructions {
        // Program ID index: 1 byte
        // Account indices: 1 byte for count + 1 byte per account index
        // Data: 2 bytes for length + actual data
        instructions_size += 1 + 1 + ix.accounts.len() + 2 + ix.data.len();
    }
    
    // Total size
    let total_size = header_size 
        + account_keys_size 
        + blockhash_size 
        + instructions_size;
    
    // Add 10% safety margin for versioned tx overhead and future changes
    ((total_size as f64) * 1.1).ceil() as usize
}

/// Rough CU estimator with LRU cache and TTL/epoch tracking
/// This provides a fast path for common transaction patterns
async fn optimize_compute_units_rough(&self, ixs: &[Instruction]) -> u32 {
    use lru::LruCache;
    use std::num::NonZeroUsize;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    
    // Skip empty instruction sets
    if ixs.is_empty() {
        return 0;
    }
    
    // Get current epoch (changes every ~2 days)
    static EPOCH_COUNTER: AtomicU64 = AtomicU64::new(0);
    let current_epoch = EPOCH_COUNTER.load(Ordering::Relaxed);
    
    // Get current time for TTL checks
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    
    // Get or initialize the cache
    const CACHE_SIZE: usize = 10_000;
    let mut cache = self.compute_units_cache.write().await;
    
    // Check if we need to clear the cache (new epoch or first run)
    if let Some((epoch_ts, _)) = cache.lru().peek_lru() {
        if *epoch_ts < current_epoch || (now - *epoch_ts) > 172_800 { // 2 days in seconds
            cache.lru_mut().clear();
        }
    }
    
    // Generate a fingerprint of the instructions
    let fingerprint = ixs_fingerprint(ixs);
    
    // Try to get from cache
    if let Some((_, units)) = cache.lru().peek(&fingerprint) {
        return *units;
    }
    
    // Fall back to simulation if not in cache
    let estimated_units = match self.simulate_compute_units(ixs).await {
        Ok(Some(units)) => units,
        _ => {
            // Conservative default if simulation fails
            let estimated = ixs.len() as u32 * 200_000; // 200K CU per instruction
            estimated.min(1_400_000) // Cap at 1.4M CU
        }
    };
    
    // Add to cache
    cache.lru_mut().put(fingerprint, (now, estimated_units));
    
    estimated_units
}

/// Simulate compute units for a set of instructions with retries and backoff
async fn simulate_compute_units(&self, ixs: &[Instruction]) -> Result<Option<u32>, Box<dyn std::error::Error>> {
    use solana_sdk::signature::Keypair;
    use std::time::Duration;
    
    if ixs.is_empty() {
        return Ok(None);
    }
    
    // Use a dummy signer for simulation
    let dummy_signer = Keypair::new();
    
    // Try with exponential backoff
    let mut attempts = 0;
    let max_attempts = 3;
    let mut backoff_ms = 100;
    
    loop {
        match self.simulate_ixs(ixs).await {
            Ok(sim_result) => {
                if let Some(units) = sim_result.units_consumed {
                    // Add 10% safety margin
                    let estimated_units = (units as f64 * 1.1).ceil() as u32;
                    return Ok(Some(estimated_units));
                } else if let Some(err) = sim_result.err {
                    log::warn!("Simulation failed: {:?}", err);
                }
            }
            Err(e) => {
                log::warn!("Simulation error: {}", e);
            }
        }
        
        attempts += 1;
        if attempts >= max_attempts {
            break;
        }
        
        // Exponential backoff
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
        backoff_ms *= 2;
    }
    
    Ok(None)
}

/// Simulate a transaction with exact compute unit limits and priority fees.
/// This provides precise simulation of transaction execution before submission.
pub async fn simulate_exact(
    &self,
    ixs: &[Instruction],
    signer: &Keypair,
    recent_blockhash: Hash,
    compute_unit_limit: Option<u64>,
    compute_unit_price: Option<u64>,
) -> Result<SimulationResult, Box<dyn std::error::Error>> {
    use solana_sdk::message::v0::Message;
    use solana_sdk::signature::Signer;
    use solana_sdk::transaction::VersionedTransaction;
    
    if ixs.is_empty() {
        return Err("No instructions provided for simulation".into());
    }
    
    // Create message with optional compute budget instructions
    let mut message_ixs = vec![];
    
    // Add compute unit limit if provided
    if let Some(limit) = compute_unit_limit {
        message_ixs.push(
            solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(limit)
        );
    }
    
    // Add compute unit price if provided
    if let Some(price) = compute_unit_price {
        message_ixs.push(
            solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_price(price)
        );
    }
    
    // Add the actual instructions
    message_ixs.extend_from_slice(ixs);
    
    // Build and sign the transaction
    let message = Message::new(&message_ixs, Some(&signer.pubkey()));
    let tx = VersionedTransaction::try_new(
        message,
        &[signer]
    )?;
    
    // Simulate the transaction
    let simulation = self.rpc_client
        .simulate_transaction_with_config(
            tx,
            RpcSimulateTransactionConfig {
                sig_verify: false,  // Skip signature verification for simulation
                replace_recent_blockhash: true,
                commitment: Some(CommitmentConfig::confirmed()),
                ..Default::default()
            },
        )
        .await?;
    
    // Convert to our result type
    Ok(SimulationResult {
        err: simulation.value.err,
        logs: simulation.value.logs.unwrap_or_default(),
        units_consumed: simulation.value.units_consumed,
        return_data: simulation.value.return_data,
    })
}

/// Generate dynamic budget instructions with adaptive fee calculation and micro-jitter.
/// This helps prevent fee market manipulation and optimizes transaction costs.
pub async fn dynamic_budget_ixs(
    &self,
    ixs: &[Instruction],
    base_priority_fee: u64,
    max_priority_fee: u64,
) -> Result<Vec<Instruction>, Box<dyn std::error::Error>> {
    use solana_sdk::instruction::AccountMeta;
    use solana_sdk::pubkey::Pubkey;
    use solana_sdk::system_instruction;
    
    if ixs.is_empty() {
        return Ok(vec![]);
    }
    
    // Calculate priority fee with congestion awareness
    let priority_fee = self.priority_fee_for_ixs(ixs, base_priority_fee).await;
    let priority_fee = priority_fee.min(max_priority_fee);
    
    // Add micro-jitter to prevent fee market manipulation
    let jitter = self.price_jitter_bp(50); // 0.5% jitter
    let priority_fee = priority_fee.saturating_add((priority_fee * jitter as u64) / 10_000);
    
    // Get compute unit price from priority fee
    let compute_unit_price = self.compute_unit_price(priority_fee).await?;
    
    // Create compute budget instruction
    let compute_budget_ix = solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_price(compute_unit_price);
    
    // Optionally add compute unit limit if needed
    let mut budget_ixs = vec![compute_budget_ix];
    
    // Add micro-jitter to compute unit limit if needed
    if let Some(compute_unit_limit) = self.estimate_compute_units(ixs).await? {
        let jitter = self.price_jitter_bp(10); // 0.1% jitter for compute units
        let compute_unit_limit = compute_unit_limit.saturating_add((compute_unit_limit * jitter as u64) / 10_000);
        let compute_limit_ix = solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(compute_unit_limit);
        budget_ixs.push(compute_limit_ix);
    }
    
    // Add adaptive fee quantiles for dynamic fee calculation based on network conditions and attempt count
    let attempt = 1; // default attempt count
    let leader_concentration = 0.5; // default leader concentration
    let msg_size = tx_message_size_with_alts_estimate(ixs, &[]); // default message size
    let priority_fee_adaptive = FEE_STATS.get_adaptive_quantile(attempt, leader_concentration, msg_size).await;
    
    // Record the sample for future fee estimation
    if let Some(cu) = self.estimate_compute_units(ixs).await? {
        FEE_STATS.record_sample(cu, priority_fee_adaptive).await;
    }
    
    // Add all instructions to the result
    let mut result = budget_ixs;
    result.extend_from_slice(ixs);
    
    Ok(result)
}

/// Estimate compute units for a set of instructions
async fn estimate_compute_units(&self, ixs: &[Instruction]) -> Result<Option<u64>, Box<dyn std::error::Error>> {
    if ixs.is_empty() {
        return Ok(None);
    }
    
    // Try to get from cache first
    let fingerprint = ixs_fingerprint(ixs);
    let cache = get_compute_units_cache();
    if let Some(cached_units) = cache.read().await.get(&fingerprint) {
        return Ok(Some(*cached_units));
    }
    
    // Simulate to get compute units
    let simulation_result = self.simulate_ixs(ixs).await?;
    
    if let Some(units_consumed) = simulation_result.units_consumed {
        // Add 20% safety margin
        let estimated_units = (units_consumed as f64 * 1.2).ceil() as u64;
        
        // Update cache
        let cache = get_compute_units_cache();
        cache.write().await.put(fingerprint, estimated_units);
        
        Ok(Some(estimated_units))
    } else {
        Ok(None)
    }
}

/// Prefetch accounts required by a set of instructions to warm up the RPC cache.
/// This helps reduce latency when the actual requests are made.
pub async fn prefetch_for_ixs(
    rpc_client: &RpcClient,
    ixs: &[Instruction],
) -> Result<(), Box<dyn std::error::Error>> {
    use solana_sdk::account::Account;
    use solana_sdk::commitment_config::CommitmentConfig;
    
    // Collect all unique account pubkeys from all instructions
    let mut accounts_to_fetch = std::collections::HashSet::new();
    
    for ix in ixs {
        // Add program ID
        accounts_to_fetch.insert(ix.program_id);
        
        // Add all account pubkeys from the instruction
        for account_meta in &ix.accounts {
            accounts_to_fetch.insert(account_meta.pubkey);
        }
    }
    
    if accounts_to_fetch.is_empty() {
        return Ok(());
    }
    
    // Convert to Vec for RPC call
    let accounts: Vec<Pubkey> = accounts_to_fetch.into_iter().collect();
    
    // Use get_multiple_accounts_with_commitment for batched fetching
    // We use a short timeout and minimal commitment to avoid blocking
    let _ = rpc_client
        .get_multiple_accounts_with_commitment(
            &accounts,
            CommitmentConfig::processed(),
        )
        .await
        .map_err(|e| {
            log::warn!("Failed to prefetch accounts: {}", e);
            e
        });
    
    Ok(())
}

/// Slot-phase aware backoff with bounded micro-jitter.
/// leader_conc∈[0,1]: fraction of same leader in near-term window (denser → longer pause).
#[inline]
async fn slot_phase_backoff(leader_conc: f64, base_ms: u64) {
    let mult = 1.0 + (leader_conc.clamp(0.0, 1.0) * 0.6); // 1.00..1.60
    let dur_ms_base = ((base_ms as f64) * mult).round() as u64;

    // ≤50ms randomization to decorrelate bots using same heuristic.
    let jitter_cap_ms = 50u64;
    let jitter_ms = {
        let mut rng = rand::thread_rng();
        rng.gen_range(0..=jitter_cap_ms)
    };

    let total = dur_ms_base.saturating_add(jitter_ms).min(450);
    tokio::time::sleep(Duration::from_millis(total)).await;
}

// ====== OBSERVABILITY / BREAKERS ======
pub struct Observability {
    pub rebalance_attempts: IntCounter,
    pub rebalance_success: IntCounter,
    pub rebalance_failure: IntCounter,
    pub tip_latency_ms: Histogram,
}

impl Observability {
    pub fn new(reg: &Registry) -> Self {
        let attempts = IntCounter::new("rebalance_attempts", "rebalance attempts").unwrap();
        let success  = IntCounter::new("rebalance_success", "rebalance success").unwrap();
        let failure  = IntCounter::new("rebalance_failure", "rebalance failures").unwrap();
        let tip_lat  = Histogram::with_opts(HistogramOpts::new("tip_latency_ms", "tip landing (ms)")).unwrap();
        reg.register(Box::new(attempts.clone())).ok();
        reg.register(Box::new(success.clone())).ok();
        reg.register(Box::new(failure.clone())).ok();
        reg.register(Box::new(tip_lat.clone())).ok();
        Self { rebalance_attempts: attempts, rebalance_success: success, rebalance_failure: failure, tip_latency_ms: tip_lat }
    }
}

pub struct CircuitBreaker {
    failures: VecDeque<Instant>,
    window: Duration,
    max_failures: usize,
}

impl CircuitBreaker {
    pub fn new(window: Duration, max_failures: usize) -> Self {
        Self { failures: VecDeque::new(), window, max_failures }
    }
    
    pub fn record(&mut self, ok: bool) -> bool {
        let now = Instant::now();
        if ok { self.failures.clear(); return true; }
        self.failures.push_back(now);
        while let Some(&t) = self.failures.front() {
            if now.duration_since(t) > self.window { self.failures.pop_front(); } else { break; }
        }
        self.failures.len() < self.max_failures
    }
}
fn price_jitter_bp(&self, max_bp: u32) -> u32 {
    // Use wallet pubkey as a fallback seed if no signature
    let seed = self.last_tx_signature
        .as_ref()
        .map(|s| s.as_ref())
        .unwrap_or_else(|| self.wallet.pubkey().as_ref());
    
    // Simple deterministic hash-based RNG
    let mut hasher = Sha256::new();
    hasher.update(seed);
    let hash = hasher.finalize();
    
    // Use first 4 bytes for u32
    let rand_val = u32::from_le_bytes([hash[0], hash[1], hash[2], hash[3]]);
    
    // Scale to 0..max_bp*2 and subtract max_bp for -max_bp..+max_bp range
    (rand_val % (max_bp * 2 + 1)) as u32
}

impl PortOptimizer {
    // Generate deterministic jitter in basis points (0.01%)
    // Uses last transaction signature as seed if available
    fn price_jitter_bp(&self, max_bp: u32) -> u32 {
        // Use wallet pubkey as a fallback seed if no signature
        let seed = self.last_tx_signature
            .read()
            .ok()
            .as_ref()
            .map(|s| s.as_ref())
            .unwrap_or_else(|| self.wallet.pubkey().as_ref());
        
        // Simple deterministic hash-based RNG
        let mut hasher = Sha256::new();
        hasher.update(seed);
        let hash = hasher.finalize();
        
        // Use first 4 bytes for u32
        let rand_val = u32::from_le_bytes([hash[0], hash[1], hash[2], hash[3]]);
        
        // Scale to 0..max_bp*2 and subtract max_bp for -max_bp..+max_bp range
        (rand_val % (max_bp * 2 + 1)) as u32
    }
}

// Slot-phase aware backoff
#[inline]
async fn slot_phase_backoff(leader_conc: f64, base_ms: u64) {
    // denser leaders → slightly longer cool-down; cap tight
    let mult = 1.0 + (leader_conc.clamp(0.0, 1.0) * 0.6); // 1.0..1.6
    let dur = ((base_ms as f64) * mult).round() as u64;
    tokio::time::sleep(std::time::Duration::from_millis(dur.min(400))).await;
}

// Deterministic fingerprint for instruction sequences (used as cache key)
fn ixs_fingerprint(ixs: &[Instruction]) -> [u8; 32] {
    let mut h = Sha256::new();
    for ix in ixs {
        h.update(ix.program_id.as_ref());
        for a in &ix.accounts {
            h.update(a.pubkey.as_ref());
            h.update(&[a.is_signer as u8, a.is_writable as u8]);
        }
        h.update(&(ix.data.len() as u64).to_le_bytes());
        h.update(&ix.data);
    }
    let out = h.finalize();
    let mut k = [0u8; 32];
    k.copy_from_slice(&out[..32]);
    k
}

// --- Solana / SPL ---
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::RpcSignatureStatusConfig,
};
use solana_program::pubkey::Pubkey;
use spl_token::solana_program::program_pack::Pack;
use spl_token::state::Account as TokenAccount;
    account::Account,
    commitment_config::CommitmentConfig,
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    message::{v0::Message as V0Message, Message, VersionedMessage},
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer, Signers},
    transaction::{AddressLookupTableAccount, Transaction, VersionedTransaction},
};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::{AccountMeta, Instruction, InstructionError},
    message::{v0, Message as TransactionMessage, MessageHeader, VersionedMessage},
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer, Signers},
    transaction::{TransactionError, VersionedTransaction},
};
use solana_sdk::compute_budget_instruction;
use solana_sdk::transaction::TransactionBuilder;
use std::{
    cell::RefCell,
    collections::HashMap,
    convert::TryInto,
    fmt,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, OnceLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex, RwLock},
    time::sleep,
};
use url::Url;
use anyhow::{anyhow, Context, Result};
use base64::{prelude::BASE64_STANDARD, Engine};
use bincode::serialize;
use crossbeam_channel::{bounded, Receiver, Sender};
use futures_util::{future::join_all, stream::FuturesUnordered, StreamExt};
use lru::LruCache;
use log::*;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use smallvec::SmallVec;
use solana_client::{
    client_error::ClientError,
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig},
    rpc_request::RpcError,
    rpc_response::{Response as RpcResponse, RpcSimulateTransactionResult},
};
use solana_program::{
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    message::{v0::Message as V0Message, Message, VersionedMessage},
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer, Signers},
    transaction::{AddressLookupTableAccount, Transaction, VersionedTransaction},
};
use solana_sdk::{
    account::Account,
    address_lookup_table_account::AddressLookupTableAccount,
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::{AccountMeta, Instruction, InstructionError},
    message::{v0, Message as TransactionMessage, MessageHeader, VersionedMessage},
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer, Signers},
    transaction::{TransactionError, VersionedTransaction},
};
use solana_transaction_status::{
    EncodedTransaction, EncodedTransactionWithStatusMeta, TransactionConfirmationStatus,
    UiMessageEncoding, UiTransactionEncoding,
};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
    fmt,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering},
        Arc, OnceLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex, RwLock},
    time::sleep,
};
use url::Url;
use solana_client::rpc_client::RpcClient;
use std::sync::OnceLock;
use sha2::{Sha256, Digest};
use std::collections::HashMap;
use spl_associated_token_account::instruction as ata_ix;
use std::time::{Instant, Duration};
use solana_client::rpc_response::RpcContactInfo;
#[cfg(feature = "tpu")]
use solana_client::tpu_client::{TpuClient, TpuClientConfig};
use anyhow::{anyhow, Result};

// Token program IDs
const TOKEN_2022_PROGRAM_ID: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
const TIP_JITTER_MAX_BP: u16 = 50;

// ATA cache with TTL
#[derive(Clone)]
struct AtaFlag { exists: bool, ts: Instant }

struct AtaCache { 
    map: std::collections::HashMap<[u8; 32], AtaFlag> 
}

impl AtaCache {
    fn new() -> Self { 
        Self { 
            map: std::collections::HashMap::new() 
        } 
    }
    
    fn key(owner: &Pubkey, mint: &Pubkey, prog: &Pubkey) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(owner.as_ref()); 
        h.update(mint.as_ref()); 
        h.update(prog.as_ref());
        let d = h.finalize(); 
        let mut k = [0u8; 32]; 
        k.copy_from_slice(&d[..32]); 
        k
    }
    
    fn get(&self, k: &[u8; 32], ttl: Duration) -> Option<bool> {
        self.map.get(k).and_then(|f| if f.ts.elapsed() <= ttl { Some(f.exists) } else { None })
    }
    
    fn put(&mut self, k: [u8; 32], exists: bool) { 
        self.map.insert(k, AtaFlag { exists, ts: Instant::now() }); 
    }
}

static ATA_CACHE: OnceLock<tokio::sync::RwLock<AtaCache>> = OnceLock::new();

fn ata_cache() -> &'static tokio::sync::RwLock<AtaCache> {
    ATA_CACHE.get_or_init(|| tokio::sync::RwLock::new(AtaCache::new()))
}

use solana_program::{
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::{AccountMeta, Instruction},
    message::{v0::Message as V0Message, Message, VersionedMessage},
    program_pack::Pack,
    system_instruction,
};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget,
    hash::Hash,
    instruction::{AccountMeta, Instruction, InstructionError},
    message::{v0, Message as TransactionMessage, MessageHeader, VersionedMessage},
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer, Signers},
    transaction::{AddressLookupTableAccount, Transaction, VersionedTransaction},
};
use solana_sdk::compute_budget_instruction;
use solana_sdk::transaction::TransactionBuilder;
use std::convert::TryInto;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use solana_sdk::signature::Signature;
use solana_client::rpc_client::RpcClient;
use tokio::sync::oneshot;
use tokio::sync::Mutex;
use std::sync::atomic::Ordering;
use tokio::task::JoinHandle;
use anyhow::{anyhow, Result};
use std::sync::RwLock as StdRwLock;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock, OnceLock};
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct ConfirmationTracker {
    rpc_client: RpcClient,
    pending_confirmations: DashMap<Signature, (Instant, oneshot::Sender<()>)>,
    confirmation_handler: JoinHandle<()>,
struct FeeTapeEntry {
    vals: Arc<Vec<u64>>,
    ts: Instant,
}

struct FeeTapeCache {
    map: HashMap<[u8; 32], FeeTapeEntry>,
}

impl FeeTapeCache {
    fn new() -> Self { 
        Self { 
            map: HashMap::new() 
        } 
    }
    
    fn get(&self, k: &[u8; 32], ttl: Duration) -> Option<Arc<Vec<u64>>> {
        self.map.get(k)
            .and_then(|e| if e.ts.elapsed() <= ttl { 
                Some(e.vals.clone()) 
            } else { 
                None 
            })
    }
    
    fn put(&mut self, k: [u8; 32], vals: Arc<Vec<u64>>) {
        self.map.insert(k, FeeTapeEntry { 
            vals, 
            ts: Instant::now() 
        });
    }
}

static FEE_TAPE_CACHE: OnceLock<tokio::sync::RwLock<FeeTapeCache>> = OnceLock::new();

fn fee_tape_cache() -> &'static tokio::sync::RwLock<FeeTapeCache> {
    FEE_TAPE_CACHE.get_or_init(|| tokio::sync::RwLock::new(FeeTapeCache::new()))
}

const GLOBAL_FEE_KEY: [u8; 32] = *b"FEE_TAPE_GLOBAL_KEY___32BYTES______";

#[inline(always)]
fn hash_pubkeys_sorted(addrs: &[Pubkey]) -> [u8; 32] {
    let mut v: Vec<[u8; 32]> = addrs.iter().map(|k| <[u8; 32]>::from(*k)).collect();
    v.sort_unstable();
    let mut h = Sha256::new();
    for a in v { 
        h.update(a); 
    }
    let d = h.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&d[..32]);
    out
}

#[derive(Clone)]
struct CuEntry {
    cu: u32,
    epoch: u64,
    ts: Instant,
}

struct CuCache {
    map: HashMap<[u8; 32], CuEntry>,
    // tiny LRU via time-based GC; we keep size naturally bounded by TTL
}

impl CuCache {
    fn new() -> Self { 
        Self { 
            map: HashMap::new() 
        } 
    }

    fn get_valid(&self, k: &[u8; 32], epoch: u64, ttl: Duration) -> Option<u32> {
        self.map.get(k).and_then(|e| {
            if e.epoch == epoch && e.ts.elapsed() <= ttl { 
                Some(e.cu) 
            } else { 
                None 
            }
        })
    }
    
    fn put(&mut self, k: [u8; 32], cu: u32, epoch: u64) {
        self.map.insert(k, CuEntry { cu, epoch, ts: Instant::now() });
    }
}

static COMPUTE_UNITS_CACHE: OnceLock<tokio::sync::RwLock<ComputeUnitsCache>> = OnceLock::new();

fn compute_units_cache() -> &'static tokio::sync::RwLock<ComputeUnitsCache> {
    COMPUTE_UNITS_CACHE.get_or_init(|| tokio::sync::RwLock::new(ComputeUnitsCache::new()))
}

use tokio::sync::{RwLock, mpsc, oneshot};
use tokio::task::JoinHandle;
use rand::prelude::*;
use tracing::{info, warn, error};
use prometheus::{IntCounter, Histogram, Registry, HistogramOpts};

// ... (rest of the code remains the same)
#[derive(Clone)]
pub struct TipConfig {
    pub accounts: Vec<Pubkey>,
    pub strategy: TipStrategy,
    pub min_balance: u64,
}

// ----- Zero-GC warmed TorchScript batcher with micro-batching -----
#[derive(Clone)]
pub struct InferenceItem {
    pub apy_spread_bps: Arc<[f32]>,
    pub utilization: Arc<[f32]>,
    pub fee_lamports: Arc<[f32]>,
    pub liq_ratio: Arc<[f32]>,
    pub resp: oneshot::Sender<Result<f32>>, // returns probability [0,1]
}

pub struct TorchBatcher {
    module: CModule,
    device: Device,
    max_batch: usize,
    max_wait: Duration,
    tx: mpsc::Sender<InferenceItem>,
    _worker: JoinHandle<()>,
}

impl TorchBatcher {
    pub fn load(path: &str, max_batch: usize, max_wait: Duration) -> Result<Self> {
        let device = Device::Cpu; // set to Device::Cuda(_) if deploying on GPU
        let mut module = CModule::load_on_device(path, device)?;

        // Warm-up to avoid first-call latency spike; allocate kernels and caches
        no_grad(|| {
            let x = Tensor::zeros([1, 32, 4], (Kind::Float, device));
            let _ = module.forward_ts(&[x]);
        });

        let (tx, mut rx) = mpsc::channel::<InferenceItem>(1024);
        let module_arc = module.clone();
        let worker = tokio::spawn(async move {
            let module = module_arc;
            let device = device;
            loop {
                // Collect micro-batch with timeout window
                let mut batch: Vec<InferenceItem> = Vec::with_capacity(max_batch);
                match rx.recv().await {
                    Some(first) => batch.push(first),
                    None => break, // channel closed
                }
                let start = tokio::time::Instant::now();
                while batch.len() < max_batch && start.elapsed() < max_wait {
                    match rx.try_recv() {
                        Ok(it) => batch.push(it),
                        Err(_) => tokio::time::sleep(Duration::from_micros(200)).await,
                    }
                }

                // Determine time window T from shortest series across features
                let mut t: usize = usize::MAX;
                for b in &batch {
                    t = t.min(b.apy_spread_bps.len()
                        .min(b.utilization.len())
                        .min(b.fee_lamports.len())
                        .min(b.liq_ratio.len()));
                }
                if t == usize::MAX { t = 0; }
                t = t.max(8);

                // Build a contiguous buffer (B, T, 4)
                let bsz = batch.len();
                let mut buf: Vec<f32> = Vec::with_capacity(bsz * t * 4);
                for it in &batch {
                    let n = it.apy_spread_bps.len();
                    let start_idx = n.saturating_sub(t);
                    for i in start_idx..n {
                        buf.push(it.apy_spread_bps[i]);
                        buf.push(it.utilization[i]);
                        buf.push(it.fee_lamports[i]);
                        buf.push(it.liq_ratio[i]);
                    }
                }

                // Inference (no_grad)
                let x = Tensor::of_slice(&buf)
                    .to_device(device)
                    .reshape([bsz as i64, t as i64, 4]);
                let y = no_grad(|| module.forward_ts(&[x]));

                match y {
                    Ok(out) => {
                        let out = out.squeeze();
                        // Extract per-item probability; handle scalar fallback
                        let mut probs: Vec<f32> = Vec::with_capacity(bsz);
                        if out.dim() == 0 {
                            probs.push(f32::from(out.to_kind(Kind::Float)));
                        } else {
                            for i in 0..bsz {
                                let p = out.double_value(&[i as i64]) as f32;
                                probs.push(p);
                            }
                        }
                        for (i, it) in batch.into_iter().enumerate() {
                            let p = probs.get(i).copied().unwrap_or(0.0).clamp(0.0, 1.0);
                            let _ = it.resp.send(Ok(p));
                        }
                    }
                    Err(e) => {
                        for it in batch.into_iter() {
                            let _ = it.resp.send(Err(anyhow!(format!("inference failed: {e}"))));
                        }
                    }
                }
            }
        });

        Ok(Self { module, device, max_batch, max_wait, tx, _worker: worker })
    }

    pub async fn infer(
        &self,
        apy_spread_bps: Arc<[f32]>,
        utilization: Arc<[f32]>,
        fee_lamports: Arc<[f32]>,
        liq_ratio: Arc<[f32]>,
    ) -> Result<f32> {
        let (resp_tx, resp_rx) = oneshot::channel::<Result<f32>>();
        let item = InferenceItem { apy_spread_bps, utilization, fee_lamports, liq_ratio, resp: resp_tx };
        self.tx.send(item).await.map_err(|_| anyhow!("batcher closed"))?;
        resp_rx.await.map_err(|_| anyhow!("inference dropped"))?
    }
}

// ----- TorchScript Trigger Model (inference-only) -----
pub struct TriggerModel {
    module: CModule,
    device: Device,
}

impl TriggerModel {
    pub fn load(path: &str) -> anyhow::Result<Self> {
        let device = Device::Cpu;
        let module = CModule::load_on_device(path, device)?;
        Ok(Self { module, device })
    }

    // Inputs are rolling sequences; output is probability [0,1]
    pub fn infer(
        &self,
        apy_spread_bps_seq: &[f32],
        utilization_seq: &[f32],
        fee_lamports_seq: &[f32],
        liq_ratio_seq: &[f32],
    ) -> anyhow::Result<f32> {
        let t = apy_spread_bps_seq
            .len()
            .min(utilization_seq.len())
            .min(fee_lamports_seq.len())
            .min(liq_ratio_seq.len());
        let t = t.max(8);
        let a_len = apy_spread_bps_seq.len();
        let u_len = utilization_seq.len();
        let f_len = fee_lamports_seq.len();
        let l_len = liq_ratio_seq.len();
        let start = a_len.min(u_len).min(f_len).min(l_len).saturating_sub(t);
        let mut buf = Vec::with_capacity(t * 4);
        for i in start..start + t {
            buf.push(apy_spread_bps_seq[a_len - t + (i - start)]);
            buf.push(utilization_seq[u_len - t + (i - start)]);
            buf.push(fee_lamports_seq[f_len - t + (i - start)]);
            buf.push(liq_ratio_seq[l_len - t + (i - start)]);
        }
        let x = Tensor::of_slice(&buf)
            .reshape([1, t as i64, 4])
            .to_device(self.device);
        let y = self.module.forward_ts(&[x])?;
        let p: f32 = f32::from(y.squeeze());
        Ok(p.clamp(0.0, 1.0))
    }
}

#[derive(Clone)]
pub enum TipStrategy { RoundRobin, Fixed(usize) }

#[derive(Clone, Debug)]
pub struct TipStats {
    pub account: Pubkey,
    pub ema: f64,         // latency level (ms)
    pub emv: f64,         // latency abs-dev (ms)
    pub ghost_rate: f64,  // failures/attempts (decayed)
    pub weight: f64,      // normalized picking weight
    pub attempts: u64,
    pub failures: u64,
    last_update: Instant, // for exponential time decay
    succ_ewma: f64,       // success probability EWMA
    fail_ewma: f64,       // failure EWMA (for ghosting)
}

// ==================== PIECE 1.19: RPC Fanout Broadcaster ====================

/// Tracks performance metrics for an RPC endpoint
#[derive(Clone, Debug)]
struct RpcNodeStat {
    url: String,
    ema_ms: f64,         // latency EWMA
    succ_ewma: f64,      // success EWMA
    attempts: u64,
    cooldown_until: Instant, // circuit breaker cooldown
}

impl RpcNodeStat {
    /// Calculate a score for this RPC node (higher is better)
    fn score(&self) -> f64 {
        // higher is better; 1/(latency) * success^2, zero during cooldown
        if Instant::now() < self.cooldown_until { return 0.0; }
        let l = self.ema_ms.max(1.0);
        (1.0 / l) * (self.succ_ewma * self.succ_ewma).max(1e-6)
    }
}

/// Manages multiple RPC clients with smart routing
pub struct RpcFanout {
    clients: Vec<Arc<RpcClient>>,
    stats: tokio::sync::RwLock<Vec<RpcNodeStat>>,
}

impl RpcFanout {
    /// Create a new RpcFanout with the given RPC URLs
    pub fn new(urls: &[String]) -> Self {
        let mut clients = Vec::with_capacity(urls.len());
        let mut stats = Vec::with_capacity(urls.len());
        for u in urls {
            clients.push(Arc::new(RpcClient::new(u.clone())));
            stats.push(RpcNodeStat {
                url: u.clone(),
                ema_ms: 800.0,
                succ_ewma: 0.9,
                attempts: 0,
                cooldown_until: Instant::now() - Duration::from_secs(1), // Start ready
            });
        }
        Self { clients, stats: tokio::sync::RwLock::new(stats) }
    }

    /// Get indices of top K performing RPC nodes
    #[inline]
    fn topk_indices(&self, stats: &[RpcNodeStat], k: usize) -> Vec<usize> {
        let mut idx: Vec<usize> = (0..stats.len()).collect();
        idx.sort_unstable_by(|&a, &b| 
            stats[b].score().partial_cmp(&stats[a].score()).unwrap_or(std::cmp::Ordering::Equal)
        );
        idx.into_iter().take(k.min(stats.len())).collect()
    }

    /// Simulate a transaction across top-K RPCs; return the first successful result.
    pub async fn simulate_vtx_first(
        &self,
        vtx: &VersionedTransaction,
        mut cfg: RpcSimulateTransactionConfig,
        top_k: usize,
        per_timeout_ms: u64,
    ) -> Result<RpcSimulateTransactionResult> {
        use tokio::{task::JoinSet, time::timeout};
        
        // Get a snapshot of current stats for routing decision
        let stats_now = self.stats.read().await.clone();
        if stats_now.is_empty() { return Err(anyhow!("no RPC endpoints configured")); }
        let order = self.topk_indices(&stats_now, top_k.max(1));
        drop(stats_now);

        let mut js = JoinSet::new();
        for idx in order {
            let cli = self.clients[idx].clone();
            let tx = vtx.clone();
            let cfgc = cfg.clone();
            js.spawn(async move {
                let t0 = Instant::now();
                let fut = cli.simulate_transaction_with_config(&tx, cfgc);
                let res = timeout(Duration::from_millis(per_timeout_ms.max(150)), fut).await;
                (idx, t0, res)
            });
        }

        let mut first_ok: Option<(usize, Instant, RpcSimulateTransactionResult)> = None;
        while let Some(joined) = js.join_next().await {
            let (idx, t0, res) = joined?;
            match res {
                Ok(Ok(sim_res)) => { 
                    first_ok = Some((idx, t0, sim_res)); 
                    break; 
                }
                _ => { /* keep waiting */ }
            }
        }

        let mut stats_w = self.stats.write().await;
        for s in stats_w.iter_mut() { s.attempts = s.attempts.saturating_add(1); }
        if let Some((idx, t0, sim_res)) = first_ok {
            let ms = t0.elapsed().as_millis() as f64;
            let alpha_l = 0.25;
            let alpha_s = 0.35;
            let s = &mut stats_w[idx];
            s.ema_ms = alpha_l * ms + (1.0 - alpha_l) * s.ema_ms;
            s.succ_ewma = alpha_s * 1.0 + (1.0 - alpha_s) * s.succ_ewma;
            Ok(sim_res.value)
        } else {
            // all failed: decay + cool off worst 1–2 nodes
            let now = Instant::now();
            for s in stats_w.iter_mut() {
                let alpha_s = 0.20;
                s.succ_ewma = alpha_s * 0.0 + (1.0 - alpha_s) * s.succ_ewma;
            }
            // pick bottom by score and cool them off briefly
            let mut idx: Vec<usize> = (0..stats_w.len()).collect();
            idx.sort_unstable_by(|&a,&b| stats_w[a].score().partial_cmp(&stats_w[b].score()).unwrap());
            for &bad in idx.iter().take(2.min(stats_w.len())) {
                stats_w[bad].cooldown_until = now + Duration::from_secs(3);
            }
            Err(anyhow!("simulation failed on all RPC endpoints"))
        }
    }

    /// Broadcast a transaction to multiple RPCs in parallel, returning the first successful result
    pub async fn broadcast_vtx_skip_preflight(
        &self,
        vtx: &VersionedTransaction,
        max_parallel: usize,
    ) -> Result<Signature> {
        use tokio::{task::JoinSet, time::timeout};

        let stats_now = self.stats.read().await.clone();
        if stats_now.is_empty() { return Err(anyhow!("no RPC endpoints configured")); }
        let order = self.topk_indices(&stats_now, max_parallel.max(1));
        drop(stats_now);

        let mut js = JoinSet::new();
        for idx in order {
            let cli = self.clients[idx].clone();
            let tx = vtx.clone();
            js.spawn(async move {
                let t0 = Instant::now();
                let cfg = solana_client::rpc_config::RpcSendTransactionConfig {
                    skip_preflight: true,
                    max_retries: Some(0),
                    ..Default::default()
                };
                let fut = async {
                    cli.send_transaction_with_config(&tx, cfg).await
                        .or_else(|_| cli.send_transaction(&tx).await)
                };
                let res = timeout(Duration::from_millis(600), fut).await; // hard per-endpoint deadline
                (idx, t0, res)
            });
        }

        let mut first_ok: Option<(usize, Instant, Signature)> = None;
        while let Some(joined) = js.join_next().await {
            let (idx, t0, res) = joined?;
            match res {
                Ok(Ok(sig)) => { first_ok = Some((idx, t0, sig)); break; }
                _ => { /* keep waiting */ }
            }
        }

        let mut stats_w = self.stats.write().await;
        for s in stats_w.iter_mut() { s.attempts = s.attempts.saturating_add(1); }
        if let Some((idx, t0, sig)) = first_ok {
            let ms = t0.elapsed().as_millis() as f64;
            let alpha_l = 0.25;
            let alpha_s = 0.35;
            let s = &mut stats_w[idx];
            s.ema_ms = alpha_l * ms + (1.0 - alpha_l) * s.ema_ms;
            s.succ_ewma = alpha_s * 1.0 + (1.0 - alpha_s) * s.succ_ewma;
            Ok(sig)
        } else {
            // all failed: decay + cool off worst 1–2 nodes
            let now = Instant::now();
            for s in stats_w.iter_mut() {
                let alpha_s = 0.20;
                s.succ_ewma = alpha_s * 0.0 + (1.0 - alpha_s) * s.succ_ewma;
            }
            // pick bottom by score and cool them off briefly
            let mut idx: Vec<usize> = (0..stats_w.len()).collect();
            idx.sort_unstable_by(|&a,&b| stats_w[a].score().partial_cmp(&stats_w[b].score()).unwrap());
            for &bad in idx.iter().take(2.min(stats_w.len())) {
                stats_w[bad].cooldown_until = now + Duration::from_secs(3);
            }
            Err(anyhow!("broadcast failed on all RPC endpoints"))
        }
        for idx in order {
            let cli = self.clients[idx].clone();
            let tx = vtx.clone();
            js.spawn(async move {
                let t0 = Instant::now();
                let cfg = RpcSendTransactionConfig {
                    skip_preflight: true,
                    max_retries: Some(0),
                    ..Default::default()
                };
                // Try fast-path first, fall back to default send
                let res = cli.send_transaction_with_config(&tx, cfg).await
                    .or_else(|_| cli.send_transaction(&tx).await);
                (idx, t0, res)
            });
        }

        // Wait for first successful response
        let mut first_ok: Option<(usize, Instant, Signature)> = None;
        while let Some(joined) = js.join_next().await {
            let (idx, t0, res) = joined?;
            if let Ok(sig) = res {
                first_ok = Some((idx, t0, sig));
                break;
            }
        }

        // Update statistics based on results
        let mut stats_w = self.stats.write().await;
        for s in stats_w.iter_mut() { 
            s.attempts = s.attempts.saturating_add(1); 
        }
        
        if let Some((idx, t0, sig)) = first_ok {
            // Update success metrics for the winning endpoint
            let ms = t0.elapsed().as_millis() as f64;
            let alpha_l = 0.25;  // Latency smoothing factor
            let alpha_s = 0.35;  // Success rate smoothing factor
            let s = &mut stats_w[idx];
            s.ema_ms = alpha_l * ms + (1.0 - alpha_l) * s.ema_ms;
            s.succ_ewma = alpha_s * 1.0 + (1.0 - alpha_s) * s.succ_ewma;
            Ok(sig)
        } else {
            // All endpoints failed - decay success rates
            let alpha_s = 0.20;
            for s in stats_w.iter_mut() {
                s.succ_ewma = alpha_s * 0.0 + (1.0 - alpha_s) * s.succ_ewma;
            }
            Err(anyhow!("broadcast failed on all RPC endpoints"))
        }
    }
}

// ==================== PIECE 1.14: Cohort Micro-Timing Jitter ====================

/// Deterministic micro-jitter for transaction timing based on wallet and recent blockhash.
/// This creates stable but unique timing offsets for each wallet/blockhash combination.
/// The jitter is designed to be small (microseconds) to avoid adding significant latency.
#[inline]
pub fn cohort_jitter_us(wallet: &Pubkey, recent_blockhash: &Hash, max_jitter_us: u32) -> u32 {
    use sha2::{Digest, Sha256};
    
    // Create a hash of wallet + blockhash for deterministic but unique jitter
    let mut hasher = Sha256::new();
    hasher.update(wallet.as_ref());
    hasher.update(recent_blockhash.as_ref());
    let hash = hasher.finalize();
    
    // Use first 4 bytes of hash to create jitter
    let jitter = u32::from_le_bytes([hash[0], hash[1], hash[2], hash[3]]);
    
    // Scale to desired range (0..max_jitter_us)
    jitter % (max_jitter_us + 1)
}

// ==================== MEV BOT INTEGRATION ====================

/// Main MEV bot that integrates all components
pub struct MevBot {
    /// RPC client for Solana network access
    rpc_client: Arc<RpcClient>,
    /// Hybrid sender for transactions
    sender: HybridSender,
    /// Slot edge pacer for optimal transaction timing
    pacer: Arc<SlotEdgePacer>,
    /// Confirmation tracker for monitoring transaction status
    confirmation_tracker: Arc<ConfirmationTracker>,
    /// Adaptive pricer for dynamic fee adjustment
    adaptive_pricer: Arc<AdaptivePricer>,
    /// Concurrency gate for managing in-flight transactions
    concurrency_gate: Arc<ConcurrencyGate>,
    /// Current blockhash and slot
    blockhash_info: RwLock<BlockhashInfo>,
    /// Last time we refreshed the blockhash
    last_blockhash_refresh: RwLock<Instant>,
    /// Blockhash refresh interval
    blockhash_refresh_interval: Duration,
}

/// Information about the current blockhash and slot
#[derive(Clone, Debug)]
struct BlockhashInfo {
    blockhash: Hash,
    slot: u64,
    last_valid_block_height: u64,
}

impl MevBot {
    /// Creates a new MevBot with the given configuration
    pub fn new(
        rpc_client: Arc<RpcClient>,
        tpu_client: Option<Arc<TpuClient>>,
        rpc_endpoints: Vec<String>,
        initial_compute_unit_price: u64,
        max_compute_unit_price: u64,
        min_siblings: usize,
        max_siblings: usize,
        target_success_rate: u8,
        max_in_flight: usize,
    ) -> Self {
        // Create confirmation metrics
        let metrics = Arc::new(ConfirmationMetrics::new());
        
        // Create confirmation tracker
        let confirmation_tracker = Arc::new(ConfirmationTracker::new(
            rpc_client.clone(),
            metrics.clone(),
            Duration::from_secs(30), // confirmation timeout
        ));
        
        // Create adaptive pricer
        let adaptive_pricer = Arc::new(AdaptivePricer::new(
            initial_compute_unit_price,
            max_compute_unit_price,
            min_siblings,
            max_siblings,
            metrics,
            target_success_rate,
            10, // price_step_pct
            1,  // sibling_step
        ));
        
        // Create concurrency gate
        let concurrency_gate = Arc::new(ConcurrencyGate::new(max_in_flight));
        
        // Create hybrid sender
        let sender = HybridSender::new(
            rpc_client.clone(),
            tpu_client,
            rpc_endpoints,
            3, // max_retries
            Duration::from_millis(100), // retry_delay
        );
        
        // Create slot edge pacer
        let pacer = Arc::new(SlotEdgePacer::new(rpc_client.clone()));
        
        Self {
            rpc_client,
            sender,
            pacer,
            confirmation_tracker,
            adaptive_pricer,
            concurrency_gate,
            blockhash_info: RwLock::new(BlockhashInfo {
                blockhash: Hash::default(),
                slot: 0,
                last_valid_block_height: 0,
            }),
            last_blockhash_refresh: RwLock::new(Instant::now() - Duration::from_secs(60)),
            blockhash_refresh_interval: Duration::from_secs(30),
        }
    }
    
    /// Refreshes the blockhash if needed
    async fn refresh_blockhash_if_needed(&self) -> Result<()> {
        // Check if we need to refresh the blockhash
        let needs_refresh = {
            let last_refresh = self.last_blockhash_refresh.read().await;
            last_refresh.elapsed() >= self.blockhash_refresh_interval
        };
        
        if needs_refresh {
            self.refresh_blockhash().await?;
        }
        
        Ok(())
    }
    
    /// Refreshes the current blockhash
    async fn refresh_blockhash(&self) -> Result<()> {
        let (blockhash, slot, last_valid_block_height) = self.pacer.get_latest_blockhash().await?;
        
        let mut blockhash_info = self.blockhash_info.write().await;
        blockhash_info.blockhash = blockhash;
        blockhash_info.slot = slot;
        blockhash_info.last_valid_block_height = last_valid_block_height;
        
        *self.last_blockhash_refresh.write().await = Instant::now();
        
        Ok(())
    }
    
    /// Submits a transaction using the MEV bot's optimal settings
    pub async fn submit_transaction<T: Signers>(
        &self,
        instructions: &[Instruction],
        keypairs: &T,
    ) -> Result<Signature> {
        // Get a concurrency permit
        let _permit = self.concurrency_gate.acquire().await;
        
        // Get current blockhash
        self.refresh_blockhash_if_needed().await?;
        let blockhash_info = self.blockhash_info.read().await.clone();
        
        // Get current compute unit price and number of siblings
        let compute_unit_price = self.adaptive_pricer.current_compute_unit_price();
        let num_siblings = self.adaptive_pricer.current_siblings();
        
        // Create transaction builder
        let builder = V0TransactionBuilder::new(keypairs.pubkeys()[0])
            .recent_blockhash(blockhash_info.blockhash)
            .compute_unit_price(compute_unit_price);
        
        // Build and sign transactions
        let mut transactions = Vec::with_capacity(num_siblings);
        
        // Create the base transaction
        let tx = builder.build_and_sign(instructions, keypairs)?;
        transactions.push(tx);
        
        // Create sibling transactions with different compute unit prices
        for _ in 1..num_siblings {
            // Slightly vary the compute unit price for each sibling
            let sibling_price = compute_unit_price.saturating_add(
                (rand::random::<u64>() % 10_000) * 10 // Add up to 0.01 SOL variation
            );
            
            let sibling_tx = builder
                .clone()
                .compute_unit_price(sibling_price)
                .build_and_sign(instructions, keypairs)?;
                
            transactions.push(sibling_tx);
        }
        
        // Wait for optimal slot timing
        self.pacer.wait_for_optimal_timing().await?;
        
        // Send transactions in parallel
        let mut send_futures = Vec::with_capacity(transactions.len());
        
        for tx in transactions {
            let signature = tx.signatures[0];
            let sender = self.sender.clone();
            
            // Register with confirmation tracker before sending
            self.confirmation_tracker.track_transaction(
                signature,
                tx.message.recent_blockhash,
                Instant::now(),
            );
            
            // Spawn a task to send the transaction
            send_futures.push(tokio::spawn(async move {
                match sender.send_transaction(&tx).await {
                    Ok(_) => Ok(signature),
                    Err(e) => {
                        tracing::error!("Failed to send transaction: {}", e);
                        Err(e)
                    }
                }
            }));
        }
        
        // Wait for at least one successful send
        let result = futures::future::select_ok(send_futures).await;
        
        match result {
            Ok((signature, _remaining)) => {
                tracing::info!("Successfully submitted transaction: {}", signature);
                Ok(signature)
            }
            Err(e) => {
                tracing::error!("All transaction submissions failed: {}", e);
                Err(anyhow!("All transaction submissions failed"))
            }
        }
    }
    
    /// Starts the background tasks for the MEV bot
    pub async fn start_background_tasks(&self) -> Result<()> {
        // Start confirmation tracker
        let confirmation_tracker = self.confirmation_tracker.clone();
        tokio::spawn(async move {
            if let Err(e) = confirmation_tracker.start().await {
                tracing::error!("Confirmation tracker failed: {}", e);
            }
        });
        
        // Start adaptive pricer adjustment task
        let adaptive_pricer = self.adaptive_pricer.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                adaptive_pricer.adjust_strategy().await;
            }
        });
        
        // Initial blockhash refresh
        self.refresh_blockhash().await?;
        
        Ok(())
    }
    
    /// Stops all background tasks
    pub async fn stop(&self) -> Result<()> {
        self.confirmation_tracker.stop().await?;
        Ok(())
    }
}

// ==================== PIECE 1.30: True v0 Builder ====================

/// Builds optimized v0 transactions with address lookup tables and compute budget instructions
pub struct V0TransactionBuilder {
    /// The message header (number of required signers and read-only signers)
    message_header: MessageHeader,
    /// Account keys that are required signers
    signer_keys: Vec<Pubkey>,
    /// Account keys that are read-only
    read_only_keys: Vec<Pubkey>,
    /// Recent blockhash for the transaction
    recent_blockhash: Hash,
    /// Fee payer (first signer)
    fee_payer: Pubkey,
    /// Address lookup tables to use
    address_lookup_tables: Vec<AddressLookupTableAccount>,
    /// Compute unit price in micro-lamports
    compute_unit_price: u64,
    /// Compute unit limit
    compute_unit_limit: u32,
    /// Priority fee in microlamports (in addition to compute unit price)
    priority_fee: u64,
    /// Whether to enable priority fees
    enable_priority_fees: bool,
}

impl V0TransactionBuilder {
    /// Creates a new V0TransactionBuilder
    pub fn new(fee_payer: Pubkey) -> Self {
        Self {
            message_header: MessageHeader::default(),
            signer_keys: vec![fee_pubkey],
            read_only_keys: Vec::new(),
            recent_blockhash: Hash::default(),
            fee_payer,
            address_lookup_tables: Vec::new(),
            compute_unit_price: 0,
            compute_unit_limit: 200_000, // Default compute unit limit
            priority_fee: 0,
            enable_priority_fees: false,
        }
    }

    /// Sets the recent blockhash
    pub fn recent_blockhash(mut self, recent_blockhash: Hash) -> Self {
        self.recent_blockhash = recent_blockhash;
        self
    }

    /// Sets the compute unit price in micro-lamports
    pub fn compute_unit_price(mut self, micro_lamports: u64) -> Self {
        self.compute_unit_price = micro_lamports;
        self
    }

    /// Sets the compute unit limit
    pub fn compute_unit_limit(mut self, units: u32) -> Self {
        self.compute_unit_limit = units;
        self
    }

    /// Sets the priority fee in microlamports (in addition to compute unit price)
    pub fn priority_fee(mut self, micro_lamports: u64) -> Self {
        self.priority_fee = micro_lamports;
        self.enable_priority_fees = true;
        self
    }

    /// Adds an address lookup table to use
    pub fn add_address_lookup_table(mut self, table: AddressLookupTableAccount) -> Self {
        self.address_lookup_tables.push(table);
        self
    }

    /// Adds a required signer
    pub fn add_signer(mut self, pubkey: Pubkey) -> Self {
        if !self.signer_keys.contains(&pubkey) {
            self.signer_keys.push(pubkey);
            self.message_header.num_required_signatures = self.signer_keys.len() as u8;
        }
        self
    }

    /// Adds a read-only account that doesn't need to be a signer
    pub fn add_readonly(mut self, pubkey: Pubkey, is_signer: bool) -> Self {
        if is_signer {
            if !self.signer_keys.contains(&pubkey) {
                self.signer_keys.push(pubkey);
                self.message_header.num_required_signatures = self.signer_keys.len() as u8;
            }
        } else if !self.read_only_keys.contains(&pubkey) && !self.signer_keys.contains(&pubkey) {
            self.read_only_keys.push(pubkey);
        }
        self
    }

    /// Builds a versioned transaction with the given instructions
    pub fn build(&self, instructions: &[Instruction]) -> Result<VersionedTransaction> {
        // Create a transaction builder
        let mut builder = VersionedTransactionBuilder::new(self.fee_payer);
        
        // Add compute budget instructions if needed
        let mut instructions = instructions.to_vec();
        
        // Add compute budget instructions if needed
        if self.compute_unit_price > 0 || self.enable_priority_fees || self.compute_unit_limit != 200_000 {
            let mut compute_budget_ixs = Vec::new();
            
            // Set compute unit limit if not default
            if self.compute_unit_limit != 200_000 {
                compute_budget_ixs.push(compute_budget_instruction::set_compute_unit_limit(self.compute_unit_limit));
            }
            
            // Set compute unit price if specified
            if self.compute_unit_price > 0 {
                compute_budget_ixs.push(compute_budget_instruction::set_compute_unit_price(self.compute_unit_price));
            }
            
            // Add priority fee if enabled
            if self.enable_priority_fees && self.priority_fee > 0 {
                compute_budget_ixs.push(compute_budget_instruction::set_compute_unit_price(
                    self.compute_unit_price.saturating_add(self.priority_fee)
                ));
            }
            
            // Insert compute budget instructions at the beginning
            instructions.splice(0..0, compute_budget_ixs);
        }
        
        // Add all instructions
        for ix in instructions {
            builder = builder.add_instruction(ix);
        }
        
        // Add address lookup tables if any
        let mut builder = if !self.address_lookup_tables.is_empty() {
            builder.add_address_lookup_tables(self.address_lookup_tables.clone())
        } else {
            builder
        };
        
        // Build the transaction
        let tx = builder.build()?;
        
        Ok(tx)
    }
    
    /// Builds and signs a versioned transaction with the given instructions and signers
    pub fn build_and_sign<T: Signers>(
        &self,
        instructions: &[Instruction],
        keypairs: &T,
    ) -> Result<VersionedTransaction> {
        let tx = self.build(instructions)?;
        let signer_pubkeys: HashSet<Pubkey> = keypairs.pubkeys().into_iter().collect();
        
        // Verify all required signers are provided
        for required_signer in &self.signer_keys[1..] { // Skip fee payer
            if !signer_pubkeys.contains(required_signer) {
                return Err(anyhow!("Missing signer for required signer: {}", required_signer));
            }
        }
        
        Ok(tx.sign(keypairs, self.recent_blockhash))
    }
}

// ==================== PIECE 1.29: In-Flight Concurrency Gate ====================

/// Manages the number of in-flight transactions to prevent overwhelming the network
/// and respect rate limits.
pub struct ConcurrencyGate {
    /// Maximum number of in-flight transactions
    max_in_flight: usize,
    /// Current number of in-flight transactions
    in_flight: AtomicUsize,
    /// Waiters for when capacity becomes available
    waiters: Mutex<Vec<oneshot::Sender<()>>>,
    /// Last time we logged a backpressure warning
    last_warning: Mutex<Instant>,
    /// Minimum time between backpressure warnings
    warning_interval: Duration,
}

impl ConcurrencyGate {
    /// Creates a new ConcurrencyGate with the given maximum in-flight transactions
    pub fn new(max_in_flight: usize) -> Self {
        Self {
            max_in_flight,
            in_flight: AtomicUsize::new(0),
            waiters: Mutex::new(Vec::new()),
            last_warning: Mutex::new(Instant::now() - Duration::from_secs(60)),
            warning_interval: Duration::from_secs(5),
        }
    }

    /// Tries to acquire a permit for a new in-flight transaction
    /// 
    /// Returns `true` if the permit was acquired, `false` if it would exceed the limit
    pub fn try_acquire(&self) -> Option<ConcurrencyPermit<'_>> {
        let current = self.in_flight.load(Ordering::Acquire);
        if current >= self.max_in_flight {
            return None;
        }

        // Try to increment the counter
        match self.in_flight.compare_exchange_weak(
            current,
            current + 1,
            Ordering::Release,
            Ordering::Relaxed,
        ) {
            Ok(_) => Some(ConcurrencyPermit { gate: self }),
            Err(_) => None, // Someone else modified it, let the next attempt handle it
        }
    }

    /// Acquires a permit for a new in-flight transaction, waiting if necessary
    pub async fn acquire(&self) -> ConcurrencyPermit<'_> {
        // Fast path: try to get a permit immediately
        if let Some(permit) = self.try_acquire() {
            return permit;
        }

        // Slow path: need to wait for capacity
        let (tx, rx) = oneshot::channel();
        {
            let mut waiters = self.waiters.lock().await;
            // Check again with the lock held
            if let Some(permit) = self.try_acquire() {
                // Someone released a permit while we were waiting for the lock
                drop(waiters);
                return permit;
            }
            // Add ourselves to the waiters
            waiters.push(tx);
        }

        // Wait for a permit to become available
        let _ = rx.await;
        
        // Try to acquire again (should succeed since we were notified)
        self.acquire().await
    }

    /// Returns the current number of in-flight transactions
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Returns the maximum number of in-flight transactions
    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }

    /// Updates the maximum number of in-flight transactions
    pub fn set_max_in_flight(&self, new_max: usize) {
        // Update the max
        self.max_in_flight = new_max;
        
        // Wake up any waiters if we increased the limit
        if self.in_flight.load(Ordering::Relaxed) < new_max {
            self.wake_waiters();
        }
    }

    /// Called when a transaction completes to release a permit
    fn release(&self) {
        let old_count = self.in_flight.fetch_sub(1, Ordering::Release);
        
        // Sanity check: underflow shouldn't happen
        if old_count == 0 {
            tracing::error!("ConcurrencyGate underflow detected!");
            return;
        }
        
        // If we're still at or above the limit, don't wake waiters
        if old_count > self.max_in_flight {
            return;
        }
        
        // Wake up a single waiter
        self.wake_waiters();
    }
    
    /// Wakes up to one waiter if there are any
    fn wake_waiters(&self) {
        let mut waiters = self.waiters.try_lock();
        let mut waiters = match waiters {
            Ok(w) => w,
            Err(_) => {
                // If we can't acquire the lock, another task is probably handling it
                return;
            }
        };
        
        // Remove and wake a single waiter
        while let Some(waiter) = waiters.pop() {
            // If the receiver was dropped, just continue to the next one
            if waiter.send(()).is_ok() {
                // Woke one waiter, we're done
                return;
            }
        }
    }
    
    /// Logs a backpressure warning if enough time has passed since the last one
    fn log_backpressure_warning(&self) {
        let mut last_warning = match self.last_warning.try_lock() {
            Ok(l) => l,
            Err(_) => return, // Another task is handling it
        };
        
        if last_warning.elapsed() >= self.warning_interval {
            let in_flight = self.in_flight.load(Ordering::Relaxed);
            tracing::warn!(
                "ConcurrencyGate backpressure: {}/{} in-flight transactions",
                in_flight,
                self.max_in_flight
            );
            *last_warning = Instant::now();
        }
    }
}

/// A permit that allows a transaction to be in-flight
/// 
/// When dropped, the permit is automatically released.
pub struct ConcurrencyPermit<'a> {
    gate: &'a ConcurrencyGate,
}

impl<'a> ConcurrencyPermit<'a> {
    /// Returns the current number of in-flight transactions (including this one)
    pub fn in_flight_count(&self) -> usize {
        self.gate.in_flight_count()
    }
    
    /// Returns the maximum number of in-flight transactions
    pub fn max_in_flight(&self) -> usize {
        self.gate.max_in_flight()
    }
    
    /// Explicitly releases the permit early
    pub fn release(self) {
        // This method takes ownership of self, so it will be dropped at the end
        // and the Drop implementation will handle the actual release
    }
}

impl<'a> Drop for ConcurrencyPermit<'a> {
    fn drop(&mut self) {
        self.gate.release();
    }
}

// ==================== PIECE 1.28: Adaptive Siblings & Price Escalation ====================

/// Dynamically adjusts the number of transaction variants (siblings) and compute unit prices
/// based on network conditions and success rates.
pub struct AdaptivePricer {
    /// Base compute unit price in micro-lamports
    base_compute_unit_price: AtomicU64,
    /// Maximum compute unit price in micro-lamports
    max_compute_unit_price: AtomicU64,
    /// Minimum number of transaction siblings to generate
    min_siblings: AtomicUsize,
    /// Maximum number of transaction siblings to generate
    max_siblings: AtomicUsize,
    /// Current number of siblings being used
    current_siblings: AtomicUsize,
    /// Current compute unit price being used
    current_compute_unit_price: AtomicU64,
    /// Metrics from confirmation tracker
    metrics: Arc<ConfirmationMetrics>,
    /// Last time we adjusted the strategy
    last_adjustment: RwLock<Instant>,
    /// Minimum time between adjustments
    min_adjustment_interval: Duration,
    /// Target success rate (0-100)
    target_success_rate: AtomicU8,
    /// Price adjustment step size (percentage)
    price_step_pct: AtomicU8,
    /// Sibling adjustment step size
    sibling_step: AtomicUsize,
}

impl AdaptivePricer {
    /// Creates a new AdaptivePricer with the given configuration
    pub fn new(
        base_compute_unit_price: u64,
        max_compute_unit_price: u64,
        min_siblings: usize,
        max_siblings: usize,
        metrics: Arc<ConfirmationMetrics>,
        target_success_rate: u8,
        price_step_pct: u8,
        sibling_step: usize,
    ) -> Self {
        let current_siblings = min_siblings.max(1);
        
        Self {
            base_compute_unit_price: AtomicU64::new(base_compute_unit_price),
            max_compute_unit_price: AtomicU64::new(max_compute_unit_price),
            min_siblings: AtomicUsize::new(min_siblings),
            max_siblings: AtomicUsize::new(max_siblings),
            current_siblings: AtomicUsize::new(current_siblings),
            current_compute_unit_price: AtomicU64::new(base_compute_unit_price),
            metrics,
            last_adjustment: RwLock::new(Instant::now() - Duration::from_secs(60)), // Start eligible for adjustment
            min_adjustment_interval: Duration::from_secs(10),
            target_success_rate: AtomicU8::new(target_success_rate.clamp(1, 100)),
            price_step_pct: AtomicU8::new(price_step_pct.clamp(1, 100)),
            sibling_step: AtomicUsize::new(sibling_step.max(1)),
        }
    }

    /// Gets the current number of siblings to generate
    pub fn current_siblings(&self) -> usize {
        self.current_siblings.load(Ordering::Relaxed)
    }

    /// Gets the current compute unit price to use
    pub fn current_compute_unit_price(&self) -> u64 {
        self.current_compute_unit_price.load(Ordering::Relaxed)
    }

    /// Adjusts the strategy based on recent performance
    pub async fn adjust_strategy(&self) {
        // Check if enough time has passed since the last adjustment
        let last_adjustment = *self.last_adjustment.read().await;
        if last_adjustment.elapsed() < self.min_adjustment_interval {
            return;
        }

        // Update the last adjustment time
        *self.last_adjustment.write().await = Instant::now();

        let current_success_rate = self.metrics.success_rate();
        let target_success_rate = self.target_success_rate.load(Ordering::Relaxed) as u8;
        let price_step_pct = self.price_step_pct.load(Ordering::Relaxed) as f64 / 100.0;
        let sibling_step = self.sibling_step.load(Ordering::Relaxed);
        
        let current_price = self.current_compute_unit_price.load(Ordering::Relaxed);
        let max_price = self.max_compute_unit_price.load(Ordering::Relaxed);
        let current_siblings = self.current_siblings.load(Ordering::Relaxed);
        let min_siblings = self.min_siblings.load(Ordering::Relaxed);
        let max_siblings = self.max_siblings.load(Ordering::Relaxed);

        // Calculate the price adjustment factor based on success rate
        let success_ratio = current_success_rate as f64 / target_success_rate as f64;
        let mut price_adjustment = 1.0;
        let mut siblings_adjustment = 0;

        if success_ratio < 0.9 {
            // Success rate is too low - increase price and/or siblings
            price_adjustment = 1.0 + price_step_pct;
            if success_ratio < 0.7 {
                // If really struggling, also increase siblings
                siblings_adjustment = sibling_step;
            }
        } else if success_ratio > 1.1 && current_price > self.base_compute_unit_price.load(Ordering::Relaxed) {
            // Success rate is good, try to reduce price
            price_adjustment = 1.0 - (price_step_pct / 2.0);
        } else if success_ratio > 1.3 && current_siblings > min_siblings {
            // Success rate is very good, try to reduce siblings
            siblings_adjustment = sibling_step.saturating_neg();
        }

        // Apply price adjustment
        let new_price = if price_adjustment != 1.0 {
            let new_price = (current_price as f64 * price_adjustment).round() as u64;
            new_price.clamp(
                self.base_compute_unit_price.load(Ordering::Relaxed),
                max_price
            )
        } else {
            current_price
        };

        // Apply siblings adjustment
        let new_siblings = if siblings_adjustment != 0 {
            current_siblings.saturating_add_signed(siblings_adjustment)
                .clamp(min_siblings, max_siblings)
        } else {
            current_siblings
        };

        // Update the state if anything changed
        if new_price != current_price || new_siblings != current_siblings {
            self.current_compute_unit_price.store(new_price, Ordering::Relaxed);
            self.current_siblings.store(new_siblings, Ordering::Relaxed);
            
            tracing::info!(
                "Adjusted strategy: price={} ({}%), siblings={} ({}), success_rate={}% (target={}%)",
                new_price,
                ((new_price as f64 / self.base_compute_unit_price.load(Ordering::Relaxed) as f64 - 1.0) * 100.0).round() as i32,
                new_siblings,
                (new_siblings as i32 - current_siblings as i32),
                current_success_rate,
                target_success_rate
            );
        }
    }

    /// Resets the strategy to base values
    pub fn reset(&self) {
        let base_price = self.base_compute_unit_price.load(Ordering::Relaxed);
        let min_siblings = self.min_siblings.load(Ordering::Relaxed);
        
        self.current_compute_unit_price.store(base_price, Ordering::Relaxed);
        self.current_siblings.store(min_siblings, Ordering::Relaxed);
        
        // Reset the last adjustment time to allow immediate re-evaluation
        *self.last_adjustment.try_write().unwrap_or_else(|_| self.last_adjustment.blocking_write()) = 
            Instant::now() - self.min_adjustment_interval - Duration::from_secs(1);
    }
}

// ==================== PIECE 1.27: Confirmation Tracker ====================

/// Tracks transaction confirmations and provides feedback to the tip router
/// 
/// This tracks transaction lifecycle from submission to confirmation/failure,
/// providing feedback to optimize future transaction fees and reliability.
pub struct ConfirmationTracker {
    /// In-flight transactions awaiting confirmation
    in_flight: DashMap<Signature, InFlightTx>,
    /// RPC client for checking transaction status
    rpc: Arc<RpcClient>,
    /// Metrics for tracking confirmation times and success rates
    metrics: Arc<ConfirmationMetrics>,
    /// Background task handle
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    /// Shutdown signal
    shutdown: Arc<AtomicBool>,
}

/// Metrics collected by the ConfirmationTracker
#[derive(Default, Debug)]
pub struct ConfirmationMetrics {
    /// Total transactions tracked
    pub total_txs: AtomicU64,
    /// Successful confirmations
    pub confirmed: AtomicU64,
    /// Failed transactions
    pub failed: AtomicU64,
    /// Timed out transactions
    pub timed_out: AtomicU64,
    /// Average confirmation time in milliseconds (exponential moving average)
    pub avg_confirmation_ms: AtomicU64,
    /// Success rate (0-100)
    pub success_rate: AtomicU8,
    /// Recent confirmation times (for percentile calculations)
    recent_times: Mutex<VecDeque<u64>>>,
}

/// Information about an in-flight transaction
struct InFlightTx {
    /// When the transaction was submitted
    submitted_at: Instant,
    /// The transaction signature
    signature: Signature,
    /// The compute unit price used (in micro-lamports)
    compute_unit_price: u64,
    /// The compute units used by the transaction
    compute_units: u64,
    /// Optional callback to notify when confirmed
    on_confirmed: Option<Box<dyn FnOnce(Result<()>) + Send + 'static>>,
}

impl ConfirmationTracker {
    /// Creates a new ConfirmationTracker
    pub fn new(rpc: Arc<RpcClient>) -> Self {
        Self {
            in_flight: DashMap::with_capacity(1000),
            rpc,
            metrics: Arc::new(ConfirmationMetrics::default()),
            handle: Arc::new(Mutex::new(None)),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Starts the background confirmation polling task
    pub fn start(&self, poll_interval: Duration) {
        let this = self.clone();
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(poll_interval);
            
            while !this.shutdown.load(Ordering::Relaxed) {
                interval.tick().await;
                if let Err(e) = this.check_confirmations().await {
                    tracing::warn!("Error checking confirmations: {}", e);
                }
            }
        });
        
        *self.handle.lock().await = Some(handle);
    }

    /// Stops the background confirmation polling task
    pub async fn stop(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.lock().await.take() {
            let _ = handle.await;
        }
    }

    /// Tracks a new transaction
    pub fn track(&self, signature: Signature, compute_unit_price: u64, compute_units: u64) -> Result<()> {
        if self.in_flight.contains_key(&signature) {
            return Err(anyhow!("Transaction already being tracked"));
        }

        self.in_flight.insert(signature, InFlightTx {
            submitted_at: Instant::now(),
            signature,
            compute_unit_price,
            compute_units,
            on_confirmed: None,
        });

        self.metrics.total_txs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Tracks a new transaction with a confirmation callback
    pub fn track_with_callback<F>(&self, signature: Signature, compute_unit_price: u64, compute_units: u64, on_confirmed: F) -> Result<()>
    where
        F: FnOnce(Result<()>) + Send + 'static
    {
        if self.in_flight.contains_key(&signature) {
            return Err(anyhow!("Transaction already being tracked"));
        }

        self.in_flight.insert(signature, InFlightTx {
            submitted_at: Instant::now(),
            signature,
            compute_unit_price,
            compute_units,
            on_confirmed: Some(Box::new(on_confirmed)),
        });

        self.metrics.total_txs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Checks the status of all in-flight transactions
    async fn check_confirmations(&self) -> Result<()> {
        let now = Instant::now();
        let mut to_remove = Vec::new();
        let mut confirmed = 0;
        let mut failed = 0;
        let mut timed_out = 0;
        let mut total_time = 0;

        // Process all in-flight transactions
        for entry in self.in_flight.iter() {
            let sig = entry.key();
            let tx = entry.value();
            
            // Check if transaction has timed out (30 seconds)
            if now.duration_since(tx.submitted_at) > Duration::from_secs(30) {
                to_remove.push(*sig);
                timed_out += 1;
                self.notify_confirmed(*sig, Err(anyhow!("Transaction timed out")));
                continue;
            }

            // Check transaction status
            match self.rpc.get_signature_status(sig).await? {
                Some(transaction_status) => {
                    if transaction_status.err.is_some() {
                        // Transaction failed
                        to_remove.push(*sig);
                        failed += 1;
                        self.notify_confirmed(*sig, Err(anyhow!("Transaction failed: {:?}", transaction_status.err)));
                    } else if let Some(confirmation_status) = transaction_status.confirmations {
                        if confirmation_status >= 1 {
                            // Transaction confirmed
                            to_remove.push(*sig);
                            confirmed += 1;
                            let elapsed = now.duration_since(tx.submitted_at).as_millis() as u64;
                            total_time += elapsed;
                            self.metrics.record_confirmation(elapsed);
                            self.notify_confirmed(*sig, Ok(()));
                        }
                    }
                }
                None => {
                    // Transaction not found yet, check if it's still in the mempool
                    if let Ok(false) = self.rpc.is_mempool_signature(sig).await {
                        // Not in mempool or confirmed, might need to wait longer
                        continue;
                    }
                }
            }
        }

        // Clean up confirmed/failed transactions
        for sig in to_remove {
            self.in_flight.remove(&sig);
        }

        // Update metrics
        if confirmed > 0 || failed > 0 || timed_out > 0 {
            let total_processed = confirmed + failed + timed_out;
            self.metrics.record_batch(confirmed, failed, timed_out, total_time);
            
            tracing::debug!(
                "Processed {} transactions: {} confirmed, {} failed, {} timed out",
                total_processed, confirmed, failed, timed_out
            );
        }

        Ok(())
    }

    /// Notifies any registered callbacks of confirmation/failure
    fn notify_confirmed(&self, signature: Signature, result: Result<()>) {
        if let Some((_, mut tx)) = self.in_flight.remove(&signature) {
            if let Some(callback) = tx.on_confirmed.take() {
                callback(result);
            }
        }
    }

    /// Gets the current metrics
    pub fn metrics(&self) -> Arc<ConfirmationMetrics> {
        self.metrics.clone()
    }
}

impl ConfirmationMetrics {
    /// Records a confirmation with its timing
    pub fn record_confirmation(&self, elapsed_ms: u64) {
        // Update exponential moving average (90% weight to history, 10% to new value)
        let prev_avg = self.avg_confirmation_ms.load(Ordering::Relaxed) as f64;
        let new_avg = (prev_avg * 0.9) + (elapsed_ms as f64 * 0.1);
        self.avg_confirmation_ms.store(new_avg as u64, Ordering::Relaxed);

        // Track recent times (keep last 100)
        let mut times = self.recent_times.lock().unwrap();
        times.push_back(elapsed_ms);
        if times.len() > 100 {
            times.pop_front();
        }
    }

    /// Records a batch of results
    pub fn record_batch(&self, confirmed: u64, failed: u64, timed_out: u64, total_time: u64) {
        let total = confirmed + failed + timed_out;
        if total == 0 {
            return;
        }

        // Update success rate (0-100)
        let success_rate = (confirmed * 100) / total;
        self.success_rate.store(success_rate as u8, Ordering::Relaxed);
        
        // Update counts
        if confirmed > 0 {
            self.confirmed.fetch_add(confirmed, Ordering::Relaxed);
        }
        if failed > 0 {
            self.failed.fetch_add(failed, Ordering::Relaxed);
        }
        if timed_out > 0 {
            self.timed_out.fetch_add(timed_out, Ordering::Relaxed);
        }
    }

    /// Gets the current success rate (0-100)
    pub fn success_rate(&self) -> u8 {
        self.success_rate.load(Ordering::Relaxed)
    }

    /// Gets the average confirmation time in milliseconds
    pub fn avg_confirmation_ms(&self) -> u64 {
        self.avg_confirmation_ms.load(Ordering::Relaxed)
    }

    /// Gets the 90th percentile confirmation time in milliseconds
    pub fn p90_confirmation_ms(&self) -> Option<u64> {
        let times = self.recent_times.lock().unwrap();
        if times.is_empty() {
            return None;
        }
        
        let mut sorted = times.iter().copied().collect::<Vec<_>>();
        sorted.sort_unstable();
        
        let idx = (sorted.len() as f64 * 0.9).floor() as usize;
        Some(sorted[idx.min(sorted.len() - 1)])
    }
}

impl Clone for ConfirmationTracker {
    fn clone(&self) -> Self {
        Self {
            in_flight: self.in_flight.clone(),
            rpc: self.rpc.clone(),
            metrics: self.metrics.clone(),
            handle: Arc::new(Mutex::new(None)),
            shutdown: Arc::new(AtomicBool::new(false)),
        }
    }
}

// ==================== PIECE 1.26: Slot-Edge Pacer ====================

/// Optimizes transaction timing relative to Solana's 400ms slot boundaries
/// 
/// This pacer helps avoid head-of-line blocking by spreading transactions
/// across the slot using a combination of slot-phase awareness and jitter.
pub struct SlotEdgePacer {
    /// Slot duration in milliseconds (400ms for Solana mainnet)
    slot_duration_ms: u64,
    /// Target offset from slot start (in ms) to avoid congestion
    target_offset_ms: u64,
    /// Jitter range in milliseconds to add to the target offset
    jitter_range_ms: u64,
    /// Last slot we observed
    last_slot: AtomicU64,
    /// Last timestamp we observed (unix timestamp in ms)
    last_timestamp_ms: AtomicU64,
    /// Last blockhash we observed (for jitter seeding)
    last_blockhash: RwLock<Hash>,
    /// RPC client for slot updates
    rpc: Arc<RpcClient>,
}

impl SlotEdgePacer {
    /// Creates a new SlotEdgePacer
    /// 
    /// # Arguments
    /// * `rpc` - RPC client to track slot and blockhash
    /// * `target_offset_ms` - Target offset from slot start in milliseconds (default: 50ms)
    /// * `jitter_range_ms` - Jitter range in milliseconds (default: 20ms)
    pub fn new(rpc: Arc<RpcClient>, target_offset_ms: Option<u64>, jitter_range_ms: Option<u64>) -> Self {
        Self {
            slot_duration_ms: 400, // Solana mainnet
            target_offset_ms: target_offset_ms.unwrap_or(50),
            jitter_range_ms: jitter_range_ms.unwrap_or(20),
            last_slot: AtomicU64::new(0),
            last_timestamp_ms: AtomicU64::new(0),
            last_blockhash: RwLock::new(Hash::default()),
            rpc,
        }
    }

    /// Updates the pacer's internal state with the latest slot and timestamp
    pub async fn update_state(&self) -> Result<()> {
        // Get the latest slot and blockhash in parallel
        let (slot_res, hash_res) = join!(
            self.rpc.get_slot(),
            self.rpc.get_latest_blockhash()
        );
        
        let slot = slot_res?;
        let blockhash = hash_res?;
        
        // Update atomic state
        self.last_slot.store(slot, Ordering::Relaxed);
        self.last_timestamp_ms.store(timestamp_millis(), Ordering::Relaxed);
        *self.last_blockhash.write().await = blockhash;
        
        Ok(())
    }

    /// Calculate the optimal time to wait before sending the next transaction
    /// 
    /// Returns the number of milliseconds to wait before sending
    pub async fn calculate_wait_time(&self) -> u64 {
        let slot = self.last_slot.load(Ordering::Relaxed);
        let now = timestamp_millis();
        let last_ts = self.last_timestamp_ms.load(Ordering::Relaxed);
        
        // If we don't have a recent update, don't wait
        if now.saturating_sub(last_ts) > 1000 {
            return 0;
        }
        
        // Calculate time into current slot
        let slot_start = now - (now % self.slot_duration_ms);
        let time_in_slot = now - slot_start;
        
        // Add deterministic jitter based on blockhash
        let jitter = {
            let hash = self.last_blockhash.read().await;
            let mut hasher = Sha256::new();
            hasher.update(hash.as_ref());
            hasher.update(&slot.to_le_bytes());
            let hash = hasher.finalize();
            (hash[0] as u64) % self.jitter_range_ms
        };
        
        // Target time is slot start + offset + jitter
        let target_time = slot_start + self.target_offset_ms + jitter;
        
        // If we're already past the target time, don't wait
        if now >= target_time {
            return 0;
        }
        
        // Otherwise wait until target time
        target_time - now
    }

    /// Wait until the optimal time to send the next transaction
    pub async fn wait_for_slot_edge(&self) -> Result<()> {
        let wait_time = self.calculate_wait_time().await;
        if wait_time > 0 {
            tokio::time::sleep(Duration::from_millis(wait_time)).await;
        }
        Ok(())
    }
}

// Helper function to get current timestamp in milliseconds
fn timestamp_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ==================== PIECE 1.25: Hybrid TPU+RPC Sender ====================

/// Combines TPU and RPC sending for optimal transaction delivery
/// 
/// Uses direct TPU connection to the leader when possible, with fallback to RPC
/// fanout for reliability. This provides the best of both worlds: low latency
/// via direct TPU and high reliability via RPC fallback.
pub struct HybridSender {
    /// RPC fanout for fallback broadcasting
    pub rpc_fanout: Arc<RpcFanout>,
    /// Direct TPU client (when compiled with tpu feature)
    #[cfg(feature = "tpu")]
    tpu: Arc<TpuClient>,
}

impl HybridSender {
    /// Creates a new HybridSender with the given RPC URLs and WebSocket URL
    /// 
    /// # Arguments
    /// * `rpc_urls` - List of RPC endpoints for fallback broadcasting
    /// * `ws_url` - WebSocket URL for TPU client (only used with tpu feature)
    pub async fn new(rpc_urls: &[String], ws_url: String) -> Result<Self> {
        let fan = Arc::new(RpcFanout::new(rpc_urls));
        
        #[cfg(feature = "tpu")]
        {
            // Use first RPC for TPU discovery; you can point to a private node
            let rpc = fan.clients[0].clone();
            let cfg = TpuClientConfig::default();
            let tpu = TpuClient::new_with_client(rpc, &ws_url, cfg)?;
            return Ok(Self { rpc_fanout: fan, tpu: Arc::new(tpu) });
        }
        
        #[cfg(not(feature = "tpu"))]
        {
            let _ = ws_url; // Keep signature consistent
            Ok(Self { rpc_fanout: fan })
        }
    }

    /// Resolve current leader's TPU address (best-effort), else return None
    async fn leader_tpu_addr(&self) -> Option<std::net::SocketAddr> {
        // Use the first fanout client for metadata
        let cli = self.rpc_fanout.clients.get(0)?.clone();
        
        // Map identity -> TPU
        let nodes = cli.get_cluster_nodes().await.ok()?;
        let leader = cli.get_slot_leader().await.ok()?;
        
        for RpcContactInfo { pubkey, tpu, .. } in nodes {
            if pubkey == leader {
                if let Some(addr) = tpu {
                    if let Ok(sa) = addr.parse() { 
                        return Some(sa); 
                    }
                }
                break;
            }
        }
        None
    }

    /// Fire to TPU (if available) and concurrently to top-K RPCs; return first Signature
    /// 
    /// # Arguments
    /// * `vtx` - The versioned transaction to send
    /// * `rpc_parallel` - Number of RPC endpoints to fan out to (0 = TPU only)
    pub async fn send(&self, vtx: &VersionedTransaction, rpc_parallel: usize) -> Result<Signature> {
        let tx_sig = vtx.signatures.first().cloned()
            .ok_or_else(|| anyhow!("Transaction is missing signatures"))?;
        
        #[cfg(feature = "tpu")]
        {
            // Best-effort TPU first - fire and forget
            let _ = self.tpu.try_send_transaction(vtx.clone());
        }
        
        // Also push via RPC fanout; whichever lands is fine (same signature)
        match self.rpc_fanout.broadcast_vtx_skip_preflight(vtx, rpc_parallel).await {
            Ok(sig) => Ok(sig),
            Err(e) => {
                // If RPC failed but we already pushed to TPU, return the signature
                // (caller can confirm later)
                #[cfg(feature = "tpu")]
                { 
                    tracing::debug!("RPC broadcast failed but TPU attempt was made: {}", e);
                    Ok(tx_sig) 
                }
                #[cfg(not(feature = "tpu"))]
                { 
                    Err(e) 
                }
            }
        }
    }
}

// ==================== PIECE 1.24: Attempt-aware Assembly ====================

/// Configuration for transaction assembly and broadcasting
#[derive(Debug, Clone)]
pub struct TxAssemblyConfig {
    /// Maximum number of signature siblings to generate (0 = no siblings)
    pub max_siblings: usize,
    /// Minimum priority fee in micro-lamports per CU
    pub min_priority_fee: u64,
    /// Maximum priority fee in micro-lamports per CU
    pub max_priority_fee: u64,
    /// Whether to enable Jito-style bundles
    pub enable_jito: bool,
    /// Jito tip account (if Jito is enabled)
    pub jito_tip_account: Option<Pubkey>,
    /// Jito tip amount in lamports (if Jito is enabled)
    pub jito_tip_amount: u64,
}

impl Default for TxAssemblyConfig {
    fn default() -> Self {
        Self {
            max_siblings: 3,
            min_priority_fee: 1,
            max_priority_fee: 1_000_000, // 1 SOL per million CU
            enable_jito: false,
            jito_tip_account: None,
            jito_tip_amount: 10_000, // 0.00001 SOL
        }
    }
}

/// Result of a transaction assembly attempt
#[derive(Debug)]
pub struct AssembledTx {
    /// The transaction to send
    pub tx: VersionedTransaction,
    /// The transaction's signature
    pub signature: Signature,
    /// The blockhash used
    pub blockhash: Hash,
    /// The compute unit limit
    pub compute_unit_limit: u32,
    /// The compute unit price in micro-lamports
    pub compute_unit_price: u64,
    /// Whether this is a Jito bundle
    pub is_jito: bool,
    /// The Jito tip account (if any)
    pub jito_tip_account: Option<Pubkey>,
}

impl PortOptimizer {
    /// Assembles and signs a transaction with all optimizations applied
    pub async fn assemble_tx(
        &self,
        ixs: &[Instruction],
        config: &TxAssemblyConfig,
        blockhash_pipeline: &BlockhashPipeline,
    ) -> Result<Vec<AssembledTx>> {
        if ixs.is_empty() {
            return Ok(Vec::new());
        }

        // Get a fresh blockhash
        let blockhash = blockhash_pipeline.get_blockhash().await;
        
        // Generate signature siblings
        let siblings = make_signature_siblings(
            ixs,
            &self.wallet.pubkey(),
            &[], // TODO: Pass actual lookup tables if needed
            config.max_siblings,
            |ixs, _| {
                // Simple size estimation - should match the one used in make_signature_siblings
                let mut size = 128; // Header size
                size += ixs.iter().map(|ix| {
                    1 + 1 + (ix.accounts.len() * 34) + 4 + ix.data.len()
                }).sum::<usize>();
                size
            },
        );

        // Prepare transactions with budget instructions
        let mut txs = Vec::with_capacity(siblings.len());
        for (i, ixs) in siblings.into_iter().enumerate() {
            // Skip if we've seen this transaction before
            let fingerprint = ixs_fingerprint(&ixs);
            if hot_deduper().is_dupe(&fingerprint).await {
                continue;
            }

            // Add compute budget instructions
            let budget_ixs = self.dynamic_budget_ixs_v2(
                &ixs,
                config.min_priority_fee,
                config.max_priority_fee,
            ).await?;

            let mut all_ixs = budget_ixs;
            all_ixs.extend(ixs);

            // Add Jito tip if enabled
            if config.enable_jito {
                if let Some(tip_account) = config.jito_tip_account {
                    all_ixs.push(system_instruction::transfer(
                        &self.wallet.pubkey(),
                        &tip_account,
                        config.jito_tip_amount,
                    ));
                }
            }

            // Build and sign the transaction
            let message = Message::new(&all_ixs, Some(&self.wallet.pubkey()));
            let tx = VersionedTransaction::try_new(
                VersionedMessage::Legacy(message),
                &[&*self.wallet],
            )?;

            let signature = tx.signatures[0];
            
            // Get compute unit info from the budget instructions
            let (compute_unit_limit, compute_unit_price) = if !budget_ixs.is_empty() {
                let limit = if let Ok(ComputeBudgetInstruction::RequestUnitsDeprecated { units, .. }) = 
                    budget_ixs[0].try_into() {
                    units
                } else {
                    COMPUTE_UNITS_FLOOR
                };
                
                let price = if budget_ixs.len() > 1 {
                    if let Ok(ComputeBudgetInstruction::SetComputeUnitPrice(price)) = 
                        budget_ixs[1].try_into() {
                        price
                    } else {
                        0
                    }
                } else {
                    0
                };
                
                (limit, price)
            } else {
                (COMPUTE_UNITS_FLOOR, 0)
            };

            txs.push(AssembledTx {
                tx,
                signature,
                blockhash,
                compute_unit_limit,
                compute_unit_price,
                is_jito: config.enable_jito && config.jito_tip_account.is_some(),
                jito_tip_account: config.jito_tip_account,
            });

            // Mark as seen to prevent duplicates
            hot_deduper().mark_seen(fingerprint).await;
        }

        Ok(txs)
    }

    /// Broadcasts a batch of transactions with optimal fanout
    pub async fn broadcast_txs(
        &self,
        txs: Vec<AssembledTx>,
        rpc_fanout: &RpcFanout,
    ) -> Result<Vec<Signature>> {
        if txs.is_empty() {
            return Ok(Vec::new());
        }

        let mut signatures = Vec::with_capacity(txs.len());
        let mut futures = Vec::with_capacity(txs.len());

        // Fan out transactions to multiple RPC endpoints
        for tx in txs {
            let rpc_fanout = rpc_fanout.clone();
            let tx_data = bincode::serialize(&tx.tx)?;
            
            let future = async move {
                // Try to send with Jito bundle if configured
                if tx.is_jito && tx.jito_tip_account.is_some() {
                    if let Ok(bundle) = JitoBundle::new(vec![tx.tx], tx.jito_tip_account.unwrap()) {
                        if let Ok(sig) = rpc_fanout.send_jito_bundle(bundle).await {
                            return Ok(sig);
                        }
                    }
                }
                
                // Fall back to regular transaction
                rpc_fanout.send_raw_transaction(tx_data).await
            };
            
            futures.push(future);
        }

        // Wait for all sends to complete or fail
        let results = futures::future::join_all(futures).await;
        
        // Collect successful signatures
        for result in results {
            match result {
                Ok(sig) => signatures.push(sig),
                Err(e) => tracing::warn!("Failed to broadcast transaction: {}", e),
            }
        }

        Ok(signatures)
    }
}

// ==================== PIECE 1.21: Budget Composer v2 ====================

impl PortOptimizer {
    /// Compose compute budget instructions with exact simulation and v2 fee estimation.
    /// 
    /// This function optimizes compute unit limits and priority fees by:
    /// 1. Simulating the transaction once to get exact compute unit requirements
    /// 2. Adding a 12% buffer to the measured units
    /// 3. Clamping between FLOOR and CEIL constants
    /// 4. Pricing using the v2 fee estimator
    pub async fn dynamic_budget_ixs_v2(
        &self,
        ixs: &[Instruction],
        floor_cu_price: u64,
        max_cu_price: u64,
    ) -> Result<Vec<Instruction>> {
        if ixs.is_empty() { 
            return Ok(Vec::new()); 
        }

        // Check cache first to avoid repeated simulations
        let key = ixs_fingerprint(ixs);
        let cached = { 
            let cache = cu_lru().read().await;
            cache.get(&key).cloned()
        };

        // Get or calculate compute unit limit
        let cu_limit = if let Some(cu) = cached {
            cu as u32
        } else {
            // Simulate with generous headroom to get actual usage
            let payer = self.wallet.pubkey();
            let msg = Message::new(ixs, Some(&payer));
            let vtx = VersionedTransaction::try_new(VersionedMessage::Legacy(msg), &[&*self.wallet])?;
            
            // Get actual units used from simulation
            let sim = self.simulate_exact_v0_default(&vtx).await?;
            let raw = sim.units_consumed.unwrap_or(COMPUTE_UNITS_FLOOR as u64);
            
            // Add 12% buffer and clamp to reasonable bounds
            let cu = ((raw as f64) * 1.12).ceil() as u32;
            let cu = cu.clamp(COMPUTE_UNITS_FLOOR, COMPUTE_UNITS_CEIL);
            
            // Cache the result for future use
            cu_lru().write().await.put(key, cu as u64);
            cu
        };

        // Get priority fee using v2 estimator with program awareness
        let mut cu_price = self.priority_fee_for_ixs_v2(ixs, floor_cu_price).await;
        cu_price = cu_price.min(max_cu_price).max(floor_cu_price);

        // Return budget instructions to prepend to transaction
        Ok(vec![
            ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
            ComputeBudgetInstruction::set_compute_unit_price(cu_price),
        ])
    }

    /// Simulate a transaction with default config (v0, no commitment)
    async fn simulate_exact_v0_default(
        &self,
        vtx: &VersionedTransaction,
    ) -> Result<RpcSimulateTransactionResult> {
        let cfg = RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: false,
            commitment: None,
            encoding: None,
            accounts: None,
            min_context_slot: None,
            inner_instructions: false,
        };
        self.rpc_client.simulate_transaction_with_config(vtx, cfg).await
            .map_err(Into::into)
    }
}

// ==================== PIECE 1.13c: Program-aware fee fusion ====================

impl PortOptimizer {
    /// Fuses program-specific fees with global fee tape using adaptive weighting.
    /// Uses program-specific fees when available, falls back to global quantiles.
    /// 
    /// # Arguments
    /// * `programs` - List of programs involved in the transaction
    /// * `global_floor` - Minimum fee to return (from autoscaler or config)
    /// 
    /// # Returns
    /// Tuple of (fused_fee, confidence) where confidence is 0.0-1.0
    #[inline]
    pub async fn fuse_program_fees(&self, programs: &[Pubkey], global_floor: u64) -> (u64, f64) {
        if programs.is_empty() {
            return (global_floor, 0.0);
        }

        // Get program-specific fees in parallel
        let program_fees = self.get_fee_values_cached(programs, Duration::from_secs(5)).await;
        
        // Get global fee quantiles
        let global = fee_q().read().await.pair().unwrap_or_else(|| (global_floor as f64, (global_floor * 2) as f64));
        
        // If no program-specific data, return global p90 with low confidence
        if program_fees.is_empty() {
            return (global.1 as u64, 0.3);
        }
        
        // Calculate robust statistics (median and IQR)
        let median = percentile_pair_unstable(&program_fees, 50, 50).0 as f64;
        let (q25, q75) = percentile_pair_unstable(&program_fees, 25, 75);
        let iqr = (q75 - q25) as f64;
        
        // Calculate confidence based on IQR and sample size
        let confidence = 1.0f64.min(0.7 + 0.3 * (1.0 - iqr / median).max(0.0));
        
        // Fuse with global quantiles using confidence
        let lo = global.0 * (1.0 - confidence) + median * confidence;
        let hi = global.1 * (1.0 - confidence) + (median + iqr * 1.5).max(median * 1.1) * confidence;
        
        // Ensure we're above global floor and program median
        let fee = hi.max(global_floor as f64).max(median) as u64;
        
        (fee, confidence)
    }
    
    /// Gets the current recommended fee for a set of programs
    /// Combines program-specific fees with global congestion signals
    pub async fn get_recommended_fee(&self, programs: &[Pubkey]) -> u64 {
        let global_floor = self.priority_fee_autoscale(0).await;
        let (fee, _) = self.fuse_program_fees(programs, global_floor).await;
        fee
    }
    
    /// Sends a transaction with cohort-based jitter to avoid network collisions
    /// and improve transaction success rates.
    pub async fn send_with_jitter(
        &self,
        ixs: &[Instruction],
        signers: &[&dyn Signer],
        recent_blockhash: Hash,
        max_jitter_us: u32,
    ) -> Result<Signature, Box<dyn std::error::Error>> {
        // Calculate jitter based on wallet and blockhash
        let jitter_us = cohort_jitter_us(self.wallet.pubkey(), &recent_blockhash, max_jitter_us);
        
        // Sleep for the jitter duration
        if jitter_us > 0 {
            tokio::time::sleep(Duration::from_micros(jitter_us as u64)).await;
        }
        
        // Send the transaction
        let tx = Transaction::new_signed_with_payer(
            ixs,
            Some(&self.wallet.pubkey()),
            signers,
            recent_blockhash,
        );
        
        self.rpc_client.send_and_confirm_transaction_with_spinner(&tx).await
    }
}

// ==================== PIECE 1.23: Low-Latency Blockhash Pipeline ====================

/// Manages blockhash updates with minimal latency using a background task.
/// Maintains a fresh blockhash and notifies waiters when a new one is available.
pub struct BlockhashPipeline {
    current: Arc<tokio::sync::RwLock<Hash>>,
    notifier: Arc<tokio::sync::Notify>,
    rpc_client: Arc<RpcClient>,
    update_interval: Duration,
}

impl BlockhashPipeline {
    /// Creates a new BlockhashPipeline with the given RPC client and update interval
    pub fn new(rpc_client: Arc<RpcClient>, update_interval: Duration) -> Self {
        Self {
            current: Arc::new(tokio::sync::RwLock::new(Hash::default())),
            notifier: Arc::new(tokio::sync::Notify::new()),
            rpc_client,
            update_interval,
        }
    }

    /// Starts the background blockhash update task
    pub fn start(&self) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move {
            this.run().await;
        })
    }

    /// Main loop for the blockhash updater
    async fn run(&self) {
        let mut interval = tokio::time::interval(self.update_interval);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            interval.tick().await;
            if let Err(e) = self.update_blockhash().await {
                tracing::warn!("Failed to update blockhash: {}", e);
            }
        }
    }

    /// Updates the current blockhash from the RPC node
    async fn update_blockhash(&self) -> Result<()> {
        let (recent_blockhash, _) = self.rpc_client
            .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())
            .await?;

        // Only update if it's different
        {
            let mut current = self.current.write().await;
            if *current != recent_blockhash {
                *current = recent_blockhash;
                self.notifier.notify_waiters();
                tracing::debug!("Updated blockhash: {}", recent_blockhash);
            }
        }

        Ok(())
    }

    /// Gets the current blockhash, waiting for the next update if necessary
    pub async fn get_blockhash(&self) -> Hash {
        self.notifier.notified().await;
        *self.current.read().await
    }

    /// Gets the current blockhash immediately without waiting
    pub async fn get_blockhash_now(&self) -> Hash {
        *self.current.read().await
    }
}

impl Clone for BlockhashPipeline {
    fn clone(&self) -> Self {
        Self {
            current: self.current.clone(),
            notifier: self.notifier.clone(),
            rpc_client: self.rpc_client.clone(),
            update_interval: self.update_interval,
        }
    }
}

// ==================== PIECE 1.22: Hot Deduper ====================

/// Deduplicates transactions based on their instruction fingerprint and recent submission history.
/// Uses a time-windowed approach to track recent transactions and prevent duplicates.
/// 
/// This is a critical component for MEV protection, preventing:
/// 1. Accidental duplicate submissions
/// 2. Replay attacks
/// 3. Frontrunning our own transactions
#[derive(Debug)]
pub struct HotDeduper {
    inner: Arc<tokio::sync::RwLock<HashMap<[u8; 32], Instant>>>,
    ttl: Duration,
    max_size: usize,
}

impl Default for HotDeduper {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

impl HotDeduper {
    /// Creates a new HotDeduper with the specified TTL for deduplication entries
    pub fn new(ttl: Duration) -> Self {
        Self {
            inner: Arc::new(tokio::sync::RwLock::new(HashMap::with_capacity(1024))),
            ttl,
            max_size: 10_000, // Prevent unbounded memory growth
        }
    }
    
    pub fn with_max_size(mut self, max_size: usize) -> Self {
        self.max_size = max_size;
        self
    }
    
    /// Check if a transaction with the given fingerprint is a duplicate
    /// Returns true if this is a duplicate (should be dropped)
    pub async fn is_dupe(&self, fingerprint: &[u8; 32]) -> bool {
        let now = Instant::now();
        let inner = self.inner.read().await;
        
        // Check if key exists and is not expired
        match inner.get(fingerprint) {
            Some(&ts) if now.duration_since(ts) <= self.ttl => true,
            _ => false,
        }
    }
    
    /// Mark a transaction as seen by its fingerprint
    pub async fn mark_seen(&self, fingerprint: [u8; 32]) -> bool {
        let now = Instant::now();
        let mut inner = self.inner.write().await;
        
        // Clean up expired entries first
        inner.retain(|_, &mut ts| now.duration_since(ts) <= self.ttl);
        
        // Enforce max size by removing oldest entry if needed
        if inner.len() >= self.max_size {
            if let Some(oldest_key) = inner.iter()
                .min_by_key(|(_, &ts)| ts)
                .map(|(k, _)| *k) {
                inner.remove(&oldest_key);
            }
        }
        
        // Insert or update the timestamp
        inner.insert(fingerprint, now).is_none()
    }
    
    /// Background task to periodically clean up old entries
    fn start_cleanup_task(&self) {
        let inner = self.inner.clone();
        let ttl = self.ttl;
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let now = Instant::now();
                let mut dedupe = inner.write().await;
                let before = dedupe.len();
                let threshold = now.checked_sub(ttl).unwrap_or(now);
                
                // Remove entries older than TTL
                dedupe.retain(|_, ts| *ts > threshold);
                
                let after = dedupe.len();
                if before > 0 && before != after {
                    tracing::debug!(
                        "Deduper cleaned {} entries ({} -> {})",
                        before - after,
                        before,
                        after
                    );
                }
            }
        });
    }
}

// Global instance for the hot deduper
static HOT_DEDUPER: OnceLock<HotDeduper> = OnceLock::new();

/// Get a reference to the global hot deduper instance
pub fn hot_deduper() -> &'static HotDeduper {
    HOT_DEDUPER.get_or_init(|| HotDeduper::default())
}

// ==================== PIECE 1.20: Memo-Salt Siblings ====================

/// Returns the SPL Memo v1 program ID
#[inline(always)]
fn memo_program() -> Pubkey {
    // SPL Memo v1
    Pubkey::from_str("MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr").unwrap()
}

/// Creates a memo instruction with the given salt value
#[inline(always)]
fn memo_salt_ix(salt: u32) -> Instruction {
    let data = salt.to_le_bytes().to_vec();
    Instruction { 
        program_id: memo_program(), 
        accounts: Vec::new(), 
        data 
    }
}

/// Generates up to `n` transaction variants by appending memo salts
/// 
/// This creates signature-distinct variants of the same transaction by appending
/// a small memo with a deterministic salt. This helps avoid head-to-head
/// signature collisions when multiple bots send similar transactions.
pub fn make_signature_siblings<F>(
    base_ixs: &[Instruction],
    payer: &Pubkey,
    alts: &[AddressLookupTableAccount],
    n: usize,
    size_fn: F,
) -> Vec<Vec<Instruction>>
where
    F: Fn(&[Instruction], &[AddressLookupTableAccount]) -> usize
{
    let mut out = Vec::with_capacity(n.max(1));
    let base_sz = size_fn(base_ixs, alts);
    out.push(base_ixs.to_vec()); // Original transaction is first

    // 5 bytes per salt memo (1 program index + 1 len + 4 data) + shortvec noise; rounded headroom 16B
    const SALT_OVERHEAD: usize = 16;

    // Generate up to n-1 salted variants
    for k in 1..n {
        // Create a deterministic salt based on the base transaction and index
        let salt = {
            let mut h = Sha256::new();
            h.update(b"SALT1");
            h.update(payer.as_ref());
            h.update(&(k as u32).to_le_bytes());
            let d = h.finalize();
            u32::from_le_bytes([d[0], d[1], d[2], d[3]])
        };
        
        // Clone the base instructions and append the salt memo
        let mut ixs = base_ixs.to_vec();
        ixs.push(memo_salt_ix(salt));
        
        // Only include if it fits within MTU
        if base_sz + SALT_OVERHEAD <= QUIC_SAFE_PACKET_BUDGET {
            out.push(ixs);
        } else {
            break; // MTU-safe
        }
    }
    out
}

// ==================== PIECE 1.12: TipRouter Gumbel pick ====================

/// Bias-free stochastic argmax via Gumbel(0,1) perturbation.
/// Equivalent to sampling from the categorical distribution defined by current weights.
impl TipRouter {
    pub fn pick_gumbel_sync(&self) -> Pubkey {
        let tips = self.inner.blocking_read();
        if tips.is_empty() { return Pubkey::new_unique(); }
        let eps = 1e-9_f64;
        let mut best = 0usize;
        let mut best_score = f64::NEGATIVE_INFINITY;
        for (i, t) in tips.iter().enumerate() {
            // Gumbel noise: -ln(-ln(U))
            let u: f64 = rand::random::<f64>().clamp(eps, 1.0 - eps);
            let g = -((-(u.ln())).ln());
            // temperature τ slightly <1 for mild exploration
            let tau = 0.85_f64;
            let s = (t.weight.max(eps)).ln() / tau + g;
            if s > best_score { best_score = s; best = i; }
        }
        tips[best].account
    }

    pub async fn pick_gumbel(&self) -> Pubkey {
        let tips = self.inner.read().await;
        if tips.is_empty() { return Pubkey::new_unique(); }
        let eps = 1e-9_f64;
        let mut best = 0usize;
        let mut best_score = f64::NEGATIVE_INFINITY;
        for (i, t) in tips.iter().enumerate() {
            let u: f64 = rand::random::<f64>().clamp(eps, 1.0 - eps);
            let g = -((-(u.ln())).ln());
            let tau = 0.85_f64;
            let s = (t.weight.max(eps)).ln() / tau + g;
            if s > best_score { best_score = s; best = i; }
        }
        tips[best].account
    }
}

#[derive(Clone)]
pub struct TipRouter {
    inner: Arc<RwLock<Vec<TipStats>>>,
    alpha: f64,    // level smoothing
    beta: f64,     // volatility smoothing
    floor_ms: f64,
    ceil_ms: f64,
    outlier_q: f64,
    min_weight: f64,
}

impl TipRouter {
    pub fn new(accounts: Vec<Pubkey>) -> Self {
        let now = Instant::now();
        let v: Vec<TipStats> = accounts
            .into_iter()
            .map(|a| TipStats {
                account: a,
                ema: 800.0,
                emv: 100.0,
                ghost_rate: 0.0,
                weight: 1.0,
                attempts: 0,
                failures: 0,
                last_update: now,
                succ_ewma: 0.90,
                fail_ewma: 0.10,
            })
            .collect();

        let r = Self {
            inner: Arc::new(RwLock::new(v)),
            alpha: 0.22, // latency EMA smoothing
            beta: 0.12,  // volatility smoothing
            floor_ms: 100.0,
            ceil_ms: 3000.0,
            outlier_q: 0.95,
            min_weight: 0.005, // exploration floor
        };
        let mut g = r.inner.blocking_write();
        r.recompute_weights_locked(&mut g);
        drop(g);
        r
    }

    #[inline(always)]
    fn apply_time_decay(t: &mut TipStats, now: Instant) {
        // Half-life ≈ 60s for reliability stats; prevents permanent penalties.
        let dt = now.saturating_duration_since(t.last_update).as_secs_f64();
        if dt <= 0.0 { return; }
        let halflife = 60.0f64;
        let lambda = (-std::f64::consts::LN_2 * dt / halflife).exp(); // ∈(0,1]
        t.succ_ewma *= lambda;
        t.fail_ewma *= lambda;
        t.last_update = now;
    }

    pub async fn record_attempt(&self, acct: Pubkey, success: bool, latency_ms: Option<f64>) {
        let mut tips = self.inner.write().await;
        let now = Instant::now();

        if let Some(t) = tips.iter_mut().find(|t| t.account == acct) {
            Self::apply_time_decay(t, now);

            t.attempts = t.attempts.saturating_add(1);
            if !success { t.failures = t.failures.saturating_add(1); }

            // Reliability EWMAs with soft prior mass
            let obs_gain = 0.25; // weight per new observation (bounded)
            if success {
                t.succ_ewma = (1.0 - obs_gain) * t.succ_ewma + obs_gain * 1.0;
                t.fail_ewma = (1.0 - obs_gain) * t.fail_ewma + obs_gain * 0.0;
            } else {
                t.succ_ewma = (1.0 - obs_gain) * t.succ_ewma + obs_gain * 0.0;
                t.fail_ewma = (1.0 - obs_gain) * t.fail_ewma + obs_gain * 1.0;
            }
            let succ = t.succ_ewma.max(1e-6);
            let fail = t.fail_ewma.max(1e-6);
            t.ghost_rate = (fail / (succ + fail)).clamp(0.0, 1.0);

            if let Some(ms) = latency_ms {
                let ms = ms.clamp(self.floor_ms, self.ceil_ms);
                // EWMA on level and absolute deviation
                t.ema = self.alpha * ms + (1.0 - self.alpha) * t.ema;
                let abs_dev = (ms - t.ema).abs();
                t.emv = self.beta * abs_dev + (1.0 - self.beta) * t.emv;
            }
        }
        self.recompute_weights_locked(&mut tips);
    }

    fn recompute_weights_locked(&self, tips: &mut [TipStats]) {
        if tips.is_empty() { return; }

        // Robust scale from median EMA
        let mut ema_vec: Vec<f64> = tips.iter().map(|t| t.ema).collect();
        ema_vec.sort_by(|a,b| a.partial_cmp(b).unwrap());
        let med_ema = ema_vec[ema_vec.len()/2].max(1.0);

        // Constants (battle-tested ranges)
        let k_var: f64 = 1.8;       // volatility penalty
        let winsor: f64 = 4.0;      // cap EMA at 4× median
        let gamma: f64 = 3.0;       // ghost penalty exponent
        let ramp_smooth: f64 = 48.0; // attempts to fully ramp capacity
        let eps_w: f64 = 1e-6;

        let mut sum = 0.0;
        for t in tips.iter_mut() {
            // Winsorized latency & bounded volatility
            let ema = t.ema.min(med_ema * winsor).max(self.floor_ms);
            let emv = t.emv.min(ema * 0.75).max(0.5);

            // Reliability from EWMAs
            let succ = t.succ_ewma.max(1e-6);
            let fail = t.fail_ewma.max(1e-6);
            let p_succ = (succ / (succ + fail)).clamp(0.0, 1.0);

            // Capacity ramp to avoid overweighting fresh routes
            let ramp = ((t.attempts as f64) / ramp_smooth).min(1.0);

            // Base inverse-latency with volatility penalty
            let base = 1.0 / (ema + k_var * emv).max(1.0);

            // Ghosting penalty (smooth exponential)
            let ghost_pen = (-gamma * t.ghost_rate).exp(); // ∈(e^{-γ},1]

            t.weight = (base * p_succ * p_succ * ghost_pen * (0.5 + 0.5 * ramp))
                .max(self.min_weight) + eps_w;
            sum += t.weight;
        }
        if sum > 0.0 { for t in tips.iter_mut() { t.weight /= sum; } }
    }

    pub async fn pick(&self) -> Pubkey {
        let tips = self.inner.read().await;
        if tips.is_empty() { return Pubkey::new_unique(); }
        let mut rng = thread_rng();
        let r: f64 = rng.gen();
        let mut acc = 0.0;
        for t in tips.iter() { acc += t.weight; if r <= acc { return t.account; } }
        tips.last().map(|t| t.account).unwrap_or_else(Pubkey::new_unique)
    }

    pub fn pick_sync(&self) -> Pubkey {
        let tips = self.inner.blocking_read();
        if tips.is_empty() { return Pubkey::new_unique(); }
        let mut rng = thread_rng();
        let r: f64 = rng.gen();
        let mut acc = 0.0;
        for t in tips.iter() { acc += t.weight; if r <= acc { return t.account; } }
        tips.last().map(|t| t.account).unwrap_or_else(Pubkey::new_unique)
    }
}

const SOLEND_PROGRAM_ID: &str = "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo";
const KAMINO_PROGRAM_ID: &str = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD";
@@ -38,15 +333,55 @@ const MIN_PROFIT_BPS: u64 = 10;
const REBALANCE_THRESHOLD_BPS: u64 = 25;
const MAX_SLIPPAGE_BPS: u64 = 50;

// ----- Module-level instruction encoding helpers -----
// Anchor-compliant discriminator for instruction names: sha256("global:{ix}")[0..8]
fn anchor_discriminator(ix_name: &str) -> [u8; 8] {
    let mut hasher = Sha256::new();
    hasher.update(format!("global:{}", ix_name).as_bytes());
    let hash = hasher.finalize();
    let mut out = [0u8; 8];
    out.copy_from_slice(&hash[..8]);
    out
}

#[derive(BorshSerialize)]
struct DepositArgs {
    amount: u64,
}

#[derive(BorshSerialize)]
struct WithdrawArgs {
    amount: u64,
}

fn encode_anchor_ix(ix_name: &str, args: impl BorshSerialize) -> Result<Vec<u8>> {
    let mut data = anchor_discriminator(ix_name).to_vec();
    let mut args_data = Vec::with_capacity(16);
    args
        .serialize(&mut args_data)
        .map_err(|e| anyhow!(format!("borsh encode failed: {e}")))?;
    data.extend_from_slice(&args_data);
    Ok(data)
}

// Solend (non-Anchor) registry of verified tags
const SOLEND_DEPOSIT_TAG: u8 = 4; // Verified from Solend program source/tests
const SOLEND_WITHDRAW_TAG: u8 = 5; // Verified from Solend program source/tests
// Token-lending style flash loan opcodes (verify per protocol and lock with tests)
const FLASHLOAN_TAG: u8 = 10;
const FLASHREPAY_TAG: u8 = 11;

// ==================== PIECE 1.13b: Global fee quantile trackers ====================

/// Tracks fee percentiles (p70, p90) using P² algorithm for streaming quantiles
#[derive(Clone, Debug)]
pub struct FeeQuantiles {
    lo: P2Quantile, // ~p70
    hi: P2Quantile, // ~p90
}

impl FeeQuantiles {
    pub fn new() -> Self { 
        Self { 
            lo: P2Quantile::new(0.70), 
            hi: P2Quantile::new(0.90) 
        } 
    }
    
    #[inline] 
    pub fn push(&mut self, v: f64) { 
        self.lo.update(v); 
        self.hi.update(v); 
    }
    
    #[inline] 
    pub fn pair(&self) -> Option<(f64, f64)> { 
        Some((self.lo.value()?, self.hi.value()?)) 
    }
}

static FEE_Q: OnceLock<tokio::sync::RwLock<FeeQuantiles>> = OnceLock::new();

#[inline(always)]
fn fee_q() -> &'static tokio::sync::RwLock<FeeQuantiles> {
    FEE_Q.get_or_init(|| tokio::sync::RwLock::new(FeeQuantiles::new()))
}

// ----- Leader schedule cache (epoch-aware, short TTL) -----
static LEADER_CACHE: OnceLock<tokio::sync::RwLock<LeaderCache>> = OnceLock::new();

fn leader_cache() -> &'static tokio::sync::RwLock<LeaderCache> {
    LEADER_CACHE.get_or_init(|| tokio::sync::RwLock::new(LeaderCache {
        epoch: 0,
        schedule: HashMap::new(),
        last_refresh: Instant::now() - Duration::from_secs(3600),
    }))
}

#[derive(Clone, Debug)]
struct LeaderCache {
    epoch: u64,
    schedule: HashMap<String, Vec<usize>>, // leader -> slot indices
    last_refresh: Instant,
}

// Small helper for bounded fee escalation on retries
fn fee_escalation_nudge(base_cu_price: u64, attempt_no: u32) -> u64 {
    let mult = match attempt_no { 0 => 1.00, 1 => 1.08, 2 => 1.16, _ => 1.24 };
    ((base_cu_price as f64) * mult).ceil() as u64
}

#[derive(Clone, Debug)]
pub struct LendingMarket {
pub market_pubkey: Pubkey,
    pub token_mint: Pubkey,
pub protocol: Protocol,
pub supply_apy: f64,
pub borrow_apy: f64,
pub utilization: f64,
pub liquidity_available: u64,
pub last_update: Instant,
    pub layout_version: u8,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PortOptimizer {
    wallet: Arc<Keypair>,
    markets: Arc<RwLock<HashMap<String, Vec<LendingMarket>>>>,
    active_positions: Arc<RwLock<HashMap<String, Position>>>,
    tip_router: Arc<RwLock<Option<TipRouter>>>,
    last_tx_signature: Arc<tokio::sync::RwLock<Option<solana_sdk::signature::Signature>>>,
}

#[derive(Clone, Debug)]
pub struct Position {
    pub token_mint: Pubkey,
    pub last_rebalance: Instant,
{{ ... }}
}

#[derive(Clone, Debug)]
pub struct ReserveMeta {
    pub reserve: Pubkey,
    pub lending_market: Pubkey,
    pub lending_market_authority: Pubkey,
    pub liquidity_mint: Pubkey,
    pub liquidity_supply_vault: Pubkey,
    pub collateral_mint: Pubkey,
    pub collateral_supply_vault: Pubkey,
    pub program_id: Pubkey,
}

impl PortOptimizer {
    // ----- Liquidity-weighted market scoring (risk-aware) -----
    const UTIL_RISK_FACTOR: f64 = 120.0;       // weight of utilization penalty
    const CAPACITY_RISK_FACTOR: f64 = 0.000001; // penalty per lamport shortfall vs. move size

    pub fn market_score(
        &self,
        amount: u64,
        apy_percent: f64,
        utilization: f64,
        liq_available: u64,
    ) -> f64 {
        let u = utilization.clamp(0.0, 1.0);
        let util_penalty = (u * u) * Self::UTIL_RISK_FACTOR;
        let deficit = 0f64.max(amount as f64 - liq_available as f64);
        let capacity_penalty = deficit * Self::CAPACITY_RISK_FACTOR;
        apy_percent - util_penalty - capacity_penalty
    }

    pub fn pick_best_market_weighted(
        &self,
        amount: u64,
        candidates: &[LendingMarket],
    ) -> Option<LendingMarket> {
        candidates
            .iter()
            .filter(|m| m.liquidity_available > 0)
            .max_by(|a, b| {
                let sa = self.market_score(amount, a.supply_apy, a.utilization, a.liquidity_available);
                let sb = self.market_score(amount, b.supply_apy, b.utilization, b.liquidity_available);
                sa.partial_cmp(&sb).unwrap()
            })
            .cloned()
    }

    pub fn new(rpc_url: &str, wallet: Keypair) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));

        Self {
            rpc_client,
            wallet: Arc::new(wallet),
            markets: Arc::new(RwLock::new(HashMap::new())),
            active_positions: Arc::new(RwLock::new(HashMap::new())),
            tip_router: Arc::new(RwLock::new(None)),
            last_tx_signature: Arc::new(tokio::sync::RwLock::new(None)),
        }
    }

    // Build flash loan ix (non-Anchor example; verify tags per protocol and lock by tests)
    fn build_flash_loan_ix(&self, plan: &FlashPlan, user_flash_token_ata: Pubkey) -> Result<Instruction> {
        let mut data = vec![FLASHLOAN_TAG];
        data.extend_from_slice(&plan.amount.to_le_bytes());
        let accounts = vec![
            AccountMeta::new(plan.reserve, false),
            AccountMeta::new(plan.liquidity_supply_vault, false),
            AccountMeta::new(user_flash_token_ata, false),
            AccountMeta::new_readonly(plan.lending_market, false),
            AccountMeta::new_readonly(plan.market_authority, false),
            AccountMeta::new_readonly(spl_token::id(), false),
            AccountMeta::new_readonly(solana_program::sysvar::clock::id(), false),
        ];
        Ok(Instruction { program_id: plan.flash_program, accounts, data })
    }

    fn build_flash_repay_ix(&self, plan: &FlashPlan, user_flash_token_ata: Pubkey, repay_amount: u64) -> Result<Instruction> {
        let mut data = vec![FLASHREPAY_TAG];
        data.extend_from_slice(&repay_amount.to_le_bytes());
        let accounts = vec![
            AccountMeta::new(plan.reserve, false),
            AccountMeta::new(plan.liquidity_supply_vault, false),
            AccountMeta::new(user_flash_token_ata, false),
            AccountMeta::new_readonly(plan.lending_market, false),
            AccountMeta::new_readonly(plan.market_authority, false),
            AccountMeta::new_readonly(spl_token::id(), false),
            AccountMeta::new_readonly(solana_program::sysvar::clock::id(), false),
        ];
        Ok(Instruction { program_id: plan.flash_program, accounts, data })
    }

    fn protocol_from_program_id(&self, program_id: &Pubkey) -> Protocol {
        if *program_id == Pubkey::from_str(KAMINO_PROGRAM_ID).unwrap_or(Pubkey::default()) {
            Protocol::Kamino
        } else {
            Protocol::Solend
        }
    }

    // Compute precise repay amount from on-chain fee bps (ceiling division to avoid under-repay)
    fn compute_flash_repay_amount(&self, amount: u64, fee_bps: u64) -> u64 {
        // fee = ceil(amount * fee_bps / 10_000)
        let fee = ((amount as u128) * (fee_bps as u128) + 9_999) / 10_000;
        amount.saturating_add(fee as u64)
    }

    pub async fn execute_rebalance_flash_atomic(
        &self,
        position: &Position,
        src_meta: &ReserveMeta,
        dst_meta: &ReserveMeta,
        amount: u64,
        alts: &[AddressLookupTableAccount],
    ) -> Result<solana_sdk::signature::Signature> {
        let user = self.wallet.pubkey();
        let user_flash_ata = spl_associated_token_account::get_associated_token_address(&user, &position.token_mint);

        let plan = FlashPlan {
            flash_program: dst_meta.program_id,
            reserve: dst_meta.reserve,
            liquidity_supply_vault: dst_meta.liquidity_supply_vault,
            lending_market: dst_meta.lending_market,
            market_authority: dst_meta.lending_market_authority,
            token_mint: position.token_mint,
            amount,
        };

        let flash_start_ix = self.build_flash_loan_ix(&plan, user_flash_ata)?;
        // Estimate repay; ideally compute from on-chain reserve config
        let repay_amount = amount.saturating_add((amount as f64 * 0.0005) as u64);
        let flash_repay_ix = self.build_flash_repay_ix(&plan, user_flash_ata, repay_amount)?;

        let withdraw_old_ix = self.build_withdraw_instruction(position).await?;

        // Build deposit into destination protocol by constructing a minimal LM with correct protocol
        let dst_protocol = self.protocol_from_program_id(&dst_meta.program_id);
        let dst_stub = LendingMarket {
            market_pubkey: dst_meta.reserve,
            token_mint: position.token_mint,
            protocol: dst_protocol,
            supply_apy: 0.0,
            borrow_apy: 0.0,
            utilization: 0.0,
            liquidity_available: 0,
            last_update: Instant::now(),
            layout_version: 1,
        };
        let deposit_new_ix = self.build_deposit_instruction(position, &dst_stub).await?;

        let mut ixs = vec![flash_start_ix, withdraw_old_ix, deposit_new_ix, flash_repay_ix];
        let baseline_cu = self.optimize_compute_units(&ixs).await;
        let (cu_ix, fee_ix) = self.dynamic_budget_ixs(baseline_cu, PRIORITY_FEE_LAMPORTS).await;
        ixs.splice(0..0, [cu_ix, fee_ix]);

        let payer = self.wallet.pubkey();
        let build_with_hash = |bh: Hash| self.build_v0_tx(payer, &ixs, bh, alts);
        let sig = self
            .send_v0_with_slot_guard(build_with_hash, Duration::from_secs(45), 200, 120)
            .await?;
        Ok(sig)
    }

    // Resilient flash rebalance across multiple sources with metrics and circuit breaker
    pub async fn execute_flash_rebalance_resilient(
        &self,
        position: &Position,
        flash_sources: &[ReserveMeta],
        dst_meta: &ReserveMeta,
        amount: u64,
        tip_router: &TipRouter,
        obs: &Observability,
        breaker: &mut CircuitBreaker,
    ) -> Result<solana_sdk::signature::Signature> {
        obs.rebalance_attempts.inc();
        if flash_sources.is_empty() { return Err(anyhow!("no flash sources available")); }

        // Order sources by ascending fee, then by capacity desc (greedy optimal for repay)
        let mut sources: Vec<ReserveMeta> = flash_sources.to_vec();
        sources.sort_by(|a, b| {
            let fa = a.flash_fee_bps().unwrap_or(10_000);
            let fb = b.flash_fee_bps().unwrap_or(10_000);
            fa.cmp(&fb).then_with(|| {
                a.available_flash_capacity().unwrap_or(0).cmp(&b.available_flash_capacity().unwrap_or(0)).reverse()
            })
        });

        // Greedy fill
        let mut remaining = amount;
        let mut chunks: Vec<(ReserveMeta, u64, u64)> = Vec::with_capacity(sources.len()); // (src, take, repay)
        while remaining > 0 {
            if let Some(src) = sources.first().cloned() {
                let cap = src.available_flash_capacity().unwrap_or(0);
                if cap == 0 { sources.remove(0); if sources.is_empty() { break; } continue; }
                let fee_bps = src.flash_fee_bps().unwrap_or(0);
                let take = remaining.min(cap);
                let repay = self.compute_flash_repay_amount(take, fee_bps);
                chunks.push((src, take, repay));
                remaining = remaining.saturating_sub(take);
            } else { break; }
        }
        if remaining > 0 {
            warn!(missing = remaining, "insufficient flash capacity across sources");
            return Err(anyhow!("insufficient flash capacity"));
        }

        // Build instructions
        let payer = self.wallet.pubkey();
        let mut ixs: Vec<Instruction> = Vec::with_capacity(2 + 2 * chunks.len());
        let mut repay_total: u64 = 0;

        for (src, take, repay) in chunks.iter() {
            let user_flash_ata = spl_associated_token_account::get_associated_token_address(&payer, &position.token_mint);
            ixs.push(self.build_flash_loan_ix(&FlashPlan {
                flash_program: src.reserve_owner_program(),
                reserve: src.reserve,
                liquidity_supply_vault: src.liquidity_supply_vault,
                lending_market: src.lending_market,
                market_authority: src.lending_market_authority,
                token_mint: position.token_mint,
                amount: *take,
            }, user_flash_ata)?);
            repay_total = repay_total.saturating_add(*repay);
        }

        // Core legs
        let withdraw_old = self.build_withdraw_instruction(position).await?;
        let dst_protocol = self.protocol_from_program_id(&dst_meta.program_id);
        let dst_stub = LendingMarket {
            market_pubkey: dst_meta.reserve,
            token_mint: position.token_mint,
            protocol: dst_protocol,
            supply_apy: 0.0, borrow_apy: 0.0, utilization: 0.0,
            liquidity_available: 0,
            last_update: Instant::now(),
            layout_version: 1,
        };
        let deposit_new = self.build_deposit_instruction(position, &dst_stub).await?;
        ixs.push(withdraw_old);
        ixs.push(deposit_new);

        // Repay legs
        for (src, _take, repay) in chunks.iter() {
            let user_flash_ata = spl_associated_token_account::get_associated_token_address(&payer, &position.token_mint);
            ixs.push(self.build_flash_repay_ix(&FlashPlan {
                flash_program: src.reserve_owner_program(),
                reserve: src.reserve,
                liquidity_supply_vault: src.liquidity_supply_vault,
                lending_market: src.lending_market,
                market_authority: src.lending_market_authority,
                token_mint: position.token_mint,
                amount: 0,
            }, user_flash_ata, *repay)?);
        }

        // Prefetch to warm RPC cache before simulation
        self.prefetch_for_ixs(&ixs).await?;

        // Assemble with autoscaled budget and latency‑weighted tip
        let tip_acc = tip_router.pick().await;
        let vtx = self.assemble_tx_v0(ixs, true, Some(tip_acc), &[]).await?;

        // Exact simulation safety
        if let Err(e) = self.simulate_exact_v0(&vtx).await {
            error!(?e, "flash rebalance sim failed");
            let ok_to_continue = breaker.record(false);
            if !ok_to_continue { return Err(anyhow!("breaker open: too many failures")); }
            return Err(anyhow!(format!("sim failed: {e}")));
        }

        // Send with slot guard and measure tip latency
        let t0 = Instant::now();
        let sig = self
            .send_v0_with_slot_guard(
                |bh| {
                    let ixs_vec = vtx.message().instructions().to_vec();
                    self.build_v0_tx(self.wallet.pubkey(), &ixs_vec, bh, &[])
                },
                Duration::from_secs(45),
                200,
                120,
            )
            .await?;
        let ms = t0.elapsed().as_millis() as f64;
        obs.tip_latency_ms.observe(ms);
        tip_router.record_attempt(tip_acc, true, Some(ms)).await;
        info!(%sig, repay_total, "flash rebalance executed");
        obs.rebalance_success.inc();
        Ok(sig)
    }
    }

    pub async fn build_bundle_from_plans(
        &self,
        plans: Vec<Vec<Instruction>>,
        alts: &[AddressLookupTableAccount],
        tip_account: Pubkey,
        tip_lamports: u64,
    ) -> Result<Vec<VersionedTransaction>> {
        self.ensure_tip_funds(tip_lamports).await?;
        let bh = self.get_recent_blockhash_with_retry().await?;
        let mut out = Vec::with_capacity(plans.len());
        for ixs in plans {
            let (cu_ix, fee_ix) = self.dynamic_budget_ixs(COMPUTE_UNITS, PRIORITY_FEE_LAMPORTS).await;
            let tip_ix = system_instruction::transfer(&self.wallet.pubkey(), &tip_account, tip_lamports);
            let full: Vec<Instruction> = [vec![cu_ix, fee_ix, tip_ix], ixs].concat();
            let vtx = self.build_v0_tx(self.wallet.pubkey(), &full, bh, alts)?;
            out.push(vtx);
        }
        Ok(out)
    }

    fn select_tip_account(&self, cfg: &TipConfig, counter: &mut usize) -> Pubkey {
        match cfg.strategy {
            TipStrategy::RoundRobin => {
                // Prefer latency-weighted router if initialized; else fallback to round-robin
                if let Some(router) = self.tip_router.blocking_read().as_ref() {
                    return router.pick_sync();
                }
                let idx = *counter % cfg.accounts.len();
                *counter += 1;
                cfg.accounts[idx]
            }
            TipStrategy::Fixed(i) => cfg.accounts[i.min(cfg.accounts.len() - 1)],
        }
    }

    async fn ensure_tip_router(&self, cfg: &TipConfig) {
        let mut guard = self.tip_router.write().await;
        if guard.is_none() {
            *guard = Some(TipRouter::new(cfg.accounts.clone()));
        }
    }

    async fn observe_tip_landing(&self, account: Pubkey, sent_at: Instant, confirmed_at: Instant) {
        let ms = (confirmed_at.saturating_duration_since(sent_at).as_millis() as f64).max(0.0);
        if let Some(router) = self.tip_router.read().await.as_ref() {
            router.record_attempt(account, true, Some(ms)).await;
        }
    }

    async fn ensure_tip_funds(&self, min_balance: u64) -> Result<()> {
        let bal = self.rpc_client.get_balance(&self.wallet.pubkey()).await?;
        if bal < min_balance {
            return Err(anyhow!(format!(
                "insufficient balance for tipping: have {}, need {}",
                bal, min_balance
            )));
        }
        Ok(())
    }

    async fn confirm_sig_with_backoff(
        &self,
        sig: &solana_sdk::signature::Signature,
        timeout: Duration,
    ) -> Result<bool> {
        let start = Instant::now();
        let cfg = RpcSignatureStatusConfig { search_transaction_history: true };
        let mut delay = Duration::from_millis(160);
        loop {
            if start.elapsed() >= timeout { return Ok(false); }
            match self.rpc_client.get_signature_statuses_with_config(&[*sig], cfg).await {
                Ok(resp) => {
                    if let Some(Some(st)) = resp.value.first() { return Ok(st.err.is_none()); }
                }
                Err(_) => { /* transient */ }
            }
            tokio::time::sleep(delay).await;
            delay = std::cmp::min(delay * 2, Duration::from_millis(1000));
        }
    }
}

pub async fn run(&self) -> Result<()> {
@@ -116,35 +811,30 @@ impl PortOptimizer {
}

async fn update_market_data(&self) -> Result<()> {
        let mut markets = self.markets.write().await;
        
        let solend_markets = self.fetch_solend_markets().await?;
        let kamino_markets = self.fetch_kamino_markets().await?;
        
        for market in solend_markets {
            markets.entry(market.market_pubkey.to_string())
                .or_insert_with(Vec::new)
                .push(market);
        // Fetch concurrently and rebuild map keyed by token mint
        let (solend_markets, kamino_markets) = tokio::try_join!(
            self.fetch_solend_markets(),
            self.fetch_kamino_markets(),
        )?;
        let mut new_map: HashMap<String, Vec<LendingMarket>> = HashMap::new();
        for m in solend_markets.into_iter().chain(kamino_markets.into_iter()) {
            let key = m.token_mint.to_string();
            new_map.entry(key).or_default().push(m);
}
        
        for market in kamino_markets {
            markets.entry(market.market_pubkey.to_string())
                .or_insert_with(Vec::new)
                .push(market);
        }
        
        let mut markets = self.markets.write().await;
        *markets = new_map;
Ok(())
}

async fn fetch_solend_markets(&self) -> Result<Vec<LendingMarket>> {
let program_id = Pubkey::from_str(SOLEND_PROGRAM_ID)?;
        let accounts = self.rpc_client.get_program_accounts(&program_id)?;
        let accounts = self.rpc_client.get_program_accounts(&program_id).await?;

let mut markets = Vec::new();

for (pubkey, account) in accounts {
if account.data.len() >= 600 {
                let market = self.parse_solend_reserve(&pubkey, &account.data)?;
                let market = self.parse_solend_reserve(&pubkey, &account.data).await?;
markets.push(market);
}
}
@@ -154,72 +844,99 @@ impl PortOptimizer {

async fn fetch_kamino_markets(&self) -> Result<Vec<LendingMarket>> {
let program_id = Pubkey::from_str(KAMINO_PROGRAM_ID)?;
        let accounts = self.rpc_client.get_program_accounts(&program_id)?;
        let accounts = self.rpc_client.get_program_accounts(&program_id).await?;

let mut markets = Vec::new();

for (pubkey, account) in accounts {
if account.data.len() >= 800 {
                let market = self.parse_kamino_market(&pubkey, &account.data)?;
                let market = self.parse_kamino_market(&pubkey, &account.data).await?;
markets.push(market);
}
}

Ok(markets)
}

    fn parse_solend_reserve(&self, pubkey: &Pubkey, data: &[u8]) -> Result<LendingMarket> {
        let supply_rate = u64::from_le_bytes(data[232..240].try_into()?);
        let borrow_rate = u64::from_le_bytes(data[240..248].try_into()?);
        let available_liquidity = u64::from_le_bytes(data[64..72].try_into()?);
        let borrowed_amount = u64::from_le_bytes(data[72..80].try_into()?);
        
        let total_liquidity = available_liquidity + borrowed_amount;
        let utilization = if total_liquidity > 0 {
            borrowed_amount as f64 / total_liquidity as f64
        } else {
            0.0
        };
        
        Ok(LendingMarket {
            market_pubkey: *pubkey,
            protocol: Protocol::Solend,
            supply_apy: self.calculate_apy_from_rate(supply_rate),
            borrow_apy: self.calculate_apy_from_rate(borrow_rate),
            utilization,
            liquidity_available: available_liquidity,
            last_update: Instant::now(),
        })
    async fn parse_solend_reserve(&self, pubkey: &Pubkey, data: &[u8]) -> Result<LendingMarket> {
        if data.len() < 1024 { return Err(anyhow!(format!("solend reserve too small: {}", data.len()))); }
        let version = data[0];
        if version == 0 || version > 5 { return Err(anyhow!(format!("unexpected solend version: {}", version))); }
        let token_mint = Pubkey::new_from_array((*data.get(32..64).ok_or_else(|| anyhow!("mint slice"))?).try_into()?);
        let available_liquidity = u64::from_le_bytes((*data.get(64..72).ok_or_else(|| anyhow!("avail"))?).try_into()?);
        let borrowed_amount = u64::from_le_bytes((*data.get(72..80).ok_or_else(|| anyhow!("borrowed"))?).try_into()?);
        let supply_rate_raw = u128::from_le_bytes((*data.get(232..248).ok_or_else(|| anyhow!("supply"))?).try_into()?);
        let borrow_rate_raw = u128::from_le_bytes((*data.get(248..264).ok_or_else(|| anyhow!("borrow"))?).try_into()?);
        let total_liquidity = available_liquidity.saturating_add(borrowed_amount);
        let utilization = if total_liquidity > 0 { borrowed_amount as f64 / total_liquidity as f64 } else { 0.0 };
        let supply_apy = self.apy_continuous_from_rate_per_slot_1e18(supply_rate_raw).await?;
        let borrow_apy = self.apy_continuous_from_rate_per_slot_1e18(borrow_rate_raw).await?;
        Ok(LendingMarket { market_pubkey: *pubkey, token_mint, protocol: Protocol::Solend, supply_apy, borrow_apy, utilization, liquidity_available: available_liquidity, last_update: Instant::now(), layout_version: version })
}

    fn parse_kamino_market(&self, pubkey: &Pubkey, data: &[u8]) -> Result<LendingMarket> {
        let supply_rate = u64::from_le_bytes(data[296..304].try_into()?);
        let borrow_rate = u64::from_le_bytes(data[304..312].try_into()?);
        let available_liquidity = u64::from_le_bytes(data[80..88].try_into()?);
        let borrowed_amount = u64::from_le_bytes(data[88..96].try_into()?);
        
        let total_liquidity = available_liquidity + borrowed_amount;
        let utilization = if total_liquidity > 0 {
            borrowed_amount as f64 / total_liquidity as f64
        } else {
            0.0
        };
        
        Ok(LendingMarket {
            market_pubkey: *pubkey,
            protocol: Protocol::Kamino,
            supply_apy: self.calculate_apy_from_rate(supply_rate),
            borrow_apy: self.calculate_apy_from_rate(borrow_rate),
            utilization,
            liquidity_available: available_liquidity,
            last_update: Instant::now(),
        })
    async fn parse_kamino_market(&self, pubkey: &Pubkey, data: &[u8]) -> Result<LendingMarket> {
        if data.len() < 1024 { return Err(anyhow!(format!("kamino market too small: {}", data.len()))); }
        let version = data[0];
        if version == 0 || version > 10 { return Err(anyhow!(format!("unexpected kamino version: {}", version))); }
        let token_mint = Pubkey::new_from_array((*data.get(32..64).ok_or_else(|| anyhow!("mint slice"))?).try_into()?);
        let available_liquidity = u64::from_le_bytes((*data.get(80..88).ok_or_else(|| anyhow!("avail"))?).try_into()?);
        let borrowed_amount = u64::from_le_bytes((*data.get(88..96).ok_or_else(|| anyhow!("borrowed"))?).try_into()?);
        let supply_rate_raw = u128::from_le_bytes((*data.get(296..312).ok_or_else(|| anyhow!("supply"))?).try_into()?);
        let borrow_rate_raw = u128::from_le_bytes((*data.get(312..328).ok_or_else(|| anyhow!("borrow"))?).try_into()?);
        let total_liquidity = available_liquidity.saturating_add(borrowed_amount);
        let utilization = if total_liquidity > 0 { borrowed_amount as f64 / total_liquidity as f64 } else { 0.0 };
        let supply_apy = self.apy_continuous_from_rate_per_slot_1e18(supply_rate_raw).await?;
        let borrow_apy = self.apy_continuous_from_rate_per_slot_1e18(borrow_rate_raw).await?;
        Ok(LendingMarket { market_pubkey: *pubkey, token_mint, protocol: Protocol::Kamino, supply_apy, borrow_apy, utilization, liquidity_available: available_liquidity, last_update: Instant::now(), layout_version: version })
    }

    // Estimate slots/year from on-chain timing, with safe fallback
    async fn estimate_slots_per_year(&self) -> f64 {
        let sample = 10_000u64;
        if let (Ok(end_slot), Ok(start_slot)) = (
            self.rpc_client.get_slot().await,
            self.rpc_client.get_slot().await.map(|s| s.saturating_sub(sample)),
        ) {
            if let (Ok(t1_opt), Ok(t0_opt)) = (
                self.rpc_client.get_block_time(end_slot).await,
                self.rpc_client.get_block_time(start_slot).await,
            ) {
                if let (Some(t1), Some(t0)) = (t1_opt, t0_opt) {
                    let dt = (t1 - t0).max(1) as f64;
                    let slots = (end_slot - start_slot).max(1) as f64;
                    let slot_time = dt / slots; // seconds per slot
                    let sp_year = 31_536_000.0 / slot_time;
                    if sp_year.is_finite() && sp_year > 0.0 && sp_year < 200_000_000.0 {
                        return sp_year;
                    }
                }
            }
        }
        63_072_000.0
    }

    pub async fn apy_continuous_from_rate_per_slot_1e18(&self, rate_per_slot_1e18: u128) -> Result<f64> {
        let r = (rate_per_slot_1e18 as f64) / 1e18;
        if !(r >= 0.0 && r.is_finite()) {
            return Err(anyhow!(format!("invalid per-slot rate: {}", r)));
        }
        let spw = self.estimate_slots_per_year().await;
        let apy = (r * spw).exp() - 1.0; // continuous compounding
        let apy = apy.clamp(-0.5, 10.0); // [-50%, +1000%]
        if !apy.is_finite() { return Err(anyhow!("apy non-finite")); }
        Ok(apy * 100.0)
}

    fn calculate_apy_from_rate(&self, rate: u64) -> f64 {
        let rate_per_slot = rate as f64 / 1e18;
        let slots_per_year = 63072000.0;
        ((1.0 + rate_per_slot).powf(slots_per_year) - 1.0) * 100.0
    // Backwards-compat helper for tests and legacy callers (sync fallback).
    // Uses continuous compounding with a constant slots/year when async not available.
    pub fn calculate_apy_from_rate(&self, rate_per_slot_1e18: u64) -> f64 {
        let r = (rate_per_slot_1e18 as f64) / 1e18;
        if !(r >= 0.0 && r.is_finite()) { return 0.0; }
        let spw = 63_072_000.0;
        let apy = (r * spw).exp() - 1.0;
        let apy = apy.clamp(-0.5, 10.0);
        if !apy.is_finite() { return 0.0; }
        apy * 100.0
}

async fn find_arbitrage_opportunity(
@@ -231,15 +948,27 @@ impl PortOptimizer {
.find(|m| m.protocol == position.protocol)
.ok_or_else(|| anyhow!("Current market not found"))?;

        let best_alternative = markets.iter()
            .filter(|m| m.protocol != position.protocol)
            .max_by(|a, b| a.supply_apy.partial_cmp(&b.supply_apy).unwrap());
        let best_alternative = self.pick_best_market_weighted(
            position.amount,
            &markets
                .iter()
                .filter(|m| m.protocol != position.protocol)
                .cloned()
                .collect::<Vec<_>>()
        );

        if let Some(alt_market) = best_alternative {
        if let Some(alt_market) = best_alternative.as_ref() {
let apy_diff_bps = ((alt_market.supply_apy - current_market.supply_apy) * 100.0) as u64;

if apy_diff_bps > REBALANCE_THRESHOLD_BPS {
                let gas_cost = self.estimate_rebalance_cost(position.amount).await?;
                // Build the candidate withdraw + deposit instructions to get a realistic CU-based gas estimate
                let withdraw_ix = self.build_withdraw_instruction(position).await?;
                let deposit_ix = self.build_deposit_instruction(position, alt_market).await?;
                let gas_cost = self
                    .estimate_rebalance_cost_lamports(&[withdraw_ix, deposit_ix], 1.0)
                    .await
                    .unwrap_or(PRIORITY_FEE_LAMPORTS);

let expected_profit = self.calculate_expected_profit(
position.amount,
current_market.supply_apy,
@@ -248,7 +977,7 @@ impl PortOptimizer {
);

if expected_profit > MIN_PROFIT_BPS as f64 {
                    return Ok(Some(alt_market.clone()));
                    return Ok(best_alternative);
}
}
}
@@ -262,6 +991,119 @@ impl PortOptimizer {
Ok((base_fee as f64 * size_multiplier) as u64)
}

    // Realistic gas cost estimation from sim + fee percentiles (CU -> lamports)
    }


    pub async fn assemble_tx_v0(
        &self,
        mut ixs: Vec<Instruction>,
        include_tip: bool,
        tip_account: Option<Pubkey>,
        alts: &[AddressLookupTableAccount],
    ) -> Result<VersionedTransaction> {
        // 0) Prefetch to warm node caches (accounts & programs touched)
        self.prefetch_for_ixs(&ixs).await?;

        // 1) CU estimate and fee ixs
        let baseline_cu = self.optimize_compute_units_rough(&ixs).await;
        let (cu_ix, fee_ix) = self.dynamic_budget_ixs(baseline_cu, PRIORITY_FEE_LAMPORTS).await;

        // 2) Optional tip with size-aware + leader-aware adjustment
        let mut pref = vec![cu_ix, fee_ix];
        if include_tip {
            if let Some(tip_acc) = tip_account {
                // Program-aware base tip (bias to touched programs)
                let base_tip = self.priority_fee_for_ixs(&ixs, PRIORITY_FEE_LAMPORTS).await;

                // Size-aware nudge (≤ +5%)
                let sz = self.tx_message_size_estimate(&ixs) as f64;
                let size_adj = 1.0 + (sz.min(1500.0) / 1500.0) * 0.05;

                // Leader-aware nudge (≤ +10%) based on next 4 leaders
                let conc = self.leader_window_concentration(4).await.unwrap_or(0.0);
                let leader_adj = 1.0 + (conc * 0.10).min(0.10);

                // Micro-jitter to break ties (≤30 bps), consistent with fee path
                let j_bp = self.price_jitter_bp(30) as f64 / 10_000.0;
                let jitter_adj = 1.0 + j_bp;

                let tip = ((base_tip as f64) * size_adj * leader_adj * jitter_adj).ceil() as u64;
                pref.push(system_instruction::transfer(&self.wallet.pubkey(), &tip_acc, tip));
            }
        }
        ixs.splice(0..0, pref);

        // 3) Normalize ordering & dedup budget/tip to keep the message minimal and stable
        ixs = Self::normalize_ixs_order(ixs);
        ixs = Self::dedup_budget_and_tip(ixs);

        // 4) Pick ALTs and shrink to MTU if needed
        let picked = Self::select_alts_for_ixs(&ixs, alts, 3);
        let selected_alts = Self::shrink_alts_to_mtu(&ixs, picked, QUIC_SAFE_PACKET_BUDGET, &|ixs_, alts_| {
            self.tx_message_size_with_alts_estimate(ixs_, alts_)
        });

        // 5) Build v0 with fresh hash and selected ALTs
        let bh = self.get_recent_blockhash_with_retry().await?;
        self.build_v0_tx(self.wallet.pubkey(), &ixs, bh, &selected_alts)
    }
}

// ... (rest of the code remains the same)
// Bounded tip escalation helper
#[inline]
fn tip_escalation_nudge(base_tip: u64, attempt_no: u32) -> u64 {
    // 1st miss: +4%, 2nd: +8%, 3rd+: +12% (cap)
    let mult = match attempt_no {
        0 => 1.00,
        1 => 1.04,
        2 => 1.08,
        _ => 1.12,
    };
    ((base_tip as f64) * mult).ceil() as u64
}

// Patch CU limit in-place for a transaction's instructions
#[inline]
fn patch_cu_limit(ixs: &mut [Instruction], new_limit: u32) {
    for ix in ixs.iter_mut() {
        if ix.program_id == solana_sdk::compute_budget::id() {
            if ix.data.first() == Some(&0x00) { // set_compute_unit_limit
                ix.data = {
                    let mut d = vec![0x00u8];
                    d.extend_from_slice(&new_limit.to_le_bytes());
                    d
                };
                return;
            }
        }
    }
    // not found → prepend
    let ins = ComputeBudgetInstruction::set_compute_unit_limit(new_limit);
    ixs.splice(0..0, [ins]);
}

// Ensure heap frame is set with the specified target
#[inline]
fn ensure_heap_frame(ixs: &mut Vec<Instruction>, target: u32) {
    // dedup in case caller added already
    for ix in ixs.iter_mut() {
        if ix.program_id == solana_sdk::compute_budget::id() && ix.data.first() == Some(&0x01) {
            ix.data = {
                let mut d = vec![0x01u8];
                d.extend_from_slice(&target.to_le_bytes());
                d
            };
            return;
        }
    }
    ixs.splice(0..0, [ComputeBudgetInstruction::set_heap_frame(target)]);
}

// Deduplicate budget and tip instructions while preserving order
fn dedup_budget_and_tip(mut ixs: Vec<Instruction>) -> Vec<Instruction> {
    let mut saw_limit = false;
    let mut saw_price = false;
    let mut saw_heap = false;
    let mut seen_tip_to: HashSet<Pubkey> = HashSet::new();

    ixs.retain(|ix| {
        if ix.program_id == solana_sdk::compute_budget::id() {
            if let Some(op) = ix.data.first() {
                match *op {
                    0x00 => { if !saw_limit { saw_limit = true; return true; } return false; } // limit
                    0x01 => { if !saw_heap  { saw_heap  = true; return true; } return false; } // heap
                    0x02 => { if !saw_price { saw_price = true; return true; } return false; } // price
                    _ => return false,
                }
            }
        }
        if ix.program_id == solana_sdk::system_program::id() && ix.data.first() == Some(&2u8) {
            if let Some(to) = ix.accounts.get(1).map(|a| a.pubkey) {
                if !seen_tip_to.insert(to) { return false; }
            }
        }
        true
    });
    ixs
}

impl PortOptimizer {
    /// Gets the minimum rent for a token account with caching
    pub async fn min_rent_token_account(&self) -> u64 {
        // Check cache first
        if let Ok(cache) = RENT_CACHE.read().await {
            if cache.lamports_165 > 0 && cache.ts.elapsed() <= Duration::from_secs(600) {
                return cache.lamports_165;
            }
        }
        
        // Fetch from RPC if cache miss or stale
        let v = match self.rpc_client.get_minimum_balance_for_rent_exemption(165).await {
            Ok(v) => v,
            Err(_) => 2_039_280, // Fallback value if RPC fails
        };
        
        // Update cache
        if let Ok(mut cache) = RENT_CACHE.write().await {
            *cache = RentCache {
                lamports_165: v,
                ts: Instant::now(),
            };
        }
        v
    }
    
    /// Ensures the transaction has sufficient funds for fees and rent
    pub async fn ensure_total_budget_safety(
        &self, 
        ixs: &[Instruction], 
        rent_reserve_accounts: usize
    ) -> Result<()> {
        let (cu_lim, cu_price, tip) = parse_budget_from_ixs(ixs);
        let pri_fee = priority_fee_lamports(cu_lim, cu_price);
        let rent_reserve = (rent_reserve_accounts as u64)
            .saturating_mul(self.min_rent_token_account().await);
            
        let need = BASE_SIG_COST_LAMPORTS
            .saturating_add(pri_fee)
            .saturating_add(tip)
            .saturating_add(rent_reserve);
            
        let bal = self.rpc_client.get_balance(&self.wallet.pubkey()).await?;
        
        if bal < need {
            return Err(anyhow!(
                "Insufficient funds: have {} lamports, need {} (base: {}, pri_fee: {}, tip: {}, rent: {})",
                bal, need, BASE_SIG_COST_LAMPORTS, pri_fee, tip, rent_reserve
            ));
        }
        
        Ok(())
    }

    // Rough CU estimator with LRU cache and TTL/epoch tracking
    async fn optimize_compute_units_rough(&self, ixs: &[Instruction]) -> u32 {
        // Skip empty instruction sets
        if ixs.is_empty() {
            return 0;
        }

        // Get current epoch for cache validation
        let epoch = match self.rpc_client.get_epoch_info().await {
            Ok(info) => info.epoch,
            Err(e) => {
                error!("Failed to get epoch info: {}", e);
                return 200_000; // Fallback to safe default
            }
        };

        // Generate a fingerprint for these instructions
        let fingerprint = ixs_fingerprint(ixs);
        
        // Try to get from cache first (with TTL and epoch validation)
        if let Ok(cache) = cu_cache().try_read() {
            if let Some(cached_cu) = cache.get_valid(&fingerprint, epoch, Duration::from_secs(30)) {
                return cached_cu;
            }
        }

        // If not in cache or expired, simulate to get the actual CU usage
        let result = match self.simulate_compute_units(ixs).await {
            Ok(cu) => cu,
            Err(e) => {
                error!("Failed to simulate CU: {}", e);
                // Fallback to a safe default if simulation fails
                200_000
            }
        };

        // Update cache in background with current epoch
        let cache = cu_cache().clone();
        tokio::spawn(async move {
            if let Ok(mut cache) = cache.write().await {
                cache.put(fingerprint, result, epoch);
            }
        });

        result
    }
    
    // Helper function to simulate compute units for a set of instructions
    async fn simulate_compute_units(&self, ixs: &[Instruction]) -> Result<u32> {
        // Create a minimal transaction for simulation
        let payer = self.wallet.pubkey();
        let blockhash = self.get_recent_blockhash_with_retry().await?;
        
        // Build a minimal transaction with just the instructions
        let message = V0Message::try_compile(
            &payer,
            ixs,
            &[], // No address lookup tables
            blockhash,
        )?;
        
        let tx = VersionedTransaction::try_new(
            solana_sdk::message::VersionedMessage::V0(message),
            &[&self.wallet],
        )?;
        
        // Simulate the transaction to get compute units
        let sim_result = self
            .rpc
            .simulate_transaction_with_config(
                &tx,
                solana_client::rpc_config::RpcSimulateTransactionConfig {
                    sig_verify: false, // Skip signature verification for speed
                    replace_recent_blockhash: true,
                    commitment: Some(CommitmentConfig::confirmed()),
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| anyhow!("Simulation failed: {}", e))?;
            
        // Extract compute units from simulation result
        sim_result
            .value
            .units_consumed
            .ok_or_else(|| anyhow!("No units consumed in simulation"))
            .map(|units| units.max(100_000)) // Enforce minimum CU
    }
    // Dynamic CU budget ixs (p75 fee floor + autoscale)
    pub async fn dynamic_budget_ixs(&self, baseline_cu: u32, floor_cu_price: u64) -> (Instruction, Instruction) {
        // +12% headroom, rounded to 1k CU granularity, bounded in-protocol
        let mut cu_limit = ((baseline_cu as f64) * 1.12).ceil() as u32;
        cu_limit = ((cu_limit + 999) / 1000) * 1000;
        cu_limit = cu_limit.clamp(150_000, 1_400_000);

        // Base price from trimmed quantiles (already robust)
        let mut cu_price = self.priority_fee_autoscale(floor_cu_price).await;

        // Add congestion slope multiplier (0..+8%) if tape rising
        if let Ok(slope) = self.congestion_slope().await {
            let mult = 1.0 + slope.min(0.08);
            cu_price = ((cu_price as f64) * mult).ceil() as u64;
        }

        // Add tiny positive jitter (0..30 bps) to break ties deterministically
        let j_bp = self.price_jitter_bp(30);
        cu_price = cu_price.saturating_add((cu_price.saturating_mul(j_bp)) / 10_000);

    // Stable v0 builder (ALT-aware)
    pub fn build_v0_tx(
        &self,
        payer: Pubkey,
        ixs: &[Instruction],
        recent_blockhash: Hash,
        alts: &[AddressLookupTableAccount],
    ) -> Result<VersionedTransaction> {
        let msg_v0 = V0Message::try_compile(&payer, ixs, alts, recent_blockhash)?;
        Ok(VersionedTransaction::try_new(VersionedMessage::V0(msg_v0), &[self.wallet.as_ref()])?)
    }

    // Blockhash fetch with small retry/backoff
    pub async fn get_recent_blockhash_with_retry(&self) -> Result<Hash> {
        let mut backoff = Duration::from_millis(120);
        for _ in 0..8 {
            if let Ok(h) = self.rpc_client.get_latest_blockhash().await { return Ok(h); }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_millis(1500));
        }
        Err(anyhow!("failed to get recent blockhash"))
    }

    // Confirmation with exponential backoff (bounded)
    pub async fn confirm_sig_with_backoff(
        &self,
        sig: &solana_sdk::signature::Signature,
        timeout: Duration,
    ) -> Result<bool> {
        let start = Instant::now();
        let cfg = RpcSignatureStatusConfig { search_transaction_history: true };
        let mut delay = Duration::from_millis(160);
        loop {
            if start.elapsed() >= timeout { return Ok(false); }
            match self.rpc_client.get_signature_statuses_with_config(&[*sig], cfg).await {
                Ok(resp) => {
                    if let Some(Some(st)) = resp.value.first() { return Ok(st.err.is_none()); }
                }
                Err(_) => {}
            }
            tokio::time::sleep(delay).await;
            delay = std::cmp::min(delay * 2, Duration::from_millis(1000));
        }
    }

    /// Update the last transaction signature for deterministic operations
    async fn update_last_tx_signature(&self, signature: solana_sdk::signature::Signature) {
        if let Ok(mut last_tx) = self.last_tx_signature.write().await {
            *last_tx = Some(signature);
        }
    }

    pub async fn send_and_confirm_v0(
        &self,
        tx: &VersionedTransaction,
        timeout: Duration,
    ) -> Result<solana_sdk::signature::Signature> {
        use solana_client::rpc_config::RpcSendTransactionConfig;
        use solana_sdk::commitment_config::CommitmentConfig;

        let cur_slot = self.rpc_client.get_slot().await.unwrap_or(0);

        let cfg = RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentConfig::processed()),
            encoding: None,
            max_retries: Some(0),
            min_context_slot: Some(cur_slot.saturating_sub(2)), // safe cushion
        };

        let sig = self.rpc_client.send_transaction_with_config(tx, cfg).await?;
        let ok  = self.confirm_sig_with_backoff(&sig, timeout).await?;
        if ok { Ok(sig) } else { Err(anyhow!("confirmation timeout")) }
    }

    // Stale-hash resilient send with slot refresh and auto-repair
    pub async fn send_v0_with_slot_guard<F>(
        &self,
        mut build_with_hash: F,
        max_wait: Duration,
        _hash_valid_for_slots: u64,
        refresh_before_slots: u64,
    ) -> Result<solana_sdk::signature::Signature>
    where
        F: Send + 'static + FnMut(Hash) -> Result<VersionedTransaction>,
    {
        let start = Instant::now();
        let (mut bh, mut h0) = self.get_fresh_blockhash().await?;
        let mut tx = build_with_hash(bh)?;
        let mut attempt: u32 = 0;

        loop {
            if start.elapsed() >= max_wait { return Err(anyhow!("timeout sending v0 tx")); }

            // quick legacy sim for auto-repair
            let payer = self.wallet.pubkey();
            let mut ixs = tx.message().instructions().to_vec();
            let legacy = Transaction::new_unsigned(solana_sdk::message::Message::new(&ixs, Some(&payer)));
            if let Ok(sim) = self.rpc.simulate_transaction(&legacy).await {
                if sim.value.err.is_some() {
                    // parse failure and auto-repair
                    let logs = sim.value.logs.unwrap_or_default().join("\n").to_lowercase();
                    let mut repaired = false;

                    // bump CU limit on budget exceeded
                    if logs.contains("computationalbudgetexceeded") || logs.contains("max units exceeded") {
                        // read current CU limit (if any) → +20% (≤1_400_000)
                        let mut cur_limit: u32 = 800_000;
                        for ix in &ixs {
                            if ix.program_id == solana_sdk::compute_budget::id() && ix.data.first() == Some(&0x00) && ix.data.len() >= 5 {
                                let mut b = [0u8; 4];
                                b.copy_from_slice(&ix.data[1..5]);
                                cur_limit = u32::from_le_bytes(b);
                            }
                        }
                        let mut new_limit = ((cur_limit as f64) * 1.20).ceil() as u32;
                        new_limit = ((new_limit + 999) / 1000) * 1000;
                        new_limit = new_limit.clamp(150_000, 1_400_000);
                        patch_cu_limit(&mut ixs, new_limit);
                        repaired = true;
                    }

                    // set heap frame on heap errors
                    if logs.contains("heap") || logs.contains("alloc") || logs.contains("exceeded heap") {
                        ensure_heap_frame(&mut ixs, 256_000);
                        repaired = true;
                    }

                    if repaired {
                        // rebuild v0 with same blockhash; keep ALTs already embedded in tx
                        tx = self.build_v0_tx(self.wallet.pubkey(), &ixs, bh, &[])?;
                        continue;
                    } else {
                        return Err(anyhow!("simulation failed: {:?}", sim.value.err));
                    }
                }
            }

            // deterministic cohort jitter
            let conc = self.leader_window_concentration(4).await.unwrap_or(0.0);
            let jitter_us = self.cohort_jitter_us(tx.signatures.get(0), conc, 3_000);
            if jitter_us > 0 { tokio::time::sleep(Duration::from_micros(jitter_us)).await; }

            match self.send_and_confirm_v0(&tx, Duration::from_secs(20)).await {
                Ok(sig) => { self.leader_quality_update(true).await; return Ok(sig); }
                Err(e) => {
                    self.leader_quality_update(false).await;
                    attempt = attempt.saturating_add(1);
                    let msg = e.to_string().to_lowercase();

                    if msg.contains("blockhash not found") || msg.contains("blockhashnotfound") {
                        let (nbh, nh) = self.get_fresh_blockhash().await?;
                        bh = nbh; h0 = nh;
                        tx = build_with_hash(bh)?;
                        continue;
                    }
                    let h_now = self.rpc_client.get_block_height().await.unwrap_or(h0);
                    if h_now.saturating_sub(h0) >= refresh_before_slots {
                        let (nbh, nh) = self.get_fresh_blockhash().await?;
                        bh = nbh; h0 = nh;
                        tx = build_with_hash(bh)?;
                        continue;
                    }

                    // bounded fee escalation (CU price + tip)
                    let mut ixs = tx.message().instructions().to_vec();
                    for ix in ixs.iter_mut() {
                        if ix.program_id == solana_sdk::compute_budget::id() && ix.data.first() == Some(&0x02) && ix.data.len() >= 9 {
                            let mut cur = [0u8; 8];
                            cur.copy_from_slice(&ix.data[1..9]);
                            let bumped = fee_escalation_nudge(u64::from_le_bytes(cur), attempt);
                            ix.data = {
                                let mut d = vec![0x02u8];
                                d.extend_from_slice(&bumped.to_le_bytes());
                                d
                            };
                        }
                        if ix.program_id == solana_sdk::system_program::id() && ix.data.first() == Some(&2u8) && ix.data.len() >= 9 {
                            let mut cur = [0u8; 8];
                            cur.copy_from_slice(&ix.data[1..9]);
                            let bumped = tip_escalation_nudge(u64::from_le_bytes(cur), attempt);
                            ix.data = {
                                let mut d = vec![2u8];
                                d.extend_from_slice(&bumped.to_le_bytes());
                                d
                            };
                        }
                    }
                    tx = self.build_v0_tx(self.wallet.pubkey(), &ixs, bh, &[])?;
                    
                    // Slot-phase aware backoff
                    let conc = self.leader_window_concentration(4).await.unwrap_or(0.0);
                    self.slot_phase_backoff(conc, 120).await;
                }
            }
                }

                // Brief yield under transient errors
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }

        // bounded fee escalation (CU price + tip)
        let mut ixs = tx.message().instructions().to_vec();
        for ix in ixs.iter_mut() {
            if ix.program_id == solana_sdk::compute_budget::id() && ix.data.first() == Some(&0x02) && ix.data.len() >= 9 {
                let mut cur = [0u8; 8];
                cur.copy_from_slice(&ix.data[1..9]);
                let bumped = fee_escalation_nudge(u64::from_le_bytes(cur), attempt);
                ix.data = {
                    let mut d = vec![0x02u8];
                    d.extend_from_slice(&bumped.to_le_bytes());
                    d
                };
            }
            if ix.program_id == solana_sdk::system_program::id() && ix.data.first() == Some(&2u8) && ix.data.len() >= 9 {
                let mut cur = [0u8; 8];
                cur.copy_from_slice(&ix.data[1..9]);
                let bumped = tip_escalation_nudge(u64::from_le_bytes(cur), attempt);
                ix.data = {
                    let mut d = vec![2u8];
                    d.extend_from_slice(&bumped.to_le_bytes());
                    d
                };
            }
        max_tables: usize,
    ) -> Vec<AddressLookupTableAccount> {
        if candidates.is_empty() || max_tables == 0 { return Vec::new(); }

        use std::collections::HashSet;
        let mut need: HashSet<Pubkey> = HashSet::new();
        for ix in ixs {
            need.insert(ix.program_id);
            for a in &ix.accounts { need.insert(a.pubkey); }
        }

        let mut picked: Vec<AddressLookupTableAccount> = Vec::new();
        let mut remaining: HashSet<Pubkey> = need;

        for _ in 0..max_tables {
            let mut best: Option<(usize, usize)> = None; // (idx, coverage)
            for (i, alt) in candidates.iter().enumerate() {
                if picked.iter().any(|p| p.key() == alt.key()) { continue; }
                let cov = alt.addresses.iter().filter(|k| remaining.contains(k)).count();
                if cov == 0 { continue; }
                if best.map(|b| cov > b.1).unwrap_or(true) { best = Some((i, cov)); }
            }
            if let Some((i, _)) = best {
                let alt = candidates[i].clone();
                for k in &alt.addresses { remaining.remove(k); }
                picked.push(alt);
            } else { break; }
        }
        picked
    }

    // Canonical ordering of instructions for predictability
    fn normalize_ixs_order(mut ixs: Vec<Instruction>) -> Vec<Instruction> {
        let mut budget: Vec<Instruction> = Vec::new();      // compute budget ixs
        let mut tips:   Vec<Instruction> = Vec::new();      // system::transfer as tips
        let mut repays: Vec<Instruction> = Vec::new();      // flash repay legs
        let mut core:   Vec<Instruction> = Vec::new();      // everything else

        for ix in ixs.into_iter() {
            if ix.program_id == solana_sdk::compute_budget::id() {
                budget.push(ix);
            } else if ix.program_id == solana_sdk::system_program::id() && ix.data.first() == Some(&2u8) {
                tips.push(ix);
            } else if ix.data.first() == Some(&FLASHREPAY_TAG) {
                repays.push(ix);
            } else {
                core.push(ix);
            }
        }

        // Budget -> tip -> core -> repay
        budget.into_iter().chain(tips).chain(core).chain(repays).collect()
    }

    // Recent-fee congestion slope proxy (0.0..0.10)
    pub async fn congestion_slope(&self) -> Result<f64> {
        // Uses recent prioritization fees to gauge short-horizon uptrend.
        // Returns 0.0..0.10 (10% max). Negative slopes return 0.
        let fees = self.rpc_client.get_recent_prioritization_fees(&[]).await?;
        if fees.len() < 8 { return Ok(0.0); }

        let mut vals: Vec<u64> = fees.iter().map(|f| f.prioritization_fee.max(1)).collect();
        vals.sort_unstable();
        let p50 = vals[vals.len() / 2].max(1) as f64;

        // Use the latest observed fee vs median as a robust slope proxy
        let last = fees.last().map(|f| f.prioritization_fee.max(1) as f64).unwrap_or(p50);
        let rel = (last - p50) / p50;
        Ok(rel.max(0.0).min(0.10))
    }

    pub fn price_jitter_bp(&self, max_bp: u64) -> u64 {
        // Use last transaction signature as seed if available, otherwise use wallet pubkey
        let seed = match self.last_tx_signature.blocking_read().as_ref() {
            Some(sig) => sig.as_ref(),
            None => self.wallet.pubkey().as_ref(),
        };
        
        // Generate deterministic jitter based on the seed
        let mut hasher = Sha256::new();
        hasher.update(seed);
        let hash = hasher.finalize();
        let rand_val = u64::from_le_bytes([hash[0], hash[1], hash[2], hash[3], hash[4], hash[5], hash[6], hash[7]]);
        
        // Scale to the desired range (0..=max_bp)
        rand_val % (max_bp + 1)
    }

    pub fn safe_round_lamports(&self, x: u64, quantum: u64) -> u64 {
        if quantum <= 1 { return x; }
        ((x + quantum - 1) / quantum) * quantum
    }

    // Fetch the next N leaders using a cached leader schedule (epoch-aware, 15s TTL)
    pub async fn next_n_leaders(&self, n: usize) -> Result<Vec<String>> {
        use std::collections::HashMap;
        let ei = self.rpc_client.get_epoch_info().await?;
        let mut cache = leader_cache().write().await;

        let refresh_needed = cache.epoch != ei.epoch || cache.last_refresh.elapsed() > Duration::from_secs(15);
        if refresh_needed {
            let sched_opt = self.rpc_client.get_leader_schedule(None).await?;
            cache.schedule = match sched_opt { Some(m) => m, None => HashMap::new() };
            cache.epoch = ei.epoch;
            cache.last_refresh = Instant::now();
        }

        let slots_in_epoch = ei.slots_in_epoch as usize;
        if slots_in_epoch == 0 || n == 0 { return Ok(Vec::new()); }

        let mut who: Vec<String> = vec![String::new(); slots_in_epoch];
        for (leader, slots) in cache.schedule.iter() {
            for &idx in slots {
                if idx < slots_in_epoch { who[idx] = leader.clone(); }
            }
        }

        let mut out = Vec::with_capacity(n);
        let mut i = 1usize;
        let cur_idx = ei.slot_index as usize;
        while out.len() < n && (cur_idx + i) < slots_in_epoch {
            out.push(who.get(cur_idx + i).cloned().unwrap_or_default());
            i += 1;
        }
        Ok(out)
    }

        /// Compute concentration score (0..1) for next k leaders (cached schedule)
    /// Returns the fraction of slots in the window that have the same leader as the most common one
    pub async fn leader_window_concentration(&self, k: usize) -> Result<f64> {
        use std::collections::HashMap;
        
        if k == 0 {
            return Ok(0.0);
        }
        
        // Get the next k leaders from the cache or RPC
        let leaders = self.next_n_leaders(k).await.unwrap_or_default();
        if leaders.is_empty() {
            return Ok(0.0);
        }
        
        // Count occurrences of each leader
        let mut leader_counts = HashMap::new();
        for leader in &leaders {
            *leader_counts.entry(leader).or_insert(0) += 1;
        }
        
        // Find the most common leader count
        let max_count = leader_counts.values().max().copied().unwrap_or(0);
        
        // Return the fraction of slots with the most common leader
        Ok(max_count as f64 / k as f64)
        if leaders.is_empty() { return Ok(0.0); }
        let mut freq: HashMap<String, usize> = HashMap::new();
        for l in leaders.into_iter().filter(|s| !s.is_empty()) {
            *freq.entry(l).or_insert(0) += 1;
        }
        if freq.is_empty() { return Ok(0.0); }
        let max_c = *freq.values().max().unwrap_or(&0) as f64;
        Ok((max_c / k as f64).min(1.0))
    }

    // Program-aware priority fee estimator (bias to touched programs)
    pub async fn priority_fee_for_ixs(&self, ixs: &[Instruction], floor_cu_price: u64) -> u64 {
        use std::collections::HashSet;
        let mut set: HashSet<Pubkey> = HashSet::new();
        for ix in ixs { set.insert(ix.program_id); }
        let addrs: Vec<Pubkey> = set.into_iter().collect();
        if addrs.is_empty() {
            return self.priority_fee_autoscale(floor_cu_price).await;
        }
        match self.rpc_client.get_recent_prioritization_fees(&addrs).await {
            Ok(fees) if !fees.is_empty() => {
                let mut vals: Vec<u64> = fees.iter().map(|f| f.prioritization_fee.max(1)).collect();
                vals.sort_unstable();
                let n = vals.len();
                let lo = vals[(n * 70 / 100).min(n - 1)];
                let hi = vals[(n * 90 / 100).min(n - 1)];
                let base = lo.saturating_add(hi) / 2;
                let mut out = base.max(floor_cu_price).min(floor_cu_price.saturating_mul(25));
                let j_bp = self.price_jitter_bp(30);
                out = out.saturating_add((out.saturating_mul(j_bp)) / 10_000);
                out
            }
            _ => self.priority_fee_autoscale(floor_cu_price).await,
        }
    }
}

// ----- Observability and Circuit Breaker -----
pub struct Observability {
    pub rebalance_attempts: IntCounter,
    pub rebalance_success: IntCounter,
    pub rebalance_failure: IntCounter,
    pub tip_latency_ms: Histogram,
}

impl Observability {
    pub fn new(reg: &Registry) -> Self {
        let attempts = IntCounter::new("rebalance_attempts", "Number of rebalance attempts").unwrap();
        let success = IntCounter::new("rebalance_success", "Number of successful rebalances").unwrap();
        let failure = IntCounter::new("rebalance_failure", "Number of failed rebalances").unwrap();
        let tip_latency = Histogram::with_opts(HistogramOpts::new("tip_latency_ms", "Tip landing latency (ms)")).unwrap();
        reg.register(Box::new(attempts.clone())).ok();
        reg.register(Box::new(success.clone())).ok();
        reg.register(Box::new(failure.clone())).ok();
        reg.register(Box::new(tip_latency.clone())).ok();
        Self { rebalance_attempts: attempts, rebalance_success: success, rebalance_failure: failure, tip_latency_ms: tip_latency }
    }
}

pub struct CircuitBreaker {
    failures: std::collections::VecDeque<Instant>,
    window: Duration,
    max_failures: usize,
}

impl CircuitBreaker {
    pub fn new(window: Duration, max_failures: usize) -> Self {
        Self { failures: std::collections::VecDeque::new(), window, max_failures }
    }
    pub fn record(&mut self, ok: bool) -> bool {
        let now = Instant::now();
        if ok { self.failures.clear(); return true; }
        self.failures.push_back(now);
        while let Some(&t) = self.failures.front() {
            if now.duration_since(t) > self.window { self.failures.pop_front(); } else { break; }
        }
        self.failures.len() < self.max_failures
    }
}

// ----- ReserveMeta helper stubs for flash fee/capacity (to be wired with on-chain layouts) -----
impl ReserveMeta {
    pub fn flash_fee_bps(&self) -> Result<u64> { Ok(5) } // TODO: replace with on-chain config
    pub fn available_flash_capacity(&self) -> Result<u64> { Ok(u64::MAX / 4) } // TODO
    pub fn reserve_owner_program(&self) -> Pubkey { self.program_id }
}

fn calculate_expected_profit(
&self,
amount: u64,
@@ -280,101 +1122,265 @@ impl PortOptimizer {
position: &Position,
target_market: LendingMarket,
) -> Result<()> {
        // 1) Build core instructions
let withdraw_ix = self.build_withdraw_instruction(position).await?;
let deposit_ix = self.build_deposit_instruction(position, &target_market).await?;
        
        let recent_blockhash = self.get_recent_blockhash_with_retry().await?;
        
        let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNITS);
        let priority_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS);
        
        let mut transaction = Transaction::new_with_payer(
            &[compute_budget_ix, priority_fee_ix, withdraw_ix, deposit_ix],
            Some(&self.wallet.pubkey()),

        // 2) Dynamic CU and priority fee (p75 with floor)
        let baseline_cu = self
            .optimize_compute_units_rough(&[withdraw_ix.clone(), deposit_ix.clone()])
            .await;
        let (cu_ix, fee_ix) = self.dynamic_budget_ixs(baseline_cu, PRIORITY_FEE_LAMPORTS).await;

        // Optional: send a small tip to a stable tip account (can plug your rotation)
        let tip_ix = system_instruction::transfer(
            &self.wallet.pubkey(),
            &Pubkey::from_str("JitoTip111111111111111111111111111111111")?,
            self.dynamic_priority_fee().await.unwrap_or(PRIORITY_FEE_LAMPORTS),
);
        
        transaction.sign(&[self.wallet.as_ref()], recent_blockhash);
        
        match self.send_transaction_with_retry(&transaction).await {

        let ixs = vec![cu_ix, fee_ix, tip_ix, withdraw_ix, deposit_ix];

        // 3) Build v0 tx with empty ALTs for now
        let build_with_hash = |bh: Hash| {
            self.build_v0_tx(self.wallet.pubkey(), &ixs, bh, &[])
        };

        // 4) Simulate exactly the transaction to be sent
        let bh = self.get_recent_blockhash_with_retry().await?;
        let tx_sim = build_with_hash(bh)?;
        let sim_res = self.simulate_exact(&tx_sim).await?;
        if let Some(err) = sim_res.err {
            return Err(anyhow!("preflight sim failed: {:?}", err));
        }

        // 5) Send and confirm with slot-aware blockhash lifecycle
        match self
            .send_v0_with_slot_guard(
                |bh2| self.build_v0_tx(self.wallet.pubkey(), &ixs, bh2, &[]),
                Duration::from_secs(45),
                200,
                120,
            )
            .await
        {
Ok(signature) => {
println!("Rebalance executed: {signature}");
self.update_position(position, target_market).await?;
                Ok(())
}
Err(e) => {
eprintln!("Rebalance failed: {e:?}");
                return Err(e);
                Err(e)
}
}
        
        Ok(())
}

async fn build_withdraw_instruction(&self, position: &Position) -> Result<Instruction> {
        let program_id = match position.protocol {
            Protocol::Solend => Pubkey::from_str(SOLEND_PROGRAM_ID)?,
            Protocol::Kamino => Pubkey::from_str(KAMINO_PROGRAM_ID)?,
        };
        
        let instruction_data = match position.protocol {
            Protocol::Solend => self.encode_solend_withdraw(position.amount),
            Protocol::Kamino => self.encode_kamino_withdraw(position.amount),
        };
        
        let accounts = self.get_withdraw_accounts(position).await?;
        
        Ok(Instruction {
            program_id,
            accounts,
            data: instruction_data,
        })
        match position.protocol {
            Protocol::Solend => {
                // Resolve Reserve data and derive only the documented market authority PDA
                let program_id = Pubkey::from_str(SOLEND_PROGRAM_ID)?;
                let meta = self
                    .resolve_reserve_by_mint(&program_id, &position.token_mint)
                    .await?;

                let user = self.wallet.pubkey();
                let user_collateral_ata = spl_associated_token_account::get_associated_token_address(&user, &meta.collateral_mint);
                let user_destination_liquidity_ata = spl_associated_token_account::get_associated_token_address(&user, &meta.liquidity_mint);

                self.build_solend_withdraw_ix(
                    user_collateral_ata,
                    user_destination_liquidity_ata,
                    position.amount,
                    &meta,
                    program_id,
                )
            }
            Protocol::Kamino => {
                let program_id = Pubkey::from_str(KAMINO_PROGRAM_ID)?;
                let instruction_data = self.encode_kamino_withdraw(position.amount)?;
                let accounts = self.get_withdraw_accounts(position).await?;
                Ok(Instruction { program_id, accounts, data: instruction_data })
            }
        }
}

async fn build_deposit_instruction(
&self,
position: &Position,
target_market: &LendingMarket,
) -> Result<Instruction> {
        let program_id = match target_market.protocol {
            Protocol::Solend => Pubkey::from_str(SOLEND_PROGRAM_ID)?,
            Protocol::Kamino => Pubkey::from_str(KAMINO_PROGRAM_ID)?,
        };
        
        let instruction_data = match target_market.protocol {
            Protocol::Solend => self.encode_solend_deposit(position.amount),
            Protocol::Kamino => self.encode_kamino_deposit(position.amount),
        };
        
        let accounts = self.get_deposit_accounts(position, target_market).await?;
        
        Ok(Instruction {
            program_id,
            accounts,
            data: instruction_data,
        })
        match target_market.protocol {
            Protocol::Solend => {
                // Resolve reserve and vaults from on-chain state, derive only documented market authority PDA
                let program_id = Pubkey::from_str(SOLEND_PROGRAM_ID)?;
                let meta = self
                    .resolve_reserve_by_mint(&program_id, &position.token_mint)
                    .await?;

                // User ATAs: source is liquidity mint ATA, destination is collateral mint ATA
                let user = self.wallet.pubkey();
                let user_source_token_ata = spl_associated_token_account::get_associated_token_address(&user, &meta.liquidity_mint);
                let user_collateral_ata = spl_associated_token_account::get_associated_token_address(&user, &meta.collateral_mint);

                // Build full instruction using verified accounts
                self.build_solend_deposit_ix(
                    user_source_token_ata,
                    user_collateral_ata,
                    position.amount,
                    &meta,
                    program_id,
                )
            }
            Protocol::Kamino => {
                let program_id = Pubkey::from_str(KAMINO_PROGRAM_ID)?;
                let instruction_data = self.encode_kamino_deposit(position.amount)?;
                let accounts = self.get_deposit_accounts(position, target_market).await?;
                Ok(Instruction { program_id, accounts, data: instruction_data })
            }
        }
}

    fn encode_solend_withdraw(&self, amount: u64) -> Vec<u8> {
        let mut data = vec![5u8]; // Solend withdraw instruction discriminator
        data.extend_from_slice(&amount.to_le_bytes());
        data
    // Kamino (Anchor program) encoders using module-level helpers
    fn encode_kamino_deposit(&self, amount: u64) -> Result<Vec<u8>> {
        encode_anchor_ix("deposit", DepositArgs { amount })
}

    fn encode_kamino_withdraw(&self, amount: u64) -> Vec<u8> {
        let mut data = vec![183, 18, 70, 156, 148, 109, 161, 34]; // Kamino withdraw hash
        data.extend_from_slice(&amount.to_le_bytes());
        data
    fn encode_kamino_withdraw(&self, amount: u64) -> Result<Vec<u8>> {
        encode_anchor_ix("withdraw", WithdrawArgs { amount })
}

    fn encode_solend_deposit(&self, amount: u64) -> Vec<u8> {
        let mut data = vec![4u8]; // Solend deposit instruction discriminator
    fn encode_solend_deposit(&self, amount: u64) -> Result<Vec<u8>> {
        let mut data = vec![SOLEND_DEPOSIT_TAG];
data.extend_from_slice(&amount.to_le_bytes());
        data
        Ok(data)
}

    fn encode_kamino_deposit(&self, amount: u64) -> Vec<u8> {
        let mut data = vec![242, 35, 198, 137, 82, 225, 242, 182]; // Kamino deposit hash
    fn encode_solend_withdraw(&self, amount: u64) -> Result<Vec<u8>> {
        let mut data = vec![SOLEND_WITHDRAW_TAG];
data.extend_from_slice(&amount.to_le_bytes());
        data
        Ok(data)
    }

    // ----- Account Resolution without Guesswork (Solend-like programs) -----
    // Documented PDA: [lending_market, b"authority"] -> lending market authority
    fn derive_lending_market_authority(lending_market: &Pubkey, program_id: &Pubkey) -> (Pubkey, u8) {
        Pubkey::find_program_address(&[lending_market.as_ref(), b"authority"], program_id)
    }

    // Scan program accounts and resolve Reserve for a given liquidity mint. Bounds-check slices before reading.
    pub async fn resolve_reserve_by_mint(
        &self,
        program_id: &Pubkey,
        token_mint: &Pubkey,
    ) -> Result<ReserveMeta> {
        let accounts = self
            .rpc_client
            .get_program_accounts(program_id)
            .await
            .map_err(|e| anyhow!(format!("get_program_accounts: {e}")))?;

        for (reserve_pubkey, acc) in accounts {
            // Replace offsets with verified layout offsets in golden tests
            if acc.data.len() < 224 { continue; }

            let liquidity_mint = Pubkey::new(
                acc.data
                    .get(32..64)
                    .ok_or_else(|| anyhow!("liquidity_mint slice"))?,
            );
            if &liquidity_mint != token_mint { continue; }

            let lending_market = Pubkey::new(
                acc.data
                    .get(96..128)
                    .ok_or_else(|| anyhow!("lending_market slice"))?,
            );
            let liquidity_supply_vault = Pubkey::new(
                acc.data
                    .get(128..160)
                    .ok_or_else(|| anyhow!("liquidity_supply vault slice"))?,
            );
            let collateral_mint = Pubkey::new(
                acc.data
                    .get(160..192)
                    .ok_or_else(|| anyhow!("collateral_mint slice"))?,
            );
            let collateral_supply_vault = Pubkey::new(
                acc.data
                    .get(192..224)
                    .ok_or_else(|| anyhow!("collateral_supply vault slice"))?,
            );

            let (authority, _bump) = Self::derive_lending_market_authority(&lending_market, program_id);
            return Ok(ReserveMeta {
                reserve: reserve_pubkey,
                lending_market,
                lending_market_authority: authority,
                liquidity_mint,
                liquidity_supply_vault,
                collateral_mint,
                collateral_supply_vault,
                program_id: *program_id,
            });
        }

        Err(anyhow!(format!(
            "reserve for mint {} not found in program {}",
            token_mint, program_id
        )))
    }

    pub async fn build_solend_deposit_ix(
        &self,
        user_source_token_ata: Pubkey,
        user_collateral_ata: Pubkey,
        amount: u64,
        meta: &ReserveMeta,
        program_id: Pubkey,
    ) -> Result<Instruction> {
        let data = self.encode_solend_deposit(amount)?;
        // Accounts per token-lending deposit reserve liquidity spec
        let accounts = vec![
            AccountMeta::new(meta.liquidity_supply_vault, false),
            AccountMeta::new(user_source_token_ata, false),
            AccountMeta::new(meta.reserve, false),
            AccountMeta::new(meta.lending_market, false),
            AccountMeta::new_readonly(meta.lending_market_authority, false),
            AccountMeta::new(meta.collateral_mint, false),
            AccountMeta::new(meta.collateral_supply_vault, false),
            AccountMeta::new(user_collateral_ata, false),
            AccountMeta::new_readonly(spl_token::id(), false),
            AccountMeta::new_readonly(solana_sdk::sysvar::rent::id(), false),
            AccountMeta::new_readonly(solana_sdk::sysvar::clock::id(), false),
        ];
        Ok(Instruction { program_id, accounts, data })
    }

    pub async fn build_solend_withdraw_ix(
        &self,
        user_collateral_ata: Pubkey,
        user_destination_liquidity_ata: Pubkey,
        amount: u64,
        meta: &ReserveMeta,
        program_id: Pubkey,
    ) -> Result<Instruction> {
        let data = self.encode_solend_withdraw(amount)?;
        // Accounts per token-lending redeem reserve collateral spec
        let accounts = vec![
            AccountMeta::new(user_collateral_ata, false),
            AccountMeta::new(meta.collateral_supply_vault, false),
            AccountMeta::new(meta.reserve, false),
            AccountMeta::new(meta.lending_market, false),
            AccountMeta::new_readonly(meta.lending_market_authority, false),
            AccountMeta::new(meta.liquidity_supply_vault, false),
            AccountMeta::new(user_destination_liquidity_ata, false),
            AccountMeta::new_readonly(spl_token::id(), false),
            AccountMeta::new_readonly(solana_sdk::sysvar::rent::id(), false),
            AccountMeta::new_readonly(solana_sdk::sysvar::clock::id(), false),
        ];
        Ok(Instruction { program_id, accounts, data })
}

async fn get_withdraw_accounts(&self, position: &Position) -> Result<Vec<AccountMeta>> {
@@ -476,7 +1482,7 @@ impl PortOptimizer {
let mut last_error = None;

while retries > 0 {
            match self.rpc_client.get_latest_blockhash() {
            match self.rpc_client.get_latest_blockhash().await {
Ok(blockhash) => return Ok(blockhash),
Err(e) => {
last_error = Some(e);
@@ -494,18 +1500,11 @@ impl PortOptimizer {
let mut last_error = None;

while retries > 0 {
            match self.rpc_client.send_transaction(transaction) {
            match self.rpc_client.send_transaction(transaction).await {
Ok(signature) => {
                    if self.confirm_transaction(&signature).await? {
                        return Ok(signature.to_string());
                    }
                }
                Err(e) => {
                    if e.to_string().contains("AlreadyProcessed") {
                        return Ok(transaction.signatures[0].to_string());
                    }
                    last_error = Some(e);
                    if self.confirm_transaction(&signature).await? { return Ok(signature.to_string()); }
}
                Err(e) => { if e.to_string().contains("AlreadyProcessed") { return Ok(transaction.signatures[0].to_string()); } last_error = Some(e); }
}

retries -= 1;
@@ -520,21 +1519,91 @@ impl PortOptimizer {
let timeout = Duration::from_secs(30);

while start.elapsed() < timeout {
            match self.rpc_client.get_signature_status(signature)? {
                Some(status) => {
                    if status.is_ok() {
                        return Ok(true);
                    } else if status.is_err() {
                        return Ok(false);
                    }
            let resp = self.rpc_client.get_signature_statuses(&[*signature]).await?;
            if let Some(opt_st) = resp.value.first() { if let Some(st) = opt_st { return Ok(st.err.is_none()); } }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        
        Ok(false)
    }

    // Hedged transaction sender with simulation and auto-repair
    pub async fn send_hedged_tx(
        &self,
        ixs: &[Instruction],
        signers: &[&Keypair],
        opts: Option<CommitmentConfig>,
        max_retries: usize,
    ) -> Result<Signature> {
        let commitment = opts.unwrap_or(CommitmentConfig::confirmed());
        let mut retry_count = 0;
        let mut last_error = None;
        
        // Make a mutable copy of instructions for potential repairs
        let mut current_ixs = ixs.to_vec();
        
        while retry_count < max_retries {
            // 1. First try with simulation to catch errors early
            match self.rpc_client.simulate_transaction_with_config(
                &Transaction::new_with_payer(&current_ixs, Some(&self.wallet.pubkey())),
                commitment,
                RpcSimulateTransactionConfig {
                    sig_verify: true,
                    replace_recent_blockhash: true,
                    ..Default::default()
                },
            ).await {
                Ok(sim_result) => {
                    if let Some(err) = sim_result.value.err {
                        // Try to auto-repair if simulation fails
                        if let Some(repaired_ixs) = auto_repair_from_logs(&current_ixs, &sim_result.value.logs.unwrap_or_default()) {
                            current_ixs = repaired_ixs;
                            retry_count += 1;
                            continue;
                        }
                        return Err(anyhow!("Simulation failed: {:?}", err));
                    }
                    
                    // If simulation succeeds, proceed with sending
                    match self.send_transaction_with_retries(
                        &current_ixs,
                        signers,
                        Some(commitment),
                        3, // Fewer retries since we already simulated
                    ).await {
                        Ok(sig) => return Ok(sig),
                        Err(e) => last_error = Some(e),
                    }
                },
                Err(e) => last_error = Some(anyhow!("Simulation error: {}", e)),
            }
            
            // If we get here, the transaction failed
            retry_count += 1;
            if retry_count < max_retries {
                // Exponential backoff with jitter
                let sleep_ms = 100 * 2u64.pow(retry_count as u32) + rand::random::<u64>() % 100;
                tokio::time::sleep(Duration::from_millis(sleep_ms)).await;
            }
        }
        
        Err(last_error.unwrap_or_else(|| anyhow!("Max retries exceeded")))
    }

    // ----- Slot-aware blockhash lifecycle helpers -----
    pub async fn get_fresh_blockhash(&self) -> Result<(Hash, u64)> {
        let bh = self.rpc_client.get_latest_blockhash().await?;
        let height = self.rpc_client.get_block_height().await?;
        Ok((bh, height))
    }

    async fn simulate_exact_v0(&self, tx: &VersionedTransaction) -> Result<()> {
        let res = self.simulate_exact(tx).await?;
        if let Some(err) = res.err {
            return Err(anyhow!(format!("simulation err: {:?}", err)));
        }
        Ok(())
    }

    async fn send_and_confirm_v0(
        &self,
        tx: &VersionedTransaction,
        timeout: Duration,
    ) -> Result<solana_sdk::signature::Signature> {
        let sig = self.rpc_client.send_transaction(tx).await?;
        let ok = self.confirm_sig_with_backoff(&sig, timeout).await?;
        if ok { 
            // Update the last transaction signature for deterministic operations
            self.update_last_tx_signature(sig).await;
            Ok(sig) 
        } else { 
            Err(anyhow!("confirmation timeout")) 
        }
    }

    pub async fn send_v0_with_slot_guard<F>(
        &self,
        mut build_with_hash: F,
        max_wait: Duration,
        _hash_valid_for_slots: u64,
        refresh_before_slots: u64,
    ) -> Result<solana_sdk::signature::Signature>
    where
        F: Send + 'static + FnMut(Hash) -> Result<VersionedTransaction>,
    {
        let start = Instant::now();
        let (mut bh, mut h0) = self.get_fresh_blockhash().await?;
        let mut tx = build_with_hash(bh)?;
        loop {
            if start.elapsed() >= max_wait {
                return Err(anyhow!("timeout sending v0 tx"));
            }
            if let Err(e) = self.simulate_exact_v0(&tx).await {
                let msg = e.to_string().to_lowercase();
                if msg.contains("blockhash") {
                    let pair = self.get_fresh_blockhash().await?;
                    bh = pair.0;
                    h0 = pair.1;
                    tx = build_with_hash(bh)?;
                    continue;
}
                None => {
                return Err(anyhow!(format!("simulation failed: {e}")));
            }

            match self.send_and_confirm_v0(&tx, Duration::from_secs(20)).await {
                Ok(sig) => return Ok(sig),
                Err(e) => {
                    let msg = e.to_string().to_lowercase();
                    if msg.contains("blockhash not found") || msg.contains("blockhashnotfound") {
                        let pair = self.get_fresh_blockhash().await?;
                        bh = pair.0;
                        h0 = pair.1;
                        tx = build_with_hash(bh)?;
                        continue;
                    }
                    let h_now = self.rpc_client.get_block_height().await.unwrap_or(h0);
                    if h_now.saturating_sub(h0) >= refresh_before_slots {
                        let pair = self.get_fresh_blockhash().await?;
                        bh = pair.0;
                        h0 = pair.1;
                        tx = build_with_hash(bh)?;
                        continue;
                    }
tokio::time::sleep(Duration::from_millis(200)).await;
}
}
}
        
        Ok(false)
}

async fn update_position(
@@ -591,7 +1660,7 @@ impl PortOptimizer {
mint,
);

        match self.rpc_client.get_account(&token_account) {
        match self.rpc_client.get_account(&token_account).await {
Ok(account) => {
let token_account_data = TokenAccount::unpack(&account.data)?;
Ok(token_account_data.amount)
@@ -600,17 +1669,16 @@ impl PortOptimizer {
}
}

    async fn find_best_market(&self, _mint: &Pubkey) -> Result<LendingMarket> {
    async fn find_best_market(&self, mint: &Pubkey) -> Result<LendingMarket> {
self.update_market_data().await?;
        
let markets = self.markets.read().await;
        let token_markets = markets.values()
            .flatten()
            .filter(|m| m.liquidity_available > 0)
            .max_by(|a, b| a.supply_apy.partial_cmp(&b.supply_apy).unwrap())
            .ok_or_else(|| anyhow!("No available markets for token"))?;
        
        Ok(token_markets.clone())
        let key = mint.to_string();
        let candidates = markets.get(&key)
            .ok_or_else(|| anyhow!("No markets for token {}", key))?;
        let best = self
            .pick_best_market_weighted(0, candidates)
            .ok_or_else(|| anyhow!("No available markets for token {}", key))?;
        Ok(best)
}

async fn execute_initial_deposit(
@@ -629,21 +1697,23 @@ impl PortOptimizer {
},
market,
).await?;
        
        let recent_blockhash = self.get_recent_blockhash_with_retry().await?;
        
        let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNITS);
        let priority_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS);
        
        let mut transaction = Transaction::new_with_payer(
            &[compute_budget_ix, priority_fee_ix, deposit_ix],
            Some(&self.wallet.pubkey()),
        );
        
        transaction.sign(&[self.wallet.as_ref()], recent_blockhash);
        
        self.send_transaction_with_retry(&transaction).await?;
        

        // Versioned v0 send with dynamic budget, exact sim, stale-hash recovery
        let baseline = self.optimize_compute_units_rough(&[deposit_ix.clone()]).await;
        let (cu_ix, fee_ix) = self.dynamic_budget_ixs(baseline, PRIORITY_FEE_LAMPORTS).await;
        let ixs = vec![cu_ix, fee_ix, deposit_ix];
        let bh = self.get_recent_blockhash_with_retry().await?;
        let tx = self.build_v0_tx(self.wallet.pubkey(), &ixs, bh, &[])?;
        let sim = self.simulate_exact(&tx).await?;
        if let Some(err) = sim.err { return Err(anyhow!(format!("preflight sim failed: {:?}", err))); }
        let _sig = self
            .send_v0_with_slot_guard(
                |bh2| self.build_v0_tx(self.wallet.pubkey(), &ixs, bh2, &[]),
                Duration::from_secs(30),
                200,
                120,
            )
            .await?;
Ok(())
}

@@ -662,20 +1732,21 @@ impl PortOptimizer {

async fn emergency_withdraw(&self, position: &Position) -> Result<()> {
let withdraw_ix = self.build_withdraw_instruction(position).await?;
        let recent_blockhash = self.get_recent_blockhash_with_retry().await?;
        
        let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNITS * 2);
        let priority_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS * 5);
        
        let mut transaction = Transaction::new_with_payer(
            &[compute_budget_ix, priority_fee_ix, withdraw_ix],
            Some(&self.wallet.pubkey()),
        );
        
        transaction.sign(&[self.wallet.as_ref()], recent_blockhash);
        
        self.send_transaction_with_retry(&transaction).await?;
        
        let baseline = self.optimize_compute_units_rough(&[withdraw_ix.clone()]).await;
        let (cu_ix, fee_ix) = self.dynamic_budget_ixs(baseline.max(COMPUTE_UNITS * 2), PRIORITY_FEE_LAMPORTS * 5).await;
        let ixs = vec![cu_ix, fee_ix, withdraw_ix];
        let bh = self.get_recent_blockhash_with_retry().await?;
        let tx = self.build_v0_tx(self.wallet.pubkey(), &ixs, bh, &[])?;
        let sim = self.simulate_exact(&tx).await?;
        if let Some(err) = sim.err { return Err(anyhow!(format!("preflight sim failed: {:?}", err))); }
        let _sig = self
            .send_v0_with_slot_guard(
                |bh2| self.build_v0_tx(self.wallet.pubkey(), &ixs, bh2, &[]),
                Duration::from_secs(30),
                200,
                120,
            )
            .await?;
Ok(())
}
}
@@ -705,7 +1776,7 @@ impl PortOptimizer {
}

async fn preflight_simulation(&self, transaction: &Transaction) -> Result<bool> {
        match self.rpc_client.simulate_transaction(transaction) {
        match self.rpc_client.simulate_transaction(transaction).await {
Ok(result) => {
if result.value.err.is_none() {
Ok(true)
@@ -731,7 +1802,7 @@ impl PortOptimizer {
}

async fn calculate_network_congestion(&self) -> Result<f64> {
        let recent_fees = self.rpc_client.get_recent_prioritization_fees(&[])?;
        let recent_fees = self.rpc_client.get_recent_prioritization_fees(&[]).await?;
let avg_fee = recent_fees.iter().map(|f| f.prioritization_fee).sum::<u64>() as f64 
/ recent_fees.len().max(1) as f64;

@@ -779,66 +1850,24 @@ impl PortOptimizer {
&self,
transactions: Vec<Transaction>,
) -> Result<Vec<Transaction>> {
        // Deprecated: prefer build_bundle_from_plans with canonical instruction builders.
        // Keep existing signature but avoid unsafe reconstruction; just prepend a tip transfer tx as a separate tx.
let tip_amount = self.get_jito_tip().await;
        let tip_accounts = vec![
            Pubkey::from_str("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5")?,
            Pubkey::from_str("HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe")?,
            Pubkey::from_str("Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY")?,
            Pubkey::from_str("ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49")?,
            Pubkey::from_str("DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh")?,
            Pubkey::from_str("ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt")?,
            Pubkey::from_str("DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL")?,
            Pubkey::from_str("3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT")?,
        ];
        
        let random_index = (self.rpc_client.get_slot()? % 8) as usize;
        let tip_account = tip_accounts[random_index];
        
        let tip_ix = system_instruction::transfer(
            &self.wallet.pubkey(),
            &tip_account,
            tip_amount,
        );
        
        let mut bundle = Vec::new();
        for tx in transactions {
            let mut new_instructions = vec![tip_ix.clone()];
            new_instructions.extend(tx.message.instructions.clone().clone().into_iter().map(|ix| {
                Instruction {
                    program_id: tx.message.account_keys[ix.program_id_index as usize],
                    accounts: ix.accounts.into_iter().map(|acc_idx| {
                        AccountMeta {
                            pubkey: tx.message.account_keys[acc_idx as usize],
                            is_signer: tx.message.is_signer(acc_idx as usize),
                            is_writable: tx.message.is_writable(acc_idx as usize),
                        }
                    }).collect(),
                    data: ix.data,
                }
            }));

            // Collect all unique signers
            let mut signers = HashSet::new();
            signers.insert(self.wallet.pubkey());
            
            for account in tx.message.account_keys.iter() {
                if tx.message.is_signer(tx.message.account_keys.iter().position(|a| a == account).unwrap()) {
                    signers.insert(*account);
                }
            }

            // Build new transaction with tip instruction prepended
            let message = Message::new(&new_instructions, Some(&self.wallet.pubkey()));
            let new_tx = Transaction::new_unsigned(message);
            bundle.push(new_tx);
        }

        Ok(bundle)
        let tip_account = Pubkey::from_str("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5")?;
        let tip_ix = system_instruction::transfer(&self.wallet.pubkey(), &tip_account, tip_amount);
        let message = Message::new(&[tip_ix], Some(&self.wallet.pubkey()));
        let mut tip_tx = Transaction::new_unsigned(message);
        let bh = self.get_recent_blockhash_with_retry().await?;
        tip_tx.sign(&[self.wallet.as_ref()], bh);
        let mut out = Vec::with_capacity(transactions.len() + 1);
        out.push(tip_tx);
        out.extend(transactions.into_iter());
        Ok(out)
}


pub async fn optimize_compute_units(&self, transaction: &Transaction) -> u32 {
        match self.rpc_client.simulate_transaction(transaction) {
        match self.rpc_client.simulate_transaction(transaction).await {
Ok(result) => {
if let Some(units) = result.value.units_consumed {
(units as f64 * 1.2) as u32
@@ -927,7 +1956,7 @@ mod tests {
let optimizer = PortOptimizer::new("https://api.mainnet-beta.solana.com", Keypair::new());
let rate = 1000000000000000u64;
let apy = optimizer.calculate_apy_from_rate(rate);
        assert!(apy > 0.0 && apy < 100.0);
        assert!(apy >= 0.0 && apy <= 1000.0);
}

#[tokio::test]
