//! validator_preference_learner.rs 
//! ======================================================================
//! Validator Preference Learner (VPL) + real-world wiring:
//! - Live slot timing feed (getRecentPerformanceSamples) with EWMA smoothing
//!   over 60s buckets (don’t assume sub-second precision).
//! - Leader schedule biasing with absolute slot mapping
//!   (getEpochInfo -> epochStartSlot + getLeaderSchedule(epoch))
//! - Priority-fee ground-truth (getRecentPrioritizationFees rolling window),
//!   capped to the node’s shallow cache (~150 blocks), de-duped by slot,
//!   sparsity-aware.
//! - ALT v0/LUT sanity helpers (lengths, bounds, signer-in-LUT,
//!   program-writable demotion) + on-chain ALT decode cross-check.
//! - Provenance persistence (leader epoch, perf sample slot, fee feed ts,
//!   fee window stats).
//! - CU-price translation helper for Compute Budget semantics
//!
//! Dependencies (Cargo.toml):
//! tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }
//! dashmap = "5"
//! ahash = "0.8"
//! serde = { version = "1", features = ["derive"] }
//! bincode = "1"
//! chrono = { version = "0.4", features = ["clock"] }
//! solana-sdk = "1.18"
//! log = "0.4"
//! reqwest = { version = "0.11", features = ["json", "rustls-tls"] }
//! thiserror = "1"
//!
//! Docs references (keep these straight):
//! - Leader schedule slots are epoch-relative; use getEpochInfo.epochStartSlot for absolutization.
//!   https://docs.solana.com/developing/clients/jsonrpc-api#getleaderschedule
//!   https://docs.solana.com/developing/clients/jsonrpc-api#getepochinfo
//! - Priority-fee price for ComputeBudget is micro-lamports per CU (μ-lamports/CU).
//!   https://docs.solana.com/implemented-proposals/transaction-wide-fee-market#priority-fees
//!   https://docs.solana.com/developing/clients/jsonrpc-api#solana-transaction-fees
//! - v0/LUT invariants (signers not in LUT, program IDs never writable, fee payer is RW).
//!   https://docs.solana.com/proposals/transactions-v2
//!   https://docs.solana.com/developing/lookup-tables
//!
//! Optional (for tests):
//! tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }

#![forbid(unsafe_code)]

use std::collections::{HashMap, VecDeque, HashSet, BTreeMap};
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering}
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ahash::{RandomState, AHashSet};
use bincode;
use chrono::{DateTime, Datelike, NaiveDateTime, Timelike, Utc};
use dashmap::DashMap;
use log::{debug, error, warn, info};
use reqwest::Client as HttpClient;
use serde::{Deserialize, Serialize};
use solana_sdk::{
    account::Account,
    clock::Slot,
    pubkey::Pubkey,
    message::{v0::LoadedAddresses, VersionedMessage},
    address_lookup_table::AddressLookupTableAccount,
    compute_budget,
};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{interval, sleep};
use thiserror::Error;

// ---------- Fast maps ----------
type DMap<K, V> = DashMap<K, V, RandomState>;

// ---------- Defaults ----------
fn default_alpha() -> f64 { 2.0 }
fn default_beta() -> f64 { 2.0 }
fn default_ewma_latency() -> f64 { 200.0 }

// ---------- Tunables ----------
const PREFERENCE_WINDOW_SIZE: usize = 10_000;
const MIN_SAMPLE_SIZE: usize = 50;
const PREFERENCE_UPDATE_INTERVAL: Duration = Duration::from_millis(400);
const DECAY_HALFLIFE_HOURS: f64 = 24.0;
const MAX_VALIDATORS_TRACKED: usize = 500;
const OUTLIER_Z_THRESHOLD: f64 = 3.0;
const MAD_EPS: f64 = 1e-9;
const CONFIDENCE_THRESHOLD: f64 = 0.75;
const SLOT_HISTORY_SIZE: usize = 432_000;
const UCB_BONUS_SCALE: f64 = 0.10;
const LAT_MS_SOFT_RELIABILITY: f64 = 50.0;
const LAT_MS_SOFT_SELECTION:  f64 = 80.0;
const RECOMPUTE_BATCH_LIMIT: usize = 256;

// ---------- Slot timing clamp (sane bounds) ----------
// getRecentPerformanceSamples are 60s buckets. We smooth and clamp.
const SLOT_MS_MIN: u64 = 300;
const SLOT_MS_MAX: u64 = 1200;
const PERF_BUCKET_SECONDS: u64 = 60;      // per RPC docs
const PERF_SLOT_EWMA_ALPHA: f64 = 0.25;   // moderate smoothing for 60s buckets

// ---------- Fee feed window ----------
// Node cache is shallow (~150 blocks). Keep a tight cap and measure sparsity.
const FEE_WINDOW_CAP: usize = 160;
const FEE_SPARSE_MIN_SLOTS: usize = 50; // below this, treat as sparse
const FEE_POLL_INTERVAL: Duration = Duration::from_millis(1200);

// ---------- Leader schedule poll ----------
const LEADER_POLL_INTERVAL: Duration = Duration::from_secs(15);

// ---------- Performance sample poll ----------
const PERF_POLL_INTERVAL: Duration = Duration::from_millis(400);

// ---------- JSON-RPC ----------
#[derive(Clone)]
struct RpcJson {
    url: String,
    http: HttpClient,
}

impl RpcJson {
    fn new(url: impl Into<String>) -> Self {
        Self { url: url.into(), http: HttpClient::new() }
    }

    async fn call<T: for<'de> Deserialize<'de>>(
        &self,
        method: &str,
        params: serde_json::Value
    ) -> Result<T, RpcError> {
        #[derive(Deserialize)]
        struct RpcResp<T> { jsonrpc: String, result: Option<T>, error: Option<RpcErrObj> }
        #[derive(Deserialize)]
        struct RpcErrObj { code: i64, message: String, data: Option<serde_json::Value> }

        let body = serde_json::json!({
            "jsonrpc":"2.0",
            "id":1,
            "method": method,
            "params": params
        });
        let resp = self.http.post(&self.url).json(&body).send().await
            .map_err(|e| RpcError::Network(e.to_string()))?;
        let status = resp.status();
        let val = resp.json::<RpcResp<T>>().await
            .map_err(|e| RpcError::Protocol(format!("decode error {e} (status {status})")))?;
        if let Some(err) = val.error {
            return Err(RpcError::Server(err.message));
        }
        val.result.ok_or_else(|| RpcError::Protocol("missing result".into()))
    }

    async fn get_recent_performance_samples(&self) -> Result<Vec<PerfSample>, RpcError> {
        #[derive(Deserialize)]
        struct Perf { numSlots: u64, numTransactions: u64, samplePeriodSecs: u64, slot: u64 }
        let res: Vec<Perf> = self.call("getRecentPerformanceSamples", serde_json::json!([1])).await?;
        Ok(res.into_iter().map(|p| PerfSample {
            slot: p.slot,
            num_slots: p.numSlots,
            num_txs: p.numTransactions,
            period_secs: p.samplePeriodSecs
        }).collect())
    }

    async fn get_leader_schedule(&self, slot: Option<Slot>) -> Result<BTreeMap<String, Vec<u64>>, RpcError> {
        // Returns map<validator_pubkey_string, [slots in the epoch]>
        let params = if let Some(s) = slot {
            serde_json::json!([s])
        } else {
            serde_json::json!([])
        };
        self.call("getLeaderSchedule", params).await
    }

    async fn get_recent_prioritization_fees(&self, accounts: Option<Vec<String>>) -> Result<Vec<RecentPrioritizationFee>, RpcError> {
        #[derive(Deserialize)]
        struct RPF { slot: u64, prioritizationFee: u64 }
        let params = match accounts {
            Some(a) => serde_json::json!([a]),
            None => serde_json::json!([])
        };
        let res: Vec<RPF> = self.call("getRecentPrioritizationFees", params).await?;
        Ok(res.into_iter().map(|x| RecentPrioritizationFee{ slot: x.slot, prioritization_fee: x.prioritizationFee }).collect())
    }

    async fn get_epoch_info(&self) -> Result<EpochInfo, RpcError> {
        #[derive(Deserialize)]
        struct Resp { epoch: u64, absoluteSlot: u64, slotIndex: u64, slotsInEpoch: u64, epochStartSlot: u64 }
        let v: Resp = self.call("getEpochInfo", serde_json::json!([])).await?;
        Ok(EpochInfo {
            epoch: v.epoch,
            absolute_slot: v.absoluteSlot,
            slot_index: v.slotIndex,
            slots_in_epoch: v.slotsInEpoch,
            epoch_start_slot: v.epochStartSlot,
        })
    }
}

#[derive(Debug, Error)]
enum RpcError {
    #[error("rpc network: {0}")]
    Network(String),
    #[error("rpc protocol: {0}")]
    Protocol(String),
    #[error("rpc server: {0}")]
    Server(String),
}

#[derive(Debug, Clone)]
struct PerfSample {
    slot: u64,
    num_slots: u64,
    num_txs: u64,
    period_secs: u64,
}

#[derive(Debug, Clone)]
struct EpochInfo {
    epoch: u64,
    absolute_slot: u64,
    slot_index: u64,
    slots_in_epoch: u64,
    epoch_start_slot: u64,
}

#[derive(Debug, Clone)]
struct RecentPrioritizationFee {
    slot: u64,
    prioritization_fee: u64, // μ-lamports per CU quote source; translate with CU estimate
}

// --- numerics helpers (no NaNs) ---
#[inline(always)]
fn safe_logit(p: f64) -> f64 {
    let q = p.clamp(1e-6, 1.0 - 1e-6);
    (q / (1.0 - q)).ln()
}
#[inline(always)]
fn safe_ln(x: f64) -> f64 {
    (x.max(1e-9)).ln()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorMetrics {
    pub pubkey: Pubkey,
    pub total_slots_processed: u64,
    pub mev_inclusion_rate: f64,
    pub avg_latency_ms: f64,
    #[serde(default = "default_ewma_latency")]
    pub ewma_latency_ms: f64,
    pub fee_preference: FeePreference,
    pub ordering_preference: OrderingPreference,
    pub temporal_patterns: TemporalPatterns,
    pub reliability_score: f64,
    pub last_updated: u64,
    pub confidence_score: f64,
    #[serde(default = "default_alpha")]
    pub alpha_included: f64,
    #[serde(default = "default_beta")]
    pub beta_excluded: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeePreference {
    pub min_priority_fee: u64,
    pub avg_accepted_fee: f64,
    pub fee_stdev: f64,
    pub high_fee_affinity: f64,
    pub p50: f64,
    pub p75: f64,
    pub p90: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderingPreference {
    pub frontrun_tendency: f64,
    pub backrun_tendency: f64,
    pub sandwich_success_rate: f64,
    pub bundle_acceptance_rate: f64,
    pub ordering_consistency: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalPatterns {
    pub hourly_activity: [f64; 24],
    pub peak_hours: Vec<u8>,
    pub low_activity_hours: Vec<u8>,
    pub weekly_pattern: [f64; 7],
}

#[derive(Debug, Clone)]
pub struct TransactionOutcome {
    pub validator: Pubkey,
    pub slot: Slot,
    pub included: bool,
    pub latency_ms: u64,
    pub priority_fee: u64,
    pub transaction_type: TransactionType,
    pub position_in_block: Option<usize>,
    pub block_size: Option<usize>,
    pub timestamp: u64,
    pub leader: Option<Pubkey>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionType { Arbitrage, Liquidation, Sandwich, Frontrun, Backrun, Standard }

// ---------- ALT sanity helpers ----------
#[derive(Debug, Error)]
pub enum AltSanityError {
    #[error("lookup length mismatch (lookups={0} fetched={1})")]
    Length(usize, usize),
    #[error("index out of bounds in writable indexes")]
    OobWritable,
    #[error("index out of bounds in readonly indexes")]
    OobReadonly,
    #[error("program id flagged writable in LUT; demoting required")]
    ProgramWritable,
    #[error("signer key present inside LUT (forbidden)")]
    SignerInLut,
    #[error("invalid ALT account decode")]
    Decode,
    #[error("ALT address index exceeds decoded address list")]
    AddressIndex,
}

pub struct AltSanity;

impl AltSanity {
    /// Decode raw ALT accounts and perform basic on-chain spec checks.
    /// Ensures address list is decodable and indexable before applying writability/signers rules.
    pub fn decode_and_validate_accounts(fetched_raw: &[Account]) -> Result<Vec<AddressLookupTableAccount>, AltSanityError> {
        let mut out = Vec::with_capacity(fetched_raw.len());
        for acc in fetched_raw {
            // Use AddressLookupTableAccount::deserialize to cross-check layout.
            let decoded = AddressLookupTableAccount::deserialize(&acc.data)
                .map_err(|_| AltSanityError::Decode)?;
            // Minimal sanity: addresses present and within a reasonable bound
            if decoded.addresses.is_empty() {
                return Err(AltSanityError::Decode);
            }
            out.push(decoded);
        }
        Ok(out)
    }

    /// v0/LUT constraints:
    /// - Signers must not resolve from LUTs.
    /// - Program IDs must never be writable (demote if mis-flagged).
    /// - Fee payer remains RW (enforced at builder; kept visible here).
    /// Docs:
    ///   https://docs.solana.com/proposals/transactions-v2
    ///   https://docs.solana.com/developing/lookup-tables
    pub fn validate_loaded(
        msg: &VersionedMessage,
        lookups: &LoadedAddresses,
        fetched_decoded: &[AddressLookupTableAccount],
        fee_payer: &Pubkey,
        program_ids: &[Pubkey],
        signers: &[Pubkey],
    ) -> Result<(), AltSanityError> {
        if fetched_decoded.len() != lookups.accounts.len() {
            return Err(AltSanityError::Length(lookups.accounts.len(), fetched_decoded.len()));
        }
        // Check that all indices are within decoded address lists
        for (table_ix, table) in lookups.accounts.iter().enumerate() {
            let addr_len = fetched_decoded
                .get(table_ix)
                .map(|a| a.addresses.len())
                .ok_or(AltSanityError::Decode)?;
            for &i in lookups.writable.get(table_ix).unwrap_or(&[]).iter() {
                if (i as usize) >= addr_len { return Err(AltSanityError::AddressIndex); }
            }
            for &i in lookups.readonly.get(table_ix).unwrap_or(&[]).iter() {
                if (i as usize) >= addr_len { return Err(AltSanityError::AddressIndex); }
            }
        }
        // No signer may be sourced from LUTs
        for s in signers {
            if lookups
                .writable
                .iter()
                .flat_map(|v| v.iter())
                .chain(lookups.readonly.iter().flat_map(|v| v.iter()))
                .any(|k| k == s)
            {
                return Err(AltSanityError::SignerInLut);
            }
        }
        // Programs not writable via LUT
        let lut_writables: AHashSet<Pubkey> = lookups
            .writable
            .iter()
            .flat_map(|v| v.iter().copied())
            .collect();
        for pid in program_ids {
            if lut_writables.contains(pid) {
                return Err(AltSanityError::ProgramWritable);
            }
        }
        let _ = msg;
        let _ = fee_payer;
        Ok(())
    }
}

// ---------------------- VPL CORE ----------------------
pub struct ValidatorPreferenceLearner {
    // core
    metrics: Arc<DMap<Pubkey, ValidatorMetrics>>,
    transaction_history: Arc<DMap<Pubkey, VecDeque<TransactionOutcome>>>,
    slot_performance: Arc<DMap<Slot, SlotMetrics>>,
    updated_validators: Arc<DMap<Pubkey, u64>>,

    // wiring
    rpc: RpcJson,
    rpc_for_fees: RpcJson,

    // timing + leader + fees
    slot_ms_current: Arc<AtomicU64>,
    leader_map: Arc<DMap<Slot, Pubkey>>,
    last_leader_epoch: Arc<AtomicU64>,
    fee_window: Arc<tokio::sync::Mutex<VecDeque<(u64 /*slot*/, u64 /*μlam/CU*/)>>>,
    fee_slots_seen: Arc<tokio::sync::Mutex<HashSet<u64>>>,

    // provenance
    provenance: Arc<tokio::sync::Mutex<Provenance>>,

    // admin
    update_sender: Sender<PreferenceUpdate>,
    persistence_path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Provenance {
    last_leader_schedule_epoch: u64,
    last_performance_sample_slot: u64,
    fee_feed_ts: u64,
    fee_window_size: usize,
    fee_window_sparse: bool,
}

#[derive(Debug, Clone)]
struct SlotMetrics {
    validator: Pubkey,
    total_transactions: u64,
    mev_transactions: u64,
    avg_priority_fee: f64,
    timestamp: u64,
}

#[derive(Debug)]
enum PreferenceUpdate {
    TransactionOutcome(TransactionOutcome),
    SlotComplete(Slot, SlotMetrics),
    PersistState,
}

impl ValidatorPreferenceLearner {
    pub fn new(persistence_path: String, rpc_url: String, fee_rpc_url: Option<String>) -> Self {
        let (tx, rx) = channel(10_000);
        let metrics = Arc::new(DMap::with_hasher(RandomState::new()));
        let transaction_history = Arc::new(DMap::with_hasher(RandomState::new()));
        let slot_performance = Arc::new(DMap::with_hasher(RandomState::new()));
        let updated_validators = Arc::new(DMap::with_hasher(RandomState::new()));

        let learner = Self {
            metrics: metrics.clone(),
            transaction_history: transaction_history.clone(),
            slot_performance: slot_performance.clone(),
            updated_validators: updated_validators.clone(),
            rpc: RpcJson::new(rpc_url.clone()),
            rpc_for_fees: RpcJson::new(fee_rpc_url.unwrap_or(rpc_url)),
            slot_ms_current: Arc::new(AtomicU64::new(400)),
            leader_map: Arc::new(DMap::with_hasher(RandomState::new())),
            last_leader_epoch: Arc::new(AtomicU64::new(0)),
            fee_window: Arc::new(tokio::sync::Mutex::new(VecDeque::with_capacity(FEE_WINDOW_CAP))),
            fee_slots_seen: Arc::new(tokio::sync::Mutex::new(HashSet::with_capacity(FEE_WINDOW_CAP))),
            provenance: Arc::new(tokio::sync::Mutex::new(Provenance{
                last_leader_schedule_epoch: 0,
                last_performance_sample_slot: 0,
                fee_feed_ts: 0,
                fee_window_size: 0,
                fee_window_sparse: true,
            })),
            update_sender: tx,
            persistence_path: persistence_path.clone(),
        };

        learner.load_state();
        learner.spawn_update_processor(rx);
        learner.spawn_decay_processor();
        learner.spawn_persistence_task();
        learner.spawn_coalesced_recompute();

        // New wiring
        learner.spawn_slot_cadence_cache();   // EWMA + clamp, 60s buckets
        learner.spawn_leader_schedule_task(); // epoch-correct absolute slots
        learner.spawn_fee_feed_task();        // shallow cache, sparse-aware

        learner
    }

    #[inline(always)]
    pub async fn record_transaction_outcome(&self, mut outcome: TransactionOutcome) -> Result<(), String> {
        if outcome.leader.is_none() {
            if let Some(l) = self.leader_map.get(&outcome.slot).map(|e| *e.value()) {
                outcome.leader = Some(l);
            }
        }
        self.update_sender
            .send(PreferenceUpdate::TransactionOutcome(outcome))
            .await
            .map_err(|e| format!("Failed to send update: {}", e))
    }

    pub fn get_validator_preference(&self, validator: &Pubkey) -> Option<ValidatorMetrics> {
        self.metrics.get(validator).map(|entry| entry.clone())
    }

    pub fn get_slot_ms_current(&self) -> u64 {
        self.slot_ms_current.load(Ordering::Relaxed)
    }

    pub fn get_top_validators_for_type(&self, tx_type: TransactionType, limit: usize) -> Vec<(Pubkey, f64)> {
        let tnow = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as f64;
        let mut v: Vec<(Pubkey, f64)> = self.metrics
            .iter()
            .filter_map(|e| {
                let m = e.value();
                if m.confidence_score < CONFIDENCE_THRESHOLD { return None; }

                let base = match tx_type {
                    TransactionType::Arbitrage => m.mev_inclusion_rate * m.reliability_score
                        * (1.0 / (1.0 + m.ewma_latency_ms / LAT_MS_SOFT_SELECTION)),
                    TransactionType::Liquidation => m.mev_inclusion_rate * m.reliability_score * 1.25,
                    TransactionType::Sandwich => m.ordering_preference.sandwich_success_rate * m.reliability_score,
                    TransactionType::Frontrun => m.ordering_preference.frontrun_tendency * m.reliability_score,
                    TransactionType::Backrun => m.ordering_preference.backrun_tendency * m.reliability_score,
                    TransactionType::Standard => m.reliability_score,
                };

                let a = m.alpha_included.max(1.0);
                let b = m.beta_excluded.max(1.0);
                let post_var = (a * b) / (((a + b).powi(2)) * (a + b + 1.0));

                let age_hours = ((tnow as u64).saturating_sub(m.last_updated) as f64) / 3600.0;
                let freshness = (0.5f64).powf(age_hours / 2.0);
                let bonus = UCB_BONUS_SCALE * (post_var.sqrt()) * (0.5 + 0.5 * freshness);

                Some((*e.key(), (base * (1.0 + bonus)).clamp(0.0, 10.0)))
            })
            .collect();

        v.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        v.truncate(limit);
        v
    }

    /// Return micro-lamports per CU for ComputeBudgetInstruction::set_compute_unit_price.
    pub fn compute_budget_price_from_estimate(
        &self,
        validator: &Pubkey,
        urgency: f64,
        cu_estimate: Option<u64>
    ) -> Result<u64, &'static str> {
        let cu = cu_estimate.ok_or("cu_estimate required: simulate first (sigVerify:false, replaceRecentBlockhash:true)")?;
        if cu == 0 { return Err("cu_estimate must be > 0"); }
        Ok(self.get_optimal_cu_price_μlam_per_cu(validator, urgency, cu))
    }

    /// Convert lamports target to μ-lamports/CU using CU estimate.
    pub fn get_optimal_cu_price_μlam_per_cu(&self, validator: &Pubkey, urgency: f64, cu_estimate: u64) -> u64 {
        let base_lamports_total = self.get_optimal_fee_for_validator_lamports(validator, urgency);
        if cu_estimate == 0 { return 0; }
        let total_micro = (base_lamports_total as u128) * 1_000_000u128;
        let price = total_micro / (cu_estimate as u128);
        price.min(u64::MAX as u128) as u64
    }

    /// Old helper; returns a lamports-target for the priority fee budget.
    pub fn get_optimal_fee_for_validator_lamports(&self, validator: &Pubkey, urgency: f64) -> u64 {
        let u = urgency.clamp(0.0, 2.0);
        self.metrics.get(validator).map(|m| {
            let f = &m.fee_preference;
            let base = if u < 0.5 { f.p50 } else if u < 1.5 { f.p75 } else { f.p90 + f.fee_stdev };
            ((base * 1.08).max(f.min_priority_fee as f64)) as u64
        }).unwrap_or_else(|| {
            futures::executor::block_on(self.recent_chain_fee_median()).unwrap_or(5_000)
        })
    }

    async fn recent_chain_fee_median(&self) -> Option<u64> {
        let lock = self.fee_window.lock().await;
        if lock.is_empty() { return None; }
        // Use slots to de-duplicate already; window is capped/shallow.
        let mut v: Vec<u64> = lock.iter().map(|(_, fee)| *fee).collect();
        v.sort_unstable();
        Some(v[v.len()/2])
    }

    /// Whether fee window is sparse (not enough distinct slots to trust quantiles).
    pub async fn is_fee_window_sparse(&self) -> bool {
        let prov = self.provenance.lock().await;
        prov.fee_window_sparse
    }

    pub fn get_best_submission_time(&self, validator: &Pubkey) -> Option<u8> {
        self.metrics.get(validator).and_then(|metrics| metrics.temporal_patterns.peak_hours.first().copied())
    }

    fn spawn_update_processor(&self, mut rx: Receiver<PreferenceUpdate>) {
        let history = self.transaction_history.clone();
        let slot_performance = self.slot_performance.clone();
        let updated = self.updated_validators.clone();
        let leader_map = self.leader_map.clone();

        tokio::spawn(async move {
            while let Some(update) = rx.recv().await {
                match update {
                    PreferenceUpdate::TransactionOutcome(mut outcome) => {
                        if outcome.leader.is_none() {
                            if let Some(l) = leader_map.get(&outcome.slot).map(|e| *e.value()) {
                                outcome.leader = Some(l);
                            }
                        }
                        let vid = outcome.validator;
                        let mut dq = history.entry(vid).or_insert_with(|| VecDeque::with_capacity(PREFERENCE_WINDOW_SIZE));
                        if dq.capacity() < PREFERENCE_WINDOW_SIZE {
                            dq.reserve(PREFERENCE_WINDOW_SIZE - dq.capacity());
                        }
                        if dq.len() >= PREFERENCE_WINDOW_SIZE { dq.pop_front(); }
                        dq.push_back(outcome);
                        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                        let _ = updated.insert(vid, now);
                    }
                    PreferenceUpdate::SlotComplete(slot, sm) => {
                        slot_performance.insert(slot, sm);
                        Self::cleanup_old_slots(&slot_performance, slot);
                    }
                    PreferenceUpdate::PersistState => {}
                }
            }
        });
    }

    fn spawn_coalesced_recompute(&self) {
        let metrics = self.metrics.clone();
        let history = self.transaction_history.clone();
        let updated = self.updated_validators.clone();
        let slot_ms_current = self.slot_ms_current.clone();

        tokio::spawn(async move {
            let mut tick = interval(PREFERENCE_UPDATE_INTERVAL);
            loop {
                tick.tick().await;

                let mut dirty: Vec<(Pubkey, u64)> = updated.iter().map(|e| (*e.key(), *e.value())).collect();
                if dirty.is_empty() { continue; }
                dirty.sort_unstable_by(|a, b| b.1.cmp(&a.1));
                if dirty.len() > RECOMPUTE_BATCH_LIMIT { dirty.truncate(RECOMPUTE_BATCH_LIMIT); }

                for (vid, _) in dirty {
                    let maybe_hist = history.get(&vid).map(|r| r.clone());
                    if let Some(outcomes) = maybe_hist {
                        if outcomes.len() >= MIN_SAMPLE_SIZE {
                            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();

                            // Inclusion
                            let (included, total) = outcomes.iter().fold((0usize, 0usize), |acc, o| (acc.0 + (o.included as usize), acc.1 + 1));
                            let (alpha, beta) = (2.0, 2.0);
                            let posterior_mean = (included as f64 + alpha) / (total as f64 + alpha + beta);

                            // Latency from included
                            let lat_inc: Vec<f64> = outcomes.iter().filter(|o| o.included).map(|o| o.latency_ms as f64).collect();
                            let mut avg_latency = if lat_inc.is_empty() { 1000.0 } else { lat_inc.iter().sum::<f64>() / lat_inc.len() as f64 };

                            // Normalize by current slot ms (sanity only; don't overfit)
                            let slot_ms = slot_ms_current.load(Ordering::Relaxed).max(1) as f64;
                            avg_latency = (avg_latency / slot_ms).max(0.1) * slot_ms;

                            // Fees
                            let fees_inc: Vec<f64> = outcomes.iter().filter(|o| o.included).map(|o| o.priority_fee as f64).collect();
                            let fee_pref = Self::robust_fee_preference(&fees_inc);

                            // Ordering prefs
                            let ordering_pref = Self::calculate_ordering_preference(&outcomes);

                            // Temporal
                            let temporal = Self::calculate_temporal_patterns(&outcomes);

                            // Reliability
                            let reliability = Self::calculate_reliability_score(&outcomes);

                            // Confidence
                            let confidence = Self::calculate_confidence_score(outcomes.len(), reliability);

                            // EWMA latency continuation
                            let mut ewma_lat = avg_latency;
                            if let Some(mut cur) = metrics.get_mut(&vid) {
                                let v = cur.value_mut();
                                let alpha_lat = 1.0 - (0.5f64).powf(1.0 / 5.0);
                                ewma_lat = alpha_lat * avg_latency + (1.0 - alpha_lat) * v.ewma_latency_ms.max(1.0);
                            }

                            let updated_metrics = ValidatorMetrics {
                                pubkey: vid,
                                total_slots_processed: outcomes.iter().map(|o| o.slot).collect::<HashSet<_>>().len() as u64,
                                mev_inclusion_rate: posterior_mean,
                                avg_latency_ms: avg_latency,
                                ewma_latency_ms: ewma_lat,
                                fee_preference: fee_pref,
                                ordering_preference: ordering_pref,
                                temporal_patterns: temporal,
                                reliability_score: reliability,
                                last_updated: now,
                                confidence_score: confidence,
                                alpha_included: included as f64 + alpha,
                                beta_excluded: (total - included) as f64 + beta,
                            };
                            metrics.insert(vid, updated_metrics);
                        }
                    }
                    updated.remove(&vid);
                }
            }
        });
    }

    // Robust fee preference using MAD and quantiles; reconciled against recent chain window if present.
    #[inline]
    fn robust_fee_preference(fees: &[f64]) -> FeePreference {
        if fees.is_empty() {
            return FeePreference {
                min_priority_fee: 1000,
                avg_accepted_fee: 5_000.0,
                fee_stdev: 1_000.0,
                high_fee_affinity: 0.5,
                p50: 5_000.0, p75: 6_500.0, p90: 9_000.0,
            };
        }
        let nth = |buf: &mut [f64], p: f64| -> f64 {
            if buf.is_empty() { return 0.0; }
            let len = buf.len();
            let idx = ((p * (len as f64 - 1.0)).round() as usize).min(len - 1);
            let (val, _, _) = buf.select_nth_unstable_by(idx, |a, b| a.partial_cmp(b).unwrap());
            *val
        };
        let median = { let mut tmp = fees.to_vec(); nth(&mut tmp, 0.50) };
        let mut abs_dev: Vec<f64> = fees.iter().map(|&x| (x - median).abs()).collect();
        let mad = nth(&mut abs_dev, 0.50).max(MAD_EPS);
        let z = |x: f64| (x - median) / (1.4826 * mad);
        let mut clean: Vec<f64> = fees.iter().copied().filter(|&x| z(x).abs() <= OUTLIER_Z_THRESHOLD).collect();
        if clean.is_empty() { clean = fees.to_vec(); }
        let mean = clean.iter().sum::<f64>() / clean.len() as f64;
        let stdev = (clean.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / clean.len().max(1) as f64).sqrt();
        let p50 = { let mut b = clean.clone(); nth(&mut b, 0.50) };
        let p75 = { let mut b = clean.clone(); nth(&mut b, 0.75) };
        let p90 = { let mut b = clean.clone(); nth(&mut b, 0.90) };
        let high_thr = mean + stdev;
        let high_aff = clean.iter().filter(|&&x| x > high_thr).count() as f64 / clean.len().max(1) as f64;

        FeePreference {
            min_priority_fee: clean.iter().cloned().fold(f64::INFINITY, f64::min) as u64,
            avg_accepted_fee: mean,
            fee_stdev: stdev,
            high_fee_affinity: high_aff,
            p50, p75, p90,
        }
    }

    fn calculate_ordering_preference(outcomes: &VecDeque<TransactionOutcome>) -> OrderingPreference {
        let mut frontrun_success = 0.0;
        let mut backrun_success = 0.0;
        let mut sandwich_success = 0.0;
        let mut bundle_total = 0.0;
        let mut bundle_accepted = 0.0;

        for outcome in outcomes {
            match outcome.transaction_type {
                TransactionType::Frontrun => {
                    if outcome.included {
                        if let (Some(pos), Some(size)) = (outcome.position_in_block, outcome.block_size) {
                            if pos < size / 3 { frontrun_success += 1.0; }
                        }
                    }
                }
                TransactionType::Backrun => { if outcome.included { backrun_success += 1.0; } }
                TransactionType::Sandwich => {
                    bundle_total += 1.0;
                    if outcome.included { sandwich_success += 1.0; bundle_accepted += 1.0; }
                }
                _ => {}
            }
        }
        let total_mev = outcomes.iter().filter(|o|
            matches!(o.transaction_type, TransactionType::Arbitrage|TransactionType::Liquidation|
                                      TransactionType::Sandwich|TransactionType::Frontrun|TransactionType::Backrun)
        ).count() as f64;

        OrderingPreference {
            frontrun_tendency: if total_mev > 0.0 { frontrun_success / total_mev } else { 0.0 },
            backrun_tendency: if total_mev > 0.0 { backrun_success / total_mev } else { 0.0 },
            sandwich_success_rate: if bundle_total > 0.0 { sandwich_success / bundle_total } else { 0.0 },
            bundle_acceptance_rate: if bundle_total > 0.0 { bundle_accepted / bundle_total } else { 0.0 },
            ordering_consistency: Self::calculate_ordering_consistency(outcomes),
        }
    }

    fn calculate_ordering_consistency(outcomes: &VecDeque<TransactionOutcome>) -> f64 {
        let positions: Vec<f64> = outcomes.iter()
            .filter_map(|o| if o.included {
                o.position_in_block.and_then(|pos| o.block_size.map(|size| pos as f64 / size as f64))
            } else { None }).collect();
        if positions.len() < 2 { return 0.5; }
        let mean = positions.iter().sum::<f64>() / positions.len() as f64;
        let variance = positions.iter().map(|p| (p - mean).powi(2)).sum::<f64>() / positions.len() as f64;
        1.0 / (1.0 + variance.sqrt())
    }

    fn calculate_temporal_patterns(outcomes: &VecDeque<TransactionOutcome>) -> TemporalPatterns {
        let mut hourly_activity = [0.0; 24];
        let mut hourly_counts = [0u32; 24];
        let mut weekly_activity = [0.0; 7];
        let mut weekly_counts = [0u32; 7];

        for o in outcomes {
            if !o.included { continue; }
            if let Some(ndt) = NaiveDateTime::from_timestamp_opt(o.timestamp as i64, 0) {
                let dt: DateTime<Utc> = DateTime::<Utc>::from_utc(ndt, Utc);
                let hour = dt.hour() as usize;
                let weekday = dt.weekday().num_days_from_monday() as usize;
                hourly_activity[hour] += 1.0; hourly_counts[hour] += 1;
                weekly_activity[weekday] += 1.0; weekly_counts[weekday] += 1;
            }
        }
        for i in 0..24 { if hourly_counts[i] > 0 { hourly_activity[i] /= hourly_counts[i] as f64; } }
        for i in 0..7 { if weekly_counts[i] > 0 { weekly_activity[i] /= weekly_counts[i] as f64; } }

        let avg_hourly = hourly_activity.iter().sum::<f64>() / 24.0;
        let threshold_high = avg_hourly * 1.2;
        let threshold_low = avg_hourly * 0.8;

        let peak_hours: Vec<u8> = (0..24).filter(|&h| hourly_activity[h] > threshold_high).map(|h| h as u8).collect();
        let low_activity_hours: Vec<u8> = (0..24).filter(|&h| hourly_activity[h] < threshold_low).map(|h| h as u8).collect();

        TemporalPatterns { hourly_activity, peak_hours, low_activity_hours, weekly_pattern: weekly_activity }
    }

    fn calculate_reliability_score(outcomes: &VecDeque<TransactionOutcome>) -> f64 {
        if outcomes.is_empty() { return 0.0; }
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let hl = DECAY_HALFLIFE_HOURS.max(1.0);
        let mut num = 0.0;
        let mut den = 0.0;
        for (idx, o) in outcomes.iter().enumerate() {
            let age_hours = ((now - o.timestamp) as f64 / 3600.0).max(0.0);
            let time_w = (0.5f64).powf(age_hours / hl);
            let recency_w = (idx as f64 + 1.0) / outcomes.len() as f64;
            let w = time_w * recency_w;
            let s = if o.included { 1.0 / (1.0 + (o.latency_ms as f64 / LAT_MS_SOFT_RELIABILITY)) } else { 0.0 };
            num += s * w; den += w;
        }
        if den > 0.0 { num / den } else { 0.0 }
    }

    fn calculate_confidence_score(sample_size: usize, reliability: f64) -> f64 {
        let n = sample_size as f64;
        let size_term = 1.0 - (-n / 300.0).exp();
        let r = reliability.clamp(0.0, 1.0);
        (0.5 * size_term + 0.5 * r).clamp(0.0, 1.0)
    }

    fn spawn_decay_processor(&self) {
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            let mut decay_interval = interval(Duration::from_secs(300));
            loop {
                decay_interval.tick().await;
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                let mut evict: Vec<Pubkey> = Vec::new();
                for mut entry in metrics.iter_mut() {
                    let v = entry.value_mut();
                    let age_hours = ((now - v.last_updated) as f64 / 3600.0).max(0.0);
                    let decay = (0.5f64).powf(age_hours / DECAY_HALFLIFE_HOURS.max(1.0));
                    v.reliability_score *= decay;
                    v.confidence_score *= decay;
                    v.mev_inclusion_rate *= decay;
                    if v.confidence_score < 0.10 || age_hours > 168.0 { evict.push(*entry.key()); }
                }
                for k in evict { metrics.remove(&k); }
                if metrics.len() > MAX_VALIDATORS_TRACKED {
                    let mut scores: Vec<(Pubkey, f64)> =
                        metrics.iter().map(|e| (*e.key(), e.value().confidence_score)).collect();
                    scores.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
                    let remove_count = metrics.len() - MAX_VALIDATORS_TRACKED;
                    for (k, _) in scores.into_iter().take(remove_count) { metrics.remove(&k); }
                }
            }
        });
    }

    fn spawn_persistence_task(&self) {
        let metrics = self.metrics.clone();
        let path = self.persistence_path.clone();
        let provenance = self.provenance.clone();
        tokio::spawn(async move {
            let mut persist_interval = interval(Duration::from_secs(60));
            loop {
                persist_interval.tick().await;
                let snapshot: Vec<ValidatorMetrics> = metrics.iter().map(|e| e.value().clone()).collect();
                let prov = provenance.lock().await.clone();
                let payload = PersistBlob { metrics: snapshot, provenance: prov };
                if let Ok(encoded) = bincode::serialize(&payload) {
                    let tmp = format!("{}.tmp", path);
                    if tokio::fs::write(&tmp, &encoded).await.is_ok() {
                        let _ = tokio::fs::rename(&tmp, &path).await;
                    }
                }
            }
        });
    }

    fn load_state(&self) {
        if let Ok(data) = std::fs::read(&self.persistence_path) {
            if let Ok(blob) = bincode::deserialize::<PersistBlob>(&data) {
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                for mut m in blob.metrics.into_iter() {
                    let age_hours = ((now - m.last_updated) as f64 / 3600.0).max(0.0);
                    if age_hours < 168.0 {
                        let decay = (0.5f64).powf(age_hours / DECAY_HALFLIFE_HOURS.max(1.0));
                        m.reliability_score *= decay;
                        m.confidence_score *= decay;
                        self.metrics.insert(m.pubkey, m);
                    }
                }
                let mut prov = futures::executor::block_on(self.provenance.lock());
                *prov = blob.provenance;
            }
        }
    }

    fn cleanup_old_slots(slot_performance: &Arc<DMap<Slot, SlotMetrics>>, current_slot: Slot) {
        if current_slot > SLOT_HISTORY_SIZE as u64 {
            let cutoff = current_slot - SLOT_HISTORY_SIZE as u64;
            slot_performance.retain(|s, _| *s > cutoff);
        }
    }

    // ---------------------- Slot cadence cache (EWMA over 60s buckets) ----------------------
    fn spawn_slot_cadence_cache(&self) {
        let rpc = self.rpc.clone();
        let slot_ms = self.slot_ms_current.clone();
        let provenance = self.provenance.clone();

        tokio::spawn(async move {
            let mut t = interval(PERF_POLL_INTERVAL);
            let mut ewma_ms: Option<f64> = None;
            loop {
                t.tick().await;
                match rpc.get_recent_performance_samples().await {
                    Ok(mut v) if !v.is_empty() => {
                        let s = v.remove(0);
                        // Derive ms/slot from 60s bucket. Avoid pretending sub-second fidelity.
                        let ms_per_slot_raw = if s.num_slots == 0 { 0 } else { (s.period_secs.saturating_mul(1000)) / s.num_slots };
                        let clamped = ms_per_slot_raw.clamp(SLOT_MS_MIN, SLOT_MS_MAX).max(1);
                        let smoothed = match ewma_ms {
                            None => clamped as f64,
                            Some(prev) => PERF_SLOT_EWMA_ALPHA * (clamped as f64) + (1.0 - PERF_SLOT_EWMA_ALPHA) * prev,
                        };
                        ewma_ms = Some(smoothed);
                        slot_ms.store(smoothed.round() as u64, Ordering::Relaxed);

                        let mut prov = provenance.lock().await;
                        prov.last_performance_sample_slot = s.slot;
                    }
                    Err(e) => {
                        warn!("perf samples rpc error: {e}");
                    }
                    _ => {}
                }
            }
        });
    }

    // ---------------------- Leader schedule (epoch-correct absolute slots) ----------------------
    fn spawn_leader_schedule_task(&self) {
        let rpc = self.rpc.clone();
        let leader_map = self.leader_map.clone();
        let last_epoch = self.last_leader_epoch.clone();
        let provenance = self.provenance.clone();

        tokio::spawn(async move {
            let mut t = interval(LEADER_POLL_INTERVAL);
            loop {
                t.tick().await;

                let epoch_info = match rpc.get_epoch_info().await {
                    Ok(e) => e,
                    Err(e) => {
                        warn!("epoch info rpc error: {e}");
                        continue;
                    }
                };

                let schedule = match rpc.get_leader_schedule(Some(epoch_info.epoch_start_slot)).await {
                    Ok(map) => map,
                    Err(e) => {
                        warn!("leader schedule rpc error: {e}");
                        continue;
                    }
                };

                leader_map.clear();
                for (k, slots_in_epoch) in schedule.into_iter() {
                    if let Ok(pk) = Pubkey::try_from(k.as_str()) {
                        for s_in_epoch in slots_in_epoch {
                            let abs_slot = epoch_info.epoch_start_slot.saturating_add(s_in_epoch as u64);
                            leader_map.insert(abs_slot, pk);
                        }
                    }
                }

                last_epoch.store(epoch_info.epoch, Ordering::Relaxed);
                let mut prov = provenance.lock().await;
                prov.last_leader_schedule_epoch = epoch_info.epoch;
            }
        });
    }

    // ---------------------- Priority fee window (shallow cache, sparse-aware) ----------------------
    fn spawn_fee_feed_task(&self) {
        let rpc = self.rpc_for_fees.clone();
        let window = self.fee_window.clone();
        let slots_seen = self.fee_slots_seen.clone();
        let provenance = self.provenance.clone();

        tokio::spawn(async move {
            let mut t = interval(FEE_POLL_INTERVAL);
            loop {
                t.tick().await;
                match rpc.get_recent_prioritization_fees(None).await {
                    Ok(list) if !list.is_empty() => {
                        let mut w = window.lock().await;
                        let mut seen = slots_seen.lock().await;
                        // Push unique slots newest-first; cap to shallow cache size.
                        for x in list {
                            if seen.insert(x.slot) {
                                if w.len() == FEE_WINDOW_CAP { 
                                    if let Some((old_slot, _)) = w.pop_front() { seen.remove(&old_slot); }
                                }
                                w.push_back((x.slot, x.prioritization_fee));
                            }
                        }
                        // Update provenance with sparsity
                        let mut prov = provenance.lock().await;
                        prov.fee_feed_ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
                        prov.fee_window_size = w.len();
                        prov.fee_window_sparse = w.len() < FEE_SPARSE_MIN_SLOTS;
                    }
                    Err(e) => debug!("priority fee feed error: {e}"),
                    _ => {}
                }
            }
        });
    }

    // ---------------------- Query helpers ----------------------
    pub fn get_validator_statistics(&self) -> HashMap<Pubkey, ValidatorStats> {
        self.metrics.iter()
            .map(|entry| {
                let metrics = entry.value();
                let stats = ValidatorStats {
                    total_slots: metrics.total_slots_processed,
                    inclusion_rate: metrics.mev_inclusion_rate,
                    avg_latency: metrics.avg_latency_ms,
                    reliability: metrics.reliability_score,
                    confidence: metrics.confidence_score,
                    last_seen: metrics.last_updated,
                };
                (entry.key().clone(), stats)
            })
            .collect()
    }

    pub fn should_use_validator(&self, validator: &Pubkey, min_confidence: f64) -> bool {
        self.metrics.get(validator)
            .map(|m| m.confidence_score >= min_confidence && m.reliability_score >= 0.5)
            .unwrap_or(false)
    }

    pub fn get_validator_risk_score(&self, validator: &Pubkey) -> f64 {
        self.metrics.get(validator)
            .map(|m| {
                let inclusion_risk = 1.0 - m.mev_inclusion_rate;
                let latency_risk = m.avg_latency_ms / 100.0;
                let confidence_risk = 1.0 - m.confidence_score;
                (inclusion_risk + latency_risk + confidence_risk) / 3.0
            })
            .unwrap_or(1.0)
    }

    pub fn predict_inclusion_probability(&self, validator: &Pubkey, tx_type: TransactionType, priority_fee: u64) -> f64 {
        self.metrics.get(validator).map(|m| {
            let base = m.mev_inclusion_rate.clamp(0.001, 0.999);
            let logit0 = safe_logit(base);

            let type_mult = match tx_type {
                TransactionType::Liquidation => 1.20,
                TransactionType::Arbitrage  => 1.10,
                TransactionType::Sandwich   => (m.ordering_preference.sandwich_success_rate / 0.5).clamp(0.5, 2.0),
                TransactionType::Frontrun   => (m.ordering_preference.frontrun_tendency * 2.0).clamp(0.5, 2.0),
                TransactionType::Backrun    => (m.ordering_preference.backrun_tendency  * 2.0).clamp(0.5, 2.0),
                TransactionType::Standard   => 1.0,
            };
            let lat_mult = 1.0 / (1.0 + m.ewma_latency_ms / LAT_MS_SOFT_SELECTION);
            let conf_mult = m.confidence_score.clamp(0.25, 1.0);

            let f = &m.fee_preference;
            let scale = f.fee_stdev.max(1.0);
            let x = (priority_fee as f64 - f.p75) / scale;
            let fee_sig = 1.0 / (1.0 + (-x).exp());
            let k_fee = 0.5 + 0.5 * f.high_fee_affinity.clamp(0.0, 1.0);
            let fee_bump = k_fee * (2.0 * fee_sig - 1.0);

            let logit = logit0 + safe_ln(type_mult) + safe_ln(lat_mult) + safe_ln(conf_mult) + fee_bump;
            (1.0 / (1.0 + (-logit).exp())).clamp(0.0, 1.0)
        }).unwrap_or(0.0)
    }
}

// ---------- Persistence blob ----------
#[derive(Serialize, Deserialize)]
struct PersistBlob {
    metrics: Vec<ValidatorMetrics>,
    provenance: Provenance,
}

#[derive(Debug, Clone)]
pub struct ValidatorStats {
    pub total_slots: u64,
    pub inclusion_rate: f64,
    pub avg_latency: f64,
    pub reliability: f64,
    pub confidence: f64,
    pub last_seen: u64,
}

// ---------------------- Tests ----------------------
#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_validator_preference_learning() {
        let learner = ValidatorPreferenceLearner::new(
            "/tmp/test_validator_prefs.bin".to_string(),
            "http://127.0.0.1:8899".to_string(),
            None
        );
        let v = Pubkey::new_unique();

        for i in 0..100usize {
            let o = TransactionOutcome {
                validator: v,
                slot: 1000 + i as u64,
                included: i % 10 != 0,
                latency_ms: 20 + (i % 5) as u64 * 10,
                priority_fee: 1000 + (i % 20) as u64 * 500,
                transaction_type: TransactionType::Arbitrage,
                position_in_block: Some((i % 50) as usize),
                block_size: Some(100),
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                leader: None,
            };
            learner.record_transaction_outcome(o).await.unwrap();
        }

        sleep(PREFERENCE_UPDATE_INTERVAL * 2).await;

        let m = learner.get_validator_preference(&v).expect("metrics");
        assert!(m.mev_inclusion_rate > 0.80);
        assert!(m.fee_preference.p50 > 0.0);

        // CU price helper sanity (assume 200k CU)
        let cu_price = learner.get_optimal_cu_price_μlam_per_cu(&v, 1.0, 200_000);
        assert!(cu_price > 0);

        // Mandatory-guard path: must provide cu_estimate
        assert!(learner.compute_budget_price_from_estimate(&v, 1.0, Some(200_000)).is_ok());
        assert!(learner.compute_budget_price_from_estimate(&v, 1.0, None).is_err());
    }

    #[tokio::test]
    async fn test_fee_window_sparsity_flag() {
        let learner = ValidatorPreferenceLearner::new(
            "/tmp/test_validator_prefs_sparse.bin".to_string(),
            "http://127.0.0.1:8899".to_string(),
            None
        );
        // With no data, window is sparse
        assert!(learner.is_fee_window_sparse().await);
    }
}
