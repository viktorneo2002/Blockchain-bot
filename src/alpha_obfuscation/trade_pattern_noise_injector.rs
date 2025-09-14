// (moved UPGRADE-D/E/G block below imports; see later in file)
// ======================[UPGRADE: imports tuned for perf + safety]======================
// [UPGRADE] parking_lot RwLock (faster, no poisoning), atomic utils, precise time.
#![allow(unused_imports)]
use solana_client::rpc_client::RpcClient;
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    ed25519_instruction,
    secp256k1_instruction,
    pubkey::Pubkey,
    hash::Hash as SolHash,
    signature::{Keypair, Signer, Signature},
    system_instruction,
    message::{VersionedMessage, v0::Message as V0Message, v0::MessageAddressTableLookup},
    transaction::{Transaction, VersionedTransaction},
};

use std::collections::{BTreeMap, HashMap, VecDeque, BinaryHeap};
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, AtomicBool, AtomicI64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::collections::HashSet;
use std::sync::mpsc::{channel, TryRecvError};
use parking_lot::Mutex;
use std::thread;

use parking_lot::RwLock;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use serde::{Deserialize, Serialize};
use anyhow::Result;
use statrs::function::logistic::logistic;
use siphasher::sip::SipHasher13;
use ahash::AHashMap;
use hmac::{Hmac, Mac};
use sha2::Sha256;
type HmacSha256 = Hmac<Sha256>;
use lru::LruCache;
use smallvec::SmallVec;
use ordered_float::OrderedFloat;
use dashmap::DashMap;
use std::hash::{Hash, Hasher};
use spl_memo as memo_prog;

// Alias for fast map; now AHashMap for ultra-low-latency hashing
type FastMap<K, V> = AHashMap<K, V>;

// ======================[UPGRADE-9 wiring: local module paths]=======================
// We scope module declarations to this file only to avoid editing crate root.
#[path = "../time9/mod.rs"] mod time9;            // exposes time9::sync_clock
#[path = "../pipeline9/mod.rs"] mod pipeline9;    // exposes pipeline9::mpsc_ring
#[path = "../analytics9/mod.rs"] mod analytics9;  // exposes analytics9::prob_calibrator
#[path = "../execution9/mod.rs"] mod execution9;  // exposes execution9::preflight_cache
#[path = "../network9/mod.rs"] mod network9;      // exposes network9::relay_entropy_quota
#[path = "../scheduling9/mod.rs"] mod scheduling9;// exposes scheduling9::leader_prefetch
#[path = "../security9/mod.rs"] mod security9;    // exposes security9::guardrails
#[path = "../planning9/mod.rs"] mod planning9;    // exposes planning9::iso_spend_controller

use once_cell::sync::Lazy;
use time9::sync_clock::{PhaseSync, SyncCfg};
use analytics9::prob_calibrator::Calibrator;
use execution9::preflight_cache::{PreflightCache, Preflight, route_key};
use network9::relay_entropy_quota::{EntropyCfg, EntropyState};
use scheduling9::leader_prefetch::{prefetch as leader_prefetch, PrefetchCfg as LeaderPrefetchCfg, PrefetchPlan as LeaderPrefetchPlan};
use security9::guardrails::{RailCfg, sane_tip, sane_mu, SlotCounter};
use planning9::iso_spend_controller::{SpendCfg, SpendState};

// ======================[UPGRADE-9 A) Slot-phase clock sync]=========================
static SYNC: Lazy<RwLock<PhaseSync>> = Lazy::new(|| {
    RwLock::new(PhaseSync::new(SyncCfg { window: 21, clusters: 3, target_slot_ms: 400 }))
});

// ======================[UPGRADE-A: Phase-accurate slot clock]=======================
use std::sync::atomic::{AtomicU64, Ordering};
use parking_lot::RwLock as PlRwLockAliasJustForDocs;
#[derive(Clone, Copy)]
struct SlotClk { slot_ms: u64, slot_start_ns: u64, cur_slot: u64 }
impl Default for SlotClk { fn default() -> Self { Self { slot_ms: 400, slot_start_ns: mono_ns(), cur_slot: 0 } } }
static SLOTCLK: Lazy<RwLock<SlotClk>> = Lazy::new(|| RwLock::new(SlotClk::default()));
static CUR_SLOT_ATOM: AtomicU64 = AtomicU64::new(0);

#[inline] fn mono_ns() -> u64 { now_ns() as u64 }

/// Call on every PubSub slot update (wired below)
#[inline]
pub fn on_slot_update(new_slot: u64) {
    let mut c = SLOTCLK.write();
    if new_slot != c.cur_slot {
        c.cur_slot = new_slot;
        c.slot_start_ns = mono_ns();
        CUR_SLOT_ATOM.store(new_slot, Ordering::Relaxed);
    }
}

/// Milliseconds elapsed since the start of the current slot (phase-local)
#[inline]
fn now_ms_into_slot_local() -> i64 {
    let c = SLOTCLK.read();
    let dt_ns = mono_ns().saturating_sub(c.slot_start_ns);
    (dt_ns / 1_000_000).min(c.slot_ms) as i64
}

/// Returns the current slot length in ms
#[inline]
pub fn slot_len_ms() -> u64 { SLOTCLK.read().slot_ms }

#[inline]
pub fn sync_observe_leader_phase(leader_phase_ms: i64) {
    let local = now_ms_into_slot_local();
    SYNC.write().observe(leader_phase_ms, local);
}

#[inline]
pub fn adjusted_phase_ms() -> i64 {
    let local = now_ms_into_slot_local();
    SYNC.read().adjusted_phase_ms(local)
}

// Example usage (kept as comments to avoid unresolved symbols here):
// let adj = SYNC.read().adjusted_phase_ms(now_ms_into_slot_local());
// sleep_until_slot_phase(adj as u64)?;

// ======================[UPGRADE-9 B) Lock-free ring for build queue]================
use pipeline9::mpsc_ring::Ring;
// Note: We do not declare a static BUILD_Q here to avoid referencing unknown types like BundlePlan.
// Example pattern:
// static BUILD_Q: Lazy<Arc<Ring<BundlePlan>>> = Lazy::new(|| Ring::new(2048));
// let pushed = BUILD_Q.try_push(plan);
// if !pushed { /* drop lowest-ev or apply backpressure */ }
// while let Some(plan) = BUILD_Q.try_pop() { build_and_submit(plan)?; }

// ======================[UPGRADE-9 C) Probability calibration]=======================
static CAL: Lazy<RwLock<Calibrator>> = Lazy::new(|| RwLock::new(Calibrator::new(10, 1.5, 1.5)));

#[inline]
pub fn cal_observe(raw_p: f64, included: bool) { CAL.write().observe(raw_p, included); }
#[inline]
pub fn cal_apply(raw_p: f64) -> f64 { CAL.read().calibrate(raw_p) }

// ======================[UPGRADE-9 D) Preflight cache]===============================
static PREF: Lazy<RwLock<PreflightCache>> = Lazy::new(|| RwLock::new(PreflightCache::new(4096)));

#[inline]
pub fn preflight_get(program_ids: &[[u8; 32]], params_blob: &[u8]) -> Option<Preflight> {
    let key = route_key(program_ids, params_blob);
    PREF.read().get(key).cloned()
}

#[inline]
pub fn preflight_put(program_ids: &[[u8; 32]], params_blob: &[u8], cu: u32, write_keys: Vec<[u8; 32]>, ts_slot: u64) {
    let key = route_key(program_ids, params_blob);
    PREF.write().put(key, Preflight { cu, write_keys, ts_slot });
}

#[inline]
pub fn preflight_gc(keep_after_slot: u64) { PREF.write().gc_slot(keep_after_slot); }

// ======================[UPGRADE-9 E) Entropy quota per relay]=======================
// Callers should size RELAY_COUNT appropriately and index safely.
pub const DEFAULT_ENTROPY_CFG: EntropyCfg = EntropyCfg { slot_quota_bits: 128, min_bits_per_msg: 16 };
static RELAY_ENTROPY: Lazy<RwLock<Vec<EntropyState>>> = Lazy::new(|| RwLock::new(Vec::new()));

#[inline]
pub fn init_relay_entropy_states(relay_count: usize) { *RELAY_ENTROPY.write() = vec![EntropyState::new(); relay_count.max(1)]; }

#[inline]
pub fn relay_entropy_allow(relay_idx: usize, cfg: EntropyCfg, slot: u64) -> bool {
    let mut v = RELAY_ENTROPY.write();
    if relay_idx >= v.len() { return false; }
    v[relay_idx].allow(cfg, slot)
}

// ======================[UPGRADE-9 F) Leader prefetch]===============================
#[inline]
pub fn leader_prefetch_plan(leaders_ring: &[[u8; 32]], cur_idx: usize, lookahead: usize) -> LeaderPrefetchPlan {
    leader_prefetch(leaders_ring, cur_idx, LeaderPrefetchCfg { lookahead })
}

// ======================[UPGRADE-9 G) Guardrails]====================================
static COUNTER: Lazy<RwLock<SlotCounter>> = Lazy::new(|| RwLock::new(SlotCounter::default()));

#[inline]
pub fn rails_allow_submit(rails: RailCfg, current_slot: u64, final_tip_lamports: i64, mu_per_cu: u64) -> bool {
    if !sane_tip(final_tip_lamports, rails) || !sane_mu(mu_per_cu, rails) { return false; }
    COUNTER.write().allow(rails, current_slot)
}

// ======================[UPGRADE-9 H) Iso-spend controller]==========================
static SSTATE: Lazy<RwLock<SpendState>> = Lazy::new(|| RwLock::new(SpendState::new()));

#[inline]
pub fn spend_allow(slot_cap: i64, current_slot: u64, plan_cost: i64) -> bool {
    SSTATE.write().allow(SpendCfg { slot_cap }, current_slot, plan_cost)
}

// Compute budget helpers
#[inline]
pub fn cu_budget_instrs(cu_limit: u32, micro_lamports_per_cu: u64) -> [Instruction; 2] {
    [
        ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
        ComputeBudgetInstruction::set_compute_unit_price(micro_lamports_per_cu),
    ]
}

// ======================[PIECE 141: CB cache with loaded cap]=========================
#[derive(Clone)]
pub struct CbCacheExt { inner: Arc<DashMap<(u32,u64,u32,u32), Vec<Instruction>>> }
impl Default for CbCacheExt { fn default() -> Self { Self { inner: Arc::new(DashMap::new()) } } }
impl CbCacheExt {
    pub fn get(&self, cu: u32, price: u64, heap: u32, loaded_cap: u32) -> Vec<Instruction> {
        if let Some(v) = self.inner.get(&(cu,price,heap,loaded_cap)) { return v.clone(); }
        let mut v = Vec::with_capacity(4);
        v.push(ComputeBudgetInstruction::set_compute_unit_limit(cu));
        v.push(ComputeBudgetInstruction::set_compute_unit_price(price));
        if heap > 0 { v.push(ComputeBudgetInstruction::set_heap_frame(heap)); }
        if loaded_cap > 0 { v.push(ComputeBudgetInstruction::set_loaded_accounts_data_size_limit(loaded_cap)); }
        self.inner.insert((cu,price,heap,loaded_cap), v.clone());
        v
    }
}

// ======================[PIECE 142: BlockhashFeeder v2]===============================
pub struct BlockhashFeeder {
    rpc: Vec<Arc<RpcClient>>,
    cur_hash: Arc<RwLock<SolHash>>,
    cur_slot: Arc<AtomicU64>,
    min_life_slots: u64,
}
impl BlockhashFeeder {
    pub fn new(rpcs: Vec<Arc<RpcClient>>, min_life_slots: u64) -> Self {
        Self { rpc: rpcs, cur_hash: Arc::new(RwLock::new(SolHash::default())), cur_slot: Arc::new(AtomicU64::new(0)), min_life_slots }
    }
    pub fn start_slot_ws(&self, ws_url: &str) -> anyhow::Result<()> {
        let sw = SlotWatcher::spawn(ws_url)?;
        let cur = self.cur_slot.clone();
        std::thread::spawn(move || loop {
            let s = sw.cur_slot.load(Ordering::Relaxed);
            cur.store(s, Ordering::Relaxed);
            // fuse into slot-phase clock
            on_slot_update(s);
            std::thread::sleep(Duration::from_millis(2));
        });
        Ok(())
    }
    pub fn refresh_loop(&self) {
        let rpcs = self.rpc.clone(); let cur = self.cur_hash.clone(); let slot = self.cur_slot.clone();
        std::thread::spawn(move || {
            let mut backoff_ms = 50u64;
            loop {
                let mut got: Option<(SolHash,u64)> = None;
                for r in &rpcs {
                    if let Ok(x) = r.get_latest_blockhash_with_commitment(CommitmentConfig::processed()) {
                        got = Some((x.0, x.1.context.slot));
                        break;
                    }
                }
                if let Some((h,s)) = got {
                    *cur.write() = h;
                    slot.store(s, Ordering::Relaxed);
                    backoff_ms = 50;
                } else {
                    backoff_ms = (backoff_ms.saturating_mul(2)).min(800);
                }
                std::thread::sleep(Duration::from_millis(backoff_ms));
            }
        });
    }
    #[inline] pub fn fetch(&self) -> (SolHash, u64) { (*self.cur_hash.read(), self.cur_slot.load(Ordering::Relaxed)) }
    #[inline] pub fn fresh(&self, now_slot: u64, bh_slot: u64) -> bool { now_slot.saturating_sub(bh_slot) < self.min_life_slots.saturating_sub(4) }
}

// ======================[PIECE 143: bucket cool-down across slots]====================
pub struct BucketCooldown { last: Arc<RwLock<FastMap<u64, u64>>>, gap_slots: u64 }
impl BucketCooldown { pub fn new(gap_slots: u64)->Self{ Self { last:Arc::new(RwLock::new(FastMap::default())), gap_slots } } pub fn admit(&self, bucket: u64, cur_slot: u64)->bool{ let mut m = self.last.write(); if let Some(&s)=m.get(&bucket){ if cur_slot <= s.saturating_add(self.gap_slots){ return false; } } m.insert(bucket, cur_slot); true } }

// ======================[PIECE 144: RPC/TPU pre-warm keepalives]======================
pub struct Prewarm { rpcs: Vec<Arc<RpcClient>>, tpu: Arc<TpuClient> }
impl Prewarm { pub fn new(rpcs: Vec<Arc<RpcClient>>, tpu: Arc<TpuClient>)->Self{ Self{ rpcs, tpu } } pub fn start(&self) { let r = self.rpcs.clone(); let t = self.tpu.clone(); std::thread::spawn(move || loop { for c in &r { let _ = c.get_health(); } let _ = t.try_send_transaction(VersionedTransaction{ signatures: vec![], message: VersionedMessage::V0(V0Message::default()) }); std::thread::sleep(Duration::from_secs(2)); }); } }

// ======================[PIECE 145: effective μ/CU]===================================
#[inline]
pub fn effective_micro_per_cu(priority_micro: u64, tip_lamports: u64, est_cu: u32) -> u64 {
    priority_micro.saturating_add(((tip_lamports as u128) * 1_000_000u128 / (est_cu.max(1) as u128)) as u64)
}

// ======================[PIECE 146: dynamic heap sizing]==============================
impl TipModel { pub fn heap_for_route(&self, route: Option<RouteKey>) -> u32 { let mut h = 64*1024u32; if let Some(k)=route { if let Some(e)=self.route_bytes_ewma.get(&k) { h = ((e*1.6) as u32).clamp(32*1024, 256*1024); } } h } }

// ======================[PIECE 147: pre-sim loaded cap tuning]========================
pub fn precheck_loaded_cap(rpc: &RpcClient, vtx: &VersionedTransaction, base_cap: u32) -> u32 {
    let cfg = solana_client::rpc_config::RpcSimulateTransactionConfig{ sig_verify:false, replace_recent_blockhash:true, commitment:Some(CommitmentConfig::processed()), ..solana_client::rpc_config::RpcSimulateTransactionConfig::default() };
    if let Ok(res)=rpc.simulate_transaction_with_config(vtx, cfg) { if let Some(logs)=res.value.logs { let joined = logs.join("|"); if joined.contains("max loaded accounts data size exceeded") { return (base_cap as f64 * 1.25) as u32; } } }
    base_cap
}

// ======================[PIECE 148: EV gate per slot]=================================
pub fn should_emit_tail(tm: &TipModel, tps: f64, iso: &IsoCurve) -> bool {
    let p_head = iso.price_for_target(0.90, tm.p2.value() as u64);
    let p_tail = iso.price_for_target(0.70, tm.p2.value() as u64);
    if tps > 3500.0 && (p_tail.saturating_sub(p_head) as i64) < 150 { return false; }
    true
}

// ======================[PIECE 149: lock-free-ish status bus]=========================
use ringbuf::HeapRb;
pub struct StatusBus { rb: Arc<(crossbeam_utils::Backoff, parking_lot::Mutex<HeapRb<(Signature,String,Instant)>>)> }
impl StatusBus { pub fn new(cap: usize)->Self{ Self{ rb:Arc::new((crossbeam_utils::Backoff::new(), parking_lot::Mutex::new(HeapRb::new(cap)))) } } pub fn push(&self, item: (Signature,String,Instant)) { let (_,rb) = &*self.rb; let mut r = rb.lock(); let _ = r.push(item); } pub fn drain_into<F: FnMut((Signature,String,Instant))>(&self, mut f: F) { let (_,rb)=&*self.rb; let mut r=rb.lock(); while let Some(x)=r.pop() { f(x); } } }

// ======================[PIECE 150: LD micro phase jitter]============================
impl TradePatternNoiseInjector { pub fn phase_with_ld(&self, base: f64) -> f64 { let mut h = self.halton.write(); let j = (h.next_dim(31) - 0.5) * 0.06; (base + j).clamp(0.10, 0.90) } }

// ======================[Reality Check Wiring Helpers]===============================
/// Lift IsoCurve landing target under congestion to avoid waste.
#[inline]
pub fn iso_target_for_tps(tps: f64) -> f64 {
    if tps > 3500.0 { 0.93 } else if tps > 2500.0 { 0.90 } else { 0.88 }
}

/// Choose bundle width conservatively: cap at 4 unless evidence of low conflicts.
#[inline]
pub fn bundle_width_for(sr_head: f64) -> usize {
    let w = optimal_bundle_width(sr_head.max(0.75), 0.12, 8, 0.04);
    w.min(4).max(1)
}

/// Decoy policy tied to effective μ/CU and venue SR. Heavy decoys only when frontier is cheap and venue is healthy.
#[derive(Clone, Copy, Debug)]
pub struct DecoyPolicy;
impl DecoyPolicy {
    /// Whether heavy decoys (ed25519/secp) are allowed near the head.
    #[inline]
    pub fn allow_heavy(effective_micro_per_cu: u64, venue_sr: f64) -> bool {
        effective_micro_per_cu < 700 && venue_sr >= 0.92
    }
    /// Recommended decoy probability for light decoys (e.g., memo-only) based on frontier and SR.
    /// Returns a probability in [0, 0.2].
    #[inline]
    pub fn prob(effective_micro_per_cu: u64, venue_sr: f64) -> f64 {
        if effective_micro_per_cu > 900 || venue_sr < 0.85 { 0.0 }
        else if effective_micro_per_cu > 700 { if venue_sr >= 0.88 { 0.10 } else { 0.05 } }
        else { if venue_sr >= 0.92 { 0.20 } else { 0.10 } }
    }
}

// ======================[PIECE 151: Leader-keyed IsoCurve store]======================
#[derive(Default, Clone)]
pub struct LeaderIso { m: Arc<RwLock<FastMap<Pubkey, IsoCurve>>> }
impl LeaderIso {
    pub fn new()->Self{ Self{ m:Arc::new(RwLock::new(FastMap::default())) } }
    #[inline] pub fn observe(&self, leader:&Pubkey, price:u64, landed:bool){ let mut g=self.m.write(); let e=g.entry(*leader).or_insert_with(IsoCurve::default); e.observe(price, landed); }
    #[inline] pub fn price_for(&self, leader:&Pubkey, target:f64, robust:u64)->u64{ self.m.read().get(leader).map(|c| c.price_for_target(target, robust)).unwrap_or(robust) }
}

// ======================[PIECE 152: prebuilt retry ladder]============================
pub fn build_retry_ladder(
    cb_cache: &CbCacheExt,
    vtx_builder: &VtxBuilder,
    route_ixs: &[Instruction],
    recent_bh: SolHash,
    fee_payer: &Keypair,
    cu_limit: u32,
    heap: u32,
    base_loaded_cap: u32,
    base_price: u64,
    bumps: &[u64],
) -> Vec<VersionedTransaction> {
    let mut out = Vec::with_capacity(bumps.len());
    for p in bumps {
        let mut ixs = cb_cache.get(cu_limit, *p, heap, base_loaded_cap);
        ixs.extend_from_slice(route_ixs);
        let vtx = vtx_builder.build_v0(ixs, recent_bh, fee_payer, true);
        out.push(vtx);
    }
    out
}

// ======================[PIECE 153: RPC error-class gate helpers]=====================
#[derive(Copy,Clone,Eq,PartialEq)]
enum RpcErrKind { Stale, Congested, NodeBehind, Unknown }
fn classify_rpc_err(e: &str) -> RpcErrKind {
    if e.contains("blockhash not found") || e.contains("min context slot not satisfied") { RpcErrKind::Stale }
    else if e.contains("Too many requests") || e.contains("Transaction would exceed max") { RpcErrKind::Congested }
    else if e.contains("Node is behind") || e.contains("slot was skipped") { RpcErrKind::NodeBehind }
    else { RpcErrKind::Unknown }
}
impl RpcHedger {
    pub fn note_error(&self, i: usize, err: &str, current_slot: u64, floors: &Arc<Vec<AtomicU64>>, cooloffs: &Arc<Vec<AtomicU64>>) {
        match classify_rpc_err(err) {
            RpcErrKind::Stale | RpcErrKind::NodeBehind => {
                cooloffs[i].store(current_slot.saturating_add(8), Ordering::Relaxed);
                let f = floors[i].load(Ordering::Relaxed);
                floors[i].store(f.saturating_add(2), Ordering::Relaxed);
            }
            RpcErrKind::Congested => { cooloffs[i].store(current_slot.saturating_add(2), Ordering::Relaxed); }
            _ => {}
        }
    }
}

// ======================[PIECE 154: LUT coverage optimizer]===========================
pub struct LutOptimizer { pubs: Vec<(Pubkey, Vec<Pubkey>)> }
impl LutOptimizer {
    pub fn new(pairs: Vec<(Pubkey, Vec<Pubkey>)>) -> Self { let v = pairs.into_iter().filter(|(_,v)| !v.is_empty()).collect(); Self { pubs: v } }
    pub fn from_rpc(rpc:&RpcClient, keys:&[Pubkey])->Self {
        use solana_address_lookup_table_program::state::AddressLookupTable;
        let mut v=Vec::new();
        for k in keys { if let Ok(acc)=rpc.get_account(k) { if let Ok(state)=AddressLookupTable::deserialize(&acc.data) { v.push((*k, state.addresses().to_vec())); } } }
        Self::new(v)
    }
    pub fn best_for_ixs(&self, ixs:&[Instruction], max_tables:usize)->Vec<MessageAddressTableLookup>{
        type FastSet<T> = std::collections::HashSet<T, ahash::RandomState>;
        let mut need_ro: FastSet<Pubkey> = FastSet::default();
        let mut need_wr: FastSet<Pubkey> = FastSet::default();
        for ix in ixs { for a in &ix.accounts { if a.is_writable { need_wr.insert(a.pubkey); } else { need_ro.insert(a.pubkey); } } }
        let mut scored: Vec<(usize, usize)> = self.pubs.iter().enumerate().map(|(i,(_,addrs))|{
            let mut c=0usize; for pk in addrs { if need_wr.contains(pk) || need_ro.contains(pk) { c+=1; } } (i,c)
        }).collect();
        scored.sort_by(|a,b| b.1.cmp(&a.1));
        let mut out = Vec::new();
        for (i,_c) in scored.into_iter().take(max_tables.max(1)) {
            let (table, addrs)=&self.pubs[i]; let mut w_idx=Vec::new(); let mut r_idx=Vec::new();
            for (j,pk) in addrs.iter().enumerate() { let u=j as u8; if need_wr.contains(pk) { w_idx.push(u); } else if need_ro.contains(pk) { r_idx.push(u); } }
            if !(w_idx.is_empty() && r_idx.is_empty()) { out.push(MessageAddressTableLookup{ account_key:*table, writable_indexes:w_idx, readonly_indexes:r_idx }); }
        }
        out
    }
}

// ======================[PIECE 155: slot-end safety guard]============================
pub fn guard_slot_tail(now: Instant, slot_len_ms: u64, slot_start: Instant, rtt_p90_ms: u64, margin_ms: u64) -> bool {
    let elapsed = now.duration_since(slot_start).as_millis() as u64; let left = slot_len_ms.saturating_sub(elapsed.min(slot_len_ms));
    left > rtt_p90_ms.saturating_add(margin_ms)
}

// ======================[PIECE 156: bundle write-amp limiter]=========================
fn collect_writable_set(ixs: &[Instruction]) -> std::collections::HashSet<Pubkey, ahash::RandomState> {
    let mut s: std::collections::HashSet<Pubkey, ahash::RandomState> = std::collections::HashSet::default();
    for ix in ixs { for a in &ix.accounts { if a.is_writable { s.insert(a.pubkey); } } }
    s
}
pub fn split_by_writable_union(mut batches: Vec<Vec<Instruction>>, max_union: usize) -> Vec<Vec<Instruction>> {
    let mut out = Vec::new(); let mut cur: Vec<Vec<Instruction>> = Vec::new();
    let mut ws: std::collections::HashSet<Pubkey, ahash::RandomState> = std::collections::HashSet::default();
    for ixs in batches.drain(..) {
        let wset = collect_writable_set(&ixs);
        let proj = wset.iter().filter(|p| !ws.contains(p)).count();
        if ws.len()+proj > max_union && !cur.is_empty() { out.push(cur); cur = Vec::new(); ws.clear(); }
        ws.extend(&wset); cur.push(ixs);
    }
    if !cur.is_empty() { out.push(cur); }
    out
}

// ======================[PIECE 157: Tip PI controller]================================
#[derive(Clone)]
pub struct TipPI { target_lp: f64, k_p: f64, k_i: f64, integ: f64, clamp: (f64,f64) }
impl TipPI { pub fn new(target_lp:f64)->Self{ Self{ target_lp, k_p:0.50, k_i:0.02, integ:0.0, clamp:(-0.5,0.5) } } pub fn step(&mut self, observed_lp:f64, base_bps:u16)->u16{ let err = (self.target_lp - observed_lp).clamp(-0.5,0.5); self.integ = (self.integ + err).clamp(self.clamp.0, self.clamp.1); let mul = (1.0 + self.k_p*err + self.k_i*self.integ).clamp(0.70, 1.30); ((base_bps as f64)*mul) as u16 } }

// ======================[PIECE 158: vote-accounts health gate]========================
pub struct VoteHealth { pub min_stake: u64 }
impl VoteHealth { pub fn allow(&self, rpc:&RpcClient, leader:&Pubkey)->bool { if let Ok(v)=rpc.get_vote_accounts() { if let Some(a)=v.current.iter().find(|x| x.node_pubkey==leader.to_string() || x.vote_pubkey==leader.to_string()){ let stake = a.activated_stake; if a.delinquent || stake < self.min_stake { return false; } } } true } }

// ======================[PIECE 159: pacer overflow check]=============================
impl QuicPacer { #[inline] pub fn would_wait_ns(&self, need_bytes: usize) -> i64 { let now = now_ns(); let last = self.last_ref.load(Ordering::Relaxed); let dt = (now - last).max(0); let rate = self.rate_bytes_per_s.load(Ordering::Relaxed).max(1); let cap = self.bucket_bytes.load(Ordering::Relaxed); let add = (dt as i128 * rate as i128 / 1_000_000_000i128) as i64; let tokens = (cap + add).max(0); let deficit = (need_bytes as i64) - tokens; deficit.max(0) * 1_000_000_000i64 / rate } }

// ======================[PIECE 160: price hysteresis filter]==========================
pub struct Hysteresis { up: u64, down: u64, last: AtomicU64 }
impl Hysteresis { pub fn new(up:u64, down:u64, seed:u64)->Self{ Self{ up, down, last: AtomicU64::new(seed) } } pub fn filter(&self, proposed:u64) -> u64 { let cur = self.last.load(Ordering::Relaxed); let out = if proposed > cur && proposed-cur >= self.up { proposed } else if proposed < cur && cur - proposed >= self.down { proposed } else { cur }; self.last.store(out, Ordering::Relaxed); out } }

// ======================[PIECE 121: Isotonic landing curve (PAVA)]====================
#[derive(Clone, Default)]
pub struct IsoCurve { pub bins: Vec<(u64 /*price*/, u64 /*hits*/, u64 /*fills*/)> }
impl IsoCurve {
    #[inline] pub fn observe(&mut self, price: u64, landed: bool) {
        let b = (price / 50) * 50;
        match self.bins.binary_search_by_key(&b, |x| x.0) {
            Ok(i) => { let (p,h,f) = self.bins[i]; self.bins[i] = (p, h+1, f + (landed as u64)); }
            Err(i) => self.bins.insert(i, (b, 1, landed as u64)),
        }
        if self.bins.len() > 192 { let ( _p, h, f) = self.bins.remove(0); if let Some(x) = self.bins.get_mut(0) { x.1 += h; x.2 += f; } }
        self.pava();
    }
    fn pava(&mut self) {
        let n = self.bins.len(); if n == 0 { return; }
        let mut y: Vec<f64> = self.bins.iter().map(|(_,h,f)| (*f as f64)/(*h as f64)).collect();
        let mut w: Vec<f64> = self.bins.iter().map(|(_,h,_)| *h as f64).collect();
        let mut i = 0;
        while i+1 < n {
            if y[i] > y[i+1] {
                let mut j = i;
                let mut num = y[i]*w[i] + y[i+1]*w[i+1];
                let mut den = w[i] + w[i+1];
                let mut avg = num/den; y[i] = avg; y[i+1] = avg; w[i] = den; w[i+1] = den;
                while j > 0 && y[j-1] > y[j] {
                    num = y[j-1]*w[j-1] + y[j]*w[j]; den = w[j-1] + w[j]; avg = num/den; y[j-1] = avg; y[j] = avg; w[j-1] = den; w[j] = den; j -= 1;
                }
            }
            i += 1;
        }
        for k in 0..n { let h = self.bins[k].1.max(1) as f64; self.bins[k].2 = (y[k]*h).round() as u64; }
    }
    pub fn price_for_target(&self, target_p: f64, robust_fallback: u64) -> u64 {
        if self.bins.len() < 4 { return robust_fallback; }
        for (p, h, f) in &self.bins { let phat = (*f as f64)/(*h as f64); if phat >= target_p { return *p; } }
        robust_fallback
    }
}

// ======================[PIECE 122: bundle fee budget allocator]======================
pub fn allocate_bundle_prices(seeds: &[u64], max_mul: f64, budget_lamports: u64, step: u64, iso: &IsoCurve) -> Vec<u64> {
    let n = seeds.len(); let mut p: Vec<u64> = seeds.to_vec(); let mut spent: u64 = 0;
    let mut mq: BinaryHeap<(OrderedFloat<f64>, usize)> = BinaryHeap::new();
    for i in 0..n { let cur = p[i]; let cap = ((cur as f64) * max_mul).floor() as u64; if cap > cur {
        let gain = prob(iso, cur.saturating_add(step)) - prob(iso, cur); mq.push((OrderedFloat(gain.max(0.0)), i)); } }
    while let Some((_g, i)) = mq.pop() {
        let cap = ((seeds[i] as f64) * max_mul).floor() as u64;
        if p[i] + step > cap { continue; }
        if spent + step > budget_lamports { break; }
        let _before = prob(iso, p[i]); p[i] += step; let _after = prob(iso, p[i]); spent += step;
        let next_gain = prob(iso, (p[i] + step).min(cap)) - prob(iso, p[i]); if next_gain > 0.0 { mq.push((OrderedFloat(next_gain), i)); }
    }
    p
}
#[inline] fn prob(iso: &IsoCurve, price: u64) -> f64 {
    if iso.bins.is_empty() { return 0.5; }
    for (b,h,f) in &iso.bins { if *b >= price { return (*f as f64)/(*h as f64); } }
    let ( _b, h, f) = *iso.bins.last().unwrap(); (f as f64)/(h as f64)
}

// ======================[PIECE 123: adaptive stagger synthesis]=======================
fn synthesize_staggers(stats: &[(f64 /*rtt*/, f64 /*sr*/)], base_ms: u64) -> Vec<u64> {
    let mut scored: Vec<(usize, f64)> = stats.iter().enumerate().map(|(i,(r,s))| (i, (s.max(0.6))/r.max(1.0))).collect();
    scored.sort_by(|a,b| b.1.partial_cmp(&a.1).unwrap());
    let m = scored.len() as f64; let mut out = vec![0u64; stats.len()];
    for (rank, (i,_)) in scored.into_iter().enumerate() { let frac = rank as f64 / m.max(1.0); let delay = (base_ms as f64 * (frac*frac*3.0)).round() as u64; out[i] = delay; }
    out
}

// ======================[PIECE 124: QUIC pacer (token bucket)]========================
pub struct QuicPacer { rate_bytes_per_s: AtomicI64, bucket_bytes: AtomicI64, last_ref: AtomicI64 }
impl QuicPacer {
    pub fn new(rate_bps: u64) -> Self { Self { rate_bytes_per_s: AtomicI64::new((rate_bps/8) as i64), bucket_bytes: AtomicI64::new(((rate_bps/8)/50) as i64), last_ref: AtomicI64::new(now_ns()) } }
    #[inline] pub fn admit(&self, need_bytes: usize) { loop { let now = now_ns(); let last = self.last_ref.load(Ordering::Relaxed); let dt = (now - last).max(0); let rate = self.rate_bytes_per_s.load(Ordering::Relaxed).max(1); let add = (dt as i128 * rate as i128 / 1_000_000_000i128) as i64; let cap = self.bucket_bytes.load(Ordering::Relaxed); let mut tokens = (cap.min(cap + add)).max(0); if self.last_ref.compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed).is_ok() { if tokens >= need_bytes as i64 { tokens -= need_bytes as i64; self.bucket_bytes.store(tokens, Ordering::Relaxed); break; } } std::thread::sleep(Duration::from_micros(200)); } }
}
#[inline] fn now_ns() -> i64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64 }

// ======================[PIECE 125: slot watcher via WS]==============================
pub struct SlotWatcher { pub cur_slot: Arc<AtomicU64> }
impl SlotWatcher { pub fn spawn(ws_url: &str) -> anyhow::Result<Self> { use solana_client::pubsub_client::PubsubClient; let (sub, _h) = PubsubClient::slot_subscribe(ws_url)?; let cur_slot = Arc::new(AtomicU64::new(0)); let c2 = cur_slot.clone(); std::thread::spawn(move || { for upd in sub { c2.store(upd.slot, Ordering::Relaxed); on_slot_update(upd.slot); } }); Ok(Self { cur_slot }) } }

// ======================[PIECE 126: ComputeBudget ix cache]===========================
#[derive(Clone)]
pub struct CbCache { inner: Arc<DashMap<(u32,u64,u32), Vec<Instruction>>> }
impl Default for CbCache { fn default() -> Self { Self { inner: Arc::new(DashMap::new()) } } }
impl CbCache { pub fn get(&self, cu: u32, price: u64, heap: u32) -> Vec<Instruction> { if let Some(v) = self.inner.get(&(cu,price,heap)) { return v.clone(); } let mut v = Vec::with_capacity(3); v.push(ComputeBudgetInstruction::set_compute_unit_limit(cu)); v.push(ComputeBudgetInstruction::set_compute_unit_price(price)); if heap > 0 { v.push(ComputeBudgetInstruction::set_heap_frame(heap)); } self.inner.insert((cu,price,heap), v.clone()); v } }

// ======================[PIECE 127: per-RPC context-slot floors]======================
impl RpcHedger {
    pub fn send_hedged_vtx_ctxvec(&self, vtx: &VersionedTransaction, timeout_ms: u64, floors: Vec<u64>) -> anyhow::Result<Signature> {
        let wire = encode_vtx_into_tls(vtx);
        let (tx_sig, rx_sig) = channel::<(usize, anyhow::Result<Signature>, u128)>();
        for (i, (c, st, gate)) in self.clients.iter().enumerate() {
            let c = c.clone(); let stats = st.clone(); let gate = gate.clone(); let bytes = wire.clone(); let tx_sig2 = tx_sig.clone();
            let mut cfg = self.cfg; cfg.min_context_slot = floors.get(i).cloned();
            std::thread::spawn(move || {
                if !gate.try_enter() { return; }
                let t0 = Instant::now(); let res = c.send_raw_transaction_with_config(bytes.as_slice(), cfg); let dt = t0.elapsed().as_millis();
                { let mut s = stats.lock(); s.obs(res.is_ok(), dt as f64); }
                gate.leave(); let _ = tx_sig2.send((i, res.map_err(|e| anyhow::anyhow!(e.to_string())), dt));
            });
        }
        let deadline = Instant::now() + Duration::from_millis(timeout_ms.max(100));
        loop { match rx_sig.try_recv() { Ok((_i, Ok(sig), _)) => return Ok(sig), Ok((_i, Err(_), _)) => {}, Err(TryRecvError::Empty) => {}, Err(TryRecvError::Disconnected) => break, } if Instant::now() >= deadline { break; } std::thread::sleep(Duration::from_millis(2)); }
        Err(anyhow::anyhow!("hedged send timed out"))
    }
}

// ======================[PIECE 128: slot-phase calibrator]============================
#[derive(Default, Clone)]
pub struct PhaseCal { bias: Arc<RwLock<f64>> }
impl PhaseCal { pub fn observe(&self, planned_phase: f64, landed_at_ms: u64, slot_ms: u64) { let actual = (landed_at_ms as f64 / slot_ms as f64).clamp(0.0, 1.0); let err = (actual - planned_phase).clamp(-0.5, 0.5); let mut b = self.bias.write(); *b = (*b*0.9) + (err*0.1); } pub fn phase(&self, base: f64) -> f64 { (base - *self.bias.read()).clamp(0.10, 0.90) } }

// ======================[PIECE 129: rotating nonce farm]==============================
pub struct NonceFarm { lanes: Vec<NonceLane>, idx: AtomicUsize }
impl NonceFarm { pub fn new(lanes: Vec<NonceLane>) -> Self { Self { lanes, idx: AtomicUsize::new(0) } } pub fn pick(&self) -> &NonceLane { let i = self.idx.fetch_add(1, Ordering::Relaxed) % self.lanes.len().max(1); &self.lanes[i] } }

// ======================[PIECE 130: MTU-aware message sizer]==========================
pub struct MtuSizer { bins: [usize; 6] }
impl Default for MtuSizer { fn default() -> Self { Self { bins: [512,640,768,896,1024,1184] } } }
impl MtuSizer { pub fn pad_to_mtu(&self, vmsg: &VersionedMessage) -> Option<Instruction> { let cur_len = bincode::serialize(vmsg).unwrap().len(); let target = self.bins.iter().find(|&&b| b >= cur_len).copied(); if let Some(t) = target { let need = t.saturating_sub(cur_len); if need >= 16 { let mut bytes = vec![0u8; need]; for b in &mut bytes { *b = 0xCC; } return Some(Instruction { program_id: memo_prog::id(), accounts: vec![], data: bytes }); } } None } }

// ======================[PIECE 131: Hedger early-winner cancellation]=================
pub struct CancelToken(Arc<AtomicBool>);
impl CancelToken {
    pub fn new() -> Self { Self(Arc::new(AtomicBool::new(false))) }
    #[inline] pub fn cancel(&self) { self.0.store(true, Ordering::Relaxed); }
    #[inline] pub fn is_canceled(&self) -> bool { self.0.load(Ordering::Relaxed) }
    #[inline] pub fn clone(&self) -> Self { Self(self.0.clone()) }
}
impl RpcHedger {
    pub fn send_hedged_vtx_cancel(&self, vtx: &VersionedTransaction, timeout_ms: u64, min_ctx_slot: Option<u64>) -> anyhow::Result<Signature> {
        let wire = encode_vtx_into_tls(vtx);
        let (tx_sig, rx_sig) = channel::<(usize, anyhow::Result<Signature>, u128)>();
        let cancel = CancelToken::new();

        // order by score
        let mut idx: Vec<usize> = (0..self.clients.len()).collect();
        idx.sort_by(|&a, &b| {
            let sa = self.clients[a].1.lock(); let sb = self.clients[b].1.lock();
            (sa.sr / sa.rtt_ms.max(1.0)).partial_cmp(&(sb.sr / sb.rtt_ms.max(1.0))).unwrap().reverse()
        });
        // optional top-K breadth control (PIECE 135)
        let idx_full = idx.clone();
        idx = pick_topk(&idx_full, &self.clients, 3);

        // synthesized staggers (PIECE 123)
        let snapshot: Vec<(f64,f64)> = self.clients.iter().map(|(_,st,_)| { let s = st.lock(); (s.rtt_ms, s.sr) }).collect();
        let stag = synthesize_staggers(&snapshot, 10);

        for (rank, i) in idx.into_iter().enumerate() {
            let c = self.clients[i].0.clone();
            let stats = self.clients[i].1.clone();
            let gate  = self.clients[i].2.clone();
            let bytes = wire.clone(); let tx_sig2 = tx_sig.clone();
            let cancel_t = cancel.clone();
            let mut cfg = self.cfg; cfg.min_context_slot = min_ctx_slot;
            std::thread::spawn(move || {
                // cooperative pre-flight abort
                if cancel_t.is_canceled() || !gate.try_enter() { return; }
                let stagger = stag.get(i).copied().unwrap_or((rank as u64)*6);
                if stagger > 0 {
                    let step = 2u64; let mut waited = 0u64;
                    while waited < stagger {
                        if cancel_t.is_canceled() { gate.leave(); return; }
                        std::thread::sleep(Duration::from_millis(step)); waited += step;
                    }
                }
                if cancel_t.is_canceled() { gate.leave(); return; }
                let t0 = Instant::now(); let res = c.send_raw_transaction_with_config(bytes.as_slice(), cfg); let dt = t0.elapsed().as_millis();
                { let mut st = stats.lock().unwrap(); st.obs(res.is_ok(), dt as f64); }
                if res.is_ok() { cancel_t.cancel(); }
                gate.leave(); let _ = tx_sig2.send((i, res.map_err(|e| anyhow::anyhow!(e.to_string())), dt));
            });
        }
        let deadline = Instant::now() + Duration::from_millis(timeout_ms.max(100));
        loop {
            match rx_sig.try_recv() {
                Ok((_i, Ok(sig), _dt)) => return Ok(sig),
                Ok((_i, Err(_e), _dt)) => {},
                Err(TryRecvError::Empty) => {},
                Err(TryRecvError::Disconnected) => break,
            }
            if Instant::now() >= deadline { break; }
            thread::sleep(Duration::from_millis(2));
        }
        Err(anyhow::anyhow!("hedged send timed out"))
    }
}

// ======================[PIECE 132: P² quantile RTT tracker]==========================
#[derive(Clone)]
pub struct P2Quant { q: [f64;5], n: [f64;5], np: [f64;5], dn: [f64;5], init: usize }
impl P2Quant {
    pub fn new(p: [f64;3]) -> Self { Self { q: [0.0;5], n: [0.0,1.0,2.0,3.0,4.0], np: [0.0,0.0,0.0,0.0,0.0], dn: [0.0,p[0],p[1],p[2],1.0], init: 0 } }
    pub fn insert(&mut self, x: f64) {
        if self.init < 5 { self.q[self.init]=x; self.init+=1; if self.init==5 { self.q.sort_by(|a,b| a.partial_cmp(b).unwrap()); } return; }
        let k = if x < self.q[0] { self.q[0]=x; 0 } else if x < self.q[1] { 0 } else if x < self.q[2] { 1 } else if x < self.q[3] { 2 } else if x <= self.q[4] { 3 } else { self.q[4]=x; 3 };
        for i in (k+1)..5 { self.n[i]+=1.0; }
        self.np[1]+=self.dn[1]; self.np[2]+=self.dn[2]; self.np[3]+=self.dn[3];
        for i in 1..4 { let d = self.np[i]-self.n[i]; if (d>=1.0 && self.n[i+1]-self.n[i]>1.0) || (d<=-1.0 && self.n[i-1]-self.n[i]<-1.0) { let s = d.signum(); let qn = self.q[i] + s*( (self.q[i+1]-self.q[i])/(self.n[i+1]-self.n[i]) + (self.q[i-1]-self.q[i])/(self.n[i-1]-self.n[i]) ); self.q[i] = qn.clamp(self.q[i-1], self.q[i+1]); self.n[i]+=s; } }
    }
    #[inline] pub fn p50(&self)->f64{ self.q[2].max(1.0) }
    #[inline] pub fn p90(&self)->f64{ self.q[3].max(self.p50()) }
    #[inline] pub fn p95(&self)->f64{ self.q[4].max(self.p90()) }
}
#[derive(Clone)]
pub struct RttTrackerQ { p2: Arc<RwLock<P2Quant>> }
impl RttTrackerQ { pub fn new()->Self { Self{ p2: Arc::new(RwLock::new(P2Quant::new([0.5,0.9,0.95])))} } pub fn observe(&self, ms: f64){ self.p2.write().insert(ms.max(1.0)); } pub fn choose_phase(&self, congested: bool)->f64 { let p = self.p2.read(); let r = if congested { p.p90() } else { p.p50() }; let ms = r.clamp(5.0, 120.0); let g = (ms / 400.0).clamp(0.02,0.20); (0.55 - g).clamp(0.10,0.80) } pub fn choose_phase_capped(&self, congested: bool, slot_std_ms: f64) -> f64 { let base = self.choose_phase(congested); let cap = if slot_std_ms < 20.0 { 0.04 } else if slot_std_ms < 60.0 { 0.06 } else { 0.08 }; (base - cap).clamp(0.10, 0.80) } }

// ======================[PIECE 133: Leader hot-set gate]==============================
pub struct LeaderHotset { ok: Arc<RwLock<FastMap<Pubkey, (f64,f64)>>>, sr_thr: f64, rtt_thr: f64 }
impl LeaderHotset { pub fn new(sr_thr: f64, rtt_thr: f64)->Self{ Self{ ok:Arc::new(RwLock::new(FastMap::default())), sr_thr, rtt_thr } } pub fn update(&self, leader: Pubkey, sr: f64, rtt: f64){ self.ok.write().insert(leader,(sr,rtt)); } pub fn allow(&self, leader: &Pubkey)->bool{ if let Some((sr,rtt)) = self.ok.read().get(leader).copied(){ sr>=self.sr_thr && rtt<=self.rtt_thr } else { true } } }

// ======================[PIECE 134: per-route IsoCurve store]=========================
#[derive(Default, Clone)]
pub struct RouteIsoStore { inner: Arc<RwLock<FastMap<RouteKey, IsoCurve>>> }
impl RouteIsoStore {
    pub fn route_price(&self, route: Option<RouteKey>, target_p: f64, robust: u64)->u64{ if let Some(k)=route { if let Some(c)=self.inner.read().get(&k){ return c.price_for_target(target_p, robust); } } robust }
    pub fn observe_route_price(&self, route: Option<RouteKey>, price: u64, landed: bool){ if let Some(k)=route { let mut g = self.inner.write(); let e = g.entry(k).or_insert_with(IsoCurve::default); e.observe(price, landed); } }
}

// ======================[PIECE 135: pick top-K RPCs]==================================
fn pick_topk(indices: &[usize], clients: &[(Arc<RpcClient>, Arc<Mutex<RpcStats>>, RpcGate)], k: usize)->Vec<usize>{ let mut v = indices.to_vec(); v.sort_by(|&a,&b|{ let sa=clients[a].1.lock(); let sb=clients[b].1.lock(); (sa.sr/sa.rtt_ms.max(1.0)).partial_cmp(&(sb.sr/sb.rtt_ms.max(1.0))).unwrap().reverse() }); v.truncate(k.max(1)); v }

// ======================[PIECE 136: PubSub kill-switch]===============================
pub struct SigKill { m: Arc<DashMap<Signature, ()>> }
impl SigKill { pub fn new()->Self{ Self{ m:Arc::new(DashMap::new()) } } pub fn arm(&self, s: Signature){ self.m.insert(s,()); } pub fn tripped(&self, s: &Signature)->bool{ self.m.contains_key(s) } pub fn trip(&self, s:&Signature){ self.m.insert(*s,()); } }

// ======================[PIECE 137: IsoCurve slope-aware bump]========================
impl IsoCurve { pub fn bump_for_target(&self, cur: u64, target_p: f64, max_mul: f64)->u64 { if self.bins.len()<3 { return ((cur as f64)*1.10) as u64; } let mut next = cur; for (p,h,f) in &self.bins { let ph = (*f as f64)/(*h as f64); if *p >= cur && ph < target_p { next = *p; } } let cap = ((cur as f64)*max_mul) as u64; next.min(cap).max(cur + 25) } }

// ======================[PIECE 138: mode-seeking snap-up]=============================
pub fn snap_to_mode_above_hist<R: Rng>(hist: &FastMap<u64,u64>, step: u64, robust: u64, rng: &mut R)->u64 { if hist.is_empty(){ return robust; } let mut best=(0u64,0u64); for (bin,c) in hist.iter(){ if *bin>=robust && *c>best.1 { best=(*bin,*c); } } if best.1==0 { return robust; } best.0 + rng.gen_range(0..step.max(1)) }

// ======================[PIECE 139: per-leader phase calibrator]======================
pub struct LeaderPhase { m: Arc<RwLock<FastMap<Pubkey, PhaseCal>>> }
impl LeaderPhase { pub fn new()->Self{ Self{ m:Arc::new(RwLock::new(FastMap::default())) } } pub fn phase_for(&self, leader: &Pubkey, base: f64)->f64{ self.m.read().get(leader).map(|c| c.phase(base)).unwrap_or(base) } pub fn observe(&self, leader: &Pubkey, planned: f64, landed_ms: u64, slot_ms: u64){ let mut g = self.m.write(); let e = g.entry(*leader).or_insert_with(PhaseCal::default); e.observe(planned, landed_ms, slot_ms); } }

// ======================[PIECE 140: Micro-probe canary]===============================
pub fn build_canary(payer: &Keypair, bh: SolHash)->VersionedTransaction{
    use solana_sdk::{message::v0::Message as V0Message, compute_budget::ComputeBudgetInstruction as CBI};
    let ixs = vec![ CBI::set_compute_unit_limit(10_000), CBI::set_compute_unit_price(100), Instruction{ program_id: memo_prog::id(), accounts: vec![], data: vec![0xA5] }, ];
    let msg = V0Message::try_compile(&payer.pubkey(), &ixs, &[], bh).expect("compile canary");
    VersionedTransaction::try_new(VersionedMessage::V0(msg), &[payer]).expect("sign canary")
}


// ======================[PIECE 87: Tx de-dup and TTL guard]============================
#[derive(Clone)]
pub struct TxDeduper {
    inner: Arc<Mutex<LruCache<[u8;32], u64>>>, // msg hash -> expiry_slot
    ttl_slots: u64,
}
impl TxDeduper {
    pub fn new(capacity: usize, ttl_slots: u64) -> Self {
        let cap = NonZeroUsize::new(capacity.max(64)).unwrap();
        Self { inner: Arc::new(Mutex::new(LruCache::new(cap))), ttl_slots }
    }
    #[inline]
    pub fn msg_digest(message: &VersionedMessage) -> [u8;32] {
        let data = bincode::serialize(message).expect("serialize message");
        solana_sdk::hash::hash(&data).to_bytes()
    }
    /// Returns true if this tx is fresh (should be sent), false if duplicate within TTL.
    pub fn check_and_record(&self, vtx: &VersionedTransaction, current_slot: u64) -> bool {
        let key = Self::msg_digest(&vtx.message);
        let exp = current_slot.saturating_add(self.ttl_slots);
        let mut guard = self.inner.lock().unwrap();
        if let Some(&old_exp) = guard.get(&key) {
            if old_exp >= current_slot { return false; }
        }
        guard.put(key, exp);
        true
    }
}

// ======================[PIECE 88: v0 VersionedTx builder with LUT parity]============
pub struct VtxBuilder {
    payer: Pubkey,
    lookups: Option<Arc<Mutex<(Vec<MessageAddressTableLookup>, usize)>>>,
}
impl VtxBuilder {
    pub fn new(payer: Pubkey, luts: Vec<MessageAddressTableLookup>) -> Self {
        let lookups = if luts.is_empty() { None } else { Some(Arc::new(Mutex::new((luts, 0)))) };
        Self { payer, lookups }
    }
    /// Build a VersionedTransaction from ixs with optional LUT usage (parity).
    pub fn build_v0(&self,
                    ixs: Vec<Instruction>,
                    recent_blockhash: SolHash,
                    fee_payer_kp: &Keypair,
                    use_lut: bool) -> VersionedTransaction {
        if use_lut {
            if let Some(luts) = &self.lookups {
                let mut g = luts.lock().unwrap();
                let (ref vec_luts, ref mut idx) = *g;
                if !vec_luts.is_empty() {
                    let sel = vec_luts[*idx % vec_luts.len()].clone();
                    *idx = idx.wrapping_add(1);
                    if let Ok(msg) = V0Message::try_compile(&self.payer, &ixs, &[sel], recent_blockhash) {
                        let vmsg = VersionedMessage::V0(msg);
                        return VersionedTransaction::try_new(vmsg, &[fee_payer_kp]).expect("sign");
                    }
                }
            }
        }
        let msg = V0Message::try_compile(&self.payer, &ixs, &[], recent_blockhash).expect("compile");
        VersionedTransaction::try_new(VersionedMessage::V0(msg), &[fee_payer_kp]).expect("sign")
    }
}

// ======================[PIECE 89: slot-phase scheduler + RTT tracker]=================
#[derive(Clone, Default)]
pub struct RttTracker { ewma_ms: f64, alpha: f64 }
impl RttTracker {
    pub fn new() -> Self { Self { ewma_ms: 18.0, alpha: 0.25 } }
    pub fn observe(&mut self, ms: f64) { self.ewma_ms = self.alpha*ms + (1.0-self.alpha)*self.ewma_ms; }
    pub fn ms(&self) -> u64 { self.ewma_ms.clamp(4.0, 120.0) as u64 }
}
impl TradePatternNoiseInjector {
    pub fn schedule_at_slot_phase(&self, _current_slot: u64, phase: f64, rtt: &RttTracker) -> Instant {
        let st = self.slot_time.read();
        let slot_ms = st.slot_ms().max(200);
        let now = Instant::now();
        let since = now.duration_since(*self.last_trade_time.read());
        let ms_into_slot = (since.as_millis() as u64) % slot_ms;
        let target = (phase.clamp(0.10, 0.90) * slot_ms as f64) as u64;
        let mut wait = if target > ms_into_slot { target - ms_into_slot } else { slot_ms - (ms_into_slot - target) };
        wait = wait.saturating_sub(rtt.ms());
        now + Duration::from_millis(wait.min(slot_ms))
    }
}

// ======================[PIECE 90: adaptive hedger (weighted)]========================
#[derive(Clone, Default)]
struct RpcStats { rtt_ms: f64, sr: f64, a_rtt: f64, a_sr: f64 }
impl RpcStats { fn new() -> Self { Self { rtt_ms: 30.0, sr: 0.92, a_rtt: 0.25, a_sr: 0.15 } }
    fn obs(&mut self, ok: bool, rtt: f64) { self.rtt_ms = self.a_rtt*rtt + (1.0-self.a_rtt)*self.rtt_ms; self.sr = self.a_sr*(ok as u8 as f64) + (1.0-self.a_sr)*self.sr; } }

pub struct RpcHedger {
    clients: Vec<(Arc<RpcClient>, Arc<Mutex<RpcStats>>, RpcGate)>,
    cfg: RpcSendTransactionConfig,
}
impl RpcHedger {
    pub fn new_weighted(mut clients: Vec<Arc<RpcClient>>) -> Self {
        let cfg = RpcSendTransactionConfig { skip_preflight: true, max_retries: Some(2), preflight_commitment: None, encoding: None, min_context_slot: None };
        let v = clients.drain(..).map(|c| (c, Arc::new(Mutex::new(RpcStats::new())), RpcGate::new(64))).collect();
        Self { clients: v, cfg }
    }
    pub fn send_hedged_vtx(&self, vtx: &VersionedTransaction, timeout_ms: u64, min_ctx_slot: Option<u64>) -> anyhow::Result<Signature> {
        let wire = encode_vtx_into_tls(vtx);
        let (tx_sig, rx_sig) = channel::<(usize, anyhow::Result<Signature>, u128)>();
        let mut idx: Vec<usize> = (0..self.clients.len()).collect();
        idx.sort_by(|&a, &b| {
            let sa = self.clients[a].1.lock().unwrap(); let sb = self.clients[b].1.lock().unwrap();
            (sa.sr / sa.rtt_ms.max(1.0)).partial_cmp(&(sb.sr / sb.rtt_ms.max(1.0))).unwrap().reverse()
        });
        for (rank, i) in idx.into_iter().enumerate() {
            let c = self.clients[i].0.clone(); let stats = self.clients[i].1.clone(); let gate = self.clients[i].2.clone();
            let mut cfg = self.cfg; cfg.min_context_slot = min_ctx_slot; let bytes = wire.clone(); let tx_sig2 = tx_sig.clone();
            thread::spawn(move || {
                if !gate.try_enter() { return; }
                let stagger = (rank as u64) * 6; if stagger > 0 { thread::sleep(Duration::from_millis(stagger)); }
                let t0 = Instant::now(); let res = c.send_raw_transaction_with_config(bytes.as_slice(), cfg); let dt = t0.elapsed().as_millis();
                {
                    let mut st = stats.lock(); 
                    st.obs(res.is_ok(), dt as f64); 
                    if !res.is_ok() && st.sr < 0.75 { gate.trip(); } else if st.sr > 0.90 { gate.reset(); }
                }
                gate.leave();
                let _ = tx_sig2.send((i, res.map_err(|e| anyhow::anyhow!(e.to_string())), dt));
            });
        }
{{ ... }}
// ======================[PIECE 110: durable nonce lane]===============================
use solana_sdk::{system_program, system_instruction::advance_nonce_account};
pub struct NonceLane { pub nonce_account: Pubkey, pub nonce_authority: Arc<Keypair> }
impl NonceLane {
    pub fn fetch_nonce(&self, rpc: &RpcClient) -> anyhow::Result<(SolHash, u64)> {
        use solana_sdk::nonce::state::{Data as NonceData, Versions as NonceVersions};
        use bincode::deserialize;
        let acc = rpc.get_account(&self.nonce_account)?;
        let versions: NonceVersions = deserialize(&acc.data)?;
        let data = match versions { NonceVersions::Current(d) => d, _ => anyhow::bail!("Unsupported NonceAccount version"), };
        let NonceData { blockhash, .. } = data;
        let ctx = rpc.get_slot_with_commitment(CommitmentConfig::processed())?;
        Ok((blockhash, ctx))
    }
    pub fn wrap_with_nonce(&self, ixs: Vec<Instruction>, _recent_nonce: SolHash) -> Vec<Instruction> {
        let mut out = Vec::with_capacity(ixs.len()+1);
        out.push(advance_nonce_account(&self.nonce_account, &self.nonce_authority.pubkey()));
        out.extend(ixs); out
    }
}

// ======================[PIECE 99: cross-process signature guard]=====================
#[cfg(unix)]
mod sigguard {
    use super::*;
    use memmap2::MmapMut;
    use std::fs::OpenOptions;
    use std::os::unix::fs::OpenOptionsExt;

    pub struct SigGuard { mmap: MmapMut }
    impl SigGuard {
        pub fn new(path: &str, bytes: usize) -> anyhow::Result<Self> {
            let f = OpenOptions::new().read(true).write(true).create(true).mode(0o600).open(path)?;
            f.set_len(bytes as u64)?;
            let mmap = unsafe { MmapMut::map_mut(&f)? };
            Ok(Self { mmap })
        }
        /// Returns true if signature is newly claimed; false if seen.
        pub fn claim(&mut self, sig: &Signature) -> bool {
            let key = &sig.to_bytes()[..8];
            let idx = u64::from_le_bytes(key.try_into().unwrap()) as usize % self.mmap.len();
            let prev = self.mmap[idx];
            self.mmap[idx] = self.mmap[idx].wrapping_add(1);
            prev == 0
        }
    }
}

// ======================[UPGRADE: SAFE non-state shuffler; CB always pre-route]========
#[inline]
fn reorder_non_state_ixs_safe(ixs: &mut Vec<Instruction>, ld_coin: f64) {
    use solana_sdk::{compute_budget, ed25519_program};
    let mut cb_pre:  Vec<Instruction> = Vec::new();
{{ ... }}
    let mut pre_ns:  Vec<Instruction> = Vec::new();
    let mut route:   Vec<Instruction> = Vec::new();
    let mut post_ns: Vec<Instruction> = Vec::new();

    let mut seen_route = false;
    for ix in ixs.drain(..) {
        let pid = ix.program_id;
        let is_cb   = pid == compute_budget::id();
        let is_memo = pid == memo_prog::id();
        let is_ed   = pid == ed25519_program::id();
        if is_cb {
            cb_pre.push(ix);
        } else if is_memo || is_ed {
            if seen_route { post_ns.push(ix); } else { pre_ns.push(ix); }
        } else {
            seen_route = true; route.push(ix);
        }
    }
    let p = (ld_coin * 3.0).floor() as u8;
    ixs.extend(cb_pre);
    match p {
        0 => { ixs.extend(pre_ns); ixs.extend(route); ixs.extend(post_ns); }
        1 => { ixs.extend(post_ns); ixs.extend(route); ixs.extend(pre_ns); }
        _ => { ixs.extend(pre_ns); ixs.extend(post_ns); ixs.extend(route); }
    }
}

// ======================[UPGRADE: leader-aware slot targeting]==========================
impl TradePatternNoiseInjector {
    /// Bias cumulative delays so expected hit-slots align with 'preferred' leaders
    pub fn bias_delays_to_preferred_slots(&mut self,
                                          plan: &mut [NoiseInjectedTrade],
                                          preferred_slots: &[u64],
                                          current_slot: u64,
                                          window_slots: u64)
    {
        if plan.is_empty() || preferred_slots.is_empty() { return; }
        let slot_ms = self.slot_time.read().slot_ms().max(1) as u64;
        for t in plan.iter_mut() {
            let future_ms = t.delay_ms;
            let est_slot = current_slot.saturating_add(future_ms / slot_ms);
            if let Some(&target_slot) = preferred_slots.iter()
                .min_by_key(|&&s| s.abs_diff(est_slot))
                .filter(|&&s| s.abs_diff(est_slot) <= window_slots)
            {
                let target_ms = (target_slot.saturating_sub(current_slot)).saturating_mul(slot_ms);
                let desired = target_ms + (slot_ms / 2);
                let delta = desired as i64 - future_ms as i64;
                let cap = self.scale_jitter_by_slot_variance(40) as i64;
                let adj = delta.clamp(-cap, cap);
                t.delay_ms = (future_ms as i64 + adj).max(1) as u64;
            }
        }
        let mut cum = 0u64; for tt in plan.iter_mut() { cum = cum.saturating_add(tt.delay_ms); tt.delay_ms = cum; }
    }
    /// Extend adaptive heavy path to mix Ed25519 + Secp256k1 verifies when cheap
    #[inline]
    fn decoy_instructions_adaptive(&mut self) -> Vec<Instruction> {
        let frontier = { self.tip_model.read().p2.value() };
        let heavy = frontier < 1_800.0 && self.rng.gen::<f64>() < 0.65;
        if heavy {
            let n = self.config.ed25519_count.max(1);
            let mut v = Vec::with_capacity(n as usize);
            for _ in 0..n {
                if self.rng.gen::<f64>() < 0.5 { v.push(self.build_ed25519_verify_ix()); }
                else { v.push(self.build_secp256k1_verify_ix()); }
            }
            v
        } else {
            let memo = format!("d:{}", self.secret_salt);
            vec![build_memo_decoy(&memo)]
        }
    }
    #[inline]
    fn random_secp_secret(&mut self) -> secp256k1::SecretKey {
        loop {
            let bytes: [u8; 32] = self.rng.gen();
            if let Ok(sk) = secp256k1::SecretKey::from_slice(&bytes) { return sk; }
        }
    }
    #[inline]
    fn build_secp256k1_verify_ix(&mut self) -> Instruction {
        let len = self.config.ed25519_payload_len.clamp(16, 1024);
        let mut msg = vec![0u8; len];
        self.rng.fill(msg.as_mut_slice());
        let sk = self.random_secp_secret();
        secp256k1_instruction::new_secp256k1_instruction(&sk, &msg)
    }
}

// ======================[UPGRADE: multi-base Halton]===================================
const HALTON_PRIMES: [u32; 7] = [2, 3, 5, 7, 11, 13, 17];
impl Halton {
    #[inline]
    fn next_dim(&mut self, dim: usize) -> f64 {
        let base = HALTON_PRIMES[dim % HALTON_PRIMES.len()];
        self.next(base)
    }
}

// ======================[UPGRADE: randomized CB trio + order shuffle]===================
#[inline]
fn cb_trio_randomized(limit: u32, micro_lamports_per_cu: u64, loaded_data_limit: Option<u32>, coin: f64) -> Vec<Instruction> {
    let i_lim   = ComputeBudgetInstruction::set_compute_unit_limit(limit);
    let i_price = ComputeBudgetInstruction::set_compute_unit_price(micro_lamports_per_cu);
    let i_data  = loaded_data_limit.map(ComputeBudgetInstruction::set_loaded_accounts_data_size_limit);
    let mut v = Vec::with_capacity(3);
    let pick = (coin * 6.0).floor() as u8;
    match pick {
        0 => { v.push(i_lim.clone());   v.push(i_price.clone()); if let Some(d) = i_data.clone(){ v.push(d); } }
        1 => { v.push(i_price.clone()); v.push(i_lim.clone());   if let Some(d) = i_data.clone(){ v.push(d); } }
        2 => { if let Some(d) = i_data.clone(){ v.push(d); } v.push(i_lim.clone());   v.push(i_price.clone()); }
        3 => { if let Some(d) = i_data.clone(){ v.push(d); } v.push(i_price.clone()); v.push(i_lim.clone());   }
        4 => { v.push(i_lim.clone());   if let Some(d) = i_data.clone(){ v.push(d); } v.push(i_price.clone()); }
        _ => { v.push(i_price.clone()); if let Some(d) = i_data.clone(){ v.push(d); } v.push(i_lim.clone());   }
    }
    v
}

// ======================[UPGRADE: venue metrics + backoff]=============================
#[derive(Default, Clone)]
struct VenueStats { sr_ewma: f64, slip_ewma: f64, latency_ewma: f64, alpha: f64 }
impl VenueStats {
    fn new() -> Self { Self { sr_ewma: 0.9, slip_ewma: 15.0, latency_ewma: 2.0, alpha: 0.2 } }
    fn observe(&mut self, success: bool, slippage_bps: f64, latency_slots: f64) {
        let a = self.alpha;
        self.sr_ewma = a * (success as i32 as f64) + (1.0 - a) * self.sr_ewma;
        self.slip_ewma = a * slippage_bps + (1.0 - a) * self.slip_ewma;
        self.latency_ewma = a * latency_slots + (1.0 - a) * self.latency_ewma;
    }
    fn reward(&self) -> f64 {
        let r_sr = self.sr_ewma.clamp(0.0, 1.0);
        let r_slip = (1.0 - (self.slip_ewma / 50.0).clamp(0.0, 1.0)).clamp(0.0, 1.0);
        let r_lat = (1.0 - (self.latency_ewma / 5.0).clamp(0.0, 1.0)).clamp(0.0, 1.0);
        (r_sr * 0.60 + r_slip * 0.15 + r_lat * 0.25).clamp(0.0, 1.0)
    }
    /// Scale jitter based on slot-time variance
    pub fn scale_jitter_by_slot_variance(&self, base_ms: u64) -> u64 {
        let st = self.slot_time.read();
        let std = st.slot_std_ms();
        let f = if std < 20.0 { 1.0 } else if std < 60.0 { 0.75 } else { 0.55 };
        (base_ms as f64 * f) as u64
    }
    /// Decoy LUT parity decision
    pub fn decoy_should_use_lut(&mut self) -> bool { self.mixed_uniform() < self.config.decoy_lut_parity_prob }
}

// ======================[UPGRADE: CU pair cache (8-slot LRU)]===========================
struct CuEntry { lim: u32, price: u64, i1: Instruction, i2: Instruction }
#[derive(Default)]
struct CuCache8 { ring: [Option<CuEntry>; 8], ptr: usize }
impl CuCache8 {
    fn get_or_build(&mut self, lim: u32, price: u64) -> [Instruction;2] {
        for ent in self.ring.iter() {
            if let Some(e) = ent { if e.lim == lim && e.price == price { return [e.i1.clone(), e.i2.clone()]; } }
        }
        let [i1,i2] = cu_budget_instrs(lim, price);
        self.ring[self.ptr] = Some(CuEntry { lim, price, i1: i1.clone(), i2: i2.clone() });
        self.ptr = (self.ptr + 1) & 7;
        [i1, i2]
    }

    // ======================[UPGRADE: memo encoding variants]===========================
    #[inline]
    fn pick_codec(&mut self) -> MemoCodec {
        let r = self.mixed_uniform();
        if r < 0.25 { MemoCodec::Raw } else if r < 0.50 { MemoCodec::LenPrefixU16 }
        else if r < 0.75 { MemoCodec::Base64Ascii } else { MemoCodec::TLV }
    }
    #[inline]
    fn encode_memo_with_variant(&mut self, s: &str) -> Vec<u8> {
        match self.pick_codec() {
            MemoCodec::Raw => s.as_bytes().to_vec(),
            MemoCodec::LenPrefixU16 => {
                let mut out = Vec::with_capacity(2 + s.len());
                let l = (s.len() as u16).to_le_bytes();
                out.extend_from_slice(&l); out.extend_from_slice(s.as_bytes()); out
            }
            MemoCodec::Base64Ascii => {
                const T: &[u8;64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
                let bytes = s.as_bytes();
                let mut out = Vec::with_capacity(((bytes.len()+2)/3)*4);
                let mut i = 0;
                while i < bytes.len() {
                    let b0 = bytes[i];
                    let b1 = if i+1 < bytes.len() { bytes[i+1] } else { 0 };
                    let b2 = if i+2 < bytes.len() { bytes[i+2] } else { 0 };
                    out.push(T[( b0 >> 2) as usize]);
                    out.push(T[((b0 & 0x03)<<4 | (b1>>4)) as usize]);
                    out.push(if i+1 < bytes.len() { T[(((b1 & 0x0F)<<2) | (b2>>6)) as usize] } else { b'=' });
                    out.push(if i+2 < bytes.len() { T[(b2 & 0x3F) as usize] } else { b'=' });
                    i += 3;
                }
                out
            }
            MemoCodec::TLV => {
                let mut out = Vec::with_capacity(s.len()+3);
                out.push(0x01); // type=1
                out.push((s.len() & 0xFF) as u8);
                out.push(((s.len() >> 8) & 0xFF) as u8);
                out.extend_from_slice(s.as_bytes()); out
            }
        }
    }
}
#[derive(Clone, Default)]
struct VenueBackoff { until_slot: u64 }


// ======================[UPGRADE: memo padding + builders]==============================
#[inline]
fn build_memo_decoy_bytes(bytes: Vec<u8>) -> Instruction {
    Instruction { program_id: memo_prog::id(), accounts: vec![], data: bytes }
}

impl TradePatternNoiseInjector {
    #[inline]
    fn pad_memo_bytes(&mut self, s: &str) -> Vec<u8> {
        let mut data = s.as_bytes().to_vec();
        let q = self.config.memo_pad_quantum.max(16);
        let jit = self.config.memo_pad_jitter.min(q / 2);
        let r = ((self.mixed_uniform() - 0.5) * 2.0 * jit as f64) as isize;
        let mut target = (((data.len() + q - 1) / q) * q) as isize + r;
        let min_target = (data.len() + 8) as isize;
        if target < min_target { target = min_target; }
        let target = target as usize;
        if data.len() < target {
            data.resize(target, 0u8);
            let tail_start = target.saturating_sub(jit.max(8));
            for b in &mut data[tail_start..] { *b = (self.rng.gen::<u8>() & 0x7F) | 1; }
        }
        data
    }
    #[inline]
    fn pad_memo_raw_bytes(&mut self, mut data: Vec<u8>) -> Vec<u8> {
        let q = self.config.memo_pad_quantum.max(16);
        let jit = self.config.memo_pad_jitter.min(q / 2);
        let r = ((self.mixed_uniform() - 0.5) * 2.0 * jit as f64) as isize;
        let mut target = (((data.len() + q - 1) / q) * q) as isize + r;
        let min_target = (data.len() + 8) as isize;
        if target < min_target { target = min_target; }
        let target = target as usize;
        if data.len() < target {
            data.resize(target, 0u8);
            let tail_start = target.saturating_sub(jit.max(8));
            for b in &mut data[tail_start..] { *b = (self.rng.gen::<u8>() & 0x7F) | 1; }
        }
        data
    }
}

// ======================[UPGRADE: instruction padding helper]===========================
#[inline]
fn pad_instruction_list<R: Rng>(ixs: &mut Vec<Instruction>, rng: &mut R, halton: &mut Halton, quantum: usize, jitter: usize) {
    let quantum = quantum.max(4);
    let base = ((ixs.len() + quantum - 1) / quantum) * quantum;
    let ld = halton.next(19);
    let jit = ((ld - 0.5) * 2.0 * (jitter as f64)).round() as isize;
    let target = (base as isize + jit).max((ixs.len() + 1) as isize) as usize;
    while ixs.len() < target {
        let bump = (rng.gen_range(0..3) as u32) * 4096;
        ixs.push(ComputeBudgetInstruction::set_heap_frame(HEAP_FRAME_BASE + bump));
    }
}

// ======================[UPGRADE: P² quantile estimator]================================
#[derive(Clone)]
struct P2Quantile {
    q: f64,
    init: SmallVec<[f64; 5]>,
    n: [f64; 5],
    qv: [f64; 5],
    d: [f64; 5],
}
impl P2Quantile {
    fn new(q: f64) -> Self { Self { q, init: SmallVec::new(), n: [0.0;5], qv: [0.0;5], d: [0.0;5] } }
    fn insert(&mut self, x: f64) {
        if self.init.len() < 5 {
            self.init.push(x);
            if self.init.len() == 5 {
                let mut v = self.init.clone().into_vec();
                v.sort_by(|a,b| a.partial_cmp(b).unwrap());
                self.qv.copy_from_slice(&[v[0], v[1], v[2], v[3], v[4]]);
                self.n = [1.0, 2.0, 3.0, 4.0, 5.0];
                self.d = [0.0, self.q/2.0, self.q, (1.0+self.q)/2.0, 1.0];
                for i in 0..5 { self.d[i] = self.d[i]*4.0 + 1.0; }
            }
            return;
        }
        let mut k = 0usize;
        if x < self.qv[0] { self.qv[0] = x; k = 0; }
        else if x >= self.qv[4] { self.qv[4] = x; k = 3; }
        else { while k < 4 && x >= self.qv[k+1] { k += 1; } }
        for i in (k+1)..5 { self.n[i] += 1.0; }
        self.d[1] += self.q/2.0; self.d[2] += self.q; self.d[3] += (1.0+self.q)/2.0; self.d[4] += 1.0;
        for i in 1..4 {
            let di = self.d[i] - self.n[i];
            if (di >= 1.0 && self.n[i+1] - self.n[i] > 1.0) || (di <= -1.0 && self.n[i-1] - self.n[i] < -1.0) {
                let d = di.signum();
                let qi = self.qv[i] + d * (self.qv[i+1] - self.qv[i-1]) / (self.n[i+1] - self.n[i-1]);
                if self.qv[i-1] < qi && qi < self.qv[i+1] {
                    self.qv[i] = qi;
                } else {
                    let j = (i as isize + d as isize) as usize;
                    self.qv[i] += d * (self.qv[j] - self.qv[i]) / (self.n[j] - self.n[i]);
                }
                self.n[i] += d;
            }
        }
    }
    #[inline]
    fn value(&self) -> f64 {
        if self.init.len() < 5 {
            if self.init.is_empty() { return 1_000.0; }
            let mut v = self.init.clone().into_vec(); v.sort_by(|a,b| a.partial_cmp(b).unwrap()); v[v.len()/2]
        } else { self.qv[2] }
    }
}

// ======================[UPGRADE: Tip model with SR controller + TPS bias]==============
#[derive(Clone)]
struct TipModel {
    p2: P2Quantile,
    venue_cu_baseline: FastMap<String, u32>,
    sr_ewma: f64,
    sr_alpha: f64,
    target_sr: f64,
    gain: f64,
    venue_cu_used_ewma: FastMap<String, f64>,
}
impl TipModel {
    fn new() -> Self {
        let mut base: FastMap<String, u32> = FastMap::default();
        base.insert("raydium".into(), 120_000);
        base.insert("orca".into(),    100_000);
        base.insert("serum".into(),   140_000);
        base.insert("jupiter".into(), 110_000);
        Self { p2: P2Quantile::new(0.80), venue_cu_baseline: base, sr_ewma: 0.9, sr_alpha: 0.15, target_sr: 0.92, gain: 0.6, venue_cu_used_ewma: FastMap::default() }
    }
    #[inline] fn observe_price(&mut self, micro_lamports_per_cu: u64) { self.p2.insert(micro_lamports_per_cu as f64); }
    #[inline] fn observe_landing(&mut self, success: bool) { let s = if success {1.0} else {0.0}; self.sr_ewma = self.sr_alpha * s + (1.0 - self.sr_alpha) * self.sr_ewma; }
    #[inline] fn next_cu_price<R: Rng>(&self, rng: &mut R) -> u64 {
        let base = self.p2.value().max(500.0);
        let error = (self.target_sr - self.sr_ewma).clamp(-0.5, 0.5);
        let control = (1.0 + self.gain * error).clamp(0.8, 1.5);
        let jitter = 1.0 + (rng.gen::<f64>() - 0.5) * 0.10;
        (base * control * jitter) as u64
    }
    #[inline] fn next_cu_limit<R: Rng>(&self, venue: &str, _rng: &mut R, halton: &mut Halton) -> u32 {
        let base = *self.venue_cu_baseline.get(venue).unwrap_or(&110_000);
        let ld = halton.next(11);
        let span = (base as f64 * 0.08) as i64;
        let adj = ((ld - 0.5) * 2.0 * span as f64) as i64;
        (base as i64 + adj).max(50_000) as u32
    }
    #[inline] pub fn adapt_to_tps(&mut self, tps: f64) {
        let bias = if tps < 2000.0 { 0.85 } else if tps < 3500.0 { 1.00 } else { 1.18 };
        self.gain = (self.gain * bias.clamp(0.75, 1.25)).clamp(0.4, 0.9);
        self.target_sr = (self.target_sr + if tps > 3500.0 { 0.02 } else { 0.0 }).clamp(0.88, 0.97);
    }
}

// ======================[UPGRADE: HKDF helpers + PRNG seeding]==========================
#[inline]
fn derive_slot_salt(master: &[u8; 32], slot: u64) -> u64 {
    let mut mac = HmacSha256::new_from_slice(master).expect("HMAC key");
    mac.update(&slot.to_le_bytes());
    let out = mac.finalize().into_bytes();
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&out[..8]);
    u64::from_le_bytes(buf)
}

#[inline]
fn splitmix64(x: u64) -> u64 {
    let mut z = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

// ======================[UPGRADE: EXP3-IX bandit with decay]============================
#[derive(Debug)]
struct Exp3IxBandit {
    logw: FastMap<String, f64>,
    gamma: f64,
    eta: f64,
    decay: f64,
    probs: FastMap<String, f64>,
}

impl Exp3IxBandit {
    fn new(init: &FastMap<String, f64>, gamma: f64, eta: f64, decay: f64) -> Self {
        let mut logw = FastMap::default();
        for (k, v) in init {
            logw.insert(k.clone(), v.max(1e-6).ln());
        }
        Self { logw, gamma, eta, decay, probs: FastMap::default() }
    }

    fn refresh_probs(&mut self) {
        let max_log = self.logw.values().fold(f64::NEG_INFINITY, |a, &b| a.max(b));
        let mut sum = 0.0;
        let mut raw: FastMap<String, f64> = FastMap::default();
        for (k, &lw) in self.logw.iter() {
            let v = (lw - max_log).exp();
            raw.insert(k.clone(), v);
            sum += v;
        }
        let k = self.logw.len().max(1) as f64;
        self.probs.clear();
        for (venue, v) in raw.into_iter() {
            let p = (1.0 - self.gamma) * (v / sum.max(1e-18)) + self.gamma / k;
            self.probs.insert(venue, p.max(1e-9));
        }
    }

    fn sample<R: Rng>(&mut self, rng: &mut R) -> String {
        self.refresh_probs();
        let r = rng.gen::<f64>();
        let mut acc = 0.0;
        for (venue, p) in self.probs.iter() {
            acc += *p;
            if r <= acc { return venue.clone(); }
        }
        self.probs.keys().next().cloned().unwrap_or_else(|| "raydium".to_string())
    }

    fn update(&mut self, chosen: &str, reward: f64) {
        let p = *self.probs.get(chosen).unwrap_or(&1e-9);
        let est = (reward.clamp(0.0, 1.0) / p).min(10.0);
        for lw in self.logw.values_mut() { *lw *= 1.0 - self.decay; }
        if let Some(lw) = self.logw.get_mut(chosen) { *lw += self.eta * est; }
    }
}

// Memo decoy helper (zero-loss)
#[inline]
pub fn build_memo_decoy(memo: &str) -> Instruction {
    Instruction { program_id: memo_prog::id(), accounts: vec![], data: memo.as_bytes().to_vec() }
}

// ======================[UPGRADE: Jito tip helpers]====================================
#[inline]
fn estimated_priority_lamports(cu_limit: u32, micro_lamports_per_cu: u64) -> u64 {
    ((cu_limit as u128 * micro_lamports_per_cu as u128) / 1_000_000u128) as u64
}
#[inline]
fn tip_from_frontier(cu_limit: u32, micro_lamports_per_cu: u64, ratio_bps: u16) -> u64 {
    let base = estimated_priority_lamports(cu_limit, micro_lamports_per_cu) as u128;
    ((base * ratio_bps as u128) / 10_000u128) as u64
}
#[inline]
fn build_tip_ix(fee_payer: &Pubkey, tip_to: &Pubkey, lamports: u64) -> Instruction {
    system_instruction::transfer(fee_payer, tip_to, lamports)
}

// ======================[UPGRADE: constants tuned for HFT]==============================
// Notes:
// - MIN/MAX interval kept, but timing generation uses Poisson thinning and blue-noise mix.
// - Volume factors now shape via Dirichlet-like splits.
// - Thresholds harmonized with EWMA controller (see below).
const MAX_PATTERN_HISTORY: usize = 1024;
const NOISE_INJECTION_THRESHOLD: f64 = 0.85;
const PATTERN_DETECTION_WINDOW: u64 = 300; // 5 minutes
const MIN_TRADE_INTERVAL_MS: u64 = 40;     // tighter edge
const MAX_TRADE_INTERVAL_MS: u64 = 4500;
const VOLUME_NOISE_FACTOR: f64 = 0.28;     // avoid fat tails
const TIMING_JITTER_RANGE: f64 = 0.45;
const ORDER_SPLIT_THRESHOLD: u64 = 10_000_000; // 0.01 SOL
const MAX_SPLIT_ORDERS: usize = 7;
const PATTERN_ENTROPY_THRESHOLD: f64 = 0.70;
// rotate every ~64 slots (~25s @ 400ms/slot) without thrashing
const RESEED_PERIOD_SLOTS: u64 = 64;
// instruction padding base for heap frames
const HEAP_FRAME_BASE: u32 = 65_536;
// prime-modulo timing buckets for de-resonance
const PRIME_BUCKETS: [u64; 5] = [89, 97, 101, 103, 109];

// ======================[UPGRADE: slot-time tracker]===================================
#[derive(Debug, Clone)]
pub struct SlotTimeTracker {
    alpha: f64,
    slot_ms_ewma: f64,
    ms2_ewma: f64,
    last_instant: Option<Instant>,
}
impl SlotTimeTracker {
    pub fn new() -> Self { Self { alpha: 0.18, slot_ms_ewma: 400.0, ms2_ewma: 400.0*400.0, last_instant: None } }
    #[inline] pub fn on_blockhash_tick(&mut self, now: Instant) {
        if let Some(prev) = self.last_instant.replace(now) {
            let dt_ms = (now - prev).as_millis().max(1) as f64;
            let a = self.alpha;
            self.slot_ms_ewma = a * dt_ms + (1.0 - a) * self.slot_ms_ewma;
            self.ms2_ewma     = a * (dt_ms*dt_ms) + (1.0 - a) * self.ms2_ewma;
            self.slot_ms_ewma = self.slot_ms_ewma.clamp(200.0, 1200.0);
        }
    }
    #[inline] pub fn slot_ms(&self) -> u64 { self.slot_ms_ewma as u64 }
    #[inline] pub fn slot_std_ms(&self) -> f64 { (self.ms2_ewma - self.slot_ms_ewma*self.slot_ms_ewma).max(0.0).sqrt() }
}

// Decoy modes (default no-loss via Memo program)
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub enum DecoyMode {
    /// Memo-only tx (no state touch). Zero fill risk, obfuscates fingerprint.
    MemoOnly,
    /// Serum post-only far-from-touch (if you wire an OB venue). Near-zero fill risk.
    PostOnly,
    /// Stateless but compute-heavy decoys using ed25519 verifies
    Ed25519Heavy,
    /// Disabled.
    Off,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NoiseConfig {
    pub volume_randomization: bool,
    pub timing_randomization: bool,
    pub order_splitting: bool,
    pub venue_rotation: bool,
    pub pattern_breaking: bool,
    pub entropy_target: f64,
    pub max_slippage_bps: u16,
    // New fields
    pub decoy_mode: DecoyMode,
    /// key to salt RNG and signature hashing (rotate via ops if needed)
    pub noise_salt: u64,
    /// desired min k-anonymity bucket (keeps you inside the herd)
    pub crowd_k_target: usize,
    /// memo padding quantum and jitter to normalize tx sizes for decoys
    pub memo_pad_quantum: usize,
    pub memo_pad_jitter: usize,
    /// ed25519-heavy decoy tuning
    pub ed25519_payload_len: usize,
    pub ed25519_count: u8,
    /// Dirichlet-α control for split variability
    pub dirichlet_alpha: f64,
    /// Probability to add a small memo on real trades for parity
    pub real_memo_pad_prob: f64,
    /// Probability decoy should use LUT parity when building versioned txs
    pub decoy_lut_parity_prob: f64,
}

impl Default for NoiseConfig {
    fn default() -> Self {
        Self {
            volume_randomization: true,
            timing_randomization: true,
            order_splitting: true,
            venue_rotation: true,
            pattern_breaking: true,
            entropy_target: 0.85,
            max_slippage_bps: 30,
            decoy_mode: DecoyMode::MemoOnly,
            noise_salt: 0xA5A5_BEEF_F00D_2025,
            crowd_k_target: 8,
            memo_pad_quantum: 64,
            memo_pad_jitter: 16,
            ed25519_payload_len: 96,
            ed25519_count: 2,
            dirichlet_alpha: 0.9,
            real_memo_pad_prob: 0.18,
            decoy_lut_parity_prob: 0.25,
        }
    }
}

// ======================[UPGRADE: core struct]==========================================
pub struct TradePatternNoiseInjector {
    config: NoiseConfig,
    pattern_history: Arc<RwLock<VecDeque<TradePattern>>>,
    rng: ChaCha20Rng,
    // [UPGRADE] adaptive venue bandit (EXP3-IX + decay)
    bandit: RwLock<Exp3IxBandit>,
    // [UPGRADE] slot awareness
    last_observed_slot: AtomicU64,
    // [UPGRADE] incremental stats
    interval_stats: RwLock<EwmaStats>,
    volume_stats: RwLock<EwmaStats>,
    // [UPGRADE] detectors
    interval_cusum: RwLock<Cusum>,
    volume_cusum: RwLock<Cusum>,
    // [UPGRADE] low-discrepancy mixer
    halton: RwLock<Halton>,
    // state
    pattern_metrics: Arc<RwLock<PatternMetrics>>,
    last_trade_time: Arc<RwLock<Instant>>,
    volume_distribution: Arc<RwLock<Vec<f64>>>,
    // keyed salt for signatures
    secret_salt: u64,
    // rotating salt master + reseed tracking
    master_salt: [u8; 32],
    last_reseeded_slot: AtomicU64,
    // tip model for CU price/limit autopricer
    tip_model: RwLock<TipModel>,
    // slot-time tracker (EWMA)
    slot_time: RwLock<SlotTimeTracker>,
    // venue composite stats and backoff maps
    venue_stats: RwLock<FastMap<String, VenueStats>>,
    backoff: RwLock<FastMap<String, VenueBackoff>>,
    // CU instruction cache (8-slot ring)
    cu_cache: RwLock<CuCache8>,
    // retry policy for deterministic resend bumps (PIECE 84)
    retry_policy: RetryBumpPolicy,
}

impl TradePatternNoiseInjector {
    pub fn new(config: NoiseConfig) -> Self {
        let mut venue_weights = HashMap::new();
        // Initial priors; EXP3 will adapt
        venue_weights.insert("raydium".to_string(), 0.35);
        venue_weights.insert("orca".to_string(), 0.25);
        venue_weights.insert("serum".to_string(), 0.20);
        venue_weights.insert("jupiter".to_string(), 0.20);

        // Seed RNG from noise_salt for preimage resistance
        let seed = config.noise_salt.to_le_bytes();
        let mut seed32 = [0u8; 32];
        for (i, b) in seed.iter().enumerate() { seed32[i] = *b; }

        // initialize master salt from noise_salt
        let mut master = [0u8; 32];
        master[..8].copy_from_slice(&config.noise_salt.to_le_bytes());

        Self {
            config,
            pattern_history: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_PATTERN_HISTORY))),
            rng: ChaCha20Rng::from_seed(seed32),
            bandit: RwLock::new(Exp3IxBandit::new(&venue_weights, 0.10, 0.06, 0.002)),
            last_observed_slot: AtomicU64::new(0),
            interval_stats: RwLock::new(EwmaStats::with_alpha(0.35)),
            volume_stats: RwLock::new(EwmaStats::with_alpha(0.30)),
            interval_cusum: RwLock::new(Cusum::new(0.25, 3.0)),
            volume_cusum: RwLock::new(Cusum::new(0.22, 3.0)),
            halton: RwLock::new(Halton::default()),
            pattern_metrics: Arc::new(RwLock::new(PatternMetrics {
                entropy: 1.0,
                periodicity_score: 0.0,
                volume_consistency: 0.0,
                timing_regularity: 0.0,
                directional_bias: 0.5,
            })),
            last_trade_time: Arc::new(RwLock::new(Instant::now())),
            volume_distribution: Arc::new(RwLock::new(Vec::new())),
            secret_salt: config.noise_salt,
            master_salt: master,
            last_reseeded_slot: AtomicU64::new(0),
            tip_model: RwLock::new(TipModel::new()),
            slot_time: RwLock::new(SlotTimeTracker::new()),
            venue_stats: RwLock::new(FastMap::default()),
            backoff: RwLock::new(FastMap::default()),
            cu_cache: RwLock::new(CuCache8::default()),
            retry_policy: RetryBumpPolicy::default(),
        }
    }

    /// Bias planned delays to the center of each slot to reduce bank-edge collisions.
    /// slot_ms: observed slot duration (typ. 400ms). center in [0.2,0.8], e.g., 0.55.
    pub fn align_delays_to_slot_center(&mut self, plan: &mut [NoiseInjectedTrade], slot_ms: u64, center: f64) {
        let center = center.clamp(0.2, 0.8);
        for t in plan.iter_mut() {
            let ld = { let mut h = self.halton.write(); h.next(7) };
            let target = (center + (ld - 0.5) * 0.18).clamp(0.25, 0.75);
            let modulo = if slot_ms == 0 { 0 } else { t.delay_ms % slot_ms };
            let desired = (slot_ms as f64 * target) as u64;
            let adj = if modulo <= desired { desired - modulo } else { slot_ms.saturating_sub(modulo - desired) };
            t.delay_ms = t.delay_ms.saturating_add(adj.min(35));
        }
    }

    /// Set current slot and maybe reseed rotating salt + RNG
    #[inline]
    pub fn set_current_slot_and_reseed(&mut self, slot: u64) {
        self.last_observed_slot.store(slot, Ordering::Relaxed);
        self.maybe_reseed(slot);
    }

    /// Combined blockhash hook that updates RNG/LD state and slot-time EWMA
    pub fn on_new_blockhash_timed(&mut self, blockhash: [u8; 32], now: Instant) {
        self.on_new_blockhash(blockhash);
        self.slot_time.write().on_blockhash_tick(now);
    }

    /// Mix leader's vote pubkey into master_salt and reseed in this slot (PIECE 65)
    pub fn on_new_leader(&mut self, leader_vote: &Pubkey, current_slot: u64) {
        let bytes = leader_vote.to_bytes();
        let mut arr = [0u8; 8]; arr.copy_from_slice(&bytes[..8]);
        let mix = splitmix64(u64::from_le_bytes(arr));
        for i in 0..8 { self.master_salt[i] ^= ((mix >> (i*8)) & 0xFF) as u8; }
        self.maybe_reseed(current_slot);
    }

    // Internal: maybe reseed salt/RNG on slot boundary cadence
    #[inline]
    fn maybe_reseed(&mut self, current_slot: u64) {
        let last = self.last_reseeded_slot.load(Ordering::Relaxed);
        if current_slot < last || current_slot.saturating_sub(last) < RESEED_PERIOD_SLOTS { return; }
        let slot_salt = derive_slot_salt(&self.master_salt, current_slot);
        self.secret_salt = slot_salt;
        let mut seed32 = [0u8; 32];
        let k0 = splitmix64(slot_salt);
        let k1 = splitmix64(k0);
        seed32[..8].copy_from_slice(&k0.to_le_bytes());
        seed32[8..16].copy_from_slice(&k1.to_le_bytes());
        self.rng = ChaCha20Rng::from_seed(seed32);
        self.last_reseeded_slot.store(current_slot, Ordering::Relaxed);
    }

    pub fn on_new_blockhash(&mut self, blockhash: [u8; 32]) {
        let lo = u64::from_le_bytes(blockhash[0..8].try_into().unwrap());
        let hi = u64::from_le_bytes(blockhash[8..16].try_into().unwrap());
        let mix = splitmix64(lo ^ hi ^ self.secret_salt);
        {
            let mut h = self.halton.write();
            // advance Halton sequence a bounded number of steps to decorrelate
            let steps = (mix & 0x3F) as usize; // up to 63 steps
            for _ in 0..steps { let _ = h.next(5); }
        }
        let mut seed32 = [0u8; 32];
        seed32[..8].copy_from_slice(&mix.to_le_bytes());
        seed32[8..16].copy_from_slice(&splitmix64(mix).to_le_bytes());
        self.rng = ChaCha20Rng::from_seed(seed32);
    }

    // ======================[UPGRADE: primary entrypoint (unchanged signature)]=========
    pub fn inject_noise(&mut self, original_volume: u64, direction: TradeDirection) -> Vec<NoiseInjectedTrade> {
        self.update_pattern_metrics();
{{ ... }}

        let metrics = self.pattern_metrics.read();
        let needs_noise =
            metrics.entropy < self.config.entropy_target
            || metrics.periodicity_score > NOISE_INJECTION_THRESHOLD
            || metrics.volume_consistency > NOISE_INJECTION_THRESHOLD
            || metrics.timing_regularity > NOISE_INJECTION_THRESHOLD;
        drop(metrics);

        if !needs_noise {
            return vec![NoiseInjectedTrade {
                volume: original_volume,
                direction,
                venue: self.select_venue(),
                delay_ms: 0,
                is_decoy: false,
                decoy_memo: None,
            }];
        }

        let mut trades = Vec::new();

        // [UPGRADE] Volume randomization uses lognormal-ish multiplier bounded by VOLUME_NOISE_FACTOR
        let volume = if self.config.volume_randomization {
            self.randomize_volume(original_volume)
        } else {
            original_volume
        };

        // [UPGRADE] Order splitting via Dirichlet-like sampling (blue-noise volumes)
        if self.config.order_splitting && volume > ORDER_SPLIT_THRESHOLD {
            let base_parts = (volume / ORDER_SPLIT_THRESHOLD) as usize;
            let parts = base_parts.clamp(2, MAX_SPLIT_ORDERS);
            let splits = self.split_order_dirichlet_alpha(volume, parts, self.config.dirichlet_alpha);
            for (i, split_volume) in splits.iter().enumerate() {
                let delay = if self.config.timing_randomization {
                    self.generate_arrival_delay_mixture(i as u64)
                } else {
                    (i as u64) * 100
                };

                let venue = if self.config.venue_rotation { self.select_venue() } else { "raydium".to_string() };

                trades.push(NoiseInjectedTrade {
                    volume: *split_volume,
                    direction,
                    venue,
                    delay_ms: delay,
                    is_decoy: false,
                    decoy_memo: None,
                });
            }
        } else {
            let delay = if self.config.timing_randomization { self.generate_arrival_delay_mixture(0) } else { 0 };
            trades.push(NoiseInjectedTrade {
                volume,
                direction,
                venue: self.select_venue(),
                delay_ms: delay,
                is_decoy: false,
                decoy_memo: None,
            });
        }

        // [UPGRADE] Pattern breaking: decoys are *non-executing* by default
        if self.config.pattern_breaking && self.should_inject_decoy() {
            let decoy_trades = self.generate_decoy_trades_non_loss(original_volume, direction);
            trades.extend(decoy_trades);
        }

        self.update_volume_distribution(volume);
        trades
    }

    // ======================[UPGRADE: metrics are incremental + k-crowd feedback]=======
    fn update_pattern_metrics(&self) {
        let history = self.pattern_history.read();
        if history.len() < 4 { return; }

        // interval / volume updates (EWMA + CUSUM)
        if let Some((last, prev)) = history.iter().rev().take(2).collect::<Vec<_>>().split_first() {
            let dt = last[0].timestamp.saturating_sub(prev[0].timestamp) as f64;
            let vol = last[0].volume as f64;

            {
                let mut is = self.interval_stats.write();
                is.push(dt);
                let mut cs = self.interval_cusum.write();
                let _ = cs.push(dt, is.mean);
            }
            {
                let mut vs = self.volume_stats.write();
                vs.push(vol);
                let mut cs = self.volume_cusum.write();
                let _ = cs.push(vol, vs.mean);
            }
        }

        let patterns: Vec<TradePattern> = history.iter().cloned().collect();
        drop(history);

        let entropy = self.calculate_entropy(&patterns);
        let periodicity = self.calculate_periodicity(&patterns);
        let volume_consistency = self.calculate_volume_consistency(&patterns);
        let timing_regularity = self.calculate_timing_regularity(&patterns);
        let directional_bias = self.calculate_directional_bias(&patterns);

        // [UPGRADE] k-anonymity pressure: n/k crowd factor
        let k_crowd = (patterns.len() / self.config.crowd_k_target.max(1)).max(1) as f64;
        let entropy_target = (self.config.entropy_target * (1.0 - (1.0 / (k_crowd + 1.0)))).clamp(0.6, 0.96);

        let mut metrics = self.pattern_metrics.write();
        metrics.entropy = entropy.max(entropy_target.min(1.2));
        metrics.periodicity_score = periodicity;
        metrics.volume_consistency = volume_consistency;
        metrics.timing_regularity = timing_regularity;
        metrics.directional_bias = directional_bias;
    }

    // unchanged signatures; internals optimized
    fn calculate_entropy(&self, patterns: &[TradePattern]) -> f64 {
        let mut volume_bins: HashMap<u64, usize> = HashMap::new();
        let mut time_bins: HashMap<u64, usize> = HashMap::new();

        for p in patterns {
            let vbin = p.volume / 1_000_000_000; // 1 SOL bins
            let tbin = p.timestamp / 60; // 1 minute bins
            *volume_bins.entry(vbin).or_insert(0) += 1;
            *time_bins.entry(tbin).or_insert(0) += 1;
        }

        let total = patterns.len() as f64;
        let h = |bins: &HashMap<u64, usize>| -> f64 {
            bins.values().fold(0.0, |acc, &c| {
                let p = c as f64 / total;
                if p > 0.0 { acc - p * p.log2() } else { acc }
            })
        };
        (h(&volume_bins) + h(&time_bins)) / 2.0
    }

    fn calculate_periodicity(&self, patterns: &[TradePattern]) -> f64 {
        if patterns.len() < 12 { return 0.0; }
        let mut prev = None;
        let mut diffs = Vec::with_capacity(patterns.len());
        for p in patterns {
            if let Some(t) = prev {
                diffs.push(p.timestamp.saturating_sub(t) as f64);
            }
            prev = Some(p.timestamp);
        }
        let mean = diffs.iter().sum::<f64>() / diffs.len().max(1) as f64;
        let var = diffs.iter().map(|d| { let z = d - mean; z*z }).sum::<f64>() / diffs.len().max(1) as f64;
        let cv = if mean > 0.0 { (var.sqrt() / mean).clamp(0.0, 1.0) } else { 1.0 };
        1.0 - cv
    }

    fn calculate_volume_consistency(&self, patterns: &[TradePattern]) -> f64 {
        let vols: Vec<f64> = patterns.iter().map(|p| p.volume as f64).collect();
        let mean = vols.iter().sum::<f64>() / vols.len().max(1) as f64;
        let var = vols.iter().map(|v| { let z = *v - mean; z*z }).sum::<f64>() / vols.len().max(1) as f64;
        let cv = if mean > 0.0 { (var.sqrt() / mean).clamp(0.0, 1.0) } else { 1.0 };
        1.0 - cv
    }

    fn calculate_timing_regularity(&self, patterns: &[TradePattern]) -> f64 {
        if patterns.len() < 8 { return 0.0; }
        let mut hour_counts = [0u32; 24];
        for p in patterns {
            let h = (p.timestamp / 3600) % 24;
            hour_counts[h as usize] += 1;
        }
        let total = patterns.len() as f64;
        let max_c = *hour_counts.iter().max().unwrap_or(&0) as f64;
        (max_c / total).clamp(0.0, 1.0)
    }

    fn calculate_directional_bias(&self, patterns: &[TradePattern]) -> f64 {
        let buys = patterns.iter().filter(|p| p.direction == TradeDirection::Buy).count() as f64;
        buys / patterns.len().max(1) as f64
    }

    // ======================[UPGRADE: lognormal bounded noise]===========================
    fn randomize_volume(&mut self, original_volume: u64) -> u64 {
        // lognormal-ish multiplier in [1 - f, 1 + f]
        let f = VOLUME_NOISE_FACTOR.clamp(0.01, 0.45);
        let u = self.mixed_uniform();
        let mult = 1.0 + (2.0 * u - 1.0) * f;
        let noisy = (original_volume as f64 * mult).max(1.0);
        self.benford_weighted_round(noisy as u64)
    }

    fn benford_weighted_round(&mut self, v: u64) -> u64 {
        // Benford's law weighted rounding
        let mut rng = self.rng.clone();
        let digit = (v / 10).log10() as u32;
        let p = match digit {
            0 => 0.301,
            1 => 0.176,
            2 => 0.125,
            3 => 0.097,
            4 => 0.079,
            5 => 0.067,
            6 => 0.058,
            7 => 0.051,
            8 => 0.046,
            _ => 0.042,
        };
        if rng.gen_bool(p) {
            v + 1
        } else {
            v
        }
    }

    // ======================[UPGRADE: Dirichlet-like splits via normalized exponentials]
    fn split_order_dirichlet(&mut self, volume: u64) -> Vec<u64> {
        let base_parts = (volume / ORDER_SPLIT_THRESHOLD) as usize;
        let n_parts = base_parts.clamp(2, MAX_SPLIT_ORDERS);

        let mut draws = Vec::with_capacity(n_parts);
        let mut sum = 0.0;
        for _ in 0..n_parts {
            // exponential(1) sample: -ln(U)
            let u = self.mixed_uniform().clamp(1e-12, 1.0);
            let x = -u.ln();
            draws.push(x);
            sum += x;
        }
        let mut out = Vec::with_capacity(n_parts);
        let mut acc = 0u64;
        for (i, x) in draws.iter().enumerate() {
            let frac = x / sum;
            let part = ((volume as f64) * frac).max(1.0) as u64;
            out.push(part);
            acc += part;
            if i == n_parts - 1 && acc != volume {
                // fix rounding drift
                let diff = if acc > volume { acc - volume } else { volume - acc };
                if acc > volume {
                    out[i] = out[i].saturating_sub(diff);
                } else {
                    out[i] = out[i].saturating_add(diff);
                }
            }
        }
        // low-discrepancy shuffle via Halton index map
        self.ld_shuffle(&mut out);
        out
    }

    // ======================[UPGRADE: Poisson-thinned arrival + blue-noise jitter]======
    fn generate_arrival_delay(&mut self, sequence_idx: u64) -> u64 {
        // baseline lambda from EWMA intervals
        let mean_ms = {
            let is = self.interval_stats.read();
            let m = is.mean.max(200.0); // guard
            m
        };
        // Poisson arrival delta sampled as exponential(mean=mean_ms)
        let u = self.mixed_uniform().clamp(1e-12, 1.0);
        let exp_delta = -mean_ms * u.ln();

        // blue-noise / low-discrepancy jitter component
        let ld = { let mut h = self.halton.write(); h.next_dim(1) };

        let jitter_span = ((MAX_TRADE_INTERVAL_MS - MIN_TRADE_INTERVAL_MS) as f64 * TIMING_JITTER_RANGE).max(50.0);
        let jitter = ld * jitter_span;

        let base = MIN_TRADE_INTERVAL_MS as f64 + exp_delta * 0.35 + (sequence_idx as f64).sqrt() * 12.0;
        let delay = (base + jitter).clamp(MIN_TRADE_INTERVAL_MS as f64, MAX_TRADE_INTERVAL_MS as f64);
        delay as u64
    }

    /// Mixture arrival: Erlang-2 vs Exponential with blue-noise jitter (PIECE 69)
    fn generate_arrival_delay_mixture(&mut self, sequence_idx: u64) -> u64 {
        let mean_ms = { let is = self.interval_stats.read(); is.mean.max(200.0) };
        let base = if self.rng.gen::<f64>() < 0.55 {
            self.erlang2_ms(mean_ms)
        } else {
            let u = self.mixed_uniform().clamp(1e-12, 1.0);
            -mean_ms * u.ln()
        };
        let ld = { let mut h = self.halton.write(); h.next_dim(1) };
        let jitter_span = ((MAX_TRADE_INTERVAL_MS - MIN_TRADE_INTERVAL_MS) as f64 * TIMING_JITTER_RANGE).max(50.0);
        let jitter = ld * jitter_span;
        let seed = MIN_TRADE_INTERVAL_MS as f64 + base * 0.35 + (sequence_idx as f64).sqrt() * 12.0;
        let delay = (seed + jitter).clamp(MIN_TRADE_INTERVAL_MS as f64, MAX_TRADE_INTERVAL_MS as f64);
        delay as u64
    }

    /// Erlang-2 draw with mean mean_ms (sum of two exponentials) (PIECE 69)
    #[inline]
    fn erlang2_ms(&mut self, mean_ms: f64) -> f64 {
        let u1 = self.mixed_uniform().clamp(1e-12,1.0);
        let u2 = self.mixed_uniform().clamp(1e-12,1.0);
        let x1 = -(mean_ms/2.0) * u1.ln();
        let x2 = -(mean_ms/2.0) * u2.ln();
        x1 + x2
    }

    // ======================[UPGRADE: venue via EXP3 + reward hooks]====================
    fn select_venue(&mut self) -> String {
        let cur_slot = self.last_observed_slot.load(Ordering::Relaxed);
        let mut b = self.bandit.write();
        for _ in 0..4 {
            let v = b.sample(&mut self.rng);
            let blocked = self.backoff.read().get(&v).map(|bo| cur_slot < bo.until_slot).unwrap_or(false);
            if !blocked { return v; }
        }
        b.sample(&mut self.rng)
    }

    pub fn update_venue_reward(&self, venue: &str, reward_0_to_1: f64) {
        let mut b = self.bandit.write();
        b.update(venue, reward_0_to_1.clamp(0.0, 1.0));
    }

    /// Report observed inclusion price (μ-lamports/CU) from confirmed or landed tx.
    pub fn on_inclusion_feedback(&self, micro_lamports_per_cu: u64) {
        self.tip_model.write().observe_price(micro_lamports_per_cu);
    }

    /// Composite venue result: success, slippage, latency, and bandit reward
    pub fn on_tx_result(&self, venue: &str, success: bool, slippage_bps: f64, sent_slot: u64, landed_slot: Option<u64>) {
        let latency_slots = landed_slot.map(|ls| ls.saturating_sub(sent_slot) as f64).unwrap_or(6.0);
        {
            let mut map = self.venue_stats.write();
            let vs = map.entry(venue.to_string()).or_insert_with(VenueStats::new);
            vs.observe(success, slippage_bps.max(0.0), latency_slots.max(0.0));
        }
        let r = self.venue_stats.read().get(venue).map(|v| v.reward()).unwrap_or(0.5);
        let mut b = self.bandit.write();
        b.update(venue, r);
    }

    // ======================[UPGRADE: decoy trades that don't lose]=====================
    fn should_inject_decoy(&mut self) -> bool {
        let m = self.pattern_metrics.read();
        let detection_risk = (1.0 - m.entropy) * 0.3
            + m.periodicity_score * 0.3
            + m.volume_consistency * 0.2
            + m.timing_regularity * 0.2;
        self.rng.gen_bool(detection_risk.min(0.8))
    }

    fn generate_decoy_trades_non_loss(&mut self, original_volume: u64, direction: TradeDirection) -> Vec<NoiseInjectedTrade> {
        if matches!(self.config.decoy_mode, DecoyMode::Off) {
            return Vec::new();
        }
        let num_decoys = self.rng.gen_range(1..=3);
        let mut v = Vec::with_capacity(num_decoys);
        for i in 0..num_decoys {
            let venue = self.select_venue();
            let memo = format!("decoy:{}:{}:{}:{}", self.secret_salt, i, SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis(), venue);
            let decoy_vol = (original_volume / (20 * (i + 2) as u64)).max(1000);

            v.push(NoiseInjectedTrade {
                volume: decoy_vol,
                direction,
                venue,
                delay_ms: self.rng.gen_range(80..800),
                is_decoy: true,
                decoy_memo: Some(memo),
            });
        }
        v
    }

    fn update_volume_distribution(&self, volume: u64) {
        let mut distribution = self.volume_distribution.write();
        distribution.push(volume as f64);
        if distribution.len() > MAX_PATTERN_HISTORY { distribution.remove(0); }
    }

    pub fn record_trade(&self, pattern: TradePattern) {
        let mut history = self.pattern_history.write();
        history.push_back(pattern);
        if history.len() > MAX_PATTERN_HISTORY { history.pop_front(); }
        *self.last_trade_time.write() = Instant::now();
    }

    pub fn get_pattern_health(&self) -> PatternHealth {
        let metrics = self.pattern_metrics.read();
        let entropy_score = (metrics.entropy / self.config.entropy_target).min(1.0);
        let periodicity_health = (1.0 - metrics.periodicity_score).clamp(0.0, 1.0);
        let volume_health = (1.0 - metrics.volume_consistency).clamp(0.0, 1.0);
        let timing_health = (1.0 - metrics.timing_regularity).clamp(0.0, 1.0);

        let overall = (entropy_score * 0.45 + periodicity_health * 0.2 + volume_health * 0.2 + timing_health * 0.15).min(1.0);
        PatternHealth {
            overall_score: overall,
            entropy_score,
            periodicity_health,
            volume_health,
            timing_health,
            requires_intervention: overall < 0.70,
        }
    }

    pub fn adjust_noise_parameters(&mut self, market_volatility: f64, detection_pressure: f64) {
        let base_entropy_target = 0.85;
        self.config.entropy_target = (base_entropy_target + detection_pressure * 0.12).min(0.96);
        self.config.volume_randomization = detection_pressure > 0.25;
        self.config.timing_randomization = detection_pressure > 0.18;
        self.config.order_splitting = detection_pressure > 0.35;
        self.config.pattern_breaking = detection_pressure > 0.45;
        let vol_factor = (market_volatility * 100.0).min(50.0) as u16;
        self.config.max_slippage_bps = 28 + vol_factor;
    }

    pub fn calculate_noise_cost(&self, trades: &[NoiseInjectedTrade]) -> NoiseCost {
        let total_volume: u64 = trades.iter().map(|t| t.volume).sum();
        let decoy_volume: u64 = trades.iter().filter(|t| t.is_decoy).map(|t| t.volume).sum();
        let num_splits = trades.iter().filter(|t| !t.is_decoy).count().max(1);
        let split_cost_bps = (num_splits as u16).saturating_sub(1) * 2;
        let timing_cost_bps = trades.iter().map(|t| (t.delay_ms / 1000) as u16).sum::<u16>().min(10);
        let decoy_cost_bps = if matches!(self.config.decoy_mode, DecoyMode::Off) { 0 } else { ((decoy_volume as f64 / total_volume.max(1) as f64) * 100.0) as u16 };
        NoiseCost {
            total_cost_bps: split_cost_bps + timing_cost_bps + decoy_cost_bps,
            split_cost_bps,
            timing_cost_bps,
            decoy_cost_bps,
            estimated_slippage_bps: self.config.max_slippage_bps,
        }
    }

    // [UPGRADE] keyed hashing with salt mixed in to avoid correlating fingerprints
    pub fn generate_signature_hash(&mut self, trade: &NoiseInjectedTrade) -> u64 {
        // Keyed SipHash13 with salt-derived keys; coarse time bin to reduce leakage
        let k0 = self.secret_salt.rotate_left(13) ^ 0x9E37_79B9_7F4A_7C15;
        let k1 = self.secret_salt.rotate_right(7) ^ 0xC2B2_AE35_7D3E_DBF5;
        let mut hasher = SipHasher13::new_with_keys(k0, k1);
        self.secret_salt.hash(&mut hasher);
        trade.volume.hash(&mut hasher);
        trade.venue.hash(&mut hasher);
        (trade.direction as u8).hash(&mut hasher);
        let secs = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() / 2) * 2;
        secs.hash(&mut hasher);
        hasher.finish()
    }

    pub fn validate_noise_effectiveness(&self, window_minutes: u64) -> NoiseEffectiveness {
        let history = self.pattern_history.read();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let start = now - window_minutes * 60;
        let recent: Vec<&TradePattern> = history.iter().filter(|p| p.timestamp >= start).collect();
        drop(history);

        if recent.is_empty() {
            return NoiseEffectiveness { pattern_disruption_score: 1.0, timing_randomness: 1.0, volume_dispersion: 1.0, venue_distribution: 1.0, overall_effectiveness: 1.0 };
        }
        let pattern_disruption = self.calculate_pattern_disruption(&recent);
        let timing_randomness = self.calculate_timing_randomness(&recent);
        let volume_dispersion = self.calculate_volume_dispersion(&recent);
        let venue_distribution = self.calculate_venue_distribution(&recent);
        NoiseEffectiveness {
            pattern_disruption_score: pattern_disruption,
            timing_randomness,
            volume_dispersion,
            venue_distribution,
            overall_effectiveness: (pattern_disruption * 0.33 + timing_randomness * 0.27 + volume_dispersion * 0.20 + venue_distribution * 0.20),
        }
    }

    fn calculate_pattern_disruption(&self, patterns: &[&TradePattern]) -> f64 {
        if patterns.len() < 3 { return 1.0; }
        let mut s = 0.0;
        for w in patterns.windows(3) {
            let d1 = (w[1].volume as f64 - w[0].volume as f64).abs();
            let d2 = (w[2].volume as f64 - w[1].volume as f64).abs();
            let avg = (w[0].volume + w[1].volume + w[2].volume) as f64 / 3.0;
            s += ((d1 + d2) / 2.0) / avg.max(1.0);
        }
        (s / (patterns.len() - 2) as f64).min(1.0)
    }

    fn calculate_timing_randomness(&self, patterns: &[&TradePattern]) -> f64 {
        if patterns.len() < 2 { return 1.0; }
        let mut prev = None;
        let mut iv = Vec::with_capacity(patterns.len());
        for p in patterns {
            if let Some(t) = prev { iv.push(p.timestamp.saturating_sub(t) as f64); }
            prev = Some(p.timestamp);
        }
        let mean = iv.iter().sum::<f64>() / iv.len().max(1) as f64;
        let var = iv.iter().map(|x| { let z = *x - mean; z*z }).sum::<f64>() / iv.len().max(1) as f64;
        let cv = if mean > 0.0 { (var.sqrt() / mean).min(1.0) } else { 1.0 };
        cv
    }

    fn calculate_volume_dispersion(&self, patterns: &[&TradePattern]) -> f64 {
        let vols: Vec<f64> = patterns.iter().map(|p| p.volume as f64).collect();
        let mean = vols.iter().sum::<f64>() / vols.len().max(1) as f64;
        let var = vols.iter().map(|v| { let z = *v - mean; z*z }).sum::<f64>() / vols.len().max(1) as f64;
        let cv = if mean > 0.0 { (var.sqrt() / mean).min(1.0) } else { 1.0 };
        cv
    }

    fn calculate_venue_distribution(&self, patterns: &[&TradePattern]) -> f64 {
        let mut c: HashMap<&str, usize> = HashMap::new();
        for p in patterns { *c.entry(&p.venue).or_insert(0) += 1; }
        let total = patterns.len() as f64;
        let ent: f64 = c.values().map(|&n| { let p = n as f64 / total; if p > 0.0 { -p * p.log2() } else { 0.0 } }).sum();
        let max_ent = (c.len().max(1) as f64).log2();
        if max_ent > 0.0 { (ent / max_ent).clamp(0.0, 1.0) } else { 0.0 }
    }

    pub fn optimize_for_market_conditions(&mut self, liquidity: f64, volatility: f64, competition_level: f64) {
        // [UPGRADE] couple to bandit exploration rate
        {
            let mut b = self.bandit.write();
            b.gamma = (0.08 + competition_level * 0.12).clamp(0.05, 0.22);
            b.eta = (0.05 + volatility * 0.08).clamp(0.04, 0.18);
        }

        // keep earlier toggles; these work well
        self.config.pattern_breaking = competition_level > 0.5;
        self.config.order_splitting = competition_level > 0.55 || liquidity > 0.5;

        // small slippage adaptation
        let extra = ((liquidity - 0.5).abs() + volatility).min(1.0);
        self.config.max_slippage_bps = (self.config.max_slippage_bps as f64 * (1.0 + 0.5 * extra)).min(120.0) as u16;
    }

    pub fn emergency_pattern_break(&mut self) -> Vec<NoiseInjectedTrade> {
        // [UPGRADE] emergency = memo-only/no-fill decoys + venue churn; safe by design
        let num = self.rng.gen_range(5..=10);
        let mut v = Vec::with_capacity(num);
        for i in 0..num {
            let venue = self.select_venue();
            let memo = format!("emerg_decoy:{}:{}", self.secret_salt, i);
            v.push(NoiseInjectedTrade {
                volume: self.rng.gen_range(500_000..5_000_000), // tiny
                direction: if self.rng.gen_bool(0.5) { TradeDirection::Buy } else { TradeDirection::Sell },
                venue,
                delay_ms: self.rng.gen_range(40..1600),
                is_decoy: true,
                decoy_memo: Some(memo),
            });
        }
        v
    }

    pub fn calculate_optimal_noise_level(&self, current_pnl: f64, detection_risk: f64, market_impact: f64) -> f64 {
        let pnl_factor = if current_pnl > 0.0 { 1.0 + current_pnl.min(0.10)*2.0 } else { 1.0 - current_pnl.abs().min(0.05)*2.0 };
        let risk_factor = 1.0 + (detection_risk * 3.0);
        let impact_factor = 1.0 - (market_impact.min(0.3) * 0.5);
        (pnl_factor * risk_factor * impact_factor).clamp(0.5, 2.0)
    }

    pub fn get_next_trade_timing(&mut self, base_delay: u64) -> u64 {
        let timing_risk = self.pattern_metrics.read().timing_regularity;
        if timing_risk > 0.8 {
            let f = self.rng.gen_range(0.1..3.0);
            (base_delay as f64 * f) as u64
        } else if timing_risk > 0.5 {
            let f = self.rng.gen_range(0.5..1.6);
            (base_delay as f64 * f) as u64
        } else {
            let jitter = (base_delay / 10).max(5);
            base_delay + self.rng.gen_range(0..jitter)
        }
    }

    pub fn should_skip_trade(&mut self, consecutive_trades: usize) -> bool {
        let pattern_risk = self.pattern_metrics.read().periodicity_score;
        let p = match consecutive_trades {
            0..=2 => 0.0,
            3..=5 => 0.1 + pattern_risk * 0.1,
            6..=8 => 0.2 + pattern_risk * 0.2,
            _ => 0.4 + pattern_risk * 0.3,
        };
        self.rng.gen_bool(p.min(0.7))
    }

    pub fn generate_anti_pattern_sequence(&mut self, expected_volume: u64, expected_direction: TradeDirection) -> Vec<NoiseInjectedTrade> {
        let mut seq = Vec::new();
        let opposite = match expected_direction { TradeDirection::Buy => TradeDirection::Sell, TradeDirection::Sell => TradeDirection::Buy };
        let volumes = [expected_volume * 2, expected_volume / 3, expected_volume * 5 / 4, expected_volume / 2];

        for (i, vol) in volumes.iter().enumerate() {
            let dir = if i % 2 == 0 { opposite } else { expected_direction };
            seq.push(NoiseInjectedTrade {
                volume: self.randomize_volume(*vol),
                direction: dir,
                venue: self.select_venue(),
                delay_ms: self.rng.gen_range(100..1500),
                is_decoy: true,
                decoy_memo: Some(format!("anti_pat:{}:{}", self.secret_salt, i)),
            });
        }
        seq
    }

    pub fn adapt_to_competitor_patterns(&mut self, competitor_patterns: &[TradePattern]) {
        if competitor_patterns.is_empty() { return; }
        let mut prev = None;
        let mut iv = Vec::with_capacity(competitor_patterns.len());
        for p in competitor_patterns {
            if let Some(t) = prev { iv.push(p.timestamp.saturating_sub(t)); }
            prev = Some(p.timestamp);
        }
        if !iv.is_empty() {
            let avg = iv.iter().sum::<u64>() / iv.len() as u64;
            // [UPGRADE] we just ensure timing_randomization stays on; Poisson sampler will de-correlate
            self.config.timing_randomization = true;
            // small nudge: alter EWMA baseline by pushing synthetic point
            self.interval_stats.write().push(avg as f64 * if avg < 1000 { 2.5 } else { 0.5 });
        }

        // pull volumes apart from competitor average
        let avg_vol = competitor_patterns.iter().map(|p| p.volume).sum::<u64>() / competitor_patterns.len().max(1) as u64;
        let mut d = self.volume_distribution.write();
        d.clear();
        d.push((avg_vol as f64 * 0.28).max(500_000.0));
        d.push((avg_vol as f64 * 2.6).min(2_000_000_000.0));
    }

    pub fn finalize_trade_batch(&mut self, trades: &mut Vec<NoiseInjectedTrade>) {
        // low-discrepancy shuffle
        let mut idx: Vec<usize> = (0..trades.len()).collect();
        self.ld_shuffle(&mut idx);

        let original = trades.clone();
        for (new_i, &old_i) in idx.iter().enumerate() { trades[new_i] = original[old_i].clone(); }

        // monotone cumulative delay
        let mut cum = 0u64;
        for t in trades.iter_mut() { t.delay_ms = { cum += t.delay_ms; cum }; }

        // soft venue rebalance
        let mut count: HashMap<String, usize> = HashMap::new();
        for t in trades.iter() { *count.entry(t.venue.clone()).or_insert(0) += 1; }
        let max_per = (trades.len() as f64 * 0.42).ceil() as usize;
        for (vstr, c) in count {
            if c > max_per {
                let mut changed = 0;
                for t in trades.iter_mut() {
                    if changed >= c - max_per { break; }
                    if t.venue == vstr {
                        t.venue = self.select_venue();
                        changed += 1;
                    }
                }
            }
        }
    }

    pub fn get_pattern_signature(&self) -> PatternSignature {
        let history = self.pattern_history.read();
        let metrics = self.pattern_metrics.read();
        PatternSignature {
            entropy: metrics.entropy,
            periodicity: metrics.periodicity_score,
            volume_consistency: metrics.volume_consistency,
            timing_regularity: metrics.timing_regularity,
            trade_count: history.len(),
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        }
    }

    pub fn reset_pattern_history(&mut self) {
        self.pattern_history.write().clear();
        self.volume_distribution.write().clear();
        *self.last_trade_time.write() = Instant::now();
        let mut m = self.pattern_metrics.write();
        *m = PatternMetrics { entropy: 1.0, periodicity_score: 0.0, volume_consistency: 0.0, timing_regularity: 0.0, directional_bias: 0.5 };
        *self.interval_stats.write() = EwmaStats::with_alpha(0.35);
        *self.volume_stats.write() = EwmaStats::with_alpha(0.30);
        *self.interval_cusum.write() = Cusum::new(0.25, 3.0);
        *self.volume_cusum.write() = Cusum::new(0.22, 3.0);
        *self.halton.write() = Halton::default();
    }

    // ======================[UPGRADE: helpers]==========================================
    fn mixed_uniform(&mut self) -> f64 {
        // Mix PRNG with Halton to kill clustering & fingerprints
        let u = self.rng.gen::<f64>();
        let ld = { self.halton.write().next(3) }; // base-3
        (u * 0.6 + ld * 0.4).clamp(1e-12, 1.0 - 1e-12)
    }

    fn ld_shuffle<T>(&mut self, arr: &mut [T]) {
        // Fisher-Yates with low-discrepancy biased picks (harder to fingerprint)
        for i in (1..arr.len()).rev() {
            let r = { self.halton.write().next(5) };
            let j = (r * (i as f64 + 1.0)).floor() as usize;
            arr.swap(i, j.min(i));
        }
    }
}

// ======================[UPGRADE: plan shaping + timing jitter]=========================
impl TradePatternNoiseInjector {
    /// Optional: adjust plan volumes by venue profile before building instructions
    pub fn pre_shape_plan_by_venue(&mut self, plan: &mut [NoiseInjectedTrade]) {
        let mut h = self.halton.write();
        for t in plan.iter_mut() {
            let b = match t.venue.as_str() { "serum" => 1.06, "raydium" => 1.00, "jupiter" => 1.02, "orca" => 0.98, _ => 1.00 };
            let ld = h.next(13);
            let mult = b + (ld - 0.5) * 0.06;
            t.volume = ((t.volume as f64 * mult).max(1.0)) as u64;
        }
    }

    /// Nudge each delay toward nearest {prime}ms bucket with tiny LD jitter.
    pub fn prime_jitterize_plan(&mut self, plan: &mut [NoiseInjectedTrade]) {
        let mut h = self.halton.write();
        for t in plan.iter_mut() {
            let p = PRIME_BUCKETS[(h.next(23) * PRIME_BUCKETS.len() as f64).floor() as usize % PRIME_BUCKETS.len()];
            let rem = (t.delay_ms % p) as i64;
            let target = (p / 2) as i64;
            let delta = target - rem;
            let tweak = (delta + ((h.next(29) - 0.5) * 6.0) as i64).clamp(-15, 15);
            t.delay_ms = (t.delay_ms as i64 + tweak).max(1) as u64;
        }
    }

    /// Add extra decoys to reach entropy target given a soft bps budget.
    pub fn rebalance_decoy_density(&mut self,
                                   plan: &mut Vec<NoiseInjectedTrade>,
                                   base_volume: u64,
                                   direction: TradeDirection,
                                   max_extra_decoys: usize,
                                   soft_cost_bps_budget: u16) {
        let m = self.pattern_metrics.read();
        let deficit = (self.config.entropy_target - m.entropy).max(0.0);
        drop(m);
        if deficit <= 0.0 || matches!(self.config.decoy_mode, DecoyMode::Off) { return; }
        let needed = ((deficit * 4.0).ceil() as usize).min(max_extra_decoys);

        let current_cost = self.calculate_noise_cost(plan).total_cost_bps as u32;
        if current_cost >= soft_cost_bps_budget as u32 { return; }

        for i in 0..needed {
            let venue = self.select_venue();
            let memo = format!("dens_decoy:{}:{}:{}", self.secret_salt, i, venue);
            let vol = (base_volume / (15 * (i + 3) as u64)).max(500);
            plan.push(NoiseInjectedTrade { volume: vol, direction, venue, delay_ms: self.rng.gen_range(60..600), is_decoy: true, decoy_memo: Some(memo) });
        }
        self.finalize_trade_batch(plan);
    }
}

// ======================[UPGRADE: autopricer batch builders]============================
impl TradePatternNoiseInjector {
    /// Builds instruction batches with adaptive CU limit & price per trade.
    pub fn to_instruction_batches_autoprice<F>(&self,
                                               trades: &[NoiseInjectedTrade],
                                               mut build_real_trade: F) -> Vec<Vec<Instruction>>
    where
        F: FnMut(&NoiseInjectedTrade) -> Vec<Instruction>,
    {
        let mut out = Vec::with_capacity(trades.len());
        for t in trades {
            let mut h = self.halton.write();
            let mut tm = self.tip_model.write();
            let mut rng = rand::thread_rng();
            let cu_limit = tm.next_cu_limit(&t.venue, &mut rng, &mut h);
            let cu_price = tm.next_cu_price(&mut rng);
            drop(tm);
            let ld = h.next(41);
            let data_cap = if rng.gen::<f64>() < 0.35 { Some(rng.gen_range(8_000..64_000)) } else { None };
            let mut v: Vec<Instruction> = cb_trio_randomized(cu_limit, cu_price, data_cap, ld);
            if t.is_decoy || matches!(self.config.decoy_mode, DecoyMode::MemoOnly | DecoyMode::Ed25519Heavy) {
                // Use configured decoy instructions
                let mut decoys = self.decoy_instructions();
                v.append(&mut decoys);
                reorder_non_state_ixs(&mut v);
                out.push(v);
            } else {
                let real = build_real_trade(t);
                v.extend(real);
                reorder_non_state_ixs(&mut v);
                // parity memo pad for real trades
                // SAFETY: small and stateless
                // (do before instruction padding)
                // mutable self not available in &self method; keep parity in padded variant
                pad_instruction_list(&mut v, &mut rng, &mut h, 8, 2);
                out.push(v);
            }
        }
        out
    }

    /// Same as autopricer, but pads decoy memos to normalized sizes.
    pub fn to_instruction_batches_autoprice_padded<F>(&mut self,
                                                      trades: &[NoiseInjectedTrade],
                                                      mut build_real_trade: F) -> Vec<Vec<Instruction>>
    where
        F: FnMut(&NoiseInjectedTrade) -> Vec<Instruction>,
    {
        let mut out = Vec::with_capacity(trades.len());
        for t in trades {
            let mut h = self.halton.write();
            let mut tm = self.tip_model.write();
            let mut rng = rand::thread_rng();
            let cu_limit = tm.next_cu_limit(&t.venue, &mut rng, &mut h);
            let cu_price = tm.next_cu_price(&mut rng);
            drop(tm);
            let ld = h.next(41);
            let data_cap = if rng.gen::<f64>() < 0.35 { Some(rng.gen_range(8_000..64_000)) } else { None };
            let mut v: Vec<Instruction> = cb_trio_randomized(cu_limit, cu_price, data_cap, ld);
            if t.is_decoy || matches!(self.config.decoy_mode, DecoyMode::MemoOnly | DecoyMode::Ed25519Heavy) {
                match self.config.decoy_mode {
                    DecoyMode::MemoOnly => {
                        let memo = t.decoy_memo.clone().unwrap_or_else(|| "d".to_string());
                        let raw = self.encode_memo_with_variant(&memo);
                        let bytes = self.pad_memo_raw_bytes(raw);
                        v.push(build_memo_decoy_bytes(bytes));
                    }
                    DecoyMode::Ed25519Heavy => {
                        let mut decoys = self.decoy_instructions();
                        v.append(&mut decoys);
                    }
                    _ => {}
                }
                reorder_non_state_ixs(&mut v);
                out.push(v);
            } else {
                let real = build_real_trade(t);
                v.extend(real);
                self.maybe_real_memo_pad(&mut v);
                reorder_non_state_ixs(&mut v);
                pad_instruction_list(&mut v, &mut rng, &mut h, 8, 2);
                out.push(v);
            }
        }
        out
    }
}

// ======================[UPGRADE: SmallVec fast builder]===============================
impl TradePatternNoiseInjector {
    pub fn to_instruction_batches_autoprice_padded_fast<F>(&mut self,
                                                           trades: &[NoiseInjectedTrade],
                                                           mut build_real_trade: F) -> Vec<Vec<Instruction>>
    where
        F: FnMut(&NoiseInjectedTrade) -> Vec<Instruction>,
    {
        let mut out = Vec::with_capacity(trades.len());
        for t in trades {
            let mut h = self.halton.write();
            let mut tm = self.tip_model.write();
            let mut rng = rand::thread_rng();
            let cu_limit = tm.next_cu_limit(&t.venue, &mut rng, &mut h);
            let cu_price = tm.next_cu_price(&mut rng);
            let ld = h.next(41);
            let data_cap = if rng.gen::<f64>() < 0.35 { Some(rng.gen_range(8_000..64_000)) } else { None };
            let cb = cb_trio_randomized(cu_limit, cu_price, data_cap, ld);
            drop(tm); drop(h);

            let mut v: SmallVec<[Instruction; 8]> = SmallVec::new();
            v.extend(cb);

            if t.is_decoy || matches!(self.config.decoy_mode, DecoyMode::MemoOnly | DecoyMode::Ed25519Heavy) {
                match self.config.decoy_mode {
                    DecoyMode::MemoOnly => {
                        let memo = t.decoy_memo.clone().unwrap_or_else(|| "d".to_string());
                        let raw = self.encode_memo_with_variant(&memo);
                        let bytes = self.pad_memo_raw_bytes(raw);
                        v.push(build_memo_decoy_bytes(bytes));
                    }
                    DecoyMode::Ed25519Heavy => { for ix in self.decoy_instructions() { v.push(ix); } }
                    _ => {}
                }
            } else {
                let real = build_real_trade(t);
                v.extend(real);
                self.maybe_real_memo_pad(&mut v);
                let mut h2 = self.halton.write();
                let mut rng2 = rand::thread_rng();
                pad_instruction_list(&mut v, &mut rng2, &mut h2, 8, 2);
            }
            let mut batch = v.into_vec();
            let ld_order = { self.halton.write().next_dim(5) };
            reorder_non_state_ixs_safe(&mut batch, ld_order);
            out.push(batch);
        }
        out
    }
}

// ======================[UPGRADE: Plan TTL split]======================================
impl TradePatternNoiseInjector {
    /// Returns split index where trades[0..idx) are safe under ttl_slots given current_slot.
    pub fn split_by_ttl(&self, trades: &[NoiseInjectedTrade], ttl_slots: u64, _current_slot: u64) -> usize {
        let slot_ms = self.slot_time.read().slot_ms().max(1);
        let ttl_ms  = ttl_slots.saturating_mul(slot_ms);
        let mut safe = 0usize;
        for (i, t) in trades.iter().enumerate() { if t.delay_ms <= ttl_ms { safe = i + 1; } else { break; } }
        safe
    }
}

// ======================[UPGRADE: LUT pool (struct only)]==============================
use solana_sdk::message::v0::MessageAddressTableLookup;
#[derive(Clone)]
pub struct LutPool { lookups: Vec<MessageAddressTableLookup>, idx: u64 }
impl LutPool {
    pub fn new(lookups: Vec<MessageAddressTableLookup>) -> Self { assert!(!lookups.is_empty(), "LUT pool cannot be empty"); Self { lookups, idx: 0 } }
    #[inline] pub fn select(&mut self, h: &mut Halton) -> MessageAddressTableLookup {
        self.idx = self.idx.wrapping_add(1);
        let r = h.next(37);
        let i = (r * self.lookups.len() as f64).floor() as usize;
        self.lookups[i.min(self.lookups.len()-1)].clone()
    }
}

// ======================[UPGRADE: Timing shape for splits]=============================
#[derive(Copy, Clone)]
enum TimingShape { Linear, Convex, Concave, Saw }
impl TradePatternNoiseInjector {
    #[inline] fn pick_timing_shape(&mut self) -> TimingShape {
        let r = self.mixed_uniform();
        if r < 0.25 { TimingShape::Linear } else if r < 0.50 { TimingShape::Convex } else if r < 0.75 { TimingShape::Concave } else { TimingShape::Saw }
    }
    pub fn shape_split_delays(&mut self, plan: &mut [NoiseInjectedTrade]) {
        if plan.len() < 3 { return; }
        let shape = self.pick_timing_shape();
        let n = plan.len() as f64;
        for (i, t) in plan.iter_mut().enumerate() {
            let x = (i as f64 + 1.0) / n;
            let mul = match shape {
                TimingShape::Linear  => 1.0,
                TimingShape::Convex  => (x * x * 0.5 + 0.75).clamp(0.8, 1.3),
                TimingShape::Concave => ((1.0 - (1.0 - x).powi(2)) * 0.5 + 0.75).clamp(0.8, 1.3),
                TimingShape::Saw     => (1.0 + (if i % 2 == 0 { 0.12 } else { -0.10 })).clamp(0.85, 1.2),
            };
            t.delay_ms = ((t.delay_ms as f64) * mul) as u64;
        }
        let mut cum = 0u64; for tt in plan.iter_mut() { cum = cum.saturating_add(tt.delay_ms); tt.delay_ms = cum; }
    }
}

// ======================[UPGRADE: bundles grouping]=====================================
pub fn group_batches_into_bundles(batches: Vec<Vec<Instruction>>, max_per_bundle: usize) -> Vec<Vec<Vec<Instruction>>> {
    let m = max_per_bundle.max(1);
    let mut out: Vec<Vec<Vec<Instruction>>> = Vec::with_capacity((batches.len()+m-1)/m);
    let mut cur: Vec<Vec<Instruction>> = Vec::with_capacity(m);
    for b in batches {
        cur.push(b);
        if cur.len() == m { out.push(cur); cur = Vec::with_capacity(m); }
    }
    if !cur.is_empty() { out.push(cur); }
    out
}

// ======================[UPGRADE: fee-payer pool]=======================================
#[derive(Clone)]
pub struct FeePayerPool {
    pool_real: Vec<Arc<Keypair>>, 
    pool_decoy: Vec<Arc<Keypair>>, 
    idx: u64,
}
impl FeePayerPool {
    pub fn new(real: Vec<Arc<Keypair>>, decoy: Vec<Arc<Keypair>>) -> Self {
        assert!(!real.is_empty(), "real fee-payer pool cannot be empty");
        assert!(!decoy.is_empty(), "decoy fee-payer pool cannot be empty");
        Self { pool_real: real, pool_decoy: decoy, idx: 0 }
    }
    #[inline]
    pub fn select(&mut self, is_decoy: bool, halton: &mut Halton) -> Arc<Keypair> {
        self.idx = self.idx.wrapping_add(1);
        let r = halton.next(17);
        if is_decoy {
            let i = ((r * self.pool_decoy.len() as f64).floor() as usize).min(self.pool_decoy.len()-1);
            self.pool_decoy[i].clone()
        } else {
            let i = (((r + 0.37).fract()) * self.pool_real.len() as f64).floor() as usize;
            self.pool_real[i.min(self.pool_real.len()-1)].clone()
        }
    }
}

// ======================[types]=========================================================
#[derive(Debug, Clone)]
pub struct NoiseInjectedTrade {
    pub volume: u64,
    pub direction: TradeDirection,
    pub venue: String,
    pub delay_ms: u64,
    pub is_decoy: bool,
    // [UPGRADE] optional memo for decoy/memo-mode
    pub decoy_memo: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PatternHealth {
    pub overall_score: f64,
    pub entropy_score: f64,
    pub periodicity_health: f64,
    pub volume_health: f64,
    pub timing_health: f64,
    pub requires_intervention: bool,
}

#[derive(Debug, Clone)]
  pub struct NoiseCost {
    pub total_cost_bps: u16,
    pub split_cost_bps: u16,
    pub timing_cost_bps: u16,
    pub decoy_cost_bps: u16,
    pub estimated_slippage_bps: u16,
  }

 // ======================[INTEGRATION: Execution + Analytics modules inline]======================
 // All requested modules are included here as nested modules to avoid symbol conflicts and to
 // satisfy the requirement that everything lives in this file.
 
 pub mod execution {
     pub mod tip_optimizer {
         use anyhow::Result;
         use serde::{Deserialize, Serialize};
         use statrs::function::logistic::logistic;
 
         #[derive(Clone, Copy, Debug, Serialize, Deserialize)]
         pub struct TipEnv {
             pub gross_profit_lamports: i64,
             pub cu: u32,
             pub cu_price_micro: f64,
             pub tip_frontier: i64,
             pub tps: f64,
             pub block_fill: f64,
             pub relay_slope_k: f64,
             pub relay_mid_t0: f64,
         }
 
 // ======================[UPGRADE-5 modules inline]====================================
 // Analytics: per-leader validator profile
 pub mod analytics5 {
     pub mod validator_profile {
         use std::collections::HashMap;
         use serde::{Deserialize, Serialize};
 
         #[derive(Clone, Copy, Debug, Serialize, Deserialize)]
         pub struct LeaderKey(pub [u8;32]);
 
         #[derive(Clone, Copy, Debug, Serialize, Deserialize)]
         pub struct LeaderStats {
             pub k: f64,
             pub t0: f64,
             pub ghost_rate: f64,
             pub cu_weight: f64,
             pub updated_slot: u64,
         }
 
         #[derive(Default)]
         pub struct LeaderBook {
             map: HashMap<[u8;32], LeaderStats>,
             k_bounds: (f64,f64),
             t0_floor: f64,
         }
 
         impl LeaderBook {
             pub fn new(k_min: f64, k_max: f64, t0_floor: f64) -> Self {
                 Self { map: HashMap::new(), k_bounds:(k_min,k_max), t0_floor }
             }
             pub fn upsert(&mut self, key: LeaderKey, k: f64, t0: f64, ghost: f64, cu_weight: f64, slot: u64) {
                 let k = k.clamp(self.k_bounds.0, self.k_bounds.1);
                 let t0 = t0.max(self.t0_floor);
                 let ghost = ghost.clamp(0.0, 1.0);
                 let cuw = cu_weight.clamp(0.0, 1.0);
                 self.map.insert(key.0, LeaderStats { k, t0, ghost_rate: ghost, cu_weight: cuw, updated_slot: slot });
             }
             pub fn get(&self, key: LeaderKey) -> Option<LeaderStats> { self.map.get(&key.0).copied() }
             pub fn fallback(&self, k: f64, t0: f64) -> LeaderStats {
                 LeaderStats { k, t0, ghost_rate: 0.0, cu_weight: 0.5, updated_slot: 0 }
             }
         }
     }
 }
 
 // Scheduling: TTI cutoff
 pub mod scheduling5 {
     pub mod tti_cutoff {
         #[derive(Clone, Copy, Debug)]
         pub struct TtiEnv {
             pub slot_ms: u64,
             pub now_ms_into_slot: u64,
             pub relay_rtt_ms: u64,
             pub leader_buffer_ms: u64,
             pub build_ms: u64,
         }
         pub fn can_land(env: TtiEnv) -> bool {
             let left = env.slot_ms.saturating_sub(env.now_ms_into_slot);
             let need = env.build_ms + env.relay_rtt_ms + env.leader_buffer_ms;
             left > need
         }
     }
 }
 
 // Network: relay health
 pub mod network5 {
     pub mod relay_health {
         use serde::{Deserialize, Serialize};
         #[derive(Clone, Copy, Debug, Serialize, Deserialize)]
         pub struct HealthCfg { pub target_p50_ms: f64, pub target_err: f64, pub decay: f64 }
         #[derive(Clone, Copy, Debug, Serialize, Deserialize)]
         pub struct HealthState { pub p50_ms: f64, pub err_rate: f64, pub score: f64 }
         impl HealthState {
             pub fn new() -> Self { Self { p50_ms: 999.0, err_rate: 1.0, score: 0.0 } }
             pub fn update(&mut self, cfg: HealthCfg, sample_ms: f64, ok: bool) {
                 let d = cfg.decay.clamp(0.0, 1.0);
                 self.p50_ms = d*self.p50_ms + (1.0-d)*sample_ms;
                 let e_inc = if ok { 0.0 } else { 1.0 };
                 self.err_rate = (d*self.err_rate + (1.0-d)*e_inc).clamp(0.0, 1.0);
                 let lat_pen = (cfg.target_p50_ms / self.p50_ms.max(1.0)).clamp(0.0, 1.0);
                 let err_pen = ((cfg.target_err + 1e-6) / (self.err_rate + 1e-6)).clamp(0.0, 1.0);
                 self.score = (lat_pen * 0.6 + err_pen * 0.4).clamp(0.0, 1.0);
             }
         }
     }
 }
 
 // Analysis: account hotness map
 pub mod analysis5 {
     pub mod account_hotness {
         use std::collections::hash_map::Entry;
         use std::collections::HashMap;
         #[derive(Clone, Copy, Debug)]
         pub struct DecayCfg { pub half_life_obs: f64, pub cap: f64 }
         #[derive(Default)]
         pub struct HotMap { map: HashMap<[u8;32], f64>, lambda: f64, cap: f64 }
         impl HotMap {
             pub fn new(cfg: DecayCfg) -> Self { let lambda = (0.5f64).powf(1.0 / cfg.half_life_obs.max(1.0)); Self { map: HashMap::new(), lambda, cap: cfg.cap } }
             pub fn observe(&mut self, key: [u8;32]) { match self.map.entry(key) { Entry::Occupied(mut e) => { let v = e.get_mut(); *v = (*v * self.lambda + 1.0).min(self.cap); }, Entry::Vacant(v) => { v.insert(1.0); } } }
             pub fn intensity(&self, key: &[u8;32]) -> f64 { *self.map.get(key).unwrap_or(&0.0) }
             pub fn bundle_hotness(&self, keys: &[[u8;32]]) -> f64 { if keys.is_empty() { return 0.0; } let s: f64 = keys.iter().map(|k| self.intensity(k)).sum(); s / (keys.len() as f64) }
         }
     }
 }
 
 // Execution: tail pricer and bundle de-dup
 pub mod execution5 {
     pub mod tail_pricer {
         #[derive(Clone, Copy, Debug)]
         pub struct TailEnv { pub k: f64, pub t0: f64, pub tip_base: f64, pub gross_lamports: f64, pub cu_fee_lamports: f64, pub cap_mult: f64 }
         fn sigmoid(x: f64) -> f64 { 1.0/(1.0+(-x).exp()) }
         pub fn price_tail(e: TailEnv) -> i64 {
             let p = sigmoid(e.k * (e.tip_base - e.t0));
             let dp = e.k * p * (1.0 - p);
             let m = dp * (e.gross_lamports - e.tip_base - e.cu_fee_lamports) - p;
             if m <= 0.0 { return 0; }
             let denom = (e.k * dp * (e.gross_lamports - e.tip_base - e.cu_fee_lamports)).abs() + dp + 1e-9;
             let newton = (m / denom).max(0.0);
             let delta = (0.3 * newton).min(e.tip_base * e.cap_mult);
             delta.round() as i64
         }
     }
     pub mod bundle_dedup {
         use std::collections::HashSet;
         #[derive(Default)]
         pub struct Dedup { seen: HashSet<u64> }
         fn hash64(v: &[u8]) -> u64 { use std::hash::{Hash, Hasher}; use std::collections::hash_map::DefaultHasher; let mut h = DefaultHasher::new(); v.hash(&mut h); h.finish() }
         pub fn econ_fingerprint(accounts: &[[u8;32]], tip: i64, mu_per_cu: u64, cu: u32) -> u64 {
             let mut acc_bytes = Vec::with_capacity(accounts.len()*32);
             let mut acc = accounts.to_vec(); acc.sort_unstable();
             for a in acc { acc_bytes.extend_from_slice(&a); }
             let spend = tip as i128 + ((mu_per_cu as i128 * cu as i128) / 1_000_000);
             let mut total = acc_bytes; total.extend_from_slice(&spend.to_le_bytes()); hash64(&total)
         }
         impl Dedup { pub fn new() -> Self { Self { seen: HashSet::new() } } pub fn check_and_mark(&mut self, fp: u64) -> bool { if self.seen.contains(&fp) { false } else { self.seen.insert(fp); true } } pub fn clear_slot(&mut self) { self.seen.clear(); } }
     }
 }
 
 // Security: key splay
 pub mod security5 {
     pub mod key_splay {
         use rand::{Rng, SeedableRng};
         use rand::rngs::StdRng;
         pub fn splay_fee_payer<'a>(pubkeys: &'a [[u8;32]], slot: u64, seed: u64) -> &'a [u8;32] {
             assert!(!pubkeys.is_empty(), "empty fee payer set");
             let mut rng = StdRng::seed_from_u64(slot ^ seed ^ 0xD1A5_5P1A);
             let idx = rng.gen_range(0..pubkeys.len());
             &pubkeys[idx]
         }
     }
 }
 
         #[derive(Clone, Copy, Debug, Serialize, Deserialize)]
         pub struct TipDecision {
             pub tip_lamports: i64,
             pub cu_price_paid_lamports: i64,
             pub ev_lamports: f64,
             pub inclusion_prob: f64,
             pub lifted: bool,
         }
 
         const TPS_PEAK: f64 = 3_500.0;
         const LIFT_MULT_SOFT: f64 = 1.10;
         const LIFT_MULT_HARD: f64 = 1.25;
 
         fn inclusion_prob_lamports(tip: f64, k: f64, t0: f64) -> f64 {
             logistic(k * (tip - t0))
         }
 
         pub fn optimize_tip(env: TipEnv) -> Result<TipDecision> {
             let mut t0 = env.relay_mid_t0.max(1.0);
             let mut k = env.relay_slope_k.max(1e-6);
             let mut lifted = false;
 
             if env.tps >= TPS_PEAK || env.block_fill >= 0.85 {
                 t0 *= if env.block_fill >= 0.92 { LIFT_MULT_HARD } else { LIFT_MULT_SOFT };
                 lifted = true;
                 k *= 1.08;
             }
 
             let cu_fee = (env.cu as f64) * env.cu_price_micro / 1_000_000.0;
             let base_max_tip = (env.gross_profit_lamports as f64).max(0.0) * 0.80;
             let start = env.tip_frontier.max(0) as f64;
             let end = (start * 3.0).min(base_max_tip.max(start + 1.0));
 
             let steps = 24usize;
             let mut cand = Vec::with_capacity(steps);
             for i in 0..steps {
                 let tip = start + (end - start) * (i as f64) / (steps - 1) as f64;
                 let p = inclusion_prob_lamports(tip, k, t0).clamp(0.0, 1.0);
                 let ev = p * (env.gross_profit_lamports as f64 - tip - cu_fee);
                 cand.push((tip, p, ev));
             }
             cand.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap());
 
             let (t_peak, p_peak, ev_peak) = cand[0];
             let mut tip_opt = t_peak;
             let mut p_opt = p_peak;
             let mut ev_opt = ev_peak;
             let mut r = (t_peak * 0.15).max(10.0);
             for _ in 0..3 {
                 let mut local_best = (tip_opt, p_opt, ev_opt);
                 for j in 0..20 {
                     let t = (t_peak - r) + (2.0 * r) * (j as f64) / 19.0;
                     if t <= 0.0 { continue; }
                     let p = inclusion_prob_lamports(t, k, t0).clamp(0.0, 1.0);
                     let ev = p * (env.gross_profit_lamports as f64 - t - cu_fee);
                     if ev > local_best.2 { local_best = (t, p, ev); }
                 }
                 tip_opt = local_best.0; p_opt = local_best.1; ev_opt = local_best.2;
                 r *= 0.5;
             }
 
             let tip_final = tip_opt.max(start).round() as i64;
             Ok(TipDecision {
                 tip_lamports: tip_final,
                 cu_price_paid_lamports: cu_fee.round() as i64,
                 ev_lamports: ev_opt.max(0.0),
                 inclusion_prob: p_opt,
                 lifted,
             })
         }
     }
 
     pub mod bundle_width {
         use serde::{Deserialize, Serialize};
 
         #[derive(Clone, Copy, Debug, Serialize, Deserialize)]
         pub struct WidthEnv {
             pub gross_profit_lamports: f64,
             pub tip_each_lamports: f64,
             pub cu_fee_each_lamports: f64,
             pub base_success_prob: f64,
             pub disjointness: f64,
             pub max_width: usize,
         }
 
         pub fn optimal_bundle_width(e: WidthEnv) -> usize {
             let maxw = e.max_width.clamp(1, 4);
             let unit_cost = e.tip_each_lamports + e.cu_fee_each_lamports;
             let mut best_w = 1usize;
             let mut best_ev = f64::MIN;
 
             for w in 1..=maxw {
                 let mut prod_fail = 1.0;
                 for i in 0..w {
                     let cf = 1.0 - (1.0 - e.disjointness) * (i as f64 / w as f64);
                     let p_i = (e.base_success_prob * cf).clamp(0.0, 1.0);
                     prod_fail *= 1.0 - p_i;
                 }
                 let p_any = 1.0 - prod_fail;
                 let ev = p_any * e.gross_profit_lamports - (w as f64) * unit_cost;
 
                 if ev > best_ev {
                     best_ev = ev;
                     best_w = w;
                 }
             }
             best_w
         }
     }
 
     pub mod decoy_throttle {
         #[derive(Clone, Copy, Debug)]
         pub struct DecoyEnv {
             pub mu_per_cu: f64,
             pub slot_risk: f64,
             pub tip_frontier: f64,
             pub decoy_unit_cost: f64,
         }
 
         pub struct DecoyDecision { pub allow: bool, pub count: u32 }
 
         pub fn decoy_budget(env: DecoyEnv) -> DecoyDecision {
             if env.slot_risk >= 0.65 { return DecoyDecision { allow: false, count: 0 }; }
             let mu_ok = env.mu_per_cu >= 50.0;
             if !mu_ok { return DecoyDecision { allow: false, count: 0 }; }
             let cheap_frontier = (env.tip_frontier <= 50_000.0) as i32 as f64;
             let scale = (1.0 - env.slot_risk).powf(1.5) * (1.0 + 0.6 * cheap_frontier);
             let max_cost = (env.mu_per_cu * 500.0).max(50_000.0);
             let n = (max_cost / env.decoy_unit_cost * scale).floor() as i64;
             DecoyDecision { allow: n > 0, count: n.clamp(0, 4) as u32 }
         }
     }
 
     pub mod cu_allocator {
         use serde::{Deserialize, Serialize};
 
         #[derive(Clone, Copy, Debug, Serialize, Deserialize)]
         pub struct Candidate { pub id: u64, pub cu: u32, pub mu_per_cu: f64 }
         #[derive(Clone, Debug)]
         pub struct Allocation { pub id: u64, pub send: bool }
 
         fn project_simplex(v: &mut [f64], z: f64) {
             let mut u = v.to_vec();
             u.sort_by(|a, b| b.partial_cmp(a).unwrap());
             let mut css = 0.0;
             let mut rho = 0usize;
             for (i, ui) in u.iter().enumerate() {
                 css += ui;
                 let t = (css - z) / ((i + 1) as f64);
                 if ui - t > 0.0 { rho = i; }
             }
             let theta = (u[..=rho].iter().sum::<f64>() - z) / ((rho + 1) as f64);
             for vi in v.iter_mut() { *vi = (*vi - theta).max(0.0); }
         }
 
         pub fn allocate(budget_cu: u32, cands: Vec<Candidate>) -> Vec<Allocation> {
             let n = cands.len();
             if n == 0 { return vec![]; }
             let total_cu: f64 = cands.iter().map(|c| c.cu as f64).sum();
             let weights: Vec<f64> = cands.iter().map(|c| c.mu_per_cu.max(0.0)).collect();
             let wsum: f64 = weights.iter().sum::<f64>().max(1e-9);
             let mut x: Vec<f64> = weights.iter().map(|w| w / wsum).collect();
             let z = (budget_cu as f64) / total_cu;
             project_simplex(&mut x, z.clamp(0.0, 1.0));
             let mut idx: Vec<usize> = (0..n).collect();
             idx.sort_by(|&i, &j| x[j].partial_cmp(&x[i]).unwrap());
             let mut used: u32 = 0;
             let mut out = Vec::with_capacity(n);
             for i in idx {
                 let c = cands[i];
                 let take = used + c.cu <= budget_cu;
                 if take { used += c.cu; }
                 out.push(Allocation { id: c.id, send: take });
                 if used >= budget_cu { break; }
             }
 
    pub mod should_emit_tail {
        pub struct TailEnv { pub slot_risk: f64, pub ev_lamports: f64, pub inclusion_prob: f64 }
        pub fn should_emit_tail(e: TailEnv) -> bool {
            if e.slot_risk >= 0.55 { return false; }
            if e.inclusion_prob >= 0.92 { return false; }
            e.ev_lamports > 0.0
        }
    }
    pub mod inclusion_fit {
        #[derive(Clone, Copy, Debug)]
        pub struct FitState { pub k: f64, pub t0: f64 }
        #[derive(Clone, Copy, Debug)]
        pub struct Obs { pub tip: f64, pub included: bool }
        fn sigmoid(z: f64) -> f64 { 1.0 / (1.0 + (-z).exp()) }
        pub fn update_fit(mut fs: FitState, obs: Obs, lr: f64) -> FitState {
            let y = if obs.included { 1.0 } else { 0.0 };
            let z = fs.k * (obs.tip - fs.t0);
            let p = sigmoid(z).clamp(1e-6, 1.0 - 1e-6);
            let grad_k = (y - p) * (obs.tip - fs.t0);
            let grad_t0 = -(y - p) * fs.k;
            fs.k = (fs.k + lr * grad_k).clamp(1e-6, 5.0);
            fs.t0 = (fs.t0 + lr * grad_t0).max(1.0);
            fs
        }
    }
    pub mod frontier_tracker {
        #[derive(Clone, Debug)]
        pub struct FrontierTracker { ema: f64, alpha: f64, window: std::collections::VecDeque<i64>, cap: usize }
        impl FrontierTracker {
            pub fn new(alpha: f64, cap: usize) -> Self { Self { ema: 0.0, alpha: alpha.clamp(0.01, 0.5), window: std::collections::VecDeque::new(), cap } }
            pub fn update(&mut self, tip_included: i64) { let x = tip_included.max(0) as f64; self.ema = if self.ema == 0.0 { x } else { self.alpha * x + (1.0 - self.alpha) * self.ema }; if self.window.len() == self.cap { self.window.pop_front(); } self.window.push_back(tip_included.max(0)); }
            fn quantile(&self, q: f64) -> i64 { if self.window.is_empty() { return self.ema.round() as i64; } let mut v: Vec<i64> = self.window.iter().copied().collect(); v.sort_unstable(); let idx = ((v.len() as f64 - 1.0) * q).round().clamp(0.0, (v.len() - 1) as f64) as usize; v[idx] }
            pub fn p50(&self) -> i64 { self.quantile(0.50) }
            pub fn p90(&self) -> i64 { self.quantile(0.90) }
            pub fn frontier(&self) -> i64 { let base = self.ema.round() as i64; let p50 = self.p50(); let p90 = self.p90(); base.max(p50).min(p90) }
        }
    }
}
         pub fn update_fit(mut fs: FitState, obs: Obs, lr: f64) -> FitState {
             let y = if obs.included { 1.0 } else { 0.0 };
             let z = fs.k * (obs.tip - fs.t0);
             let p = sigmoid(z).clamp(1e-6, 1.0 - 1e-6);
             let grad_k = (y - p) * (obs.tip - fs.t0);
             let grad_t0 = -(y - p) * fs.k;
             fs.k = (fs.k + lr * grad_k).clamp(1e-6, 5.0);
             fs.t0 = (fs.t0 + lr * grad_t0).max(1.0);
             fs
         }
     }
     pub mod frontier_tracker {
         #[derive(Clone, Debug)]
         pub struct FrontierTracker { ema: f64, alpha: f64, window: std::collections::VecDeque<i64>, cap: usize }
         impl FrontierTracker {
             pub fn new(alpha: f64, cap: usize) -> Self { Self { ema: 0.0, alpha: alpha.clamp(0.01, 0.5), window: std::collections::VecDeque::new(), cap } }
             pub fn update(&mut self, tip_included: i64) { let x = tip_included.max(0) as f64; self.ema = if self.ema == 0.0 { x } else { self.alpha * x + (1.0 - self.alpha) * self.ema }; if self.window.len() == self.cap { self.window.pop_front(); } self.window.push_back(tip_included.max(0)); }
             fn quantile(&self, q: f64) -> i64 { if self.window.is_empty() { return self.ema.round() as i64; } let mut v: Vec<i64> = self.window.iter().copied().collect(); v.sort_unstable(); let idx = ((v.len() as f64 - 1.0) * q).round().clamp(0.0, (v.len() - 1) as f64) as usize; v[idx] }
             pub fn p50(&self) -> i64 { self.quantile(0.50) }
             pub fn p90(&self) -> i64 { self.quantile(0.90) }
             pub fn frontier(&self) -> i64 { let base = self.ema.round() as i64; let p50 = self.p50(); let p90 = self.p90(); base.max(p50).min(p90) }
         }
     }
 }
 
 pub mod network { 
     pub mod relay_selector {
         use rand::distributions::{Distribution, Beta};
         use rand::rngs::StdRng;
         use rand::SeedableRng;
         #[derive(Clone, Copy, Debug)]
         pub struct RelayStats { pub ok: u64, pub fail: u64, pub avg_tip_lamports: f64, pub ghost_rate: f64, pub cu_price_micro: f64 }
         #[derive(Clone, Copy, Debug)]
         pub struct RelayChoice { pub index: usize, pub score: f64 }
         pub fn pick_best_relay(stats: &[RelayStats], seed: u64) -> Option<RelayChoice> {
             if stats.is_empty() { return None; }
             let mut rng = StdRng::seed_from_u64(seed);
             let mut best = RelayChoice { index: 0, score: f64::MIN };
             for (i, s) in stats.iter().enumerate() {
                 let alpha = (s.ok as f64 + 1.0).max(1.0);
                 let beta = (s.fail as f64 + 1.0).max(1.0);
                 let x = Beta::new(alpha, beta).unwrap().sample(&mut rng);
                 let eff = (s.cu_price_micro.max(1.0).ln()).recip();
                 let tip_pen = 1.0 / (1.0 + s.avg_tip_lamports.max(1.0).ln());
                 let ghost_pen = (1.0 - s.ghost_rate.clamp(0.0, 1.0)).powf(2.0);
                 let score = x * tip_pen * ghost_pen * eff;
                 if score > best.score { best = RelayChoice { index: i, score }; }
             }
             Some(best)
         }
     }
 }
 
 pub mod analysis { 
     pub mod account_disjoint {
         use std::collections::HashSet;
         pub fn disjointness<A: Eq + std::hash::Hash + Clone>(a: &HashSet<A>, b: &HashSet<A>) -> f64 {
             if a.is_empty() && b.is_empty() { return 1.0; }
             let inter = a.intersection(b).count() as f64;
             let uni = (a.len() + b.len()) as f64 - inter;
             if uni <= 0.0 { 0.0 } else { 1.0 - inter / uni }
         }
     }
 }
 
 pub mod scheduling { 
     pub mod slot_scheduler {
         use rand::rngs::StdRng;
         use rand::{Rng, SeedableRng};
         #[derive(Clone, Copy, Debug)]
         pub struct PhaseCfg { pub phase_ms: u64, pub jitter_ms: u64, pub slot_ms: u64 }
         pub struct PhasePlan { pub send_at_ms: u64 }
         pub fn plan_phase(cfg: PhaseCfg, slot_index: u64, seed: u64) -> PhasePlan {
             let mut rng = StdRng::seed_from_u64(slot_index ^ seed);
             let j = if cfg.jitter_ms == 0 { 0 } else { rng.gen_range(0..=cfg.jitter_ms) };
             let mut when = cfg.phase_ms.saturating_add(j);
             if when + 2 >= cfg.slot_ms { when = cfg.slot_ms.saturating_sub(2); }
             PhasePlan { send_at_ms: when }
         }
     }
 }
 
 pub mod execution2 { 
     pub mod retry_backoff {
         #[derive(Clone, Copy, Debug)]
         pub struct RetryEnv { pub slot_risk: f64, pub max_retries: u8 }
         #[derive(Clone, Copy, Debug)]
         pub struct RetryPlan { pub attempts: u8, pub rotate_relay: bool }
         pub fn plan_retry(e: RetryEnv, prev_fails: u8) -> RetryPlan {
             if e.slot_risk >= 0.70 { return RetryPlan { attempts: 0, rotate_relay: true }; }
             let left = e.max_retries.saturating_sub(prev_fails);
             let rotate = prev_fails >= 2;
             RetryPlan { attempts: left, rotate_relay: rotate }
         }
     }
     pub mod tip_ladder {
         pub fn ladder(tip_base: i64, width: usize) -> Vec<i64> {
             let w = width.max(1).min(8);
             let deltas = match w {
                 1 => vec![0.0],
                 2 => vec![-0.005, 0.0],
                 3 => vec![-0.007, 0.0, 0.007],
                 4 => vec![-0.010, -0.003, 0.003, 0.010],
                 5 => vec![-0.012, -0.005, 0.0, 0.005, 0.012],
                 _ => (-3..=3).map(|i| i as f64 * 0.004).collect(),
             };
             deltas.into_iter().map(|d| ((tip_base as f64) * (1.0 + d)).round() as i64).collect()
         }
     }
 }
 
 pub mod risk { 
     pub mod spend_guard {
         #[derive(Clone, Copy, Debug)]
         pub struct SpendEnv { pub pnl_7d_lamports: i64, pub ev_lamports: f64, pub tips_cost_lamports: i64 }
         pub fn allow_spend(e: SpendEnv) -> bool {
             if e.ev_lamports <= 0.0 { return false; }
             let max_burn = (e.pnl_7d_lamports.max(0) as f64 * 0.15).round() as i64;
             e.tips_cost_lamports <= max_burn
         }
     }
 }

 // ======================[UPGRADE-4 modules inline]====================================
 // Execution: joint compute budget tuner (μ/CU) + tip; blockhash refresher; bribe splitter
 pub mod execution4 {
     pub mod compute_budget_tuner {
         use serde::{Deserialize, Serialize};
         
         #[derive(Clone, Copy, Debug, Serialize, Deserialize)]
         pub struct Sensitivity { pub a_mu: f64, pub b_tip: f64, pub c_bias: f64 }
         
         #[derive(Clone, Copy, Debug, Serialize, Deserialize)]
         pub struct JointEnv {
             pub gross_lamports: f64,
             pub cu: u32,
             pub spend_cap: f64,
             pub min_mu: f64,
             pub max_mu: f64,
             pub min_tip: f64,
             pub max_tip: f64,
             pub sens: Sensitivity,
         }
         
         #[derive(Clone, Copy, Debug, Serialize, Deserialize)]
         pub struct JointDecision { pub mu_per_cu: u64, pub tip_lamports: i64, pub inclusion_prob: f64, pub ev_lamports: f64 }
         
         fn sigmoid(x: f64) -> f64 { 1.0 / (1.0 + (-x).exp()) }
         
         pub fn optimize_joint(env: JointEnv) -> JointDecision {
             let cu = env.cu as f64;
             let s_min = (0.6 * env.spend_cap).max(env.min_tip);
             let s_max = env.spend_cap.min(env.max_tip + env.max_mu * cu / 1e6);
             let mut best = JointDecision { mu_per_cu: env.min_mu.round() as u64, tip_lamports: env.min_tip.round() as i64, inclusion_prob: 0.0, ev_lamports: f64::MIN };
             let steps_s = 10usize;
             for i in 0..steps_s {
                 let s = s_min + (s_max - s_min) * (i as f64) / (steps_s - 1) as f64;
                 let mut lo = 0.10f64; let mut hi = 0.90f64;
                 for _ in 0..16 {
                     let g1 = hi - (hi - lo) * 0.6180339887;
                     let g2 = lo + (hi - lo) * 0.6180339887;
                     let ev1 = eval_split(g1, s, cu, env);
                     let ev2 = eval_split(g2, s, cu, env);
                     if ev1.3 > ev2.3 { hi = g2; } else { lo = g1; }
                 }
                 let f = (lo + hi) * 0.5;
                 let d = eval_split(f, s, cu, env);
                 if d.3 > best.ev_lamports { best = JointDecision { mu_per_cu: d.0, tip_lamports: d.1, inclusion_prob: d.2, ev_lamports: d.3 }; }
             }
             best
         }
         fn eval_split(f_mu: f64, spend: f64, cu: f64, env: JointEnv) -> (u64, i64, f64, f64) {
             let spend_mu = (f_mu * spend).max(0.0);
             let spend_tip = (spend - spend_mu).max(0.0);
             let mu = ((spend_mu * 1e6) / cu).clamp(env.min_mu, env.max_mu);
             let tip = spend_tip.clamp(env.min_tip, env.max_tip);
             let pincl = sigmoid(env.sens.a_mu * mu.ln() + env.sens.b_tip * (tip + 1.0).ln() - env.sens.c_bias).clamp(1e-6, 1.0 - 1e-6);
             let ev = pincl * (env.gross_lamports - tip - mu * cu / 1e6).max(0.0);
             (mu.round() as u64, tip.round() as i64, pincl, ev)
         }
     }
 
     pub mod blockhash_refresher {
         #[derive(Clone, Copy, Debug)]
         pub struct FreshCfg { pub ttl_slots: u64, pub max_age_frac: f64, pub tight_frac: f64 }
         pub fn should_refresh_blockhash(current_slot: u64, obtained_slot: u64, slot_risk: f64, cfg: FreshCfg) -> bool {
             let age = current_slot.saturating_sub(obtained_slot);
             let cap = if slot_risk >= 0.65 { cfg.tight_frac } else { cfg.max_age_frac };
             let max_age = (cfg.ttl_slots as f64 * cap).round() as u64;
             age >= max_age
         }
     }
 
     pub mod bribe_splitter {
         #[derive(Clone, Copy, Debug)]
         pub struct SplitEnv { pub total_spend_lamports: i64, pub cu_estimate: u32, pub mu_min: u64, pub mu_max: u64, pub tip_min: i64 }
         #[derive(Clone, Copy, Debug)]
         pub struct SplitDecision { pub mu_per_cu: u64, pub tip_lamports: i64 }
         pub fn split_bribe(beta: f64, e: SplitEnv) -> SplitDecision {
             let b = beta.clamp(0.0, 1.0);
             let s = e.total_spend_lamports.max(e.tip_min);
             let s_mu = (b * s as f64).max(0.0);
             let s_tip = s as f64 - s_mu;
             let mut mu = ((s_mu * 1e6) / (e.cu_estimate as f64)).round() as i64;
             mu = mu.clamp(e.mu_min as i64, e.mu_max as i64);
             let tip = (s_tip.round() as i64).max(e.tip_min).max(0);
             SplitDecision { mu_per_cu: mu as u64, tip_lamports: tip }
         }
     }
 }
 
 // Analysis: lockset predictor (Bloom)
 pub mod analysis4 {
     pub mod lockset_predictor {
         use std::collections::hash_map::DefaultHasher;
         use std::hash::{Hash, Hasher};
         
         #[derive(Clone, Debug)]
         pub struct Bloom { m: usize, k: usize, bits: Vec<u64> }
         impl Bloom {
             pub fn new(m_bits: usize, k: usize) -> Self { let words = (m_bits + 63) / 64; Self { m: m_bits, k, bits: vec![0u64; words] } }
             fn idx(&self, h: u64) -> (usize, u64) { let i = (h as usize) % self.m; (i >> 6, 1u64 << (i & 63)) }
             pub fn insert(&mut self, key: &[u8]) { let mut h = hash64(key); for _ in 0..self.k { let (w, mask) = self.idx(h); self.bits[w] |= mask; h = mix64(h); } }
             pub fn might_contain(&self, key: &[u8]) -> bool { let mut h = hash64(key); for _ in 0..self.k { let (w, mask) = self.idx(h); if (self.bits[w] & mask) == 0 { return false; } h = mix64(h); } true }
         }
         fn hash64(data: &[u8]) -> u64 { let mut hasher = DefaultHasher::new(); data.hash(&mut hasher); hasher.finish() }
         #[inline] fn mix64(x: u64) -> u64 { x ^ (x.rotate_left(13)).wrapping_mul(0x9E3779B97F4A7C15) }
         
         #[derive(Clone, Debug)]
         pub struct LocksetPredictor { bloom: Bloom, inserts: usize, max_inserts: usize }
         impl LocksetPredictor {
             pub fn new(m_bits: usize, k: usize, max_inserts: usize) -> Self { Self { bloom: Bloom::new(m_bits, k), inserts: 0, max_inserts } }
             pub fn observe_write_key(&mut self, key32: &[u8;32]) { self.bloom.insert(key32); self.inserts += 1; if self.inserts > self.max_inserts { *self = Self::new(self.bloom.m, self.bloom.k, self.max_inserts); } }
             pub fn collision_prob(&self, keys: &[[u8;32]]) -> f64 { if keys.is_empty() { return 0.0; } let mut hits = 0usize; for k in keys { if self.bloom.might_contain(k) { hits += 1; } } (hits as f64) / (keys.len() as f64) }
         }
     }
 }
 
 // Security: fee-payer/key pool rotation
 pub mod security {
     pub mod keypool {
         use rand::{Rng, SeedableRng};
         use rand::rngs::StdRng;
         use std::collections::VecDeque;
         
         #[derive(Clone, Debug)]
         pub struct KeyHealth { pub pubkey: [u8;32], pub score: f64, pub cool_until_slot: u64 }
         
         #[derive(Clone, Debug)]
         pub struct KeyPool { deck: VecDeque<KeyHealth>, rng: StdRng }
         
         impl KeyPool {
             pub fn new(mut keys: Vec<[u8;32]>, seed: u64) -> Self {
                 let deck = keys.drain(..).map(|k| KeyHealth{ pubkey:k, score:0.5, cool_until_slot:0 }).collect();
                 Self { deck: VecDeque::from(deck), rng: StdRng::seed_from_u64(seed) }
             }
             pub fn next_fee_payer(&mut self, current_slot: u64) -> [u8;32] {
                 let mut best: Option<(usize, f64)> = None;
                 for (i, kh) in self.deck.iter().enumerate() { if kh.cool_until_slot > current_slot { continue; } let jitter = self.rng.gen::<f64>() * 1e-6; let s = kh.score + jitter; if best.map_or(true, |(_,bs)| s > bs) { best = Some((i, s)); } }
                 let idx = best.map(|x| x.0).unwrap_or(0);
                 let kh = self.deck[idx].clone();
                 self.deck.rotate_left(idx);
                 self.deck.back().unwrap().pubkey
             }
             pub fn report(&mut self, used_pubkey: [u8;32], included: bool, current_slot: u64, cooldown: u64) {
                 for kh in self.deck.iter_mut() {
                     if kh.pubkey == used_pubkey { let delta = if included { 0.05 } else { -0.08 }; kh.score = (kh.score + delta).clamp(0.0, 1.0); if !included { kh.cool_until_slot = current_slot.saturating_add(cooldown); } break; }
                 }
             }
         }
     }
 }
 
 // Scheduling: send pacer (token bucket)
 pub mod scheduling4 {
     pub mod send_pacer {
         #[derive(Clone, Copy, Debug)]
         pub struct PacerCfg { pub capacity: u32, pub refill_per_ms: f64 }
         #[derive(Clone, Copy, Debug)]
         pub struct PacerState { pub tokens: f64, pub last_ms: u64 }
         impl PacerState {
             pub fn new(capacity: u32, now_ms: u64) -> Self { Self { tokens: capacity as f64, last_ms: now_ms } }
             pub fn allow(&mut self, cfg: PacerCfg, now_ms: u64, cost: f64) -> bool {
                 let dt = now_ms.saturating_sub(self.last_ms) as f64; self.last_ms = now_ms;
                 self.tokens = (self.tokens + cfg.refill_per_ms * dt).min(cfg.capacity as f64);
                 if self.tokens >= cost { self.tokens -= cost; true } else { false }
             }
         }
     }
 }
 
 // Risk: CUSUM drawdown detector
 pub mod risk4 {
     pub mod cusum_drawdown {
         #[derive(Clone, Copy, Debug)]
         pub struct CusumCfg { pub k: f64, pub h: f64, pub decay: f64 }
         #[derive(Clone, Copy, Debug)]
         pub struct CusumState { pub pos: f64, pub neg: f64 }
         impl CusumState { pub fn new() -> Self { Self { pos: 0.0, neg: 0.0 } }
             pub fn update(&mut self, x: f64, cfg: CusumCfg) -> bool {
                 self.pos = (cfg.decay*self.pos + (x - cfg.k)).max(0.0);
                 self.neg = (cfg.decay*self.neg - (x + cfg.k)).max(0.0);
                 self.pos > cfg.h || self.neg > cfg.h
             }
         }
     }
 }
 
 // ======================[UPGRADE-6 modules inline]====================================
 // Scheduling: Contextual Thompson bandit over slot phases
 pub mod scheduling6 {
     pub mod phase_bandit {
         use rand::distributions::{Beta, Distribution};
         use rand::rngs::StdRng;
         use rand::SeedableRng;
 
         #[derive(Clone, Copy, Debug)]
         pub struct PhaseCtx { pub relay_idx: u16, pub leader_entropy_band: u8, pub block_fill_band: u8 }
         #[derive(Clone, Copy, Debug)]
         pub struct Bucket { pub start_ms: u16, pub end_ms: u16 }
         #[derive(Clone, Debug)]
         pub struct Arm { pub ctx: PhaseCtx, pub bucket: Bucket, pub alpha: f64, pub beta: f64 }
         impl Arm { pub fn new(ctx: PhaseCtx, bucket: Bucket) -> Self { Self { ctx, bucket, alpha: 1.0, beta: 1.0 } }
             pub fn sample(&self, rng: &mut StdRng) -> f64 { Beta::new(self.alpha, self.beta).unwrap().sample(rng) }
             pub fn update(&mut self, success: bool) { if success { self.alpha += 1.0; } else { self.beta += 1.0; } } }
         pub struct PhaseBandit { pub arms: Vec<Arm> }
         impl PhaseBandit {
             pub fn pick(&self, seed: u64) -> Option<&Arm> {
                 let mut rng = StdRng::seed_from_u64(seed);
                 self.arms.iter().max_by(|a,b| a.sample(&mut rng).partial_cmp(&b.sample(&mut rng)).unwrap())
             }
             pub fn pick_mut(&mut self, seed: u64) -> Option<&mut Arm> {
                 let mut rng = StdRng::seed_from_u64(seed);
                 let idx = (0..self.arms.len()).max_by(|&i,&j| {
                     let mut ri = rng.clone(); let mut rj = rng.clone();
                     let si = self.arms[i].sample(&mut ri); let sj = self.arms[j].sample(&mut rj);
                     si.partial_cmp(&sj).unwrap()
                 })?;
                 self.arms.get_mut(idx)
             }
         }
     }
 }
 
 // Network: Per-relay congestion window (CUBIC-lite)
 pub mod network6 {
     pub mod relay_cwnd {
         #[derive(Clone, Copy, Debug)]
         pub struct CwndCfg { pub cwnd_min: u32, pub cwnd_max: u32, pub rto_ms_min: u64, pub rto_ms_max: u64 }
         #[derive(Clone, Copy, Debug)]
         pub struct CwndState { pub cwnd: u32, pub inflight: u32, pub rto_ms: u64 }
         impl CwndState {
             pub fn new(cfg: CwndCfg) -> Self { Self { cwnd: cfg.cwnd_min.max(1), inflight: 0, rto_ms: cfg.rto_ms_min } }
             pub fn can_send(&self) -> bool { self.inflight < self.cwnd }
             pub fn on_send(&mut self) { self.inflight += 1; }
             pub fn on_ack(&mut self, cfg: CwndCfg, sample_rtt_ms: u64) {
                 self.inflight = self.inflight.saturating_sub(1);
                 self.cwnd = (self.cwnd + 1).min(cfg.cwnd_max);
                 self.rto_ms = self.rto_ms.saturating_sub(self.rto_ms/8).saturating_add(sample_rtt_ms/4).clamp(cfg.rto_ms_min, cfg.rto_ms_max);
             }
             pub fn on_timeout(&mut self, cfg: CwndCfg) {
                 self.cwnd = (self.cwnd / 2).max(cfg.cwnd_min);
                 self.inflight = self.inflight.saturating_sub(1);
                 self.rto_ms = (self.rto_ms * 5 / 4).clamp(cfg.rto_ms_min, cfg.rto_ms_max);
             }
         }
     }
 }
 
 // Risk: Kelly budget multiplier
 pub mod risk6 {
     pub mod kelly_budget {
         #[derive(Clone, Copy, Debug)]
         pub struct KellyEnv { pub inclusion_p: f64, pub gross_edge: f64, pub cost: f64, pub cap: f64 }
         pub fn kelly_multiplier(e: KellyEnv) -> f64 {
             let p = e.inclusion_p.clamp(1e-6, 1.0-1e-6);
             let b = (e.gross_edge / e.cost).max(1e-9);
             let f = (p - (1.0 - p) / b).max(0.0);
             f.min(e.cap).max(0.0)
         }
     }
 }
 
 // Analytics: P2 quantile estimator (p50/p90)
 pub mod analytics6 {
     pub mod ewmh_quantile {
         #[derive(Clone, Copy, Debug)]
         pub struct P2 { q: [f64;5], n: [f64;5], np: [f64;5], dn: [f64;5], init: usize }
         impl P2 {
             pub fn new(p: f64) -> Self { let dn = [0.0, p/2.0, p, (1.0+p)/2.0, 1.0]; Self { q:[0.0;5], n:[0.0,1.0,2.0,3.0,4.0], np:[0.0;5], dn, init:0 } }
             pub fn insert(&mut self, x: f64) {
                 if self.init < 5 { self.q[self.init]=x; self.init+=1; if self.init==5 { self.q.sort_by(|a,b| a.partial_cmp(b).unwrap()); self.n=[0.0,1.0,2.0,3.0,4.0]; self.np=self.n; } return; }
                 let mut k = 0usize; if x < self.q[0] { self.q[0]=x; k=0; }
                 else if x >= self.q[4] { self.q[4]=x; k=3; } else { for i in 0..4 { if x >= self.q[i] && x < self.q[i+1] { k=i; break; } } }
                 for i in k+1..5 { self.n[i]+=1.0; self.np[i]+=self.dn[i]; }
                 for i in 0..=k { self.np[i]+=self.dn[i]; }
                 for i in 1..4 {
                     let d = self.np[i]-self.n[i];
                     if (d>=1.0 && self.n[i+1]-self.n[i]>1.0) || (d<=-1.0 && self.n[i]-self.n[i-1]>1.0) {
                         let s = d.signum();
                         let qi = self.q[i] + s*( ((self.n[i]-self.n[i-1]+s)*(self.q[i+1]-self.q[i])/(self.n[i+1]-self.n[i]) + (self.n[i+1]-self.n[i]-s)*(self.q[i]-self.q[i-1])/(self.n[i]-self.n[i-1])) / (self.n[i+1]-self.n[i-1]) );
                         if qi > self.q[i-1] && qi < self.q[i+1] { self.q[i]=qi; } else { self.q[i] = self.q[i] + s*(self.q[(i as isize + s as isize) as usize] - self.q[i]) / (self.n[(i as isize + s as isize) as usize]-self.n[i]); }
                         self.n[i]+=s;
                     }
                 }
             }
             pub fn quantile(&self) -> f64 { if self.init < 5 { self.q[0] } else { self.q[2] } }
         }
     }
 }
 
 // Execution: atomic tip slicer and instruction ordering
 pub mod execution6 {
     pub mod atomic_tip_slicer {
         #[derive(Clone, Copy, Debug)]
         pub struct Curve { pub k: f64, pub t0: f64 }
         #[derive(Clone, Copy, Debug)]
         pub struct SliceEnv { pub curve: Curve, pub width: usize, pub base_tip: f64, pub cu_fee: f64, pub gross: f64, pub cap_per: f64 }
         fn sigmoid(x: f64) -> f64 { 1.0/(1.0+(-x).exp()) }
         fn marginal_ev(k: f64, t0: f64, tip: f64, gross: f64, cu_fee: f64) -> f64 { let p = sigmoid(k*(tip - t0)); let dp = k*p*(1.0-p); dp*(gross - tip - cu_fee) - p }
         pub fn slice_tips(e: SliceEnv) -> Vec<i64> {
             let w = e.width.max(1).min(8);
             let mut tips: Vec<f64> = match w { 1 => vec![e.base_tip], 2 => vec![e.base_tip*0.995, e.base_tip], 3 => vec![e.base_tip*0.993, e.base_tip, e.base_tip*1.007], 4 => vec![e.base_tip*0.990, e.base_tip*0.997, e.base_tip*1.003, e.base_tip*1.010], _ => (-(w as i32)/2..=(w as i32)/2).take(w).map(|i| e.base_tip*(1.0 + i as f64*0.004)).collect(), };
             for _ in 0..8 {
                 let mut argmax=0usize; let mut argmin=0usize; let mut best=f64::MIN; let mut worst=f64::MAX;
                 for (i,&t) in tips.iter().enumerate() { let m = marginal_ev(e.curve.k, e.curve.t0, t, e.gross, e.cu_fee); if m>best { best=m; argmax=i; } if m<worst { worst=m; argmin=i; } }
                 if best <= 0.0 { break; }
                 let q = (tips[argmin]*0.003).max(10.0);
                 let new_max = (tips[argmax]+q).min(e.cap_per);
                 let new_min = (tips[argmin]-q).max(0.0);
                 if new_max == tips[argmax] || new_min == tips[argmin] { break; }
                 tips[argmax]=new_max; tips[argmin]=new_min;
             }
             tips.into_iter().map(|t| t.round() as i64).collect()
         }
     }
     pub mod ix_order {
         #[derive(Clone, Copy, Debug, PartialEq, Eq)]
         pub enum IxKind { ComputeBudget, PriorityFee, Memo, Program, Other }
         #[derive(Clone, Debug)]
         pub struct Ix<'a> { pub kind: IxKind, pub raw: &'a [u8] }
         pub fn order_instructions<'a>(ixs: &mut [Ix<'a>]) {
             ixs.sort_by_key(|ix| match ix.kind { IxKind::ComputeBudget => 0, IxKind::PriorityFee => 1, IxKind::Memo => 2, IxKind::Program => 3, IxKind::Other => 4 });
         }
     }
 }
 
 // Analysis: account set LRU cache
 pub mod analysis6 {
     pub mod account_set_cache {
         use lru::LruCache; use std::num::NonZeroUsize;
         pub struct AccCache { lru: LruCache<u64, f64> }
         fn hash_accounts(acc: &[[u8;32]]) -> u64 { use std::hash::{Hash, Hasher}; use std::collections::hash_map::DefaultHasher; let mut v = acc.to_vec(); v.sort_unstable(); let mut h = DefaultHasher::new(); for a in v { a.hash(&mut h); } h.finish() }
         impl AccCache { pub fn new(cap: usize) -> Self { Self { lru: LruCache::new(NonZeroUsize::new(cap.max(1)).unwrap()) } } pub fn get_or<F: FnOnce()->f64>(&mut self, acc: &[[u8;32]], compute: F) -> f64 { let key = hash_accounts(acc); if let Some(v) = self.lru.get(&key) { *v } else { let v = compute(); self.lru.put(key, v); v } } }
     }
 }
 
 // Security: μ/CU ceiling governor
 pub mod security6 {
     pub mod priority_fee_ceiling {
         #[derive(Clone, Copy, Debug)]
         pub struct MuCeilCfg { pub ema_alpha: f64, pub headroom_mult: f64, pub hard_cap: u64 }
         #[derive(Clone, Copy, Debug)]
         pub struct MuCeilState { pub ema_mu: f64 }
         impl MuCeilState { pub fn new() -> Self { Self { ema_mu: 0.0 } } pub fn update(&mut self, cfg: MuCeilCfg, observed_mu: f64) { let a = cfg.ema_alpha.clamp(0.01, 0.5); self.ema_mu = if self.ema_mu==0.0 { observed_mu } else { a*observed_mu + (1.0-a)*self.ema_mu }; } pub fn cap(&self, cfg: MuCeilCfg) -> u64 { let soft = (self.ema_mu * cfg.headroom_mult).round() as u64; soft.min(cfg.hard_cap.max(1)) } }
     }
 }
 
 // ======================[UPGRADE-7 modules inline]====================================
 // Stat: Online moments (Welford + optional EWMA)
 pub mod stat7 {
     pub mod online_moments {
         #[derive(Clone, Copy, Debug)]
         pub struct MomentsCfg { pub alpha: f64 }
         #[derive(Clone, Copy, Debug)]
         pub struct Moments { pub n: u64, pub mean: f64, pub m2: f64, pub alpha: f64 }
         impl Moments {
             pub fn new(cfg: MomentsCfg) -> Self { Self { n:0, mean:0.0, m2:0.0, alpha:cfg.alpha.clamp(0.0,1.0) } }
             pub fn push(&mut self, x: f64) {
                 self.n += 1;
                 if self.alpha == 0.0 {
                     let delta = x - self.mean;
                     self.mean += delta / (self.n as f64);
                     let delta2 = x - self.mean;
                     self.m2 += delta * delta2;
                 } else {
                     let prev = self.mean;
                     self.mean = self.alpha * x + (1.0 - self.alpha) * self.mean;
                     let e = x - prev;
                     self.m2 = self.alpha * e*e + (1.0 - self.alpha) * self.m2;
                 }
             }
             pub fn var(&self) -> f64 { if self.alpha == 0.0 { if self.n < 2 { 0.0 } else { self.m2 / ((self.n - 1) as f64) } } else { self.m2.max(0.0) } }
             pub fn std(&self) -> f64 { self.var().sqrt() }
         }
     }
 }
 
 // Risk: Variance-aware allocator (Neyman)
 pub mod risk7 {
     pub mod variance_aware_allocator {
         use serde::{Deserialize, Serialize};
         #[derive(Clone, Copy, Debug, Serialize, Deserialize)]
         pub struct RouteStat { pub id: u64, pub cu: u32, pub mu_per_cu: f64, pub std_mu: f64 }
         #[derive(Clone, Debug)]
         pub struct Pick { pub id: u64, pub send: bool }
         pub fn allocate_variance_aware(budget_cu: u32, mut stats: Vec<RouteStat>) -> Vec<Pick> {
             if budget_cu == 0 || stats.is_empty() { return vec![]; }
             for s in stats.iter_mut() { s.std_mu = s.std_mu.max(1e-6); }
             stats.sort_by(|a,b| (b.mu_per_cu / b.std_mu).partial_cmp(&(a.mu_per_cu / a.std_mu)).unwrap().then_with(|| a.cu.cmp(&b.cu)));
             let mut used = 0u32; let mut out = Vec::with_capacity(stats.len());
             for s in stats { if used + s.cu > budget_cu { continue; } out.push(Pick { id: s.id, send: true }); used += s.cu; if used >= budget_cu { break; } }
             out
         }
     }
 }
 
 // Network: Jitter guard based on RTT variance
 pub mod network7 {
     pub mod jitter_guard {
         #[derive(Clone, Copy, Debug)]
         pub struct JitterEnv { pub rtt_p50_ms: f64, pub rtt_p90_ms: f64, pub base_jitter_ms: u64, pub hard_cap_ms: u64 }
         pub fn shaped_jitter(env: JitterEnv) -> u64 {
             let spread = (env.rtt_p90_ms - env.rtt_p50_ms).max(0.0);
             let scale = (1.0 + (spread / env.rtt_p50_ms.max(1.0)).clamp(0.0, 1.5)).min(2.0);
             let j = (env.base_jitter_ms as f64 * scale).round() as u64;
             j.min(env.hard_cap_ms)
         }
     }
 }
 
 // Execution: Cross-relay economic de-dup
 pub mod execution7 {
     pub mod xrelay_dedup {
         use std::collections::HashSet;
         #[derive(Default)]
         pub struct XDedup { seen: HashSet<u64> }
         fn h64(v: &[u8]) -> u64 { use std::hash::{Hash, Hasher}; use std::collections::hash_map::DefaultHasher; let mut h = DefaultHasher::new(); v.hash(&mut h); h.finish() }
         pub fn econ_fp(accounts: &[[u8;32]], tip: i64, mu_per_cu: u64, cu: u32) -> u64 {
             let mut acc = accounts.to_vec(); acc.sort_unstable(); let mut buf = Vec::with_capacity(acc.len()*32 + 16);
             for a in acc { buf.extend_from_slice(&a); }
             let spend = tip as i128 + ((mu_per_cu as i128 * cu as i128) / 1_000_000);
             buf.extend_from_slice(&spend.to_le_bytes()); h64(&buf)
         }
         impl XDedup { pub fn new() -> Self { Self { seen: HashSet::new() } } pub fn clear_slot(&mut self) { self.seen.clear(); } pub fn try_mark(&mut self, fp: u64) -> bool { if self.seen.contains(&fp) { false } else { self.seen.insert(fp); true } } }
     }
 }
 
 // Analytics: Leader drift guard
 pub mod analytics7 {
     pub mod leader_drift {
         #[derive(Clone, Copy, Debug)]
         pub struct DriftCfg { pub stale_slots: u64, pub inflate_soft: f64, pub inflate_hard: f64 }
         pub fn inflate_t0_if_stale(now: u64, updated_slot: u64, base_t0: f64, cfg: DriftCfg) -> (f64, bool) {
             let age = now.saturating_sub(updated_slot);
             if age == 0 { return (base_t0.max(1.0), false); }
             let lifted = if age > cfg.stale_slots * 2 { cfg.inflate_hard } else { cfg.inflate_soft };
             (base_t0.max(1.0) * lifted, true)
         }
     }
 }
 
 // Execution: Priority fee floor (μ/CU)
 pub mod execution7_floor {
     pub mod priority_fee_floor {
         #[derive(Clone, Copy, Debug)]
         pub struct FloorCfg { pub ema_alpha: f64, pub hard_floor: u64 }
         #[derive(Clone, Copy, Debug)]
         pub struct FloorState { pub ema_min_mu: f64 }
         impl FloorState {
             pub fn new() -> Self { Self { ema_min_mu: 0.0 } }
             pub fn observe_inclusion(&mut self, cfg: FloorCfg, mu_per_cu: u64) {
                 let a = cfg.ema_alpha.clamp(0.05, 0.5);
                 if self.ema_min_mu == 0.0 { self.ema_min_mu = mu_per_cu as f64; }
                 else { self.ema_min_mu = a*(mu_per_cu as f64).min(self.ema_min_mu) + (1.0-a)*self.ema_min_mu; }
             }
             pub fn floor(&self, cfg: FloorCfg) -> u64 { let soft = self.ema_min_mu.round() as u64; soft.max(cfg.hard_floor) }
         }
     }
 }
 
 // Planning: CU shadow price (dual λ)
 pub mod planning7 {
     pub mod cu_shadow_price {
         #[derive(Clone, Copy, Debug)]
         pub struct ShadowCfg { pub ema_alpha: f64 }
         #[derive(Clone, Copy, Debug)]
         pub struct ShadowState { pub lambda: f64 }
         impl ShadowState {
             pub fn new() -> Self { Self { lambda: 0.0 } }
             pub fn update(&mut self, cfg: ShadowCfg, demand_cu: u32, used_cu: u32, realized_mu_per_cu: f64) {
                 let gap = (demand_cu as f64 - used_cu as f64).max(0.0);
                 let tight = (gap / (used_cu as f64 + 1.0)).clamp(0.0, 3.0);
                 let target = realized_mu_per_cu * tight;
                 let a = cfg.ema_alpha.clamp(0.05, 0.5);
                 self.lambda = a*target + (1.0-a)*self.lambda;
             }
             pub fn penalty_lamports(&self, cu: u32) -> f64 { self.lambda * (cu as f64) }
         }
     }
 }
 
 // Scheduling: Microburst controller
 pub mod scheduling7 {
     pub mod microburst_controller {
         #[derive(Clone, Copy, Debug)]
         pub struct MicroburstCfg { pub base: u32, pub max: u32 }
         pub fn burst_size(cwnd: u32, rtt_p90_ms: u64, cfg: MicroburstCfg) -> u32 {
             let r = (rtt_p90_ms as f64).max(1.0);
             let damp = (60.0 / r).clamp(0.5, 1.5);
             let b = ((cfg.base as f64) * damp).round() as u32;
             b.min(cwnd).clamp(1, cfg.max)
         }
     }
 }
 
 #[cfg(test)]
 mod tests {
     use super::*;
     
     #[test]
     fn test_noise_injection() {
         let mut cfg = NoiseConfig::default();
         cfg.noise_salt = 42;
         let mut injector = TradePatternNoiseInjector::new(cfg);
         let trades = injector.inject_noise(100_000_000, TradeDirection::Buy);
         assert!(!trades.is_empty());
         assert!(trades.iter().all(|t| t.volume > 0));
     }
     
     #[test]
     fn test_pattern_metrics() {
         let mut cfg = NoiseConfig::default();
         cfg.noise_salt = 7;
         let injector = TradePatternNoiseInjector::new(cfg);
         for i in 0..24 {
             injector.record_trade(TradePattern {
                 timestamp: i * 60,
                 volume: 50_000_000,
                 direction: TradeDirection::Buy,
                 price_impact: 0.001,
                 venue: "raydium".to_string(),
                 signature_hash: i as u64,
             });
         }
         let health = injector.get_pattern_health();
         assert!(health.overall_score < 0.9); // should detect pattern pressure
     }
     
     #[test]
     fn test_emergency_break() {
         let mut injector = TradePatternNoiseInjector::new(NoiseConfig::default());
         let e = injector.emergency_pattern_break();
         assert!(e.len() >= 5);
         assert!(e.iter().all(|t| t.is_decoy));
     }
 }
