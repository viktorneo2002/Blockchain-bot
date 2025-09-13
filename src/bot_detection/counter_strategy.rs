use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    transaction::Transaction,
    commitment_config::CommitmentConfig,
    clock::Slot,
    instruction::{Instruction, AccountMeta},
    message::Message,
    sysvar,
};
use std::collections::{HashMap, VecDeque, HashSet, BTreeMap, BTreeSet};
use std::sync::Arc;
use parking_lot::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::time::{timeout, sleep};
use serde::{Deserialize, Serialize};
use crate::types::codec::UnixMillis;
use thiserror::Error;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha20Rng;
use anyhow::{anyhow, Result as AnyhowResult};
use sha2::{Sha256, Digest};
use serde_json::Value;
use reqwest::Client as HttpClient;
use reqwest::ClientBuilder;
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use base64;
use base64::engine::general_purpose::STANDARD as B64STD;
use bincode;
use tokio::task::JoinSet;
use solana_client::rpc_response::RpcSimulateTransactionResult;
use solana_client::rpc_config::RpcSendTransactionConfig;
use solana_sdk::hash::Hash;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::signature::{Keypair, Signer};
use solana_sdk::address_lookup_table_account::AddressLookupTableAccount;
use solana_sdk::message::{v0::Message as V0Message, VersionedMessage};
use solana_sdk::transaction::{VersionedTransaction};
use solana_client::rpc_config::{RpcBlockConfig, TransactionDetails, RpcSimulateTransactionConfig};
use solana_transaction_status::{UiConfirmedBlock, EncodedTransaction, UiTransactionEncoding};

#[derive(Error, Debug)]
pub enum CounterStrategyError {
    #[error("Lock timeout: {0}")]
    LockTimeout(String),
    #[error("Pattern detection failed: {0}")]
    PatternDetectionError(String),
    #[error("Resource limit exceeded: {0}")]
    ResourceLimitExceeded(String),
    #[error("Market conditions unsafe: {0}")]
    UnsafeMarketConditions(String),
    #[error("Configuration error: {0}")]
    ConfigurationError(String),
    #[error("Computation error: {0}")]
    ComputationError(String),
    #[error("RPC error: {0}")]
    RpcError(String),
    #[error("Network analysis failed: {0}")]
    NetworkAnalysisError(String),
    #[error("Transaction analysis failed: {0}")]
    TransactionAnalysisError(String),
    #[error("ML model error: {0}")]
    MLModelError(String),
    #[error("Memory overflow: {0}")]
    MemoryOverflow(String),
    #[error("Concurrency error: {0}")]
    ConcurrencyError(String),
}

// Optional registry to provide CMS/leaders/scorecard/network to the weight oracle without changing call sites.
pub struct WeightCtx {
    pub cms: CountMin,
    pub leaders: Vec<Pubkey>,
    pub scorecard: ValidatorScorecard,
    pub net: NetworkConditions,
}
static WEIGHT_CTX: once_cell::sync::OnceCell<std::sync::Arc<WeightCtx>> = once_cell::sync::OnceCell::new();
pub fn set_weight_ctx(ctx: std::sync::Arc<WeightCtx>) -> Result<(), std::sync::Arc<WeightCtx>> { WEIGHT_CTX.set(ctx) }

// ==============================
// PIECE 93 removed: use subscribe_processed_topk (120) instead.

// ==============================
// PIECE 94 — Compute/MTU Aware Splitter v4.frontier
//   - Explicit Legacy (left) ⟂ V0+ALTs (right) crossover targeting
//   - Exact ALT knapsack (bitset) for right-side V0 (reuses v4 logic locally)
//   - CU-balanced coarse scan + local refine; hard frontier penalty
//   - Safe fallback: if frontier pair overshoots MTU, we compare with stable builder
// ==============================

#[derive(Default, Clone)]
pub struct ProgCuEwma {
    alpha: f64,
    map: std::collections::HashMap<Pubkey, f64>,
}
impl ProgCuEwma {
    pub fn new() -> Self { Self { alpha: 0.18, map: std::collections::HashMap::with_capacity(128) } }
    #[inline] pub fn record(&mut self, prog: Pubkey, used_cu: u64) {
        let e = self.map.entry(prog).or_insert(60_000.0);
        *e = (1.0 - self.alpha) * *e + self.alpha * (used_cu as f64);
    }
    #[inline] pub fn predict_ix(&self, ix: &Instruction) -> f64 {
        *self.map.get(&ix.program_id).unwrap_or(&60_000.0)
    }
}
#[inline]
fn cum_cu(instrs: &[Instruction], ewma: &ProgCuEwma) -> Vec<f64> {
    let mut pref = Vec::with_capacity(instrs.len() + 1);
    pref.push(0.0);
    for ix in instrs { pref.push(pref.last().unwrap() + ewma.predict_ix(ix)); }
    pref
}

#[inline]
fn build_legacy_min_bytes_fit_mtu(
    payer: &Keypair, bh: Hash, instrs: Vec<Instruction>, cu_price: u64, cu_limit: u32
) -> VersionedTransaction {
    let v = normalize_budget(canonicalize_instructions(instrs), cu_limit, cu_price);
    let msg = solana_sdk::message::Message::new(&v, Some(&payer.pubkey()));
    let tx = solana_sdk::transaction::Transaction::new(&[payer], msg, bh);
    tx.into()
}

// Dedicated v0 builder that ALWAYS returns V0 (forced), using exact ALT coverage (bitset knapsack)
mod frontier_v0_exact {
    use super::*;
    #[derive(Clone)]
    struct Bitset { w: Vec<u64>, nbits: usize }
    impl Bitset {
        fn new(nbits: usize) -> Self { Self { w: vec![0u64; (nbits+63)>>6], nbits } }
        #[inline] fn set(&mut self, i: usize) { self.w[i>>6] |= 1u64 << (i & 63); }
        #[inline] fn or_in(&mut self, other:&Bitset){ for (a,b) in self.w.iter_mut().zip(other.w.iter()) { *a |= *b; } }
        #[inline] fn clone_and_or(&self, other:&Bitset)->Self{ let mut t=self.clone(); t.or_in(other); t }
        #[inline] fn count(&self)->usize{ self.w.iter().map(|x| x.count_ones() as usize).sum() }
        #[inline] fn diff_count_with(&self, other:&Bitset)->usize{
            let mut c=0usize; for (a,b) in self.w.iter().zip(other.w.iter()) { c += ((!a)&b).count_ones() as usize; } c
        }
    }
    fn sort_luts_canonical(mut luts: Vec<AddressLookupTableAccount>) -> Vec<AddressLookupTableAccount> {
        luts.sort_by(|a,b| a.key.to_bytes().cmp(&b.key.to_bytes()));
        luts
    }
    pub fn build_forced_v0(
        payer:&Keypair, bh:Hash, instrs:&[Instruction], cu_price:u64, cu_limit:u32,
        all_luts:&[AddressLookupTableAccount]
    ) -> VersionedTransaction {
        let start = std::time::Instant::now();
        const MAX_DFS_MICROS: u64 = 1200;
        let v = normalize_budget(canonicalize_instructions(instrs.to_vec()), cu_limit, cu_price);
        let need = super::required_pubkeys_for_instrs(&v, &payer.pubkey());
        if all_luts.is_empty() || need.len() <= 1 {
            let v0 = V0Message::compile(&payer.pubkey(), &v, &[]);
            if let Ok(vtx) = VersionedTransaction::try_new(VersionedMessage::V0(v0), &[payer]) { return vtx; }
            // Fallback to legacy if V0 signing fails for any reason
            let legacy_msg = solana_sdk::message::Message::new(&v, Some(&payer.pubkey()));
            return solana_sdk::transaction::Transaction::new(&[payer], legacy_msg, bh).into();
        }
        use std::collections::HashMap;
        let mut idx_of: HashMap<Pubkey,usize> = HashMap::with_capacity(need.len());
        for (i,k) in need.iter().enumerate(){ idx_of.insert(*k,i); }
        let luts = sort_luts_canonical(all_luts.to_vec());

        #[derive(Clone)]
        struct Cand { i: usize, bits: Bitset, hit: usize, dens: f64 }
        let mut cands: Vec<Cand> = Vec::with_capacity(luts.len());
        for (i,lut) in luts.iter().enumerate() {
            let mut bs = Bitset::new(need.len()); let mut hit=0usize;
            for k in &lut.addresses { if let Some(&ix) = idx_of.get(k) { bs.set(ix); hit+=1; } }
            if hit>0 { let dens=(hit as f64)/(lut.addresses.len().max(1) as f64); cands.push(Cand{ i, bits:bs, hit, dens }); }
        }
        if cands.is_empty() {
            let v0 = V0Message::compile(&payer.pubkey(), &v, &[]);
            return VersionedTransaction::try_new(VersionedMessage::V0(v0), &[payer]).expect("sign v0");
        }
        const MAXC:usize = 24;
        cands.sort_by(|a,b| b.hit.cmp(&a.hit)
            .then_with(|| b.dens.partial_cmp(&a.dens).unwrap())
            .then_with(|| luts[b.i].key.to_bytes().cmp(&luts[a.i].key.to_bytes())));
        if cands.len()>MAXC { cands.truncate(MAXC); }

        let m=cands.len(); let mut suf: Vec<Bitset> = (0..=m).map(|_| Bitset::new(need.len())).collect();
        for i in (0..m).rev(){ suf[i]=suf[i+1].clone(); suf[i].or_in(&cands[i].bits); }

        let max_pick = 8usize.min(m);
        let mut best_cov=0usize; let mut best_len=usize::MAX; let mut best_sel:Vec<usize>=Vec::new();
        let mut cur_sel:Vec<usize>=Vec::with_capacity(max_pick);
        fn dfs(
            idx:usize, picked:usize, cover:&Bitset, cands:&[Cand], luts:&[AddressLookupTableAccount], suf:&[Bitset],
            payer:&Keypair, bh:Hash, v:&[Instruction], max_pick:usize,
            best_cov:&mut usize, best_len:&mut usize, best_sel:&mut Vec<usize>, cur_sel:&mut Vec<usize>,
            start:&std::time::Instant
        ){
            if start.elapsed().as_micros() as u64 > MAX_DFS_MICROS { return; }
            let ub_extra = cover.diff_count_with(&suf[idx]);
            if cover.count() + ub_extra < *best_cov { return; }
            if idx==cands.len() || picked==max_pick {
                let chosen: Vec<AddressLookupTableAccount> = cur_sel.iter().map(|&j| luts[cands[j].i].clone()).collect();
                let v0 = V0Message::compile(&payer.pubkey(), v, &chosen);
                if let Ok(vtx) = VersionedTransaction::try_new(VersionedMessage::V0(v0), &[payer]) {
                    let len = super::wire_len(&vtx);
                    let cov = cover.count();
                    if cov > *best_cov || (cov==*best_cov && len < *best_len) { *best_cov=cov; *best_len=len; *best_sel=cur_sel.clone(); }
                }
                let cov = cover.count();
                return;
            }
            cur_sel.push(idx);
            let with = cover.clone_and_or(&cands[idx].bits);
            dfs(idx+1, picked+1, &with, cands, luts, suf, payer, bh, v, max_pick, best_cov, best_len, best_sel, cur_sel, start);
            cur_sel.pop();
            let ub_extra2 = cover.diff_count_with(&suf[idx+1]);
            if cover.count() + ub_extra2 < *best_cov { return; }
            dfs(idx+1, picked, cover, cands, luts, suf, payer, bh, v, max_pick, best_cov, best_len, best_sel, cur_sel, start);
        }
        let cover0 = Bitset::new(need.len());
        dfs(0,0,&cover0,&cands,&luts,&suf,payer,bh,&v,max_pick,&mut best_cov,&mut best_len,&mut best_sel,&mut cur_sel,&start);
        let chosen: Vec<AddressLookupTableAccount> = best_sel.iter().map(|&j| luts[cands[j].i].clone()).collect();
        // Protocol-cap guard: if V0 compile/sign fails (e.g., too many accounts/lookups), drop lowest-yield LUTs and retry
        let mut chosen_cur = chosen;
        loop {
            let v0 = V0Message::compile(&payer.pubkey(), &v, &chosen_cur);
            match VersionedTransaction::try_new(VersionedMessage::V0(v0), &[payer]) {
                Ok(vtx) => break vtx,
                Err(_e) => {
                    if chosen_cur.is_empty() { // fallback to legacy
                        let legacy_msg = solana_sdk::message::Message::new(&v, Some(&payer.pubkey()));
                        break solana_sdk::transaction::Transaction::new(&[payer], legacy_msg, bh).into();
                    }
                    // drop a LUT with smallest coverage heuristic
                    let mut drop_i = 0usize; let mut best_hit = usize::MAX;
                    for (i, lut) in chosen_cur.iter().enumerate() {
                        let hit = lut.addresses.iter().filter(|k| need.contains(k)).count();
                        if hit < best_hit { best_hit = hit; drop_i = i; }
                    }
                    chosen_cur.remove(drop_i);
                }
            }
        }
    }
}

// ==============================
// PIECE 94W-RT — Streaming Weight Oracle (pinned micro-thread)
//   - Emits leader-window smoothed multipliers for hazard/MTU/CMS
//   - Readers do O(1) atomic snapshot; no locks, no allocs
//   - Drop-in with your existing PIECE 94C allocator
// ==============================
// PIECE 126 — Cacheline Seqlock (shared utility)
#[repr(align(64))]
pub struct Seqlock<T> { seq: core::sync::atomic::AtomicU64, val: T }
impl<T: Copy> Seqlock<T> { pub const fn new(init: T) -> Self { Self { seq: core::sync::atomic::AtomicU64::new(0), val: init } }
    #[inline] pub fn read(&self, f: impl Fn(&T) -> T) -> T { use core::sync::atomic::Ordering::{Acquire,Relaxed}; loop { let s0=self.seq.load(Acquire); if s0 & 1 != 0 { core::hint::spin_loop(); continue; } let snap=f(&self.val); let s1=self.seq.load(Relaxed); if s0==s1 { return snap; } } }
    #[inline] pub fn write(&self, mut up: impl FnMut(&mut T)) { use core::sync::atomic::Ordering::{Release,Relaxed,SeqCst}; self.seq.fetch_add(1, Relaxed); up(unsafe { &mut *(&self.val as *const _ as *mut T) }); self.seq.fetch_add(1, Release); core::sync::atomic::fence(SeqCst); } }

// ==============================
// PIECE 94W-RT.v2 — Streaming Weight Oracle (seqlock+pinned)
// ==============================
#[inline] fn f2a(x: f64) -> u64 { x.to_bits() }
#[inline] fn a2f(x: u64) -> f64 { f64::from_bits(x) }

#[derive(Clone, Copy)]
pub struct RtCfg { pub alpha_base: f64, pub tick: std::time::Duration, pub w_min: f64, pub w_max: f64, pub beta_mtu: f64, pub beta_cms: f64, pub beta_ldr: f64, pub delta_soft: usize, pub stale_ns_cutoff: u64 }
impl Default for RtCfg { fn default() -> Self { Self { alpha_base: 0.22, tick: std::time::Duration::from_millis(4), w_min: 0.90, w_max: 1.12, beta_mtu: 0.35, beta_cms: 0.25, beta_ldr: 0.25, delta_soft: 16, stale_ns_cutoff: 220_000_000 } } }

#[repr(C)]
#[derive(Clone, Copy)]
pub enum RtEvent { SlackOverSoftBytes(f32), CmsHeat01(f32), LeaderHazard01(f32), RotateWindow, SlotPhase01(f32) }

struct MpscRing { buf: Box<[core::cell::UnsafeCell<RtEvent>]>, cap_mask: usize, head: core::sync::atomic::AtomicU64, tail: core::sync::atomic::AtomicU64 }
unsafe impl Send for MpscRing {} unsafe impl Sync for MpscRing {}
impl MpscRing { fn new(cap_pow2: usize) -> Self { let cap=cap_pow2.next_power_of_two().max(64); let v=(0..cap).map(|_| core::cell::UnsafeCell::new(RtEvent::RotateWindow)).collect::<Vec<_>>(); Self { buf: v.into_boxed_slice(), cap_mask: cap-1, head: core::sync::atomic::AtomicU64::new(0), tail: core::sync::atomic::AtomicU64::new(0) } }
    #[inline] fn push(&self, ev: RtEvent) { use core::sync::atomic::Ordering::Relaxed; let h=self.head.fetch_add(1, Relaxed); let idx=(h as usize)&self.cap_mask; unsafe{ core::ptr::write(self.buf[idx].get(), ev); } let t=self.tail.load(Relaxed); if h - t > (self.cap_mask as u64) { let _=self.tail.fetch_add(1, Relaxed); } }
    #[inline] fn drain(&self, mut f: impl FnMut(RtEvent)) { use core::sync::atomic::Ordering::Relaxed; loop { let t=self.tail.load(Relaxed); let h=self.head.load(Relaxed); if t>=h { break; } let idx=(t as usize)&self.cap_mask; let ev=unsafe{ core::ptr::read(self.buf[idx].get()) }; if self.tail.compare_exchange_weak(t,t+1,Relaxed,Relaxed).is_ok(){ f(ev); } } }
}

pub struct RtSnapshot { pub inner: Seqlock<(f64,f64,f64,u64)> }
impl Default for RtSnapshot { fn default() -> Self { Self { inner: Seqlock::new((1.0,1.0,1.0,0)) } } }

pub struct WeightOracleRt { q: std::sync::Arc<MpscRing>, pub snap: std::sync::Arc<RtSnapshot>, pub cfg: RtCfg }
impl WeightOracleRt { pub fn spawn(cfg: RtCfg) -> Self { let q=std::sync::Arc::new(MpscRing::new(512)); let snap=std::sync::Arc::new(RtSnapshot::default()); let q_cl=q.clone(); let snap_cl=snap.clone(); std::thread::Builder::new().name("94W-RT".into()).spawn(move || run_rt_thread(cfg, q_cl, snap_cl)).expect("spawn 94W-RT"); Self { q, snap, cfg } }
    #[inline] pub fn submit(&self, ev: RtEvent) { self.q.push(ev); }
    #[allow(clippy::too_many_arguments)] pub fn pair_weights_rt(&self, mtu:&MtuLearner, delta_soft:usize, base_w_bounds:(f64,f64), beta:(f64,f64,f64), len_l:usize, len_r:usize, cms_l:f64, cms_r:f64) -> (f64,f64) {
        let (mtu_mult, cms_mult, ldr_mult, _age_ns) = self.snap.inner.read(|t| *t);
        #[inline] fn slack_lift(len: usize, mtu: &MtuLearner, delta_soft: usize) -> f64 { let soft=mtu.safe.saturating_sub(delta_soft); let deficit=len.saturating_sub(soft) as f64; 1.0 + (deficit/96.0).min(1.0) }
        let (bmtu,bcms,bldr)=beta; let ldrv=1.0 + bldr*(ldr_mult-1.0);
        let mut wl=(1.0 + bmtu*(mtu_mult-1.0)) * slack_lift(len_l, mtu, delta_soft);
        let mut wr=(1.0 + bmtu*(mtu_mult-1.0)) * slack_lift(len_r, mtu, delta_soft);
        wl *= 1.0 + bcms*((cms_mult-1.0)+cms_l); wr *= 1.0 + bcms*((cms_mult-1.0)+cms_r);
        wl *= ldrv; wr *= ldrv; let (wmin,wmax)=base_w_bounds; wl=wl.clamp(wmin,wmax); wr=wr.clamp(wmin,wmax); let m=0.5*(wl+wr); (wl/m, wr/m)
    }
    /// Triple-weights from RT snapshot (mean-normalized, clamp to base bounds).
    /// Inputs:
    ///   - len_*: wire lengths in bytes of (left, mid, right) candidates
    ///   - cms_*: per-shard CMS hotness in [0,1] (already scaled)
    ///   - base_w_bounds: (w_min, w_max)
    ///   - beta: (beta_mtu, beta_cms, beta_ldr) same semantics as pair helper
    #[allow(clippy::too_many_arguments)]
    pub fn triple_weights_rt(
        &self,
        mtu: &MtuLearner,
        delta_soft: usize,
        base_w_bounds: (f64, f64),
        beta: (f64, f64, f64),
        len_l: usize, len_m: usize, len_r: usize,
        cms_l: f64,  cms_m: f64,  cms_r: f64,
    ) -> (f64, f64, f64) {
        let (mtu_mult, cms_mult, ldr_mult, _age_ns) = self.snap.inner.read(|t| *t);
        #[inline] fn slack_lift(len: usize, mtu: &MtuLearner, delta_soft: usize) -> f64 {
            let soft = mtu.safe.saturating_sub(delta_soft);
            let deficit = len.saturating_sub(soft) as f64;
            1.0 + (deficit / 96.0).min(1.0)
        }
        let (bmtu, bcms, bldr) = beta;
        let ldrv = 1.0 + bldr * (ldr_mult - 1.0);
        let mut wl = (1.0 + bmtu * (mtu_mult - 1.0)) * slack_lift(len_l, mtu, delta_soft);
        let mut wm = (1.0 + bmtu * (mtu_mult - 1.0)) * slack_lift(len_m, mtu, delta_soft);
        let mut wr = (1.0 + bmtu * (mtu_mult - 1.0)) * slack_lift(len_r, mtu, delta_soft);
        wl *= 1.0 + bcms * ((cms_mult - 1.0) + cms_l);
        wm *= 1.0 + bcms * ((cms_mult - 1.0) + cms_m);
        wr *= 1.0 + bcms * ((cms_mult - 1.0) + cms_r);
        wl *= ldrv; wm *= ldrv; wr *= ldrv;
        let (wmin, wmax) = base_w_bounds;
        wl = wl.clamp(wmin, wmax); wm = wm.clamp(wmin, wmax); wr = wr.clamp(wmin, wmax);
        let m = (wl + wm + wr) / 3.0;
        (wl/m, wm/m, wr/m)
    }
    #[inline] pub fn is_stale(&self) -> bool { let (_,_,_,age)=self.snap.inner.read(|t| *t); age > self.cfg.stale_ns_cutoff }
}

fn run_rt_thread(cfg: RtCfg, q: std::sync::Arc<MpscRing>, snap: std::sync::Arc<RtSnapshot>) { let mut mtu_tension=1.0f64; let mut cms_heat=0.0f64; let mut ldr_hazard=0.0f64; let mut alpha=cfg.alpha_base; let t0=std::time::Instant::now();
    #[cfg(all(target_os = "linux", feature = "rt"))] unsafe { let mut param = libc::sched_param { sched_priority: 1 }; let _ = libc::sched_setscheduler(0, libc::SCHED_FIFO, &mut param); if let Some(core) = core_affinity::get_core_ids().and_then(|mut v| v.pop()) { let _ = core_affinity::set_for_current(core); } }
    loop { q.drain(|ev| { match ev { RtEvent::SlackOverSoftBytes(v)=>{ let lift=(v as f64/96.0).clamp(0.0,1.0); mtu_tension=(1.0-alpha)*mtu_tension + alpha*(1.0+lift); }, RtEvent::CmsHeat01(h)=>{ let h=(h as f64).clamp(0.0,1.0); cms_heat=(1.0-alpha)*cms_heat + alpha*h; }, RtEvent::LeaderHazard01(h)=>{ let h=(h as f64).clamp(0.0,1.0); ldr_hazard=(1.0-alpha)*ldr_hazard + alpha*h; }, RtEvent::RotateWindow=>{ mtu_tension=1.0 + (mtu_tension-1.0)*0.55; cms_heat*=0.78; ldr_hazard*=0.78; }, RtEvent::SlotPhase01(p)=>{ let ph=(p as f64).clamp(0.0,1.0); alpha=(cfg.alpha_base*(0.75+0.5*ph)).clamp(0.10,0.45); } } }); let age_ns=t0.elapsed().as_nanos() as u64; let m=mtu_tension.clamp(1.0,2.0); let c=1.0+cms_heat; let l=1.0+ldr_hazard; snap.inner.write(|t| *t=(m,c,l,age_ns)); std::thread::sleep(cfg.tick); } }

// ==============================
// PIECE 94W — Weight Oracle
//  - Derives per-shard weights w_i ∈ [w_min, w_max] from:
//      (1) soft-MTU slack     → smaller slack ⇒ larger weight
//      (2) CMS hotness        → higher conflict risk ⇒ larger weight
//      (3) Leader hazard      → worse leader & higher congestion ⇒ larger weight
//  - Deterministic, branch-minimal hot path, renormalized to mean 1.0
// ==============================

pub struct WeightOracle {
    pub beta_mtu: f64,
    pub beta_cms: f64,
    pub beta_ldr: f64,
    pub delta_soft: usize,
    pub w_min: f64,
    pub w_max: f64,
}

impl Default for WeightOracle {
    fn default() -> Self {
        Self { beta_mtu: 0.35, beta_cms: 0.25, beta_ldr: 0.25, delta_soft: 16, w_min: 0.90, w_max: 1.12 }
    }
}

#[inline]
fn collect_rw_sets_local(ixs: &[Instruction]) -> (Vec<Pubkey>, Vec<Pubkey>) {
    use std::collections::HashSet;
    let mut ro = HashSet::with_capacity(64);
    let mut rw = HashSet::with_capacity(64);
    for ix in ixs {
        for a in &ix.accounts { if a.is_writable { rw.insert(a.pubkey); } else { ro.insert(a.pubkey); } }
        ro.insert(ix.program_id);
    }
    (ro.into_iter().collect(), rw.into_iter().collect())
}

#[inline]
fn cms_hotness_weight(ixs: &[Instruction], cms: &CountMin, base: f64) -> f64 {
    let (ro, rw) = collect_rw_sets_local(ixs);
    let risk = weighted_conflict_score(cms, &ro, &rw).clamp(0.0, 1.0);
    base * (1.0 + risk)
}

#[inline]
fn mtu_slack_weight<V: Into<VersionedTransaction> + Clone>(vtx: &V, mtu: &MtuLearner, delta_soft: usize, base: f64) -> f64 {
    let len = wire_len(vtx);
    let soft_cap = mtu.safe.saturating_sub(delta_soft);
    let deficit = len.saturating_sub(soft_cap) as f64;
    let lift = (deficit / 96.0).min(1.0);
    base * (1.0 + lift)
}

#[inline]
fn leader_hazard_weight(leaders: &[Pubkey], scorecard: &ValidatorScorecard, net: &NetworkConditions, base: f64) -> f64 {
    let mut ls: Vec<f64> = leaders.iter().filter_map(|v| scorecard.raw_score(*v)).collect();
    ls.sort_by(|a,b| b.partial_cmp(a).unwrap());
    let q = if ls.is_empty() { 0.0 } else { ls.iter().take(ls.len().min(3)).sum::<f64>() / (ls.len().min(3) as f64) };
    let quality = 1.0 / (1.0 + (-q * 0.8).exp());
    let hazard = (1.0 - quality) * net.congestion_level.clamp(0.0, 1.0);
    base * (1.0 + hazard)
}

impl WeightOracle {
    #[allow(clippy::too_many_arguments)]
    pub fn pair_weights(
        &self,
        payer: &Keypair,
        bh: Hash,
        left: &[Instruction],
        right: &[Instruction],
        cu_price: u64,
        cu_guess: (u32,u32),
        alts: &[AddressLookupTableAccount],
        mtu: &MtuLearner,
        cms: &CountMin,
        leaders: &[Pubkey],
        scorecard: &ValidatorScorecard,
        net: &NetworkConditions,
    ) -> (f64, f64) {
        let v_l = build_min_bytes_fit_mtu_stable(payer, bh, left.to_vec(),  cu_price, cu_guess.0, alts, mtu);
        let v_r = build_min_bytes_fit_mtu_stable(payer, bh, right.to_vec(), cu_price, cu_guess.1, alts, mtu);
        let mut wl = 1.0; let mut wr = 1.0;
        wl = mtu_slack_weight(&v_l, mtu, self.delta_soft, wl);
        wr = mtu_slack_weight(&v_r, mtu, self.delta_soft, wr);
        wl *= 1.0 + self.beta_cms * (cms_hotness_weight(left,  cms, 1.0) - 1.0);
        wr *= 1.0 + self.beta_cms * (cms_hotness_weight(right, cms, 1.0) - 1.0);
        let w_ldr = leader_hazard_weight(leaders, scorecard, net, 1.0);
        wl *= 1.0 + self.beta_ldr * (w_ldr - 1.0);
        wr *= 1.0 + self.beta_ldr * (w_ldr - 1.0);
        wl = wl.clamp(self.w_min, self.w_max);
        wr = wr.clamp(self.w_min, self.w_max);
        let m = 0.5 * (wl + wr);
        (wl / m, wr / m)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn triple_weights(
        &self,
        payer: &Keypair,
        bh: Hash,
        left: &[Instruction],
        mid:  &[Instruction],
        right:&[Instruction],
        cu_price: u64,
        cu_guess: (u32,u32,u32),
        alts: &[AddressLookupTableAccount],
        mtu: &MtuLearner,
        cms: &CountMin,
        leaders: &[Pubkey],
        scorecard: &ValidatorScorecard,
        net: &NetworkConditions,
    ) -> (f64, f64, f64) {
        let v_l = build_min_bytes_fit_mtu_stable(payer, bh, left.to_vec(),  cu_price, cu_guess.0, alts, mtu);
        let v_m = build_min_bytes_fit_mtu_stable(payer, bh, mid.to_vec(),   cu_price, cu_guess.1, alts, mtu);
        let v_r = build_min_bytes_fit_mtu_stable(payer, bh, right.to_vec(), cu_price, cu_guess.2, alts, mtu);
        let mut wl = mtu_slack_weight(&v_l, mtu, self.delta_soft, 1.0);
        let mut wm = mtu_slack_weight(&v_m, mtu, self.delta_soft, 1.0);
        let mut wr = mtu_slack_weight(&v_r, mtu, self.delta_soft, 1.0);
        wl *= 1.0 + self.beta_cms * (cms_hotness_weight(left,  cms, 1.0) - 1.0);
        wm *= 1.0 + self.beta_cms * (cms_hotness_weight(mid,   cms, 1.0) - 1.0);
        wr *= 1.0 + self.beta_cms * (cms_hotness_weight(right, cms, 1.0) - 1.0);
        let w_ldr = leader_hazard_weight(leaders, scorecard, net, 1.0);
        let ldr_factor = 1.0 + self.beta_ldr * (w_ldr - 1.0);
        wl *= ldr_factor; wm *= ldr_factor; wr *= ldr_factor;
        wl = wl.clamp(self.w_min, self.w_max);
        wm = wm.clamp(self.w_min, self.w_max);
        wr = wr.clamp(self.w_min, self.w_max);
        let m = (wl + wm + wr) / 3.0;
        (wl / m, wm / m, wr / m)
    }
}

// ==============================
// PIECE 94C — CU Allotment QP (Weighted Water-Filling + Safety Guard)
// ==============================

#[inline]
fn predict_cu_for_instrs(instrs: &[Instruction], ewma: &ProgCuEwma) -> f64 {
    let mut s = 0.0; for ix in instrs { s += ewma.predict_ix(ix); } s.max(1.0)
}

fn qp_waterfill_with_bounds(
    mu: &[f64], w: &[f64], l: &[f64], u: &[f64], total: f64,
) -> Vec<f64> {
    debug_assert!(mu.len() == w.len() && w.len() == l.len() && l.len() == u.len());
    let k = mu.len(); let sum_l: f64 = l.iter().sum(); let sum_u: f64 = u.iter().sum();
    let t_eff = total.clamp(sum_l, sum_u.max(sum_l + 1.0));
    let mut lam_lo = f64::INFINITY; let mut lam_hi = f64::NEG_INFINITY;
    for i in 0..k { let wi=w[i].max(1e-9); lam_lo = lam_lo.min(wi*(mu[i]-u[i])); lam_hi = lam_hi.max(wi*(mu[i]-l[i])); }
    let mut lo = lam_lo - 1e9; let mut hi = lam_hi + 1e9;
    for _ in 0..52 { let mid=0.5*(lo+hi); let mut s=0.0; for i in 0..k { let wi=w[i].max(1e-9); let xi=(mu[i] - mid/wi).clamp(l[i], u[i]); s+=xi; } if s > t_eff { lo=mid; } else { hi=mid; } }
    let lam = hi; let mut x=vec![0.0; k]; for i in 0..k { let wi=w[i].max(1e-9); x[i] = (mu[i] - lam/wi).clamp(l[i], u[i]); } x
}

fn round_with_exact_sum(mut xs: Vec<f64>, sum_target: u32, caps: &[u32]) -> Vec<u32> {
    let k=xs.len(); let mut xi:Vec<u32>=xs.iter().map(|v| v.floor() as u32).collect();
    let mut frac:Vec<(usize,f64)>=xs.iter().enumerate().map(|(i,v)|(i, v - (*v).floor())).collect();
    let mut cur: u32 = xi.iter().sum();
    if cur < sum_target { frac.sort_by(|a,b| b.1.partial_cmp(&a.1).unwrap()); let mut need=sum_target-cur; for (i,_) in frac { if need==0 {break;} if xi[i] < caps[i] { xi[i]+=1; need-=1; } } }
    else if cur > sum_target { frac.sort_by(|a,b| a.1.partial_cmp(&b.1).unwrap()); let mut extra=cur-sum_target; for (i,_) in frac { if extra==0 {break;} if xi[i] > 0 { xi[i]-=1; extra-=1; } } }
    xi
}

pub fn cu_allotment_qp(
    mu_guess: &[f64], weights_opt: Option<&[f64]>, floors_u32: &[u32], caps_u32: &[u32], total_cu_exact: u32, safety_mul: f64,
) -> Vec<u32> {
    let k=mu_guess.len(); assert!(floors_u32.len()==k && caps_u32.len()==k);
    let mut mu:Vec<f64>=mu_guess.to_vec(); let mut w:Vec<f64>=vec![1.0; k];
    if let Some(ws)=weights_opt { assert!(ws.len()==k); for i in 0..k { w[i]=ws[i].max(1e-6); } }
    let mut floors_f:Vec<f64>=Vec::with_capacity(k); let mut caps_f:Vec<f64>=Vec::with_capacity(k);
    for i in 0..k { let guard=(mu[i]*safety_mul).ceil().max(1.0); let l_i=(floors_u32[i] as f64).max(guard); let u_i=(caps_u32[i] as f64).max(l_i); floors_f.push(l_i); caps_f.push(u_i); }
    let x_cont = qp_waterfill_with_bounds(&mu, &w, &floors_f, &caps_f, total_cu_exact as f64);
    round_with_exact_sum(x_cont, total_cu_exact, caps_u32)
}

#[inline]
pub fn cu_shape_pair_from_instrs(
    total_cu: u32,
    left_instrs: &[Instruction], right_instrs: &[Instruction],
    ewma: &ProgCuEwma,
    floors: (u32,u32), caps: (u32,u32),
    safety_mul: f64,
    weights_opt: Option<(f64,f64)>,
) -> (u32,u32) {
    let mu_l=predict_cu_for_instrs(left_instrs, ewma); let mu_r=predict_cu_for_instrs(right_instrs, ewma);
    let mu=[mu_l, mu_r]; let weights=weights_opt.map(|(a,b)| vec![a,b]);
    let xs=cu_allotment_qp(&mu, weights.as_deref(), &[floors.0, floors.1], &[caps.0, caps.1], total_cu, safety_mul);
    (xs[0], xs[1])
}

#[inline]
pub fn cu_shape_triple_from_instrs(
    total_cu: u32,
    left_instrs: &[Instruction], mid_instrs: &[Instruction], right_instrs: &[Instruction],
    ewma: &ProgCuEwma,
    floors: (u32,u32,u32), caps: (u32,u32,u32),
    safety_mul: f64,
    weights_opt: Option<(f64,f64,f64)>,
) -> (u32,u32,u32) {
    let mu_l=predict_cu_for_instrs(left_instrs, ewma); let mu_m=predict_cu_for_instrs(mid_instrs, ewma); let mu_r=predict_cu_for_instrs(right_instrs, ewma);
    let mu=[mu_l, mu_m, mu_r]; let weights=weights_opt.map(|(a,b,c)| vec![a,b,c]);
    let xs=cu_allotment_qp(&mu, weights.as_deref(), &[floors.0, floors.1, floors.2], &[caps.0, caps.1, caps.2], total_cu, safety_mul);
    (xs[0], xs[1], xs[2])
}

#[inline]
fn quic_oversize_penalties(len: usize, mtu: &MtuLearner, delta_soft: usize) -> (f64, f64) {
    let over_hard = (!mtu.quic_safe(len) as u8 as f64);
    let soft = (len.saturating_sub(mtu.safe.saturating_sub(delta_soft)) as f64).max(0.0);
    (over_hard, soft)
}

/// Core: find split index i s.t. left prefers Legacy, right prefers V0; minimize CU gap & MTU penalties.
pub fn split_instrs_cu_frontier_mtu(
    payer: &Keypair,
    bh: Hash,
    instrs: &[Instruction],
    cu_price: u64,
    cu_limit_each: (u32, u32),
    alts: &[AddressLookupTableAccount],
    mtu: &MtuLearner,
    ewma: &ProgCuEwma,
) -> usize {
    let n = instrs.len();
    if n <= 1 { return 1; }

    let canon = canonicalize_instructions(instrs.to_vec());
    let pref = cum_cu(&canon, ewma);
    let total_cu = *pref.last().unwrap();

    let mut i0 = n / 2;
    let mut gap0 = (total_cu - 2.0 * pref[i0]).abs();
    let step = (n / 16).max(1);
    for i in (1..n).step_by(step) {
        let g = (total_cu - 2.0 * pref[i]).abs();
        if g < gap0 { gap0 = g; i0 = i; }
    }

    let r = 10.min(n.saturating_sub(1));
    let lambda_hard = 1e8_f64;
    let lambda_soft = 6000.0;
    let lambda_frontier = 10000.0;
    let delta_soft = 16usize;

    let mut best = i0;
    let mut best_obj = f64::INFINITY;

    for i in i0.saturating_sub(r)..=i0.saturating_add(r).min(n - 1) {
        let (left, right) = (&canon[..i], &canon[i..]);

        let vtx_l_leg = build_legacy_min_bytes_fit_mtu(payer, bh, left.to_vec(),  cu_price, cu_limit_each.0);
        let vtx_l_v0  = frontier_v0_exact::build_forced_v0( payer, bh, left,  cu_price, cu_limit_each.0, alts);
        let vtx_r_v0  = frontier_v0_exact::build_forced_v0( payer, bh, right, cu_price, cu_limit_each.1, alts);
        let vtx_r_leg = build_legacy_min_bytes_fit_mtu(payer, bh, right.to_vec(), cu_price, cu_limit_each.1);

        let len_ll = wire_len(&vtx_l_leg);
        let len_lv0= wire_len(&vtx_l_v0);
        let len_rv0= wire_len(&vtx_r_v0);
        let len_rl = wire_len(&vtx_r_leg);

        let frontier_pen = (len_ll > len_lv0) as u8 as f64 * lambda_frontier
                         + (len_rv0 >= len_rl) as u8 as f64 * lambda_frontier;

        let (over_hard_l, soft_l) = quic_oversize_penalties(len_ll, mtu, delta_soft);
        let (over_hard_r, soft_r) = quic_oversize_penalties(len_rv0, mtu, delta_soft);

        let cu_gap = (total_cu - 2.0 * pref[i]).abs();

        let obj = cu_gap
                + lambda_hard * (over_hard_l + over_hard_r)
                + lambda_soft * (soft_l + soft_r)
                + frontier_pen;

        if obj < best_obj { best_obj = obj; best = i; }
    }

    best
}

pub fn build_split_cu_aware(
    payer: &Keypair,
    bh: Hash,
    instrs: Vec<Instruction>,
    cu_price: u64,
    cu_limit_each: (u32, u32),
    alts: &[AddressLookupTableAccount],
    prog_cu: &ProgCuEwma,
    mtu: &MtuLearner,
) -> (VersionedTransaction, VersionedTransaction) {
    let canon = canonicalize_instructions(instrs);
    let cut = split_instrs_cu_frontier_mtu(payer, bh, &canon, cu_price, cu_limit_each, alts, mtu, prog_cu);

    let (left, right) = (canon[..cut].to_vec(), canon[cut..].to_vec());
    // Shape CU pair using PIECE 94C allocator (safety-guarded water-filling)
    let total_cu = cu_limit_each.0.saturating_add(cu_limit_each.1);
    // Derive per-shard weights if context is available
    let weights_opt = if let Some(ctx) = WEIGHT_CTX.get() {
        let w_orc = WeightOracle::default();
        let (w_l, w_r) = w_orc.pair_weights(
            payer, bh, &left, &right, cu_price, cu_limit_each, alts, mtu,
            &ctx.cms, &ctx.leaders, &ctx.scorecard, &ctx.net,
        );
        Some((w_l, w_r))
    } else { None };
    let (cu_l, cu_r) = cu_shape_pair_from_instrs(
        total_cu,
        &left,
        &right,
        prog_cu,
        (cu_limit_each.0.min(60_000).max(40_000), cu_limit_each.1.min(60_000).max(40_000)),
        (cu_limit_each.0.max(80_000), cu_limit_each.1.max(80_000)),
        1.12,
        weights_opt,
    );
    let cu_pair = (cu_l, cu_r);

    let a_frontier = build_legacy_min_bytes_fit_mtu(payer, bh, left.clone(),  cu_price, cu_pair.0);
    let b_frontier = frontier_v0_exact::build_forced_v0( payer, bh, &right, cu_price, cu_pair.1, alts);

    let a_stable = build_min_bytes_fit_mtu_stable(payer, bh, left,  cu_price, cu_pair.0, alts, mtu);
    let b_stable = build_min_bytes_fit_mtu_stable(payer, bh, right, cu_price, cu_pair.1, alts, mtu);

    let pair_frontier_len = wire_len(&a_frontier) + wire_len(&b_frontier);
    let pair_stable_len   = wire_len(&a_stable)   + wire_len(&b_stable);

    let frontier_ok = assert_quic_safe(&a_frontier) && assert_quic_safe(&b_frontier);
    let stable_ok   = assert_quic_safe(&a_stable)   && assert_quic_safe(&b_stable);

    match (frontier_ok, stable_ok) {
        (true, false) => (a_frontier, b_frontier),
        (false, true) => (a_stable, b_stable),
        (true, true)  => if pair_frontier_len <= pair_stable_len { (a_frontier, b_frontier) } else { (a_stable, b_stable) },
        (false, false)=> if pair_frontier_len <= pair_stable_len { (a_frontier, b_frontier) } else { (a_stable, b_stable) },
    }
}

// ==============================
// PIECE 94T — Triple Frontier Splitter (Legacy | V0 | V0)
//   - Two cut points c1 < c2 over canon instrs
//   - Left chunk forced LEGACY; middle & right forced V0 with exact ALT knapsack
//   - Coarse grid search + local refine; frontier penalties + MTU & CU balance
//   - Safe comparator vs your existing 2-split frontier/stable builders
// ==============================

#[inline]
fn frontier_len_triplet(
    payer: &Keypair,
    bh: Hash,
    left: &[Instruction],
    mid: &[Instruction],
    right: &[Instruction],
    cu_price: u64,
    cu_limits: (u32, u32, u32),
    alts: &[AddressLookupTableAccount],
) -> (VersionedTransaction, VersionedTransaction, VersionedTransaction, usize, usize, usize, usize, usize, usize) {
    let a_leg = build_legacy_min_bytes_fit_mtu(payer, bh, left.to_vec(),  cu_price, cu_limits.0);
    let b_v0  = frontier_v0_exact::build_forced_v0( payer, bh,  mid,  cu_price, cu_limits.1, alts);
    let c_v0  = frontier_v0_exact::build_forced_v0( payer, bh, right, cu_price, cu_limits.2, alts);
    let la = wire_len(&a_leg);
    let lb = wire_len(&b_v0);
    let lc = wire_len(&c_v0);

    let a_v0  = frontier_v0_exact::build_forced_v0( payer, bh, left, cu_price, cu_limits.0, alts);
    let b_leg = build_legacy_min_bytes_fit_mtu(payer, bh, mid.to_vec(), cu_price, cu_limits.1);
    let c_leg = build_legacy_min_bytes_fit_mtu(payer, bh, right.to_vec(), cu_price, cu_limits.2);
    let la_v0 = wire_len(&a_v0);
    let lb_leg= wire_len(&b_leg);
    let lc_leg= wire_len(&c_leg);

    (a_leg, b_v0, c_v0, la, lb, lc, la_v0, lb_leg, lc_leg)
}

#[inline]
fn quic_pen_pair(len: usize, mtu: &MtuLearner, delta_soft: usize) -> (f64, f64) {
    let over_hard = (!mtu.quic_safe(len) as u8 as f64);
    let soft = (len.saturating_sub(mtu.safe.saturating_sub(delta_soft)) as f64).max(0.0);
    (over_hard, soft)
}

pub fn split_instrs_cu_frontier3_mtu(
    payer: &Keypair,
    bh: Hash,
    instrs: &[Instruction],
    cu_price: u64,
    cu_limits: (u32, u32, u32),
    alts: &[AddressLookupTableAccount],
    mtu: &MtuLearner,
    ewma: &ProgCuEwma,
) -> (usize, usize) {
    let n = instrs.len();
    let canon = canonicalize_instructions(instrs.to_vec());
    if n < 3 { return (1.max(n/3), (2*n/3).max(2).min(n-1)); }

    let pref = cum_cu(&canon, ewma);
    let total = *pref.last().unwrap();
    let target = total / 3.0;

    let step = (n / 12).max(1);
    let delta_soft = 16usize;

    let lambda_hard      = 1e8_f64;
    let lambda_soft      = 6000.0;
    let lambda_frontier  = 10000.0;

    let mut best_i = n/3;
    let mut best_j = 2*n/3;
    let mut best_obj = f64::INFINITY;

    for i in (1..n-1).step_by(step) {
        for j in ((i+1)..n).step_by(step) {
            if j >= n { break; }
            let (l, m, r) = (&canon[..i], &canon[i..j], &canon[j..]);
            let (a_leg, b_v0, c_v0, la, lb, lc, la_v0, lb_leg, lc_leg) =
                frontier_len_triplet(payer, bh, l, m, r, cu_price, cu_limits, alts);

            let front_pen = (la > la_v0) as u8 as f64 * lambda_frontier
                          + (lb >= lb_leg) as u8 as f64 * lambda_frontier
                          + (lc >= lc_leg) as u8 as f64 * lambda_frontier;

            let (oh1, sf1) = quic_pen_pair(la, mtu, delta_soft);
            let (oh2, sf2) = quic_pen_pair(lb, mtu, delta_soft);
            let (oh3, sf3) = quic_pen_pair(lc, mtu, delta_soft);

            let cu1 = pref[i] - pref[0];
            let cu2 = pref[j] - pref[i];
            let cu3 = total - cu1 - cu2;
            let cu_dev = (cu1 - target).abs() + (cu2 - target).abs() + (cu3 - target).abs();

            let obj = cu_dev + lambda_hard * (oh1 + oh2 + oh3) + lambda_soft * (sf1 + sf2 + sf3) + front_pen;

            if obj < best_obj { best_obj = obj; best_i = i; best_j = j; }

            drop((a_leg, b_v0, c_v0));
        }
    }

    let r1 = 6usize.min(best_i.saturating_sub(1));
    let r2 = 6usize.min((n-1).saturating_sub(best_j));
    let mut loc_best = best_obj; let mut loc_i = best_i; let mut loc_j = best_j;
    for i in best_i.saturating_sub(r1)..=best_i.saturating_add(r1).min(n-2) {
        let j_lo = (i+1).max(best_j.saturating_sub(r2));
        let j_hi = (i+1 + r2).min(n-1);
        for j in j_lo..=j_hi {
            let (l, m, r) = (&canon[..i], &canon[i..j], &canon[j..]);
            let (a_leg, b_v0, c_v0, la, lb, lc, la_v0, lb_leg, lc_leg) =
                frontier_len_triplet(payer, bh, l, m, r, cu_price, cu_limits, alts);

            let front_pen = (la > la_v0) as u8 as f64 * lambda_frontier
                          + (lb >= lb_leg) as u8 as f64 * lambda_frontier
                          + (lc >= lc_leg) as u8 as f64 * lambda_frontier;

            let (oh1, sf1) = quic_pen_pair(la, mtu, delta_soft);
            let (oh2, sf2) = quic_pen_pair(lb, mtu, delta_soft);
            let (oh3, sf3) = quic_pen_pair(lc, mtu, delta_soft);

            let cu1 = pref[i] - pref[0];
            let cu2 = pref[j] - pref[i];
            let cu3 = total - cu1 - cu2;
            let cu_dev = (cu1 - target).abs() + (cu2 - target).abs() + (cu3 - target).abs();

            let obj = cu_dev + lambda_hard * (oh1 + oh2 + oh3) + lambda_soft * (sf1 + sf2 + sf3) + front_pen;
            if obj < loc_best { loc_best = obj; loc_i = i; loc_j = j; }
            drop((a_leg, b_v0, c_v0));
        }
    }

    (loc_i, loc_j)
}

pub fn build_split3_cu_frontier(
    payer: &Keypair,
    bh: Hash,
    instrs: Vec<Instruction>,
    cu_price: u64,
    cu_limits: (u32, u32, u32),
    alts: &[AddressLookupTableAccount],
    prog_cu: &ProgCuEwma,
    mtu: &MtuLearner,
) -> (VersionedTransaction, VersionedTransaction, VersionedTransaction) {
    let canon = canonicalize_instructions(instrs);
    let n = canon.len();
    if n < 3 {
        let (a,b) = build_split_cu_aware(payer, bh, canon.clone(), cu_price, (cu_limits.0, cu_limits.2), alts, prog_cu, mtu);
        let c = build_legacy_min_bytes_fit_mtu(payer, bh, Vec::new(), cu_price, cu_limits.1);
        return (a,b,c);
    }

    let (i,j) = split_instrs_cu_frontier3_mtu(payer, bh, &canon, cu_price, cu_limits, alts, mtu, prog_cu);
    let (left, mid, right) = (canon[..i].to_vec(), canon[i..j].to_vec(), canon[j..].to_vec());

    // Shape triple CU using PIECE 94C allocator
    let total_cu = cu_limits.0.saturating_add(cu_limits.1).saturating_add(cu_limits.2);
    // Derive 3-way weights if context is available
    let weights_opt = if let Some(ctx) = WEIGHT_CTX.get() {
        let w_orc = WeightOracle::default();
        let (w_l, w_m, w_r) = w_orc.triple_weights(
            payer, bh, &left, &mid, &right, cu_price, cu_limits, alts, mtu,
            &ctx.cms, &ctx.leaders, &ctx.scorecard, &ctx.net,
        );
        Some((w_l, w_m, w_r))
    } else { None };
    let (cu_l, cu_m, cu_r) = cu_shape_triple_from_instrs(
        total_cu,
        &left, &mid, &right,
        prog_cu,
        (cu_limits.0.min(60_000).max(40_000), cu_limits.1.min(60_000).max(40_000), cu_limits.2.min(60_000).max(40_000)),
        (cu_limits.0.max(80_000),             cu_limits.1.max(80_000),             cu_limits.2.max(80_000)),
        1.12,
        weights_opt,
    );

    let a_leg = build_legacy_min_bytes_fit_mtu(payer, bh, left.clone(),  cu_price, cu_l);
    let b_v0  = frontier_v0_exact::build_forced_v0( payer, bh, &mid,   cu_price, cu_m, alts);
    let c_v0  = frontier_v0_exact::build_forced_v0( payer, bh, &right, cu_price, cu_r, alts);

    let la = wire_len(&a_leg); let lb = wire_len(&b_v0); let lc = wire_len(&c_v0);
    let frontier_ok = assert_quic_safe(&a_leg) && assert_quic_safe(&b_v0) && assert_quic_safe(&c_v0);

    let a_st = build_min_bytes_fit_mtu_stable(payer, bh, left,  cu_price, cu_l, alts, mtu);
    let b_st = build_min_bytes_fit_mtu_stable(payer, bh, mid,   cu_price, cu_m, alts, mtu);
    let c_st = build_min_bytes_fit_mtu_stable(payer, bh, right, cu_price, cu_r, alts, mtu);

    let lsa = wire_len(&a_st); let lsb = wire_len(&b_st); let lsc = wire_len(&c_st);
    let stable_ok = assert_quic_safe(&a_st) && assert_quic_safe(&b_st) && assert_quic_safe(&c_st);

    let sum_frontier = la + lb + lc;
    let sum_stable   = lsa + lsb + lsc;

    let (ta,tb,tc) = match (frontier_ok, stable_ok) {
        (true, false) => (a_leg, b_v0, c_v0),
        (false, true) => (a_st,  b_st, c_st),
        (true, true)  => if sum_frontier <= sum_stable { (a_leg,b_v0,c_v0) } else { (a_st,b_st,c_st) },
        (false,false) => if sum_frontier <= sum_stable { (a_leg,b_v0,c_v0) } else { (a_st,b_st,c_st) },
    };

    (ta,tb,tc)
}

pub enum SplitPlan {
    Pair(VersionedTransaction, VersionedTransaction),
    Triple(VersionedTransaction, VersionedTransaction, VersionedTransaction),
}

pub fn choose_split_best_2_or_3(
    payer: &Keypair,
    bh: Hash,
    instrs: Vec<Instruction>,
    cu_price: u64,
    cu_limits_3: (u32, u32, u32),
    cu_limits_2: (u32, u32),
    alts: &[AddressLookupTableAccount],
    prog_cu: &ProgCuEwma,
    mtu: &MtuLearner,
) -> SplitPlan {
    let (a3,b3,c3) = build_split3_cu_frontier(payer, bh, instrs.clone(), cu_price, cu_limits_3, alts, prog_cu, mtu);
    let bytes3 = wire_len(&a3) + wire_len(&b3) + wire_len(&c3);
    let ok3 = assert_quic_safe(&a3) && assert_quic_safe(&b3) && assert_quic_safe(&c3);

    let (a2,b2) = build_split_cu_aware(payer, bh, instrs, cu_price, cu_limits_2, alts, prog_cu, mtu);
    let bytes2 = wire_len(&a2) + wire_len(&b2);
    let ok2 = assert_quic_safe(&a2) && assert_quic_safe(&b2);

    match (ok3, ok2) {
        (true, false) => SplitPlan::Triple(a3,b3,c3),
        (false, true) => SplitPlan::Pair(a2,b2),
        (true, true)  => if bytes3 <= bytes2 { SplitPlan::Triple(a3,b3,c3) } else { SplitPlan::Pair(a2,b2) },
        (false,false) => if bytes3 <= bytes2 { SplitPlan::Triple(a3,b3,c3) } else { SplitPlan::Pair(a2,b2) },
    }
}

// ==============================
// PIECE 95 — Tick-Fit Ladder Planner (time-budgeted rungs)
// ==============================
pub struct LadderSchedule { pub offsets_ms: Vec<u64> }
pub fn plan_ladder_offsets(window_ms: u64, safety_ms: u64, emit_ms: u64, max_rungs: usize) -> LadderSchedule { let tick=50u64; let mut offsets=Vec::new(); let mut t=0u64; for _ in 0..max_rungs { if t + safety_ms + emit_ms > window_ms { break; } offsets.push(t); t += tick; } LadderSchedule { offsets_ms: offsets } }

// PIECE 96 removed: replace with SigRingTtl (123).

// ==============================
// PIECE 97 — Multi-Engine Hedge (Jito-compatible engines)
// ==============================
#[derive(Clone)] pub struct Engine { pub base: String, pub http: std::sync::Arc<reqwest::Client>, pub headers: reqwest::header::HeaderMap }
pub async fn hedge_engines_send_bundle(engines: Vec<Engine>, b64s:&[String]) -> anyhow::Result<(String, reqwest::header::HeaderMap, usize)> { use tokio::task::JoinSet; let mut set=JoinSet::new(); for (i, eng) in engines.into_iter().enumerate() { let body=b64s.to_vec(); set.spawn(async move { let res=eng.send_bundle_base64(&body).await; (i,res) }); } while let Some(res)=set.join_next().await { if let Ok((i,Ok((id,hdrs))))=res { return Ok((id,hdrs,i)); } } anyhow::bail!("all engines failed") }

// PIECE 98 removed: use subscribe_processed_topk (120) instead.

// ==============================
// PIECE 99 — Ladder Optimizer (EV-min cost for target P[clear])
// ==============================
pub fn fit_lognormal_from_p50_p95(m50: f64, p95: f64) -> (f64, f64) {
    let mu = m50.ln();
    let sigma = ((p95.ln() - mu) / 1.6448536269514722).clamp(0.10, 1.20);
    (mu, sigma)
}
#[inline] fn normal_cdf(z: f64) -> f64 { 0.5 * (1.0 + statrs::function::erf::erf(z / std::f64::consts::SQRT_2)) }
fn p_clear_le(x: f64, mu: f64, sigma: f64) -> f64 { let z = (x.ln() - mu) / sigma.max(1e-6); normal_cdf(z) }
pub fn optimize_ladder_2(base: f64, mu: f64, sigma: f64, target: f64) -> (f64, f64) {
    let mut best=(1.0,1.0); let mut best_cost=f64::INFINITY;
    for x2 in (110..=260).step_by(5) { let m2=base*(x2 as f64/100.0); let p2=p_clear_le(m2,mu,sigma); if p2<target { continue; }
        for x1 in (100..=x2).step_by(5) { let m1=base*(x1 as f64/100.0); let p1=p_clear_le(m1,mu,sigma); let e_cost = m1*p1 + (m1+m2)*(p2-p1); if e_cost<best_cost { best_cost=e_cost; best=(x1 as f64/100.0, x2 as f64/100.0); } }
    } best }
pub fn optimize_ladder_3(base: f64, mu: f64, sigma: f64, target: f64) -> (f64, f64, f64) {
    let mut best=(1.0,1.0,1.0); let mut best_cost=f64::INFINITY;
    for x3 in (120..=260).step_by(10) { let m3=base*(x3 as f64/100.0); let p3=p_clear_le(m3,mu,sigma); if p3<target { continue; }
        for x2 in (110..=x3).step_by(10) { let m2=base*(x2 as f64/100.0); let p2=p_clear_le(m2,mu,sigma);
            for x1 in (100..=x2).step_by(10) { let m1=base*(x1 as f64/100.0); let p1=p_clear_le(m1,mu,sigma);
                let e_cost = m1*p1 + (m1+m2)*(p2-p1) + (m1+m2+m3)*(p3-p2);
            }
{{ ... }}
    } best }

// ==============================
// PIECE P2 — P2 Online Quantile (median/MAD helper for RobustClock)
// ==============================
pub struct P2Quantile {
    p: f64,
    boot: Option<Vec<f64>>, // per-instance bootstrap buffer (no static mut)
    n: [f64;5],
    q: [f64;5],
    np: [f64;5],
    dn: [f64;5],
    initialized: bool,
}
impl P2Quantile {
    pub fn new(p: f64) -> Self {
        Self {
            p: p.clamp(0.0, 1.0),
            boot: Some(Vec::with_capacity(5)),
            n: [0.0;5], q: [0.0;5], np: [0.0;5], dn: [0.0;5], initialized: false
        }
    }
    pub fn observe(&mut self, x: f64) {
        if !self.initialized {
            let b = self.boot.as_mut().unwrap();
            b.push(x);
            if b.len() == 5 {
                b.sort_by(|a,b| a.partial_cmp(b).unwrap());
                self.q.copy_from_slice(&[b[0], b[1], b[2], b[3], b[4]]);
                self.n  = [1.0, 2.0, 3.0, 4.0, 5.0];
                self.np = [1.0, 1.0 + 2.0*self.p, 1.0 + 4.0*self.p, 3.0 + 2.0*self.p, 5.0];
                self.dn = [0.0, self.p/2.0, self.p, (1.0+self.p)/2.0, 1.0];
                self.initialized = true;
                self.boot = None;
            }
            return;
        }
        let k = if x < self.q[0] { self.q[0] = x; 0 }
                else if x < self.q[1] { 0 }
                else if x < self.q[2] { 1 }
                else if x < self.q[3] { 2 }
                else if x <= self.q[4] { 3 }
                else { self.q[4] = x; 3 };
        for i in (k+1)..5 { self.n[i] += 1.0; }
        for i in 0..5 { self.np[i] += self.dn[i]; }
        for i in 1..4 {
            let d = self.np[i] - self.n[i];
            if (d >= 1.0 && (self.n[i+1]-self.n[i]) > 1.0) || (d <= -1.0 && (self.n[i-1]-self.n[i]) < -1.0) {
                let dsign = d.signum();
                let qi = self.q[i]; let qip = self.q[i+1]; let qim = self.q[i-1];
                let num = (dsign*(self.n[i]-self.n[i-1]+dsign)*(qip-qi)/(self.n[i+1]-self.n[i]))
                        + (dsign*(self.n[i+1]-self.n[i]-dsign)*(qi-qim)/(self.n[i]-self.n[i-1]));
                let den = (self.n[i+1]-self.n[i-1]).max(1.0);
                let qp = qi + num/den;
                self.q[i] = qp.clamp(qim + 1e-12, qip - 1e-12);
                self.n[i] += dsign;
            }
        }
    }
    pub fn quantile(&self) -> Option<f64> { if !self.initialized { None } else { Some(self.q[2]) } }
}

// ==============================
// PIECE 101 — Time-Decayed CMS + TDigest (production-ready)
// ==============================
#[derive(Clone, Copy, Debug)]
struct Centroid { mean: f64, weight: f64 }

pub struct TDigest {
    comp: f64,                // compression factor (typ. 100–500)
    cents: Vec<Centroid>,     // always kept sorted by mean
    total_w: f64,
}
impl TDigest {
    pub fn new(comp: f64) -> Self {
        let c = comp.clamp(50.0, 1000.0);
        Self { comp: c, cents: Vec::with_capacity(c as usize * 2), total_w: 0.0 }
    }
    #[inline]
    pub fn len(&self) -> usize { self.cents.len() }

    pub fn add(&mut self, x: f64) {
        if self.cents.is_empty() {
            self.cents.push(Centroid { mean: x, weight: 1.0 });
            self.total_w = 1.0;
            return;
        }
        // Binary search for insertion point
        let mut lo = 0usize; let mut hi = self.cents.len();
        while lo < hi { let mid = (lo + hi) / 2; if self.cents[mid].mean < x { lo = mid + 1; } else { hi = mid; } }
        // Merge with nearest centroid (left or right)
        let pick = if lo == 0 { 0 } else if lo == self.cents.len() { self.cents.len() - 1 } else {
            let dl = (x - self.cents[lo - 1].mean).abs(); let dr = (self.cents[lo].mean - x).abs(); if dl <= dr { lo - 1 } else { lo }
        };
        let c = &mut self.cents[pick];
        let new_w = c.weight + 1.0; c.mean = (c.mean * c.weight + x) / new_w; c.weight = new_w; self.total_w += 1.0;
        if self.cents.len() > (self.comp as usize) * 4 { self.compress(); }
    }

    pub fn quantile(&self, q: f64) -> f64 {
        if self.cents.is_empty() { return 0.0; }
        let q = q.clamp(0.0, 1.0); let target = q * self.total_w.max(1e-9);
        let mut acc = 0.0;
        for c in &self.cents {
            let prev = acc; acc += c.weight; if target <= acc {
                let _frac = ((target - prev) / c.weight).clamp(0.0, 1.0);
                return c.mean;
            }
        }
        self.cents.last().unwrap().mean
    }

    fn compress(&mut self) {
        if self.cents.len() < 2 { return; }
        self.cents.sort_by(|a,b| a.mean.partial_cmp(&b.mean).unwrap());
        let mut merged: Vec<Centroid> = Vec::with_capacity(self.cents.len());
        let mut cur = self.cents[0]; let mut cum_w = 0.0;
        for c in self.cents.iter().skip(1) {
            let q = if self.total_w > 0.0 { (cum_w + cur.weight * 0.5) / self.total_w } else { 0.5 };
            let kcap = (4.0 * self.total_w * q * (1.0 - q) / self.comp).max(1.0);
            if cur.weight + c.weight <= kcap { let w = cur.weight + c.weight; cur.mean = (cur.mean * cur.weight + c.mean * c.weight) / w; cur.weight = w; }
            else { merged.push(cur); cur = *c; }
            cum_w += c.weight;
        }
        merged.push(cur); self.cents = merged;
    }
}

// ---- Time-decayed Count-Min (u16 saturated, cache-friendly) ----
pub struct CmsDecay {
    w: usize,
    d: usize,
    table: Vec<u16>,
    salts: [u64;4],
    last_decay: std::time::Instant,
    halflife: std::time::Duration,
}
impl CmsDecay {
    pub fn new(width_pow2: usize, halflife: std::time::Duration) -> Self {
        let w = width_pow2.next_power_of_two().max(256);
        Self { w, d: 4, table: vec![0u16; w*4], salts: [0x9E37,0xC2B2,0xBF58,0x94D0], last_decay: std::time::Instant::now(), halflife }
    }
    #[inline] fn idx(&self,row:usize,h:u64)->usize{ (row*self.w) + ((h as usize) & (self.w-1)) }
    #[inline] fn h(&self,key:&solana_sdk::pubkey::Pubkey,row:usize)->u64{ let mut x=self.salts[row]; for b in key.as_ref(){ x = x.rotate_left(13) ^ (*b as u64); x = x.wrapping_mul(0x9E3779B185EBCA87);} x }
    fn maybe_decay(&mut self){ let dt=self.last_decay.elapsed(); if dt < self.halflife/8 { return; } let f=(0.5f64).powf(dt.as_secs_f64()/self.halflife.as_secs_f64()) as f32; for v in self.table.iter_mut(){ let nv = ((*v as f32) * f).round() as u16; *v = nv; } self.last_decay=std::time::Instant::now(); }
    pub fn add(&mut self,k:&solana_sdk::pubkey::Pubkey){ self.maybe_decay(); for r in 0..self.d { let i=self.idx(r,self.h(k,r)); if let Some(c)=self.table.get_mut(i){ *c = c.saturating_add(1); } } }
    pub fn estimate(&mut self,k:&solana_sdk::pubkey::Pubkey)->u32{ self.maybe_decay(); let mut m=u16::MAX; for r in 0..self.d { let i=self.idx(r,self.h(k,r)); m=m.min(self.table[i]); } m as u32 }
}

// ==============================
// PIECE 102 — TPU Pacer v2 (TSC-based with safe fallback)
// ==============================
{{ ... }}
{{ ... }}
pub struct TpuPacer {
    min_gap_ns: u64,
    last_tsc: core::sync::atomic::AtomicU64,
    cycles_per_ns_x1e3: u64, // (cycles/ns) * 1000
}
impl TpuPacer {
    pub fn new(min_gap_micros: u64) -> Self {
        let min_gap_ns = min_gap_micros.saturating_mul(1000);
        let cycles_per_ns = calibrate_cycles_per_ns().unwrap_or(3.0);
        Self {
            min_gap_ns,
            last_tsc: core::sync::atomic::AtomicU64::new(read_tsc()),
            cycles_per_ns_x1e3: (cycles_per_ns * 1000.0) as u64,
        }
    }
    #[inline] pub fn wait(&self) {
        use std::hint::spin_loop;
        let prev = self.last_tsc.load(core::sync::atomic::Ordering::Relaxed);
        let now = read_tsc();
        let target = prev.saturating_add(self.min_gap_ns.saturating_mul(self.cycles_per_ns_x1e3) / 1000);
        if now >= target {
            self.last_tsc.store(now, core::sync::atomic::Ordering::Relaxed);
            return;
        }
        while read_tsc() < target { spin_loop(); }
        self.last_tsc.store(read_tsc(), core::sync::atomic::Ordering::Relaxed);
    }
}
#[inline]
fn read_tsc() -> u64 {
    #[cfg(all(target_arch = "x86_64", not(miri)))]
    unsafe { core::arch::x86_64::_rdtsc() }
    #[cfg(not(all(target_arch = "x86_64", not(miri))))]
    {
        let start = once_cell::sync::Lazy::new(std::time::Instant::now);
        start.elapsed().as_nanos() as u64
    }
}
fn calibrate_cycles_per_ns() -> Option<f64> {
    #[cfg(all(target_arch = "x86_64", not(miri)))]
    unsafe {
        use std::time::{Duration, Instant};
        let t0 = Instant::now(); let c0 = core::arch::x86_64::_rdtsc();
        std::thread::sleep(Duration::from_millis(20));
        let dt = t0.elapsed(); let c1 = core::arch::x86_64::_rdtsc();
        let cycles = (c1 - c0) as f64; let ns = dt.as_nanos() as f64;
        if ns > 0.0 { return Some(cycles / ns); }
        None
    }
    #[cfg(not(all(target_arch = "x86_64", not(miri))))]
    { Some(1.0) }
}

// ==============================
// PIECE 103 — Rung-Distinct Tip Receivers (salted consistent hashing)
// ==============================
pub fn tip_receiver_for_rung(payload_sig: &solana_sdk::signature::Signature, tips:&[Pubkey], rung_idx:u8) -> Pubkey { assert!(!tips.is_empty()); let mut h=sha2::Sha256::new(); use sha2::Digest; h.update(payload_sig.as_ref()); h.update(&[rung_idx]); let d=h.finalize(); let idx=(u64::from_le_bytes([d[0],d[1],d[2],d[3],d[4],d[5],d[6],d[7]]) as usize)%tips.len(); tips[idx] }

// ==============================
// PIECE 104 — Preflight-Free Sanity Gate (size, payer, budget, venue)
// ==============================
pub struct PreflightCtx { pub min_balance: u64, pub max_wire_len: usize, pub min_cu_price: u64, pub max_cu_limit: u32 }
pub enum GateReason { Ok, Oversize, LowBalance, CuPriceTooLow, CuLimitTooHigh }
pub fn gate_tx(vtx:&VersionedTransaction, payer_balance:u64, cu_price:u64, cu_limit:u32, ctx:&PreflightCtx) -> GateReason { if !assert_quic_safe(vtx) || wire_len(vtx) > ctx.max_wire_len { return GateReason::Oversize; } if payer_balance < ctx.min_balance { return GateReason::LowBalance; } if cu_price < ctx.min_cu_price { return GateReason::CuPriceTooLow; } if cu_limit > ctx.max_cu_limit { return GateReason::CuLimitTooHigh; } GateReason::Ok }

// ==============================
// PIECE 105 — MtuLearner v2.hys (streak-aware, hysteresis)
// ==============================
pub struct MtuLearner {
    pub safe: usize,
    hard_min: usize,
    hard_max: usize,
    down_step: usize,
    up_step: usize,
    succ_streak: u16,
    fail_streak: u16,
}
impl MtuLearner {
    pub fn new(initial: usize) -> Self {
        Self {
            safe: initial.clamp(800, 1300),
            hard_min: 800, hard_max: 1300,
            down_step: 48, up_step: 16,
            succ_streak: 0, fail_streak: 0,
        }
    }
    #[inline] pub fn quic_safe(&self, wire_len: usize) -> bool { wire_len <= self.safe }
    pub fn observe_success(&mut self, wire_len: usize) {
        if wire_len + 8 >= self.safe {
            self.succ_streak = self.succ_streak.saturating_add(1);
            self.fail_streak = 0;
            if self.succ_streak >= 3 && self.safe + self.up_step <= self.hard_max {
                self.safe += self.up_step;
                self.succ_streak = 0;
            }
        } else {
            self.succ_streak = 0;
        }
    }
    pub fn observe_drop(&mut self, wire_len: usize) {
        self.fail_streak = self.fail_streak.saturating_add(1);
        self.succ_streak = 0;
        if (wire_len + 8 >= self.safe) && self.safe >= self.hard_min + self.down_step {
            let step = if self.fail_streak >= 3 { self.down_step * 2 } else { self.down_step };
            self.safe = self.safe.saturating_sub(step).max(self.hard_min);
        }
    }
}

// ==============================
// PIECE 106 — Memory Lock & Prefault (eliminate first-touch stalls)
// ==============================
#[cfg(target_os = "linux")]
pub fn mlock_and_prefault(buffers: &mut [Vec<u8>]) {
    unsafe { let _ = libc::mlockall(libc::MCL_CURRENT | libc::MCL_FUTURE); }
    for b in buffers.iter_mut() { let step=4096; if b.len()<step { b.resize(step,0); } let len=b.capacity().max(b.len()); unsafe { b.set_len(len); } for i in (0..len).step_by(step) { b[i] = b[i].wrapping_add(1); } }
    for b in buffers.iter_mut() { b.clear(); }
}
#[cfg(not(target_os = "linux"))]
pub fn mlock_and_prefault(_buffers: &mut [Vec<u8>]) { }

// ==============================
// PIECE 111 — ALT Index-Map + MTU-Constrained Shrinker (exact bytes)
// ==============================
fn build_lut_index(all: &[AddressLookupTableAccount]) -> std::collections::HashMap<Pubkey, Vec<usize>> {
    let mut map=std::collections::HashMap::with_capacity(1024);
    for (i,lut) in all.iter().enumerate(){ for k in &lut.addresses { map.entry(*k).or_default().push(i); } }
    map
}
fn required_pubkeys_for_instrs(instrs:&[Instruction], payer:&Pubkey)->std::collections::HashSet<Pubkey>{ let mut s=std::collections::HashSet::with_capacity(64); s.insert(*payer); for ix in instrs { for a in &ix.accounts { s.insert(a.pubkey); } s.insert(ix.program_id); } s }
pub fn build_min_bytes_fit_mtu(
    payer: &Keypair,
    bh: Hash,
    instrs: Vec<Instruction>,
    cu_price: u64,
    cu_limit: u32,
    all_luts: &[AddressLookupTableAccount],
    mtu: &MtuLearner,
) -> VersionedTransaction {
    // Redirect legacy fit to stable variant
    build_min_bytes_fit_mtu_stable(payer, bh, instrs, cu_price, cu_limit, all_luts, mtu)
}

// PIECE 107 removed: using simpler MTU-constrained logic and assemble_ultra (118).

// ==============================
// PIECE 108 — Max-Independent-Set Scheduler (conflict-free multi-send)
// ==============================
#[derive(Clone)]
pub struct TxMeta { pub rw: std::collections::HashSet<Pubkey>, pub ro: std::collections::HashSet<Pubkey> }
impl TxMeta { #[inline] fn conflicts(&self, other:&TxMeta) -> bool { if self.rw.iter().any(|k| other.rw.contains(k) || other.ro.contains(k)) { return true; } if other.rw.iter().any(|k| self.rw.contains(k) || self.ro.contains(k)) { return true; } false } }
pub fn select_independent_set(metas:&[TxMeta]) -> Vec<usize> {
    let n=metas.len(); if n==0 { return vec![]; }
    let mut deg:Vec<usize>=vec![0;n]; for i in 0..n { for j in (i+1)..n { if metas[i].conflicts(&metas[j]) { deg[i]+=1; deg[j]+=1; } } }
    let mut idx:Vec<usize>=(0..n).collect(); idx.sort_by(|&i,&j| deg[i].cmp(&deg[j]).then_with(|| metas[i].rw.len().cmp(&metas[j].rw.len())));
    let mut chosen:Vec<usize>=Vec::new(); for i in idx { if chosen.iter().all(|&k| !metas[i].conflicts(&metas[k])) { chosen.push(i); } }
    chosen
}

// ==============================
// PIECE 109 — Hazard-Shaped Rung Offsets (time-optimal within window)
// ==============================
#[inline] fn norm_cdf(z:f64)->f64{ 0.5*(1.0 + statrs::function::erf::erf(z/std::f64::consts::SQRT_2)) }
fn hazard_logn(x:f64, mu:f64, sigma:f64)->f64{ let z=(x.ln()-mu)/sigma.max(1e-9); let f=((-(z*z)/2.0).exp())/(x*sigma*(2.0*std::f64::consts::PI).sqrt()); let s=1.0 - norm_cdf(z); (f / s.max(1e-12)).min(1e6) }
pub fn plan_offsets_hazard(window_ms:u64, max_rungs:usize, mu:f64, sigma:f64)->Vec<u64>{ let mut out=Vec::new(); if max_rungs==0 { return out; } let tick=50.0; let steps=(window_ms as f64 / tick).ceil() as usize; let mut hz:Vec<(u64,f64)>=Vec::new(); for i in 0..steps { let x = 1.0 + 0.3*(i as f64); hz.push(((i as u64)*50, hazard_logn(x,mu,sigma))); } hz.sort_by(|a,b| b.1.partial_cmp(&a.1).unwrap().then_with(|| a.0.cmp(&b.0))); for (t,_) in hz.into_iter().take(max_rungs) { if t<window_ms { out.push(t); } } out.sort_unstable(); out }

// ==============================
// PIECE 110 — Endpoint UCB Selector (correlation-aware top-K ordering)
// ==============================
#[derive(Clone)] pub struct EpUcb { pulls:u64, succ_ewma:f64, lat_ms_ewma:f64 }
impl Default for EpUcb { fn default()->Self{ Self{ pulls:1, succ_ewma:0.9, lat_ms_ewma:120.0 } } }
pub struct UcbSelector { alpha:f64, explore_c:f64, lat_penalty:f64, total_pulls:u64, eps:Vec<EpUcb> }
impl UcbSelector { pub fn new(n_endpoints:usize, explore_c:f64, lat_penalty:f64)->Self{ Self{ alpha:0.2, explore_c, lat_penalty, total_pulls:n_endpoints as u64, eps:vec![EpUcb::default(); n_endpoints] } }
    pub fn observe(&mut self, idx:usize, ok:bool, lat_ms:f64){ let a=self.alpha; if let Some(e)=self.eps.get_mut(idx){ e.succ_ewma=(1.0-a)*e.succ_ewma + a*if ok{1.0}else{0.0}; e.lat_ms_ewma=(1.0-a)*e.lat_ms_ewma + a*lat_ms.max(1.0); e.pulls+=1; self.total_pulls+=1; } }
    fn score(&self, idx:usize)->f64{ let e=&self.eps[idx]; let ucb=self.explore_c * ((self.total_pulls as f64).ln() / (e.pulls as f64)).sqrt(); e.succ_ewma + ucb - self.lat_penalty * (e.lat_ms_ewma/200.0) }
    pub fn order(&self)->Vec<usize>{ let mut idx:Vec<usize>=(0..self.eps.len()).collect(); idx.sort_by(|&i,&j| self.score(j).partial_cmp(&self.score(i)).unwrap()); idx }
}

// ==============================
// PIECE 56 — TPU Health Gate + Backoff (slot-phase aware)
// ==============================
#[derive(Clone)]
pub struct TpuHealthGate {
    alpha: f64,
    succ: f64,
    lat_ms: f64,
    banned_until: Option<std::time::Instant>,
    base_backoff: std::time::Duration,
}
impl TpuHealthGate {
    pub fn new() -> Self { Self { alpha: 0.2, succ: 0.9, lat_ms: 80.0, banned_until: None, base_backoff: std::time::Duration::from_millis(300) } }
    pub fn record(&mut self, ok: bool, latency_ms: f64) {
        let a = self.alpha;
        self.succ  = (1.0 - a) * self.succ  + a * (if ok { 1.0 } else { 0.0 });
        self.lat_ms= (1.0 - a) * self.lat_ms+ a * latency_ms.max(1.0);
        if self.succ < 0.3 || self.lat_ms > 300.0 {
            let mult = if self.succ < 0.15 { 8 } else { 2 };
            let dur = self.base_backoff * mult;
            self.banned_until = Some(std::time::Instant::now() + dur);
            self.succ = 0.5;
            self.lat_ms = 120.0;
        }
    }
    pub fn allow_now(&self, slot_phase_0_1: f64) -> bool {
        if let Some(t) = self.banned_until { if std::time::Instant::now() < t { return false; } }
        if slot_phase_0_1 >= 0.75 { return self.succ > 0.6 && self.lat_ms < 120.0; }
        true
    }
}

// ==============================
// PIECE 57 — Dual-Path First-Ack (TPU ⟂ Hedged RPC)
// ==============================
pub async fn send_first_ack(
    tpu: std::sync::Arc<TpuFastPath>,
    rpc: std::sync::Arc<HedgedRpc>,
    vtx: VersionedTransaction,
    tpu_budget_ms: u64,
) -> Result<solana_sdk::signature::Signature> {
    let vtx_rpc = vtx.clone();
    tokio::select! {
        res = async {
            let t0=std::time::Instant::now();
            match tpu.send_versioned(vtx).await {
                Ok(sig) => Ok(sig),
                Err(e) => Err(CounterStrategyError::RpcError(format!("tpu failed in {}ms: {e:?}", t0.elapsed().as_millis()))),
            }
        } => res,
        res = async {
            tokio::time::sleep(std::time::Duration::from_millis(tpu_budget_ms.min(10))).await;
            rpc.send_versioned_tx(vtx_rpc).await
        } => res,
    }
}

// ==============================
// PIECE 58 — TPU-Aware Resubmitter (fresh hash + immediate re-TPU)
// ==============================
pub async fn send_with_tpu_resubmit(
    tpu: std::sync::Arc<TpuFastPath>,
    rpc: std::sync::Arc<HedgedRpc>,
    build: impl Fn(Hash) -> VersionedTransaction + Send + Sync + 'static,
    max_retries: u8,
) -> Result<solana_sdk::signature::Signature> {
    let mut attempts: u8 = 0;
    loop {
        let bh = rpc.get_latest_blockhash().await?;
        let vtx = build(bh);
        if let Ok(sig) = tpu.send_versioned(vtx.clone()).await { return Ok(sig); }
        match rpc.send_versioned_tx(vtx.clone()).await {
            Ok(sig) => return Ok(sig),
            Err(e) => {
                let esc = classify_and_escalate(&format!("{e:?}"));
                if esc.refetch_blockhash && attempts < max_retries { attempts = attempts.saturating_add(1); continue; }
                return Err(CounterStrategyError::RpcError(format!("tpu_resubmit exhausted: {e}")));
            }
        }
    }
}

// ==============================
// PIECE 59 — Live Ladder Abort (watch channel bus)
// ==============================
pub struct LadderAbort { tx: tokio::sync::watch::Sender<bool>, pub rx: tokio::sync::watch::Receiver<bool> }
impl LadderAbort { pub fn new() -> Self { let (tx, rx) = tokio::sync::watch::channel(false); Self { tx, rx } } pub fn abort(&self) { let _ = self.tx.send(true); } pub fn subscriber(&self) -> tokio::sync::watch::Receiver<bool> { self.rx.clone() } }
pub async fn send_ladder_with_abort(
    jito: std::sync::Arc<JitoClient>,
    clock: &ClusterClock,
    payload_vtx: VersionedTransaction,
    tip_builder: impl Fn(u64)->VersionedTransaction + Clone + Send + 'static,
    rung_tips: Vec<u64>,
    safety_ms: u64,
    abort_rx: tokio::sync::watch::Receiver<bool>,
) -> Result<Vec<String>> {
    let mut out = Vec::with_capacity(rung_tips.len());
    let b64_payload = vtx_to_base64(&payload_vtx);
    let mut rx = abort_rx.clone();
    for (i, tip) in rung_tips.into_iter().enumerate() {
        if *rx.borrow() { break; }
        let tip_vtx = tip_builder(tip);
        let b64_tip = vtx_to_base64(&tip_vtx);
        let now_ms = {
            let d = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default(); d.as_millis() as u64
        } as u64;
        let dly = delay_to_next_tick_ms(now_ms + (i as u64)*50, safety_ms);
        clock.corrected_sleep_ms(dly as i64).await;
        if let Ok(id) = jito.send_bundle_base64(&[b64_payload.clone(), b64_tip]).await { out.push(id); }
    }
    Ok(out)
}

// ==============================
// PIECE 60 — Canonical Budget & Meta Ordering (signature-stable rebuilds)
// ==============================
pub fn canonicalize_instructions(mut instrs: Vec<Instruction>) -> Vec<Instruction> {
    instrs.sort_by(|a,b| {
        let ca = is_budget(a) as u8; let cb = is_budget(b) as u8;
        match cb.cmp(&ca) { std::cmp::Ordering::Equal => a.program_id.to_bytes().cmp(&b.program_id.to_bytes()), ord => ord }
    });
    for ix in instrs.iter_mut() {
        ix.accounts.sort_by(|x,y| match (x.is_writable, y.is_writable) {
            (true,false) => std::cmp::Ordering::Less,
            (false,true) => std::cmp::Ordering::Greater,
            _ => x.pubkey.to_bytes().cmp(&y.pubkey.to_bytes()),
        });
    }
    instrs.sort_by(|a,b| match (is_limit(a), is_limit(b), is_price(a), is_price(b)) {
        (true,false,_,_) => std::cmp::Ordering::Less,
        (false,true,_,_) => std::cmp::Ordering::Greater,
        (_,_,true,false) => std::cmp::Ordering::Less,
        (_,_,false,true) => std::cmp::Ordering::Greater,
        _ => std::cmp::Ordering::Equal,
    });
    instrs
}
#[inline] fn is_budget(ix: &Instruction) -> bool { ix.program_id == solana_sdk::compute_budget::id() }
#[inline] fn is_limit(ix: &Instruction) -> bool { is_budget(ix) && matches!(ix.data.first(), Some(&0)) }
#[inline] fn is_price(ix: &Instruction) -> bool { is_budget(ix) && matches!(ix.data.first(), Some(&1)) }

// ==============================
// PIECE 125 — Normalize Budget (prepend limit then price; remove duplicates)
// ==============================
pub fn normalize_budget(mut instrs: Vec<Instruction>, cu_limit: u32, cu_price: u64) -> Vec<Instruction> {
    // strip any existing compute budget ixs
    instrs.retain(|ix| ix.program_id != solana_sdk::compute_budget::id());
    // prepend canonical budget
    use solana_sdk::compute_budget::ComputeBudgetInstruction as CBI;
    let mut out = Vec::with_capacity(instrs.len() + 2);
    out.push(CBI::set_compute_unit_limit(cu_limit).into());
    out.push(CBI::set_compute_unit_price(cu_price).into());
    out.extend(instrs.into_iter());
    out
}

// ==============================
// PIECE 61 — Joint Venue Cost Optimizer (tip ⟂ cu_price)
// ==============================
#[derive(Clone, Copy)]
pub enum Venue { Bundle, Raw }

pub struct VenueInputs {
    pub notional_lamports: u64,
    pub congestion_0_1: f64,
    pub leader_score_z: f64,
    pub conflict_risk_0_1: f64,
    pub cu_limit: u32,
    pub p50: u64, pub p75: u64, pub p95: u64,
}
pub struct VenuePlan { pub venue: Venue, pub tip_lamports: u64, pub cu_price: u64, pub total_cost_lamports: u64, pub target_prob: f64 }
fn ln1p64(x: u64) -> f64 { (x as f64).ln_1p() }
fn search_action<F: Fn(f64)->f64>(mut lo: f64, mut hi: f64, target: f64, predict: F) -> f64 { for _ in 0..22 { let mid = 0.5*(lo+hi); let p = predict(mid); if p < target { lo = mid; } else { hi = mid; } } hi }
pub fn optimize_bundle(model_bundle: &InclusionModel, tip_eff: &TipEffModel, inp: &VenueInputs, target_prob: f64) -> VenuePlan {
    let eff_mid = ((tip_eff.ema[1] + tip_eff.ema[2]) * 0.5).clamp(tip_eff.min_eff, tip_eff.max_eff);
    let cu = inp.cu_limit.max(1) as f64; let nv = ln1p64(inp.notional_lamports);
    let predict = |log_tip_rel: f64| { let feats = [ nv, inp.congestion_0_1, inp.leader_score_z, inp.conflict_risk_0_1, log_tip_rel ]; model_bundle.predict(&feats) };
    let lo = (0.5 * eff_mid).ln(); let hi = (3.0 * eff_mid).ln(); let log_rel = search_action(lo, hi, target_prob, predict);
    let tip_per_cu = (eff_mid * log_rel.exp()).max(tip_eff.min_eff); let tip = (tip_per_cu * cu).ceil() as u64;
    VenuePlan { venue: Venue::Bundle, tip_lamports: tip, cu_price: 0, total_cost_lamports: tip, target_prob }
}
pub fn optimize_raw(model_raw: &InclusionModel, inp: &VenueInputs, target_prob: f64) -> VenuePlan {
    let nv = ln1p64(inp.notional_lamports); let p75 = inp.p75.max(1) as f64;
    let predict = |log_price_rel: f64| { let feats = [ nv, inp.congestion_0_1, inp.leader_score_z, inp.conflict_risk_0_1, log_price_rel ]; model_raw.predict(&feats) };
    let lo = (0.5f64).ln(); let hi = (2.5f64).ln(); let log_rel = search_action(lo, hi, target_prob, predict);
    let cu_price = (p75 * log_rel.exp()).round() as u64; let cost = (cu_price as u128) * (inp.cu_limit as u128);
    VenuePlan { venue: Venue::Raw, tip_lamports: 0, cu_price, total_cost_lamports: cost as u64, target_prob }
}

// PIECE 62 removed: use WalletPoolJump (118) with JumpSalt (128) instead.

// ==============================
// PIECE 63 — Blockhash TTL Guard (last_valid_block_height)
// ==============================
#[derive(Clone)] pub struct HashTtl { pub hash: Hash, pub last_valid_block_height: u64, pub fetched_at: std::time::Instant }
#[derive(Clone)] pub struct BlockhashTtlCache { inner: std::sync::Arc<tokio::sync::RwLock<HashTtl>>, rpc: std::sync::Arc<solana_client::nonblocking::rpc_client::RpcClient>, refresh_blocks_margin: u64 }
impl BlockhashTtlCache {
    pub async fn new(rpc: std::sync::Arc<solana_client::nonblocking::rpc_client::RpcClient>, margin: u64) -> Result<Self> {
        let resp = rpc.get_latest_blockhash_with_commitment(solana_sdk::commitment_config::CommitmentConfig::processed()).await
            .map_err(|e| CounterStrategyError::RpcError(format!("bh_ttl init: {e}")))?;
        let v = resp.value.ok_or_else(|| CounterStrategyError::RpcError("no blockhash".into()))?;
        Ok(Self { inner: std::sync::Arc::new(tokio::sync::RwLock::new(HashTtl { hash: v.blockhash, last_valid_block_height: v.last_valid_block_height, fetched_at: std::time::Instant::now() })), rpc, refresh_blocks_margin: margin.max(20) })
    }
    pub async fn get(&self) -> HashTtl { self.inner.read().await.clone() }
    pub async fn refresh_if_needed(&self) -> Result<()> {
        let cur_height = self.rpc.get_block_height().await.map_err(|e| CounterStrategyError::RpcError(format!("get_block_height: {e}")))?;
        let cur = self.inner.read().await.clone();
        if cur.last_valid_block_height.saturating_sub(cur_height) <= self.refresh_blocks_margin {
            drop(cur);
            let resp = self.rpc.get_latest_blockhash_with_commitment(solana_sdk::commitment_config::CommitmentConfig::processed()).await
                .map_err(|e| CounterStrategyError::RpcError(format!("bh_ttl refresh: {e}")))?;
            if let Some(v) = resp.value { *self.inner.write().await = HashTtl { hash: v.blockhash, last_valid_block_height: v.last_valid_block_height, fetched_at: std::time::Instant::now() }; }
        }
        Ok(())
    }
    pub async fn start(self) { tokio::spawn(async move { loop { let _ = self.refresh_if_needed().await; tokio::time::sleep(std::time::Duration::from_millis(150)).await; } }); }
}

// ==============================
// PIECE 64 — Hedged Tasks with Early Abort
// ==============================
pub struct AbortableTask<T> { handle: tokio::task::JoinHandle<Option<T>>, cancel: futures::future::AbortHandle }
impl<T: Send + 'static> AbortableTask<T> {
    pub fn spawn<F>(fut: F) -> Self where F: std::future::Future<Output = T> + Send + 'static {
        let (abort, reg) = futures::future::AbortHandle::new_pair();
        let handle = tokio::spawn(async move { let res = futures::future::Abortable::new(fut, reg).await; match res { Ok(v) => Some(v), Err(_) => None } });
        Self { handle, cancel: abort }
    }
    pub fn abort(&self) { self.cancel.abort(); }
    pub async fn join(self) -> Option<T> { self.handle.await.ok().flatten() }
}

// ==============================
// PIECE 65 — Count-Min Sketch hotness weighting
// ==============================
pub struct CountMin { w: usize, d: usize, table: Vec<u32>, salts: [u64; 4] }
impl CountMin {
    pub fn new(width_pow2: usize) -> Self { let w = width_pow2.next_power_of_two(); Self { w, d: 4, table: vec![0u32; w*4], salts: [0x9E37,0xC2B2,0xBF58,0x94D0] } }
    #[inline] fn idx(&self, row: usize, h: u64) -> usize { (row*self.w) + ((h as usize) & (self.w-1)) }
    #[inline] fn h(&self, key: &Pubkey, row: usize) -> u64 { let mut x = self.salts[row]; for b in key.as_ref() { x = x.rotate_left(13) ^ (*b as u64); x = x.wrapping_mul(0x9E3779B185EBCA87); } x }
    pub fn add(&mut self, k: &Pubkey) { for r in 0..self.d { let i = self.idx(r, self.h(k, r)); if let Some(c) = self.table.get_mut(i) { *c = c.saturating_add(1); } } }
    pub fn estimate(&self, k: &Pubkey) -> u32 { let mut m = u32::MAX; for r in 0..self.d { let i = self.idx(r, self.h(k, r)); m = m.min(self.table[i]); } m }
}
pub fn weighted_conflict_score(cms: &CountMin, accounts_ro: &[Pubkey], accounts_rw: &[Pubkey]) -> f64 {
    let mut s = 0.0; for a in accounts_rw { s += 1.0 + (cms.estimate(a) as f64).sqrt(); } for a in accounts_ro { s += 0.35 * (1.0 + (cms.estimate(a) as f64).sqrt()); }
    let denom = (accounts_rw.len() as f64) + 0.35 * (accounts_ro.len() as f64); if denom <= 0.0 { 0.0 } else { (s / denom).min(20.0) / 20.0 }
}

// PIECE 66 removed: migrate callers to assemble_ultra (118) and build_min_bytes_fit_mtu_stable (121).

// ==============================
// PIECE 25 — EV-Gated Opportunity Selector
// ==============================
#[derive(Clone)]
pub struct Opportunity<'a> {
    pub notional_lamports: u64,
    pub expected_edge_bps: f64,
    pub est_slippage_bps: f64,
    pub instrs: &'a [Instruction],
    pub leader_candidates: &'a [Pubkey],
    pub risk_tolerance_prob: f64,
    pub generous_cu_limit: u32,
}

#[derive(Clone, Copy)]
pub struct Decision { pub accept: bool, pub target_prob: f64, pub cu_price: u64, pub cu_limit_hint: u32 }

pub fn score_and_decide(
    opp: &Opportunity,
    model: &InclusionModel,
    p50: u64, p75: u64, p95: u64,
    bloom: &BloomFilterCache,
    net: &NetworkConditions,
    scorecard: &ValidatorScorecard,
) -> Decision {
    let (ro, rw) = split_rw_sets(opp.instrs);
    let conflict = estimate_conflict_risk(bloom, &ro, &rw).clamp(0.0, 1.0);
    let mut lscores = opp.leader_candidates.iter().map(|v| scorecard.raw_score(*v).unwrap_or(0.0)).collect::<Vec<_>>();
    lscores.sort_by(|a,b| b.partial_cmp(a).unwrap());
    let leader_score = if lscores.is_empty() { 0.0 } else { lscores.iter().take(lscores.len().min(3)).sum::<f64>() / (lscores.len().min(3) as f64) };
    let target_prob = opp.risk_tolerance_prob.clamp(0.5, 0.99);
    let cu_price = price_for_target(p50, p75, p95, opp.notional_lamports, net.congestion_level, leader_score, conflict, model, target_prob, 2_500_000);

    let gross = (opp.notional_lamports as f64) * (opp.expected_edge_bps - opp.est_slippage_bps) / 10_000.0;
    let exp_fee = cu_price as f64 * (opp.generous_cu_limit as f64) * 0.8_f64;
    let p_incl = model.predict(&[
        (opp.notional_lamports as f64).ln_1p(),
        net.congestion_level,
        leader_score,
        conflict,
        ((cu_price.max(1) as f64) / (p75.max(1) as f64)).ln(),
    ]);
    let ev = p_incl * (gross - exp_fee);
    let accept = ev > 0.0 && gross > exp_fee * 1.05;
    Decision { accept, target_prob, cu_price, cu_limit_hint: opp.generous_cu_limit }
}

// ==============================
// PIECE 26 — Endpoint Backpressure & Rate Budget
// ==============================
use tokio::sync::{Semaphore, OwnedSemaphorePermit};
pub struct EndpointLimiter { sem: Semaphore, last_ok: Instant, burst: u32, refill: Duration }
impl EndpointLimiter { pub fn new(max_concurrency: usize, burst: u32, refill: Duration) -> Self { Self { sem: Semaphore::new(max_concurrency), last_ok: Instant::now(), burst, refill } }
    pub async fn acquire(&self) -> Result<OwnedSemaphorePermit> { self.sem.clone().acquire_owned().await.map_err(|_| CounterStrategyError::ResourceLimitExceeded("semaphore closed".into())) }
}
pub struct HedgeBackpressure { lanes: HashMap<String, Arc<EndpointLimiter>>, global: Semaphore }
impl HedgeBackpressure {
    pub fn new(endpoints: &[String], per_ep: usize, global: usize) -> Self { let mut lanes=HashMap::new(); for e in endpoints { lanes.insert(e.clone(), Arc::new(EndpointLimiter::new(per_ep, 4, Duration::from_millis(50)))); } Self { lanes, global: Semaphore::new(global) } }
    pub async fn acquire_pair(&self, endpoint: &str) -> Result<(OwnedSemaphorePermit, OwnedSemaphorePermit)> { let g=self.global.clone().acquire_owned().await.map_err(|_| CounterStrategyError::ResourceLimitExceeded("global semaphore closed".into()))?; let ep=self.lanes.get(endpoint).expect("endpoint exists").acquire().await?; Ok((g, ep)) }
}

// ==============================
// PIECE 27 — Cluster Clock Calibrator
// ==============================
pub struct ClusterClock { client: Arc<RpcClient>, pub offset_ms: i64, refresh: Duration }
impl ClusterClock {
    pub fn new(client: Arc<RpcClient>, refresh: Duration) -> Self { Self { client, offset_ms: 0, refresh } }
    #[inline] fn now_ms_local() -> i64 { let d = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default(); d.as_millis() as i64 }
    pub async fn run(mut self) -> Self { loop { let _ = self.sample_once().await; sleep(self.refresh).await; } }
    pub async fn sample_once(&mut self) -> Result<()> { let slot = timeout(Duration::from_millis(120), self.client.get_slot()).await.map_err(|_| CounterStrategyError::RpcError("get_slot timeout".into()))?.map_err(|e| CounterStrategyError::RpcError(format!("get_slot: {e}")))?; let btime = timeout(Duration::from_millis(150), self.client.get_block_time(slot)).await.map_err(|_| CounterStrategyError::RpcError("get_block_time timeout".into()))?.map_err(|e| CounterStrategyError::RpcError(format!("get_block_time: {e}")))?; let cluster_ms = (btime.unwrap_or(ClusterClock::now_ms_local() / 1000) as i64) * 1000; let local_ms = ClusterClock::now_ms_local(); self.offset_ms = cluster_ms - local_ms; Ok(()) }
    pub fn corrected_sleep_ms(&self, delay_ms: i64) -> tokio::time::Sleep { let adj = (delay_ms + self.offset_ms).max(0) as u64; tokio::time::sleep(Duration::from_millis(adj)) }
}

// ==============================
// PIECE 28 — Multi-Route Planner
// ==============================
pub struct RouteCandidate { pub instrs: Vec<Instruction>, pub generous_cu_limit: u32, pub est_slippage_bps: f64, pub id: u64 }
pub struct RoutePick { pub id: u64, pub decision: Decision, pub instrs: Vec<Instruction> }
pub fn choose_best_route(
    routes: &[RouteCandidate],
    notional_lamports: u64,
    expected_edge_bps: f64,
    leaders: &[Pubkey],
    model: &InclusionModel,
    p50: u64, p75: u64, p95: u64,
    bloom: &BloomFilterCache,
    net: &NetworkConditions,
    scorecard: &ValidatorScorecard,
) -> Option<RoutePick> {
    let mut best: Option<RoutePick> = None;
    for r in routes {
        let opp = Opportunity { notional_lamports, expected_edge_bps, est_slippage_bps: r.est_slippage_bps, instrs: &r.instrs, leader_candidates: leaders, risk_tolerance_prob: 0.90, generous_cu_limit: r.generous_cu_limit };
        let decision = score_and_decide(&opp, model, p50, p75, p95, bloom, net, scorecard);
        if !decision.accept { continue; }
        match &best {
            None => best = Some(RoutePick { id: r.id, decision, instrs: r.instrs.clone() }),
            Some(cur) => {
                let better = decision.cu_price < cur.decision.cu_price || (decision.cu_price == cur.decision.cu_price && r.generous_cu_limit < cur.decision.cu_limit_hint);
                if better { best = Some(RoutePick { id: r.id, decision, instrs: r.instrs.clone() }); }
            }
        }
    }
    best
}

// ==============================
// PIECE 29 — Recent-Block Account Harvester
// ==============================
pub struct HotsetHarvester { client: Arc<RpcClient>, pub slots_back: u64, pub interval: Duration }
impl HotsetHarvester {
    pub fn new(client: Arc<RpcClient>, slots_back: u64, interval: Duration) -> Self { Self { client, slots_back, interval } }
    pub async fn run(&self, bloom: &Arc<tokio::sync::RwLock<BloomFilterCache>>) -> Result<()> { loop { if let Err(e) = self.harvest_once(bloom).await { log::warn!("hotset harvest error: {e:?}"); } tokio::time::sleep(self.interval).await; } }
    async fn harvest_once(&self, bloom: &Arc<tokio::sync::RwLock<BloomFilterCache>>) -> Result<()> { let tip = timeout(Duration::from_millis(120), self.client.get_slot()).await.map_err(|_| CounterStrategyError::RpcError("get_slot timeout".into()))?.map_err(|e| CounterStrategyError::RpcError(format!("get_slot: {e}")))?; let start = tip.saturating_sub(self.slots_back); let cfg = RpcBlockConfig { encoding: Some(UiTransactionEncoding::Json), transaction_details: Some(TransactionDetails::Signatures), rewards: Some(false), max_supported_transaction_version: Some(0), ..Default::default() }; for s in start..=tip { let blk = match timeout(Duration::from_millis(180), self.client.get_block_with_config(s, cfg.clone())).await { Ok(Ok(Some(b))) => b, _ => continue, }; insert_block_accounts(bloom, &blk).await; } Ok(()) }
}
async fn insert_block_accounts(bloom: &Arc<tokio::sync::RwLock<BloomFilterCache>>, blk: &UiConfirmedBlock) { let mut w = bloom.write().await; if let Some(txns) = &blk.transactions { for t in txns { if let EncodedTransaction::Json(j) = &t.transaction { for k in &j.message.account_keys { if let Ok(pk) = k.parse::<Pubkey>() { w.insert_account(&pk); } } } } } }

// ==============================
// PIECE 30 — Jito bundle client (hedged JSON-RPC)
// ==============================
#[derive(Clone)]
pub struct JitoClient {
    http: Arc<HttpClient>,
    endpoints: Arc<Vec<String>>, // e.g. https://mainnet.block-engine.jito.wtf
    auth_uuid: Option<String>,
    req_deadline: Duration,
    fanout: usize,
    jitter: Duration,
}
impl JitoClient {
    pub fn new(http: Arc<HttpClient>, endpoints: Vec<String>, auth_uuid: Option<String>) -> Self {
        Self { http, endpoints: Arc::new(endpoints), auth_uuid, req_deadline: Duration::from_millis(250), fanout: 3, jitter: Duration::from_millis(7) }
    }
    fn headers(&self) -> HeaderMap { let mut h=HeaderMap::new(); h.insert(CONTENT_TYPE, HeaderValue::from_static("application/json")); if let Some(u)=&self.auth_uuid { if let Ok(v)=HeaderValue::from_str(u) { h.insert("x-jito-auth", v); } } h }
    fn ep_slice(&self) -> &[String] { &self.endpoints[..self.fanout.min(self.endpoints.len())] }
    async fn post_hedged(&self, path: &str, body: serde_json::Value) -> Result<(String, HeaderMap)> {
        let mut set = JoinSet::new();
        for (i, ep) in self.ep_slice().iter().enumerate() {
            let url = format!("{ep}/{path}"); let client=self.http.clone(); let headers=self.headers(); let payload=body.clone(); let d=self.req_deadline; let j=self.jitter.mul_f32((i as f32)/(self.fanout.max(1) as f32));
            set.spawn(async move {
                if !j.is_zero() { sleep(j).await; }
                timeout(d, async {
                    let resp = client.post(&url).headers(headers).json(&payload).send().await?;
                    let status = resp.status(); let hdrs = resp.headers().clone(); let txt = resp.text().await?; anyhow::Ok((status, txt, hdrs))
                }).await
            });
        }
        while let Some(res) = set.join_next().await { if let Ok(Ok(Ok((status, txt, hdrs)))) = res { if status.is_success() { return Ok((txt, hdrs)); } } }
        Err(CounterStrategyError::RpcError("jito: all hedged POSTs failed".into()))
    }
    pub async fn get_tip_accounts(&self) -> Result<Vec<String>> {
        #[derive(Serialize)] struct Req<'a>{ jsonrpc:&'a str, id: u64, method:&'a str, params:[();0] }
        #[derive(Deserialize)] struct Resp{ result: Option<Vec<String>> }
        let body = serde_json::to_value(Req{ jsonrpc:"2.0", id:1, method:"getTipAccounts", params:[] }).unwrap();
        let (txt, _) = self.post_hedged("api/v1/getTipAccounts", body).await?; let r: Resp = serde_json::from_str(&txt).map_err(|e| CounterStrategyError::RpcError(format!("decode tip accounts: {e}")))?; Ok(r.result.unwrap_or_default())
    }
    pub async fn send_bundle_base64(&self, txs_b64: &[String]) -> Result<String> {
        #[derive(Serialize)] struct Opt{ encoding: &'static str }
        #[derive(Serialize)] struct Params<'a>(&'a [String], Opt);
        #[derive(Serialize)] struct Req<'a>{ jsonrpc:&'a str, id:u64, method:&'a str, params: Params<'a> }
        let req = Req{ jsonrpc:"2.0", id:1, method:"sendBundle", params: Params(txs_b64, Opt{ encoding:"base64" }) };
        let body = serde_json::to_value(req).unwrap();
        let (txt, hdrs) = self.post_hedged("api/v1/bundles", body).await?;
        if let Some(bid) = hdrs.get("x-bundle-id") { return Ok(bid.to_str().unwrap_or("").to_string()); }
        #[derive(Deserialize)] struct Resp{ result: Option<String> }
        let r: Resp = serde_json::from_str(&txt).unwrap_or(Resp{ result: None }); Ok(r.result.unwrap_or_else(|| "unknown-bundle-id".to_string()))
    }
    pub async fn get_inflight_statuses(&self, ids: &[String]) -> Result<String> {
        #[derive(Serialize)] struct Req<'a>{ jsonrpc:&'a str, id:u64, method:&'a str, params:&'a [String] }
        let req = Req{ jsonrpc:"2.0", id:1, method:"getInflightBundleStatuses", params: ids };
        let body = serde_json::to_value(req).unwrap();
        let (txt, _) = self.post_hedged("api/v1/getInflightBundleStatuses", body).await?; Ok(txt)
    }
}

// ==============================
// PIECE 31 — Tip-per-CU EWMA (auction floor)
// ==============================
#[derive(Clone)]
pub struct TipEffModel { ema: [f64;4], alpha: f64, min_eff: f64, max_eff: f64 }
impl TipEffModel {
    pub fn new() -> Self { Self { ema:[0.0008,0.0012,0.0018,0.0024], alpha:0.20, min_eff:0.0004, max_eff:0.0050 } }
    #[inline] fn bucket(c: f64) -> usize { if c<0.25 {0} else if c<0.5 {1} else if c<0.75 {2} else {3} }
    pub fn record(&mut self, congestion_0_1:f64, cu_limit:u32, tip_lamports:u64, landed: bool) { let b=Self::bucket(congestion_0_1); let eff=(tip_lamports as f64)/(cu_limit.max(1) as f64); let target= if landed {eff} else { eff*1.25 }; let cur=self.ema[b]; self.ema[b] = ((1.0-self.alpha)*cur + self.alpha*target).clamp(self.min_eff,self.max_eff); }
    pub fn propose(&self, congestion_0_1:f64, cu_limit:u32, notional_lamports:u64) -> u64 { let b=Self::bucket(congestion_0_1); let eff=self.ema[b]; let base=(eff*(cu_limit as f64)).ceil() as u64; let bps=(congestion_0_1*2.0).clamp(0.0,2.0); let value_bump=((notional_lamports as f64)*(bps/10_000.0)).round() as u64; (base+value_bump).clamp(10_000,5_000_000) }
}

// ==============================
// PIECE 32 — Tip instruction & bundle assembler helpers
// ==============================
pub fn pick_tip_receiver(mut rng: ChaCha20Rng, list:&[Pubkey]) -> Pubkey { if list.is_empty() { return Pubkey::new_from_array([0u8;32]); } let i=rng.gen_range(0..list.len()); list[i] }
pub fn build_tip_instruction(payer:&Pubkey, tip_receiver:&Pubkey, lamports:u64) -> Instruction { solana_sdk::system_instruction::transfer(payer, tip_receiver, lamports) }
pub fn build_tip_v0_tx(payer:&Keypair, bh:Hash, tip_ix:Instruction) -> VersionedTransaction { let mut v=Vec::with_capacity(3); v.extend_from_slice(&build_priority_budget(0, 50_000)); v.push(tip_ix); let msg=V0Message::compile(&payer.pubkey(), &v, &[]); VersionedTransaction::try_new(VersionedMessage::V0(msg), &[payer]).expect("sign") }
pub fn vtx_to_base64(vtx:&VersionedTransaction) -> String { let raw=bincode::serialize(vtx).expect("serialize vtx"); B64STD.encode(raw) }

// ==============================
// PIECE 33 — Jito bundle submitter pipeline (simplified)
// ==============================
pub struct JitoSubmitCtx<'a> {
    pub payer: &'a Keypair,
    pub rpc: Arc<HedgedRpc>,
    pub jito: Arc<JitoClient>,
    pub alts: &'a [AddressLookupTableAccount],
    pub predictor_p50p75p95: (u64,u64,u64),
    pub incl_model: &'a InclusionModel,
    pub tip_eff: &'a TipEffModel,
    pub scorecard: &'a ValidatorScorecard,
    pub net: &'a NetworkConditions,
    pub bloom: &'a BloomFilterCache,
    pub leaders: &'a [Pubkey],
    pub counters: &'a PerfCounters,
    pub sig_ring: &'a SigRingTtl,
}

pub async fn send_route_as_bundle(
    ctx: &JitoSubmitCtx<'_>,
    notional_lamports: u64,
    expected_edge_bps: f64,
    generous_cu_limit: u32,
    payload_instrs: Vec<Instruction>,
) -> Result<(String, u64, u64)> {
    let opp = Opportunity { notional_lamports, expected_edge_bps, est_slippage_bps: 0.0, instrs: &payload_instrs, leader_candidates: ctx.leaders, risk_tolerance_prob: 0.90, generous_cu_limit };
    let (p50,p75,p95)=ctx.predictor_p50p75p95; let decision = score_and_decide(&opp, ctx.incl_model, p50,p75,p95, ctx.bloom, ctx.net, ctx.scorecard);
    if !decision.accept { return Err(CounterStrategyError::UnsafeMarketConditions("EV<0 after auction economics".into())); }
    let build_with = |bh: Hash, cu_price: u64, cu_limit: u32| { build_v0_tx(ctx.payer, bh, payload_instrs.clone(), cu_price, cu_limit, ctx.alts) };
    let (tight_cu_limit, _used) = tune_cu_limit(ctx.rpc.clone(), build_with, generous_cu_limit, decision.cu_price, 1.10, 100_000).await?;
    let bh = ctx.rpc.get_latest_blockhash().await?;
    let vtx_payload = build_v0_tx(ctx.payer, bh, payload_instrs.clone(), decision.cu_price, tight_cu_limit, ctx.alts);
    let sig = vtx_payload.signatures()[0]; if !ctx.sig_ring.check_and_insert(sig) { return Err(CounterStrategyError::RpcError("duplicate signature filtered".into())); }
    let tip_accounts = ctx.jito.get_tip_accounts().await?; let tip_pks: Vec<Pubkey> = tip_accounts.iter().filter_map(|s| s.parse().ok()).collect();
    let rng = ChaCha20Rng::from_seed(ctx.payer.pubkey().to_bytes()); let tip_receiver = pick_tip_receiver(rng, &tip_pks);
    let tip_lamports = ctx.tip_eff.propose(ctx.net.congestion_level, tight_cu_limit, notional_lamports);
    let tip_ix = build_tip_instruction(&ctx.payer.pubkey(), &tip_receiver, tip_lamports); let vtx_tip = build_tip_v0_tx(ctx.payer, bh, tip_ix);
    let b64_payload = vtx_to_base64(&vtx_payload); let b64_tip = vtx_to_base64(&vtx_tip);
    let bundle_id = ctx.jito.send_bundle_base64(&[b64_payload, b64_tip]).await?;
    ctx.counters.bundles_submitted(true);
    Ok((bundle_id, tight_cu_limit as u64, tip_lamports))
}

// ==============================
// PIECE 34 — Graceful fallback: bundle → raw RPC
// ==============================
pub async fn fallback_send_raw(rpc: Arc<HedgedRpc>, mut vtx_payload: VersionedTransaction) -> Result<Signature> { let raw = serialize_vtx(&vtx_payload); hedged_send_raw(rpc.clone(), raw, None).await }

// ==============================
// PIECE 35 — Slot-tick synchronizer
// ==============================
pub fn delay_to_next_tick_ms(now_ms: u64, safety_ms: u64) -> u64 { let rem = now_ms % 50; let to_next = if rem==0 {50} else {50 - rem}; to_next.saturating_sub(safety_ms).max(1) }

// ==============================
// PIECE 36 — Bundle laddering across next ticks
// ==============================
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
pub struct LadderPlan { pub tip_lamports: Vec<u64>, pub safety_ms: u64, pub max_ladders: usize }
pub async fn send_bundle_ladder(
    jito: Arc<JitoClient>,
    clock: &ClusterClock,
    payload_vtx: VersionedTransaction,
    tip_vtx_builder: impl Fn(u64)->VersionedTransaction + Send + Sync + 'static + Clone,
    plan: LadderPlan,
) -> Result<Vec<String>> {
    if plan.tip_lamports.is_empty() { return Err(CounterStrategyError::ConfigurationError("empty ladder".into())); }
    let mut sent_ids = Vec::with_capacity(plan.tip_lamports.len().min(plan.max_ladders));
    let abort = Arc::new(AtomicBool::new(false));
    for (i, tip) in plan.tip_lamports.iter().take(plan.max_ladders).cloned().enumerate() {
        if abort.load(AtomicOrdering::Relaxed) { break; }
        let tip_vtx = tip_vtx_builder(tip);
        let b64_payload = vtx_to_base64(&payload_vtx);
        let b64_tip = vtx_to_base64(&tip_vtx);
        let now_ms = (ClusterClock::now_ms_local() as i64 + clock.offset_ms) as u64;
        let to_next = delay_to_next_tick_ms(now_ms + (i as u64)*50, plan.safety_ms);
        clock.corrected_sleep_ms(to_next as i64).await;
        if let Ok(id) = jito.send_bundle_base64(&[b64_payload.clone(), b64_tip]).await {
            sent_ids.push(id);
            sleep(Duration::from_millis(2)).await;
        }
    }
    Ok(sent_ids)
}

// ==============================
// PIECE 37 — Consistent-hash tip receiver selection
// ==============================
pub fn pick_tip_receiver_consistent(sig: &Signature, tips: &[Pubkey]) -> Option<Pubkey> {
    if tips.is_empty() { return None; }
    let mut h = Sha256::new();
    h.update(sig.as_ref());
    h.update(b"tip");
    let d = h.finalize();
    let idx = (u64::from_le_bytes([d[0],d[1],d[2],d[3],d[4],d[5],d[6],d[7]]) as usize) % tips.len();
    Some(tips[idx])
}

// ==============================
// PIECE 38 — Inflight watcher → online model updates
// ==============================
#[derive(Clone)]
pub struct InflightMeta { pub validator: Pubkey, pub cu_limit: u32, pub tip_lamports: u64, pub congestion_snapshot: f64 }
#[derive(Deserialize)] struct StatusResp { #[allow(dead_code)] jsonrpc: Option<String>, #[allow(dead_code)] id: Option<u64>, result: Option<Vec<InflightStatus>> }
#[derive(Deserialize)] struct InflightStatus { #[serde(default)] bundle_id: String, #[serde(default)] state: String }
pub struct InflightWatcher { jito: Arc<JitoClient>, pub table: HashMap<String, InflightMeta>, pub poll_every: Duration }
impl InflightWatcher {
    pub fn new(jito: Arc<JitoClient>) -> Self { Self { jito, table: HashMap::new(), poll_every: Duration::from_millis(120) } }
    pub fn track(&mut self, bundle_id: String, meta: InflightMeta) { self.table.insert(bundle_id, meta); }
    pub async fn run(&mut self, tip_eff: &mut TipEffModel, scorecard: &mut ValidatorScorecard) {
        loop {
            if self.table.is_empty() { sleep(self.poll_every).await; continue; }
            let mut batch: Vec<String> = Vec::with_capacity(5);
            for k in self.table.keys().take(5) { batch.push(k.clone()); }
            match self.jito.get_inflight_statuses(&batch).await {
                Ok(txt) => {
                    if let Ok(resp) = serde_json::from_str::<StatusResp>(&txt) {
                        if let Some(list) = resp.result {
                            for entry in list {
                                if let Some(meta) = self.table.remove(&entry.bundle_id) {
                                    let landed = entry.state.to_ascii_lowercase().contains("land");
                                    tip_eff.record(meta.congestion_snapshot, meta.cu_limit, meta.tip_lamports, landed);
                                    scorecard.record(meta.validator, landed, meta.tip_lamports);
                                }
                            }
                        }
                    }
                }
                Err(_) => { /* ignore transient errors */ }
            }
            sleep(self.poll_every).await;
}

// ==============================
// PIECE 40 — Bundle planner: leader-aware tick alignment
// ==============================
pub struct BundleWindowPlan { pub leader: Pubkey, pub delay_ms: i64, pub per_rung_ms: u64, pub rungs: usize }
pub fn plan_windows(
    leaders: &[Pubkey],
    net: &NetworkConditions,
    lds: &mut LdsSampler,
    phase_ms_in_slot: (u64,u64),
    rungs: usize,
) -> Vec<BundleWindowPlan> {
    let mut out = Vec::new();
    let lds_off = lds.next_offset_ms((-7,7));
    for (i, leader) in leaders.iter().take(2).enumerate() {
        let slot_delta = i; // simplistic: take first two as next leaders
        let base_delay = delay_to_leader_ms(net, slot_delta, phase_ms_in_slot, lds_off);
        out.push(BundleWindowPlan { leader: *leader, delay_ms: base_delay, per_rung_ms: 50, rungs });
    }
    out
}

// ==============================
// PIECE 41 — Multi-payload bundle packer (rw-disjoint, CU-bounded, EV-aware)
// ==============================
#[derive(Clone)]
pub struct PreparedRoute { pub id: u64, pub instrs: Vec<Instruction>, pub tuned_cu_limit: u32, pub gross_lamports: i64 }
#[derive(Clone)]
pub struct PackedBundle { pub selected_ids: Vec<u64>, pub total_cu: u32, pub tip_lamports: u64 }
fn conflicts(a_rw: &std::collections::HashSet<Pubkey>, a_ro: &std::collections::HashSet<Pubkey>, b_rw: &std::collections::HashSet<Pubkey>, b_ro: &std::collections::HashSet<Pubkey>) -> bool {
    !a_rw.is_disjoint(b_rw) || !a_rw.is_disjoint(b_ro) || !b_rw.is_disjoint(a_ro)
}
pub fn pack_routes(
    routes: &[PreparedRoute],
    tip_eff: &TipEffModel,
    net: &NetworkConditions,
    target_prob: f64,
    cu_cap: u32,
) -> PackedBundle {
    #[derive(Clone)] struct Annot { id: u64, cu: u32, gross: i64, ro: std::collections::HashSet<Pubkey>, rw: std::collections::HashSet<Pubkey> }
    let mut anns: Vec<Annot> = routes.iter().map(|r| { let (ro, rw) = split_rw_sets(&r.instrs); Annot { id: r.id, cu: r.tuned_cu_limit, gross: r.gross_lamports, ro: ro.into_iter().collect(), rw: rw.into_iter().collect() } }).collect();
    anns.sort_by(|a,b| { let da=(a.gross as f64)/(a.cu.max(1) as f64); let db=(b.gross as f64)/(b.cu.max(1) as f64); db.partial_cmp(&da).unwrap() });
    let mut sel_ids: Vec<u64> = Vec::new();
    let mut sel_ro: std::collections::HashSet<Pubkey> = std::collections::HashSet::new();
    let mut sel_rw: std::collections::HashSet<Pubkey> = std::collections::HashSet::new();
    let mut total_cu: u32 = 0; let mut total_gross: i64 = 0;
    for a in anns { if total_cu.saturating_add(a.cu) > cu_cap { continue; } if conflicts(&sel_rw,&sel_ro,&a.rw,&a.ro) { continue; } let next_cu = total_cu + a.cu; let tip = tip_eff.propose(net.congestion_level, next_cu, 0); let ev_next = (total_gross + a.gross) as f64 - tip as f64; if target_prob * ev_next <= 0.0 { continue; } sel_ids.push(a.id); total_cu = next_cu; total_gross += a.gross; sel_ro.extend(a.ro.into_iter()); sel_rw.extend(a.rw.into_iter()); }
    let final_tip = tip_eff.propose(net.congestion_level, total_cu, 0);
    PackedBundle { selected_ids: sel_ids, total_cu, tip_lamports: final_tip }
}

// ==============================
// PIECE 42 — Precise tick sleeper (coarse sleep + busy spin)
// ==============================
pub async fn precise_corrected_sleep(_clock: &ClusterClock, delay_ms: u64, spin_budget_ms: u64) {
    use std::time::{Duration, Instant};
    use std::hint::spin_loop;
    let start = Instant::now();
    let target = start + Duration::from_millis(delay_ms);
    let now = Instant::now();
    let remain = if target > now { target - now } else { Duration::from_millis(0) };
    let spin_budget = Duration::from_millis(spin_budget_ms.min(remain.as_millis() as u64));
    let coarse = remain.saturating_sub(spin_budget);
    if coarse.as_millis() > 0 { tokio::time::sleep(coarse).await; }
    while Instant::now() < target { spin_loop(); }
}

// ==============================
            set.spawn(async move { if !j.is_zero() { tokio::time::sleep(j).await; } let t0=Instant::now(); let res = timeout(d, c.send_transaction(txc)).await; let dt = t0.elapsed().as_millis() as f64; (i, dt, res) });
        }
        while let Some(res) = set.join_next().await { if let Ok((idx, ms, out)) = res { let ok = matches!(out, Ok(Ok(_))); { let mut h = health.lock().unwrap(); h.observe(idx, ms, ok); } if let Ok(Ok(sig)) = out { return Ok(sig); } } }
        Err(CounterStrategyError::RpcError("health-hedge: all attempts failed".into()))
    }
}

// ==============================
// PIECE 45 — Ladder amounts auto-shape from auction variance
// ==============================
pub fn ladder_amounts(base: u64, tip_eff: &TipEffModel, congestion_0_1: f64, rungs: usize) -> Vec<u64> {
    let b = if rungs < 1 { 1 } else { rungs };
    let eff_lo = tip_eff.ema[0].min(tip_eff.ema[1]);
    let eff_hi = tip_eff.ema[2].max(tip_eff.ema[3]);
    let spread = ((eff_hi / eff_lo).clamp(1.0, 2.5) - 1.0) as f64;
    let step = 0.10 + 0.25 * congestion_0_1 + 0.40 * spread;
    let mut out = Vec::with_capacity(b);
    let mut cur = base as f64;
    for _ in 0..b { out.push(cur.round() as u64); cur *= 1.0 + step; }
    out
}

// ==============================
// PIECE 46 — Stable Bloom (aging, fixed FP under streams)
// ==============================
pub struct StableBloom {
    bits: Vec<u8>,
    k: u8,
    m_bits: usize,
    s_clear: u32,
    rng: core::sync::atomic::AtomicU64,
}
impl StableBloom {
    pub fn new(capacity: usize, fp_target: f64) -> Self {
        let m = ((-(capacity as f64) * fp_target.ln()) / (2f64.ln().powi(2))).ceil() as usize;
        let k = (((m as f64) / (capacity as f64)) * 2f64.ln()).ceil() as u8;
        let m_aligned = (m + 7) & !7;
        let s = (k as u32).max(1);
        Self { bits: vec![0u8; m_aligned/8], k: k.max(1), m_bits: m_aligned, s_clear: s, rng: core::sync::atomic::AtomicU64::new(0x9E3779B97F4A7C15) }
    }
    #[inline] fn mix64(x: u64) -> u64 { let mut z = x.wrapping_add(0x9E3779B97F4A7C15); z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9); z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB); z ^ (z >> 31) }
    #[inline] fn next_u64(&self) -> u64 { let old = self.rng.fetch_add(0x9E3779B97F4A7C15, core::sync::atomic::Ordering::Relaxed); Self::mix64(old) }
    #[inline] fn set_bit(&mut self, idx: usize) { self.bits[idx >> 3] |= 1u8 << (idx & 7); }
    #[inline] fn get_bit(&self, idx: usize) -> bool { (self.bits[idx >> 3] & (1u8 << (idx & 7))) != 0 }
    #[inline] fn clr_bit(&mut self, idx: usize) { self.bits[idx >> 3] &= !(1u8 << (idx & 7)); }
    #[inline] fn hash_i(key: &[u8], i: u8) -> u64 { let mut x: u64 = 0x9E3779B185EBCA87 ^ (i as u64); for &b in key { x = (x ^ (b as u64)).rotate_left(13).wrapping_mul(0xC2B2AE3D27D4EB4F); } x }
    pub fn might_contain(&self, acc: &solana_sdk::pubkey::Pubkey) -> bool { let m = self.m_bits; (0..self.k).all(|i| self.get_bit((Self::hash_i(acc.as_ref(), i) as usize) % m)) }
    pub fn insert_stable(&mut self, acc: &solana_sdk::pubkey::Pubkey) {
        let m = self.m_bits;
        for _ in 0..self.s_clear { let idx = (self.next_u64() as usize) % m; self.clr_bit(idx); }
        for i in 0..self.k { let idx = (Self::hash_i(acc.as_ref(), i) as usize) % m; self.set_bit(idx); }
    }
}

// ==============================
// PIECE 47 — Cacheline-hard perf counters (no false sharing)
// ==============================
#[repr(align(64))]
pub struct AlignedAtomicU64(pub core::sync::atomic::AtomicU64);
impl Default for AlignedAtomicU64 { fn default() -> Self { Self(core::sync::atomic::AtomicU64::new(0)) } }
#[derive(Default)]
pub struct PerfCountersAligned {
    pub succ: AlignedAtomicU64,
    pub fail: AlignedAtomicU64,
    pub profit: AlignedAtomicU64,
    pub loss: AlignedAtomicU64,
    pub tips: AlignedAtomicU64,
    pub bundles: AlignedAtomicU64,
    pub bundles_ok: AlignedAtomicU64,
}
impl PerfCountersAligned {
    #[inline] pub fn inc_success(&self) { self.succ.0.fetch_add(1, core::sync::atomic::Ordering::Relaxed); }
    #[inline] pub fn inc_failure(&self) { self.fail.0.fetch_add(1, core::sync::atomic::Ordering::Relaxed); }
    #[inline] pub fn add_profit(&self, lamports: i64) { if lamports >= 0 { self.profit.0.fetch_add(lamports as u64, core::sync::atomic::Ordering::Relaxed); } else { self.loss.0.fetch_add((-lamports) as u64, core::sync::atomic::Ordering::Relaxed); } }
    #[inline] pub fn add_tip(&self, lamports: u64) { self.tips.0.fetch_add(lamports, core::sync::atomic::Ordering::Relaxed); }
    #[inline] pub fn bundles_submitted(&self, ok: bool) { self.bundles.0.fetch_add(1, core::sync::atomic::Ordering::Relaxed); if ok { self.bundles_ok.0.fetch_add(1, core::sync::atomic::Ordering::Relaxed); } }
}

// ==============================
// PIECE 48 — Robust cluster clock (median + MAD)
// ==============================
pub struct RobustClock {
    rpc: std::sync::Arc<solana_client::nonblocking::rpc_client::RpcClient>,
    med: P2Quantile,
    mad: P2Quantile,
    pub offset_ms: i64,
    pub mad_ms: u64,
}
impl RobustClock {
    pub fn new(rpc: std::sync::Arc<solana_client::nonblocking::rpc_client::RpcClient>) -> Self { Self { rpc, med: P2Quantile::new(0.5), mad: P2Quantile::new(0.5), offset_ms: 0, mad_ms: 2 } }
    #[inline] fn now_ms_local() -> i64 { let d = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default(); d.as_millis() as i64 }
    pub async fn sample_once(&mut self) -> Result<()> {
        let slot = tokio::time::timeout(std::time::Duration::from_millis(120), self.rpc.get_slot()).await.map_err(|_| CounterStrategyError::RpcError("get_slot timeout".into()))?.map_err(|e| CounterStrategyError::RpcError(format!("get_slot: {e}")))?;
        let t_s = tokio::time::timeout(std::time::Duration::from_millis(150), self.rpc.get_block_time(slot)).await.map_err(|_| CounterStrategyError::RpcError("get_block_time timeout".into()))?.map_err(|e| CounterStrategyError::RpcError(format!("get_block_time: {e}")))?;
        let cluster_ms = (t_s.unwrap_or(Self::now_ms_local()/1000) as i64) * 1000; let local_ms = Self::now_ms_local(); let offset = (cluster_ms - local_ms) as f64;
        self.med.observe(offset); let m = self.med.quantile().unwrap_or(0.0); self.mad.observe((offset - m).abs());
        self.offset_ms = m.round() as i64; self.mad_ms = self.mad.quantile().unwrap_or(1.0).clamp(0.0, 10.0).round() as u64; Ok(())
    }
    pub async fn corrected_precise_sleep(&self, delay_ms: u64) { let adj = (delay_ms as i64 + self.offset_ms).max(0) as u64; let spin = self.mad_ms.max(1).min(3); precise_corrected_sleep_like(adj, spin).await; }
}

// helper shim (non-circular) matching PIECE 42 behavior
pub async fn precise_corrected_sleep_like(delay_ms: u64, spin_budget_ms: u64) {
    use std::time::{Duration, Instant};
    use std::hint::spin_loop;
    let start = Instant::now(); let target = start + Duration::from_millis(delay_ms);
    let now = Instant::now(); let remain = if target > now { target - now } else { Duration::from_millis(0) };
    let spin_budget = Duration::from_millis(spin_budget_ms.min(remain.as_millis() as u64));
    let coarse = remain.saturating_sub(spin_budget);
    if coarse.as_millis() > 0 { tokio::time::sleep(coarse).await; }
    while Instant::now() < target { spin_loop(); }
}

// ==============================
// PIECE 49 — Validator gate with hysteresis (auto-ban/unban)
// ==============================
#[derive(Clone, Copy)]
pub struct GateState { succ: u64, fail: u64, banned_until: Option<std::time::Instant>, strikes: u32 }
impl Default for GateState { fn default() -> Self { Self { succ:0, fail:0, banned_until: None, strikes:0 } } }
pub struct ValidatorGate { map: std::collections::HashMap<solana_sdk::pubkey::Pubkey, GateState>, min_obs: u64, wr_floor: f64, base_ban: std::time::Duration }
impl ValidatorGate {
    pub fn new(min_obs: u64, wr_floor: f64, base_ban: std::time::Duration) -> Self { Self { map: std::collections::HashMap::new(), min_obs, wr_floor, base_ban } }
    pub fn record(&mut self, v: solana_sdk::pubkey::Pubkey, ok: bool) { let e = self.map.entry(v).or_default(); if ok { e.succ += 1; } else { e.fail += 1; } self.recompute(v); }
    fn recompute(&mut self, v: solana_sdk::pubkey::Pubkey) { let e = self.map.get_mut(&v).unwrap(); let n = e.succ + e.fail; if n < self.min_obs { return; } let wr = (e.succ as f64)/(n as f64); if wr < self.wr_floor { e.strikes = e.strikes.saturating_add(1); let mult = 1u32 << (e.strikes.saturating_sub(1).min(4)); e.banned_until = Some(std::time::Instant::now() + self.base_ban * mult); e.succ /= 2; e.fail /= 2; } else { if e.strikes > 0 && n >= 64 { e.strikes -= 1; } if let Some(t) = e.banned_until { if std::time::Instant::now() > t { e.banned_until = None; } } } }
    pub fn filter_candidates(&self, cands: &[solana_sdk::pubkey::Pubkey]) -> Vec<solana_sdk::pubkey::Pubkey> { cands.iter().cloned().filter(|v| match self.map.get(v).and_then(|s| s.banned_until) { Some(t) if std::time::Instant::now() < t => false, _ => true, }).collect() }
}

// ==============================
// PIECE 50 — In-flight de-dupe guard (single sender per route/signature)
// ==============================
pub struct InflightDedupe { set: std::sync::Arc<tokio::sync::Mutex<std::collections::HashSet<u64>>> }
impl InflightDedupe { pub fn new() -> Self { Self { set: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashSet::with_capacity(1024))) } }
    pub async fn try_acquire(&self, key: u64) -> Option<InflightGuard> { let mut s = self.set.lock().await; if s.contains(&key) { return None; } s.insert(key); Some(InflightGuard { key, set: self.set.clone() }) }
}
pub struct InflightGuard { key: u64, set: std::sync::Arc<tokio::sync::Mutex<std::collections::HashSet<u64>>> }
impl Drop for InflightGuard { fn drop(&mut self) { let key = self.key; let set = self.set.clone(); tokio::spawn(async move { set.lock().await.remove(&key); }); } }
pub fn key64_from_bytes(b: &[u8]) -> u64 { use std::collections::hash_map::DefaultHasher; use std::hash::{Hash, Hasher}; let mut h = DefaultHasher::new(); b.hash(&mut h); h.finish() }

// ==============================
// PIECE 51 — Leader-TPU fast-path (QUIC) with hedged RPC fallback
// ==============================
pub struct TpuFastPath {
    tpu: std::sync::Arc<solana_client::tpu_client::TpuClient>,
    rpc_fallback: std::sync::Arc<HedgedRpc>,
    try_timeout: std::time::Duration,
}
impl TpuFastPath {
    pub fn new(rpc_http_url: String, ws_url: String, rpc_fallback: std::sync::Arc<HedgedRpc>) -> Self {
        let rpc = solana_client::rpc_client::RpcClient::new(rpc_http_url);
        let tpu = solana_client::tpu_client::TpuClient::new_with_client(&rpc, &ws_url, solana_client::tpu_client::TpuClientConfig::default()).expect("TPU client init");
        Self { tpu: std::sync::Arc::new(tpu), rpc_fallback, try_timeout: std::time::Duration::from_millis(80) }
    }
    pub async fn send_versioned(&self, vtx: VersionedTransaction) -> Result<solana_sdk::signature::Signature> {
        let sig = vtx.signatures()[0];
        let bytes = bincode::serialize(&vtx).map_err(|e| CounterStrategyError::RpcError(format!("vtx serialize: {e}")))?;
        let tpu = self.tpu.clone();
        let ok = tokio::time::timeout(self.try_timeout, tokio::task::spawn_blocking(move || tpu.try_send_wire_transaction(bytes)))
            .await.ok().and_then(|j| j.ok()).unwrap_or(false);
        if ok { return Ok(sig); }
        self.rpc_fallback.send_versioned_tx(vtx).await
    }
}

// ==============================
// PIECE 122 — FastSignerMulti Auto-Orderer (message-driven signer order)
// ==============================
use ed25519_dalek::{ExpandedSecretKey, PublicKey as DalekPublicKey, SecretKey as DalekSecretKey, Signer as _};
pub struct FastSignerAuto {
    // expanded keys in message signer order
    esk: Vec<(Pubkey, ExpandedSecretKey, DalekPublicKey)>,
}
impl FastSignerAuto {
    /// Build from compiled message and unordered keypairs; map into message signer order
    pub fn from_message_and_keys(msg: &VersionedMessage, all_keys: &[&Keypair]) -> Self {
        use std::collections::HashMap;
        // 1) pubkey -> keypair map
        let mut mp: HashMap<Pubkey, &Keypair> = HashMap::with_capacity(all_keys.len());
        for kp in all_keys { mp.insert(kp.pubkey(), *kp); }

        // 2) extract signer list from message
        let signer_pubs: Vec<Pubkey> = match msg {
            VersionedMessage::Legacy(m) => {
                let n = m.header.num_required_signatures as usize;
                m.account_keys[0..n].to_vec()
            }
            VersionedMessage::V0(m) => {
                let n = m.header.num_required_signatures as usize;
                m.account_keys[0..n].to_vec()
            }
        };

        // 3) build expanded secret keys in that order
        let mut esk = Vec::with_capacity(signer_pubs.len());
        for pk in signer_pubs {
            let kp = mp.get(&pk).expect("missing keypair for signer in message");
            let bytes = kp.to_bytes();
            let sk = DalekSecretKey::from_bytes(&bytes[..32]).expect("secret");
            let esk_exp = ExpandedSecretKey::from(&sk);
            let pk_d = DalekPublicKey::from(&sk);
            esk.push((pk, esk_exp, pk_d));
        }
        Self { esk }
    }

    /// Convenience wrapper for existing callsites
    pub fn from_keypairs(keys: &[&Keypair], msg: &VersionedMessage) -> Self {
        Self::from_message_and_keys(msg, keys)
    }

    #[inline]
    pub fn sign_into(&self, msg: VersionedMessage) -> VersionedTransaction {
        let m = bincode::serialize(&msg).expect("msg serialize");
        let mut sigs = Vec::with_capacity(self.esk.len());
        for (_, e, p) in &self.esk {
            let s = e.sign(&m, p);
            // solana_sdk::signature::Signature::from_bytes accepts &[u8;64] in current SDKs
            sigs.push(solana_sdk::signature::Signature::from_bytes(&s.to_bytes()));
        }
        VersionedTransaction { signatures: sigs, message: msg }
    }
}

// ==============================
// PIECE 122B — FastSignerAutoExt (sign best-of)
// ==============================
pub struct FastSignerAutoExt;
impl FastSignerAutoExt {
    /// Sign each candidate message with the provided signers (unordered),
    /// returning the fully signed VersionedTransaction with the smallest wire_len.
    pub fn sign_best_of(msgs: &[VersionedMessage], signers: &[&Keypair]) -> VersionedTransaction {
        let mut best_len = usize::MAX;
        let mut best: Option<VersionedTransaction> = None;
        for msg in msgs {
            let fsa = FastSignerAuto::from_keypairs(signers, msg);
            let vtx = fsa.sign_into(msg.clone());
            let len = wire_len(&vtx);
            if len < best_len { best_len = len; best = Some(vtx); }
        }
        best.expect("at least one candidate")
    }
}

// (Removed earlier placeholder SigRingTtl and HTTP client; see finalized implementations below.)

// ==============================
// PIECE 118 — Ultra Assembler (Jump-Wallet + CU Optimizer + ALT-Fit + FastSignerMulti)
// ==============================
// WalletPoolJump: jump-consistent-hash based wallet assigner
pub struct WalletPoolJump { wallets: Vec<std::sync::Arc<Keypair>> }
impl WalletPoolJump {
    pub fn new(keys: Vec<std::sync::Arc<Keypair>>) -> Self { Self { wallets: keys } }
    #[inline] pub fn assign(&self, id: u64) -> std::sync::Arc<Keypair> {
        if self.wallets.is_empty() { return std::sync::Arc::new(Keypair::new()); }
        let idx = jump_consistent_hash(id, self.wallets.len() as u32) as usize;
        self.wallets[idx].clone()
    }
}
#[inline]
fn jump_consistent_hash(key: u64, buckets: u32) -> u32 {
    // Lamping & Veach algorithm
    let mut b: i64 = -1;
    let mut j: i64 = 0;
    let mut k = key;
    while j < buckets as i64 {
        b = j;
        k = k.wrapping_mul(2862933555777941757).wrapping_add(1);
        let x = (((b + 1) as f64) * ((1u64 << 31) as f64) / (((k >> 33) + 1) as f64)) as i64;
        j = x;
    }
    b as u32
}
pub struct UltraCtx<'a> { pub wallet_pool: &'a WalletPoolJump, pub alts_all: &'a [AddressLookupTableAccount], pub mtu: &'a MtuLearner, pub cu_ewma: &'a CuEwma }
pub struct UltraInputs<'a> { pub route_id: u64, pub bh: Hash, pub instrs: Vec<Instruction>, pub base_cu_price: u64, pub min_cu_floor: u32, pub max_cu_cap: u32, pub extra_signers: &'a [&'a Keypair] }
pub fn assemble_ultra(uctx:&UltraCtx<'_>, inp:UltraInputs<'_>) -> (std::sync::Arc<Keypair>, VersionedTransaction) {
    let payer = uctx.wallet_pool.assign(inp.route_id);
    let cu_limit = optimize_cu_limit(inp.route_id, uctx.cu_ewma, 0.995, inp.min_cu_floor, inp.max_cu_cap).map(|p| p.cu_limit).unwrap_or(inp.max_cu_cap);
    let canon = canonicalize_instructions(inp.instrs);
    let mut vtx = build_min_bytes_fit_mtu_stable(&payer, inp.bh, canon.clone(), inp.base_cu_price, cu_limit, uctx.alts_all, uctx.mtu);
    if !inp.extra_signers.is_empty() {
        let mut signers: Vec<&Keypair> = Vec::with_capacity(1 + inp.extra_signers.len()); signers.push(&payer); signers.extend_from_slice(inp.extra_signers);
        let msg = vtx.message.clone(); let fsa = FastSignerAuto::from_keypairs(&signers, &msg); vtx = fsa.sign_into(msg);
    }
    (payer, vtx)
}

// ==============================
// PIECE 119 — Lamport Reserve (per-wallet atomic reservation)
// ==============================
pub struct LamportReserve { inner: std::sync::Mutex<std::collections::HashMap<Pubkey, u64>>, min_buffer: u64, rpc: std::sync::Arc<solana_client::nonblocking::rpc_client::RpcClient> }
impl LamportReserve {
    pub fn new(rpc: std::sync::Arc<solana_client::nonblocking::rpc_client::RpcClient>, min_buffer:u64)->Self{ Self{ inner: std::sync::Mutex::new(std::collections::HashMap::with_capacity(64)), min_buffer, rpc } }
    pub async fn try_reserve(&self, payer:&Pubkey, want:u64)->bool{
        use tokio::time::{timeout, Duration};
        let bal = match timeout(Duration::from_millis(120), self.rpc.get_balance(payer)).await { Ok(Ok(b))=>b, _=>0 };
        if bal==0 { return false; }
        let mut m=self.inner.lock().unwrap(); let cur=*m.get(payer).unwrap_or(&0);
        if bal.saturating_sub(cur) >= want + self.min_buffer { m.insert(*payer, cur + want); true } else { false }
    }
    pub fn release(&self, payer:&Pubkey, amt:u64){ let mut m=self.inner.lock().unwrap(); if let Some(v)=m.get_mut(payer){ *v = v.saturating_sub(amt); } }
}

// ==============================
// PIECE 120 — WS UCB Selector (pick top-K PubSub endpoints)
// ==============================
#[derive(Clone, Default)] struct WSEndpoint { pulls:u64, succ:f64, lat_ms:f64 }
pub struct WsUcb { eps: std::sync::Mutex<Vec<WSEndpoint>>, c:f64 }
impl WsUcb { pub fn new(n:usize, explore_c:f64)->Self{ Self{ eps: std::sync::Mutex::new(vec![WSEndpoint::default(); n]), c: explore_c } }
    fn order(&self)->Vec<usize>{ let eps=self.eps.lock().unwrap(); let tot:u64=eps.iter().map(|e| e.pulls.max(1)).sum(); let mut idx:Vec<usize>=(0..eps.len()).collect(); idx.sort_by(|&i,&j|{ let ei=&eps[i]; let ej=&eps[j]; let si= ei.succ + self.c * ((tot as f64).ln() / (ei.pulls.max(1) as f64)).sqrt() - 0.002*(ei.lat_ms/200.0); let sj= ej.succ + self.c * ((tot as f64).ln() / (ej.pulls.max(1) as f64)).sqrt() - 0.002*(ej.lat_ms/200.0); sj.partial_cmp(&si).unwrap() }); idx }
    fn observe(&self,i:usize, ok:bool, ms:f64){ let mut eps=self.eps.lock().unwrap(); if let Some(e)=eps.get_mut(i){ e.pulls+=1; e.succ = 0.8*e.succ + 0.2*if ok{1.0}else{0.0}; e.lat_ms = 0.8*e.lat_ms + 0.2*ms.max(1.0); } }
}
// ==============================
// PIECE QACK — Quick-Ack RPC registry (used by PIECE 120)
// ==============================
pub struct QuickAckRpc {
    clients: Vec<solana_client::nonblocking::rpc_client::RpcClient>,
    fan: usize,
}
impl QuickAckRpc {
    pub fn new(urls: &[String], fan: usize) -> Self {
        let clients = urls.iter().map(|u| solana_client::nonblocking::rpc_client::RpcClient::new(u.clone())).collect();
        Self { clients, fan: fan.max(1) }
    }
    #[inline] pub fn clients_iter(&self) -> impl Iterator<Item=&solana_client::nonblocking::rpc_client::RpcClient> { self.clients.iter() }
    #[inline] pub fn fanout(&self) -> usize { self.fan }
}
static QUICK_ACK_RPC: once_cell::sync::OnceCell<std::sync::Arc<QuickAckRpc>> = once_cell::sync::OnceCell::new();

pub async fn subscribe_processed_topk(ws_urls:&[String], sig: solana_sdk::signature::Signature, top_k:usize, sel: std::sync::Arc<WsUcb>) -> anyhow::Result<tokio::sync::watch::Receiver<bool>> {
    use tokio::sync::watch; use tokio::time::{timeout, Duration}; use futures_util::StreamExt; use solana_client::nonblocking::pubsub_client::PubsubClient; use solana_client::rpc_config::RpcSignatureSubscribeConfig; use solana_transaction_status::UiTransactionEncoding;
    let (tx,rx)=watch::channel(false); let order=sel.order();
    for &i in order.iter().take(top_k.max(1)) { let url=ws_urls[i].clone(); let txc=tx.clone(); let selc=sel.clone(); tokio::spawn(async move { let t0=std::time::Instant::now(); if let Ok((client, mut stream))=PubsubClient::signature_subscribe(&url, &sig, Some(RpcSignatureSubscribeConfig{ commitment: Some(solana_sdk::commitment_config::CommitmentConfig::processed()), encoding: Some(UiTransactionEncoding::Base64) })).await { let res=timeout(Duration::from_millis(1200), async { while let Some(_u)=stream.next().await { let _=txc.send(true); break; } }).await; let ms=t0.elapsed().as_millis() as f64; selc.observe(i, res.is_ok(), ms); drop(client); } else { selc.observe(i,false,1500.0); } }); }
    // Quick-Ack safety poller: hedged getSignatureStatuses at ~135ms cadence
    if let Some(hrpc) = QUICK_ACK_RPC.get() {
        let txc = tx.clone();
        let sigc = sig;
        let hrpc = hrpc.clone();
        tokio::spawn(async move {
            let mut ticks = 0u32;
            loop {
                if *txc.borrow() { break; }
                // poll a few RPC clients hedged
                let mut ok = false;
                for c in hrpc.clients_iter().take(hrpc.fanout()) {
                    if let Ok(res) = tokio::time::timeout(std::time::Duration::from_millis(200), c.get_signature_statuses(&[sigc.clone()])).await {
                        if let Ok(val) = res { if let Some(Some(_st)) = val.value.first() { ok = true; break; } }
                    }
                }
                if ok { let _=txc.send(true); break; }
                ticks += 1; if ticks > 20 { break; } // ~2.7s max
                tokio::time::sleep(std::time::Duration::from_millis(135)).await;
            }
        });
    }
    Ok(rx)
}

// ==============================
// PIECE 121 — build_min_bytes_fit_mtu_stable v4.kp-exact
//  - Exact max-coverage search (bitset) over up to 8 LUTs with strong B&B pruning
//  - Legacy/V0 crossover predictor to skip futile V0 paths
//  - Deterministic tiebreaker: smallest fully signed wire_len
// ==============================

fn sort_luts_canonical(mut luts: Vec<AddressLookupTableAccount>) -> Vec<AddressLookupTableAccount> {
    luts.sort_by(|a,b| a.key.to_bytes().cmp(&b.key.to_bytes()));
    luts
}

#[derive(Clone)]
struct Bitset { w: Vec<u64>, nbits: usize }
impl Bitset {
    fn new(nbits: usize) -> Self { let nw = (nbits + 63) >> 6; Self { w: vec![0u64; nw], nbits } }
    #[inline] fn set(&mut self, i: usize) { self.w[i >> 6] |= 1u64 << (i & 63); }
    #[inline] fn or_in(&mut self, other: &Bitset) { for (a,b) in self.w.iter_mut().zip(other.w.iter()) { *a |= *b; } }
    #[inline] fn and_not(&mut self, other: &Bitset) { for (a,b) in self.w.iter_mut().zip(other.w.iter()) { *a &= !*b; } }
    #[inline] fn clone_and_or(&self, other: &Bitset) -> Bitset { let mut out = self.clone(); out.or_in(other); out }
    #[inline] fn count(&self) -> usize { self.w.iter().map(|x| x.count_ones() as usize).sum() }
    #[inline] fn diff_count_with(&self, other: &Bitset) -> usize { let mut c=0usize; for (a,b) in self.w.iter().zip(other.w.iter()) { c += ((!a) & b).count_ones() as usize; } c }
}

#[inline]
pub fn wire_len<V: Into<VersionedTransaction> + Clone>(v: &V) -> usize {
    let vt: VersionedTransaction = v.clone().into();
    let sz = bincode::serialized_size(&vt).unwrap_or(1500_u64);
    (sz as usize).min(16 * 1024)
}

struct KnapsackCtx<'a> { payer: &'a Keypair, bh: Hash, instrs_norm: &'a [Instruction], all_luts_sorted: &'a [AddressLookupTableAccount], mtu: &'a MtuLearner }

fn build_legacy_signed(payer: &Keypair, bh: Hash, instrs_norm: &[Instruction]) -> VersionedTransaction {
    let legacy_msg = solana_sdk::message::Message::new(instrs_norm, Some(&payer.pubkey()));
    let legacy_tx = solana_sdk::transaction::Transaction::new(&[payer], legacy_msg, bh);
    VersionedTransaction::from(legacy_tx)
}
fn build_v0_signed(payer: &Keypair, _bh: Hash, instrs_norm: &[Instruction], chosen: &[AddressLookupTableAccount]) -> VersionedTransaction {
    let v0 = V0Message::compile(&payer.pubkey(), instrs_norm, chosen);
    VersionedTransaction::try_new(VersionedMessage::V0(v0), &[payer]).expect("sign v0")
}

struct SearchState { best_cov: usize, best_len: usize, best_sel: Vec<usize> }

fn pick_optimal_luts_exact(
    ctx: &KnapsackCtx,
    cov_bits: &[Bitset],
    suffix_union: &[Bitset],
    max_pick: usize,
    legacy_len: usize,
) -> (Vec<usize>, usize) {
    let mut st = SearchState { best_cov: 0, best_len: usize::MAX, best_sel: Vec::new() };
    let mut chosen: Vec<usize> = Vec::with_capacity(max_pick);
    fn dfs(i: usize, picked: usize, covered: &Bitset, cov_bits: &[Bitset], suffix_union: &[Bitset], max_pick: usize, ctx: &KnapsackCtx, st: &mut SearchState, chosen: &mut Vec<usize>) {
        let ub_extra = covered.diff_count_with(&suffix_union[i]);
        if covered.count() + ub_extra < st.best_cov { return; }
        if picked == max_pick || i == cov_bits.len() {
            let selected: Vec<AddressLookupTableAccount> = chosen.iter().map(|&idx| ctx.all_luts_sorted[idx].clone()).collect();
            let vtx = build_v0_signed(ctx.payer, ctx.bh, ctx.instrs_norm, &selected);
            let len = wire_len(&vtx);
            let cov_now = covered.count();
            if cov_now > st.best_cov || (cov_now == st.best_cov && len < st.best_len) { st.best_cov = cov_now; st.best_len = len; st.best_sel = chosen.clone(); }
            return;
        }
        // include
        chosen.push(i);
        let with = covered.clone_and_or(&cov_bits[i]);
        dfs(i+1, picked+1, &with, cov_bits, suffix_union, max_pick, ctx, st, chosen);
        chosen.pop();
        // skip
        let ub_extra_skip = covered.diff_count_with(&suffix_union[i+1]);
        if covered.count() + ub_extra_skip < st.best_cov { return; }
        dfs(i+1, picked, covered, cov_bits, suffix_union, max_pick, ctx, st, chosen);
    }
    let covered0 = Bitset::new(suffix_union[0].nbits);
    dfs(0, 0, &covered0, cov_bits, suffix_union, max_pick, ctx, &mut st, &mut chosen);
    if st.best_sel.is_empty() && st.best_len >= legacy_len { return (Vec::new(), st.best_len); }
    (st.best_sel, st.best_len)
}

pub fn build_min_bytes_fit_mtu_stable(
    payer: &Keypair,
    bh: Hash,
    instrs: Vec<Instruction>,
    cu_price: u64,
    cu_limit: u32,
    all_luts: &[AddressLookupTableAccount],
    mtu: &MtuLearner,
) -> VersionedTransaction {
    let instrs_norm = normalize_budget(canonicalize_instructions(instrs), cu_limit, cu_price);
    let legacy_tx = build_legacy_signed(payer, bh, &instrs_norm);
    let legacy_len = wire_len(&legacy_tx);
    let need = required_pubkeys_for_instrs(&instrs_norm, &payer.pubkey());
    if all_luts.is_empty() || need.len() <= 1 { return legacy_tx; }
    use std::collections::HashMap; let mut idx_of: HashMap<Pubkey, usize> = HashMap::with_capacity(need.len());
    for (i,k) in need.iter().enumerate() { idx_of.insert(*k, i); }
    let mut luts = sort_luts_canonical(all_luts.to_vec());
    #[derive(Clone)] struct Cand { lut_idx: usize, score: f64, bits: Bitset, cover_cnt: usize }
    let mut cands: Vec<Cand> = Vec::with_capacity(luts.len());
    for (i, lut) in luts.iter().enumerate() {
        let mut bs = Bitset::new(need.len()); let mut gain = 0usize;
        for k in &lut.addresses { if let Some(&ix) = idx_of.get(k) { bs.set(ix); gain += 1; } }
        if gain == 0 { continue; }
        let dens = (gain as f64) / (lut.addresses.len().max(1) as f64);
        cands.push(Cand { lut_idx: i, score: dens, bits: bs, cover_cnt: gain });
    }
    if cands.is_empty() { return legacy_tx; }
    const MAX_CAND: usize = 24;
    cands.sort_by(|a,b| b.cover_cnt.cmp(&a.cover_cnt).then_with(|| b.score.partial_cmp(&a.score).unwrap()).then_with(|| luts[b.lut_idx].key.to_bytes().cmp(&luts[a.lut_idx].key.to_bytes())));
    if cands.len() > MAX_CAND { cands.truncate(MAX_CAND); }
    let mut union = Bitset::new(need.len()); for c in &cands { union.or_in(&c.bits); }
    let max_keys_movable = union.count(); let est_lut_overhead = 36usize * 8.min(cands.len()); let est_gain = 32usize.saturating_mul(max_keys_movable);
    if est_gain <= est_lut_overhead && legacy_len <= mtu.safe { return legacy_tx; }
    let cov_bits: Vec<Bitset> = cands.iter().map(|c| c.bits.clone()).collect();
    let lut_index_map: Vec<usize> = cands.iter().map(|c| c.lut_idx).collect();
    let m = cov_bits.len(); let mut suffix_union: Vec<Bitset> = (0..=m).map(|_| Bitset::new(need.len())).collect();
    for i in (0..m).rev() { suffix_union[i] = suffix_union[i+1].clone(); suffix_union[i].or_in(&cov_bits[i]); }
    let kmax = 8usize.min(m);
    let ctx = KnapsackCtx { payer, bh, instrs_norm: &instrs_norm, all_luts_sorted: &luts, mtu };
    let (best_sel_compact, best_v0_len) = pick_optimal_luts_exact(&ctx, &cov_bits, &suffix_union, kmax, legacy_len);
    if best_sel_compact.is_empty() && legacy_len <= mtu.safe { return legacy_tx; }
    let chosen: Vec<AddressLookupTableAccount> = best_sel_compact.iter().map(|&i| luts[lut_index_map[i]].clone()).collect();
    let v0_tx = build_v0_signed(payer, bh, &instrs_norm, &chosen);
    let v0_len = best_v0_len; let legacy_fits = legacy_len <= mtu.safe; let v0_fits = v0_len <= mtu.safe;
    match (v0_fits, legacy_fits) { (true,false)=>v0_tx, (false,true)=>legacy_tx, (true,true)=> if v0_len <= legacy_len { v0_tx } else { legacy_tx }, (false,false)=> if v0_len <= legacy_len { v0_tx } else { legacy_tx } }
}

// Conservative QUIC-safe ceiling helper (utility-only; MTU learner tightens at runtime)
#[inline]
pub fn assert_quic_safe<V: Into<VersionedTransaction> + Clone>(v: &V) -> bool {
    wire_len(v) <= 1232
}

// ==============================
// PIECE 73 — Pinned-core micro-RT + realtime hint
// ==============================
pub struct MicroRtPinned { rt: tokio::runtime::Runtime }
impl MicroRtPinned {
    pub fn new() -> Self { let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().expect("micro rt"); Self { rt } }
    pub fn start_thread(self, core_idx: Option<usize>, fifo_priority: Option<i32>) -> std::sync::Arc<Self> {
        let arc = std::sync::Arc::new(self); let runner = arc.clone(); let name = format!("tick-rt-core{:?}", core_idx);
        std::thread::Builder::new().name(name).spawn(move || {
            // Pin to a core if possible (requires core_affinity crate; ignore if unavailable)
            #[cfg(feature = "core_affinity")]
            if let Some(i) = core_idx { if let Some(cores) = core_affinity::get_core_ids() { if let Some(core) = cores.into_iter().find(|c| c.id == i) { let _ = core_affinity::set_for_current(core); } } }
            // Best-effort realtime (Linux only)
            #[cfg(target_os = "linux")]
            if let Some(prio) = fifo_priority { unsafe { let pthread = libc::pthread_self(); let mut sp: libc::sched_param = std::mem::zeroed(); sp.sched_priority = prio.clamp(1, 80); let _ = libc::pthread_setschedparam(pthread, libc::SCHED_FIFO, &sp); } }
            runner.rt.block_on(async { futures::future::pending::<()>().await; });
        }).expect("pinned micro rt thread");
        arc
    }
}

// ==============================
// PIECE 74 — WalletPool Balance Sentinel + auto top-up
// ==============================
pub struct BalanceSentinel { rpc: std::sync::Arc<solana_client::nonblocking::rpc_client::RpcClient>, hedged: std::sync::Arc<HedgedRpc>, treasury: std::sync::Arc<Keypair>, min_fee_buffer: u64, topup_amount: u64 }
impl BalanceSentinel {
    pub fn new(rpc: std::sync::Arc<solana_client::nonblocking::rpc_client::RpcClient>, hedged: std::sync::Arc<HedgedRpc>, treasury: std::sync::Arc<Keypair>, min_fee_buffer: u64, topup_amount: u64) -> Self { Self { rpc, hedged, treasury, min_fee_buffer, topup_amount } }
    pub async fn ensure(&self, payer: &solana_sdk::pubkey::Pubkey) -> Result<()> {
        let bal = tokio::time::timeout(std::time::Duration::from_millis(150), self.rpc.get_balance(payer)).await.map_err(|_| CounterStrategyError::RpcError("balance timeout".into()))?.map_err(|e| CounterStrategyError::RpcError(format!("get_balance: {e}")))?;
        if bal >= self.min_fee_buffer { return Ok(()); }
        let bh = self.hedged.get_latest_blockhash().await?;
        let ix = solana_sdk::system_instruction::transfer(&self.treasury.pubkey(), payer, self.topup_amount);
        let msg = solana_sdk::message::Message::new(&[ix], Some(&self.treasury.pubkey()));
        let tx = solana_sdk::transaction::Transaction::new(&[&*self.treasury], msg, bh);
        let raw = serialize_vtx(&tx.into());
        let _sig = hedged_send_raw(self.hedged.clone(), raw, None).await?;
        Ok(())
    }
}

// ==============================
// PIECE 75 — Tip receiver selection by hotness (CMS-weighted)
// ==============================
pub fn pick_tip_least_hot(tips: &[Pubkey], cms: &CountMin) -> Option<Pubkey> { if tips.is_empty() { return None; } let mut best=tips[0]; let mut best_c=cms.estimate(&best); for &t in &tips[1..] { let ct=cms.estimate(&t); if ct < best_c || (ct==best_c && t.to_bytes()<best.to_bytes()) { best=t; best_c=ct; } } Some(best) }

// ==============================
// PIECE 76 — Hedged RAW v2 (early-abort + backpressure)
// ==============================
pub async fn hedged_send_raw_v2(
    hedged: std::sync::Arc<HedgedRpc>,
    lanes: std::sync::Arc<HedgeBackpressure>,
    raw: Vec<u8>,
    cfg: Option<solana_client::rpc_config::RpcSendTransactionConfig>,
) -> Result<solana_sdk::signature::Signature> {
    let fan = hedged.fanout();
    let mut tasks: Vec<AbortableTask<anyhow::Result<solana_sdk::signature::Signature>>> = Vec::new();
    for (i, c) in hedged.clients_iter().take(fan).cloned().enumerate() {
        let url_key = format!("ep-{}", i);
        let (g, ep) = lanes.acquire_pair(&url_key).await?;
        let bytes = raw.clone(); let cfgc = cfg.clone(); let d = hedged.policy_deadline();
        let t = AbortableTask::spawn(async move {
            let _guards = (g, ep);
            let res = tokio::time::timeout(d, c.send_raw_transaction_with_config(&bytes, cfgc)).await;
            match res { Ok(Ok(sig)) => Ok(sig), Ok(Err(e)) => Err(anyhow::anyhow!(e)), Err(_) => Err(anyhow::anyhow!("raw timeout")) }
        });
        tasks.push(t);
    }
    let mut last_err: Option<String> = None;
    // Await tasks concurrently; first Ok wins, abort others
    for t in tasks.iter() { if let Some(res) = t.handle.now_or_never() { if let Some(Ok(sig)) = res { for tt in tasks.iter() { tt.abort(); } return Ok(sig); } }
    }
    // If none finished immediately, await sequentially
    for t in tasks { if let Some(res) = t.join().await { match res { Ok(sig) => return Ok(sig), Err(e) => last_err = Some(format!("{e}")) } } }
    Err(CounterStrategyError::RpcError(format!("raw_v2 failed: {last_err:?}")))
}

// ==============================
// PIECE 77 — MessageByteCache (blockhash-scoped)
// ==============================
#[derive(Hash, PartialEq, Eq, Clone)] pub struct MsgKey { pub route_hash: u64, pub cu_price: u64, pub cu_limit: u32, pub payer: Pubkey }
#[derive(Clone)] pub struct MsgCache { inner: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<MsgKey, Vec<u8>>>> }
impl MsgCache { pub fn new() -> Self { Self { inner: std::sync::Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::with_capacity(1024))) } }
    pub async fn get_or_insert(&self, k: MsgKey, builder: impl FnOnce() -> solana_sdk::message::VersionedMessage) -> Vec<u8> { if let Some(v)=self.inner.read().await.get(&k).cloned() { return v; } let msg = builder(); let bytes = bincode::serialize(&msg).expect("msg serialize"); self.inner.write().await.insert(k, bytes.clone()); bytes }
    pub async fn clear(&self) { self.inner.write().await.clear(); }
}

// ==============================
// PIECE 78 — Slot-boundary hedge (current ⟂ next leader)
// ==============================
pub async fn send_near_boundary(
    tpu_cur: std::sync::Arc<TpuFastPath>,
    rpc: std::sync::Arc<HedgedRpc>,
    clock: &ClusterClock,
    vtx_now: VersionedTransaction,
    vtx_next: VersionedTransaction,
) -> Result<solana_sdk::signature::Signature> {
    let now = (crate::clock::ClusterClock::now_ms_local() as i64 + clock.offset_ms) as u64;
    let to_next = delay_to_next_tick_ms(now, 2);
    let cur_sig = send_first_ack(tpu_cur.clone(), rpc.clone(), vtx_now, 6).await?;
    tokio::spawn({ let tpu=tpu_cur.clone(); async move { tokio::time::sleep(std::time::Duration::from_millis(to_next + 50)).await; let _ = tpu.send_versioned(vtx_next).await; } });
    Ok(cur_sig)
}

// PIECE 79 removed: superseded by assemble_ultra (118) + FastSignerAuto (122).

// ==============================
// PIECE 80 — Per-wallet lamport spend governor (local & global)
// ==============================
pub struct WalletSpendGov { global: SpendGovernor, local: std::sync::Mutex<std::collections::HashMap<solana_sdk::pubkey::Pubkey, SpendGovernor>>, per_wallet_daily: u64 }
impl WalletSpendGov { pub fn new(global_daily: u64, per_wallet_daily: u64) -> Self { Self { global: SpendGovernor::new(global_daily, 2.0), local: std::sync::Mutex::new(std::collections::HashMap::new()), per_wallet_daily } }
    pub fn allow(&self, wallet: &solana_sdk::pubkey::Pubkey, lamports: u64) -> bool { if !self.global.allow(lamports) { return false; } let mut m = self.local.lock().unwrap(); let e = m.entry(*wallet).or_insert_with(|| SpendGovernor::new(self.per_wallet_daily, 1.5)); e.allow(lamports) }
}

// ==============================
// PIECE 81 — Dynamic Hedge Fanout Controller (adaptive N)
// ==============================
#[derive(Clone)]
pub struct FanoutCtrl { alpha: f64, succ: f64, lat_ms: f64, mcost: f64, min_n: usize, max_n: usize }
impl FanoutCtrl {
    pub fn new(min_n: usize, max_n: usize, mcost: f64) -> Self { Self { alpha: 0.2, succ: 0.9, lat_ms: 120.0, mcost, min_n: min_n.max(1), max_n: max_n.max(min_n) } }
    pub fn observe(&mut self, ok: bool, first_ack_ms: f64) { let a=self.alpha; self.succ=(1.0-a)*self.succ + a*if ok{1.0}else{0.0}; self.lat_ms=(1.0-a)*self.lat_ms + a*first_ack_ms.max(1.0); }
    pub fn select(&self) -> usize { let heat = (1.0 - self.succ).clamp(0.0,1.0) + (self.lat_ms/200.0).clamp(0.0,1.0); let desired = (self.min_n as f64 + heat * (self.max_n - self.min_n) as f64).round() as usize; desired.clamp(self.min_n, self.max_n) }
}
pub type FanoutShared = std::sync::Arc<std::sync::Mutex<FanoutCtrl>>;

// ==============================
// PIECE 82 — Signature PubSub Abort (fast ladder cancel on Processed)
// ==============================
pub async fn subscribe_abort_on_processed(ws_url: String, sig: solana_sdk::signature::Signature) -> Result<tokio::sync::watch::Receiver<bool>> {
    use tokio::sync::watch; use solana_client::nonblocking::pubsub_client::PubsubClient; use solana_client::rpc_config::RpcSignatureSubscribeConfig; use solana_transaction_status::UiTransactionEncoding; use futures::StreamExt;
    let (tx, rx) = watch::channel(false);
    let (client, mut stream) = PubsubClient::signature_subscribe(&ws_url, &sig, Some(RpcSignatureSubscribeConfig { commitment: Some(solana_sdk::commitment_config::CommitmentConfig::processed()), encoding: Some(UiTransactionEncoding::Base64) })).await?;
    tokio::spawn(async move { while let Some(_update) = stream.next().await { let _ = tx.send(true); break; } drop(client); });
    Ok(rx)
}

// ==============================
// PIECE 83 — TPU↔RPC budget auto-tuner (ms head-start)
// ==============================
#[derive(Clone)]
pub struct AcknowledgementBudget { alpha: f64, tpu_win: f64, rpc_win: f64, pub ms: u64 }
impl AcknowledgementBudget { pub fn new() -> Self { Self { alpha: 0.2, tpu_win: 0.5, rpc_win: 0.5, ms: 6 } } pub fn record_tpu(&mut self, _dt_ms: f64) { let a=self.alpha; self.tpu_win=(1.0-a)*self.tpu_win + a*1.0; self.rpc_win=(1.0-a)*self.rpc_win; self.recalc(); } pub fn record_rpc(&mut self, _dt_ms: f64) { let a=self.alpha; self.rpc_win=(1.0-a)*self.rpc_win + a*1.0; self.tpu_win=(1.0-a)*self.tpu_win; self.recalc(); } fn recalc(&mut self) { let bias=(self.tpu_win - self.rpc_win).clamp(-0.8,0.8); self.ms = (6.0 + 6.0*bias).clamp(0.0, 10.0) as u64; } }

// ==============================
// PIECE 124 — CuEwma + optimize_cu_limit (quantile-guarded CU)
// ==============================
pub struct CuEwma {
        Self { alpha: alpha.clamp(0.05, 0.5), map: parking_lot::Mutex::new(std::collections::HashMap::with_capacity(1024)), floor }
    }
            // EWMA mean/var
            let a = self.alpha;
            let prev_mu = *mu;
            *mu  = (1.0 - a) * (*mu) + a * x;
            let dx = x - prev_mu;
            *var = (1.0 - a) * (*var) + a * dx * dx;
        }).or_insert((x, x.max(1.0) * 0.10));
    }
    pub fn predict_q(&self, route_id: u64, z: f64) -> u32 {
        let m = self.map.lock();
        if let Some((mu, var)) = m.get(&route_id) {
            let sigma = var.max(&0.0).sqrt();
            let q = (*mu + z * sigma).max(self.floor as f64);
            q.ceil() as u32
        } else {
            self.floor.max(150_000)
        }
    }
}

// ==============================
// PIECE 85 — Raw-path price ladder (abort on signature seen)
// ==============================
pub struct RawLadderPlan { pub prices: Vec<u64>, pub safety_ms: u64 }
pub async fn send_raw_ladder(
    tpu: std::sync::Arc<TpuFastPath>,
    rpc: std::sync::Arc<HedgedRpc>,
    ws_url: String,
    build: impl Fn(u64, Hash) -> VersionedTransaction + Clone + Send + 'static,
    bh: Hash,
    plan: RawLadderPlan,
) -> Result<solana_sdk::signature::Signature> {
    if plan.prices.is_empty() { return Err(CounterStrategyError::RpcError("empty price ladder".into())); }
    let v1 = build(plan.prices[0], bh); let sig1 = v1.signatures()[0];
    let abort_rx = subscribe_abort_on_processed(ws_url.clone(), sig1).await?;
    let s = send_first_ack(tpu.clone(), rpc.clone(), v1, 6).await?;
    if *abort_rx.borrow() { return Ok(s); }
    for &p in plan.prices.iter().skip(1) { if *abort_rx.borrow() { break; } tokio::time::sleep(std::time::Duration::from_millis(plan.safety_ms.min(12))).await; let v = build(p, rpc.get_latest_blockhash().await?); let _ = tpu.send_versioned(v).await; if *abort_rx.borrow() { break; } }
    Ok(s)
}

// ==============================
// PIECE 86 — Jito endpoint health ordering (EWMA + latency)
// ==============================
#[derive(Clone)] pub struct JitoHealth { alpha: f64, lat_ms: Vec<f64>, succ: Vec<f64> }
impl JitoHealth { pub fn new(n: usize) -> Self { Self { alpha: 0.2, lat_ms: vec![120.0; n], succ: vec![0.9; n] } } pub fn observe(&mut self, idx: usize, dt_ms: f64, ok: bool) { let a=self.alpha; self.lat_ms[idx]=(1.0-a)*self.lat_ms[idx] + a*dt_ms.max(1.0); self.succ[idx]=(1.0-a)*self.succ[idx] + a*if ok{1.0}else{0.0}; } pub fn order(&self) -> Vec<usize> { let mut idx: Vec<usize>=(0..self.lat_ms.len()).collect(); idx.sort_by(|&i,&j| { let si=self.succ[i] - 0.002*self.lat_ms[i]; let sj=self.succ[j] - 0.002*self.lat_ms[j]; sj.partial_cmp(&si).unwrap() }); idx } }
impl JitoClient {
    pub async fn post_hedged_health(&self, path: &str, body: serde_json::Value, health: std::sync::Arc<std::sync::Mutex<JitoHealth>>) -> Result<(String, reqwest::header::HeaderMap)> {
        use tokio::{task::JoinSet, time::{timeout, sleep}}; let mut set=JoinSet::new(); let order={ health.lock().unwrap().order() };
        for (rank, &i) in order.iter().take(self.fanout).enumerate() { let ep=&self.endpoints[i]; let url=format!("{ep}/{path}"); let client=self.http.clone(); let headers=self.headers(); let payload=body.clone(); let d=self.req_deadline; let j=self.jitter.mul_f32((rank as f32)/(self.fanout.max(1) as f32)); let health=health.clone(); set.spawn(async move { if !j.is_zero() { sleep(j).await; } let t0=std::time::Instant::now(); let res=timeout(d, async { let resp = client.post(&url).headers(headers).json(&payload).send().await?; let status=resp.status(); let hdrs=resp.headers().clone(); let txt=resp.text().await?; anyhow::Ok((status, txt, hdrs)) }).await; let dt=t0.elapsed().as_millis() as f64; match res { Ok(Ok((status, txt, hdrs))) => { health.lock().unwrap().observe(i, dt, status.is_success()); Ok::<_, anyhow::Error>((status, txt, hdrs)) } _ => { health.lock().unwrap().observe(i, dt, false); Err(anyhow::anyhow!("post failed")) } } }); }
        while let Some(res) = set.join_next().await { if let Ok(Ok((status, txt, hdrs))) = res { if status.is_success() { return Ok((txt, hdrs)); } } }
        Err(CounterStrategyError::RpcError("jito: all hedged POSTs failed".into()))
    }
}

// ==============================
// PIECE 87 — Bundle payload splitter (size-safe, order-stable)
// ==============================
pub struct SplitBundle { pub payload_a: VersionedTransaction, pub payload_b: VersionedTransaction }
pub fn build_split_payload(payer: &Keypair, bh: Hash, instrs: Vec<Instruction>, cu_price: u64, cu_limit_each: u32, alts: &[AddressLookupTableAccount]) -> SplitBundle {
    let canon = canonicalize_instructions(instrs); let n=canon.len(); let mid=(n/2).max(1); let (left,right)=(canon[..mid].to_vec(), canon[mid..].to_vec()); let a = build_min_bytes(payer, bh, left, cu_price, cu_limit_each, alts); let b = build_min_bytes(payer, bh, right, cu_price, cu_limit_each, alts); SplitBundle { payload_a: a, payload_b: b }
}

// ==============================
// PIECE 88 — Deadline-Aware Fanout (tick-budget N, probability guarantee)
// ==============================
#[derive(Clone, Default)]
pub struct EpStats { mu: f64, sigma: f64, succ: f64, alpha: f64 }
impl EpStats {
    pub fn new() -> Self { Self { mu: (120.0f64).ln(), sigma: 0.45, succ: 0.9, alpha: 0.2 } }
    pub fn observe(&mut self, ok: bool, lat_ms: f64) { let a=self.alpha; let x=lat_ms.max(1.0).ln(); let prev_mu=self.mu; self.mu=(1.0-a)*self.mu + a*x; let dev=(x-prev_mu).abs(); self.sigma=((1.0-a)*self.sigma + a*dev).clamp(0.15,1.0); self.succ=(1.0-a)*self.succ + a*if ok{1.0}else{0.0}; }
    pub fn p_before(&self, t_ms: f64) -> f64 { let z=((t_ms.max(1.0).ln() - self.mu) / (self.sigma.max(1e-6))); let cdf = 0.5*(1.0 + statrs::function::erf::erf(z / std::f64::consts::SQRT_2)); (cdf * self.succ).clamp(0.0,1.0) }
}
#[derive(Clone)]
pub struct DeadlineFanout { eps_target: f64, eps_hyster: f64, min_n: usize, max_n: usize, pub stats: Vec<EpStats> }
impl DeadlineFanout {
    pub fn new(num_endpoints: usize, min_n: usize, max_n: usize, eps_target: f64) -> Self { Self { eps_target: eps_target.clamp(0.01,0.20), eps_hyster: 0.02, min_n, max_n: max_n.max(min_n), stats: (0..num_endpoints).map(|_| EpStats::new()).collect() } }
    pub fn observe(&mut self, idx: usize, ok: bool, lat_ms: f64) { if let Some(s)=self.stats.get_mut(idx) { s.observe(ok, lat_ms); } }
    pub fn plan(&self, budget_ms: u64) -> (Vec<usize>, usize) {
        let t = budget_ms as f64; let mut idx: Vec<usize> = (0..self.stats.len()).collect();
        idx.sort_by(|&i,&j| { let si = self.stats[i].p_before(t) - 0.002*self.stats[i].mu.exp(); let sj = self.stats[j].p_before(t) - 0.002*self.stats[j].mu.exp(); sj.partial_cmp(&si).unwrap() });
        let mut prod_none = 1.0; let mut n=0usize; for &i in &idx { if n>=self.max_n { break; } n+=1; prod_none *= 1.0 - self.stats[i].p_before(t); if n>=self.min_n && prod_none <= (self.eps_target - self.eps_hyster) { break; } }
        n = n.clamp(self.min_n, self.max_n); (idx, n)
    }
}

// ==============================
// PIECE 89 — Ladder Pre-Signer (FastSigner + MsgCache)
// ==============================
pub async fn presign_ladder_variants(
    payer: std::sync::Arc<Keypair>,
    bh: Hash,
    instrs: Vec<Instruction>,
    alts: &[AddressLookupTableAccount],
    rung_params: &[u64],
    cu_limit: u32,
    _route_hash: u64,
    variant_fn: impl Fn(&mut Vec<Instruction>, u64) + Copy,
) -> Vec<VersionedTransaction> {
    let base = canonicalize_instructions(instrs); let mut out=Vec::with_capacity(rung_params.len());
    for &param in rung_params { let mut v = base.clone(); variant_fn(&mut v, param); let vtx = build_min_bytes(&payer, bh, v, 0, cu_limit, alts); out.push(vtx); }
    out
}

// ==============================
// PIECE 90 — Bundle Sig Index (bundle-id → signatures)
// ==============================
#[derive(Default, Clone)]
pub struct BundleSigIndex { inner: std::sync::Arc<std::sync::RwLock<std::collections::HashMap<String, Vec<solana_sdk::signature::Signature>>>> }
impl BundleSigIndex { pub fn new() -> Self { Self::default() } pub fn insert(&self, bundle_id: String, sigs: Vec<solana_sdk::signature::Signature>) { self.inner.write().unwrap().insert(bundle_id, sigs); } pub fn sigs_for(&self, bundle_id: &str) -> Vec<solana_sdk::signature::Signature> { self.inner.read().unwrap().get(bundle_id).cloned().unwrap_or_default() } }

// ==============================
// PIECE 91 — Safety-ms Auto-Tuner (tick boundary skid control)
// ==============================
#[derive(Clone)] pub struct SafetyTuner { med_skid: P2Quantile, p95_skid: P2Quantile, pub safety_ms: u64 }
impl SafetyTuner { pub fn new() -> Self { Self { med_skid: P2Quantile::new(0.5), p95_skid: P2Quantile::new(0.95), safety_ms: 3 } } pub fn observe(&mut self, scheduled_at_ms: u64, processed_at_ms: u64) { let skid = processed_at_ms.saturating_sub(scheduled_at_ms) as f64; self.med_skid.observe(skid); self.p95_skid.observe(skid); let p95 = self.p95_skid.quantile().unwrap_or(2.0).clamp(1.0, 10.0); self.safety_ms = p95.round() as u64; } }

// ==============================
// PIECE 92 — Dual-Venue Hedge (bundle ⟂ raw) with budget guard
// ==============================
pub async fn hedge_bundle_and_raw(
    jito: std::sync::Arc<JitoClient>,
    tpu: std::sync::Arc<TpuFastPath>,
    rpc: std::sync::Arc<HedgedRpc>,
    ws_url: String,
    b64_bundle: (String, String),
    vtx_raw: VersionedTransaction,
) -> anyhow::Result<solana_sdk::signature::Signature> {
    let _bid = jito.send_bundle_base64(&[b64_bundle.0.clone(), b64_bundle.1.clone()]).await?;
    let payload_sig = vtx_raw.signatures()[0];
    let abort_rx = subscribe_abort_on_processed(ws_url, payload_sig).await?;
    let raw_task = tokio::spawn({ let tpu=tpu.clone(); let rpc=rpc.clone(); let v=vtx_raw.clone(); async move { send_first_ack(tpu, rpc, v, 4).await } });
    if *abort_rx.borrow() { let _ = raw_task.abort(); return Ok(payload_sig); }
    match raw_task.await { Ok(Ok(sig)) => Ok(sig), _ => Ok(payload_sig) }
}

// ==============================
// PIECE 52 — Speculative Blockhash Prefetcher (lock-free reads)
// ==============================
#[derive(Clone)]
pub struct BlockhashCache {
    bh: std::sync::Arc<tokio::sync::RwLock<Hash>>, last_update: std::sync::Arc<tokio::sync::RwLock<std::time::Instant>>, max_age: std::time::Duration, rpc: std::sync::Arc<HedgedRpc>
}
impl BlockhashCache {
    pub async fn new(rpc: std::sync::Arc<HedgedRpc>) -> Result<Self> { let h = rpc.get_latest_blockhash().await?; Ok(Self { bh: std::sync::Arc::new(tokio::sync::RwLock::new(h)), last_update: std::sync::Arc::new(tokio::sync::RwLock::new(std::time::Instant::now())), max_age: std::time::Duration::from_millis(400), rpc }) }
    pub async fn run(self) { let this=self.clone(); tokio::spawn(async move { loop { if let Ok(h) = this.rpc.get_latest_blockhash().await { *this.bh.write().await = h; *this.last_update.write().await = std::time::Instant::now(); } tokio::time::sleep(std::time::Duration::from_millis(150)).await; } }); }
    pub async fn get_fresh(&self) -> Hash { let age = self.last_update.read().await.elapsed(); if age > self.max_age { let rpc=self.rpc.clone(); let bh=self.bh.clone(); let ts=self.last_update.clone(); tokio::spawn(async move { if let Ok(h) = rpc.get_latest_blockhash().await { *bh.write().await = h; *ts.write().await = std::time::Instant::now(); } }); } *self.bh.read().await }
}

// ==============================
// PIECE 53 — Spend Governor (lamports/sec token-bucket)
// ==============================
pub struct SpendGovernor { capacity: u64, refill_per_sec: f64, tokens: core::sync::atomic::AtomicU64, last: core::sync::atomic::AtomicU64 }
impl SpendGovernor {
    pub fn new(daily_budget_lamports: u64, capacity_mult: f64) -> Self { let per_sec = (daily_budget_lamports as f64)/(24.0*3600.0); let cap = (per_sec*3600.0*capacity_mult).round() as u64; let now_ms = now_millis(); Self { capacity: cap.max(daily_budget_lamports/24), refill_per_sec: per_sec, tokens: core::sync::atomic::AtomicU64::new(cap), last: core::sync::atomic::AtomicU64::new(now_ms) } }
    #[inline] fn refill(&self) { let now = now_millis(); let last = self.last.swap(now, core::sync::atomic::Ordering::Relaxed); let dt = now.saturating_sub(last) as f64 / 1000.0; if dt <= 0.0 { return; } let add = (dt * self.refill_per_sec).round() as u64; if add > 0 { let prev = self.tokens.load(core::sync::atomic::Ordering::Relaxed); let newv = (prev.saturating_add(add)).min(self.capacity); self.tokens.store(newv, core::sync::atomic::Ordering::Relaxed); } }
    pub fn allow(&self, lamports: u64) -> bool { self.refill(); loop { let cur = self.tokens.load(core::sync::atomic::Ordering::Relaxed); if cur < lamports { return false; } let nxt = cur - lamports; if self.tokens.compare_exchange(cur, nxt, core::sync::atomic::Ordering::Relaxed, core::sync::atomic::Ordering::Relaxed).is_ok() { return true; } } }
}
#[inline] fn now_millis() -> u64 { let d = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default(); d.as_millis() as u64 }

// ==============================
// PIECE 54 — ALT-aware min-size builder (v0 vs legacy by bytes)
// ==============================
pub fn build_min_bytes(
    payer: &Keypair,
    bh: Hash,
    mut instrs: Vec<Instruction>,
    cu_price: u64,
    cu_limit: u32,
    all_luts: &[AddressLookupTableAccount],
) -> VersionedTransaction {
    // Redirect legacy builder to stable MTU-fitting builder (PIECE 121)
    let v = canonicalize_instructions(instrs);
    let mtu = default_mtu();
    build_min_bytes_fit_mtu_stable(payer, bh, v, cu_price, cu_limit, all_luts, mtu)
}

// ==============================
// PIECE 55 — Tick-critical micro-runtime (single-thread current-thread)
// ==============================
pub struct MicroRuntime { rt: tokio::runtime::Runtime }
impl MicroRuntime {
    pub fn new() -> Self { let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().expect("micro runtime"); Self { rt } }
    pub fn spawn<F>(&self, fut: F) where F: std::future::Future<Output=()> + 'static { self.rt.spawn(fut); }
    pub fn start_thread(self) -> std::sync::Arc<Self> { let arc = std::sync::Arc::new(self); let runner = arc.clone(); std::thread::Builder::new().name("tick-micro-rt".into()).spawn(move || { runner.rt.block_on(async { futures::future::pending::<()>().await; }); }).expect("micro rt thread"); arc }
}

// ==============================
// PIECE 20 — Auto-CU tuner (simulate → exact CU limit)
// ==============================
async fn hedged_simulate_vtx(
    rpc: Arc<HedgedRpc>,
    vtx: &VersionedTransaction,
    deadline: Duration,
) -> Result<RpcSimulateTransactionResult> {
    use solana_client::rpc_config::RpcSimulateTransactionConfig;
    let cfg = RpcSimulateTransactionConfig {
        replace_recent_blockhash: true,
        sig_verify: false,
        encoding: None,
        commitment: Some(rpc.commitment()),
        ..Default::default()
    };
    let mut set = JoinSet::new();
    let fan = rpc.fanout();
    for (i, c) in rpc.clients_iter().take(fan).cloned().enumerate() {
        let d = deadline;
        let v = vtx.clone();
        let cfgc = cfg.clone();
        let j = rpc.start_jitter_for(i, fan);
        set.spawn(async move {
            if !j.is_zero() { sleep(j).await; }
            timeout(d, c.simulate_transaction_with_config(&v, cfgc)).await
        });
    }
    while let Some(res) = set.join_next().await {
        if let Ok(Ok(Ok(sim))) = res { return Ok(sim.value); }
    }
    Err(CounterStrategyError::RpcError("simulate vtx: all attempts timed out/failed".into()))
}

pub async fn tune_cu_limit(
    rpc: Arc<HedgedRpc>,
    build_with: impl Fn(Hash, u64, u32) -> VersionedTransaction,
    generous_cu_limit: u32,
    cu_price: u64,
    safety_margin: f64,
    min_ceiling: u32,
) -> Result<(u32, u64)> {
    let bh = rpc.get_latest_blockhash().await?;
    let vtx = build_with(bh, cu_price, generous_cu_limit);
    let sim = hedged_simulate_vtx(rpc.clone(), &vtx, Duration::from_millis(150)).await?;
    let used = sim.units_consumed.unwrap_or(generous_cu_limit as u64).min(generous_cu_limit as u64);
    let target = ((used as f64) * safety_margin).ceil() as u32;
    let cu_limit = target.max(min_ceiling).min(generous_cu_limit);
    Ok((cu_limit, used))
}

// ==============================
// PIECE 21 — Adaptive Fee Controller (logistic model)
// ==============================
#[derive(Clone)]
pub struct InclusionModel { w: [f64; 6], lr: f64 }
impl InclusionModel {
    pub fn new() -> Self { Self { w: [0.0; 6], lr: 0.02 } }
    #[inline] fn sigm(x: f64) -> f64 { 1.0 / (1.0 + (-x).exp()) }
    fn phi(&self, f: &[f64;5]) -> [f64;6] { [1.0, f[0], f[1], f[2], f[3], f[4]] }
    pub fn predict(&self, f: &[f64;5]) -> f64 { let x=self.phi(f); let z=self.w.iter().zip(x.iter()).map(|(w,xi)| w*xi).sum::<f64>(); Self::sigm(z).clamp(0.0001,0.9999) }
    pub fn update(&mut self, f: &[f64;5], success: bool) { let y= if success {1.0} else {0.0}; let p=self.predict(f); let err=y-p; let x=self.phi(f); for i in 0..self.w.len() { self.w[i] += self.lr * err * x[i]; } }
}

pub fn price_for_target(
    p50: u64, p75: u64, p95: u64,
    notional_lamports: u64,
    congestion_0_1: f64,
    leader_score_z: f64,
    conflict_risk_0_1: f64,
    model: &InclusionModel,
    target_prob: f64,
    price_cap: u64,
) -> u64 {
    let base = if target_prob < 0.5 { p50 } else if target_prob < 0.85 { p75 } else { p95 };
    let mut lo = (0.5 * base as f64) as u64;
    let mut hi = (2.5 * base as f64).round() as u64;
    hi = hi.min(price_cap).max(base / 2);
    for _ in 0..18 {
        let mid = ((lo + hi) / 2).max(1);
        let nv = (notional_lamports as f64).ln_1p();
        let price_z = ((mid as f64) / (p75.max(1) as f64)).ln();
        let feats = [nv, congestion_0_1, leader_score_z, conflict_risk_0_1, price_z];
        let p = model.predict(&feats);
        if p < target_prob { lo = mid + 1; } else { hi = mid; }
    }
    hi.min(price_cap)
}

// ==============================
// PIECE 22 — Slot-Phase Scheduler (phase-aligned send)
// ==============================
pub fn delay_to_leader_ms(
    net: &NetworkConditions,
    slot_delta: usize,
    phase_ms_in_slot: (u64, u64),
    lds_offset_ms: i64,
) -> i64 {
    let base = (slot_delta as f64 * net.average_slot_time_ms).round() as i64;
    let phase = ((phase_ms_in_slot.0 + phase_ms_in_slot.1) / 2) as i64;
    (base + phase + lds_offset_ms).max(0)
}

// ==============================
// PIECE 24 — TxBytes cache + raw sender
// ==============================
pub fn serialize_vtx(vtx: &VersionedTransaction) -> Vec<u8> { bincode::serialize(vtx).expect("serialize vtx") }

pub async fn hedged_send_raw(
    hedged: Arc<HedgedRpc>,
    raw: Vec<u8>,
    cfg: Option<RpcSendTransactionConfig>,
) -> Result<Signature> {
    let mut set = JoinSet::new();
    let fan = hedged.fanout();
    for (i, c) in hedged.clients_iter().take(fan).cloned().enumerate() {
        let d = hedged.policy_deadline();
        let j = hedged.start_jitter_for(i, fan);
        let bytes = raw.clone();
        let cfgc = cfg.clone();
        set.spawn(async move {
            if !j.is_zero() { sleep(j).await; }
            timeout(d, c.send_raw_transaction_with_config(&bytes, cfgc)).await
        });
    }
    while let Some(res) = set.join_next().await {
        if let Ok(Ok(Ok(sig))) = res { return Ok(sig); }
    }
    Err(CounterStrategyError::RpcError("all hedged raw sends failed".into()))
}

// ==============================
// PIECE 14 — ALT cache + v0 message builder
// ==============================
pub struct AltCache {
    ttl: Duration,
    entries: HashMap<Pubkey, (AddressLookupTableAccount, Instant)>,
}
impl AltCache {
    pub fn new(ttl: Duration) -> Self { Self { ttl, entries: HashMap::new() } }
    pub fn get_fresh(&self, key: &Pubkey) -> Option<AddressLookupTableAccount> {
        self.entries.get(key).and_then(|(acc, ts)| if ts.elapsed() < self.ttl { Some(acc.clone()) } else { None })
    }
    pub fn put(&mut self, key: Pubkey, acc: AddressLookupTableAccount) { self.entries.insert(key, (acc, Instant::now())); }
}

pub async fn fetch_alt(
    rpc: Arc<RpcClient>,
    cache: &mut AltCache,
    alt_key: Pubkey,
    deadline: Duration,
) -> Result<AddressLookupTableAccount> {
    if let Some(x) = cache.get_fresh(&alt_key) { return Ok(x); }
    let resp = timeout(deadline, rpc.get_address_lookup_table(&alt_key))
        .await.map_err(|_| CounterStrategyError::RpcError("get_address_lookup_table timeout".into()))?
        .map_err(|e| CounterStrategyError::RpcError(format!("get_address_lookup_table: {e}")))?;
    let data = resp.value.ok_or_else(|| CounterStrategyError::RpcError("ALT not found".into()))?;
    cache.put(alt_key, data.clone());
    Ok(data)
}

pub fn build_v0_tx(
    payer: &Keypair,
    recent_blockhash: Hash,
    mut instrs: Vec<Instruction>,
    cu_price: u64,
    cu_limit: u32,
    alts: &[AddressLookupTableAccount],
) -> VersionedTransaction {
    let mut v = Vec::with_capacity(instrs.len() + 2);
    v.extend_from_slice(&build_priority_budget(cu_price, cu_limit));
    v.append(&mut instrs);
    let msg = V0Message::compile(&payer.pubkey(), &v, alts);
    VersionedTransaction::try_new(VersionedMessage::V0(msg), &[payer]).expect("sign")
}

// ==============================
// PIECE 15 — Conflict-Aware Scheduler
// ==============================
#[derive(Clone, Copy)]
pub struct ConflictDecision { pub risk_0_1: f64, pub submit_now: bool, pub delay_ms: i64 }

pub fn estimate_conflict_risk(
    bloom: &BloomFilterCache,
    accounts_ro: &[Pubkey],
    accounts_rw: &[Pubkey],
) -> f64 {
    let mut hits = 0usize; let mut total = 0usize;
    for a in accounts_rw.iter() { total += 1; if bloom.might_contain_account(a) { hits += 1; } }
    for a in accounts_ro.iter() { total += 1; if bloom.might_contain_account(a) { hits += 1; } }
    if total == 0 { 0.0 } else { (hits as f64) / (total as f64) * 0.9 }
}

pub fn split_rw_sets(instrs: &[Instruction]) -> (Vec<Pubkey>, Vec<Pubkey>) {
    use std::collections::HashSet; let mut ro=HashSet::new(); let mut rw=HashSet::new();
    for ix in instrs { for meta in &ix.accounts { if meta.is_writable { rw.insert(meta.pubkey); } else { ro.insert(meta.pubkey); } } }
    (ro.into_iter().collect(), rw.into_iter().collect())
}

pub fn conflict_aware_decide(
    bloom: &BloomFilterCache,
    lds: &mut LdsSampler,
    instrs: &[Instruction],
    allowed_offset_ms: (i64, i64),
    risk_threshold: f64,
) -> ConflictDecision {
    let (ro, rw) = split_rw_sets(instrs);
    let r = estimate_conflict_risk(bloom, &ro, &rw);
    if r <= risk_threshold { ConflictDecision { risk_0_1: r, submit_now: true, delay_ms: 0 } }
    else { let delay = lds.next_offset_ms(allowed_offset_ms); ConflictDecision { risk_0_1: r, submit_now: false, delay_ms: delay } }
}

// ==============================
// PIECE 16 — Resubmitter (Transaction-based variant)
// ==============================
pub struct ResubmitPolicy { pub max_attempts: u8, pub attempt_deadline: Duration, pub inter_attempt_backoff: Duration }

pub async fn send_and_confirm_resilient_tx(
    rpc: Arc<HedgedRpc>,
    confirm_client: Arc<RpcClient>,
    mut tx_builder: impl FnMut(Hash) -> Transaction + Send,
    policy: ResubmitPolicy,
) -> Result<Signature> {
    let mut last_err: Option<String> = None;
    for _ in 0..policy.max_attempts {
        let bh = rpc.get_latest_blockhash().await?;
        let tx = tx_builder(bh);
        let sig = tx.signatures[0];
        let send_res = timeout(policy.attempt_deadline, rpc.send_tx(tx)).await;
        match send_res {
            Ok(Ok(s)) => {
                use solana_client::rpc_config::RpcSignatureStatusConfig;
                let cfg = RpcSignatureStatusConfig { search_transaction_history: true };
                let st = timeout(policy.attempt_deadline, confirm_client.get_signature_status_with_commitment(&s, cfg, CommitmentConfig::processed())).await;
                if let Ok(Ok(Some(status))) = st { if status.is_ok() { return Ok(s); } last_err = Some(format!("confirmed failure: {:?}", status.err)); }
                else { return Ok(s); }
            }
            Ok(Err(e)) => { last_err = Some(format!("send_tx: {e}")); }
            Err(_) => { last_err = Some("send_tx timeout".into()); }
        }
        sleep(policy.inter_attempt_backoff).await;
    }
    Err(CounterStrategyError::RpcError(format!("resubmit exhausted: {:?}", last_err)))
}

// ==============================
// PIECE 17 — Error-aware escalator
// ==============================
#[derive(Debug, Clone, Copy)]
pub struct Escalation { pub cu_price_bump: u64, pub cu_limit_delta: i32, pub delay_ms: i64, pub refetch_blockhash: bool }
pub fn classify_and_escalate(err_msg: &str) -> Escalation {
    let m = err_msg.to_ascii_lowercase();
    if m.contains("blockhashnotfound") || m.contains("blockhash not found") { return Escalation { cu_price_bump: 0, cu_limit_delta: 0, delay_ms: 5, refetch_blockhash: true }; }
    if m.contains("priority fe") || m.contains("fee too low") || m.contains("would exceed max cost") { return Escalation { cu_price_bump: 15_000, cu_limit_delta: 0, delay_ms: 0, refetch_blockhash: false }; }
    if m.contains("account in use") || m.contains("already in use") { return Escalation { cu_price_bump: 5_000, cu_limit_delta: 0, delay_ms: 20, refetch_blockhash: false }; }
    if m.contains("max compute units exceeded") || m.contains("computebudget exceeded") { return Escalation { cu_price_bump: 0, cu_limit_delta: 50_000, delay_ms: 0, refetch_blockhash: false }; }
    if m.contains("node is behind") || m.contains("slot was skipped") { return Escalation { cu_price_bump: 8_000, cu_limit_delta: 0, delay_ms: 10, refetch_blockhash: true }; }
    Escalation { cu_price_bump: 0, cu_limit_delta: 0, delay_ms: 0, refetch_blockhash: false }
}

// ==============================
// PIECE 18 — Validator scorecard
// ==============================
#[derive(Clone, Copy, Default)]
pub struct ValScore { succ: u64, fail: u64, avg_tip: f64 }
impl ValScore {
    pub fn record(&mut self, ok: bool, tip: u64) { if ok { self.succ += 1; } else { self.fail += 1; } if ok { let alpha=0.2; self.avg_tip = if self.avg_tip==0.0 { tip as f64 } else { (1.0-alpha)*self.avg_tip + alpha*(tip as f64) }; } }
    pub fn score(&self) -> f64 { let attempts=(self.succ+self.fail).max(1) as f64; let wr=(self.succ as f64)/attempts; wr - 0.0000005*self.avg_tip }
}
pub struct ValidatorScorecard { map: HashMap<Pubkey, ValScore> }
impl ValidatorScorecard {
    pub fn new() -> Self { Self { map: HashMap::new() } }
    pub fn record(&mut self, v: Pubkey, ok: bool, tip: u64) { self.map.entry(v).or_default().record(ok, tip); }
    pub fn best_of(&self, candidates: &[Pubkey], k: usize) -> Vec<Pubkey> { let mut v=candidates.iter().map(|c| (*c, self.map.get(c).map(|s| s.score()).unwrap_or(0.0))).collect::<Vec<_>>(); v.sort_by(|a,b| b.1.partial_cmp(&a.1).unwrap()); v.into_iter().take(k).map(|x| x.0).collect() }
    pub fn raw_score(&self, v: Pubkey) -> Option<f64> { self.map.get(&v).map(|s| s.score()) }
}

// ==============================
// PIECE 123 — SigRingTtl vFinal (dedupe/TTL, lock-light)
// ==============================
pub struct SigRingTtl {
    ttl_ms: u64,
    map: parking_lot::Mutex<std::collections::HashMap<solana_sdk::signature::Signature, u64>>,
    last_gc: core::sync::atomic::AtomicU64,
}
impl SigRingTtl {
    pub fn new(ttl_ms: u64) -> Self {
        Self {
            ttl_ms,
            map: parking_lot::Mutex::new(std::collections::HashMap::with_capacity(8192)),
            last_gc: core::sync::atomic::AtomicU64::new(now_ms()),
        }
    }
    #[inline] pub fn check_and_insert(&self, sig: solana_sdk::signature::Signature) -> bool {
        let t = now_ms();
        let last = self.last_gc.load(core::sync::atomic::Ordering::Relaxed);
        if t.wrapping_sub(last) > 2_000 {
            if self.last_gc.compare_exchange(last, t, core::sync::atomic::Ordering::Relaxed, core::sync::atomic::Ordering::Relaxed).is_ok() {
                let ttl = self.ttl_ms; let mut m = self.map.lock(); m.retain(|_, ts| t.wrapping_sub(*ts) <= ttl);
            }
        }
        let mut m = self.map.lock();
        if let Some(ts) = m.get(&sig) { if t.wrapping_sub(*ts) <= self.ttl_ms { return false; } }
        m.insert(sig, t);
        true
    }
}
#[inline] fn now_ms() -> u64 { let d = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default(); d.as_millis() as u64 }

// optimize_cu_limit wrapper compatible with existing CuEwma (PIECE 84)
pub struct CuPlan { pub cu_limit: u32 }
pub fn optimize_cu_limit(route_id: u64, ew: &CuEwma, q_tight: f64, floor: u32, cap: u32) -> Option<CuPlan> {
    // fallback using PIECE 84 predict() if available; otherwise clamp to bounds
    if let Some(pred) = ew.predict(route_id, (1.0 + (q_tight - 0.9).max(0.0) * 0.8), floor) {
        let cu = pred.clamp(floor.max(50_000), cap.max(floor));
        return Some(CuPlan { cu_limit: cu });
    }
    None
}

// ==============================
// PIECE 124 — Engine Pre-Warming + Pooled HTTP
// ==============================
pub fn jito_http_client() -> reqwest::Client { ClientBuilder::new().tcp_keepalive(std::time::Duration::from_secs(30)).pool_max_idle_per_host(16).pool_idle_timeout(std::time::Duration::from_secs(60)).http2_adaptive_window(true).http2_keep_alive_interval(std::time::Duration::from_secs(15)).http2_keep_alive_timeout(std::time::Duration::from_secs(30)).connect_timeout(std::time::Duration::from_millis(200)).timeout(std::time::Duration::from_millis(900)).gzip(false).brotli(false).deflate(false).build().expect("reqwest client") }
pub async fn warm_engine(client: &reqwest::Client, base: &str) -> anyhow::Result<()> { let paths=["/livez","/version","/api/v1/bundles"]; for p in &paths { let url=format!("{base}{p}"); let _ = client.get(&url).send().await; break; } Ok(()) }

// ==============================
// PIECE 9 — Compute-Budget + Tx builder
// ==============================
pub fn build_priority_budget(cu_price_lamports_per_cu: u64, cu_limit: u32) -> [Instruction; 2] {
    [
        ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
        ComputeBudgetInstruction::set_compute_unit_price(cu_price_lamports_per_cu),
    ]
}

/// Compose budget + payload; ALT use is left to the caller (attach message with lookups if you have LUTs).
pub fn build_tx(payer: &Keypair, recent_blockhash: Hash, mut instrs: Vec<Instruction>, cu_price: u64, cu_limit: u32) -> Transaction {
    let mut v = Vec::with_capacity(instrs.len() + 2);
    v.extend_from_slice(&build_priority_budget(cu_price, cu_limit));
    v.append(&mut instrs);
    let msg = Message::new(&v, Some(&payer.pubkey()));
    Transaction::new(&[payer], msg, recent_blockhash)
}

// ==============================
// PIECE 10 — LeaderScheduleTracker
// ==============================
pub struct LeaderScheduleTracker {
    client: Arc<RpcClient>,
    last_refresh: Instant,
    schedule: HashMap<Pubkey, Vec<usize>>, // validator -> slots within epoch
    refresh_interval: Duration,
}

impl LeaderScheduleTracker {
    pub fn new(client: Arc<RpcClient>) -> Self {
        Self { client, last_refresh: Instant::now() - Duration::from_secs(3600), schedule: HashMap::new(), refresh_interval: Duration::from_secs(60) }
    }
    pub async fn refresh_if_stale(&mut self) -> Result<()> {
        if self.last_refresh.elapsed() < self.refresh_interval { return Ok(()); }
        let ls = self.client
            .get_leader_schedule_with_commitment(None, CommitmentConfig::processed())
            .await
            .map_err(|e| CounterStrategyError::RpcError(format!("leader_schedule: {e}")))?;
        self.schedule.clear();
        for (k, v) in ls.unwrap_or_default() {
            let pk = Pubkey::from_str(&k).map_err(|e| CounterStrategyError::RpcError(format!("bad leader key: {e}")))?;
            self.schedule.insert(pk, v);
        }
        self.last_refresh = Instant::now();
        Ok(())
    }
    /// Return validators who lead soonest after `current_slot`, limited to `n`.
    pub fn next_leaders(&self, current_slot_in_epoch: usize, n: usize) -> Vec<Pubkey> {
        let mut best: Vec<(usize, Pubkey)> = Vec::with_capacity(n * 2);
        for (v, slots) in self.schedule.iter() {
            if let Some(delta) = slots.iter().filter(|s| **s >= current_slot_in_epoch).map(|s| *s - current_slot_in_epoch).min() {
                best.push((delta, *v));
            }
        }
        best.sort_unstable_by_key(|x| x.0);
        best.truncate(n);
        best.into_iter().map(|x| x.1).collect()
    }
}

// ==============================
// PIECE 11 — Streaming P² quantiles (standalone)
// ==============================
#[derive(Clone)]
pub struct P2Quantile {
    n: [i64; 5],
    q: [f64; 5],
    m: [f64; 5],
    dn: [f64; 5],
    count: i64,
    init: Vec<f64>,
}
impl P2Quantile {
    pub fn new(target_p: f64) -> Self { Self { n: [0;5], q: [0.0;5], m: [0.0;5], dn: [0.0;5], count: 0, init: Vec::with_capacity(5) }.with_p(target_p) }
    fn with_p(mut self, p: f64) -> Self { self.dn = [0.0, p/2.0, p, (1.0+p)/2.0, 1.0]; self }
    pub fn observe(&mut self, x: f64) {
        self.count += 1;
        if self.init.len() < 5 { self.init.push(x); if self.init.len() == 5 { self.init.sort_by(|a,b| a.partial_cmp(b).unwrap()); self.q.copy_from_slice(&[self.init[0], self.init[1], self.init[2], self.init[3], self.init[4]]); self.n = [1,2,3,4,5]; self.m = [1.0, 1.0 + 2.0*self.dn[1], 1.0 + 4.0*self.dn[2], 1.0 + 6.0*self.dn[3], 5.0]; } return; }
        let k = if x < self.q[0] { self.q[0] = x; 0 } else if x < self.q[1] { 0 } else if x < self.q[2] { 1 } else if x < self.q[3] { 2 } else { if x > self.q[4] { self.q[4] = x; } 3 };
        for i in (k+1)..5 { self.n[i] += 1; }
        for i in 0..5 { self.m[i] += self.dn[i]; }
        for i in 1..4 { let d = self.m[i] - self.n[i] as f64; if (d >= 1.0 && (self.n[i+1]-self.n[i]) > 1) || (d <= -1.0 && (self.n[i]-self.n[i-1]) > 1) { let d_sign = d.signum(); let qi = self.parabolic(i, d_sign as i64); let (q_prev, q_next) = (self.q[i-1], self.q[i+1]); self.q[i] = if qi > q_prev && qi < q_next { qi } else { self.linear(i, d_sign as i64) }; self.n[i] = (self.n[i] as f64 + d_sign) as i64; } }
    }
    #[inline] fn parabolic(&self, i: usize, d: i64) -> f64 {
        let d = d as f64;
        self.q[i] + d / ((self.n[i+1]-self.n[i-1]) as f64) * ( (self.n[i]-self.n[i-1]) as f64 * (self.q[i+1]-self.q[i]) / ((self.n[i+1]-self.n[i]) as f64) + (self.n[i+1]-self.n[i]) as f64 * (self.q[i]-self.q[i-1]) / ((self.n[i]-self.n[i-1]) as f64) )
    }
    #[inline] fn linear(&self, i: usize, d: i64) -> f64 { self.q[i] + (d as f64) * (self.q[i + d as usize] - self.q[i]) / ((self.n[i + d as usize] - self.n[i]) as f64) }
    pub fn quantile(&self) -> Option<f64> { if self.init.len() < 5 { None } else { Some(self.q[2]) } }
}

pub struct P2Triple { p50: P2Quantile, p75: P2Quantile, p95: P2Quantile }
impl P2Triple {
    pub fn new() -> Self { Self { p50: P2Quantile::new(0.5), p75: P2Quantile::new(0.75), p95: P2Quantile::new(0.95) } }
    pub fn observe(&mut self, x: u64) { let v = x as f64; self.p50.observe(v); self.p75.observe(v); self.p95.observe(v); }
    pub fn get(&self) -> Option<(u64,u64,u64)> { match (self.p50.quantile(), self.p75.quantile(), self.p95.quantile()) { (Some(a),Some(b),Some(c)) => Some((a.round() as u64, b.round() as u64, c.round() as u64)), _ => None } }
}

// ==============================
// PIECE 12 — Low-discrepancy timing offsets (Halton base-2)
// ==============================
pub struct LdsSampler { idx: u64 }
impl LdsSampler {
    pub fn new(seed: u64) -> Self { Self { idx: seed.max(1) } }
    #[inline] fn vdc_base2(mut n: u64) -> f64 { let mut inv=0.0f64; let mut denom=1.0f64; while n>0 { denom*=2.0; inv += ((n & 1) as f64) / denom; n >>= 1; } inv }
    pub fn next_offset_ms(&mut self, range: (i64, i64)) -> i64 { self.idx = self.idx.wrapping_add(1); let u = Self::vdc_base2(self.idx); let (lo,hi)=range; let span=(hi-lo) as f64; (lo as f64 + u*span).round() as i64 }
}

// ==============================
// PIECE 13 — Lock-free perf counters (atomics) with zero contention
// ==============================
#[derive(Default)]
pub struct PerfCounters {
    succ: AtomicU64,
    fail: AtomicU64,
    profit: AtomicU64,
    loss: AtomicU64,
    tips: AtomicU64,
    bundles: AtomicU64,
    bundles_ok: AtomicU64,
}
impl PerfCounters {
    pub fn inc_success(&self) { self.succ.fetch_add(1, Ordering::Relaxed); }
    pub fn inc_failure(&self) { self.fail.fetch_add(1, Ordering::Relaxed); }
    pub fn add_profit(&self, lamports: i64) { if lamports >= 0 { self.profit.fetch_add(lamports as u64, Ordering::Relaxed); } else { self.loss.fetch_add((-lamports) as u64, Ordering::Relaxed); } }
    pub fn add_tip(&self, lamports: u64) { self.tips.fetch_add(lamports, Ordering::Relaxed); }
    pub fn bundles_submitted(&self, ok: bool) { self.bundles.fetch_add(1, Ordering::Relaxed); if ok { self.bundles_ok.fetch_add(1, Ordering::Relaxed); } }
    pub fn snapshot(&self) -> PerformanceMetrics {
        let succ = self.succ.load(Ordering::Relaxed);
        let fail = self.fail.load(Ordering::Relaxed);
        let tp = self.profit.load(Ordering::Relaxed);
        let tl = self.loss.load(Ordering::Relaxed);
        let trades = (succ + fail).max(1);
        PerformanceMetrics {
            successful_counter_trades: succ,
            failed_counter_trades: fail,
            total_profit: (tp as i64) - (tl as i64),
            total_loss: tl as i64,
            average_response_time_ms: 0.0,
            sandwich_attacks_prevented: 0,
            frontrun_attempts_countered: 0,
            mev_opportunities_captured: succ,
            gas_efficiency_score: 0.0,
            win_rate: (succ as f64) / (trades as f64),
            profit_per_trade: if trades > 0 { ((tp as i64 - tl as i64) as f64) / (trades as f64) } else { 0.0 },
            validator_tips_paid: self.tips.load(Ordering::Relaxed),
            jito_bundles_submitted: self.bundles.load(Ordering::Relaxed),
            jito_bundles_successful: self.bundles_ok.load(Ordering::Relaxed),
            network_conditions: NetworkConditions::default(),
            last_updated: SystemTime::now(),
        }
    }
}

pub type Result<T> = std::result::Result<T, CounterStrategyError>;

// --- lock helpers: infallible acquire with deadline ---
use tokio::time::Duration as TokioDuration;

/// Hard timeouts stop hung tasks from starving the engine.
const LOCK_DEADLINE: TokioDuration = TokioDuration::from_millis(25);

async fn read_with_deadline<T>(l: &Arc<RwLock<T>>) -> Result<RwLockReadGuard<'_, T>> {
    match timeout(LOCK_DEADLINE, l.read()).await {
        Ok(g) => Ok(g),
        Err(_) => Err(CounterStrategyError::LockTimeout("RwLock.read".into())),
    }
}

async fn write_with_deadline<T>(l: &Arc<RwLock<T>>) -> Result<RwLockWriteGuard<'_, T>> {
    match timeout(LOCK_DEADLINE, l.write()).await {
        Ok(g) => Ok(g),
        Err(_) => Err(CounterStrategyError::LockTimeout("RwLock.write".into())),
    }
}

// ==============================
// Hedged, quorum-checked RPC (PIECE 8)
// ==============================
#[derive(Clone, Debug)]
pub struct HedgePolicy {
    pub fanout: usize,
    pub deadline: Duration,
    pub start_jitter: Duration,
}

pub struct HedgedRpc {
    clients: Vec<Arc<RpcClient>>,    
    commitment: CommitmentConfig,
    policy: HedgePolicy,
}

impl HedgedRpc {
    pub fn new(endpoints: Vec<String>, commitment: CommitmentConfig, policy: HedgePolicy) -> Self {
        let clients = endpoints
            .into_iter()
            .map(|e| Arc::new(RpcClient::new_with_commitment(e, commitment.clone())))
            .collect();
        Self { clients, commitment, policy }
    }

    pub async fn get_latest_blockhash(&self) -> Result<Hash> {
        let mut set = JoinSet::new();
        let fan = self.policy.fanout.min(self.clients.len());
        for (i, c) in self.clients.iter().take(fan).cloned().enumerate() {
            let d = self.policy.deadline;
            let j = self.policy.start_jitter.mul_f32((i as f32) / (fan as f32));
            set.spawn(async move {
                if !j.is_zero() { sleep(j).await; }
                timeout(d, c.get_latest_blockhash()).await
            });
        }
        while let Some(res) = set.join_next().await {
            if let Ok(Ok(Ok(h))) = res { return Ok(h); }
        }
        Err(CounterStrategyError::RpcError("all get_latest_blockhash attempts timed out/failed".into()))
    }

    pub async fn simulate_tx(&self, tx: Transaction) -> Result<RpcSimulateTransactionResult> {
        use solana_client::rpc_config::RpcSimulateTransactionConfig;
        let cfg = RpcSimulateTransactionConfig {
            replace_recent_blockhash: true,
            sig_verify: false,
            commitment: Some(self.commitment),
            encoding: None, // avoid extra dependency; default encoding is fine
            ..Default::default()
        };
        let mut set = JoinSet::new();
        let fan = self.policy.fanout.min(self.clients.len());
        for (i, c) in self.clients.iter().take(fan).cloned().enumerate() {
            let d = self.policy.deadline;
            let j = self.policy.start_jitter.mul_f32((i as f32) / (fan as f32));
            let txc = tx.clone();
            set.spawn(async move {
                if !j.is_zero() { sleep(j).await; }
                timeout(d, c.simulate_transaction_with_config(&txc, cfg.clone())).await
            });
        }
        while let Some(res) = set.join_next().await {
            if let Ok(Ok(Ok(sim))) = res { return Ok(sim.value); }
        }
        Err(CounterStrategyError::RpcError("all simulate_tx attempts timed out/failed".into()))
    }

    pub async fn send_tx(&self, tx: Transaction) -> Result<solana_sdk::signature::Signature> {
        let mut set = JoinSet::new();
        let fan = self.policy.fanout.min(self.clients.len());
        for (i, c) in self.clients.iter().take(fan).cloned().enumerate() {
            let d = self.policy.deadline;
            let j = self.policy.start_jitter.mul_f32((i as f32) / (fan as f32));
            let txc = tx.clone();
            set.spawn(async move {
                if !j.is_zero() { sleep(j).await; }
                timeout(d, c.send_transaction(&txc)).await
            });
        }
        while let Some(res) = set.join_next().await {
            if let Ok(Ok(Ok(sig))) = res { return Ok(sig); }
        }
        Err(CounterStrategyError::RpcError("all send_tx attempts timed out/failed".into()))
    }

    // === PIECE 23: helpers for versioned path (getters + vtx send) ===
    pub fn commitment(&self) -> CommitmentConfig { self.commitment }
    pub fn clients_iter(&self) -> std::slice::Iter<'_, Arc<RpcClient>> { self.clients.iter() }
    pub fn fanout(&self) -> usize { self.policy.fanout.min(self.clients.len()) }
    pub fn start_jitter_for(&self, i: usize, fan: usize) -> Duration {
        self.policy.start_jitter.mul_f32((i as f32) / (fan as f32))
    }
    pub fn policy_deadline(&self) -> Duration { self.policy.deadline }

    pub async fn send_versioned_tx(&self, vtx: VersionedTransaction) -> Result<Signature> {
        let mut set = JoinSet::new();
        let fan = self.fanout();
        for (i, c) in self.clients.iter().take(fan).cloned().enumerate() {
            let d = self.policy.deadline;
            let j = self.start_jitter_for(i, fan);
            let txc = vtx.clone();
            set.spawn(async move {
                if !j.is_zero() { sleep(j).await; }
                timeout(d, c.send_transaction(txc)).await
            });
        }
        while let Some(res) = set.join_next().await {
            if let Ok(Ok(Ok(sig))) = res { return Ok(sig); }
        }
        Err(CounterStrategyError::RpcError("all send_versioned_tx attempts timed out/failed".into()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CounterStrategyConfig {
    pub pattern_window_size: usize,
    pub confidence_threshold: f64,
    pub detection_sensitivity: f64,
    pub base_priority_multiplier: f64,
    pub max_priority_fee: u64,
    pub timing_offset_range_ms: (i64, i64),
    pub compute_unit_padding: u64,
    pub circuit_breaker_threshold: u32,
    pub circuit_breaker_reset_duration: Duration,
    pub max_tracked_wallets: usize,
    pub pattern_cache_size: usize,
    pub cleanup_interval: Duration,
    pub rpc_endpoints: Vec<String>,
    pub websocket_endpoint: String,
    pub jito_endpoint: String,
    pub ml_model_path: Option<String>,
    pub feature_extraction_window: usize,
    pub ml_confidence_threshold: f64,
    pub api_keys: ApiKeyConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyConfig {
    pub helius_api_key: Option<String>,
    pub quicknode_api_key: Option<String>,
    pub solana_fm_api_key: Option<String>,
    pub enable_fallback_on_missing_keys: bool,
}

impl ApiKeyConfig {
    pub fn from_env() -> Self {
        Self {
            helius_api_key: std::env::var("HELIUS_API_KEY").ok(),
            quicknode_api_key: std::env::var("QUICKNODE_API_KEY").ok(),
            solana_fm_api_key: std::env::var("SOLANA_FM_API_KEY").ok(),
            enable_fallback_on_missing_keys: std::env::var("ENABLE_API_FALLBACK")
                .map(|s| s == "1" || s.eq_ignore_ascii_case("true"))
                .unwrap_or(true),
        }
    }
    pub fn validate(&self) -> Result<()> {
        if !self.enable_fallback_on_missing_keys
            && (self.helius_api_key.is_none() || self.quicknode_api_key.is_none() || self.solana_fm_api_key.is_none())
        {
            return Err(CounterStrategyError::ConfigurationError(
                "Missing API keys and fallback disabled".into(),
            ));
        }
        Ok(())
    }

    pub fn redacted(&self) -> Self {
        fn r(x: &Option<String>) -> Option<String> { x.as_ref().map(|_| "****".to_string()) }
        Self {
            helius_api_key: r(&self.helius_api_key),
            quicknode_api_key: r(&self.quicknode_api_key),
            solana_fm_api_key: r(&self.solana_fm_api_key),
            enable_fallback_on_missing_keys: self.enable_fallback_on_missing_keys,
        }
    }
}

impl Default for CounterStrategyConfig {
    fn default() -> Self {
        Self {
            pattern_window_size: 96,
            confidence_threshold: 0.78,
            detection_sensitivity: 0.82,
            base_priority_multiplier: 1.4,
            max_priority_fee: 1_500_000,
            timing_offset_range_ms: (-120, 90),
            compute_unit_padding: 12_000,
            circuit_breaker_threshold: 5,
            circuit_breaker_reset_duration: Duration::from_secs(45),
            max_tracked_wallets: 20_000,
            pattern_cache_size: 4_096,
            cleanup_interval: Duration::from_secs(180),
            rpc_endpoints: vec!["https://api.mainnet-beta.solana.com".to_string()],
            websocket_endpoint: "wss://api.mainnet-beta.solana.com".to_string(),
            jito_endpoint: "https://mainnet.block-engine.jito.wtf".to_string(),
            ml_model_path: None,
            feature_extraction_window: 64,
            ml_confidence_threshold: 0.86,
            api_keys: ApiKeyConfig::from_env(),
        }
    }
}

impl CounterStrategyConfig {
    pub fn sanitize(&mut self) {
        self.confidence_threshold = self.confidence_threshold.clamp(0.5, 0.99);
        self.detection_sensitivity = self.detection_sensitivity.clamp(0.5, 0.99);
        self.base_priority_multiplier = self.base_priority_multiplier.clamp(1.0, 3.0);
        self.max_priority_fee = self.max_priority_fee.clamp(10_000, 10_000_000);
        self.pattern_window_size = self.pattern_window_size.max(16).min(4096);
        self.pattern_cache_size = self.pattern_cache_size.max(256).min(65_536);
        self.max_tracked_wallets = self.max_tracked_wallets.max(1024).min(100_000);
        if self.timing_offset_range_ms.0 > self.timing_offset_range_ms.1 {
            self.timing_offset_range_ms = (self.timing_offset_range_ms.1, self.timing_offset_range_ms.0);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompetitorPattern {
    pub wallet: Pubkey,
    pub pattern_type: PatternType,
    pub confidence: f64,
    pub frequency: f64,
    pub avg_priority_fee: u64,
    pub success_rate: f64,
    #[serde(skip)]
    pub last_seen: Instant,
    pub transaction_history: VecDeque<TransactionMetadata>,
    pub total_volume: u64,
    pub win_rate: f64,
    // Advanced ML features based on research
    pub signature_entropy: f64,
    pub timing_predictability: f64,
    pub account_clustering_score: f64,
    pub priority_fee_pattern: Vec<f64>,
    pub instruction_pattern_hash: String,
    pub jito_bundle_frequency: f64,
    pub cross_program_invocation_depth: u8,
    pub associated_validators: BTreeSet<Pubkey>,
    pub gas_efficiency_score: f64,
    pub sophistication_level: SophisticationLevel,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum PatternType {
    Sandwich,
    FrontRunner,
    Arbitrageur,
    Liquidator,
    HighFrequencyTrader,
    MEVBot,
    JitoSearcher,
    StatisticalArbitrageur,
    LatencyArbitrageur,
    VolumeManipulator,
    PumpAndDump,
    WashTrader,
    CrossChainArbitrageur,
    FlashLoanExploiter,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum SophisticationLevel {
    Basic,      // Simple patterns, basic MEV
    Intermediate, // Uses timing analysis, multiple strategies
    Advanced,   // ML-based, sophisticated bundling
    Professional, // Advanced analytics, multi-hop arbitrage
    Institutional, // Complex strategies, high-frequency trading
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionMetadata {
    pub signature: String,
    pub wallet: Pubkey,
    #[serde(skip)]
    pub timestamp: Instant,
    pub priority_fee: u64,
    pub compute_units: u64,
    pub target_program: Pubkey,
    pub transaction_size: usize,
    pub slot: Slot,
    pub success: bool,
    // Advanced analysis fields based on research
    pub accounts_accessed: Vec<Pubkey>,
    // Raw instruction data (human-readable for tests); when absent, fall back to balances
    pub instruction_data: String,
    pub instruction_data_hash: String,
    pub cross_program_invocations: u8,
    pub lookup_table_usage: bool,
    pub bundle_position: Option<u8>,
    pub jito_tip: Option<u64>,
    pub validator_leader: Option<Pubkey>,
    pub account_lock_conflicts: u8,
    pub pre_token_balance: Option<u64>,
    pub post_token_balance: Option<u64>,
    pub slippage_tolerance: Option<f64>,
    pub dex_program_used: Option<DexProgram>,
    // Values referenced by analytics and tests
    pub value_transferred: u64,
    pub jito_bundle_info: Option<JitoBundleInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DexProgram {
    Raydium,
    Orca,
    Serum,
    Phoenix,
    PumpFun,
    Jupiter,
    Unknown,
}

#[derive(Debug, Clone)]
pub struct CounterStrategy {
    config: Arc<CounterStrategyConfig>,
    patterns: Arc<RwLock<HashMap<Pubkey, CompetitorPattern>>>,
    sandwich_detections: Arc<RwLock<VecDeque<SandwichAttempt>>>,
    mev_protection: Arc<RwLock<MEVProtectionState>>,
    response_strategies: Arc<RwLock<HashMap<PatternType, ResponseStrategy>>>,
    performance_metrics: Arc<RwLock<PerformanceMetrics>>,
    circuit_breaker: Arc<RwLock<CircuitBreaker>>,
    rng: Arc<Mutex<ChaCha20Rng>>,
    last_cleanup: Arc<RwLock<Instant>>,
    // Advanced components based on research
    ml_feature_extractor: Arc<RwLock<MLFeatureExtractor>>,
    signature_clustering: Arc<RwLock<SignatureClustering>>,
    network_analyzer: Arc<RwLock<NetworkConditionAnalyzer>>,
    priority_fee_predictor: Arc<RwLock<PriorityFeePredictor>>,
    jito_bundle_tracker: Arc<RwLock<JitoBundleTracker>>,
    validator_rotation_tracker: Arc<RwLock<ValidatorRotationTracker>>,
    http_client: HttpClient,
    bloom_filter_cache: Arc<RwLock<BloomFilterCache>>,
    lru_pattern_cache: Arc<RwLock<LRUCache<String, CompetitorPattern>>>,
}

#[derive(Debug, Clone)]
pub struct JitoBundleInfo {
    pub bundle_id: String,
    // Optional fields retained for broader compatibility
    pub transactions: Vec<String>,
    pub total_tip: u64,
    // Fields used by sandwich/bundle analysis and tests
    pub position_in_bundle: u8,
    pub total_transactions: u8,
    pub leader_validator: Pubkey,
    pub submission_ts_ms: UnixMillis,
    pub success: Option<bool>,
    pub bundle_type: BundleType,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum BundleType {
    Sandwich,
    Arbitrage,
    Liquidation,
    Backrun,
    Frontrun,
    Mixed,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AttackSophistication {
    BasicSandwich,      // Simple front-run/back-run pattern
    BasicMEV,           // MEV with timing optimization
    IntermediateMEV,    // Advanced timing + fee optimization
    AdvancedJitoMEV,    // Jito bundles + ML-based optimization
    InstitutionalMEV,   // Multi-pool, cross-program sophisticated attacks
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundlePosition {
    pub position_in_bundle: u8,
    pub total_bundle_size: u8,
    pub frontrun_position: u8,
    pub backrun_position: u8,
    pub expected_victim_count: u8,
    pub multi_pool_coordination: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MLPatternFeatures {
    pub instruction_hash_entropy: f64,
    pub account_reuse_frequency: f64,
    pub timing_variance_coefficient: f64,
    pub fee_escalation_pattern: Vec<f64>,
    pub cross_program_complexity: u8,
    pub validator_targeting_score: f64,
    pub bundle_coordination_score: f64,
    pub profit_extraction_efficiency: f64,
}

// Missing type definitions for mainnet-ready counter-strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SandwichAttempt {
    pub attacker: Pubkey,
    pub victim_tx: String,
    pub front_tx: String,
    pub back_tx: Option<String>,
    pub detected_at: Instant,
    pub pool: Pubkey,
    pub estimated_profit: u64,
    pub jito_bundle_id: Option<String>,
    pub bundle_position: Option<BundlePosition>,
    pub slippage_extracted: f64,
    pub price_impact: f64,
    pub victim_slippage_tolerance: f64,
    pub frontrun_token_amount: u64,
    pub backrun_token_amount: u64,
    pub validator_tip: u64,
    pub attack_sophistication: AttackSophistication,
    pub detection_confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResponseStrategy {
    pub base_priority_multiplier: f64,
    pub timing_offset_ms: i64,
    pub compute_unit_padding: u64,
    pub use_decoy_transactions: bool,
    pub adaptive_routing: bool,
    pub fee_escalation_rate: f64,
    pub max_retries: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MEVProtectionState {
    pub active_protection: bool,
    pub decoy_transactions: Vec<String>,
    pub obfuscation_level: u8,
    pub dynamic_routing: bool,
    pub priority_fee_randomization: (u64, u64),
    #[serde(skip)]
    pub last_update: Instant,
    pub stealth_mode_active: bool,
    pub timing_randomization_seed: u64,
    pub decoy_pattern_complexity: u8,
    pub multi_path_routing: bool,
    pub validator_selection_strategy: ValidatorSelectionStrategy,
    pub transaction_ordering_obfuscation: bool,
    pub priority_fee_masking: bool,
    pub lookup_table_rotation: bool,
    pub account_clustering_prevention: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PerformanceMetrics {
    pub successful_counter_trades: u64,
    pub failed_counter_trades: u64,
    pub total_profit: i64,
    pub total_loss: i64,
    pub average_response_time_ms: f64,
    pub sandwich_attacks_prevented: u64,
    pub frontrun_attempts_countered: u64,
    pub mev_opportunities_captured: u64,
    pub gas_efficiency_score: f64,
    pub win_rate: f64,
    pub profit_per_trade: f64,
    pub validator_tips_paid: u64,
    pub jito_bundles_submitted: u64,
    pub jito_bundles_successful: u64,
    pub network_conditions: NetworkConditions,
    pub last_updated: SystemTime,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum ValidatorSelectionStrategy {
    Diversified,       // Spread across multiple validators
    Performance,       // Target high-performance validators
    MEVFriendly,      // Target MEV-friendly validators
    Random,           // Random selection for unpredictability
    Stealth,          // Avoid known MEV validators
    Custom(Vec<Pubkey>), // Custom validator list
}

#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    pub failure_count: u32,
    pub last_failure: Option<Instant>,
    pub is_open: bool,
    pub threshold: u32,
    pub reset_duration: Duration,
}

#[derive(Debug, Clone)]
pub struct MLFeatureExtractor {
    pub feature_window: VecDeque<MLPatternFeatures>,
    pub feature_cache: HashMap<String, Vec<f64>>,
    pub model_version: String,
    pub last_update: Instant,
}

impl MLFeatureExtractor {
    pub fn new() -> Self {
        Self {
            feature_window: VecDeque::with_capacity(200),
            feature_cache: HashMap::new(),
            model_version: "v1.0.0".to_string(),
            last_update: Instant::now(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SignatureClustering {
    pub clusters: HashMap<String, Vec<String>>,
    pub threshold: f64,
    pub cluster_metadata: HashMap<String, ClusterMetadata>,
}

impl SignatureClustering {
    pub fn new(threshold: f64) -> Self {
        Self {
            clusters: HashMap::new(),
            threshold,
            cluster_metadata: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ClusterMetadata {
    pub cluster_id: String,
    pub centroid: Vec<f64>,
    pub size: usize,
    pub confidence: f64,
    #[serde(skip)]
    pub last_updated: Instant,
}

#[derive(Debug, Clone)]
pub struct CompetitorFeeAnalysis {
    pub active_bot_count: usize,
    pub average_competitor_fee: u64,
    pub top_percentile_fee: u64,
    pub market_volatility: f64,
}

#[derive(Debug, Clone)]
pub struct NetworkConditionAnalyzer {
    pub current_conditions: NetworkConditions,
    pub historical_data: VecDeque<NetworkConditions>,
    pub congestion_score: f64,
    #[serde(skip)]
    pub last_update: Instant,
}

impl NetworkConditionAnalyzer {
    pub async fn new(config: &ApiKeyConfig) -> Self {
        let mut analyzer = Self {
            current_conditions: NetworkConditions::default(),
            historical_data: VecDeque::with_capacity(100),
            congestion_score: 0.0,
            last_update: Instant::now(),
        };
        
        // Attempt to fetch initial data, but don't fail if unavailable
        if let Ok(priority_data) = get_network_priority_fees(config).await {
            analyzer.current_conditions.average_priority_fee = priority_data.percentiles.p75 as u64;
        }
        
        analyzer
    }
}

impl CounterStrategy {
    pub async fn optimize_memory_usage(&self) -> Result<MemoryOptimizationReport> {
        let start_time = Instant::now();
        
        // Calculate optimal priority fee
        let base_fee = self.calculate_base_priority_fee().await?;
        let network_conditions = self.analyze_network_conditions().await?;
        let competitor_fees = self.analyze_competitor_fees(&self.config.api_keys).await?;

        // Clean up pattern history
        let mut patterns = self.acquire_write_lock(&self.patterns).await?;
        let initial_pattern_count = patterns.len();
        self.prune_inactive_competitors(&mut patterns)?;
        let patterns_pruned = initial_pattern_count - patterns.len();
        drop(patterns);

        // Clean up bloom filters
        let mut bloom_cache = self.acquire_write_lock(&self.bloom_cache).await?;
        bloom_cache.reset_if_needed();
        drop(bloom_cache);

        // Clean up LRU cache
        let mut lru_cache = self.acquire_write_lock(&self.lru_cache).await?;
        let cache_hit_rate = lru_cache.hit_rate();
        
        // Prune old cache entries
        let cutoff = Instant::now() - Duration::from_secs(300);
        let initial_cache_size = lru_cache.cache_entries.len();
        lru_cache.cache_entries.retain(|_, entry| entry.last_accessed > cutoff);
        let cache_entries_pruned = initial_cache_size - lru_cache.cache_entries.len();
        drop(lru_cache);

        // Clean up Jito bundle tracker
        let mut jito_tracker = self.acquire_write_lock(&self.jito_tracker).await?;
        jito_tracker.prune_old_bundles();
        drop(jito_tracker);

        // Update performance metrics
        let optimization_time = start_time.elapsed();
        
        let report = MemoryOptimizationReport {
            patterns_pruned,
            cache_entries_pruned,
            cache_hit_rate,
            optimization_duration_ms: optimization_time.as_millis() as u64,
            memory_freed_estimated_kb: (patterns_pruned * 1000 + cache_entries_pruned * 200) as u64 / 1024,
        };

        log::info!("Memory optimization completed: {:?}", report);
        Ok(report)
    }
    
    pub async fn cleanup_stale_data(&self) -> Result<()> {
        let cutoff_time = Instant::now() - Duration::from_secs(600); // 10 minutes
        
        // Clean up old patterns
        let mut patterns = self.acquire_write_lock(&self.patterns).await?;
        patterns.retain(|_, p| p.last_seen > cutoff_time);
        drop(patterns);
        
        // Clean up old cache entries
        let mut lru_cache = self.acquire_write_lock(&self.lru_pattern_cache).await?;
        lru_cache.cache_entries.retain(|_, entry| entry.last_accessed > cutoff_time);
        drop(lru_cache);
        
        // Clean up fee history
        let mut fee_predictor = self.acquire_write_lock(&self.priority_fee_predictor).await?;
        fee_predictor.fee_history.retain(|(timestamp, _)| *timestamp > cutoff_time);
        drop(fee_predictor);

        log::debug!("Cleaned up stale data older than 10 minutes");
        Ok(())
    }
    
    async fn analyze_competitor_fees(&self, api_keys: &ApiKeyConfig) -> Result<CompetitorFeeAnalysis> {
        let patterns = self.acquire_read_lock(&self.patterns).await?;
        
        let active_bots = patterns.values()
            .filter(|p| p.last_seen.elapsed() < Duration::from_secs(60))
            .count();

        // Calculate average competitor fee
        let avg_competitor_fee = patterns.values()
            .map(|p| p.avg_priority_fee)
            .sum::<u64>()
            .checked_div(patterns.len() as u64)
            .unwrap_or(0);

        // Get latest priority fee data with fallback
        let top_percentile_fee = if let Ok(priority_data) = get_network_priority_fees(api_keys).await {
            priority_data.percentiles.p95 as u64
        } else {
            // Fallback to cached or default
            self.get_cached_priority_fees().await?
                .into_iter()
                .max()
                .unwrap_or(100000)
        };
        
        Ok(CompetitorFeeAnalysis {
            active_bot_count: active_bots,
            average_competitor_fee: avg_competitor_fee,
            top_percentile_fee,
            market_volatility: 0.0,
        })
    }
    
    pub async fn get_memory_usage_stats(&self) -> Result<MemoryUsageStats> {
        let patterns = self.acquire_read_lock(&self.patterns).await?;
        let bloom_cache = self.acquire_read_lock(&self.bloom_filter_cache).await?;
        let lru_cache = self.acquire_read_lock(&self.lru_pattern_cache).await?;
        let jito_tracker = self.acquire_read_lock(&self.jito_bundle_tracker).await?;

        let total_patterns = patterns.len();
        let total_transactions = patterns.values()
            .map(|p| p.transaction_history.len())
            .sum::<usize>();
        
        let cache_entries = lru_cache.cache_entries.len();
        let active_bundles = jito_tracker.active_bundles.len();
        let bloom_filter_load = bloom_cache.items_inserted as f64 / bloom_cache.filter_capacity as f64;

        // Estimate memory usage (rough approximation)
        let estimated_memory_kb = 
            (total_patterns * 1000 +     // ~1KB per pattern
             total_transactions * 500 +  // ~500B per transaction
             cache_entries * 200 +       // ~200B per cache entry
             active_bundles * 300) as u64 / 1024; // ~300B per bundle

        Ok(MemoryUsageStats {
            total_patterns,
            total_transactions,
            cache_entries,
            active_bundles,
            bloom_filter_load_factor: bloom_filter_load,
            estimated_memory_usage_kb: estimated_memory_kb,
            last_cleanup: self.last_cleanup_time().await,
        })
    }

    async fn last_cleanup_time(&self) -> Instant {
        // Return the most recent cleanup time from various components
        let last_cleanup = self.acquire_read_lock(&self.last_cleanup).await
            .map(|lc| *lc)
            .unwrap_or_else(|_| Instant::now() - Duration::from_secs(3600));
        last_cleanup
    }
    
    // Helper methods for API fallbacks
    async fn get_network_conditions_fallback(&self) -> Result<NetworkConditions> {
        // Use cached or default network conditions when API is unavailable
        Ok(NetworkConditions {
            average_slot_time_ms: 400.0,
            transaction_throughput: 2000.0,
            average_priority_fee: 50000,
            congestion_level: 0.5,
            validator_performance: 0.85,
            network_stake_concentration: 0.3,
        })
    }
    
    async fn get_cached_priority_fees(&self) -> Result<Vec<u64>> {
        // Return cached or default priority fees
        let priority_fee_predictor = read_with_deadline(&self.priority_fee_predictor).await?;
        let fees = priority_fee_predictor.fee_history
            .iter()
            .map(|(_, fee)| *fee)
            .collect::<Vec<_>>();
        
        if fees.is_empty() {
            Ok(vec![10000, 25000, 50000, 100000]) // Default fee levels
        } else {
            Ok(fees)
        }
    }
    
    // Placeholder methods to avoid compilation errors
    async fn acquire_read_lock<T>(&self, lock: &Arc<RwLock<T>>) -> Result<RwLockReadGuard<'_, T>> {
        read_with_deadline(lock).await
    }
    
    async fn acquire_write_lock<T>(&self, lock: &Arc<RwLock<T>>) -> Result<RwLockWriteGuard<'_, T>> {
        write_with_deadline(lock).await
    }
    
    async fn calculate_base_priority_fee(&self) -> Result<u64> {
        Ok(50000) // Placeholder
    }
    
    async fn analyze_network_conditions(&self) -> Result<NetworkConditions> {
        self.get_network_conditions_fallback().await
    }
    
    fn prune_inactive_competitors(&self, patterns: &mut HashMap<Pubkey, CompetitorPattern>) -> Result<()> {
        let cutoff = Instant::now() - Duration::from_secs(300);
        patterns.retain(|_, p| p.last_seen > cutoff);
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CompetitiveLandscape {
    pub active_bot_count: usize,
    pub total_tracked_competitors: usize,
    pub average_sophistication_score: f64,
    pub competition_intensity: f64,
    pub dominant_strategy_types: Vec<PatternType>,
    pub estimated_market_share: f64,
}

#[derive(Debug, Clone)]
pub struct MemoryOptimizationReport {
    pub patterns_pruned: usize,
    pub cache_entries_pruned: usize,
    pub cache_hit_rate: f64,
    pub optimization_duration_ms: u64,
    pub memory_freed_estimated_kb: u64,
}

#[derive(Debug, Clone)]
pub struct MemoryUsageStats {
    pub total_patterns: usize,
    pub total_transactions: usize,
    pub cache_entries: usize,
    pub active_bundles: usize,
    pub bloom_filter_load_factor: f64,
    pub estimated_memory_usage_kb: u64,
    pub last_cleanup: Instant,
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;
    
    #[tokio::test]
    async fn test_counter_strategy_creation() {
        let strategy = CounterStrategyBuilder::new()
            .with_pattern_window_size(50)
            .with_confidence_threshold(0.9)
            .build();
        
        assert!(strategy.is_ok());
    }

    #[tokio::test]
    async fn test_pattern_detection_accuracy() {
        let strategy = create_test_strategy().await;
        
        // Test MEV bot pattern detection
        let mev_bot_wallet = Keypair::new().pubkey();
        let mut transactions = create_mev_bot_transactions(mev_bot_wallet, 10);
        
        for tx in &transactions {
            strategy.record_transaction(tx.clone()).await.unwrap();
        }
        
        let patterns = strategy.acquire_read_lock(&strategy.patterns).await.unwrap();
        let detected_pattern = patterns.get(&mev_bot_wallet).unwrap();
        
        assert!(matches!(detected_pattern.pattern_type, PatternType::MEVBot));
        assert!(detected_pattern.confidence > 0.7);
        assert!(detected_pattern.ml_features.timing_predictability > 0.5);
    }

    #[tokio::test]
    async fn test_sandwich_attack_detection() {
        let strategy = create_test_strategy().await;
        
        // Create sandwich attack pattern
        let attacker_wallet = Keypair::new().pubkey();
        let sandwich_txs = create_sandwich_attack_transactions(attacker_wallet);
        
        for tx in &sandwich_txs {
            strategy.record_transaction(tx.clone()).await.unwrap();
        }
        
        let patterns = strategy.acquire_read_lock(&strategy.patterns).await.unwrap();
        let pattern = patterns.get(&attacker_wallet).unwrap();
        
        let sandwich_attempt = strategy.detect_sandwich_attack(pattern, &sandwich_txs[0]).unwrap();
        assert!(sandwich_attempt.is_some());
        
        let attack = sandwich_attempt.unwrap();
        assert!(attack.detection_confidence > 0.6);
        assert!(matches!(attack.attack_sophistication, AttackSophistication::BasicMEV | AttackSophistication::IntermediateMEV));
    }

    #[tokio::test]
    async fn test_memory_management() {
        let strategy = create_test_strategy().await;
        
        // Fill up memory with test data
        for i in 0..100 {
            let wallet = Keypair::new().pubkey();
            let txs = create_test_transactions(wallet, 5);
            for tx in txs {
                strategy.record_transaction(tx).await.unwrap();
            }
        }
        
        let initial_stats = strategy.get_memory_usage_stats().await.unwrap();
        assert!(initial_stats.total_patterns > 50);
        
        // Test memory optimization
        let optimization_report = strategy.optimize_memory_usage().await.unwrap();
        assert!(optimization_report.patterns_pruned > 0 || optimization_report.cache_entries_pruned > 0);
        
        let final_stats = strategy.get_memory_usage_stats().await.unwrap();
        assert!(final_stats.estimated_memory_usage_kb <= initial_stats.estimated_memory_usage_kb);
    }

    #[tokio::test]
    async fn test_mev_protection_mechanisms() {
        let strategy = create_test_strategy().await;
        
        // Test stealth mode activation
        strategy.implement_stealth_mode().await.unwrap();
        let protection = strategy.acquire_read_lock(&strategy.mev_protection).await.unwrap();
        assert!(protection.stealth_mode);
        assert!(protection.use_decoy_transactions);
        assert_eq!(protection.obfuscation_level, 5);
        drop(protection);
        
        // Test decoy transaction generation
        let decoys = strategy.generate_decoy_transactions(3).await.unwrap();
        assert_eq!(decoys.len(), 3);
        
        // Test timing randomization
        let base_timing = Duration::from_millis(100);
        let randomized = strategy.implement_timing_randomization(base_timing).await.unwrap();
        assert!(randomized >= Duration::from_millis(10));
        assert!(randomized <= Duration::from_millis(500));
    }

    #[tokio::test]
    async fn test_adaptive_response_generation() {
        let strategy = create_test_strategy().await;
        
        // Add some competitor patterns
        let competitor = Keypair::new().pubkey();
        let txs = create_high_threat_transactions(competitor);
        for tx in txs {
            strategy.record_transaction(tx).await.unwrap();
        }
        
        let target_pool = Keypair::new().pubkey();
        let response = strategy.get_adaptive_response(&target_pool, 1_000_000_000).await.unwrap();
        
        assert!(response.threat_level > 0.0);
        assert!(response.priority_fee_range.0 > 0);
        assert!(response.priority_fee_range.1 > response.priority_fee_range.0);
        assert!(response.compute_buffer > 0);
    }

    #[tokio::test]
    async fn test_competitive_landscape_analysis() {
        let strategy = create_test_strategy().await;
        
        // Add various competitor types
        add_test_competitors(&strategy).await;
        
        let landscape = strategy.analyze_competitive_landscape().await.unwrap();
        
        assert!(landscape.active_bot_count > 0);
        assert!(landscape.competition_intensity >= 0.0 && landscape.competition_intensity <= 1.0);
        assert!(landscape.average_sophistication_score > 0.0);
        assert!(!landscape.dominant_strategy_types.is_empty());
    }

    #[tokio::test]
    async fn test_error_handling_robustness() {
        let strategy = create_test_strategy().await;
        
        // Test various error conditions
        let invalid_response = CounterResponse {
            priority_fee: u64::MAX, // Invalid high fee
            compute_units: 0,
            timing_offset_ms: 0,
            use_decoy: false,
            routing_strategy: RoutingStrategy::Direct,
            obfuscation_level: 10, // Invalid high level
            max_retries: 0,
        };
        
        let config = CounterStrategyConfig::default();
        let validation_result = validate_counter_strategy_params(&invalid_response, &config);
        assert!(validation_result.is_err());
        
        // Test lock timeout handling
        let result = strategy.record_transaction_result("test_sig", false, None).await;
        assert!(result.is_ok()); // Should handle gracefully
    }

    #[tokio::test]
    async fn test_performance_metrics_tracking() {
        let strategy = create_test_strategy().await;
        
        // Record some successful transactions
        strategy.record_transaction_result("success1", true, Some(1000000)).await.unwrap();
        strategy.record_transaction_result("success2", true, Some(2000000)).await.unwrap();
        strategy.record_transaction_result("failure1", false, None).await.unwrap();
        
        let metrics = strategy.get_performance_metrics().await.unwrap();
        assert_eq!(metrics.successful_counter_trades, 2);
        assert_eq!(metrics.failed_counter_trades, 1);
        assert!(metrics.total_profit > 0);
    }

    #[tokio::test]
    async fn test_circuit_breaker_functionality() {
        let mut config = CounterStrategyConfig::default();
        config.circuit_breaker_threshold = 2; // Low threshold for testing
        
        let strategy = CounterStrategy::new(config).unwrap();
        
        // Trigger circuit breaker with failures
        strategy.record_transaction_result("fail1", false, None).await.unwrap();
        strategy.record_transaction_result("fail2", false, None).await.unwrap();
        
        let breaker = strategy.acquire_read_lock(&strategy.circuit_breaker).await.unwrap();
        assert!(breaker.is_open);
        drop(breaker);
        
        // Test reset
        strategy.reset_circuit_breaker().await.unwrap();
        let breaker = strategy.acquire_read_lock(&strategy.circuit_breaker).await.unwrap();
        assert!(!breaker.is_open);
    }

    // Helper functions for testing
    async fn create_test_strategy() -> CounterStrategy {
        CounterStrategyBuilder::new()
            .with_pattern_window_size(20)
            .with_confidence_threshold(0.7)
            .build()
            .unwrap()
    }

    fn create_mev_bot_transactions(wallet: Pubkey, count: usize) -> Vec<TransactionMetadata> {
        let mut transactions = Vec::new();
        let base_time = Instant::now();
        
        for i in 0..count {
            transactions.push(TransactionMetadata {
                signature: format!("mev_bot_tx_{}", i),
                wallet,
                timestamp: base_time + Duration::from_millis((i * 200) as u64), // Predictable timing
                priority_fee: 50_000 + (i as u64 * 1000), // Escalating fees
                compute_units: 800_000, // High compute for MEV
                target_program: Keypair::new().pubkey(),
                instruction_data: "swap_exact_tokens_for_tokens".to_string(),
                accounts_accessed: vec![Keypair::new().pubkey(); 5],
                value_transferred: 1_000_000_000,
                success: true,
                jito_bundle_info: Some(JitoBundleInfo {
                    bundle_id: format!("bundle_{}", i / 3),
                    position_in_bundle: (i % 3) as u8,
                    total_transactions: 3,
                }),
            });
        }
        
        transactions
    }

    fn create_sandwich_attack_transactions(attacker: Pubkey) -> Vec<TransactionMetadata> {
        let base_time = Instant::now();
        let target_program = Keypair::new().pubkey();
        
        vec![
            // Frontrun transaction
            TransactionMetadata {
                signature: "frontrun_tx".to_string(),
                wallet: attacker,
                timestamp: base_time,
                priority_fee: 100_000,
                compute_units: 500_000,
                target_program,
                instruction_data: "swap_exact_tokens_for_tokens".to_string(),
                accounts_accessed: vec![Keypair::new().pubkey(); 4],
                value_transferred: 5_000_000_000,
                success: true,
                jito_bundle_info: Some(JitoBundleInfo {
                    bundle_id: "sandwich_bundle".to_string(),
                    position_in_bundle: 0,
                    total_transactions: 3,
                }),
            },
            // Backrun transaction
            TransactionMetadata {
                signature: "backrun_tx".to_string(),
                wallet: attacker,
                timestamp: base_time + Duration::from_millis(50),
                priority_fee: 95_000,
                compute_units: 480_000,
                target_program,
                instruction_data: "swap_tokens_for_exact_tokens".to_string(),
                accounts_accessed: vec![Keypair::new().pubkey(); 4],
                value_transferred: 5_100_000_000,
                success: true,
                jito_bundle_info: Some(JitoBundleInfo {
                    bundle_id: "sandwich_bundle".to_string(),
                    position_in_bundle: 2,
                    total_transactions: 3,
                }),
            },
        ]
    }

    fn create_test_transactions(wallet: Pubkey, count: usize) -> Vec<TransactionMetadata> {
        let mut transactions = Vec::new();
        let base_time = Instant::now();
        
        for i in 0..count {
            transactions.push(TransactionMetadata {
                signature: format!("test_tx_{}_{}", wallet.to_string()[..8].to_string(), i),
                wallet,
                timestamp: base_time + Duration::from_millis((i * 1000) as u64),
                priority_fee: 5_000 + (i as u64 * 500),
                compute_units: 200_000,
                target_program: Keypair::new().pubkey(),
                instruction_data: "generic_instruction".to_string(),
                accounts_accessed: vec![Keypair::new().pubkey(); 3],
                value_transferred: 100_000_000,
                success: i % 4 != 0, // 75% success rate
                jito_bundle_info: None,
            });
        }
        
        transactions
    }

    fn create_high_threat_transactions(wallet: Pubkey) -> Vec<TransactionMetadata> {
        let base_time = Instant::now();
        
        vec![
            TransactionMetadata {
                signature: "high_threat_1".to_string(),
                wallet,
                timestamp: base_time,
                priority_fee: 200_000, // Very high priority fee
                compute_units: 1_000_000, // Maximum compute units
                target_program: Keypair::new().pubkey(),
                instruction_data: "advanced_mev_strategy".to_string(),
                accounts_accessed: vec![Keypair::new().pubkey(); 8],
                value_transferred: 10_000_000_000, // 10 SOL
                success: true,
                jito_bundle_info: Some(JitoBundleInfo {
                    bundle_id: "advanced_bundle".to_string(),
                    position_in_bundle: 0,
                    total_transactions: 5,
                }),
            },
            TransactionMetadata {
                signature: "high_threat_2".to_string(),
                wallet,
                timestamp: base_time + Duration::from_millis(100),
                priority_fee: 180_000,
                compute_units: 950_000,
                target_program: Keypair::new().pubkey(),
                instruction_data: "advanced_mev_strategy".to_string(),
                accounts_accessed: vec![Keypair::new().pubkey(); 8],
                value_transferred: 8_000_000_000,
                success: true,
                jito_bundle_info: Some(JitoBundleInfo {
                    bundle_id: "advanced_bundle".to_string(),
                    position_in_bundle: 1,
                    total_transactions: 5,
                }),
            },
        ]
    }

    async fn add_test_competitors(strategy: &CounterStrategy) {
        let competitor_types = vec![
            (PatternType::MEVBot, AttackSophistication::AdvancedJitoMEV),
            (PatternType::FrontRunner, AttackSophistication::IntermediateMEV),
            (PatternType::Sandwich, AttackSophistication::BasicMEV),
            (PatternType::Arbitrageur, AttackSophistication::BasicSandwich),
        ];

        for (pattern_type, sophistication) in competitor_types {
            let wallet = Keypair::new().pubkey();
            let txs = match pattern_type {
                PatternType::MEVBot => create_mev_bot_transactions(wallet, 5),
                PatternType::Sandwich => create_sandwich_attack_transactions(wallet),
                _ => create_test_transactions(wallet, 3),
            };

            for tx in txs {
                strategy.record_transaction(tx).await.unwrap();
            }
        }
    }

    #[tokio::test]
    async fn test_ml_feature_extraction() {
        let strategy = create_test_strategy().await;
        let wallet = Keypair::new().pubkey();
        let transactions = create_mev_bot_transactions(wallet, 15);
        
        for tx in &transactions {
            strategy.record_transaction(tx.clone()).await.unwrap();
        }
        
        let patterns = strategy.acquire_read_lock(&strategy.patterns).await.unwrap();
        let pattern = patterns.get(&wallet).unwrap();
        
        // Validate ML features are properly calculated
        assert!(pattern.ml_features.signature_entropy > 0.0);
        assert!(pattern.ml_features.timing_predictability >= 0.0);
        assert!(pattern.ml_features.account_clustering_score >= 0.0);
        assert!(!pattern.ml_features.instruction_pattern_hash.is_empty());
    }

    #[tokio::test]
    async fn test_priority_fee_prediction() {
        let predictor = PriorityFeePredictor::new();
        
        // Test with various urgency levels and transaction values
        let low_urgency_fee = predictor.predict_optimal_fee(0.2, 100_000_000).unwrap();
        let high_urgency_fee = predictor.predict_optimal_fee(0.9, 100_000_000).unwrap();
        let high_value_fee = predictor.predict_optimal_fee(0.5, 10_000_000_000).unwrap();
        
        assert!(high_urgency_fee > low_urgency_fee);
        assert!(high_value_fee > low_urgency_fee);
        assert!(high_urgency_fee >= 1000 && high_urgency_fee <= 1_000_000);
    }

    #[tokio::test]
    async fn test_network_condition_analysis() {
        let mut analyzer = NetworkConditionAnalyzer::new();
        
        // Test congestion estimation with mock data
        for i in 0..10 {
            analyzer.slot_history.push_back(SlotInfo {
                slot: 1000 + i,
                timestamp: Instant::now() - Duration::from_secs((10 - i) * 60),
                transaction_count: 2000 + (i * 100) as u32, // Increasing congestion
                priority_fee_avg: 5000 + (i * 1000),
                success_rate: 0.95 - (i as f64 * 0.01),
            });
        }
        
        let congestion = analyzer.estimate_congestion_level();
        assert!(congestion > 0.5); // Should detect increased congestion
        assert!(congestion <= 1.0);
    }

    #[tokio::test]
    async fn test_signature_clustering() {
        let mut clustering = SignatureClustering::new(100);
        
        // Add signatures from the same "bot family"
        let bot_wallet = Keypair::new().pubkey();
        let similar_signatures = vec![
            "12345abc123456789abcdef".to_string(),
            "12345def123456789abcdef".to_string(),
            "12345xyz123456789abcdef".to_string(),
        ];
        
        for sig in similar_signatures {
            clustering.add_signature(sig, bot_wallet).unwrap();
        }
        
        let cluster_score = clustering.get_cluster_score(&bot_wallet);
        assert!(cluster_score > 0.0);
    }

    #[tokio::test]
    async fn test_bloom_filter_functionality() {
        let mut bloom = BloomFilterCache::new(1000, 0.01);
        
        let test_account = Keypair::new().pubkey();
        
        // Initially should not contain account
        assert!(!bloom.might_contain_account(&test_account));
        
        // After insertion, should contain account
        bloom.insert_account(&test_account);
        assert!(bloom.might_contain_account(&test_account));
        
        // Test reset functionality
        bloom.items_inserted = 700; // Trigger reset threshold
        bloom.reset_if_needed();
        assert!(!bloom.might_contain_account(&test_account));
    }

    #[tokio::test]
    async fn test_lru_cache_operations() {
        let mut cache = LRUCache::new(3);
        
        // Insert test entries
        cache.insert("key1".to_string(), CacheEntry {
            data: "data1".to_string(),
            created_at: Instant::now(),
            access_count: 0,
            last_accessed: Instant::now(),
        });
        
        cache.insert("key2".to_string(), CacheEntry {
            data: "data2".to_string(),
            created_at: Instant::now(),
            access_count: 0,
            last_accessed: Instant::now(),
        });
        
        // Test retrieval
        assert!(cache.get("key1").is_some());
        assert!(cache.get("nonexistent").is_none());
        
        // Test capacity limits
        cache.insert("key3".to_string(), CacheEntry {
            data: "data3".to_string(),
            created_at: Instant::now(),
            access_count: 0,
            last_accessed: Instant::now(),
        });
        
        cache.insert("key4".to_string(), CacheEntry {
            data: "data4".to_string(),
            created_at: Instant::now(),
            access_count: 0,
            last_accessed: Instant::now(),
        });
        
        // Should evict oldest entry
        assert_eq!(cache.cache_entries.len(), 3);
        
        let hit_rate = cache.hit_rate();
        assert!(hit_rate >= 0.0 && hit_rate <= 1.0);
    }

    #[tokio::test]
    async fn test_mainnet_readiness_validation() {
        let strategy = create_test_strategy().await;
        
        // Validate all critical components are initialized
        assert!(strategy.acquire_read_lock(&strategy.patterns).await.is_ok());
        assert!(strategy.acquire_read_lock(&strategy.mev_protection).await.is_ok());
        assert!(strategy.acquire_read_lock(&strategy.performance_metrics).await.is_ok());
        assert!(strategy.acquire_read_lock(&strategy.circuit_breaker).await.is_ok());
        
        // Test that no panics occur during normal operations
        let test_tx = TransactionMetadata {
            signature: "mainnet_test".to_string(),
            wallet: Keypair::new().pubkey(),
            timestamp: Instant::now(),
            priority_fee: 25_000,
            compute_units: 400_000,
            target_program: Keypair::new().pubkey(),
            instruction_data: "mainnet_instruction".to_string(),
            accounts_accessed: vec![Keypair::new().pubkey(); 4],
            value_transferred: 500_000_000,
            success: true,
            jito_bundle_info: None,
        };
        
        // These operations should never panic
        let result = strategy.record_transaction(test_tx).await;
        assert!(result.is_ok());
        
        let adaptive_response = strategy.get_adaptive_response(&Keypair::new().pubkey(), 1_000_000_000).await;
        assert!(adaptive_response.is_ok());
        
        let memory_stats = strategy.get_memory_usage_stats().await;
        assert!(memory_stats.is_ok());
        
        println!("✅ All mainnet readiness validations passed - no panics detected");
    }
}
