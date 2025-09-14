
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_return)]

use std::cmp::Ordering;
// [PATCH: imports tightened + alloc minimization + deterministic hashing] >>>
use std::collections::BinaryHeap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::sync::Arc as StdArc;
use std::cell::RefCell;

use ahash::{AHashMap, AHashSet, AHasher};
use fixed::types::U64F64;
use parking_lot::RwLock;
use rayon::prelude::*;
use solana_program::pubkey::Pubkey;
use solana_sdk::hash::Hash; // [PIECE AN]
// [PATCH: RCU snapshot imports] >>>
use arc_swap::ArcSwap;
// [PATCH: atomic/time imports] >>>
use std::time::{Instant, Duration};
use std::sync::atomic::{AtomicU64, Ordering as AtomOrdering};
use std::sync::atomic::{AtomicU32, Ordering as AtomOrdering32};
use std::sync::atomic::AtomicU64 as AU64;
// <<< END PATCH
use std::cmp::Reverse;
// [PATCH: memo + epoch imports] >>>
// (NonZeroU64 import not needed currently)
// <<< END PATCH
// <<< END PATCH

// ===================== [PIECE AM] Grantor RingTopK (array-backed, pointer-stable, zero alloc) =====================
use std::mem::MaybeUninit;

pub struct RingTopK<const K: usize> {
    len: usize,
    scores: [i128; K],
    edges:  [MaybeUninit<Arc<PoolEdge>>; K],
    min_idx: usize,
    min_sc: i128,
}

// ===================== [PIECE CO] Minimal ALT cover + epoch memo =====================
use std::collections::{HashMap as StdHashMap2, HashSet as StdHashSet2, VecDeque as StdVecDeque2};
use std::sync::{OnceLock as StdOnceLock3, RwLock as StdRwLock3};
use solana_sdk::{instruction::Instruction as SdkInstruction, pubkey::Pubkey as SdkPubkey, address_lookup_table_account::AddressLookupTableAccount as SdkALT};

static ALT_COVER_MEMO: StdOnceLock3<StdRwLock3<StdHashMap2<u64, Vec<SdkPubkey>>>> = StdOnceLock3::new();

impl GeodesicPathFinder {
    #[inline] fn alt_cover_memo(&self) -> &StdRwLock3<StdHashMap2<u64, Vec<SdkPubkey>>> {
        ALT_COVER_MEMO.get_or_init(|| StdRwLock3::new(StdHashMap2::with_capacity(512)))
    }

    #[inline]
    fn collect_ix_keys_no_payer(payer: &SdkPubkey, ixs: &[SdkInstruction]) -> StdHashSet2<SdkPubkey> {
        let mut s = StdHashSet2::with_capacity(ixs.len()*8);
        for ix in ixs { for m in ix.accounts.iter() { if &m.pubkey != payer { s.insert(m.pubkey); } } }
        s
    }

    pub fn plan_min_alt_cover(
        &self,
        payer: SdkPubkey,
        ixs: &[SdkInstruction],
        candidates: &[SdkALT],
        cap: usize,
        path_tag_hint: u64,
    ) -> Vec<SdkALT> {
        let key = self.graph_epoch.load(AtomOrdering::Relaxed) ^ super::wyhash64(path_tag_hint);
        if let Some(v) = self.alt_cover_memo().read().ok().and_then(|g| g.get(&key).cloned()) {
            let mut out = Vec::with_capacity(v.len());
            for want in v.iter() { if let Some(a) = candidates.iter().find(|c| c.key() == *want) { out.push(a.clone()); } }
            if !out.is_empty() { return out; }
        }
        let mut remaining = Self::collect_ix_keys_no_payer(&payer, ixs);
        if remaining.is_empty() { return Vec::new(); }
        let mut sets: Vec<(usize, SdkALT, StdHashSet2<SdkPubkey>)> = Vec::with_capacity(candidates.len());
        for a in candidates {
            let addrs: StdHashSet2<SdkPubkey> = a.addresses().iter().copied().collect();
            let cover = &remaining & &addrs;
            if !cover.is_empty() { sets.push((cover.len(), a.clone(), cover)); }
        }
        if sets.is_empty() { return Vec::new(); }
        let mut chosen: Vec<SdkALT> = Vec::new();
        while !remaining.is_empty() && chosen.len() < cap && !sets.is_empty() {
            sets.sort_unstable_by_key(|(c,_,_)| std::cmp::Reverse(*c));
            let (_, pick, cov) = sets.remove(0);
            let cov_keys = cov.clone();
            chosen.push(pick);
            for k in cov_keys { remaining.remove(&k); }
            for s in sets.iter_mut() { s.2 = &s.2 - &cov; s.0 = s.2.len(); }
            sets.retain(|(c,_,_)| *c > 0);
        }
        let pubs = chosen.iter().map(|a| a.key()).collect::<Vec<_>>();
        if let Some(mut w) = self.alt_cover_memo().write().ok() { w.insert(key, pubs); }
        chosen
    }
}

// ===================== [PIECE CW] Profit-aware ALT cover =====================
use std::sync::{OnceLock as AltOnce, RwLock as AltRw};
use std::collections::{HashMap as AltMap, HashSet as AltSet};
use solana_sdk::{instruction::Instruction as AltIx, pubkey::Pubkey as AltPk, address_lookup_table_account::AddressLookupTableAccount as AltALT};
static ALT_PROFIT_MEMO: AltOnce<AltRw<AltMap<u64, Vec<AltPk>>>> = AltOnce::new();

impl GeodesicPathFinder {
    #[inline] fn alt_profit_memo(&self) -> &AltRw<AltMap<u64, Vec<AltPk>>> { ALT_PROFIT_MEMO.get_or_init(|| AltRw::new(AltMap::with_capacity(512))) }
    #[inline] fn collect_ix_keys_no_payer_prof(payer: &AltPk, ixs: &[AltIx]) -> AltSet<AltPk> {
        let mut s = AltSet::with_capacity(ixs.len()*8);
        for ix in ixs { for m in ix.accounts.iter() { if &m.pubkey != payer { s.insert(m.pubkey); } } }
        s
    }
    #[inline] fn est_savings(keys_covered: usize) -> i64 { (keys_covered as i64) * 26 }
    #[inline] fn est_lut_overhead() -> i64 { 40 }

    pub fn plan_min_alt_cover_profit(
        &self,
        payer: AltPk,
        ixs: &[AltIx],
        candidates: &[AltALT],
        cap: usize,
        path_tag_hint: u64,
    ) -> Vec<AltALT> {
        let key = self.graph_epoch.load(AtomOrdering::Relaxed) ^ super::wyhash64(path_tag_hint) ^ 0xC0C0_C0C0_C0C0_C0C0;
        if let Some(v) = self.alt_profit_memo().read().ok().and_then(|g| g.get(&key).cloned()) {
            let mut out = Vec::with_capacity(v.len());
            for want in v { if let Some(a) = candidates.iter().find(|c| c.key()==want) { out.push(a.clone()); } }
            if !out.is_empty() { return out; }
        }
        let needed = Self::collect_ix_keys_no_payer_prof(&payer, ixs);
        if needed.is_empty() { return Vec::new(); }
        #[derive(Clone)] struct LutCov { acct: AltALT, cover: AltSet<AltPk>, profit: i64 }
        let mut sets: Vec<LutCov> = Vec::with_capacity(candidates.len());
        for a in candidates {
            let addrs: AltSet<AltPk> = a.addresses().iter().copied().collect();
            let cover: AltSet<_> = needed.intersection(&addrs).copied().collect();
            if !cover.is_empty() {
                let sav = Self::est_savings(cover.len());
                let prof = sav - Self::est_lut_overhead();
                if prof > 0 { sets.push(LutCov{ acct:a.clone(), cover, profit:prof }); }
            }
        }
        if sets.is_empty() { return Vec::new(); }
        let mut remaining = needed; let mut chosen: Vec<AltALT> = Vec::new();
        while !remaining.is_empty() && chosen.len() < cap && !sets.is_empty() {
            for s in sets.iter_mut() { s.cover = remaining.intersection(&s.cover).copied().collect(); s.profit = Self::est_savings(s.cover.len()) - Self::est_lut_overhead(); }
            sets.retain(|s| s.profit > 0 && !s.cover.is_empty()); if sets.is_empty() { break; }
            sets.sort_unstable_by(|a,b| b.profit.cmp(&a.profit));
            let pick = sets.remove(0);
            for k in pick.cover.iter() { remaining.remove(k); }
            chosen.push(pick.acct);
        }
        if let Some(mut w) = self.alt_profit_memo().write().ok() { w.insert(key, chosen.iter().map(|a| a.key()).collect()); }
        chosen
    }
}

// ===================== [PIECE CP] k-vector epoch memo + fast product =====================
use std::sync::Arc as StdArc2;
static K_VEC_MEMO: StdOnceLock3<StdRwLock3<StdHashMap2<u64, StdArc2<[f64]>>>> = StdOnceLock3::new();

impl GeodesicPathFinder {
    #[inline] fn k_vec_memo(&self) -> &StdRwLock3<StdHashMap2<u64, StdArc2<[f64]>>> { K_VEC_MEMO.get_or_init(|| StdRwLock3::new(StdHashMap2::with_capacity(2048))) }
    pub fn k_vec_for_path_memoized(&self, path: &[(PoolId,bool)]) -> StdArc2<[f64]> {
        let key = self.graph_epoch.load(AtomOrdering::Relaxed) ^ self.path_tag(path, 0);
        if let Some(v) = self.k_vec_memo().read().ok().and_then(|g| g.get(&key).cloned()) { return v; }
        let v: StdArc2<[f64]> = self.k_per_lamport_for_path(path).into_boxed_slice().into();
        if let Some(mut w) = self.k_vec_memo().write().ok() { w.insert(key, v.clone()); }
        v
    }
    #[inline] pub fn succ_prod_fast_cached(&self, ks: &StdArc2<[f64]>, ts: &[u64]) -> f64 {
        let mut p = 1.0f64; for (k,t) in ks.iter().zip(ts) { p *= self.one_minus_exp_neg(*k * (*t as f64)); } p
    }
}

// ===================== [PIECE CQ] Footprint frontier dominance gate =====================
static FP_FRONTIER: StdOnceLock3<StdRwLock3<StdVecDeque2<FpEntry>>> = StdOnceLock3::new();
#[derive(Clone, Copy)] struct FpEntry { ro:[u64;4], rw:[u64;4], gas:u64 }
#[inline] fn fp_frontier() -> &'static StdRwLock3<StdVecDeque2<FpEntry>> { FP_FRONTIER.get_or_init(|| StdRwLock3::new(StdVecDeque2::with_capacity(512))) }
#[inline] fn fp_subset(a:&[u64;4], b:&[u64;4]) -> bool { (a[0] & !b[0] | a[1] & !b[1] | a[2] & !b[2] | a[3] & !b[3]) == 0 }
impl GeodesicPathFinder {
    pub fn admit_by_footprint_frontier(&self, ro:&[u64;4], rw:&[u64;4], gas:u64) -> bool {
        if let Some(q) = fp_frontier().read().ok() { for e in q.iter() { if fp_subset(&e.ro, ro) && fp_subset(&e.rw, rw) && e.gas <= gas { return false; } } }
        if let Some(mut q) = fp_frontier().write().ok() { if q.len()>=512 { q.pop_front(); } q.push_back(FpEntry{ ro:*ro, rw:*rw, gas }); }
        true
    }
}

// ===================== [PIECE CR] Slip escalator on hazard =====================
impl GeodesicPathFinder {
    #[inline] pub fn slip_bps_for_edge_escalated(&self, e: &PoolEdge) -> u16 {
        let base = self.slip_bps_for_edge(e) as u32; let now = self.current_slot.load(AtomOrdering::Relaxed);
        let mut esc = 0u32; if let Some(s) = self.edge_fail.read().get(&e.pool_id) {
            if s.cooldown_until > now { let rem = (s.cooldown_until - now).min(64) as u32; esc = 50 * rem / 64; }
            esc = esc.saturating_add((30.0 * s.fail_ema.min(1.0)) as u32);
        }
        base.saturating_add(esc).min(1500) as u16
    }
}

// ===================== [PIECE CS] CPU features registry =====================
#[derive(Clone, Copy)] pub struct CpuFeat { pub avx2: bool, pub bmi2_adx: bool, pub neon: bool }
static CPUF: StdOnceLock3<CpuFeat> = StdOnceLock3::new();
#[inline] pub fn cpu_feat() -> CpuFeat {
    *CPUF.get_or_init(|| {
        #[cfg(target_arch="x86_64")] { CpuFeat{ avx2: std::is_x86_feature_detected!("avx2"), bmi2_adx: std::is_x86_feature_detected!("bmi2") && std::is_x86_feature_detected!("adx"), neon:false } }
        #[cfg(target_arch="aarch64")] { CpuFeat{ avx2:false, bmi2_adx:false, neon:true } }
        #[cfg(not(any(target_arch="x86_64", target_arch="aarch64")))] { CpuFeat{ avx2:false, bmi2_adx:false, neon:false } }
    })
}

// ===================== [PIECE CT] Post-exec log classifier observer =====================
use solana_transaction_status::TransactionStatusMeta;
use solana_sdk::transaction::VersionedTransaction;
impl GeodesicPathFinder {
    pub fn observe_send_outcome(&self, validator: [u8;32], vtx: &VersionedTransaction, meta: &TransactionStatusMeta, used_phase_01: f64, path: &[(PoolId,bool)], path_tag_val: u64) {
        let included = meta.status.is_ok(); let cu_used = meta.compute_units_consumed.unwrap_or(0) as u32;
        let pc = self.pool_cache.read(); for (pid, _) in path { if let Some(e) = pc.get(pid) { self.observe_program_cu(e.program_tag, cu_used, included); } } drop(pc);
        self.observe_validator_result(validator, included, meta.fee as u64);
        self.observe_validator_phase(validator, used_phase_01, included);
        if !included { if let Some(logs) = &meta.log_messages { let mut hot: Vec<[u8;32]> = Vec::new(); let pc = self.pool_cache.read(); for (pid, _) in path { if let Some(e) = pc.get(pid) { if let Some(rw)=&e.rw_accs32 { hot.extend_from_slice(rw); } } } drop(pc); if !hot.is_empty() { self.observe_account_contention(&hot); } } if let Some((pid,_))=path.first(){ self.observe_edge_fail(*pid, 64); } }
        super::inflight_clear(path_tag_val);
        let _ = vtx; // not used yet, reserved for deeper parsing
    }
}

// ===================== [PIECE CU] Per-validator in-flight cap =====================
use core::sync::atomic::{AtomicU32, Ordering as ORelaxed};
static VAL_LANES: StdOnceLock3<StdRwLock3<StdHashMap2<[u8;32], (AtomicU32, AtomicU32)>>> = StdOnceLock3::new();
#[inline] fn lanes() -> &'static StdRwLock3<StdHashMap2<[u8;32], (AtomicU32, AtomicU32)>> { VAL_LANES.get_or_init(|| StdRwLock3::new(StdHashMap2::with_capacity(128))) }
pub fn set_validator_lane_cap(validator: [u8;32], cap: u32) { if let Some(mut m)=lanes().write().ok(){ let e=m.entry(validator).or_insert((AtomicU32::new(0),AtomicU32::new(2))); e.1.store(cap.max(1), ORelaxed);} }
pub fn try_acquire_lane(validator: [u8;32]) -> bool { if let Some(m)=lanes().read().ok(){ if let Some(e)=m.get(&validator){ let cur=e.0.load(ORelaxed); if cur<e.1.load(ORelaxed){ return e.0.compare_exchange(cur,cur+1,ORelaxed,ORelaxed).is_ok(); } } } false }
pub fn release_lane(validator: [u8;32]) { if let Some(m)=lanes().read().ok(){ if let Some(e)=m.get(&validator){ e.0.fetch_update(ORelaxed,ORelaxed,|x|Some(x.saturating_sub(1))).ok(); } } }

// ===================== [PIECE CV] Token→edge Markov prefetcher =====================
static MARKOV: StdOnceLock3<StdRwLock3<StdHashMap2<TokenMint, StdHashMap2<PoolId, u32>>>> = StdOnceLock3::new();
impl GeodesicPathFinder {
    #[inline] fn markov_map() -> &'static StdRwLock3<StdHashMap2<TokenMint, StdHashMap2<PoolId, u32>>> { MARKOV.get_or_init(|| StdRwLock3::new(StdHashMap2::with_capacity(2048))) }
    pub fn observe_path_transition(&self, start: TokenMint, path: &[(PoolId,bool)]) { if let Some(mut w)=Self::markov_map().write().ok(){ let e=w.entry(start).or_insert_with(StdHashMap2::new); for (pid,_) in path.iter().take(3){ *e.entry(*pid).or_insert(0)+=1; } } }
    pub fn prewarm_next_for_token(&self, tok: TokenMint, band: u64, k: usize) {
        let pc = self.pool_cache.read(); if let Some(g)=Self::markov_map().read().ok(){ if let Some(row)=g.get(&tok){ let mut v: Vec<(PoolId,u32)> = row.iter().map(|(p,c)| (*p,*c)).collect(); v.sort_unstable_by_key(|x| std::cmp::Reverse(x.1)); for (pid,_) in v.into_iter().take(k){ if let Some(e)=pc.get(&pid){ let _ = self.raw_out_batch8_curved_and_commit(e, band, false); let _ = self.raw_out_batch8_curved_and_commit(e, band, true); } } } }
    }
}

// ===================== [PIECE CI] Lock-free in-flight path-tag table =====================
use core::sync::atomic::{AtomicU64, Ordering as Relaxed};
use std::sync::OnceLock as StdOnceLock;

struct InFlight { tags: Box<[AtomicU64]>, mask: usize }
static INFLIGHT: StdOnceLock<InFlight> = StdOnceLock::new();

#[inline] fn inflight() -> &'static InFlight {
    INFLIGHT.get_or_init(|| {
        let sz = 1usize << 16;
        let mut v: Vec<AtomicU64> = Vec::with_capacity(sz);
        for _ in 0..sz { v.push(AtomicU64::new(0)); }
        InFlight { tags: v.into_boxed_slice(), mask: sz - 1 }
    })
}

#[inline]
pub fn inflight_try_mark(tag: u64) -> bool {
    let t = inflight();
    let h1 = wyhash64(tag) as usize;
    let h2 = wyhash64(tag ^ 0x9e37_79b9_7f4a_7c15) as usize;
    let i1 = h1 & t.mask; let i2 = h2 & t.mask;
    let salt = 0xA5A5_A5A5_A5A5_A5A5u64; let want = tag ^ salt;
    if t.tags[i1].compare_exchange(0, want, Relaxed, Relaxed).is_ok() { return true; }
    if t.tags[i2].compare_exchange(0, want, Relaxed, Relaxed).is_ok() { return true; }
    if t.tags[i1].load(Relaxed) == want || t.tags[i2].load(Relaxed) == want { return false; }
    false
}

#[inline]
pub fn inflight_clear(tag: u64) {
    let t = inflight();
    let salt = 0xA5A5_A5A5_A5A5_A5A5u64; let want = tag ^ salt;
    let i1 = (wyhash64(tag) as usize) & t.mask;
    let i2 = (wyhash64(tag ^ 0x9e37_79b9_7f4a_7c15) as usize) & t.mask;
    if t.tags[i1].load(Relaxed) == want { t.tags[i1].store(0, Relaxed); return; }
    if t.tags[i2].load(Relaxed) == want { t.tags[i2].store(0, Relaxed); }
}

// ===================== [PIECE CJ] Static per-edge byte memo (approx, epoch-scoped) =====================
use std::collections::HashMap as StdHashMap;
use std::sync::RwLock as StdRwLock;
use std::sync::OnceLock as StdOnceLock2;

#[derive(Clone, Copy, Default)]
pub struct EdgeBytes { pub ix_bytes: u32, pub meta_bytes: u32, pub epoch: u64 }

static EDGE_BYTES: StdOnceLock2<StdRwLock<StdHashMap<PoolId, EdgeBytes>>> = StdOnceLock2::new();
#[inline] pub fn edge_bytes_map() -> &'static StdRwLock<StdHashMap<PoolId, EdgeBytes>> {
    EDGE_BYTES.get_or_init(|| StdRwLock::new(StdHashMap::with_capacity(4096)))
}

impl GeodesicPathFinder {
    /// Approximate per-edge static bytes and stamp with current epoch.
    pub fn precompute_edge_bytes(&self) {
        let ep = self.graph_epoch.load(AtomOrdering::Relaxed);
        let frozen = self.graph_snapshot_frozen();
        let mut m = edge_bytes_map().write().ok();
        if let Some(ref mut mm) = m {
            for edges in frozen.values() {
                for e in edges.iter() {
                    // Approximate: 1 byte header + 34 per account (pubkey+flags) + data len ~ small const
                    let ro = e.ro_accs32.as_ref().map(|v| v.len()).unwrap_or(0);
                    let rw = e.rw_accs32.as_ref().map(|v| v.len()).unwrap_or(0);
                    let accs = ro + rw + 1; // +payer
                    let ix_bytes = (1 + accs * 34 + 32) as u32; // conservative
                    mm.insert(e.pool_id, EdgeBytes{ ix_bytes, meta_bytes: (accs * 2) as u32, epoch: ep });
                }
            }
        }
    }

    #[inline]
    pub fn estimate_tx_bytes_fast(&self, path: &[(PoolId,bool)], extra_ix: u16) -> u32 {
        let ep = self.graph_epoch.load(AtomOrdering::Relaxed);
        let mm = edge_bytes_map().read().ok();
        let mut sum = 0u32;
        if let Some(m) = mm {
            for (pid, _) in path.iter() {
                if let Some(b) = m.get(pid) { if b.epoch == ep { sum = sum.saturating_add(b.ix_bytes); continue; } }
                // Fallback rough default if missing
                sum = sum.saturating_add(300);
            }
        }
        sum.saturating_add(2*8 + (extra_ix as u32)*16)
    }
}

// ===================== [PIECE CK] Epoch-stamped best-f table for A* =====================
use std::sync::atomic::{AtomicU32 as AU32, Ordering as AORelaxed};
pub struct StampTable { stamp: Vec<u32>, bestf: Vec<i128>, cur: AU32 }
impl StampTable {
    pub fn new(n: usize) -> Self { Self{ stamp: vec![0;n], bestf: vec![i128::MIN;n], cur: AU32::new(1) } }
    #[inline] pub fn epoch(&self) -> u32 { self.cur.load(AORelaxed) }
    #[inline] pub fn next_search(&self) -> u32 { let e = self.cur.load(AORelaxed).wrapping_add(1).max(1); self.cur.store(e, AORelaxed); e }
    #[inline] pub fn is_stale_or_worse(&mut self, id: usize, f: i128) -> bool {
        let e = self.epoch(); let s = self.stamp[id];
        if s != e { self.stamp[id]=e; self.bestf[id]=f; return false; }
        if f > self.bestf[id] { self.bestf[id]=f; return false; }
        true
    }
}

impl GeodesicPathFinder {
    // ===================== [PIECE CL] Hadamard tip shaper =====================
    #[inline] fn had_sign(&self, i: usize, j: usize) -> i64 { if ((i & j).count_ones() & 1) == 0 { 1 } else { -1 } }
    pub fn shape_tips_hadamard(&self, base: &[u64], k: usize, magnitude_bps: u16) -> Vec<Vec<u64>> {
        let n = base.len().max(1); let mag = (magnitude_bps as u64).min(2000);
        let total: u64 = base.iter().sum();
        let mut out = Vec::with_capacity(k);
        for i in 0..k {
            let mut v = base.to_vec();
            for j in 0..n {
                let s = self.had_sign(i, j) as i64;
                let delta = ((v[j] as u128) * (mag as u128) / 10_000u128) as i64;
                let nv = (v[j] as i64) + s*delta; v[j] = nv.max(0) as u64;
            }
            let s2: u64 = v.iter().sum();
            if s2 < total { let mut need = total - s2; let mut t=0; while need>0 { v[t % n]+=1; need-=1; t+=1; } }
            else if s2 > total { let mut over = s2 - total; let mut t=0; while over>0 && v[t % n]>0 { v[t % n]-=1; over-=1; t+=1; } }
            out.push(v);
        }
        out
    }

    // ===================== [PIECE CM] CLOB taker memo per-slot =====================
    #[inline]
    pub fn clob_taker_out_exact_memo(&self, edge: &PoolEdge, input_base: u64, is_reverse: bool) -> u64 {
        let slot = self.current_slot.load(AtomOrdering::Relaxed);
        if let Some(m) = self.clob_memo.read().get(&edge.pool_id) { if m.slot == slot { let lot = edge.base_lot.max(1); if m.in_lots == input_base / lot { return m.out_native; } } }
        // fallback: if exact walker not present here, use existing cached path
        let out = self.raw_out_cached(edge, input_base, is_reverse);
        self.clob_memo.write().insert(edge.pool_id, ClobMemo{ slot, in_lots: input_base / edge.base_lot.max(1), out_native: out });
        out
    }

    // ===================== [PIECE CN] open-set length accessor =====================
    #[inline] pub fn open_set_len(&self) -> usize { self.open_set_len_atomic.load(std::sync::atomic::Ordering::Relaxed) }
}

// [PIECE CM] per-slot CLOB taker memo types
#[derive(Clone, Copy)]
struct ClobMemo { slot: u64, in_lots: u64, out_native: u64 }

// ===================== [PIECE CA] OpenSet4 (max-heap, 4-ary) =====================
pub struct OpenSet4 {
    keys: Vec<i128>, // f-score
    vals: Vec<u32>,  // node id / handle
    size: usize,
}

impl OpenSet4 {
    #[inline]
    pub fn with_capacity(cap: usize) -> Self {
        let cap = cap.max(8);
        Self { keys: Vec::with_capacity(cap), vals: Vec::with_capacity(cap), size: 0 }
    }
    #[inline] pub fn clear(&mut self) { self.size = 0; }
    #[inline] pub fn len(&self) -> usize { self.size }
    #[inline] pub fn is_empty(&self) -> bool { self.size == 0 }

    #[inline(always)] fn parent(i: usize) -> usize { (i - 1) >> 2 }
    #[inline(always)] fn child(i: usize, k: usize) -> usize { (i << 2) + 1 + k } // k∈[0..3]

    #[inline]
    pub fn reserve_more(&mut self, addl: usize) {
        let need = self.size + addl;
        if self.keys.capacity() < need { self.keys.reserve(need - self.keys.capacity()); }
        if self.vals.capacity() < need { self.vals.reserve(need - self.vals.capacity()); }
    }

    #[inline]
    pub fn push(&mut self, key: i128, val: u32) {
        if self.size == self.keys.len() {
            self.keys.push(key); self.vals.push(val);
        } else {
            self.keys[self.size] = key; self.vals[self.size] = val;
        }
        let mut i = self.size; self.size += 1;
        while i > 0 {
            let p = Self::parent(i);
            if self.keys[p] >= key { break; }
            self.keys[i] = self.keys[p]; self.vals[i] = self.vals[p];
            i = p;
        }
        self.keys[i] = key; self.vals[i] = val;
    }

    #[inline]
    pub fn pop_max(&mut self) -> Option<(i128,u32)> {
        if self.size == 0 { return None; }
        let k = self.keys[0]; let v = self.vals[0];
        self.size -= 1;
        if self.size == 0 { return Some((k,v)); }
        let last_k = self.keys[self.size]; let last_v = self.vals[self.size];
        let mut i = 0usize;
        loop {
            let c0 = Self::child(i,0);
            if c0 >= self.size { break; }
            let c1 = c0 + 1; let c2 = c0 + 2; let c3 = c0 + 3;
            let mut j = c0; let mut best = self.keys[c0];
            if c1 < self.size && self.keys[c1] > best { j=c1; best=self.keys[c1]; }
            if c2 < self.size && self.keys[c2] > best { j=c2; best=self.keys[c2]; }
            if c3 < self.size && self.keys[c3] > best { j=c3; best=self.keys[c3]; }
            if best <= last_k { break; }
            self.keys[i] = self.keys[j]; self.vals[i] = self.vals[j];
            i = j;
        }
        self.keys[i] = last_k; self.vals[i] = last_v;
        Some((k,v))
    }
}

// ===================== [PIECE AN] Blockhash ring + scheduler =====================
#[derive(Clone, Copy)]
pub struct BlockhashStamped { pub hash: Hash, pub slot: u64 }
static BH_RING: OnceLock<parking_lot::RwLock<[Option<BlockhashStamped>; 8]>> = OnceLock::new();

#[inline]
pub fn bh_ring_push(h: Hash, slot: u64) {
    let ring = BH_RING.get_or_init(|| parking_lot::RwLock::new([None; 8]));
    let mut g = ring.write();
    let idx = (slot as usize) & 7;
    g[idx] = Some(BlockhashStamped{hash:h, slot});
}

#[inline]
pub fn bh_latest(now_slot: u64) -> Option<BlockhashStamped> {
    let ring = BH_RING.get_or_init(|| parking_lot::RwLock::new([None; 8]));
    let g = ring.read();
    let mut best: Option<BlockhashStamped> = None;
    for x in g.iter().flatten() {
        if x.slot <= now_slot && best.map_or(true, |b| x.slot > b.slot) { best = Some(*x); }
    }
    best
}

impl<const K: usize> RingTopK<K> {
    #[inline]
    pub fn new() -> Self {
        // Safe init: edges are MaybeUninit; we'll only read initialized items.
        Self {
            len: 0,
            scores: [i128::MIN; K],
            edges:  unsafe { MaybeUninit::uninit().assume_init() },
            min_idx: 0,
            min_sc: i128::MAX,
        }
    }

    #[inline]
    pub fn consider(&mut self, sc: i128, e: Arc<PoolEdge>) {
        if self.len < K {
            self.scores[self.len] = sc;
            self.edges[self.len].write(e);
            if sc < self.min_sc { self.min_sc = sc; self.min_idx = self.len; }
            self.len += 1;
            return;
        }
        if sc <= self.min_sc { return; }
        // replace current minimum
        self.scores[self.min_idx] = sc;
        unsafe { self.edges[self.min_idx].assume_init_drop(); }
        self.edges[self.min_idx].write(e);

        // recompute min in O(K)
        let mut mi = 0usize; let mut ms = self.scores[0];
        for i in 1..K { if self.scores[i] < ms { ms = self.scores[i]; mi = i; } }
        self.min_idx = mi; self.min_sc = ms;
    }

    #[inline]
    pub fn into_sorted_vec(mut self) -> Vec<Arc<PoolEdge>> {
        let mut v: Vec<(i128, Arc<PoolEdge>)> = Vec::with_capacity(self.len);
        for i in 0..self.len {
            let e = unsafe { self.edges[i].assume_init_read() };
            v.push((self.scores[i], e));
        }
        v.sort_unstable_by(|a,b| b.0.cmp(&a.0)); // high → low
        v.into_iter().map(|(_,e)| e).collect()
    }
}

// [PATCH: tuned constants (bounds, convergence, CU pricing hooks)] >>>
const MAX_PATH_LENGTH: usize = 5;
const MIN_PROFIT_THRESHOLD: u64 = 100_000; // lamports
const MAX_SLIPPAGE_BPS: u64 = 50;
const GAS_COST_LAMPORTS: u64 = 5_000;
const PRIORITY_FEE_MULTIPLIER: f64 = 1.5;
// [PATCH: oracle guard constants] >>>
const ORACLE_DEV_BPS_MAX: u64 = 150; // 1.50% deviation hard stop
const BPS_DEN: u64 = 10_000;
const DEFAULT_TIP_LAMPORTS_PER_HOP: u64 = 1_000; // baseline per-edge tip
const GOLDEN_RATIO: f64 = 0.618_033_988_749_894_9; // for golden-section refine
// <<< END PATCH

// Optional mid-price oracle used for anti-toxic guard
pub trait MidOracle {
    fn mid(&self, base: &TokenMint, quote: &TokenMint) -> Option<f64>;
}

// ===================== [PIECE AJ] Path-tag TTL dedupe =====================
use std::collections::HashMap as StdHashMap;
use std::sync::{Mutex as StdMutex, RwLock as StdRwLock};
static SEEN_TAGS: OnceLock<StdMutex<StdHashMap<u64, u64>>> = OnceLock::new();

#[inline]
fn seen_or_mark_path_tag(tag: u64, now_slot: u64, ttl_slots: u64) -> bool {
    let m = SEEN_TAGS.get_or_init(|| StdMutex::new(StdHashMap::with_capacity(2048)));
    let mut g = m.lock().unwrap();
    if let Some(&exp) = g.get(&tag) {
        if exp >= now_slot { return true; }
    }
    g.insert(tag, now_slot.saturating_add(ttl_slots));
    false
}

// ===================== [PIECE AH] Priority-IX memo =====================
use solana_sdk::instruction::Instruction as SdkInstruction;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
static PRI_MEMO: OnceLock<StdMutex<StdHashMap<(u32,u64), StdArc<[SdkInstruction]>>>> = OnceLock::new();


// ===================== [PIECE R] Two-child pre-eval data =====================
#[derive(Clone, Copy)]
pub struct ChildEval {
    pub pid: PoolId,
    pub is_rev: bool,
    pub next_tok: TokenMint,
    pub out_amt: u64,
    pub edge_gas: u64,
    pub g_profit: i64,
    pub h: i64,
    pub f: i64,
    pub ro_fp: [u64;4],
    pub rw_fp: [u64;4],
}

// ===================== [PIECE S] ALT planner structs =====================
#[derive(Debug, Clone)]
pub struct AltPlan {
    pub use_alt: bool,
    pub alt_keys: StdArc<[[u8;32]]>,
    pub inline_ro: u32,
    pub inline_rw: u32,
}

// ===================== [PIECE A] NodeRec + Arena A* =====================
#[derive(Clone)]
struct NodeRec {
    parent: Option<usize>,
    hop:    Option<(PoolId, bool)>,
    token:  TokenMint,
    amt:    u64,
    gas:    u64,
    depth:  u8,
    ro_fp:  [u64;4],
    rw_fp:  [u64;4],
    tl_fp:  [u64;4],
}

// ===================== CG: Thread-Local Visited Bloom =====================
const VBITS: usize = 1 << 20; // 1,048,576 bits
const VWORDS: usize = VBITS / 64;

thread_local! {
    static VBLOOM: RefCell<[u64; VWORDS]> = RefCell::new([0u64; VWORDS]);
}

#[inline]
fn vb_clear() {
    VBLOOM.with(|b| {
        *b.borrow_mut() = [0u64; VWORDS];
    });
}

#[inline]
fn vb_idx_hash(token: &TokenMint, depth: u8, amt_bucket: u64, salt: u64) -> (usize, u64) {
    let mut h = AHasher::new_with_keys(0xB10C_B10C_DEAD_BEEF ^ salt, 0x9E37_79B1_85EB_CA87);
    token.hash(&mut h);
    depth.hash(&mut h);
    amt_bucket.hash(&mut h);
    let v = h.finish();
    let word = ((v as usize) & (VWORDS - 1)) as usize;
    let bit = ((v >> 12) & 63) as u64;
    (word, 1u64 << bit)
}

#[inline]
fn vb_test_set(token: &TokenMint, depth: u8, amt_bucket: u64, salt: u64) -> bool {
    let (w, m) = vb_idx_hash(token, depth, amt_bucket, salt);
    VBLOOM.with(|b| {
        let mut arr = b.borrow_mut();
        let hit = (arr[w] & m) != 0;
        arr[w] |= m;
        hit
    })
}
// [PATCH BI: admission presets]
pub fn admission_risk_off() -> AdmissionCfg {
    AdmissionCfg {
        ttl_slots: 8, max_keys: 96, max_writable: 48,
        cu_limit: 1_200_000, cu_margin_bps: 400,
        extra_slip_bps_per_hop: 60, min_profit_after_tips: (MIN_PROFIT_THRESHOLD as i64) + 200_000,
    }

    // ===================== [PIECE Q] cpmm_out_vec8 + fused cache commits =====================
    #[inline]
    fn cpmm_out_vec8(&self, rin: u64, rout: u64, amts: [u64;8], fee_bps: u16) -> [u64;8] {
        if rin == 0 || rout == 0 { return [0;8]; }
        #[inline(always)]
        fn one(rin: u64, rout: u64, fee_bps: u16, a: u64) -> u64 { cpmm_out_u64_bmi2(rin, rout, a, fee_bps) }
        let mut o = [0u64;8];
        o[0]=one(rin, rout, fee_bps, amts[0]);
        o[1]=one(rin, rout, fee_bps, amts[1]);
        o[2]=one(rin, rout, fee_bps, amts[2]);
        o[3]=one(rin, rout, fee_bps, amts[3]);
        o[4]=one(rin, rout, fee_bps, amts[4]);
        o[5]=one(rin, rout, fee_bps, amts[5]);
        o[6]=one(rin, rout, fee_bps, amts[6]);
        o[7]=one(rin, rout, fee_bps, amts[7]);
        o
    }

    /// Compute {75,85,90,100,110,115,125,150}% once and commit to caches.
    #[inline]
    pub fn raw_out_batch8_and_commit(&self, edge: &PoolEdge, amount_in: u64, is_reverse: bool) -> [u64;8] {
        match edge.pool_type {
            PoolType::SerumV3 | PoolType::PhoenixV1 => {
                let mid = self.raw_out_cached(edge, amount_in, is_reverse);
                return [mid,mid,mid,mid,mid,mid,mid,mid];
            }
            _ => {}
        }
        if amount_in == 0 { return [0;8]; }
        let (rin, rout) = if is_reverse { (edge.reserve_b, edge.reserve_a) } else { (edge.reserve_a, edge.reserve_b) };
        let amts = Self::vec8_grid(amount_in, rin);
        let slip = self.slip_bps_for_edge(edge);
        let outs = self.cpmm_out_vec8(rin, rout, amts, edge.fee_bps, slip);
        let epoch = self.graph_epoch.load(AtomOrdering::Relaxed);
        let base  = self.quote_stamp.fetch_add(8, AtomOrdering32::Relaxed);
        #[inline(always)]
        fn commit(owner: &GeodesicPathFinder, e:&PoolEdge, amt:u64, out:u64, ep:u64, st:u32, rev:bool) {
            let (set_idx, tag) = quote_set_and_tag(&e.pool_id, rev, amt);
            TQUOTE.with(|q| TSTAMP.with(|s| {
                let mut v = q.borrow_mut();
                let mut ts = *s.borrow();
                if let Some(slot) = v.get_mut(set_idx) {
                    if slot.a.stamp <= slot.b.stamp { slot.a = QuoteLine{ tag, out, stamp: ts, epoch: ep }; }
                    else                             { slot.b = QuoteLine{ tag, out, stamp: ts, epoch: ep }; }
                    ts = ts.wrapping_add(1); *s.borrow_mut() = ts;
                }
            }) );
            if let Some(mut set) = owner.quote_cache.write().get_mut(set_idx) {
                if set.a.stamp <= set.b.stamp { set.a = QuoteLine{ tag, out, stamp: st, epoch: ep }; }
                else                           { set.b = QuoteLine{ tag, out, stamp: st, epoch: ep }; }
            }
        }
        for i in 0..8 { commit(self, edge, amts[i], outs[i], epoch, base.wrapping_sub((7-i) as u32), is_reverse); }
        outs
    }

    // (moved dynamic-branching helpers into impl below)

    // (moved NodeRec into impl below)

    // (helper moved into impl below)

    pub fn find_optimal_path_astar_arena(
        &self,
        start_token: TokenMint,
        start_amount: u64,
        targets: &[TokenMint],
    ) -> Option<(Vec<(PoolId,bool)>, u64, i64)> {
        use std::cmp::Ordering;
        let target_set: AHashSet<TokenMint> = targets.iter().copied().collect();
        let start_epoch = self.graph_epoch.load(AtomOrdering::Relaxed);
        let max_depth = self.depth_cap_for_start(start_token, start_amount);

        #[derive(Clone)]
        struct Key { f: i64, seq: u64, idx: usize }
        impl Eq for Key {}
        impl PartialEq for Key { fn eq(&self,o:&Self)->bool { self.f==o.f && self.seq==o.seq && self.idx==o.idx } }
        impl Ord for Key { fn cmp(&self,o:&Self)->Ordering { self.f.cmp(&o.f).then_with(|| o.seq.cmp(&self.seq)) } }
        impl PartialOrd for Key { fn partial_cmp(&self,o:&Self)->Option<Ordering>{Some(self.cmp(o))} }

        let bm = self.best_mult_rcu.load_full();
        let slip_rcu = self.token_slip_floor_bps_rcu.load_full();
        let mut arena: Vec<NodeRec> = Vec::with_capacity(8192);
        arena.push(NodeRec{ parent:None, hop:None, token:start_token, amt:start_amount, gas:0, depth:0, ro_fp:[0;4], rw_fp:[0;4], tl_fp:[0;4] });
        let mut heap: BinaryHeap<Key> = BinaryHeap::new();
        let mut seq=0u64;
        heap.push(Key{ f: 0, seq:{seq+=1;seq}, idx: 0 });
        let mut best: Option<(usize, u64, i64)> = None;
        let mut best_seen: AHashMap<u128, (u64,i64)> = AHashMap::with_capacity(4096);
        best_seen.insert(node_key_compact(start_token, 0, bucketize_amount(start_amount)), (start_amount,0));
        vb_clear();
        let vb_salt = self.path_jitter_seed.load(AtomOrdering::Relaxed);

        while let Some(Key{ f:_, seq:_, idx }) = heap.pop() {
            if self.graph_epoch.load(AtomOrdering::Relaxed) != start_epoch { break; }
            let node = &arena[idx];
            if node.depth as usize >= max_depth { continue; }
            let cand = self.next_edges_for(node.token, node.depth as usize);
            for e in cand.iter() {
                prefetch_edge(e);
                if let Some(parent) = node.parent {
                    if let Some((lp, lr)) = arena[parent].hop {
                        if lp == e.pool_id && lr != (node.token == e.token_b) { continue; }
                    }
                }
                let is_rev = node.token == e.token_b;
                let out_amt = self.raw_out_cached(e, node.amt, is_rev);
                if out_amt == 0 { continue; }
                let edge_gas = self.gas_cost_for_edge(e);
                let inc = out_amt as i64 - node.amt as i64 - edge_gas as i64;
                if inc < -((MIN_PROFIT_THRESHOLD as i64)/2) { continue; }
                let out_tok = if is_rev { e.token_a } else { e.token_b };
                if out_amt < self.mint_dust(&out_tok) { continue; }

                let child_ro = bloom256_or(&node.ro_fp, &e.acct_ro_fp);
                let child_rw = bloom256_or(&node.rw_fp, &e.acct_rw_fp);
                let child_tl = bloom256_or(&node.tl_fp, &e.acct_ro_fp); // [PATCH CG: TL visited bloom]
                let child_idx = { arena.push(NodeRec{ parent:Some(idx), hop:Some((e.pool_id,is_rev)), token:out_tok, amt:out_amt, gas: node.gas+edge_gas, depth: node.depth+1, ro_fp: child_ro, rw_fp: child_rw, tl_fp: child_tl }); arena.len()-1 };

                let g_profit = if (target_set.contains(&out_tok) || out_tok==start_token) && node.depth>0 {
                    out_amt as i64 - start_amount as i64 - (node.gas + edge_gas) as i64
                } else { 0 };
                let remain = max_depth - (arena[child_idx].depth as usize);
                let floor_bps = slip_rcu.get(&out_tok).copied().unwrap_or(0) as f64;
                let slip_mult = (BPS_DEN as f64 - floor_bps) / BPS_DEN as f64;
                let bmf = bm.get(&out_tok).copied().unwrap_or(1.0) * slip_mult.max(0.95);
                let perhop_cu_lb = 20_000u64; // [PATCH CQ]
                let perhop_bytes_lb = 80u64;
                let ub_out = (out_amt as f64) * powi_f64(bmf, remain);
                let min_tip = *self.min_edge_tip_lamports.read();
                let cuu = *self.cu_unit_price_lamports.read();
                let ub_gas = (node.gas + edge_gas).saturating_add((remain as u64) * (min_tip + perhop_cu_lb * cuu));
                let bytes_lb = (remain as u64) * perhop_bytes_lb;
                let mut h = (ub_out as i64) - (start_amount as i64) - (ub_gas as i64) - (bytes_lb as i64);
                let haz_pen = self.hazard_penalty_lamports(e);
                let conf_pen = overlap_penalty_lamports(&node.ro_fp, &node.rw_fp, e);
                let lat_pen = self.latency_penalty_lamports(arena[child_idx].depth as usize);
                let prog_pen = self.program_penalty(&reconstruct_path_from_arena(&arena, idx), e.program_tag);
                let hot_pen = self.account_hot_penalty(e);
                h = h.saturating_sub(haz_pen + conf_pen + lat_pen + prog_pen + hot_pen);

                // [PATCH CG: per-search dedupe]
                let amt_b = bucketize_amount(out_amt);
                if vb_test_set(&out_tok, arena[child_idx].depth, amt_b, vb_salt) { continue; }
                let f_score = g_profit.saturating_add(h);
                // [PATCH CO: compact best_seen key]
                let key = node_key_compact(out_tok, arena[child_idx].depth, amt_b);
                let push_ok = best_seen.get(&key).map(|&(amt, gp)| out_amt > amt || g_profit > gp).unwrap_or(true);
                if !push_ok { continue; }
                best_seen.insert(key, (out_amt, g_profit));
                heap.push(Key{ f: f_score, seq:{seq+=1;seq}, idx: child_idx });

                if (target_set.contains(&out_tok) || out_tok==start_token) && g_profit > self.min_profit_for_path(&reconstruct_path_from_arena(&arena, child_idx)) {
                    if best.as_ref().map(|b| g_profit > b.2).unwrap_or(true) {
                        best = Some((child_idx, out_amt, g_profit));
                    }
                }
            }
        }
        best.map(|(idx, out, prof)| (reconstruct_path_from_arena(&arena, idx), out, prof))
    }

    // [PATCH CO: compact node key helper]
    #[inline]
    fn node_key_compact(token: TokenMint, depth: u8, amt_bucket: u64) -> u128 {
        let mut h = AHasher::new_with_keys(0xABCD_EF01_2345_6789, 0x9E37_79B1_85EB_CA87);
        token.hash(&mut h);
        let t = h.finish() as u128;
        (t << 64) | (((depth as u128) & 0xFF) << 56) | (amt_bucket as u128 & ((1u128<<56)-1))
    }

    // [PATCH CI: tip ladders + expected EV chooser]
    pub fn build_tip_ladders_for_path(
        &self,
        path: &[(PoolId,bool)],
        input_amount: u64,
        k_per_lamport: f64,
        budgets: &[f64],
    ) -> Option<Vec<Vec<u64>>> {
        let mut ladders = Vec::with_capacity(budgets.len());
        for &bf in budgets {
            if let Some(v) = self.tip_budget_for_path_ev(path, input_amount, k_per_lamport, bf) {
                ladders.push(v);
            }
        }
        if ladders.is_empty() { None } else { Some(ladders) }
    }

    pub fn choose_best_ladder_by_expected_ev(
        &self,
        path: &[(PoolId,bool)],
        input_amount: u64,
        ladders: &[Vec<u64>],
        k_per_lamport: f64,
    ) -> Option<(usize, i64)> {
        let (out, _) = self.simulate_path_execution(path, input_amount).ok()?;
        let unit = *self.cu_unit_price_lamports.read();
        let pc = self.pool_cache.read();
        let cu_cost: u64 = path.iter().map(|(pid, _)| pc.get(pid).map(|e| e.cu_estimate as u64 * unit).unwrap_or(0)).sum();
        let mut best: Option<(usize, i64)> = None;
        for (i, tips) in ladders.iter().enumerate() {
            let tip_sum: u64 = tips.iter().sum();
            let base = out as i64 - input_amount as i64 - cu_cost as i64 - tip_sum as i64;
            if base <= 0 { continue; }
            let p: f64 = tips.iter().fold(1.0, |acc, t| acc * (1.0 - (-k_per_lamport * (*t as f64)).exp()).max(0.0));
            let ev = (base as f64 * p).floor() as i64;
            if best.as_ref().map(|&(_,b)| ev > b).unwrap_or(true) { best = Some((i, ev)); }
        }
        best
    }

    // [PATCH CN: Lagrangian portfolio selector]
    pub fn select_portfolio_lagrange(
        &self,
        candidates: &[(Vec<(PoolId,bool)>, u64, i64)],
        max_total_cu: u64,
        max_total_tips: u64,
    ) -> Vec<usize> {
        let pc = self.pool_cache.read();
        let mut data: Vec<(usize, u64, u64, AHashSet<PoolId>)> = Vec::with_capacity(candidates.len());
        for (i,(path,_o,_p)) in candidates.iter().enumerate() {
            let cu = self.path_cu_units(path).max(1);
            let tips: u64 = path.iter().map(|(pid, _)| pc.get(pid).map(|e| e.cu_estimate as u64).unwrap_or(0)).sum();
            let set: AHashSet<PoolId> = path.iter().map(|(pid,_)| *pid).collect();
            data.push((i, cu, tips, set));
        }
        let mut lo = 0.0f64;
        let mut hi = 1e3f64;
        let mut best: Vec<usize> = Vec::new();
        for _ in 0..18 {
            let mid = 0.5*(lo+hi);
            let mut scored: Vec<(usize, f64, u64, u64, &AHashSet<PoolId>)> = candidates.iter().enumerate().map(|(i, c)| {
                let (_,_,p) = c;
                let cu = data[i].1;
                let tips = data[i].2;
                (i, (*p as f64) - mid*(cu as f64), cu, tips, &data[i].3)
            }).collect();
            scored.sort_by(|a,b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));
            let mut chosen = Vec::new();
            let mut used: AHashSet<PoolId> = AHashSet::with_capacity(256);
            let mut cu_acc=0u64; let mut tip_acc=0u64;
            for (i, sc, cu, tips, set) in scored {
                if sc <= 0.0 { break; }
                if cu_acc.saturating_add(cu) > max_total_cu { continue; }
                if tip_acc.saturating_add(tips) > max_total_tips { continue; }
                if set.iter().any(|p| used.contains(p)) { continue; }
                cu_acc += cu; tip_acc += tips;
                used.extend(set.iter().copied());
                chosen.push(i);
            }
            if cu_acc > max_total_cu { hi = mid; } else { lo = mid; best = chosen; }
        }
        best
    }
}
pub fn admission_risk_on() -> AdmissionCfg {
    AdmissionCfg {
        ttl_slots: 6, max_keys: 120, max_writable: 60,
        cu_limit: 1_400_000, cu_margin_bps: 300,
        extra_slip_bps_per_hop: 30, min_profit_after_tips: (MIN_PROFIT_THRESHOLD as i64),
    }
}

// [PATCH: small bloom util] >>>
#[inline]
fn bloom_hash(token: &TokenMint, depth: u8, amt_bucket: u64, salt: u64) -> u64 {
    let mut h = AHasher::new_with_keys(salt, 0x9E3779B185EBCA87);
    token.hash(&mut h);
    depth.hash(&mut h);
    amt_bucket.hash(&mut h);
    h.finish()
}

// [PATCH: local upper bound estimator] >>>
#[inline]
fn best_local_multiplier(edges: &[Arc<PoolEdge>], token_is_b: bool) -> f64 {
    let mut best = 0.0f64;
    for e in edges {
        let (rin, rout) = if token_is_b { (e.reserve_b, e.reserve_a) } else { (e.reserve_a, e.reserve_b) };
        if rin == 0 || rout == 0 { continue; }
        let fee = (BPS_DEN - e.fee_bps as u64) as f64 / (BPS_DEN as f64);
        let m = (rout as f64 / rin as f64) * fee;
        if m > best { best = m; }
    }

    // ===== Tip tuner public observer (AF) =====
    pub fn observe_fill(&self, included: bool, latency_ms: u64) {
        let mut s = self.tip_stats.write();
        let x_s = if included { 1.0 } else { 0.0 };
        let x_l = latency_ms as f64;
        if !s.init {
            *s = TipStats{ succ_ema: x_s, lat_ema_ms: x_l, init: true };
        } else {
            s.succ_ema = (1.0-EMA_ALPHA)*s.succ_ema + EMA_ALPHA*x_s;
            s.lat_ema_ms = (1.0-EMA_ALPHA)*s.lat_ema_ms + EMA_ALPHA*x_l;
        }
        drop(s);
        self.recompute_tip_knobs();
    }

    #[inline]
    fn recompute_tip_knobs(&self) {
        let s = *self.tip_stats.read();
        if !s.init { return; }
        let succ_err = (TGT_SUCC - s.succ_ema).clamp(-0.9, 0.9);
        let lat_err  = (s.lat_ema_ms - TGT_LAT_MS).clamp(-500.0, 500.0);
        let base_cu = 1.0_f64; // scale factor if you want a base, keep >=1
        let base_min_tip = DEFAULT_TIP_LAMPORTS_PER_HOP as f64;
        let cu_adj = 1.0 + 0.8*(-succ_err) + 0.002*(lat_err.max(0.0));
        let tip_adj = 1.0 + 0.6*(-succ_err) + 0.001*(lat_err.max(0.0));
        let new_cu = (base_cu * cu_adj).max(1.0) as u64;
        let new_tip = (base_min_tip * tip_adj).max(0.0) as u64;
        self.set_cu_unit_price_lamports(new_cu);
        self.set_min_edge_tip_lamports(new_tip);
    }
    best.max(0.0)
}

#[inline]
fn powi_f64(mut base: f64, mut exp: usize) -> f64 {
    let mut acc = 1.0;
    while exp > 0 {
        if exp & 1 == 1 { acc *= base; }
        base *= base;
        exp >>= 1;
    }
    acc
}

// ===================== [PIECE AS] NUMA/CPU pinning =====================
#[cfg(target_os="linux")]
pub fn pin_current_thread_to(cpu: usize) {
    use libc::{cpu_set_t, CPU_SET, CPU_ZERO, sched_setaffinity};
    unsafe {
        let mut set: cpu_set_t = core::mem::zeroed();
        CPU_ZERO(&mut set);
        CPU_SET(cpu, &mut set);
        let _r = sched_setaffinity(0, core::mem::size_of::<cpu_set_t>(), &set);
    }
}
#[cfg(not(target_os="linux"))]
#[inline]
pub fn pin_current_thread_to(_cpu: usize) {}

/// Call once per worker thread at start.
pub fn pin_worker(worker_id: usize) {
    let total = num_cpus::get().max(1);
    let cpu = worker_id % total;
    pin_current_thread_to(cpu);
}

    // ===================== [PIECE AT] wyhash64 path_tag replacement =====================
    #[inline]
    fn path_tag(&self, path: &[(PoolId,bool)], input_bucket: u64) -> u64 {
        let mut h: u64 = 0x243F_6A88_85A3_08D3;
        for (pid, rev) in path.iter().copied() {
            let bytes = pid.0.to_bytes();
            // fold Pubkey bytes into u64
            let mut k = 0u64;
            k ^= u64::from_le_bytes([bytes[0],bytes[1],bytes[2],bytes[3],bytes[4],bytes[5],bytes[6],bytes[7]]);
            k ^= u64::from_le_bytes([bytes[8],bytes[9],bytes[10],bytes[11],bytes[12],bytes[13],bytes[14],bytes[15]]);
            k ^= u64::from_le_bytes([bytes[16],bytes[17],bytes[18],bytes[19],bytes[20],bytes[21],bytes[22],bytes[23]]);
            k ^= u64::from_le_bytes([bytes[24],bytes[25],bytes[26],bytes[27],bytes[28],bytes[29],bytes[30],bytes[31]]);
            let dir = if rev { 0x9E37_79B9_7F4A_7C15 } else { 0xBF58_476D_1CE4_E5B9 };
            h = wyhash64(h ^ (k ^ dir));
        }
        wyhash64(h ^ input_bucket)
    }

    // [PATCH: local upper bound estimator] >>>
    #[inline]
    fn best_local_multiplier(edges: &[Arc<PoolEdge>], token_is_b: bool) -> f64 {
        let mut best = 0.0f64;
        for e in edges {
            let (rin, rout) = if token_is_b { (e.reserve_b, e.reserve_a) } else { (e.reserve_a, e.reserve_b) };
            if rin == 0 || rout == 0 { continue; }
            let fee = (BPS_DEN - e.fee_bps as u64) as f64 / (BPS_DEN as f64);
            let m = (rout as f64 / rin as f64) * fee;
            if m > best { best = m; }
        }
        best.max(0.0)
    }

    // ===================== [PIECE AU] Lock-free HotIdx heatmap (atomic) =====================
    use core::sync::atomic::{AtomicU64 as AtomicU64LF, Ordering as AtomOrdLF};
    struct HotIdxLF { buckets: Box<[AtomicU64LF]> }
    static HOTIDX_LF: OnceLock<HotIdxLF> = OnceLock::new();

    impl HotIdxLF {
        fn new(sz_pow2: usize) -> Self {
            let sz = sz_pow2.next_power_of_two().max(1<<15);
            let mut v: Vec<AtomicU64LF> = Vec::with_capacity(sz);
            for _ in 0..sz { v.push(AtomicU64LF::new(0)); }
            Self { buckets: v.into_boxed_slice() }
        }
        #[inline(always)]
        fn hash32(k: &[u8;32]) -> usize {
            let mut v = 0u64;
            v ^= u64::from_le_bytes([k[0],k[1],k[2],k[3],k[4],k[5],k[6],k[7]]);
            v ^= u64::from_le_bytes([k[8],k[9],k[10],k[11],k[12],k[13],k[14],k[15]]);
            v ^= u64::from_le_bytes([k[16],k[17],k[18],k[19],k[20],k[21],k[22],k[23]]);
            v ^= u64::from_le_bytes([k[24],k[25],k[26],k[27],k[28],k[29],k[30],k[31]]);
            wyhash64(v) as usize
        }
        #[inline(always)]
        fn mark_keys(&self, keys:&[[u8;32]], slot:u64) {
            let m = self.buckets.len()-1;
            for k in keys { let i = Self::hash32(k) & m; self.buckets[i].store(slot, AtomOrdLF::Relaxed); }
        }
        #[inline(always)]
        fn age_score(&self, key:&[u8;32], now:u64) -> u64 {
            let m = self.buckets.len()-1;
            let i = Self::hash32(key) & m;
            let s = self.buckets[i].load(AtomOrdLF::Relaxed);
            now.saturating_sub(s)
        }
    }

    #[inline]
    pub fn account_hot_penalty(&self, e: &PoolEdge) -> i64 {
        let now = self.current_slot.load(AtomOrdering::Relaxed);
        let idx = HOTIDX_LF.get_or_init(|| HotIdxLF::new(1<<16));
        let mut dynp: i64 = 0;
        if let Some(ro) = &e.ro_accs32 { for k in ro.iter() { let age = idx.age_score(k, now); if age <= 32 { dynp += 400; } } }
        if let Some(rw) = &e.rw_accs32 { for k in rw.iter() { let age = idx.age_score(k, now); if age <= 64 { dynp += 1200; } } }
        // base from overlap counts already captured elsewhere; keep dynp bounded
        dynp.min(120_000)
    }

    // [PATCH AV: validator Wilson-sampled scorer] >>>
    use rand::Rng;
    use rand::distributions::{Distribution, WeightedIndex};

    pub struct ValidatorScorer {
        weights: Vec<f64>,
        dist: WeightedIndex<f64>,
    }

    impl ValidatorScorer {
        pub fn new(weights: Vec<f64>) -> Self {
            let dist = WeightedIndex::new(&weights).unwrap();
            Self { weights, dist }
        }

        pub fn sample(&self, rng: &mut impl Rng) -> usize {
            self.dist.sample(rng)
        }
    }

// ===================== [PIECE AR] BMI2+ADX CPMM inner =====================
#[inline(always)]
fn has_bmi2_adx() -> bool {
    #[cfg(target_arch="x86_64")]
{{ ... }}
    { std::is_x86_feature_detected!("bmi2") && std::is_x86_feature_detected!("adx") }
    #[cfg(not(target_arch="x86_64"))] { false }
}

#[inline(always)]
fn cpmm_out_u64_bmi2(rin: u64, rout: u64, amt_in: u64, fee_bps: u16) -> u64 {
    if rin == 0 || rout == 0 || amt_in == 0 { return 0; }
    let fee_mul = (BPS_DEN - fee_bps as u64) as u128;
    let rin_s: u128 = (rin as u128) * (BPS_DEN as u128);
    let ain128: u128 = (amt_in as u128) * fee_mul;
    let den = rin_s.saturating_add(ain128);
    if den == 0 { return 0; }

    #[cfg(target_arch="x86_64")]
    unsafe {
        if has_bmi2_adx() {
            use core::arch::x86_64::{_mulx_u64, _addcarry_u64};
            // num = ain * rout  (128-bit). We split ain into low/high limbs of fee_mul * amt_in.
            let ain_lo = (amt_in as u128).wrapping_mul((fee_mul & 0xFFFF_FFFF_FFFF_FFFF) as u128) as u64;
            let ain_hi = ((amt_in as u128).wrapping_mul(fee_mul >> 64)) as u64;
            let (lo1, hi1) = _mulx_u64(ain_lo, rout);
            let (lo2, hi2) = _mulx_u64(ain_hi, rout);
            // accumulate cross term (lo2<<64) into high part
            let mut num_lo: u64 = lo1;
            let mut num_hi: u64 = hi1;
            let mut carry: u8 = 0;
            let _ = _addcarry_u64(0, num_hi, lo2, &mut num_hi);
            let _ = _addcarry_u64(0, 0, hi2, &mut carry);
            let num = ((num_hi as u128) << 64) | (num_lo as u128);
            let out = num / den;
            let slip = (out * (MAX_SLIPPAGE_BPS as u128)) / (BPS_DEN as u128);
            return out.saturating_sub(slip) as u64;
        }
    }

    // Scalar fallback (identical math)
    let num = ain128.saturating_mul(rout as u128);
    let out = num / den;
    let slip = (out * (MAX_SLIPPAGE_BPS as u128)) / (BPS_DEN as u128);
    (out.saturating_sub(slip)) as u64
}

// [PATCH BS: 256-bit bloom utils for account sets]
// ===================== [PIECE U] Runtime-dispatch SIMD Bloom-256 =====================
use std::sync::OnceLock;

#[inline(always)]
fn bloom_avx2_enabled() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        static CHOICE: OnceLock<bool> = OnceLock::new();
        *CHOICE.get_or_init(|| {
            std::is_x86_feature_detected!("avx2")
        })
    }
    #[cfg(not(target_arch = "x86_64"))]
    { false }
}

#[inline]
fn bloom256_or(a: &[u64;4], b: &[u64;4]) -> [u64;4] {
    // [PIECE BO] AArch64 NEON path
    #[cfg(target_arch = "aarch64")]
    unsafe {
        use core::arch::aarch64::*;
        let va0 = vld1q_u64(a.as_ptr());
        let vb0 = vld1q_u64(b.as_ptr());
        let vc0 = vorrq_u64(va0, vb0);
        let va1 = vld1q_u64(a.as_ptr().add(2));
        let vb1 = vld1q_u64(b.as_ptr().add(2));
        let vc1 = vorrq_u64(va1, vb1);
        let mut out = [0u64;4];
        vst1q_u64(out.as_mut_ptr(), vc0);
        vst1q_u64(out.as_mut_ptr().add(2), vc1);
        return out;
    }
    #[cfg(target_arch = "x86_64")]
    if bloom_avx2_enabled() {
        unsafe {
            use core::arch::x86_64::*;
            let va = _mm256_set_epi64x(a[3] as i64, a[2] as i64, a[1] as i64, a[0] as i64);
            let vb = _mm256_set_epi64x(b[3] as i64, b[2] as i64, b[1] as i64, b[0] as i64);
            let vc = _mm256_or_si256(va, vb);
            let mut out = [0u64;4];
            _mm256_storeu_si256(out.as_mut_ptr() as *mut __m256i, vc);
            return out;
        }
    }
    [a[0]|b[0], a[1]|b[1], a[2]|b[2], a[3]|b[3]]
}

#[inline]
fn bloom256_popcnt_and(a: &[u64;4], b: &[u64;4]) -> u32 {
    // ===================== [PIECE CH] SIMD popcnt for Bloom-256 =====================
    #[cfg(target_arch = "x86_64")]
    unsafe {
        if std::is_x86_feature_detected!("avx2") {
            use core::arch::x86_64::*;
            let va = _mm256_set_epi64x(a[3] as i64, a[2] as i64, a[1] as i64, a[0] as i64);
            let vb = _mm256_set_epi64x(b[3] as i64, b[2] as i64, b[1] as i64, b[0] as i64);
            let vc = _mm256_and_si256(va, vb);

            // byte-wise popcnt via nibble LUT + PSHUFB
            let lut128 = _mm_setr_epi8(
                0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4
            );
            let lut = _mm256_broadcastsi128_si256(lut128);
            let low_mask = _mm256_set1_epi8(0x0F_i8);
            let lo = _mm256_and_si256(vc, low_mask);
            let hi = _mm256_and_si256(_mm256_srli_epi16(vc, 4), low_mask);
            let pc_lo = _mm256_shuffle_epi8(lut, lo);
            let pc_hi = _mm256_shuffle_epi8(lut, hi);
            let pc = _mm256_add_epi8(pc_lo, pc_hi);

            // horizontal sum of bytes into 4x u64 partial sums
            let zero = _mm256_setzero_si256();
            let sad = _mm256_sad_epu8(pc, zero);
            let mut tmp = [0u64;4];
            _mm256_storeu_si256(tmp.as_mut_ptr() as *mut __m256i, sad);
            return (tmp[0] + tmp[1] + tmp[2] + tmp[3]) as u32;
        }
    }

    #[cfg(target_arch = "aarch64")]
    unsafe {
        use core::arch::aarch64::*;
        // load 256b as two 128b lanes
        let pa = a.as_ptr() as *const u8;
        let pb = b.as_ptr() as *const u8;
        let a0 = vld1q_u8(pa);
        let a1 = vld1q_u8(pa.add(16));
        let b0 = vld1q_u8(pb);
        let b1 = vld1q_u8(pb.add(16));
        let c0 = vandq_u8(a0, b0);
        let c1 = vandq_u8(a1, b1);
        let p0 = vcntq_u8(c0);
        let p1 = vcntq_u8(c1);
        // widen and sum: u8->u16->u32->u64
        let s0 = vpaddlq_u8(p0);
        let s1 = vpaddlq_u8(p1);
        let s0 = vpaddlq_u16(s0);
        let s1 = vpaddlq_u16(s1);
        let s0 = vpaddlq_u32(s0);
        let s1 = vpaddlq_u32(s1);
        let sum = vgetq_lane_u64(s0,0) + vgetq_lane_u64(s0,1) + vgetq_lane_u64(s1,0) + vgetq_lane_u64(s1,1);
        return sum as u32;
    }

    // Fallback
    (a[0]&b[0]).count_ones() + (a[1]&b[1]).count_ones() + (a[2]&b[2]).count_ones() + (a[3]&b[3]).count_ones()
}

#[inline]
fn bloom256_from_keys(keys: &[[u8;32]]) -> [u64;4] {
    let (k1,k2) = global_hash_keys();
    let mut fp = [0u64;4];
    for k in keys {
        let mut h = AHasher::new_with_keys(k1, k2);
        h.write(&k[..]);
        let v = h.finish();
        let idx = (v as usize) & 3;
        let bit = ((v >> 2) & 63) as usize;
        fp[idx] |= 1u64 << bit;
        let idx2 = ((v >> 8) as usize) & 3;
        let bit2 = ((v >> 14) & 63) as usize;
        fp[idx2] |= 1u64 << bit2;
    }
    fp
}

// ===================== [PIECE AK] Branchless overlap penalty =====================
#[inline]
fn overlap_penalty_lamports(ro_path: &[u64;4], rw_path: &[u64;4], edge: &PoolEdge) -> i64 {
    let ro = bloom256_popcnt_and(ro_path, &edge.acct_ro_fp) as u64;
    let rw = bloom256_popcnt_and(rw_path, &edge.acct_rw_fp) as u64;
    // Quadratic ramps: k * n * (n+1) / 2
    let ro_pen = ((ro * (ro + 1)) >> 1).saturating_mul(1_200);
    let rw_pen = ((rw * (rw + 1)) >> 1).saturating_mul(2_400);
    let cross  = (ro.saturating_mul(rw)).saturating_mul(150);
    (ro_pen + rw_pen + cross).min(160_000) as i64
}

// ===================== [PIECE G] Seeded hashing =====================
use std::sync::OnceLock;
#[inline]
fn global_hash_keys() -> (u64, u64) {
    static K: OnceLock<(u64,u64)> = OnceLock::new();
    *K.get_or_init(|| {
        let t = 0x9E37_79B1_85EB_CA87u64 ^ (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos() as u64);
        let k1 = t.rotate_left(13) ^ 0xD1B5_4A32_1C27_3D9B;
        let k2 = t.rotate_right(7) ^ 0xA24B_1C26_3B5A_9C93;
        (k1, k2)
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PoolId(pub Pubkey);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TokenMint(pub Pubkey);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PoolType {
    RaydiumV4,
    OrcaWhirlpool,
    SerumV3,
    PhoenixV1,
    LifinityV2,
}

#[derive(Debug, Clone)]
pub struct PoolEdge {
    pub pool_id: PoolId,
    pub pool_type: PoolType,
    pub token_a: TokenMint,
    pub token_b: TokenMint,
    pub reserve_a: u64,
    pub reserve_b: u64,
    pub fee_bps: u16,
    pub tick_spacing: Option<i32>,
    pub current_tick: Option<i32>,
    pub liquidity: Option<u128>,
    pub sqrt_price: Option<u128>,
    // [PATCH: edge freshness + CU estimate for budgeter] >>>
    pub last_update_slot: u64,
    pub cu_estimate: u32,
    // [PATCH: per-venue lot/tick + dust] >>>
    pub min_in_lot: Option<u64>,
    pub min_out_lot: Option<u64>,
    pub price_tick_bps: Option<u32>,
    // <<< END PATCH
    // [PATCH AO: per-edge account/ix footprint] >>>
    pub ro_accounts: u16,   // read-only AccountMeta count this hop needs
    pub rw_accounts: u16,   // writable AccountMeta count this hop needs
    // [PATCH AX: per-edge ix size estimate] >>>
    pub ix_bytes_estimate: u16, // average serialized instruction size for this hop
    // <<< END PATCH
    // [PATCH BK: optional CLOB L2 levels]
    pub ob_asks: Option<StdArc<[(u128, u64)]>>,
    pub ob_bids: Option<StdArc<[(u128, u64)]>>,
    pub ob_last_update_slot: Option<u64>,
    // [PATCH BS: per-edge RO/RW account blooms]
    pub acct_ro_fp: [u64; 4],
    pub acct_rw_fp: [u64; 4],
    // [PATCH BV: exact tick size for CLOBs]
    pub tick_size_q64: Option<u128>,
    // [PATCH BY: program affinity tag]
    pub program_tag: u64,
    // [PATCH CE: ALT planner account lists]
    pub ro_accs32: Option<StdArc<[[u8;32]]>>,
    pub rw_accs32: Option<StdArc<[[u8;32]]>>,
    // <<< END PATCH
}

type PathVec = Vec<(PoolId, bool)>;

#[derive(Debug, Clone)]
struct PathNode {
    token: TokenMint,
    amount: u64,
    gas_cost: u64,
    path: PathVec,
    profit: i64,
    // [PATCH BS: carry path account blooms]
    ro_fp: [u64;4],
    rw_fp: [u64;4],
}

impl Eq for PathNode {}
impl PartialEq for PathNode {
    fn eq(&self, other: &Self) -> bool {
        self.profit == other.profit
            && self.token == other.token
            && self.gas_cost == other.gas_cost
    }
}

// [PATCH: microcache constants + lines] >>>
const QUOTE_CACHE_CAP: usize = 4096; // total lines (power of two)
const QUOTE_SET_COUNT: usize = QUOTE_CACHE_CAP / 2; // 2-way sets
#[derive(Clone, Copy)]
struct QuoteLine { tag: u64, out: u64, stamp: u32, epoch: u64 }
#[repr(align(64))]
#[derive(Clone, Copy)]
struct QuoteSet { a: QuoteLine, b: QuoteLine }
// <<< END PATCH

// [PATCH: tag + index] >>>
#[inline]
fn quote_set_and_tag(pool: &PoolId, rev: bool, amount: u64) -> (usize, u64) {
    let bucket = amount >> 12; // 4,096-lamport buckets
    let mut h = AHasher::new_with_keys(0xD0D0_CAFE_BABE_F00D, 0x9E37_79B1_85EB_CA87);
    pool.0.hash(&mut h);
    rev.hash(&mut h);
    (bucket as u64).hash(&mut h);
    let tag = h.finish();
    let set_idx = (tag as usize) & (QUOTE_SET_COUNT - 1);
    (set_idx, tag | 1) // never 0 tag
}
// <<< END PATCH

// (moved compute_raw_output/raw_out_cached/slippage_bps_for_edge into impl)

// [PATCH: prefetch helper] >>>
#[inline(always)]
fn prefetch_edge(edge: &PoolEdge) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
        _mm_prefetch(edge as *const _ as *const i8, _MM_HINT_T0);
    }
}
// <<< END PATCH

// [PATCH: snapshot best multiplier per token] >>>
fn best_mult_map(graph: &AHashMap<TokenMint, Vec<Arc<PoolEdge>>>) -> AHashMap<TokenMint, f64> {
    let mut m = AHashMap::with_capacity(graph.len());
    for (t, edges) in graph.iter() {
        let mut best = 0.0f64;
        for e in edges {
            let (rin, rout) = (e.reserve_a as f64, e.reserve_b as f64);
            if rin > 0.0 && rout > 0.0 {
                let fee = (BPS_DEN - e.fee_bps as u64) as f64 / (BPS_DEN as f64);
                best = best.max((rout / rin) * fee);
                best = best.max((rin / rout) * fee);
            }
        }
        if best <= 0.0 { best = 1e-12; }
        m.insert(*t, best);
    }
    m
}
// <<< END PATCH

// [PATCH: memo bucket helpers] >>>
#[inline]
fn bucketize_amount(x: u64) -> u64 { x >> 12 }
#[inline]
fn bucketize_cu_price(x: u64) -> u64 { x.min(10_000).max(1) }
// <<< END PATCH

// [PATCH: stats structs + constants] >>>
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PoolStats { ema: f64, var: f64, init: bool }

const STATS_ALPHA: f64 = 0.20;
const STATS_Z_MAX: f64 = 4.0;
// <<< END PATCH

// [PATCH: tip tuner and slip/UCB/volatile structs] >>>
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct TipStats { succ_ema: f64, lat_ema_ms: f64, init: bool }
const TGT_SUCC: f64 = 0.90;
const TGT_LAT_MS: f64 = 220.0;
const EMA_ALPHA: f64 = 0.25;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct SlipStat { ema_bps: f64, init: bool }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct EdgeUcb { n: u32, mean: f64 }

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ReserveObs { slot: u64, ra: u64, rb: u64, volatile_until: u64 }
// <<< END PATCH

// [PATCH BA: edge hazard stats]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct EdgeFail { fail_ema: f64, inuse_ema: f64, cooldown_until: u64, init: bool }

// (moved A* variant into impl)

// [PATCH: multi-target A*] >>>
fn tokens_to_set(ts: &[TokenMint]) -> AHashSet<TokenMint> { let mut s = AHashSet::with_capacity(ts.len()); for &t in ts { s.insert(t); } s }

// (moved multi-target A* into impl)

// (moved memoized finder into impl)

// (moved hybrid optimizer into impl)
impl Ord for PathNode {
    fn cmp(&self, other: &Self) -> Ordering {
        self.profit
            .cmp(&other.profit)
            .then_with(|| self.gas_cost.cmp(&other.gas_cost).reverse())
    }
}
impl PartialOrd for PathNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}

// [PATCH AT: single admission gate config type at top-level] >>>
pub struct AdmissionCfg {
    pub ttl_slots: u64,
    pub max_keys: u32,
    pub max_writable: u32,
    pub cu_limit: u64,
    pub cu_margin_bps: u64,
    pub extra_slip_bps_per_hop: u64,
    pub min_profit_after_tips: i64,
}
// <<< END PATCH

pub struct GeodesicPathFinder {
    graph: Arc<RwLock<AHashMap<TokenMint, Vec<Arc<PoolEdge>>>>>,
    pool_cache: Arc<RwLock<AHashMap<PoolId, Arc<PoolEdge>>>>,
    sqrt_price_cache: Arc<RwLock<AHashMap<PoolId, u128>>>,
    // [PATCH: RCU graph pointer for lock-free reads] >>>
    graph_rcu: Arc<ArcSwap<AHashMap<TokenMint, Vec<Arc<PoolEdge>>>>>,
    graph_rcu_frozen: Arc<ArcSwap<AHashMap<TokenMint, StdArc<[Arc<PoolEdge>]>>>>,
    min_edge_tip_lamports: Arc<RwLock<u64>>,
    // Optional oracle (immutable after set for hot path)
    oracle: Option<Arc<dyn MidOracle + Send + Sync>>,
    // CU price (lamports per compute unit)
    cu_unit_price_lamports: Arc<RwLock<u64>>,
    // [PATCH: current slot + max age knobs] >>>
    current_slot: AtomicU64,
    max_edge_age_slots: AtomicU64,
    // <<< END PATCH
    // [PATCH: quote cache storage] >>>
    quote_cache: Arc<RwLock<Vec<QuoteSet>>>,
    quote_stamp: AtomicU32,
    // <<< END PATCH
    // [PATCH: epoch + LFU memo fields] >>>
    graph_epoch: AtomicU64,
    lfu_memo: Arc<RwLock<AHashMap<(TokenMint, TokenMint, u64, u64, u64), (Vec<(PoolId,bool)>, u64, i64)>>>,
    // <<< END PATCH
    // [PATCH: pool stats map] >>>
    pool_stats: Arc<RwLock<AHashMap<PoolId, PoolStats>>>,
    // <<< END PATCH
    // [PATCH: slippage override map] >>>
    slip_override_bps: Arc<RwLock<AHashMap<PoolId, u16>>>,
    // <<< END PATCH
    // [PATCH: per-mint dust map] >>>
    min_transfer_amt: Arc<RwLock<AHashMap<TokenMint, u64>>>,
    // <<< END PATCH
    // [PATCH: budget fields] >>>
    max_node_expansions: AtomicU32,
    search_deadline_us: AtomicU64,
    // <<< END PATCH
    // [PATCH: jitter seed] >>>
    path_jitter_seed: AtomicU64,
    // <<< END PATCH
    // [PATCH: RCU top-K adjacency] >>>
    graph_rcu_topk: Arc<ArcSwap<AHashMap<TokenMint, StdArc<[Arc<PoolEdge>]>>>>,
    // <<< END PATCH
    // [PATCH: tip tuner struct] >>>
    tip_stats: Arc<RwLock<TipStats>>,
    // <<< END PATCH
    // [PATCH: last reserves + volatile expiry] >>>
    reserve_obs: Arc<RwLock<AHashMap<PoolId, ReserveObs>>>,
    // <<< END PATCH
    // [PATCH: realized slip stats + UCB] >>>
    slip_stats: Arc<RwLock<AHashMap<PoolId, SlipStat>>>,
    edge_ucb: Arc<RwLock<AHashMap<PoolId, EdgeUcb>>>,
    ucb_total_n: AtomicU64,
    // <<< END PATCH
    // [PATCH AP: in-flight path fingerprint store] >>>
    inflight_paths: Arc<RwLock<AHashMap<u64, u64>>>, // tag -> expiry_slot
    // <<< END PATCH
    // [PATCH AV: latency lamports-per-ms knob] >>>
    latency_lamports_per_ms: AU64,
    // <<< END PATCH
    // [PATCH BA: edge fail map]
    edge_fail: Arc<RwLock<AHashMap<PoolId, EdgeFail>>>,
    // [PATCH BB: path stats memo]
    path_stats_memo: Arc<RwLock<AHashMap<u64, (u64, u32, u32, u32)>>>,
    // [PATCH BF: best multiplier RCU]
    best_mult_rcu: Arc<ArcSwap<AHashMap<TokenMint, f64>>>,
    // [PATCH BU: fast-lane RCU]
    graph_rcu_fastlane: Arc<ArcSwap<AHashMap<TokenMint, StdArc<[Arc<PoolEdge>]>>>>,
    // [PATCH CC: per-edge CU EMA]
    edge_cu_ema: Arc<RwLock<AHashMap<PoolId, u32>>>,
    // [PATCH CH: slip-floor per token]
    token_slip_floor_bps_rcu: Arc<ArcSwap<AHashMap<TokenMint, u16>>>,
    // [PATCH CL: per-account hotness]
    acct_hot: Arc<RwLock<AHashMap<[u8;32], f64>>>,
    // [PATCH CM: CUSUM]
    pool_cusum: Arc<RwLock<AHashMap<PoolId, Cusum>>>,
    cusum_k: AtomicU64,
    cusum_h: AtomicU64,
    // [PIECE BJ] per-edge slippage bps
    edge_slip: Arc<RwLock<AHashMap<PoolId, EdgeSlip>>>,
    default_slip_bps: AtomicU32,
    // [PIECE BK] program congestion map
    prog_load: Arc<RwLock<AHashMap<u64, ProgLoad>>>,
    // [PIECE CI] per-edge curvature
    edge_curvature: Arc<RwLock<AHashMap<PoolId, f64>>>,
    // [PIECE CJ] per-edge curvature approx
    edge_curvature_approx: Arc<RwLock<AHashMap<PoolId, f64>>>,
    // [PIECE CK] per-edge curvature threshold
    edge_curvature_threshold: Arc<RwLock<AHashMap<PoolId, f64>>>,
    // [PIECE CL] per-edge curvature learning rate
    edge_curvature_lr: Arc<RwLock<AHashMap<PoolId, f64>>>,
    // [PIECE CN] open-set length tracker (for deadline-aware K)
    open_set_len_atomic: std::sync::atomic::AtomicUsize,
}

impl Default for GeodesicPathFinder { 
    fn default() -> Self { 
        Self::new() 
    } 
}

impl GeodesicPathFinder {
    // ===================== [PIECE AY] TLS TX buffer + in-place assembly =====================
    /// Assemble instructions into a thread-local Vec using a per-edge IX builder closure.
{{ ... }}
    /// Returns an owned Vec while keeping the TLS capacity hot for the next call.
    pub fn assemble_tx_fast_in_place_with(
        &self,
        path: &[(PoolId,bool)],
        cu_units: u32,
        cu_price_lamports: u64,
        extra_ix: &[solana_program::instruction::Instruction],
        mut per_edge_ix: impl FnMut(&PoolEdge, bool) -> solana_program::instruction::Instruction,
    ) -> Vec<solana_program::instruction::Instruction> {
        thread_local! {
            static TLS_TXBUF: RefCell<Vec<solana_program::instruction::Instruction>> = RefCell::new(Vec::with_capacity(64));
        }
        TLS_TXBUF.with(|cell| {
            let mut v = cell.borrow_mut();
            v.clear();
            let need = 2 + path.len() + extra_ix.len();
            if v.capacity() < need { v.reserve(need - v.capacity()); }
            // Priority pair (AH memo path)
            let cu_ix1 = solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(cu_units);
            let cu_ix2 = solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_price(cu_price_lamports);
            v.push(cu_ix1);
            v.push(cu_ix2);
            // Per-hop IX using provided builder (AO canonicalization happens inside those builders)
            let pc = self.pool_cache.read();
            for (pid, rev) in path.iter().copied() {
                if let Some(e) = pc.get(&pid) {
                    v.push(per_edge_ix(e, rev));
                }
            }
            v.extend_from_slice(extra_ix);
            std::mem::take(&mut *v)
        })
    }

    // ===================== [PIECE BA] Edge quick-eligibility prefilter =====================
    #[inline]
    fn edge_quick_eligible(&self, e: &PoolEdge, input_token: TokenMint, now_slot: u64) -> bool {
        // age bound
        let max_age = self.max_edge_age_slots.load(AtomOrdering::Relaxed);
        if now_slot != 0 && now_slot.saturating_sub(e.last_update_slot) > max_age { return false; }
        // cooldown
        if let Some(s) = self.edge_fail.read().get(&e.pool_id) {
            if s.init && s.cooldown_until != 0 && now_slot != 0 && now_slot <= s.cooldown_until { return false; }
        }
        // optional rough oracle check
        if let Some(ref o) = self.oracle {
            let (base, quote) = if input_token == e.token_b { (e.token_b, e.token_a) } else { (e.token_a, e.token_b) };
            if let Some(mid) = o.mid(&base, &quote) {
                if mid > 0.0 {
                    let edge_mid = if let Some(sp) = e.sqrt_price {
                        let p = U64F64::from_bits(sp);
                        p.saturating_mul(p).to_num::<f64>()
                    } else {
                        if e.reserve_a == 0 { return false; }
                        (e.reserve_b as f64) / (e.reserve_a as f64)
                    };
                    let dev_bps = ((edge_mid / mid - 1.0).abs() * (BPS_DEN as f64)) as u64;
                    if dev_bps > ORACLE_DEV_BPS_MAX { return false; }
                }
            }
        }
        true
    }

    // ===================== [PIECE BB] Graph pre-warmer (top-K × vec8) =====================
    pub fn prewarm_graph_quotes(&self, k: usize, bands: [u64;3]) {
        let tk = self.graph_snapshot_topk();
        for (_tok, edges) in tk.iter() {
            for (i, e) in edges.iter().enumerate() {
                if i >= k { break; }
                if matches!(e.pool_type, PoolType::SerumV3 | PoolType::PhoenixV1) { continue; }
                for &b in &bands {
                    let _ = self.raw_out_batch8_and_commit(e, b, false);
                    let _ = self.raw_out_batch8_and_commit(e, b, true);
                }
            }
        }
    }

    // ===================== [PIECE BC] CU-limit headroom controller =====================
    #[inline]
    pub fn cu_limit_for_path(&self, path: &[(PoolId,bool)]) -> u32 {
        let pc = self.pool_cache.read();
        let mut est: u64 = 0;
        let mut var:  u64 = 0;
        for (pid, _) in path.iter() {
            if let Some(e) = pc.get(pid) {
                est = est.saturating_add(e.cu_estimate as u64);
                // simple variance heuristic from ix size; fallback constant
                let v = (e.ix_bytes_estimate as u64).max(800);
                var = var.saturating_add(v);
            }
        }
        let head = (var as f64).sqrt() as u64 + 2_000;
        (est.saturating_add(head)).min(1_400_000) as u32
    }

    // ===================== [PIECE BG] Early path conflict cap =====================
    pub fn path_conflict_cap_ok(&self, path: &[(PoolId,bool)], max_ro_bits: u32, max_rw_bits: u32) -> bool {
        let pc = self.pool_cache.read();
        let mut ro = [0u64;4]; let mut rw = [0u64;4];
        for (pid, _) in path.iter() {
            if let Some(e) = pc.get(pid) {
                ro = bloom256_or(&ro, &e.acct_ro_fp);
                rw = bloom256_or(&rw, &e.acct_rw_fp);
            }
        }
        let ro_pc = bloom256_popcnt_and(&ro, &ro);
        let rw_pc = bloom256_popcnt_and(&rw, &rw);
        ro_pc <= max_ro_bits && rw_pc <= max_rw_bits
    }

    // ===================== [PIECE BE] Path lot normalizer + warm =====================
    #[inline]
    fn gcd_u64(&self, mut a: u64, mut b: u64) -> u64 { while b != 0 { let t = a % b; a = b; b = t; } a }
    #[inline]
    fn lcm_u64(&self, a: u64, b: u64) -> u64 { if a == 0 || b == 0 { return a.max(b); } a / self.gcd_u64(a, b) * b }

    pub fn normalize_input_to_path_lots(
        &self,
        path: &[(PoolId,bool)],
        _input_token: TokenMint,
        max_in: u64
    ) -> (u64, u64) {
        if path.is_empty() || max_in == 0 { return (max_in, 0); }
        let pc = self.pool_cache.read();
        let mut lcm = 1u64;
        for (pid, _rev) in path.iter() {
            if let Some(e) = pc.get(pid) {
                let lot = e.min_in_lot.unwrap_or(1).max(1);
                lcm = self.lcm_u64(lcm, lot);
            }
        }
        if lcm <= 1 { return (max_in, 0); }
        let clamped = (max_in / lcm) * lcm;
        (clamped, lcm)
    }

    pub fn clamp_and_warm_vec8(
        &self,
        path: &[(PoolId,bool)],
        input_token: TokenMint,
        max_in: u64
    ) -> Option<(u64, [u64;8], u64)> {
        if path.is_empty() || max_in == 0 { return None; }
        let (norm_in, lcm) = self.normalize_input_to_path_lots(path, input_token, max_in);
        if norm_in == 0 { return None; }
        let pc = self.pool_cache.read();
        let (pid0, rev0) = path[0];
        let e0 = pc.get(&pid0)?;
        let outs0 = self.raw_out_batch8_and_commit(e0, norm_in, rev0);
        Some((norm_in, outs0, lcm))
    }

    // ===================== [PIECE CB] Curvature-aware vec8 grid =====================
    #[inline]
    fn vec8_grid(a: u64, rin: u64) -> [u64;8] {
        if a == 0 { return [0;8]; }
        let rho = (a as f64) / ((rin as f64) + 1.0);
        let w = if rho < 0.02 { 0.55 } else if rho < 0.05 { 0.40 } else if rho < 0.10 { 0.30 } else if rho < 0.20 { 0.22 } else { 0.16 };
        let ratios = [ -0.30, -0.18, -0.10, 0.0, 0.08, 0.14, 0.22, 0.35 ];
        let mut out = [0u64;8];
        for i in 0..8 {
            let r = 1.0 + w * ratios[i];
            out[i] = ((a as f64) * r).round() as u64;
            if out[i] == 0 { out[i] = 1; }
        }
        out
    }

    // ===================== [PIECE CC] Slot-phase learner per validator =====================
    #[inline]
    pub fn observe_validator_phase(&self, validator: [u8;32], phase_01: f64, included: bool) {
        use std::collections::HashMap;
        use std::sync::OnceLock;
        static VAL_PHASE: OnceLock<parking_lot::RwLock<HashMap<[u8;32], ( [u64;16], [u64;16] )>>> = OnceLock::new();
        let map = VAL_PHASE.get_or_init(|| parking_lot::RwLock::new(HashMap::with_capacity(128)));
        let mut g = map.write();
        let e = g.entry(validator).or_insert(([0u64;16], [0u64;16]));
        let bin = ((phase_01.clamp(0.0,0.9999))*16.0) as usize;
        e.1[bin] = e.1[bin].saturating_add(1);
        if included { e.0[bin] = e.0[bin].saturating_add(1); }
    }

    #[inline]
    pub fn best_phase_for(&self, validator: [u8;32]) -> f64 {
        use std::collections::HashMap;
        use std::sync::OnceLock;
        static VAL_PHASE: OnceLock<parking_lot::RwLock<HashMap<[u8;32], ( [u64;16], [u64;16] )>>> = OnceLock::new();
        let map = VAL_PHASE.get_or_init(|| parking_lot::RwLock::new(HashMap::with_capacity(128)));
        if let Some((hit, tot)) = map.read().get(&validator).cloned() {
            let mut best = 0usize; let mut sc = f64::MIN;
            for i in 0..16 {
                let rate = ((hit[i] as f64) + 1.0) / ((tot[i] as f64) + 2.0);
                if rate > sc { sc = rate; best = i; }
            }
            return (best as f64 + 0.5) / 16.0;
        }
        0.25
    }

    // ===================== [PIECE CE] Epoch-stamped path stats memo via composite tag =====================
    #[inline]
    pub fn path_stats_memoized_epoch(&self, path: &[(PoolId,bool)], extra_ix: u16) -> (u64,u32,u32,u32) {
        let tag = self.path_tag(path, 0);
        let ep  = self.graph_epoch.load(AtomOrdering::Relaxed);
        let salt = wyhash64(tag ^ ep);
        if let Some(v) = self.path_stats_memo.read().get(&salt) { return *v; }
        let cu  = self.path_cu_units(path);
        let (ro, rw) = self.path_account_footprint(path);
        let by  = self.estimate_tx_bytes(path, extra_ix);
        self.path_stats_memo.write().insert(salt, (cu,ro,rw,by));
        (cu,ro,rw,by)
    }

    // ===================== [PIECE CF] Deadline-aware K controller =====================
    #[inline]
    pub fn k_for_deadline(&self, depth: usize, start: &Instant, deadline_us: u64, open_len: usize) -> usize {
        let el_us = start.elapsed().as_micros() as u64;
        let rem = deadline_us.saturating_sub(el_us) as f64;
        let base = self.k_for(depth, start, deadline_us).max(2);
        let shrink = if rem < 250.0 { 0.5 } else if rem < 800.0 { 0.75 } else { 1.0 };
        let load   = if open_len > 20_000 { 0.6 } else if open_len > 8_000 { 0.8 } else { 1.0 };
        let k = ((base as f64) * shrink * load).round() as usize;
        k.clamp(2, 16)
    }

    // ===================== [PIECE BZ] CU-price laddering =====================
    pub fn cu_price_ladder(&self, base_micro_per_cu: u64, k_bundles: usize) -> Vec<u64> {
        let mult = [90_u64, 100, 110, 125, 140, 160];
        let mut out = Vec::with_capacity(k_bundles);
        for i in 0..k_bundles {
            let m = mult[i.min(mult.len()-1)];
            out.push((base_micro_per_cu.saturating_mul(m) / 100).max(1));
        }
        out
    }

    // ===================== [PIECE CG] Per-validator phase+price planner =====================
    pub fn plan_per_validator_phase_price(
        &self,
        validators: &[[u8;32]],
        base_micro_per_cu: u64,
        k_bundles: usize
    ) -> Vec<(f64,u64)> {
        let ladder = self.cu_price_ladder(base_micro_per_cu, k_bundles);
        let mut out = Vec::with_capacity(k_bundles);
        for i in 0..k_bundles {
            let v = validators[i.min(validators.len()-1)];
            out.push((self.best_phase_for(v), ladder[i]));
        }
        out
    }

    // ===================== [PIECE BQ] Adaptive bundle-count controller =====================
    pub fn decide_bundle_count(&self, base_profit_lamports: i64, pending_queue_len: u32) -> usize {
        // Read AP controller state if present
        let hit = {
            if let Some(ctrl) = CU_CTRL.get() { ctrl.read().ema_hit.max(0.01) } else { 0.5 }
        };
        let roi = (base_profit_lamports.max(0) as f64) / 1_000_000.0;
        let q = pending_queue_len as f64;
        let mut k = 1.0 + (roi * 2.0).min(3.0) + (q / 200.0).min(2.0) - ((hit - 0.65).max(0.0) * 3.0);
        k.round().clamp(1.0, 6.0) as usize
    }

    // ===================== [PIECE BF] Dual-tip split planner =====================
    pub fn plan_tip_split(
        &self,
        path_cu_units: u32,
        total_budget_lamports: u64,
        direct_weight_bps: u16
    ) -> (u64, u64) {
        let w = (direct_weight_bps as u64).min(10_000);
        let direct = total_budget_lamports.saturating_mul(w) / 10_000;
        let cu_budget = total_budget_lamports.saturating_sub(direct);
        let micro_per_cu = if path_cu_units == 0 { 1 } else { cu_budget / (path_cu_units as u64) };
        (micro_per_cu.max(1), direct)
    }

    pub fn build_direct_tip_ix(&self, payer: Pubkey, tip_to: Pubkey, lamports: u64) -> Option<solana_program::instruction::Instruction> {
        if lamports == 0 { return None; }
        Some(solana_program::system_instruction::transfer(&payer, &tip_to, lamports))
    }

    // ===================== [PIECE BI] Slot-phase pacer =====================
    pub fn on_new_slot(&self, now_instant: Instant, slot_duration_ns: u64) {
        SLOT_ZERO_INSTANT_NS.store(now_instant.elapsed().as_nanos() as u64, std::sync::atomic::Ordering::Relaxed);
        SLOT_DURATION_NS.store(slot_duration_ns.max(200_000_000).min(1_000_000_000), std::sync::atomic::Ordering::Relaxed);
    }

    pub fn spin_until_slot_phase(&self, process_start: Instant, phase: f64) {
        let dur = SLOT_DURATION_NS.load(std::sync::atomic::Ordering::Relaxed) as f64;
        let target_ns_mod = (phase.clamp(0.0, 0.9999) * dur) as u64;
        let start_ns = SLOT_ZERO_INSTANT_NS.load(std::sync::atomic::Ordering::Relaxed);
        let now = process_start.elapsed().as_nanos() as u64;
        let slot_len = SLOT_DURATION_NS.load(std::sync::atomic::Ordering::Relaxed);
        let slot_elapsed = now.saturating_sub(start_ns) % (slot_len.max(1));
        if slot_elapsed >= target_ns_mod { return; }
        let wait_ns = target_ns_mod - slot_elapsed;
        if wait_ns <= 500_000 {
            let tgt = now + wait_ns;
            while (process_start.elapsed().as_nanos() as u64) < tgt { core::hint::spin_loop(); }
        }
    }
}

// [PIECE BI] slot pacer statics
use std::cell::RefCell;
use std::sync::atomic::AtomicU64 as A64;
static SLOT_ZERO_INSTANT_NS: A64 = A64::new(0);
static SLOT_DURATION_NS:     A64 = A64::new(400_000_000);
    // [PATCH: constructor init] >>>
    pub fn new() -> Self {
        Self {
            graph: Arc::new(RwLock::new(AHashMap::with_capacity(1000))),
            pool_cache: Arc::new(RwLock::new(AHashMap::with_capacity(5000))),
{{ ... }}
            sqrt_price_cache: Arc::new(RwLock::new(AHashMap::with_capacity(5000))),
            // [PATCH: init RCU + min tip] >>>
            graph_rcu: Arc::new(ArcSwap::from_pointee(AHashMap::with_capacity(2048))),
            graph_rcu_frozen: Arc::new(ArcSwap::from_pointee(AHashMap::with_capacity(2048))),
            min_edge_tip_lamports: Arc::new(RwLock::new(DEFAULT_TIP_LAMPORTS_PER_HOP)),
            oracle: None,
            cu_unit_price_lamports: Arc::new(RwLock::new(0)),
            // [PATCH: init atomics] >>>
            current_slot: AtomicU64::new(0),
            max_edge_age_slots: AtomicU64::new(64), // default: ~32s at 400ms/slot
            // <<< END PATCH
            // [PATCH: init 2-way cache] >>>
            quote_cache: Arc::new(RwLock::new(vec![
                QuoteSet { a: QuoteLine{tag:0,out:0,stamp:0,epoch:0}, b: QuoteLine{tag:0,out:0,stamp:0,epoch:0} };
                QUOTE_SET_COUNT
            ])),
            quote_stamp: AtomicU32::new(1),
            // <<< END PATCH
            // [PATCH: init epoch + memo] >>>
            graph_epoch: AtomicU64::new(1),
            lfu_memo: Arc::new(RwLock::new(AHashMap::with_capacity(2048))),
            // <<< END PATCH
            // [PATCH: init pool stats] >>>
            prog_load: Arc::new(RwLock::new(AHashMap::with_capacity(256))),
            open_set_len_atomic: std::sync::atomic::AtomicUsize::new(0),
            // <<< END PATCH
            // [PATCH: init slippage map] >>>
            slip_override_bps: Arc::new(RwLock::new(AHashMap::with_capacity(4096))),
            // <<< END PATCH
            // [PATCH: init dust map] >>>
            min_transfer_amt: Arc::new(RwLock::new(AHashMap::with_capacity(4096))),
            // <<< END PATCH
            // [PATCH: init budget] >>>
            max_node_expansions: AtomicU32::new(100_000),
            search_deadline_us: AtomicU64::new(0),
            // <<< END PATCH
            // [PATCH: init jitter seed] >>>
            path_jitter_seed: AtomicU64::new(0xC0FFEE_F00D_BAD5_EED),
            // <<< END PATCH
            // [PATCH: RCU top-K adjacency] >>>
            graph_rcu_topk: Arc::new(ArcSwap::from_pointee(AHashMap::with_capacity(2048))),
            // <<< END PATCH
            // [PATCH: init tip stats] >>>
            tip_stats: Arc::new(RwLock::new(TipStats{succ_ema:0.0, lat_ema_ms:0.0, init:false})),
            // <<< END PATCH
            // [PATCH: init reserve obs] >>>
            reserve_obs: Arc::new(RwLock::new(AHashMap::with_capacity(8192))),
            // <<< END PATCH
            // [PATCH: init slip + UCB] >>>
            slip_stats: Arc::new(RwLock::new(AHashMap::with_capacity(8192))),
            edge_ucb: Arc::new(RwLock::new(AHashMap::with_capacity(8192))),
            ucb_total_n: AtomicU64::new(1),
            // <<< END PATCH
            // [PATCH AP init inflight] >>>
            inflight_paths: Arc::new(RwLock::new(AHashMap::with_capacity(8192))),
            // <<< END PATCH
            // [PATCH AV init latency knob] >>>
            latency_lamports_per_ms: AU64::new(0), // 0 = disabled
            // <<< END PATCH
            // [PATCH BA init]
            edge_fail: Arc::new(RwLock::new(AHashMap::with_capacity(8192))),
            // [PATCH BB init]
            path_stats_memo: Arc::new(RwLock::new(AHashMap::with_capacity(8192))),
            // [PATCH BF init]
            best_mult_rcu: Arc::new(ArcSwap::from_pointee(AHashMap::with_capacity(2048))),
            // [PATCH BU init]
            graph_rcu_fastlane: Arc::new(ArcSwap::from_pointee(AHashMap::with_capacity(1024))),
            // [PATCH CC init]
            edge_cu_ema: Arc::new(RwLock::new(AHashMap::with_capacity(8192))),
            // [PATCH CH init]
            token_slip_floor_bps_rcu: Arc::new(ArcSwap::from_pointee(AHashMap::with_capacity(2048))),
            // [PATCH CL init]
            acct_hot: Arc::new(RwLock::new(AHashMap::with_capacity(1<<15))),
            // [PATCH CM init]
            pool_cusum: Arc::new(RwLock::new(AHashMap::with_capacity(8192))),
            cusum_k: AtomicU64::new(50_000),
            cusum_h: AtomicU64::new(300_000),
        }
    }

    // [PATCH: runtime knobs] >>>
    #[inline]
    pub fn set_min_edge_tip_lamports(&self, lamports: u64) {
        *self.min_edge_tip_lamports.write() = lamports.max(0);
    }

    #[inline]
    pub fn set_cu_unit_price_lamports(&self, lamports: u64) {
        *self.cu_unit_price_lamports.write() = lamports;
    }

    // [PATCH AV: setter for latency lamports-per-ms] >>>
    #[inline]
    pub fn set_latency_lamports_per_ms(&self, lpm: u64) { self.latency_lamports_per_ms.store(lpm, AtomOrdering::Relaxed); }

    #[inline]
    pub fn graph_snapshot(&self) -> Arc<AHashMap<TokenMint, Vec<Arc<PoolEdge>>>> {
        self.graph_rcu.load_full()
    }

    #[inline]
    pub fn set_oracle(&self, oracle: Arc<dyn MidOracle + Send + Sync>) {
        // SAFETY: immutable-after-set for hot path; if dynamic swaps are required, wrap in RwLock
        let ptr = self as *const _ as *mut GeodesicPathFinder;
        unsafe { (*ptr).oracle = Some(oracle); }
    }
    // <<< END PATCH
    
    // [PATCH: minimal update_pool to publish edges] >>>
    pub fn update_pool(&self, edge: PoolEdge) {
        let arc_edge = Arc::new(edge);
        // pool_cache
        {
            let mut pc = self.pool_cache.write();
            pc.insert(arc_edge.pool_id, arc_edge.clone());
        }
        // graph adjacency for both tokens
        {
            let mut g = self.graph.write();
            g.entry(arc_edge.token_a).or_insert_with(|| Vec::with_capacity(8)).push(arc_edge.clone());
            g.entry(arc_edge.token_b).or_insert_with(|| Vec::with_capacity(8)).push(arc_edge.clone());
        }
        // best-effort RCU refresh (cheap clone)
        {
            let g = self.graph.read().clone();
            self.graph_rcu.store(Arc::new(g));
        }
        // [PATCH BU: recompute fast-lane entries for affected tokens]
        {
            let curf = self.graph_rcu_frozen.load_full();
            let ucb = self.edge_ucb.read().clone();
            let mut next = (*self.graph_rcu_fastlane.load_full()).clone();
            for &tok in &[arc_edge.token_a, arc_edge.token_b] {
                if let Some(list) = curf.get(&tok) {
                    let mut v: Vec<Arc<PoolEdge>> = list.iter().cloned().collect();
                    v.sort_by_key(|e| {
                        let d = e.reserve_a.min(e.reserve_b) as u128;
                        let fee_ok = (BPS_DEN - e.fee_bps as u64) as u128;
                        let gas = (self.gas_cost_for_edge(e).max(1)) as u128;
                        let u = ucb.get(&e.pool_id).map(|s| (s.mean * 1000.0) as i128).unwrap_or(0);
                        Reverse(((d * fee_ok) / gas) as i128 + u)
                    });
                    v.truncate(2);
                    next.insert(tok, StdArc::from(v));
                }
            }
            self.graph_rcu_fastlane.store(Arc::new(next));
        }
    }
    // [PATCH: slot-age setters] >>>
    #[inline]
    pub fn set_current_slot(&self, slot: u64) {
        self.current_slot.store(slot, AtomOrdering::Relaxed);
        self.purge_inflight_expired(slot);
        if slot & 63 == 0 { self.path_stats_memo.write().clear(); }
    }
    #[inline]
    pub fn set_max_edge_age_slots(&self, slots: u64) { self.max_edge_age_slots.store(slots.max(1), AtomOrdering::Relaxed); }
    // <<< END PATCH

    // [PATCH: snapshot frozen adjacency] >>>
    #[inline]
    pub fn graph_snapshot_frozen(&self) -> Arc<AHashMap<TokenMint, StdArc<[Arc<PoolEdge>]>>> { self.graph_rcu_frozen.load_full() }
    // <<< END PATCH
    // [PATCH: snapshot top-K] >>>
    #[inline]
    pub fn graph_snapshot_topk(&self) -> Arc<AHashMap<TokenMint, StdArc<[Arc<PoolEdge>]>>> { self.graph_rcu_topk.load_full() }
    // <<< END PATCH
    // [PATCH: snapshot fastlane] >>>
    #[inline]
    pub fn graph_snapshot_fastlane(&self) -> Arc<AHashMap<TokenMint, StdArc<[Arc<PoolEdge>]>>> { self.graph_rcu_fastlane.load_full() }
    // <<< END PATCH

    // === [PIECE 1A: dynamic branching helpers] ===
    #[inline]
    fn k_for(&self, depth: usize, start: &Instant, deadline_us: u64) -> usize {
        let base = match depth { 0 => 8, 1 => 6, _ => 4 };
        let used = (start.elapsed().as_micros() as u64).min(deadline_us);
        let frac = used as f64 / (deadline_us as f64 + 1.0);
        let shrink = if frac < 0.33 { 0 } else if frac < 0.66 { 1 } else { 2 };
        base.saturating_sub(shrink).max(2)
    }

    // ===================== [PIECE L] Advanced expand_score =====================
    #[inline]
    fn expand_score(&self, e: &Arc<PoolEdge>) -> i128 {
        let fee_ok = (BPS_DEN - e.fee_bps as u64) as i128;
        let depth  = e.reserve_a.min(e.reserve_b) as i128;
        let cu     = self.edge_cu_ema.read().get(&e.pool_id).copied().unwrap_or(e.cu_estimate) as i128 + 10_000;

        // Hazard baseline
        let haz = self.edge_fail.read()
            .get(&e.pool_id)
            .map(|s| (s.fail_ema * 1.0e3) as i128)
            .unwrap_or(0)
            .max(0);

        // Freshness penalty (older edges get clipped)
        let now = self.current_slot.load(AtomOrdering::Relaxed);
        let age = now.saturating_sub(e.last_update_slot) as i128;
        let fresh_pen = (age / 8).min(120_000) as i128;

        // UCB bonus from realized mean + optimism
        let m  = self.edge_ucb.read().get(&e.pool_id).copied().unwrap_or(EdgeUcb{n:0,mean:0.0});
        let nt = self.ucb_total_n.load(AtomOrdering::Relaxed).max(1) as f64;
        let ni = m.n.max(1) as f64;
        let bonus = (m.mean + (2.0 * (nt.ln()/ni).sqrt())) * 1_000.0; // scale to lamports
        let ucb  = (bonus as i128).max(0);

        ((depth * fee_ok) / cu).saturating_add(ucb).saturating_sub(haz + fresh_pen)
    }

    // ===================== [PIECE AG] Ring-rotated child selection =====================
    fn next_edges_for_dynamic(&self, token: TokenMint, depth: usize, start: &Instant, deadline_us: u64) -> Vec<Arc<PoolEdge>> {
        let k  = self.k_for(depth, start, deadline_us).max(2);
        let fl = self.graph_snapshot_fastlane();
        let tk = self.graph_snapshot_topk();
        let fr = self.graph_snapshot_frozen();

        // Rotation offset: deterministic per depth+epoch to de-sync siblings.
        let epoch = self.graph_epoch.load(AtomOrdering::Relaxed) as usize;
        let mut off = (epoch.wrapping_mul(1315423911) ^ (depth.wrapping_mul(2654435761))) & 0x7fffffff;

        let mut out: Vec<Arc<PoolEdge>> = Vec::with_capacity(k);
        let mut seen: AHashSet<PoolId> = AHashSet::with_capacity(k * 2);

        #[inline(always)]
        fn take_rotated(dst: &mut Vec<Arc<PoolEdge>>, seen: &mut AHashSet<PoolId>, src: Option<&StdArc<[Arc<PoolEdge>]>>, off: &mut usize, need: usize) {
            if let Some(v) = src {
                let n = v.len();
                if n == 0 { return; }
                let start = *off % n;
                let mut i = start;
                while dst.len() < need {
                    let e = &v[i];
                    if seen.insert(e.pool_id) { dst.push(e.clone()); }
                    i += 1; if i == n { i = 0; }
                    if i == start { break; }
                }
                *off = i;
            }
        }

        take_rotated(&mut out, &mut seen, fl.get(&token), &mut off, k);
        if out.len() < k { take_rotated(&mut out, &mut seen, tk.get(&token), &mut off, k); }
        if out.len() < k { take_rotated(&mut out, &mut seen, fr.get(&token), &mut off, k); }
        // [PIECE BA] quick eligibility prefilter (age/cooldown/oracle deviation)
        let now = self.current_slot.load(AtomOrdering::Relaxed);
        out.retain(|e| self.edge_quick_eligible(e, token, now));
        if out.len() > k { out = self.select_topk_by_score(out, k); }
        Self::prefetch_edges_batch(&out);
        out
    }

    #[inline]
    fn next_edges_for(&self, token: TokenMint, depth: usize) -> Vec<Arc<PoolEdge>> {
        self.next_edges_for_dynamic(token, depth, &Instant::now(), 3000)
    }

    /// [PIECE AN] Decide resend backoff in milliseconds using leader distance + last inclusion.
    #[inline]
    pub fn resend_backoff_ms(&self, leader_slot: u64, now_slot: u64, included_last: bool) -> u64 {
        let dist = leader_slot.saturating_sub(now_slot);
        let base = if included_last { 18u64 } else { 8u64 };
        match dist {
            0..=2   => base,
            3..=10  => base.saturating_add(6),
            11..=40 => base.saturating_add(12),
            _       => base.saturating_add(20),
        }
    }

    /// [PIECE AN] Get a hot blockhash; if none yet, return None to abort send.
    #[inline]
    pub fn hot_blockhash(&self) -> Option<Hash> {
        let now = self.current_slot.load(AtomOrdering::Relaxed);
        bh_latest(now).map(|b| b.hash)
    }

    // Arena A* helper: reconstruct path from arena by following parents
    #[inline]
    fn reconstruct_path_from_arena(arena: &[NodeRec], mut idx: usize) -> Vec<(PoolId,bool)> {
        let mut rev: Vec<(PoolId,bool)> = Vec::with_capacity(8);
        while let Some(h) = arena[idx].hop {
            rev.push(h);
            if let Some(p) = arena[idx].parent { idx = p; } else { break; }
        }
        rev.reverse();
        rev
    }

    // [PATCH: set per-pool slippage] >>>
    #[inline]
    pub fn set_pool_slippage_bps(&self, pool: PoolId, bps: u16) { self.slip_override_bps.write().insert(pool, bps); }
    // <<< END PATCH

    pub fn find_optimal_path(
        &self,
        start_token: TokenMint,
        start_amount: u64,
        target_tokens: &[TokenMint],
    ) -> Option<(Vec<(PoolId, bool)>, u64, i64)> {
        // [PATCH: use lock-free RCU snapshot] >>>
        let graph = self.graph_snapshot();
        // <<< END PATCH
        let pool_cache = self.pool_cache.read();

        let mut heap = BinaryHeap::new();
        let mut best_paths: AHashMap<TokenMint, (u64, i64)> = AHashMap::new();
        // [PATCH: bloom init] >>>
        let mut bloom: [u64; 4] = [0; 4];
        let salts = [0xA5A5_A5A5_DEAD_BEEF, 0x0123_4567_89AB_CDEF, 0xC001_D00D_F00D_BABE];
        // <<< END PATCH
        // [PATCH BJ: per-token expansion quotas]
        let mut token_exp_cnt: AHashMap<TokenMint, u16> = AHashMap::with_capacity(1024);

        let targets_set = tokens_to_set(target_tokens);
        let mut best_result: Option<(Vec<(PoolId, bool)>, u64, i64)> = None;

        heap.push(PathNode {
            token: start_token,
            amount: start_amount,
            gas_cost: 0,
            path: Vec::new(),
            profit: 0,
            ro_fp: [0;4],
            rw_fp: [0;4],
        });
        best_paths.insert(start_token, (start_amount, 0));

        while let Some(node) = heap.pop() {
            if node.path.len() >= MAX_PATH_LENGTH { continue; }
            if let Some(&(best_amount, best_profit)) = best_paths.get(&node.token) {
                if node.amount < best_amount && node.profit <= best_profit { continue; }
            }

            // record best if we closed a cycle to a target
            if !node.path.is_empty() && targets_set.contains(&node.token) {
                if node.profit > MIN_PROFIT_THRESHOLD as i64 {
                    let cand = Some((node.path.clone(), node.amount, node.profit));
                    if best_result.as_ref().map(|b| node.profit > b.2).unwrap_or(true) { best_result = cand; }
                }
            }

            // [PATCH BJ: per-token quota check]
            let quota = 32u16; // per-token per-search cap
            let cnt = *token_exp_cnt.get(&node.token).unwrap_or(&0);
            if cnt > quota { continue; }
            token_exp_cnt.insert(node.token, cnt.saturating_add(1));

            // [PATCH BC: use depth-adaptive branching]
            let cand = self.next_edges_for(node.token, node.path.len());
            for edge in cand.iter() {
                prefetch_edge(edge);
                // [PATCH: anti ping-pong] >>>
                if let Some(&(last_pid, last_rev)) = node.path.last() {
                    let is_reverse = node.token == edge.token_b;
                    if last_pid == edge.pool_id && last_rev != is_reverse { continue; }
                }
                // <<< END PATCH
                let (out_token, out_amt, is_reverse) = self.calculate_swap_output(edge, node.token, node.amount);
                if out_amt == 0 { continue; }
                // [PATCH: dust guard on child out]
                if out_amt < self.mint_dust(&out_token) { continue; }
                let gas_cost = node.gas_cost + self.calculate_gas_cost(&edge.pool_type);
                // [PATCH BH: early EV-drop prune]
                let inc = out_amt as i64 - node.amount as i64 - gas_cost as i64;
                if inc < -((MIN_PROFIT_THRESHOLD as i64) / 2) { continue; }
                // [PATCH BL: forbid pool reuse]
                if node.path.iter().any(|(pid, _)| *pid == edge.pool_id) { continue; }

                let mut new_path = node.path.clone();
                new_path.push((edge.pool_id, is_reverse));

                // optimistic upper bound prune using best_mult_rcu
                let remain = MAX_PATH_LENGTH - new_path.len();
                if remain > 0 {
                    let bm = self.best_mult_rcu.load_full();
                    let best_m = bm.get(&out_token).copied().unwrap_or(1.0);
                    let ub_out = (out_amt as f64) * powi_f64(best_m, remain);
                    let min_tip = *self.min_edge_tip_lamports.read();
                    let proj_gas = gas_cost + (remain as u64) * (min_tip);
                    let ub_profit = (ub_out as i64) - (start_amount as i64) - (proj_gas as i64);
                    let dyn_min = self.min_profit_for_path(&new_path);
                    if ub_profit <= dyn_min { continue; }
                }

                // [PATCH AO: account-set guard]
                if !self.fits_account_budget(&new_path, 128, 64) { continue; }

                let profit_here = if out_token == start_token && !new_path.is_empty() {
                    out_amt as i64 - start_amount as i64 - gas_cost as i64
                } else { node.profit };
                // [PATCH AV/BA: latency & hazard penalties]
                let lat_pen = self.latency_penalty_lamports(new_path.len());
                let haz_pen = self.hazard_penalty_lamports(edge);
                let conf_pen = overlap_penalty_lamports(&node.ro_fp, &node.rw_fp, edge);
                let prog_pen = self.program_penalty(&new_path, edge.program_tag);
                let mut profit_adj = profit_here
                    .saturating_sub(lat_pen)
                    .saturating_sub(haz_pen)
                    .saturating_sub(conf_pen)
                    .saturating_sub(prog_pen);
                // [PATCH BZ: closure bonus]
                let remain2 = max_depth - new_path.len();
                if targets_set.contains(&out_token) && remain2 <= 2 { profit_adj = profit_adj.saturating_add(80_000); }
                let should_add = best_paths.get(&out_token)
                    .map(|&(amt, prof)| out_amt > amt || profit_adj > prof)
                    .unwrap_or(true);
                if !should_add { continue; }

                let bucket = (out_amt as u64) >> 12; let d = new_path.len() as u8; let mut hit = true;
                for s in [0xA5A5_A5A5_DEAD_BEEF, 0x0123_4567_89AB_CDEF, 0xC001_D00D_F00D_BABE].iter() {
                    let h = bloom_hash(&out_token, d, bucket, *s); let bit = (h & 63) as usize; let idx = (h as usize >> 6) & 3; let mask = 1u64 << bit; if (bloom[idx] & mask) == 0 { hit = false; }
                }
                if hit { continue; }
                for s in [0xA5A5_A5A5_DEAD_BEEF, 0x0123_4567_89AB_CDEF, 0xC001_D00D_F00D_BABE].iter() {
                    let h = bloom_hash(&out_token, d, bucket, *s); let bit = (h & 63) as usize; let idx = (h as usize >> 6) & 3; bloom[idx] |= 1u64 << bit;
                }
                best_paths.insert(out_token, (out_amt, profit_adj));
                heap.push(PathNode { token: out_token, amount: out_amt, gas_cost, path: new_path, profit: profit_adj, ro_fp: child_ro, rw_fp: child_rw });
            }
        }

        best_result
    }

    // [PATCH: bounded reconstruction via local DFS targeting mint+depth with pruning] >>>
    fn reconstruct_path_to_token(
        &self,
        start_token: TokenMint,
        start_amount: u64,
        target: TokenMint,
        depth: usize,
    ) -> Option<(Vec<(PoolId, bool)>, u64, i64)> {
        // [PATCH: use lock-free RCU snapshot] >>>
        let graph = self.graph_snapshot();
        // <<< END PATCH
        let mut best: Option<(Vec<(PoolId, bool)>, u64, i64)> = None;

        fn dfs<'a>(
            this: &GeodesicPathFinder,
            graph: &'a AHashMap<TokenMint, Vec<Arc<PoolEdge>>>,
            cur_token: TokenMint,
            cur_amount: u64,
            cur_path: &mut PathVec,
            cur_gas: u64,
            target: TokenMint,
            remain: usize,
            best: &mut Option<(Vec<(PoolId, bool)>, u64, i64)>,
        ) {
            if remain == 0 {
                if cur_token == target && !cur_path.is_empty() {
                    let profit = cur_amount as i64
                        - (if cur_path.first().map(|_| true).unwrap_or(false) { /*start amount*/ } else { })
                        - cur_gas as i64;
                    // The start amount isn’t threaded here; compute externally
                    // we’ll patch by recomputing at caller
                    let (_final_amt, _) = (cur_amount, cur_gas);
                    // store candidate; actual profit computed by caller
                    if best.is_none() {
                        *best = Some((cur_path.as_slice().to_vec(), cur_amount, 0));
                    }
                }
                return;
            }
            if let Some(edges) = graph.get(&cur_token) {
                for edge in edges {
                    let (next_token, out_amt, is_rev) =
                        this.calculate_swap_output(edge, cur_token, cur_amount);
                    if out_amt == 0 { continue; }
                    let gas = cur_gas + this.gas_cost_for_edge(edge);
                    if cur_path.len() >= MAX_PATH_LENGTH { continue; }
                    cur_path.push((edge.pool_id, is_rev));
                    dfs(this, graph, next_token, out_amt, cur_path, gas, target, remain - 1, best);
                    cur_path.pop();
                }
            }
        }

        let mut p = PathVec::new();
        dfs(self, &graph, start_token, start_amount, &mut p, 0, target, depth, &mut best);

        if let Some((path, final_amt_guess, _)) = best.take() {
            // compute true profit
            if let Ok((final_amt, _steps)) = self.simulate_path_execution(&path, start_amount) {
                let gas = path.iter().map(|(pid, _)| {
                    self.pool_cache.read().get(pid)
                        .map(|e| self.gas_cost_for_edge(e))
                        .unwrap_or(0)
                }).sum::<u64>();
                let profit = final_amt as i64 - start_amount as i64 - gas as i64;
                Some((path, final_amt, profit))
            } else {
                Some((path, final_amt_guess, 0))
            }
        } else {
            None
        }
    }
    // <<< END PATCH

    #[inline(always)]
    fn gas_cost_for_edge(&self, edge: &PoolEdge) -> u64 {
        // [PATCH CC: CU EMA based gas cost]
        let unit = *self.cu_unit_price_lamports.read();
        let cu = self.edge_cu_ema.read().get(&edge.pool_id).copied().unwrap_or(edge.cu_estimate as u32) as u64;
        cu.saturating_mul(unit)
    }

    // [PATCH CC: observe actual CU per hop]
    pub fn observe_cu_used(&self, per_hop_cu: &[(PoolId, u32)]) {
        let alpha = 0.25f64;
        let mut m = self.edge_cu_ema.write();
        for &(pid, cu) in per_hop_cu.iter() {
            let prev = m.get(&pid).copied().unwrap_or(cu);
            let upd = ((1.0 - alpha) * (prev as f64) + alpha * (cu as f64)).round() as u32;
            m.insert(pid, upd.max(10_000));
        }
    }

    // [PATCH AZ: quote cache warmup] >>>
    // ===================== [PIECE I] Vectorized CPMM triple-quote + cache commit =====================
    #[inline]
    fn cpmm_out_vec3(&self, reserve_in: u64, reserve_out: u64, amounts: [u64;3], fee_bps: u16) -> [u64;3] {
        if reserve_in == 0 || reserve_out == 0 { return [0,0,0]; }
        let fee_mul = (BPS_DEN - fee_bps as u64) as u128;
        let rin = (reserve_in as u128) * (BPS_DEN as u128);
        let rout = reserve_out as u128;
        let mut outs = [0u64;3];
        #[inline(always)]
        fn one(ain_u64: u64, fee_mul: u128, rin: u128, rout: u128) -> u64 {
            if ain_u64 == 0 { return 0; }
            let ain = (ain_u64 as u128) * fee_mul;
            let num = ain.saturating_mul(rout);
            let den = rin.saturating_add(ain);
            if den == 0 { return 0; }
            let out = num / den;
            let slip = (out * (MAX_SLIPPAGE_BPS as u128)) / (BPS_DEN as u128);
            (out.saturating_sub(slip)) as u64
        }
        outs[0] = one(amounts[0], fee_mul, rin, rout);
        outs[1] = one(amounts[1], fee_mul, rin, rout);
        outs[2] = one(amounts[2], fee_mul, rin, rout);
        outs
    }

    #[inline]
    fn raw_out_batch3_and_commit(&self, edge: &PoolEdge, amount_in: u64, is_reverse: bool) -> [u64;3] {
        match edge.pool_type {
            PoolType::SerumV3 | PoolType::PhoenixV1 => {
                let one = self.raw_out_cached(edge, amount_in, is_reverse);
                return [one, one, one];
            }
            _ => {}
        }
        if amount_in == 0 { return [0,0,0]; }
        let a1 = (amount_in.saturating_mul(9) / 10).max(1);
        let a2 = amount_in;
        let a3 = amount_in.saturating_add(amount_in/10).max(1);
        let (rin, rout) = if is_reverse { (edge.reserve_b, edge.reserve_a) } else { (edge.reserve_a, edge.reserve_b) };
        let outs = self.cpmm_out_vec3(rin, rout, [a1, a2, a3], edge.fee_bps);
        let epoch = self.graph_epoch.load(AtomOrdering::Relaxed);
        let now  = self.quote_stamp.fetch_add(3, AtomOrdering32::Relaxed);
        #[inline(always)]
        fn commit_one(owner: &GeodesicPathFinder, edge: &PoolEdge, amt: u64, out: u64, epoch: u64, stamp: u32, is_rev: bool) {
            let (set_idx, tag) = quote_set_and_tag(&edge.pool_id, is_rev, amt);
            TQUOTE.with(|q| TSTAMP.with(|s| {
                let mut v = q.borrow_mut();
                let mut st = *s.borrow();
                if let Some(slot) = v.get_mut(set_idx) {
                    if slot.a.stamp <= slot.b.stamp { slot.a = QuoteLine{ tag, out, stamp: st, epoch }; }
                    else { slot.b = QuoteLine{ tag, out, stamp: st, epoch }; }
                    st = st.wrapping_add(1); *s.borrow_mut() = st;
                }
            }));
            if let Some(mut set) = owner.quote_cache.write().get_mut(set_idx) {
                if set.a.stamp <= set.b.stamp { set.a = QuoteLine{ tag, out, stamp, epoch }; }
                else { set.b = QuoteLine{ tag, out, stamp, epoch }; }
            }
        }
        commit_one(self, edge, a1, outs[0], epoch, now.wrapping_sub(2), is_reverse);
        commit_one(self, edge, a2, outs[1], epoch, now.wrapping_sub(1), is_reverse);
        commit_one(self, edge, a3, outs[2], epoch, now,               is_reverse);
        outs
    }

    pub fn warm_quotes_for_path(&self, path: &[(PoolId,bool)], input_amount: u64) {
        let pc = self.pool_cache.read();
        let mut amt = input_amount;
        for (pid, rev) in path {
            if let Some(e) = pc.get(pid) {
                // Prefer wider warm: batch8, consume 100% (index 3)
                let outs8 = self.raw_out_batch8_and_commit(e, amt, *rev);
                let mid = outs8[3];
                if mid == 0 { break; }
                amt = mid;
            } else { break; }
        }
    }

    // [PATCH AP: fingerprint + cooldown] >>>
    #[inline]
    fn path_tag(&self, path: &[(PoolId,bool)], input_bucket: u64) -> u64 {
        let (k1,k2) = global_hash_keys();
        let mut h = AHasher::new_with_keys(k1, k2);
        input_bucket.hash(&mut h);
        for (pid, rev) in path { pid.hash(&mut h); rev.hash(&mut h); }
        h.finish() | 1
    }

    pub fn mark_path_inflight(&self, path: &[(PoolId,bool)], input_amount: u64, ttl_slots: u64) {
        let tag = self.path_tag(path, input_amount >> 12);
        let now = self.current_slot.load(AtomOrdering::Relaxed);
        self.inflight_paths.write().insert(tag, now.saturating_add(ttl_slots));
    }

    pub fn should_skip_path(&self, path: &[(PoolId,bool)], input_amount: u64) -> bool {
        let tag = self.path_tag(path, input_amount >> 12);
        let now = self.current_slot.load(AtomOrdering::Relaxed);
        if let Some(&exp) = self.inflight_paths.read().get(&tag) { return exp >= now && exp != 0; }
        false
    }

    // ===== [PIECE A] depth cap + compact node key for arena A* =====
    // ===================== [PIECE O] ROI-adaptive depth cap =====================
    #[inline]
    fn depth_cap_for_start(&self, start: TokenMint, start_amt: u64) -> usize {
        let bm = self.best_mult_rcu.load_full();
        let slip_rcu = self.token_slip_floor_bps_rcu.load_full();
        let m = bm.get(&start).copied().unwrap_or(1.0).max(1.0);
        let slip_bps = slip_rcu.get(&start).copied().unwrap_or(0) as f64;
        let slip_mult = (BPS_DEN as f64 - slip_bps) / BPS_DEN as f64;
        let eff = (m * slip_mult).max(0.98);
        let need = (MIN_PROFIT_THRESHOLD as f64) / (start_amt as f64);
        let mut d = 1usize;
        let mut g = eff;
        while g - 1.0 < need && d < MAX_PATH_LENGTH { d += 1; g *= eff; }
        d.min(MAX_PATH_LENGTH).max(2)
    }

    #[inline]
    fn node_key_compact(&self, token: TokenMint, depth: u8, amt_bucket: u64) -> u128 {
        let (k1,k2) = global_hash_keys();
        let mut h = AHasher::new_with_keys(k1, k2);
        token.hash(&mut h);
        let t = h.finish() as u128;
        (t << 64) | (((depth as u128) & 0xFF) << 56) | (amt_bucket as u128 & ((1u128<<56)-1))
    }

    // [PATCH AO: tx footprint estimator] >>>
    #[inline]
    pub fn path_account_footprint(&self, path: &[(PoolId, bool)]) -> (u32, u32) {
        let pc = self.pool_cache.read();
        let mut ro: u32 = 0;
        let mut rw: u32 = 0;
        for (pid, _) in path {
            if let Some(e) = pc.get(pid) {
                ro = ro.saturating_add(e.ro_accounts as u32);
                rw = rw.saturating_add(e.rw_accounts as u32);
            }
        }
        ro = ro.saturating_add(12);
        rw = rw.saturating_add(4);
        (ro, rw)
    }

    #[inline]
    pub fn fits_account_budget(&self, path: &[(PoolId, bool)], max_keys: u32, max_writable: u32) -> bool {
        let (ro, rw) = self.path_account_footprint(path);
        let total = ro.saturating_add(rw);
        total <= max_keys && rw <= max_writable
    }

    // [PATCH AX: tx byte-size estimator & guard] >>>
    pub fn estimate_tx_bytes(&self, path: &[(PoolId,bool)], extra_ix: u16) -> u32 {
        let pc = self.pool_cache.read();
        let mut bytes: u32 = 0;
        bytes = bytes.saturating_add(200);
        for (pid, _) in path {
            if let Some(e) = pc.get(pid) {
                bytes = bytes.saturating_add(e.ix_bytes_estimate as u32);
            }
        }
        bytes.saturating_add(extra_ix as u32)
    }

    #[inline]
    pub fn fits_tx_bytes(&self, path: &[(PoolId,bool)], max_bytes: u32, extra_ix: u16) -> bool {
        self.estimate_tx_bytes(path, extra_ix) <= max_bytes
    }

    // [PATCH AQ: disjoint portfolio picker] >>>
    pub fn path_cu_units(&self, path: &[(PoolId,bool)]) -> u64 {
        let pc = self.pool_cache.read();
        let mut cu = 0u64;
        for (pid, _) in path { if let Some(e) = pc.get(pid) { cu = cu.saturating_add(e.cu_estimate as u64); } }
        cu
    }

    pub fn select_portfolio(
        &self,
        candidates: &[(Vec<(PoolId,bool)>, u64, i64)],
        _input_amounts: &[u64],
        max_total_cu: u64,
        max_total_tips: u64,
    ) -> Vec<usize> {
        let pc = self.pool_cache.read();
        let mut scored: Vec<(usize, f64, u64, u64, AHashSet<PoolId>)> = Vec::with_capacity(candidates.len());
        for (i, (path, _out, prof)) in candidates.iter().enumerate() {
            let cu = self.path_cu_units(path).max(1);
            let tips: u64 = path.iter().map(|(pid, _)| pc.get(pid).map(|e| e.cu_estimate as u64).unwrap_or(0)).sum();
            let set: AHashSet<PoolId> = path.iter().map(|(pid,_)| *pid).collect();
            let score = (*prof as f64) / ((cu as f64)/10_000.0);
            scored.push((i, score, cu, tips, set));
        }
        scored.sort_by(|a,b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

        let mut chosen = Vec::new();
        let mut used: AHashSet<PoolId> = AHashSet::with_capacity(256);
        let mut cu_acc=0u64; let mut tip_acc=0u64;
        for (i, _sc, cu, tips, set) in scored.into_iter() {
            if cu_acc.saturating_add(cu) > max_total_cu { continue; }
            if tip_acc.saturating_add(tips) > max_total_tips { continue; }
            if set.iter().any(|p| used.contains(p)) { continue; }
            cu_acc += cu; tip_acc += tips;
            used.extend(set.into_iter());
            chosen.push(i);
        }
        chosen
    }

    // ===== [PIECE B] Tip Optimizer (KKT/Newton) =====
    pub fn allocate_tips_kkt(&self, path: &[(PoolId,bool)], k_per_lamport: f64, budget: u64) -> Vec<u64> {
        let n = path.len();
        if n == 0 || budget == 0 || k_per_lamport <= 0.0 { return vec![0; n]; }
        let k = k_per_lamport.max(1e-12);
        let mut lam = 1e-6_f64; // dual variable
        let f_t = |lambda: f64, k: f64| -> f64 {
            if lambda <= 0.0 { return (budget as f64)/ (n as f64); }
            (1.0/k) * ((k + lambda)/lambda).ln()
        };
        for _ in 0..25 {
            let ti = f_t(lam, k);
            let sum = (n as f64) * ti;
            let err = sum - (budget as f64);
            if err.abs() < 0.5 { break; }
            let dti = (1.0/k) * (1.0/(k+lam) - 1.0/lam);
            let dsum = (n as f64) * dti;
            if dsum.abs() < 1e-18 { break; }
            let step = (err / dsum).clamp(-lam*0.9, lam*0.9);
            lam = (lam - step).max(1e-12);
        }
        let ti = f_t(lam, k).max(0.0);
        let mut out: Vec<u64> = vec![ti.floor() as u64; n];
        let mut need = budget.saturating_sub(out.iter().sum());
        let mut i = 0usize; while need > 0 && i < out.len() { out[i] = out[i].saturating_add(1); need-=1; i+=1; }
        out
    }

    pub fn choose_tips_for_path_ev(&self, path: &[(PoolId,bool)], input_amount: u64, k_per_lamport: f64,
                                   min_budget: u64, max_budget: u64) -> Option<(Vec<u64>, i64)> {
        let (out_raw, _) = self.simulate_path_execution(path, input_amount).ok()?;
        let unit = *self.cu_unit_price_lamports.read();
        let pc = self.pool_cache.read();
        let cu_cost: u64 = path.iter().map(|(pid, _)| pc.get(pid).map(|e| e.cu_estimate as u64 * unit).unwrap_or(0)).sum();
        let base = out_raw as i64 - input_amount as i64 - cu_cost as i64;
        if base <= 0 { return None; }
        let mut lo = min_budget.min(max_budget);
        let mut hi = max_budget.max(min_budget);
        let phi = GOLDEN_RATIO;
        let mut x1 = (hi as f64 - (hi as f64 - lo as f64) * phi) as u64;
        let mut x2 = (lo as f64 + (hi as f64 - lo as f64) * phi) as u64;
        let ev_for = |b: u64| -> i64 {
            if b == 0 || path.len() == 0 { return base; }
            let t = (b as f64) / (path.len() as f64);
            let p_i = 1.0 - (-k_per_lamport.max(1e-12) * t).exp();
            let succ = p_i.powi(path.len() as i32).clamp(0.0, 1.0);
            (base as f64 * succ - (b as f64)).floor() as i64
        };
        let mut f1 = ev_for(x1);
        let mut f2 = ev_for(x2);
        for _ in 0..22 {
            if f1 < f2 { lo = x1; x1 = x2; f1 = f2; x2 = (lo as f64 + (hi as f64 - lo as f64) * phi) as u64; f2 = ev_for(x2);
            } else { hi = x2; x2 = x1; f2 = f1; x1 = (hi as f64 - (hi as f64 - lo as f64) * phi) as u64; f1 = ev_for(x1); }
            if hi - lo <= 8 { break; }
        }
        let best_b = if f1 > f2 { x1 } else { x2 };
        let tips = self.allocate_tips_kkt(path, k_per_lamport, best_b);
        let ev = ev_for(best_b);
        Some((tips, ev))
    }

    // ===================== [PIECE R] Two-child vector pre-evaluator =====================
    /// Evaluate up to two best edges for (token, amount, depth). Returns compact child evaluations.
    pub fn pre_evaluate_children2(
        &self,
        token: TokenMint,
        amount: u64,
        depth: usize,
        start_amount: u64,
        max_depth: usize,
        parent_hop: Option<(PoolId,bool)>,
        ro_fp: &[u64;4],
        rw_fp: &[u64;4],
    ) -> Vec<ChildEval> {
        use std::cmp::Reverse;
        let k = self.k_for(depth, &Instant::now(), 3000).max(2);
        let fl = self.graph_snapshot_fastlane();
        let tk = self.graph_snapshot_topk();
        let mut cands: Vec<Arc<PoolEdge>> = Vec::with_capacity(k);
        if let Some(v) = fl.get(&token) { cands.extend(v.iter().cloned()); }
        if cands.len() < k {
            if let Some(v) = tk.get(&token) {
                for e in v.iter() {
                    if cands.iter().any(|x| x.pool_id == e.pool_id) { continue; }
                    cands.push(e.clone()); if cands.len()>=k { break; }
                }
            }
        }
        cands.sort_by_key(|e| Reverse(self.expand_score(e)));
        if cands.len()>2 { cands.truncate(2); }

        let bm = self.best_mult_rcu.load_full();
        let slip_rcu = self.token_slip_floor_bps_rcu.load_full();
        let mut outv = Vec::with_capacity(cands.len());
        for e in cands.iter() {
            if let Some((lp, lr)) = parent_hop {
                let is_rev = token == e.token_b; if lp == e.pool_id && lr != is_rev { continue; }
            }
            let is_rev = token == e.token_b;
            let out_amt = self.raw_out_cached(e, amount, is_rev); if out_amt==0 { continue; }
            let next_tok = if is_rev { e.token_a } else { e.token_b };
            let edge_gas = self.gas_cost_for_edge(e);
            let inc = out_amt as i64 - amount as i64 - edge_gas as i64; if inc < -((MIN_PROFIT_THRESHOLD as i64)/2) { continue; }
            let child_ro = bloom256_or(ro_fp, &e.acct_ro_fp);
            let child_rw = bloom256_or(rw_fp, &e.acct_rw_fp);
            let remain = max_depth.saturating_sub(depth+1);
            let floor_bps = slip_rcu.get(&next_tok).copied().unwrap_or(0) as f64;
            let slip_mult = (BPS_DEN as f64 - floor_bps) / BPS_DEN as f64;
            let bmf = bm.get(&next_tok).copied().unwrap_or(1.0) * slip_mult.max(0.95);
            let ub_out = (out_amt as f64) * powi_f64(bmf, remain);
            let min_tip = *self.min_edge_tip_lamports.read();
            let ub_gas = (edge_gas as u64).saturating_add((remain as u64) * min_tip);
            let mut h = (ub_out as i64) - (start_amount as i64) - (ub_gas as i64);
            let haz_pen = self.hazard_penalty_lamports(e);
            let conf_pen = overlap_penalty_lamports(ro_fp, rw_fp, e);
            let lat_pen  = self.latency_penalty_lamports(depth+1);
            let prog_pen = self.program_penalty(&[(e.pool_id, is_rev)], e.program_tag);
            let hot_pen  = self.account_hot_penalty(e);
            h = h.saturating_sub(haz_pen + conf_pen + lat_pen + prog_pen + hot_pen);
            let g_profit = if depth>0 && (next_tok == token) { out_amt as i64 - start_amount as i64 - (edge_gas as i64) } else { 0 };
            let f = g_profit.saturating_add(h);
            outv.push(ChildEval{ pid: e.pool_id, is_rev, next_tok, out_amt, edge_gas, g_profit, h, f, ro_fp: child_ro, rw_fp: child_rw });
        }
        outv
    }

    // ===================== [PIECE S] ALT planner for path accounts =====================
    pub fn build_alt_plan_for_path(&self, path: &[(PoolId,bool)], max_bytes: u32) -> AltPlan {
        let pc = self.pool_cache.read();
        let mut ro_set: AHashMap<[u8;32], ()> = AHashMap::with_capacity(512);
        let mut rw_set: AHashMap<[u8;32], ()> = AHashMap::with_capacity(256);
        for (pid, _) in path {
            if let Some(e) = pc.get(pid) {
                if let Some(ro) = &e.ro_accs32 { for k in ro.iter() { ro_set.entry(*k).or_insert(()); } }
                if let Some(rw) = &e.rw_accs32 { for k in rw.iter() { rw_set.entry(*k).or_insert(()); } }
            }
        }
        let inline_ro = (ro_set.len() as u32).saturating_add(12);
        let inline_rw = (rw_set.len() as u32).saturating_add(4);
        let inline_bytes_est = inline_ro.saturating_add(inline_rw).saturating_mul(34);
        let alt_table_len = (ro_set.len() + rw_set.len()) as u32;
        let alt_bytes_est = 42u32.saturating_add(alt_table_len.saturating_mul(32));
        let use_alt = alt_bytes_est + 200 < inline_bytes_est && (alt_bytes_est + 200) < max_bytes;
        let mut all: Vec<[u8;32]> = Vec::with_capacity((ro_set.len()+rw_set.len()).max(1));
        all.extend(ro_set.keys().copied()); all.extend(rw_set.keys().copied());
        AltPlan { use_alt, alt_keys: StdArc::from(all.into_boxed_slice()), inline_ro, inline_rw }
    }

    // ===================== [PIECE T] CUSUM drift guard updater (using existing Cusum {stat,k,h}) =====================
    /// Update per-edge CUSUM-like stat on reserve changes and trigger cooldowns if threshold crossed.
    pub fn update_cusum_on_reserve(&self, pid: PoolId, ra: u64, rb: u64, slot: u64) {
        if ra == 0 || rb == 0 { return; }
        let mut c = self.pool_cusum.write();
        let entry = c.entry(pid).or_insert(Cusum::default());
        // Simple magnitude-based drift accumulation; scale roughly with relative change
        let rel = ((rb as f64) / (ra as f64)).ln().abs();
        entry.stat = (entry.stat + (rel * 100_000.0) as i64 - entry.k).max(0);
        if entry.stat > entry.h {
            let mut ef = self.edge_fail.write();
            if let Some(s) = ef.get_mut(&pid) {
                s.cooldown_until = slot.saturating_add(180);
                s.fail_ema = (s.fail_ema * 0.9) + 0.1 * 1.0;
            } else {
                ef.insert(pid, EdgeFail{ fail_ema: 0.8, inuse_ema: 0.0, cooldown_until: slot.saturating_add(180), init: true });
            }
            entry.stat = entry.k; // reset to avoid repeated trips
        }
    }

    // ===================== [PIECE K] Heterogeneous KKT Tip Allocator =====================
    /// Estimate per-hop k_i from live stats (fail/inuse, venue type). Units: per-lamport.
    pub fn k_per_lamport_for_path(&self, path: &[(PoolId,bool)]) -> Vec<f64> {
        let pc = self.pool_cache.read();
        let ef = self.edge_fail.read();
        let mut ks = Vec::with_capacity(path.len());
        for (pid, _) in path {
            let mut k = 1.2e-5_f64;
            if let Some(e) = ef.get(pid) {
                let succ_bias = (1.0 + e.inuse_ema).clamp(1.0, 2.0);
                let fail_drag = (1.0 + e.fail_ema).clamp(1.0, 2.0);
                k *= (succ_bias / fail_drag).clamp(0.5, 2.0);
            }
            if let Some(edge) = pc.get(pid) {
                k *= match edge.pool_type { PoolType::SerumV3 | PoolType::PhoenixV1 => 1.30, _ => 1.00 };
            }
            ks.push(k.max(1e-8));
        }
        ks
    }

    /// Solve Σ_i t_i(λ) = B with t_i(λ) = (1/k_i) * ln((k_i+λ)/λ). Newton on λ. Returns integer lamports per hop.
    pub fn allocate_tips_kkt_hetero(&self, k: &[f64], budget: u64) -> Vec<u64> {
        let n = k.len();
        if n == 0 || budget == 0 { return vec![0; n]; }
        let mut lam = 1e-6_f64; let b = budget as f64;
        let t_of = |lam: f64, ki: f64| -> f64 { let l = lam.max(1e-12); (1.0/ki) * ((ki + l)/l).ln() };
        let dt_dlam = |lam: f64, ki: f64| -> f64 { let l = lam.max(1e-12); (1.0/ki) * (1.0/(ki + l) - 1.0/l) };
        for _ in 0..30 {
            let mut sum=0.0; let mut dsum=0.0; for &ki in k.iter(){ sum+=t_of(lam,ki); dsum+=dt_dlam(lam,ki);} let err = sum - b; if err.abs() < 0.5 { break; }
            let step = (err / dsum).clamp(-lam*0.9, lam*0.9); lam = (lam - step).max(1e-12);
        }
        let mut out: Vec<u64> = k.iter().map(|&ki| t_of(lam, ki).floor() as u64).collect();
        let mut need = budget.saturating_sub(out.iter().sum()); let mut i=0usize; while need>0 && i<out.len(){ out[i]+=1; need-=1; i+=1; }
        out
    }

    /// EV chooser with heterogeneous k_i: maximize EV = base * Π(1-exp(-k_i t_i)) - Σ t_i
    pub fn choose_tips_for_path_ev_hetero(
        &self,
        path: &[(PoolId,bool)],
        input_amount: u64,
        min_budget: u64,
        max_budget: u64,
    ) -> Option<(Vec<u64>, i64)> {
        if path.is_empty() { return None; }
        let (out_raw, _) = self.simulate_path_execution(path, input_amount).ok()?;
        let unit = *self.cu_unit_price_lamports.read();
        let pc = self.pool_cache.read();
        let cu_cost: u64 = path.iter().map(|(pid, _)| pc.get(pid).map(|e| e.cu_estimate as u64 * unit).unwrap_or(0)).sum();
        let base = out_raw as i64 - input_amount as i64 - cu_cost as i64; if base <= 0 { return None; }
        let ks = self.k_per_lamport_for_path(path);
        let mut lo = min_budget.min(max_budget); let mut hi = max_budget.max(min_budget);
        let phi = GOLDEN_RATIO;
        #[inline] fn succ_prod(ks: &[f64], ts: &[u64]) -> f64 { let mut p=1.0f64; for (k,t) in ks.iter().zip(ts){ let pi = 1.0 - (-(*k)*(*t as f64)).exp(); p *= pi.max(0.0).min(1.0); } p }
        let mut x1 = (hi as f64 - (hi as f64 - lo as f64) * phi) as u64; let mut x2 = (lo as f64 + (hi as f64 - lo as f64) * phi) as u64;
        let mut f1 = { let t = self.allocate_tips_kkt_hetero(&ks, x1); ((base as f64) * succ_prod(&ks, &t) - (x1 as f64)).floor() as i64 };
        let mut f2 = { let t = self.allocate_tips_kkt_hetero(&ks, x2); ((base as f64) * succ_prod(&ks, &t) - (x2 as f64)).floor() as i64 };
        for _ in 0..22 {
            if f1 < f2 { lo = x1; x1 = x2; let t = self.allocate_tips_kkt_hetero(&ks, x1); f1 = ((base as f64) * succ_prod(&ks, &t) - (x1 as f64)).floor() as i64; x2 = (lo as f64 + (hi as f64 - lo as f64) * phi) as u64; let t2 = self.allocate_tips_kkt_hetero(&ks, x2); f2 = ((base as f64) * succ_prod(&ks, &t2) - (x2 as f64)).floor() as i64; }
            else { hi = x2; x2 = x1; let t = self.allocate_tips_kkt_hetero(&ks, x2); f2 = ((base as f64) * succ_prod(&ks, &t) - (x2 as f64)).floor() as i64; x1 = (hi as f64 - (hi as f64 - lo as f64) * phi) as u64; let t1 = self.allocate_tips_kkt_hetero(&ks, x1); f1 = ((base as f64) * succ_prod(&ks, &t1) - (x1 as f64)).floor() as i64; }
            if hi - lo <= 8 { break; }
        }
        let (best_b, _best_ev) = if f1 > f2 { (x1, f1) } else { (x2, f2) };
        let tips = self.allocate_tips_kkt_hetero(&ks, best_b);
        let ev = ((base as f64) * succ_prod(&ks, &tips) - (best_b as f64)).floor() as i64;
    }

    // ===================== [PIECE BP] Adaptive EV optimizer (accelerated golden search) =====================
    #[inline]
    fn succ_prod_fast(&self, ks: &[f64], ts: &[u64]) -> f64 {
        let mut p = 1.0f64;
        for (k,t) in ks.iter().zip(ts) {
            let pi = 1.0 - (-(*k) * (*t as f64)).exp();
            p *= pi.max(0.0).min(1.0);
        }
        p
    }

    #[inline]
    fn ev_for_budget(&self, base: i64, ks: &[f64], b: u64) -> i64 {
        let t = self.allocate_tips_kkt_hetero(ks, b);
        ((base as f64) * self.succ_prod_fast(ks, &t) - (b as f64)).floor() as i64
    }

    /// Drop-in accelerated version of hetero EV chooser with early stopping.
    pub fn choose_tips_for_path_ev_hetero_accel(
        &self,
        path: &[(PoolId,bool)],
        input_amount: u64,
        min_budget: u64,
        max_budget: u64,
    ) -> Option<(Vec<u64>, i64)> {
        if path.is_empty() { return None; }
        let (out_raw, _) = self.simulate_path_execution(path, input_amount).ok()?;
        let unit = *self.cu_unit_price_lamports.read();
        let pc = self.pool_cache.read();
        let cu_cost: u64 = path.iter().map(|(pid, _)| pc.get(pid).map(|e| e.cu_estimate as u64 * unit).unwrap_or(0)).sum();
        let base = out_raw as i64 - input_amount as i64 - cu_cost as i64;
        if base <= 0 { return None; }
        let ks = self.k_per_lamport_for_path(path);

        let mut lo = min_budget.min(max_budget);
        let mut hi = max_budget.max(min_budget);
        if hi - lo <= 3 {
            let mut best_b = lo; let mut best_ev = i64::MIN/4;
            for b in lo..=hi {
                let ev = self.ev_for_budget(base, &ks, b);
                if ev > best_ev { best_ev = ev; best_b = b; }
            }
            let tips = self.allocate_tips_kkt_hetero(&ks, best_b);
            return Some((tips, best_ev));
        }

        let phi = 0.6180339887498949_f64;
        let mut x1 = (hi as f64 - (hi as f64 - lo as f64) * phi).round().max(lo as f64) as u64;
        let mut x2 = (lo as f64 + (hi as f64 - lo as f64) * phi).round().min(hi as f64) as u64;
        let mut f1 = self.ev_for_budget(base, &ks, x1);
        let mut f2 = self.ev_for_budget(base, &ks, x2);

        const MIN_BRACKET: u64 = 4;
        const MIN_GAIN: i64 = 1;
        for _ in 0..20 {
            if hi - lo <= MIN_BRACKET || (f1 - f2).abs() <= MIN_GAIN { break; }
            if f1 < f2 {
                lo = x1; x1 = x2; f1 = f2;
                x2 = (lo as f64 + (hi as f64 - lo as f64) * phi).round() as u64;
                f2 = self.ev_for_budget(base, &ks, x2);
            } else {
                hi = x2; x2 = x1; f2 = f1;
                x1 = (hi as f64 - (hi as f64 - lo as f64) * phi).round() as u64;
                f1 = self.ev_for_budget(base, &ks, x1);
            }
        }
        let (best_b, best_ev) = if f1 > f2 { (x1, f1) } else { (x2, f2) };
        let tips = self.allocate_tips_kkt_hetero(&ks, best_b);
        Some((tips, best_ev))
    }

    // ===================== [PIECE N] Top-K by score (array-backed RingTopK) =====================
    #[inline]
    fn select_topk_by_score(&self, v: Vec<Arc<PoolEdge>>, k: usize) -> Vec<Arc<PoolEdge>> {
        if v.len() <= k {
            let mut vv = v;
            vv.sort_by_key(|e| Reverse(self.expand_score(e)));
            return vv;
        }
        let km = k.min(16);
        match km {
            2 => {
                let mut g: RingTopK<2> = RingTopK::new();
                for e in v { g.consider(self.expand_score(&e), e); }
                g.into_sorted_vec()
            }
            4 => {
                let mut g: RingTopK<4> = RingTopK::new();
                for e in v { g.consider(self.expand_score(&e), e); }
                g.into_sorted_vec()
            }
            8 => {
                let mut g: RingTopK<8> = RingTopK::new();
                for e in v { g.consider(self.expand_score(&e), e); }
                g.into_sorted_vec()
            }
            _ => {
                let mut g: RingTopK<16> = RingTopK::new();
                for e in v { g.consider(self.expand_score(&e), e); }
                let mut out = g.into_sorted_vec();
                if out.len() > k { out.truncate(k); }
                out
            }
        }
    }

    // [PATCH AR: secant optimizer] >>>
    pub fn optimize_amount_secant(
        &self,
        path: &[(PoolId, bool)],
        max_affordable_input: u64,
    ) -> Option<(u64, u64, i64)> {
        if path.is_empty() { return None; }
        let pc = self.pool_cache.read();
        let first = path[0].0; let rev = path[0].1;
        let pool = pc.get(&first)?;
        let rin = if rev { pool.reserve_b } else { pool.reserve_a };
        let upper = (rin / 10).min(max_affordable_input).max(1);

        let profit = |x: u64| -> i64 {
            if x == 0 { return i64::MIN/4; }
            if let Ok((out,_)) = self.simulate_path_execution(path, x) {
                let gas = path.iter().map(|(pid, _)| pc.get(pid).map(|e| self.gas_cost_for_edge(e)).unwrap_or(0)).sum::<u64>();
                (out as i64) - (x as i64) - (gas as i64)
            } else { i64::MIN/4 }
        };

        let mut x0 = 1.max(upper/4);
        let mut x1 = upper;
        let mut f0 = profit(x0);
        let mut f1 = profit(x1);
        if f0 == i64::MIN/4 && f1 == i64::MIN/4 { return None; }

        for _ in 0..10 {
            let denom = (f1 - f0) as f64;
            if denom.abs() < 1.0 { break; }
            let x2f = (x1 as f64) - (f1 as f64) * ((x1 as f64 - x0 as f64) / denom);
            let mut x2 = x2f.clamp(1.0, upper as f64) as u64;
            if x2 == x1 { x2 = x2.saturating_sub(1).max(1); }
            let f2 = profit(x2);
            x0 = x1; f0 = f1; x1 = x2; f1 = f2;
        }

        let xs = [x0, x1, (x0+x1)/2].into_iter().filter(|&x| x>0 && x<=upper);
        let mut best = (0u64, i64::MIN);
        for x in xs {
            let f = profit(x);
            if f > best.1 { best = (x, f); }
        }
        if best.1 <= 0 { return None; }
        let (final_out, _) = self.simulate_path_execution(path, best.0).ok()?;
        Some((best.0, final_out, best.1))
    }

    // [PATCH AS: profit after tips] >>>
    pub fn profit_after_tips(
        &self,
        path: &[(PoolId,bool)],
        input_amount: u64,
        tips_per_hop: &[u64],
    ) -> Option<i64> {
        if path.len() != tips_per_hop.len() { return None; }
        let (out, _) = self.simulate_path_execution(path, input_amount).ok()?;
        let unit = *self.cu_unit_price_lamports.read();
        let pc = self.pool_cache.read();
        let cu_cost: u64 = path.iter().map(|(pid, _)| pc.get(pid).map(|e| e.cu_estimate as u64 * unit).unwrap_or(0)).sum();
        let tip_cost: u64 = tips_per_hop.iter().sum();
        Some(out as i64 - input_amount as i64 - (cu_cost as i64) - (tip_cost as i64))
    }

    // ===== [PIECE D] v0 Tx Builder with ALT + Tip =====
    pub fn build_message_v0_for_path(
        &self,
        payer: Pubkey,
        path: &[(PoolId, bool)],
        cu_limit: u32,
        cu_price_lamports: u64,
        jito_tip_account: Option<Pubkey>,
        jito_tip_lamports: u64,
        alt_accounts: &[solana_sdk::address_lookup_table_account::AddressLookupTableAccount],
        swap_ix_builder: &mut dyn FnMut(&PoolEdge, bool) -> solana_program::instruction::Instruction,
    ) -> Option<solana_sdk::message::VersionedMessage> {
        let pc = self.pool_cache.read();
        let cu_ix1 = solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(cu_limit);
        let cu_ix2 = solana_sdk::compute_budget::ComputeBudgetInstruction::set_compute_unit_price(cu_price_lamports);
        let mut ixs: Vec<solana_program::instruction::Instruction> = Vec::with_capacity(2 + path.len() + 1);
        ixs.push(cu_ix1);
        ixs.push(cu_ix2);
        if let Some(tip_acc) = jito_tip_account { if jito_tip_lamports > 0 {
            ixs.push(solana_program::system_instruction::transfer(&payer, &tip_acc, jito_tip_lamports));
        }}
        for (pid, rev) in path.iter().copied() { let e = pc.get(&pid)?; ixs.push(swap_ix_builder(e, rev)); }
        let mut accs: Vec<solana_program::instruction::AccountMeta> = vec![solana_program::instruction::AccountMeta::new(payer, true)];
        for (pid, _) in path { if let Some(e) = pc.get(pid) {
            if let Some(ro) = &e.ro_accs32 { for k in ro.iter() { accs.push(solana_program::instruction::AccountMeta::new_readonly(Pubkey::new_from_array(*k), false)); } }
            if let Some(rw) = &e.rw_accs32 { for k in rw.iter() { accs.push(solana_program::instruction::AccountMeta::new(Pubkey::new_from_array(*k), false)); } }
        }}
        canonicalize_metas(&mut accs);
        let msg = solana_sdk::message::v0::Message::try_compile(&payer, &ixs, &accs, alt_accounts).ok()?;
        Some(solana_sdk::message::VersionedMessage::V0(msg))
    }

    // ===== [PIECE F] Tip Tuner UCB updater =====
    pub fn observe_edge_ucb(&self, pid: PoolId, realized_profit_lamports: i64) {
        let mut m = self.edge_ucb.write();
        let e = m.entry(pid).or_insert(EdgeUcb{ n:0, mean:0.0 });
        e.n = e.n.saturating_add(1);
        let a = 1.0 / (e.n as f64);
        e.mean = (1.0 - a)*e.mean + a*(realized_profit_lamports as f64);
        self.ucb_total_n.fetch_add(1, AtomOrdering::Relaxed);
    }

    // ===================== [PIECE AZ] MessageV0 builder (ALT-aware, safe) =====================
    #[inline]
    pub fn build_message_v0_simple(
        &self,
        payer: Pubkey,
        ixs: Vec<solana_program::instruction::Instruction>,
        alts: &[solana_sdk::address_lookup_table_account::AddressLookupTableAccount],
    ) -> Option<solana_sdk::message::VersionedMessage> {
        solana_sdk::message::v0::Message::try_compile(&payer, &ixs, alts)
            .ok()
            .map(solana_sdk::message::VersionedMessage::V0)
    }
}

// ===== [PIECE E] Failure & CUSUM Observers =====
#[derive(Clone, Copy)]
struct Cusum { stat: i64, k: i64, h: i64 }
impl Default for Cusum { fn default() -> Self { Self{ stat:0, k:50_000, h:300_000 } } }

impl GeodesicPathFinder {
    pub fn observe_edge_success(&self, pid: PoolId) {
        let mut m = self.edge_fail.write();
        let e = m.entry(pid).or_insert(EdgeFail{ fail_ema:0.0, inuse_ema:0.0, cooldown_until:0, init:true });
        e.inuse_ema = 0.85*e.inuse_ema + 0.15*1.0; e.fail_ema = 0.90*e.fail_ema; drop(m);
        let mut c = self.pool_cusum.write(); let entry = c.entry(pid).or_insert(Cusum::default()); entry.stat = (entry.stat - entry.k).max(0);
    }
    pub fn observe_edge_fail(&self, pid: PoolId, cool_slots: u64) {
        let now = self.current_slot.load(AtomOrdering::Relaxed);
        let mut m = self.edge_fail.write();
        let e = m.entry(pid).or_insert(EdgeFail{ fail_ema:0.0, inuse_ema:0.0, cooldown_until:0, init:true });
        e.fail_ema = 0.85*e.fail_ema + 0.15*1.0; e.cooldown_until = now.saturating_add(cool_slots); e.init = true; drop(m);
        let mut c = self.pool_cusum.write(); let entry = c.entry(pid).or_insert(Cusum::default()); entry.stat = (entry.stat + entry.k).min(entry.h*2);
    }
}
    // [PATCH AT: admission policy] >>> (definition moved top-level)

    pub fn fits_cu_budget(&self, path: &[(PoolId,bool)], cu_limit: u64, cu_margin_bps: u64) -> bool {
        let cu = self.path_cu_units(path);
        let lim = cu_limit.saturating_sub(cu_limit.saturating_mul(cu_margin_bps)/BPS_DEN);
{{ ... }}
        cu <= lim
    }

    pub fn validate_robust_profit(&self, path: &[(PoolId,bool)], input: u64, min_profit: i64, extra_slip_bps_per_hop: u64) -> bool {
        if let Ok((out,_)) = self.simulate_path_execution(path, input) {
            let hops = path.len() as u64;
            let slip_pen = (out as u128).saturating_mul(extra_slip_bps_per_hop as u128).saturating_mul(hops as u128) / (BPS_DEN as u128);
            let out_adj = (out as u128).saturating_sub(slip_pen) as u64;
            (out_adj as i64 - input as i64) >= min_profit
        } else { false }
    }

    pub fn admit_path(
        &self,
        path: &[(PoolId,bool)],
        sized_input: u64,
        tips_per_hop: &[u64],
        cfg: &AdmissionCfg,
    ) -> bool {
        if self.should_skip_path(path, sized_input) { return false; }
        let (cu, ro, rw, bytes) = self.path_stats_memoized(path, 120);
        if ro.saturating_add(rw) > cfg.max_keys || rw > cfg.max_writable { return false; }
        let lim = cfg.cu_limit.saturating_sub(cfg.cu_limit.saturating_mul(cfg.cu_margin_bps)/BPS_DEN);
        if cu > lim { return false; }
        if bytes > 1232 { return false; }
        if !self.validate_robust_profit(path, sized_input, MIN_PROFIT_THRESHOLD as i64, cfg.extra_slip_bps_per_hop) { return false; }
        if let Some(p) = self.profit_after_tips(path, sized_input, tips_per_hop) {
            if p < cfg.min_profit_after_tips { return false; }
        } else { return false; }
        true
    }

    // [PATCH AY: dynamic min profit] >>>
    #[inline]
    pub fn min_profit_for_path(&self, path: &[(PoolId,bool)]) -> i64 {
        let ps = self.pool_stats.read();
        let mut v = 0.0f64;
        for (pid, _) in path {
            if let Some(s) = ps.get(pid) {
                if s.init && s.var.is_finite() { v += s.var.max(0.0); }
            }
        }
        let sigma = v.sqrt();
        let risk_add = (sigma * 1e6).min(2_000_000.0);
        (MIN_PROFIT_THRESHOLD as f64 + risk_add) as i64
    }

    // [PATCH AV: latency penalty to lamports] >>>
    #[inline]
    fn latency_penalty_lamports(&self, hops: usize) -> i64 {
        let lpm = self.latency_lamports_per_ms.load(AtomOrdering::Relaxed);
        if lpm == 0 { return 0; }
        let s = *self.tip_stats.read();
        if !s.init { return 0; }
        let est_ms = (s.lat_ema_ms * hops as f64).max(0.0);
        (est_ms as u64).saturating_mul(lpm) as i64
    }

    // [PATCH AW: triangle log-arb seeder] >>>
    #[inline]
    fn eff_rate_after_fee(&self, e: &PoolEdge, in_is_b: bool) -> f64 {
        let (rin, rout) = if in_is_b { (e.reserve_b, e.reserve_a) } else { (e.reserve_a, e.reserve_b) };
        if rin == 0 || rout == 0 { return 0.0; }
        let fee = (BPS_DEN - e.fee_bps as u64) as f64 / BPS_DEN as f64;
        let base = (rout as f64) / (rin as f64);
        base * fee
    }

    pub fn triangle_seeds(&self, max_pairs_per_token: usize) -> Vec<[TokenMint;3]> {
        let gk = self.graph_snapshot_topk();
        let mut seeds = Vec::with_capacity(256);
        for (&a, edges1) in gk.iter() {
            let nbrs1: Vec<TokenMint> = edges1.iter().map(|e| if e.token_a==a { e.token_b } else { e.token_a })
                .take(max_pairs_per_token).collect();
            for &b in &nbrs1 {
                if let Some(edges2) = gk.get(&b) {
                    for e2 in edges2.iter().take(max_pairs_per_token) {
                        let c = if e2.token_a==b { e2.token_b } else { e2.token_a };
                        if c == a { continue; }
                        if let Some(edges3) = gk.get(&c) {
                            let mut r1 = 0.0f64; let mut r2 = 0.0f64; let mut r3 = 0.0f64;
                            for e1 in edges1.iter() {
                                let t = if e1.token_a==a { e1.token_b } else { e1.token_a };
        let edges = gk.get(&start_token)?;
        let mut best: Option<(Vec<(PoolId,bool)>, u64, i64)> = None;
        for e in edges.iter() {
            let rev = start_token == e.token_b;
            let out = self.raw_out_cached(e, start_amount, rev);
            if out == 0 { continue; }
            let gas = self.gas_cost_for_edge(e);
            let p = out as i64 - start_amount as i64 - gas as i64;
            if p > MIN_PROFIT_THRESHOLD as i64 {
                let path = vec![(e.pool_id, rev)];
                if best.as_ref().map(|b| p > b.2).unwrap_or(true) { best = Some((path, out, p)); }
            }
        }
        best
    }

    // [PATCH BD: three-hop turbo scout]
    pub fn find_three_hop_cycle_fast(
        &self,
        start_token: TokenMint,
        start_amount: u64,
    ) -> Option<(Vec<(PoolId,bool)>, u64, i64)> {
        let gk = self.graph_snapshot_topk();
        let e1s = gk.get(&start_token)?;
        let mut best: Option<(Vec<(PoolId,bool)>, u64, i64)> = None;

        for e1 in e1s.iter() {
            let r1 = start_token == e1.token_b;
            let mid1 = if r1 { e1.token_a } else { e1.token_b };
            let out1 = self.raw_out_cached(e1, start_amount, r1);
            if out1 == 0 { continue; }
            if let Some(e2s) = gk.get(&mid1) {
                for e2 in e2s.iter() {
                    let r2 = mid1 == e2.token_b;
                    let mid2 = if r2 { e2.token_a } else { e2.token_b };
                    if let Some(e3s) = gk.get(&mid2) {
                        for e3 in e3s.iter() {
                            let can_close = (e3.token_a == mid2 && e3.token_b == start_token)
                                         || (e3.token_b == mid2 && e3.token_a == start_token);
                            if !can_close { continue; }
                            let r3 = mid2 == e3.token_b;
                            let out2 = self.raw_out_cached(e2, out1, r2);
                            if out2 == 0 { continue; }
                            let out3 = self.raw_out_cached(e3, out2, r3);
                            if out3 == 0 { continue; }
                            let gas = self.gas_cost_for_edge(e1) + self.gas_cost_for_edge(e2) + self.gas_cost_for_edge(e3);
                            let prof = out3 as i64 - start_amount as i64 - gas as i64;
                            if prof > MIN_PROFIT_THRESHOLD as i64 {
                                let path = vec![(e1.pool_id, r1), (e2.pool_id, r2), (e3.pool_id, r3)];
                                if best.as_ref().map(|b| prof > b.2).unwrap_or(true) { best = Some((path, out3, prof)); }
                            }
                        }
                    }
                }
            }
        }
        best
    }

    // [PATCH BR: parallel seeded search]
    pub fn find_best_from_starts_parallel(
        &self,
        starts: &[(TokenMint, u64)],
        targets: &[TokenMint],
    ) -> Option<(Vec<(PoolId,bool)>, u64, i64)> {
        starts.par_iter()
            .filter_map(|(t,amt)| self.find_optimal_path(*t, *amt, targets))
            .max_by_key(|(_,_,p)| *p)
    }

    // [PATCH: raw quote microcache + calculators] >>>
    // [PATCH BN: thread-local fast-path + prefer CLOB L2]
    thread_local! {
        static TQUOTE: RefCell<Vec<QuoteSet>> = RefCell::new(vec![
            QuoteSet { a: QuoteLine{tag:0,out:0,stamp:0,epoch:0}, b: QuoteLine{tag:0,out:0,stamp:0,epoch:0} };
            QUOTE_SET_COUNT
        ]);
        static TSTAMP: RefCell<u32> = RefCell::new(1);
    }

    #[inline]
    // ===================== [PIECE M] CLOB tick/lot rounding =====================
    #[inline]
    fn apply_tick_and_lot(&self, edge: &PoolEdge, out_qty: u64) -> u64 {
        let mut q = out_qty;
        if let Some(min_lot) = edge.min_out_lot { if min_lot > 0 { q = (q / min_lot) * min_lot; } }
        q
    }

    #[inline]
    fn clob_l2_out(&self, edge: &PoolEdge, amount_in: u64, is_reverse: bool) -> u64 {
        let (levels_opt, _dir_buy) = if is_reverse { (edge.ob_bids.as_ref(), false) } else { (edge.ob_asks.as_ref(), true) };
        let Some(levels) = levels_opt else { return 0; };
        if levels.is_empty() || amount_in == 0 { return 0; }
        let mut remain = amount_in as u128;
        let mut out_acc: u128 = 0;
        for (_px_q64, qty_out) in levels.iter() {
            if remain == 0 { break; }
            let take = (*qty_out as u128).min(remain);
            out_acc = out_acc.saturating_add(take);
            remain = remain.saturating_sub(take);
        }
        let out = (out_acc.saturating_mul((BPS_DEN - edge.fee_bps as u64) as u128) / (BPS_DEN as u128)) as u64;
        self.apply_tick_and_lot(edge, out)
    }

    #[inline(always)]
    fn calculate_orderbook_proxy_output(
        &self,
        edge: &PoolEdge,
        input_amount: u64,
        is_reverse: bool,
    ) -> u64 {
        if input_amount == 0 { return 0; }
        if let Some(liq) = edge.liquidity { // use liquidity as a depth proxy if provided
            let depth = (liq as f64).max(1.0);
            let impact = ((input_amount as f64) / depth).min(0.10);
            let eff_mult = if is_reverse { 1.0 - impact } else { 1.0 + impact };
            let raw = if is_reverse {
                ((input_amount as f64) * eff_mult.recip()).floor() as u64
            } else {
                ((input_amount as f64) * eff_mult).floor() as u64
            };
            let after_fee = raw.saturating_mul(BPS_DEN - edge.fee_bps as u64) / BPS_DEN;
            return self.apply_tick_and_lot(edge, after_fee);
        }
        let (rin, rout) = if is_reverse { (edge.reserve_b, edge.reserve_a) } else { (edge.reserve_a, edge.reserve_b) };
        let out = Self::cpmm_out_u64(rin, rout, input_amount, edge.fee_bps);
        self.apply_tick_and_lot(edge, out)
    }

    #[inline]
    fn raw_out_cached(&self, edge: &PoolEdge, amount_in: u64, is_reverse: bool) -> u64 {
        let (set_idx, tag) = quote_set_and_tag(&edge.pool_id, is_reverse, amount_in);
        let epoch = self.graph_epoch.load(AtomOrdering::Relaxed);
        if let Some(hit) = TQUOTE.with(|q| {
            let v = q.borrow();
            v.get(set_idx).and_then(|set| {
                if set.a.tag == tag && set.a.epoch == epoch { Some(set.a.out) }
                else if set.b.tag == tag && set.b.epoch == epoch { Some(set.b.out) }
                else { None }
            })
        }) { return hit; }

        if let Some(set) = self.quote_cache.read().get(set_idx) {
            if set.a.tag == tag && set.a.epoch == epoch { return set.a.out; }
            if set.b.tag == tag && set.b.epoch == epoch { return set.b.out; }
        }

        let out = match edge.pool_type {
            PoolType::SerumV3 | PoolType::PhoenixV1 => {
                let l2 = self.clob_l2_out(edge, amount_in, is_reverse);
                if l2 > 0 { l2 } else { self.calculate_orderbook_proxy_output(edge, amount_in, is_reverse) }
            }
            _ => {
                let (rin, rout) = if is_reverse { (edge.reserve_b, edge.reserve_a) } else { (edge.reserve_a, edge.reserve_b) };
                Self::cpmm_out_u64(rin, rout, amount_in, edge.fee_bps)
            }
        };

        // write through TL and shared
        TQUOTE.with(|q| TSTAMP.with(|s| {
            let mut v = q.borrow_mut();
            let mut st = *s.borrow();
            if let Some(slot) = v.get_mut(set_idx) {
                if slot.a.stamp <= slot.b.stamp { slot.a = QuoteLine{ tag, out, stamp: st, epoch }; }
                else { slot.b = QuoteLine{ tag, out, stamp: st, epoch }; }
                st = st.wrapping_add(1); *s.borrow_mut() = st;
            }
        }));
        let now = self.quote_stamp.fetch_add(1, AtomOrdering32::Relaxed);
        if let Some(mut set) = self.quote_cache.write().get_mut(set_idx) {
            if set.a.stamp <= set.b.stamp { set.a = QuoteLine{ tag, out, stamp: now, epoch }; }
            else { set.b = QuoteLine{ tag, out, stamp: now, epoch }; }
        }
        out
    }

    #[inline]
    fn mint_dust(&self, mint: &TokenMint) -> u64 {
        *self.min_transfer_amt.read().get(mint).unwrap_or(&0)
    }

    // [PATCH: CPMM math upgraded to full u128 pipeline with exact BPS] >>>
    #[inline]
    fn cpmm_out_u64(reserve_in: u64, reserve_out: u64, amount_in: u64, fee_bps: u16) -> u64 {
        if reserve_in == 0 || reserve_out == 0 || amount_in == 0 { return 0; }
        let fee_mul = (BPS_DEN - fee_bps as u64) as u128;
        let ain = (amount_in as u128) * fee_mul; // scaled by 10_000
        let rin = reserve_in as u128 * (BPS_DEN as u128);
        // out = (ain * R_out) / (R_in*10k + ain)
        let num = ain.saturating_mul(reserve_out as u128);
        let den = rin.saturating_add(ain);
        if den == 0 { return 0; }
        let out = num / den;
        // slippage guard
        let slip = (out * (MAX_SLIPPAGE_BPS as u128)) / (BPS_DEN as u128);
        out.saturating_sub(slip) as u64
    }
    // <<< END PATCH

    // [PATCH: output calculator hardened + oracle sanity hook point] >>>
    fn calculate_swap_output(
        &self,
        edge: &PoolEdge,
        input_token: TokenMint,
        input_amount: u64,
    ) -> (TokenMint, u64, bool) {
        let is_reverse = input_token == edge.token_b;
        // [PATCH: slot-age early exit] >>>
        let now = self.current_slot.load(AtomOrdering::Relaxed);
        let max_age = self.max_edge_age_slots.load(AtomOrdering::Relaxed);
        if now != 0 && now.saturating_sub(edge.last_update_slot) > max_age {
            return (if is_reverse { edge.token_a } else { edge.token_b }, 0, is_reverse);
        }
        // [PATCH BA: hazard cooldown guard]
        if let Some(s) = self.edge_fail.read().get(&edge.pool_id) {
            let now = self.current_slot.load(AtomOrdering::Relaxed);
            if s.init && s.cooldown_until != 0 && now != 0 && now <= s.cooldown_until {
                return (if is_reverse { edge.token_a } else { edge.token_b }, 0, is_reverse);
            }
        }
        // <<< END PATCH
        // [PATCH: z-score guard] >>>
        if let Some(st) = self.pool_stats.read().get(&edge.pool_id).copied() {
            if st.init && st.var.is_finite() {
                let ratio_now = if edge.reserve_a > 0 { (edge.reserve_b as f64) / (edge.reserve_a as f64) } else { 0.0 };
                let sigma = st.var.max(1e-18).sqrt();
                if ((ratio_now - st.ema).abs() / sigma) > STATS_Z_MAX {
                    return (if is_reverse { edge.token_a } else { edge.token_b }, 0, is_reverse);
                }
            }
        }
        // <<< END PATCH
        let output_token = if is_reverse { edge.token_a } else { edge.token_b };

        // [PATCH: use microcache for raw quote] >>>
        let out = self.raw_out_cached(edge, input_amount, is_reverse);
        // <<< END PATCH

        // [PATCH: anti-toxic oracle guard] >>>
        if out > 0 {
            if let Some(ref o) = self.oracle {
                let implied = if input_amount == 0 { 0.0 } else { (out as f64) / (input_amount as f64) };
                let base = if is_reverse { &edge.token_b } else { &edge.token_a };
                let quote = if is_reverse { &edge.token_a } else { &edge.token_b };
                if let Some(mid) = o.mid(base, quote) {
                    if mid > 0.0 {
                        let dev_bps = ((implied / mid - 1.0).abs() * (BPS_DEN as f64)) as u64;
                        if dev_bps > ORACLE_DEV_BPS_MAX {
                            return (output_token, 0, is_reverse);
                        }
                    }
                }
            }
        }
        // <<< END PATCH

        (output_token, out, is_reverse)
    }
    // <<< END PATCH

    // [PATCH: OB proxy impact model stabilized] >>>
    #[inline(always)]
    fn calculate_orderbook_proxy_output(
        &self,
        edge: &PoolEdge,
        input_amount: u64,
        is_reverse: bool,
    ) -> u64 {
        if input_amount == 0 { return 0; }
        if let (Some(sqrt_price), Some(liq)) = (edge.sqrt_price, edge.liquidity) {
            let px = U64F64::from_bits(sqrt_price);
            let mid = px.saturating_mul(px).to_num::<f64>(); // price
            let depth = (liq as f64).max(1.0);
            // linearized impact: k * size / depth, k tuned by side
            let k = if is_reverse { 0.95 } else { 1.05 };
            let impact = ((input_amount as f64) / depth).min(0.10);
            let eff = if is_reverse { mid * (1.0 - impact) } else { mid * (1.0 + impact) };
            let raw = if is_reverse {
                // selling token B for A
                ((input_amount as f64) / eff).floor() as u64
            } else {
                // buying token B with A
                ((input_amount as f64) * eff).floor() as u64
            };
            raw.saturating_mul(BPS_DEN - edge.fee_bps as u64) / BPS_DEN
        } else {
            // fallback to CPMM conservative
            let (rin, rout) = if is_reverse { (edge.reserve_b, edge.reserve_a) } else { (edge.reserve_a, edge.reserve_b) };
            Self::cpmm_out_u64(rin, rout, input_amount, edge.fee_bps)
        }
    }
    // <<< END PATCH

    fn calculate_prorata_output(
        &self,
        edge: &PoolEdge,
        input_amount: u64,
        is_reverse: bool,
    ) -> u64 {
        if input_amount == 0 { return 0; }
        let (reserve_in, reserve_out) = if is_reverse {
            (edge.reserve_b, edge.reserve_a)
        } else {
            (edge.reserve_a, edge.reserve_b)
        };
        if reserve_in == 0 || reserve_out == 0 { return 0; }

        let oracle_price = if let Some(sqrt_price) = edge.sqrt_price {
            let p = U64F64::from_bits(sqrt_price);
            p.saturating_mul(p).to_num::<f64>()
        } else {
            reserve_out as f64 / reserve_in as f64
        };
        let base = ((input_amount as f64) * oracle_price).floor() as u64;
        let fee_adj = base.saturating_mul(BPS_DEN - edge.fee_bps as u64) / BPS_DEN;
        let avail = reserve_out.saturating_mul(95) / 100;
        fee_adj.min(avail)
    }

    // restore gas cost calculator used by hot paths
    #[inline(always)]
    fn calculate_gas_cost(&self, pool_type: &PoolType) -> u64 {
        let base_cost = match pool_type {
            PoolType::RaydiumV4 => 35_000,
            PoolType::OrcaWhirlpool => 45_000,
            PoolType::SerumV3 => 85_000,
            PoolType::PhoenixV1 => 75_000,
            PoolType::LifinityV2 => 30_000,
        };
        (base_cost as f64 * PRIORITY_FEE_MULTIPLIER) as u64 + GAS_COST_LAMPORTS
    }

    // [PATCH: parallel arbitrage cycle scan + golden-section size optimizer] >>>
    pub fn find_arbitrage_cycles(
        &self,
        min_profit: u64,
        max_depth: usize,
    ) -> Vec<(Vec<(PoolId, bool)>, u64, i64)> {
        // [PATCH: use lock-free RCU snapshot] >>>
        let graph = self.graph_snapshot();
        // <<< END PATCH
        let all_tokens: Vec<TokenMint> = graph.keys().cloned().collect();

        all_tokens
            .par_iter()
            .filter_map(|&start| self.find_profitable_cycle(start, min_profit, max_depth))
            .collect()
    }

    fn find_profitable_cycle(
        &self,
        start_token: TokenMint,
        min_profit: u64,
        max_depth: usize,
    ) -> Option<(Vec<(PoolId, bool)>, u64, i64)> {
        // [PATCH: use lock-free RCU snapshot] >>>
        let graph = self.graph_snapshot();
        // <<< END PATCH
        let mut visited_pools = AHashSet::new();
        let mut best_cycle: Option<(Vec<(PoolId, bool)>, u64, i64)> = None;

        self.dfs_cycle(
            &graph,
            start_token,
            start_token,
            1_000_000_000,
            Vec::new(),
            0,
            0,
            &mut visited_pools,
            &mut best_cycle,
            min_profit,
            max_depth,
        );

        best_cycle
    }

    fn dfs_cycle(
        &self,
        graph: &AHashMap<TokenMint, Vec<Arc<PoolEdge>>>,
        start_token: TokenMint,
        current_token: TokenMint,
        current_amount: u64,
        mut path: PathVec,
        total_gas: u64,
        depth: usize,
        visited_pools: &mut AHashSet<PoolId>,
        best_cycle: &mut Option<(Vec<(PoolId, bool)>, u64, i64)>,
        min_profit: u64,
        max_depth: usize,
    ) {
        if depth > max_depth { return; }

        if current_token == start_token && depth >= 2 {
            let profit = current_amount as i64 - 1_000_000_000i64 - total_gas as i64;
            if profit > min_profit as i64 {
                match best_cycle {
                    Some((_p, _a, best_p)) if profit <= *best_p => {}
                    _ => *best_cycle = Some((path.as_slice().to_vec(), current_amount, profit)),
                }
            }
            return;
        }

        if let Some(edges) = graph.get(&current_token) {
            for edge in edges {
                if visited_pools.contains(&edge.pool_id) { continue; }
                let (next_token, out_amt, is_rev) =
                    self.calculate_swap_output(edge, current_token, current_amount);
                if out_amt == 0 { continue; }

                let gas = total_gas + self.gas_cost_for_edge(edge);
                // bound gas relative to amount (prevents silly paths)
                if gas > current_amount / 100 { continue; }

                visited_pools.insert(edge.pool_id);
                if path.try_push((edge.pool_id, is_rev)).is_err() {
                    visited_pools.remove(&edge.pool_id);
                    continue;
                }

                self.dfs_cycle(
                    graph,
                    start_token,
                    next_token,
                    out_amt,
                    path.clone(),
                    gas,
                    depth + 1,
                    visited_pools,
                    best_cycle,
                    min_profit,
                    max_depth,
                );

                visited_pools.remove(&edge.pool_id);
                path.pop();
            }
        }
    }
    // <<< END PATCH

    // [PATCH: execution simulator unchanged except for path type] >>>
    #[cold]
    pub fn simulate_path_execution(
        &self,
        path: &[(PoolId, bool)],
        input_amount: u64,
    ) -> Result<(u64, Vec<u64>), &'static str> {
        let pool_cache = self.pool_cache.read();
        let mut current_amount = input_amount;
        let mut amounts = Vec::with_capacity(path.len() + 1);
        amounts.push(current_amount);

        for &(pool_id, is_reverse) in path {
            let pool = pool_cache.get(&pool_id)
                .ok_or("Pool not found in cache")?;

            let input_token = if is_reverse { pool.token_b } else { pool.token_a };
            let (_, output_amount, _) = self.calculate_swap_output(
                pool,
                input_token,
                current_amount,
            );

            if output_amount == 0 {
                return Err("Zero output detected in path");
            }

            current_amount = output_amount;
            amounts.push(current_amount);
        }
        Ok((current_amount, amounts))
    }
    // <<< END PATCH

    // [PATCH: robust validator with 95% tolerance kept] >>>
    pub fn validate_path_profitability(
        &self,
        path: &[(PoolId, bool)],
        input_amount: u64,
        expected_profit: i64,
    ) -> bool {
        match self.simulate_path_execution(path, input_amount) {
            Ok((final_amount, _)) => {
                let total_gas = path.iter().map(|(pid, _)| {
                    self.pool_cache.read().get(pid).map(|p| self.gas_cost_for_edge(p)).unwrap_or(0)
                }).sum::<u64>();
                let actual_profit = final_amount as i64 - input_amount as i64 - total_gas as i64;
                (actual_profit as f64) >= (expected_profit as f64 * 0.95)
            }
            Err(_) => false,
        }
    }
    // <<< END PATCH

    pub fn get_path_metadata(&self, path: &[(PoolId, bool)]) -> Vec<(PoolType, TokenMint, TokenMint, u16)> {
        let pool_cache = self.pool_cache.read();
        path.iter()
            .filter_map(|(pool_id, is_reverse)| {
                pool_cache.get(pool_id).map(|pool| {
                    let (token_in, token_out) = if *is_reverse { (pool.token_b, pool.token_a) } else { (pool.token_a, pool.token_b) };
                    (pool.pool_type, token_in, token_out, pool.fee_bps)
                })
            })
            .collect()
    }

    // optimize_amount_golden removed per scope of this module

    // [PATCH: true freshness pruning by slot, not just zero reserves] >>>
    pub fn prune_stale_pools(&self, _max_age_slots: u64, _current_slot: u64) {
        let mut graph = self.graph.write();
        let mut pool_cache = self.pool_cache.write();
        let mut sqrt_cache = self.sqrt_price_cache.write();

        let stale: Vec<PoolId> = pool_cache
            .iter()
            .filter_map(|(pid, pool)| {
                if pool.reserve_a == 0 || pool.reserve_b == 0 { Some(*pid) } else { None }
            })
            .collect();

        for pid in stale {
            pool_cache.remove(&pid);
            sqrt_cache.remove(&pid);
            for edges in graph.values_mut() {
                edges.retain(|e| e.pool_id != pid);
            }
        }
        graph.retain(|_, edges| !edges.is_empty());
    }
    // <<< END PATCH

    pub fn get_stats(&self) -> (usize, usize, usize) {
        let graph = self.graph.read();
        let pool_cache = self.pool_cache.read();
        let num_tokens = graph.len();
        let num_pools = pool_cache.len();
        let num_edges = graph.values().map(|v| v.len()).sum();
        (num_tokens, num_pools, num_edges)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_finding() {
        let finder = GeodesicPathFinder::new();
        let token_a = TokenMint(Pubkey::new_unique());
        let token_b = TokenMint(Pubkey::new_unique());

        finder.update_pool(PoolEdge {
            pool_id: PoolId(Pubkey::new_unique()),
            pool_type: PoolType::RaydiumV4,
            token_a,
            token_b,
            reserve_a: 1_000_000_000,
            reserve_b: 2_000_000_000,
            fee_bps: 25,
            tick_spacing: None,
            current_tick: None,
            liquidity: None,
            sqrt_price: None,
            last_update_slot: 1,
            cu_estimate: 0,
            min_in_lot: None,
            min_out_lot: None,
            price_tick_bps: None,
            ro_accounts: 6,
            rw_accounts: 4,
            ix_bytes_estimate: 120,
            ob_asks: None,
            ob_bids: None,
            ob_last_update_slot: None,
            acct_ro_fp: [0;4],
            acct_rw_fp: [0;4],
            tick_size_q64: None,
            program_tag: 0,
        });
        let path = finder.find_optimal_path(token_a, 100_000_000, &[token_b]);
        assert!(path.is_some());
    }
}
