#![allow(clippy::too_many_arguments)]
#![allow(clippy::float_cmp)]
#![cfg_attr(feature = "strict_lints", deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    clippy::dbg_macro,
    unused_must_use,
    unused_assignments,
    clippy::redundant_clone,
    clippy::indexing_slicing,
    clippy::alloc_instead_of_core,
    clippy::std_instead_of_alloc
))]
#![forbid(unsafe_code)]
// Allocators OFF-CHAIN only (never BPF / on-chain)
#[cfg(all(feature = "mimalloc",
          not(target_os = "solana"),
          not(target_arch = "bpfel"),
          not(target_arch = "bpfeb")))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(all(feature = "jemalloc",
          not(feature = "mimalloc"),
          not(target_os = "solana"),
          not(target_arch = "bpfel"),
          not(target_arch = "bpfeb")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;
use anchor_lang::prelude::*;
use solana_sdk::{clock::Clock, pubkey::Pubkey, sysvar::clock as sysclock};
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use indexmap::IndexMap;

use std::{
    cmp::Ordering,
    collections::VecDeque,
    sync::Arc,
    time::Duration,
};
use core::hash::{BuildHasher, Hasher};
use core::sync::atomic::{AtomicU64, AtomicUsize, Ordering as AOrd};
#[cfg(feature = "pl")]
use parking_lot::RwLock as PLRwLock;
#[cfg(not(feature = "pl"))]
use std::sync::RwLock as PLRwLock;
use once_cell::sync::Lazy;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use arrayref::{array_ref, array_refs};
use borsh::{BorshDeserialize, BorshSerialize};
use fixed::types::I80F48;

// hot/cold markers (stable, optimizer-friendly)
#[inline(always)]
#[allow(dead_code)]
fn hot<T>(x: T) -> T { x }

// ===== Lock-free hot cache for venue impact (RCU double-gen) =====
mod impact_hot {
    use core::sync::atomic::{AtomicU32, AtomicU64, AtomicU8, AtomicI64, Ordering};

    // Gen increments: odd = being written, even = stable.
    static GEN:    AtomicU64 = AtomicU64::new(0);

    // Internal venue tags (independent of enum discriminants)
    pub const VENUE_NONE: u8 = 0;
    pub const VENUE_OB:   u8 = 1;
    pub const VENUE_CPMM: u8 = 2;
    pub const VENUE_SS:   u8 = 3;

    static VENUE:  AtomicU8  = AtomicU8::new(VENUE_NONE);

    // ---- CPMM hot fields ----
    static CPMM_RES_A:   AtomicU64 = AtomicU64::new(0);
    static CPMM_RES_B:   AtomicU64 = AtomicU64::new(0);
    static CPMM_FEE_BPS: AtomicU32 = AtomicU32::new(0);
    static CPMM_LAST:    AtomicU64 = AtomicU64::new(0);

    // ---- StableSwap hot fields (n=2 fast path) ----
    static SS_R0:       AtomicU64 = AtomicU64::new(0);
    static SS_R1:       AtomicU64 = AtomicU64::new(0);
    static SS_A:        AtomicU64 = AtomicU64::new(1);
    static SS_FEE_BPS:  AtomicU32 = AtomicU32::new(0);

    // ---- OrderBook hot fields (summary only) ----
    static OB_MID_BITS:    AtomicU64 = AtomicU64::new(0);
    static OB_SPREAD_BITS: AtomicU64 = AtomicU64::new(0);
    static OB_TS:          AtomicI64 = AtomicI64::new(0);

    #[inline(always)] fn read_gen() -> u64 { GEN.load(Ordering::Acquire) }
    #[inline(always)] pub fn begin_write() { GEN.fetch_add(1, Ordering::Release); } // -> odd
    #[inline(always)] pub fn end_write()   { GEN.fetch_add(1, Ordering::Release); } // -> even

    // ---- Writers (called from setters) ----
    #[inline(always)]
    pub fn write_venue_ob(mid: f64, spread: f64, ts: i64) {
        begin_write();
        OB_MID_BITS.store(mid.to_bits(), Ordering::Relaxed);
        OB_SPREAD_BITS.store(spread.to_bits(), Ordering::Relaxed);
        OB_TS.store(ts, Ordering::Relaxed);
        VENUE.store(VENUE_OB, Ordering::Release);
        end_write();
    }
    #[inline(always)]
    pub fn write_venue_cpmm(res_a: u64, res_b: u64, fee_bps: u32, last: u64) {
        begin_write();
        CPMM_RES_A.store(res_a, Ordering::Relaxed);
        CPMM_RES_B.store(res_b, Ordering::Relaxed);
        CPMM_FEE_BPS.store(fee_bps, Ordering::Relaxed);
        CPMM_LAST.store(last, Ordering::Relaxed);
        VENUE.store(VENUE_CPMM, Ordering::Release);
        end_write();
    }
    #[inline(always)]
    pub fn write_venue_ss(r0: u64, r1: u64, a: u64, fee_bps: u32) {
        begin_write();
        SS_R0.store(r0, Ordering::Relaxed);
        SS_R1.store(r1, Ordering::Relaxed);
        SS_A.store(a.max(1), Ordering::Relaxed);
        SS_FEE_BPS.store(fee_bps, Ordering::Relaxed);
        VENUE.store(VENUE_SS, Ordering::Release);
        end_write();
    }

    // ---- Readers (lock-free; consistent via gen check) ----
    #[derive(Copy, Clone)]
    pub struct CpmmHot { pub x0: u64, pub y0: u64, pub fee_bps: u32 }
    #[derive(Copy, Clone)]
    pub struct SsHot   { pub r0: u64, pub r1: u64, pub a: u64,  pub fee_bps: u32 }
    #[derive(Copy, Clone)]
    pub struct ObHot   { pub mid: f64, pub spread: f64, pub ts: i64 }

    #[inline(always)]
    pub fn cpmm() -> Option<CpmmHot> {
        loop {
            let g1 = read_gen();
            if g1 & 1 == 1 { continue; }
            if VENUE.load(Ordering::Relaxed) != VENUE_CPMM { return None; }
            let x0 = CPMM_RES_A.load(Ordering::Relaxed);
            let y0 = CPMM_RES_B.load(Ordering::Relaxed);
            let fee_bps = CPMM_FEE_BPS.load(Ordering::Relaxed);
            let g2 = read_gen();
            if g1 == g2 { return Some(CpmmHot { x0, y0, fee_bps }); }
        }
    }

    #[inline(always)]
    pub fn ss() -> Option<SsHot> {
        loop {
            let g1 = read_gen();
            if g1 & 1 == 1 { continue; }
            if VENUE.load(Ordering::Relaxed) != VENUE_SS { return None; }
            let r0 = SS_R0.load(Ordering::Relaxed);
            let r1 = SS_R1.load(Ordering::Relaxed);
            let a  = SS_A.load(Ordering::Relaxed);
            let fee_bps = SS_FEE_BPS.load(Ordering::Relaxed);
            let g2 = read_gen();
            if g1 == g2 { return Some(SsHot { r0, r1, a, fee_bps }); }
        }
    }

    #[inline(always)]
    pub fn ob() -> Option<ObHot> {
        loop {
            let g1 = read_gen();
            if g1 & 1 == 1 { continue; }
            if VENUE.load(Ordering::Relaxed) != VENUE_OB { return None; }
            let mid    = f64::from_bits(OB_MID_BITS.load(Ordering::Relaxed));
            let spread = f64::from_bits(OB_SPREAD_BITS.load(Ordering::Relaxed));
            let ts     = OB_TS.load(Ordering::Relaxed);
            let g2 = read_gen();
            if g1 == g2 { return Some(ObHot { mid, spread, ts }); }
        }
    }
}

// ===== Impact model epoch (readers can poll this without locking) =====
mod impact_epoch {
    use core::sync::atomic::{AtomicU64, Ordering::Relaxed};
    pub static MODEL_EPOCH: AtomicU64 = AtomicU64::new(1);
    #[inline(always)] pub fn bump() -> u64 { MODEL_EPOCH.fetch_add(1, Relaxed) + 1 }
    #[inline(always)] pub fn current() -> u64 { MODEL_EPOCH.load(Relaxed) }
}

// --- Delegators to preserve existing call sites ---
impl FractionalPositionOptimizer {
    #[inline(always)] fn slot_phase_from_now() -> SlotPhase { slot_clock::slot_phase_from_now() }
    #[inline(always)] fn note_slot_boundary(ms: u64) { slot_clock::note_slot_boundary(ms) }
    #[inline(always)] fn ms_to_next_phase_boundary() -> u64 { slot_clock::ms_to_next_phase_boundary() }
    #[inline(always)] fn phase_timing() -> (SlotPhase,u64,u64) { slot_clock::phase_timing() }
    #[inline(always)] fn should_send_now(grace_ms: u64) -> bool { slot_clock::should_send_now(grace_ms) }
    #[inline(always)] fn next_send_delay(grace_ms: u64) -> u64 { slot_clock::next_send_delay(grace_ms) }
    #[inline(always)] fn process_stagger_ms(max_ms: u64) -> u64 { slot_clock::process_stagger_ms(max_ms) }
}

// ========================= Slot clock (V6: unified state) ====================
// One source of truth for slot EWMA, offset, last phase + thread-local epoch.
// Exposes helpers used by the optimizer (+ slot_ms_estimate).
mod slot_clock {
    use std::sync::atomic::{AtomicU64, AtomicU8, Ordering as AOrd};
    use std::time::Instant;
    use super::SlotPhase;

    pub(super) mod state {
        use super::*;
        pub static SLOT_MS_EWMA: AtomicU64 = AtomicU64::new(400); // default mainnet
        pub static SLOT_OFFSET_MS: AtomicU64 = AtomicU64::new(0);
        pub static LAST_PHASE:    AtomicU8  = AtomicU8::new(0);   // 0=Early,1=Mid,2=Late
        thread_local! { pub static T0: Instant = Instant::now(); }
    }

    #[inline(always)]
    pub fn slot_ms_estimate() -> u64 {
        state::SLOT_MS_EWMA.load(AOrd::Relaxed).clamp(100, 1_500)
    }

    #[inline(always)]
    pub fn slot_phase_from_now() -> SlotPhase {
        use state::*;
        let slot_ms = SLOT_MS_EWMA.load(AOrd::Relaxed).clamp(100, 1_500);
        let elapsed_ms = T0.with(|t0| t0.elapsed().as_millis() as u64);
        let ofs = SLOT_OFFSET_MS.load(AOrd::Relaxed) % slot_ms;
        let frac = ((elapsed_ms + ofs) % slot_ms) as f64 / (slot_ms as f64);

        const EARLY_FRAC: f64 = 1.0/3.0;
        const MID_FRAC:   f64 = 2.0/3.0;
        const HYS: f64 = 0.03;

        let cand = if frac < EARLY_FRAC { 0u8 } else if frac < MID_FRAC { 1 } else { 2 };
        let near_early_mid = (frac - EARLY_FRAC).abs() < HYS;
        let near_mid_late  = (frac - MID_FRAC).abs()  < HYS;

        let out = if near_early_mid || near_mid_late {
            LAST_PHASE.load(AOrd::Relaxed)
        } else { cand };

        if out != LAST_PHASE.load(AOrd::Relaxed) { LAST_PHASE.store(out, AOrd::Relaxed); }
        match out { 0 => SlotPhase::Early, 1 => SlotPhase::Mid, _ => SlotPhase::Late }
    }

    #[inline(always)]
    pub fn note_slot_boundary(observed_ms: u64) {
        use state::*;
        const MIN: u64 = 200; const MAX: u64 = 1_000;
        let x = observed_ms.clamp(MIN, MAX);
        let cur = SLOT_MS_EWMA.load(AOrd::Relaxed).clamp(MIN, MAX);
        let upd = cur + ((x.saturating_sub(cur)) >> 4); // α=1/16 EWMA
        SLOT_MS_EWMA.store(upd, AOrd::Relaxed);

        let elapsed_ms = T0.with(|t0| t0.elapsed().as_millis() as u64);
        let slot_ms = upd.max(MIN);
        let offset = (slot_ms.saturating_sub(elapsed_ms % slot_ms)) % slot_ms;
        SLOT_OFFSET_MS.store(offset, AOrd::Relaxed);
    }

    #[inline(always)]
    pub fn ms_to_next_phase_boundary() -> u64 {
        use state::*;
        let slot_ms = SLOT_MS_EWMA.load(AOrd::Relaxed).clamp(100, 1_500);
        let ofs = SLOT_OFFSET_MS.load(AOrd::Relaxed) % slot_ms;
        let prog = T0.with(|t0| ((t0.elapsed().as_millis() as u64) + ofs) % slot_ms);
        let b1 = slot_ms/3; let b2 = (2*slot_ms)/3;
        if prog < b1 { b1 - prog } else if prog < b2 { b2 - prog } else { slot_ms - prog }
    }

    #[inline(always)]
    pub fn phase_timing() -> (SlotPhase, u64, u64) {
        use state::*;
        let slot_ms = SLOT_MS_EWMA.load(AOrd::Relaxed).clamp(100, 1_500);
        let ofs = SLOT_OFFSET_MS.load(AOrd::Relaxed) % slot_ms;
        let prog = T0.with(|t0| ((t0.elapsed().as_millis() as u64) + ofs) % slot_ms);
        let b1 = slot_ms/3; let b2 = (2*slot_ms)/3;
        if      prog < b1 { (SlotPhase::Early, prog,          b1 - prog) }
        else if prog < b2 { (SlotPhase::Mid,   prog - b1,     b2 - prog) }
        else              { (SlotPhase::Late,  prog - b2,     slot_ms - prog) }
    }

    #[inline(always)]
    pub fn next_send_delay(grace_ms: u64) -> u64 {
        let (_ph, _into, rem) = phase_timing();
        if rem > grace_ms { 0 } else { ms_to_next_phase_boundary() + 1 }
    }
    #[inline(always)]
    pub fn should_send_now(grace_ms: u64) -> bool {
        let (_ph, _into, rem) = phase_timing(); rem > grace_ms
    }

    #[inline(always)]
    pub fn process_stagger_ms(max_stagger_ms: u64) -> u64 {
        use state::*;
        static SEED: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);
        let s0 = SEED.load(AOrd::Relaxed);
        if s0 == 0 {
            let now = T0.with(|t0| t0.elapsed().as_nanos() as u64);
            SEED.store((now ^ 0xD6E8_F1F2_9CDE_3EAD) | 1, AOrd::Relaxed);
        }
        let mut x = SEED.load(AOrd::Relaxed);
        x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
        let z = x.wrapping_mul(0x2545_F491_4F6C_DD1D);
        (z % (max_stagger_ms.max(1))) as u64
    }

    #[test]
    fn test_cpmm_impact_monotone() {
        let pool = CpmmPool { reserves_a: 1_000_000, reserves_b: 1_000_000, fee_bps: 30, last_updated: 1 };
        let s1 = FractionalPositionOptimizer::cpmm_impact_cost(10_000.0, &pool);
        let s2 = FractionalPositionOptimizer::cpmm_impact_cost(100_000.0, &pool);
        assert!(s2 > s1 && s1 >= 0.0);
    }

    #[test]
    fn test_snapshot_robust_vol() {
        let opt = FractionalPositionOptimizer::new(1_000_000);
        {
            let mut vb = opt.volatility_buffer.write().unwrap();
            vb.clear();
            vb.extend_from_slice(&[0.02, 0.021, 0.0205, 0.2, 0.019, 0.018]);
        }
        let s = opt.snapshot();
        assert!(s.ewma_vol < 0.05 && s.ewma_vol > 0.015);
    }
}

#[cold]
#[allow(dead_code)]
fn cold<T>(x: T) -> T { x }

#[inline(always)]
fn now_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => d.as_millis() as u64,
        Err(_) => 0,
    }
}

// ========== Inclusion-aware gas (V6: starvation + miss ratchet) =============
const MAX_TIP_LAMPORTS: u64 = 3_000_000;
const MAX_TIP_MULT: f64    = 6.0;
const MIN_TIP_LAMPORTS: u64 = 5_000;

static TIP_FLOOR: [AtomicU64; 3] = [
    AtomicU64::new(MIN_TIP_LAMPORTS),
    AtomicU64::new(MIN_TIP_LAMPORTS),
    AtomicU64::new(MIN_TIP_LAMPORTS),
];
static TIP_FLOOR_LAST_MS: [AtomicU64; 3] = [
    AtomicU64::new(0),
    AtomicU64::new(0),
    AtomicU64::new(0),
];

#[inline(always)]
fn phase_idx(p: SlotPhase) -> usize { match p { SlotPhase::Early => 0, SlotPhase::Mid => 1, SlotPhase::Late => 2 } }

#[inline(always)]
fn now_ms_monotonic() -> u64 { slot_clock::state::T0.with(|t0| t0.elapsed().as_millis() as u64) }

const FLOOR_HALF_LIFE_MS: u64 = 15_000;
#[inline(always)]
fn decayed_phase_floor(phase: SlotPhase) -> u64 {
    let i = phase_idx(phase);
    let floor = TIP_FLOOR[i].load(AOrd::Relaxed);
    let last  = TIP_FLOOR_LAST_MS[i].load(AOrd::Relaxed);
    if last == 0 { return floor; }
    let dt = now_ms_monotonic().saturating_sub(last);
    if dt < FLOOR_HALF_LIFE_MS { return floor; }
    let mut excess = floor.saturating_sub(MIN_TIP_LAMPORTS);
    let mut n = (dt / FLOOR_HALF_LIFE_MS) as u32;
    while n > 0 && excess > 0 { excess >>= 1; n -= 1; }
    MIN_TIP_LAMPORTS + excess
}

// Bounded starvation boost per phase
#[inline(always)]
fn phase_starvation_boost(phase: SlotPhase) -> f64 {
    let i = phase_idx(phase);
    let last = TIP_FLOOR_LAST_MS[i].load(AOrd::Relaxed);
    let now = now_ms_monotonic();
    let slot = slot_clock::slot_ms_estimate();
    if last == 0 || slot == 0 { return 1.0; }
    let slots_since = ((now.saturating_sub(last)) as f64) / (slot as f64);
    (1.0 + (0.02 * slots_since)).min(1.25)
}

#[cfg(feature = "anti_herd")]
#[inline(always)]
fn jitter_tip_lamports(tip: u64) -> u64 {
    use core::sync::atomic::AtomicU64 as A64;
    static RNG: A64 = A64::new(0x9E37_79B9_7F4A_7C15);
    let x0 = RNG.fetch_add(0x9E37_79B9_7F4A_7C15, AOrd::Relaxed).wrapping_add(now_ms_monotonic());
    let mut z = x0 ^ (x0 >> 12) ^ (x0 << 25) ^ (x0 >> 27);
    z = z.wrapping_mul(0x2545_F491_4F6C_DD1D);
    let r = (z >> 11) as u32;
    const J: i32 = 30; // ±3%
    let delta_permille = (r % (2 * J as u32 + 1)) as i32 - J;
    let adj = ((tip as u128) * (1000 + delta_permille as u128) / 1000) as u64;
    adj.max(MIN_TIP_LAMPORTS)
}

// ========================= Observability hooks (UPGRADED) ====================
#[derive(Clone, Copy)]
pub struct MetricKey {
    pub name: &'static str,
    pub hash: u64,
}
impl MetricKey {
    #[inline(always)]
    pub const fn new(name: &'static str) -> Self {
        // const FNV-1a 64
        const INIT: u64 = 0xcbf29ce484222325;
        const PRIME: u64 = 0x0000_0100_0000_01B3;
        let bytes = name.as_bytes();
        let mut h = INIT;
        let mut i = 0;
        while i < bytes.len() {
            h ^= bytes[i] as u64;
            h = h.wrapping_mul(PRIME);
            i += 1;
        }
        Self { name, hash: h }
    }
    #[inline(always)] pub const fn as_str(&self) -> &'static str { self.name }
}

pub trait MetricsSink: Send + Sync {
    fn gauge(&self, key: &str, value: f64);
    fn counter(&self, key: &str, delta: f64);
    fn event(&self, key: &str, msg: &str);

    // ID-accelerated defaults (non-breaking):
    #[inline(always)] fn gauge_key(&self, key: MetricKey, v: f64) { self.gauge(key.as_str(), v) }
    #[inline(always)] fn counter_key(&self, key: MetricKey, d: f64) { self.counter(key.as_str(), d) }
    #[inline(always)] fn event_key(&self, key: MetricKey, msg: &str) { self.event(key.as_str(), msg) }
}

pub trait Tracer: Send + Sync {
    fn span<F: FnOnce()>(&self, name: &str, f: F);
    fn log_kv(&self, key: &str, value: &str);
}

#[cfg(not(feature="obs_off"))]
macro_rules! m_gauge { ($sink:expr, $key:expr, $val:expr) => { $sink.gauge_key($key, $val); }; }
#[cfg(feature="obs_off")]
macro_rules! m_gauge { ($sink:expr, $key:expr, $val:expr) => { let _ = (&$sink, $key, $val); }; }

#[cfg(not(feature="obs_off"))]
macro_rules! m_counter { ($sink:expr, $key:expr, $val:expr) => { $sink.counter_key($key, $val); }; }
#[cfg(feature="obs_off")]
macro_rules! m_counter { ($sink:expr, $key:expr, $val:expr) => { let _ = (&$sink, $key, $val); }; }

#[cfg(not(feature="obs_off"))]
macro_rules! m_event { ($sink:expr, $key:expr, $msg:expr) => { $sink.event_key($key, $msg); }; }
#[cfg(feature="obs_off")]
macro_rules! m_event { ($sink:expr, $key:expr, $msg:expr) => { let _ = (&$sink, $key, $msg); }; }

struct NoopMetrics;
impl MetricsSink for NoopMetrics {
    #[inline(always)] fn gauge(&self, _key: &str, _value: f64) {}
    #[inline(always)] fn counter(&self, _key: &str, _delta: f64) {}
    #[inline(always)] fn event(&self, _key: &str, _msg: &str) {}
}

pub struct SpanTimer {
    name: &'static str,
    t0_ms: u64,
    tracer: Arc<dyn Tracer>,
}
impl SpanTimer {
    #[inline(always)]
    pub fn new(tracer: Arc<dyn Tracer>, name: &'static str) -> Self {
        Self { name, t0_ms: now_ms(), tracer }
    }
}
impl Drop for SpanTimer {
    #[inline(always)]
    fn drop(&mut self) {
        let dt = now_ms().saturating_sub(self.t0_ms);
        self.tracer.log_kv("span_ms", self.name);
        self.tracer.log_kv("span_elapsed_ms", &dt.to_string());
    }
}

struct NoopTracer;
impl Tracer for NoopTracer {
    #[inline(always)] fn span<F: FnOnce()>(&self, _name: &str, f: F) { f() }
    #[inline(always)] fn log_kv(&self, _key: &str, _value: &str) {}
}
// === [ELITE v20: PIECE H15 — FNV hasher + IndexMap alias] ====================
#[derive(Clone, Copy)]
struct FnvHasher(u64);
impl Default for FnvHasher { fn default() -> Self { Self(0xcbf29ce484222325) } }
impl Hasher for FnvHasher {
    #[inline(always)]
    fn write(&mut self, bytes: &[u8]) {
        let mut h = self.0;
        for &b in bytes {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01B3);
        }
        self.0 = h;
    }
    #[inline(always)]
    fn finish(&self) -> u64 { self.0 }
}

#[derive(Clone, Copy, Default)]
pub struct FnvBuildHasher;
impl BuildHasher for FnvBuildHasher {
    type Hasher = FnvHasher;
    #[inline(always)]
    fn build_hasher(&self) -> Self::Hasher { FnvHasher::default() }
}

/// Stable, deterministic IndexMap (use `IxMap<..>` where order and speed matter)
pub type IxMap<K, V> = indexmap::IndexMap<K, V, FnvBuildHasher>;

// === [ELITE v20: PIECE H16 — spin backoff] ===================================
#[inline(always)]
const fn is_bpf() -> bool {
    cfg!(any(target_arch = "bpfel", target_arch = "bpfeb", target_os = "solana"))
}

#[derive(Clone, Copy, Debug)]
pub struct Backoff { n: u32 }
impl Backoff {
    #[inline(always)] pub const fn new() -> Self { Self { n: 0 } }
    #[inline(always)] pub fn reset(&mut self) { self.n = 0; }
    #[inline(always)]
    pub fn snooze(&mut self) {
        if is_bpf() { return; }
        let k = self.n.min(16);
        for _ in 0..(1u32 << k) { core::hint::spin_loop(); }
        #[cfg(not(any(target_arch="bpfel", target_arch="bpfeb", target_os="solana")))]
        {
            if self.n >= 10 { std::thread::yield_now(); }
        }
        self.n = self.n.saturating_add(1);
    }
}

// === [ELITE v20: PIECE H17 — CPU caps] =======================================
#[derive(Clone, Copy, Debug)]
pub struct CpuCaps { pub sse42: bool, pub avx2: bool, pub neon: bool, pub aes: bool, pub sha: bool }

pub static CPU_CAPS: Lazy<CpuCaps> = Lazy::new(|| {
    if is_bpf() {
        CpuCaps { sse42:false, avx2:false, neon:false, aes:false, sha:false }
    } else {
        #[allow(unused_mut)]
        let mut caps = CpuCaps { sse42:false, avx2:false, neon:false, aes:false, sha:false };
        #[cfg(any(target_arch="x86", target_arch="x86_64"))] {
            caps.sse42 = std::is_x86_feature_detected!("sse4.2");
            caps.avx2  = std::is_x86_feature_detected!("avx2");
            caps.aes   = std::is_x86_feature_detected!("aes");
            caps.sha   = std::is_x86_feature_detected!("sha");
        }
        #[cfg(target_arch="aarch64")] {
            caps.neon = true;
            caps.aes = true;
            caps.sha = false;
        }
        caps
    }
});
#[inline(always)] pub fn cpu_caps() -> &'static CpuCaps { &*CPU_CAPS }

// === [ELITE v20: PIECE H18 — sanity macros] ==================================
#[macro_export]
macro_rules! sanity_check {
    ($cond:expr, $tr:expr, $($msg:tt)*) => {{
        #[cfg(all(feature="sanity",
                  not(any(target_arch="bpfel", target_arch="bpfeb", target_os="solana"))))]
        {
            if !$cond {
                let m = format!($($msg)*);
                $tr.log_kv("sanity_fail", &m);
            }
        }
        $cond
    }};
}

#[macro_export]
macro_rules! sanity_note {
    ($tr:expr, $($msg:tt)*) => {{
        #[cfg(all(feature="sanity",
                  not(any(target_arch="bpfel", target_arch="bpfeb", target_os="solana"))))]
        {
            let m = format!($($msg)*);
            $tr.log_kv("sanity", &m);
        }
    }};
}

// Monotonic wall clock with optional backends and slot helpers
pub struct MonotonicClock {
    start: Instant,
    base_ms: u64,
}
impl MonotonicClock {
    fn epoch_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
    pub fn new() -> Self {
        Self { start: Instant::now(), base_ms: Self::epoch_ms() }
    }
    #[inline(always)]
    pub fn now_ms(&self) -> u64 { self.base_ms + self.start.elapsed().as_millis() as u64 }
    #[inline(always)]
    pub fn now_us(&self) -> u64 { self.base_ms * 1000 + self.start.elapsed().as_micros() as u64 }
    #[inline(always)]
    pub fn elapsed_ms_since(&self, t0_ms: u64) -> u64 { self.now_ms().saturating_sub(t0_ms) }
}
pub static GLOBAL_CLOCK: Lazy<MonotonicClock> = Lazy::new(MonotonicClock::new);
#[inline(always)]
fn now_ms() -> u64 { GLOBAL_CLOCK.now_ms() }

// Env knobs (once-parsed)
#[inline(always)]
fn env_f64(key: &str, default: f64, lo: f64, hi: f64) -> f64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .map(|v| v.clamp(lo, hi))
        .unwrap_or(default)
}
#[inline(always)]
fn env_u64(key: &str, default: u64, lo: u64, hi: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .map(|v| v.clamp(lo, hi))
        .unwrap_or(default)
}

// === [IMPROVED v17: PIECE G3 — env knobs++++] ===
macro_rules! knob { ($lazy_ident:ident) => { *$lazy_ident } }

pub static SLOT_MS_D: Lazy<f64> = Lazy::new(|| env_f64("QA_SLOT_MS", 400.0, 200.0, 1000.0));
// Optional live knobs (safe clamps; ignore if not set)
pub static KELLY_FRACTION_D:       Lazy<f64> = Lazy::new(|| env_f64("QA_KELLY_FRACTION",     0.25,  0.0,  0.5 ));
pub static MAX_POSITION_RATIO_D:   Lazy<f64> = Lazy::new(|| env_f64("QA_MAX_POSITION_RATIO", 0.15,  0.01, 0.50));
pub static CONFIDENCE_THRESHOLD_D: Lazy<f64> = Lazy::new(|| env_f64("QA_CONFIDENCE",         0.68,  0.50, 0.95));
pub static SLIPPAGE_BUFFER_D:      Lazy<f64> = Lazy::new(|| env_f64("QA_SLIPPAGE_BUFFER",    0.003, 0.0001, 0.02));
pub static TARGET_PORTFOLIO_VOL_D: Lazy<f64> = Lazy::new(|| env_f64("QA_TGT_VOL",            0.10,  0.01, 0.50));
pub static ES95_BUDGET_D:          Lazy<f64> = Lazy::new(|| env_f64("QA_ES95",               0.02,  0.005, 0.20));
pub static ES99_BUDGET_D:          Lazy<f64> = Lazy::new(|| env_f64("QA_ES99",               0.05,  0.01,  0.50));

#[inline(always)]
pub fn slot_duration_ms() -> f64 { *SLOT_MS_D }
#[inline(always)]
pub fn slot_phase_fraction(slot_start_ms: u64) -> f64 {
    let dur = slot_duration_ms().max(1.0);
    let t = GLOBAL_CLOCK.elapsed_ms_since(slot_start_ms) as f64;
    (t / dur).clamp(0.0, 1.0)
}

// === [IMPROVED v11: PIECE S48 — type-safe units (opt-in, zero-cost)] ===
#[cfg(feature = "strong_units")]
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lamports(pub u64);
#[cfg(not(feature = "strong_units"))]
pub type Lamports = u64;

#[cfg(feature = "strong_units")]
#[repr(transparent)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Bps(pub u16);
#[cfg(not(feature = "strong_units"))]
pub type Bps = u16;

#[cfg(feature = "strong_units")]
impl Lamports { #[inline(always)] pub const fn get(self) -> u64 { self.0 } }
#[cfg(feature = "strong_units")]
impl Bps       { #[inline(always)] pub const fn get(self) -> u16 { self.0 } }

#[cfg(feature = "strong_units")]
impl From<u64> for Lamports { #[inline(always)] fn from(v: u64) -> Self { Lamports(v) } }
#[cfg(feature = "strong_units")]
impl From<Lamports> for u64 { #[inline(always)] fn from(v: Lamports) -> Self { v.0 } }
#[cfg(feature = "strong_units")]
impl From<u16> for Bps { #[inline(always)] fn from(v: u16) -> Self { Bps(v) } }
#[cfg(feature = "strong_units")]
impl From<Bps> for u16 { #[inline(always)] fn from(v: Bps) -> Self { v.0 } }
#[cfg(feature = "strong_units")]
impl core::ops::Add for Lamports {
    type Output = Lamports;
    #[inline(always)] fn add(self, rhs: Lamports) -> Lamports { Lamports(self.0.saturating_add(rhs.0)) }
}
#[cfg(feature = "strong_units")]
impl core::ops::Sub for Lamports {
    type Output = Lamports;
    #[inline(always)] fn sub(self, rhs: Lamports) -> Lamports { Lamports(self.0.saturating_sub(rhs.0)) }
}

#[inline(always)]
pub const fn bps_to_ratio(bps: Bps) -> f64 {
    #[cfg(feature = "strong_units")]
    { (bps.get() as f64) / 10_000.0 }
    #[cfg(not(feature = "strong_units"))]
    { (bps as f64) / 10_000.0 }
}

#[inline(always)]
pub fn lamports_mul_ratio(base: Lamports, r: f64) -> Lamports {
    let rr = if r.is_finite() { r.max(0.0) } else { 0.0 };
    #[cfg(feature = "strong_units")]
    { Lamports(f64_to_u64_sat((base.get() as f64) * rr)) }
    #[cfg(not(feature = "strong_units"))]
    { f64_to_u64_sat((base as f64) * rr) }
}

// === [IMPROVED v11: PIECE S54 — saturating lamport math helpers] ===
pub struct LamportMath;
impl LamportMath {
    #[inline(always)]
    pub fn add(a: u64, b: u64) -> u64 { a.saturating_add(b) }
    #[inline(always)]
    pub fn sub(a: u64, b: u64) -> u64 { a.saturating_sub(b) }
    /// (a * b) / c with saturation; c==0 -> 0
    #[inline(always)]
    pub fn mul_div_sat(a: u64, b: u64, c: u64) -> u64 {
        if c == 0 { return 0; }
        let prod = (a as u128).saturating_mul(b as u128);
        let q = prod / (c as u128);
        if q > (u64::MAX as u128) { u64::MAX } else { q as u64 }
    }
}

// === [IMPROVED v17: PIECE G9 — property tests+++++] =========================
#[cfg(test)]
mod tests_v17 {
    use super::*;
    use proptest::prelude::*;

    proptest!{
        #[test]
        fn cdf_monotone(tip in 5_000u64..200_000u64) {
            let hist = InclusionHistogram::new((1..=10).map(|i| i*10_000).collect(), 0.05);
            let p0 = hist.p_inclusion(SlotPhase::Mid, tip);
            let p1 = hist.p_inclusion(SlotPhase::Mid, tip.saturating_add(1_000));
            prop_assert!(p1 + 1e-12 >= p0);
        }

        #[test]
        fn ob_vwap_nonnegative(sz in 1_000u64..5_000_000u64) {
            let asks = vec![
                Order{ price: 100.0, size: 100_000.0, ..Default::default() },
                Order{ price: 101.0, size: 200_000.0, ..Default::default() },
                Order{ price: 103.0, size: 300_000.0, ..Default::default() },
            ];
            let bids = vec![
                Order{ price:  99.0, size: 100_000.0, ..Default::default() },
                Order{ price:  98.0, size: 200_000.0, ..Default::default() },
                Order{ price:  97.0, size: 300_000.0, ..Default::default() },
            ];
            let mut snap = OrderBookSnapshot{ bids, asks, timestamp: 0, mid_price: 100.0, spread: 200.0, asks_ps: None, bids_ps: None };
            let mut m = ImpactModel::default();
            m.update_venue_ob(snap.clone());
            let ib = m.estimate_ob_impact(sz, true);
            let is_ = m.estimate_ob_impact(sz, false);
            prop_assert!(ib.is_finite() && ib >= 0.0);
            prop_assert!(is_.is_finite() && is_ >= 0.0);

            // VWAP identity check at exact bucket boundaries
            snap.asks_ps = Some(super::PackedSide::from_levels(&snap.asks));
            let ps = snap.asks_ps.as_ref().unwrap();
            if let (Some(&q), Some(&v)) = (ps.cq.last(), ps.cv.last()) {
                if q > 0.0 {
                    let vwap_all = ps.vwap_for(q);
                    prop_assert!((vwap_all - v / q).abs() < 1e-9);
                }
            }
        }

        #[test]
        fn cpmm_positive(a in 1_000u64..10_000_000, b in 1_000u64..10_000_000, fee in 0u16..50_00, ain in 1_000u64..50_000_000) {
            let mut m = ImpactModel::default();
            m.update_cpmm_pool(CpmmPool{ reserves_a: a, reserves_b: b, fee_bps: fee, last_updated: 0 });
            let imp_b = m.estimate_cpmm_impact(ain, true);
            let imp_s = m.estimate_cpmm_impact(ain, false);
            prop_assert!(imp_b.is_finite() && imp_b >= 0.0);
            prop_assert!(imp_s.is_finite() && imp_s >= 0.0);
        }

        #[test]
        fn stableswap_finite(a in 500u64..5_000_000, b in 500u64..5_000_000, amp in 10u64..10_000, fee in 0u16..50_00, ain in 1_000u64..2_000_000) {
            let mut m = ImpactModel::default();
            m.update_stableswap_pool(StableSwapPool{ reserves: vec![a,b], a: amp, fee_bps: fee, admin_fee_bps: 0, last_updated: 0 });
            let imp_b = m.estimate_stableswap_impact(ain, true);
            let imp_s = m.estimate_stableswap_impact(ain, false);
            prop_assert!(imp_b.is_finite() && imp_b >= 0.0);
            prop_assert!(imp_s.is_finite() && imp_s >= 0.0);
        }
    }
}

// (lock alias now provided via cfg use above)

// === [IMPROVED v11: PIECE S49 — relay-aware pressure] ===
#[derive(Clone, Copy, Debug)]
pub struct RelayStats { pub ewma_queue_ms: f64, pub ewma_success: f64 }
impl Default for RelayStats { fn default() -> Self { Self { ewma_queue_ms: 200.0, ewma_success: 0.9 } } }

#[derive(Clone, Debug)]
pub struct RelayModel { stats: [RelayStats; 8], lambda: f64 }
impl RelayModel {
    pub fn new(lambda: f64) -> Self { Self { stats: [RelayStats::default(); 8], lambda: lambda.clamp(0.8, 0.9999) } }
    #[inline(always)]
    pub fn update(&mut self, relay_id: usize, queue_ms: f64, success: bool) {
        let s = &mut self.stats[relay_id % self.stats.len()];
        let q = if queue_ms.is_finite() { queue_ms.max(0.0) } else { s.ewma_queue_ms };
        s.ewma_queue_ms = self.lambda * s.ewma_queue_ms + (1.0 - self.lambda) * q;
        let obs = if success { 1.0 } else { 0.0 };
        s.ewma_success  = self.lambda * s.ewma_success  + (1.0 - self.lambda) * obs;
    }
    /// tip pressure multiplier (1.0..2.2) by relay
    #[inline(always)]
    pub fn pressure(&self, relay_id: usize) -> f64 {
        let s = self.stats[relay_id % self.stats.len()];
        let q = (s.ewma_queue_ms / 300.0).clamp(0.0, 3.0);
        let p = (1.0 - s.ewma_success).clamp(0.0, 1.0);
        (1.0 + 0.5 * q + 0.7 * p).clamp(1.0, 2.2)
    }
}

// === [IMPROVED v11: PIECE S52 — online probability calibrator] ===
#[derive(Clone, Copy, Debug)]
pub struct ProbCalibrator { lambda: f64, m_pred: f64, m_obs: f64 }
impl ProbCalibrator {
    pub fn new(lambda: f64) -> Self { Self { lambda: lambda.clamp(0.8, 0.9999), m_pred: 0.5, m_obs: 0.5 } }
    #[inline(always)] pub fn update(&mut self, p_pred: f64, outcome: bool) {
        let p = p_pred.clamp(0.0, 1.0);
        let y = if outcome { 1.0 } else { 0.0 };
        self.m_pred = self.lambda * self.m_pred + (1.0 - self.lambda) * p;
        self.m_obs  = self.lambda * self.m_obs  + (1.0 - self.lambda) * y;
    }
    /// corrected probability (multiplicative calibration; bounded)
    #[inline(always)] pub fn apply(&self, p_pred: f64) -> f64 {
        let eps = 1e-6;
        let k = (self.m_obs + eps) / (self.m_pred + eps);
        (p_pred * k).clamp(0.0, 1.0)
    }
}

// ========================= Constants & defaults =========================

const KELLY_FRACTION: f64 = 0.25;
const MIN_POSITION_SIZE: u64 = 100_000;
const MAX_POSITION_RATIO: f64 = 0.15;
const VOLATILITY_WINDOW: usize = 120;
const CONFIDENCE_THRESHOLD: f64 = 0.68;
const SLIPPAGE_BUFFER: f64 = 0.003;
const GAS_SAFETY_MULTIPLIER: f64 = 1.5;
const DECAY_FACTOR: f64 = 0.995;
const MIN_LIQUIDITY_RATIO: f64 = 0.02;
const MAX_LEVERAGE: f64 = 3.0;
const RISK_FREE_RATE: f64 = 0.02;
const POSITION_SCALE_FACTOR: f64 = 0.8;

// Small numeric helpers used across normalization paths
#[inline(always)] fn clamp01(x: f64) -> f64 { if x.is_finite() { x.clamp(0.0, 1.0) } else { 0.0 } }
#[inline(always)] fn clean(x: f64)   -> f64 { if x.is_finite() { x } else { 0.0 } }

#[inline(always)]
pub fn f64_to_u64_sat(x: f64) -> u64 {
    if !x.is_finite() || x <= 0.0 { 0 } else if x >= u64::MAX as f64 { u64::MAX } else { x as u64 }
}
#[inline(always)]
pub fn lerp(a: f64, b: f64, t: f64) -> f64 { a.mul_add(1.0 - t, b * t) }
#[inline(always)]
pub fn hmean(a: f64, b: f64) -> f64 { let (a,b)=(a.max(1e-15), b.max(1e-15)); 2.0*a*b/(a+b) }
#[inline(always)]
pub fn sigmoid_stable(x: f64) -> f64 { if x >= 0.0 { 1.0 / (1.0 + (-x).exp()) } else { let e = x.exp(); e / (1.0 + e) } }
#[inline(always)]
pub fn logit_stable(p: f64) -> f64 { let pp = clamp01(p).max(1e-15).min(1.0 - 1e-15); (pp / (1.0 - pp)).ln() }

// Risk targets
const TARGET_PORTFOLIO_VOL: f64 = 0.10; // annualized
const ES95_BUDGET: f64 = 0.02; // 2% of capital (session)
const ES99_BUDGET: f64 = 0.05; // 5% of capital (session)

// ========================= Data types =========================

/// Represents different types of market venues
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueModel {
    /// Constant product AMM (e.g., Uniswap)
    Cpmm,
    /// Curve/stable invariant AMM
    StableSwap,
    /// Central limit order book or L2 order book
    OrderBook,
}

// === [IMPROVED v10: PIECE S46 — property tests] =============================
#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    // Helper to build a normalized MarketConditions instance
    fn mk_market(base: u64, cong: f64, vol: f64) -> MarketConditions {
        MarketConditions {
            volatility: vol,
            liquidity_depth: 1_000_000,
            spread_bps: 10.0,
            priority_fee: base,
            network_congestion: cong,
            oracle_confidence: 0.9,
            recent_slippage: 0.005,
            venue: VenueModel::OrderBook,
            slot_phase: SlotPhase::Mid,
        }.normalized()
    }

    proptest! {
        #[test]
        fn chosen_tip_bounded(base in 1_000u64..200_000u64, cong in 0.0f64..1.0, vol in 0.0f64..0.3) {
            let fpo = FractionalPositionOptimizer::new(1_000_000_000);
            let mut model = ImpactModel::default();
            let mkt = mk_market(base, cong, vol);
            let tip = fpo.calculate_max_gas_inclusion_aware(1_000_000, &mkt, &model, 1.0);
            // Bounds: adjust if your production bounds differ
            prop_assert!(tip >= 5_000 && tip <= 500_000);
        }

        #[test]
        fn cdf_monotone(tip in 5_000u64..200_000u64) {
            let hist = InclusionHistogram::new((1..=10).map(|i| i*10_000).collect(), 0.05);
            // Fresh hist has zeros; monotone trivially. We still check local monotonicity
            let p0 = hist.p_inclusion(SlotPhase::Mid, tip);
            let p1 = hist.p_inclusion(SlotPhase::Mid, tip.saturating_add(1_000));
            prop_assert!(p1 + 1e-12 >= p0);
        }
    }
}

/// Represents the phase within a Solana slot
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotPhase {
    /// First ~200ms of the slot (highest priority)
    Early,
    /// Middle of the slot window (medium priority)
    Mid,
    /// Last ~200ms of the slot (lowest priority)
    Late,
}

// === [IMPROVED v12: PIECE B6 — canonical MarketConditions] ===
#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "align64", repr(align(64)))]
pub struct MarketConditions {
    pub volatility: f64,          // per-period (>=0)
    pub liquidity_depth: u64,     // quote units
    pub spread_bps: f64,          // >=0
    pub priority_fee: u64,        // lamports
    pub network_congestion: f64,  // 0..1
    pub oracle_confidence: f64,   // 0..1
    pub recent_slippage: f64,     // fraction (>=0)
    pub venue: VenueModel,        // venue kind
    pub slot_phase: SlotPhase,    // timing
}

impl Default for MarketConditions {
    fn default() -> Self {
        Self {
            volatility: 0.02,
            liquidity_depth: 1_000_000,
            spread_bps: 5.0,
            priority_fee: 10_000,
            network_congestion: 0.2,
            oracle_confidence: 0.95,
            recent_slippage: 0.001,
            venue: VenueModel::OrderBook,
            slot_phase: SlotPhase::Mid,
        }
    }
}

impl MarketConditions {
    #[must_use]
    #[inline(always)]
    pub fn normalized(mut self) -> Self {
        self.volatility         = clean(self.volatility.max(0.0));
        self.spread_bps         = clean(self.spread_bps.max(0.0));
        self.network_congestion = clamp01(self.network_congestion);
        self.oracle_confidence  = clamp01(self.oracle_confidence);
        self.recent_slippage    = clean(self.recent_slippage.max(0.0));
        self
    }
}

// === [IMPROVED v14: PIECE D9 — property tests++++] ==========================
#[cfg(test)]
mod tests_v14 {
    use super::*;
    use proptest::prelude::*;

    proptest!{
        #[test]
        fn cdf_monotone(tip in 5_000u64..200_000u64) {
            let hist = InclusionHistogram::new((1..=10).map(|i| i*10_000).collect(), 0.05);
            let p0 = hist.p_inclusion(SlotPhase::Mid, tip);
            let p1 = hist.p_inclusion(SlotPhase::Mid, tip.saturating_add(1_000));
            prop_assert!(p1 + 1e-12 >= p0);
        }

        #[test]
        fn ob_vwap_nonnegative(sz in 1_000u64..5_000_000u64) {
            let asks = vec![
                Order{ price: 100.0, size: 100_000.0, ..Default::default() },
                Order{ price: 101.0, size: 200_000.0, ..Default::default() },
                Order{ price: 103.0, size: 300_000.0, ..Default::default() },
            ];
            let bids = vec![
                Order{ price: 99.0, size: 100_000.0, ..Default::default() },
                Order{ price: 98.0, size: 200_000.0, ..Default::default() },
                Order{ price: 97.0, size: 300_000.0, ..Default::default() },
            ];
            let snap = OrderBookSnapshot{
                bids, asks, timestamp: 0, mid_price: 100.0, spread: 200.0,
            };
            let mut m = ImpactModel::default();
            m.update_venue(VenueModel::OrderBook, Some(snap));
            let ib = m.estimate_ob_impact(sz, true);
            let is_ = m.estimate_ob_impact(sz, false);
            prop_assert!(ib.is_finite() && ib >= 0.0);
            prop_assert!(is_.is_finite() && is_ >= 0.0);
        }

        #[test]
        fn cpmm_positive(a in 1_000u64..10_000_000, b in 1_000u64..10_000_000, fee in 0u16..50_00, ain in 1_000u64..50_000_000) {
            let mut m = ImpactModel::default();
            m.update_cpmm_pool(CpmmPool{ reserves_a: a, reserves_b: b, fee_bps: fee, last_updated: 0 });
            let imp_b = m.estimate_cpmm_impact(ain, true);
            let imp_s = m.estimate_cpmm_impact(ain, false);
            prop_assert!(imp_b.is_finite() && imp_b >= 0.0);
            prop_assert!(imp_s.is_finite() && imp_s >= 0.0);
        }
    }
}

/// Represents an order in the order book
#[derive(Debug, Clone)]
pub struct Order {
    /// Price of the order
    pub price: f64,
    /// Size/amount of the order
    pub size: f64,
    /// Order ID or identifier
    pub order_id: Option<String>,
    /// Timestamp of when the order was placed
    pub timestamp: i64,
    /// Whether this is a bid (buy) or ask (sell) order
    pub side: TradeSide,
}

impl Default for Order {
    fn default() -> Self {
        Self {
            price: 0.0,
            size: 0.0,
            order_id: None,
            timestamp: 0,
            side: TradeSide::Buy,
        }
    }
}

// (removed old MarketConditions definition)

// === [IMPROVED v17: PIECE G8 — OB prefix sums] ==============================
/// Prefix-summed side for O(log N) bracketing + O(1) VWAP combine
#[derive(Debug, Clone)]
struct PackedSide {
    px: Vec<f64>,     // level prices
    sz: Vec<f64>,     // level sizes
    cq: Vec<f64>,     // cumulative qty
    cv: Vec<f64>,     // cumulative value (sum(size*price))
}
impl PackedSide {
    #[inline(always)]
    fn from_levels(levels: &Vec<Order>) -> Self {
        let n = levels.len();
        let mut px = Vec::with_capacity(n);
        let mut sz = Vec::with_capacity(n);
        let mut cq = Vec::with_capacity(n);
        let mut cv = Vec::with_capacity(n);
        let mut q = 0.0;
        let mut v = 0.0;
        for l in levels.iter() {
            let s = l.size.max(0.0);
            let p = l.price.max(0.0);
            q += s;
            // Correct FMA: v += s * p
            v = v.mul_add(1.0, s * p); // == v + s*p
            px.push(p);
            sz.push(s);
            cq.push(q);
            cv.push(v);
        }
        Self { px, sz, cq, cv }
    }
    #[inline(always)]
    fn vwap_for(&self, qty: f64) -> f64 {
        if qty <= 0.0 { return 0.0; }
        let total = *self.cq.last().unwrap_or(&0.0);
        let capped = qty.min(total).max(0.0);

        // binary search first level to hit capped
        let mut lo = 0usize;
        let mut hi = self.cq.len().saturating_sub(1);
        while lo < hi {
            let mid = (lo + hi) >> 1;
            if self.cq[mid] >= capped { hi = mid; } else { lo = mid + 1; }
        }
        let i = lo;
        let prev_q = if i == 0 { 0.0 } else { self.cq[i-1] };
        let prev_v = if i == 0 { 0.0 } else { self.cv[i-1] };
        let take_level = (capped - prev_q).max(0.0).min(self.sz[i]);
        let total_v = prev_v + take_level * self.px[i];
        let total_q = prev_q + take_level;
        if total_q <= 0.0 { 0.0 } else { clean(total_v / total_q) }
    }
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct PositionMetrics {
    pub win_rate: f64,
    pub avg_profit: f64,
    pub avg_loss: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub current_streak: i32,
    pub total_trades: u64,
    pub last_update: i64,
}

// (removed duplicate MarketConditions definition)

#[derive(Debug, Clone)]
pub struct OptimizedPosition {
    pub size: u64,
    pub leverage: f64,
    pub stop_loss: f64,
    pub take_profit: f64,
    pub confidence: f64,
    pub expected_value: f64,
    pub risk_adjusted_size: u64,
    pub max_gas_price: u64,
}

#[derive(Debug, Clone)]
struct Snapshot {
    capital: u64,
    metrics: PositionMetrics,
    recent_trades: Vec<TradeResult>,
    ewma_vol: f64,
    cfg: Config,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub max_position_pct: f64,
    pub vol_adjustment_factor: f64,
    pub streak_penalty: f64,
    pub congestion_discount: f64,
    pub confidence_weight: f64,
    pub regime_trending_boost: f64,
    pub regime_ranging_cut: f64,
    pub regime_volatile_cut: f64,
    pub regime_illiquid_cut: f64,
    pub min_trades_for_stats: usize,
    pub min_trades_for_kelly: usize,
    pub es_conf_levels: (f64, f64), // (0.95, 0.99)
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // Position sizing
            max_position_pct: MAX_POSITION_RATIO,
            vol_adjustment_factor: 2.0,
            
            // Performance adjustments
            streak_penalty: 0.30,  // Increased from 0.05
            congestion_discount: 0.30,
            confidence_weight: 1.5,
            
            // Market regime adjustments
            regime_trending_boost: 1.20,
            regime_ranging_cut: 0.80,
            regime_volatile_cut: 0.60,
            regime_illiquid_cut: 0.40,
            
            // Statistical thresholds
            min_trades_for_stats: 30,
            min_trades_for_kelly: 20,
            
            // Risk management
            es_conf_levels: (0.95, 0.99),  // ES95 and ES99 confidence levels
        }
    }
}

/// Tracks empirical inclusion probabilities for different tip levels and slot phases
#[derive(Debug)]
struct InclusionHistogram {
    // Tip buckets in native units; monotonically increasing
    tip_edges: Vec<u64>,
    // For each phase, empirical CDF (P(inclusion | tip in bucket))
    cdf_early: Vec<f64>,
    cdf_mid:   Vec<f64>,
    cdf_late:  Vec<f64>,
    // EWMA smoothing alpha
    alpha: f64,
    // tiny per-phase caches: packed (tip_low32 << 32 | idx_low32)
    cache_e: AtomicU64,
    cache_m: AtomicU64,
    cache_l: AtomicU64,
}

impl Clone for InclusionHistogram {
    fn clone(&self) -> Self {
        Self {
            tip_edges: self.tip_edges.clone(),
            cdf_early: self.cdf_early.clone(),
            cdf_mid:   self.cdf_mid.clone(),
            cdf_late:  self.cdf_late.clone(),
            alpha: self.alpha,
            cache_e: AtomicU64::new(0),
            cache_m: AtomicU64::new(0),
            cache_l: AtomicU64::new(0),
        }
    }
}

impl InclusionHistogram {
    #[inline(always)]
    fn new(tip_edges: Vec<u64>, alpha: f64) -> Self {
        let n = tip_edges.len().max(1);
        Self {
            tip_edges,
            cdf_early: vec![0.0; n],
            cdf_mid:   vec![0.0; n],
            cdf_late:  vec![0.0; n],
            alpha,
            cache_e: AtomicU64::new(0),
            cache_m: AtomicU64::new(0),
            cache_l: AtomicU64::new(0),
        }
    }

    #[inline(always)] fn assert_well_formed(&self) {
        debug_assert_eq!(self.tip_edges.len(), self.cdf_early.len());
        debug_assert_eq!(self.cdf_early.len(), self.cdf_mid.len());
        debug_assert_eq!(self.cdf_mid.len(),   self.cdf_late.len());
    }

    #[inline(always)] fn pack_cache_key(tip: u64, idx: usize) -> u64 {
        ((tip as u64 & 0xFFFF_FFFF) << 32) | ((idx as u64) & 0xFFFF_FFFF)
    }
    #[inline(always)] fn unpack_idx(packed: u64) -> usize { (packed & 0xFFFF_FFFF) as usize }

    #[inline(always)]
    fn bucket_slow(&self, tip: u64) -> usize {
        match self.tip_edges.binary_search(&tip) {
            Ok(i) => i,
            Err(i) => i.saturating_sub(1).min(self.tip_edges.len().saturating_sub(1)),
        }
    }
    #[inline(always)]
    fn cache_load(&self, phase: SlotPhase) -> u64 {
        match phase {
            SlotPhase::Early => self.cache_e.load(AOrd::Relaxed),
            SlotPhase::Mid   => self.cache_m.load(AOrd::Relaxed),
            SlotPhase::Late  => self.cache_l.load(AOrd::Relaxed),
        }
    }
    #[inline(always)]
    fn cache_store(&self, phase: SlotPhase, tip: u64, idx: usize) {
        let v = Self::pack_cache_key(tip, idx);
        match phase {
            SlotPhase::Early => self.cache_e.store(v, AOrd::Relaxed),
            SlotPhase::Mid   => self.cache_m.store(v, AOrd::Relaxed),
            SlotPhase::Late  => self.cache_l.store(v, AOrd::Relaxed),
        }
    }

    /// Floor bucket (fast path via per-phase cache)
    #[inline(always)]
    fn bucket(&self, phase: SlotPhase, tip: u64) -> usize {
        let c = self.cache_load(phase);
        let tip_low = (c >> 32) & 0xFFFF_FFFF;
        if tip_low == (tip & 0xFFFF_FFFF) { return Self::unpack_idx(c); }
        let idx = self.bucket_slow(tip);
        self.cache_store(phase, tip, idx);
        idx
    }

    #[inline(always)]
    fn enforce_monotonic_from(xs: &mut [f64], start: usize) {
        let mut run_max = if start == 0 { 0.0 } else { xs[start - 1].max(0.0).min(1.0) };
        for x in xs.iter_mut().skip(start) {
            if !x.is_finite() { *x = 0.0; }
            if *x < run_max { *x = run_max; } else { run_max = (*x).min(1.0); }
            if *x > 1.0 { *x = 1.0; }
        }
    }

    /// Updates the histogram with a new observation
    #[inline(always)]
    fn update(&mut self, phase: SlotPhase, tip: u64, included: bool) {
        self.assert_well_formed();
        if self.tip_edges.is_empty() { return; }
        let idx = self.bucket(phase, tip);
        let obs = if included { 1.0 } else { 0.0 };
        let a = self.alpha;

        let cdf = match phase {
            SlotPhase::Early => &mut self.cdf_early,
            SlotPhase::Mid   => &mut self.cdf_mid,
            SlotPhase::Late  => &mut self.cdf_late,
        };
        cdf[idx] = clean(cdf[idx].mul_add(1.0 - a, obs * a)).clamp(0.0, 1.0);
        Self::enforce_monotonic_from(cdf, idx);
    }

    /// Gets the probability of inclusion for a given tip and phase
    #[inline(always)]
    fn p_inclusion(&self, phase: SlotPhase, tip: u64) -> f64 {
        if self.tip_edges.is_empty() { return 0.0; }
        let ys = match phase {
            SlotPhase::Early => &self.cdf_early,
            SlotPhase::Mid   => &self.cdf_mid,
            SlotPhase::Late  => &self.cdf_late,
        };

        // use cached floor bucket to avoid a fresh binary_search every time
        let li = self.bucket(phase, tip);
        let hi = (li + 1).min(self.tip_edges.len() - 1);
        if li == hi { return clamp01(clean(ys[li])); }

        // Hyman monotone cubic on [li,hi]
        let (x0, x1) = (self.tip_edges[li] as f64, self.tip_edges[hi] as f64);
        let (y0, y1) = (ys[li], ys[hi]);
        let h  = (x1 - x0).abs().max(1e-9);
        let t  = ((tip as f64 - x0) / h).clamp(0.0, 1.0);

        let dli= if li>0 { (ys[li]-ys[li-1]) / ((self.tip_edges[li] as f64 - self.tip_edges[li-1] as f64).abs().max(1e-9)) } else { (y1-y0)/h };
        let dhi= if hi+1<ys.len() { (ys[hi+1]-ys[hi]) / ((self.tip_edges[hi+1] as f64 - self.tip_edges[hi] as f64).abs().max(1e-9)) } else { (y1-y0)/h };
        fn hyman(m_prev: f64, m_curr: f64, di: f64) -> f64 {
            if di == 0.0 || m_prev == 0.0 || m_curr == 0.0 || (di.signum()!=m_prev.signum()) || (di.signum()!=m_curr.signum()) { 0.0 }
            else {
                let a=m_prev.abs(); let b=m_curr.abs(); let c=di.abs();
                let bound = 3.0 * c.min(a.min(b));
                di.signum() * ((a + b) / 2.0).min(bound)
            }
        }
        let di = (y1 - y0) / h;
        let m0 = hyman(dli, di,  di);
        let m1 = hyman(di,  dhi, di);
        let t2 = t*t; let t3 = t2*t;
        let y = (2.0*t3 - 3.0*t2 + 1.0)*y0 + (t3 - 2.0*t2 + t)*h*m0
              + (-2.0*t3 + 3.0*t2)*y1 + (t3 - t2)*h*m1;
        clamp01(clean(y))
    }

    // ========================= Inclusion CDF isotonic smoother ===================
    // PAVA on cold path to keep CDFs monotone and denoised.
    #[cold]
    #[inline(never)]
    pub fn enforce_isotonic(&mut self) {
        Self::pava(&mut self.cdf_early);
        Self::pava(&mut self.cdf_mid);
        Self::pava(&mut self.cdf_late);
    }

    #[cold]
    #[inline(never)]
    fn pava(cdf: &mut [f64]) {
        if cdf.len() <= 1 { return; }
        for v in cdf.iter_mut() { *v = v.clamp(0.0, 1.0); }

        #[derive(Copy, Clone)]
        struct Block { s: usize, e: usize, m: f64 }

        #[cfg(feature="smallvec")]
        type BlockVec = smallvec::SmallVec<[Block; 16]>;
        #[cfg(not(feature="smallvec"))]
        type BlockVec = Vec<Block>;

        let mut stk: BlockVec = BlockVec::new();
        for (i, &x) in cdf.iter().enumerate() {
            stk.push(Block { s: i, e: i, m: x });
            while stk.len() >= 2 {
                let b = stk.pop().unwrap();
                let a = stk.pop().unwrap();
                if a.m <= b.m { 
                    stk.push(a); stk.push(b); 
                    break; 
                }
                let la = (a.e - a.s + 1) as f64;
                let lb = (b.e - b.s + 1) as f64;
                let mean = (a.m * la + b.m * lb) / (la + lb);
                stk.push(Block { s: a.s, e: b.e, m: mean });
            }
        }
        for blk in stk.into_iter() {
            for i in blk.s..=blk.e { cdf[i] = blk.m; }
        }
        for v in cdf.iter_mut() { *v = v.clamp(0.0, 1.0); }
    }
}

/// Ensures a sequence is non-decreasing
fn enforce_monotonic(xs: &mut [f64]) {
    for i in 1..xs.len() {
        if xs[i] < xs[i-1] {
            xs[i] = xs[i-1];
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrderBookSnapshot {
    pub bids: Vec<Order>,
    pub asks: Vec<Order>,
    pub timestamp: i64,
    pub mid_price: f64,
    pub spread: f64,
    pub asks_ps: Option<PackedSide>,
    pub bids_ps: Option<PackedSide>,
}

impl Default for OrderBookSnapshot {
    fn default() -> Self {
        Self {
            bids: Vec::new(),
            asks: Vec::new(),
            timestamp: 0,
            mid_price: 0.0,
            spread: 0.0,
            asks_ps: None,
            bids_ps: None,
        }
    }
}

/// CPMM pool state snapshot
#[derive(Debug, Clone)]
pub struct CpmmPool {
    pub reserves_a: u64,
    pub reserves_b: u64,
    pub fee_bps: u16,
    pub last_updated: i64,
}

/// StableSwap pool state snapshot
#[derive(Debug, Clone)]
pub struct StableSwapPool {
    pub reserves: Vec<u64>,
    pub a: u64, // Amplification coefficient
    pub fee_bps: u16,
    pub admin_fee_bps: u16,
    pub last_updated: i64,
}

/// Models the market impact and inclusion probability of transactions
#[derive(Debug, Clone)]
pub struct ImpactModel {
    // Empirical inclusion probability histogram
    pub inclusion_hist: InclusionHistogram,
    
    // Fallback parameters for sigmoid inclusion probability
    pub incl_sigmoid_k: f64,
    pub incl_sigmoid_mid: f64,
    
    // Impact model parameters
    pub impact_alpha: f64,
    
    // Venue impact context
    pub venue: VenueModel,
    
    // Optional snapshots for different venue models
    pub ob_snapshot: Option<OrderBookSnapshot>,
    pub cpmm: Option<CpmmPool>,
    pub stableswap: Option<StableSwapPool>,
}

impl Default for ImpactModel {
    fn default() -> Self {
        // Define tip buckets from 1k to 100k lamports (10 buckets)
        let tip_edges: Vec<u64> = (1..=10).map(|i| i * 10_000).collect();
        
        Self {
            inclusion_hist: InclusionHistogram::new(tip_edges, 0.05),
            incl_sigmoid_k: 0.0001,
            incl_sigmoid_mid: 5000.0, // 5k lamports
            impact_alpha: 0.1,
            venue: VenueModel::OrderBook, // Default to order book
            ob_snapshot: None,
            cpmm: None,
            stableswap: None,
        }
    }
    
    /// Records an inclusion observation for the given tip and phase
    pub fn record_inclusion(&mut self, phase: SlotPhase, tip: u64, included: bool) {
        self.inclusion_hist.update(phase, tip, included);
    }
    
    /// Gets the probability of inclusion for a given tip and phase
    pub fn get_inclusion_probability(&self, phase: SlotPhase, tip: u64) -> f64 {
        self.inclusion_hist.p_inclusion(phase, tip)
    }
    
    /// Updates the venue model and associated snapshot
    pub fn update_venue(&mut self, venue: VenueModel, snapshot: Option<OrderBookSnapshot>) {
        self.venue = venue;
        self.ob_snapshot = snapshot;
        // Clear other snapshots when changing venue type
        self.cpmm = None;
        self.stableswap = None;
    }
    
    /// Updates the CPMM pool state
    pub fn update_cpmm_pool(&mut self, pool: CpmmPool) {
        self.venue = VenueModel::Cpmm;
        self.cpmm = Some(pool);
        // Clear other snapshots
        self.ob_snapshot = None;
        self.stableswap = None;
    }
    
    /// Updates the StableSwap pool state
    pub fn update_stableswap_pool(&mut self, pool: StableSwapPool) {
        self.venue = VenueModel::StableSwap;
        self.stableswap = Some(pool);
        // Clear other snapshots
        self.ob_snapshot = None;
        self.cpmm = None;
    }
    
    /// Estimates the price impact of a trade of the given size
    pub fn estimate_impact(&self, size: u64, is_buy: bool) -> f64 {
        match self.venue {
            VenueModel::OrderBook => {
                self.estimate_ob_impact(size, is_buy)
            }
            VenueModel::Cpmm => {
                self.estimate_cpmm_impact(size, is_buy)
            }
            VenueModel::StableSwap => {
                self.estimate_stableswap_impact(size, is_buy)
            }
        }
    }
    
    /// Estimates impact for order book venues (prefix-sum accelerated)
    fn estimate_ob_impact(&self, size: u64, is_buy: bool) -> f64 {
        let s = match &self.ob_snapshot { Some(s) => s, None => return 0.0 };
        if size == 0 { return 0.0; }
        let qty = size as f64;

        let vwap = if is_buy {
            if let Some(ps) = &s.asks_ps { ps.vwap_for(qty) } else {
                let mut rem=qty; let mut v=0.0; let mut q=0.0;
                for l in &s.asks { if rem<=0.0 { break; } let take=rem.min(l.size.max(0.0)); v+=take*l.price; q+=take; rem-=take; }
                if q<=0.0 { 0.0 } else { clean(v / q) }
            }
        } else {
            if let Some(ps) = &s.bids_ps { ps.vwap_for(qty) } else {
                let mut rem=qty; let mut v=0.0; let mut q=0.0;
                for l in &s.bids { if rem<=0.0 { break; } let take=rem.min(l.size.max(0.0)); v+=take*l.price; q+=take; rem-=take; }
                if q<=0.0 { 0.0 } else { clean(v / q) }
            }
        };
        if vwap <= 0.0 { return 0.0; }
        let mid = clean(s.mid_price.max(1e-9));
        let bps = if is_buy { (vwap / mid - 1.0) } else { (1.0 - vwap / mid) } * 10_000.0;
        clean(bps.max(0.0))
    }
    
    /// Estimates impact for CPMM venues
    fn estimate_cpmm_impact(&self, size: u64, is_buy: bool) -> f64 {
        let pool = match &self.cpmm {
            Some(p) => p,
            None => return 0.0,
        };
        
        let (reserve_in, reserve_out) = if is_buy {
            (pool.reserves_b, pool.reserves_a)
        } else {
            (pool.reserves_a, pool.reserves_b)
        };
        
        let fee_factor = 1.0 - (pool.fee_bps as f64 / 10_000.0);
        let amount_in = size as f64 * fee_factor;
        
        // Constant product formula: x * y = k
        let amount_out = (reserve_out as f64 * amount_in) / (reserve_in as f64 + amount_in);
        let price = amount_in / amount_out;
        
        // Calculate price impact as basis points from 1:1
        if is_buy {
            (price - 1.0) * 10_000.0
        } else {
            (1.0 - 1.0 / price) * 10_000.0
        }
    }
    
    /// Estimates impact for StableSwap venues using Newton iteration on invariant
    fn estimate_stableswap_impact(&self, amount_in: u64, is_buy: bool) -> f64 {
        let p = match &self.stableswap { Some(p) => p, None => return 0.0 };
        if amount_in == 0 || p.reserves.len() < 2 { return 0.0; }
        let n = p.reserves.len() as f64; let a = (p.a as f64).max(1.0);
        let mut x = p.reserves[0] as f64; let mut y = p.reserves[1] as f64;
        let fee = 1.0 - (p.fee_bps as f64 / 10_000.0);
        if is_buy { std::mem::swap(&mut x, &mut y); }

        // Invariant D via iteration
        let d = {
            let mut s=0.0; for r in &p.reserves { s += *r as f64; }
            if s<=0.0 { return 0.0; }
            let mut d_prev; let mut d = s;
            for _ in 0..64 {
                let mut d_p = d;
                for r in &p.reserves { d_p = d_p * d / (*r as f64 * n); }
                d_prev = d;
                let num = (a*n*s + d_p*n) * d;
                let den = (a*n - 1.0)*d + (n + 1.0)*d_p;
                d = clean(num / den);
                if (d - d_prev).abs() <= 1e-7 { break; }
            }
            d
        };

        let ain = (amount_in as f64) * fee;
        let x_new = x + ain;
        let mut y_new = y;
        for _ in 0..64 {
            let c = d * d / (x_new * n * a) * d / n;
            let b = x_new + d / a;
            let y_prev = y_new;
            let den = (2.0*y_new + b - d);
            if den.abs() <= 1e-15 { break; } // guard rare degenerate
            y_new = (y_new*y_new + c) / den;
            if (y_new - y_prev).abs() <= 1e-9 { break; }
        }
        let aout = (y - y_new).max(0.0);
        if aout <= 0.0 { return 0.0; }
        let px = ain / aout;
        let bps = if is_buy { (px - 1.0) } else { (1.0 - 1.0/px) } * 10_000.0;
        clean(bps.max(0.0))
    }

    /// Update venue with OB snapshot and precompute prefix sums
    pub fn update_venue_ob(&mut self, mut snapshot: OrderBookSnapshot) {
        snapshot.asks_ps = Some(PackedSide::from_levels(&snapshot.asks));
        snapshot.bids_ps = Some(PackedSide::from_levels(&snapshot.bids));
        self.venue = VenueModel::OrderBook;
        self.ob_snapshot = Some(snapshot);
        self.cpmm = None;
        self.stableswap = None;
    }
}

#[derive(Debug, Clone)]
struct RiskParameters {
    max_position_pct: f64,
    vol_adjustment_factor: f64,
    streak_penalty: f64,
    congestion_discount: f64,
    confidence_weight: f64,
}

#[derive(Debug, Clone)]
struct TradeResult {
    pnl: f64,
    size: u64,
    timestamp: i64,
    slippage: f64,
    gas_cost: u64,
}

#[derive(Debug, Clone)]
struct PerformanceTracker {
    rolling_sharpe: f64,
    rolling_sortino: f64,
    calmar_ratio: f64,
    omega_ratio: f64,
    tail_ratio: f64,
    es95: f64,
    es99: f64,
}

#[derive(Debug, Clone)]
pub struct PerformanceStats {
    pub sharpe: f64,
    pub sortino: f64,
    pub calmar: f64,
    pub omega: f64,
    pub tail: f64,
    pub total_trades: u64,
    pub win_rate: f64,
    pub avg_profit: f64,
    pub avg_loss: f64,
    pub max_drawdown: f64,
    pub es95: f64,
    pub es99: f64,
}

#[derive(Debug, Clone, Copy)]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy)]
pub enum MarketState {
    Trending,
    Ranging,
    Volatile,
    Illiquid,
}

// ========================= Optimizer =========================

pub struct FractionalPositionOptimizer {
    capital_base: Arc<AtomicU64>,
    position_metrics: Arc<PLRwLock<PositionMetrics>>,
    performance_tracker: Arc<PLRwLock<PerformanceTracker>>,
    volatility_buffer: Arc<PLRwLock<VecDeque<f64>>>,
    recent_trades: Arc<PLRwLock<VecDeque<TradeResult>>>,

    // Config / models
    cfg: Arc<PLRwLock<Config>>,
    impact_model: Arc<PLRwLock<ImpactModel>>,

    // Obs
    metrics: Arc<dyn MetricsSink>,
    tracer: Arc<dyn Tracer>,

    // === v11 additions ===
    // Relay-aware pressure model and chosen relay id
    relay_model: Arc<PLRwLock<RelayModel>>,
    chosen_relay_id: Arc<AtomicUsize>,
    // Online probability calibrator
    prob_cal: Arc<PLRwLock<ProbCalibrator>>,
}

impl Default for FractionalPositionOptimizer {
    fn default() -> Self {
        Self::new(1_000_000_000)
    }
}

impl FractionalPositionOptimizer {
    pub fn new(initial_capital: u64) -> Self {
        Self {
            capital_base: Arc::new(AtomicU64::new(initial_capital)),
            position_metrics: Arc::new(PLRwLock::new(PositionMetrics {
                win_rate: 0.5,
                avg_profit: 0.0,
                avg_loss: 0.0,
                sharpe_ratio: 0.0,
                max_drawdown: 0.0,
                current_streak: 0,
                total_trades: 0,
                last_update: 0,
            })),
            performance_tracker: Arc::new(PLRwLock::new(PerformanceTracker {
                rolling_sharpe: 0.0,
                rolling_sortino: 0.0,
                calmar_ratio: 0.0,
                omega_ratio: 1.0,
                tail_ratio: 1.0,
                es95: 0.0,
                es99: 0.0,
            })),
            volatility_buffer: Arc::new(PLRwLock::new(VecDeque::with_capacity(VOLATILITY_WINDOW))),
            recent_trades: Arc::new(PLRwLock::new(VecDeque::with_capacity(2048))),
            cfg: Arc::new(PLRwLock::new(Config::default())),
            impact_model: Arc::new(PLRwLock::new(ImpactModel::default())),
            metrics: Arc::new(NoopMetrics),
            tracer: Arc::new(NoopTracer),
            // v11
            relay_model: Arc::new(PLRwLock::new(RelayModel::new(0.98))),
            chosen_relay_id: Arc::new(AtomicUsize::new(0)),
            prob_cal: Arc::new(PLRwLock::new(ProbCalibrator::new(0.98))),
        }
    }

    // --------------------- Public API ---------------------

    // Observability: const metric keys + span helper
    const MK_POS_SIZE: MetricKey      = MetricKey::new("fpo.position.size");
    const MK_POS_LEV: MetricKey       = MetricKey::new("fpo.position.leverage");
    const MK_POS_CONF: MetricKey      = MetricKey::new("fpo.position.confidence");
    const MK_POS_EV: MetricKey        = MetricKey::new("fpo.position.ev");

    #[inline(always)]
    pub fn span(&self, name: &'static str) -> SpanTimer { SpanTimer::new(self.tracer.clone(), name) }

    pub fn calculate_optimal_position(
        &self,
        opportunity_value: u64,
        market: &MarketConditions,
        timestamp: i64,
    ) -> OptimizedPosition {
        let _span = self.span("fpo.calculate_optimal_position");
        let snap = self.snapshot();

        // Kelly base size with uncertainty shrinkage
        let kelly_size = self.calculate_kelly_position_uncertain(&snap, opportunity_value);

        // Volatility adjustment with EWMA buffer
        let vol_adjusted = self.adjust_for_volatility(kelly_size, market, snap.ewma_vol);

        // Risk constraints (VaR/ES, drawdown, max position)
        let risk_adj = self.apply_risk_constraints(vol_adjusted, &snap);

        // Liquidity & impact constraint
        let liq_adj = self.constrain_by_liquidity_and_impact(risk_adj, market);

        // Network-aware adjustment
        let net_adj = self.adjust_for_network_conditions(liq_adj, market, &snap);

        // Dynamic scaling by performance and market regime
        let final_size = self.apply_dynamic_scaling(net_adj, &snap.metrics, market, timestamp);

        // Optimal leverage under vol/drawdown constraints
        let leverage = self.calculate_optimal_leverage(&snap.metrics, market);

        // ATR-like stops
        let (stop_loss, take_profit) =
            self.calculate_stop_levels(&snap.metrics, market, final_size);

        // Confidence and EV with inclusion/gas model
        let confidence = self.calculate_position_confidence(&snap.metrics, market);
        let expected_value = self.calculate_expected_value(final_size, &snap.metrics, market);

        // Tip/priority fee budgeting under inclusion curve
        let max_gas_price = self.calculate_max_gas_inclusion_aware(
            final_size,
            market,
            &self.impact_model.read().unwrap(),
            expected_value,
        );

        let mut out = OptimizedPosition {
            size: final_size,
            leverage,
            stop_loss,
            take_profit,
            confidence,
            expected_value,
            risk_adjusted_size: final_size,
            max_gas_price,
        };
        m_gauge!(self.metrics, Self::MK_POS_SIZE, out.size as f64);
        m_gauge!(self.metrics, Self::MK_POS_LEV, out.leverage);
        m_gauge!(self.metrics, Self::MK_POS_CONF, out.confidence);
        m_gauge!(self.metrics, Self::MK_POS_EV, out.expected_value);
        out
    }

    pub fn update_trade_result(&self, result: TradeResult) {
        {
            let mut trades = self.recent_trades.write().unwrap();
            trades.push_front(result.clone());
            if trades.len() > 2048 {
                trades.pop_back();
            }
        }

        self.update_metrics_and_trackers(&result);
    }

    pub fn update_volatility(&self, abs_price_change: f64) {
        let mut vol_buffer = self.volatility_buffer.write().unwrap();
        vol_buffer.push_front(abs_price_change.abs());
        if vol_buffer.len() > VOLATILITY_WINDOW {
            vol_buffer.pop_back();
        }
    }

    pub fn update_capital(&self, new_capital: u64) { self.capital_base.store(new_capital, AOrd::Relaxed); }

    pub fn set_metrics_sink(&mut self, sink: Arc<dyn MetricsSink>) {
        self.metrics = sink;
    }

    pub fn set_tracer(&mut self, tracer: Arc<dyn Tracer>) {
        self.tracer = tracer;
    }

    // === v11 helpers ===
    #[inline(always)]
    pub fn set_relay_id(&self, id: usize) { self.chosen_relay_id.store(id, AOrd::Relaxed); }

    #[inline(always)]
    pub fn capital(&self) -> u64 { self.capital_base.load(AOrd::Relaxed) }

    #[inline(always)]
    pub fn record_calibration(&self, p_pred: f64, included: bool) {
        if let Ok(mut cal) = self.prob_cal.write() { cal.update(p_pred, included); }
    }

    pub fn get_current_metrics(&self) -> PositionMetrics {
        self.position_metrics.read().unwrap().clone()
    }

    pub fn get_performance_stats(&self) -> PerformanceStats {
        let m = self.position_metrics.read().unwrap().clone();
        let p = self.performance_tracker.read().unwrap().clone();
        PerformanceStats {
            sharpe: p.rolling_sharpe,
            sortino: p.rolling_sortino,
            calmar: p.calmar_ratio,
            omega: p.omega_ratio,
            tail: p.tail_ratio,
            total_trades: m.total_trades,
            win_rate: m.win_rate,
            avg_profit: m.avg_profit,
            avg_loss: m.avg_loss,
            max_drawdown: m.max_drawdown,
            es95: p.es95,
            es99: p.es99,
        }
    }

    #[inline(always)]
    pub fn should_trade(&self, confidence: f64, expected_value: f64) -> bool {
        // Fast negatives first
        if !(confidence >= CONFIDENCE_THRESHOLD) { return false; }
        if !(expected_value > 0.0) { return false; }
        let capital = self.capital_base.load(AOrd::Relaxed);
        if capital < MIN_POSITION_SIZE * 10 { return false; }

        let metrics = self.position_metrics.read().unwrap();
        if metrics.current_streak <= -5 {
            return false;
        }
        if metrics.max_drawdown > 0.2 && metrics.current_streak < 0 {
            return false;
        }

        let perf = self.performance_tracker.read().unwrap();
        if perf.es99 > ES99_BUDGET { return false; }
        true
    }

    pub fn emergency_stop_check(&self) -> bool {
        let metrics = self.position_metrics.read().unwrap();

        if metrics.max_drawdown > 0.30 {
            return true;
        }
        if metrics.current_streak <= -8 {
            return true;
        }
        if metrics.sharpe_ratio < -1.0 && metrics.total_trades > 50 {
            return true;
        }

        let recent_trades = self.recent_trades.read().unwrap();
        if recent_trades.len() >= 10 {
            let recent_losses = recent_trades
                .iter()
                .take(10)
                .filter(|t| t.pnl < 0.0)
                .count();
            if recent_losses >= 8 {
                return true;
            }
        }

        let perf = self.performance_tracker.read().unwrap();
        if perf.es99 > ES99_BUDGET {
            return true;
        }

        false
    }

    pub fn get_risk_adjusted_size(&self, base_size: u64, urgency: f64) -> u64 {
        let metrics = self.position_metrics.read().unwrap();
        let capital = self.capital_base.load(AOrd::Relaxed);

        let urgency_factor = urgency.clamp(0.5, 2.0);
        let confidence_factor = if metrics.total_trades < 50 { 0.5 } else { 1.0 };

        let streak_factor = match metrics.current_streak {
            s if s >= 3 => 1.1,
            s if s <= -3 => 0.7,
            _ => 1.0,
        };

        let sharpe_factor = if metrics.sharpe_ratio > 1.5 {
            1.1
        } else if metrics.sharpe_ratio < 0.5 {
            0.8
        } else {
            1.0
        };

        let adjusted =
            (base_size as f64 * urgency_factor * confidence_factor * streak_factor * sharpe_factor)
                as u64;
        adjusted
            .min((capital as f64 * MAX_POSITION_RATIO) as u64)
            .max(MIN_POSITION_SIZE)
    }

    pub fn calculate_adaptive_stops(
        &self,
        entry_price: f64,
        _position_size: u64,
        side: TradeSide,
    ) -> (f64, f64) {
        let metrics = self.position_metrics.read().unwrap();
        let vol_buffer = self.volatility_buffer.read().unwrap();

        let current_vol = if vol_buffer.len() >= 10 {
            vol_buffer.iter().take(10).sum::<f64>() / 10.0
        } else {
            0.02
        };

        let base_stop_distance = current_vol * 2.0;
        let streak_adjustment = if metrics.current_streak < -2 {
            0.7
        } else if metrics.current_streak > 2 {
            1.3
        } else {
            1.0
        };

        let stop_distance = base_stop_distance * streak_adjustment;
        let profit_distance = stop_distance * 2.5;

        match side {
            TradeSide::Buy => (
                entry_price * (1.0 - stop_distance),
                entry_price * (1.0 + profit_distance),
            ),
            TradeSide::Sell => (
                entry_price * (1.0 + stop_distance),
                entry_price * (1.0 - profit_distance),
            ),
        }
    }

    pub fn optimize_for_market_conditions(
        &self,
        base_position: OptimizedPosition,
        market_state: MarketState,
    ) -> OptimizedPosition {
        let mut optimized = base_position;

        match market_state {
            MarketState::Trending => {
                optimized.size = (optimized.size as f64 * 1.2) as u64;
                optimized.leverage = (optimized.leverage * 1.1).min(MAX_LEVERAGE);
                optimized.take_profit *= 1.5;
            }
            MarketState::Ranging => {
                optimized.size = (optimized.size as f64 * 0.8) as u64;
                optimized.leverage = (optimized.leverage * 0.9).max(1.0);
                optimized.stop_loss *= 0.8;
                optimized.take_profit *= 0.7;
            }
            MarketState::Volatile => {
                optimized.size = (optimized.size as f64 * 0.6) as u64;
                optimized.leverage = 1.0;
                optimized.stop_loss *= 1.5;
                optimized.max_gas_price = (optimized.max_gas_price as f64 * 1.5) as u64;
            }
            MarketState::Illiquid => {
                optimized.size = (optimized.size as f64 * 0.4) as u64;
                optimized.leverage = 1.0;
                optimized.confidence *= 0.7;
            }
        }

        optimized
    }

    pub fn calculate_portfolio_heat(&self) -> f64 {
        let capital = *self.capital_base.read().unwrap();
        let metrics = self.position_metrics.read().unwrap();
        let recent_trades = self.recent_trades.read().unwrap();

        let open_risk = recent_trades
            .iter()
            .take(5)
            .map(|t| t.size as f64 / capital as f64)
            .sum::<f64>();

        let drawdown_heat = (metrics.max_drawdown * 2.0).clamp(0.0, 1.0);
        let streak_heat = if metrics.current_streak < 0 {
            (metrics.current_streak.abs() as f64 * 0.05).clamp(0.0, 1.0)
        } else {
            0.0
        };

        (open_risk + drawdown_heat + streak_heat).min(1.0)
    }

    pub fn adjust_for_correlation(&self, position: OptimizedPosition, correlation: f64) -> OptimizedPosition {
        let mut adjusted = position;
        let correlation_factor = 1.0 - correlation.abs() * 0.5;

        adjusted.size = (adjusted.size as f64 * correlation_factor) as u64;
        adjusted.confidence *= correlation_factor;

        if correlation.abs() > 0.7 {
            adjusted.leverage = (adjusted.leverage * 0.8).max(1.0);
        }

        adjusted
    }

    pub fn calculate_time_decay_factor(&self, opportunity_age_ms: u64) -> f64 {
        let age_seconds = opportunity_age_ms as f64 / 1000.0;

        if age_seconds < 0.1 {
            1.0
        } else if age_seconds < 0.5 {
            0.95
        } else if age_seconds < 1.0 {
            0.8
        } else if age_seconds < 2.0 {
            0.6
        } else {
            0.3
        }
    }

    pub fn finalize_position(&self, mut position: OptimizedPosition) -> OptimizedPosition {
        let capital = *self.capital_base.read().unwrap();
        let metrics = self.position_metrics.read().unwrap();

        position.size = position.size.min((capital as f64 * 0.95) as u64);
        position.size = ((position.size / 1000) * 1000).max(MIN_POSITION_SIZE);

        position.stop_loss = (position.stop_loss * 10000.0).round() / 10000.0;
        position.take_profit = (position.take_profit * 10000.0).round() / 10000.0;

        if metrics.total_trades < 20 {
            position.size = (position.size as f64 * 0.5) as u64;
            position.leverage = 1.0;
        }

        position
    }

    // --------------------- Observation Hooks ---------------------
    
    /// Records the outcome of a transaction (included or not) with the given tip and slot phase.
    /// This updates the internal inclusion probability model.
    ///
    /// # Example
    /// ```rust
    /// let phase = SlotPhase::Mid; // Your slot phase tracker
    /// optimizer.record_inclusion_outcome(phase, 5000, true);
    /// ```
    pub fn record_inclusion_outcome(&self, phase: SlotPhase, tip: u64, included: bool) {
        self.update_inclusion_observation(phase, tip, included);
        if included { self.note_tip_success(phase, tip); } else { self.note_tip_miss(phase, tip); }
        #[cfg(feature = "iso_cdf")]
        if let Ok(mut m) = self.impact_model.write() {
            m.inclusion_hist.enforce_isotonic();
        }
        // Uncomment if you have metrics enabled:
        // self.metrics.counter("fpo.inclusion.obs", 1.0);
    }

    // Tip success/miss hooks feeding the phase floors (starvation+ratchet)
    #[inline(always)]
    fn note_tip_success(&self, phase: SlotPhase, tip: u64) {
        let i = phase_idx(phase);
        let target = tip.saturating_sub(tip / 10); // ~90%
        let cur = TIP_FLOOR[i].load(AOrd::Relaxed);
        if target > cur { TIP_FLOOR[i].store(target, AOrd::Relaxed); }
        TIP_FLOOR_LAST_MS[i].store(now_ms_monotonic(), AOrd::Relaxed);
    }
    #[inline(always)]
    fn note_tip_miss(&self, phase: SlotPhase, tip: u64) {
        let i = phase_idx(phase);
        let cur = TIP_FLOOR[i].load(AOrd::Relaxed);
        let target = tip.saturating_sub(tip / 5); // ~80%
        if target > cur {
            let cap = cur.saturating_add(cur/2 + 1);
            TIP_FLOOR[i].store(target.min(cap), AOrd::Relaxed);
        }
        // Do not update last_ms — only successes reset starvation
    }
    
    // --------------------- Impact Model Setters ---------------------
    
    /// Updates the venue model type (fast no-op if unchanged) and bumps epoch
    pub fn set_venue_model(&self, venue: VenueModel) {
        if let Ok(rd) = self.impact_model.try_read() {
            if rd.venue == venue { return; }
        }
        self.impact_model.write().unwrap().venue = venue;
        let _ = impact_epoch::bump();
    }
    
    /// Updates the order book snapshot (monotone + hot publish)
    pub fn set_orderbook_snapshot(&self, ob: OrderBookSnapshot) {
        if let Ok(rd) = self.impact_model.try_read() {
            if matches!(rd.venue, VenueModel::OrderBook) {
                if let Some(ref cur) = rd.ob_snapshot {
                    if ob.timestamp <= cur.timestamp
                       && (ob.mid_price - cur.mid_price).abs() <= cur.mid_price.max(1.0) * 1e-8
                       && (ob.spread    - cur.spread   ).abs() <= 1e-9 { return; }
                }
            }
        }
        {
            let mut model = self.impact_model.write().unwrap();
            model.venue = VenueModel::OrderBook;
            model.ob_snapshot = Some(ob.clone());
            model.cpmm = None; model.stableswap = None;
        }
        impact_hot::write_venue_ob(ob.mid_price, ob.spread, ob.timestamp);
        let _ = impact_epoch::bump();
    }
    
    /// Updates the CPMM pool state (monotone + hot publish)
    pub fn set_cpmm_pool(&self, pool: CpmmPool) {
        if let Ok(rd) = self.impact_model.try_read() {
            if matches!(rd.venue, VenueModel::Cpmm) {
                if let Some(ref cur) = rd.cpmm {
                    if pool.last_updated <= cur.last_updated
                        && pool.reserves_a == cur.reserves_a
                        && pool.reserves_b == cur.reserves_b
                        && pool.fee_bps   == cur.fee_bps { return; }
                }
            }
        }
        {
            let mut model = self.impact_model.write().unwrap();
            model.venue = VenueModel::Cpmm;
            model.cpmm = Some(pool.clone());
            model.ob_snapshot = None; model.stableswap = None;
        }
        impact_hot::write_venue_cpmm(pool.reserves_a, pool.reserves_b, pool.fee_bps as u32, pool.last_updated);
        let _ = impact_epoch::bump();
    }
    
    /// Updates the StableSwap pool state (freshness heuristic + hot publish)
    pub fn set_stableswap_pool(&self, pool: StableSwapPool) {
        if let Ok(rd) = self.impact_model.try_read() {
            if matches!(rd.venue, VenueModel::StableSwap) {
                if let Some(ref cur) = rd.stableswap {
                    let cur_sum: u128 = cur.reserves.iter().map(|&x| x as u128).sum();
                    let new_sum: u128 = pool.reserves.iter().map(|&x| x as u128).sum();
                    if new_sum == cur_sum && pool.fee_bps == cur.fee_bps && pool.a == cur.a { return; }
                }
            }
        }
        {
            let mut model = self.impact_model.write().unwrap();
            model.venue = VenueModel::StableSwap;
            model.stableswap = Some(pool.clone());
            model.ob_snapshot = None; model.cpmm = None;
        }
        let r0 = *pool.reserves.get(0).unwrap_or(&0) as u64;
        let r1 = *pool.reserves.get(1).unwrap_or(&0) as u64;
        impact_hot::write_venue_ss(r0, r1, pool.a as u64, pool.fee_bps as u32);
        let _ = impact_epoch::bump();
    }
    
    /// Records an inclusion observation for the given tip and phase
    pub fn update_inclusion_observation(&self, phase: SlotPhase, tip: u64, included: bool) {
        self.impact_model.write().unwrap().record_inclusion(phase, tip, included);
    }
    
    // --------------------- Core internals ---------------------

    fn snapshot(&self) -> Snapshot {
        let capital = self.capital_base.load(AOrd::Relaxed);
        let metrics = self.position_metrics.read().unwrap().clone();
        let cfg = self.cfg.read().unwrap().clone();
        let ewma_vol = {
            let v = self.volatility_buffer.read().unwrap();
            if v.is_empty() {
                0.02
            } else {
                // Simple average over last N for robustness
                let n = v.len().min(20);
                v.iter().take(n).sum::<f64>() / n as f64
            }
        };
        let recent_trades = {
            let r = self.recent_trades.read().unwrap();
            // Materialize a bounded slice to stable vec
            let take = r.len().min(252);
            r.iter().cloned().take(take).collect::<Vec<_>>()
        };
        Snapshot {
            capital,
            metrics,
            recent_trades,
            ewma_vol,
            cfg,
        }
    }

    fn calculate_kelly_position_uncertain(&self, snap: &Snapshot, opportunity_value: u64) -> u64 {
        let n_tot = snap.metrics.total_trades as usize;
        if n_tot < snap.cfg.min_trades_for_kelly || snap.metrics.avg_loss == 0.0 {
            return ((opportunity_value as f64) * 0.01).round() as u64;
        }

        let trades = self.recent_trades.read().unwrap();
        let mut wins: u64 = 0; let mut total: u64 = 0;
        let mut wins_vec: Vec<f64> = Vec::with_capacity(128);
        let mut loss_vec: Vec<f64> = Vec::with_capacity(128);
        for t in trades.iter().take(200) {
            total += 1;
            let r = t.pnl / (t.size as f64).max(1.0);
            if r > 0.0 { wins += 1; wins_vec.push(r); } else if r < 0.0 { loss_vec.push(-r); }
        }
        drop(trades);

        // Jeffreys (0.5,0.5) + Wilson LB
        let z = 1.96;
        let jeff_p = (wins as f64 + 0.5) / (total as f64 + 1.0);
        let wl     = wilson_lower_bound(wins, total, z);
        let p_lb   = 0.5 * jeff_p + 0.5 * wl;

        // Winsorized means (10%)
        let avg_win  = if wins_vec.is_empty() { 0.0 } else { winsorized_mean_inplace(&mut wins_vec, 0.10) };
        let avg_loss = if loss_vec.is_empty() { 1e-6 } else { winsorized_mean_inplace(&mut loss_vec, 0.10).max(1e-6) };

        let odds = avg_win / avg_loss;
        let kelly_raw = if odds > 0.0 { ((p_lb * odds) - (1.0 - p_lb)) / odds } else { 0.0 };

        // IR-aware dampening + hard cap
        let ir = snap.metrics.sharpe_ratio.max(0.0);
        let base_k = (kelly_raw.max(0.0) * KELLY_FRACTION * (ir / 2.0).clamp(0.0, 1.0)).min(0.25);

        // Small-sample shrinkage (K = 50)
        let n = total as f64;
        let shrink = (n / (n + 50.0)).sqrt().clamp(0.2, 1.0);

        let adj_k = base_k * shrink;
        let capacity = self.calculate_risk_capacity(snap) as f64;
        let kelly_sz = ((opportunity_value as f64) * adj_k).max(MIN_POSITION_SIZE as f64);
        kelly_sz.min(capacity).round() as u64
    }

    fn adjust_for_volatility(&self, base_size: u64, market: &MarketConditions, ewma_vol: f64) -> u64 {
        let recent_vol = ewma_vol.max(1e-6).max(market.volatility);
        let vol_ratio = recent_vol / 0.02; // normalize to baseline vol
        let vol_multiplier = if vol_ratio > 2.0 {
            0.5
        } else if vol_ratio > 1.5 {
            0.7
        } else if vol_ratio < 0.5 {
            1.3
        } else {
            1.0
        };

        (base_size as f64 * vol_multiplier) as u64
    }

    fn apply_risk_constraints(&self, size: u64, snap: &Snapshot) -> u64 {
        let max_position_cap = (snap.capital as f64 * snap.cfg.max_position_pct) as u64;

        let dd_adjusted = if snap.metrics.max_drawdown > 0.10 {
            (max_position_cap as f64 * (1.0 - snap.metrics.max_drawdown * 2.0).max(0.3)) as u64
        } else {
            max_position_cap
        };

        let risk_capacity = self.calculate_risk_capacity(snap);
        size.min(risk_capacity).min(dd_adjusted)
    }

    fn calculate_risk_capacity(&self, snap: &Snapshot) -> u64 {
        let (es95, es99) = self.empirical_es_levels(&snap.recent_trades, snap.capital);
        {
            let mut perf = self.performance_tracker.write().unwrap();
            perf.es95 = es95;
            perf.es99 = es99;
        }
        let risk_budget = (snap.capital as f64 * ES95_BUDGET) as u64;
        let var_constrained = (snap.capital as f64 * (1.0 - es95)).max(0.0) as u64;

        risk_budget.min(var_constrained).max(MIN_POSITION_SIZE)
    }

    /// Fallback impact model when no venue-specific data is available
    fn fallback_impact(size: f64, market: &MarketConditions) -> f64 {
        let spread = market.spread_bps / 10_000.0;
        let depth = market.liquidity_depth.max(1) as f64;
        let alpha = 2.0 * spread.max(0.0001);
        alpha * (size / depth).min(0.02).powf(1.2)
    }

    fn apply_dynamic_scaling(
        &self,
        size: u64,
        metrics: &PositionMetrics,
        market: &MarketConditions,
        timestamp: i64,
    ) -> u64 {
        let time_decay = if metrics.last_update > 0 {
            let elapsed = ((timestamp - metrics.last_update) as f64 / 3600.0).max(0.0);
            DECAY_FACTOR.powf(elapsed)
        } else {
            1.0
        };

        let performance_scale = self.calculate_performance_scale(metrics);
        let market_scale = self.calculate_market_scale(market);

        let total_scale = time_decay * performance_scale * market_scale * POSITION_SCALE_FACTOR;
        (size as f64 * total_scale.clamp(0.1, 1.5)) as u64
    }

    fn calculate_performance_scale(&self, metrics: &PositionMetrics) -> f64 {
        let sharpe_scale = (metrics.sharpe_ratio / 2.0).clamp(0.0, 1.5);
        let win_rate_scale = (metrics.win_rate * 2.0).clamp(0.5, 1.2);
        let drawdown_scale = (1.0 - metrics.max_drawdown).max(0.3);
        (sharpe_scale * win_rate_scale * drawdown_scale).powf(1.0 / 3.0)
    }

    fn calculate_market_scale(&self, market: &MarketConditions) -> f64 {
        let spread_scale = (10.0 / (market.spread_bps + 1.0)).min(1.0);
        let vol_scale = (0.02 / market.volatility.max(0.001)).clamp(0.5, 1.2);
        let liquidity_scale = ((market.liquidity_depth as f64).ln() / 20.0).clamp(0.3, 1.0);
        let slippage_scale = (1.0 - market.recent_slippage * 10.0).max(0.5);
        (spread_scale * vol_scale * liquidity_scale * slippage_scale).powf(0.25)
    }

    fn calculate_optimal_leverage(&self, metrics: &PositionMetrics, market: &MarketConditions) -> f64 {
        let base = if metrics.sharpe_ratio > 1.5 {
            2.0
        } else if metrics.sharpe_ratio > 1.0 {
            1.5
        } else if metrics.sharpe_ratio > 0.5 {
            1.2
        } else {
            1.0
        };

        let vol_adj = (0.02 / market.volatility.max(0.01)).clamp(0.5, 1.5);
        let win_adj = (metrics.win_rate * 2.0).clamp(0.7, 1.3);
        let dd_pen = (1.0 - metrics.max_drawdown * 2.0).max(0.5);

        (base * vol_adj * win_adj * dd_pen).clamp(1.0, MAX_LEVERAGE)
    }

    fn calculate_stop_levels(
        &self,
        metrics: &PositionMetrics,
        market: &MarketConditions,
        position_size: u64,
    ) -> (f64, f64) {
        let atr_estimate = market.volatility * 2.0;
        let spread_cost = market.spread_bps / 10_000.0;

        let avg_loss_pct = if metrics.avg_loss != 0.0 {
            (metrics.avg_loss / position_size as f64).abs()
        } else {
            0.02
        };

        let stop_loss_distance = (atr_estimate * 1.5 + spread_cost)
            .max(avg_loss_pct * 0.7)
            .min(0.05);

        let risk_reward_ratio = if metrics.avg_profit > 0.0 && metrics.avg_loss < 0.0 {
            (metrics.avg_profit / metrics.avg_loss.abs()).max(1.5)
        } else {
            2.0
        };

        let take_profit_distance = stop_loss_distance * risk_reward_ratio;

        let slippage_adjusted_stop = stop_loss_distance * (1.0 + market.recent_slippage);
        let slippage_adjusted_tp = take_profit_distance * (1.0 - market.recent_slippage * 0.5);

        (slippage_adjusted_stop, slippage_adjusted_tp)
    }

    fn calculate_position_confidence(&self, metrics: &PositionMetrics, market: &MarketConditions) -> f64 {
        let win_rate_confidence = if metrics.total_trades > 100 {
            metrics.win_rate * 0.9 + 0.1
        } else if metrics.total_trades > 50 {
            metrics.win_rate * 0.7 + 0.15
        } else {
            0.5
        };

        let sharpe_confidence = (metrics.sharpe_ratio / 3.0).clamp(0.0, 1.0);
        let oracle_confidence = market.oracle_confidence;

        let recency_weight = if metrics.total_trades > 0 {
            let recent_count = self.recent_trades.read().unwrap().iter().take(20).count();
            (recent_count as f64 / 20.0).min(1.0)
        } else {
            0.0
        };

        let market_confidence = 1.0 - (market.network_congestion * 0.5 + market.volatility * 2.0).min(0.7);

        let weighted_confidence = win_rate_confidence * 0.30
            + sharpe_confidence * 0.25
            + oracle_confidence * 0.20
            + recency_weight * 0.15
            + market_confidence * 0.10;

        weighted_confidence.clamp(CONFIDENCE_THRESHOLD * 0.5, 0.95)
    }

    fn calculate_expected_value(
        &self,
        size: u64,
        metrics: &PositionMetrics,
        market: &MarketConditions,
    ) -> f64 {
        let win_prob = metrics.win_rate.clamp(0.0, 1.0);
        let loss_prob = 1.0 - win_prob;

        let expected_profit = if metrics.avg_profit > 0.0 {
            metrics.avg_profit * (1.0 - market.recent_slippage)
        } else {
            size as f64 * 0.01
        };

        let expected_loss = if metrics.avg_loss < 0.0 {
            metrics.avg_loss * (1.0 + market.recent_slippage)
        } else {
            -(size as f64 * 0.02)
        };

        let gas_cost = market.priority_fee as f64 * GAS_SAFETY_MULTIPLIER;
        let spread_cost = size as f64 * (market.spread_bps / 10_000.0);

        let gross_ev = win_prob * expected_profit + loss_prob * expected_loss;
        let net_ev = gross_ev - gas_cost - spread_cost;

        net_ev * (1.0 - market.network_congestion * 0.2)
    }

    /// Helper function to generate candidate tips around the base tip
    fn candidate_tips(base_tip: u64) -> Vec<u64> {
        let base = base_tip as f64;
        let mut v = Vec::with_capacity(11);
        let mut scale = 0.5_f64;
        let mul = 1.3_f64;
        for _ in 0..=10 { v.push((base * scale) as u64); scale *= mul; }
        v
    }

    /// Sigmoid fallback for inclusion probability
    fn sigmoid_inclusion(tip: u64, k: f64, mid: f64) -> f64 {
        1.0 / (1.0 + (-k * (tip as f64 - mid)).exp())
    }



    fn calculate_max_gas_inclusion_aware(
        &self,
        position_size: u64,
        market: &MarketConditions,
        model: &ImpactModel,
        ev: f64,
    ) -> u64 {
        if ev <= 0.0 { return MIN_TIP_LAMPORTS; }

        let base_tip = market.priority_fee.max(1);
        let sz_root = (position_size as f64).max(1.0).sqrt();
        let vol_mult = (market.volatility * 50.0).clamp(1.0, 2.0);
        let phase = Self::slot_phase_from_now();

        let relay_attn = 1.0_f64; // placeholder for future relay attention

        let (risk_mult, cost_coeff) = {
            let perf = self.performance_tracker.read().unwrap();
            let tight = (perf.es99 / ES99_BUDGET).clamp(0.0, 2.0);
            let risk_mult = (1.0 - 0.25 * tight).clamp(0.5, 1.0);
            let coeff = vol_mult * (0.25 + 0.75 / (1.0 + 0.000_001 * sz_root));
            (risk_mult, coeff.max(1e-12))
        };

        #[inline(always)]
        fn score(model: &ImpactModel, phase: SlotPhase, relay_attn: f64,
                 tip: u64, ev: f64, vol_mult: f64, sz_root: f64) -> (f64, f64) {
            let p_emp = model.inclusion_hist.p_inclusion(phase, tip) * relay_attn;
            let p_sig = FractionalPositionOptimizer::sigmoid_inclusion(tip, model.incl_sigmoid_k, model.incl_sigmoid_mid) * 0.5;
            let p = p_emp.max(p_sig).clamp(0.0, 1.0);
            let cost = (vol_mult * (tip as f64)) * (0.25 + 0.75 / (1.0 + 0.000_001 * sz_root));
            (p.mul_add(ev, -cost), p)
        }

        // dynamic min floor with decay + starvation boost
        let base_floor = decayed_phase_floor(phase).max(MIN_TIP_LAMPORTS);
        let min_allowed = ((base_floor as f64) * phase_starvation_boost(phase)).round() as u64;

        // 1) coarse candidates
        let cand = Self::candidate_tips(base_tip);
        let mut best_tip = base_tip;
        let mut best_obj = f64::NEG_INFINITY;
        let mut best_p   = 0.0;
        for raw in cand.iter().copied() {
            let tip = raw.max(min_allowed);
            let (obj, p) = score(model, phase, relay_attn, tip, ev, vol_mult, sz_root);
            if obj > best_obj { best_obj = obj; best_tip = tip; best_p = p; }
        }

        // 2) refine ±10%, ±20%
        let mut refine = [0u64; 5];
        let mut i = 0usize;
        let try_add = |arr: &mut [u64;5], i: &mut usize, v: u64| { if *i < 5 { arr[*i] = v; *i += 1; } };
        let up = |x: u64, pct: f64| ((x as f64) * (1.0 + pct)).round() as u64;
        let dn = |x: u64, pct: f64| ((x as f64) * (1.0 - pct)).round().max(1.0) as u64;
        try_add(&mut refine, &mut i, best_tip);
        try_add(&mut refine, &mut i, dn(best_tip, 0.10).max(min_allowed));
        try_add(&mut refine, &mut i, up(best_tip, 0.10));
        try_add(&mut refine, &mut i, dn(best_tip, 0.20).max(min_allowed));
        try_add(&mut refine, &mut i, up(best_tip, 0.20));
        for k in 0..i {
            let t = refine[k];
            let (obj, p) = score(model, phase, relay_attn, t, ev, vol_mult, sz_root);
            if obj > best_obj { best_obj = obj; best_tip = t; best_p = p; }
        }

        // 3) saturation-aware downgrade
        if best_p > 0.985 {
            let cheaper = (best_tip as f64 * 0.9).round() as u64;
            if cheaper >= min_allowed {
                let (obj2, _p2) = score(model, phase, relay_attn, cheaper, ev, vol_mult, sz_root);
                if obj2 >= best_obj * 0.999 { best_tip = cheaper; best_obj = obj2; }
            }
        }

        // congestion + caps + anti-herd (optional)
        let c = market.network_congestion;
        let cong_mult = if c > 0.8 { 2.25 } else if c > 0.6 { 1.75 } else if c > 0.4 { 1.35 } else { 1.10 };
        let mut tip = (best_tip as f64 * cong_mult).min((base_tip as f64) * MAX_TIP_MULT) as u64;

        #[cfg(feature = "anti_herd")]
        { tip = jitter_tip_lamports(tip); }

        let affordable = ((ev * (risk_mult)) / (cost_coeff)).max(MIN_TIP_LAMPORTS as f64) as u64;
        tip.min(affordable).min(MAX_TIP_LAMPORTS).max(MIN_TIP_LAMPORTS)
    }

    // Optional: tip + delay orchestrator (phase-aware tiny stagger)
    /// Returns (tip_lamports, delay_ms) with phase-aware grace + tiny stagger.
    /// If delay_ms == 0, submit now.
    #[inline(always)]
    fn pick_tip_and_send_delay(
        &self,
        position_size: u64,
        market: &MarketConditions,
        model: &ImpactModel,
        ev: f64,
    ) -> (u64, u64) {
        let tip = self.calculate_max_gas_inclusion_aware(position_size, market, model, ev);
        let phase = Self::slot_phase_from_now();
        let grace = match phase { SlotPhase::Early => 10, SlotPhase::Mid => 8, SlotPhase::Late => 12 };
        let stagger = Self::process_stagger_ms(3); // 0..2ms cushion
        let delay = if Self::should_send_now(grace) { 0 } else { Self::next_send_delay(grace) + stagger };
        (tip, delay)
    }

    // --------------------- Metrics maintenance ---------------------

    fn update_metrics_and_trackers(&self, result: &TradeResult) {
        // Update metrics
        {
            let mut metrics = self.position_metrics.write().unwrap();
            metrics.total_trades += 1;
            metrics.last_update = result.timestamp;

            let is_win = result.pnl > 0.0;
            let alpha = 2.0 / (metrics.total_trades as f64 + 1.0).min(100.0);

            if is_win {
                metrics.current_streak = metrics.current_streak.max(0) + 1;
                metrics.avg_profit = metrics.avg_profit * (1.0 - alpha) + result.pnl * alpha;
            } else {
                metrics.current_streak = metrics.current_streak.min(0) - 1;
                metrics.avg_loss = metrics.avg_loss * (1.0 - alpha) + result.pnl * alpha;
            }

            let win_count = self
                .recent_trades
                .read()
                .unwrap()
                .iter()
                .take(100)
                .filter(|t| t.pnl > 0.0)
                .count();
            metrics.win_rate = if metrics.total_trades < 100 {
                win_count as f64 / metrics.total_trades as f64
            } else {
                win_count as f64 / 100.0
            };

            self.update_sharpe_ratio(&mut metrics);
            self.update_max_drawdown(&mut metrics);
        }

        // Update performance tracker
        self.update_performance_tracker();
    }

    fn update_sharpe_ratio(&self, metrics: &mut PositionMetrics) {
        let trades = self.recent_trades.read().unwrap();
        if trades.len() < 30 { return; }

        // Welford on last 252 normalized returns
        let mut n = 0f64; let mut mean = 0f64; let mut m2 = 0f64;
        for r in trades.iter().take(252).map(|t| t.pnl / (t.size as f64).max(1.0)) {
            n += 1.0; let d = r - mean; mean += d / n; m2 += d * (r - mean);
        }
        if n < 2.0 { metrics.sharpe_ratio = 0.0; return; }
        let sd = (m2 / (n - 1.0)).max(0.0).sqrt();
        if sd == 0.0 { metrics.sharpe_ratio = 0.0; return; }

        let rf_per_period = RISK_FREE_RATE / 252.0;
        let excess = mean - rf_per_period;
        metrics.sharpe_ratio = (excess / sd) * 252.0_f64.sqrt();
    }

    fn update_max_drawdown(&self, metrics: &mut PositionMetrics) {
        let trades = self.recent_trades.read().unwrap();
        if trades.is_empty() {
            metrics.max_drawdown = 0.0;
            return;
        }

        // Equity curve on normalized per-trade returns
        let mut eq: f64 = 0.0;
        let mut peak: f64 = 0.0;
        let mut max_dd: f64 = 0.0;

        for t in trades.iter().rev().take(1000) {
            let r = t.pnl / (t.size as f64).max(1.0);
            eq += r;
            peak = peak.max(eq);
            let dd = if peak > 0.0 { (peak - eq) / peak } else { 0.0 };
            max_dd = max_dd.max(dd);
        }

        metrics.max_drawdown = max_dd.clamp(0.0, 1.0);
    }

    fn update_performance_tracker(&self) {
        let trades = self.recent_trades.read().unwrap();
        if trades.len() < 30 { return; }
        let mut rets: Vec<f64> = trades.iter().take(252)
            .map(|t| t.pnl / (t.size as f64).max(1.0)).collect();
        if rets.is_empty() { return; }

        // Welford stats
        let mut n = 0f64; let mut mu = 0f64; let mut m2 = 0f64;
        for &r in &rets { n += 1.0; let d = r - mu; mu += d / n; m2 += d * (r - mu); }
        let sd = if n > 1.0 { (m2 / (n - 1.0)).sqrt() } else { 0.0 };
        let rf = RISK_FREE_RATE / 252.0;
        let excess = mu - rf;

        // Sortino
        let downside: Vec<f64> = rets.iter().map(|r| (*r - rf).min(0.0)).collect();
        let dd = std_sample(&downside);
        let sortino = if dd > 0.0 { (excess / dd) * 252.0_f64.sqrt() } else { 0.0 };

        // Calmar on equity curve of normalized returns
        let mut eq = 0.0; let mut peak = 0.0; let mut max_dd = 0.0;
        for r in &rets { eq += *r; peak = peak.max(eq); let d = if peak > 0.0 { (peak - eq)/peak } else { 0.0 }; max_dd = max_dd.max(d); }
        let ann_return = mu * 252.0;
        let calmar = if max_dd > 0.0 { ann_return / max_dd } else { 0.0 };

        // Omega
        let (gains, losses) = rets.iter().fold((0.0, 0.0), |(g,l), r| {
            let ex = *r - rf; if ex >= 0.0 { (g+ex, l) } else { (g, l + (-ex)) }
        });
        let omega = if losses > 0.0 { gains / losses } else { f64::INFINITY };

        // Tails
        rets.sort_by(|a,b| a.total_cmp(b));
        let p95 = percentile(&rets, 0.95);
        let p05 = percentile(&rets, 0.05);
        let tail_ratio = if p05 != 0.0 { p95 / p05.abs() } else { 0.0 };

        // ES on sorted returns (return-space)
        let (es95_r, es99_r) = empirical_es(&rets, (0.95, 0.99));

        let mut tracker = self.performance_tracker.write().unwrap();
        tracker.rolling_sharpe = if sd > 0.0 { (excess / sd) * 252.0_f64.sqrt() } else { 0.0 };
        tracker.rolling_sortino = sortino;
        tracker.calmar_ratio = calmar;
        tracker.omega_ratio = omega;
        tracker.tail_ratio = tail_ratio;
        tracker.es95 = es95_r.abs();
        tracker.es99 = es99_r.abs();
    }

    // --------------------- Risk utilities ---------------------

    fn empirical_es_levels(&self, trades: &[TradeResult], capital: u64) -> (f64, f64) {
        if trades.len() < 30 {
            // Fallback conservative
            return (0.05, 0.10);
        }
        let mut rets: Vec<f64> = trades
            .iter()
            .map(|t| t.pnl / (t.size as f64).max(1.0))
            .collect();

        rets.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let (es95_r, es99_r) = empirical_es(&rets, (0.95, 0.99));
        // Convert to capital relative scale using a conservative mapping:
        // assume position is a fraction of capital bounded by MAX_POSITION_RATIO.
        let pos_frac = MAX_POSITION_RATIO;
        let es95_cap = (es95_r.abs() * pos_frac).clamp(0.0, 1.0);
        let es99_cap = (es99_r.abs() * pos_frac * 1.5).clamp(0.0, 1.0);

        (es95_cap, es99_cap)
    }
}

// ========================= Math helpers =========================

fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    xs.iter().sum::<f64>() / xs.len() as f64
}

fn std_sample(xs: &[f64]) -> f64 {
    let n = xs.len();
    if n <= 1 {
        return 0.0;
    }
    let m = mean(xs);
    let var = xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (n as f64 - 1.0);
    var.sqrt()
}

fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let n = sorted.len() as f64;
    let idx = ((n - 1.0) * q).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// Empirical Expected Shortfall at levels (c1, c2) on sorted returns
fn empirical_es(sorted_rets: &[f64], levels: (f64, f64)) -> (f64, f64) {
    let n = sorted_rets.len();
    if n == 0 {
        return (0.0, 0.0);
    }
    let idx95 = ((1.0 - levels.0) * n as f64).floor().clamp(0.0, (n - 1) as f64) as usize;
    let idx99 = ((1.0 - levels.1) * n as f64).floor().clamp(0.0, (n - 1) as f64) as usize;

    let es95 = if idx95 == 0 {
        sorted_rets[0]
    } else {
        sorted_rets[..=idx95].iter().sum::<f64>() / (idx95 + 1) as f64
    };
    let es99 = if idx99 == 0 {
        sorted_rets[0]
    } else {
        sorted_rets[..=idx99].iter().sum::<f64>() / (idx99 + 1) as f64
    };
    (es95, es99)
}

fn mean_and_se(xs: &[f64]) -> (f64, f64) {
    if xs.is_empty() {
        return (0.0, 0.0);
    }
    let m = mean(xs);
    let sd = std_sample(xs);
    let se = if xs.len() > 0 { sd / (xs.len() as f64).sqrt() } else { 0.0 };
    (m, se)
}

fn wilson_lower_bound(successes: u64, n: u64, z: f64) -> f64 {
    if n == 0 {
        return 0.0;
    }
    let p = successes as f64 / n as f64;
    let denom = 1.0 + (z * z) / n as f64;
    let centre = p + (z * z) / (2.0 * n as f64);
    let adj = z * ((p * (1.0 - p) + (z * z) / (4.0 * n as f64)) / n as f64).sqrt();
    ((centre - adj) / denom).clamp(0.0, 1.0)
}

fn sigmoid_inclusion(tip: f64, k: f64, mid: f64) -> f64 {
    let x = (tip - mid) / mid.max(1.0);
    1.0 / (1.0 + (-k * x).exp())
}

// ========================= Examples =========================

/// Example usage of the optimizer with venue models and observation recording
/// 
/// ```rust
/// use fractional_position_optimizer::{
///     FractionalPositionOptimizer, 
///     SlotPhase,
///     VenueModel,
///     OrderBookSnapshot,
///     CpmmPool
/// };
///
/// // Initialize the optimizer
/// let optimizer = FractionalPositionOptimizer::new(1_000_000_000);
///
/// // Set up venue model (Order Book example)
/// optimizer.set_venue_model(VenueModel::OrderBook);
/// optimizer.set_orderbook_snapshot(OrderBookSnapshot {
///     bids: vec![],
///     asks: vec![],
///     timestamp: 0,
///     mid_price: 100.0,
///     spread: 0.001,
/// });
///
/// // Or for AMM pools:
/// optimizer.set_venue_model(VenueModel::Cpmm);
/// optimizer.set_cpmm_pool(CpmmPool {
///     reserves_a: 1_000_000,
///     reserves_b: 1_000_000,
///     fee_bps: 30,
///     last_updated: 0,
/// });
///
/// // After submitting a transaction, record the outcome
/// let phase = SlotPhase::Mid; // Your slot phase tracker
/// optimizer.record_inclusion_outcome(phase, 5000, true);
/// ```

// ========================= Tests =========================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_optimizer_initialization() {
        let optimizer = FractionalPositionOptimizer::new(1_000_000_000);
        assert_eq!(optimizer.capital(), 1_000_000_000);
    }

    #[test]
    fn test_optimal_position_calculation() {
        let optimizer = FractionalPositionOptimizer::new(1_000_000_000);
        let market = MarketConditions {
            volatility: 0.02,
            liquidity_depth: 10_000_000,
            spread_bps: 5.0,
            priority_fee: 10_000,
            network_congestion: 0.3,
            oracle_confidence: 0.95,
            recent_slippage: 0.001,
        };

        let position = optimizer.calculate_optimal_position(1_000_000, &market, 1000);
        assert!(position.size >= MIN_POSITION_SIZE);
        assert!((1.0..=MAX_LEVERAGE).contains(&position.leverage));
    }

    #[test]
    fn test_wilson_lower_bound() {
        let w = wilson_lower_bound(60, 100, 1.96);
        assert!(w > 0.45 && w < 0.65);
    }

    #[test]
    fn test_empirical_es() {
        let mut rets = vec![-0.05, -0.02, -0.01, 0.0, 0.01, 0.02, 0.05];
        rets.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let (es95, es99) = empirical_es(&rets, (0.95, 0.99));
        assert!(es95 <= 0.0 && es99 <= es95);
    }
    
    #[test]
    fn test_hist_monotonicity() {
        let mut h = InclusionHistogram::new(vec![5_000, 10_000, 20_000, 50_000], 0.5);
        
        // Add some observations
        h.update(SlotPhase::Mid, 5_000, true);
        h.update(SlotPhase::Mid, 5_000, false);
        h.update(SlotPhase::Mid, 10_000, true);
        h.update(SlotPhase::Mid, 20_000, true);
        
        // enforce_monotonic ensures non-decreasing CDF
        assert!(h.cdf_mid[0] <= h.cdf_mid[1], "CDF should be non-decreasing");
        assert!(h.cdf_mid[1] <= h.cdf_mid[2], "CDF should be non-decreasing");
        assert!(h.cdf_mid[2] <= h.cdf_mid[3], "CDF should be non-decreasing");
    }
    
    #[test]
    fn test_orderbook_impact_increasing() {
        let ob = OrderBookSnapshot {
            bids: vec![],
            asks: vec![],
            timestamp: 0,
            mid_price: 100.0,
            spread: 0.001,
            ..OrderBookSnapshot::default()
        };
        
        // Test that larger orders have higher impact
        let small_order_impact = FractionalPositionOptimizer::orderbook_impact_cost(10_000.0, &ob);
        let large_order_impact = FractionalPositionOptimizer::orderbook_impact_cost(100_000.0, &ob);
        
        assert!(
            large_order_impact > small_order_impact,
            "Larger orders should have higher impact"
        );
        
        // Test edge case: zero size has zero impact
        assert_eq!(
            FractionalPositionOptimizer::orderbook_impact_cost(0.0, &ob),
            0.0,
            "Zero size should have zero impact"
        );
    }
}
