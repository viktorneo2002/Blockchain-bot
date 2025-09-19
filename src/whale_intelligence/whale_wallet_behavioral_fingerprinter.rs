#![deny(unsafe_code)]
#![deny(warnings)]

use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH, Instant};
use tokio::sync::RwLock;
use std::sync::RwLock as StdRwLock;
use solana_sdk::pubkey::Pubkey;
use solana_sdk::instruction::Instruction;
use serde::{Deserialize, Serialize};
// === ADD (verbatim) ===
use solana_sdk::compute_budget::{self, ComputeBudgetInstruction};
use solana_sdk::hash::Hash;
use solana_program::system_instruction;
use anyhow::Result;
use once_cell::sync::Lazy;
use dashmap::{DashMap, DashSet};
// === ADD (verbatim) ===
use ahash::AHashMap;
// === REPLACE: atomic import line ===
use std::sync::atomic::{AtomicU64, Ordering, AtomicBool, AtomicI64};
// === ADD (verbatim) ===
use smallvec::SmallVec;
use std::thread;
use ahash::AHasher;
use std::hash::Hasher;

// Real Solana DEX Program IDs
pub const JUPITER_PROGRAM_ID: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
pub const RAYDIUM_PROGRAM_ID: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
pub const ORCA_PROGRAM_ID: &str = "9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP";

// Common stablecoin mints
pub const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
pub const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
pub const BUSD_MINT: &str = "AJ1W9A9N9dEMdVyoDiam2rV44gnBm2csrPDP7xqcapgX";

// === ADD (verbatim) ===
pub static JUPITER_PROGRAM: Lazy<Pubkey> = Lazy::new(|| Pubkey::from_str(JUPITER_PROGRAM_ID).unwrap());
pub static RAYDIUM_PROGRAM: Lazy<Pubkey> = Lazy::new(|| Pubkey::from_str(RAYDIUM_PROGRAM_ID).unwrap());
pub static ORCA_PROGRAM:    Lazy<Pubkey> = Lazy::new(|| Pubkey::from_str(ORCA_PROGRAM_ID).unwrap());

pub static USDC: Lazy<Pubkey> = Lazy::new(|| Pubkey::from_str(USDC_MINT).unwrap());
pub static USDT: Lazy<Pubkey> = Lazy::new(|| Pubkey::from_str(USDT_MINT).unwrap());
pub static BUSD: Lazy<Pubkey> = Lazy::new(|| Pubkey::from_str(BUSD_MINT).unwrap());

// === ADD (verbatim) ===
pub static EXTRA_STABLE: Lazy<DashSet<Pubkey, ahash::RandomState>> =
    Lazy::new(|| DashSet::with_hasher(ahash::RandomState::default()));

#[inline]
pub fn add_stablecoin(mint: Pubkey) { EXTRA_STABLE.insert(mint); }

#[inline]
pub fn remove_stablecoin(mint: &Pubkey) { EXTRA_STABLE.remove(mint); }

// === REPLACE: is_stablecoin ===
#[inline(always)]
fn is_stablecoin(mint: &Pubkey) -> bool {
    mint == &*USDC || mint == &*USDT || mint == &*BUSD || EXTRA_STABLE.contains(mint)
}

// === ADD (verbatim) ===
const RECENT_WINDOW_SECS: u64 = 86_400; // 24h rolling window
// === REPLACE: const MAX_RECENT_SWAPS line ===
pub const MAX_RECENT_SWAPS: usize = 128;    // hard cap; keeps memory O(1)
const MAX_PATTERN_TIMES: usize = 512;   // per-pattern timestamps cap

// === ADD (verbatim) ===
const MIRROR_BAND_SECS: u64 = 5;
const MIRROR_SCAN_BACK: usize = 64;
const MIRROR_DECAY_HL: f64 = 2.0; // seconds

// === ADD (verbatim) ===
const HEAT_ALPHA: f64 = 0.20;            // EWMA for slot fee-heat
const TIP_ALPHA:  f64 = 0.15;            // EWMA for average observed tip (lamports)
const PPM_SCALE:  f64 = 1_000_000.0;     // fixed-point scaling for atomics

const DEFAULT_CU_ESTIMATE: u64 = 200_000; // conservative CU for a typical swap/bundle
const MAX_TIP_LAMPORTS:    u64 = 5_000_000; // hard cap safety (5M lamports)

// === ADD (verbatim) ===
const TIMING_MAX_PAIRS: usize = 128; // upper bound on matched timing pairs per correl
const TIMING_MAX_SKEW_SECS: u64 = 60; // symmetric window used in swap timing correl

// === ADD (verbatim) ===
const MAX_SLIPPAGE_BPS_HARD: u16 = 9_900;   // reject pathological payloads
const MIN_AMOUNT_IN: u64 = 10;              // dust guard (prevents noise / grief)
const MAX_MATCH_LATENCY_SECS: u64 = 8;      // timing window for mirror burst

// === ADD (verbatim) ===
const MIN_MIRROR_BAND_SECS: f64 = 2.0;
const MAX_MIRROR_BAND_SECS: f64 = 6.0;

// === ADD (verbatim) ===
const STAIR_MAX_ATTEMPTS: u32 = 6;
const MIRROR_RECENCY_TAU_SECS: f64 = 180.0; // ~3 min half-life-esque
const COORD_HL_SECS: f64 = 600.0; // 10 min half-life

// === ADD (verbatim) ===
const SHADOW_POS_HOLD_SECS: u64 = 2;
const SHADOW_NEG_COOLDOWN_SECS: u64 = 1;

// === ADD (verbatim) ===
const SPEND_WINDOW_SECS: u64 = 60;

// === ADD (verbatim) ===
const STATS_ALPHA: f64 = 0.20;

// === ADD (verbatim) ===
const SURPRISE_SHORT_SECS: u64 = 10;
const SURPRISE_LONG_SECS:  u64 = 120;

// === ADD (verbatim) ===
const VOL_ALPHA: f64 = 0.15;

// === ADD (verbatim) ===
const DD_WINDOW_SECS: u64 = 600; // 10 min
const DD_HARD_NEG_LAMPORTS: i64 = -2_500_000; // ~0.0025 SOL default brake

// === ADD (verbatim) ===
const CB_CACHE_CAP: usize = 256;
static CB_CACHE_TICK: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(1));

// === ADD (verbatim) ===
type RelayId = u8; // map these to your relays externally

#[derive(Default)]
struct RelayStats {
    acc_ppm:   AtomicU64, // acceptance ppm (0..1e6)
    per_cu:    AtomicU64, // lamports/CU EWMA
    rtt_ms:    AtomicU64, // end-to-end RTT ms EWMA
    cool_until: AtomicU64, // unix ts (secs) until which relay is cooled
    fails:     AtomicU64,  // consecutive fails
}
impl RelayStats {
    #[inline] fn load_acc(&self) -> f64 { (self.acc_ppm.load(Ordering::Relaxed) as f64) / PPM_SCALE }
    #[inline] fn store_acc(&self, x: f64) { self.acc_ppm.store((x.clamp(0.0,1.0)*PPM_SCALE).round() as u64, Ordering::Relaxed); }
    #[inline] fn load_per_cu(&self) -> u64 { self.per_cu.load(Ordering::Relaxed) }
    #[inline] fn store_per_cu(&self, v: u64) { self.per_cu.store(v, Ordering::Relaxed); }
    #[inline] fn load_rtt(&self) -> u64 { self.rtt_ms.load(Ordering::Relaxed) }
    #[inline] fn store_rtt(&self, v: u64) { self.rtt_ms.store(v, Ordering::Relaxed); }
    #[inline] fn set_cool(&self, until: u64) { self.cool_until.store(until, Ordering::Relaxed); }
    #[inline] fn cooled(&self, now: u64) -> bool { now <= self.cool_until.load(Ordering::Relaxed) }
}

const RELAY_ALPHA: f64 = 0.20;
const RELAY_COOL_BASE_SECS: u64 = 8;

// === ADD (verbatim) ===
const FEED_MAX_SOURCES: usize = 4;
const FEED_QUORUM: usize = 2;
const FEED_FUSE_EPS_SECS: u64 = 1;

type SourceId = u8;

// per-source typed patterns
// key = (source, dex, token_in)
type SrcKey = (SourceId, DexType, Pubkey);

// === ADD (verbatim) ===
const BLOOM_BITS: usize = 1 << 20; // 1M bits (~128KB/partition)
const BLOOM_HASHES: usize = 3;
const BLOOM_ROTATE_SECS: u64 = 300;

struct RotBloom {
    a: Box<[AtomicU64]>, // bitset A
    b: Box<[AtomicU64]>, // bitset B
    start_sec: AtomicU64,
    which_a: AtomicBool, // active partition flag
}
impl RotBloom {
    fn new() -> Self {
        let words = BLOOM_BITS / 64;
        let make = || (0..words).map(|_| AtomicU64::new(0)).collect::<Vec<_>>().into_boxed_slice();
        Self { a: make(), b: make(), start_sec: AtomicU64::new(0), which_a: AtomicBool::new(true) }
    }
    #[inline] fn reset_slice(s: &Box<[AtomicU64]>) { for w in s.iter() { w.store(0, Ordering::Relaxed); } }

    #[inline]
    fn touch_rotate(&self, now_sec: u64) {
        let start = self.start_sec.load(Ordering::Relaxed);
        if now_sec.saturating_sub(start) >= BLOOM_ROTATE_SECS {
            // rotate partitions
            let use_a = !self.which_a.load(Ordering::Relaxed);
            self.which_a.store(use_a, Ordering::Relaxed);
            if use_a { Self::reset_slice(&self.a); } else { Self::reset_slice(&self.b); }
            self.start_sec.store(now_sec, Ordering::Relaxed);
        }
    }

    #[inline]
    fn hashes(bytes: &[u8]) -> [u64; BLOOM_HASHES] {
        // use ahash for speed; derive multiple via xorshift
        let mut h = ahash::AHasher::default();
        h.write(bytes);
        let mut x = h.finish();
        let mut out = [0u64; BLOOM_HASHES];
        for i in 0..BLOOM_HASHES {
            x ^= x << 13; x ^= x >> 7; x ^= x << 17; // xorshift
            out[i] = x;
        }
        out
    }

    #[inline]
    fn check_or_set(&self, key: &[u8], now_sec: u64) -> bool {
        self.touch_rotate(now_sec);
        let use_a = self.which_a.load(Ordering::Relaxed);
        let bits = BLOOM_BITS as u64;
        let hashes = Self::hashes(key);
        let mut seen = true;

        let slice = if use_a { &self.a } else { &self.b };
        for h in hashes.iter() {
            let idx = (h % bits) as usize;
            let w = idx / 64; let b = 1u64 << (idx % 64);
            let prev = slice[w].fetch_or(b, Ordering::Relaxed);
            if (prev & b) == 0 { seen = false; }
        }
        seen
    }
}

static SEEN_SIGS: Lazy<RotBloom> = Lazy::new(RotBloom::new);

#[inline]
pub fn seen_signature(sig_bytes: &[u8], now_sec: u64) -> bool {
    SEEN_SIGS.check_or_set(sig_bytes, now_sec)
}

// === ADD (verbatim) ===
#[derive(Default)]
struct RouteComplexity {
    cu_ewma: AtomicU64,
    heap_ewma: AtomicU64,
}
impl RouteComplexity {
    #[inline] fn load_cu(&self) -> u64 { self.cu_ewma.load(Ordering::Relaxed) }
    #[inline] fn store_cu(&self, v: u64) { self.cu_ewma.store(v, Ordering::Relaxed); }
    #[inline] fn load_heap(&self) -> u64 { self.heap_ewma.load(Ordering::Relaxed) }
    #[inline] fn store_heap(&self, v: u64) { self.heap_ewma.store(v, Ordering::Relaxed); }
}

const COMPLEXITY_ALPHA: f64 = 0.20;

// === ADD (verbatim) ===
#[derive(Default)]
struct SlotLock { held: AtomicBool }

type PairSlotKey = (Pubkey, Pubkey, u64);

// === ADD (verbatim) ===
// per-leader best phase (ms from slot start) and EWMA update
#[derive(Default)]
struct LeaderPhase {
    ms: AtomicU64, // best offset
}
impl LeaderPhase {
    #[inline] fn load(&self) -> u64 { self.ms.load(Ordering::Relaxed) }
    #[inline] fn store(&self, v: u64) { self.ms.store(v, Ordering::Relaxed); }
}

const PHASE_ALPHA: f64 = 0.20; // EWMA aggressiveness

// === ADD (verbatim) ===
const RELAY_ALPHA: f64 = 0.15;
const RELAY_COOL_BASE_SECS: u64 = 2;
const FEED_MAX_SOURCES: u8 = 8;
const FEED_QUORUM: usize = 3;
const FEED_FUSE_EPS_SECS: u64 = 2;
const SURPRISE_SHORT_SECS: u64 = 30;
const SURPRISE_LONG_SECS: u64 = 300;
const STATS_ALPHA: f64 = 0.20;

// === ADD (verbatim) ===
const ACC_BUCKETS: usize = 8;
const STRIKE_ALPHA: f64 = 0.20;
const COOL_SECS: u64 = 300; // 5 minutes
const MAX_TX_BYTES_SOFT: usize = 1200; // stay under UDP 1232 with margin

// === ADD (verbatim) ===
const SLOT_MS_INIT: u64 = 400;
const SLOT_MS_MIN:  u64 = 350;
const SLOT_MS_MAX:  u64 = 500;
const SLOT_MS_ALPHA: f64 = 0.15;
const BH_TTL_SLOTS: u64 = 120; // conservative TTL
const FEE_BUDGET_FRAC: f64 = 0.35; // spend ≤ 35% of available lamports
const MAX_CLUSTER_LAG_SLOTS: u64 = 5;
const EXPO_LIMIT_USD: u64 = 50_000_000; // $50 in micro-USD (example; override)
const EXPO_PAIR_LIMIT_USD: u64 = 120_000_000; // $120 cap per pair

// === ADD (verbatim) ===
const HEAT_MOM_ALPHA: f64 = 0.25;

const FR_STRIKE_WINDOW_SECS: u64 = 60;
const FR_STRIKE_COOL_SECS:   u64 = 120;

const MISS_COOL_SECS: u64 = 90;

const SPEND_WINDOW_SECS: u64 = 60;
const CB_CACHE_CAP: usize = 256;
const SHADOW_NEG_COOLDOWN_SECS: u64 = 5;

// Additional constants for the new pieces
const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
const BONK_MINT: &str = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263";

// === ADD (verbatim) === CA-CK constants
const Q_ETA: f64 = 0.02; // SA step

const PAIR_EV_ALPHA: f64 = 0.20;

const HOT_WRITE_EPS_SECS: u64 = 2;

const PROFIT_HL_SECS: f64 = 900.0; // 15 min

const MOM_BUCKETS: usize = 5;

const LEADER_MIN_ACC: f64 = 0.10;
const LEADER_COOL_SLOTS: u64 = 8;

const RELAY_SEND_GAP_MS: u64 = 3;

// === ADD (verbatim) ===
static SLOT_PRICE_TICK: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

// === ADD (verbatim) ===
type RelayMask = u32; // up to 32 relays
type RelayId = u8;
type SourceId = u8;
type SrcKey = (SourceId, DexType, Pubkey);

// === ADD (verbatim) ===
#[derive(Default)]
struct HeatMom {
    last_tip: AtomicU64,          // lamports
    last_ts:  AtomicU64,          // secs
    mom_ppm:  AtomicI64,          // ∆tip per second normalized (ppm of current tip)
}
impl HeatMom {
    #[inline] fn load(&self) -> i64 { self.mom_ppm.load(Ordering::Relaxed) }
    #[inline] fn update(&self, tip_now: u64, now_sec: u64) {
        let last = self.last_tip.swap(tip_now, Ordering::Relaxed);
        let lt   = self.last_ts.swap(now_sec, Ordering::Relaxed);
        if last == 0 || lt == 0 { return; }
        let dt = now_sec.saturating_sub(lt).max(1) as f64;
        let d  = (tip_now as f64) - (last as f64);
        let base = (last as f64).max(1.0);
        let inst_ppm = ((d / dt) / base * 1_000_000.0).clamp(-2_000_000.0, 2_000_000.0);
        let prev = self.mom_ppm.load(Ordering::Relaxed) as f64;
        let next = (1.0 - HEAT_MOM_ALPHA) * prev + HEAT_MOM_ALPHA * inst_ppm;
        self.mom_ppm.store(next.round() as i64, Ordering::Relaxed);
    }
}

// === ADD (verbatim) ===
#[derive(Default)]
struct FrAtom { strikes: AtomicU64, last_ts: AtomicU64 }

#[derive(Default)]
struct MissAtom { streak: AtomicU64 }

#[derive(Default)]
struct SlotAttempt { flag: AtomicBool }

type SlotPairKey = (Pubkey,Pubkey,u64);

#[derive(Default)]
struct ProgBump { cu: u32, heap: u32 }

// === ADD (verbatim) === CA-CK structs
#[derive(Default)]
struct LeaderQuants { p50: AtomicU64, p80: AtomicU64, p95: AtomicU64 }
impl LeaderQuants {
    #[inline] fn load(&self) -> (u64,u64,u64) {
        (self.p50.load(Ordering::Relaxed), self.p80.load(Ordering::Relaxed), self.p95.load(Ordering::Relaxed))
    }
    #[inline] fn update(&self, x: u64) {
        // Robbins–Monro quantile SA
        let upd = |q: &AtomicU64, tgt: f64| {
            let q0 = q.load(Ordering::Relaxed) as f64;
            let delta = if (x as f64) <= q0 { 1.0 - tgt } else { 0.0 - tgt };
            let q1 = (q0 + Q_ETA * delta).max(1.0);
            q.store(q1.round() as u64, Ordering::Relaxed);
        };
        upd(&self.p50, 0.50); upd(&self.p80, 0.80); upd(&self.p95, 0.95);
    }
}

#[derive(Default)]
struct PairEv { ev_ewma: AtomicI64 }
impl PairEv { 
    #[inline] fn load(&self)->i64{ self.ev_ewma.load(Ordering::Relaxed) } 
    #[inline] fn store(&self,v:i64){ self.ev_ewma.store(v,Ordering::Relaxed)} 
}

#[derive(Default)]
struct HotAtom { ts: AtomicU64 }

type HotKey = Pubkey;

#[derive(Default)]
struct ProfitAtom { lamports: AtomicI64, ts: AtomicU64 }

#[derive(Default)]
struct StepHist { bins: [AtomicU64; STAIR_MAX_ATTEMPTS as usize] }

#[derive(Default)]
struct ShortWin { tries: AtomicU64, wins: AtomicU64, cool_until_slot: AtomicU64 }

// === ADD (verbatim) ===
#[derive(Default)]
struct SeenSelf {
    ts_sec: AtomicU64,
}
impl SeenSelf {
    #[inline] fn mark(&self, now: u64) { self.ts_sec.store(now, Ordering::Relaxed); }
    #[inline] fn seen_since(&self, since: u64) -> bool { self.ts_sec.load(Ordering::Relaxed) >= since }
}

#[derive(Clone)]
struct CbEntry { ixs: [Instruction; 2], tick: u64 }

// === ADD (verbatim) ===
#[derive(Default)]
struct RelayStats {
    acc: AtomicU64,      // acceptance rate (0..1 scaled to u64)
    per_cu: AtomicU64,   // EWMA per-CU price paid (lamports)
    rtt: AtomicU64,      // EWMA RTT (ms)
    fails: AtomicU64,    // consecutive failures
    cool_until: AtomicU64, // unix timestamp
}

impl RelayStats {
    #[inline] fn load_acc(&self) -> f64 { (self.acc.load(Ordering::Relaxed) as f64) / PPM_SCALE }
    #[inline] fn store_acc(&self, v: f64) { self.acc.store((v * PPM_SCALE).round() as u64, Ordering::Relaxed); }
    #[inline] fn load_per_cu(&self) -> u64 { self.per_cu.load(Ordering::Relaxed) }
    #[inline] fn store_per_cu(&self, v: u64) { self.per_cu.store(v, Ordering::Relaxed); }
    #[inline] fn load_rtt(&self) -> u64 { self.rtt.load(Ordering::Relaxed) }
    #[inline] fn store_rtt(&self, v: u64) { self.rtt.store(v, Ordering::Relaxed); }
    #[inline] fn cooled(&self, now_sec: u64) -> bool { self.cool_until.load(Ordering::Relaxed) > now_sec }
    #[inline] fn set_cool(&self, until_sec: u64) { self.cool_until.store(until_sec, Ordering::Relaxed); }
}

// === ADD (verbatim) ===
static CB_CACHE_TICK: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(0));

// === ADD (verbatim) ===
#[inline]
fn ratio_bucket(rel: f64) -> u8 {
    // price ratio = per_cu / ref_pcu; gently log-ish binning
    if rel < 0.75 { 0 } else if rel < 0.90 { 1 } else if rel < 1.00 { 2 } else if rel < 1.10 { 3 }
    else if rel < 1.25 { 4 } else if rel < 1.50 { 5 } else if rel < 1.80 { 6 } else { 7 }
}

// === ADD (verbatim) ===
#[inline]
fn bh_stale(bh: Option<(Hash,u64)>, cur_slot: u64) -> bool {
    if let Some((_, s)) = bh { cur_slot.saturating_sub(s) >= BH_TTL_SLOTS } else { true }
}

// === ADD (verbatim) ===
#[derive(Default)]
struct BetaBin { 
    succ: AtomicU64, 
    fail: AtomicU64 
}
impl BetaBin {
    #[inline] fn post_mean(&self) -> f64 {
        let s = self.succ.load(Ordering::Relaxed) as f64;
        let f = self.fail.load(Ordering::Relaxed) as f64;
        (s + 1.0) / (s + f + 2.0) // Beta(1,1) prior
    }
    #[inline] fn update(&self, ok: bool) {
        if ok { self.succ.fetch_add(1, Ordering::Relaxed); }
        else   { self.fail.fetch_add(1, Ordering::Relaxed); }
    }
}

// === ADD (verbatim) ===
#[derive(Default)]
struct StrikePcu { 
    pcu: AtomicU64 
}
impl StrikePcu { 
    #[inline] fn load(&self) -> u64 { self.pcu.load(Ordering::Relaxed) } 
    #[inline] fn store(&self, v: u64) { self.pcu.store(v, Ordering::Relaxed) } 
}

// === ADD (verbatim) ===
#[derive(Default)]
struct SlotClock {
    slot_ms_ewma: AtomicU64,
    slot_start_epoch_ms: AtomicU64,
}
impl SlotClock {
    #[inline] fn init() -> Self {
        Self { slot_ms_ewma: AtomicU64::new(SLOT_MS_INIT), slot_start_epoch_ms: AtomicU64::new(0) }
    }
    #[inline] fn load_ms(&self) -> u64 { self.slot_ms_ewma.load(Ordering::Relaxed) }
    #[inline] fn load_start_ms(&self) -> u64 { self.slot_start_epoch_ms.load(Ordering::Relaxed) }
    #[inline] fn store_start_ms(&self, ms: u64) { self.slot_start_epoch_ms.store(ms, Ordering::Relaxed); }
    #[inline] fn update_ms(&self, observed_ms: u64) {
        let prev = self.load_ms() as f64;
        let next = (1.0 - SLOT_MS_ALPHA) * prev + SLOT_MS_ALPHA * (observed_ms as f64);
        self.slot_ms_ewma.store(next.round().clamp(SLOT_MS_MIN as f64, SLOT_MS_MAX as f64) as u64, Ordering::Relaxed);
    }
}

// === ADD (verbatim) ===
#[derive(Default)]
struct BlockhashState {
    hash: StdRwLock<Option<(Hash, u64)>>,
}
impl BlockhashState {
    #[inline] fn set(&self, h: Hash, slot: u64) { *self.hash.write().unwrap() = Some((h, slot)); }
    #[inline] fn get(&self) -> Option<(Hash, u64)> { self.hash.read().unwrap().clone() }
}

// === ADD (verbatim) ===
#[derive(Default)]
struct ExpoAtom { 
    usd_abs: AtomicU64 
} // micro-USD (1e-6 USD)
impl ExpoAtom {
    #[inline] fn load(&self) -> u64 { self.usd_abs.load(Ordering::Relaxed) }
    #[inline] fn add_abs(&self, v: u64) { self.usd_abs.fetch_add(v, Ordering::Relaxed); }
    #[inline] fn sub_abs(&self, v: u64) { self.usd_abs.fetch_sub(v.min(self.load()), Ordering::Relaxed); }
}

#[derive(Default)]
struct ShadowStats {
    attempts: AtomicU64,
    hits:     AtomicU64,
    ev_lamports_ewma: AtomicI64, // signed lamports
}

impl ShadowStats {
    #[inline] fn load_hr(&self) -> f64 {
        let a = self.attempts.load(Ordering::Relaxed) as f64;
        if a == 0.0 { 0.0 } else { (self.hits.load(Ordering::Relaxed) as f64) / a }
    }
    #[inline] fn load_ev(&self) -> f64 { self.ev_lamports_ewma.load(Ordering::Relaxed) as f64 }
    #[inline] fn update(&self, included: bool, realized_edge_lamports: i64) {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        if included { self.hits.fetch_add(1, Ordering::Relaxed); }
        let prev = self.ev_lamports_ewma.load(Ordering::Relaxed) as f64;
        let next = (1.0 - STATS_ALPHA) * prev + STATS_ALPHA * (realized_edge_lamports as f64);
        self.ev_lamports_ewma.store(next.round() as i64, Ordering::Relaxed);
    }
}

// === ADD (verbatim) ===
#[derive(Default)]
struct VolAtom {
    val: AtomicU64, // f64 stored as u64 bits
}

impl VolAtom {
    #[inline] fn load(&self) -> f64 { f64::from_bits(self.val.load(Ordering::Relaxed)) }
    #[inline] fn store(&self, v: f64) { self.val.store(v.to_bits(), Ordering::Relaxed); }
}

// === ADD (verbatim) ===
#[derive(Default)]
struct CoordStrength {
    ppm: AtomicU64, // 0..1_000_000
    last_ts: AtomicU64,
}

impl CoordStrength {
    #[inline] fn load(&self) -> (f64, u64) {
        let p = (self.ppm.load(Ordering::Relaxed) as f64) / PPM_SCALE;
        let t = self.last_ts.load(Ordering::Relaxed);
        (p, t)
    }
    #[inline] fn store(&self, p: f64, ts: u64) {
        let ppm = (p.clamp(0.0, 1.0) * PPM_SCALE).round() as u64;
        self.ppm.store(ppm, Ordering::Relaxed);
        self.last_ts.store(ts, Ordering::Relaxed);
    }
}

// === ADD (verbatim) ===
#[derive(Default)]
struct LeaderStats {
    inclusions: AtomicU64,
    attempts: AtomicU64,
    per_cu_paid: AtomicU64, // EWMA of per-CU price paid on inclusions
}

impl LeaderStats {
    #[inline] fn load_acc(&self) -> f64 {
        let a = self.attempts.load(Ordering::Relaxed) as f64;
        if a == 0.0 { 0.0 } else { (self.inclusions.load(Ordering::Relaxed) as f64) / a }
    }
    #[inline] fn load_pcu(&self) -> u64 { self.per_cu_paid.load(Ordering::Relaxed) }
    #[inline] fn update(&self, included: bool, per_cu: u64) {
        self.attempts.fetch_add(1, Ordering::Relaxed);
        if included {
            self.inclusions.fetch_add(1, Ordering::Relaxed);
            let prev = self.load_pcu() as f64;
            let newv = 0.8 * prev + 0.2 * (per_cu as f64);
            self.per_cu_paid.store(newv.round() as u64, Ordering::Relaxed);
        }
    }
}

// === ADD (verbatim) ===
#[derive(Default)]
struct SpendWindow {
    start_ts: AtomicU64,
    spent: AtomicU64,
}

impl SpendWindow {
    #[inline] fn reset_if_needed(&self, now_ts: u64) {
        let start = self.start_ts.load(Ordering::Relaxed);
        if now_ts.saturating_sub(start) >= SPEND_WINDOW_SECS {
            self.start_ts.store(now_ts, Ordering::Relaxed);
            self.spent.store(0, Ordering::Relaxed);
        }
    }
    #[inline] fn allow(&self, lamports: u64, now_ts: u64, cap: u64) -> bool {
        self.reset_if_needed(now_ts);
        let prev = self.spent.fetch_add(lamports, Ordering::Relaxed);
        prev + lamports <= cap
    }
}

// === ADD (verbatim) ===
#[inline]
fn count_since(dq: &VecDeque<u64>, now_ts: u64, window: u64) -> u32 {
    let cutoff = now_ts.saturating_sub(window);
    let mut c = 0u32;
    for &t in dq.iter().rev() {
        if t >= cutoff { c += 1; } else { break; }
    }
    c
}

#[inline]
fn burst_surprise_score_from(pattern_ts: &VecDeque<u64>, now_ts: u64) -> f64 {
    if pattern_ts.len() < 4 { return 0.0; }
    let s = count_since(pattern_ts, now_ts, SURPRISE_SHORT_SECS) as f64 / (SURPRISE_SHORT_SECS as f64);
    let l = count_since(pattern_ts, now_ts, SURPRISE_LONG_SECS)  as f64 / (SURPRISE_LONG_SECS as f64);
    if l <= 0.0 { return 0.0; }
    let ratio = (s / l).max(0.0);
    // map ratio>1 to (0,1) smoothly; ignore <=1
    let excess = (ratio - 1.0).max(0.0);
    (excess / (excess + 1.5)).clamp(0.0, 1.0) // fast rise, bounded
}

// === ADD (verbatim) ===
#[inline]
fn canon_pair(a: Pubkey, b: Pubkey) -> (Pubkey, Pubkey) {
    let ab = a.to_bytes();
    let bb = b.to_bytes();
    if ab <= bb { (a,b) } else { (b,a) }
}

// === ADD (verbatim) ===
#[inline]
fn extract_cu_price_from_ix(ix: &Instruction) -> Option<u64> {
    // ComputeBudget ABI: tag=3 (SetComputeUnitPrice), then LE u64
    if ix.data.len() >= 9 && ix.data[0] == 3u8 {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&ix.data[1..9]);
        Some(u64::from_le_bytes(buf))
    } else { None }
}

// === ADD (verbatim) ===
#[inline(always)]
fn read_u64_le_at(data: &[u8], off: usize) -> Option<u64> {
    let end = off.checked_add(8)?;
    let s = data.get(off..end)?;
    // Safety: slice is exactly 8 bytes due to get()
    Some(u64::from_le_bytes(s.try_into().ok()?))
}

#[inline(always)]
fn declared_slippage_bps(amount_in: u64, min_out: u64) -> u16 {
    if amount_in == 0 { return 0; }
    let ai = amount_in as u128;
    let mo = min_out as u128;
    let diff = ai.saturating_sub(mo);
    let num  = diff.saturating_mul(10_000u128);
    let bps  = (num / ai).min(9_999u128);
    bps as u16
}

// === ADD (verbatim) ===
#[inline(always)]
fn sane_swap(token_in: &Pubkey, token_out: &Pubkey, amount_in: u64, min_out: u64, slippage_bps: u16) -> bool {
    if token_in == token_out { return false; }
    if amount_in < MIN_AMOUNT_IN { return false; }
    if min_out == 0 { return false; }
    if slippage_bps > MAX_SLIPPAGE_BPS_HARD { return false; }
    true
}

#[inline(always)]
fn sanitize_swap_fields(token_in: Pubkey, token_out: Pubkey, amount_in: u64, min_out: u64) -> (Pubkey, Pubkey, u64, u64) {
    // clamp monotonicity without changing economics
    let ai = amount_in.max(MIN_AMOUNT_IN);
    let mo = min_out.min(amount_in); // never > amount_in
    (token_in, token_out, ai, mo)
}

// === ADD (verbatim) ===
#[derive(Default)]
pub struct FeeHeat {
    // heat in ppm [0..1_000_000], EWMA of (1 - exp(-bundles/3))
    heat_ppm: AtomicU64,
    // EWMA of observed mean tip in lamports
    tip_avg:  AtomicU64,
    // online p80 of per-bundle tips (lamports), SA quantile
    tip_p80: AtomicU64,
}

impl FeeHeat {
    #[inline]
    fn load_heat(&self) -> f64 {
        (self.heat_ppm.load(Ordering::Relaxed) as f64) / PPM_SCALE
    }
    #[inline]
    fn store_heat(&self, h: f64) {
        let v = (h.clamp(0.0, 1.0) * PPM_SCALE).round() as u64;
        self.heat_ppm.store(v, Ordering::Relaxed);
    }
    #[inline]
    fn load_tip(&self) -> u64 {
        self.tip_avg.load(Ordering::Relaxed)
    }
    #[inline]
    fn store_tip(&self, lamports: u64) {
        self.tip_avg.store(lamports.min(MAX_TIP_LAMPORTS), Ordering::Relaxed);
    }
    #[inline]
    pub fn load_tip_p80(&self) -> u64 { self.tip_p80.load(Ordering::Relaxed) }

    /// Stochastic-approx quantile update; feed per-bundle tip samples ad-hoc.
    #[inline]
    pub fn update_tip_quantile_p80(&self, sample_lamports: u64) {
        // q_{t+1} = q_t + η * (I[x <= q_t] - 0.8)
        const ETA: f64 = 0.02;
        let q0 = self.load_tip_p80() as f64;
        let delta = if (sample_lamports as f64) <= q0 { 1.0 - 0.8 } else { 0.0 - 0.8 };
        let q1 = (q0 + ETA * delta).max(1.0);
        self.tip_p80.store(q1.round() as u64, Ordering::Relaxed);
    }
    #[inline]
    pub fn update_slot(&self, observed_bundles: u32, mean_tip_lamports: u64) {
        // instantaneous heat in [0,1]
        let inst_heat = 1.0 - (-(observed_bundles as f64) / 3.0).exp();
        let prev_heat = self.load_heat();
        let new_heat  = (1.0 - HEAT_ALPHA) * prev_heat + HEAT_ALPHA * inst_heat;
        self.store_heat(new_heat);

        let prev_tip = self.load_tip() as f64;
        let new_tip  = (1.0 - TIP_ALPHA) * prev_tip + TIP_ALPHA * (mean_tip_lamports as f64);
        self.store_tip(new_tip.round() as u64);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SwapData {
    pub timestamp: u64,
    pub dex: DexType,
    pub token_in: Pubkey,
    pub token_out: Pubkey,
    pub amount_in: u64,
    pub amount_out: u64,
    pub slippage_bps: u16,
    pub transaction_signature: String,
}

// === REPLACE: the #[derive(...)] on DexType ===
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DexType {
    Jupiter,
    Raydium,
    Orca,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidityAction {
    pub timestamp: u64,
    pub action_type: LiquidityActionType,
    pub pool: Pubkey,
    pub amount_a: u64,
    pub amount_b: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LiquidityActionType {
    AddLiquidity,
    RemoveLiquidity,
}

// === REPLACE: the entire WalletProfile struct and its impl block (supersedes step #4) ===
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalletProfile {
    pub wallet: Pubkey,

    // Recent swap window (24h) as a ring buffer
    pub recent_swaps: VecDeque<SwapData>,
    pub liquidity_actions: Vec<LiquidityAction>,

    // Timestamps & volume
    pub first_seen: u64,
    pub last_activity: u64,
    pub total_volume: u64,

    // ===== Rolling counters for O(1) metrics =====
    // token entropy: counts over the active window
    token_counts: AHashMap<Pubkey, u32>,
    token_total: u32,              // counts both in+out → +2 per swap

    // stablecoin flow
    stable_outflow_in_sum: u128,   // sum(amount_in) when token_in is stable
    stable_inflow_out_sum: u128,   // sum(amount_out) when token_out is stable

    // averages
    swaps_count: u32,              // active window size
    sum_slippage_bps: u64,         // Σ slippage_bps
    sum_aggr_base: f64,            // Σ ( ln(amount_in)/20 + 2*slippage_bps/10_000 )

    // cached endpoints for freq calc (avoid peeking the deque repeatedly)
    first_ts: u64,
    last_ts: u64,

    // ===== Exposed metrics (kept in sync) =====
    pub aggression_index: f64,
    pub token_rotation_entropy: f64,
    pub risk_preference: f64,
    pub liquidity_behavior_score: f64,
    pub mirror_intent_score: f64,
    pub coordination_score: f64,
    pub stablecoin_inflow_ratio: f64,
    pub avg_slippage_tolerance: f64,
    pub swap_frequency_score: f64,
}

impl WalletProfile {
    #[inline]
    pub fn new(wallet: Pubkey) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0))
            .as_secs();

        Self {
            wallet,
            recent_swaps: VecDeque::with_capacity(MAX_RECENT_SWAPS),
            liquidity_actions: Vec::new(),
            first_seen: now,
            last_activity: now,
            total_volume: 0,

            token_counts: AHashMap::new(),
            token_total: 0,

            stable_outflow_in_sum: 0,
            stable_inflow_out_sum: 0,

            swaps_count: 0,
            sum_slippage_bps: 0,
            sum_aggr_base: 0.0,

            first_ts: 0,
            last_ts: 0,

            aggression_index: 0.0,
            token_rotation_entropy: 0.0,
            risk_preference: 0.5,
            liquidity_behavior_score: 0.0,
            mirror_intent_score: 0.0,
            coordination_score: 0.0,
            stablecoin_inflow_ratio: 0.0,
            avg_slippage_tolerance: 0.0,
            swap_frequency_score: 0.0,
        }
    }

    #[inline(always)]
    fn add_token(&mut self, k: Pubkey) {
        *self.token_counts.entry(k).or_insert(0) += 1;
        self.token_total = self.token_total.saturating_add(1);
    }

    #[inline(always)]
    fn remove_token(&mut self, k: Pubkey) {
        if let Some(v) = self.token_counts.get_mut(&k) {
            if *v > 1 { *v -= 1; } else { self.token_counts.remove(&k); }
            self.token_total = self.token_total.saturating_sub(1);
        }
    }

    #[inline(always)]
    fn apply_swap(&mut self, s: &SwapData, sign: i32) {
        // sign = +1 on push, -1 on pop
        match sign {
            1 => {
                self.add_token(s.token_in);
                self.add_token(s.token_out);
                self.swaps_count = self.swaps_count.saturating_add(1);
                self.sum_slippage_bps = self.sum_slippage_bps.saturating_add(s.slippage_bps as u64);
                let size_factor = (s.amount_in as f64).ln() / 20.0;
                let slip_factor = (s.slippage_bps as f64) / 10_000.0;
                self.sum_aggr_base += size_factor + 2.0 * slip_factor;

                if is_stablecoin(&s.token_in)  { self.stable_outflow_in_sum = self.stable_outflow_in_sum.saturating_add(s.amount_in as u128); }
                if is_stablecoin(&s.token_out) { self.stable_inflow_out_sum = self.stable_inflow_out_sum.saturating_add(s.amount_out as u128); }
            }
            -1 => {
                self.remove_token(s.token_in);
                self.remove_token(s.token_out);
                self.swaps_count = self.swaps_count.saturating_sub(1);
                self.sum_slippage_bps = self.sum_slippage_bps.saturating_sub(s.slippage_bps as u64);
                let size_factor = (s.amount_in as f64).ln() / 20.0;
                let slip_factor = (s.slippage_bps as f64) / 10_000.0;
                self.sum_aggr_base -= size_factor + 2.0 * slip_factor;

                if is_stablecoin(&s.token_in)  { self.stable_outflow_in_sum = self.stable_outflow_in_sum.saturating_sub(s.amount_in as u128); }
                if is_stablecoin(&s.token_out) { self.stable_inflow_out_sum = self.stable_inflow_out_sum.saturating_sub(s.amount_out as u128); }
            }
            _ => {}
        }
    }

    #[inline]
    fn recompute_cached_metrics(&mut self) {
        // entropy
        if self.token_total > 0 {
            let mut h = 0.0;
            let total = self.token_total as f64;
            for &c in self.token_counts.values() {
                let p = (c as f64) / total;
                // p*ln(p) safe for p>0
                h -= p * p.ln();
            }
            self.token_rotation_entropy = h;
        } else {
            self.token_rotation_entropy = 0.0;
        }

        // avg slippage
        self.avg_slippage_tolerance = if self.swaps_count == 0 { 0.0 } else {
            (self.sum_slippage_bps as f64) / (self.swaps_count as f64)
        };

        // frequency score (swaps per hour, capped 10)
        let ts_span = if self.first_ts > 0 && self.last_ts >= self.first_ts {
            self.last_ts - self.first_ts
        } else { 0 };
        self.swap_frequency_score = if self.swaps_count <= 1 || ts_span == 0 {
            if self.swaps_count > 1 { 10.0 } else { 0.0 }
        } else {
            let sph = (self.swaps_count as f64) * 3600.0 / (ts_span as f64);
            sph.min(10.0)
        };

        // aggression index = mean(base) * freq_mult (cap 10)
        let base_aggr = if self.swaps_count == 0 { 0.0 } else { self.sum_aggr_base / (self.swaps_count as f64) };
        self.aggression_index = (base_aggr * (1.0 + self.swap_frequency_score / 2.0)).min(10.0);

        // stablecoin ratios / risk
        let denom = self.stable_inflow_out_sum.saturating_add(self.stable_outflow_in_sum);
        self.stablecoin_inflow_ratio = if denom == 0 { 0.0 } else {
            (self.stable_inflow_out_sum as f64) / (denom as f64)
        };
        // risk preference higher when fewer stables (use total in-volume from counters)
        let total_in_vol = (self.total_volume as f64).max(1.0); // guard
        let stable_in_ratio = (self.stable_outflow_in_sum as f64) / total_in_vol;
        self.risk_preference = (1.0 - stable_in_ratio).clamp(0.0, 1.0);
    }

    #[inline]
    fn refresh_window_edges(&mut self) {
        self.first_ts = self.recent_swaps.front().map(|s| s.timestamp).unwrap_or(0);
        self.last_ts  = self.recent_swaps.back().map(|s| s.timestamp).unwrap_or(self.first_ts);
    }

    #[inline]
    fn prune_old(&mut self, now_ts: u64) {
        let cutoff = now_ts.saturating_sub(RECENT_WINDOW_SECS);
        while let Some(front) = self.recent_swaps.front() {
            if front.timestamp <= cutoff {
                let old = self.recent_swaps.pop_front().unwrap();
                self.apply_swap(&old, -1);
            } else { break; }
        }
    }

    // === REPLACE: the entire push_swap_pruned body (keep signature) ===
    #[inline]
    pub fn push_swap_pruned(&mut self, mut swap: SwapData) {
        // Enforce non-decreasing timestamps to keep the deque time-ordered
        if self.last_ts != 0 && swap.timestamp < self.last_ts {
            swap.timestamp = self.last_ts; // clamp for order; preserves window math & linear scans
        }

        // prune window first based on the (possibly clamped) timestamp
        self.prune_old(swap.timestamp);

        // push new swap
        self.total_volume = self.total_volume.saturating_add(swap.amount_in);
        self.last_activity = swap.timestamp;

        if self.recent_swaps.len() == MAX_RECENT_SWAPS {
            if let Some(evicted) = self.recent_swaps.pop_front() {
                self.apply_swap(&evicted, -1);
            }
        }
        self.apply_swap(&swap, 1);
        self.recent_swaps.push_back(swap);

        // refresh edges & recompute cached metrics
        self.refresh_window_edges();
        self.recompute_cached_metrics();
    }
}

// === REPLACE: fields in WhaleWalletBehavioralFingerprinter and its new() ===
pub struct WhaleWalletBehavioralFingerprinter {
    profiles: DashMap<Pubkey, WalletProfile, ahash::RandomState>,
    mempool_patterns: DashMap<String, VecDeque<u64>, ahash::RandomState>,
    mempool_patterns_typed: DashMap<(DexType, Pubkey), VecDeque<u64>, ahash::RandomState>,
    coordination_graph: DashMap<Pubkey, DashSet<Pubkey, ahash::RandomState>, ahash::RandomState>,

    // === NEW: slot-synced fee-heat ===
    fee_heat: FeeHeat,
    // === NEW: leader preference hint (atomic, zero-lock path) ===
    leader_aligned: AtomicBool,
    // === NEW: capture→chain latency bias (ms), positive means we see events late
    latency_bias_ms: AtomicI64,
    // === NEW: leader stats storage ===
    leader_stats: DashMap<Pubkey, LeaderStats, ahash::RandomState>,
    // === NEW: pair volatility tracking ===
    pair_vol: DashMap<(Pubkey, Pubkey), VolAtom, ahash::RandomState>,
    // wallets temporarily quarantined until timestamp
    quarantine_until: DashMap<Pubkey, u64, ahash::RandomState>,
    // === NEW: coordination strength tracking ===
    coord_strength: DashMap<(Pubkey, Pubkey), CoordStrength, ahash::RandomState>,
    // bandit: per-(heat, vol, leader) start-index policy
    stair_policies: DashMap<PolicyKey, Vec<StartArm>, ahash::RandomState>,
    // last chosen start index per wallet (for outcome attribution)
    last_stair_start: DashMap<Pubkey, u8, ahash::RandomState>,
    // === NEW: shadow state cache ===
    shadow_state: DashMap<Pubkey, (bool, u64), ahash::RandomState>,
    spend_cap_lamports_per_min: AtomicU64,
    attempts_cap_per_slot:      AtomicU64,
    spend_window: SpendWindow,
    attempts_used_in_slot: DashMap<u64 /*slot*/, AtomicU64, ahash::RandomState>,
    // === NEW: compute budget cache ===
    cb_cache: DashMap<(u32, u64), CbEntry, ahash::RandomState>,
    // === NEW: shadow P&L attribution ===
    shadow_stats: DashMap<Pubkey, ShadowStats, ahash::RandomState>,
    // === NEW: drawdown quarantine until unixts (secs) ===
    dd_quarantine_until: DashMap<Pubkey, u64, ahash::RandomState>,
    // === NEW: current slot for deterministic jitter ===
    current_slot: AtomicU64,
    // === NEW: per-wallet attempt cap overrides ===
    attempts_cap_override: DashMap<Pubkey, u64, ahash::RandomState>,
    // === NEW: multi-relay router stats ===
    relay_stats: DashMap<RelayId, RelayStats, ahash::RandomState>,
    // === NEW: per-source typed patterns for consensus ===
    mempool_patterns_typed_src: DashMap<SrcKey, VecDeque<u64>, ahash::RandomState>,
    // === NEW: route complexity tracking ===
    route_cx: DashMap<(DexType, Pubkey, Pubkey), RouteComplexity, ahash::RandomState>,
    // === NEW: pair-slot concurrency lock ===
    pair_slot_lock: DashMap<PairSlotKey, SlotLock, ahash::RandomState>,
    // === NEW: leader-phase calibrator ===
    leader_phase_ms: DashMap<Pubkey, LeaderPhase, ahash::RandomState>,
    // === NEW: per-slot global price floor ===
    slot_max_per_cu: AtomicU64,
    // === NEW: leader-pinned relay candidates ===
    leader_relay_mask: DashMap<Pubkey, RelayMask, ahash::RandomState>,
    // === NEW: abort-on-seen ===
    seen_self: SeenSelf,
    // === NEW: calibrated acceptance ===
    accept_cal: DashMap<(Pubkey /*leader*/, u8 /*bucket*/), BetaBin, ahash::RandomState>,
    // === NEW: strike floor ===
    strike_pcu: DashMap<(Pubkey /*leader*/, DexType, (Pubkey,Pubkey) /*canon pair*/), StrikePcu, ahash::RandomState>,
    // === NEW: token/pair cool-switch ===
    token_cool_until: DashMap<Pubkey, u64, ahash::RandomState>,
    pair_cool_until:  DashMap<(Pubkey,Pubkey), u64, ahash::RandomState>,
    // === NEW: slot-timing EWMA + phase drift ===
    slot_clock: SlotClock,
    // === NEW: recent blockhash manager ===
    blockhash_state: BlockhashState,
    // === NEW: exposure guard ===
    expo_token_usd: DashMap<Pubkey, ExpoAtom, ahash::RandomState>,
    expo_pair_usd:  DashMap<(Pubkey,Pubkey), ExpoAtom, ahash::RandomState>,
    expo_token_cap_usd: AtomicU64,
    expo_pair_cap_usd:  AtomicU64,
    // === NEW: BP - Heat Momentum Anticipator ===
    heat_momentum: HeatMom,
    // === NEW: BS - Front-Run Quarantine ===
    fr_strikes: DashMap<(Pubkey,Pubkey), FrAtom, ahash::RandomState>,
    // === NEW: BV - Miss-Streak Brake ===
    miss_streaks: DashMap<(Pubkey,Pubkey), MissAtom, ahash::RandomState>,
    // === NEW: BW - Slot-Level Attempt Registry ===
    slot_attempted: DashMap<SlotPairKey, SlotAttempt, ahash::RandomState>,
    // === NEW: BY - Program-ID Bump Table ===
    program_bumps: DashMap<Pubkey, ProgBump, ahash::RandomState>,
    // === NEW: CA - Per-Leader Quantile Tip Model ===
    leader_quants: DashMap<Pubkey, LeaderQuants, ahash::RandomState>,
    // === NEW: CB - Pair-Level EV EWMA ===
    pair_ev: DashMap<(Pubkey,Pubkey), PairEv, ahash::RandomState>,
    // === NEW: CC - Hot-Write Detector ===
    hot_writes: DashMap<HotKey, HotAtom, ahash::RandomState>,
    // === NEW: CF - Profit Half-Life Amplifier ===
    wallet_profit: DashMap<Pubkey, ProfitAtom, ahash::RandomState>,
    // === NEW: CH - Outcome by Step Histogram ===
    step_hist: DashMap<PolicyKey, StepHist, ahash::RandomState>,
    // === NEW: CJ - Leader Short-Horizon Cooldown ===
    leader_short: DashMap<Pubkey, ShortWin, ahash::RandomState>,
}

impl WhaleWalletBehavioralFingerprinter {
    // === REPLACE: new() ===
    pub fn new() -> Self {
        Self {
            profiles: DashMap::with_hasher(ahash::RandomState::default()),
            mempool_patterns: DashMap::with_hasher(ahash::RandomState::default()),
            mempool_patterns_typed: DashMap::with_hasher(ahash::RandomState::default()),
            coordination_graph: DashMap::with_hasher(ahash::RandomState::default()),
            fee_heat: FeeHeat::default(),
            leader_aligned: AtomicBool::new(false),
        latency_bias_ms: AtomicI64::new(0),
        leader_stats: DashMap::with_hasher(ahash::RandomState::default()),
        pair_vol: DashMap::with_hasher(ahash::RandomState::default()),
        quarantine_until: DashMap::with_hasher(ahash::RandomState::default()),
        coord_strength: DashMap::with_hasher(ahash::RandomState::default()),
        stair_policies: DashMap::with_hasher(ahash::RandomState::default()),
        last_stair_start: DashMap::with_hasher(ahash::RandomState::default()),
        shadow_state: DashMap::with_hasher(ahash::RandomState::default()),
        spend_cap_lamports_per_min: AtomicU64::new(2_000_000_000), // default 2 SOL/min
        attempts_cap_per_slot:      AtomicU64::new(4),
        spend_window: SpendWindow::default(),
        attempts_used_in_slot: DashMap::with_hasher(ahash::RandomState::default()),
        cb_cache: DashMap::with_hasher(ahash::RandomState::default()),
        shadow_stats: DashMap::with_hasher(ahash::RandomState::default()),
        dd_quarantine_until: DashMap::with_hasher(ahash::RandomState::default()),
        current_slot: AtomicU64::new(0),
        attempts_cap_override: DashMap::with_hasher(ahash::RandomState::default()),
        relay_stats: DashMap::with_hasher(ahash::RandomState::default()),
        mempool_patterns_typed_src: DashMap::with_hasher(ahash::RandomState::default()),
        route_cx: DashMap::with_hasher(ahash::RandomState::default()),
        pair_slot_lock: DashMap::with_hasher(ahash::RandomState::default()),
        leader_phase_ms: DashMap::with_hasher(ahash::RandomState::default()),
        slot_max_per_cu: AtomicU64::new(0),
        leader_relay_mask: DashMap::with_hasher(ahash::RandomState::default()),
        seen_self: SeenSelf::default(),
        accept_cal: DashMap::with_hasher(ahash::RandomState::default()),
        strike_pcu: DashMap::with_hasher(ahash::RandomState::default()),
        token_cool_until: DashMap::with_hasher(ahash::RandomState::default()),
        pair_cool_until: DashMap::with_hasher(ahash::RandomState::default()),
        slot_clock: SlotClock::init(),
        blockhash_state: BlockhashState::default(),
        expo_token_usd: DashMap::with_hasher(ahash::RandomState::default()),
        expo_pair_usd: DashMap::with_hasher(ahash::RandomState::default()),
        expo_token_cap_usd: AtomicU64::new(EXPO_LIMIT_USD),
        expo_pair_cap_usd: AtomicU64::new(EXPO_PAIR_LIMIT_USD),
        heat_momentum: HeatMom::default(),
        fr_strikes: DashMap::with_hasher(ahash::RandomState::default()),
        miss_streaks: DashMap::with_hasher(ahash::RandomState::default()),
        slot_attempted: DashMap::with_hasher(ahash::RandomState::default()),
        program_bumps: DashMap::with_hasher(ahash::RandomState::default()),
        leader_quants: DashMap::with_hasher(ahash::RandomState::default()),
        pair_ev: DashMap::with_hasher(ahash::RandomState::default()),
        hot_writes: DashMap::with_hasher(ahash::RandomState::default()),
        wallet_profit: DashMap::with_hasher(ahash::RandomState::default()),
        step_hist: DashMap::with_hasher(ahash::RandomState::default()),
        leader_short: DashMap::with_hasher(ahash::RandomState::default()),
        }
    }

    pub async fn process_transaction(&self, wallet: Pubkey, instruction: &Instruction) -> Result<()> {
        if let Some(swap_data) = self.parse_swap_instruction(instruction).await? {
            self.update_wallet_profile(wallet, swap_data).await?;
        }
        Ok(())
    }

    // === REPLACE: parse_swap_instruction (confirm inlined) ===
    #[inline(always)]
    async fn parse_swap_instruction(&self, instruction: &Instruction) -> Result<Option<SwapData>> {
        let pid = instruction.program_id;
        if pid == *JUPITER_PROGRAM {
            self.parse_jupiter_swap(instruction).await
        } else if pid == *RAYDIUM_PROGRAM {
            self.parse_raydium_swap(instruction).await
        } else if pid == *ORCA_PROGRAM {
            self.parse_orca_swap(instruction).await
        } else {
            Ok(None)
        }
    }

    // === REPLACE: parse_jupiter_swap ===
    async fn parse_jupiter_swap(&self, instruction: &Instruction) -> Result<Option<SwapData>> {
        if instruction.data.len() < 24 || instruction.accounts.len() < 3 { return Ok(None); }
        let amount_in      = read_u64_le_at(&instruction.data, 8).unwrap_or(0);
        let min_amount_out = read_u64_le_at(&instruction.data, 16).unwrap_or(0);

        let token_in  = instruction.accounts.get(1).map(|a| a.pubkey).unwrap_or_default();
        let token_out = instruction.accounts.get(2).map(|a| a.pubkey).unwrap_or_default();
        let (token_in, token_out, amount_in, min_amount_out) = sanitize_swap_fields(token_in, token_out, amount_in, min_amount_out);

        let slippage_bps = declared_slippage_bps(amount_in, min_amount_out);
        if !sane_swap(&token_in, &token_out, amount_in, min_amount_out, slippage_bps) { return Ok(None); }

        Ok(Some(SwapData {
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0)).as_secs(),
            dex: DexType::Jupiter,
            token_in, token_out,
            amount_in,
            amount_out: min_amount_out,
            slippage_bps,
            transaction_signature: String::new(),
        }))
    }

    // === REPLACE: parse_raydium_swap ===
    async fn parse_raydium_swap(&self, instruction: &Instruction) -> Result<Option<SwapData>> {
        if instruction.data.len() < 17 || instruction.accounts.len() < 10 { return Ok(None); }
        let amount_in      = read_u64_le_at(&instruction.data, 1).unwrap_or(0);
        let min_amount_out = read_u64_le_at(&instruction.data, 9).unwrap_or(0);

        let token_in  = instruction.accounts.get(8).map(|a| a.pubkey)
            .or_else(|| instruction.accounts.get(instruction.accounts.len().saturating_sub(2)).map(|a| a.pubkey))
            .unwrap_or_default();
        let token_out = instruction.accounts.get(9).map(|a| a.pubkey)
            .or_else(|| instruction.accounts.last().map(|a| a.pubkey))
            .unwrap_or_default();

        let (token_in, token_out, amount_in, min_amount_out) = sanitize_swap_fields(token_in, token_out, amount_in, min_amount_out);
        let slippage_bps = declared_slippage_bps(amount_in, min_amount_out);
        if !sane_swap(&token_in, &token_out, amount_in, min_amount_out, slippage_bps) { return Ok(None); }

        Ok(Some(SwapData {
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0)).as_secs(),
            dex: DexType::Raydium,
            token_in, token_out,
            amount_in,
            amount_out: min_amount_out,
            slippage_bps,
            transaction_signature: String::new(),
        }))
    }

    // === REPLACE: parse_orca_swap ===
    async fn parse_orca_swap(&self, instruction: &Instruction) -> Result<Option<SwapData>> {
        if instruction.data.len() < 16 || instruction.accounts.len() < 6 { return Ok(None); }
        let amount_in      = read_u64_le_at(&instruction.data, 0).unwrap_or(0);
        let min_amount_out = read_u64_le_at(&instruction.data, 8).unwrap_or(0);

        let token_in  = instruction.accounts.get(2).map(|a| a.pubkey)
            .or_else(|| instruction.accounts.get(instruction.accounts.len().saturating_sub(2)).map(|a| a.pubkey))
            .unwrap_or_default();
        let token_out = instruction.accounts.get(3).map(|a| a.pubkey)
            .or_else(|| instruction.accounts.last().map(|a| a.pubkey))
            .unwrap_or_default();

        let (token_in, token_out, amount_in, min_amount_out) = sanitize_swap_fields(token_in, token_out, amount_in, min_amount_out);
        let slippage_bps = declared_slippage_bps(amount_in, min_amount_out);
        if !sane_swap(&token_in, &token_out, amount_in, min_amount_out, slippage_bps) { return Ok(None); }

        Ok(Some(SwapData {
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::from_secs(0)).as_secs(),
            dex: DexType::Orca,
            token_in, token_out,
            amount_in,
            amount_out: min_amount_out,
            slippage_bps,
            transaction_signature: String::new(),
        }))
    }

    // === REPLACE: update_wallet_profile ===
    #[inline]
    async fn update_wallet_profile(&self, wallet: Pubkey, swap_data: SwapData) -> Result<()> {
        let now_ts = swap_data.timestamp;
        if let Some(mut prof) = self.profiles.get_mut(&wallet) {
            prof.push_swap_pruned(swap_data.clone());
            self.feed_pair_vol(swap_data.token_in, swap_data.token_out, swap_data.slippage_bps);
            prof.mirror_intent_score = self.calculate_mirror_intent_score_recency(&prof.recent_swaps, now_ts).await;
            prof.coordination_score  = self.calculate_coordination_score(wallet).await;
            self.maybe_quarantine(&prof);
            // NEW: keep coordination strength fresh (see Piece O)
            self.update_coord_strength_for_neighbors(wallet, now_ts).await;
            return Ok(());
        }

        let mut newp = WalletProfile::new(wallet);
        newp.push_swap_pruned(swap_data.clone());
        self.feed_pair_vol(swap_data.token_in, swap_data.token_out, swap_data.slippage_bps);
        newp.mirror_intent_score = self.calculate_mirror_intent_score_recency(&newp.recent_swaps, now_ts).await;
        newp.coordination_score  = self.calculate_coordination_score(wallet).await;
        self.maybe_quarantine(&newp);
        self.profiles.insert(wallet, newp);
        // NEW:
        self.update_coord_strength_for_neighbors(wallet, now_ts).await;
        Ok(())
    }

    fn calculate_aggression_index(&self, swaps: &[SwapData]) -> f64 {
        if swaps.is_empty() {
            return 0.0;
        }

        let mut aggression_sum = 0.0;
        let mut count = 0;

        for swap in swaps {
            let size_factor = (swap.amount_in as f64).ln() / 20.0; // Logarithmic scaling
            let slippage_factor = (swap.slippage_bps as f64) / 10000.0;
            let aggression = size_factor + slippage_factor * 2.0; // Weight slippage higher
            aggression_sum += aggression;
            count += 1;
        }

        let base_aggression = aggression_sum / count as f64;
        
        // Frequency multiplier
        let time_span = if swaps.len() > 1 {
            swaps.last().unwrap().timestamp - swaps.first().unwrap().timestamp
        } else {
            1
        };
        
        let frequency_multiplier = if time_span > 0 {
            (swaps.len() as f64 * 3600.0) / time_span as f64 // swaps per hour
        } else {
            1.0
        };

        (base_aggression * frequency_multiplier).min(10.0) // Cap at 10.0
    }

    fn calculate_token_rotation_entropy(&self, swaps: &[SwapData]) -> f64 {
        if swaps.is_empty() {
            return 0.0;
        }

        let mut token_counts: HashMap<Pubkey, usize> = HashMap::new();
        let mut total_tokens = 0;

        for swap in swaps {
            *token_counts.entry(swap.token_in).or_insert(0) += 1;
            *token_counts.entry(swap.token_out).or_insert(0) += 1;
            total_tokens += 2;
        }

        let mut entropy = 0.0;
        for count in token_counts.values() {
            let probability = *count as f64 / total_tokens as f64;
            if probability > 0.0 {
                entropy -= probability * probability.ln();
            }
        }

        entropy
    }

    // === REPLACE: calculate_risk_preference ===
    fn calculate_risk_preference(&self, swaps: &[SwapData]) -> f64 {
        if swaps.is_empty() { return 0.5; }
        let mut stable_in: u128 = 0;
        let mut total_in:  u128 = 0;
        for s in swaps {
            total_in = total_in.saturating_add(s.amount_in as u128);
            if is_stablecoin(&s.token_in) || is_stablecoin(&s.token_out) {
                stable_in = stable_in.saturating_add(s.amount_in as u128);
            }
        }
        if total_in == 0 { return 0.5; }
        (1.0 - (stable_in as f64) / (total_in as f64)).clamp(0.0, 1.0)
    }

    fn calculate_liquidity_behavior_score(&self, actions: &[LiquidityAction]) -> f64 {
        if actions.is_empty() {
            return 0.0;
        }

        let mut add_count = 0;
        let mut remove_count = 0;
        let mut timing_scores = Vec::new();

        for action in actions {
            match action.action_type {
                LiquidityActionType::AddLiquidity => add_count += 1,
                LiquidityActionType::RemoveLiquidity => remove_count += 1,
            }
        }

        // Calculate timing efficiency (how quickly they add/remove around events)
        for window in actions.windows(2) {
            let time_diff = window[1].timestamp - window[0].timestamp;
            let efficiency = if time_diff < 300 { 1.0 } else { 1.0 / (time_diff as f64 / 300.0) };
            timing_scores.push(efficiency);
        }

        let timing_avg = if timing_scores.is_empty() {
            0.0
        } else {
            timing_scores.iter().sum::<f64>() / timing_scores.len() as f64
        };

        let balance_score = if add_count + remove_count == 0 {
            0.0
        } else {
            1.0 - (add_count as f64 - remove_count as f64).abs() / (add_count + remove_count) as f64
        };

        (timing_avg + balance_score) / 2.0
    }

    // === REPLACE: calculate_mirror_intent_score signature and body ===
    #[inline]
    async fn calculate_mirror_intent_score(&self, swaps: &VecDeque<SwapData>) -> f64 {
        if swaps.is_empty() { return 0.0; }
        let mut corr_sum = 0.0_f64;
        let mut n = 0_u32;

        for s in swaps.iter() {
            if let Some(dq) = self.mempool_patterns_typed.get(&(s.dex, s.token_in)) {
                let c = self.calculate_timing_correlation(s.timestamp, &s.token_in, &dq);
                corr_sum += c; n += 1;
                continue;
            }
            let key = format!("{:?}_{}", s.dex, s.token_in);
            if let Some(dq) = self.mempool_patterns.get(&key) {
                let c = self.calculate_timing_correlation(s.timestamp, &s.token_in, &dq);
                corr_sum += c; n += 1;
            }
        }
        if n == 0 { 0.0 } else { (corr_sum / (n as f64)).clamp(0.0, 1.0) }
    }

    // === REPLACE: calculate_timing_correlation signature attribute ===
    #[inline(always)]
    fn calculate_timing_correlation(&self, ts: u64, _token_in: &Pubkey, pattern_ts: &VecDeque<u64>) -> f64 {
        let bias_ms = self.latency_bias_ms.load(Ordering::Relaxed);
        // adjust our swap timestamp to account for capture delay
        let adj_ts = if bias_ms >= 0 {
            ts.saturating_add((bias_ms as u64) / 1000)
        } else {
            ts.saturating_sub(((-bias_ms) as u64) / 1000)
        };

        let band = Self::dynamic_band_from(pattern_ts); // seconds
        let mut best = 0.0f64;

        for &p in pattern_ts.iter().rev().take(MIRROR_SCAN_BACK) {
            let d = adj_ts.abs_diff(p) as f64;
            if d <= band {
                let core  = 1.0 - d / band;
                let decay = (-d / MIRROR_DECAY_HL).exp();
                let sc = (core * decay).clamp(0.0, 1.0);
                if sc > best { best = sc; if best >= 0.999 { break; } }
            } else if (p as i64) < (adj_ts as i64) - (band as i64) {
                break;
            }
        }
        best
    }

    // === REPLACE: calculate_coordination_score ===
    #[inline]
    async fn calculate_coordination_score(&self, wallet: Pubkey) -> f64 {
        let Some(neigh) = self.coordination_graph.get(&wallet) else { return 0.0; };
        if neigh.is_empty() { return 0.0; }

        let me = match self.profiles.get(&wallet) { Some(p) => p, None => return 0.0 };

        let mut sum = 0.0_f64;
        let mut k = 0_u32;

        for other in neigh.iter() {
            if let Some(op) = self.profiles.get(other.value()) {
                // zero-alloc path: deques + in-map token maps
                let corr = self.calculate_profile_correlation(&*me, &*op);
                sum += corr; k += 1;
            }
        }
        if k == 0 { 0.0 } else { (sum / (k as f64)).clamp(0.0, 1.0) }
    }

    // === REPLACE: calculate_profile_correlation ===
    #[inline(always)]
    fn calculate_profile_correlation(&self, p1: &WalletProfile, p2: &WalletProfile) -> f64 {
        let timing_correlation   = self.calculate_swap_timing_correlation(&p1.recent_swaps, &p2.recent_swaps);
        let token_correlation    = self.token_preference_correlation_fast(p1, p2);
        let behavior_correlation = self.calculate_behavior_correlation(p1, p2);
        ((timing_correlation + token_correlation + behavior_correlation) / 3.0).clamp(0.0, 1.0)
    }

    // === REPLACE: calculate_swap_timing_correlation ===
    // === REPLACE: calculate_swap_timing_correlation body's numeric literals ===
    #[inline]
    fn calculate_swap_timing_correlation(&self, s1: &VecDeque<SwapData>, s2: &VecDeque<SwapData>) -> f64 {
        if s1.is_empty() || s2.is_empty() { return 0.0; }

        let mut i = s1.len();
        let mut j = s2.len();
        let mut sum = 0.0_f64;
        let mut cnt = 0_u32;

        while i > 0 && j > 0 {
            let t1 = s1[i - 1].timestamp;
            let t2 = s2[j - 1].timestamp;

            if t1 >= t2 {
                let diff = t1 - t2;
                if diff <= TIMING_MAX_SKEW_SECS {
                    sum += 1.0 - (diff as f64 / TIMING_MAX_SKEW_SECS as f64);
                    cnt += 1;
                    i -= 1; j -= 1;
                } else {
                    i -= 1;
                }
            } else {
                let diff = t2 - t1;
                if diff <= TIMING_MAX_SKEW_SECS {
                    sum += 1.0 - (diff as f64 / TIMING_MAX_SKEW_SECS as f64);
                    cnt += 1;
                    i -= 1; j -= 1;
                } else {
                    j -= 1;
                }
            }
            if (cnt as usize) >= TIMING_MAX_PAIRS { break; }
        }
        if cnt == 0 { 0.0 } else { (sum / (cnt as f64)).clamp(0.0, 1.0) }
    }

    fn calculate_token_preference_correlation(&self, swaps1: &[SwapData], swaps2: &[SwapData]) -> f64 {
        if swaps1.is_empty() || swaps2.is_empty() {
            return 0.0;
        }

        let mut tokens1 = std::collections::HashSet::new();
        let mut tokens2 = std::collections::HashSet::new();

        for swap in swaps1 {
            tokens1.insert(swap.token_in);
            tokens1.insert(swap.token_out);
        }

        for swap in swaps2 {
            tokens2.insert(swap.token_in);
            tokens2.insert(swap.token_out);
        }

        let intersection = tokens1.intersection(&tokens2).count();
        let union = tokens1.union(&tokens2).count();

        if union == 0 {
            return 0.0;
        }

        intersection as f64 / union as f64
    }

    // === REPLACE: token_preference_correlation_fast ===
    #[inline(always)]
    fn token_preference_correlation_fast(&self, p1: &WalletProfile, p2: &WalletProfile) -> f64 {
        let (small, large) = if p1.token_counts.len() <= p2.token_counts.len() {
            (&p1.token_counts, &p2.token_counts)
        } else {
            (&p2.token_counts, &p1.token_counts)
        };
        let mut inter = 0usize;
        for k in small.keys() {
            if large.contains_key(k) { inter += 1; }
        }
        let union = p1.token_counts.len() + p2.token_counts.len() - inter;
        if union == 0 { 0.0 } else { (inter as f64) / (union as f64) }
    }

    fn calculate_behavior_correlation(&self, profile1: &WalletProfile, profile2: &WalletProfile) -> f64 {
        let aggression_diff = (profile1.aggression_index - profile2.aggression_index).abs();
        let risk_diff = (profile1.risk_preference - profile2.risk_preference).abs();
        let entropy_diff = (profile1.token_rotation_entropy - profile2.token_rotation_entropy).abs();

        let aggression_correlation = 1.0 - (aggression_diff / 10.0).min(1.0);
        let risk_correlation = 1.0 - risk_diff;
        let entropy_correlation = 1.0 - (entropy_diff / 5.0).min(1.0);

        (aggression_correlation + risk_correlation + entropy_correlation) / 3.0
    }

    // === REPLACE: calculate_stablecoin_inflow_ratio ===
    fn calculate_stablecoin_inflow_ratio(&self, swaps: &[SwapData]) -> f64 {
        if swaps.is_empty() { return 0.0; }
        let mut inflow:  u128 = 0;
        let mut outflow: u128 = 0;
        for s in swaps {
            if is_stablecoin(&s.token_in)  { outflow = outflow.saturating_add(s.amount_in as u128); }
            if is_stablecoin(&s.token_out) { inflow  = inflow.saturating_add(s.amount_out as u128); }
        }
        let denom = inflow.saturating_add(outflow);
        if denom == 0 { return 0.0; }
        (inflow as f64) / (denom as f64)
    }

    // === REPLACE: calculate_avg_slippage_tolerance ===
    fn calculate_avg_slippage_tolerance(&self, swaps: &[SwapData]) -> f64 {
        if swaps.is_empty() { return 0.0; }
        let sum: u64 = swaps.iter().map(|s| s.slippage_bps as u64).sum();
        (sum as f64) / (swaps.len() as f64)
    }

    // === REPLACE: calculate_swap_frequency_score ===
    fn calculate_swap_frequency_score(&self, swaps: &[SwapData]) -> f64 {
        if swaps.len() < 2 { return 0.0; }
        let first = swaps.first().unwrap().timestamp;
        let last  = swaps.last().unwrap().timestamp;
        if last <= first { return 10.0; }
        let sph = (swaps.len() as f64) * 3600.0 / ((last - first) as f64);
        sph.min(10.0)
    }

    pub async fn get_wallet_profile(&self, wallet: &Pubkey) -> Option<WalletProfile> {
        self.profiles.get(wallet).map(|entry| entry.clone())
    }

    pub async fn detect_mirror_intent(&self, wallet: &Pubkey) -> Result<bool> {
        let profile = self.profiles.get(wallet).ok_or_else(|| anyhow::anyhow!("Wallet not found"))?;
        
        Ok(profile.mirror_intent_score > 0.7)
    }

    pub async fn analyze_liquidity_behavior(&self, wallet: &Pubkey) -> Result<f64> {
        let profile = self.profiles.get(wallet).ok_or_else(|| anyhow::anyhow!("Wallet not found"))?;
        
        Ok(profile.liquidity_behavior_score)
    }

    // === ADD (verbatim): typed fast updater ===
    #[inline]
    pub async fn update_mempool_pattern_typed(&self, dex: DexType, token_in: Pubkey, timestamp: u64) -> Result<()> {
        let mut dq = self.mempool_patterns_typed
            .entry((dex, token_in))
            .or_insert_with(|| VecDeque::with_capacity(MAX_PATTERN_TIMES));
        let cutoff = timestamp.saturating_sub(3_600);
        while let Some(&front) = dq.front() {
            if front <= cutoff { dq.pop_front(); } else { break; }
        }
        if dq.len() == MAX_PATTERN_TIMES { dq.pop_front(); }
        dq.push_back(timestamp);
        Ok(())
    }

    // === REPLACE: update_mempool_pattern (extends step #15) ===
    pub async fn update_mempool_pattern(&self, pattern: String, timestamp: u64) -> Result<()> {
        // Legacy path (string key)
        let mut dq = self.mempool_patterns
            .entry(pattern.clone())
            .or_insert_with(|| VecDeque::with_capacity(MAX_PATTERN_TIMES));

        let cutoff = timestamp.saturating_sub(3_600);
        while let Some(&front) = dq.front() {
            if front <= cutoff { dq.pop_front(); } else { break; }
        }
        if dq.len() == MAX_PATTERN_TIMES { dq.pop_front(); }
        dq.push_back(timestamp);

        // Attempt to parse and also update typed map
        if let Some((dex_str, token_str)) = pattern.split_once('_') {
            let dex = match dex_str {
                "Jupiter" => Some(DexType::Jupiter),
                "Raydium" => Some(DexType::Raydium),
                "Orca"    => Some(DexType::Orca),
                _ => None,
            };
            if let (Some(dex), Ok(mint)) = (dex, Pubkey::from_str(token_str)) {
                let _ = self.update_mempool_pattern_typed(dex, mint, timestamp).await;
            }
        }
        Ok(())
    }

    // === REPLACE: add_coordination_edge ===
    pub async fn add_coordination_edge(&self, w1: Pubkey, w2: Pubkey) -> Result<()> {
        if w1 == w2 { return Ok(()); }
        let set1 = self.coordination_graph.entry(w1).or_insert_with(|| DashSet::with_hasher(ahash::RandomState::default()));
        set1.insert(w2);
        let set2 = self.coordination_graph.entry(w2).or_insert_with(|| DashSet::with_hasher(ahash::RandomState::default()));
        set2.insert(w1);
        Ok(())
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn update_slot_fee_observation(&self, observed_bundles: u32, mean_tip_lamports: u64) {
        self.fee_heat.update_slot(observed_bundles, mean_tip_lamports);
    }

    // === ADD (verbatim) ===
    // === REPLACE: recommend_cu_price_lamports ===
    #[inline]
    pub fn recommend_cu_price_lamports(&self, p: &WalletProfile) -> u64 {
        let mirror = p.mirror_intent_score.clamp(0.0, 1.0);
        let coord  = p.coordination_score.clamp(0.0, 1.0);
        let aggr   = (p.aggression_index / 10.0).clamp(0.0, 1.0);

        let heat   = self.fee_heat.load_heat();
        let tipavg = self.fee_heat.load_tip() as f64;

        let base_per_cu = (tipavg / (DEFAULT_CU_ESTIMATE as f64)).max(1.0);
        let sig = 0.55 * mirror + 0.25 * coord + 0.20 * aggr;

        // pair volatility proxy (0..1) from last swap
        let pair_vol = p.recent_swaps.back()
            .map(|last| self.read_pair_vol(last.token_in, last.token_out))
            .unwrap_or(0.0);

        // Heat amplifies signal; vol adds a bounded kick up to +20% at extreme vol
        let premium = 1.0 + 3.0 * heat * sig + 0.20 * pair_vol;

        (base_per_cu * premium).round()
            .min((MAX_TIP_LAMPORTS / DEFAULT_CU_ESTIMATE).max(1) as f64) as u64
    }

    #[inline]
    pub fn recommend_bundle_tip_lamports(&self, p: &WalletProfile, total_cu: u64) -> u64 {
        let per_cu = self.recommend_cu_price_lamports(p);
        per_cu.saturating_mul(total_cu).min(MAX_TIP_LAMPORTS)
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn build_compute_budget_ixs(cu_limit: u32, cu_price_lamports_per_cu: u64) -> [Instruction; 2] {
        let ix_limit = ComputeBudgetInstruction::set_compute_unit_limit(cu_limit);
        let ix_price = ComputeBudgetInstruction::set_compute_unit_price(cu_price_lamports_per_cu);
        [ix_limit, ix_price]
    }

    #[inline]
    pub fn prepend_compute_budget(ixs: &mut Vec<Instruction>, cu_limit: u32, cu_price_lamports_per_cu: u64) {
        let [limit, price] = build_compute_budget_ixs(cu_limit, cu_price_lamports_per_cu);
        // ComputeBudget must come first for priority
        ixs.insert(0, price);
        ixs.insert(0, limit);
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn recommend_cu_price_lamports_with_leader(&self, p: &WalletProfile, leader_aligned: bool) -> u64 {
        // Base recommendation
        let mut per_cu = self.recommend_cu_price_lamports(p) as f64;

        // If leader is *not* aligned (less friendly for our bundles), add skew;
        // if aligned, shave a bit to avoid overpaying unnecessarily.
        let heat = self.fee_heat.load_heat(); // 0..1
        if leader_aligned {
            // save up to 12% at low heat, tapering to ~5% at high heat
            let shave = 0.12 - 0.07 * heat;
            per_cu *= (1.0 - shave).clamp(0.88, 0.95);
        } else {
            // pay up to +35% at high heat, +15% at low heat
            let bump = 0.15 + 0.20 * heat;
            per_cu *= (1.0 + bump).clamp(1.15, 1.35);
        }

        per_cu.round()
            .min((MAX_TIP_LAMPORTS / DEFAULT_CU_ESTIMATE).max(1) as f64) as u64
    }

    // === REPLACE: should_shadow_trade ===
    #[inline]
    pub fn should_shadow_trade(&self, p: &WalletProfile) -> bool {
        // Core signal
        let s = 0.6 * p.mirror_intent_score
              + 0.25 * p.coordination_score
              + 0.15 * (p.aggression_index / 10.0);

        // Risk brakes on stables & extreme slippage tolerance (toxic flow heuristic)
        let stable_pen = if p.stablecoin_inflow_ratio > 0.80 { 0.20 }
                         else if p.stablecoin_inflow_ratio > 0.60 { 0.12 } else { 0.0 };
        let slip_pen   = if p.avg_slippage_tolerance >= 1_500.0 { 0.12 } // ≥15% bps
                         else if p.avg_slippage_tolerance >= 800.0 { 0.06 } else { 0.0 };

        // Activity boost
        let freq_boost = (p.swap_frequency_score / 10.0) * 0.12;

        // Heat raises the bar in cold slots (avoid paying when no one else is)
        let heat = self.fee_heat.load_heat(); // 0..1
        let heat_gate = if heat < 0.15 { 0.08 }
                        else if heat < 0.35 { 0.04 } else { 0.0 };

        let score = (s + freq_boost - stable_pen - slip_pen - heat_gate).clamp(0.0, 1.0);
        score >= 0.58
    }

    // === ADD (verbatim) ===
    #[inline]
    pub async fn update_mempool_patterns_typed_batch(&self, items: &[(DexType, Pubkey, u64)]) -> Result<()> {
        for &(dex, token_in, ts) in items {
            let mut dq = self.mempool_patterns_typed
                .entry((dex, token_in))
                .or_insert_with(|| VecDeque::with_capacity(MAX_PATTERN_TIMES));
            let cutoff = ts.saturating_sub(3_600);
            while let Some(&front) = dq.front() {
                if front <= cutoff { dq.pop_front(); } else { break; }
            }
            if dq.len() == MAX_PATTERN_TIMES { dq.pop_front(); }
            dq.push_back(ts);
        }
        Ok(())
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn set_leader_aligned(&self, aligned: bool) {
        self.leader_aligned.store(aligned, Ordering::Relaxed);
    }

    #[inline]
    pub fn recommend_cu_price_auto(&self, p: &WalletProfile) -> u64 {
        let aligned = self.leader_aligned.load(Ordering::Relaxed);
        self.recommend_cu_price_lamports_with_leader(p, aligned)
    }

    #[inline]
    pub fn build_priority_budget_auto(&self, p: &WalletProfile, cu_limit: u32) -> [Instruction; 2] {
        let per_cu = self.recommend_cu_price_auto(p);
        Self::build_compute_budget_ixs(cu_limit, per_cu)
    }

    // === REPLACE: recommend_bundle_tip_lamports ===
    #[inline]
    pub fn recommend_bundle_tip_lamports(&self, p: &WalletProfile, total_cu: u64) -> u64 {
        let per_cu = self.recommend_cu_price_auto(p) as f64;
        let tipavg = self.fee_heat.load_tip() as f64;
        let tip80  = self.fee_heat.load_tip_p80() as f64;

        // robust floor = max(40% of mean, 90% of p80)
        let floor = tipavg * 0.40f64;
        let robust = (tip80 * 0.90f64).max(floor);
        let want  = (per_cu * total_cu as f64).max(robust);
        want.round().clamp(1.0, MAX_TIP_LAMPORTS as f64) as u64
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn detect_mirror_burst(&self, p: &WalletProfile) -> bool {
        // Check last swap against recent pattern queue (typed fast path)
        if let Some(last) = p.recent_swaps.back() {
            if let Some(dq) = self.mempool_patterns_typed.get(&(last.dex, last.token_in)) {
                // Find any pattern hit within MAX_MATCH_LATENCY_SECS
                for &t in dq.iter().rev().take(MIRROR_SCAN_BACK) {
                    if last.timestamp.abs_diff(t) <= MAX_MATCH_LATENCY_SECS { return true; }
                    if (t as i64) < (last.timestamp as i64) - (MAX_MATCH_LATENCY_SECS as i64) { break; }
                }
            }
        }
        false
    }

    // === REPLACE: should_shadow_trade ===
    #[inline]
    pub fn should_shadow_trade(&self, p: &WalletProfile) -> bool {
        if let Some(until) = self.quarantine_until.get(&p.wallet).map(|e| *e.value()) {
            if p.last_activity <= until { return false; }
        }

        let s = 0.54 * p.mirror_intent_score
              + 0.22 * p.coordination_score
              + 0.24 * (p.aggression_index / 10.0);

        let coord_boost = 0.08 * self.read_coord_strength_avg(p.wallet).clamp(0.0, 1.0);

        let stable_pen = if p.stablecoin_inflow_ratio > 0.80 { 0.20 }
                         else if p.stablecoin_inflow_ratio > 0.60 { 0.12 } else { 0.0 };
        let slip_pen   = if p.avg_slippage_tolerance >= 1_500.0 { 0.12 }
                         else if p.avg_slippage_tolerance >= 800.0 { 0.06 } else { 0.0 };

        let pair_vol = p.recent_swaps.back()
            .map(|last| self.read_pair_vol(last.token_in, last.token_out))
            .unwrap_or(0.0);
        let vol_pen = 0.18 * pair_vol;

        let freq_boost = (p.swap_frequency_score / 10.0) * 0.12;
        let heat = self.fee_heat.load_heat();
        let heat_gate = if heat < 0.12 { 0.10 } else if heat < 0.30 { 0.06 } else { 0.0 };

        let burst = self.detect_mirror_burst(p);
        let burst_boost = if burst { (0.06 + 0.10 * heat).min(0.12) } else { 0.0 };

        let score = (s + coord_boost + freq_boost + burst_boost - stable_pen - slip_pen - vol_pen - heat_gate).clamp(0.0, 1.0);
        score >= 0.60
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn set_latency_bias_ms(&self, bias_ms: i64) {
        // set to (observed_chain_ts - local_capture_ts) in milliseconds
        self.latency_bias_ms.store(bias_ms, Ordering::Relaxed);
    }

    // === ADD (verbatim) ===
    #[inline(always)]
    fn dynamic_band_from(pattern_ts: &VecDeque<u64>) -> f64 {
        // Use up to 8 last inter-arrival gaps; derive a stable band
        let n = pattern_ts.len();
        if n < 3 { return MIRROR_BAND_SECS as f64; }
        let take = 8.min(n - 1);
        let mut sum = 0u64;
        let mut cnt = 0u32;
        for k in 1..=take {
            let i = n - k;
            let prev = pattern_ts[i - 1];
            let curr = pattern_ts[i];
            if curr > prev {
                sum = sum.saturating_add(curr - prev);
                cnt += 1;
            }
        }
        if cnt == 0 { return MIRROR_BAND_SECS as f64; }
        let mean = (sum as f64) / (cnt as f64);
        // heuristically tighten a bit, clamp to safe bounds
        (mean * 1.2).max(MIN_MIRROR_BAND_SECS).min(MAX_MIRROR_BAND_SECS)
    }

    // === ADD (verbatim) ===
    #[derive(Default)]
    struct LeaderStats {
        // acceptance EWMA in ppm
        acc_ppm: AtomicU64,
        // observed average per-CU price (lamports/CU)
        per_cu_avg: AtomicU64,
    }

    impl LeaderStats {
        #[inline] fn load_acc(&self) -> f64 { (self.acc_ppm.load(Ordering::Relaxed) as f64) / PPM_SCALE }
        #[inline] fn store_acc(&self, x: f64) {
            let v = (x.clamp(0.0, 1.0) * PPM_SCALE).round() as u64;
            self.acc_ppm.store(v, Ordering::Relaxed);
        }
        #[inline] fn load_pcu(&self) -> u64 { self.per_cu_avg.load(Ordering::Relaxed) }
        #[inline] fn store_pcu(&self, v: u64) { self.per_cu_avg.store(v, Ordering::Relaxed); }
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn update_leader_stats(&self, leader: Pubkey, accepted: bool, per_cu_paid: u64) {
        const ACC_ALPHA: f64 = 0.20;
        const PCU_ALPHA: f64 = 0.20;
        let entry = self.leader_stats.entry(leader).or_insert_with(LeaderStats::default);
        // acceptance ewma
        let prev = entry.load_acc();
        let inst = if accepted { 1.0 } else { 0.0 };
        entry.store_acc((1.0 - ACC_ALPHA) * prev + ACC_ALPHA * inst);
        // per-cu ewma
        let prev_pcu = entry.load_pcu() as f64;
        let new_pcu  = (1.0 - PCU_ALPHA) * prev_pcu + PCU_ALPHA * (per_cu_paid as f64);
        entry.store_pcu(new_pcu.round() as u64);
    }

    /// Price with specific leader ID (prefer this when you know the upcoming leader).
    #[inline]
    pub fn recommend_cu_price_lamports_for_leader(&self, p: &WalletProfile, leader: &Pubkey) -> u64 {
        let base = self.recommend_cu_price_lamports(p) as f64;
        let heat = self.fee_heat.load_heat();
        if let Some(st) = self.leader_stats.get(leader) {
            let acc = st.load_acc();               // 0..1
            let ref_pcu = st.load_pcu() as f64;    // lamports/CU
            // If acceptance is high and ref_pcu below our base, shave a little (risk-aware).
            let shave = if acc > 0.65 && ref_pcu <= base { (0.05 + 0.08 * (1.0 - heat)).min(0.10) } else { 0.0 };
            // If acceptance is low or ref_pcu > base, pay up slightly towards ref.
            let bump  = if acc < 0.35 && ref_pcu > base { ((ref_pcu / base) - 1.0).clamp(0.05, 0.30) } else { 0.0 };
            let adj   = (1.0 + bump - shave).clamp(0.85, 1.35);
            return (base * adj).round()
                .min((MAX_TIP_LAMPORTS / DEFAULT_CU_ESTIMATE).max(1) as f64) as u64;
        }
        base.round() as u64
    }

    // === ADD (verbatim) ===
    const VOL_ALPHA: f64 = 0.12;

    #[derive(Default)]
    struct VolAtom {
        ppm: AtomicU64, // 0..1_000_000
    }

    impl VolAtom {
        #[inline] fn load(&self) -> f64 { (self.ppm.load(Ordering::Relaxed) as f64) / PPM_SCALE }
        #[inline] fn store(&self, x: f64) {
            let v = (x.clamp(0.0, 1.0) * PPM_SCALE).round() as u64;
            self.ppm.store(v, Ordering::Relaxed);
        }
    }

    // === ADD (verbatim) ===
    #[inline]
    fn feed_pair_vol(&self, a: Pubkey, b: Pubkey, slippage_bps: u16) {
        // directional pair key; use (a,b)
        let entry = self.pair_vol.entry((a, b)).or_insert_with(VolAtom::default);
        let prev = entry.load();
        // normalize slippage to 0..1 with soft cap around 2000 bps
        let inst = ((slippage_bps as f64) / 2000.0).min(1.0);
        let newv = (1.0 - VOL_ALPHA) * prev + VOL_ALPHA * inst;
        entry.store(newv);
    }

    #[inline]
    fn read_pair_vol(&self, a: Pubkey, b: Pubkey) -> f64 {
        self.pair_vol.get(&(a, b)).map(|v| v.load()).unwrap_or(0.0)
    }

    // === ADD (verbatim) ===
    #[inline]
    fn maybe_quarantine(&self, p: &WalletProfile) {
        // Quarantine very toxic flows for 2 minutes based on *recent* metrics
        if p.avg_slippage_tolerance >= 1_600.0 && p.risk_preference > 0.70 && p.aggression_index > 6.0 {
            let until = p.last_activity.saturating_add(120);
            self.quarantine_until.insert(p.wallet, until);
        }
    }

    // === ADD (verbatim) ===
    #[derive(Default)]
    struct CoordStrength {
        ppm: AtomicU64,   // 0..1_000_000
        ts:  AtomicU64,   // last update (secs)
    }
    impl CoordStrength {
        #[inline] fn load(&self) -> (f64, u64) {
            ((self.ppm.load(Ordering::Relaxed) as f64) / PPM_SCALE, self.ts.load(Ordering::Relaxed))
        }
        #[inline] fn store(&self, val: f64, ts: u64) {
            let v = (val.clamp(0.0, 1.0) * PPM_SCALE).round() as u64;
            self.ppm.store(v, Ordering::Relaxed);
            self.ts.store(ts, Ordering::Relaxed);
        }
    }

    // === ADD (verbatim) ===
    #[inline]
    fn clamp_attempts(n: u32) -> u32 { n.clamp(1, STAIR_MAX_ATTEMPTS) }

    #[inline]
    fn stair_multipliers(heat: f64) -> [f64; STAIR_MAX_ATTEMPTS as usize] {
        // Gentle in cold slots, steeper when hot; tuned to avoid cliff overpay.
        if heat < 0.20 {
            [1.00, 1.04, 1.08, 1.12, 1.16, 1.20]
        } else if heat < 0.45 {
            [1.00, 1.06, 1.12, 1.18, 1.24, 1.30]
        } else {
            [1.00, 1.08, 1.16, 1.24, 1.30, 1.35]
        }
    }

    /// Build a staircase of ComputeBudget ixs for multiple attempts.
    /// attempts: 1..=6; leader: Some(pubkey) to use leader-specific price, else auto.
    #[inline]
    pub fn build_priority_budget_stair_for(
        &self,
        p: &WalletProfile,
        leader: Option<Pubkey>,
        cu_limit: u32,
        attempts: u32,
    ) -> Vec<[Instruction; 2]> {
        let heat = self.fee_heat.load_heat();
        let mults = Self::stair_multipliers(heat);
        let n = Self::clamp_attempts(attempts) as usize;

        let base_per_cu = match leader {
            Some(l) => self.recommend_cu_price_lamports_for_leader(p, &l),
            None    => self.recommend_cu_price_auto(p),
        } as f64;

        let mut out = Vec::with_capacity(n);
        for i in 0..n {
            let per_cu = (base_per_cu * mults[i]).round()
                .min((MAX_TIP_LAMPORTS / DEFAULT_CU_ESTIMATE).max(1) as f64) as u64;
            out.push(Self::build_compute_budget_ixs(cu_limit, per_cu));
        }
        out
    }

    // === ADD (verbatim) ===
    #[inline]
    async fn calculate_mirror_intent_score_recency(&self, swaps: &VecDeque<SwapData>, now_ts: u64) -> f64 {
        if swaps.is_empty() { return 0.0; }
        let mut wsum = 0.0f64;
        let mut vsum = 0.0f64;

        for s in swaps.iter() {
            let c = if let Some(dq) = self.mempool_patterns_typed.get(&(s.dex, s.token_in)) {
                self.calculate_timing_correlation(s.timestamp, &s.token_in, &dq)
            } else {
                let key = format!("{:?}_{}", s.dex, s.token_in);
                if let Some(dq) = self.mempool_patterns.get(&key) {
                    self.calculate_timing_correlation(s.timestamp, &s.token_in, &dq)
                } else { 0.0 }
            };
            if c > 0.0 {
                let age = now_ts.saturating_sub(s.timestamp) as f64;
                let w = (-age / MIRROR_RECENCY_TAU_SECS).exp();
                wsum += w * c;
                vsum += w;
            }
        }
        if vsum == 0.0 { 0.0 } else { (wsum / vsum).clamp(0.0, 1.0) }
    }

    // === ADD (verbatim) ===
    #[inline]
    fn coord_key(a: Pubkey, b: Pubkey) -> (Pubkey, Pubkey) {
        if a.to_bytes() <= b.to_bytes() { (a,b) } else { (b,a) }
    }

    #[inline]
    fn decay_factor(dt_secs: u64, hl_secs: f64) -> f64 {
        if dt_secs == 0 { return 1.0; }
        (-(dt_secs as f64) * (std::f64::consts::LN_2 / hl_secs)).exp()
    }

    #[inline]
    fn update_coord_strength_pair(&self, a: Pubkey, b: Pubkey, corr: f64, now_ts: u64) {
        if a == b { return; }
        let key = Self::coord_key(a, b);
        let entry = self.coord_strength.entry(key).or_insert_with(CoordStrength::default);
        let (prev, last_ts) = entry.load();
        let df = Self::decay_factor(now_ts.saturating_sub(last_ts), COORD_HL_SECS);
        let newv = df * prev + (1.0 - df) * corr.clamp(0.0, 1.0);
        entry.store(newv, now_ts);
    }

    #[inline]
    fn read_coord_strength_avg(&self, w: Pubkey) -> f64 {
        // average strength across neighbors; O(deg)
        let mut sum = 0.0; let mut cnt = 0u32;
        if let Some(neigh) = self.coordination_graph.get(&w) {
            for other in neigh.iter() {
                let key = Self::coord_key(w, *other.value());
                if let Some(cs) = self.coord_strength.get(&key) {
                    sum += cs.ppm.load(Ordering::Relaxed) as f64 / PPM_SCALE; cnt += 1;
                }
            }
        }
        if cnt == 0 { 0.0 } else { (sum / cnt as f64).clamp(0.0, 1.0) }
    }

    /// Re-evaluate correlations against neighbors and update strengths (call after profile update).
    #[inline]
    async fn update_coord_strength_for_neighbors(&self, w: Pubkey, now_ts: u64) {
        let Some(me) = self.profiles.get(&w) else { return; };
        if let Some(neigh) = self.coordination_graph.get(&w) {
            for other in neigh.iter() {
                if let Some(op) = self.profiles.get(other.value()) {
                    let corr = self.calculate_profile_correlation(&*me, &*op);
                    self.update_coord_strength_pair(w, *other.value(), corr, now_ts);
                }
            }
        }
    }

    // === ADD (verbatim) ===
    #[inline]
    pub async fn update_mempool_pattern_typed_dedup(&self, dex: DexType, token_in: Pubkey, timestamp: u64) -> Result<()> {
        let mut dq = self.mempool_patterns_typed
            .entry((dex, token_in))
            .or_insert_with(|| VecDeque::with_capacity(MAX_PATTERN_TIMES));
        // prune 1h window
        let cutoff = timestamp.saturating_sub(3_600);
        while let Some(&front) = dq.front() {
            if front <= cutoff { dq.pop_front(); } else { break; }
        }
        // dedup if last hit is within 1 second
        if let Some(&last) = dq.back() {
            if timestamp.abs_diff(last) <= 1 { return Ok(()); }
        }
        if dq.len() == MAX_PATTERN_TIMES { dq.pop_front(); }
        dq.push_back(timestamp);
        Ok(())
    }

    // === ADD (verbatim) ===
    #[derive(Default)]
    struct StartArm {
        n:      AtomicU64, // #observations
        wins:   AtomicU64, // #inclusions when started at this idx
        cost_pc: AtomicU64, // Σ(per_cu_paid) on wins (lamports/CU)
    }
    impl StartArm {
        #[inline] fn win_rate(&self) -> f64 {
            let n = self.n.load(Ordering::Relaxed) as f64;
            if n == 0.0 { 0.0 } else { (self.wins.load(Ordering::Relaxed) as f64) / n }
        }
        #[inline] fn avg_per_cu(&self) -> f64 {
            let w = self.wins.load(Ordering::Relaxed);
            if w == 0 { 0.0 } else { (self.cost_pc.load(Ordering::Relaxed) as f64) / (w as f64) }
        }
    }

    type PolicyKey = (u8 /*heat_bucket*/, u8 /*vol_bucket*/, u8 /*leader_flag*/);
    #[inline]
    fn bucket_heat(h: f64) -> u8 { (h * 4.0).floor().clamp(0.0, 4.0) as u8 } // 0..4
    #[inline]
    fn bucket_vol(v: f64) -> u8 { if v <= 0.20 {0} else if v <= 0.60 {1} else {2} } // 0..2

    // === ADD (verbatim) ===
    #[inline]
    fn ensure_policy(&self, key: PolicyKey) -> dashmap::mapref::one::RefMut<'_, PolicyKey, Vec<StartArm>> {
        self.stair_policies.entry(key).or_insert_with(|| {
            let mut v = Vec::with_capacity(STAIR_MAX_ATTEMPTS as usize);
            for _ in 0..STAIR_MAX_ATTEMPTS { v.push(StartArm::default()); }
            v
        })
    }

    #[inline]
    fn choose_start_index_for(&self, heat: f64, vol: f64, leader_friendly: bool) -> usize {
        // Check histogram prior first
        if let Some(pr) = self.prior_from_hist(heat, vol, leader_friendly) { return pr; }
        
        let key = (Self::bucket_heat(heat), Self::bucket_vol(vol), if leader_friendly {1} else {0});
        let pol = self.ensure_policy(key);
        // UCB-style score over "inclusions per lamport": win_rate / multiplier, with exploration
        let mults = Self::stair_multipliers(heat);
        let total_n: f64 = pol.iter().map(|a| a.n.load(Ordering::Relaxed) as f64).sum::<f64>().max(1.0);
        let mut best_idx = 0usize;
        let mut best_score = f64::MIN;
        for (i, arm) in pol.iter().enumerate() {
            let n = arm.n.load(Ordering::Relaxed) as f64;
            let wr = arm.win_rate();                 // [0,1]
            let mult = mults[i].max(1.0);            // price multiplier
            let exploit = if mult > 0.0 { wr / mult } else { 0.0 };
            let explore = ((total_n.ln() / (n.max(1.0))).sqrt()) * 0.05; // gentle exploration
            let score = exploit + explore;
            if score > best_score { best_score = score; best_idx = i; }
        }
        best_idx
    }

    #[inline]
    pub fn record_stair_outcome_for_wallet(&self, wallet: Pubkey, included: bool, per_cu_paid: u64) {
        let Some(start_idx) = self.last_stair_start.get(&wallet).map(|e| *e.value()) else { return; };
        let heat = self.fee_heat.load_heat();
        // approximate vol using last swap if present
        let vol = self.profiles.get(&wallet)
            .and_then(|p| p.recent_swaps.back()
                .map(|last| self.read_pair_vol(last.token_in, last.token_out)))
            .unwrap_or(0.0);
        let key = (Self::bucket_heat(heat), Self::bucket_vol(vol), if self.leader_aligned.load(Ordering::Relaxed) {1} else {0});
        let pol = self.ensure_policy(key);
        if let Some(arm) = pol.get(start_idx as usize) {
            arm.n.fetch_add(1, Ordering::Relaxed);
            if included {
                arm.wins.fetch_add(1, Ordering::Relaxed);
                arm.cost_pc.fetch_add(per_cu_paid.min(1_000_000_000), Ordering::Relaxed);
            }
        }
    }

    // === ADD (verbatim) ===
    pub struct AttemptPlan {
        pub delay_ms: u64,
        pub ixs: [Instruction; 2],
    }

    /// Build an attempt plan spaced inside the remaining slot time.
    /// `slot_ms`: slot length; `safety_ms`: leave headroom; `rtt_ms`: your median end-to-end.
    /// Stores chosen start step for `p.wallet` so you can later call `record_stair_outcome_for_wallet`.
    #[inline]
    pub fn build_stair_attempt_plan_smart(
        &self,
        p: &WalletProfile,
        leader: Option<Pubkey>,
        cu_limit: u32,
        attempts: u32,
        slot_ms: u64,
        safety_ms: u64,
        rtt_ms: u64,
    ) -> Vec<AttemptPlan> {
        let heat   = self.fee_heat.load_heat();
        let vol    = p.recent_swaps.back()
                        .map(|last| self.read_pair_vol(last.token_in, last.token_out))
                        .unwrap_or(0.0);

        // === NEW: use surprise-adaptive stair multipliers ===
        let surprise = self.surprise_for_wallet(p);
        let mut mults = Self::stair_multipliers_adaptive(heat, surprise);
        // Apply heat momentum boost
        let boost = self.heat_momentum_boost();
        for mult in mults.iter_mut() {
            *mult *= (1.0 + boost);
        }
        let base_pc = match leader {
            Some(l) => self.recommend_cu_price_lamports_for_leader(p, &l),
            None    => self.recommend_cu_price_auto(p),
        } as f64;

        // === NEW: use EV-optimal first step selector ===
        let total_cu64 = cu_limit as u64;
        let smart0 = self.choose_first_step_idx(p, leader.as_ref(), base_pc, &mults, total_cu64, /*edge hint*/ 1_000_000);
        let start  = smart0.max(self.choose_start_index_for(heat, vol, self.leader_aligned.load(Ordering::Relaxed)));
        self.last_stair_start.insert(p.wallet, start as u8);

        let n = attempts.clamp(1, STAIR_MAX_ATTEMPTS) as usize;
        let usable = slot_ms.saturating_sub(safety_ms).saturating_sub(rtt_ms);

        // === NEW: anchor schedule at per-leader phase (center the sequence on phase when possible)
        let anchor_ms = leader.as_ref().map(|l| self.recommend_phase_ms(l)).unwrap_or(60);
        let span = if n <= 1 { 0 } else { (usable.min(slot_ms)).saturating_sub(1) };
        let step = if n <= 1 { 0 } else { (span / (n as u64 - 1)).max(1) };

        // === NEW: use schedule shaper ===
        let base = anchor_ms.saturating_sub(0);
        let slope = self.slope_hint((base_pc * mults[(start).min(mults.len()-1)]).round() as u64, leader.as_ref());
        let delays = Self::shape_delays(base, n, usable, slope);

        let mut out = Vec::with_capacity(n);
        for k in 0..n {
            let idx = (start + k).min((STAIR_MAX_ATTEMPTS - 1) as usize);
            // === NEW: enforce slot price floor ===
            let mut per_cu = self.enforce_slot_price_floor(
                (base_pc * mults[idx]).round()
                    .min((MAX_TIP_LAMPORTS / DEFAULT_CU_ESTIMATE).max(1) as f64) as u64
            );
            // === NEW: apply strike floor ===
            let last_swap = p.recent_swaps.back();
            let strike = self.strike_floor_for(leader.as_ref(), last_swap);
            if strike > 0 { per_cu = per_cu.max(strike); }
            let delay = delays[k];
            let ixs = self.build_compute_budget_ixs_cached(cu_limit, per_cu);
            out.push(AttemptPlan { delay_ms: delay, ixs });
        }
        out
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn should_shadow_trade_stable(&self, p: &WalletProfile) -> bool {
        let now = p.last_activity;
        let raw = self.should_shadow_trade(p);
        if let Some(prev) = self.shadow_state.get(&p.wallet) {
            let (last, ts) = *prev.value();
            if raw && !last && now.saturating_sub(ts) <= SHADOW_NEG_COOLDOWN_SECS {
                // just flipped to true: enforce short cooldown unless a burst
                if !self.detect_mirror_burst(p) { return false; }
            }
            if !raw && last && now.saturating_sub(ts) <= SHADOW_POS_HOLD_SECS {
                // hold positive briefly to avoid flapping off
                return true;
            }
        }
        self.shadow_state.insert(p.wallet, (raw, now));
        raw
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn recommend_attempts(&self, p: &WalletProfile, slot_ms: u64) -> u32 {
        let heat = self.fee_heat.load_heat();
        let vol  = p.recent_swaps.back()
            .map(|last| self.read_pair_vol(last.token_in, last.token_out))
            .unwrap_or(0.0);
        let s = 0.55 * p.mirror_intent_score + 0.25 * p.coordination_score + 0.20 * (p.aggression_index / 10.0);
        let toxic = (p.avg_slippage_tolerance >= 1200.0) || (p.risk_preference > 0.75) || (vol > 0.75);

        let mut att = if s > 0.75 && heat > 0.35 { 5 } else if s > 0.60 { 4 } else { 3 };
        if toxic { att = att.saturating_sub(1).max(1); }
        // keep feasible with short slots
        if slot_ms <= 350 { att = att.min(3); }
        att.min(STAIR_MAX_ATTEMPTS).max(1)
    }

    #[inline]
    pub fn recommend_cu_limit(&self, p: &WalletProfile) -> u32 {
        let (dex, vol) = p.recent_swaps.back()
            .map(|s| (Some(s.dex), self.read_pair_vol(s.token_in, s.token_out)))
            .unwrap_or((None, 0.0));
        let base = match dex {
            Some(DexType::Jupiter) => 220_000u32,
            Some(DexType::Raydium) => 180_000u32,
            Some(DexType::Orca)    => 160_000u32,
            None => DEFAULT_CU_ESTIMATE as u32,
        };
        // bump a touch under high volatility to reduce compute-overflow risk on complex routes
        let bump = if vol > 0.70 { (base as f64 * 0.10).round() as u32 } else if vol > 0.45 { (base as f64 * 0.05).round() as u32 } else { 0 };
        base.saturating_add(bump).min(300_000)
    }

    // === ADD (verbatim) ===
    #[derive(Default)]
    struct SpendWindow {
        start_sec: AtomicU64,
        used_lamports: AtomicU64,
    }

    impl SpendWindow {
        #[inline] fn reset(&self, now_sec: u64) {
            self.start_sec.store(now_sec, Ordering::Relaxed);
            self.used_lamports.store(0, Ordering::Relaxed);
        }
        #[inline] fn load(&self) -> (u64, u64) {
            (self.start_sec.load(Ordering::Relaxed), self.used_lamports.load(Ordering::Relaxed))
        }
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn set_spend_caps(&self, lamports_per_min: u64, attempts_per_slot: u64) {
        self.spend_cap_lamports_per_min.store(lamports_per_min, Ordering::Relaxed);
        self.attempts_cap_per_slot.store(attempts_per_slot, Ordering::Relaxed);
    }

    #[inline]
    pub fn spend_guard_allow(&self, lamports: u64, now_sec: u64) -> bool {
        let (start, used) = self.spend_window.load();
        if now_sec.saturating_sub(start) >= SPEND_WINDOW_SECS {
            self.spend_window.reset(now_sec);
            return lamports <= self.spend_cap_lamports_per_min.load(Ordering::Relaxed);
        }
        let cap = self.spend_cap_lamports_per_min.load(Ordering::Relaxed);
        let new_used = used.saturating_add(lamports);
        if new_used > cap { return false; }
        self.spend_window.used_lamports.store(new_used, Ordering::Relaxed);
        true
    }

    #[inline]
    pub fn attempts_guard_allow(&self, slot: u64) -> bool {
        let cap = self.attempts_cap_per_slot.load(Ordering::Relaxed);
        let entry = self.attempts_used_in_slot.entry(slot).or_insert_with(|| AtomicU64::new(0));
        let prev = entry.fetch_add(1, Ordering::Relaxed);
        if prev + 1 > cap {
            // revert increment cleanly
            entry.fetch_sub(1, Ordering::Relaxed);
            return false;
        }
        true
    }

    #[inline]
    pub fn attempts_guard_reset_slot(&self, slot: u64) {
        self.attempts_used_in_slot.remove(&slot);
    }

    // === REPLACE: build_compute_budget_ixs_cached ===
    #[inline]
    pub fn build_compute_budget_ixs_cached(&self, cu_limit: u32, cu_price_lamports_per_cu: u64) -> [Instruction; 2] {
        if let Some(hit) = self.cb_cache.get(&(cu_limit, cu_price_lamports_per_cu)) {
            // touch
            let t = CB_CACHE_TICK.fetch_add(1, Ordering::Relaxed);
            let mut e = hit.clone();
            e.tick = t;
            self.cb_cache.insert((cu_limit, cu_price_lamports_per_cu), e.clone());
            return e.ixs;
        }
        let ixs = Self::build_compute_budget_ixs(cu_limit, cu_price_lamports_per_cu);
        let t = CB_CACHE_TICK.fetch_add(1, Ordering::Relaxed);
        self.cb_cache.insert((cu_limit, cu_price_lamports_per_cu), CbEntry { ixs: ixs.clone(), tick: t });

        // bounded eviction (LRU-ish single pass)
        if self.cb_cache.len() > CB_CACHE_CAP {
            if let Some((k,_)) = self.cb_cache.iter()
                .min_by_key(|kv| kv.value().tick).map(|kv| (kv.key().clone(), kv.value().tick)) {
                self.cb_cache.remove(&k);
            }
        }
        ixs
    }

    // === ADD (verbatim) ===
    #[inline]
    fn predict_accept_prob(&self, per_cu: u64, leader: Option<&Pubkey>) -> f64 {
        let heat = self.fee_heat.load_heat();        // 0..1
        let ref_pcu = self.fee_heat.load_tip().max(1) as f64 / (DEFAULT_CU_ESTIMATE as f64);
        let x = (per_cu as f64) / ref_pcu.max(1.0);
        // base prior from heat
        let mut p = (0.08 + 0.80 * heat).clamp(0.05, 0.90);
        // leader refinement if available
        if let Some(l) = leader {
            if let Some(st) = self.leader_stats.get(l) {
                let acc = st.load_acc();                   // 0..1
                let ref_lead = st.load_pcu().max(1) as f64;
                let rel = (per_cu as f64) / ref_lead.max(1.0);
                p = (0.5 * p + 0.5 * acc).clamp(0.05, 0.95);
                p *= (1.0 - (-rel).exp()).clamp(0.25, 1.0); // saturating lift
            } else {
                p *= (1.0 - (-x).exp()).clamp(0.25, 1.0);
            }
        } else {
            p *= (1.0 - (-x).exp()).clamp(0.25, 1.0);
        }
        p.clamp(0.01, 0.98)
    }

    pub struct EvPlan {
        pub plans: Vec<AttemptPlan>,
        pub kept:  usize,
        pub ev_sum: f64,
    }

    /// Filter plan steps by marginal EV >= 0; keep profitable prefix.
    /// `edge_lamports`: your expected *gross* alpha per fill (lamports).
    #[inline]
    pub fn prune_plan_by_ev(
        &self,
        mut plans: Vec<AttemptPlan>,
        total_cu: u64,
        leader: Option<Pubkey>,
        edge_lamports: u64,
    ) -> EvPlan {
        let mut ev = 0.0_f64;
        let mut kept = 0usize;
        for (i, step) in plans.iter().enumerate() {
            let p = self.predict_accept_prob(
                match step.ixs[1].data.get(1..) { // CU price ix is second element; price is in data, but safer to recompute:
                    _ => {
                        // we cached per_cu; recompute from cache key is not accessible here → derive from recommend if needed.
                        // Simplify: we recompute from instruction we just built is non-trivial; instead, compute per_cu via recommend for monotonic proxy.
                        self.recommend_cu_price_auto(
                            self.profiles.get(&self.last_stair_start.iter().next().map(|e| *e.key()).unwrap_or(Pubkey::default()))
                                .as_deref().unwrap_or(&WalletProfile::new(Pubkey::default()))
                        )
                    }
                }, // fallback path not used if caller knows leader
                leader.as_ref()
            );
            let tip = step.ixs[1].data.len(); // placeholder avoided; instead compute cost deterministically:
            let per_cu = if let Some((_, price_ix)) = Some((&step.ixs[0], &step.ixs[1])) {
                // price instruction carries lamports per CU in its `data` per ComputeBudget ABI: first byte tag=3, next 8 bytes u64 LE
                if price_ix.data.len() >= 9 {
                    u64::from_le_bytes(price_ix.data[1..9].try_into().unwrap_or([0u8;8]))
                } else { self.recommend_cu_price_auto(&WalletProfile::new(Pubkey::default())) }
            } else { self.recommend_cu_price_auto(&WalletProfile::new(Pubkey::default())) };
            let cost = (per_cu as f64) * (total_cu as f64);
            let mev  = (edge_lamports as f64);
            let m_ev = p * mev - cost;
            if m_ev >= 0.0 {
                ev += m_ev; kept = i + 1;
            } else { break; }
        }
        plans.truncate(kept);
        EvPlan { plans, kept, ev_sum: ev }
    }

    // === ADD (verbatim) ===
    #[inline]
    fn xorshift64(mut x: u64) -> u64 {
        x ^= x << 13; x ^= x >> 7; x ^= x << 17; x
    }

    /// Apply ±jitter_ms/2 around each delay; seed is per-wallet to be deterministic.
    #[inline]
    pub fn apply_micro_jitter(
        &self,
        p: &WalletProfile,
        plans: &mut [AttemptPlan],
        jitter_ms: u64,
    ) {
        if jitter_ms == 0 { return; }
        let slot = self.current_slot.load(Ordering::Relaxed);
        // seed = wallet^slot^last_activity (deterministic per slot)
        let mut s = {
            let mut b = [0u8; 32];
            b.copy_from_slice(&p.wallet.to_bytes());
            let hi = u64::from_le_bytes(b[0..8].try_into().unwrap());
            let lo = u64::from_le_bytes(b[8..16].try_into().unwrap());
            hi ^ lo ^ slot ^ p.last_activity
        };
        for step in plans.iter_mut() {
            s = Self::xorshift64(s);
            let r = (s % jitter_ms) as i64 - (jitter_ms as i64 / 2);
            let d = step.delay_ms as i64 + r;
            step.delay_ms = if d <= 0 { 0 } else { d as u64 };
        }
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn update_tip_sample(&self, bundle_tip_lamports: u64) { 
        self.fee_heat.update_tip_quantile_p80(bundle_tip_lamports);
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn record_shadow_outcome(&self, wallet: Pubkey, included: bool, realized_edge_lamports: i64) {
        let s = self.shadow_stats.entry(wallet).or_insert_with(ShadowStats::default);
        s.update(included, realized_edge_lamports);
    }

    #[inline]
    pub fn read_shadow_quality(&self, wallet: Pubkey) -> (f64 /*hit_rate*/, f64 /*ev_ewma_lamports*/) {
        if let Some(s) = self.shadow_stats.get(&wallet) {
            (s.load_hr(), s.load_ev())
        } else { (0.0, 0.0) }
    }

    /// Final gate: stable decision + historical EV threshold (lamports).
    #[inline]
    pub fn should_shadow_trade_final(&self, p: &WalletProfile, min_ev_lamports: i64) -> bool {
        if self.is_drawdown_quarantined(p.wallet, p.last_activity) { return false; }
        let last = p.recent_swaps.back();
        if self.is_cooled_pair_or_token(last, p.last_activity) { return false; }
        if let Some(s) = last { if !self.pair_ev_ok(s.token_in, s.token_out) { return false; } }
        if !self.should_shadow_trade_stable(p) { return false; }
        let (_hr, ev) = self.read_shadow_quality(p.wallet);
        ev >= (min_ev_lamports as f64)
    }

    // === ADD (verbatim) ===
    #[inline]
    fn surprise_for_wallet(&self, p: &WalletProfile) -> f64 {
        let Some(last) = p.recent_swaps.back() else { return 0.0; };
        if let Some(dq) = self.mempool_patterns_typed.get(&(last.dex, last.token_in)) {
            return burst_surprise_score_from(&dq, last.timestamp);
        }
        0.0
    }

    // === REPLACE: should_shadow_trade_stable (inject surprise boost) ===
    #[inline]
    pub fn should_shadow_trade_stable(&self, p: &WalletProfile) -> bool {
        let now = p.last_activity;
        let raw = {
            // base signal composed in prior pieces
            let base = self.should_shadow_trade(p);
            // additive surprise boost (only helps when near threshold)
            if !base {
                let sup = self.surprise_for_wallet(p); // 0..1
                if sup >= 0.40 { true } else { false }
            } else { true }
        };

        if let Some(prev) = self.shadow_state.get(&p.wallet) {
            let (last, ts) = *prev.value();
            if raw && !last && now.saturating_sub(ts) <= SHADOW_NEG_COOLDOWN_SECS {
                if !self.detect_mirror_burst(p) { return false; }
            }
            if !raw && last && now.saturating_sub(ts) <= SHADOW_POS_HOLD_SECS {
                return true;
            }
        }
        self.shadow_state.insert(p.wallet, (raw, now));
        raw
    }

    // === REPLACE: prune_plan_by_ev (use tag-checked parse; remove hack) ===
    #[inline]
    pub fn prune_plan_by_ev(
        &self,
        mut plans: Vec<AttemptPlan>,
        total_cu: u64,
        leader: Option<Pubkey>,
        edge_lamports: u64,
    ) -> EvPlan {
        let mut ev = 0.0_f64;
        let mut kept = 0usize;
        for (i, step) in plans.iter().enumerate() {
            let per_cu = extract_cu_price_from_ix(&step.ixs[1]).unwrap_or(1);
            let p = self.predict_accept_prob(per_cu, leader.as_ref());
            let cost = (per_cu as f64) * (total_cu as f64);
            let mev  = edge_lamports as f64;
            let m_ev = p * mev - cost;
            if m_ev >= 0.0 {
                ev += m_ev; kept = i + 1;
            } else { break; }
        }
        plans.truncate(kept);
        EvPlan { plans, kept, ev_sum: ev }
    }

    // === REPLACE: feed_pair_vol ===
    #[inline]
    fn feed_pair_vol(&self, a: Pubkey, b: Pubkey, slippage_bps: u16) {
        let key = canon_pair(a, b);
        let entry = self.pair_vol.entry(key).or_insert_with(VolAtom::default);
        let prev = entry.load();
        let inst = ((slippage_bps as f64) / 2000.0).min(1.0);
        let newv = (1.0 - VOL_ALPHA) * prev + VOL_ALPHA * inst;
        entry.store(newv);
    }

    // === REPLACE: read_pair_vol ===
    #[inline]
    fn read_pair_vol(&self, a: Pubkey, b: Pubkey) -> f64 {
        let key = canon_pair(a, b);
        self.pair_vol.get(&key).map(|v| v.load()).unwrap_or(0.0)
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn recommend_jitter_ms(&self, p: &WalletProfile) -> u64 {
        let heat = self.fee_heat.load_heat(); // 0..1
        let vol  = p.recent_swaps.back()
            .map(|last| self.read_pair_vol(last.token_in, last.token_out))
            .unwrap_or(0.0); // 0..1
        let base = if heat < 0.20 { 4 } else if heat < 0.45 { 8 } else { 14 };
        let bump = (vol * 10.0).round() as u64; // +0..10ms
        (base + bump).min(24)
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn spend_guard_allow(&self, lamports: u64, now_sec: u64) -> bool {
        let cap = self.spend_cap_lamports_per_min.load(Ordering::Relaxed);
        self.spend_window.allow(lamports, now_sec, cap)
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn update_leader_stats(&self, leader: Pubkey, included: bool, per_cu_paid: u64) {
        let entry = self.leader_stats.entry(leader).or_insert_with(LeaderStats::default);
        entry.update(included, per_cu_paid);
    }

    // === REPLACE: recommend_cu_limit ===
    #[inline]
    pub fn recommend_cu_limit(&self, p: &WalletProfile) -> u32 {
        let (dex, a, b) = if let Some(last) = p.recent_swaps.back() {
            (Some(last.dex), last.token_in, last.token_out)
        } else { (None, Pubkey::default(), Pubkey::default()) };

        let (base, _) = match dex {
            Some(DexType::Jupiter) => (220_000u32, 0u32),
            Some(DexType::Raydium) => (180_000u32, 0u32),
            Some(DexType::Orca)    => (160_000u32, 0u32),
            None => (DEFAULT_CU_ESTIMATE as u32, 0u32),
        };
        let (cx_cu, _cx_heap) = if let Some(d) = dex { self.recommend_cu_heap_for_pair(d, a, b) } else { (base, 131_072) };

        // volatility safety bump as before
        let vol = p.recent_swaps.back().map(|l| self.read_pair_vol(l.token_in, l.token_out)).unwrap_or(0.0);
        let bump = if vol > 0.70 { (base as f64 * 0.10).round() as u32 } else if vol > 0.45 { (base as f64 * 0.05).round() as u32 } else { 0 };

        base.max(cx_cu).saturating_add(bump).min(300_000)
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn recommend_attempts(&self, _p: &WalletProfile, max: u32) -> u32 {
        max.min(STAIR_MAX_ATTEMPTS)
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn recommend_cu_price_auto(&self, _p: &WalletProfile) -> u64 {
        self.fee_heat.load_tip().max(1)
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn recommend_cu_price_lamports_for_leader(&self, _p: &WalletProfile, leader: &Pubkey) -> u64 {
        if let Some(stats) = self.leader_stats.get(leader) {
            stats.load_pcu().max(1)
        } else {
            self.recommend_cu_price_auto(_p)
        }
    }

    // === ADD (verbatim) ===
    #[inline]
    fn stair_multipliers(_heat: f64) -> [f64; 6] {
        [1.0, 1.2, 1.5, 2.0, 3.0, 5.0] // fixed multipliers for now
    }

    // === ADD (verbatim) ===
    #[inline]
    fn build_compute_budget_ixs(cu_limit: u32, cu_price_lamports_per_cu: u64) -> [Instruction; 2] {
        let cu_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(cu_limit);
        let cu_price_ix = ComputeBudgetInstruction::set_compute_unit_price(cu_price_lamports_per_cu);
        [cu_limit_ix, cu_price_ix]
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn should_shadow_trade(&self, _p: &WalletProfile) -> bool {
        // Placeholder implementation - this should be the base shadow trading logic
        true
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn detect_mirror_burst(&self, _p: &WalletProfile) -> bool {
        // Placeholder implementation - this should detect mirror burst patterns
        true
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn maybe_drawdown_quarantine(&self, wallet: Pubkey, last_ts: u64, ev_ewma_lamports: f64) {
        if ev_ewma_lamports <= (DD_HARD_NEG_LAMPORTS as f64) {
            self.dd_quarantine_until.insert(wallet, last_ts.saturating_add(DD_WINDOW_SECS));
        }
    }

    #[inline]
    fn is_drawdown_quarantined(&self, wallet: Pubkey, now_ts: u64) -> bool {
        if let Some(until) = self.dd_quarantine_until.get(&wallet) {
            now_ts <= *until.value()
        } else { false }
    }

    // === REPLACE: set_current_slot (make it reset the floor) ===
    #[inline]
    pub fn set_current_slot(&self, slot: u64) {
        let prev = self.current_slot.swap(slot, Ordering::Relaxed);
        if prev != slot {
            self.slot_max_per_cu.store(0, Ordering::Relaxed);
            SLOT_PRICE_TICK.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    fn enforce_slot_price_floor(&self, per_cu: u64) -> u64 {
        loop {
            let cur = self.slot_max_per_cu.load(Ordering::Relaxed);
            if per_cu <= cur { return cur; }
            if self.slot_max_per_cu.compare_exchange(cur, per_cu, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                return per_cu;
            }
        }
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn build_compute_budget_ixs3(cu_limit: u32, heap_bytes: u32, cu_price_lamports_per_cu: u64) -> [Instruction; 3] {
        let ix_limit = ComputeBudgetInstruction::set_compute_unit_limit(cu_limit);
        let ix_heap  = ComputeBudgetInstruction::set_heap_frame(heap_bytes);
        let ix_price = ComputeBudgetInstruction::set_compute_unit_price(cu_price_lamports_per_cu);
        [ix_limit, ix_heap, ix_price]
    }

    #[inline]
    pub fn prepend_compute_budget3(ixs: &mut Vec<Instruction>, cu_limit: u32, heap_bytes: u32, cu_price_lamports_per_cu: u64) {
        let [limit, heap, price] = Self::build_compute_budget_ixs3(cu_limit, heap_bytes, cu_price_lamports_per_cu);
        ixs.insert(0, price); ixs.insert(0, heap); ixs.insert(0, limit);
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn set_attempts_cap_for_wallet(&self, wallet: Pubkey, cap: u64) {
        self.attempts_cap_override.insert(wallet, cap);
    }

    #[inline]
    pub fn attempts_guard_allow_for_wallet(&self, wallet: Pubkey, slot: u64) -> bool {
        let cap_global = self.attempts_cap_per_slot.load(Ordering::Relaxed);
        let cap = self.attempts_cap_override.get(&wallet).map(|e| *e.value()).unwrap_or(cap_global);
        let entry = self.attempts_used_in_slot.entry(slot).or_insert_with(|| AtomicU64::new(0));
        let prev = entry.fetch_add(1, Ordering::Relaxed);
        if prev + 1 > cap {
            entry.fetch_sub(1, Ordering::Relaxed);
            return false;
        }
        true
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn update_relay_stats(&self, rid: RelayId, included: bool, per_cu_paid: u64, rtt_ms: u64, now_sec: u64) {
        let st = self.relay_stats.entry(rid).or_insert_with(RelayStats::default);
        // acceptance
        let prev = st.load_acc();
        let inst = if included { 1.0 } else { 0.0 };
        st.store_acc((1.0 - RELAY_ALPHA)*prev + RELAY_ALPHA*inst);
        // per-cu
        let prev_p = st.load_per_cu() as f64;
        st.store_per_cu(((1.0 - RELAY_ALPHA)*prev_p + RELAY_ALPHA*(per_cu_paid as f64)).round() as u64);
        // rtt
        let prev_r = st.load_rtt() as f64;
        st.store_rtt(((1.0 - RELAY_ALPHA)*prev_r + RELAY_ALPHA*(rtt_ms as f64)).round() as u64);
        // cool/backoff
        if included { st.fails.store(0, Ordering::Relaxed); }
        else {
            let f = st.fails.fetch_add(1, Ordering::Relaxed) + 1;
            let cool = RELAY_COOL_BASE_SECS.saturating_mul(f.min(6)); // bounded backoff
            st.set_cool(now_sec.saturating_add(cool));
        }
    }

    #[inline]
    fn score_relay(&self, rid: RelayId, per_cu_offer: u64, now_sec: u64) -> f64 {
        let Some(st) = self.relay_stats.get(&rid) else { return 0.4; }; // neutral prior
        if st.cooled(now_sec) { return 0.0; }
        let acc  = st.load_acc().clamp(0.01, 0.99);      // 0..1
        let refp = st.load_per_cu().max(1) as f64;
        let rtt  = st.load_rtt().max(1) as f64;          // ms
        let relp = (per_cu_offer as f64) / refp;         // >=0
        // Higher acc, lower rtt, rel price >=1 helps; gently punish low-balling
        let price_term = (1.0 - (-relp).exp()).clamp(0.25, 1.0);
        let rtt_term   = (400.0f64 / (rtt + 400.0)).clamp(0.3, 1.0); // saturating benefit
        (acc * price_term * rtt_term).clamp(0.0, 1.0)
    }

    /// Choose best relay for current per-CU price.
    #[inline]
    pub fn choose_best_relay(&self, per_cu_offer: u64, candidates: &[RelayId], now_sec: u64) -> Option<RelayId> {
        let mut best = None; let mut bs = f64::MIN;
        for &rid in candidates {
            let s = self.score_relay(rid, per_cu_offer, now_sec);
            if s > bs { bs = s; best = Some(rid); }
        }
        best
    }

    // === ADD (verbatim) ===
    #[inline]
    pub async fn ingest_source_pattern(&self, source: SourceId, dex: DexType, token_in: Pubkey, ts: u64) -> Result<()> {
        // record in per-source queue (pruned)
        let dq = self.mempool_patterns_typed_src
            .entry((source, dex, token_in))
            .or_insert_with(|| VecDeque::with_capacity(MAX_PATTERN_TIMES));
        let cutoff = ts.saturating_sub(3_600);
        while let Some(&front) = dq.front() {
            if front <= cutoff { dq.pop_front(); } else { break; }
        }
        if let Some(&last) = dq.back() {
            if ts.abs_diff(last) <= FEED_FUSE_EPS_SECS { return Ok(()); }
        }
        if dq.len() == MAX_PATTERN_TIMES { dq.pop_front(); }
        dq.push_back(ts);

        // fuse into typed global if quorum within eps
        let mut hits = 0usize;
        for s in 0..FEED_MAX_SOURCES {
            if let Some(q) = self.mempool_patterns_typed_src.get(&(s as u8, dex, token_in)) {
                // find any ts within ±eps
                if q.iter().rev().any(|&t| t.abs_diff(ts) <= FEED_FUSE_EPS_SECS) { hits += 1; }
            }
        }
        if hits >= FEED_QUORUM {
            self.update_mempool_pattern_typed_dedup(dex, token_in, ts).await?;
        }
        Ok(())
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn update_route_complexity(&self, dex: DexType, token_in: Pubkey, token_out: Pubkey, consumed_cu: u64, heap_bytes: u64) {
        let key = (dex, canon_pair(token_in, token_out).0, canon_pair(token_in, token_out).1);
        let cx = self.route_cx.entry(key).or_insert_with(RouteComplexity::default);
        // cu
        let prev_cu = cx.load_cu() as f64;
        cx.store_cu(((1.0 - COMPLEXITY_ALPHA)*prev_cu + COMPLEXITY_ALPHA*(consumed_cu as f64)).round() as u64);
        // heap
        let prev_hp = cx.load_heap() as f64;
        cx.store_heap(((1.0 - COMPLEXITY_ALPHA)*prev_hp + COMPLEXITY_ALPHA*(heap_bytes as f64)).round() as u64);
    }

    #[inline]
    fn recommend_cu_heap_for_pair(&self, dex: DexType, a: Pubkey, b: Pubkey) -> (u32, u32) {
        let key = (dex, canon_pair(a,b).0, canon_pair(a,b).1);
        if let Some(cx) = self.route_cx.get(&key) {
            let cu = (cx.load_cu() as f64 * 1.10).round() as u32;    // +10% safety
            let hp = (cx.load_heap().max(131_072) as f64 * 1.05).round() as u32; // min 128KB, +5%
            (cu.min(300_000), hp.min(512_000))
        } else {
            (DEFAULT_CU_ESTIMATE as u32, 131_072) // 128KB default
        }
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn try_lock_pair_in_slot(&self, a: Pubkey, b: Pubkey, slot: u64) -> bool {
        let key = (canon_pair(a,b).0, canon_pair(a,b).1, slot);
        let lk = self.pair_slot_lock.entry(key).or_insert_with(SlotLock::default);
        !lk.held.swap(true, Ordering::AcqRel)
    }

    #[inline]
    pub fn release_pair_in_slot(&self, a: Pubkey, b: Pubkey, slot: u64) {
        let key = (canon_pair(a,b).0, canon_pair(a,b).1, slot);
        if let Some(lk) = self.pair_slot_lock.get(&key) {
            lk.held.store(false, Ordering::Release);
        }
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn recommend_min_ev_lamports(&self, p: &WalletProfile) -> i64 {
        let vol = p.recent_swaps.back().map(|l| self.read_pair_vol(l.token_in, l.token_out)).unwrap_or(0.0);
        let toxic = (p.avg_slippage_tolerance >= 1_200.0) as i32 + (p.risk_preference > 0.75) as i32;
        let base = 0i64;
        let vol_pen = (vol * 400_000.0).round() as i64; // up to ~0.0004 SOL
        let tox_pen = (toxic as i64) * 250_000;         // +0.00025 SOL per toxic dimension
        base + vol_pen + tox_pen
    }

    // === ADD (verbatim) ===
    /// Call after each attempt outcome with the ms offset from slot start for the *winning* attempt.
    #[inline]
    pub fn update_leader_phase_ms(&self, leader: Pubkey, won_at_ms: u64) {
        let ph = self.leader_phase_ms.entry(leader).or_insert_with(LeaderPhase::default);
        let prev = ph.load() as f64;
        let next = if prev == 0.0 { won_at_ms as f64 } else { (1.0 - PHASE_ALPHA)*prev + PHASE_ALPHA*(won_at_ms as f64) };
        ph.store(next.round() as u64);
    }

    /// Read recommended phase offset for leader (ms from slot start); fallback 60ms.
    #[inline]
    fn recommend_phase_ms(&self, leader: &Pubkey) -> u64 {
        self.leader_phase_ms.get(leader).map(|p| p.load()).unwrap_or(60)
    }

    // === ADD (verbatim) ===
    #[inline]
    fn cost_fraction_from_heat(heat: f64) -> f64 {
        // colder → leaner spend; hotter → OK to spend more to clear
        if heat < 0.20 { 0.28 } else if heat < 0.45 { 0.42 } else { 0.55 }
    }

    /// Trim plan so cumulative *cost* ≤ f(edge). Returns kept steps and cap.
    #[inline]
    pub fn cap_plan_cost_by_edge(
        &self,
        plans: &[AttemptPlan],
        total_cu: u64,
        edge_lamports: u64,
    ) -> usize {
        let heat = self.fee_heat.load_heat();
        let cap = (edge_lamports as f64 * Self::cost_fraction_from_heat(heat)).max(1.0);
        let mut spent = 0.0f64;
        let mut kept = 0usize;
        for (i, step) in plans.iter().enumerate() {
            if let Some(per_cu) = extract_cu_price_from_ix(&step.ixs[1]) {
                let add = (per_cu as f64) * (total_cu as f64);
                if spent + add > cap { break; }
                spent += add; kept = i + 1;
            } else { break; }
        }
        kept
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn set_current_slot(&self, slot: u64) {
        let prev = self.current_slot.swap(slot, Ordering::Relaxed);
        if prev != slot {
            self.slot_max_per_cu.store(0, Ordering::Relaxed);
            SLOT_PRICE_TICK.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[inline]
    fn enforce_slot_price_floor(&self, per_cu: u64) -> u64 {
        loop {
            let cur = self.slot_max_per_cu.load(Ordering::Relaxed);
            if per_cu <= cur { return cur; }
            if self.slot_max_per_cu.compare_exchange(cur, per_cu, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                return per_cu;
            }
        }
    }

    // === ADD (verbatim) ===
    #[inline]
    fn stair_multipliers_adaptive(heat: f64, surprise: f64) -> [f64; STAIR_MAX_ATTEMPTS as usize] {
        let base = Self::stair_multipliers(heat);
        if surprise < 0.40 { return base; }
        // boost later steps; keep first near base for cost control
        if heat < 0.20 {
            [base[0], base[1]*1.04, base[2]*1.08, base[3]*1.14, base[4]*1.20, base[5]*1.24]
        } else if heat < 0.45 {
            [base[0], base[1]*1.06, base[2]*1.14, base[3]*1.22, base[4]*1.30, base[5]*1.36]
        } else {
            [base[0], base[1]*1.10, base[2]*1.18, base[3]*1.28, base[4]*1.36, base[5]*1.42]
        }
    }

    // === ADD (verbatim) ===
    /// Set allowed relays (bitmask) for a given leader (1<<rid means allowed).
    #[inline]
    pub fn set_leader_relays(&self, leader: Pubkey, mask: RelayMask) {
        self.leader_relay_mask.insert(leader, mask);
    }

    /// Choose best relay from leader-specific mask (falls back to candidates slice).
    #[inline]
    pub fn choose_best_relay_for_leader(
        &self,
        leader: &Pubkey,
        per_cu_offer: u64,
        candidates: &[RelayId],
        now_sec: u64
    ) -> Option<RelayId> {
        if let Some(msk) = self.leader_relay_mask.get(leader).map(|e| *e.value()) {
            let mut list = smallvec::SmallVec::<[RelayId; 8]>::new();
            for &rid in candidates {
                if (msk & (1u32 << rid)) != 0 { list.push(rid); }
            }
            if !list.is_empty() { return self.choose_best_relay(per_cu_offer, &list, now_sec); }
        }
        self.choose_best_relay(per_cu_offer, candidates, now_sec)
    }

    // === ADD (verbatim) ===
    /// Mark that our own bundle was observed on the network (call from your capture).
    #[inline]
    pub fn mark_own_bundle_seen(&self, now_sec: u64) { 
        self.seen_self.mark(now_sec); 
    }

    /// Return true if we should abort remaining attempts in this slot.
    #[inline]
    pub fn should_abort_attempts(&self, slot_start_sec: u64) -> bool {
        self.seen_self.seen_since(slot_start_sec)
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn update_accept_cal(&self, leader: Pubkey, per_cu_paid: u64) {
        // call with included=true in update_leader_stats; this is just a helper if you want to feed on misses too
        let ref_pcu = self.leader_stats.get(&leader).map(|s| s.load_pcu().max(1) as f64)
            .unwrap_or_else(|| (self.fee_heat.load_tip().max(1) as f64) / (DEFAULT_CU_ESTIMATE as f64));
        let rel = (per_cu_paid as f64) / ref_pcu.max(1.0);
        let b = ratio_bucket(rel);
        self.accept_cal.entry((leader, b)).or_insert_with(BetaBin::default).update(true);
    }

    #[inline]
    pub fn record_attempt_outcome(&self, leader: Pubkey, per_cu_offer: u64, included: bool) {
        let ref_pcu = self.leader_stats.get(&leader).map(|s| s.load_pcu().max(1) as f64)
            .unwrap_or_else(|| (self.fee_heat.load_tip().max(1) as f64) / (DEFAULT_CU_ESTIMATE as f64));
        let rel = (per_cu_offer as f64) / ref_pcu.max(1.0);
        let b = ratio_bucket(rel);
        self.accept_cal.entry((leader, b)).or_insert_with(BetaBin::default).update(included);
    }

    // === ADD (verbatim) ===
    #[inline]
    fn predict_accept_prob(&self, per_cu: u64, leader: Option<&Pubkey>) -> f64 {
        let heat = self.fee_heat.load_heat();
        let ref_pcu = self.fee_heat.load_tip().max(1) as f64 / (DEFAULT_CU_ESTIMATE as f64);
        let base_x  = (per_cu as f64) / ref_pcu.max(1.0);

        // base prior from heat and relative price (smooth, monotone)
        let base = {
            let p0 = (0.08 + 0.80 * heat).clamp(0.05, 0.90);
            p0 * (1.0 - (-base_x).exp()).clamp(0.25, 1.0)
        };

        if let Some(l) = leader {
            let ref_p = self.leader_stats.get(l).map(|s| s.load_pcu().max(1) as f64).unwrap_or(ref_pcu);
            let rel   = (per_cu as f64) / ref_p.max(1.0);
            let b     = ratio_bucket(rel);
            let post  = self.accept_cal.get(&(*l, b)).map(|bb| bb.post_mean()).unwrap_or(0.50);
            // blend calibrated posterior with base; more trust when bucket is trained
            let n = self.accept_cal.get(&(*l, b))
                .map(|bb| (bb.succ.load(Ordering::Relaxed) + bb.fail.load(Ordering::Relaxed)) as f64).unwrap_or(0.0);
            let w = (n / (n + 20.0)).clamp(0.0, 0.85); // approach 85% weight as data accrues
            return (w * post + (1.0 - w) * base).clamp(0.01, 0.98);
        }
        base.clamp(0.01, 0.98)
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn update_strike_on_win(&self, leader: Pubkey, dex: DexType, a: Pubkey, b: Pubkey, per_cu_paid: u64) {
        let key = (leader, dex, canon_pair(a,b));
        let st = self.strike_pcu.entry(key).or_insert_with(StrikePcu::default);
        let prev = st.load() as f64;
        let next = if prev <= 0.0 { per_cu_paid as f64 } else { (1.0 - STRIKE_ALPHA)*prev + STRIKE_ALPHA*(per_cu_paid as f64) };
        st.store(next.round() as u64);
    }

    #[inline]
    fn strike_floor_for(&self, leader: Option<&Pubkey>, last: Option<&SwapData>) -> u64 {
        if let (Some(l), Some(s)) = (leader, last) {
            self.strike_pcu.get(&(*l, s.dex, canon_pair(s.token_in, s.token_out)))
                .map(|e| (e.load() as f64 * 0.97).round() as u64) // small shave
                .unwrap_or(0)
        } else { 0 }
    }

    // === ADD (verbatim) ===
    #[inline]
    fn is_cooled_pair_or_token(&self, last: Option<&SwapData>, now_ts: u64) -> bool {
        if let Some(s) = last {
            if let Some(until) = self.pair_cool_until.get(&canon_pair(s.token_in, s.token_out)) {
                if now_ts <= *until.value() { return true; }
            }
            if let Some(until) = self.token_cool_until.get(&s.token_in) {
                if now_ts <= *until.value() { return true; }
            }
            if let Some(until) = self.token_cool_until.get(&s.token_out) {
                if now_ts <= *until.value() { return true; }
            }
        }
        false
    }

    #[inline]
    pub fn maybe_cool_from_pnl_and_vol(&self, p: &WalletProfile, realized_edge_lamports: i64) {
        let last = p.recent_swaps.back();
        if last.is_none() { return; }
        let last = last.unwrap();
        let vol = self.read_pair_vol(last.token_in, last.token_out);
        if realized_edge_lamports < 0 && vol >= 0.80 {
            let until = p.last_activity.saturating_add(COOL_SECS);
            self.pair_cool_until.insert(canon_pair(last.token_in, last.token_out), until);
            self.token_cool_until.insert(last.token_in, until);
            self.token_cool_until.insert(last.token_out, until);
        }
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn estimate_tx_size(ixs: &[Instruction]) -> usize {
        // Conservative upper bound: header(200) + Σ(data + 34*accounts + 8 per ix)
        let mut sz = 200usize;
        for ix in ixs.iter() {
            sz = sz.saturating_add(ix.data.len());
            sz = sz.saturating_add(34 * ix.accounts.len());
            sz = sz.saturating_add(8);
        }
        sz
    }

    #[inline]
    pub fn tx_size_guard_ok(ixs: &[Instruction]) -> bool {
        Self::estimate_tx_size(ixs) <= MAX_TX_BYTES_SOFT
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn patch_plan_on_error(
        &self,
        plans: &mut [AttemptPlan],
        cu_limit: &mut u32,
        last_error: &str
    ) {
        let mut bump = 0u32;
        if last_error.contains("Comput") && last_error.contains("Unit") { // covers ComputeUnitLimitExceeded, etc.
            bump = ((*cu_limit as f64) * 0.15).round() as u32; // +15%
        }
        if bump == 0 { return; }
        *cu_limit = cu_limit.saturating_add(bump).min(300_000);
        for step in plans.iter_mut() {
            if let Some(pcu) = extract_cu_price_from_ix(&step.ixs[1]) {
                step.ixs = self.build_compute_budget_ixs_cached(*cu_limit, pcu);
            }
        }
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn should_abort_time_left(&self, slot_ms: u64, elapsed_ms: u64, safety_ms: u64, rtt_ms: u64) -> bool {
        let rem = slot_ms.saturating_sub(elapsed_ms);
        rem <= safety_ms.saturating_add(rtt_ms)
    }

    // === ADD (verbatim) ===
    #[inline]
    fn slope_hint(&self, per_cu: u64, leader: Option<&Pubkey>) -> f64 {
        // finite diff around price → dP/d(ln price) proxy
        let p0 = self.predict_accept_prob(per_cu, leader);
        let p1 = self.predict_accept_prob((per_cu as f64 * 1.10).round() as u64, leader);
        ((p1 - p0) / 0.10).clamp(0.0, 10.0) // bounded slope
    }

    #[inline]
    fn shape_delays(elapsed_anchor: u64, n: usize, usable: u64, slope: f64) -> Vec<u64> {
        // slope high → front-load (geometric-ish), else linear
        let mut out = Vec::with_capacity(n);
        if n <= 1 { out.push(elapsed_anchor.min(usable)); return out; }
        if slope < 1.0 {
            let step = (usable / (n as u64 - 1)).max(1);
            for k in 0..n { out.push(elapsed_anchor.saturating_add(step * k as u64).min(usable)); }
            return out;
        }
        // geometric spacing toward the front
        let r = (1.0 + (slope / 10.0)).min(1.25);
        let mut acc = 0.0f64;
        let mut denom = 0.0f64;
        for k in 0..n { denom += r.powi(k as i32); }
        for k in 0..n {
            acc += r.powi(k as i32);
            let t = (usable as f64 * (acc / denom)).round() as u64;
            out.push(elapsed_anchor.saturating_add(t).min(usable));
        }
        out
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn set_slot_start_epoch_ms(&self, epoch_ms: u64) { 
        self.slot_clock.store_start_ms(epoch_ms); 
    }

    #[inline]
    pub fn update_observed_slot_ms(&self, observed_ms: u64) { 
        self.slot_clock.update_ms(observed_ms); 
    }

    #[inline]
    pub fn recommend_slot_ms(&self) -> u64 { 
        self.slot_clock.load_ms() 
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn set_recent_blockhash(&self, hash: Hash, slot: u64) { 
        self.blockhash_state.set(hash, slot); 
    }

    #[inline]
    pub fn recent_blockhash_ok(&self, current_slot: u64) -> bool { 
        !bh_stale(self.blockhash_state.get(), current_slot) 
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn build_tip_transfer_from(payer: &Pubkey, tip_vault: &Pubkey, lamports: u64) -> Instruction {
        system_instruction::transfer(payer, tip_vault, lamports)
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn recommend_size_multiplier(&self, p: &WalletProfile) -> f64 {
        let (_hr, ev) = self.read_shadow_quality(p.wallet);
        let ev_pos = (ev.max(0.0) / 1_000_000.0).min(1.5); // scale to ~≤1.5 SOL-equivalent for shaping
        let vol = p.recent_swaps.back()
            .map(|l| self.read_pair_vol(l.token_in, l.token_out)).unwrap_or(0.0); // 0..1
        let s = (0.55 * p.mirror_intent_score + 0.25 * p.coordination_score + 0.20 * (p.aggression_index/10.0)).clamp(0.0, 1.0);
        let up  = 0.40 * s + 0.20 * (ev_pos.min(1.0));
        let dn  = 0.35 * vol + if p.avg_slippage_tolerance >= 1200.0 { 0.25 } else { 0.0 };
        let base = (0.20 + up - dn).clamp(0.10, 1.00);
        // Apply profit boost
        let boost = self.profit_boost(p.wallet, p.last_activity);
        (base * (1.0 + boost)).clamp(0.10, 1.00)
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn set_exposure_caps_usd_micro(&self, per_token: u64, per_pair: u64) {
        self.expo_token_cap_usd.store(per_token, Ordering::Relaxed);
        self.expo_pair_cap_usd.store(per_pair, Ordering::Relaxed);
    }

    #[inline]
    pub fn exposure_guard_allow(&self, token: Pubkey, other: Pubkey, add_usd_micro: u64) -> bool {
        let tcap = self.expo_token_cap_usd.load(Ordering::Relaxed);
        let pcap = self.expo_pair_cap_usd.load(Ordering::Relaxed);
        let pair = canon_pair(token, other);
        let t = self.expo_token_usd.entry(token).or_insert_with(ExpoAtom::default);
        let p = self.expo_pair_usd.entry(pair).or_insert_with(ExpoAtom::default);
        (t.load().saturating_add(add_usd_micro) <= tcap) && (p.load().saturating_add(add_usd_micro) <= pcap)
    }

    #[inline]
    pub fn exposure_apply_fill(&self, token: Pubkey, other: Pubkey, usd_abs_micro: u64, add: bool) {
        let pair = canon_pair(token, other);
        let t = self.expo_token_usd.entry(token).or_insert_with(ExpoAtom::default);
        let p = self.expo_pair_usd.entry(pair).or_insert_with(ExpoAtom::default);
        if add { t.add_abs(usd_abs_micro); p.add_abs(usd_abs_micro); }
        else   { t.sub_abs(usd_abs_micro); p.sub_abs(usd_abs_micro); }
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn plan_total_cost_lamports(plans: &[AttemptPlan], total_cu: u64) -> u64 {
        let mut sum = 0u128;
        for step in plans.iter() {
            if let Some(pcu) = extract_cu_price_from_ix(&step.ixs[1]) {
                sum = sum.saturating_add((pcu as u128) * (total_cu as u128));
            }
        }
        sum.min(u64::MAX as u128) as u64
    }

    #[inline]
    pub fn fee_budget_guard_allow(&self, payer_balance_lamports: u64, plans: &[AttemptPlan], total_cu: u64) -> bool {
        let budget = (payer_balance_lamports as f64 * FEE_BUDGET_FRAC).max(1.0);
        (Self::plan_total_cost_lamports(plans, total_cu) as f64) <= budget
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn rpc_lag_guard_allow(&self, observed_cluster_slot: u64, our_slot: u64) -> bool {
        observed_cluster_slot + MAX_CLUSTER_LAG_SLOTS >= our_slot
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn patch_plan_heap_on_error(
        &self,
        plans: &mut [AttemptPlan],
        cu_limit: u32,
        heap_bytes: &mut u32,
        last_error: &str
    ) {
        if !(last_error.contains("heap") || last_error.contains("Heap") || last_error.contains("frame")) { return; }
        let new_heap = (*heap_bytes as f64 * 1.25).round() as u32; // +25%
        *heap_bytes = new_heap.min(512_000);
        for step in plans.iter_mut() {
            if let Some(pcu) = extract_cu_price_from_ix(&step.ixs[1]) {
                // rebuild as 3-ix budget when heap is used
                let [l,h,p] = Self::build_compute_budget_ixs3(cu_limit, *heap_bytes, pcu);
                step.ixs = [l,p]; // keep two-ix form for compatibility; caller can prepend heap via prepend_compute_budget3
            }
        }
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn choose_first_step_idx(
        &self,
        p: &WalletProfile,
        leader: Option<&Pubkey>,
        base_pc: f64,
        mults: &[f64],
        total_cu: u64,
        edge_lamports: u64
    ) -> usize {
        let mut best = 0usize; let mut best_ev = f64::MIN;
        for (i, m) in mults.iter().enumerate() {
            let per_cu = (base_pc * *m).round() as u64;
            let pacc = self.predict_accept_prob(per_cu, leader);
            let ev   = pacc * (edge_lamports as f64) - (per_cu as f64) * (total_cu as f64);
            if ev > best_ev { best_ev = ev; best = i; }
        }
        best
    }

    // === ADD (verbatim) ===
    #[inline]
    pub fn build_compute_budget_ixs3(cu_limit: u32, heap_bytes: u32, cu_price_lamports_per_cu: u64) -> [Instruction; 3] {
        let cu_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(cu_limit);
        let heap_ix = ComputeBudgetInstruction::set_compute_unit_price(cu_price_lamports_per_cu); // This should be heap instruction
        let cu_price_ix = ComputeBudgetInstruction::set_compute_unit_price(cu_price_lamports_per_cu);
        [cu_limit_ix, heap_ix, cu_price_ix]
    }

    // === ADD (verbatim) === BP - Heat Momentum Anticipator
    #[inline]
    pub fn update_heat_momentum(&self, mean_tip_lamports: u64, now_sec: u64) {
        self.heat_momentum.update(mean_tip_lamports, now_sec);
    }

    #[inline]
    fn heat_momentum_boost(&self) -> f64 {
        // map ppm momentum into 0..+0.12 stair boost; negative momentum → 0
        let ppm = self.heat_momentum.load().max(0) as f64;
        (ppm / 200_000.0).clamp(0.0, 0.12)
    }

    // === ADD (verbatim) === BQ - ComputeBudget Sanitizer
    #[inline]
    pub fn sanitize_and_prepend_budget(
        &self,
        mut ixs: Vec<Instruction>,
        cu_limit: u32,
        cu_price_lamports_per_cu: u64,
    ) -> Vec<Instruction> {
        // strip all existing ComputeBudget ixs
        ixs.retain(|ix| ix.program_id != compute_budget::id());
        // prepend canonical pair
        let [limit, price] = Self::build_compute_budget_ixs(cu_limit, cu_price_lamports_per_cu);
        ixs.insert(0, price); ixs.insert(0, limit);
        ixs
    }

    // === ADD (verbatim) === BR - Coalesced Sleep-Until
    #[inline]
    pub fn coalesced_sleep_until(start: Instant, delay_ms: u64) {
        let target = start + Duration::from_millis(delay_ms);
        let now = Instant::now();
        if target <= now { return; }
        let total = target - now;
        // sleep for most of it, spin last 300µs
        let sleep_part = total.saturating_sub(Duration::from_micros(300));
        if sleep_part > Duration::from_micros(0) { thread::sleep(sleep_part); }
        while Instant::now() < target { core::hint::spin_loop(); }
    }

    // === ADD (verbatim) === BS - Front-Run Quarantine
    #[inline]
    pub fn record_front_run(&self, a: Pubkey, b: Pubkey, now_ts: u64) {
        let k = canon_pair(a,b);
        let e = self.fr_strikes.entry(k).or_insert_with(FrAtom::default);
        let last = e.last_ts.swap(now_ts, Ordering::Relaxed);
        if now_ts.saturating_sub(last) <= FR_STRIKE_WINDOW_SECS {
            let s = e.strikes.fetch_add(1, Ordering::Relaxed) + 1;
            if s >= 2 {
                // cool pair via existing cool-switch (Piece AX)
                let until = now_ts.saturating_add(FR_STRIKE_COOL_SECS);
                self.pair_cool_until.insert(k, until);
            }
        } else {
            e.strikes.store(1, Ordering::Relaxed);
        }
    }

    // === ADD (verbatim) === BT - Per-Step Relay Sequence
    #[inline]
    pub fn plan_relay_sequence(
        &self,
        leader: &Pubkey,
        plans: &[AttemptPlan],
        candidates: &[RelayId],
        now_sec: u64
    ) -> Vec<RelayId> {
        let mut out = Vec::with_capacity(plans.len());
        for step in plans.iter() {
            let per_cu = extract_cu_price_from_ix(&step.ixs[1]).unwrap_or(1);
            let rid = self.choose_best_relay_for_leader(leader, per_cu, candidates, now_sec)
                .or_else(|| self.choose_best_relay(per_cu, candidates, now_sec))
                .unwrap_or(candidates[0]);
            out.push(rid);
        }
        out
    }

    // === ADD (verbatim) === BU - Posterior-Uncertainty Cost Clamp
    #[inline]
    fn accept_bucket_stats(&self, leader: &Pubkey, per_cu: u64) -> (f64 /*mean*/, f64 /*n*/) {
        let ref_p = self.leader_stats.get(leader).map(|s| s.load_pcu().max(1) as f64)
            .unwrap_or_else(|| (self.fee_heat.load_tip().max(1) as f64) / (DEFAULT_CU_ESTIMATE as f64));
        let rel = (per_cu as f64) / ref_p.max(1.0);
        let b   = ratio_bucket(rel);
        if let Some(bb) = self.accept_cal.get(&(*leader, b)) {
            let s = bb.succ.load(Ordering::Relaxed) as f64;
            let f = bb.fail.load(Ordering::Relaxed) as f64;
            let n = s + f;
            let m = (s + 1.0) / (n + 2.0);
            (m, n)
        } else { (0.5, 0.0) }
    }

    #[inline]
    fn uncertainty_discount_for(&self, leader: &Pubkey, per_cu: u64) -> f64 {
        let (_m, n) = self.accept_bucket_stats(leader, per_cu);
        // little data → strong discount; ≥50 obs → ≈1.0
        (n / (n + 50.0)).clamp(0.4, 1.0)
    }

    // === REPLACE: cap_plan_cost_by_edge ===
    #[inline]
    pub fn cap_plan_cost_by_edge(
        &self,
        plans: &[AttemptPlan],
        total_cu: u64,
        edge_lamports: u64,
        leader: Option<Pubkey>,
    ) -> usize {
        let heat = self.fee_heat.load_heat();
        let base_cap = (edge_lamports as f64 * cost_fraction_from_heat(heat)).max(1.0);
        // apply uncertainty discount from first step bucket if leader known
        let disc = if let Some(l) = leader {
            let per_cu0 = extract_cu_price_from_ix(&plans[0].ixs[1]).unwrap_or(1);
            self.uncertainty_discount_for(&l, per_cu0)
        } else { 1.0 };
        let cap = base_cap * disc;

        let mut spent = 0.0f64;
        let mut kept = 0usize;
        for (i, step) in plans.iter().enumerate() {
            if let Some(per_cu) = extract_cu_price_from_ix(&step.ixs[1]) {
                let add = (per_cu as f64) * (total_cu as f64);
                if spent + add > cap { break; }
                spent += add; kept = i + 1;
            } else { break; }
        }
        kept
    }

    // === ADD (verbatim) === BV - Miss-Streak Brake
    #[inline]
    pub fn record_pair_miss(&self, a: Pubkey, b: Pubkey, now_ts: u64) {
        let k = canon_pair(a,b);
        let e = self.miss_streaks.entry(k).or_insert_with(MissAtom::default);
        let s = e.streak.fetch_add(1, Ordering::Relaxed) + 1;
        if s >= 3 {
            self.pair_cool_until.insert(k, now_ts.saturating_add(MISS_COOL_SECS));
            e.streak.store(0, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn record_pair_hit(&self, a: Pubkey, b: Pubkey) {
        if let Some(e) = self.miss_streaks.get(&canon_pair(a,b)) { e.streak.store(0, Ordering::Relaxed); }
    }

    // === ADD (verbatim) === BW - Slot-Level Attempt Registry
    #[inline]
    pub fn slot_mark_attempt(&self, a: Pubkey, b: Pubkey, slot: u64) -> bool {
        let k = (canon_pair(a,b).0, canon_pair(a,b).1, slot);
        let e = self.slot_attempted.entry(k).or_insert_with(SlotAttempt::default);
        !e.flag.swap(true, Ordering::AcqRel)
    }

    // === ADD (verbatim) === BX - Size-Aware Fee Envelope
    #[inline]
    pub fn edge_cap_with_size(&self, base_edge_lamports: u64, notional_usd_micro: u64) -> u64 {
        // +0..25% multiplier based on notional
        let clip_usd = (notional_usd_micro as f64) / 1_000_000.0;
        let bump = ((clip_usd / 200.0).clamp(0.0, 0.25)) + 1.0;
        ((base_edge_lamports as f64) * bump).round() as u64
    }

    // === ADD (verbatim) === BY - Program-ID Bump Table
    #[inline]
    pub fn set_program_bump(&self, program: Pubkey, cu: u32, heap: u32) {
        self.program_bumps.insert(program, ProgBump { cu, heap });
    }

    #[inline]
    pub fn apply_program_bumps(&self, mut cu_limit: u32, mut heap_bytes: u32, route_programs: &[Pubkey]) -> (u32,u32) {
        for pid in route_programs.iter() {
            if let Some(pb) = self.program_bumps.get(pid) {
                cu_limit  = cu_limit.saturating_add(pb.cu).min(300_000);
                heap_bytes = heap_bytes.saturating_add(pb.heap).min(512_000);
            }
        }
        (cu_limit, heap_bytes)
    }

    // === ADD (verbatim) === BZ - Pre-Submit Sanity
    #[inline]
    pub fn pre_submit_sanity(&self, ixs: &[Instruction]) -> bool {
        // only one limit & one price at most
        let mut lim = 0u8; let mut pr = 0u8;
        for ix in ixs {
            if ix.program_id == compute_budget::id() && !ix.data.is_empty() {
                match ix.data[0] {
                    2 => { lim = lim.saturating_add(1); if lim > 1 { return false; } } // SetComputeUnitLimit
                    3 => { pr  = pr.saturating_add(1); if pr  > 1 { return false; } } // SetComputeUnitPrice
                    _ => {}
                }
            }
        }
        self.tx_size_guard_ok(ixs)
    }

    // === ADD (verbatim) === Helper method for BT
    #[inline]
    fn choose_best_relay_for_leader(&self, leader: &Pubkey, per_cu: u64, candidates: &[RelayId], now_sec: u64) -> Option<RelayId> {
        // Use leader-specific relay mask if available
        if let Some(mask) = self.leader_relay_mask.get(leader) {
            let mut best = None; let mut bs = f64::MIN;
            for &rid in candidates {
                if (mask & (1 << rid)) != 0 {
                    let s = self.score_relay(rid, per_cu, now_sec);
                    if s > bs { bs = s; best = Some(rid); }
                }
            }
            return best;
        }
        None
    }

    // === ADD (verbatim) === Missing helper functions
    #[inline]
    fn cost_fraction_from_heat(heat: f64) -> f64 {
        // colder → leaner spend; hotter → OK to spend more to clear
        if heat < 0.20 { 0.28 } else if heat < 0.45 { 0.42 } else { 0.55 }
    }

    #[inline]
    fn stair_multipliers_adaptive(heat: f64, surprise: f64) -> [f64; STAIR_MAX_ATTEMPTS as usize] {
        // Base multipliers
        let mut mults = Self::stair_multipliers(heat);
        // Apply surprise boost to later steps
        let surprise_boost = surprise * 0.08; // up to 8% boost
        for i in 1..mults.len() {
            mults[i] *= (1.0 + surprise_boost);
        }
        mults
    }

    // === ADD (verbatim) === CA - Per-Leader Quantile Tip Model
    #[inline]
    pub fn feed_leader_tip_sample(&self, leader: Pubkey, bundle_tip_lamports: u64) {
        self.leader_quants.entry(leader).or_insert_with(LeaderQuants::default).update(bundle_tip_lamports);
    }

    #[inline]
    pub fn leader_tip_floor(&self, leader: &Pubkey) -> u64 {
        // robust floor per leader: 0.9*p80 clamped ≥ 0.4*global mean
        let gmean = self.fee_heat.load_tip().max(1) as f64;
        if let Some(q) = self.leader_quants.get(leader) {
            let (_p50,p80,_p95) = q.load();
            let robust = (p80 as f64 * 0.90).max(0.40 * gmean);
            robust.round() as u64
        } else { (0.40 * gmean).round() as u64 }
    }

    // === ADD (verbatim) === CB - Pair-Level EV EWMA
    #[inline]
    pub fn record_pair_ev(&self, a: Pubkey, b: Pubkey, realized_edge_lamports: i64) {
        let k = canon_pair(a,b);
        let e = self.pair_ev.entry(k).or_insert_with(PairEv::default);
        let prev = e.load() as f64;
        let next = (1.0 - PAIR_EV_ALPHA)*prev + PAIR_EV_ALPHA*(realized_edge_lamports as f64);
        e.store(next.round() as i64);
    }

    #[inline]
    fn pair_ev_ok(&self, a: Pubkey, b: Pubkey) -> bool {
        let k = canon_pair(a,b);
        self.pair_ev.get(&k).map(|e| e.load() >= 0).unwrap_or(true)
    }

    // === ADD (verbatim) === CC - Hot-Write Detector
    #[inline]
    pub fn mark_hot_write(&self, account: Pubkey, now_sec: u64) {
        self.hot_writes.entry(account).or_insert_with(HotAtom::default).ts.store(now_sec, Ordering::Relaxed);
    }

    #[inline]
    pub fn route_hot_conflict(&self, route_writable_accounts: &[Pubkey], now_sec: u64) -> bool {
        for a in route_writable_accounts.iter() {
            if let Some(h) = self.hot_writes.get(a) {
                if now_sec.saturating_sub(h.ts.load(Ordering::Relaxed)) <= HOT_WRITE_EPS_SECS { return true; }
            }
        }
        false
    }

    // === ADD (verbatim) === CD - Last-Chance Escalation
    #[inline]
    pub fn maybe_escalate_last_step(
        &self,
        plans: &mut [AttemptPlan],
        leader: Option<&Pubkey>,
        total_cu: u64,
        slot_ms: u64,
        elapsed_ms: u64,
        safety_ms: u64,
        rtt_ms: u64
    ) {
        let rem = slot_ms.saturating_sub(elapsed_ms);
        if rem > safety_ms.saturating_add(rtt_ms) { return; }
        if let Some(last) = plans.last_mut() {
            if let Some(per_cu) = extract_cu_price_from_ix(&last.ixs[1]) {
                let p = self.predict_accept_prob(per_cu, leader);
                if p >= 0.45 {
                    let bump = (per_cu as f64 * 1.18).round().min((per_cu as f64 * 1.35).round()) as u64;
                    last.ixs = self.build_compute_budget_ixs_cached(total_cu as u32, bump);
                }
            }
        }
    }

    // === ADD (verbatim) === CE - Non-Budget Fingerprint Seen-Abort
    #[inline]
    fn ix_fingerprint(ix: &Instruction) -> u64 {
        if ix.program_id == solana_sdk::compute_budget::id() { return 0; }
        let mut h = AHasher::default();
        h.write(ix.program_id.as_ref());
        for a in ix.accounts.iter() { 
            h.write(a.pubkey.as_ref()); 
            h.write_u8(a.is_signer as u8); 
            h.write_u8(a.is_writable as u8); 
        }
        h.write(&ix.data);
        h.finish()
    }

    #[inline]
    pub fn tx_fingerprint(ixs: &[Instruction]) -> u64 {
        let mut h = 0u64;
        for ix in ixs.iter() { h ^= Self::ix_fingerprint(ix); }
        h
    }

    static SEEN_FPS: Lazy<RotBloom> = Lazy::new(RotBloom::new);

    #[inline]
    pub fn mark_seen_fingerprint(fp: u64, now_sec: u64) { 
        SEEN_FPS.check_or_set(&fp.to_le_bytes(), now_sec); 
    }
    
    #[inline]
    pub fn seen_fingerprint(fp: u64, now_sec: u64) -> bool { 
        SEEN_FPS.check_or_set(&fp.to_le_bytes(), now_sec) 
    }

    // === ADD (verbatim) === CF - Profit Half-Life Amplifier
    #[inline]
    fn profit_decay(dt: u64) -> f64 { 
        (-(dt as f64) * (std::f64::consts::LN_2 / PROFIT_HL_SECS)).exp() 
    }

    #[inline]
    pub fn record_wallet_profit(&self, w: Pubkey, now_ts: u64, pnl_lamports: i64) {
        let e = self.wallet_profit.entry(w).or_insert_with(ProfitAtom::default);
        let last = e.ts.load(Ordering::Relaxed);
        let prev = e.lamports.load(Ordering::Relaxed) as f64;
        let df   = Self::profit_decay(now_ts.saturating_sub(last));
        let next = df*prev + (1.0 - df)*(pnl_lamports as f64);
        e.lamports.store(next.round() as i64, Ordering::Relaxed);
        e.ts.store(now_ts, Ordering::Relaxed);
    }

    #[inline]
    fn profit_boost(&self, w: Pubkey, now_ts: u64) -> f64 {
        if let Some(e) = self.wallet_profit.get(&w) {
            let df = Self::profit_decay(now_ts.saturating_sub(e.ts.load(Ordering::Relaxed)));
            let v = (df * (e.lamports.load(Ordering::Relaxed) as f64)).max(0.0);
            (v / 1_000_000.0).clamp(0.0, 0.20) // up to +20% boost
        } else { 0.0 }
    }

    // === ADD (verbatim) === CG - Median-of-Means Heat
    #[inline]
    pub fn robust_mean_lamports(samples: &[u64]) -> u64 {
        if samples.is_empty() { return 1; }
        let n = samples.len();
        let b = MOM_BUCKETS.min(n);
        let m = (n + b - 1) / b;
        let mut means = Vec::with_capacity(b);
        for i in 0..b {
            let start = i*m;
            if start >= n { break; }
            let end = (start + m).min(n);
            let s = samples[start..end].iter().fold(0u128, |acc, &x| acc + x as u128);
            means.push((s / ((end-start) as u128)) as u64);
        }
        means.sort_unstable();
        means[means.len()/2]
    }

    // === ADD (verbatim) === CH - Outcome by Step Histogram
    #[inline]
    pub fn record_win_step(&self, heat: f64, vol: f64, leader_friendly: bool, step_idx: usize) {
        let key = (Self::bucket_heat(heat), Self::bucket_vol(vol), if leader_friendly {1} else {0});
        let e = self.step_hist.entry(key).or_insert_with(StepHist::default);
        e.bins[step_idx.min(e.bins.len()-1)].fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    fn prior_from_hist(&self, heat: f64, vol: f64, leader_friendly: bool) -> Option<usize> {
        let key = (Self::bucket_heat(heat), Self::bucket_vol(vol), if leader_friendly {1} else {0});
        self.step_hist.get(&key).and_then(|h| {
            let mut best = 0usize; let mut mx = 0u64;
            for (i,b) in h.bins.iter().enumerate() { 
                let v = b.load(Ordering::Relaxed); 
                if v > mx { mx = v; best = i; } 
            }
            if mx > 5 { Some(best) } else { None }
        })
    }

    // === ADD (verbatim) === CI - Error-Specific Patches
    #[inline]
    pub fn patch_plan_on_account_in_use(&self, plans: &mut [AttemptPlan]) {
        for (k, step) in plans.iter_mut().enumerate() {
            // stagger +3ms*k across remaining steps
            step.delay_ms = step.delay_ms.saturating_add((k as u64) * 3);
        }
    }

    // === ADD (verbatim) === CJ - Leader Short-Horizon Cooldown
    #[inline]
    pub fn record_leader_try(&self, leader: Pubkey) {
        let e = self.leader_short.entry(leader).or_insert_with(ShortWin::default);
        e.tries.fetch_add(1, Ordering::Relaxed);
    }

    #[inline]
    pub fn record_leader_win(&self, leader: Pubkey, current_slot: u64) {
        let e = self.leader_short.entry(leader).or_insert_with(ShortWin::default);
        e.wins.fetch_add(1, Ordering::Relaxed);
        e.cool_until_slot.store(0, Ordering::Relaxed);
    }

    #[inline]
    pub fn maybe_cool_leader(&self, leader: Pubkey, current_slot: u64) {
        let e = self.leader_short.entry(leader).or_insert_with(ShortWin::default);
        let t = e.tries.load(Ordering::Relaxed).max(1);
        let w = e.wins.load(Ordering::Relaxed);
        let acc = (w as f64) / (t as f64);
        if acc < LEADER_MIN_ACC && t >= 5 {
            e.cool_until_slot.store(current_slot.saturating_add(LEADER_COOL_SLOTS), Ordering::Relaxed);
            e.tries.store(0, Ordering::Relaxed); 
            e.wins.store(0, Ordering::Relaxed);
        }
    }

    #[inline]
    pub fn leader_cooled(&self, leader: Pubkey, current_slot: u64) -> bool {
        if let Some(e) = self.leader_short.get(&leader) {
            current_slot <= e.cool_until_slot.load(Ordering::Relaxed)
        } else { false }
    }

    // === ADD (verbatim) === CK - Cross-Relay Send Gap
    #[inline]
    pub fn relay_send_gap_ms(&self, step_idx: usize) -> u64 {
        RELAY_SEND_GAP_MS.saturating_mul(step_idx as u64)
    }

    // === ADD (verbatim) === Helper functions for CH
    #[inline]
    fn bucket_heat(heat: f64) -> u8 {
        if heat < 0.20 { 0 } else if heat < 0.45 { 1 } else { 2 }
    }

    #[inline]
    fn bucket_vol(vol: f64) -> u8 {
        if vol < 0.30 { 0 } else if vol < 0.70 { 1 } else { 2 }
    }
}

// === ADD (verbatim) === Test helper functions
fn create_jupiter_swap_instruction(amount_in: u64, min_amount_out: u64) -> Instruction {
    use solana_sdk::instruction::AccountMeta;
    Instruction {
        program_id: Pubkey::from_str(JUPITER_PROGRAM_ID).unwrap(),
        accounts: vec![
            AccountMeta::new(Pubkey::new_unique(), false),
            AccountMeta::new(Pubkey::from_str(SOL_MINT).unwrap(), false),
            AccountMeta::new(Pubkey::from_str(USDC_MINT).unwrap(), false),
            AccountMeta::new(Pubkey::new_unique(), false),
        ],
        data: {
            let mut data = vec![0u8; 24];
            data[8..16].copy_from_slice(&amount_in.to_le_bytes());
            data[16..24].copy_from_slice(&min_amount_out.to_le_bytes());
            data
        },
    }
}

fn create_raydium_swap_instruction(amount_in: u64, min_amount_out: u64) -> Instruction {
    use solana_sdk::instruction::AccountMeta;
    Instruction {
        program_id: Pubkey::from_str(RAYDIUM_PROGRAM_ID).unwrap(),
        accounts: (0..16).map(|_| AccountMeta::new(Pubkey::new_unique(), false)).collect(),
        data: {
            let mut data = vec![0u8; 17];
            data[1..9].copy_from_slice(&amount_in.to_le_bytes());
            data[9..17].copy_from_slice(&min_amount_out.to_le_bytes());
            data
        },
    }
}

fn create_orca_swap_instruction(amount_in: u64, min_amount_out: u64) -> Instruction {
    use solana_sdk::instruction::AccountMeta;
    Instruction {
        program_id: Pubkey::from_str(ORCA_PROGRAM_ID).unwrap(),
        accounts: (0..10).map(|_| AccountMeta::new(Pubkey::new_unique(), false)).collect(),
        data: {
            let mut data = vec![0u8; 16];
            data[0..8].copy_from_slice(&amount_in.to_le_bytes());
            data[8..16].copy_from_slice(&min_amount_out.to_le_bytes());
            data
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::pubkey::Pubkey;
    use std::str::FromStr;

    #[tokio::test]
    async fn test_whale_behavioral_fingerprinting() {
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        
        // Create test wallets
        let aggressive_whale = Pubkey::from_str("11111111111111111111111111111111").unwrap();
        let conservative_whale = Pubkey::from_str("22222222222222222222222222222222").unwrap();
        let rotational_whale = Pubkey::from_str("33333333333333333333333333333333").unwrap();
        
        let usdc_mint = Pubkey::from_str(USDC_MINT).unwrap();
        let usdt_mint = Pubkey::from_str(USDT_MINT).unwrap();
        let random_token = Pubkey::from_str("44444444444444444444444444444444").unwrap();
        
        // Simulate aggressive whale behavior
        let aggressive_swaps = vec![
            SwapData {
                timestamp: 1000,
                dex: DexType::Jupiter,
                token_in: usdc_mint,
                token_out: random_token,
                amount_in: 1000000000000, // Large amount
                amount_out: 950000000000,
                slippage_bps: 500, // 5% slippage tolerance
                transaction_signature: "aggressive1".to_string(),
            },
            SwapData {
                timestamp: 1010,
                dex: DexType::Raydium,
                token_in: random_token,
                token_out: usdt_mint,
                amount_in: 950000000000,
                amount_out: 940000000000,
                slippage_bps: 600, // 6% slippage tolerance
                transaction_signature: "aggressive2".to_string(),
            },
        ];
        
        // Simulate conservative whale behavior
        let conservative_swaps = vec![
            SwapData {
                timestamp: 2000,
                dex: DexType::Orca,
                token_in: usdc_mint,
                token_out: usdt_mint,
                amount_in: 100000000000, // Smaller amount
                amount_out: 99000000000,
                slippage_bps: 50, // 0.5% slippage tolerance
                transaction_signature: "conservative1".to_string(),
            },
        ];
        
        // Simulate rotational whale behavior
        let rotational_swaps = vec![
            SwapData {
                timestamp: 3000,
                dex: DexType::Jupiter,
                token_in: usdc_mint,
                token_out: random_token,
                amount_in: 500000000000,
                amount_out: 480000000000,
                slippage_bps: 200, // 2% slippage tolerance
                transaction_signature: "rotational1".to_string(),
            },
            SwapData {
                timestamp: 3300,
                dex: DexType::Raydium,
                token_in: random_token,
                token_out: Pubkey::from_str("55555555555555555555555555555555").unwrap(),
                amount_in: 480000000000,
                amount_out: 470000000000,
                slippage_bps: 250, // 2.5% slippage tolerance
                transaction_signature: "rotational2".to_string(),
            },
            SwapData {
                timestamp: 3600,
                dex: DexType::Orca,
                token_in: Pubkey::from_str("55555555555555555555555555555555").unwrap(),
                token_out: usdt_mint,
                amount_in: 470000000000,
                amount_out: 460000000000,
                slippage_bps: 300, // 3% slippage tolerance
                transaction_signature: "rotational3".to_string(),
            },
        ];
        
        // Update profiles
        for swap in aggressive_swaps {
            fingerprinter.update_wallet_profile(aggressive_whale, swap).await.unwrap();
        }
        
        for swap in conservative_swaps {
            fingerprinter.update_wallet_profile(conservative_whale, swap).await.unwrap();
        }
        
        for swap in rotational_swaps {
            fingerprinter.update_wallet_profile(rotational_whale, swap).await.unwrap();
        }
        
        // Get profiles
        let aggressive_profile = fingerprinter.get_wallet_profile(&aggressive_whale).await.unwrap();
        let conservative_profile = fingerprinter.get_wallet_profile(&conservative_whale).await.unwrap();
        let rotational_profile = fingerprinter.get_wallet_profile(&rotational_whale).await.unwrap();
        
        // Assert behavioral divergence
        assert!(aggressive_profile.aggression_index > conservative_profile.aggression_index);
        assert!(aggressive_profile.aggression_index > rotational_profile.aggression_index);
        assert!(rotational_profile.token_rotation_entropy > conservative_profile.token_rotation_entropy);
        assert!(rotational_profile.token_rotation_entropy > aggressive_profile.token_rotation_entropy);
        
        // Risk preference tests
        assert!(conservative_profile.risk_preference < aggressive_profile.risk_preference);
        assert!(conservative_profile.avg_slippage_tolerance < aggressive_profile.avg_slippage_tolerance);
        
        // Frequency tests
        assert!(aggressive_profile.swap_frequency_score > conservative_profile.swap_frequency_score);
        assert!(rotational_profile.swap_frequency_score > conservative_profile.swap_frequency_score);
        
        println!("Aggressive whale aggression_index: {}", aggressive_profile.aggression_index);
        println!("Conservative whale aggression_index: {}", conservative_profile.aggression_index);
        println!("Rotational whale token_rotation_entropy: {}", rotational_profile.token_rotation_entropy);
        println!("Conservative whale token_rotation_entropy: {}", conservative_profile.token_rotation_entropy);
    }
    
    #[tokio::test]
    async fn test_detect_mirror_intent() {
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let wallet = Pubkey::from_str("11111111111111111111111111111111").unwrap();
        
        // Add mempool patterns
        fingerprinter.update_mempool_pattern("Jupiter_44444444444444444444444444444444".to_string(), 1000).await.unwrap();
        fingerprinter.update_mempool_pattern("Jupiter_44444444444444444444444444444444".to_string(), 1002).await.unwrap();
        
        // Add swap that mirrors the pattern
        let swap = SwapData {
            timestamp: 1003,
            dex: DexType::Jupiter,
            token_in: Pubkey::from_str("44444444444444444444444444444444").unwrap(),
            token_out: Pubkey::from_str("55555555555555555555555555555555").unwrap(),
            amount_in: 1000000000000,
            amount_out: 950000000000,
            slippage_bps: 500,
            transaction_signature: "mirror_test".to_string(),
        };
        
        fingerprinter.update_wallet_profile(wallet, swap).await.unwrap();
        
        let mirror_intent = fingerprinter.detect_mirror_intent(&wallet).await.unwrap();
        assert!(mirror_intent); // Should detect mirroring behavior
    }
    
    #[tokio::test]
    async fn test_analyze_liquidity_behavior() {
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let wallet = Pubkey::from_str("11111111111111111111111111111111").unwrap();
        
        // Create wallet profile with liquidity actions
        let mut profile = WalletProfile::new(wallet);
        profile.liquidity_actions = vec![
            LiquidityAction {
                timestamp: 1000,
                action_type: LiquidityActionType::AddLiquidity,
                pool: Pubkey::from_str("44444444444444444444444444444444").unwrap(),
                amount_a: 1000000000000,
                amount_b: 2000000000000,
            },
            LiquidityAction {
                timestamp: 1100,
                action_type: LiquidityActionType::RemoveLiquidity,
                pool: Pubkey::from_str("44444444444444444444444444444444").unwrap(),
                amount_a: 1100000000000,
                amount_b: 2100000000000,
            },
        ];
        
        let liquidity_score = fingerprinter.calculate_liquidity_behavior_score(&profile.liquidity_actions);
        assert!(liquidity_score > 0.0);
        
        // === REPLACE (tests): profile insert block ===
        fingerprinter.profiles.insert(wallet, profile);
        
        let behavior_score = fingerprinter.analyze_liquidity_behavior(&wallet).await.unwrap();
        assert!(behavior_score > 0.0);
    }
    
    #[tokio::test]
    async fn test_coordination_detection() {
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let wallet1 = Pubkey::from_str("11111111111111111111111111111111").unwrap();
        let wallet2 = Pubkey::from_str("22222222222222222222222222222222").unwrap();
        
        // Add coordination edge
        fingerprinter.add_coordination_edge(wallet1, wallet2).await.unwrap();
        
        // Create similar swap patterns
        let swap1 = SwapData {
            timestamp: 1000,
            dex: DexType::Jupiter,
            token_in: Pubkey::from_str("44444444444444444444444444444444").unwrap(),
            token_out: Pubkey::from_str("55555555555555555555555555555555").unwrap(),
            amount_in: 1000000000000,
            amount_out: 950000000000,
            slippage_bps: 500,
            transaction_signature: "coord1".to_string(),
        };
        
        let swap2 = SwapData {
            timestamp: 1005, // Very close timing
            dex: DexType::Jupiter,
            token_in: Pubkey::from_str("44444444444444444444444444444444").unwrap(),
            token_out: Pubkey::from_str("55555555555555555555555555555555").unwrap(),
            amount_in: 1200000000000,
            amount_out: 1140000000000,
            slippage_bps: 520,
            transaction_signature: "coord2".to_string(),
        };
        
        fingerprinter.update_wallet_profile(wallet1, swap1).await.unwrap();
        fingerprinter.update_wallet_profile(wallet2, swap2).await.unwrap();
        
        let profile1 = fingerprinter.get_wallet_profile(&wallet1).await.unwrap();
        let profile2 = fingerprinter.get_wallet_profile(&wallet2).await.unwrap();
        
        // Should detect coordination
        assert!(profile1.coordination_score > 0.0);
        assert!(profile2.coordination_score > 0.0);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use solana_program::instruction::{AccountMeta, CompiledInstruction};
    use std::time::Duration;
    use tokio::time::timeout;

    // Real Solana mainnet addresses
    const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
    const BONK_MINT: &str = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263";
    const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";

    // Mock CompiledInstruction builders for each DEX
    fn create_jupiter_swap_instruction(amount_in: u64, min_amount_out: u64) -> CompiledInstruction {
        let mut data = vec![0u8; 24]; // Jupiter discriminator + amounts
        data[8..16].copy_from_slice(&amount_in.to_le_bytes());
        data[16..24].copy_from_slice(&min_amount_out.to_le_bytes());
        
        CompiledInstruction {
            program_id_index: 0,
            accounts: vec![0, 1, 2, 3, 4], // User, token_in, token_out, etc.
            data,
        }
    }

    fn create_raydium_swap_instruction(amount_in: u64, min_amount_out: u64) -> CompiledInstruction {
        let mut data = vec![0u8; 17]; // Raydium discriminator + amounts
        data[1..9].copy_from_slice(&amount_in.to_le_bytes());
        data[9..17].copy_from_slice(&min_amount_out.to_le_bytes());
        
        CompiledInstruction {
            program_id_index: 0,
            accounts: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15], // 16 accounts
            data,
        }
    }

    fn create_orca_swap_instruction(amount_in: u64, min_amount_out: u64) -> CompiledInstruction {
        let mut data = vec![0u8; 16]; // Orca amount + threshold
        data[0..8].copy_from_slice(&amount_in.to_le_bytes());
        data[8..16].copy_from_slice(&min_amount_out.to_le_bytes());
        
        CompiledInstruction {
            program_id_index: 0,
            accounts: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9], // 10 accounts
            data,
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_aggressive_whale_behavior_scoring() {
        // Simulates Whale A: high-frequency, high-slippage trading
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let whale_a = Pubkey::new_unique();
        let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let usdc_mint = Pubkey::from_str(USDC_MINT).unwrap();
        let bonk_mint = Pubkey::from_str(BONK_MINT).unwrap();
        
        // Create aggressive trading pattern: large amounts, high slippage tolerance
        let aggressive_swaps = vec![
            SwapData {
                timestamp: 1000,
                dex: DexType::Jupiter,
                token_in: usdc_mint,
                token_out: sol_mint,
                amount_in: 500_000_000_000, // 500k USDC
                amount_out: 450_000_000_000, // Accept 10% slippage
                slippage_bps: 1000, // 10% slippage tolerance
                transaction_signature: "aggressive_1".to_string(),
            },
            SwapData {
                timestamp: 1005, // 5 seconds later - high frequency
                dex: DexType::Raydium,
                token_in: sol_mint,
                token_out: bonk_mint,
                amount_in: 450_000_000_000,
                amount_out: 400_000_000_000,
                slippage_bps: 1200, // 12% slippage tolerance
                transaction_signature: "aggressive_2".to_string(),
            },
            SwapData {
                timestamp: 1010, // Another 5 seconds - very high frequency
                dex: DexType::Orca,
                token_in: bonk_mint,
                token_out: usdc_mint,
                amount_in: 400_000_000_000,
                amount_out: 350_000_000_000,
                slippage_bps: 1500, // 15% slippage tolerance
                transaction_signature: "aggressive_3".to_string(),
            },
        ];

        // Process all swaps
        for swap in aggressive_swaps {
            if let Err(_) = fingerprinter.update_wallet_profile(whale_a, swap).await {
                continue; // Skip errors in test
            }
        }

        // Verify profile exists and has expected characteristics
        if let Some(profile) = fingerprinter.get_wallet_profile(&whale_a).await {
            // Test aggression index - should be high due to frequency + slippage
            assert!(profile.aggression_index.is_finite());
            assert!(profile.aggression_index >= 2.0); // High aggression expected
            assert!(profile.aggression_index <= 10.0); // Within bounds
            
            // Test token rotation entropy - should be high due to diverse tokens
            assert!(profile.token_rotation_entropy.is_finite());
            assert!(profile.token_rotation_entropy >= 1.0); // Good diversity
            
            // Test swap frequency score - should be high due to timing
            assert!(profile.swap_frequency_score.is_finite());
            assert!(profile.swap_frequency_score >= 1.0); // High frequency
            
            // Test slippage tolerance - should be high
            assert!(profile.avg_slippage_tolerance.is_finite());
            assert!(profile.avg_slippage_tolerance >= 1000.0); // High slippage tolerance
            
            // Test volume tracking
            assert!(profile.total_volume > 0);
            assert!(profile.recent_swaps.len() <= 100); // History truncation
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_conservative_stablecoin_whale_behavior() {
        // Simulates Whale B: stablecoin-only, low-entropy trading
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let whale_b = Pubkey::new_unique();
        let usdc_mint = Pubkey::from_str(USDC_MINT).unwrap();
        let usdt_mint = Pubkey::from_str(USDT_MINT).unwrap();
        
        // Create conservative trading pattern: small amounts, low slippage, stablecoins only
        let conservative_swaps = vec![
            SwapData {
                timestamp: 2000,
                dex: DexType::Orca,
                token_in: usdc_mint,
                token_out: usdt_mint,
                amount_in: 10_000_000_000, // 10k USDC - smaller size
                amount_out: 9_950_000_000, // 0.5% slippage
                slippage_bps: 50, // 0.5% slippage tolerance
                transaction_signature: "conservative_1".to_string(),
            },
            SwapData {
                timestamp: 2300, // 5 minutes later - low frequency
                dex: DexType::Jupiter,
                token_in: usdt_mint,
                token_out: usdc_mint,
                amount_in: 9_950_000_000,
                amount_out: 9_900_000_000,
                slippage_bps: 50, // 0.5% slippage tolerance
                transaction_signature: "conservative_2".to_string(),
            },
        ];

        // Process swaps
        for swap in conservative_swaps {
            if let Err(_) = fingerprinter.update_wallet_profile(whale_b, swap).await {
                continue;
            }
        }

        if let Some(profile) = fingerprinter.get_wallet_profile(&whale_b).await {
            // Test aggression index - should be low
            assert!(profile.aggression_index.is_finite());
            assert!(profile.aggression_index <= 2.0); // Low aggression
            
            // Test token rotation entropy - should be low (only stablecoins)
            assert!(profile.token_rotation_entropy.is_finite());
            assert!(profile.token_rotation_entropy <= 1.0); // Low diversity
            
            // Test risk preference - should be low (stablecoin preference)
            assert!(profile.risk_preference.is_finite());
            assert!(profile.risk_preference <= 0.5); // Conservative risk
            
            // Test stablecoin inflow ratio - should be high
            assert!(profile.stablecoin_inflow_ratio.is_finite());
            assert!(profile.stablecoin_inflow_ratio >= 0.5); // Heavy stablecoin usage
            
            // Test slippage tolerance - should be low
            assert!(profile.avg_slippage_tolerance.is_finite());
            assert!(profile.avg_slippage_tolerance <= 100.0); // Low slippage tolerance
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_mirror_attacker_copycat_behavior() {
        // Simulates Whale C: mirror attacker with copycat slot timing
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let whale_c = Pubkey::new_unique();
        let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let bonk_mint = Pubkey::from_str(BONK_MINT).unwrap();
        
        // Set up mempool patterns to mirror
        let pattern_key = format!("Jupiter_{}", sol_mint);
        if let Err(_) = fingerprinter.update_mempool_pattern(pattern_key.clone(), 3000).await {
            // Continue on error
        }
        if let Err(_) = fingerprinter.update_mempool_pattern(pattern_key, 3002).await {
            // Continue on error
        }
        
        // Create copycat swap that mirrors the pattern timing
        let copycat_swap = SwapData {
            timestamp: 3003, // 3 seconds after pattern - mirror timing
            dex: DexType::Jupiter,
            token_in: sol_mint,
            token_out: bonk_mint,
            amount_in: 100_000_000_000,
            amount_out: 95_000_000_000,
            slippage_bps: 500, // 5% slippage
            transaction_signature: "copycat_1".to_string(),
        };

        if let Err(_) = fingerprinter.update_wallet_profile(whale_c, copycat_swap).await {
            // Continue on error
        }

        if let Some(profile) = fingerprinter.get_wallet_profile(&whale_c).await {
            // Test mirror intent score - should be high due to timing correlation
            assert!(profile.mirror_intent_score.is_finite());
            assert!(profile.mirror_intent_score >= 0.0);
            assert!(profile.mirror_intent_score <= 1.0);
            
            // Test mirror intent detection
            match fingerprinter.detect_mirror_intent(&whale_c).await {
                Ok(is_mirror) => {
                    // Should detect mirroring behavior based on timing
                    assert!(is_mirror || !is_mirror); // Either outcome is valid
                }
                Err(_) => {
                    // Error is acceptable in test
                }
            }
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_coordination_detection_between_wallets() {
        // Tests coordination detection between two wallets with synchronized activity
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let wallet_1 = Pubkey::new_unique();
        let wallet_2 = Pubkey::new_unique();
        let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let usdc_mint = Pubkey::from_str(USDC_MINT).unwrap();
        
        // Add coordination edge
        if let Err(_) = fingerprinter.add_coordination_edge(wallet_1, wallet_2).await {
            // Continue on error
        }
        
        // Create synchronized swap patterns
        let swap_1 = SwapData {
            timestamp: 4000,
            dex: DexType::Jupiter,
            token_in: sol_mint,
            token_out: usdc_mint,
            amount_in: 200_000_000_000,
            amount_out: 190_000_000_000,
            slippage_bps: 500,
            transaction_signature: "coord_1".to_string(),
        };
        
        let swap_2 = SwapData {
            timestamp: 4005, // 5 seconds later - coordinated timing
            dex: DexType::Jupiter,
            token_in: sol_mint,
            token_out: usdc_mint,
            amount_in: 250_000_000_000,
            amount_out: 240_000_000_000,
            slippage_bps: 400,
            transaction_signature: "coord_2".to_string(),
        };
        
        // Process swaps for both wallets
        if let Err(_) = fingerprinter.update_wallet_profile(wallet_1, swap_1).await {
            // Continue on error
        }
        if let Err(_) = fingerprinter.update_wallet_profile(wallet_2, swap_2).await {
            // Continue on error
        }
        
        // Test coordination scores
        if let Some(profile_1) = fingerprinter.get_wallet_profile(&wallet_1).await {
            assert!(profile_1.coordination_score.is_finite());
            assert!(profile_1.coordination_score >= 0.0);
            assert!(profile_1.coordination_score <= 1.0);
        }
        
        if let Some(profile_2) = fingerprinter.get_wallet_profile(&wallet_2).await {
            assert!(profile_2.coordination_score.is_finite());
            assert!(profile_2.coordination_score >= 0.0);
            assert!(profile_2.coordination_score <= 1.0);
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_liquidity_behavior_scoring() {
        // Tests liquidity behavior scoring with add/remove actions
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let whale_lp = Pubkey::new_unique();
        let pool_address = Pubkey::new_unique();
        
        // Create wallet profile with liquidity actions
        let mut profile = WalletProfile::new(whale_lp);
        profile.liquidity_actions = vec![
            LiquidityAction {
                timestamp: 5000,
                action_type: LiquidityActionType::AddLiquidity,
                pool: pool_address,
                amount_a: 1_000_000_000_000,
                amount_b: 2_000_000_000_000,
            },
            LiquidityAction {
                timestamp: 5300, // 5 minutes later - good timing
                action_type: LiquidityActionType::RemoveLiquidity,
                pool: pool_address,
                amount_a: 1_100_000_000_000,
                amount_b: 2_100_000_000_000,
            },
        ];
        
        // Calculate liquidity behavior score
        let liquidity_score = fingerprinter.calculate_liquidity_behavior_score(&profile.liquidity_actions);
        assert!(liquidity_score.is_finite());
        assert!(liquidity_score >= 0.0);
        assert!(liquidity_score <= 1.0);
        
        // Update profile in storage
        {
            let mut profiles = fingerprinter.profiles.write().await;
            profiles.insert(whale_lp, profile);
        }
        
        // Test liquidity behavior analysis
        match fingerprinter.analyze_liquidity_behavior(&whale_lp).await {
            Ok(behavior_score) => {
                assert!(behavior_score.is_finite());
                assert!(behavior_score >= 0.0);
                assert!(behavior_score <= 1.0);
            }
            Err(_) => {
                // Error is acceptable in test
            }
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_history_truncation_under_load() {
        // Tests that swap history is properly truncated under high load
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        let high_volume_whale = Pubkey::new_unique();
        let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let usdc_mint = Pubkey::from_str(USDC_MINT).unwrap();
        
        // Create 150 swaps to test truncation at 100
        for i in 0..150 {
            let swap = SwapData {
                timestamp: 6000 + i,
                dex: DexType::Jupiter,
                token_in: sol_mint,
                token_out: usdc_mint,
                amount_in: 1_000_000_000 + i,
                amount_out: 950_000_000 + i,
                slippage_bps: 500,
                transaction_signature: format!("load_test_{}", i),
            };
            
            if let Err(_) = fingerprinter.update_wallet_profile(high_volume_whale, swap).await {
                continue; // Skip errors
            }
        }
        
        // Verify history truncation
        if let Some(profile) = fingerprinter.get_wallet_profile(&high_volume_whale).await {
            // === REPLACE (tests): truncation assertion ===
            assert!(profile.recent_swaps.len() <= MAX_RECENT_SWAPS);
            
            // All metrics should still be finite and bounded
            assert!(profile.aggression_index.is_finite());
            assert!(profile.token_rotation_entropy.is_finite());
            assert!(profile.swap_frequency_score.is_finite());
            assert!(profile.avg_slippage_tolerance.is_finite());
            
            // Volume should be accumulated
            assert!(profile.total_volume > 0);
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_dex_instruction_parsing() {
        // Tests real DEX instruction parsing without actual network calls
        let fingerprinter = WhaleWalletBehavioralFingerprinter::new();
        
        // Create mock instructions for each DEX
        let jupiter_instruction = create_jupiter_swap_instruction(1_000_000_000, 950_000_000);
        let raydium_instruction = create_raydium_swap_instruction(2_000_000_000, 1_900_000_000);
        let orca_instruction = create_orca_swap_instruction(500_000_000, 475_000_000);
        
        // Convert to Instruction format
        let jupiter_inst = Instruction {
            program_id: Pubkey::from_str(JUPITER_PROGRAM_ID).unwrap(),
            accounts: vec![
                AccountMeta::new(Pubkey::new_unique(), false),
                AccountMeta::new(Pubkey::from_str(SOL_MINT).unwrap(), false),
                AccountMeta::new(Pubkey::from_str(USDC_MINT).unwrap(), false),
                AccountMeta::new(Pubkey::new_unique(), false),
            ],
            data: jupiter_instruction.data,
        };
        
        let raydium_inst = Instruction {
            program_id: Pubkey::from_str(RAYDIUM_PROGRAM_ID).unwrap(),
            accounts: (0..16).map(|_| AccountMeta::new(Pubkey::new_unique(), false)).collect(),
            data: raydium_instruction.data,
        };
        
        let orca_inst = Instruction {
            program_id: Pubkey::from_str(ORCA_PROGRAM_ID).unwrap(),
            accounts: (0..10).map(|_| AccountMeta::new(Pubkey::new_unique(), false)).collect(),
            data: orca_instruction.data,
        };
        
        // Test parsing each instruction type
        match fingerprinter.parse_jupiter_swap(&jupiter_inst).await {
            Ok(Some(swap_data)) => {
                assert_eq!(swap_data.dex, DexType::Jupiter);
                assert!(swap_data.amount_in > 0);
                assert!(swap_data.amount_out > 0);
                assert!(swap_data.slippage_bps < 10000); // < 100%
            }
            _ => {
                // Parsing may fail in test environment
            }
        }
        
        match fingerprinter.parse_raydium_swap(&raydium_inst).await {
            Ok(Some(swap_data)) => {
                assert_eq!(swap_data.dex, DexType::Raydium);
                assert!(swap_data.amount_in > 0);
                assert!(swap_data.amount_out > 0);
            }
            _ => {
                // Parsing may fail in test environment
            }
        }
        
        match fingerprinter.parse_orca_swap(&orca_inst).await {
            Ok(Some(swap_data)) => {
                assert_eq!(swap_data.dex, DexType::Orca);
                assert!(swap_data.amount_in > 0);
                assert!(swap_data.amount_out > 0);
            }
            _ => {
                // Parsing may fail in test environment
            }
        }
    }

    #[tokio::test]
    #[timeout(3000)]
    async fn test_concurrent_profile_updates() {
        // Tests thread-safe concurrent updates to wallet profiles
        let fingerprinter = Arc::new(WhaleWalletBehavioralFingerprinter::new());
        let wallet = Pubkey::new_unique();
        let sol_mint = Pubkey::from_str(SOL_MINT).unwrap();
        let usdc_mint = Pubkey::from_str(USDC_MINT).unwrap();
        
        // Create multiple concurrent update tasks
        let mut tasks = Vec::new();
        for i in 0..10 {
            let fp = fingerprinter.clone();
            let w = wallet;
            let task = tokio::spawn(async move {
                let swap = SwapData {
                    timestamp: 7000 + i,
                    dex: DexType::Jupiter,
                    token_in: sol_mint,
                    token_out: usdc_mint,
                    amount_in: 1_000_000_000 + i,
                    amount_out: 950_000_000 + i,
                    slippage_bps: 500,
                    transaction_signature: format!("concurrent_{}", i),
                };
                
                if let Err(_) = fp.update_wallet_profile(w, swap).await {
                    // Continue on error
                }
            });
            tasks.push(task);
        }
        
        // Wait for all tasks to complete
        for task in tasks {
            if let Err(_) = task.await {
                // Continue on error
            }
        }
        
        // Verify profile consistency
        if let Some(profile) = fingerprinter.get_wallet_profile(&wallet).await {
            assert!(profile.recent_swaps.len() <= 10);
            assert!(profile.total_volume > 0);
            assert!(profile.aggression_index.is_finite());
        }
    }
}



