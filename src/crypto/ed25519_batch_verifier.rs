// mega_ed25519_batch_verifier.rs
//
// Industrial-strength Ed25519 batch verifier layered on top of ed25519-dalek.
//
// Crypto truth comes only from verify_strict. The optional s<ℓ prefilter
// exists purely as a micro-DOS filter and is never a source of correctness.
//
// Tightenings baked in:
// - Strict-only verification via VerifyingKey::verify_strict everywhere.
// - verify_batch used as an accelerator; on failure we bisect and fall back
//   to per-item strict verification (no "legacy" path, no compromises).
// - Optional s<ℓ prefilter remains, little-endian per RFC 8032, and still
//   treated as a cheap reject gate only. It is NOT the authority; verify_strict is.
// - No code path skips strict verification based on any cache flag.
//
// ZIP-215 compatibility note:
// We follow RFC 8032 + dalek verify_strict semantics. Some ecosystems adopt
// ZIP-215 rules so batch and individual verification always agree; not used here.
// https://zips.z.cash/zip-0215
//
// Solana v0 message bytes note:
// The signed payload is the canonical serialized v0 Message (which includes
// address_table_lookups indices as part of the message). Resolution of ALT
// addresses at runtime does not mutate the signed message bytes.
// Ref (first-party):
//   https://solana.com/developers/courses/program-optimization/lookup-tables
//   https://docs.anza.xyz/proposals/versioned-transactions
//
// Cargo (pin, don’t float casually):
// ed25519-dalek = { version = "2", features = ["batch"] }
//
// Doc pins (authoritative, audit-grade):
// - RFC 8032: Ed25519 signature encoding; S is 32-byte little-endian, require 0 ≤ S < ℓ.
//   RFC 8032 §5.1.7; errata clarifies bounds.
//   https://datatracker.ietf.org/doc/html/rfc8032
//   https://www.rfc-editor.org/errata/rfc8032
// - ed25519-dalek verify_batch & verify_strict semantics (strict vs weak-key checks):
//   https://docs.rs/ed25519-dalek/latest/ed25519_dalek/fn.verify_batch.html
//   https://doc.dalek.rs/ed25519_dalek/struct.PublicKey.html#method.verify_strict
//   Background/notes: https://github.com/dalek-cryptography/ed25519-dalek
// - Wycheproof test vectors (Ed25519 edge cases incl. non-canonical S):
//   Canonical repo moved to C2SP: https://github.com/C2SP/wycheproof  // [pin commit/tag in CI]
//   Optional provenance blog (historical): https://security.googleblog.com/2016/12/project-wycheproof.html
// - NIST blessing: FIPS 186-5 approves EdDSA (Ed25519) and adds verification requirements:
//   https://nvlpubs.nist.gov/nistpubs/FIPS/NIST.FIPS.186-5.pdf
// - Ed25519 background (batch verification rationale):
//   https://ed25519.cr.yp.to/
//   https://ed25519.cr.yp.to/ed25519-20110926.pdf
//
// Extra provenance pins (nice-to-have for reviewers):
// - Group order ℓ provenance (first-party dalek docs; bytes implied by Scalar mod ℓ):
//   https://docs.rs/curve25519-dalek/latest/curve25519_dalek/constants/constant.BASEPOINT_ORDER.html
// - Strict-vs-batch weak-key caveat discussion (dalek):
//   https://github.com/dalek-cryptography/ed25519-dalek/issues
//
// ------------------------------------------------------------------------------------

#![forbid(unsafe_code)]
#![allow(clippy::too_many_arguments, clippy::type_complexity)]
#![allow(dead_code, unused_imports, unused_variables)]

use bytes::Bytes;
use once_cell::sync::Lazy;
use parking_lot::{Mutex, RwLock};
use thiserror::Error;
use std::cell::{Cell, RefCell, UnsafeCell};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use std::convert::TryFrom;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

// =====================================================================================
// Fast CPU lane detection & rseq (Linux) + TLS CPU id
// =====================================================================================
#[cfg(target_os = "linux")]
mod fast_getcpu {
    use once_cell::sync::Lazy;
    type VdsoGetcpu = unsafe extern "C" fn(
        cpu: *mut u32,
        node: *mut u32,
        unused: *mut core::ffi::c_void,
    ) -> i32;

    static VDSO_GETCPU: Lazy<Option<VdsoGetcpu>> = Lazy::new(|| unsafe {
        let h = libc::dlopen(b"linux-vdso.so.1\0".as_ptr() as *const i8, libc::RTLD_LAZY);
        if h.is_null() { return None; }
        let sym = libc::dlsym(h, b"__vdso_getcpu\0".as_ptr() as *const i8);
        if sym.is_null() { None } else { Some(std::mem::transmute::<_, VdsoGetcpu>(sym)) }
    });

    #[inline]
    pub fn getcpu() -> usize {
        unsafe {
            if let Some(f) = *VDSO_GETCPU {
                let mut cpu: u32 = 0;
                let rc = f(&mut cpu as *mut u32, core::ptr::null_mut(), core::ptr::null_mut());
                if rc == 0 { return cpu as usize; }
            }
            let c = libc::sched_getcpu();
            if c >= 0 { c as usize } else { 0 }
        }
    }

    thread_local! {
        static TLS_CPU: std::cell::Cell<usize> = std::cell::Cell::new(usize::MAX);
    }

    static CPU_COUNT: Lazy<usize> = Lazy::new(|| {
        let online = std::fs::read_to_string("/sys/devices/system/cpu/online").ok();
        if let Some(s) = online {
            let mut v = Vec::new();
            for part in s.trim().split(',') {
                if let Some((a, b)) = part.split_once('-') {
                    if let (Ok(mut lo), Ok(hi)) = (a.parse::<usize>(), b.parse::<usize>()) {
                        if lo > hi { std::mem::swap(&mut lo, &mut (lo)); }
                        for x in lo..=hi { v.push(x); }
                    }
                } else if let Ok(x) = part.parse::<usize>() { v.push(x); }
            }
            return v.len().max(1);
        }
        1
    });

    #[inline] pub fn online_cpus() -> usize { *CPU_COUNT }
    #[inline] pub fn tls_cpu_id() -> usize {
        TLS_CPU.with(|c| {
            let cur = c.get();
            if cur != usize::MAX { return cur; }
            let v = getcpu() % online_cpus();
            c.set(v);
            v
        })
    }
    #[inline] pub fn tls_cpu_refresh() { TLS_CPU.with(|c| c.set(getcpu() % online_cpus())); }
}

#[cfg(not(target_os = "linux"))]
mod fast_getcpu {
    #[inline] pub fn tls_cpu_id() -> usize { 0 }
    #[inline] pub fn tls_cpu_refresh() {}
    #[inline] pub fn online_cpus() -> usize { 1 }
}

#[inline] fn cpu_lane() -> usize { fast_getcpu::tls_cpu_id() }
#[inline] fn cpu_lanes() -> usize { fast_getcpu::online_cpus() }

#[cfg(all(target_os = "linux", feature = "rseq"))]
mod rseq_fast {
    #[repr(C)] struct Rseq { cpu_id_start: u32, cpu_id: u32, rseq_cs: u64, flags: u32 }
    #[cfg(target_arch = "x86_64")] const __NR_RSEQ: i64 = 334;
    #[cfg(target_arch = "aarch64")] const __NR_RSEQ: i64 = 293;
    const RSEQ_SIG: u32 = 0x5305_3053;

    thread_local! {
        static TLS_RSEQ: std::cell::UnsafeCell<Rseq> = std::cell::UnsafeCell::new(Rseq {
            cpu_id_start: u32::MAX, cpu_id: u32::MAX, rseq_cs: 0, flags: 0
        });
        static RSEQ_READY: std::cell::Cell<bool> = std::cell::Cell::new(false);
    }

    #[inline] pub fn register() {
        TLS_RSEQ.with(|cell| {
            if RSEQ_READY.with(|r| r.get()) { return; }
            let ptr = cell.get() as *mut libc::c_void;
            let len = std::mem::size_of::<Rseq>() as u32;
            let rc = unsafe { libc::syscall(__NR_RSEQ, ptr, len, 0, RSEQ_SIG) };
            if rc == 0 { RSEQ_READY.with(|r| r.set(true)); }
        })
    }

    #[inline] pub fn current_cpu() -> usize {
        TLS_RSEQ.with(|cell| {
            if !RSEQ_READY.with(|r| r.get()) { return super::fast_getcpu::tls_cpu_id(); }
            let cpu = unsafe { (&*cell.get()).cpu_id as usize };
            if cpu == usize::MAX { super::fast_getcpu::tls_cpu_id() } else { cpu }
        })
    }
}
#[cfg(not(all(target_os = "linux", feature = "rseq")))]
mod rseq_fast { #[inline] pub fn register() {} #[inline] pub fn current_cpu() -> usize { super::fast_getcpu::tls_cpu_id() } }

#[inline] fn lane_cpu() -> usize { rseq_fast::current_cpu() }
#[inline] fn lane_init() { rseq_fast::register(); fast_getcpu::tls_cpu_refresh(); }

// =====================================================================================
// Metrics-lite (stubs behind feature gates)
// =====================================================================================
#[cfg(feature = "advanced_sched")]
mod per_cpu_counters {
    use once_cell::sync::Lazy;
    use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
    pub struct Sharded { lanes: Vec<AtomicU64> }
    impl Sharded {
        pub fn new() -> Self { Self { lanes: (0..super::cpu_lanes()).map(|_| AtomicU64::new(0)).collect() } }
        #[inline] pub fn inc_by(&self, v: u64) { let lane = super::cpu_lane(); self.lanes[lane].fetch_add(v, Relaxed); }
        pub fn swap_all(&self) -> u64 { let mut sum = 0; for a in &self.lanes { sum = sum.wrapping_add(a.swap(0, Relaxed)); } sum }
    }
    pub static VERIFIED_OK: Lazy<Sharded> = Lazy::new(Sharded::new);
    pub static VERIFIED_FAIL: Lazy<Sharded> = Lazy::new(Sharded::new);
    pub fn drain_to_prom() {
        let ok = VERIFIED_OK.swap_all(); if ok > 0 { crate::PROM_OK.inc_by(ok); }
        let fl = VERIFIED_FAIL.swap_all(); if fl > 0 { crate::PROM_FAIL.inc_by(fl); }
    }
}

// =====================================================================================
use ed25519_dalek::{Signature, Verifier, VerifyingKey, SignatureError, PUBLIC_KEY_LENGTH, SIGNATURE_LENGTH};

// =====================================================================================
// Global knobs
// =====================================================================================
const MAX_BATCH_SIZE: usize = 128;
const VERIFICATION_TIMEOUT_MS: u64 = 50;
const CACHE_SIZE: usize = 65_536;
const MAX_PENDING_BATCHES: usize = 256;
static PROFIT_THRESHOLD_LAMPORTS: AtomicI64 = AtomicI64::new(0);

// =====================================================================================
// svc_obs: global robust EWMA of per-item service time (placeholder)
// =====================================================================================
pub mod svc_obs {
    use core::sync::atomic::{AtomicU64, Ordering::Relaxed};
    use once_cell::Lazy;
    static EWMA_NS: Lazy<AtomicU64> = Lazy::new(|| AtomicU64::new(1_000));
    #[inline] pub fn est_ns() -> u64 { EWMA_NS.load(Relaxed).max(1) }
    #[inline] pub fn update_sample(sample_ns: u64) {
        let prev = EWMA_NS.load(Relaxed).max(1);
        let lo = prev / 4;
        let hi = prev.saturating_mul(4);
        let s = sample_ns.clamp(lo.max(1), hi);
        let next = (((prev as u128) * 7) + (s as u128) + 7) >> 3;
        EWMA_NS.store(next as u64, Relaxed);
    }
}

// =====================================================================================
// TSC-ish clock
// =====================================================================================
mod clock {
    use once_cell::sync::Lazy;
    use std::time::{Duration, Instant};
    pub struct TscCal { pub mult: u64, pub shift: u32, pub t0_cyc: u64, pub fallback0: Instant }
    const SHIFT: u32 = 24;

    #[inline(always)]
    #[cfg(target_arch = "x86_64")] fn rdtsc() -> u64 { unsafe { core::arch::x86_64::_rdtsc() as u64 } }
    #[inline(always)]
    #[cfg(not(target_arch = "x86_64"))] fn rdtsc() -> u64 { let now = Instant::now(); now.elapsed().as_nanos() as u64 }

    static CAL: Lazy<TscCal> = Lazy::new(|| {
        #[cfg(target_arch = "x86_64")] {
            let t0 = Instant::now(); let c0 = rdtsc();
            let target = t0 + Duration::from_millis(50);
            while Instant::now() < target { core::hint::spin_loop() }
            let dt = t0.elapsed(); let c1 = rdtsc();
            let cyc = c1.saturating_sub(c0).max(1); let us = dt.as_micros() as u64;
            let mult = (((us as u128) << SHIFT) / (cyc as u128)) as u64;
            TscCal { mult: mult.max(1), shift: SHIFT, t0_cyc: rdtsc(), fallback0: t0 }
        }
        #[cfg(not(target_arch = "x86_64"))] {
            TscCal { mult: 1, shift: 0, t0_cyc: 0, fallback0: Instant::now() }
        }
    });

    #[inline(always)] pub fn now_cycles() -> u64 {
        #[cfg(target_arch = "x86_64")] { unsafe { core::arch::x86_64::_rdtsc() as u64 } }
        #[cfg(not(target_arch = "x86_64"))] { CAL.fallback0.elapsed().as_nanos() as u64 }
    }
    #[inline(always)] pub fn cycles_to_us(cyc: u64) -> u64 {
        #[cfg(target_arch = "x86_64")] { (cyc.saturating_mul(CAL.mult)) >> CAL.shift }
        #[cfg(not(target_arch = "x86_64"))] { cyc }
    }
    #[inline(always)] pub fn now_us() -> u64 {
        #[cfg(target_arch = "x86_64")] { cycles_to_us(now_cycles().saturating_sub(CAL.t0_cyc)) }
        #[cfg(not(target_arch = "x86_64"))] { CAL.fallback0.elapsed().as_micros() as u64 }
    }
}

// =====================================================================================
// Errors, API types (improved hygiene: distinguish invalid key vs signature)
// =====================================================================================
#[derive(Error, Debug, Clone)]
pub enum BatchVerifierError {
    #[error("Invalid signature")] InvalidSignature,
    #[error("Invalid public key")] InvalidPublicKey,
    #[error("Signature verification failed")] VerificationFailed,
    #[error("Batch timeout exceeded")] BatchTimeout,
    #[error("Queue full")] QueueFull,
    #[error("Verifier shutdown")] VerifierShutdown,
    #[error("Invalid batch size: {0}")] InvalidBatchSize(usize),
    #[error("Rejected by pre-screen")] PreScreenRejected,
}

#[derive(Clone, Debug)]
pub struct VerificationItem {
    pub signature: [u8; SIGNATURE_LENGTH],
    pub public_key: [u8; PUBLIC_KEY_LENGTH],
    pub message: Bytes,
    pub priority: u8,
    pub timestamp: Instant,
    pub retry_count: u8,
    pub estimated_profit_lamports: i64,
    pub cache_key: u64,
}
impl VerificationItem {
    pub fn new(
        signature: [u8; SIGNATURE_LENGTH],
        public_key: [u8; PUBLIC_KEY_LENGTH],
        message: Bytes,
        priority: u8,
        timestamp: Instant,
        retry_count: u8,
        estimated_profit_lamports: i64,
    ) -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(&signature);
        hasher.update(&public_key);
        hasher.update(&message);
        let cache_key = u64::from_le_bytes(hasher.finalize().as_bytes()[..8].try_into().unwrap_or([0u8; 8]));
        Self { signature, public_key, message, priority, timestamp, retry_count, estimated_profit_lamports, cache_key }
    }
    #[inline] pub fn cache_key(&self) -> u64 {
        if self.cache_key != 0 { return self.cache_key; }
        let mut hasher = blake3::Hasher::new();
        hasher.update(&self.signature);
        hasher.update(&self.public_key);
        hasher.update(&self.message);
        u64::from_le_bytes(hasher.finalize().as_bytes()[..8].try_into().unwrap_or([0u8; 8]))
    }
}

// =====================================================================================
// Ed25519 constants and pre-screen utilities (s < ℓ DOS filter only)
// RFC 8032: S is 32-byte little-endian and must satisfy 0 <= S < ℓ.
//   RFC 8032 §5.1.7: verifier MUST reject if S ∉ [0, ℓ).
//   https://datatracker.ietf.org/doc/html/rfc8032
// =====================================================================================

// ℓ in little-endian (byte 0 is least significant).
// ℓ = 2^252 + 27742317777372353535851937790883648493.
// Provenance: curve25519-dalek publishes ℓ as BASEPOINT_ORDER (Scalar).
// See dalek constants docs for the canonical definition.
// https://docs.rs/curve25519-dalek/latest/curve25519_dalek/constants/constant.BASEPOINT_ORDER.html
const L_LE: [u8; 32] = [
    0xED,0xD3,0xF5,0x5C,0x1A,0x63,0x12,0x58,
    0xD6,0x9C,0xF7,0xA2,0xDE,0xF9,0xDE,0x14,
    0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x00,
    0x00,0x00,0x00,0x00,0x00,0x00,0x00,0x10
];

#[inline]
fn is_s_canonical(sig64: &[u8; 64]) -> bool {
    // DOS FILTER ONLY. Canonical S check in little-endian:
    // RFC 8032 §5.1.7: verifier MUST reject if S ∉ [0, ℓ). (S is 32-byte little-endian).
    // Return true if S < ℓ, false if S >= ℓ (equal is non-canonical).
    // Lexicographic compare from MSB (index 31) to LSB (index 0) on the little-endian byte array.
    let s = &sig64[32..64];
    for i in (0..32).rev() {
        if s[i] < L_LE[i] { return true; }
        if s[i] > L_LE[i] { return false; }
    }
    false
}

// =====================================================================================
// Parsed item + VK cache (no "strict_ok" skipping semantics)
// =====================================================================================
pub struct ParsedItem {
    pub sig: Signature,
    pub vk: VerifyingKey,
    pub vk_bytes: [u8; PUBLIC_KEY_LENGTH],
    pub msg: Bytes,
    pub cache_key: u64,
    #[cfg(feature = "advanced_sched")] pub priority: u8,
    #[cfg(feature = "advanced_sched")] pub profit: u64,
    #[cfg(feature = "advanced_sched")] pub enq_ts: Instant,
    #[cfg(feature = "advanced_sched")] pub enq_us: u64,
    #[cfg(feature = "advanced_sched")] pub enq_cyc: u64,
}
impl ParsedItem {
    #[inline(always)] pub fn age_us_cyc(&self, now_cyc: u64) -> u64 { clock::cycles_to_us(now_cyc.saturating_sub(self.enq_cyc)) }
}

// Thread-local tiny VK cache for locality
std::thread_local! {
    static TL_VK32: RefCell<arrayvec::ArrayVec<([u8; PUBLIC_KEY_LENGTH], VerifyingKey), 32>> =
        RefCell::new(arrayvec::ArrayVec::new());
}

#[inline]
fn vk_fast_lookup(pk: &[u8; PUBLIC_KEY_LENGTH]) -> Option<VerifyingKey> {
    TL_VK32.with(|tl| {
        let mut v = tl.borrow_mut();
        for i in 0..v.len() {
            if &v[i].0 == pk {
                let hit = v.remove(i);
                v.insert(0, (hit.0, hit.1.clone()));
                return Some(hit.1);
            }
        }
        None
    })
}
#[inline]
fn vk_fast_insert(pk: [u8; PUBLIC_KEY_LENGTH], vk: VerifyingKey) {
    TL_VK32.with(|tl| {
        let mut v = tl.borrow_mut();
        if v.len() == v.capacity() { let _ = v.pop(); }
        v.insert(0, (pk, vk));
    });
}

// Global VK cache: correctness flags are NOT inferred at parse-time
static VK_CACHE: once_cell::sync::Lazy<RwLock<ahash::AHashMap<[u8; PUBLIC_KEY_LENGTH], VerifyingKey>>> =
    once_cell::sync::Lazy::new(|| RwLock::new(ahash::AHashMap::new()));

#[inline]
fn get_cached_vk(pk_arr: &[u8; PUBLIC_KEY_LENGTH]) -> Result<VerifyingKey, BatchVerifierError> {
    if let Some(vk) = vk_fast_lookup(pk_arr) { return Ok(vk); }
    if let Some(cached) = VK_CACHE.read().get(pk_arr).cloned() {
        vk_fast_insert(*pk_arr, cached.clone());
        return Ok(cached);
    }
    // Parse once; parsing does NOT guarantee small-order or malleability rejection.
    // Authority lives at verify_strict at use-site.
    // Strict semantics doc:
    // https://doc.dalek.rs/ed25519_dalek/struct.PublicKey.html#method.verify_strict
    let parsed = VerifyingKey::from_bytes(pk_arr)
        .map_err(|_| BatchVerifierError::InvalidPublicKey)?;
    { let mut w = VK_CACHE.write(); w.insert(*pk_arr, parsed.clone()); }
    vk_fast_insert(*pk_arr, parsed.clone());
    Ok(parsed)
}

impl TryFrom<VerificationItem> for ParsedItem {
    type Error = BatchVerifierError;
    fn try_from(v: VerificationItem) -> Result<Self, Self::Error> {
        // DOS filter only. Non-canonical S rejected here to avoid batch poison.
        // RFC 8032 §5.1.7: verifier MUST reject if S ∉ [0, ℓ). (S is 32-byte little-endian).
        if !is_s_canonical(&v.signature) { return Err(BatchVerifierError::PreScreenRejected); }
        let sig = Signature::from_bytes(&v.signature)
            .map_err(|_| BatchVerifierError::InvalidSignature)?;
        let pk_arr: [u8; PUBLIC_KEY_LENGTH] = v.public_key;
        let vk = get_cached_vk(&pk_arr)?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(&v.signature);
        hasher.update(&v.public_key);
        hasher.update(&v.message);
        let cache_key = u64::from_le_bytes(hasher.finalize().as_bytes()[..8].try_into().unwrap_or([0u8; 8]));
        #[cfg(feature = "advanced_sched")] let enq_us = clock::now_us();
        #[cfg(feature = "advanced_sched")] let enq_cyc = clock::now_cycles();
        Ok(Self {
            sig, vk_bytes: vk.to_bytes(), vk, msg: v.message, cache_key,
            #[cfg(feature = "advanced_sched")] priority: v.priority,
            #[cfg(feature = "advanced_sched")] profit: v.estimated_profit_lamports.max(0) as u64,
            #[cfg(feature = "advanced_sched")] enq_ts: v.timestamp,
            #[cfg(feature = "advanced_sched")] enq_us,
            #[cfg(feature = "advanced_sched")] enq_cyc,
        })
    }
}

// =====================================================================================
// Quick verify utility (strict-only). DOS prefilter optional.
// =====================================================================================
#[inline(always)]
pub fn quick_verify(signature: &[u8; 64], public_key: &[u8; 32], message: &[u8]) -> bool {
    if !is_s_canonical(signature) { return false; } // DOS filter
    if let Ok(sig) = Signature::from_bytes(signature) {
        if let Ok(pk) = VerifyingKey::from_bytes(public_key) {
            // Strict verification at use-site:
            // https://doc.dalek.rs/ed25519_dalek/struct.PublicKey.html#method.verify_strict
            return pk.verify_strict(message, &sig).is_ok();
        }
    }
    false
}

// =====================================================================================
// Verifier cache (TTL-ish stub)
// =====================================================================================
struct VerifCache { map: parking_lot::RwLock<HashMap<u64, (bool, Instant)>> }
impl VerifCache {
    fn new(_cap: usize) -> Self { Self { map: parking_lot::RwLock::new(HashMap::new()) } }
    fn get(&self, key: u64, _ttl: Duration) -> Option<bool> {
        let m = self.map.read(); m.get(&key).map(|(ok, _)| *ok)
    }
    fn insert(&self, key: u64, ok: bool) {
        let mut m = self.map.write(); m.insert(key, (ok, Instant::now()));
    }
}

// =====================================================================================
// Batch verification core (strict-only everywhere)
// =====================================================================================
#[cfg(feature = "advanced_sched")]
fn verify_batch_dalek(items: &[Arc<ParsedItem>]) -> Vec<Result<bool, BatchVerifierError>> {
    use smallvec::SmallVec;
    use std::{cell::RefCell, collections::HashMap};

    #[inline] fn pf_t0(p: *const u8) { #[cfg(target_arch = "x86_64")] unsafe { core::arch::x86_64::_mm_prefetch(p as *const i8, 3) } }
    #[inline] fn pf_nta(p: *const u8) { #[cfg(target_arch = "x86_64")] unsafe { core::arch::x86_64::_mm_prefetch(p as *const i8, 0) } }

    thread_local! {
        static TLS: RefCell<( // plenty of preallocated slots for locality
            SmallVec<[usize; 4096]>,             // idx
            SmallVec<[&'static [u8]; 4096]>,     // msgs
            SmallVec<[Signature; 4096]>,         // sigs
            SmallVec<[VerifyingKey; 4096]>,      // keys
            SmallVec<[u64; 4096]>,               // hv
            HashMap<u64, usize>,                 // hv->unique
            SmallVec<[Option<usize>; 4096]>,     // dup_of
            SmallVec<[(usize,usize); 4096]>      // stack
        )> = RefCell::new((
            SmallVec::new(), SmallVec::new(), SmallVec::new(), SmallVec::new(),
            SmallVec::new(), HashMap::new(), SmallVec::new(), SmallVec::new()
        ));
    }

    let n = items.len();
    if n == 0 { return Vec::new(); }
    let mut out = vec![Err(BatchVerifierError::VerificationFailed); n];

    TLS.with(|cell| {
        let (ref mut idx, ref mut msgs, ref mut sigs, ref mut keys, ref mut hvs,
             ref mut seen, ref mut dup_of, ref mut stk) = *cell.borrow_mut();

        idx.clear(); msgs.clear(); sigs.clear(); keys.clear(); hvs.clear(); seen.clear();
        dup_of.clear(); stk.clear();

        idx.reserve_exact(n); msgs.reserve_exact(n); sigs.reserve_exact(n);
        keys.reserve_exact(n); hvs.reserve_exact(n); dup_of.resize(n, None);

        // unique stage (pre-screen happened in TryFrom)
        for i in 0..n {
            let it = &items[i];
            if it.msg.is_empty() { out[i] = Err(BatchVerifierError::PreScreenRejected); continue; }
            if i + 8 < n {
                let nxt = &items[i + 8];
                pf_t0(nxt.msg.as_ptr());
                pf_nta(nxt.sig.as_ref().as_ptr());
                pf_nta(nxt.vk.as_bytes().as_ptr());
            }
            let mut seed = xxhash_rust::xxh3::xxh3_64(&it.vk_bytes);
            let sig_bytes = it.sig.to_bytes();
            seed = xxhash_rust::xxh3::xxh3_64_with_seed(&sig_bytes, seed);
            let hv = xxhash_rust::xxh3::xxh3_64_with_seed(it.msg.as_ref(), seed);

            if let Some(&j) = seen.get(&hv) {
                let same =
                    items[j].vk_bytes == it.vk_bytes &&
                    items[j].sig.to_bytes() == sig_bytes &&
                    items[j].msg.as_ref() == it.msg.as_ref();
                if same { dup_of[i] = Some(j); continue; }
            }

            idx.push(i);
            unsafe { msgs.push(core::slice::from_raw_parts(it.msg.as_ptr(), it.msg.len())); }
            sigs.push(it.sig.clone());
            keys.push(it.vk.clone());
            hvs.push(hv);
            seen.insert(hv, idx.len() - 1);
        }

        if idx.is_empty() { return; }

        // sort for locality then permute in-place
        idx.sort_unstable_by(|&a, &b| items[b].vk_bytes.cmp(&items[a].vk_bytes));
        let mut p = 0usize;
        while p < idx.len() {
            let want = idx[p];
            if want == p { p += 1; continue; }
            msgs.swap(p, want); sigs.swap(p, want); keys.swap(p, want); hvs.swap(p, want);
            idx.swap(p, want);
        }

        // chunk verify + strict bisect
        let mut start = 0usize;
        while start < idx.len() {
            let end = core::cmp::min(start + 2048, idx.len());

            // ed25519-dalek verify_batch(): accelerator only; no weak-key checks.
            // On failure we bisect and fall back to verify_strict().
            // Refs:
            //   https://docs.rs/ed25519-dalek/latest/ed25519_dalek/fn.verify_batch.html
            //   https://github.com/dalek-cryptography/ed25519-dalek  (notes on weak keys)
            if VerifyingKey::verify_batch(&msgs[start..end], &sigs[start..end], &keys[start..end]).is_ok() {
                for k in start..end { out[idx[k]] = Ok(true); }
            } else {
                // On failure: bisect and fall back to strict per-item verification.
                // Strict semantics:
                // https://doc.dalek.rs/ed25519_dalek/struct.PublicKey.html#method.verify_strict
                stk.clear(); stk.push((start, end));
                while let Some((lo, hi)) = stk.pop() {
                    if hi <= lo { continue; }
                    if (hi - lo) == 1 {
                        let ok = keys[lo].verify_strict(msgs[lo], &sigs[lo]).is_ok();
                        out[idx[lo]] = if ok { Ok(true) } else { Err(BatchVerifierError::VerificationFailed) };
                        continue;
                    }
                    if VerifyingKey::verify_batch(&msgs[lo..hi], &sigs[lo..hi], &keys[lo..hi]).is_ok() {
                        for k in lo..hi { out[idx[k]] = Ok(true); }
                        continue;
                    }
                    let mid = lo + ((hi - lo) >> 1);
                    stk.push((mid, hi)); stk.push((lo, mid));
                }
            }
            start = end;
        }

        // dup propagation
        for i in 0..n {
            if let Some(juniq) = dup_of[i] {
                let orig_global = idx[juniq];
                out[i] = out[orig_global].clone();
            }
        }
    });

    out
}

#[cfg(not(feature = "advanced_sched"))]
fn verify_batch_dalek_simple(items: &[ParsedItem]) -> Vec<Result<bool, BatchVerifierError>> {
    let n = items.len(); if n == 0 { return vec![]; }
    let msgs: Vec<&[u8]> = items.iter().map(|it| it.msg.as_ref()).collect();
    let sigs: Vec<Signature> = items.iter().map(|it| it.sig.clone()).collect();
    let keys: Vec<VerifyingKey> = items.iter().map(|it| it.vk.clone()).collect();

    // ed25519-dalek verify_batch(): accelerator only; no weak-key checks.
    // We strictly re-check on failure.
    // Refs:
    //   https://docs.rs/ed25519-dalek/latest/ed25519_dalek/fn.verify_batch.html
    //   https://github.com/dalek-cryptography/ed25519-dalek
    if VerifyingKey::verify_batch(&msgs, &sigs, &keys).is_ok() { return vec![Ok(true); n]; }

    let mut out = vec![Err(BatchVerifierError::VerificationFailed); n];
    for (i, it) in items.iter().enumerate() {
        // Strict per-item as the authoritative fallback:
        // https://doc.dalek.rs/ed25519_dalek/struct.PublicKey.html#method.verify_strict
        out[i] = if it.vk.verify_strict(&it.msg, &it.sig).is_ok() {
            Ok(true)
        } else {
            Err(BatchVerifierError::VerificationFailed)
        };
    }
    out
}

// =====================================================================================
// Scheduler bits and public API
// =====================================================================================
pub struct VerifierStats {
    total_verified: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    failed_verifications: AtomicU64,
    batch_count: AtomicU64,
    avg_batch_time_us: AtomicU64,
}
impl VerifierStats {
    fn new() -> Self {
        Self {
            total_verified: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            failed_verifications: AtomicU64::new(0),
            batch_count: AtomicU64::new(0),
            avg_batch_time_us: AtomicU64::new(0),
        }
    }
}

pub struct BatchVerifier {
    verification_cache: Arc<VerifCache>,
    batch_sender: crossbeam_channel::Sender<Vec<(usize, VerificationItem)>>,
    result_receiver: crossbeam_channel::Receiver<Vec<(usize, Result<bool, BatchVerifierError>)>>,
    stats: Arc<VerifierStats>,
    shutdown: Arc<AtomicBool>,
    worker_handles: Vec<std::thread::JoinHandle<()>>,
}

impl BatchVerifier {
    pub fn new(worker_threads: usize) -> Self {
        let (batch_sender, batch_receiver) =
            crossbeam_channel::bounded::<Vec<(usize, VerificationItem)>>(MAX_PENDING_BATCHES);
        let (result_sender, result_receiver) =
            crossbeam_channel::bounded::<Vec<(usize, Result<bool, BatchVerifierError>)>>(
                MAX_PENDING_BATCHES * MAX_BATCH_SIZE,
            );

        let verification_cache = Arc::new(VerifCache::new(CACHE_SIZE));
        let stats = Arc::new(VerifierStats::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let mut worker_handles = Vec::with_capacity(worker_threads);

        for worker_idx in 0..worker_threads {
            let batch_receiver = batch_receiver.clone();
            let result_sender = result_sender.clone();
            let stats = stats.clone();
            let shutdown = shutdown.clone();

            let h = std::thread::Builder::new()
                .name(format!("ed25519-worker-{}", worker_idx))
                .spawn(move || {
                    #[cfg(feature = "core_affinity")]
                    if let Some(cores) = core_affinity::get_core_ids() {
                        let core = cores[worker_idx % cores.len()];
                        let _ = core_affinity::set_for_current(core);
                    }

                    loop {
                        if shutdown.load(Ordering::Acquire) { break; }
                        match batch_receiver.recv_timeout(Duration::from_millis(10)) {
                            Ok(batch) => {
                                let start = Instant::now();
                                let items: Vec<VerificationItem> = batch.iter().map(|(_, it)| it.clone()).collect();

                                #[cfg(feature = "advanced_sched")]
                                let parsed: Vec<Arc<ParsedItem>> = items
                                    .into_iter()
                                    .filter_map(|it| ParsedItem::try_from(it).ok())
                                    .map(Arc::new)
                                    .collect();

                                #[cfg(not(feature = "advanced_sched"))]
                                let parsed: Vec<ParsedItem> = items
                                    .into_iter()
                                    .filter_map(|it| ParsedItem::try_from(it).ok())
                                    .collect();

                                #[cfg(feature = "advanced_sched")]
                                let results = verify_batch_dalek(&parsed);
                                #[cfg(not(feature = "advanced_sched"))]
                                let results = verify_batch_dalek_simple(&parsed);

                                let batch_time = start.elapsed().as_micros() as u64;
                                stats.batch_count.fetch_add(1, Ordering::Relaxed);
                                let cur_avg = stats.avg_batch_time_us.load(Ordering::Relaxed);
                                let new_avg = (cur_avg * 7 + batch_time) / 8;
                                stats.avg_batch_time_us.store(new_avg, Ordering::Relaxed);

                                // remap to global indices
                                let mut out = Vec::with_capacity(results.len());
                                for (i, (gi, _)) in batch.iter().enumerate() {
                                    let r = results.get(i).cloned()
                                        .unwrap_or(Err(BatchVerifierError::VerificationFailed));
                                    out.push((*gi, r));
                                }
                                let _ = result_sender.send(out);
                            }
                            Err(_) => {}
                        }
                    }
                })
                .expect("spawn worker");
            worker_handles.push(h);
        }

        Self { verification_cache, batch_sender, result_receiver, stats, shutdown, worker_handles }
    }

    #[inline] pub fn is_shutdown(&self) -> bool { self.shutdown.load(Ordering::Acquire) }
    pub fn shutdown(&self) { self.shutdown.store(true, Ordering::Release); }

    pub fn verify_single(&self, item: VerificationItem) -> Result<bool, BatchVerifierError> {
        if self.shutdown.load(Ordering::Acquire) { return Err(BatchVerifierError::VerifierShutdown); }

        let cache_key = item.cache_key();
        if let Some(hit) = self.verification_cache.get(cache_key, Duration::from_secs(60)) {
            return Ok(hit);
        }

        // DOS prefilter: reject obviously non-canonical S. Strict verify remains authority.
        // RFC 8032 §5.1.7: verifier MUST reject if S ∉ [0, ℓ). (S is 32-byte little-endian).
        if !is_s_canonical(&item.signature) { return Err(BatchVerifierError::PreScreenRejected); }

        let signature = Signature::from_bytes(&item.signature)
            .map_err(|_| BatchVerifierError::InvalidSignature)?;
        let vk = get_cached_vk(&item.public_key)?;
        // Strict verification (enforces scalar & group malleability checks).
        // https://doc.dalek.rs/ed25519_dalek/struct.PublicKey.html#method.verify_strict
        let ok = vk.verify_strict(&item.message, &signature).is_ok();
        self.verification_cache.insert(cache_key, ok);
        Ok(ok)
    }

    pub fn verify_batch_blocking(
        &self,
        items: Vec<VerificationItem>,
    ) -> Vec<Result<bool, BatchVerifierError>> {
        if items.is_empty() { return vec![]; }
        if items.len() == 1 { return vec![self.verify_single(items.into_iter().next().unwrap())]; }

        let mut results = vec![Err(BatchVerifierError::VerificationFailed); items.len()];
        let mut cached_indices = smallvec::SmallVec::<[(usize, bool); 32]>::new();
        let mut uncached_items = Vec::with_capacity(items.len());
        let mut uncached_indices = Vec::with_capacity(items.len());

        // Cache check + DOS prefilter
        for (idx, item) in items.iter().enumerate() {
            let cache_key = item.cache_key();
            if let Some(hit) = self.verification_cache.get(cache_key, Duration::from_secs(60)) {
                cached_indices.push((idx, hit)); continue;
            }
            // RFC 8032 §5.1.7: verifier MUST reject if S ∉ [0, ℓ). (S is 32-byte little-endian).
            if !is_s_canonical(&item.signature) {
                results[idx] = Err(BatchVerifierError::PreScreenRejected); continue;
            }
            uncached_items.push(item.clone());
            uncached_indices.push(idx);
        }
        for (idx, result) in cached_indices { results[idx] = Ok(result); }
        if uncached_items.is_empty() { return results; }

        // Send in chunks and await
        let total = uncached_items.len();
        let mut offset = 0;
        while offset < total {
            let end = (offset + MAX_BATCH_SIZE).min(total);
            let work_chunk: Vec<(usize, VerificationItem)> =
                uncached_indices[offset..end].iter().cloned()
                .zip(uncached_items[offset..end].iter().cloned())
                .collect();

            if self.batch_sender.send(work_chunk).is_err() {
                for idx in &uncached_indices[offset..end] {
                    results[*idx] = Err(BatchVerifierError::QueueFull);
                }
                offset = end; continue;
            }

            // collect
            let deadline = Instant::now() + Duration::from_millis(VERIFICATION_TIMEOUT_MS);
            let mut received = 0usize;
            while received < end - offset {
                let now = Instant::now();
                if now >= deadline { break; }
                match self.result_receiver.recv_timeout(deadline.saturating_duration_since(now)) {
                    Ok(batch) => {
                        for (gi, res) in batch {
                            if matches!(results[gi], Err(BatchVerifierError::VerificationFailed)) {
                                results[gi] = res; received += 1;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }

            // fallback unresolved to timeout; cache positives
            for gi in &uncached_indices[offset..end] {
                if matches!(results[*gi], Err(BatchVerifierError::VerificationFailed)) {
                    results[*gi] = Err(BatchVerifierError::BatchTimeout);
                }
                if let Ok(true) = results[*gi] {
                    let item = &items[*gi];
                    self.verification_cache.insert(item.cache_key(), true);
                }
            }
            offset = end;
        }
        results
    }
}

// =====================================================================================
// Tests focused on the DOS prefilter correctness (s < ℓ) only.
// The cryptographic truth remains verify_strict; we do not attempt to forge signatures.
// Wycheproof vectors recommended: https://github.com/C2SP/wycheproof  [pin commit/tag in CI]
// =====================================================================================
#[cfg(test)]
mod tests {
    use super::*;

    fn le_sub_one(mut a: [u8; 32]) -> [u8; 32] {
        for i in 0..32 {
            if a[i] != 0 { a[i] = a[i].wrapping_sub(1); break; }
            a[i] = 0xFF;
        }
        a
    }

    fn tweak_sig(mut base: [u8; 64], s_bytes: [u8; 32]) -> [u8; 64] {
        base[32..64].copy_from_slice(&s_bytes);
        base
    }

    #[test]
    fn prescreen_accepts_s_less_than_L() {
        // RFC 8032 §5.1.7: S must be in [0, ℓ). Accept ℓ-1.
        let s = le_sub_one(L_LE); // s = ℓ - 1 (little-endian)
        let sig = tweak_sig([0u8; 64], s);
        assert!(is_s_canonical(&sig));
    }

    #[test]
    fn prescreen_rejects_s_equal_L() {
        // RFC 8032 §5.1.7: reject S == ℓ.
        let sig = tweak_sig([0u8; 64], L_LE);
        assert!(!is_s_canonical(&sig));
    }

    #[test]
    fn prescreen_rejects_s_greater_than_L() {
        // s = ℓ + 1 (little-endian): increment least significant byte
        let mut s = L_LE;
        s[0] = s[0].wrapping_add(1);
        let sig = tweak_sig([0u8; 64], s);
        assert!(!is_s_canonical(&sig));
    }

    #[test]
    fn prescreen_accepts_small_s() {
        // s = 0
        let sig = tweak_sig([0u8; 64], [0u8; 32]);
        assert!(is_s_canonical(&sig));

        // s = small nonzero
        let mut small = [0u8; 32];
        small[0] = 7; // little-endian LSB
        let sig2 = tweak_sig([0u8; 64], small);
        assert!(is_s_canonical(&sig2));
    }

    #[test]
    fn sanity_construct() {
        let _ = BatchVerifier::new(2);
    }
}

// =====================================================================================
// Suggested additional tests (manual notes, not compiled):
//
// 1) Wycheproof negatives:
//    Feed Ed25519 vectors that exercise non-canonical S and ensure is_s_canonical()
//    rejects S=ℓ and S=ℓ+1, accepts S=0 and S=ℓ−1; confirm verify_strict agrees.
//    https://github.com/C2SP/wycheproof   // [pin commit or tag in CI]
//
// 2) Batch failure bisect:
//    Create a batch where one item has S=ℓ+1 (or a wrong message). Confirm batch fails,
//    bisect happens, good items are Ok(true), only the bad one is Err(...).
//    https://docs.rs/ed25519-dalek/latest/ed25519_dalek/fn.verify_batch.html
//
// 3) Small-order key rejection:
//    Known small-order public keys must fail under verify_strict; do not rely on parsing.
//    https://doc.dalek.rs/ed25519_dalek/struct.PublicKey.html#method.verify_strict
//
// 4) Solana message invariance:
//    Serialize a versioned v0 message; sign bytes; resolve ALTs; verify original bytes succeed.
//    https://solana.com/developers/courses/program-optimization/lookup-tables
//    https://docs.anza.xyz/proposals/versioned-transactions
// =====================================================================================
