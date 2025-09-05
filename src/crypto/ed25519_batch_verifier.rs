// NUMA/core pinning helper
#[cfg(all(feature = "core_affinity", target_os = "linux"))]
fn pin_to_core_and_node(core_id: usize) {
    use numa::{bind, Node};
    if let Some(cores) = core_affinity::get_core_ids() {
        if let Some(core) = cores.get(core_id) {
            let _ = core_affinity::set_for_current(*core);
            let node = Node::of_cpu(core.id).unwrap_or(0);
            let _ = bind(Node::new(node));
        }

// ========================= Advanced sync core with NUMA work-stealing =========================
#[cfg(feature = "advanced_sched")]
type ShardItem = Arc<ParsedItem>;

#[cfg(feature = "advanced_sched")]
struct PriorityShardsParsed {
    shards: Vec<Injector<ShardItem>>,
}
#[cfg(feature = "advanced_sched")]
impl PriorityShardsParsed {
    fn new() -> Self {
        let mut shards = Vec::with_capacity(PRIORITY_LEVELS);
        for _ in 0..PRIORITY_LEVELS { shards.push(Injector::new()); }
        Self { shards }
    }
    #[inline]
    fn push(&self, item: ShardItem) {
        self.shards[item.priority as usize].push(item);
    }
    fn drain_ordered(&self, target: usize, out: &mut Vec<ShardItem>) {
        out.clear();
        for pr in (0..PRIORITY_LEVELS).rev() {
            while out.len() < target {
                match self.shards[pr].steal() {
                    Steal::Success(it) => out.push(it),
                    Steal::Empty => break,
                    Steal::Retry => continue,
                }
            }
            if out.len() >= target { break; }
        }
    }
}

#[cfg(feature = "advanced_sched")]
pub struct Ed25519BatchVerifier {
    shutdown: Arc<AtomicBool>,
    inflight: Arc<AtomicU64>,
    // scheduling
    priority_inbox: Arc<PriorityShardsParsed>,
    injector: Arc<Injector<ShardItem>>,
    workers: Vec<Worker<ShardItem>>,
    stealers: Vec<Stealer<ShardItem>>,
    // cache + admission
    cache: TinyLfuCache<u64, bool>,
    admit: Arc<Admittance>,
    // metrics + tuning
    metrics: Arc<Metrics>,
    pid: Mutex<Pid>,
    target_latency_us: u64,
    batch_size: Mutex<usize>,
}

#[cfg(feature = "advanced_sched")]
impl Ed25519BatchVerifier {
    pub fn new(num_workers: usize, target_latency_us: u64, cache_capacity: u64) -> Arc<Self> {
        let shutdown = Arc::new(AtomicBool::new(false));
        let injector = Arc::new(Injector::new());
        let mut workers = Vec::with_capacity(num_workers);
        let mut stealers = Vec::with_capacity(num_workers);
        for _ in 0..num_workers {
            let w = Worker::new_fifo();
            stealers.push(w.stealer());
            workers.push(w);
        }
        let cache = TinyLfuCache::builder()
            .max_capacity(cache_capacity)
            .time_to_live(Duration::from_secs(60))
            .build();
        let this = Arc::new(Self {
            shutdown: shutdown.clone(),
            inflight: Arc::new(AtomicU64::new(0)),
            priority_inbox: Arc::new(PriorityShardsParsed::new()),
            injector,
            workers,
            stealers,
            cache,
            admit: Arc::new(Admittance::new()),
            metrics: Arc::new(Metrics::new()),
            pid: Mutex::new(Pid::new(0.015, 0.002, 0.010)),
            target_latency_us,
            batch_size: Mutex::new(MIN_BATCH_SIZE),
        });
        // Spawn worker threads
        for i in 0..num_workers {
            let s = Arc::clone(&this);
            let core_count = core_affinity::get_core_ids().map(|v| v.len()).unwrap_or(1);
            let core_id = if core_count > 0 { i % core_count } else { 0 };
            std::thread::Builder::new()
                .name(format!("ed25519-worker-{}", i))
                .spawn(move || { pin_to_core_and_node(core_id); s.worker_loop(i); })
                .expect("spawn worker");
        }
        // Scheduler thread: pulls from priority shards into injector
        {
            let s = Arc::clone(&this);
            std::thread::Builder::new()
                .name("ed25519-scheduler".into())
                .spawn(move || s.scheduler_loop())
                .expect("spawn scheduler");
        }
        this
    }

    fn scheduler_loop(&self) {
        let mut buf: Vec<ShardItem> = Vec::with_capacity(MAX_BATCH_SIZE * 4);
        while !self.shutdown.load(Ordering::Acquire) {
            if (self.inflight.load(Ordering::Relaxed) as usize) >= MAX_INFLIGHT_ITEMS {
                std::thread::sleep(Duration::from_micros(50));
                continue;
            }
            self.priority_inbox.drain_ordered(MAX_BATCH_SIZE * 4, &mut buf);
            if buf.is_empty() {
                std::thread::sleep(Duration::from_micros(50));
                continue;
            }
            // Profit-aware local ordering inside this burst
            buf.sort_unstable_by_key(|it| Reverse(mk_priority_key(it.priority, it.profit, it.enq_ts)));
            // Distribute to injector; workers will steal
            for it in buf.drain(..) { self.injector.push(it); }
        }
    }

    fn worker_loop(&self, worker_id: usize) {
        let local = &self.workers[worker_id];
        let stealers = &self.stealers;
        let mut batch: Vec<ShardItem> = Vec::with_capacity(MAX_BATCH_SIZE);
        while !self.shutdown.load(Ordering::Acquire) {
            batch.clear();
            let target = self.cur_batch_size();
            // Fill from local -> injector -> stealers
            while batch.len() < target {
                if let Some(it) = local.pop() { batch.push(it); continue; }
                match self.injector.steal() {
                    Steal::Success(it) => { batch.push(it); continue; }
                    Steal::Retry => continue,
                    Steal::Empty => {
                        let mut stole_any = false;
                        for st in stealers {
                            match st.steal() {
                                Steal::Success(it) => { batch.push(it); stole_any = true; if batch.len() >= target { break; } }
                                Steal::Retry => continue,
                                Steal::Empty => {}
                            }
                        }
                        if !stole_any { break; }
                    }
                }
            }
            if batch.is_empty() { std::thread::sleep(Duration::from_micros(50)); continue; }

            let start = Instant::now();
            for it in &batch { self.metrics.queue_wait_us.observe(it.enq_ts.elapsed().as_micros() as f64); }
            // Verify using SIMD batch core with binary-search isolation
            let results = Self::verify_batch_dalek(&batch);
            let elapsed = start.elapsed().as_micros() as u64;
            self.metrics.batch_time_us.observe(elapsed as f64);
            self.metrics.batches.inc();

            for (it, res) in batch.into_iter().zip(results.into_iter()) {
                match res {
                    Ok(true) => {
                        self.cache.insert(it.cache_key, true);
                        self.metrics.total_verified.inc();
                        let pw = (it.profit as f64).ln_1p() * (elapsed as f64);
                        self.metrics.profit_weighted_latency.observe(pw);
                        // track failures for blacklist only on failures
                    }
                    Ok(false) => {
                        self.cache.insert(it.cache_key, false);
                        self.metrics.failed_verifications.inc();
                        // blacklist escalation on excessive fails
                        self.fail_stats.note(it.vk.to_bytes().into(), false, &self.admit);
                    }
                    Err(_) => {
                        self.metrics.failed_verifications.inc();
                        self.fail_stats.note(it.vk.to_bytes().into(), false, &self.admit);
                    }
                }
                self.inflight.fetch_sub(1, Ordering::Relaxed);
            }
            // PID adjust batch size toward target latency
            {
                let mut pid = self.pid.lock();
                let adj = pid.update(self.target_latency_us as f64, elapsed as f64);
                if adj.abs() > 1.0 {
                    let mut bs = self.batch_size.lock();
                    let mut new_bs = *bs as f64 + adj.signum() * (adj.abs().sqrt().max(1.0));
                    new_bs = new_bs.clamp(MIN_BATCH_SIZE as f64, MAX_BATCH_SIZE as f64);
                    *bs = new_bs.round() as usize;
                }
            }
        }
    }

    #[inline]
    fn cur_batch_size(&self) -> usize { *self.batch_size.lock() }

    // Public API: enqueue with priority and profit
    pub fn enqueue(&self, item: VerificationItem) -> Result<(), BatchVerifierError> {
        if self.shutdown.load(Ordering::Acquire) { return Err(BatchVerifierError::VerifierShutdown); }
        if (self.inflight.load(Ordering::Relaxed) as usize) >= MAX_INFLIGHT_ITEMS { return Err(BatchVerifierError::QueueFull); }
        // Speculative rejections
        if self.admit.is_blacklisted(&item.public_key) { return Err(BatchVerifierError::VerificationFailed); }
        // Fast cache path with Bloom
        let cache_key = item.cache_key();
        if self.admit.seen_maybe(cache_key) {
            if let Some(hit) = self.cache.get(&cache_key) {
                self.metrics.cache_hits.inc();
                // Shortcut: immediate accounting, skip scheduling
                self.inflight.fetch_add(1, Ordering::Relaxed);
                self.inflight.fetch_sub(1, Ordering::Relaxed);
                if hit { self.metrics.total_verified.inc(); } else { self.metrics.failed_verifications.inc(); }
                return Ok(());
            }
        } else {
            self.metrics.cache_misses.inc();
        }
        let parsed = Arc::new(ParsedItem::try_from(item)?);
        self.admit.note(parsed.cache_key);
        self.priority_inbox.push(parsed);
        self.inflight.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    pub fn shutdown(&self) { self.shutdown.store(true, Ordering::Release); }

    pub fn export_metrics(&self) -> String {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        let mf = prometheus::gather();
        let _ = encoder.encode(&mf, &mut buffer);
        String::from_utf8_lossy(&buffer).into_owned()
    }
}

#[cfg(feature = "advanced_sched")]
impl Ed25519BatchVerifier {
    #[inline]
    fn verify_batch_dalek(items: &[ShardItem]) -> Vec<Result<bool, BatchVerifierError>> {
        use ed25519_dalek::{Signature, VerifyingKey};
        if items.is_empty() { return vec![]; }
        // prepare slices without clone of bytes; signatures/keys are cloned to own vectors
        let n = items.len();
        let mut msgs: Vec<&[u8]> = Vec::with_capacity(n);
        let mut sigs: Vec<Signature> = Vec::with_capacity(n);
        let mut keys: Vec<VerifyingKey> = Vec::with_capacity(n);
        for it in items.iter() {
            msgs.push(it.msg.as_ref());
            sigs.push(it.sig.clone());
            keys.push(it.vk.clone());
        }
        match ed25519_dalek::verify_batch(&msgs, &sigs, &keys) {
            Ok(_) => vec![Ok(true); n],
            Err(_) => {
                fn isolate(
                    msgs: &[&[u8]],
                    sigs: &[Signature],
                    keys: &[VerifyingKey],
                    out: &mut [Result<bool, BatchVerifierError>],
                ) {
                    if msgs.is_empty() { return; }
                    if msgs.len() == 1 {
                        let ok = keys[0].verify(msgs[0], &sigs[0]).is_ok();
                        out[0] = Ok(ok);
                        return;
                    }
                    let mid = msgs.len() / 2;
                    let (m1, s1, k1, o1) = (&msgs[..mid], &sigs[..mid], &keys[..mid], &mut out[..mid]);
                    let (m2, s2, k2, o2) = (&msgs[mid..], &sigs[mid..], &keys[mid..], &mut out[mid..]);
                    if ed25519_dalek::verify_batch(m1, s1, k1).is_ok() { for r in o1.iter_mut() { *r = Ok(true); } } else { isolate(m1, s1, k1, o1); }
                    if ed25519_dalek::verify_batch(m2, s2, k2).is_ok() { for r in o2.iter_mut() { *r = Ok(true); } } else { isolate(m2, s2, k2, o2); }
                }
                let mut out = vec![Err(BatchVerifierError::VerificationFailed); n];
                isolate(&msgs, &sigs, &keys, &mut out);
                out
            }
        }
    }
}

// Async wrapper around the advanced sync verifier
#[cfg(feature = "advanced_sched")]
pub struct Ed25519BatchVerifierAsync { inner: Arc<Ed25519BatchVerifier> }
#[cfg(feature = "advanced_sched")]
impl Ed25519BatchVerifierAsync {
    pub fn new(inner: Arc<Ed25519BatchVerifier>) -> Self { Self { inner } }
    pub async fn enqueue(&self, item: VerificationItem) -> Result<(), BatchVerifierError> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || inner.enqueue(item)).await.map_err(|_| BatchVerifierError::VerifierShutdown)??;
        Ok(())
    }
    pub fn export_metrics(&self) -> String { self.inner.export_metrics() }
    pub fn shutdown(&self) { self.inner.shutdown() }
}

// Failure stats with blacklisting
#[cfg(feature = "advanced_sched")]
struct FailStats { map: parking_lot::Mutex<AHashMap<[u8; PUBLIC_KEY_LENGTH], u32>> }
#[cfg(feature = "advanced_sched")]
impl FailStats {
    fn new() -> Self { Self { map: parking_lot::Mutex::new(AHashMap::new()) } }
    fn note(&self, pk: [u8; PUBLIC_KEY_LENGTH], ok: bool, admit: &Admittance) {
        if ok { return; }
        let mut m = self.map.lock();
        let c = m.entry(pk).or_insert(0);
        *c += 1;
        const FAIL_BLACKLIST_THRESHOLD: u32 = 32;
        if *c >= FAIL_BLACKLIST_THRESHOLD { admit.blacklist(pk); }
    }
}
    }
}
#[cfg(all(feature = "core_affinity", not(target_os = "linux")))]
fn pin_to_core_and_node(core_id: usize) {
    if let Some(cores) = core_affinity::get_core_ids() {
        if let Some(core) = cores.get(core_id) { let _ = core_affinity::set_for_current(*core); }
    }
}
#[cfg(not(feature = "core_affinity"))]
fn pin_to_core_and_node(_core_id: usize) {}

// Lock-free per-priority shards (advanced scheduler)
#[cfg(feature = "advanced_sched")]
struct PriorityShards {
    shards: Vec<Injector<VerificationItem>>,
}
#[cfg(feature = "advanced_sched")]
impl PriorityShards {
    fn new() -> Self {
        let mut shards = Vec::with_capacity(PRIORITY_LEVELS);
        for _ in 0..PRIORITY_LEVELS { shards.push(Injector::new()); }
        Self { shards }
    }
    #[inline]
    fn push(&self, item: VerificationItem) {
        self.shards[item.priority as usize].push(item);
    }
    // Drain highest priorities first into out up to target
    fn drain_ordered(&self, target: usize, out: &mut Vec<VerificationItem>) {
        out.clear();
        for pr in (0..PRIORITY_LEVELS).rev() {
            while out.len() < target {
                match self.shards[pr].steal() {
                    Steal::Success(it) => out.push(it),
                    Steal::Empty => break,
                    Steal::Retry => continue,
                }
            }
            if out.len() >= target { break; }
        }
    }
}
// Simple per-priority queue with aging info (used in non-advanced scheduler path)
struct PriQueue {
    q: VecDeque<VerificationItem>,
    last_served: Instant,
}

// Admission filter combining a tiny-LFU cache and a Bloom filter to shed obvious spam/dupes
#[cfg(feature = "advanced_sched")]
struct Admittance {
    bloom: Bloom<[u8; 8]>,
    blacklist: RwLock<AHashSet<[u8; PUBLIC_KEY_LENGTH]>>,
}

#[cfg(feature = "advanced_sched")]
impl Admittance {
    fn new() -> Self {
        let bloom = Bloom::new_for_fp_rate(1_000_000, 0.01);
        Self { bloom, blacklist: RwLock::new(AHashSet::new()) }
    }
    #[inline] fn key64(k: u64) -> [u8;8] { k.to_le_bytes() }
    fn seen_maybe(&self, cache_key: u64) -> bool { self.bloom.check(&Self::key64(cache_key)) }
    fn note(&self, cache_key: u64) { self.bloom.set(&Self::key64(cache_key)); }
    fn is_blacklisted(&self, pk: &[u8; PUBLIC_KEY_LENGTH]) -> bool { self.blacklist.read().contains(pk) }
    fn blacklist(&self, pk: [u8; PUBLIC_KEY_LENGTH]) { self.blacklist.write().insert(pk); }
}

// Optional Prometheus metrics bundle (feature-gated)
#[cfg(feature = "advanced_sched")]
struct Metrics {
    total_verified: IntCounter,
    failed_verifications: IntCounter,
    cache_hits: IntCounter,
    cache_misses: IntCounter,
    batches: IntCounter,
    batch_time_us: Histogram,
    verify_time_us: Histogram,
    queue_wait_us: Histogram,
    profit_weighted_latency: Histogram,
}
#[cfg(feature = "advanced_sched")]
impl Metrics {
    fn new() -> Self {
        Self {
            total_verified: register_int_counter!("ed25519_total_verified", "Total successful verifications").unwrap(),
            failed_verifications: register_int_counter!("ed25519_failed", "Failed verifications").unwrap(),
            cache_hits: register_int_counter!("ed25519_cache_hits", "Cache hits").unwrap(),
            cache_misses: register_int_counter!("ed25519_cache_misses", "Cache misses").unwrap(),
            batches: register_int_counter!("ed25519_batches", "Batches processed").unwrap(),
            batch_time_us: register_histogram!("ed25519_batch_time_us", "Batch wall time (us)", vec![50.0, 100.0, 200.0, 400.0, 800.0, 1600.0, 3200.0]).unwrap(),
            verify_time_us: register_histogram!("ed25519_verify_time_us", "Verify time (us)", vec![10.0, 25.0, 50.0, 100.0, 200.0, 400.0]).unwrap(),
            queue_wait_us: register_histogram!("ed25519_queue_wait_us", "Queue wait (us)", vec![10.0, 50.0, 100.0, 200.0, 500.0, 1000.0]).unwrap(),
            profit_weighted_latency: register_histogram!("ed25519_profit_weighted_latency", "Profit-weighted latency (us)", vec![10.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 5000.0]).unwrap(),
        }
    }
}

// PID controller for adaptive batch sizing
struct Pid { kp: f64, ki: f64, kd: f64, integ: f64, prev_err: f64, last: Instant }
impl Pid {
    fn new(kp: f64, ki: f64, kd: f64) -> Self { Self { kp, ki, kd, integ: 0.0, prev_err: 0.0, last: Instant::now() } }
    fn update(&mut self, target_us: f64, actual_us: f64) -> f64 {
        let now = Instant::now();
        let dt = (now - self.last).as_secs_f64().max(1e-6);
        self.last = now;
        let err = target_us - actual_us;
        self.integ = (self.integ + err * dt).clamp(-1e6, 1e6);
        let deriv = (err - self.prev_err) / dt;
        self.prev_err = err;
        self.kp * err + self.ki * self.integ + self.kd * deriv
    }
}
use ed25519_dalek::{
    Signature, Verifier, VerifyingKey, SignatureError,
    SIGNATURE_LENGTH, PUBLIC_KEY_LENGTH
};
use rayon::prelude::*;
use std::sync::{Arc, atomic::{AtomicU64, AtomicBool, Ordering, AtomicI64}};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use thiserror::Error;
use parking_lot::{Mutex, RwLock};
use crossbeam_channel::{bounded, Sender, Receiver};
use crossbeam_queue::ArrayQueue;
use bytes::Bytes;
use blake3::Hasher;
use smallvec::SmallVec;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use metrics::{counter, histogram};
use rand::random;
#[cfg(feature = "advanced_sched")]
use crossbeam_deque::{Injector, Stealer, Worker, Steal};
#[cfg(feature = "advanced_sched")]
use prometheus::{Encoder, IntCounter, Histogram, TextEncoder, register_int_counter, register_histogram};
#[cfg(feature = "advanced_sched")]
use moka::sync::Cache as TinyLfuCache;
#[cfg(feature = "advanced_sched")]
use bloomfilter::Bloom;
#[cfg(feature = "advanced_sched")]
use ahash::{AHashSet, AHashMap};

const MAX_BATCH_SIZE: usize = 128;
const MIN_BATCH_SIZE: usize = 8;
const VERIFICATION_TIMEOUT_MS: u64 = 50;
const CACHE_SIZE: usize = 65536;
const PARALLEL_THRESHOLD: usize = 16;
const MAX_PENDING_BATCHES: usize = 256;
const RETRY_ATTEMPTS: u8 = 3;
const BACKOFF_BASE_MS: u64 = 1;
const AGE_BOOST_MS: u64 = 5; // aging threshold to prevent starvation
const AVG_MSG_LEN_THRESH: usize = 1024; // bytes; scale down batch size when average exceeds this
static PROFIT_THRESHOLD_LAMPORTS: AtomicI64 = AtomicI64::new(0);
const SIMD_CHUNK: usize = 32; // target chunk for SIMD-friendly batch
#[cfg(feature = "gpu_verify")]
const GPU_THRESHOLD: usize = 96; // speculative threshold for GPU path
const PRIORITY_LEVELS: usize = 256;
const MAX_INFLIGHT_ITEMS: usize = 128 * 1024;

// Compose a monotonic priority key from (priority, profit, age, tiebreaker)
#[inline]
fn mk_priority_key(priority: u8, profit: u64, enq_ts: Instant) -> u128 {
    let now = Instant::now();
    let age_us = now.saturating_duration_since(enq_ts).as_micros() as u64;
    let profit_b = (profit.min((1u64 << 56) - 1)) as u128;
    let age_b = (age_us.min((1u64 << 56) - 1)) as u128;
    let pri = priority as u128;
    let tb = ((profit ^ age_us.rotate_left(13)) & 0xff) as u128;
    (pri << 120) | (profit_b << 64) | (age_b << 8) | tb
}

#[derive(Error, Debug, Clone)]
pub enum BatchVerifierError {
    #[error("Invalid signature length: {0}")]
    InvalidSignatureLength(usize),
    
    #[error("Invalid public key length: {0}")]
    InvalidPublicKeyLength(usize),
    
    #[error("Signature verification failed")]
    VerificationFailed,
    
    #[error("Batch timeout exceeded")]
    BatchTimeout,
    
    #[error("Queue full")]
    QueueFull,
    
    #[error("Verifier shutdown")]
    VerifierShutdown,
    
    #[error("Invalid batch size: {0}")]
    InvalidBatchSize(usize),
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
        let mut hasher = Hasher::new();
        hasher.update(&signature);
        hasher.update(&public_key);
        hasher.update(&message);
        let cache_key = u64::from_le_bytes(
            hasher.finalize().as_bytes()[..8].try_into().unwrap_or([0u8; 8])
        );
        Self {
            signature,
            public_key,
            message,
            priority,
            timestamp,
            retry_count,
            estimated_profit_lamports,
            cache_key,
        }
    }
    fn cache_key(&self) -> u64 {
        if self.cache_key != 0 { return self.cache_key; }
        let mut hasher = Hasher::new();
        hasher.update(&self.signature);
        hasher.update(&self.public_key);
        hasher.update(&self.message);
        u64::from_le_bytes(hasher.finalize().as_bytes()[..8].try_into().unwrap_or([0u8;8]))
    }
}

// Parsed item to avoid repeated decoding and cloning
pub struct ParsedItem {
    pub sig: Signature,
    pub vk: VerifyingKey,
    pub msg: Bytes,
    pub cache_key: u64,
}

impl TryFrom<VerificationItem> for ParsedItem {
    type Error = BatchVerifierError;
    fn try_from(v: VerificationItem) -> Result<Self, Self::Error> {
        let sig = Signature::from_bytes(&v.signature)
            .map_err(|_| BatchVerifierError::InvalidSignatureLength(v.signature.len()))?;
        let vk = VerifyingKey::from_bytes(&v.public_key)
            .map_err(|_| BatchVerifierError::InvalidPublicKeyLength(v.public_key.len()))?;
        let mut hasher = blake3::Hasher::new();
        hasher.update(&v.signature);
        hasher.update(&v.public_key);
        hasher.update(&v.message);
        let cache_key = u64::from_le_bytes(
            hasher.finalize().as_bytes()[..8].try_into().unwrap_or([0u8;8])
        );
        Ok(Self { sig, vk, msg: v.message, cache_key })
    }
}

impl BatchVerifier {
    fn verify_uncached_batch_dalek(items: &[VerificationItem]) -> Vec<Result<bool, BatchVerifierError>> {
        // Pre-parse to avoid repeated decoding and heap churn
        let mut parsed: Vec<ParsedItem> = Vec::with_capacity(items.len());
        for it in items.iter().cloned() {
            match ParsedItem::try_from(it) {
                Ok(p) => parsed.push(p),
                Err(_) => return items.iter().map(|x| Self::verify_item(x)).collect(),
            }
        }
        Self::verify_batch_from_parsed(&parsed)
    }

    fn verify_batch_from_parsed(parsed: &[ParsedItem]) -> Vec<Result<bool, BatchVerifierError>> {
        use ed25519_dalek::{Signature, VerifyingKey};
        let n = parsed.len();
        if n == 0 { return vec![]; }
        // Build aligned vectors
        let mut msgs: Vec<&[u8]> = parsed.iter().map(|p| p.msg.as_ref()).collect();
        let mut sigs: Vec<Signature> = parsed.iter().map(|p| p.sig.clone()).collect();
        let mut keys: Vec<VerifyingKey> = parsed.iter().map(|p| p.vk.clone()).collect();

        // Batch fusion: sort by verifying key bytes to improve locality and reduce branch mispredicts
        let mut idx: Vec<usize> = (0..n).collect();
        idx.sort_unstable_by_key(|&i| keys[i].to_bytes());

        let mut msgs_sorted: Vec<&[u8]> = Vec::with_capacity(n);
        let mut sigs_sorted: Vec<Signature> = Vec::with_capacity(n);
        let mut keys_sorted: Vec<VerifyingKey> = Vec::with_capacity(n);
        for i in &idx { msgs_sorted.push(msgs[*i]); sigs_sorted.push(sigs[*i]); keys_sorted.push(keys[*i]); }

        // Process in SIMD-friendly chunks in parallel; leftover fallback to singles
        let mut out = vec![Err(BatchVerifierError::VerificationFailed); n];
        // closure to process a chunk [start, end)
        let process_chunk = |start: usize, end: usize, out: &mut [Result<bool, BatchVerifierError>]| {
            let len = end - start;
            if len == 0 { return; }
            let m = &msgs_sorted[start..end];
            let s = &sigs_sorted[start..end];
            let k = &keys_sorted[start..end];
            if VerifyingKey::verify_batch(m, s, k).is_ok() {
                for j in start..end { out[idx[j]] = Ok(true); }
            } else {
                // Fallback: single verifies within the chunk
                for j in start..end {
                    let ok = keys_sorted[j].verify(msgs_sorted[j], &sigs_sorted[j]).is_ok();
                    out[idx[j]] = Ok(ok);
                }
            }
        };

        // Parallelize over chunks of SIMD_CHUNK
        let mut pos = 0;
        while pos + SIMD_CHUNK <= n {
            let end = pos + SIMD_CHUNK;
            process_chunk(pos, end, &mut out);
            pos = end;
        }
        // Leftover
        if pos < n { process_chunk(pos, n, &mut out); }
        // Optional GPU offload first
        #[cfg(feature = "gpu_verify")]
        {
            if Self::verify_batch_gpu(&msgs, &sigs, &keys) {
                return vec![Ok(true); n];
            }
        }
        out
    }
}

pub struct BatchVerifier {
    verification_cache: Arc<VerifCache>,
    pending_queue: Arc<ArrayQueue<VerificationItem>>,
    // carry original indices along with items
    batch_sender: Sender<Vec<(usize, VerificationItem)>>,
    result_receiver: Receiver<Vec<(usize, Result<bool, BatchVerifierError>)>>,
    stats: Arc<VerifierStats>,
    shutdown: Arc<AtomicBool>,
    worker_handles: Vec<std::thread::JoinHandle<()>>,
}

struct VerifierStats {
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

impl BatchVerifier {
    #[inline]
    fn recv_with_deadline(
        rx: &Receiver<Vec<(usize, Result<bool, BatchVerifierError>)>>,
        expected: usize,
        timeout: Duration,
        out: &mut [Result<bool, BatchVerifierError>],
    ) {
        let deadline = Instant::now() + timeout;
        let mut received = 0usize;
        while received < expected {
            let now = Instant::now();
            if now >= deadline { break; }
            let remaining = deadline - now;
            match rx.recv_timeout(remaining) {
                Ok(batch) => {
                    for (idx, res) in batch {
                        if let Err(BatchVerifierError::VerificationFailed) = out[idx] {
                            out[idx] = res;
                            received += 1;
                        }
                    }
                }
                Err(_) => break,
            }
        }
        // Mark unresolved as timeout
        for r in out.iter_mut() {
            if matches!(r, Err(BatchVerifierError::VerificationFailed)) {
                *r = Err(BatchVerifierError::BatchTimeout);
            }
        }
    }
    pub fn new(worker_threads: usize) -> Self {
        let (batch_sender, batch_receiver) = bounded::<Vec<(usize, VerificationItem)>>(MAX_PENDING_BATCHES);
        let (result_sender, result_receiver) = bounded::<Vec<(usize, Result<bool, BatchVerifierError>)>>(MAX_PENDING_BATCHES * MAX_BATCH_SIZE);
        
        let verification_cache = Arc::new(VerifCache::new(CACHE_SIZE));
        let pending_queue = Arc::new(ArrayQueue::new(MAX_BATCH_SIZE * 4));
        let stats = Arc::new(VerifierStats::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        #[cfg(feature = "advanced_sched")]
        let inbox_per_priority = Arc::new(PriorityShards::new());
        #[cfg(feature = "advanced_sched")]
        let admission = Admittance::new();
        
        let mut worker_handles = Vec::with_capacity(worker_threads);
        
        for i in 0..worker_threads {
            let batch_receiver = batch_receiver.clone();
            let result_sender = result_sender.clone();
            let stats = stats.clone();
            let shutdown = shutdown.clone();
            let worker_idx = i;
            
            let handle = std::thread::spawn(move || {
                // Optional NUMA/core pinning if feature enabled
                #[cfg(feature = "core_affinity")]
                if let Some(cores) = core_affinity::get_core_ids() {
                    let core = cores[worker_idx % cores.len()];
                    let _ = core_affinity::set_for_current(core);
                }
                while !shutdown.load(Ordering::Acquire) {
                    match batch_receiver.recv_timeout(Duration::from_millis(10)) {
                        Ok(batch) => {
                            let start = Instant::now();
                            let items: Vec<VerificationItem> = batch.iter().map(|(_, it)| it.clone()).collect();
                            let results = Self::verify_batch_internal(&items);
                            
                            let batch_time = start.elapsed().as_micros() as u64;
                            stats.batch_count.fetch_add(1, Ordering::Relaxed);
                            
                            let current_avg = stats.avg_batch_time_us.load(Ordering::Relaxed);
                            let new_avg = (current_avg * 7 + batch_time) / 8;
                            stats.avg_batch_time_us.store(new_avg, Ordering::Relaxed);
                            histogram!("verifier.batch_time_us", batch_time as f64);
                            histogram!("verifier.batch_size", items.len() as f64);
                            
                            // echo original indices back
                            let indexed_results: Vec<_> = batch
                                .iter()
                                .map(|(orig_idx, _)| *orig_idx)
                                .zip(results.into_iter())
                                .collect();
                            
                            let _ = result_sender.send(indexed_results);
                        }
                        Err(_) => {
                            if shutdown.load(Ordering::Acquire) {
                                break;
                            }
                        }
                    }
                }
            });
            
            worker_handles.push(handle);
        }
        
        Self {
            verification_cache,
            pending_queue,
            batch_sender,
            result_receiver,
            stats,
            shutdown,
            worker_handles,
        }
    }
    
    pub fn verify_single(&self, item: VerificationItem) -> Result<bool, BatchVerifierError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(BatchVerifierError::VerifierShutdown);
        }
        
        let cache_key = item.cache_key();
        
        if let Some(hit) = self.verification_cache.get(cache_key, Duration::from_secs(60)) {
            self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(hit);
        }
        
        self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
        
        let result = self.verify_single_internal(&item);
        
        self.verification_cache.insert(cache_key, result.is_ok());
        
        result
    }
    
    pub async fn verify_batch(&self, items: Vec<VerificationItem>) -> Vec<Result<bool, BatchVerifierError>> {
        if items.is_empty() {
            return vec![];
        }
        
        if items.len() == 1 {
            return vec![self.verify_single(items.into_iter().next().unwrap())];
        }
        
        let mut results = vec![Err(BatchVerifierError::VerificationFailed); items.len()];
        let mut cached_indices = SmallVec::<[(usize, bool); 32]>::new();
        let mut uncached_items = Vec::with_capacity(items.len());
        let mut uncached_indices = Vec::with_capacity(items.len());
        
        for (idx, item) in items.iter().enumerate() {
            let cache_key = item.cache_key();
            
            if let Some(hit) = self.verification_cache.get(cache_key, Duration::from_secs(60)) {
                cached_indices.push((idx, hit));
                self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            
            self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
            uncached_items.push(item.clone());
            uncached_indices.push(idx);
        }
        
        for (idx, result) in cached_indices {
            results[idx] = Ok(result);
        }
        
        if uncached_items.is_empty() {
            return results;
        }
        
        // enqueue chunks with original indices carried along
        let total = uncached_items.len();
        let mut offset = 0;
        while offset < total {
            let end = (offset + MAX_BATCH_SIZE).min(total);
            let work_chunk: Vec<(usize, VerificationItem)> = uncached_indices[offset..end]
                .iter()
                .cloned()
                .zip(uncached_items[offset..end].iter().cloned())
                .collect();
            if self.batch_sender.send(work_chunk).is_err() {
                for idx in &uncached_indices[offset..end] {
                    results[*idx] = Err(BatchVerifierError::QueueFull);
                }
                offset = end;
                continue;
            }
            // Await results up to deadline, accumulating partials
            Self::recv_with_deadline(
                &self.result_receiver,
                end - offset,
                Duration::from_millis(VERIFICATION_TIMEOUT_MS),
                &mut results,
            );
            // Cache only resolved successes
            for gi in &uncached_indices[offset..end] {
                if let Ok(true) = results[*gi] {
                    let item = &items[*gi];
                    self.verification_cache.insert(item.cache_key(), true);
                }
            }
            offset = end;
        }
        
        results
    }

    // Synchronous core alias for clarity at call sites
    #[inline]
    pub fn verify_batch_sync(&self, items: Vec<VerificationItem>) -> Vec<Result<bool, BatchVerifierError>> {
        self.verify_batch_blocking(items)
    }

    // Blocking variant for non-async contexts
    pub fn verify_batch_blocking(&self, items: Vec<VerificationItem>) -> Vec<Result<bool, BatchVerifierError>> {
        // Reuse the async path logic without requiring a runtime by inlining the core
        // Note: This duplicates the structure above but without async signature
        if items.is_empty() { return vec![]; }
        if items.len() == 1 { return vec![self.verify_single(items.into_iter().next().unwrap())]; }

        let mut results = vec![Err(BatchVerifierError::VerificationFailed); items.len()];
        let mut cached_indices = SmallVec::<[(usize, bool); 32]>::new();
        let mut uncached_items = Vec::with_capacity(items.len());
        let mut uncached_indices = Vec::with_capacity(items.len());

        for (idx, item) in items.iter().enumerate() {
            let cache_key = item.cache_key();
            if let Some(hit) = self.verification_cache.get(cache_key, Duration::from_secs(60)) {
                cached_indices.push((idx, hit));
                self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
            uncached_items.push(item.clone());
            uncached_indices.push(idx);
        }
        for (idx, result) in cached_indices { results[idx] = Ok(result); }
        if uncached_items.is_empty() { return results; }

        let total = uncached_items.len();
        let mut offset = 0;
        while offset < total {
            let end = (offset + MAX_BATCH_SIZE).min(total);
            let work_chunk: Vec<(usize, VerificationItem)> = uncached_indices[offset..end]
                .iter()
                .cloned()
                .zip(uncached_items[offset..end].iter().cloned())
                .collect();
            if self.batch_sender.send(work_chunk).is_err() {
                for idx in &uncached_indices[offset..end] { results[*idx] = Err(BatchVerifierError::QueueFull); }
                offset = end; continue;
            }
            Self::recv_with_deadline(
                &self.result_receiver,
                end - offset,
                Duration::from_millis(VERIFICATION_TIMEOUT_MS),
                &mut results,
            );
            for gi in &uncached_indices[offset..end] {
                if let Ok(true) = results[*gi] {
                    let item = &items[*gi];
                    self.verification_cache.insert(item.cache_key(), true);
                }
            }
            offset = end;
        }
        results
    }
    
    fn verify_single_internal(&self, item: &VerificationItem) -> Result<bool, BatchVerifierError> {
        let signature = Signature::from_bytes(&item.signature)
            .map_err(|_| BatchVerifierError::InvalidSignatureLength(item.signature.len()))?;
        
        let public_key = VerifyingKey::from_bytes(&item.public_key)
            .map_err(|_| BatchVerifierError::InvalidPublicKeyLength(item.public_key.len()))?;
        
        match public_key.verify(&item.message, &signature) {
            Ok(_) => {
                self.stats.total_verified.fetch_add(1, Ordering::Relaxed);
                Ok(true)
            }
            Err(_) => {
                self.stats.failed_verifications.fetch_add(1, Ordering::Relaxed);
                Ok(false)
            }
        }
    }
    
    fn verify_batch_internal(items: &[VerificationItem]) -> Vec<Result<bool, BatchVerifierError>> {
        // For very small batches, the overhead of batch MSM can dominate; keep simple path
        if items.len() < PARALLEL_THRESHOLD {
            return items.iter().map(|item| Self::verify_item(item)).collect();
        }

        // True batch verification using dalek's multi-scalar algorithm
        Self::verify_uncached_batch_dalek(items)
    }
    
    fn verify_item(item: &VerificationItem) -> Result<bool, BatchVerifierError> {
        let signature = match Signature::from_bytes(&item.signature) {
            Ok(sig) => sig,
            Err(_) => return Err(BatchVerifierError::InvalidSignatureLength(item.signature.len())),
        };
        
        let public_key = match VerifyingKey::from_bytes(&item.public_key) {
            Ok(pk) => pk,
            Err(_) => return Err(BatchVerifierError::InvalidPublicKeyLength(item.public_key.len())),
        };
        
        match public_key.verify(&item.message, &signature) {
            Ok(_) => Ok(true),
            Err(_) => Ok(false),
        }
    }
    pub fn queue_verification(&self, item: VerificationItem) -> Result<(), BatchVerifierError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(BatchVerifierError::VerifierShutdown);
        }
        if self.pending_queue.is_full() {
            return Err(BatchVerifierError::QueueFull);
        }
        self.pending_queue.push(item).map_err(|_| BatchVerifierError::QueueFull)?;
        if self.pending_queue.len() >= MIN_BATCH_SIZE {
            self.process_pending_queue_pop()?;
        }
        Ok(())
    }

    fn process_pending_queue_pop(&self) -> Result<(), BatchVerifierError> {
        let mut drained: Vec<VerificationItem> = Vec::with_capacity(MAX_BATCH_SIZE);
        while drained.len() < MAX_BATCH_SIZE {
            if let Some(it) = self.pending_queue.pop() {
                drained.push(it);
            } else { break; }
        }
        if drained.is_empty() { return Ok(()); }

        // Adaptive batch sizing: cap by average message length and add small jitter
        let avg_len = drained.iter().map(|i| i.message.len()).sum::<usize>() / drained.len().max(1);
        let batch_cap = if avg_len > AVG_MSG_LEN_THRESH { 32 } else { MAX_BATCH_SIZE };
        let jitter = 0.8 + 0.2 * (random::<f64>());
        let dynamic_batch = ((batch_cap as f64) * jitter).round() as usize;
        let dynamic_batch = dynamic_batch.max(MIN_BATCH_SIZE).min(drained.len());

        // Truncate to dynamic size; attempt to requeue remainder if any
        let remainder = if drained.len() > dynamic_batch { Some(drained.split_off(dynamic_batch)) } else { None };
        if let Some(rest) = remainder {
            for it in rest {
                let _ = self.pending_queue.push(it);
            }
        }

        let batch: Vec<(usize, VerificationItem)> = drained.into_iter().enumerate().collect();

        if batch.is_empty() {
            return Ok(());
        }

        self.batch_sender
            .send(batch)
            .map_err(|_| BatchVerifierError::QueueFull)
    }

    pub fn flush_pending(&self) -> Result<Vec<Result<bool, BatchVerifierError>>, BatchVerifierError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(BatchVerifierError::VerifierShutdown);
        }
        let mut all_results = Vec::new();
        loop {
            let mut drained: Vec<VerificationItem> = Vec::with_capacity(MAX_BATCH_SIZE);
            while drained.len() < MAX_BATCH_SIZE {
                if let Some(it) = self.pending_queue.pop() {
                    drained.push(it);
                } else { break; }
            }
            // Locally index items 0..n for this batch; receiver maps back using provided indices
            let batch: Vec<(usize, VerificationItem)> = drained.into_iter().enumerate().collect();
            let batch_len = batch.len();

            if self.batch_sender.send(batch).is_err() {
                return Err(BatchVerifierError::QueueFull);
            }
            match self.result_receiver.recv_timeout(Duration::from_millis(VERIFICATION_TIMEOUT_MS * 2)) {
                Ok(results_vec) => {
                    all_results.extend(results_vec.into_iter().map(|(_, r)| r));
                }
                Err(_) => {
                    all_results.extend(vec![Err(BatchVerifierError::BatchTimeout); batch_len]);
                }
            }
            if self.pending_queue.is_empty() { break; }
        }

        Ok(all_results)
    }

    // ... (rest of the methods remain the same)
}

impl Drop for BatchVerifier {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        
        for handle in self.worker_handles.drain(..) {
            let _ = handle.join();
        }
    }
}

#[derive(Debug, Clone)]
pub struct VerifierStatsSnapshot {
    pub total_verified: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub failed_verifications: u64,
    pub batch_count: u64,
    pub avg_batch_time_us: u64,
    pub cache_size: usize,
}

pub struct PriorityBatchVerifier {
    verifier: Arc<BatchVerifier>,
    priority_queue: Arc<Mutex<Vec<PriQueue>>>,
    // Adaptive slot scheduler: nearest-deadline-first
    sched_pq: Arc<Mutex<BinaryHeap<(Reverse<u128>, VerificationItem)>>>,
    #[cfg(feature = "advanced_sched")]
    inbox_per_priority: Arc<PriorityShards>,
    #[cfg(feature = "advanced_sched")]
    admission: Admittance,
    processing_thread: Option<std::thread::JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

impl PriorityBatchVerifier {
    pub fn new(worker_threads: usize) -> Self {
        let verifier = Arc::new(BatchVerifier::new(worker_threads));
        let priority_queue = Arc::new(Mutex::new({
            let mut v = Vec::with_capacity(256);
            for _ in 0..256 {
                v.push(PriQueue { q: VecDeque::new(), last_served: Instant::now() });
            }
            v
        }));
        let sched_pq = Arc::new(Mutex::new(BinaryHeap::new()));
        let shutdown = Arc::new(AtomicBool::new(false));
        
        let verifier_clone = verifier.clone();
        let queue_clone = priority_queue.clone();
        let sched_clone = sched_pq.clone();
        let shutdown_clone = shutdown.clone();
        #[cfg(feature = "advanced_sched")]
        let inbox_clone = inbox_per_priority.clone();
        
        let processing_thread = std::thread::spawn(move || {
            let mut last_flush = Instant::now();
            let mut consecutive_empty = 0;
            
            while !shutdown_clone.load(Ordering::Acquire) {
                let mut found_items = false;
                let mut batch: Vec<VerificationItem> = Vec::with_capacity(MAX_BATCH_SIZE);

                // 1) Prefer scheduled items with nearest deadline first
                {
                    let mut pq = sched_clone.lock();
                    while batch.len() < MAX_BATCH_SIZE {
                        if let Some((_prio, it)) = pq.pop() {
                            batch.push(it);
                            found_items = true;
                        } else {
                            break;
                        }
                    }
                }

                // 1b) Drain lock-free inboxes staged by priority (if enabled)
                #[cfg(feature = "advanced_sched")]
                {
                    let mut buf = Vec::with_capacity(MAX_BATCH_SIZE);
                    inbox_clone.drain_ordered(MAX_BATCH_SIZE - batch.len(), &mut buf);
                    if !buf.is_empty() { found_items = true; batch.extend(buf.into_iter()); }
                }

                // 2) If still room, build batch with aging-aware priority without holding lock too long
                if batch.len() < MAX_BATCH_SIZE {
                    let mut queues = queue_clone.lock();
                    let now = Instant::now();
                    let mut picked = batch.len();
                    for pr in (0u16..=255u16).rev() {
                        if picked >= MAX_BATCH_SIZE { break; }
                        for idx in (0..=255usize).rev() {
                            if picked >= MAX_BATCH_SIZE { break; }
                            let pq = &mut queues[idx];
                            if pq.q.is_empty() { continue; }
                            let aged = now.duration_since(pq.last_served).as_millis() as u64 >= AGE_BOOST_MS;
                            if aged || idx as u16 == pr {
                                while picked < MAX_BATCH_SIZE {
                                    if let Some(it) = pq.q.pop_front() {
                                        batch.push(it);
                                        picked += 1;
                                        found_items = true;
                                    } else { break; }
                                }
                                pq.last_served = now;
                                if picked >= MAX_BATCH_SIZE { break; }
                            }
                        }
                    }
                }

                if !batch.is_empty() {
                    // Use blocking variant to avoid any runtime coupling
                    let _ = verifier_clone.verify_batch_blocking(batch);
                    consecutive_empty = 0;
                } else {
                    consecutive_empty += 1;
                }
                
                if last_flush.elapsed() >= Duration::from_millis(10) || 
                   (found_items) {
                    let _ = verifier_clone.flush_pending();
                    last_flush = Instant::now();
                }
                
                if consecutive_empty > 10 {
                    std::thread::sleep(Duration::from_micros(100));
                }
            }
        });
        
        Self {
            verifier,
            priority_queue,
            sched_pq,
            #[cfg(feature = "advanced_sched")]
            inbox_per_priority,
            #[cfg(feature = "advanced_sched")]
            admission,
            processing_thread: Some(processing_thread),
            shutdown,
        }
    }
    
    pub fn queue_with_priority(&self, item: VerificationItem) -> Result<(), BatchVerifierError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(BatchVerifierError::VerifierShutdown);
        }
        
        let priority = item.priority as usize;
        #[cfg(feature = "advanced_sched")]
        {
            if self.admission.is_blacklisted(&item.public_key) {
                counter!("verifier.tx_admission_reject", 1);
                return Ok(());
            }
            if !self.admission.seen_maybe(item.cache_key()) { self.admission.note(item.cache_key()); }
            self.inbox_per_priority.push(item);
            histogram!("verifier.queue_len", 0.0);
            return Ok(());
        }
        let mut queues = self.priority_queue.lock();
        // Adaptive backpressure: drop low-profit when queue is congested
        let congested = queues[priority].q.len() >= MAX_BATCH_SIZE * 2;
        if congested && item.estimated_profit_lamports < PROFIT_THRESHOLD_LAMPORTS.load(Ordering::Relaxed) {
            counter!("verifier.tx_dropped_low_profit", 1);
            return Ok(()); // drop early
        }
        if queues[priority].q.len() >= MAX_BATCH_SIZE * 2 {
            return Err(BatchVerifierError::QueueFull);
        }
        queues[priority].q.push_back(item);
        histogram!("verifier.queue_len", queues[priority].q.len() as f64);
        Ok(())
    }

    /// Enqueue with explicit deadline for adaptive slot scheduling
    pub fn queue_with_deadline(&self, item: VerificationItem, deadline: Instant) -> Result<(), BatchVerifierError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(BatchVerifierError::VerifierShutdown);
        }
        // Adaptive backpressure for scheduled path
        if item.estimated_profit_lamports < PROFIT_THRESHOLD_LAMPORTS.load(Ordering::Relaxed) {
            // Only drop if many scheduled are pending
            let pq_len = { self.sched_pq.lock().len() };
            if pq_len >= MAX_PENDING_BATCHES {
                counter!("verifier.tx_dropped_low_profit", 1);
                return Ok(());
            }
        }
        let pq_len_after = {
            #[cfg(feature = "advanced_sched")]
            {
                if self.admission.is_blacklisted(&item.public_key) {
                    counter!("verifier.tx_admission_reject", 1);
                    0usize
                } else {
                    if !self.admission.seen_maybe(item.cache_key()) { self.admission.note(item.cache_key()); }
                    let k = mk_priority_key(item.priority, item.estimated_profit_lamports as u64, item.timestamp);
                    let mut pq = self.sched_pq.lock();
                    pq.push((Reverse(k), item));
                    pq.len()
                }
            }
            #[cfg(not(feature = "advanced_sched"))]
            {
                let ts = deadline.duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos() as u128;
                let mut pq = self.sched_pq.lock();
                pq.push((Reverse(ts), item));
                pq.len()
            }
        };
        if pq_len_after > 0 { histogram!("verifier.sched_pq_len", pq_len_after as f64); }
        Ok(())
    }
    
    pub async fn verify_critical(&self, item: VerificationItem) -> Result<bool, BatchVerifierError> {
        let mut item = item;
        item.priority = 255;
        
        for attempt in 0..RETRY_ATTEMPTS {
            match self.verifier.verify_single(item.clone()) {
                Ok(result) => return Ok(result),
                Err(e) => {
                    if attempt < RETRY_ATTEMPTS - 1 {
                        // Exponential backoff with jitter to avoid retry stampedes
                        let base = BACKOFF_BASE_MS * (1u64 << attempt);
                        let jitter = (random::<u64>() % BACKOFF_BASE_MS);
                        tokio::time::sleep(Duration::from_millis(base + jitter)).await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        
        Err(BatchVerifierError::VerificationFailed)
    }

    /// Profit^2-weighted scheduling enqueue (P^2 fairness)
    pub fn queue_profit_weighted(&self, item: VerificationItem) -> Result<(), BatchVerifierError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(BatchVerifierError::VerifierShutdown);
        }
        // Profit^2, penalize retries, favor freshness
        let profit = (item.estimated_profit_lamports.max(1) as u128);
        let mut score = profit.saturating_mul(profit) / (1 + item.retry_count as u128);
        let age_ms = item.timestamp.elapsed().as_millis() as u128;
        score = score.saturating_sub(age_ms);
        let mut pq = self.sched_pq.lock();
        pq.push((Reverse(score), item));
        histogram!("verifier.sched_pq_len", pq.len() as f64);
        Ok(())
    }
    
    pub fn get_verifier(&self) -> Arc<BatchVerifier> {
        self.verifier.clone()
    }
    
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.verifier.shutdown();
    }
}

impl Drop for PriorityBatchVerifier {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        
        if let Some(thread) = self.processing_thread.take() {
            let _ = thread.join();
        }
    }
}

pub struct AdaptiveBatchVerifier {
    verifier: Arc<PriorityBatchVerifier>,
    batch_size: Arc<AtomicU64>,
    success_rate: Arc<AtomicU64>,
    latency_target_us: u64,
    min_success_rate: f64,
    latency_ewma: Ewma,
}

impl AdaptiveBatchVerifier {
    pub fn new(worker_threads: usize, latency_target_us: u64, min_success_rate: f64) -> Self {
        Self {
            verifier: Arc::new(PriorityBatchVerifier::new(worker_threads)),
            batch_size: Arc::new(AtomicU64::new(MIN_BATCH_SIZE as u64)),
            success_rate: Arc::new(AtomicU64::new(f64::to_bits(1.0))),
            latency_target_us,
            min_success_rate,
            latency_ewma: Ewma::new(latency_target_us as f64),
        }
    }
    
    /// Verifies a batch of verification items adaptively, adjusting the batch size based on latency and success rate.
    pub async fn verify_adaptive(&self, items: Vec<VerificationItem>) -> Vec<Result<bool, BatchVerifierError>> {
        let start = Instant::now();
        let current_batch_size = self.batch_size.load(Ordering::Relaxed) as usize;
        
        let mut results = Vec::with_capacity(items.len());
        let chunks: Vec<_> = items.chunks(current_batch_size).collect();
        
        let mut successful = 0u64;
        let mut total = 0u64;
        let mut total_msg_len: usize = 0;
        let mut total_msgs: usize = 0;
        
        for chunk in chunks {
            let chunk_results = self.verifier.get_verifier().verify_batch(chunk.to_vec()).await;
            
            for result in &chunk_results {
                total += 1;
                if result.is_ok() {
                    successful += 1;
                }
            }
            for it in chunk.iter() {
                total_msg_len = total_msg_len.saturating_add(it.message.len());
                total_msgs = total_msgs.saturating_add(1);
            }
            
            results.extend(chunk_results);
        }
        
        let elapsed_us = start.elapsed().as_micros() as u64;
        let success_rate = if total > 0 { successful as f64 / total as f64 } else { 1.0 };
        
        self.success_rate.store(f64::to_bits(success_rate), Ordering::Relaxed);
        
        let avg_len = if total_msgs > 0 { (total_msg_len / total_msgs) as u64 } else { 0 };
        self.adjust_batch_size(elapsed_us, success_rate, avg_len);
        
        results
    }

    /// Adjusts the batch size based on latency, success rate, and average message length.
    fn adjust_batch_size(&self, latency_us: u64, success: f64, avg_msg_len: u64) {
        // EWMA with clipping to reduce noise sensitivity
        let alpha = 0.2;
        let clip = (latency_us.min(self.latency_target_us.saturating_mul(4))) as f64;
        self.latency_ewma.update(clip, alpha);

        let l = f64::from_bits(self.latency_ewma.val.load(Ordering::Relaxed));
        let target = self.latency_target_us as f64;
        let pressure = (l / target).powf(3.0); // cubic response for faster convergence

        let mut size = self.batch_size.load(Ordering::Relaxed) as f64;
        if pressure > 1.0 {
            // Under pressure: contract smoothly; denominator grows with pressure
            size = (size / (1.0 + 0.5 * (pressure - 1.0))).max(MIN_BATCH_SIZE as f64);
        } else if success >= self.min_success_rate && pressure < 0.5 {
            // Plenty of headroom and high success: expand more aggressively
            size = (size * 1.25).min(MAX_BATCH_SIZE as f64);
        }

        // Admission control by message size: reduce size if payloads are large
        if (avg_msg_len as usize) > AVG_MSG_LEN_THRESH {
            let scale = (AVG_MSG_LEN_THRESH as f64) / (avg_msg_len as f64);
            // Clip scale to [0.5, 1.0]
            let scale = scale.clamp(0.5, 1.0);
            size = (size * scale).max(MIN_BATCH_SIZE as f64);
        }

        self.batch_size.store(size.round() as u64, Ordering::Relaxed);
    }
}

// EWMA helper for latency smoothing
struct Ewma { val: AtomicU64 }
impl Ewma {
    fn new(init: f64) -> Self { Self { val: AtomicU64::new(init.to_bits()) } }
    fn update(&self, sample: f64, alpha: f64) {
        let cur = f64::from_bits(self.val.load(Ordering::Relaxed));
        let new = cur + alpha * (sample - cur);
        self.val.store(new.to_bits(), Ordering::Relaxed);
    }
}
    
    pub fn get_current_batch_size(&self) -> usize {
        self.batch_size.load(Ordering::Relaxed) as usize
    }
    
{{ ... }}
        f64::from_bits(self.success_rate.load(Ordering::Relaxed))
    }
}

#[inline(always)]
pub fn quick_verify(signature: &[u8; 64], public_key: &[u8; 32], message: &[u8]) -> bool {
    if let Ok(sig) = Signature::from_bytes(signature) {
        if let Ok(pk) = VerifyingKey::from_bytes(public_key) {
            return pk.verify(message, &sig).is_ok();
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_batch_verifier_creation() {
        let verifier = BatchVerifier::new(4);
        assert!(!verifier.is_shutdown());
    }
}

