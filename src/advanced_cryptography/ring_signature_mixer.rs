use solana_sdk::{
    signature::{Keypair, Signature, Signer},
    pubkey::Pubkey,
    transaction::Transaction,
    instruction::Instruction,
    hash::Hash,
    commitment_config::CommitmentConfig,
};
use dashmap::{DashMap, DashSet};
use futures::{stream, StreamExt};
use std::time::{SystemTime, UNIX_EPOCH};
use prometheus::{IntCounterVec, HistogramVec, register_int_counter_vec, register_histogram_vec};
use serde::Serialize;
use solana_client::rpc_client::RpcClient;
use borsh::{BorshDeserialize, BorshSerialize};
use curve25519_dalek::{
    edwards::{CompressedEdwardsY, EdwardsPoint},
    scalar::Scalar,
};
use curve25519_dalek::constants::ED25519_BASEPOINT_POINT as G;
use sha3::{Sha3_512, Digest};
use sha2::Sha512;
use ed25519_dalek::{SigningKey, VerifyingKey, SECRET_KEY_LENGTH};
use zeroize::Zeroize;
use merlin::Transcript;
use rand_chacha::ChaCha20Rng;
use getrandom::getrandom;
use rand::{rngs::StdRng, SeedableRng, RngCore, CryptoRng, Rng};
use std::{
    collections::{HashMap, VecDeque, HashSet},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
    error::Error,
};
use tokio::sync::{mpsc, Semaphore};
use parking_lot::RwLock as PRwLock;
use rayon::prelude::*;
use solana_sdk::{
    pubkey,
    message::v0::Message as MessageV0,
    message::VersionedMessage,
    address_lookup_table::AddressLookupTableAccount,
    compute_budget::ComputeBudgetInstruction,
    transaction::VersionedTransaction,
};

// Mixer program ID (replace with your deployed program ID)
const MIXER_PROGRAM_ID: Pubkey = pubkey!("YourMixerProgram111111111111111111111111111");

const RING_SIZE: usize = 16;

const MAX_DECOY_CACHE: usize = 256;
const SIGNATURE_TIMEOUT_MS: u64 = 150;
const MAX_RETRY_ATTEMPTS: u8 = 3;
const DECOY_REFRESH_INTERVAL_MS: u64 = 500;

#[derive(Debug, Clone)]
pub struct RingSignatureMixer {
    rpc_client: Arc<RpcClient>,
    decoy_cache: Arc<DecoyCache>,
    active_rings: Arc<RwLock<HashMap<[u8; 32], RingContext>>>,
    // Reuse protections
    recent_key_images: Arc<DashMap<[u8; 32], ()>>, // prevents key image reuse
    recent_ring_hashes: Arc<DashMap<[u8; 32], ()>>, // prevents same ring composition reuse
    mixer_keypair: Arc<Keypair>,
    // Metrics
    m_sim_ok: IntCounterVec,
    m_ring_build_lat_us: HistogramVec,
    m_cu_used: HistogramVec,
    m_tip_paid: HistogramVec,
    m_decoy_cache_churn: IntCounterVec,
}

// =============================
// Ed25519 scalar+point extractor
// =============================

pub fn ed25519_scalar_and_point_from_keypair(kp: &Keypair) -> (Scalar, EdwardsPoint) {
    // Solana Keypair: [secret(32) || public(32)]
    let bytes = kp.to_bytes();
    let mut seed = [0u8; SECRET_KEY_LENGTH];
    seed.copy_from_slice(&bytes[..SECRET_KEY_LENGTH]);

    // RFC8032 clamping of SHA-512(seed)[..32]
    let h = Sha512::digest(&seed);
    let mut a_bytes = [0u8; 32];
    a_bytes.copy_from_slice(&h[..32]);
    a_bytes[0]  &= 248;
    a_bytes[31] &= 63;
    a_bytes[31] |= 64;
    let a = Scalar::from_bits(a_bytes);

    // Decompress public key to EdwardsPoint
    let mut pk = [0u8; 32];
    pk.copy_from_slice(&bytes[SECRET_KEY_LENGTH..SECRET_KEY_LENGTH + 32]);
    let A = CompressedEdwardsY(pk)
        .decompress()
        .expect("valid ed25519 pk");

    // Zeroize secrets
    seed.zeroize();
    a_bytes.zeroize();

    (a, A)
}

// =============================
// Ring size selection based on CU
// =============================

pub struct RingSizer {
    pub min_size: usize,
    pub max_size: usize,
    pub target_cu_per_member: u32, // empirical CU per ring member
}

impl RingSizer {
    pub fn new(min_size: usize, max_size: usize, target_cu_per_member: u32) -> Self {
        Self { min_size, max_size, target_cu_per_member }
    }
    pub fn choose_size(&self, cu: &CuEstimator, decoy_pool_len: usize, tx_value_lamports: u64) -> usize {
        let (cu_limit, _price) = cu.suggest(0.0, 0.0);
        let budget_members = (cu_limit / self.target_cu_per_member).max(self.min_size as u32) as usize;
        let pool_bound = decoy_pool_len.saturating_sub(1).max(self.min_size);
        let value_bias = if tx_value_lamports >= 10_000_000_000 { 1.25 } else if tx_value_lamports >= 1_000_000_000 { 1.10 } else { 1.0 };
        ((budget_members as f64) * value_bias).round() as usize
            .clamp(self.min_size, self.max_size)
            .min(pool_bound)
    }
}

// =============================
// Decoy health metrics and reporting
// =============================

#[derive(Default, Clone)]
pub struct DecoyHealth {
    pub total: usize,
    pub recent: usize,
    pub mid: usize,
    pub old: usize,
    pub avg_balance: f64,
    pub stale_ratio: f64,
    pub active_use_avg: f64,
}

impl DecoyHealth {
    pub fn report(&self) {
        metrics::gauge!("decoy_total", self.total as f64);
        metrics::gauge!("decoy_recent", self.recent as f64);
        metrics::gauge!("decoy_mid", self.mid as f64);
        metrics::gauge!("decoy_old", self.old as f64);
        metrics::gauge!("decoy_avg_balance", self.avg_balance);
        metrics::gauge!("decoy_stale_ratio", self.stale_ratio);
        metrics::gauge!("decoy_active_use_avg", self.active_use_avg);
    }
}

pub fn compute_health(cache: &DecoyCache) -> DecoyHealth {
    let snap = cache.snapshot.read().clone();
    let total = snap.total_len;
    let recent = snap.recent.len();
    let mid = snap.mid.len();
    let old = snap.old.len();
    let sample_n = (total as f64).sqrt() as usize + 32;
    let mut sum_bal: u128 = 0;
    let mut sum_use: u128 = 0;
    let mut sampled = 0usize;
    for kv in cache.entries.iter().take(sample_n) {
        let e = kv.value();
        sum_bal += e.balance as u128;
        sum_use += e.recent_use_count as u128;
        sampled += 1;
    }
    let avg_balance = if sampled > 0 { (sum_bal as f64) / (sampled as f64) } else { 0.0 };
    let active_use_avg = if sampled > 0 { (sum_use as f64) / (sampled as f64) } else { 0.0 };
    let stale_ratio = if total > 0 { (old as f64) / (total as f64) } else { 0.0 };
    DecoyHealth { total, recent, mid, old, avg_balance, stale_ratio, active_use_avg }
}

pub fn trigger_fetch_if_degraded(cache: &DecoyCache, min_total: usize, max_stale_ratio: f64) -> bool {
    let h = compute_health(cache);
    h.report();
    h.total < min_total || h.stale_ratio > max_stale_ratio
}

// =============================
// TimedSet with TTL and random capping
// =============================

pub struct TimedSet {
    inner: DashMap<[u8; 32], Instant>,
    ttl: Duration,
}

impl TimedSet {
    pub fn new(ttl: Duration) -> Self { Self { inner: DashMap::new(), ttl } }
    pub fn insert(&self, key: [u8; 32]) { self.inner.insert(key, Instant::now()); }
    pub fn contains(&self, key: &[u8; 32]) -> bool { self.inner.get(key).is_some() }
    pub fn prune(&self) {
        let now = Instant::now();
        self.inner.retain(|_, t| now.duration_since(*t) < self.ttl);
    }
    pub fn cap_random(&self, max_len: usize) {
        let len = self.inner.len();
        if len <= max_len { return; }
        let mut rng = CsRng::new();
        let drop_n = len - max_len;
        let mut n = 0usize;
        for k in self.inner.iter() {
            if n >= drop_n { break; }
            if rng.inner_mut().next_u32() & 1 == 1 {
                self.inner.remove(k.key());
                n += 1;
            }
        }
    }
}

// =============================
// Versioned transaction builder (v0 + ALTs)
// =============================

pub struct TxBuilderConfig {
    pub cu_limit: u32,
    pub cu_price_micro_lamports: u64,
}

pub fn build_versioned_with_alt(
    payer: &impl solana_sdk::signature::Signer,
    program_id: Pubkey,
    ring_ix_data: Vec<u8>,
    extra_ixs: Vec<Instruction>,
    alts: &[AddressLookupTableAccount],
    recent_blockhash: Hash,
    cfg: TxBuilderConfig,
) -> VersionedTransaction {
    let mut ixs = Vec::with_capacity(extra_ixs.len() + 3);
    ixs.push(ComputeBudgetInstruction::set_compute_unit_limit(cfg.cu_limit));
    ixs.push(ComputeBudgetInstruction::set_compute_unit_price(cfg.cu_price_micro_lamports));
    ixs.push(Instruction::new_with_bytes(program_id, &ring_ix_data, vec![]));
    ixs.extend(extra_ixs);

    let mut msg_v0 = MessageV0::compile(&payer.pubkey(), &ixs, alts);
    msg_v0.recent_blockhash = recent_blockhash;
    let vmsg = VersionedMessage::V0(msg_v0);
    VersionedTransaction::try_new(vmsg, &[payer]).expect("sign")
}
// =============================
// Secure RNG utilities (ChaCha20Rng)
// =============================

#[derive(Clone)]
pub struct CsRng(ChaCha20Rng);

impl CsRng {
    pub fn new() -> Self {
        let mut seed = [0u8; 32];
        getrandom(&mut seed).expect("OS RNG");
        let rng = ChaCha20Rng::from_seed(seed);
        seed.zeroize();
        Self(rng)
    }
    pub fn from_seed(seed: [u8; 32]) -> Self { Self(ChaCha20Rng::from_seed(seed)) }
    #[inline] pub fn inner_mut(&mut self) -> &mut ChaCha20Rng { &mut self.0 }
}

pub fn fill_secure(dst: &mut [u8]) { getrandom(dst).expect("OS RNG"); }

pub fn gen_u64() -> u64 { let mut r = CsRng::new(); r.inner_mut().next_u64() }

// =============================
// LSAG transcript domain helpers
// =============================

pub struct LsagDomain;

impl LsagDomain {
    #[inline]
    pub fn start(msg32: &[u8; 32]) -> Transcript {
        let mut t = Transcript::new(b"ring-mixer-lsag-v1");
        t.append_message(b"msg32", msg32);
        t
    }
    #[inline]
    pub fn bind_member(t: &mut Transcript, member_compressed: &[u8; 32]) {
        t.append_message(b"ring_member", member_compressed);
    }
    #[inline]
    pub fn bind_key_image(t: &mut Transcript, ki_compressed: &[u8; 32]) {
        t.append_message(b"key_image", ki_compressed);
    }
    #[inline]
    pub fn bind_L(t: &mut Transcript, L_compressed: &[u8; 32]) {
        t.append_message(b"L_point", L_compressed);
    }
    #[inline]
    pub fn challenge_scalar(t: &mut Transcript, label: &'static [u8]) -> Scalar {
        let mut buf = [0u8; 64];
        t.challenge_bytes(label, &mut buf);
        let s = Scalar::from_bytes_mod_order_wide(&buf);
        buf.fill(0);
        s
    }
}

// =============================
// Stateless LSAG precomputation helpers
// =============================

fn key_image_stateless(A: &EdwardsPoint, a: &Scalar) -> EdwardsPoint {
    // Hash-to-scalar over public key (Sha3_512 for consistency with compute_key_image)
    let mut hasher = Sha3_512::new();
    hasher.update(b"Hp");
    hasher.update(&A.compress().to_bytes());
    let hp_scalar = Scalar::from_bytes_mod_order_wide(&hasher.finalize().into());
    let HpA = G * hp_scalar;
    HpA * *a
}

fn lsag_sign_stateless(
    msg32: &[u8; 32],
    ring: &[EdwardsPoint],
    signer_idx: usize,
    a: &Scalar,
    A_signer: &EdwardsPoint,
    rng: &mut impl rand::RngCore,
) -> RingSignature {
    let n = ring.len();
    assert!(n >= 2 && signer_idx < n);

    let I_point = key_image_stateless(A_signer, a);
    let I_comp = I_point.compress();

    let mut s_vec = vec![Scalar::zero(); n];
    for i in 0..n { if i != signer_idx { s_vec[i] = Scalar::random(rng); } }
    let alpha = Scalar::random(rng);

    // Initial transcript
    let mut t = LsagDomain::start(msg32);
    for P in ring { LsagDomain::bind_member(&mut t, P.compress().as_bytes()); }
    LsagDomain::bind_key_image(&mut t, I_comp.as_bytes());
    let L_signer = G * alpha;
    LsagDomain::bind_L(&mut t, L_signer.compress().as_bytes());
    let mut c = LsagDomain::challenge_scalar(&mut t, b"c0");
    let c0 = c;

    // Walk ring
    for k in 1..=n {
        let j = (signer_idx + k) % n;
        if j == signer_idx {
            s_vec[j] = alpha - (c * a);
        } else {
            let L_j = (G * s_vec[j]) + (ring[j] * c);
            let mut tt = LsagDomain::start(msg32);
            LsagDomain::bind_member(&mut tt, ring[j].compress().as_bytes());
            LsagDomain::bind_key_image(&mut tt, I_comp.as_bytes());
            LsagDomain::bind_L(&mut tt, L_j.compress().as_bytes());
            c = LsagDomain::challenge_scalar(&mut tt, b"c");
        }
    }

    RingSignature {
        c0: c0.to_bytes(),
        responses: s_vec.into_iter().map(|s| s.to_bytes()).collect(),
        key_image: I_comp.to_bytes(),
        ring_pubkeys: ring.iter().map(|p| p.compress().to_bytes()).collect(),
    }
}

pub fn precompute_signatures_rayon(
    msg32: &[u8; 32],
    rings: Vec<(Vec<EdwardsPoint>, usize, Scalar, EdwardsPoint)>,
) -> Vec<RingSignature> {
    rings
        .into_par_iter()
        .map(|(ring, idx, a, A)| {
            let mut rng = CsRng::new();
            lsag_sign_stateless(msg32, &ring, idx, &a, &A, rng.inner_mut())
        })
        .collect()
}

pub async fn precompute_signatures_tokio(
    msg32: [u8; 32],
    rings: Vec<(Vec<EdwardsPoint>, usize, Scalar, EdwardsPoint)>,
) -> Vec<RingSignature> {
    use tokio::task::JoinSet;
    let mut set = JoinSet::new();
    for (ring, idx, a, A) in rings.into_iter() {
        let msg = msg32;
        set.spawn_blocking(move || {
            let mut rng = CsRng::new();
            lsag_sign_stateless(&msg, &ring, idx, &a, &A, rng.inner_mut())
        });
    }
    let mut out = Vec::new();
    while let Some(res) = set.join_next().await { out.push(res.expect("join ok")); }
    out
}

// =========== DashMap-based Decoy Cache and alias sampling ===========

#[inline]
fn now_unix() -> u64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() }

#[derive(Debug, Clone)]
pub struct DecoyEntry {
    pub pubkey: Pubkey,
    pub balance: u64,
    pub last_active_unix: u64,
    pub last_seen_slot: u64,
    pub recent_fee: u64,
    pub recent_use_count: u32,
    pub last_used_slot: u64,
    pub priority: f64,
}

impl DecoyEntry {
    #[inline]
    pub fn compute_priority(balance: u64, last_active_unix: u64, recent_fee: u64, now_unix: u64) -> f64 {
        let bal = (balance.max(1) as f64).ln();
        let age_hours = ((now_unix.saturating_sub(last_active_unix)) as f64 / 3600.0).max(0.001);
        let activity = 1.0 / (1.0 + age_hours);
        let fee_score = (recent_fee as f64).min(200_000.0) / 200_000.0;
        let w_bal = 0.35; let w_act = 0.45; let w_fee = 0.20;
        let raw = w_bal * bal + w_act * activity + w_fee * fee_score;
        (1.0 - (-raw).exp()) * 1e6f64
    }
}

// =============================================================
// Ring selection, CU estimator, and ring reuse cache utilities
// =============================================================

#[derive(Clone)]
pub struct RingSelector {
    pub cache: Arc<DecoyCache>,
}

impl RingSelector {
    pub fn new(cache: Arc<DecoyCache>) -> Self { Self { cache } }
    pub fn select_ring_pubkeys(&self, ring_size: usize, seed: [u8; 32]) -> Vec<Pubkey> {
        let mut rng = StdRng::from_seed(seed);
        self.cache.select_ring(ring_size, &mut rng)
    }
}

use std::collections::VecDeque;

#[derive(Clone)]
pub struct CuEstimator {
    alpha: f64,
    ewma_cu: f64,
    ewma_price: f64,
    recent_prices: VecDeque<u64>,
    last_update: Instant,
    max_window: usize,
    min_price: u64,
    max_price: u64,
    floor_cu: u32,
    ceil_cu: u32,
}

impl CuEstimator {
    pub fn new() -> Self {
        Self {
            alpha: 0.2,
            ewma_cu: 150_000.0,
            ewma_price: 2_000.0,
            recent_prices: VecDeque::with_capacity(64),
            last_update: Instant::now(),
            max_window: 64,
            min_price: 500,
            max_price: 50_000,
            floor_cu: 100_000,
            ceil_cu: 800_000,
        }
    }
    pub fn observe(&mut self, used_cu: u64, paid_price: u64) {
        let u = used_cu as f64;
        let p = paid_price as f64;
        self.ewma_cu = self.alpha * u + (1.0 - self.alpha) * self.ewma_cu;
        self.ewma_price = self.alpha * p + (1.0 - self.alpha) * self.ewma_price;
        self.recent_prices.push_back(paid_price);
        if self.recent_prices.len() > self.max_window { self.recent_prices.pop_front(); }
        self.last_update = Instant::now();
    }
    pub fn suggest(&self, leader_bias: f64, congestion: f64) -> (u32, u64) {
        let mut cu = self.ewma_cu * (1.10 + 0.25 * congestion).max(1.0);
        let mut price = self.ewma_price * (1.10 + leader_bias + 0.25 * congestion).max(1.0);
        cu = cu.clamp(self.floor_cu as f64, self.ceil_cu as f64);
        price = price.clamp(self.min_price as f64, self.max_price as f64);
        (cu as u32, price as u64)
    }
}

use tokio::sync::RwLock as TRwLock;

pub struct RingReuseCache {
    rings: DashMap<u64, Arc<Vec<Pubkey>>>,
    order: TRwLock<Vec<u64>>, // keep only latest N slots
    max_slots: usize,
}

impl RingReuseCache {
    pub fn new(max_slots: usize) -> Self {
        Self { rings: DashMap::new(), order: TRwLock::new(Vec::with_capacity(max_slots)), max_slots }
    }
    pub async fn put(&self, slot: u64, ring: Arc<Vec<Pubkey>>) {
        self.rings.insert(slot, ring);
        let mut o = self.order.write().await;
        o.push(slot);
        while o.len() > self.max_slots {
            if let Some(old) = o.first().copied() {
                o.remove(0);
                self.rings.remove(&old);
            }
        }
    }
    pub fn get(&self, slot: u64) -> Option<Arc<Vec<Pubkey>>> {
        self.rings.get(&slot).map(|v| v.value().clone())
    }
}

pub async fn batch_mix_transactions_reuse_rings(
    current_slot: u64,
    txs: Vec<Transaction>,
    cache: Arc<DecoyCache>,
    reuse_cache: Arc<RingReuseCache>,
    ring_size: usize,
) -> Vec<(Transaction, Arc<Vec<Pubkey>>)> {
    let selector = RingSelector::new(cache.clone());
    let ring_for_slot = if let Some(r) = reuse_cache.get(current_slot) {
        r
    } else {
        let hash = blake3::hash(&current_slot.to_le_bytes());
        let mut seed32 = [0u8; 32];
        seed32.copy_from_slice(hash.as_bytes());
        let ring = selector.select_ring_pubkeys(ring_size, seed32);
        let arc = Arc::new(ring);
        reuse_cache.put(current_slot, arc.clone()).await;
        arc
    };
    txs.into_iter().map(|t| (t, ring_for_slot.clone())).collect()
}

// =============================
// Bounded-parallel RPC helpers
// =============================

pub async fn parallel_get_transactions(
    rpc: Arc<RpcClient>,
    signatures: Vec<solana_sdk::signature::Signature>,
    concurrency: usize,
) -> Vec<solana_client::rpc_response::EncodedConfirmedTransactionWithStatusMeta> {
    use solana_transaction_status::UiTransactionEncoding;
    let sem = Arc::new(Semaphore::new(concurrency));
    stream::iter(signatures.into_iter())
        .map(|sig| {
            let rpc = rpc.clone();
            let sem = sem.clone();
            async move {
                let _p = sem.acquire().await.unwrap();
                rpc.get_transaction(&sig, UiTransactionEncoding::Base64).ok().flatten()
            }
        })
        .buffer_unordered(concurrency)
        .filter_map(async move |res| res)
        .collect::<Vec<_>>()
        .await
}

// =============================
// Short-lived generic cache
// =============================

pub struct ShortCache<T> {
    val: parking_lot::RwLock<Option<(T, Instant)>>,
    ttl: Duration,
}
impl<T: Clone> ShortCache<T> {
    pub fn new(ttl: Duration) -> Self { Self { val: parking_lot::RwLock::new(None), ttl } }
    pub fn get_or_set_with<F: FnOnce() -> T>(&self, f: F) -> T {
        {
            let g = self.val.read();
            if let Some((v, t)) = &*g {
                if t.elapsed() < self.ttl { return v.clone(); }
            }
        }
        let v = f();
        *self.val.write() = Some((v.clone(), Instant::now()));
        v
    }
}

// ==================================
// Randomized decoy transfer generator
// ==================================

pub fn randomized_decoy_transfer(
    payer: &Keypair,
    base_amount: u64,
    jitter_bp: u64,
    seed: [u8; 32],
    dest_hint: Option<Pubkey>,
) -> (Transaction, Keypair) {
    use solana_sdk::{instruction::Instruction, system_instruction};
    let mut rng = StdRng::from_seed(seed);
    let mut amount = base_amount;
    if jitter_bp > 0 {
        let jitter = rng.gen_range(0..=jitter_bp as i64) - (jitter_bp as i64 / 2);
        let delta = (amount as i128 * jitter as i128) / 10_000i128;
        amount = (amount as i128 + delta).max(1) as u64;
    }
    let dest = dest_hint.unwrap_or_else(|| Keypair::new().pubkey());
    let ix: Instruction = system_instruction::transfer(&payer.pubkey(), &dest, amount);
    let tx = Transaction::new_with_payer(&[ix], Some(&payer.pubkey()));
    (tx, Keypair::new())
}

// =============================
// Metrics exporter bootstrap
// =============================

pub fn init_metrics(addr: ([u8; 4], u16)) {
    use metrics_exporter_prometheus::PrometheusBuilder;
    let builder = PrometheusBuilder::new();
    let handle = builder.with_http_listener((addr.0, addr.1)).install().expect("prometheus exporter");
    let _ = handle;
    metrics::counter!("init_total", 1);
}

// =============================
// Integration demo (example)
// =============================

pub async fn integrate_example() -> Result<(), Box<dyn Error>> {
    let rpc = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
    let cache = Arc::new(DecoyCache::new(150_000));
    // Parallel refresh from RPC
    let source_addrs = vec![Keypair::new().pubkey()];
    cache.refresh_from_rpc(rpc.clone(), source_addrs, 64, 16).await?;
    // Rebuild snapshot and select a ring
    cache.rebuild_snapshot().await;
    let selector = RingSelector::new(cache.clone());
    let mut rng = StdRng::from_entropy();
    let seed: [u8; 32] = rng.gen();
    let ring_pubkeys = selector.select_ring_pubkeys(16, seed);
    // Map pubkeys to Edwards points for demo only (production: use real Edwards keys when available)
    let ring_points: Vec<EdwardsPoint> = ring_pubkeys
        .iter()
        .map(|k| EdwardsPoint::hash_from_bytes::<sha2::Sha512>(k.as_ref()))
        .collect();
    // Demo LSAG sign/verify with a synthetic signer
    let signer_idx = 3usize;
    let a = Scalar::random(&mut rng);
    let A = G * a;
    let mut rp = ring_points.clone();
    rp[signer_idx] = A;
    let msg = blake3::hash(b"ring-msg");
    let mut msg32 = [0u8; 32];
    msg32.copy_from_slice(msg.as_bytes());
    let sig = self::lsag_sign(&msg32, &rp, signer_idx, &a, &A, &mut rng)?;
    assert!(self::lsag_verify_internal(&rp, &sig.responses.iter().map(|b| Scalar::from_bytes_mod_order(*b)).collect::<Vec<_>>(), Scalar::from_bytes_mod_order(sig.c0), &msg32, CompressedEdwardsY(sig.key_image).decompress().ok_or("bad I")?));
    Ok(())
}

#[derive(Debug, Clone)]
pub struct AliasTable { prob: Vec<f32>, alias: Vec<u32>, keys: Vec<Pubkey> }
impl AliasTable {
    pub fn empty() -> Self { Self { prob: vec![], alias: vec![], keys: vec![] } }
    #[inline] pub fn len(&self) -> usize { self.keys.len() }
    #[inline] pub fn is_empty(&self) -> bool { self.keys.is_empty() }
    pub fn from_weighted(pairs: Vec<(Pubkey, f64)>) -> Self {
        if pairs.is_empty() { return Self::empty(); }
        let n = pairs.len();
        let mut keys = Vec::with_capacity(n);
        let mut weights = Vec::with_capacity(n);
        for (k, w) in pairs { keys.push(k); weights.push(if w.is_finite() && w > 0.0 { w } else { 1.0 }); }
        let sum: f64 = weights.iter().copied().sum::<f64>().max(core::f64::EPSILON);
        let mut scaled: Vec<f64> = weights.into_iter().map(|w| (w / sum) * n as f64).collect();
        let mut prob = vec![0f32; n];
        let mut alias = vec![0u32; n];
        let mut small = Vec::with_capacity(n);
        let mut large = Vec::with_capacity(n);
        for (i, &p) in scaled.iter().enumerate() { if p < 1.0 { small.push(i as u32); } else { large.push(i as u32); } }
        while let (Some(l), Some(g)) = (small.pop(), large.pop()) {
            prob[l as usize] = scaled[l as usize] as f32;
            alias[l as usize] = g;
            scaled[g as usize] = (scaled[g as usize] + scaled[l as usize]) - 1.0;
            if scaled[g as usize] < 1.0 { small.push(g); } else { large.push(g); }
        }
        for g in large { prob[g as usize] = 1.0; }
        for l in small { prob[l as usize] = 1.0; }
        Self { prob, alias, keys }
    }
    #[inline]
    pub fn sample_one<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> Option<Pubkey> {
        let n = self.len(); if n == 0 { return None; }
        let i = rng.gen_range(0..n) as u32; let y: f32 = rng.gen();
        let pick = if y < self.prob[i as usize] { i } else { self.alias[i as usize] };
        self.keys.get(pick as usize).copied()
    }
}

#[derive(Debug, Clone)]
pub struct AliasSnapshot { pub recent: AliasTable, pub mid: AliasTable, pub old: AliasTable, pub total_len: usize, pub epoch: u64 }
impl AliasSnapshot { pub fn empty() -> Self { Self { recent: AliasTable::empty(), mid: AliasTable::empty(), old: AliasTable::empty(), total_len: 0, epoch: 0 } } }

pub struct DecoyCache {
    entries: DashMap<Pubkey, DecoyEntry>,
    recent_ring_hashes: DashSet<[u8; 32]>,
    recent_key_images: DashSet<[u8; 32]>,
    snapshot: Arc<PRwLock<AliasSnapshot>>,
    max_entries: usize,
    prune_slot_window: u64,
    prune_inactive_secs: u64,
    max_reuse_per_window: u32,
    refresh_guard: Arc<Semaphore>,
}

impl DecoyCache {
    pub fn new(max_entries: usize) -> Self { Self { entries: DashMap::new(), recent_ring_hashes: DashSet::new(), recent_key_images: DashSet::new(), snapshot: Arc::new(PRwLock::new(AliasSnapshot::empty())), max_entries, prune_slot_window: 12_000, prune_inactive_secs: 7 * 24 * 3600, max_reuse_per_window: 3, refresh_guard: Arc::new(Semaphore::new(1)) } }
    pub fn upsert_batch(&self, mut batch: Vec<DecoyEntry>) { let now = now_unix(); for mut e in batch.drain(..) { e.priority = DecoyEntry::compute_priority(e.balance, e.last_active_unix, e.recent_fee, now); self.entries.insert(e.pubkey, e); } self.cap_if_needed(); }
    fn cap_if_needed(&self) { let len = self.entries.len(); if len <= self.max_entries { return; } let target = self.max_entries; let now = now_unix(); let mut v: Vec<_> = self.entries.iter().map(|kv| kv.value().clone()).collect(); v.sort_by(|a,b|{ let a_age=(now.saturating_sub(a.last_active_unix)) as f64/3600.0; let b_age=(now.saturating_sub(b.last_active_unix)) as f64/3600.0; let a_score=a_age+(a.recent_use_count as f64*2.0)-a.priority*0.001; let b_score=b_age+(b.recent_use_count as f64*2.0)-b.priority*0.001; a_score.partial_cmp(&b_score).unwrap()}); for ev in v.into_iter().take(len - target) { self.entries.remove(&ev.pubkey); } }
    pub async fn rebuild_snapshot(&self) { let _permit = self.refresh_guard.acquire().await.unwrap(); let now = now_unix(); let mut recent_pairs=Vec::with_capacity(1024); let mut mid_pairs=Vec::with_capacity(1024); let mut old_pairs=Vec::with_capacity(1024); for kv in self.entries.iter() { let e=kv.value(); let age=now.saturating_sub(e.last_active_unix); let pair=(e.pubkey,e.priority); if age <= 3*3600 { recent_pairs.push(pair);} else if age <= 24*3600 { mid_pairs.push(pair);} else { old_pairs.push(pair);} } let snap=AliasSnapshot{ recent:AliasTable::from_weighted(recent_pairs), mid:AliasTable::from_weighted(mid_pairs), old:AliasTable::from_weighted(old_pairs), total_len:self.entries.len(), epoch:now }; *self.snapshot.write()=snap; /* metrics::gauge!("decoy_cache_size", self.entries.len() as f64); */ }
    pub fn select_ring(&self, ring_size: usize, rng: &mut StdRng) -> Vec<Pubkey> { let snap=self.snapshot.read().clone(); let target_recent=(ring_size as f32*0.4).round() as usize; let target_mid=(ring_size as f32*0.35).round() as usize; let mut out=Vec::with_capacity(ring_size); let mut seen: HashSet<Pubkey>=HashSet::with_capacity(ring_size*2); let mut sample_unique = |tab:&AliasTable,want:usize|{ let mut taken=0usize; let cap=(want*4).max(want+2); for _ in 0..cap { if taken>=want { break; } if let Some(k)=tab.sample_one(rng) { if seen.insert(k) { out.push(k); taken+=1; } } else { break; } } taken }; let got_r=sample_unique(&snap.recent,target_recent); let got_m=sample_unique(&snap.mid,target_mid); let got_o=sample_unique(&snap.old, ring_size.saturating_sub(got_r+got_m)); let mut remaining=ring_size.saturating_sub(got_r+got_m+got_o); while remaining>0 && (!snap.recent.is_empty()||!snap.mid.is_empty()||!snap.old.is_empty()) { if !snap.recent.is_empty() && remaining>0 && sample_unique(&snap.recent,1)==1 { remaining-=1; } if !snap.mid.is_empty() && remaining>0 && sample_unique(&snap.mid,1)==1 { remaining-=1; } if !snap.old.is_empty() && remaining>0 && sample_unique(&snap.old,1)==1 { remaining-=1; } if remaining==0 { break; } } out }
    #[inline] pub fn record_ring_hash(&self, ring_hash:[u8;32]) { self.recent_ring_hashes.insert(ring_hash); }
    #[inline] pub fn ring_hash_seen(&self, ring_hash:&[u8;32]) -> bool { self.recent_ring_hashes.contains(ring_hash) }
    #[inline] pub fn record_key_image(&self, ki:[u8;32]) { self.recent_key_images.insert(ki); }
    #[inline] pub fn key_image_seen(&self, ki:&[u8;32]) -> bool { self.recent_key_images.contains(ki) }
    pub fn prune_reuse_windows(&self, keep_limit: usize) { let purge = |set:&DashSet<[u8;32]>, limit:usize| { let len=set.len(); if len<=limit { return; } let to_remove=len - limit; let mut n=0usize; for k in set.iter() { if n>=to_remove { break; } set.remove(k.key()); n+=1; } }; purge(&self.recent_ring_hashes, keep_limit); purge(&self.recent_key_images, keep_limit); }
    pub fn prune_entries(&self, current_slot: u64) { let now=now_unix(); let prune_slot_window=self.prune_slot_window; let prune_inactive_secs=self.prune_inactive_secs; let mut removed=0usize; self.entries.retain(|_,e| { let by_slot=current_slot.saturating_sub(e.last_seen_slot) <= prune_slot_window; let by_time=now.saturating_sub(e.last_active_unix) <= prune_inactive_secs; let keep=by_slot && by_time; if !keep { removed+=1; } keep }); /* if removed>0 { metrics::counter!("decoy_cache_pruned", removed as u64); } */ }
    pub async fn refresh_from_rpc(&self, rpc: Arc<RpcClient>, source_addrs: Vec<Pubkey>, max_sigs_per_addr: usize, concurrency: usize) -> Result<(), Box<dyn Error>> { use futures::{stream, StreamExt}; use solana_transaction_status::UiTransactionEncoding; let sem=Arc::new(Semaphore::new(concurrency)); let sigs = stream::iter(source_addrs.into_iter()).map(|addr|{ let rpc=rpc.clone(); let sem=sem.clone(); async move { let _p=sem.acquire().await.unwrap(); rpc.get_signatures_for_address(&addr, None).ok() } }).buffer_unordered(concurrency).filter_map(async move |res| res).collect::<Vec<_>>().await; let mut tx_sigs=Vec::new(); for batch in sigs { for s in batch.into_iter().take(max_sigs_per_addr) { if let Ok(sig)=s.signature.parse() { tx_sigs.push(sig); } } } let txs = stream::iter(tx_sigs.into_iter()).map(|sig|{ let rpc=rpc.clone(); let sem=sem.clone(); async move { let _p=sem.acquire().await.unwrap(); rpc.get_transaction(&sig, UiTransactionEncoding::Base64).ok().flatten() } }).buffer_unordered(concurrency).filter_map(async move |res| res).collect::<Vec<_>>().await; let mut decoys=Vec::with_capacity(txs.len()*8); for tx in txs { if let (Some(meta), Some(xtx)) = (tx.transaction.meta.as_ref(), tx.transaction.transaction.decode()) { let block_time=tx.block_time.unwrap_or(now_unix() as i64).max(0) as u64; let slot=tx.slot; for (i, bal) in meta.post_balances.iter().enumerate() { if let Some(pk)=xtx.message.account_keys.get(i) { let entry=DecoyEntry{ pubkey:*pk, balance:*bal, last_active_unix:block_time, last_seen_slot:slot, recent_fee:meta.fee, recent_use_count:0, last_used_slot:0, priority:0.0 }; decoys.push(entry); } } } } self.upsert_batch(decoys); self.rebuild_snapshot().await; Ok(()) }
}

#[derive(Debug, Clone)]
struct DecoyAddress {
    pubkey: Pubkey,
    balance: u64,
    last_active: u64,
    priority_score: f64,
}

#[derive(Debug, Clone)]
struct RingContext {
    ring_members: Vec<EdwardsPoint>,
    key_images: Vec<CompressedEdwardsY>,
    created_at: Instant,
    tx_hash: [u8; 32],
}

#[derive(BorshSerialize, BorshDeserialize, Debug)]
pub struct RingSignature {
    pub c0: [u8; 32],
    pub responses: Vec<[u8; 32]>,
    pub key_image: [u8; 32],
    pub ring_pubkeys: Vec<[u8; 32]>,
}

impl RingSignatureMixer {
    pub fn new(rpc_url: &str, mixer_keypair: Keypair) -> Result<Self, Box<dyn Error>> {

        let rpc_client = Arc::new(RpcClient::new_with_timeout_and_commitment(
            rpc_url.to_string(),
            Duration::from_millis(500),
            CommitmentConfig::processed(),
        ));

        Ok(Self {
            rpc_client,
            decoy_cache: Arc::new(DecoyCache::new(MAX_DECOY_CACHE)),

            active_rings: Arc::new(RwLock::new(HashMap::new())),
            recent_key_images: Arc::new(DashMap::new()),
            recent_ring_hashes: Arc::new(DashMap::new()),
            mixer_keypair: Arc::new(mixer_keypair),
            m_sim_ok: register_int_counter_vec!("mixer_sim_ok", "simulate success", &["rpc","leader","tier"]).unwrap(),
            m_ring_build_lat_us: register_histogram_vec!("mixer_ring_build_us", "ring build latency", &["size"], vec![50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0]).unwrap(),
            m_cu_used: register_histogram_vec!("mixer_cu_used", "compute units used", &["rpc"], vec![50_000.0, 100_000.0, 200_000.0, 400_000.0, 800_000.0]).unwrap(),
            m_tip_paid: register_histogram_vec!("mixer_tip_paid", "priority fee paid (lamports)", &["rpc"], vec![0.0, 1_000.0, 10_000.0, 100_000.0, 1_000_000.0]).unwrap(),
            m_decoy_cache_churn: register_int_counter_vec!("mixer_decoy_cache_churn", "decoy cache updates", &["action"]).unwrap(),
        })
    }

    // ============================
    // LSAG with Merlin transcript
    // ============================
    fn lsag_sign(
        &self,
        msg32: &[u8; 32],
        ring: &[EdwardsPoint],
        signer_idx: usize,
        a: &Scalar,
        A_signer: &EdwardsPoint,
        rng: &mut impl rand::RngCore,
    ) -> Result<RingSignature, Box<dyn Error>> {
        let n = ring.len();
        if n < 2 || signer_idx >= n { return Err("bad ring".into()); }
        let I = self.compute_key_image(a, A_signer)?;
        let I_point = I.decompress().ok_or("bad key image")?;

        // Random responses except signer
        let mut s_vec = vec![Scalar::zero(); n];
        for i in 0..n { if i != signer_idx { s_vec[i] = Scalar::random(rng); } }
        let alpha = Scalar::random(rng);

        // Initial transcript covers message, ring members and key image, and L at signer index
        let L_signer = G * alpha;
        let mut t = Transcript::new(b"LSAG");
        t.append_message(b"msg", msg32);
        for P in ring { t.append_message(b"ring_member", P.compress().as_bytes()); }
        t.append_message(b"key_image", I.as_bytes());
        t.append_message(b"L", L_signer.compress().as_bytes());
        let mut buf = [0u8; 64];
        t.challenge_bytes(b"c0", &mut buf);
        let mut c = Scalar::from_bytes_mod_order_wide(&buf);
        let c0 = c;

        // Walk the ring
        for k in 1..=n {
            let j = (signer_idx + k) % n;
            if j == signer_idx {
                s_vec[j] = alpha - (c * a);
            } else {
                let L_j = (G * s_vec[j]) + (ring[j] * c);
                let mut tt = Transcript::new(b"LSAG");
                tt.append_message(b"msg", msg32);
                tt.append_message(b"ring_member", ring[j].compress().as_bytes());
                tt.append_message(b"key_image", I.as_bytes());
                tt.append_message(b"L", L_j.compress().as_bytes());
                let mut nb = [0u8; 64];
                tt.challenge_bytes(b"c_next", &mut nb);
                c = Scalar::from_bytes_mod_order_wide(&nb);
            }
        }

        let ring_pubkeys = ring.iter().map(|p| p.compress().to_bytes()).collect();
        Ok(RingSignature {
            c0: c0.to_bytes(),
            responses: s_vec.into_iter().map(|x| x.to_bytes()).collect(),
            key_image: I.to_bytes(),
            ring_pubkeys,
        })
    }

    fn lsag_verify_internal(
        &self,
        ring: &[EdwardsPoint],
        s: &[Scalar],
        c0: Scalar,
        msg32: &[u8; 32],
        I: EdwardsPoint,
    ) -> bool {
        if ring.len() == 0 || ring.len() != s.len() { return false; }
        let mut c = c0;
        for j in 0..ring.len() {
            let L_j = (G * s[j]) + (ring[j] * c);
            let mut tt = Transcript::new(b"LSAG");
            tt.append_message(b"msg", msg32);
            tt.append_message(b"ring_member", ring[j].compress().as_bytes());
            tt.append_message(b"key_image", I.compress().as_bytes());
            tt.append_message(b"L", L_j.compress().as_bytes());
            let mut buf = [0u8; 64];
            tt.challenge_bytes(b"c_next", &mut buf);
            c = Scalar::from_bytes_mod_order_wide(&buf);
        }
        c == c0
    }

    pub async fn mix_transaction(
        &self,
        tx: Transaction,
        signer_keypair: &Keypair,
    ) -> Result<Transaction, Box<dyn Error>> {
        let tx_hash = self.compute_tx_hash(&tx);
        
        self.refresh_decoy_cache_if_needed().await?;
        // Offload CPU-heavy ring creation to blocking pool
        let t0 = Instant::now();
        let mixer = self.clone();
        let signer = signer_keypair.clone();
        let message = tx_hash;
        let ring_signature = tokio::task::spawn_blocking(move || mixer.create_ring_signature(&message, &signer))
            .await
            .map_err(|_| "spawn_blocking failed")??;
        let dt = t0.elapsed().as_micros() as f64;
        self.m_ring_build_lat_us.with_label_values(&[&format!("{}", RING_SIZE)]).observe(dt);

        let mixed_tx = self.build_mixed_transaction(
            tx,
            ring_signature,
            signer_keypair,
        )?;

        Ok(mixed_tx)
    }

    fn create_ring_signature(
        &self,
        message: &[u8; 32],
        signer: &Keypair,
    ) -> Result<RingSignature, Box<dyn Error>> {
        let mut rng = OsRng;
        
        let (a_scalar, A_signer) = self.ed25519_to_scalar_and_point(signer)?;

        // Build ring: signer + decoys from alias snapshot; fallback to old path if empty
        let mut ring_members = Vec::with_capacity(RING_SIZE);
        ring_members.push(A_signer);

        let mut rng = StdRng::from_entropy();
        let decoy_pubkeys = self.decoy_cache.select_ring(RING_SIZE - 1, &mut rng);
        if decoy_pubkeys.is_empty() { return Err("insufficient valid decoys".into()); }
        for pk in decoy_pubkeys { if let Some(p) = self.pubkey_to_edwards_point(&pk) { ring_members.push(p); } }

        if ring_members.len() != RING_SIZE { return Err("insufficient valid decoys".into()); }

        let signer_index = rng.gen_range(0..RING_SIZE);
        if signer_index != 0 { ring_members.swap(0, signer_index); }

        // LSAG sign with Merlin transcript
        let sig = self.lsag_sign(message, &ring_members, signer_index, &a_scalar, &A_signer, &mut rng)?;
        Ok(sig)
    }

    fn generate_ring_responses<R: RngCore + CryptoRng>(
        &self,
        message: &[u8; 32],
        ring: &[EdwardsPoint],
        signer_index: usize,
        private_key: &Scalar,
        rng: &mut R,
    ) -> Result<(Scalar, Vec<Scalar>), Box<dyn Error>> {
        let n = ring.len();
        let mut responses = vec![Scalar::zero(); n];
        let mut commitments = vec![G; n];
        
        let alpha = Scalar::random(rng);
        commitments[signer_index] = G * alpha;

        for i in 0..n {
            if i != signer_index {
                responses[i] = Scalar::random(rng);
                let c_i = self.hash_to_scalar(&[
                    message.as_ref(),
                    &ring[i].compress().to_bytes(),
                ].concat());
                commitments[i] = (G * responses[i]) + (ring[i] * c_i);
            }
        }

        let challenge_input = self.build_challenge_input(message, &commitments);
        let c0 = self.hash_to_scalar(&challenge_input);
        
        let mut c = c0;
        for i in 0..n {
            if i == signer_index {
                let c_next = if i == n - 1 { c0 } else {
                    self.compute_next_challenge(&c, &commitments[i + 1], message)
                };
                responses[i] = alpha - (c * private_key);
                c = c_next;
            } else if i < n - 1 {
                c = self.compute_next_challenge(&c, &commitments[i + 1], message);
            }
        }

        Ok((c0, responses))
    }

    fn verify_ring_signature(
        &self,
        signature: &RingSignature,
        message: &[u8; 32],
    ) -> Result<bool, Box<dyn Error>> {
        let ring: Vec<EdwardsPoint> = signature.ring_pubkeys
            .par_iter()
            .map(|bytes| CompressedEdwardsY::from_slice(bytes).decompress())
            .collect::<Option<Vec<_>>>()
            .ok_or("Invalid ring member")?;
        let s: Vec<Scalar> = signature.responses.iter().map(|b| Scalar::from_bytes_mod_order(*b)).collect();
        let c0 = Scalar::from_bytes_mod_order(signature.c0);
        Ok(self.lsag_verify_internal(&ring, &s, c0, message, &CompressedEdwardsY(signature.key_image).decompress().ok_or("bad I")?))
    }

    async fn refresh_decoy_cache_if_needed(&self) -> Result<(), Box<dyn Error>> {
        let should_refresh = {
            let cache = self.decoy_cache.read().unwrap();
            cache.last_refresh.elapsed() > Duration::from_millis(DECOY_REFRESH_INTERVAL_MS)
                || cache.addresses.len() < RING_SIZE * 2
        };

        if should_refresh {
            self.refresh_decoy_cache().await?;
        }

        Ok(())
    }

    async fn refresh_decoy_cache(&self) -> Result<(), Box<dyn Error>> {
        let recent_accounts = self.fetch_active_accounts().await?;
        
        let mut cache = self.decoy_cache.write().unwrap();
        // Incremental refresh: add new up to cap, pop oldest
        let before = cache.addresses.len();
        for account in recent_accounts.into_iter().take(MAX_DECOY_CACHE) {
            cache.addresses.push_back(account);
            if cache.addresses.len() > MAX_DECOY_CACHE { cache.addresses.pop_front(); }
        }
        let after = cache.addresses.len();
        if after > before { self.m_decoy_cache_churn.with_label_values(&["insert"]).inc_by((after - before) as u64); }
        cache.last_refresh = Instant::now();
        Ok(())
    }

    async fn fetch_active_accounts(&self) -> Result<Vec<DecoyAddress>, Box<dyn Error>> {
        let recent_signatures = self.rpc_client
            .get_signatures_for_address(
                &self.mixer_keypair.pubkey(),
                None,
            )?;

        let mut active_accounts = HashMap::new();
        
        for sig_info in recent_signatures.iter().take(200) {
            if let Ok(tx) = self.rpc_client.get_transaction(
                &sig_info.signature.parse()?,
                solana_transaction_status::UiTransactionEncoding::Base64,
            ) {
                if let Some(meta) = tx.transaction.meta {
                    for (i, _) in meta.pre_balances.iter().enumerate() {
                        if let Some(pubkey) = tx.transaction.transaction
                            .decode()
                            .and_then(|t| t.message.account_keys.get(i))
                        {
                            // Filter PDAs (off-curve) and service hubs
                            if !pubkey.is_on_curve() { continue; }
                            if self.is_service_hub(pubkey) { continue; }
                            let entry = active_accounts.entry(*pubkey).or_insert(DecoyAddress {
                                pubkey: *pubkey,
                                balance: meta.post_balances[i],
                                last_active: tx.block_time.unwrap_or(0) as u64,
                                priority_score: 0.0,
                            });
                            
                            entry.priority_score = self.calculate_decoy_priority(
                                entry.balance,
                                entry.last_active,
                                meta.fee,
                            );
                        }
                    }
                }
            }
        }

        let mut accounts: Vec<_> = active_accounts.into_values().collect();
        accounts.sort_by(|a, b| b.priority_score.partial_cmp(&a.priority_score).unwrap());
        
        Ok(accounts)
    }

    fn calculate_decoy_priority(&self, balance: u64, last_active_unix: u64, fee: u64) -> f64 {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let balance_weight = 0.4;
        let activity_weight = 0.4;
        let fee_weight = 0.2;
        let balance_score = ((balance.max(1)) as f64).log10() / 10.0;
        let age_hours = ((now.saturating_sub(last_active_unix)) as f64 / 3600.0).max(0.001);
        let activity_score = 1.0 / (1.0 + age_hours);
        let fee_score = (fee as f64) / 50_000.0;
        balance_weight * balance_score + activity_weight * activity_score + fee_weight * fee_score
    }

    fn select_optimal_decoys(&self, count: usize) -> Result<Vec<DecoyAddress>, Box<dyn Error>> {
        let cache = self.decoy_cache.read().unwrap();
        if cache.addresses.len() < count { return Err("Insufficient decoys in cache".into()); }

        // Stratified sampling by last_active: recent, mid, older
        let mut recent = Vec::new();
        let mut mid = Vec::new();
        let mut older = Vec::new();
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        for d in cache.addresses.iter() {
            let age = now.saturating_sub(d.last_active);
            if age <= 6 * 3600 { recent.push(d.clone()); }
            else if age <= 48 * 3600 { mid.push(d.clone()); }
            else { older.push(d.clone()); }
        }
        let r_quota = (count as f64 * 0.34).ceil() as usize;
        let m_quota = (count as f64 * 0.33).floor() as usize;
        let o_quota = count.saturating_sub(r_quota + m_quota);

        let mut out = Vec::with_capacity(count);
        let mut rng = rand::thread_rng();

        // Helper to build weighted pool by priority and sample without replacement
        fn sample_weighted_no_replacement(pool: Vec<DecoyAddress>, k: usize, rng: &mut impl rand::Rng) -> Vec<DecoyAddress> {
            let mut items: Vec<(DecoyAddress, f64)> = pool.into_iter().map(|d| {
                // Weight from balance/activity proxy
                let w = ((d.balance.max(1)) as f64).log10() + 1.0;
                (d, w)
            }).collect();
            let mut out = Vec::with_capacity(k);
            for _ in 0..k.min(items.len()) {
                let total: f64 = items.iter().map(|(_, w)| *w).sum();
                if total <= 0.0 { break; }
                let mut draw = rng.gen::<f64>() * total;
                let mut idx = 0usize;
                for (i, (_, w)) in items.iter().enumerate() { if draw <= *w { idx = i; break; } else { draw -= *w; } }
                out.push(items[idx].0.clone());
                items.swap_remove(idx);
            }
            out
        }

        for (bucket, quota) in [(recent, r_quota), (mid, m_quota), (older, o_quota)] {
            if quota == 0 || bucket.is_empty() { continue; }
            let take = sample_weighted_no_replacement(bucket, quota, &mut rng);
            for d in take { out.push(d); if out.len() == count { break; } }
            if out.len() == count { break; }
        }
        if out.len() < count {
            // Fill from remaining cache uniformly
            let mut idxs: Vec<usize> = (0..cache.addresses.len()).collect();
            idxs.shuffle(&mut rng);
            for i in idxs { out.push(cache.addresses[i].clone()); if out.len() == count { break; } }
        }
        Ok(out)
    }

    fn build_mixed_transaction(
        &self,
        mut original_tx: Transaction,
        ring_sig: RingSignature,
        signer: &Keypair,
    ) -> Result<Transaction, Box<dyn Error>> {
        let ring_sig_data = borsh::to_vec(&ring_sig)?;
        
        // Use the mixer program ID, not the fee payer pubkey
        let mixer_instruction = Instruction::new_with_bytes(MIXER_PROGRAM_ID, &ring_sig_data, vec![]);
        
        original_tx.message.instructions.insert(0, mixer_instruction);
        
        let recent_blockhash = self.rpc_client.get_latest_blockhash()?;
        original_tx.message.recent_blockhash = recent_blockhash;
        
        original_tx.try_partial_sign(&[signer], recent_blockhash)?;
        
        Ok(original_tx)
    }

    // Build a Versioned v0 message with optional ALTs and compute budget settings
    fn build_v0_with_alt(
        &self,
        payer: &Pubkey,
        mut ixs: Vec<Instruction>,
        alts: &[AddressLookupTableAccount],
        recent_blockhash: Hash,
    ) -> VersionedMessage {
        // Dynamic fee hooks (could be EWMA/leader-aware); placeholder values for now
        let (cu_limit, cu_price_micro_lamports) = self.compute_dynamic_fee();
        let mut full_ixs = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
            ComputeBudgetInstruction::set_compute_unit_price(cu_price_micro_lamports),
        ];
        full_ixs.append(&mut ixs);
        let mut msg = MessageV0::compile(payer, &full_ixs, alts);
        msg.recent_blockhash = recent_blockhash;
        VersionedMessage::V0(msg)
    }

    // Versioned message flow using ALTs for ring member pubkeys
    pub fn build_mixed_transaction_v0(
        &self,
        ring_sig: RingSignature,
        signer: &Keypair,
        alts: &[AddressLookupTableAccount],
    ) -> Result<VersionedTransaction, Box<dyn Error>> {
        let ring_sig_data = borsh::to_vec(&ring_sig)?;
        let ix = Instruction::new_with_bytes(MIXER_PROGRAM_ID, &ring_sig_data, vec![]);
        let recent_blockhash = self.rpc_client.get_latest_blockhash()?;
        let vmsg = self.build_v0_with_alt(&signer.pubkey(), vec![ix], alts, recent_blockhash);
        let vtx = VersionedTransaction::try_new(vmsg, &[signer])?;
        Ok(vtx)
    }

    // Full builder: take an existing legacy Transaction, prepend compute budget and mixer ix, convert to v0 with ALTs and sign
    pub fn build_mixed_transaction_v0_full(
        &self,
        mut base_tx: Transaction,
        ring_sig_bytes: Vec<u8>,
        program_id: Pubkey,
        alts: &[AddressLookupTableAccount],
        fee_micro_lamports_per_cu: u64,
        cu_limit: u32,
    ) -> Result<VersionedTransaction, Box<dyn Error>> {
        // Mixer instruction
        let mixer_ix = Instruction::new_with_bytes(program_id, &ring_sig_bytes, vec![]);
        // Compute budget ixs
        let mut ixs: Vec<Instruction> = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
            ComputeBudgetInstruction::set_compute_unit_price(fee_micro_lamports_per_cu),
        ];
        ixs.push(mixer_ix);
        // Append original instructions
        ixs.extend(base_tx.message.instructions.clone());
        // Payer
        let payer = base_tx.message.account_keys[0];
        // Blockhash
        let recent_blockhash = self.rpc_client.get_latest_blockhash()?;
        // Compile v0
        let mut v0 = MessageV0::compile(&payer, &ixs, alts);
        v0.recent_blockhash = recent_blockhash;
        let msg = VersionedMessage::V0(v0);
        // Sign
        let mut vtx = VersionedTransaction::new(msg, &[&*self.mixer_keypair]);
        vtx.sign(&[&*self.mixer_keypair], recent_blockhash);
        Ok(vtx)
    }

    // Compute dynamic compute-unit limit/price; TODO: plug in EWMA and leader-aware logic
    fn compute_dynamic_fee(&self) -> (u32, u64) {
        // Defaults: 500k CU, 3000 micro-lamports per CU; tune dynamically at runtime
        (500_000u32, 3_000u64)
    }

    fn compute_tx_hash(&self, tx: &Transaction) -> [u8; 32] {
        // Bind to canonical message bytes only (no signatures)
        let mut hasher = Sha3_512::new();
        hasher.update(&tx.message_data());
        let result = hasher.finalize();
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&result[..32]);
        hash
    }

    #[derive(Serialize)]
    struct TxLog<'a> { tx_hash: String, leader: Option<String>, tip: u64, cu: u64, outcome: &'a str }

    fn log_event(&self, tx_hash: [u8;32], leader: Option<&Pubkey>, tip: u64, cu: u64, outcome: &str) {
        let rec = TxLog { tx_hash: hex::encode(tx_hash), leader: leader.map(|p| p.to_string()), tip, cu, outcome };
        if let Ok(s) = serde_json::to_string(&rec) { println!("{}", s); }
    }

    #[cfg(test)]
    mod tests_lsag {
        use super::*;
        use rand::thread_rng;

        #[test]
        fn lsag_roundtrip_small_ring() {
            let mixer = RingSignatureMixer::new(
                "https://api.mainnet-beta.solana.com",
                Keypair::new(),
            ).unwrap();
            let mut rng = thread_rng();
            // Build ring
            let mut ring: Vec<EdwardsPoint> = (0..16).map(|_| { let s = Scalar::random(&mut rng); G * s }).collect();
            let signer_idx = 5usize;
            let a = Scalar::random(&mut rng);
            let A = G * a;
            ring[signer_idx] = A;
            let msg = [42u8; 32];
            let sig = mixer.lsag_sign(&msg, &ring, signer_idx, &a, &A, &mut rng).unwrap();
            // Verify via public API
            assert!(mixer.verify_ring_signature(&sig, &msg).unwrap());
        }
    }

    fn ed25519_to_scalar_and_point(&self, keypair: &Keypair) -> Result<(Scalar, EdwardsPoint), Box<dyn Error>> {
        // Solana Keypair bytes: [secret(32) || pubkey(32)]
        let sk_bytes = keypair.to_bytes();
        let mut seed = [0u8; SECRET_KEY_LENGTH];
        seed.copy_from_slice(&sk_bytes[..SECRET_KEY_LENGTH]);
        let signing = SigningKey::from_bytes(&seed);
        let verifying: VerifyingKey = signing.verifying_key();
        let comp = CompressedEdwardsY(verifying.to_bytes());
        let A = comp.decompress().ok_or("Invalid ed25519 pubkey")?;
        // RFC8032 hash and clamp
        let h = Sha512::digest(&seed);
        let mut a_bytes = [0u8; 32];
        a_bytes.copy_from_slice(&h[..32]);
        a_bytes[0] &= 248;
        a_bytes[31] &= 63;
        a_bytes[31] |= 64;
        let a = Scalar::from_bits(a_bytes);
        let mut tmp = seed;
        tmp.zeroize();
        Ok((a, A))
    }

    fn pubkey_to_edwards_point(&self, pubkey: &Pubkey) -> Option<EdwardsPoint> {
        let comp = CompressedEdwardsY(pubkey.to_bytes());
        comp.decompress()
    }

    fn extract_private_scalar(&self, keypair: &Keypair) -> Result<Scalar, Box<dyn Error>> {
        // Derive ed25519 secret scalar per RFC8032
        let sk_bytes = keypair.to_bytes();
        let mut seed = [0u8; SECRET_KEY_LENGTH];
        seed.copy_from_slice(&sk_bytes[..SECRET_KEY_LENGTH]);
        let h = Sha512::digest(&seed);
        let mut a_bytes = [0u8; 32];
        a_bytes.copy_from_slice(&h[..32]);
        a_bytes[0] &= 248;
        a_bytes[31] &= 63;
        a_bytes[31] |= 64;
        let a = Scalar::from_bits(a_bytes);
        let mut tmp = seed; tmp.zeroize();
        Ok(a)
    }

    fn compute_key_image(&self, private_key: &Scalar, public_point: &EdwardsPoint) -> Result<CompressedEdwardsY, Box<dyn Error>> {
        // Monero-style: I = H_p(A) * a, approximate H_p via hash to scalar times basepoint
        let mut hasher = Sha3_512::new();
        hasher.update(b"Hp");
        hasher.update(&public_point.compress().to_bytes());
        let hp_scalar = Scalar::from_bytes_mod_order_wide(&hasher.finalize().into());
        let HpA = G * hp_scalar;
        let key_image = HpA * *private_key;
        Ok(key_image.compress())
    }

    // For Edwards, we avoid ad-hoc hash-to-point; use hash-to-scalar * basepoint where needed.

    fn hash_to_scalar(&self, data: &[u8]) -> Scalar {
        let mut hasher = Sha3_512::new();
        hasher.update(b"HashToScalar");
        hasher.update(data);
        let hash = hasher.finalize();
        
        Scalar::from_bytes_mod_order_wide(&hash.into())
    }

    fn build_challenge_input(&self, message: &[u8], commitments: &[EdwardsPoint]) -> Vec<u8> {
        let mut input = Vec::with_capacity(32 + commitments.len() * 32);
        input.extend_from_slice(message);
        
        for commitment in commitments {
            input.extend_from_slice(&commitment.compress().to_bytes());
        }
        
        input
    }

    fn compute_next_challenge(&self, current: &Scalar, commitment: &EdwardsPoint, message: &[u8]) -> Scalar {
        let mut hasher = Sha3_512::new();
        hasher.update(&current.to_bytes());
        hasher.update(&commitment.compress().to_bytes());
        hasher.update(message);
        
        let hash = hasher.finalize();
        Scalar::from_bytes_mod_order_wide(&hash.into())
    }

    pub async fn batch_mix_transactions(
        &self,
        transactions: Vec<(Transaction, Keypair)>,
    ) -> Result<Vec<Transaction>, Box<dyn Error>> {
        let (tx_sender, mut tx_receiver) = mpsc::channel(transactions.len());
        let semaphore = Arc::new(tokio::sync::Semaphore::new(8));
        
        let tasks: Vec<_> = transactions
            .into_iter()
            .map(|(tx, keypair)| {
                let mixer = self.clone();
                let tx_sender = tx_sender.clone();
                let semaphore = semaphore.clone();
                
                tokio::spawn(async move {
                    let _permit = semaphore.acquire().await.unwrap();
                    let result = mixer.mix_transaction(tx, &keypair).await;
                    tx_sender.send(result).await.unwrap();
                })
            })
            .collect();

        drop(tx_sender);
        
        let mut mixed_txs = Vec::new();
        while let Some(result) = tx_receiver.recv().await {
            mixed_txs.push(result?);
        }
        
        for task in tasks {
            task.await?;
        }
        
        Ok(mixed_txs)
    }

    pub fn create_decoy_transaction(&self, amount: u64) -> Result<Transaction, Box<dyn Error>> {
        let decoy_keypair = Keypair::new();
        let instruction = solana_sdk::system_instruction::transfer(
            &self.mixer_keypair.pubkey(),
            &decoy_keypair.pubkey(),
            amount,
        );
        
        let recent_blockhash = self.rpc_client.get_latest_blockhash()?;
        let tx = Transaction::new_signed_with_payer(
            &[instruction],
            Some(&self.mixer_keypair.pubkey()),
            &[&*self.mixer_keypair],
            recent_blockhash,
        );
        
        Ok(tx)
    }

    pub async fn inject_decoy_transactions(&self, count: usize) -> Result<Vec<Signature>, Box<dyn Error>> {
        let mut signatures = Vec::with_capacity(count);
        let base_amount = 1000u64;
        
        for i in 0..count {
            let amount = base_amount + (i as u64 * 100);
            let tx = self.create_decoy_transaction(amount)?;
            
            match self.rpc_client.send_transaction(&tx) {
                Ok(sig) => signatures.push(sig),
                Err(e) => {
                    if signatures.len() >= count / 2 {
                        break;
                    } else {
                        return Err(format!("Failed to send decoy transaction: {}", e).into());
                    }
                }
            }
            
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        
        Ok(signatures)
    }

    pub fn cleanup_expired_rings(&self) -> usize {
        let mut active_rings = self.active_rings.write().unwrap();
        let now = Instant::now();
        let expiry = Duration::from_secs(300);
        
        active_rings.retain(|_, context| {
            now.duration_since(context.created_at) < expiry
        });
        
        active_rings.len()
    }

    pub async fn emergency_shutdown(&self) -> Result<(), Box<dyn Error>> {
        let mut active_rings = self.active_rings.write().unwrap();
        active_rings.clear();
        
        let mut cache = self.decoy_cache.write().unwrap();
        cache.addresses.clear();
        
        Ok(())
    }

    pub fn get_mixer_stats(&self) -> MixerStats {
        let cache = self.decoy_cache.read().unwrap();
        let rings = self.active_rings.read().unwrap();
        
        MixerStats {
            cached_decoys: cache.addresses.len(),
            active_rings: rings.len(),
            last_cache_refresh: cache.last_refresh.elapsed().as_secs(),
            avg_decoy_score: cache.addresses.iter()
                .map(|d| d.priority_score)
                .sum::<f64>() / cache.addresses.len().max(1) as f64,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MixerStats {
    pub cached_decoys: usize,
    pub active_rings: usize,
    pub last_cache_refresh: u64,
    pub avg_decoy_score: f64,
}

impl Default for RingSignatureMixer {
    fn default() -> Self {
        Self::new(
            "https://api.mainnet-beta.solana.com",
            Keypair::new(),
        ).expect("Failed to create default mixer")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_signature_creation() {
        let mixer = RingSignatureMixer::default();
        let message = [1u8; 32];
        let signer = Keypair::new();
        
        let result = mixer.create_ring_signature(&message, &signer);
        assert!(result.is_ok());
        
        let sig = result.unwrap();
        assert_eq!(sig.responses.len(), RING_SIZE);
        assert_eq!(sig.ring_pubkeys.len(), RING_SIZE);
    }

    #[tokio::test]
    async fn test_transaction_mixing() {
        let mixer = RingSignatureMixer::default();
        let keypair = Keypair::new();
        
        let instruction = solana_sdk::system_instruction::transfer(
            &keypair.pubkey(),
            &Pubkey::new_unique(),
            1000,
        );
        
        let tx = Transaction::new_with_payer(
            &[instruction],
            Some(&keypair.pubkey()),
        );
        
        let result = mixer.mix_transaction(tx, &keypair).await;
        assert!(result.is_ok());
    }
}

