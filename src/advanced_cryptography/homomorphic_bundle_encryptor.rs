// homomorphic_bundle_encryptor_v4.rs

use borsh::{BorshDeserialize, BorshSerialize};
use chacha20poly1305::{aead::{Aead, KeyInit, OsRng}, ChaCha20Poly1305, Key, Nonce};
use curve25519_dalek::{ristretto::RistrettoPoint, scalar::Scalar, traits::Identity};
use dashmap::DashMap;
use arc_swap::ArcSwap;
use merlin::Transcript;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;
use rug::{integer::Order, Integer};
use sha3::{Digest, Keccak256, Sha3_256};
use bulletproofs::{BulletproofGens, PedersenGens, RangeProof};
use subtle::ConstantTimeEq;
use zeroize::{Zeroize, Zeroizing};
use std::cell::UnsafeCell;
use getrandom::getrandom;
use blake3::hash as blake3_hash;

use std::{
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

// =============================
// Constants and configuration
// =============================

const VERSION: u8 = 4;
const PAILLIER_BITS: usize = 2048;        // mainnet: consider 3072 or 4096 with GMP acceleration
const MAX_SCALARS: usize = 32;            // number of homomorphic scalar items per bundle
const MAX_OPAQUE_BYTES: usize = 1_000_000;
const KEY_ROTATION_SECS: u64 = 300;       // 5 min epochs
const MAX_CACHED_KEYS: usize = 128;
const RANGE_BITS_DEFAULT: usize = 64;     // range for Bulletproofs; tune per label
const AEAD_NONCE_SIZE: usize = 12;

// =============================
// Utility: constant-time equals
// =============================

fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.ct_eq(b).into()
}

#[inline]
fn ct_eq32(a: &[u8; 32], b: &[u8; 32]) -> bool { a.ct_eq(b).into() }

#[inline]
fn u64_from_integer_mod_2k(m: &Integer, k: usize) -> u64 {
    let mask = if k >= 64 { !0u64 } else { (1u64 << k) - 1 };
    let be = to_be(m);
    let mut v: u64 = 0;
    let start = be.len().saturating_sub(8);
    for i in 0..8 {
        let b = *be.get(start + i).unwrap_or(&0u8);
        v = (v << 8) | (b as u64);
    }
    v & mask
}

// =================================
// Thread-local RNG and nonce helpers
// =================================

thread_local! {
    static TL_RNG: UnsafeCell<ChaCha20Rng> = UnsafeCell::new(new_thread_rng());
    static TL_NONCE_COUNTER: UnsafeCell<u64> = UnsafeCell::new(0u64);
    static TL_STREAM_ID: u64 = {
        let mut seed = [0u8; 32];
        getrandom(&mut seed).expect("OS RNG");
        let mut x = u64::from_le_bytes(blake3_hash(&seed).as_bytes()[..8].try_into().unwrap());
        if x == 0 { x = 1; }
        x
    };
}

fn new_thread_rng() -> ChaCha20Rng {
    let mut seed = [0u8; 32];
    getrandom(&mut seed).expect("OS RNG");
    ChaCha20Rng::from_seed(seed)
}

#[inline]
fn rng_fill(buf: &mut [u8]) {
    TL_RNG.with(|cell| unsafe { (&mut *cell.get()).fill_bytes(buf) });
}

#[inline]
fn next_nonce_96() -> [u8; 12] {
    // 96-bit nonce = [stream_id:64 || counter:32 truncated]
    let stream: u64 = TL_STREAM_ID.with(|&id| id);
    let ctr: u64 = TL_NONCE_COUNTER.with(|cell| unsafe { let p = &mut *cell.get(); let c = *p; *p = c.wrapping_add(1); c });
    let mut out = [0u8; 12];
    out[0..8].copy_from_slice(&stream.to_le_bytes());
    out[8..12].copy_from_slice(&(ctr as u32).to_le_bytes());
    out
}

// =================================
// Batch Paillier utilities
// =================================

use rayon::prelude::*;

#[inline]
fn random_coprime_mod_n(n: &Integer) -> Integer {
    let n_len = to_be(n).len();
    let mut buf = vec![0u8; n_len * 2];
    rng_fill(&mut buf);
    let mut r = integer_from_be(&buf) % n;
    if r == 0 { r = Integer::from(1); }
    while r.gcd_ref(n) != 1 { r += 1; }
    r
}

struct GPrecomp { table: Vec<Integer> }

fn precompute_g_powers(g: &Integer, n2: &Integer) -> GPrecomp {
    let w = 4usize;
    let maxk = (1usize << w) - 1;
    let mut table = Vec::with_capacity(maxk);
    table.push(g.clone() % n2); // g^1
    for i in 2..=maxk {
        let next = (&table[i-2] * g) % n2;
        table.push(next);
    }
    GPrecomp { table }
}

fn pow_g_m(pre: &GPrecomp, n2: &Integer, m: u64) -> Integer {
    let w = 4usize;
    let base = 1u64 << w;
    let mut acc = Integer::from(1);
    let mut x = m;
    let mut started = false;
    while x > 0 {
        let chunk = (x & (base - 1)) as usize;
        x >>= w;
        if started {
            for _ in 0..w { acc = (&acc * &acc) % n2; }
        } else { started = true; }
        if chunk != 0 { acc = (acc * &pre.table[chunk - 1]) % n2; }
    }
    acc
}

fn paillier_encrypt_batch_u64(key: &PaillierKey, values: &[u64]) -> Vec<Integer> {
    let ctx = key.ctx.as_ref().expect("ctx");
    let pre = precompute_g_powers(&ctx.g, &ctx.n2);
    values.par_iter().map(|&m| {
        let gm = pow_g_m(&pre, &ctx.n2, m);
        let r = random_coprime_mod_n(&ctx.n);
        let rn = r.pow_mod(&ctx.n, &ctx.n2).unwrap();
        (gm * rn) % &ctx.n2
    }).collect()
}

// Free function for Paillier parameter generation so it can be used from key constructors
fn generate_paillier_params(prime_bits: usize) -> Result<(Integer, Integer, Integer), String> {
    // ChaCha20Rng seeded from OS for determinism in tests if desired
    let mut rng = ChaCha20Rng::from_rng(OsRng).map_err(|e| e.to_string())?;
    loop {
        let p = Integer::random_bits(prime_bits as u32, &mut rng) | 1; // odd
        let q = Integer::random_bits(prime_bits as u32, &mut rng) | 1; // odd
        let p = next_prime(p);
        let q = next_prime(q);
        if p != q && p.gcd_ref(&q) == 1 {
            let n = &p * &q;
            return Ok((n, p, q));
        }
    }

    // =============================
    // V5 fast path: batch modexp + aggregated proofs
    // =============================
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct EncryptedScalarV5 {
    pub c_paillier: Vec<u8>,
    pub com_point: [u8; 32],
    pub label16: [u8; 16],
    pub range_bits: u32,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct EncryptedBundleV5 {
    pub version: u8,
    pub key_id: [u8; 32],
    pub epoch: u64,
    pub timestamp: u64,
    pub bundle_id: [u8; 32],
    pub scalars: Vec<EncryptedScalarV5>,
    pub agg_proof: AggRangeProof,
    pub opaque: Option<OpaquePayload>,
    pub binder: [u8; 32],
}

impl HomomorphicBundleEncryptor {
    fn aead_encrypt_det(&self, aead: &ChaCha20Poly1305, aad: &[u8], pt: &[u8]) -> Result<OpaquePayload, String> {
        let nonce = next_nonce_96();
        let ct = aead.encrypt(Nonce::from_slice(&nonce), chacha20poly1305::aead::Payload { msg: pt, aad })
            .map_err(|_| "aead encrypt")?;
        Ok(OpaquePayload { nonce, ciphertext: ct })
    }

    pub fn encrypt_bundle_fast(
        &self,
        labels_and_values: &[(impl AsRef<[u8]>, u64)],
        opaque_tx_bytes: Option<&[u8]>,
        range_bits: usize,
    ) -> Result<EncryptedBundleV5, String> {
        if labels_and_values.is_empty() || labels_and_values.len() > MAX_SCALARS {
            return Err("invalid scalar set size".into());
        }
        self.operation_counter.fetch_add(1, Ordering::Relaxed);
        self.maybe_rotate();

        let key = self.get_current_key();
        let _ctx = key.ctx.as_ref().ok_or("missing ctx")?;
        let timestamp = now_unix();

        // Batch encrypt in parallel
        let values: Vec<u64> = labels_and_values.iter().map(|(_, v)| *v).collect();
        let c_vec = paillier_encrypt_batch_u64(&key, &values);
        let c_bytes_vec: Vec<Vec<u8>> = c_vec.iter().map(|c| to_be(c)).collect();

        // Derive blindings and commitments
        let blinds = derive_blindings(&key.key_id, &c_bytes_vec);
        let commits = commit_values(&values, &blinds);

        // Aggregated range proof
        let agg = prove_aggregated_range(&values, &blinds, range_bits, b"HBEnc/AGG_RANGE/v1");

        // Build scalars
        let scalars: Vec<EncryptedScalarV5> = labels_and_values.iter().enumerate().map(|(i, (label, &_val))| {
            EncryptedScalarV5 {
                c_paillier: c_bytes_vec[i].clone(),
                com_point: commits[i],
                label16: normalize_label16(label.as_ref()),
                range_bits: range_bits as u32,
            }
        }).collect();

        // AEAD opaque
        let opaque = if let Some(bytes) = opaque_tx_bytes {
            let key_aead = aead_key_from_keyid_epoch(&key.key_id, key.epoch);
            let aead = ChaCha20Poly1305::new(&key_aead);
            let aad = bundle_aad_static(VERSION, &key.key_id, key.epoch, timestamp, scalars.len());
            Some(self.aead_encrypt_det(&aead, &aad, bytes)?)
        } else { None };

        // Transcript binder
        let mut t = Transcript::new(b"HBEnc/BUNDLE/v5");
        t.append_message(b"version", &[VERSION]);
        t.append_message(b"key_id", &key.key_id);
        t.append_u64(b"epoch", key.epoch);
        t.append_u64(b"timestamp", timestamp);
        t.append_u64(b"count", scalars.len() as u64);
        for s in &scalars {
            t.append_message(b"item/label", &s.label16);
            t.append_message(b"item/c", &s.c_paillier);
            t.append_message(b"item/com", &s.com_point);
            t.append_u64(b"item/rbits", s.range_bits as u64);
        }
        t.append_message(b"agg_proof", &agg.proof);
        let mut bundle_id = [0u8; 32];
        let mut binder = [0u8; 32];
        t.challenge_bytes(b"bundle_id", &mut bundle_id);
        t.challenge_bytes(b"binder", &mut binder);

        Ok(EncryptedBundleV5 {
            version: VERSION,
            key_id: key.key_id,
            epoch: key.epoch,
            timestamp,
            bundle_id,
            scalars,
            agg_proof: agg,
            opaque,
            binder,
        })
    }

    pub fn verify_bundle_fast(&self, b: &EncryptedBundleV5) -> Result<(), String> {
        if b.version != VERSION { return Err("version".into()); }
        let _k = self.get_key_by_id(b.key_id)?; // presence check
        if !verify_aggregated_range(&b.agg_proof, b"HBEnc/AGG_RANGE/v1") { return Err("agg range proof".into()); }
        if b.agg_proof.commits.len() != b.scalars.len() { return Err("commit count mismatch".into()); }
        for (i, s) in b.scalars.iter().enumerate() {
            if s.com_point != b.agg_proof.commits[i] { return Err(format!("commit mismatch {}", i)); }
        }
        let mut t = Transcript::new(b"HBEnc/BUNDLE/v5");
        t.append_message(b"version", &[b.version]);
        t.append_message(b"key_id", &b.key_id);
        t.append_u64(b"epoch", b.epoch);
        t.append_u64(b"timestamp", b.timestamp);
        t.append_u64(b"count", b.scalars.len() as u64);
        for s in &b.scalars {
            t.append_message(b"item/label", &s.label16);
            t.append_message(b"item/c", &s.c_paillier);
            t.append_message(b"item/com", &s.com_point);
            t.append_u64(b"item/rbits", s.range_bits as u64);
        }
        t.append_message(b"agg_proof", &b.agg_proof.proof);
        let mut bundle_id = [0u8; 32];
        let mut binder = [0u8; 32];
        t.challenge_bytes(b"bundle_id", &mut bundle_id);
        t.challenge_bytes(b"binder", &mut binder);
        if !ct_eq(&bundle_id, &b.bundle_id) { return Err("bundle_id".into()); }
        if !ct_eq(&binder, &b.binder) { return Err("binder".into()); }
        Ok(())
    }

    pub fn decrypt_scalars_v5(&self, b: &EncryptedBundleV5) -> Result<Vec<u64>, String> {
        let key = self.get_key_by_id(b.key_id)?;
        let mut out = Vec::with_capacity(b.scalars.len());
        for (i, s) in b.scalars.iter().enumerate() {
            let c = integer_from_be(&s.c_paillier);
            let m = self.paillier_decrypt_c(&key, &c)?;
            let k = (s.range_bits as usize).min(64);
            let v = u64_from_integer_mod_2k(&m, k);
            out.push(v);
        }
        Ok(out)
    }

    // Homomorphic add with regenerated aggregate proof (requires decryption ability)
    pub fn homomorphic_add_fast(&self, a: &EncryptedBundleV5, b: &EncryptedBundleV5) -> Result<EncryptedBundleV5, String> {
        if a.version != b.version || a.key_id != b.key_id || a.epoch != b.epoch { return Err("bundle mismatch".into()); }
        let key = self.get_key_by_id(a.key_id)?;
        let ctx = key.ctx.as_ref().ok_or("ctx")?;
        let n2 = &ctx.n2;
        let len = a.scalars.len().min(b.scalars.len());

        let mut c_bytes: Vec<Vec<u8>> = Vec::with_capacity(len);
        let mut labels: Vec<[u8;16]> = Vec::with_capacity(len);
        let mut rbits: Vec<u32> = Vec::with_capacity(len);
        for i in 0..len {
            if a.scalars[i].label16 != b.scalars[i].label16 || a.scalars[i].range_bits != b.scalars[i].range_bits {
                return Err(format!("label/range mismatch {}", i));
            }
            let c1 = integer_from_be(&a.scalars[i].c_paillier);
            let c2 = integer_from_be(&b.scalars[i].c_paillier);
            let c = (c1 * c2) % n2;
            c_bytes.push(to_be(&c));
            labels.push(a.scalars[i].label16);
            rbits.push(a.scalars[i].range_bits);
        }

        // Requires decrypt capability to re-prove
        let vals_a = self.decrypt_scalars_v5(a)?;
        let vals_b = self.decrypt_scalars_v5(b)?;
        let values: Vec<u64> = (0..len).map(|i| vals_a[i].wrapping_add(vals_b[i])).collect();

        let blinds = derive_blindings(&a.key_id, &c_bytes);
        let commits = commit_values(&values, &blinds);
        let bits = a.agg_range.bits as usize;
        let agg = prove_aggregated_range(&values, &blinds, bits, b"HBEnc/AGG_RANGE/v1");

        let scalars: Vec<EncryptedScalarV5> = (0..len).map(|i| EncryptedScalarV5 {
            c_paillier: c_bytes[i].clone(),
            com_point: commits[i],
            label16: labels[i],
            range_bits: rbits[i],
        }).collect();

        let timestamp = now_unix();
        let mut t = Transcript::new(b"HBEnc/BUNDLE/v5");
        t.append_message(b"version", &[a.version]);
        t.append_message(b"key_id", &a.key_id);
        t.append_u64(b"epoch", a.epoch);
        t.append_u64(b"timestamp", timestamp);
        t.append_u64(b"count", scalars.len() as u64);
        for s in &scalars {
            t.append_message(b"item/label", &s.label16);
            t.append_message(b"item/c", &s.c_paillier);
            t.append_message(b"item/com", &s.com_point);
            t.append_u64(b"item/rbits", s.range_bits as u64);
        }
        t.append_message(b"agg_proof", &agg.proof);
        let mut bundle_id = [0u8; 32];
        let mut binder = [0u8; 32];
        t.challenge_bytes(b"bundle_id", &mut bundle_id);
        t.challenge_bytes(b"binder", &mut binder);

        Ok(EncryptedBundleV5 {
            version: a.version,
            key_id: a.key_id,
            epoch: a.epoch,
            timestamp,
            bundle_id,
            scalars,
            agg_proof: agg,
            opaque: None,
            binder,
        })
    }

    // Homomorphic scale with regenerated aggregate proof (requires decryption ability)
    pub fn homomorphic_scale_fast(&self, a: &EncryptedBundleV5, k: u64) -> Result<EncryptedBundleV5, String> {
        if k == 0 { return Err("scale by zero".into()); }
        let key = self.get_key_by_id(a.key_id)?;
        let ctx = key.ctx.as_ref().ok_or("ctx")?;
        let n2 = &ctx.n2;
        let len = a.scalars.len();

        let mut c_bytes: Vec<Vec<u8>> = Vec::with_capacity(len);
        let mut labels: Vec<[u8;16]> = Vec::with_capacity(len);
        let mut rbits: Vec<u32> = Vec::with_capacity(len);
        for i in 0..len {
            let c = integer_from_be(&a.scalars[i].c_paillier);
            let c_scaled = c.pow_mod(&Integer::from(k), n2).unwrap();
            c_bytes.push(to_be(&c_scaled));
            labels.push(a.scalars[i].label16);
            rbits.push(a.scalars[i].range_bits);
        }

        let vals = self.decrypt_scalars_v5(a)?;
        let values: Vec<u64> = vals.into_iter().map(|v| v.saturating_mul(k)).collect();

        let blinds = derive_blindings(&a.key_id, &c_bytes);
        let commits = commit_values(&values, &blinds);
        let bits = a.agg_range.bits as usize;
        let agg = prove_aggregated_range(&values, &blinds, bits, b"HBEnc/AGG_RANGE/v1");

        let scalars: Vec<EncryptedScalarV5> = (0..len).map(|i| EncryptedScalarV5 {
            c_paillier: c_bytes[i].clone(),
            com_point: commits[i],
            label16: labels[i],
            range_bits: rbits[i],
        }).collect();

        let timestamp = now_unix();
        let mut t = Transcript::new(b"HBEnc/BUNDLE/v5");
        t.append_message(b"version", &[a.version]);
        t.append_message(b"key_id", &a.key_id);
        t.append_u64(b"epoch", a.epoch);
        t.append_u64(b"timestamp", timestamp);
        t.append_u64(b"count", scalars.len() as u64);
        for s in &scalars {
            t.append_message(b"item/label", &s.label16);
            t.append_message(b"item/c", &s.c_paillier);
            t.append_message(b"item/com", &s.com_point);
            t.append_u64(b"item/rbits", s.range_bits as u64);
        }
        t.append_message(b"agg_proof", &agg.proof);
        let mut bundle_id = [0u8; 32];
        let mut binder = [0u8; 32];
        t.challenge_bytes(b"bundle_id", &mut bundle_id);
        t.challenge_bytes(b"binder", &mut binder);

        Ok(EncryptedBundleV5 {
            version: a.version,
            key_id: a.key_id,
            epoch: a.epoch,
            timestamp,
            bundle_id,
            scalars,
            agg_proof: agg,
            opaque: None,
            binder,
        })
    }
}

impl PaillierKey {
    fn placeholder() -> Self {
        Self {
            n: Integer::from(1),
            n2: Integer::from(1),
            g: Integer::from(2),
            lambda: None,
            mu: None,
            created_at: 0,
            epoch: 0,
            key_id: [0u8; 32],
            ctx: None,
        }
    }
    pub fn with_ctx(mut self, ctx: PaillierCtx) -> Self {
        self.ctx = Some(ctx);
        self
    }
    pub fn generate_with_ctx(bits: usize) -> Result<(Self, PaillierCtx), String> {
        let (n, p, q) = generate_paillier_params(bits / 2)?;
        let n2 = &n * &n;
        let g = &n + 1;
        let p1 = &p - 1;
        let q1 = &q - 1;
        let lambda = lcm(&p1, &q1);
        let u = g.clone().pow_mod(&lambda, &n2).unwrap();
        let l = l_function(&u, &n);
        let mu = modinv(&l, &n).ok_or("no inverse for mu")?;
        let key_id = keccak32(&to_be(&n));
        static NEXT_EPOCH: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let epoch = NEXT_EPOCH.fetch_add(1, Ordering::SeqCst);
        let created_at = now_unix();
        let ctx = PaillierCtx { n: n.clone(), n2: n2.clone(), g: g.clone() };
        Ok((Self { n, n2, g, lambda: Some(lambda), mu: Some(mu), created_at, epoch, key_id, ctx: None }, ctx))
    }
}

// =============================
// Paillier key and operations
// =============================

#[derive(Clone)]
pub struct PaillierCtx {
    pub n: Integer,
    pub n2: Integer,
    pub g: Integer,
}

#[derive(Clone)]
struct PaillierKey {
    n: Integer,          // modulus
    n2: Integer,         // n^2
    g: Integer,          // n + 1
    // Secret for decryption (only on encryptor/decryptor side)
    lambda: Option<Integer>,
    mu: Option<Integer>,

    created_at: u64,     // unix seconds
    epoch: u64,          // rotation epoch
    key_id: [u8; 32],    // H(n)
    // Optional reusable context for hot path modexp
    ctx: Option<PaillierCtx>,
}

impl Drop for PaillierKey {
    fn drop(&mut self) {
        if let Some(l) = &mut self.lambda { l.zeroize(); }
        if let Some(m) = &mut self.mu { m.zeroize(); }
    }
}

fn keccak32(bytes: &[u8]) -> [u8; 32] {
    let mut h = Keccak256::new();
    h.update(bytes);
    let out = h.finalize();
    let mut id = [0u8; 32];
    id.copy_from_slice(&out);
    id
}

fn sha3_32(bytes: &[u8]) -> [u8; 32] {
    let mut h = Sha3_256::new();
    h.update(bytes);
    let out = h.finalize();
    let mut id = [0u8; 32];
    id.copy_from_slice(&out);
    id
}

fn integer_from_be(bytes: &[u8]) -> Integer {
    Integer::from_digits(bytes, Order::MsfBe)
}

fn to_be(integer: &Integer) -> Vec<u8> {
    integer.to_digits(Order::MsfBe)
}

fn l_function(u: &Integer, n: &Integer) -> Integer {
    (u - 1) / n
}

fn modinv(a: &Integer, m: &Integer) -> Option<Integer> {
    // Extended gcd for modular inverse
    let (g, x, _) = a.clone().extended_gcd_ref(m);
    if g == 1 {
        let mut inv = x % m;
        if inv < 0 { inv += m; }
        Some(inv)
    } else {
        None
    }
}

// =============================
// Pedersen commitments and BP
// =============================

static PEDERSEN_GENS: once_cell::sync::Lazy<PedersenGens> = once_cell::sync::Lazy::new(PedersenGens::default);
static BULLET_GENS: once_cell::sync::Lazy<BulletproofGens> =
    once_cell::sync::Lazy::new(|| BulletproofGens::new(64, 64));

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct PedersenCommitment {
    // Compressed Ristretto point
    pub point: [u8; 32],
}

fn commit_pedersen(value_u64: u64, blinding: &Scalar) -> PedersenCommitment {
    let v = Scalar::from(value_u64);
    let C: RistrettoPoint = PEDERSEN_GENS.commit(v, *blinding);
    PedersenCommitment { point: C.compress().to_bytes() }
}

fn add_commitments(a: &PedersenCommitment, b: &PedersenCommitment) -> PedersenCommitment {
    let pa = curve25519_dalek::ristretto::CompressedRistretto(a.point).decompress().expect("valid point A");
    let pb = curve25519_dalek::ristretto::CompressedRistretto(b.point).decompress().expect("valid point B");
    let sum = pa + pb;
    PedersenCommitment { point: sum.compress().to_bytes() }
}

fn scale_commitment(c: &PedersenCommitment, k: u64) -> PedersenCommitment {
    let p = curve25519_dalek::ristretto::CompressedRistretto(c.point).decompress().expect("valid point");
    let ks = Scalar::from(k);
    let scaled = p * ks;
    PedersenCommitment { point: scaled.compress().to_bytes() }
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct AggRangeProof {
    pub proof: Vec<u8>,
    pub commits: Vec<[u8; 32]>,
    pub bits: u32,
}

fn derive_blindings(key_id: &[u8; 32], c_bytes_vec: &[Vec<u8>]) -> Vec<Scalar> {
    use rayon::prelude::*;
    c_bytes_vec.par_iter().enumerate().map(|(i, c)| {
        let mut h = Sha3_256::new();
        h.update(b"HBEnc/BLIND/v2");
        h.update(key_id);
        h.update(c);
        h.update(&(i as u64).to_le_bytes());
        let digest = h.finalize();
        let mut wide = [0u8; 64];
        wide[..32].copy_from_slice(&digest);
        Scalar::from_bytes_mod_order_wide(&wide)
    }).collect()
}

fn commit_values(values: &[u64], blindings: &[Scalar]) -> Vec<[u8; 32]> {
    use rayon::prelude::*;
    values.par_iter().zip(blindings.par_iter()).map(|(&v, r)| {
        let C: RistrettoPoint = PEDERSEN_GENS.commit(Scalar::from(v), *r);
        C.compress().to_bytes()
    }).collect()
}

fn prove_aggregated_range(values: &[u64], blindings: &[Scalar], bits: usize, label: &'static [u8]) -> AggRangeProof {
    let commits = commit_values(values, blindings);
    let mut t = Transcript::new(label);
    t.append_u64(b"values_len", values.len() as u64);
    let (proof, _) = RangeProof::prove_multiple(
        &*BULLET_GENS,
        &*PEDERSEN_GENS,
        &mut t,
        values,
        blindings,
        bits,
    ).expect("agg range proof");
    AggRangeProof { proof: proof.to_bytes(), commits, bits: bits as u32 }
}

fn verify_aggregated_range(agg: &AggRangeProof, label: &'static [u8]) -> bool {
    let mut t = Transcript::new(label);
    t.append_u64(b"values_len", agg.commits.len() as u64);
    let Ok(proof) = RangeProof::from_bytes(&agg.proof) else { return false; };
    let commits: Vec<_> = agg.commits.iter().map(|c| curve25519_dalek::ristretto::CompressedRistretto(*c)).collect();
    proof.verify_multiple(&*BULLET_GENS, &*PEDERSEN_GENS, &mut t, &commits, agg.bits as usize).is_ok()
}

// =============================
// Homomorphic scalar item
// =============================

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct EncryptedScalar {
    // Paillier ciphertext (c = g^m * r^n mod n^2) big-endian bytes
    pub c_paillier: Vec<u8>,
    // Pedersen commitment to the same value m (mod 2^range_bits)
    pub com: PedersenCommitment,
    // Optional label for semantics e.g. "tip_wei"
    pub label16: [u8; 16],
    // Range bits used
    pub range_bits: u32,
}

fn scalar_from_biguint_mod_l(m: &Integer) -> Scalar {
    // Reduce arbitrary integer modulo curve order l
    // curve25519_dalek Scalar expects 32 bytes little-endian; we reduce then encode
    use curve25519_dalek::constants::l as CURVE_L;
    // Convert m to 32 bytes LE after mod l
    let mut tmp = m.clone() % Integer::from_digits(&CURVE_L.to_bytes(), Order::LsfLe);
    if tmp < 0 { tmp += Integer::from_digits(&CURVE_L.to_bytes(), Order::LsfLe); }
    // Export as 32 LE
    let mut le = tmp.to_digits(Order::LsfLe);
    le.resize(32, 0);
    Scalar::from_bits(le.try_into().expect("32 bytes"))
}

// =============================
// Bundle structures (versioned)
// =============================

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct OpaquePayload {
    pub nonce: [u8; AEAD_NONCE_SIZE],
    pub ciphertext: Vec<u8>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct EncryptedBundle {
    pub version: u8,            // = 4
    pub key_id: [u8; 32],       // H(n)
    pub epoch: u64,             // key epoch
    pub timestamp: u64,         // unix seconds
    pub bundle_id: [u8; 32],    // transcript-bound id
    pub scalars: Vec<EncryptedScalar>,
    pub opaque: Option<OpaquePayload>, // AEAD of opaque tx bytes (if any)
    // Aggregate transcript binding proof (hash-based binder)
    pub binder: [u8; 32],
    // Aggregated bulletproofs for all scalars
    pub agg_range: Option<AggRangeProof>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct EncryptorStats {
    pub total_operations: usize,
    pub current_epoch: u64,
    pub cached_keys: usize,
    pub key_rotation_interval: u64,
}

// =============================
// Encryptor
// =============================

pub struct HomomorphicBundleEncryptor {
    keys: Arc<DashMap<[u8; 32], Arc<PaillierKey>>>,
    current_key: Arc<ArcSwap<PaillierKey>>,
    epoch_counter: Arc<AtomicU64>,
    operation_counter: Arc<AtomicUsize>,
    rotation_interval: Duration,
    // Monotonic nonce counter (per-process) for AEAD payloads to avoid RNG on hot path
    aead_nonce_counter: Arc<AtomicU64>,
}

impl Default for HomomorphicBundleEncryptor {
    fn default() -> Self { Self::new() }
}

impl HomomorphicBundleEncryptor {
    pub fn new() -> Self {
        let s = Self {
            keys: Arc::new(DashMap::new()),
            current_key: Arc::new(ArcSwap::from_pointee(PaillierKey::placeholder())),
            epoch_counter: Arc::new(AtomicU64::new(0)),
            operation_counter: Arc::new(AtomicUsize::new(0)),
            rotation_interval: Duration::from_secs(KEY_ROTATION_SECS),
            aead_nonce_counter: Arc::new(AtomicU64::new(0)),
        };
        s.rotate_keys().expect("initial keygen");
        s
    }

    // ----- Key management -----

    pub fn rotate_keys(&self) -> Result<(), String> {
        let (key, ctx) = PaillierKey::generate_with_ctx(PAILLIER_BITS)?;
        let key_id = key.key_id;
        let key_arc = Arc::new(key.with_ctx(ctx));
        self.keys.insert(key_id, key_arc.clone());
        self.current_key.store(key_arc);
        let _ = self.epoch_counter.fetch_add(1, Ordering::SeqCst);
        self.cleanup_old_keys();
        Ok(())
    }

    pub fn get_public_key(&self) -> Result<Vec<u8>, String> { Ok(to_be(&self.get_current_key().n)) }

    fn get_current_key(&self) -> Arc<PaillierKey> { self.current_key.load_full() }

    fn get_key_by_id(&self, key_id: [u8; 32]) -> Result<Arc<PaillierKey>, String> {
        self.keys.get(&key_id).map(|e| e.clone()).ok_or("unknown key_id".into())
    }

    fn maybe_rotate(&self) {
        let k = self.current_key.load();
        let need = now_unix().saturating_sub(k.created_at) > KEY_ROTATION_SECS || k.created_at == 0;
        if need { let _ = self.rotate_keys(); }
    }

    /// Spawn background rotation that checks halfway through the rotation interval.
    /// Requires a Tokio runtime.
    pub fn spawn_background_rotation(self: &Arc<Self>) {
        let me = Arc::clone(self);
        tokio::spawn(async move {
            let sleep_dur = Duration::from_secs(KEY_ROTATION_SECS / 2);
            loop {
                tokio::time::sleep(sleep_dur).await;
                me.maybe_rotate();
            }
        });
    }

    fn cleanup_old_keys(&self) {
        if self.keys.len() > MAX_CACHED_KEYS {
            // Retain only the most recent epochs per time window
            let cutoff = now_unix().saturating_sub(KEY_ROTATION_SECS * 3);
            self.keys.retain(|_, v| v.created_at >= cutoff);
        }
    }

    // ----- Paillier primitives -----

    fn generate_paillier_params(&self, prime_bits: usize) -> Result<(Integer, Integer, Integer), String> {
        // ChaCha20Rng seeded from OS for determinism in tests if desired
        let mut rng = ChaCha20Rng::from_rng(OsRng).map_err(|e| e.to_string())?;

        loop {
            let p = Integer::random_bits(prime_bits as u32, &mut rng) | 1; // odd
            let q = Integer::random_bits(prime_bits as u32, &mut rng) | 1; // odd

            let p = next_prime(p);
            let q = next_prime(q);

            if p != q && p.gcd_ref(&q) == 1 {
                let n = &p * &q;
                return Ok((n, p, q));
            }
        }
    }

    fn paillier_encrypt_m(&self, key: &PaillierKey, m: &Integer, rng: &mut ChaCha20Rng) -> Result<Integer, String> {
        if m >= &key.n || m < 0 { return Err("plaintext out of domain".into()); }

        // pick r in [1, n-1] with gcd(r, n) = 1
        let r = loop {
            let candidate = random_in_mod_n(&key.n, rng);
            if candidate.gcd_ref(&key.n) == 1 { break candidate; }
        };

        // c = g^m * r^n mod n^2
        let gm = key.g.clone().pow_mod(m, &key.n2).unwrap();
        let rn = r.pow_mod(&key.n, &key.n2).unwrap();
        Ok((gm * rn) % &key.n2)
    }

    fn paillier_decrypt_c(&self, key: &PaillierKey, c: &Integer) -> Result<Integer, String> {
        let lambda = key.lambda.as_ref().ok_or("missing lambda")?;
        let mu = key.mu.as_ref().ok_or("missing mu")?;
        let u = c.clone().pow_mod(lambda, &key.n2).unwrap();
        let l = l_function(&u, &key.n);
        let m = (l * mu) % &key.n;
        Ok(m)
    }

    // ----- AEAD -----

    fn aead_encrypt(&self, aead_key: &Key, aad: &[u8], plaintext: &[u8], epoch: u64) -> Result<OpaquePayload, String> {
        if plaintext.len() > MAX_OPAQUE_BYTES { return Err("opaque payload too large".into()); }
        let cipher = ChaCha20Poly1305::new(aead_key);
        // 96-bit per-thread deterministic nonce (stream_id || counter)
        let nonce = next_nonce_96();
        let tag = cipher.encrypt(Nonce::from_slice(&nonce), chacha20poly1305::aead::Payload { msg: plaintext, aad })
            .map_err(|_| "AEAD encrypt failed")?;
        Ok(OpaquePayload { nonce, ciphertext: tag })
    }

    fn aead_decrypt(&self, aead_key: &Key, aad: &[u8], payload: &OpaquePayload) -> Result<Vec<u8>, String> {
        let cipher = ChaCha20Poly1305::new(aead_key);
        cipher.decrypt(Nonce::from_slice(&payload.nonce), chacha20poly1305::aead::Payload {
            msg: &payload.ciphertext,
            aad,
        }).map_err(|_| "AEAD decrypt failed")
    }

    // ----- Bundle API -----

    pub fn encrypt_bundle(
        &self,
        labels_and_values: &[(impl AsRef<[u8]>, u64)], // homomorphic scalars
        opaque_tx_bytes: Option<&[u8]>,                // AEAD-protected opaque payload
        range_bits: Option<usize>,
    ) -> Result<EncryptedBundle, String> {
        if labels_and_values.is_empty() || labels_and_values.len() > MAX_SCALARS {
            return Err("invalid scalar set size".into());
        }

        self.operation_counter.fetch_add(1, Ordering::Relaxed);
        self.maybe_rotate();

        let key = self.get_current_key();
        let timestamp = now_unix();
        let range_bits = range_bits.unwrap_or(RANGE_BITS_DEFAULT);

        // Transcript to derive bundle_id and bind all fields
        let mut t = Transcript::new(b"HBEnc/BUNDLE/v4");
        t.append_message(b"version", &[VERSION]);
        t.append_message(b"key_id", &key.key_id);
        t.append_u64(b"epoch", key.epoch);
        t.append_u64(b"timestamp", timestamp);
        t.append_u64(b"count", labels_and_values.len() as u64);

        // Encrypt scalars and generate commitments + aggregated range proof
        let mut scalars = Vec::with_capacity(labels_and_values.len());

        // Batch and parallel Paillier encryption using windowed g^m precomputation
        let values_u64: Vec<u64> = labels_and_values.iter().map(|(_, &v)| v).collect();
        let pre_c_ints = paillier_encrypt_batch_u64(&key, &values_u64);
        let pre_c: Vec<Vec<u8>> = pre_c_ints.into_iter().map(|c| to_be(&c)).collect();

        for (i, (label, &val)) in labels_and_values.iter().enumerate() {
            if (val as u128) >= (1u128 << range_bits.min(64) as u32) {
                return Err("value exceeds declared range".into());
            }
            let label16 = normalize_label16(label.as_ref());

            // Use precomputed Paillier ciphertext bytes
            let c_bytes = pre_c[i].clone();

            t.append_message(b"item/label", &label16);
            t.append_message(b"item/c", &c_bytes);
            // com and proof added after we compute blindings collectively
            scalars.push(EncryptedScalar {
                c_paillier: c_bytes,
                com: PedersenCommitment { point: [0u8; 32] },
                label16,
                range_bits: range_bits as u32,
            });
        }

        // Compute blindings and commitments collectively, then aggregate proof
        let blindings = derive_blindings(&key.key_id, &pre_c);
        let values_u64: Vec<u64> = labels_and_values.iter().map(|(_, &v)| v).collect();
        let commits = commit_values(&values_u64, &blindings);
        for (i, c) in commits.iter().enumerate() {
            scalars[i].com.point = *c;
        }
        let agg = prove_aggregated_range(&values_u64, &blindings, range_bits, b"HBEnc/AGG_RANGE/v1");

        // AEAD for opaque payload (optional)
        let (opaque, opaque_hash_opt) = if let Some(bytes) = opaque_tx_bytes {
            let aead_key = aead_key_from_keyid_epoch(&key.key_id, key.epoch);
            let aad = bundle_aad_static(VERSION, &key.key_id, key.epoch, timestamp, labels_and_values.len());
            let payload = self.aead_encrypt(&aead_key, &aad, bytes, key.epoch)?;
            // Bind encrypted bytes to transcript by hashing (nonce || ciphertext)
            let mut hasher = blake3::Hasher::new();
            hasher.update(&payload.nonce);
            hasher.update(&payload.ciphertext);
            let mut h = [0u8;32]; h.copy_from_slice(hasher.finalize().as_bytes());
            (Some(payload), Some(h))
        } else { (None, None) };

        // Derive bundle_id and binder from transcript
        // Include opaque hash in transcript if present
        if let Some(h) = opaque_hash_opt { t.append_message(b"opaque_hash", &h); }

        let mut id_bytes = [0u8; 32];
        t.challenge_bytes(b"bundle_id", &mut id_bytes);
        let mut binder = [0u8; 32];
        t.challenge_bytes(b"binder", &mut binder);

        Ok(EncryptedBundle {
            version: VERSION,
            key_id: key.key_id,
            epoch: key.epoch,
            timestamp,
            bundle_id: id_bytes,
            scalars,
            opaque,
            binder,
            agg_range: Some(agg),
        })
    }

    pub fn verify_bundle(&self, bundle: &EncryptedBundle) -> Result<(), String> {
        if bundle.version != VERSION { return Err("version mismatch".into()); }
        if bundle.scalars.is_empty() || bundle.scalars.len() > MAX_SCALARS { return Err("invalid count".into()); }

        let _key = self.get_key_by_id(bundle.key_id)?; // ensure key is known (or accept future keys via discovery)

        // Rebuild transcript and check binder deterministically
        let mut t = Transcript::new(b"HBEnc/BUNDLE/v4");
        t.append_message(b"version", &[bundle.version]);
        t.append_message(b"key_id", &bundle.key_id);
        t.append_u64(b"epoch", bundle.epoch);
        t.append_u64(b"timestamp", bundle.timestamp);
        t.append_u64(b"count", bundle.scalars.len() as u64);

        for (_i, item) in bundle.scalars.iter().enumerate() {
            if item.range_bits == 0 || item.range_bits > 64 { return Err("unsupported range bits".into()); }
            t.append_message(b"item/label", &item.label16);
            t.append_message(b"item/c", &item.c_paillier);
            t.append_message(b"item/com", &item.com.point);
        }

        // Verify aggregated range proof if present
        if let Some(agg) = &bundle.agg_range {
            if !verify_aggregated_range(agg, b"HBEnc/AGG_RANGE/v1") { return Err("aggregated range proof failed".into()); }
            // Ensure commits match the ones in scalars
            if agg.commits.len() != bundle.scalars.len() { return Err("agg commits length mismatch".into()); }
            for (i, c) in agg.commits.iter().enumerate() {
                if !ct_eq32(c, &bundle.scalars[i].com.point) { return Err("agg commit mismatch".into()); }
            }
        }

        // If opaque payload exists, sanity-check AAD decrypt feasibility (cannot decrypt without key)
        if let Some(opaque) = &bundle.opaque {
            if opaque.nonce.len() != AEAD_NONCE_SIZE { return Err("invalid opaque nonce size".into()); }
            // Optional: try to decrypt if we have the key
        }

        let mut id_bytes = [0u8; 32];
        t.challenge_bytes(b"bundle_id", &mut id_bytes);
        let mut binder = [0u8; 32];
        t.challenge_bytes(b"binder", &mut binder);

        if !ct_eq(&id_bytes, &bundle.bundle_id) { return Err("bundle_id mismatch".into()); }
        if !ct_eq(&binder, &bundle.binder) { return Err("binder mismatch".into()); }

        Ok(())
    }

    // Decrypt only the scalar integers (caller enforces semantics)
    pub fn decrypt_scalars(&self, bundle: &EncryptedBundle) -> Result<Vec<u64>, String> {
        self.verify_bundle(bundle)?;
        let key = self.get_key_by_id(bundle.key_id)?;
        let mut out = Vec::with_capacity(bundle.scalars.len());
        for (i, item) in bundle.scalars.iter().enumerate() {
            let c = integer_from_be(&item.c_paillier);
            let m = self.paillier_decrypt_c(&key, &c)?;
            // Restrict to u64 for declared range
            let val = m.to_u64_wrapping();
            if (val as u128) >= (1u128 << (item.range_bits.min(64) as u32)) {
                return Err(format!("decrypted value out of declared range at index {}", i));
            }
            out.push(val);
        }
        Ok(out)
    }

    // Decrypt opaque payload (if present)
    pub fn decrypt_opaque(&self, bundle: &EncryptedBundle) -> Result<Option<Vec<u8>>, String> {
        self.verify_bundle(bundle)?;
        let Some(opaque) = &bundle.opaque else { return Ok(None); };
        let aead_key = aead_key_from_keyid_epoch(&bundle.key_id, bundle.epoch);
        let aad = bundle_aad_static(bundle.version, &bundle.key_id, bundle.epoch, bundle.timestamp, bundle.scalars.len());
        let pt = self.aead_decrypt(&aead_key, &aad, opaque)?;
        Ok(Some(pt))
    }

    // Homomorphic add: elementwise add up to min length
    pub fn homomorphic_add(&self, a: &EncryptedBundle, b: &EncryptedBundle) -> Result<EncryptedBundle, String> {
        // Must be same version, key_id, epoch
        if a.version != VERSION || b.version != VERSION { return Err("version mismatch".into()); }
        if a.key_id != b.key_id { return Err("key_id mismatch".into()); }
        if a.epoch != b.epoch { return Err("epoch mismatch".into()); }

        // Rebuild new transcript
        let count = a.scalars.len().min(b.scalars.len());
        if count == 0 { return Err("empty inputs".into()); }

        let mut t = Transcript::new(b"HBEnc/BUNDLE/v4");
        t.append_message(b"version", &[VERSION]);
        t.append_message(b"key_id", &a.key_id);
        t.append_u64(b"epoch", a.epoch);
        let timestamp = now_unix();
        t.append_u64(b"timestamp", timestamp);
        t.append_u64(b"count", count as u64);

        let mut combined = Vec::with_capacity(count);
        for i in 0..count {
            let ia = &a.scalars[i];
            let ib = &b.scalars[i];
            if ia.label16 != ib.label16 || ia.range_bits != ib.range_bits {
                return Err(format!("label/range mismatch at {}", i));
            }

            // Paillier add: c = c1 * c2 mod n^2
            let key = self.get_key_by_id(a.key_id)?;
            let c1 = integer_from_be(&ia.c_paillier);
            let c2 = integer_from_be(&ib.c_paillier);
            let c_comb = (c1 * c2) % &key.n2;
            let c_bytes = to_be(&c_comb);

            // Pedersen add: C = C1 + C2
            let com = add_commitments(&ia.com, &ib.com);

            // Range proof remains valid for m1+m2 only if we regenerate a proof for the sum.
            // Since we don't know plaintexts here, we can't regenerate; instead, we carry a derived binder that binds c and com together.
            // Consumers should verify algebraic link by Paillier then decrypt to check actual ranges as needed,
            // or require the party performing homomorphic add to also provide a fresh proof for the sum (not possible here without secrets).
            // We therefore mark the range proof as "combined" proof: we set it empty and require downstream to reprove after decrypt if needed.
            let empty_proof = RangeProofBytes { proof: Vec::new() };

            t.append_message(b"item/label", &ia.label16);
            t.append_message(b"item/c", &c_bytes);
            t.append_message(b"item/com", &com.point);
            t.append_message(b"item/proof", &empty_proof.proof);

            combined.push(EncryptedScalar {
                c_paillier: c_bytes,
                com,
                range_proof: empty_proof,
                label16: ia.label16,
                range_bits: ia.range_bits,
            });
        }

        // Opaque payload cannot be homomorphically added; drop by default
        let opaque = None;

        let mut bundle_id = [0u8; 32];
        t.challenge_bytes(b"bundle_id", &mut bundle_id);
        let mut binder = [0u8; 32];
        t.challenge_bytes(b"binder", &mut binder);

        Ok(EncryptedBundle {
            version: VERSION,
            key_id: a.key_id,
            epoch: a.epoch,
            timestamp,
            bundle_id,
            scalars: combined,
            opaque,
            binder,
        })
    }

    // Scalar multiply by k: c' = c^k mod n^2, com' = k * com
    pub fn homomorphic_scale(&self, a: &EncryptedBundle, k: u64) -> Result<EncryptedBundle, String> {
        if k == 0 { return Err("scale by zero".into()); }

        let mut t = Transcript::new(b"HBEnc/BUNDLE/v4");
        t.append_message(b"version", &[VERSION]);
        t.append_message(b"key_id", &a.key_id);
        t.append_u64(b"epoch", a.epoch);
        let timestamp = now_unix();
        t.append_u64(b"timestamp", timestamp);
        t.append_u64(b"count", a.scalars.len() as u64);

        let key = self.get_key_by_id(a.key_id)?;
        let mut out = Vec::with_capacity(a.scalars.len());
        for (i, item) in a.scalars.iter().enumerate() {
            let c = integer_from_be(&item.c_paillier);
            let c_scaled = c.pow_mod(&Integer::from(k), &key.n2).unwrap();
            let c_bytes = to_be(&c_scaled);

            let com = scale_commitment(&item.com, k);

            // Range proof must be regenerated after scaling; we set empty and require reproof downstream.
            let empty_proof = RangeProofBytes { proof: Vec::new() };

            t.append_message(b"item/label", &item.label16);
            t.append_message(b"item/c", &c_bytes);
            t.append_message(b"item/com", &com.point);
            t.append_message(b"item/proof", &empty_proof.proof);

            out.push(EncryptedScalar {
                c_paillier: c_bytes,
                com,
                range_proof: empty_proof,
                label16: item.label16,
                range_bits: item.range_bits,
            });
        }

        let mut bundle_id = [0u8; 32];
        t.challenge_bytes(b"bundle_id", &mut bundle_id);
        let mut binder = [0u8; 32];
        t.challenge_bytes(b"binder", &mut binder);

        Ok(EncryptedBundle {
            version: VERSION,
            key_id: a.key_id,
            epoch: a.epoch,
            timestamp,
            bundle_id,
            scalars: out,
            opaque: None,
            binder,
        })
    }

    pub fn get_stats(&self) -> EncryptorStats {
        EncryptorStats {
            total_operations: self.operation_counter.load(Ordering::Relaxed),
            current_epoch: self.epoch_counter.load(Ordering::Relaxed),
            cached_keys: self.keys.len(),
            key_rotation_interval: self.rotation_interval.as_secs(),
        }
    }
}

// =============================
// Helpers
// =============================

fn now_unix() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn lcm(a: &Integer, b: &Integer) -> Integer {
    (a * b) / a.gcd_ref(b)
}

fn random_in_mod_n(n: &Integer, rng: &mut ChaCha20Rng) -> Integer {
    // Sample uniformly in [1, n-1]
    let bytes = to_be(n);
    let mut out = vec![0u8; bytes.len()];
    rng.fill_bytes(&mut out);
    // Ensure < n
    let mut r = integer_from_be(&out) % n;
    if r == 0 { r = Integer::from(1); }
    r
}

fn next_prime(mut x: Integer) -> Integer {
    if x.is_probably_prime(25) != rug::integer::IsPrime::No { return x; }
    loop {
        x += 2;
        if x.is_probably_prime(25) != rug::integer::IsPrime::No { return x; }
    }
}

// Derive Pedersen blinding scalar bound to ciphertext and key_id
fn derive_blinding(key_id: &[u8; 32], c_bytes: &[u8], idx: u64) -> Scalar {
    let mut h = Sha3_256::new();
    h.update(b"HBEnc/BLIND/v1");
    h.update(key_id);
    h.update(c_bytes);
    h.update(&idx.to_le_bytes());
    let digest = h.finalize();
    // Map to Scalar via reduction
    let mut wide = [0u8; 64];
    wide[..32].copy_from_slice(&digest);
    Scalar::from_bytes_mod_order_wide(&wide)
}

// Bundle AAD for AEAD
fn bundle_aad_static(version: u8, key_id: &[u8; 32], epoch: u64, ts: u64, count: usize) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + 32 + 8 + 8 + 8);
    v.push(version);
    v.extend_from_slice(key_id);
    v.extend_from_slice(&epoch.to_le_bytes());
    v.extend_from_slice(&ts.to_le_bytes());
    v.extend_from_slice(&(count as u64).to_le_bytes());
    v
}

fn aead_key_from_keyid_epoch(key_id: &[u8; 32], epoch: u64) -> Key {
    let mut h = blake3::Hasher::new();
    h.update(b"HBEnc/AEAD/v1");
    h.update(key_id);
    h.update(&epoch.to_le_bytes());
    let out = h.finalize();
    let mut key_bytes = [0u8; 32];
    key_bytes.copy_from_slice(out.as_bytes());
    Key::from_slice(&key_bytes).to_owned()
}

// Normalize any label into 16 bytes (right-padded with zeros, truncated if longer)
fn normalize_label16(label: &[u8]) -> [u8; 16] {
    let mut out = [0u8; 16];
    let copy_len = label.len().min(16);
    out[..copy_len].copy_from_slice(&label[..copy_len]);
    out
}
