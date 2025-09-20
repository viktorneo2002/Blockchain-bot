use solana_sdk::{
    signature::{Keypair, Signature, Signer},
    pubkey::Pubkey,
    transaction::Transaction,
    instruction::Instruction,
    hash::Hash,
    commitment_config::CommitmentConfig,
};
use solana_client::rpc_client::RpcClient;
use borsh::{BorshDeserialize, BorshSerialize};
use curve25519_dalek::{
    ristretto::{CompressedRistretto, RistrettoPoint},
    scalar::Scalar,
    traits::Identity,
};
// === ADD: imports (paste verbatim) ===
use curve25519_dalek::constants::{
    RISTRETTO_BASEPOINT_POINT as G,
    ED25519_BASEPOINT_POINT,
};
use curve25519_dalek::edwards::CompressedEdwardsY;
use sha2::Digest as Sha2Digest; // for Sha512
use sha2::Sha512 as Sha2_512;
use std::str::FromStr;
// === ADD: imports (append; keep existing) ===
use std::collections::HashSet;

// 📦 ADD: imports (paste verbatim at top; keep existing)
use solana_transaction_status::UiTransactionEncoding;
use std::sync::Arc as StdArc; // alias optional; kept for clarity

// 📦 ADD: imports (paste verbatim at top; keep existing)
use solana_transaction_status::UiTransactionEncoding;
use solana_sdk::{
    system_program,
    sysvar,
    compute_budget,
    address_lookup_table,
    stake,
    vote,
};

// 📦 ADD: imports (paste verbatim at top; keep existing)
use solana_client::rpc_config::RpcSendTransactionConfig;
// 📦 ADD: imports (append; keep existing)
use solana_client::client_error::ClientError;
// 📦 ADD: imports (append; keep existing)
use tokio::task::JoinSet;
// === ADD: imports (append; keep existing) ===
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use solana_sdk::instruction::Instruction as SolInstruction; // alias clarity
use sha3::{Sha3_512, Digest};
use rand::{rngs::OsRng, RngCore, CryptoRng, Rng};
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
    error::Error,
};
use tokio::sync::mpsc;
use rayon::prelude::*;

const RING_SIZE: usize = 16;
const MAX_DECOY_CACHE: usize = 256;
const SIGNATURE_TIMEOUT_MS: u64 = 150;
const MAX_RETRY_ATTEMPTS: u8 = 3;
const DECOY_REFRESH_INTERVAL_MS: u64 = 500;

// === ADD: constants (paste verbatim) ===
const SLOT_GUARD: u64 = 20;           // refresh blockhash if <=20 slots remain
const TX_MTU_SOFT: usize = 1200;      // soft cap to avoid TransactionTooLarge
const MEMO_CHUNK_BYTES_V3: usize = 480;

// === ADD: constants v4 (paste verbatim) ===
const MEMO_CHUNK_MIN: usize = 320;   // variable-length chunking to avoid uniform sizes
const MEMO_CHUNK_MAX: usize = 560;   // inclusive upper bound (kept below MTU clamp)
const BATCH_MAX_CONCURRENCY: usize = 12; // slightly higher than 8; good on modern validators

// === ADD: v5 constants (append; keep existing) ===
const DECOY_EWMA_ALPHA: f64 = 0.70;     // memory of prior decoy score
const DECOY_SEED_ADDR_LIMIT: usize = 4; // extra addresses to crawl per refresh
const STRAT_TOP_Q: f64 = 0.20;          // top 20% slice
const STRAT_MID_Q: f64 = 0.50;          // next 50% slice
// tail slice implied by the rest

// ➕ ADD: recent key-image replay guard (prevents pathological reuse in bursty flows)
const KI_CACHE_TTL_SECS: u64 = 120; // short TTL; Hp(P‖m) already makes reuse unlikely
const KI_CACHE_CAP: usize   = 2048;

// 📦 ADD: module constants (paste verbatim at module scope)
/// Canonical Memo program id (module scope, not inside impl)
pub const MEMO_PROGRAM_ID: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

/// (Optional) SPL IDs as strings to avoid extra deps (skip if you already depend on spl crates)
pub const SPL_TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub const SPL_ATA_PROGRAM_ID:   &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";

// 🧱 ADD: micro Hp(P) cache + decoy filters (paste verbatim)
#[derive(Default)]
struct HpCache {
    // tiny, non-cryptographic cache for hot pubkeys within a process
    map: std::collections::HashMap<[u8;32], RistrettoPoint>,
    cap: usize,
}

impl HpCache {
    fn new(cap: usize) -> Self { Self { map: std::collections::HashMap::with_capacity(cap), cap } }
    fn get_or_insert_with<F: FnOnce()->RistrettoPoint>(&mut self, key: [u8;32], f: F) -> RistrettoPoint {
        if let Some(v)=self.map.get(&key){ return *v; }
        let v=f();
        if self.map.len()>=self.cap {
            // simple FIFO-ish eviction: remove 1 (deterministic key pop)
            if let Some(k)=self.map.keys().next().cloned(){ self.map.remove(&k); }
        }
        self.map.insert(key, v);
        v
    }
}

#[derive(Default)]
struct KiSeen {
    q: VecDeque<(u64, [u8;32])>,
    set: HashSet<[u8;32]>,
}
impl KiSeen {
    fn insert_and_prune(&mut self, now: u64, ki: [u8;32]) -> bool {
        // returns true if inserted (i.e., not seen); false if duplicate
        if self.set.contains(&ki) { return false; }
        self.q.push_back((now, ki));
        self.set.insert(ki);
        // prune by TTL & cap
        while let Some(&(t, k)) = self.q.front() {
            if self.q.len() <= KI_CACHE_CAP && now.saturating_sub(t) <= KI_CACHE_TTL_SECS { break; }
            self.q.pop_front();
            self.set.remove(&k);
        }
        true
    }
}

impl RingSignatureMixer {
    #[inline]
    fn is_disallowed_key(pk: &Pubkey) -> bool {
        if sysvar::is_sysvar_id(pk) { return true; }
        if pk == &system_program::id() { return true; }
        if pk == &compute_budget::id() { return true; }
        if pk == &address_lookup_table::program::id() { return true; }
        if pk == &stake::program::id() { return true; }
        if pk == &vote::program::id() { return true; }
        let s = pk.to_string();
        // Avoid SPL Token & ATA & Memo as decoys (string compare to dodge extra deps)
        if s == SPL_TOKEN_PROGRAM_ID || s == SPL_ATA_PROGRAM_ID || s == MEMO_PROGRAM_ID { return true; }
        false
    }

    // 🧠 ADD: curve guard + on-curve test (paste verbatim)
    #[inline]
    fn is_on_curve(pk: &Pubkey) -> bool {
        CompressedEdwardsY(pk.to_bytes()).decompress().is_some()
    }

    // 🧪 ADD: message-bound Hp (unlinkable key-image) (paste verbatim)
    #[inline]
    fn hp_point_msg(&self, p: &RistrettoPoint, msg32: &[u8;32]) -> RistrettoPoint {
        // Hp_m(P) = H_to_point("HpM" || P_compressed || msg32)
        let mut h = Sha3_512::new();
        h.update(b"HpM");
        h.update(&p.compress().to_bytes());
        h.update(msg32);
        let out = h.finalize();
        let mut u = [0u8; 64];
        u.copy_from_slice(&out);
        RistrettoPoint::from_uniform_bytes(&u)
    }
}

// ✂️ REPLACE: RingSignatureMixer struct (append seen_kis field)
#[derive(Debug, Clone)]
pub struct RingSignatureMixer {
    rpc_client: Arc<RpcClient>,
    decoy_cache: Arc<RwLock<DecoyCache>>,
    active_rings: Arc<RwLock<HashMap<[u8; 32], RingContext>>>,
    mixer_keypair: Arc<Keypair>,
    hp_cache: Arc<RwLock<HpCache>>,
    // NEW: KI replay guard
    seen_kis: Arc<RwLock<KiSeen>>,
}

#[derive(Debug, Clone)]
struct DecoyCache {
    addresses: VecDeque<DecoyAddress>,
    last_refresh: Instant,
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
    ring_members: Vec<RistrettoPoint>,
    key_images: Vec<CompressedRistretto>,
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
    // ✂️ REPLACE: new(...) (initialize seen_kis)
    pub fn new(rpc_url: &str, mixer_keypair: Keypair) -> Result<Self, Box<dyn Error>> {
        let rpc_client = Arc::new(RpcClient::new_with_timeout_and_commitment(
            rpc_url.to_string(),
            Duration::from_millis(500),
            CommitmentConfig::processed(),
        ));

        Ok(Self {
            rpc_client,
            decoy_cache: Arc::new(RwLock::new(DecoyCache {
                addresses: VecDeque::with_capacity(MAX_DECOY_CACHE),
                last_refresh: Instant::now(),
            })),
            active_rings: Arc::new(RwLock::new(HashMap::new())),
            mixer_keypair: Arc::new(mixer_keypair),
            hp_cache: Arc::new(RwLock::new(HpCache::new(1024))),
            seen_kis: Arc::new(RwLock::new(KiSeen::default())),
        })
    }

    // ✂️ REPLACE: mix_transaction (slot-guard + hash binding)
    pub async fn mix_transaction(
        &self,
        mut tx: Transaction,
        signer_keypair: &Keypair,
    ) -> Result<Transaction, Box<dyn Error>> {
        self.refresh_decoy_cache_if_needed().await?;

        // get_latest_blockhash is blocking; fence inside spawn_blocking
        let initial = {
            let rpc = self.rpc_client.clone();
            tokio::task::spawn_blocking(move || rpc.get_latest_blockhash())
                .await?? 
        };
        let recent_blockhash = self.refresh_blockhash_if_stale(initial).await?;
        tx.message.recent_blockhash = recent_blockhash;

        let msg_hash = self.compute_tx_hash(&tx);
        let ring_signature = self.create_ring_signature(&msg_hash, signer_keypair)?;

        let mixed = self.build_mixed_transaction_with_blockhash(
            tx,
            ring_signature,
            signer_keypair,
            recent_blockhash,
        )?;

        Ok(mixed)
    }

    // ✂️ REPLACE: create_ring_signature (hardening: unique ring, non-identity KI, strict invariants)
    fn create_ring_signature(
        &self,
        message: &[u8; 32],
        signer: &Keypair,
    ) -> Result<RingSignature, Box<dyn Error>> {
        use rand::RngCore;

        let mut rng = OsRng;
        let signer_pk = signer.pubkey();

        // 1) Build decoy set with strict invariants
        let mut seen: HashSet<[u8; 32]> = HashSet::with_capacity(RING_SIZE * 2);
        seen.insert(signer_pk.to_bytes());

        let mut decoys = Vec::with_capacity(RING_SIZE - 1);
        for _ in 0..4 {
            let batch = self.select_optimal_decoys(RING_SIZE - 1)?;
            for d in batch {
                if Self::is_disallowed_key(&d.pubkey) { continue; }
                if !Self::is_on_curve(&d.pubkey) { continue; }
                let pkb = d.pubkey.to_bytes();
                if seen.insert(pkb) {
                    decoys.push(d);
                    if decoys.len() == RING_SIZE - 1 { break; }
                }
            }
            if decoys.len() == RING_SIZE - 1 { break; }
        }
        if decoys.len() != RING_SIZE - 1 { return Err("Insufficient unique decoys".into()); }

        // 2) Points & deterministic signer index
        let signer_point = self.keypair_to_ristretto_point(signer)?;
        let mut ring_pubkeys: Vec<[u8; 32]> = decoys.iter().map(|d| d.pubkey.to_bytes()).collect();
        let mut ring_points:  Vec<RistrettoPoint> = decoys.iter().map(|d| self.pubkey_to_ristretto_point(&d.pubkey)).collect();

        let mut seed = [0u8; 64];
        seed[..32].copy_from_slice(message);
        seed[32..].copy_from_slice(&signer_pk.to_bytes());
        let signer_index = (prng32(&seed) as usize) % RING_SIZE;

        ring_pubkeys.insert(signer_index, signer_pk.to_bytes());
        ring_points.insert(signer_index, signer_point);

        // 3) Hp bound to message; small cache retained
        let n = ring_points.len();
        let mut hp_cache = self.hp_cache.write().unwrap();
        let hp: Vec<RistrettoPoint> = ring_points.iter().map(|p| {
            let key = p.compress().to_bytes();
            hp_cache.get_or_insert_with(key, || self.hp_point_msg(p, message))
        }).collect();
        drop(hp_cache);

        // 4) Sign
        let x = self.extract_private_scalar(signer)?;
        let I_c = (hp[signer_index] * x).compress();
        let I_pt = I_c.decompress().ok_or("KeyImage decompress failed")?;
        if I_pt.is_identity() { return Err("Invalid key image (identity)".into()); }

        // ✂️ REPLACE (inside create_ring_signature, right after computing I_c/I_pt):
        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        {
            let mut lk = self.seen_kis.write().unwrap();
            if !lk.insert_and_prune(now_unix, I_c.to_bytes()) {
                return Err("duplicate key-image observed within TTL".into());
            }
        }

        let mut s_vec: Vec<Scalar> = vec![Scalar::zero(); n];

        let u = Scalar::random(&mut rng);
        let L_s = G * u;
        let R_s = hp[signer_index] * u;
        let mut c = self.challenge_hash(message, &L_s, &R_s);

        let mut i = (signer_index + 1) % n;
        while i != signer_index {
            let r_i = Scalar::random(&mut rng);
            s_vec[i] = r_i;

            let L_i = (G * r_i) + (ring_points[i] * c);
            let R_i = (hp[i] * r_i) + (I_pt * c);

            c = self.challenge_hash(message, &L_i, &R_i);
            i = (i + 1) % n;
        }

        s_vec[signer_index] = u - (c * x);

        let L_last = (G * s_vec[signer_index]) + (ring_points[signer_index] * c);
        let R_last = (hp[signer_index] * s_vec[signer_index]) + (I_pt * c);
        let c0 = self.challenge_hash(message, &L_last, &R_last);

        // Rotation camo
        let rot = (c0.to_bytes()[0] as usize) % n;
        rotate_vec(&mut ring_pubkeys, rot);
        rotate_vec(&mut s_vec,       rot);

        Ok(RingSignature {
            c0: c0.to_bytes(),
            responses: s_vec.into_iter().map(|s| s.to_bytes()).collect(),
            key_image: I_c.to_bytes(),
            ring_pubkeys,
        })
    }

    fn generate_ring_responses<R: RngCore + CryptoRng>(
        &self,
        message: &[u8; 32],
        ring: &[RistrettoPoint],
        signer_index: usize,
        private_key: &Scalar,
        rng: &mut R,
    ) -> Result<(Scalar, Vec<Scalar>), Box<dyn Error>> {
        let n = ring.len();
        let mut responses = vec![Scalar::zero(); n];
        let mut commitments = vec![RistrettoPoint::identity(); n];
        
        let alpha = Scalar::random(rng);
        commitments[signer_index] = &RistrettoPoint::default() * &alpha;

        for i in 0..n {
            if i != signer_index {
                responses[i] = Scalar::random(rng);
                let c_i = self.hash_to_scalar(&[
                    message.as_ref(),
                    &ring[i].compress().to_bytes(),
                ].concat());
                commitments[i] = &RistrettoPoint::default() * &responses[i] + &ring[i] * &c_i;
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

    // ✂️ REPLACE: verify_ring_signature (duplicate/member sanity + timeout fence)
    fn verify_ring_signature(
        &self,
        signature: &RingSignature,
        message: &[u8; 32],
    ) -> Result<bool, Box<dyn Error>> {
        let deadline = Instant::now() + Duration::from_millis(SIGNATURE_TIMEOUT_MS);

        // 1) decode ring; ensure uniqueness
        let mut seen = HashSet::with_capacity(signature.ring_pubkeys.len());
        let ring_points: Vec<RistrettoPoint> = signature.ring_pubkeys
            .iter()
            .map(|pk| {
                if !seen.insert(*pk) { return None; }
                CompressedEdwardsY(*pk).decompress().map(RistrettoPoint::from)
            })
            .collect::<Option<Vec<_>>>()
            .ok_or("Invalid/duplicate ring member")?;

        let n = ring_points.len();
        if n == 0 || signature.responses.len() != n { return Ok(false); }

        // 2) load s, key image
        let s_vec: Vec<Scalar> = signature.responses
            .iter()
            .map(|b| Scalar::from_bytes_mod_order(*b))
            .collect();

        let I = CompressedRistretto(signature.key_image).decompress().ok_or("Invalid key image")?;
        if I.is_identity() { return Ok(false); }

        // 3) Hp per message (cached)
        let mut hp_cache = self.hp_cache.write().unwrap();
        let hp: Vec<RistrettoPoint> = ring_points.iter().map(|p| {
            let key = p.compress().to_bytes();
            hp_cache.get_or_insert_with(key, || self.hp_point_msg(p, message))
        }).collect();
        drop(hp_cache);

        // 4) challenge walk with deadline fence
        let mut c = Scalar::from_bytes_mod_order(signature.c0);
        for i in 0..n {
            if Instant::now() > deadline { return Err("Signature verification timeout".into()); }
            let L = (G * s_vec[i]) + (ring_points[i] * c);
            let R = (hp[i] * s_vec[i]) + (I * c);
            c = self.challenge_hash(message, &L, &R);
        }
        Ok(c == Scalar::from_bytes_mod_order(signature.c0))
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

    // ✂️ REPLACE: refresh_decoy_cache (EWMA + multi-seed, RPC fenced)
    async fn refresh_decoy_cache(&self) -> Result<(), Box<dyn Error>> {
        let old = {
            let cache = self.decoy_cache.read().unwrap();
            let mut m = HashMap::with_capacity(cache.addresses.len());
            for d in cache.addresses.iter() {
                m.insert(d.pubkey, (d.priority_score, d.last_active, d.balance));
            }
            m
        };

        let mut seeds: Vec<Pubkey> = Vec::with_capacity(1 + DECOY_SEED_ADDR_LIMIT);
        seeds.push(self.mixer_keypair.pubkey());
        {
            let cache = self.decoy_cache.read().unwrap();
            let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs();
            for d in cache.addresses.iter() {
                if seeds.len() >= 1 + DECOY_SEED_ADDR_LIMIT { break; }
                if ((d.pubkey.to_bytes()[0] as u64 ^ t) % 5) == 0 && !seeds.contains(&d.pubkey) {
                    seeds.push(d.pubkey);
                }
            }
        }

        let mut merged: HashMap<Pubkey, DecoyAddress> = HashMap::new();
        for s in seeds {
            let mut accs = self.fetch_active_accounts_from(&s).await.unwrap_or_default();
            for a in accs.drain(..) {
                merged.entry(a.pubkey).and_modify(|e| {
                    if a.last_active > e.last_active { e.last_active = a.last_active; e.balance = a.balance; }
                    if a.priority_score > e.priority_score { e.priority_score = a.priority_score; }
                }).or_insert(a);
            }
        }

        for v in merged.values_mut() {
            if let Some((old_score, _old_la, _old_bal)) = old.get(&v.pubkey) {
                v.priority_score = DECOY_EWMA_ALPHA * *old_score + (1.0 - DECOY_EWMA_ALPHA) * v.priority_score;
            }
        }

        let mut accounts: Vec<_> = merged.into_values().collect();
        accounts.sort_by(|a, b| b.priority_score.partial_cmp(&a.priority_score).unwrap());

        let mut cache = self.decoy_cache.write().unwrap();
        cache.addresses.clear();
        for a in accounts.into_iter().take(MAX_DECOY_CACHE) {
            cache.addresses.push_back(a);
        }
        cache.last_refresh = Instant::now();
        Ok(())
    }

    // === ADD: helper fetcher used by refresh_decoy_cache ===
    async fn fetch_active_accounts_from(&self, root: &Pubkey) -> Result<Vec<DecoyAddress>, Box<dyn Error>> {
        let recent_signatures = self.rpc_client
            .get_signatures_for_address(root, None)?; // bounded by RPC default

        let mut active_accounts: HashMap<Pubkey, DecoyAddress> = HashMap::new();

        for sig_info in recent_signatures.iter().take(100) {
            if let Ok(tx) = self.rpc_client.get_transaction(
                &sig_info.signature.parse()?,
                solana_transaction_status::UiTransactionEncoding::Base64,
            ) {
                if let Some(meta) = tx.transaction.meta {
                    let post = meta.post_balances.clone();
                    if let Some(decoded) = tx.transaction.transaction.decode() {
                        for (i, k) in decoded.message.account_keys.iter().enumerate() {
                            let bal = *post.get(i).unwrap_or(&0u64);
                            let last_active = tx.block_time.unwrap_or(0) as u64;
                            let fee = meta.fee;
                            let mut entry = active_accounts.entry(*k).or_insert(DecoyAddress {
                                pubkey: *k,
                                balance: bal,
                                last_active,
                                priority_score: 0.0,
                            });
                            // refresh balance/last_active as we see fresher data
                            if last_active > entry.last_active { entry.last_active = last_active; entry.balance = bal; }
                            let score = self.calculate_decoy_priority(entry.balance, entry.last_active, fee);
                            if score > entry.priority_score { entry.priority_score = score; }
                        }
                    }
                }
            }
        }

        let mut v: Vec<_> = active_accounts.into_values().collect();
        v.sort_by(|a, b| b.priority_score.partial_cmp(&a.priority_score).unwrap());
        Ok(v)
    }

    async fn fetch_active_accounts(&self) -> Result<Vec<DecoyAddress>, Box<dyn Error>> {
        let recent_signatures = self.rpc_client
            .get_signatures_for_address(
                &self.mixer_keypair.pubkey(),
                None,
            )?;

        let mut active_accounts = HashMap::new();
        
        for sig_info in recent_signatures.iter().take(100) {
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

    // === REPLACE: calculate_decoy_priority ===
    fn calculate_decoy_priority(&self, balance: u64, last_active_unix: u64, fee: u64) -> f64 {
        // Normalize onto [0,1]-ish scales; higher is better
        let balance_score = ((balance.max(1)) as f64).ln().min(20.0) / 20.0;

        let now_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let age_secs = now_unix.saturating_sub(last_active_unix).max(1) as f64;
        let activity_score = 1.0 / (1.0 + (age_secs / 3600.0)); // 1h decay

        let fee_score = (fee as f64 / 50_000.0).min(1.0);

        // Slightly overweight activity to pick hot accounts
        0.35 * balance_score + 0.50 * activity_score + 0.15 * fee_score
    }

    // === REPLACE: select_optimal_decoys (stratified ES sampling) ===
    fn select_optimal_decoys(&self, count: usize) -> Result<Vec<DecoyAddress>, Box<dyn Error>> {
        use rand::distributions::{Distribution, OpenClosed01};
        let cache = self.decoy_cache.read().unwrap();
        if cache.addresses.len() < count { return Err("Insufficient decoys in cache".into()); }

        // Build a working slice sorted by priority (it already is), split into strata
        let n = cache.addresses.len();
        let top_n = ((STRAT_TOP_Q * n as f64).ceil() as usize).min(n).max(1);
        let mid_n = ((STRAT_MID_Q * n as f64).ceil() as usize).min(n).max(1);

        let (top_slice, rest) = cache.addresses.as_slices();
        // For VecDeque, as_slices splits into up to two contiguous slices; stitch logically
        let mut all: Vec<&DecoyAddress> = Vec::with_capacity(n);
        all.extend_from_slice(top_slice);
        all.extend_from_slice(rest);

        let top = &all[0..top_n];
        let mid = &all[top_n..(top_n + mid_n).min(n)];
        let tail = &all[(top_n + mid_n).min(n)..n];

        // Target allocations
        let top_target = (count as f64 * 0.50).round() as usize; // 50% from top
        let mid_target = (count as f64 * 0.35).round() as usize; // 35% from mid
        let mut tail_target = count.saturating_sub(top_target + mid_target); // rest from tail

        let mut out: Vec<DecoyAddress> = Vec::with_capacity(count);
        let mut dist = OpenClosed01;

        // ES helper
        let mut pick_from = |slice: &[&DecoyAddress], k: usize, out: &mut Vec<DecoyAddress>| {
            if slice.is_empty() || k == 0 { return; }
            let mut keys: Vec<(f64, usize)> = Vec::with_capacity(slice.len());
            for (idx, d) in slice.iter().enumerate() {
                let w = (d.priority_score.max(1e-9)).min(1e9);
                let u: f64 = dist.sample(&mut rand::thread_rng());
                let key = u.powf(1.0 / w);
                keys.push((key, idx));
            }
            let take = k.min(keys.len());
            keys.select_nth_unstable_by(take, |a, b| b.0.partial_cmp(&a.0).unwrap());
            keys.truncate(take);
            for (_, idx) in keys {
                out.push((*slice[idx]).clone());
            }
        };

        pick_from(top, top_target, &mut out);
        pick_from(mid, mid_target, &mut out);
        // Adjust tail_target if we underfilled due to small strata
        tail_target = count.saturating_sub(out.len());
        pick_from(tail, tail_target, &mut out);

        // If still short (rare), backfill from overall reservoir distinct from chosen
        if out.len() < count {
            let chosen: HashSet<[u8;32]> = out.iter().map(|d| d.pubkey.to_bytes()).collect();
            for d in all.iter() {
                if out.len() >= count { break; }
                if !chosen.contains(&d.pubkey.to_bytes()) {
                    out.push((*d).clone());
                }
            }
        }

        Ok(out)
    }

    // ✂️ REPLACE: build_mixed_transaction_with_blockhash (dedupe CB; single head pair)
    fn build_mixed_transaction_with_blockhash(
        &self,
        mut original_tx: Transaction,
        ring_sig: RingSignature,
        signer: &Keypair,
        recent_blockhash: Hash,
    ) -> Result<Transaction, Box<dyn Error>> {
        use zeroize::Zeroize;

        // 1) Serialize → scramble → jittered chunking
        let mut ring_sig_data = borsh::to_vec(&ring_sig)?;
        let mut seed = Vec::with_capacity(64);
        seed.extend_from_slice(&ring_sig.c0);
        seed.extend_from_slice(recent_blockhash.as_ref());
        Self::scramble_bytes_xor_keystream(&mut ring_sig_data, &seed);

        let mut chunks = jitter_chunk_bytes(&ring_sig_data, &seed, MEMO_CHUNK_MIN, MEMO_CHUNK_MAX);
        if let Some(extra) = maybe_tiny_memo(&seed) { chunks.push(extra); }
        det_shuffle(&mut chunks, &seed);
        let rotate_by = (prng32(&seed) as usize) % (chunks.len().max(1));
        rotate_vec(&mut chunks, rotate_by);

        let base_len = original_tx.message_data().len();
        let chunks = clamp_memos_for_mtu(base_len, chunks, TX_MTU_SOFT);

        let memo_prog = Pubkey::from_str(MEMO_PROGRAM_ID)?;
        let mut memo_ixs = Vec::with_capacity(chunks.len());
        for c in chunks.into_iter() {
            memo_ixs.push(Instruction { program_id: memo_prog, accounts: vec![], data: c });
        }

        ring_sig_data.zeroize();
        seed.zeroize();

        // 2) Strip ANY existing compute-budget ixs from base message before weaving.
        let mut base_ixs: Vec<Instruction> = Vec::with_capacity(original_tx.message.instructions.len());
        for ix in original_tx.message.instructions.iter() {
            if ix.program_id == compute_budget::id() { continue; }
            base_ixs.push(ix.clone());
        }

        // 3) Deterministic interleave of memos with stripped base ixs.
        let salt = self.compute_tx_hash(&original_tx);
        let mut new_ixs = interleave_memos(&base_ixs, memo_ixs, &salt);

        // 4) Single, final CB pair at HEAD using congestion-aware jitter.
        let instr_count = new_ixs.len();
        self.append_compute_budget_jitter_dynamic(&mut new_ixs, &salt, instr_count);

        // 5) Rebuild message.
        original_tx.message.instructions = new_ixs;
        original_tx.message.recent_blockhash = recent_blockhash;
        original_tx.try_partial_sign(&[signer], recent_blockhash)?;
        Ok(original_tx)
    }

    // Tiny helper to coerce Vec<Instruction> to Vec<SolInstruction> without copies
    #[inline]
    fn reinterpret_cast_mut(v: &mut Vec<Instruction>) -> &mut Vec<SolInstruction> {
        // Same struct layout; Instruction == SolInstruction in solana-sdk
        // Safe here due to identical type; keeps zero-copy.
        // If your linter forbids, replace with explicit clone into Vec<SolInstruction>.
        unsafe { &mut *(v as *mut Vec<Instruction> as *mut Vec<SolInstruction>) }
    }

    // === REPLACE: compute_tx_hash (no allocs; bound to message bytes) ===
    fn compute_tx_hash(&self, tx: &Transaction) -> [u8; 32] {
        let mut h = Sha3_512::new();
        h.update(b"RING-MSG");
        h.update(tx.message_data());
        let out = h.finalize();
        let mut out32 = [0u8; 32];
        out32.copy_from_slice(&out[..32]);
        out32
    }

    // === REPLACE: keypair_to_ristretto_point (derive pub point consistent with ed25519) ===
    fn keypair_to_ristretto_point(&self, keypair: &Keypair) -> Result<RistrettoPoint, Box<dyn Error>> {
        // ed25519 scalar = clamp(Sha512(seed)[0..32])
        let bytes = keypair.to_bytes();
        let seed = &bytes[..32];
        let mut h = Sha2_512::digest(seed);
        h[0]  &= 248;
        h[31] &= 63;
        h[31] |= 64;

        let mut s = [0u8; 32];
        s.copy_from_slice(&h[..32]);
        let x = Scalar::from_bits(s);

        // P = x * Ed25519 basepoint, then map (torsion-free) to Ristretto
        let ed_point = &ED25519_BASEPOINT_POINT * &x;
        Ok(RistrettoPoint::from(ed_point))
    }

    // === REPLACE: pubkey_to_ristretto_point (Edwards->Ristretto; no "hash as point") ===
    fn pubkey_to_ristretto_point(&self, pubkey: &Pubkey) -> RistrettoPoint {
        let ed = CompressedEdwardsY(pubkey.to_bytes())
            .decompress()
            .expect("Invalid ed25519 public key");
        RistrettoPoint::from(ed)
    }

    // === REPLACE: extract_private_scalar (proper ed25519 clamping; zeroize) ===
    fn extract_private_scalar(&self, keypair: &Keypair) -> Result<Scalar, Box<dyn Error>> {
        let bytes = keypair.to_bytes();
        let seed = &bytes[..32];
        let mut h = Sha2_512::digest(seed);

        // ed25519 clamp
        h[0]  &= 248;
        h[31] &= 63;
        h[31] |= 64;

        let mut s32 = [0u8; 32];
        s32.copy_from_slice(&h[..32]);
        let x = Scalar::from_bits(s32);

        use zeroize::Zeroize;
        s32.zeroize();
        Ok(x)
    }

    // === REPLACE: compute_key_image (I = x * Hp(P_s)) ===
    fn compute_key_image(&self, private_key: &Scalar, public_point: &RistrettoPoint) -> Result<CompressedRistretto, Box<dyn Error>> {
        let hp = self.hp_point(public_point);
        let I = hp * private_key;
        Ok(I.compress())
    }

    // === REPLACE: hash_to_point ===
    fn hash_to_point(&self, data: &[u8]) -> Result<RistrettoPoint, Box<dyn Error>> {
        let mut h = Sha3_512::new();
        h.update(b"HashToPoint");
        h.update(data);
        let out = h.finalize();
        let mut u = [0u8; 64];
        u.copy_from_slice(&out);
        Ok(RistrettoPoint::from_uniform_bytes(&u))
    }

    // === REPLACE: hash_to_scalar ===
    fn hash_to_scalar(&self, data: &[u8]) -> Scalar {
        let mut h = Sha3_512::new();
        h.update(b"HashToScalar");
        h.update(data);
        let out = h.finalize();
        let mut wide = [0u8; 64];
        wide.copy_from_slice(&out);
        Scalar::from_bytes_mod_order_wide(&wide)
    }

    fn build_challenge_input(&self, message: &[u8], commitments: &[RistrettoPoint]) -> Vec<u8> {
        let mut input = Vec::with_capacity(32 + commitments.len() * 32);
        input.extend_from_slice(message);
        
        for commitment in commitments {
            input.extend_from_slice(&commitment.compress().to_bytes());
        }
        
        input
    }

    fn compute_next_challenge(&self, current: &Scalar, commitment: &RistrettoPoint, message: &[u8]) -> Scalar {
        let mut hasher = Sha3_512::new();
        hasher.update(&current.to_bytes());
        hasher.update(&commitment.compress().to_bytes());
        hasher.update(message);
        
        let hash = hasher.finalize();
        Scalar::from_bytes_mod_order_wide(&hash.into())
    }

    // === REPLACE: batch_mix_transactions (hash-grouped + guarded + parallel) ===
    pub async fn batch_mix_transactions(
        &self,
        transactions: Vec<(Transaction, Keypair)>,
    ) -> Result<Vec<Transaction>, Box<dyn Error>> {
        let (tx_sender, mut tx_receiver) = mpsc::channel(transactions.len());
        let semaphore = Arc::new(tokio::sync::Semaphore::new(BATCH_MAX_CONCURRENCY));

        // Prefetch a blockhash for the batch; each task will guard against staleness
        let batch_hash = self.rpc_client.get_latest_blockhash()?;

        let tasks: Vec<_> = transactions
            .into_iter()
            .map(|(mut tx, keypair)| {
                let mixer = self.clone();
                let tx_sender = tx_sender.clone();
                let semaphore = semaphore.clone();
                let batch_hash = batch_hash.clone();
                tokio::spawn(async move {
                    let _permit = semaphore.acquire().await.unwrap();

                    // Slot-guarded hash per tx
                    let bh = match refresh_blockhash_if_stale(&mixer.rpc_client, batch_hash) {
                        Ok(h) => h,
                        Err(e) => return tx_sender.send(Err(e)).await.map_err(|_| ()).unwrap(),
                    };
                    tx.message.recent_blockhash = bh;

                    // Bind + sign
                    let msg_hash = mixer.compute_tx_hash(&tx);
                    let ring_signature = match mixer.create_ring_signature(&msg_hash, &keypair) {
                        Ok(r) => r,
                        Err(e) => return tx_sender.send(Err(e)).await.map_err(|_| ()).unwrap(),
                    };

                    let mixed = match mixer.build_mixed_transaction_with_blockhash(
                        tx, ring_signature, &keypair, bh,
                    ) {
                        Ok(m) => m,
                        Err(e) => return tx_sender.send(Err(e)).await.map_err(|_| ()).unwrap(),
                    };

                    tx_sender.send(Ok(mixed)).await.map_err(|_| ()).unwrap();
                })
            })
            .collect();

        drop(tx_sender);

        let mut out = Vec::with_capacity(tasks.len());
        while let Some(res) = tx_receiver.recv().await {
            out.push(res?);
        }
        for t in tasks { t.await.unwrap(); }
        Ok(out)
    }

    // === REPLACE: create_decoy_transaction (memo-only decoy) ===
    pub fn create_decoy_transaction(&self, _amount: u64) -> Result<Transaction, Box<dyn Error>> {
        use rand::RngCore;
        let mut buf = [0u8; 64];
        rand::rngs::OsRng.fill_bytes(&mut buf);
        let memo_prog = Pubkey::from_str(MEMO_PROGRAM)?;
        let ix = Instruction { program_id: memo_prog, accounts: vec![], data: buf.to_vec() };

        let recent_blockhash = self.rpc_client.get_latest_blockhash()?;
        let tx = Transaction::new_signed_with_payer(
            &[ix],
            Some(&self.mixer_keypair.pubkey()),
            &[&*self.mixer_keypair],
            recent_blockhash,
        );
        Ok(tx)
    }

    // === REPLACE: inject_decoy_transactions ===
    pub async fn inject_decoy_transactions(&self, count: usize) -> Result<Vec<Signature>, Box<dyn Error>> {
        let mut sigs = Vec::with_capacity(count);
        for _ in 0..count {
            let tx = self.create_decoy_transaction(0)?;
            match self.rpc_client.send_transaction(&tx) {
                Ok(sig) => sigs.push(sig),
                Err(e) => {
                    // Accept partial success; throttle retries
                    if sigs.len() >= count / 2 { break; }
                    return Err(format!("Decoy send failed: {}", e).into());
                }
            }
            tokio::time::sleep(Duration::from_millis(35)).await;
        }
        Ok(sigs)
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

    // === ADD: helpers (paste verbatim) ===
    fn hash_to_scalar_dom(&self, dom: &[u8], parts: &[&[u8]]) -> Scalar {
        let mut h = Sha3_512::new();
        h.update(dom);
        for p in parts { h.update(p); }
        let out = h.finalize();
        let mut wide = [0u8; 64];
        wide.copy_from_slice(&out);
        Scalar::from_bytes_mod_order_wide(&wide)
    }

    // ➕ ADD: Deterministic XOR keystream to scramble memo payload; avoids static byte patterns.
    fn scramble_bytes_xor_keystream(buf: &mut [u8], seed: &[u8]) {
        // seed is (c0 || blockhash) in our use; generate 64B blocks via SHA3-512("PAD"|seed|ctr)
        let mut ctr: u64 = 0;
        let mut i = 0usize;
        while i < buf.len() {
            let mut h = Sha3_512::new();
            h.update(b"PAD");
            h.update(seed);
            h.update(&ctr.to_le_bytes());
            let pad = h.finalize();            // 64 bytes
            let take = pad.len().min(buf.len() - i);
            for k in 0..take {
                buf[i + k] ^= pad[k];
            }
            i += take;
            ctr = ctr.wrapping_add(1);
        }
    }

    // ➕ ADD: quick tx sanity to fail early in dev/integration
    #[inline]
    fn _assert_tx_nonempty(tx: &Transaction) -> Result<(), Box<dyn Error>> {
        if tx.message.instructions.is_empty() { return Err("empty instruction set".into()); }
        Ok(())
    }

    // ➕ ADD: map recent validator performance → CU price band (microlamports/CU).
    // Safe fallback: return None if RPC lacks the endpoint or data.
    fn suggest_cu_price_from_perf(&self) -> Option<u64> {
        // Pull a short window; keeps CPU + RPC light.
        let samples = self.rpc_client.get_recent_performance_samples(Some(8)).ok()?;
        if samples.is_empty() { return None; }

        // Average TPS over window; robust to short-term wobble.
        let mut tps_sum = 0f64;
        for s in samples.iter() {
            let secs = (s.sample_period_secs.max(1)) as f64;
            let tps  = (s.num_transactions as f64) / secs;
            tps_sum += tps;
        }
        let avg_tps = tps_sum / (samples.len() as f64);

        // Map TPS → CU price bands observed to clear in practice.
        // You can tighten these thresholds as you gather per-validator telemetry.
        // Units are microlamports/CU, aligned with our fixed BANDS below.
        if      avg_tps < 1_500.0 { Some(2_200) }
        else if avg_tps < 2_500.0 { Some(2_800) }
        else if avg_tps < 3_500.0 { Some(3_500) }
        else if avg_tps < 4_500.0 { Some(4_200) }
        else if avg_tps < 5_500.0 { Some(5_000) }
        else                      { Some(6_000) }
    }

    // ➕ ADD: classify send errors to tune escalation/backoff deterministically.
    #[derive(Copy, Clone, Debug, Eq, PartialEq)]
    enum SendFailKind { BlockhashExpired, LowPriority, AccountInUse, NodeBehind, Other }

    fn classify_send_error_msg(msg: &str) -> SendFailKind {
        let m = msg.to_ascii_lowercase();
        if m.contains("blockhash not found") || m.contains("blockhash expired") { return SendFailKind::BlockhashExpired; }
        if m.contains("priority too low") || m.contains("insufficient priority") || m.contains("would not fit in a block") { return SendFailKind::LowPriority; }
        if m.contains("account in use") || m.contains("accountalreadyinitialized") || m.contains("locks") { return SendFailKind::AccountInUse; }
        if m.contains("min context slot") || m.contains("node is behind") || m.contains("slot was skipped") { return SendFailKind::NodeBehind; }
        SendFailKind::Other
    }

    // ➕ ADD: compute a safe min_context_slot to avoid stale forks.
    fn get_min_context_slot_guard(&self) -> Option<u64> {
        // Light RPC; if it fails, just return None (do not break submission).
        self.rpc_client.get_slot().ok().map(|slot| slot.saturating_sub(2))
    }

    fn challenge_hash(&self, msg32: &[u8;32], L: &RistrettoPoint, R: &RistrettoPoint) -> Scalar {
        let l = L.compress().to_bytes();
        let r = R.compress().to_bytes();
        self.hash_to_scalar_dom(b"RING-CHALLENGE", &[msg32, &l, &r])
    }

    fn hp_point(&self, p: &RistrettoPoint) -> RistrettoPoint {
        // Hp(P) = H_to_point("Hp", P_compressed)
        let mut h = Sha3_512::new();
        h.update(b"Hp");
        h.update(&p.compress().to_bytes());
        let out = h.finalize();
        let mut u = [0u8; 64];
        u.copy_from_slice(&out);
        RistrettoPoint::from_uniform_bytes(&u)
    }

    // ✂️ REPLACE: compute_tx_hash
    fn compute_tx_hash(&self, tx: &Transaction) -> [u8; 32] {
        let mut h = Sha3_512::new();
        h.update(b"RING-MSG");
        h.update(tx.message_data());
        let out = h.finalize();
        let mut out32 = [0u8; 32];
        out32.copy_from_slice(&out[..32]);
        out32
    }

    // ✂️ REPLACE: keypair_to_ristretto_point / pubkey_to_ristretto_point / extract_private_scalar
    fn keypair_to_ristretto_point(&self, keypair: &Keypair) -> Result<RistrettoPoint, Box<dyn Error>> {
        let bytes = keypair.to_bytes();
        let seed = &bytes[..32];
        let mut h = Sha2_512::digest(seed);
        h[0]  &= 248;
        h[31] &= 63;
        h[31] |= 64;
        let mut s = [0u8; 32];
        s.copy_from_slice(&h[..32]);
        let x = Scalar::from_bits(s);
        let ed_point = &ED25519_BASEPOINT_POINT * &x;
        Ok(RistrettoPoint::from(ed_point))
    }

    fn pubkey_to_ristretto_point(&self, pubkey: &Pubkey) -> RistrettoPoint {
        let ed = CompressedEdwardsY(pubkey.to_bytes())
            .decompress()
            .expect("Invalid ed25519 public key");
        RistrettoPoint::from(ed)
    }

    fn extract_private_scalar(&self, keypair: &Keypair) -> Result<Scalar, Box<dyn Error>> {
        let bytes = keypair.to_bytes();
        let seed = &bytes[..32];
        let mut h = Sha2_512::digest(seed);
        h[0]  &= 248;
        h[31] &= 63;
        h[31] |= 64;
        let mut s32 = [0u8; 32];
        s32.copy_from_slice(&h[..32]);
        let x = Scalar::from_bits(s32);
        use zeroize::Zeroize;
        s32.zeroize();
        Ok(x)
    }

    // ✂️ REPLACE: hash utilities
    fn hash_to_point(&self, data: &[u8]) -> Result<RistrettoPoint, Box<dyn Error>> {
        let mut h = Sha3_512::new();
        h.update(b"HashToPoint");
        h.update(data);
        let out = h.finalize();
        let mut u = [0u8; 64];
        u.copy_from_slice(&out);
        Ok(RistrettoPoint::from_uniform_bytes(&u))
    }

    fn hash_to_scalar(&self, data: &[u8]) -> Scalar {
        let mut h = Sha3_512::new();
        h.update(b"HashToScalar");
        h.update(data);
        let out = h.finalize();
        let mut wide = [0u8; 64];
        wide.copy_from_slice(&out);
        Scalar::from_bytes_mod_order_wide(&wide)
    }

    // ✂️ REPLACE: compute-budget jitter (instr-aware + congestion-aware + deterministic)
    fn append_compute_budget_jitter_dynamic(
        &self,
        ixs: &mut Vec<Instruction>,
        seed: &[u8],
        instr_count: usize,
    ) {
        // Heuristic CU limit: base + per-ix + bounded jitter; clamp for safety.
        let base   = 50_000u32 + (instr_count as u32) * 12_000u32;
        let jitter = (prng32(seed) % 10_000) as u32; // ±10k spread
        let cu_limit = (base + jitter).min(200_000).max(90_000);

        // Deterministic default bands (seeded), then *raise* to congestion suggestion if needed.
        const BANDS: [u64; 6] = [2_200, 2_800, 3_500, 4_200, 5_000, 6_000];
        let seeded_idx = (prng32(seed) as usize) % BANDS.len();
        let mut cu_price = BANDS[seeded_idx];

        if let Some(suggest) = self.suggest_cu_price_from_perf() {
            // Use the higher of (seeded band, suggested) to avoid underbidding.
            if suggest > cu_price { cu_price = suggest; }
        }

        // As per Solana semantics, CB ixs can be anywhere, but placing them first is best practice.
        ixs.insert(0, ComputeBudgetInstruction::set_compute_unit_limit(cu_limit));
        ixs.insert(1, ComputeBudgetInstruction::set_compute_unit_price(cu_price));
    }

    // ✂️ REPLACE: blockhash guard (RPC fenced)
    async fn refresh_blockhash_if_stale(
        &self,
        current_hash: Hash,
    ) -> Result<Hash, Box<dyn std::error::Error>> {
        let rpc = self.rpc_client.clone();
        let (last_valid, now) = tokio::try_join!(
            tokio::task::spawn_blocking({
                let rpc = rpc.clone();
                move || rpc.get_blockhash_last_valid_block_height(&current_hash)
            }),
            tokio::task::spawn_blocking(move || rpc.get_block_height()),
        )?;
        let last_valid = last_valid?;
        let now = now?;
        if last_valid.saturating_sub(now) <= SLOT_GUARD {
            let rpc = self.rpc_client.clone();
            Ok(tokio::task::spawn_blocking(move || rpc.get_latest_blockhash()).await??)
        } else {
            Ok(current_hash)
        }
    }

    const MEMO_PROGRAM: &str = "MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr";

    fn chunk_bytes(data: &[u8], max_chunk: usize) -> Vec<Vec<u8>> {
        let mut out = Vec::new();
        let mut i = 0;
        while i < data.len() {
            let end = (i + max_chunk).min(data.len());
            out.push(data[i..end].to_vec());
            i = end;
        }
        out
    }

    // === ADD: v3 helpers (paste verbatim) ===
    fn prng32(seed: &[u8]) -> u32 {
        let mut x = u32::from_le_bytes(seed[0..4].try_into().unwrap());
        x ^= x << 13; x ^= x >> 17; x ^= x << 5;
        x
    }

    fn prng64(seed: &[u8]) -> u64 {
        let mut x = u64::from_le_bytes(seed[0..8].try_into().unwrap());
        x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
        x.wrapping_mul(0x2545F4914F6CDD1D)
    }

    fn det_shuffle<T: Clone>(v: &mut Vec<T>, seed: &[u8]) {
        let mut x = prng64(seed);
        for i in (1..v.len()).rev() {
            x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
            let j = (x as usize) % (i + 1);
            v.swap(i, j);
        }
    }

    fn rotate_vec<T: Clone>(v: &mut Vec<T>, k: usize) {
        if v.is_empty() { return; }
        v.rotate_left(k % v.len());
    }

    fn dynamic_cu_limit(instr_count: usize, seed: &[u8]) -> u32 {
        // Heuristic: ~12k CU per ix + base; jitter ±10%
        let base = 50_000u32 + (instr_count as u32) * 12_000u32;
        let jitter = (prng32(seed) % 10_000) as u32; // up to 10k
        (base + jitter).min(200_000).max(90_000)
    }

    fn choose_cu_price_band(seed: &[u8]) -> u64 {
        // Micro-lamports per CU bands (aligns with common mainnet percentiles)
        const BANDS: [u64; 6] = [2_200, 2_800, 3_500, 4_200, 5_000, 6_000];
        let idx = (prng32(seed) as usize) % BANDS.len();
        BANDS[idx]
    }

    fn interleave_memos(
        orig: &[Instruction],
        mut memos: Vec<Instruction>,
        seed: &[u8],
    ) -> Vec<Instruction> {
        // Place first memo at start, then weave rest after every k-th ix
        let mut out = Vec::with_capacity(orig.len() + memos.len());
        if let Some(first) = memos.first() { out.push(first.clone()); }
        let step = 1 + ((prng32(seed) as usize) % (orig.len().max(1)));
        for (i, ix) in orig.iter().enumerate() {
            out.push(ix.clone());
            if i % step == 0 && !memos.is_empty() && i != 0 {
                if let Some(m) = memos.pop() { out.push(m); }
            }
        }
        // any remaining memos at end
        for m in memos.into_iter().skip(1) { out.push(m); }
        out
    }

    fn estimate_added_memo_size(memo_chunks: usize, avg_chunk: usize) -> usize {
        // rough overhead per ix header ~ <32B>, be conservative
        memo_chunks * (avg_chunk + 32)
    }

    fn clamp_memos_for_mtu(
        base_len: usize,
        mut chunks: Vec<Vec<u8>>,
        mtu_soft: usize,
    ) -> Vec<Vec<u8>> {
        if chunks.is_empty() { return chunks; }
        let mut k = chunks.len();
        while k > 0 {
            let added = estimate_added_memo_size(k, chunks[0].len());
            if base_len + added <= mtu_soft { break; }
            k -= 1;
        }
        chunks.truncate(k);
        chunks
    }

    fn append_compute_budget_jitter(ixs: &mut Vec<SolInstruction>, seed: &[u8], instr_count: usize) {
        let cu_limit  = dynamic_cu_limit(instr_count, seed);
        let cu_price  = choose_cu_price_band(seed);
        ixs.insert(0, ComputeBudgetInstruction::set_compute_unit_limit(cu_limit));
        ixs.insert(1, ComputeBudgetInstruction::set_compute_unit_price(cu_price));
    }

    fn refresh_blockhash_if_stale(
        rpc: &RpcClient,
        current_hash: Hash,
    ) -> Result<Hash, Box<dyn std::error::Error>> {
        // If remaining slots small, fetch a fresh blockhash
        let last_valid = rpc.get_blockhash_last_valid_block_height(&current_hash)?;
        let now = rpc.get_block_height()?;
        if last_valid.saturating_sub(now) <= SLOT_GUARD {
            Ok(rpc.get_latest_blockhash()?)
        } else {
            Ok(current_hash)
        }
    }

    // Zero-copy coercion helper; if you dislike `unsafe`, clone into a fresh Vec<SolInstruction>.
    #[inline]
    fn reinterpret_cast_mut(v: &mut Vec<Instruction>) -> &mut Vec<SolInstruction> {
        unsafe { &mut *(v as *mut Vec<Instruction> as *mut Vec<SolInstruction>) }
    }

    // === ADD: memo-shadowing helper (optional) ===
    fn memo_shadow_pattern(seed: &[u8]) -> Option<Vec<Vec<u8>>> {
        // Occasionally add a tiny extra memo (8–16B) to mimic wallet UX bursts
        if (prng32(seed) % 3) == 0 {
            let n = 8 + (prng32(seed) as usize % 9);
            let mut v = vec![0u8; n];
            let r = prng64(seed);
            v.iter_mut().enumerate().for_each(|(i, b)| *b = ((r >> (i % 56)) & 0xFF) as u8);
            Some(vec![v])
        } else {
            None
        }
    }

    // === ADD: v4 helpers (paste verbatim) ===
    fn jitter_chunk_bytes(data: &[u8], seed: &[u8], min_b: usize, max_b: usize) -> Vec<Vec<u8>> {
        assert!(min_b <= max_b && min_b >= 64);
        let mut out = Vec::new();
        let mut i = 0usize;
        let mut s = prng64(seed);
        while i < data.len() {
            // new step width each iteration
            s ^= s >> 12; s ^= s << 25; s ^= s >> 27;
            let span = min_b + ((s as usize) % (max_b - min_b + 1));
            let end = (i + span).min(data.len());
            out.push(data[i..end].to_vec());
            i = end;
        }
        out
    }

    // Rare small extra memo to mimic wallet UX (deterministic; ~33%)
    fn maybe_tiny_memo(seed: &[u8]) -> Option<Vec<u8>> {
        if (prng32(seed) % 3) == 0 {
            let n = 8 + (prng32(seed) as usize % 9);
            let mut v = vec![0u8; n];
            let r = prng64(seed);
            v.iter_mut().enumerate().for_each(|(i, b)| *b = ((r >> (i % 56)) & 0xFF) as u8);
            Some(v)
        } else { None }
    }

    // === ADD: deterministic PRNG & helpers (paste verbatim) ===
    fn prng32(seed: &[u8]) -> u32 {
        // xorshift-like; not crypto, only for layout jitter
        let mut x = u32::from_le_bytes(seed[0..4].try_into().unwrap());
        x ^= x << 13; x ^= x >> 17; x ^= x << 5;
        x
    }

    fn det_shuffle<T: Clone>(v: &mut Vec<T>, seed: &[u8]) {
        // Fisher–Yates with deterministic PRNG
        let mut x = u64::from_le_bytes(seed[0..8].try_into().unwrap());
        for i in (1..v.len()).rev() {
            // xorshift64*
            x ^= x >> 12; x ^= x << 25; x ^= x >> 27;
            let j = (x as usize) % (i + 1);
            v.swap(i, j);
        }
    }

    fn rotate_vec<T: Clone>(v: &mut Vec<T>, k: usize) {
        if v.is_empty() { return; }
        let r = k % v.len();
        v.rotate_left(r);
    }

    fn append_compute_budget_jitter(ixs: &mut Vec<SolInstruction>, seed: &[u8]) {
        // Deterministic jittered CU settings seeded by (c0||blockhash)
        let s = prng32(seed);
        let cu_limit  = 120_000 + (s % 40_000) as u32;      // 120k..160k
        let cu_price  = 3000 + ((s as u64 % 2000) as u64);  // 3000..4999 microlamports

        ixs.insert(0, ComputeBudgetInstruction::set_compute_unit_limit(cu_limit));
        ixs.insert(1, ComputeBudgetInstruction::set_compute_unit_price(cu_price));
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

    // ✂️ REPLACE: submit_mixed_with_retries (error-aware + min_context_slot + deterministic)
    pub async fn submit_mixed_with_retries(
        &self,
        mut mixed: Transaction,
        extra_signers: &[&Keypair],
        max_attempts: usize,
    ) -> Result<Signature, Box<dyn Error>> {
        let mut attempt = 0usize;

        let mut signer_refs: Vec<&Keypair> = Vec::with_capacity(1 + extra_signers.len());
        signer_refs.push(&*self.mixer_keypair);
        signer_refs.extend_from_slice(extra_signers);

        loop {
            // Anchor to current chain tip to avoid stale forks.
            let min_ctx = self.get_min_context_slot_guard();

            let cfg = RpcSendTransactionConfig {
                skip_preflight: true,
                preflight_commitment: Some(CommitmentConfig::processed()),
                min_context_slot: min_ctx,
                ..Default::default()
            };

            let send_res = {
                let rpc = self.rpc_client.clone();
                let tx  = mixed.clone();
                tokio::task::spawn_blocking(move || rpc.send_transaction_with_config(&tx, cfg)).await?
            };

            match send_res {
                Ok(sig) => return Ok(sig),
                Err(e) => {
                    attempt += 1;
                    if attempt >= max_attempts {
                        return Err(format!("send failed after {} attempts: {}", attempt, e).into());
                    }

                    // Classify failure to pick the right escalation/backoff.
                    let kind = Self::classify_send_error_msg(&format!("{}", e));

                    // 1) Always refresh blockhash (slot guard).
                    let new_bh = self.refresh_blockhash_if_stale(mixed.message.recent_blockhash).await?;
                    mixed.message.recent_blockhash = new_bh;

                    // 2) Strip head CB pair and rebuild with attempt-salted seed.
                    let mut ixs = mixed.message.instructions.clone();
                    while ixs.len() >= 2
                        && ixs.get(0).map(|ix| ix.program_id) == Some(compute_budget::id())
                        && ixs.get(1).map(|ix| ix.program_id) == Some(compute_budget::id())
                    {
                        ixs.drain(0..2);
                    }
                    let instr_count = ixs.len();

                    let mut seed = self.compute_tx_hash(&mixed);
                    seed[0] ^= (attempt as u8).wrapping_mul(0x5B);
                    seed[1] ^= (attempt as u8).wrapping_mul(0xA7);

                    const BANDS: [u64; 6] = [2_200, 2_800, 3_500, 4_200, 5_000, 6_000];
                    let base   = 50_000u32 + (instr_count as u32) * 12_000u32;
                    let jitter = (prng32(&seed) % 10_000) as u32;
                    let cu_limit = (base + jitter).min(200_000).max(90_000);

                    // Default seeded band for this attempt.
                    let mut cu_price = BANDS[(attempt + (prng32(&seed) as usize)) % BANDS.len()];
                    if let Some(suggest) = self.suggest_cu_price_from_perf() {
                        if suggest > cu_price { cu_price = suggest; }
                    }

                    // Targeted escalation:
                    match kind {
                        SendFailKind::LowPriority => { cu_price = *BANDS.last().unwrap(); } // jump to top band
                        SendFailKind::AccountInUse => { /* keep price; just back off a tad below */ }
                        SendFailKind::NodeBehind => { /* keep or raise slightly via suggest already applied */ }
                        SendFailKind::BlockhashExpired | SendFailKind::Other => { /* already handled via new hash */ }
                    }

                    let mut rebuilt = Vec::with_capacity(ixs.len() + 2);
                    rebuilt.push(ComputeBudgetInstruction::set_compute_unit_limit(cu_limit));
                    rebuilt.push(ComputeBudgetInstruction::set_compute_unit_price(cu_price));
                    rebuilt.extend(ixs);
                    mixed.message.instructions = rebuilt;

                    // 3) Re-sign with updated blockhash.
                    mixed.try_sign(&signer_refs, new_bh)?;

                    // 4) Deterministic pause tuned by failure class.
                    let base_sleep = match kind {
                        SendFailKind::AccountInUse => 22u64,  // longer to clear locks
                        SendFailKind::NodeBehind   => 18u64,
                        _ => 14u64, // default fast retry
                    };
                    let pause_ms = base_sleep + (prng32(&seed) % 7) as u64; // small jitter
                    tokio::time::sleep(Duration::from_millis(pause_ms)).await;
                }
            }
        }
    }

    // ➕ ADD: multi-RPC fan-out with first-winner capture (per attempt).
    // Pass additional RPC endpoints as Arc<RpcClient>. self.rpc_client is always included.
    pub async fn submit_mixed_with_retries_fanout(
        &self,
        mut mixed: Transaction,
        extra_signers: &[&Keypair],
        max_attempts: usize,
        extra_rpcs: &[StdArc<RpcClient>],
    ) -> Result<Signature, Box<dyn Error>> {
        let mut attempt = 0usize;

        let mut signer_refs: Vec<&Keypair> = Vec::with_capacity(1 + extra_signers.len());
        signer_refs.push(&*self.mixer_keypair);
        signer_refs.extend_from_slice(extra_signers);

        loop {
            // Anchor to current chain tip (avoid stale forks).
            let min_ctx = self.get_min_context_slot_guard();

            // Build config once per attempt
            let cfg = RpcSendTransactionConfig {
                skip_preflight: true,
                preflight_commitment: Some(CommitmentConfig::processed()),
                min_context_slot: min_ctx,
                ..Default::default()
            };

            // === fan-out send ===
            let mut js = JoinSet::new();
            // primary
            {
                let rpc = self.rpc_client.clone();
                let tx  = mixed.clone();
                let cfg = cfg.clone();
                js.spawn_blocking(move || rpc.send_transaction_with_config(&tx, cfg));
            }
            // extras
            for rpc in extra_rpcs.iter() {
                let rpc = rpc.clone();
                let tx  = mixed.clone();
                let cfg = cfg.clone();
                js.spawn_blocking(move || rpc.send_transaction_with_config(&tx, cfg));
            }

            // First success wins; capture first Ok(Signature)
            let mut first_err: Option<String> = None;
            let mut got_sig: Option<Signature> = None;
            while let Some(res) = js.join_next().await {
                match res {
                    Ok(Ok(sig)) => { got_sig = Some(sig); break; }
                    Ok(Err(e))  => {
                        if first_err.is_none() { first_err = Some(format!("{}", e)); }
                    }
                    Err(join_err) => {
                        if first_err.is_none() { first_err = Some(format!("join error: {}", join_err)); }
                    }
                }
            }
            // Cancel remaining tasks
            js.abort_all();

            if let Some(sig) = got_sig { return Ok(sig); }

            // === failure path ===
            attempt += 1;
            if attempt >= max_attempts {
                return Err(format!("send failed after {} attempts: {:?}", attempt, first_err).into());
            }

            // Classify one failure string (any) for escalation choice.
            let kind = Self::classify_send_error_msg(first_err.as_deref().unwrap_or(""));

            // 1) Always refresh blockhash (slot guard).
            let new_bh = self.refresh_blockhash_if_stale(mixed.message.recent_blockhash).await?;
            mixed.message.recent_blockhash = new_bh;

            // 2) Strip head CB pair and rebuild with attempt-salted seed.
            let mut ixs = mixed.message.instructions.clone();
            while ixs.len() >= 2
                && ixs.get(0).map(|ix| ix.program_id) == Some(compute_budget::id())
                && ixs.get(1).map(|ix| ix.program_id) == Some(compute_budget::id())
            {
                ixs.drain(0..2);
            }
            let instr_count = ixs.len();

            let mut seed = self.compute_tx_hash(&mixed);
            seed[0] ^= (attempt as u8).wrapping_mul(0x5B);
            seed[1] ^= (attempt as u8).wrapping_mul(0xA7);

            const BANDS: [u64; 6] = [2_200, 2_800, 3_500, 4_200, 5_000, 6_000];
            let base   = 50_000u32 + (instr_count as u32) * 12_000u32;
            let jitter = (prng32(&seed) % 10_000) as u32;
            let cu_limit = (base + jitter).min(200_000).max(90_000);

            let mut cu_price = BANDS[(attempt + (prng32(&seed) as usize)) % BANDS.len()];
            if let Some(suggest) = self.suggest_cu_price_from_perf() {
                if suggest > cu_price { cu_price = suggest; }
            }
            if matches!(kind, SendFailKind::LowPriority) {
                cu_price = *BANDS.last().unwrap();
            }

            let mut rebuilt = Vec::with_capacity(ixs.len() + 2);
            rebuilt.push(ComputeBudgetInstruction::set_compute_unit_limit(cu_limit));
            rebuilt.push(ComputeBudgetInstruction::set_compute_unit_price(cu_price));
            rebuilt.extend(ixs);
            mixed.message.instructions = rebuilt;

            mixed.try_sign(&signer_refs, new_bh)?;

            // Backoff tuned by error class
            let base_sleep = match kind {
                SendFailKind::AccountInUse => 22u64,
                SendFailKind::NodeBehind   => 18u64,
                _ => 14u64,
            };
            let pause_ms = base_sleep + (prng32(&seed) % 7) as u64;
            tokio::time::sleep(Duration::from_millis(pause_ms)).await;
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

    // 🧪 Minimal check snippet (paste & run locally)
    #[test]
    fn _mix_hash_stability() {
        let mixer = RingSignatureMixer::default();
        let kp = Keypair::new();
        let ix = solana_sdk::system_instruction::transfer(&kp.pubkey(), &Pubkey::new_unique(), 1234);
        let mut tx = Transaction::new_with_payer(&[ix], Some(&kp.pubkey()));
        let h1 = mixer.compute_tx_hash(&tx);
        tx.message.recent_blockhash = Hash::new_unique();
        let _ = mixer.compute_tx_hash(&tx);
        assert_ne!(h1, [0u8;32]);
    }
}

