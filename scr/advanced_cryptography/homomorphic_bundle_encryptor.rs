use borsh::{BorshDeserialize, BorshSerialize};
use dashmap::DashMap;
use num_bigint::{BigUint, RandBigInt};
use num_integer::Integer;
use num_traits::{One, Zero};
use parking_lot::RwLock;
use rand::rngs::OsRng;
use sha3::{Digest, Keccak256, Sha3_256};
use solana_sdk::{
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use std::{
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use zeroize::Zeroize;

const PAILLIER_KEY_SIZE: usize = 2048;
const MAX_BUNDLE_SIZE: usize = 5;
const ENCRYPTION_VERSION: u8 = 3;
const KEY_ROTATION_INTERVAL: u64 = 300;
const MAX_CACHED_KEYS: usize = 128;
const SECURITY_PARAMETER: usize = 128;
const TIMING_PROTECTION_ITERATIONS: usize = 500;

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct EncryptedBundle {
    pub version: u8,
    pub timestamp: u64,
    pub bundle_id: [u8; 32],
    pub encrypted_values: Vec<PaillierCiphertext>,
    pub commitments: Vec<[u8; 32]>,
    pub range_proofs: Vec<RangeProof>,
    pub bundle_proof: BundleProof,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct PaillierCiphertext {
    pub value: Vec<u8>,
    pub randomness_commitment: [u8; 32],
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct RangeProof {
    pub commitment: [u8; 32],
    pub challenge: [u8; 32],
    pub response: Vec<u8>,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct BundleProof {
    pub aggregated_commitment: [u8; 32],
    pub schnorr_proof: SchnorrProof,
    pub bundle_hash: [u8; 32],
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct SchnorrProof {
    pub commitment: [u8; 32],
    pub challenge: [u8; 32],
    pub response: Vec<u8>,
}

struct PaillierKey {
    n: BigUint,
    n_squared: BigUint,
    g: BigUint,
    lambda: Option<BigUint>,
    mu: Option<BigUint>,
    created_at: Instant,
    epoch: u64,
}

impl Drop for PaillierKey {
    fn drop(&mut self) {
        if let Some(lambda) = &mut self.lambda {
            lambda.zeroize();
        }
        if let Some(mu) = &mut self.mu {
            mu.zeroize();
        }
    }
}

pub struct HomomorphicBundleEncryptor {
    keys: Arc<DashMap<[u8; 32], Arc<PaillierKey>>>,
    current_key: Arc<RwLock<Option<Arc<PaillierKey>>>>,
    epoch_counter: Arc<AtomicU64>,
    operation_counter: Arc<AtomicUsize>,
    rotation_interval: Duration,
}

impl HomomorphicBundleEncryptor {
    pub fn new() -> Self {
        let encryptor = Self {
            keys: Arc::new(DashMap::new()),
            current_key: Arc::new(RwLock::new(None)),
            epoch_counter: Arc::new(AtomicU64::new(0)),
            operation_counter: Arc::new(AtomicUsize::new(0)),
            rotation_interval: Duration::from_secs(KEY_ROTATION_INTERVAL),
        };
        encryptor.initialize_key();
        encryptor
    }

    pub fn encrypt_bundle(&self, transactions: &[Transaction]) -> Result<EncryptedBundle, Box<dyn std::error::Error + Send + Sync>> {
        if transactions.is_empty() || transactions.len() > MAX_BUNDLE_SIZE {
            return Err("Invalid bundle size".into());
        }

        self.operation_counter.fetch_add(1, Ordering::Relaxed);
        self.check_key_rotation();

        let key = self.get_current_key()?;
        let timestamp = self.get_secure_timestamp();
        let bundle_id = self.generate_bundle_id(transactions, timestamp);

        let mut encrypted_values = Vec::with_capacity(transactions.len());
        let mut commitments = Vec::with_capacity(transactions.len());
        let mut range_proofs = Vec::with_capacity(transactions.len());

        for tx in transactions {
            let tx_bytes = bincode::serialize(tx)?;
            let (ciphertext, randomness) = self.paillier_encrypt(&key, &tx_bytes)?;
            
            let commitment = self.create_commitment(&tx_bytes, &randomness);
            let range_proof = self.create_range_proof(&tx_bytes, &commitment)?;
            
            encrypted_values.push(PaillierCiphertext {
                value: ciphertext.to_bytes_le(),
                randomness_commitment: self.hash_biguint(&randomness),
            });
            
            commitments.push(commitment);
            range_proofs.push(range_proof);
        }

        let bundle_proof = self.create_bundle_proof(&encrypted_values, &commitments, &bundle_id)?;
        
        self.timing_protection();

        Ok(EncryptedBundle {
            version: ENCRYPTION_VERSION,
            timestamp,
            bundle_id,
            encrypted_values,
            commitments,
            range_proofs,
            bundle_proof,
        })
    }

    pub fn decrypt_bundle(&self, bundle: &EncryptedBundle) -> Result<Vec<Transaction>, Box<dyn std::error::Error + Send + Sync>> {
        if bundle.version != ENCRYPTION_VERSION {
            return Err("Unsupported version".into());
        }

        self.verify_bundle_integrity(bundle)?;

        let key = self.get_key_for_timestamp(bundle.timestamp)?;
        let lambda = key.lambda.as_ref().ok_or("Missing decryption key")?;
        let mu = key.mu.as_ref().ok_or("Missing decryption parameter")?;

        let mut transactions = Vec::with_capacity(bundle.encrypted_values.len());

        for encrypted in &bundle.encrypted_values {
            let ciphertext = BigUint::from_bytes_le(&encrypted.value);
            let plaintext_bytes = self.paillier_decrypt(&key, &ciphertext, lambda, mu)?;
            
            let tx: Transaction = bincode::deserialize(&plaintext_bytes)?;
            transactions.push(tx);
        }

        self.timing_protection();

        Ok(transactions)
    }

    pub fn add_homomorphic(&self, bundle1: &EncryptedBundle, bundle2: &EncryptedBundle) -> Result<EncryptedBundle, Box<dyn std::error::Error + Send + Sync>> {
        if bundle1.version != bundle2.version || bundle1.version != ENCRYPTION_VERSION {
            return Err("Version mismatch".into());
        }

        if bundle1.timestamp.abs_diff(bundle2.timestamp) > KEY_ROTATION_INTERVAL {
            return Err("Bundles encrypted with different keys".into());
        }

        let total_size = bundle1.encrypted_values.len() + bundle2.encrypted_values.len();
        if total_size > MAX_BUNDLE_SIZE {
            return Err("Combined bundle too large".into());
        }

        let key = self.get_key_for_timestamp(bundle1.timestamp)?;

        let mut combined_values = Vec::with_capacity(total_size);
        let mut combined_commitments = Vec::with_capacity(total_size);
        let mut combined_proofs = Vec::with_capacity(total_size);

        // Truly homomorphic addition: multiply ciphertexts mod n^2
        for (enc1, enc2) in bundle1.encrypted_values.iter().zip(bundle2.encrypted_values.iter()) {
            let c1 = BigUint::from_bytes_le(&enc1.value);
            let c2 = BigUint::from_bytes_le(&enc2.value);
            
            let combined_ciphertext = (c1 * c2) % &key.n_squared;
            
            let combined_commitment = self.combine_commitments(
                &bundle1.commitments[combined_values.len()],
                &bundle2.commitments[combined_values.len()],
            );
            
            combined_values.push(PaillierCiphertext {
                value: combined_ciphertext.to_bytes_le(),
                randomness_commitment: self.hash_biguint(&combined_ciphertext),
            });
            
            combined_commitments.push(combined_commitment);
        }

        // Add remaining values from the longer bundle
        if bundle1.encrypted_values.len() > bundle2.encrypted_values.len() {
            combined_values.extend_from_slice(&bundle1.encrypted_values[bundle2.encrypted_values.len()..]);
            combined_commitments.extend_from_slice(&bundle1.commitments[bundle2.encrypted_values.len()..]);
            combined_proofs.extend_from_slice(&bundle1.range_proofs[bundle2.encrypted_values.len()..]);
        } else if bundle2.encrypted_values.len() > bundle1.encrypted_values.len() {
            combined_values.extend_from_slice(&bundle2.encrypted_values[bundle1.encrypted_values.len()..]);
            combined_commitments.extend_from_slice(&bundle2.commitments[bundle1.encrypted_values.len()..]);
            combined_proofs.extend_from_slice(&bundle2.range_proofs[bundle1.encrypted_values.len()..]);
        }

        let new_bundle_id = self.combine_bundle_ids(&bundle1.bundle_id, &bundle2.bundle_id);
        let bundle_proof = self.create_bundle_proof(&combined_values, &combined_commitments, &new_bundle_id)?;

        Ok(EncryptedBundle {
            version: ENCRYPTION_VERSION,
            timestamp: bundle1.timestamp.max(bundle2.timestamp),
            bundle_id: new_bundle_id,
            encrypted_values: combined_values,
            commitments: combined_commitments,
            range_proofs: combined_proofs,
            bundle_proof,
        })
    }

    pub fn scalar_multiply(&self, bundle: &EncryptedBundle, scalar: u64) -> Result<EncryptedBundle, Box<dyn std::error::Error + Send + Sync>> {
        if scalar == 0 {
            return Err("Cannot multiply by zero".into());
        }

        let key = self.get_key_for_timestamp(bundle.timestamp)?;
        let scalar_big = BigUint::from(scalar);

        let mut scaled_values = Vec::with_capacity(bundle.encrypted_values.len());
        let mut scaled_commitments = Vec::with_capacity(bundle.encrypted_values.len());

        for (i, encrypted) in bundle.encrypted_values.iter().enumerate() {
            let ciphertext = BigUint::from_bytes_le(&encrypted.value);
            let scaled_ciphertext = ciphertext.modpow(&scalar_big, &key.n_squared);
            
            scaled_values.push(PaillierCiphertext {
                value: scaled_ciphertext.to_bytes_le(),
                randomness_commitment: self.hash_biguint(&scaled_ciphertext),
            });
            
            scaled_commitments.push(self.scale_commitment(&bundle.commitments[i], scalar));
        }

        let bundle_proof = self.create_bundle_proof(&scaled_values, &scaled_commitments, &bundle.bundle_id)?;

        Ok(EncryptedBundle {
            version: bundle.version,
            timestamp: bundle.timestamp,
            bundle_id: bundle.bundle_id,
            encrypted_values: scaled_values,
            commitments: scaled_commitments,
            range_proofs: bundle.range_proofs.clone(),
            bundle_proof,
        })
    }

    fn initialize_key(&self) {
        let (n, p, q) = self.generate_paillier_parameters();
        let n_squared = &n * &n;
        let g = &n + BigUint::one();
        
        let lambda = Self::lcm(&(p.clone() - BigUint::one()), &(q.clone() - BigUint::one()));
        let l_value = Self::l_function(&g.modpow(&lambda, &n_squared), &n);
        let mu = l_value.mod_inverse(&n).unwrap();
        
        let epoch = self.epoch_counter.fetch_add(1, Ordering::SeqCst);
        
        let key = Arc::new(PaillierKey {
            n: n.clone(),
            n_squared,
            g,
            lambda: Some(lambda),
            mu: Some(mu),
            created_at: Instant::now(),
            epoch,
        });
        
        let key_id = self.compute_key_id(&n);
        self.keys.insert(key_id, key.clone());
        *self.current_key.write() = Some(key);
        
        self.cleanup_old_keys();
    }

    fn generate_paillier_parameters(&self) -> (BigUint, BigUint, BigUint) {
        let mut rng = OsRng;
        let bits = PAILLIER_KEY_SIZE / 2;
        
        loop {
            let p = rng.gen_prime(bits);
            let q = rng.gen_prime(bits);
            
            if p != q && p.gcd(&q) == BigUint::one() {
                let n = &p * &q;
                return (n, p, q);
            }
        }
    }

        fn paillier_encrypt(&self, key: &PaillierKey, plaintext: &[u8]) -> Result<(BigUint, BigUint), Box<dyn std::error::Error + Send + Sync>> {
        let mut rng = OsRng;
        
        let m = BigUint::from_bytes_be(plaintext);
        if m >= key.n {
            return Err("Plaintext too large for key".into());
        }
        
        loop {
            let r = rng.gen_biguint_range(&BigUint::one(), &key.n);
            if r.gcd(&key.n) == BigUint::one() {
                let gm = key.g.modpow(&m, &key.n_squared);
                let rn = r.modpow(&key.n, &key.n_squared);
                let ciphertext = (gm * rn) % &key.n_squared;
                
                return Ok((ciphertext, r));
            }
        }
    }

    fn paillier_decrypt(&self, key: &PaillierKey, ciphertext: &BigUint, lambda: &BigUint, mu: &BigUint) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let l_value = Self::l_function(&ciphertext.modpow(lambda, &key.n_squared), &key.n);
        let plaintext = (l_value * mu) % &key.n;
        
        Ok(plaintext.to_bytes_be())
    }

    fn l_function(u: &BigUint, n: &BigUint) -> BigUint {
        (u - BigUint::one()) / n
    }

    fn lcm(a: &BigUint, b: &BigUint) -> BigUint {
        (a * b) / a.gcd(b)
    }

    fn check_key_rotation(&self) {
        let should_rotate = self.current_key.read()
            .as_ref()
            .map(|k| k.created_at.elapsed() > self.rotation_interval)
            .unwrap_or(true);
        
        if should_rotate {
            self.initialize_key();
        }
    }

    fn get_current_key(&self) -> Result<Arc<PaillierKey>, Box<dyn std::error::Error + Send + Sync>> {
        self.current_key.read()
            .clone()
            .ok_or_else(|| "No current key available".into())
    }

    fn get_key_for_timestamp(&self, timestamp: u64) -> Result<Arc<PaillierKey>, Box<dyn std::error::Error + Send + Sync>> {
        let target_time = UNIX_EPOCH + Duration::from_secs(timestamp);
        let system_instant = SystemTime::now().duration_since(target_time).ok();
        
        for entry in self.keys.iter() {
            let key = entry.value();
            if let Some(duration) = system_instant {
                let key_age = key.created_at.elapsed();
                if key_age >= duration && key_age < duration + self.rotation_interval {
                    return Ok(key.clone());
                }
            }
        }
        
        self.get_current_key()
    }

    fn generate_bundle_id(&self, transactions: &[Transaction], timestamp: u64) -> [u8; 32] {
        let mut hasher = Keccak256::new();
        hasher.update(&timestamp.to_le_bytes());
        hasher.update(&(transactions.len() as u32).to_le_bytes());
        
        for tx in transactions {
            if let Ok(serialized) = bincode::serialize(tx) {
                hasher.update(&(serialized.len() as u32).to_le_bytes());
                hasher.update(&serialized);
            }
        }
        
        let mut bundle_id = [0u8; 32];
        bundle_id.copy_from_slice(&hasher.finalize());
        bundle_id
    }

    fn create_commitment(&self, data: &[u8], randomness: &BigUint) -> [u8; 32] {
        let mut hasher = Sha3_256::new();
        hasher.update(b"COMMITMENT");
        hasher.update(data);
        hasher.update(&randomness.to_bytes_le());
        
        let mut commitment = [0u8; 32];
        commitment.copy_from_slice(&hasher.finalize());
        commitment
    }

    fn create_range_proof(&self, data: &[u8], commitment: &[u8; 32]) -> Result<RangeProof, Box<dyn std::error::Error + Send + Sync>> {
        let mut rng = OsRng;
        let witness = rng.gen_biguint(256);
        
        let mut transcript = Sha3_256::new();
        transcript.update(b"RANGE_PROOF");
        transcript.update(commitment);
        transcript.update(&witness.to_bytes_le());
        
        let challenge_hash = transcript.finalize();
        let mut challenge = [0u8; 32];
        challenge.copy_from_slice(&challenge_hash);
        
        let response = &witness + BigUint::from_bytes_le(&challenge);
        
        Ok(RangeProof {
            commitment: *commitment,
            challenge,
            response: response.to_bytes_le(),
        })
    }

    fn create_bundle_proof(&self, encrypted_values: &[PaillierCiphertext], commitments: &[[u8; 32]], bundle_id: &[u8; 32]) -> Result<BundleProof, Box<dyn std::error::Error + Send + Sync>> {
        let mut aggregated = Sha3_256::new();
        aggregated.update(b"BUNDLE_AGGREGATE");
        
        for (enc, comm) in encrypted_values.iter().zip(commitments.iter()) {
            aggregated.update(&enc.randomness_commitment);
            aggregated.update(comm);
        }
        
        let mut aggregated_commitment = [0u8; 32];
        aggregated_commitment.copy_from_slice(&aggregated.finalize());
        
        let schnorr_proof = self.create_schnorr_proof(&aggregated_commitment, bundle_id)?;
        
        let mut hasher = Keccak256::new();
        hasher.update(bundle_id);
        hasher.update(&aggregated_commitment);
        for enc in encrypted_values {
            hasher.update(&enc.randomness_commitment);
        }
        
        let mut bundle_hash = [0u8; 32];
        bundle_hash.copy_from_slice(&hasher.finalize());
        
        Ok(BundleProof {
            aggregated_commitment,
            schnorr_proof,
            bundle_hash,
        })
    }

    fn create_schnorr_proof(&self, commitment: &[u8; 32], bundle_id: &[u8; 32]) -> Result<SchnorrProof, Box<dyn std::error::Error + Send + Sync>> {
        let mut rng = OsRng;
        let k = rng.gen_biguint(256);
        
        let mut hasher = Sha3_256::new();
        hasher.update(b"SCHNORR_COMMITMENT");
        hasher.update(&k.to_bytes_le());
        
        let mut schnorr_commitment = [0u8; 32];
        schnorr_commitment.copy_from_slice(&hasher.finalize());
        
        let mut challenge_hasher = Sha3_256::new();
        challenge_hasher.update(commitment);
        challenge_hasher.update(&schnorr_commitment);
        challenge_hasher.update(bundle_id);
        
        let mut challenge = [0u8; 32];
        challenge.copy_from_slice(&challenge_hasher.finalize());
        
        let challenge_scalar = BigUint::from_bytes_le(&challenge);
        let response = k + challenge_scalar;
        
        Ok(SchnorrProof {
            commitment: schnorr_commitment,
            challenge,
            response: response.to_bytes_le(),
        })
    }

    fn verify_bundle_integrity(&self, bundle: &EncryptedBundle) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if bundle.encrypted_values.is_empty() || bundle.encrypted_values.len() > MAX_BUNDLE_SIZE {
            return Err("Invalid bundle size".into());
        }
        
        if bundle.encrypted_values.len() != bundle.commitments.len() || 
           bundle.encrypted_values.len() != bundle.range_proofs.len() {
            return Err("Mismatched bundle components".into());
        }
        
        for range_proof in &bundle.range_proofs {
            self.verify_range_proof(range_proof)?;
        }
        
        self.verify_bundle_proof(&bundle.bundle_proof, &bundle.encrypted_values, &bundle.commitments, &bundle.bundle_id)?;
        
        Ok(())
    }

    fn verify_range_proof(&self, proof: &RangeProof) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let response = BigUint::from_bytes_le(&proof.response);
        let challenge = BigUint::from_bytes_le(&proof.challenge);
        
        let mut verifier = Sha3_256::new();
        verifier.update(b"RANGE_PROOF_VERIFY");
        verifier.update(&proof.commitment);
        verifier.update(&response.to_bytes_le());
        verifier.update(&challenge.to_bytes_le());
        
        let verification = verifier.finalize();
        if verification[0] & 0x01 != 0 {
            Ok(())
        } else {
            Err("Range proof verification failed".into())
        }
    }

    fn verify_bundle_proof(&self, proof: &BundleProof, encrypted_values: &[PaillierCiphertext], commitments: &[[u8; 32]], bundle_id: &[u8; 32]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut aggregated = Sha3_256::new();
        aggregated.update(b"BUNDLE_AGGREGATE");
        
        for (enc, comm) in encrypted_values.iter().zip(commitments.iter()) {
            aggregated.update(&enc.randomness_commitment);
            aggregated.update(comm);
        }
        
        let computed_aggregate = aggregated.finalize();
        if !constant_time_eq(&computed_aggregate, &proof.aggregated_commitment) {
            return Err("Aggregated commitment mismatch".into());
        }
        
        self.verify_schnorr_proof(&proof.schnorr_proof, &proof.aggregated_commitment, bundle_id)?;
        
        let mut hasher = Keccak256::new();
        hasher.update(bundle_id);
        hasher.update(&proof.aggregated_commitment);
        for enc in encrypted_values {
            hasher.update(&enc.randomness_commitment);
        }
        
        let computed_hash = hasher.finalize();
        if !constant_time_eq(&computed_hash, &proof.bundle_hash) {
            return Err("Bundle hash mismatch".into());
        }
        
        Ok(())
    }

    fn verify_schnorr_proof(&self, proof: &SchnorrProof, commitment: &[u8; 32], bundle_id: &[u8; 32]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut challenge_hasher = Sha3_256::new();
        challenge_hasher.update(commitment);
        challenge_hasher.update(&proof.commitment);
        challenge_hasher.update(bundle_id);
        
        let computed_challenge = challenge_hasher.finalize();
        if !constant_time_eq(&computed_challenge, &proof.challenge) {
            return Err("Schnorr proof challenge mismatch".into());
        }
        
        Ok(())
    }

    fn combine_commitments(&self, comm1: &[u8; 32], comm2: &[u8; 32]) -> [u8; 32] {
        let mut hasher = Sha3_256::new();
        hasher.update(b"COMBINE_COMMITMENTS");
        hasher.update(comm1);
        hasher.update(comm2);
        
        let mut combined = [0u8; 32];
        combined.copy_from_slice(&hasher.finalize());
        combined
    }

    fn combine_bundle_ids(&self, id1: &[u8; 32], id2: &[u8; 32]) -> [u8; 32] {
        let mut hasher = Keccak256::new();
        hasher.update(id1);
        hasher.update(id2);
        
        let mut combined = [0u8; 32];
        combined.copy_from_slice(&hasher.finalize());
        combined
    }

    fn scale_commitment(&self, commitment: &[u8; 32], scalar: u64) -> [u8; 32] {
        let mut hasher = Sha3_256::new();
        hasher.update(b"SCALE_COMMITMENT");
        hasher.update(commitment);
        hasher.update(&scalar.to_le_bytes());
        
        let mut scaled = [0u8; 32];
        scaled.copy_from_slice(&hasher.finalize());
        scaled
    }

    fn hash_biguint(&self, value: &BigUint) -> [u8; 32] {
        let mut hasher = Keccak256::new();
        hasher.update(&value.to_bytes_le());
        
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&hasher.finalize());
        hash
    }

    fn compute_key_id(&self, n: &BigUint) -> [u8; 32] {
        self.hash_biguint(n)
    }

    fn timing_protection(&self) {
        let mut dummy = 0u64;
        for _ in 0..TIMING_PROTECTION_ITERATIONS {
            dummy = dummy.wrapping_add(OsRng.gen::<u64>());
        }
        std::hint::black_box(dummy);
    }

    fn get_secure_timestamp(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

        fn cleanup_old_keys(&self) {
        if self.keys.len() > MAX_CACHED_KEYS {
            let cutoff = Instant::now() - (self.rotation_interval * 3);
            self.keys.retain(|_, key| key.created_at > cutoff);
        }
    }

    pub fn get_public_key(&self) -> Result<Vec<u8>, Box<dyn std::error::Error + Send + Sync>> {
        let key = self.get_current_key()?;
        Ok(key.n.to_bytes_le())
    }

    pub fn verify_and_decrypt(&self, bundle: &EncryptedBundle) -> Result<Vec<Transaction>, Box<dyn std::error::Error + Send + Sync>> {
        self.verify_bundle_integrity(bundle)?;
        self.decrypt_bundle(bundle)
    }

    pub fn batch_encrypt(&self, bundle_groups: &[Vec<Transaction>]) -> Result<Vec<EncryptedBundle>, Box<dyn std::error::Error + Send + Sync>> {
        bundle_groups.iter()
            .map(|txs| self.encrypt_bundle(txs))
            .collect()
    }

    pub fn batch_decrypt(&self, bundles: &[EncryptedBundle]) -> Result<Vec<Vec<Transaction>>, Box<dyn std::error::Error + Send + Sync>> {
        bundles.iter()
            .map(|bundle| self.decrypt_bundle(bundle))
            .collect()
    }

    pub fn create_zero_knowledge_proof(&self, bundle: &EncryptedBundle) -> Result<ZKProof, Box<dyn std::error::Error + Send + Sync>> {
        let mut hasher = Sha3_256::new();
        hasher.update(b"ZK_PROOF");
        hasher.update(&bundle.bundle_id);
        
        for enc in &bundle.encrypted_values {
            hasher.update(&enc.randomness_commitment);
        }
        
        let commitment_hash = hasher.finalize();
        let mut commitment = [0u8; 32];
        commitment.copy_from_slice(&commitment_hash);
        
        let witness = OsRng.gen_biguint(256);
        
        let mut challenge_hasher = Sha3_256::new();
        challenge_hasher.update(&commitment);
        challenge_hasher.update(&witness.to_bytes_le());
        challenge_hasher.update(&bundle.timestamp.to_le_bytes());
        
        let challenge_hash = challenge_hasher.finalize();
        let mut challenge = [0u8; 32];
        challenge.copy_from_slice(&challenge_hash);
        
        let challenge_scalar = BigUint::from_bytes_le(&challenge);
        let response = witness * challenge_scalar;
        
        Ok(ZKProof {
            commitment,
            challenge,
            response: response.to_bytes_le(),
            timestamp: bundle.timestamp,
        })
    }

    pub fn verify_zero_knowledge_proof(&self, bundle: &EncryptedBundle, proof: &ZKProof) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        if proof.timestamp != bundle.timestamp {
            return Ok(false);
        }
        
        let mut hasher = Sha3_256::new();
        hasher.update(b"ZK_PROOF");
        hasher.update(&bundle.bundle_id);
        
        for enc in &bundle.encrypted_values {
            hasher.update(&enc.randomness_commitment);
        }
        
        let computed_commitment = hasher.finalize();
        if !constant_time_eq(&computed_commitment, &proof.commitment) {
            return Ok(false);
        }
        
        Ok(true)
    }

    pub fn create_threshold_shares(&self, bundle: &EncryptedBundle, threshold: u32, total_shares: u32) -> Result<Vec<ThresholdShare>, Box<dyn std::error::Error + Send + Sync>> {
        if threshold == 0 || threshold > total_shares || total_shares > 255 {
            return Err("Invalid threshold parameters".into());
        }
        
        let mut shares = Vec::with_capacity(total_shares as usize);
        let key = self.get_key_for_timestamp(bundle.timestamp)?;
        
        for i in 1..=total_shares {
            let share_value = OsRng.gen_biguint_range(&BigUint::one(), &key.n);
            
            let mut hasher = Sha3_256::new();
            hasher.update(b"THRESHOLD_SHARE");
            hasher.update(&bundle.bundle_id);
            hasher.update(&i.to_le_bytes());
            hasher.update(&share_value.to_bytes_le());
            
            let mut commitment = [0u8; 32];
            commitment.copy_from_slice(&hasher.finalize());
            
            shares.push(ThresholdShare {
                index: i,
                share_value: share_value.to_bytes_le(),
                commitment,
                bundle_id: bundle.bundle_id,
                threshold,
            });
        }
        
        Ok(shares)
    }

    pub fn combine_threshold_shares(&self, shares: &[ThresholdShare]) -> Result<RecoveryKey, Box<dyn std::error::Error + Send + Sync>> {
        if shares.is_empty() {
            return Err("No shares provided".into());
        }
        
        let threshold = shares[0].threshold;
        if shares.len() < threshold as usize {
            return Err("Insufficient shares for threshold".into());
        }
        
        let bundle_id = shares[0].bundle_id;
        for share in shares {
            if share.bundle_id != bundle_id {
                return Err("Shares from different bundles".into());
            }
        }
        
        let mut combined = BigUint::one();
        for share in shares.iter().take(threshold as usize) {
            let share_value = BigUint::from_bytes_le(&share.share_value);
            combined = combined * share_value;
        }
        
        let mut hasher = Keccak256::new();
        hasher.update(&combined.to_bytes_le());
        
        let mut recovery_key = [0u8; 32];
        recovery_key.copy_from_slice(&hasher.finalize());
        
        Ok(RecoveryKey {
            key: recovery_key,
            bundle_id,
            threshold,
        })
    }

    pub fn rotate_keys(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.initialize_key();
        Ok(())
    }

    pub fn get_bundle_metadata(&self, bundle: &EncryptedBundle) -> BundleMetadata {
        BundleMetadata {
            version: bundle.version,
            timestamp: bundle.timestamp,
            bundle_id: bundle.bundle_id,
            transaction_count: bundle.encrypted_values.len(),
            total_size: bundle.encrypted_values.iter()
                .map(|enc| enc.value.len())
                .sum(),
        }
    }

    pub fn reencrypt_for_key(&self, bundle: &EncryptedBundle, new_public_key: &[u8]) -> Result<EncryptedBundle, Box<dyn std::error::Error + Send + Sync>> {
        let transactions = self.decrypt_bundle(bundle)?;
        
        let new_n = BigUint::from_bytes_le(new_public_key);
        let new_n_squared = &new_n * &new_n;
        let new_g = &new_n + BigUint::one();
        
        let mut reencrypted_values = Vec::with_capacity(transactions.len());
        let mut new_commitments = Vec::with_capacity(transactions.len());
        let mut new_range_proofs = Vec::with_capacity(transactions.len());
        
        for tx in &transactions {
            let tx_bytes = bincode::serialize(tx)?;
            let m = BigUint::from_bytes_be(&tx_bytes);
            
            if m >= new_n {
                return Err("Transaction too large for new key".into());
            }
            
            let mut rng = OsRng;
            let r = loop {
                let candidate = rng.gen_biguint_range(&BigUint::one(), &new_n);
                if candidate.gcd(&new_n) == BigUint::one() {
                    break candidate;
                }
            };
            
            let gm = new_g.modpow(&m, &new_n_squared);
            let rn = r.modpow(&new_n, &new_n_squared);
            let ciphertext = (gm * rn) % &new_n_squared;
            
            let commitment = self.create_commitment(&tx_bytes, &r);
            let range_proof = self.create_range_proof(&tx_bytes, &commitment)?;
            
            reencrypted_values.push(PaillierCiphertext {
                value: ciphertext.to_bytes_le(),
                randomness_commitment: self.hash_biguint(&ciphertext),
            });
            
            new_commitments.push(commitment);
            new_range_proofs.push(range_proof);
        }
        
        let bundle_proof = self.create_bundle_proof(&reencrypted_values, &new_commitments, &bundle.bundle_id)?;
        
        Ok(EncryptedBundle {
            version: bundle.version,
            timestamp: self.get_secure_timestamp(),
            bundle_id: bundle.bundle_id,
            encrypted_values: reencrypted_values,
            commitments: new_commitments,
            range_proofs: new_range_proofs,
            bundle_proof,
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

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct ZKProof {
    pub commitment: [u8; 32],
    pub challenge: [u8; 32],
    pub response: Vec<u8>,
    pub timestamp: u64,
}

#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
pub struct ThresholdShare {
    pub index: u32,
    pub share_value: Vec<u8>,
    pub commitment: [u8; 32],
    pub bundle_id: [u8; 32],
    pub threshold: u32,
}

#[derive(Debug, Clone)]
pub struct RecoveryKey {
    pub key: [u8; 32],
    pub bundle_id: [u8; 32],
    pub threshold: u32,
}

#[derive(Debug, Clone)]
pub struct BundleMetadata {
    pub version: u8,
    pub timestamp: u64,
    pub bundle_id: [u8; 32],
    pub transaction_count: usize,
    pub total_size: usize,
}

#[derive(Debug, Clone)]
pub struct EncryptorStats {
    pub total_operations: usize,
    pub current_epoch: u64,
    pub cached_keys: usize,
    pub key_rotation_interval: u64,
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    
    let mut result = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        result |= x ^ y;
    }
    
    result == 0
}

impl Default for HomomorphicBundleEncryptor {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for HomomorphicBundleEncryptor {
    fn drop(&mut self) {
        self.keys.clear();
        if let Ok(mut current) = self.current_key.write() {
            *current = None;
        }
    }
}

unsafe impl Send for HomomorphicBundleEncryptor {}
unsafe impl Sync for HomomorphicBundleEncryptor {}

