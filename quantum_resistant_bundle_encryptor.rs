use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{RwLock, Semaphore};
use dashmap::DashMap;
use parking_lot::Mutex;
use solana_sdk::{
    signature::Keypair,
    transaction::Transaction,
    pubkey::Pubkey,
    hash::Hash,
};
use bincode::{serialize, deserialize};
use serde::{Serialize, Deserialize};
use blake3::Hasher;
use chacha20poly1305::{
    aead::{Aead, KeyInit, OsRng},
    ChaCha20Poly1305, Nonce,
};
use pqcrypto_kyber::kyber768;
use pqcrypto_dilithium::dilithium3;
use rand::{RngCore, SeedableRng};
use rand_chacha::ChaCha20Rng;
use zeroize::Zeroize;
use thiserror::Error;
use crossbeam_queue::ArrayQueue;
use bytes::{Bytes, BytesMut};

const BUNDLE_VERSION: u8 = 1;
const MAX_BUNDLE_SIZE: usize = 232 * 1024;
const NONCE_SIZE: usize = 12;
const KEY_ROTATION_INTERVAL: u64 = 300;
const MAX_CACHED_KEYS: usize = 2048;
const BUNDLE_EXPIRY_MS: u64 = 1500;
const CIPHER_KEY_SIZE: usize = 32;
const MAX_CONCURRENT_OPERATIONS: usize = 256;
const CACHE_CLEANUP_INTERVAL: u64 = 60;
const METRICS_INTERVAL: u64 = 10;

#[derive(Error, Debug)]
pub enum EncryptorError {
    #[error("Serialization failed: {0}")]
    SerializationError(String),
    #[error("Encryption failed: {0}")]
    EncryptionError(String),
    #[error("Key generation failed: {0}")]
    KeyGenerationError(String),
    #[error("Bundle too large: {0} bytes")]
    BundleTooLarge(usize),
    #[error("Invalid bundle format")]
    InvalidBundleFormat,
    #[error("Bundle expired")]
    BundleExpired,
    #[error("Decryption failed")]
    DecryptionFailed,
    #[error("Signature verification failed")]
    SignatureVerificationFailed,
    #[error("Concurrent operation limit exceeded")]
    ConcurrencyLimitExceeded,
    #[error("Invalid key material")]
    InvalidKeyMaterial,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct EncryptedBundle {
    pub version: u8,
    pub timestamp: u64,
    pub sender_pubkey: Bytes,
    pub kyber_ciphertext: Bytes,
    pub encrypted_payload: Bytes,
    pub nonce: [u8; NONCE_SIZE],
    pub signature: Bytes,
    pub bundle_hash: [u8; 32],
    pub priority_class: u8,
}

#[derive(Serialize, Deserialize)]
struct BundlePayload {
    pub transactions: Vec<Transaction>,
    pub priority_fee: u64,
    pub max_bundle_tip: u64,
    pub target_slot: u64,
    pub expiry_ms: u64,
    pub sender_pubkey: Pubkey,
    pub bundle_sequence: u64,
}

struct KeyMaterial {
    kyber_pk: kyber768::PublicKey,
    kyber_sk: kyber768::SecretKey,
    dilithium_pk: dilithium3::PublicKey,
    dilithium_sk: dilithium3::SecretKey,
    created_at: u64,
}

pub struct QuantumResistantBundleEncryptor {
    current_keys: Arc<RwLock<KeyMaterial>>,
    previous_keys: Arc<RwLock<Option<KeyMaterial>>>,
    key_cache: Arc<DashMap<[u8; 32], (Bytes, u64)>>,
    operation_semaphore: Arc<Semaphore>,
    rng: Arc<Mutex<ChaCha20Rng>>,
    bundle_counter: Arc<RwLock<u64>>,
    metrics: Arc<RwLock<EncryptorMetrics>>,
    cleanup_handle: Arc<RwLock<Option<tokio::task::JoinHandle<()>>>>,
}

#[derive(Default)]
struct EncryptorMetrics {
    total_encryptions: u64,
    total_decryptions: u64,
    failed_operations: u64,
    cache_hits: u64,
    cache_misses: u64,
    key_rotations: u64,
}

impl QuantumResistantBundleEncryptor {
    pub async fn new() -> Result<Arc<Self>, EncryptorError> {
        let mut seed = [0u8; 32];
        OsRng.fill_bytes(&mut seed);
        let rng = ChaCha20Rng::from_seed(seed);
        seed.zeroize();

        let key_material = Self::generate_key_material()?;

        let encryptor = Arc::new(Self {
            current_keys: Arc::new(RwLock::new(key_material)),
            previous_keys: Arc::new(RwLock::new(None)),
            key_cache: Arc::new(DashMap::new()),
            operation_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_OPERATIONS)),
            rng: Arc::new(Mutex::new(rng)),
            bundle_counter: Arc::new(RwLock::new(0)),
            metrics: Arc::new(RwLock::new(EncryptorMetrics::default())),
            cleanup_handle: Arc::new(RwLock::new(None)),
        });

        encryptor.start_background_tasks().await;
        Ok(encryptor)
    }

    pub async fn encrypt_bundle(
        &self,
        transactions: Vec<Transaction>,
        priority_fee: u64,
        max_bundle_tip: u64,
        target_slot: u64,
        sender_keypair: &Keypair,
        priority_class: u8,
    ) -> Result<EncryptedBundle, EncryptorError> {
        let _permit = self.operation_semaphore.acquire().await
            .map_err(|_| EncryptorError::ConcurrencyLimitExceeded)?;

        let timestamp = current_timestamp();
        let expiry_ms = timestamp + BUNDLE_EXPIRY_MS;
        
        let bundle_sequence = {
            let mut counter = self.bundle_counter.write().await;
            let seq = *counter;
            *counter = counter.wrapping_add(1);
            seq
        };

        let bundle_payload = BundlePayload {
            transactions,
            priority_fee,
            max_bundle_tip,
            target_slot,
            expiry_ms,
            sender_pubkey: sender_keypair.pubkey(),
            bundle_sequence,
        };

        let serialized_payload = serialize(&bundle_payload)
            .map_err(|e| EncryptorError::SerializationError(e.to_string()))?;
        
        if serialized_payload.len() > MAX_BUNDLE_SIZE {
            self.increment_failed_operations().await;
            return Err(EncryptorError::BundleTooLarge(serialized_payload.len()));
        }

        let keys = self.current_keys.read().await;
        let (kyber_ciphertext, shared_secret) = kyber768::encapsulate(&keys.kyber_pk);
        
        let cipher_key = self.derive_cipher_key(&shared_secret, timestamp, bundle_sequence);
        let cipher = ChaCha20Poly1305::new_from_slice(&cipher_key)
            .map_err(|_| {
                self.increment_failed_operations_sync();
                EncryptorError::EncryptionError("Invalid key size".to_string())
            })?;
        
        let mut nonce = [0u8; NONCE_SIZE];
        {
            let mut rng = self.rng.lock();
            rng.fill_bytes(&mut nonce);
        }
        
        let encrypted_payload = cipher
            .encrypt(&Nonce::from_slice(&nonce), serialized_payload.as_ref())
            .map_err(|_| {
                self.increment_failed_operations_sync();
                EncryptorError::EncryptionError("Encryption failed".to_string())
            })?;
        
        let bundle_hash = self.compute_bundle_hash(
            &kyber_ciphertext,
            &encrypted_payload,
            &nonce,
            timestamp,
            priority_class,
        );
        
        let signature = self.sign_bundle(&bundle_hash, &keys.dilithium_sk)?;
        
        self.cache_shared_secret(bundle_hash, shared_secret.as_bytes().to_vec(), timestamp).await;
        self.increment_encryptions().await;
        
        Ok(EncryptedBundle {
            version: BUNDLE_VERSION,
            timestamp,
            sender_pubkey: Bytes::copy_from_slice(keys.kyber_pk.as_bytes()),
            kyber_ciphertext: Bytes::copy_from_slice(kyber_ciphertext.as_bytes()),
            encrypted_payload: Bytes::from(encrypted_payload),
            nonce,
            signature: Bytes::copy_from_slice(signature.as_bytes()),
            bundle_hash,
            priority_class,
        })
    }

    pub async fn decrypt_bundle(
        &self,
        encrypted_bundle: &EncryptedBundle,
    ) -> Result<BundlePayload, EncryptorError> {
        let _permit = self.operation_semaphore.acquire().await
            .map_err(|_| EncryptorError::ConcurrencyLimitExceeded)?;

        if encrypted_bundle.version != BUNDLE_VERSION {
            self.increment_failed_operations().await;
            return Err(EncryptorError::InvalidBundleFormat);
        }
        
        let current_time = current_timestamp();
        if current_time > encrypted_bundle.timestamp + BUNDLE_EXPIRY_MS * 2 {
            self.increment_failed_operations().await;
            return Err(EncryptorError::BundleExpired);
        }
        
        let computed_hash = self.compute_bundle_hash(
            &encrypted_bundle.kyber_ciphertext,
            &encrypted_bundle.encrypted_payload,
            &encrypted_bundle.nonce,
            encrypted_bundle.timestamp,
            encrypted_bundle.priority_class,
        );
        
        if computed_hash != encrypted_bundle.bundle_hash {
            self.increment_failed_operations().await;
            return Err(EncryptorError::InvalidBundleFormat);
        }
        
        let shared_secret = if let Some(cached) = self.get_cached_secret(&encrypted_bundle.bundle_hash).await {
            self.increment_cache_hits().await;
            cached
        } else {
            self.increment_cache_misses().await;
            let kyber_ciphertext = kyber768::Ciphertext::from_bytes(&encrypted_bundle.kyber_ciphertext)
                .map_err(|_| {
                    self.increment_failed_operations_sync();
                    EncryptorError::InvalidBundleFormat
                })?;
            
            let shared_secret = self.try_decrypt_with_keys(&kyber_ciphertext).await?;
            self.cache_shared_secret(encrypted_bundle.bundle_hash, shared_secret.clone(), current_time).await;
            shared_secret
        };
        
        let bundle_payload: BundlePayload = deserialize(&encrypted_bundle.encrypted_payload)
            .and_then(|serialized: Vec<u8>| {
                let cipher_key = self.derive_cipher_key(
                    &shared_secret,
                    encrypted_bundle.timestamp,
                    0
                );
                let cipher = ChaCha20Poly1305::new_from_slice(&cipher_key).ok()?;
                
                cipher
                    .decrypt(
                        &Nonce::from_slice(&encrypted_bundle.nonce),
                        serialized.as_ref(),
                    )
                    .ok()
            })
            .and_then(|decrypted| deserialize(&decrypted).ok())
            .ok_or_else(|| {
                self.increment_failed_operations_sync();
                EncryptorError::DecryptionFailed
            })?;
        
        if bundle_payload.expiry_ms < current_time {
            self.increment_failed_operations().await;
            return Err(EncryptorError::BundleExpired);
        }
        
        self.increment_decryptions().await;
        Ok(bundle_payload)
    }

    pub async fn batch_encrypt_bundles(
        &self,
        requests: Vec<(Vec<Transaction>, u64, u64, u64, Keypair, u8)>,
    ) -> Vec<Result<EncryptedBundle, EncryptorError>> {
        let mut handles = Vec::with_capacity(requests.len());
        
        for (txs, fee, tip, slot, kp, priority) in requests {
            let self_clone = self.clone();
            let handle = tokio::spawn(async move {
                self_clone.encrypt_bundle(txs, fee, tip, slot, &kp, priority).await
            });
            handles.push(handle);
        }
        
        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(_) => results.push(Err(EncryptorError::EncryptionError("Task failed".to_string()))),
            }
        }
        
        results
    }

    pub async fn rotate_keys(&self) -> Result<(), EncryptorError> {
        let new_keys = Self::generate_key_material()?;
        let current = self.current_keys.read().await.clone();
        
        *self.previous_keys.write().await = Some(current);
        *self.current_keys.write().await = new_keys;
        
        self.metrics.write().await.key_rotations += 1;
        
        tokio::spawn({
            let previous = self.previous_keys.clone();
            async move {
                tokio::time::sleep(Duration::from_secs(KEY_ROTATION_INTERVAL * 2)).await;
                *previous.write().await = None;
            }
        });
        
        Ok(())
    }

        pub async fn get_public_keys(&self) -> (Bytes, Bytes) {
        let keys = self.current_keys.read().await;
        (
            Bytes::copy_from_slice(keys.kyber_pk.as_bytes()),
            Bytes::copy_from_slice(keys.dilithium_pk.as_bytes()),
        )
    }

    pub fn validate_bundle_integrity(&self, encrypted_bundle: &EncryptedBundle) -> bool {
        if encrypted_bundle.version != BUNDLE_VERSION {
            return false;
        }
        
        if encrypted_bundle.kyber_ciphertext.len() != kyber768::CIPHERTEXTBYTES {
            return false;
        }
        
        if encrypted_bundle.signature.len() != dilithium3::SIGNATUREBYTES {
            return false;
        }
        
        if encrypted_bundle.encrypted_payload.len() > MAX_BUNDLE_SIZE {
            return false;
        }
        
        if encrypted_bundle.priority_class > 3 {
            return false;
        }
        
        let computed_hash = self.compute_bundle_hash(
            &encrypted_bundle.kyber_ciphertext,
            &encrypted_bundle.encrypted_payload,
            &encrypted_bundle.nonce,
            encrypted_bundle.timestamp,
            encrypted_bundle.priority_class,
        );
        
        computed_hash == encrypted_bundle.bundle_hash
    }

    pub fn estimate_encrypted_size(&self, num_transactions: usize, avg_tx_size: usize) -> usize {
        let payload_size = num_transactions * avg_tx_size + 96;
        let overhead = kyber768::CIPHERTEXTBYTES + dilithium3::SIGNATUREBYTES + NONCE_SIZE + 80;
        payload_size + overhead
    }

    pub async fn get_metrics(&self) -> EncryptorMetrics {
        self.metrics.read().await.clone()
    }

    pub fn generate_bundle_id(&self, encrypted_bundle: &EncryptedBundle) -> [u8; 32] {
        let mut hasher = Hasher::new();
        hasher.update(&encrypted_bundle.bundle_hash);
        hasher.update(&encrypted_bundle.timestamp.to_le_bytes());
        hasher.update(&[encrypted_bundle.priority_class]);
        
        let hash = hasher.finalize();
        let mut result = [0u8; 32];
        result.copy_from_slice(hash.as_bytes());
        result
    }

    async fn try_decrypt_with_keys(&self, kyber_ciphertext: &kyber768::Ciphertext) -> Result<Vec<u8>, EncryptorError> {
        let current_keys = self.current_keys.read().await;
        match kyber768::decapsulate(kyber_ciphertext, &current_keys.kyber_sk) {
            shared_secret => return Ok(shared_secret.as_bytes().to_vec()),
        }
        
        if let Some(ref previous) = *self.previous_keys.read().await {
            match kyber768::decapsulate(kyber_ciphertext, &previous.kyber_sk) {
                shared_secret => return Ok(shared_secret.as_bytes().to_vec()),
            }
        }
        
        self.increment_failed_operations().await;
        Err(EncryptorError::DecryptionFailed)
    }

    fn derive_cipher_key(&self, shared_secret: &[u8], timestamp: u64, sequence: u64) -> [u8; CIPHER_KEY_SIZE] {
        let mut hasher = Hasher::new();
        hasher.update(b"SOLANA_MEV_QUANTUM_BUNDLE_V1");
        hasher.update(shared_secret);
        hasher.update(&timestamp.to_le_bytes());
        hasher.update(&sequence.to_le_bytes());
        
        let hash = hasher.finalize();
        let mut key = [0u8; CIPHER_KEY_SIZE];
        key.copy_from_slice(&hash.as_bytes()[..CIPHER_KEY_SIZE]);
        key
    }

    fn compute_bundle_hash(
        &self,
        kyber_ciphertext: &[u8],
        encrypted_payload: &[u8],
        nonce: &[u8],
        timestamp: u64,
        priority_class: u8,
    ) -> [u8; 32] {
        let mut hasher = Hasher::new();
        hasher.update(&[BUNDLE_VERSION]);
        hasher.update(&timestamp.to_le_bytes());
        hasher.update(&[priority_class]);
        hasher.update(kyber_ciphertext);
        hasher.update(encrypted_payload);
        hasher.update(nonce);
        
        let hash = hasher.finalize();
        let mut result = [0u8; 32];
        result.copy_from_slice(hash.as_bytes());
        result
    }

    fn sign_bundle(&self, bundle_hash: &[u8; 32], dilithium_sk: &dilithium3::SecretKey) -> Result<dilithium3::Signature, EncryptorError> {
        Ok(dilithium3::sign(bundle_hash, dilithium_sk))
    }

    async fn verify_signature(&self, bundle_hash: &[u8; 32], signature: &[u8], dilithium_pk: &dilithium3::PublicKey) -> bool {
        if let Ok(sig) = dilithium3::Signature::from_bytes(signature) {
            dilithium3::verify(&sig, bundle_hash, dilithium_pk).is_ok()
        } else {
            false
        }
    }

    async fn cache_shared_secret(&self, bundle_hash: [u8; 32], shared_secret: Vec<u8>, timestamp: u64) {
        if self.key_cache.len() >= MAX_CACHED_KEYS {
            self.cleanup_cache_sync();
        }
        self.key_cache.insert(bundle_hash, (Bytes::from(shared_secret), timestamp));
    }

    async fn get_cached_secret(&self, bundle_hash: &[u8; 32]) -> Option<Vec<u8>> {
        self.key_cache.get(bundle_hash).map(|entry| entry.0.to_vec())
    }

    fn cleanup_cache_sync(&self) {
        let current_time = current_timestamp();
        let expired_threshold = current_time.saturating_sub(BUNDLE_EXPIRY_MS * 4);
        
        self.key_cache.retain(|_, (_, timestamp)| *timestamp > expired_threshold);
    }

    async fn start_background_tasks(self: &Arc<Self>) {
        let cleanup_task = {
            let encryptor = self.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(CACHE_CLEANUP_INTERVAL));
                loop {
                    interval.tick().await;
                    encryptor.cleanup_cache_sync();
                    
                    let current_time = current_timestamp();
                    let keys = encryptor.current_keys.read().await;
                    if current_time.saturating_sub(keys.created_at) > KEY_ROTATION_INTERVAL * 1000 {
                        drop(keys);
                        let _ = encryptor.rotate_keys().await;
                    }
                }
            })
        };
        
        *self.cleanup_handle.write().await = Some(cleanup_task);
    }

    fn generate_key_material() -> Result<KeyMaterial, EncryptorError> {
        let (kyber_pk, kyber_sk) = kyber768::keypair();
        let (dilithium_pk, dilithium_sk) = dilithium3::keypair();
        
        Ok(KeyMaterial {
            kyber_pk,
            kyber_sk,
            dilithium_pk,
            dilithium_sk,
            created_at: current_timestamp(),
        })
    }

    async fn increment_encryptions(&self) {
        self.metrics.write().await.total_encryptions += 1;
    }

    async fn increment_decryptions(&self) {
        self.metrics.write().await.total_decryptions += 1;
    }

    async fn increment_failed_operations(&self) {
        self.metrics.write().await.failed_operations += 1;
    }

    fn increment_failed_operations_sync(&self) {
        if let Ok(mut metrics) = self.metrics.try_write() {
            metrics.failed_operations += 1;
        }
    }

    async fn increment_cache_hits(&self) {
        self.metrics.write().await.cache_hits += 1;
    }

    async fn increment_cache_misses(&self) {
        self.metrics.write().await.cache_misses += 1;
    }
}

impl Clone for QuantumResistantBundleEncryptor {
    fn clone(&self) -> Self {
        Self {
            current_keys: self.current_keys.clone(),
            previous_keys: self.previous_keys.clone(),
            key_cache: self.key_cache.clone(),
            operation_semaphore: self.operation_semaphore.clone(),
            rng: self.rng.clone(),
            bundle_counter: self.bundle_counter.clone(),
            metrics: self.metrics.clone(),
            cleanup_handle: self.cleanup_handle.clone(),
        }
    }
}

impl Clone for KeyMaterial {
    fn clone(&self) -> Self {
        Self {
            kyber_pk: self.kyber_pk.clone(),
            kyber_sk: self.kyber_sk.clone(),
            dilithium_pk: self.dilithium_pk.clone(),
            dilithium_sk: self.dilithium_sk.clone(),
            created_at: self.created_at,
        }
    }
}

impl Clone for EncryptorMetrics {
    fn clone(&self) -> Self {
        Self {
            total_encryptions: self.total_encryptions,
            total_decryptions: self.total_decryptions,
            failed_operations: self.failed_operations,
            cache_hits: self.cache_hits,
            cache_misses: self.cache_misses,
            key_rotations: self.key_rotations,
        }
    }
}

impl Drop for QuantumResistantBundleEncryptor {
    fn drop(&mut self) {
        if let Ok(mut handle) = self.cleanup_handle.try_write() {
            if let Some(task) = handle.take() {
                task.abort();
            }
        }
        
        self.key_cache.clear();
        
        if let Ok(mut keys) = self.current_keys.try_write() {
            zeroize_key_material(&mut keys);
        }
        
        if let Ok(mut previous) = self.previous_keys.try_write() {
            if let Some(ref mut keys) = *previous {
                zeroize_key_material(keys);
            }
        }
    }
}

fn zeroize_key_material(keys: &mut KeyMaterial) {
    let sk_bytes = keys.kyber_sk.as_bytes();
    let sk_slice = unsafe {
        std::slice::from_raw_parts_mut(sk_bytes.as_ptr() as *mut u8, sk_bytes.len())
    };
    sk_slice.zeroize();
    
    let dil_sk_bytes = keys.dilithium_sk.as_bytes();
    let dil_sk_slice = unsafe {
        std::slice::from_raw_parts_mut(dil_sk_bytes.as_ptr() as *mut u8, dil_sk_bytes.len())
    };
    dil_sk_slice.zeroize();
}

#[inline(always)]
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub async fn create_high_priority_encryptor() -> Result<Arc<QuantumResistantBundleEncryptor>, EncryptorError> {
    QuantumResistantBundleEncryptor::new().await
}

impl EncryptedBundle {
    #[inline]
    pub fn size(&self) -> usize {
        self.sender_pubkey.len() + 
        self.kyber_ciphertext.len() + 
        self.encrypted_payload.len() + 
        self.signature.len() + 
        NONCE_SIZE + 
        std::mem::size_of::<u8>() * 2 + 
        std::mem::size_of::<u64>() + 
        32
    }

    #[inline]
    pub fn is_expired(&self) -> bool {
        current_timestamp() > self.timestamp + BUNDLE_EXPIRY_MS
    }

    #[inline]
    pub fn remaining_ttl_ms(&self) -> Option<u64> {
        let current = current_timestamp();
        let expiry = self.timestamp + BUNDLE_EXPIRY_MS;
        if current < expiry {
            Some(expiry - current)
        } else {
            None
        }
    }
}

impl BundlePayload {
    #[inline]
    pub fn compute_total_fee(&self) -> u64 {
        self.priority_fee.saturating_add(self.max_bundle_tip)
    }

    #[inline]
    pub fn is_valid_for_slot(&self, current_slot: u64) -> bool {
        self.target_slot >= current_slot && self.target_slot <= current_slot + 4
    }
}

impl EncryptorMetrics {
    #[inline]
    pub fn success_rate(&self) -> f64 {
        let total_ops = self.total_encryptions + self.total_decryptions;
        if total_ops == 0 {
            1.0
        } else {
            1.0 - (self.failed_operations as f64 / total_ops as f64)
        }
    }

    #[inline]
    pub fn cache_hit_rate(&self) -> f64 {
        let total_lookups = self.cache_hits + self.cache_misses;
        if total_lookups == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total_lookups as f64
        }
    }
}

#[inline]
pub fn validate_priority_class(priority_class: u8) -> bool {
    priority_class <= 3
}

#[inline]
pub fn compute_bundle_score(priority_fee: u64, max_tip: u64, priority_class: u8) -> u64 {
    let base_score = priority_fee.saturating_add(max_tip);
    let class_multiplier = match priority_class {
        0 => 100,
        1 => 150,
        2 => 200,
        3 => 300,
        _ => 100,
    };
    base_score.saturating_mul(class_multiplier) / 100
}

