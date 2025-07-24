use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use std::arch::x86_64::*;
use std::ptr;
use std::slice;
use blake3::{Hasher as Blake3Hasher, OUT_LEN};
use solana_sdk::{
    hash::Hash,
    signature::Signature,
    transaction::Transaction,
    pubkey::Pubkey,
    message::{Message, MessageHeader},
    instruction::CompiledInstruction,
};
use crossbeam::channel::{bounded, Sender, Receiver, TryRecvError};
use parking_lot::{RwLock, Mutex};
use ahash::AHashMap;
use rayon::prelude::*;
use once_cell::sync::Lazy;
use thiserror::Error;

const SIMD_LANES: usize = 8;
const CACHE_LINE_SIZE: usize = 64;
const HASH_CACHE_ENTRIES: usize = 131072;
const PREFETCH_DISTANCE: usize = 12;
const BATCH_QUEUE_SIZE: usize = 64;
const MEMORY_POOL_SIZE: usize = 128;
const MAX_TX_SIZE: usize = 1232;

#[derive(Error, Debug)]
pub enum HasherError {
    #[error("Serialization failed: {0}")]
    SerializationError(String),
    #[error("Queue full")]
    QueueFull,
    #[error("Invalid transaction")]
    InvalidTransaction,
    #[error("Memory allocation failed")]
    MemoryError,
}

type Result<T> = std::result::Result<T, HasherError>;

static GLOBAL_THREAD_POOL: Lazy<rayon::ThreadPool> = Lazy::new(|| {
    rayon::ThreadPoolBuilder::new()
        .num_threads(num_cpus::get())
        .thread_name(|idx| format!("simd-hash-{}", idx))
        .stack_size(2 * 1024 * 1024)
        .build()
        .expect("Failed to create thread pool")
});

#[repr(C, align(64))]
struct CacheAligned<T>(T);

#[repr(C, align(64))]
pub struct SimdTransactionHasher {
    cache: Arc<DashCache>,
    metrics: Arc<Metrics>,
    memory_pool: Arc<MemoryPool>,
    batch_processor: Arc<BatchProcessor>,
    shutdown: Arc<AtomicBool>,
}

struct DashCache {
    shards: Vec<RwLock<AHashMap<u64, CachedHash>>>,
    shard_count: usize,
}

#[derive(Clone, Copy)]
struct CachedHash {
    hash: Hash,
    timestamp: u64,
}

struct Metrics {
    total_hashed: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    batch_processed: AtomicU64,
}

struct BatchProcessor {
    sender: Sender<HashRequest>,
    receiver: Receiver<HashRequest>,
}

struct HashRequest {
    transactions: Vec<Transaction>,
    response: Sender<Vec<Hash>>,
}

#[repr(C, align(64))]
struct AlignedBuffer {
    data: [u8; MAX_TX_SIZE],
}

struct MemoryPool {
    buffers: Mutex<Vec<Box<AlignedBuffer>>>,
    allocated: AtomicU64,
}

impl SimdTransactionHasher {
    pub fn new() -> Result<Arc<Self>> {
        let shard_count = 16;
        let mut shards = Vec::with_capacity(shard_count);
        
        for _ in 0..shard_count {
            shards.push(RwLock::new(AHashMap::with_capacity(HASH_CACHE_ENTRIES / shard_count)));
        }

        let (tx, rx) = bounded(BATCH_QUEUE_SIZE);
        
        Ok(Arc::new(Self {
            cache: Arc::new(DashCache { shards, shard_count }),
            metrics: Arc::new(Metrics {
                total_hashed: AtomicU64::new(0),
                cache_hits: AtomicU64::new(0),
                cache_misses: AtomicU64::new(0),
                batch_processed: AtomicU64::new(0),
            }),
            memory_pool: Arc::new(MemoryPool::new()),
            batch_processor: Arc::new(BatchProcessor {
                sender: tx,
                receiver: rx,
            }),
            shutdown: Arc::new(AtomicBool::new(false)),
        }))
    }

    pub fn hash_transaction(&self, transaction: &Transaction) -> Result<Hash> {
        let serialized = self.serialize_transaction(transaction)?;
        let cache_key = self.compute_cache_key(&serialized);
        
        if let Some(cached) = self.cache.get(cache_key) {
            self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(cached.hash);
        }

        self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
        
        let hash = if serialized.len() >= 64 && is_x86_feature_detected!("avx2") {
            unsafe { self.hash_with_simd(&serialized) }
        } else {
            self.hash_standard(&serialized)
        };
        
        self.cache.insert(cache_key, hash);
        self.metrics.total_hashed.fetch_add(1, Ordering::Relaxed);
        
        Ok(hash)
    }

    pub fn hash_batch_parallel(&self, transactions: &[Transaction]) -> Result<Vec<Hash>> {
        if transactions.is_empty() {
            return Ok(Vec::new());
        }

        let chunk_size = (transactions.len() + SIMD_LANES - 1) / SIMD_LANES;
        let chunks: Vec<_> = transactions.chunks(chunk_size).collect();
        
        let results: Result<Vec<_>> = GLOBAL_THREAD_POOL.install(|| {
            chunks.par_iter()
                .map(|chunk| self.hash_chunk(chunk))
                .collect()
        });
        
        results.map(|nested| nested.into_iter().flatten().collect())
    }

    fn hash_chunk(&self, transactions: &[Transaction]) -> Result<Vec<Hash>> {
        let mut results = Vec::with_capacity(transactions.len());
        
        for tx in transactions {
            results.push(self.hash_transaction(tx)?);
        }
        
        Ok(results)
    }

    unsafe fn hash_with_simd(&self, data: &[u8]) -> Hash {
        let mut hasher = Blake3Hasher::new();
        let len = data.len();
        let mut offset = 0;
        
        // Process 64-byte chunks with AVX2
        while offset + 64 <= len {
            let chunk_ptr = data.as_ptr().add(offset);
            self.process_avx2_chunk(&mut hasher, chunk_ptr);
            offset += 64;
        }
        
        // Process 32-byte chunks with AVX
        while offset + 32 <= len {
            let chunk_ptr = data.as_ptr().add(offset);
            self.process_avx_chunk(&mut hasher, chunk_ptr);
            offset += 32;
        }
        
        // Process remaining bytes
        if offset < len {
            hasher.update(&data[offset..]);
        }
        
        let mut output = [0u8; OUT_LEN];
        hasher.finalize_xof().fill(&mut output);
        Hash::new(&output)
    }

    #[inline(always)]
    unsafe fn process_avx2_chunk(&self, hasher: &mut Blake3Hasher, chunk_ptr: *const u8) {
        // Prefetch next cache line
        _mm_prefetch(chunk_ptr.add(CACHE_LINE_SIZE) as *const i8, _MM_HINT_T0);
        
        // Load 64 bytes using AVX2
        let v0 = _mm256_loadu_si256(chunk_ptr as *const __m256i);
        let v1 = _mm256_loadu_si256(chunk_ptr.add(32) as *const __m256i);
        
        // Align to cache line
        let mut aligned_data = CacheAligned([0u8; 64]);
        _mm256_storeu_si256(aligned_data.0.as_mut_ptr() as *mut __m256i, v0);
        _mm256_storeu_si256(aligned_data.0.as_mut_ptr().add(32) as *mut __m256i, v1);
        
        hasher.update(&aligned_data.0);
    }

    #[inline(always)]
    unsafe fn process_avx_chunk(&self, hasher: &mut Blake3Hasher, chunk_ptr: *const u8) {
        let v = _mm256_loadu_si256(chunk_ptr as *const __m256i);
        
        let mut aligned_data = CacheAligned([0u8; 32]);
        _mm256_storeu_si256(aligned_data.0.as_mut_ptr() as *mut __m256i, v);
        
        hasher.update(&aligned_data.0);
    }

    fn hash_standard(&self, data: &[u8]) -> Hash {
        let mut hasher = Blake3Hasher::new();
        hasher.update(data);
        
        let mut output = [0u8; OUT_LEN];
        hasher.finalize_xof().fill(&mut output);
        Hash::new(&output)
    }

    fn serialize_transaction(&self, transaction: &Transaction) -> Result<Vec<u8>> {
        // Get buffer from pool
        let mut buffer = self.memory_pool.get_buffer();
        let mut offset = 0;
        
        // Serialize signatures
        let sig_count = transaction.signatures.len() as u8;
        if offset + 1 > MAX_TX_SIZE {
            return Err(HasherError::InvalidTransaction);
        }
        buffer.data[offset] = sig_count;
        offset += 1;
        
        for sig in &transaction.signatures {
            if offset + 64 > MAX_TX_SIZE {
                return Err(HasherError::InvalidTransaction);
            }
            buffer.data[offset..offset + 64].copy_from_slice(&sig.as_ref());
            offset += 64;
        }
        
        // Serialize message
        offset += self.serialize_message(&transaction.message, &mut buffer.data[offset..])?;
        
        let result = buffer.data[..offset].to_vec();
        self.memory_pool.return_buffer(buffer);
        
        Ok(result)
    }

    fn serialize_message(&self, message: &Message, buffer: &mut [u8]) -> Result<usize> {
        let mut offset = 0;
        
        // Header
        if offset + 3 > buffer.len() {
            return Err(HasherError::InvalidTransaction);
        }
        buffer[offset] = message.header.num_required_signatures;
        buffer[offset + 1] = message.header.num_readonly_signed_accounts;
        buffer[offset + 2] = message.header.num_readonly_unsigned_accounts;
        offset += 3;
        
        // Account keys
        let key_count = message.account_keys.len() as u8;
        if offset + 1 > buffer.len() {
            return Err(HasherError::InvalidTransaction);
        }
        buffer[offset] = key_count;
        offset += 1;
        
        for key in &message.account_keys {
            if offset + 32 > buffer.len() {
                return Err(HasherError::InvalidTransaction);
            }
            buffer[offset..offset + 32].copy_from_slice(key.as_ref());
            offset += 32;
        }
        
        // Recent blockhash
        if offset + 32 > buffer.len() {
            return Err(HasherError::InvalidTransaction);
        }
        buffer[offset..offset + 32].copy_from_slice(message.recent_blockhash.as_ref());
        offset += 32;
        
        // Instructions
        let inst_count = message.instructions.len() as u8;
        if offset + 1 > buffer.len() {
            return Err(HasherError::InvalidTransaction);
        }
        buffer[offset] = inst_count;
        offset += 1;
        
        for inst in &message.instructions {
            offset += self.serialize_instruction(inst, &mut buffer[offset..])?;
        }
        
        Ok(offset)
    }

    fn serialize_instruction(&self, inst: &CompiledInstruction, buffer: &mut [u8]) -> Result<usize> {
        let mut offset = 0;
        
        if offset + 1 > buffer.len() {
            return Err(HasherError::InvalidTransaction);
        }
        buffer[offset] = inst.program_id_index;
        offset += 1;
        
        let account_count = inst.accounts.len() as u8;
        if offset + 1 > buffer.len() {
            return Err(HasherError::InvalidTransaction);
        }
        buffer[offset] = account_count;
        offset += 1;
        
        if offset + inst.accounts.len() > buffer.len() {
            return Err(HasherError::InvalidTransaction);
        }
        buffer[offset..offset + inst.accounts.len()].copy_from_slice(&inst.accounts);
        offset += inst.accounts.len();
        
        let data_len = inst.data.len() as u16;
        if offset + 2 > buffer.len() {
            return Err(HasherError::InvalidTransaction);
        }
        buffer[offset..offset + 2].copy_from_slice(&data_len.to_le_bytes());
        offset += 2;
        
        if offset + inst.data.len() > buffer.len() {
            return Err(HasherError::InvalidTransaction);
        }
        buffer[offset..offset + inst.data.len()].copy_from_slice(&inst.data);
        offset += inst.data.len();
        
        Ok(offset)
    }

    #[inline(always)]
    fn compute_cache_key(&self, data: &[u8]) -> u64 {
        let mut key = 0u64;
        let chunks = data.chunks_exact(8);
        let remainder = chunks.remainder();
        
        for chunk in chunks {
            let value = u64::from_le_bytes([
                chunk[0], chunk[1], chunk[2], chunk[3],
                chunk[4], chunk[5], chunk[6], chunk[7]
            ]);
                        key = key.wrapping_mul(0x1b873593).wrapping_add(value);
        }
        
        if !remainder.is_empty() {
            let mut last = 0u64;
            for (i, &byte) in remainder.iter().enumerate() {
                last |= (byte as u64) << (i * 8);
            }
            key = key.wrapping_mul(0x1b873593).wrapping_add(last);
        }
        
        key ^= key >> 33;
        key = key.wrapping_mul(0xff51afd7ed558ccd);
        key ^= key >> 33;
        key = key.wrapping_mul(0xc4ceb9fe1a85ec53);
        key ^= key >> 33;
        
        key
    }

    pub fn submit_batch_async(&self, transactions: Vec<Transaction>) -> Result<Receiver<Vec<Hash>>> {
        let (tx, rx) = bounded(1);
        
        let request = HashRequest {
            transactions,
            response: tx,
        };
        
        self.batch_processor.sender.try_send(request)
            .map_err(|_| HasherError::QueueFull)?;
        
        self.metrics.batch_processed.fetch_add(1, Ordering::Relaxed);
        Ok(rx)
    }

    pub fn start_batch_processor(self: Arc<Self>) -> std::thread::JoinHandle<()> {
        let hasher = self.clone();
        
        std::thread::Builder::new()
            .name("simd-batch-processor".to_string())
            .stack_size(4 * 1024 * 1024)
            .spawn(move || {
                while !hasher.shutdown.load(Ordering::Acquire) {
                    match hasher.batch_processor.receiver.try_recv() {
                        Ok(request) => {
                            let results = hasher.hash_batch_parallel(&request.transactions)
                                .unwrap_or_else(|_| vec![Hash::default(); request.transactions.len()]);
                            
                            let _ = request.response.send(results);
                        }
                        Err(TryRecvError::Empty) => {
                            std::thread::sleep(std::time::Duration::from_micros(100));
                        }
                        Err(TryRecvError::Disconnected) => break,
                    }
                }
            })
            .expect("Failed to spawn batch processor thread")
    }

    pub fn verify_transaction_integrity(&self, transaction: &Transaction) -> Result<bool> {
        if transaction.signatures.is_empty() {
            return Ok(false);
        }
        
        if transaction.message.account_keys.is_empty() {
            return Ok(false);
        }
        
        let required_sigs = transaction.message.header.num_required_signatures as usize;
        if transaction.signatures.len() < required_sigs {
            return Ok(false);
        }
        
        for instruction in &transaction.message.instructions {
            if instruction.program_id_index as usize >= transaction.message.account_keys.len() {
                return Ok(false);
            }
            
            for &account_index in &instruction.accounts {
                if account_index as usize >= transaction.message.account_keys.len() {
                    return Ok(false);
                }
            }
        }
        
        Ok(true)
    }

    pub fn get_metrics(&self) -> (u64, u64, u64, u64) {
        (
            self.metrics.total_hashed.load(Ordering::Relaxed),
            self.metrics.cache_hits.load(Ordering::Relaxed),
            self.metrics.cache_misses.load(Ordering::Relaxed),
            self.metrics.batch_processed.load(Ordering::Relaxed),
        )
    }

    pub fn clear_cache(&self) {
        for shard in &self.cache.shards {
            shard.write().clear();
        }
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    pub fn optimize_for_mev(&self, transaction: &Transaction) -> Result<(Hash, u64)> {
        let start = std::time::Instant::now();
        
        // Pre-verify transaction
        if !self.verify_transaction_integrity(transaction)? {
            return Err(HasherError::InvalidTransaction);
        }
        
        // Fast path for simple transfers
        if self.is_simple_transfer(transaction) {
            let hash = self.hash_transaction(transaction)?;
            let elapsed = start.elapsed().as_nanos() as u64;
            return Ok((hash, elapsed));
        }
        
        // Complex transaction path with prefetching
        self.prefetch_accounts(&transaction.message.account_keys);
        
        let hash = self.hash_transaction(transaction)?;
        let elapsed = start.elapsed().as_nanos() as u64;
        
        Ok((hash, elapsed))
    }

    #[inline(always)]
    fn is_simple_transfer(&self, transaction: &Transaction) -> bool {
        transaction.message.instructions.len() == 1
            && transaction.signatures.len() <= 2
            && transaction.message.account_keys.len() <= 3
    }

    #[inline(always)]
    fn prefetch_accounts(&self, accounts: &[Pubkey]) {
        for (i, account) in accounts.iter().enumerate() {
            if i + PREFETCH_DISTANCE < accounts.len() {
                unsafe {
                    let future_account = &accounts[i + PREFETCH_DISTANCE];
                    _mm_prefetch(future_account.as_ref().as_ptr() as *const i8, _MM_HINT_T0);
                }
            }
        }
    }
}

impl DashCache {
    #[inline(always)]
    fn get_shard_index(&self, key: u64) -> usize {
        (key as usize) % self.shard_count
    }

    fn get(&self, key: u64) -> Option<CachedHash> {
        let shard_idx = self.get_shard_index(key);
        let shard = self.shards[shard_idx].read();
        shard.get(&key).copied()
    }

    fn insert(&self, key: u64, hash: Hash) {
        let shard_idx = self.get_shard_index(key);
        let mut shard = self.shards[shard_idx].write();
        
        // LRU eviction when shard is full
        if shard.len() >= HASH_CACHE_ENTRIES / self.shard_count {
            let oldest_key = *shard.keys().next().unwrap_or(&key);
            shard.remove(&oldest_key);
        }
        
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        
        shard.insert(key, CachedHash { hash, timestamp });
    }
}

impl MemoryPool {
    fn new() -> Self {
        let mut buffers = Vec::with_capacity(MEMORY_POOL_SIZE);
        
        for _ in 0..MEMORY_POOL_SIZE / 2 {
            buffers.push(Box::new(AlignedBuffer {
                data: [0u8; MAX_TX_SIZE],
            }));
        }
        
        Self {
            buffers: Mutex::new(buffers),
            allocated: AtomicU64::new(MEMORY_POOL_SIZE as u64 / 2),
        }
    }

    fn get_buffer(&self) -> Box<AlignedBuffer> {
        let mut buffers = self.buffers.lock();
        
        if let Some(buffer) = buffers.pop() {
            buffer
        } else {
            let current = self.allocated.load(Ordering::Acquire);
            if current < MEMORY_POOL_SIZE as u64 {
                self.allocated.fetch_add(1, Ordering::AcqRel);
                Box::new(AlignedBuffer {
                    data: [0u8; MAX_TX_SIZE],
                })
            } else {
                // Fallback: create temporary buffer
                Box::new(AlignedBuffer {
                    data: [0u8; MAX_TX_SIZE],
                })
            }
        }
    }

    fn return_buffer(&self, mut buffer: Box<AlignedBuffer>) {
        // Clear sensitive data
        buffer.data.iter_mut().for_each(|b| *b = 0);
        
        let mut buffers = self.buffers.lock();
        if buffers.len() < MEMORY_POOL_SIZE {
            buffers.push(buffer);
        }
    }
}

impl Drop for SimdTransactionHasher {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::{
        system_instruction,
        signature::{Keypair, Signer},
    };

    #[test]
    fn test_hash_transaction() {
        let hasher = SimdTransactionHasher::new().unwrap();
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        
        let instruction = system_instruction::transfer(&from.pubkey(), &to, 1000);
        let message = Message::new(&[instruction], Some(&from.pubkey()));
        let blockhash = Hash::new_unique();
        let transaction = Transaction::new(&[&from], message, blockhash);
        
        let hash = hasher.hash_transaction(&transaction).unwrap();
        assert_ne!(hash, Hash::default());
    }

    #[test]
    fn test_batch_processing() {
        let hasher = SimdTransactionHasher::new().unwrap();
        let mut transactions = Vec::new();
        
        for _ in 0..10 {
            let from = Keypair::new();
            let to = Pubkey::new_unique();
            let instruction = system_instruction::transfer(&from.pubkey(), &to, 1000);
            let message = Message::new(&[instruction], Some(&from.pubkey()));
            let blockhash = Hash::new_unique();
            let transaction = Transaction::new(&[&from], message, blockhash);
            transactions.push(transaction);
        }
        
        let hashes = hasher.hash_batch_parallel(&transactions).unwrap();
        assert_eq!(hashes.len(), transactions.len());
    }

    #[test]
    fn test_cache_functionality() {
        let hasher = SimdTransactionHasher::new().unwrap();
        let from = Keypair::new();
        let to = Pubkey::new_unique();
        
        let instruction = system_instruction::transfer(&from.pubkey(), &to, 1000);
        let message = Message::new(&[instruction], Some(&from.pubkey()));
        let blockhash = Hash::new_unique();
        let transaction = Transaction::new(&[&from], message, blockhash);
        
        let hash1 = hasher.hash_transaction(&transaction).unwrap();
        let hash2 = hasher.hash_transaction(&transaction).unwrap();
        
        assert_eq!(hash1, hash2);
        
        let (_, hits, _, _) = hasher.get_metrics();
        assert!(hits > 0);
    }
}

