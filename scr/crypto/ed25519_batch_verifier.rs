use ed25519_dalek::{
    Signature, Verifier, VerifyingKey, SignatureError,
    SIGNATURE_LENGTH, PUBLIC_KEY_LENGTH
};
use rayon::prelude::*;
use std::sync::{Arc, RwLock, atomic::{AtomicU64, AtomicBool, Ordering}};
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};
use thiserror::Error;
use parking_lot::Mutex;
use crossbeam_channel::{bounded, Sender, Receiver};
use bytes::Bytes;
use blake3::Hasher;
use smallvec::SmallVec;

const MAX_BATCH_SIZE: usize = 128;
const MIN_BATCH_SIZE: usize = 8;
const VERIFICATION_TIMEOUT_MS: u64 = 50;
const CACHE_SIZE: usize = 65536;
const PARALLEL_THRESHOLD: usize = 16;
const MAX_PENDING_BATCHES: usize = 256;
const RETRY_ATTEMPTS: u8 = 3;
const BACKOFF_BASE_MS: u64 = 1;

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
}

impl VerificationItem {
    fn cache_key(&self) -> u64 {
        let mut hasher = Hasher::new();
        hasher.update(&self.signature);
        hasher.update(&self.public_key);
        hasher.update(&self.message);
        hasher.finalize().as_bytes()[..8]
            .try_into()
            .map(u64::from_le_bytes)
            .unwrap_or(0)
    }
}

#[derive(Clone)]
struct CacheEntry {
    result: bool,
    timestamp: Instant,
}

pub struct BatchVerifier {
    verification_cache: Arc<RwLock<HashMap<u64, CacheEntry>>>,
    pending_queue: Arc<Mutex<VecDeque<VerificationItem>>>,
    batch_sender: Sender<Vec<VerificationItem>>,
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
    pub fn new(worker_threads: usize) -> Self {
        let (batch_sender, batch_receiver) = bounded(MAX_PENDING_BATCHES);
        let (result_sender, result_receiver) = bounded(MAX_PENDING_BATCHES * MAX_BATCH_SIZE);
        
        let verification_cache = Arc::new(RwLock::new(HashMap::with_capacity(CACHE_SIZE)));
        let pending_queue = Arc::new(Mutex::new(VecDeque::with_capacity(MAX_BATCH_SIZE * 4)));
        let stats = Arc::new(VerifierStats::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        
        let mut worker_handles = Vec::with_capacity(worker_threads);
        
        for _ in 0..worker_threads {
            let batch_receiver = batch_receiver.clone();
            let result_sender = result_sender.clone();
            let stats = stats.clone();
            let shutdown = shutdown.clone();
            
            let handle = std::thread::spawn(move || {
                while !shutdown.load(Ordering::Acquire) {
                    match batch_receiver.recv_timeout(Duration::from_millis(10)) {
                        Ok(batch) => {
                            let start = Instant::now();
                            let results = Self::verify_batch_internal(&batch);
                            
                            let batch_time = start.elapsed().as_micros() as u64;
                            stats.batch_count.fetch_add(1, Ordering::Relaxed);
                            
                            let current_avg = stats.avg_batch_time_us.load(Ordering::Relaxed);
                            let new_avg = (current_avg * 7 + batch_time) / 8;
                            stats.avg_batch_time_us.store(new_avg, Ordering::Relaxed);
                            
                            let indexed_results: Vec<_> = results
                                .into_iter()
                                .enumerate()
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
        
        if let Ok(cache) = self.verification_cache.read() {
            if let Some(entry) = cache.get(&cache_key) {
                if entry.timestamp.elapsed() < Duration::from_secs(60) {
                    self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                    return Ok(entry.result);
                }
            }
        }
        
        self.stats.cache_misses.fetch_add(1, Ordering::Relaxed);
        
        let result = self.verify_single_internal(&item);
        
        if let Ok(mut cache) = self.verification_cache.write() {
            if cache.len() >= CACHE_SIZE {
                let oldest_key = cache
                    .iter()
                    .min_by_key(|(_, entry)| entry.timestamp)
                    .map(|(k, _)| *k);
                
                if let Some(key) = oldest_key {
                    cache.remove(&key);
                }
            }
            
            cache.insert(cache_key, CacheEntry {
                result: result.is_ok(),
                timestamp: Instant::now(),
            });
        }
        
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
            
            if let Ok(cache) = self.verification_cache.read() {
                if let Some(entry) = cache.get(&cache_key) {
                    if entry.timestamp.elapsed() < Duration::from_secs(60) {
                        cached_indices.push((idx, entry.result));
                        self.stats.cache_hits.fetch_add(1, Ordering::Relaxed);
                        continue;
                    }
                }
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
        
        let chunks: Vec<_> = uncached_items
            .chunks(MAX_BATCH_SIZE)
            .map(|chunk| chunk.to_vec())
            .collect();
        
        for (chunk_idx, chunk) in chunks.into_iter().enumerate() {
            if let Err(e) = self.batch_sender.send(chunk) {
                for idx in &uncached_indices[chunk_idx * MAX_BATCH_SIZE..] {
                    results[*idx] = Err(BatchVerifierError::QueueFull);
                }
                continue;
            }
            
            match self.result_receiver.recv_timeout(Duration::from_millis(VERIFICATION_TIMEOUT_MS)) {
                Ok(batch_results) => {
                    for (batch_idx, result) in batch_results {
                        let global_idx = uncached_indices[chunk_idx * MAX_BATCH_SIZE + batch_idx];
                        results[global_idx] = result;
                        
                        if let Ok(verified) = result {
                            if let Ok(mut cache) = self.verification_cache.write() {
                                let item = &items[global_idx];
                                cache.insert(item.cache_key(), CacheEntry {
                                    result: verified,
                                    timestamp: Instant::now(),
                                });
                            }
                        }
                    }
                }
                Err(_) => {
                    for idx in &uncached_indices[chunk_idx * MAX_BATCH_SIZE..] {
                        results[*idx] = Err(BatchVerifierError::BatchTimeout);
                    }
                }
            }
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
        if items.len() < PARALLEL_THRESHOLD {
            items.iter()
                .map(|item| Self::verify_item(item))
                .collect()
        } else {
            items.par_iter()
                .map(|item| Self::verify_item(item))
                .collect()
        }
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
        
        let mut queue = self.pending_queue.lock();
        
        if queue.len() >= MAX_BATCH_SIZE * 4 {
            return Err(BatchVerifierError::QueueFull);
        }
        
        queue.push_back(item);
        
        if queue.len() >= MIN_BATCH_SIZE {
            self.process_pending_queue(&mut queue)?;
        }
        
        Ok(())
    }
    
        fn process_pending_queue(&self, queue: &mut VecDeque<VerificationItem>) -> Result<(), BatchVerifierError> {
        let batch_size = queue.len().min(MAX_BATCH_SIZE);
        let batch: Vec<_> = queue.drain(..batch_size).collect();
        
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
        
        let mut queue = self.pending_queue.lock();
        let mut all_results = Vec::new();
        
        while !queue.is_empty() {
            let batch_size = queue.len().min(MAX_BATCH_SIZE);
            let batch: Vec<_> = queue.drain(..batch_size).collect();
            let batch_len = batch.len();
            
            if self.batch_sender.send(batch).is_err() {
                return Err(BatchVerifierError::QueueFull);
            }
            
            match self.result_receiver.recv_timeout(Duration::from_millis(VERIFICATION_TIMEOUT_MS * 2)) {
                Ok(results) => {
                    all_results.extend(results.into_iter().map(|(_, r)| r));
                }
                Err(_) => {
                    all_results.extend(vec![Err(BatchVerifierError::BatchTimeout); batch_len]);
                }
            }
        }
        
        Ok(all_results)
    }
    
    pub fn get_stats(&self) -> VerifierStatsSnapshot {
        VerifierStatsSnapshot {
            total_verified: self.stats.total_verified.load(Ordering::Relaxed),
            cache_hits: self.stats.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.stats.cache_misses.load(Ordering::Relaxed),
            failed_verifications: self.stats.failed_verifications.load(Ordering::Relaxed),
            batch_count: self.stats.batch_count.load(Ordering::Relaxed),
            avg_batch_time_us: self.stats.avg_batch_time_us.load(Ordering::Relaxed),
            cache_size: self.verification_cache.read().map(|c| c.len()).unwrap_or(0),
        }
    }
    
    pub fn clear_cache(&self) {
        if let Ok(mut cache) = self.verification_cache.write() {
            cache.clear();
        }
    }
    
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }
    
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }
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
    priority_queue: Arc<Mutex<Vec<VecDeque<VerificationItem>>>>,
    processing_thread: Option<std::thread::JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

impl PriorityBatchVerifier {
    pub fn new(worker_threads: usize) -> Self {
        let verifier = Arc::new(BatchVerifier::new(worker_threads));
        let priority_queue = Arc::new(Mutex::new(vec![VecDeque::new(); 256]));
        let shutdown = Arc::new(AtomicBool::new(false));
        
        let verifier_clone = verifier.clone();
        let queue_clone = priority_queue.clone();
        let shutdown_clone = shutdown.clone();
        
        let processing_thread = std::thread::spawn(move || {
            let mut last_flush = Instant::now();
            let mut consecutive_empty = 0;
            
            while !shutdown_clone.load(Ordering::Acquire) {
                let mut found_items = false;
                let mut batch = Vec::with_capacity(MAX_BATCH_SIZE);
                
                {
                    let mut queues = queue_clone.lock();
                    
                    for priority in (0..=255).rev() {
                        let queue = &mut queues[priority as usize];
                        
                        while !queue.is_empty() && batch.len() < MAX_BATCH_SIZE {
                            if let Some(item) = queue.pop_front() {
                                batch.push(item);
                                found_items = true;
                            }
                        }
                        
                        if batch.len() >= MAX_BATCH_SIZE {
                            break;
                        }
                    }
                }
                
                if !batch.is_empty() {
                    let _ = tokio::runtime::Handle::current().block_on(
                        verifier_clone.verify_batch(batch)
                    );
                    consecutive_empty = 0;
                } else {
                    consecutive_empty += 1;
                }
                
                if last_flush.elapsed() >= Duration::from_millis(10) || 
                   (found_items && batch.len() >= MIN_BATCH_SIZE) {
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
            processing_thread: Some(processing_thread),
            shutdown,
        }
    }
    
    pub fn queue_with_priority(&self, item: VerificationItem) -> Result<(), BatchVerifierError> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err(BatchVerifierError::VerifierShutdown);
        }
        
        let priority = item.priority as usize;
        let mut queues = self.priority_queue.lock();
        
        if queues[priority].len() >= MAX_BATCH_SIZE * 2 {
            return Err(BatchVerifierError::QueueFull);
        }
        
        queues[priority].push_back(item);
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
                        tokio::time::sleep(Duration::from_millis(
                            BACKOFF_BASE_MS * (1 << attempt)
                        )).await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        
        Err(BatchVerifierError::VerificationFailed)
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
}

impl AdaptiveBatchVerifier {
    pub fn new(worker_threads: usize, latency_target_us: u64, min_success_rate: f64) -> Self {
        Self {
            verifier: Arc::new(PriorityBatchVerifier::new(worker_threads)),
            batch_size: Arc::new(AtomicU64::new(MIN_BATCH_SIZE as u64)),
            success_rate: Arc::new(AtomicU64::new(f64::to_bits(1.0))),
            latency_target_us,
            min_success_rate,
        }
    }
    
    pub async fn verify_adaptive(&self, items: Vec<VerificationItem>) -> Vec<Result<bool, BatchVerifierError>> {
        let start = Instant::now();
        let current_batch_size = self.batch_size.load(Ordering::Relaxed) as usize;
        
        let mut results = Vec::with_capacity(items.len());
        let chunks: Vec<_> = items.chunks(current_batch_size).collect();
        
        let mut successful = 0u64;
        let mut total = 0u64;
        
        for chunk in chunks {
            let chunk_results = self.verifier.get_verifier().verify_batch(chunk.to_vec()).await;
            
            for result in &chunk_results {
                total += 1;
                if result.is_ok() {
                    successful += 1;
                }
            }
            
            results.extend(chunk_results);
        }
        
        let elapsed_us = start.elapsed().as_micros() as u64;
        let success_rate = if total > 0 { successful as f64 / total as f64 } else { 1.0 };
        
        self.success_rate.store(f64::to_bits(success_rate), Ordering::Relaxed);
        
        self.adjust_batch_size(elapsed_us, success_rate);
        
        results
    }
    
    fn adjust_batch_size(&self, latency_us: u64, success_rate: f64) {
        let current_size = self.batch_size.load(Ordering::Relaxed);
        let mut new_size = current_size;
        
        if latency_us > self.latency_target_us && current_size > MIN_BATCH_SIZE as u64 {
            new_size = (current_size * 3 / 4).max(MIN_BATCH_SIZE as u64);
        } else if latency_us < self.latency_target_us / 2 && 
                  success_rate >= self.min_success_rate && 
                  current_size < MAX_BATCH_SIZE as u64 {
            new_size = (current_size * 5 / 4).min(MAX_BATCH_SIZE as u64);
        }
        
        if success_rate < self.min_success_rate && current_size > MIN_BATCH_SIZE as u64 {
            new_size = (current_size / 2).max(MIN_BATCH_SIZE as u64);
        }
        
        self.batch_size.store(new_size, Ordering::Relaxed);
    }
    
    pub fn get_current_batch_size(&self) -> usize {
        self.batch_size.load(Ordering::Relaxed) as usize
    }
    
    pub fn get_success_rate(&self) -> f64 {
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

