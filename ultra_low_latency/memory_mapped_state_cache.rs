use anyhow::{Context, Result};
use bytemuck::{Pod, Zeroable};
use crossbeam::epoch::{self, Atomic, Owned, Shared};
use dashmap::DashMap;
use memmap2::{MmapMut, MmapOptions};
use parking_lot::{Mutex, RwLock};
use solana_sdk::{
    account::AccountSharedData,
    clock::Slot,
    pubkey::Pubkey,
};
use std::{
    collections::{BTreeMap, HashMap},
    fs::{File, OpenOptions},
    io::Write,
    mem::size_of,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

const CACHE_VERSION: u32 = 0x4D454D43;
const PAGE_SIZE: usize = 4096;
const MAX_CACHE_SIZE: usize = 32 * 1024 * 1024 * 1024;
const SEGMENT_SIZE: usize = 256 * 1024 * 1024;
const INDEX_BUCKET_COUNT: usize = 65536;
const EVICTION_BATCH_SIZE: usize = 1024;
const SLOT_HISTORY_SIZE: usize = 512;

#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
struct CacheHeader {
    version: u32,
    magic: u32,
    total_size: u64,
    used_size: u64,
    entry_count: u64,
    last_slot: u64,
    last_update: u64,
    checksum: u64,
}

#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
struct CacheEntry {
    pubkey: [u8; 32],
    offset: u64,
    size: u64,
    slot: u64,
    last_access: u64,
    checksum: u32,
    flags: u32,
}

#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
struct AccountData {
    lamports: u64,
    data_len: u64,
    owner: [u8; 32],
    executable: u8,
    rent_epoch: u64,
    padding: [u8; 7],
}

struct MemorySegment {
    mmap: Arc<Mutex<MmapMut>>,
    free_list: Arc<Mutex<Vec<(u64, u64)>>>,
    used_bytes: AtomicU64,
}

pub struct MemoryMappedStateCache {
    base_path: PathBuf,
    segments: Vec<Arc<MemorySegment>>,
    index: Arc<DashMap<Pubkey, Arc<Atomic<CacheEntry>>>>,
    slot_index: Arc<RwLock<BTreeMap<Slot, Vec<Pubkey>>>>,
    access_counts: Arc<DashMap<Pubkey, AtomicU32>>,
    total_size: AtomicUsize,
    hit_count: AtomicU64,
    miss_count: AtomicU64,
    eviction_count: AtomicU64,
    last_eviction: Arc<Mutex<Instant>>,
    active_segment: AtomicUsize,
}

impl MemoryMappedStateCache {
    pub fn new(base_path: impl AsRef<Path>) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_path)?;

        let mut segments = Vec::with_capacity(MAX_CACHE_SIZE / SEGMENT_SIZE);
        
        for i in 0..(MAX_CACHE_SIZE / SEGMENT_SIZE) {
            let segment_path = base_path.join(format!("segment_{:04}.mmap", i));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&segment_path)?;
            
            file.set_len(SEGMENT_SIZE as u64)?;
            
            let mmap = unsafe {
                MmapOptions::new()
                    .len(SEGMENT_SIZE)
                    .map_mut(&file)?
            };

            let mut free_list = vec![(size_of::<CacheHeader>() as u64, SEGMENT_SIZE as u64 - size_of::<CacheHeader>() as u64)];
            
            if i == 0 {
                let header = CacheHeader {
                    version: CACHE_VERSION,
                    magic: 0x534F4C4D,
                    total_size: MAX_CACHE_SIZE as u64,
                    used_size: size_of::<CacheHeader>() as u64,
                    entry_count: 0,
                    last_slot: 0,
                    last_update: 0,
                    checksum: 0,
                };
                
                let header_bytes = bytemuck::bytes_of(&header);
                mmap.as_ptr().copy_from_nonoverlapping(header_bytes.as_ptr(), header_bytes.len());
            }

            let segment = Arc::new(MemorySegment {
                mmap: Arc::new(Mutex::new(mmap)),
                free_list: Arc::new(Mutex::new(free_list)),
                used_bytes: AtomicU64::new(if i == 0 { size_of::<CacheHeader>() as u64 } else { 0 }),
            });

            segments.push(segment);
        }

        Ok(Self {
            base_path,
            segments,
            index: Arc::new(DashMap::with_capacity(INDEX_BUCKET_COUNT)),
            slot_index: Arc::new(RwLock::new(BTreeMap::new())),
            access_counts: Arc::new(DashMap::new()),
            total_size: AtomicUsize::new(0),
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
            eviction_count: AtomicU64::new(0),
            last_eviction: Arc::new(Mutex::new(Instant::now())),
            active_segment: AtomicUsize::new(0),
        })
    }

    pub fn get(&self, pubkey: &Pubkey) -> Result<Option<AccountSharedData>> {
        let guard = epoch::pin();
        
        if let Some(entry_atomic) = self.index.get(pubkey) {
            let entry_ptr = entry_atomic.load(Ordering::Acquire, &guard);
            
            if let Some(entry_ref) = unsafe { entry_ptr.as_ref() } {
                self.hit_count.fetch_add(1, Ordering::Relaxed);
                
                self.access_counts
                    .entry(*pubkey)
                    .or_insert_with(|| AtomicU32::new(0))
                    .fetch_add(1, Ordering::Relaxed);

                let segment_idx = (entry_ref.offset / SEGMENT_SIZE as u64) as usize;
                let segment_offset = (entry_ref.offset % SEGMENT_SIZE as u64) as usize;
                
                if segment_idx >= self.segments.len() {
                    return Ok(None);
                }

                let segment = &self.segments[segment_idx];
                let mmap = segment.mmap.lock();

                let account_data_offset = segment_offset;
                let account_data: &AccountData = bytemuck::from_bytes(
                    &mmap[account_data_offset..account_data_offset + size_of::<AccountData>()]
                );

                let data_offset = account_data_offset + size_of::<AccountData>();
                let data_end = data_offset + account_data.data_len as usize;
                
                if data_end > mmap.len() {
                    return Ok(None);
                }

                let mut data = vec![0u8; account_data.data_len as usize];
                data.copy_from_slice(&mmap[data_offset..data_end]);

                let account = AccountSharedData::new_data(
                    account_data.lamports,
                    &data,
                    &Pubkey::new_from_array(account_data.owner),
                )?;

                return Ok(Some(account));
            }
        }

        self.miss_count.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }

    pub fn insert(&self, pubkey: &Pubkey, account: &AccountSharedData, slot: Slot) -> Result<()> {
        let data = account.data();
        let total_size = size_of::<AccountData>() + data.len();
        
        if total_size > SEGMENT_SIZE / 4 {
            return Ok(());
        }

        let (segment_idx, offset) = self.allocate_space(total_size)?;
        let segment = &self.segments[segment_idx];
        
        let account_data = AccountData {
            lamports: account.lamports(),
            data_len: data.len() as u64,
            owner: account.owner().to_bytes(),
            executable: if account.executable() { 1 } else { 0 },
            rent_epoch: account.rent_epoch(),
            padding: [0; 7],
        };

        {
            let mut mmap = segment.mmap.lock();
            let segment_offset = (offset % SEGMENT_SIZE as u64) as usize;
            
            let account_bytes = bytemuck::bytes_of(&account_data);
            mmap[segment_offset..segment_offset + account_bytes.len()]
                .copy_from_slice(account_bytes);
            
            let data_offset = segment_offset + size_of::<AccountData>();
            mmap[data_offset..data_offset + data.len()].copy_from_slice(data);
        }

        let entry = CacheEntry {
            pubkey: pubkey.to_bytes(),
            offset,
            size: total_size as u64,
            slot,
            last_access: Instant::now().elapsed().as_secs(),
            checksum: 0,
            flags: 0,
        };

        let guard = epoch::pin();
        let entry_owned = Owned::new(entry);
        
        self.index.insert(*pubkey, Arc::new(Atomic::from(entry_owned)));
        
        {
            let mut slot_index = self.slot_index.write();
            slot_index.entry(slot).or_insert_with(Vec::new).push(*pubkey);
            
            if slot_index.len() > SLOT_HISTORY_SIZE {
                if let Some((oldest_slot, pubkeys)) = slot_index.pop_first() {
                    for pk in pubkeys {
                        self.index.remove(&pk);
                    }
                }
            }
        }

        self.total_size.fetch_add(total_size, Ordering::Relaxed);
        
        if self.should_evict() {
            self.evict_lru()?;
        }

        Ok(())
    }

    pub fn remove(&self, pubkey: &Pubkey) -> Result<()> {
        if let Some((_, entry_atomic)) = self.index.remove(pubkey) {
            let guard = epoch::pin();
            let entry_ptr = entry_atomic.load(Ordering::Acquire, &guard);
            
            if let Some(entry) = unsafe { entry_ptr.as_ref() } {
                let segment_idx = (entry.offset / SEGMENT_SIZE as u64) as usize;
                let segment = &self.segments[segment_idx];
                
                let mut free_list = segment.free_list.lock();
                free_list.push((entry.offset % SEGMENT_SIZE as u64, entry.size));
                free_list.sort_by_key(|(offset, _)| *offset);
                
                let mut i = 0;
                while i + 1 < free_list.len() {
                    if free_list[i].0 + free_list[i].1 == free_list[i + 1].0 {
                        free_list[i].1 += free_list[i + 1].1;
                        free_list.remove(i + 1);
                    } else {
                        i += 1;
                    }
                }
                
                self.total_size.fetch_sub(entry.size as usize, Ordering::Relaxed);
            }
        }
        
        self.access_counts.remove(pubkey);
        Ok(())
    }

    pub fn clear_slot(&self, slot: Slot) -> Result<()> {
        if let Some(pubkeys) = self.slot_index.write().remove(&slot) {
            for pubkey in pubkeys {
                self.remove(&pubkey)?;
            }
        }
        Ok(())
    }

    fn allocate_space(&self, size: usize) -> Result<(usize, u64)> {
        let aligned_size = ((size + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;
        
        for attempt in 0..self.segments.len() {
            let segment_idx = (self.active_segment.load(Ordering::Relaxed) + attempt) % self.segments.len();
            let segment = &self.segments[segment_idx];
            
            let mut free_list = segment.free_list.lock();
            
            for (i, &(offset, free_size)) in free_list.iter().enumerate() {
                if free_size >= aligned_size as u64 {
                    let allocation_offset = segment_idx as u64 * SEGMENT_SIZE as u64 + offset;
                    
                    if free_size > aligned_size as u64 {
                        free_list[i] = (offset + aligned_size as u64, free_size - aligned_size as u64);
                    } else {
                        free_list.remove(i);
                    }
                    
                    segment.used_bytes.fetch_add(aligned_size as u64, Ordering::Relaxed);
                    self.active_segment.store(segment_idx, Ordering::Relaxed);
                    
                                        return Ok((segment_idx, allocation_offset));
                }
            }
        }
        
        Err(anyhow::anyhow!("No space available in cache"))
    }

    fn should_evict(&self) -> bool {
        let total = self.total_size.load(Ordering::Relaxed);
        let threshold = MAX_CACHE_SIZE * 9 / 10;
        
        if total > threshold {
            let last_eviction = *self.last_eviction.lock();
            last_eviction.elapsed() > Duration::from_millis(100)
        } else {
            false
        }
    }

    fn evict_lru(&self) -> Result<()> {
        let mut candidates: Vec<(Pubkey, u32, u64)> = Vec::with_capacity(EVICTION_BATCH_SIZE * 2);
        
        for entry in self.access_counts.iter() {
            let pubkey = *entry.key();
            let access_count = entry.value().load(Ordering::Relaxed);
            
            if let Some(cache_entry) = self.index.get(&pubkey) {
                let guard = epoch::pin();
                let entry_ptr = cache_entry.load(Ordering::Acquire, &guard);
                
                if let Some(entry_ref) = unsafe { entry_ptr.as_ref() } {
                    let age = Instant::now().elapsed().as_secs() - entry_ref.last_access;
                    let score = (access_count as u64).saturating_add(1) * 1000 / (age.saturating_add(1));
                    
                    candidates.push((pubkey, access_count, score));
                    
                    if candidates.len() >= EVICTION_BATCH_SIZE * 2 {
                        break;
                    }
                }
            }
        }
        
        candidates.sort_by_key(|(_, _, score)| *score);
        candidates.truncate(EVICTION_BATCH_SIZE);
        
        for (pubkey, _, _) in candidates {
            self.remove(&pubkey)?;
            self.eviction_count.fetch_add(1, Ordering::Relaxed);
        }
        
        *self.last_eviction.lock() = Instant::now();
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        for segment in &self.segments {
            let mmap = segment.mmap.lock();
            mmap.flush()?;
        }
        Ok(())
    }

    pub fn snapshot(&self, path: impl AsRef<Path>) -> Result<()> {
        let snapshot_dir = path.as_ref();
        std::fs::create_dir_all(snapshot_dir)?;
        
        let metadata = SnapshotMetadata {
            version: CACHE_VERSION,
            timestamp: Instant::now().elapsed().as_secs(),
            entry_count: self.index.len() as u64,
            total_size: self.total_size.load(Ordering::Relaxed) as u64,
            segments: self.segments.len() as u32,
        };
        
        let metadata_path = snapshot_dir.join("metadata.bin");
        let mut metadata_file = File::create(&metadata_path)?;
        metadata_file.write_all(bytemuck::bytes_of(&metadata))?;
        
        for (i, segment) in self.segments.iter().enumerate() {
            let segment_path = snapshot_dir.join(format!("segment_{:04}.snap", i));
            let mut segment_file = File::create(&segment_path)?;
            
            let mmap = segment.mmap.lock();
            let used_bytes = segment.used_bytes.load(Ordering::Relaxed) as usize;
            segment_file.write_all(&mmap[..used_bytes.min(mmap.len())])?;
        }
        
        let index_path = snapshot_dir.join("index.bin");
        let mut index_file = File::create(&index_path)?;
        
        for entry in self.index.iter() {
            let pubkey = entry.key();
            let guard = epoch::pin();
            let entry_ptr = entry.value().load(Ordering::Acquire, &guard);
            
            if let Some(entry_ref) = unsafe { entry_ptr.as_ref() } {
                index_file.write_all(&pubkey.to_bytes())?;
                index_file.write_all(bytemuck::bytes_of(entry_ref))?;
            }
        }
        
        Ok(())
    }

    pub fn restore(&mut self, path: impl AsRef<Path>) -> Result<()> {
        let snapshot_dir = path.as_ref();
        
        let metadata_path = snapshot_dir.join("metadata.bin");
        let metadata_data = std::fs::read(&metadata_path)?;
        let metadata: &SnapshotMetadata = bytemuck::from_bytes(&metadata_data);
        
        if metadata.version != CACHE_VERSION {
            return Err(anyhow::anyhow!("Incompatible snapshot version"));
        }
        
        self.index.clear();
        self.slot_index.write().clear();
        self.access_counts.clear();
        
        for (i, segment) in self.segments.iter().enumerate() {
            let segment_path = snapshot_dir.join(format!("segment_{:04}.snap", i));
            
            if segment_path.exists() {
                let segment_data = std::fs::read(&segment_path)?;
                let mut mmap = segment.mmap.lock();
                
                let copy_len = segment_data.len().min(mmap.len());
                mmap[..copy_len].copy_from_slice(&segment_data[..copy_len]);
                
                segment.used_bytes.store(copy_len as u64, Ordering::Relaxed);
            }
        }
        
        let index_path = snapshot_dir.join("index.bin");
        if index_path.exists() {
            let index_data = std::fs::read(&index_path)?;
            let entry_size = 32 + size_of::<CacheEntry>();
            
            for chunk in index_data.chunks_exact(entry_size) {
                let pubkey = Pubkey::new_from_array(chunk[..32].try_into()?);
                let entry: CacheEntry = *bytemuck::from_bytes(&chunk[32..]);
                
                let entry_owned = Owned::new(entry);
                self.index.insert(pubkey, Arc::new(Atomic::from(entry_owned)));
                
                self.slot_index.write()
                    .entry(entry.slot)
                    .or_insert_with(Vec::new)
                    .push(pubkey);
            }
        }
        
        self.total_size.store(metadata.total_size as usize, Ordering::Relaxed);
        Ok(())
    }

    pub fn stats(&self) -> CacheStats {
        CacheStats {
            total_size: self.total_size.load(Ordering::Relaxed),
            entry_count: self.index.len(),
            hit_count: self.hit_count.load(Ordering::Relaxed),
            miss_count: self.miss_count.load(Ordering::Relaxed),
            eviction_count: self.eviction_count.load(Ordering::Relaxed),
            hit_rate: {
                let hits = self.hit_count.load(Ordering::Relaxed);
                let misses = self.miss_count.load(Ordering::Relaxed);
                let total = hits + misses;
                if total > 0 {
                    hits as f64 / total as f64
                } else {
                    0.0
                }
            },
        }
    }

    pub fn prefetch(&self, pubkeys: &[Pubkey]) -> Result<()> {
        for pubkey in pubkeys {
            if let Some(entry_atomic) = self.index.get(pubkey) {
                let guard = epoch::pin();
                let entry_ptr = entry_atomic.load(Ordering::Acquire, &guard);
                
                if let Some(entry_ref) = unsafe { entry_ptr.as_ref() } {
                    let segment_idx = (entry_ref.offset / SEGMENT_SIZE as u64) as usize;
                    let segment_offset = (entry_ref.offset % SEGMENT_SIZE as u64) as usize;
                    
                    if segment_idx < self.segments.len() {
                        let segment = &self.segments[segment_idx];
                        let mmap = segment.mmap.lock();
                        
                        unsafe {
                            let ptr = mmap.as_ptr().add(segment_offset);
                            std::ptr::read_volatile(ptr);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn compact(&self) -> Result<()> {
        for segment in &self.segments {
            let mut free_list = segment.free_list.lock();
            
            if free_list.len() > 100 {
                free_list.sort_by_key(|(offset, _)| *offset);
                
                let mut compacted = Vec::new();
                let mut current_start = 0u64;
                let mut current_size = 0u64;
                
                for &(offset, size) in free_list.iter() {
                    if current_size == 0 {
                        current_start = offset;
                        current_size = size;
                    } else if current_start + current_size == offset {
                        current_size += size;
                    } else {
                        compacted.push((current_start, current_size));
                        current_start = offset;
                        current_size = size;
                    }
                }
                
                if current_size > 0 {
                    compacted.push((current_start, current_size));
                }
                
                *free_list = compacted;
            }
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<bool> {
        let mut valid = true;
        
        for entry in self.index.iter() {
            let guard = epoch::pin();
            let entry_ptr = entry.value().load(Ordering::Acquire, &guard);
            
            if let Some(entry_ref) = unsafe { entry_ptr.as_ref() } {
                let segment_idx = (entry_ref.offset / SEGMENT_SIZE as u64) as usize;
                
                if segment_idx >= self.segments.len() {
                    valid = false;
                    break;
                }
                
                let segment_offset = (entry_ref.offset % SEGMENT_SIZE as u64) as usize;
                if segment_offset + entry_ref.size as usize > SEGMENT_SIZE {
                    valid = false;
                    break;
                }
            }
        }
        
        Ok(valid)
    }
}

#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
struct SnapshotMetadata {
    version: u32,
    timestamp: u64,
    entry_count: u64,
    total_size: u64,
    segments: u32,
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub total_size: usize,
    pub entry_count: usize,
    pub hit_count: u64,
    pub miss_count: u64,
    pub eviction_count: u64,
    pub hit_rate: f64,
}

impl Drop for MemoryMappedStateCache {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

unsafe impl Send for MemoryMappedStateCache {}
unsafe impl Sync for MemoryMappedStateCache {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_cache_operations() {
        let temp_dir = TempDir::new().unwrap();
        let cache = MemoryMappedStateCache::new(temp_dir.path()).unwrap();
        
        let pubkey = Pubkey::new_unique();
        let account = AccountSharedData::new(1000, 32, &Pubkey::default());
        
        cache.insert(&pubkey, &account, 1).unwrap();
        
        let retrieved = cache.get(&pubkey).unwrap().unwrap();
        assert_eq!(retrieved.lamports(), 1000);
        assert_eq!(retrieved.data().len(), 32);
        
        cache.remove(&pubkey).unwrap();
        assert!(cache.get(&pubkey).unwrap().is_none());
    }

    #[test]
    fn test_snapshot_restore() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join("cache");
        let snapshot_dir = temp_dir.path().join("snapshot");
        
        let mut cache = MemoryMappedStateCache::new(&cache_dir).unwrap();
        
        for i in 0..10 {
            let pubkey = Pubkey::new_unique();
            let account = AccountSharedData::new(i * 100, 64, &Pubkey::default());
            cache.insert(&pubkey, &account, i).unwrap();
        }
        
        cache.snapshot(&snapshot_dir).unwrap();
        
        let mut restored_cache = MemoryMappedStateCache::new(&cache_dir).unwrap();
        restored_cache.restore(&snapshot_dir).unwrap();
        
        assert_eq!(cache.stats().entry_count, restored_cache.stats().entry_count);
    }
}

