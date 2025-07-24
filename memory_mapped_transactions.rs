use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::ptr;
use std::slice;
use std::mem;
use std::fs::{File, OpenOptions};
use std::io::{self, Error, ErrorKind};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};
use memmap2::{MmapMut, MmapOptions};
use solana_sdk::transaction::Transaction;
use solana_sdk::signature::Signature;
use solana_sdk::hash::Hash;
use bincode::{serialize, deserialize};
use crossbeam::queue::ArrayQueue;
use parking_lot::Mutex;
use ahash::AHashMap;

const PAGE_SIZE: usize = 4096;
const TX_SLOT_SIZE: usize = 1536;
const HEADER_SIZE: usize = 64;
const INDEX_CACHE_SIZE: usize = 65536;
const WRITE_BUFFER_SIZE: usize = 16384;
const MAX_MMAP_SIZE: usize = 1 << 30; // 1GB
const SYNC_INTERVAL: u64 = 1000;
const TOMBSTONE_MARKER: u64 = u64::MAX;

#[repr(C, packed)]
#[derive(Clone, Copy)]
struct TxSlotHeader {
    signature: [u8; 64],
    timestamp: u64,
    size: u32,
    flags: u32,
    next_offset: u64,
    checksum: u32,
    padding: [u8; 4],
}

#[repr(C)]
struct MmapHeader {
    magic: u64,
    version: u32,
    slot_count: u32,
    free_list_head: u64,
    write_position: u64,
    transaction_count: u64,
    last_sync: u64,
    checksum: u32,
    reserved: [u8; 12],
}

pub struct MemoryMappedTransactions {
    mmap: Arc<Mutex<MmapMut>>,
    file: Arc<Mutex<File>>,
    index: Arc<RwLock<AHashMap<Signature, u64>>>,
    free_slots: Arc<ArrayQueue<u64>>,
    write_position: Arc<AtomicU64>,
    tx_count: Arc<AtomicUsize>,
    last_sync: Arc<AtomicU64>,
    write_buffer: Arc<Mutex<Vec<(Transaction, u64)>>>,
    slot_cache: Arc<Mutex<HashMap<u64, Vec<u8>>>>,
}

impl MemoryMappedTransactions {
    pub fn new<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        
        let file_size = file.metadata()?.len();
        if file_size < MAX_MMAP_SIZE as u64 {
            file.set_len(MAX_MMAP_SIZE as u64)?;
        }

        let mut mmap = unsafe { MmapOptions::new().map_mut(&file)? };
        
        let needs_init = unsafe {
            let header = &*(mmap.as_ptr() as *const MmapHeader);
            header.magic != 0x534F4C4D4D415054
        };

        if needs_init {
            Self::initialize_mmap(&mut mmap)?;
        }

        let (write_pos, tx_count, free_list) = unsafe {
            let header = &*(mmap.as_ptr() as *const MmapHeader);
            (
                header.write_position,
                header.transaction_count as usize,
                header.free_list_head,
            )
        };

        let free_slots = Arc::new(ArrayQueue::new(INDEX_CACHE_SIZE));
        if free_list != 0 {
            Self::load_free_list(&mmap, free_list, &free_slots);
        }

        let index = Self::rebuild_index(&mmap)?;

        Ok(Self {
            mmap: Arc::new(Mutex::new(mmap)),
            file: Arc::new(Mutex::new(file)),
            index: Arc::new(RwLock::new(index)),
            free_slots,
            write_position: Arc::new(AtomicU64::new(write_pos)),
            tx_count: Arc::new(AtomicUsize::new(tx_count)),
            last_sync: Arc::new(AtomicU64::new(0)),
            write_buffer: Arc::new(Mutex::new(Vec::with_capacity(WRITE_BUFFER_SIZE))),
            slot_cache: Arc::new(Mutex::new(HashMap::with_capacity(1024))),
        })
    }

    fn initialize_mmap(mmap: &mut MmapMut) -> io::Result<()> {
        let header = MmapHeader {
            magic: 0x534F4C4D4D415054,
            version: 1,
            slot_count: ((MAX_MMAP_SIZE - HEADER_SIZE) / TX_SLOT_SIZE) as u32,
            free_list_head: 0,
            write_position: HEADER_SIZE as u64,
            transaction_count: 0,
            last_sync: 0,
            checksum: 0,
            reserved: [0; 12],
        };

        unsafe {
            ptr::write(mmap.as_mut_ptr() as *mut MmapHeader, header);
        }
        
        mmap.flush()?;
        Ok(())
    }

    fn load_free_list(mmap: &MmapMut, mut head: u64, queue: &ArrayQueue<u64>) {
        while head != 0 && queue.len() < INDEX_CACHE_SIZE / 2 {
            unsafe {
                let slot = &*(mmap.as_ptr().add(head as usize) as *const TxSlotHeader);
                if slot.flags == TOMBSTONE_MARKER as u32 {
                    let _ = queue.push(head);
                    head = slot.next_offset;
                } else {
                    break;
                }
            }
        }
    }

    fn rebuild_index(mmap: &MmapMut) -> io::Result<AHashMap<Signature, u64>> {
        let mut index = AHashMap::with_capacity(INDEX_CACHE_SIZE);
        let mut offset = HEADER_SIZE;
        
        while offset < MAX_MMAP_SIZE {
            unsafe {
                let slot = &*(mmap.as_ptr().add(offset) as *const TxSlotHeader);
                if slot.size > 0 && slot.size < TX_SLOT_SIZE as u32 && slot.flags != TOMBSTONE_MARKER as u32 {
                    if Self::verify_checksum(slot, mmap.as_ptr().add(offset + mem::size_of::<TxSlotHeader>())) {
                        let sig = Signature::from(slot.signature);
                        index.insert(sig, offset as u64);
                    }
                }
            }
            offset += TX_SLOT_SIZE;
        }
        
        Ok(index)
    }

    fn verify_checksum(header: &TxSlotHeader, data: *const u8) -> bool {
        if header.size == 0 || header.size > (TX_SLOT_SIZE - mem::size_of::<TxSlotHeader>()) as u32 {
            return false;
        }
        
        let data_slice = unsafe { slice::from_raw_parts(data, header.size as usize) };
        let calculated = Self::calculate_checksum(data_slice);
        calculated == header.checksum
    }

    fn calculate_checksum(data: &[u8]) -> u32 {
        let mut hash = 0x811c9dc5u32;
        for &byte in data {
            hash ^= byte as u32;
            hash = hash.wrapping_mul(0x01000193);
        }
        hash
    }

    pub fn store_transaction(&self, tx: &Transaction) -> io::Result<Signature> {
        let serialized = serialize(tx).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        if serialized.len() > TX_SLOT_SIZE - mem::size_of::<TxSlotHeader>() {
            return Err(Error::new(ErrorKind::InvalidInput, "Transaction too large"));
        }

        let sig = tx.signatures.first()
            .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "No signature"))?;

        if self.index.read().unwrap().contains_key(sig) {
            return Ok(*sig);
        }

        let offset = self.allocate_slot()?;
        self.write_transaction(offset, sig, &serialized)?;

        self.index.write().unwrap().insert(*sig, offset);
        self.tx_count.fetch_add(1, Ordering::Release);

        if self.should_sync() {
            self.sync_to_disk()?;
        }

        Ok(*sig)
    }

    fn allocate_slot(&self) -> io::Result<u64> {
        if let Some(offset) = self.free_slots.pop() {
            return Ok(offset);
        }

        let offset = self.write_position.fetch_add(TX_SLOT_SIZE as u64, Ordering::AcqRel);
        if offset + TX_SLOT_SIZE as u64 > MAX_MMAP_SIZE as u64 {
            return Err(Error::new(ErrorKind::Other, "Memory map full"));
        }

        Ok(offset)
    }

    fn write_transaction(&self, offset: u64, sig: &Signature, data: &[u8]) -> io::Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;

        let header = TxSlotHeader {
            signature: sig.to_bytes(),
            timestamp,
            size: data.len() as u32,
            flags: 0,
            next_offset: 0,
            checksum: Self::calculate_checksum(data),
            padding: [0; 4],
        };

        let mmap = self.mmap.lock();
        unsafe {
            let slot_ptr = mmap.as_ptr().add(offset as usize) as *mut TxSlotHeader;
            ptr::write_volatile(slot_ptr, header);
            
            let data_ptr = mmap.as_ptr().add(offset as usize + mem::size_of::<TxSlotHeader>()) as *mut u8;
            ptr::copy_nonoverlapping(data.as_ptr(), data_ptr, data.len());
        }

        Ok(())
    }

    pub fn get_transaction(&self, sig: &Signature) -> io::Result<Option<Transaction>> {
        let index = self.index.read().unwrap();
        let offset = match index.get(sig) {
            Some(&off) => off,
            None => return Ok(None),
        };
        drop(index);

        if let Some(cached) = self.slot_cache.lock().get(&offset) {
            return deserialize(cached)
                .map(Some)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e));
        }

        let mmap = self.mmap.lock();
        unsafe {
            let slot = &*(mmap.as_ptr().add(offset as usize) as *const TxSlotHeader);
            if slot.flags == TOMBSTONE_MARKER as u32 {
                return Ok(None);
            }

            let data_ptr = mmap.as_ptr().add(offset as usize + mem::size_of::<TxSlotHeader>());
            let data = slice::from_raw_parts(data_ptr, slot.size as usize);
            
            if !Self::verify_checksum(slot, data_ptr) {
                return Err(Error::new(ErrorKind::InvalidData, "Checksum mismatch"));
            }

            let tx = deserialize(data)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

            if self.slot_cache.lock().len() < 1024 {
                self.slot_cache.lock().insert(offset, data.to_vec());
            }

            Ok(Some(tx))
        }
    }

    pub fn remove_transaction(&self, sig: &Signature) -> io::Result<bool> {
        let mut index = self.index.write().unwrap();
        let offset = match index.remove(sig) {
            Some(off) => off,
            None => return Ok(false),
        };

        let mmap = self.mmap.lock();
        unsafe {
            let slot = &mut *(mmap.as_ptr().add(offset as usize) as *mut TxSlotHeader);
            slot.flags = TOMBSTONE_MARKER as u32;
            slot.next_offset = 0;
        }

        self.free_slots.push(offset);
        self.tx_count.fetch_sub(1, Ordering::Release);
        self.slot_cache.lock().remove(&offset);

        Ok(true)
    }

    pub fn batch_store(&self, transactions: Vec<Transaction>) -> io::Result<Vec<Signature>> {
        let mut signatures = Vec::with_capacity(transactions.len());
        let mut write_buffer = self.write_buffer.lock();

        for tx in transactions {
            let serialized = serialize(&tx).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            if serialized.len() > TX_SLOT_SIZE - mem::size_of::<TxSlotHeader>() {
                continue;
            }

            let sig = tx.signatures.first()
                .ok_or_else(|| Error::new(ErrorKind::InvalidInput, "No signature"))?;

            if !self.index.read().unwrap().contains_key(sig) {
                let offset = self.allocate_slot()?;
                write_buffer.push((tx, offset));
                signatures.push(*sig);
            }
        }

        if !write_buffer.is_empty() {
            self.flush_write_buffer()?;
        }

        Ok(signatures)
    }

    fn flush_write_buffer(&self) -> io::Result<()> {
        let mut buffer = self.write_buffer.lock();
        let mmap = self.mmap.lock();
        let mut index = self.index.write().unwrap();

        for (tx, offset) in buffer.drain(..) {
            let sig = tx.signatures[0];
            let serialized = serialize(&tx).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
            
            let timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64;

            let header = TxSlotHeader {
                signature: sig.to_bytes(),
                timestamp,
                size: serialized.len() as u32,
                flags: 0,
                next_offset: 0,
                checksum: Self::calculate_checksum(&serialized),
                padding: [0; 4],
            };

            unsafe {
                let slot_ptr = mmap.as_ptr().add(offset as usize) as *mut TxSlotHeader;
                ptr::write_volatile(slot_ptr, header);
                
                let data_ptr = mmap.as_ptr().add(offset as usize + mem::size_of::<TxSlotHeader>()) as *mut u8;
                ptr::copy_nonoverlapping(serialized.as_ptr(), data_ptr, serialized.len());
            }

            index.insert(sig, offset);
            self.tx_count.fetch_add(1, Ordering::Release);
        }

        Ok(())
    }

    fn should_sync(&self) -> bool {
        let last = self.last_sync.load(Ordering::Acquire);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        
        now - last > SYNC_INTERVAL
    }

    fn sync_to_disk(&self) -> io::Result<()> {
        let mmap = self.mmap.lock();
        mmap.flush_async()?;
        
        unsafe {
            let header = &mut *(mmap.as_ptr() as *mut MmapHeader);
            header.write_position = self.write_position.load(Ordering::Acquire);
            header.transaction_count = self.tx_count.load(Ordering::Acquire) as u64;
            header.last_sync = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            
            if let Some(free_slot) = self.free_slots.pop() {
                header.free_list_head = free_slot;
                self.free_slots.push(free_slot);
            }
        }

        self.last_sync.store(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            Ordering::Release
        );

        Ok(())
    }

    pub fn get_transaction_with_metadata(&self, sig: &Signature) -> io::Result<Option<(Transaction, u64)>> {
        let index = self.index.read().unwrap();
        let offset = match index.get(sig) {
            Some(&off) => off,
            None => return Ok(None),
        };
        drop(index);

        let mmap = self.mmap.lock();
        unsafe {
            let slot = &*(mmap.as_ptr().add(offset as usize) as *const TxSlotHeader);
            if slot.flags == TOMBSTONE_MARKER as u32 {
                return Ok(None);
            }

            let data_ptr = mmap.as_ptr().add(offset as usize + mem::size_of::<TxSlotHeader>());
            let data = slice::from_raw_parts(data_ptr, slot.size as usize);
            
            if !Self::verify_checksum(slot, data_ptr) {
                return Err(Error::new(ErrorKind::InvalidData, "Checksum mismatch"));
            }

            let tx = deserialize(data)
                .map_err(|e| Error::new(ErrorKind::InvalidData, e))?;

            Ok(Some((tx, slot.timestamp)))
        }
    }

    pub fn scan_transactions_since(&self, timestamp: u64, limit: usize) -> io::Result<Vec<(Signature, Transaction)>> {
        let mut results = Vec::with_capacity(limit.min(1000));
        let mmap = self.mmap.lock();
        let mut offset = HEADER_SIZE;
        let mut scanned = 0;

        while offset < MAX_MMAP_SIZE && results.len() < limit {
            unsafe {
                let slot = &*(mmap.as_ptr().add(offset) as *const TxSlotHeader);
                
                if slot.size > 0 && slot.size < TX_SLOT_SIZE as u32 && slot.flags != TOMBSTONE_MARKER as u32 {
                    if slot.timestamp >= timestamp {
                        let data_ptr = mmap.as_ptr().add(offset + mem::size_of::<TxSlotHeader>());
                        
                        if Self::verify_checksum(slot, data_ptr) {
                            let data = slice::from_raw_parts(data_ptr, slot.size as usize);
                            if let Ok(tx) = deserialize::<Transaction>(data) {
                                let sig = Signature::from(slot.signature);
                                results.push((sig, tx));
                            }
                        }
                    }
                }
            }
            
            offset += TX_SLOT_SIZE;
            scanned += 1;
            
            if scanned % 10000 == 0 && results.len() >= limit / 2 {
                break;
            }
        }

        results.sort_by_key(|(_, tx)| {
            tx.message.recent_blockhash
        });

        Ok(results)
    }

    pub fn compact(&self) -> io::Result<usize> {
        let mut mmap = self.mmap.lock();
        let mut index = self.index.write().unwrap();
        let mut new_index = AHashMap::with_capacity(index.len());
        let mut write_offset = HEADER_SIZE;
        let mut moved_count = 0;
        let mut read_offset = HEADER_SIZE;

        let temp_buffer = vec![0u8; TX_SLOT_SIZE];
        let mut temp_slots = Vec::with_capacity(self.tx_count.load(Ordering::Acquire));

        while read_offset < MAX_MMAP_SIZE {
            unsafe {
                let slot = &*(mmap.as_ptr().add(read_offset) as *const TxSlotHeader);
                
                if slot.size > 0 && slot.size < TX_SLOT_SIZE as u32 && slot.flags != TOMBSTONE_MARKER as u32 {
                    let data_ptr = mmap.as_ptr().add(read_offset + mem::size_of::<TxSlotHeader>());
                    
                    if Self::verify_checksum(slot, data_ptr) {
                        let total_size = mem::size_of::<TxSlotHeader>() + slot.size as usize;
                        let mut slot_data = vec![0u8; total_size];
                        
                        ptr::copy_nonoverlapping(
                            mmap.as_ptr().add(read_offset),
                            slot_data.as_mut_ptr(),
                            total_size
                        );
                        
                        temp_slots.push((write_offset, slot_data, Signature::from(slot.signature)));
                        write_offset += TX_SLOT_SIZE;
                        moved_count += 1;
                    }
                }
            }
            
            read_offset += TX_SLOT_SIZE;
        }

        for (new_offset, slot_data, sig) in temp_slots {
            unsafe {
                ptr::copy_nonoverlapping(
                    slot_data.as_ptr(),
                    mmap.as_mut_ptr().add(new_offset),
                    slot_data.len()
                );
            }
            new_index.insert(sig, new_offset as u64);
        }

        let remaining_space = MAX_MMAP_SIZE - write_offset;
        unsafe {
            ptr::write_bytes(
                mmap.as_mut_ptr().add(write_offset),
                0,
                remaining_space
            );
        }

        *index = new_index;
        self.write_position.store(write_offset as u64, Ordering::Release);
        self.free_slots.clear();
        self.slot_cache.lock().clear();

        unsafe {
            let header = &mut *(mmap.as_ptr() as *mut MmapHeader);
            header.write_position = write_offset as u64;
            header.free_list_head = 0;
        }

        mmap.flush()?;
        Ok(moved_count)
    }

    pub fn transaction_count(&self) -> usize {
        self.tx_count.load(Ordering::Acquire)
    }

    pub fn memory_usage(&self) -> usize {
        self.write_position.load(Ordering::Acquire) as usize
    }

    pub fn get_all_signatures(&self) -> Vec<Signature> {
        self.index.read().unwrap().keys().cloned().collect()
    }

    pub fn contains(&self, sig: &Signature) -> bool {
        self.index.read().unwrap().contains_key(sig)
    }

    pub fn clear_cache(&self) {
        self.slot_cache.lock().clear();
    }

    pub fn prefetch(&self, signatures: &[Signature]) -> io::Result<()> {
        let index = self.index.read().unwrap();
        let mmap = self.mmap.lock();
        let mut cache = self.slot_cache.lock();

        for sig in signatures.iter().take(100) {
            if let Some(&offset) = index.get(sig) {
                if !cache.contains_key(&offset) && cache.len() < 1024 {
                    unsafe {
                        let slot = &*(mmap.as_ptr().add(offset as usize) as *const TxSlotHeader);
                        if slot.size > 0 && slot.flags != TOMBSTONE_MARKER as u32 {
                            let data_ptr = mmap.as_ptr().add(offset as usize + mem::size_of::<TxSlotHeader>());
                            let data = slice::from_raw_parts(data_ptr, slot.size as usize);
                            cache.insert(offset, data.to_vec());
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub fn optimize_layout(&self) -> io::Result<()> {
        let index = self.index.read().unwrap();
        let mut access_patterns: Vec<(u64, usize)> = Vec::with_capacity(index.len());
        
        for &offset in index.values() {
            access_patterns.push((offset, 1));
        }

        access_patterns.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

        let hot_zone_size = (access_patterns.len() / 10).max(100);
        let mut cache = self.slot_cache.lock();
        cache.clear();

        let mmap = self.mmap.lock();
        for (offset, _) in access_patterns.iter().take(hot_zone_size) {
            unsafe {
                let slot = &*(mmap.as_ptr().add(*offset as usize) as *const TxSlotHeader);
                if slot.size > 0 && slot.flags != TOMBSTONE_MARKER as u32 {
                    let data_ptr = mmap.as_ptr().add(*offset as usize + mem::size_of::<TxSlotHeader>());
                    let data = slice::from_raw_parts(data_ptr, slot.size as usize);
                    cache.insert(*offset, data.to_vec());
                }
            }
        }

        Ok(())
    }

    pub fn repair(&self) -> io::Result<usize> {
        let mut mmap = self.mmap.lock();
        let mut repaired = 0;
        let mut offset = HEADER_SIZE;
        let mut new_index = AHashMap::with_capacity(INDEX_CACHE_SIZE);

        while offset < MAX_MMAP_SIZE {
            unsafe {
                let slot = &mut *(mmap.as_ptr().add(offset) as *mut TxSlotHeader);
                
                if slot.size > 0 && slot.size < TX_SLOT_SIZE as u32 {
                    let data_ptr = mmap.as_ptr().add(offset + mem::size_of::<TxSlotHeader>());
                    
                    if slot.flags != TOMBSTONE_MARKER as u32 {
                        if !Self::verify_checksum(slot, data_ptr) {
                            let data = slice::from_raw_parts(data_ptr, slot.size as usize);
                            slot.checksum = Self::calculate_checksum(data);
                            repaired += 1;
                        }
                        
                        let sig = Signature::from(slot.signature);
                        new_index.insert(sig, offset as u64);
                    }
                }
            }
            
            offset += TX_SLOT_SIZE;
        }

        *self.index.write().unwrap() = new_index;
        self.tx_count.store(new_index.len(), Ordering::Release);
        
        Ok(repaired)
    }

    pub fn force_sync(&self) -> io::Result<()> {
        let mmap = self.mmap.lock();
        mmap.flush()?;
        
        unsafe {
            let header = &mut *(mmap.as_ptr() as *mut MmapHeader);
            header.write_position = self.write_position.load(Ordering::Acquire);
            header.transaction_count = self.tx_count.load(Ordering::Acquire) as u64;
            header.last_sync = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
        }

        self.file.lock().sync_all()?;
        Ok(())
    }
}

impl Drop for MemoryMappedTransactions {
    fn drop(&mut self) {
        let _ = self.force_sync();
    }
}

unsafe impl Send for MemoryMappedTransactions {}
unsafe impl Sync for MemoryMappedTransactions {}

#[cfg(target_os = "linux")]
impl MemoryMappedTransactions {
    pub fn enable_huge_pages(&self) -> io::Result<()> {
        use libc::{madvise, MADV_HUGEPAGE};
        
        let mmap = self.mmap.lock();
        let ret = unsafe {
            madvise(
                mmap.as_ptr() as *mut libc::c_void,
                mmap.len(),
                MADV_HUGEPAGE
            )
        };
        
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
        
        Ok(())
    }

    pub fn lock_memory(&self) -> io::Result<()> {
        use libc::{mlock, ML_CURRENT};
        
        let mmap = self.mmap.lock();
        let ret = unsafe {
            mlock(
                mmap.as_ptr() as *const libc::c_void,
                mmap.len()
            )
        };
        
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
        
        Ok(())
    }
}

impl MemoryMappedTransactions {
    pub fn batch_get(&self, signatures: &[Signature]) -> io::Result<Vec<Option<Transaction>>> {
        let mut results = Vec::with_capacity(signatures.len());
        let index = self.index.read().unwrap();
        let mmap = self.mmap.lock();
        
        for sig in signatures {
            if let Some(&offset) = index.get(sig) {
                unsafe {
                    let slot = &*(mmap.as_ptr().add(offset as usize) as *const TxSlotHeader);
                    
                    if slot.flags == TOMBSTONE_MARKER as u32 {
                        results.push(None);
                        continue;
                    }
                    
                    let data_ptr = mmap.as_ptr().add(offset as usize + mem::size_of::<TxSlotHeader>());
                    let data = slice::from_raw_parts(data_ptr, slot.size as usize);
                    
                    if Self::verify_checksum(slot, data_ptr) {
                        match deserialize(data) {
                            Ok(tx) => results.push(Some(tx)),
                            Err(_) => results.push(None),
                        }
                    } else {
                        results.push(None);
                    }
                }
            } else {
                results.push(None);
            }
        }
        
        Ok(results)
    }

    pub fn batch_remove(&self, signatures: &[Signature]) -> io::Result<usize> {
        let mut index = self.index.write().unwrap();
        let mmap = self.mmap.lock();
        let mut removed = 0;
        
        for sig in signatures {
            if let Some(offset) = index.remove(sig) {
                unsafe {
                    let slot = &mut *(mmap.as_ptr().add(offset as usize) as *mut TxSlotHeader);
                    slot.flags = TOMBSTONE_MARKER as u32;
                    slot.next_offset = 0;
                }
                
                self.free_slots.push(offset);
                removed += 1;
            }
        }
        
        self.tx_count.fetch_sub(removed, Ordering::Release);
        
        let mut cache = self.slot_cache.lock();
        for sig in signatures {
            if let Some(&offset) = index.get(sig) {
                cache.remove(&offset);
            }
        }
        
        Ok(removed)
    }

    pub fn get_recent_transactions(&self, count: usize) -> io::Result<Vec<(Signature, Transaction, u64)>> {
        let mut transactions = Vec::with_capacity(count);
        let mmap = self.mmap.lock();
        let write_pos = self.write_position.load(Ordering::Acquire);
        
        let mut offset = write_pos.saturating_sub((count * TX_SLOT_SIZE * 2) as u64);
        offset = offset.max(HEADER_SIZE as u64);
        offset = (offset / TX_SLOT_SIZE as u64) * TX_SLOT_SIZE as u64;
        
        while offset < write_pos && transactions.len() < count {
            unsafe {
                let slot = &*(mmap.as_ptr().add(offset as usize) as *const TxSlotHeader);
                
                if slot.size > 0 && slot.size < TX_SLOT_SIZE as u32 && slot.flags != TOMBSTONE_MARKER as u32 {
                    let data_ptr = mmap.as_ptr().add(offset as usize + mem::size_of::<TxSlotHeader>());
                    
                    if Self::verify_checksum(slot, data_ptr) {
                        let data = slice::from_raw_parts(data_ptr, slot.size as usize);
                        if let Ok(tx) = deserialize::<Transaction>(data) {
                            let sig = Signature::from(slot.signature);
                            transactions.push((sig, tx, slot.timestamp));
                        }
                    }
                }
            }
            
            offset += TX_SLOT_SIZE as u64;
        }
        
        transactions.sort_by_key(|(_, _, timestamp)| std::cmp::Reverse(*timestamp));
        transactions.truncate(count);
        
        Ok(transactions)
    }

    pub fn estimate_size(&self, tx: &Transaction) -> io::Result<usize> {
        let serialized = serialize(tx).map_err(|e| Error::new(ErrorKind::InvalidData, e))?;
        Ok(mem::size_of::<TxSlotHeader>() + serialized.len())
    }

    pub fn available_space(&self) -> usize {
        let used = self.write_position.load(Ordering::Acquire) as usize;
        let free_slots = self.free_slots.len() * TX_SLOT_SIZE;
        MAX_MMAP_SIZE - used + free_slots
    }

    pub fn defragment(&self) -> io::Result<()> {
        if self.free_slots.len() > 1000 {
            self.compact()?;
        }
        Ok(())
    }

    pub fn validate_integrity(&self) -> io::Result<(usize, usize)> {
        let mmap = self.mmap.lock();
        let mut valid = 0;
        let mut invalid = 0;
        let mut offset = HEADER_SIZE;
        
        while offset < self.write_position.load(Ordering::Acquire) as usize {
            unsafe {
                let slot = &*(mmap.as_ptr().add(offset) as *const TxSlotHeader);
                
                if slot.size > 0 && slot.size < TX_SLOT_SIZE as u32 && slot.flags != TOMBSTONE_MARKER as u32 {
                    let data_ptr = mmap.as_ptr().add(offset + mem::size_of::<TxSlotHeader>());
                    
                    if Self::verify_checksum(slot, data_ptr) {
                        valid += 1;
                    } else {
                        invalid += 1;
                    }
                }
            }
            
            offset += TX_SLOT_SIZE;
        }
        
        Ok((valid, invalid))
    }

    pub fn export_transactions<F>(&self, mut callback: F) -> io::Result<usize>
    where
        F: FnMut(Signature, Transaction) -> bool,
    {
        let mmap = self.mmap.lock();
        let mut exported = 0;
        let mut offset = HEADER_SIZE;
        
        while offset < MAX_MMAP_SIZE {
            unsafe {
                let slot = &*(mmap.as_ptr().add(offset as usize) as *const TxSlotHeader);
                
                if slot.size > 0 && slot.size < TX_SLOT_SIZE as u32 && slot.flags != TOMBSTONE_MARKER as u32 {
                    let data_ptr = mmap.as_ptr().add(offset + mem::size_of::<TxSlotHeader>());
                    
                    if Self::verify_checksum(slot, data_ptr) {
                        let data = slice::from_raw_parts(data_ptr, slot.size as usize);
                        if let Ok(tx) = deserialize::<Transaction>(data) {
                            let sig = Signature::from(slot.signature);
                            if !callback(sig, tx) {
                                break;
                            }
                            exported += 1;
                        }
                    }
                }
            }
            
            offset += TX_SLOT_SIZE;
        }
        
        Ok(exported)
    }

    pub fn get_stats(&self) -> MemoryMapStats {
        let total_slots = (MAX_MMAP_SIZE - HEADER_SIZE) / TX_SLOT_SIZE;
        let used_slots = self.tx_count.load(Ordering::Acquire);
        let free_slot_count = self.free_slots.len();
        let cache_size = self.slot_cache.lock().len();
        let write_pos = self.write_position.load(Ordering::Acquire);
        
        MemoryMapStats {
            total_capacity: MAX_MMAP_SIZE,
            used_bytes: write_pos as usize,
            transaction_count: used_slots,
            total_slots,
            free_slots: free_slot_count,
            cache_entries: cache_size,
            fragmentation_ratio: (free_slot_count as f64) / (total_slots as f64),
        }
    }

    pub fn warm_cache(&self, limit: usize) -> io::Result<usize> {
        let index = self.index.read().unwrap();
        let mut cache = self.slot_cache.lock();
        let mmap = self.mmap.lock();
        let mut warmed = 0;
        
        for &offset in index.values().take(limit) {
            if cache.contains_key(&offset) {
                continue;
            }
            
            unsafe {
                let slot = &*(mmap.as_ptr().add(offset as usize) as *const TxSlotHeader);
                if slot.size > 0 && slot.flags != TOMBSTONE_MARKER as u32 {
                    let data_ptr = mmap.as_ptr().add(offset as usize + mem::size_of::<TxSlotHeader>());
                    let data = slice::from_raw_parts(data_ptr, slot.size as usize);
                    
                    if cache.len() < 1024 {
                        cache.insert(offset, data.to_vec());
                        warmed += 1;
                    } else {
                        break;
                    }
                }
            }
        }
        
        Ok(warmed)
    }

    pub fn resize(&self, new_size: usize) -> io::Result<()> {
        if new_size <= MAX_MMAP_SIZE {
            return Err(Error::new(ErrorKind::InvalidInput, "Cannot shrink memory map"));
        }
        
        if new_size > 1 << 31 {
            return Err(Error::new(ErrorKind::InvalidInput, "Size too large"));
        }
        
        self.force_sync()?;
        
        let file = self.file.lock();
        file.set_len(new_size as u64)?;
        
        Ok(())
    }
}

pub struct MemoryMapStats {
    pub total_capacity: usize,
    pub used_bytes: usize,
    pub transaction_count: usize,
    pub total_slots: usize,
    pub free_slots: usize,
    pub cache_entries: usize,
    pub fragmentation_ratio: f64,
}

impl MemoryMapStats {
    pub fn utilization(&self) -> f64 {
        (self.used_bytes as f64) / (self.total_capacity as f64)
    }

    pub fn average_transaction_size(&self) -> usize {
        if self.transaction_count == 0 {
            0
        } else {
            self.used_bytes / self.transaction_count
        }
    }
}
