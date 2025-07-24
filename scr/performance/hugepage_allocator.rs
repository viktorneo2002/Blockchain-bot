use std::alloc::{GlobalAlloc, Layout};
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::collections::{HashMap, BTreeMap};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::io::AsRawFd;
use std::time::{Duration, Instant};
use std::mem::{self, MaybeUninit};
use libc::{
    c_void, size_t, mmap, munmap, mremap, madvise, mlock, munlock, mlockall,
    MAP_ANONYMOUS, MAP_FAILED, MAP_HUGETLB, MAP_PRIVATE, MAP_POPULATE, MAP_LOCKED,
    PROT_READ, PROT_WRITE, MADV_HUGEPAGE, MADV_DONTFORK, MADV_SEQUENTIAL, MADV_WILLNEED,
    MREMAP_MAYMOVE, MCL_CURRENT, MCL_FUTURE, MAP_HUGE_2MB, MAP_HUGE_1GB,
    ENOMEM, EINVAL, sysconf, _SC_PAGESIZE
};
use once_cell::sync::Lazy;
use crossbeam_utils::CachePadded;

// Constants optimized for Solana MEV operations
const HUGE_PAGE_SIZE_2MB: usize = 2 * 1024 * 1024;
const HUGE_PAGE_SIZE_1GB: usize = 1024 * 1024 * 1024;
const MIN_ALLOCATION_SIZE: usize = 64;
const CACHE_LINE_SIZE: usize = 64;
const MAX_ALLOCATION_RETRIES: usize = 5;
const MEMORY_POOL_INITIAL_SIZE: usize = 256 * 1024 * 1024;
const MAX_FREE_LIST_FRAGMENTS: usize = 1024;
const DEFRAG_THRESHOLD: f64 = 0.3;
const STATS_UPDATE_INTERVAL: u64 = 1000;
const ALLOCATION_GUARD_SIZE: usize = 64;

// Huge page flags for different architectures
#[cfg(target_arch = "x86_64")]
const MAP_HUGE_SHIFT: i32 = 26;
#[cfg(not(target_arch = "x86_64"))]
const MAP_HUGE_SHIFT: i32 = 26;

#[repr(C, align(64))]
#[derive(Debug)]
struct AllocationMetadata {
    size: usize,
    original_size: usize,
    layout_size: usize,
    layout_align: usize,
    allocation_time: u64,
    thread_id: u64,
    checksum: u32,
    flags: u32,
    generation: u64,
    _padding: [u8; 24],
}

impl AllocationMetadata {
    fn new(layout: Layout, actual_size: usize, thread_id: u64, generation: u64) -> Self {
        let metadata = Self {
            size: actual_size,
            original_size: layout.size(),
            layout_size: layout.size(),
            layout_align: layout.align(),
            allocation_time: get_timestamp(),
            thread_id,
            checksum: 0,
            flags: 0,
            generation,
            _padding: [0; 24],
        };
        metadata
    }

    fn calculate_checksum(&self) -> u32 {
        let data = unsafe {
            std::slice::from_raw_parts(
                self as *const _ as *const u8,
                std::mem::size_of::<Self>() - 4
            )
        };
        data.iter().fold(0u32, |acc, &byte| acc.wrapping_add(byte as u32))
    }

    fn verify(&self) -> bool {
        self.checksum == self.calculate_checksum()
    }
}

#[repr(C, align(64))]
struct MemoryChunk {
    base_addr: NonNull<u8>,
    size: usize,
    chunk_type: ChunkType,
    used: CachePadded<AtomicUsize>,
    peak_used: AtomicUsize,
    free_list: RwLock<BTreeMap<usize, usize>>,
    allocations: RwLock<HashMap<usize, AllocationMetadata>>,
    chunk_id: u64,
    creation_time: Instant,
    last_defrag: AtomicU64,
    allocation_count: CachePadded<AtomicUsize>,
    deallocation_count: CachePadded<AtomicUsize>,
    fragmentation_score: AtomicUsize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkType {
    HugePage2MB,
    HugePage1GB,
    Standard,
}

impl MemoryChunk {
    fn new(base_addr: NonNull<u8>, size: usize, chunk_type: ChunkType, chunk_id: u64) -> Self {
        let mut free_list = BTreeMap::new();
        free_list.insert(0, size);

        Self {
            base_addr,
            size,
            chunk_type,
            used: CachePadded::new(AtomicUsize::new(0)),
            peak_used: AtomicUsize::new(0),
            free_list: RwLock::new(free_list),
            allocations: RwLock::new(HashMap::with_capacity(1024)),
            chunk_id,
            creation_time: Instant::now(),
            last_defrag: AtomicU64::new(0),
            allocation_count: CachePadded::new(AtomicUsize::new(0)),
            deallocation_count: CachePadded::new(AtomicUsize::new(0)),
            fragmentation_score: AtomicUsize::new(0),
        }
    }

    fn calculate_fragmentation(&self) -> f64 {
        let free_list = self.free_list.read().unwrap();
        if free_list.is_empty() {
            return 0.0;
        }

        let total_free: usize = free_list.values().sum();
        let largest_free = free_list.values().max().copied().unwrap_or(0);
        
        if total_free == 0 {
            return 0.0;
        }

        1.0 - (largest_free as f64 / total_free as f64)
    }

    fn update_peak_usage(&self, current_used: usize) {
        let mut peak = self.peak_used.load(Ordering::Relaxed);
        while current_used > peak {
            match self.peak_used.compare_exchange_weak(
                peak,
                current_used,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
    }
}

impl Drop for MemoryChunk {
    fn drop(&mut self) {
        unsafe {
            let result = munmap(self.base_addr.as_ptr() as *mut c_void, self.size);
            if result != 0 {
                eprintln!("Failed to unmap memory chunk {}: {}", self.chunk_id, io::Error::last_os_error());
            }
        }
    }
}

pub struct HugePageAllocator {
    chunks: Arc<RwLock<Vec<Arc<MemoryChunk>>>>,
    total_allocated: CachePadded<AtomicUsize>,
    total_capacity: AtomicUsize,
    allocation_counter: CachePadded<AtomicU64>,
    stats: Arc<AllocationStats>,
    config: AllocatorConfig,
    shutdown: AtomicBool,
    defrag_thread: Option<std::thread::JoinHandle<()>>,
    monitor_thread: Option<std::thread::JoinHandle<()>>,
}

#[derive(Clone)]
struct AllocatorConfig {
    enable_2mb_pages: bool,
    enable_1gb_pages: bool,
    initial_2mb_chunks: usize,
    initial_1gb_chunks: usize,
    max_chunks: usize,
    enable_defrag: bool,
    defrag_interval: Duration,
    enable_monitoring: bool,
    monitor_interval: Duration,
    fallback_to_system: bool,
    lock_memory: bool,
    numa_aware: bool,
}

impl Default for AllocatorConfig {
    fn default() -> Self {
        Self {
            enable_2mb_pages: true,
            enable_1gb_pages: true,
            initial_2mb_chunks: 8,
            initial_1gb_chunks: 2,
            max_chunks: 64,
            enable_defrag: true,
            defrag_interval: Duration::from_secs(60),
            enable_monitoring: true,
            monitor_interval: Duration::from_secs(10),
            fallback_to_system: true,
            lock_memory: true,
            numa_aware: false,
        }
    }
}

#[derive(Default)]
struct AllocationStats {
    successful_allocations: CachePadded<AtomicUsize>,
    failed_allocations: CachePadded<AtomicUsize>,
    total_bytes_allocated: AtomicUsize,
    total_bytes_freed: AtomicUsize,
    current_bytes_allocated: AtomicUsize,
    peak_bytes_allocated: AtomicUsize,
    allocation_time_ns: AtomicU64,
    deallocation_time_ns: AtomicU64,
    defrag_count: AtomicUsize,
    defrag_time_ns: AtomicU64,
}

impl HugePageAllocator {
    pub fn new() -> io::Result<Self> {
        Self::with_config(AllocatorConfig::default())
    }

    pub fn with_config(config: AllocatorConfig) -> io::Result<Self> {
        Self::verify_system_support()?;
        Self::configure_system_hugepages(&config)?;

        if config.lock_memory {
            unsafe {
                if mlockall(MCL_CURRENT | MCL_FUTURE) != 0 {
                    eprintln!("Warning: Failed to lock memory pages: {}", io::Error::last_os_error());
                }
            }
        }

        let allocator = Self {
            chunks: Arc::new(RwLock::new(Vec::with_capacity(config.max_chunks))),
            total_allocated: CachePadded::new(AtomicUsize::new(0)),
            total_capacity: AtomicUsize::new(0),
            allocation_counter: CachePadded::new(AtomicU64::new(0)),
            stats: Arc::new(AllocationStats::default()),
            config: config.clone(),
            shutdown: AtomicBool::new(false),
            defrag_thread: None,
            monitor_thread: None,
        };

        allocator.preallocate_chunks()?;

        Ok(allocator)
    }

    fn verify_system_support() -> io::Result<()> {
        let page_size = unsafe { sysconf(_SC_PAGESIZE) };
        if page_size <= 0 {
            return Err(io::Error::new(io::ErrorKind::Other, "Failed to get page size"));
        }

        // Check if huge pages are available
        let hugepages_file = std::fs::read_to_string("/proc/meminfo")?;
        if !hugepages_file.contains("HugePages") {
            return Err(io::Error::new(io::ErrorKind::Other, "Huge pages not supported"));
        }

        Ok(())
    }

    fn configure_system_hugepages(config: &AllocatorConfig) -> io::Result<()> {
        if config.enable_2mb_pages {
            if let Ok(mut file) = OpenOptions::new()
                .write(true)
                .open("/proc/sys/vm/nr_hugepages")
            {
                let _ = file.write_all(format!("{}\n", config.initial_2mb_chunks * 2).as_bytes());
            }
        }

        if config.enable_1gb_pages {
            if let Ok(mut file) = OpenOptions::new()
                .write(true)
                .open("/sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages")
            {
                let _ = file.write_all(format!("{}\n", config.initial_1gb_chunks * 2).as_bytes());
            }
        }

        // Enable transparent huge pages
        if let Ok(mut file) = OpenOptions::new()
            .write(true)
            .open("/sys/kernel/mm/transparent_hugepage/enabled")
        {
            let _ = file.write_all(b"always\n");
        }

        Ok(())
    }

    fn preallocate_chunks(&self) -> io::Result<()> {
        let mut chunks = self.chunks.write().unwrap();
        
        if self.config.enable_2mb_pages {
            for i in 0..self.config.initial_2mb_chunks {
                match self.allocate_chunk_internal(HUGE_PAGE_SIZE_2MB, ChunkType::HugePage2MB) {
                    Ok(chunk) => {
                        self.total_capacity.fetch_add(chunk.size, Ordering::SeqCst);
                        chunks.push(Arc::new(chunk));
                    }
                    Err(e) => {
                        if i == 0 {
                            eprintln!("Warning: Failed to allocate 2MB huge page: {}", e);
                        }
                        break;
                    }
                }
            }
        }

                if self.config.enable_1gb_pages {
            for i in 0..self.config.initial_1gb_chunks {
                match self.allocate_chunk_internal(HUGE_PAGE_SIZE_1GB, ChunkType::HugePage1GB) {
                    Ok(chunk) => {
                        self.total_capacity.fetch_add(chunk.size, Ordering::SeqCst);
                        chunks.push(Arc::new(chunk));
                    }
                    Err(e) => {
                        if i == 0 {
                            eprintln!("Warning: Failed to allocate 1GB huge page: {}", e);
                        }
                        break;
                    }
                }
            }
        }

        if chunks.is_empty() && self.config.fallback_to_system {
            // Fallback to standard pages if huge pages unavailable
            for _ in 0..2 {
                match self.allocate_chunk_internal(16 * 1024 * 1024, ChunkType::Standard) {
                    Ok(chunk) => {
                        self.total_capacity.fetch_add(chunk.size, Ordering::SeqCst);
                        chunks.push(Arc::new(chunk));
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        drop(chunks);

        if self.config.enable_defrag {
            self.start_defrag_thread();
        }

        if self.config.enable_monitoring {
            self.start_monitor_thread();
        }

        Ok(())
    }

    fn allocate_chunk_internal(&self, size: usize, chunk_type: ChunkType) -> io::Result<MemoryChunk> {
        let mut flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE;
        
        match chunk_type {
            ChunkType::HugePage2MB => {
                flags |= MAP_HUGETLB | (21 << MAP_HUGE_SHIFT);
            }
            ChunkType::HugePage1GB => {
                flags |= MAP_HUGETLB | (30 << MAP_HUGE_SHIFT);
            }
            ChunkType::Standard => {
                // No additional flags needed
            }
        }

        if self.config.lock_memory {
            flags |= MAP_LOCKED;
        }

        let mut retry_count = 0;
        let mut last_error = None;

        while retry_count < MAX_ALLOCATION_RETRIES {
            let ptr = unsafe {
                mmap(
                    ptr::null_mut(),
                    size,
                    PROT_READ | PROT_WRITE,
                    flags,
                    -1,
                    0,
                )
            };

            if ptr != MAP_FAILED {
                let base_addr = NonNull::new(ptr as *mut u8)
                    .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Invalid pointer from mmap"))?;

                unsafe {
                    // Advise kernel about usage patterns
                    madvise(ptr, size, MADV_HUGEPAGE);
                    madvise(ptr, size, MADV_WILLNEED);
                    madvise(ptr, size, MADV_DONTFORK);

                    if self.config.lock_memory {
                        if mlock(ptr, size) != 0 {
                            eprintln!("Warning: Failed to lock memory: {}", io::Error::last_os_error());
                        }
                    }

                    // Touch all pages to ensure they're allocated
                    let page_size = if chunk_type == ChunkType::HugePage1GB {
                        HUGE_PAGE_SIZE_1GB
                    } else if chunk_type == ChunkType::HugePage2MB {
                        HUGE_PAGE_SIZE_2MB
                    } else {
                        4096
                    };

                    for offset in (0..size).step_by(page_size) {
                        ptr::write_volatile((ptr as *mut u8).add(offset), 0);
                    }
                }

                let chunk_id = self.allocation_counter.fetch_add(1, Ordering::SeqCst);
                return Ok(MemoryChunk::new(base_addr, size, chunk_type, chunk_id));
            }

            last_error = Some(io::Error::last_os_error());
            retry_count += 1;

            if retry_count < MAX_ALLOCATION_RETRIES {
                std::thread::sleep(Duration::from_millis(10 * retry_count as u64));
            }
        }

        Err(last_error.unwrap_or_else(|| io::Error::new(io::ErrorKind::Other, "Failed to allocate memory")))
    }

    fn find_suitable_chunk(&self, layout: Layout) -> Option<Arc<MemoryChunk>> {
        let chunks = self.chunks.read().unwrap();
        let required_size = layout.size() + ALLOCATION_GUARD_SIZE;
        let align = layout.align().max(MIN_ALLOCATION_SIZE);

        // Try to find chunk with best fit
        let mut best_chunk = None;
        let mut best_fit_size = usize::MAX;

        for chunk in chunks.iter() {
            let free_list = chunk.free_list.read().unwrap();
            
            for (&offset, &chunk_size) in free_list.iter() {
                let aligned_offset = align_offset(offset, align);
                let padding = aligned_offset - offset;
                
                if chunk_size >= required_size + padding {
                    let waste = chunk_size - required_size - padding;
                    if waste < best_fit_size {
                        best_fit_size = waste;
                        best_chunk = Some(Arc::clone(chunk));
                        if waste < align {
                            // Nearly perfect fit
                            return best_chunk;
                        }
                    }
                }
            }
        }

        best_chunk
    }

    fn allocate_from_chunk(&self, chunk: &Arc<MemoryChunk>, layout: Layout) -> Option<NonNull<u8>> {
        let size = layout.size();
        let align = layout.align().max(MIN_ALLOCATION_SIZE);
        let total_size = size + ALLOCATION_GUARD_SIZE;

        let mut free_list = chunk.free_list.write().unwrap();
        let mut allocations = chunk.allocations.write().unwrap();

        // Find suitable free block
        let mut selected_block = None;
        for (&offset, &block_size) in free_list.iter() {
            let aligned_offset = align_offset(offset, align);
            let padding = aligned_offset - offset;
            
            if block_size >= total_size + padding {
                selected_block = Some((offset, block_size, aligned_offset, padding));
                break;
            }
        }

        if let Some((offset, block_size, aligned_offset, padding)) = selected_block {
            // Remove the free block
            free_list.remove(&offset);

            // Add padding back to free list if significant
            if padding >= MIN_ALLOCATION_SIZE {
                free_list.insert(offset, padding);
            }

            // Add remaining space back to free list
            let remaining = block_size - total_size - padding;
            if remaining >= MIN_ALLOCATION_SIZE {
                free_list.insert(aligned_offset + total_size, remaining);
            }

            // Create allocation metadata
            let thread_id = std::thread::current().id().as_u64().get();
            let metadata = AllocationMetadata::new(layout, total_size, thread_id, chunk.chunk_id);
            allocations.insert(aligned_offset, metadata);

            // Update statistics
            let new_used = chunk.used.fetch_add(total_size, Ordering::SeqCst) + total_size;
            chunk.update_peak_usage(new_used);
            chunk.allocation_count.fetch_add(1, Ordering::Relaxed);

            self.stats.successful_allocations.fetch_add(1, Ordering::Relaxed);
            self.stats.total_bytes_allocated.fetch_add(size, Ordering::Relaxed);
            self.stats.current_bytes_allocated.fetch_add(size, Ordering::Relaxed);
            self.update_peak_allocation(self.stats.current_bytes_allocated.load(Ordering::Relaxed));

            unsafe {
                let ptr = chunk.base_addr.as_ptr().add(aligned_offset);
                // Clear the memory for security
                ptr::write_bytes(ptr, 0, total_size);
                NonNull::new(ptr)
            }
        } else {
            None
        }
    }

    fn deallocate_from_chunk(&self, chunk: &Arc<MemoryChunk>, ptr: *mut u8) -> bool {
        let base = chunk.base_addr.as_ptr() as usize;
        let addr = ptr as usize;
        
        if addr < base || addr >= base + chunk.size {
            return false;
        }

        let offset = addr - base;
        let mut free_list = chunk.free_list.write().unwrap();
        let mut allocations = chunk.allocations.write().unwrap();

        if let Some(metadata) = allocations.remove(&offset) {
            // Verify metadata integrity
            if !metadata.verify() {
                eprintln!("Warning: Corrupted allocation metadata detected at offset {}", offset);
            }

            let size = metadata.size;
            chunk.used.fetch_sub(size, Ordering::SeqCst);
            chunk.deallocation_count.fetch_add(1, Ordering::Relaxed);

            self.stats.total_bytes_freed.fetch_add(metadata.original_size, Ordering::Relaxed);
            self.stats.current_bytes_allocated.fetch_sub(metadata.original_size, Ordering::Relaxed);

            // Clear memory before returning to pool
            unsafe {
                ptr::write_bytes(ptr, 0, size);
            }

            // Coalesce with adjacent free blocks
            let mut coalesced_start = offset;
            let mut coalesced_size = size;

            // Check for adjacent free block before
            let mut blocks_to_remove = Vec::new();
            for (&free_offset, &free_size) in free_list.iter() {
                if free_offset + free_size == coalesced_start {
                    coalesced_start = free_offset;
                    coalesced_size += free_size;
                    blocks_to_remove.push(free_offset);
                } else if coalesced_start + coalesced_size == free_offset {
                    coalesced_size += free_size;
                    blocks_to_remove.push(free_offset);
                }
            }

            // Remove merged blocks
            for block in blocks_to_remove {
                free_list.remove(&block);
            }

            // Insert coalesced block
            free_list.insert(coalesced_start, coalesced_size);

            // Update fragmentation score
            let fragmentation = chunk.calculate_fragmentation();
            chunk.fragmentation_score.store((fragmentation * 1000.0) as usize, Ordering::Relaxed);

            true
        } else {
            false
        }
    }

    fn update_peak_allocation(&self, current: usize) {
        let mut peak = self.stats.peak_bytes_allocated.load(Ordering::Relaxed);
        while current > peak {
            match self.stats.peak_bytes_allocated.compare_exchange_weak(
                peak,
                current,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
    }

    fn start_defrag_thread(&mut self) {
        let chunks = Arc::clone(&self.chunks);
        let stats = Arc::clone(&self.stats);
        let shutdown = self.shutdown.clone();
        let interval = self.config.defrag_interval;

        let handle = std::thread::spawn(move || {
            while !shutdown.load(Ordering::Acquire) {
                std::thread::sleep(interval);
                
                let start = Instant::now();
                defragment_chunks(&chunks, &stats);
                let elapsed = start.elapsed().as_nanos() as u64;
                
                stats.defrag_count.fetch_add(1, Ordering::Relaxed);
                stats.defrag_time_ns.fetch_add(elapsed, Ordering::Relaxed);
            }
        });

        self.defrag_thread = Some(handle);
    }

    fn start_monitor_thread(&mut self) {
        let chunks = Arc::clone(&self.chunks);
        let stats = Arc::clone(&self.stats);
        let shutdown = self.shutdown.clone();
        let interval = self.config.monitor_interval;

        let handle = std::thread::spawn(move || {
            while !shutdown.load(Ordering::Acquire) {
                std::thread::sleep(interval);
                monitor_memory_pressure(&chunks, &stats);
            }
        });

        self.monitor_thread = Some(handle);
    }

    pub fn force_defragmentation(&self) {
        let start = Instant::now();
        defragment_chunks(&self.chunks, &self.stats);
        let elapsed = start.elapsed().as_nanos() as u64;
        
        self.stats.defrag_count.fetch_add(1, Ordering::Relaxed);
        self.stats.defrag_time_ns.fetch_add(elapsed, Ordering::Relaxed);
    }

    pub fn get_metrics(&self) -> AllocationMetrics {
        let chunks = self.chunks.read().unwrap();
        
        let mut total_used = 0;
        let mut total_capacity = 0;
        let mut chunk_count_2mb = 0;
        let mut chunk_count_1gb = 0;
        let mut fragmentation_sum = 0.0;
        
        for chunk in chunks.iter() {
            total_used += chunk.used.load(Ordering::Relaxed);
            total_capacity += chunk.size;
            
            match chunk.chunk_type {
                ChunkType::HugePage2MB => chunk_count_2mb += 1,
                ChunkType::HugePage1GB => chunk_count_1gb += 1,
                ChunkType::Standard => {}
            }
            
            fragmentation_sum += chunk.calculate_fragmentation();
        }
        
        let avg_fragmentation = if !chunks.is_empty() {
            fragmentation_sum / chunks.len() as f64
        } else {
            0.0
        };

        AllocationMetrics {
            total_allocated: self.stats.total_bytes_allocated.load(Ordering::Relaxed),
            total_freed: self.stats.total_bytes_freed.load(Ordering::Relaxed),
            current_allocated: self.stats.current_bytes_allocated.load(Ordering::Relaxed),
            peak_allocated: self.stats.peak_bytes_allocated.load(Ordering::Relaxed),
            successful_allocations: self.stats.successful_allocations.load(Ordering::Relaxed),
            failed_allocations: self.stats.failed_allocations.load(Ordering::Relaxed),
            chunk_count_2mb,
            chunk_count_1gb,
            total_capacity,
                        fragmentation_ratio: avg_fragmentation,
            defrag_count: self.stats.defrag_count.load(Ordering::Relaxed),
            average_allocation_ns: {
                let count = self.stats.successful_allocations.load(Ordering::Relaxed);
                if count > 0 {
                    self.stats.allocation_time_ns.load(Ordering::Relaxed) / count as u64
                } else {
                    0
                }
            },
            memory_efficiency: if total_capacity > 0 {
                (total_used as f64 / total_capacity as f64) * 100.0
            } else {
                0.0
            },
        }
    }
}

unsafe impl GlobalAlloc for HugePageAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let start = Instant::now();
        
        // Validate layout
        if layout.size() == 0 {
            return ptr::null_mut();
        }

        let aligned_layout = match layout.align_to(MIN_ALLOCATION_SIZE) {
            Ok(l) => l,
            Err(_) => {
                self.stats.failed_allocations.fetch_add(1, Ordering::Relaxed);
                return ptr::null_mut();
            }
        };

        // Try existing chunks first
        if let Some(chunk) = self.find_suitable_chunk(aligned_layout) {
            if let Some(ptr) = self.allocate_from_chunk(&chunk, aligned_layout) {
                let elapsed = start.elapsed().as_nanos() as u64;
                self.stats.allocation_time_ns.fetch_add(elapsed, Ordering::Relaxed);
                return ptr.as_ptr();
            }
        }

        // Allocate new chunk if needed
        let chunks = self.chunks.read().unwrap();
        if chunks.len() >= self.config.max_chunks {
            drop(chunks);
            
            // Try defragmentation before giving up
            self.force_defragmentation();
            
            if let Some(chunk) = self.find_suitable_chunk(aligned_layout) {
                if let Some(ptr) = self.allocate_from_chunk(&chunk, aligned_layout) {
                    let elapsed = start.elapsed().as_nanos() as u64;
                    self.stats.allocation_time_ns.fetch_add(elapsed, Ordering::Relaxed);
                    return ptr.as_ptr();
                }
            }
            
            self.stats.failed_allocations.fetch_add(1, Ordering::Relaxed);
            
            if self.config.fallback_to_system {
                return libc::malloc(layout.size()) as *mut u8;
            }
            
            return ptr::null_mut();
        }
        drop(chunks);

        // Determine chunk size and type
        let (chunk_size, chunk_type) = if layout.size() > HUGE_PAGE_SIZE_2MB / 2 && self.config.enable_1gb_pages {
            (HUGE_PAGE_SIZE_1GB, ChunkType::HugePage1GB)
        } else if self.config.enable_2mb_pages {
            (HUGE_PAGE_SIZE_2MB, ChunkType::HugePage2MB)
        } else {
            (16 * 1024 * 1024, ChunkType::Standard)
        };

        match self.allocate_chunk_internal(chunk_size, chunk_type) {
            Ok(new_chunk) => {
                let new_chunk = Arc::new(new_chunk);
                if let Some(ptr) = self.allocate_from_chunk(&new_chunk, aligned_layout) {
                    let mut chunks = self.chunks.write().unwrap();
                    self.total_capacity.fetch_add(chunk_size, Ordering::SeqCst);
                    chunks.push(new_chunk);
                    
                    let elapsed = start.elapsed().as_nanos() as u64;
                    self.stats.allocation_time_ns.fetch_add(elapsed, Ordering::Relaxed);
                    return ptr.as_ptr();
                }
            }
            Err(e) => {
                eprintln!("Failed to allocate new chunk: {}", e);
            }
        }

        self.stats.failed_allocations.fetch_add(1, Ordering::Relaxed);
        
        if self.config.fallback_to_system {
            libc::malloc(layout.size()) as *mut u8
        } else {
            ptr::null_mut()
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let start = Instant::now();
        
        if ptr.is_null() {
            return;
        }

        let chunks = self.chunks.read().unwrap();
        for chunk in chunks.iter() {
            if self.deallocate_from_chunk(chunk, ptr) {
                let elapsed = start.elapsed().as_nanos() as u64;
                self.stats.deallocation_time_ns.fetch_add(elapsed, Ordering::Relaxed);
                return;
            }
        }
        drop(chunks);

        // Not found in our chunks, might be system allocation
        if self.config.fallback_to_system {
            libc::free(ptr as *mut c_void);
        }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size == 0 {
            self.dealloc(ptr, layout);
            return ptr::null_mut();
        }

        if ptr.is_null() {
            return self.alloc(Layout::from_size_align_unchecked(new_size, layout.align()));
        }

        let new_layout = match Layout::from_size_align(new_size, layout.align()) {
            Ok(l) => l,
            Err(_) => return ptr::null_mut(),
        };

        // Try to extend in place
        let chunks = self.chunks.read().unwrap();
        for chunk in chunks.iter() {
            let base = chunk.base_addr.as_ptr() as usize;
            let addr = ptr as usize;
            
            if addr >= base && addr < base + chunk.size {
                let offset = addr - base;
                let allocations = chunk.allocations.read().unwrap();
                
                if let Some(metadata) = allocations.get(&offset) {
                    if metadata.size >= new_size {
                        // Already have enough space
                        return ptr;
                    }

                    // Check if we can extend
                    let free_list = chunk.free_list.read().unwrap();
                    let next_offset = offset + metadata.size;
                    
                    if let Some(&free_size) = free_list.get(&next_offset) {
                        if metadata.size + free_size >= new_size {
                            drop(free_list);
                            drop(allocations);
                            
                            // Extend the allocation
                            let mut free_list = chunk.free_list.write().unwrap();
                            let mut allocations = chunk.allocations.write().unwrap();
                            
                            if let Some(metadata) = allocations.get_mut(&offset) {
                                let extra_needed = new_size - metadata.size;
                                let free_size = free_list.remove(&next_offset).unwrap();
                                
                                metadata.size = new_size;
                                metadata.original_size = new_size;
                                
                                if free_size > extra_needed {
                                    free_list.insert(next_offset + extra_needed, free_size - extra_needed);
                                }
                                
                                chunk.used.fetch_add(extra_needed, Ordering::SeqCst);
                                self.stats.current_bytes_allocated.fetch_add(extra_needed, Ordering::Relaxed);
                                
                                return ptr;
                            }
                        }
                    }
                }
                break;
            }
        }
        drop(chunks);

        // Fallback to alloc + copy + dealloc
        let new_ptr = self.alloc(new_layout);
        if !new_ptr.is_null() {
            ptr::copy_nonoverlapping(ptr, new_ptr, layout.size().min(new_size));
            self.dealloc(ptr, layout);
        }
        
        new_ptr
    }
}

impl Drop for HugePageAllocator {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        
        if let Some(handle) = self.defrag_thread.take() {
            let _ = handle.join();
        }
        
        if let Some(handle) = self.monitor_thread.take() {
            let _ = handle.join();
        }
        
        // Chunks will be unmapped when dropped
    }
}

// Helper functions
#[inline(always)]
fn align_offset(offset: usize, align: usize) -> usize {
    (offset + align - 1) & !(align - 1)
}

#[inline(always)]
fn get_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn defragment_chunks(chunks: &Arc<RwLock<Vec<Arc<MemoryChunk>>>>, stats: &Arc<AllocationStats>) {
    let chunks = chunks.read().unwrap();
    
    for chunk in chunks.iter() {
        let fragmentation = chunk.calculate_fragmentation();
        
        if fragmentation > DEFRAG_THRESHOLD {
            // Mark chunk for defragmentation
            chunk.last_defrag.store(get_timestamp(), Ordering::Relaxed);
            
            // In production, you would implement actual memory compaction here
            // For now, we just update statistics
            let free_list = chunk.free_list.read().unwrap();
            if free_list.len() > MAX_FREE_LIST_FRAGMENTS {
                eprintln!("Warning: Chunk {} has {} free fragments", chunk.chunk_id, free_list.len());
            }
        }
    }
}

fn monitor_memory_pressure(chunks: &Arc<RwLock<Vec<Arc<MemoryChunk>>>>, stats: &Arc<AllocationStats>) {
    let chunks = chunks.read().unwrap();
    
    let mut total_used = 0;
    let mut total_capacity = 0;
    
    for chunk in chunks.iter() {
        total_used += chunk.used.load(Ordering::Relaxed);
        total_capacity += chunk.size;
    }
    
    let usage_ratio = if total_capacity > 0 {
        total_used as f64 / total_capacity as f64
    } else {
        0.0
    };
    
    if usage_ratio > 0.9 {
        eprintln!("Warning: Memory usage at {:.1}% - consider increasing chunk count", usage_ratio * 100.0);
    }
}

// Thread-local optimized allocator
thread_local! {
    static LOCAL_CACHE: std::cell::RefCell<LocalCache> = std::cell::RefCell::new(LocalCache::new());
}

struct LocalCache {
    small_blocks: Vec<(*mut u8, usize)>,
    medium_blocks: Vec<(*mut u8, usize)>,
    cache_hits: usize,
    cache_misses: usize,
}

impl LocalCache {
    fn new() -> Self {
        Self {
            small_blocks: Vec::with_capacity(32),
            medium_blocks: Vec::with_capacity(16),
            cache_hits: 0,
            cache_misses: 0,
        }
    }

    fn try_alloc(&mut self, size: usize) -> Option<*mut u8> {
        let blocks = if size <= 1024 {
            &mut self.small_blocks
        } else if size <= 65536 {
            &mut self.medium_blocks
        } else {
            return None;
        };

        for i in 0..blocks.len() {
            if blocks[i].1 >= size {
                self.cache_hits += 1;
                let (ptr, _) = blocks.swap_remove(i);
                return Some(ptr);
            }
        }

        self.cache_misses += 1;
        None
    }

    fn cache_dealloc(&mut self, ptr: *mut u8, size: usize) -> bool {
        let blocks = if size <= 1024 {
            &mut self.small_blocks
        } else if size <= 65536 {
            &mut self.medium_blocks
        } else {
            return false;
        };

        if blocks.len() < blocks.capacity() {
            blocks.push((ptr, size));
            true
        } else {
            false
        }
    }
}

// Public API
pub struct AllocationMetrics {
    pub total_allocated: usize,
    pub total_freed: usize,
    pub current_allocated: usize,
    pub peak_allocated: usize,
    pub successful_allocations: usize,
    pub failed_allocations: usize,
    pub chunk_count_2mb: usize,
    pub chunk_count_1gb: usize,
    pub total_capacity: usize,
    pub fragmentation_ratio: f64,
    pub defrag_count: usize,
    pub average_allocation_ns: u64,
    pub memory_efficiency: f64,
}

// FFI Interface
#[no_mangle]
pub extern "C" fn hugepage_allocator_new() -> *mut HugePageAllocator {
    match HugePageAllocator::new() {
        Ok(allocator) => Box::into_raw(Box::new(allocator)),
        Err(e) => {
            eprintln!("Failed to create allocator: {}", e);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub unsafe extern "C" fn hugepage_allocator_alloc(
    allocator: *mut HugePageAllocator,
    size: usize,
    align: usize,
) -> *mut u8 {
    if allocator.is_null() {
        return ptr::null_mut();
    }
    
    match Layout::from_size_align(size, align) {
        Ok(layout) => (*allocator).alloc(layout),
        Err(_) => ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn hugepage_allocator_dealloc(
    allocator: *mut HugePageAllocator,
    ptr: *mut u8,
    size: usize,
    align: usize,
) {
    if allocator.is_null() || ptr.is_null() {
        return;
    }
    
    if let Ok(layout) = Layout::from_size_align(size, align) {
        (*allocator).dealloc(ptr, layout);
    }
}

#[no_mangle]
pub unsafe extern "C" fn hugepage_allocator_free(allocator: *mut HugePageAllocator) {
    if !allocator.is_null() {
        let _ = Box::from_raw(allocator);
    }
}

#[no_mangle]
pub unsafe extern "C" fn hugepage_allocator_get_metrics(
    allocator: *mut HugePageAllocator,
    metrics: *mut AllocationMetrics,
) -> i32 {
    if allocator.is_null() || metrics.is_null() {
        return -1;
    }
    
    let allocator_metrics = (*allocator).get_metrics();
    ptr::write(metrics, allocator_metrics);
    0
}

#[no_mangle]
pub unsafe extern "C" fn hugepage_allocator_force_defrag(allocator: *mut HugePageAllocator) -> i32 {
    if allocator.is_null() {
        return -1;
    }
    
    (*allocator).force_defragmentation();
    0
}

// Builder pattern for configuration
pub struct HugePageAllocatorBuilder {
    config: AllocatorConfig,
}

impl HugePageAllocatorBuilder {
    pub fn new() -> Self {
        Self {
            config: AllocatorConfig::default(),
        }
    }

    pub fn with_2mb_pages(mut self, count: usize) -> Self {
        self.config.initial_2mb_chunks = count;
        self.config.enable_2mb_pages = count > 0;
        self
    }

    pub fn with_1gb_pages(mut self, count: usize) -> Self {
        self.config.initial_1gb_chunks = count;
        self.config.enable_1gb_pages = count > 0;
        self
    }

    pub fn max_chunks(mut self, max: usize) -> Self {
        self.config.max_chunks = max;
        self
    }

    pub fn enable_defragmentation(mut self, enable: bool) -> Self {
        self.config.enable_defrag = enable;
        self
    }

    pub fn defrag_interval(mut self, interval: Duration) -> Self {
        self.config.defrag_interval = interval;
        self
    }

    pub fn enable_monitoring(mut self, enable: bool) -> Self {
        self.config.enable_monitoring = enable;
        self
    }

    pub fn monitor_interval(mut self, interval: Duration) -> Self {
        self.config.monitor_interval = interval;
        self
    }

    pub fn fallback_to_system(mut self, enable: bool) -> Self {
        self.config.fallback_to_system = enable;
        self
    }

    pub fn lock_memory(mut self, enable: bool) -> Self {
        self.config.lock_memory = enable;
        self
    }

    pub fn numa_aware(mut self, enable: bool) -> Self {
        self.config.numa_aware = enable;
        self
    }

    pub fn build(self) -> io::Result<HugePageAllocator> {
        HugePageAllocator::with_config(self.config)
    }
}

// Global allocator instance
static GLOBAL_ALLOCATOR: Lazy<Arc<RwLock<Option<Arc<HugePageAllocator>>>>> = 
    Lazy::new(|| Arc::new(RwLock::new(None)));

pub fn initialize_global_allocator(config: AllocatorConfig) -> io::Result<()> {
    let allocator = Arc::new(HugePageAllocator::with_config(config)?);
    let mut global = GLOBAL_ALLOCATOR.write().unwrap();
    *global = Some(allocator);
    Ok(())
}

pub fn get_global_allocator() -> Option<Arc<HugePageAllocator>> {
    GLOBAL_ALLOCATOR.read().unwrap().clone()
}

// Optimized memory operations
#[inline(always)]
pub fn prefetch_read(ptr: *const u8, size: usize) {
    unsafe {
        for offset in (0..size).step_by(CACHE_LINE_SIZE) {
            std::arch::x86_64::_mm_prefetch::<0>(ptr.add(offset) as *const i8);
        }
    }
}

#[inline(always)]
pub fn prefetch_write(ptr: *mut u8, size: usize) {
    unsafe {
        for offset in (0..size).step_by(CACHE_LINE_SIZE) {
            std::arch::x86_64::_mm_prefetch::<1>(ptr.add(offset) as *const i8);
        }
    }
}

#[inline(always)]
pub unsafe fn zero_memory_fast(ptr: *mut u8, size: usize) {
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::*;
        
        if is_x86_feature_detected!("avx2") && size >= 32 {
            let zero = _mm256_setzero_si256();
            let mut offset = 0;
            
            // Align to 32 bytes
            let align_offset = ptr.align_offset(32);
            if align_offset > 0 && align_offset < size {
                ptr::write_bytes(ptr, 0, align_offset);
                offset = align_offset;
            }
            
            // Zero 32 bytes at a time
            while offset + 32 <= size {
                _mm256_store_si256(ptr.add(offset) as *mut __m256i, zero);
                offset += 32;
            }
            
            // Handle remainder
            if offset < size {
                ptr::write_bytes(ptr.add(offset), 0, size - offset);
            }
        } else {
            ptr::write_bytes(ptr, 0, size);
        }
    }
    
    #[cfg(not(target_arch = "x86_64"))]
    {
        ptr::write_bytes(ptr, 0, size);
    }
}

#[inline(always)]
pub unsafe fn copy_memory_fast(dst: *mut u8, src: *const u8, size: usize) {
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::*;
        
        if is_x86_feature_detected!("avx2") && size >= 32 {
            let mut offset = 0;
            
            // Copy 32 bytes at a time
            while offset + 32 <= size {
                let data = _mm256_loadu_si256(src.add(offset) as *const __m256i);
                _mm256_storeu_si256(dst.add(offset) as *mut __m256i, data);
                offset += 32;
            }
            
            // Handle remainder
            if offset < size {
                ptr::copy_nonoverlapping(src.add(offset), dst.add(offset), size - offset);
            }
        } else {
            ptr::copy_nonoverlapping(src, dst, size);
        }
    }
    
    #[cfg(not(target_arch = "x86_64"))]
    {
        ptr::copy_nonoverlapping(src, dst, size);
    }
}

// Memory pool for specific allocation sizes
pub struct MemoryPool {
    allocator: Arc<HugePageAllocator>,
    block_size: usize,
    free_blocks: Mutex<Vec<NonNull<u8>>>,
    total_blocks: AtomicUsize,
    used_blocks: AtomicUsize,
}

impl MemoryPool {
    pub fn new(allocator: Arc<HugePageAllocator>, block_size: usize, initial_blocks: usize) -> io::Result<Self> {
        let pool = Self {
            allocator,
            block_size: block_size.max(MIN_ALLOCATION_SIZE),
            free_blocks: Mutex::new(Vec::with_capacity(initial_blocks)),
            total_blocks: AtomicUsize::new(0),
            used_blocks: AtomicUsize::new(0),
        };
        
        pool.expand(initial_blocks)?;
        Ok(pool)
    }

    fn expand(&self, count: usize) -> io::Result<()> {
        let layout = Layout::from_size_align(self.block_size * count, CACHE_LINE_SIZE)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid layout"))?;
        
        unsafe {
            let ptr = self.allocator.alloc(layout);
            if ptr.is_null() {
                return Err(io::Error::new(io::ErrorKind::OutOfMemory, "Failed to allocate pool memory"));
            }
            
            let mut free_blocks = self.free_blocks.lock().unwrap();
            for i in 0..count {
                let block_ptr = ptr.add(i * self.block_size);
                zero_memory_fast(block_ptr, self.block_size);
                free_blocks.push(NonNull::new_unchecked(block_ptr));
            }
            
            self.total_blocks.fetch_add(count, Ordering::SeqCst);
        }
        
        Ok(())
    }

    pub fn alloc(&self) -> Option<NonNull<u8>> {
        let mut free_blocks = self.free_blocks.lock().unwrap();
        
        if let Some(ptr) = free_blocks.pop() {
            self.used_blocks.fetch_add(1, Ordering::Relaxed);
            Some(ptr)
        } else {
            drop(free_blocks);
            
            // Try to expand pool
            if self.expand(16).is_ok() {
                let mut free_blocks = self.free_blocks.lock().unwrap();
                if let Some(ptr) = free_blocks.pop() {
                    self.used_blocks.fetch_add(1, Ordering::Relaxed);
                    return Some(ptr);
                }
            }
            
            None
        }
    }

    pub fn dealloc(&self, ptr: NonNull<u8>) {
        unsafe {
            zero_memory_fast(ptr.as_ptr(), self.block_size);
        }
        
        let mut free_blocks = self.free_blocks.lock().unwrap();
        free_blocks.push(ptr);
        self.used_blocks.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn stats(&self) -> (usize, usize) {
        (
            self.used_blocks.load(Ordering::Relaxed),
            self.total_blocks.load(Ordering::Relaxed),
        )
    }
}

// Aligned buffer for SIMD operations
#[repr(align(64))]
pub struct AlignedBuffer {
    ptr: NonNull<u8>,
    size: usize,
    layout: Layout,
    allocator: Arc<HugePageAllocator>,
}

impl AlignedBuffer {
    pub fn new(size: usize, allocator: Arc<HugePageAllocator>) -> io::Result<Self> {
        Self::new_with_align(size, 64, allocator)
    }

    pub fn new_with_align(size: usize, align: usize, allocator: Arc<HugePageAllocator>) -> io::Result<Self> {
        let layout = Layout::from_size_align(size, align)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid layout"))?;
        
        unsafe {
            let ptr = allocator.alloc(layout);
            if ptr.is_null() {
                return Err(io::Error::new(io::ErrorKind::OutOfMemory, "Allocation failed"));
            }
            
            zero_memory_fast(ptr, size);
            
            Ok(Self {
                ptr: NonNull::new_unchecked(ptr),
                size,
                layout,
                allocator,
            })
        }
    }

    #[inline(always)]
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr.as_ptr()
    }

    #[inline(always)]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.size
    }

    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.size) }
    }

    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size) }
    }

    pub fn resize(&mut self, new_size: usize) -> io::Result<()> {
        if new_size == self.size {
            return Ok(());
        }

        unsafe {
            let new_layout = Layout::from_size_align(new_size, self.layout.align())
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "Invalid layout"))?;
            
            let new_ptr = self.allocator.realloc(self.ptr.as_ptr(), self.layout, new_size);
            if new_ptr.is_null() {
                return Err(io::Error::new(io::ErrorKind::OutOfMemory, "Reallocation failed"));
            }
            
            self.ptr = NonNull::new_unchecked(new_ptr);
            self.size = new_size;
            self.layout = new_layout;
        }
        
        Ok(())
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe {
            self.allocator.dealloc(self.ptr.as_ptr(), self.layout);
        }
    }
}

unsafe impl Send for AlignedBuffer {}
unsafe impl Sync for AlignedBuffer {}

// Initialize everything on module load
pub fn init() -> io::Result<()> {
    let config = AllocatorConfig {
        enable_2mb_pages: true,
        enable_1gb_pages: true,
        initial_2mb_chunks: 8,
        initial_1gb_chunks: 2,
        max_chunks: 64,
        enable_defrag: true,
        defrag_interval: Duration::from_secs(60),
        enable_monitoring: true,
        monitor_interval: Duration::from_secs(10),
        fallback_to_system: true,
        lock_memory: true,
        numa_aware: false,
    };
    
    initialize_global_allocator(config)
}

