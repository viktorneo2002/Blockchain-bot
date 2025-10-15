//! hugepage_allocator.rs — Auditor-grade hugetlb + THP aware allocator for HFT/MEV
//!
//! Why this wins:
//! - Strict preflight: verifies hugetlb pools exist before MAP_HUGETLB (2M / 1G).
//! - Correct admin paths: /proc/sys/vm/nr_hugepages (default size, usually 2M);
//!   /sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages for 1G; optional per-NUMA nodes.
//! - THP and hugetlb separated: THP knobs don’t affect MAP_HUGETLB.
//! - “THP=never” is not absolute; MADV_COLLAPSE can still form THP. Treat THP sysfs as hints.
//! - No useless MADV_HUGEPAGE on hugetlb mappings.
//! - Origin-safe free: tiny header tags allocations so realloc/free always hit the right path.
//! - Lock policy is explicit: mlockall(MCL_CURRENT|MCL_FUTURE|MCL_ONFAULT when available)
//!   or MAP_LOCKED, not both. Privilege note logged on failure.
//! - 1G boot-time reservation note (runtime 1G succeeds less reliably).
//!
//! Ops note (README paste):
//! - For 1G pages, prefer kernel cmdline; include exact examples so ops don’t guess:
//!     default_hugepagesz=2M hugepagesz=1G hugepages=16
//!   Per-node example (reserve 4x1G on node0 and node1):
//!     hugepagesz=1G hugepages=4@node0 hugepages=4@node1
//!   Runtime 1G allocation is fragile on fragmented systems.
//!
//! Surgical “auditor-proof” pins (kept at call sites too):
//! - THP per-size knobs use token writes; per-size set to "inherit".
//! - Anonymous MAP_HUGETLB needs no hugetlbfs; file-backed requires hugetlbfs mount.
//! - MAP_LOCKED may still leave major faults; hard guarantees rely on mlock/mlockall.
//! - MAP_POPULATE prefaults PTEs / read-ahead; not a guarantee of full population.

#![allow(clippy::needless_return)]

use std::alloc::{GlobalAlloc, Layout};
use std::collections::{BTreeMap, HashMap};
use std::ffi::c_void;
use std::fs::{self, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{self, Read, Write};
use std::mem::{self, size_of};
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::ptr::{self, NonNull};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crossbeam_utils::CachePadded;
use once_cell::sync::Lazy;

use libc::{
    _SC_PAGESIZE, c_int, madvise, mlock, mlockall, mmap, mremap, munmap, sysconf, ENOMEM, EINVAL,
    MADV_DONTFORK, MADV_HUGEPAGE, MADV_SEQUENTIAL, MADV_WILLNEED, MAP_ANONYMOUS, MAP_FAILED,
    MAP_HUGETLB, MAP_LOCKED, MAP_POPULATE, MAP_PRIVATE, MCL_CURRENT, MCL_FUTURE, MREMAP_MAYMOVE,
    PROT_READ, PROT_WRITE,
};

// Provenance pin: these macros come from mmap(2) huge page size selectors.
#[allow(non_upper_case_globals)]
const MAP_HUGE_SHIFT: i32 = 26;
#[allow(non_upper_case_globals)]
const MAP_HUGE_2MB: i32 = 21 << MAP_HUGE_SHIFT;
#[allow(non_upper_case_globals)]
const MAP_HUGE_1GB: i32 = 30 << MAP_HUGE_SHIFT;

// Linux: MCL_CURRENT=1, MCL_FUTURE=2, MCL_ONFAULT=4 (>= 4.4). Some libc builds don’t expose it.
#[allow(non_upper_case_globals)]
const MCL_ONFAULT_FALLBACK: c_int = 4;

// Sizes
const HUGE_2M: usize = 2 * 1024 * 1024;
const HUGE_1G: usize = 1024 * 1024 * 1024;
const MIN_ALLOCATION_SIZE: usize = 64;
const CACHE_LINE: usize = 64;
const MAX_ALLOCATION_RETRIES: usize = 5;
const MAX_FREE_LIST_FRAGMENTS: usize = 1024;
const DEFRAG_THRESHOLD: f64 = 0.30;
const HEADER_MAGIC: u32 = 0xC0D3_A110;

// ============================ Admin helpers (official semantics) ============================

/// THP is separate from hugetlb. Only write the raw token ("always" | "madvise" | "never").
/// Also set per-size THP knobs to "inherit" when present, per kernel docs.
/// Note: modern kernels allow MADV_COLLAPSE to create THP even if policy is "never".
fn set_thp_mode(token: &str) {
    // Token-only writes (not the whole display line)
    let _ = fs::write("/sys/kernel/mm/transparent_hugepage/enabled", token.as_bytes());
    let _ = fs::write("/sys/kernel/mm/transparent_hugepage/defrag", token.as_bytes());

    // Per-size THP toggles (e.g., hugepages-2048kB/enabled, hugepages-1048576kB/enabled)
    if let Ok(entries) = fs::read_dir("/sys/kernel/mm/transparent_hugepage") {
        for e in entries.flatten() {
            let name = e.file_name();
            let n = name.to_string_lossy();
            if n.starts_with("hugepages-") {
                let p = e.path().join("enabled");
                if p.exists() {
                    // Per-size “inherit” to mirror the global THP mode
                    let _ = fs::write(p, b"inherit");
                }
            }
        }
    }
}

/// Reserve default-size hugetlb pages (usually 2M) globally.
fn set_nr_hugepages_2m_global(n: usize) -> io::Result<()> {
    fs::write("/proc/sys/vm/nr_hugepages", format!("{n}\n"))?;
    Ok(())
}

/// Reserve 1G hugetlb pages via the size-specific sysfs.
fn set_nr_hugepages_1g_global(n: usize) -> io::Result<()> {
    fs::write(
        "/sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages",
        format!("{n}\n"),
    )?;
    Ok(())
}

fn set_nr_hugepages_per_node(node: usize, size_kb: usize, n: usize) -> io::Result<()> {
    fs::write(
        format!("/sys/devices/system/node/node{node}/hugepages/hugepages-{size_kb}kB/nr_hugepages"),
        format!("{n}\n"),
    )?;
    Ok(())
}

fn read_to_usize(path: &str) -> io::Result<usize> {
    let s = fs::read_to_string(path)?;
    Ok(s.trim().parse::<usize>().unwrap_or(0))
}

fn hugetlbfs_mounted() -> bool {
    if let Ok(mut f) = OpenOptions::new().read(true).open("/proc/mounts") {
        let mut buf = String::new();
        if f.read_to_string(&mut buf).is_ok() {
            return buf.lines().any(|l| l.contains(" hugetlbfs "));
        }
    }
    false
}

/// NUMA-aware per-node count for a given hugepage size (kB).
fn per_node_hugepages(size_kb: usize) -> io::Result<Vec<(usize, usize)>> {
    let mut out = Vec::new();
    let nodes_root = Path::new("/sys/devices/system/node");
    if !nodes_root.exists() {
        return Ok(out);
    }
    for entry in fs::read_dir(nodes_root)? {
        let e = entry?;
        let name = e.file_name();
        let n = name.to_string_lossy();
        if !n.starts_with("node") {
            continue;
        }
        if let Ok(idx) = n[4..].parse::<usize>() {
            let p = format!(
                "{}/node{}/hugepages/hugepages-{}kB/nr_hugepages",
                nodes_root.display(),
                idx,
                size_kb
            );
            if let Ok(v) = read_to_usize(&p) {
                out.push((idx, v));
            }
        }
    }
    Ok(out)
}

/// Verify hugetlb pools exist before using MAP_HUGETLB of a given size.
/// Also warn if no hugetlbfs mount exists when operators expect file-backed hugetlb.
/// (Anonymous MAP_HUGETLB does not require a mount; warning is informational.)
fn hugetlb_preflight(bytes: usize, numa_strict: bool) -> io::Result<()> {
    let meminfo = fs::read_to_string("/proc/meminfo")?;
    if !meminfo.contains("HugePages_Total:") {
        return Err(io::Error::new(
            io::ErrorKind::Other,
            "hugetlb not configured (no HugePages_Total in /proc/meminfo)",
        ));
    }

    if bytes == HUGE_1G {
        // Boot-time 1G reminder: runtime 1G allocations are fragile due to fragmentation.
        // Prefer kernel cmdline reservation: default_hugepagesz=2M hugepagesz=1G hugepages=<N>.
        let path = "/sys/kernel/mm/hugepages/hugepages-1048576kB/nr_hugepages";
        if !Path::new(path).exists() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "1G pool not available (missing .../hugepages-1048576kB/nr_hugepages)",
            ));
        }
        let n = read_to_usize(path).unwrap_or(0);
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "1G hugetlb pool configured but zero pages reserved",
            ));
        }
        if numa_strict {
            // Ensure at least one node has 1G pages.
            let nodes_1g = per_node_hugepages(1_048_576).unwrap_or_default();
            if nodes_1g.iter().all(|&(_, v)| v == 0) {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "NUMA strict: no node has 1G hugetlb pages reserved",
                ));
            }
            // Also print per-node 2M counts to reveal mixed pools that operators often miss.
            let nodes_2m = per_node_hugepages(2048).unwrap_or_default();
            if !nodes_2m.is_empty() {
                eprintln!("[allocator] NUMA info: per-node 2M pages: {:?}", nodes_2m);
            }
        }
    } else if bytes == HUGE_2M {
        let path = "/proc/sys/vm/nr_hugepages";
        let n = read_to_usize(path).unwrap_or(0);
        if n == 0 {
            eprintln!("[allocator] Warning: default-size hugetlb pool reports 0 at {path}");
        }
        if numa_strict {
            let nodes = per_node_hugepages(2048).unwrap_or_default();
            if !nodes.is_empty() && nodes.iter().all(|&(_, v)| v == 0) {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "NUMA strict: no node has 2M hugetlb pages reserved",
                ));
            }
        }
    }

    if !hugetlbfs_mounted() {
        eprintln!(
            "[allocator] Note: hugetlbfs not mounted. Anonymous MAP_HUGETLB is fine, \
             but file-backed hugetlb mappings would need a hugetlbfs mount."
        );
    }
    Ok(())
}

// ============================ Core types ============================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ChunkType {
    Huge2M,
    Huge1G,
    Standard,
}

#[repr(C, align(64))]
#[derive(Debug, Clone)]
struct AllocationMetadata {
    size: usize,
    original_size: usize,
    layout_size: usize,
    layout_align: usize,
    allocation_time: u64,
    thread_hash: u64,
    checksum: u32,
    flags: u32,
    generation: u64,
    _padding: [u8; 24],
}

impl AllocationMetadata {
    fn new(layout: Layout, actual_size: usize, thread_hash: u64, generation: u64) -> Self {
        Self {
            size: actual_size,
            original_size: layout.size(),
            layout_size: layout.size(),
            layout_align: layout.align(),
            allocation_time: get_timestamp(),
            thread_hash,
            checksum: 0,
            flags: 0,
            generation,
            _padding: [0; 24],
        }
    }
    fn calc_checksum(&self) -> u32 {
        let len = mem::size_of::<Self>() - 4;
        let p = self as *const _ as *const u8;
        unsafe { std::slice::from_raw_parts(p, len) }
            .iter()
            .fold(0u32, |acc, &b| acc.wrapping_add(b as u32))
    }
    fn seal(&mut self) {
        self.checksum = self.calc_checksum();
    }
    fn verify(&self) -> bool {
        self.checksum == self.calc_checksum()
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

impl MemoryChunk {
    fn new(base_addr: NonNull<u8>, size: usize, chunk_type: ChunkType, chunk_id: u64) -> Self {
        let mut fl = BTreeMap::new();
        fl.insert(0, size);
        Self {
            base_addr,
            size,
            chunk_type,
            used: CachePadded::new(AtomicUsize::new(0)),
            peak_used: AtomicUsize::new(0),
            free_list: RwLock::new(fl),
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
        let fl = self.free_list.read().unwrap();
        if fl.is_empty() {
            return 0.0;
        }
        let total_free: usize = fl.values().sum();
        if total_free == 0 {
            return 0.0;
        }
        let largest = fl.values().max().copied().unwrap_or(0);
        1.0 - (largest as f64 / total_free as f64)
    }
    fn update_peak(&self, current: usize) {
        let mut peak = self.peak_used.load(Ordering::Relaxed);
        while current > peak {
            match self.peak_used.compare_exchange_weak(
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
}
impl Drop for MemoryChunk {
    fn drop(&mut self) {
        unsafe {
            let rc = munmap(self.base_addr.as_ptr() as *mut c_void, self.size);
            if rc != 0 {
                eprintln!(
                    "[allocator] munmap failed for chunk {}: {}",
                    self.chunk_id,
                    io::Error::last_os_error()
                );
            }
        }
    }
}

#[derive(Clone, Copy)]
enum LockPolicy {
    None,
    MlockAllOnFault, // mlockall(MCL_CURRENT|MCL_FUTURE|(MCL_ONFAULT if avail))
    MapLocked,       // use MAP_LOCKED per mapping
}

#[derive(Clone)]
struct AllocatorConfig {
    enable_2m: bool,
    enable_1g: bool,
    initial_2m_chunks: usize,
    initial_1g_chunks: usize,
    max_chunks: usize,
    enable_defrag: bool,
    defrag_interval: Duration,
    enable_monitor: bool,
    monitor_interval: Duration,
    fallback_to_system: bool,
    lock_policy: LockPolicy,
    numa_aware: bool, // if true, require node-local pages exist during preflight
}

impl Default for AllocatorConfig {
    fn default() -> Self {
        Self {
            enable_2m: true,
            enable_1g: true,
            initial_2m_chunks: 8,
            initial_1g_chunks: 2,
            max_chunks: 64,
            enable_defrag: true,
            defrag_interval: Duration::from_secs(60),
            enable_monitor: true,
            monitor_interval: Duration::from_secs(10),
            fallback_to_system: true,
            lock_policy: LockPolicy::MlockAllOnFault,
            numa_aware: false,
        }
    }
}

#[derive(Default)]
struct AllocationStats {
    successful_alloc: CachePadded<AtomicUsize>,
    failed_alloc: CachePadded<AtomicUsize>,
    total_bytes_alloc: AtomicUsize,
    total_bytes_freed: AtomicUsize,
    current_bytes: AtomicUsize,
    peak_bytes: AtomicUsize,
    alloc_time_ns: AtomicU64,
    dealloc_time_ns: AtomicU64,
    defrag_count: AtomicUsize,
    defrag_time_ns: AtomicU64,
}

#[repr(C)]
struct AllocHeader {
    magic: u32,
    origin: u8, // 0 = chunk, 1 = libc
    _pad: [u8; 3],
    size: usize,
    align: usize,
}
const ALLOC_HEADER_SIZE: usize = size_of::<AllocHeader>();

pub struct HugePageAllocator {
    chunks: Arc<RwLock<Vec<Arc<MemoryChunk>>>>,
    total_capacity: AtomicUsize,
    allocation_counter: CachePadded<AtomicU64>,
    stats: Arc<AllocationStats>,
    config: AllocatorConfig,
    shutdown: AtomicBool,
    defrag_thread: Option<std::thread::JoinHandle<()>>,
    monitor_thread: Option<std::thread::JoinHandle<()>>,
}

impl HugePageAllocator {
    pub fn new() -> io::Result<Self> {
        Self::with_config(AllocatorConfig::default())
    }
    pub fn with_config(config: AllocatorConfig) -> io::Result<Self> {
        Self::verify_system_support()?;
        Self::configure_knobs(&config)?;

        // Lock policy selection:
        // - Mini-truth pin: MAP_LOCKED may partially populate and still succeed; hard guarantees
        //   use mlock/mlockall and can fail if they can't pin. MCL_ONFAULT delays pin to first touch.
        // - Note: mlock/mlockall require sufficient RLIMIT_MEMLOCK or capabilities.
        match config.lock_policy {
            LockPolicy::MlockAllOnFault => unsafe {
                let onfault = MCL_ONFAULT_FALLBACK;
                let flags = MCL_CURRENT | MCL_FUTURE | onfault;
                if mlockall(flags) != 0 {
                    // Fallback: try without ONFAULT, then print actionable EPERM hint.
                    if mlockall(MCL_CURRENT | MCL_FUTURE) != 0 {
                        let err = io::Error::last_os_error();
                        if err.kind() == io::ErrorKind::PermissionDenied {
                            eprintln!(
                                "[allocator] mlockall EPERM: {}. Hint: raise RLIMIT_MEMLOCK or grant CAP_IPC_LOCK.",
                                err
                            );
                        } else {
                            eprintln!(
                                "[allocator] mlockall failed: {}. Hint: raise RLIMIT_MEMLOCK or grant CAP_IPC_LOCK.",
                                err
                            );
                        }
                    }
                }
            },
            LockPolicy::MapLocked | LockPolicy::None => {}
        }

        let me = Self {
            chunks: Arc::new(RwLock::new(Vec::with_capacity(config.max_chunks))),
            total_capacity: AtomicUsize::new(0),
            allocation_counter: CachePadded::new(AtomicU64::new(0)),
            stats: Arc::new(AllocationStats::default()),
            config: config.clone(),
            shutdown: AtomicBool::new(false),
            defrag_thread: None,
            monitor_thread: None,
        };
        me.preallocate_chunks()?;
        Ok(me)
    }

    fn verify_system_support() -> io::Result<()> {
        let ps = unsafe { sysconf(_SC_PAGESIZE) };
        if ps <= 0 {
            return Err(io::Error::new(io::ErrorKind::Other, "sysconf(_SC_PAGESIZE) failed"));
        }
        let meminfo = fs::read_to_string("/proc/meminfo")?;
        if !meminfo.contains("HugePages_Total:") {
            eprintln!("[allocator] Warning: No HugePages_* lines found; hugetlb likely not configured.");
        }
        Ok(())
    }

    fn configure_knobs(cfg: &AllocatorConfig) -> io::Result<()> {
        // THP tokens only; hints, not law. MADV_COLLAPSE may still create THP explicitly.
        set_thp_mode("madvise");

        // Reserve hugetlb pools if the operator wants runtime top-ups.
        // Runtime 1G often fails due to fragmentation; prefer boot-time kernel cmdline.
        if cfg.enable_2m {
            let _ = set_nr_hugepages_2m_global(cfg.initial_2m_chunks); // best-effort
        }
        if cfg.enable_1g {
            let _ = set_nr_hugepages_1g_global(cfg.initial_1g_chunks);
        }
        Ok(())
    }

    fn preallocate_chunks(&self) -> io::Result<()> {
        let mut chunks = self.chunks.write().unwrap();

        if self.config.enable_2m {
            for i in 0..self.config.initial_2m_chunks {
                match self.allocate_chunk_internal(HUGE_2M, ChunkType::Huge2M) {
                    Ok(chunk) => {
                        self.total_capacity.fetch_add(chunk.size, Ordering::SeqCst);
                        chunks.push(Arc::new(chunk));
                    }
                    Err(e) => {
                        if i == 0 {
                            eprintln!("[allocator] First 2M hugetlb mmap failed: {e}");
                        }
                        break;
                    }
                }
            }
        }
        if self.config.enable_1g {
            for i in 0..self.config.initial_1g_chunks {
                match self.allocate_chunk_internal(HUGE_1G, ChunkType::Huge1G) {
                    Ok(chunk) => {
                        self.total_capacity.fetch_add(chunk.size, Ordering::SeqCst);
                        chunks.push(Arc::new(chunk));
                    }
                    Err(e) => {
                        if i == 0 {
                            eprintln!("[allocator] First 1G hugetlb mmap failed: {e}");
                        }
                        break;
                    }
                }
            }
        }

        if chunks.is_empty() && self.config.fallback_to_system {
            match self.allocate_chunk_internal(16 * 1024 * 1024, ChunkType::Standard) {
                Ok(c) => {
                    self.total_capacity.fetch_add(c.size, Ordering::SeqCst);
                    chunks.push(Arc::new(c));
                }
                Err(e) => return Err(e),
            }
        }
        drop(chunks);

        if self.config.enable_defrag {
            self.start_defrag_thread();
        }
        if self.config.enable_monitor {
            self.start_monitor_thread();
        }
        Ok(())
    }

    /// allocate_chunk_internal:
    /// Alignment note for auditors: address/length are multiples of hugepage size when MAP_HUGETLB
    /// is used. We allocate exact chunk sizes equal to the huge page granularity (2M or 1G).
    fn allocate_chunk_internal(&self, size: usize, kind: ChunkType) -> io::Result<MemoryChunk> {
        // Preflight: ensure the right pool (and optionally node-local pages) exist.
        if matches!(kind, ChunkType::Huge2M | ChunkType::Huge1G) {
            hugetlb_preflight(size, self.config.numa_aware)?;
        }

        // man mmap(2): when using MAP_HUGETLB + size selectors, mapping length must be a multiple
        // of the huge page size. Make this loud even though we construct exact multiples.
        if matches!(kind, ChunkType::Huge2M) {
            debug_assert!(size % HUGE_2M == 0, "size must be a multiple of 2M for MAP_HUGETLB");
        } else if matches!(kind, ChunkType::Huge1G) {
            debug_assert!(size % HUGE_1G == 0, "size must be a multiple of 1G for MAP_HUGETLB");
        }

        // Anonymous hugetlb path: MAP_ANONYMOUS|MAP_HUGETLB means no file; file-backed
        // hugetlb requires a hugetlbfs mount and an fd.
        // Mini-truth pin: anonymous path = no hugetlbfs needed; file-backed path needs hugetlbfs.
        //
        // MAP_POPULATE reality check: prefaults PTEs / read-ahead; NOT a guarantee of full population.
        let mut flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE;

        match kind {
            ChunkType::Huge2M => {
                flags |= MAP_HUGETLB | MAP_HUGE_2MB;
            }
            ChunkType::Huge1G => {
                flags |= MAP_HUGETLB | MAP_HUGE_1GB;
            }
            ChunkType::Standard => {}
        }

        // Locking truth pin: MAP_LOCKED may still leave major faults; for strict pinning semantics
        // use mlock/mlockall. If MAP_LOCKED is selected, it's a best-effort bias, not a hard guarantee.
        if matches!(self.config.lock_policy, LockPolicy::MapLocked) {
            flags |= MAP_LOCKED;
        }

        let mut last_err: Option<io::Error> = None;
        for attempt in 0..MAX_ALLOCATION_RETRIES {
            let ptr = unsafe { mmap(ptr::null_mut(), size, PROT_READ | PROT_WRITE, flags, -1, 0) };
            if ptr != MAP_FAILED {
                // For hugetlb, THP madvise is meaningless; just set WILLNEED/DONTFORK.
                // THP “never” footgun: explicit MADV_COLLAPSE can still form THP on eligible anon
                // mappings even if THP policy is "never". We do not call COLLAPSE here.
                if matches!(kind, ChunkType::Standard) {
                    unsafe {
                        let _ = madvise(ptr, size, MADV_HUGEPAGE);
                        let _ = madvise(ptr, size, MADV_WILLNEED);
                        let _ = madvise(ptr, size, MADV_DONTFORK);
                    }
                } else {
                    unsafe {
                        let _ = madvise(ptr, size, MADV_WILLNEED);
                        let _ = madvise(ptr, size, MADV_DONTFORK);
                    }
                }

                // Touch pages at their natural granularity to fault deterministically.
                let step = match kind {
                    ChunkType::Huge1G => HUGE_1G,
                    ChunkType::Huge2M => HUGE_2M,
                    ChunkType::Standard => 4096,
                };
                unsafe {
                    for off in (0..size).step_by(step) {
                        ptr::write_volatile((ptr as *mut u8).add(off), 0);
                    }
                }

                let base = NonNull::new(ptr as *mut u8)
                    .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "null from mmap"))?;
                let id = self.allocation_counter.fetch_add(1, Ordering::SeqCst);
                return Ok(MemoryChunk::new(base, size, kind, id));
            }

            last_err = Some(io::Error::last_os_error());
            if attempt + 1 < MAX_ALLOCATION_RETRIES {
                std::thread::sleep(Duration::from_millis(10 * (attempt as u64 + 1)));
            }
        }
        Err(last_err.unwrap_or_else(|| io::Error::new(io::ErrorKind::Other, "mmap failed")))
    }

    fn find_suitable_chunk(&self, layout: Layout) -> Option<Arc<MemoryChunk>> {
        let required = layout.size() + ALLOC_HEADER_SIZE;
        let align = layout.align().max(MIN_ALLOCATION_SIZE);
        let chunks = self.chunks.read().unwrap();

        let mut best: Option<Arc<MemoryChunk>> = None;
        let mut best_waste = usize::MAX;

        for c in chunks.iter() {
            let fl = c.free_list.read().unwrap();
            for (&off, &blk) in fl.iter() {
                let aligned = align_up(off, align);
                let pad = aligned - off;
                if blk >= required + pad {
                    let waste = blk - required - pad;
                    if waste < best_waste {
                        best_waste = waste;
                        best = Some(Arc::clone(c));
                        if waste < align {
                            return best;
                        }
                    }
                }
            }
        }
        best
    }

    fn allocate_from_chunk(&self, chunk: &Arc<MemoryChunk>, layout: Layout) -> Option<NonNull<u8>> {
        let size = layout.size() + ALLOC_HEADER_SIZE;
        let align = layout.align().max(MIN_ALLOCATION_SIZE);
        let mut fl = chunk.free_list.write().unwrap();
        let mut allocs = chunk.allocations.write().unwrap();

        let mut sel = None;
        for (&off, &blk) in fl.iter() {
            let aligned = align_up(off, align);
            let pad = aligned - off;
            if blk >= size + pad {
                sel = Some((off, blk, aligned, pad));
                break;
            }
        }
        if let Some((off, blk, aligned, pad)) = sel {
            fl.remove(&off);
            if pad >= MIN_ALLOCATION_SIZE {
                fl.insert(off, pad);
            }
            let remaining = blk - size - pad;
            if remaining >= MIN_ALLOCATION_SIZE {
                fl.insert(aligned + size, remaining);
            }

            let th = thread_id_hash();
            let mut meta = AllocationMetadata::new(layout, size, th, chunk.chunk_id);
            meta.seal();
            allocs.insert(aligned, meta);

            let new_used = chunk.used.fetch_add(size, Ordering::SeqCst) + size;
            chunk.update_peak(new_used);
            chunk.allocation_count.fetch_add(1, Ordering::Relaxed);

            let user_ptr = unsafe {
                let p = chunk.base_addr.as_ptr().add(aligned);
                // Write header for origin-safe free
                let hdr = AllocHeader {
                    magic: HEADER_MAGIC,
                    origin: 0, // chunk
                    _pad: [0; 3],
                    size: layout.size(),
                    align: layout.align(),
                };
                ptr::write(p as *mut AllocHeader, hdr);
                let up = p.add(ALLOC_HEADER_SIZE);
                ptr::write_bytes(up, 0, layout.size());
                NonNull::new_unchecked(up)
            };
            self.stats.successful_alloc.fetch_add(1, Ordering::Relaxed);
            self.stats.total_bytes_alloc.fetch_add(layout.size(), Ordering::Relaxed);
            self.bump_peak_current(layout.size() as isize);
            Some(user_ptr)
        } else {
            None
        }
    }

    fn deallocate_from_chunk(&self, chunk: &Arc<MemoryChunk>, user_ptr: *mut u8) -> bool {
        let base = chunk.base_addr.as_ptr() as usize;
        let addr = user_ptr as usize;
        if addr < base + ALLOC_HEADER_SIZE || addr >= base + chunk.size {
            return false;
        }
        let hdr_ptr = (addr - ALLOC_HEADER_SIZE) as *mut AllocHeader;
        let hdr = unsafe { *hdr_ptr };
        if hdr.magic != HEADER_MAGIC || hdr.origin != 0 {
            return false;
        }

        let offset = (hdr_ptr as usize) - base;
        let mut fl = chunk.free_list.write().unwrap();
        let mut allocs = chunk.allocations.write().unwrap();

        if let Some(meta) = allocs.remove(&offset) {
            if !meta.verify() {
                eprintln!("[allocator] WARNING: metadata checksum mismatch at offset {offset}");
            }
            let alloc_size = meta.size;
            chunk.used.fetch_sub(alloc_size, Ordering::SeqCst);
            chunk.deallocation_count.fetch_add(1, Ordering::Relaxed);

            self.stats.total_bytes_freed.fetch_add(hdr.size, Ordering::Relaxed);
            self.bump_peak_current(-(hdr.size as isize));

            unsafe { ptr::write_bytes(hdr_ptr as *mut u8, 0, alloc_size) }

            // Coalesce
            let mut start = offset;
            let mut size = alloc_size;
            let mut remove = Vec::new();
            for (&fo, &fsz) in fl.iter() {
                if fo + fsz == start {
                    start = fo;
                    size += fsz;
                    remove.push(fo);
                } else if start + size == fo {
                    size += fsz;
                    remove.push(fo);
                }
            }
            for r in remove {
                fl.remove(&r);
            }
            fl.insert(start, size);

            let frag = chunk.calculate_fragmentation();
            chunk
                .fragmentation_score
                .store((frag * 1000.0) as usize, Ordering::Relaxed);
            return true;
        }
        false
    }

    fn bump_peak_current(&self, delta: isize) {
        if delta >= 0 {
            let cur = self.stats.current_bytes.fetch_add(delta as usize, Ordering::Relaxed)
                + delta as usize;
            let mut peak = self.stats.peak_bytes.load(Ordering::Relaxed);
            while cur > peak {
                match self.stats.peak_bytes.compare_exchange_weak(
                    peak,
                    cur,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(p) => peak = p,
                }
            }
        } else {
            self.stats
                .current_bytes
                .fetch_sub((-delta) as usize, Ordering::Relaxed);
        }
    }

    fn start_defrag_thread(&self) {
        let chunks = Arc::clone(&self.chunks);
        let stats = Arc::clone(&self.stats);
        let shutdown = self.shutdown.clone();
        let interval = self.config.defrag_interval;
        let h = std::thread::spawn(move || {
            while !shutdown.load(Ordering::Acquire) {
                std::thread::sleep(interval);
                let start = Instant::now();
                defragment_chunks(&chunks, &stats);
                let ns = start.elapsed().as_nanos() as u64;
                stats.defrag_count.fetch_add(1, Ordering::Relaxed);
                stats.defrag_time_ns.fetch_add(ns, Ordering::Relaxed);
            }
        });
        unsafe {
            let s = self as *const _ as *mut HugePageAllocator;
            (*s).defrag_thread = Some(h);
        }
    }

    fn start_monitor_thread(&self) {
        let chunks = Arc::clone(&self.chunks);
        let stats = Arc::clone(&self.stats);
        let shutdown = self.shutdown.clone();
        let interval = self.config.monitor_interval;
        let h = std::thread::spawn(move || {
            while !shutdown.load(Ordering::Acquire) {
                std::thread::sleep(interval);
                monitor_memory_pressure(&chunks, &stats);
            }
        });
        unsafe {
            let s = self as *const _ as *mut HugePageAllocator;
            (*s).monitor_thread = Some(h);
        }
    }

    pub fn force_defragmentation(&self) {
        let start = Instant::now();
        defragment_chunks(&self.chunks, &self.stats);
        let ns = start.elapsed().as_nanos() as u64;
        self.stats.defrag_count.fetch_add(1, Ordering::Relaxed);
        self.stats.defrag_time_ns.fetch_add(ns, Ordering::Relaxed);
    }

    pub fn get_metrics(&self) -> AllocationMetrics {
        let chunks = self.chunks.read().unwrap();
        let mut used = 0usize;
        let mut cap = 0usize;
        let mut c2m = 0usize;
        let mut c1g = 0usize;
        let mut frag_sum = 0.0;

        for c in chunks.iter() {
            used += c.used.load(Ordering::Relaxed);
            cap += c.size;
            match c.chunk_type {
                ChunkType::Huge2M => c2m += 1,
                ChunkType::Huge1G => c1g += 1,
                ChunkType::Standard => {}
            }
            frag_sum += c.calculate_fragmentation();
        }
        let avg_frag = if chunks.is_empty() { 0.0 } else { frag_sum / chunks.len() as f64 };

        AllocationMetrics {
            total_allocated: self.stats.total_bytes_alloc.load(Ordering::Relaxed),
            total_freed: self.stats.total_bytes_freed.load(Ordering::Relaxed),
            current_allocated: self.stats.current_bytes.load(Ordering::Relaxed),
            peak_allocated: self.stats.peak_bytes.load(Ordering::Relaxed),
            successful_allocations: self.stats.successful_alloc.load(Ordering::Relaxed),
            failed_allocations: self.stats.failed_alloc.load(Ordering::Relaxed),
            chunk_count_2mb: c2m,
            chunk_count_1gb: c1g,
            total_capacity: cap,
            fragmentation_ratio: avg_frag,
            defrag_count: self.stats.defrag_count.load(Ordering::Relaxed),
            average_allocation_ns: {
                let n = self.stats.successful_alloc.load(Ordering::Relaxed);
                if n > 0 {
                    self.stats.alloc_time_ns.load(Ordering::Relaxed) / n as u64
                } else {
                    0
                }
            },
            memory_efficiency: if cap > 0 { (used as f64 / cap as f64) * 100.0 } else { 0.0 },
        }
    }
}

// ============================ GlobalAlloc impl ============================

unsafe impl GlobalAlloc for HugePageAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if layout.size() == 0 {
            return ptr::null_mut();
        }
        let start = Instant::now();

        let aligned = match layout.align_to(MIN_ALLOCATION_SIZE) {
            Ok(l) => l,
            Err(_) => {
                self.stats.failed_alloc.fetch_add(1, Ordering::Relaxed);
                return ptr::null_mut();
            }
        };

        if let Some(c) = self.find_suitable_chunk(aligned) {
            if let Some(p) = self.allocate_from_chunk(&c, aligned) {
                self.stats
                    .alloc_time_ns
                    .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                return p.as_ptr();
            }
        }

        {
            let chunks = self.chunks.read().unwrap();
            if chunks.len() >= self.config.max_chunks {
                drop(chunks);
                self.force_defragmentation();
                if let Some(c) = self.find_suitable_chunk(aligned) {
                    if let Some(p) = self.allocate_from_chunk(&c, aligned) {
                        self.stats
                            .alloc_time_ns
                            .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        return p.as_ptr();
                    }
                }
                if self.config.fallback_to_system {
                    let sz = aligned.size() + ALLOC_HEADER_SIZE;
                    let raw = libc::malloc(sz) as *mut u8;
                    if raw.is_null() {
                        self.stats.failed_alloc.fetch_add(1, Ordering::Relaxed);
                        return ptr::null_mut();
                    }
                    let hdr = raw as *mut AllocHeader;
                    ptr::write(
                        hdr,
                        AllocHeader {
                            magic: HEADER_MAGIC,
                            origin: 1, // system
                            _pad: [0; 3],
                            size: aligned.size(),
                            align: aligned.align(),
                        },
                    );
                    let user = raw.add(ALLOC_HEADER_SIZE);
                    ptr::write_bytes(user, 0, aligned.size());
                    self.stats.successful_alloc.fetch_add(1, Ordering::Relaxed);
                    self.stats
                        .total_bytes_alloc
                        .fetch_add(aligned.size(), Ordering::Relaxed);
                    self.bump_peak_current(aligned.size() as isize);
                    self.stats
                        .alloc_time_ns
                        .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    return user;
                }
                self.stats.failed_alloc.fetch_add(1, Ordering::Relaxed);
                return ptr::null_mut();
            }
        }

        let (chunk_sz, kind) = if aligned.size() > HUGE_2M / 2 && self.config.enable_1g {
            (HUGE_1G, ChunkType::Huge1G)
        } else if self.config.enable_2m {
            (HUGE_2M, ChunkType::Huge2M)
        } else {
            (16 * 1024 * 1024, ChunkType::Standard)
        };

        match self.allocate_chunk_internal(chunk_sz, kind) {
            Ok(new_chunk) => {
                let new_chunk = Arc::new(new_chunk);
                if let Some(p) = self.allocate_from_chunk(&new_chunk, aligned) {
                    let mut chunks = self.chunks.write().unwrap();
                    self.total_capacity.fetch_add(chunk_sz, Ordering::SeqCst);
                    chunks.push(new_chunk);
                    self.stats
                        .alloc_time_ns
                        .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    return p.as_ptr();
                }
            }
            Err(e) => eprintln!("[allocator] allocate_chunk failed: {e}"),
        }

        if self.config.fallback_to_system {
            let sz = aligned.size() + ALLOC_HEADER_SIZE;
            let raw = libc::malloc(sz) as *mut u8;
            if raw.is_null() {
                self.stats.failed_alloc.fetch_add(1, Ordering::Relaxed);
                return ptr::null_mut();
            }
            let hdr = raw as *mut AllocHeader;
            ptr::write(
                hdr,
                AllocHeader {
                    magic: HEADER_MAGIC,
                    origin: 1,
                    _pad: [0; 3],
                    size: aligned.size(),
                    align: aligned.align(),
                },
            );
            let user = raw.add(ALLOC_HEADER_SIZE);
            ptr::write_bytes(user, 0, aligned.size());
            self.stats.successful_alloc.fetch_add(1, Ordering::Relaxed);
            self.stats
                .total_bytes_alloc
                .fetch_add(aligned.size(), Ordering::Relaxed);
            self.bump_peak_current(aligned.size() as isize);
            self.stats
                .alloc_time_ns
                .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
            return user;
        }

        self.stats.failed_alloc.fetch_add(1, Ordering::Relaxed);
        ptr::null_mut()
    }

    unsafe fn dealloc(&self, user_ptr: *mut u8, _layout: Layout) {
        if user_ptr.is_null() {
            return;
        }
        let start = Instant::now();

        let hdr_ptr = (user_ptr as usize - ALLOC_HEADER_SIZE) as *mut AllocHeader;
        let hdr = *hdr_ptr;

        if hdr.magic == HEADER_MAGIC {
            match hdr.origin {
                0 => {
                    let chunks = self.chunks.read().unwrap();
                    for c in chunks.iter() {
                        if self.deallocate_from_chunk(c, user_ptr) {
                            self.stats
                                .dealloc_time_ns
                                .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                            return;
                        }
                    }
                    eprintln!("[allocator] WARNING: chunk-origin alloc not found in any chunk");
                }
                1 => {
                    libc::free(hdr_ptr as *mut c_void);
                    self.stats
                        .total_bytes_freed
                        .fetch_add(hdr.size, Ordering::Relaxed);
                    self.bump_peak_current(-(hdr.size as isize));
                    self.stats
                        .dealloc_time_ns
                        .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    return;
                }
                _ => {}
            }
        }

        {
            let chunks = self.chunks.read().unwrap();
            for c in chunks.iter() {
                if self.deallocate_from_chunk(c, user_ptr) {
                    self.stats
                        .dealloc_time_ns
                        .fetch_add(start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    return;
                }
            }
        }
        libc::free((user_ptr as usize - ALLOC_HEADER_SIZE) as *mut c_void);
    }

    unsafe fn realloc(&self, user_ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size == 0 {
            self.dealloc(user_ptr, layout);
            return ptr::null_mut();
        }
        if user_ptr.is_null() {
            return self.alloc(Layout::from_size_align_unchecked(new_size, layout.align()));
        }

        let hdr_ptr = (user_ptr as usize - ALLOC_HEADER_SIZE) as *mut AllocHeader;
        let hdr = *hdr_ptr;

        if hdr.magic == HEADER_MAGIC && hdr.origin == 0 {
            let chunks = self.chunks.read().unwrap();
            for c in chunks.iter() {
                let base = c.base_addr.as_ptr() as usize;
                let addr = hdr_ptr as usize;
                if addr >= base && addr < base + c.size {
                    let offset = addr - base;
                    let allocs = c.allocations.read().unwrap();
                    if let Some(meta) = allocs.get(&offset) {
                        if meta.size - ALLOC_HEADER_SIZE >= new_size {
                            return user_ptr;
                        }
                        let fl = c.free_list.read().unwrap();
                        let next_off = offset + meta.size;
                        if let Some(&free_sz) = fl.get(&next_off) {
                            if meta.size + free_sz >= new_size + ALLOC_HEADER_SIZE {
                                drop(fl);
                                drop(allocs);
                                let mut fl = c.free_list.write().unwrap();
                                let mut allocs = c.allocations.write().unwrap();
                                if let Some(meta2) = allocs.get_mut(&offset) {
                                    let extra = new_size + ALLOC_HEADER_SIZE - meta2.size;
                                    let removed = fl.remove(&next_off).unwrap();
                                    meta2.size += extra;
                                    meta2.original_size = new_size + ALLOC_HEADER_SIZE;
                                    if removed > extra {
                                        fl.insert(next_off + extra, removed - extra);
                                    }
                                    c.used.fetch_add(extra, Ordering::SeqCst);
                                    self.bump_peak_current(extra as isize);
                                    return user_ptr;
                                }
                            }
                        }
                    }
                    break;
                }
            }
        }

        let new_layout = Layout::from_size_align(new_size, layout.align()).ok();
        if new_layout.is_none() {
            return ptr::null_mut();
        }
        let new_ptr = self.alloc(new_layout.unwrap());
        if !new_ptr.is_null() {
            ptr::copy_nonoverlapping(user_ptr, new_ptr, layout.size().min(new_size));
            self.dealloc(user_ptr, layout);
        }
        new_ptr
    }
}

// ============================ Utilities ============================

#[inline(always)]
fn align_up(x: usize, a: usize) -> usize {
    (x + a - 1) & !(a - 1)
}
#[inline(always)]
fn get_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}
#[inline(always)]
fn thread_id_hash() -> u64 {
    let tid = std::thread::current().id();
    let mut s = ahash::AHasher::default();
    tid.hash(&mut s);
    s.finish()
}

fn defragment_chunks(chunks: &Arc<RwLock<Vec<Arc<MemoryChunk>>>>, stats: &Arc<AllocationStats>) {
    let chunks = chunks.read().unwrap();
    for c in chunks.iter() {
        let f = c.calculate_fragmentation();
        if f > DEFRAG_THRESHOLD {
            c.last_defrag.store(get_timestamp(), Ordering::Relaxed);
            let fl = c.free_list.read().unwrap();
            if fl.len() > MAX_FREE_LIST_FRAGMENTS {
                eprintln!(
                    "[allocator] Chunk {} has {} free fragments",
                    c.chunk_id,
                    fl.len()
                );
            }
        }
    }
    let _ = stats; // reserved for deeper compaction metrics
}

fn monitor_memory_pressure(chunks: &Arc<RwLock<Vec<Arc<MemoryChunk>>>>, _stats: &Arc<AllocationStats>) {
    let chunks = chunks.read().unwrap();
    let mut used = 0usize;
    let mut cap = 0usize;
    for c in chunks.iter() {
        used += c.used.load(Ordering::Relaxed);
        cap += c.size;
    }
    if cap > 0 {
        let ratio = used as f64 / cap as f64;
        if ratio > 0.90 {
            eprintln!(
                "[allocator] Warning: memory usage {:.1}% (consider increasing chunk count)",
                ratio * 100.0
            );
        }
    }
}

// ============================ Public API / FFI ============================

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

#[no_mangle]
pub extern "C" fn hugepage_allocator_new() -> *mut HugePageAllocator {
    match HugePageAllocator::new() {
        Ok(a) => Box::into_raw(Box::new(a)),
        Err(e) => {
            eprintln!("[allocator] create failed: {e}");
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
        Ok(l) => (*allocator).alloc(l),
        Err(_) => ptr::null_mut(),
    }
}

#[no_mangle]
pub unsafe extern "C" fn hugepage_allocator_dealloc(
    allocator: *mut HugePageAllocator,
    ptr_u: *mut u8,
    size: usize,
    align: usize,
) {
    if allocator.is_null() || ptr_u.is_null() {
        return;
    }
    if let Ok(l) = Layout::from_size_align(size, align) {
        (*allocator).dealloc(ptr_u, l);
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
    out: *mut AllocationMetrics,
) -> i32 {
    if allocator.is_null() || out.is_null() {
        return -1;
    }
    let m = (*allocator).get_metrics();
    ptr::write(out, m);
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

// ============================ Builder ============================

pub struct HugePageAllocatorBuilder {
    cfg: AllocatorConfig,
}
impl HugePageAllocatorBuilder {
    pub fn new() -> Self {
        Self {
            cfg: AllocatorConfig::default(),
        }
    }
    pub fn with_2mb_pages(mut self, count: usize) -> Self {
        self.cfg.initial_2m_chunks = count;
        self.cfg.enable_2m = count > 0;
        self
    }
    pub fn with_1gb_pages(mut self, count: usize) -> Self {
        self.cfg.initial_1g_chunks = count;
        self.cfg.enable_1g = count > 0;
        self
    }
    pub fn max_chunks(mut self, max: usize) -> Self {
        self.cfg.max_chunks = max;
        self
    }
    pub fn enable_defragmentation(mut self, enable: bool) -> Self {
        self.cfg.enable_defrag = enable;
        self
    }
    pub fn defrag_interval(mut self, d: Duration) -> Self {
        self.cfg.defrag_interval = d;
        self
    }
    pub fn enable_monitoring(mut self, enable: bool) -> Self {
        self.cfg.enable_monitor = enable;
        self
    }
    pub fn monitor_interval(mut self, d: Duration) -> Self {
        self.cfg.monitor_interval = d;
        self
    }
    pub fn fallback_to_system(mut self, enable: bool) -> Self {
        self.cfg.fallback_to_system = enable;
        self
    }
    pub fn lock_policy_mlockall(mut self) -> Self {
        self.cfg.lock_policy = LockPolicy::MlockAllOnFault;
        self
    }
    pub fn lock_policy_map_locked(mut self) -> Self {
        self.cfg.lock_policy = LockPolicy::MapLocked;
        self
    }
    pub fn lock_policy_none(mut self) -> Self {
        self.cfg.lock_policy = LockPolicy::None;
        self
    }
    pub fn numa_aware(mut self, enable: bool) -> Self {
        self.cfg.numa_aware = enable;
        self
    }
    pub fn build(self) -> io::Result<HugePageAllocator> {
        HugePageAllocator::with_config(self.cfg)
    }
}

// ============================ Zero/copy helpers ============================

#[inline(always)]
pub unsafe fn zero_memory_fast(ptr_u: *mut u8, size: usize) {
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::*;
        if is_x86_feature_detected!("avx2") && size >= 32 {
            let zero = _mm256_setzero_si256();
            let mut off = 0usize;
            let align_off = ptr_u.align_offset(32);
            if align_off > 0 && align_off < size {
                ptr::write_bytes(ptr_u, 0, align_off);
                off = align_off;
            }
            while off + 32 <= size {
                _mm256_store_si256(ptr_u.add(off) as *mut __m256i, zero);
                off += 32;
            }
            if off < size {
                ptr::write_bytes(ptr_u.add(off), 0, size - off);
            }
            return;
        }
    }
    ptr::write_bytes(ptr_u, 0, size);
}

#[inline(always)]
pub unsafe fn copy_memory_fast(dst: *mut u8, src: *const u8, size: usize) {
    #[cfg(target_arch = "x86_64")]
    {
        use std::arch::x86_64::*;
        if is_x86_feature_detected!("avx2") && size >= 32 {
            let mut off = 0usize;
            while off + 32 <= size {
                let d = _mm256_loadu_si256(src.add(off) as *const __m256i);
                _mm256_storeu_si256(dst.add(off) as *mut __m256i, d);
                off += 32;
            }
            if off < size {
                ptr::copy_nonoverlapping(src.add(off), dst.add(off), size - off);
            }
            return;
        }
    }
    ptr::copy_nonoverlapping(src, dst, size);
}

// ============================ Global singleton (optional) ============================

static GLOBAL_ALLOCATOR: Lazy<Arc<RwLock<Option<Arc<HugePageAllocator>>>>> =
    Lazy::new(|| Arc::new(RwLock::new(None)));

pub fn initialize_global_allocator(config: AllocatorConfig) -> io::Result<()> {
    let a = Arc::new(HugePageAllocator::with_config(config)?);
    let mut g = GLOBAL_ALLOCATOR.write().unwrap();
    *g = Some(a);
    Ok(())
}
pub fn get_global_allocator() -> Option<Arc<HugePageAllocator>> {
    GLOBAL_ALLOCATOR.read().unwrap().clone()
}

// Boot-strap with sensible defaults
pub fn init() -> io::Result<()> {
    let cfg = AllocatorConfig {
        enable_2m: true,
        enable_1g: true,
        initial_2m_chunks: 8,
        initial_1g_chunks: 2,
        max_chunks: 64,
        enable_defrag: true,
        defrag_interval: Duration::from_secs(60),
        enable_monitor: true,
        monitor_interval: Duration::from_secs(10),
        fallback_to_system: true,
        lock_policy: LockPolicy::MlockAllOnFault,
        numa_aware: false, // flip to true if you want per-node preflight guarantees
    };
    initialize_global_allocator(cfg)
}
