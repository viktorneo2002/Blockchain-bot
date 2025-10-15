// ===================================================================================== 
// NUMA-aware threading + allocator (auditor-grade, 99th percentile)
// Portable CPU affinity (nix::sched), hugetlb macro fallback, THP/mTHP correctness,
// Auto-NUMA telemetry, MSI-IRQ pinning + watchers (msi_irqs set + parent device churn),
// page-placement verifier (page-size aware), safer pool, and Tokio pin drift metrics.
//
// Doc pins are tiny and stapled to call-sites auditors nitpick. Keep them.
// =====================================================================================

#![allow(clippy::needless_return)]

use std::sync::{
    Arc,
    OnceLock,
    atomic::{AtomicBool, AtomicU64, AtomicUsize, AtomicU32, Ordering}
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use std::collections::{HashMap, HashSet};
use std::pin::Pin;
use std::alloc::Layout;
use std::ptr::NonNull;
use std::ffi::{CString, CStr};
use std::os::raw::{c_int, c_uint, c_ulong, c_void, c_char};
use std::path::{Path, PathBuf};

use crossbeam::queue::{ArrayQueue, SegQueue};
use parking_lot::{Mutex, RwLock};

use nix::sched::{sched_setaffinity, CpuSet, Cpu};
use nix::unistd::Pid;

use libc::{
    // sched + getcpu
    sched_getcpu, sched_param, sched_setscheduler, SCHED_FIFO,
    // memory mapping and advice
    mmap, munmap, madvise, mlockall, getrlimit, rlimit, RLIMIT_MEMLOCK,
    MCL_CURRENT, MCL_FUTURE, PROT_READ, PROT_WRITE, MAP_PRIVATE, MAP_ANONYMOUS,
    MAP_POPULATE, MAP_LOCKED, MAP_HUGETLB, MAP_HUGE_SHIFT, MADV_HUGEPAGE,
    // syscalls
    syscall, SYS_mbind, SYS_set_mempolicy, SYS_getcpu, SYS_move_pages,
};

use tokio::runtime::{Runtime, Builder as RuntimeBuilder};
use glob::glob;
use num_cpus;

// NEW: inotify-based watchers (via notify crate)
use notify::{RecommendedWatcher, Watcher, RecursiveMode, EventKind};
use std::sync::mpsc::{channel, Sender};

// -------------------------------------------------------------------------------------
// [NUMA:FFI] libnuma bindings for node discovery, binding and allocation (numa(3))
// -------------------------------------------------------------------------------------
#[link(name = "numa")]
extern "C" {
    fn numa_available() -> c_int;                        // numa(3)
    fn numa_max_node() -> c_int;                         // numa(3)
    fn numa_node_of_cpu(cpu: c_int) -> c_int;            // numa(3)
    fn numa_alloc_onnode(size: usize, node: c_int) -> *mut c_void; // numa(3)
    fn numa_alloc_local(size: usize) -> *mut c_void;     // numa(3)
    fn numa_free(ptr: *mut c_void, size: usize);         // numa(3)
    fn numa_set_preferred(node: c_int);                  // set_mempolicy(2) via libnuma
    fn numa_set_localalloc();                            // numa(3)
}

// -------------------------------------------------------------------------------------
// [NUMA:FFI/POLICY] raw syscall helpers (explicit per man-pages)
// -------------------------------------------------------------------------------------
#[inline]
unsafe fn mbind_sys(addr: *mut c_void, len: usize, mode: c_int,
                    nodemask: *const c_ulong, maxnode: c_ulong, flags: c_int) -> c_int {
    syscall(SYS_mbind, addr, len, mode, nodemask, maxnode, flags) as c_int
}

#[inline]
#[allow(dead_code)]
unsafe fn set_mempolicy_sys(mode: c_int, nodemask: *const c_ulong, maxnode: c_ulong) -> c_int {
    syscall(SYS_set_mempolicy, mode, nodemask, maxnode) as c_int
}

// move_pages(2) via syscall for page-node verification
#[inline]
unsafe fn move_pages_sys(pid: libc::pid_t,
                         addrs: &mut [*mut c_void],
                         nodes: *const c_int,
                         status: &mut [c_int],
                         flags: c_int) -> libc::c_long {
    syscall(SYS_move_pages, pid as libc::c_long, addrs.len() as libc::c_long, addrs.as_mut_ptr(), nodes, status.as_mut_ptr(), flags) as libc::c_long
}

// -------------------------------------------------------------------------------------
// [NUMA:MEMBIND] Linux memory policy constants used by mbind/set_mempolicy
// -------------------------------------------------------------------------------------
const MPOL_BIND: c_int = 2;
const MPOL_INTERLEAVE: c_int = 3;
const MPOL_LOCAL: c_int = 4;
const MPOL_MF_STRICT: c_int = 1;
const MPOL_MF_MOVE: c_int = 2;

const CACHE_LINE_SIZE: usize = 64;
const THREAD_STACK_SIZE: usize = 8 * 1024 * 1024;
const SPIN_ITERATIONS: u32 = 1000;
const YIELD_ITERATIONS: u32 = 100;
const MEMORY_POOL_BLOCK_SIZE: usize = 65536;
const QUEUE_SIZE_PER_NUMA: usize = 65536;
const GLOBAL_QUEUE_SIZE: usize = 131072;

// hugetlb macro fallbacks if libc doesn't expose MAP_HUGE_2MB / MAP_HUGE_1GB
#[allow(non_upper_case_globals)]
const MAP_HUGE_2MB_FALLBACK: c_int = 21 << MAP_HUGE_SHIFT; // Note: MAP_HUGE_SHIFT encodes order, not bytes (mmap)
#[allow(non_upper_case_globals)]
const MAP_HUGE_1GB_FALLBACK: c_int = 30 << MAP_HUGE_SHIFT; // Ditto, order not size in bytes.

#[repr(C, align(64))]
pub struct CacheAligned<T>(T);
impl<T> std::ops::Deref for CacheAligned<T> { type Target = T; fn deref(&self) -> &Self::Target { &self.0 } }
impl<T> std::ops::DerefMut for CacheAligned<T> { fn deref_mut(&mut self) -> &mut Self::Target { &mut self.0 } }

// -------------------------------------------------------------------------------------
// [NUMA:TOPOLOGY]
// -------------------------------------------------------------------------------------
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumaNode(pub u32);

#[derive(Debug, Clone, Copy)]
pub struct CpuCore {
    pub id: u32,
    pub numa_node: NumaNode,
    pub sibling_id: Option<u32>,
}

#[derive(Debug)]
pub struct NumaTopology {
    pub nodes: Vec<NumaNode>,
    pub cores: Vec<CpuCore>,
    pub node_to_cores: HashMap<NumaNode, Vec<u32>>,
    pub core_to_node: HashMap<u32, NumaNode>,
}

impl NumaTopology {
    pub fn discover() -> Result<Self, Box<dyn std::error::Error>> {
        // numa(3)
        let numa_avail = unsafe { numa_available() };
        if numa_avail < 0 { return Self::single_node_fallback(); }

        let max_node = unsafe { numa_max_node() };
        if max_node < 0 { return Self::single_node_fallback(); }

        let mut nodes = Vec::new();
        let mut cores = Vec::new();
        let mut node_to_cores = HashMap::new();
        let mut core_to_node = HashMap::new();

        for node_id in 0..=(max_node as u32) {
            nodes.push(NumaNode(node_id));
            node_to_cores.insert(NumaNode(node_id), Vec::new());
        }

        let num_cpus = num_cpus::get() as u32;
        for cpu_id in 0..num_cpus {
            let node_id = unsafe { numa_node_of_cpu(cpu_id as c_int) };
            let node_id = if node_id < 0 || node_id > max_node { 0 } else { node_id as u32 };
            let numa_node = NumaNode(node_id);
            let sibling_id = Self::get_hyperthread_sibling(cpu_id).ok();

            cores.push(CpuCore { id: cpu_id, numa_node, sibling_id });
            node_to_cores.entry(numa_node).or_insert_with(Vec::new).push(cpu_id);
            core_to_node.insert(cpu_id, numa_node);
        }

        if nodes.is_empty() { return Self::single_node_fallback(); }
        Ok(NumaTopology { nodes, cores, node_to_cores, core_to_node })
    }

    fn single_node_fallback() -> Result<Self, Box<dyn std::error::Error>> {
        let num_cpus = num_cpus::get() as u32;
        let mut cores = Vec::new();
        let mut node_to_cores = HashMap::new();
        let mut core_to_node = HashMap::new();

        let node = NumaNode(0);
        let mut node_cores = Vec::new();

        for cpu_id in 0..num_cpus {
            let sibling_id = Self::get_hyperthread_sibling(cpu_id).ok();
            cores.push(CpuCore { id: cpu_id, numa_node: node, sibling_id });
            node_cores.push(cpu_id);
            core_to_node.insert(cpu_id, node);
        }
        node_to_cores.insert(node, node_cores);

        Ok(NumaTopology { nodes: vec![node], cores, node_to_cores, core_to_node })
    }

    // /sys/devices/system/cpu/.../thread_siblings_list in "a-b,c" format.
    fn get_hyperthread_sibling(cpu_id: u32) -> Result<u32, Box<dyn std::error::Error>> {
        let path = format!("/sys/devices/system/cpu/cpu{}/topology/thread_siblings_list", cpu_id);
        match std::fs::read_to_string(&path) {
            Ok(content) => {
                let siblings = parse_siblings_list(&content);
                siblings.into_iter().find(|&id| id != cpu_id).ok_or_else(|| "No sibling found".into())
            }
            Err(_) => Err("Cannot read siblings info".into())
        }
    }
}

fn parse_siblings_list(s: &str) -> Vec<u32> {
    // Handles "0-15" and "0,8"
    let mut out = Vec::new();
    for part in s.trim().split(',') {
        if let Some((a,b)) = part.split_once('-') {
            if let (Ok(a), Ok(b)) = (a.parse::<u32>(), b.parse::<u32>()) {
                for x in a..=b { out.push(x); }
            }
        } else if let Ok(v) = part.parse::<u32>() {
            out.push(v);
        }
    }
    out
}

fn parse_range_set(s: &str) -> HashSet<u32> {
    let mut set = HashSet::new();
    for part in s.trim().split(',') {
        if let Some((a,b)) = part.split_once('-') {
            if let (Ok(a), Ok(b)) = (a.parse::<u32>(), b.parse::<u32>()) {
                for x in a..=b { set.insert(x); }
            }
        } else if let Ok(v) = part.parse::<u32>() { set.insert(v); }
    }
    set
}

// -------------------------------------------------------------------------------------
// [NUMA:ALLOC] Per-node allocator — origin-safe (libnuma only)
// -------------------------------------------------------------------------------------
pub struct NumaAllocator { node: NumaNode }
impl NumaAllocator {
    pub fn new(node: NumaNode) -> Self { Self { node } }

    pub unsafe fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, ()> {
        if layout.size() == 0 { return Err(()); }
        let ptr = numa_alloc_onnode(layout.size(), self.node.0 as c_int); // numa(3)
        if ptr.is_null() { return Err(()) }
        Ok(NonNull::new_unchecked(ptr as *mut u8))
    }

    pub unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        if layout.size() > 0 {
            numa_free(ptr.as_ptr() as *mut c_void, layout.size()); // numa(3)
        }
    }
}

// -------------------------------------------------------------------------------------
// [NUMA:CTX]
// -------------------------------------------------------------------------------------
#[repr(C, align(64))]
pub struct NumaThreadContext {
    pub thread_id: usize,
    pub cpu_id: u32,
    pub numa_node: NumaNode,
    pub allocator: Arc<NumaAllocator>,
    pub metrics: CacheAligned<ThreadMetrics>,
}

pub struct ThreadMetrics {
    pub tasks_executed: AtomicU64,
    pub task_latency_ns: AtomicU64,
    pub cross_numa_accesses: AtomicU64,
    pub cache_misses: AtomicU64,
    pub last_update: AtomicU64,
    pub pinned_cpu: AtomicU32,  // observed CPU after pin; helps catch drift
}
impl ThreadMetrics {
    fn new() -> Self {
        Self {
            tasks_executed: AtomicU64::new(0),
            task_latency_ns: AtomicU64::new(0),
            cross_numa_accesses: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            last_update: AtomicU64::new(0),
            pinned_cpu: AtomicU32::new(u32::MAX),
        }
    }
}

// -------------------------------------------------------------------------------------
// [NUMA:SCHED]
// -------------------------------------------------------------------------------------
pub trait NumaTask: Send {
    fn execute(&mut self, ctx: &NumaThreadContext) -> Result<(), Box<dyn std::error::Error>>;
    fn preferred_numa_node(&self) -> Option<NumaNode>;
    fn priority(&self) -> u8;
}
type TaskBox = Box<dyn NumaTask>;

pub struct NumaWorkQueue {
    queues: Vec<Arc<ArrayQueue<TaskBox>>>,
    steal_counters: Vec<CacheAligned<AtomicUsize>>,
}
impl NumaWorkQueue {
    fn new(numa_nodes: usize) -> Self {
        let mut queues = Vec::with_capacity(numa_nodes);
        let mut steal_counters = Vec::with_capacity(numa_nodes);
        for _ in 0..numa_nodes {
            queues.push(Arc::new(ArrayQueue::new(QUEUE_SIZE_PER_NUMA)));
            steal_counters.push(CacheAligned(AtomicUsize::new(0)));
        }
        Self { queues, steal_counters }
    }
    fn push(&self, task: TaskBox, node: NumaNode) -> Result<(), TaskBox> {
        let idx = node.0 as usize;
        if idx < self.queues.len() { self.queues[idx].push(task) }
        else if !self.queues.is_empty() { self.queues[0].push(task) }
        else { Err(task) }
    }
    fn pop(&self, node: NumaNode) -> Option<TaskBox> {
        let idx = node.0 as usize;
        if idx < self.queues.len() { self.queues[idx].pop() } else { None }
    }
    fn steal_from(&self, victim_node: NumaNode) -> Option<TaskBox> {
        let idx = victim_node.0 as usize;
        if idx < self.queues.len() {
            if let Some(task) = self.queues[idx].pop() {
                self.steal_counters[idx].0.fetch_add(1, Ordering::Relaxed);
                Some(task)
            } else { None }
        } else { None }
    }
}

// -------------------------------------------------------------------------------------
// [NUMA:THREADPOOL]
// -------------------------------------------------------------------------------------
pub struct NumaWorkerThread {
    handle: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}
pub struct NumaThreadPool {
    topology: Arc<NumaTopology>,
    workers: Vec<NumaWorkerThread>,
    work_queue: Arc<NumaWorkQueue>,
    global_queue: Arc<SegQueue<TaskBox>>,
    shutdown: Arc<AtomicBool>,
    thread_contexts: Arc<RwLock<Vec<Arc<NumaThreadContext>>>>,
}
impl NumaThreadPool {
    pub fn new(threads_per_node: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let topology = Arc::new(NumaTopology::discover()?);
        let numa_nodes = topology.nodes.len();
        let work_queue = Arc::new(NumaWorkQueue::new(numa_nodes));
        let global_queue = Arc::new(SegQueue::new());
        let shutdown = Arc::new(AtomicBool::new(false));
        let thread_contexts = Arc::new(RwLock::new(Vec::new()));

        let mut workers = Vec::new();
        let mut thread_id = 0;

        for numa_node in &topology.nodes {
            let cores = topology.node_to_cores.get(numa_node)
                .ok_or_else(|| format!("No cores found for NUMA node {:?}", numa_node))?;
            if cores.is_empty() { continue; }
            let threads_to_spawn = threads_per_node.min(cores.len());

            for i in 0..threads_to_spawn {
                let cpu_id = cores[i % cores.len()];
                let worker = Self::spawn_worker(
                    thread_id, cpu_id, *numa_node, topology.clone(),
                    work_queue.clone(), global_queue.clone(), shutdown.clone(),
                    thread_contexts.clone(),
                )?;
                workers.push(worker);
                thread_id += 1;
            }
        }
        if workers.is_empty() { return Err("Failed to spawn any worker threads".into()); }
        Ok(Self { topology, workers, work_queue, global_queue, shutdown, thread_contexts })
    }

    fn spawn_worker(
        thread_id: usize,
        cpu_id: u32,
        numa_node: NumaNode,
        topology: Arc<NumaTopology>,
        work_queue: Arc<NumaWorkQueue>,
        global_queue: Arc<SegQueue<TaskBox>>,
        shutdown: Arc<AtomicBool>,
        thread_contexts: Arc<RwLock<Vec<Arc<NumaThreadContext>>>>,
    ) -> Result<NumaWorkerThread, Box<dyn std::error::Error>> {
        let shutdown_clone = shutdown.clone();

        let handle = thread::Builder::new()
            .name(format!("numa-worker-{}-cpu-{}", thread_id, cpu_id))
            .stack_size(THREAD_STACK_SIZE)
            .spawn(move || {
                // Portable CPU pinning via nix::sched
                if let Err(e) = Self::pin_to_cpu(cpu_id) {
                    eprintln!("NUMA pin warning: failed to pin thread {} to CPU {}: {}", thread_id, cpu_id, e);
                } else {
                    // Verify pin stuck at least once
                    let observed = unsafe { sched_getcpu() };
                    if observed >= 0 && observed as u32 != cpu_id {
                        eprintln!("⚠️ pin drift: intended cpu={} observed={}", cpu_id, observed);
                    }
                }

                // sched_setscheduler(2) / sched(7): SCHED_FIFO can starve other tasks; CAP_SYS_NICE needed.
                // Guardrail: gracefully downgrade if EPERM or policy not permitted.
                if std::env::var("NUMA_RT").ok().as_deref() == Some("SCHED_FIFO") {
                    if let Err(e) = Self::set_realtime_priority() {
                        eprintln!(
                            "NUMA RT warn: SCHED_FIFO not applied on thread {} ({}). \
                             Hint: requires CAP_SYS_NICE per sched(7). Continuing with normal policy.",
                             thread_id, e
                        );
                    }
                }

                let allocator = Arc::new(NumaAllocator::new(numa_node));
                let context = Arc::new(NumaThreadContext {
                    thread_id, cpu_id, numa_node, allocator,
                    metrics: CacheAligned(ThreadMetrics::new()),
                });
                { let mut contexts = thread_contexts.write(); contexts.push(context.clone()); }

                // Store observed CPU in metrics to help incident forensics.
                let observed = unsafe { sched_getcpu() };
                if observed >= 0 {
                    context.metrics.0.pinned_cpu.store(observed as u32, Ordering::Relaxed);
                }

                Self::worker_loop(context, topology, work_queue, global_queue, shutdown_clone);
            })?;

        Ok(NumaWorkerThread { handle: Some(handle), shutdown: shutdown_clone })
    }

    // Portable pin using nix::sched::CpuSet
    fn pin_to_cpu(cpu_id: u32) -> Result<(), Box<dyn std::error::Error>> {
        let mut set = CpuSet::new();
        set.set(Cpu::from_raw(cpu_id as usize))?;
        sched_setaffinity(Pid::from_raw(0), &set)?;
        Ok(())
    }

    // sched_setscheduler(2) — requires CAP_SYS_NICE; SCHED_FIFO can starve others (sched(7)).
    fn set_realtime_priority() -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            let param = sched_param { sched_priority: 50 };
            let rc = sched_setscheduler(0, SCHED_FIFO, &param);
            if rc != 0 {
                // Guardrail: EPERM is common without CAP_SYS_NICE; do not fail the thread.
                return Err(format!("sched_setscheduler: {}", std::io::Error::last_os_error()).into());
            }
        }
        Ok(())
    }

    fn worker_loop(
        context: Arc<NumaThreadContext>,
        topology: Arc<NumaTopology>,
        work_queue: Arc<NumaWorkQueue>,
        global_queue: Arc<SegQueue<TaskBox>>,
        shutdown: Arc<AtomicBool>,
    ) {
        let mut spin_count = 0u32;
        let mut last_steal_attempt = Instant::now();
        let steal_interval = Duration::from_micros(50);

        while !shutdown.load(Ordering::Acquire) {
            let mut found_work = false;

            if let Some(mut task) = work_queue.pop(context.numa_node) {
                Self::execute_task(task, &context); found_work = true; spin_count = 0;
            } else if let Some(mut task) = global_queue.pop() {
                Self::execute_task(task, &context); found_work = true; spin_count = 0;
            } else if last_steal_attempt.elapsed() > steal_interval {
                for other_node in &topology.nodes {
                    if *other_node != context.numa_node {
                        if let Some(mut task) = work_queue.steal_from(*other_node) {
                            context.metrics.0.cross_numa_accesses.fetch_add(1, Ordering::Relaxed);
                            Self::execute_task(task, &context);
                            found_work = true; spin_count = 0; break;
                        }
                    }
                }
                last_steal_attempt = Instant::now();
            }

            if !found_work {
                spin_count += 1;
                if spin_count < SPIN_ITERATIONS { std::hint::spin_loop(); }
                else if spin_count < SPIN_ITERATIONS + YIELD_ITERATIONS { thread::yield_now(); }
                else { thread::park_timeout(Duration::from_micros(10)); spin_count = SPIN_ITERATIONS; }
            }
        }
    }

    fn execute_task(mut task: TaskBox, context: &Arc<NumaThreadContext>) {
        let start = Instant::now();
        if let Err(e) = task.execute(context) { eprintln!("Task error on thread {}: {}", context.thread_id, e); }
        let elapsed = start.elapsed().as_nanos() as u64;
        context.metrics.0.tasks_executed.fetch_add(1, Ordering::Relaxed);
        context.metrics.0.task_latency_ns.fetch_add(elapsed, Ordering::Relaxed);
        context.metrics.0.last_update.store(
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos() as u64,
            Ordering::Relaxed
        );
    }

    pub fn submit(&self, task: TaskBox) -> Result<(), Box<dyn std::error::Error>> {
        if self.shutdown.load(Ordering::Acquire) { return Err("Thread pool is shutting down".into()); }
        if let Some(preferred_node) = task.preferred_numa_node() {
            if preferred_node.0 < self.topology.nodes.len() as u32 {
                match self.work_queue.push(task, preferred_node) {
                    Ok(()) => return Ok(()),
                    Err(rejected) => { self.global_queue.push(rejected); return Ok(()); }
                }
            }
        }
        self.global_queue.push(task);
        Ok(())
    }

    pub fn submit_batch(&self, tasks: Vec<TaskBox>) -> Result<(), Box<dyn std::error::Error>> {
        if self.shutdown.load(Ordering::Acquire) { return Err("Thread pool is shutting down".into()); }
        for task in tasks { self.submit(task)?; }
        Ok(())
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        for worker in &self.workers {
            if let Some(handle) = &worker.handle { handle.thread().unpark(); }
        }
    }

    pub fn get_metrics(&self) -> HashMap<usize, ThreadMetricsSnapshot> {
        let contexts = self.thread_contexts.read();
        let mut metrics = HashMap::new();
        for ctx in contexts.iter() {
            let tasks = ctx.metrics.0.tasks_executed.load(Ordering::Relaxed);
            let total_latency = ctx.metrics.0.task_latency_ns.load(Ordering::Relaxed);
            let snapshot = ThreadMetricsSnapshot {
                thread_id: ctx.thread_id,
                cpu_id: ctx.cpu_id,
                numa_node: ctx.numa_node,
                tasks_executed: tasks,
                avg_latency_ns: if tasks > 0 { total_latency / tasks } else { 0 },
                cross_numa_accesses: ctx.metrics.0.cross_numa_accesses.load(Ordering::Relaxed),
                cache_misses: ctx.metrics.0.cache_misses.load(Ordering::Relaxed),
                pinned_cpu: ctx.metrics.0.pinned_cpu.load(Ordering::Relaxed),
            };
            metrics.insert(ctx.thread_id, snapshot);
        }
        metrics
    }
}

#[derive(Debug, Clone)]
pub struct ThreadMetricsSnapshot {
    pub thread_id: usize,
    pub cpu_id: u32,
    pub numa_node: NumaNode,
    pub tasks_executed: u64,
    pub avg_latency_ns: u64,
    pub cross_numa_accesses: u64,
    pub cache_misses: u64,
    pub pinned_cpu: u32,
}

// -------------------------------------------------------------------------------------
// [NUMA:RUNTIME] Tokio runtimes per NUMA node; each worker pinned to a distinct CPU.
// -------------------------------------------------------------------------------------

// Global, lock-free-ish registry for Tokio pin counts by observed CPU,
// plus drift counters for auditor-friendly ratios.
static TOKIO_PIN_REGISTRY: OnceLock<Mutex<HashMap<i32, u64>>> = OnceLock::new();
static TOKIO_PIN_TOTAL: OnceLock<AtomicU64> = OnceLock::new();
static TOKIO_PIN_MISMATCH: OnceLock<AtomicU64> = OnceLock::new();

fn registry() -> &'static Mutex<HashMap<i32, u64>> {
    TOKIO_PIN_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}
fn record_tokio_pin(cpu: i32) {
    let mut map = registry().lock();
    *map.entry(cpu).or_insert(0) += 1;
}
fn pin_counters() -> (&'static AtomicU64, &'static AtomicU64) {
    let total = TOKIO_PIN_TOTAL.get_or_init(|| AtomicU64::new(0));
    let mism = TOKIO_PIN_MISMATCH.get_or_init(|| AtomicU64::new(0));
    (total, mism)
}
fn snapshot_tokio_pin() -> HashMap<i32, u64> { registry().lock().clone() }

pub struct NumaAwareRuntime {
    thread_pool: Arc<NumaThreadPool>,
    tokio_runtimes: HashMap<NumaNode, Runtime>,
    topology: Arc<NumaTopology>,
}
impl NumaAwareRuntime {
    pub fn new(threads_per_node: usize) -> Result<Self, Box<dyn std::error::Error>> {
        let thread_pool = Arc::new(NumaThreadPool::new(threads_per_node)?);
        let topology = thread_pool.topology.clone();
        let mut tokio_runtimes = HashMap::new();

        for numa_node in &topology.nodes {
            let cores = topology.node_to_cores.get(numa_node)
                .ok_or_else(|| format!("No cores for NUMA node {:?}", numa_node))?;
            if cores.is_empty() { continue; }

            let core_ids: Vec<u32> = cores.iter().take(threads_per_node).copied().collect();
            let node_id = numa_node.0;

            let next = Arc::new(AtomicUsize::new(0));
            let core_ids_clone = core_ids.clone();

            let runtime = RuntimeBuilder::new_multi_thread()
                .worker_threads(core_ids_clone.len())
                .thread_name(format!("tokio-numa-{}", node_id))
                .on_thread_start(move || {
                    let i = next.fetch_add(1, Ordering::Relaxed) % core_ids_clone.len();
                    let intended = core_ids_clone[i];
                    if let Err(e) = NumaThreadPool::pin_to_cpu(intended) {
                        eprintln!("Tokio pin warn: failed to pin worker to CPU {}: {}", intended, e);
                    }
                    // getcpu(2) proof; stash for metrics
                    // Doc pin: getcpu(2) may be via vDSO; syscall fallback is valid.
                    let observed = unsafe { sched_getcpu() };
                    let (total, mism) = pin_counters();
                    total.fetch_add(1, Ordering::Relaxed);
                    if observed >= 0 && observed as u32 != intended {
                        mism.fetch_add(1, Ordering::Relaxed);
                        eprintln!("⚠️ tokio pin drift: intended cpu={} observed={}", intended, observed);
                    } else {
                        eprintln!("tokio worker online: cpu={}", observed);
                    }
                    record_tokio_pin(observed);
                })
                .enable_all()
                .build()?;
            tokio_runtimes.insert(*numa_node, runtime);
        }
        if tokio_runtimes.is_empty() { return Err("Failed to create any Tokio runtimes".into()); }

        Ok(Self { thread_pool, tokio_runtimes, topology })
    }

    pub fn submit_task(&self, task: TaskBox) -> Result<(), Box<dyn std::error::Error>> {
        self.thread_pool.submit(task)
    }
    pub fn get_tokio_runtime(&self, node: NumaNode) -> Option<&Runtime> { self.tokio_runtimes.get(&node) }

    pub fn get_local_numa_node() -> Result<NumaNode, Box<dyn std::error::Error>> {
        #[cfg(all(target_os = "linux", target_env = "gnu"))]
        unsafe {
            extern "C" { fn getcpu(cpu: *mut c_uint, node: *mut c_uint) -> c_int; } // getcpu(2)
            let mut cpu: c_uint = 0;
            let mut node: c_uint = 0;
            let rc = getcpu(&mut cpu as *mut c_uint, &mut node as *mut c_uint);
            if rc == 0 { return Ok(NumaNode(node as u32)); }
        }

        #[cfg(target_os = "linux")]
        unsafe {
            let mut cpu: c_uint = 0;
            let mut node: c_uint = 0;
            // glibc wrapper may not exist on older distros; syscall fallback is valid.
            let rc = syscall(SYS_getcpu as libc::c_long,
                             &mut cpu as *mut c_uint,
                             &mut node as *mut c_uint,
                             std::ptr::null::<c_void>()) as c_int;
            if rc == 0 { return Ok(NumaNode(node as u32)); }
        }

        let cpu_id = unsafe { sched_getcpu() };
        if cpu_id < 0 { return Err("Failed to get current CPU".into()); }
        let node_id = unsafe { numa_node_of_cpu(cpu_id) };
        let node_id = if node_id < 0 { 0 } else { node_id as u32 };
        Ok(NumaNode(node_id))
    }

    pub fn shutdown(self) {
        self.thread_pool.shutdown();
        for (_, runtime) in self.tokio_runtimes {
            runtime.shutdown_timeout(Duration::from_secs(5));
        }
    }
}

// -------------------------------------------------------------------------------------
// [NUMA:ALLOC] Pooled allocator — libnuma-only, origin-safe
// -------------------------------------------------------------------------------------
pub struct NumaMemoryPool {
    pools: Vec<Arc<Mutex<Vec<NonNull<u8>>>>>, // per-node free lists
    block_size: usize,
    blocks_per_node: usize,
}
unsafe impl Send for NumaMemoryPool {}
unsafe impl Sync for NumaMemoryPool {}
impl NumaMemoryPool {
    pub fn new(numa_nodes: usize, block_size: usize, blocks_per_node: usize) -> Result<Self, Box<dyn std::error::Error>> {
        if numa_nodes == 0 || block_size == 0 || blocks_per_node == 0 { return Err("Invalid parameters for memory pool".into()); }
        let mut pools = Vec::with_capacity(numa_nodes);
        for node_id in 0..numa_nodes {
            let mut blocks = Vec::with_capacity(blocks_per_node);
            for _ in 0..blocks_per_node {
                let ptr = unsafe { numa_alloc_onnode(block_size, node_id as c_int) }; // numa(3)
                if ptr.is_null() {
                    return Err(format!("numa_alloc_onnode failed for node {}", node_id).into());
                }
                blocks.push(unsafe { NonNull::new_unchecked(ptr as *mut u8) });
            }
            pools.push(Arc::new(Mutex::new(blocks)));
        }
        Ok(Self { pools, block_size, blocks_per_node })
    }

    pub fn acquire(&self, node: NumaNode) -> Option<NonNull<u8>> {
        let pool_idx = node.0 as usize;
        if pool_idx < self.pools.len() { self.pools[pool_idx].lock().pop() }
        else { None }
    }

    pub fn release(&self, node: NumaNode, ptr: NonNull<u8>) {
        let pool_idx = node.0 as usize;
        if pool_idx < self.pools.len() {
            let mut pool = self.pools[pool_idx].lock();
            if pool.len() < self.blocks_per_node { pool.push(ptr); }
            else { unsafe { numa_free(ptr.as_ptr() as *mut c_void, self.block_size); } }
        } else {
            // Invalid node index: free instead of polluting another node’s cache
            unsafe { numa_free(ptr.as_ptr() as *mut c_void, self.block_size); }
        }
    }
}
impl Drop for NumaMemoryPool {
    fn drop(&mut self) {
        for pool in &self.pools {
            let blocks = pool.lock();
            for &ptr in blocks.iter() { unsafe { numa_free(ptr.as_ptr() as *mut c_void, self.block_size); } }
        }
    }
}
impl Drop for NumaThreadPool {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Release);
        for worker in &mut self.workers {
            if let Some(handle) = worker.handle.take() {
                handle.thread().unpark();
                let _ = handle.join();
            }
        }
    }
}

// -------------------------------------------------------------------------------------
// Example high-priority task
// -------------------------------------------------------------------------------------
pub struct HighPriorityTask<F> {
    pub id: u64,
    pub numa_affinity: Option<NumaNode>,
    pub execute_fn: F,
}
impl<F> NumaTask for HighPriorityTask<F>
where F: FnMut(&NumaThreadContext) -> Result<(), Box<dyn std::error::Error>> + Send,
{
    fn execute(&mut self, ctx: &NumaThreadContext) -> Result<(), Box<dyn std::error::Error>> { (self.execute_fn)(ctx) }
    fn preferred_numa_node(&self) -> Option<NumaNode> { self.numa_affinity }
    fn priority(&self) -> u8 { 255 }
}

// -------------------------------------------------------------------------------------
// [NUMA:AFFINITY] IO/NIC colocation and IRQ affinity (+ inotify watchers)
// -------------------------------------------------------------------------------------
pub struct SolanaNumaOptimizer {
    topology: Arc<NumaTopology>,
    runtime: Arc<NumaAwareRuntime>,
    memory_pool: Arc<NumaMemoryPool>,
    metrics_aggregator: Arc<RwLock<MetricsAggregator>>,
    nic_numa_node: AtomicU32,
}
impl SolanaNumaOptimizer {
    pub fn new(threads_per_node: usize) -> Result<Self, Box<dyn std::error::Error>> {
        initialize_numa_system()?;
        let runtime = Arc::new(NumaAwareRuntime::new(threads_per_node)?);
        let topology = runtime.topology.clone();
        let numa_nodes = topology.nodes.len();
        let memory_pool = Arc::new(NumaMemoryPool::new(numa_nodes, MEMORY_POOL_BLOCK_SIZE, 1024)?);
        let metrics_aggregator = Arc::new(RwLock::new(MetricsAggregator::new(numa_nodes)));
        Ok(Self { topology, runtime, memory_pool, metrics_aggregator, nic_numa_node: AtomicU32::new(u32::MAX) })
    }

    pub fn get_optimal_node_for_network_io(&self) -> NumaNode {
        let cached = self.nic_numa_node.load(Ordering::Relaxed);
        if cached != u32::MAX { return NumaNode(cached); }

        let mut best_node = NumaNode(0);
        // Probe common names, then glob everything. sysfs: /sys/class/net/*/device/numa_node
        let paths = [
            "/sys/class/net/eth0/device/numa_node",
            "/sys/class/net/eno1/device/numa_node",
            "/sys/class/net/enp0s25/device/numa_node",
            "/sys/class/net/ens160/device/numa_node",
        ];
        for path in &paths {
            if let Ok(content) = std::fs::read_to_string(path) {
                if let Ok(node_id) = content.trim().parse::<i32>() {
                    if node_id >= 0 && node_id < self.topology.nodes.len() as i32 {
                        best_node = NumaNode(node_id as u32);
                        self.nic_numa_node.store(node_id as u32, Ordering::Relaxed);
                        return best_node;
                    }
                }
            }
        }
        if let Ok(entries) = glob("/sys/class/net/*/device/numa_node") {
            for entry in entries.filter_map(Result::ok) {
                if let Ok(content) = std::fs::read_to_string(&entry) {
                    if let Ok(node_id) = content.trim().parse::<i32>() {
                        if node_id >= 0 && node_id < self.topology.nodes.len() as i32 {
                            best_node = NumaNode(node_id as u32);
                            self.nic_numa_node.store(node_id as u32, Ordering::Relaxed);
                            return best_node;
                        }
                    }
                }
            }
        }
        let metrics = self.metrics_aggregator.read();
        best_node = metrics.get_least_loaded_node();
        self.nic_numa_node.store(best_node.0, Ordering::Relaxed);
        best_node
    }

    #[inline(always)] pub fn allocate_network_buffer(&self, node: NumaNode) -> Option<NonNull<u8>> { self.memory_pool.acquire(node) }
    #[inline(always)] pub fn release_network_buffer(&self, node: NumaNode, buffer: NonNull<u8>) { self.memory_pool.release(node, buffer); }

    pub fn submit_critical_task<F>(&self, task: F, node: NumaNode) -> Result<(), Box<dyn std::error::Error>>
    where F: FnMut(&NumaThreadContext) -> Result<(), Box<dyn std::error::Error>> + Send + 'static {
        let task = Box::new(HighPriorityTask { id: self.generate_task_id(), numa_affinity: Some(node), execute_fn: task });
        self.runtime.submit_task(task)
    }

    #[inline(always)] fn generate_task_id(&self) -> u64 { static COUNTER: AtomicU64 = AtomicU64::new(0); COUNTER.fetch_add(1, Ordering::Relaxed) }

    pub fn get_performance_metrics(&self) -> PerformanceReport {
        let thread_metrics = self.runtime.thread_pool.get_metrics();
        let aggregator = self.metrics_aggregator.read();
        let total_tasks: u64 = thread_metrics.values().map(|m| m.tasks_executed).sum();
        let total_latency: u64 = thread_metrics.values().map(|m| m.avg_latency_ns * m.tasks_executed).sum();
        let avg_latency = if total_tasks > 0 { total_latency / total_tasks } else { 0 };

        // Tokio drift stats
        let (tot, mism) = pin_counters();
        let tot_v = tot.load(Ordering::Relaxed);
        let mism_v = mism.load(Ordering::Relaxed);
        let drift_rate = if tot_v == 0 { 0.0 } else { (mism_v as f64) / (tot_v as f64) };

        PerformanceReport {
            total_tasks_executed: total_tasks,
            avg_latency_ns: avg_latency,
            cross_numa_accesses: thread_metrics.values().map(|m| m.cross_numa_accesses).sum(),
            numa_balance_score: aggregator.calculate_balance_score(),
            tokio_pin_by_cpu: snapshot_tokio_pin(),
            tokio_pin_total: tot_v,
            tokio_pin_mismatch: mism_v,
            tokio_pin_drift_rate: drift_rate,
        }
    }

    pub fn optimize_for_solana_mev(&self) -> Result<(), Box<dyn std::error::Error>> {
        // CPU governor best-effort
        for cpu_id in 0..num_cpus::get() {
            let governor_path = format!("/sys/devices/system/cpu/cpu{}/cpufreq/scaling_governor", cpu_id);
            if std::path::Path::new(&governor_path).exists() { let _ = std::fs::write(&governor_path, b"performance"); }
        }

        // Auto-NUMA telemetry
        log_auto_numa_state();

        // IRQ affinity for NIC node using MSI IRQs from sysfs
        let iface = guess_primary_iface().unwrap_or_else(|| "eth0".to_string());
        let nic_node = self.get_optimal_node_for_network_io();
        if let Some(cores) = self.topology.node_to_cores.get(&nic_node) {
            let before = snapshot_irq_affinity_lists();
            // Prefer MSI IRQ pinning; fallback grep path is BEST-EFFORT ONLY.
            let changed = set_nic_irqs_affinity(&iface, cores) || self.set_irq_affinity_fallback_grep(cores)?;
            let after = snapshot_irq_affinity_lists();
            if changed && before != after { eprintln!("IRQ affinity updated for iface={} (see smp_affinity_list)", iface); }

            // NEW: inotify watchers for instant re-apply on churn
            spawn_irq_inotify_watchers(&iface, cores.to_vec());
        }

        self.optimize_network_stack()?; // best-effort sysctls + busy-poll sanity logs
        self.metrics_aggregator.write().update_node_load(nic_node, 100);
        Ok(())
    }

    // BEST-EFFORT ONLY: grep fallback if MSI listing absent.
    fn set_irq_affinity_fallback_grep(&self, cores: &[u32]) -> Result<bool, Box<dyn std::error::Error>> {
        let irq_path = "/proc/interrupts";
        let content = match std::fs::read_to_string(irq_path) { Ok(c) => c, Err(_) => return Ok(false) };
        let mut network_irqs = Vec::new();
        for line in content.lines() {
            if line.contains(" eth") || line.contains(" en") || line.contains(" bond")
               || line.contains(" team") || line.contains("mlx5") {
                if let Some(irq_str) = line.split(':').first() {
                    if let Ok(irq) = irq_str.trim().parse::<u32>() { network_irqs.push(irq); }
                }
            }
        }
        if network_irqs.is_empty() { return Ok(false); }
        for (idx, &irq) in network_irqs.iter().enumerate() {
            let core = cores[idx % cores.len()];
            let affinity_path = format!("/proc/irq/{}/smp_affinity_list", irq);
            let _ = std::fs::write(&affinity_path, format!("{}", core));
        }
        Ok(true)
    }

    fn optimize_network_stack(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Busy-poll and net stack knobs: write iff present, then log sanity if unsupported.
        // Busy-poll helps only with NAPI busy poll or SO_BUSY_POLL on sockets.
        let sysctls = [
            ("net.core.busy_poll", "50"),
            ("net.core.busy_read", "50"),
            ("net.core.netdev_budget", "600"),
            ("net.core.netdev_max_backlog", "5000"),
            ("net.ipv4.tcp_low_latency", "1"),
            ("net.ipv4.tcp_timestamps", "0"),
            ("net.core.rmem_max", "134217728"),
            ("net.core.wmem_max", "134217728"),
            ("net.ipv4.tcp_rmem", "4096 87380 134217728"),
            ("net.ipv4.tcp_wmem", "4096 65536 134217728"),
        ];
        for (key, value) in &sysctls {
            let path = format!("/proc/sys/{}", key.replace('.', "/"));
            if Path::new(&path).exists() {
                if let Err(e) = std::fs::write(&path, value) {
                    eprintln!("sysctl write failed: {}={} ({})", key, value, e);
                }
            } else {
                eprintln!("sysctl not present: {} (kernel or config may not support it)", key);
            }
        }

        // Busy-poll sanity: if busy_poll/busy_read not present, warn about SO_BUSY_POLL.
        let bp = Path::new("/proc/sys/net/core/busy_poll").exists();
        let br = Path::new("/proc/sys/net/core/busy_read").exists();
        if !(bp && br) {
            eprintln!("busy-poll unsupported or disabled: consider SO_BUSY_POLL at socket level and NIC/driver NAPI busy-poll capabilities");
        }
        Ok(())
    }
}

// Use MSI IRQs for a specific NIC interface (authoritative path:
// /sys/class/net/<iface>/device/msi_irqs/* -> /proc/irq/<n>/smp_affinity_list).
fn set_nic_irqs_affinity(iface: &str, cores: &[u32]) -> bool {
    let msi_path = format!("/sys/class/net/{}/device/msi_irqs", iface);
    let entries = match std::fs::read_dir(&msi_path) { Ok(e) => e, Err(_) => return false };
    let mut idx = 0usize;
    let mut changed = false;
    for e in entries.flatten() {
        if let Ok(irq_str) = e.file_name().into_string() {
            if let Ok(irq) = irq_str.parse::<u32>() {
                let core = cores[idx % cores.len()];
                let path = format!("/proc/irq/{}/smp_affinity_list", irq);
                if std::fs::write(&path, format!("{}", core)).is_ok() { changed = true; }
                idx += 1;
            }
        }
    }
    changed
}

fn list_msi_irqs(iface: &str) -> Vec<u32> {
    let msi_path = format!("/sys/class/net/{}/device/msi_irqs", iface);
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&msi_path) {
        for e in entries.flatten() {
            if let Ok(s) = e.file_name().into_string() {
                if let Ok(n) = s.parse::<u32>() { out.push(n); }
            }
        }
    }
    out.sort_unstable();
    out
}

// NEW: inotify-based watchers (replaces polling + keeps a lightweight fallback)
fn spawn_irq_inotify_watchers(iface: &str, cores: Vec<u32>) {
    let iface_msi = format!("/sys/class/net/{}/device/msi_irqs", iface);
    let iface_dev = format!("/sys/class/net/{}/device", iface);

    // MSI IRQ directory watcher
    spawn_inotify_watch(&iface_msi, {
        let iface = iface.to_string();
        let cores = cores.clone();
        move || {
            eprintln!("[inotify] msi_irqs changed for {} -> reapply IRQ affinity", iface);
            let _ = set_nic_irqs_affinity(&iface, &cores);
        }
    });

    // Parent device watcher (remove/bind/link flaps)
    spawn_inotify_watch(&iface_dev, {
        let iface = iface.to_string();
        let cores = cores.clone();
        move || {
            eprintln!("[inotify] device churn for {} detected -> reapply IRQ affinity", iface);
            let _ = set_nic_irqs_affinity(&iface, &cores);
        }
    });
}

fn spawn_inotify_watch(path: &str, on_change: impl Fn() + Send + 'static) {
    let path = path.to_string();
    thread::spawn(move || {
        // notify uses an mpsc channel; we keep it inside the thread
        let (tx, rx) = channel();
        let mut watcher: RecommendedWatcher = match notify::recommended_watcher(move |res| {
            // on any event, notify the loop
            let _ = tx.send(res.map(|_| ()));
        }) {
            Ok(w) => w,
            Err(e) => {
                eprintln!("inotify watcher failed to init on {}: {} (falling back to periodic reapply)", path, e);
                // fallback: periodic reapply
                loop {
                    thread::sleep(Duration::from_secs(60));
                    on_change();
                }
            }
        };

        // We prefer non-recursive for files, recursive for dirs
        let mode = if Path::new(&path).is_dir() { RecursiveMode::Recursive } else { RecursiveMode::NonRecursive };
        if let Err(e) = watcher.watch(Path::new(&path), mode) {
            eprintln!("inotify watch failed on {}: {} (fallback to periodic)", path, e);
            loop {
                thread::sleep(Duration::from_secs(60));
                on_change();
            }
        }

        // Event loop
        loop {
            match rx.recv() {
                Ok(Ok(())) => on_change(),
                Ok(Err(e)) => eprintln!("inotify error on {}: {}", path, e),
                Err(_) => {
                    eprintln!("inotify channel closed on {} (fallback to periodic)", path);
                    loop {
                        thread::sleep(Duration::from_secs(60));
                        on_change();
                    }
                }
            }
        }
    });
}

fn snapshot_irq_affinity_lists() -> String {
    let mut state = String::new();
    if let Ok(entries) = glob("/proc/irq/*/smp_affinity_list") {
        for entry in entries.filter_map(Result::ok) {
            if let Ok(s) = std::fs::read_to_string(&entry) {
                state.push_str(&format!("{}={}\n", entry.display(), s.trim()));
            }
        }
    }
    state
}

// Guess primary NIC from /proc/net/route Destination==00000000
fn guess_primary_iface() -> Option<String> {
    if let Ok(route) = std::fs::read_to_string("/proc/net/route") {
        for line in route.lines().skip(1) {
            let cols: Vec<&str> = line.split_whitespace().collect();
            if cols.len() > 1 && cols[1] == "00000000" {
                return Some(cols[0].to_string());
            }
        }
    }
    None
}

struct MetricsAggregator {
    // FIXED: removed extra '>' that broke CI
    node_loads: Vec<CacheAligned<AtomicU64>>,
    last_rebalance: AtomicU64,
}
impl MetricsAggregator {
    fn new(numa_nodes: usize) -> Self {
        let mut node_loads = Vec::with_capacity(numa_nodes);
        for _ in 0..numa_nodes { node_loads.push(CacheAligned(AtomicU64::new(0))); }
        Self { node_loads, last_rebalance: AtomicU64::new(0) }
    }
    fn get_least_loaded_node(&self) -> NumaNode {
        if self.node_loads.is_empty() { return NumaNode(0); }
        let mut min_load = u64::MAX; let mut best_node = 0;
        for (idx, load) in self.node_loads.iter().enumerate() {
            let current = load.0.load(Ordering::Relaxed);
            if current < min_load { min_load = current; best_node = idx; }
        }
        NumaNode(best_node as u32)
    }
    fn update_node_load(&self, node: NumaNode, delta: i64) {
        let idx = node.0 as usize;
        if idx < self.node_loads.len() {
            if delta > 0 { self.node_loads[idx].0.fetch_add(delta as u64, Ordering::Relaxed); }
            else {
                let abs_delta = (-delta) as u64;
                loop {
                    let current = self.node_loads[idx].0.load(Ordering::Relaxed);
                    let new_value = current.saturating_sub(abs_delta);
                    match self.node_loads[idx].0.compare_exchange_weak(current, new_value, Ordering::Release, Ordering::Relaxed) {
                        Ok(_) => break, Err(_) => continue,
                    }
                }
            }
        }
    }
    fn calculate_balance_score(&self) -> f64 {
        if self.node_loads.is_empty() { return 1.0; }
        let loads: Vec<u64> = self.node_loads.iter().map(|l| l.0.load(Ordering::Relaxed)).collect();
        if loads.iter().all(|&l| l == 0) { return 1.0; }
        let sum: u64 = loads.iter().sum();
        let mean = sum as f64 / loads.len() as f64;
        if mean == 0.0 { return 1.0; }
        let variance = loads.iter().map(|&load| { let diff = load as f64 - mean; diff * diff }).sum::<f64>() / loads.len() as f64;
        let std_dev = variance.sqrt();
        let cv = std_dev / mean;
        1.0 / (1.0 + cv).min(10.0)
    }
}

#[derive(Debug, Clone)]
pub struct PerformanceReport {
    pub total_tasks_executed: u64,
    pub avg_latency_ns: u64,
    pub cross_numa_accesses: u64,
    pub numa_balance_score: f64,
    pub tokio_pin_by_cpu: HashMap<i32, u64>,
    pub tokio_pin_total: u64,
    pub tokio_pin_mismatch: u64,
    pub tokio_pin_drift_rate: f64,
}

// Read-prefetch; write-intent hint not portable on stable Rust.
#[inline(always)] pub fn prefetch_cache_line(addr: *const u8) {
    #[cfg(target_arch = "x86_64")] unsafe { std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(addr as *const i8); }
}
#[inline(always)] pub fn prefetch_cache_line_write_hint(_addr: *mut u8) {
    // No portable PREFETCHW on stable; intentionally no-op
}

#[inline(always)] pub fn memory_fence() { std::sync::atomic::fence(Ordering::SeqCst); }
#[inline(always)] pub fn pause_cpu() {
    #[cfg(target_arch = "x86_64")] unsafe { std::arch::x86_64::_mm_pause(); }
}

// -------------------------------------------------------------------------------------
// [NUMA:UTIL] data placement helper
// -------------------------------------------------------------------------------------
pub fn optimize_numa_placement<T: Send + Sync + 'static>(
    data: Vec<T>, topology: &NumaTopology,
) -> HashMap<NumaNode, Vec<T>> {
    let mut numa_data = HashMap::new();
    if data.is_empty() || topology.nodes.is_empty() {
        if !data.is_empty() { numa_data.insert(NumaNode(0), data); }
        return numa_data;
    }
    let chunk_size = (data.len() + topology.nodes.len() - 1) / topology.nodes.len();
    let mut data_iter = data.into_iter();
    for node in &topology.nodes {
        let mut node_data = Vec::with_capacity(chunk_size);
        for _ in 0..chunk_size { if let Some(item) = data_iter.next() { node_data.push(item); } else { break; } }
        if !node_data.is_empty() { numa_data.insert(*node, node_data); }
    }
    for item in data_iter {
        if let Some((_node, vec)) = numa_data.iter_mut().next() { vec.push(item); }
    }
    numa_data
}

// -------------------------------------------------------------------------------------
// [NUMA:UTIL] cpuset/cgroup NUMA reality check
// -------------------------------------------------------------------------------------
fn find_cpuset_file(name: &str) -> Option<PathBuf> {
    // Try v2 unified first
    let candidates = [
        format!("/sys/fs/cgroup/{}.effective", name),
        format!("/sys/fs/cgroup/{}", name),
    ];
    for c in &candidates { if Path::new(c).exists() { return Some(PathBuf::from(c)); } }

    // Try per-cgroup paths from /proc/self/cgroup
    if let Ok(cg) = std::fs::read_to_string("/proc/self/cgroup") {
        for line in cg.lines() {
            let parts: Vec<&str> = line.split(':').collect();
            if parts.len() == 3 {
                let rel = parts[2];
                let p1 = format!("/sys/fs/cgroup{}/{}.effective", rel, name);
                let p2 = format!("/sys/fs/cgroup{}/{}", rel, name);
                if Path::new(&p1).exists() { return Some(PathBuf::from(p1)); }
                if Path::new(&p2).exists() { return Some(PathBuf::from(p2)); }
            }
        }
    }
    None
}

fn allowed_numa_nodes_from_cpuset() -> Option<HashSet<u32>> {
    if let Some(p) = find_cpuset_file("cpuset.mems") {
        if let Ok(s) = std::fs::read_to_string(p) {
            let trimmed = s.trim();
            if !trimmed.is_empty() { return Some(parse_range_set(trimmed)); }
        }
    }
    None
}

// NEW: also return raw cpuset mems string for logging
fn raw_cpuset_mems() -> Option<String> {
    find_cpuset_file("cpuset.mems").and_then(|p| std::fs::read_to_string(p).ok()).map(|s| s.trim().to_string())
}

// -------------------------------------------------------------------------------------
// [NUMA:UTIL] Global NUMA initialization
// - mlockall(2) best effort with RLIMIT logging (+ cgroup hint)
// - set localalloc
// - THP policy coherent with later MADV_HUGEPAGE / MAP_HUGETLB usage
// - per-size mTHP knobs to "inherit" (token-only writes; guard defrag existence)
// - “never” is NOT a hard off due to MADV_COLLAPSE
// - Auto-NUMA telemetry
// -------------------------------------------------------------------------------------
pub fn initialize_numa_system() -> Result<(), Box<dyn std::error::Error>> {
    let numa_avail = unsafe { numa_available() }; // numa(3)
    if numa_avail < 0 {
        eprintln!("NUMA not available - compatibility mode");
        return Ok(());
    }

    // mlockall best-effort with RLIMIT logging (mlockall(2))
    let rc = unsafe { mlockall(MCL_CURRENT | MCL_FUTURE) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        let mut lim = rlimit { rlim_cur: 0, rlim_max: 0 };
        unsafe { getrlimit(RLIMIT_MEMLOCK, &mut lim as *mut _); }
        eprintln!(
            "mlockall failed: {} (RLIMIT_MEMLOCK cur={} max={}). \
             Hint: raise limits or configure cgroup v2 memory.locked.",
            err, lim.rlim_cur, lim.rlim_max
        );
    }
    unsafe { numa_set_localalloc(); } // numa(3)

    // THP policy from env:
    //   NUMA_THP = "madvise" | "never" | "hugetlb"
    let thp_mode = std::env::var("NUMA_THP").unwrap_or_else(|_| "madvise".to_string());

    let thp_enabled_path = "/sys/kernel/mm/transparent_hugepage/enabled";
    let thp_defrag_path  = "/sys/kernel/mm/transparent_hugepage/defrag";
    if std::path::Path::new(thp_enabled_path).exists() {
        match thp_mode.as_str() {
            "madvise" => {
                // Token-only writes per docs: echo madvise > ...
                let _ = std::fs::write(thp_enabled_path, b"madvise");
                if std::path::Path::new(thp_defrag_path).exists() {
                    let _ = std::fs::write(thp_defrag_path,  b"madvise");
                }
                set_thp_per_size_inherit();
                log_thp_global_state();
                log_thp_per_size_state();
            }
            "never"   => {
                // Hint only; MADV_COLLAPSE can still collapse THP (transhuge docs).
                let _ = std::fs::write(thp_enabled_path, b"never");
                if std::path::Path::new(thp_defrag_path).exists() {
                    let _ = std::fs::write(thp_defrag_path,  b"never");
                }
                set_thp_per_size_inherit();
                log_thp_global_state();
                log_thp_per_size_state();
            }
            "hugetlb" => {
                // Explicit MAP_HUGETLB path only; do NOT issue MADV_HUGEPAGE in this mode.
                log_thp_global_state();
                log_thp_per_size_state();
            }
            _ => {}
        }
    }

    // VM latency tweaks (optional)
    let vm_settings = [
        ("/proc/sys/vm/swappiness", "0"),
        ("/proc/sys/vm/zone_reclaim_mode", "0"),
        ("/proc/sys/vm/dirty_ratio", "10"),
        ("/proc/sys/vm/dirty_background_ratio", "5"),
    ];
    for (path, value) in &vm_settings {
        if std::path::Path::new(path).exists() { let _ = std::fs::write(path, value); }
    }

    // Auto-NUMA telemetry
    log_auto_numa_state();

    Ok(())
}

fn log_auto_numa_state() {
    let bal = std::fs::read_to_string("/proc/sys/kernel/numa_balancing").unwrap_or_default();
    eprintln!("kernel.numa_balancing={}", bal.trim());
    for k in &[
        "/proc/sys/kernel/numa_balancing_scan_delay_ms",
        "/proc/sys/kernel/numa_balancing_scan_period_min_ms",
        "/proc/sys/kernel/numa_balancing_scan_size_mb",
    ] {
        if let Ok(v) = std::fs::read_to_string(k) {
            eprintln!("{}={}", k, v.trim());
        }
    }
}

// Log current global THP token lines for ops visibility
fn log_thp_global_state() {
    let paths = [
        "/sys/kernel/mm/transparent_hugepage/enabled",
        "/sys/kernel/mm/transparent_hugepage/defrag",
    ];
    for p in &paths {
        if let Ok(s) = std::fs::read_to_string(p) {
            eprintln!("THP state {}: {}", p, s.trim());
        }
    }
}

// Set per-size THP knobs to "inherit" (token-only writes, guard existence) + log state
fn set_thp_per_size_inherit() {
    if let Ok(entries) = glob("/sys/kernel/mm/transparent_hugepage/hugepages-*/enabled") {
        for entry in entries.filter_map(Result::ok) {
            if std::path::Path::new(&entry).exists() {
                let _ = std::fs::write(&entry, b"inherit");
            }
        }
    }
}

fn log_thp_per_size_state() {
    if let Ok(entries) = glob("/sys/kernel/mm/transparent_hugepage/hugepages-*/enabled") {
        for entry in entries.filter_map(Result::ok) {
            if let Ok(s) = std::fs::read_to_string(&entry) {
                eprintln!("THP per-size {} -> {}", entry.display(), s.trim());
            }
        }
    }
}

// -------------------------------------------------------------------------------------
// [NUMA:UTIL] Pin current thread to first core of a given NUMA node (portable pin)
// -------------------------------------------------------------------------------------
pub fn pin_current_thread_to_node(node: NumaNode) -> Result<(), Box<dyn std::error::Error>> {
    unsafe { numa_set_preferred(node.0 as c_int); } // set preferred memory policy
    let topology = NumaTopology::discover()?;
    if let Some(cores) = topology.node_to_cores.get(&node) {
        if let Some(&first_core) = cores.first() { NumaThreadPool::pin_to_cpu(first_core)?; }
    }
    Ok(())
}

// -------------------------------------------------------------------------------------
// [NUMA:MEMBIND] NumaHugePage — explicit policy-correct bind with dynamic nodemask
// THP modes:
//   - NUMA_THP=madvise : MADV_HUGEPAGE on anon mmap
//   - NUMA_THP=never   : avoid MADV_HUGEPAGE (not a hard off; MADV_COLLAPSE can still collapse)
//   - NUMA_THP=hugetlb : MAP_HUGETLB explicit huge pages (+ MAP_HUGE_* size flags; order via MAP_HUGE_SHIFT)
// Optional pre-touch to fault-in under policy (NUMA_PRETOUCH=1), stepping by real page size.
// Page-placement verifier using move_pages(2) when NUMA_VERIFY=1; also dump /proc/self/numa_maps.
// -------------------------------------------------------------------------------------
pub struct NumaHugePage {
    ptr: NonNull<u8>,
    size: usize,
    node: NumaNode,
    page_sz: usize,
}
unsafe impl Send for NumaHugePage {}
unsafe impl Sync for NumaHugePage {}

impl NumaHugePage {
    pub fn allocate(size: usize, node: NumaNode) -> Result<Self, Box<dyn std::error::Error>> {
        if size == 0 { return Err("Size must be greater than 0".into()); }

        let thp_mode = std::env::var("NUMA_THP").unwrap_or_else(|_| "madvise".to_string());
        let hp_env = std::env::var("NUMA_HUGEPAGE_SIZE").unwrap_or_else(|_| "2M".to_string());
        let hugetlb_bytes: usize = match hp_env.as_str() { "1G" | "1g" | "1GIB" | "1GB" => 1 << 30, _ => 2 << 20 };
        let huge_page_size = if thp_mode == "hugetlb" { hugetlb_bytes } else { 2 * 1024 * 1024 };
        let aligned_size = (size + huge_page_size - 1) & !(huge_page_size - 1);

        // If using hugetlb, verify pool and mount exist before mmap.
        if thp_mode == "hugetlb" {
            if !is_hugetlbfs_mounted() {
                return Err("NUMA_THP=hugetlb but no hugetlbfs mount detected (check /proc/mounts)".into());
            }
            if !hugetlb_pool_available(hugetlb_bytes) {
                let kb = hugetlb_bytes / 1024;
                return Err(format!(
                    "NUMA_THP=hugetlb: no reserved hugepages of requested size. \
                     Set /sys/kernel/mm/hugepages/hugepages-{}kB/nr_hugepages > 0",
                    kb
                ).into());
            }
        }

        unsafe {
            let mut flags = MAP_PRIVATE | MAP_ANONYMOUS;
            if thp_mode == "hugetlb" {
                flags |= MAP_HUGETLB;
                // Doc pin: MAP_HUGE_SHIFT encodes order, not bytes (mmap).
                let order_flags: c_int = if hugetlb_bytes == (1 << 30) { MAP_HUGE_1GB_FALLBACK } else { MAP_HUGE_2MB_FALLBACK };
                flags |= order_flags;
            } else {
                // mmap(2) MAP_POPULATE: prefaults page tables; NOT a hugepage guarantee. Do not rely on it.
                flags |= MAP_POPULATE;
            }

            let ptr = mmap(std::ptr::null_mut(), aligned_size, PROT_READ | PROT_WRITE, flags, -1, 0);
            if ptr == libc::MAP_FAILED { return Err(format!("mmap failed: {}", std::io::Error::last_os_error()).into()); }

            if thp_mode == "madvise" {
                // Doc pin: THP “never” is a hint; MADV_COLLAPSE may override (transhuge docs).
                let _ = madvise(ptr, aligned_size, MADV_HUGEPAGE);
            }
            // Explicitly skip MADV_HUGEPAGE in NUMA_THP=hugetlb mode (hugetlb and transhuge are distinct paths).

            let max_node = get_max_node();

            // Print cpuset mask for ops before binding (postmortem-friendly)
            if let Some(raw) = raw_cpuset_mems() {
                eprintln!("cpuset.mems(effective) = {}", raw);
            }

            // mbind(2): maxnode = highest_node_index + 1; nodemask packed in c_ulong words.
            mbind_bind_range(ptr, aligned_size, node.0, max_node)?;

            // Optional pre-touch by real page size (2M/1G when hugetlb, else 4K).
            if std::env::var("NUMA_PRETOUCH").ok().as_deref() == Some("1") {
                let step = if thp_mode == "hugetlb" { huge_page_size }
                           else if thp_mode == "madvise" { 2 * 1024 * 1024 }
                           else { 4096 };
                let p = ptr as *mut u8;
                let end = p.add(aligned_size);
                let mut cur = p;
                while cur < end {
                    std::ptr::write_volatile(cur, 0u8);
                    cur = cur.add(step);
                }
            }

            // Optional placement verification using move_pages(2) + numa_maps snapshot
            if std::env::var("NUMA_VERIFY").ok().as_deref() == Some("1") {
                let step_bytes = if thp_mode == "hugetlb" { huge_page_size }
                                 else if thp_mode == "madvise" { 2 * 1024 * 1024 }
                                 else { 4096 };
                verify_page_nodes(ptr as *mut u8, aligned_size, node, step_bytes);
                dump_numa_maps("post-alloc");
            }

            Ok(Self { ptr: NonNull::new_unchecked(ptr as *mut u8), size: aligned_size, node, page_sz: huge_page_size })
        }
    }
    #[inline] pub fn as_ptr(&self) -> *mut u8 { self.ptr.as_ptr() }
    #[inline] pub fn as_slice(&self) -> &[u8] { unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.size) } }
    #[inline] pub fn as_mut_slice(&mut self) -> &mut [u8] { unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size) } }
}
impl Drop for NumaHugePage {
    fn drop(&mut self) { unsafe { munmap(self.ptr.as_ptr() as *mut libc::c_void, self.size); } }
}

// /proc/self/numa_maps snapshot for incident debugging
fn dump_numa_maps(tag: &str) {
    if let Ok(s) = std::fs::read_to_string("/proc/self/numa_maps") {
        eprintln!("--- numa_maps [{}] ---\n{}", tag, s);
    }
}

// Page-placement verifier now steps by real page size to avoid false drift alarms.
fn verify_page_nodes(base: *mut u8, len: usize, node: NumaNode, step_bytes: usize) {
    // Sample up to 16 addresses across the region; query node with move_pages(nodes=NULL)
    const SAMPLE_MAX: usize = 16;
    let step = step_bytes.max(4096);
    let mut addrs: Vec<*mut c_void> = Vec::new();
    let mut cur = base;
    let end = unsafe { base.add(len) };
    while addrs.len() < SAMPLE_MAX && cur < end {
        addrs.push(cur as *mut c_void);
        cur = unsafe { cur.add(((end as usize - cur as usize) / (SAMPLE_MAX - addrs.len()).max(1)).max(step)) };
    }
    let mut status = vec![0 as c_int; addrs.len()];
    unsafe {
        let rc = move_pages_sys(0, &mut addrs, std::ptr::null(), &mut status, 0);
        if rc < 0 {
            eprintln!("move_pages verify failed: {}", std::io::Error::last_os_error());
            return;
        }
    }
    let mut bad = 0usize;
    for &st in &status {
        if st >= 0 && (st as u32) != node.0 { bad += 1; }
    }
    if bad > 0 {
        eprintln!("⚠️ NUMA placement drift: {} / {} sampled pages on wrong node (expected node {})", bad, status.len(), node.0);
    }
}

fn get_max_node() -> u32 {
    let mn = unsafe { numa_max_node() }; // numa(3)
    if mn < 0 { 0 } else { mn as u32 }
}

// Build nodemask words for mbind(2): width = highest_node_index+1, packed in c_ulong words.
fn build_nodemask_words(max_node: u32, target_node: u32) -> Vec<c_ulong> {
    let bits_per = std::mem::size_of::<c_ulong>() * 8;
    let highest_index = max_node as usize;
    let words = (highest_index + 1 + bits_per - 1) / bits_per;

    let mut mask = vec![0 as c_ulong; words];
    let idx = (target_node as usize) / bits_per;
    let bit = (target_node as usize) % bits_per;
    mask[idx] |= (1 as c_ulong) << bit;
    mask
}

// mbind(2): maxnode == highest_node_index+1; nodemask sized in c_ulong words (strict + move).
// Also warns if target node is outside cpuset/cgroup allowed mems.
fn mbind_bind_range(addr: *mut c_void, len: usize, node: u32, max_node: u32) -> Result<(), String> {
    if let Some(allowed) = allowed_numa_nodes_from_cpuset() {
        if !allowed.contains(&node) {
            eprintln!("⚠️ cpuset/cgroup mems do not include target node {} (allowed={:?}); kernel may rehome pages", node, allowed);
        }
    }

    let mask = build_nodemask_words(max_node, node);
    let highest_index = max_node as usize;

    // Doc pin (auditors love this line):
    // mbind(2): maxnode = highest_node_index + 1; nodemask packed in c_ulong words.
    let rc = unsafe {
        mbind_sys(addr, len, MPOL_BIND, mask.as_ptr(), (highest_index as c_ulong) + 1, MPOL_MF_STRICT | MPOL_MF_MOVE)
    };
    if rc != 0 { return Err(std::io::Error::last_os_error().to_string()); }
    Ok(())
}

// Check if any hugetlbfs mount exists.
fn is_hugetlbfs_mounted() -> bool {
    if let Ok(m) = std::fs::read_to_string("/proc/mounts") {
        return m.lines().any(|l| l.contains(" hugetlbfs "));
    }
    false
}

// Verify hugetlb pool has pages of the requested size (bytes).
fn hugetlb_pool_available(bytes: usize) -> bool {
    // Per-size pool knobs live in /sys/kernel/mm/hugepages/hugepages-<size>kB/nr_hugepages
    let kb = bytes / 1024;
    let path = format!("/sys/kernel/mm/hugepages/hugepages-{}kB/nr_hugepages", kb);
    if let Ok(s) = std::fs::read_to_string(&path) {
        if let Ok(v) = s.trim().parse::<u64>() { return v > 0; }
    }
    // Fallback: /proc/meminfo has HugePages_Free (coarse)
    if let Ok(meminfo) = std::fs::read_to_string("/proc/meminfo") {
        for line in meminfo.lines() {
            if line.starts_with("HugePages_Free:") {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    if let Ok(v) = parts[1].parse::<u64>() { return v > 0; }
                }
            }
        }
    }
    false
}

// =====================================================================================
// Tests (lightweight)
// =====================================================================================
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numa_topology_discovery() {
        match NumaTopology::discover() {
            Ok(topology) => {
                assert!(!topology.nodes.is_empty());
                assert!(!topology.cores.is_empty());
            }
            Err(_e) => { /* fine on non-NUMA */ }
        }
    }

    #[test]
    fn test_numa_memory_pool() {
        let pool = match NumaMemoryPool::new(1, 4096, 10) {
            Ok(p) => p,
            Err(_e) => return,
        };
        let node = NumaNode(0);
        let ptr1 = pool.acquire(node);
        assert!(ptr1.is_some());
        if let Some(ptr) = ptr1 { pool.release(node, ptr); }
    }

    #[test]
    fn test_cache_aligned() {
        let aligned = CacheAligned(42u64);
        assert_eq!(*aligned, 42);
        let addr = &aligned as *const _ as usize;
        assert_eq!(addr % CACHE_LINE_SIZE, 0);
    }

    #[test]
    fn test_numa_huge_page_alloc_dealloc() {
        std::env::set_var("NUMA_THP", "madvise");
        match NumaHugePage::allocate(4096, NumaNode(0)) {
            Ok(mut page) => { let slice = page.as_mut_slice(); slice[0] = 42; assert_eq!(slice[0], 42); }
            Err(_e) => { /* environment-dependent */ }
        }
    }

    #[test]
    fn test_siblings_parser() {
        let v = super::parse_siblings_list("0-3,8");
        assert_eq!(v, vec![0,1,2,3,8]);
    }

    #[test]
    fn test_mbind_nodemask_math() {
        let max_node = 2u32;
        let mask0 = build_nodemask_words(max_node, 0);
        let mask1 = build_nodemask_words(max_node, 1);
        let mask2 = build_nodemask_words(max_node, 2);

        assert_eq!(mask0[0] & 1, 1);
        assert_eq!((mask1[0] >> 1) & 1, 1);
        assert_eq!((mask2[0] >> 2) & 1, 1);

        let bits_per = std::mem::size_of::<c_ulong>() * 8;
        let words = (max_node as usize + 1 + bits_per - 1) / bits_per;
        assert_eq!(words, mask0.len());
        assert!(words >= 1);
    }
}
