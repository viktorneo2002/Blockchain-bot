use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, AtomicUsize, AtomicU32, Ordering}};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::pin::Pin;
use std::alloc::{alloc, dealloc, Layout};
use std::ptr::NonNull;
use std::ffi::{CString, CStr};
use std::os::raw::{c_int, c_void, c_ulong, c_char};
use crossbeam::channel::{bounded, unbounded, Sender, Receiver};
use crossbeam::queue::{ArrayQueue, SegQueue};
use parking_lot::{Mutex, RwLock};
use libc::{
    cpu_set_t, CPU_SET, CPU_ZERO, sched_setaffinity, SCHED_FIFO, sched_param,
    sched_setscheduler, sched_getcpu, mmap, munmap, PROT_READ, PROT_WRITE,
    MAP_PRIVATE, MAP_ANONYMOUS, MAP_POPULATE, MAP_LOCKED, MADV_HUGEPAGE,
    madvise, mlockall, MCL_CURRENT, MCL_FUTURE
};
use tokio::runtime::{Runtime, Builder as RuntimeBuilder};
use once_cell::sync::Lazy;
use glob::glob;

// FFI bindings for libnuma
#[link(name = "numa")]
extern "C" {
    fn numa_available() -> c_int;
    fn numa_max_node() -> c_int;
    fn numa_node_of_cpu(cpu: c_int) -> c_int;
    fn numa_alloc_onnode(size: usize, node: c_int) -> *mut c_void;
    fn numa_alloc_local(size: usize) -> *mut c_void;
    fn numa_free(ptr: *mut c_void, size: usize);
    fn numa_set_preferred(node: c_int);
    fn numa_set_localalloc();
    fn numa_set_membind(nodemask: *const c_ulong);
    fn numa_get_membind(nodemask: *mut c_ulong);
    fn mbind(addr: *mut c_void, len: c_ulong, mode: c_int, 
             nodemask: *const c_ulong, maxnode: c_ulong, flags: c_int) -> c_int;
    fn set_mempolicy(mode: c_int, nodemask: *const c_ulong, maxnode: c_ulong) -> c_int;
}

const MPOL_BIND: c_int = 2;
const MPOL_INTERLEAVE: c_int = 3;
const MPOL_LOCAL: c_int = 4;
const MPOL_MF_STRICT: c_int = 1;
const MPOL_MF_MOVE: c_int = 2;

const MAX_NUMA_NODES: usize = 8;
const CACHE_LINE_SIZE: usize = 64;
const THREAD_STACK_SIZE: usize = 8 * 1024 * 1024;
const SPIN_ITERATIONS: u32 = 1000;
const YIELD_ITERATIONS: u32 = 100;
const MEMORY_POOL_BLOCK_SIZE: usize = 65536;
const QUEUE_SIZE_PER_NUMA: usize = 65536;
const GLOBAL_QUEUE_SIZE: usize = 131072;

#[repr(C, align(64))]
pub struct CacheAligned<T>(T);

impl<T> std::ops::Deref for CacheAligned<T> {
    type Target = T;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> std::ops::DerefMut for CacheAligned<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

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
        // Check if NUMA is available
        let numa_avail = unsafe { numa_available() };
        if numa_avail < 0 {
            // Fallback to single node system
            return Self::single_node_fallback();
        }

        let max_node = unsafe { numa_max_node() };
        if max_node < 0 {
            return Self::single_node_fallback();
        }

        let mut nodes = Vec::new();
        let mut cores = Vec::new();
        let mut node_to_cores = HashMap::new();
        let mut core_to_node = HashMap::new();

        // Discover NUMA nodes
        for node_id in 0..=(max_node as u32).min(MAX_NUMA_NODES as u32 - 1) {
            nodes.push(NumaNode(node_id));
            node_to_cores.insert(NumaNode(node_id), Vec::new());
        }

        // Discover CPU cores
        let num_cpus = num_cpus::get() as u32;
        for cpu_id in 0..num_cpus {
            let node_id = unsafe { numa_node_of_cpu(cpu_id as c_int) };
            let node_id = if node_id < 0 || node_id > max_node {
                0
            } else {
                node_id as u32
            };
            
            let numa_node = NumaNode(node_id);
            let sibling_id = Self::get_hyperthread_sibling(cpu_id).ok();
            
            cores.push(CpuCore {
                id: cpu_id,
                numa_node,
                sibling_id,
            });
            
            node_to_cores.entry(numa_node)
                .or_insert_with(Vec::new)
                .push(cpu_id);
            core_to_node.insert(cpu_id, numa_node);
        }

        if nodes.is_empty() {
            return Self::single_node_fallback();
        }

        Ok(NumaTopology {
            nodes,
            cores,
            node_to_cores,
            core_to_node,
        })
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
            cores.push(CpuCore {
                id: cpu_id,
                numa_node: node,
                sibling_id,
            });
            node_cores.push(cpu_id);
            core_to_node.insert(cpu_id, node);
        }
        
        node_to_cores.insert(node, node_cores);
        
        Ok(NumaTopology {
            nodes: vec![node],
            cores,
            node_to_cores,
            core_to_node,
        })
    }

    fn get_hyperthread_sibling(cpu_id: u32) -> Result<u32, Box<dyn std::error::Error>> {
        let path = format!("/sys/devices/system/cpu/cpu{}/topology/thread_siblings_list", cpu_id);
        
        match std::fs::read_to_string(&path) {
            Ok(content) => {
                let siblings: Vec<u32> = content.trim()
                    .split(&[',', '-'][..])
                    .filter_map(|s| s.parse().ok())
                    .collect();
                    
                siblings.into_iter()
                    .find(|&id| id != cpu_id)
                    .ok_or_else(|| "No sibling found".into())
            }
            Err(_) => Err("Cannot read siblings info".into())
        }
    }
}

pub struct NumaAllocator {
    node: NumaNode,
}

impl NumaAllocator {
    pub fn new(node: NumaNode) -> Self {
        Self { node }
    }

    pub unsafe fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, ()> {
        if layout.size() == 0 {
            return Err(());
        }

        let ptr = numa_alloc_onnode(layout.size(), self.node.0 as c_int);
        if ptr.is_null() {
            // Fallback to system allocator
            let ptr = alloc(layout);
            if ptr.is_null() {
                return Err(());
            }
            return Ok(NonNull::new_unchecked(ptr));
        }

        Ok(NonNull::new_unchecked(ptr as *mut u8))
    }

    pub unsafe fn deallocate(&self, ptr: NonNull<u8>, layout: Layout) {
        if layout.size() > 0 {
            // Try NUMA free first
            numa_free(ptr.as_ptr() as *mut c_void, layout.size());
        }
    }
}

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
}

impl ThreadMetrics {
    fn new() -> Self {
        Self {
            tasks_executed: AtomicU64::new(0),
            task_latency_ns: AtomicU64::new(0),
            cross_numa_accesses: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            last_update: AtomicU64::new(0),
        }
    }
}

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
        if idx < self.queues.len() {
            self.queues[idx].push(task)
        } else if !self.queues.is_empty() {
            self.queues[0].push(task)
        } else {
            Err(task)
        }
    }

    fn pop(&self, node: NumaNode) -> Option<TaskBox> {
        let idx = node.0 as usize;
        if idx < self.queues.len() {
            self.queues[idx].pop()
        } else {
            None
        }
    }

    fn steal_from(&self, victim_node: NumaNode) -> Option<TaskBox> {
        let idx = victim_node.0 as usize;
        if idx < self.queues.len() {
            if let Some(task) = self.queues[idx].pop() {
                self.steal_counters[idx].fetch_add(1, Ordering::Relaxed);
                Some(task)
            } else {
                None
            }
        } else {
            None
        }
    }
}

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
            
            if cores.is_empty() {
                continue;
            }
            
            let threads_to_spawn = threads_per_node.min(cores.len());
            
                        for i in 0..threads_to_spawn {
                let cpu_id = cores[i % cores.len()];
                let worker = Self::spawn_worker(
                    thread_id,
                    cpu_id,
                    *numa_node,
                    topology.clone(),
                    work_queue.clone(),
                    global_queue.clone(),
                    shutdown.clone(),
                    thread_contexts.clone(),
                )?;
                
                workers.push(worker);
                thread_id += 1;
            }
        }
        
        if workers.is_empty() {
            return Err("Failed to spawn any worker threads".into());
        }
        
        Ok(Self {
            topology,
            workers,
            work_queue,
            global_queue,
            shutdown,
            thread_contexts,
        })
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
                // Pin to CPU
                if let Err(e) = Self::pin_to_cpu(cpu_id) {
                    eprintln!("Warning: Failed to pin thread to CPU {}: {}", cpu_id, e);
                }
                
                // Set realtime priority
                if let Err(e) = Self::set_realtime_priority() {
                    eprintln!("Warning: Failed to set realtime priority: {}", e);
                }
                
                // Create thread context
                let allocator = Arc::new(NumaAllocator::new(numa_node));
                let context = Arc::new(NumaThreadContext {
                    thread_id,
                    cpu_id,
                    numa_node,
                    allocator,
                    metrics: CacheAligned(ThreadMetrics::new()),
                });
                
                // Register context
                {
                    let mut contexts = thread_contexts.write();
                    contexts.push(context.clone());
                }
                
                // Run worker loop
                Self::worker_loop(
                    context,
                    topology,
                    work_queue,
                    global_queue,
                    shutdown_clone,
                );
            })?;
        
        Ok(NumaWorkerThread {
            handle: Some(handle),
            shutdown: shutdown_clone,
        })
    }

    fn pin_to_cpu(cpu_id: u32) -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            let mut set = std::mem::MaybeUninit::<cpu_set_t>::uninit();
            let set_ptr = set.as_mut_ptr();
            CPU_ZERO(set_ptr);
            CPU_SET(cpu_id as usize, set_ptr);
            
            let result = sched_setaffinity(
                0,
                std::mem::size_of::<cpu_set_t>(),
                set_ptr
            );
            
            if result != 0 {
                return Err(format!("sched_setaffinity failed: {}", std::io::Error::last_os_error()).into());
            }
        }
        Ok(())
    }

    fn set_realtime_priority() -> Result<(), Box<dyn std::error::Error>> {
        unsafe {
            let param = sched_param { 
                sched_priority: 50 
            };
            let result = sched_setscheduler(0, SCHED_FIFO, &param);
            
            if result != 0 {
                // Non-critical: fall back to normal priority
                return Ok(());
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
            
            // Try local queue first
            if let Some(mut task) = work_queue.pop(context.numa_node) {
                Self::execute_task(task, &context);
                found_work = true;
                spin_count = 0;
            }
            // Try global queue
            else if let Some(mut task) = global_queue.pop() {
                Self::execute_task(task, &context);
                found_work = true;
                spin_count = 0;
            }
            // Try work stealing
            else if last_steal_attempt.elapsed() > steal_interval {
                for other_node in &topology.nodes {
                    if *other_node != context.numa_node {
                        if let Some(mut task) = work_queue.steal_from(*other_node) {
                            context.metrics.0.cross_numa_accesses.fetch_add(1, Ordering::Relaxed);
                            Self::execute_task(task, &context);
                            found_work = true;
                            spin_count = 0;
                            break;
                        }
                    }
                }
                last_steal_attempt = Instant::now();
            }
            
            if !found_work {
                spin_count += 1;
                if spin_count < SPIN_ITERATIONS {
                    std::hint::spin_loop();
                } else if spin_count < SPIN_ITERATIONS + YIELD_ITERATIONS {
                    thread::yield_now();
                } else {
                    thread::park_timeout(Duration::from_micros(10));
                    spin_count = SPIN_ITERATIONS;
                }
            }
        }
    }

    fn execute_task(mut task: TaskBox, context: &Arc<NumaThreadContext>) {
        let start = Instant::now();
        
        match task.execute(context) {
            Ok(_) => {},
            Err(e) => {
                eprintln!("Task execution error on thread {}: {}", context.thread_id, e);
            }
        }
        
        let elapsed = start.elapsed().as_nanos() as u64;
        context.metrics.0.tasks_executed.fetch_add(1, Ordering::Relaxed);
        context.metrics.0.task_latency_ns.fetch_add(elapsed, Ordering::Relaxed);
        context.metrics.0.last_update.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
            Ordering::Relaxed
        );
    }

    pub fn submit(&self, task: TaskBox) -> Result<(), Box<dyn std::error::Error>> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err("Thread pool is shutting down".into());
        }
        
        if let Some(preferred_node) = task.preferred_numa_node() {
            if preferred_node.0 < self.topology.nodes.len() as u32 {
                match self.work_queue.push(task, preferred_node) {
                    Ok(()) => return Ok(()),
                    Err(rejected) => {
                        self.global_queue.push(rejected);
                        return Ok(());
                    }
                }
            }
        }
        
        self.global_queue.push(task);
        Ok(())
    }

    pub fn submit_batch(&self, tasks: Vec<TaskBox>) -> Result<(), Box<dyn std::error::Error>> {
        if self.shutdown.load(Ordering::Acquire) {
            return Err("Thread pool is shutting down".into());
        }
        
        for task in tasks {
            self.submit(task)?;
        }
        
        Ok(())
    }

    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        
        for worker in &self.workers {
            if let Some(handle) = &worker.handle {
                handle.thread().unpark();
            }
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
}

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
            
            if cores.is_empty() {
                continue;
            }
            
            let core_ids: Vec<usize> = cores.iter()
                .take(threads_per_node)
                .map(|&id| id as usize)
                .collect();
            
            let node_id = numa_node.0;
            let runtime = RuntimeBuilder::new_multi_thread()
                .worker_threads(core_ids.len())
                .thread_name(format!("tokio-numa-{}", node_id))
                .on_thread_start(move || {
                    if let Some(&cpu_id) = core_ids.first() {
                        let _ = NumaThreadPool::pin_to_cpu(cpu_id as u32);
                    }
                })
                .enable_all()
                .build()?;
            
            tokio_runtimes.insert(*numa_node, runtime);
        }
        
        if tokio_runtimes.is_empty() {
            return Err("Failed to create any Tokio runtimes".into());
        }
        
        Ok(Self {
            thread_pool,
            tokio_runtimes,
            topology,
        })
    }

    pub fn submit_task(&self, task: TaskBox) -> Result<(), Box<dyn std::error::Error>> {
        self.thread_pool.submit(task)
    }

    pub fn get_tokio_runtime(&self, node: NumaNode) -> Option<&Runtime> {
        self.tokio_runtimes.get(&node)
    }

    pub fn get_local_numa_node() -> Result<NumaNode, Box<dyn std::error::Error>> {
        let cpu_id = unsafe { sched_getcpu() };
        if cpu_id < 0 {
            return Err("Failed to get current CPU".into());
        }
        
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

pub struct NumaMemoryPool {
    pools: Vec<Arc<Mutex<Vec<NonNull<u8>>>>>,
    block_size: usize,
    blocks_per_node: usize,
}

unsafe impl Send for NumaMemoryPool {}
unsafe impl Sync for NumaMemoryPool {}

impl NumaMemoryPool {
    pub fn new(numa_nodes: usize, block_size: usize, blocks_per_node: usize) -> Result<Self, Box<dyn std::error::Error>> {
        if numa_nodes == 0 || block_size == 0 || blocks_per_node == 0 {
            return Err("Invalid parameters for memory pool".into());
        }
        
        let mut pools = Vec::with_capacity(numa_nodes);
        
        for node_id in 0..numa_nodes {
            let mut blocks = Vec::with_capacity(blocks_per_node);
            
            for _ in 0..blocks_per_node {
                let ptr = unsafe {
                    numa_alloc_onnode(block_size, node_id as c_int)
                };
                
                if ptr.is_null() {
                    // Fallback to regular allocation
                    let layout = Layout::from_size_align(block_size, 64)
                        .map_err(|_| "Invalid layout")?;
                    let ptr = unsafe { alloc(layout) };
                    if ptr.is_null() {
                        // Cleanup and fail
                        for block in &blocks {
                            unsafe { numa_free(block.as_ptr() as *mut c_void, block_size); }
                        }
                        return Err(format!("Failed to allocate memory for node {}", node_id).into());
                    }
                                        blocks.push(unsafe { NonNull::new_unchecked(ptr) });
                } else {
                    blocks.push(unsafe { NonNull::new_unchecked(ptr as *mut u8) });
                }
            }
            
            pools.push(Arc::new(Mutex::new(blocks)));
        }
        
        Ok(Self {
            pools,
            block_size,
            blocks_per_node,
        })
    }

    pub fn acquire(&self, node: NumaNode) -> Option<NonNull<u8>> {
        let pool_idx = node.0 as usize;
        if pool_idx < self.pools.len() {
            self.pools[pool_idx].lock().pop()
        } else if !self.pools.is_empty() {
            // Fallback to first pool
            self.pools[0].lock().pop()
        } else {
            None
        }
    }

    pub fn release(&self, node: NumaNode, ptr: NonNull<u8>) {
        let pool_idx = node.0 as usize;
        if pool_idx < self.pools.len() {
            let mut pool = self.pools[pool_idx].lock();
            if pool.len() < self.blocks_per_node {
                pool.push(ptr);
            } else {
                // Pool is full, free the memory
                unsafe { numa_free(ptr.as_ptr() as *mut c_void, self.block_size); }
            }
        } else if !self.pools.is_empty() {
            // Fallback to first pool
            let mut pool = self.pools[0].lock();
            if pool.len() < self.blocks_per_node {
                pool.push(ptr);
            } else {
                unsafe { numa_free(ptr.as_ptr() as *mut c_void, self.block_size); }
            }
        }
    }
}

impl Drop for NumaMemoryPool {
    fn drop(&mut self) {
        for pool in &self.pools {
            let blocks = pool.lock();
            for &ptr in blocks.iter() {
                unsafe { numa_free(ptr.as_ptr() as *mut c_void, self.block_size); }
            }
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

pub struct HighPriorityTask<F> {
    pub id: u64,
    pub numa_affinity: Option<NumaNode>,
    pub execute_fn: F,
}

impl<F> NumaTask for HighPriorityTask<F>
where
    F: FnMut(&NumaThreadContext) -> Result<(), Box<dyn std::error::Error>> + Send,
{
    fn execute(&mut self, ctx: &NumaThreadContext) -> Result<(), Box<dyn std::error::Error>> {
        (self.execute_fn)(ctx)
    }

    fn preferred_numa_node(&self) -> Option<NumaNode> {
        self.numa_affinity
    }

    fn priority(&self) -> u8 {
        255
    }
}

pub struct SolanaNumaOptimizer {
    topology: Arc<NumaTopology>,
    runtime: Arc<NumaAwareRuntime>,
    memory_pool: Arc<NumaMemoryPool>,
    metrics_aggregator: Arc<RwLock<MetricsAggregator>>,
    nic_numa_node: AtomicU32,
}

impl SolanaNumaOptimizer {
    pub fn new(threads_per_node: usize) -> Result<Self, Box<dyn std::error::Error>> {
        // Initialize NUMA system first
        initialize_numa_system()?;
        
        let runtime = Arc::new(NumaAwareRuntime::new(threads_per_node)?);
        let topology = runtime.topology.clone();
        let numa_nodes = topology.nodes.len();
        
        let memory_pool = Arc::new(NumaMemoryPool::new(
            numa_nodes,
            MEMORY_POOL_BLOCK_SIZE,
            1024,
        )?);
        
        let metrics_aggregator = Arc::new(RwLock::new(MetricsAggregator::new(numa_nodes)));
        
        Ok(Self {
            topology,
            runtime,
            memory_pool,
            metrics_aggregator,
            nic_numa_node: AtomicU32::new(u32::MAX),
        })
    }

    pub fn get_optimal_node_for_network_io(&self) -> NumaNode {
        let cached = self.nic_numa_node.load(Ordering::Relaxed);
        if cached != u32::MAX {
            return NumaNode(cached);
        }
        
        let mut best_node = NumaNode(0);
        
        // Try to find NIC NUMA affinity
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
        
        // Try glob pattern for other network interfaces
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
        
        // Fallback: use least loaded node
        let metrics = self.metrics_aggregator.read();
        best_node = metrics.get_least_loaded_node();
        self.nic_numa_node.store(best_node.0, Ordering::Relaxed);
        best_node
    }

    #[inline(always)]
    pub fn allocate_network_buffer(&self, node: NumaNode) -> Option<NonNull<u8>> {
        self.memory_pool.acquire(node)
    }

    #[inline(always)]
    pub fn release_network_buffer(&self, node: NumaNode, buffer: NonNull<u8>) {
        self.memory_pool.release(node, buffer);
    }

    pub fn submit_critical_task<F>(&self, task: F, node: NumaNode) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnMut(&NumaThreadContext) -> Result<(), Box<dyn std::error::Error>> + Send + 'static,
    {
        let task = Box::new(HighPriorityTask {
            id: self.generate_task_id(),
            numa_affinity: Some(node),
            execute_fn: task,
        });
        
        self.runtime.submit_task(task)
    }

    #[inline(always)]
    fn generate_task_id(&self) -> u64 {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        COUNTER.fetch_add(1, Ordering::Relaxed)
    }

    pub fn get_performance_metrics(&self) -> PerformanceReport {
        let thread_metrics = self.runtime.thread_pool.get_metrics();
        let aggregator = self.metrics_aggregator.read();
        
        let total_tasks: u64 = thread_metrics.values()
            .map(|m| m.tasks_executed)
            .sum();
            
        let total_latency: u64 = thread_metrics.values()
            .map(|m| m.avg_latency_ns * m.tasks_executed)
            .sum();
            
        let avg_latency = if total_tasks > 0 {
            total_latency / total_tasks
        } else {
            0
        };
        
        PerformanceReport {
            total_tasks_executed: total_tasks,
            avg_latency_ns: avg_latency,
            cross_numa_accesses: thread_metrics.values()
                .map(|m| m.cross_numa_accesses)
                .sum(),
            numa_balance_score: aggregator.calculate_balance_score(),
        }
    }

    pub fn optimize_for_solana_mev(&self) -> Result<(), Box<dyn std::error::Error>> {
        // Disable CPU frequency scaling
        for cpu_id in 0..num_cpus::get() {
            let governor_path = format!("/sys/devices/system/cpu/cpu{}/cpufreq/scaling_governor", cpu_id);
            if std::path::Path::new(&governor_path).exists() {
                let _ = std::fs::write(&governor_path, b"performance");
            }
        }
        
        // Set interrupt affinity for network card
        let nic_node = self.get_optimal_node_for_network_io();
        if let Some(cores) = self.topology.node_to_cores.get(&nic_node) {
            self.set_irq_affinity(cores)?;
        }
        
        // Optimize network stack
        self.optimize_network_stack()?;
        
        // Update metrics
        self.metrics_aggregator.write().update_node_load(nic_node, 100);
        
        Ok(())
    }

    fn set_irq_affinity(&self, cores: &[u32]) -> Result<(), Box<dyn std::error::Error>> {
        // Find network card IRQs
        let irq_path = "/proc/interrupts";
        let content = std::fs::read_to_string(irq_path)?;
        
        let mut network_irqs = Vec::new();
        for line in content.lines() {
            if line.contains("eth") || line.contains("enp") || line.contains("eno") || line.contains("ens") {
                if let Some(irq_str) = line.split(':').next() {
                    if let Ok(irq) = irq_str.trim().parse::<u32>() {
                        network_irqs.push(irq);
                    }
                }
            }
        }
        
        // Distribute IRQs across cores
        for (idx, &irq) in network_irqs.iter().enumerate() {
            let core = cores[idx % cores.len()];
            let affinity_path = format!("/proc/irq/{}/smp_affinity_list", irq);
            let _ = std::fs::write(&affinity_path, format!("{}", core));
        }
        
        Ok(())
    }

    fn optimize_network_stack(&self) -> Result<(), Box<dyn std::error::Error>> {
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
            let _ = std::fs::write(&path, value);
        }
        
        Ok(())
    }
}

struct MetricsAggregator {
    node_loads: Vec<CacheAligned<AtomicU64>>,
    last_rebalance: AtomicU64,
}

impl MetricsAggregator {
    fn new(numa_nodes: usize) -> Self {
        let mut node_loads = Vec::with_capacity(numa_nodes);
        for _ in 0..numa_nodes {
            node_loads.push(CacheAligned(AtomicU64::new(0)));
        }
        
        Self {
            node_loads,
            last_rebalance: AtomicU64::new(0),
        }
    }

    fn get_least_loaded_node(&self) -> NumaNode {
        if self.node_loads.is_empty() {
            return NumaNode(0);
        }
        
        let mut min_load = u64::MAX;
        let mut best_node = 0;
        
        for (idx, load) in self.node_loads.iter().enumerate() {
            let current = load.0.load(Ordering::Relaxed);
            if current < min_load {
                min_load = current;
                best_node = idx;
            }
        }
        
        NumaNode(best_node as u32)
    }

    fn update_node_load(&self, node: NumaNode, delta: i64) {
        let idx = node.0 as usize;
        if idx < self.node_loads.len() {
            if delta > 0 {
                self.node_loads[idx].0.fetch_add(delta as u64, Ordering::Relaxed);
            } else {
                let abs_delta = (-delta) as u64;
                loop {
                    let current = self.node_loads[idx].0.load(Ordering::Relaxed);
                    let new_value = current.saturating_sub(abs_delta);
                    match self.node_loads[idx].0.compare_exchange_weak(
                        current,
                        new_value,
                        Ordering::Release,
                        Ordering::Relaxed
                    ) {
                        Ok(_) => break,
                        Err(_) => continue,
                    }
                }
            }
        }
    }

    fn calculate_balance_score(&self) -> f64 {
        if self.node_loads.is_empty() {
            return 1.0;
        }
        
                let loads: Vec<u64> = self.node_loads.iter()
            .map(|l| l.0.load(Ordering::Relaxed))
            .collect();
        
        if loads.iter().all(|&l| l == 0) {
            return 1.0;
        }
        
        let sum: u64 = loads.iter().sum();
        let mean = sum as f64 / loads.len() as f64;
        
        if mean == 0.0 {
            return 1.0;
        }
        
        let variance = loads.iter()
            .map(|&load| {
                let diff = load as f64 - mean;
                diff * diff
            })
            .sum::<f64>() / loads.len() as f64;
        
        let std_dev = variance.sqrt();
        let cv = std_dev / mean;
        
        // Score from 0 to 1, where 1 is perfectly balanced
        1.0 / (1.0 + cv).min(10.0)
    }
}

#[derive(Debug, Clone)]
pub struct PerformanceReport {
    pub total_tasks_executed: u64,
    pub avg_latency_ns: u64,
    pub cross_numa_accesses: u64,
    pub numa_balance_score: f64,
}

#[inline(always)]
pub fn prefetch_cache_line(addr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(
            addr as *const i8
        );
    }
}

#[inline(always)]
pub fn prefetch_write_cache_line(addr: *mut u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_prefetch::<{ std::arch::x86_64::_MM_HINT_T0 }>(
            addr as *const i8
        );
    }
}

#[inline(always)]
pub fn memory_fence() {
    std::sync::atomic::fence(Ordering::SeqCst);
}

#[inline(always)]
pub fn pause_cpu() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_mm_pause();
    }
}

pub fn optimize_numa_placement<T: Send + Sync + 'static>(
    data: Vec<T>,
    topology: &NumaTopology,
) -> HashMap<NumaNode, Vec<T>> {
    let mut numa_data = HashMap::new();
    
    if data.is_empty() || topology.nodes.is_empty() {
        if !data.is_empty() {
            numa_data.insert(NumaNode(0), data);
        }
        return numa_data;
    }
    
    let chunk_size = (data.len() + topology.nodes.len() - 1) / topology.nodes.len();
    let mut data_iter = data.into_iter();
    
    for node in &topology.nodes {
        let mut node_data = Vec::with_capacity(chunk_size);
        
        for _ in 0..chunk_size {
            if let Some(item) = data_iter.next() {
                node_data.push(item);
            } else {
                break;
            }
        }
        
        if !node_data.is_empty() {
            numa_data.insert(*node, node_data);
        }
    }
    
    // Handle remaining items
    for item in data_iter {
        if let Some((node, vec)) = numa_data.iter_mut().next() {
            vec.push(item);
        }
    }
    
    numa_data
}

pub fn initialize_numa_system() -> Result<(), Box<dyn std::error::Error>> {
    // Check NUMA availability
    let numa_avail = unsafe { numa_available() };
    if numa_avail < 0 {
        eprintln!("Warning: NUMA not available on this system - running in compatibility mode");
        return Ok(());
    }
    
    // Lock memory pages for predictable latency
    let result = unsafe { mlockall(MCL_CURRENT | MCL_FUTURE) };
    if result != 0 {
        eprintln!("Warning: Failed to lock memory pages - continuing without mlockall");
    }
    
    // Set memory policy for local allocation
    unsafe {
        numa_set_localalloc();
    }
    
    // Disable THP for predictable latency
    let thp_paths = [
        "/sys/kernel/mm/transparent_hugepage/enabled",
        "/sys/kernel/mm/transparent_hugepage/defrag",
    ];
    
    for path in &thp_paths {
        if std::path::Path::new(path).exists() {
            let _ = std::fs::write(path, b"never");
        }
    }
    
    // Set VM settings for low latency
    let vm_settings = [
        ("/proc/sys/vm/swappiness", "0"),
        ("/proc/sys/vm/zone_reclaim_mode", "0"),
        ("/proc/sys/vm/dirty_ratio", "10"),
        ("/proc/sys/vm/dirty_background_ratio", "5"),
    ];
    
    for (path, value) in &vm_settings {
        if std::path::Path::new(path).exists() {
            let _ = std::fs::write(path, value);
        }
    }
    
    Ok(())
}

static NUMA_OPTIMIZER: Lazy<RwLock<Option<Arc<SolanaNumaOptimizer>>>> = Lazy::new(|| {
    RwLock::new(None)
});

pub fn get_numa_optimizer() -> Result<Arc<SolanaNumaOptimizer>, Box<dyn std::error::Error>> {
    // Fast path - read lock
    {
        let read_guard = NUMA_OPTIMIZER.read();
        if let Some(ref optimizer) = *read_guard {
            return Ok(optimizer.clone());
        }
    }
    
    // Slow path - write lock
    let mut write_guard = NUMA_OPTIMIZER.write();
    
    // Double check after acquiring write lock
    if let Some(ref optimizer) = *write_guard {
        return Ok(optimizer.clone());
    }
    
    // Create new optimizer
    let threads_per_node = std::env::var("NUMA_THREADS_PER_NODE")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    
    let optimizer = Arc::new(SolanaNumaOptimizer::new(threads_per_node)?);
    *write_guard = Some(optimizer.clone());
    
    Ok(optimizer)
}

pub fn pin_current_thread_to_node(node: NumaNode) -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        numa_set_preferred(node.0 as c_int);
    }
    
    // Get cores for this NUMA node
    let topology = NumaTopology::discover()?;
    if let Some(cores) = topology.node_to_cores.get(&node) {
        if let Some(&first_core) = cores.first() {
            NumaThreadPool::pin_to_cpu(first_core)?;
        }
    }
    
    Ok(())
}

pub struct NumaHugePage {
    ptr: NonNull<u8>,
    size: usize,
    node: NumaNode,
}

unsafe impl Send for NumaHugePage {}
unsafe impl Sync for NumaHugePage {}

impl NumaHugePage {
    pub fn allocate(size: usize, node: NumaNode) -> Result<Self, Box<dyn std::error::Error>> {
        if size == 0 {
            return Err("Size must be greater than 0".into());
        }
        
        // Round up to 2MB huge page size
        let huge_page_size = 2 * 1024 * 1024;
        let aligned_size = (size + huge_page_size - 1) & !(huge_page_size - 1);
        
        unsafe {
            // Set preferred node
            numa_set_preferred(node.0 as c_int);
            
            let ptr = mmap(
                std::ptr::null_mut(),
                aligned_size,
                PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE,
                -1,
                0,
            );
            
            if ptr == libc::MAP_FAILED {
                return Err("Failed to allocate huge page".into());
            }
            
            // Advise kernel to use huge pages
            let result = madvise(ptr, aligned_size, MADV_HUGEPAGE);
            if result != 0 {
                munmap(ptr, aligned_size);
                return Err("Failed to set huge page advice".into());
            }
            
            // Bind memory to NUMA node
            let nodemask = 1u64 << node.0;
            let result = mbind(
                ptr,
                aligned_size as c_ulong,
                MPOL_BIND,
                &nodemask as *const u64 as *const c_ulong,
                MAX_NUMA_NODES as c_ulong,
                MPOL_MF_STRICT | MPOL_MF_MOVE,
            );
            
            if result != 0 {
                munmap(ptr, aligned_size);
                return Err("Failed to bind memory to NUMA node".into());
            }
            
            Ok(Self {
                ptr: NonNull::new_unchecked(ptr as *mut u8),
                size: aligned_size,
                node,
            })
        }
    }
    
    #[inline]
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }
    
    #[inline]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.size) }
    }
    
    #[inline]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size) }
    }
}

impl Drop for NumaHugePage {
    fn drop(&mut self) {
        unsafe {
            munmap(self.ptr.as_ptr() as *mut libc::c_void, self.size);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numa_topology_discovery() {
        match NumaTopology::discover() {
            Ok(topology) => {
                assert!(!topology.nodes.is_empty());
                assert!(!topology.cores.is_empty());
                println!("NUMA nodes: {:?}", topology.nodes);
                println!("CPU cores: {}", topology.cores.len());
            }
            Err(e) => {
                eprintln!("NUMA discovery failed (expected on non-NUMA systems): {}", e);
            }
        }
    }

    #[test]
    fn test_numa_memory_pool() {
        let pool = match NumaMemoryPool::new(1, 4096, 10) {
            Ok(p) => p,
            Err(e) => {
                eprintln!("Failed to create memory pool: {}", e);
                return;
            }
        };
        
        let node = NumaNode(0);
        
        // Test acquire and release
        let ptr1 = pool.acquire(node);
        assert!(ptr1.is_some());
        
        if let Some(ptr) = ptr1 {
            pool.release(node, ptr);
        }
    }

    #[test]
    fn test_cache_aligned() {
        let aligned = CacheAligned(42u64);
        assert_eq!(*aligned, 42);
        
        // Check alignment
        let addr = &aligned as *const _ as usize;
        assert_eq!(addr % CACHE_LINE_SIZE, 0);
    }

    #[test]
    fn test_numa_huge_page() {
        match NumaHugePage::allocate(4096, NumaNode(0)) {
            Ok(mut page) => {
                let slice = page.as_mut_slice();
                slice[0] = 42;
                assert_eq!(slice[0], 42);
            }
            Err(e) => {
                eprintln!("Huge page allocation failed (may require privileges): {}", e);
            }
        }
    }

    #[test]
    fn test_metrics_aggregator() {
        let aggregator = MetricsAggregator::new(2);
        
        aggregator.update_node_load(NumaNode(0), 100);
        aggregator.update_node_load(NumaNode(1), 100);
        
        let least_loaded = aggregator.get_least_loaded_node();
        assert!(least_loaded.0 < 2);
        
        let score = aggregator.calculate_balance_score();
        assert!(score > 0.0 && score <= 1.0);
    }
}

