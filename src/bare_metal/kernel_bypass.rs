use std::{
    collections::{HashMap, VecDeque},
    ffi::CString,
    io,
    marker::PhantomData,
    mem::{self, ManuallyDrop, MaybeUninit},
    net::SocketAddr,
    num::NonZeroUsize,
    os::unix::io::AsRawFd,
    ptr,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use backoff::{future::retry, ExponentialBackoff};
use bytes::{Bytes, BytesMut};
use crossbeam_queue::ArrayQueue;
use lazy_static::lazy_static;
use log::{debug, error, info, trace, warn};
use numa_rs::Node;
use num_cpus;
use parking_lot::Mutex;
use rand::Rng;

use crate::{
    bare_metal::{
        driver::{Driver, RxTx},
        numa::{bind_to_numa_node, get_numa_node_for_device},
        pool::{PacketPool, TxBufferPool},
    },
    metrics::NetworkMetrics,
    utils::time::nanos_to_ticks,
};

#[repr(align(64))]
struct Al64<T>(T);

/// A single-producer single-consumer ring buffer with proper memory ordering
/// and safe cleanup of remaining elements.
///
/// Safety:
/// Single-producer single-consumer lock-free ring buffer
///
/// # Safety
/// - Safe for concurrent access between exactly one producer and one consumer
/// - All operations use atomic operations with appropriate ordering
/// - Drop implementation properly drains remaining items
/// - Cache padding prevents false sharing
pub struct SpscRing<T> {
    buf: Box<[CachePadded<UnsafeCell<MaybeUninit<T>>>]>,
    mask: usize,
    capacity: usize,
    head: CachePadded<AtomicUsize>, // producer index
    tail: CachePadded<AtomicUsize>, // consumer index
}

unsafe impl<T: Send> Send for SpscRing<T> {}
unsafe impl<T: Send> Sync for SpscRing<T> {}

impl<T> SpscRing<T> {
    pub fn with_capacity_pow2(cap: usize) -> Self {
        assert!(cap.is_power_of_two(), "capacity must be power of two");
        let mut v = Vec::with_capacity(cap);
        for _ in 0..cap {
            v.push(CachePadded::new(UnsafeCell::new(MaybeUninit::uninit())));
        }
        Self {
            buf: v.into_boxed_slice(),
            mask: cap - 1,
            capacity: cap,
            head: CachePadded::new(AtomicUsize::new(0)),
            tail: CachePadded::new(AtomicUsize::new(0)),
        }
    }

    /// Push an item into the ring buffer.
    ///
    /// # Safety
    /// - Must only be called from a single producer thread
    /// - The buffer must not be full (check with `is_full()` or handle the `Err` case)
    pub fn push(&self, value: T) -> Result<(), T> {
        // Load the current head and tail with appropriate ordering
        let head = self.head.0.load(Ordering::Relaxed);
        let tail = self.tail.0.load(Ordering::Acquire);

        // Check if buffer is full using wrapping arithmetic to handle overflow
        if head.wrapping_sub(tail) >= self.capacity {
            return Err(value);
        }

        // Write the value to the buffer
        unsafe {
            let cell = self.buf.get_unchecked(head & self.mask).get();
            ptr::write((*cell).as_mut_ptr(), value);
        }

        // Publish the write to other threads
        self.head.0.store(head.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    pub fn pop(&self) -> Option<T> {
        // Use AcqRel to ensure we have the latest head value
        let tail = self.tail.0.fetch_add(1, Ordering::AcqRel);
        let head = self.head.0.load(Ordering::Acquire);
        
        // Check if the queue is empty
        if tail >= head {
            // Rollback the tail increment
            self.tail.0.fetch_sub(1, Ordering::Release);
            return None;
        }
        
        // Ensure we have the latest value from the slot
        std::sync::atomic::fence(Ordering::Acquire);
        
        // Read the value from the slot
        unsafe {
            let cell = self.buf.get_unchecked(tail & self.mask).get();
            // Use acquire ordering to ensure we see the value written by push
            let value = ptr::read_volatile((*cell).as_ptr());
            Some(value)
        }
    }

    pub fn len(&self) -> usize {
        let h = self.head.0.load(Ordering::Acquire);
        let t = self.tail.0.load(Ordering::Acquire);
        h.wrapping_sub(t)
    }

    pub fn is_empty(&self) -> bool { self.len() == 0 }
    pub fn is_full(&self) -> bool { self.len() == self.capacity }
}

impl<T> Drop for SpscRing<T> {
    fn drop(&mut self) {
        let t = self.tail.0.load(Ordering::Relaxed);
        let h = self.head.0.load(Ordering::Relaxed);
        let mut cur = t;
        while cur != h {
            let idx = cur & self.mask;
            unsafe {
                let cell = self.buf.get_unchecked(idx).get();
                ptr::drop_in_place((*cell).as_mut_ptr());
            }
            cur = cur.wrapping_add(1);
        }
    }
}

use crossbeam_utils::CachePadded;
use crossbeam_queue::SegQueue;
use log::{error, warn, info, debug};
use std::sync::Arc;
use std::io;
use std::sync::atomic::AtomicU64;

// Thread-local buffer pool for NUMA-aware memory allocation
#[derive(Debug)]
struct NumaBufferPool {
    node: i32,
    buffers: ArrayQueue<BytesMut>,
    total_allocated: AtomicUsize,
    max_buffers: usize,
}

impl NumaBufferPool {
    fn new(node: i32, max_buffers: usize) -> Self {
        Self {
            node,
            buffers: ArrayQueue::new(max_buffers),
            total_allocated: AtomicUsize::new(0),
            max_buffers,
        }
    }

    fn allocate(&self, size: usize) -> Option<BytesMut> {
        // Try to get a buffer from the pool first
        if let Some(mut buf) = self.buffers.pop() {
            if buf.capacity() >= size {
                buf.clear();
                return Some(buf);
            }
            // Buffer is too small, return it to the pool and allocate a new one
            let _ = self.buffers.push(buf);
        }

        // Allocate a new buffer if we haven't reached the limit
        if self.total_allocated.load(Ordering::Relaxed) < self.max_buffers {
            let buf = if self.node >= 0 {
                unsafe {
                    let ptr = numa_alloc_onnode(size, self.node) as *mut u8;
                    if !ptr.is_null() {
                        let vec = Vec::from_raw_parts(ptr, 0, size);
                        BytesMut::from(vec)
                    } else {
                        BytesMut::with_capacity(size)
                    }
                }
            } else {
                BytesMut::with_capacity(size)
            };
            self.total_allocated.fetch_add(1, Ordering::Relaxed);
            Some(buf)
        } else {
            None
        }
    }

    fn deallocate(&self, mut buf: BytesMut) {
        if self.buffers.len() < self.max_buffers {
            let _ = self.buffers.push(buf);
        }
        // If the queue is full, the buffer will be dropped
    }
}

// Global NUMA buffer manager
struct NumaBufferManager {
    pools: HashMap<i32, Arc<NumaBufferPool>>,
    default_pool: Arc<NumaBufferPool>,
    max_buffers_per_node: usize,
}

impl NumaBufferManager {
    fn new(max_buffers_per_node: usize) -> Self {
        let mut pools = HashMap::new();
        
        // Initialize a pool for each NUMA node
        if unsafe { numa_available() == 0 } {
            let max_node = unsafe { numa_max_node() };
            for node in 0..=max_node {
                pools.insert(node, Arc::new(NumaBufferPool::new(node, max_buffers_per_node)));
            }
        }
        
        // Fallback pool when NUMA is not available
        let default_pool = Arc::new(NumaBufferPool::new(-1, max_buffers_per_node));
        
        Self {
            pools,
            default_pool,
            max_buffers_per_node,
        }
    }
    
    fn get_pool(&self, node: Option<i32>) -> Arc<NumaBufferPool> {
        if let Some(node) = node.and_then(|n| self.pools.get(&n)) {
            node.clone()
        } else {
            self.default_pool.clone()
        }
    }
}

lazy_static! {
    static ref BUFFER_MANAGER: Mutex<NumaBufferManager> = 
        Mutex::new(NumaBufferManager::new(1024)); // Adjust max buffers as needed
}

// Helper function to get a buffer from the appropriate NUMA node
fn get_numa_buffer(node: Option<i32>, size: usize) -> Option<BytesMut> {
    let manager = BUFFER_MANAGER.lock().unwrap();
    let pool = manager.get_pool(node);
    drop(manager); // Release the lock as soon as possible
    
    pool.allocate(size)
}

// Helper function to return a buffer to the pool
fn return_numa_buffer(node: Option<i32>, buf: BytesMut) {
    let manager = BUFFER_MANAGER.lock().unwrap();
    let pool = manager.get_pool(node);
    drop(manager); // Release the lock as soon as possible
    
    pool.deallocate(buf);
}

/// Minimal Rx/Tx driver trait
pub trait RxTx: Send + Sync {
    fn rx_batch(&mut self, out: &mut [Vec<u8>]) -> usize;
    fn tx_batch(&mut self, inpkts: &[&[u8]]) -> usize;
}

/// Single TxSubmitter trait (use only this one)
pub trait TxSubmitter: Send + Sync {
    /// Send a batch of packets (slices)
    fn send_batch(&self, bufs: &[&[u8]]) -> io::Result<usize>;
    
    /// Send a batch of owned byte vectors
    fn send_batch_owned(&self, bufs: &[Vec<u8>]) -> io::Result<usize> {
        // Default implementation converts to slices and calls send_batch
        let refs: Vec<&[u8]> = bufs.iter().map(|b| &b[..]).collect();
        self.send_batch(&refs)
    }
    
    /// Submit a single TPU packet
    fn submit_tpu(&self, pkt: &[u8]) -> io::Result<()> {
        // Default implementation uses send_batch
        self.send_batch(&[pkt])?;
        Ok(())
    }
    
    /// Submit a bundle of transactions with a tip
    fn submit_bundle(&self, txs: &[Vec<u8>], _tip: u64) -> io::Result<()> {
        // Default implementation uses send_batch_owned
        self.send_batch_owned(txs)?;
        Ok(())
    }
}

/// Adapter that makes an Arc<RxTx> look like a TxSubmitter
#[derive(Clone)]
pub struct DriverAdapter<T: RxTx>(Arc<T>);

impl<T: RxTx> DriverAdapter<T> {
    pub fn new(inner: Arc<T>) -> Self { Self(inner) }
}

impl<T: RxTx> TxSubmitter for DriverAdapter<T> {
    fn send_batch(&self, bufs: &[&[u8]]) -> io::Result<usize> {
        // call tx_batch on an Arc<T>
        Ok(self.0.tx_batch(bufs))
    }
}

use std::cell::UnsafeCell;
use std::mem::{self, MaybeUninit};
use std::sync::atomic::Ordering;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::ptr;

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::mem::{self, MaybeUninit};
use std::ptr::{self, NonNull};
use std::os::unix::io::{AsRawFd, RawFd, FromRawFd};
use std::time::{Duration, Instant};
use std::thread;
use std::collections::VecDeque;
use std::net::{SocketAddr, UdpSocket};
use std::io;
use libc::{
    c_void, c_int, sockaddr_ll, sockaddr, socklen_t,
    socket, bind, close, setsockopt, recv, send, poll, pollfd,
    mmap, munmap, mlock, munlock,
    AF_PACKET, SOCK_RAW, ETH_P_ALL,
    SOL_PACKET, PACKET_VERSION, TPACKET_V3,
    PACKET_RX_RING, PACKET_TX_RING, PACKET_LOSS,
    PACKET_TIMESTAMP, SOL_SOCKET, SO_TIMESTAMPNS,
    MAP_SHARED, MAP_LOCKED, MAP_POPULATE, PROT_READ, PROT_WRITE,
    IFF_PROMISC, SIOCGIFINDEX, SIOCSIFFLAGS, SIOCGIFFLAGS,
    cpu_set_t, CPU_SET, CPU_ZERO, sched_setaffinity,
    SCHED_FIFO, sched_param, sched_setscheduler,
    MADV_HUGEPAGE, madvise, EAGAIN, POLLIN, POLLOUT,
};
use nix::sys::socket::{sockopt::ReuseAddr, setsockopt as nix_setsockopt};
use crossbeam::channel::{bounded, Sender, Receiver, TryRecvError};
use parking_lot::{RwLock, Mutex};
use bytes::{Bytes, BytesMut, BufMut};
use crc32fast::Hasher as Crc32;

const PAGE_SIZE: usize = 4096;
const BLOCK_SIZE: usize = 1 << 20; // 1MB blocks
const FRAME_SIZE: usize = 2048;
const BLOCK_NR: usize = 64;
const FRAME_NR: usize = BLOCK_SIZE / FRAME_SIZE * BLOCK_NR;
const BATCH_SIZE: usize = 32;
const RING_SIZE: usize = BLOCK_SIZE * BLOCK_NR;
const SOLANA_PORT_START: u16 = 8000;
const SOLANA_PORT_END: u16 = 8020;
const MAX_PACKET_SIZE: usize = 1500;
const CACHE_LINE_SIZE: usize = 64;
const TX_BATCH_SIZE: usize = 32;
const TX_FLUSH_TIMEOUT_US: u64 = 100; // 100μs

// Packet socket constants
const SOL_PACKET: c_int = 263;
const SIOCGIFFLAGS: c_int = 0x8913;
const SIOCSIFFLAGS: c_int = 0x8914;
const IFF_PROMISC: c_int = 0x100;

// New packet socket constants
const PACKET_QDISC_BYPASS: c_int = 20;
const PACKET_STATISTICS: c_int = 6;
const PACKET_FANOUT: c_int = 18;
const PACKET_FANOUT_HASH: u32 = 0;
const PACKET_FANOUT_CPU: u32 = 2;
const PACKET_FANOUT_FLAG_DEFRAG: u32 = 0x8000;

// Packet status flags
const TP_STATUS_KERNEL: u32 = 0;
const TP_STATUS_USER: u32 = 1;
const TP_STATUS_SEND_REQUEST: u32 = 1;
const TP_STATUS_SENDING: u32 = 2;

// Packet header size
const TPACKET3_HDRLEN: usize = 48;

#[repr(C)]
pub struct tpacket_stats {
    tp_packets: u32,
    tp_drops: u32,
}

#[derive(Copy, Clone)]
pub struct IfState {
    had_promisc: bool,
    orig_flags: u32,
}

#[repr(C)]
struct TpacketReq3 {
    tp_block_size: u32,
    tp_block_nr: u32,
    tp_frame_size: u32,
    tp_frame_nr: u32,
    tp_retire_blk_tov: u32,
    tp_sizeof_priv: u32,
    tp_feature_req_word: u32,
}

#[repr(C)]
struct Tpacket3Hdr {
    tp_next_offset: u32,
    tp_sec: u32,
    tp_nsec: u32,
    tp_snaplen: u32,
    tp_len: u32,
    tp_status: u32,
    tp_mac: u16,
    tp_net: u16,
    // union
    hv1: TpacketHdrVariant1,
    tp_padding: [u8; 8],
}

#[repr(C)]
#[derive(Copy, Clone)]
struct TpacketHdrVariant1 {
    tp_rxhash: u32,
    tp_vlan_tci: u32,
    tp_vlan_tpid: u16,
    tp_padding: u16,
}

#[repr(C)]
struct BlockDesc {
    version: u32,
    offset_to_priv: u32,
    hdr: TpacketBdHdr,
}

#[repr(C)]
struct TpacketBdHdr {
    block_status: u32,
    num_pkts: u32,
    offset_to_first_pkt: u32,
}

#[repr(C)]
struct BlockHeader {
    block_status: u32,
    num_pkts: u32,
    offset_to_first_pkt: u32,
    // ... other fields
}

#[repr(C)]
struct BlockDesc {
    hdr: BlockHeader,
    // ... padding and union fields
}

#[derive(Default)]
pub struct Stats {
    rx_packets: AtomicU64,
    rx_bytes: AtomicU64,
    rx_drops: AtomicU64,
    tx_packets: AtomicU64,
    tx_bytes: AtomicU64,
    solana_packets: AtomicU64,
}

pub struct Metrics {
    pub rx_pkts: AtomicU64,
    pub tx_pkts: AtomicU64,
    pub rx_bytes: AtomicU64,
    pub tx_bytes: AtomicU64,
    pub rx_drops_kernel: AtomicU64,
    pub solana_hits: AtomicU64,
    pub bundle_submits: AtomicU64,
    pub tpu_submits: AtomicU64,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            rx_pkts: AtomicU64::new(0),
            tx_pkts: AtomicU64::new(0),
            rx_bytes: AtomicU64::new(0),
            tx_bytes: AtomicU64::new(0),
            rx_drops_kernel: AtomicU64::new(0),
            solana_hits: AtomicU64::new(0),
            bundle_submits: AtomicU64::new(0),
            tpu_submits: AtomicU64::new(0),
        }
    }
    
    pub fn to_prometheus(&self) -> String {
        format!(
            "# TYPE kernel_rx_packets counter\n"
            "kernel_rx_packets {}\n"
            "# TYPE kernel_tx_packets counter\n"
            "kernel_tx_packets {}\n"
            "# TYPE kernel_rx_bytes counter\n"
            "kernel_rx_bytes {}\n"
            "# TYPE kernel_tx_bytes counter\n"
            "kernel_tx_bytes {}\n"
            "# TYPE kernel_drops counter\n"
            "kernel_drops {}\n"
            "# TYPE solana_packets counter\n"
            "solana_packets {}\n"
            "# TYPE bundle_submits counter\n"
            "bundle_submits {}\n"
            "# TYPE tpu_submits counter\n"
            "tpu_submits {}\n",
            self.rx_pkts.load(Ordering::Relaxed),
            self.tx_pkts.load(Ordering::Relaxed),
            self.rx_bytes.load(Ordering::Relaxed),
            self.tx_bytes.load(Ordering::Relaxed),
            self.rx_drops_kernel.load(Ordering::Relaxed),
            self.solana_hits.load(Ordering::Relaxed),
            self.bundle_submits.load(Ordering::Relaxed),
            self.tpu_submits.load(Ordering::Relaxed),
        )
    }
}

pub trait TxSubmitter: Send + Sync {
    fn send_batch(&self, bufs: &[&[u8]]) -> io::Result<usize>;
    // Other methods if needed...
}

impl<T: RxTx> TxSubmitter for RxTxAdapter<T> {
    fn send_batch(&self, bufs: &[&[u8]]) -> io::Result<usize> {
        Ok(self.0.tx_batch(bufs))
    }
}

pub trait RxTx {
    fn rx_batch(&mut self, out: &mut [Vec<u8>]) -> usize;
    fn tx_batch(&mut self, inpkts: &[&[u8]]) -> usize;
}

/// Configuration for TX queue backpressure
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Maximum number of spin retries before falling back to direct send
    pub max_spin_retries: u32,
    /// Maximum number of direct send retries
    pub max_direct_retries: u32,
    /// Initial backoff duration in microseconds
    pub initial_backoff_us: u64,
    /// Maximum backoff duration in microseconds
    pub max_backoff_us: u64,
    /// Maximum queue size before applying backpressure
    pub max_queue_size: usize,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            max_spin_retries: 1000,
            max_direct_retries: 3,
            initial_backoff_us: 1,
            max_backoff_us: 1000, // 1ms max backoff
            max_queue_size: 1024 * 1024, // 1M packets
        }
    }
}

pub struct KernelBypass<T: RxTx> {
    /// The underlying driver implementing RxTx
    driver: Arc<T>,
    /// Raw socket file descriptor
    raw_fd: RawFd,
    /// Network interface name as bytes
    interface_name: [u8; 16],
    /// Network interface name as string
    interface_name_str: String,
    /// Interface state for promiscuous mode
    if_state: Option<IfState>,
    /// Statistics counters
    stats: Arc<Stats>,
    /// Performance metrics
    metrics: Metrics,
    /// Flag to control the main loop
    running: Arc<AtomicBool>,
    /// Queue for outgoing packets
    tx_queue: Arc<SpscRing<Bytes>>,
    /// Batcher for efficient packet transmission
    tx_batcher: TxBatcher<DriverAdapter<T>>,
    /// XDP rings if using AF_XDP
    xdp_rings: Option<NonNull<c_void>>,
    /// Pool of reusable packet buffers
    packet_pool: PacketPool,
    /// Pool of reusable TX buffers
    tx_buffer_pool: TxBufferPool,
    /// Backpressure configuration
    backpressure_config: BackpressureConfig,
    /// NUMA node for this interface
    numa_node: Option<i32>,
    /// Statistics
    stats_tx_queued: AtomicU64,
    stats_tx_dropped: AtomicU64,
    stats_tx_direct: AtomicU64,
    stats_tx_retries: AtomicU64,
    /// Reserved for future use
    _phantom: PhantomData<T>,
}

impl<T: RxTx> KernelBypass<T> {
    /// Create a new KernelBypass instance with default configuration
    pub fn new(driver: Arc<T>, interface_name: &str) -> io::Result<Self> {
        Self::with_config(driver, interface_name, BackpressureConfig::default())
    }

    /// Create a new KernelBypass instance with custom configuration
    pub fn with_config(
        driver: Arc<T>,
        interface_name: &str,
        config: BackpressureConfig,
    ) -> io::Result<Self> {
        let numa_node = get_numa_node_for_device(interface_name);
        
        if let Some(node) = numa_node {
            info!("Using NUMA node {} for interface {}", node, interface_name);
            if let Err(e) = bind_to_numa_node(interface_name, "kernel-bypass") {
                warn!("Failed to bind to NUMA node: {}", e);
            }
        } else {
            info!("NUMA not available or not detected for interface {}", interface_name);
        }
        // Create interface name as bytes and string
        // IFNAMSIZ is 16 bytes including null terminator, so max name length is 15
        const MAX_IFNAME_LEN: usize = 15;
        let mut ifname_bytes = [0u8; 16];
        let bytes = interface_name.as_bytes();
        let len = bytes.len();
        
        if len == 0 || len > MAX_IFNAME_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Interface name must be 1-{} characters", MAX_IFNAME_LEN),
            ));
        }
        
        // Copy interface name bytes
        ifname_bytes[..len].copy_from_slice(bytes);
        
        // Get NUMA node for this interface
        let numa_node = get_numa_node_for_device(interface_name);
        
        if let Some(node) = numa_node {
            info!("Using NUMA node {} for interface {}", node, interface_name);
            if let Err(e) = bind_to_numa_node(interface_name, "kernel-bypass") {
                warn!("Failed to bind to NUMA node: {}", e);
            }
        } else {
            info!("NUMA not available or not detected for interface {}", interface_name);
        }
        
        // Initialize packet and buffer pools
        let packet_pool = PacketPool::new();
        let tx_buffer_pool = TxBufferPool::new();
        
        // Initialize TX queue with size from config
        let tx_queue = Arc::new(ArrayQueue::new(config.max_queue_size));
        
        // Create the instance
        Ok(Self {
            driver: driver.clone(),
            raw_fd: 0, // Will be set by the driver
            interface_name: ifname_bytes,
            interface_name_str: interface_name.to_string(),
            running: AtomicBool::new(true),
            backpressure_config: config,
            numa_node,
            stats_tx_queued: AtomicU64::new(0),
            stats_tx_dropped: AtomicU64::new(0),
            stats_tx_direct: AtomicU64::new(0),
            stats_tx_retries: AtomicU64::new(0),
            tx_queue,
            tx_batcher: TxBatcher::new(Arc::new(DriverAdapter(driver))),
            xdp_rings: None,
            packet_pool,
            tx_buffer_pool,
            _phantom: PhantomData,
        })
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Interface name must be 1-{} bytes", MAX_IFNAME_LEN)
            ));
        }
        
        // Copy the interface name bytes
        ifname_bytes[..len].copy_from_slice(bytes);
        let interface_name_str = interface_name.to_string();
        
        // Get NUMA node for the interface
        let numa_node = get_numa_node_for_device(&interface_name_str);
        if let Some(node) = numa_node {
            if let Err(e) = bind_to_numa_node(node) {
                warn!("Failed to bind to NUMA node {}: {}", node, e);
            }
        }
        
        // Create the driver adapter and tx_batcher
        let adapter = Arc::new(DriverAdapter(Arc::clone(&driver)));
        let tx_batcher = TxBatcher::new(adapter);
        
        // Create raw packet socket with hugepage support for ring buffers
        let fd = unsafe {
            let fd = libc::socket(libc::AF_PACKET, libc::SOCK_RAW, (libc::ETH_P_ALL as i32).to_be());
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }

            // Set socket options for TPACKET_V3 with hugepage support
            let version = libc::TPACKET_V3 as i32;
            if libc::setsockopt(
                fd,
                libc::SOL_PACKET,
                libc::PACKET_VERSION,
                &version as *const _ as *const libc::c_void,
                std::mem::size_of_val(&version) as libc::socklen_t,
            ) < 0
            {
                let err = io::Error::last_os_error();
                let _ = unsafe { libc::close(fd) };
                return Err(err);
            }

            fd
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        // Configure socket options
        let version = TPACKET_V3 as i32;
        if unsafe {
            // SAFETY: setsockopt is safe as we control the buffer size and alignment
            libc::setsockopt(fd, SOL_PACKET, PACKET_VERSION, &version as *const _ as *const _, std::mem::size_of_val(&version) as u32)
        } < 0 {
            let err = io::Error::last_os_error();
            unsafe { libc::close(fd); }
            return Err(err);
        }

        // Set promiscuous mode using the socket fd
        let if_state = unsafe {
            // SAFETY: set_promiscuous_socket is safe as it checks return values and handles errors
            set_promiscuous_socket(interface_name)?
        };

        // Construct final object
        Ok(Self {
            driver,
            raw_fd: fd,
            interface_name: ifname_bytes,
            interface_name_str,
            if_state: Some(if_state),
            stats: Arc::new(Stats::default()),
            metrics: Metrics::new(),
            running: Arc::new(AtomicBool::new(true)),
            tx_queue: Arc::new(SpscRing::with_capacity_pow2(1024)),
            tx_batcher,
            xdp_rings: None,
            packet_pool: PacketPool::new(),
            tx_buffer_pool: TxBufferPool::new(),
            _phantom: PhantomData,
        })
    }

    pub fn process_rx_batch(&mut self, out: &mut [Vec<u8>]) -> usize {
        self.driver.rx_batch(out)
    }

    /// Send all queued packets in batches
    pub fn send_tx_batch(&mut self) -> io::Result<usize> {
        let mut sent = 0;
        
        // Process all queued packets
        while let Some(packet) = self.tx_queue.pop() {
            self.tx_batcher.add_packet(packet)?;
            sent += 1;
        }
        
        // Flush any remaining packets in the batcher
        self.tx_batcher.flush()?;
        
        // Update stats
        self.stats.tx_packets.fetch_add(sent, Ordering::Relaxed);
        
        Ok(sent)
    }

    /// Queue a packet for transmission with backpressure handling.
    /// Implements exponential backoff and fallback to direct send.
    pub fn queue_tx_packet(&mut self, packet: &[u8]) -> io::Result<()> {
        let config = &self.backpressure_config;
        
        // Try to allocate a NUMA-aware buffer
        let mut buf = match get_numa_buffer(self.numa_node, packet.len()) {
            Some(b) => b,
            None => {
                self.stats_tx_dropped.fetch_add(1, Ordering::Relaxed);
                return Err(io::Error::new(
                    io::ErrorKind::OutOfMemory,
                    "Failed to allocate NUMA buffer"
                ));
            }
        };
        
        // Copy packet data into the buffer
        buf.clear();
        buf.extend_from_slice(packet);
        let buf = buf.freeze();  // Convert to immutable Bytes
        
        // Try to push to the queue with backpressure handling
        let mut spin_count = 0;
        let mut backoff = config.initial_backoff_us;
        
        // Phase 1: Try with exponential backoff
        while spin_count < config.max_spin_retries {
            match self.tx_queue.push(buf.clone()) {
                Ok(()) => {
                    self.stats_tx_queued.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                Err(_) => {
                    // Apply exponential backoff with jitter
                    spin_count += 1;
                    if spin_count % 10 == 0 {
                        debug!("TX queue full, retry {}/{}", 
                              spin_count, config.max_spin_retries);
                    }
                    
                    // Exponential backoff with jitter
                    std::thread::sleep(Duration::from_micros(
                        backoff + (rand::random::<u64>() % backoff) / 2
                    ));
                    backoff = (backoff * 2).min(config.max_backoff_us);
                }
            }
        }
        
        // Phase 2: Fall back to direct send if queue is still full
        self.stats_tx_retries.fetch_add(1, Ordering::Relaxed);
        warn!("TX queue full after {} retries, falling back to direct send", 
              config.max_spin_retries);
        
        // Try direct send with retries
        let mut retries = 0;
        while retries < config.max_direct_retries {
            match self.driver.tx_batch(&[&buf]) {
                Ok(sent) if sent > 0 => {
                    self.stats_tx_direct.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                Ok(_) => {
                    // No packets sent, but no error
                    retries += 1;
                    std::thread::yield_now();
                }
                Err(e) => {
                    // Driver error, log and retry
                    retries += 1;
                    debug!("Direct send failed (attempt {}/{}): {}", 
                          retries, config.max_direct_retries, e);
                    std::thread::yield_now();
                }
            }
        }
        
        // If we get here, all retries failed
        self.stats_tx_dropped.fetch_add(1, Ordering::Relaxed);
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!("Failed to send after {} direct retries", config.max_direct_retries)
        ))
        
        // Phase 2: Fall back to direct send
        self.stats_tx_retries.fetch_add(1, Ordering::Relaxed);
        warn!("TX queue full after {} retries, falling back to direct send", 
              config.max_spin_retries);
        
              config.max_spin_retries);
        
        // Try direct send with retries
        let mut retries = 0;
        while retries < config.max_direct_retries {
            match self.driver.tx_batch(&[packet]) {
                Ok(sent) if sent > 0 => {
                    self.stats_tx_direct.fetch_add(1, Ordering::Relaxed);
                    return Ok(());
                }
                Ok(_) => {
                    // No packets sent, but no error
                    retries += 1;
                    std::thread::yield_now();
                }
                Err(e) => {
                    self.stats_tx_dropped.fetch_add(1, Ordering::Relaxed);
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("Direct send failed: {}", e)
                    ));
                }
            }
        }
        
        // If we get here, all retries failed
        self.stats_tx_dropped.fetch_add(1, Ordering::Relaxed);
        Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            format!("Failed to send after {} direct retries", config.max_direct_retries)
        ))
        // Try to acquire a buffer with NUMA awareness
        let numa_node = get_numa_node_for_device(&self.interface_name_str);
        let mut buf = self.acquire_numa_buffer(numa_node, packet.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "Failed to allocate NUMA-aware buffer"))?;
        
        // Copy packet data into the buffer
        buf.clear();
        buf.extend_from_slice(packet);
        
        // Try to push to the queue with backpressure
        let bytes = buf.freeze();
        let mut retry_count = 0;
        const MAX_RETRIES: u32 = 1000; // Adjust based on your latency requirements
        
        loop {
            match self.tx_queue.push(bytes.clone()) {
                Ok(()) => return Ok(()),
                Err(_) if retry_count < MAX_RETRIES => {
                    // Busy wait with exponential backoff
                    let backoff = 1 << retry_count.min(8); // Cap at 256 iterations
                    for _ in 0..backoff {
                        std::hint::spin_loop();
                    }
                    retry_count += 1;
                }
                Err(e) => {
                    // Fallback to direct send if queue is still full after retries
                    log::warn!("TX queue full, falling back to direct send");
                    return self.driver.tx_batch(&[&packet])
                        .map(|_| ())
                        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Direct send failed: {}", e)));
                }
            }
        }
    }

    pub fn run(&mut self) -> io::Result<()> {
        // Pin RX thread to core 0
        if let Err(e) = bind_to_numa_node(&self.interface_name_str, "rx") {
            log::warn!("Failed to bind RX thread to NUMA node: {}", e);
        }
        
        unsafe {
            let mut cpu_set = libc::cpu_set_t::default();
            libc::CPU_SET(0, &mut cpu_set);
            
            if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &cpu_set) != 0 {
                log::warn!("Failed to set CPU affinity: {}", io::Error::last_os_error());
            }
            
            let param = libc::sched_param {
                sched_priority: 99,
            };
            
            if libc::sched_setscheduler(0, libc::SCHED_FIFO, &param) != 0 {
                let e = io::Error::last_os_error();
                log::warn!("Failed to set real-time priority (CAP_SYS_NICE required): {}", e);
            }
        }
        
        let mut polls = vec![pollfd {
            fd: self.raw_fd,
            events: POLLIN,
            revents: 0,
        }];

        while self.running.load(Ordering::Relaxed) {
            // SAFETY: poll syscall is safe as we control the fds
            let ready = unsafe { libc::poll(polls.as_mut_ptr(), polls.len() as u32, -1) };
            if ready < 0 {
                return Err(io::Error::last_os_error());
            }

            if polls[0].revents & POLLIN != 0 {
                self.process_rx_batch(&mut [])?;
            }
        }
        Ok(())
    }

    pub fn spawn_tx_thread(&self) -> io::Result<()> {
        // Pin TX thread to core 1
        if let Err(e) = bind_to_numa_node(&self.interface_name_str, "tx") {
            log::warn!("Failed to bind TX thread to NUMA node: {}", e);
        }
        
        unsafe {
            let mut cpu_set = libc::cpu_set_t::default();
            libc::CPU_SET(1, &mut cpu_set);
            
            if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &cpu_set) != 0 {
                log::warn!("Failed to set CPU affinity: {}", io::Error::last_os_error());
            }
            
            let param = libc::sched_param {
                sched_priority: 98,
            };
            
            if libc::sched_setscheduler(0, libc::SCHED_FIFO, &param) != 0 {
                let e = io::Error::last_os_error();
                log::warn!("Failed to set real-time priority (CAP_SYS_NICE required): {}", e);
            }
        }
        
        // TX processing logic
        Ok(())
    }

    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};

    #[inline(always)]
    pub unsafe fn prefetch_packet(addr: *const u8, len: usize) {
        #[cfg(target_arch = "x86_64")]
        {
            // Prefetch at least one cache line, but no more than needed
            let prefetch_lines = (len + 63) / 64;
            let lines_to_prefetch = prefetch_lines.min(4); // Don't prefetch more than 4 cache lines
            
            for i in 0..lines_to_prefetch {
                _mm_prefetch(addr.add(i * 64) as *const i8, _MM_HINT_T0);
            }
        }
    }

    pub fn print_stats(&self) {
        let rx_packets = self.stats.rx_packets.load(Ordering::Relaxed);
        let tx_packets = self.stats.tx_packets.load(Ordering::Relaxed);
        let rx_bytes = self.stats.rx_bytes.load(Ordering::Relaxed);
        let tx_bytes = self.stats.tx_bytes.load(Ordering::Relaxed);
        let rx_drops = self.stats.rx_drops.load(Ordering::Relaxed);
        let solana_packets = self.stats.solana_packets.load(Ordering::Relaxed);
        
        let rx_mbps = (rx_bytes * 8) as f64 / 1_000_000.0;
        let tx_mbps = (tx_bytes * 8) as f64 / 1_000_000.0;
        
        println!("[STATS] RX: {} pkts ({:.2} Mbps) | TX: {} pkts ({:.2} Mbps) | Solana: {} | Drops: {}",
                 rx_packets, rx_mbps, tx_packets, tx_mbps, solana_packets, rx_drops);
    }

    pub fn get_stats(&self) -> (u64, u64, u64, u64, u64) {
        (
            self.stats.rx_packets.load(Ordering::Relaxed),
            self.stats.tx_packets.load(Ordering::Relaxed),
            self.stats.rx_bytes.load(Ordering::Relaxed),
            self.stats.tx_bytes.load(Ordering::Relaxed),
            self.stats.solana_packets.load(Ordering::Relaxed),
        )
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub fn reset_stats(&self) {
        self.stats.rx_packets.store(0, Ordering::Relaxed);
        self.stats.tx_packets.store(0, Ordering::Relaxed);
        self.stats.rx_bytes.store(0, Ordering::Relaxed);
        self.stats.tx_bytes.store(0, Ordering::Relaxed);
        self.stats.rx_drops.store(0, Ordering::Relaxed);
        self.stats.solana_packets.store(0, Ordering::Relaxed);
    }

    pub fn read_packet_stats(&self) -> io::Result<(u64, u64)> {
        let mut stats: tpacket_stats = std::mem::zeroed();
        let mut len = std::mem::size_of::<tpacket_stats>() as socklen_t;
        
        if unsafe {
            // SAFETY: getsockopt is safe as we control the buffer size and alignment
            libc::getsockopt(
                self.raw_fd,
                SOL_PACKET,
                PACKET_STATISTICS,
                &mut stats as *mut _ as *mut c_void,
                &mut len
            )
        } < 0 {
            return Err(io::Error::last_os_error());
        }
        
        Ok((stats.tp_packets as u64, stats.tp_drops as u64))
    }

    pub mod replay {
        use super::*;

        #[repr(C)]
        pub struct BlockRecordHeader {
            pub ts_ns: u64,
            pub block_len: u32,
            pub num_pkts: u32,
        }

        pub struct Replayer {
            data: Vec<u8>,
            cursor: usize,
        }

        impl Replayer {
            pub fn from_file(path: &str) -> io::Result<Self> {
                let data = std::fs::read(path)?;
                Ok(Self { data, cursor: 0 })
            }

            pub fn next_block(&mut self) -> Option<(&[u8], u32)> {
                if self.cursor + std::mem::size_of::<BlockRecordHeader>() > self.data.len() {
                    return None;
                }

                let hdr: &BlockRecordHeader = unsafe {
                    // SAFETY: cursor is within bounds of data and BlockRecordHeader is properly aligned
                    &*(self.data.as_ptr().add(self.cursor) as *const BlockRecordHeader)
                };
                self.cursor += std::mem::size_of::<BlockRecordHeader>();

                if self.cursor + (hdr.block_len as usize) > self.data.len() {
                    return None;
                }

                let block = &self.data[self.cursor..self.cursor + hdr.block_len as usize];
                self.cursor += hdr.block_len as usize;

                Some((block, hdr.num_pkts))
            }
        }
    }

    #[derive(Clone)]
    pub struct LeaderSchedule {
        pub current_slot: u64,
        slot_leaders: Vec<[u8; 32]>,
    }

    impl LeaderSchedule {
        pub fn leader_for(&self, slot: u64) -> [u8; 32] {
            self.slot_leaders[(slot % self.slot_leaders.len() as u64) as usize]
        }
        
        pub fn slot_phase(&self, now_ns: u64) -> SlotPhase {
            let slot_progress = now_ns % 400_000_000; // 400ms slot duration
            match slot_progress {
                p if p < 100_000_000 => SlotPhase::Early,
                p if p < 300_000_000 => SlotPhase::Mid,
                _ => SlotPhase::Late,
            }
        }
    }

    #[derive(Copy, Clone, Eq, PartialEq)]
    pub enum SlotPhase { Early, Mid, Late }

    pub struct InclusionModel {
        ewma_tip: f64,
        ewma_alpha: f64,
    }

    impl InclusionModel {
        pub fn predict_prob(&self, _leader: [u8; 32], tip: u64, phase: SlotPhase) -> f64 {
            let phase_mult = match phase {
                SlotPhase::Early => 0.9,
                SlotPhase::Mid => 0.7,
                SlotPhase::Late => 0.4,
            };
            (tip as f64 / self.ewma_tip.max(1.0)).min(1.0) * phase_mult
        }
        
        pub fn suggest_tip(&self, leader: [u8; 32], target_prob: f64, phase: SlotPhase) -> u64 {
            let base = (target_prob * self.ewma_tip) / match phase {
                SlotPhase::Early => 0.9,
                SlotPhase::Mid => 0.7,
                SlotPhase::Late => 0.4,
            };
            base.ceil() as u64
        }
    }

    pub struct JitoRouter<S: TxSubmitter> {
        pub submit: S,
        pub leaders: LeaderSchedule,
        pub model: InclusionModel,
        pub max_fanout: usize,
    }

    impl<S: TxSubmitter> JitoRouter<S> {
        /// Route transactions to the appropriate destination based on leader schedule
        pub fn route(&self, txs: &[Vec<u8>], cu_price: Option<u64>, now_ns: u64) -> io::Result<()> {
            if txs.is_empty() {
                return Ok(());
            }
            
            let leader = self.leaders.leader_for(self.leaders.current_slot);
            let phase = self.leaders.slot_phase(now_ns);
            let target = 0.9;
            let base_tip = cu_price.unwrap_or(0);
            let suggested = self.model.suggest_tip(leader, target, phase).max(base_tip);
            
            // Submit to leader if available, otherwise broadcast to all
            if let Some(_leader) = leader {
                // Submit first transaction to leader
                self.submit.submit_tpu(&txs[0])?;
                
                // Submit remaining transactions as a bundle with suggested tip
                if txs.len() > 1 {
                    self.submit.submit_bundle(&txs[1..], suggested)?;
                }
            } else {
                // No leader, broadcast all transactions
                self.submit.submit_bundle(txs, 0)?;
            }
            
            Ok(())
        }
    }

    pub mod solana_parse {
        use std::convert::TryInto;

        #[derive(Debug, Clone)]
        pub struct ParsedTxPriority {
            pub cu_price: Option<u64>,
            pub cu_limit: Option<u64>,
            pub sig: Option<[u8; 64]>,
        }

        // ComputeBudget program id (raw bytes of Pubkey)
        const COMPUTE_BUDGET: [u8; 32] = [
            0x3a,0x9c,0x44,0x27,0x1c,0x6b,0x7d,0x0e,
            0x83,0x49,0x8f,0xba,0xf1,0x13,0x9b,0xc3,
            0x7b,0x73,0xa4,0x5c,0x59,0x5a,0x0a,0xad,
            0xef,0x01,0x2f,0x7a,0x3d,0xa9,0x2c,0x6f,
        ];

        pub fn parse_priority(tx: &[u8]) -> ParsedTxPriority {
            let mut result = ParsedTxPriority {
                cu_price: None,
                cu_limit: None,
                sig: None,
            };

            // Basic length check
            if tx.len() < 48 {
                return result;
            }

            // Extract signature if available
            if tx.len() >= 64 {
                result.sig = Some(tx[..64].try_into().unwrap());
            }

            // Determine legacy vs v0 format
            let is_v0 = tx.get(0).map(|b| *b == 1).unwrap_or(false);

            // Skip header and signatures
            let data_start = if is_v0 {
                // v0 format has additional address table lookups
                // Skip 1 byte version + 64 bytes signatures + 3 bytes header
                68
            } else {
                // legacy format: skip 64 bytes signatures + 3 bytes header
                67
            };

            if tx.len() <= data_start {
                return result;
            }

            // Extract compute budget instructions
            let account_keys_len = tx[data_start] as usize;
            let instructions_start = data_start + 1 + (account_keys_len * 32);

            if tx.len() <= instructions_start {
                return result;
            }

            let instructions_len = tx[instructions_start] as usize;
            let mut pos = instructions_start + 1;

            for _ in 0..instructions_len {
                if tx.len() <= pos + 2 {
                    break;
                }

                let program_id_idx = tx[pos] as usize;
                let accounts_len = tx[pos + 1] as usize;
                pos += 2;

                // Skip accounts
                pos += accounts_len;

                if tx.len() <= pos + 1 {
                    break;
                }

                let data_len = tx[pos] as usize;
                pos += 1;

                if program_id_idx < account_keys_len && data_len >= 8 {
                    let program_key_start = data_start + 1 + (program_id_idx * 32);
                    if program_key_start + 32 <= tx.len() {
                        let program_key = &tx[program_key_start..program_key_start + 32];
                        if program_key == COMPUTE_BUDGET {
                            let data = &tx[pos..pos + data_len];
                            if data_len >= 16 {
                                // First 8 bytes: u64 le for compute unit price
                                result.cu_price = Some(u64::from_le_bytes(data[..8].try_into().unwrap()));
                                // Next 8 bytes: u64 le for compute unit limit
                                result.cu_limit = Some(u64::from_le_bytes(data[8..16].try_into().unwrap()));
                            }
                        }
                    }
                }

                pos += data_len;
            }

            result
        }
    }

    #[test]
    fn test_power_of_two_sizes() {
        assert!(BLOCK_SIZE.is_power_of_two());
        assert!(FRAME_SIZE.is_power_of_two());
        assert_eq!(BLOCK_SIZE % FRAME_SIZE, 0);
            let header = vec![
                0x45, 0x00, 0x00, 0x54, 0x00, 0x00, 0x40, 0x00,
                0x40, 0x01, 0x00, 0x00, 0xc0, 0xa8, 0x00, 0x01,
                0xc0, 0xa8, 0x00, 0x02
            ];
            
            let checksum = kb.calculate_ip_checksum(&header);
            assert_ne!(checksum, 0);
        }

        #[test]
        fn test_power_of_two_sizes() {
            assert!(BLOCK_SIZE.is_power_of_two());
            assert!(FRAME_SIZE.is_power_of_two());
            assert_eq!(BLOCK_SIZE % FRAME_SIZE, 0);
        }
    }

    #[cfg(test)]
    mod integration_tests {
        use super::*;
        use crate::bare_metal::mock_rxtx::MockRxTx;

        #[test]
        fn test_packet_processing() {
            let mock = MockRxTx::new();
            let mut kb = KernelBypass::new(mock, "lo").unwrap();
            
            // Test would normally use mock to inject packets
            // and verify processing behavior
            assert!(kb.process_rx_batch(&mut []).is_ok());
        }
    }

    pub fn metrics_handler(&self) -> String {
        let stats = self.stats;
        format!(
            "kernel_bypass_packets_total{{type=\"rx\"}} {}\n"
            "kernel_bypass_packets_total{{type=\"tx\"}} {}\n"
            "kernel_bypass_bytes_total{{type=\"rx\"}} {}\n"
            "kernel_bypass_bytes_total{{type=\"tx\"}} {}\n"
            "kernel_bypass_drops_total {}\n",
            stats.rx_packets.load(Ordering::Relaxed), stats.tx_packets.load(Ordering::Relaxed),
            stats.rx_bytes.load(Ordering::Relaxed), stats.tx_bytes.load(Ordering::Relaxed),
            stats.rx_drops.load(Ordering::Relaxed)
        )
    }
}

impl<T: RxTx> Drop for KernelBypass<T> {
    fn drop(&mut self) {
        // Restore interface flags if we changed them
        if let Some(if_state) = self.if_state.take() {
            if !if_state.had_promisc {
                unsafe {
                    let fd = libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0);
                    if fd >= 0 {
                        let mut req: libc::ifreq = std::mem::zeroed();
                        ptr::copy_nonoverlapping(
                            self.interface_name.as_ptr(),
                            req.ifr_name.as_mut_ptr(),
                            self.interface_name.len()
                        );
                        req.ifr_flags = (if_state.orig_flags & 0xFFFF) as i16;
                        let _ = libc::ioctl(fd, libc::SIOCSIFFLAGS, &req);
                        libc::close(fd);
                    }
                }
            }
        }
        
        // Close raw socket
        unsafe { libc::close(self.raw_fd); }
        
        // Unmap any ring buffers if using AF_XDP
        if let Some(ring_ptr) = self.xdp_rings.as_ref() {
            unsafe { libc::munmap(ring_ptr.addr, ring_ptr.size); }
        }
    }
}

use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use log::{error, warn, info, debug};
use crossbeam_channel::{bounded, Sender, Receiver};
use log::{debug, warn};

/// Batches packets for efficient transmission with micro-batching and timed flush
pub struct TxBatcher<S: TxSubmitter + Send + 'static> {
    submitter: Arc<S>,
    batch_sender: Sender<Vec<u8>>,
    flush_notify: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

// Configuration for TxBatcher
#[derive(Debug, Clone, Copy)]
pub struct TxBatcherConfig {
    pub max_batch_size: usize,
    pub flush_timeout: Duration,
    pub cu_tip: u64, // Compute unit tip for Jito
}

impl Default for TxBatcherConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 32, // Optimal for most NICs
            flush_timeout: Duration::from_micros(100), // 100μs
            cu_tip: 0, // Default to no tip
        }
    }
}

impl<S: TxSubmitter + Send + 'static> TxBatcher<S> {
    /// Create a new TxBatcher with default configuration
    pub fn new(submitter: Arc<S>) -> Self {
        Self::with_config(submitter, TxBatcherConfig::default())
    }

    /// Create a new TxBatcher with custom configuration
    pub fn with_config(submitter: Arc<S>, config: TxBatcherConfig) -> Self {
        let (tx, rx) = bounded(1024); // Bounded channel to prevent unbounded memory growth
        let flush_notify = Arc::new(AtomicBool::new(false));
        let flush_notify_clone = flush_notify.clone();
        let submitter_clone = submitter.clone();

        // Spawn worker thread for batching and sending
        let handle = thread::Builder::new()
            .name("tx-batcher".into())
            .spawn(move || {
                let mut batch = Vec::with_capacity(config.max_batch_size);
                let mut last_flush = Instant::now();
                
                loop {
                    // Check for new packets with timeout
                    match rx.recv_timeout(Duration::from_micros(100)) {
                        Ok(packet) => {
                            batch.push(packet);
                            
                            // Flush if batch is full or timeout reached
                            if batch.len() >= config.max_batch_size || 
                               last_flush.elapsed() >= config.flush_timeout {
                                if let Err(e) = submitter_clone.send_batch_owned(&batch) {
                                    warn!("Failed to send batch: {}", e);
                                }
                                batch.clear();
                                last_flush = Instant::now();
                            }
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                            // Flush on timeout if we have pending packets
                            if !batch.is_empty() {
                                if let Err(e) = submitter_clone.send_batch_owned(&batch) {
                                    warn!("Failed to send batch: {}", e);
                                }
                                batch.clear();
                                last_flush = Instant::now();
                            }
                        }
                        Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                            // Channel disconnected, flush any remaining packets and exit
                            if !batch.is_empty() {
                                let _ = submitter_clone.send_batch_owned(&batch);
                            }
                            break;
                        }
                    }
                    
                    // Check for explicit flush request
                    if flush_notify_clone.swap(false, Ordering::Relaxed) {
                        if !batch.is_empty() {
                            if let Err(e) = submitter_clone.send_batch_owned(&batch) {
                                warn!("Failed to send batch: {}", e);
                            }
                            batch.clear();
                            last_flush = Instant::now();
                        }
                    }
                }
            })
            .expect("Failed to spawn TxBatcher thread");

        Self {
            submitter,
            batch_sender: tx,
            flush_notify,
            handle: Some(handle),
        }
    }

    /// Add a packet to the current batch
    pub fn add_packet(&self, packet: Vec<u8>) -> io::Result<()> {
        self.batch_sender.send(packet).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Failed to queue packet: {}", e))
        })
    }

    /// Request a flush of the current batch
    pub fn flush(&self) -> io::Result<()> {
        self.flush_notify.store(true, Ordering::Relaxed);
        self.pending.clear();
        self.last_flush = Instant::now();
        Ok(())
    }
}

use bytes::BytesMut;
use crossbeam_queue::ArrayQueue;

const PACKET_POOL_SIZE: usize = 1024;
const DEFAULT_BUFFER_SIZE: usize = 2048;

struct PacketPool {
    buffers: ArrayQueue<BytesMut>,
}

impl PacketPool {
    fn new() -> Self {
        let queue = ArrayQueue::new(PACKET_POOL_SIZE);
        for _ in 0..PACKET_POOL_SIZE {
            queue.push(BytesMut::with_capacity(DEFAULT_BUFFER_SIZE)).unwrap();
        }
        Self { buffers: queue }
    }

    fn acquire(&self) -> Option<BytesMut> {
        self.buffers.pop()
    }

    fn release(&self, mut buf: BytesMut) {
        buf.clear();
        let _ = self.buffers.push(buf);
    }
}

use crossbeam_queue::ArrayQueue;
use bytes::BytesMut;

const TX_BUFFER_POOL_SIZE: usize = 1024;
const MTU_SIZE: usize = 1500;

struct TxBufferPool {
    buffers: ArrayQueue<BytesMut>,
}

impl TxBufferPool {
    fn new() -> Self {
        let queue = ArrayQueue::new(TX_BUFFER_POOL_SIZE);
        for _ in 0..TX_BUFFER_POOL_SIZE {
            queue.push(BytesMut::with_capacity(MTU_SIZE)).unwrap();
        }
        Self { buffers: queue }
    }

    fn acquire(&self) -> Option<BytesMut> {
        self.buffers.pop()
    }

    fn release(&self, mut buf: BytesMut) {
        buf.clear();
        let _ = self.buffers.push(buf);
    }
}

pub struct XdpPath {
    // Will contain:
    // - UMEM configuration
    // - Fill/Completion rings
    // - RX/TX rings
    // - Queue ID
    _priv: (), // Marker for future fields
}

impl XdpPath {
    pub fn new(_ifname: &str, _queue_id: u32) -> io::Result<Self> {
        // TODO: Implement actual AF_XDP setup:
        // 1. Create and configure UMEM
        // 2. Setup fill/completion rings
        // 3. Bind socket to queue
        // 4. Configure zero-copy mode
        Ok(Self { _priv: () })
    }
}

impl RxTx for XdpPath {
    fn rx_batch(&mut self, _out: &mut [Vec<u8>]) -> usize {
        // TODO: Implement AF_XDP RX batch
        0
    }

    fn tx_batch(&mut self, _inpkts: &[&[u8]]) -> usize {
        // TODO: Implement AF_XDP TX batch
        0
    }
}

#[cfg(target_os = "linux")]
extern "C" {
    fn numa_available() -> i32;
    fn numa_max_node() -> i32;
    fn numa_node_of_cpu(cpu: i32) -> i32;
    fn numa_run_on_node(node: i32) -> i32;
}

fn get_numa_node_for_device(iface: &str) -> Option<i32> {
    #[cfg(target_os = "linux")]
    unsafe {
        if numa_available() < 0 { return None; }
        
        let path = format!("/sys/class/net/{}/device/numa_node", iface);
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .filter(|&n| n >= 0)
    }
    #[cfg(not(target_os = "linux"))]
    None
}

fn bind_to_numa_node(iface: &str, thread_name: &str) -> io::Result<()> {
    if let Some(node) = get_numa_node_for_device(iface) {
        #[cfg(target_os = "linux")]
        unsafe {
            if numa_run_on_node(node) == 0 {
                log::info!("Bound {} to NUMA node {}", thread_name, node);
            }
        }
    }

    // Existing CPU pinning logic
    let cpu = match thread_name {
        "rx" => 0,
        "tx" => 1,
        _ => return Ok(()),
    };
    set_thread_affinity(cpu)
}

fn set_thread_affinity(core_id: usize) -> io::Result<()> {
    let mut cpu_set = unsafe { std::mem::zeroed::<libc::cpu_set_t>() };
    unsafe { libc::CPU_SET(core_id, &mut cpu_set); }
    
    // SAFETY: Valid cpu_set pointer
    if unsafe { libc::sched_setaffinity(0, std::mem::size_of_val(&cpu_set), &cpu_set) } < 0 {
        return Err(io::Error::last_os_error());
    }
    
    // Set real-time priority
    let param = libc::sched_param {
        sched_priority: 99,
    };
    
    // SAFETY: Valid sched_param
    if unsafe { libc::sched_setscheduler(0, libc::SCHED_FIFO, &param) } < 0 {
        return Err(io::Error::last_os_error());
    }
    
    Ok(())
}

pub struct TpacketBlockDesc {
    // ... other fields
}

impl TpacketBlockDesc {
    pub fn status(&self) -> u32 {
        // ... implementation
    }

    pub fn hdr_ptr(&self) -> *const u8 {
        // ... implementation
    }

    pub fn offset_to_first_pkt(&self) -> u32 {
        // ... implementation
    }

    pub fn packets(&self) -> Vec<&[u8]> {
        // ... implementation
    }
}

impl<T: RxTx> KernelBypass<T> {
    pub fn process_rx_block(&mut self, block: &TpacketBlockDesc) -> io::Result<usize> {
        use std::ptr;

        // Check if block is ready for processing
        if block.status() != TP_STATUS_USER {
            return Ok(0);
        }

        // Get all packets from the block once and store them
        let packets = block.packets();
        if packets.is_empty() {
            return Ok(0);
        }

        // Prefetch the first packet if we have any packets
        if !packets.is_empty() {
            #[cfg(target_arch = "x86_64")]
            unsafe {
                use std::arch::x86_64::_mm_prefetch as prefetch;
                let hdr_ptr = block.hdr_ptr();
                let first_pkt = hdr_ptr.add(block.offset_to_first_pkt() as usize);
                prefetch(first_pkt as *const i8, 0); // _MM_HINT_T0
            }
        }

        let mut processed = 0usize;
        
        // Process each packet in the block
        for packet in packets {
            // Try to get a buffer from the pool
            if let Some(mut buf) = self.packet_pool.acquire() {
                // Clear any existing data and copy in the new packet
                buf.clear();
                buf.extend_from_slice(packet);
                
                // Convert to immutable Bytes and queue for transmission
                match self.tx_queue.push(buf.freeze()) {
                    Ok(()) => processed += 1,
                    Err(e) => {
                        // Count drops if the queue is full
                        self.stats.rx_drops.fetch_add(1, Ordering::Relaxed);
                        log::debug!("TX queue full, dropping packet: {}", e);
                    }
                }
            } else {
                // No buffer available - count drop
                self.stats.rx_drops.fetch_add(1, Ordering::Relaxed);
                log::debug!("No packet buffers available, dropping packet");
            }
        }
        
        Ok(processed)
    }
}
