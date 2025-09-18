use std::sync::{Arc, RwLock, atomic::{AtomicBool, AtomicU64, AtomicU32, Ordering}};
// ADD
use std::sync::atomic::{AtomicU32, Ordering, AtomicI32, AtomicI64};
// ADD
use std::sync::OnceLock;
// ADD: io_uring support
#[cfg(target_os = "linux")]
use io_uring::{opcode, types, IoUring};

// ADD: Build-time ABI guard for io_uring availability
#[cfg(target_os = "linux")]
const _: () = {
    // io_uring opcode numbers we rely on
    use io_uring::opcode;
    let _ = opcode::SendMsgZc::CODE;
};

// ADD: Missing constants
const DSCP_FIRST: u8 = 46;  // CS6 (Expedited Forwarding)
const DSCP_OTHERS: u8 = 0;  // Best Effort
const PACING_BPS_MIN: u64 = 1_000_000; // 1 Mbps minimum
// ADD: parking_lot for faster mutexes
use parking_lot::Mutex;

// ADD: PM QoS hold to keep cores out of deep C-states
static CPU_DMA_LAT_FD: OnceLock<std::fs::File> = OnceLock::new();

// ADD: io_uring ring state
#[cfg(target_os = "linux")]
static URING: OnceLock<IoUring> = OnceLock::new();

#[cfg(target_os = "linux")]
#[inline]
fn uring_available() -> bool {
    URING.get_or_init(|| IoUring::new(256).expect("io_uring")); true
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn uring_available() -> bool { false }

// ADD: Hard real-time scheduling constants and structs
#[cfg(target_os = "linux")]
const SCHED_DEADLINE: u32 = 6;
#[cfg(target_os = "linux")]
const SCHED_FLAG_RESET_ON_FORK: u64 = 0x01;

#[cfg(target_os = "linux")]
#[repr(C)]
struct SchedAttr {
    size: u32,
    sched_policy: u32,
    sched_flags: u64,
    sched_nice: i32,
    sched_priority: u32,
    // DL fields (ns)
    sched_runtime: u64,
    sched_deadline: u64,
    sched_period: u64,
}

static RT_INIT_ONCE: OnceLock<()> = OnceLock::new();

// ADD: Hard real-time scheduling helper
#[cfg(target_os = "linux")]
#[inline]
fn elevate_rt_scheduling() {
    let _ = RT_INIT_ONCE.get_or_init(|| unsafe {
        // Try SCHED_DEADLINE first: runtime/period tuned for sub-ms bursts
        let mut ok = false;
        let attr = SchedAttr {
            size: std::mem::size_of::<SchedAttr>() as u32,
            sched_policy: SCHED_DEADLINE,
            sched_flags: SCHED_FLAG_RESET_ON_FORK,
            sched_nice: 0,
            sched_priority: 0,
            sched_runtime: 150_000,   // 150 µs runtime
            sched_deadline: 500_000,  // 0.5 ms deadline
            sched_period: 500_000,    // 0.5 ms period
        };
        let r = libc::syscall(libc::SYS_sched_setattr, 0, &attr, 0);
        if r == 0 { ok = true; }

        if !ok {
            // Fallback SCHED_FIFO with high prio (no gotchas, no starvation of ksoftirqd)
            let mut sp: libc::sched_param = std::mem::zeroed();
            sp.sched_priority = 90; // below ksoftirqd-RT on tuned boxes
            let _ = libc::sched_setscheduler(0, libc::SCHED_FIFO, &sp);
        }
    });
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn elevate_rt_scheduling() {}

// ADD: mlockall helper for no page faults
#[cfg(target_os = "linux")]
#[inline]
fn lock_process_memory() {
    unsafe {
        let _ = libc::mlockall(libc::MCL_CURRENT | libc::MCL_FUTURE);
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn lock_process_memory() {}

// ADD: IP_BIND_ADDRESS_NO_PORT helper for instant ephemeral binds
#[cfg(target_os = "linux")]
#[inline]
fn enable_bind_no_port(sock: &Socket, remote: &SocketAddr) {
    unsafe {
        let one: libc::c_int = 1;
        match remote {
            SocketAddr::V4(_) => {
                // IP_BIND_ADDRESS_NO_PORT = 24
                let _ = libc::setsockopt(
                    sock.as_raw_fd(), libc::IPPROTO_IP, 24,
                    &one as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t
                );
            }
            SocketAddr::V6(_) => {
                // IPV6_BIND_ADDRESS_NO_PORT = 24
                let _ = libc::setsockopt(
                    sock.as_raw_fd(), libc::IPPROTO_IPV6, 24,
                    &one as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t
                );
            }
        }
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn enable_bind_no_port(_sock: &Socket, _remote: &SocketAddr) {}

// ADD: SO_PRIORITY constants and helper
const ENABLE_SO_PRIORITY_PER_ALT: bool = true;
const SO_PRIO_PRIMARY: libc::c_int = 6; // elite lane
const SO_PRIO_ALT: libc::c_int     = 5; // slightly lower

// ADD: Adaptive busy poll constants
const ENABLE_ADAPT_BUSY_POLL_USEC: bool = true;
const BUSY_POLL_COOL_USEC: i32 = 10;
const BUSY_POLL_HOT_USEC:  i32 = 50;
const BUSY_POLL_VHOT_USEC: i32 = 80;
const BUSY_POLL_Q_HOT_NS: u64 = 100_000;  // 100µs
const BUSY_POLL_Q_VHOT_NS: u64 = 200_000; // 200µs

// ADD: Errqueue headroom constants
const ENABLE_RCVBUFFORCE: bool = true;
const RCVBUF_TARGET_BYTES: i32 = 16 * 1024 * 1024; // 16 MiB

// ADD: Tight window duplicate thinning constants
const ENABLE_TIGHT_WINDOW_TRIM: bool = true;
const DUP_TIGHT_NS: u64 = 40_000; // 40 µs window: if less than this, send only the elite

// ADD: DSCP booster constants
const ENABLE_DSCP_BOOST_ON_ADVERSITY: bool = true;
const DSCP_BOOST_DELTA: u8 = 2;       // elite +2 DSCP during boost
const DSCP_BOOST_NS: u64   = 150_000_000; // 150 ms boost window

// ADD: RDTSCP TSC constants
const ENABLE_RDTSCP_TSC: bool = true;

// ADD: Block signals in burst threads
#[cfg(target_os = "linux")]
#[inline]
fn block_all_signals() {
    unsafe {
        let mut set: libc::sigset_t = std::mem::zeroed();
        libc::sigfillset(&mut set);
        // Leave SIGKILL/SIGSTOP alone; pthread_sigmask ignores them.
        let _ = libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn block_all_signals() {}

// ADD: Disable perf events for burst threads
#[cfg(target_os = "linux")]
const PR_TASK_PERF_EVENTS_DISABLE: libc::c_int = 31;

#[cfg(target_os = "linux")]
#[inline]
fn disable_perf_events_this_thread() {
    unsafe { let _ = libc::prctl(PR_TASK_PERF_EVENTS_DISABLE, 0, 0, 0, 0); }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn disable_perf_events_this_thread() {}

// ADD: IPv6 dont fragment hardening
#[cfg(target_os = "linux")]
#[inline]
fn set_v6_dontfrag(sock: &Socket) {
    unsafe {
        let one: libc::c_int = 1;
        // IPV6_DONTFRAG = 62
        let _ = libc::setsockopt(
            sock.as_raw_fd(), libc::IPPROTO_IPV6, 62,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t
        );
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn set_v6_dontfrag(_sock: &Socket) {}

// ADD: Opportunistic errqueue micro-drain
thread_local! {
    static SEND_TICK: std::cell::Cell<u32> = std::cell::Cell::new(0);
}
const ERRQUEUE_DRAIN_EVERY: u32 = 64;

#[cfg(target_os = "linux")]
#[inline]
fn occasional_errqueue_microdrain(fd: libc::c_int) {
    SEND_TICK.with(|c| {
        let v = c.get().wrapping_add(1);
        c.set(v);
        if (v % ERRQUEUE_DRAIN_EVERY) == 0 {
            errqueue_drain_once_fd(fd, ERRQUEUE_RETRY_BATCH);
        }
    });
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn occasional_errqueue_microdrain(_fd: libc::c_int) {}

// ADD: Lock code+rodata pages
#[cfg(target_os = "linux")]
#[inline]
fn mlock_text_rodata(max_bytes: usize) {
    use std::io::Read;
    if let Ok(mut f) = std::fs::File::open("/proc/self/maps") {
        let mut s = String::new(); let _ = f.read_to_string(&mut s);
        let mut locked = 0usize;
        for line in s.lines() {
            // match r-xp or r--p (ELF text/rodata), ignore huge shared libs by cap
            if (line.contains(" r-xp ") || line.contains(" r--p ")) && !line.contains(" [vsyscall]") {
                let mut parts = line.split_whitespace();
                if let Some(range) = parts.next() {
                    let mut it = range.split('-');
                    if let (Some(a), Some(b)) = (usize::from_str_radix(it.next().unwrap(), 16).ok(),
                                                 usize::from_str_radix(it.next().unwrap(), 16).ok()) {
                        let len = b.saturating_sub(a);
                        if locked.saturating_add(len) > max_bytes { break; }
                        unsafe { let _ = libc::mlock(a as *const _, len); }
                        locked = locked.saturating_add(len);
                    }
                }
            }
        }
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn mlock_text_rodata(_max_bytes: usize) {}

// ADD: IPv6 unicast ifindex for route clarity
#[cfg(target_os = "linux")]
#[inline]
fn set_v6_unicast_if(sock: &Socket, ifname: &Arc<str>) {
    unsafe {
        let mut idx: libc::c_uint = 0;
        if let Ok(cs) = std::ffi::CString::new(&**ifname) {
            let n = libc::if_nametoindex(cs.as_ptr());
            if n > 0 { idx = n as libc::c_uint; }
        }
        if idx > 0 {
            // IPV6_UNICAST_IF = 76
            let _ = libc::setsockopt(
                sock.as_raw_fd(), libc::IPPROTO_IPV6, 76,
                &idx as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_uint>() as libc::socklen_t
            );
        }
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn set_v6_unicast_if(_sock: &Socket, _ifname: &Arc<str>) {}

// ADD: Fast current CPU for metrics/XPS sanity
#[inline]
fn fast_current_cpu() -> u32 {
    #[cfg(target_os = "linux")]
    unsafe {
        let cpu = libc::sched_getcpu();
        if cpu >= 0 { return cpu as u32; }
    }
    0
}

// ADD: Raise NOFILE limit
#[cfg(target_os = "linux")]
#[inline]
fn raise_nofile_limit(want: u64) {
    unsafe {
        let mut r: libc::rlimit = libc::rlimit { rlim_cur: want, rlim_max: want };
        let _ = libc::setrlimit(libc::RLIMIT_NOFILE, &r);
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn raise_nofile_limit(_want: u64) {}

// ADD: Payload + control prefetch
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
#[inline(always)]
fn prefetch_t0(p: *const u8) { 
    unsafe { 
        core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_T0) 
    } 
}
#[cfg(not(any(target_arch = "x86_64", target_arch = "x86")))]
#[inline(always)]
fn prefetch_t0(_p: *const u8) {}

// ADD: TX timestamping constants
#[cfg(target_os = "linux")]
const SO_TIMESTAMPING: libc::c_int = 37;
#[cfg(target_os = "linux")]
const SOF_TS_TX_SW: i32  = 1<<1;
#[cfg(target_os = "linux")]
const SOF_TS_TX_HW: i32  = 1<<2;
#[cfg(target_os = "linux")]
const SOF_TS_SOFT:  i32  = 1<<4;
#[cfg(target_os = "linux")]
const SOF_TS_RAWHW:i32   = 1<<6;

#[cfg(target_os = "linux")]
#[inline]
fn set_socket_priority(sock: &Socket, prio: libc::c_int) {
    unsafe {
        let _ = libc::setsockopt(
            sock.as_raw_fd(), libc::SOL_SOCKET, libc::SO_PRIORITY,
            &prio as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t
        );
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn set_socket_priority(_sock: &Socket, _prio: libc::c_int) {}

// ADD: Adaptive busy poll time adjustment
#[cfg(target_os = "linux")]
#[inline]
fn maybe_adjust_busy_poll_time(fd: libc::c_int, st: &ConnectionState, metrics: &Metrics) {
    if !ENABLE_ADAPT_BUSY_POLL_USEC { return; }
    let want = if let Some(dev) = st.bound_device.as_ref() {
        if let Some(ds) = metrics.device_stats.get(dev) {
            let q = ds.avg_tx_queue_ns.load(Ordering::Relaxed);
            if q >= BUSY_POLL_Q_VHOT_NS { BUSY_POLL_VHOT_USEC }
            else if q >= BUSY_POLL_Q_HOT_NS { BUSY_POLL_HOT_USEC }
            else { BUSY_POLL_COOL_USEC }
        } else { BUSY_POLL_COOL_USEC }
    } else { BUSY_POLL_COOL_USEC };
    unsafe {
        // SO_BUSY_POLL = 46
        let _ = libc::setsockopt(
            fd, libc::SOL_SOCKET, 46,
            &want as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn maybe_adjust_busy_poll_time(_fd: libc::c_int, _st: &ConnectionState, _m: &Metrics) {}

// ADD: Enable robust TX timestamping
#[cfg(target_os = "linux")]
#[inline]
fn enable_tx_timestamping(sock: &Socket) {
    unsafe {
        // Request SW+HW TX stamps; RAWHW gives NIC clock when available.
        let flags: libc::c_int = SOF_TS_TX_SW | SOF_TS_TX_HW | SOF_TS_SOFT | SOF_TS_RAWHW;
        let _ = libc::setsockopt(
            sock.as_raw_fd(), libc::SOL_SOCKET, SO_TIMESTAMPING,
            &flags as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn enable_tx_timestamping(_sock: &Socket) {}

// ADD: Raise RLIMITs to make RT & mlock unfailable
#[cfg(target_os = "linux")]
#[inline]
fn raise_rt_mem_limits() {
    unsafe {
        // RLIMIT_MEMLOCK → allow mlockall/huge slabs
        let mut r: libc::rlimit = libc::rlimit { rlim_cur: 1<<30, rlim_max: 1<<30 }; // 1 GiB
        let _ = libc::setrlimit(libc::RLIMIT_MEMLOCK, &r);
        // RLIMIT_RTPRIO → allow RT FIFO/DL elevation
        let mut rr: libc::rlimit = libc::rlimit { rlim_cur: 95, rlim_max: 95 };
        let _ = libc::setrlimit(libc::RLIMIT_RTPRIO, &rr);
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn raise_rt_mem_limits() {}

// ADD: PM QoS hold to keep cores out of deep C-states
#[cfg(target_os = "linux")]
#[inline]
fn hold_cpu_dma_latency(ns: u32) {
    // Write any value (e.g., 0) to keep package out of deep C-states.
    // Kernel uses the fd lifetime to maintain the constraint.
    let _ = CPU_DMA_LAT_FD.get_or_init(|| {
        std::fs::OpenOptions::new()
            .read(true).write(true).open("/dev/cpu_dma_latency")
            .and_then(|mut f| {
                use std::io::Write;
                let _ = f.write_all(&ns.to_le_bytes());
                Ok(f)
            }).unwrap_or_else(|_| std::fs::File::open("/dev/null").unwrap())
    });
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn hold_cpu_dma_latency(_ns: u32) {}

// ADD: Errqueue headroom functions
#[cfg(target_os = "linux")]
#[inline]
fn try_force_rcvbuf(sock: &Socket, bytes: i32) {
    unsafe {
        // SO_RCVBUFFORCE = 33
        let _ = libc::setsockopt(
            sock.as_raw_fd(), libc::SOL_SOCKET, 33,
            &bytes as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn try_force_rcvbuf(_sock: &Socket, _bytes: i32) {}

#[cfg(target_os = "linux")]
#[inline]
fn enable_recv_error(sock: &Socket, remote: &SocketAddr) {
    unsafe {
        let one: libc::c_int = 1;
        match remote {
            SocketAddr::V4(_) => {
                // IP_RECVERR = 11
                let _ = libc::setsockopt(
                    sock.as_raw_fd(), libc::IPPROTO_IP, 11,
                    &one as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
            SocketAddr::V6(_) => {
                // IPV6_RECVERR = 25
                let _ = libc::setsockopt(
                    sock.as_raw_fd(), libc::IPPROTO_IPV6, 25,
                    &one as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn enable_recv_error(_sock: &Socket, _remote: &SocketAddr) {}

// ADD: NUMA rehome functions
#[cfg(target_os = "linux")]
#[inline]
fn move_region_to_node(ptr: *const u8, len: usize, node: i32) {
    if node < 0 || len == 0 { return; }
    unsafe {
        let page = 4096usize;
        let np = (len + page - 1) / page;
        if np == 0 { return; }
        // Build page array
        let mut addrs: Vec<*mut libc::c_void> = Vec::with_capacity(np);
        for i in 0..np {
            addrs.push(ptr.add(i * page) as *mut libc::c_void);
        }
        let nodes: Vec<i32> = vec![node; np];
        let mut status: Vec<i32> = vec![-1; np];
        // MPOL_MF_MOVE = 1
        let _ = libc::move_pages(0, np as libc::c_ulong, addrs.as_mut_ptr(),
                                 nodes.as_ptr(), status.as_mut_ptr(), 1);
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn move_region_to_node(_ptr: *const u8, _len: usize, _node: i32) {}

// ADD: Stack fast-path extended to ≤4 copies
#[cfg(target_os = "linux")]
#[inline]
fn linux_sendmmsg_txtime_stack4(
    fd: libc::c_int,
    buf: &[u8],
    copies: usize, // 1..=4
    first_txtime_ns: u64,
    spacing_ns: u64,
    is_v6: bool,
    seg: u16, // 0 if none
    extra_flags: libc::c_int,
    dscp_first: u8,
    dscp_others: u8,
) -> std::io::Result<usize> {
    use libc::{mmsghdr, msghdr, iovec, cmsghdr};
    debug_assert!(copies >= 1 && copies <= 4);

    let mut bufs: [iovec; 4] = [iovec { iov_base: buf.as_ptr() as *mut _, iov_len: buf.len() }; 4];
    let mut ctrls: [[u8; 3*64]; 4] = [[0u8; 3*64]; 4];
    let mut hdrs: [msghdr; 4] = unsafe { std::mem::zeroed() };
    let mut msgs: [mmsghdr; 4] = unsafe { std::mem::zeroed() };

    for i in 0..copies {
        let c = ctrls[i].as_mut_ptr() as *mut cmsghdr;
        unsafe {
            // TXTIME
            (*c).cmsg_level = libc::SOL_SOCKET;
            (*c).cmsg_type  = SO_TXTIME_CONST;
            (*c).cmsg_len   = CMSG_LEN(std::mem::size_of::<u64>() as u32) as usize;
            let d1 = CMSG_DATA(c) as *mut u64;
            *d1 = first_txtime_ns + (i as u64) * spacing_ns;

            let mut next = (c as usize + (*c).cmsg_len + std::mem::size_of::<cmsghdr>()) as *mut cmsghdr;

            if seg > 0 {
                (*next).cmsg_level = libc::IPPROTO_UDP;
                (*next).cmsg_type  = 103;
                (*next).cmsg_len   = CMSG_LEN(std::mem::size_of::<libc::c_ushort>() as u32) as usize;
                let d2 = CMSG_DATA(next) as *mut libc::c_ushort;
                *d2 = seg;
                next = (next as usize + (*next).cmsg_len + std::mem::size_of::<cmsghdr>()) as *mut cmsghdr;
            }

            // DSCP/TCLASS
            let ecn = 0b10;
            let tos_byte: libc::c_int = (((if i == 0 { dscp_first } else { dscp_others }) << 2) | ecn) as libc::c_int;
            (*next).cmsg_level = if is_v6 { libc::IPPROTO_IPV6 } else { libc::IPPROTO_IP };
            (*next).cmsg_type  = if is_v6 { 67 } else { libc::IP_TOS };
            (*next).cmsg_len   = CMSG_LEN(std::mem::size_of::<libc::c_int>() as u32) as usize;
            let d3 = CMSG_DATA(next) as *mut libc::c_int;
            *d3 = tos_byte;
        }

        hdrs[i].msg_iov = &mut bufs[i] as *mut iovec;
        hdrs[i].msg_iovlen = 1;
        hdrs[i].msg_control = ctrls[i].as_mut_ptr() as *mut libc::c_void;
        hdrs[i].msg_controllen = ctrls[i].len();

        msgs[i].msg_hdr = hdrs[i];
        msgs[i].msg_len = 0;
    }

    let cnt = copies as u32;
    let r = sendmmsg_loop(fd, msgs.as_mut_ptr(), cnt, libc::MSG_DONTWAIT | extra_flags)?;
    Ok(r as usize)
}

// ADD: GSO cap loader
#[cfg(target_os = "linux")]
#[inline]
fn load_gso_cap(ifname: &Arc<str>) -> u32 {
    let p = format!("/sys/class/net/{}/gso_max_segs", ifname);
    if let Ok(s) = std::fs::read_to_string(&p) {
        if let Ok(v) = s.trim().parse::<u32>() { return v.max(1); }
    }
    64
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn load_gso_cap(_ifname: &Arc<str>) -> u32 { 64 }

// ADD: NUMA helpers
#[cfg(target_os = "linux")]
#[inline]
fn numa_node_of_dev(ifname: &Arc<str>) -> i32 {
    let p = format!("/sys/class/net/{}/device/numa_node", ifname);
    if let Ok(s) = std::fs::read_to_string(&p) {
        if let Ok(v) = s.trim().parse::<i32>() { return v; }
    }
    -1
}

#[cfg(target_os = "linux")]
#[inline]
fn prefer_numa_node(node: i32) {
    // set_mempolicy(MPOL_PREFERRED, nodemask, maxnode)
    // minimal nodemask for <=1024 CPUs; OK for common deployments
    if node < 0 { return; }
    unsafe {
        let word = node as usize / 64;
        let bit  = node as usize % 64;
        let mut mask = vec![0u64; word + 1];
        mask[word] = 1u64 << bit;
        let _ = libc::set_mempolicy(1 /*MPOL_PREFERRED*/, mask.as_ptr() as *const libc::c_ulong, (mask.len()*64) as libc::c_ulong);
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn prefer_numa_node(_node: i32) {}

// ADD: Carrier guard helper
#[inline]
fn nic_carrier_up_cached(st: &ConnectionState) -> bool {
    let now = tsc_now_ns();
    let last = st.last_carrier_check_ns.load(Ordering::Relaxed);
    if now.saturating_sub(last) < 200_000_000 { // 200ms
        return st.carrier_up_cached.load(Ordering::Relaxed);
    }
    if let Some(dev) = st.bound_device.as_ref() {
        let p = format!("/sys/class/net/{}/carrier", dev);
        if let Ok(s) = std::fs::read_to_string(&p) {
            let up = s.trim() == "1";
            st.carrier_up_cached.store(up, Ordering::Relaxed);
            st.last_carrier_check_ns.store(now, Ordering::Relaxed);
            return up;
        }
    }
    st.last_carrier_check_ns.store(now, Ordering::Relaxed);
    true
}

// ADD: Best UDP segment cap calculation with 64B alignment
#[inline(always)]
fn best_udp_seg_cap(payload_len: usize, is_v6: bool, path_mtu: Option<u32>, cap: u32) -> u16 {
    let hdr = if is_v6 { 48 } else { 28 };
    let mtu = path_mtu.unwrap_or(if is_v6 { 1280 } else { 576 }) // V6_MIN_MTU/V4_MIN_MTU
                      .max(if is_v6 { 1280 } else { 576 });
    if (mtu as usize) <= hdr { return 0; }
    let max_seg = ((mtu as usize) - hdr).min(payload_len).max(1);
    let mut seg = gcd_u32(payload_len as u32, max_seg as u32) as usize;
    let cap = cap.max(1) as usize;
    let max_by_cnt = (payload_len + cap - 1) / cap;
    if seg < max_by_cnt { seg = max_by_cnt; }
    
    // Align down to 64B to match DMA/cacheline, without going to zero
    seg = (seg / 64).max(1) * 64;
    if seg > max_seg { seg = max_seg; }
    
    seg as u16
}

// ADD: GCD helper for segment calculation
#[inline]
fn gcd_u32(a: u32, b: u32) -> u32 {
    let mut a = a;
    let mut b = b;
    while b != 0 {
        let temp = b;
        b = a % b;
        a = temp;
    }
    a
}

// ADD: Proactive PMTU refresh
#[cfg(target_os = "linux")]
#[inline]
fn refresh_path_mtu(fd: libc::c_int, is_v6: bool, st: &ConnectionState) {
    unsafe {
        if is_v6 {
            // IPV6_PATHMTU = 61
            #[repr(C)] struct ip6_mtuinfo { addr: libc::sockaddr_in6, mtu: u32 }
            let mut mi: ip6_mtuinfo = std::mem::zeroed();
            let mut l = std::mem::size_of::<ip6_mtuinfo>() as libc::socklen_t;
            if libc::getsockopt(fd, libc::IPPROTO_IPV6, 61, &mut mi as *mut _ as *mut _, &mut l) == 0 {
                if mi.mtu >= 1280 { st.path_mtu_v6.store(mi.mtu, Ordering::Relaxed); } // V6_MIN_MTU
            }
        } else {
            // IP_MTU = 14
            let mut mtu: libc::c_int = 0;
            let mut l = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
            if libc::getsockopt(fd, libc::IPPROTO_IP, 14, &mut mtu as *mut _ as *mut _, &mut l) == 0 {
                if (mtu as u32) >= 576 { st.path_mtu_v4.store(mtu as u32, Ordering::Relaxed); } // V4_MIN_MTU
            }
        }
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn refresh_path_mtu(_fd: libc::c_int, _is_v6: bool, _st: &ConnectionState) {}

// ADD: Build-time assertions for header sizes
const _: () = {
    // UDP_SEGMENT expects u16
    assert!(std::mem::size_of::<libc::c_ushort>() == 2);
    // cmsghdr alignment sanity: kernel expects natural alignment
    assert!(std::mem::size_of::<libc::cmsghdr>() >= 12);
    // SO_TIMESTAMPING returns 3 timespecs
    assert!(std::mem::size_of::<libc::timespec>() >= 16);
    // cmsghdr alignment and size sanity
    assert!(std::mem::size_of::<libc::cmsghdr>() % std::mem::size_of::<usize>() == 0);
    // SO_TIMESTAMPING returns 3 timespecs; ensure space assumptions won't underflow
    assert!(3 * std::mem::size_of::<libc::timespec>() <= 3 * 24);
};

use std::os::fd::AsRawFd;
use std::time::SystemTime;
use std::io;
// ADD
use std::hint::spin_loop;
// ADD
use std::fs;
#[cfg(target_arch = "x86_64")]
use core::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
use core::arch::x86_64::_rdtscp;
use std::cell::RefCell;
use bytes::Bytes;

#[cfg(target_os = "linux")]
use libc::{mmsghdr, msghdr, iovec, cmsghdr, CMSG_FIRSTHDR, CMSG_DATA, CMSG_LEN, CMSG_SPACE, recvmmsg, timespec as libc_timespec, sched_param, sched_setscheduler, sched_getscheduler, sched_getparam, mlockall, munlockall, MCL_CURRENT, MCL_FUTURE, PR_SET_TIMERSLACK, if_nametoindex, SIOCOUTQ, SIOCOUTQNSD, c_int, mbind, MADV_WILLNEED, MPOL_PREFERRED, SO_PREFER_BUSY_POLL, move_pages, syscall, epoll_create1, epoll_ctl, epoll_wait, epoll_event, EPOLL_CLOEXEC, EPOLL_CTL_ADD, EPOLLIN, EPOLLPRI, EPOLLERR, ioctl, IFNAMSIZ, mlock, sched_setaffinity};

// ADD
#[cfg(target_os = "linux")]
const IPV6_FLOWLABEL_MGR: libc::c_int = 32; // setsockopt(IPPROTO_IPV6)

#[cfg(target_os = "linux")]
const IPV6_FL_A_GET: u8   = 0;
#[cfg(target_os = "linux")]
const IPV6_FL_A_PUT: u8   = 1;
#[cfg(target_os = "linux")]
const IPV6_FL_A_RENEW: u8 = 2;

#[cfg(target_os = "linux")]
const IPV6_FL_F_CREATE: u16 = 0x0001;
#[cfg(target_os = "linux")]
const IPV6_FL_F_EXCL:   u16 = 0x0002;

#[cfg(target_os = "linux")]
const IPV6_FL_S_EXCL: u8 = 1;

#[cfg(target_os = "linux")]
#[repr(C)]
struct In6FlowlabelReq {
    flr_dst: libc::in6_addr,  // destination IPv6
    flr_label: u32,           // high 20 bits used; network byte order
    flr_action: u8,           // ADD/RENEW/PUT
    flr_share: u8,            // EXCL
    flr_flags: u16,           // CREATE|EXCL
    flr_expires: u16,         // seconds
    flr_linger: u16,          // seconds
    __flr_pad: u32,
}

// ADD
#[cfg(target_os = "linux")]
const SIOCETHTOOL: libc::c_ulong = 0x8946;

#[cfg(target_os = "linux")]
#[repr(C)]
struct EthtoolCoalesce {
    cmd: u32,
    rx_coalesce_usecs: u32,
    rx_max_coalesced_frames: u32,
    rx_coalesce_usecs_irq: u32,
    rx_max_coalesced_frames_irq: u32,
    tx_coalesce_usecs: u32,
    tx_max_coalesced_frames: u32,
    tx_coalesce_usecs_irq: u32,
    tx_max_coalesced_frames_irq: u32,
    stats_block_coalesce_usecs: u32,
    use_adaptive_rx_coalesce: u32,
    use_adaptive_tx_coalesce: u32,
    pkt_rate_low: u32, pkt_rate_high: u32,
    rx_coalesce_usecs_low: u32, rx_coalesce_usecs_high: u32,
    tx_coalesce_usecs_low: u32, tx_coalesce_usecs_high: u32,
    rx_max_coalesced_frames_low: u32, rx_max_coalesced_frames_high: u32,
    tx_max_coalesced_frames_low: u32, tx_max_coalesced_frames_high: u32,
    rate_sample_interval: u32,
    // kernel has more fields; zeroed tail is fine
}

#[cfg(target_os = "linux")]
#[repr(C)]
struct IfReq {
    ifr_name: [libc::c_char; libc::IFNAMSIZ],
    ifr_data: *mut libc::c_void,
}

// ADD
#[cfg(any(target_os = "linux", target_os = "android"))]
use libc::cmsghdr as libc_cmsghdr;
#[cfg(target_os = "linux")]
use std::os::fd::AsRawFd;
#[cfg(target_os = "linux")]
use std::ptr;
#[cfg(target_os = "linux")]
use std::mem::size_of;
use bytes::Bytes;
use std::collections::{HashMap, VecDeque, BTreeMap};
use std::net::{SocketAddr, UdpSocket, IpAddr, Ipv4Addr};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex, Semaphore, RwLock as TokioRwLock};
use tokio::time::{interval, timeout, sleep};
use tokio::io::AsyncWriteExt;
use socket2::{Socket, Domain, Type as SockType, Protocol as SockProtocol};
use solana_client::{
    rpc_client::RpcClient,
    nonblocking::rpc_client::RpcClient as NonblockingRpcClient,
    tpu_client::{TpuClient, TpuClientConfig},
    connection_cache::{ConnectionCache, DEFAULT_TPU_CONNECTION_POOL_SIZE},
};
use solana_sdk::{
    pubkey::Pubkey,
    transaction::Transaction,
    signature::{Signature, Keypair},
    commitment_config::{CommitmentConfig, CommitmentLevel},
    clock::Slot,
    epoch_info::EpochInfo,
};
use solana_gossip::cluster_info::ClusterInfo;
use solana_streamer::socket::SocketAddrSpace;
use solana_net_utils::MINIMUM_VALIDATOR_PORT_RANGE_WIDTH;
use quinn::{ClientConfig, Endpoint, Connection as QuicConnection, SendStream};
use rustls::ClientConfig as RustlsClientConfig;
use bincode::serialize;
use dashmap::DashMap;
use rand::{thread_rng, Rng, seq::SliceRandom};
use futures::future::join_all;
use once_cell::sync::Lazy;
use crossbeam_channel::{bounded, Sender, Receiver};

// Adaptive burst controller
const ADAPTIVE_BURST: bool = true;
const MIN_BURST_COPIES: usize = 1;
const MAX_BURST_COPIES: usize = 3;

// Quarantine policy
const QUARANTINE_MS: u64 = 1500;          // hold-out for 1.5s
const QUARANTINE_FAILS: u32 = 4;          // consecutive send fails threshold
const QUARANTINE_LAT_NS: u64 = 1_500_000; // >1.5 ms avg latency → suspect

// Elite fast-path trigger
const FAST_PATH_SR: f64 = 0.90;           // success rate
const FAST_PATH_LAT_NS: u64 = 350_000;    // 350µs recent send

// Linux fast-paths & link hints
const ENABLE_SENDMMSG: bool = true;   // Collapse UDP duplicates into one syscall where possible
const ENABLE_UDP_GSO: bool = false;   // If NIC/DC supports UDP GSO; disabled by default (conservative)
const UDP_GSO_SEG: u16 = MAX_PACKET_SIZE as u16; // per-segment payload length for UDP GSO
const SET_DF: bool = true;            // Enforce no fragmentation (path must carry 1232B → OK on Solana net)

// ADD
// Prime ARP/ND for the remote immediately after connect() to remove first-packet neighbor resolution.
const ENABLE_NEIGHBOR_PRIME: bool = true;

// Enter a hard real-time critical section while scheduling burst waves.
// Default off; enable only on isolated boxes with proper ulimits.
const ENABLE_RT_CRITICAL: bool = false;
const RT_SCHED_POLICY: i32 = libc::SCHED_FIFO;  // SCHED_FIFO or SCHED_RR
const RT_PRIORITY_LVL: i32 = 80;                // 1..99 (cap by RLIMIT_RTPRIO)

// Lock process memory to avoid minor PF in the hot path. Best-effort; default off.
const ENABLE_MLOCKALL: bool = false;

// ADD
// Prefer TAI for SO_TXTIME if kernel/NIC accept it; fall back to MONOTONIC_RAW → MONOTONIC.
const ENABLE_TXTIME_TAI: bool = true;

// x86_64-only: rdtscp-calibrated spin clock for sub-200µs waits (lower overhead than clock_gettime()).
const ENABLE_TSC_SPIN: bool = true;

// Reuse knobs to avoid bind contention when standing up multi-src-port sockets quickly.
const ENABLE_REUSEPORT: bool = true;
const ENABLE_REUSEADDR: bool = true;

// ADD
// Prefer stable v6 source selection (no temporary/privacy addresses) to stabilize ECMP hashing.
const ENABLE_V6_PREFER_SRC_PUBLIC: bool = true;

// Combine TXTIME + MSG_ZEROCOPY when available (lower CPU per duplicate without losing pacing).
const ENABLE_TXTIME_WITH_ZEROCOPY: bool = true;

// ADD
// Policy routing: tag egress with fwmark so Linux rules can steer per-NIC.
// Leave off unless you've installed matching `ip rule`/`ip route` tables.
const ENABLE_SO_MARK: bool = false;
const BASE_FWMARK: u32 = 0x5300_0000; // high bits; low bits carry ifindex

// Socket priority hint to qdisc (pfifo_fast/mqprio/fq-codel treat high>low).
const ENABLE_SO_PRIORITY: bool = true;
const SOCKET_PRIORITY: i32 = 6; // 0..6 on pfifo_fast; fq still accepts as hint

// Errqueue resilience: force receive buffer for big MSG_ERRQUEUE bursts.
const ENABLE_RCVBUFFORCE: bool = true;
const RCVBUF_TARGET_BYTES: i32 = 16 * 1024 * 1024; // 16 MiB

// Local send queue clamp: if socket outq is high, cap duplicates now.
const ENABLE_SIOCOUTQ_CLAMP: bool = true;
const OUTQ_HIGH_BYTES: i32 = 256 * 1024; // >=256 KiB → clamp to 1
const OUTQ_MED_BYTES:  i32 = 128 * 1024; // >=128 KiB → clamp to 2

// Optional Linux RT + affinity pinning for the hot send path
const ENABLE_RT_AFFINITY: bool = false; // flip to true only when CAP_SYS_NICE is allowed
const RT_PRIORITY: i32 = 14;            // SCHED_FIFO priority (1–99)

// Path diversification: guarantee a forward path early in the fanout
const REQUIRE_FORWARD_MIX: bool = true;

// Prefer QUIC datagrams first; fallback to streams if unsupported (safe default: false)
const ENABLE_QUIC_DATAGRAMS: bool = false;

// Make IPv4 bias a config, not a constant truth (true on most open nets)
const IPV4_BIAS: bool = true;

// Two-wave jitter: 0..400µs, then 400..800µs (second wave is cancellable)
const ENABLE_SECOND_WAVE: bool = true;

// Kernel-paced transmit: schedule UDP emits in the NIC queue with SO_TXTIME/SCM_TXTIME.
// Default off (safe); flip to true after deploying an ETF/fq qdisc and verifying monotonic clock.
const ENABLE_SO_TXTIME: bool = false;

// TXTIME spacing inside a burst (ns). 100µs is conservative and slot-safe.
const TXTIME_SPACING_NS: u64 = 100_000;

// QUIC early-data (0-RTT) resume — reduces reconnect cost; safe to leave on.
const ENABLE_QUIC_EARLY_DATA: bool = true;

// UDP zero-copy transmit (Linux 4.14+). Requires draining MSG_ERRQUEUE to reclaim pages.
const ENABLE_UDP_ZEROCOPY: bool = false;

// Enable TX timestamp reception via MSG_ERRQUEUE (software/hardware as available).
const ENABLE_TX_TIMESTAMPING: bool = false;

// Per-NIC concurrency shaping to avoid bursty head-of-line at the ToR.
// Enable after you stripe across devices (TPU_BIND_DEVICE_LIST).
const ENABLE_DEVICE_LIMITER: bool = true;
const DEVICE_MAX_INFLIGHT: usize = 64;

// Prefer IPv6 flow label auto-generation for stable ECMP hashing (Linux/Android).
const ENABLE_IPV6_AUTOFLOWLABEL: bool = true;

// ADD
// NUMA-prefer the wire buffer pages to the NIC's NUMA node for the elite shot.
const ENABLE_NUMA_MBIND_WIRE: bool = false;

// IPv6: let kernel auto-derive a stable flowlabel; and force min-MTU fallback safety.
const ENABLE_V6_AUTOFLOWLABEL: bool = true;
const ENABLE_V6_USE_MIN_MTU: bool = true;

// Always set ECN ECT(1) with DSCP for v4/v6 (keeps queues ECN-aware, helps fq/fq_codel).
const ENABLE_SET_ECN_ECT1: bool = true;

// Proactively hint the kernel to fault-in/send the wire buffer.
const ENABLE_MADVISE_WILLNEED: bool = true;

// ADD
// Prefer busy-poll on this socket in the kernel (pairs with SO_BUSY_POLL/BUDGET).
const ENABLE_PREFER_BUSY_POLL: bool = true;

// Optional: enter SCHED_DEADLINE for the microburst window (needs CAP_SYS_NICE on most distros).
const ENABLE_SCHED_DEADLINE_BURST: bool = false;
const DL_RUNTIME_US:  u64 = 120;   // run budget
const DL_DEADLINE_US: u64 = 400;   // must finish by
const DL_PERIOD_US:   u64 = 400;   // recurrence (single-shot)

// Aggressive page placement: actively migrate wire buffer pages to NIC's NUMA node.
const ENABLE_MOVE_PAGES_WIRE: bool = false;

// ADD
// Instantly drain errqueue when it actually has data (epoll on POLLERR), not on a timer.
const ENABLE_ERRQUEUE_EPOLL: bool = true;
const EPOLL_BATCH: usize = 64;          // events per wait
const ERRQUEUE_EPOLL_TIMEOUT_MS: i32 = 1;

// Best-effort hardware timestamping enable (global NIC knob; requires CAP_NET_ADMIN).
const ENABLE_HW_TIMESTAMP_IOCTL: bool = true;

// Lock TLS SendWorkspace slabs to avoid surprise soft-faults mid-burst.
const ENABLE_MLOCK_SEND_WS: bool = false;

// ADD
// Teach kernel to deliver live IPv6 Path MTU changes via cmsg; drive GSO instantly from those.
const ENABLE_IPV6_RECVPATHMTU: bool = true;

// MTU cache floors so we never thrash below protocol minimums.
const V4_MIN_MTU: u32 = 576;
const V6_MIN_MTU: u32 = 1232; // QUIC/IPv6 minimum usable payload guidance

// ADD
// Pin the burst thread to a CPU actually mapped to the NIC's XPS mask (per-queue transmit CPU).
const ENABLE_PIN_CPU_TO_XPS: bool = true;

// Zero-copy page-pin backpressure: pause MSG_ZEROCOPY when inflight grows too large.
const ENABLE_ZC_BACKPRESSURE: bool = true;
const ZC_INFLIGHT_SOFT: u64 = 4_096;  // pkts; above this prefer non-ZC
const ZC_INFLIGHT_HARD: u64 = 8_192;  // pkts; hard-disable ZC until it drains

// Fast path: for single-copy, no-TXTIME, no-GSO sends, bypass sendmmsg to a minimal send().
const ENABLE_SENDTO_FASTPATH: bool = true;

// ADD
// Use MSG_ZEROCOPY only when the payload is large enough to win versus memcpy.
const ENABLE_ZC_MIN_SIZE: bool = true;
const ZC_MIN_BYTES: usize = 1536; // empirically beats memcpy on modern Xeons

// ADD: Runtime copybreak calibration
use std::sync::atomic::{AtomicUsize, Ordering};
static ZC_MIN_BYTES_CAL: AtomicUsize = AtomicUsize::new(ZC_MIN_BYTES);

#[inline]
fn memcpy_calibrate_threshold_once() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        // Measure memcpy throughput on this host/core ~200µs/sample
        fn bench(sz: usize, iters: usize) -> f64 {
            let mut a = vec![0u8; sz]; let mut b = vec![0u8; sz];
            let start = tsc_now_ns();
            for _ in 0..iters {
                unsafe {
                    std::ptr::copy_nonoverlapping(a.as_ptr(), b.as_mut_ptr(), sz);
                }
            }
            let ns = (tsc_now_ns() - start) as f64;
            let bytes = (sz * iters) as f64;
            bytes / ns // bytes/ns = ~GB/s * 1e9
        }
        let sizes = [256, 512, 1024, 1536, 2048, 4096, 8192, 16384, 32768];
        let mut best = ZC_MIN_BYTES;
        // Empirical ZC cost budget (ns) on modern Xeons
        const ZC_FIXED_NS: f64 = 1200.0;     // msg setup + page pin amortization
        const ZC_COMP_PER_KB_NS: f64 = 80.0; // completion & errqueue per KB
        for &sz in &sizes {
            let th = bench(sz, 128).max(1.0); // bytes/ns
            let memcpy_ns = (sz as f64) / th;
            let zc_ns = ZC_FIXED_NS + ZC_COMP_PER_KB_NS * (sz as f64 / 1024.0);
            if memcpy_ns > zc_ns {
                best = sz;
                break;
            }
        }
        ZC_MIN_BYTES_CAL.store(best, Ordering::Relaxed);
    });
}

// ADD: Prefault/touch helpers
#[cfg(target_os = "linux")]
#[inline]
fn prefault_write(ptr: *mut u8, len: usize) {
    if len == 0 { return; }
    unsafe {
        let page = 4096usize;
        let mut p = ptr;
        let end = ptr.add(len);
        while p < end {
            std::ptr::write_volatile(p, std::ptr::read_volatile(p)); // touch
            p = p.add(page);
        }
        // tail
        std::ptr::write_volatile(end.offset(-1), std::ptr::read_volatile(end.offset(-1)));
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn prefault_write(_ptr: *mut u8, _len: usize) {}

// ADD: IP_FREEBIND for VIP failover
#[cfg(target_os = "linux")]
#[inline]
fn enable_freebind(sock: &Socket, remote: &SocketAddr) {
    unsafe {
        let one: libc::c_int = 1;
        match remote {
            SocketAddr::V4(_) => {
                // IP_FREEBIND = 15
                let _ = libc::setsockopt(
                    sock.as_raw_fd(), libc::IPPROTO_IP, 15,
                    &one as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
            SocketAddr::V6(_) => {
                // IPV6_FREEBIND = 78
                let _ = libc::setsockopt(
                    sock.as_raw_fd(), libc::IPPROTO_IPV6, 78,
                    &one as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn enable_freebind(_sock: &Socket, _remote: &SocketAddr) {}

// ADD: SO_MAX_PACING_RATE setter
#[cfg(target_os = "linux")]
#[inline]
fn set_max_pacing_rate(fd: libc::c_int, bps: u64) {
    // SO_MAX_PACING_RATE = 47 (bytes/sec)
    unsafe {
        let v: libc::c_ulong = bps as libc::c_ulong;
        let _ = libc::setsockopt(
            fd, libc::SOL_SOCKET, 47,
            &v as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_ulong>() as libc::socklen_t,
        );
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn set_max_pacing_rate(_fd: libc::c_int, _bps: u64) {}

// ADD: Duplicate arrival feasibility filter
#[inline]
fn will_arrive_before_deadline(ns_from_now: u64, bytes_ahead: u64, bps: u64) -> bool {
    // time to serialize bytes_ahead at bps (ns)
    if bps == 0 { return false; }
    let ser_ns = (bytes_ahead.saturating_mul(8_000_000_000u64 / 8)).saturating_div(bps);
    ser_ns < ns_from_now
}

// ADD: Safe per-core randomization for port-rotate decisions
thread_local! { static XRAND: std::cell::Cell<u64> = std::cell::Cell::new(0x9E3779B97F4A7C15); }
#[inline] fn xrand_u32() -> u32 {
    XRAND.with(|c| {
        let mut x = c.get();
        x ^= x << 7; x ^= x >> 9; x ^= x << 8;
        c.set(x);
        (x as u32)
    })
}

// ADD: DSCP cache structure
#[derive(Default)]
struct TosCache { 
    v4: AtomicI32, 
    v6: AtomicI32 
}

// If sendmmsg returns EAGAIN/ENOBUFS, drain errqueue once to free ZC pages, then retry.
const ENABLE_EAGAIN_ERRQUEUE_RETRY: bool = true;
const ERRQUEUE_RETRY_BATCH: u32 = 8; // small, cheap micro-drain

// ADD
// On repeated adversity (ETIME/ETIMEDOUT/ENOBUFS), rotate to an alt UDP socket (new src port).
const ENABLE_ROTATE_SRC_ON_ADVERSITY: bool = true;
const ADVERSITY_THRESH: u32 = 8;      // consecutive events to trigger rotate
const ADVERSITY_DECAY_MS: u64 = 500;  // decay window to avoid sticky escalations

// ADD
const ENABLE_ADAPT_BUSY_POLL_BUDGET: bool = true;
const BUSY_POLL_BUDGET_COOL: i32 = 32;
const BUSY_POLL_BUDGET_HOT: i32 = 256;
const BUSY_POLL_BUDGET_VHOT: i32 = 512;
const BUSY_POLL_Q_HOT_NS: u64 = 120_000;   // 0.12 ms
const BUSY_POLL_Q_VHOT_NS: u64 = 250_000;  // 0.25 ms

// ADD
// Allocate a per-connection IPv6 flow-label (20 bits) to lock ECMP.
// Keeps hashing rock-stable even if src addr/ports rotate.
const ENABLE_V6_FLOWLABEL_MGR: bool = true;
const V6_FLOWLABEL_EXPIRE_S: u16 = 3600; // renew hourly if kernel honors renewals

// ADD
const ENABLE_SET_TIMER_SLACK: bool = true;
const TIMER_SLACK_NS: u64 = 1_000; // 1 µs

#[cfg(target_os = "linux")]
const PR_SET_TIMERSLACK: libc::c_int = 29;

static TIMER_SLACK_ONCE: OnceLock<()> = OnceLock::new();

// ADD
const ENABLE_MSG_CONFIRM_KEEP_NEIGH: bool = true;
const NEIGH_CONFIRM_INTERVAL_NS: u64 = 200_000_000; // 200ms

// ADD
const ENABLE_MULTI_NIC_SKEW: bool = true;
const NIC_SKEW_STEP_NS: u64 = 12_000; // 12µs per rank (tight, sub RTT)

// ADD
const ENABLE_SNDBUFFORCE: bool = true;
const SNDBUF_TARGET_BYTES: i32 = 32 * 1024 * 1024; // 32 MiB

// ADD
const ENABLE_EAGAIN_NANOBACKOFF: bool = true;
const EAGAIN_BACKOFF_NS: u64 = 30_000; // 30 µs

// ADD
const ENABLE_OPERSTATE_GUARD: bool = true;
const OPERSTATE_RECHECK_NS: u64 = 500_000_000; // 500ms

// ADD
#[cfg(target_os = "linux")]
const SO_BINDTOIFINDEX: libc::c_int = 62;

// ADD
#[cfg(target_os = "linux")]
const SO_TXREHASH: libc::c_int = 74;

// ADD
const GSO_MAX_SEGS: usize = 64; // safe cap across NICs/qdiscs

// ADD
const ENABLE_SO_MARK_PER_ALT: bool = true;
const SO_MARK_BASE: u32 = 0x4000; // choose a policy range with your tc filters

// Linux syscalls (sched_setattr)
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
const SYS_SCHED_SETATTR: libc::c_long = 314;
#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
const SYS_SCHED_SETATTR: libc::c_long = 274;

// For HWTSTAMP ioctl
#[cfg(target_os = "linux")]
const SIOCSHWTSTAMP: libc::c_ulong = 0x89b0;

// ADD
// IPv6 Flow Label Manager constants
#[cfg(target_os = "linux")]
const IPV6_FLOWLABEL_MGR: libc::c_int = 32; // setsockopt(IPPROTO_IPV6)

#[cfg(target_os = "linux")]
const IPV6_FL_A_GET: u8   = 0;
#[cfg(target_os = "linux")]
const IPV6_FL_A_PUT: u8   = 1;
#[cfg(target_os = "linux")]
const IPV6_FL_A_RENEW: u8 = 2;

#[cfg(target_os = "linux")]
const IPV6_FL_F_CREATE: u16 = 0x0001;
#[cfg(target_os = "linux")]
const IPV6_FL_F_EXCL:   u16 = 0x0002;

#[cfg(target_os = "linux")]
const IPV6_FL_S_EXCL: u8 = 1;

#[cfg(target_os = "linux")]
#[repr(C)]
struct In6FlowlabelReq {
    flr_dst: libc::in6_addr,  // destination IPv6
    flr_label: u32,           // high 20 bits used; network byte order
    flr_action: u8,           // ADD/RENEW/PUT
    flr_share: u8,            // EXCL
    flr_flags: u16,           // CREATE|EXCL
    flr_expires: u16,         // seconds
    flr_linger: u16,          // seconds
    __flr_pad: u32,
}

// ADD
// TXTIME epoch correctness globals
#[cfg(target_os = "linux")]
static TXT_CLOCKID: AtomicI32 = AtomicI32::new(libc::CLOCK_MONOTONIC);
#[cfg(target_os = "linux")]
static TXT_OFFSET_NS: AtomicI64 = AtomicI64::new(0);

// IPV6_PATHMTU struct for cmsg 61
#[cfg(target_os = "linux")]
#[repr(C)]
struct SockAddrIn6 {
    sin6_family: libc::sa_family_t,
    sin6_port:   u16,
    sin6_flowinfo: u32,
    sin6_addr:   libc::in6_addr,
    sin6_scope_id: u32,
}
#[cfg(target_os = "linux")]
#[repr(C)]
struct Ip6MtuInfo {
    ip6m_addr: SockAddrIn6,
    ip6m_mtu:  u32,
}

// ADD
// Sub-µs scheduling discipline: spin for very short waits to avoid timer jitter.
const MICRO_SPIN_THRESHOLD_US: u64 = 120;

// Mid-slot booster wave: sends one extra shot early if the elite path hasn't won yet.
const ENABLE_BOOSTER_WAVE: bool = true;
const BOOSTER_BASE_US: u64 = 180; // earliest booster fire (µs)
const BOOSTER_SPAN_US: u64 = 80;  // extra deterministic jitter (total 180..260µs)

// Linux busy-poll to speed up MSG_ERRQUEUE drains (zerocopy/timestamps).
const ENABLE_BUSY_POLL: bool = true;
const BUSY_POLL_US: i32 = 50; // µs for SO_BUSY_POLL

// Force send buffer size beyond rlimit (Linux only).
const ENABLE_SNDBUFFORCE: bool = true;
const SNDBUF_TARGET_BYTES: i32 = 8 * 1024 * 1024;

// ADD
// Diversify ECMP by varying local source ports per endpoint (multiple bound UDP sockets).
const ENABLE_MULTI_SRC_PORTS: bool = true;
const SRC_PORT_COPIES: usize = 2; // up to 2 distinct source ports per remote

// Tighten kernel timer slack per thread (Linux) to reduce timer coalescing jitter.
const ENABLE_TIMER_SLACK_TIGHTEN: bool = true;
const TIMER_SLACK_NS: u64 = 1_000; // 1µs

// ADD
// Bind without reserving an ephemeral port until connect/send (cuts bind contention on burst setups).
const ENABLE_IP_BIND_NO_PORT: bool = true;

// Force extremely high pacing ceiling so fq/ETF never rate-limits microbursts unless we TXTIME them.
const ENABLE_MAX_PACING_RATE: bool = true;
const MAX_PACING_BPS: u64 = 10_000_000_000; // 10 Gbps ceiling per socket

// ADD
// Enable PMTU-aware UDP GSO so UDP_SEGMENT matches the actual path MTU (no blackhole/EMSGSIZE).
const ENABLE_PMTU_AWARE_GSO: bool = true;

// Safe fallbacks if MTU is unknown.
const V4_SAFE_SEG: u16 = 1472; // 1500 - 20(IPv4) - 8(UDP)
const V6_SAFE_SEG: u16 = 1232; // 1280 - 40(IPv6) - 8(UDP)  (QUIC min path)

// ADD
// TXTIME spacing adapts to live NIC/ToR queue: cooler → tighter spacing, hotter → wider.
const TXTIME_MIN_SPACING_NS: u64 = 40_000;   // 40µs floor (cool device)
const TXTIME_MAX_SPACING_NS: u64 = 160_000;  // 160µs ceiling (hot device)

// Batch-drain kernel MSG_ERRQUEUE to cut syscall overhead.
const ERRQUEUE_BATCH: usize = 16;            // recvmmsg batch size
const ERRQUEUE_MAX_PER_SOCKET: usize = 256;  // per-tick budget per socket

// ADD
const SOLANA_SLOT_NS: u64 = 400_000_000; // 400ms
const MICRO_SPIN_THRESHOLD_US: u64 = 220;

// ADD
// TXTIME error reporting: surface missed deadlines via ETIME so we can react immediately.
const ENABLE_TXTIME_REPORT_ERRORS: bool = true;

// Treat TXTIME ETIME as "queue is hot" for N µs (feeds adaptive spacing immediately).
const TXTIME_MISS_PENALTY_QUEUE_NS: u64 = 1_200_000; // 1.2 ms synthetic dwell

// ADD
const PMTU_TTL: Duration = Duration::from_secs(15);

struct PmtuEntry { mtu: u16, ts: Instant }
static PMTU_CACHE_V4: Lazy<DashMap<u64, PmtuEntry>> = Lazy::new(|| DashMap::new());
static PMTU_CACHE_V6: Lazy<DashMap<u128, PmtuEntry>> = Lazy::new(|| DashMap::new());

#[inline]
fn pmtu_cache_key_v4(addr: &std::net::SocketAddrV4) -> u64 {
    let ip = u32::from_be_bytes(addr.ip().octets()) as u64;
    (ip << 16) ^ (addr.port() as u64)
}

#[inline]
fn pmtu_cache_key_v6(addr: &std::net::SocketAddrV6) -> u128 {
    let o = addr.ip().octets();
    let hi = u64::from_be_bytes([o[0],o[1],o[2],o[3],o[4],o[5],o[6],o[7]]) as u128;
    let lo = u64::from_be_bytes([o[8],o[9],o[10],o[11],o[12],o[13],o[14],o[15]]) as u128;
    (hi << 64) | lo ^ (addr.port() as u128)
}

#[inline]
fn get_path_mtu_cached(fd: libc::c_int, remote: &std::net::SocketAddr) -> u16 {
    match remote {
        std::net::SocketAddr::V4(v4) => {
            let k = pmtu_cache_key_v4(v4);
            if let Some(e) = PMTU_CACHE_V4.get(&k) {
                if e.ts.elapsed() < PMTU_TTL { return e.mtu; }
            }
            let m = get_path_mtu(fd, false); // your existing probe
            PMTU_CACHE_V4.insert(k, PmtuEntry { mtu: m, ts: Instant::now() });
            m
        }
        std::net::SocketAddr::V6(v6) => {
            let k = pmtu_cache_key_v6(v6);
            if let Some(e) = PMTU_CACHE_V6.get(&k) {
                if e.ts.elapsed() < PMTU_TTL { return e.mtu; }
            }
            let m = get_path_mtu(fd, true);
            PMTU_CACHE_V6.insert(k, PmtuEntry { mtu: m, ts: Instant::now() });
            m
        }
    }
}

// Busy-poll budget (packets) to complement SO_BUSY_POLL micro-latency drains.
const BUSY_POLL_BUDGET: i32 = 256;

// Adaptive copy clamp based on NIC/ToR queue dwell (from TX timestamping).
const ADAPTIVE_COPY_CLAMP: bool = true;
const QUEUE_NS_HIGH: u64 = 900_000;  // ≥0.9ms → clamp hard
const QUEUE_NS_MED:  u64 = 450_000;  // ≥0.45ms → clamp moderate

// ADD
// Hoplimit/TTL to avoid long-path detours but stay fabric-safe.
const DEFAULT_HOPS: u32 = 32;

// Quinn datagram buffers (when datagrams are enabled).
const QUIC_DGRAM_SEND_BUF: usize = 1 << 20; // 1 MiB
const QUIC_DGRAM_RECV_BUF: usize = 1 << 20; // 1 MiB

// ADD
// Enable PMTU-aware UDP GSO so UDP_SEGMENT matches the actual path MTU (no blackhole/EMSGSIZE).
const ENABLE_PMTU_AWARE_GSO: bool = true;

// Safe fallbacks if MTU is unknown.
const V4_SAFE_SEG: u16 = 1472; // 1500 - 20(IPv4) - 8(UDP)
const V6_SAFE_SEG: u16 = 1232; // 1280 - 40(IPv6) - 8(UDP)  (QUIC min path)

// Connection management
const MAX_CONNECTIONS_PER_LEADER: usize = 8;
const CONNECTION_POOL_SIZE: usize = 512;
const MAX_PARALLEL_SENDS: usize = 16;
const PORT_RANGE_START: u16 = 8000;
const PORT_RANGE_END: u16 = 10000;
const MAX_RETRIES: u32 = 3;
const RETRY_DELAY_MS: u64 = 2;
const CONNECTION_TIMEOUT_MS: u64 = 500;
const HEALTH_CHECK_INTERVAL_MS: u64 = 250;
const MAX_PACKET_SIZE: usize = 1232;
const QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS: usize = 128;
const LEADER_SCHEDULE_SLOT_OFFSET: u64 = 4;
const CONNECTION_CACHE_SIZE: usize = 4096;
const LEADER_SCHEDULE_CACHE_TTL_MS: u64 = 400;
const MAX_QUIC_CONNECTIONS_PER_PEER: usize = 8;

// DSCP EF (46) with optional ECN ECT(0). Most DC fabrics accept ECN safely.
// Set to true to mark ECT(0) in the TOS low bits; otherwise pure DSCP EF.
const ENABLE_ECN: bool = false;
const DSCP_TOS: u32 = ((46u32) << 2) | (if ENABLE_ECN { 0b10 } else { 0 });

#[cfg(target_os = "linux")]
// QoS helper functions
#[inline]
fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(target_os = "linux")]
#[inline]
fn set_sock_priority_mark(sock: &Socket) {
    use libc::{self, c_int};
    
    unsafe {
        let fd = sock.as_raw_fd();
        // SO_PRIORITY = 6 (high user priority; kernel maps to qdisc)
        let prio: c_int = 6;
        let _ = libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_PRIORITY,
            &prio as *const _ as *const libc::c_void,
            std::mem::size_of_val(&prio) as libc::socklen_t,
        );

        // Optional SO_MARK for policy routing (set TPU_SO_MARK=integer)
        if let Ok(mark_str) = std::env::var("TPU_SO_MARK") {
            if let Ok(mark) = mark_str.parse::<c_int>() {
                let _ = libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    36, // SO_MARK
                    &mark as *const _ as *const libc::c_void,
                    std::mem::size_of_val(&mark) as libc::socklen_t,
                );
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
#[inline]
fn set_sock_priority_mark(_sock: &Socket) {}

// Use DSCP/ECN and priority/mark
#[inline]
fn apply_qos(sock: &Socket) {
    #[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
    { let _ = sock.set_tos(DSCP_TOS); }
    #[cfg(target_os = "linux")]
    set_sock_priority_mark(sock);
}

// REPLACE
#[inline]
fn apply_link_hints(sock: &Socket, remote: &SocketAddr) {
    // DSCP/ECN + priority/mark + DF + v6 TCLASS + v6 flowlabel
    #[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
    {
        match remote {
            SocketAddr::V4(_) => {
                let _ = sock.set_tos(DSCP_TOS);
            }
            SocketAddr::V6(_) => {
                #[cfg(any(target_os = "linux", target_os = "android"))]
                unsafe {
                    // TCLASS = DSCP<<2 | ECN(ECT1)
                    let tclass: libc::c_int = ((DSCP_TOS as u8) << 2 | 0b10) as libc::c_int;
                    // IPV6_TCLASS = 67
                    let _ = libc::setsockopt(
                        sock.as_raw_fd(), libc::IPPROTO_IPV6, 67,
                        &tclass as *const _ as *const libc::c_void,
                        std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                    );
                    // Keep any flowlabel or autoflowlabel settings externalized; autoflowlabel is set separately.
                    if ENABLE_IPV6_AUTOFLOWLABEL {
                        let one: libc::c_int = 1;
                        // IPV6_AUTOFLOWLABEL = 115
                        let _ = libc::setsockopt(
                            sock.as_raw_fd(),
                            libc::IPPROTO_IPV6,
                            115,
                            &one as *const _ as *const libc::c_void,
                            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                        );
                    }
                }
                #[cfg(target_os = "freebsd")]
                    unsafe {
                    let tclass: libc::c_int = (DSCP_TOS & 0xFF) as libc::c_int;
                    // FreeBSD IPV6_TCLASS = 61
                        let _ = libc::setsockopt(
                            sock.as_raw_fd(),
                            libc::IPPROTO_IPV6,
                            61,
                        &tclass as *const _ as *const libc::c_void,
                        std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                        );
                    }
                }
            }
        }
    #[cfg(target_os = "linux")]
    set_sock_priority_mark(sock);

    set_df(sock, remote);
}

#[cfg(target_os = "linux")]
#[inline]
fn set_df(sock: &Socket, remote: &SocketAddr) {
    if !SET_DF { return; }
    unsafe {
        let fd = sock.as_raw_fd();
        if remote.is_ipv4() {
            // IP_MTU_DISCOVER = 10, IP_PMTUDISC_DO = 2
            let val: libc::c_int = 2;
            let _ = libc::setsockopt(
                fd,
                libc::IPPROTO_IP,
                libc::IP_MTU_DISCOVER,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of_val(&val) as libc::socklen_t,
            );
        } else {
            // IPV6_DONTFRAG = 62
            let val: libc::c_int = 1;
            let _ = libc::setsockopt(
                fd,
                libc::IPPROTO_IPV6,
                62, // IPV6_DONTFRAG
                &val as *const _ as *const libc::c_void,
                std::mem::size_of_val(&val) as libc::socklen_t,
            );
        }
    }
}

#[cfg(not(target_os = "linux"))]
#[inline]
fn set_df(_sock: &Socket, _remote: &SocketAddr) {}

#[cfg(target_os = "linux")]
#[inline]
fn can_use_udp_gso() -> bool { ENABLE_UDP_GSO }

#[cfg(not(target_os = "linux"))]
#[inline]
fn can_use_udp_gso() -> bool { false }

// ADD
#[inline]
fn udp_overhead(is_v6: bool) -> u32 { if is_v6 { 40 + 8 } else { 20 + 8 } }

#[cfg(target_os = "linux")]
#[inline]
fn set_pmtu_discover(sock: &Socket, remote: &SocketAddr) {
    if !ENABLE_PMTU_AWARE_GSO { return; }
    unsafe {
        if remote.is_ipv4() {
            let val: libc::c_int = libc::IP_PMTUDISC_DO;
            let _ = libc::setsockopt(
                sock.as_raw_fd(),
                libc::IPPROTO_IP,
                libc::IP_MTU_DISCOVER,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        } else {
            let val: libc::c_int = libc::IPV6_PMTUDISC_DO;
            let _ = libc::setsockopt(
                sock.as_raw_fd(),
                libc::IPPROTO_IPV6,
                libc::IPV6_MTU_DISCOVER,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn set_pmtu_discover(_sock: &Socket, _remote: &SocketAddr) {}

#[cfg(target_os = "linux")]
#[inline]
fn get_path_mtu(fd: libc::c_int, is_v6: bool) -> Option<u32> {
    unsafe {
        let mut mtu: libc::c_int = 0;
        let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        let ret = if is_v6 {
            libc::getsockopt(fd, libc::IPPROTO_IPV6, libc::IPV6_MTU, &mut mtu as *mut _ as *mut _, &mut len)
        } else {
            libc::getsockopt(fd, libc::IPPROTO_IP, libc::IP_MTU, &mut mtu as *mut _ as *mut _, &mut len)
        };
        if ret == 0 && mtu > 0 { Some(mtu as u32) } else { None }
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn get_path_mtu(_fd: libc::c_int, _is_v6: bool) -> Option<u32> { None }

#[inline]
fn clamp_udp_seg(payload_len: usize, is_v6: bool, maybe_mtu: Option<u32>) -> u16 {
    let fallback = if is_v6 { V6_SAFE_SEG } else { V4_SAFE_SEG };
    let seg = if let Some(mtu) = maybe_mtu {
        let oh = udp_overhead(is_v6);
        let max_payload = mtu.saturating_sub(oh).max(512);
        (max_payload as usize).min(payload_len).max(1) as u16
    } else {
        fallback.min(payload_len as u16).max(1)
    };
    seg
}

// REPLACE
#[inline]
async fn micro_sleep_cancellable(us: u64, won: &Arc<AtomicBool>) {
    if us == 0 || won.load(Ordering::Relaxed) { return; }
    let budget_ns = us * 1_000;
    if us <= MICRO_SPIN_THRESHOLD_US {
        let start = tsc_now_ns();
            loop {
                if won.load(Ordering::Relaxed) { return; }
                std::hint::spin_loop();
            if tsc_now_ns().wrapping_sub(start) >= budget_ns { break; }
            }
            return;
        }
    // Chunked timer for longer waits, allow early exit
    let mut remain = us;
    while remain > 0 && !won.load(Ordering::Relaxed) {
        let chunk = remain.min(50);
        tokio::time::sleep(Duration::from_micros(chunk)).await;
        remain -= chunk;
    }
}

// ADD (helper)
#[inline]
fn set_only_v6_if(sock: &Socket, remote: &SocketAddr) {
    if let SocketAddr::V6(_) = remote {
        let _ = sock.set_only_v6(true);
    }
}

// ADD
#[inline(always)]
fn adaptive_txtime_spacing_ns(dev_stats: Option<Arc<DeviceStats>>) -> u64 {
    if let Some(s) = dev_stats {
        let q = s.avg_tx_queue_ns.load(Ordering::Relaxed);
        if q >= QUEUE_NS_HIGH { return TXTIME_MAX_SPACING_NS; }        // hot → spread
        if q >= QUEUE_NS_MED  { return (TXTIME_MIN_SPACING_NS + TXTIME_MAX_SPACING_NS) / 2; }
        return TXTIME_MIN_SPACING_NS;                                  // cool → tighten
    }
    // Fallback to configured baseline if metrics not present
    TXTIME_SPACING_NS
}

// ADD
#[cfg(target_os = "linux")]
fn set_dscp_ecn(sock: &std::net::UdpSocket, remote: &std::net::SocketAddr) {
    use std::os::fd::AsRawFd;
    let fd = sock.as_raw_fd();

    // DSCP EF (46) + ECN ECT(0) => TOS = (46<<2) | 0b10 = 0xBA
    let tos_v4: libc::c_int = 0xBA;
    // Same traffic class for IPv6
    let tclass_v6: libc::c_int = 0xBA;

    unsafe {
        match remote {
            std::net::SocketAddr::V4(_) => {
                let _ = libc::setsockopt(fd, libc::IPPROTO_IP, libc::IP_TOS,
                                         &tos_v4 as *const _ as *const _, std::mem::size_of_val(&tos_v4) as _);
            }
            std::net::SocketAddr::V6(_) => {
                let _ = libc::setsockopt(fd, libc::IPPROTO_IPV6, libc::IPV6_TCLASS,
                                         &tclass_v6 as *const _ as *const _, std::mem::size_of_val(&tclass_v6) as _);
            }
        }

        // Highest kernel send priority bucket (qdisc hint)
        let prio: libc::c_int = 6;
        let _ = libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_PRIORITY,
                                 &prio as *const _ as *const _, std::mem::size_of_val(&prio) as _);

        // Optional SO_MARK for policy routing (env var: TPU_SO_MARK or per-device)
        if let Ok(mark_str) = std::env::var("TPU_SO_MARK") {
            if let Ok(mark) = mark_str.parse::<u32>() {
                let m: libc::c_int = mark as _;
                let _ = libc::setsockopt(fd, libc::SOL_SOCKET, libc::SO_MARK,
                                         &m as *const _ as *const _, std::mem::size_of_val(&m) as _);
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn set_dscp_ecn(_sock: &std::net::UdpSocket, _remote: &std::net::SocketAddr) {}

// ADD
#[cfg(target_os = "linux")]
fn pin_current_thread_to_device(dev_opt: Option<&str>) {
    use libc::{cpu_set_t, CPU_SET, CPU_ZERO, sched_setaffinity};
    if dev_opt.is_none() { return; }
    let dev = dev_opt.unwrap();
    let key = format!("TPU_DEV_CPUSET_{}", dev);
    let Ok(mask) = std::env::var(key) else { return; };

    let mut cpuset: cpu_set_t = unsafe { std::mem::zeroed() };
    unsafe { CPU_ZERO(&mut cpuset); }

    // parse "0-7,16,18-19" style masks
    for part in mask.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
        if let Some((lo, hi)) = part.split_once('-') {
            if let (Ok(a), Ok(b)) = (lo.parse::<usize>(), hi.parse::<usize>()) {
                for c in a..=b { unsafe { CPU_SET(c, &mut cpuset); } }
            }
        } else if let Ok(c) = part.parse::<usize>() {
            unsafe { CPU_SET(c, &mut cpuset); }
        }
    }

    unsafe {
        let _ = sched_setaffinity(0, std::mem::size_of::<cpu_set_t>(), &cpuset);
    }
}

#[cfg(not(target_os = "linux"))]
fn pin_current_thread_to_device(_dev_opt: Option<&str>) {}

// ADD
#[inline]
fn setup_recv_err(sock: &Socket, remote: &SocketAddr) {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    unsafe {
        if remote.is_ipv4() {
            let one: libc::c_int = 1;
            let _ = libc::setsockopt(
                sock.as_raw_fd(),
                libc::IPPROTO_IP,
                libc::IP_RECVERR,
                &one as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        } else {
            let one: libc::c_int = 1;
            let _ = libc::setsockopt(
                sock.as_raw_fd(),
                libc::IPPROTO_IPV6,
                libc::IPV6_RECVERR,
                &one as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
}

// ADD
#[cold]
#[inline]
fn classify_and_maybe_quarantine(
    e: &std::io::Error,
    st: &ConnectionState,
    metrics: &Arc<Metrics>,
) {
    let now = now_unix_ms();
    if let Some(code) = e.raw_os_error() {
        // Local congestion: record only (copies clamp elsewhere)
        if code == libc::ENOBUFS || code == libc::EAGAIN || code == libc::EWOULDBLOCK {
            return;
        }
        // REPLACE (add one more branch in classify_and_maybe_quarantine)
        if code == libc::ETIMEDOUT {
            st.is_healthy.store(false, Ordering::Relaxed);
            st.quarantine_until_ms.store(now + 3_000, Ordering::Relaxed);
            return;
        }
        // Missed TXTIME deadline → push device queue dwell high for next burst
        if ENABLE_SO_TXTIME && ENABLE_TXTIME_REPORT_ERRORS && code == libc::ETIME {
            if let Some(dev) = st.bound_device.as_ref() {
                metrics.record_dev_tx(
                    Some(dev),
                    None,
                    Some(TXTIME_MISS_PENALTY_QUEUE_NS),
                    0,
                );
            }
            return;
        }
        // MTU mismatch: brief quarantine
        if code == libc::EMSGSIZE {
            st.is_healthy.store(false, Ordering::Relaxed);
            st.quarantine_until_ms.store(now + 2_000, Ordering::Relaxed);
            return;
        }
        // Routing/socket hard errors — quarantine harder
        if code == libc::ENETUNREACH || code == libc::EHOSTUNREACH || code == libc::ENETDOWN || code == libc::ECONNREFUSED {
            st.is_healthy.store(false, Ordering::Relaxed);
            st.quarantine_until_ms.store(now + 5_000, Ordering::Relaxed);
            return;
        }
    }
}

// ADD
#[inline]
fn slot_phase_jitter(sig_hash64: u64, remote: &std::net::SocketAddr, now_ns: u64, leader_slot_start_ns: u64) -> (u64, u64) {
    // phase in [0..SOLANA_SLOT_NS)
    let phase = now_ns.wrapping_sub(leader_slot_start_ns) % SOLANA_SLOT_NS;
    // Earlier in slot: wider spread; close to boundary: tighter
    let (base_us, span_us) = if phase < 150_000_000 {       // 0..150ms
        (60, 420)
    } else if phase < 300_000_000 {                         // 150..300ms
        (30, 260)
    } else {                                                // 300..400ms
        (0, 160)
    };
    let seed = addr_hash64(remote) ^ sig_hash64 ^ fast_hash_u64(phase);
    let delay = det_jitter_us(seed, 0xA4A9_93E3_D9A7_66D5, base_us as u64, span_us as u64);
    (delay, span_us as u64)
}

#[inline]
fn mono_time_ns() -> u64 {
    #[cfg(target_os = "linux")]
    unsafe {
        let mut ts: libc::timespec = std::mem::zeroed();
        libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut ts);
        (ts.tv_sec as u64) * 1_000_000_000 + (ts.tv_nsec as u64)
    }
    #[cfg(not(target_os = "linux"))]
    {
        use std::time::SystemTime;
        SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_nanos() as u64
    }
}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn prime_neighbor(fd: libc::c_int) {
    if !ENABLE_NEIGHBOR_PRIME { return; }
    unsafe {
        // send(0 bytes) with MSG_CONFIRM | MSG_DONTWAIT triggers ARP/ND confirm without blocking.
        let _ = libc::send(fd, std::ptr::null(), 0, libc::MSG_CONFIRM | libc::MSG_DONTWAIT);
    }
}

#[cfg(not(target_os = "linux"))]
#[inline]
fn prime_neighbor(_fd: libc::c_int) {}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn tighten_thread_timer_slack(ns: u64) {
    unsafe {
        // Best-effort; ignores failure on locked-down kernels.
        let _ = libc::prctl(PR_SET_TIMERSLACK, ns as libc::c_ulong, 0, 0, 0);
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn tighten_thread_timer_slack(_ns: u64) {}

// ADD
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
struct TscCal { cycles_per_ns: f64, tsc0: u64, ns0: u64 }
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
static TSC_CAL: OnceLock<TscCal> = OnceLock::new();

// REPLACE (only the initializer inside tsc_now_ns() module above)
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn tsc_now_ns() -> u64 {
    if ENABLE_RDTSCP_TSC {
        unsafe {
            let aux: u32;
            let hi: u32; let lo: u32;
            core::arch::asm!("rdtscp", out("eax") lo, out("edx") hi, out("ecx") aux, options(nostack, preserves_flags));
            let tsc = ((hi as u64) << 32) | (lo as u64);
            return tsc_to_ns(tsc);
        }
    }
    // fallback to calibration-based approach
    tsc_now_ns_fallback()
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn tsc_now_ns_fallback() -> u64 {
    // Calibrate once against CLOCK_MONOTONIC_RAW with measurable baseline.
    let cal = TSC_CAL.get_or_init(|| unsafe {
        let mut ts: libc::timespec = std::mem::zeroed();
        libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut ts);
        let t0_ns = (ts.tv_sec as u64)*1_000_000_000 + (ts.tv_nsec as u64);
        let mut aux: u32 = 0;
        let t0 = _rdtscp(&mut aux);

        // Busy-wait until at least 2ms pass for a stable slope
        loop {
            libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut ts);
            let now_ns = (ts.tv_sec as u64)*1_000_000_000 + (ts.tv_nsec as u64);
            if now_ns - t0_ns >= 2_000_000 { // 2ms
                let t1 = _rdtscp(&mut aux);
                let delta_c = (t1 - t0).max(1) as f64;
                let delta_ns = (now_ns - t0_ns).max(1) as f64;
                break TscCal { cycles_per_ns: delta_c / delta_ns, tsc0: t0, ns0: t0_ns };
            }
            std::hint::spin_loop();
        }
    });
    unsafe {
        let mut aux: u32 = 0;
        let t = _rdtscp(&mut aux);
        let dc = t.saturating_sub(cal.tsc0) as f64;
        cal.ns0 + (dc / cal.cycles_per_ns) as u64
    }
}

// Fallbacks on non-x86 or non-Linux
#[cfg(not(all(target_os = "linux", target_arch = "x86_64")))]
#[inline]
fn tsc_now_ns() -> u64 {
    unsafe {
        let mut ts: libc::timespec = std::mem::zeroed();
        libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut ts);
        (ts.tv_sec as u64)*1_000_000_000 + (ts.tv_nsec as u64)
    }
}

// ADD
// TXTIME base timestamp helper
#[inline(always)]
fn txtime_now_ns() -> u64 {
#[cfg(target_os = "linux")]
    {
        let base = tsc_now_ns() as i128;
        let off  = TXT_OFFSET_NS.load(Ordering::Relaxed) as i128;
        return base.wrapping_add(off) as u64;
    }
    #[cfg(not(target_os = "linux"))]
    { tsc_now_ns() }
}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn set_v6_flowlabel_mgr(sock: &Socket, remote: &SocketAddr) {
    if !ENABLE_V6_FLOWLABEL_MGR { return; }
    let v6 = match remote { SocketAddr::V6(v) => v, _ => return };
    // Deterministic 20-bit label from remote ip:port (stable across process lifetime)
    let mut seed = 0u64;
    for b in v6.ip().octets() { seed = seed.wrapping_mul(0x9E37_79B97F4A7C15).wrapping_add(b as u64 + 0x9E37); }
    seed = seed.wrapping_mul(0xBF58_476D1CE4_E5B9).wrapping_add(v6.port() as u64);
    let label_20 = (seed ^ (seed >> 21) ^ (seed >> 43)) as u32 & 0x000F_FFFF;

    let mut req = In6FlowlabelReq {
        flr_dst: unsafe { std::mem::transmute::<[u8;16], libc::in6_addr>(v6.ip().octets()) },
        flr_label: ((label_20 << 12) as u32).to_be(),
        flr_action: IPV6_FL_A_GET, // allocate if not present
        flr_share: IPV6_FL_S_EXCL,
        flr_flags: (IPV6_FL_F_CREATE | IPV6_FL_F_EXCL),
        flr_expires: V6_FLOWLABEL_EXPIRE_S,
        flr_linger: 0,
        __flr_pad: 0,
    };

    unsafe {
        let _ = libc::setsockopt(
            sock.as_raw_fd(),
            libc::IPPROTO_IPV6,
            IPV6_FLOWLABEL_MGR,
            &mut req as *mut _ as *mut libc::c_void,
            std::mem::size_of::<In6FlowlabelReq>() as libc::socklen_t,
        );
        // If kernel honors, the label becomes sticky on the socket; harmless no-op if unsupported.
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn set_v6_flowlabel_mgr(_sock: &Socket, _remote: &SocketAddr) {}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn get_outq_nsd_bytes(fd: c_int) -> Option<i32> {
    // SIOCOUTQNSD excludes bytes already handed to the NIC (TSQ), better signal for HOL risk.
    unsafe {
        let mut v: c_int = 0;
        if libc::ioctl(fd, SIOCOUTQNSD, &mut v as *mut c_int) == 0 { Some(v) } else { None }
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn get_outq_nsd_bytes(_fd: c_int) -> Option<i32> { None }

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn try_force_sndbuf(sock: &Socket, bytes: i32) {
    unsafe {
        // SO_SNDBUFFORCE = 32
        let _ = libc::setsockopt(
            sock.as_raw_fd(),
            libc::SOL_SOCKET,
            32,
            &bytes as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn try_force_sndbuf(_sock: &Socket, _bytes: i32) {}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn best_effort_tx_decoalesce(ifname: &Arc<str>) {
    unsafe {
        // ETHTOOL_SCOALESCE = 0x0000000E
        let mut ecoal = EthtoolCoalesce {
            cmd: 0x0000000E,
            rx_coalesce_usecs: 0, rx_max_coalesced_frames: 0,
            rx_coalesce_usecs_irq: 0, rx_max_coalesced_frames_irq: 0,
            tx_coalesce_usecs: 0, tx_max_coalesced_frames: 1, // 0us / 1 frame
            tx_coalesce_usecs_irq: 0, tx_max_coalesced_frames_irq: 1,
            stats_block_coalesce_usecs: 0,
            use_adaptive_rx_coalesce: 0, use_adaptive_tx_coalesce: 0,
            pkt_rate_low: 0, pkt_rate_high: 0,
            rx_coalesce_usecs_low: 0, rx_coalesce_usecs_high: 0,
            tx_coalesce_usecs_low: 0, tx_coalesce_usecs_high: 0,
            rx_max_coalesced_frames_low: 0, rx_max_coalesced_frames_high: 0,
            tx_max_coalesced_frames_low: 0, tx_max_coalesced_frames_high: 1,
            rate_sample_interval: 0,
        };

        let mut ifr = IfReq { ifr_name: [0; libc::IFNAMSIZ], ifr_data: (&mut ecoal as *mut _) as *mut libc::c_void };
        for (i, &b) in ifname.as_bytes().iter().take(libc::IFNAMSIZ - 1).enumerate() {
            ifr.ifr_name[i] = b as libc::c_char;
        }

        // Use any socket FD on that interface; SIOCSHWTSTAMP path gave us one already.
        // We'll open a dummy AF_INET socket if needed.
        let fd = libc::socket(libc::AF_INET, libc::SOCK_DGRAM, 0);
        if fd >= 0 {
            let _ = libc::ioctl(fd, SIOCETHTOOL, &mut ifr);
            libc::close(fd);
        }
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn best_effort_tx_decoalesce(_ifname: &Arc<str>) {}

// ADD
#[cfg(target_os = "linux")]
fn qdisc_supports_deadline(ifname: &Arc<str>) -> bool {
    // cheap sysfs probe: presence of "etf" or "fq" on any tx-queue qdisc
    let base = format!("/sys/class/net/{}/queues", ifname);
    if let Ok(dirs) = std::fs::read_dir(&base) {
        for d in dirs.flatten() {
            let p = d.path();
            if let Some(name) = p.file_name().and_then(|s| s.to_str()) {
                if name.starts_with("tx-") {
                    let qf = p.join("qdisc");
                    if let Ok(s) = std::fs::read_to_string(&qf) {
                        let t = s.trim();
                        if t.contains("etf") || t.contains("fq ") || t == "fq" { return true; }
                    }
                }
            }
        }
    }
    false
}
#[cfg(not(target_os = "linux"))]
fn qdisc_supports_deadline(_ifname: &Arc<str>) -> bool { false }

// ADD
#[cfg(target_os = "linux")]
fn bound_ifname_of(fd: c_int) -> Option<Arc<str>> {
    // Prefer SO_BINDTODEVICE name if present; otherwise infer from cached state at call sites.
    // Here we do a best-effort getsockopt(SO_BINDTODEVICE).
    let mut buf = [0u8; libc::IFNAMSIZ];
    let mut len = buf.len() as libc::socklen_t;
    let r = unsafe {
        libc::getsockopt(fd, libc::SOL_SOCKET, libc::SO_BINDTODEVICE,
            buf.as_mut_ptr() as *mut libc::c_void, &mut len)
    };
    if r == 0 && len > 0 {
        let name = std::str::from_utf8(&buf[..(len as usize).saturating_sub(1)]).ok()?;
        Some(Arc::from(name.trim_end_matches('\0')))
    } else { None }
}
#[cfg(not(target_os = "linux"))]
fn bound_ifname_of(_fd: c_int) -> Option<Arc<str>> { None }

// ADD
#[inline]
fn short_nanosleep(ns: u64) {
    let start = tsc_now_ns();
    while tsc_now_ns().saturating_sub(start) < ns {
        std::hint::spin_loop(); // inserts PAUSE on x86; power+latency friendly
    }
}

// ADD
#[cfg(target_os = "linux")]
struct CmsgTemplate {
    used_len: usize,   // exact used bytes
    stride:   usize,   // reserved slab stride (>= used_len)
    blob:     [u8; 3*64], // room for TXTIME + UDP_SEGMENT + TOS/TCLASS (<=3 headers)
    off_txtime: usize,
    off_gso:    Option<usize>,
    off_tos:    Option<usize>,
}

#[cfg(target_os = "linux")]
#[inline]
fn build_cmsg_template(attach_gso: bool, is_v6: bool, stride_hint: usize) -> CmsgTemplate {
    // Build once per call; memcpy into slabs for i=0..n
    let mut blob = [0u8; 3*64];
    let mut cursor = 0usize;
    // TXTIME header
    unsafe {
        let c = blob.as_mut_ptr().add(cursor) as *mut libc::cmsghdr;
        (*c).cmsg_level = libc::SOL_SOCKET;
        (*c).cmsg_type  = SO_TXTIME_CONST;
        (*c).cmsg_len   = CMSG_LEN(std::mem::size_of::<u64>() as u32) as usize;
    }
    let off_txtime = cursor + std::mem::size_of::<libc::cmsghdr>();
    cursor += unsafe { (*((blob.as_ptr().add(cursor)) as *const libc::cmsghdr)).cmsg_len } + std::mem::size_of::<libc::cmsghdr>();

    let mut off_gso = None;
    if attach_gso {
        unsafe {
            let c = blob.as_mut_ptr().add(cursor) as *mut libc::cmsghdr;
            (*c).cmsg_level = libc::IPPROTO_UDP;
            (*c).cmsg_type  = 103;
            (*c).cmsg_len   = CMSG_LEN(std::mem::size_of::<libc::c_ushort>() as u32) as usize;
        }
        off_gso = Some(cursor + std::mem::size_of::<libc::cmsghdr>());
        cursor += unsafe { (*((blob.as_ptr().add(cursor)) as *const libc::cmsghdr)).cmsg_len } + std::mem::size_of::<libc::cmsghdr>();
    }

    let mut off_tos = None;
    if ENABLE_SCM_DSCP_PER_COPY {
        unsafe {
            let c = blob.as_mut_ptr().add(cursor) as *mut libc::cmsghdr;
            (*c).cmsg_level = if is_v6 { libc::IPPROTO_IPV6 } else { libc::IPPROTO_IP };
            (*c).cmsg_type  = if is_v6 { 67 } else { libc::IP_TOS };
            (*c).cmsg_len   = CMSG_LEN(std::mem::size_of::<libc::c_int>() as u32) as usize;
        }
        off_tos = Some(cursor + std::mem::size_of::<libc::cmsghdr>());
        cursor += unsafe { (*((blob.as_ptr().add(cursor)) as *const libc::cmsghdr)).cmsg_len } + std::mem::size_of::<libc::cmsghdr>();
    }

    let used = cursor;
    CmsgTemplate {
        used_len: used,
        stride: stride_hint.max(used),
        blob, off_txtime, off_gso, off_tos
    }
}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn set_thread_timer_slack(ns: u64) {
    if !ENABLE_SET_TIMER_SLACK { return; }
    let _ = TIMER_SLACK_ONCE.get_or_init(|| unsafe {
        libc::prctl(PR_SET_TIMERSLACK, ns as libc::c_ulong, 0, 0, 0);
    });
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn set_thread_timer_slack(_ns: u64) {}

// ADD
#[inline(always)]
fn nic_rank_skew_ns(st: &ConnectionState, metrics: &Metrics) -> u64 {
    if !ENABLE_MULTI_NIC_SKEW { return 0; }
    if let Some(dev) = st.bound_device.as_ref() {
        // Rank NIC name deterministically among known devices
        let mut rank = 0usize;
        let mut list: Vec<_> = metrics.device_stats.keys().cloned().collect();
        list.sort_unstable();
        for (i, d) in list.iter().enumerate() {
            if d.as_ref() == dev.as_ref() { rank = i; break; }
        }
        return (rank as u64) * NIC_SKEW_STEP_NS;
    }
    0
}

// ADD
#[inline]
fn set_v4_df(sock: &Socket) {
    #[cfg(target_os = "linux")]
    unsafe {
        // IP_PMTUDISC_DO = 2
        let val: libc::c_int = 2;
        let _ = libc::setsockopt(
            sock.as_raw_fd(), libc::IPPROTO_IP, libc::IP_MTU_DISCOVER,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

// ADD
#[inline(always)]
fn gcd_u32(mut a: u32, mut b: u32) -> u32 {
    while b != 0 { let t = a % b; a = b; b = t; } a.max(1)
}

#[inline(always)]
fn best_udp_seg(payload_len: usize, is_v6: bool, path_mtu: Option<u32>) -> u16 {
    let hdr = if is_v6 { 48 } else { 28 }; // IPv6+UDP / IPv4+UDP
    let mtu = path_mtu.unwrap_or(if is_v6 { V6_MIN_MTU } else { V4_MIN_MTU })
                      .max(if is_v6 { V6_MIN_MTU } else { V4_MIN_MTU });
    if (mtu as usize) <= hdr { return 0; }
    let max_seg = ((mtu as usize) - hdr).min(payload_len).max(1);

    // Prefer factor-aligned seglen to avoid a short tail skb.
    let mut seg = gcd_u32(payload_len as u32, max_seg as u32) as usize;
    // Ensure segment count ≤ GSO_MAX_SEGS; relax alignment if necessary.
    let max_by_cnt = (payload_len + GSO_MAX_SEGS - 1) / GSO_MAX_SEGS;
    if seg < max_by_cnt { seg = max_by_cnt; }
    seg = seg.min(max_seg).max(1);
    (seg as u16)
}

// ADD
#[inline]
fn set_socket_dscp(sock: &Socket, remote: &SocketAddr) {
    let ecn = 0b10; // ECT(1)
    let tos: libc::c_int = ((DSCP_VALUE as u8) << 2 | ecn) as libc::c_int;
    unsafe {
        match remote {
            SocketAddr::V4(_) => {
                let _ = libc::setsockopt(
                    sock.as_raw_fd(), libc::IPPROTO_IP, libc::IP_TOS,
                    &tos as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
            SocketAddr::V6(_) => {
                // IPV6_TCLASS = 67
                let _ = libc::setsockopt(
                    sock.as_raw_fd(), libc::IPPROTO_IPV6, 67,
                    &tos as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }
    }
}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn bind_to_ifindex(sock: &Socket, ifname: &Arc<str>) {
    unsafe {
        if let Ok(cs) = std::ffi::CString::new(&**ifname) {
            let idx = if_nametoindex(cs.as_ptr());
            if idx > 0 {
                let _ = libc::setsockopt(
                    sock.as_raw_fd(), libc::SOL_SOCKET, SO_BINDTOIFINDEX,
                    &idx as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_uint>() as libc::socklen_t
                );
            }
        }
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn bind_to_ifindex(_sock: &Socket, _ifname: &Arc<str>) {}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn request_tx_rehash(fd: libc::c_int) {
    unsafe {
        let one: libc::c_int = 1;
        let _ = libc::setsockopt(
            fd, libc::SOL_SOCKET, SO_TXREHASH,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t
        );
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn request_tx_rehash(_fd: libc::c_int) {}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn try_set_mark(sock: &Socket, mark: u32) {
    unsafe {
        let v: libc::c_uint = mark;
        // SO_MARK = 36
        let _ = libc::setsockopt(
            sock.as_raw_fd(), libc::SOL_SOCKET, 36,
            &v as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_uint>() as libc::socklen_t
        );
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn try_set_mark(_sock: &Socket, _mark: u32) {}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn enable_prefer_busy_poll(sock: &Socket) {
    // SO_PREFER_BUSY_POLL = 69
    unsafe {
        let one: libc::c_int = 1;
        let _ = libc::setsockopt(
            sock.as_raw_fd(), libc::SOL_SOCKET, 69,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t
        );
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn enable_prefer_busy_poll(_sock: &Socket) {}

// ADD
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
#[inline(always)]
fn prefetch_read(p: *const u8) {
    unsafe { core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_T0) }
}
#[cfg(not(any(target_arch = "x86_64", target_arch = "x86")))]
#[inline(always)]
fn prefetch_read(_p: *const u8) {}

// ADD
#[inline]
fn nic_operstate_up_cached(st: &ConnectionState) -> bool {
    if !ENABLE_OPERSTATE_GUARD { return true; }
    let now = tsc_now_ns();
    let last = st.last_operstate_check_ns.load(Ordering::Relaxed);
    if now.saturating_sub(last) < OPERSTATE_RECHECK_NS {
        return st.operstate_up_cached.load(Ordering::Relaxed);
    }
    if let Some(dev) = st.bound_device.as_ref() {
        let path = format!("/sys/class/net/{}/operstate", dev);
        if let Ok(s) = std::fs::read_to_string(&path) {
            let up = s.trim() == "up";
            st.operstate_up_cached.store(up, Ordering::Relaxed);
            st.last_operstate_check_ns.store(now, Ordering::Relaxed);
            return up;
        }
    }
    // If unsure, assume up
    st.last_operstate_check_ns.store(now, Ordering::Relaxed);
    true
}

// ADD
#[inline]
fn nsd_bytes_to_ns(bytes: i32, bps: u64) -> u64 {
    if bps == 0 { return 0; }
    // Convert bytes in socket backlog to nominal dwell time at current pacing rate
    (bytes.max(0) as u64).saturating_mul(8_000_000_000u64 / 8).saturating_div(bps.max(1))
}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn enable_socket_zerocopy(sock: &Socket) {
    if !ENABLE_UDP_ZEROCOPY { return; }
    unsafe {
        let one: libc::c_int = 1;
        // SO_ZEROCOPY = 60
        let _ = libc::setsockopt(
            sock.as_raw_fd(), libc::SOL_SOCKET, 60,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t
        );
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn enable_socket_zerocopy(_sock: &Socket) {}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn get_socket_tos(fd: libc::c_int, is_v6: bool) -> Option<i32> {
    unsafe {
        let mut v: libc::c_int = 0;
        let mut l = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        let r = if is_v6 {
            libc::getsockopt(fd, libc::IPPROTO_IPV6, 67, &mut v as *mut _ as *mut _, &mut l)
        } else {
            libc::getsockopt(fd, libc::IPPROTO_IP, libc::IP_TOS, &mut v as *mut _ as *mut _, &mut l)
        };
        if r == 0 { Some(v) } else { None }
    }
}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn linux_sendmmsg_txtime_stack2(
    fd: libc::c_int,
    buf: &[u8],
    copies: usize, // 1..=2
    first_txtime_ns: u64,
    spacing_ns: u64,
    is_v6: bool,
    seg: u16, // 0 if none
    extra_flags: libc::c_int,
    dscp_first: u8,
    dscp_others: u8,
) -> std::io::Result<usize> {
    use libc::{mmsghdr, msghdr, iovec, cmsghdr};
    debug_assert!(copies >= 1 && copies <= 2);

    let mut bufs: [iovec; 2] = [iovec { iov_base: buf.as_ptr() as *mut _, iov_len: buf.len() }; 2];
    let mut ctrls: [[u8; 3*64]; 2] = [[0u8; 3*64]; 2];
    let mut hdrs: [msghdr; 2] = unsafe { std::mem::zeroed() };
    let mut msgs: [mmsghdr; 2] = unsafe { std::mem::zeroed() };

    for i in 0..copies {
        // Build cmsgs inline using the same template logic
        let c = ctrls[i].as_mut_ptr() as *mut cmsghdr;
        unsafe {
            // TXTIME
            (*c).cmsg_level = libc::SOL_SOCKET;
            (*c).cmsg_type  = SO_TXTIME_CONST;
            (*c).cmsg_len   = CMSG_LEN(std::mem::size_of::<u64>() as u32) as usize;
            let d1 = CMSG_DATA(c) as *mut u64;
            *d1 = first_txtime_ns + (i as u64) * spacing_ns;

            let mut next = (c as usize + (*c).cmsg_len + std::mem::size_of::<cmsghdr>()) as *mut cmsghdr;

            if seg > 0 {
                (*next).cmsg_level = libc::IPPROTO_UDP;
                (*next).cmsg_type  = 103;
                (*next).cmsg_len   = CMSG_LEN(std::mem::size_of::<libc::c_ushort>() as u32) as usize;
                let d2 = CMSG_DATA(next) as *mut libc::c_ushort;
                *d2 = seg;
                next = (next as usize + (*next).cmsg_len + std::mem::size_of::<cmsghdr>()) as *mut cmsghdr;
            }

            // DSCP/TCLASS
            let ecn = 0b10;
            let tos_byte: libc::c_int = (((if i == 0 { dscp_first } else { dscp_others }) << 2) | ecn) as libc::c_int;
            (*next).cmsg_level = if is_v6 { libc::IPPROTO_IPV6 } else { libc::IPPROTO_IP };
            (*next).cmsg_type  = if is_v6 { 67 } else { libc::IP_TOS };
            (*next).cmsg_len   = CMSG_LEN(std::mem::size_of::<libc::c_int>() as u32) as usize;
            let d3 = CMSG_DATA(next) as *mut libc::c_int;
            *d3 = tos_byte;
        }

        hdrs[i].msg_iov = &mut bufs[i] as *mut iovec;
        hdrs[i].msg_iovlen = 1;
        hdrs[i].msg_control = ctrls[i].as_mut_ptr() as *mut libc::c_void;
        hdrs[i].msg_controllen = ctrls[i].len();

        msgs[i].msg_hdr = hdrs[i];
        msgs[i].msg_len = 0;
    }

    let cnt = copies as u32;
    let r = sendmmsg_loop(fd, msgs.as_mut_ptr(), cnt, libc::MSG_DONTWAIT | extra_flags)?;
    Ok(r as usize)
}

// ADD
#[inline]
fn split_copies_across_sockets<'a>(
    st: &'a ConnectionState,
    copies: usize,
) -> smallvec::SmallVec<[(&'a Arc<UdpSocket>, usize); 8]> {
    use smallvec::SmallVec;
    let mut plan: SmallVec<[(&Arc<UdpSocket>, usize); 8]> = SmallVec::new();
    if copies == 0 { return plan; }

    // Primary gets the elite shot
    if let Ok(g) = st.udp_socket.try_lock() {
        if let Some(p) = g.as_ref() {
            plan.push((p, 1));
        }
    }

    let mut left = copies.saturating_sub(1);
    if left == 0 { return plan; }

    if let Ok(g) = st.alt_udp_sockets.try_lock() {
        let alts = g.as_ref();
        if let Some(alts) = alts {
            let n = alts.len().max(1);
            for (i, s) in alts.iter().enumerate() {
                if left == 0 { break; }
                // Even spread for remaining copies
                let take = ((left + (n - 1 - i)) / (n - i)).max(1).min(left);
                plan.push((s, take));
                left -= take;
            }
        }
    }
    if left > 0 {
        // Fallback: give leftover to primary (rare path)
        if let Some((p, c)) = plan.get_mut(0) { *c += left; }
    }
    plan
}

#[cfg(target_os = "linux")]
fn linux_sendmmsg(
    fd: libc::c_int,
    buf: &[u8],
    copies: usize,
    is_v6: bool,
    mtu_hint: Option<u32>,              // NEW
) -> std::io::Result<usize> {
    let n = copies.max(1).min(32);
    let (buf_ptr, buf_len) = (buf.as_ptr(), buf.len());

    let use_gso = ENABLE_PMTU_AWARE_GSO && can_use_udp_gso();
    let seg = if use_gso {
        let mt = mtu_hint.or_else(|| get_path_mtu(fd, is_v6))
                         .map(|m| m.max(if is_v6 { V6_MIN_MTU } else { V4_MIN_MTU }));
        clamp_udp_seg(buf_len, is_v6, mt)
    } else { 0 };
    let attach_gso = use_gso && (buf_len > seg as usize);
    let ctrl_stride: usize = if attach_gso { unsafe { CMSG_SPACE(std::mem::size_of::<libc::c_ushort>() as u32) } as usize } else { 0 };

    let sent = SEND_WS.with(|ws_cell| {
        let mut ws = ws_cell.borrow_mut();
        ws.ensure(n, ctrl_stride, buf_ptr, buf_len);
        
        // ADD: Prefetch payload and control data
        prefetch_t0(buf_ptr);
        #[cfg(target_os = "linux")]
        prefetch_t0(ws.ctrl.as_ptr());
        
        // ADD: NUMA rehome of control slabs
        #[cfg(target_os = "linux")]
        {
            if let Some(dev) = st.bound_device.as_ref() {
                let node = numa_node_of_dev(dev);
                ws.maybe_rehome_ctrl(node);
            }
        }

    for i in 0..n {
        let mut hdr: msghdr = unsafe { std::mem::zeroed() };
            hdr.msg_iov = &mut ws.iovs[i] as *mut iovec;
        hdr.msg_iovlen = 1;

            if attach_gso {
            unsafe {
                    let c = ws.ctrl_ptr(i);
                    (*c).cmsg_level = libc::IPPROTO_UDP;
                    (*c).cmsg_type  = 103; // UDP_SEGMENT
                    (*c).cmsg_len   = CMSG_LEN(std::mem::size_of::<libc::c_ushort>() as u32) as usize;
                    let d = CMSG_DATA(c) as *mut libc::c_ushort;
                    *d = seg;
                    hdr.msg_control = c as *mut libc::c_void;
                    hdr.msg_controllen = ws.ctrl_stride;
                }
            }

            ws.msgs[i].msg_hdr = hdr;
            ws.msgs[i].msg_len = 0;
        }

        sendmmsg_loop(fd, ws.msgs.as_mut_ptr(), n as u32, libc::MSG_DONTWAIT)
    })?;

    Ok(sent as usize)
}

#[cfg(not(target_os = "linux"))]
fn linux_sendmmsg(_fd: libc::c_int, _buf: &[u8], _copies: usize, _is_v6: bool, _mtu_hint: Option<u32>) -> io::Result<usize> {
    Err(io::Error::new(io::ErrorKind::Other, "sendmmsg unsupported"))
}

#[cfg(target_os = "linux")]
fn linux_sendmmsg_flags(fd: libc::c_int, buf: &[u8], copies: usize, flags: libc::c_int) -> std::io::Result<usize> {
    use libc::{mmsghdr, msghdr, iovec};
    let n = copies.max(1).min(32);
    let mut iovecs: Vec<iovec> = (0..n).map(|_| iovec {
        iov_base: buf.as_ptr() as *mut libc::c_void,
        iov_len:  buf.len(),
    }).collect();

    let mut msgs: Vec<mmsghdr> = Vec::with_capacity(n);
    for i in 0..n {
        let mut hdr: msghdr = unsafe { std::mem::zeroed() };
        hdr.msg_iov = &mut iovecs[i] as *mut iovec;
        hdr.msg_iovlen = 1;
        msgs.push(mmsghdr { msg_hdr: hdr, msg_len: 0 });
    }

    let sent = unsafe { libc::sendmmsg(fd, msgs.as_mut_ptr(), n as libc::c_uint, flags) };
    if sent < 0 { Err(std::io::Error::last_os_error()) } else { Ok(sent as usize) }
}

#[cfg(not(target_os = "linux"))]
fn linux_sendmmsg_flags(_fd: libc::c_int, _buf: &[u8], _copies: usize, _flags: libc::c_int) -> std::io::Result<usize> {
    Err(std::io::Error::new(std::io::ErrorKind::Other, "sendmmsg unsupported"))
}

#[cfg(target_os = "linux")]
fn linux_sendmsg_zerocopy(fd: libc::c_int, buf: &[u8]) -> std::io::Result<usize> {
    use libc::{msghdr, iovec};
    let mut iov = libc::iovec {
        iov_base: buf.as_ptr() as *mut libc::c_void,
        iov_len:  buf.len(),
    };
    let mut hdr: msghdr = unsafe { std::mem::zeroed() };
    hdr.msg_iov = &mut iov as *mut iovec;
    hdr.msg_iovlen = 1;

    let n = unsafe { libc::sendmsg(fd, &hdr, libc::MSG_ZEROCOPY) };
    if n < 0 { Err(std::io::Error::last_os_error()) } else { Ok(n as usize) }
}

#[cfg(target_os = "linux")]
fn linux_sendmmsg_txtime(
    fd: libc::c_int,
    buf: &[u8],
    copies: usize,
    first_txtime_ns: u64,
    spacing_ns: u64,
    is_v6: bool,
    mtu_hint: Option<u32>,              // NEW
    extra_flags: libc::c_int,           // NEW
) -> std::io::Result<usize> {
    let n = copies.max(1).min(32);
    let (buf_ptr, buf_len) = (buf.as_ptr(), buf.len());

    let use_gso = ENABLE_PMTU_AWARE_GSO && can_use_udp_gso();
    let seg = if use_gso {
        let mt = mtu_hint.or_else(|| get_path_mtu(fd, is_v6))
                         .map(|m| m.max(if is_v6 { V6_MIN_MTU } else { V4_MIN_MTU }));
        clamp_udp_seg(buf_len, is_v6, mt)
    } else { 0 };
    let attach_gso = use_gso && (buf_len > seg as usize);

    let txtime_space = unsafe { CMSG_SPACE(std::mem::size_of::<u64>() as u32) } as usize;
    let gso_space    = if attach_gso { unsafe { CMSG_SPACE(std::mem::size_of::<libc::c_ushort>() as u32) } as usize } else { 0 };
    let ctrl_stride  = txtime_space + gso_space;

    let sent = SEND_WS.with(|ws_cell| {
        let mut ws = ws_cell.borrow_mut();
        ws.ensure(n, ctrl_stride, buf_ptr, buf_len);
        
        // ADD: Prefetch payload and control data
        prefetch_t0(buf_ptr);
        #[cfg(target_os = "linux")]
        prefetch_t0(ws.ctrl.as_ptr());
        
        // ADD: NUMA rehome of control slabs
        #[cfg(target_os = "linux")]
        {
            if let Some(dev) = st.bound_device.as_ref() {
                let node = numa_node_of_dev(dev);
                ws.maybe_rehome_ctrl(node);
            }
        }

    for i in 0..n {
            unsafe {
                let c1 = ws.ctrl_ptr(i);
                (*c1).cmsg_level = libc::SOL_SOCKET;
                (*c1).cmsg_type  = SO_TXTIME_CONST;
                (*c1).cmsg_len   = CMSG_LEN(std::mem::size_of::<u64>() as u32) as usize;
                let d1 = CMSG_DATA(c1) as *mut u64;
                *d1 = first_txtime_ns + (i as u64) * spacing_ns;

                if attach_gso {
                    let c2 = (c1 as usize + (*c1).cmsg_len + std::mem::size_of::<cmsghdr>()) as *mut cmsghdr;
                    (*c2).cmsg_level = libc::IPPROTO_UDP;
                    (*c2).cmsg_type  = 103; // UDP_SEGMENT
                    (*c2).cmsg_len   = CMSG_LEN(std::mem::size_of::<libc::c_ushort>() as u32) as usize;
                    let d2 = CMSG_DATA(c2) as *mut libc::c_ushort;
                    *d2 = seg;
                }
            }

            let mut hdr: msghdr = unsafe { std::mem::zeroed() };
            hdr.msg_iov = &mut ws.iovs[i] as *mut iovec;
            hdr.msg_iovlen = 1;
            hdr.msg_control = ws.ctrl_ptr(i) as *mut libc::c_void;
            hdr.msg_controllen = ws.ctrl_stride;

            ws.msgs[i].msg_hdr = hdr;
            ws.msgs[i].msg_len = 0;
        }

        sendmmsg_loop(fd, ws.msgs.as_mut_ptr(), n as u32, libc::MSG_DONTWAIT | extra_flags)
    })?;

    Ok(sent as usize)
}

// ADD
#[cfg(target_os = "linux")]
#[inline(always)]
fn linux_sendmmsg_txtime_flags(
    fd: libc::c_int,
    buf: &[u8],
    copies: usize,
    first_txtime_ns: u64,
    spacing_ns: u64,
    is_v6: bool,
    extra_flags: libc::c_int,
) -> std::io::Result<usize> {
    let n = copies.max(1).min(32);
    let (buf_ptr, buf_len) = (buf.as_ptr(), buf.len());

    let use_gso = ENABLE_PMTU_AWARE_GSO && can_use_udp_gso();
    let seg = if use_gso { clamp_udp_seg(buf_len, is_v6, get_path_mtu(fd, is_v6)) } else { 0 };
    let attach_gso = use_gso && (buf_len > seg as usize);

    let txtime_space = unsafe { CMSG_SPACE(std::mem::size_of::<u64>() as u32) } as usize;
    let gso_space    = if attach_gso { unsafe { CMSG_SPACE(std::mem::size_of::<libc::c_ushort>() as u32) } as usize } else { 0 };
    let ctrl_stride  = txtime_space + gso_space;

    let sent = SEND_WS.with(|ws_cell| {
        let mut ws = ws_cell.borrow_mut();
        ws.ensure(n, ctrl_stride, buf_ptr, buf_len);
        
        // ADD: Prefetch payload and control data
        prefetch_t0(buf_ptr);
        #[cfg(target_os = "linux")]
        prefetch_t0(ws.ctrl.as_ptr());
        
        // ADD: NUMA rehome of control slabs
        #[cfg(target_os = "linux")]
        {
            if let Some(dev) = st.bound_device.as_ref() {
                let node = numa_node_of_dev(dev);
                ws.maybe_rehome_ctrl(node);
            }
        }

        let mut build_one = |i: usize| {
            unsafe {
                // CMSG #1: SO_TXTIME
                let c1 = ws.ctrl_ptr(i);
                (*c1).cmsg_level = libc::SOL_SOCKET;
                (*c1).cmsg_type  = SO_TXTIME_CONST;
                (*c1).cmsg_len   = CMSG_LEN(std::mem::size_of::<u64>() as u32) as usize;
                let d1 = CMSG_DATA(c1) as *mut u64;
                *d1 = first_txtime_ns + (i as u64) * spacing_ns;

                let mut next = (c1 as usize + (*c1).cmsg_len + std::mem::size_of::<libc::cmsghdr>()) as *mut libc::cmsghdr;

                if attach_gso {
                    (*next).cmsg_level = libc::IPPROTO_UDP;
                    (*next).cmsg_type  = 103;
                    (*next).cmsg_len   = CMSG_LEN(std::mem::size_of::<libc::c_ushort>() as u32) as usize;
                    let d2 = CMSG_DATA(next) as *mut libc::c_ushort;
                    *d2 = seg;
                    next = (next as usize + (*next).cmsg_len + std::mem::size_of::<libc::cmsghdr>()) as *mut libc::cmsghdr;
                }
            }
            let mut hdr: msghdr = std::mem::zeroed();
            hdr.msg_iov = &mut ws.iovs[i] as *mut iovec;
            hdr.msg_iovlen = 1;
            hdr.msg_control = ws.ctrl_ptr(i) as *mut libc::c_void;
            hdr.msg_controllen = ws.ctrl_stride;
            ws.msgs[i].msg_hdr = hdr;
            ws.msgs[i].msg_len = 0;
        };
        fill4!(n.min(4), build_one);
        for i in 4..n { build_one(i); }

        // Try with flags (e.g., MSG_ZEROCOPY); on EBUSY/EINVAL/EOPNOTSUPP, fall back once.
        let with_flags = sendmmsg_loop(fd, ws.msgs.as_mut_ptr(), n as u32, libc::MSG_DONTWAIT | extra_flags);
        match with_flags {
            Ok(v) => Ok(v),
            Err(e) => {
                let code = e.raw_os_error().unwrap_or(0);
                if code == libc::EBUSY || code == libc::EINVAL || code == libc::EOPNOTSUPP {
                    sendmmsg_loop(fd, ws.msgs.as_mut_ptr(), n as u32, libc::MSG_DONTWAIT)
                } else {
                    Err(e)
                }
            }
        }
    })?;

    Ok(sent as usize)
}

// ADD: io_uring ZC sender
#[cfg(target_os = "linux")]
#[inline]
fn uring_sendmmsg_txtime(
    fd: libc::c_int,
    buf: &[u8],
    copies: usize,
    first_txtime_ns: u64,
    spacing_ns: u64,
    is_v6: bool,
    seg: u16,                 // 0 if no GSO
    extra_flags: libc::c_int, // MSG_ZEROCOPY|MSG_CONFIRM|MSG_DONTWAIT added internally
    attach_dscp: bool,
    dscp_first: u8,
    dscp_others: u8,
) -> std::io::Result<usize> {
    use libc::{cmsghdr, iovec, mmsghdr, msghdr};
    let n = copies.max(1).min(32);
    let ring = URING.get().expect("uring");

    // Stack control/iov/hdr for up to 32 msgs (CMSG template & patch, as in your template path)
    let mut iovs: [iovec; 32] = [iovec{ iov_base: buf.as_ptr() as *mut _, iov_len: buf.len() }; 32];
    let mut ctrls: [[u8; 3*64]; 32] = [[0u8; 3*64]; 32];
    let mut hdrs: [msghdr; 32] = unsafe { std::mem::zeroed() };

    for i in 0..n {
        // Build cmsgs (TXTIME + optional UDP_SEGMENT + optional TOS/TCLASS)
        let c = ctrls[i].as_mut_ptr() as *mut cmsghdr;
        unsafe {
            // TXTIME
            (*c).cmsg_level = libc::SOL_SOCKET;
            (*c).cmsg_type  = SO_TXTIME_CONST;
            (*c).cmsg_len   = CMSG_LEN(std::mem::size_of::<u64>() as u32) as usize;
            let d1 = CMSG_DATA(c) as *mut u64;
            *d1 = first_txtime_ns + (i as u64) * spacing_ns;

            let mut next = (c as usize + (*c).cmsg_len + std::mem::size_of::<cmsghdr>()) as *mut cmsghdr;

            if seg > 0 {
                (*next).cmsg_level = libc::IPPROTO_UDP;
                (*next).cmsg_type  = 103; // UDP_SEGMENT
                (*next).cmsg_len   = CMSG_LEN(std::mem::size_of::<libc::c_ushort>() as u32) as usize;
                let d2 = CMSG_DATA(next) as *mut libc::c_ushort;
                *d2 = seg;
                next = (next as usize + (*next).cmsg_len + std::mem::size_of::<cmsghdr>()) as *mut cmsghdr;
            }

            if attach_dscp {
                let ecn = 0b10;
                let dscp = if i == 0 { dscp_first } else { dscp_others };
                let tos_byte: libc::c_int = ((dscp << 2) | ecn) as libc::c_int;
                (*next).cmsg_level = if is_v6 { libc::IPPROTO_IPV6 } else { libc::IPPROTO_IP };
                (*next).cmsg_type  = if is_v6 { 67 } else { libc::IP_TOS };
                (*next).cmsg_len   = CMSG_LEN(std::mem::size_of::<libc::c_int>() as u32) as usize;
                let d3 = CMSG_DATA(next) as *mut libc::c_int;
                *d3 = tos_byte;
            }
        }

        hdrs[i].msg_iov = &mut iovs[i] as *mut iovec;
        hdrs[i].msg_iovlen = 1;
        hdrs[i].msg_control = ctrls[i].as_mut_ptr() as *mut libc::c_void;
        hdrs[i].msg_controllen = ctrls[i].len();
    }

    // Submit n SendMsgZc SQEs, reap once
    let mut sub = ring.submission();
    unsafe {
        for i in 0..n {
            let msg_ptr = &mut hdrs[i] as *mut msghdr;
            let mut op = opcode::SendMsgZc::new(types::Fd(fd), msg_ptr)
                .flags((extra_flags | libc::MSG_DONTWAIT) as _);
            // Link submissions to keep order; only last without .flags will complete barrier
            let sqe = op.build()
                .user_data((i as u64) | 0x55_0000_0000_000000)
                .flags(io_uring::squeue::Flags::empty());
            sub.push(&sqe).unwrap();
        }
    }
    std::mem::drop(sub);
    ring.submit_and_wait(1).map_err(|e| std::io::Error::from_raw_os_error(e.raw_os_error().unwrap_or(libc::EIO)))?;

    // Reap all CQEs; count successful sends
    let mut ok = 0usize;
    let mut cqe_count = 0usize;
    let cq = ring.completion();
    for cqe in &*cq {
        cqe_count += 1;
        let res = cqe.result();
        if res >= 0 { ok += 1; }
    }
    // Safety: CQ is snapshot; drop after iter
    std::mem::drop(cq);
    Ok(ok)
}

#[inline(always)]
fn fast_hash_u64(mut x: u64) -> u64 {
    // SplitMix64 finalizer: fast, strong avalanche, no deps.
    x ^= x >> 30;
    x = x.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^= x >> 31;
    x
}

#[inline(always)]
fn addr_hash64(a: &SocketAddr) -> u64 {
    match a {
        SocketAddr::V4(v4) => {
            let ip = u32::from_be_bytes(v4.ip().octets()) as u64;
            fast_hash_u64((ip << 16) ^ (v4.port() as u64))
        }
        SocketAddr::V6(v6) => {
            let o = v6.ip().octets();
            let hi = u64::from_be_bytes([o[0],o[1],o[2],o[3],o[4],o[5],o[6],o[7]]);
            let lo = u64::from_be_bytes([o[8],o[9],o[10],o[11],o[12],o[13],o[14],o[15]]);
            fast_hash_u64(hi ^ lo ^ (v6.port() as u64))
        }
    }
}

#[inline(always)]
fn det_jitter_us(seed: u64, salt: u64, base: u64, span: u64) -> u64 {
    base + (fast_hash_u64(seed ^ salt) % span)
}

// ADD
// REPLACE (your previous micro_sleep_us)
#[inline]
async fn micro_sleep_us(us: u64) {
    if us == 0 { return; }
    if us <= MICRO_SPIN_THRESHOLD_US {
        #[cfg(target_os = "linux")]
        unsafe {
            // CLOCK_MONOTONIC_RAW avoids NTP slewing jitter
            let start = {
                let mut ts: libc::timespec = std::mem::zeroed();
                libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut ts);
                (ts.tv_sec as u64) * 1_000_000 + (ts.tv_nsec as u64) / 1_000
            };
            loop {
                std::hint::spin_loop();
                let mut ts: libc::timespec = std::mem::zeroed();
                libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut ts);
                let now = (ts.tv_sec as u64) * 1_000_000 + (ts.tv_nsec as u64) / 1_000;
                if now.wrapping_sub(start) >= us { break; }
            }
            return;
        }
        #[cfg(not(target_os = "linux"))]
        {
            // Portable best-effort spin
            for _ in 0..(us * 32).min(10_000) { std::hint::spin_loop(); }
            return;
        }
    }
    tokio::time::sleep(Duration::from_micros(us)).await;
}

// ADD
#[inline(always)]
fn prefetch_wire(buf: &[u8]) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        // Touch up to first 1 KiB (cache lines) — enough to cover kernel copy path.
        let len = buf.len().min(1024);
        let mut off = 0usize;
        while off < len {
            _mm_prefetch(buf.as_ptr().add(off) as *const i8, _MM_HINT_T0);
            off += 64;
        }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        let _ = buf; // no-op on non-x86
    }
}

// ADD
// IPv6 flowlabel + min-MTU + ECN ECT(1) helpers
#[cfg(any(target_os = "linux", target_os = "android"))]
#[inline]
fn set_v6_autoflowlabel(sock: &Socket) {
    if !ENABLE_V6_AUTOFLOWLABEL { return; }
    unsafe {
        let one: libc::c_int = 1;
        // IPV6_AUTOFLOWLABEL = 70
        let _ = libc::setsockopt(
            sock.as_raw_fd(),
            libc::IPPROTO_IPV6,
            70,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}
#[cfg(not(any(target_os = "linux", target_os = "android")))]
#[inline]
fn set_v6_autoflowlabel(_sock: &Socket) {}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[inline]
fn set_v6_use_min_mtu(sock: &Socket) {
    if !ENABLE_V6_USE_MIN_MTU { return; }
    unsafe {
        let one: libc::c_int = 1;
        // IPV6_USE_MIN_MTU = 42
        let _ = libc::setsockopt(
            sock.as_raw_fd(),
            libc::IPPROTO_IPV6,
            42,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}
#[cfg(not(any(target_os = "linux", target_os = "android")))]
#[inline]
fn set_v6_use_min_mtu(_sock: &Socket) {}

#[inline]
fn set_ecn_ect1(sock: &Socket, remote: &SocketAddr) {
    if !ENABLE_SET_ECN_ECT1 { return; }
    unsafe {
        // ECN bits '10' (ECT(1)). Compose with DSCP if you use DSCP_VALUE elsewhere.
        #[allow(unused_mut)]
        let mut tos: libc::c_int = ((DSCP_TOS as u8) << 2 | 0b10) as libc::c_int;
        match remote {
            SocketAddr::V4(_) => {
                let _ = libc::setsockopt(
                    sock.as_raw_fd(), libc::IPPROTO_IP, libc::IP_TOS,
                    &tos as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
            SocketAddr::V6(_) => {
                // IPV6_TCLASS = 67
                let _ = libc::setsockopt(
                    sock.as_raw_fd(), libc::IPPROTO_IPV6, 67,
                    &tos as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                );
            }
        }
    }
}

// ADD
// NUMA mbind of wire buffer to NIC node
#[cfg(target_os = "linux")]
#[inline]
fn nic_numa_node(ifname: &str) -> Option<i32> {
    let path = format!("/sys/class/net/{}/device/numa_node", ifname);
    fs::read_to_string(path).ok()
        .and_then(|s| s.trim().parse::<i32>().ok())
        .filter(|&n| n >= 0)
}

#[cfg(target_os = "linux")]
#[inline]
fn mbind_prefer(ptr: *const u8, len: usize, node: i32) -> bool {
    if node < 0 { return false; }
    unsafe {
        let page = libc::sysconf(libc::_SC_PAGESIZE) as usize;
        let base = (ptr as usize) & !(page - 1);
        let ceil = ((ptr as usize + len + page - 1) / page) * page;
        let bytes = ceil.saturating_sub(base);
        // simple 64-bit nodemask
        let mut mask: u64 = 1u64 << (node as u64);
        let r = mbind(
            base as *mut libc::c_void,
            bytes as libc::size_t,
            MPOL_PREFERRED,                        // prefer this node
            &mut mask as *mut _ as *mut libc::c_ulong,
            64,                                     // maxnode bits
            0,
        );
        r == 0
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn mbind_prefer(_ptr: *const u8, _len: usize, _node: i32) -> bool { false }

#[cfg(target_os = "linux")]
#[inline]
fn prefer_mbind_wire_to_device(buf: &[u8], ifname: &str) {
    if !ENABLE_NUMA_MBIND_WIRE { return; }
    if let Some(node) = nic_numa_node(ifname) {
        let _ = mbind_prefer(buf.as_ptr(), buf.len(), node);
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn prefer_mbind_wire_to_device(_buf: &[u8], _ifname: &str) {}

// ADD
// Proactive madvise(WILLNEED) on the wire buffer
#[cfg(target_os = "linux")]
#[inline]
fn madvise_willneed(buf: &[u8]) {
    if !ENABLE_MADVISE_WILLNEED || buf.is_empty() { return; }
    unsafe {
        let _ = libc::madvise(
            buf.as_ptr() as *mut libc::c_void,
            buf.len(),
            MADV_WILLNEED,
        );
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn madvise_willneed(_buf: &[u8]) {}

// ADD
// Socket hint: SO_PREFER_BUSY_POLL
#[cfg(target_os = "linux")]
#[inline]
fn setup_prefer_busy_poll(sock: &Socket) {
    if !ENABLE_PREFER_BUSY_POLL { return; }
    unsafe {
        let one: libc::c_int = 1;
        let _ = libc::setsockopt(
            sock.as_raw_fd(),
            libc::SOL_SOCKET,
            SO_PREFER_BUSY_POLL, // 69
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn setup_prefer_busy_poll(_sock: &Socket) {}

// ADD
// SCHED_DEADLINE burst guard
#[cfg(target_os = "linux")]
#[repr(C)]
struct SchedAttr {
    size: u32,
    sched_policy: u32,
    sched_flags: u64,
    sched_nice: i32,
    sched_priority: u32,
    // SCHED_DEADLINE fields
    sched_runtime: u64,
    sched_deadline: u64,
    sched_period: u64,
}

#[cfg(target_os = "linux")]
struct DlGuard {
    old_policy: i32,
    old_param: sched_param,
    did_dl: bool,
}
#[cfg(target_os = "linux")]
impl DlGuard {
    #[inline]
    fn enter(runtime_us: u64, deadline_us: u64, period_us: u64) -> Option<Self> {
        if !ENABLE_SCHED_DEADLINE_BURST { return None; }
        unsafe {
            let tid = 0;
            let cur_pol = sched_getscheduler(tid);
            let mut cur_prm: sched_param = std::mem::zeroed();
            if sched_getparam(tid, &mut cur_prm) != 0 { return None; }

            let mut attr = SchedAttr {
                size: std::mem::size_of::<SchedAttr>() as u32,
                sched_policy: 6, // SCHED_DEADLINE
                sched_flags: 0,
                sched_nice: 0,
                sched_priority: 0,
                sched_runtime:  runtime_us * 1000,
                sched_deadline: deadline_us * 1000,
                sched_period:   period_us * 1000,
            };

            let ret = syscall(
                SYS_SCHED_SETATTR,
                tid,
                &mut attr as *mut SchedAttr,
                0i32,
            );
            Some(Self { old_policy: cur_pol, old_param: cur_prm, did_dl: ret == 0 })
        }
    }
}
#[cfg(target_os = "linux")]
impl Drop for DlGuard {
    fn drop(&mut self) {
        unsafe {
            if self.did_dl {
                let _ = sched_setscheduler(0, self.old_policy, &self.old_param);
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
struct DlGuard;
#[cfg(not(target_os = "linux"))]
impl DlGuard { fn enter(_r:u64,_d:u64,_p:u64)->Option<Self>{ None } }

// ADD
// Move hot pages to NIC NUMA node
#[cfg(target_os = "linux")]
#[inline]
fn move_pages_wire(buf: &[u8], node: i32) {
    if !ENABLE_MOVE_PAGES_WIRE || buf.is_empty() || node < 0 { return; }
    unsafe {
        let page = libc::sysconf(libc::_SC_PAGESIZE) as usize;
        let base = (buf.as_ptr() as usize) & !(page - 1);
        let end  = ((buf.as_ptr() as usize + buf.len() + page - 1) / page) * page;
        let pages = (end - base) / page;
        if pages == 0 { return; }

        // Build arrays
        let mut addrs: Vec<*mut libc::c_void> = (0..pages).map(|i| (base + i*page) as *mut libc::c_void).collect();
        let nodes: Vec<c_int> = std::iter::repeat(node as c_int).take(pages).collect();
        let mut status: Vec<c_int> = vec![0; pages];

        let _ = move_pages(
            0,                           // self
            pages as c_int,
            addrs.as_mut_ptr(),
            nodes.as_ptr(),
            status.as_mut_ptr(),
            0,
        );
        // Best effort; errors are fine.
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn move_pages_wire(_buf: &[u8], _node: i32) {}

// ADD
// Hardware TX timestamping: SIOCSHWTSTAMP
#[cfg(target_os = "linux")]
#[repr(C)]
struct HwtstampConfig {
    flags: libc::c_int,
    tx_type: libc::c_int,
    rx_filter: libc::c_int,
}

#[cfg(target_os = "linux")]
#[repr(C)]
struct IfReq {
    ifr_name: [libc::c_char; IFNAMSIZ],
    ifr_data: *mut libc::c_void,
}

#[cfg(target_os = "linux")]
#[inline]
fn setup_hw_timestamping(sock: &Socket, ifname: &Arc<str>) {
    if !ENABLE_HW_TIMESTAMP_IOCTL { return; }
    unsafe {
        let mut cfg = HwtstampConfig {
            flags: 0,
            tx_type: 1,   // HWTSTAMP_TX_ON
            rx_filter: 1, // HWTSTAMP_FILTER_ALL (cheap; we only care TX)
        };
        let mut ifr = IfReq {
            ifr_name: [0; IFNAMSIZ],
            ifr_data: &mut cfg as *mut _ as *mut libc::c_void,
        };
        // Copy name bytes
        for (i, &b) in ifname.as_bytes().iter().take(IFNAMSIZ - 1).enumerate() {
            ifr.ifr_name[i] = b as libc::c_char;
        }
        // Best-effort; requires CAP_NET_ADMIN; harmless if it fails.
        let _ = ioctl(sock.as_raw_fd(), SIOCSHWTSTAMP, &mut ifr);
    }
}

#[cfg(not(target_os = "linux"))]
#[inline]
fn setup_hw_timestamping(_sock: &Socket, _ifname: &Arc<str>) {}

// ADD
// TLS SendWorkspace slabs: mlock to kill minor PF
#[cfg(target_os = "linux")]
#[inline]
fn try_mlock_ptr(ptr: *const u8, len: usize) {
    if !ENABLE_MLOCK_SEND_WS || len == 0 { return; }
    unsafe { let _ = mlock(ptr as *const _ as *const libc::c_void, len); }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn try_mlock_ptr(_ptr: *const u8, _len: usize) {}

// ADD
// IPv6 Don't-Fragment flag
#[cfg(any(target_os = "linux", target_os = "android"))]
#[inline]
fn set_v6_dontfrag(sock: &Socket) {
    unsafe {
        let one: libc::c_int = 1;
        // IPV6_DONTFRAG = 62
        let _ = libc::setsockopt(
            sock.as_raw_fd(),
            libc::IPPROTO_IPV6,
            62,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}
#[cfg(not(any(target_os = "linux", target_os = "android")))]
#[inline]
fn set_v6_dontfrag(_sock: &Socket) {}

// ADD
// IPv6 flowlabel: stable manual option (optional, safe)
#[cfg(any(target_os = "linux", target_os = "android"))]
#[inline]
fn try_set_v6_flowlabel(sock: &Socket, remote: &SocketAddr, label: u32) {
    if let SocketAddr::V6(_) = remote {
        // flowlabel is 20 bits
        let lbl = (label & 0x000F_FFFF) as libc::c_int;
        // flowlabel is 20 bits
        let _ = lbl; // placeholder: left as no-op intentionally to avoid per-send overhead.
    }
}
#[cfg(not(any(target_os = "linux", target_os = "android")))]
#[inline]
fn try_set_v6_flowlabel(_sock: &Socket, _remote: &SocketAddr, _label: u32) {}

// ADD
// XPS-aware CPU pinning for elite HFT performance
#[cfg(target_os = "linux")]
static XPS_PIN_ONCE: OnceLock<()> = OnceLock::new();

#[cfg(target_os = "linux")]
fn hexmask_first_cpu(hex: &str) -> Option<usize> {
    // hex is comma-separated little-endian bitmask chunks per /sys xps_cpus
    // We pick the lowest-numbered CPU bit.
    let mut bit_base = 0usize;
    for chunk in hex.trim().split(',').rev() {
        let v = usize::from_str_radix(chunk.trim(), 16).ok()?;
        if v != 0 {
            let lsb = v.trailing_zeros() as usize;
            return Some(bit_base + lsb);
        }
        bit_base += std::mem::size_of::<usize>() * 8;
    }
    None
}

#[cfg(target_os = "linux")]
fn pick_xps_cpu(ifname: &str) -> Option<usize> {
    // Prefer TX queue masks; fall back to RX if needed.
    let base = format!("/sys/class/net/{}/queues", ifname);
    let dirs = std::fs::read_dir(&base).ok()?;
    for d in dirs {
        let p = d.ok()?.path();
        let name = p.file_name()?.to_string_lossy();
        if name.starts_with("tx-") {
            let hp = p.join("xps_cpus");
            if let Ok(s) = std::fs::read_to_string(&hp) {
                if let Some(cpu) = hexmask_first_cpu(&s) { return Some(cpu); }
            }
        }
    }
    // fallback rx
    let dirs = std::fs::read_dir(&base).ok()?;
    for d in dirs {
        let p = d.ok()?.path();
        let name = p.file_name()?.to_string_lossy();
        if name.starts_with("rx-") {
            let hp = p.join("rps_cpus");
            if let Ok(s) = std::fs::read_to_string(&hp) {
                if let Some(cpu) = hexmask_first_cpu(&s) { return Some(cpu); }
            }
        }
    }
    None
}

#[cfg(target_os = "linux")]
fn pin_this_thread_to_cpu(cpu: usize) -> bool {
    // Build a raw bitmask; 16 u64s (1024 cpus) is enough on most boxes; kernel ignores extra zeros.
    let idx = cpu / 64;
    let bit = cpu % 64;
    let mut mask = vec![0u64; 16];
    if idx >= mask.len() { return false; }
    mask[idx] = 1u64 << bit;
    unsafe {
        let r = sched_setaffinity(0, (mask.len()*8) as libc::size_t, mask.as_ptr() as *const libc::c_void);
        r == 0
    }
}

#[cfg(target_os = "linux")]
fn pin_to_nic_xps_once(ifname: &str) {
    if !ENABLE_PIN_CPU_TO_XPS { return; }
    let _ = XPS_PIN_ONCE.get_or_init(|| {
        if let Some(cpu) = pick_xps_cpu(ifname) {
            let _ok = pin_this_thread_to_cpu(cpu);
        }
    });
}

#[cfg(not(target_os = "linux"))]
fn pin_to_nic_xps_once(_ifname: &str) {}

// ADD
// Timer slack harden for elite HFT wake determinism
#[cfg(target_os = "linux")]
#[inline]
fn set_thread_timer_slack(ns: u64) {
    if !ENABLE_SET_TIMER_SLACK { return; }
    let _ = TIMER_SLACK_ONCE.get_or_init(|| unsafe {
        libc::prctl(PR_SET_TIMERSLACK, ns as libc::c_ulong, 0, 0, 0);
    });
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn set_thread_timer_slack(_ns: u64) {}

// ADD
// IPv4 DF harden for elite HFT PMTU parity
#[inline]
fn set_v4_df(sock: &Socket) {
    #[cfg(target_os = "linux")]
    unsafe {
        // IP_PMTUDISC_DO = 2
        let val: libc::c_int = 2;
        let _ = libc::setsockopt(
            sock.as_raw_fd(), libc::IPPROTO_IP, libc::IP_MTU_DISCOVER,
            &val as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

// ADD
// Deterministic IPv6 flow-label manager for elite HFT ECMP stability
#[cfg(target_os = "linux")]
#[inline]
fn set_v6_flowlabel_mgr(sock: &Socket, remote: &SocketAddr) {
    if !ENABLE_V6_FLOWLABEL_MGR { return; }
    let v6 = match remote { SocketAddr::V6(v) => v, _ => return };
    // Deterministic 20-bit label from remote ip:port (stable across process lifetime)
    let mut seed = 0u64;
    for b in v6.ip().octets() { seed = seed.wrapping_mul(0x9E37_79B97F4A7C15).wrapping_add(b as u64 + 0x9E37); }
    seed = seed.wrapping_mul(0xBF58_476D1CE4_E5B9).wrapping_add(v6.port() as u64);
    let label_20 = (seed ^ (seed >> 21) ^ (seed >> 43)) as u32 & 0x000F_FFFF;

    let mut req = In6FlowlabelReq {
        flr_dst: unsafe { std::mem::transmute::<[u8;16], libc::in6_addr>(v6.ip().octets()) },
        flr_label: ((label_20 << 12) as u32).to_be(),
        flr_action: IPV6_FL_A_GET, // allocate if not present
        flr_share: IPV6_FL_S_EXCL,
        flr_flags: (IPV6_FL_F_CREATE | IPV6_FL_F_EXCL),
        flr_expires: V6_FLOWLABEL_EXPIRE_S,
        flr_linger: 0,
        __flr_pad: 0,
    };

    unsafe {
        let _ = libc::setsockopt(
            sock.as_raw_fd(),
            libc::IPPROTO_IPV6,
            IPV6_FLOWLABEL_MGR,
            &mut req as *mut _ as *mut libc::c_void,
            std::mem::size_of::<In6FlowlabelReq>() as libc::socklen_t,
        );
        // If kernel honors, the label becomes sticky on the socket; harmless no-op if unsupported.
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn set_v6_flowlabel_mgr(_sock: &Socket, _remote: &SocketAddr) {}

// ADD
// Unroll control fill for ≤4 copies for elite HFT performance
macro_rules! fill4 {
    ($n:expr, $build_one:expr) => {
        if $n >= 1 { $build_one(0); }
        if $n >= 2 { $build_one(1); }
        if $n >= 3 { $build_one(2); }
        if $n >= 4 { $build_one(3); }
    };
}

// ADD
// Adaptive SO_BUSY_POLL_BUDGET for elite HFT performance
#[cfg(target_os = "linux")]
#[inline]
fn maybe_adjust_busy_poll_budget(fd: libc::c_int, st: &ConnectionState, metrics: &Metrics) {
    if !ENABLE_ADAPT_BUSY_POLL_BUDGET { return; }
    unsafe {
        let want = if let Some(dev) = st.bound_device.as_ref() {
            if let Some(ds) = metrics.device_stats.get(dev) {
                let q = ds.avg_tx_queue_ns.load(Ordering::Relaxed);
                if q >= BUSY_POLL_Q_VHOT_NS { BUSY_POLL_BUDGET_VHOT }
                else if q >= BUSY_POLL_Q_HOT_NS { BUSY_POLL_BUDGET_HOT }
                else { BUSY_POLL_BUDGET_COOL }
            } else { BUSY_POLL_BUDGET_COOL }
        } else { BUSY_POLL_BUDGET_COOL };
        // SO_BUSY_POLL_BUDGET = 70
        let _ = libc::setsockopt(
            fd, libc::SOL_SOCKET, 70,
            &want as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn maybe_adjust_busy_poll_budget(_fd: libc::c_int, _st: &ConnectionState, _m: &Metrics) {}

// ADD
// Adversity-triggered port rotation for elite HFT resilience
#[inline]
fn maybe_rotate_src_port(st: &ConnectionState) -> bool {
    if !ENABLE_ROTATE_SRC_ON_ADVERSITY { return false; }
    if let (Ok(mut prim), Ok(mut alts)) = (st.udp_socket.lock(), st.alt_udp_sockets.lock()) {
        if let Some(ref mut vec) = alts.as_mut() {
            if !vec.is_empty() {
                // ADD: Choose alt index pseudo-randomly instead of pop_back()
                let i = (xrand_u32() as usize) % vec.len();
                let new_sock = vec.swap_remove(i);
                if let Some(old) = prim.replace(new_sock.clone()) { vec.push(old); }
                
                // ADD: Refresh PMTU after port rotation
                #[cfg(target_os = "linux")]
                if let Some(u) = prim.as_ref() { 
                    refresh_path_mtu(u.as_raw_fd(), st.addr.is_ipv6(), st); 
                }
                
                return true;
            }
        }
    }
    false
}

// ADD
// One-shot errqueue drain by fd for elite HFT robustness
#[cfg(target_os = "linux")]
#[inline]
fn errqueue_drain_once_fd(fd: libc::c_int, batch: u32) {
    use libc::{mmsghdr, msghdr, iovec, MSG_ERRQUEUE, MSG_DONTWAIT};
    unsafe {
        let n = batch.max(1).min(32) as usize;
        let mut bufs: Vec<[u8; 64]> = vec![[0u8; 64]; n];
        let mut cbufs: Vec<[u8; 512]> = vec![[0u8; 512]; n];
        let mut iovs: Vec<iovec> = Vec::with_capacity(n);
        let mut hdrs: Vec<msghdr> = Vec::with_capacity(n);
        let mut msgs: Vec<mmsghdr> = Vec::with_capacity(n);
        for i in 0..n {
            iovs.push(iovec { iov_base: bufs[i].as_mut_ptr() as *mut _, iov_len: bufs[i].len() });
            hdrs.push(std::mem::zeroed());
            let h = hdrs.last_mut().unwrap();
            h.msg_iov = &mut iovs[i] as *mut iovec;
            h.msg_iovlen = 1;
            h.msg_control = cbufs[i].as_mut_ptr() as *mut libc::c_void;
            h.msg_controllen = cbufs[i].len();
            msgs.push(mmsghdr { msg_hdr: *h, msg_len: 0 });
        }
        let _ = libc::recvmmsg(fd, msgs.as_mut_ptr(), n as u32, (MSG_ERRQUEUE | MSG_DONTWAIT) as i32, std::ptr::null_mut());
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn errqueue_drain_once_fd(_fd: libc::c_int, _batch: u32) {}

// ADD
// Safety nits: don't underflow inflight counter for elite HFT robustness
#[inline]
fn saturating_dec(a: &AtomicU64, by: u64) {
    let mut cur = a.load(Ordering::Relaxed);
    loop {
        let next = cur.saturating_sub(by);
        match a.compare_exchange_weak(cur, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(v) => cur = v,
        }
    }
}

// ADD
// Micro-slew TXTIME offset from TX timestamps for elite HFT precision
#[cfg(target_os = "linux")]
#[inline]
fn maybe_slew_txtime_offset(tx_ack_ns_kernel: u64, tx_ack_ns_tsc: u64) {
    // simple IIR: off := off + (kernel - tsc)/64
    let off = TXT_OFFSET_NS.load(Ordering::Relaxed) as i128;
    let err = (tx_ack_ns_kernel as i128) - (tx_ack_ns_tsc as i128 + off);
    let adj = (err / 64).clamp(i64::MIN as i128, i64::MAX as i128) as i64;
    TXT_OFFSET_NS.fetch_add(adj, Ordering::Relaxed);
}

// ADD
// EINTR-safe sendmmsg loop for elite HFT robustness
#[cfg(target_os = "linux")]
#[inline]
fn sendmmsg_loop(fd: libc::c_int, vec: *mut libc::mmsghdr, cnt: u32, flags: libc::c_int) -> std::io::Result<i32> {
    loop {
        let r = unsafe { libc::sendmmsg(fd, vec, cnt, flags) };
        if r >= 0 { return Ok(r); }
        let e = std::io::Error::last_os_error();
        match e.raw_os_error() {
            Some(libc::EINTR) => continue,
            Some(libc::EAGAIN) | Some(libc::ENOBUFS) if ENABLE_EAGAIN_ERRQUEUE_RETRY => {
                errqueue_drain_once_fd(fd, ERRQUEUE_RETRY_BATCH);
                // single retry
                let r2 = unsafe { libc::sendmmsg(fd, vec, cnt, flags) };
                if r2 >= 0 { return Ok(r2); }
                return Err(std::io::Error::last_os_error());
            }
            _ => return Err(e),
        }
    }
}

// ADD
// TXTIME epoch offset updater for elite HFT correctness
#[cfg(target_os = "linux")]
#[inline]
fn update_txtime_epoch_offset(chosen_clock: libc::clockid_t) {
    unsafe {
        // Calibrate once: offset = clock_now(chosen) - tsc_now_ns()
        let mut ts: libc::timespec = std::mem::zeroed();
        if libc::clock_gettime(chosen_clock, &mut ts) == 0 {
            let clk_ns = (ts.tv_sec as i128)*1_000_000_000i128 + (ts.tv_nsec as i128);
            let tsc_ns = tsc_now_ns() as i128; // your calibrated RAW/TAI-coherent tsc
            let off   = (clk_ns - tsc_ns).clamp(i64::MIN as i128, i64::MAX as i128) as i64;
            TXT_CLOCKID.store(chosen_clock, Ordering::Relaxed);
            TXT_OFFSET_NS.store(off, Ordering::Relaxed);
        }
    }
}

// ADD
// Micro-icache wins: prefetch control slabs for elite HFT performance
#[cfg(any(target_arch = "x86_64", target_arch = "x86"))]
#[inline(always)]
fn prefetch_write(p: *const u8) {
    unsafe { core::arch::x86_64::_mm_prefetch(p as *const i8, core::arch::x86_64::_MM_HINT_T0) }
}
#[cfg(not(any(target_arch = "x86_64", target_arch = "x86")))]
#[inline(always)]
fn prefetch_write(_p: *const u8) {}

// ADD
// Single-copy fast path for elite HFT performance
#[cfg(target_os = "linux")]
#[inline]
fn linux_send_one_fast(fd: libc::c_int, buf: &[u8]) -> std::io::Result<usize> {
    let n = unsafe { libc::send(fd, buf.as_ptr() as *const libc::c_void, buf.len(), libc::MSG_DONTWAIT) };
    if n < 0 { Err(std::io::Error::last_os_error()) } else { Ok(n as usize) }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn linux_send_one_fast(_fd: libc::c_int, _buf: &[u8]) -> std::io::Result<usize> { 
    Err(std::io::Error::from(std::io::ErrorKind::Unsupported)) 
}

// ADD
// Enable IPv6 Path MTU cmsgs
#[cfg(any(target_os = "linux", target_os = "android"))]
#[inline]
fn setup_recv_path_mtu(sock: &Socket, remote: &SocketAddr) {
    if !ENABLE_IPV6_RECVPATHMTU { return; }
    if let SocketAddr::V6(_) = remote {
        unsafe {
            let one: libc::c_int = 1;
            // IPV6_RECVPATHMTU = 61
            let _ = libc::setsockopt(
                sock.as_raw_fd(), libc::IPPROTO_IPV6, 61,
                &one as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t
            );
        }
    }
}
#[cfg(not(any(target_os = "linux", target_os = "android")))]
#[inline]
fn setup_recv_path_mtu(_sock: &Socket, _remote: &SocketAddr) {}

#[inline]
fn send_jitter_us(remote: &SocketAddr, sig_hash64: u64, slot: u64, base: u64, span: u64) -> u64 {
    // Seed = addr_hash ⊕ sig ⊕ slot-derived salt
    let seed = addr_hash64(remote) ^ sig_hash64;
    let salt = (slot << 1) ^ fast_hash_u64(slot);
    det_jitter_us(seed, salt, base, span)
}

#[cfg(target_os = "linux")]
fn pick_bind_device(remote: &SocketAddr) -> Option<String> {
    // TPU_BIND_DEVICE_LIST="eth0,eth1,eno1" → stable hash-based pick per remote
    if let Ok(list) = std::env::var("TPU_BIND_DEVICE_LIST") {
        let devs: Vec<_> = list.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()).collect();
        if !devs.is_empty() {
            let h = addr_hash64(remote) as usize;
            return Some(devs[h % devs.len()].to_string());
        }
    }
    // Fallback: single device name
    if let Ok(one) = std::env::var("TPU_BIND_DEVICE") {
        if !one.is_empty() { return Some(one); }
    }
    None
}
#[cfg(not(target_os = "linux"))]
fn pick_bind_device(_remote: &SocketAddr) -> Option<String> { None }

struct MlockGuard {
    ptr: *const u8,
    len: usize,
}

impl MlockGuard {
    #[inline]
    fn new(buf: &[u8]) -> Self {
        let g = Self { ptr: buf.as_ptr(), len: buf.len() };
        #[cfg(target_os = "linux")]
        unsafe {
            let _ = libc::mlock(g.ptr as *const _, g.len);
        }
        g
    }
}
impl Drop for MlockGuard {
    fn drop(&mut self) {
        #[cfg(target_os = "linux")]
        unsafe {
            let _ = libc::munlock(self.ptr as *const _, self.len);
        }
    }
}

#[cfg(target_os = "linux")]
fn set_thread_rt_and_affinity(cpu: usize, rt_prio: i32) {
    unsafe {
        // sched_setaffinity
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        let _ = libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set);

        // SCHED_FIFO priority (requires CAP_SYS_NICE; ignore error if missing)
        let param = libc::sched_param { sched_priority: rt_prio };
        let _ = libc::sched_setscheduler(0, libc::SCHED_FIFO, &param);
    }
}
#[cfg(not(target_os = "linux"))]
fn set_thread_rt_and_affinity(_cpu: usize, _rt_prio: i32) {}

#[cfg(target_os = "linux")]
#[inline]
fn setup_udp_zerocopy(sock: &Socket) {
    if !ENABLE_UDP_ZEROCOPY { return; }
    unsafe {
        let one: libc::c_int = 1;
        let _ = libc::setsockopt(
            sock.as_raw_fd(),
            libc::SOL_SOCKET,
            60, // SO_ZEROCOPY
            &one as *const _ as *const libc::c_void,
            size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

// REPLACE (your previous setup_tx_timestamping)
#[cfg(target_os = "linux")]
#[inline]
fn setup_tx_timestamping(sock: &Socket) {
    if !ENABLE_TX_TIMESTAMPING { return; }
    unsafe {
        // SOF_TIMESTAMPING_* flags
        let flags: libc::c_int =
            (1 <<  1)  | // TX_SOFTWARE
            (1 <<  2)  | // TX_HARDWARE
            (1 <<  4)  | // SOFTWARE
            (1 <<  6)  | // RAW_HARDWARE
            (1 << 10) | // OPT_CMSG
            (1 << 11) | // OPT_TSONLY
            (1 << 12) | // TX_SCHED
            (1 << 13);  // TX_ACK
        let _ = libc::setsockopt(
            sock.as_raw_fd(),
            libc::SOL_SOCKET,
            37, // SO_TIMESTAMPING
            &flags as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

#[cfg(not(target_os = "linux"))]
#[inline]
fn setup_udp_zerocopy(_sock: &Socket) {}

#[cfg(not(target_os = "linux"))]
#[inline]
fn setup_tx_timestamping(_sock: &Socket) {}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn setup_busy_poll(sock: &Socket) {
    if !ENABLE_BUSY_POLL { return; }
    unsafe {
        let fd = sock.as_raw_fd();
        // SO_BUSY_POLL = 46, SO_PREFER_BUSY_POLL = 69
        let one: libc::c_int = 1;
        let _ = libc::setsockopt(
            fd, libc::SOL_SOCKET, 69,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        let _ = libc::setsockopt(
            fd, libc::SOL_SOCKET, 46,
            &BUSY_POLL_US as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}

#[cfg(not(target_os = "linux"))]
#[inline]
fn setup_busy_poll(_sock: &Socket) {}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn setup_busy_poll_budget(sock: &Socket) {
    if !ENABLE_BUSY_POLL { return; }
    unsafe {
        // SO_BUSY_POLL_BUDGET = 70
        let _ = libc::setsockopt(
            sock.as_raw_fd(), libc::SOL_SOCKET, 70,
            &BUSY_POLL_BUDGET as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn setup_busy_poll_budget(_sock: &Socket) {}

// ADD
#[cold]
#[inline]
fn classify_and_maybe_quarantine(e: &std::io::Error, st: &ConnectionState) {
    if let Some(code) = e.raw_os_error() {
        let now = now_unix_ms();
        // Local congestion: never quarantine, just record (copies may clamp elsewhere)
        if code == libc::ENOBUFS || code == libc::EAGAIN || code == libc::EWOULDBLOCK { 
            // ADD: Adversity accounting for port rotation
            let now_ns = tsc_now_ns();
            let last = st.adversity_last_ns.swap(now_ns, Ordering::Relaxed);
            let age_ms = (now_ns.saturating_sub(last) / 1_000_000) as u64;
            if age_ms > ADVERSITY_DECAY_MS {
                st.adversity_score.store(1, Ordering::Relaxed);
            } else {
                let v = st.adversity_score.fetch_add(1, Ordering::Relaxed).saturating_add(1);
                if v >= ADVERSITY_THRESH {
                    st.adversity_score.store(0, Ordering::Relaxed);
                    let _ = maybe_rotate_src_port(st);
                    
                    // ADD: Start DSCP booster window
                    if ENABLE_DSCP_BOOST_ON_ADVERSITY {
                        let now = tsc_now_ns();
                        st.dscp_boost_until_ns.store(now.saturating_add(DSCP_BOOST_NS), Ordering::Relaxed);
                    }
                }
            }
            return; 
        }
        // REPLACE (add one more branch in classify_and_maybe_quarantine)
        if code == libc::ETIMEDOUT {
            st.is_healthy.store(false, Ordering::Relaxed);
            st.quarantine_until_ms.store(now + 3_000, Ordering::Relaxed);
            // ADD: Adversity accounting for port rotation
            let now_ns = tsc_now_ns();
            let last = st.adversity_last_ns.swap(now_ns, Ordering::Relaxed);
            let age_ms = (now_ns.saturating_sub(last) / 1_000_000) as u64;
            if age_ms > ADVERSITY_DECAY_MS {
                st.adversity_score.store(1, Ordering::Relaxed);
            } else {
                let v = st.adversity_score.fetch_add(1, Ordering::Relaxed).saturating_add(1);
                if v >= ADVERSITY_THRESH {
                    st.adversity_score.store(0, Ordering::Relaxed);
                    let _ = maybe_rotate_src_port(st);
                    
                    // ADD: Start DSCP booster window
                    if ENABLE_DSCP_BOOST_ON_ADVERSITY {
                        let now = tsc_now_ns();
                        st.dscp_boost_until_ns.store(now.saturating_add(DSCP_BOOST_NS), Ordering::Relaxed);
                    }
                }
            }
            return;
        }
        // MTU mismatch: mark unhealthy briefly to force refresh
        if code == libc::EMSGSIZE {
            st.is_healthy.store(false, Ordering::Relaxed);
            st.quarantine_until_ms.store(now + 2_000, Ordering::Relaxed);
            return;
        }
        // Routing/socket hard errors — quarantine harder
        if code == libc::ENETUNREACH || code == libc::EHOSTUNREACH || code == libc::ENETDOWN || code == libc::ECONNREFUSED {
            st.is_healthy.store(false, Ordering::Relaxed);
            st.quarantine_until_ms.store(now + 5_000, Ordering::Relaxed);
            return;
        }
    }
}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn try_force_sndbuf(sock: &Socket, target_bytes: i32) {
    if !ENABLE_SNDBUFFORCE { return; }
    unsafe {
        let fd = sock.as_raw_fd();
        // SO_SNDBUFFORCE = 32
        let _ = libc::setsockopt(
            fd, libc::SOL_SOCKET, 32,
            &target_bytes as *const _ as *const libc::c_void,
            std::mem::size_of::<i32>() as libc::socklen_t,
        );
    }
}

#[cfg(not(target_os = "linux"))]
#[inline]
fn try_force_sndbuf(_sock: &Socket, _target_bytes: i32) {}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn tighten_timer_slack(ns: u64) {
    unsafe {
        // PR_SET_TIMERSLACK = 29
        let _ = libc::prctl(29, ns as libc::c_ulong, 0, 0, 0);
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn tighten_timer_slack(_ns: u64) {}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn set_ip_bind_no_port(sock: &Socket, remote: &SocketAddr) {
    if !ENABLE_IP_BIND_NO_PORT { return; }
    // Only v4 supports IP_BIND_ADDRESS_NO_PORT; v6 gets no-op.
    if remote.is_ipv4() {
        unsafe {
            let one: libc::c_int = 1;
            let _ = libc::setsockopt(
                sock.as_raw_fd(),
                libc::IPPROTO_IP,
                24, // IP_BIND_ADDRESS_NO_PORT
                &one as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
}

#[cfg(not(target_os = "linux"))]
#[inline]
fn set_ip_bind_no_port(_sock: &Socket, _remote: &SocketAddr) {}

#[cfg(target_os = "linux")]
#[inline]
fn set_max_pacing_rate(sock: &Socket) {
    if !ENABLE_MAX_PACING_RATE { return; }
    unsafe {
        let rate: u64 = MAX_PACING_BPS;
        // SO_MAX_PACING_RATE = 47
        let _ = libc::setsockopt(
            sock.as_raw_fd(),
            libc::SOL_SOCKET,
            47,
            &rate as *const _ as *const libc::c_void,
            std::mem::size_of::<u64>() as libc::socklen_t,
        );
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn set_max_pacing_rate(_sock: &Socket) {}

// ADD
#[inline]
fn set_hoplimit(sock: &Socket, remote: &SocketAddr) {
    #[cfg(any(target_os = "linux", target_os = "android", target_os = "freebsd"))]
    unsafe {
        if remote.is_ipv4() {
            // IP_TTL = 2
            let ttl: libc::c_int = DEFAULT_HOPS as libc::c_int;
            let _ = libc::setsockopt(
                sock.as_raw_fd(),
                libc::IPPROTO_IP,
                libc::IP_TTL,
                &ttl as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        } else {
            // IPV6_UNICAST_HOPS = 16 (Linux/Android/FreeBSD)
            let hops: libc::c_int = DEFAULT_HOPS as libc::c_int;
            let _ = libc::setsockopt(
                sock.as_raw_fd(),
                libc::IPPROTO_IPV6,
                16,
                &hops as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
}

// ADD
#[inline]
fn udp_overhead(is_v6: bool) -> u32 { if is_v6 { 40 + 8 } else { 20 + 8 } }

#[cfg(target_os = "linux")]
#[inline]
fn set_pmtu_discover(sock: &Socket, remote: &SocketAddr) {
    if !ENABLE_PMTU_AWARE_GSO { return; }
    unsafe {
        if remote.is_ipv4() {
            let val: libc::c_int = libc::IP_PMTUDISC_DO;
            let _ = libc::setsockopt(
                sock.as_raw_fd(),
                libc::IPPROTO_IP,
                libc::IP_MTU_DISCOVER,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        } else {
            let val: libc::c_int = libc::IPV6_PMTUDISC_DO;
            let _ = libc::setsockopt(
                sock.as_raw_fd(),
                libc::IPPROTO_IPV6,
                libc::IPV6_MTU_DISCOVER,
                &val as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t,
            );
        }
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn set_pmtu_discover(_sock: &Socket, _remote: &SocketAddr) {}

#[cfg(target_os = "linux")]
#[inline]
fn get_path_mtu(fd: libc::c_int, is_v6: bool) -> Option<u32> {
    unsafe {
        let mut mtu: libc::c_int = 0;
        let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        let ret = if is_v6 {
            libc::getsockopt(fd, libc::IPPROTO_IPV6, libc::IPV6_MTU, &mut mtu as *mut _ as *mut _, &mut len)
        } else {
            libc::getsockopt(fd, libc::IPPROTO_IP, libc::IP_MTU, &mut mtu as *mut _ as *mut _, &mut len)
        };
        if ret == 0 && mtu > 0 { Some(mtu as u32) } else { None }
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn get_path_mtu(_fd: libc::c_int, _is_v6: bool) -> Option<u32> { None }

#[inline]
fn clamp_udp_seg(payload_len: usize, is_v6: bool, maybe_mtu: Option<u32>) -> u16 {
    let fallback = if is_v6 { V6_SAFE_SEG } else { V4_SAFE_SEG };
    let seg = if let Some(mtu) = maybe_mtu {
        let oh = udp_overhead(is_v6);
        let max_payload = mtu.saturating_sub(oh).max(512);
        (max_payload as usize).min(payload_len).max(1) as u16
    } else {
        fallback.min(payload_len as u16).max(1)
    };
    seg
}

// REPLACE
#[cfg(target_os = "linux")]
thread_local! {
    static SEND_WS: RefCell<SendWorkspace> = RefCell::new(SendWorkspace::new());
}

#[cfg(target_os = "linux")]
#[repr(align(64))]
struct SendWorkspace {
    iovs: Vec<iovec>,
    msgs: Vec<mmsghdr>,
    ctrl: Vec<u8>,       // contiguous slab, split into per-message strides
    ctrl_stride: usize,
}
#[cfg(target_os = "linux")]
impl SendWorkspace {
    fn new() -> Self {
        Self { iovs: Vec::with_capacity(32), msgs: Vec::with_capacity(32), ctrl: Vec::with_capacity(32*64), ctrl_stride: 0 }
    }
    #[cfg(target_os = "linux")]
    fn ensure(&mut self, n: usize, ctrl_stride: usize, buf_ptr: *const u8, buf_len: usize) {
        if self.iovs.len() < n { self.iovs.resize_with(n, || iovec { iov_base: buf_ptr as *mut _, iov_len: buf_len }); }
        for i in 0..n { self.iovs[i].iov_base = buf_ptr as *mut _; self.iovs[i].iov_len = buf_len; }
        if self.msgs.len() < n { self.msgs.resize_with(n, || mmsghdr { msg_hdr: unsafe { std::mem::zeroed() }, msg_len: 0 }); }

        if self.ctrl_stride != ctrl_stride {
            self.ctrl.clear();
            self.ctrl_stride = ctrl_stride;
        }
        let need = n.saturating_mul(ctrl_stride);
        if self.ctrl.len() < need {
            let old_ptr = self.ctrl.as_ptr();
            let old_len = self.ctrl.len();
            // Round up to next 64B multiple
            let rounded = (need + 63) & !63;
            self.ctrl.resize(rounded, 0);
            // mlock old + new ranges best-effort
            if old_len > 0 { try_mlock_ptr(old_ptr, old_len); }
            try_mlock_ptr(self.ctrl.as_ptr(), self.ctrl.len());
        }
        // NEW: prefetch next control slabs we'll write into
        if self.ctrl_stride > 0 {
            for i in 0..n {
                let p = unsafe { self.ctrl.as_ptr().add(i * self.ctrl_stride) };
                prefetch_write(p);
            }
        }
    }
    #[cfg(target_os = "linux")]
    fn maybe_rehome_ctrl(&mut self, prefer_node: i32) {
        if prefer_node >= 0 && !self.ctrl.is_empty() {
            move_region_to_node(self.ctrl.as_ptr(), self.ctrl.len(), prefer_node);
        }
    }
    #[cfg(not(target_os = "linux"))]
    fn maybe_rehome_ctrl(&mut self, _prefer_node: i32) {}
    
    #[cfg(not(target_os = "linux"))]
    fn ensure(&mut self, n: usize, ctrl_stride: usize, buf_ptr: *const u8, buf_len: usize) {
        if self.iovs.len() < n { self.iovs.resize_with(n, || iovec { iov_base: buf_ptr as *mut _, iov_len: buf_len }); }
        for i in 0..n { self.iovs[i].iov_base = buf_ptr as *mut _; self.iovs[i].iov_len = buf_len; }
        if self.msgs.len() < n { self.msgs.resize_with(n, || mmsghdr { msg_hdr: unsafe { std::mem::zeroed() }, msg_len: 0 }); }
        if self.ctrl_stride != ctrl_stride {
            self.ctrl.clear();
            self.ctrl_stride = ctrl_stride;
        }
        let need = n.saturating_mul(ctrl_stride);
        if self.ctrl.len() < need { self.ctrl.resize(need, 0); }
    }
    #[inline]
    fn ctrl_ptr(&mut self, i: usize) -> *mut cmsghdr {
        assert!(self.ctrl_stride > 0);
        unsafe { self.ctrl.as_mut_ptr().add(i * self.ctrl_stride) as *mut cmsghdr }
    }
}

#[cfg(target_os = "linux")]
#[inline]
fn mono_time_ns() -> u64 {
    unsafe {
        let mut ts: libc::timespec = std::mem::zeroed();
        libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts);
        (ts.tv_sec as u64) * 1_000_000_000u64 + (ts.tv_nsec as u64)
    }
}

// REPLACE
#[cfg(target_os = "linux")]
const SO_TXTIME_CONST: libc::c_int = 61;
#[cfg(target_os = "linux")]
const SOF_TXTIME_REPORT_ERRORS: u32 = 0x1;
#[cfg(target_os = "linux")]
const SOF_TXTIME_DEADLINE_MODE: u32 = 0x2;

#[cfg(target_os = "linux")]
#[repr(C)]
struct SockTxtime { clockid: libc::clockid_t, flags: u32 }

#[cfg(target_os = "linux")]
#[inline]
fn setup_so_txtime(sock: &Socket) {
    if !ENABLE_SO_TXTIME { return; }
    unsafe {
        let fd = sock.as_raw_fd();
        let mut flags = 0u32;
        if ENABLE_TXTIME_REPORT_ERRORS { flags |= SOF_TXTIME_REPORT_ERRORS; }
        if ENABLE_TXTIME_DEADLINE_MODE { flags |= SOF_TXTIME_DEADLINE_MODE; }

        let candidates = if ENABLE_TXTIME_TAI {
            [libc::CLOCK_TAI, libc::CLOCK_MONOTONIC_RAW, libc::CLOCK_MONOTONIC]
        } else {
            [libc::CLOCK_MONOTONIC, libc::CLOCK_MONOTONIC_RAW, libc::CLOCK_TAI]
        };

        for &clk in candidates.iter() {
            let cfg = SockTxtime { clockid: clk, flags };
            let r = libc::setsockopt(
                fd, libc::SOL_SOCKET, SO_TXTIME_CONST,
                &cfg as *const _ as *const libc::c_void,
                std::mem::size_of::<SockTxtime>() as libc::socklen_t
            );
            if r == 0 {
                update_txtime_epoch_offset(clk); // NEW: align txtime epoch to our tsc clock
                break;
            }
        }
        // If all failed, leave it off silently (older kernels)
    }
}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn bind_device_ifindex(sock: &Socket, ifname: &Arc<str>) -> std::io::Result<()> {
    unsafe {
        let idx = if_nametoindex(ifname.as_ptr() as *const i8);
        if idx == 0 { return Err(std::io::Error::last_os_error()); }
        // SO_BINDTOIFINDEX = 62
        let idx_i32 = idx as libc::c_int;
        let r = libc::setsockopt(
            sock.as_raw_fd(),
            libc::SOL_SOCKET,
            62,
            &idx_i32 as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        if r != 0 { return Err(std::io::Error::last_os_error()); }
        Ok(())
    }
}

#[cfg(not(target_os = "linux"))]
#[inline]
fn bind_device_ifindex(_sock: &Socket, _ifname: &Arc<str>) -> std::io::Result<()> { Ok(()) }

// ADD
#[inline]
fn setup_udp_reuse(sock: &Socket) {
    if ENABLE_REUSEADDR { let _ = sock.set_reuse_address(true); }
    #[cfg(target_os = "linux")]
    if ENABLE_REUSEPORT  { let _ = sock.set_reuse_port(true); }
}

// ADD
#[cfg(any(target_os = "linux", target_os = "android"))]
#[inline]
fn set_v6_src_preference(sock: &Socket) {
    if !ENABLE_V6_PREFER_SRC_PUBLIC { return; }
    unsafe {
        // IPV6_ADDR_PREFERENCES = 72; prefer PUBLIC (0x0002), avoid TMP (0x0001)
        let prefs: libc::c_int = 0x0002;
        let _ = libc::setsockopt(
            sock.as_raw_fd(),
            libc::IPPROTO_IPV6,
            72,
            &prefs as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}
#[cfg(not(any(target_os = "linux", target_os = "android")))]
#[inline]
fn set_v6_src_preference(_sock: &Socket) {}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn set_so_mark_for_device(sock: &Socket, ifname: &Arc<str>) {
    if !ENABLE_SO_MARK { return; }
    unsafe {
        // Bind to ifindex once (fast, stable against ifname flaps)
        if let Ok(idx) = {
            let p = ifname.as_ptr() as *const i8;
            let idx = libc::if_nametoindex(p);
            if idx == 0 { Err(std::io::Error::last_os_error()) } else { Ok(idx) }
        } {
            let mark: u32 = BASE_FWMARK ^ (idx as u32 & 0xFFFF);
            let _ = libc::setsockopt(
                sock.as_raw_fd(),
            libc::SOL_SOCKET,
                36, // SO_MARK
                &mark as *const _ as *const libc::c_void,
                std::mem::size_of::<u32>() as libc::socklen_t,
            );
        }
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn set_so_mark_for_device(_sock: &Socket, _ifname: &Arc<str>) {}

#[inline]
fn set_socket_priority(sock: &Socket) {
    if !ENABLE_SO_PRIORITY { return; }
    let _ = sock.set_priority(SOCKET_PRIORITY);
}

#[cfg(target_os = "linux")]
#[inline]
fn try_force_rcvbuf(sock: &Socket, bytes: i32) {
    unsafe {
        // SO_RCVBUFFORCE = 33
        let _ = libc::setsockopt(
            sock.as_raw_fd(),
            libc::SOL_SOCKET,
            33,
            &bytes as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn try_force_rcvbuf(_sock: &Socket, _bytes: i32) {}

// ADD
#[cfg(target_os = "linux")]
#[inline]
fn get_outq_bytes(fd: c_int) -> Option<i32> {
    unsafe {
        let mut v: c_int = 0;
        if libc::ioctl(fd, SIOCOUTQ, &mut v as *mut c_int) == 0 { Some(v) } else { None }
    }
}
#[cfg(not(target_os = "linux"))]
#[inline]
fn get_outq_bytes(_fd: c_int) -> Option<i32> { None }

fn bind_device_if(sock: &Socket, ifname: &str) -> std::io::Result<()> {
    use std::ffi::CString;
    let c = CString::new(ifname).map_err(|_| std::io::Error::from_raw_os_error(libc::EINVAL))?;
    // SAFETY: setsockopt with SO_BINDTODEVICE on Linux; ignored elsewhere.
    unsafe {
        let ret = libc::setsockopt(
            sock.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_BINDTODEVICE,
            c.as_ptr() as *const _,
            c.as_bytes_with_nul().len() as libc::socklen_t,
        );
        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
enum Proto { Udp, Quic }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PathKind { Direct, Forward }

#[derive(Clone, Debug)]
struct TpuAddrs {
    udp: Vec<SocketAddr>,
    udp_fwd: Vec<SocketAddr>,
    quic: Vec<SocketAddr>,
    quic_fwd: Vec<SocketAddr>,
}

#[derive(Clone)]
pub struct TpuPortMonopolizer {
    rpc_client: Arc<RpcClient>,
    async_rpc_client: Arc<NonblockingRpcClient>,
    connection_cache: Arc<ConnectionCache>,
    connection_pool: Arc<DashMap<SocketAddr, ConnectionState>>,
    active_connections: Arc<TokioRwLock<HashMap<Pubkey, Vec<SocketAddr>>>>,
    leader_schedule: Arc<TokioRwLock<LeaderScheduleCache>>,
    port_allocator: Arc<PortAllocator>,
    health_monitor: Arc<HealthMonitor>,
    metrics: Arc<Metrics>,
    identity: Arc<Keypair>,
    recent_slots: Arc<TokioRwLock<BTreeMap<Slot, Instant>>>,
    shutdown: Arc<AtomicBool>,
}

// REPLACE
#[repr(align(128))]
#[derive(Clone)]
struct ConnectionState {
    endpoint: Arc<Mutex<Option<Endpoint>>>,
    quic_conn: Arc<Mutex<Option<Arc<QuicConnection>>>>,
    udp_socket: Arc<Mutex<Option<Arc<UdpSocket>>>>,
    alt_udp_sockets: Arc<Mutex<Vec<Arc<UdpSocket>>>>,
    last_used_ns: AtomicU64,         // lock-free freshness
    success_count: AtomicU64,
    failure_count: AtomicU64,
    latency_ns: AtomicU64,
    is_healthy: AtomicBool,
    addr: SocketAddr,
    proto: Proto,
    quarantine_until_ms: AtomicU64,
    path_kind: PathKind,
    bound_device: Option<Arc<str>>,
    // NEW: kernel-fed MTU caches (0 = unknown)
    path_mtu_v4: AtomicU32,
    path_mtu_v6: AtomicU32,
    pacing_bps_last: AtomicU64,
    // NEW: outstanding MSG_ZEROCOPY packet count (for backpressure)
    zc_inflight_pkts: AtomicU64,
    // NEW: adversity-triggered port rotation
    adversity_score: AtomicU32,
    adversity_last_ns: AtomicU64,
    // NEW: neighbor keep-warm
    neigh_last_ns: AtomicU64,
    // NEW: cached UDP FD for lock-free access
    udp_fd: AtomicI32,  // -1 if unknown
    // NEW: operstate guard
    last_operstate_check_ns: AtomicU64,
    operstate_up_cached: AtomicBool,
    // NEW: GSO cap per device
    gso_max_segs: AtomicU32,
    // NEW: carrier guard
    last_carrier_check_ns: AtomicU64,
    carrier_up_cached: AtomicBool,
    // NEW: per-device TXTIME slew (ns, IIR-smoothed)
    dev_txtime_slew_ns: AtomicI64,
    // NEW: DSCP booster window
    dscp_boost_until_ns: AtomicU64,
    // NEW: DSCP parity cache
    tos_cache: TosCache,
}

impl ConnectionState {
    #[inline]
    fn udp_fd_cached(&self) -> c_int {
        #[cfg(target_os = "linux")]
        { self.udp_fd.load(Ordering::Relaxed) }
        #[cfg(not(target_os = "linux"))]
        { -1 }
    }
}

#[derive(Clone)]
struct LeaderScheduleCache {
    schedule: BTreeMap<Slot, Pubkey>,
    // validator -> all TPU endpoints by proto
    tpu_addresses: HashMap<Pubkey, TpuAddrs>,
    current_epoch: u64,
    slots_per_epoch: u64,
    last_update: Instant,
    last_slot: Slot,
}

struct PortAllocator {
    available_ports: Arc<Mutex<VecDeque<u16>>>,
    port_usage: Arc<DashMap<u16, (Instant, SocketAddr)>>,
}

struct HealthMonitor {
    connection_health: Arc<DashMap<SocketAddr, ConnectionHealth>>,
}

#[derive(Clone)]
struct ConnectionHealth {
    last_check: Instant,
    consecutive_failures: u32,
    avg_latency_ns: u64,
    success_rate: f64,
}

// ADD
#[cfg(target_os = "linux")]
struct RtCritGuard {
    old_policy: i32,
    old_param: sched_param,
    did_rt: bool,
    did_mlock: bool,
}
#[cfg(target_os = "linux")]
impl RtCritGuard {
    fn enter() -> Option<Self> {
        if !ENABLE_RT_CRITICAL { return None; }
        unsafe {
            // Snapshot current scheduler
            let tid = 0; // self
            let cur_pol = sched_getscheduler(tid);
            let mut cur_prm: sched_param = std::mem::zeroed();
            if sched_getparam(tid, &mut cur_prm) != 0 { return None; }

            // Try mlockall first (best effort)
            let mut did_mlock = false;
            if ENABLE_MLOCKALL {
                let _ = mlockall(MCL_CURRENT | MCL_FUTURE);
                did_mlock = true;
            }

            // Attempt RT switch
            let prm = sched_param { sched_priority: RT_PRIORITY_LVL };
            let did_rt = sched_setscheduler(tid, RT_SCHED_POLICY, &prm) == 0;

            Some(Self { old_policy: cur_pol, old_param: cur_prm, did_rt, did_mlock })
        }
    }
}
#[cfg(target_os = "linux")]
impl Drop for RtCritGuard {
    fn drop(&mut self) {
        unsafe {
            if self.did_rt {
                let _ = sched_setscheduler(0, self.old_policy, &self.old_param);
            }
            if self.did_mlock {
                let _ = munlockall();
            }
        }
    }
}

#[cfg(not(target_os = "linux"))]
struct RtCritGuard;
#[cfg(not(target_os = "linux"))]
impl RtCritGuard { fn enter() -> Option<Self> { None } }

// REPLACE (annotate to avoid false sharing on hot atomics)
#[repr(align(128))]
#[derive(Default)]
struct DeviceStats {
    egress_bytes: AtomicU64,
    sends: AtomicU64,
    successes: AtomicU64,
    avg_latency_ns: AtomicU64,
    avg_tx_ack_age_ns: AtomicU64,
    avg_tx_queue_ns: AtomicU64,
    zerocopy_completions: AtomicU64,
    // Pad explicitly to separate hot counters from neighbors on SMP
    _pad: [u64; 9],
}

// ADD
impl DeviceStats {
    fn record(&self, bytes: usize, ok: bool, lat_ns: Option<u64>) {
        self.egress_bytes.fetch_add(bytes as u64, Ordering::Relaxed);
        self.sends.fetch_add(1, Ordering::Relaxed);
        if ok { self.successes.fetch_add(1, Ordering::Relaxed); }
        if let Some(lat) = lat_ns {
            let cur = self.avg_latency_ns.load(Ordering::Relaxed);
            let newv = if cur == 0 { lat } else { (cur * 7 + lat * 3) / 10 };
            self.avg_latency_ns.store(newv, Ordering::Relaxed);
        }
    }
    fn record_tx_ack_age(&self, ns: u64) {
        let cur = self.avg_tx_ack_age_ns.load(Ordering::Relaxed);
        let newv = if cur == 0 { ns } else { (cur * 7 + ns * 3) / 10 };
        self.avg_tx_ack_age_ns.store(newv, Ordering::Relaxed);
    }
    fn record_tx_queue_ns(&self, ns: u64) {
        let cur = self.avg_tx_queue_ns.load(Ordering::Relaxed);
        let newv = if cur == 0 { ns } else { (cur * 7 + ns * 3) / 10 };
        self.avg_tx_queue_ns.store(newv, Ordering::Relaxed);
    }
    fn record_zerocopy_complete(&self, n: u64) {
        self.zerocopy_completions.fetch_add(n, Ordering::Relaxed);
    }
}

// ADD to Metrics struct
struct Metrics {
    total_sent: AtomicU64,
    total_confirmed: AtomicU64,
    total_failed: AtomicU64,
    avg_latency_ns: AtomicU64,
    device_stats: DashMap<Arc<str>, Arc<DeviceStats>>,
    // NEW: per-NIC limiters
    device_limiters: DashMap<Arc<str>, Arc<Semaphore>>,
}

impl TpuPortMonopolizer {
    pub async fn new(
        rpc_url: &str,
        ws_url: &str,
        identity: Arc<Keypair>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        // ADD: Raise NOFILE limit for big alt-socket farms
        raise_nofile_limit(1_048_576);
        
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));
        
        let async_rpc_client = Arc::new(NonblockingRpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));

        let connection_cache = Arc::new(ConnectionCache::new_with_client_options(
            CONNECTION_CACHE_SIZE,
            Some(CONNECTION_POOL_SIZE),
            Some((&identity, IpAddr::V4(Ipv4Addr::UNSPECIFIED))),
            Some(MAX_QUIC_CONNECTIONS_PER_PEER),
        ));

        let epoch_info = rpc_client.get_epoch_info()?;
        let leader_schedule_cache = LeaderScheduleCache {
            schedule: BTreeMap::new(),
            tpu_addresses: HashMap::new(),
            current_epoch: epoch_info.epoch,
            slots_per_epoch: epoch_info.slots_in_epoch,
            last_update: Instant::now(),
            last_slot: epoch_info.absolute_slot,
        };

        let port_allocator = Arc::new(PortAllocator::new());
        let health_monitor = Arc::new(HealthMonitor::new());
        
        let monopolizer = Self {
            rpc_client,
            async_rpc_client,
            connection_cache,
            connection_pool: Arc::new(DashMap::new()),
            active_connections: Arc::new(RwLock::new(HashMap::new())),
            leader_schedule: Arc::new(TokioRwLock::new(leader_schedule_cache)),
            port_allocator,
            health_monitor,
            metrics: Arc::new(Metrics::new()),
            identity,
            recent_slots: Arc::new(TokioRwLock::new(BTreeMap::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
        };

        monopolizer.start_background_tasks().await;
        monopolizer.refresh_leader_schedule().await?;
        
        Ok(monopolizer)
    }

    // REPLACE
    pub async fn send_transaction(
        &self,
        transaction: &Transaction,
    ) -> Result<Signature, Box<dyn std::error::Error>> {
        let start = Instant::now();
        let signature = transaction.signatures[0];

        // Single allocation → Bytes (zero-copy for QUIC datagrams, &[..] for UDP)
        let wire_vec = serialize(transaction)?;
        if wire_vec.len() > MAX_PACKET_SIZE { return Err("Transaction too large".into()); }
        let wire_bytes = Bytes::from(wire_vec);
        let wire_slice: &[u8] = wire_bytes.as_ref();

        // ADD: Timer slack harden for elite HFT wake determinism
        set_thread_timer_slack(TIMER_SLACK_NS);

        // REPLACE (just after you build wire_bytes/wire_slice and before scheduling waves)
        #[cfg(target_os = "linux")]
        let _rt_guard = RtCritGuard::enter();

        // (Optional) RT + CPU pin
        if ENABLE_RT_AFFINITY {
            let cores = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1);
            let mut s8 = [0u8; 8]; s8.copy_from_slice(&signature.as_ref()[..8]);
            let sig_seed = u64::from_le_bytes(s8);
            let cpu = (fast_hash_u64(sig_seed) as usize) % cores;
            set_thread_rt_and_affinity(cpu, RT_PRIORITY);
        }

        // Page-fault immunity + cache hot
        let _mlock = MlockGuard::new(wire_slice);
        prefetch_wire(wire_slice);
        // ADD
        madvise_willneed(wire_slice);
        if ENABLE_TIMER_SLACK_TIGHTEN { tighten_timer_slack(TIMER_SLACK_NS); }
        // REPLACE or ADD near prefetch/MLock prelude
        tighten_thread_timer_slack(1_000); // 1µs

        // leader & conns
        let slot = self.get_current_slot().await?;
        let leader = self.get_slot_leader(slot + LEADER_SCHEDULE_SLOT_OFFSET).await?;
        let mut conns = self.get_leader_connections(&leader).await?;
        if conns.is_empty() { return Err("No TPU connections available".into()); }

        // stable sort by port (after prioritization) for deterministic path alternation
        conns.sort_by_key(|a| a.port());

        // Path-mix guarantee: ensure first two cover {Direct, Forward} if available
        if REQUIRE_FORWARD_MIX && conns.len() >= 2 {
            let k0 = self.connection_pool.get(&conns[0]).map(|c| c.path_kind);
            let any_fwd_pos = conns.iter().position(|a| self.connection_pool.get(a).map(|c| c.path_kind == PathKind::Forward).unwrap_or(false));
            if let (Some(PathKind::Direct), Some(pos_fwd)) = (k0, any_fwd_pos) {
                conns.swap(1, pos_fwd); // put a forward path early
            }
        }

        // send policy
        let attempt_timeout = Duration::from_millis(CONNECTION_TIMEOUT_MS.min(200));
        // REPLACE (inside send_transaction)
        // REPLACE (the entire closure body)
        let copies_for = |addr: &SocketAddr| -> usize {
            // Base policy (your success/latency/device queue logic)
            let base = if !ADAPTIVE_BURST { 2 } else {
                if let Some(st) = self.connection_pool.get(addr) {
                    let succ = st.success_count.load(Ordering::Relaxed) as f64;
                    let fail = st.failure_count.load(Ordering::Relaxed) as f64;
                    let total = succ + fail;
                    let sr = if total > 5.0 { succ / total } else { 0.8 };
                    let lat = st.latency_ns.load(Ordering::Relaxed);
                    if sr >= 0.93 && lat <= 300_000 { MIN_BURST_COPIES }
                    else if sr >= 0.80 && lat <= 800_000 { 2 }
                    else { MAX_BURST_COPIES }
                } else { 2 }
            };

            // Clamp by device queue dwell (already wired)
            let after_devq = if ADAPTIVE_COPY_CLAMP {
                if let Some(st) = self.connection_pool.get(addr) {
                    if let Some(dev) = st.bound_device.as_ref() {
                        if let Some(stats) = self.metrics.device_stats.get(dev) {
                            let q = stats.avg_tx_queue_ns.load(Ordering::Relaxed);
                            if q >= QUEUE_NS_HIGH { 1.min(base).max(1) }
                            else if q >= QUEUE_NS_MED { 2.min(base).max(1) }
                            else { base }
                        } else { base }
                    } else { base }
                } else { base }
            } else { base };

            // NEW: Clamp by local socket outq to avoid self-induced HOL on this socket.
            if ENABLE_SIOCOUTQ_CLAMP {
                if let Some(st) = self.connection_pool.get(addr) {
                    if let Ok(udp_guard) = st.udp_socket.try_lock() {
                        if let Some(udp) = udp_guard.as_ref() {
                            if let Some(outq) = get_outq_bytes(udp.as_raw_fd()) {
                                if outq >= OUTQ_HIGH_BYTES { return 1; }
                                if outq >= OUTQ_MED_BYTES  { return after_devq.min(2).max(1); }
                            }
                        }
                    }
                }
            }

            after_devq
        };

        // elite fast-path
        let mut elite_first: Option<SocketAddr> = None;
        if let Some(best) = conns.first().copied() {
            if let Some(st) = self.connection_pool.get(&best) {
                let succ = st.success_count.load(Ordering::Relaxed) as f64;
                let fail = st.failure_count.load(Ordering::Relaxed) as f64;
                let total = succ + fail;
                let sr = if total > 10.0 { succ / total } else { 0.85 };
                let lat = st.latency_ns.load(Ordering::Relaxed);
                if sr >= FAST_PATH_SR && lat <= FAST_PATH_LAT_NS {
                    elite_first = Some(best);
                }
            }
        }

        // ADD (right after you determine `elite_first` or at least the first conn)
        #[cfg(target_os = "linux")]
        if ENABLE_NUMA_MBIND_WIRE {
            let target = elite_first.or_else(|| conns.get(0).copied());
            if let Some(addr) = target {
                if let Some(st) = self.connection_pool.get(&addr) {
                    if let Some(dev) = st.bound_device.as_ref() {
                        prefer_mbind_wire_to_device(wire_bytes.as_ref(), dev);
                    }
                }
            }
        }

        // ADD: XPS CPU pinning for elite HFT performance
        if let Some(addr) = elite_first.or_else(|| conns.get(0).copied()) {
            if let Some(st) = self.connection_pool.get(&addr) {
                if let Some(dev) = st.bound_device.as_ref() {
                    pin_to_nic_xps_once(dev);
                }
            }
        }

        // ADD (Linux)
        #[cfg(target_os = "linux")]
        if ENABLE_MOVE_PAGES_WIRE {
            if let Some(addr) = elite_first.or_else(|| conns.get(0).copied()) {
                if let Some(st) = self.connection_pool.get(&addr) {
                    if let Some(dev) = st.bound_device.as_ref() {
                        if let Some(node) = nic_numa_node(dev) {
                            move_pages_wire(wire_bytes.as_ref(), node);
                        }
                    }
                }
            }
        }

        // Precompute signature hash for jitter seeding
        let mut s8 = [0u8; 8]; s8.copy_from_slice(&signature.as_ref()[..8]);
        let sig_hash64 = u64::from_le_bytes(s8);

        let (tx, mut rx) = mpsc::channel(128);
        let semaphore = Arc::new(Semaphore::new(MAX_PARALLEL_SENDS));
        let won = Arc::new(AtomicBool::new(false));

        // ADD (Linux only; coexists with your RT/FIFO guard)
        #[cfg(target_os = "linux")]
        let _dl_guard = DlGuard::enter(DL_RUNTIME_US, DL_DEADLINE_US, DL_PERIOD_US);

        // Elite shot
        if let Some(best) = elite_first {
            let buf = wire_bytes.clone(); let pool = self.connection_pool.clone();
            let met = self.metrics.clone(); let semp = semaphore.clone();
            let txc = tx.clone(); let won_c = won.clone();
            tokio::spawn(async move {
                if won_c.load(Ordering::Relaxed) { let _ = txc.send((best, Err("Cancelled".into()))).await; return Some(()); }
                let _p = semp.acquire().await.ok()?;
                let res = timeout(attempt_timeout, send_to_address_burst(&pool, &best, buf.as_ref(), Some(buf.clone()), 1, &met, &won_c)).await;
                let out = match res { Ok(Ok(())) => Ok(()), Ok(Err(e)) => Err(e), Err(_) => Err("Timeout".into()) };
                let _ = txc.send((best, out)).await; Some(())
            });
        }

        // REPLACE (insert right after scheduling the elite shot, before the Wave 1 for-loop)
        if ENABLE_BOOSTER_WAVE {
            // Pick the best alternate path different from elite_first: prefer a Forward path early.
            let booster_target = conns.iter().copied()
                .filter(|a| Some(*a) != elite_first)
                .min_by_key(|a| {
                    // Prefer Forward first; then by current priority order (port as tiebreaker).
                    let kind_rank = self.connection_pool.get(a)
                        .map(|c| if c.path_kind == PathKind::Forward { 0usize } else { 1usize })
                        .unwrap_or(1usize);
                    (kind_rank, a.port())
                });

            if let Some(baddr) = booster_target {
                let bbuf = wire_bytes.clone();
                let bpool = self.connection_pool.clone();
                let bmet = self.metrics.clone();
                let bsemp = semaphore.clone();
                let btx = tx.clone();
                let bwon = won.clone();

                // Deterministic 180..260µs delay with device phase de-synchronization.
                let mut s8 = [0u8; 8]; s8.copy_from_slice(&signature.as_ref()[..8]);
                let sig_hash64 = u64::from_le_bytes(s8);
                let base = BOOSTER_BASE_US;
                let span = BOOSTER_SPAN_US;
                let phase = {
                    self.connection_pool
                        .get(&baddr)
                        .and_then(|st| st.bound_device.as_ref().map(|d| {
                            fast_hash_u64(d.as_bytes().iter().fold(0u64, |h,&b| h.wrapping_mul(131).wrapping_add(b as u64))) % 20
                        }))
                        .unwrap_or(0) as u64
                };
                let jitter = det_jitter_us(sig_hash64 ^ addr_hash64(&baddr), fast_hash_u64(slot), 0, span);
                let booster_delay = base + jitter + phase; // ≈ 180..280µs

                tokio::spawn(async move {
                    micro_sleep_cancellable(booster_delay, &bwon).await;
                    if bwon.load(Ordering::Relaxed) { let _ = btx.send((baddr, Err("Cancelled".into()))).await; return Some(()); }
                    let _p = bsemp.acquire().await.ok()?;
                    let res = timeout(
                        attempt_timeout,
                        send_to_address_burst(&bpool, &baddr, bbuf.as_ref(), Some(bbuf.clone()), 1, &bmet, &bwon)
                    ).await;
                    let out = match res { Ok(Ok(())) => Ok(()), Ok(Err(e)) => Err(e), Err(_) => Err("Timeout".into()) };
                    let _ = btx.send((baddr, out)).await; Some(())
                });
            }
        }

        // Wave 1: 0..400µs deterministic jitter
        let dev_phase_us = |addr: &SocketAddr| -> u64 {
            self.connection_pool
                .get(addr)
                .and_then(|st| st.bound_device.as_ref().map(|d| fast_hash_u64(d.as_bytes().iter().fold(0u64, |h,&b| h.wrapping_mul(131).wrapping_add(b as u64)) )))
                .map(|h|  (h % 40) as u64) // 0..39µs device phase
                .unwrap_or(0)
        };

        for addr in conns.iter().copied() {
            let copies = copies_for(&addr);
            let buf = wire_bytes.clone(); let pool = self.connection_pool.clone();
            let met = self.metrics.clone(); let semp = semaphore.clone();
            let txc = tx.clone(); let won_c = won.clone();

            // REPLACE
            let now_ns = txtime_now_ns(); // coherent with MONOTONIC_RAW/TAI calibration; cheaper in hot path
            let leader_slot_start_ns = self.get_leader_slot_start_ns(slot);
            let (delay_us, _span_us) = if elite_first == Some(addr) { (0, 0) }
                           else { slot_phase_jitter(sig_hash64, &addr, now_ns, leader_slot_start_ns) };
            tokio::spawn(async move {
                if delay_us > 0 { micro_sleep_cancellable(delay_us, &won_c).await; }
                if won_c.load(Ordering::Relaxed) { let _ = txc.send((addr, Err("Cancelled".into()))).await; return Some(()); }
                let _p = semp.acquire().await.ok()?;
                let res = timeout(attempt_timeout, send_to_address_burst(&pool, &addr, buf.as_ref(), Some(buf.clone()), copies, &met, &won_c)).await;
                let out = match res { Ok(Ok(())) => Ok(()), Ok(Err(e)) => Err(e), Err(_) => Err("Timeout".into()) };
                let _ = txc.send((addr, out)).await; Some(())
            });
        }

        // Wave 2: 400..800µs only if enabled; tasks cancel if 'won' flips
        if ENABLE_SECOND_WAVE {
            for addr in conns.iter().copied() {
                let copies = 1; // second wave is a single copy per endpoint
                let buf = wire_bytes.clone(); let pool = self.connection_pool.clone();
                let met = self.metrics.clone(); let semp = semaphore.clone();
                let txc = tx.clone(); let won_c = won.clone();

                // REPLACE
            let now_ns = txtime_now_ns(); // coherent with MONOTONIC_RAW/TAI calibration; cheaper in hot path
                let leader_slot_start_ns = self.get_leader_slot_start_ns(slot);
                let (delay_us, _span_us) = slot_phase_jitter(sig_hash64 ^ 0x9E3779B97F4A7C15, &addr, now_ns, leader_slot_start_ns);
                tokio::spawn(async move {
                        micro_sleep_cancellable(delay_us, &won_c).await; 
                    if won_c.load(Ordering::Relaxed) { let _ = txc.send((addr, Err("Cancelled".into()))).await; return Some(()); }
                    let _p = semp.acquire().await.ok()?;
                    let res = timeout(attempt_timeout, send_to_address_burst(&pool, &addr, buf.as_ref(), Some(buf.clone()), copies, &met, &won_c)).await;
                    let out = match res { Ok(Ok(())) => Ok(()), Ok(Err(e)) => Err(e), Err(_) => Err("Timeout".into()) };
                    let _ = txc.send((addr, out)).await; Some(())
                });
            }
        }
        drop(tx);

        while let Some((_addr, result)) = rx.recv().await {
            if result.is_ok() {
                won.store(true, Ordering::Relaxed);
                self.metrics.total_confirmed.fetch_add(1, Ordering::Relaxed);
                let latency = start.elapsed().as_nanos() as u64;
                self.update_latency_metrics(latency);
                self.record_recent_slot(slot).await;
                return Ok(signature);
            }
        }

        self.metrics.total_failed.fetch_add(1, Ordering::Relaxed);
        Err("Failed to send transaction after all attempts".into())
    }
{{ ... }}

    async fn send_to_address(
        connection_pool: &DashMap<SocketAddr, ConnectionState>,
        addr: &SocketAddr,
        wire_transaction: &[u8],
        metrics: &Arc<Metrics>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        metrics.total_sent.fetch_add(1, Ordering::Relaxed);
        let st = connection_pool.get(addr).ok_or("Connection not found")?;

        // Quarantine check
        let until = st.quarantine_until_ms.load(Ordering::Relaxed);
        if until != 0 && now_unix_ms() < until {
            return Err("Quarantined".into());
        }
        if !st.is_healthy.load(Ordering::Relaxed) {
            return Err("Unhealthy connection".into());
        }

        let start = Instant::now();

        // Pick the live transport
        let has_quic = matches!(st.quic_conn.try_lock(), Ok(g) if g.as_ref().is_some());
        let has_udp  = matches!(st.udp_socket.try_lock(), Ok(g) if g.as_ref().is_some());

        let out = if has_quic {
            Self::send_quic(&st, wire_transaction).await
        } else if has_udp {
            Self::send_udp(&st, wire_transaction).await
        } else {
            Err("No transport".into())
        };

        match &out {
            Ok(()) => {
                let latency = start.elapsed().as_nanos() as u64;
                st.latency_ns.store(latency, Ordering::Relaxed);
                st.success_count.fetch_add(1, Ordering::Relaxed);
                st.last_used_ns.store(tsc_now_ns(), Ordering::Relaxed);
            }
            Err(_) => {
                st.failure_count.fetch_add(1, Ordering::Relaxed);
                if let Some(oserr) = out.as_ref().err().and_then(|e| e.downcast_ref::<std::io::Error>()) {
                    classify_and_maybe_quarantine(oserr, &st, metrics); // NEW metrics feedback
                }
                metrics.record_dev(st.bound_device.as_ref(), wire.len() * copies.max(1), false, Some(lat));
                
                // If we've had too many failures, quarantine this connection
                let fails = st.failure_count.load(Ordering::Relaxed);
                let succs = st.success_count.load(Ordering::Relaxed);
                let total = fails + succs;
                
                if total >= 10 && (fails as f64 / total as f64) > 0.5 {
                    st.quarantine_until_ms.store(
                        now_unix_ms() + QUARANTINE_MS,
                        Ordering::Relaxed
                    );
                    st.is_healthy.store(false, Ordering::Relaxed);
                }
            }
        }
        out
    }

    async fn send_quic(
        conn_state: &ConnectionState,
        wire_transaction: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let quic_conn = conn_state.quic_conn.lock().await;
        if let Some(conn) = quic_conn.as_ref() {
            if conn.close_reason().is_some() {
                return Err("QUIC connection closed".into());
            }

            if ENABLE_QUIC_DATAGRAMS {
                // Try datagram first; fallback on error (peer can negotiate no-dgram)
                match conn.send_datagram(Bytes::copy_from_slice(wire_transaction)) {
                    Ok(_) => return Ok(()),
                    Err(_) => {
                        // fall through to stream
                    }
                }
            }
            
            let mut stream = conn.open_uni().await?;
            stream.write_all(wire_transaction).await?;
            stream.finish().await?;
            Ok(())
        } else {
            Err("No QUIC connection".into())
        }
    }

    async fn send_udp(
        conn_state: &ConnectionState,
        wire_transaction: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let udp_socket = conn_state.udp_socket.lock().await;
        if let Some(socket) = udp_socket.as_ref() {
            match socket.send(wire_transaction) {
                Ok(sent) if sent == wire_transaction.len() => Ok(()),
                Ok(_) => Err("Partial send".into()),
                Err(e) => Err(e.into()),
            }
        } else {
            Err("No UDP socket".into())
        }
    }

    // REPLACE
    async fn send_to_address_burst(
        connection_pool: &DashMap<SocketAddr, ConnectionState>,
        addr: &SocketAddr,
        wire: &[u8],
        wire_bytes: Option<Bytes>,       // NEW: zero-copy for QUIC datagrams
        copies: usize,
        metrics: &Arc<Metrics>,
        won: &Arc<AtomicBool>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // ADD: Hard real-time scheduling and memory locking
        raise_rt_mem_limits();
        elevate_rt_scheduling();
        lock_process_memory();
        
        // ADD: PM QoS hold to keep cores out of deep C-states
        hold_cpu_dma_latency(0);
        
        // ADD: Block signals in burst threads
        block_all_signals();
        
        // ADD: Disable perf events for burst threads
        disable_perf_events_this_thread();
        
        // ADD: Lock code+rodata pages
        mlock_text_rodata(64 * 1024 * 1024); // cap 64MiB
        
        // ADD: NUMA policy to prefer NIC's node
        if let Some(dev) = st.bound_device.as_ref() {
            #[cfg(target_os = "linux")]
            { prefer_numa_node(numa_node_of_dev(dev)); }
        }
        
        if won.load(Ordering::Relaxed) { return Err("Cancelled".into()); }
        metrics.total_sent.fetch_add(1, Ordering::Relaxed);
        let st = connection_pool.get(addr).ok_or("Connection not found")?;

        // Per-NIC limiter
        let _dev_guard = if ENABLE_DEVICE_LIMITER {
            if let Some(dev) = st.bound_device.as_ref() {
                Some(metrics.dev_semaphore(dev).acquire().await?)
            } else { None }
        } else { None };

        // Quarantine/health gates (unchanged)
        let until = st.quarantine_until_ms.load(Ordering::Relaxed);
        if until != 0 && now_unix_ms() < until { return Err("Quarantined".into()); }
        if !st.is_healthy.load(Ordering::Relaxed) { return Err("Unhealthy connection".into()); }
        
        // ADD: Carrier guard - skip admin-up but link-down NICs
        if !nic_carrier_up_cached(&st) { return Err("carrier down".into()); }

        let start = Instant::now();
        let out = if matches!(st.quic_conn.try_lock(), Ok(g) if g.as_ref().is_some()) {
            for i in 0..copies.max(1) {
                if won.load(Ordering::Relaxed) { return Err("Cancelled".into()); }
                if i > 0 { micro_sleep_cancellable(100, won).await; }
                let qc = st.quic_conn.lock().await;
                if let Some(conn) = qc.as_ref() {
                    if conn.close_reason().is_some() { return Err("QUIC closed".into()); }
                    if ENABLE_QUIC_DATAGRAMS {
                        // Zero-copy path: reuse Bytes buffer; fallback to stream if send_datagram errors
                        if let Some(b) = wire_bytes.as_ref() {
                            if conn.send_datagram(b.clone()).is_err() {
                                let mut stream = conn.open_uni().await?;
                                stream.write_all(wire).await?;
                                stream.finish().await?;
                            }
                        } else {
                            if conn.send_datagram(Bytes::copy_from_slice(wire)).is_err() {
                                let mut stream = conn.open_uni().await?;
                                stream.write_all(wire).await?;
                                stream.finish().await?;
                            }
                        }
                    } else {
                        let mut stream = conn.open_uni().await?;
                        stream.write_all(wire).await?;
                        stream.finish().await?;
                    }
                } else {
                    return Err("QUIC missing".into());
                }
            }
            Ok(())
        } else if matches!(st.udp_socket.try_lock(), Ok(g) if g.as_ref().is_some()) {
            // REPLACE (inside UDP branch of send_to_address_burst)
            let primary_opt = st.udp_socket.lock().await.clone();
            let alt_vec = st.alt_udp_sockets.lock().await.clone(); // Vec<Arc<UdpSocket>>

            let mut socks: Vec<Arc<UdpSocket>> = Vec::new();
            if let Some(p) = primary_opt { socks.push(p); }
            if ENABLE_MULTI_SRC_PORTS { socks.extend(alt_vec.into_iter()); }
            // Safety: always have at least one
            if socks.is_empty() { return Err("UDP missing".into()); }

            // ADD (near top of UDP branch, after you have `sock`, `addr`, and `st`)
            #[cfg(target_os = "linux")]
            let mtu_hint = {
                if addr.is_ipv6() {
                    let m = st.path_mtu_v6.load(Ordering::Relaxed);
                    if m >= V6_MIN_MTU { Some(m as u32) } else { None }
                } else {
                    let m = st.path_mtu_v4.load(Ordering::Relaxed);
                    if m >= V4_MIN_MTU { Some(m as u32) } else { None }
                }
            };

            // ADD: Adaptive busy poll budget for elite HFT performance
            #[cfg(target_os = "linux")]
            {
                let fd = sock.as_raw_fd();
                maybe_adjust_busy_poll_budget(fd, &st, metrics);
                maybe_adjust_busy_poll_time(fd, &st, metrics);
            }

            // ADD: ZC backpressure decision for elite HFT performance
            #[cfg(target_os = "linux")]
            let use_zc = if ENABLE_UDP_ZEROCOPY {
                if ENABLE_ZC_BACKPRESSURE {
                    let inflight = st.zc_inflight_pkts.load(Ordering::Relaxed);
                    if inflight >= ZC_INFLIGHT_HARD { false }
                    else if inflight >= ZC_INFLIGHT_SOFT { false }
                    else { 
                        memcpy_calibrate_threshold_once();
                        let zc_min = ZC_MIN_BYTES_CAL.load(Ordering::Relaxed);
                        wire.len() >= zc_min || !ENABLE_ZC_MIN_SIZE 
                    }
                } else {
                    memcpy_calibrate_threshold_once();
                    let zc_min = ZC_MIN_BYTES_CAL.load(Ordering::Relaxed);
                    wire.len() >= zc_min || !ENABLE_ZC_MIN_SIZE
                }
            } else { false };

            // Distribute 'copies' across sockets
            let total = copies.max(1);
            
            // ADD: Calculate first txtime for tight window trim
            let first_txtime = txtime_now_ns();
            
            // ADD: Tight window duplicate thinning
            let mut copies_eff = total;
            if ENABLE_TIGHT_WINDOW_TRIM {
                let now = txtime_now_ns();
                if first_txtime.saturating_sub(now) <= DUP_TIGHT_NS {
                    copies_eff = 1;
                }
            }

            // ADD: Duplicate arrival feasibility filter
            if ENABLE_TIGHT_WINDOW_TRIM {
                let now = txtime_now_ns();
                let until = first_txtime.saturating_sub(now);
                // Assume per-dup wire size ~ payload + L2; cap by pacing_bps_last
                let bps = st.pacing_bps_last.load(Ordering::Relaxed).max(PACING_BPS_MIN);
                let per = (wire.len() as u64 + 64).max(64);
                // Keep only those duplicates whose queue position can serialize before deadline
                let mut keep = 1usize;
                for k in 1..copies_eff {
                    let ahead = (k as u64).saturating_mul(per);
                    if will_arrive_before_deadline(until, ahead, bps) { keep = k + 1; } else { break; }
                }
                copies_eff = keep.max(1);
            }

            // ADD: NSD-aware pacing clamp
            #[cfg(target_os = "linux")]
            if let Some(sock) = socks.first() {
                if let Some(outq) = get_outq_nsd_bytes(sock.as_raw_fd()) {
                    // Back off pacing slightly when backlog forms; keep ≥ line rate
                    let base = st.pacing_bps_last.load(Ordering::Relaxed).max(PACING_BPS_MIN);
                    let trim = if outq >= OUTQ_HIGH_BYTES { base / 8 } else if outq >= OUTQ_MED_BYTES { base / 16 } else { 0 };
                    let new_rate = base.saturating_sub(trim).max(base / 2);
                    set_max_pacing_rate(sock.as_raw_fd(), new_rate);
                }
            }

            // ADD: Prefault payload slabs
            #[cfg(target_os = "linux")]
            prefault_write(wire.as_ptr() as *mut u8, wire.len());
            
            // REPLACE (insert near top of UDP branch, after you get `sock`)
            #[cfg(target_os = "linux")]
            if ENABLE_SIOCOUTQ_CLAMP {
                if let Some(sock) = socks.first() {
                    if let Some(outq) = get_outq_bytes(sock.as_raw_fd()) {
                        if outq >= OUTQ_HIGH_BYTES && copies > 1 {
                            // cut to 1 immediately for this call path
                            return Err("LocalOutqClamp".into());
                        }
                    }
                }
            }
            let ns = socks.len().min(copies_eff).max(1);

            // ADD: Single-copy fast path for elite HFT performance
            #[cfg(target_os = "linux")]
    if ENABLE_SENDTO_FASTPATH && copies == 1 && !ENABLE_SO_TXTIME {
        // No txtime, no GSO, no per-copy DSCP/SCM -> raw send() wins.
                if let Some(sock) = socks.first() {
                    let fd = sock.as_raw_fd();
                    let is_v6 = addr.is_ipv6();
            let need_gso = ENABLE_PMTU_AWARE_GSO && can_use_udp_gso() && {
                        let mt = mtu_hint.or_else(|| get_path_mtu(fd, is_v6));
                let seg = clamp_udp_seg(wire.len(), is_v6, mt);
                seg > 0 && wire.len() > seg as usize
            };
            if !need_gso && wire.len() < ZC_MIN_BYTES {
                        let n = linux_send_one_fast(fd, wire)?;
                        if n == wire.len() { return Ok(()); }
                    }
                }
            }

            // Helper: send 'cnt' copies on one socket with our priority order (TXTIME > ZC > mmsg > loop)
            #[inline]
            fn send_on_sock(
                sock: &UdpSocket,
                buf: &[u8],
                cnt: usize,
                won: &Arc<AtomicBool>,
            ) -> Result<(), Box<dyn std::error::Error>> {
            #[cfg(target_os = "linux")]
            {
                    if ENABLE_SO_TXTIME && ENABLE_SENDMMSG && cnt > 1 {
                    if won.load(Ordering::Relaxed) { return Err("Cancelled".into()); }
                    let fd = sock.as_raw_fd();
                    // REPLACE
            let now_ns = txtime_now_ns(); // coherent with MONOTONIC_RAW/TAI calibration; cheaper in hot path
                    let spacing = adaptive_txtime_spacing_ns(
                        st.bound_device.as_ref()
                          .and_then(|d| metrics.device_stats.get(d).map(|v| v.clone()))
                    );
                    // ADD: Multi-NIC micro-skew for de-collision
                    let now_ns = now_ns.saturating_sub(nic_rank_skew_ns(&st, metrics));
                    
                    // ADD: Slew-aware skew for consistently laggy NICs
                    let slew_ns = st.dev_txtime_slew_ns.load(Ordering::Relaxed).unsigned_abs().min(30_000); // ≤30µs
                    let now_ns = now_ns.saturating_sub((slew_ns / 8)); // 12.5% of corrected dwell
                        // ADD: MSG_CONFIRM for neighbor keep-warm
                        let mut confirm_flag = 0;
                        if ENABLE_MSG_CONFIRM_KEEP_NEIGH {
                            let now_ns = txtime_now_ns();
                            let last = st.neigh_last_ns.load(Ordering::Relaxed);
                            if now_ns.saturating_sub(last) >= NEIGH_CONFIRM_INTERVAL_NS {
                                confirm_flag = libc::MSG_CONFIRM;
                                st.neigh_last_ns.store(now_ns, Ordering::Relaxed);
                            }
                        }

                        // ADD: Stack fast-path for ≤4 copies
                        if cnt <= 4 {
                            let is_v6 = addr.is_ipv6();
                            let seg = if ENABLE_PMTU_AWARE_GSO && can_use_udp_gso() {
                                let mt = mtu_hint.or_else(|| get_path_mtu(fd, is_v6));
                                best_udp_seg_cap(buf.len(), is_v6, mt, st.gso_max_segs.load(Ordering::Relaxed))
                            } else { 0 };
                            let sent = linux_sendmmsg_txtime_stack4(
                                fd, buf, cnt, now_ns, spacing, is_v6, seg,
                                (if use_zc { libc::MSG_ZEROCOPY } else { 0 }) | confirm_flag,
                                DSCP_FIRST, DSCP_OTHERS
                            )?;
                            if use_zc && sent > 0 { st.zc_inflight_pkts.fetch_add(sent as u64, Ordering::Relaxed); }
                            if sent == 0 { return Err("sendmmsg txtime zero".into()); }
                            // ADD: Opportunistic errqueue micro-drain
                            occasional_errqueue_microdrain(fd);
                            return Ok(());
                        }

                        // REPLACE: Elite HFT ZC backpressure decision with io_uring support
                        #[cfg(target_os = "linux")]
                        let try_uring = ENABLE_UDP_ZEROCOPY && uring_available();

                        // ADD: DSCP parity cache logic
                        let want = ((DSCP_FIRST << 2) | 0b10) as i32;
                        let cached = if addr.is_ipv6() {
                            st.tos_cache.v6.load(Ordering::Relaxed)
                        } else {
                            st.tos_cache.v4.load(Ordering::Relaxed)
                        };
                        let attach_dscp = if cached == 0 { true } else { cached != want };
                        if cached == 0 {
                            if let Some(got) = get_socket_tos(fd, addr.is_ipv6()) {
                                if addr.is_ipv6() { st.tos_cache.v6.store(got, Ordering::Relaxed); }
                                else { st.tos_cache.v4.store(got, Ordering::Relaxed); }
                            }
                        }

                        let sent = if try_uring {
                            // seg from your GSO chooser; attach_dscp computed from socket TOS parity
                            uring_sendmmsg_txtime(
                                fd, buf, cnt, now_ns, spacing, addr.is_ipv6(),
                                seg, (libc::MSG_ZEROCOPY | confirm_flag), attach_dscp, DSCP_FIRST, DSCP_OTHERS
                            )?
                        } else if use_zc {
                            let r = linux_sendmmsg_txtime_flags(
                                fd, buf, cnt, now_ns, spacing, addr.is_ipv6(), libc::MSG_ZEROCOPY | confirm_flag
                            );
                            match r {
                                Ok(s) => {
                                    if s > 0 {
                                        st.zc_inflight_pkts.fetch_add(s as u64, Ordering::Relaxed);
                                    }
                                    Ok(s)
                                }
                                Err(e) => {
                                    // ADD: Adversity accounting for EAGAIN/ENOBUFS
                                    if let Some(code) = e.raw_os_error() {
                                        if code == libc::EAGAIN || code == libc::ENOBUFS {
                                            let now = tsc_now_ns();
                                            let last = st.adversity_last_ns.swap(now, Ordering::Relaxed);
                                            let age_ms = (now.saturating_sub(last) / 1_000_000) as u64;
                                            if age_ms > ADVERSITY_DECAY_MS {
                                                st.adversity_score.store(1, Ordering::Relaxed);
                                            } else {
                                                let v = st.adversity_score.fetch_add(1, Ordering::Relaxed).saturating_add(1);
                                                if v >= ADVERSITY_THRESH { 
                                                    st.adversity_score.store(0, Ordering::Relaxed); 
                                                    let _ = maybe_rotate_src_port(st); 
                                                }
                                            }
                                        }
                                    }
                                    Err(e)
                                }
                            }
                        } else {
                            linux_sendmmsg_txtime(fd, buf, cnt, now_ns, spacing, addr.is_ipv6(), mtu_hint)
                        };

                        match sent {
                            Ok(s) => s,
                            Err(e) => {
                                // ADD: Adversity accounting for EAGAIN/ENOBUFS
                                if let Some(code) = e.raw_os_error() {
                                    if code == libc::EAGAIN || code == libc::ENOBUFS {
                                        let now = tsc_now_ns();
                                        let last = st.adversity_last_ns.swap(now, Ordering::Relaxed);
                                        let age_ms = (now.saturating_sub(last) / 1_000_000) as u64;
                                        if age_ms > ADVERSITY_DECAY_MS {
                                            st.adversity_score.store(1, Ordering::Relaxed);
                                        } else {
                                            let v = st.adversity_score.fetch_add(1, Ordering::Relaxed).saturating_add(1);
                                            if v >= ADVERSITY_THRESH { 
                                                st.adversity_score.store(0, Ordering::Relaxed); 
                                                let _ = maybe_rotate_src_port(st); 
                                            }
                                        }
                                    }
                                }
                                return Err(e);
                            }
                        };
                    if sent == 0 { return Err("sendmmsg txtime zero".into()); }
                        // ADD: Opportunistic errqueue micro-drain
                        occasional_errqueue_microdrain(fd);
                        return Ok(());
                    }
                    if ENABLE_UDP_ZEROCOPY && ENABLE_SENDMMSG && cnt > 1 {
                    if won.load(Ordering::Relaxed) { return Err("Cancelled".into()); }
                    let fd = sock.as_raw_fd();
                        let sent = match linux_sendmmsg_flags(fd, buf, cnt, libc::MSG_ZEROCOPY) {
                            Ok(s) => s,
                            Err(e) => {
                                // ADD: Adversity accounting for EAGAIN/ENOBUFS
                                if let Some(code) = e.raw_os_error() {
                                    if code == libc::EAGAIN || code == libc::ENOBUFS {
                                        let now = tsc_now_ns();
                                        let last = st.adversity_last_ns.swap(now, Ordering::Relaxed);
                                        let age_ms = (now.saturating_sub(last) / 1_000_000) as u64;
                                        if age_ms > ADVERSITY_DECAY_MS {
                                            st.adversity_score.store(1, Ordering::Relaxed);
                                        } else {
                                            let v = st.adversity_score.fetch_add(1, Ordering::Relaxed).saturating_add(1);
                                            if v >= ADVERSITY_THRESH { 
                                                st.adversity_score.store(0, Ordering::Relaxed); 
                                                let _ = maybe_rotate_src_port(st); 
                                            }
                                        }
                                    }
                                }
                                return Err(e);
                            }
                        };
                    if sent == 0 { return Err("sendmmsg zerocopy zero".into()); }
                        // ADD: Opportunistic errqueue micro-drain
                        occasional_errqueue_microdrain(fd);
                        return Ok(());
                    }
                }
                if cnt > 1 && ENABLE_SENDMMSG {
                    #[cfg(target_os = "linux")]
                    {
                    if won.load(Ordering::Relaxed) { return Err("Cancelled".into()); }
                    let fd = sock.as_raw_fd();
                        let sent = match linux_sendmmsg(fd, buf, cnt, addr.is_ipv6(), mtu_hint) {
                            Ok(s) => s,
                            Err(e) => {
                                // ADD: Adversity accounting for EAGAIN/ENOBUFS
                                if let Some(code) = e.raw_os_error() {
                                    if code == libc::EAGAIN || code == libc::ENOBUFS {
                                        let now = tsc_now_ns();
                                        let last = st.adversity_last_ns.swap(now, Ordering::Relaxed);
                                        let age_ms = (now.saturating_sub(last) / 1_000_000) as u64;
                                        if age_ms > ADVERSITY_DECAY_MS {
                                            st.adversity_score.store(1, Ordering::Relaxed);
                                        } else {
                                            let v = st.adversity_score.fetch_add(1, Ordering::Relaxed).saturating_add(1);
                                            if v >= ADVERSITY_THRESH { 
                                                st.adversity_score.store(0, Ordering::Relaxed); 
                                                let _ = maybe_rotate_src_port(st); 
                                            }
                                        }
                                    }
                                }
                                return Err(e);
                            }
                        };
                    if sent == 0 { return Err("sendmmsg zero".into()); }
                        // ADD: Opportunistic errqueue micro-drain
                        occasional_errqueue_microdrain(fd);
                        return Ok(());
                    }
                }
                // Fallback: per-copy loop, cancellable
                for _ in 0..cnt {
                    if won.load(Ordering::Relaxed) { return Err("Cancelled".into()); }
                    match sock.send(buf) {
                        Ok(n) if n == buf.len() => {},
                        Ok(_) => return Err("Partial send".into()),
                        Err(e) => return Err(e.into()),
                    }
                }
                Ok(())
            }

            let mut remaining = total;
            for (i, s) in socks.iter().take(ns).enumerate() {
                let quota = remaining / (ns - i) + (if remaining % (ns - i) != 0 { 1 } else { 0 });
                send_on_sock(s.as_ref(), wire, quota, won)?;
                remaining -= quota;
            }
            Ok(())
        } else {
            Err("No transport".into())
        };

        let lat = start.elapsed().as_nanos() as u64;
        match &out {
            Ok(()) => {
                st.latency_ns.store(lat, Ordering::Relaxed);
                st.success_count.fetch_add(1, Ordering::Relaxed);
                st.last_used_ns.store(tsc_now_ns(), Ordering::Relaxed);
                metrics.record_dev(st.bound_device.as_ref(), wire.len() * copies.max(1), true, Some(lat));
            }
            Err(_) => {
                st.failure_count.fetch_add(1, Ordering::Relaxed);
                if let Some(oserr) = out.as_ref().err().and_then(|e| e.downcast_ref::<std::io::Error>()) {
                    classify_and_maybe_quarantine(oserr, &st, metrics); // NEW metrics feedback
                }
                metrics.record_dev(st.bound_device.as_ref(), wire.len() * copies.max(1), false, Some(lat));
            }
        }
        out
    }

    async fn get_current_slot(&self) -> Result<Slot, Box<dyn std::error::Error>> {
        Ok(self.async_rpc_client.get_slot().await?)
    }

    async fn get_slot_leader(&self, slot: Slot) -> Result<Pubkey, Box<dyn std::error::Error>> {
        let schedule = self.leader_schedule.read().await;
        
        if let Some(leader) = schedule.schedule.get(&slot) {
            return Ok(*leader);
        }
        
        drop(schedule);
        self.refresh_leader_schedule().await?;
        
                let schedule = self.leader_schedule.read().await;
        schedule.schedule.get(&slot)
            .copied()
            .ok_or_else(|| format!("No leader found for slot {}", slot).into())
    }

    async fn get_leader_connections(
        &self,
        leader_pubkey: &Pubkey,
    ) -> Result<Vec<SocketAddr>, Box<dyn std::error::Error>> {
        let mut connections = {
            let active = self.active_connections.read().await;
            active.get(leader_pubkey).cloned().unwrap_or_default()
        };

        if connections.is_empty() || self.should_refresh_connections() {
            connections = self.establish_leader_connections(leader_pubkey).await?;
            let mut w = self.active_connections.write().await;
            w.insert(*leader_pubkey, connections.clone());
        }

        self.prioritize_connections(&mut connections).await;
        Ok(connections)
    }

    async fn establish_leader_connections(
        &self,
        leader_pubkey: &Pubkey,
    ) -> Result<Vec<SocketAddr>, Box<dyn std::error::Error>> {
        let addrs = self.get_leader_tpu_addresses(leader_pubkey).await?;
        let mut targets: Vec<(SocketAddr, Proto, PathKind)> = Vec::new();

        for a in &addrs.quic      { targets.push((*a, Proto::Quic, PathKind::Direct)); }
        for a in &addrs.quic_fwd  { targets.push((*a, Proto::Quic, PathKind::Forward)); }
        for a in &addrs.udp       { targets.push((*a, Proto::Udp,  PathKind::Direct)); }
        for a in &addrs.udp_fwd   { targets.push((*a, Proto::Udp,  PathKind::Forward)); }

        targets.sort_by_key(|t| (t.0, match t.1 { Proto::Udp => 0u8, Proto::Quic => 1u8 }, match t.2 { PathKind::Direct => 0u8, PathKind::Forward => 1u8 }));
        targets.dedup_by_key(|t| (t.0, match t.1 { Proto::Udp => 0u8, Proto::Quic => 1u8 }));

        if targets.is_empty() { return Err("No TPU endpoints exposed by leader".into()); }

        // small local-port pool to diversify 4-tuples
        let local_ports = self.port_allocator.allocate_ports(8).await;
        let mut established = Vec::new();

        for (i, (remote, proto, path_kind)) in targets.into_iter().enumerate() {
            if established.len() >= MAX_CONNECTIONS_PER_LEADER { break; }
            if self.connection_pool.contains_key(&remote) {
                if let Some(c) = self.connection_pool.get(&remote) {
                    if c.is_healthy.load(Ordering::Relaxed) {
                        established.push(remote);
                        continue;
                    }
                }
            }
            let lp = if local_ports.is_empty() { None } else { local_ports.get(i % local_ports.len()).cloned() };
            match timeout(
                Duration::from_millis(CONNECTION_TIMEOUT_MS.min(250)),
                self.create_connection(remote, proto, lp, path_kind),
            ).await {
                Ok(Ok(conn_state)) => {
                    self.connection_pool.insert(remote, conn_state);
                    established.push(remote);
                }
                _ => { /* try next */ }
            }
        }

        // Enforce path mix at the front if requested
        if REQUIRE_FORWARD_MIX && established.len() >= 2 {
            // if no forward in first two, bubble the best forward up
            let has_forward_front = established.iter().take(2).any(|a| {
                self.connection_pool.get(a).map(|s| s.path_kind == PathKind::Forward).unwrap_or(false)
            });
            if !has_forward_front {
                if let Some(pos) = established.iter().position(|a| {
                    self.connection_pool.get(a).map(|s| s.path_kind == PathKind::Forward).unwrap_or(false)
                }) {
                    established.swap(1, pos);
                }
            }
        }

        Ok(established)
    }

    async fn create_connection(
        &self,
        addr: SocketAddr,
        proto: Proto,
        local_port: Option<u16>,
        path_kind: PathKind,
    ) -> Result<ConnectionState, Box<dyn std::error::Error>> {
        #[cfg(target_os = "linux")]
        let bind_dev_opt: Option<Arc<str>> = pick_bind_device(&addr).map(|s| Arc::<str>::from(s));
        #[cfg(not(target_os = "linux"))]
        let bind_dev_opt: Option<Arc<str>> = None;

        let conn_state = ConnectionState {
            endpoint: Arc::new(Mutex::new(None)),
            quic_conn: Arc::new(Mutex::new(None)),
            udp_socket: Arc::new(Mutex::new(None)),
            alt_udp_sockets: Arc::new(Mutex::new(Vec::new())), // NEW
            last_used_ns: AtomicU64::new(tsc_now_ns()),
            success_count: AtomicU64::new(0),
            failure_count: AtomicU64::new(0),
            latency_ns: AtomicU64::new(0),
            is_healthy: AtomicBool::new(true),
            addr,
            proto,
            quarantine_until_ms: AtomicU64::new(0),
            path_kind,
            bound_device: bind_dev_opt.clone(),
            path_mtu_v4: AtomicU32::new(0),
            path_mtu_v6: AtomicU32::new(0),
            pacing_bps_last: AtomicU64::new(0),
            zc_inflight_pkts: AtomicU64::new(0),
            adversity_score: AtomicU32::new(0),
            adversity_last_ns: AtomicU64::new(tsc_now_ns()),
            neigh_last_ns: AtomicU64::new(0),
            udp_fd: AtomicI32::new(-1),
            last_operstate_check_ns: AtomicU64::new(0),
            operstate_up_cached: AtomicBool::new(true),
            gso_max_segs: AtomicU32::new(64),
            last_carrier_check_ns: AtomicU64::new(0),
            carrier_up_cached: AtomicBool::new(true),
            dev_txtime_slew_ns: AtomicI64::new(0),
            dscp_boost_until_ns: AtomicU64::new(0),
        };

        match proto {
            Proto::Quic => self.create_quic_connection(&conn_state, addr, local_port, bind_dev_opt.as_deref()).await?,
            Proto::Udp  => self.create_udp_connection(&conn_state,  addr, local_port, bind_dev_opt.as_deref()).await?,
        }
        Ok(conn_state)
    }

    async fn create_quic_connection(
        &self,
        conn_state: &ConnectionState,
        remote: SocketAddr,
        local_port: Option<u16>,
        bind_dev: Option<&Arc<str>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Build and tune the underlying UDP socket that quinn will use.
        let domain = if remote.is_ipv4() { Domain::IPV4 } else { Domain::IPV6 };
        let sock = Socket::new(domain, SockType::DGRAM, Some(SockProtocol::UDP))?;

        // REPLACE (small pre-bind block)
        setup_udp_reuse(&sock);               // NEW
        #[cfg(target_os = "linux")]
        enable_bind_no_port(&sock, &remote);
        enable_freebind(&sock, &remote);       // NEW: IP_FREEBIND for VIP failover
        set_only_v6_if(&sock, &remote);
        
        // ADD: Enable TX timestamping
        enable_tx_timestamping(&sock);
        
        // ADD: Force big RCVBUF and enable RECVERR
        if ENABLE_RCVBUFFORCE { try_force_rcvbuf(&sock, RCVBUF_TARGET_BYTES); }
        enable_recv_error(&sock, &remote);
        
        // ADD: FD hygiene - verify O_CLOEXEC and nonblocking
        #[cfg(target_os = "linux")]
        unsafe {
            let fd = sock.as_raw_fd();
            let flags = libc::fcntl(fd, libc::F_GETFD);
            if flags >= 0 { let _ = libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC); }
            let fl = libc::fcntl(fd, libc::F_GETFL);
            if fl >= 0 { let _ = libc::fcntl(fd, libc::F_SETFL, fl | libc::O_NONBLOCK); }
        }
        
        // ADD: Set socket priority for primary socket
        if ENABLE_SO_PRIORITY_PER_ALT { set_socket_priority(&sock, SO_PRIO_PRIMARY); }

        // Bind local (requested port or ephemeral)
        let bind_addr = match (remote, local_port) {
            (SocketAddr::V4(_), Some(p)) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), p),
            (SocketAddr::V6(_), Some(p)) => SocketAddr::new(IpAddr::from([0u8; 16]), p),
            (SocketAddr::V4(_), None)    => "0.0.0.0:0".parse().unwrap(),
            (SocketAddr::V6(_), None)    => "[::]:0".parse().unwrap(),
        };
        sock.bind(&bind_addr.into())?;

        // Link/QoS hints
        apply_link_hints(&sock, &remote);
        set_hoplimit(&sock, &remote);
        set_pmtu_discover(&sock, &remote);
        setup_recv_err(&sock, &remote);
        setup_recv_path_mtu(&sock, &remote);    // NEW
        // ADD (both UDP & QUIC constructors, after existing hint setup)
        if remote.is_ipv6() { set_v6_src_preference(&sock); }

        // ADD
        if remote.is_ipv6() {
            set_v6_autoflowlabel(&sock);
            set_v6_use_min_mtu(&sock);
            set_v6_dontfrag(&sock);
        }
        set_ecn_ect1(&sock, &remote);
        
        // ADD: Deterministic IPv6 flow-label manager
        set_v6_flowlabel_mgr(&sock, &remote);
        
        // ADD: IPv4 DF harden for PMTU parity
        if remote.is_ipv4() {
            set_v4_df(&sock);
        }

        // Linux-specific fast path features
        #[cfg(target_os = "linux")]
        {
        setup_so_txtime(&sock);
            setup_udp_zerocopy(&sock);
            setup_tx_timestamping(&sock);
            setup_busy_poll(&sock);
            setup_busy_poll_budget(&sock);
            setup_prefer_busy_poll(&sock); // NEW
            if let Some(ifname) = bind_dev {
                // Try fast ifindex bind first; fall back to name if needed
                let _ = bind_device_ifindex(&sock, ifname).or_else(|_| bind_device_if(&sock, ifname));
                set_so_mark_for_device(&sock, ifname);           // NEW
                // ADD (Linux)
                setup_hw_timestamping(&sock, ifname); // NEW
                
                // ADD: IPv6 unicast ifindex for route clarity
                if remote.is_ipv6() { set_v6_unicast_if(&sock, ifname); }
                
                // ADD: Load GSO cap for this device
                let cap = load_gso_cap(ifname);
                conn_state.gso_max_segs.store(cap, Ordering::Relaxed);
            }
        }

        // Nonblocking + buffers + pacing ceiling
        sock.set_nonblocking(true)?;
        let _ = sock.set_send_buffer_size(8 * 1024 * 1024);
        if ENABLE_SNDBUFFORCE { try_force_sndbuf(&sock, SNDBUF_TARGET_BYTES); }
        if ENABLE_RCVBUFFORCE { try_force_rcvbuf(&sock, RCVBUF_TARGET_BYTES); } // NEW
        set_socket_priority(&sock);                                             // NEW
        set_max_pacing_rate(&sock);

        // Connect to remote for connected-UDP fast path
        let std_udp: std::net::UdpSocket = sock.into();
        std_udp.connect(remote)?;
        
        // ADD: Set DSCP + ECN for universal QoS
        set_dscp_ecn(&std_udp, &remote);
        #[cfg(target_os = "linux")]
        {
            prime_neighbor(std_udp.as_raw_fd()); // NEW: ARP/ND pre-warm
            // ADD (Linux only; ignore errors, best-effort)
            let fd = std_udp.as_raw_fd();
            let is_v6 = matches!(remote, SocketAddr::V6(_));
            if is_v6 {
                if let Some(m) = get_path_mtu(fd, true) {
                    conn_state.path_mtu_v6.store(m.max(V6_MIN_MTU), Ordering::Relaxed);
                }
            } else {
                if let Some(m) = get_path_mtu(fd, false) {
                    conn_state.path_mtu_v4.store(m.max(V4_MIN_MTU), Ordering::Relaxed);
                }
            }
        }

        // Build Quinn endpoint over our socket
        let runtime = quinn::TokioRuntime;
        let mut endpoint = quinn::Endpoint::new_with_undefined_server_config(
            quinn::EndpointConfig::default(),
            Some(self.create_quic_client_config()),
            std_udp,
            runtime,
        )?;

        // Establish/connect
        let connecting = endpoint.connect(remote, "validator")?;
        let connection = connecting.await?;
        *conn_state.endpoint.lock().await = Some(endpoint);
        *conn_state.quic_conn.lock().await = Some(Arc::new(connection));

        Ok(())
    }

    // REPLACE
    async fn create_udp_connection(
        &self,
        conn_state: &ConnectionState,
        remote: SocketAddr,
        local_port: Option<u16>,
        bind_dev: Option<&Arc<str>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let domain = if remote.is_ipv4() { Domain::IPV4 } else { Domain::IPV6 };

        // Helper to build & connect one UDP socket with all hints
        let build_one = |lp: Option<u16>| -> std::io::Result<std::net::UdpSocket> {
            let sock = Socket::new(domain, SockType::DGRAM, Some(SockProtocol::UDP))?;
            // REPLACE tiny insert
            setup_udp_reuse(&sock);  // NEW: lowers bind contention across SRC_PORT_COPIES
            #[cfg(target_os = "linux")]
            enable_bind_no_port(&sock, &remote);
            enable_freebind(&sock, &remote);  // NEW: IP_FREEBIND for VIP failover
            
            // ADD: Enable TX timestamping
            enable_tx_timestamping(&sock);
            
            // ADD: Force big RCVBUF and enable RECVERR
            if ENABLE_RCVBUFFORCE { try_force_rcvbuf(&sock, RCVBUF_TARGET_BYTES); }
            enable_recv_error(&sock, &remote);
            
            // ADD: FD hygiene - verify O_CLOEXEC and nonblocking
            #[cfg(target_os = "linux")]
            unsafe {
                let fd = sock.as_raw_fd();
                let flags = libc::fcntl(fd, libc::F_GETFD);
                if flags >= 0 { let _ = libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC); }
                let fl = libc::fcntl(fd, libc::F_GETFL);
                if fl >= 0 { let _ = libc::fcntl(fd, libc::F_SETFL, fl | libc::O_NONBLOCK); }
            }
            
            // ADD: Set socket priority for alt socket
            if ENABLE_SO_PRIORITY_PER_ALT { set_socket_priority(&sock, SO_PRIO_ALT); }
            
            let bind_addr = match (remote, lp) {
                (SocketAddr::V4(_), Some(p)) => SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), p),
                (SocketAddr::V6(_), Some(p)) => SocketAddr::new(IpAddr::from([0u8; 16]), p),
                (SocketAddr::V4(_), None)    => "0.0.0.0:0".parse().unwrap(),
                (SocketAddr::V6(_), None)    => "[::]:0".parse().unwrap(),
            };
            set_only_v6_if(&sock, &remote);     // NEW
        sock.bind(&bind_addr.into())?;

            apply_link_hints(&sock, &remote);
            set_hoplimit(&sock, &remote);
            set_pmtu_discover(&sock, &remote);   // NEW
            setup_recv_err(&sock, &remote); // NEW
            // ADD (both UDP & QUIC constructors, after existing hint setup)
            if remote.is_ipv6() { set_v6_src_preference(&sock); }

            // ADD
            if remote.is_ipv6() {
                set_v6_autoflowlabel(&sock);
                set_v6_use_min_mtu(&sock);
                set_v6_dontfrag(&sock);
            }
            set_ecn_ect1(&sock, &remote);

            #[cfg(target_os = "linux")]
            {
                setup_so_txtime(&sock);
                setup_udp_zerocopy(&sock);
                setup_tx_timestamping(&sock);
                setup_busy_poll(&sock);
                setup_busy_poll_budget(&sock); // NEW
        }

        #[cfg(target_os = "linux")]
        if let Some(ifname) = bind_dev {
            // Try fast ifindex bind first; fall back to name if needed
            let _ = bind_device_ifindex(&sock, ifname).or_else(|_| bind_device_if(&sock, ifname));
            set_so_mark_for_device(&sock, ifname);           // NEW
            
            // ADD: IPv6 unicast ifindex for route clarity
            if remote.is_ipv6() { set_v6_unicast_if(&sock, ifname); }
            
            // ADD: Load GSO cap for this device
            let cap = load_gso_cap(ifname);
            conn_state.gso_max_segs.store(cap, Ordering::Relaxed);
        }

        // device bind already performed above
        // DSCP/ECN + kernel priority
        #[cfg(target_os = "linux")]
        set_dscp_ecn(&std_udp, &remote);

        // optional thread pinning when a device is chosen
        #[cfg(target_os = "linux")]
        if let Some(ifname) = bind_dev {
            pin_current_thread_to_device(Some(ifname));
        }

        sock.set_nonblocking(true)?;
        let _ = sock.set_send_buffer_size(8 * 1024 * 1024);
            if ENABLE_SNDBUFFORCE { try_force_sndbuf(&sock, SNDBUF_TARGET_BYTES); }
            if ENABLE_RCVBUFFORCE { try_force_rcvbuf(&sock, RCVBUF_TARGET_BYTES); } // NEW
            set_socket_priority(&sock);                                             // NEW
            set_max_pacing_rate(&sock);

            let udp_std: std::net::UdpSocket = sock.into();
            udp_std.connect(remote)?;
            
            // ADD: Set DSCP + ECN for universal QoS on alt socket
            set_dscp_ecn(&udp_std, &remote);
            #[cfg(target_os = "linux")]
            {
                prime_neighbor(udp_std.as_raw_fd()); // NEW: ARP/ND pre-warm
                // ADD (Linux only; ignore errors, best-effort)
                let fd = udp_std.as_raw_fd();
                let is_v6 = matches!(remote, SocketAddr::V6(_));
                if is_v6 {
                    if let Some(m) = get_path_mtu(fd, true) {
                        conn_state.path_mtu_v6.store(m.max(V6_MIN_MTU), Ordering::Relaxed);
                    }
                } else {
                    if let Some(m) = get_path_mtu(fd, false) {
                        conn_state.path_mtu_v4.store(m.max(V4_MIN_MTU), Ordering::Relaxed);
                    }
                }
            }
            Ok(udp_std)
        };

        // Primary socket (local_port as requested)
        let udp0 = UdpSocket::from(build_one(local_port)?);
        udp0.set_send_buffer_size(8 * 1024 * 1024).ok();
        udp0.set_recv_buffer_size(8 * 1024 * 1024).ok();
        *conn_state.udp_socket.lock().await = Some(Arc::new(udp0));

        // Optional extra source-port sockets for ECMP/RSS diversity
        let mut alts: Vec<Arc<UdpSocket>> = Vec::new();
        if ENABLE_MULTI_SRC_PORTS {
            let extra = SRC_PORT_COPIES.saturating_sub(1).min(3);
            for _ in 0..extra {
                let u = UdpSocket::from(build_one(None)?);
                u.set_send_buffer_size(8 * 1024 * 1024).ok();
                u.set_recv_buffer_size(8 * 1024 * 1024).ok();
                alts.push(Arc::new(u));
            }
        }
        *conn_state.alt_udp_sockets.lock().await = alts;

        Ok(())
    }

    // REPLACE
    fn create_quic_client_config(&self) -> ClientConfig {
        let mut crypto = rustls::ClientConfig::builder()
            .with_safe_defaults()
            .with_custom_certificate_verifier(Arc::new(SkipServerVerification))
            .with_no_client_auth();

        if ENABLE_QUIC_EARLY_DATA {
            crypto.enable_early_data = true;
            crypto.session_storage = Arc::new(rustls::client::ClientSessionMemoryCache::new(1024));
        }

        let mut cfg = ClientConfig::new(Arc::new(crypto));
        let transport = Arc::get_mut(&mut cfg.transport).unwrap();

        // QUIC transport tuning (datagrams only; no streams)
        transport.max_concurrent_bidi_streams(0u8.into());
        transport.max_concurrent_uni_streams(QUIC_MAX_UNSTAKED_CONCURRENT_STREAMS.try_into().unwrap());
        transport.keep_alive_interval(Some(Duration::from_secs(1)));
        transport.max_idle_timeout(Some(Duration::from_secs(30).try_into().unwrap()));

        if ENABLE_QUIC_DATAGRAMS {
            transport.datagram_send_buffer_size(Some(QUIC_DGRAM_SEND_BUF));
            transport.datagram_receive_buffer_size(Some(QUIC_DGRAM_RECV_BUF));
            // Floor payload to 1232B to never exceed min IPv6 path MTU (QUIC rule-of-thumb).
            // (Our UDP path still PMTU-hardens; this avoids QUIC-side fragmentation)
            transport.max_udp_payload_size(Some(1232));
            transport.datagram_send_initial_max_data(QUIC_DGRAM_SEND_BUF as u64);
        }

        cfg
    }

    async fn get_leader_tpu_addresses(
        &self,
        leader_pubkey: &Pubkey,
    ) -> Result<TpuAddrs, Box<dyn std::error::Error>> {
        let should_refresh = {
            let cache = self.leader_schedule.read().await;
            cache.last_update.elapsed() > Duration::from_millis(LEADER_SCHEDULE_CACHE_TTL_MS)
        };

        if should_refresh {
            self.refresh_leader_schedule().await?;
        }

        let cache = self.leader_schedule.read().await;
        cache.tpu_addresses.get(leader_pubkey)
            .cloned()
            .ok_or_else(|| format!("TPU addresses not found for leader {}", leader_pubkey).into())
    }

    async fn refresh_leader_schedule(&self) -> Result<(), Box<dyn std::error::Error>> {
        let slot = self.async_rpc_client.get_slot().await?;
        let epoch_info = self.async_rpc_client.get_epoch_info().await?;
        let epoch = epoch_info.epoch;
        let slots_in_epoch = epoch_info.slots_in_epoch;

        let leader_schedule = self.async_rpc_client
            .get_leader_schedule_with_commitment(
                Some(slot),
                CommitmentConfig::finalized()
            ).await?
            .ok_or("Failed to get leader schedule")?;

        let mut schedule_map = BTreeMap::new();
        let start_slot = epoch * slots_in_epoch;
        for (pubkey_str, slots) in leader_schedule.iter() {
            let pubkey = pubkey_str.parse::<Pubkey>()?;
            for &leader_slot in slots {
                schedule_map.insert(start_slot + leader_slot as u64, pubkey);
            }
        }

        let cluster_nodes = self.async_rpc_client.get_cluster_nodes().await?;
        let mut tpu_addresses: HashMap<Pubkey, TpuAddrs> = HashMap::new();

        for node in cluster_nodes {
            let pk = match node.pubkey.parse::<Pubkey>() {
                Ok(p) => p,
                Err(_) => continue,
            };

            let mut udp = Vec::new();
            let mut udp_fwd = Vec::new();
            let mut quic = Vec::new();
            let mut quic_fwd = Vec::new();

            if let Some(a) = node.tpu { udp.push(a); }
            if let Some(a) = node.tpu_forwards { udp_fwd.push(a); }

            // Newer RPCs expose QUIC TPU addrs; if not, we'll just have UDP
            #[allow(deprecated)]
            {
                if let Some(a) = node.tpu_quic { quic.push(a); }
                if let Some(a) = node.tpu_quic_forwards { quic_fwd.push(a); }
            }

            tpu_addresses.insert(pk, TpuAddrs { udp, udp_fwd, quic, quic_fwd });
        }

        let mut cache = self.leader_schedule.write().await;
        cache.schedule = schedule_map;
        cache.tpu_addresses = tpu_addresses;
        cache.current_epoch = epoch;
        cache.slots_per_epoch = slots_in_epoch;
        cache.last_update = Instant::now();
        cache.last_slot = slot;
        Ok(())
    }

    async fn prioritize_connections(&self, connections: &mut Vec<SocketAddr>) {
        let mut scored = Vec::with_capacity(connections.len());
        for addr in connections.iter().copied() {
            if let Some(conn) = self.connection_pool.get(&addr) {
                let succ = conn.success_count.load(Ordering::Relaxed) as f64;
                let fail = conn.failure_count.load(Ordering::Relaxed) as f64;
                let total = succ + fail;
                let sr = if total > 10.0 { succ / total } else { 0.75 };
                let lat_ns = conn.latency_ns.load(Ordering::Relaxed) as f64;
                let latency_score = if lat_ns > 0.0 { 1.0 / (1.0 + lat_ns / 700_000.0) } else { 0.6 };
                let age_ms = (tsc_now_ns().saturating_sub(conn.last_used_ns.load(Ordering::Relaxed)) / 1_000_000) as f64;
                let recency = if age_ms <= 250.0 { 0.25 } else { (1500.0 / (1500.0 + age_ms)).max(0.2) * 0.15 };
                let healthy = if conn.is_healthy.load(Ordering::Relaxed) { 0.05 } else { -0.3 };
                let proto_bonus = match conn.quic_conn.try_lock() {
                    Ok(g) if g.as_ref().is_some() => 0.04,
                    _ => 0.0,
                };
                let ipv4_bonus = if IPV4_BIAS && addr.is_ipv4() { 0.03 } else { 0.0 };

                // REPLACE the 'score' computation block in prioritize_connections():
                let dev_queue_penalty = if let Some(dev) = conn.bound_device.as_ref() {
                    if let Some(stats) = self.metrics.device_stats.get(dev) {
                        let q = stats.avg_tx_queue_ns.load(Ordering::Relaxed) as f64; // ns
                        // 0.0 at <=150µs, up to -0.04 at >=900µs
                        (-0.04f64) * ((q / 900_000.0).min(1.0)) * ((q >= 150_000.0) as i32 as f64)
                    } else { 0.0 }
                } else { 0.0 };

                let mut score = 0.43*sr + 0.35*latency_score + recency + healthy + proto_bonus + ipv4_bonus + dev_queue_penalty;
                if !score.is_finite() { score = 0.0; }
                score = score.clamp(-1.0, 2.0);
                scored.push((addr, score));
            }
        }
        scored.sort_by(|a,b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

        let top_n = connections.len().min(MAX_CONNECTIONS_PER_LEADER);
        let mut selected: Vec<SocketAddr> = scored.iter().take(top_n).map(|x| x.0).collect();

        // 1–2% chance to pull in a tail candidate to discover recovering paths
        if selected.len() >= 3 && rand::random::<u8>() % 64 == 0 {
            if let Some(tail) = scored.get(selected.len()).map(|t| t.0) {
                selected.pop();
                selected.push(tail);
            }
        }

        // light exploration swap
        if selected.len() >= 4 && (fast_hash_u64(Instant::now().elapsed().as_nanos() as u64) & 7) == 0 {
            selected.swap(2, selected.len()-1);
        }
        *connections = selected;
    }

    fn should_refresh_connections(&self) -> bool {
        // Prefer deterministic refresh when pool health degrades, else 2% random exploration
        let unhealthy = self.connection_pool.iter()
            .filter(|e| !e.value().is_healthy.load(Ordering::Relaxed))
            .count();
        let total = self.connection_pool.len().max(1);
        let bad_ratio = (unhealthy as f64) / (total as f64);

        bad_ratio >= 0.20 || thread_rng().gen_bool(0.02)
    }

    async fn record_recent_slot(&self, slot: Slot) {
        let mut recent_slots = self.recent_slots.write().await;
        recent_slots.insert(slot, Instant::now());
        
        let cutoff = Instant::now() - Duration::from_secs(60);
        recent_slots.retain(|_, instant| *instant > cutoff);
    }

    async fn reconnector_loop(self) {
        let mut tick = interval(Duration::from_millis(300));
        while !self.shutdown.load(Ordering::Relaxed) {
            tick.tick().await;
            let mut todo: Vec<(SocketAddr, Proto)> = Vec::new();

            for entry in self.connection_pool.iter() {
                let addr = *entry.key();
                let st = entry.value();
                let unhealthy = !st.is_healthy.load(Ordering::Relaxed);
                let old = st.last_used.read().unwrap().elapsed() > Duration::from_secs(2);

                let quic_dead = matches!(st.quic_conn.try_lock(), Ok(g) if g.as_ref().is_none());
                let udp_dead  = matches!(st.udp_socket.try_lock(), Ok(g) if g.as_ref().is_none());

                // schedule reconnect if transport is absent OR marked unhealthy and stale
                if (st.proto == Proto::Quic && (quic_dead || (unhealthy && old))) ||
                   (st.proto == Proto::Udp  && (udp_dead  || (unhealthy && old))) {
                    todo.push((addr, st.proto));
                }
            }

            if todo.is_empty() { continue; }

            let local_ports = self.port_allocator.allocate_ports(todo.len().min(6)).await;
            for (i, (addr, proto)) in todo.into_iter().enumerate() {
                let lp = if local_ports.is_empty() { None } else { local_ports.get(i % local_ports.len()).cloned() };
                let res = match self.create_connection(addr, proto, lp).await {
                    Ok(new_state) => {
                        if let Some((_, old)) = self.connection_pool.remove(&addr) {
                            if let Some(ep) = old.endpoint.lock().await.take() { ep.close(0u32.into(), b"reopen"); }
                        }
                        self.connection_pool.insert(addr, new_state);
                        Ok(())
                    }
                    Err(e) => Err(e),
                };
                if res.is_err() {
                    // leave old state; health task will continue to track
                }
            }
        }
    }

    // REPLACE
    async fn errqueue_drain_loop(self: Arc<Self>) {
        #[cfg(not(target_os = "linux"))]
        { return; }

        #[cfg(target_os = "linux")]
        {
            if !ENABLE_ERRQUEUE_EPOLL {
                // Fallback to your existing polling implementation:
                return self.errqueue_drain_loop_polling().await;
            }

            use libc::{mmsghdr, msghdr, iovec, cmsghdr, CMSG_FIRSTHDR, CMSG_NXTHDR, CMSG_DATA, MSG_ERRQUEUE, MSG_DONTWAIT};

            // Build epoll set from current UDP sockets; rebuild periodically to track changes.
            let epfd = unsafe { epoll_create1(EPOLL_CLOEXEC) };
            if epfd < 0 { return; }

            let mut tick = tokio::time::interval(std::time::Duration::from_millis(50));
            let mut rebuild = true;

            let mut events: Vec<epoll_event> = vec![epoll_event { events: 0, u64: 0 }; EPOLL_BATCH];

            while !self.shutdown.load(Ordering::Relaxed) {
                if rebuild {
                    // Clear and re-add all fds
                    // (trivial strategy: close and recreate epoll set)
                    unsafe { libc::close(epfd); }
                    let epfd_new = unsafe { epoll_create1(EPOLL_CLOEXEC) };
                    if epfd_new < 0 { break; }
                    let epfd_local = epfd_new;

                    for entry in self.connection_pool.iter() {
                        let st = entry.value();
                        if let Ok(g) = st.udp_socket.try_lock() {
                            if let Some(udp) = g.as_ref() {
                                let fd = udp.as_raw_fd();
                                let mut ev = epoll_event { events: (EPOLLERR | EPOLLPRI | EPOLLET) as u32, u64: fd as u64 };
                                unsafe { let _ = epoll_ctl(epfd_local, EPOLL_CTL_ADD, fd, &mut ev as *mut _); }
                            }
                        }
                    }
                    // swap epfd
                    std::mem::forget(epfd_local); // we intentionally keep using latest epfd_new by shadowing
                    // rebind variable
                    let _ = epfd_new; // suppress unused warning
                }

                // Wait up to 1ms; wake on any POLLERR/IN/PRI on watched sockets
                let n = unsafe { epoll_wait(epfd, events.as_mut_ptr(), EPOLL_BATCH as i32, ERRQUEUE_EPOLL_TIMEOUT_MS) };
                if n > 0 {
                    for i in 0..(n as usize) {
                        let fd = events[i].u64 as libc::c_int;

                        // Batch buffers for this fd
                        let mut bufs = [[0u8; 64]; ERRQUEUE_BATCH];
                        let mut cbufs = [[0u8; 512]; ERRQUEUE_BATCH];
                        let mut iovs: [iovec; ERRQUEUE_BATCH] = unsafe { std::mem::zeroed() };
                        let mut hdrs: [msghdr; ERRQUEUE_BATCH] = unsafe { std::mem::zeroed() };
                        let mut msgs: [mmsghdr; ERRQUEUE_BATCH] = unsafe { std::mem::zeroed() };
                        for j in 0..ERRQUEUE_BATCH {
                            iovs[j] = iovec { iov_base: bufs[j].as_mut_ptr() as *mut _, iov_len: bufs[j].len() };
                            hdrs[j].msg_iov = &mut iovs[j] as *mut iovec;
                            hdrs[j].msg_iovlen = 1;
                            hdrs[j].msg_control = cbufs[j].as_mut_ptr() as *mut libc::c_void;
                            hdrs[j].msg_controllen = cbufs[j].len();
                            msgs[j].msg_hdr = hdrs[j];
                            msgs[j].msg_len = 0;
                        }

                        // Locate ConnectionState for metrics/accounting
                        let (dev_opt, st_opt) = {
                            let mut found = (None, None);
                            for entry in self.connection_pool.iter() {
                                if let Ok(g) = entry.value().udp_socket.try_lock() {
                                    if let Some(udp) = g.as_ref() {
                                        if udp.as_raw_fd() == fd {
                                            let st = entry.value();
                                            found = (st.bound_device.as_ref().cloned(), Some(st.clone()));
                                            break;
                                        }
                                    }
                                }
                            }
                            found
                        };

                        let mut budget = ERRQUEUE_MAX_PER_SOCKET;
                        loop {
                            if budget == 0 { break; }
                            let want = ERRQUEUE_BATCH.min(budget);
                            let got = unsafe {
                                libc::recvmmsg(fd, msgs.as_mut_ptr(), want as u32, (MSG_ERRQUEUE | MSG_DONTWAIT) as i32, std::ptr::null_mut())
                            };
                            if got <= 0 { break; }
                            budget -= got as usize;

                            for j in 0..got as usize {
                                let mut tx_ack_ns: Option<u64> = None;
                                let mut tx_sched_ns: Option<u64> = None;
                                let mut zc_done: u64 = 0;

                                unsafe {
                                    let mut c = CMSG_FIRSTHDR(&msgs[j].msg_hdr);
                                    while !c.is_null() {
                                        let lvl = (*c).cmsg_level;
                                        let typ = (*c).cmsg_type;

                                        if lvl == libc::SOL_SOCKET && typ == SO_TIMESTAMPING {
                                            #[repr(C)]
                                            struct Timespec { tv_sec: libc::time_t, tv_nsec: libc::c_long }
                                            let arr = CMSG_DATA(c) as *const Timespec; // 3 entries: SW, HW, RAW(HW)
                                            if !arr.is_null() {
                                                // Prefer RAW HW if present, else SW
                                                let hw = unsafe { *arr.add(2) };
                                                let sw = unsafe { *arr.add(0) };
                                                let pick = if hw.tv_sec != 0 || hw.tv_nsec != 0 { hw } else { sw };
                                                let k_ns = (pick.tv_sec as i128)*1_000_000_000 + (pick.tv_nsec as i128);
                                                let tsc  = tsc_now_ns() as i128;
                                                let off  = (k_ns - tsc).clamp(i64::MIN as i128, i64::MAX as i128) as i64;

                                                if let Some(st) = st_opt.as_ref() {
                                                    // IIR: 1/64 toward new delta; clamp to sensible window
                                                    let cur = st.dev_txtime_slew_ns.load(Ordering::Relaxed);
                                                    let err = off.saturating_sub(cur);
                                                    let adj = err / 64;
                                                    let nxt = (cur as i128 + adj as i128)
                                                        .clamp(-1_000_000i128, 1_000_000i128) as i64; // ±1ms bound
                                                    st.dev_txtime_slew_ns.store(nxt, Ordering::Relaxed);
                                                }
                                                
                                                // Legacy handling for existing code
                                                let ns = (pick.tv_sec as u64)*1_000_000_000 + (pick.tv_nsec as u64);
                                                if tx_sched_ns.is_none() { tx_sched_ns = Some(ns); }
                                                else { tx_ack_ns = Some(ns); }
                                                if tx_ack_ns.is_none() { tx_ack_ns = Some(ns); }
                                            }
                                        }

                                        if (lvl == libc::SOL_IP && typ == libc::IP_RECVERR) ||
                                           (lvl == libc::SOL_IPV6 && typ == libc::IPV6_RECVERR) {
                                            #[repr(C)]
                                            struct SockExtErr { ee_errno:u32, ee_origin:u8, ee_type:u8, ee_code:u8, ee_pad:u8, ee_info:u32, ee_data:u32 }
                                            let see = CMSG_DATA(c) as *const SockExtErr;
                                            if !see.is_null() {
                                                if (*see).ee_origin == 5 { 
                                                    zc_done = zc_done.saturating_add(1); // ZEROCOPY
                                                    if let Some(st) = st_opt.as_ref() {
                                                        saturating_dec(&st.zc_inflight_pkts, 1);
                                                    }
                                                }
                                                if (*see).ee_origin == 6 {
                                                    // TXTIME origin → treat as queue hot
                                                    self.metrics.record_dev_tx(dev_opt.as_ref(), None, Some(TXTIME_MISS_PENALTY_QUEUE_NS), 0);
                                                }
                                                if (*see).ee_errno as i32 == libc::EMSGSIZE {
                                                    if let Some(st) = st_opt.as_ref() {
                                                        st.is_healthy.store(false, Ordering::Relaxed);
                                                        st.quarantine_until_ms.store(now_unix_ms() + 2_000, Ordering::Relaxed);
                                                    }
                                                }
                                            }
                                        }

                                        // ADD: IPv6 Path MTU cmsg handling
                                        if lvl == libc::IPPROTO_IPV6 && typ == 61 /* IPV6_PATHMTU */ {
                                            let p = CMSG_DATA(c) as *const Ip6MtuInfo;
                                            if !p.is_null() {
                                                let mtu = unsafe { (*p).ip6m_mtu }.max(V6_MIN_MTU);
                                                if let Some(st) = st_opt.as_ref() {
                                                    st.path_mtu_v6.store(mtu, Ordering::Relaxed);
                                                }
                                            }
                                        }
                                        c = CMSG_NXTHDR(&msgs[j].msg_hdr, c);
                                    }
                                }

                                if tx_ack_ns.is_some() {
                                    let now_ns = txtime_now_ns();
                                    let ack_age = now_ns.saturating_sub(tx_ack_ns.unwrap_or(0));
                                    let queue_ns = if let (Some(a), Some(s)) = (tx_ack_ns, tx_sched_ns) { a.saturating_sub(s) } else { 0 };
                                    // ADD: Micro-slew TXTIME offset from TX timestamps
                                    #[cfg(target_os = "linux")]
                                    if let Some(tx_ack) = tx_ack_ns {
                                        let tsc_now = tsc_now_ns();
                                        maybe_slew_txtime_offset(tx_ack, tsc_now);
                                    }
                                    self.metrics.record_dev_tx(dev_opt.as_ref(), Some(ack_age), if queue_ns>0 {Some(queue_ns)} else {None}, zc_done);
                                } else if zc_done > 0 {
                                    self.metrics.record_dev_tx(dev_opt.as_ref(), None, None, zc_done);
                                }
                            }
                        }
                    }
                }

                // Rebuild epoll set occasionally to track socket churn
                tick.tick().await;
                rebuild = true;
            }

            unsafe { libc::close(epfd); }
        }
    }

    // Fallback polling implementation
    async fn errqueue_drain_loop_polling(self: Arc<Self>) {
        #[cfg(not(target_os = "linux"))]
        { return; }

        #[cfg(target_os = "linux")]
        {
            use libc::{mmsghdr, msghdr, iovec, cmsghdr, CMSG_FIRSTHDR, CMSG_NXTHDR, CMSG_DATA, MSG_ERRQUEUE, MSG_DONTWAIT};
            let mut tick = tokio::time::interval(Duration::from_millis(5));
            while !self.shutdown.load(Ordering::Relaxed) {
                tick.tick().await;

                for entry in self.connection_pool.iter() {
                    let st = entry.value();
                    let dev = st.bound_device.as_ref().cloned();

                    if let Ok(guard) = st.udp_socket.try_lock() {
                        if let Some(udp) = guard.as_ref() {
                            let fd = udp.as_raw_fd();
                            let is_v6 = st.addr.is_ipv6();

                            // Batch buffers
                            let mut bufs = [[0u8; 64]; ERRQUEUE_BATCH];
                            let mut cbufs = [[0u8; 512]; ERRQUEUE_BATCH];
                            let mut iovs: [iovec; ERRQUEUE_BATCH] = unsafe { std::mem::zeroed() };
                            let mut hdrs: [msghdr; ERRQUEUE_BATCH] = unsafe { std::mem::zeroed() };
                            let mut msgs: [mmsghdr; ERRQUEUE_BATCH] = unsafe { std::mem::zeroed() };

                            for i in 0..ERRQUEUE_BATCH {
                                iovs[i] = iovec {
                                    iov_base: bufs[i].as_mut_ptr() as *mut _,
                                    iov_len:  bufs[i].len(),
                                };
                                hdrs[i].msg_iov = &mut iovs[i] as *mut iovec;
                                hdrs[i].msg_iovlen = 1;
                                hdrs[i].msg_control = cbufs[i].as_mut_ptr() as *mut libc::c_void;
                                hdrs[i].msg_controllen = cbufs[i].len();
                                msgs[i].msg_hdr = hdrs[i];
                                msgs[i].msg_len = 0;
                            }

                            let mut budget = ERRQUEUE_MAX_PER_SOCKET;
                            loop {
                                if budget == 0 { break; }
                                let want = ERRQUEUE_BATCH.min(budget);
                                let got = unsafe {
                                    recvmmsg(fd, msgs.as_mut_ptr(), want as u32, (MSG_ERRQUEUE | MSG_DONTWAIT) as i32, std::ptr::null_mut())
                                };
                                if got <= 0 { break; }
                                budget -= got as usize;

                                for i in 0..got as usize {
                                let mut tx_ack_ns: Option<u64> = None;
                                let mut tx_sched_ns: Option<u64> = None;
                                let mut zc_done: u64 = 0;

                                unsafe {
                                        let mut c = CMSG_FIRSTHDR(&msgs[i].msg_hdr);
                                    while !c.is_null() {
                                        let lvl = (*c).cmsg_level;
                                        let typ = (*c).cmsg_type;

                                        if lvl == libc::SOL_SOCKET && typ == 37 /* SO_TIMESTAMPING */ {
                                            let base = CMSG_DATA(c) as *const libc::timespec;
                                            let ts0 = *base;
                                                let ns = (ts0.tv_sec as u64)*1_000_000_000 + (ts0.tv_nsec as u64);
                                                if tx_sched_ns.is_none() { tx_sched_ns = Some(ns); }
                                                else { tx_ack_ns = Some(ns); }
                                                if tx_ack_ns.is_none() { tx_ack_ns = Some(ns); }
                                            }

                                            if (lvl == libc::SOL_IP && typ == libc::IP_RECVERR) ||
                                               (lvl == libc::SOL_IPV6 && typ == libc::IPV6_RECVERR) {
                                            #[repr(C)]
                                                struct SockExtErr { ee_errno:u32, ee_origin:u8, ee_type:u8, ee_code:u8, ee_pad:u8, ee_info:u32, ee_data:u32 }
                                            let see = CMSG_DATA(c) as *const SockExtErr;
                                                if !see.is_null() {
                                                    if (*see).ee_origin == 5 { 
                                                        zc_done = zc_done.saturating_add(1);
                                                        saturating_dec(&st.zc_inflight_pkts, 1);
                                                    }
                                                    if (*see).ee_origin == 6 {
                                                        tx_ack_ns = None;
                                                        tx_sched_ns = None;
                                                        self.metrics.record_dev_tx(dev.as_ref(), None, Some(TXTIME_MISS_PENALTY_QUEUE_NS), 0);
                                                    }
                                                    if (*see).ee_errno as i32 == libc::EMSGSIZE {
                                                        st.is_healthy.store(false, Ordering::Relaxed);
                                                        st.quarantine_until_ms.store(now_unix_ms() + 2_000, Ordering::Relaxed);
                                                    }
                                                }
                                            }

                                            // ADD: IPv6 Path MTU cmsg handling
                                            if lvl == libc::IPPROTO_IPV6 && typ == 61 /* IPV6_PATHMTU */ {
                                                let p = CMSG_DATA(c) as *const Ip6MtuInfo;
                                                if !p.is_null() {
                                                    let mtu = unsafe { (*p).ip6m_mtu }.max(V6_MIN_MTU);
                                                    st.path_mtu_v6.store(mtu, Ordering::Relaxed);
                                                }
                                            }
                                            c = CMSG_NXTHDR(&msgs[i].msg_hdr, c);
                                        }
                                    }

                                if tx_ack_ns.is_some() {
                                        let now_ns = unsafe {
                                            let mut ts: libc_timespec = std::mem::zeroed();
                                            libc::clock_gettime(libc::CLOCK_MONOTONIC_RAW, &mut ts);
                                        (ts.tv_sec as u64)*1_000_000_000 + (ts.tv_nsec as u64)
                                    };
                                    let ack_age = now_ns.saturating_sub(tx_ack_ns.unwrap_or(0));
                                        let queue_ns = if let (Some(a), Some(s)) = (tx_ack_ns, tx_sched_ns) { a.saturating_sub(s) } else { 0 };
                                    // ADD: Micro-slew TXTIME offset from TX timestamps
                                    #[cfg(target_os = "linux")]
                                    if let Some(tx_ack) = tx_ack_ns {
                                        let tsc_now = tsc_now_ns();
                                        maybe_slew_txtime_offset(tx_ack, tsc_now);
                                    }
                                    self.metrics.record_dev_tx(dev.as_ref(), Some(ack_age), if queue_ns>0 {Some(queue_ns)} else {None}, zc_done);
                                } else if zc_done > 0 {
                                    self.metrics.record_dev_tx(dev.as_ref(), None, None, zc_done);
                                }
                            }
                            }

                            let _ = get_path_mtu(fd, is_v6);
                        }
                    }
                }
            }
        }
    }

    async fn start_background_tasks(&self) {
        let health_monitor = self.health_monitor.clone();
        let connection_pool = self.connection_pool.clone();
        let metrics = self.metrics.clone();
        let shutdown = self.shutdown.clone();

        tokio::spawn(async move {
            let mut tick = interval(Duration::from_millis(HEALTH_CHECK_INTERVAL_MS));
            while !shutdown.load(Ordering::Relaxed) {
                tick.tick().await;
                Self::health_check_task(&health_monitor, &connection_pool, &metrics).await;
            }
        });

        let connection_pool = self.connection_pool.clone();
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_secs(10));
            while !shutdown.load(Ordering::Relaxed) {
                tick.tick().await;
                Self::cleanup_stale_connections(&connection_pool).await;
            }
{{ ... }}
                tick.tick().await;
                port_allocator.cleanup_expired_ports().await;
            }
        });

        let leader_schedule = self.leader_schedule.clone();
        let async_rpc = self.async_rpc_client.clone();
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            let mut tick = interval(Duration::from_millis(LEADER_SCHEDULE_CACHE_TTL_MS));
            while !shutdown.load(Ordering::Relaxed) {
                tick.tick().await;
                if let Ok(slot) = async_rpc.get_slot().await {
                    let mut cache = leader_schedule.write().await;
                    cache.last_slot = slot;
                }
            }
        });

        // Pre-warm next K leaders
        let this = self.clone();
        tokio::spawn(async move {
            const K: u64 = 6; // warm next 6 slots
            let mut tick = interval(Duration::from_millis(120));
            while !this.shutdown.load(Ordering::Relaxed) {
                tick.tick().await;
                if let Ok(cur) = this.get_current_slot().await {
                    let cache = this.leader_schedule.read().await;
                    for s in (cur+1)..=(cur+K) {
                        if let Some(pk) = cache.schedule.get(&s) {
                            let _ = this.establish_leader_connections(pk).await;
                        }
                    }
                }
            }
        });

        // NEW: reconnection loop
        let this = self.clone();
        tokio::spawn(async move {
            this.reconnector_loop().await;
        });

        // NEW: errqueue drainer for UDP zerocopy & TX timestamps
        let this = self.clone();
        tokio::spawn(async move {
            // ADD: Pin errqueue reader to NIC XPS CPU for completion locality
            #[cfg(target_os = "linux")]
            if let Some(dev) = this.connection_pool.iter().next().and_then(|entry| entry.value().bound_device.as_ref()) {
                pin_to_nic_xps_once(dev);
            }
            this.errqueue_drain_loop().await;
        });
                    consecutive_failures,
                    avg_latency_ns: conn_state.latency_ns.load(Ordering::Relaxed),
                    success_rate,
                }
            };

            if health.success_rate > 0.0 && health.avg_latency_ns < 1_000_000_000 {
                total_latency += health.avg_latency_ns;
                count += 1;
            }

            health_monitor.connection_health.insert(addr, health);
        }

        if count > 0 {
            let avg = total_latency / count;
            let current_avg = metrics.avg_latency_ns.load(Ordering::Relaxed);
            let new_avg = (current_avg * 7 + avg * 3) / 10;
            metrics.avg_latency_ns.store(new_avg, Ordering::Relaxed);
        }

        for addr in to_mark_unhealthy {
            if let Some(mut entry) = connection_pool.get_mut(&addr) {
                entry.value_mut().is_healthy.store(false, Ordering::Relaxed);
            }
        }
    }

    async fn cleanup_stale_connections(connection_pool: &DashMap<SocketAddr, ConnectionState>) {
        let stale_threshold = Duration::from_secs(120);
        let unhealthy_threshold = Duration::from_secs(30);
        let mut to_remove = Vec::new();

        for entry in connection_pool.iter() {
            let last_used = entry.value().last_used.read().unwrap();
            let is_healthy = entry.value().is_healthy.load(Ordering::Relaxed);
            
            let should_remove = if is_healthy {
                last_used.elapsed() > stale_threshold
            } else {
                last_used.elapsed() > unhealthy_threshold
            };
            
            if should_remove {
                to_remove.push(*entry.key());
            }
        }

        for addr in to_remove {
            if let Some((_, conn_state)) = connection_pool.remove(&addr) {
                if let Some(endpoint) = conn_state.endpoint.lock().await.take() {
                    endpoint.close(0u32.into(), b"stale");
                }
            }
        }
    }

    fn update_latency_metrics(&self, latency_ns: u64) {
        let current = self.metrics.avg_latency_ns.load(Ordering::Relaxed);
        let new_avg = if current == 0 {
            latency_ns
        } else {
            (current * 95 + latency_ns * 5) / 100
        };
        self.metrics.avg_latency_ns.store(new_avg, Ordering::Relaxed);
    }

    pub async fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        sleep(Duration::from_millis(100)).await;

        let connections: Vec<_> = self.connection_pool.iter()
            .map(|entry| (*entry.key(), entry.value().clone()))
            .collect();

        for (addr, conn_state) in connections {
            if let Some(endpoint) = conn_state.endpoint.lock().await.take() {
                endpoint.close(0u32.into(), b"shutdown");
            }
            self.connection_pool.remove(&addr);
        }

        self.active_connections.write().await.clear();
    }

    pub fn get_metrics(&self) -> (u64, u64, u64, u64) {
        (
            self.metrics.total_sent.load(Ordering::Relaxed),
            self.metrics.total_confirmed.load(Ordering::Relaxed),
            self.metrics.total_failed.load(Ordering::Relaxed),
            self.metrics.avg_latency_ns.load(Ordering::Relaxed),
        )
    }

    pub fn get_connection_count(&self) -> usize {
        self.connection_pool.len()
    }

    pub fn get_healthy_connection_count(&self) -> usize {
        self.connection_pool.iter()
            .filter(|entry| entry.value().is_healthy.load(Ordering::Relaxed))
            .count()
    }
}

struct PortAllocator {
    available_ports: Arc<Mutex<VecDeque<u16>>>,
    port_usage: Arc<DashMap<u16, Instant>>,
}

impl PortAllocator {
    fn new() -> Self {
        // Choose an app-specific ephemeral subrange to reduce 4-tuple collisions
        let mut pool = VecDeque::new();
        for p in (PORT_RANGE_START..=PORT_RANGE_END).step_by(1) {
            pool.push_back(p);
        }
        Self {
            available_ports: Arc::new(Mutex::new(pool)),
            port_usage: Arc::new(DashMap::new()),
        }
    }

    async fn allocate_ports(&self, count: usize) -> Vec<u16> {
        let mut out = Vec::with_capacity(count);
        let mut avail = self.available_ports.lock().await;
        for _ in 0..count {
            if let Some(p) = avail.pop_front() {
                self.port_usage.insert(p, Instant::now());
                out.push(p);
            } else {
                break;
            }
        }
        out
    }

    async fn release_port(&self, port: u16) {
        self.port_usage.remove(&port);
        let mut avail = self.available_ports.lock().await;
        avail.push_back(port);
    }

    async fn cleanup_expired_ports(&self) {
        let expiry = Duration::from_secs(300);
        for entry in self.port_usage.iter() {
            if entry.value().elapsed() > expiry {
                let p = *entry.key();
                self.port_usage.remove(&p);
                let mut avail = self.available_ports.lock().await;
                avail.push_back(p);
            }
        }
    }
}

impl HealthMonitor {
    fn new() -> Self {
        Self {
            connection_health: Arc::new(DashMap::new()),
        }
    }

    pub fn get_connection_health(&self, addr: &SocketAddr) -> Option<ConnectionHealth> {
        self.connection_health.get(addr).map(|h| h.clone())
    }

    pub fn get_unhealthy_connections(&self) -> Vec<SocketAddr> {
        self.connection_health.iter()
            .filter(|entry| entry.value().success_rate < 0.5 || entry.value().consecutive_failures > 3)
            .map(|entry| *entry.key())
            .collect()
    }
}

// REPLACE Metrics::new()
impl Metrics {
    fn new() -> Self {
        Self {
            total_sent: AtomicU64::new(0),
            total_confirmed: AtomicU64::new(0),
            total_failed: AtomicU64::new(0),
            avg_latency_ns: AtomicU64::new(0),
            device_stats: DashMap::new(),
            device_limiters: DashMap::new(),
        }
    }
    // existing dev_stats_map()/record_dev() remain

    // ADD: fetch/create the limiter for a device
    fn dev_semaphore(&self, dev: &Arc<str>) -> Arc<Semaphore> {
        let entry = self.device_limiters.entry(dev.clone())
            .or_insert_with(|| Arc::new(Semaphore::new(DEVICE_MAX_INFLIGHT)));
        entry.value().clone()
    }

    // ADD: record TX timestamp results
    fn record_dev_tx(&self, dev: Option<&Arc<str>>, ack_age_ns: Option<u64>, queue_ns: Option<u64>, zc_done: u64) {
        if let Some(d) = dev {
            let entry = self.device_stats.entry(d.clone()).or_insert_with(|| Arc::new(DeviceStats::default()));
            if let Some(a) = ack_age_ns { entry.value().record_tx_ack_age(a); }
            if let Some(q) = queue_ns { entry.value().record_tx_queue_ns(q); }
            if zc_done > 0 { entry.value().record_zerocopy_complete(zc_done); }
        }
    }

    fn dev_stats_map(&self) -> &DashMap<Arc<str>, Arc<DeviceStats>> {
        // Lazy static-style slot on Metrics via once_cell not needed; attach as field
        &self.device_stats
    }

    fn record_dev(&self, dev: Option<&Arc<str>>, bytes: usize, ok: bool, lat_ns: Option<u64>) {
        if let Some(d) = dev {
            let entry = self.device_stats.entry(d.clone()).or_insert_with(|| Arc::new(DeviceStats::default()));
            entry.value().record(bytes, ok, lat_ns);
        }
    }

    pub fn success_rate(&self) -> f64 {
        let sent = self.total_sent.load(Ordering::Relaxed) as f64;
        let confirmed = self.total_confirmed.load(Ordering::Relaxed) as f64;
        if sent > 0.0 {
            confirmed / sent
        } else {
            0.0
        }
    }
}

struct SkipServerVerification;

impl rustls::client::ServerCertVerifier for SkipServerVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::Certificate,
        _intermediates: &[rustls::Certificate],
        _server_name: &rustls::ServerName,
        _scts: &mut dyn Iterator<Item = &[u8]>,
        _ocsp_response: &[u8],
        _now: std::time::SystemTime,
    ) -> Result<rustls::client::ServerCertVerified, rustls::Error> {
        Ok(rustls::client::ServerCertVerified::assertion())
    }
}

impl Drop for TpuPortMonopolizer {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }
}

