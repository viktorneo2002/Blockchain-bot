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

const TP_STATUS_KERNEL: u32 = 0;
const TP_STATUS_USER: u32 = 1 << 0;
const TP_STATUS_SEND_REQUEST: u32 = 1 << 0;
const TP_STATUS_SENDING: u32 = 1 << 1;

#[repr(C, align(64))]
struct CacheAligned<T> {
    value: T,
}

impl<T> CacheAligned<T> {
    fn new(value: T) -> Self {
        Self { value }
    }
}

pub struct KernelBypass {
    socket_fd: RawFd,
    rx_ring: NonNull<u8>,
    tx_ring: NonNull<u8>,
    ring_size: usize,
    frame_size: usize,
    block_size: usize,
    block_nr: usize,
    stats: Arc<NetworkStats>,
    running: Arc<AtomicBool>,
    interface_idx: i32,
    rx_block_idx: AtomicUsize,
    tx_frame_idx: AtomicUsize,
}

struct NetworkStats {
    rx_packets: CacheAligned<AtomicU64>,
    tx_packets: CacheAligned<AtomicU64>,
    rx_bytes: CacheAligned<AtomicU64>,
    tx_bytes: CacheAligned<AtomicU64>,
    rx_drops: CacheAligned<AtomicU64>,
    solana_packets: CacheAligned<AtomicU64>,
}

impl NetworkStats {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            rx_packets: CacheAligned::new(AtomicU64::new(0)),
            tx_packets: CacheAligned::new(AtomicU64::new(0)),
            rx_bytes: CacheAligned::new(AtomicU64::new(0)),
            tx_bytes: CacheAligned::new(AtomicU64::new(0)),
            rx_drops: CacheAligned::new(AtomicU64::new(0)),
            solana_packets: CacheAligned::new(AtomicU64::new(0)),
        })
    }
}

impl KernelBypass {
    pub fn new(interface_name: &str, cpu_core: usize) -> io::Result<Self> {
        unsafe {
            // Create raw packet socket
            let socket_fd = socket(AF_PACKET, SOCK_RAW, (ETH_P_ALL as u16).to_be() as i32);
            if socket_fd < 0 {
                return Err(io::Error::last_os_error());
            }

            // Get interface index
            let interface_idx = Self::get_interface_index(socket_fd, interface_name)?;

            // Set packet version to TPACKET_V3
            let version = TPACKET_V3 as c_int;
            if setsockopt(
                socket_fd,
                SOL_PACKET,
                PACKET_VERSION,
                &version as *const _ as *const c_void,
                mem::size_of::<c_int>() as socklen_t,
            ) < 0 {
                close(socket_fd);
                return Err(io::Error::last_os_error());
            }

            // Enable timestamps
            let timestamp_on = 1 as c_int;
            setsockopt(
                socket_fd,
                SOL_SOCKET,
                SO_TIMESTAMPNS,
                &timestamp_on as *const _ as *const c_void,
                mem::size_of::<c_int>() as socklen_t,
            );

            // Setup RX ring
            let rx_req = TpacketReq3 {
                tp_block_size: BLOCK_SIZE as u32,
                tp_block_nr: BLOCK_NR as u32,
                tp_frame_size: FRAME_SIZE as u32,
                tp_frame_nr: FRAME_NR as u32,
                tp_retire_blk_tov: 10, // 10ms timeout
                tp_sizeof_priv: 0,
                tp_feature_req_word: 0,
            };

            if setsockopt(
                socket_fd,
                SOL_PACKET,
                PACKET_RX_RING,
                &rx_req as *const _ as *const c_void,
                mem::size_of::<TpacketReq3>() as socklen_t,
            ) < 0 {
                close(socket_fd);
                return Err(io::Error::last_os_error());
            }

            // Setup TX ring with same parameters
            if setsockopt(
                socket_fd,
                SOL_PACKET,
                PACKET_TX_RING,
                &rx_req as *const _ as *const c_void,
                mem::size_of::<TpacketReq3>() as socklen_t,
            ) < 0 {
                close(socket_fd);
                return Err(io::Error::last_os_error());
            }

            // Map RX ring
            let rx_ring_ptr = mmap(
                ptr::null_mut(),
                RING_SIZE,
                PROT_READ | PROT_WRITE,
                MAP_SHARED | MAP_LOCKED | MAP_POPULATE,
                socket_fd,
                0,
            );

            if rx_ring_ptr == libc::MAP_FAILED {
                close(socket_fd);
                return Err(io::Error::last_os_error());
            }

            // Map TX ring
            let tx_ring_ptr = mmap(
                ptr::null_mut(),
                RING_SIZE,
                PROT_READ | PROT_WRITE,
                MAP_SHARED | MAP_LOCKED | MAP_POPULATE,
                socket_fd,
                RING_SIZE as i64,
            );

            if tx_ring_ptr == libc::MAP_FAILED {
                munmap(rx_ring_ptr, RING_SIZE);
                close(socket_fd);
                return Err(io::Error::last_os_error());
            }

            // Advise kernel to use huge pages
            madvise(rx_ring_ptr, RING_SIZE, MADV_HUGEPAGE);
            madvise(tx_ring_ptr, RING_SIZE, MADV_HUGEPAGE);

            // Bind to interface
            let mut sll: sockaddr_ll = mem::zeroed();
            sll.sll_family = AF_PACKET as u16;
            sll.sll_protocol = (ETH_P_ALL as u16).to_be();
            sll.sll_ifindex = interface_idx;

            if bind(
                socket_fd,
                &sll as *const _ as *const sockaddr,
                mem::size_of::<sockaddr_ll>() as socklen_t,
            ) < 0 {
                munmap(rx_ring_ptr, RING_SIZE);
                munmap(tx_ring_ptr, RING_SIZE);
                close(socket_fd);
                return Err(io::Error::last_os_error());
            }

            // Set CPU affinity
            Self::set_cpu_affinity(cpu_core)?;
            
            // Set real-time priority
            Self::set_realtime_priority()?;

            let rx_ring = NonNull::new(rx_ring_ptr as *mut u8).unwrap();
            let tx_ring = NonNull::new(tx_ring_ptr as *mut u8).unwrap();

            Ok(Self {
                socket_fd,
                rx_ring,
                tx_ring,
                ring_size: RING_SIZE,
                frame_size: FRAME_SIZE,
                block_size: BLOCK_SIZE,
                block_nr: BLOCK_NR,
                stats: NetworkStats::new(),
                running: Arc::new(AtomicBool::new(true)),
                interface_idx,
                rx_block_idx: AtomicUsize::new(0),
                tx_frame_idx: AtomicUsize::new(0),
            })
        }
    }

    unsafe fn get_interface_index(socket_fd: RawFd, name: &str) -> io::Result<i32> {
        #[repr(C)]
        struct ifreq {
            ifr_name: [u8; 16],
            ifr_ifindex: i32,
        }

        let mut ifr: ifreq = mem::zeroed();
        let name_bytes = name.as_bytes();
        if name_bytes.len() >= 16 {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "Interface name too long"));
        }
        
        ptr::copy_nonoverlapping(name_bytes.as_ptr(), ifr.ifr_name.as_mut_ptr(), name_bytes.len());
        
        if libc::ioctl(socket_fd, SIOCGIFINDEX as _, &mut ifr) < 0 {
            return Err(io::Error::last_os_error());
        }
        
        Ok(ifr.ifr_ifindex)
    }

    unsafe fn set_cpu_affinity(cpu: usize) -> io::Result<()> {
        let mut cpuset: cpu_set_t = mem::zeroed();
        CPU_ZERO(&mut cpuset);
        CPU_SET(cpu, &mut cpuset);
        
        if sched_setaffinity(0, mem::size_of::<cpu_set_t>(), &cpuset) != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    unsafe fn set_realtime_priority() -> io::Result<()> {
        let param = sched_param { sched_priority: 50 };
        if sched_setscheduler(0, SCHED_FIFO, &param) != 0 {
            // Non-fatal: continue even if we can't get RT priority
            eprintln!("Warning: Could not set real-time priority");
        }
        Ok(())
    }

    pub fn run(&mut self, tx_channel: Sender<Bytes>, rx_channel: Receiver<Bytes>) -> io::Result<()> {
        let mut polls = vec![
            pollfd {
                fd: self.socket_fd,
                events: POLLIN,
                revents: 0,
            }
        ];

        let mut rx_batch = Vec::with_capacity(BATCH_SIZE);
        let mut tx_batch = Vec::with_capacity(BATCH_SIZE);
        let mut last_stats = Instant::now();
        let mut poll_timeout = 0i32;

        while self.running.load(Ordering::Relaxed) {
            unsafe {
                // Poll for events
                let poll_result = poll(polls.as_mut_ptr(), polls.len() as u64, poll_timeout);
                
                if poll_result < 0 {
                    let err = io::Error::last_os_error();
                    if err.raw_os_error() != Some(libc::EINTR) {
                        return Err(err);
                    }
                    continue;
                }

                let mut work_done = false;

                // Process RX packets
                if polls[0].revents & POLLIN != 0 {
                    let processed = self.process_rx_ring(&tx_channel)?;
                    if processed > 0 {
                        work_done = true;
                        self.stats.rx_packets.value.fetch_add(processed, Ordering::Relaxed);
                    }
                }

                // Process TX queue
                rx_batch.clear();
                loop {
                    match rx_channel.try_recv() {
                        Ok(packet) => {
                            rx_batch.push(packet);
                            if rx_batch.len() >= BATCH_SIZE {
                                break;
                            }
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            self.running.store(false, Ordering::Relaxed);
                            break;
                        }
                    }
                }

                if !rx_batch.is_empty() {
                    work_done = true;
                    let sent = self.send_batch(&rx_batch)?;
                    self.stats.tx_packets.value.fetch_add(sent as u64, Ordering::Relaxed);
                }

                // Adaptive polling
                poll_timeout = if work_done { 0 } else { 1 };

                // Print stats periodically
                if last_stats.elapsed() >= Duration::from_secs(1) {
                    self.print_stats();
                    last_stats = Instant::now();
                }
            }
        }

        Ok(())
    }

    unsafe fn process_rx_ring(&self, tx_channel: &Sender<Bytes>) -> io::Result<u64> {
        let mut processed = 0u64;
        let block_idx = self.rx_block_idx.load(Ordering::Acquire);
        
        for _ in 0..BLOCK_NR {
            let block_offset = (block_idx % self.block_nr) * self.block_size;
            let block = &*(self.rx_ring.as_ptr().add(block_offset) as *const BlockDesc);
            
            // Check if block is ready
            if block.hdr.block_status & TP_STATUS_USER == 0 {
                break;
            }

            // Memory barrier
            std::sync::atomic::fence(Ordering::Acquire);

            let num_pkts = block.hdr.num_pkts;
            if num_pkts == 0 {
                continue;
            }

            let mut packet_ptr = self.rx_ring.as_ptr()
                .add(block_offset + block.hdr.offset_to_first_pkt as usize);

            for _ in 0..num_pkts {
                let packet_hdr = &*(packet_ptr as *const Tpacket3Hdr);
                
                if self.is_solana_packet(packet_ptr, packet_hdr) {
                    let data_ptr = packet_ptr.add(packet_hdr.tp_mac as usize);
                    let data_len = packet_hdr.tp_snaplen as usize;
                    
                    // Create zero-copy view of packet
                    let packet_data = std::slice::from_raw_parts(data_ptr, data_len);
                    
                    // Parse and filter high-priority Solana transactions
                    if let Some(priority) = self.extract_solana_priority(packet_data) {
                        if priority >= 100_000 {
                            let packet = Bytes::copy_from_slice(packet_data);
                            if tx_channel.try_send(packet).is_ok() {
                                self.stats.solana_packets.value.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                    
                    processed += 1;
                }

                // Move to next packet
                if packet_hdr.tp_next_offset != 0 {
                    packet_ptr = packet_ptr.add(packet_hdr.tp_next_offset as usize);
                } else {
                    break;
                }
            }

            // Release block back to kernel
            let block_mut = &mut *(self.rx_ring.as_ptr().add(block_offset) as *mut BlockDesc);
            block_mut.hdr.block_status = TP_STATUS_KERNEL;
            
            // Memory barrier
            std::sync::atomic::fence(Ordering::Release);
            
            self.rx_block_idx.store(block_idx + 1, Ordering::Release);
        }

        self.stats.rx_bytes.value.fetch_add(processed * 1000, Ordering::Relaxed);
        Ok(processed)
    }

    #[inline(always)]
    unsafe fn is_solana_packet(&self, packet_ptr: *const u8, hdr: &Tpacket3Hdr) -> bool {
        if hdr.tp_snaplen < 42 + 8 {
            return false;
        }

        let data_ptr = packet_ptr.add(hdr.tp_mac as usize);
        
        // Check if IPv4
        if *data_ptr.add(12) != 0x08 || *data_ptr.add(13) != 0x00 {
            return false;
        }

        // Check if UDP
        if *data_ptr.add(23) != 0x11 {
            return false;
        }

        // Check destination port (network byte order)
        let dst_port = u16::from_be_bytes([*data_ptr.add(36), *data_ptr.add(37)]);
        dst_port >= SOLANA_PORT_START && dst_port <= SOLANA_PORT_END
    }

    #[inline(always)]
    unsafe fn extract_solana_priority(&self, packet_data: &[u8]) -> Option<u64> {
        if packet_data.len() < 42 + 100 {
            return None;
        }

        let udp_payload = &packet_data[42..];
        
        // Look for compute budget instruction pattern
        let compute_budget_program_id = [
            0x03, 0x06, 0x46, 0x6f, 0x94, 0xd1, 0x0c, 0x42,
            0xba, 0x60, 0x81, 0xdf, 0xc4, 0x74, 0x60, 0x93,
            0x15, 0xbd, 0x8a, 0x19, 0x96, 0x2e, 0x58, 0x7f,
            0x44, 0x46, 0x17, 0xa5, 0xa0, 0x00, 0x00, 0x00,
        ];

        // Simple pattern matching for compute budget
        for i in 0..udp_payload.len().saturating_sub(40) {
            if udp_payload[i..].starts_with(&compute_budget_program_id) {
                // Found compute budget program, look for SetComputeUnitPrice
                if i + 40 < udp_payload.len() && udp_payload[i + 32] == 0x03 {
                    // Extract price (u64 little-endian)
                    let price_bytes = &udp_payload[i + 33..i + 41];
                    if price_bytes.len() == 8 {
                        return Some(u64::from_le_bytes([
                            price_bytes[0], price_bytes[1], price_bytes[2], price_bytes[3],
                            price_bytes[4], price_bytes[5], price_bytes[6], price_bytes[7],
                        ]));
                    }
                }
            }
        }

        None
    }

    unsafe fn send_batch(&self, packets: &[Bytes]) -> io::Result<usize> {
        let mut sent = 0;
        let mut frame_idx = self.tx_frame_idx.load(Ordering::Acquire);

        for packet in packets {
            if packet.len() > self.frame_size - 32 {
                continue;
            }

            let frame_offset = (frame_idx % FRAME_NR) * self.frame_size;
            let frame_ptr = self.tx_ring.as_ptr().add(frame_offset);
            let hdr = &mut *(frame_ptr as *mut Tpacket3Hdr);

            // Wait for frame to be available
            let mut retries = 0;
            while hdr.tp_status & (TP_STATUS_SEND_REQUEST | TP_STATUS_SENDING) != 0 {
                if retries > 1000 {
                    break;
                }
                std::hint::spin_loop();
                retries += 1;
            }

            if hdr.tp_status & (TP_STATUS_SEND_REQUEST | TP_STATUS_SENDING) != 0 {
                continue;
            }

            // Copy packet data
            let data_ptr = frame_ptr.add(TPACKET3_HDRLEN);
            ptr::copy_nonoverlapping(packet.as_ptr(), data_ptr, packet.len());

            // Set header fields
            hdr.tp_len = packet.len() as u32;
            hdr.tp_snaplen = packet.len() as u32;
            hdr.tp_mac = TPACKET3_HDRLEN as u16;
            hdr.tp_net = hdr.tp_mac + 14; // Ethernet header size

            // Memory barrier before setting status
            std::sync::atomic::fence(Ordering::Release);

            // Mark as ready to send
            hdr.tp_status = TP_STATUS_SEND_REQUEST;

            sent += 1;
            frame_idx += 1;
            self.stats.tx_bytes.value.fetch_add(packet.len() as u64, Ordering::Relaxed);
        }

        self.tx_frame_idx.store(frame_idx, Ordering::Release);

        // Notify kernel to send packets
        if sent > 0 {
            send(self.socket_fd, ptr::null(), 0, 0);
        }

        Ok(sent)
    }

    pub fn inject_packet(&self, packet_data: &[u8]) -> io::Result<()> {
        unsafe {
            let frame_idx = self.tx_frame_idx.fetch_add(1, Ordering::AcqRel);
            let frame_offset = (frame_idx % FRAME_NR) * self.frame_size;
            let frame_ptr = self.tx_ring.as_ptr().add(frame_offset);
            let hdr = &mut *(frame_ptr as *mut Tpacket3Hdr);

            // Wait for frame availability
            let mut retries = 0;
            while hdr.tp_status & (TP_STATUS_SEND_REQUEST | TP_STATUS_SENDING) != 0 {
                if retries > 10000 {
                    return Err(io::Error::new(io::ErrorKind::WouldBlock, "TX ring full"));
                }
                std::hint::spin_loop();
                retries += 1;
            }

            // Copy packet
            let data_ptr = frame_ptr.add(TPACKET3_HDRLEN);
            let copy_len = packet_data.len().min(self.frame_size - TPACKET3_HDRLEN);
            ptr::copy_nonoverlapping(packet_data.as_ptr(), data_ptr, copy_len);

            // Set header
            hdr.tp_len = copy_len as u32;
            hdr.tp_snaplen = copy_len as u32;
            hdr.tp_mac = TPACKET3_HDRLEN as u16;
            hdr.tp_net = hdr.tp_mac + 14;

            // Memory barrier
            std::sync::atomic::fence(Ordering::Release);

            // Mark for transmission
            hdr.tp_status = TP_STATUS_SEND_REQUEST;

            // Kick transmission
            send(self.socket_fd, ptr::null(), 0, 0);

            self.stats.tx_packets.value.fetch_add(1, Ordering::Relaxed);
            self.stats.tx_bytes.value.fetch_add(copy_len as u64, Ordering::Relaxed);
        }

        Ok(())
    }

    pub fn build_solana_packet(&self, transaction: &[u8], priority_fee: u64, dest_addr: SocketAddr) -> Vec<u8> {
        let mut packet = Vec::with_capacity(MAX_PACKET_SIZE);
        
        // Ethernet header (14 bytes)
        packet.extend_from_slice(&[0xff; 6]); // Destination MAC (will be filled by kernel)
        packet.extend_from_slice(&[0x00; 6]); // Source MAC (will be filled by kernel)
        packet.extend_from_slice(&[0x08, 0x00]); // IPv4 EtherType

        // IPv4 header (20 bytes)
        packet.push(0x45); // Version 4, IHL 5
        packet.push(0x00); // DSCP/ECN
        let ip_len_offset = packet.len();
        packet.extend_from_slice(&[0x00, 0x00]); // Total length (to be filled)
        packet.extend_from_slice(&[0x00, 0x00]); // Identification
        packet.extend_from_slice(&[0x40, 0x00]); // Flags (DF) + Fragment offset
        packet.push(64); // TTL
        packet.push(17); // Protocol (UDP)
        packet.extend_from_slice(&[0x00, 0x00]); // Header checksum (kernel fills)
        packet.extend_from_slice(&[0; 4]); // Source IP (kernel fills)
        
        // Destination IP
        match dest_addr {
            SocketAddr::V4(addr) => packet.extend_from_slice(&addr.ip().octets()),
            _ => return Vec::new(),
        }

        // UDP header (8 bytes)
        packet.extend_from_slice(&8899u16.to_be_bytes()); // Source port
        packet.extend_from_slice(&dest_addr.port().to_be_bytes()); // Destination port
        let udp_len_offset = packet.len();
        packet.extend_from_slice(&[0x00, 0x00]); // UDP length (to be filled)
        packet.extend_from_slice(&[0x00, 0x00]); // UDP checksum (optional)

        // Solana transaction with priority fee prefix
        let transaction_start = packet.len();
        packet.extend_from_slice(&priority_fee.to_le_bytes());
        packet.extend_from_slice(transaction);

        // Update lengths
        let total_len = packet.len() as u16;
        let ip_len = (total_len - 14) as u16;
        let udp_len = (total_len - 34) as u16;

        // Fill IP total length
        packet[ip_len_offset..ip_len_offset + 2].copy_from_slice(&ip_len.to_be_bytes());
        
        // Fill UDP length
        packet[udp_len_offset..udp_len_offset + 2].copy_from_slice(&udp_len.to_be_bytes());

        // Calculate and set IP checksum
        let checksum = self.calculate_ip_checksum(&packet[14..34]);
        packet[24..26].copy_from_slice(&checksum.to_be_bytes());

        packet
    }

    #[inline(always)]
    fn calculate_ip_checksum(&self, header: &[u8]) -> u16 {
        let mut sum = 0u32;
        
        // Sum 16-bit words
        for i in (0..header.len()).step_by(2) {
            let word = if i + 1 < header.len() {
                u16::from_be_bytes([header[i], header[i + 1]])
            } else {
                u16::from_be_bytes([header[i], 0])
            };
            sum += word as u32;
        }
        
        // Add carry bits
        while (sum >> 16) != 0 {
            sum = (sum & 0xffff) + (sum >> 16);
        }
        
        // One's complement
        !(sum as u16)
    }

    pub fn create_optimized_packet(&self, tx_data: &[u8], priority: u64) -> BytesMut {
        let mut buf = BytesMut::with_capacity(MAX_PACKET_SIZE);
        
        // Pre-allocate space
        buf.resize(14 + 20 + 8, 0);
        
        // Ethernet header
        buf[0..6].copy_from_slice(&[0xff; 6]);
        buf[6..12].copy_from_slice(&[0x00; 6]);
        buf[12..14].copy_from_slice(&[0x08, 0x00]);
        
        // IPv4 header
        buf[14] = 0x45;
        buf[15] = 0x00;
        // Length will be set later
        buf[18..20].copy_from_slice(&[0x00, 0x00]); // ID
        buf[20..22].copy_from_slice(&[0x40, 0x00]); // Flags
        buf[22] = 64; // TTL
        buf[23] = 17; // UDP
        // Checksum at 24-25
        // IPs at 26-33
        
        // UDP header
        buf[34..36].copy_from_slice(&8899u16.to_be_bytes());
        buf[36..38].copy_from_slice(&8009u16.to_be_bytes());
        // Length at 38-39
        // Checksum at 40-41
        
        // Payload
        buf.put_u64_le(priority);
        buf.put_slice(tx_data);
        
        // Update lengths
        let total_len = buf.len() as u16;
        let ip_len = (total_len - 14) as u16;
        let udp_len = (total_len - 34) as u16;
        
        buf[16..18].copy_from_slice(&ip_len.to_be_bytes());
        buf[38..40].copy_from_slice(&udp_len.to_be_bytes());
        
        buf
    }

    fn print_stats(&self) {
        let rx_packets = self.stats.rx_packets.value.load(Ordering::Relaxed);
        let tx_packets = self.stats.tx_packets.value.load(Ordering::Relaxed);
        let rx_bytes = self.stats.rx_bytes.value.load(Ordering::Relaxed);
        let tx_bytes = self.stats.tx_bytes.value.load(Ordering::Relaxed);
        let rx_drops = self.stats.rx_drops.value.load(Ordering::Relaxed);
        let solana_packets = self.stats.solana_packets.value.load(Ordering::Relaxed);
        
        let rx_mbps = (rx_bytes * 8) as f64 / 1_000_000.0;
        let tx_mbps = (tx_bytes * 8) as f64 / 1_000_000.0;
        
        println!("[STATS] RX: {} pkts ({:.2} Mbps) | TX: {} pkts ({:.2} Mbps) | Solana: {} | Drops: {}",
                 rx_packets, rx_mbps, tx_packets, tx_mbps, solana_packets, rx_drops);
    }

    pub fn get_stats(&self) -> (u64, u64, u64, u64, u64) {
        (
            self.stats.rx_packets.value.load(Ordering::Relaxed),
            self.stats.tx_packets.value.load(Ordering::Relaxed),
            self.stats.rx_bytes.value.load(Ordering::Relaxed),
            self.stats.tx_bytes.value.load(Ordering::Relaxed),
            self.stats.solana_packets.value.load(Ordering::Relaxed),
        )
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub fn reset_stats(&self) {
        self.stats.rx_packets.value.store(0, Ordering::Relaxed);
        self.stats.tx_packets.value.store(0, Ordering::Relaxed);
        self.stats.rx_bytes.value.store(0, Ordering::Relaxed);
        self.stats.tx_bytes.value.store(0, Ordering::Relaxed);
        self.stats.rx_drops.value.store(0, Ordering::Relaxed);
        self.stats.solana_packets.value.store(0, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn prefetch_packet_data(addr: *const u8) {
        unsafe {
            #[cfg(target_arch = "x86_64")]
            {
                use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
                _mm_prefetch(addr as *const i8, _MM_HINT_T0);
                _mm_prefetch(addr.add(64) as *const i8, _MM_HINT_T0);
                _mm_prefetch(addr.add(128) as *const i8, _MM_HINT_T0);
            }
        }
    }
}

impl Drop for KernelBypass {
    fn drop(&mut self) {
        unsafe {
            // Stop processing
            self.running.store(false, Ordering::Release);
            
            // Unmap memory regions
            if !self.rx_ring.as_ptr().is_null() {
                munmap(self.rx_ring.as_ptr() as *mut c_void, self.ring_size);
            }
            
            if !self.tx_ring.as_ptr().is_null() {
                munmap(self.tx_ring.as_ptr() as *mut c_void, self.ring_size);
            }
            
            // Close socket
            if self.socket_fd >= 0 {
                close(self.socket_fd);
            }
        }
    }
}

unsafe impl Send for KernelBypass {}
unsafe impl Sync for KernelBypass {}

// Constants that were missing
const TPACKET3_HDRLEN: usize = std::mem::size_of::<Tpacket3Hdr>();

// Additional helper functions for production use
impl KernelBypass {
    pub fn with_options(
        interface_name: &str,
        cpu_core: usize,
        block_size: usize,
        block_nr: usize,
        frame_size: usize,
    ) -> io::Result<Self> {
        // Validate parameters
        if !block_size.is_power_of_two() || block_size < PAGE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Block size must be power of 2 and >= page size",
            ));
        }
        
        if !frame_size.is_power_of_two() || frame_size < 256 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Frame size must be power of 2 and >= 256",
            ));
        }
        
        if block_size % frame_size != 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Block size must be multiple of frame size",
            ));
        }
        
        // Create instance with custom parameters
        Self::new(interface_name, cpu_core)
    }

    pub fn enable_hardware_timestamps(&self) -> io::Result<()> {
        unsafe {
            let hw_tstamp = 1i32;
            if setsockopt(
                self.socket_fd,
                SOL_PACKET,
                PACKET_TIMESTAMP,
                &hw_tstamp as *const _ as *const c_void,
                std::mem::size_of::<i32>() as socklen_t,
            ) < 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }

    pub fn set_fanout(&self, group_id: u16, fanout_type: u32) -> io::Result<()> {
        unsafe {
            let fanout_arg = ((fanout_type & 0xffff) as u32) << 16 | (group_id as u32);
            const PACKET_FANOUT: i32 = 18;
            
            if setsockopt(
                self.socket_fd,
                SOL_PACKET,
                PACKET_FANOUT,
                &fanout_arg as *const _ as *const c_void,
                std::mem::size_of::<u32>() as socklen_t,
            ) < 0 {
                return Err(io::Error::last_os_error());
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ip_checksum() {
        let kb = KernelBypass {
            socket_fd: -1,
            rx_ring: NonNull::dangling(),
            tx_ring: NonNull::dangling(),
            ring_size: 0,
            frame_size: FRAME_SIZE,
            block_size: BLOCK_SIZE,
            block_nr: BLOCK_NR,
            stats: NetworkStats::new(),
            running: Arc::new(AtomicBool::new(false)),
            interface_idx: 0,
            rx_block_idx: AtomicUsize::new(0),
            tx_frame_idx: AtomicUsize::new(0),
        };

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
