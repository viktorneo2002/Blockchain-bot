#![no_std]
#![no_main]

use aya_bpf::{
    bindings::{xdp_action, TC_ACT_OK, TC_ACT_SHOT},
    helpers::{bpf_ktime_get_ns, bpf_get_prandom_u32},
    macros::{classifier, map, xdp},
    maps::{HashMap, PerfEventArray, Queue, LruHashMap},
    programs::{TcContext, XdpContext, ProbeContext},
    BpfContext,
};
use aya_log_ebpf::info;
use core::{mem, mem::size_of};
use memoffset::offset_of;

mod bindings {
    pub use aya_bpf::bindings::*;
}

const SOLANA_PORT_START: u16 = 8000;
const SOLANA_PORT_END: u16 = 8020;
const SOLANA_GOSSIP_PORT: u16 = 8001;
const SOLANA_TPU_PORT: u16 = 8003;
const SOLANA_TPU_FWD_PORT: u16 = 8004;
const SOLANA_TPU_VOTE_PORT: u16 = 8005;
const MAX_PACKET_SIZE: usize = 1280;
const SIGNATURE_LEN: usize = 64;
const PUBKEY_LEN: usize = 32;
const BLOCKHASH_LEN: usize = 32;
const MAX_INSTRUCTIONS: usize = 64;
const MAX_ACCOUNTS: usize = 256;
const ETH_P_IP: u16 = 0x0800;
const IPPROTO_UDP: u8 = 17;
const ETH_HLEN: usize = 14;
const IP_HLEN: usize = 20;
const UDP_HLEN: usize = 8;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct EthHdr {
    pub h_dest: [u8; 6],
    pub h_source: [u8; 6],
    pub h_proto: u16,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct IpHdr {
    pub version_ihl: u8,
    pub tos: u8,
    pub tot_len: u16,
    pub id: u16,
    pub frag_off: u16,
    pub ttl: u8,
    pub protocol: u8,
    pub check: u16,
    pub saddr: u32,
    pub daddr: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct UdpHdr {
    pub source: u16,
    pub dest: u16,
    pub len: u16,
    pub check: u16,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PacketEvent {
    pub timestamp: u64,
    pub src_ip: u32,
    pub dst_ip: u32,
    pub src_port: u16,
    pub dst_port: u16,
    pub packet_type: u8,
    pub priority: u8,
    pub mev_type: u8,
    pub flags: u8,
    pub signature: [u8; 64],
    pub program_id: [u8; 32],
    pub accounts_count: u8,
    pub instructions_count: u8,
    pub compute_units: u32,
    pub lamports: u64,
    pub expected_profit: u64,
    pub data_len: u16,
    pub hash: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct FilterStats {
    pub packets_processed: u64,
    pub packets_filtered: u64,
    pub mev_opportunities: u64,
    pub high_value_txs: u64,
    pub defi_interactions: u64,
    pub arbitrage_detected: u64,
    pub sandwich_detected: u64,
    pub liquidations_detected: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct ProgramInfo {
    pub program_type: u8,
    pub priority_boost: u8,
    pub min_lamports: u64,
}

#[map]
static PACKET_EVENTS: PerfEventArray<PacketEvent> = PerfEventArray::with_max_entries(4096, 0);

#[map]
static FILTER_STATS: HashMap<u32, FilterStats> = HashMap::with_max_entries(1, 0);

#[map]
static PRIORITY_QUEUE: Queue<PacketEvent> = Queue::with_max_entries(2048, 0);

#[map]
static KNOWN_DEX_PROGRAMS: HashMap<[u8; 32], ProgramInfo> = HashMap::with_max_entries(128, 0);

#[map]
static TRANSACTION_CACHE: LruHashMap<u64, u64> = LruHashMap::with_max_entries(8192, 0);

const SERUM_V3_PROGRAM: [u8; 32] = [
    0x9a, 0xf0, 0x01, 0xee, 0x78, 0x9f, 0xe5, 0x51, 0x4e, 0xb0, 0xd8, 0xee, 0x8e, 0xba, 0x87,
    0x30, 0x7b, 0x6b, 0x8d, 0x9e, 0xc0, 0x2f, 0x9f, 0xa3, 0x30, 0xe8, 0x3e, 0x10, 0xaf, 0x01,
    0xb9, 0x25
];

const RAYDIUM_AMM_V4: [u8; 32] = [
    0x67, 0x5k, 0xPX, 0x87, 0xnK, 0x7N, 0x4V, 0xvJ, 0xTA, 0x4j, 0x15, 0x4z, 0x6c, 0x3D, 0xB1,
    0x4a, 0x5C, 0xF7, 0x4B, 0x5f, 0x3e, 0xDD, 0x45, 0xA1, 0x96, 0x74, 0x81, 0x8a, 0xE5, 0x0B,
    0x8E, 0xfa
];

const ORCA_WHIRLPOOL: [u8; 32] = [
    0x85, 0xBf, 0xD9, 0x14, 0x7f, 0x16, 0xCa, 0xa8, 0xD7, 0x12, 0xEe, 0x99, 0x30, 0x01, 0x23,
    0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x01,
    0x23, 0x45
];

const OPENBOOK_V2: [u8; 32] = [
    0xC1, 0xAF, 0xA5, 0x67, 0xb5, 0x48, 0x17, 0x24, 0x98, 0xFB, 0x68, 0x1f, 0x4e, 0x49, 0x2d,
    0xaE, 0xC5, 0x93, 0x7F, 0xfE, 0x6A, 0x89, 0x96, 0x36, 0x96, 0x4D, 0x73, 0x4A, 0x8F, 0x01,
    0x23, 0x45
];

#[inline(always)]
fn parse_compact_u16(data: &[u8]) -> Result<(u16, usize), ()> {
    if data.is_empty() {
        return Err(());
    }
    
    let first_byte = data[0];
    if first_byte < 0x80 {
        Ok((first_byte as u16, 1))
    } else if first_byte < 0xFE && data.len() >= 2 {
        let value = ((first_byte & 0x7F) as u16) | ((data[1] as u16) << 7);
        Ok((value, 2))
    } else if first_byte == 0xFE && data.len() >= 3 {
        let value = u16::from_le_bytes([data[1], data[2]]);
        Ok((value, 3))
    } else {
        Err(())
    }
}

#[inline(always)]
unsafe fn ptr_at<T>(ctx: &XdpContext, offset: usize) -> Result<*const T, ()> {
    let start = ctx.data();
    let end = ctx.data_end();
    let len = size_of::<T>();

    if start + offset + len > end {
        return Err(());
    }

    Ok((start + offset) as *const T)
}

#[inline(always)]
unsafe fn ptr_at_tc<T>(ctx: &TcContext, offset: usize) -> Result<*const T, ()> {
    let start = ctx.data();
    let end = ctx.data_end();
    let len = size_of::<T>();

    if start + offset + len > end {
        return Err(());
    }

    Ok((start + offset) as *const T)
}

#[inline(always)]
fn is_dex_program(program_id: &[u8; 32]) -> bool {
    *program_id == SERUM_V3_PROGRAM || 
    *program_id == RAYDIUM_AMM_V4 ||
    *program_id == ORCA_WHIRLPOOL ||
    *program_id == OPENBOOK_V2 ||
    (program_id[0] == 0x67 && program_id[1] == 0x5b) ||
    (program_id[0] == 0x9a && program_id[1] == 0xf0) ||
    (program_id[0] == 0x85 && program_id[1] == 0xBf) ||
    (program_id[0] == 0xC1 && program_id[1] == 0xAF)
}

#[inline(always)]
fn calculate_transaction_hash(signature: &[u8; 64]) -> u64 {
    let mut hash = 0x517cc1b1c2a3e4f5u64;
    
    for i in (0..64).step_by(8) {
        if i + 8 <= 64 {
            let chunk = u64::from_le_bytes([
                signature[i], signature[i+1], signature[i+2], signature[i+3],
                signature[i+4], signature[i+5], signature[i+6], signature[i+7]
            ]);
            hash = hash.wrapping_mul(0x100000001b3).wrapping_add(chunk);
        }
    }
    
    hash
}

#[inline(always)]
fn extract_transaction_info(payload: &[u8]) -> Result<PacketEvent, ()> {
    if payload.len() < 176 {
        return Err(());
    }

    let mut event = unsafe { mem::zeroed::<PacketEvent>() };
    event.compute_units = 200_000;
    
    let mut offset = 0;
    
    let (sig_count, sig_offset) = parse_compact_u16(&payload[offset..]).map_err(|_| ())?;
    offset += sig_offset;
    
    if sig_count == 0 || sig_count > 8 || payload.len() < offset + (sig_count as usize * SIGNATURE_LEN) {
        return Err(());
    }
    
    if offset + SIGNATURE_LEN <= payload.len() {
        for i in 0..SIGNATURE_LEN {
            event.signature[i] = payload[offset + i];
        }
    }
    event.hash = calculate_transaction_hash(&event.signature);
    offset += sig_count as usize * SIGNATURE_LEN;

    if offset + 2 > payload.len() {
        return Err(());
    }
    
    let message_header = payload[offset];
    let num_required_signatures = message_header & 0x0f;
    let num_readonly_signed = (message_header >> 4) & 0x0f;
    offset += 1;

    let num_readonly_unsigned = payload[offset];
    offset += 1;

    let (account_keys_len, keys_offset) = parse_compact_u16(&payload[offset..]).map_err(|_| ())?;
    offset += keys_offset;
    
    if account_keys_len == 0 || account_keys_len > MAX_ACCOUNTS as u16 {
        return Err(());
    }
    
    event.accounts_count = account_keys_len.min(255) as u8;
    
    let account_keys_start = offset;
    let total_keys_size = account_keys_len as usize * PUBKEY_LEN;
    
    if payload.len() < offset + total_keys_size + BLOCKHASH_LEN {
        return Err(());
    }
    offset += total_keys_size;
    offset += BLOCKHASH_LEN;

    let (instructions_len, inst_offset) = parse_compact_u16(&payload[offset..]).map_err(|_| ())?;
    offset += inst_offset;
    
    event.instructions_count = instructions_len.min(255) as u8;

    if instructions_len > 0 && payload.len() > offset + 1 {
        let program_id_index = payload[offset] as usize;
        
        if program_id_index < account_keys_len as usize {
            let key_start = account_keys_start + (program_id_index * PUBKEY_LEN);
            if key_start + PUBKEY_LEN <= payload.len() {
                for i in 0..PUBKEY_LEN {
                    event.program_id[i] = payload[key_start + i];
                }
            }
        }

        offset += 1;
        let (accounts_len, acc_offset) = parse_compact_u16(&payload[offset..]).map_err(|_| ())?;
        offset += acc_offset + accounts_len as usize;

                let (data_len, data_offset) = parse_compact_u16(&payload[offset..]).map_err(|_| ())?;
        event.data_len = data_len;
        offset += data_offset;

        if data_len >= 8 && offset + 8 <= payload.len() {
            event.lamports = u64::from_le_bytes([
                payload[offset], payload[offset + 1], payload[offset + 2], payload[offset + 3],
                payload[offset + 4], payload[offset + 5], payload[offset + 6], payload[offset + 7],
            ]);
        }

        if data_len >= 12 && offset + 12 <= payload.len() {
            let potential_cu = u32::from_le_bytes([
                payload[offset + 8], payload[offset + 9], 
                payload[offset + 10], payload[offset + 11],
            ]);
            if potential_cu > 0 && potential_cu <= 1_400_000 {
                event.compute_units = potential_cu;
            }
        }
    }

    if is_dex_program(&event.program_id) {
        event.packet_type = 2;
        event.priority = 10;
    } else if is_high_value_program(&event.program_id) {
        event.packet_type = 1;
        event.priority = 8;
    } else {
        event.packet_type = 0;
        event.priority = 5;
    }

    Ok(event)
}

#[inline(always)]
fn is_high_value_program(program_id: &[u8; 32]) -> bool {
    const HIGH_VALUE_PREFIXES: [[u8; 2]; 8] = [
        [0x11, 0x11], // System Program
        [0x06, 0xdf], // Token Program
        [0xfc, 0x28], // Metaplex
        [0x4a, 0x67], // Lending
        [0x87, 0x62], // Staking
        [0x22, 0xd6], // Governance
        [0xaa, 0xaa], // Oracle
        [0x8c, 0x97], // Margin
    ];
    
    for prefix in &HIGH_VALUE_PREFIXES {
        if program_id[0] == prefix[0] && program_id[1] == prefix[1] {
            return true;
        }
    }
    false
}

#[inline(always)]
fn calculate_priority(event: &PacketEvent) -> u8 {
    let mut priority = event.priority;
    
    if event.lamports > 10_000_000_000 {
        priority = priority.saturating_add(6);
    } else if event.lamports > 1_000_000_000 {
        priority = priority.saturating_add(4);
    } else if event.lamports > 100_000_000 {
        priority = priority.saturating_add(2);
    } else if event.lamports > 10_000_000 {
        priority = priority.saturating_add(1);
    }
    
    if event.compute_units > 800_000 {
        priority = priority.saturating_add(3);
    } else if event.compute_units > 400_000 {
        priority = priority.saturating_add(2);
    } else if event.compute_units > 200_000 {
        priority = priority.saturating_add(1);
    }
    
    if event.accounts_count > 15 {
        priority = priority.saturating_add(2);
    } else if event.accounts_count > 10 {
        priority = priority.saturating_add(1);
    }
    
    if event.instructions_count > 5 {
        priority = priority.saturating_add(1);
    }
    
    priority.min(15)
}

#[inline(always)]
fn is_arbitrage_pattern(event: &PacketEvent) -> bool {
    if event.accounts_count < 6 || event.instructions_count < 2 {
        return false;
    }
    
    let is_dex = is_dex_program(&event.program_id);
    let has_high_cu = event.compute_units > 300_000;
    let has_value = event.lamports > 50_000_000;
    let multi_instruction = event.instructions_count >= 2;
    
    is_dex && has_high_cu && has_value && multi_instruction
}

#[inline(always)]
fn detect_sandwich_attack(event: &PacketEvent) -> bool {
    if !is_dex_program(&event.program_id) {
        return false;
    }
    
    let sig_pattern = (event.signature[0] ^ event.signature[1]) & 0x0F;
    let high_priority = event.priority >= 8;
    let large_value = event.lamports > 100_000_000;
    let high_compute = event.compute_units > 250_000;
    
    sig_pattern < 4 && high_priority && large_value && high_compute
}

#[inline(always)]
fn detect_liquidation(event: &PacketEvent) -> bool {
    const LIQUIDATION_PATTERNS: [[u8; 4]; 4] = [
        [0x4a, 0x67, 0xb2, 0x34],
        [0xfc, 0x28, 0x9a, 0x12],
        [0x87, 0x62, 0x11, 0xaa],
        [0x22, 0xd6, 0x33, 0x44],
    ];
    
    for pattern in &LIQUIDATION_PATTERNS {
        if event.program_id[0] == pattern[0] && event.program_id[1] == pattern[1] {
            return event.lamports > 10_000_000 && event.compute_units > 300_000;
        }
    }
    
    let has_liquidation_signature = event.data_len > 16 && 
                                   event.accounts_count > 8 &&
                                   event.compute_units > 400_000;
    
    has_liquidation_signature && event.lamports > 50_000_000
}

#[inline(always)]
fn calculate_expected_profit(event: &PacketEvent) -> u64 {
    let base_profit = event.lamports.saturating_div(1000);
    let priority_bonus = (event.priority as u64).saturating_mul(10_000_000);
    
    let mev_multiplier = match event.mev_type {
        3 => 8, // Sandwich
        2 => 5, // Arbitrage
        4 => 4, // Liquidation
        1 => 2, // DEX Trade
        _ => 1,
    };
    
    let compute_bonus = if event.compute_units > 600_000 {
        50_000_000
    } else if event.compute_units > 400_000 {
        20_000_000
    } else {
        5_000_000
    };
    
    base_profit.saturating_mul(mev_multiplier)
        .saturating_add(priority_bonus)
        .saturating_add(compute_bonus)
}

#[inline(always)]
fn classify_mev_type(event: &mut PacketEvent) {
    if detect_sandwich_attack(event) {
        event.mev_type = 3;
    } else if is_arbitrage_pattern(event) {
        event.mev_type = 2;
    } else if detect_liquidation(event) {
        event.mev_type = 4;
    } else if is_dex_program(&event.program_id) && event.lamports > 100_000_000 {
        event.mev_type = 1;
    } else {
        event.mev_type = 0;
    }
}

#[inline(always)]
fn detect_jito_bundle(payload: &[u8]) -> bool {
    if payload.len() < 256 {
        return false;
    }
    
    const JITO_TIP_ACCOUNTS: [[u8; 4]; 4] = [
        [0x0b, 0x00, 0x00, 0x00],
        [0x96, 0xbb, 0x61, 0xfa],
        [0xDT, 0xTC, 0x1z, 0xJT],
        [0x3A, 0xWR, 0xB2, 0xBT],
    ];
    
    let mut tip_indicators = 0u8;
    
    for pattern in &JITO_TIP_ACCOUNTS {
        for window in payload.windows(4) {
            if window == pattern {
                tip_indicators += 1;
                break;
            }
        }
    }
    
    tip_indicators >= 1
}

#[inline(always)]
fn should_frontrun(event: &PacketEvent) -> bool {
    if event.priority < 10 || event.mev_type == 0 {
        return false;
    }
    
    let is_valuable = event.lamports > 500_000_000;
    let is_dex_trade = is_dex_program(&event.program_id);
    let has_arbitrage = event.mev_type == 2;
    let is_sandwich = event.mev_type == 3;
    let high_profit = event.expected_profit > 100_000_000;
    
    (is_valuable && is_dex_trade) || has_arbitrage || is_sandwich || high_profit
}

#[inline(always)]
fn update_stats(event: &PacketEvent) {
    let stats_key = 0u32;
    unsafe {
        if let Some(stats) = FILTER_STATS.get_ptr_mut(&stats_key) {
            (*stats).packets_processed = (*stats).packets_processed.wrapping_add(1);
            
            if event.priority >= 8 {
                (*stats).packets_filtered = (*stats).packets_filtered.wrapping_add(1);
            }
            
            if event.mev_type > 0 {
                (*stats).mev_opportunities = (*stats).mev_opportunities.wrapping_add(1);
            }
            
            if event.lamports > 100_000_000 {
                (*stats).high_value_txs = (*stats).high_value_txs.wrapping_add(1);
            }
            
            if is_dex_program(&event.program_id) {
                (*stats).defi_interactions = (*stats).defi_interactions.wrapping_add(1);
            }
            
            match event.mev_type {
                2 => (*stats).arbitrage_detected = (*stats).arbitrage_detected.wrapping_add(1),
                3 => (*stats).sandwich_detected = (*stats).sandwich_detected.wrapping_add(1),
                4 => (*stats).liquidations_detected = (*stats).liquidations_detected.wrapping_add(1),
                _ => {}
            }
        }
    }
}

#[xdp]
pub fn solana_packet_filter(ctx: XdpContext) -> u32 {
    match process_packet(&ctx) {
        Ok(ret) => ret,
        Err(_) => xdp_action::XDP_PASS,
    }
}

#[classifier]
pub fn solana_tc_filter(ctx: TcContext) -> i32 {
    match process_tc_packet(&ctx) {
        Ok(ret) => ret,
        Err(_) => TC_ACT_OK,
    }
}

#[inline(always)]
fn process_packet(ctx: &XdpContext) -> Result<u32, ()> {
    let eth_hdr: *const EthHdr = unsafe { ptr_at(&ctx, 0) }?;
    
    if unsafe { (*eth_hdr).h_proto } != u16::from_be(ETH_P_IP) {
        return Ok(xdp_action::XDP_PASS);
    }

    let ip_hdr: *const IpHdr = unsafe { ptr_at(&ctx, ETH_HLEN) }?;
    
    if unsafe { (*ip_hdr).protocol } != IPPROTO_UDP {
        return Ok(xdp_action::XDP_PASS);
    }

    let udp_hdr: *const UdpHdr = unsafe { ptr_at(&ctx, ETH_HLEN + IP_HLEN) }?;
    
    let dst_port = u16::from_be(unsafe { (*udp_hdr).dest });
    let src_port = u16::from_be(unsafe { (*udp_hdr).source });

    if dst_port < SOLANA_PORT_START || dst_port > SOLANA_PORT_END {
        return Ok(xdp_action::XDP_PASS);
    }

    let payload_offset = ETH_HLEN + IP_HLEN + UDP_HLEN;
    let payload_len = (u16::from_be(unsafe { (*udp_hdr).len }) as usize).saturating_sub(UDP_HLEN);
    
    if payload_len < 176 || payload_len > MAX_PACKET_SIZE {
        return Ok(xdp_action::XDP_PASS);
    }

    let data_end = ctx.data_end();
    let data = ctx.data();
    
    if data + payload_offset + payload_len > data_end {
        return Ok(xdp_action::XDP_PASS);
    }

    let payload = unsafe {
        core::slice::from_raw_parts(
            (data + payload_offset) as *const u8,
            payload_len.min(MAX_PACKET_SIZE)
        )
    };

    match extract_transaction_info(payload) {
        Ok(mut event) => {
            event.timestamp = unsafe { bpf_ktime_get_ns() };
            event.src_ip = u32::from_be(unsafe { (*ip_hdr).saddr });
            event.dst_ip = u32::from_be(unsafe { (*ip_hdr).daddr });
            event.src_port = src_port;
            event.dst_port = dst_port;
            
            classify_mev_type(&mut event);
            event.priority = calculate_priority(&event);
            event.expected_profit = calculate_expected_profit(&event);
            
            if detect_jito_bundle(payload) {
                event.flags |= 0x01;
                event.priority = event.priority.saturating_add(3).min(15);
            }
            
            let cache_hit = unsafe {
                TRANSACTION_CACHE.get(&event.hash).is_some()
            };
            
            if cache_hit {
                return Ok(xdp_action::XDP_DROP);
            }
            
            unsafe {
                let _ = TRANSACTION_CACHE.insert(&event.hash, &event.timestamp, 0);
            }
            
            update_stats(&event);

            if event.priority >= 8 || event.mev_type > 0 {
                unsafe {
                    if event.priority >= 12 {
                                                let _ = PRIORITY_QUEUE.push(&event, 0);
                    }
                    PACKET_EVENTS.output(&ctx, &event, 0);
                }
            }
        }
        Err(_) => {}
    }

    Ok(xdp_action::XDP_PASS)
}

#[inline(always)]
fn process_tc_packet(ctx: &TcContext) -> Result<i32, ()> {
    let eth_hdr: *const EthHdr = unsafe { ptr_at_tc(&ctx, 0) }?;
    
    if unsafe { (*eth_hdr).h_proto } != u16::from_be(ETH_P_IP) {
        return Ok(TC_ACT_OK);
    }

    let ip_hdr: *const IpHdr = unsafe { ptr_at_tc(&ctx, ETH_HLEN) }?;
    
    if unsafe { (*ip_hdr).protocol } != IPPROTO_UDP {
        return Ok(TC_ACT_OK);
    }

    let udp_hdr: *const UdpHdr = unsafe { ptr_at_tc(&ctx, ETH_HLEN + IP_HLEN) }?;
    
    let src_port = u16::from_be(unsafe { (*udp_hdr).source });
    let dst_port = u16::from_be(unsafe { (*udp_hdr).dest });

    let is_outbound = src_port >= SOLANA_PORT_START && src_port <= SOLANA_PORT_END;
    let is_inbound = dst_port >= SOLANA_PORT_START && dst_port <= SOLANA_PORT_END;

    if !is_outbound && !is_inbound {
        return Ok(TC_ACT_OK);
    }

    let payload_offset = ETH_HLEN + IP_HLEN + UDP_HLEN;
    let payload_len = (u16::from_be(unsafe { (*udp_hdr).len }) as usize).saturating_sub(UDP_HLEN);
    
    if payload_len < 176 || payload_len > MAX_PACKET_SIZE {
        return Ok(TC_ACT_OK);
    }

    let data_end = ctx.data_end();
    let data = ctx.data();
    
    if data + payload_offset + payload_len > data_end {
        return Ok(TC_ACT_OK);
    }

    let payload = unsafe {
        core::slice::from_raw_parts(
            (data + payload_offset) as *const u8,
            payload_len.min(MAX_PACKET_SIZE)
        )
    };

    match extract_transaction_info(payload) {
        Ok(mut event) => {
            event.timestamp = unsafe { bpf_ktime_get_ns() };
            event.src_ip = u32::from_be(unsafe { (*ip_hdr).saddr });
            event.dst_ip = u32::from_be(unsafe { (*ip_hdr).daddr });
            event.src_port = src_port;
            event.dst_port = dst_port;
            
            classify_mev_type(&mut event);
            event.priority = calculate_priority(&event);
            event.expected_profit = calculate_expected_profit(&event);
            
            if detect_jito_bundle(payload) {
                event.flags |= 0x01;
                event.priority = event.priority.saturating_add(3).min(15);
            }
            
            // Outbound MEV protection - drop high priority competing transactions
            if is_outbound && event.priority >= 12 && should_frontrun(&event) {
                return Ok(TC_ACT_SHOT);
            }
            
            // Smart routing for inbound high-value transactions
            if is_inbound && event.mev_type > 0 {
                event.flags |= 0x02; // Mark for special handling
            }
            
            let cache_hit = unsafe {
                TRANSACTION_CACHE.get(&event.hash).is_some()
            };
            
            if cache_hit && is_outbound {
                return Ok(TC_ACT_SHOT);
            }
            
            unsafe {
                let _ = TRANSACTION_CACHE.insert(&event.hash, &event.timestamp, 0);
            }
            
            update_stats(&event);

            if event.priority >= 8 || event.mev_type > 0 {
                unsafe {
                    if event.priority >= 13 {
                        let _ = PRIORITY_QUEUE.push(&event, 0);
                    }
                    PACKET_EVENTS.output(&ctx, &event, 0);
                }
            }
        }
        Err(_) => {}
    }

    Ok(TC_ACT_OK)
}

#[inline(always)]
fn validate_solana_transaction(payload: &[u8]) -> bool {
    if payload.len() < 176 || payload.len() > MAX_PACKET_SIZE {
        return false;
    }
    
    let (sig_count, sig_offset) = match parse_compact_u16(payload) {
        Ok(v) => v,
        Err(_) => return false,
    };
    
    if sig_count == 0 || sig_count > 8 {
        return false;
    }
    
    let min_size = sig_offset + (sig_count as usize * SIGNATURE_LEN) + 3 + BLOCKHASH_LEN + 1;
    payload.len() >= min_size
}

#[inline(always)]
fn extract_compute_budget(payload: &[u8], start_offset: usize) -> u32 {
    const COMPUTE_BUDGET_PROGRAM: [u8; 32] = [
        0x03, 0x06, 0x46, 0x6f, 0xe5, 0x21, 0x17, 0x32, 0xff, 0xec, 0xad, 0xba, 0x72, 0xc3, 0x9b,
        0xe7, 0xbc, 0x8c, 0xe5, 0xbb, 0xc5, 0xf7, 0x12, 0x6b, 0x2c, 0x43, 0x9b, 0x3a, 0x40, 0x00,
        0x00, 0x00
    ];
    
    let mut compute_units = 200_000u32;
    let mut offset = start_offset;
    let max_search = offset + 512;
    
    while offset + 36 < payload.len() && offset < max_search {
        let mut matches = true;
        
        for i in 0..32 {
            if offset + i >= payload.len() || payload[offset + i] != COMPUTE_BUDGET_PROGRAM[i] {
                matches = false;
                break;
            }
        }
        
        if matches && offset + 36 < payload.len() {
            let instruction_type = payload[offset + 32];
            
            // SetComputeUnitLimit instruction
            if instruction_type == 0x02 && offset + 36 < payload.len() {
                compute_units = u32::from_le_bytes([
                    payload[offset + 33],
                    payload[offset + 34],
                    payload[offset + 35],
                    payload[offset + 36],
                ]);
                break;
            }
        }
        
        offset += 1;
    }
    
    compute_units.min(1_400_000)
}

#[inline(always)]
fn calculate_mev_score(event: &PacketEvent) -> u16 {
    let mut score = 0u16;
    
    // Value-based scoring
    let value_score = match event.lamports {
        v if v > 10_000_000_000 => 1500,
        v if v > 1_000_000_000 => 800,
        v if v > 100_000_000 => 400,
        v if v > 10_000_000 => 200,
        v if v > 1_000_000 => 100,
        _ => 10,
    };
    score = score.saturating_add(value_score);
    
    // Compute units scoring
    let compute_score = match event.compute_units {
        c if c > 800_000 => 500,
        c if c > 400_000 => 300,
        c if c > 200_000 => 150,
        c if c > 100_000 => 75,
        _ => 25,
    };
    score = score.saturating_add(compute_score);
    
    // Program type scoring
    if is_dex_program(&event.program_id) {
        score = score.saturating_add(1000);
    } else if is_high_value_program(&event.program_id) {
        score = score.saturating_add(500);
    }
    
    // MEV type scoring
    let mev_score = match event.mev_type {
        3 => 2000, // Sandwich
        2 => 1500, // Arbitrage
        4 => 1200, // Liquidation
        1 => 800,  // DEX Trade
        _ => 0,
    };
    score = score.saturating_add(mev_score);
    
    // Account complexity scoring
    let account_score = (event.accounts_count as u16).saturating_mul(20);
    score = score.saturating_add(account_score);
    
    // Instruction complexity scoring
    let instruction_score = (event.instructions_count as u16).saturating_mul(50);
    score = score.saturating_add(instruction_score);
    
    // Priority scoring
    let priority_score = (event.priority as u16).saturating_mul(100);
    score = score.saturating_add(priority_score);
    
    // Jito bundle bonus
    if event.flags & 0x01 != 0 {
        score = score.saturating_add(500);
    }
    
    score.min(10000)
}

#[inline(always)]
fn estimate_gas_priority_fee(event: &PacketEvent) -> u64 {
    let base_fee = 5000u64;
    let priority_multiplier = (event.priority as u64).saturating_mul(1000);
    let compute_factor = (event.compute_units as u64).saturating_div(1000);
    let value_factor = event.lamports.saturating_div(100_000_000).min(100);
    
    let mev_multiplier = match event.mev_type {
        3 => 5000, // Sandwich attacks pay premium
        2 => 3000, // Arbitrage needs speed
        4 => 2500, // Liquidations are time-sensitive
        1 => 1500, // DEX trades
        _ => 500,
    };
    
    base_fee
        .saturating_add(priority_multiplier)
        .saturating_add(compute_factor)
        .saturating_add(value_factor.saturating_mul(500))
        .saturating_add(mev_multiplier)
}

#[inline(always)]
fn check_slippage_vulnerability(event: &PacketEvent) -> bool {
    if !is_dex_program(&event.program_id) {
        return false;
    }
    
    let high_value = event.lamports > 1_000_000_000;
    let many_accounts = event.accounts_count > 8;
    let high_compute = event.compute_units > 300_000;
    let no_jito = event.flags & 0x01 == 0;
    
    high_value && many_accounts && high_compute && no_jito
}

#[inline(always)]
fn should_relay_immediately(event: &PacketEvent) -> bool {
    let mev_score = calculate_mev_score(event);
    let has_opportunity = event.mev_type > 0;
    let high_priority = event.priority >= 13;
    let critical_score = mev_score > 7000;
    let is_jito = event.flags & 0x01 != 0;
    
    (has_opportunity && high_priority) || critical_score || is_jito
}

#[inline(always)]
fn validate_signature(signature: &[u8; 64]) -> bool {
    let mut non_zero_count = 0u8;
    let mut pattern_sum = 0u32;
    
    for i in 0..64 {
        if signature[i] != 0 {
            non_zero_count += 1;
            pattern_sum = pattern_sum.wrapping_add(signature[i] as u32);
        }
    }
    
    // Valid Ed25519 signatures should have most bytes non-zero
    non_zero_count >= 48 && pattern_sum > 2000
}

#[inline(always)]
fn detect_token_operations(event: &PacketEvent) -> bool {
    const TOKEN_PROGRAM: [u8; 32] = [
        0x06, 0xdf, 0xf6, 0xe1, 0xd7, 0x65, 0xa1, 0x93, 0xd9, 0xcb, 0xe1, 0x46, 0xce, 0xeb, 0x79,
        0xac, 0x1c, 0xb4, 0x85, 0xed, 0x5f, 0x5b, 0x37, 0x91, 0x3a, 0x8c, 0xf5, 0x85, 0x7e, 0xff,
        0x00, 0xa9
    ];
    
    const TOKEN_2022_PROGRAM: [u8; 32] = [
        0x06, 0xa7, 0xd5, 0x17, 0x18, 0x7b, 0xd8, 0x4c, 0x7a, 0x4c, 0x43, 0x02, 0xf5, 0x96, 0x2c,
        0xcd, 0xae, 0x07, 0x47, 0x04, 0x59, 0x60, 0x45, 0x5c, 0xe5, 0x36, 0x77, 0x8a, 0x8b, 0x6f,
        0x70, 0x00
    ];
    
    event.program_id == TOKEN_PROGRAM || 
    event.program_id == TOKEN_2022_PROGRAM ||
    (event.program_id[0] == 0x06 && event.accounts_count >= 3)
}

#[inline(always)]
fn finalize_event_classification(event: &mut PacketEvent) {
    // Recalculate final priority based on all factors
    if should_frontrun(event) {
        event.priority = 15;
                event.flags |= 0x04; // Mark as frontrunnable
    }
    
    if check_slippage_vulnerability(event) {
        event.flags |= 0x08; // Mark as vulnerable to slippage
    }
    
    if detect_token_operations(event) {
        event.flags |= 0x10; // Mark as token operation
    }
    
    if should_relay_immediately(event) {
        event.flags |= 0x20; // Mark for immediate relay
    }
    
    // Final score calculation
    let final_score = calculate_mev_score(event);
    if final_score > 8000 {
        event.priority = 15;
        event.flags |= 0x40; // Mark as ultra-high priority
    }
    
    // Set expected profit
    event.expected_profit = calculate_expected_profit(event);
}

#[inline(always)]
fn validate_program_id(program_id: &[u8; 32]) -> bool {
    // Check if not all zeros
    let mut non_zero = false;
    for &byte in program_id.iter() {
        if byte != 0 {
            non_zero = true;
            break;
        }
    }
    
    if !non_zero {
        return false;
    }
    
    // Check if not all ones (invalid)
    let mut all_ones = true;
    for &byte in program_id.iter() {
        if byte != 0xFF {
            all_ones = false;
            break;
        }
    }
    
    !all_ones
}

#[inline(always)]
fn estimate_latency_advantage(event: &PacketEvent) -> u64 {
    const BASE_LATENCY: u64 = 2000; // microseconds
    
    let priority_reduction = (event.priority as u64).saturating_mul(100);
    let compute_penalty = (event.compute_units / 10_000) as u64;
    let size_penalty = (event.data_len as u64).saturating_div(10);
    
    let mev_advantage = match event.mev_type {
        3 => 1000, // Sandwich attacks need lowest latency
        2 => 700,  // Arbitrage is time-sensitive
        4 => 500,  // Liquidations have some buffer
        1 => 300,  // DEX trades
        _ => 0,
    };
    
    BASE_LATENCY
        .saturating_sub(priority_reduction)
        .saturating_sub(mev_advantage)
        .saturating_add(compute_penalty)
        .saturating_add(size_penalty)
}

#[inline(always)]
fn detect_nft_operations(event: &PacketEvent) -> bool {
    const METAPLEX_PREFIXES: [[u8; 4]; 4] = [
        [0xfc, 0x28, 0x9a, 0x12], // Token Metadata
        [0x11, 0x11, 0x11, 0x11], // Candy Machine
        [0xaa, 0xbb, 0xcc, 0xdd], // Auction House
        [0x12, 0x34, 0x56, 0x78], // Gumdrop
    ];
    
    for prefix in &METAPLEX_PREFIXES {
        let mut matches = true;
        for i in 0..4 {
            if event.program_id[i] != prefix[i] {
                matches = false;
                break;
            }
        }
        if matches {
            return true;
        }
    }
    
    false
}

#[inline(always)]
fn is_spam_transaction(event: &PacketEvent) -> bool {
    // Check for spam patterns
    let low_value = event.lamports < 1_000_000;
    let low_compute = event.compute_units < 50_000;
    let few_accounts = event.accounts_count < 3;
    let single_instruction = event.instructions_count == 1;
    
    // Check signature pattern for spam
    let mut sig_sum = 0u16;
    for i in 0..8 {
        sig_sum = sig_sum.wrapping_add(event.signature[i] as u16);
    }
    let suspicious_sig = sig_sum < 100 || sig_sum > 2000;
    
    (low_value && low_compute && few_accounts) || 
    (single_instruction && suspicious_sig && low_value)
}

#[inline(always)]
fn should_drop_packet(event: &PacketEvent) -> bool {
    // Drop conditions
    if is_spam_transaction(event) {
        return true;
    }
    
    // Drop if no valid signature
    if !validate_signature(&event.signature) {
        return true;
    }
    
    // Drop if invalid program ID
    if !validate_program_id(&event.program_id) {
        return true;
    }
    
    // Drop if low priority and no MEV opportunity
    if event.priority < 3 && event.mev_type == 0 && event.lamports < 10_000_000 {
        return true;
    }
    
    false
}

#[inline(always)]
fn estimate_gas_cost(event: &PacketEvent) -> u64 {
    const BASE_TRANSACTION_COST: u64 = 5000;
    const SIGNATURE_COST: u64 = 5000;
    const ACCOUNT_COST: u64 = 100;
    const COMPUTE_UNIT_COST: u64 = 25; // per 1000 CU
    
    let signature_fees = SIGNATURE_COST;
    let account_fees = (event.accounts_count as u64).saturating_mul(ACCOUNT_COST);
    let compute_fees = (event.compute_units as u64)
        .saturating_mul(COMPUTE_UNIT_COST)
        .saturating_div(1000);
    let priority_fees = estimate_gas_priority_fee(event);
    
    BASE_TRANSACTION_COST
        .saturating_add(signature_fees)
        .saturating_add(account_fees)
        .saturating_add(compute_fees)
        .saturating_add(priority_fees)
}

#[inline(always)]
fn detect_flash_loan(event: &PacketEvent) -> bool {
    // Flash loan patterns
    const FLASH_LOAN_PROGRAMS: [[u8; 4]; 3] = [
        [0x87, 0x62, 0x11, 0xaa], // Solend Flash
        [0x4a, 0x67, 0xb2, 0x34], // Port Finance Flash
        [0x22, 0xd6, 0x33, 0x44], // Larix Flash
    ];
    
    for pattern in &FLASH_LOAN_PROGRAMS {
        if event.program_id[0] == pattern[0] && 
           event.program_id[1] == pattern[1] &&
           event.program_id[2] == pattern[2] &&
           event.program_id[3] == pattern[3] {
            return event.lamports > 100_000_000 && 
                   event.compute_units > 400_000 &&
                   event.instructions_count > 3;
        }
    }
    
    // Generic flash loan detection
    let high_value = event.lamports > 1_000_000_000;
    let complex_tx = event.instructions_count > 5 && event.accounts_count > 10;
    let high_compute = event.compute_units > 600_000;
    
    high_value && complex_tx && high_compute
}

#[inline(always)]
fn optimize_packet_routing(event: &PacketEvent) -> u8 {
    // Return routing decision: 0=normal, 1=priority, 2=ultra-priority, 3=dedicated
    
    if event.flags & 0x40 != 0 { // Ultra-high priority flag
        return 3;
    }
    
    if event.mev_type == 3 || event.expected_profit > 1_000_000_000 {
        return 3; // Dedicated path for sandwich attacks and high profit
    }
    
    if event.priority >= 13 || event.mev_type == 2 {
        return 2; // Ultra-priority for arbitrage
    }
    
    if event.priority >= 10 || event.mev_type > 0 {
        return 1; // Priority path
    }
    
    0 // Normal path
}

#[inline(always)]
fn calculate_transaction_score(event: &PacketEvent) -> u32 {
    let mev_score = calculate_mev_score(event) as u32;
    let profit_score = (event.expected_profit / 1_000_000).min(10000) as u32;
    let priority_score = (event.priority as u32).saturating_mul(1000);
    let latency_score = (10000u64.saturating_sub(estimate_latency_advantage(event))).min(10000) as u32;
    
    let routing_multiplier = match optimize_packet_routing(event) {
        3 => 4,
        2 => 3,
        1 => 2,
        _ => 1,
    };
    
    (mev_score + profit_score + priority_score + latency_score)
        .saturating_mul(routing_multiplier)
        .min(100000)
}

#[inline(always)]
fn should_cache_transaction(event: &PacketEvent) -> bool {
    // Cache high-value transactions for duplicate detection
    event.lamports > 100_000_000 || 
    event.mev_type > 0 || 
    event.priority >= 10 ||
    is_dex_program(&event.program_id)
}

#[inline(always)]
fn get_cache_duration(event: &PacketEvent) -> u64 {
    // Return cache duration in nanoseconds
    match event.mev_type {
        3 => 5_000_000_000,  // 5 seconds for sandwich
        2 => 3_000_000_000,  // 3 seconds for arbitrage
        4 => 10_000_000_000, // 10 seconds for liquidations
        1 => 2_000_000_000,  // 2 seconds for DEX trades
        _ => 1_000_000_000,  // 1 second default
    }
}

#[inline(always)]
fn detect_oracle_update(event: &PacketEvent) -> bool {
    const ORACLE_PROGRAMS: [[u8; 4]; 3] = [
        [0xaa, 0xaa, 0xbb, 0xbb], // Pyth
        [0xcc, 0xcc, 0xdd, 0xdd], // Switchboard
        [0xee, 0xee, 0xff, 0xff], // Chainlink
    ];
    
    for pattern in &ORACLE_PROGRAMS {
        if event.program_id[0] == pattern[0] && event.program_id[1] == pattern[1] {
            return true;
        }
    }
    
    false
}

// Panic handler required for no_std
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    unsafe { core::hint::unreachable_unchecked() }
}

// Required eBPF metadata
#[cfg(not(test))]
#[no_mangle]
#[link_section = "license"]
pub static _license: [u8; 4] = *b"GPL\0";

#[cfg(not(test))]
#[no_mangle]
#[link_section = "version"]
pub static _version: u32 = 0xFFFFFFFE;

#[cfg(not(test))]
#[no_mangle]
#[link_section = "maps"]
pub static _maps: [u8; 0] = [];

