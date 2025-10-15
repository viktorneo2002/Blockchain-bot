// bpf/src/quic_tpu_filter.rs
//! QUIC-aware Solana TPU prefilter for Aya eBPF (hardened)
//! - Header-only QUIC classification (encrypted payloads, no TX parsing).
//! - Long header: version != 0 → QUIC v1..; version == 0 → Version Negotiation (VN) telemetry only.
//! - Initial must be in ≥1200B UDP datagram; coalesced second Initial optionally detected.
//! - VLAN-aware (single 802.1Q) and IPv6-aware UDP parsing.
//! - Optional IPv6 extension-header walker behind feature flag `ipv6_eh` (bounded).
//! - VIP allowlists are POLICY (populated from userspace via real discovery).
//! - IPv4 VIP maps at /32, /24, /16. IPv6 VIP map at /64 prefix.
//! - AF_XDP redirect with queue budgets; perf events are rate-limited.
//! - FLOW_LRU churn tempered; TC path mirrors XDP drop policy.
//!
//! Build: aya-bpf, no_std.

#![no_std]
#![no_main]
#![allow(non_snake_case)]
#![allow(dead_code)]
#![allow(clippy::too_many_arguments)]

use core::mem::size_of;

use aya_bpf::{
    helpers::bpf_ktime_get_ns,
    macros::{classifier, map, xdp},
    maps::{Array, HashMap, LruHashMap, PerCpuArray, PerfEventArray, XskMap},
    programs::{TcContext, XdpContext},
    BpfContext,
};
use aya_log_ebpf::info;

// ===================== Constants =====================

const ETH_P_IP: u16 = 0x0800;    // IPv4
const ETH_P_8021Q: u16 = 0x8100; // VLAN
const ETH_P_IPV6: u16 = 0x86DD;  // IPv6
const IPPROTO_UDP: u8 = 17;

const ETH_HLEN: usize = 14;
const VLAN_HLEN: usize = 4;
const IPV4_HLEN: usize = 20;
const UDP_HLEN: usize = 8;

const MAX_FRAME: usize = 2048;
const MAX_UDP: usize = 1500;
const QUIC_INITIAL_MIN: usize = 1200;
const COALESCE_SECOND_OFF: usize = 1200;

mod xdp_action {
    pub const XDP_ABORTED: u32 = 0;
    pub const XDP_DROP: u32 = 1;
    pub const XDP_PASS: u32 = 2;
    pub const XDP_TX: u32 = 3;
    pub const XDP_REDIRECT: u32 = 7;
}

const TC_ACT_OK: i32 = 0;
const TC_ACT_SHOT: i32 = 2;

// ===================== Headers =====================

#[repr(C)]
#[derive(Clone, Copy)]
struct EthHdr {
    h_dest: [u8; 6],
    h_source: [u8; 6],
    h_proto: u16,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct IpHdr {
    version_ihl: u8,
    tos: u8,
    tot_len: u16,
    id: u16,
    frag_off: u16,
    ttl: u8,
    protocol: u8,
    check: u16,
    saddr: u32,
    daddr: u32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Ipv6Hdr {
    ver_tc_fl: u32, // version(4) + tc(8) + flow label(20)
    payload_len: u16,
    next_header: u8,
    hop_limit: u8,
    saddr: [u8; 16],
    daddr: [u8; 16],
}

#[repr(C)]
#[derive(Clone, Copy)]
struct UdpHdr {
    source: u16,
    dest: u16,
    len: u16,
    check: u16,
}

// ===================== Telemetry event =====================

#[repr(C)]
#[derive(Clone, Copy)]
struct QuicMeta {
    ts_ns: u64,
    src_ip: u32,   // IPv4 host order (or 0 for v6)
    dst_ip: u32,   // IPv4 host order (or 0 for v6)
    src_port: u16, // host order
    dst_port: u16, // host order
    // flags bit layout:
    // bit0: long hdr, bit1: initial, bit2: vip, bit3: gated, bit4: redirected, bit5: ipv6, bit6: version_negotiation
    flags: u8,
    dcid_len: u8,  // 0 on short header
    qid: u8,       // AF_XDP queue
    _pad: u8,
    key64: u64,    // dcid (long) or 5-tuple hash (short/v6)
}

// ===================== Maps =====================

#[repr(C)]
#[derive(Clone, Copy)]
struct Cfg {
    pkt_hi: u32,
    pkt_med: u32,
    qmask: u32,  // AF_XDP queue mask; e.g. 8 queues => 0x7
    policy: u32, // reserved flags
}
#[map]
static CFG: Array<Cfg> = Array::with_max_entries(1, 0);

#[repr(C)]
#[derive(Clone, Copy)]
struct Salt {
    flow: u32,
    ts: u64,
}
#[map]
static FLOW_SALT_PCPU: PerCpuArray<Salt> = PerCpuArray::with_max_entries(1, 0);

// IPv4 VIP allowlists (keys stored BE)
#[map]
static VIP_DST32: HashMap<u32, u8> = HashMap::with_max_entries(2048, 0);
#[map]
static VIP_DST24: HashMap<u32, u8> = HashMap::with_max_entries(2048, 0);
#[map]
static VIP_DST16: HashMap<u32, u8> = HashMap::with_max_entries(1024, 0);

// IPv6 VIP allowlist by /64 prefix (store first 8 bytes as BE u64)
#[map]
static VIP6_DST64: HashMap<u64, u8> = HashMap::with_max_entries(4096, 0);

// LRU flow cache
#[repr(C)]
#[derive(Clone, Copy)]
struct CacheVal { ts: u64, score: u16, _pad: u16 }
#[map]
static FLOW_LRU: LruHashMap<u64, CacheVal> = LruHashMap::with_max_entries(64 * 1024, 0);

// IPv4 per-source token bucket
#[repr(C)]
#[derive(Clone, Copy)]
struct SrcTok { ts_refill: u64, tokens: u16 }
#[map]
static SRC_LIMIT: LruHashMap<u32, SrcTok> = LruHashMap::with_max_entries(32 * 1024, 0);

// AF_XDP queue budgets and health
#[repr(C)]
#[derive(Clone, Copy)]
struct QBudget { last_refill: u64, tokens: u16 }
#[map]
static XSK_QBUDGET: Array<QBudget> = Array::with_max_entries(256, 0);
#[map]
static QHEALTH: Array<u16> = Array::with_max_entries(256, 0);

#[map]
static XSK_SOCKS: XskMap = XskMap::with_max_entries(256, 0);

// perf events + local rate limiter
#[map]
static HOT_HEADERS: PerfEventArray<QuicMeta> = PerfEventArray::with_max_entries(4096, 0);
#[map]
static EVT_RATE: PerCpuArray<u64> = PerCpuArray::with_max_entries(1, 0);

// Slot pressure sampler
#[repr(C)]
#[derive(Clone, Copy)]
struct SlotStat { ts_ns: u64, pkts_recent: u32, drops: u32 }
#[map]
static SLOT_STATS_PCPU: PerCpuArray<SlotStat> = PerCpuArray::with_max_entries(1, 0);

// LRU insert sampler
#[map]
static LRU_SAMPLE: PerCpuArray<u32> = PerCpuArray::with_max_entries(1, 0);

// ===================== Utils =====================

#[inline(always)]
unsafe fn ptr_at<T>(ctx: &XdpContext, offset: usize) -> Result<*const T, ()> {
    let start = ctx.data();
    let end = ctx.data_end();
    let len = size_of::<T>();
    if start + offset + len > end { return Err(()); }
    Ok((start + offset) as *const T)
}

#[inline(always)]
unsafe fn ptr_at_tc<T>(ctx: &TcContext, offset: usize) -> Result<*const T, ()> {
    let start = ctx.data();
    let end = ctx.data_end();
    let len = size_of::<T>();
    if start + offset + len > end { return Err(()); }
    Ok((start + offset) as *const T)
}

#[inline(always)]
fn ipv4_header_len_bytes(ip: *const IpHdr) -> usize { unsafe { ((*(ip)).version_ihl & 0x0F) as usize * 4 } }
#[inline(always)]
fn ipv4_has_options(ip: *const IpHdr) -> bool { ipv4_header_len_bytes(ip) != IPV4_HLEN }
#[inline(always)]
fn is_udp(ip: *const IpHdr) -> bool { unsafe { (*ip).protocol } == IPPROTO_UDP }

#[inline(always)] fn u16_be(x: u16) -> u16 { u16::from_be(x) }
#[inline(always)] fn u32_be(x: u32) -> u32 { u32::from_be(x) }

#[inline(always)]
fn mix64(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^ (x >> 33)
}

#[inline(always)]
fn should_emit(now: u64) -> bool {
    unsafe {
        if let Some(p) = EVT_RATE.get_ptr_mut(0) {
            if now.wrapping_sub(*p) > 1_000_000 {
                *p = now;
                return true;
            }
        }
    }
    false
}

// ===================== VIP helpers =====================
// All IPv4 VIP keys stored BE.
// /24 and /16: mask in host order, then convert masked prefix back to BE before lookup.
#[inline(always)]
fn vip_prio_v4(dst_ip_be: u32) -> u8 {
    unsafe {
        if let Some(v) = VIP_DST32.get(&dst_ip_be) { return *v; }
        let u = u32_be(dst_ip_be);
        let k24_be = u32::to_be(u & 0xFFFFFF00);
        if let Some(v) = VIP_DST24.get(&k24_be) { return *v; }
        let k16_be = u32::to_be(u & 0xFFFF0000);
        if let Some(v) = VIP_DST16.get(&k16_be) { return *v; }
    }
    0
}

// IPv6 VIP: store /64 prefix as BE u64 of the first 8 bytes (daddr[0..8]).
#[inline(always)]
fn vip_prio_v6(dst6: &[u8; 16]) -> u8 {
    let mut hi = 0u64;
    // pack first 8 bytes as BE u64
    hi |= (dst6[0] as u64) << 56; hi |= (dst6[1] as u64) << 48;
    hi |= (dst6[2] as u64) << 40; hi |= (dst6[3] as u64) << 32;
    hi |= (dst6[4] as u64) << 24; hi |= (dst6[5] as u64) << 16;
    hi |= (dst6[6] as u64) << 8;  hi |= (dst6[7] as u64);
    unsafe { if let Some(v) = VIP6_DST64.get(&hi) { *v } else { 0 } }
}

// ===================== IPv6 extension headers (optional) =====================

#[inline(always)]
fn ipv6_skip_eh(payload: &[u8]) -> (u8, usize) {
    // Only compiled if feature flag is set; otherwise return Next Header and fixed offset
    // RFC 8200 permits a chain of EHs. We bound iterations for eBPF verifier.
    #[cfg(feature = "ipv6_eh")]
    {
        let mut next = payload[6]; // next_header at fixed offset in IPv6 header
        let mut off = size_of::<Ipv6Hdr>();
        // Bounded walker: at most 4 EHs to keep verifier happy.
        // IETF Datatracker (RFC 8200) semantics.
        for _ in 0..4 {
            // Known EH types with 8-octet length units (except Fragment)
            // 0: Hop-by-Hop, 43: Routing, 44: Fragment, 60: Destination Options
            if next == 0 || next == 43 || next == 60 {
                if payload.len() < off + 2 { return (next, off); }
                let hdr_len_units = payload[off + 1] as usize;
                let hdr_len = (hdr_len_units + 1) * 8;
                off = off.saturating_add(hdr_len);
                if payload.len() < off + 1 { return (next, off); }
                next = payload[off - hdr_len] /* new header starts here */; // safe-ish marker
                continue;
            } else if next == 44 {
                // Fragment header is fixed 8 bytes
                off = off.saturating_add(8);
                if payload.len() < off + 1 { return (next, off); }
                next = payload[off - 8];
                continue;
            }
            break;
        }
        (next, off)
    }
    #[cfg(not(feature = "ipv6_eh"))]
    {
        // Fast path: we assume UDP follows fixed IPv6 header.
        (payload[6], size_of::<Ipv6Hdr>())
    }
}

// ===================== QUIC parsing =====================

#[inline(always)]
fn quic_parse_header(payload: &[u8], five_tuple_key: u64) -> (bool, bool, bool, bool, u8, u64) {
    // returns: (is_quic, is_long, is_initial, is_vn, dcid_len, key64)
    if payload.len() < 7 { return (false, false, false, false, 0, 0); }
    let b0 = payload[0];

    // Long header quick test: 11xxxxxx (header form 1 + fixed bit 1)
    // RFC 8999 (QUIC Invariants): fixed bit must be 1 for all short/long headers.
    // RFC 9000 §17.2: Long header structure and Version field.
    if (b0 & 0xC0) == 0xC0 {
        if payload.len() < 12 { return (false, false, false, false, 0, 0); }
        let ver = ((payload[1] as u32) << 24)
                | ((payload[2] as u32) << 16)
                | ((payload[3] as u32) << 8)
                |  (payload[4] as u32);

        let dcid_len = payload[5] as usize;
        if dcid_len == 0 || dcid_len > 20 { return (false, false, false, false, 0, 0); }

        // Pack min(8, dcid_len) bytes into key64 for flow hashing
        let mut key64 = 0u64;
        let take = if dcid_len >= 8 { 8 } else { dcid_len };
        if take > 0 { key64 |= (payload[6]  as u64) }
        if take > 1 { key64 |= (payload[7]  as u64) << 8 }
        if take > 2 { key64 |= (payload[8]  as u64) << 16 }
        if take > 3 { key64 |= (payload[9]  as u64) << 24 }
        if take > 4 { key64 |= (payload[10] as u64) << 32 }
        if take > 5 { key64 |= (payload[11] as u64) << 40 }
        if take > 6 { key64 |= (payload[12] as u64) << 48 }
        if take > 7 { key64 |= (payload[13] as u64) << 56 }

        if ver == 0 {
            // RFC 9000 §17.2: version 0 = Version Negotiation (VN).
            // Treat as QUIC-class for telemetry, but never redirect.
            return (true, true, false, true, dcid_len as u8, key64);
        }

        // RFC 9000 §17.2 long header type; Initial requires ≥ 1200-byte datagram
        let long_type = (b0 >> 2) & 0x03;
        let is_initial = long_type == 0 && payload.len() >= QUIC_INITIAL_MIN; // RFC 9000 §14.1/§12.2: Initial ≥ 1200B (IETF Datatracker)
        return (true, true, is_initial, false, dcid_len as u8, key64);
    }

    // Short header:
    // RFC 8999 (Invariants) fixed bit must be 1; RFC 9000 §17.3 PN length ∈ {1,2,3,4}
    let header_form_long = (b0 & 0x80) == 0x80;
    let fixed = (b0 & 0x40) == 0x40; // RFC 8999 fixed bit = 1 (tzi.de mirror)
    if !header_form_long && fixed {
        // RFC 9000 §17.2: PN length encoded in low 2 bits + 1 ∈ {1..4} (tzi.de mirror)
        let pn_len = (b0 & 0x03) + 1; // RFC 9000 §17.2: PN length encoded in 1..4 bytes; fixed bit per RFC 8999. (tzi.de)
        if pn_len >= 1 && pn_len <= 4 {
            return (true, false, false, false, 0, five_tuple_key);
        }
    }
    (false, false, false, false, 0, 0)
}

#[inline(always)]
fn maybe_has_second_initial(payload: &[u8]) -> bool {
    if payload.len() < COALESCE_SECOND_OFF + 7 { return false; }
    let slice2 = &payload[COALESCE_SECOND_OFF..];
    // Stricter second-check: ensure it parses as long header with Initial semantics
    // and carries the fixed bit; RFC 9000 §14.1/§12.2 coalescing. (IETF Datatracker)
    let (q, l, i, _vn, _dcid, _k) = quic_parse_header(slice2, 0);
    q && l && i
}

// ===================== Congestion + gating =====================

#[inline(always)]
fn slot_pressure() -> u32 {
    unsafe { SLOT_STATS_PCPU.get_ptr(0).map(|s| unsafe { (*s).pkts_recent }).unwrap_or(0) }
}

#[inline(always)]
fn cfg() -> Cfg {
    unsafe {
        *CFG.get(0).unwrap_or(&Cfg { pkt_hi: 120_000, pkt_med: 60_000, qmask: 0x3F, policy: 1 })
    }
}

#[inline(always)]
fn flow_salt() -> u32 {
    unsafe { FLOW_SALT_PCPU.get_ptr(0).map(|s| unsafe { (*s).flow }).unwrap_or(0x9e3779b1) }
}

#[inline(always)]
fn token_admit_nonvip(now: u64, src_ip_be: u32, urgent: bool) -> bool {
    let mut st = SrcTok { ts_refill: 0, tokens: 0 };
    unsafe {
        if let Some(v) = SRC_LIMIT.get(&src_ip_be) { st = *v; }
        let mut tokens = st.tokens as u32;
        let last = st.ts_refill;
        if last == 0 || now.wrapping_sub(last) > 1_000_000 {
            let add = ((now.wrapping_sub(last)) / 1_000_000) as u32;
            tokens = core::cmp::min(tokens.saturating_add(add), 32);
            st.ts_refill = now;
        }
        let need = if urgent { 0 } else { 1 };
        if tokens >= need {
            st.tokens = (tokens - need) as u16;
            let _ = SRC_LIMIT.insert(&src_ip_be, &st, 0);
            return true;
        }
        let _ = SRC_LIMIT.insert(&src_ip_be, &st, 0);
    }
    false
}

// ===================== AF_XDP routing =====================

#[inline(always)]
fn select_qid(key: u64, qmask: u32, salt: u32) -> u32 {
    let h = mix64(key ^ salt as u64) as u32;
    h & qmask
}

#[inline(always)]
fn queue_budget_ok(qid: u32, now: u64) -> bool {
    unsafe {
        if let Some(b) = XSK_QBUDGET.get_ptr_mut(qid as usize) {
            let mut tokens = (*b).tokens as u32;
            if now.wrapping_sub((*b).last_refill) > 500_000 {
                let add = ((now.wrapping_sub((*b).last_refill)) / 500_000) as u32;
                tokens = core::cmp::min(tokens.saturating_add(add), 64);
                (*b).last_refill = now;
                (*b).tokens = tokens as u16;
            }
            if tokens > 0 {
                (*b).tokens = (tokens - 1) as u16;
                return true;
            }
        }
    }
    false
}

#[inline(always)]
fn emit(ctx: &impl BpfContext, m: &QuicMeta) {
    if should_emit(unsafe { bpf_ktime_get_ns() }) {
        unsafe { HOT_HEADERS.output(ctx, m, 0) };
    }
}

// ===================== L2/v4/v6 parsing =====================

#[inline(always)]
fn parse_l2_and_proto_xdp(ctx: &XdpContext) -> Result<(usize, u16), ()> {
    let eth: *const EthHdr = unsafe { ptr_at(ctx, 0)? };
    let mut proto = unsafe { (*eth).h_proto };
    let mut l2 = ETH_HLEN;
    if proto == u16::from_be(ETH_P_8021Q) {
        if ctx.data() + l2 + VLAN_HLEN > ctx.data_end() { return Err(()); }
        let inner_ptr = (ctx.data() + ETH_HLEN + 2) as *const u16;
        unsafe { proto = *inner_ptr; }
        l2 += VLAN_HLEN;
    }
    Ok((l2, proto))
}

#[inline(always)]
fn parse_l2_and_proto_tc(ctx: &TcContext) -> Result<(usize, u16), ()> {
    let eth: *const EthHdr = unsafe { ptr_at_tc(ctx, 0)? };
    let mut proto = unsafe { (*eth).h_proto };
    let mut l2 = ETH_HLEN;
    if proto == u16::from_be(ETH_P_8021Q) {
        if ctx.data() + l2 + VLAN_HLEN > ctx.data_end() { return Err(()); }
        let inner_ptr = (ctx.data() + ETH_HLEN + 2) as *const u16;
        unsafe { proto = *inner_ptr; }
        l2 += VLAN_HLEN;
    }
    Ok((l2, proto))
}

#[inline(always)]
fn v6_five_tuple_key(s6: &[u8;16], d6: &[u8;16], sport: u16, dport: u16) -> u64 {
    let mut hi = 0u64; let mut lo = 0u64;
    hi |= (s6[0] as u64) << 56; hi |= (s6[1] as u64) << 48;
    hi |= (s6[2] as u64) << 40; hi |= (s6[3] as u64) << 32;
    hi |= (s6[4] as u64) << 24; hi |= (s6[5] as u64) << 16;
    hi |= (s6[6] as u64) << 8;  hi |= (s6[7] as u64);

    lo |= (d6[8] as u64) << 56; lo |= (d6[9] as u64) << 48;
    lo |= (d6[10] as u64) << 40; lo |= (d6[11] as u64) << 32;
    lo |= (d6[12] as u64) << 24; lo |= (d6[13] as u64) << 16;
    lo |= (d6[14] as u64) << 8;  lo |= (d6[15] as u64);

    mix64(mix64(hi ^ lo) ^ (((sport as u64) << 16) ^ (dport as u64)))
}

// ===================== Core XDP =====================

#[xdp]
pub fn quic_tpu_filter(ctx: XdpContext) -> u32 {
    match handle_xdp(&ctx) {
        Ok(a) => a,
        Err(_) => xdp_action::XDP_PASS,
    }
}

fn handle_xdp(ctx: &XdpContext) -> Result<u32, ()> {
    let (l2, proto) = parse_l2_and_proto_xdp(ctx)?;

    // IPv4 fast-path
    if proto == u16::from_be(ETH_P_IP) {
        let ip: *const IpHdr = unsafe { ptr_at(ctx, l2)? };
        if ipv4_has_options(ip) { return Ok(xdp_action::XDP_PASS); }
        if !is_udp(ip) { return Ok(xdp_action::XDP_PASS); }

        let ihl = ipv4_header_len_bytes(ip);
        let udp: *const UdpHdr = unsafe { ptr_at(ctx, l2 + ihl)? };
        let udp_len = u16_be(unsafe { (*udp).len }) as usize;
        if udp_len < UDP_HLEN || udp_len > MAX_UDP { return Ok(xdp_action::XDP_PASS); }

        let payload_off = l2 + ihl + UDP_HLEN;
        let data = ctx.data(); let end = ctx.data_end();
        let plen = udp_len - UDP_HLEN;
        if data + payload_off + plen > end { return Ok(xdp_action::XDP_PASS); }
        let payload = unsafe { core::slice::from_raw_parts((data + payload_off) as *const u8, plen.min(MAX_FRAME - payload_off)) };

        let saddr_be = unsafe { (*ip).saddr };
        let daddr_be = unsafe { (*ip).daddr };
        let sport = u16_be(unsafe { (*udp).source }) as u64;
        let dport = u16_be(unsafe { (*udp).dest }) as u64;
        let saddr = u32_be(saddr_be) as u64;
        let daddr = u32_be(daddr_be) as u64;
        let five_key = mix64((saddr << 32) | daddr) ^ ((sport << 16) ^ dport);

        let (mut is_quic, is_long, mut is_initial, is_vn, dcid_len, key64) = quic_parse_header(payload, five_key);
        if is_quic && !is_initial && payload.len() >= COALESCE_SECOND_OFF + QUIC_INITIAL_MIN {
            // RFC 9000 §14.1/§12.2: coalesced Initials permitted (IETF Datatracker)
            if maybe_has_second_initial(payload) { is_initial = true; }
        }
        if !is_quic { return Ok(xdp_action::XDP_PASS); }

        let now = unsafe { bpf_ktime_get_ns() };
        let press = slot_pressure();
        let cfg = cfg();
        let vip = vip_prio_v4(daddr_be);
        let is_vip = vip > 0;

        let urgent = is_vip && is_initial;
        let mut gated = false;

        if press >= cfg.pkt_hi && !is_vip {
            // Allow only long+Initial at high pressure; throttle others
            if !(is_long && is_initial) { return Ok(xdp_action::XDP_DROP); }
            if !token_admit_nonvip(now, saddr_be, urgent) { return Ok(xdp_action::XDP_DROP); }
            gated = true;
        } else if press >= cfg.pkt_med && !is_vip {
            if !(is_initial || token_admit_nonvip(now, saddr_be, urgent)) { return Ok(xdp_action::XDP_DROP); }
            gated = true;
        }

        unsafe {
            if let Some(v) = FLOW_LRU.get(&key64) {
                let score = v.score.saturating_add(1);
                let _ = FLOW_LRU.insert(&key64, &CacheVal { ts: now, score }, 0);
            } else if let Some(cnt) = LRU_SAMPLE.get_ptr_mut(0) {
                let c = (*cnt).wrapping_add(1); *cnt = c;
                if (c & 0x1F) == 0 { let _ = FLOW_LRU.insert(&key64, &CacheVal { ts: now, score: 1 }, 0); }
            }
        }

        let salt = flow_salt();
        let qid = select_qid(key64, cfg.qmask, salt);
        let can_redirect = !is_vn && queue_budget_ok(qid, now); // VN telemetry only; never redirect

        let mut flags: u8 = 0;
        if is_long { flags |= 1; }
        if is_initial { flags |= 1 << 1; }
        if is_vip { flags |= 1 << 2; }
        if gated { flags |= 1 << 3; }
        if is_vn   { flags |= 1 << 6; }

        let meta = QuicMeta {
            ts_ns: now,
            src_ip: saddr as u32,
            dst_ip: daddr as u32,
            src_port: sport as u16,
            dst_port: dport as u16,
            flags,
            dcid_len,
            qid: qid as u8,
            _pad: 0,
            key64,
        };
        emit(ctx, &meta);

        if can_redirect {
            // Kernel docs: redirect to AF_XDP via XSKMAP + XDP_REDIRECT. (docs.kernel.org/net/af_xdp/, bpf/xdp/)
            unsafe { let _ = XSK_SOCKS.redirect(ctx, qid); }
            let mut m2 = meta; m2.flags |= 1 << 4; emit(ctx, &m2);
            return Ok(xdp_action::XDP_REDIRECT);
        }
        return Ok(xdp_action::XDP_PASS);
    }

    // IPv6 parity
    if proto == u16::from_be(ETH_P_IPV6) {
        let ip6: *const Ipv6Hdr = unsafe { ptr_at(ctx, l2)? };
        // Optional EH walk; if disabled, assumes UDP next.
        let data = ctx.data(); let end = ctx.data_end();
        let ip6_off = l2;
        let ipv6_slice = unsafe {
            core::slice::from_raw_parts((data + ip6_off) as *const u8, (end - (data + ip6_off)).min(MAX_FRAME - ip6_off))
        };
        let (next, udp_off) = ipv6_skip_eh(ipv6_slice);
        if next != IPPROTO_UDP { return Ok(xdp_action::XDP_PASS); }

        let udp_abs = l2 + udp_off;
        let udp: *const UdpHdr = unsafe { ptr_at(ctx, udp_abs)? };
        let udp_len = u16_be(unsafe { (*udp).len }) as usize;
        if udp_len < UDP_HLEN || udp_len > MAX_UDP { return Ok(xdp_action::XDP_PASS); }

        let payload_off = udp_abs + UDP_HLEN;
        let data = ctx.data(); let end = ctx.data_end();
        let plen = udp_len - UDP_HLEN;
        if data + payload_off + plen > end { return Ok(xdp_action::XDP_PASS); }
        let payload = unsafe { core::slice::from_raw_parts((data + payload_off) as *const u8, plen.min(MAX_FRAME - payload_off)) };

        let sport = u16_be(unsafe { (*udp).source });
        let dport = u16_be(unsafe { (*udp).dest });
        let s6 = unsafe { &(*ip6).saddr }; let d6 = unsafe { &(*ip6).daddr };

        let key64 = v6_five_tuple_key(s6, d6, sport, dport);
        let (mut is_quic, is_long, mut is_initial, is_vn, dcid_len, key64_final) = quic_parse_header(payload, key64);
        if is_quic && !is_initial && payload.len() >= COALESCE_SECOND_OFF + QUIC_INITIAL_MIN {
            // RFC 9000 §14.1/§12.2: coalesced Initials permitted (IETF Datatracker)
            if maybe_has_second_initial(payload) { is_initial = true; }
        }
        if !is_quic { return Ok(xdp_action::XDP_PASS); }

        let now = unsafe { bpf_ktime_get_ns() };
        let press = slot_pressure();
        let cfg = cfg();

        let vip = vip_prio_v6(d6);
        let is_vip = vip > 0;
        let mut gated = false;

        if press >= cfg.pkt_hi && !is_vip {
            if !(is_long && is_initial) { return Ok(xdp_action::XDP_DROP); }
            // v6 per-src token bucket omitted; policy usually VIP-only for v6
            gated = true;
        } else if press >= cfg.pkt_med && !is_vip {
            if !is_initial { return Ok(xdp_action::XDP_DROP); }
            gated = true;
        }

        unsafe {
            if let Some(v) = FLOW_LRU.get(&key64_final) {
                let score = v.score.saturating_add(1);
                let _ = FLOW_LRU.insert(&key64_final, &CacheVal { ts: now, score }, 0);
            } else if let Some(cnt) = LRU_SAMPLE.get_ptr_mut(0) {
                let c = (*cnt).wrapping_add(1); *cnt = c;
                if (c & 0x1F) == 0 { let _ = FLOW_LRU.insert(&key64_final, &CacheVal { ts: now, score: 1 }, 0); }
            }
        }

        let qid = select_qid(key64_final, cfg.qmask, flow_salt());
        let can_redirect = !is_vn && queue_budget_ok(qid, now); // VN telemetry only; never redirect

        let mut flags: u8 = 0;
        if is_long { flags |= 1; }
        if is_initial { flags |= 1 << 1; }
        if is_vip { flags |= 1 << 2; }
        if gated { flags |= 1 << 3; }
        flags |= 1 << 5; // ipv6
        if is_vn { flags |= 1 << 6; }

        let meta = QuicMeta {
            ts_ns: now,
            src_ip: 0, dst_ip: 0,
            src_port: sport, dst_port: dport,
            flags,
            dcid_len,
            qid: qid as u8,
            _pad: 0,
            key64: key64_final,
        };
        emit(ctx, &meta);

        if can_redirect {
            // Kernel docs: redirect to AF_XDP via XSKMAP + XDP_REDIRECT. (docs.kernel.org/net/af_xdp/, bpf/xdp/)
            unsafe { let _ = XSK_SOCKS.redirect(ctx, qid); }
            return Ok(xdp_action::XDP_REDIRECT);
        }
        return Ok(xdp_action::XDP_PASS);
    }

    Ok(xdp_action::XDP_PASS)
}

// ===================== TC ingress parity =====================

#[classifier]
pub fn quic_tpu_tc(ctx: TcContext) -> i32 {
    match handle_tc(&ctx) {
        Ok(a) => a,
        Err(_) => TC_ACT_OK,
    }
}

fn handle_tc(ctx: &TcContext) -> Result<i32, ()> {
    let (l2, proto) = parse_l2_and_proto_tc(ctx)?;
    if proto != u16::from_be(ETH_P_IP) { return Ok(TC_ACT_OK); }

    let ip: *const IpHdr = unsafe { ptr_at_tc(ctx, l2)? };
    if ipv4_has_options(ip) { return Ok(TC_ACT_OK); }
    if !is_udp(ip) { return Ok(TC_ACT_OK); }

    let ihl = ipv4_header_len_bytes(ip);
    let udp: *const UdpHdr = unsafe { ptr_at_tc(ctx, l2 + ihl)? };
    let udp_len = u16_be(unsafe { (*udp).len }) as usize;
    if udp_len < UDP_HLEN || udp_len > MAX_UDP { return Ok(TC_ACT_OK); }

    let payload_off = l2 + ihl + UDP_HLEN;
    let data = ctx.data(); let end = ctx.data_end();
    let plen = udp_len - UDP_HLEN;
    if data + payload_off + plen > end { return Ok(TC_ACT_OK); }
    let payload = unsafe { core::slice::from_raw_parts((data + payload_off) as *const u8, plen) };

    let saddr_be = unsafe { (*ip).saddr };
    let daddr_be = unsafe { (*ip).daddr };
    let sport = u16_be(unsafe { (*udp).source }) as u64;
    let dport = u16_be(unsafe { (*udp).dest }) as u64;
    let saddr = u32_be(saddr_be) as u64;
    let daddr = u32_be(daddr_be) as u64;
    let five_key = mix64((saddr << 32) | daddr) ^ ((sport << 16) ^ dport);

    let (mut is_quic, is_long, mut is_initial, is_vn, _dcid_len, key64) = quic_parse_header(payload, five_key);
    if is_quic && !is_initial && payload.len() >= COALESCE_SECOND_OFF + QUIC_INITIAL_MIN {
        // RFC 9000 §14.1/§12.2: coalesced Initials permitted (IETF Datatracker)
        if maybe_has_second_initial(payload) { is_initial = true; }
    }
    if !is_quic { return Ok(TC_ACT_OK); }

    let now = unsafe { bpf_ktime_get_ns() };
    let press = slot_pressure();
    let cfg = cfg();
    let is_vip = vip_prio_v4(daddr_be) > 0;

    if press >= cfg.pkt_hi && !is_vip {
        if !(is_long && is_initial) { return Ok(TC_ACT_SHOT); }
        if !is_vn && !token_admit_nonvip(now, saddr_be, false) { return Ok(TC_ACT_SHOT); }
    } else if press >= cfg.pkt_med && !is_vip {
        if !(is_initial || (!is_vn && token_admit_nonvip(now, saddr_be, false))) { return Ok(TC_ACT_SHOT); }
    }

    unsafe {
        if let Some(v) = FLOW_LRU.get(&key64) {
            let score = v.score.saturating_add(1);
            let _ = FLOW_LRU.insert(&key64, &CacheVal { ts: now, score }, 0);
        } else if let Some(cnt) = LRU_SAMPLE.get_ptr_mut(0) {
            let c = (*cnt).wrapping_add(1); *cnt = c;
            if (c & 0x1F) == 0 { let _ = FLOW_LRU.insert(&key64, &CacheVal { ts: now, score: 1 }, 0); }
        }
    }

    Ok(TC_ACT_OK)
}

// ===================== Panic + license =====================

#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    unsafe { core::hint::unreachable_unchecked() }
}

#[no_mangle]
#[link_section = "license"]
pub static _license: [u8; 4] = *b"GPL\0";

#[no_mangle]
#[link_section = "version"]
pub static _version: u32 = 0xFFFFFFFE;
