// ===== Verifier-friendly bounded readers =====
// REPLACE: load_bytes
#[inline(always)]
fn load_bytes<'a>(payload: &'a [u8], off: usize, n: usize) -> Result<&'a [u8], ()> {
    let end = off.checked_add(n).ok_or(())?;
    if end > payload.len() { return Err(()); }
    Ok(&payload[off..end])
}

// REPLACE: skip_n_u8
#[inline(always)]
fn skip_n_u8(payload: &[u8], off: &mut usize, n: usize) -> Result<(), ()> {
    let _ = load_bytes(payload, *off, n)?;
    *off = (*off).saturating_add(n);
    Ok(())
}

// ===== Per-slot congestion stats (per-CPU) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct SlotStats { epoch_ns: u64, pkts: u32 }

#[map]
static SLOT_STATS_PCPU: PerCpuArray<SlotStats> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn slot_congestion(now: u64) -> u32 {
    unsafe {
        if let Some(s) = SLOT_STATS_PCPU.get_ptr_mut(0) {
            let rollover = now.wrapping_sub((*s).epoch_ns) > SLOT_NS;
            if rollover { (*s).epoch_ns = now; (*s).pkts = 0; }
            (*s).pkts = (*s).pkts.wrapping_add(1);
            (*s).pkts
        } else { 0 }
    }
}

// REPLACE: dynamic_thresholds
#[inline(always)]
fn dynamic_thresholds(pkts: u32) -> (u64, u32) {
    if pkts > 60_000 { (400_000_000u64, 400_000u32) }
    else if pkts > 30_000 { (200_000_000u64, 300_000u32) }
    else if pkts > 15_000 { (120_000_000u64, 240_000u32) }
    else { (80_000_000u64, 180_000u32) }
}

#[inline(always)]
fn classify_mev_type_v2(event: &PacketEvent, corr_flags: u8, is_bundle_flow: bool) -> u8 {
    let now = unsafe { bpf_ktime_get_ns() };
    let pkts = slot_congestion(now);
    let (lam_min, cu_min) = dynamic_thresholds(pkts);
    // Liquidation signature
    if (event.compute_units as u32) > cu_min + 100_000 && event.accounts_count > 8 && event.lamports > lam_min + 50_000_000 { return 4; }
    // Arbitrage
    if is_dex_program(&event.program_id) && event.instructions_count >= 2 && (event.compute_units as u32) > cu_min && event.lamports > lam_min { return 2; }
    // Sandwich with bundle flow
    if is_dex_program(&event.program_id) && is_bundle_flow && (event.compute_units as u32) > cu_min && event.lamports > lam_min / 2 { return 3; }
    // DEX trade
    if is_dex_program(&event.program_id) && event.lamports > lam_min / 4 { return 1; }
    // Flash loan correlated pattern overrides
    if detect_flash_loan_correlated(event, corr_flags) { return 2; }
    0
}

// Priority-aware AF_XDP redirect wrapper
#[inline(always)]
fn redirect_priority(ctx: &XdpContext, event: &PacketEvent) -> u32 {
    let qid: u32 = (event.hash as u32) & 0x3F;
    unsafe { XSK_SOCKS.redirect(ctx, qid) }
}

// ===== Hot/Cold cache: per-CPU ring + shared LRU keyed by (sig_hash ^ slot_bucket) =====
const HOT_RING: usize = 128; // power of two

#[repr(C)]
#[derive(Clone, Copy)]
struct HotRing { slots: [u64; HOT_RING], head: u32, epoch_ns: u64 }

#[map]
static HOT_PCPU: PerCpuArray<HotRing> = PerCpuArray::with_max_entries(1, 0);

#[repr(C)]
#[derive(Clone, Copy)]
struct ColdVal { ts: u64 }

#[map]
static COLD_LRU: LruHashMap<u64, ColdVal> = LruHashMap::with_max_entries(16384, 0);

#[inline(always)]
fn slot_bucket_ns(now: u64) -> u64 { now / SLOT_NS }

#[inline(always)]
fn composite_tx_key(sig_hash: u64, now: u64) -> u64 { sig_hash ^ slot_bucket_ns(now).rotate_left(13) }

#[inline(always)]
fn cache_ttl_ns(event: &PacketEvent) -> u64 {
    let base = match event.mev_type { 3 => 5_000_000_000, 2 => 3_000_000_000, 4 => 10_000_000_000, 1 => 2_000_000_000, _ => 1_000_000_000 };
    if event.priority < 5 { base / 2 } else { base }
}

#[inline(always)]
fn hot_cold_cache_check(now: u64, key: u64, ttl: u64) -> bool {
    let mut hit = false;
    unsafe {
        if let Some(h) = HOT_PCPU.get_ptr_mut(0) {
            // probe last 16 entries
            let mut i = 0usize;
            while i < 16 { let idx = ((*h).head as usize).wrapping_sub(i) & (HOT_RING - 1); if (*h).slots[idx] == key { hit = true; break; } i += 1; }
            let idx = ((*h).head as usize) & (HOT_RING - 1); (*h).slots[idx] = key; (*h).head = (*h).head.wrapping_add(1);
        }
    }
    if hit { return true; }
    unsafe {
        if let Some(v) = COLD_LRU.get(&key) { if now.wrapping_sub((*v).ts) < ttl { return true; } }
        let val = ColdVal { ts: now }; let _ = COLD_LRU.insert(&key, &val, 0);
    }
    false
}

// ===== Oracle whitelist v2 (exact match + pattern) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct OracleTag { _rsvd: u8 }

#[map]
static ORACLE_WHITELIST: HashMap<[u8; 32], OracleTag> = HashMap::with_max_entries(64, 0);

#[inline(always)]
fn is_oracle_exact(program_id: &[u8; 32]) -> bool { unsafe { ORACLE_WHITELIST.get(program_id).is_some() } }

#[inline(always)]
fn oracle_update_pattern(event: &PacketEvent) -> bool { event.lamports == 0 && event.accounts_count <= 6 && event.compute_units >= 150_000 }

#[inline(always)]
fn detect_oracle_update_v2(event: &PacketEvent) -> bool { is_oracle_exact(&event.program_id) && oracle_update_pattern(event) }

// ===== Per-CPU score histogram for adaptive routing =====
const HIST_BUCKETS: usize = 32; // 0..31
const HIST_WINDOW_NS: u64 = 1_000_000_000; // ~1s

#[repr(C)]
#[derive(Clone, Copy)]
struct ScoreHist { counts: [u32; HIST_BUCKETS], total: u32, epoch_ns: u64 }

#[map]
static SCORE_HIST_PCPU: PerCpuArray<ScoreHist> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn score_to_bucket(score: u32) -> usize {
    let s = if score > 100_000 { 100_000 } else { score } as u64;
    ((s * ((HIST_BUCKETS as u64) - 1)) / 100_000u64) as usize
}

#[inline(always)]
fn hist_add(now: u64, score: u32) {
    unsafe {
        if let Some(h) = SCORE_HIST_PCPU.get_ptr_mut(0) {
            if now.wrapping_sub((*h).epoch_ns) > HIST_WINDOW_NS { (*h).counts = [0u32; HIST_BUCKETS]; (*h).total = 0; (*h).epoch_ns = now; }
            let b = score_to_bucket(score);
            (*h).counts[b] = (*h).counts[b].wrapping_add(1);
            (*h).total = (*h).total.wrapping_add(1);
        }
    }
}

// REPLACE: hist_percentile
#[inline(always)]
fn hist_percentile(pct: u32) -> usize {
    unsafe {
        if let Some(h) = SCORE_HIST_PCPU.get_ptr(0) {
            let total = (*h).total; if total == 0 { return HIST_BUCKETS - 1; }
            let target = (((total as u64) * (pct as u64) + 99) / 100) as u32; // ceil
            let mut accum = 0u32;
            let mut i = 0usize;
            while i < HIST_BUCKETS {
                accum = accum.wrapping_add((*h).counts[i]);
                if accum >= target { return i; }
                i += 1;
            }
            HIST_BUCKETS - 1
        } else { HIST_BUCKETS - 1 }
    }
}

#[inline(always)]
fn adaptive_routing(score: u32, est_latency_ns: u64) -> u8 {
    let p95_bucket = hist_percentile(95);
    let cur_bucket = score_to_bucket(score);
    let hi = cur_bucket >= p95_bucket;
    let latency_bump = est_latency_ns < 1_000; // < 1us
    let mut tier = 0u8; // 0 normal, 1 priority, 2 ultra, 3 dedicated
    if hi { tier = 3; } else if cur_bucket + 2 >= p95_bucket { tier = 2; } else if cur_bucket + 5 >= p95_bucket { tier = 1; }
    if latency_bump && tier < 3 { tier += 1; }
    tier
}

// ===== Hardened scoring without FP and spoofing =====
#[map]
static PROGRAM_PREFIX_HITS: LruHashMap<u64, u32> = LruHashMap::with_max_entries(2048, 0);

#[inline(always)]
fn ilog2_u64(mut v: u64) -> u32 { let mut r = 0u32; while v >= 2 { v >>= 1; r += 1; } r }

#[inline(always)]
fn ilog10_scaled(x: u64) -> u32 { if x <= 1 { return 0; } let l2 = ilog2_u64(x); (l2 as u32).saturating_mul(1233) >> 12 }

#[inline(always)]
fn rarity_penalty(program_hits: u32) -> u16 { if program_hits > 1000 { 200 } else if program_hits > 200 { 100 } else { 0 } }

#[inline(always)]
fn calculate_transaction_score_hardened(event: &PacketEvent, program_hits: u32) -> u32 {
    let mev = calculate_mev_score(event) as u32;
    let profit_clamped = if event.expected_profit > 100_000_000_000 { 100_000_000_000 } else { event.expected_profit };
    let profit_log = ilog10_scaled(profit_clamped.saturating_add(1)) * 1000; // scaled
    let prio = (event.priority as u32).saturating_mul(800);
    let latency = { let adv = estimate_latency_advantage(event); let cap = if adv > 5000 { 0 } else { 5000 - adv as u32 }; cap };
    let base = mev.saturating_add(profit_log.min(10_000)).saturating_add(prio).saturating_add(latency);
    let penalty = rarity_penalty(program_hits) as u32;
    base.saturating_sub(penalty).min(100_000)
}

// ===== Flash loan correlation flags and helpers =====
const FL_BORROW: u8 = 0x01;
const FL_TRADE:  u8 = 0x02;
const FL_REPAY:  u8 = 0x04;

#[inline(always)]
fn flash_prog_prefix(program_id: &[u8; 32]) -> bool {
    (program_id[0] == 0x87 && program_id[1] == 0x62) || // Solend (example prefix)
    (program_id[0] == 0x4a && program_id[1] == 0x67) || // Port
    (program_id[0] == 0x22 && program_id[1] == 0xd6)    // Larix
}

#[inline(always)]
fn correlate_flash_bits(program_id: &[u8; 32], data_len: u16, accs: u8, flags: &mut u8) {
    if flash_prog_prefix(program_id) && data_len >= 8 { *flags |= FL_BORROW; }
    if accs >= 4 && data_len >= 8 { *flags |= FL_TRADE; }
    if flash_prog_prefix(program_id) && data_len >= 4 && accs >= 2 { *flags |= FL_REPAY; }
}

#[inline(always)]
fn detect_flash_loan_correlated(event: &PacketEvent, corr_flags: u8) -> bool {
    let pattern = (corr_flags & (FL_BORROW | FL_TRADE | FL_REPAY)) == (FL_BORROW | FL_TRADE | FL_REPAY);
    if pattern { return true; }
    let program_match = (event.program_id[0] == 0x87 && event.program_id[1] == 0x62) ||
                        (event.program_id[0] == 0x4a && event.program_id[1] == 0x67) ||
                        (event.program_id[0] == 0x22 && event.program_id[1] == 0xd6);
    let suspicious = event.compute_units > 350_000 && event.instructions_count >= 3 && event.accounts_count >= 6;
    program_match && suspicious
}

// ===== Per-slot rolling Bloom for flash bursts =====
const FL_BLOOM_BITS: usize = 2048;
const FL_BLOOM_WORDS: usize = FL_BLOOM_BITS / 64;
const SLOT_NS: u64 = 400_000_000; // ~400ms

#[repr(C)]
#[derive(Clone, Copy)]
struct FlashBloom { words: [u64; FL_BLOOM_WORDS], epoch_ns: u64 }

#[map]
static FLASH_BLOOM_PCPU: PerCpuArray<FlashBloom> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn mix64(mut x: u64) -> u64 { x = x.wrapping_add(0x9E37_79B9_7F4A_7C15); let mut z = x; z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9); z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB); z ^ (z >> 31) }

#[inline(always)]
fn dedupe_key_from_sig_and_bh(signature: &[u8; 64], bh_first8: u64) -> u64 {
    // Strengthen the existing mix with SplitMix64 over 8-byte chunks + blockhash salt.
    let mut h = 0x517c_c1b1_c2a3_e4f5u64 ^ bh_first8.rotate_left(17);
    let mut i = 0usize;
    while i < 64 {
        let chunk = u64::from_le_bytes([
            signature[i], signature[i+1], signature[i+2], signature[i+3],
            signature[i+4], signature[i+5], signature[i+6], signature[i+7]
        ]);
        // mix64 is already defined in your file; reuse it.
        h = mix64(h ^ chunk);
        h = h.rotate_left(13).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        i += 8;
    }
    h
}

#[inline(always)]
fn fl_bloom_index(h: u64) -> (usize, u64, usize, u64) {
    let h1 = mix64(h); let h2 = mix64(h ^ 0x517c_c1b1_c2a3_e4f5);
    let i1 = (h1 as usize) & (FL_BLOOM_BITS - 1); let i2 = (h2 as usize) & (FL_BLOOM_BITS - 1);
    (i1 / 64, 1u64 << (i1 & 63), i2 / 64, 1u64 << (i2 & 63))
}

#[inline(always)]
fn amount_bucket(lamports: u64) -> u32 {
    if lamports >= 1_000_000_000 { 3 } else if lamports >= 100_000_000 { 2 } else if lamports >= 10_000_000 { 1 } else { 0 }
}

#[inline(always)]
fn flash_burst_mark(now: u64, borrower_prefix8: u64, lamports: u64) -> bool {
    unsafe {
        if let Some(b) = FLASH_BLOOM_PCPU.get_ptr_mut(0) {
            let rotate = now.wrapping_sub((*b).epoch_ns) > SLOT_NS;
            if rotate { (*b).words = [0u64; FL_BLOOM_WORDS]; (*b).epoch_ns = now; }
            let key = borrower_prefix8 ^ (amount_bucket(lamports) as u64);
            let (w1,m1,w2,m2) = fl_bloom_index(key);
            let seen = ((*b).words[w1] & m1) != 0 && ((*b).words[w2] & m2) != 0;
            (*b).words[w1] |= m1; (*b).words[w2] |= m2;
            seen
        } else { false }
    }
}

// ================= Tail-call stages =================
#[inline(always)]
fn prefilter_stage(ctx: &XdpContext) -> Result<(), ()> {
    let eth_hdr: *const EthHdr = unsafe { ptr_at(ctx, 0) }?;
    if unsafe { (*eth_hdr).h_proto } != u16::from_be(ETH_P_IP) { return Err(()); }
    let ip_hdr: *const IpHdr = unsafe { ptr_at(ctx, ETH_HLEN) }?;
    if unsafe { (*ip_hdr).protocol } != IPPROTO_UDP { return Err(()); }
    let udp_hdr: *const UdpHdr = unsafe { ptr_at(ctx, ETH_HLEN + IP_HLEN) }?;
    let dst_port = u16::from_be(unsafe { (*udp_hdr).dest });
    let src_port = u16::from_be(unsafe { (*udp_hdr).source });
    let payload_off = (ETH_HLEN + IP_HLEN + UDP_HLEN) as u16;
    let payload_len = (u16::from_be(unsafe { (*udp_hdr).len }) as usize).saturating_sub(UDP_HLEN);
    if payload_len < 24 || payload_len > MAX_PACKET_SIZE { return Err(()); }
    let data = ctx.data(); let data_end = ctx.data_end();
    if (data + payload_off as usize + payload_len) > data_end { return Err(()); }
    let payload = unsafe { core::slice::from_raw_parts((data + payload_off as usize) as *const u8, payload_len) };
    if !is_quic_like(payload) { return Err(()); }
    unsafe {
        if let Some(s) = SCRATCH_PCPU.get_ptr_mut(0) {
            (*s).dst_port = dst_port; (*s).src_port = src_port;
            (*s).payload_off = payload_off; (*s).payload_len = payload_len as u16;
            (*s).flags = 1; // quic-like
        }
    }
    Ok(())
}

#[inline(always)]
fn signature_stage(_ctx: &XdpContext) -> Result<(), ()> {
    // Minimal stage reserved for future bounded signature pre-decode.
    Ok(())
}

#[inline(always)]
fn hot_path_decision(ctx: &XdpContext, payload: &[u8], event: &mut PacketEvent) -> u32 {
    let now = unsafe { bpf_ktime_get_ns() };
    if mark_bundle_flow(now, payload) { event.flags |= 0x01; }
    let ttl = get_cache_duration(event);
    if bloom_check_set(now, ttl, event.hash) {
        // Keep policy simple: drop at XDP for duplicates
        return xdp_action::XDP_DROP;
    }
    // Hot/cold cache keyed by slot bucket to avoid cross-slot collisions
    let key = composite_tx_key(event.hash, now);
    if hot_cold_cache_check(now, key, cache_ttl_ns(event)) { return xdp_action::XDP_DROP; }
    // Flash-loan correlation using bounded proxies
    let mut corr: u8 = 0;
    correlate_flash_bits(&event.program_id, event.data_len, event.accounts_count, &mut corr);
    // Per-slot burst mark keyed by program_id prefix (approx borrower)
    let mut borrower_prefix8 = 0u64;
    borrower_prefix8 = u64::from_le_bytes([event.program_id[0],event.program_id[1],event.program_id[2],event.program_id[3],event.program_id[4],event.program_id[5],event.program_id[6],event.program_id[7]]);
    let burst = flash_burst_mark(now, borrower_prefix8, event.lamports);
    if detect_flash_loan_correlated(event, corr) || ( (corr & (FL_BORROW|FL_TRADE)) == (FL_BORROW|FL_TRADE) && burst ) {
        // Mark as flash loan aggressively
        event.mev_type = 4; // Liquidation/flash bucket re-used for signal
        event.priority = event.priority.saturating_add(2).min(15);
    }
    // v2 classification with dynamic thresholds
    let is_bundle = (event.flags & 0x01) != 0;
    event.mev_type = classify_mev_type_v2(event, corr, is_bundle);
    event.priority = mev_priority_sigmoid(event);
    // Oracle updates v2: flag and slight deprioritization avoidance by keeping routing tier
    if detect_oracle_update_v2(event) { event.flags |= 0x20; event.priority = event.priority.saturating_add(1).min(15); }
    // Hardened score + histogram + adaptive routing tier
    let mut hits = 0u32;
    unsafe { if let Some(h) = PROGRAM_PREFIX_HITS.get(&borrower_prefix8) { hits = *h; } let new_hits = hits.saturating_add(1); let _ = PROGRAM_PREFIX_HITS.insert(&borrower_prefix8, &new_hits, 0); }
    let score = calculate_transaction_score_hardened(event, hits);
    hist_add(now, score);
    let tier = adaptive_routing(score, estimate_latency_advantage(event));
    // Emit lite event
    let lite = PacketEventLite { ts: event.timestamp, src_ip: event.src_ip, dst_ip: event.dst_ip,
        src_port: event.src_port, dst_port: event.dst_port, lamports: event.lamports,
        cu: event.compute_units, prio: event.priority, mev: event.mev_type, flags: event.flags,
        accs: event.accounts_count, insts: event.instructions_count, hash: event.hash,
        program_id_prefix: [event.program_id[0],event.program_id[1],event.program_id[2],event.program_id[3]] };
    unsafe { HOT_HEADERS.output(ctx, &lite, 0); }
    // AF_XDP redirect for ultra-priority
    if tier >= 2 || event.priority >= 13 || optimize_packet_routing(event) >= 2 {
        let act = redirect_priority(ctx, event);
        if act != 0 { return act; }
    }
    xdp_action::XDP_PASS
}

#[inline(always)]
fn classify_stage(ctx: &XdpContext) -> Result<u32, ()> {
    // Fetch scratch
    let s = unsafe { SCRATCH_PCPU.get_ptr(0) }.ok_or(())?;
    if unsafe { (*s).flags & 1 } == 0 { return Err(()); }
    let payload_off = unsafe { (*s).payload_off } as usize;
    let payload_len = unsafe { (*s).payload_len } as usize;
    let data = ctx.data();
    let payload = unsafe { core::slice::from_raw_parts((data + payload_off) as *const u8, payload_len) };

    // Bounded extract
    let mut event = unsafe { mem::zeroed::<PacketEvent>() };
    match extract_transaction_info(payload) {
        Ok(mut ev) => { event = ev; }
        Err(_) => { return Err(()); }
    }
    // Fill ip/ports
    let ip_hdr: *const IpHdr = unsafe { ptr_at(ctx, ETH_HLEN) }?;
    event.timestamp = unsafe { bpf_ktime_get_ns() };
    event.src_ip = u32::from_be(unsafe { (*ip_hdr).saddr });
    event.dst_ip = u32::from_be(unsafe { (*ip_hdr).daddr });
    let udp_hdr: *const UdpHdr = unsafe { ptr_at(ctx, ETH_HLEN + IP_HLEN) }?;
    event.src_port = u16::from_be(unsafe { (*udp_hdr).source });
    event.dst_port = u16::from_be(unsafe { (*udp_hdr).dest });

    let act = hot_path_decision(ctx, payload, &mut event);
    if act != xdp_action::XDP_PASS { return Ok(act); }
    // For very hot flows, also emit full event
    if event.priority >= 12 {
        unsafe { let _ = PRIORITY_QUEUE.push(&event, 0); PACKET_EVENTS.output(ctx, &event, 0); }
    }
    Ok(xdp_action::XDP_PASS)
}

// Tail-called XDP entrypoints
#[xdp]
pub fn prog_prefilter(ctx: XdpContext) -> u32 {
    if prefilter_stage(&ctx).is_ok() {
        unsafe { PROGS.tail_call(&ctx, 1); }
    }
    xdp_action::XDP_PASS
}

#[xdp]
pub fn prog_sigparse(ctx: XdpContext) -> u32 {
    if signature_stage(&ctx).is_ok() {
        unsafe { PROGS.tail_call(&ctx, 2); }
    }
    xdp_action::XDP_PASS
}

#[xdp]
pub fn prog_classify(ctx: XdpContext) -> u32 {
    match classify_stage(&ctx) { Ok(a) => a, Err(_) => xdp_action::XDP_PASS }
}

#[inline(always)]
fn load_u16_le(payload: &[u8], off: usize) -> Result<u16, ()> {
    let b = load_bytes(payload, off, 2)?;
    Ok(u16::from_le_bytes([b[0], b[1]]))
}

#[inline(always)]
fn load_u32_le(payload: &[u8], off: usize) -> Result<u32, ()> {
    let b = load_bytes(payload, off, 4)?;
    Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

#[inline(always)]
fn load_u32_be(payload: &[u8], off: usize) -> Result<u32, ()> {
    let b = load_bytes(payload, off, 4)?;
    Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
}

#[inline(always)]
fn copy_32(payload: &[u8], off: usize, dst: &mut [u8; 32]) -> Result<(), ()> {
    let s = load_bytes(payload, off, 32)?;
    unsafe { core::ptr::copy_nonoverlapping(s.as_ptr(), dst.as_mut_ptr(), 32) };
    Ok(())
}

// Compact-u16, bounded and local
#[inline(always)]
fn parse_compact_u16_bounded(payload: &[u8], off: &mut usize) -> Result<u16, ()> {
    let b0 = *load_bytes(payload, *off, 1)? .first().ok_or(())?;
    if b0 < 0x80 {
        *off += 1;
        Ok(b0 as u16)
    } else if b0 < 0xFE {
        let b1 = *load_bytes(payload, *off + 1, 1)? .first().ok_or(())?;
        let v = ((b0 & 0x7F) as u16) | ((b1 as u16) << 7);
        *off += 2;
        Ok(v)
    } else {
        let v = load_u16_le(payload, *off + 1)?;
        *off += 3;
        Ok(v)
    }
}

// ===== ShortVec helpers (bounded) =====
#[inline(always)]
fn parse_shortvec_len_u8(payload: &[u8], off: &mut usize) -> Result<usize, ()> {
    // ShortU16 semantics are used widely; here we bound to 3 bytes max as per ShortU16.
    let b0 = *load_bytes(payload, *off, 1)?.first().ok_or(())?;
    if b0 < 0x80 {
        *off += 1;
        Ok(b0 as usize)
    } else if b0 < 0xFE {
        let b1 = *load_bytes(payload, *off + 1, 1)?.first().ok_or(())?;
        let v = ((b0 & 0x7F) as usize) | ((b1 as usize) << 7);
        *off += 2;
        Ok(v)
    } else {
        let v = load_u16_le(payload, *off + 1)? as usize;
        *off += 3;
        Ok(v)
    }
}

// REPLACE: skip_n_u8
#[inline(always)]
fn skip_n_u8(payload: &[u8], off: &mut usize, n: usize) -> Result<(), ()> {
    // Prove to the verifier we can read n bytes at *off, then advance *off.
    let _ = load_bytes(payload, *off, n)?;
    *off = (*off).saturating_add(n);
    Ok(())
}

#![no_std]
#![no_main]

use aya_bpf::{
    bindings::{xdp_action, TC_ACT_OK, TC_ACT_SHOT},
    helpers::{bpf_ktime_get_ns, bpf_get_prandom_u32},
    macros::{classifier, map, xdp},
    maps::{HashMap, PerfEventArray, Queue, LruHashMap, PerCpuArray, XskMap, ProgramArray},
    programs::{TcContext, XdpContext, ProbeContext},
    BpfContext,
};
use aya_log_ebpf::info;
use core::{mem, mem::size_of};
use memoffset::offset_of;

mod bindings {
    pub use aya_bpf::bindings::*;
}

// REPLACE: is_solana_port
#[inline(always)]
fn is_solana_port(p: u16) -> bool {
    unsafe { TPU_PORTS.get(&p).is_some() } ||
    (p == SOLANA_TPU_PORT || p == SOLANA_TPU_FWD_PORT || p == SOLANA_TPU_VOTE_PORT)
}

// REPLACE: should_deep_parse_udp
#[inline(always)]
fn should_deep_parse_udp(dst_port: u16, payload: &[u8]) -> bool {
    // Solana TPU (QUIC) + enough bytes to safely inspect header structures
    payload.len() >= 24 && is_solana_port(dst_port) && is_quic_like(payload)
}

// ===== Adaptive Bloom (per-CPU rotating) + global LRU =====
const BLOOM_BITS: usize = 4096; // 4k bits per CPU
const BLOOM_WORDS: usize = BLOOM_BITS / 64;

#[repr(C)]
#[derive(Clone, Copy)]
struct Bloom { words: [u64; BLOOM_WORDS], epoch_ns: u64 }

#[map]
static BLOOM_PCPU: PerCpuArray<Bloom> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn hash_mix64(mut x: u64) -> u64 {
    // SplitMix64
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[inline(always)]
fn bloom_indices(h: u64) -> (usize, u64, usize, u64) {
    let h1 = hash_mix64(h);
    let h2 = hash_mix64(h ^ 0x517c_c1b1_c2a3_e4f5);
    let i1 = (h1 as usize) & (BLOOM_BITS - 1);
    let i2 = (h2 as usize) & (BLOOM_BITS - 1);
    (i1 / 64, 1u64 << (i1 & 63), i2 / 64, 1u64 << (i2 & 63))
}

#[inline(always)]
fn bloom_check_set(now: u64, ttl_ns: u64, sig_hash: u64) -> bool {
    unsafe {
        if let Some(b) = BLOOM_PCPU.get_ptr_mut(0) {
            // rotate every ~100ms to prevent saturation
            let rotate = now.wrapping_sub((*b).epoch_ns) > 100_000_000;
            if rotate { (*b).words = [0u64; BLOOM_WORDS]; (*b).epoch_ns = now; }
            let (w1, m1, w2, m2) = bloom_indices(sig_hash);
            let seen = ((*b).words[w1] & m1) != 0 && ((*b).words[w2] & m2) != 0;
            (*b).words[w1] |= m1; (*b).words[w2] |= m2;
            // confirm via LRU TTL
            if let Some(v) = TRANSACTION_CACHE.get(&sig_hash) { if now.wrapping_sub((*v).ts) < ttl_ns { return true; } }
            let v = CacheVal { ts: now }; let _ = TRANSACTION_CACHE.insert(&sig_hash, &v, 0);
            seen
        } else { false }
    }
}

// AF_XDP socket map for zero-copy handoff of hot flows to userspace
#[map]
static XSK_SOCKS: XskMap = XskMap::with_max_entries(64, 0);

// Program array for tail calls (future split: prog0/prefilter, prog1/parse, prog2/score)
#[map]
static PROGS: ProgramArray = ProgramArray::with_max_entries(4, 0);

// Probabilistic priority via LUT (maps into 0..15)
static SIGMOID_LUT: [u8; 17] = [0,1,2,3,5,7,9,11,12,13,14,14,15,15,15,15,15];

#[inline(always)]
fn clamp_i32(x: i32, lo: i32, hi: i32) -> i32 { if x < lo { lo } else if x > hi { hi } else { x } }

#[inline(always)]
fn mev_priority_sigmoid(event: &PacketEvent) -> u8 {
    let mut s: i32 = 0;
    s = s.saturating_add(((event.lamports / 100_000_000) as i32).min(100) * 3);
    s = s.saturating_add(((event.compute_units / 100_000) as i32).min(20) * 4);
    s = s.saturating_add((event.accounts_count as i32).min(32));
    s = s.saturating_add((event.instructions_count as i32).min(16) * 2);
    s = s.saturating_add(match event.mev_type { 4 => 10, 2 => 8, 3 => 6, 1 => 4, _ => 0 } as i32);
    s = clamp_i32(s - 32, -64, 64);
    let idx = ((s + 64) as u32 / 8) as usize;
    SIGMOID_LUT[idx.min(SIGMOID_LUT.len() - 1)]
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

// Shared per-CPU scratch for tail-call pipeline
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Scratch {
    pub dst_port: u16,
    pub src_port: u16,
    pub payload_off: u16,
    pub payload_len: u16,
    pub flags: u8, // bit0: quic-like
}

#[map]
static SCRATCH_PCPU: PerCpuArray<Scratch> = PerCpuArray::with_max_entries(1, 0);

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

// Lightweight event for perf (compact hot path emission)
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PacketEventLite {
    pub ts: u64,
    pub src_ip: u32,
    pub dst_ip: u32,
    pub src_port: u16,
    pub dst_port: u16,
    pub lamports: u64,
    pub cu: u32,
    pub prio: u8,
    pub mev: u8,
    pub flags: u8,
    pub accs: u8,
    pub insts: u8,
    pub hash: u64,
    pub program_id_prefix: [u8; 4],
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
static HOT_HEADERS: PerfEventArray<PacketEventLite> = PerfEventArray::with_max_entries(4096, 0);

#[map]
static FILTER_STATS: PerCpuArray<FilterStats> = PerCpuArray::with_max_entries(1, 0);

#[map]
static PRIORITY_QUEUE: Queue<PacketEvent> = Queue::with_max_entries(2048, 0);

#[map]
static KNOWN_DEX_PROGRAMS: HashMap<[u8; 32], ProgramInfo> = HashMap::with_max_entries(128, 0);

#[repr(C)]
#[derive(Clone, Copy)]
struct CacheVal { ts: u64 }

#[map]
static TRANSACTION_CACHE: LruHashMap<u64, CacheVal> = LruHashMap::with_max_entries(8192, 0);

// Flow-level tagging for bundles via DCID hashing
#[map]
static BUNDLE_FLOWS: LruHashMap<u64, CacheVal> = LruHashMap::with_max_entries(4096, 0);

// ===== Dynamic TPU ports & core program IDs (userspace-populated) =====
#[map]
static TPU_PORTS: HashMap<u16, u8> = HashMap::with_max_entries(512, 0); // key: port, val: 1

// Compute Budget Program IDs (usually one): key = program_id bytes, val = 1
#[map]
static COMPUTE_BUDGET_IDS: HashMap<[u8; 32], u8> = HashMap::with_max_entries(2, 0);

// SPL Token and Token-2022 IDs (optional, for cheap token-op flags)
#[map]
static TOKEN_PROGRAM_IDS: HashMap<[u8; 32], u8> = HashMap::with_max_entries(4, 0);

#[inline(always)]
fn cache_check_and_update(hash: u64, now: u64, ttl_ns: u64) -> bool {
    // returns true if considered duplicate within TTL
    unsafe {
        if let Some(val) = TRANSACTION_CACHE.get(&hash) {
            if now.wrapping_sub((*val).ts) < ttl_ns { return true; }
        }
        let v = CacheVal { ts: now };
        let _ = TRANSACTION_CACHE.insert(&hash, &v, 0);
    }
    false
}

// Program IDs are supplied from userland via KNOWN_DEX_PROGRAMS map.

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
    // Prefer classifier map to allow hot-swappable updates from userland.
    unsafe { KNOWN_DEX_PROGRAMS.get(program_id).is_some() }
}

// ===== QUIC gate: shallow, bounded, verifier-friendly =====
// REPLACE: is_quic_like
#[inline(always)]
fn is_quic_like(payload: &[u8]) -> bool {
    if payload.len() < 7 { return false; }
    let b0 = payload[0];

    // Long header: Header Form=1 and Fixed Bit=1
    if (b0 & 0xC0) == 0xC0 {
        if payload.len() < 12 { return false; }
        let ver = u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]);
        if ver == 0 { return false; } // version negotiation isn't the Solana data path
        let dcid_len = payload[5] as usize;
        if dcid_len == 0 || dcid_len > 20 { return false; }
        let off_scid_len = 6 + dcid_len;
        if off_scid_len >= payload.len() { return false; }
        let scid_len = payload[off_scid_len] as usize;
        if scid_len > 20 { return false; }
        let after_ids = off_scid_len + 1 + scid_len;
        if after_ids >= payload.len() { return false; }
        let pn_len = ((b0 & 0x03) as usize) + 1; // 1..4
        return after_ids + pn_len <= payload.len();
    }

    // Short header: Fixed Bit must be 1
    (b0 & 0x40) == 0x40 && payload.len() >= 5
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

// REPLACE: validate_solana_transaction
#[inline(always)]
fn validate_solana_transaction(payload: &[u8]) -> bool {
    // Minimum practical size (1 sig, header, 1 key, blockhash, 1 empty instruction vec)
    if payload.len() < 96 || payload.len() > MAX_PACKET_SIZE { return false; }
    let mut off = 0usize;

    // signatures ShortVec<u8> count (we cap to <= 8)
    let Ok(sig_cnt) = parse_compact_u16_bounded(payload, &mut off) else { return false; };
    if sig_cnt == 0 || sig_cnt > 8 { return false; }
    let sig_bytes = (sig_cnt as usize).saturating_mul(64);
    if load_bytes(payload, off, sig_bytes).is_err() { return false; }
    off = off.saturating_add(sig_bytes);

    // message version flag byte must exist
    let Ok(b1) = load_bytes(payload, off, 1) else { return false; };
    let first = b1[0];
    off += 1;

    // Legacy or v0
    let versioned = (first & 0x80) != 0;
    if versioned && (first & 0x7F) != 0 { return false; } // only v0 supported

    // Parse common body
    let mut check_body = |offp: &mut usize| -> bool {
        if load_bytes(payload, *offp, 3).is_err() { return false; } *offp += 3; // header
        let Ok(acct_cnt) = parse_compact_u16_bounded(payload, offp) else { return false; };
        if acct_cnt == 0 || acct_cnt as usize > MAX_ACCOUNTS { return false; }
        let keys_len = (acct_cnt as usize).saturating_mul(32);
        if load_bytes(payload, *offp, keys_len).is_err() { return false; } *offp += keys_len;
        if load_bytes(payload, *offp, 32).is_err() { return false; } *offp += 32; // blockhash
        // instructions shortvec
        let Ok(inst_cnt) = parse_compact_u16_bounded(payload, offp) else { return false; };
        // Cheap bounded walk of zero or more instructions
        let mut i = 0usize;
        while i < core::cmp::min(inst_cnt as usize, MAX_INSTRUCTIONS) {
            if load_bytes(payload, *offp, 1).is_err() { return false; } *offp += 1; // program_id_index
            let Ok(al) = parse_compact_u16_bounded(payload, offp) else { return false; };
            if skip_n_u8(payload, offp, al as usize).is_err() { return false; }
            let Ok(dl) = parse_compact_u16_bounded(payload, offp) else { return false; };
            if load_bytes(payload, *offp, dl as usize).is_err() { return false; } *offp += dl as usize;
            i += 1;
        }
        true
    };

    if !versioned {
        if !check_body(&mut off) { return false; }
        return true;
    } else {
        // v0: body then ALT lookups (bounded shortvecs of u8 indices)
        if !check_body(&mut off) { return false; }
        let Ok(alt_cnt) = parse_compact_u16_bounded(payload, &mut off) else { return false; };
        let mut j = 0usize;
        while j < alt_cnt as usize {
            if load_bytes(payload, off, 32).is_err() { return false; } off += 32; // table key
            let Ok(w) = parse_shortvec_len_u8(payload, &mut off) else { return false; };
            if skip_n_u8(payload, &mut off, w).is_err() { return false; }
            let Ok(r) = parse_shortvec_len_u8(payload, &mut off) else { return false; };
            if skip_n_u8(payload, &mut off, r).is_err() { return false; }
            j += 1;
        }
        return true;
    }
}

// REPLACE: extract_transaction_info
#[inline(always)]
fn extract_transaction_info(payload: &[u8]) -> Result<PacketEvent, ()> {
    if payload.len() < 96 { return Err(()); }
    let mut event = unsafe { core::mem::zeroed::<PacketEvent>() };
    let mut off: usize = 0;

    // --- Signatures (ShortVec of 64B sigs) ---
    let sig_count = parse_compact_u16_bounded(payload, &mut off)? as usize;
    if sig_count == 0 || sig_count > 8 { return Err(()); }
    let sig_bytes = sig_count.saturating_mul(64);
    let sigs = load_bytes(payload, off, sig_bytes)?;
    // Copy the first signature into the fixed field (used for dedupe hash)
    unsafe { core::ptr::copy_nonoverlapping(sigs.as_ptr(), event.signature.as_mut_ptr(), 64); }
    off = off.saturating_add(sig_bytes);

    // --- Message version flag ---
    let first = *load_bytes(payload, off, 1)?.first().ok_or(())?;
    let versioned = (first & 0x80) != 0;

    // Common body (legacy or v0)
    #[inline(always)]
    fn parse_body(payload: &[u8], off: &mut usize, event: &mut PacketEvent) -> Result<u64, ()> {
        // Header (3 bytes): num_required_signatures, num_readonly_signed, num_readonly_unsigned
        let _hdr = load_bytes(payload, *off, 3)?; *off += 3;

        // Account keys shortvec
        let acct_count = parse_compact_u16_bounded(payload, off)? as usize;
        if acct_count == 0 || acct_count > MAX_ACCOUNTS { return Err(()); }
        event.accounts_count = acct_count.min(255) as u8;
        let keys_start = *off;
        let keys_len = acct_count.saturating_mul(32);
        let _keys = load_bytes(payload, keys_start, keys_len)?; *off = off.saturating_add(keys_len);

        // Recent blockhash (32B)
        let bh = load_bytes(payload, *off, 32)?; *off += 32;
        let bh_first8 = u64::from_le_bytes([bh[0], bh[1], bh[2], bh[3], bh[4], bh[5], bh[6], bh[7]]);

        // Instructions shortvec
        let inst_count = (parse_compact_u16_bounded(payload, off)? as usize).min(MAX_INSTRUCTIONS);
        event.instructions_count = inst_count.min(255) as u8;

        // Sensible default; ComputeBudget will overwrite when present
        event.compute_units = 200_000;

        // Track first program id we see for coarse classification
        let mut set_program = false;

        // Iterate bounded instructions
        let mut i = 0usize;
        while i < inst_count {
            // program_id_index
            let pid_idx = *load_bytes(payload, *off, 1)?.first().ok_or(())? as usize; *off += 1;

            // account indices (u8 each) with shortvec length
            let acc_len = parse_compact_u16_bounded(payload, off)? as usize;
            skip_n_u8(payload, off, acc_len)?; // just advance; bounded above

            // data length + data
            let data_len = parse_compact_u16_bounded(payload, off)? as usize;
            let data = load_bytes(payload, *off, data_len)?; *off = off.saturating_add(data_len);

            // First program id copy for event.program_id
            if pid_idx < acct_count && !set_program {
                let pid = load_bytes(payload, keys_start + pid_idx * 32, 32)?;
                unsafe { core::ptr::copy_nonoverlapping(pid.as_ptr(), event.program_id.as_mut_ptr(), 32) };
                set_program = true;
            }

            // Compute Budget program fast-path via map (official ID set from userland)
            unsafe {
                if pid_idx < acct_count {
                    let mut pid_arr = [0u8; 32];
                    let pid_slice = load_bytes(payload, keys_start + pid_idx * 32, 32)?;
                    core::ptr::copy_nonoverlapping(pid_slice.as_ptr(), pid_arr.as_mut_ptr(), 32);
                    if COMPUTE_BUDGET_IDS.get(&pid_arr).is_some() && !data.is_empty() {
                        match data[0] {
                            0x02 => { // SetComputeUnitLimit(u32 LE)
                                if data.len() >= 5 {
                                    let cu = u32::from_le_bytes([data[1], data[2], data[3], data[4]]);
                                    event.compute_units = cu.min(2_000_000);
                                }
                            }
                            0x03 => { // SetComputeUnitPrice(u64 LE) in micro-lamports per CU
                                if data.len() >= 9 {
                                    let price_micro = u64::from_le_bytes([data[1],data[2],data[3],data[4],data[5],data[6],data[7],data[8]]);
                                    // Store microLamportsPerCU * CU in expected_profit field as a scratch area.
                                    event.expected_profit = price_micro.saturating_mul(event.compute_units as u64);
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            i += 1;
        }

        Ok(bh_first8)
    }

    if !versioned {
        let bh_first8 = parse_body(payload, &mut off, &mut event)?;
        event.hash = dedupe_key_from_sig_and_bh(&event.signature, bh_first8);
    } else {
        // v0 message (Address Lookup Tables)
        let ver = (first & 0x7F) as u8;
        if ver != 0 { return Err(()); }
        off += 1;
        let bh_first8 = parse_body(payload, &mut off, &mut event)?;

        // Address table lookups (each: 32B table key, writable indices shortvec<u8>, readonly indices shortvec<u8>)
        let alt_len = parse_compact_u16_bounded(payload, &mut off)? as usize;
        let mut j = 0usize;
        while j < alt_len {
            let _table_key = load_bytes(payload, off, 32)?; *off += 32;
            let wlen = parse_shortvec_len_u8(payload, &mut off)?; skip_n_u8(payload, &mut off, wlen)?;
            let rlen = parse_shortvec_len_u8(payload, &mut off)?; skip_n_u8(payload, &mut off, rlen)?;
            j += 1;
        }

        event.hash = dedupe_key_from_sig_and_bh(&event.signature, bh_first8);
    }

    event.packet_type = if is_dex_program(&event.program_id) { 2 } else { 0 };
    event.priority = if event.packet_type == 2 { 10 } else { 5 };
    event.data_len = payload.len().min(u16::MAX as usize) as u16;
    Ok(event)
}

// REPLACE: is_high_value_program
#[inline(always)]
fn is_high_value_program(program_id: &[u8; 32]) -> bool {
    unsafe {
        if let Some(info) = KNOWN_DEX_PROGRAMS.get(program_id) {
            (*info).priority_boost > 0 || (*info).min_lamports > 0
        } else { false }
    }
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

// REPLACE: detect_liquidation
#[inline(always)]
fn detect_liquidation(event: &PacketEvent) -> bool {
    let complex   = event.accounts_count >= 9 && event.instructions_count >= 3;
    let expensive = event.compute_units >= 350_000;
    let token_ops = detect_token_operations(event);
    // Oracle updates often precede liquidation calc; our detection is exact-allowlist based.
    let maybe_oracle_ctx = detect_oracle_update(event);
    (expensive && complex && token_ops) || (expensive && maybe_oracle_ctx)
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

// REPLACE: detect_jito_bundle
#[inline(always)]
fn detect_jito_bundle(_payload: &[u8]) -> bool {
    // Bundles are off-chain packaging/auction; identify via userland context.
    false
}

// REPLACE: mark_bundle_flow
#[inline(always)]
fn mark_bundle_flow(now: u64, payload: &[u8]) -> bool {
    if payload.len() < 12 { return false; }
    let b0 = payload[0];
    if (b0 & 0xC0) != 0xC0 { return false; } // long header only
    let dcid_len = payload[5] as usize;
    if dcid_len == 0 || dcid_len > 20 || 6 + dcid_len > payload.len() { return false; }
    let dcid = &payload[6..6+dcid_len];
    let mut h = 0x517c_c1b1_c2a3_e4f5u64;
    let mut i = 0usize;
    while i < dcid.len() {
        let mut b = [0u8;8];
        let take = core::cmp::min(8, dcid.len() - i);
        b[..take].copy_from_slice(&dcid[i..i+take]);
        let chunk = u64::from_le_bytes(b);
        h = mix64(h ^ chunk);
        i += take;
    }
    unsafe {
        let ttl = 500_000_000u64; // 0.5s
        let present = if let Some(v) = BUNDLE_FLOWS.get(&h) { now.wrapping_sub((*v).ts) < ttl } else { false };
        let v = CacheVal { ts: now }; let _ = BUNDLE_FLOWS.insert(&h, &v, 0);
        present
    }
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
    unsafe {
        if let Some(stats) = FILTER_STATS.get_ptr_mut(0) {
            (*stats).packets_processed = (*stats).packets_processed.wrapping_add(1);
            if event.priority >= 8 { (*stats).packets_filtered = (*stats).packets_filtered.wrapping_add(1); }
            if event.mev_type > 0 { (*stats).mev_opportunities = (*stats).mev_opportunities.wrapping_add(1); }
            if event.lamports > 100_000_000 { (*stats).high_value_txs = (*stats).high_value_txs.wrapping_add(1); }
            if is_dex_program(&event.program_id) { (*stats).defi_interactions = (*stats).defi_interactions.wrapping_add(1); }
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
    
    if payload_len < 7 || payload_len > MAX_PACKET_SIZE {
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

    // QUIC-aware gating via helper
    if !(should_deep_parse_udp(dst_port, payload) && payload.len() >= 176) {
        return Ok(xdp_action::XDP_PASS);
    }

    match extract_transaction_info(payload) {
        Ok(mut event) => {
            event.timestamp = unsafe { bpf_ktime_get_ns() };
            event.src_ip = u32::from_be(unsafe { (*ip_hdr).saddr });
            event.dst_ip = u32::from_be(unsafe { (*ip_hdr).daddr });
            event.src_port = src_port;
            event.dst_port = dst_port;
            
            classify_mev_type(&mut event);
            event.priority = mev_priority_sigmoid(&event);
            event.expected_profit = calculate_expected_profit(&event);
            
            if detect_jito_bundle(payload) {
                event.flags |= 0x01;
                event.priority = event.priority.saturating_add(3).min(15);
            }
            
            // Adaptive Bloom + LRU TTL
            let now = event.timestamp;
            let ttl = get_cache_duration(&event);
            if bloom_check_set(now, ttl, event.hash) { return Ok(xdp_action::XDP_DROP); }

            update_stats(&event);

            // Optional mark bundle flows (DCID-based) for slight bump
            if mark_bundle_flow(now, payload) { event.flags |= 0x01; }

            // Optional AF_XDP fast-path handoff for ultra-hot flows
            let routing = optimize_packet_routing(&event);
            if event.priority >= 13 || routing >= 2 {
                let qid: u32 = (event.hash as u32) & 0x3F; // 64 queues
                let act = unsafe { XSK_SOCKS.redirect(&ctx, qid) };
                if act != 0 {
                    let lite = PacketEventLite { ts: event.timestamp, src_ip: event.src_ip, dst_ip: event.dst_ip, src_port: event.src_port, dst_port: event.dst_port, lamports: event.lamports, cu: event.compute_units, prio: event.priority, mev: event.mev_type, flags: event.flags, accs: event.accounts_count, insts: event.instructions_count, hash: event.hash, program_id_prefix: [event.program_id[0],event.program_id[1],event.program_id[2],event.program_id[3]] };
                    unsafe { HOT_HEADERS.output(&ctx, &lite, 0); }
                    return Ok(act);
                }
            }

            // Emit lightweight header widely; full event for selected packets
            if event.priority >= 8 || event.mev_type > 0 {
                let lite = PacketEventLite { ts: event.timestamp, src_ip: event.src_ip, dst_ip: event.dst_ip, src_port: event.src_port, dst_port: event.dst_port, lamports: event.lamports, cu: event.compute_units, prio: event.priority, mev: event.mev_type, flags: event.flags, accs: event.accounts_count, insts: event.instructions_count, hash: event.hash, program_id_prefix: [event.program_id[0],event.program_id[1],event.program_id[2],event.program_id[3]] };
                unsafe { HOT_HEADERS.output(&ctx, &lite, 0); }
                if event.priority >= 12 {
                    unsafe {
                        let _ = PRIORITY_QUEUE.push(&event, 0);
                        PACKET_EVENTS.output(&ctx, &event, 0);
                    }
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
    
    if payload_len < 7 || payload_len > MAX_PACKET_SIZE {
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

    // QUIC-aware gating for TC path as well
    let is_tpu = dst_port == SOLANA_TPU_PORT || dst_port == SOLANA_TPU_FWD_PORT || dst_port == SOLANA_TPU_VOTE_PORT;
    if !(is_tpu && is_quic_like(payload) && payload.len() >= 176) {
        return Ok(TC_ACT_OK);
    }

    match extract_transaction_info(payload) {
        Ok(mut event) => {
            event.timestamp = unsafe { bpf_ktime_get_ns() };
            event.src_ip = u32::from_be(unsafe { (*ip_hdr).saddr });
            event.dst_ip = u32::from_be(unsafe { (*ip_hdr).daddr });
            event.src_port = src_port;
            event.dst_port = dst_port;
            
            classify_mev_type(&mut event);
            event.priority = mev_priority_sigmoid(&event);
            event.expected_profit = calculate_expected_profit(&event);
            
            if detect_jito_bundle(payload) {
                event.flags |= 0x01;
                event.priority = event.priority.saturating_add(3).min(15);
            }
            
            let now = event.timestamp; let ttl = get_cache_duration(&event);
            if is_outbound && bloom_check_set(now, ttl, event.hash) { return Ok(TC_ACT_SHOT); }

            update_stats(&event);

            // Smooth priority and emit telemetry
            classify_mev_type(&mut event);
            event.priority = mev_priority_sigmoid(&event);
            if event.priority >= 8 || event.mev_type > 0 {
                let lite = PacketEventLite { ts: event.timestamp, src_ip: event.src_ip, dst_ip: event.dst_ip, src_port: event.src_port, dst_port: event.dst_port, lamports: event.lamports, cu: event.compute_units, prio: event.priority, mev: event.mev_type, flags: event.flags, accs: event.accounts_count, insts: event.instructions_count, hash: event.hash, program_id_prefix: [event.program_id[0],event.program_id[1],event.program_id[2],event.program_id[3]] };
                unsafe { HOT_HEADERS.output(&ctx, &lite, 0); }
                if event.priority >= 13 {
                    unsafe {
                        let _ = PRIORITY_QUEUE.push(&event, 0);
                        PACKET_EVENTS.output(&ctx, &event, 0);
                    }
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

// REPLACE: estimate_gas_priority_fee
#[inline(always)]
fn estimate_gas_priority_fee(event: &PacketEvent) -> u64 {
    let micro_total = event.expected_profit as u128; // CU_limit * price_microLamports
    ((micro_total + 999_999u128) / 1_000_000u128) as u64
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

// REPLACE: get_cache_duration
#[inline(always)]
fn get_cache_duration(event: &PacketEvent) -> u64 {
    // Base by MEV class (ns)
    let base = match event.mev_type {
        3 => 5_000_000_000,  // sandwich
        2 => 3_000_000_000,  // arbitrage
        4 => 8_000_000_000,  // liquidation, slightly lowered from 10s
        1 => 2_000_000_000,  // dex trade
        _ => 1_000_000_000,
    };
    // Fee-aligned bump: payers deserve longer dedupe window within the blockhash horizon
    let prio = estimate_gas_priority_fee(event);
    let fee_bump = if prio >= 100_000 { 2_000_000_000 }
                   else if prio >= 30_000 { 1_000_000_000 }
                   else { 0 };
    // Hard cap well under ~60–90s recent-blockhash horizon (keep dup TTLs tight)
    (base + fee_bump).min(9_000_000_000)
}

// REPLACE: validate_signature
#[inline(always)]
fn validate_signature(signature: &[u8; 64]) -> bool {
    let mut nz = 0u8;
    let mut x: u32 = 0;
    let mut y: u32 = 0;
    let mut i = 0usize;
    while i < 64 {
        let b = signature[i];
        nz += (b != 0) as u8;
        x = x.wrapping_add(b as u32);
        y ^= (b as u32) << (i & 7);
        i += 1;
    }
    // Most bytes non-zero and not trivially structured
    nz >= 48 && x > 2000 && (y & 0xFFFF) != 0
}

// REPLACE: detect_token_operations
#[inline(always)]
fn detect_token_operations(event: &PacketEvent) -> bool {
    unsafe { TOKEN_PROGRAM_IDS.get(&event.program_id).is_some() }
}

// REPLACE: detect_oracle_update
#[inline(always)]
fn detect_oracle_update(event: &PacketEvent) -> bool {
    // Exact program match + typical update shape
    unsafe { ORACLE_WHITELIST.get(&event.program_id).is_some() }
        && event.lamports == 0
        && event.accounts_count <= 6
        && event.compute_units >= 150_000
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

// REPLACE: should_drop_packet
#[inline(always)]
fn should_drop_packet(event: &PacketEvent) -> bool {
    if event.accounts_count == 0 || event.instructions_count == 0 { return true; }
    if !validate_program_id(&event.program_id) { return true; }
    if !validate_signature(&event.signature) { return true; }
    let trivial = event.lamports < 1_000_000
        && event.compute_units < 50_000
        && event.instructions_count == 1
        && !is_dex_program(&event.program_id);
    trivial
}

// REPLACE: estimate_gas_cost
#[inline(always)]
fn estimate_gas_cost(event: &PacketEvent) -> u64 {
    let base_fee_per_sig = 5_000u64; // lamports per signature
    base_fee_per_sig.saturating_add(estimate_gas_priority_fee(event))
}

// REPLACE: optimize_packet_routing
#[inline(always)]
fn optimize_packet_routing(event: &PacketEvent) -> u8 {
    if event.flags & 0x40 != 0 { return 3; }
    let prio_fee = estimate_gas_priority_fee(event);
    if event.mev_type == 3 || prio_fee >= 100_000 { return 3; }
    if event.mev_type == 2 || event.priority >= 13 || prio_fee >= 30_000 { return 2; }
    if event.mev_type > 0 || is_dex_program(&event.program_id) || prio_fee >= 10_000 { return 1; }
    0
}

// REPLACE: detect_flash_loan
#[inline(always)]
fn detect_flash_loan(event: &PacketEvent) -> bool {
    // Structural, map-free, prefix-free
    let high_compute = event.compute_units > 500_000;
    let complex      = event.instructions_count >= 4 && event.accounts_count >= 10;
    let high_value   = event.lamports > 500_000_000; // >= 0.5 SOL
    let token_ops    = detect_token_operations(event); // runtime map-backed
    (high_compute && complex) && (high_value || token_ops)
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
