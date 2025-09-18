// ===== Per-CPU "now" cache for this packet =====
#[repr(C, align(64))]
struct NowPc { ts: u64 }

#[map]
static NOW_PCPU: PerCpuArray<NowPc> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn now_cached_set(ts: u64) {
    unsafe { if let Some(n) = NOW_PCPU.get_ptr_mut(0) { (*n).ts = ts; } }
}
#[inline(always)]
fn now_cached() -> u64 {
    unsafe { NOW_PCPU.get_ptr(0).map(|p| unsafe { (*p).ts }).unwrap_or_else(|| unsafe { bpf_ktime_get_ns() }) }
}

// ===== Per-CPU "cpu id" cache for this packet =====
#[repr(C, align(64))]
struct CpuPc { id: u32 }

#[map]
static CPU_PCPU: PerCpuArray<CpuPc> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn cpu_cached_set(id: u32) {
    unsafe { if let Some(c) = CPU_PCPU.get_ptr_mut(0) { (*c).id = id; } }
}
#[inline(always)]
fn cpu_cached() -> u32 {
    unsafe { CPU_PCPU.get_ptr(0).map(|p| unsafe { (*p).id }).unwrap_or_else(|| unsafe { bpf_get_smp_processor_id() as u32 }) }
}

// ===== Per-CPU config snapshot (5ms) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct CfgSnap { ts: u64, qmask: u32, strat: u32, min_fee_per_cu: u32, drop_enable: u8, _pad: [u8;3] }

#[map]
static CFG_SNAP_PCPU: PerCpuArray<CfgSnap> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn cfg_snapshot(now: u64) -> CfgSnap {
    unsafe {
        if let Some(s) = CFG_SNAP_PCPU.get_ptr_mut(0) {
            if now.wrapping_sub((*s).ts) < 5_000_000 { return *s; }

            let mut snap = CfgSnap { ts: now, qmask: 0x3F, strat: 0x04, min_fee_per_cu: 1, drop_enable: 0, _pad: [0;3] };
            if let Some(x) = XSK_CFG.get(0) {
                snap.qmask = unsafe { (*x).qmask };
                snap.strat = unsafe { (*x).strategy };
            }
            if let Some(f) = FEE_CFG.get(0) {
                snap.min_fee_per_cu = unsafe { (*f).min_fee_per_cu };
            }
            if let Some(d) = DROP_CFG.get(0) {
                snap.drop_enable = unsafe { (*d).enable };
            }
            (*s) = snap;
            return snap;
        }
    }
    CfgSnap { ts: now, qmask: 0x3F, strat: 0x04, min_fee_per_cu: 1, drop_enable: 0, _pad: [0;3] }
}

// ===== Per-CPU "heat" cache for this packet =====
#[repr(C, align(64))]
struct HeatPc { v: u32 }

#[map]
static HEAT_PCPU: PerCpuArray<HeatPc> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn heat_cached_set(v: u32) {
    unsafe { if let Some(h) = HEAT_PCPU.get_ptr_mut(0) { (*h).v = v; } }
}
#[inline(always)]
fn heat_cached() -> u32 {
    unsafe { HEAT_PCPU.get_ptr(0).map(|p| unsafe { (*p).v }).unwrap_or(0) }
}

// ===== Per-CPU lamports-per-byte EWMA =====
#[repr(C)]
#[derive(Clone, Copy)]
struct Lpb { ts: u64, ew: u64 } // ew scaled ×16 (lamports per byte ×16)

#[map]
static LPB_PCPU: PerCpuArray<Lpb> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn lpb_update_and_floor(now: u64, lamports: u64, bytes: u32) -> u64 {
    unsafe {
        if let Some(l) = LPB_PCPU.get_ptr_mut(0) {
            let mut ew = (*l).ew;
            // decay every 100ms up to 32 steps
            let mut steps = (now.wrapping_sub((*l).ts) / 100_000_000) as u32; if steps > 32 { steps = 32; }
            let mut i = 0u32; while i < steps { ew = ew.saturating_sub(ew >> 3); i += 1; }

            let b = (bytes as u64).max(1);
            // contribute ~12.5% current sample; scale ×16 to keep precision
            let sample = ((lamports << 4) / b).min(10_000_000_000u64); // cap
            ew = ew.saturating_add(sample >> 3);

            (*l).ew = ew; (*l).ts = now;

            // floor = (ew/16) * 1/8  → divide by 128 overall
            (ew >> 7).clamp(2_000, 200_000) // lamports/byte floor (2k..200k)
        } else { 2_000 }
    }
}

#[inline(always)]
fn lpb_underfloor_drop(ev: &PacketEvent) -> bool {
    let heat = heat_cached();
    if heat <= 50_000 { return false; }
    let now = now_cached();
    let floor_lp_per_b = lpb_update_and_floor(now, ev.lamports, ev.data_len as u32);
    // compare without division: lamports < floor * bytes / 2 (slack)
    let lhs = (ev.lamports as u128) << 1;
    let rhs = (floor_lp_per_b as u128) * (ev.data_len as u128);
    lhs < rhs
}

// ===== Per-CPU negative port cache (allowlist misses) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct PortNeg { k: [u16;8], ts: [u64;8] }

#[map]
static PORT_NEG_PCPU: PerCpuArray<PortNeg> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn port_neg_hit(p: u16) -> bool {
    unsafe {
        if let Some(n) = PORT_NEG_PCPU.get_ptr(0) {
            let mut i = 0usize; while i < 8 { if (*n).k[i] == p { return true; } i += 1; }
        }
    }
    false
}
#[inline(always)]
fn port_neg_set(p: u16) {
    unsafe {
        if let Some(n) = PORT_NEG_PCPU.get_ptr_mut(0) {
            let now = now_cached();
            let mut idx = 0usize; let mut o = (*n).ts[0];
            let mut i = 1usize; while i < 8 {
                if now.wrapping_sub((*n).ts[i]) > now.wrapping_sub(o) { idx = i; o = (*n).ts[i]; }
                i += 1;
            }
            (*n).k[idx] = p; (*n).ts[idx] = now;
        }
    }
}

// ===== Per-queue starvation timestamps (last seen dry) =====
#[map]
static QSTARVE_TS: Array<u64> = Array::with_max_entries(64, 0); // supports qid 0..63
#[inline(always)]
fn mark_starved(qid: u32, now: u64) {
    unsafe { if let Some(p) = QSTARVE_TS.get_ptr_mut(qid) { *p = now; } }
}
#[inline(always)]
fn starved_recent(qid: u32, now: u64) -> bool {
    unsafe { QSTARVE_TS.get(qid).map(|t| now.wrapping_sub(*t) < 300_000).unwrap_or(false) }
}

// ===== Per-CPU short flow ban (2 entries) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct Ban2 { k1: u64, t1: u64, k2: u64, t2: u64 }

#[map]
static BAN_PCPU: PerCpuArray<Ban2> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn ban_flow_short(fk: u64) {
    unsafe {
        if let Some(b) = BAN_PCPU.get_ptr_mut(0) {
            let now = now_cached();
            // replace older slot
            let use1 = now.wrapping_sub((*b).t1) > now.wrapping_sub((*b).t2);
            if use1 { (*b).k1 = fk; (*b).t1 = now; } else { (*b).k2 = fk; (*b).t2 = now; }
        }
    }
}
#[inline(always)]
fn is_banned_flow(fk: u64) -> bool {
    unsafe {
        if let Some(b) = BAN_PCPU.get_ptr(0) {
            let now = now_cached();
            return ((*b).k1 == fk && now.wrapping_sub((*b).t1) < 400_000)
                || ((*b).k2 == fk && now.wrapping_sub((*b).t2) < 400_000);
        }
    }
    false
}

// ===== Cross-burst DCID→queue persistence (LRU) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct QPin { qid: u32, ts: u64 }

#[map]
static FLOW_QPIN: LruHashMap<u64, QPin> = LruHashMap::with_max_entries(65536, 0);

// ===== Escalation on repeated per-flow bans by src /24 =====
#[repr(C)]
#[derive(Clone, Copy)]
struct Esc24 { ts: u64, cnt: u8 }

#[map]
static BAN_SRC24: LruHashMap<u32, Esc24> = LruHashMap::with_max_entries(32768, 0);

#[inline(always)]
fn src24_key(ip_be: u32) -> u32 { ip_be & 0xFFFF_FF00 }

#[inline(always)]
fn ban_src24_touch(ip_be: u32) {
    let k = src24_key(ip_be);
    let now = now_cached();
    let mut st = Esc24 { ts: now, cnt: 0 };
    unsafe { if let Some(v) = BAN_SRC24.get(&k) { st = *v; } }
    let gap = now.wrapping_sub(st.ts);
    if gap > 1_000_000 { st.cnt = 0; } // reset after 1ms
    st.cnt = st.cnt.saturating_add(1);
    st.ts = now;
    unsafe { let _ = BAN_SRC24.insert(&k, &st, 0); }
}

#[inline(always)]
fn ban_src24_drop(ip_be: u32) -> bool {
    let heat = heat_cached();
    if heat <= 80_000 { return false; }
    let k = src24_key(ip_be);
    unsafe {
        if let Some(v) = BAN_SRC24.get(&k) {
            // ≥4 bans in last ~1ms → drop new arrivals for ~500µs
            let now = now_cached();
            return (*v).cnt >= 4 && now.wrapping_sub((*v).ts) < 500_000;
        }
    }
    false
}

// ===== Optional UMEM pressure (0..100) fed by userspace =====
#[map]
static UMEM_PRESSURE: Array<u32> = Array::with_max_entries(1, 0);

#[inline(always)]
fn umem_pressure() -> u32 {
    unsafe { UMEM_PRESSURE.get(0).copied().unwrap_or(0) }
}

// ===== Per-CPU flow salt with periodic reseed =====
#[map]
static SALT_PCPU: PerCpuArray<u64> = PerCpuArray::with_max_entries(1, 0);

// ===== SCRATCH prefix8 cache (read once, reuse everywhere) =====
#[inline(always)]
fn prog_prefix8_cached(event: &PacketEvent) -> u64 {
    unsafe {
        if let Some(s) = SCRATCH_PCPU.get_ptr(0) {
            let p = (*s).prog_pref8;
            if p != 0 { return p; }
        }
    }
    // fallback: compute once, stash
    let p = u64::from_le_bytes([
        event.program_id[0],event.program_id[1],event.program_id[2],event.program_id[3],
        event.program_id[4],event.program_id[5],event.program_id[6],event.program_id[7]
    ]);
    unsafe { if let Some(s) = SCRATCH_PCPU.get_ptr_mut(0) { (*s).prog_pref8 = p; } }
    p
}

// ===== Per-CPU admission cut with hysteresis (5ms cache) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct AdmCut { ts: u64, cut: u16 }

#[map]
static ADMCUT_PCPU: PerCpuArray<AdmCut> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn admit_hysteresis(bucket: u16) -> bool {
    let now = now_cached();
    let (p80, p95, _) = pctls(now);
    let target = p80; // base cut (cheap pre-score)
    unsafe {
        if let Some(a) = ADMCUT_PCPU.get_ptr_mut(0) {
            // refresh every 5ms; move up fast, down slow
            if now.wrapping_sub((*a).ts) >= 5_000_000 {
                let prev = (*a).cut;
                let want = target;
                let next = if want > prev {
                    want // rise immediately
                } else {
                    // decay 1 bucket per refresh until we meet target
                    if prev > 0 { prev - 1 } else { 0 }
                };
                (*a).cut = next; (*a).ts = now;
            }
            return bucket >= (*a).cut.saturating_sub(4); // small grace below cut
        }
    }
    bucket >= target.saturating_sub(4)
}

// ===== Per-CPU 4-way×64-set signature cache (blockhash-scoped) =====
const SIGDM_SETS: usize = 64;
const SIGDM_WAYS: usize = 4;

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct SigDM4 {
    key: [[u64; SIGDM_WAYS]; SIGDM_SETS],
    ts:  [[u64; SIGDM_WAYS]; SIGDM_SETS],
    bh8_epoch: u64, // new: current-blockhash scope
}

#[map]
static SIGDM_PCPU: PerCpuArray<SigDM4> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn sigdm_hit_or_set(now: u64, h: u64) -> bool {
    let set = (h as usize) & (SIGDM_SETS - 1);
    let cur_bh = unsafe {
        SCRATCH_PCPU.get_ptr(0).map(|s| unsafe { (*s).bh8 }).unwrap_or(0)
    };
    unsafe {
        if let Some(t) = SIGDM_PCPU.get_ptr_mut(0) {
            // epoch mismatch → treat as cold (no hit), refresh scope
            let epoch_miss = (*t).bh8_epoch != cur_bh;
            if epoch_miss { (*t).bh8_epoch = cur_bh; }

            if !epoch_miss {
                let ks = &mut (*t).key[set];
                let ts = &mut (*t).ts[set];
                let mut w = 0usize;
                while w < SIGDM_WAYS {
                    if ks[w] == h { ts[w] = now; return true; }
                    w += 1;
                }
                // miss: replace stalest way
                let mut idx = 0usize; let mut oldest = ts[0];
                w = 1;
                while w < SIGDM_WAYS {
                    if now.wrapping_sub(ts[w]) > now.wrapping_sub(oldest) { idx = w; oldest = ts[w]; }
                    w += 1;
                }
                ks[idx] = h; ts[idx] = now;
                return false;
            } else {
                // cold scope: populate set directly
                let ts = &mut (*t).ts[set];
                let ks = &mut (*t).key[set];
                // choose stalest lane in the fresh scope (usually zeros)
                let mut idx = 0usize; let mut oldest = ts[0];
                let mut w = 1usize;
                while w < SIGDM_WAYS {
                    if now.wrapping_sub(ts[w]) > now.wrapping_sub(oldest) { idx = w; oldest = ts[w]; }
                    w += 1;
                }
                ks[idx] = h; ts[idx] = now;
                return false;
            }
        }
    }
    false
}

// ===== Per-CPU 2-entry telemetry dedup for HOT_HEADERS =====
#[repr(C)]
#[derive(Clone, Copy)]
struct Tele2 { k1: u64, t1: u64, k2: u64, t2: u64 }

#[map]
static TELE_PCPU: PerCpuArray<Tele2> = PerCpuArray::with_max_entries(1, 0);

// ===== Per-CPU last seen BH (for micro-flush triggers) =====
#[map]
static BHEPOCH_PCPU: PerCpuArray<u64> = PerCpuArray::with_max_entries(1, 0);

// ===== Per-CPU sigtiny bitset (rotate 1ms) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct SigTiny { words: [u64; 4], epoch_ns: u64 }

#[map]
static SIGTINY_PCPU: PerCpuArray<SigTiny> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn sigtiny_hit_or_set(now: u64, h: u64) -> bool {
    unsafe {
        if let Some(s) = SIGTINY_PCPU.get_ptr_mut(0) {
            if now.wrapping_sub((*s).epoch_ns) > 1_000_000 { (*s).words = [0;4]; (*s).epoch_ns = now; }
            let salt = (flow_salt() as u64) ^ bloom_slot_salt(now);
            let i1 = ((h ^ salt)        & 0x3F) as usize; // 0..63
            let i2 = (((h >> 13) ^ salt) & 0x3F) as usize;
            let (w1, m1) = (i1 >> 6, 1u64 << (i1 & 63));
            let (w2, m2) = (i2 >> 6, 1u64 << (i2 & 63));
            let hit = ((*s).words[w1] & m1) != 0 && ((*s).words[w2] & m2) != 0;
            if !hit { (*s).words[w1] |= m1; (*s).words[w2] |= m2; }
            hit
        } else { false }
    }
}

#[inline(always)]
fn bh_edge_microflush(bh8: u64) {
    unsafe {
        if let Some(p) = BHEPOCH_PCPU.get_ptr_mut(0) {
            if *p == bh8 { return; }
            *p = bh8;
            // Force lazy rotation on next touch (epoch_ns=0) with jitter to avoid synchronous cold-start
            let j = jitter_ns_small();
            if let Some(b) = BLOOM_PCPU.get_ptr_mut(0)    { (*b).epoch_ns = now_cached().saturating_sub(j); (*b).words = [0; BLOOM_WORDS]; (*b).set_ctr = 0; }
            if let Some(b) = SIGSLOW_PCPU.get_ptr_mut(0)  { (*b).epoch_ns = now_cached().saturating_sub(j); (*b).words = [0; SB_WORDS]; (*b).set_ctr = 0; }
            if let Some(b) = IFP_BLOOM_PCPU.get_ptr_mut(0){ (*b).epoch_ns = now_cached().saturating_sub(j); (*b).words = [0; IFP_WORDS]; (*b).set_ctr = 0; }
            if let Some(b) = AIF_BLOOM_PCPU.get_ptr_mut(0){ (*b).epoch_ns = now_cached().saturating_sub(j); (*b).words = [0; AIF_WORDS]; (*b).set_ctr = 0; }
            if let Some(b) = AP_BLOOM_PCPU.get_ptr_mut(0) { (*b).epoch_ns = now_cached().saturating_sub(j); (*b).words = [0; AP_WORDS]; (*b).set_ctr = 0; }
        }
    }
}

#[inline(always)]
fn early_slot_freeze_drop(event: &PacketEvent) -> bool {
    let (s_ns, _p80, p95, _p97) = slot_snapshot();
    let phase = now_cached() % s_ns;
    if phase >= (s_ns / 64) { return false; }              // ~1.56% of slot
    if is_vip_dst_fast(event.dst_ip.to_be()) { return false; }
    if is_top01_score(event) { return false; }
    let ps = cheap_pre_score(event);
    let psb = score_to_bucket_fast(ps);
    psb + 2 < p95 // require near-p95 right at slot start
}

#[inline(always)]
fn early_pair_spam_drop() -> bool {
    let (s_ns, _, _, _) = slot_snapshot();
    (now_cached() % s_ns) < (s_ns / 64) // ~1.56% at head
}

#[inline(always)]
fn quic_short_min_len_ok(payload: &[u8]) -> bool {
    if payload.len() < 22 { return false; } // 1(b0)+1(pn)+16(sample)+4(min ciphertext) conservative
    true
}

#[inline(always)]
fn tele_key_lite(l: &PacketEventLite) -> u64 {
    // tiny fingerprint: (dst_ip, dst_port, flags, prio) + first 8B of sig if you carry it in lite
    (l.dst_ip as u64) ^ ((l.dst_port as u64) << 32) ^ ((l.flags as u64) << 48) ^ ((l.priority as u64) << 56)
}

#[inline(always)]
fn tele2_should_emit(now: u64, k: u64) -> bool {
    unsafe {
        if let Some(s) = TELE_PCPU.get_ptr_mut(0) {
            // 200µs window
            if (*s).k1 == k && now.wrapping_sub((*s).t1) < 200_000 { return false; }
            if (*s).k2 == k && now.wrapping_sub((*s).t2) < 200_000 { return false; }
            // replace older
            let use1 = now.wrapping_sub((*s).t1) > now.wrapping_sub((*s).t2);
            if use1 { (*s).k1 = k; (*s).t1 = now; } else { (*s).k2 = k; (*s).t2 = now; }
        }
    }
    true
}

// ===== Solana header plausibility fence =====
#[inline(always)]
fn solana_header_plausible(num_req: u8, ro_signed: u8, ro_unsigned: u8, accounts_len: u16) -> bool {
    // Typical mainnet envelopes: req sigs 1..16, readonly <= accounts_len, sums bounded
    if num_req == 0 || num_req > 16 { return false; }
    let a = accounts_len as u32;
    let rs = ro_signed as u32; let ru = ro_unsigned as u32;
    (rs <= a) & (ru <= a) & ((rs + ru) <= a)
}

// ===== Per-CPU per-packet map-ops budget =====
#[repr(C)]
#[derive(Clone, Copy)]
struct MapBudget { left: u8, ts: u64 }

#[map]
static MAPBUD_PCPU: PerCpuArray<MapBudget> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn map_budget_reset(now: u64, heat: u32) {
    let base: u8 = if heat > 80_000 { 12 } else if heat > 50_000 { 20 } else { 255 };
    unsafe { if let Some(m) = MAPBUD_PCPU.get_ptr_mut(0) { (*m).left = base; (*m).ts = now; } }
}
#[inline(always)]
fn map_budget_take(n: u8) -> bool {
    unsafe {
        if let Some(m) = MAPBUD_PCPU.get_ptr_mut(0) {
            let l = (*m).left;
            if l < n { return false; }
            (*m).left = l - n; return true;
        }
    }
    true
}

// ===== Per-CPU VIP IPv4 micro-caches (pos/neg) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct Vip4L0 { k: [u32;4], ts: [u64;4] }

#[map]
static VIP4_POS_PCPU: PerCpuArray<Vip4L0> = PerCpuArray::with_max_entries(1, 0);
#[map]
static VIP4_NEG_PCPU: PerCpuArray<Vip4L0> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn vip4_cache_hit(set: bool, ip_be: u32) -> bool {
    unsafe {
        let m = if set { VIP4_POS_PCPU.get_ptr_mut(0) } else { VIP4_NEG_PCPU.get_ptr_mut(0) };
        if let Some(c) = m {
            let now = now_cached();
            let mut i = 0usize;
            while i < 4 {
                if (*c).k[i] == ip_be {
                    (*c).ts[i] = now;
                    return true;
                }
                i += 1;
            }
        }
    }
    false
}
#[inline(always)]
fn vip4_cache_set(set: bool, ip_be: u32) {
    unsafe {
        let m = if set { VIP4_POS_PCPU.get_ptr_mut(0) } else { VIP4_NEG_PCPU.get_ptr_mut(0) };
        if let Some(c) = m {
            let now = now_cached();
            // insert into stalest slot
            let mut idx = 0usize; let mut oldest = (*c).ts[0];
            let mut i = 1usize;
            while i < 4 {
                if now.wrapping_sub((*c).ts[i]) > now.wrapping_sub(oldest) { idx = i; oldest = (*c).ts[i]; }
                i += 1;
            }
            (*c).k[idx] = ip_be; (*c).ts[idx] = now;
        }
    }
}

#[inline(always)]
fn is_vip_dst_fast(dst_ip_be: u32) -> bool {
    // L0 pos/neg check
    if vip4_cache_hit(true, dst_ip_be) { return true; }
    if vip4_cache_hit(false, dst_ip_be) { return false; }

    // Fallback maps (32/24/16) once; remember outcome
    let yes = unsafe {
        VIP_DST32.get(&dst_ip_be).is_some()
            || VIP_DST24.get(&(dst_ip_be & 0xFFFF_FF00)).is_some()
            || VIP_DST16.get(&(dst_ip_be & 0xFFFF_0000)).is_some()
    };
    vip4_cache_set(yes, dst_ip_be);
    yes
}

// ===== Fast UDP checksum screen =====
#[inline(always)]
fn udp_len_header_ok(ip: *const IpHdr, udp: *const UdpHdr) -> bool {
    // Already enforce tot/ulen equality; this adds a tiny extra guard on very small frames
    let tot = u16::from_be(unsafe { (*ip).tot_len }) as usize;
    let ulen = u16::from_be(unsafe { (*udp).len }) as usize;
    (tot >= (IP_HLEN + UDP_HLEN)) & (ulen >= UDP_HLEN)
}

// ===== QUIC short-header minimal ciphertext sample check =====
#[inline(always)]
fn quic_short_has_sample(payload: &[u8]) -> bool {
    if payload.len() < 2 { return false; }
    let b0 = payload[0];
    // short header (0b0100_0000..0b0100_0011)
    if (b0 & 0x40) == 0 { return false; }
    let pnlen = ((b0 & 0x03) as usize) + 1; // 1..4
    // need at least 1(b0)+pnlen and a 16B sample after PN (for HP), conservative
    let need = 1 + pnlen + 16;
    payload.len() >= need
}

// ===== Safer early QUIC long-header DCID len fence =====
#[inline(always)]
fn quic_long_dcid_min_ok(payload: &[u8]) -> bool {
    if payload.len() < 7 { return false; }
    let b0 = payload[0];
    if (b0 & 0xC0) != 0xC0 { return false; } // not long
    // Long header: [flags|ver|DCID len|DCID...]; require DCID len >= 8
    let dcid_len = payload[5] as usize;
    dcid_len >= 8 && 6 + dcid_len <= payload.len()
}

// ===== Urgency bonus (time-to-slot-end) in hardened score =====
#[inline(always)]
fn urgency_bonus(now: u64) -> u32 {
    let s = slot_ns();
    let phase = now % s;
    // piecewise without divides
    if phase > (s - (s / 16)) { 1600 }     // last ~6.25%
    else if phase > (s - (s / 8)) { 900 }  // last ~12.5%
    else if phase > (s - (s / 4)) { 400 }  // last ~25%
    else { 0 }
}

// ===== Cache effective CU per packet =====
#[inline(always)]
fn cu_eff_cached(event: &PacketEvent) -> u32 {
    unsafe {
        if let Some(s) = SCRATCH_PCPU.get_ptr(0) {
            let c = (*s).cu_eff;
            if c != 0 { return c; }
        }
    }
    let v = effective_compute_units_for_scoring(event);
    unsafe { if let Some(s) = SCRATCH_PCPU.get_ptr_mut(0) { (*s).cu_eff = v; } }
    v
}

// ===== QUIC Retry & 0-RTT long-header drop =====
#[inline(always)]
fn quic_long_type(b0: u8) -> u8 { (b0 >> 4) & 0x3 } // 0=Initial,1=0-RTT,2=Handshake,3=Retry

#[inline(always)]
fn quic_drop_retry_or_0rtt(payload: &[u8]) -> bool {
    if payload.len() < 1 { return false; }
    let b0 = payload[0];
    if (b0 & 0xC0) != 0xC0 { return false; }
    let t = quic_long_type(b0);
    t == 1 || t == 3
}

// ===== /20 limiter alongside /24 (heat-gated) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct Esc20 { ts: u64, tok: u8 }

#[map]
static LIM_SRC20: LruHashMap<u32, Esc20> = LruHashMap::with_max_entries(32768, 0);

#[inline(always)]
fn src20_key(ip_be: u32) -> u32 { ip_be & 0xFFFF_F000 }

#[inline(always)]
fn src20_drop(now: u64, ip_be: u32) -> bool {
    if heat_cached() <= 80_000 { return false; }
    let k = src20_key(ip_be);
    let mut st = Esc20 { ts: now, tok: 8 };
    unsafe { if let Some(v) = LIM_SRC20.get(&k) { st = *v; } }
    let gap = now.wrapping_sub(st.ts);
    if gap >= 500_000 {
        let add = ((gap / 500_000) as u8).saturating_mul(2);
        st.tok = (st.tok as u16 + add as u16).min(8) as u8;
        st.ts = now;
    }
    if st.tok == 0 { unsafe { let _ = LIM_SRC20.insert(&k, &st, 0); } return true; }
    st.tok -= 1; st.ts = now;
    unsafe { let _ = LIM_SRC20.insert(&k, &st, 0); }
    false
}

// ===== Percentiles & slot params one-shot per packet (micro-cache) =====
#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct SlotSnap { ts: u64, s_ns: u64, p80: u16, p95: u16, p97: u16 }

#[map]
static SLOT_SNAP_PCPU: PerCpuArray<SlotSnap> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn slot_snapshot() -> (u64, u16, u16, u16) {
    let now = now_cached();
    unsafe {
        if let Some(ss) = SLOT_SNAP_PCPU.get_ptr_mut(0) {
            if now.wrapping_sub((*ss).ts) < 5_000_000 {
                return ((*ss).s_ns, (*ss).p80, (*ss).p95, (*ss).p97);
            }
            let s = slot_ns();
            let (p80, p95, p97) = pctls(now);
            (*ss).ts = now; (*ss).s_ns = s; (*ss).p80 = p80; (*ss).p95 = p95; (*ss).p97 = p97;
            return (s, p80, p95, p97);
        }
    }
    (slot_ns(), hist_percentile(80), hist_percentile(95), hist_percentile(97))
}

// ===== L2/IP frame length fence =====
#[inline(always)]
fn frame_len_ok(ctx: &XdpContext, l2off: usize, ip_tot_len: usize) -> bool {
    let frame = unsafe { ctx.data_end().offset_from(ctx.data_ptr()) } as isize;
    if frame <= 0 { return false; }
    let plen = frame as usize;
    l2off.saturating_add(ip_tot_len) <= plen
}

// ===== Top-0.1% alpha shield =====
#[inline(always)]
fn is_top01_score(event: &PacketEvent) -> bool {
    let now = now_cached();
    let (_, p95, p97) = pctls(now);
    let cur = score_to_bucket(cheap_pre_score(event));
    // approximate p99.9 ≈ p99 + (p99 - p97); use p95 as lower anchor if p97==p95
    let p99 = hist_percentile(99);
    let step = p99.saturating_sub(p97.max(p95));
    cur >= p99.saturating_add(step)
}

// ===== VLAN PCP bias configuration =====
#[repr(C)]
#[derive(Clone, Copy)]
struct VlanPcpCfg { enable: u8, min_pcp: u8 } // e.g., >=4 gets CPU-local bias
#[map]
static VLAN_PCP_CFG: Array<VlanPcpCfg> = Array::with_max_entries(1, 0);

#[inline(always)]
fn vlan_pcp_bias_ok(pcp: u8) -> bool {
    unsafe {
        if let Some(c) = VLAN_PCP_CFG.get(0) {
            if (*c).enable == 0 { return false; }
            return pcp >= (*c).min_pcp;
        }
    }
    false
}

// ===== Capture PCP (0..7) in SCRATCH during peel_vlan =====
#[inline(always)]
fn stash_pcp_from_tci(tci_be: u16) {
    let pcp = ((u16::from_be(tci_be) >> 13) & 0x7) as u8;
    unsafe { if let Some(s) = SCRATCH_PCPU.get_ptr_mut(0) { (*s).vlan_pcp = pcp; } }
}

// ===== Per-CPU bucket LUT for cheap_pre_score → bucket (5ms valid) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct BuckLut { ts: u64, base: u32, step: u32 } // linear approx over current window

#[map]
static BUCKLUT_PCPU: PerCpuArray<BuckLut> = PerCpuArray::with_max_entries(1, 0);

// ===== Per-queue hard reserve for alpha/HOT (tokens) =====
#[map]
static QRESV: Array<u8> = Array::with_max_entries(64, 0);

// ===== Neighbor starvation timestamps (shorter holdoff) =====
#[map]
static QSTARVE_NEI_TS: Array<u64> = Array::with_max_entries(64, 0);

#[inline(always)]
fn mark_starved_nei(qid: u32, now: u64) {
    unsafe { if let Some(p) = QSTARVE_NEI_TS.get_ptr_mut(qid) { *p = now; } }
}
#[inline(always)]
fn starved_nei_recent(qid: u32, now: u64) -> bool {
    unsafe { QSTARVE_NEI_TS.get(qid).map(|t| now.wrapping_sub(*t) < 150_000).unwrap_or(false) }
}

#[inline(always)]
fn qresv(qid: u32) -> u8 {
    unsafe { QRESV.get(qid).copied().unwrap_or(0) }
}

// ===== Per-prefix LPB EWMA (LRU) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct PfxLpb { ts: u64, ew: u64 } // ×16 scale lamports/byte

#[map]
static PFX_LPB_LRU: LruHashMap<u64, PfxLpb> = LruHashMap::with_max_entries(8192, 0);

#[inline(always)]
fn pfx_lpb_floor(now: u64, prefix8: u64, lamports: u64, bytes: u32) -> u64 {
    let mut st = PfxLpb { ts: now, ew: 0 };
    unsafe { if let Some(v) = PFX_LPB_LRU.get(&prefix8) { st = *v; } }
    let mut ew = st.ew;
    let mut steps = (now.wrapping_sub(st.ts) / 100_000_000) as u32; if steps > 32 { steps = 32; }
    let mut i = 0u32; while i < steps { ew = ew.saturating_sub(ew >> 3); i += 1; }
    let b = (bytes as u64).max(1);
    let sample = ((lamports << 4) / b).min(10_000_000_000u64);
    ew = ew.saturating_add(sample >> 3);
    let upd = PfxLpb { ts: now, ew };
    unsafe { let _ = PFX_LPB_LRU.insert(&prefix8, &upd, 0); }
    (ew >> 7).clamp(2_000, 300_000) // 2k..300k lamports/byte
}

#[inline(always)]
fn under_pfx_lpb_floor(ev: &PacketEvent) -> bool {
    if heat_cached() <= 50_000 { return false; }
    let p8 = prog_prefix8_cached(ev);
    let now = now_cached();
    let f = pfx_lpb_floor(now, p8, ev.lamports, ev.data_len as u32);
    // slack factor 0.5×, compare without division
    ((ev.lamports as u128) << 1) < (f as u128) * (ev.data_len as u128)
}

// ===== Per-prefix loss counter (LRU) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct PfxLoss { ts: u64, cnt: u16 }

#[map]
static PFX_LOSS: LruHashMap<u64, PfxLoss> = LruHashMap::with_max_entries(8192, 0);

// ===== Per-prefix alpha credit (heat-gated) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct PfxCred { ts: u64, tok: u8 }

#[map]
static PFX_CREDIT: LruHashMap<u64, PfxCred> = LruHashMap::with_max_entries(8192, 0);

// ===== Dead-ring short quarantine (no refill observed) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct QDead { ts: u64, ctr: u8 }

#[map]
static QDEAD_TS: Array<QDead> = Array::with_max_entries(64, 0);

// ===== BODYDM scoped to BH =====
const BODYDM_SETS: usize = 64;
const BODYDM_WAYS: usize = 2;

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct BodyDM {
    key: [[u64; BODYDM_WAYS]; BODYDM_SETS],
    ts:  [[u64; BODYDM_WAYS]; BODYDM_SETS],
    bh8_epoch: u64,
}
#[map]
static BODYDM_PCPU: PerCpuArray<BodyDM> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn bodydm_hit_or_set(now: u64, k: u64) -> bool {
    let set = (k as usize) & (BODYDM_SETS - 1);
    let bh = unsafe { SCRATCH_PCPU.get_ptr(0).map(|s| unsafe { (*s).bh8 }).unwrap_or(0) };
    unsafe {
        if let Some(t) = BODYDM_PCPU.get_ptr_mut(0) {
            if (*t).bh8_epoch != bh {
                (*t).bh8_epoch = bh;
                let mut i = 0; while i < BODYDM_SETS { (*t).ts[i] = [0; BODYDM_WAYS]; i += 1; }
            }
            let ks = &mut (*t).key[set];
            let ts = &mut (*t).ts[set];
            let mut w = 0usize;
            while w < BODYDM_WAYS {
                if ks[w] == k { ts[w] = now; return true; }
                w += 1;
            }
            let mut idx = 0usize; let mut oldest = ts[0];
            let mut i2 = 1usize;
            while i2 < BODYDM_WAYS {
                if now.wrapping_sub(ts[i2]) > now.wrapping_sub(oldest) { idx = i2; oldest = ts[i2]; }
                i2 += 1;
            }
            ks[idx] = k; ts[idx] = now; return false;
        }
    }
    false
}

// ===== DCID → program-prefix conflict detector (LRU, short TTL) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct DcidPref { p8: u64, ts: u64, cnt: u8 }

#[map]
static DCID_PREF_LRU: LruHashMap<u64, DcidPref> = LruHashMap::with_max_entries(65536, 0);

// ===== Per-CPU payload size EWMA (×16) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct PldEw { ts: u64, ew: u32 }

#[map]
static PLD_EW_PCPU: PerCpuArray<PldEw> = PerCpuArray::with_max_entries(1, 0);

// ===== Per-queue refill slope EWMA (tokens gained per probe, ×16) =====
#[map]
static QSLOPE: Array<u16> = Array::with_max_entries(64, 0);

// ===== Per-prefix seen timestamp (LRU) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct PfxSeen { ts: u64 }

#[map]
static PFX_SEEN: LruHashMap<u64, PfxSeen> = LruHashMap::with_max_entries(16384, 0);

// ===== PDM (per-prefix sig) scoped to BH =====
const PDM_SETS: usize = 16;
const PDM_WAYS: usize = 2;

#[repr(C, align(64))]
#[derive(Clone, Copy)]
struct Pdm {
    key: [[u64; PDM_WAYS]; PDM_SETS],
    ts:  [[u64; PDM_WAYS]; PDM_SETS],
    bh8_epoch: u64,
}
#[map]
static PDM_PCPU: PerCpuArray<Pdm> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn pdm_hit_or_set(now: u64, p8: u64, sig: u64) -> bool {
    let set = ((hash_mix64(p8) ^ sig) as usize) & (PDM_SETS - 1);
    let bh = unsafe { SCRATCH_PCPU.get_ptr(0).map(|s| unsafe { (*s).bh8 }).unwrap_or(0) };
    unsafe {
        if let Some(t) = PDM_PCPU.get_ptr_mut(0) {
            if (*t).bh8_epoch != bh {
                (*t).bh8_epoch = bh;
                // lazy clear by timestamp wipe
                let mut i = 0; while i < PDM_SETS { (*t).ts[i] = [0; PDM_WAYS]; i += 1; }
            }
            let ks = &mut (*t).key[set];
            let ts = &mut (*t).ts[set];
            let mut w = 0usize;
            while w < PDM_WAYS {
                if ks[w] == sig { ts[w] = now; return true; }
                w += 1;
            }
            let mut idx = 0usize; let mut oldest = ts[0];
            let mut i2 = 1usize;
            while i2 < PDM_WAYS {
                if now.wrapping_sub(ts[i2]) > now.wrapping_sub(oldest) { idx = i2; oldest = ts[i2]; }
                i2 += 1;
            }
            ks[idx] = sig; ts[idx] = now; return false;
        }
    }
    false
}

// ===== IPv4 DSCP capture + policy =====
#[repr(C)]
#[derive(Clone, Copy)]
struct DscpCfg { enable: u8, min_dscp: u8 } // if enable=1 and allowlist empty → only enforce min_dscp

#[map]
static DSCP_CFG: Array<DscpCfg> = Array::with_max_entries(1, 0);

#[map]
static DSCP_ALLOW: HashMap<u8, u8> = HashMap::with_max_entries(64, 0); // optional allowlist

// ===== Flow backoff (LRU) for repeated losers =====
#[repr(C)]
#[derive(Clone, Copy)]
struct BanBk { until: u64, level: u8, ts: u64 }

#[map]
static BANBK_LRU: LruHashMap<u64, BanBk> = LruHashMap::with_max_entries(65536, 0);

// ===== VIP/DST → queue persistence (LRU) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct QPinDst { qid: u32, ts: u64 }

#[map]
static DST_QPIN: LruHashMap<u32, QPinDst> = LruHashMap::with_max_entries(65536, 0);

// ===== Per-queue token variance (EWMA of |Δtokens| ×16) =====
#[map]
static QLASTTOK: Array<u16> = Array::with_max_entries(64, 0);
#[map]
static QVAR:     Array<u16> = Array::with_max_entries(64, 0);

// ===== Per-prefix quality EWMA =====
#[repr(C)]
#[derive(Clone, Copy)]
struct PfxQ { ts: u64, ew: u16 } // 0..65535

#[map]
static PFX_QLTY: LruHashMap<u64, PfxQ> = LruHashMap::with_max_entries(8192, 0);

// ===== Optional IPv6 ingress gate =====
#[map]
static IPV6_ENABLE: Array<u8> = Array::with_max_entries(1, 0);

#[inline(always)]
fn ipv6_allowed() -> bool {
    unsafe { IPV6_ENABLE.get(0).copied().unwrap_or(0) != 0 }
}

#[inline(always)]
fn tail_slot_freeze_drop(event: &PacketEvent) -> bool {
    let (s_ns, _p80, p95, _p97) = slot_snapshot();
    let phase = now_cached() % s_ns;
    if phase <= (s_ns - (s_ns / 100)) { return false; } // last ~1%
    if is_vip_dst_fast(event.dst_ip.to_be()) { return false; }
    if is_top01_score(event) { return false; }
    let psb = score_to_bucket_fast(cheap_pre_score(event));
    psb + 1 < p95
}

#[inline(always)]
fn body_mix64(sample8: u64, payload: &[u8]) -> u64 {
    // Mix first 8B with a mid-8B when present; branch-light
    if payload.len() >= 40 {
        let mid = 16usize;
        let mut v: u64 = 0;
        // safe bounded reads
        if let Some(s) = payload.get(mid..mid+8) {
            v = u64::from_le_bytes([s[0],s[1],s[2],s[3],s[4],s[5],s[6],s[7]]);
        }
        return hash_mix64(sample8 ^ v.rotate_left(17));
    }
    hash_mix64(sample8)
}

// ===== Prefetch helper =====
extern "C" {
    #[link_name = "bpf_prefetch"]
    fn bpf_prefetch(ptr: *const core::ffi::c_void) -> i32;
}
#[inline(always)]
fn prefetch(ptr: *const u8) { unsafe { let _ = bpf_prefetch(ptr as *const _); } }

#[inline(always)]
fn udp_checksum_bad(ip: *const IpHdr, udp: *const UdpHdr, l2off: usize, ctx: &XdpContext) -> bool {
    if heat_cached() <= 100_000 { return false; }
    let csum = u16::from_be(unsafe { (*udp).check });
    if csum == 0 { return false; } // optional in IPv4; skip validation if zero
    // Minimal pseudoheader+UDP checksum over header-only (cheap guard)
    // Sum: src,dst,proto,len + UDP header words (skip data)
    let mut sum: u32 = 0;
    let ip16 = ip as *const u16;
    sum += unsafe { *ip16.add(6) } as u32; // src high
    sum += unsafe { *ip16.add(7) } as u32; // src low
    sum += unsafe { *ip16.add(8) } as u32; // dst high
    sum += unsafe { *ip16.add(9) } as u32; // dst low
    sum += 0x0011; // proto=17
    sum += u16::from_be(unsafe { (*udp).len }) as u32;

    let udp16 = udp as *const u16;
    let mut i = 0;
    while i < 4 { // 8 bytes header
        if i == 3 { i += 1; continue; } // skip checksum field
        sum += unsafe { *udp16.add(i) } as u32;
        i += 1;
    }
    while (sum >> 16) != 0 { sum = (sum & 0xFFFF) + (sum >> 16); }
    let res = (!sum as u16);
    res != csum
}

#[inline(always)]
fn pfx_quality_touch(now: u64, p8: u64, success: bool) {
    let mut st = PfxQ { ts: now, ew: 0 };
    unsafe { if let Some(v) = PFX_QLTY.get(&p8) { st = *v; } }
    // decay ~every 1ms by 1/16
    if now.wrapping_sub(st.ts) > 1_000_000 { st.ew = st.ew.saturating_sub((st.ew >> 4).max(1)); }
    let inc = if success { 512u16 } else { 64u16 }; // stronger update on success
    st.ew = st.ew.saturating_add(inc).min(u16::MAX);
    st.ts = now;
    unsafe { let _ = PFX_QLTY.insert(&p8, &st, 0); }
}

#[inline(always)]
fn pfx_quality_scale(p8: u64) -> u8 {
    unsafe {
        if let Some(v) = PFX_QLTY.get(&p8) {
            let ew = unsafe { (*v).ew } as u32;
            // 0..65535 → 0..4
            return ((ew + 4096) >> 13).min(4) as u8;
        }
    }
    0
}

#[inline(always)]
fn qvar_update(qid: u32, tokens: u16) {
    unsafe {
        let last = QLASTTOK.get(qid).copied().unwrap_or(tokens);
        let delta = if tokens > last { tokens - last } else { last - tokens };
        if let Some(v) = QVAR.get_ptr_mut(qid) {
            let mut ew = *v;
            ew = ew.saturating_sub(ew >> 2).saturating_add(delta.saturating_mul(16));
            *v = ew;
        }
        if let Some(l) = QLASTTOK.get_ptr_mut(qid) { *l = tokens; }
    }
}

#[inline(always)]
fn qvar_penalty(qid: u32) -> u32 {
    // map higher variance to small penalty
    (unsafe { QVAR.get(qid).copied().unwrap_or(0) } as u32) >> 4 // scale down
}

// ===== /24 sequential src-port spam killer =====
#[repr(C)]
#[derive(Clone, Copy)]
struct Seq24 { last_p: u16, ts: u64, streak: u8 }

#[map]
static SEQ24_LRU: LruHashMap<u32, Seq24> = LruHashMap::with_max_entries(32768, 0);

#[inline(always)]
fn ban_backoff(now: u64, fk: u64) {
    if heat_cached() <= 80_000 { return; }
    let mut st = BanBk { until: 0, level: 0, ts: now };
    unsafe { if let Some(v) = BANBK_LRU.get(&fk) { st = *v; } }
    let decay = now.wrapping_sub(st.ts) > 2_000_000;
    let lvl = if decay { st.level.saturating_sub(1) } else { st.level.saturating_add(1).min(6) };
    let dur = match lvl { 0 => 200_000, 1 => 300_000, 2 => 500_000, 3 => 800_000, 4 => 1_200_000, _ => 1_800_000 };
    st.level = lvl; st.until = now.saturating_add(dur); st.ts = now;
    unsafe { let _ = BANBK_LRU.insert(&fk, &st, 0); }
}

#[inline(always)]
fn banned_backoff(now: u64, fk: u64) -> bool {
    if heat_cached() <= 80_000 { return false; }
    unsafe { BANBK_LRU.get(&fk).map(|s| now < unsafe { (*s).until }).unwrap_or(false) }
}

// ===== /24 sequential src-port spam killer =====
#[repr(C)]
#[derive(Clone, Copy)]
struct Seq24 { last_p: u16, ts: u64, streak: u8 }

#[map]
static SEQ24_LRU: LruHashMap<u32, Seq24> = LruHashMap::with_max_entries(32768, 0);

#[inline(always)]
fn seq24_drop(now: u64, src_ip_be: u32, src_port: u16) -> bool {
    if heat_cached() <= 80_000 { return false; }
    let k = src_ip_be & 0xFFFF_FF00;
    let mut st = Seq24 { last_p: src_port, ts: now, streak: 0 };
    unsafe { if let Some(v) = SEQ24_LRU.get(&k) { st = *v; } }
    let d = if src_port > st.last_p { src_port - st.last_p } else { st.last_p - src_port };
    let gap = now.wrapping_sub(st.ts);
    if (d == 1 || d == 0) && gap < 200_000 { st.streak = st.streak.saturating_add(1); }
    else { st.streak = 1; }
    st.last_p = src_port; st.ts = now;
    unsafe { let _ = SEQ24_LRU.insert(&k, &st, 0); }
    st.streak >= 12
}

#[inline(always)]
fn qscore(qid: u32, tokens: u16, reserve: u16, ultra: bool) -> u32 {
    let avail = if ultra { tokens } else { tokens.saturating_sub(reserve) };
    let h  = qhealth(qid) as u32;
    let sl = qslope(qid)  as u32;
    let vp = qvar_penalty(qid);
    (((avail as u32) << 12) | ((h & 0x3FF) << 2) | (sl & 0x3)).saturating_sub(vp.min(800))
}

#[inline(always)]
fn pfx_credit_take(now: u64, p8: u64) -> bool {
    if heat_cached() <= 80_000 { return false; }
    let mut st = PfxCred { ts: now, tok: 0 };
    unsafe { if let Some(v) = PFX_CREDIT.get(&p8) { st = *v; } }
    // decay to zero after ~3ms idle
    if now.wrapping_sub(st.ts) > 3_000_000 { st.tok = 0; }
    if st.tok == 0 { return false; }
    st.tok -= 1; st.ts = now;
    unsafe { let _ = PFX_CREDIT.insert(&p8, &st, 0); }
    true
}

#[inline(always)]
fn pfx_credit_give(now: u64, p8: u64, amt: u8) {
    if heat_cached() <= 80_000 { return; }
    let mut st = PfxCred { ts: now, tok: amt.min(8) };
    unsafe { if let Some(v) = PFX_CREDIT.get(&p8) { st = *v; } }
    let nt = (st.tok as u16 + amt as u16).min(8) as u8;
    st.tok = nt; st.ts = now;
    unsafe { let _ = PFX_CREDIT.insert(&p8, &st, 0); }
}

#[inline(always)]
fn pfx_loss_touch(prefix8: u64) {
    let now = now_cached();
    let mut st = PfxLoss { ts: now, cnt: 0 };
    unsafe { if let Some(v) = PFX_LOSS.get(&prefix8) { st = *v; } }
    if now.wrapping_sub(st.ts) > 2_000_000 { st.cnt = 0; } // decay after 2ms idle
    st.cnt = st.cnt.saturating_add(1);
    st.ts = now;
    unsafe { let _ = PFX_LOSS.insert(&prefix8, &st, 0); }
}

#[inline(always)]
fn pfx_loss_relief(prefix8: u64) {
    unsafe {
        if let Some(v) = PFX_LOSS.get(&prefix8) {
            let mut st = *v;
            st.cnt = st.cnt.saturating_sub(2); // fast relief on success
            let _ = PFX_LOSS.insert(&prefix8, &st, 0);
        }
    }
}

#[inline(always)]
fn mark_dead_probe(qid: u32, had_tokens: bool, now: u64) {
    unsafe {
        if let Some(q) = QDEAD_TS.get_ptr_mut(qid) {
            if had_tokens { (*q).ctr = 0; (*q).ts = now; }
            else {
                let c = (*q).ctr.saturating_add(1);
                (*q).ctr = c; (*q).ts = now;
            }
        }
    }
}

#[inline(always)]
fn dead_recent(qid: u32, now: u64) -> bool {
    unsafe {
        if let Some(q) = QDEAD_TS.get(qid) {
            return (*q).ctr >= 3 && now.wrapping_sub((*q).ts) < 1_000_000; // ~1ms after 3 consecutive zeros
        }
    }
    false
}

#[inline(always)]
fn bodydm_hit_or_set(now: u64, k: u64) -> bool {
    let set = (k as usize) & (BODYDM_SETS - 1);
    unsafe {
        if let Some(t) = BODYDM_PCPU.get_ptr_mut(0) {
            let ks = &mut (*t).key[set];
            let ts = &mut (*t).ts[set];
            let mut w = 0usize;
            while w < BODYDM_WAYS {
                if ks[w] == k { ts[w] = now; return true; }
                w += 1;
            }
            // replace stalest
            let mut idx = 0usize; let mut oldest = ts[0];
            let mut i = 1usize;
            while i < BODYDM_WAYS {
                if now.wrapping_sub(ts[i]) > now.wrapping_sub(oldest) { idx = i; oldest = ts[i]; }
                i += 1;
            }
            ks[idx] = k; ts[idx] = now; return false;
        }
    }
    false
}

#[inline(always)]
fn dcid_conflict_drop(now: u64, dcid_key: u64, prefix8: u64) -> bool {
    if dcid_key == 0 { return false; }
    let mut st = DcidPref { p8: prefix8, ts: now, cnt: 0 };
    unsafe { if let Some(v) = DCID_PREF_LRU.get(&dcid_key) { st = *v; } }
    let gap = now.wrapping_sub(st.ts);
    if st.p8 == prefix8 {
        // same mapping → decay the counter
        if gap > 2_000_000 { st.cnt = st.cnt.saturating_sub(1); }
        st.ts = now;
        unsafe { let _ = DCID_PREF_LRU.insert(&dcid_key, &st, 0); }
        return false;
    }
    // conflict: promote cnt and treat repeated conflicts under heat as hostile
    st.p8 = prefix8; st.ts = now; st.cnt = st.cnt.saturating_add(1);
    unsafe { let _ = DCID_PREF_LRU.insert(&dcid_key, &st, 0); }
    heat_cached() > 80_000 && st.cnt >= 3 && gap < 2_000_000
}

#[inline(always)]
fn payload_too_large(now: u64, bytes: u32) -> bool {
    if heat_cached() <= 80_000 { return false; }
    unsafe {
        if let Some(p) = PLD_EW_PCPU.get_ptr_mut(0) {
            let mut ew = (*p).ew;
            // decay @100ms, up to 32 steps
            let mut steps = (now.wrapping_sub((*p).ts) / 100_000_000) as u32; if steps > 32 { steps = 32; }
            let mut i = 0u32; while i < steps { ew = ew.saturating_sub(ew >> 3); i += 1; }
            let sample = (bytes << 4).min(1_000_000u32); // clamp
            ew = ew.saturating_add(sample >> 3); // 12.5% update
            (*p).ew = ew; (*p).ts = now;
            // fence: drop if > 4× EWMA when boiling
            let thresh = ew << 2; // ×4
            return (bytes << 4) > thresh;
        }
    }
    false
}

#[inline(always)]
fn qslope_update(qid: u32, gained: u16) {
    unsafe {
        if let Some(s) = QSLOPE.get_ptr_mut(qid) {
            let mut ew = *s;
            ew = ew.saturating_sub(ew >> 2)
                 .saturating_add(gained.saturating_mul(16));
            *s = ew;
        }
    }
}

#[inline(always)]
fn qslope(qid: u32) -> u16 {
    unsafe { QSLOPE.get(qid).copied().unwrap_or(0) }
}

#[inline(always)]
fn qscore(qid: u32, tokens: u16, reserve: u16, ultra: bool) -> u32 {
    let avail = if ultra { tokens } else { tokens.saturating_sub(reserve) };
    let h  = qhealth(qid) as u32;
    let sl = qslope(qid)  as u32;
    let vp = qvar_penalty(qid);
    (((avail as u32) << 12) | ((h & 0x3FF) << 2) | (sl & 0x3)).saturating_sub(vp.min(800))
}

#[inline(always)]
fn pfx_credit_give_roi(now: u64, p8: u64, roi_u32: u32, prio: u8) {
    if heat_cached() <= 80_000 { return; }
    let q = pfx_quality_scale(p8);
    let mut amt: u8 = if roi_u32 >= 900 { 3 } else if roi_u32 >= 600 { 2 } else { 1 };
    amt = amt.saturating_add(q.min(2));
    if prio >= 14 { amt = amt.saturating_add(1); }
    pfx_credit_give(now, p8, amt);
}

#[inline(always)]
fn value_density_under(ev: &PacketEvent) -> bool {
    if heat_cached() <= 100_000 { return false; }
    let lpb = lpb_update_and_floor(now_cached(), ev.lamports, ev.data_len as u32); // lamports/byte floor
    let cu = cu_eff_cached(ev) as u64; if cu == 0 { return false; }
    let lpc = ((ev.lamports as u128) << 6) / (cu as u128); // lamports/CU ×64
    // harmonic mean H ≈ 2 / (1/lpb + 1/lpc_scaled); compare without division
    // Require both reasonably high: (lamports * 2) >= bytes*lpb + CU_scaled
    let lhs = (ev.lamports as u128) << 1;
    let rhs = (lpb as u128) * (ev.data_len as u128) + ((cu as u128) << 0); // CU term is smaller weight
    lhs < rhs
}

#[inline(always)]
fn pfx_warm_start_drop(now: u64, p8: u64, ps_bucket: u16) -> bool {
    if heat_cached() <= 80_000 { return false; }
    let mut st = PfxSeen { ts: 0 };
    unsafe { if let Some(v) = PFX_SEEN.get(&p8) { st = *v; } }
    let seen = st.ts != 0 && now.wrapping_sub(st.ts) < 2_000_000; // seen within 2ms
    if !seen {
        // first sight in 2ms → require near p90
        let (_, p95, _) = pctls(now);
        if ps_bucket + 1 < p95 { 
            unsafe { let _ = PFX_SEEN.insert(&p8, &PfxSeen { ts: now }, 0); }
            return true; 
        }
    }
    unsafe { let _ = PFX_SEEN.insert(&p8, &PfxSeen { ts: now }, 0); }
    false
}

#[inline(always)]
fn pdm_hit_or_set(now: u64, p8: u64, sig: u64) -> bool {
    let set = ((hash_mix64(p8) ^ sig) as usize) & (PDM_SETS - 1);
    let bh = unsafe { SCRATCH_PCPU.get_ptr(0).map(|s| unsafe { (*s).bh8 }).unwrap_or(0) };
    unsafe {
        if let Some(t) = PDM_PCPU.get_ptr_mut(0) {
            if (*t).bh8_epoch != bh {
                (*t).bh8_epoch = bh;
                // lazy clear by timestamp wipe
                let mut i = 0; while i < PDM_SETS { (*t).ts[i] = [0; PDM_WAYS]; i += 1; }
            }
            let ks = &mut (*t).key[set];
            let ts = &mut (*t).ts[set];
            let mut w = 0usize;
            while w < PDM_WAYS {
                if ks[w] == sig { ts[w] = now; return true; }
                w += 1;
            }
            let mut idx = 0usize; let mut oldest = ts[0];
            let mut i2 = 1usize;
            while i2 < PDM_WAYS {
                if now.wrapping_sub(ts[i2]) > now.wrapping_sub(oldest) { idx = i2; oldest = ts[i2]; }
                i2 += 1;
            }
            ks[idx] = sig; ts[idx] = now; return false;
        }
    }
    false
}

#[inline(always)]
fn map_budget_take_alpha(n: u8, prio: u8) -> bool {
    // prio ≥14 consumes half budget units (rounded up)
    let use = if prio >= 14 { (n + 1) >> 1 } else { n };
    map_budget_take(use)
}

#[inline(always)]
fn fk_cached(event: &PacketEvent, dcid_key_opt: u64) -> u64 {
    unsafe {
        if let Some(s) = SCRATCH_PCPU.get_ptr(0) {
            let k = (*s).flowk;
            if k != 0 { return k; }
        }
    }
    let k = flow_key(event, dcid_key_opt);
    unsafe { if let Some(s) = SCRATCH_PCPU.get_ptr_mut(0) { (*s).flowk = k; } }
    k
}

#[inline(always)]
fn vip_slothead_grace(is_vip: bool) -> bool {
    if !is_vip { return false; }
    let (s_ns, _, _, _) = slot_snapshot();
    (now_cached() % s_ns) < (s_ns / 32) // first ~3.1% of slot
}

// stash DSCP once per packet
#[inline(always)]
fn stash_dscp(ip: *const IpHdr) {
    let dscp = unsafe { (*ip).tos >> 2 }; // upper 6 bits
    unsafe { if let Some(s) = SCRATCH_PCPU.get_ptr_mut(0) { (*s).ip_dscp = dscp; } }
}

#[inline(always)]
fn dscp_allowed(dscp: u8) -> bool {
    unsafe {
        let cfg = DSCP_CFG.get(0);
        if cfg.is_none() { return true; }
        let c = *cfg.unwrap();
        if c.enable == 0 { return true; }
        // if allowlist populated, require membership; else enforce min_dscp only
        let listed = DSCP_ALLOW.get(&(dscp as u8)).is_some();
        if listed { return true; }
        dscp >= c.min_dscp
    }
}

#[inline(always)]
fn dscp_bias_ultra(dscp: u8) -> bool {
    unsafe {
        if let Some(c) = DSCP_CFG.get(0) {
            return (*c).enable != 0 && dscp >= (*c).min_dscp && heat_cached() > 80_000;
        }
    }
    false
}

// ===== IPv4 ECN capture + policy =====
#[inline(always)]
fn stash_ecn(ip: *const IpHdr) {
    let ecn = unsafe { (*ip).tos & 0x03 }; // 0..3, 3 = CE
    unsafe { if let Some(s) = SCRATCH_PCPU.get_ptr_mut(0) { (*s).ip_ecn = ecn; } }
}

#[inline(always)]
fn ecn_ce_penalty() -> u32 {
    // CE → subtract points in score when boiling; neutral otherwise
    let h = heat_cached();
    if h <= 80_000 { return 0; }
    let e = unsafe { SCRATCH_PCPU.get_ptr(0).map(|p| unsafe { (*p).ip_ecn }).unwrap_or(0) };
    if e == 3 { 400 } else { 0 }
}

#[inline(always)]
fn ecn_drop_under_extreme() -> bool {
    // Optional hard drop only in extreme boil and non-ultra
    let h = heat_cached();
    if h <= 120_000 { return false; }
    let e = unsafe { SCRATCH_PCPU.get_ptr(0).map(|p| unsafe { (*p).ip_ecn }).unwrap_or(0) };
    e == 3 && !is_top1_score()
}

#[inline(always)]
fn ip_len_eq_udp(ip: *const IpHdr, udp: *const UdpHdr) -> bool {
    // assumes IHL==5 (we already drop options)
    let tot = u16::from_be(unsafe { (*ip).tot_len }) as usize;
    let ulen = u16::from_be(unsafe { (*udp).len }) as usize;
    tot == (20 + ulen)
}

// RFC 1071 header checksum (IHL==5), heat-gated
#[inline(always)]
fn ipv4_header_checksum_ok(ip: *const IpHdr) -> bool {
    if heat_cached() <= 100_000 { return true; }
    let p = ip as *const u16;
    let mut sum: u32 = 0;
    let mut i = 0;
    while i < 10 {
        if i == 5 { i += 1; continue; } // skip checksum word
        let w = unsafe { *p.add(i) } as u32;
        sum = (sum + w) & 0xFFFF_FFFF;
        i += 1;
    }
    while (sum >> 16) != 0 { sum = (sum & 0xFFFF) + (sum >> 16); }
    let cksum = unsafe { *p.add(5) } as u32;
    (!sum as u16) as u32 == cksum
}

#[inline(always)]
fn ban_backoff(now: u64, fk: u64) {
    if heat_cached() <= 80_000 { return; }
    let mut st = BanBk { until: 0, level: 0, ts: now };
    unsafe { if let Some(v) = BANBK_LRU.get(&fk) { st = *v; } }
    let decay = now.wrapping_sub(st.ts) > 2_000_000;
    let lvl = if decay { st.level.saturating_sub(1) } else { st.level.saturating_add(1).min(6) };
    let dur = match lvl { 0 => 200_000, 1 => 300_000, 2 => 500_000, 3 => 800_000, 4 => 1_200_000, _ => 1_800_000 };
    st.level = lvl; st.until = now.saturating_add(dur); st.ts = now;
    unsafe { let _ = BANBK_LRU.insert(&fk, &st, 0); }
}

#[inline(always)]
fn banned_backoff(now: u64, fk: u64) -> bool {
    if heat_cached() <= 80_000 { return false; }
    unsafe { BANBK_LRU.get(&fk).map(|s| now < unsafe { (*s).until }).unwrap_or(false) }
}

#[inline(always)]
fn seq24_drop(now: u64, src_ip_be: u32, src_port: u16) -> bool {
    if heat_cached() <= 80_000 { return false; }
    let k = src_ip_be & 0xFFFF_FF00;
    let mut st = Seq24 { last_p: src_port, ts: now, streak: 0 };
    unsafe { if let Some(v) = SEQ24_LRU.get(&k) { st = *v; } }
    let d = if src_port > st.last_p { src_port - st.last_p } else { st.last_p - src_port };
    let gap = now.wrapping_sub(st.ts);
    if (d == 1 || d == 0) && gap < 200_000 { st.streak = st.streak.saturating_add(1); }
    else { st.streak = 1; }
    st.last_p = src_port; st.ts = now;
    unsafe { let _ = SEQ24_LRU.insert(&k, &st, 0); }
    st.streak >= 12
}

#[inline(always)]
fn pfx_loss_penalty(prefix8: u64) -> u32 {
    unsafe {
        if let Some(v) = PFX_LOSS.get(&prefix8) {
            // 4/8/12+ recent losses → 100/250/500 penalty
            let c = (*v).cnt as u32;
            let mut pen = if c >= 12 { 500 } else if c >= 8 { 250 } else if c >= 4 { 100 } else { 0 };
            let q = pfx_quality_scale(prefix8) as u32;
            pen = pen.saturating_sub(q * 60);
            return pen;
        }
    }
    0
}

#[inline(always)]
fn score_to_bucket_fast(score: u32) -> u16 {
    let now = now_cached();
    unsafe {
        if let Some(b) = BUCKLUT_PCPU.get_ptr_mut(0) {
            if now.wrapping_sub((*b).ts) >= 5_000_000 {
                // calibrate: map [p80..p97] scores to [80..97] buckets using two points
                let p80 = hist_percentile(80) as u32;
                let p97 = hist_percentile(97) as u32;
                (*b).base = p80; (*b).step = (p97.saturating_sub(p80)).max(1) / 17;
                (*b).ts = now;
            }
            let d = score.saturating_sub((*b).base);
            let approx = 80u32.saturating_add(d / (*b).step);
            return approx.min(100) as u16;
        }
    }
    score_to_bucket(score) // fallback
}

// ===== Flash prefix classifier map (2-byte key) =====
#[map]
static FLASH_PREFIXES: HashMap<[u8; 2], u8> = HashMap::with_max_entries(64, 0); // userland-controlled

// ===== Flow→queue short stickiness =====
#[repr(C)]
#[derive(Clone, Copy)]
struct QStick { qid: u32, ts: u64 }

#[map]
static FLOW_QSTICK: LruHashMap<u64, QStick> = LruHashMap::with_max_entries(65536, 0);

// ===== Per-prefix score EWMA =====
#[repr(C)]
#[derive(Clone, Copy)]
struct PfxScore { ts: u64, ew: u32 } // ew scaled ×16

#[map]
static PFX_SCORE_EMA: LruHashMap<u64, PfxScore> = LruHashMap::with_max_entries(4096, 0);

// ===== Per-CPU negative cache for non-flash prefixes (8 entries) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct FpNeg { k: [u16;8], ts: [u64;8] }

#[map]
static FP_NEG_PCPU: PerCpuArray<FpNeg> = PerCpuArray::with_max_entries(1, 0);

// ===== Per-CPU recent blockhash horizon (cur/prev + candidate) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct BhState {
    cur:  u64, cur_ts:  u64,
    prev: u64, prev_ts: u64,
    cand: u64, cand_ts: u64, cand_cnt: u16,
}

#[map]
static BHSTATE_PCPU: PerCpuArray<BhState> = PerCpuArray::with_max_entries(1, 0);

// ===== Per-CPU last-two signature hashes per flow =====
#[repr(C)]
#[derive(Clone, Copy)]
struct Aba2 { k_flow: u64, h1: u64, h2: u64, ts: u64 }

#[map]
static ABA_PCPU: PerCpuArray<Aba2> = PerCpuArray::with_max_entries(1, 0);

// ===== Per-CPU percentile cache for 5ms =====
#[repr(C)]
#[derive(Clone, Copy)]
struct Pctl { ts: u64, p80: u16, p95: u16, p97: u16 }

#[map]
static PCTL_PCPU: PerCpuArray<Pctl> = PerCpuArray::with_max_entries(1, 0);

// ===== Writable account (prefix8) EWMA =====
#[repr(C)]
#[derive(Clone, Copy)]
struct AccHot { ts: u64, ew: u32 }

#[map]
static ACC_HOT_LRU: LruHashMap<u64, AccHot> = LruHashMap::with_max_entries(65536, 0);

// ===== Slow signature Bloom (900ms) to relieve LRU pressure =====
const SB_BITS: usize  = 1024;
const SB_WORDS: usize = SB_BITS / 64;

#[repr(C)]
#[derive(Clone, Copy)]
struct SigSlow { words: [u64; SB_WORDS], epoch_ns: u64, set_ctr: u16 }

#[map]
static SIGSLOW_PCPU: PerCpuArray<SigSlow> = PerCpuArray::with_max_entries(1, 0);

// ===== Per-flow last arrival (LRU) to kill ultra-tight replays =====
#[repr(C)]
#[derive(Clone, Copy)]
struct FlowTs { ts: u64, hits: u16 }

#[map]
static FLOW_TS_LRU: LruHashMap<u64, FlowTs> = LruHashMap::with_max_entries(65536, 0);

// ===== VLAN peel (802.1Q / 802.1AD) =====
#[repr(C)]
struct VlanHdr { tci: u16, tpid: u16 } // on-wire: [TPID][TCI], we read via eth->h_proto chain

// ===== Optional UDP dst-port allowlist =====
#[map]
static PORT_ALLOW: HashMap<u16, u8> = HashMap::with_max_entries(256, 0);

#[repr(C)]
#[derive(Clone, Copy)]
struct PortAllowCfg { enable: u8 }
#[map]
static PORT_ALLOW_CFG: Array<PortAllowCfg> = Array::with_max_entries(1, 0);

// ===== Per-prefix token bucket (heat-gated) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct PfxTok { ts: u64, tok: u16 }

#[map]
static PFX_TOK_LRU: LruHashMap<u64, PfxTok> = LruHashMap::with_max_entries(8192, 0);

// ===== Per (src_ip /16, program prefix16) limiter =====
#[repr(C)]
#[derive(Clone, Copy)]
struct SpLim { ts: u64, tok: u8 }

#[map]
static SP_LIMIT: LruHashMap<u32, SpLim> = LruHashMap::with_max_entries(32768, 0);

// ===== Flash loan correlation flags and helpers =====
const FL_BORROW: u8 = 0x01;
const FL_TRADE:  u8 = 0x02;
const FL_REPAY:  u8 = 0x04;

// ===== Verifier-friendly bounded readers (upgraded) =====
#[inline(always)]
fn load_bytes<'a>(payload: &'a [u8], off: usize, n: usize) -> Result<&'a [u8], ()> {
    // Fast fail on overflow and length
    let end = off.checked_add(n).ok_or(())?;
    if end > payload.len() { return Err(()); }
    // Safety: slice bounds checked above; no unaligned loads performed here
    Ok(&payload[off..end])
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
    // Safety: bounds verified; memcpy is verifier-friendly
    unsafe { core::ptr::copy_nonoverlapping(s.as_ptr(), dst.as_mut_ptr(), 32) };
    Ok(())
}

#[inline(always)]
fn parse_compact_u16_bounded(payload: &[u8], off: &mut usize) -> Result<u16, ()> {
    let b0 = *load_bytes(payload, *off, 1)?.get(0).ok_or(())?;
    if b0 < 0x80 {
        *off += 1;
        Ok(b0 as u16)
    } else if b0 < 0xFE {
        let b1 = *load_bytes(payload, *off + 1, 1)?.get(0).ok_or(())?;
        let v = ((b0 & 0x7F) as u16) | ((b1 as u16) << 7);
        *off += 2;
        Ok(v)
    } else {
        let v = load_u16_le(payload, *off + 1)?;
        *off += 3;
        Ok(v)
    }
}

#[inline(always)]
fn calculate_transaction_hash(signature: &[u8; 64]) -> u64 {
    // FNV-1a per byte, unrolled on u64 chunks + SplitMix64 post-mix.
    let mut h: u64 = 0xcbf29ce484222325;
    let p:  u64 = 0x100000001b3;

    let mut i = 0;
    while i < 64 {
        let chunk = u64::from_le_bytes([
            signature[i], signature[i+1], signature[i+2], signature[i+3],
            signature[i+4], signature[i+5], signature[i+6], signature[i+7],
        ]);
        // fold 8 bytes bytewise (verifier-friendly, fixed bound)
        h ^= (chunk        & 0xFF) as u64; h = h.wrapping_mul(p);
        h ^= ((chunk>>8)   & 0xFF) as u64; h = h.wrapping_mul(p);
        h ^= ((chunk>>16)  & 0xFF) as u64; h = h.wrapping_mul(p);
        h ^= ((chunk>>24)  & 0xFF) as u64; h = h.wrapping_mul(p);
        h ^= ((chunk>>32)  & 0xFF) as u64; h = h.wrapping_mul(p);
        h ^= ((chunk>>40)  & 0xFF) as u64; h = h.wrapping_mul(p);
        h ^= ((chunk>>48)  & 0xFF) as u64; h = h.wrapping_mul(p);
        h ^= ((chunk>>56)  & 0xFF) as u64; h = h.wrapping_mul(p);
        i += 8;
    }
    let mut z = h.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

// ===== Flow→queue short stickiness helpers =====
#[inline(always)]
fn flow_key(event: &PacketEvent, dcid_key_opt: u64) -> u64 {
    if dcid_key_opt != 0 { return dcid_key_opt; }
    // 4-tuple key; stable, low collision
    ((event.src_ip as u64) << 32)
        ^ (event.dst_ip as u64)
        ^ (((event.src_port as u64) << 48) ^ ((event.dst_port as u64) << 16))
}

// ===== Per-prefix score EWMA helpers =====
#[inline(always)]
fn pfx_score_bump(now: u64, prefix8: u64, score: u32) -> u8 {
    let mut ps = PfxScore { ts: now, ew: (score as u32) << 4 };
    unsafe { if let Some(v) = PFX_SCORE_EMA.get(&prefix8) { ps = *v; } }

    // decay every 100ms (bounded 32 steps)
    let mut steps = ((now.wrapping_sub(ps.ts)) / 100_000_000) as u32; if steps > 32 { steps = 32; }
    let mut ew = ps.ew;
    let mut i = 0u32; while i < steps { ew = ew.saturating_sub(ew >> 3); i += 1; } // ew -= ew/8
    ew = ew.saturating_add((score as u32) << 4);

    let upd = PfxScore { ts: now, ew };
    unsafe { let _ = PFX_SCORE_EMA.insert(&prefix8, &upd, 0); }

    // If this score exceeds the prefix baseline by a clear margin, bump tier a notch.
    let baseline = ew >> 4;
    if (score as u32) > baseline.saturating_add(1500) { 1 }   // +1 tier for strong outlier
    else { 0 }
}

// ===== Negative flash-prefix micro-cache helpers =====
#[inline(always)]
fn fp_neg_hit(k: u16) -> bool {
    unsafe {
        if let Some(n) = FP_NEG_PCPU.get_ptr(0) {
            let mut i = 0usize; while i < 8 { if (*n).k[i] == k { return true; } i += 1; }
        }
    }
    false
}

#[inline(always)]
fn fp_neg_set(k: u16) {
    unsafe {
        if let Some(n) = FP_NEG_PCPU.get_ptr_mut(0) {
            let now = bpf_ktime_get_ns();
            // insert into stalest slot
            let mut idx = 0usize; let mut o = (*n).ts[0];
            let mut i = 1usize; while i < 8 {
                if now.wrapping_sub((*n).ts[i]) > now.wrapping_sub(o) { idx = i; o = (*n).ts[i]; }
                i += 1;
            }
            (*n).k[idx] = k; (*n).ts[idx] = now;
        }
    }
}

// ===== Heat-adaptive Bloom strictness helpers =====
#[inline(always)]
fn bloom_indices3(h: u64) -> (usize,u64,usize,u64,usize,u64) {
    let salt = (flow_salt() as u64) ^ bloom_slot_salt(now_cached());
    let h1 = hash_mix64(h ^ (salt.wrapping_mul(0x9E3779B185EBCA87)));
    let h2 = hash_mix64(h ^ (salt ^ 0x517c_c1b1_c2a3_e4f5));
    let h3 = hash_mix64(h ^ 0x94D0_49BB_1331_11EBu64);
    let i1 = (h1 as usize) & (BLOOM_BITS - 1);
    let i2 = (h2 as usize) & (BLOOM_BITS - 1);
    let i3 = (h3 as usize) & (BLOOM_BITS - 1);
    (i1/64,1u64<<(i1&63), i2/64,1u64<<(i2&63), i3/64,1u64<<(i3&63))
}

#[inline(always)]
fn bloom_indices4(h: u64, salt: u64) -> (usize,u64,usize,u64,usize,u64,usize,u64) {
    let h1 = hash_mix64(h ^ salt);
    let h2 = hash_mix64(h ^ 0xBF58_476D_1CE4_E5B9u64);
    let h3 = hash_mix64(h ^ 0x94D0_49BB_1331_11EBu64);
    let h4 = hash_mix64(h ^ 0xC4CE_B9FE_1A85_EC53u64);
    let i1 = (h1 as usize) & (BLOOM_BITS - 1);
    let i2 = (h2 as usize) & (BLOOM_BITS - 1);
    let i3 = (h3 as usize) & (BLOOM_BITS - 1);
    let i4 = (h4 as usize) & (BLOOM_BITS - 1);
    (i1/64,1u64<<(i1&63), i2/64,1u64<<(i2&63), i3/64,1u64<<(i3&63), i4/64,1u64<<(i4&63))
}

// ===== QUIC VN detection =====
#[inline(always)]
fn looks_like_quic_vn(payload: &[u8]) -> bool {
    if payload.len() < 12 { return false; }
    let b0 = payload[0];
    if (b0 & 0xC0) != 0xC0 { return false; }
    let ver = u32::from_be_bytes([payload[1],payload[2],payload[3],payload[4]]);
    ver == 0 // QUIC Version Negotiation packet
}

// ===== Blockhash horizon classifier =====
#[inline(always)]
fn bh_classify(now: u64, bh8: u64) -> u8 {
    unsafe {
        if let Some(bs) = BHSTATE_PCPU.get_ptr_mut(0) {
            let s_ns = slot_ns();
            // first sighting
            if (*bs).cur == 0 {
                (*bs).cur = bh8; (*bs).cur_ts = now;
                (*bs).prev = 0;  (*bs).prev_ts = 0;
                (*bs).cand = 0;  (*bs).cand_ts = 0; (*bs).cand_cnt = 0;
                return 1;
            }
            if (*bs).cur == bh8 { (*bs).cur_ts = now; return 1; }
            if (*bs).prev == bh8 { (*bs).prev_ts = now; return 2; }

            // Unknown BH: treat as OLD unless rotation is justified.
            // Only rotate to new CUR if (a) we've passed a small fraction of the slot
            // OR (b) we have multiple confirmations of the candidate within half a slot.
            let allow_time = now.wrapping_sub((*bs).cur_ts) > (s_ns / 8);
            if (*bs).cand == bh8 {
                (*bs).cand_cnt = (*bs).cand_cnt.saturating_add(1);
            } else {
                (*bs).cand = bh8; (*bs).cand_ts = now; (*bs).cand_cnt = 1;
            }
            let allow_repeat = (*bs).cand_cnt >= 8 && now.wrapping_sub((*bs).cand_ts) < (s_ns / 2);

            if allow_time || allow_repeat {
                (*bs).prev    = (*bs).cur;    (*bs).prev_ts = (*bs).cur_ts;
                (*bs).cur     = bh8;          (*bs).cur_ts  = now;
                (*bs).cand    = 0;            (*bs).cand_ts = 0; (*bs).cand_cnt = 0;
                return 1; // new current
            }
            3 // old/stale until rotation permitted
        } else {
            0
        }
    }
}

// ===== Rotate-jitter on all short-window Blooms =====
#[inline(always)]
fn jitter_ns_small() -> u64 {
    // 0..~524µs backdating; bounded & verifier-friendly
    let r = unsafe { bpf_get_prandom_u32() } as u64;
    (r & 0x3FF) * 512
}

// ===== ROI benefit tie-breaker =====
#[inline(always)]
fn roi_benefit(ev: &PacketEvent) -> u32 {
    let fee = estimate_gas_priority_fee(ev).saturating_add(1) as u64;
    let pnl = (ev.expected_profit as u64).min(100_000_000_000); // cap 0.1 SOL
    // Scale: (profit / fee) into 0..1200. Guard tiny/huge outliers.
    let ratio = (pnl / fee).min(5000);
    (ratio as u32).min(1200)
}

// ===== L0 same-flow ABA guard =====
#[inline(always)]
fn aba_dupe(now: u64, flowk: u64, sig_hash: u64) -> bool {
    unsafe {
        if let Some(a) = ABA_PCPU.get_ptr_mut(0) {
            if (*a).k_flow != flowk {
                (*a).k_flow = flowk; (*a).h1 = sig_hash; (*a).h2 = 0; (*a).ts = now;
                return false;
            }
            // if we see A-B-A within ~2ms, treat as dupe
            let recent = now.wrapping_sub((*a).ts) < 2_000_000;
            let hit = recent && (sig_hash == (*a).h1 || sig_hash == (*a).h2);
            // rotate window
            if sig_hash != (*a).h1 { (*a).h2 = (*a).h1; (*a).h1 = sig_hash; }
            (*a).ts = now;
            hit
        } else { false }
    }
}

// ===== On-heat pre-score gate =====
#[inline(always)]
fn cheap_pre_score(ev: &PacketEvent) -> u32 {
    // profit log ≈ ilog10_scaled: keep your call but clamp once; rest integer adds.
    let pl = ilog10_scaled_fast(ev.expected_profit.saturating_add(1)).saturating_mul(1000);
    let pr = (ev.priority as u32) << 9; // ×512 ≈ 800 but cheaper; tuned with larger margin below
    let ef = cu_per_value_benefit(ev);
    let lp = lpc_benefit(ev);
    pl.saturating_add(pr).saturating_add(ef).saturating_add(lp).min(100_000)
}

// ===== Percentile cache =====
#[inline(always)]
fn pctls(now: u64) -> (u16,u16,u16) {
    unsafe {
        if let Some(pc) = PCTL_PCPU.get_ptr_mut(0) {
            if now.wrapping_sub((*pc).ts) < 5_000_000 {
                return ((*pc).p80, (*pc).p95, (*pc).p97);
            }
            let p80 = hist_percentile(80);
            let p95 = hist_percentile(95);
            let p97 = hist_percentile(97);
            (*pc).ts = now; (*pc).p80 = p80; (*pc).p95 = p95; (*pc).p97 = p97;
            return (p80,p95,p97);
        }
    }
    (hist_percentile(80), hist_percentile(95), hist_percentile(97))
}

// ===== Writable account hotness EWMA =====
#[inline(always)]
fn acc_hot_update(now: u64, acc_pref8: u64) -> u32 {
    let mut ah = AccHot { ts: now, ew: 0 };
    unsafe { if let Some(v) = ACC_HOT_LRU.get(&acc_pref8) { ah = *v; } }
    let elapsed = now.wrapping_sub(ah.ts);
    let mut steps = (elapsed / 100_000_000) as u32; if steps > 32 { steps = 32; }
    let mut ew = ah.ew;
    let mut i = 0u32; while i < steps { ew = ew.saturating_sub(ew >> 3); i += 1; }
    ew = ew.saturating_add(256); // unit weight per sighting
    let upd = AccHot { ts: now, ew };
    unsafe { let _ = ACC_HOT_LRU.insert(&acc_pref8, &upd, 0); }
    ew
}

#[inline(always)]
fn acc_hot_penalty(ew: u32, heat: u32) -> u16 {
    // Heavier when slot is hot; mild otherwise. Tuned to hit account-floods, not organic flow.
    if heat > 80_000 {
        if ew > 8000 { 200 } else if ew > 3000 { 100 } else { 0 }
    } else {
        if ew > 12000 { 150 } else if ew > 5000 { 50 } else { 0 }
    }
}

// ===== IPv4 header-options hard-drop =====
#[inline(always)]
fn ipv4_has_options(ip: *const IpHdr) -> bool {
    let ihl = (unsafe { (*ip).ihl } & 0x0F) as u8;
    ihl != 5 // TPU paths never need IP options; treat as junk
}

#[inline(always)]
fn ipv4_header_len_bytes(ip: *const IpHdr) -> usize {
    (((unsafe { (*ip).ihl } & 0x0F) as usize) * 4).max(20)
}

// ===== Slow signature Bloom =====
#[inline(always)]
fn sigslow_hit_or_set(now: u64, h: u64) -> bool {
    unsafe {
        if let Some(b) = SIGSLOW_PCPU.get_ptr_mut(0) {
            if now.wrapping_sub((*b).epoch_ns) > 900_000_000 {
                (*b).epoch_ns = now.saturating_sub(jitter_ns_small());
                (*b).words = [0u64; SB_WORDS]; (*b).set_ctr = 0;
            }
            let salt = flow_salt() as u64;
            let h1 = hash_mix64(h ^ (salt << 1)); let h2 = hash_mix64(h ^ 0xC4CE_B9FE_1A85_EC53u64);
            let i1 = (h1 as usize) & (SB_BITS - 1); let i2 = (h2 as usize) & (SB_BITS - 1);
            let (w1,m1,w2,m2) = (i1/64, 1u64<<(i1&63), i2/64, 1u64<<(i2&63));
            let hit = (((*b).words[w1] & m1) != 0) & (((*b).words[w2] & m2) != 0);
            if !hit {
                (*b).words[w1] |= m1; (*b).set_ctr = (*b).set_ctr.saturating_add(1);
                (*b).words[w2] |= m2; (*b).set_ctr = (*b).set_ctr.saturating_add(1);
            }
            hit
        } else { false }
    }
}

// ===== Flow inter-arrival guard =====
#[inline(always)]
fn flow_gap_drop(now: u64, fk: u64) -> bool {
    let mut st = FlowTs { ts: now, hits: 0 };
    unsafe { if let Some(v) = FLOW_TS_LRU.get(&fk) { st = *v; } }
    let gap = now.wrapping_sub(st.ts);
    let mut drop = false;
    // three hits inside 150µs ⇒ drop subsequent ones for ~300µs
    if gap < 150_000 {
        let nh = st.hits.saturating_add(1);
        if nh >= 3 { drop = true; }
        st.hits = nh;
    } else {
        st.hits = 1;
    }
    st.ts = now;
    unsafe { let _ = FLOW_TS_LRU.insert(&fk, &st, 0); }
    drop
}

// ===== VLAN peel (802.1Q / 802.1AD) =====
#[inline(always)]
fn peel_vlan(mut h_proto: u16, data: *const u8, mut off: usize, end: usize) -> Result<(u16, usize), ()> {
    // Peel up to 2 VLAN tags (802.1Q 0x8100, 802.1AD 0x88A8)
    let mut layers = 0u32;
    while (h_proto == u16::from_be(0x8100) || h_proto == u16::from_be(0x88A8)) && layers < 2 {
        if off + core::mem::size_of::<VlanHdr>() > end { return Err(()); }
        // Capture TCI for PCP extraction
        let tci = unsafe { *(data.add(off) as *const u16) };
        stash_pcp_from_tci(tci);
        // next protocol after VLAN header lives at data[off+2..off+4]
        let tp = unsafe { *(data.add(off + 2) as *const u16) };
        h_proto = tp;
        off += core::mem::size_of::<VlanHdr>();
        layers += 1;
    }
    Ok((h_proto, off))
}

// ===== UDP destination allowlist =====
#[inline(always)]
fn udp_allowed(dst_port: u16) -> bool {
    // If disabled or empty, allow all; if enabled, require presence.
    let enabled = unsafe { PORT_ALLOW_CFG.get(0).map(|c| unsafe { (*c).enable }).unwrap_or(0) } != 0;
    if enabled && port_neg_hit(dst_port) { return false; }
    if !enabled { return true; }
    let found = unsafe { PORT_ALLOW.get(&dst_port).is_some() };
    if !found { port_neg_set(dst_port); }
    found
}

// ===== Program-prefix token bucket =====
#[inline(always)]
fn pfx_rate_limit_hot(now: u64, prefix8: u64, heat: u32) -> bool {
    if heat <= 80_000 { return false; } // active only when boiling
    let mut st = PfxTok { ts: now, tok: 64 };
    unsafe { if let Some(v) = PFX_TOK_LRU.get(&prefix8) { st = *v; } }
    let elapsed = now.wrapping_sub(st.ts);
    // Refill 4 tokens every 250µs, cap 64
    if elapsed >= 250_000 {
        let add = ((elapsed / 250_000) as u16).saturating_mul(4);
        let nt = st.tok.saturating_add(add);
        st.tok = if nt > 64 { 64 } else { nt };
        st.ts = now;
    }
    if st.tok == 0 { return true; }
    st.tok -= 1;
    unsafe { let _ = PFX_TOK_LRU.insert(&prefix8, &st, 0); }
    false
}

// ===== Queue-pressure admission guard =====
#[inline(always)]
fn tokens_avail(q: u32) -> bool {
    unsafe {
        if let Some(b) = XSK_QBUDGET.get_ptr(q) { return unsafe { (*b).tokens } > 0; }
    }
    true
}

#[inline(always)]
fn queues_saturated_drop(event: &PacketEvent) -> bool {
    let snap = cfg_snapshot(now_cached());
    let (qmask, strat) = (snap.qmask, snap.strat);
    let cpu  = cpu_cached();
    let salt = flow_salt();
    let hash_q = (flow_prehash(event) ^ (event.hash as u32) ^ salt) & qmask;
    let ultra_like = is_top1_score() || event.priority >= 14;
    
    // Tail bias uses health delta (hashed if clearly healthier)
    let h_cpu  = qhealth(cpu & qmask);
    let h_hash = qhealth(hash_q);
    let (s_ns, _, _, _) = slot_snapshot();
    let tail = now_cached() % s_ns > (s_ns - (s_ns / 8));
    let prefer_hash_tail = tail && (h_hash > h_cpu.saturating_add(64));
    
    let pref_q = if (strat & 0x01) != 0 && (strat & 0x02) == 0 { cpu & qmask }
                 else if (strat & 0x02) != 0 && (strat & 0x01) == 0 { hash_q }
                 else if ultra_like && !prefer_hash_tail { cpu & qmask } else { hash_q };

    // same rotation as spill
    let now = unsafe { bpf_ktime_get_ns() };
    let sb  = (slot_bucket_ns(now) as u32) & 0x7;
    let r   = ((event.hash as u32) ^ (cpu << 1) ^ (sb << 2)) & 0x7;
    let a1 = (pref_q ^ 0x1) & qmask; let a2 = (pref_q ^ 0x2) & qmask; let a3 = (pref_q ^ 0x4) & qmask;
    let c1 = if (r & 1) != 0 { a2 } else { a1 };
    let c2 = if (r & 2) != 0 { a3 } else { a2 };
    let c3 = if (r & 4) != 0 { a1 } else { a3 };

    // If none have tokens and not a hot/ultra packet, drop it here.
    if !tokens_avail(pref_q) && !tokens_avail(c1) && !tokens_avail(c2) && !tokens_avail(c3) {
        let (_, p95, _) = pctls(now);
        let sbuck = score_to_bucket(cheap_pre_score(event));
        if sbuck + 2 < p95 && event.priority < 12 { return true; }
    }
    false
}

// ===== Ultra-fast ilog10 for hot proxies =====
#[inline(always)]
fn ilog10_scaled_fast(x: u64) -> u32 {
    // Approx log10(x) * 16 using integer log2 and a fixed-point factor (log10(2)≈0.30103)
    if x <= 1 { return 0; }
    let l2 = 63u32 - x.leading_zeros();                  // floor(log2)
    let frac = (x >> (l2.saturating_sub(16))) & 0xFFFF;  // crude fraction on 16 bits
    // 16 * log10(x) ≈ 16*(l2*log10(2) + frac/2^16*log10(2))
    let k = 19728u32; // round(16*log10(2)*2^16) = 0.30103*16*65536 ≈ 315655 → scale split
    ((l2 << 16) + frac) * k >> 16  // returns ~16*log10(x)
}

// ===== Source×Program fairness =====
#[inline(always)]
fn srcprog_limit(now: u64, src_ip_be: u32, program_id: &[u8;32], heat: u32) -> bool {
    if heat <= 80_000 { return false; }
    let src16 = (src_ip_be >> 16) & 0xFFFF;
    let p16 = ((program_id[1] as u32) << 8) | (program_id[0] as u32);
    let key = (src16 << 16) | p16;
    let mut st = SpLim { ts: now, tok: 16 };
    unsafe { if let Some(v) = SP_LIMIT.get(&key) { st = *v; } }
    let elapsed = now.wrapping_sub(st.ts);
    if elapsed >= 500_000 {
        let add = ((elapsed / 500_000) as u8).saturating_mul(2);
        let nt = (st.tok as u16 + add as u16).min(16) as u8;
        st.tok = nt; st.ts = now;
    }
    if st.tok == 0 { return true; }
    st.tok -= 1;
    unsafe { let _ = SP_LIMIT.insert(&key, &st, 0); }
    false
}

// ===== Harden QUIC short-header parse =====
#[inline(always)]
fn quic_pn_len_ok(b0: u8) -> bool {
    let pnlen = ((b0 & 0x03) as usize) + 1;
    pnlen <= 4
}

// ===== Early "alpha-only" when rings are critically saturated =====
#[inline(always)]
fn rings_critically_dry(event: &PacketEvent) -> bool {
    let snap = cfg_snapshot(now_cached());
    let (qmask, strat) = (snap.qmask, snap.strat);
    let cpu  = cpu_cached();
    let salt = flow_salt();
    let hash_q = (flow_prehash(event) ^ (event.hash as u32) ^ salt) & qmask;
    let pref_q = if (strat & 0x01) != 0 && (strat & 0x02) == 0 { cpu & qmask }
                 else if (strat & 0x02) != 0 && (strat & 0x01) == 0 { hash_q }
                 else { hash_q };
    // Consider dry if preferred & 2 alts lack tokens (cheaper than reading all)
    let a1 = (pref_q ^ 0x1) & qmask; let a2 = (pref_q ^ 0x2) & qmask;
    (!tokens_avail(pref_q)) & (!tokens_avail(a1)) & (!tokens_avail(a2))
}

// ===== Per-CPU salt for flow sharding =====
#[repr(C)]
#[derive(Clone, Copy)]
struct Salt { val: u32, ts: u64 }

#[map]
static FLOW_SALT_PCPU: PerCpuArray<Salt> = PerCpuArray::with_max_entries(1, 0);

// ===== Per-CPU bundle queue hint =====
#[repr(C)]
#[derive(Clone, Copy)]
struct BundleHint { qid: u32, _pad: u32, ts: u64, key: u64 }

#[map]
static BUNDLE_HINT_PCPU: PerCpuArray<BundleHint> = PerCpuArray::with_max_entries(1, 0);

// ===== Program prefix value EWMA (lamports) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct PrefixVal { ts: u64, ew: u64 }

#[map]
static PROGRAM_PREFIX_VALUE: LruHashMap<u64, PrefixVal> = LruHashMap::with_max_entries(4096, 0);

// ===== DCID Count-Min sketch (per-CPU, 2×64 counters) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct CmSketch { epoch_ns: u64, c0: [u16;64], c1: [u16;64] }

#[map]
static CM_DCID_PCPU: PerCpuArray<CmSketch> = PerCpuArray::with_max_entries(1, 0);

// Per-CPU bundle flags (hot hint)
#[repr(C)]
#[derive(Clone, Copy)]
struct BundleFlags { hot: u8, _pad: [u8;7], ts: u64 }

#[map]
static BUNDLE_FLAGS_PCPU: PerCpuArray<BundleFlags> = PerCpuArray::with_max_entries(1, 0);

// ===== Fee/CU sanity =====
#[repr(C)]
#[derive(Clone, Copy)]
struct FeeCfg { min_fee_per_cu: u32 } // in lamports per CU

#[map]
static FEE_CFG: Array<FeeCfg> = Array::with_max_entries(1, 0);

// ===== DCID priority EWMA (LRU) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct PrioEma { ts: u64, ew: u16 } // ew scaled by ×16

#[map]
static BUNDLE_PRIO_EMA: LruHashMap<u64, PrioEma> = LruHashMap::with_max_entries(16384, 0);

// ===== Per-CPU instruction fingerprint Bloom (1k bits) =====
const IF_BLOOM_BITS: usize  = 1024;
const IF_BLOOM_WORDS: usize = IF_BLOOM_BITS / 64;

#[repr(C)]
#[derive(Clone, Copy)]
struct IFpBloom { words: [u64; IF_BLOOM_WORDS], epoch_ns: u64, set_ctr: u16 }

#[map]
static IFP_BLOOM_PCPU: PerCpuArray<IFpBloom> = PerCpuArray::with_max_entries(1, 0);

// ===== Per-CPU 2-entry account-index fingerprint =====
#[repr(C)]
#[derive(Clone, Copy)]
struct AccIdx2 { k1: u64, k2: u64, t1: u64, t2: u64 }

#[map]
static ACCIDX_PCPU: PerCpuArray<AccIdx2> = PerCpuArray::with_max_entries(1, 0);

// ===== Per-CPU 1k-bit bloom for account-index fingerprint =====
const AIF_BLOOM_BITS: usize  = 1024;
const AIF_BLOOM_WORDS: usize = AIF_BLOOM_BITS / 64;

#[repr(C)]
#[derive(Clone, Copy)]
struct AifBloom { words: [u64; AIF_BLOOM_WORDS], epoch_ns: u64, set_ctr: u16 }

#[map]
static AIF_BLOOM_PCPU: PerCpuArray<AifBloom> = PerCpuArray::with_max_entries(1, 0);

// ===== Per-CPU flash prefix micro-cache (4 entries) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct FpCache { k: [u16;4], ts: [u64;4] }

#[map]
static FP_CACHE_PCPU: PerCpuArray<FpCache> = PerCpuArray::with_max_entries(1, 0);

// ===== Signature 32-bit guard for LRU confirm =====
#[map]
static SIGCHK_LRU: LruHashMap<u64, u32> = LruHashMap::with_max_entries(131072, 0);

#[inline(always)]
fn sigcheck32(sig: &[u8; 64]) -> u32 {
    // SplitMix-style fold over 8×8B chunks (bounded, verifier-safe)
    let mut i = 0usize;
    let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
    while i < 64 {
        let c = u64::from_le_bytes([
            sig[i],sig[i+1],sig[i+2],sig[i+3],sig[i+4],sig[i+5],sig[i+6],sig[i+7]
        ]);
        x ^= c; x ^= x >> 30; x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
        x ^= x >> 27; x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
        x ^= x >> 31;
        i += 8;
    }
    (x as u32) ^ ((x >> 32) as u32)
}

// ===== Per-source token bucket (cheap spam limiter) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct SrcTok { ts: u64, tokens: u16 }

#[map]
static SRC_LIMIT: LruHashMap<u32, SrcTok> = LruHashMap::with_max_entries(16384, 0);

#[inline(always)]
fn src_token_take(now: u64, src_ip_be: u32) -> bool {
    unsafe {
        if VIP_DST32.get(&src_ip_be).is_some()
            || VIP_DST24.get(&(src_ip_be & 0xFFFF_FF00)).is_some()
            || VIP_DST16.get(&(src_ip_be & 0xFFFF_0000)).is_some() {
            return false; // VIP exempt
        }
        let mut st = SrcTok { ts: now, tokens: 64 };
        if let Some(v) = SRC_LIMIT.get(&src_ip_be) { st = *v; }
        let elapsed = now.wrapping_sub(st.ts);
        // Refill 8 tokens every 500us, cap 128
        if elapsed >= 500_000 {
            let add = ((elapsed / 500_000) as u16).saturating_mul(8);
            let nt = st.tokens.saturating_add(add);
            st.tokens = if nt > 128 { 128 } else { nt };
            st.ts = now;
        }
        if st.tokens == 0 { return true; }
        st.tokens -= 1;
        let _ = SRC_LIMIT.insert(&src_ip_be, &st, 0);
        false
    }
}

#[inline(always)]
fn ipv4_is_fragment(ip: *const IpHdr) -> bool {
    // frag_off: | flags(3) | fragment_offset(13) |  network order
    let fo = unsafe { (*ip).frag_off };
    let v = u16::from_be(fo);
    // MF or nonzero offset -> fragmented
    (v & 0x3FFF) != 0
}

#[inline(always)]
fn ipv4_fragmented(ip: *const IpHdr) -> bool {
    // frag_off: | flags(3) | offset(13) |, network byte order
    let fo = u16::from_be(unsafe { (*ip).frag_off });
    (fo & 0x1FFF) != 0 || (fo & 0x2000) != 0 // offset!=0 or MF=1
}

#[inline(always)]
fn udp_src_zero_drop(udp: *const UdpHdr) -> bool {
    u16::from_be(unsafe { (*udp).source }) == 0
}

#[inline(always)]
fn insane_instcount_drop(inst_cnt: usize) -> bool {
    heat_cached() > 80_000 && inst_cnt > 64 // TPU money paths never need >64 instructions
}

#[inline(always)]
fn ipv4_ttl_too_low(ip: *const IpHdr) -> bool {
    if heat_cached() <= 80_000 { return false; }
    let ttl = unsafe { (*ip).ttl };
    ttl < 16 // conservative floor; TPU paths typically ≥32/64
}

#[inline(always)]
fn prio_jitter(event: &PacketEvent) -> u8 {
    // stable per-packet nibble; avoids priority herding on identical scores
    (((event.hash as u32) ^ flow_salt() as u32) & 0x3) as u8 // 0..3
}

#[inline(always)]
fn ip_udp_len_sane(ip: *const IpHdr, udp: *const UdpHdr) -> bool {
    let tot = u16::from_be(unsafe { (*ip).tot_len }) as usize;
    let ulen = u16::from_be(unsafe { (*udp).len }) as usize;
    // tot_len covers IP header + UDP hdr + payload; require consistency
    if tot < IP_HLEN + UDP_HLEN { return false; }
    if ulen < UDP_HLEN { return false; }
    if (IP_HLEN + ulen) != tot { return false; }
    // Soft clamp payload to 1400 bytes to avoid pathological jumbo (QUIC/TPU fits)
    let pay = ulen - UDP_HLEN;
    pay <= 1400
}

// ===== Per-CPU 2-entry victim cache for dupes (zero map touch) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct Victim2 { k1: u64, k2: u64, t1: u64, t2: u64 }

#[map]
static VICTIM_PCPU: PerCpuArray<Victim2> = PerCpuArray::with_max_entries(1, 0);

// ===== Per-CPU 2-entry instruction fingerprint cache =====
#[repr(C)]
#[derive(Clone, Copy)]
struct InstrFp2 { k1: u64, k2: u64, t1: u64, t2: u64 }

#[map]
static INSTRFP_PCPU: PerCpuArray<InstrFp2> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn instr_fp_key(program_id: &[u8;32], first8: u64) -> u64 {
    // mix 4B program prefix with first 8B data
    let p = u32::from_le_bytes([program_id[0],program_id[1],program_id[2],program_id[3]]) as u64;
    hash_mix64(p ^ first8 ^ 0x517c_c1b1_9e37_79b9)
}

#[inline(always)]
fn instrfp_hit_or_set(now: u64, key: u64) -> bool {
    unsafe {
        if let Some(v) = INSTRFP_PCPU.get_ptr_mut(0) {
            if (*v).k1 == key { (*v).t1 = now; return true; }
            if (*v).k2 == key { (*v).t2 = now; return true; }
            // Insert into older slot
            let use1 = now.wrapping_sub((*v).t1) > now.wrapping_sub((*v).t2);
            if use1 { (*v).k1 = key; (*v).t1 = now; } else { (*v).k2 = key; (*v).t2 = now; }
        }
    }
    false
}

// ===== 2-byte program prefix Bloom (soft rarity firewall) =====
const PP_BLOOM_BITS: usize = 1024;
const PP_BLOOM_WORDS: usize = PP_BLOOM_BITS / 64;

#[repr(C)]
#[derive(Clone, Copy)]
struct PpBloom { words: [u64; PP_BLOOM_WORDS], epoch_ns: u64, set_ctr: u16 }

#[map]
static PP_BLOOM_PCPU: PerCpuArray<PpBloom> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn pp_bloom_indices(k: u16, salt: u32) -> (usize, u64, usize, u64) {
    let x = (k as u64) ^ ((salt as u64) << 17);
    let h1 = hash_mix64(x);
    let h2 = hash_mix64(x ^ 0x9e37_79b9_517c_c1b1);
    let i1 = (h1 as usize) & (PP_BLOOM_BITS - 1);
    let i2 = (h2 as usize) & (PP_BLOOM_BITS - 1);
    (i1 / 64, 1u64 << (i1 & 63), i2 / 64, 1u64 << (i2 & 63))
}


#[inline(always)]
fn prefix_rare_penalty(program_id: &[u8; 32]) -> u16 {
    // small "first sighting" hit, disabled under heat
    let k = ((program_id[1] as u16) << 8) | (program_id[0] as u16);
    unsafe {
        if let Some(b) = PP_BLOOM_PCPU.get_ptr_mut(0) {
            let now = bpf_ktime_get_ns();
            if now.wrapping_sub((*b).epoch_ns) > 1_000_000_000 { (*b).words = [0u64; PP_BLOOM_WORDS]; (*b).epoch_ns = now.saturating_sub(jitter_ns_small()); (*b).set_ctr = 0; }
            let salt = flow_salt();
            let (w1,m1,w2,m2) = pp_bloom_indices(k, salt);
            let seen = ((*b).words[w1] & m1) != 0 && ((*b).words[w2] & m2) != 0;

            if !seen {
                (*b).words[w1] |= m1; (*b).set_ctr = (*b).set_ctr.saturating_add(1);
                (*b).words[w2] |= m2; (*b).set_ctr = (*b).set_ctr.saturating_add(1);
                // heat-aware: if slot heat is high, skip the penalty (reduce false throttling)
                if let Some(h) = SCORE_HIST_PCPU.get_ptr(0) { if (*h).total > 10_000 { return 0; } }
                return 50;
            } else { return 0; }
        }
    }
    0
}

#[inline(always)]
fn victim2_hit_or_set(now: u64, key: u64) -> bool {
    unsafe {
        if let Some(v) = VICTIM_PCPU.get_ptr_mut(0) {
            // check hits
            if (*v).k1 == key { (*v).t1 = now; return true; }
            if (*v).k2 == key { (*v).t2 = now; return true; }
            // insert into older slot
            let use1 = now.wrapping_sub((*v).t1) > now.wrapping_sub((*v).t2);
            if use1 {
                (*v).k1 = key; (*v).t1 = now;
            } else {
                (*v).k2 = key; (*v).t2 = now;
            }
            return false;
        }
    }
    false
}

#[inline(always)]
fn flow_salt() -> u32 {
    unsafe {
        if let Some(s) = SALT_PCPU.get_ptr_mut(0) {
            let now = now_cached();
            let v = (*s);
            let ts  = v & 0xFFFF_FFFF_FFFF_0000;
            let val = (v & 0xFFFF) as u32;
            if now.wrapping_sub(ts >> 16) > 200_000_000 {
                // reseed ~every 200ms: SplitMix on (now ^ prandom)
                let mut x = now ^ (bpf_get_prandom_u32() as u64);
                x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
                x ^= x >> 30; x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
                x ^= x >> 27; x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
                x ^= x >> 31;
                let new = ((now << 16) & 0xFFFF_FFFF_FFFF_0000) | (x as u64 & 0xFFFF);
                *s = new;
                return (x as u32) | 1; // keep odd
            }
            return (val | 1);
        }
    }
    (0xA5A5_5A5A | 1)
}

// ===== Program prefix value EWMA helpers =====
#[inline(always)]
fn prefix_value_update(prefix8: u64, now: u64, val: u64) -> u64 {
    let mut pv = PrefixVal { ts: now, ew: 0 };
    unsafe { if let Some(v) = PROGRAM_PREFIX_VALUE.get(&prefix8) { pv = *v; } }
    let elapsed = now.wrapping_sub(pv.ts);
    // decay every 100ms up to 32 steps (verifier-bounded)
    let mut steps = (elapsed / 100_000_000) as u32; if steps > 32 { steps = 32; }
    let mut ew = pv.ew;
    let mut i = 0u32;
    while i < steps {
        ew = ew.saturating_sub(ew >> 3); // ew -= ew/8
        i += 1;
    }
    // bump (cap per update to limit adversarial spikes)
    let add = if val > 1_000_000_000 { 1_000_000_000 } else { val };
    ew = ew.saturating_add(add >> 3); // 12.5% of observed value
    let upd = PrefixVal { ts: now, ew };
    unsafe { let _ = PROGRAM_PREFIX_VALUE.insert(&prefix8, &upd, 0); }
    ew
}

#[inline(always)]
fn value_floor_from_ewma(ew: u64) -> u64 {
    // ask 1/16th of recent family median-ish value (soft floor)
    (ew >> 4).clamp(20_000_000, 1_000_000_000) // 0.02–1.0 SOL
}

// ===== DCID heavy-hitter sketch helpers =====
#[inline(always)]
fn cm_idx64(k: u64, salt: u32) -> (usize, usize) {
    let h1 = hash_mix64(k ^ ((salt as u64) << 17));
    let h2 = hash_mix64(k ^ 0x9e37_79b9_517c_c1b1);
    ((h1 as usize) & 63, (h2 as usize) & 63)
}

#[inline(always)]
fn dcid_hot_mark(now: u64, key: u64) -> bool {
    unsafe {
        if let Some(s) = CM_DCID_PCPU.get_ptr_mut(0) {
            // 1s rotate
            if now.wrapping_sub((*s).epoch_ns) > 1_000_000_000 {
                (*s).epoch_ns = now;
                // zero two arrays; bounded clear
                let mut i = 0usize; while i < 64 { (*s).c0[i] = 0; (*s).c1[i] = 0; i += 1; }
            }
            let salt = flow_salt();
            let (i0,i1) = cm_idx64(key, salt);
            let a = (*s).c0[i0].saturating_add(1); (*s).c0[i0] = a;
            let b = (*s).c1[i1].saturating_add(1); (*s).c1[i1] = b;
            let m = if a < b { a } else { b };
            let hot = m > 64; // threshold ~64 hits/sec
            if let Some(f) = BUNDLE_FLAGS_PCPU.get_ptr_mut(0) {
                if hot { (*f).hot = 1; (*f).ts = now; } else { /* leave state */ }
            }
            hot
        } else { false }
    }
}

#[inline(always)]
fn bundle_hot_recent(now: u64) -> bool {
    unsafe {
        if let Some(f) = BUNDLE_FLAGS_PCPU.get_ptr(0) {
            return (*f).hot != 0 && now.wrapping_sub((*f).ts) < 1_000_000_000;
        }
    }
    false
}

// ===== Fee underwater check =====
#[inline(always)]
fn fee_underwater(ev: &PacketEvent) -> bool {
    let min_ppcu = cfg_snapshot(now_cached()).min_fee_per_cu as u64;
    let heat = heat_cached();
    // heat multiplier: calm=1×, moderate=1.5×, hot=2×
    let mult = if heat > 80_000 { 2u64 } else if heat > 50_000 { 3u64 / 2u64 } else { 1u64 };

    let is_vip = is_vip_dst_fast(ev.dst_ip.to_be());

    let min_ppcu = if is_vip { min_ppcu / 4 } else { min_ppcu.saturating_mul(mult) };
    let est_fee  = estimate_gas_priority_fee(ev).max(1);
    let need     = (ev.compute_units.max(1) as u64).saturating_mul(min_ppcu);

    est_fee < (need / 2) // cull only egregious losers
}

// ===== DCID priority smoothing =====
#[inline(always)]
fn dcid_prio_smooth(now: u64, key: u64, prio_now: u8) -> u8 {
    let mut pe = PrioEma { ts: now, ew: (prio_now as u16) << 4 };
    unsafe { if let Some(v) = BUNDLE_PRIO_EMA.get(&key) { pe = *v; } }
    // decay every 100ms, bounded 32 steps
    let mut steps = ((now.wrapping_sub(pe.ts)) / 100_000_000) as u32; if steps > 32 { steps = 32; }
    let mut ew = pe.ew;
    let mut i = 0u32;
    while i < steps { ew = ew.saturating_sub(ew >> 2); i += 1; } // ew -= ew/4
    ew = ew.saturating_add((prio_now as u16) << 4);
    let upd = PrioEma { ts: now, ew };
    unsafe { let _ = BUNDLE_PRIO_EMA.insert(&key, &upd, 0); }
    let sm = (ew >> 4) as u8; // back to 0..15
    if sm > prio_now { sm } else { prio_now } // only lift, never lower
}

// ===== Instruction fingerprint L2 Bloom helpers =====
#[inline(always)]
fn ifp_indices(k: u64, salt: u32) -> (usize, u64, usize, u64) {
    let h1 = hash_mix64(k ^ (salt as u64));
    let h2 = hash_mix64(k ^ 0x94D0_49BB_1331_11EBu64);
    let i1 = (h1 as usize) & (IF_BLOOM_BITS - 1);
    let i2 = (h2 as usize) & (IF_BLOOM_BITS - 1);
    (i1 / 64, 1u64 << (i1 & 63), i2 / 64, 1u64 << (i2 & 63))
}

#[inline(always)]
fn instrfp_bloom_hit_or_set(now: u64, key: u64) -> bool {
    unsafe {
        if let Some(b) = IFP_BLOOM_PCPU.get_ptr_mut(0) {
            if now.wrapping_sub((*b).epoch_ns) > 250_000_000 {
                (*b).epoch_ns = now.saturating_sub(jitter_ns_small()); (*b).words = [0u64; IF_BLOOM_WORDS]; (*b).set_ctr = 0;
            }
            let (w1,m1,w2,m2) = ifp_indices(key, flow_salt());
            let hit = ((*b).words[w1] & m1) != 0 && ((*b).words[w2] & m2) != 0;
            if !hit {
                (*b).words[w1] |= m1; (*b).set_ctr = (*b).set_ctr.saturating_add(1);
                (*b).words[w2] |= m2; (*b).set_ctr = (*b).set_ctr.saturating_add(1);
            }
            hit
        } else { false }
    }
}

// ===== Compute budget anti-bloat =====
#[inline(always)]
fn effective_compute_units_for_scoring(ev: &PacketEvent) -> u32 {
    // cap budget benefit by instruction & account footprint (branch-light)
    let inst = (ev.instructions_count as u32).max(1);
    let accs = (ev.accounts_count as u32).max(1);
    let hard = ev.compute_units.min(1_400_000);
    // allow up to (80k + 40k*inst + 8k*accs)
    let ceiling = 80_000u32
        .saturating_add(inst.saturating_mul(40_000))
        .saturating_add(accs.saturating_mul(8_000))
        .min(1_200_000);
    hard.min(ceiling)
}

// ===== Slot bucket helper =====
#[inline(always)]
fn slot_bucket_ns(now: u64) -> u64 {
    let s_ns = slot_ns();
    now / s_ns
}

// ===== Account-index fingerprint helpers =====
#[inline(always)]
fn accidx_fp_key(program_id: &[u8;32], idx16: u16) -> u64 {
    // mix (4B prog prefix || 2 idx bytes) with a keyed mixer
    let p = u32::from_le_bytes([program_id[0],program_id[1],program_id[2],program_id[3]]) as u64;
    hash_mix64((p << 16) ^ (idx16 as u64) ^ 0x9e37_79b9_517c_c1b1)
}

#[inline(always)]
fn accidx_hit_or_set(now: u64, key: u64) -> bool {
    unsafe {
        if let Some(v) = ACCIDX_PCPU.get_ptr_mut(0) {
            if (*v).k1 == key { (*v).t1 = now; return true; }
            if (*v).k2 == key { (*v).t2 = now; return true; }
            let use1 = now.wrapping_sub((*v).t1) > now.wrapping_sub((*v).t2);
            if use1 { (*v).k1 = key; (*v).t1 = now; } else { (*v).k2 = key; (*v).t2 = now; }
        }
    }
    false
}

#[inline(always)]
fn aif_indices(k: u64, salt: u32) -> (usize, u64, usize, u64) {
    let h1 = hash_mix64(k ^ (salt as u64));
    let h2 = hash_mix64(k ^ 0xBF58_476D_1CE4_E5B9u64);
    let i1 = (h1 as usize) & (AIF_BLOOM_BITS - 1);
    let i2 = (h2 as usize) & (AIF_BLOOM_BITS - 1);
    (i1 / 64, 1u64 << (i1 & 63), i2 / 64, 1u64 << (i2 & 63))
}

#[inline(always)]
fn accidx_bloom_hit_or_set(now: u64, key: u64) -> bool {
    unsafe {
        if let Some(b) = AIF_BLOOM_PCPU.get_ptr_mut(0) {
            if now.wrapping_sub((*b).epoch_ns) > 250_000_000 {
                (*b).epoch_ns = now.saturating_sub(jitter_ns_small()); (*b).words = [0u64; AIF_BLOOM_WORDS]; (*b).set_ctr = 0;
            }
            let (w1,m1,w2,m2) = aif_indices(key, flow_salt());
            let hit = ((*b).words[w1] & m1) != 0 && ((*b).words[w2] & m2) != 0;
            if !hit {
                (*b).words[w1] |= m1; (*b).set_ctr = (*b).set_ctr.saturating_add(1);
                (*b).words[w2] |= m2; (*b).set_ctr = (*b).set_ctr.saturating_add(1);
            }
            hit
        } else { false }
    }
}

// ===== Per-slot congestion stats (per-CPU) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct SlotStats { epoch_ns: u64, pkts: u32, smooth: u32 }

#[map]
static SLOT_STATS_PCPU: PerCpuArray<SlotStats> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn slot_congestion(now: u64) -> u32 {
    unsafe {
        if let Some(s) = SLOT_STATS_PCPU.get_ptr_mut(0) {
            let s_ns = slot_ns();
            let rollover = now.wrapping_sub((*s).epoch_ns) > s_ns;
            if rollover { (*s).epoch_ns = now; (*s).pkts = 0; }
            (*s).pkts = (*s).pkts.wrapping_add(1);
            // smooth = smooth - smooth/8 + 32  (bounded EWMA)
            let sm = (*s).smooth.saturating_sub((*s).smooth >> 3).saturating_add(32);
            (*s).smooth = sm;
            sm
        } else { 0 }
    }
}

#[inline(always)]
fn dynamic_thresholds(_pkts: u32) -> (u64, u32) {
    // Use histogram to set gates: p80 for lamports, p85 for CU (coarser is safer).
    let p80b = hist_percentile(80);
    let p85b = hist_percentile(85);
    // Map buckets back to thresholds; inverse of score_to_bucket monotone scale
    // We approximate using linear map against max scores (100_000)
    let lam = ((p80b as u64) * 100_000u64 / ((HIST_BUCKETS as u64) - 1)).saturating_mul(2_000_000); // ~2e6 lamports per score bucket
    let cu  = ((p85b as u64) * 100_000u64 / ((HIST_BUCKETS as u64) - 1)) as u32;
    // clamp into sane ranges
    let lam_min = lam.clamp(80_000_000, 500_000_000);   // 0.08–0.5 SOL
    let cu_min  = cu.clamp(180_000, 600_000);           // 180k–600k CU
    (lam_min, cu_min)
}

#[inline(always)]
fn classify_mev_type_v2(event: &PacketEvent, corr_flags: u8, is_bundle_flow: bool) -> u8 {
    let now = unsafe { bpf_ktime_get_ns() };
    let pkts = slot_congestion(now);
    let (lam_min, cu_min) = dynamic_thresholds(pkts);

    // Liquidation / flash-heavy
    if (effective_compute_units_for_scoring(event) as u32) > cu_min + 120_000 &&
       event.accounts_count > 8 &&
       event.lamports > lam_min + 60_000_000 &&
       detect_flash_loan_correlated(event, corr_flags) {
        return 4;
    }

    // Arbitrage
    if is_dex_program(&event.program_id) &&
       event.instructions_count >= 2 &&
       (effective_compute_units_for_scoring(event) as u32) > cu_min &&
       event.lamports > lam_min {
        return 2;
    }

    // Sandwich (bundle flow strongly indicative)
    if is_dex_program(&event.program_id) &&
       is_bundle_flow &&
       (effective_compute_units_for_scoring(event) as u32) > cu_min &&
       event.lamports > lam_min / 2 {
        return 3;
    }

    // Plain DEX
    if is_dex_program(&event.program_id) && event.lamports > lam_min / 4 { return 1; }

    // Flash correlation fallback for odd shapes
    if detect_flash_loan_correlated(event, corr_flags) { return 2; }

    0
}

// Priority-aware AF_XDP redirect wrapper
#[inline(always)]
fn qid_with_spill(qmask: u32, pref_q: u32, alt_seed: u32) -> u32 {
    unsafe {
        let idx0 = pref_q & qmask;
        if let Some(b0) = XSK_QBUDGET.get_ptr_mut(idx0) {
            let now = now_cached();
            let heat = heat_cached();
            let pres = umem_pressure();
            let period = if pres > 80 { 550_000 }
                         else if pres > 50 { 400_000 }
                         else if heat > 80_000 { 500_000 } else if heat > 50_000 { 350_000 } else { 250_000 };
            let el = now.wrapping_sub((*b0).last_refill);
            if el >= period {
                let adds = (el / period) as u16;
                (*b0).tokens = (*b0).tokens.saturating_add(adds.saturating_mul(8)).min(256);
                (*b0).last_refill = now;
                qslope_update(idx0, adds as u16);
            }
            let ultra_like = is_top1_score() || (heat_cached() > 90_000);
            let heat = heat_cached();
            let base_rsv = qresv(idx0) as u16;
            let eff_rsv  = if heat > 120_000 { base_rsv.saturating_add(8) }
                           else if heat > 100_000 { base_rsv.saturating_add(4) }
                           else { base_rsv };
            if (*b0).tokens > 0 {
                let had = (*b0).tokens > 0;
                mark_dead_probe(idx0, had, now);
                if dead_recent(idx0, now) {
                    // skip this candidate
                } else {
                    // honor reserve: only ultra can dip into ≤reserve depth
                    if (*b0).tokens as u16 <= eff_rsv && !ultra_like {
                        mark_starved(idx0, now);
                    } else {
                        (*b0).tokens -= 1; 
                        qhealth_update(idx0, (*b0).tokens as u16);
                        qvar_update(idx0, (*b0).tokens as u16);
                        return idx0;
                    }
                }
            }
            // preferred is dry → remember, so non-ultra flows can avoid it briefly
            mark_starved(idx0, now);
            
            let n1 = (idx0 ^ 0x1) & qmask;
            let n2 = (idx0 ^ 0x2) & qmask;
            mark_starved_nei(n1, now);
            mark_starved_nei(n2, now);
            
            // Ultra packets should try immediate neighbor first (stable locality step)
            let ultra = is_top1_score() || heat_cached() > 90_000;
            if ultra {
                let neigh = ((idx0 ^ 0x1) & qmask) as u32;
                if let Some(bn) = XSK_QBUDGET.get_ptr_mut(neigh) {
                    let had = (*bn).tokens > 0;
                    mark_dead_probe(neigh, had, now);
                    if !dead_recent(neigh, now) {
                        let base_rsv = qresv(neigh) as u16;
                        let eff_rsv = if heat > 120_000 { base_rsv.saturating_add(8) }
                                      else if heat > 100_000 { base_rsv.saturating_add(4) }
                                      else { base_rsv };
                        if (*bn).tokens as u16 > eff_rsv || ultra {
                            (*bn).tokens -= 1; 
                            qvar_update(neigh, (*bn).tokens as u16);
                            return neigh;
                        }
                    }
                }
            }
        }

            // slot/CPU-salted 3-alt rotation
            let cpu = bpf_get_smp_processor_id() as u32;
            let sb  = (slot_bucket_ns(now) as u32) & 0x7;
            let r   = (alt_seed ^ (cpu << 1) ^ (sb << 2)) & 0x7;

            let a1 = (pref_q ^ 0x1) & qmask;
            let a2 = (pref_q ^ 0x2) & qmask;
            let a3 = (pref_q ^ 0x4) & qmask;

            let c1 = if (r & 1) != 0 { a2 } else { a1 };
            let c2 = if (r & 2) != 0 { a3 } else { a2 };
            let c3 = if (r & 4) != 0 { a1 } else { a3 };

            let now = now_cached();
            let ultra_like = is_top1_score() || (heat_cached() > 90_000);
            let mut best_q = u32::MAX; let mut best_s = 0u32;

            macro_rules! consider {
                ($qid:expr, $bptr:ident) => {{
                    let rsv = if heat > 120_000 { qresv($qid) as u16 + 8 }
                              else if heat > 100_000 { qresv($qid) as u16 + 4 }
                              else { qresv($qid) as u16 };
                    qhealth_update($qid, (*$bptr).tokens as u16);
                    qvar_update($qid, (*$bptr).tokens as u16);
                    qslope_update($qid, 0); // no explicit gain observed right now
                    if !starved_recent($qid, now) && !starved_nei_recent($qid, now) {
                        let s = qscore($qid, (*$bptr).tokens as u16, rsv, ultra_like);
                        if s > best_s { best_s = s; best_q = $qid; }
                    }
                }};
            }

            if let Some(b1) = XSK_QBUDGET.get_ptr_mut(c1) { consider!(c1, b1); }
            if let Some(b2) = XSK_QBUDGET.get_ptr_mut(c2) { consider!(c2, b2); }
            if let Some(b3) = XSK_QBUDGET.get_ptr_mut(c3) { consider!(c3, b3); }
            if best_q != u32::MAX {
                if let Some(b) = XSK_QBUDGET.get_ptr_mut(best_q) { 
                    (*b).tokens -= 1; 
                    qvar_update(best_q, (*b).tokens as u16);
                }
                return best_q;
            }
        }
    }
    pref_q & qmask
}

#[inline(always)]
fn redirect_priority(ctx: &XdpContext, event: &PacketEvent) -> u32 {
    let snap = cfg_snapshot(now_cached());
    let (qmask, strat) = (snap.qmask, snap.strat);

    let now = now_cached();
    let mut dcid_key: u64 = 0;
    if (event.flags & 0x01) != 0 { // bundle
        if let Some(h) = unsafe { BUNDLE_HINT_PCPU.get_ptr(0) } {
            dcid_key = unsafe { (*h).key };
        }
    }
    let fk = flow_key(event, dcid_key);

    // Sticky queue within a tiny window (prevents flip-flop)
    let sfrac = slot_ns() / 800; // ~0.125% of slot (~0.5ms @ 400ms)
    if let Some(stk) = unsafe { FLOW_QSTICK.get(&fk) } {
        if now.wrapping_sub(unsafe { (*stk).ts }) < sfrac {
            let qid = unsafe { (*stk).qid } & qmask;
            return unsafe { XSK_SOCKS.redirect(ctx, qid) };
        }
    }

    // If we recently saw this dst_ip on a queue, stick to it (warms cache), VIP extends TTL
    let is_vip = is_vip_dst_fast(event.dst_ip.to_be());
    let ext = if is_vip { 6_000_000 } else { 3_000_000 };
    let dip = event.dst_ip.to_be();
    if let Some(qp) = unsafe { DST_QPIN.get(&dip) } {
        if now.wrapping_sub(qp.ts) < ext {
            let qid = qp.qid & qmask;
            return unsafe { XSK_SOCKS.redirect(ctx, qid) };
        }
    }

    // Bundle hint: deterministic DCID-affined queue for current packet
    if (event.flags & 0x01) != 0 {
        if let Some(h) = unsafe { BUNDLE_HINT_PCPU.get_ptr(0) } {
            let key = unsafe { (*h).key };
            if key != 0 {
                // Persistent DCID pin (fresh < ~4ms) before any strategy logic
                let ext = if is_vip_dst_fast(event.dst_ip.to_be()) { 6_000_000 } else { 4_000_000 };
                if let Some(qp) = unsafe { FLOW_QPIN.get(&key) } {
                    if now.wrapping_sub(qp.ts) < ext {
                        let qid = qp.qid & qmask;
                        return unsafe { XSK_SOCKS.redirect(ctx, qid) };
                    }
                }
                // hint freshness tied to slot size (≈ 0.25% of slot)
                let sfrac = slot_ns() / 400; // ~1ms at 400ms slots
                if now.wrapping_sub(unsafe { (*h).ts }) < sfrac {
                    let qid = unsafe { (*h).qid } & qmask;
                    return unsafe { XSK_SOCKS.redirect(ctx, qid) };
                }
            }
        }
    }

    // Optional NUMA-aware CPU→queue override
    let cpu  = cpu_cached();
    if let Some(qo) = cpuq_override(cpu, qmask) {
        let qid = qo & qmask;
        return unsafe { XSK_SOCKS.redirect(ctx, qid) };
    }

    // Salted selection (HYBRID semantics) + multi-alt spill
    let salt = flow_salt();
    let hash_q = (flow_prehash(event) ^ (event.hash as u32) ^ salt) & qmask;
    
    // Top-1% score behaves as ultra for locality (CPU ring)
    let mut ultra_like = is_top1_score() || event.priority >= 14;
    
    // DSCP bias: high DSCP values lift to CPU-local at heat
    let dscp = unsafe { SCRATCH_PCPU.get_ptr(0).map(|p| unsafe { (*p).ip_dscp }).unwrap_or(0) };
    if dscp_bias_ultra(dscp) { ultra_like = true; } // DSCP lifts to CPU-local at heat
    // PCP≥min lifts to CPU-local at slot tail (unless already ultra)
    if !ultra_like {
        if let Some(s) = unsafe { SCRATCH_PCPU.get_ptr(0) } {
            let pcp = unsafe { (*s).vlan_pcp };
            let s_ns = slot_ns();
            let phase = now_cached() % s_ns;
            if vlan_pcp_bias_ok(pcp) && phase > (s_ns - (s_ns / 8)) {
                ultra_like = true;
            }
        }
    }

    // Tail-of-slot: favor hashed queues over CPU-local unless ultra_like
    let s_ns2 = slot_ns();
    let phase2 = now % s_ns2;
    let prefer_hash = phase2 > (s_ns2 - (s_ns2 / 8)) && !ultra_like;
    
    let pref_q = if (strat & 0x01) != 0 && (strat & 0x02) == 0 { cpu & qmask }
                 else if (strat & 0x02) != 0 && (strat & 0x01) == 0 { hash_q }
                 else if ultra_like && !prefer_hash { cpu & qmask } else { hash_q };

    let qid = qid_with_spill(qmask, pref_q, (event.hash as u32));
    
    // Record stickiness for next packet in the burst
    let _ = unsafe { FLOW_QSTICK.insert(&fk, &QStick { qid, ts: now }, 0) };
    
    // Record DCID pin for bundle flows
    if (event.flags & 0x01) != 0 {
        if let Some(h) = unsafe { BUNDLE_HINT_PCPU.get_ptr(0) } {
            let key = unsafe { (*h).key };
            if key != 0 {
                let now = now_cached();
                let _ = unsafe { FLOW_QPIN.insert(&key, &QPin { qid, ts: now }, 0) };
            }
        }
    }
    
    // Success admission → grant small credit to this program family (buffers future bursts)
    let roi = roi_benefit(event);
    let p8 = prog_prefix8_cached(event);
    pfx_credit_give_roi(now_cached(), p8, roi, event.priority);
    pfx_loss_relief(p8);
    pfx_quality_touch(now_cached(), p8, true);
    
    // Record destination queue pin for future packets
    let _ = unsafe { DST_QPIN.insert(&event.dst_ip.to_be(), &QPinDst { qid, ts: now_cached() }, 0) };
    
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
fn composite_tx_key(sig_hash: u64, now: u64) -> u64 { sig_hash ^ slot_bucket_ns(now).rotate_left(13) }

#[inline(always)]
fn cache_ttl_ns(event: &PacketEvent) -> u64 {
    let base = match event.mev_type {
        3 => 5_000_000_000, 2 => 3_000_000_000, 4 => 10_000_000_000, 1 => 2_000_000_000, _ => 1_000_000_000
    };
    let adj = if event.priority < 5 { base / 2 } else { base };
    // ±2% jitter to decorrelate expirations (bounded; verifier-safe)
    let r = unsafe { bpf_get_prandom_u32() } as u64;
    let jitter = (adj / 50) * ((r & 3) as u64).saturating_sub(1); // {-1,0,1,2}% but capped
    adj.saturating_add(jitter)
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
    // Piecewise: 0..80k linear; 80k..100k stretched to occupy 40% of buckets
    let buckets = (HIST_BUCKETS as u64) - 1;
    let b = if s <= 80_000 {
        (s * (buckets * 6 / 10)) / 80_000  // 60% buckets for first 80%
    } else {
        let top = s - 80_000;               // 0..20k
        (buckets * 6 / 10) + (top * (buckets * 4 / 10)) / 20_000
    };
    (if b > buckets { buckets } else { b }) as usize
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

#[inline(always)]
fn hist_percentile(pct: u32) -> usize {
    unsafe {
        if let Some(h) = SCORE_HIST_PCPU.get_ptr(0) {
            let total = (*h).total; if total == 0 { return HIST_BUCKETS - 1; }
            let target = ((total as u64) * (pct as u64)).saturating_div(100) as u32;
            let mut accum = 0u32; let mut i = 0usize;
            while i < HIST_BUCKETS { accum = accum.wrapping_add((*h).counts[i]); if accum >= target { return i; } i += 1; }
            return HIST_BUCKETS - 1;
        }
        HIST_BUCKETS - 1
    }
}

#[inline(always)]
fn is_top1_score() -> bool {
    // Use p99 threshold and require it to hold across two reads spaced by slot fraction
    let p99a = hist_percentile(99);
    let p99b = hist_percentile(99);
    p99b >= p99a
}

#[inline(always)]
fn adaptive_routing(score: u32, est_latency_ns: u64) -> u8 {
    let now = unsafe { bpf_ktime_get_ns() };
    let heat = slot_congestion(now);
    let (_, p95, p97) = pctls(now);
    let p_hi = if heat > 80_000 { p97 } else { p95 };
    let cb  = score_to_bucket(score);
    let hi  = cb >= p_hi;
    let mut tier = if hi { 3 } else if cb + 2 >= p_hi { 2 } else if cb + 5 >= p_hi { 1 } else { 0 };
    if est_latency_ns < 1_000 && tier < 3 { tier += 1; }
    tier
}

// ===== Program prefix EWMA stats (time-decayed) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct PrefixStats { ts: u64, ewma: u32, count: u32 }

#[map]
static PROGRAM_PREFIX_STATS: LruHashMap<u64, PrefixStats> = LruHashMap::with_max_entries(4096, 0);

#[inline(always)]
fn prefix_hits_update(prefix8: u64, now: u64) -> (u32, u32) {
    // EWMA with ~1/8 decay per step; decay steps bounded to 32 to satisfy verifier
    let mut ps = PrefixStats { ts: now, ewma: 0, count: 0 };
    unsafe {
        if let Some(v) = PROGRAM_PREFIX_STATS.get(&prefix8) { ps = *v; }
    }
    let elapsed = now.wrapping_sub(ps.ts);
    // convert ns to steps (~100ms window)
    let mut steps = (elapsed / 100_000_000) as u32; // 100ms
    if steps > 32 { steps = 32; }

    let mut ew = ps.ewma;
    let mut i = 0u32;
    while i < steps {
        ew = ew.saturating_sub(ew >> 3); // ew -= ew/8
        i += 1;
    }

    // bump
    ew = ew.saturating_add(256); // unit weight

    let cnt = ps.count.saturating_add(1);
    let upd = PrefixStats { ts: now, ewma: ew, count: cnt };
    unsafe { let _ = PROGRAM_PREFIX_STATS.insert(&prefix8, &upd, 0); }

    (ew, cnt)
}

#[inline(always)]
fn rarity_penalty_ewma(ewma: u32) -> u16 {
    // Tuned to hit synthetic floods but leave organic traffic unharmed
    if ewma > 8000 { 200 } else if ewma > 1500 { 100 } else { 0 }
}

// ===== Early ultra-cheap drops + hot-sig sketch =====
#[map]
static HOT_SIG_SKETCH: LruHashMap<u64, u64> = LruHashMap::with_max_entries(1024, 0);

#[inline(always)]
fn unlikely_cheap_drop(event: &PacketEvent) -> bool {
    // Ultra-cheap drop for obviously low-value packets
    if event.lamports < 1_000_000 || event.compute_units < 10_000 || event.accounts_count < 2 {
        return true;
    }
    
    // Payload-size EWMA fence (heat-gated, per-CPU)
    if payload_too_large(now_cached(), event.data_len as u32) {
        return true;
    }
    
    false
}

#[inline(always)]
fn hot_sig_bump(now: u64, sig_hash: u64) -> bool {
    // Second hit within 1s = dupe
    unsafe {
        if let Some(ts) = HOT_SIG_SKETCH.get(&sig_hash) {
            if now.wrapping_sub(*ts) < 1_000_000_000 { return true; }
        }
        let _ = HOT_SIG_SKETCH.insert(&sig_hash, &now, 0);
    }
    false
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
fn cu_per_value_benefit(ev: &PacketEvent) -> u32 {
    // Smaller is better; map into [0..1000]
    if ev.lamports == 0 { return 0; }
    let cu = ev.compute_units.max(1) as u64;
    let v  = ev.lamports.max(1);
    // scale: cu per 1e8 lamports (≈1 SOL) → invert to benefit
    let ratio = (cu * 100_000_000) / v; // higher = worse
    let mut b = 1000i64 - (ratio as i64 / 1000i64).min(1000);
    if b < 0 { b = 0; }
    b as u32
}

#[inline(always)]
fn fee_afford_benefit(ev: &PacketEvent) -> u32 {
    // Simple, branch-light: higher estimated priority fee ⇒ higher benefit (0..2000)
    let est = estimate_gas_priority_fee(ev).min(50_000_000); // cap
    ((ilog10_scaled(est.saturating_add(1)) as u32) * 2000 / 16).min(2000)
}

#[inline(always)]
fn accounts_eff_benefit(ev: &PacketEvent) -> u32 {
    // Map small accounts_count to a positive benefit (0..800)
    let a = ev.accounts_count as u32;
    let b = if a <= 4 { 800 } else if a <= 8 { 400 } else if a <= 12 { 150 } else { 0 };
    b
}

#[inline(always)]
fn lpc_benefit(event: &PacketEvent) -> u32 {
    let cu = cu_eff_cached(event) as u64;
    if cu == 0 { return 0; }
    // scale lamports/CU into ~0..1200 band, guard outliers
    let v = ((event.lamports as u128) << 6) / (cu as u128); // ×64 for precision
    (v.min(1200) as u32)
}

#[inline(always)]
fn queue_pressure_penalty(event: &PacketEvent) -> u32 {
    let snap = cfg_snapshot(now_cached());
    let qmask = snap.qmask;
    let cpu_q = cpu_cached() & qmask;
    let hash_q = (flow_prehash(event) ^ (event.hash as u32) ^ (flow_salt() as u32)) & qmask;

    let mut tmin: u16 = 512;
    unsafe {
        if let Some(b1) = XSK_QBUDGET.get_ptr(cpu_q) { tmin = tmin.min((*b1).tokens as u16); }
        if let Some(b2) = XSK_QBUDGET.get_ptr(hash_q) { tmin = tmin.min((*b2).tokens as u16); }
    }
    // map low tokens to penalty up to ~800
    if tmin >= 32 { 0 } else { (800u32).saturating_sub((tmin as u32) * 25) }
}

#[inline(always)]
fn calculate_transaction_score_hardened(event: &PacketEvent, _ignored: u32) -> u32 {
    let mev = calculate_mev_score(event) as u32;
    let profit_clamped = if event.expected_profit > 100_000_000_000 { 100_000_000_000 } else { event.expected_profit };
    let profit_log = ilog10_scaled(profit_clamped.saturating_add(1)) * 1000;
    let cu_eff = effective_compute_units_for_scoring(event);
    let prio = (event.priority as u32).saturating_mul(800)
        .saturating_add((cu_eff / 2)); // modest CU lift, post-ceiling
    let latency = { let adv = estimate_latency_advantage(event); let cap = if adv > 5000 { 0 } else { 5000 - adv as u32 }; cap };
    let eff_cu = cu_per_value_benefit(event);
    let fee    = fee_afford_benefit(event);
    let accb   = accounts_eff_benefit(event);

    // prefix value EWMA shaping (soft penalty if way below recent)
    let prefix8 = u64::from_le_bytes([
        event.program_id[0], event.program_id[1], event.program_id[2], event.program_id[3],
        event.program_id[4], event.program_id[5], event.program_id[6], event.program_id[7]
    ]);
    let now = unsafe { bpf_ktime_get_ns() };
    let ew  = prefix_value_update(prefix8, now, event.lamports);
    let pf  = value_floor_from_ewma(ew);
    let deficit = if event.lamports >= pf { 0 } else { ((pf - event.lamports) / 1_000_000) as u32 }; // 1e6 lamports unit

    // base + soft penalty
    let roi = roi_benefit(event);
    let base = mev
        .saturating_add(profit_log.min(10_000))
        .saturating_add(prio)
        .saturating_add(latency)
        .saturating_add(eff_cu)
        .saturating_add(fee)
        .saturating_add(accb)
        .saturating_add(roi)
        .saturating_add(lpc_benefit(event));

    let base = base.saturating_add(urgency_bonus(now)).saturating_sub(queue_pressure_penalty(event));

    let pj = prio_jitter(event) as u32 * 50; // ≤150 points; preserves global ordering
    let base = base.saturating_add(pj);

    let penalty_rare = prefix_rare_penalty(&event.program_id) as u32;
    
    // Account hotness penalty
    let heat_local = slot_congestion(unsafe { bpf_ktime_get_ns() });
    let mut acc_pen: u32 = 0;
    if let Some(s) = unsafe { SCRATCH_PCPU.get_ptr(0) } {
        let p8 = unsafe { (*s).acc0_pref8 };
        if p8 != 0 {
            let ew = acc_hot_update(unsafe { bpf_ktime_get_ns() }, p8);
            acc_pen = acc_hot_penalty(ew, heat_local) as u32;
        }
    }
    let base = base.saturating_sub(acc_pen);
    
    let ploss = pfx_loss_penalty(prog_prefix8_cached(event));
    let base = base.saturating_sub(ploss);
    
    let base = base.saturating_sub(ecn_ce_penalty());
    
    base.saturating_sub(deficit.min(3_000)).saturating_sub(penalty_rare).min(100_000)
}

// ===== Flash loan correlation flags and helpers =====
const FL_BORROW: u8 = 0x01;
const FL_TRADE:  u8 = 0x02;
const FL_REPAY:  u8 = 0x04;

#[inline(always)]
fn flash_prog_prefix(program_id: &[u8; 32]) -> bool {
    let k = ((program_id[1] as u16) << 8) | (program_id[0] as u16);
    if fp_neg_hit(k) { return false; }

    unsafe {
        // L0: per-CPU 4-way check
        if let Some(fc) = FP_CACHE_PCPU.get_ptr_mut(0) {
            let mut i = 0usize;
            while i < 4 {
                if (*fc).k[i] == k { (*fc).ts[i] = bpf_ktime_get_ns(); return true; }
                i += 1;
            }
            // Miss → consult map once
            if FLASH_PREFIXES.get(&[program_id[0], program_id[1]]).is_some() {
                // Insert into the stalest slot
                let mut oldest = 0usize; let mut otime = (*fc).ts[0];
                let mut j = 1usize; let now = bpf_ktime_get_ns();
                while j < 4 {
                    if now.wrapping_sub((*fc).ts[j]) > now.wrapping_sub(otime) { oldest = j; otime = (*fc).ts[j]; }
                    j += 1;
                }
                (*fc).k[oldest] = k; (*fc).ts[oldest] = now;
                return true;
            } else {
                // map miss → remember negative
                fp_neg_set(k);
                return false;
            }
        } else {
            // No cache? fall back to map
            if FLASH_PREFIXES.get(&[program_id[0], program_id[1]]).is_some() { return true; } else {
                // map miss → remember negative
                fp_neg_set(k);
                return false;
            }
        }
    }
}

#[inline(always)]
fn correlate_flash_bits(program_id: &[u8; 32], data_len: u16, accs: u8, flags: &mut u8) {
    // Borrow/Trade/Repay triplet using bounded proxies only
    if flash_prog_prefix(program_id) && data_len >= 8 { *flags |= FL_BORROW; }
    if accs >= 4 && data_len >= 8 { *flags |= FL_TRADE; }
    if flash_prog_prefix(program_id) && data_len >= 4 && accs >= 2 { *flags |= FL_REPAY; }
}

#[inline(always)]
fn detect_flash_loan_correlated(event: &PacketEvent, corr_flags: u8) -> bool {
    // Exact triplet → flash loan
    if (corr_flags & (FL_BORROW | FL_TRADE | FL_REPAY)) == (FL_BORROW | FL_TRADE | FL_REPAY) {
        return true;
    }
    // Prefix + "heavy" shape → flash loan
    let suspicious = event.compute_units > 350_000 &&
                     event.instructions_count >= 3 &&
                     event.accounts_count >= 6;
    flash_prog_prefix(&event.program_id) && suspicious
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
            let s_ns = slot_ns();
            let rotate = now.wrapping_sub((*b).epoch_ns) > s_ns;
            if rotate { (*b).words = [0u64; FL_BLOOM_WORDS]; (*b).epoch_ns = now.saturating_sub(jitter_ns_small()); }
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
    let mut hproto = unsafe { (*eth_hdr).h_proto };
    let mut l2off = core::mem::size_of::<EthHdr>();
    // Peel optional VLANs (0 or up to 2), otherwise continue as-is
    if let Ok((hp2, off2)) = peel_vlan(hproto, ctx.data_ptr(), l2off, ctx.data_end()) {
        hproto = hp2; l2off = off2;
    }
    if hproto == u16::from_be(0x86DD) && !ipv6_allowed() { return Err(()); } // drop IPv6 unless enabled
    if hproto != u16::from_be(ETH_P_IP) { return Err(()); }

    let ip_hdr = (ctx.data_ptr().wrapping_add(l2off)) as *const IpHdr;
    if ipv4_has_options(ip_hdr) { return Err(()); }
    if unsafe { (*ip_hdr).protocol } != IPPROTO_UDP { return Err(()); }

    if ipv4_fragmented(ip_hdr) { return Err(()); }
    
    if ipv4_ttl_too_low(ip_hdr) { return Err(()); }
    
    stash_dscp(ip_hdr);
    stash_ecn(ip_hdr);
    let dscp = unsafe { SCRATCH_PCPU.get_ptr(0).map(|p| unsafe { (*p).ip_dscp }).unwrap_or(0) };
    if !dscp_allowed(dscp) { return Err(()); }
    if ecn_drop_under_extreme() { return Err(()); }

    let udp_hdr: *const UdpHdr = unsafe { ptr_at(ctx, l2off + ipv4_header_len_bytes(ip_hdr)) }?;
    
    if !ip_len_eq_udp(ip_hdr, udp_hdr) { return Err(()); }
    if !ipv4_header_checksum_ok(ip_hdr) { return Err(()); }
    if udp_checksum_bad(ip_hdr, udp_hdr, l2off, ctx) { return Err(()); }
    
    if udp_src_zero_drop(udp_hdr) { return Err(()); }
    
    if !ip_udp_len_sane(ip_hdr, udp_hdr) { return Err(()); }
    
    if !udp_len_header_ok(ip_hdr, udp_hdr) { return Err(()); }
    
    let tot = u16::from_be(unsafe { (*ip_hdr).tot_len }) as usize;
    if !frame_len_ok(ctx, l2off, tot) { return Err(()); }
    
    let dst_port = u16::from_be(unsafe { (*udp_hdr).dest }); 
    let src_port = u16::from_be(unsafe { (*udp_hdr).source }); 

    if !udp_allowed(dst_port) { return Err(()); }

    let payload_off = (l2off + ipv4_header_len_bytes(ip_hdr) + UDP_HLEN) as u16;
    let payload_len = (u16::from_be(unsafe { (*udp_hdr).len }) as usize).saturating_sub(UDP_HLEN);
    if payload_len < 24 || payload_len > MAX_PACKET_SIZE { return Err(()); }

    let data = ctx.data(); let data_end = ctx.data_end();
    if (data + payload_off as usize + payload_len) > data_end { return Err(()); }

    let payload = unsafe { core::slice::from_raw_parts((data + payload_off as usize) as *const u8, payload_len) };
    if !is_quic_like(payload) { return Err(()); }
    if looks_like_quic_vn(payload) { return Err(()); }

    // Congestion-aware VIP fast-lane
    let now = unsafe { bpf_ktime_get_ns() };
    let pkts = slot_congestion(now);
    if pkts > 80_000 {
        // BE address straight from header for VIP check
        let vip = vip_prio_boost(unsafe { (*ip_hdr).daddr });
        if vip == 0 { return Err(()); } // drop to later stages = reject early
    }

    if pressure_drop_udp_port(pkts, dst_port) { return Err(()); }

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
    now_cached_set(now);
    cpu_cached_set(unsafe { bpf_get_smp_processor_id() as u32 });
    
    // Prefetch hot offsets (account keys & first instruction)
    if payload.len() >= 24 {
        let base = ctx.data_ptr();
        let end = ctx.data_end();
        let ak0 = base.wrapping_add(24); // account keys start after header
        let inst = base.wrapping_add(32); // instruction table start
        if ak0 < end { prefetch(ak0); }
        if inst < end { prefetch(inst); }
    }
    
    let heat = slot_congestion(now);
    heat_cached_set(heat);
    map_budget_reset(now, heat);

    // VIP duplicate grace: shift only the timestamps used for dupe detection
    let dst_ip_be = event.dst_ip.to_be();
    let vip_grace = vip_dup_grace(now, dst_ip_be);
    let now_eff   = now.saturating_add(vip_grace);

    // Blockhash horizon: drop stale-slot replays when hot
    let mut bh_state: u8 = 0;
    if let Some(s) = unsafe { SCRATCH_PCPU.get_ptr(0) } {
        let bh8 = unsafe { (*s).bh8 };
        if bh8 != 0 { 
            bh_state = bh_classify(now, bh8);
            bh_edge_microflush(bh8);
        }
    }
    let heat = slot_congestion(now);
    if bh_state == 3 /* old */ && heat > 50_000 { return xdp_action::XDP_DROP; }

    // 0) Early cheap drop (configurable)
    if !alpha01 && unlikely_cheap_drop(event) { return xdp_action::XDP_DROP; }

    // Early "alpha-only" when rings are critically saturated
    if rings_critically_dry(event) && !(is_top1_score() || event.priority >= 14) {
        return xdp_action::XDP_DROP;
    }

    // Tail-slot freeze for low tiers (symmetric to early freeze)
    if tail_slot_freeze_drop(event) { return xdp_action::XDP_DROP; }

    // Fee-underwater losers: bail early under pressure
    if fee_underwater(event) { return xdp_action::XDP_DROP; }

    // Compute flow key early for ban tracking
    let fk = fk_cached(event, 0);

    // Sequential src-port spray detector (/24) — delta=±1 streak
    if seq24_drop(now, event.src_ip.to_be(), event.src_port) { return xdp_action::XDP_DROP; }

    // Check for VIP status for slot-head grace
    let is_vip = is_vip_dst_fast(event.dst_ip.to_be());
    let vip_head = vip_slothead_grace(is_vip);

    // Check for top-0.1% alpha shield
    let alpha01 = is_top01_score(event);

    // Early-slot cold-tier freeze (first ~1.6% of slot)
    if !vip_head && early_slot_freeze_drop(event) { return xdp_action::XDP_DROP; }

    // Check for src /24 ban escalation early
    if !vip_head && ban_src24_drop(event.src_ip.to_be()) { return xdp_action::XDP_DROP; }

    // Check for src /20 limiter early
    if !vip_head && src20_drop(now, event.src_ip.to_be()) { return xdp_action::XDP_DROP; }

    // Fixed-src-port burst drop (per /24 → TPU port)
    if fixed_port_burst_drop(now, event.src_ip.to_be(), event.dst_port) {
        pfx_loss_touch(prog_prefix8_cached(event));
        return xdp_action::XDP_DROP;
    }

    // L1 victim cache uses real time (instant replay), Bloom/LRU use now_eff
    if victim2_hit_or_set(now, event.hash) { ban_flow_short(fk); ban_backoff(now, fk); ban_src24_touch(event.src_ip.to_be()); pfx_loss_touch(prog_prefix8_cached(event)); pfx_quality_touch(now, prog_prefix8_cached(event), false); return xdp_action::XDP_DROP; }

    // L1b: instruction fingerprint cache (program-prefix + first8 data)
    if let Some(s) = unsafe { SCRATCH_PCPU.get_ptr(0) } {
        let f8 = unsafe { (*s).first8_data };
        if f8 != 0 {
            let k = instr_fp_key(&event.program_id, f8);
            if instrfp_hit_or_set(now, k) { ban_flow_short(fk); ban_backoff(now, fk); ban_src24_touch(event.src_ip.to_be()); pfx_loss_touch(prog_prefix8_cached(event)); pfx_quality_touch(now, prog_prefix8_cached(event), false); return xdp_action::XDP_DROP; }
        }
    }

    // L2 instruction-shape Bloom (~250ms horizon)
    if let Some(s) = unsafe { SCRATCH_PCPU.get_ptr(0) } {
        let f8 = unsafe { (*s).first8_data };
        if f8 != 0 {
            let k = instr_fp_key(&event.program_id, f8);
            if instrfp_bloom_hit_or_set(now, k) { ban_flow_short(fk); ban_backoff(now, fk); ban_src24_touch(event.src_ip.to_be()); pfx_loss_touch(prog_prefix8_cached(event)); pfx_quality_touch(now, prog_prefix8_cached(event), false); return xdp_action::XDP_DROP; }
        }
    }

    // L1/L2 account-index fingerprint: kills same-route spam (zero global map touch)
    if let Some(s) = unsafe { SCRATCH_PCPU.get_ptr(0) } {
        let idx16 = unsafe { (*s).accidx16 };
        if idx16 != 0 {
            let k = accidx_fp_key(&event.program_id, idx16);
            if accidx_hit_or_set(now, k) { ban_flow_short(fk); ban_backoff(now, fk); ban_src24_touch(event.src_ip.to_be()); pfx_loss_touch(prog_prefix8_cached(event)); pfx_quality_touch(now, prog_prefix8_cached(event), false); return xdp_action::XDP_DROP; }
            if accidx_bloom_hit_or_set(now, k) { ban_flow_short(fk); ban_backoff(now, fk); ban_src24_touch(event.src_ip.to_be()); pfx_loss_touch(prog_prefix8_cached(event)); pfx_quality_touch(now, prog_prefix8_cached(event), false); return xdp_action::XDP_DROP; }
        }
    }

    // ABA alternating spam killer (within ~2ms)
    if is_banned_flow(fk) { return xdp_action::XDP_DROP; }
    if aba_dupe(now, fk, event.hash) { return xdp_action::XDP_DROP; }

    // Flow inter-arrival guard (per-flow anti-microburst)
    if flow_gap_drop(now, fk) { return xdp_action::XDP_DROP; }

    // Heat-driven early bail for obvious non-contenders
    let heat2 = slot_congestion(now);
    if heat2 > 80_000 {
        let ps = cheap_pre_score(event);
        let psb = score_to_bucket_fast(ps);
        if !admit_hysteresis(psb) { return xdp_action::XDP_DROP; }
        
        // Per-prefix warm-start guard (new families must be near-cut)
        let p8 = prog_prefix8_cached(event);
        if pfx_warm_start_drop(now, p8, psb) { return xdp_action::XDP_DROP; }
    }

    // Heat-gated program family rate cap
    let p8 = prog_prefix8_cached(event);
    
    // DCID↔program-prefix conflict quarantine (anti-alias + slot-safe)
    let dcid_key = if (event.flags & 0x01) != 0 {
        if let Some(h) = unsafe { BUNDLE_HINT_PCPU.get_ptr(0) } { unsafe { (*h).key } } else { 0 }
    } else { 0 };
    if dcid_conflict_drop(now, dcid_key, p8) { return xdp_action::XDP_DROP; }
    
    let has_credit = pfx_credit_take(now, p8);
    if !vip_head && !has_credit && !alpha01 && pfx_rate_limit_hot(now, p8, heat2) { return xdp_action::XDP_DROP; }

    // Source×Program fairness (2-dimensional limiter)
    if !vip_head && !has_credit && !alpha01 && srcprog_limit(now, event.src_ip.to_be(), &event.program_id, heat2) {
        return xdp_action::XDP_DROP;
    }

    // Queue-pressure admission guard (don't shove into dry rings)
    if queues_saturated_drop(event) { return xdp_action::XDP_DROP; }

    // 1) Bundle flow tag (slot-sticky)
    if mark_bundle_flow(now, payload) { 
        event.flags |= 0x01; 
        if bundle_hot_recent(now) {
            event.flags |= 0x40; // HOT DCID
            event.priority = event.priority.saturating_add(1).min(15);
        }
        
        // Smooth priority for current DCID to reduce jitter
        if let Some(h) = unsafe { BUNDLE_HINT_PCPU.get_ptr(0) } {
            let key = unsafe { (*h).key };
            if key != 0 {
                event.priority = dcid_prio_smooth(now, key, event.priority);
            }
        }
    }

    // 2) Ultra-cheap "hot signature" sketch: second hit within 1s = dupe
    if hot_sig_bump(now_eff, event.hash) { return xdp_action::XDP_DROP; }

    // 2.25) Sigtiny 256-bit 2-of-2 (only under extreme heat, before DM)
    if heat_cached() > 100_000 && sigtiny_hit_or_set(now, event.hash) {
        ban_flow_short(fk); ban_backoff(now, fk); pfx_loss_touch(prog_prefix8_cached(event)); pfx_quality_touch(now, prog_prefix8_cached(event), false);
        return xdp_action::XDP_DROP;
    }

    // 2.4) Per-prefix signature DM (tiny, associative) to offload LRU
    if heat_cached() > 80_000 && pdm_hit_or_set(now, p8, event.hash) {
        ban_flow_short(fk); ban_backoff(now, fk); pfx_loss_touch(p8); pfx_quality_touch(now, p8, false);
        return xdp_action::XDP_DROP;
    }

    // 2.5) Per-CPU direct-mapped signature cache (64-way) - kills bursty replays
    if sigdm_hit_or_set(now, event.hash) { ban_flow_short(fk); ban_backoff(now, fk); ban_src24_touch(event.src_ip.to_be()); pfx_loss_touch(prog_prefix8_cached(event)); pfx_quality_touch(now, prog_prefix8_cached(event), false); return xdp_action::XDP_DROP; }

    // 2.6) BODYDM: content-sample DM cache (defeats sig-churn spam)
    if heat_cached() > 90_000 {
        if let Some(s) = unsafe { SCRATCH_PCPU.get_ptr(0) } {
            let b8 = unsafe { (*s).body8 };
            if b8 != 0 {
                let bk = body_mix64(b8, payload);
                if bodydm_hit_or_set(now, bk) {
                    ban_flow_short(fk); ban_backoff(now, fk); pfx_loss_touch(prog_prefix8_cached(event)); pfx_quality_touch(now, prog_prefix8_cached(event), false);
                    return xdp_action::XDP_DROP;
                }
            }
        }
    }

    // 3) Per-slot dup killer (adaptive Bloom + global LRU)
    if bloom_check_set(now_eff, get_cache_duration(event), event.hash, event.priority) {
        ban_flow_short(fk);
        ban_backoff(now, fk);
        ban_src24_touch(event.src_ip.to_be());
        pfx_loss_touch(prog_prefix8_cached(event));
        pfx_quality_touch(now, prog_prefix8_cached(event), false);
        return xdp_action::XDP_DROP;
    }

    // 4) Hot/cold cache keyed by slot bucket to avoid cross-slot collisions
    let key = composite_tx_key(event.hash, now_eff);
    if hot_cold_cache_check(now_eff, key, cache_ttl_ns(event)) {
        return xdp_action::XDP_DROP;
    }

    // 5) Account-pair Bloom filter (kills route-shape spam)
    if let Some(s) = unsafe { SCRATCH_PCPU.get_ptr(0) } {
        let a0 = unsafe { (*s).acc0_pref8 };
        let a1 = unsafe { (*s).acc1_pref8 };
        if a0 != 0 && a1 != 0 {
            let k = acc_pair_key(a0, a1);
            let hit = accpair_bloom_hit_or_set(now, k);
            if hit && early_pair_spam_drop() {
                ban_flow_short(fk); 
                ban_backoff(now, fk);
                pfx_loss_touch(prog_prefix8_cached(event)); 
                pfx_quality_touch(now, prog_prefix8_cached(event), false);
                return xdp_action::XDP_DROP; 
            }
        }
    }

    // 5) Flash correlation
    let mut corr: u8 = 0;
    correlate_flash_bits(&event.program_id, event.data_len, event.accounts_count, &mut corr);

    // 6) Burst tracking by borrower prefix
    let borrower_prefix8 = u64::from_le_bytes([
        event.program_id[0], event.program_id[1], event.program_id[2], event.program_id[3],
        event.program_id[4], event.program_id[5], event.program_id[6], event.program_id[7]
    ]);
    let burst = flash_burst_mark(now, borrower_prefix8, event.lamports);
    if detect_flash_loan_correlated(event, corr) ||
       (((corr & (FL_BORROW|FL_TRADE)) == (FL_BORROW|FL_TRADE)) && burst) {
        event.mev_type = 4;
        event.priority = event.priority.saturating_add(2).min(15);
    }

    // 7) v2 classification + sigmoid priority
    let is_bundle = (event.flags & 0x01) != 0;
    event.mev_type = classify_mev_type_v2(event, corr, is_bundle);
    event.priority = mev_priority_sigmoid(event);

    // Late-slot profit nudge: genuine winners get +1 near slot tail
    let s_ns = slot_ns();
    let phase = now % s_ns;
    if phase > (s_ns - (s_ns / 8)) { // last ~12.5%
        if event.expected_profit > 20_000_000_000 { // >0.02 SOL
            event.priority = event.priority.saturating_add(1).min(15);
        }
        if roi_benefit(event) > 800 { // high ROI at tail
            event.priority = event.priority.saturating_add(1).min(15);
        }
    }

    // 8) Oracle updates: slight bump, preserve routing tier stability
    if detect_oracle_update_v2(event) { event.flags |= 0x20; event.priority = event.priority.saturating_add(1).min(15); }

    // 9) EWMA rarity + hardened score + histogram
    let (ewma, _cnt) = prefix_hits_update(borrower_prefix8, now);
    let score = calculate_transaction_score_hardened(event, ewma);
    hist_add(now, score);

    // Per-prefix score EWMA → routing tier micro-boost
    let prefix8 = u64::from_le_bytes([
        event.program_id[0],event.program_id[1],event.program_id[2],event.program_id[3],
        event.program_id[4],event.program_id[5],event.program_id[6],event.program_id[7]
    ]);
    let bump = pfx_score_bump(now, prefix8, score);

    // 10) Tiering + emit + redirect
    let mut tier = adaptive_routing(score, estimate_latency_advantage(event));
    if bump > 0 && tier < 3 { tier += 1; }

    let lite = PacketEventLite {
        ts: event.timestamp, src_ip: event.src_ip, dst_ip: event.dst_ip,
        src_port: event.src_port, dst_port: event.dst_port, lamports: event.lamports,
        cu: event.compute_units, prio: event.priority, mev: event.mev_type, flags: event.flags,
        accs: event.accounts_count, insts: event.instructions_count, hash: event.hash,
        program_id_prefix: [event.program_id[0], event.program_id[1], event.program_id[2], event.program_id[3]]
    };
    emit_lite_throttled(ctx, &lite, event.priority);

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

    // VIP boost based on net-order address in header (use the raw daddr)
    let vip_boost = vip_prio_boost(unsafe { (*ip_hdr).daddr }); // BE as stored in header
    if vip_boost > 0 {
        event.priority = event.priority.saturating_add(vip_boost.min(3)).min(15);
    }

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

    macros::{classifier, map, xdp},
    maps::{HashMap, PerfEventArray, Queue, LruHashMap, PerCpuArray, XskMap, ProgramArray, Array},
    programs::{TcContext, XdpContext, ProbeContext},
    BpfContext,
};

// ===== AF_XDP queue selection config =====
#[repr(C)]
#[derive(Clone, Copy)]
struct XskCfg { 
    qmask: u32,  // Queue mask (e.g., 0x3F for 64 queues)
    strategy: u32 // bit0=CPU, bit1=HASH, bit2=HYBRID 
}

// Single-element array for config (userland sets qmask=stride_mask like 0x3F for 64 queues)
#[map]
static XSK_CFG: Array<XskCfg> = Array::with_max_entries(1, 0);

// ===== Optional CPU→queue override (NUMA-aware) =====
#[map]
static XSK_CPUQ: Array<u32> = Array::with_max_entries(256, 0);

#[inline(always)]
fn cpuq_override(cpu: u32, qmask: u32) -> Option<u32> {
    unsafe {
        if let Some(v) = XSK_CPUQ.get(cpu as u32) {
            let q = *v & qmask;
            if q != 0xFFFF_FFFF { return Some(q); }
        }
    }
    None
}

// ===== Per-qid redirect budget (anti-hotspot) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct QBudget { last_refill: u64, tokens: u16 }

#[map]
static XSK_QBUDGET: Array<QBudget> = Array::with_max_entries(64, 0);

// ===== Per-queue health EWMA (tokens proxy, ×16) =====
#[map]
static QHEALTH: Array<u16> = Array::with_max_entries(64, 0);

#[inline(always)]
fn qhealth_update(qid: u32, tokens: u16) {
    unsafe {
        if let Some(h) = QHEALTH.get_ptr_mut(qid) {
            let mut ew = *h;
            ew = ew.saturating_sub(ew >> 2)       // decay 25%
                 .saturating_add(tokens.saturating_mul(16));
            *h = ew;
        }
    }
}
#[inline(always)]
fn qhealth(qid: u32) -> u16 {
    unsafe { QHEALTH.get(qid).copied().unwrap_or(0) }
}

// ===== Per-CPU account-pair Bloom (1k bits) =====
const AP_BLOOM_BITS: usize  = 1024;
const AP_BLOOM_WORDS: usize = AP_BLOOM_BITS / 64;
#[repr(C)]
#[derive(Clone, Copy)]
struct ApBloom { words: [u64; AP_BLOOM_WORDS], epoch_ns: u64, set_ctr: u16 }

#[map]
static AP_BLOOM_PCPU: PerCpuArray<ApBloom> = PerCpuArray::with_max_entries(1, 0);

// ===== /24 → dst_port fixed-burst limiter (LRU) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct Spb { ts: u64, last: u16, cnt: u8 }

#[map]
static SPB_LRU: LruHashMap<u32, Spb> = LruHashMap::with_max_entries(32768, 0);

// ===== Per-CPU emitter sampler =====
#[repr(C)]
#[derive(Clone, Copy)]
struct EmitCtr { ctr: u32, ts: u64 }

#[map]
static EMITCTR_PCPU: PerCpuArray<EmitCtr> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn emit_should_skip(prio: u8) -> bool {
    if prio >= 12 || is_top1_score() { return false; } // keep near-ultra
    let h = heat_cached();
    if h <= 90_000 { return false; }
    let n = if h > 120_000 { 8u32 } else { 4u32 };
    unsafe {
        if let Some(ec) = EMITCTR_PCPU.get_ptr_mut(0) {
            (*ec).ctr = (*ec).ctr.wrapping_add(1);
            return ((*ec).ctr % n) != 0;
        }
    }
    false
}

#[inline(always)]
fn spb_key(src_ip_be: u32, dst_port: u16) -> u32 {
    ((src_ip_be & 0xFFFF_FF00) ^ (dst_port as u32))
}
#[inline(always)]
fn fixed_port_burst_drop(now: u64, src_ip_be: u32, dst_port: u16) -> bool {
    if heat_cached() <= 80_000 { return false; }
    let k = spb_key(src_ip_be, dst_port);
    let mut st = Spb { ts: now, last: dst_port, cnt: 0 };
    unsafe { if let Some(v) = SPB_LRU.get(&k) { st = *v; } }
    let gap = now.wrapping_sub(st.ts);
    if dst_port == st.last && gap < 200_000 {
        st.cnt = st.cnt.saturating_add(1);
        st.ts = now;
        unsafe { let _ = SPB_LRU.insert(&k, &st, 0); }
        return st.cnt >= 8; // 8 hits in 200µs window → drop
    } else {
        st.last = dst_port; st.cnt = 1; st.ts = now;
        unsafe { let _ = SPB_LRU.insert(&k, &st, 0); }
        return false;
    }
}

#[inline(always)]
fn acc_pair_key(a0p8: u64, a1p8: u64) -> u64 {
    hash_mix64(a0p8.rotate_left(17) ^ a1p8.rotate_right(13) ^ 0xA24B_C36Du64)
}
#[inline(always)]
fn ap_indices(k: u64, salt: u64) -> (usize,u64,usize,u64) {
    let h1 = hash_mix64(k ^ salt);
    let h2 = hash_mix64(k ^ 0x94D0_49BB_1331_11EBu64);
    let i1 = (h1 as usize) & (AP_BLOOM_BITS - 1);
    let i2 = (h2 as usize) & (AP_BLOOM_BITS - 1);
    (i1/64, 1u64<<(i1&63), i2/64, 1u64<<(i2&63))
}
#[inline(always)]
fn bloom_slot_salt(now: u64) -> u64 {
    (slot_bucket_ns(now) as u64) << 32
}

#[inline(always)]
fn accpair_bloom_hit_or_set(now: u64, k: u64) -> bool {
    unsafe {
        if let Some(b) = AP_BLOOM_PCPU.get_ptr_mut(0) {
            if now.wrapping_sub((*b).epoch_ns) > 250_000_000 {
                (*b).epoch_ns = now.saturating_sub(jitter_ns_small());
                (*b).words = [0u64; AP_BLOOM_WORDS]; (*b).set_ctr = 0;
            }
            let (w1,m1,w2,m2) = ap_indices(k, (flow_salt() as u64) ^ bloom_slot_salt(now));
            let hit = ((*b).words[w1] & m1) != 0 && ((*b).words[w2] & m2) != 0;
            if !hit {
                (*b).words[w1] |= m1; (*b).set_ctr = (*b).set_ctr.saturating_add(1);
                (*b).words[w2] |= m2; (*b).set_ctr = (*b).set_ctr.saturating_add(1);
            }
            hit
        } else { false }
    }
}

// ===== Slot config (runtime) =====
#[repr(C)]
#[derive(Clone, Copy)]
struct SlotCfg { slot_ns: u64 }

#[map]
static SLOT_CFG: Array<SlotCfg> = Array::with_max_entries(1, 0);

#[inline(always)]
fn slot_ns() -> u64 {
    unsafe {
        if let Some(c) = SLOT_CFG.get(0) {
            let v = (*c).slot_ns;
            if v >= 200_000_000 && v <= 1_000_000_000 { return v; } // sane bounds: 200ms..1s
        }
    }
    400_000_000
}

// ===== Per-CPU emission budget =====
#[repr(C)]
#[derive(Clone, Copy)]
struct EmitBudget { 
    last_refill: u64, 
    refill_ns: u64, 
    tokens: u32, 
    burst: u32 
}

#[map]
static EMIT_BUDGET: PerCpuArray<EmitBudget> = PerCpuArray::with_max_entries(1, 0);

// ===== Early drop configuration =====
#[repr(C)]
#[derive(Clone, Copy)]
struct DropCfg { 
    min_lamports: u64, 
    min_cu: u32, 
    max_accs: u8, 
    enable: u8 
}

#[map]
static DROP_CFG: Array<DropCfg> = Array::with_max_entries(1, 0);

// ===== Program tagging =====
const TAG_DEX:    u8 = 0x01;
const TAG_LEND:   u8 = 0x02;
const TAG_TOKEN:  u8 = 0x04;
const TAG_ORACLE: u8 = 0x08;
const TAG_NFT:    u8 = 0x10;

#[map]
static PROGRAM_TAGS: HashMap<[u8; 32], u8> = HashMap::with_max_entries(512, 0);

// ===== VIP IPv4 prefix tables =====
#[map]
static VIP_DST32: HashMap<u32, u8> = HashMap::with_max_entries(256, 0); // exact /32
#[map]
static VIP_DST24: HashMap<u32, u8> = HashMap::with_max_entries(256, 0); // masked /24
#[map]
static VIP_DST16: HashMap<u32, u8> = HashMap::with_max_entries(64, 0);  // masked /16

#[inline(always)]
fn vip_prio_boost(dst_ip_be: u32) -> u8 {
    // Net-order in headers; treat as host-order u32 value consistently
    let k32 = dst_ip_be;
    let k24 = dst_ip_be & 0xFFFF_FF00;
    let k16 = dst_ip_be & 0xFFFF_0000;
    unsafe {
        if let Some(v) = VIP_DST32.get(&k32) { return *v; }
        if let Some(v) = VIP_DST24.get(&k24) { return *v; }
        if let Some(v) = VIP_DST16.get(&k16) { return *v; }
    }
    0
}

#[inline(always)]
fn vip_dup_grace(now: u64, dst_ip_be: u32) -> u64 {
    if is_vip_dst_fast(dst_ip_be) {
        return 150_000u64; // 150µs grace for dup detection
    }
    0
}

#[inline(always)]
fn pressure_drop_udp_port(pkts: u32, port: u16) -> bool {
    if pkts > 80_000 {
        if port == SOLANA_GOSSIP_PORT || port == SOLANA_TPU_VOTE_PORT { return true; }
    }
    false
}

use aya_log_ebpf::info;
use core::{mem, mem::size_of};
use memoffset::offset_of;

mod bindings {
    pub use aya_bpf::bindings::*;
}

#[inline(always)]
#[inline(always)]
fn flow_prehash(event: &PacketEvent) -> u32 {
    // Mix src/dst ip:port and proto; emulate RSS-ish spread with per-CPU salt
    let s = flow_salt() as u64;
    let x0 = ((event.src_ip as u64) << 32) ^ (event.dst_ip as u64);
    let x1 = (((event.src_port as u64) << 48) ^ ((event.dst_port as u64) << 16)) ^ 0x11u64;
    let h = hash_mix64(x0 ^ (x1.rotate_left(13)) ^ (s << 7));
    (h ^ (h >> 32)) as u32
}

#[inline(always)]
fn select_xsk_qid(event: &PacketEvent) -> u32 {
    let cpu = unsafe { bpf_get_smp_processor_id() } as u32;
    let cfg = unsafe { XSK_CFG.get(0) };
    let (qmask, strat) = if let Some(c) = cfg {
        (unsafe{(*c)}.qmask, unsafe{(*c)}.strategy)
    } else { (0x3F, 0x04) }; // default: 64 queues & HYBRID

    let salt = flow_salt();
    let hash_q = (flow_prehash(event) ^ (event.hash as u32) ^ salt) & qmask;

    if (strat & 0x01) != 0 && (strat & 0x02) == 0 { // CPU-only
        return cpu & qmask;
    }
    if (strat & 0x02) != 0 && (strat & 0x01) == 0 { // HASH-only
        return hash_q;
    }
    // HYBRID: Ultra priority → CPU-local; else salted HASH
    if event.priority >= 14 { (cpu & qmask) } else { hash_q }
}

#[inline(always)]
fn is_solana_port(p: u16) -> bool {
    (p >= SOLANA_PORT_START && p <= SOLANA_PORT_END) ||
    p == SOLANA_GOSSIP_PORT ||
    p == SOLANA_TPU_PORT ||
    p == SOLANA_TPU_FWD_PORT ||
    p == SOLANA_TPU_VOTE_PORT
}

#[inline(always)]
#[inline(always)]
fn emit_lite_throttled(ctx: &XdpContext, lite: &PacketEventLite, prio: u8) {
    // Alpha & bundle-HOT bypass
    if prio >= 14 || (lite.flags & 0x40) != 0 || is_top1_score() {
        unsafe { HOT_HEADERS.output(ctx, lite, 0); }
        return;
    }
    
    // HOT_HEADERS heat-adaptive sampling (non-ultra, deep boil only)
    if emit_should_skip(prio) { return; }
    
    let now = unsafe { bpf_ktime_get_ns() };
    let nk = tele_key_lite(lite);
    if !tele2_should_emit(now, nk) { return; }
    
    let heat = slot_congestion(now);
    let pres = umem_pressure();
    let mut refill_ns = if heat > 80_000 { 500_000 } else if heat > 50_000 { 350_000 } else { 250_000 };
    let mut burst_cap = if heat > 80_000 { 256 } else { 512 };
    if pres > 80 { refill_ns = 600_000; burst_cap = 192; }
    else if pres > 50 { refill_ns = 450_000; burst_cap = 256; }
    let reserve   = if prio >= 12 { 32 } else { 0 }; // small hard reserve

    unsafe {
        if let Some(b) = EMIT_BUDGET.get_ptr_mut(0) {
            if (*b).refill_ns == 0 { (*b).refill_ns = refill_ns; (*b).burst = burst_cap; (*b).tokens = burst_cap; (*b).last_refill = now; }
            (*b).refill_ns = refill_ns; (*b).burst = burst_cap;
            let elapsed = now.wrapping_sub((*b).last_refill);
            if elapsed >= (*b).refill_ns {
                let mut add = (elapsed / (*b).refill_ns) as u32; if add > 2048 { add = 2048; }
                let mut t = (*b).tokens.saturating_add(add);
                if t > (*b).burst { t = (*b).burst; }
                (*b).tokens = t; (*b).last_refill = now;
            }
            // Soft reserve for prio 12–13
            if prio >= 12 && (*b).tokens <= reserve { (*b).tokens = reserve; }
            if (*b).tokens > 0 { (*b).tokens -= 1; HOT_HEADERS.output(ctx, lite, 0); }
        } else { HOT_HEADERS.output(ctx, lite, 0); }
    }
}

#[inline(always)]
fn should_deep_parse_udp(dst_port: u16, payload: &[u8]) -> bool {
    if !is_solana_port(dst_port) { return false; }
    if payload.len() < 24 { return false; }
    is_quic_like(payload)
}

#[inline(always)]
fn unlikely_cheap_drop(ev: &PacketEvent) -> bool {
    if cfg_snapshot(now_cached()).drop_enable == 0 { return false; }
    
    if lpb_underfloor_drop(ev) { return true; }
    
    if under_pfx_lpb_floor(ev) { return true; }
    
    if let Some(cfg) = unsafe { DROP_CFG.get(0) } {
        let c = unsafe { *cfg };

        // static gates
        let low_val  = ev.lamports < c.min_lamports;
        let low_cu   = ev.compute_units < c.min_cu;
        let few_acc  = ev.accounts_count <= c.max_accs;
        let simple   = ev.instructions_count <= 1;
        let vpbf     = (ev.data_len as u64).saturating_mul(5_000);
        let bad_vpb  = ev.lamports < vpbf;

        // prefix floor (EWMA)
        let prefix8 = u64::from_le_bytes([
            ev.program_id[0], ev.program_id[1], ev.program_id[2], ev.program_id[3],
            ev.program_id[4], ev.program_id[5], ev.program_id[6], ev.program_id[7]
        ]);
        let now = unsafe { bpf_ktime_get_ns() };
        let ew  = prefix_value_update(prefix8, now, ev.lamports);
        let pf  = value_floor_from_ewma(ew);
        let under_pf = ev.lamports < pf;

        // throttle only when clearly junk (and not VIP)
        if (low_val & low_cu & (few_acc | simple)) | (low_val & bad_vpb) | (under_pf & low_cu) {
            return src_token_take(now, ev.src_ip.to_be());
        }
        
        // Combined density gate (LPB ⊗ LPC, harmonic) under deep boil
        if value_density_under(ev) { return true; }
        
        false
    } else { false }
}

// ===== Program tag helpers =====
#[inline(always)]
fn program_has_tag(program_id: &[u8; 32], mask: u8) -> bool {
    unsafe {
        if let Some(v) = PROGRAM_TAGS.get(program_id) { return (*v & mask) != 0; }
    }
    false
}

// ===== Hot-signature 2-hash sketch =====
const HOT_SKETCH_SIZE: usize = 4096;
const HOT_SKETCH_MASK: usize = HOT_SKETCH_SIZE - 1;
const HOT_SKETCH_NS: u64 = 1_000_000_000; // 1s window

#[repr(C)]
#[derive(Clone, Copy)]
struct HotSketch { epoch_ns: u64, ctr: [u8; HOT_SKETCH_SIZE] }

#[map]
static HOTSIG_PCPU: PerCpuArray<HotSketch> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn hot_sig_bump(now: u64, h: u64) -> bool {
    unsafe {
        if let Some(s) = HOTSIG_PCPU.get_ptr_mut(0) {
            // rotate window
            if now.wrapping_sub((*s).epoch_ns) > HOT_SKETCH_NS {
                (*s).epoch_ns = now;
                // zero counters cheaply
                let mut i = 0usize;
                while i < HOT_SKETCH_SIZE { (*s).ctr[i] = 0; i += 1; }
            }
            let i1 = (hash_mix64(h) as usize) & HOT_SKETCH_MASK;
            let i2 = (hash_mix64(h ^ 0x517c_c1b1_c2a3_e4f5) as usize) & HOT_SKETCH_MASK;
            let c1 = (*s).ctr[i1];
            let c2 = (*s).ctr[i2];
            // bump with saturation
            (*s).ctr[i1] = c1.saturating_add(1);
            (*s).ctr[i2] = c2.saturating_add(1);
            // hot if seen at least twice via both hashes (very low FP)
            (c1 >= 1) & (c2 >= 1)
        } else { false }
    }
}

// ===== Adaptive Bloom (per-CPU rotating) + global LRU =====
const BLOOM_BITS: usize = 4096; // 4k bits per CPU
const BLOOM_WORDS: usize = BLOOM_BITS / 64;

#[repr(C)]
#[derive(Clone, Copy)]
struct Bloom { words: [u64; BLOOM_WORDS], epoch_ns: u64, set_ctr: u16 }

#[map]
static BLOOM_PCPU: PerCpuArray<Bloom> = PerCpuArray::with_max_entries(1, 0);

#[inline(always)]
fn hash_mix64(mut x: u64) -> u64 {
    // SplitMix64 with per-CPU salt blended in, branchless
    x ^= (flow_salt() as u64) << 17;
    x = x.wrapping_add(0x9E3779B97F4A7C15u64);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58476D1CE4E5B9u64);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D049BB133111EBu64);
    x ^ (x >> 31)
}

#[inline(always)]
fn bloom_indices(h: u64) -> (usize, u64, usize, u64) {
    // Per-CPU salt makes bit placements non-deterministic across restarts/cores
    let salt = (flow_salt() as u64) ^ bloom_slot_salt(now_cached());
    let h1 = hash_mix64(h ^ (salt.wrapping_mul(0x9E3779B185EBCA87)));
    let h2 = hash_mix64(h ^ (salt ^ 0x517c_c1b1_c2a3_e4f5));
    let i1 = (h1 as usize) & (BLOOM_BITS - 1);
    let i2 = (h2 as usize) & (BLOOM_BITS - 1);
    (i1 / 64, 1u64 << (i1 & 63), i2 / 64, 1u64 << (i2 & 63))
}

#[inline(always)]
fn bloom_check_set(now: u64, ttl_ns: u64, sig_hash: u64, prio: u8) -> bool {
    unsafe {
        if let Some(b) = BLOOM_PCPU.get_ptr_mut(0) {
            // load-aware rotate (uses SCORE_HIST_PCPU)
            let mut rotate_ns = 100_000_000u64; // 100ms
            let mut rotate_fill = (BLOOM_BITS as u32 / 2) as u16;
            if let Some(h) = SCORE_HIST_PCPU.get_ptr(0) {
                let total = (*h).total;
                if total > 20_000 { rotate_ns = 50_000_000; rotate_fill = (BLOOM_BITS as u32 / 3) as u16; }
                else if total > 5_000 { rotate_ns = 75_000_000; rotate_fill = (BLOOM_BITS as u32 / 2) as u16; }
            }
            if now.wrapping_sub((*b).epoch_ns) > rotate_ns || (*b).set_ctr > rotate_fill {
                (*b).words = [0u64; BLOOM_WORDS]; (*b).epoch_ns = now.saturating_sub(jitter_ns_small()); (*b).set_ctr = 0;
            }

            let heat = if let Some(hh) = SCORE_HIST_PCPU.get_ptr(0) { (*hh).total } else { 0 };
            let occ = (*b).set_ctr as u32;
            let hot = heat > 80_000;
            let dense = occ > ((BLOOM_BITS as u32) * 3 / 2); // ~≥75% of bits set (2 bits per insert)
            let bloom_hit = if hot && dense {
                let (w1,m1,w2,m2,w3,m3,w4,m4) = bloom_indices4(sig_hash, (flow_salt() as u64) ^ bloom_slot_salt(now));
                let hitc = (((*b).words[w1] & m1) != 0) as u8
                         + (((*b).words[w2] & m2) != 0) as u8
                         + (((*b).words[w3] & m3) != 0) as u8
                         + (((*b).words[w4] & m4) != 0) as u8;
                if ((*b).words[w1] & m1) == 0 { (*b).words[w1] |= m1; (*b).set_ctr = (*b).set_ctr.saturating_add(1); }
                if ((*b).words[w2] & m2) == 0 { (*b).words[w2] |= m2; (*b).set_ctr = (*b).set_ctr.saturating_add(1); }
                if ((*b).words[w3] & m3) == 0 { (*b).words[w3] |= m3; (*b).set_ctr = (*b).set_ctr.saturating_add(1); }
                if ((*b).words[w4] & m4) == 0 { (*b).words[w4] |= m4; (*b).set_ctr = (*b).set_ctr.saturating_add(1); }
                hitc >= 3
            } else if heat > 80_000 {
                let (w1,m1,w2,m2,w3,m3) = bloom_indices3(sig_hash);
                let hitc = (((*b).words[w1] & m1) != 0) as u8
                         + (((*b).words[w2] & m2) != 0) as u8
                         + (((*b).words[w3] & m3) != 0) as u8;
                if ((*b).words[w1] & m1) == 0 { (*b).words[w1] |= m1; (*b).set_ctr = (*b).set_ctr.saturating_add(1); }
                if ((*b).words[w2] & m2) == 0 { (*b).words[w2] |= m2; (*b).set_ctr = (*b).set_ctr.saturating_add(1); }
                if ((*b).words[w3] & m3) == 0 { (*b).words[w3] |= m3; (*b).set_ctr = (*b).set_ctr.saturating_add(1); }
                hitc >= 2
            } else {
                let (w1, m1, w2, m2) = bloom_indices(sig_hash);
                let hit1 = ((*b).words[w1] & m1) != 0;
                let hit2 = ((*b).words[w2] & m2) != 0;
                if !hit1 { (*b).words[w1] |= m1; (*b).set_ctr = (*b).set_ctr.saturating_add(1); }
                if !hit2 { (*b).words[w2] |= m2; (*b).set_ctr = (*b).set_ctr.saturating_add(1); }
                hit1 & hit2
            };

            // Slow-window Bloom to cheaply catch older repeats (pre-LRU)
            let slow_hit = sigslow_hit_or_set(now, sig_hash);
            let bloom_hit = bloom_hit | slow_hit;

            // Skip LRU confirm under exhausted per-packet budget; rely on Bloom windows
            if !map_budget_take_alpha(2, prio) {
                return bloom_hit;
            }

            // LRU confirm with 32-bit guard
            let mut drop_dupe = false;
            if let Some(v) = TRANSACTION_CACHE.get(&sig_hash) {
                if now.wrapping_sub((*v).ts) < ttl_ns {
                    if let Some(gc) = SIGCHK_LRU.get(&sig_hash) {
                        if let Some(s) = SCRATCH_PCPU.get_ptr(0) {
                            let guard_now = unsafe { (*s).sig_guard32 };
                            if guard_now == *gc { drop_dupe = true; }
                        } else { drop_dupe = true; }
                    } else { drop_dupe = true; }
                }
            }
            if !drop_dupe {
                let v = CacheVal { ts: now };
                let _ = TRANSACTION_CACHE.insert(&sig_hash, &v, 0);
                if let Some(s) = SCRATCH_PCPU.get_ptr(0) {
                    let g = unsafe { (*s).sig_guard32 };
                    let _ = SIGCHK_LRU.insert(&sig_hash, &g, 0);
                }
            }
            drop_dupe | bloom_hit
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
#[repr(C, align(64))]
#[derive(Clone, Copy)]
pub struct Scratch {
    pub dst_port: u16,
    pub src_port: u16,
    pub payload_off: u16,
    pub payload_len: u16,
    pub flags: u8, // bit0: quic-like
    pub _pad0: [u8; 3], // padding to align sig_guard32
    pub sig_guard32: u32, // 32-bit signature guard for collision protection
    pub _pad_sg: u32, // keep alignment
    pub first8_data: u64, // first 8 bytes of data for instruction fingerprinting
    pub accidx16: u16, // first-two account indices fingerprint seed
    pub _pad_acc: u16, // keep alignment
    pub bh8: u64, // first 8 bytes of recent blockhash for horizon gating
    pub acc0_pref8: u64, // first account prefix8 for hotness tracking
    pub acc1_pref8: u64, // second account prefix8 for pair Bloom
    pub prog_pref8: u64, // program prefix8 cache
    pub vlan_pcp: u8, // VLAN PCP (0..7)
    pub cu_eff: u32,
    pub _pad_cu: u32,
    pub body8: u64, // first 8B of instruction region for body-sample cache
    pub flowk: u64, // cached flow key to avoid recomputation
    pub ip_dscp: u8, _pad_dscp: [u8;7],
    pub ip_ecn: u8, _pad_ecn: [u8;7],
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

#[inline(always)]
fn try_short_header_dcid4(payload: &[u8]) -> Option<u32> {
    // Short header: b0[6]==1, DCID MAY be present but length implicit/negotiated.
    // We use a conservative heuristic: if len>=7, grab next 4 bytes after b0 as pseudo-DCID.
    if payload.len() < 7 { return None; }
    let b0 = payload[0];
    if (b0 & 0x40) != 0x40 { return None; } // not short header
    // Take bytes [1..=4] as a stable pseudo-DCID; bounded and verifier-friendly
    let d0 = payload[1] as u32;
    let d1 = (payload[2] as u32) << 8;
    let d2 = (payload[3] as u32) << 16;
    let d3 = (payload[4] as u32) << 24;
    Some(d0 | d1 | d2 | d3)
}

#[inline(always)]
fn mark_bundle_flow(now: u64, payload: &[u8]) -> bool {
    if payload.len() < 12 { return false; }
    let b0 = payload[0];
    
    // If not long header, attempt safe short-header pseudo-DCID
    if (b0 & 0xC0) != 0xC0 {
        if !quic_pn_len_ok(b0) { return false; }
        if let Some(k4) = try_short_header_dcid4(payload) {
            let mut ttl = 2_000_000_000u64; // 2s base
            unsafe {
                if let Some(h) = SCORE_HIST_PCPU.get_ptr(0) {
                    let total = (*h).total;
                    if total > 20_000 { ttl = 3_000_000_000; }
                    else if total > 5_000 { ttl = 2_500_000_000; }
                }

                if let Some(v) = BUNDLE_FLOWS.get(&(k4 as u64)) {
                    if now.wrapping_sub((*v).ts) < ttl { return true; }
                }
                let val = CacheVal { ts: now };
                let _ = BUNDLE_FLOWS.insert(&(k4 as u64), &val, 0);

                // also set a per-CPU queue hint for this packet
                if let Some(cfg) = XSK_CFG.get(0) {
                    let qmask = unsafe { (*cfg).qmask };
                    let salt  = flow_salt();
                    let qid   = (((k4 as u32) ^ salt) & qmask) as u32;
                    if let Some(h) = BUNDLE_HINT_PCPU.get_ptr_mut(0) {
                        (*h).qid = qid;
                        (*h).ts  = now;
                        (*h).key = k4 as u64; // stash DCID key for downstream smoothing
                    }
                }

                // record heavy-hitter status
                let _ = dcid_hot_mark(now, k4 as u64);
            }
        }
        return false;
    }

    let dcid_len = payload[5] as usize;
    if dcid_len == 0 || dcid_len > 20 { return false; }
    let off = 6usize;
    if off + dcid_len > payload.len() { return false; }

    // First up to 8 bytes of DCID as key
    let mut key_bytes = [0u8; 8];
    let copy_len = if dcid_len >= 8 { 8 } else { dcid_len };
    let src = &payload[off..off+copy_len];
    unsafe { core::ptr::copy_nonoverlapping(src.as_ptr(), key_bytes.as_mut_ptr(), copy_len); }
    let key = u64::from_le_bytes(key_bytes);

    // TTL scales with histogram heat
    let mut ttl = 2_000_000_000u64; // 2s base
    unsafe {
        if let Some(h) = SCORE_HIST_PCPU.get_ptr(0) {
            let total = (*h).total;
            if total > 20_000 { ttl = 3_000_000_000; }
            else if total > 5_000 { ttl = 2_500_000_000; }
        }

        if let Some(v) = BUNDLE_FLOWS.get(&key) {
            if now.wrapping_sub((*v).ts) < ttl { return true; }
        }
        let val = CacheVal { ts: now };
        let _ = BUNDLE_FLOWS.insert(&key, &val, 0);

        // also set a per-CPU queue hint for this packet
        if let Some(cfg) = XSK_CFG.get(0) {
            let qmask = unsafe { (*cfg).qmask };
            let salt  = flow_salt();
            let qid   = (((key as u32) ^ salt) & qmask) as u32;
            if let Some(h) = BUNDLE_HINT_PCPU.get_ptr_mut(0) {
                (*h).qid = qid;
                (*h).ts  = now;
                (*h).key = key; // stash DCID key for downstream smoothing
            }
        }

        // record heavy-hitter status
        let _ = dcid_hot_mark(now, key);
    }
    false
}

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
#[inline(always)]
fn is_quic_initial_min(payload: &[u8]) -> bool {
    if payload.len() < 1200 { return false; }
    let b0 = payload[0];
    // Long header + Initial (0b1100_0000 with type=0)
    (b0 & 0xC0) == 0xC0 && ((b0 >> 2) & 0x03) == 0
}

#[inline(always)]
fn is_quic_like(payload: &[u8]) -> bool {
    if payload.len() < 7 { return false; }
    let b0 = payload[0];
    if (b0 & 0xC0) == 0xC0 {
        if payload.len() < 12 { return false; } // min long header prelude
        let ver = ((payload[1] as u32) << 24) | ((payload[2] as u32) << 16) | ((payload[3] as u32) << 8) | (payload[4] as u32);
        if ver == 0 { return false; }
        let dcid_len = payload[5] as usize;
        if dcid_len == 0 || dcid_len > 20 { return false; }
        let off_scid_len = 6 + dcid_len;
        if off_scid_len >= payload.len() { return false; }
        let scid_len = payload[off_scid_len] as usize;
        let off_after_ids = off_scid_len + 1 + scid_len;
        if off_after_ids >= payload.len() { return false; }
        let pn_len = ((b0 & 0x03) as usize) + 1;
        // Require at least 1 byte payload after PN to be considered valid-ish
        let valid = (off_after_ids + pn_len + 1) <= payload.len();
        
        // If it looks like Initial, enforce 1200B minimum
        if ((b0 >> 2) & 0x03) == 0 {
            if payload.len() < 1200 { return false; }
        }
        valid
    } else {
        (b0 & 0x40) == 0x40
    }
}


#[inline(always)]
fn extract_transaction_info(payload: &[u8]) -> Result<PacketEvent, ()> {
    if payload.len() < 176 { return Err(()); }

    let mut event = unsafe { mem::zeroed::<PacketEvent>() };
    event.compute_units = 200_000;

    let mut off: usize = 0;

    // signatures count
    let sig_count = parse_compact_u16_bounded(payload, &mut off)?; // 1..=8
    if sig_count == 0 || sig_count > 8 { return Err(()); }

    // copy first signature (64) in two fixed chunks of 32 (bounds-checked)
    copy_32(payload, off, unsafe { &mut *(&mut event.signature[0] as *mut u8 as *mut [u8;32]) })?;
    copy_32(payload, off + 32, unsafe { &mut *(&mut event.signature[32] as *mut u8 as *mut [u8;32]) })?;
    event.hash = calculate_transaction_hash(&event.signature);

    // Compute and stash a 32-bit guard for dupe confirmation
    if let Some(s) = unsafe { SCRATCH_PCPU.get_ptr_mut(0) } {
        let g = sigcheck32(&event.signature);
        unsafe { (*s).sig_guard32 = g; }
    }

    // advance by sig_count * 64 safely
    let sig_bytes = (sig_count as usize).saturating_mul(SIGNATURE_LEN);
    let _ = load_bytes(payload, off, sig_bytes)?; // guard
    off += sig_bytes;

    // message header: 2 bytes
    let header = *load_bytes(payload, off, 1)?.get(0).ok_or(())?; off += 1;
    let num_readonly_unsigned = *load_bytes(payload, off, 1)?.get(0).ok_or(())?; off += 1;
    let num_required_signatures = header & 0x0f;
    let num_readonly_signed = (header >> 4) & 0x0f;

    // account keys
    let account_keys_len = parse_compact_u16_bounded(payload, &mut off)?; // 1..=256
    if account_keys_len == 0 || account_keys_len > MAX_ACCOUNTS as u16 { return Err(()); }
    
    // Solana header plausibility fence - early drop on malformed/abusive envelopes
    if !solana_header_plausible(num_required_signatures, num_readonly_signed, num_readonly_unsigned, account_keys_len) {
        return Err(());
    }
    event.accounts_count = account_keys_len.min(255) as u8;
    let account_keys_start = off;
    let total_keys = (account_keys_len as usize).saturating_mul(PUBKEY_LEN);

    // ensure keys + blockhash present
    let _ = load_bytes(payload, off, total_keys + BLOCKHASH_LEN)?;
    
    // Capture first 8 bytes of recent blockhash for horizon gating
    let bh_off = account_keys_start + total_keys;
    if let Ok(bh) = load_bytes(payload, bh_off, 8) {
        if let Some(s) = unsafe { SCRATCH_PCPU.get_ptr_mut(0) } {
            let bh8 = u64::from_le_bytes([bh[0],bh[1],bh[2],bh[3],bh[4],bh[5],bh[6],bh[7]]);
            unsafe { (*s).bh8 = bh8; }
        }
    }
    
    off += total_keys + BLOCKHASH_LEN;

    // instructions length
    let instructions_len = parse_compact_u16_bounded(payload, &mut off)?; // 0..=64 (bounded)
    if instructions_len == 0 { return Err(()); }
    
    if insane_instcount_drop(instructions_len as usize) { return Err(()); }
    
    event.instructions_count = instructions_len.min(255) as u8;

    // Capture first 8B of instruction region as body sample (if present)
    if instructions_len > 0 {
        if let Ok(sb) = load_bytes(payload, off, 8) {
            if let Some(s) = unsafe { SCRATCH_PCPU.get_ptr_mut(0) } {
                unsafe { (*s).body8 = u64::from_le_bytes([sb[0],sb[1],sb[2],sb[3],sb[4],sb[5],sb[6],sb[7]]); }
            }
        }
    }

    // Conservatively parse first instruction's program-id for coarse tagging
    if instructions_len > 0 {
        let program_id_index = *load_bytes(payload, off, 1)?.get(0).ok_or(())? as usize; off += 1;
        if program_id_index < account_keys_len as usize {
            let key_start = account_keys_start + program_id_index * PUBKEY_LEN;
            copy_32(payload, key_start, &mut event.program_id)?;
        }
        // accounts len + skip indices
        let accounts_len = parse_compact_u16_bounded(payload, &mut off)? as usize;

        // Stash first two account indices from this instruction (if present)
        let mut idx16: u16 = 0;
        if accounts_len >= 1 {
            let b0 = *load_bytes(payload, off, 1)?.get(0).ok_or(())? as u16;
            idx16 |= b0;
        }
        if accounts_len >= 2 {
            let b1 = *load_bytes(payload, off + 1, 1)?.get(0).ok_or(())? as u16;
            idx16 |= b1 << 8;
        }
        if let Some(s) = unsafe { SCRATCH_PCPU.get_ptr_mut(0) } {
            unsafe { (*s).accidx16 = idx16; }
        }

        // first account index (if present) and its pubkey prefix8 (for hotness)
        let mut idx0: usize = usize::MAX;
        if accounts_len >= 1 {
            idx0 = *load_bytes(payload, off, 1)?.get(0).ok_or(())? as usize;
        }
        // stash account prefix8 (safe if idx0 valid)
        if idx0 != usize::MAX && idx0 < account_keys_len as usize {
            let key_start = account_keys_start + idx0 * PUBKEY_LEN;
            if let Ok(kb) = load_bytes(payload, key_start, 8) {
                if let Some(s) = unsafe { SCRATCH_PCPU.get_ptr_mut(0) } {
                    unsafe {
                        (*s).acc0_pref8 = u64::from_le_bytes([kb[0],kb[1],kb[2],kb[3],kb[4],kb[5],kb[6],kb[7]]);
                    }
                }
            }
        }

        // second account index and prefix8
        if accounts_len >= 2 {
            let idx1 = *load_bytes(payload, off + 1, 1)?.get(0).ok_or(())? as usize;
            if idx1 < account_keys_len as usize {
                let key_start1 = account_keys_start + idx1 * PUBKEY_LEN;
                if let Ok(kb1) = load_bytes(payload, key_start1, 8) {
                    if let Some(s) = unsafe { SCRATCH_PCPU.get_ptr_mut(0) } {
                        unsafe {
                            (*s).acc1_pref8 = u64::from_le_bytes([kb1[0],kb1[1],kb1[2],kb1[3],kb1[4],kb1[5],kb1[6],kb1[7]]);
                        }
                    }
                }
            }
        }

        let _ = load_bytes(payload, off, accounts_len)?; off += accounts_len;
        // data length + bounded lamports sample from first 8 bytes
        let data_len = parse_compact_u16_bounded(payload, &mut off)?;
        event.data_len = data_len.min(16384); // clamp once; protects later multiplications
        if data_len >= 8 {
            if let Ok(b) = load_bytes(payload, off, 8) {
                event.lamports = u64::from_le_bytes([b[0],b[1],b[2],b[3],b[4],b[5],b[6],b[7]]);
                if let Some(s) = unsafe { SCRATCH_PCPU.get_ptr_mut(0) } {
                    let first8 = event.lamports; // we already loaded 8 bytes; reuse
                    unsafe { (*s).first8_data = first8; }
                }
            }
        }
    }

    // **Accurate ComputeBudget detection** within instruction region, bounded to +512 bytes
    let instr_start = account_keys_start + total_keys + BLOCKHASH_LEN;
    event.compute_units = extract_compute_budget(payload, instr_start)
        .max(event.compute_units)        // never reduce below default
        .min(1_400_000);                 // protocol cap

    // Base classification hints
    if is_dex_program(&event.program_id) {
        event.packet_type = 2; event.priority = 10;
    } else if is_high_value_program(&event.program_id) {
        event.packet_type = 1; event.priority = 8;
    } else {
        event.packet_type = 0; event.priority = 5;
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
    if payload.len() < 12 { return false; }
    let b0 = payload[0];
    let quic_long = (b0 & 0xC0) == 0xC0;
    let quic_short = (b0 & 0x40) == 0x40 && (b0 & 0x80) == 0x00;
    if quic_long {
        if !quic_long_dcid_min_ok(payload) { return false; }
        if quic_drop_retry_or_0rtt(payload) { return false; }
        let ver = u32::from_be_bytes([payload[1], payload[2], payload[3], payload[4]]);
        let dcid_len = payload[5];
        return ver != 0 && dcid_len > 0;
    }
    if quic_short {
        if !quic_short_has_sample(payload) { return false; }
        if !quic_short_min_len_ok(payload) { return false; }
        return true;
    }
    false
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

    // Under pressure, do not waste TC cycles on gossip/vote egress
    let now = unsafe { bpf_ktime_get_ns() };
    let pkts = slot_congestion(now);
    if pressure_drop_udp_port(pkts, dst_port) { return Ok(TC_ACT_OK); } // silently pass OS stack, but we bail early

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
    let base = match event.mev_type {
        3 => 5_000_000_000, 2 => 3_000_000_000, 4 => 10_000_000_000, 1 => 2_000_000_000, _ => 1_000_000_000,
    };
    let mut ttl = base;

    if (event.flags & 0x01) != 0 { ttl = ttl.saturating_add(base / 2); }
    if event.priority >= 14 { ttl = ttl.saturating_add(base / 2); }
    if program_has_tag(&event.program_id, TAG_ORACLE) || detect_oracle_update_v2(event) { ttl /= 2; }

    let now  = now_cached();
    let s_ns = slot_ns();
    let phase = now % s_ns;
    if phase < (s_ns / 16) { ttl = ttl.saturating_add(base / 2); }
    if phase > (s_ns - (s_ns / 10)) { ttl = ttl.saturating_sub(ttl / 5).saturating_sub(ttl / 5).max(250_000_000); }

    // Horizon shaping
    let mut bh_state: u8 = 0;
    if let Some(s) = unsafe { SCRATCH_PCPU.get_ptr(0) } {
        let bh8 = unsafe { (*s).bh8 };
        if bh8 != 0 { bh_state = bh_classify(now, bh8); } // 1=cur,2=prev,3=old
    }
    if bh_state == 2 { ttl = ttl.saturating_sub(ttl / 3); }   // prev: ~33% shorter
    if bh_state == 1 { ttl = ttl.saturating_add(base / 6); }  // cur: +~16%
    ttl
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
#[no_mangle]
#[link_section = "maps"]
pub static _maps: [u8; 0] = [];
#[link_section = "maps"]
pub static _maps: [u8; 0] = [];
#[no_mangle]
#[link_section = "maps"]
pub static _maps: [u8; 0] = [];
