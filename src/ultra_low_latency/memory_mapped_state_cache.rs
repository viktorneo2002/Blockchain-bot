// [PATCH 50] 64B-aligned atomics wrapper (to be wired where struct is defined)
#[repr(align(64))]
struct Al64<T>(T);
impl<T> core::ops::Deref for Al64<T> { type Target = T; #[inline] fn deref(&self) -> &T { &self.0 } }
#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn eq_header_impl_avx512_vbmi(a: *const u8, b: *const u8, len: usize) -> bool {
    use core::arch::x86_64::{__m512i,_mm512_cmpeq_epi8_mask,_mm512_loadu_si512,_mm512_permutex2var_epi8};
    if len == 0 { return true; }
    #[inline(always)]
    unsafe fn idx_for_shift(sh: usize) -> __m512i {
        let mut idx = [0u8; 64];
        let mut i = 0usize;
        while i < 64 {
            let off = sh + i;
            if off < 64 { idx[i] = off as u8; } else { idx[i] = 0x80 | ((off - 64) as u8); }
            i += 1;
        }
        core::mem::transmute::<[u8;64], __m512i>(idx)
    }
    let mut i = 0usize;
    while i + 64 <= len {
        let pa = a.add(i); let pb = b.add(i);
        let offa = (pa as usize) & 63; let offb = (pb as usize) & 63;
        let abase = pa.sub(offa); let bbase = pb.sub(offb);
        let a0: __m512i = _mm512_loadu_si512(abase as *const __m512i);
        let a1: __m512i = _mm512_loadu_si512(abase.add(64) as *const __m512i);
        let b0: __m512i = _mm512_loadu_si512(bbase as *const __m512i);
        let b1: __m512i = _mm512_loadu_si512(bbase.add(64) as *const __m512i);
        let ia = idx_for_shift(offa); let ib = idx_for_shift(offb);
        let va: __m512i = _mm512_permutex2var_epi8(a0, ia, a1);
        let vb: __m512i = _mm512_permutex2var_epi8(b0, ib, b1);
        if _mm512_cmpeq_epi8_mask(va, vb) != !0u64 { return false; }
        i += 64;
    }
    eq_header_impl_avx512(a.add(i), b.add(i), len - i)
}

// --------------------- [PATCH 122A: warm_touch_pages()] ---------------------
#[inline(always)]
fn warm_touch_pages(ptr: *mut u8, len: usize) {
    if std::env::var("SC_WARM_PAGES").ok().as_deref() != Some("1") { return; }
    unsafe {
        let ps: usize = {
            #[cfg(target_os = "linux")]
            { libc::sysconf(libc::_SC_PAGESIZE) as usize }
            #[cfg(not(target_os = "linux"))]
            { 4096usize }
        };
        let mut off = 0usize;
        while off < len {
            core::ptr::read_volatile(ptr.add(off));
            off = off.wrapping_add(ps);
        }
        // [PATCH 130A moved below to module scope]
        if off != len && len > 0 { core::ptr::read_volatile(ptr.add(len - 1)); }
    }
}
// ---------------------------------------------------------------------------

// --------------- [PATCH 145A: NEW pext_mask_bank() + picker] ----------------
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn pext_mask_bank() -> &'static [(u64,u64,u64); 16] {
    static BANK: [(u64,u64,u64);16] = [
        (0x8040_2010_0804_0201, 0x4020_1008_0402_0180, 0x1111_2222_4444_8888),
        (0x8080_4040_2020_1010, 0x0101_0202_0404_0808, 0x8421_0842_1084_2108),
        (0xA0A0_5050_2828_1414, 0x0505_0A0A_1414_2828, 0x4924_9249_2492_4924),
        (0xC0A0_9050_4824_1209, 0x3090_0C24_8102_4401, 0x8421_4210_0842_1084),
        (0x8040_2010_0804_0210, 0x4020_1008_0402_0810, 0x2211_8844_2266_1188),
        (0x8888_4444_2222_1111, 0x1248_9024_4812_0906, 0x8040_2010_9048_2412),
        (0xAA55_AA55_00FF_00FF, 0x55AA_55AA_FF00_FF00, 0xF0F0_0F0F_C3C3_3C3C),
        (0x9696_5A5A_3C3C_C3C3, 0x6936_9C63_369C_C936, 0x8421_7EBD_1084_2108),
        (0x8040_A010_4804_8201, 0x2040_9010_0802_0480, 0x0248_9124_4892_2449),
        (0x0124_89A5_0248_91A2, 0x2491_A224_9122_4491, 0x4444_1111_AAAA_5555),
        (0xDEAD_BEEF_CAFE_BABE, 0x1357_9BDF_0246_8ACE, 0xC3C3_3C3C_5A5A_A5A5),
        (0x8040_2010_8844_2211, 0x1100_8844_2211_0044, 0x4210_8421_0842_1084),
        (0x88AA_44CC_22EE_11FF, 0x10F0_08F8_04FC_02FE, 0xA5A5_5A5A_3C3C_C3C3),
        (0x8421_0842_1084_2108, 0x4924_9249_2492_4924, 0x8040_2010_0804_0201),
        (0xAA00_5500_FF00_00FF, 0x00AA_0055_00FF_FF00, 0x0F0F_F0F0_3333_CCCC),
        (0xC0C0_A0A0_9090_4848, 0x0C0C_3030_1212_4040, 0x1248_2481_2448_9124),
    ];
    &BANK
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn pick_mask_triplet(seed: u64) -> (u64,u64,u64) {
    let bank = pext_mask_bank();
    let idx = ((seed ^ seed.rotate_left(29) ^ 0x9E37_79B9_7F4A_7C15) as usize) & 15;
    bank[idx]
}
// ---------------------------------------------------------------------------

// -------------------- [PATCH 142A: NEW salt_per_socket()] -------------------
#[inline(always)]
fn salt_per_socket(base: u64) -> u64 {
    let slice_us: u64 = std::env::var("SC_SEED_SLICE_US").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(1_000_000);
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::_rdtscp;
        let mut aux: u32 = 0;
        let _ = _rdtscp(&mut aux);
        let cpu = (aux & 0x0FFF) as u64;
        let node = ((aux as u64) >> 12) & 0xFF;
        let now = mono_micros_fast() as u64;
        let slice = (now / slice_us).rotate_left(13);
        return base ^ cpu ^ (node << 24) ^ slice.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
    let now = mono_micros_fast() as u64;
    base ^ (now / slice_us).rotate_left(17)
}
// ---------------------------------------------------------------------------

// -------------------------- [PATCH 143A: tpause_*] --------------------------
#[inline(always)]
fn tpause_cycles(cycles: u32) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        if !has_waitpkg() { return; }
        core::arch::asm!(
            "xor edx, edx", // C0
            "mov eax, {0:e}",
            "tpause",
            in(reg) cycles,
            options(nostack, preserves_flags)
        );
    }
}
// ---------------------------------------------------------------------------

// -------------------- [PATCH 141A: eq impls and types] ----------------------
type EqHdrFn = unsafe fn(*const u8, *const u8, usize) -> bool;
type EqBigFn = unsafe fn(*const u8, *const u8, usize) -> bool;
static EQ_HDR: OnceLock<EqHdrFn> = OnceLock::new();
static EQ_BIG: OnceLock<EqBigFn> = OnceLock::new();

#[inline(always)]
unsafe fn eq_header_impl_portable(a: *const u8, b: *const u8, len: usize) -> bool {
    let mut i = 0usize; while i < len { if *a.add(i) != *b.add(i) { return false; } i += 1; } true
}
#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn eq_header_impl_avx2(a: *const u8, b: *const u8, len: usize) -> bool {
    use core::arch::x86_64::{__m256i,_mm256_cmpeq_epi8,_mm256_loadu_si256,_mm256_movemask_epi8};
    let mut i = 0usize;
    while i + 32 <= len {
        let va = _mm256_loadu_si256(a.add(i) as *const __m256i);
        let vb = _mm256_loadu_si256(b.add(i) as *const __m256i);
        if _mm256_movemask_epi8(_mm256_cmpeq_epi8(va, vb)) != -1 { return false; }
        i += 32;
    }
    eq_header_impl_portable(a.add(i), b.add(i), len - i)
}
#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn eq_header_impl_avx512(a: *const u8, b: *const u8, len: usize) -> bool {
    use core::arch::x86_64::{__m512i,_mm512_cmpeq_epi8_mask,_mm512_loadu_si512,_mm512_maskz_loadu_epi8};
    if len == 0 { return true; }
    let mut i = 0usize;
    while i + 64 <= len {
        let va: __m512i = _mm512_loadu_si512(a.add(i) as *const __m512i);
        let vb: __m512i = _mm512_loadu_si512(b.add(i) as *const __m512i);
        if _mm512_cmpeq_epi8_mask(va, vb) != !0u64 { return false; }
        i += 64;
    }
    let tail = len - i; if tail == 0 { return true; }
    let mask = (!0u64) >> (64 - tail as u64);
    let va = _mm512_maskz_loadu_epi8(mask, a.add(i) as *const _);
    let vb = _mm512_maskz_loadu_epi8(mask, b.add(i) as *const _);
    _mm512_cmpeq_epi8_mask(va, vb) == !0u64
}
#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn eq_header_impl_neon(a: *const u8, b: *const u8, len: usize) -> bool {
    use core::arch::aarch64::{uint8x16_t,vceqq_u8,vld1q_u8,vminvq_u8};
    let mut i = 0usize;
    while i + 16 <= len {
        let va: uint8x16_t = vld1q_u8(a.add(i));
        let vb: uint8x16_t = vld1q_u8(b.add(i));
        if vminvq_u8(vceqq_u8(va, vb)) != 255 { return false; }
        i += 16;
    }
    eq_header_impl_portable(a.add(i), b.add(i), len - i)
}

#[inline(always)]
unsafe fn eq_big_impl_portable(a: *const u8, b: *const u8, len: usize) -> bool { eq_header_impl_portable(a,b,len) }
#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn eq_big_impl_avx2(a: *const u8, b: *const u8, len: usize) -> bool { eq_header_impl_avx2(a,b,len) }
#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn eq_big_impl_avx512(a: *const u8, b: *const u8, len: usize) -> bool { eq_header_impl_avx512(a,b,len) }
#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn eq_big_impl_neon(a: *const u8, b: *const u8, len: usize) -> bool { eq_header_impl_neon(a,b,len) }

#[inline(always)]
fn select_eq_header_impl() -> EqHdrFn {
    #[cfg(target_arch = "x86_64")]
    {
        if core::is_x86_feature_detected!("avx512vbmi") && core::is_x86_feature_detected!("avx512bw") && core::is_x86_feature_detected!("avx512f") {
            return |a,b,l| unsafe { eq_header_impl_avx512_vbmi(a,b,l) };
        }
        if core::is_x86_feature_detected!("avx512f") && core::is_x86_feature_detected!("avx512bw") { return |a,b,l| unsafe { eq_header_impl_avx512(a,b,l) }; }
        if core::is_x86_feature_detected!("avx2") { return |a,b,l| unsafe { eq_header_impl_avx2(a,b,l) }; }
    }
    #[cfg(target_arch = "aarch64")] { return |a,b,l| unsafe { eq_header_impl_neon(a,b,l) }; }
    |_a,_b,_l| unsafe { eq_header_impl_portable(_a,_b,_l) }
}
#[inline(always)]
fn select_eq_big_impl() -> EqBigFn {
    #[cfg(target_arch = "x86_64")]
    {
        if core::is_x86_feature_detected!("avx512f") && core::is_x86_feature_detected!("avx512bw") { return |a,b,l| unsafe { eq_big_impl_avx512(a,b,l) }; }
        if core::is_x86_feature_detected!("avx2") { return |a,b,l| unsafe { eq_big_impl_avx2(a,b,l) }; }
    }
    #[cfg(target_arch = "aarch64")] { return |a,b,l| unsafe { eq_big_impl_neon(a,b,l) }; }
    |_a,_b,_l| unsafe { eq_big_impl_portable(_a,_b,_l) }
}

#[inline(always)]
fn equal_header_fast(a: *const u8, b: *const u8, len: usize) -> bool {
    let f = *EQ_HDR.get_or_init(select_eq_header_impl); unsafe { f(a,b,len) }
}
#[inline(always)]
fn equal_large_fast(a: *const u8, b: *const u8, len: usize) -> bool {
    let f = *EQ_BIG.get_or_init(select_eq_big_impl); unsafe { f(a,b,len) }
}
// ---------------------------------------------------------------------------

// --- [PATCH 139A: dk_test_and_set_seeded_batch_striped32_scatter_fast()] ---
#[inline(always)]
fn dk_test_and_set_seeded_batch_striped32_scatter_fast(
    words: &[core::sync::atomic::AtomicU64],
    keys: &[Pubkey],
    seed: u64,
    out_hits: &mut [bool],
) {
    debug_assert_eq!(keys.len(), out_hits.len());
    if keys.is_empty() { return; }
    #[derive(Copy, Clone)] struct DkLanes { w: [usize; 3], b: [u64; 3] }

    let mut base_idx = 0usize;
    while base_idx < keys.len() {
        let blk = core::cmp::min(32, keys.len() - base_idx);
        let mut lanes: [DkLanes; 32] = [DkLanes { w:[0;3], b:[0;3] }; 32];
        dk_lane_index3_batch_fast(&keys[base_idx..base_idx+blk], seed, words.len(), &mut lanes[..blk]);

        #[derive(Copy, Clone)]
        struct Stripe { base: usize, masks: [u64; 64], olds: [u64; 64], used: bool }
        const MAX_STRIPES: usize = 32;
        let mut stripes: [Stripe; MAX_STRIPES] = [Stripe { base:0, masks:[0;64], olds:[0;64], used:false }; MAX_STRIPES];

        #[inline(always)]
        fn slot_for(stripes: &mut [Stripe; MAX_STRIPES], stripe_idx: usize) -> usize {
            let base = stripe_idx * 64;
            let mut h = (stripe_idx.wrapping_mul(0x9E37_97u32 as usize)) & (MAX_STRIPES - 1);
            loop {
                let s = &mut stripes[h];
                if !s.used { s.used = true; s.base = base; return h; }
                if s.base == base { return h; }
                h = (h + 1) & (MAX_STRIPES - 1);
            }
        }

        // Bucket all (word,bit) updates by stripe.
        for L in lanes.iter().take(blk) {
            let s0 = slot_for(&mut stripes, L.w[0] >> 6);
            let s1 = if (L.w[1] >> 6) == (L.w[0] >> 6) { s0 } else { slot_for(&mut stripes, L.w[1] >> 6) };
            let s2 = if (L.w[2] >> 6) == (L.w[0] >> 6) || (L.w[2] >> 6) == (L.w[1] >> 6) {
                if (L.w[2] >> 6) == (L.w[0] >> 6) { s0 } else { s1 }
            } else { slot_for(&mut stripes, L.w[2] >> 6) };
            stripes[s0].masks[L.w[0] & 63] |= L.b[0];
            stripes[s1].masks[L.w[1] & 63] |= L.b[1];
            stripes[s2].masks[L.w[2] & 63] |= L.b[2];
        }

        #[cfg(target_arch = "x86_64")]
        if core::is_x86_feature_detected!("avx512f") && core::is_x86_feature_detected!("avx512cd") {
            unsafe {
                use core::arch::x86_64::*;
                for s in stripes.iter_mut().filter(|s| s.used) {
                    let mut idxs: [i64; 96] = [0; 96];
                    let mut vals: [u64; 96] = [0; 96];
                    let mut n = 0usize;
                    for L in lanes.iter().take(blk) {
                        let w = [L.w[0], L.w[1], L.w[2]];
                        let b = [L.b[0], L.b[1], L.b[2]];
                        for t in 0..3 {
                            if (w[t] & !63) == s.base {
                                idxs[n] = (w[t] & 63) as i64;
                                vals[n] = b[t];
                                n += 1;
                            }
                        }
                    }
                    let base_ptr = s.masks.as_mut_ptr();
                    let mut off = 0usize;
                    while off < n {
                        let chunk = core::cmp::min(16, n - off);
                        let mut idxv = _mm512_setzero_si512();
                        let mut valv = _mm512_setzero_si512();
                        {
                            let mut idx_tmp = [0i64; 8];
                            let mut val_tmp = [0u64; 8];
                            let take0 = core::cmp::min(8, chunk);
                            for j in 0..take0 { idx_tmp[j] = idxs[off + j]; val_tmp[j] = vals[off + j]; }
                            idxv = _mm512_inserti64x4(idxv, core::mem::transmute(idx_tmp), 0);
                            valv = _mm512_inserti64x4(valv, core::mem::transmute(val_tmp), 0);
                            if chunk > 8 {
                                let mut idx_tmp2 = [0i64; 8];
                                let mut val_tmp2 = [0u64; 8];
                                let take1 = chunk - 8;
                                for j in 0..take1 { idx_tmp2[j] = idxs[off + 8 + j]; val_tmp2[j] = vals[off + 8 + j]; }
                                idxv = _mm512_inserti64x4(idxv, core::mem::transmute(idx_tmp2), 1);
                                valv = _mm512_inserti64x4(valv, core::mem::transmute(val_tmp2), 1);
                            }
                        }
                        let mut rem: __mmask16 = (1u32 << chunk) as u16;
                        while rem != 0 {
                            let conf = _mm512_conflict_epi64(idxv);
                            let zero = _mm512_setzero_si512();
                            let no_conf = _mm512_cmpeq_epi64_mask(conf, zero);
                            let leaders = rem & no_conf;
                            if leaders != 0 {
                                let cur = _mm512_mask_i64gather_epi64(_mm512_setzero_si512(), leaders, idxv, base_ptr as *const i64, 8);
                                let newv = _mm512_or_epi64(cur, valv);
                                _mm512_mask_i64scatter_epi64(base_ptr as *mut i64, leaders, idxv, newv, 8);
                                rem &= !leaders;
                                let sent = _mm512_set1_epi64(0x7f);
                                idxv = _mm512_mask_mov_epi64(idxv, leaders, sent);
                            } else {
                                break;
                            }
                        }
                        off += chunk;
                    }
                }
            }
        }

        // Apply atomics for touched words and store old values for hit decisions.
        for s in stripes.iter_mut().filter(|s| s.used) {
            let base = s.base;
            let upper = core::cmp::min(base + 64, words.len());
            let mut i = base;
            while i < upper {
                let mi = s.masks[i - base];
                if mi != 0 {
                    let old = words[i].fetch_or(mi, core::sync::atomic::Ordering::AcqRel);
                    s.olds[i - base] = old;
                }
                i += 1;
            }
        }

        #[inline(always)]
        fn lookup(stripes: &[Stripe; MAX_STRIPES], w: usize) -> u64 {
            let base = w & !63;
            let mut h = ((base / 64).wrapping_mul(0x9E37_97u32 as usize)) & (MAX_STRIPES - 1);
            loop {
                let s = &stripes[h];
                if s.used && s.base == base { return s.olds[w - base]; }
                h = (h + 1) & (MAX_STRIPES - 1);
            }
        }
        for (k, L) in lanes.iter().take(blk).enumerate() {
            let o0 = lookup(&stripes, L.w[0]);
            let o1 = lookup(&stripes, L.w[1]);
            let o2 = lookup(&stripes, L.w[2]);
            out_hits[base_idx + k] = ((o0 & L.b[0]) != 0) & ((o1 & L.b[1]) != 0) & ((o2 & L.b[2]) != 0);
        }

        base_idx += blk;
    }
}
// ---------------------------------------------------------------------------

// --------------- [PATCH 138A: NUMA detect + bind helpers] -----------------
#[cfg(target_os = "linux")]
#[inline(always)]
fn linux_current_node() -> Option<u32> {
    unsafe {
        let mut cpu: u32 = 0;
        let mut node: u32 = 0;
        if libc::getcpu(&mut cpu as *mut u32, &mut node as *mut u32, std::ptr::null_mut()) == 0 {
            Some(node)
        } else { None }
    }
}

#[cfg(target_os = "linux")]
#[inline(always)]
fn mbind_preferred_region(ptr: *mut u8, len: usize, node: u32) {
    if std::env::var("SC_NUMA_BIND").ok().as_deref() != Some("1") { return; }
    if len == 0 { return; }
    unsafe {
        let ps = libc::sysconf(libc::_SC_PAGESIZE) as usize;
        let base = (ptr as usize) & !(ps - 1);
        let end = (ptr as usize).wrapping_add(len);
        let end_aln = (end + ps - 1) & !(ps - 1);
        let plen = end_aln.saturating_sub(base);
        let ulong_bits = (core::mem::size_of::<libc::c_ulong>() * 8) as usize;
        let words = ((node as usize + 1) + ulong_bits - 1) / ulong_bits;
        let mut mask = vec![0 as libc::c_ulong; words.max(1)];
        let idx = (node as usize) / ulong_bits;
        let bit = (node as usize) % ulong_bits;
        mask[idx] |= (1 as libc::c_ulong) << bit;
        let move_flags = if std::env::var("SC_NUMA_MOVE").ok().as_deref() == Some("1") { libc::MPOL_MF_MOVE } else { 0 };
        let _ = libc::mbind(
            base as *mut libc::c_void,
            plen,
            libc::MPOL_PREFERRED,
            mask.as_ptr(),
            (words * ulong_bits) as libc::c_ulong,
            move_flags as libc::c_uint,
        );
    }
}
// ---------------------------------------------------------------------------

// ---- [PATCH 135A: dk_test_and_set_seeded_batch_auto() simplified] ----------
#[inline(always)]
fn dk_test_and_set_seeded_batch_auto(
    words: &[core::sync::atomic::AtomicU64],
    keys: &[Pubkey],
    seed: u64,
    out_hits: &mut [bool],
) {
    debug_assert_eq!(keys.len(), out_hits.len());
    if keys.is_empty() { return; }
    #[cfg(target_arch = "x86_64")]
    {
        if core::is_x86_feature_detected!("avx512f") && core::is_x86_feature_detected!("avx512cd") && keys.len() >= 32 {
            return dk_test_and_set_seeded_batch_striped32_scatter_fast_v2(words, keys, seed, out_hits);
        }
    }
    // Fallback: AES-batched hash-aggregator
    dk_test_and_set_seeded_batch_fast(words, keys, seed, out_hits)
}
// ---------------------------------------------------------------------------

// --- [PATCH 149A: dk_test_and_set_seeded_batch_striped32_scatter_fast_v2()] ---
#[inline(always)]
fn dk_test_and_set_seeded_batch_striped32_scatter_fast_v2(
    words: &[core::sync::atomic::AtomicU64],
    keys: &[Pubkey],
    seed: u64,
    out_hits: &mut [bool],
) {
    debug_assert_eq!(keys.len(), out_hits.len());
    if keys.is_empty() { return; }
    #[derive(Copy, Clone)] struct DkLanes { w:[usize;3], b:[u64;3] }
    #[derive(Copy, Clone)] struct Stripe { base: usize, masks:[u64;64], olds:[u64;64], used: bool }
    const MAX_STRIPES: usize = 32;

    #[inline(always)]
    fn slot_for(slots: &mut [Stripe; MAX_STRIPES], stripe_idx: usize) -> usize {
        let base = stripe_idx * 64;
        let mut h = (stripe_idx.wrapping_mul(0x9E37_97u32 as usize)) & (MAX_STRIPES - 1);
        loop {
            let s = &mut slots[h];
            if !s.used { s.used = true; s.base = base; return h; }
            if s.base == base { return h; }
            h = (h + 1) & (MAX_STRIPES - 1);
        }
    }

    let mut base_idx = 0usize;
    while base_idx < keys.len() {
        let blk = core::cmp::min(32, keys.len() - base_idx);
        let mut lanes: [DkLanes; 32] = [DkLanes{ w:[0;3], b:[0;3] }; 32];
        dk_lane_index3_batch_fast(&keys[base_idx..base_idx+blk], seed, words.len(), &mut lanes[..blk]);

        let mut stripes: [Stripe; MAX_STRIPES] = [Stripe{ base:0, masks:[0;64], olds:[0;64], used:false }; MAX_STRIPES];

        // Bucket updates by stripe
        for L in lanes.iter().take(blk) {
            let s0 = slot_for(&mut stripes, L.w[0] >> 6);
            let s1 = if (L.w[1] >> 6) == (L.w[0] >> 6) { s0 } else { slot_for(&mut stripes, L.w[1] >> 6) };
            let s2 = if (L.w[2] >> 6) == (L.w[0] >> 6) || (L.w[2] >> 6) == (L.w[1] >> 6) {
                if (L.w[2] >> 6) == (L.w[0] >> 6) { s0 } else { s1 }
            } else { slot_for(&mut stripes, L.w[2] >> 6) };
            stripes[s0].masks[L.w[0] & 63] |= L.b[0];
            stripes[s1].masks[L.w[1] & 63] |= L.b[1];
            stripes[s2].masks[L.w[2] & 63] |= L.b[2];
        }

        // AVX-512 assist: fold duplicates in 8-lane blocks (epi64 width) before scratch ORs
        #[cfg(target_arch = "x86_64")]
        if core::is_x86_feature_detected!("avx512f") {
            for s in stripes.iter_mut().filter(|s| s.used) {
                // materialize this stripe’s pairs
                let mut idxs: [usize; 96] = [0; 96];
                let mut vals: [u64;  96] = [0; 96];
                let mut n = 0usize;
                for L in lanes.iter().take(blk) {
                    let ww = [L.w[0], L.w[1], L.w[2]];
                    let bb = [L.b[0], L.b[1], L.b[2]];
                    for t in 0..3 { if (ww[t] & !63) == s.base { idxs[n] = ww[t] & 63; vals[n] = bb[t]; n += 1; } }
                }
                let mut off = 0usize;
                while off < n {
                    let chunk = core::cmp::min(8, n - off);
                    let mut ia = [0i64; 8];
                    let mut va = [0u64; 8];
                    for j in 0..chunk { ia[j] = idxs[off + j] as i64; va[j] = vals[off + j]; }
                    // fold equal indices in the 8-lane block
                    let mut rem: u8 = (1u16.wrapping_shl(chunk as u32) - 1) as u8;
                    while rem != 0 {
                        let lead = rem.trailing_zeros() as usize;
                        let idx = ia[lead];
                        let mut acc = 0u64;
                        let mut m = rem;
                        while m != 0 {
                            let k = m.trailing_zeros() as usize;
                            if ia[k] == idx { acc |= va[k]; m &= !(1u8 << k); } else { m &= !(1u8 << k); }
                        }
                        s.masks[idx as usize] |= acc;
                        let mut mm = 0u8; for k in 0..chunk { if ia[k] == idx { mm |= 1u8 << k; } }
                        rem &= !mm;
                    }
                    off += chunk;
                }
            }
        }

        // Single fetch_or per touched word
        for s in stripes.iter_mut().filter(|s| s.used) {
            let base = s.base; let upper = core::cmp::min(base + 64, words.len());
            let mut i = base;
            while i < upper { let mi = s.masks[i - base]; if mi != 0 { let old = words[i].fetch_or(mi, core::sync::atomic::Ordering::AcqRel); s.olds[i - base] = old; } i += 1; }
        }

        #[inline(always)]
        fn lookup(s: &[Stripe; MAX_STRIPES], w: usize) -> u64 {
            let base = w & !63; let mut h = ((base / 64).wrapping_mul(0x9E37_97u32 as usize)) & (MAX_STRIPES - 1);
            loop { let st = &s[h]; if st.used && st.base == base { return st.olds[w - base]; } h = (h + 1) & (MAX_STRIPES - 1); }
        }
        for (k, L) in lanes.iter().take(blk).enumerate() {
            let o0 = lookup(&stripes, L.w[0]); let o1 = lookup(&stripes, L.w[1]); let o2 = lookup(&stripes, L.w[2]);
            out_hits[base_idx + k] = ((o0 & L.b[0]) != 0) & ((o1 & L.b[1]) != 0) & ((o2 & L.b[2]) != 0);
        }

        base_idx += blk;
    }
}
// ---------------------------------------------------------------------------

// ---- [PATCH 148A: dk_test_and_set_seeded_batch_dual_auto()] ----
#[inline(always)]
fn dk_test_and_set_seeded_batch_dual_auto(
    words: &[core::sync::atomic::AtomicU64],
    keys: &[Pubkey],
    base_seed: u64,
    out_hits: &mut [bool],
) {
    debug_assert_eq!(keys.len(), out_hits.len());
    if keys.is_empty() { return; }
    #[derive(Copy, Clone)] struct DkLanes { w:[usize;3], b:[u64;3] }
    #[derive(Copy, Clone)] struct Stripe { base: usize, masks:[u64;64], olds:[u64;64], used: bool }
    const MAX_STRIPES: usize = 64;

    let seed_a = salt_per_socket(base_seed);
    let seed_b = seed_a.rotate_left(17) ^ 0xD3C0_5EED_5AFE_C0DEu64;
    let mut la: Vec<DkLanes> = vec![DkLanes{ w:[0;3], b:[0;3] }; keys.len()];
    let mut lb: Vec<DkLanes> = vec![DkLanes{ w:[0;3], b:[0;3] }; keys.len()];
    dk_lane_index3_batch_fast(keys, seed_a, words.len(), &mut la);
    dk_lane_index3_batch_fast(keys, seed_b, words.len(), &mut lb);

    let mut stripes: [Stripe; MAX_STRIPES] = [Stripe{ base:0, masks:[0;64], olds:[0;64], used:false }; MAX_STRIPES];
    #[inline(always)]
    fn slot_for(slots: &mut [Stripe; MAX_STRIPES], stripe_idx: usize) -> usize {
        let base = stripe_idx * 64; let mut h = (stripe_idx.wrapping_mul(0x9E37_97u32 as usize)) & (MAX_STRIPES - 1);
        loop { let s = &mut slots[h]; if !s.used { s.used = true; s.base = base; return h; } if s.base == base { return h; } h = (h + 1) & (MAX_STRIPES - 1); }
    }
    for i in 0..keys.len() {
        let a = la[i]; let b = lb[i];
        let s0 = slot_for(&mut stripes, a.w[0] >> 6); let s1 = if (a.w[1]>>6)==(a.w[0]>>6){s0}else{slot_for(&mut stripes,a.w[1]>>6)};
        let s2 = if (a.w[2]>>6)==(a.w[0]>>6)||(a.w[2]>>6)==(a.w[1]>>6){ if (a.w[2]>>6)==(a.w[0]>>6){s0}else{s1} } else { slot_for(&mut stripes,a.w[2]>>6) };
        stripes[s0].masks[a.w[0]&63] |= a.b[0]; stripes[s1].masks[a.w[1]&63] |= a.b[1]; stripes[s2].masks[a.w[2]&63] |= a.b[2];
        let t0 = slot_for(&mut stripes, b.w[0] >> 6); let t1 = if (b.w[1]>>6)==(b.w[0]>>6){t0}else{slot_for(&mut stripes,b.w[1]>>6)};
        let t2 = if (b.w[2]>>6)==(b.w[0]>>6)||(b.w[2]>>6)==(b.w[1]>>6){ if (b.w[2]>>6)==(b.w[0]>>6){t0}else{t1} } else { slot_for(&mut stripes,b.w[2]>>6) };
        stripes[t0].masks[b.w[0]&63] |= b.b[0]; stripes[t1].masks[b.w[1]&63] |= b.b[1]; stripes[t2].masks[b.w[2]&63] |= b.b[2];
    }
    for s in stripes.iter_mut().filter(|s| s.used) {
        let base = s.base; let upper = core::cmp::min(base + 64, words.len());
        let mut i = base; while i < upper { let mi = s.masks[i-base]; if mi!=0 { let old = words[i].fetch_or(mi, core::sync::atomic::Ordering::AcqRel); s.olds[i-base]=old; } i+=1; }
    }
    #[inline(always)] fn lookup_olds(s:&[Stripe;MAX_STRIPES], w:usize)->u64{ let base=w & !63; let mut h=((base/64).wrapping_mul(0x9E37_97u32 as usize))&(MAX_STRIPES-1); loop{ let st=&s[h]; if st.used&&st.base==base{ return st.olds[w-base]; } h=(h+1)&(MAX_STRIPES-1); } }
    for i in 0..keys.len() {
        let a = la[i]; let b = lb[i];
        let o00=lookup_olds(&stripes,a.w[0]); let o01=lookup_olds(&stripes,a.w[1]); let o02=lookup_olds(&stripes,a.w[2]);
        let o10=lookup_olds(&stripes,b.w[0]); let o11=lookup_olds(&stripes,b.w[1]); let o12=lookup_olds(&stripes,b.w[2]);
        let a_hit = ((o00&a.b[0])!=0) & ((o01&a.b[1])!=0) & ((o02&a.b[2])!=0);
        let b_hit = ((o10&b.b[0])!=0) & ((o11&b.b[1])!=0) & ((o12&b.b[2])!=0);
        out_hits[i] = a_hit | b_hit;
    }
}
// ---------------------------------------------------------------------------

// ---------------------- [PATCH 140A: rt_isolate helpers] --------------------
#[cfg(target_os = "linux")]
#[inline]
fn parse_cpu_list(spec: &str) -> Vec<usize> {
    let mut out = Vec::new();
    for part in spec.split(',') {
        if let Some((a,b)) = part.split_once('-') {
            if let (Ok(s), Ok(e)) = (a.parse::<usize>(), b.parse::<usize>()) {
                for c in s..=e { out.push(c); }
            }
        } else if let Ok(c) = part.parse::<usize>() { out.push(c); }
    }
    out
}

#[cfg(target_os = "linux")]
#[inline]
pub fn enable_rt_isolation_from_env() {
    unsafe {
        if std::env::var("SC_RT_ISO").ok().as_deref() != Some("1") { return; }
        let _ = libc::mlockall(libc::MCL_CURRENT | libc::MCL_FUTURE);
        if let Ok(spec) = std::env::var("SC_AFFINITY") {
            let cpus = parse_cpu_list(&spec);
            if !cpus.is_empty() {
                let mut set: libc::cpu_set_t = core::mem::zeroed();
                for c in cpus {
                    let idx = c / 64; let bit = c % 64; let ptr = &mut set as *mut _ as *mut u64; *ptr.add(idx) |= 1u64 << bit;
                }
                let _ = libc::pthread_setaffinity_np(libc::pthread_self(), core::mem::size_of::<libc::cpu_set_t>(), &set);
            }
        }
        if let Ok(pol) = std::env::var("SC_RT_POLICY") {
            let prio = std::env::var("SC_RT_PRIO").ok().and_then(|v| v.parse::<i32>().ok()).unwrap_or(1);
            let mut sched: libc::sched_param = core::mem::zeroed();
            sched.sched_priority = prio;
            let policy = match pol.as_str() { "FIFO" => libc::SCHED_FIFO, "RR" => libc::SCHED_RR, _ => libc::SCHED_OTHER };
            let _ = libc::sched_setscheduler(0, policy, &sched);
        }
        if let Ok(nice) = std::env::var("SC_NICE") {
            if let Ok(n) = nice.parse::<i32>() { let _ = libc::setpriority(libc::PRIO_PROCESS, 0, n); }
        }
        if std::env::var("SC_STACK_THP").ok().as_deref() == Some("1") {
            let mut lim: libc::rlimit = core::mem::zeroed();
            if libc::getrlimit(libc::RLIMIT_STACK, &mut lim) == 0 {
                let sp = &lim as *const _ as usize; let ps = libc::sysconf(libc::_SC_PAGESIZE) as usize; let base = sp & !(ps - 1);
                let _ = libc::madvise(base as *mut _, ps, libc::MADV_HUGEPAGE);
            }
        }
    }
}
// ---------------------------------------------------------------------------

// ----------------------- [PATCH 137A: waitpkg helpers] ----------------------
use std::sync::OnceLock;
#[inline(always)]
fn has_waitpkg() -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        static HAS: OnceLock<bool> = OnceLock::new();
        return *HAS.get_or_init(|| unsafe {
            let mut eax: u32 = 7; let mut ebx: u32; let mut ecx: u32 = 0; let mut edx: u32;
            core::arch::asm!(
                "cpuid",
                inout("eax") eax, out("ebx") ebx, inout("ecx") ecx, out("edx") edx,
                options(nomem, nostack, preserves_flags)
            );
            ((ecx >> 5) & 1) != 0
        });
    }
    #[cfg(not(target_arch = "x86_64"))]
    { false }
}
#[inline(always)]
fn waitpkg_monitor(addr: *const u8, cycles: u32) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        if !has_waitpkg() { return; }
        core::arch::asm!(
            "umonitor [{0}]",
            "xor edx, edx",
            "mov eax, {1:e}",
            "umwait",
            in(reg) addr,
            in(reg) cycles,
            options(nostack, preserves_flags)
        );
    }
}
// ---------------------------------------------------------------------------

// ---------------- [PATCH 133A: NEW demote_tail_lines()] -----------------------
#[inline(always)]
fn demote_tail_lines(dst: *const u8, len: usize) {
    #[cfg(target_arch = "x86_64")]
    {
        if std::env::var("SC_CLDEMOTE_TAIL").ok().as_deref() != Some("1") { return; }
        if !core::is_x86_feature_detected!("cldemote") { return; }
        if len == 0 { return; }
        let cl = cache_line_len();
        let n_lines = ((len + cl - 1) / cl).min(
            std::env::var("SC_CLDEMOTE_TAIL_LINES").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(4)
        );
        unsafe {
            let mut off = len & !(cl - 1);
            for _ in 0..n_lines {
                let p = dst.add(off);
                core::arch::asm!("cldemote [{0}]", in(reg) p, options(nostack, preserves_flags));
                if off == 0 { break; }
                off = off.saturating_sub(cl);
            }
        }
    }
}
// ---------------------------------------------------------------------------

// ---------------- [PATCH 131A: NEW demote_written_region()] -----------------
#[inline(always)]
fn demote_written_region(p: *const u8, len: usize) {
    #[cfg(target_arch = "x86_64")]
    {
        if std::env::var("SC_CLDEMOTE").ok().as_deref() != Some("1") { return; }
        if !core::is_x86_feature_detected!("cldemote") { return; }
        let cl = cache_line_len();
        let mut a = (p as usize) & !(cl - 1);
        let end = (p as usize).wrapping_add(len);
        unsafe {
            while a < end {
                core::arch::asm!("cldemote [{0}]", in(reg) (a as *const u8), options(nostack, preserves_flags));
                a += cl;
            }
        }
    }
}
// ---------------------------------------------------------------------------

// -------- [PATCH 127A: dk_test_and_set_seeded_batch_striped32_fast()] -----
#[inline(always)]
fn dk_test_and_set_seeded_batch_striped32_fast(
    words: &[core::sync::atomic::AtomicU64],
    keys: &[Pubkey],
    seed: u64,
    out_hits: &mut [bool],
) {
    debug_assert_eq!(keys.len(), out_hits.len());
    if keys.is_empty() { return; }
    let mut idx = 0usize;
    while idx < keys.len() {
        let blk = core::cmp::min(32, keys.len() - idx);
        // lanes buffer for this block
        let mut lanes: [DkLanes; 32] = [DkLanes { w:[0;3], b:[0;3] }; 32];
        dk_lane_index3_batch_fast(&keys[idx..idx+blk], seed, words.len(), &mut lanes[..blk]);

        #[derive(Copy, Clone)]
        struct Stripe { base: usize, masks: [u64;64], olds: [u64;64], used: bool }
        const MAX_STRIPES: usize = 32;
        let mut stripes: [Stripe; MAX_STRIPES] = [Stripe { base:0, masks:[0;64], olds:[0;64], used:false }; MAX_STRIPES];

        #[inline(always)]
        fn slot_for(stripes: &mut [Stripe; MAX_STRIPES], stripe_idx: usize) -> usize {
            let base = stripe_idx * 64;
            let mut h = (stripe_idx.wrapping_mul(0x9E37_97u32 as usize)) & (MAX_STRIPES - 1);
            loop {
                let s = &mut stripes[h];
                if !s.used { s.used = true; s.base = base; return h; }
                if s.base == base { return h; }
                h = (h + 1) & (MAX_STRIPES - 1);
            }
        }

        // Try AVX-512 pack; else scalar fold
        #[cfg(target_arch = "x86_64")]
        if core::is_x86_feature_detected!("avx512f") {
            unsafe {
                use core::arch::x86_64::{_mm512_set1_epi64,_mm512_sllv_epi64,__m512i};
                for pass in 0..3 {
                    let mut p = 0usize;
                    while p < blk {
                        let chunk = core::cmp::min(8, blk - p);
                        let mut sh_arr = [0u64; 8];
                        let mut wi_arr = [0usize; 8];
                        for j in 0..chunk {
                            let L = lanes[p + j];
                            let w = L.w[pass]; let b = L.b[pass].trailing_zeros() as u64;
                            sh_arr[j] = b; wi_arr[j] = w;
                        }
                        let one: __m512i = _mm512_set1_epi64(1);
                        let sh = core::mem::transmute::<[u64;8], __m512i>(sh_arr);
                        let bits: __m512i = _mm512_sllv_epi64(one, sh);
                        let mut masks_lane = [0u64; 8];
                        core::ptr::copy_nonoverlapping(&bits as *const __m512i as *const u64, masks_lane.as_mut_ptr(), 8);
                        for j in 0..chunk {
                            let w = wi_arr[j]; let m = masks_lane[j];
                            let sidx = slot_for(&mut stripes, w >> 6);
                            stripes[sidx].masks[w & 63] |= m;
                        }
                        p += chunk;
                    }
                }
            }
        } else {
            for L in lanes.iter().take(blk) {
                let s0 = slot_for(&mut stripes, L.w[0] >> 6);
                let s1 = if (L.w[1] >> 6) == (L.w[0] >> 6) { s0 } else { slot_for(&mut stripes, L.w[1] >> 6) };
                let s2 = if (L.w[2] >> 6) == (L.w[0] >> 6) || (L.w[2] >> 6) == (L.w[1] >> 6) {
                    if (L.w[2] >> 6) == (L.w[0] >> 6) { s0 } else { s1 }
                } else { slot_for(&mut stripes, L.w[2] >> 6) };
                stripes[s0].masks[L.w[0] & 63] |= L.b[0];
                stripes[s1].masks[L.w[1] & 63] |= L.b[1];
                stripes[s2].masks[L.w[2] & 63] |= L.b[2];
            }
        }

        // Apply: one fetch_or per touched word
        for s in stripes.iter_mut().filter(|s| s.used) {
            let base = s.base;
            let upper = core::cmp::min(base + 64, words.len());
            let mut i2 = base;
            while i2 < upper {
                let mi = s.masks[i2 - base];
                if mi != 0 {
                    let old = words[i2].fetch_or(mi, Ordering::AcqRel);
                    s.olds[i2 - base] = old;
                }
                i2 += 1;
            }
        }

        #[inline(always)]
        fn lookup(stripes: &[Stripe; MAX_STRIPES], w: usize) -> u64 {
            let base = w & !63;
            let mut h = ((base / 64).wrapping_mul(0x9E37_97u32 as usize)) & (MAX_STRIPES - 1);
            loop {
                let s = &stripes[h];
                if s.used && s.base == base { return s.olds[w - base]; }
                h = (h + 1) & (MAX_STRIPES - 1);
            }
        }
        for (k, L) in lanes.iter().take(blk).enumerate() {
            let o0 = lookup(&stripes, L.w[0]);
            let o1 = lookup(&stripes, L.w[1]);
            let o2 = lookup(&stripes, L.w[2]);
            out_hits[idx + k] = ((o0 & L.b[0]) != 0) & ((o1 & L.b[1]) != 0) & ((o2 & L.b[2]) != 0);
        }

        idx += blk;
    }
}
// ---------------------------------------------------------------------------
// ---------------------- [PATCH 117A: batched doorkeeper] -------------------
#[derive(Copy, Clone)]
struct DkAgg { idx: usize, mask: u64, used: bool, old: u64 }

#[inline(always)]
fn dk_test_and_set_seeded_batch(
    words: &[core::sync::atomic::AtomicU64],
    keys: &[Pubkey],
    seed: u64,
    out_hits: &mut [bool],
) {
    debug_assert_eq!(keys.len(), out_hits.len());
    if keys.is_empty() { return; }

    // Capacity scaled to inputs (power of two, min 512)
    let mut cap = 1usize;
    while cap < keys.len() * 3 { cap <<= 1; }
    if cap < 512 { cap = 512; }
    let mut tbl: Vec<DkAgg> = vec![DkAgg { idx:0, mask:0, used:false, old:0 }; cap];

    #[inline(always)]
    fn agg_insert(tbl: &mut [DkAgg], idx: usize, bit: u64) {
        let mask = tbl.len() - 1;
        let mut h = (idx.wrapping_mul(0x9E37_79B9)) & mask;
        loop {
            let e = unsafe { tbl.get_unchecked_mut(h) };
            if !e.used {
                *e = DkAgg { idx, mask: bit, used: true, old: 0 };
                return;
            } else if e.idx == idx { e.mask |= bit; return; }
            h = (h + 1) & mask;
        }
    }

    // Record lanes for each key
    let mut lanes_w: Vec<(usize, usize, usize)> = Vec::with_capacity(keys.len());
    let mut lanes_b: Vec<(u64, u64, u64)> = Vec::with_capacity(keys.len());

    for pk in keys.iter() {
        // compute three lanes using AES-mixed 128-bit core + BMI2 (fallback safe)
        let (lo, hi) = mix_pubkey128(pk, seed);
        let (w0,b0, w1,b1, w2,b2) = {
            #[cfg(target_arch = "x86_64")]
            {
                if core::is_x86_feature_detected!("bmi2") {
                    use core::arch::x86_64::_pext_u64;
                    const M0: u64 = 0x8040_2010_0804_0201;
                    const M1: u64 = 0x4020_1008_0402_0180;
                    const M2: u64 = 0x1111_2222_4444_8888;
                    let i0 = unsafe { _pext_u64(lo, M0) } as usize % (words.len()*64);
                    let i1 = unsafe { _pext_u64(hi, M1) } as usize % (words.len()*64);
                    let i2 = unsafe { _pext_u64(lo ^ hi.rotate_left(7), M2) } as usize % (words.len()*64);
                    (i0>>6, 1u64<<(i0&63), i1>>6, 1u64<<(i1&63), i2>>6, 1u64<<(i2&63))
                } else {
                    let z0 = (lo ^ (hi>>7)).wrapping_mul(0x9E37_79B9_7F4A_7C15) as usize % (words.len()*64);
                    let z1 = (hi ^ (lo>>9)).wrapping_mul(0xC2B2_AE3D_27D4_EB4F) as usize % (words.len()*64);
                    let z2 = (lo ^ hi.rotate_left(13)).wrapping_mul(0x1656_67B1_ECDB_975B_u64) as usize % (words.len()*64);
                    (z0>>6, 1u64<<(z0&63), z1>>6, 1u64<<(z1&63), z2>>6, 1u64<<(z2&63))
                }
            }
            #[cfg(not(target_arch = "x86_64"))]
            {
                let z0 = (lo ^ (hi>>7)).wrapping_mul(0x9E37_79B9_7F4A_7C15) as usize % (words.len()*64);
                let z1 = (hi ^ (lo>>9)).wrapping_mul(0xC2B2_AE3D_27D4_EB4F) as usize % (words.len()*64);
                let z2 = (lo ^ hi.rotate_left(13)).wrapping_mul(0x1656_67B1_ECDB_975B_u64) as usize % (words.len()*64);
                (z0>>6, 1u64<<(z0&63), z1>>6, 1u64<<(z1&63), z2>>6, 1u64<<(z2&63))
            }
        };
        lanes_w.push((w0,w1,w2)); lanes_b.push((b0,b1,b2));
        agg_insert(&mut tbl, w0, b0); agg_insert(&mut tbl, w1, b1); agg_insert(&mut tbl, w2, b2);
    }

    // Single fetch_or per touched word, cache old
    for e in tbl.iter_mut() {
        if e.used {
            let old = words[e.idx].fetch_or(e.mask, Ordering::AcqRel);
            e.old = old;
        }
    }

    // Lookup helper for old snapshots
    #[inline(always)]
    fn lookup(tbl: &[DkAgg], mask: usize, idx: usize) -> u64 {
        let mut h = (idx.wrapping_mul(0x9E37_79B9)) & mask;
        loop {
            let e = unsafe { tbl.get_unchecked(h) };
            if e.used && e.idx == idx { return e.old; }
            h = (h + 1) & mask;
        }
    }

    let mask = tbl.len() - 1;
    for i in 0..keys.len() {
        let (w0,w1,w2) = lanes_w[i];
        let (b0,b1,b2) = lanes_b[i];
        let o0 = lookup(&tbl, mask, w0);
        let o1 = lookup(&tbl, mask, w1);
        let o2 = lookup(&tbl, mask, w2);
        out_hits[i] = ((o0 & b0) != 0) & ((o1 & b1) != 0) & ((o2 & b2) != 0);
    }
}
// ---------------------------------------------------------------------------

// --------------- [PATCH 120A: dk_lane_index3_batch_fast()] ----------------
#[derive(Copy, Clone)]
struct DkLanes { w: [usize; 3], b: [u64; 3] }

#[inline(always)]
fn dk_lane_index3_batch_fast(keys: &[Pubkey], seed: u64, words: usize, out: &mut [DkLanes]) {
    debug_assert_eq!(keys.len(), out.len());
    if keys.is_empty() { return; }
    #[cfg(target_arch = "x86_64")]
    if core::is_x86_feature_detected!("aes") {
        unsafe {
            use core::arch::x86_64::*;
            const M0: u64 = 0x8040_2010_0804_0201;
            const M1: u64 = 0x4020_1008_0402_0180;
            const M2: u64 = 0x1111_2222_4444_8888;
            let k1 = _mm_set1_epi64x(seed as i64);
            let k2 = _mm_set1_epi64x(seed.rotate_left(17) as i64);
            let total_bits = words * 64;
            let mut i = 0usize;
            while i < keys.len() {
                let blk = core::cmp::min(8, keys.len() - i);
                let mut a: [__m128i; 8] = core::mem::zeroed();
                let mut c: [__m128i; 8] = core::mem::zeroed();
                let mut j = 0;
                while j < blk {
                    let b = keys[i + j].to_bytes();
                    a[j] = _mm_loadu_si128(b.as_ptr() as *const __m128i);
                    c[j] = _mm_loadu_si128(b.as_ptr().add(16) as *const __m128i);
                    j += 1;
                }
                j = 0;
                while j < blk {
                    let mut x = _mm_xor_si128(a[j], k1);
                    let mut y = _mm_xor_si128(c[j], k2);
                    x = _mm_aesenc_si128(x, y);
                    y = _mm_aesenc_si128(y, x);
                    let m = _mm_xor_si128(x, y);
                    let lo = _mm_cvtsi128_si64(m) as u64;
                    let hi = _mm_extract_epi64(m, 1) as u64;
                    let (i0, i1, i2) = if core::is_x86_feature_detected!("bmi2") {
                        let a0 = _pext_u64(lo, M0) as usize;
                        let a1 = _pext_u64(hi, M1) as usize;
                        let a2 = _pext_u64(lo ^ hi.rotate_left(7), M2) as usize;
                        (a0 % total_bits, a1 % total_bits, a2 % total_bits)
                    } else {
                        let z0 = (lo ^ (hi >> 7)).wrapping_mul(0x9E37_79B9_7F4A_7C15) as usize % total_bits;
                        let z1 = (hi ^ (lo >> 9)).wrapping_mul(0xC2B2_AE3D_27D4_EB4F) as usize % total_bits;
                        let z2 = (lo ^ hi.rotate_left(13)).wrapping_mul(0x1656_67B1_ECDB_975B) as usize % total_bits;
                        (z0, z1, z2)
                    };
                    out[i + j] = DkLanes { w: [i0>>6, i1>>6, i2>>6], b: [1u64<<(i0&63), 1u64<<(i1&63), 1u64<<(i2&63)] };
                    j += 1;
                }
                i += blk;
            }
            return;
        }
    }
    #[cfg(all(target_arch = "aarch64"))]
    if std::arch::is_aarch64_feature_detected!("aes") {
        unsafe {
            use core::arch::aarch64::*;
            let ks1 = vreinterpretq_u8_u64(vdupq_n_u64(seed));
            let ks2 = vreinterpretq_u8_u64(vdupq_n_u64(seed.rotate_left(17)));
            let total_bits = words * 64;
            let mut i = 0usize;
            while i < keys.len() {
                let blk = core::cmp::min(8, keys.len() - i);
                let mut j = 0;
                while j < blk {
                    let b = keys[i + j].to_bytes();
                    let a = vld1q_u8(b.as_ptr());
                    let c = vld1q_u8(b.as_ptr().add(16));
                    let mut x = veorq_u8(a, ks1);
                    let mut y = veorq_u8(c, ks2);
                    x = vaeseq_u8(x, y); x = vaesmcq_u8(x);
                    y = vaeseq_u8(y, x); y = vaesmcq_u8(y);
                    let m = veorq_u8(x, y);
                    let lo = vgetq_lane_u64(vreinterpretq_u64_u8(m), 0);
                    let hi = vgetq_lane_u64(vreinterpretq_u64_u8(m), 1);
                    let z0 = (lo ^ (hi >> 7)).wrapping_mul(0x9E37_79B9_7F4A_7C15) as usize % total_bits;
                    let z1 = (hi ^ (lo >> 9)).wrapping_mul(0xC2B2_AE3D_27D4_EB4F) as usize % total_bits;
                    let z2 = (lo ^ hi.rotate_left(13)).wrapping_mul(0x1656_67B1_ECDB_975B) as usize % total_bits;
                    out[i + j] = DkLanes { w: [z0>>6, z1>>6, z2>>6], b: [1u64<<(z0&63), 1u64<<(z1&63), 1u64<<(z2&63)] };
                    j += 1;
                }
                i += blk;
            }
            return;
        }
    }
    for (i, pk) in keys.iter().enumerate() {
        let [(w0,b0),(w1,b1),(w2,b2)] = dk_lane_index3(pk, seed, words);
        out[i] = DkLanes { w: [w0,w1,w2], b: [b0,b1,b2] };
    }
}
// ---------------------------------------------------------------------------

// -------- [PATCH 120B: dk_test_and_set_seeded_batch_fast()] ---------------
#[inline(always)]
fn dk_test_and_set_seeded_batch_fast(
    words: &[core::sync::atomic::AtomicU64],
    keys: &[Pubkey],
    seed: u64,
    out_hits: &mut [bool],
) {
    debug_assert_eq!(keys.len(), out_hits.len());
    if keys.is_empty() { return; }
    let mut lanes: Vec<DkLanes> = vec![DkLanes { w:[0;3], b:[0;3] }; keys.len()];
    dk_lane_index3_batch_fast(keys, seed, words.len(), &mut lanes);
    const CAP: usize = 512;
    let mut tbl: [DkAgg; CAP] = [DkAgg { idx:0, mask:0, used:false, old:0 }; CAP];
    #[inline(always)]
    fn agg_insert(tbl: &mut [DkAgg; CAP], idx: usize, bit: u64) {
        let mut h = (idx.wrapping_mul(0x9E37_79B9)) & (CAP - 1);
        loop {
            let e = &mut tbl[h];
            if !e.used { *e = DkAgg { idx, mask: bit, used: true, old: 0 }; return; }
            if e.idx == idx { e.mask |= bit; return; }
            h = (h + 1) & (CAP - 1);
        }
    }
    for L in lanes.iter() {
        agg_insert(&mut tbl, L.w[0], L.b[0]);
        agg_insert(&mut tbl, L.w[1], L.b[1]);
        agg_insert(&mut tbl, L.w[2], L.b[2]);
    }
    for e in &mut tbl {
        if e.used { e.old = words[e.idx].fetch_or(e.mask, Ordering::AcqRel); }
    }
    #[inline(always)]
    fn lookup(tbl: &[DkAgg; CAP], idx: usize) -> u64 {
        let mut h = (idx.wrapping_mul(0x9E37_79B9)) & (CAP - 1);
        loop {
            let e = &tbl[h];
            if e.used && e.idx == idx { return e.old; }
            h = (h + 1) & (CAP - 1);
        }
    }
    for (i, L) in lanes.iter().enumerate() {
        let o0 = lookup(&tbl, L.w[0]); let o1 = lookup(&tbl, L.w[1]); let o2 = lookup(&tbl, L.w[2]);
        out_hits[i] = ((o0 & L.b[0]) != 0) & ((o1 & L.b[1]) != 0) & ((o2 & L.b[2]) != 0);
    }
}
// ---------------------------------------------------------------------------

// [PATCH 96A] Slot leader round atomics (global, per-process)
use std::sync::OnceLock as _OnceLockRounds;
static SLOT_ROUND: _OnceLockRounds<std::sync::atomic::AtomicU64> = _OnceLockRounds::new();
static SLOT_MAINT_DONE: _OnceLockRounds<std::sync::atomic::AtomicU64> = _OnceLockRounds::new();
static SLOT_FLUSH_DONE: _OnceLockRounds<std::sync::atomic::AtomicU64> = _OnceLockRounds::new();
#[inline]
fn slot_round() -> &'static std::sync::atomic::AtomicU64 { SLOT_ROUND.get_or_init(|| std::sync::atomic::AtomicU64::new(0)) }
#[inline]
fn slot_maint_done() -> &'static std::sync::atomic::AtomicU64 { SLOT_MAINT_DONE.get_or_init(|| std::sync::atomic::AtomicU64::new(0)) }
#[inline]
fn slot_flush_done() -> &'static std::sync::atomic::AtomicU64 { SLOT_FLUSH_DONE.get_or_init(|| std::sync::atomic::AtomicU64::new(0)) }

// ------------- [PATCH 105A: PromRing v2 (Vyukov MPSC)] -----------------------
use core::cell::UnsafeCell as _UnsafeCellRing;
use core::sync::atomic::{AtomicUsize as _AtomicUsizeRing, Ordering as _OrderingRing};

struct RingCell { seq: _AtomicUsizeRing, val: _UnsafeCellRing<CacheEntry> }
unsafe impl Send for RingCell {}
unsafe impl Sync for RingCell {}

pub struct PromRing<const N: usize> {
    buf: Box<[RingCell]>,
    head: Al64<_AtomicUsizeRing>, // producers
    tail: Al64<_AtomicUsizeRing>, // single-consumer
    mask: usize,
}

impl<const N: usize> PromRing<N> {
    pub fn new(zero: CacheEntry) -> Self {
        assert!(N.is_power_of_two());
        let mut v = Vec::with_capacity(N);
        for i in 0..N {
            v.push(RingCell { seq: _AtomicUsizeRing::new(i), val: _UnsafeCellRing::new(zero) });
        }
        Self { buf: v.into_boxed_slice(), head: Al64(_AtomicUsizeRing::new(0)), tail: Al64(_AtomicUsizeRing::new(0)), mask: N - 1 }
    }

    #[inline]
    pub fn push(&self, e: CacheEntry) -> bool {
        let mut pos = self.head.load(_OrderingRing::Relaxed);
        loop {
            let cell = &self.buf[pos & self.mask];
            let seq = cell.seq.load(_OrderingRing::Acquire);
            let dif = (seq as isize) - (pos as isize);
            if dif == 0 {
                if self.head.compare_exchange_weak(pos, pos.wrapping_add(1), _OrderingRing::Relaxed, _OrderingRing::Relaxed).is_ok() {
                    unsafe { *cell.val.get() = e; }
                    cell.seq.store(pos.wrapping_add(1), _OrderingRing::Release);
                    return true;
                }
            } else if dif < 0 {
                return false; // full
            } else {
                pos = self.head.load(_OrderingRing::Relaxed);
            }
        }
    }

    #[inline]
    pub fn pop(&self) -> Option<CacheEntry> {
        let mut pos = self.tail.load(_OrderingRing::Relaxed);
        loop {
            let cell = &self.buf[pos & self.mask];
            let seq = cell.seq.load(_OrderingRing::Acquire);
            let dif = (seq as isize) - ((pos.wrapping_add(1)) as isize);
            if dif == 0 {
                if self.tail.compare_exchange_weak(pos, pos.wrapping_add(1), _OrderingRing::Relaxed, _OrderingRing::Relaxed).is_ok() {
                    let e = unsafe { *cell.val.get() };
                    cell.seq.store(pos.wrapping_add(self.mask + 1), _OrderingRing::Release);
                    return Some(e);
                }
            } else if dif < 0 {
                return None; // empty
            } else {
                pos = self.tail.load(_OrderingRing::Relaxed);
            }
        }
    }

    // ------------- [PATCH 132A: NEW PromRing::drain_burst()] ------------------
    /// Drain up to `max` entries, applying `f` to each. If empty, perform a
    /// very short PAUSE/YIELD to avoid hammering the coherency fabric.
    #[inline]
    pub fn drain_burst<F: FnMut(CacheEntry)>(&self, max: usize, mut f: F) -> usize {
        let mut n = 0usize;
        while n < max {
            if let Some(e) = self.pop() {
                f(e);
                n += 1;
            } else {
                // brief nap when empty
                #[cfg(target_arch = "x86_64")]
                unsafe {
                    core::arch::asm!(
                        "pause; pause; pause; pause;",
                        options(nostack, preserves_flags)
                    );
                }
                #[cfg(target_arch = "aarch64")]
                unsafe { core::arch::asm!("yield", options(nostack, preserves_flags)); }
                break;
            }
        }
        n
    }
}

const PROM_RING_SIZE: usize = 1024; // power of two

fn get_prom_rings(cache: &MemoryMappedStateCache) -> &'static [PromRing<PROM_RING_SIZE>] {
    static RINGS: OnceLock<Box<[PromRing<PROM_RING_SIZE>]>> = OnceLock::new();
    RINGS.get_or_init(|| {
        let shards = cache.numa_shards.max(1);
        let zero = CacheEntry { pubkey: [0;32], offset:0, size:0, slot:0, last_access_us:0, checksum:0, flags:0, rev:0, _pad:0 };
        let mut v: Vec<PromRing<PROM_RING_SIZE>> = Vec::with_capacity(shards);
        for _ in 0..shards { v.push(PromRing::new(zero)); }
        v.into_boxed_slice()
    })
}
// ------------------------------------------------------------------------------
// --------------------- [PATCH 98A: NEW TleMutex] ------------------------------
use core::ops::{Deref, DerefMut};
use core::cell::UnsafeCell;

pub struct TleMutex<T> {
    cell: UnsafeCell<T>,
    fallback: parking_lot::Mutex<()>,
    state: core::sync::atomic::AtomicU32, // 0=free, 1=locked (fallback path)
}

unsafe impl<T: Send> Send for TleMutex<T> {}
unsafe impl<T: Send> Sync for TleMutex<T> {}

impl<T> TleMutex<T> {
    pub fn new(v: T) -> Self {
        Self { cell: UnsafeCell::new(v), fallback: parking_lot::Mutex::new(()), state: core::sync::atomic::AtomicU32::new(0) }
    }

    #[inline(always)]
    pub fn lock(&self) -> TleGuard<'_, T> {
        #[cfg(target_arch = "x86_64")]
        {
            if core::is_x86_feature_detected!("rtm") {
                unsafe {
                    use core::arch::x86_64::{_xabort, _xbegin, _xend};
                    const ABORT_MASK: u32 = 0xFF;
                    let mut spins = 0u32;
                    loop {
                        if self.state.load(core::sync::atomic::Ordering::Acquire) != 0 { break; }
                        let st = _xbegin();
                        if st == u32::MAX {
                            if self.state.load(core::sync::atomic::Ordering::Relaxed) == 0 {
                                return TleGuard { m: self, held: false };
                            }
                            _xabort(ABORT_MASK);
                        } else {
                            spins = spins.wrapping_add(1);
                            if spins > 8 { break; }
                            core::hint::spin_loop();
                        }
                    }
                }
            }
        }
        self.state.store(1, core::sync::atomic::Ordering::Release);
        let g = self.fallback.lock();
        TleGuard { m: self, held: true, g: Some(g) }
    }
}

pub struct TleGuard<'a, T> { m: &'a TleMutex<T>, held: bool, g: Option<parking_lot::MutexGuard<'a, ()>> }

impl<'a, T> Drop for TleGuard<'a, T> {
    #[inline(always)]
    fn drop(&mut self) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use core::arch::x86_64::{_xend, _xtest};
            if !self.held && _xtest() != 0 { _xend(); return; }
        }
        if self.held {
            // Drop the guard first to release the fallback lock, then clear state
            drop(self.g.take());
            self.m.state.store(0, core::sync::atomic::Ordering::Release);
        }
    }
}

impl<'a, T> Deref for TleGuard<'a, T> {
    type Target = T;
    #[inline(always)] fn deref(&self) -> &Self::Target { unsafe { &*self.m.cell.get() } }
}
impl<'a, T> DerefMut for TleGuard<'a, T> {
    #[inline(always)] fn deref_mut(&mut self) -> &mut Self::Target { unsafe { &mut *self.m.cell.get() } }
}
// ------------------------------------------------------------------------------

// ------------- [PATCH 94B: NEW copy_impl_avx512()] ----------------------------
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
#[inline(always)]
unsafe fn copy_impl_avx512(dst: *mut u8, src: *const u8, len: usize) {
    use core::arch::x86_64::{
        __m512i, _mm512_loadu_si512, _mm512_stream_si512, _mm_prefetch, _mm_sfence, _MM_HINT_NTA
    };
    let pf_stride = (cache_line_len() * 8).max(256);
    let mut d = dst; let mut s = src;

    // 64B align stores for best NT combining
    let mis64 = (d as usize) & 63;
    if mis64 != 0 {
        let head = core::cmp::min(64 - mis64, len);
        core::ptr::copy_nonoverlapping(s, d, head);
        d = d.add(head); s = s.add(head);
    }

    let n = len - ((d as usize).wrapping_sub(dst as usize));
    let chunks = n / 64; // 64B per ZMM
    let tail   = n % 64;

    let mut i = 0usize;
    while i + 3 < chunks {
        if (i & 3) == 0 { _mm_prefetch(s.add(i*64 + pf_stride) as *const i8, _MM_HINT_NTA); }
        let v0: __m512i = _mm512_loadu_si512(s.add(i*64)      as *const __m512i);
        let v1: __m512i = _mm512_loadu_si512(s.add((i+1)*64)  as *const __m512i);
        let v2: __m512i = _mm512_loadu_si512(s.add((i+2)*64)  as *const __m512i);
        let v3: __m512i = _mm512_loadu_si512(s.add((i+3)*64)  as *const __m512i);
        _mm512_stream_si512(d.add(i*64)      as *mut __m512i, v0);
        _mm512_stream_si512(d.add((i+1)*64)  as *mut __m512i, v1);
        _mm512_stream_si512(d.add((i+2)*64)  as *mut __m512i, v2);
        _mm512_stream_si512(d.add((i+3)*64)  as *mut __m512i, v3);
        i += 4;
    }
    while i < chunks {
        let v: __m512i = _mm512_loadu_si512(s.add(i*64) as *const __m512i);
        _mm512_stream_si512(d.add(i*64) as *mut __m512i, v);
        i += 1;
    }
    if tail != 0 { core::ptr::copy_nonoverlapping(s.add(chunks*64), d.add(chunks*64), tail); }
    _mm_sfence();
}
// ------------------------------------------------------------------------------
impl<T> core::ops::DerefMut for Al64<T> { #[inline] fn deref_mut(&mut self) -> &mut T { &mut self.0 } }
// --------------- [PATCH 42: TLS batched promotions] ----------------------------
struct TlsPromo { buf: heapless::Vec<CacheEntry, 32>, last_us: u64 }
// Minimal fixed-cap vec without external crate:
mod heapless {
    use core::mem::MaybeUninit;
    pub struct Vec<T, const N: usize> { pub(crate) data: [MaybeUninit<T>; N], pub len: usize }
    impl<T, const N: usize> Vec<T, N> {
        pub const fn new() -> Self { Self { data: unsafe { MaybeUninit::<[MaybeUninit<T>; N]>::uninit().assume_init() }, len: 0 } }
        #[inline] pub fn clear(&mut self){ self.len = 0; }
        #[inline] pub fn push(&mut self, v: T) { if self.len < N { self.data[self.len].write(v); self.len += 1; } }
        #[inline] pub fn iter(&self) -> impl Iterator<Item=&T> {
            (0..self.len).map(move |i| unsafe { &*self.data[i].as_ptr() })
        }
    }
}

thread_local! {
    static TLS_PROMO: std::cell::RefCell<TlsPromo> =
        std::cell::RefCell::new(TlsPromo { buf: heapless::Vec::<CacheEntry,32>::new(), last_us: 0 });
}

// -------- [PATCH 101A: TLS micro-LRU for deduping promotions] -----------------
#[derive(Copy, Clone, Default)]
struct MiniEnt { key: [u8;32], stamp_us: u64 }

thread_local! {
    static TLS_PROMO_MINI: std::cell::RefCell<[MiniEnt; 8]> =
        std::cell::RefCell::new([MiniEnt::default(); 8]);
}

#[inline(always)]
fn promo_dedupe_tls(pubkey: &Pubkey, now: u64) -> bool {
    let kb = pubkey.to_bytes();
    TLS_PROMO_MINI.with(|cell| {
        let mut arr = cell.borrow_mut();
        let i = ((kb[3] ^ kb[17] ^ kb[29]) & 7) as usize;
        let e = &mut arr[i];
        let same = pk_eq_fast(&e.key, &Pubkey::new_from_array(kb)) && (now.wrapping_sub(e.stamp_us) < 250);
        if !same { *e = MiniEnt { key: kb, stamp_us: now }; }
        same
    })
}
// -----------------------------------------------------------------------------

// --------------------- [PATCH 111A: mix_pubkey128()] --------------------------
#[inline(always)]
fn mix_pubkey128(pk: &Pubkey, salt: u64) -> (u64, u64) {
    let b = pk.to_bytes();
    #[cfg(target_arch = "x86_64")]
    {
        if core::is_x86_feature_detected!("aes") {
            unsafe {
                use core::arch::x86_64::*;
                let a = _mm_loadu_si128(b.as_ptr() as *const __m128i);
                let c = _mm_loadu_si128(b.as_ptr().add(16) as *const __m128i);
                let k1 = _mm_set1_epi64x(salt as i64);
                let k2 = _mm_set1_epi64x((salt.rotate_left(17)) as i64);
                let mut x = _mm_xor_si128(a, k1);
                let mut y = _mm_xor_si128(c, k2);
                x = _mm_aesenc_si128(x, y);
                y = _mm_aesenc_si128(y, x);
                let m = _mm_xor_si128(x, y);
                let lo = _mm_cvtsi128_si64(m) as u64;
                let hi = _mm_extract_epi64(m, 1) as u64;
                return (lo, hi);
            }
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("aes") {
            unsafe {
                use core::arch::aarch64::*;
                let a = vld1q_u8(b.as_ptr());
                let c = vld1q_u8(b.as_ptr().add(16));
                let ks1 = vreinterpretq_u8_u64(vdupq_n_u64(salt));
                let ks2 = vreinterpretq_u8_u64(vdupq_n_u64(salt.rotate_left(17)));
                let mut x = veorq_u8(a, ks1);
                let mut y = veorq_u8(c, ks2);
                x = vaeseq_u8(x, y); x = vaesmcq_u8(x);
                y = vaeseq_u8(y, x); y = vaesmcq_u8(y);
                let m = veorq_u8(x, y);
                let lo = vgetq_lane_u64(vreinterpretq_u64_u8(m), 0);
                let hi = vgetq_lane_u64(vreinterpretq_u64_u8(m), 1);
                return (lo, hi);
            }
        }
    }
    // integer fallback
    let mut x = u64::from_le_bytes(b[0..8].try_into().unwrap()) ^ salt;
    let mut y = u64::from_le_bytes(b[8..16].try_into().unwrap()).rotate_left(13);
    let mut z = u64::from_le_bytes(b[16..24].try_into().unwrap()).rotate_left(31);
    let mut w = u64::from_le_bytes(b[24..32].try_into().unwrap()) ^ salt.rotate_left(7);
    #[inline(always)]
    fn mix(a: &mut u64, b: &mut u64, k: u64) {
        *a = a.wrapping_add(*b ^ k).rotate_left(24) ^ 0x9E37_79B9_7F4A_7C15;
        *b = b.wrapping_add(*a ^ (k.rotate_left(17))).rotate_left(37) ^ 0xC2B2_AE3D_27D4_EB4F;
    }
    mix(&mut x, &mut y, 0xD1B5_4A32_9FCE_DEAD);
    mix(&mut z, &mut w, 0xA24B_1F6D_BEE5_BEEF);
    mix(&mut x, &mut z, 0x9E37_79B9_7F4A_7C15);
    (x ^ z, y ^ w)
}
// -----------------------------------------------------------------------------
// --------- [PATCH 81: TLS negative hotline (8 ways, better hash)] -------------
#[derive(Copy, Clone)]
struct NegEnt { key: [u8;32], epoch: u64, valid: u8 }
impl Default for NegEnt { fn default() -> Self { Self { key:[0;32], epoch:0, valid:0 } } }

thread_local! {
    static TLS_NEG: std::cell::RefCell<[NegEnt; 8]> =
        std::cell::RefCell::new([NegEnt::default(); 8]);
}

#[inline]
fn neg_slot(pk: &Pubkey) -> usize {
    let b = pk.to_bytes();
    let mut x = u32::from_le_bytes([b[1], b[7], b[18], b[29]]);
    x = x.rotate_left(9) ^ 0x9E37_79B9 ^ (b[3] as u32);
    (x as usize) & 7
}

// --------------------- [PATCH 106A: pk_eq_fast()] -----------------------------
#[inline(always)]
fn pk_eq_fast(a32: &[u8;32], b: &Pubkey) -> bool {
    let bb = b.to_bytes();
    #[cfg(target_arch = "x86_64")]
    {
        if core::is_x86_feature_detected!("avx2") {
            unsafe {
                use core::arch::x86_64::{__m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8};
                let va = _mm256_loadu_si256(a32.as_ptr() as *const __m256i);
                let vb = _mm256_loadu_si256(bb.as_ptr() as *const __m256i);
                return _mm256_movemask_epi8(_mm256_cmpeq_epi8(va, vb)) == -1;
            }
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        unsafe {
            use core::arch::aarch64::{uint8x16_t, vceqq_u8, vld1q_u8, vminvq_u8};
            let a0: uint8x16_t = vld1q_u8(a32.as_ptr());
            let a1: uint8x16_t = vld1q_u8(a32.as_ptr().add(16));
            let b0: uint8x16_t = vld1q_u8(bb.as_ptr());
            let b1: uint8x16_t = vld1q_u8(bb.as_ptr().add(16));
            if vminvq_u8(vceqq_u8(a0, b0)) != 255 { return false; }
            if vminvq_u8(vceqq_u8(a1, b1)) != 255 { return false; }
            return true;
        }
    }
    // Portable fallback
    *a32 == bb
}
// -----------------------------------------------------------------------------

impl MemoryMappedStateCache {
    #[inline]
    fn neg_probe(&self, pk: &Pubkey) -> bool {
        let ep = self.hotline_epoch.load(Ordering::Acquire);
        TLS_NEG.with(|cell| {
            let arr = cell.borrow();
            let i = neg_slot(pk);
            let e = arr[i];
            e.valid == 1 && e.epoch == ep && pk_eq_fast(&e.key, pk)
        })
    }
    #[inline]
    fn neg_store(&self, pk: &Pubkey) {
        let ep = self.hotline_epoch.load(Ordering::Relaxed);
        TLS_NEG.with(|cell| {
            let mut arr = cell.borrow_mut();
            let i = neg_slot(pk);
            arr[i] = NegEnt { key: pk.to_bytes(), epoch: ep, valid: 1 };
        });
    }
}
// ------------------------------------------------------------------------------

impl MemoryMappedStateCache {
    // [PATCH 41] alloc from small bins helper
    #[inline]
    fn try_alloc_from_bins(&self, seg_idx: usize, need: u64) -> Option<u64> {
        let seg = &self.segments[seg_idx];
        if let Some(mut bi) = bin_index(need) {
            loop {
                for i in bi..BIN_N {
                    let mut bin = seg.small_bins[i].lock();
                    if let Some((off, len)) = bin.pop() {
                        if len > need {
                            let rem_off = off + need;
                            let rem_len = len - need;
                            if let Some(ri) = bin_index(rem_len) {
                                seg.small_bins[ri].lock().push((rem_off, rem_len));
                            } else {
                                let mut fl = seg.free_map.lock();
                                fl.insert(rem_off, rem_len);
                            }
                        }
                        seg.used_bytes.fetch_add(need, Ordering::Relaxed);
                        return Some(off);
                    }
                }
                break;
            }
        }
        None
    }

    #[inline]
    fn promo_enqueue(&self, ce: &CacheEntry) {
        TLS_PROMO.with(|cell| {
            let now = mono_micros();
            let mut tp = cell.borrow_mut();
            tp.buf.push(*ce);
            if tp.last_us == 0 { tp.last_us = now; }
            if tp.len >= 32 || now.wrapping_sub(tp.last_us) > 1000 {
                let nowf = mono_micros_fast();
                for e in tp.buf.iter() {
                    if promo_dedupe_tls(&Pubkey::new_from_array(e.pubkey), nowf) { continue; }
                    self.hot_shard_promote(e);
                    self.hot_global_promote(e);
                }
                tp.buf.clear();
                tp.last_us = nowf;
            }
        });
    }
}
// ---------------- [/PATCH 42] ---------------------------------------------------
    // ---------------- [PATCH 29: adaptive promotion threshold] -----------------
    #[inline]
    fn maybe_adapt_promote_threshold(&self) {
        let hits = self.hit_count.load(Ordering::Relaxed);
        let misses = self.miss_count.load(Ordering::Relaxed);
        let total = hits + misses;
        if total < 1000 { return; }
        let hr = (hits as f64) / (total as f64);
        let mut t = if hr > 0.95 { 16 } else if hr > 0.90 { 12 } else if hr > 0.80 { 8 } else { 6 };
        if t < 4 { t = 4; }
        if t > 64 { t = 64; }
        self.promote_threshold.store(t as u32, Ordering::Relaxed);
    }
    // -------------- [/PATCH 29] ------------------------------------------------

// --------------- [PATCH 34: NTA prefetch for large reads] ----------------------
#[inline(always)]
fn prefetch_account_region(base_ptr: *const u8, total: usize) {
    cpu_prefetch(base_ptr);
    cpu_prefetch(unsafe { base_ptr.add(64) });
    if total >= 256 {
        cpu_prefetch(unsafe { base_ptr.add(128) });
        cpu_prefetch(unsafe { base_ptr.add(192) });
    }
    if total >= 1024 {
        // non-temporal prefetch avoids polluting L3 on large payloads
        cpu_prefetch_nta(unsafe { base_ptr.add(256) });
        cpu_prefetch_nta(unsafe { base_ptr.add(512) });
        cpu_prefetch_nta(unsafe { base_ptr.add(768) });
    }
}
// ------------------------------------------------------------------------------

// Backward-compat wrapper for call sites using the underscore variant
#[inline]
fn build_account_small_or_vec(ad: &AccountData, payload: &[u8]) -> AccountSharedData {
    build_accountsmall_or_vec(ad, payload)
}

    // ---------------- [PATCH 21: shard hotlines helpers] -----------------------
    #[inline]
    fn hot_shard_probe(&self, pk: &Pubkey) -> Option<CacheEntry> {
        let shard = current_cpu() % self.numa_shards.max(1);
        let set = shard_set_idx(pk);
        let base = set * SH_HOT_WAYS;
        let g = epoch::pin();
        for w in 0..SH_HOT_WAYS {
            let sh = self.hot_shards[shard][base + w].load(Ordering::Acquire, &g);
            let ce = unsafe { sh.as_ref()? };
            if ce.pubkey == pk.to_bytes() && ce.size > 0 { return Some(*ce); }
        }
        None
    }
    #[inline]
    fn hot_shard_promote(&self, ce: &CacheEntry) {
        let shard = current_cpu() % self.numa_shards.max(1);
        let set = shard_set_idx(&Pubkey::new_from_array(ce.pubkey));
        let base = set * SH_HOT_WAYS;
        let g = epoch::pin();
        let mut victim = base;
        let mut oldest = u64::MAX;
        for w in 0..SH_HOT_WAYS {
            let sh = self.hot_shards[shard][base + w].load(Ordering::Acquire, &g);
            if let Some(cur) = unsafe { sh.as_ref() } {
                if cur.size == 0 { victim = base + w; oldest = 0; break; }
                if cur.last_access_us < oldest { oldest = cur.last_access_us; victim = base + w; }
            } else { victim = base + w; oldest = 0; break; }
        }
        let _ = self.hot_shards[shard][victim].swap(Owned::new(*ce), Ordering::Release, &g);
    }
    // -------------- [/PATCH 21] ------------------------------------------------

    // ------------- [PATCH 32: 2-way set-associative global hotline] ----------------
    #[inline]
    fn hot_set_idx(&self, pk: &Pubkey) -> usize {
        // [PATCH 111B.1] AES-mixed + BMI2 PEXT lane extractor
        #[cfg(target_arch = "x86_64")]
        {
            use core::arch::x86_64::_pext_u64;
            let (lo, hi) = mix_pubkey128(pk, hotline_salt() as u64);
            if core::is_x86_feature_detected!("bmi2") {
                const MASK_HOT: u64 = 0x9249_2492_4924_9249;
                let y = unsafe { _pext_u64(lo ^ hi.rotate_left(9), MASK_HOT) } as usize;
                return y & (HOT_SETS - 1);
            }
            let y = (lo ^ hi ^ (hi >> 7) ^ (lo.rotate_left(13))) as usize;
            return y & (HOT_SETS - 1);
        }
        let (lo, hi) = mix_pubkey128(pk, hotline_salt() as u64);
        ((lo ^ hi ^ (hi >> 5)) as usize) & (HOT_SETS - 1)
    }

    #[inline]
    fn hot_global_promote(&self, ce: &CacheEntry) {
        let set = self.hot_set_idx(&Pubkey::new_from_array(ce.pubkey));
        let g = epoch::pin();
        let base = set * HOT_WAYS;
        let mut victim = base;
        let mut oldest = u64::MAX;
        for w in 0..HOT_WAYS {
            let sh = self.hot_global[base + w].load(Ordering::Acquire, &g);
            if let Some(cur) = unsafe { sh.as_ref() } {
                if cur.size == 0 { victim = base + w; oldest = 0; break; }
                if cur.last_access_us < oldest { oldest = cur.last_access_us; victim = base + w; }
            } else { victim = base + w; oldest = 0; break; }
        }
        let _ = self.hot_global[victim].swap(Owned::new(*ce), Ordering::Release, &g);
    }

    #[inline]
    fn hot_global_probe(&self, pk: &Pubkey) -> Option<CacheEntry> {
        let set = self.hot_set_idx(pk);
        let g = epoch::pin();
        let base = set * HOT_WAYS;
        for w in 0..HOT_WAYS {
            let sh = self.hot_global[base + w].load(Ordering::Acquire, &g);
            let ce = unsafe { sh.as_ref() }?;
            if ce.pubkey == pk.to_bytes() && ce.size > 0 { return Some(*ce); }
        }
        None
    }
    // ------------------------------------------------------------------------------

    // ---------------- [PATCH 112A: NEW dk_lane_index3()] --------------------------
    #[inline(always)]
    fn dk_lane_index3(pk: &Pubkey, seed: u64, words: usize) -> [(usize, u64); 3] {
        debug_assert!(words > 0);
        let (lo, hi) = mix_pubkey128(pk, seed);
        #[cfg(target_arch = "x86_64")]
        {
            if core::is_x86_feature_detected!("bmi2") {
                use core::arch::x86_64::_pext_u64;
                let (m0, m1, m2) = pick_mask_triplet(salt_per_socket(seed));
                let i0 = unsafe { _pext_u64(lo, m0) } as usize;
                let i1 = unsafe { _pext_u64(hi, m1) } as usize;
                let i2 = unsafe { _pext_u64(lo ^ hi.rotate_left(7), m2) } as usize;
                let b0 = i0 % (words * 64);
                let b1 = i1 % (words * 64);
                let b2 = i2 % (words * 64);
                return [
                    (b0 >> 6, 1u64 << (b0 & 63)),
                    (b1 >> 6, 1u64 << (b1 & 63)),
                    (b2 >> 6, 1u64 << (b2 & 63)),
                ];
            }
        }
        let z0 = (lo ^ (hi >> 7)).wrapping_mul(0x9E37_79B9_7F4A_7C15) as usize % (words * 64);
        let z1 = (hi ^ (lo >> 9)).wrapping_mul(0xC2B2_AE3D_27D4_EB4F) as usize % (words * 64);
        let z2 = (lo ^ hi.rotate_left(13)).wrapping_mul(0x1656_67B1_ECDB_975B) as usize % (words * 64);
        [
            (z0 >> 6, 1u64 << (z0 & 63)),
            (z1 >> 6, 1u64 << (z1 & 63)),
            (z2 >> 6, 1u64 << (z2 & 63)),
        ]
    }
    // ------------------------------------------------------------------------------

    // ---------------- [PATCH 23: async msync range] ----------------------------
    const FLUSH_ASYNC_THRESH: usize = 1 * 1024 * 1024; // 1 MiB
    #[cfg(target_os = "linux")]
    #[inline]
    fn flush_async_range(&self, seg_idx: usize, seg_off: usize, len: usize) {
        unsafe {
            let mg = self.segments[seg_idx].mmap.read();
            let p = mg.as_ptr().add(seg_off) as *mut c_void;
            let _ = libc::msync(p, len, libc::MS_ASYNC);
        }
    }
    // -------------- [/PATCH 23] ------------------------------------------------
    // ---------------- [UPGRADE EDGE: publish helpers honor rev] -----------------
    #[inline]
    fn next_rev_for(&self, pk: &Pubkey, g: &epoch::Guard) -> u32 {
        if let Some(ea) = self.index.get(pk) {
            if let Some(er) = Self::load_entry(&*ea, g) { return er.rev.wrapping_add(1); }
        }

    // ---------------- [PATCH 22: slot hotline epoch bump] -----------------------
    /// Call at the start of each new slot to invalidate TLS hotlines.
    pub fn slot_window_begin(&self, slot: Slot) {
        self.slot_window.slot.store(slot, Ordering::Relaxed);
        self.hotline_epoch.fetch_add(1, Ordering::Release);
        self.slot_window.epoch.fetch_add(1, Ordering::Release);
    }
    // ---------------- [/PATCH 22] -----------------------------------------------
        1
    }
    #[inline]
    fn publish_fresh(&self, pk: &Pubkey, mut ce: CacheEntry) {
        ce.rev = 1;
        self.publish_entry(pk, ce, true);
    }
    // -------------- [/UPGRADE EDGE: publish helpers honor rev] ------------------

    // ---------------- [UPGRADE EDGE: in-place upsert bumps rev] -----------------
    fn upsert_in_place(&self, pubkey: &Pubkey, account: &AccountSharedData, slot: Slot) -> Option<()> {
        // ---- [PATCH 109A: two-phase odd/even publish] ---------
        let g = epoch::pin();
        let ea = self.index.get(pubkey)?;
        let er = Self::load_entry(&*ea, &g)?;
        let seg_idx = (er.offset / SEGMENT_SIZE as u64) as usize;
        let seg_off = (er.offset % SEGMENT_SIZE as u64) as usize;
        if seg_idx >= self.segments.len() { return None; }

        let header_new = encode_header_from_account(account);
        let payload    = account.data();
        let total_new  = core::mem::size_of::<AccountData>() + payload.len();
        if total_new as u64 != er.size { return None; }

        // Phase 0: publish odd rev to signal in-progress
        let mut mid = *er; mid.rev = er.rev | 1;
        let _ = ea.swap(Owned::new(mid), Ordering::AcqRel, &g);

        // [PATCH 121A] Writer coalescer: compare header+payload under read lock
        let seg = &self.segments[seg_idx];
        {
            let mg = seg.mmap.read();
            if seg_off + total_new > mg.len() { return None; }
            let base = unsafe { mg.as_ptr().add(seg_off) };
            let hdr = bytemuck::bytes_of(&header_new);
            let same_hdr = unsafe { equal_header_fast(hdr.as_ptr(), base, hdr.len()) };
            let same_pay = unsafe { Self::equal_large_masked(payload.as_ptr(), base.add(hdr.len()), payload.len()) };
            if same_hdr && same_pay {
                // No data change: publish even rev with refreshed meta only
                let mut fin = *er;
                fin.slot = slot as u64;
                fin.last_access_us = mono_micros_fast();
                fin.rev = er.rev.wrapping_add(2) & !1u64;
                // checksum unchanged
                let _ = ea.swap(Owned::new(fin), Ordering::AcqRel, &g);
                publish_after_write_fence();
                return Some(());
            }
        }

        // Perform write (PATCH 114C fused copy+CRC combine)
        let mut mgw = seg.mmap.write();
        if seg_off + total_new > mgw.len() { return None; }
        let crc_full: u32 = unsafe {
            let base = mgw.as_mut_ptr().add(seg_off);
            let hdr  = bytemuck::bytes_of(&header_new);
            core::ptr::copy_nonoverlapping(hdr.as_ptr(), base, hdr.len());
            let crc_payload_final = copy_and_crc_optimized(base.add(hdr.len()), payload.as_ptr(), payload.len());
            persist_range_if_enabled(base, total_new);
            demote_written_region(base, total_new);
            let running = !crc_payload_final;
            let extended = crc32c_extend_state(running, hdr.as_ptr(), hdr.len());
            crc32c_finalize(extended)
        };

        // Phase 1: publish even rev with final checksum
        let mut fin = *er;
        fin.slot = slot as u64;
        fin.last_access_us = mono_micros_fast();
        fin.checksum = crc_full;
        fin.rev = er.rev.wrapping_add(2) & !1u64;
        let _ = ea.swap(Owned::new(fin), Ordering::AcqRel, &g);
        publish_after_write_fence();
        Some(())
    }
    // -------------- [/UPGRADE EDGE: in-place upsert bumps rev] ------------------
    
    // -------- [PATCH 78b: arch-optimized fast_equal_simd()] ---------------------
    #[inline(always)]
    unsafe fn fast_equal_simd(a: *const u8, b: *const u8, len: usize) -> bool {
        #[cfg(target_arch = "x86_64")]
        {
            if core::is_x86_feature_detected!("avx512bw") && core::is_x86_feature_detected!("avx512f") {
                use core::arch::x86_64::{__m512i, _mm512_cmpeq_epi8_mask, _mm512_loadu_si512};
                let mut i = 0usize;
                while i + 64 <= len {
                    let va: __m512i = _mm512_loadu_si512(a.add(i) as *const __m512i);
                    let vb: __m512i = _mm512_loadu_si512(b.add(i) as *const __m512i);
                    let m = _mm512_cmpeq_epi8_mask(va, vb);
                    if m != !0u64 { return false; }
                    i += 64;
                }
                while i < len { if *a.add(i) != *b.add(i) { return false; } i += 1; }
                return true;
            }
            use core::arch::x86_64::{__m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8};
            let mut i = 0usize;
            while i + 32 <= len {
                let va = _mm256_loadu_si256(a.add(i) as *const __m256i);
                let vb = _mm256_loadu_si256(b.add(i) as *const __m256i);
                if _mm256_movemask_epi8(_mm256_cmpeq_epi8(va, vb)) != -1 { return false; }
                i += 32;
            }
            while i < len { if *a.add(i) != *b.add(i) { return false; } i += 1; }
            return true;
        }
        #[cfg(target_arch = "aarch64")]
        {
            use core::arch::aarch64::{uint8x16_t, vceqq_u8, vld1q_u8, vminvq_u8};
            let mut i = 0usize;
            while i + 16 <= len {
                let va: uint8x16_t = vld1q_u8(a.add(i));
                let vb: uint8x16_t = vld1q_u8(b.add(i));
                if unsafe { vminvq_u8(vceqq_u8(va, vb)) } != 255 { return false; }
                i += 16;
            }
            while i < len { if *a.add(i) != *b.add(i) { return false; } i += 1; }
            return true;
        }
        let mut i = 0usize;
        while i < len { if *a.add(i) != *b.add(i) { return false; } i += 1; }
        true
    }
    // ---------------------------------------------------------------------------
// -------------------------- [UPGRADE: imports & constants] --------------------------
use anyhow::{Context, Result};
use bytemuck::{Pod, Zeroable};
use core::hash::{BuildHasher, Hasher};
use crc32fast::Hasher as Crc32Fast;
use crossbeam::epoch::{self, Atomic as EpochAtomic, Guard, Owned, Shared};
use dashmap::DashMap;
use memmap2::{MmapMut, MmapOptions};
use parking_lot::{Mutex, RwLock};
use ahash::RandomState as AHashState;

#[cfg(target_os = "linux")]
use libc::{
    c_void, madvise, mlock, posix_fallocate, MADV_DONTDUMP, MADV_HUGEPAGE, MADV_RANDOM, MADV_WILLNEED,
    sched_setaffinity, CPU_SET, CPU_ZERO, cpu_set_t
};
#[cfg(target_os = "linux")]
use std::os::unix::io::AsRawFd;

use solana_sdk::{
    account::{Account, AccountSharedData},
    clock::Slot,
    pubkey::Pubkey
};

use std::{
    cmp::min,
    collections::{BTreeMap, HashMap},
    fs::{File, OpenOptions},
    io::Write,
    mem::{align_of, size_of},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering},
        Arc, OnceLock,
    },
    time::{Duration, Instant},
};
// -------------------------- [UPGRADE EDGE: extra imports] --------------------------
use std::thread;
// ------------------------ [/UPGRADE EDGE: extra imports] --------------------------

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
use core::arch;
use core::mem::MaybeUninit;
use std::sync::OnceLock;

const CACHE_VERSION: u32 = 0x4D454D43; // 'MEMC'
const PAGE_SIZE: usize = 4096;
const MAX_CACHE_SIZE: usize = 32 * 1024 * 1024 * 1024; // 32 GiB
const SEGMENT_SIZE: usize = 256 * 1024 * 1024;         // 256 MiB
const INDEX_BUCKET_COUNT: usize = 65536;
const EVICTION_BATCH_SIZE: usize = 1024;
const SLOT_HISTORY_SIZE: usize = 512;
const CACHE_ALIGN: usize = 64;
// ------------- [PATCH 32: global hotline 2-way set constants] --------------
const HOT_WAYS: usize = 2;  // 2-way set
const HOT_SETS: usize = 64; // 64 sets
// ---------------------------------------------------------------------------
// -------- [PATCH 49: 2-way set-assoc for shard hotlines constants] ----------
const SH_HOT_WAYS: usize = 2;  // 2-way per shard
const SH_HOT_SETS: usize = 64; // 64 sets per shard
#[inline]
fn shard_set_idx(pk: &Pubkey) -> usize { (pk.to_bytes()[7] as usize) & (SH_HOT_SETS - 1) }
// ---------------------------------------------------------------------------
// -------------------------- [UPGRADE+++: read CRC sampling] --------------------------
const READ_CRC_RATE_SHIFT: u32 = 8; // verify ~1/256 reads
// ------------------------ [/UPGRADE+++: read CRC sampling] --------------------------
// --------------------- [UPGRADE EDGE: relay hotline constants] -------------------
const RELAY_MAX: usize  = 8;    // up to 8 relays
const RELAY_SLOTS: usize = 32;  // 32 direct-mapped slots per relay
// ------------------- [/UPGRADE EDGE: relay hotline constants] --------------------
// -------------------- [UPGRADE EDGE: slot-window constants] ---------------------
const SW_SLOTS: usize = 1024; // 1k direct-mapped entries per slot window
// ------------------ [/UPGRADE EDGE: slot-window constants] ----------------------
// ---------------------- [UPGRADE EDGE: NT copy threshold] -------------------------
const NT_COPY_THRESH: usize = 4 * 1024; // non-temporal store for payloads >= 4 KiB (legacy; dynamic below)
// -------------------- [/UPGRADE EDGE: NT copy threshold] -------------------------
// -------------- [UPGRADE EDGE: runtime NT dispatch + live threshold] -------------
static NT_COPY_THRESH_LIVE: AtomicUsize = AtomicUsize::new(4 * 1024);
// non-temporal store for payloads >= 4 KiB
// -------------- [/UPGRADE EDGE: runtime NT dispatch + live threshold] -------------
// ---------------- [PATCH 36: dynamic NT threshold] -----------------------------
use std::sync::OnceLock as _OnceLockForNt;

#[inline]
fn nt_copy_threshold() -> usize {
    use std::sync::OnceLock;
    static THR: OnceLock<usize> = OnceLock::new();
    *THR.get_or_init(|| {
        if let Ok(v) = std::env::var("SC_NT_THR") { if let Ok(x) = v.parse::<usize>() { return x.max(256); } }
        let cl = cache_line_len();
        #[cfg(target_arch = "x86_64")]
        {
            if core::is_x86_feature_detected!("avx512f") { return (cl * 48).max(3072); }
            if core::is_x86_feature_detected!("avx2")     { return (cl * 64).max(4096); }
            return (cl * 80).max(5120);
        }
        #[cfg(target_arch = "aarch64")]
        {
            if std::arch::is_aarch64_feature_detected!("neon") { return (cl * 64).max(4096); }
            return (cl * 96).max(6144);
        }
        #[allow(unreachable_code)]
        { 4096 }
    })
}
// ---------------- [/PATCH 36] ---------------------------------------------------

// ---------------- [PATCH 51: persist range (CLWB/CLFLUSHOPT)] -------------------
#[inline]
fn cache_line_len() -> usize {
    static L: OnceLock<usize> = OnceLock::new();
    *L.get_or_init(|| {
        #[cfg(target_arch = "x86_64")]
        {
            unsafe {
                let r = core::arch::x86_64::__cpuid(1);
                let sz = ((r.ebx >> 8) & 0xFF) as usize * 8;
                if sz != 0 { return sz; }
            }
        }
        64
    })
}

// [PATCH 57] DAX autodetect (env override + optional autodetect)
#[inline]
fn persist_enabled() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        if let Ok(v) = std::env::var("SC_PERSIST") {
            return v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("on");
        }
        if std::env::var("SC_PERSIST_AUTO").ok().as_deref() != Some("1") { return false; }
        #[cfg(target_os = "linux")]
        {
            if let Ok(m) = std::fs::read_to_string("/proc/mounts") {
                for line in m.lines() {
                    if line.contains(" dax,") || line.ends_with(" dax") || line.contains(",dax,") || line.contains(",dax ") {
                        return true;
                    }
                }
            }
        }
        false
    })
}

// [PATCH 56B] Dispatched persist flush implementation
type FlushFn = unsafe fn(*const u8, usize);

#[inline(always)]
fn select_flush_impl() -> FlushFn {
    #[cfg(target_arch = "x86_64")]
    {
        if core::is_x86_feature_detected!("clwb")       { return flush_impl_clwb; }
        if core::is_x86_feature_detected!("clflushopt") { return flush_impl_clflushopt; }
        return flush_impl_noop;
    }
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    {
        if std::env::var("SC_ARM_DC_CVAP").ok().as_deref() == Some("1") {
            return flush_impl_dc_cvap;
        }
        return flush_impl_msync;
    }
    #[allow(unreachable_code)]
    { flush_impl_noop }
}

#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
#[inline(always)]
unsafe fn flush_impl_msync(p: *const u8, len: usize) {
    msync_pagealigned(p, len);
}

// ------------- [PATCH 95B: NEW flush_impl_dc_cvap() on AArch64] ---------------
#[cfg(all(target_os = "linux", target_arch = "aarch64"))]
#[inline(always)]
unsafe fn flush_impl_dc_cvap(p: *const u8, len: usize) {
    let cl = cache_line_len();
    let mut a = (p as usize) & !(cl - 1);
    let end = p.add(len) as usize;
    while a < end {
        core::arch::asm!("dc cvap, {0}", in(reg) (a as *const u8), options(nostack, preserves_flags));
        a += cl;
    }
    core::arch::asm!("dsb ish", "dmb ishst", options(nostack, preserves_flags));
}

#[inline(always)]
unsafe fn flush_impl_noop(_p: *const u8, _len: usize) {}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn flush_impl_clflushopt(p: *const u8, len: usize) {
    use core::arch::x86_64::{_mm_clflushopt, _mm_sfence};
    let cl = cache_line_len();
    let mut a = (p as usize) & !(cl - 1);
    let end = p.add(len) as usize;
    while a < end { _mm_clflushopt(a as *const u8); a += cl; }
    _mm_sfence();
}

// [PATCH 72B] AArch64 NEON copy implementation
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
#[inline(always)]
unsafe fn copy_impl_neon(dst: *mut u8, src: *const u8, len: usize) {
    use core::arch::aarch64::{uint8x16_t, vld1q_u8, vst1q_u8};
    let mut d = dst;
    let mut s = src;
    let mis64 = (d as usize) & 63;
    if mis64 != 0 {
        let head = core::cmp::min(64 - mis64, len);
        core::ptr::copy_nonoverlapping(s, d, head);
        d = d.add(head); s = s.add(head);
    }
    let n = len - ((d as usize).wrapping_sub(dst as usize));
    let chunks = n / 16; // 16B per vector
    let tail   = n % 16;
    let mut i = 0usize;
    while i + 3 < chunks {
        let q0: uint8x16_t = vld1q_u8(s.add(i*16));
        let q1: uint8x16_t = vld1q_u8(s.add((i+1)*16));
        let q2: uint8x16_t = vld1q_u8(s.add((i+2)*16));
        let q3: uint8x16_t = vld1q_u8(s.add((i+3)*16));
        vst1q_u8(d.add(i*16),       q0);
        vst1q_u8(d.add((i+1)*16),   q1);
        vst1q_u8(d.add((i+2)*16),   q2);
        vst1q_u8(d.add((i+3)*16),   q3);
        i += 4;
    }
    while i < chunks {
        let q: uint8x16_t = vld1q_u8(s.add(i*16));
        vst1q_u8(d.add(i*16), q);
        i += 1;
    }
    if tail != 0 {
        core::ptr::copy_nonoverlapping(s.add(chunks*16), d.add(chunks*16), tail);
    }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn flush_impl_clwb(p: *const u8, len: usize) {
    use core::arch::x86_64::{_mm_clwb, _mm_sfence};
    let cl = cache_line_len();
    let mut a = (p as usize) & !(cl - 1);
    let end = p.add(len) as usize;
    while a < end { _mm_clwb(a as *const u8); a += cl; }
    _mm_sfence();
}

#[inline(always)]
unsafe fn persist_range_if_enabled(p: *const u8, len: usize) {

// -------------------------- [UPGRADE++: doorkeeper constants] --------------------------
const DK_BITS: usize  = 1 << 24; // 16,777,216 bits (~2MiB)
const DK_MASK: usize  = DK_BITS - 1;
const DK_WORDS: usize = DK_BITS / 64;
// ------------------------ [/UPGRADE++: doorkeeper constants] --------------------------

// Align helper and monotonic time base
#[inline]
fn align_up(off: usize, align: usize) -> usize { (off + align - 1) & !(align - 1) }

// ---------------------- [UPGRADE EDGE: typed peek POD] --------------------------
impl MemoryMappedStateCache {
    pub fn peek_as<T: bytemuck::Pod>(&self, pubkey: &Pubkey) -> Option<T> {
        let g = epoch::pin();
        let ea = self.index.get(pubkey)?;
        let er = Self::load_entry(&*ea, &g)?;
        std::sync::atomic::fence(Ordering::Acquire);
        let seg_idx = (er.offset / SEGMENT_SIZE as u64) as usize;
        let seg_off = (er.offset % SEGMENT_SIZE as u64) as usize;
        if seg_idx >= self.segments.len() { return None; }
        let guard = self.segments[seg_idx].mmap.read();
        let ad_sz = size_of::<AccountData>();
        if seg_off + (er.size as usize) > guard.len() || (er.size as usize) < ad_sz { return None; }
        let ad: &AccountData = bytemuck::from_bytes(&guard[seg_off..seg_off + ad_sz]);
        if ad.data_len as usize != core::mem::size_of::<T>() { return None; }
        let data_off = seg_off + ad_sz;
        let data_end = data_off + core::mem::size_of::<T>();
        if data_end > guard.len() { return None; }
        let t: &T = bytemuck::from_bytes(&guard[data_off..data_end]);
        Some(*t)
    }
}
// -------------------- [/UPGRADE EDGE: typed peek POD] ---------------------------

#[inline]
fn mono_micros() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let start = EPOCH.get_or_init(Instant::now);
    start.elapsed().as_micros() as u64
}

// --------------------- [PATCH 77: REPLACE compute_checksum] --------------------
use std::sync::OnceLock as _OnceLockCrc;

type CrcFn = fn(&[u8]) -> u32;

#[inline(always)]
fn select_crc_impl() -> CrcFn {
    #[cfg(target_arch = "x86_64")]
    {
        if core::is_x86_feature_detected!("sse4.2") { return crc32c_x86_sse42; }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("crc") { return crc32c_a64; }
    }
    crc32c_sw
}

#[inline(always)]
fn compute_checksum(buf: &[u8]) -> u32 {
    static IMPL: _OnceLockCrc<CrcFn> = _OnceLockCrc::new();
    let f = *IMPL.get_or_init(select_crc_impl);
    f(buf)
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn crc32c_x86_sse42(mut buf: &[u8]) -> u32 {
    use core::arch::x86_64::{_mm_crc32_u8, _mm_crc32_u32, _mm_crc32_u64};
    let mut crc: u32 = !0;
    while buf.len() >= 8 {
        let mut w = [0u8; 8]; w.copy_from_slice(&buf[..8]);
        let x = u64::from_le_bytes(w);
        crc = unsafe { _mm_crc32_u64(crc as u64, x) as u32 };
        buf = &buf[8..];
    }
    if buf.len() >= 4 {
        let mut w = [0u8; 4]; w.copy_from_slice(&buf[..4]);
        let x = u32::from_le_bytes(w);
        crc = unsafe { _mm_crc32_u32(crc, x) };
        buf = &buf[4..];
    }
    for &b in buf { crc = unsafe { _mm_crc32_u8(crc, b) }; }
    !crc
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "crc")]
#[inline(always)]
fn crc32c_a64(mut buf: &[u8]) -> u32 {
    use core::arch::aarch64::{__crc32cd, __crc32cw, __crc32cb};
    let mut crc: u32 = !0;
    while buf.len() >= 8 {
        let mut w = [0u8; 8]; w.copy_from_slice(&buf[..8]);
        let x = u64::from_le_bytes(w);
        crc = unsafe { __crc32cd(crc, x) };
        buf = &buf[8..];
    }
    if buf.len() >= 4 {
        let mut w = [0u8; 4]; w.copy_from_slice(&buf[..4]);
        let x = u32::from_le_bytes(w);
        crc = unsafe { __crc32cw(crc, x) };
        buf = &buf[4..];
    }
    for &b in buf { crc = unsafe { __crc32cb(crc, b) }; }
    !crc
}

#[inline(always)]
fn crc32c_sw(buf: &[u8]) -> u32 {
    const POLY: u32 = 0x1EDC6F41;
    let mut crc = !0u32;
    for &b in buf {
        let mut x = (crc ^ b as u32) & 0xFF;
        for _ in 0..8 { x = (x >> 1) ^ (POLY & ((x & 1).wrapping_neg())); }
        crc = (crc >> 8) ^ x;
    }
    !crc
}
// ------------------- [/PATCH 77] ---------------------------------------------

// --------------- [PATCH 114A: NEW crc32c_extend_state/finalize] ---------------
#[inline(always)]
fn crc32c_finalize(state: u32) -> u32 { !state }

#[inline(always)]
fn crc32c_extend_state(mut state: u32, mut p: *const u8, mut len: usize) -> u32 {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::{_mm_crc32_u64,_mm_crc32_u32,_mm_crc32_u8};
        while len >= 8 {
            let mut w = [0u8;8];
            w.copy_from_slice(core::slice::from_raw_parts(p, 8));
            state = _mm_crc32_u64(state as u64, u64::from_le_bytes(w)) as u32;
            len -= 8; p = p.add(8);
        }
        if len >= 4 {
            let mut w = [0u8;4];
            w.copy_from_slice(core::slice::from_raw_parts(p, 4));
            state = _mm_crc32_u32(state, u32::from_le_bytes(w));
            len -= 4; p = p.add(4);
        }
        while len != 0 { state = _mm_crc32_u8(state, *p); len -= 1; p = p.add(1); }
        return state;
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        use core::arch::aarch64::{__crc32cd,__crc32cw,__crc32cb};
        while len >= 8 {
            let mut w = [0u8;8];
            w.copy_from_slice(core::slice::from_raw_parts(p, 8));
            state = __crc32cd(state, u64::from_le_bytes(w));
            len -= 8; p = p.add(8);
        }
        if len >= 4 {
            let mut w = [0u8;4];
            w.copy_from_slice(core::slice::from_raw_parts(p, 4));
            state = __crc32cw(state, u32::from_le_bytes(w));
            len -= 4; p = p.add(4);
        }
        while len != 0 { state = __crc32cb(state, *p); len -= 1; p = p.add(1); }
        return state;
    }
    // Portable fallback
    let mut crc = state;
    const POLY: u32 = 0x1EDC6F41;
    let mut i = 0usize;
    while i < len {
        let b = unsafe { *p.add(i) };
        let mut x = (crc ^ b as u32) & 0xFF;
        for _ in 0..8 { x = (x >> 1) ^ (POLY & ((x & 1).wrapping_neg())); }
        crc = (crc >> 8) ^ x;
        i += 1;
    }
    crc
}
// ------------------------------------------------------------------------------

// [PATCH 114A+] fused payload copy with CRC (returns finalized CRC of payload)
#[inline(always)]
unsafe fn copy_and_crc_optimized(mut dst: *mut u8, mut src: *const u8, mut len: usize) -> u32 {
    #[cfg(target_arch = "x86_64")]
    {
        if core::is_x86_feature_detected!("sse4.2") {
            use core::arch::x86_64::{_mm_crc32_u64,_mm_crc32_u32,_mm_crc32_u8};
            let mut crc: u32 = !0u32;
            while len >= 8 {
                let mut w = [0u8;8];
                w.copy_from_slice(core::slice::from_raw_parts(src, 8));
                std::ptr::copy_nonoverlapping(src, dst, 8);
                crc = _mm_crc32_u64(crc as u64, u64::from_le_bytes(w)) as u32;
                src = src.add(8); dst = dst.add(8); len -= 8;
            }
            if len >= 4 {
                let mut w = [0u8;4];
                w.copy_from_slice(core::slice::from_raw_parts(src, 4));
                std::ptr::copy_nonoverlapping(src, dst, 4);
                crc = _mm_crc32_u32(crc, u32::from_le_bytes(w));
                src = src.add(4); dst = dst.add(4); len -= 4;
            }
            while len != 0 { std::ptr::copy_nonoverlapping(src, dst, 1); crc = _mm_crc32_u8(crc, *src); src = src.add(1); dst = dst.add(1); len -= 1; }
            return !crc;
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("crc") {
            use core::arch::aarch64::{__crc32cd,__crc32cw,__crc32cb};
            let mut crc: u32 = !0u32;
            while len >= 8 {
                let mut w = [0u8;8];
                w.copy_from_slice(core::slice::from_raw_parts(src, 8));
                std::ptr::copy_nonoverlapping(src, dst, 8);
                crc = __crc32cd(crc, u64::from_le_bytes(w));
                src = src.add(8); dst = dst.add(8); len -= 8;
            }
            if len >= 4 {
                let mut w = [0u8;4];
                w.copy_from_slice(core::slice::from_raw_parts(src, 4));
                std::ptr::copy_nonoverlapping(src, dst, 4);
                crc = __crc32cw(crc, u32::from_le_bytes(w));
                src = src.add(4); dst = dst.add(4); len -= 4;
            }
            while len != 0 { std::ptr::copy_nonoverlapping(src, dst, 1); crc = __crc32cb(crc, *src); src = src.add(1); dst = dst.add(1); len -= 1; }
            return !crc;
        }
    }
    // Portable fallback
    std::ptr::copy_nonoverlapping(src, dst, len);
    crc32c_sw(core::slice::from_raw_parts(src, len))
}


// ------------------ [PATCH 76A: cold helpers & publish fence] -----------------
#[inline(always)]
fn publish_after_write_fence() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::_mm_sfence;
        _mm_sfence();
        return;
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        core::arch::asm!("dmb ishst", options(nostack, preserves_flags));
        return;
    }
    core::sync::atomic::fence(core::sync::atomic::Ordering::Release);
}

#[cold]
#[inline(never)]
fn crc_fault_cold() { core::sync::atomic::compiler_fence(core::sync::atomic::Ordering::SeqCst); }

#[cold]
#[inline(never)]
fn oom_cold() -> anyhow::Error { anyhow::anyhow!("allocate_space: out of memory") }
// ------------------------------------------------------------------------------

// ---------------- [PATCH 118C: NEW hot_wait_short()] --------------------
#[inline(always)]
fn hot_wait_short() {
    #[cfg(target_arch = "x86_64")]
    {
        // tiny sleep; generic variant uses a short timeout only
        waitpkg_monitor(hot_wait_short as *const () as *const u8, 1_000);
        return;
    }
    #[cfg(target_arch = "aarch64")]
    unsafe { core::arch::asm!("yield", options(nostack, preserves_flags)); }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    core::hint::spin_loop();
}
// ------------------------------------------------------------------------

// --------------- [PATCH 46: NUMA shard detection] ------------------------------
#[inline]
fn detect_numa_shards() -> usize {
    #[cfg(target_os = "linux")]
    {
        use std::{fs, path::Path};
        if let Ok(s) = std::env::var("SC_NUMA_SHARDS") {
            if let Ok(n) = s.parse::<usize>() { return n.clamp(1, 64).next_power_of_two(); }
        }
        let nodes_dir = Path::new("/sys/devices/system/node/");
        if nodes_dir.is_dir() {
            let mut nodes = 0usize;
            if let Ok(rd) = fs::read_dir(nodes_dir) {
                for e in rd.flatten() {
                    let name = e.file_name();
                    if let Some(s) = name.to_str() {
                        if s.starts_with("node") && s[4..].chars().all(|c| c.is_ascii_digit()) { nodes += 1; }
                    }
                }
            }
            if nodes > 0 { return nodes.min(32).next_power_of_two(); }
        }
    }
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8)
        .next_power_of_two().min(64)
}
// ------------------------------------------------------------------------------

// --------------- [PATCH 31 helper: encode header from account] ---------------
#[inline]
fn encode_header_from_account(account: &AccountSharedData) -> AccountData {
    AccountData {
        lamports: account.lamports(),
        data_len: account.data().len() as u64,
        owner: account.owner().to_bytes(),
        executable: if account.executable() { 1 } else { 0 },
        rent_epoch: account.rent_epoch(),
        padding: [0u8; 7],
    }
}
// -----------------------------------------------------------------------------

// ------------------ [UPGRADE EDGE: small data inline copy] ----------------------
#[inline]
fn build_accountsmall_or_vec(ad: &AccountData, payload: &[u8]) -> AccountSharedData {
    const S: usize = 96;
    if payload.len() <= S {
        let mut buf = [0u8; S];
        let n = payload.len();
        unsafe { std::ptr::copy_nonoverlapping(payload.as_ptr(), buf.as_mut_ptr(), n); }
        AccountSharedData::new_data(ad.lamports, &buf[..n], &Pubkey::new_from_array(ad.owner)).unwrap()
    } else {
        let mut v = Vec::with_capacity(payload.len());
        v.extend_from_slice(payload);
        AccountSharedData::from(Account {
            lamports: ad.lamports,
            data: v,
            owner: Pubkey::new_from_array(ad.owner),
            executable: ad.executable != 0,
            rent_epoch: ad.rent_epoch,
        })
    }
}
// ---------------- [/UPGRADE EDGE: small data inline copy] -----------------------

// --------------------- [UPGRADE EDGE: cold fault isolate] -----------------------
#[cold]
fn handle_crc_fault(cache: &MemoryMappedStateCache, pubkey: &Pubkey) {
    let _ = cache.remove(pubkey);
    cache.crc_escalate_on_fault();
}
// ------------------- [/UPGRADE EDGE: cold fault isolate] ------------------------

// --------------------- [UPGRADE EDGE: CycleClock calib] -------------------------
#[cfg(target_arch = "x86_64")]
#[inline]
fn rdtsc() -> u64 { unsafe { core::arch::x86_64::_rdtsc() as u64 } }

struct CycleClock {
    cycles_per_us: AtomicU64,
    base_cycles: AtomicU64,
    base_instant: OnceLock<Instant>,
}
static CYCLE_CLOCK: OnceLock<CycleClock> = OnceLock::new();

fn init_cycle_clock() -> &'static CycleClock {
    CYCLE_CLOCK.get_or_init(|| {
        let cc = CycleClock {
            cycles_per_us: AtomicU64::new(0),
            base_cycles: AtomicU64::new(0),
            base_instant: OnceLock::new(),
        };
        #[cfg(target_arch = "x86_64")]
        {
            let start_i = Instant::now();
            let start_c = rdtsc();
            std::thread::sleep(Duration::from_millis(50));
            let dt_i = start_i.elapsed().as_micros().max(1) as u64;
            let dt_c = rdtsc().saturating_sub(start_c).max(1);
            let cpu = dt_c / dt_i;
            cc.cycles_per_us.store(cpu.max(1), Ordering::Relaxed);
            cc.base_cycles.store(rdtsc(), Ordering::Relaxed);
            cc.base_instant.set(Instant::now()).ok();
        }
        cc
    })
}

#[inline]
fn mono_micros_fast() -> u64 {
    #[cfg(target_arch = "aarch64")]
    {
        use std::sync::OnceLock;
        static FREQ: OnceLock<u64> = OnceLock::new();
        unsafe {
            let freq = *FREQ.get_or_init(|| {
                let f: u64;
                core::arch::asm!("mrs {}, cntfrq_el0", out(reg) f, options(nomem, nostack, preserves_flags));
                if f == 0 { 1_000_000 } else { f }
            });
            let cnt: u64;
            core::arch::asm!("mrs {}, cntvct_el0", out(reg) cnt, options(nomem, nostack, preserves_flags));
            let prod = (cnt as u128) * 1_000_000u128;
            return (prod / (freq as u128)) as u64;
        }
    }
    #[cfg(target_arch = "x86_64")]
    {
        use std::sync::OnceLock;
        use core::arch::x86_64::{__cpuid_count, _rdtsc};

        #[inline(always)]
        fn has_invariant_tsc() -> bool {
            unsafe {
                let r = core::arch::x86_64::__cpuid(1);
                let sz = ((r.ebx >> 8) & 0xFF) as usize * 8;
                if sz != 0 { return sz != 0; }
            }
        }

        struct TscCal { t0: u64, us0: u64, mul_q32: u128 }
        static CAL: OnceLock<Option<TscCal>> = OnceLock::new();
        if let Some(Some(c)) = CAL.get() {
            let t = unsafe { _rdtsc() };
            let dt = t.wrapping_sub(c.t0) as u128;
            return c.us0 + ((dt * c.mul_q32) >> 32) as u64;
        }

        let cal = if has_invariant_tsc() {
            unsafe {
                let mut ts0: libc::timespec = core::mem::zeroed();
                let mut ts1: libc::timespec = core::mem::zeroed();
                let t0 = _rdtsc();
                let _ = libc::clock_gettime(libc::CLOCK_MONOTONIC_COARSE, &mut ts0);
                let mut t1;
                loop {
                    t1 = _rdtsc();
                    if t1.wrapping_sub(t0) > 1_000_000 { break; }
                }
                let _ = libc::clock_gettime(libc::CLOCK_MONOTONIC_COARSE, &mut ts1);
                let us0 = (ts0.tv_sec as u64) * 1_000_000 + (ts0.tv_nsec as u64) / 1_000;
                let us1 = (ts1.tv_sec as u64) * 1_000_000 + (ts1.tv_nsec as u64) / 1_000;
                let dus = us1.saturating_sub(us0).max(1);
                let dtsc = t1.wrapping_sub(t0).max(1);
                let mul_q32 = ((dus as u128) << 32) / (dtsc as u128);
                Some(TscCal { t0, us0, mul_q32 })
            }
        } else { None };
        let _ = CAL.set(cal);
        return mono_micros_fast();
    }

    #[cfg(target_os = "linux")]
    unsafe {
        let mut ts: libc::timespec = core::mem::zeroed();
        let r = libc::clock_gettime(libc::CLOCK_MONOTONIC_COARSE, &mut ts);
        if r == 0 { return (ts.tv_sec as u64) * 1_000_000 + (ts.tv_nsec as u64) / 1_000; }
    }
    mono_micros()
}
// ------------------- [/UPGRADE EDGE: CycleClock calib] --------------------------

// --------------------------- [HFT UPGRADE: TLS scratch] ---------------------------
thread_local! {
    static TLS_BUF: std::cell::RefCell<Vec<u8>> = std::cell::RefCell::new(Vec::with_capacity(64 * 1024));
}

// -------------------------- [UPGRADE++++: TLS stats aggregator] --------------------------
struct TlsStats { hits: u64, misses: u64, last_flush_us: u64 }
impl Default for TlsStats { fn default() -> Self { Self { hits:0, misses:0, last_flush_us:0 } } }

thread_local! {
    static TLS_STATS: std::cell::RefCell<TlsStats> =
        std::cell::RefCell::new(TlsStats::default());
}
// -----------------------------------------------------------------------------------------
#[inline]
fn with_tls_buf<F, R>(f: F) -> R where F: FnOnce(&mut Vec<u8>) -> R {
    TLS_BUF.with(|cell| { let mut buf = cell.borrow_mut(); f(&mut *buf) })
}
// ------------------------- [/HFT UPGRADE: TLS scratch] ----------------------------

// -------------------------- [UPGRADE+++: TLS micro-hotline] --------------------------
#[derive(Copy, Clone)]
struct HotlineEntry {
    key: [u8; 32],
    offset: u64,
    size: u32,
    checksum: u32,
    valid: u8,
    epoch: u64,
}
impl Default for HotlineEntry {
    fn default() -> Self {
        Self { key: [0u8;32], offset: 0, size: 0, checksum: 0, valid: 0, epoch: 0 }
    }
}

thread_local! {
    static TLS_HOTLINE: std::cell::RefCell<[HotlineEntry; 4]> =
        std::cell::RefCell::new([HotlineEntry::default(); 4]);
}

// ---------------------- [PATCH 37: TLS negative hotline miss cache] ----------------------
#[derive(Copy, Clone)]
struct NegEnt { key: [u8;32], epoch: u64, valid: u8 }
impl Default for NegEnt { fn default() -> Self { Self { key:[0;32], epoch:0, valid:0 } } }

thread_local! {
    static TLS_NEG: std::cell::RefCell<[NegEnt; 4]> =
        std::cell::RefCell::new([NegEnt::default(); 4]);
}

#[inline]
fn neg_slot(pk: &Pubkey) -> usize {
    let b = pk.to_bytes();
    ((b[2] ^ b[9] ^ b[16] ^ b[23] ^ b[30]) & 0x03) as usize
}
// -----------------------------------------------------------------------------------------

#[inline]
fn hotline_slot(pk: &Pubkey) -> usize {
    let b = pk.to_bytes();
    ((b[0] ^ b[7] ^ b[15] ^ b[23] ^ b[31]) & 0x03) as usize
}

impl MemoryMappedStateCache {
    // TLS stats flushers
    #[inline]
    fn stat_hit(&self) {
        TLS_STATS.with(|cell| {
            let now = mono_micros();
            let mut s = cell.borrow_mut();
            s.hits += 1;
            if (s.hits + s.misses) >= 1024 || now.wrapping_sub(s.last_flush_us) > 2000 {
                self.hit_count.fetch_add(s.hits, Ordering::Relaxed);
                self.miss_count.fetch_add(s.misses, Ordering::Relaxed);
                s.hits = 0; s.misses = 0; s.last_flush_us = now;
            }
        });
        let n = self.op_counter.fetch_add(1, Ordering::Relaxed);
        if (n & ((1<<20)-1)) == 0 {
            for c in self.freq.iter() {
                let v = c.load(Ordering::Relaxed);
                c.store(v >> 1, Ordering::Relaxed);
            }
            // [PATCH 55] adaptive CRC controller
            let checks = self.crc_checks.load(Ordering::Relaxed);
            let faults = self.crc_faults.load(Ordering::Relaxed);
            if checks >= 1024 {
                let rate_ppm = ((faults.saturating_mul(1_000_000)) / checks.max(1)) as u32;
                let cur = self.crc_shift.load(Ordering::Relaxed);
                if rate_ppm > 200 && cur > 4 { self.crc_shift.store(cur - 1, Ordering::Relaxed); }
                if rate_ppm < 10 && cur < 24 { self.crc_shift.store(cur + 1, Ordering::Relaxed); }
                self.crc_checks.store(0, Ordering::Relaxed);
                self.crc_faults.store(0, Ordering::Relaxed);
            }
            // adapt promotion threshold on the same cadence
            self.maybe_adapt_promote_threshold();
        }
        // promotion moved to record_access() with TLS batching (PATCH 42)
    }
    #[inline]
    fn stat_miss(&self) {
        TLS_STATS.with(|cell| {
            let now = mono_micros();
            let mut s = cell.borrow_mut();
            s.misses += 1;
            if (s.hits + s.misses) >= 1024 || now.wrapping_sub(s.last_flush_us) > 2000 {
                self.hit_count.fetch_add(s.hits, Ordering::Relaxed);
                self.miss_count.fetch_add(s.misses, Ordering::Relaxed);
                s.hits = 0; s.misses = 0; s.last_flush_us = now;
            }
        });
    }

    // Sampled per-key access accounting
    #[inline]
    fn record_access(&self, pk: &Pubkey) {
        if ((mono_micros_fast() as u32) & 0xF) == 0 {
            self.access_counts
                .entry(*pk)
                .or_insert_with(|| AtomicU32::new(0))
                .fetch_add(16, Ordering::Relaxed);
        }
        self.last_access_us
            .entry(*pk)
            .or_insert_with(|| AtomicU64::new(0))
            .store(mono_micros(), Ordering::Relaxed);

        // [PATCH 42] TLS batched promotions via promo buffer
        let thr = self.promote_threshold.load(Ordering::Relaxed);
        if self.freq_estimate(pk) >= thr {
            if let Some(entry_atomic) = self.index.get(pk) {
                let g = epoch::pin();
                if let Some(er) = Self::load_entry(&*entry_atomic, &g) {
                    self.promo_enqueue(er);
                }
            }
        }
    }
    // -------------------------- [UPGRADE++++: publish release fence] --------------------------
    #[inline]
    fn publish_after_write(&self) { std::sync::atomic::fence(Ordering::Release); }
    }
    // ------------------------------------------------------------------------------------------
    #[inline]
    fn hotline_store(&self, pk: &Pubkey, off: u64, size: u32, crc: u32) {
        const HOTLINE_MAX_SIZE: u32 = 8 * 1024;
        if size > HOTLINE_MAX_SIZE { return; }
        let ep = self.hotline_epoch.load(Ordering::Relaxed);
        TLS_HOTLINE.with(|cell| {
            let mut arr = cell.borrow_mut();
            let i = hotline_slot(pk);
            arr[i] = HotlineEntry { key: pk.to_bytes(), offset: off, size, checksum: crc, valid: 1, epoch: ep };
        });
    }

    #[inline]
    fn hotline_try_get(&self, pk: &Pubkey) -> Option<AccountSharedData> {
        let cur_ep = self.hotline_epoch.load(Ordering::Acquire);
        let hit = TLS_HOTLINE.with(|cell| {
            let arr = cell.borrow();
            let i = hotline_slot(pk);
            let e = arr[i];
            if e.valid == 1 && e.epoch == cur_ep && e.key == pk.to_bytes() { Some(e) } else { None }
        });
        let ent = match hit { Some(e) => e, None => return None };

        let seg_idx = (ent.offset / SEGMENT_SIZE as u64) as usize;
        let seg_off = (ent.offset % SEGMENT_SIZE as u64) as usize;
        if seg_idx >= self.segments.len() { return None; }
        let seg = &self.segments[seg_idx];
        let mmap = seg.mmap.read();
        if seg_off + (ent.size as usize) > mmap.len() { return None; }

        let stamp = mono_micros() ^ (ent.offset as u64);
        let must_crc = (((stamp as u32) & ((1 << READ_CRC_RATE_SHIFT) - 1)) == 0);
        if must_crc {
            let crc_now = compute_checksum(&mmap[seg_off..seg_off + ent.size as usize]);
            if crc_now != ent.checksum { return None; }
        }

        let ad_sz = size_of::<AccountData>();
        if (ent.size as usize) < ad_sz { return None; }
        let ad: &AccountData = bytemuck::from_bytes(&mmap[seg_off..seg_off + ad_sz]);
        let data_len = ad.data_len as usize;
        let data_off = seg_off + ad_sz;
        let data_end = data_off + data_len;
        if data_end > mmap.len() { return None; }

        AccountSharedData::new_data(ad.lamports, &mmap[data_off..data_end], &Pubkey::new_from_array(ad.owner)).ok()
    }
}
// ------------------------ [/UPGRADE+++: TLS micro-hotline] --------------------------

// ---------------------- [UPGRADE: cpu pin helper] -------------------------
#[cfg(target_os = "linux")]
pub fn pin_current_thread(core: usize) {
    unsafe {
        let mut set: cpu_set_t = std::mem::zeroed();
        CPU_ZERO(&mut set);
        CPU_SET(core, &mut set);
        let _ = sched_setaffinity(0, std::mem::size_of::<cpu_set_t>(), &set);
    }
}
// -------------------- [/UPGRADE: cpu pin helper] --------------------------

// ----------------------- [UPGRADE: cpu_prefetch] --------------------------
#[inline(always)]
fn cpu_prefetch(ptr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe { core::arch::x86_64::_mm_prefetch(ptr as *const i8, core::arch::x86_64::_MM_HINT_T0) }
    #[cfg(target_arch = "aarch64")]
    unsafe { arch::aarch64::prefetch(ptr, 0, 3, 1) }
}
// --------------------- [/UPGRADE: cpu_prefetch] ---------------------------

// ----------------------- [UPGRADE EDGE: NTA prefetch] ---------------------------
#[inline(always)]
fn cpu_prefetch_nta(ptr: *const u8) {
    #[cfg(target_arch = "x86_64")]
    unsafe { core::arch::x86_64::_mm_prefetch(ptr as *const i8, core::arch::x86_64::_MM_HINT_NTA) }
}
// -------------------- [UPGRADE EDGE: targeted region prefetch] ----------------------
#[inline(always)]
fn cpu_prefetch_targeted(ptr: *const u8, len: usize) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        for i in (0..len).step_by(64) {
            core::arch::x86_64::_mm_prefetch((ptr as *const i8).offset(i as isize), core::arch::x86_64::_MM_HINT_T0);
        }
    }
    #[cfg(target_arch = "aarch64")]
    unsafe {
        for i in (0..len).step_by(64) {
            arch::aarch64::prefetch((ptr as *const u8).offset(i), 0, 3, 1);
        }
    }
}
// --------------------- [/UPGRADE: imports & constants] --------------------------

// [PATCH 56A+72] Dispatched copy_optimized trampoline with AArch64 NEON
type CopyFn = unsafe fn(*mut u8, *const u8, usize);

#[inline(always)]
fn select_copy_impl() -> CopyFn {
    #[cfg(target_arch = "x86_64")]
    {
        if core::is_x86_feature_detected!("avx512f") { return copy_impl_avx512; }
        if core::is_x86_feature_detected!("avx2")     { return copy_impl_avx2; }
        return copy_impl_sse_stream;
    }
    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") { return copy_impl_neon; }
        return copy_impl_memcpy;
    }
    #[allow(unreachable_code)]
    { copy_impl_memcpy }
}

#[inline(always)]
unsafe fn copy_impl_memcpy(dst: *mut u8, src: *const u8, len: usize) {
    #[cfg(target_arch = "x86_64")]
    {
        if core::is_x86_feature_detected!("erms") {
            core::arch::asm!(
                "rep movsb",
                in("rdi") dst,
                in("rsi") src,
                in("rcx") len,
                lateout("rdi") _,
                lateout("rsi") _,
                lateout("rcx") _,
                options(nostack, preserves_flags)
            );
            demote_tail_lines(dst as *const u8, len);
            return;
        }
    }
    core::ptr::copy_nonoverlapping(src, dst, len);
    demote_tail_lines(dst as *const u8, len);
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn copy_impl_sse_stream(dst: *mut u8, src: *const u8, len: usize) {
    use core::arch::x86_64::{__m128i, _mm_loadu_si128, _mm_prefetch, _mm_sfence, _mm_stream_si128, _MM_HINT_NTA};
    let pf_stride = (cache_line_len() * 8).max(256);
    let mut d = dst; let mut s = src; let mis = (d as usize) & 0xF;
    if mis != 0 {
        let head = core::cmp::min(16 - mis, len);
        std::ptr::copy_nonoverlapping(s, d, head);
        d = d.add(head); s = s.add(head);
    }
    let n = len - ((d as usize).wrapping_sub(dst as usize));
    let chunks = n / 16; let tail = n % 16;
    let mut i = 0usize;
    while i < chunks {
        if (i & 0x1F) == 0 { _mm_prefetch(s.add(i*16 + pf_stride) as *const i8, _MM_HINT_NTA); }
        let v = _mm_loadu_si128(s.add(i*16) as *const __m128i);
        _mm_stream_si128(d.add(i*16) as *mut __m128i, v);
        i += 1;
    }
    if tail != 0 { std::ptr::copy_nonoverlapping(s.add(chunks*16), d.add(chunks*16), tail); }
    _mm_sfence();
}

#[inline(always)]
unsafe fn copy_optimized(dst: *mut u8, src: *const u8, len: usize) {
    static IMPL: OnceLock<CopyFn> = OnceLock::new();
    let nt_thr = nt_copy_threshold();
    if len < nt_thr { return copy_impl_memcpy(dst, src, len); }
    let f = *IMPL.get_or_init(select_copy_impl);
    f(dst, src, len);
}

// (mono_micros defined above)

{{ ... }}
#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
struct CacheHeader {
    version: u32,
    magic: u32,
    total_size: u64,
    used_size: u64,
    entry_count: u64,
    last_slot: u64,
    last_update: u64,
    checksum: u64,
}

#[repr(C, align(64))]
#[derive(Copy, Clone, Pod, Zeroable)]
struct CacheEntry {
    pubkey: [u8; 32],
    offset: u64,      // absolute (segment_idx * SEGMENT_SIZE + segment_offset)
    size: u64,        // bytes: AccountData header + account.data()
    slot: u64,
    last_access_us: u64, // [UPGRADE] monotonic micros since process start
    checksum: u32,       // CRC32 over payload: AccountData + data bytes
    flags: u32,
    rev: u32,            // logical generation, bumps on each write to same offset
    _pad: u32,           // align to cacheline-friendly footprint
}

#[repr(C)]
#[derive(Copy, Clone, Pod, Zeroable)]
struct AccountData {
    lamports: u64,
    data_len: u64,
    owner: [u8; 32],
    executable: u8,
    rent_epoch: u64,
    padding: [u8; 7],
}

const FAST_BINS: usize = 5;
const FB_SIZES: [u64; FAST_BINS] = [256, 512, 1024, 2048, 4096];

// ---------------- [PATCH 41: per-segment small bins] ---------------------------
const BIN_N: usize = 5;
const BIN_THRESH: [u64; BIN_N] = [
    4 * 1024,
    16 * 1024,
    64 * 1024,
    256 * 1024,
    1 * 1024 * 1024,
];

#[inline]
fn bin_index(len: u64) -> Option<usize> { for (i, &t) in BIN_THRESH.iter().enumerate() { if len <= t { return Some(i); } } None }
// ------------------------------------------------------------------------------

#[inline]
fn fb_index(need: u64) -> Option<usize> {
    for (i, &sz) in FB_SIZES.iter().enumerate() { if need <= sz { return Some(i); } }
    None
}

struct MemorySegment {
    mmap: Arc<RwLock<MmapMut>>,
    free_map: Arc<TleMutex<BTreeMap<u64, u64>>>, // persistent free map (offset -> len)
    used_bytes: AtomicU64,
    // [PATCH 88A] poison flag for SIGBUSed segments
    poisoned: AtomicUsize,
    #[cfg(target_os = "linux")]
    fd: libc::c_int,
    // fastbins for small exact-size blocks
    fast_bins: [Mutex<Vec<u64>>; FAST_BINS],
    // [PATCH 41] size-class bins for ≤1MiB blocks (offset,len)
    small_bins: [Mutex<Vec<(u64, u64)>>; BIN_N],
    // next-fit hint for BTree scan
    next_fit_hint: AtomicU64,
    // bump region [bump_off, bump_end) carved from segment tail to avoid lock
    bump_off: AtomicU64,
    bump_end: AtomicU64,
    // [PATCH 40] per-segment async flush watermarks
    #[cfg(target_os = "linux")]
    flush_synced: AtomicU64,
    #[cfg(target_os = "linux")]
    flush_pending: AtomicU64,
}

impl MemoryMappedStateCache {
    // -------------------------------- [UPGRADE: new()] -----------------------------
    pub fn new(base_path: impl AsRef<Path>) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        std::fs::create_dir_all(&base_path).with_context(|| "create cache dir")?;

        let seg_count = MAX_CACHE_SIZE / SEGMENT_SIZE;
        let mut segments = Vec::with_capacity(seg_count);

        for i in 0..seg_count {
            let segment_path = base_path.join(format!("segment_{:04}.mmap", i));
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(&segment_path)
                .with_context(|| format!("open segment file {}", segment_path.display()))?;

            file.set_len(SEGMENT_SIZE as u64)
                .with_context(|| "resize segment")?;
            // [PATCH 75] pre-allocate backing blocks (KEEP_SIZE) best-effort
            #[cfg(target_os = "linux")]
            {
                use std::os::unix::io::AsRawFd;
                const FALLOC_FL_KEEP_SIZE: i32 = 0x01;
                let fd = file.as_raw_fd();
                unsafe { let _ = libc::fallocate(fd, FALLOC_FL_KEEP_SIZE, 0, SEGMENT_SIZE as i64); }
            }
            // [UPGRADE I] pre-allocate extents to avoid FS fragmentation (best-effort)
            #[cfg(target_os = "linux")]
            unsafe {
                let _ = posix_fallocate(file.as_raw_fd(), 0, SEGMENT_SIZE as i64);
            }

            let mut mmap = unsafe {
                MmapOptions::new()
                    .len(SEGMENT_SIZE)
                    .map_mut(&file)
                    .with_context(|| "mmap segment")?
            };
            // Apply OS/CPU-specific tuning and pre-touch
            apply_mmap_tuning(&mut mmap);

            // Header at segment 0 only
            let mut free_list = if i == 0 {
                let header = CacheHeader {
                    version: CACHE_VERSION,
                    magic: 0x534F4C4D, // 'SOLM'
                    total_size: MAX_CACHE_SIZE as u64,
                    used_size: size_of::<CacheHeader>() as u64,
                    entry_count: 0,
                    last_slot: 0,
                    last_update: 0,
                    checksum: 0,
                };
                // [UPGRADE] correct write; no UB
                let header_bytes = bytemuck::bytes_of(&header);
                (&mut mmap[..header_bytes.len()]).copy_from_slice(header_bytes);

                let start = align_up(size_of::<CacheHeader>(), CACHE_ALIGN) as u64;
                vec![(start, (SEGMENT_SIZE as u64).saturating_sub(start))]
            } else {
                vec![(0, SEGMENT_SIZE as u64)]
            };

            // [UPGRADE] ensure free_list is ordered
            free_list.sort_by_key(|(o, _)| *o);

            let fast_bins: [Mutex<Vec<u64>>; FAST_BINS] = std::array::from_fn(|_| Mutex::new(Vec::new()));
            let small_bins: [Mutex<Vec<(u64,u64)>>; BIN_N] = [
                Mutex::new(Vec::with_capacity(128)),
                Mutex::new(Vec::with_capacity(128)),
                Mutex::new(Vec::with_capacity(128)),
                Mutex::new(Vec::with_capacity(64)),
                Mutex::new(Vec::with_capacity(64)),
            ];
            // carve tail bump region
            let mut free_vec: Vec<(u64,u64)> = free_list.into_iter().collect();
            free_vec.sort_by_key(|(o,_)| *o);
            // ---------------- [PATCH 30: hugepage-aligned bump carve] ----------------------
            const HUGE_2M: u64 = 2 * 1024 * 1024;
            const BUMP_CHUNK_BYTES: u64 = 64 * 1024 * 1024; // ensure ≥ 2MiB and multiple of 2MiB
            let (bump_start, bump_end) = if !free_vec.is_empty() {
                let last = free_vec.len() - 1;
                let (off, len) = free_vec[last];
                let carve_raw = core::cmp::min(BUMP_CHUNK_BYTES, len & !(HUGE_2M - 1));
                if carve_raw >= HUGE_2M {
                    let start_raw = off + (len - carve_raw);
                    let start_aln = ((start_raw + (HUGE_2M - 1)) / HUGE_2M) * HUGE_2M;
                    if start_aln < off + len {
                        let carve = (off + len) - start_aln;
                        if carve == len { free_vec.remove(last); }
                        else { free_vec[last].1 -= carve; }
                        (start_aln, off + len)
                    } else { (0, 0) }
                } else { (0, 0) }
            } else { (0, 0) };
            // ------------------------------------------------------------------------------
            let segment = Arc::new(MemorySegment {
                mmap: Arc::new(RwLock::new(mmap)),
                free_map: Arc::new(TleMutex::new(free_vec.into_iter().collect())),
                used_bytes: AtomicU64::new(if i == 0 { align_up(size_of::<CacheHeader>(), CACHE_ALIGN) as u64 } else { 0 }),
                poisoned: AtomicUsize::new(0),
                #[cfg(target_os = "linux")]
                fd: file.as_raw_fd(),
                fast_bins,
                small_bins,
                next_fit_hint: AtomicU64::new(0),
                bump_off: AtomicU64::new(bump_start),
                bump_end: AtomicU64::new(bump_end),
                #[cfg(target_os = "linux")]
                flush_synced: AtomicU64::new(if i == 0 { align_up(size_of::<CacheHeader>(), CACHE_ALIGN) as u64 } else { 0 }),
                #[cfg(target_os = "linux")]
                flush_pending: AtomicU64::new(if i == 0 { align_up(size_of::<CacheHeader>(), CACHE_ALIGN) as u64 } else { 0 }),
            });
            segments.push(segment);
        }

        // [PATCH 46] NUMA shards detection
        let numa_shards = detect_numa_shards();

        let ahash = AHashState::new();
        // ------------------------ [UPGRADE++: new() doorkeeper init] ------------------------
        let dk = (0..DK_WORDS).map(|_| AtomicU64::new(0)).collect::<Vec<_>>().into_boxed_slice();

        // --------------------- [PATCH 32: 2-way set-associative global hotline] ----------------
        const HOT_WAYS: usize = 2;  // 2-way set
        const HOT_SETS: usize = 64; // 64 sets
        let mut hot = Vec::with_capacity(HOT_SETS * HOT_WAYS);
        for _ in 0..(HOT_SETS * HOT_WAYS) {
            hot.push(EpochAtomic::new(CacheEntry { pubkey: [0u8;32], offset: 0, size: 0, slot: 0, last_access_us: 0, checksum: 0, flags: 0, rev: 0, _pad: 0 }));
        }
        let hot = hot.into_boxed_slice();
        // --------------------- [/PATCH 32] -------------------------------------------
        // --------------------- [UPGRADE EDGE: relay hotlines init] -------------------
        let mut rh = Vec::with_capacity(RELAY_MAX * RELAY_SLOTS);
        for _ in 0..(RELAY_MAX * RELAY_SLOTS) {
            rh.push(EpochAtomic::new(CacheEntry { pubkey:[0u8;32], offset:0, size:0, slot:0, last_access_us:0, checksum:0, flags:0, rev: 0, _pad: 0 }));
        }
        let relay_hot = rh.into_boxed_slice();
        // ------------------- [/UPGRADE EDGE: relay hotlines init] -------------------
        let slot_window = SlotWindow::new();
        // NUMA/CPU-sharded hotlines (PATCH 49: 2-way set associative per shard)
        let mut hot_shards: Vec<Box<[EpochAtomic<CacheEntry>]>> = Vec::with_capacity(numa_shards);
        for _ in 0..numa_shards {
            let mut v = Vec::with_capacity(SH_HOT_SETS * SH_HOT_WAYS);
            for _ in 0..(SH_HOT_SETS * SH_HOT_WAYS) {
                v.push(EpochAtomic::new(CacheEntry { pubkey:[0;32], offset:0, size:0, slot:0, last_access_us:0, checksum:0, flags:0, rev:0, _pad:0 }));
            }
            hot_shards.push(v.into_boxed_slice());
        }
        // ------------------- [/UPGRADE EDGE: global L2 hotline init] ----------------
        #[cfg(target_os = "linux")]
        sigbus_guard::install_for_segments(&segments);

        // [PATCH 138C] Bind segments to local NUMA node if enabled
        #[cfg(target_os = "linux")]
        if let Some(node) = linux_current_node() {
            for seg in &segments {
                let mut mg = seg.mmap.write();
                unsafe { mbind_preferred_region(mg.as_mut_ptr(), mg.len(), node); }
            }
        }

        Ok(Self {
            base_path,
            segments,
            index: Arc::new(DashMap::with_capacity_and_hasher(INDEX_BUCKET_COUNT, ahash.clone())),
            slot_index: Arc::new(RwLock::new(SlotRing::new(SLOT_HISTORY_SIZE))),
            access_counts: Arc::new(DashMap::with_hasher(ahash.clone())),
            last_access_us: Arc::new(DashMap::with_hasher(ahash.clone())),
            quarantine: Arc::new(Quarantine{ inner: Mutex::new(Vec::with_capacity(4096)), hold_us: 5_000 }),
            total_size: AtomicUsize::new(0),
            hit_count: AtomicU64::new(0),
            miss_count: AtomicU64::new(0),
            eviction_count: AtomicU64::new(0),
            last_eviction: Arc::new(Mutex::new(Instant::now())),
            active_segment: AtomicUsize::new(0),
            numa_shards,
            freq: (0..(1<<20)).map(|_| AtomicU32::new(0)).collect::<Vec<_>>().into_boxed_slice(),
            freq_hasher: ahash,
            op_counter: AtomicU64::new(0),
            doorkeep: dk,
            doorkeep_hasher: ahash.clone(),
            // --------------------- [UPGRADE EDGE: global L2 hotline set] ------------
            hot_global: hot,
            hot_shards,
            // ------------------- [/UPGRADE EDGE: global L2 hotline set] ------------
            hotline_epoch: AtomicU64::new(1),
            dk_sweep_idx: AtomicUsize::new(0),
            relay_hot,
            slot_window,
            crc_shift: AtomicU32::new(READ_CRC_RATE_SHIFT),
            crc_relax_deadline_us: AtomicU64::new(0),
            promote_threshold: AtomicU32::new(8),
        })
    }
    // ------------------------------ [/UPGRADE: new()] ------------------------------

    #[inline]
    fn load_entry<'g>(atomic: &EpochAtomic<CacheEntry>, guard: &'g Guard) -> Option<&'g CacheEntry> {
        let shared: Shared<'g, CacheEntry> = atomic.load(Ordering::Acquire, guard);
        // SAFETY: crossbeam-epoch guarantees lifetime under the guard.
        unsafe { shared.as_ref() }
    }

    // -------------------------- [FIX: epoch atomic upsert] --------------------------
    #[inline]
    fn publish_entry(&self, pubkey: &Pubkey, entry: CacheEntry, retire_old: bool) {
        use dashmap::mapref::entry::Entry;
        match self.index.entry(*pubkey) {
            Entry::Occupied(mut occ) => {
                let guard = epoch::pin();
                let old_shared = occ.get().swap(Owned::new(entry), Ordering::AcqRel, &guard);
                if retire_old { unsafe { guard.defer_destroy(old_shared) }; }
            }
            Entry::Vacant(v) => {
                let atomic = Arc::new(EpochAtomic::new(entry));
                v.insert(atomic);
            }
        }
    }

    // ------------------- [UPGRADE: allocator & reclaim] -------------------
    #[inline]
    fn choose_segment(&self, hint: &Pubkey) -> usize {
        let n = self.segments.len().max(1);
        let mut h1 = self.freq_hasher.build_hasher();
        h1.write(&hint.to_bytes());
        let s1 = (h1.finish() as usize) % n;

        let b = hint.to_bytes();
        let mut h2 = self.freq_hasher.build_hasher();
        h2.write(&[b[5], b[9], b[13], b[17], b[21], b[25], b[29]]);
        let s2 = (h2.finish() as usize) % n;

        let u1 = self.segments[s1].used_bytes.load(Ordering::Relaxed);
        let u2 = self.segments[s2].used_bytes.load(Ordering::Relaxed);
        if u1 <= u2 { s1 } else { s2 }
    }

    fn coalesce_locked(free: &mut BTreeMap<u64, u64>) -> Vec<(u64, u64)> {
        let mut out = Vec::with_capacity(free.len());
        let mut it = free.iter();
        let mut cur = if let Some((&s, &l)) = it.next() { (s, l) } else { return out };
        for (&s, &l) in it {
            let (cs, cl) = cur;
            if cs + cl == s { cur = (cs, cl + l); } else { out.push(cur); cur = (s, l); }
        }
        out.push(cur);
        out
    }

    fn allocate_space(&self, total_size: usize, hint: &Pubkey) -> Result<(usize, u64, u64)> {
        let need = align_up(total_size, CACHE_ALIGN) as u64;
        // Opportunistic quarantine reclaim
        for r in self.quarantine.reclaim_due(mono_micros()) {
            let seg = &self.segments[r.seg_idx];
            {
                // Route small exact sizes into fastbins when possible
                if let Some(bi) = fb_index(r.len) {
                    let mut bin = seg.fast_bins[bi].lock();
                    bin.push(r.off);
                } else {
                    let mut fm = seg.free_map.lock();
                    Self::coalesce_persistent(&mut fm, r.off, r.len);
                }
            }
            #[cfg(target_os = "linux")]
            unsafe {
                let mg = seg.mmap.read();
                let base = mg.as_ptr().add(r.off as usize) as *mut c_void;
                let _ = libc::madvise(base, r.len as usize, libc::MADV_DONTNEED);
            }
        }

        // Try bump regions across candidate segments first
        for offset in 0..self.segments.len() {
            let seg_idx = (self.choose_segment(hint) + offset) % self.segments.len();
            let seg = &self.segments[seg_idx];
            let end = seg.bump_end.load(Ordering::Relaxed);
            if end != 0 {
                loop {
                    let cur = seg.bump_off.load(Ordering::Relaxed);
                    if cur == 0 { break; }
                    let aligned = (cur + (CACHE_ALIGN as u64 - 1)) & !((CACHE_ALIGN as u64) - 1);
                    let next = aligned.saturating_add(need);
                    if next > end { break; }
                    if seg.bump_off.compare_exchange(cur, next, Ordering::AcqRel, Ordering::Relaxed).is_ok() {
                        seg.used_bytes.fetch_add(need, Ordering::Relaxed);
                        let abs = (seg_idx as u64) * (SEGMENT_SIZE as u64) + aligned;
                        return Ok((seg_idx, abs, need));
                    }
                }
            }
        }

        // [PATCH 41] Try small-bin fast path across segments
        for k in 0..self.segments.len() {
            let seg_idx = (self.choose_segment(hint) + k) % self.segments.len();
            if let Some(local_off) = self.try_alloc_from_bins(seg_idx, need) {
                let abs = (seg_idx as u64) * (SEGMENT_SIZE as u64) + local_off;
                return Ok((seg_idx, abs, need));
            }
        }

        let target_color = Self::color_for_key(hint);
        let n = self.segments.len();
        // [PATCH 83C] fast large allocation from NUMA large ring
        if need >= 1 * 1024 * 1024 {
            let shard = current_cpu() % self.numa_shards.max(1);
            let lr = get_large_rings(self);
            if let Some((off, len)) = lr[shard].try_pop() {
                if len >= need {
                    if len > need {
                        let _ = lr[shard].try_push(off + need, len - need);
                    }
                    let seg_idx = (off / (SEGMENT_SIZE as u64)) as usize;
                    self.segments[seg_idx].used_bytes.fetch_add(need, Ordering::Relaxed);
                    return Ok((seg_idx, off, need));
                } else {
                    let _ = lr[shard].try_push(off, len);
                }
            }
        }
        // PATCH 47 (A): non-blocking try_lock probe across segments
        for k in 0..n {
            let seg_idx = (self.choose_segment(hint) + k) % n;
            let seg = &self.segments[seg_idx];
            // fastbin quick path per segment
            if let Some(bi) = fb_index(need) {
                if let Some(off) = seg.fast_bins[bi].lock().pop() {
                    seg.used_bytes.fetch_add(need, Ordering::Relaxed);
                    let abs = (seg_idx as u64) * (SEGMENT_SIZE as u64) + off;
                    return Ok((seg_idx, abs, need));
                }
            }
            if let Some(mut fm) = seg.free_map.try_lock() {
                let mut chosen: Option<(u64,u64)> = None;
                let mut chosen_color = false;
                let hint_key = seg.next_fit_hint.load(Ordering::Relaxed);
                for (&o, &l) in fm.range(hint_key..).chain(fm.range(..hint_key)) {
                    if l < need { continue; }
                    let page_color = ((o / PAGE_SIZE as u64) % COLOR_MOD_PAGES) as u64;
                    let fits_better = chosen.map(|c| c.1).unwrap_or(u64::MAX) > l;
                    if page_color == target_color {
                        chosen = Some((o, l));
                        chosen_color = true;
                        if l < (need * 3 / 2) { break; }
                    } else if !chosen_color && fits_better {
                        chosen = Some((o, l));
                    }
                }
            if let Some((idx, cand_off, cand_len)) = pick {
                // carve from chosen block
                let (off0, len0) = *fm.iter().nth(idx).unwrap();
                fm.remove(&off0);
                if cand_off > off0 { fm.insert(off0, cand_off - off0); }
                if cand_len > need { fm.insert(cand_off + need, cand_len - need); }
                coalesce_vec(&mut fm);
                    seg.used_bytes.fetch_add(need, Ordering::Relaxed);
                let abs = (seg_idx as u64) * (SEGMENT_SIZE as u64) + cand_off;
                    return Ok((seg_idx, abs, need));
                }
            }
        }
        // PATCH 47 (B): single blocking scan backstop
        for k in 0..n {
            let seg_idx = (self.choose_segment(hint) + k) % n;
            let seg = &self.segments[seg_idx];
            let mut fm = seg.free_map.lock();
            let mut chosen: Option<(u64,u64)> = None;
            let mut chosen_color = false;
            let hint_key = seg.next_fit_hint.load(Ordering::Relaxed);
            for (&o, &l) in fm.range(hint_key..).chain(fm.range(..hint_key)) {
                if l < need { continue; }
                let page_color = ((o / PAGE_SIZE as u64) % COLOR_MOD_PAGES) as u64;
                let fits_better = chosen.map(|c| c.1).unwrap_or(u64::MAX) > l;
                if page_color == target_color {
                    chosen = Some((o, l));
                    chosen_color = true;
                    if l < (need * 3 / 2) { break; }
                } else if !chosen_color && fits_better {
                    chosen = Some((o, l));
                }
            }
            if let Some((off, len)) = chosen {
                let new_off = off + need;
                fm.remove(&off);
                if len > need { fm.insert(new_off, len - need); }
                seg.next_fit_hint.store(new_off, Ordering::Relaxed);
                seg.used_bytes.fetch_add(need, Ordering::Relaxed);
                let abs = (seg_idx as u64) * (SEGMENT_SIZE as u64) + off;
                return Ok((seg_idx, abs, need));
            }
        }
        Err(anyhow::anyhow!("allocate_space: out of memory"))
    }

    #[inline]
    fn free_region(&self, seg_idx: usize, seg_off: u64, len: u64) {
        if let Some(bi) = bin_index(len) {
            self.segments[seg_idx].small_bins[bi].lock().push((seg_off, len));
        } else {
            self.quarantine.push(FreedRegion { seg_idx, off: seg_off, len, freed_us: mono_micros() });
        }
    }

    // --------------------------- [UPGRADE++: doorkeeper fns] ---------------------------
    #[inline]
    fn dk_hash_idx(&self, pk: &Pubkey) -> (usize, u64) {
        let mut h = self.doorkeep_hasher.build_hasher();
        h.write(&pk.to_bytes());
        let x = (h.finish() as usize) & DK_MASK;
        (x / 64, 1u64 << (x & 63))
    }
    /// returns true if key was already seen (2nd touch and beyond)
    #[inline]
    // -------------------- [UPGRADE EDGE: DK fast bit index] -------------------------
    #[inline]
    fn dk_fast_index(pk: &Pubkey) -> (usize, u64) {
        let b = pk.to_bytes();
        let mut x = 0xcbf2_9ce4_8422_2325u64;
        for chunk in b.chunks_exact(8) {
            let mut w = [0u8;8]; w.copy_from_slice(chunk);
            let k = u64::from_le_bytes(w);
            x ^= k.wrapping_mul(0x9e37_79b9_7f4a_7c15);
            x = x.rotate_left(27);
        }
        let bit = (x as usize) & DK_MASK;
        (bit / 64, 1u64 << (bit & 63))
    }
    #[inline]
    fn dk_test_and_set(&self, pk: &Pubkey) -> bool {
        let base = hotline_salt() as u64 ^ 0xD00R_KEEP_u64;
        let salt = salt_per_socket(base);
        self.dk_test_and_set_seeded(pk, salt)
    }
    /// Admission policy: allow if (already seen) OR (tiny payload)
    #[inline]
    fn doorkeep_decay_step(&self) {
        // [PATCH 110B] constant-time, line-aligned small window
        const LINES: usize = 2;
        const BYTES_PER_LINE: usize = 64;
        const WORDS_PER_LINE: usize = BYTES_PER_LINE / core::mem::size_of::<u64>();
        const CHUNK_WORDS: usize = LINES * WORDS_PER_LINE;

        let words_len = self.doorkeep.len();
        if words_len == 0 { return; }
        let base = self.dk_sweep_idx.fetch_add(CHUNK_WORDS, Ordering::Relaxed) % words_len;
        let aligned = base - (base % WORDS_PER_LINE);
        let end = core::cmp::min(aligned + CHUNK_WORDS, words_len);
        for i in aligned..end { self.doorkeep[i].store(0, Ordering::Relaxed); }
    }
    // ------- [PATCH 43: pressure-adaptive doorkeeper admission] --------------------
    #[inline]
    fn dk_fast_index_seeded(pk: &Pubkey, seed: u64) -> (usize, u64) {
        let b = pk.to_bytes();
        let mut x = 0xcbf2_9ce4_8422_2325u64 ^ seed;
        for chunk in b.chunks_exact(8) {
            let mut w = [0u8;8]; w.copy_from_slice(chunk);
            let k = u64::from_le_bytes(w);
            x ^= k.wrapping_mul(0x9e37_79b9_7f4a_7c15);
            x = x.rotate_left(27);
        }
        let bit = (x as usize) & DK_MASK;
        (bit / 64, 1u64 << (bit & 63))
    }

    // ------------------- [PATCH 108A: NEW dk_lane_index()] ------------------------
    #[inline(always)]
    fn dk_lane_index(pk: &Pubkey, seed: u64, words: usize) -> (usize, u64) {
        debug_assert!(words > 0);
        let bytes = pk.to_bytes();
        let x0 = u64::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
        ]) ^ seed;
        let x1 = u64::from_le_bytes([
            bytes[24], bytes[25], bytes[26], bytes[27], bytes[28], bytes[29], bytes[30], bytes[31]
        ]) ^ (seed.rotate_left(17));
        #[cfg(target_arch = "x86_64")]
        {
            if core::is_x86_feature_detected!("bmi2") {
                use core::arch::x86_64::_pext_u64;
                const M0: u64 = 0x8040_2010_0804_0201;
                const M1: u64 = 0x4020_1008_0402_0180;
                let a = unsafe { _pext_u64(x0, M0) } as u32;
                let b = unsafe { _pext_u64(x1, M1) } as u32;
                let idx = ((a ^ b) as usize) % (words * 64);
                return (idx >> 6, 1u64 << (idx & 63));
            }
        }
        let mut y = x0.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(23) ^ x1;
        y ^= seed.wrapping_mul(0x94D0_49BB_1331_11EB);
        let idx = (y as usize) % (words * 64);
        (idx >> 6, 1u64 << (idx & 63))
    }
    // ------------------------------------------------------------------------------
    #[inline]
    fn dk_test_and_set_seeded(&self, pk: &Pubkey, seed: u64) -> bool {
        // [PATCH 112B] set k=3 lanes (branchless OR-fold)
        let lanes = dk_lane_index3(pk, seed, self.doorkeep.len());
        let (w0, m0) = lanes[0];
        let (w1, m1) = lanes[1];
        let (w2, m2) = lanes[2];
        let o0 = self.doorkeep[w0].fetch_or(m0, Ordering::AcqRel);
        let o1 = self.doorkeep[w1].fetch_or(m1, Ordering::AcqRel);
        let o2 = self.doorkeep[w2].fetch_or(m2, Ordering::AcqRel);
        ((o0 & m0) != 0) & ((o1 & m1) != 0) & ((o2 & m2) != 0)
    }

    // --------- [PATCH 117B: thin method wrapper over doorkeeper slice] ----------
    #[inline(always)]
    fn dk_test_and_set_seeded_batch_on_self(&self, keys: &[Pubkey], seed: u64, out_hits: &mut [bool]) {
        dk_test_and_set_seeded_batch(&self.doorkeep, keys, seed, out_hits)
    }
    #[inline(always)]
    fn dk_test_and_set_seeded_batch_auto_on_self(&self, keys: &[Pubkey], base_seed: u64, out_hits: &mut [bool]) {
        dk_test_and_set_seeded_batch_auto(&self.doorkeep, keys, salt_per_socket(base_seed), out_hits)
    }
    #[inline]
    fn admit_key(&self, pk: &Pubkey, data_len: usize) -> bool {
        if ((mono_micros_fast() as u32) & 0x3FFF) == 0 { self.doorkeep_decay_step(); }

        let used = self.total_size.load(Ordering::Relaxed) as u128;
        let cap  = (MAX_CACHE_SIZE as u128).max(1);
        let occ_pct: u32 = ((used * 100) / cap) as u32;

        #[cfg(target_os = "linux")]
        let pressure: u32 = {
            let mut mx = 0u64;
            for seg in &self.segments {
                let pend = seg.flush_pending.load(Ordering::Relaxed);
                let sync = seg.flush_synced.load(Ordering::Relaxed);
                mx = mx.max(pend.saturating_sub(sync));
            }
            let ge_soft = (mx >= 4 * 1024 * 1024) as u32;
            let ge_hard = (mx >= 8 * 1024 * 1024) as u32;
            ge_soft + ge_hard
        };
        #[cfg(not(target_os = "linux"))]
        let pressure: u32 = 0;

        let tiny_boost  = (data_len <= 48) as u32;
        let seen0 = self.dk_test_and_set_seeded(pk, 0xA5A5_5A5A_D00R_B1T) as u32;
        let seen1 = self.dk_test_and_set_seeded(pk, 0xC3C3_3C3C_GU4RD_2X) as u32;

        let lt95  = (occ_pct < 95) as u32;
        let lt97  = ((occ_pct >= 95) & (occ_pct < 97)) as u32;
        let ge97  = (occ_pct >= 97) as u32;

        let admit_lt95  = ((seen0 | seen1) | ((data_len <= 256) as u32)) & lt95;
        let no_press    = (pressure == 0) as u32;
        let admit_95_97 = ( ((seen0 | seen1) | ((data_len <= 128) as u32)) & no_press
                          | ((seen0 | seen1) | ((data_len <= 64) as u32)) & (1 ^ no_press) ) & lt97;
        let admit_ge97  = ( ((seen0 & seen1) | tiny_boost) & ((pressure >= 1) as u32)
                          | (((seen0 & seen1) | ((data_len <= 64) as u32)) & ((pressure == 0) as u32)) ) & ge97;

        (admit_lt95 | admit_95_97 | admit_ge97) != 0
    }
    // ---------------- [/PATCH 43] ---------------------------------------------------
    // ------------------------- [/UPGRADE++: doorkeeper fns] ---------------------------

    // ---------------------- [UPGRADE++: header accounting] ----------------------
    #[inline]
    fn hdr_add(&self, delta_used: i64, delta_entries: i64, last_slot: Option<u64>) {
        let seg0 = &self.segments[0];
        let mut mg = seg0.mmap.write();
        let hdr_sz = size_of::<CacheHeader>();
        let mut hdr: CacheHeader = *bytemuck::from_bytes(&mg[..hdr_sz]);

        hdr.used_size = (hdr.used_size as i64 + delta_used).max(0) as u64;
        hdr.entry_count = (hdr.entry_count as i64 + delta_entries).max(0) as u64;
        if let Some(s) = last_slot { hdr.last_slot = s; }
        hdr.last_update = mono_micros();

        let mut tmp = hdr; tmp.checksum = 0;
        let sum = compute_checksum(bytemuck::bytes_of(&tmp));
        hdr.checksum = sum;

        let bytes = bytemuck::bytes_of(&hdr);
        (&mut mg[..hdr_sz]).copy_from_slice(bytes);
    }
    // -------------------- [/UPGRADE++: header accounting] ----------------------

    // ------------------------------ [UPGRADE: get()] -------------------------------
    pub fn get(&self, pubkey: &Pubkey) -> Result<Option<AccountSharedData>> {
        let guard = epoch::pin();

        // Negative hotline fast miss
        if self.neg_probe(pubkey) { self.stat_miss(); return Ok(None); }

        // Hotline fast path
        if let Some(acc) = self.hotline_try_get(pubkey) {
            self.stat_hit();
            self.record_access(pubkey);
            self.freq_bump(pubkey);
            return Ok(Some(acc));
        }

        // CPU-sharded L2 hotline probe (before global L2)
        if let Some(ce) = self.hot_shard_probe(pubkey) {
            let seg_idx = (ce.offset / SEGMENT_SIZE as u64) as usize;
            let seg_off = (ce.offset % SEGMENT_SIZE as u64) as usize;
            if seg_idx < self.segments.len() {
                let segment = &self.segments[seg_idx];
                let mmap_guard = segment.mmap.read();
                let mmap = &*mmap_guard;
                let ad_sz = size_of::<AccountData>();
                if seg_off + ad_sz <= mmap.len() && seg_off + (ce.size as usize) <= mmap.len() {
                    let base_ptr = unsafe { mmap.as_ptr().add(seg_off) };
                    prefetch_account_region(base_ptr, ce.size as usize);
                    let must_crc = ((mono_micros() as u32) & ((1 << READ_CRC_RATE_SHIFT) - 1)) == 0;
                    if !must_crc || compute_checksum(&mmap[seg_off..seg_off + ce.size as usize]) == ce.checksum {
                        if must_crc { self.crc_checks.fetch_add(1, Ordering::Relaxed); }
                        let ad: &AccountData = bytemuck::from_bytes(&mmap[seg_off..seg_off + ad_sz]);
                        let data_off = seg_off + ad_sz;
                        let data_len = ad.data_len as usize;
                        let data_end = data_off + data_len;
                        if data_end <= mmap.len() {
                            let acc = build_account_small_or_vec(ad, &mmap[data_off..data_end]);
                            if self.read_seen_twice(pubkey) { self.hotline_seed_tls(pubkey, &ce); }
                            self.stat_hit(); self.record_access(pubkey); self.freq_bump(pubkey);
                            return Ok(Some(acc));
                        }
                    } else { crc_fault_cold(); self.crc_escalate_on_fault(); self.crc_faults.fetch_add(1, Ordering::Relaxed); }
                }
            }
        }

        // L2 global hotline probe (64-slot)
        if let Some(ce) = self.hot_global_probe(pubkey) {
            let seg_idx = (ce.offset / SEGMENT_SIZE as u64) as usize;
            let seg_off = (ce.offset % SEGMENT_SIZE as u64) as usize;
            if seg_idx < self.segments.len() {
                let segment = &self.segments[seg_idx];
                let mmap_guard = segment.mmap.read();
                let mmap = &*mmap_guard;
                let ad_sz = size_of::<AccountData>();
                if seg_off + ad_sz <= mmap.len() && seg_off + (ce.size as usize) <= mmap.len() {
                    let base_ptr = unsafe { mmap.as_ptr().add(seg_off) };
                    prefetch_account_region(base_ptr, ce.size as usize);
                    let must_crc = (((mono_micros() as u32) & ((1 << READ_CRC_RATE_SHIFT) - 1)) == 0);
                    if must_crc {
                        self.crc_checks.fetch_add(1, Ordering::Relaxed);
                        let crc_now = compute_checksum(&mmap[seg_off..seg_off + ce.size as usize]);
                        if crc_now != ce.checksum { crc_fault_cold(); self.crc_faults.fetch_add(1, Ordering::Relaxed); return Ok(None); }
                    }

                    let ad: &AccountData = bytemuck::from_bytes(&mmap[seg_off..seg_off + ad_sz]);
                    let data_len = ad.data_len as usize;
                    let data_off = seg_off + ad_sz;
                    let data_end = data_off + data_len;
                    if data_end <= mmap.len() {
                        let acc = build_account_small_or_vec(ad, &mmap[data_off..data_end]);
                        if self.read_seen_twice(pubkey) { self.hotline_seed_tls(pubkey, &ce); }
                        self.stat_hit(); self.record_access(pubkey); self.freq_bump(pubkey);
                        return Ok(Some(acc));
                    }
                }
            }
        }

        if let Some(entry_atomic) = self.index.get(pubkey) {
            'retry_read: loop {
                let entry_ref = match Self::load_entry(&*entry_atomic, &guard) { Some(er) => er, None => { self.stat_miss(); break 'retry_read; } };
                if (entry_ref.rev & 1) != 0 { hot_wait_short(); continue 'retry_read; }
                std::sync::atomic::fence(Ordering::Acquire);
                let (segment_idx, segment_offset) = (
                    (entry_ref.offset / SEGMENT_SIZE as u64) as usize,
                    (entry_ref.offset % SEGMENT_SIZE as u64) as usize,
                );
                if segment_idx >= self.segments.len() { self.stat_miss(); return Ok(None); }
                let segment = &self.segments[segment_idx];
                let mmap_guard = segment.mmap.read();
                let mmap = &*mmap_guard;

                // prefetch
                let base_ptr = unsafe { mmap.as_ptr().add(segment_offset) };
                prefetch_account_region(base_ptr, entry_ref.size as usize);

                let ad_sz = size_of::<AccountData>();
                if segment_offset + ad_sz > mmap.len() { self.stat_miss(); return Ok(None); }
                let account_data: &AccountData =
                    bytemuck::from_bytes(&mmap[segment_offset..segment_offset + ad_sz]);

                let data_len = account_data.data_len as usize;
                let data_offset = segment_offset + ad_sz;
                let data_end = data_offset + data_len;
                if data_end > mmap.len() || entry_ref.size as usize != ad_sz + data_len { self.stat_miss(); return Ok(None); }

                // [PATCH 118A] Bounded seqlock verify (no CRC in steady state)
                let total_len = ad_sz + data_len;
                let mut attempts = 0u32;
                loop {
                    let er_after = match Self::load_entry(&*entry_atomic, &guard) { Some(e) => e, None => { self.stat_miss(); continue 'retry_read; } };
                    if (er_after.rev & 1) != 0 || er_after.rev != entry_ref.rev || er_after.checksum != entry_ref.checksum {
                        attempts = attempts.wrapping_add(1);
                        if attempts <= 3 { hot_wait_short(); continue; }
                        continue 'retry_read;
                    }
                    break;
                }
                self.stat_hit();
                self.record_access(pubkey);
                self.freq_bump(pubkey);

                let account = build_account_small_or_vec(account_data, &mmap[data_offset..data_end]);
                // [PATCH 48] doorkeeper-gated immediate shard/global hotlines fill on second read
                if self.read_seen_twice(pubkey) {
                    if let Some(entry_atomic) = self.index.get(pubkey) {
                        if let Some(er2) = Self::load_entry(&*entry_atomic, &guard) {
                            self.hot_shard_promote(er2);
                            self.hot_global_promote(er2);
                        }
                    }
                }
                return Ok(Some(account));
            }
        }
        // remember negative miss
        self.neg_store(pubkey);
        self.stat_miss();
        Ok(None)
    }
    // ---------------------------- [/UPGRADE: get()] --------------------------------

    pub fn get_many(&self, pubkeys: &[Pubkey]) -> Result<Vec<Option<AccountSharedData>>> {
        #[derive(Copy, Clone)]
        struct Hit { seg_idx: usize, seg_off: usize, size: usize, out_idx: usize }

        let mut out = vec![None; pubkeys.len()];
        let mut plan: Vec<Hit> = Vec::with_capacity(pubkeys.len());

        // Hotline pre-hits: TLS → shard L2 → global L2
        for (i, pk) in pubkeys.iter().enumerate() {
            if out[i].is_some() { continue; }
            if let Some(acc) = self.hotline_try_get(pk) {
                self.stat_hit(); self.record_access(pk); self.freq_bump(pk);
                out[i] = Some(acc); continue;
            }
            if let Some(ce) = self.hot_shard_probe(pk).or_else(|| self.hot_global_probe(pk)) {
                let seg_idx = (ce.offset / SEGMENT_SIZE as u64) as usize;
                let seg_off = (ce.offset % SEGMENT_SIZE as u64) as usize;
                if seg_idx < self.segments.len() {
                    let mmap = self.segments[seg_idx].mmap.read();
                    let ad_sz = size_of::<AccountData>();
                    if seg_off + (ce.size as usize) <= mmap.len() && ce.size as usize >= ad_sz {
                        let must_crc = self.crc_should_check(pk, ce.offset);
                        if !must_crc || compute_checksum(&mmap[seg_off..seg_off + ce.size as usize]) == ce.checksum {
                            let ad: &AccountData = bytemuck::from_bytes(&mmap[seg_off..seg_off + ad_sz]);
                            let data_len = ad.data_len as usize;
                            let data_off = seg_off + ad_sz;
                            let data_end = data_off + data_len;
                            if data_end <= mmap.len() {
                                out[i] = Some(build_account_small_or_vec(ad, &mmap[data_off..data_end]));
                                self.stat_hit(); self.record_access(pk); self.freq_bump(pk);
                                continue;
                            }
                        } else { self.crc_escalate_on_fault(); }
                    }
                }
            }
            // fall through to index plan
        }

        // Index plan for misses
        let guard = epoch::pin();
        for (i, pk) in pubkeys.iter().enumerate() {
            if out[i].is_some() { continue; }
            if let Some(ea) = self.index.get(pk) {
                if let Some(er) = Self::load_entry(&*ea, &guard) {
                    let seg_idx = (er.offset / SEGMENT_SIZE as u64) as usize;
                    let seg_off = (er.offset % SEGMENT_SIZE as u64) as usize;
                    if seg_idx >= self.segments.len() { continue; }
                    let mmap = self.segments[seg_idx].mmap.read();
                    if seg_off + (er.size as usize) > mmap.len() { continue; }

                    self.crc_maybe_relax();
                    if self.crc_should_check(pk, er.offset) {
                        let crc = compute_checksum(&mmap[seg_off..seg_off + (er.size as usize)]);
                        if crc != er.checksum { self.crc_escalate_on_fault(); continue; }
                    }
                    plan.push(Hit { seg_idx, seg_off, size: er.size as usize, out_idx: i });
                }
            }
        }

        // stable sort by (seg_idx, seg_off)
        plan.sort_unstable_by(|a, b| (a.seg_idx, a.seg_off).cmp(&(b.seg_idx, b.seg_off)));

        // Batch per-segment with deeper prefetch
        let mut i = 0usize;
        while i < plan.len() {
            let sidx = plan[i].seg_idx;
            // [PATCH 58C] fadvise once per touched segment (best-effort)
            #[cfg(target_os = "linux")]
            {
                let mut first = plan[i].seg_off as i64;
                let mut last = plan[i].seg_off as i64;
                let mut k = i;
                while k < plan.len() && plan[k].seg_idx == sidx {
                    last = (plan[k].seg_off + plan[k].size) as i64;
                    k += 1;
                }
                let seg = &self.segments[sidx];
                unsafe { let _ = libc::posix_fadvise(seg.fd, first, last - first, libc::POSIX_FADV_WILLNEED); }
            }
            let mmap = self.segments[sidx].mmap.read();
            let mut j = i;
        let mut win = 0usize;
            while j < plan.len() && plan[j].seg_idx == sidx {
                let h = plan[j];
            if h.seg_off + h.size <= mmap.len() && h.size >= core::mem::size_of::<AccountData>() {
                    let base_ptr = unsafe { mmap.as_ptr().add(h.seg_off) };
                    prefetch_account_region(base_ptr, h.size);
                if (win & 3) == 0 {
                    if j + 1 < plan.len() && plan[j+1].seg_idx == sidx {
                        let nxt = plan[j+1];
                        let p2 = unsafe { mmap.as_ptr().add(nxt.seg_off) };
                        prefetch_account_region(p2, nxt.size);
                    }
                }
                    let ad: &AccountData = bytemuck::from_bytes(
                    &mmap[h.seg_off .. h.seg_off + core::mem::size_of::<AccountData>()]
                    );
                    let data_len = ad.data_len as usize;
                let data_off = h.seg_off + core::mem::size_of::<AccountData>();
                    let data_end = data_off + data_len;
                    if data_end <= mmap.len() {
                        out[h.out_idx] = Some(build_account_small_or_vec(ad, &mmap[data_off..data_end]));
                    }
                win = win.wrapping_add(1);
                }
                j += 1;
            }
        // batch bump
            let bumped: Vec<Pubkey> = (i..j)
                .filter_map(|k| out[plan[k].out_idx].as_ref().map(|_| pubkeys[plan[k].out_idx]))
                .cloned().collect();
            self.freq_bump_many(&bumped);
            i = j;
        }
        Ok(out)
    }

    // [PATCH 60] seed TLS hotline from L2 on second hit
    #[inline]
    fn hotline_seed_tls(&self, pk: &Pubkey, ce: &CacheEntry) {
        if let Some(ea) = self.index.get(pk) {
            let g = epoch::pin();
            if let Some(er) = Self::load_entry(&*ea, &g) {
                self.hotline_store(pk, er.offset, er.size as u32, er.checksum);
                return;
            }
        }
        self.hotline_store(pk, ce.offset, ce.size as u32, ce.checksum);
    }

    // [PATCH 65D/54] bounded maintenance with promotion ring drain
    pub fn maintenance_tick(&self, budget_us: u64) -> u32 {
        let start = mono_micros();
        let mut work = 0u32;
        // decay a slice of doorkeeper
        self.doorkeep_decay_step();
        work = work.wrapping_add(1);
        if mono_micros().wrapping_sub(start) >= budget_us { return work; }
        // drain promotion rings (per shard)
        let prs = get_prom_rings(self);
        for s in 0..prs.len() {
            while let Some(e) = prs[s].pop() {
                self.hot_shard_promote(&e);
                self.hot_global_promote(&e);
                work = work.wrapping_add(1);
                if mono_micros().wrapping_sub(start) >= budget_us { break; }
            }
            if mono_micros().wrapping_sub(start) >= budget_us { break; }
        }
        if mono_micros().wrapping_sub(start) >= budget_us { return work; }
        // [PATCH 83D] bounded quarantine reclaim with large-span siphon
        let now = mono_micros();
        let lr = get_large_rings(self);
        for r in self.quarantine.reclaim_due(now) {
            let seg = &self.segments[r.seg_idx];
            {
                let mut fl = seg.free_map.lock();
                fl.insert(r.off, r.len);
                // coalesce all
                let mut vec: Vec<(u64,u64)> = fl.iter().map(|(k,v)| (*k,*v)).collect();
                coalesce_vec(&mut vec);
                fl.clear();
                for (o,l) in vec.into_iter() {
                    if l >= 1 * 1024 * 1024 {
                        let shard = current_cpu() % self.numa_shards.max(1);
                        let _ = lr[shard].try_push(o, l);
                    } else {
                        fl.insert(o, l);
                    }
                }
            }
            #[cfg(target_os = "linux")]
            unsafe {
                let mg = seg.mmap.read();
                let base = mg.as_ptr().add(r.off as usize) as *mut c_void;
                let _ = libc::madvise(base, r.len as usize, libc::MADV_DONTNEED);
            }
            work = work.wrapping_add(1);
            if mono_micros().wrapping_sub(start) >= budget_us { return work; }
        }
        // relax CRC sampling state opportunistically
        self.crc_maybe_relax();
        work
    }

    // [PATCH 66] immediate RSS drop helper for large frees (Linux)
    #[cfg(target_os = "linux")]
    #[inline]
    fn drop_rss_large(&self, seg_idx: usize, seg_off: usize, len: usize) {
        if len >= 256 * 1024 {
            unsafe {
                let mg = self.segments[seg_idx].mmap.read();
                let p = mg.as_ptr().add(seg_off) as *mut libc::c_void;
                let _ = libc::madvise(p, len, libc::MADV_DONTNEED);
            }
        }
    }
}
    // -------------- [PATCH 45: get_into_buf() zero-alloc reader] -------------------
    /// Fill `dst` with the account's data payload (not including AccountData header).
    /// Returns (lamports, owner, executable, rent_epoch) on success.
    pub fn get_into_buf(&self, pubkey: &Pubkey, dst: &mut Vec<u8>) -> Result<Option<(u64, Pubkey, bool, u64)>> {
        let try_extract = |mmap: &MmapMut, seg_off: usize, size: usize| -> Option<(u64, Pubkey, bool, u64)> {
            let ad_sz = core::mem::size_of::<AccountData>();
            if size < ad_sz || seg_off + size > mmap.len() { return None; }
            let ad: &AccountData = bytemuck::from_bytes(&mmap[seg_off .. seg_off + ad_sz]);
            let data_len = ad.data_len as usize;
            let data_off = seg_off + ad_sz;
            let data_end = data_off + data_len;
            if data_end > mmap.len() { return None; }
            dst.clear();
            dst.extend_from_slice(&mmap[data_off..data_end]);
            Some((ad.lamports, Pubkey::new_from_array(ad.owner), ad.executable != 0, ad.rent_epoch))
        };

        if let Some(acc) = self.hotline_try_get(pubkey) {
            let (lamports, owner, exec, rent) = (acc.lamports(), *acc.owner(), acc.executable(), acc.rent_epoch());
            dst.clear(); dst.extend_from_slice(acc.data());
            self.stat_hit(); self.record_access(pubkey); self.freq_bump(pubkey);
            return Ok(Some((lamports, owner, exec, rent)));
        }

        if let Some(ce) = self.hot_shard_probe(pubkey).or_else(|| self.hot_global_probe(pubkey)) {
            let seg_idx = (ce.offset / SEGMENT_SIZE as u64) as usize;
            let seg_off = (ce.offset % SEGMENT_SIZE as u64) as usize;
            if seg_idx < self.segments.len() {
                let mmap = self.segments[seg_idx].mmap.read();
                if !self.crc_should_check(pubkey, ce.offset) ||
                    compute_checksum(&mmap[seg_off..seg_off + (ce.size as usize)]) == ce.checksum {
                    if let Some(meta) = try_extract(&*mmap, seg_off, ce.size as usize) {
                        self.stat_hit(); self.record_access(pubkey); self.freq_bump(pubkey);
                        return Ok(Some(meta));
                    }
                } else { self.crc_escalate_on_fault(); }
            }
        }

        let guard = epoch::pin();
        if let Some(entry_atomic) = self.index.get(pubkey) {
            if let Some(er) = Self::load_entry(&*entry_atomic, &guard) {
                let seg_idx = (er.offset / SEGMENT_SIZE as u64) as usize;
                let seg_off = (er.offset % SEGMENT_SIZE as u64) as usize;
                if seg_idx < self.segments.len() {
                    let mmap = self.segments[seg_idx].mmap.read();
                    self.crc_maybe_relax();
                    if !self.crc_should_check(pubkey, er.offset) ||
                        compute_checksum(&mmap[seg_off..seg_off + (er.size as usize)]) == er.checksum {
                        if let Some(meta) = try_extract(&*mmap, seg_off, er.size as usize) {
                            self.stat_hit(); self.record_access(pubkey); self.freq_bump(pubkey);
                            return Ok(Some(meta));
                        }
                    } else { self.crc_escalate_on_fault(); self.stat_miss(); }
                }
            }
        }
        self.stat_miss();
        Ok(None)
    }
    // ---------------- [/PATCH 45] ---------------------------------------------------

    // ... (rest of the code remains the same)

    pub fn insert(&self, pubkey: &Pubkey, account: &AccountSharedData, slot: Slot) -> Result<()> {
        if self.upsert_in_place(pubkey, account, slot).is_some() { return Ok(()); }
        let data = account.data();
        let total_size = size_of::<AccountData>() + data.len();
        if total_size > SEGMENT_SIZE / 4 {
            // [UPGRADE] refuse pathological objects
            return Ok(());
        }
        let (segment_idx, abs_offset) = self.allocate_space(total_size, pubkey)?;
        let segment = &self.segments[segment_idx];
        let mut mg = segment.mmap.write();
        let seg_off = (abs_offset % SEGMENT_SIZE as u64) as usize;

        let header_bytes = bytemuck::bytes_of(&account.header());
        debug_assert!((seg_off & (CACHE_ALIGN - 1)) == 0);
        let data_off = seg_off + header_bytes.len();
        debug_assert!((seg_off & (CACHE_ALIGN - 1)) == 0);
        // [PATCH 114B] fused payload copy + CRC; extend with header
        let mut checksum_store: u32;
        unsafe {
            let base = mg.as_mut_ptr().add(seg_off);
            std::ptr::copy_nonoverlapping(header_bytes.as_ptr(), base, header_bytes.len());
            let crc_payload_final = copy_and_crc_optimized(base.add(header_bytes.len()), account.data().as_ptr(), account.data().len());
            persist_range_if_enabled(base, total_size);
            demote_written_region(base, total_size);
            let running = !crc_payload_final;
            let extended = crc32c_extend_state(running, header_bytes.as_ptr(), header_bytes.len());
            checksum_store = crc32c_finalize(extended);
        }

        let entry = CacheEntry {
            pubkey: pubkey.to_bytes(),
            offset: abs_offset,
            size: (header_bytes.len() + data.len()) as u64,
            slot,
            last_access_us: mono_micros(),
            checksum: checksum_store,
            flags: 0,
            rev: self.next_rev_for(pubkey, &epoch::pin()),
        };
        self.publish_fresh(pubkey, entry);

        // Async flush large bodies to keep writeback progressing
        #[cfg(target_os = "linux")]
        if total_size >= FLUSH_ASYNC_THRESH {
            self.flush_async_range(segment_idx, seg_off, total_size);
        }

        self.hdr_add(total_size as i64, 1, Some(slot));

        self.hotline_store(pubkey, abs_offset, total_size as u32, entry.checksum);

        // [PATCH 35] precise total_size accounting
        self.total_size.fetch_add(total_size, Ordering::Relaxed);

        // [PATCH 40] note write progress for async coalesced flush
        #[cfg(target_os = "linux")]
        {
            let end_off_seg_local = (seg_off + total_size) as u64;
            self.note_write_progress(segment_idx, end_off_seg_local);
        }

        self.publish_after_write();

        Ok(())
    }

    // ... (rest of the code remains the same)

    pub fn flush(&self) -> Result<()> {
        for s in &self.segments {
            let mmap = s.mmap.read();
            mmap.flush().with_context(|| "mmap flush")?;
        }
        Ok(())
    }

    // ... (rest of the code remains the same)

    pub fn snapshot(&self, path: impl AsRef<Path>) -> Result<()> {
        let snapshot_dir = path.as_ref();
        std::fs::create_dir_all(snapshot_dir)?;
        let metadata = SnapshotMetadata {
            version: CACHE_VERSION,
            timestamp: mono_micros(),
            entry_count: self.index.len() as u64,
            total_size: self.total_size.load(Ordering::Relaxed) as u64,
            segments: self.segments.len() as u32,
        };
        std::fs::write(snapshot_dir.join("meta.bin"), bytemuck::bytes_of(&metadata))?;

        // index dump
        let mut idx_bytes = Vec::with_capacity(self.index.len() * size_of::<CacheEntry>());
        {
            let guard = epoch::pin();
            for e in self.index.iter() {
                if let Some(er) = Self::load_entry(&*e.value(), &guard) {
                    idx_bytes.extend_from_slice(bytemuck::bytes_of(er));
                }
            }
        }
        // publish fence to make payload visible before index publish
        self.publish_after_write();

        // retire old region after publish
        let guard = epoch::pin();
        for e in self.index.iter() {
            if let Some(old) = Self::load_entry(&*e.value(), &guard) {
                let seg_idx_old = (old.offset / SEGMENT_SIZE as u64) as usize;
                let seg_off_old = (old.offset % SEGMENT_SIZE as u64) as u64;
                self.quarantine.push(FreedRegion { seg_idx: seg_idx_old, off: seg_off_old, len: old.size, freed_us: mono_micros() });
            }
        }

        std::fs::write(snapshot_dir.join("index.bin"), &idx_bytes)?;

        // segments
        for (i, seg) in self.segments.iter().enumerate() {
            let p = snapshot_dir.join(format!("segment_{:04}.snap", i));
            let mg = seg.mmap.read();
            std::fs::write(&p, &*mg)?;
        }

        // Seal snapshot
        let mut seal = Crc32Fast::new();
        seal.update(bytemuck::bytes_of(&metadata));
        seal.update(&idx_bytes);
        for i in 0..self.segments.len() {
            let p = snapshot_dir.join(format!("segment_{:04}.snap", i));
            if p.exists() { seal.update(&std::fs::read(p)?); }
        }
        std::fs::write(snapshot_dir.join("seal.crc32"), seal.finalize().to_le_bytes())?;
        Ok(())
    }

    // ... (rest of the code remains the same)

    pub fn restore(&mut self, path: impl AsRef<Path>) -> Result<()> {
        let snapshot_dir = path.as_ref();
        let meta: SnapshotMetadata = {
            let bytes = std::fs::read(snapshot_dir.join("meta.bin"))?;
            *bytemuck::from_bytes(&bytes)
        };

        // Verify seal first
        let seal_path = snapshot_dir.join("seal.crc32");
        let seal_bytes = std::fs::read(&seal_path)?;
        let expected = u32::from_le_bytes(seal_bytes.try_into()?);

        let mut verify = Crc32Fast::new();
        verify.update(bytemuck::bytes_of(&meta));
        let idx_bytes = std::fs::read(snapshot_dir.join("index.bin"))?;
        verify.update(&idx_bytes);
        for i in 0..self.segments.len() {
            let p = snapshot_dir.join(format!("segment_{:04}.snap", i));
            if p.exists() { verify.update(&std::fs::read(p)?); }
        }
        if verify.finalize() != expected {
            return Err(anyhow::anyhow!("snapshot seal verification failed"));
        }

        // Load index
        self.index.clear();
        for chunk in idx_bytes.chunks_exact(size_of::<CacheEntry>()) {
            let ce: CacheEntry = *bytemuck::from_bytes(chunk);
            self.publish_entry(&Pubkey::new_from_array(ce.pubkey), ce, false);
        }
        Ok(())
    }

    // ... (rest of the code remains the same)

    pub fn insert(&self, pubkey: &Pubkey, account: &AccountSharedData, slot: Slot) -> Result<()> {
        // ... (rest of the code remains the same)
                        let ptr = unsafe { mmap.as_ptr().add(seg_off) };
                        cpu_prefetch(ptr);
                        cpu_prefetch(unsafe { ptr.add(64) });
                    }
                }
            }
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<bool> {
        let guard = epoch::pin();
        for e in self.index.iter() {
            if let Some(er) = Self::load_entry(&*e.value(), &guard) {
                let seg_idx = (er.offset / SEGMENT_SIZE as u64) as usize;
                if seg_idx >= self.segments.len() { return Ok(false); }
                let seg_off = (er.offset % SEGMENT_SIZE as u64) as usize;
                if seg_off + er.size as usize > SEGMENT_SIZE { return Ok(false); }

                if (mono_micros() ^ (er.offset as u64)) & 0xFF == 0 {
                    let seg = &self.segments[seg_idx];
                    let mg = seg.mmap.read();
                    let end = seg_off + (er.size as usize);
                    if end <= mg.len() {
                        let crc = compute_checksum(&mg[seg_off..end]);
                        if crc != er.checksum { return Ok(false); }
                    }
                }
            }
        }
        Ok(true)
    }

    // -------------------------- [UPGRADE: eviction] --------------------------
    #[inline]
    fn should_evict(&self) -> bool {
        let used = self.total_size.load(Ordering::Relaxed) as u64;
        if used <= (MAX_CACHE_SIZE as u64 * 95 / 100) { return false; }
        let mut t = self.last_eviction.lock();
        let now = Instant::now();
        if now.duration_since(*t) < Duration::from_micros(500) { return false; }
        *t = now;
        true
    }

    // ---------------------- [UPGRADE++: entry_meta helper] ----------------------
    #[inline]
    fn entry_meta(&self, pk: &Pubkey, g: &Guard) -> Option<(u64, u64)> {
        if let Some(a) = self.index.get(pk) {
            if let Some(er) = unsafe { a.load(Ordering::Acquire, g).as_ref() } {
                let age = mono_micros().saturating_sub(
                    self.last_access_us
                        .get(pk).map(|x| x.load(Ordering::Relaxed)).unwrap_or(er.last_access_us)
                );
                return Some((er.size, age));
            }
        }
        None
    }

    // -------------------- [UPGRADE++: eviction size-aware] --------------------
    fn evict_adaptive(&self) -> Result<()> {
        // Compute target: drop below 92% occupancy if we exceed 95%.
        let used = self.total_size.load(Ordering::Relaxed) as u64;
        let cap  = (MAX_CACHE_SIZE as u64);
        if used <= cap * 95 / 100 { return Ok(()); }
        let target = used.saturating_sub(cap * 92 / 100);

        // Build candidate list (same sources as before)
        let mut victims: Vec<Pubkey> = Vec::with_capacity(EVICTION_BATCH_SIZE * 2);
        {
            let mut sr = self.slot_index.write();
            let end = sr.base + (SLOT_HISTORY_SIZE as u64 / 4);
            let mut slot = sr.base;
            while slot < end && victims.len() < victims.capacity() {
                for pk in sr.take_slot(slot) {
                    victims.push(pk);
                    if victims.len() >= victims.capacity() { break; }
                }
                slot += 1;
            }
            sr.advance_base(slot);
        }
        if victims.len() < victims.capacity()/2 {
            for (pk, _) in self.index.iter().take(victims.capacity() - victims.len()) {
                victims.push(*pk);
            }
        }

        // Score and evict until we free enough bytes
        let g = epoch::pin();
        let mut scored: Vec<(Pubkey, u32, u64, u64)> = Vec::with_capacity(victims.len());
        for pk in victims {
            let f = self.freq_estimate(&pk);
            if let Some((sz, age)) = self.entry_meta(&pk, &g) {
                scored.push((pk, f, age, sz));
            }
        }
        scored.sort_unstable_by(|a, b| (a.1, std::cmp::Reverse(a.2), std::cmp::Reverse(a.3))
            .cmp(&(b.1, std::cmp::Reverse(b.2), std::cmp::Reverse(b.3))));

        let mut freed = 0u64;
        let mut ev = 0u64;
        for (pk, f, _, sz) in scored.into_iter() {
            if freed >= target { break; }
            if f > 2 { continue; }
            if self.remove(&pk).is_ok() { freed = freed.saturating_add(sz); ev += 1; }
        }
        if ev > 0 { self.eviction_count.fetch_add(ev, Ordering::Relaxed); }
        Ok(())
    }

    // ------------------------- [UPGRADE: stats()] -----------------------------
    #[inline]
    pub fn stats(&self) -> CacheStats {
        let hits = self.hit_count.load(Ordering::Relaxed);
        let misses = self.miss_count.load(Ordering::Relaxed);
        let total = hits + misses;
        CacheStats {
            total_size: self.total_size.load(Ordering::Relaxed),
            entry_count: self.index.len(),
            hit_count: hits,
            miss_count: misses,
            eviction_count: self.eviction_count.load(Ordering::Relaxed),
            hit_rate: if total == 0 { 1.0 } else { hits as f64 / total as f64 },
        }
    }
}

impl Drop for MemoryMappedStateCache {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}

unsafe impl Send for MemoryMappedStateCache {}
unsafe impl Sync for MemoryMappedStateCache {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_cache_operations() {
        let tmp = TempDir::new().unwrap();
        let cache = MemoryMappedStateCache::new(tmp.path()).unwrap();

        let pk = Pubkey::new_unique();
        let account = AccountSharedData::new(1000, 32, &Pubkey::default());
        cache.insert(&pk, &account, 1).unwrap();

        let fetched = cache.get(&pk).unwrap().unwrap();
        assert_eq!(fetched.lamports(), 1000);
        assert_eq!(fetched.data().len(), 32);

        cache.remove(&pk).unwrap();
        assert!(cache.get(&pk).unwrap().is_none());
    }

    #[test]
    fn test_snapshot_restore() {
        let tmp = TempDir::new().unwrap();
        let cache_dir = tmp.path().join("cache");
        let snap_dir = tmp.path().join("snapshot");

        let mut cache = MemoryMappedStateCache::new(&cache_dir).unwrap();
        for i in 0..64 {
            let pk = Pubkey::new_unique();
            let account = AccountSharedData::new(i * 111, 64, &Pubkey::default());
            cache.insert(&pk, &account, i as u64).unwrap();
        }
        cache.snapshot(&snap_dir).unwrap();

        let mut restored = MemoryMappedStateCache::new(&cache_dir).unwrap();
        restored.restore(&snap_dir).unwrap();
        assert_eq!(cache.stats().entry_count, restored.stats().entry_count);
        assert!(restored.validate().unwrap());
    }
}
