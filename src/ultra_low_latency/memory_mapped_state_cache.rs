#![allow(dead_code, unused_imports, unused_variables, clippy::too_many_arguments, clippy::type_complexity)]
// Single-file extract with HYPER-OPT upgrades, centralized feature caps, and surgical provenance tags.
// Canonical source pack (for reviewers):
//   [SDM-V2/V3/V4] Intel® 64 and IA-32 Architectures Software Developer’s Manual (Vol.2 Instr., Vol.3 System, Vol.4 MSRs)
//   [IG]          Intel Intrinsics Guide (AVX2/AVX-512/VBMI/BMI2/AES/NT stores/masks)
//   [OPT]         Intel Optimization Reference Manual (streaming/NT stores/prefetch/fences)
//   [XCLOU]       Felix Cloutier x86 ref (UMONITOR/UMWAIT/TPAUSE/RDTSCP quick refs; mirrors SDM semantics)
//   [MAN]         Linux man-pages: getcpu/sched_getcpu(2/3), mlockall(2), pthread_setaffinity_np(3),
//                 sched_setscheduler(2), setpriority(2), madvise(2), mbind(2)
//   [RST]         Rust stdarch + reference (is_x86_feature_detected!, is_aarch64_feature_detected!, target_feature)
//   [A64]         Rust aarch64 AES intrinsics (vaeseq_u8, vaesmcq_u8)

use std::sync::OnceLock;

#[derive(Copy, Clone, Debug)]
pub struct Caps {
    pub avx2: bool,
    pub avx512f: bool,
    pub avx512bw: bool,
    pub avx512vbmi: bool,
    pub cldemote: bool,
    pub bmi2: bool,
    pub rtm: bool,
    pub waitpkg: bool,
    pub aes_a64: bool,
    pub umwait_min_cycles: u32,  // clamped from IA32_UMWAIT_CONTROL (official) or env fallback [SDM-V4: IA32_UMWAIT_CONTROL]
}

static CAPS: OnceLock<Caps> = OnceLock::new();

#[inline(always)]
fn read_umwait_min_cycles() -> u32 {
    // Primary truth: IA32_UMWAIT_CONTROL MSR — user-wait enable + minimum residency [SDM-V4: IA32_UMWAIT_CONTROL].
    // WAITPKG semantics (#UD if user-wait disabled) [SDM-V2 UMONITOR/UMWAIT/TPAUSE][XCLOU].
    // CPUID.(EAX=7,ECX=0):ECX[5] advertises WAITPKG [SDM-V2 CPUID][XCLOU].
    if let Ok(v) = std::env::var("SC_UMWAIT_MIN") {
        if let Ok(x) = v.parse::<u32>() { return x; }
    }
    #[cfg(all(target_arch = "x86_64", target_os = "linux"))]
    {
        // Best-effort MSR read via /dev/cpu/<cpu>/msr [MAN: msr(4), open(2), pread(2)].
        // UMWAIT control MSR index:
        const IA32_UMWAIT_CONTROL: u64 = 0xE1; // [SDM-V4: MSR 0xE1] “UMWAIT minimum residency clamp”
        unsafe {
            // Select correct /dev/cpu/N/msr by current CPU [MAN: sched_getcpu(3)]
            let cpu = libc::sched_getcpu();
            if cpu >= 0 {
                let path = format!("/dev/cpu/{}/msr", cpu);
                use std::os::unix::io::AsRawFd;
                if let Ok(f) = std::fs::File::open(&path) {
                    use std::os::unix::fs::FileExt;
                    let mut buf = [0u8; 8];
                    if f.read_at(&mut buf, IA32_UMWAIT_CONTROL as u64).is_ok() {
                        let raw = u64::from_le_bytes(buf);
                        let min_cycles = (raw & 0xFFFF_FFFF) as u32; // residency clamp field [SDM-V4 IA32_UMWAIT_CONTROL]
                        if min_cycles != 0 { return min_cycles; }
                    }
                }
            }
        }
    }
    // Fallback if MSR unavailable.
    500
}

#[inline(always)]
fn detect_caps() -> Caps {
    #[cfg(target_arch = "x86_64")]
    unsafe fn cpuid_has_waitpkg() -> bool {
        // CPUID.(EAX=07H, ECX=0):ECX[5] = WAITPKG [SDM-V2 CPUID leaf7][XCLOU].
        let mut eax: u32 = 7;
        let mut ebx: u32;
        let mut ecx: u32 = 0;
        let mut edx: u32;
        core::arch::asm!(
            "cpuid",
            inout("eax") eax, out("ebx") ebx, inout("ecx") ecx, out("edx") edx,
            options(nomem, nostack, preserves_flags)
        );
        ((ecx >> 5) & 1) != 0
    }

    #[cfg(target_arch = "x86_64")]
    {
        // Runtime feature detection [RST: is_x86_feature_detected!].
        let avx2        = core::is_x86_feature_detected!("avx2");          // [RST]
        let avx512f     = core::is_x86_feature_detected!("avx512f");       // [RST]
        let avx512bw    = core::is_x86_feature_detected!("avx512bw");      // [RST]
        let avx512vbmi  = core::is_x86_feature_detected!("avx512vbmi");    // [RST]
        let cldemote    = core::is_x86_feature_detected!("cldemote");      // CLDEMOTE hint [SDM-V2]
        let bmi2        = core::is_x86_feature_detected!("bmi2");          // BMI2 / PEXT path [RST][IG]
        let rtm         = core::is_x86_feature_detected!("rtm");           // TSX RTM [RST][IG]
        let waitpkg     = cpuid_has_waitpkg();                             // WAITPKG bit [SDM-V2]
        Caps {
            avx2, avx512f, avx512bw, avx512vbmi, cldemote, bmi2, rtm, waitpkg,
            aes_a64: false,
            umwait_min_cycles: read_umwait_min_cycles(),
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        // AArch64 AES extension detection [RST: is_aarch64_feature_detected!]
        let aes_a64 = std::arch::is_aarch64_feature_detected!("aes"); // [RST]
        Caps {
            avx2: false, avx512f: false, avx512bw: false, avx512vbmi: false,
            cldemote: false, bmi2: false, rtm: false, waitpkg: false, aes_a64,
            umwait_min_cycles: 0,
        }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    {
        Caps {
            avx2: false, avx512f: false, avx512bw: false, avx512vbmi: false,
            cldemote: false, bmi2: false, rtm: false, waitpkg: false, aes_a64: false,
            umwait_min_cycles: 0,
        }
    }
}
#[inline(always)]
fn caps() -> &'static Caps { CAPS.get_or_init(detect_caps) }

// ==============================================================================================
// Logger + one-shot self-report of selected fast paths
// ==============================================================================================
#[inline(always)]
fn log_once_env(key: &str, val: impl core::fmt::Display) {
    static DID: OnceLock<std::collections::HashSet<&'static str>> = OnceLock::new();
    let set = DID.get_or_init(|| std::collections::HashSet::new());
    if !set.contains(key) {
        eprintln!("[SC] {} = {}", key, val);
        let k: &'static str = Box::leak(key.to_string().into_boxed_str());
        unsafe { (*(set as *const _ as *mut std::collections::HashSet<&'static str>)).insert(k); }
    }
}

#[inline(always)]
fn report_caps_once() {
    let c = caps();
    log_once_env("SC_CAPS",
        format!(
            "avx2={} avx512f={} avx512bw={} vbmi={} bmi2={} cldemote={} rtm={} waitpkg={} aes_a64={}",
            c.avx2, c.avx512f, c.avx512bw, c.avx512vbmi, c.bmi2, c.cldemote, c.rtm, c.waitpkg, c.aes_a64
        )
    );
}

// ============================================================================
// >>> HYPER-OPT HIGHLIGHT BEGIN: 64B-aligned atomics wrapper (avoid false sharing)
#[repr(align(64))]
struct Al64<T>(T);
impl<T> core::ops::Deref for Al64<T> { type Target = T; #[inline] fn deref(&self) -> &T { &self.0 } }
impl<T> core::ops::DerefMut for Al64<T> { #[inline] fn deref_mut(&mut self) -> &mut T { &mut self.0 } }
// <<< HYPER-OPT HIGHLIGHT END
// ============================================================================

// ============================================================================
// >>> HYPER-OPT HIGHLIGHT BEGIN: AVX-512 VBMI header equality (unaligned, 64B lanes)
#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn eq_header_impl_avx512_vbmi(a: *const u8, b: *const u8, len: usize) -> bool {
    // VBMI permute-by-index + compare mask [IG: _mm512_permutex2var_epi8, _mm512_cmpeq_epi8_mask]
    use core::arch::x86_64::{__m512i,_mm512_cmpeq_epi8_mask,_mm512_loadu_si512,_mm512_permutex2var_epi8};
    if len == 0 { return true; }
    #[inline(always)]
    unsafe fn idx_for_shift(sh: usize) -> __m512i {
        let mut idx = [0u8; 64];
        let mut i = 0usize;
        while i < 64 {
            let off = sh + i;
            idx[i] = if off < 64 { off as u8 } else { 0x80 | ((off - 64) as u8) };
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
        let va: __m512i = _mm512_permutex2var_epi8(a0, ia, a1); // [IG] “VBMI permute-by-index”
        let vb: __m512i = _mm512_permutex2var_epi8(b0, ib, b1); // [IG] “VBMI permute-by-index”
        if _mm512_cmpeq_epi8_mask(va, vb) != !0u64 { return false; } // [IG] “64-byte compare mask”
        i += 64;
    }
    eq_header_impl_avx512(a.add(i), b.add(i), len - i)
}
// <<< HYPER-OPT HIGHLIGHT END
// ============================================================================

// ============================================================================
// >>> HYPER-OPT HIGHLIGHT BEGIN: warm-touch pages to fault-in memory
#[inline(always)]
fn warm_touch_pages(ptr: *mut u8, len: usize) {
    // Standard page pre-fault trick (portable best-effort).
    if std::env::var("SC_WARM_PAGES").ok().as_deref() != Some("1") { return; }
    unsafe {
        let ps: usize = {
            #[cfg(target_os = "linux")]
            { libc::sysconf(libc::_SC_PAGESIZE) as usize }  // [MAN: sysconf(3)]
            #[cfg(not(target_os = "linux"))]
            { 4096usize }
        };
        let mut off = 0usize;
        while off < len {
            core::ptr::read_volatile(ptr.add(off));
            off = off.wrapping_add(ps);
        }
        if off != len && len > 0 { core::ptr::read_volatile(ptr.add(len - 1)); }
    }
}
// <<< HYPER-OPT HIGHLIGHT END
// ============================================================================

// ============================================================================
// >>> HYPER-OPT HIGHLIGHT BEGIN: precomputed BMI2 PEXT mask bank + picker
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn pext_mask_bank() -> &'static [(u64,u64,u64); 16] {
    // BMI2 PEXT masks used with _pext_u64 [IG: _pext_u64]
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
// <<< HYPER-OPT HIGHLIGHT END
// ============================================================================

// ============================================================================
// >>> HYPER-OPT HIGHLIGHT BEGIN: per-socket salt from RDTSCP + CPU/node
#[inline(always)]
fn salt_per_socket(base: u64) -> u64 {
    let slice_us: u64 = std::env::var("SC_SEED_SLICE_US").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(1_000_000);
    #[cfg(target_arch = "x86_64")]
    unsafe {
        // RDTSCP: EDX:EAX=TSC, ECX=IA32_TSC_AUX (OS-defined packing) [SDM-V2 RDTSCP][XCLOU].
        use core::arch::x86_64::_rdtscp;
        let mut aux: u32 = 0;
        let _ = _rdtscp(&mut aux); // “EDX:EAX=TSC, ECX=TSC_AUX; AUX layout OS-defined.” [SDM-V2][XCLOU]
        let cpu = (aux & 0x0FFF) as u64;        // common Linux convention (not guaranteed)
        let node = ((aux as u64) >> 12) & 0xFF; // common Linux convention (not guaranteed)
        let now = mono_micros_fast() as u64;
        let slice = (now / slice_us).rotate_left(13);
        return base ^ cpu ^ (node << 24) ^ slice.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
    let now = mono_micros_fast() as u64;
    base ^ (now / slice_us).rotate_left(17)
}
// <<< HYPER-OPT HIGHLIGHT END
// ============================================================================

// ============================================================================
// >>> HYPER-OPT HIGHLIGHT BEGIN: Intel WAITPKG tpause with residency clamp
#[inline(always)]
fn tpause_cycles(mut cycles: u32) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        if !caps().waitpkg { return; }
        // TPAUSE (WAITPKG) availability CPUID.7.0:ECX[5]; #UD if user-wait disabled; clamp to IA32_UMWAIT_CONTROL [SDM-V2/V4][XCLOU].
        let min_cy = caps().umwait_min_cycles;       // “IA32_UMWAIT_CONTROL residency clamp; #UD if user-wait disabled; CPUID.7.0:ECX=WAITPKG.”
        if cycles < min_cy { cycles = min_cy; }      // clamp to minimum residency [SDM-V4]
        core::arch::asm!(
            "xor edx, edx",
            "mov eax, {0:e}",
            "tpause",
            in(reg) cycles,
            options(nostack, preserves_flags)
        );
    }
}
// <<< HYPER-OPT HIGHLIGHT END
// ============================================================================

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
    // AVX2 equality path [IG: _mm256_cmpeq_epi8 + _mm256_movemask_epi8]
    use core::arch::x86_64::{__m256i,_mm256_cmpeq_epi8,_mm256_loadu_si256,_mm256_movemask_epi8};
    let mut i = 0usize;
    while i + 32 <= len {
        let va = _mm256_loadu_si256(a.add(i) as *const __m256i);
        let vb = _mm256_loadu_si256(b.add(i) as *const __m256i);
        if _mm256_movemask_epi8(_mm256_cmpeq_epi8(va, vb)) != -1 { return false; } // [IG]
        i += 32;
    }
    eq_header_impl_portable(a.add(i), b.add(i), len - i)
}
#[cfg(target_arch = "x86_64")]
#[inline(always)]
unsafe fn eq_header_impl_avx512(a: *const u8, b: *const u8, len: usize) -> bool {
    // AVX-512 equality [IG: _mm512_cmpeq_epi8_mask]
    use core::arch::x86_64::{__m512i,_mm512_cmpeq_epi8_mask,_mm512_loadu_si512,_mm512_maskz_loadu_epi8};
    if len == 0 { return true; }
    let mut i = 0usize;
    while i + 64 <= len {
        let va: __m512i = _mm512_loadu_si512(a.add(i) as *const __m512i);
        let vb: __m512i = _mm512_loadu_si512(b.add(i) as *const __m512i);
        if _mm512_cmpeq_epi8_mask(va, vb) != !0u64 { return false; } // [IG]
        i += 64;
    }
    let tail = len - i; if tail == 0 { return true; }
    let mask = (!0u64) >> (64 - tail as u64);
    let va = _mm512_maskz_loadu_epi8(mask, a.add(i) as *const _);
    let vb = _mm512_maskz_loadu_epi8(mask, b.add(i) as *const _);
    _mm512_cmpeq_epi8_mask(va, vb) == !0u64 // [IG]
}
#[cfg(target_arch = "aarch64")]
#[inline(always)]
unsafe fn eq_header_impl_neon(a: *const u8, b: *const u8, len: usize) -> bool {
    // NEON equality [RST/A64: vceqq_u8 + vminvq_u8]
    use core::arch::aarch64::{uint8x16_t,vceqq_u8,vld1q_u8,vminvq_u8};
    let mut i = 0usize;
    while i + 16 <= len {
        let va: uint8x16_t = vld1q_u8(a.add(i));
        let vb: uint8x16_t = vld1q_u8(b.add(i));
        if vminvq_u8(vceqq_u8(va, vb)) != 255 { return false; } // [A64]
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
    report_caps_once();
    #[cfg(target_arch = "x86_64")]
    {
        let c = caps();
        if c.avx512vbmi && c.avx512bw && c.avx512f {
            return |a,b,l| unsafe { eq_header_impl_avx512_vbmi(a,b,l) }; // [IG: VBMI path]
        }
        if c.avx512f && c.avx512bw { return |a,b,l| unsafe { eq_header_impl_avx512(a,b,l) }; } // [IG]
        if c.avx2 { return |a,b,l| unsafe { eq_header_impl_avx2(a,b,l) }; }                    // [IG]
    }
    #[cfg(target_arch = "aarch64")] { return |a,b,l| unsafe { eq_header_impl_neon(a,b,l) }; }  // [A64]
    |_a,_b,_l| unsafe { eq_header_impl_portable(_a,_b,_l) }
}
#[inline(always)]
fn select_eq_big_impl() -> EqBigFn {
    #[cfg(target_arch = "x86_64")]
    {
        let c = caps();
        if c.avx512f && c.avx512bw { return |a,b,l| unsafe { eq_big_impl_avx512(a,b,l) }; }    // [IG]
        if c.avx2 { return |a,b,l| unsafe { eq_big_impl_avx2(a,b,l) }; }                       // [IG]
    }
    #[cfg(target_arch = "aarch64")] { return |a,b,l| unsafe { eq_big_impl_neon(a,b,l) }; }     // [A64]
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
// <<< HYPER-OPT HIGHLIGHT END
// ============================================================================

// ============================================================================
// >>> HYPER-OPT HIGHLIGHT BEGIN: AVX-512 scatter/striped batched doorkeeper (abridged)
use core::sync::atomic::Ordering;

#[derive(Copy, Clone)]
struct DkAgg { idx: usize, mask: u64, used: bool, old: u64 }

#[derive(Copy, Clone)]
struct DkLanes { w: [usize; 3], b: [u64; 3] }

#[inline(always)]
fn dk_lane_index3_batch_fast(keys: &[Pubkey], seed: u64, words: usize, out: &mut [DkLanes]) {
    for (i, pk) in keys.iter().enumerate() {
        let [(w0,b0),(w1,b1),(w2,b2)] = dk_lane_index3(pk, seed, words);
        out[i] = DkLanes { w:[w0,w1,w2], b:[b0,b1,b2] };
    }
}

#[inline(always)]
fn dk_test_and_set_seeded_batch_fast(
    words: &[core::sync::atomic::AtomicU64],
    keys: &[Pubkey],
    seed: u64,
    out_hits: &mut [bool],
) {
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
    for L in lanes.iter() { agg_insert(&mut tbl, L.w[0], L.b[0]); agg_insert(&mut tbl, L.w[1], L.b[1]); agg_insert(&mut tbl, L.w[2], L.b[2]); }
    for e in &mut tbl { if e.used { e.old = words[e.idx].fetch_or(e.mask, Ordering::AcqRel); } }
    #[inline(always)]
    fn lookup(tbl: &[DkAgg; CAP], idx: usize) -> u64 {
        let mut h = (idx.wrapping_mul(0x9E37_79B9)) & (CAP - 1);
        loop { let e = &tbl[h]; if e.used && e.idx == idx { return e.old; } h = (h + 1) & (CAP - 1); }
    }
    for (i, L) in lanes.iter().enumerate() {
        let o0 = lookup(&tbl, L.w[0]); let o1 = lookup(&tbl, L.w[1]); let o2 = lookup(&tbl, L.w[2]);
        out_hits[i] = ((o0 & L.b[0]) != 0) & ((o1 & L.b[1]) != 0) & ((o2 & L.b[2]) != 0);
    }
}
// <<< HYPER-OPT HIGHLIGHT END
// ============================================================================

// ============================================================================
// >>> HYPER-OPT HIGHLIGHT BEGIN: Linux NUMA detection and binding helpers
#[cfg(target_os = "linux")]
#[inline(always)]
fn linux_current_node() -> Option<u32> {
    unsafe {
        // getcpu(2)/sched_getcpu(3) [MAN: getcpu(2), sched_getcpu(3)]
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
    // mbind(MPOL_PREFERRED) [MAN: mbind(2)]
    if std::env::var("SC_NUMA_BIND").ok().as_deref() != Some("1") { return; }
    if len == 0 { return; }
    unsafe {
        let ps = libc::sysconf(libc::_SC_PAGESIZE) as usize; // [MAN: sysconf(3)]
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
        ); // [MAN: mbind(2)]
    }
}
// <<< HYPER-OPT HIGHLIGHT END
// ============================================================================

// ============================================================================
// >>> HYPER-OPT HIGHLIGHT BEGIN: RT isolation, affinity, mlock, policy, nice
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
        let _ = libc::mlockall(libc::MCL_CURRENT | libc::MCL_FUTURE);                       // [MAN: mlockall(2)]
        if let Ok(spec) = std::env::var("SC_AFFINITY") {
            let cpus = parse_cpu_list(&spec);
            if !cpus.is_empty() {
                let mut set: libc::cpu_set_t = core::mem::zeroed();
                for c in cpus {
                    // pthread_setaffinity_np(3) [MAN]
                    let idx = c / 64; let bit = c % 64; let ptr = &mut set as *mut _ as *mut u64; *ptr.add(idx) |= 1u64 << bit;
                }
                let _ = libc::pthread_setaffinity_np(libc::pthread_self(), core::mem::size_of::<libc::cpu_set_t>(), &set); // [MAN]
            }
        }
        if let Ok(pol) = std::env::var("SC_RT_POLICY") {
            let prio = std::env::var("SC_RT_PRIO").ok().and_then(|v| v.parse::<i32>().ok()).unwrap_or(1);
            let mut sched: libc::sched_param = core::mem::zeroed();
            sched.sched_priority = prio;
            let policy = match pol.as_str() { "FIFO" => libc::SCHED_FIFO, "RR" => libc::SCHED_RR, _ => libc::SCHED_OTHER };
            let _ = libc::sched_setscheduler(0, policy, &sched);                              // [MAN: sched_setscheduler(2)]
        }
        if let Ok(nice) = std::env::var("SC_NICE") {
            if let Ok(n) = nice.parse::<i32>() { let _ = libc::setpriority(libc::PRIO_PROCESS, 0, n); } // [MAN: setpriority(2)]
        }
        if std::env::var("SC_STACK_THP").ok().as_deref() == Some("1") {
            let mut lim: libc::rlimit = core::mem::zeroed();
            if libc::getrlimit(libc::RLIMIT_STACK, &mut lim) == 0 {
                let sp = &lim as *const _ as usize; let ps = libc::sysconf(libc::_SC_PAGESIZE) as usize; // [MAN: getrlimit(2), sysconf(3)]
                let base = sp & !(ps - 1);
                let _ = libc::madvise(base as *mut _, ps, libc::MADV_HUGEPAGE);              // [MAN: madvise(2) MADV_HUGEPAGE]
            }
        }
    }
}
// <<< HYPER-OPT HIGHLIGHT END
// ============================================================================

// ============================================================================
// WAITPKG detect + umonitor/umwait (gated through central caps)
#[inline(always)]
fn has_waitpkg() -> bool { caps().waitpkg }

#[inline(always)]
fn waitpkg_monitor(addr: *const u8, mut cycles: u32) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        if !caps().waitpkg { return; }
        // UMONITOR/UMWAIT with residency clamp; #UD if disabled; CPUID.7.0:ECX[5] [SDM-V2/V4][XCLOU].
        if cycles < caps().umwait_min_cycles { cycles = caps().umwait_min_cycles; } // “IA32_UMWAIT_CONTROL residency clamp; #UD…”
        core::arch::asm!(
            "umonitor [{0}]",  // [SDM-V2 UMONITOR][XCLOU]
            "xor edx, edx",
            "mov eax, {1:e}",
            "umwait",          // [SDM-V2 UMWAIT][XCLOU]
            in(reg) addr,
            in(reg) cycles,
            options(nostack, preserves_flags)
        );
    }
}

// ============================================================================
// CLDEMOTE heuristics and budget (avoid “demote storms”)
use core::sync::atomic::{AtomicUsize, Ordering as _Ord};
static CLDEMOTE_BUDGET_KB: Al64<AtomicUsize> = Al64(AtomicUsize::new(0));
static CLDEMOTE_LAST_TICK: Al64<AtomicUsize> = Al64(AtomicUsize::new(0));

#[inline(always)]
fn cldemote_budget_allow(kb: usize) -> bool {
    let refill_kb = std::env::var("SC_CLDEMOTE_REFILL_KB").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(128);
    let now_ms = (mono_micros_fast() / 1000) as usize;
    let last = CLDEMOTE_LAST_TICK.load(_Ord::Relaxed);
    if last != now_ms {
        CLDEMOTE_LAST_TICK.store(now_ms, _Ord::Relaxed);
        let cur = CLDEMOTE_BUDGET_KB.load(_Ord::Relaxed);
        let newv = (cur + refill_kb).min(refill_kb * 8);
        CLDEMOTE_BUDGET_KB.store(newv, _Ord::Relaxed);
    }
    let cur = CLDEMOTE_BUDGET_KB.load(_Ord::Relaxed);
    if cur >= kb {
        CLDEMOTE_BUDGET_KB.store(cur - kb, _Ord::Relaxed);
        true
    } else { false }
}

#[inline(always)]
fn cache_line_len() -> usize {
    static L: OnceLock<usize> = OnceLock::new();
    *L.get_or_init(|| 64)
}

// >>> HYPER-OPT HIGHLIGHT BEGIN: cache line demotion hints (cldemote)
#[inline(always)]
fn demote_tail_lines(dst: *const u8, len: usize) {
    #[cfg(target_arch = "x86_64")]
    {
        if !caps().cldemote { return; }
        // CLDEMOTE is a performance hint; µarch may ignore [SDM-V2 CLDEMOTE].
        if std::env::var("SC_CLDEMOTE_TAIL").ok().as_deref() != Some("1") { return; }
        if len == 0 { return; }
        let kb = (len + 1023) / 1024;
        if !cldemote_budget_allow(kb) { return; }
        let cl = cache_line_len();
        let n_lines = ((len + cl - 1) / cl).min(
            std::env::var("SC_CLDEMOTE_TAIL_LINES").ok().and_then(|v| v.parse::<usize>().ok()).unwrap_or(4)
        );
        unsafe {
            let mut off = len & !(cl - 1);
            for _ in 0..n_lines {
                let p = dst.add(off);
                core::arch::asm!("cldemote [{0}]", in(reg) p, options(nostack, preserves_flags)); // “Hint only; µarch may ignore.” [SDM-V2]
                if off == 0 { break; }
                off = off.saturating_sub(cl);
            }
        }
    }
}

#[inline(always)]
fn demote_written_region(p: *const u8, len: usize) {
    #[cfg(target_arch = "x86_64")]
    {
        if !caps().cldemote { return; }
        // CLDEMOTE hint; benefit varies by microarchitecture [SDM-V2].
        if std::env::var("SC_CLDEMOTE").ok().as_deref() != Some("1") { return; }
        let kb = (len + 1023) / 1024;
        if !cldemote_budget_allow(kb) { return; }
        let cl = cache_line_len();
        let mut a = (p as usize) & !(cl - 1);
        let end = (p as usize).wrapping_add(len);
        unsafe {
            while a < end {
                core::arch::asm!("cldemote [{0}]", in(reg) (a as *const u8), options(nostack, preserves_flags)); // “Hint only; µarch may ignore.” [SDM-V2]
                a += cl;
            }
        }
    }
}
// <<< HYPER-OPT HIGHLIGHT END
// ============================================================================

// ============================================================================
// >>> HYPER-OPT HIGHLIGHT BEGIN: lock-free MPSC ring with 64B separated head/tail
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

    /// Drain up to `max` entries, with short pause/yield when empty.
    #[inline]
    pub fn drain_burst<F: FnMut(CacheEntry)>(&self, max: usize, mut f: F) -> usize {
        let mut n = 0usize;
        while n < max {
            if let Some(e) = self.pop() {
                f(e);
                n += 1;
            } else {
                #[cfg(target_arch = "x86_64")]
                unsafe { core::arch::asm!("pause; pause; pause; pause;", options(nostack, preserves_flags)); } // [IG: PAUSE]
                #[cfg(target_arch = "aarch64")]
                unsafe { core::arch::asm!("yield", options(nostack, preserves_flags)); } // [A64]
                break;
            }
        }
        n
    }
}
// <<< HYPER-OPT HIGHLIGHT END
// ============================================================================

// ============================================================================
// >>> HYPER-OPT HIGHLIGHT BEGIN: RTM-backed mutex with correctness/backoff
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
        // RTM fast path via _xbegin/_xend; bounded retries; TPAUSE backoff [IG: RTM intrinsics][SDM-V2 TSX abort causes]
        #[cfg(target_arch = "x86_64")]
        {
            if caps().rtm {
                unsafe {
                    use core::arch::x86_64::{_xabort, _xbegin, _xend, _xtest};
                    const ABORT_MASK: u32 = 0xFF;
                    let max_retries = std::env::var("SC_RTM_RETRIES").ok().and_then(|v| v.parse::<u32>().ok()).unwrap_or(8);
                    let mut tries = 0u32;
                    while tries < max_retries {
                        if self.state.load(core::sync::atomic::Ordering::Acquire) != 0 { break; } // abort if locked [SDM-V2 TSX]
                        let st = _xbegin(); // success => 0xFFFF_FFFF [IG]
                        if st == u32::MAX {
                            if self.state.load(core::sync::atomic::Ordering::Relaxed) == 0 {
                                return TleGuard { m: self, held: false, g: None };
                            }
                            _xabort(ABORT_MASK); // contended path [IG]
                        }
                        tries = tries.wrapping_add(1);
                        core::hint::spin_loop();
                        tpause_cycles(150); // TPAUSE backoff with residency clamp [SDM-V2/V4]
                        let _ = _xtest(); // keep RTM state visible [IG]
                    }
                }
            }
        }
        // Fallback lock path.
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
            if !self.held && _xtest() != 0 { _xend(); return; } // commit transaction [IG]
        }
        if self.held {
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
// <<< HYPER-OPT HIGHLIGHT END
// ============================================================================

// ============================================================================
// >>> HYPER-OPT HIGHLIGHT BEGIN: AVX-512 non-temporal copy with prefetch and fence
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
#[inline(always)]
unsafe fn copy_impl_avx512(dst: *mut u8, src: *const u8, len: usize) {
    // Streaming copy policy: NT stores for large forward copies; prefetch NTA; fence after NT [OPT][IG].
    use core::arch::x86_64::{
        __m512i, _mm512_loadu_si512, _mm512_stream_si512, _mm_prefetch, _mm_sfence, _MM_HINT_NTA
    };
    let pf_stride = (cache_line_len() * 8).max(256); // µarch-tuned heuristic [OPT]; thresholds µarch-dependent.
    let mut d = dst; let mut s = src;
    let mis64 = (d as usize) & 63;
    if mis64 != 0 {
        let head = core::cmp::min(64 - mis64, len);
        core::ptr::copy_nonoverlapping(s, d, head);
        d = d.add(head); s = s.add(head);
    }
    let n = len - ((d as usize).wrapping_sub(dst as usize));
    let chunks = n / 64;
    let tail   = n % 64;
    let mut i = 0usize;
    while i + 3 < chunks {
        if (i & 3) == 0 { _mm_prefetch(s.add(i*64 + pf_stride) as *const i8, _MM_HINT_NTA); } // “prefetch NTA for streaming reads” [OPT]
        let v0: __m512i = _mm512_loadu_si512(s.add(i*64)      as *const __m512i);
        let v1: __m512i = _mm512_loadu_si512(s.add((i+1)*64)  as *const __m512i);
        let v2: __m512i = _mm512_loadu_si512(s.add((i+2)*64)  as *const __m512i);
        let v3: __m512i = _mm512_loadu_si512(s.add((i+3)*64)  as *const __m512i);
        _mm512_stream_si512(d.add(i*64)      as *mut __m512i, v0); // “streaming (non-temporal) store” [OPT][IG]
        _mm512_stream_si512(d.add((i+1)*64)  as *mut __m512i, v1); // [OPT][IG]
        _mm512_stream_si512(d.add((i+2)*64)  as *mut __m512i, v2); // [OPT][IG]
        _mm512_stream_si512(d.add((i+3)*64)  as *mut __m512i, v3); // [OPT][IG]
        i += 4;
    }
    while i < chunks {
        let v: __m512i = _mm512_loadu_si512(s.add(i*64) as *const __m512i);
        _mm512_stream_si512(d.add(i*64) as *mut __m512i, v);        // “streaming store” [OPT][IG]
        i += 1;
    }
    if tail != 0 { core::ptr::copy_nonoverlapping(s.add(chunks*64), d.add(chunks*64), tail); }
    _mm_sfence(); // “fence after NT stores for global visibility” [OPT]
}
// <<< HYPER-OPT HIGHLIGHT END
// ============================================================================

// ============================================================================
// TLS micro-LRU, negative hotline, promotion buffer
struct TlsPromo { buf: heapless::Vec<CacheEntry, 32>, last_us: u64 }

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

// ============================================================================
// AES/NEON mixer, BMI2-assisted lane extractor
#[inline(always)]
fn mix_pubkey128(pk: &Pubkey, salt: u64) -> (u64, u64) {
    let b = pk.to_bytes();
    #[cfg(target_arch = "x86_64")]
    {
        if core::is_x86_feature_detected!("aes") {
            unsafe {
                // AES-NI single-round diffusion [IG: _mm_aesenc_si128]
                use core::arch::x86_64::*;
                let a = _mm_loadu_si128(b.as_ptr() as *const __m128i);
                let c = _mm_loadu_si128(b.as_ptr().add(16) as *const __m128i);
                let k1 = _mm_set1_epi64x(salt as i64);
                let k2 = _mm_set1_epi64x((salt.rotate_left(17)) as i64);
                let mut x = _mm_xor_si128(a, k1);
                let mut y = _mm_xor_si128(c, k2);
                x = _mm_aesenc_si128(x, y); // [IG]
                y = _mm_aesenc_si128(y, x); // [IG]
                let m = _mm_xor_si128(x, y);
                let lo = _mm_cvtsi128_si64(m) as u64;
                let hi = _mm_extract_epi64(m, 1) as u64;
                return (lo, hi);
            }
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        if caps().aes_a64 {
            unsafe {
                // AArch64 AES ops: vaeseq_u8 + vaesmcq_u8 [A64 stdarch]
                use core::arch::aarch64::*;
                let a = vld1q_u8(b.as_ptr());
                let c = vld1q_u8(b.as_ptr().add(16));
                let ks1 = vreinterpretq_u8_u64(vdupq_n_u64(salt));
                let ks2 = vreinterpretq_u8_u64(vdupq_n_u64(salt.rotate_left(17)));
                let mut x = veorq_u8(a, ks1);
                let mut y = veorq_u8(c, ks2);
                x = vaeseq_u8(x, y); x = vaesmcq_u8(x);   // [A64]
                y = vaeseq_u8(y, x); y = vaesmcq_u8(y);   // [A64]
                let m = veorq_u8(x, y);
                let lo = vgetq_lane_u64(vreinterpretq_u64_u8(m), 0);
                let hi = vgetq_lane_u64(vreinterpretq_u64_u8(m), 1);
                return (lo, hi);
            }
        }
    }
    // portable fallback hashing
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

#[inline(always)]
fn pk_eq_fast(a32: &[u8;32], b: &Pubkey) -> bool {
    let bb = b.to_bytes();
    #[cfg(target_arch = "x86_64")]
    {
        if caps().avx2 {
            unsafe {
                // AVX2 equality [IG: _mm256_cmpeq_epi8 + _mm256_movemask_epi8]
                use core::arch::x86_64::{__m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8};
                let va = _mm256_loadu_si256(a32.as_ptr() as *const __m256i);
                let vb = _mm256_loadu_si256(bb.as_ptr() as *const __m256i);
                return _mm256_movemask_epi8(_mm256_cmpeq_epi8(va, vb)) == -1; // [IG]
            }
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        unsafe {
            // NEON equality [A64: vceqq_u8 + vminvq_u8]
            use core::arch::aarch64::{uint8x16_t, vceqq_u8, vld1q_u8, vminvq_u8};
            let a0: uint8x16_t = vld1q_u8(a32.as_ptr());
            let a1: uint8x16_t = vld1q_u8(a32.as_ptr().add(16));
            let b0: uint8x16_t = vld1q_u8(bb.as_ptr());
            let b1: uint8x16_t = vld1q_u8(bb.as_ptr().add(16));
            if vminvq_u8(vceqq_u8(a0, b0)) != 255 { return false; } // [A64]
            if vminvq_u8(vceqq_u8(a1, b1)) != 255 { return false; } // [A64]
            return true;
        }
    }
    *a32 == bb
}

#[inline(always)]
fn dk_lane_index3(pk: &Pubkey, seed: u64, words: usize) -> [(usize, u64); 3] {
    let (lo, hi) = mix_pubkey128(pk, seed);
    #[cfg(target_arch = "x86_64")]
    {
        if caps().bmi2 {
            // BMI2 PEXT lane selection [IG: _pext_u64]
            use core::arch::x86_64::_pext_u64;
            let (m0, m1, m2) = pick_mask_triplet(salt_per_socket(seed));
            let i0 = unsafe { _pext_u64(lo, m0) } as usize;              // “BMI2 PEXT.” [IG]
            let i1 = unsafe { _pext_u64(hi, m1) } as usize;              // “BMI2 PEXT.” [IG]
            let i2 = unsafe { _pext_u64(lo ^ hi.rotate_left(7), m2) } as usize; // “BMI2 PEXT.” [IG]
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

// ============================================================================
// fast_equal_simd fallback suite
#[inline(always)]
unsafe fn fast_equal_simd(a: *const u8, b: *const u8, len: usize) -> bool {
    #[cfg(target_arch = "x86_64")]
    {
        if caps().avx512bw && caps().avx512f {
            // AVX-512 compare [IG: _mm512_cmpeq_epi8_mask]
            use core::arch::x86_64::{__m512i, _mm512_cmpeq_epi8_mask, _mm512_loadu_si512};
            let mut i = 0usize;
            while i + 64 <= len {
                let va: __m512i = _mm512_loadu_si512(a.add(i) as *const __m512i);
                let vb: __m512i = _mm512_loadu_si512(b.add(i) as *const __m512i);
                let m = _mm512_cmpeq_epi8_mask(va, vb); // [IG]
                if m != !0u64 { return false; }
                i += 64;
            }
            while i < len { if *a.add(i) != *b.add(i) { return false; } i += 1; }
            return true;
        }
        // AVX2 path [IG]
        use core::arch::x86_64::{__m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8};
        let mut i = 0usize;
        while i + 32 <= len {
            let va = _mm256_loadu_si256(a.add(i) as *const __m256i);
            let vb = _mm256_loadu_si256(b.add(i) as *const __m256i);
            if _mm256_movemask_epi8(_mm256_cmpeq_epi8(va, vb)) != -1 { return false; } // [IG]
            i += 32;
        }
        while i < len { if *a.add(i) != *b.add(i) { return false; } i += 1; }
        return true;
    }
    #[cfg(target_arch = "aarch64")]
    {
        // NEON compare [A64]
        use core::arch::aarch64::{uint8x16_t, vceqq_u8, vld1q_u8, vminvq_u8};
        let mut i = 0usize;
        while i + 16 <= len {
            let va: uint8x16_t = vld1q_u8(a.add(i));
            let vb: uint8x16_t = vld1q_u8(b.add(i));
            if unsafe { vminvq_u8(vceqq_u8(va, vb)) } != 255 { return false; } // [A64]
            i += 16;
        }
        while i < len { if *a.add(i) != *b.add(i) { return false; } i += 1; }
        return true;
    }
    let mut i = 0usize;
    while i < len { if *a.add(i) != *b.add(i) { return false; } i += 1; }
    true
}

// ============================================================================
// NT copy threshold with logging/guardrails
use std::sync::OnceLock as _OnceLockForNt;

#[inline]
fn nt_copy_threshold() -> usize {
    static THR: OnceLock<usize> = OnceLock::new();
    let thr = *THR.get_or_init(|| {
        if let Ok(v) = std::env::var("SC_NT_THR") { if let Ok(x) = v.parse::<usize>() { return x.max(256); } }
        let cl = cache_line_len();
        #[cfg(target_arch = "x86_64")]
        {
            // NT store thresholds are µarch-specific; these tiers are heuristics [OPT].
            if caps().avx512f { return (cl * 48).max(3072); }
            if caps().avx2     { return (cl * 64).max(4096); }
            return (cl * 80).max(5120);
        }
        #[cfg(target_arch = "aarch64")]
        {
            // No NT store equivalent; heuristic [OPT].
            if caps().aes_a64 { return (cl * 64).max(4096); }
            return (cl * 96).max(6144);
        }
        4096
    });
    log_once_env("SC_NT_THR(resolved)", thr);
    thr
}

// ============================================================================
// Hotline and NUMA shards constants
const CACHE_ALIGN: usize = 64;
const HOT_WAYS: usize = 2;
const HOT_SETS: usize = 64;
const SH_HOT_WAYS: usize = 2;
const SH_HOT_SETS: usize = 64;
#[inline] fn shard_set_idx(pk: &Pubkey) -> usize { (pk.to_bytes()[7] as usize) & (SH_HOT_SETS - 1) }

// >>> HYPER-OPT HIGHLIGHT BEGIN: hotline set index using AES/PEXT when available
#[inline]
fn hotline_salt() -> u64 { 0xBADC_AB1Eu64 } // stub

impl MemoryMappedStateCache {
    #[inline]
    fn hot_set_idx(&self, pk: &Pubkey) -> usize {
        #[cfg(target_arch = "x86_64")]
        {
            use core::arch::x86_64::_pext_u64;
            let (lo, hi) = mix_pubkey128(pk, hotline_salt() as u64);
            if caps().bmi2 {
                // PEXT-based index extraction [IG: _pext_u64]
                const MASK_HOT: u64 = 0x9249_2492_4924_9249;
                let y = unsafe { _pext_u64(lo ^ hi.rotate_left(9), MASK_HOT) } as usize; // “BMI2 PEXT.” [IG]
                return y & (HOT_SETS - 1);
            }
            let y = (lo ^ hi ^ (hi >> 7) ^ (lo.rotate_left(13))) as usize;
            return y & (HOT_SETS - 1);
        }
        let (lo, hi) = mix_pubkey128(pk, hotline_salt() as u64);
        ((lo ^ hi ^ (hi >> 5)) as usize) & (HOT_SETS - 1)
    }
}
// <<< HYPER-OPT HIGHLIGHT END
// ============================================================================

// -----------------------------------------------------------------------------
// Types and stubs used by the highlighted code.
// -----------------------------------------------------------------------------
#[derive(Copy, Clone, Default)]
pub struct CacheEntry {
    pub pubkey: [u8;32],
    pub offset: u64,
    pub size: u64,
    pub slot: u64,
    pub last_access_us: u64,
    pub checksum: u32,
    pub flags: u32,
    pub rev: u64,
    pub _pad: u64,
}
pub struct MemoryMappedStateCache { numa_shards: usize }
impl MemoryMappedStateCache { fn new(numa_shards: usize) -> Self { Self { numa_shards } } }
pub type Slot = u64;

#[derive(Copy, Clone, Default)]
pub struct Pubkey([u8;32]);
impl Pubkey {
    pub fn to_bytes(&self) -> [u8;32] { self.0 }
    pub fn new_from_array(a: [u8;32]) -> Self { Pubkey(a) }
}

// Monotonic clock stubs (replace with your real timer)
#[inline(always)] fn mono_micros_fast() -> u64 { 0 }
#[inline(always)] fn mono_micros() -> u64 { 0 }

// ============================================================================
// cfg(test): parity micro-tests (portable vs SIMD eq; PEXT distribution; VBMI misalignment)
// ============================================================================
#[cfg(test)]
mod tests {
    use super::*;
    use rand::{Rng, SeedableRng};
    use rand::rngs::StdRng;

    #[test]
    fn simd_eq_parity() {
        let mut rng = StdRng::seed_from_u64(42);
        for len in [0, 3, 31, 32, 63, 64, 65, 511, 1024] {
            let mut a = vec![0u8; len];
            let mut b = vec![0u8; len];
            rng.fill(&mut a[..]); b.copy_from_slice(&a);
            let p = unsafe { super::eq_header_impl_portable(a.as_ptr(), b.as_ptr(), len) };
            let s = unsafe { super::fast_equal_simd(a.as_ptr(), b.as_ptr(), len) };
            assert_eq!(p, s, "len {}", len);
            if len > 0 {
                b[len/2] ^= 1;
                let p2 = unsafe { super::eq_header_impl_portable(a.as_ptr(), b.as_ptr(), len) };
                let s2 = unsafe { super::fast_equal_simd(a.as_ptr(), b.as_ptr(), len) };
                assert_eq!(p2, s2, "len {} mismatch after flip", len);
            }
        }
    }

    #[test]
    fn pext_distribution_sanity() {
        // Indices in-range and broadly covering space — BMI2 PEXT [IG].
        let words = 256usize;
        let seed = 0x1234_5678_9ABC_DEF0u64;
        let mut hit = vec![0usize; words*64];
        for i in 0..10_000 {
            let pk = Pubkey::new_from_array([i as u8; 32]);
            let v = super::dk_lane_index3(&pk, seed, words);
            for (w, bit) in v {
                let idx = (w<<6) | (bit.trailing_zeros() as usize);
                hit[idx] += 1;
            }
        }
        let total = hit.iter().filter(|&&x| x>0).count();
        assert!(total > (words*64)/4, "coverage too low: {}", total);
    }

    #[test]
    fn vbmi_unaligned_path_parity() {
        // VBMI stitcher equals naive compare under misalignment [IG: _permutex2var_epi8 + cmpeq mask].
        #[cfg(target_arch = "x86_64")]
        {
            if !(caps().avx512f && caps().avx512bw && caps().avx512vbmi) { return; }
            let mut rng = StdRng::seed_from_u64(1337);
            for _ in 0..200 {
                let len = 64 + (rng.gen::<usize>() % 1024);
                let mut buf = vec![0u8; len + 128];
                rng.fill(&mut buf[..]);
                let a_off = (rng.gen::<usize>() % 64);
                let b_off = (rng.gen::<usize>() % 64);
                let a = unsafe { buf.as_ptr().add(a_off) };
                let b = unsafe { buf.as_ptr().add(b_off) };
                let naive = unsafe { super::eq_header_impl_portable(a, b, len) };
                let vbmi  = unsafe { super::eq_header_impl_avx512_vbmi(a, b, len) };
                assert_eq!(naive, vbmi, "vbmi mismatch at len={}, a_off={}, b_off={}", len, a_off, b_off);
            }
        }
    }
}
