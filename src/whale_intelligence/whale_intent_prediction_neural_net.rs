#![deny(unsafe_code)]
#![deny(warnings)]

use std::path::Path;
use thiserror::Error;
use serde::{Deserialize, Serialize};
use blake3;

/// Whale intent classification categories
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IntentClass {
    Accumulate,
    Dump,
    Rotate,
    Spoof,
    Snipe,
}

impl IntentClass {
    #[inline]
    fn from_index(index: usize) -> Result<Self, PredictionError> {
        match index {
            0 => Ok(IntentClass::Accumulate),
            1 => Ok(IntentClass::Dump),
            2 => Ok(IntentClass::Rotate),
            3 => Ok(IntentClass::Spoof),
            4 => Ok(IntentClass::Snipe),
            _ => Err(PredictionError::InvalidClassIndex(index)),
        }
    }
}

/// Whale intent prediction result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhaleIntentScore {
    pub class: IntentClass,
    pub confidence: f32,
    pub slot: u64,
}

/// Prediction errors
#[derive(Error, Debug)]
pub enum PredictionError {
    #[error("Model file not found: {0}")]
    ModelNotFound(String),
    #[error("Invalid model format: {0}")]
    InvalidModelFormat(String),
    #[error("Feature vector size mismatch: expected {expected}, got {actual}")]
    FeatureSizeMismatch { expected: usize, actual: usize },
    #[error("Invalid class index: {0}")]
    InvalidClassIndex(usize),
    #[error("Confidence value out of range: {0}")]
    InvalidConfidence(f32),
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),
    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::Error),
    #[error("Numerical computation error")]
    NumericalError,
    #[error("Checksum mismatch in model file")]
    ChecksumMismatch,
    #[error("Magic/version mismatch in model file")]
    MagicMismatch,
}

/// === REPLACE: ModelWeights (struct only) ===
#[repr(C, align(64))]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelWeights {
    // Row-major: hidden_size x feature_count
    pub w1: Vec<f32>,
    pub b1: Vec<f32>,                 // len: hidden_size
    // Row-major: output_size x hidden_size (output_size == 5)
    pub w2: Vec<f32>,
    pub b2: Vec<f32>,                 // len: 5

    pub feature_count: usize,
    pub hidden_size: usize,
    pub output_size: usize, // must be 5

    // Standardization stats (will be folded)
    pub feature_mean: Vec<f32>,       // len: feature_count
    pub feature_std:  Vec<f32>,       // len: feature_count

    // Optional per-feature clamps (desk-grade guardrails)
    #[serde(default)]
    pub feature_min: Vec<f32>,        // len 0 or feature_count
    #[serde(default)]
    pub feature_max: Vec<f32>,        // len 0 or feature_count

    // Softmax temperature (> 0)
    pub temperature: f32,

    // Schema version
    pub version: u32,

    // Calibration & clamps
    #[serde(default)]
    pub class_bias: Vec<f32>,         // len 0 or 5
    #[serde(default = "default_logit_gain")]
    pub logit_gain: f32,              // > 0
    #[serde(default)]
    pub class_gain: Vec<f32>,         // len 0 or 5
    #[serde(default = "default_clip_hidden")]
    pub clip_hidden: f32,             // > 0
    #[serde(default = "default_clip_logits")]
    pub clip_logits: f32,             // > 0

    // Optional row-wise sparse index lists for W1 (if pruned offline)
    // w1_sparse_col_idx.len() == hidden_size OR 0; each entry holds column indices < feature_count
    #[serde(default)]
    pub w1_sparse_col_idx: Vec<Vec<u32>>,
}

#[inline(always)] fn default_logit_gain() -> f32 { 1.0 }
#[inline(always)] fn default_clip_hidden() -> f32 { 1.0e6 }
#[inline(always)] fn default_clip_logits() -> f32 { 1.0e6 }

#[repr(C, align(64))]
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ModelFile {
    magic: [u8; 8],        // b"INTENTV1"
    version: u32,          // file container version (not weights version)
    weights: ModelWeights, // the payload
    checksum: [u8; 32],    // blake3(weights bincode)
}

impl ModelWeights {
    #[inline]
    fn validate(&self) -> Result<(), PredictionError> {
        if self.output_size != 5 {
            return Err(PredictionError::InvalidModelFormat("Output size must be 5".into()));
        }
        if self.w1.len() != self.hidden_size * self.feature_count {
            return Err(PredictionError::InvalidModelFormat("w1 size mismatch".into()));
        }
        if self.b1.len() != self.hidden_size {
            return Err(PredictionError::InvalidModelFormat("b1 size mismatch".into()));
        }
        if self.w2.len() != self.output_size * self.hidden_size {
            return Err(PredictionError::InvalidModelFormat("w2 size mismatch".into()));
        }
        if self.b2.len() != self.output_size {
            return Err(PredictionError::InvalidModelFormat("b2 size mismatch".into()));
        }
        if self.feature_mean.len() != self.feature_count || self.feature_std.len() != self.feature_count {
            return Err(PredictionError::InvalidModelFormat("feature stats size mismatch".into()));
        }
        if !(self.temperature.is_finite() && self.temperature > 0.0) {
            return Err(PredictionError::InvalidModelFormat("temperature must be > 0".into()));
        }
        if !(self.class_bias.is_empty() || self.class_bias.len() == self.output_size) {
            return Err(PredictionError::InvalidModelFormat("class_bias len must be 0 or output_size".into()));
        }
        if !(self.class_gain.is_empty() || self.class_gain.len() == self.output_size) {
            return Err(PredictionError::InvalidModelFormat("class_gain len must be 0 or output_size".into()));
        }
        if !self.logit_gain.is_finite() || self.logit_gain <= 0.0 {
            return Err(PredictionError::InvalidModelFormat("logit_gain must be finite and > 0".into()));
        }
        if !(self.clip_hidden.is_finite() && self.clip_hidden > 0.0 && self.clip_logits.is_finite() && self.clip_logits > 0.0) {
            return Err(PredictionError::InvalidModelFormat("clip thresholds must be finite and > 0".into()));
        }
        if !(self.feature_min.is_empty() || self.feature_min.len() == self.feature_count) {
            return Err(PredictionError::InvalidModelFormat("feature_min len must be 0 or feature_count".into()));
        }
        if !(self.feature_max.is_empty() || self.feature_max.len() == self.feature_count) {
            return Err(PredictionError::InvalidModelFormat("feature_max len must be 0 or feature_count".into()));
        }
        if !self.w1_sparse_col_idx.is_empty() && self.w1_sparse_col_idx.len() != self.hidden_size {
            return Err(PredictionError::InvalidModelFormat("w1_sparse_col_idx len must be 0 or hidden_size".into()));
        }
        if !self.w1_sparse_col_idx.is_empty() {
            for (j, cols) in self.w1_sparse_col_idx.iter().enumerate() {
                for &c in cols {
                    if (c as usize) >= self.feature_count {
                        return Err(PredictionError::InvalidModelFormat(format!("w1_sparse_col_idx[{}] contains out-of-range col {}", j, c)));
                    }
                }
            }
        }
        // Finite checks (desk-grade hygiene)
        for &v in &self.w1 { if !v.is_finite() { return Err(PredictionError::InvalidModelFormat("w1 contains non-finite".into())); } }
        for &v in &self.b1 { if !v.is_finite() { return Err(PredictionError::InvalidModelFormat("b1 contains non-finite".into())); } }
        for &v in &self.w2 { if !v.is_finite() { return Err(PredictionError::InvalidModelFormat("w2 contains non-finite".into())); } }
        for &v in &self.b2 { if !v.is_finite() { return Err(PredictionError::InvalidModelFormat("b2 contains non-finite".into())); } }
        for &v in &self.feature_mean { if !v.is_finite() { return Err(PredictionError::InvalidModelFormat("feature_mean contains non-finite".into())); } }
        for &v in &self.feature_std  { if !v.is_finite() { return Err(PredictionError::InvalidModelFormat("feature_std contains non-finite".into())); } }
        if !self.feature_min.is_empty() {
            for &v in &self.feature_min { if !v.is_finite() { return Err(PredictionError::InvalidModelFormat("feature_min contains non-finite".into())); } }
        }
        if !self.feature_max.is_empty() {
            for &v in &self.feature_max { if !v.is_finite() { return Err(PredictionError::InvalidModelFormat("feature_max contains non-finite".into())); } }
        }
        Ok(())
    }
}

fn compute_weights_checksum(weights: &ModelWeights) -> [u8; 32] {
    let bytes = bincode::serialize(weights).expect("serializable weights");
    *blake3::hash(&bytes).as_bytes()
}

#[cold]
fn decode_and_verify_model(bytes: &[u8]) -> Result<ModelWeights, PredictionError> {
    let mf: ModelFile = bincode::deserialize(bytes)
        .map_err(|e| PredictionError::InvalidModelFormat(e.to_string()))?;
    if mf.magic != *b"INTENTV1" || mf.version != 1 {
        return Err(PredictionError::MagicMismatch);
    }
    let want = compute_weights_checksum(&mf.weights);
    if want != mf.checksum {
        return Err(PredictionError::ChecksumMismatch);
    }
    mf.weights.validate()?;
    Ok(mf.weights)
}

/// === REPLACE: config constants (class gates, hysteresis, clamps) ===
/// Class-specific ratio multipliers, min-confidence floors, hysteresis, raw feature clamp.
const CLASS_RATIO_MUL: [f32; 5] = [
    1.03, // Accumulate
    1.04, // Dump
    1.03, // Rotate
    1.08, // Spoof  (hardest to accept)
    1.02, // Snipe  (fastest path)
];

const CLASS_MIN_CONF: [f32; 5] = [
    0.48, // Accumulate
    0.52, // Dump   (require a touch more)
    0.48, // Rotate
    0.58, // Spoof  (strongest evidence)
    0.46, // Snipe
];

const CLASS_MIN_MARGIN: [f32; 5] = [
    0.5, // Accumulate
    0.5, // Dump
    0.5, // Rotate
    0.5, // Spoof
    0.5, // Snipe
];

const HWIN: usize = 8;
const HYST_MIN: [u8; 5] = [
    1, // Accumulate
    1, // Dump
    1, // Rotate
    2, // Spoof requires more support
    1, // Snipe
];

const RAW_FEATURE_CLAMP: f32 = 1.0e6;

const DEDUPE_RING: usize = 16;

/// === ADD (verbatim): sparse policy threshold ===
/// Use sparse path if total nnz across W1 rows <= SPARSE_USE_THRESHOLD * (hidden_size * feature_count)
const SPARSE_USE_THRESHOLD: f32 = 0.45;

/// === ADD (verbatim): input-sparse policy & eps ===
/// Use input-sparse path if nz/features <= threshold (after clamping).
const INPUT_SPARSE_USE_THRESHOLD: f32 = 0.22;
const INPUT_SPARSE_EPS: f32 = 1.0e-9;

/// === ADD (verbatim): input-sparse drop epsilon ===
const INPUT_SPARSE_DROP_EPS: f32 = 1.0e-6;

/// === ADD (verbatim): dynamic input-sparse threshold control ===
const DYN_THRESH_MIN: f32   = 0.12;
const DYN_THRESH_MAX: f32   = 0.40;
const DYN_THRESH_MULT: f32  = 1.15;
const NZ_EMA_ALPHA:  f32    = 0.02;

/// === ADD (verbatim): AVX-512 toggle + small-nz locality sort ===
const USE_AVX512: bool = true;              // prefer AVX-512 kernels when available
const SORT_NZ_FOR_LOCALITY: bool = true;    // sort nz indices when small for sequential W1ᵀ access
const NZ_SORT_MAX: usize = 128;             // only sort when nz <= this (O(n log n) but tiny)

/// === ADD (verbatim): SIMD NZ-builder toggle ===
const USE_SIMD_NZ_BUILDER: bool = true; // vectorized build of nz index list (AVX-512/AVX2), safe fallback

/// === ADD (verbatim): f16 weight storage toggle ===
const USE_F16_WEIGHTS: bool = true;   // store W1, W1ᵀ, and scaled W2 as IEEE-754 half (u16), convert on load

/// === ADD (verbatim): FTZ/DAZ + AVX2/FMA toggles ===
const ENABLE_FTZ_DAZ: bool = true;      // enable flush-to-zero & denormals-are-zero on x86
const USE_PREFETCH:   bool = true;      // light-touch prefetch in AVX2 loops

/// === ADD (verbatim): toggle and threshold for streaming fused path ===
const USE_STREAM_FUSED: bool = true;
const STREAM_FUSED_WHEN_PH_LE: usize = 128;

/// === ADD (verbatim): toggle ===
const USE_W2_SOA5: bool = true;   // interleave W2 rows as [5 x ph] SOA for stream-fused

/// === ADD (verbatim): bf16 storage toggle ===
const USE_BF16_WEIGHTS: bool = true;   // store W1/W1ᵀ/W2 as IEEE bfloat16 (u16) with exact widen on load

/// === ADD (verbatim): 2MiB-aligned reallocator + THP advice ===
const USE_HUGEPAGE_ADVICE: bool = true;
const ALIGN_2MB: bool = true;

/// === ADD (verbatim): missing constants ===
const SOFTMAX_EXP_CUTOFF: f32 = -50.0;
const USE_FAST_EXP: bool = false;

/// === ADD (verbatim): missing functions ===
#[inline(always)]
fn fast_exp_f32(x: f32) -> f32 {
    // Simple polynomial approximation for exp(x)
    let x = x.clamp(-50.0, 50.0);
    let x2 = x * x;
    let x3 = x2 * x;
    let x4 = x3 * x;
    1.0 + x + x2/2.0 + x3/6.0 + x4/24.0
}

#[inline(always)]
fn pack_f32_to_f16_vec(src: &[f32]) -> Vec<u16> {
    let n = src.len();
    let mut dst = vec![0u16; n];
    let mut i = 0usize;

    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::*;
        while i + 8 <= n {
            let v = _mm256_loadu_ps(src.as_ptr().add(i));
            let h = _mm256_cvtps_ph(v, _MM_FROUND_TO_NEAREST_INT);
            _mm_storeu_si128(dst.as_mut_ptr().add(i) as *mut _, h);
            i += 8;
        }
    }
    while i < n { dst[i] = f32_to_f16_bits_nearest_even(src[i]); i += 1; }
    dst
}

#[inline(always)]
fn f32_to_f16_bits_nearest_even(x: f32) -> u16 {
    let bits = x.to_bits();
    let sign = (bits >> 31) & 1;
    let exp = (bits >> 23) & 0xFF;
    let mant = bits & 0x7FFFFF;
    
    if exp == 0xFF {
        // NaN or Inf
        ((sign << 15) | 0x7C00 | ((mant != 0) as u32 * 0x200)) as u16
    } else if exp == 0 {
        // Denormal
        let shift = mant.leading_zeros() - 8;
        let mant16 = (mant << (shift + 1)) >> 13;
        ((sign << 15) | mant16) as u16
    } else {
        // Normal
        let exp16 = ((exp as i32) - 127 + 15) as u32;
        if exp16 >= 31 {
            // Overflow to Inf
            ((sign << 15) | 0x7C00) as u16
        } else if exp16 <= 0 {
            // Underflow to denormal
            let shift = 1 - exp16 as u32;
            let mant16 = (mant >> (13 + shift)) | ((mant >> (12 + shift)) & 1);
            ((sign << 15) | mant16) as u16
        } else {
            ((sign << 15) | (exp16 << 10) | (mant >> 13)) as u16
        }
    }
}

#[inline(always)]
fn lock_and_prefault_weights_f16(w1: &mut [u16], w1t: &mut [u16], w2s: &mut [u16]) {
    #[cfg(target_os = "linux")]
    unsafe {
        use core::ffi::c_void;
        extern "C" {
            fn madvise(addr: *mut c_void, len: usize, advice: i32) -> i32;
            fn mlock(addr: *const c_void, len: usize) -> i32;
        }
        const MADV_WILLNEED: i32 = 3;
        const MADV_HUGEPAGE: i32 = 14;

        for slice in [w1, w1t, w2s] {
            if slice.is_empty() { continue; }
            let ptr = slice.as_mut_ptr() as *mut c_void;
            let len = slice.len() * core::mem::size_of::<u16>();
            let _ = madvise(ptr, len, MADV_WILLNEED);
            if ENABLE_HUGEPAGE_ADVICE { let _ = madvise(ptr, len, MADV_HUGEPAGE); }
            let _ = mlock(ptr, len);
        }
    }
}

#[inline(always)]
fn logistic_from_margin(inv_t: f32, margin: f32) -> f32 {
    1.0 / (1.0 + (-margin * inv_t).exp())
}

#[inline(always)]
fn enable_denormals_zero_if_x86() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::*;
        let mut mxcsr = _mm_getcsr();
        mxcsr |= 0x8040; // FTZ + DAZ
        _mm_setcsr(mxcsr);
    }
}

#[inline(always)]
fn init_store_hidden_stream_env() {
    // Environment variable initialization for hidden store toggle
    let _ = std::env::var("PREDICTOR_STORE_HIDDEN_STREAM");
}

#[inline(always)]
fn env_f32(name: &str, default: f32) -> f32 {
    match std::env::var(name) {
        Ok(s) => s.parse().unwrap_or(default),
        Err(_) => default,
    }
}

#[inline(always)]
fn build_nz_indices_micropruned(x: &[f32], w1_col_l1: &[f32], w2_linf_max: f32, nz: &mut Vec<usize>) -> usize {
    let mut count = 0;
    for i in 0..x.len() {
        if x[i].abs() * w1_col_l1[i] > w2_linf_max {
            nz.push(i);
            count += 1;
        }
    }
    count
}

#[inline(always)]
fn should_use_sparse_cost(nz: usize, d: usize, h: usize, cost: CostModel) -> bool {
    let dense_cost = (d * h) as f32 * cost.dense_elem;
    let sparse_cost = (nz * h) as f32 * cost.sparse_elem + cost.nz_overhead;
    sparse_cost < dense_cost
}

#[derive(Clone, Copy)]
struct CostModel {
    dense_elem: f32,
    sparse_elem: f32,
    nz_overhead: f32,
}

#[inline(always)]
fn round_up_8(n: usize) -> usize {
    (n + 7) & !7
}

/// === ADD (verbatim): env flag reader (evaluated once at init) ===
#[inline(always)]
fn env_flag(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(s) => matches!(s.as_bytes(), b"1" | b"true" | b"TRUE" | b"on" | b"ON"),
        Err(_) => default,
    }
}

/// === ADD (verbatim): best-effort CPU pinning (Linux only) ===
#[inline(always)]
pub fn pin_current_thread_to_core(core: usize) -> bool {
    #[cfg(target_os = "linux")]
    unsafe {
        use libc::{cpu_set_t, sched_setaffinity};
        let mut set: cpu_set_t = core::mem::zeroed();
        let idx = core / 64;
        let off = (core % 64) as u32;
        // Linux libc defines cpu_set_t with __bits u64[]
        if idx < set.__bits.len() {
            set.__bits[idx] = 1u64 << off;
            let rc = sched_setaffinity(0, core::mem::size_of::<cpu_set_t>(), &set);
            return rc == 0;
        }
        false
    }
    #[cfg(not(target_os = "linux"))]
    { let _ = core; false }
}

/// === ADD (verbatim): small-nz insertion sort for locality ===
#[inline(always)]
fn small_nz_locality_sort(nz: &mut [usize]) {
    let n = nz.len();
    if n <= 32 {
        // classic insertion sort; branch-friendly on tiny n
        for i in 1..n {
            let key = nz[i];
            let mut j = i;
            while j > 0 && nz[j - 1] > key {
                nz[j] = nz[j - 1];
                j -= 1;
            }
            nz[j] = key;
        }
    } else {
        nz.sort_unstable();
    }
}

/// === ADD (verbatim): prefetch hint toggles ===
const USE_PREFETCH: bool = true;
const USE_PREFETCH_NTA_DENSE: bool = true; // use NTA on fused/stream paths (W1/x touched once)

#[inline(always)]
#[cfg(target_arch = "x86_64")]
unsafe fn prefetch_row(ptr: *const f32, stride: usize) {
    use core::arch::x86_64::{_mm_prefetch, _MM_HINT_NTA, _MM_HINT_T0};
    if USE_PREFETCH {
        if USE_PREFETCH_NTA_DENSE {
            _mm_prefetch(ptr.add(stride) as *const i8, _MM_HINT_NTA);
        } else {
            _mm_prefetch(ptr.add(stride) as *const i8, _MM_HINT_T0);
        }
    }
}

/// === ADD (verbatim): 2MiB-aligned reallocator + THP advice ===
#[inline(always)]
fn to_aligned_vec_with<const A: usize, T: Copy>(mut v: Vec<T>) -> Vec<T> {
    use std::alloc::{alloc, dealloc, Layout};
    assert!(A.is_power_of_two());
    let len = v.len();
    if len == 0 { return v; }
    let layout = Layout::from_size_align(len * core::mem::size_of::<T>(), A).unwrap();
    unsafe {
        let ptr = alloc(layout) as *mut T;
        if ptr.is_null() { std::alloc::handle_alloc_error(layout); }
        core::ptr::copy_nonoverlapping(v.as_ptr(), ptr, len);
        let out = Vec::from_raw_parts(ptr, len, len);
        let old = v.as_mut_ptr();
        let old_cap = v.capacity();
        let old_layout = Layout::array::<T>(old_cap).unwrap();
        core::mem::forget(v);
        dealloc(old as *mut u8, old_layout);
        out
    }
}

#[inline(always)]
fn to_aligned_vec_2mb_f32(v: Vec<f32>) -> Vec<f32> {
    if ALIGN_2MB { to_aligned_vec_with::<{ 2 * 1024 * 1024 }, f32>(v) } else { v }
}
#[inline(always)]
fn to_aligned_vec_2mb_u16(v: Vec<u16>) -> Vec<u16> {
    if ALIGN_2MB { to_aligned_vec_with::<{ 2 * 1024 * 1024 }, u16>(v) } else { v }
}

#[inline(always)]
fn advise_hugepage_f32(bufs: &[&[f32]]) {
    #[cfg(target_os = "linux")]
    unsafe {
        use core::ffi::c_void;
        extern "C" { fn madvise(addr: *mut c_void, len: usize, advice: i32) -> i32; }
        const MADV_HUGEPAGE: i32 = 14;
        if !USE_HUGEPAGE_ADVICE { return; }
        for b in bufs {
            if b.is_empty() { continue; }
            let p = b.as_ptr() as *mut c_void;
            let n = b.len() * core::mem::size_of::<f32>();
            let _ = madvise(p, n, MADV_HUGEPAGE);
        }
    }
}
#[inline(always)]
fn advise_hugepage_u16(bufs: &[&[u16]]) {
    #[cfg(target_os = "linux")]
    unsafe {
        use core::ffi::c_void;
        extern "C" { fn madvise(addr: *mut c_void, len: usize, advice: i32) -> i32; }
        const MADV_HUGEPAGE: i32 = 14;
        if !USE_HUGEPAGE_ADVICE { return; }
        for b in bufs {
            if b.is_empty() { continue; }
            let p = b.as_ptr() as *mut c_void;
            let n = b.len() * core::mem::size_of::<u16>();
            let _ = madvise(p, n, MADV_HUGEPAGE);
        }
    }
}

/// === ADD (verbatim): branchless clamp+ReLU + prefetch distance ===
#[inline(always)]
fn clamp_relu(h: f32, clip: f32) -> f32 {
    // clamp to [-clip, clip] then ReLU
    let t = h.max(-clip).min(clip);
    t.max(0.0)
}

#[inline(always)]
fn prefetch_lookahead(pd: usize) -> usize {
    if pd >= 256 { 128 } else { 64 }
}

/// === ADD (verbatim): 64B-aligned Vec reallocator for f32/f16 ===
fn to_aligned_vec_f32(mut v: Vec<f32>) -> Vec<f32> {
    use std::alloc::{alloc, dealloc, Layout};
    let len = v.len();
    if len == 0 { return v; }
    let layout = Layout::from_size_align(len * core::mem::size_of::<f32>(), 64).unwrap();
    unsafe {
        let ptr = alloc(layout) as *mut f32;
        if ptr.is_null() { std::alloc::handle_alloc_error(layout); }
        core::ptr::copy_nonoverlapping(v.as_ptr(), ptr, len);
        let out = Vec::from_raw_parts(ptr, len, len);
        // free old buffer
        let old = v.as_mut_ptr();
        let old_cap = v.capacity();
        let old_layout = Layout::array::<f32>(old_cap).unwrap();
        core::mem::forget(v); // avoid double-free
        dealloc(old as *mut u8, old_layout);
        out
    }
}

fn to_aligned_vec_u16(mut v: Vec<u16>) -> Vec<u16> {
    use std::alloc::{alloc, dealloc, Layout};
    let len = v.len();
    if len == 0 { return v; }
    let layout = Layout::from_size_align(len * core::mem::size_of::<u16>(), 64).unwrap();
    unsafe {
        let ptr = alloc(layout) as *mut u16;
        if ptr.is_null() { std::alloc::handle_alloc_error(layout); }
        core::ptr::copy_nonoverlapping(v.as_ptr(), ptr, len);
        let out = Vec::from_raw_parts(ptr, len, len);
        let old = v.as_mut_ptr();
        let old_cap = v.capacity();
        let old_layout = Layout::array::<u16>(old_cap).unwrap();
        core::mem::forget(v);
        dealloc(old as *mut u8, old_layout);
        out
    }
}

/// === ADD (verbatim): branchless top-2 on 5 floats ===
#[inline(always)]
fn top2_5(v: [f32;5]) -> (usize, f32, f32) {
    // pairwise compare and shuffle; reduces mispredicts vs loop
    let mut idx = 0usize;
    let mut best = v[0];
    let mut second = f32::NEG_INFINITY;
    macro_rules! upd { ($i:expr) => {{
        let x = v[$i];
        let gt = (x > best) as i32;
        // if x>best: second=best,best=x,idx=i else maybe update second
        let new_best   = if gt != 0 { x } else { best };
        let new_second = if gt != 0 { best } else { if x > second { x } else { second } };
        let new_idx    = if gt != 0 { $i } else { idx };
        best = new_best; second = new_second; idx = new_idx;
    }}}
    upd!(1); upd!(2); upd!(3); upd!(4);
    (idx, best, second)
}

/// === ADD (verbatim): builders (f32/f16) ===
#[inline(always)]
fn build_w2_soa5_f32(w2_scaled: &[f32], ph: usize) -> Vec<f32> {
    // Input layout: rows [0..5), each len=ph, contiguous by row
    let mut out = vec![0.0f32; 5 * ph];
    for j in 0..ph {
        let base = 5 * j;
        out[base + 0] = w2_scaled[0 * ph + j];
        out[base + 1] = w2_scaled[1 * ph + j];
        out[base + 2] = w2_scaled[2 * ph + j];
        out[base + 3] = w2_scaled[3 * ph + j];
        out[base + 4] = w2_scaled[4 * ph + j];
    }
    out
}

#[inline(always)]
fn build_w2_soa5_f16(w2_scaled_f16: &[u16], ph: usize) -> Vec<u16> {
    let mut out = vec![0u16; 5 * ph];
    for j in 0..ph {
        let base = 5 * j;
        out[base + 0] = w2_scaled_f16[0 * ph + j];
        out[base + 1] = w2_scaled_f16[1 * ph + j];
        out[base + 2] = w2_scaled_f16[2 * ph + j];
        out[base + 3] = w2_scaled_f16[3 * ph + j];
        out[base + 4] = w2_scaled_f16[4 * ph + j];
    }
    out
}

/// === ADD (verbatim): cost model + env tunables ===
#[derive(Clone, Copy)]
struct CostModel { dense_elem: f32, sparse_elem: f32, nz_overhead: f32 }

#[inline(always)]
fn env_f32(name: &str, default: f32) -> f32 {
    match std::env::var(name) {
        Ok(s) => s.parse::<f32>().unwrap_or(default),
        Err(_) => default,
    }
}

#[inline(always)]
fn should_use_sparse_cost(nz: usize, d: usize, ph: usize, c: CostModel) -> bool {
    // Units arbitrary but consistent; tuned via env. See wiring below.
    let dense = (d as f32) * (ph as f32) * c.dense_elem;
    let sparse = (nz as f32) * (ph as f32) * c.sparse_elem + (nz as f32) * c.nz_overhead;
    sparse <= dense
}

/// === ADD (verbatim): toggle to skip hidden stores in stream-fused ===
static mut STORE_HIDDEN_STREAM: bool = true;

#[inline(always)]
fn init_store_hidden_stream_env() {
    // Call once in new()
    unsafe { STORE_HIDDEN_STREAM = !env_flag("PREDICTOR_SKIP_STORE_HIDDEN_STREAM", false); }
}

/// === ADD (verbatim): f32<->bf16 helpers ===
#[inline(always)]
fn f32_to_bf16_bits_nearest_even(x: f32) -> u16 {
    let mut u = x.to_bits();
    let lsb = (u >> 16) & 1;
    let round_bias = 0x00007FFFu32 + lsb;
    u = u.wrapping_add(round_bias);
    (u >> 16) as u16
}

#[inline(always)]
fn pack_f32_to_bf16_vec(src: &[f32]) -> Vec<u16> {
    let n = src.len();
    let mut dst = vec![0u16; n];
    let mut i = 0usize;

    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::*;
        while i + 8 <= n {
            let v = _mm256_loadu_ps(src.as_ptr().add(i));
            // round to nearest-even using integer trick
            let bits: __m256i = core::mem::transmute(v);
            let lsb = _mm256_srli_epi32::<16>(bits);
            let lsb1 = _mm256_and_si256(lsb, _mm256_set1_epi32(1));
            let bias = _mm256_add_epi32(_mm256_set1_epi32(0x7FFF), lsb1);
            let rounded = _mm256_add_epi32(bits, bias);
            let hi = _mm256_srli_epi32::<16>(rounded);
            let pack = _mm256_packus_epi32(hi, hi);               // 8x u16 in lower 128
            let pack128 = _mm256_castsi256_si128(pack);
            _mm_storel_epi64(dst.as_mut_ptr().add(i) as *mut _, pack128);
            // store upper 4 via shuffle
            let shuf = _mm_shuffle_epi32(pack128, 0b11_10_01_00);
            _mm_storel_epi64(dst.as_mut_ptr().add(i + 4) as *mut _, shuf);
            i += 8;
        }
    }
    while i < n { dst[i] = f32_to_bf16_bits_nearest_even(src[i]); i += 1; }
    dst
}

/// === ADD (verbatim): build SoA-5 for f32 ===
#[inline(always)]
fn build_w2_soa5_f32(w2_f32: &[f32], ph: usize) -> Vec<f32> {
    let mut out = vec![0.0f32; 5 * ph];
    for j in 0..ph {
        let b = 5 * j;
        out[b + 0] = w2_f32[0 * ph + j];
        out[b + 1] = w2_f32[1 * ph + j];
        out[b + 2] = w2_f32[2 * ph + j];
        out[b + 3] = w2_f32[3 * ph + j];
        out[b + 4] = w2_f32[4 * ph + j];
    }
    out
}

/// === ADD (verbatim): build SoA-5 for f16 ===
#[inline(always)]
fn build_w2_soa5_f16(w2_f16: &[u16], ph: usize) -> Vec<u16> {
    let mut out = vec![0u16; 5 * ph];
    for j in 0..ph {
        let b = 5 * j;
        out[b + 0] = w2_f16[0 * ph + j];
        out[b + 1] = w2_f16[1 * ph + j];
        out[b + 2] = w2_f16[2 * ph + j];
        out[b + 3] = w2_f16[3 * ph + j];
        out[b + 4] = w2_f16[4 * ph + j];
    }
    out
}

/// === ADD (verbatim): build SoA-5 for bf16 as well ===
#[inline(always)]
fn build_w2_soa5_bf16(w2_bf16: &[u16], ph: usize) -> Vec<u16> {
    let mut out = vec![0u16; 5 * ph];
    for j in 0..ph {
        let b = 5 * j;
        out[b + 0] = w2_bf16[0 * ph + j];
        out[b + 1] = w2_bf16[1 * ph + j];
        out[b + 2] = w2_bf16[2 * ph + j];
        out[b + 3] = w2_bf16[3 * ph + j];
        out[b + 4] = w2_bf16[4 * ph + j];
    }
    out
}

/// === ADD (verbatim): protect weight pages (Linux best-effort) ===
#[inline(always)]
fn protect_weights_readonly(w1: &[f32], w1t: &[f32], w2s: &[f32]) {
    #[cfg(target_os = "linux")]
    unsafe {
        use core::ffi::c_void;
        extern "C" {
            fn madvise(addr: *mut c_void, len: usize, advice: i32) -> i32;
            fn mprotect(addr: *mut c_void, len: usize, prot: i32) -> i32;
        }
        const PROT_READ: i32 = 1;
        const MADV_DONTDUMP: i32 = 16;
        const MADV_DONTFORK: i32 = 10;

        #[inline(always)]
        unsafe fn one<T>(buf: &[T]) {
            if buf.is_empty() { return; }
            let p = buf.as_ptr() as *mut c_void;
            let n = buf.len() * core::mem::size_of::<T>();
            let _ = madvise(p, n, MADV_DONTDUMP);
            let _ = madvise(p, n, MADV_DONTFORK);
            let _ = mprotect(p, n, PROT_READ);
        }
        one(w1); one(w1t); one(w2s);
    }
}

#[inline(always)]
fn protect_weights_readonly_u16(w1: &[u16], w1t: &[u16], w2s: &[u16]) {
    #[cfg(target_os = "linux")]
    unsafe {
        use core::ffi::c_void;
        extern "C" { fn madvise(addr: *mut c_void, len: usize, advice: i32) -> i32; fn mprotect(addr:*mut c_void,len:usize,prot:i32)->i32; }
        const PROT_READ: i32 = 1;
        const MADV_DONTDUMP: i32 = 16;
        const MADV_DONTFORK: i32 = 10;
        #[inline(always)]
        unsafe fn one(buf: &[u16]) {
            if buf.is_empty() { return; }
            let p = buf.as_ptr() as *mut c_void;
            let n = buf.len() * core::mem::size_of::<u16>();
            let _ = madvise(p, n, MADV_DONTDUMP);
            let _ = madvise(p, n, MADV_DONTFORK);
            let _ = mprotect(p, n, PROT_READ);
        }
        one(w1); one(w1t); one(w2s);
    }
}

/// === ADD (verbatim): AArch64 NEON fused dense (f32) ===
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn forward_fused_scaled_neon(
    pd: usize, ph: usize,
    packed_w1: &[f32], packed_b1: &[f32], w1_row_offsets: &[usize],
    packed_w2_scaled: &[f32], w2_row_offsets: &[usize],
    out_bias: &[f32; 5], clip_hidden: f32, clip_logits: f32,
    x: &[f32], hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    use core::arch::aarch64::*;
    // hidden
    for j in 0..ph {
        let base = w1_row_offsets[j];
        let mut acc = vdupq_n_f32(0.0);
        let mut i = 0usize;
        while i + 4 <= pd {
            let rv = vld1q_f32(packed_w1.as_ptr().add(base + i));
            let xv = vld1q_f32(x.as_ptr().add(i));
            acc = vfmaq_f32(acc, rv, xv);
            i += 4;
        }
        let mut h = vaddvq_f32(acc) + packed_b1[j];
        while i < pd { h += packed_w1[base + i] * x[i]; i += 1; }
        if h < 0.0 { h = 0.0; }
        if h >  clip_hidden { h =  clip_hidden; }
        if h < -clip_hidden { h = -clip_hidden; }
        hidden_out[j] = h;
    }
    // logits
    for r in 0..5 {
        let base = w2_row_offsets[r];
        let mut acc = vdupq_n_f32(0.0);
        let mut j = 0usize;
        while j + 4 <= ph {
            let wv = vld1q_f32(packed_w2_scaled.as_ptr().add(base + j));
            let hv = vld1q_f32(hidden_out.as_ptr().add(j));
            acc = vfmaq_f32(acc, wv, hv);
            j += 4;
        }
        let mut v = vaddvq_f32(acc) + out_bias[r];
        while j < ph { v += packed_w2_scaled[base + j] * hidden_out[j]; j += 1; }
        if v >  clip_logits { v =  clip_logits; }
        if v < -clip_logits { v = -clip_logits; }
        logits_out[r] = v;
    }
    Ok(())
}

/// === ADD (verbatim): AVX2 fused dense (bf16) ===
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn forward_fused_scaled_bf16_avx2(
    pd: usize, ph: usize,
    w1_bf16: &[u16], b1: &[f32], w1_row_offsets: &[usize],
    w2s_bf16: &[u16], w2_row_offsets: &[usize],
    out_bias: &[f32; 5], clip_hidden: f32, clip_logits: f32,
    x: &[f32], hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    use core::arch::x86_64::*;
    #[inline(always)] unsafe fn widen_bf16_8(ptr: *const u16) -> __m256 {
        let h = _mm_loadu_si128(ptr as *const _);                      // 8x u16
        let w32 = _mm256_cvtepu16_epi32(h);                            // 8x u32
        let w32s = _mm256_slli_epi32::<16>(w32);                       // <<16
        core::mem::transmute::<__m256i, __m256>(w32s)                  // reinterpret as f32
    }
    #[inline(always)] unsafe fn hsum256_ps(v: __m256) -> f32 {
        let x = _mm256_hadd_ps(v, v); let y = _mm256_hadd_ps(x, x);
        _mm_cvtss_f32(_mm_add_ss(_mm256_castps256_ps128(y), _mm256_extractf128_ps(y, 1)))
    }

    // hidden
    for j in 0..ph {
        let base = w1_row_offsets[j];
        let row = w1_bf16.as_ptr().add(base);
        let mut acc = _mm256_setzero_ps();

        let mut i = 0usize;
        while i + 8 <= pd {
            let wv = widen_bf16_8(row.add(i));
            let xv = _mm256_loadu_ps(x.as_ptr().add(i));
            acc = _mm256_fmadd_ps(wv, xv, acc);
            i += 8;
        }
        let mut h = hsum256_ps(acc) + b1[j];
        while i < pd {
            let w = (w1_bf16[base + i] as u32) << 16;
            h += f32::from_bits(w) * x[i];
            i += 1;
        }
        if h < 0.0 { h = 0.0; }
        if h >  clip_hidden { h =  clip_hidden; }
        if h < -clip_hidden { h = -clip_hidden; }
        hidden_out[j] = h;
    }

    // logits
    for r in 0..5 {
        let base = w2_row_offsets[r];
        let mut acc = _mm256_setzero_ps();
        let mut j = 0usize;
        while j + 8 <= ph {
            let wv = widen_bf16_8(w2s_bf16.as_ptr().add(base + j));
            let hv = _mm256_loadu_ps(hidden_out.as_ptr().add(j));
            acc = _mm256_fmadd_ps(wv, hv, acc);
            j += 8;
        }
        let mut v = hsum256_ps(acc) + out_bias[r];
        while j < ph {
            let w = (w2s_bf16[base + j] as u32) << 16;
            v += f32::from_bits(w) * hidden_out[j];
            j += 1;
        }
        if v >  clip_logits { v =  clip_logits; }
        if v < -clip_logits { v = -clip_logits; }
        logits_out[r] = v;
    }
    Ok(())
}

/// === ADD (verbatim): AVX2 stream-fused (bf16 + SoA-5) ===
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn forward_fused_scaled_stream_avx2_soa5_bf16(
    pd: usize, ph: usize,
    w1_bf16: &[u16], b1: &[f32], w1_row_offsets: &[usize],
    w2_soa5_bf16: &[u16], out_bias: &[f32;5],
    clip_hidden: f32, clip_logits: f32,
    x: &[f32], hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    use core::arch::x86_64::*;
    #[inline(always)] unsafe fn widen_bf16_8(ptr: *const u16) -> __m256 {
        let h = _mm_loadu_si128(ptr as *const _);
        let w32 = _mm256_cvtepu16_epi32(h);
        let w32s = _mm256_slli_epi32::<16>(w32);
        core::mem::transmute::<__m256i, __m256>(w32s)
    }
    #[inline(always)] unsafe fn hsum256_ps(v: __m256) -> f32 {
        let x = _mm256_hadd_ps(v, v); let y = _mm256_hadd_ps(x, x);
        _mm_cvtss_f32(_mm_add_ss(_mm256_castps256_ps128(y), _mm256_extractf128_ps(y, 1)))
    }

    let mut acc = [0.0f32; 5];
    acc.copy_from_slice(out_bias);

    for j in 0..ph {
        let base = w1_row_offsets[j];
        let mut a0 = _mm256_setzero_ps();
        let mut i = 0usize;
        while i + 8 <= pd {
            let wv = widen_bf16_8(w1_bf16.as_ptr().add(base + i));
            let xv = _mm256_loadu_ps(x.as_ptr().add(i));
            a0 = _mm256_fmadd_ps(wv, xv, a0);
            if USE_PREFETCH { prefetch_row(x.as_ptr(), i + 64); }
            i += 8;
        }
        let mut h = hsum256_ps(a0) + b1[j];
        while i < pd {
            let w = (w1_bf16[base + i] as u32) << 16;
            h += f32::from_bits(w) * x[i];
            i += 1;
        }

        if h < 0.0 { h = 0.0; }
        if h >  clip_hidden { h =  clip_hidden; }
        if h < -clip_hidden { h = -clip_hidden; }
        if unsafe { STORE_HIDDEN_STREAM } {
            *hidden_out.get_unchecked_mut(j) = h;
        }

        let wptr = w2_soa5_bf16.as_ptr().add(5 * j);
        acc[0] = acc[0].mul_add(f32::from_bits(((*wptr.add(0)) as u32) << 16), h);
        acc[1] = acc[1].mul_add(f32::from_bits(((*wptr.add(1)) as u32) << 16), h);
        acc[2] = acc[2].mul_add(f32::from_bits(((*wptr.add(2)) as u32) << 16), h);
        acc[3] = acc[3].mul_add(f32::from_bits(((*wptr.add(3)) as u32) << 16), h);
        acc[4] = acc[4].mul_add(f32::from_bits(((*wptr.add(4)) as u32) << 16), h);
    }

    for r in 0..5 {
        let mut v = acc[r];
        if v >  clip_logits { v =  clip_logits; }
        if v < -clip_logits { v = -clip_logits; }
        logits_out[r] = v;
    }
    _mm256_zeroupper();
    Ok(())
}

/// === ADD (verbatim): AVX2 input-sparse (bf16) ===
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn forward_input_sparse_scaled_bf16_avx2(
    ph: usize,
    w1t_bf16: &[u16], b1: &[f32], w1_col_offsets: &[usize],
    w2s_bf16: &[u16], w2_row_offsets: &[usize],
    out_bias: &[f32; 5], clip_hidden: f32, clip_logits: f32,
    x: &[f32], nz_idx: &[usize],
    hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    use core::arch::x86_64::*;
    #[inline(always)] unsafe fn widen_bf16_8(ptr: *const u16) -> __m256 {
        let h = _mm_loadu_si128(ptr as *const _);
        let w32 = _mm256_cvtepu16_epi32(h);
        let w32s = _mm256_slli_epi32::<16>(w32);
        core::mem::transmute::<__m256i, __m256>(w32s)
    }
    // seed with b1
    let mut j = 0usize; while j + 8 <= ph {
        let v = _mm256_loadu_ps(b1.as_ptr().add(j));
        _mm256_storeu_ps(hidden_out.as_mut_ptr().add(j), v); j += 8;
    }
    while j < ph { hidden_out[j] = b1[j]; j += 1; }

    for &i in nz_idx {
        let base = w1_col_offsets[i];
        let col = w1t_bf16.as_ptr().add(base);
        let xib = _mm256_set1_ps(x[i]);

        let mut k = 0usize;
        while k + 8 <= ph {
            let wv = widen_bf16_8(col.add(k));
            let dst = _mm256_loadu_ps(hidden_out.as_ptr().add(k));
            let sum = _mm256_fmadd_ps(wv, xib, dst);
            _mm256_storeu_ps(hidden_out.as_mut_ptr().add(k), sum);
            k += 8;
        }
        while k < ph {
            let w = f32::from_bits(((*col.add(k)) as u32) << 16);
            hidden_out[k] = hidden_out[k].mul_add(1.0, w * x[i]);
            k += 1;
        }
    }
    // ReLU + clamp
    let zero = 0.0f32;
    for v in hidden_out.iter_mut() {
        let mut t = *v;
        if t < zero { t = zero; }
        if t >  clip_hidden { t =  clip_hidden; }
        if t < -clip_hidden { t = -clip_hidden; }
        *v = t;
    }

    // logits
    for r in 0..5 {
        let base = w2_row_offsets[r];
        let mut acc = _mm256_setzero_ps();
        let mut k = 0usize;
        while k + 8 <= ph {
            let wv = widen_bf16_8(w2s_bf16.as_ptr().add(base + k));
            let hv = _mm256_loadu_ps(hidden_out.as_ptr().add(k));
            acc = _mm256_fmadd_ps(wv, hv, acc);
            k += 8;
        }
        let mut v = {
            let x = _mm256_hadd_ps(acc, acc); let y = _mm256_hadd_ps(x, x);
            _mm_cvtss_f32(_mm_add_ss(_mm256_castps256_ps128(y), _mm256_extractf128_ps(y, 1)))
        } + out_bias[r];
        while k < ph {
            v += f32::from_bits(((w2s_bf16[base + k]) as u32) << 16) * hidden_out[k];
            k += 1;
        }
        if v >  clip_logits { v =  clip_logits; }
        if v < -clip_logits { v = -clip_logits; }
        logits_out[r] = v;
    }
    _mm256_zeroupper();
    Ok(())
}

/// === ADD (verbatim): AArch64 bf16 fused dense widen (fallback to f32 kernels otherwise) ===
#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn forward_fused_scaled_bf16_neon(
    pd: usize, ph: usize,
    w1_bf16: &[u16], b1: &[f32], w1_row_offsets: &[usize],
    w2s_bf16: &[u16], w2_row_offsets: &[usize],
    out_bias: &[f32; 5], clip_hidden: f32, clip_logits: f32,
    x: &[f32], hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    use core::arch::aarch64::*;
    #[inline(always)] unsafe fn widen_bf16_4(ptr: *const u16) -> float32x4_t {
        let h = vld1_u16(ptr);
        let u32x4 = vshll_n_u16(vget_low_u16(vcombine_u16(h,h)), 16);
        core::mem::transmute::<uint32x4_t, float32x4_t>(u32x4)
    }
    // hidden
    for j in 0..ph {
        let base = w1_row_offsets[j];
        let mut acc = vdupq_n_f32(0.0);
        let mut i = 0usize;
        while i + 4 <= pd {
            let wv = widen_bf16_4(w1_bf16.as_ptr().add(base + i));
            let xv = vld1q_f32(x.as_ptr().add(i));
            acc = vfmaq_f32(acc, wv, xv);
            i += 4;
        }
        let mut h = vaddvq_f32(acc) + b1[j];
        while i < pd {
            let w = f32::from_bits(((w1_bf16[base + i]) as u32) << 16);
            h += w * x[i]; i += 1;
        }
        if h < 0.0 { h = 0.0; }
        if h >  clip_hidden { h =  clip_hidden; }
        if h < -clip_hidden { h = -clip_hidden; }
        hidden_out[j] = h;
    }
    // logits (identical widen)
    for r in 0..5 {
        let base = w2_row_offsets[r];
        let mut acc = vdupq_n_f32(0.0);
        let mut j = 0usize;
        while j + 4 <= ph {
            let wv = widen_bf16_4(w2s_bf16.as_ptr().add(base + j));
            let hv = vld1q_f32(hidden_out.as_ptr().add(j));
            acc = vfmaq_f32(acc, wv, hv);
            j += 4;
        }
        let mut v = vaddvq_f32(acc) + out_bias[r];
        while j < ph { v += f32::from_bits(((w2s_bf16[base + j]) as u32) << 16) * hidden_out[j]; j += 1; }
        if v >  clip_logits { v =  clip_logits; }
        if v < -clip_logits { v = -clip_logits; }
        logits_out[r] = v;
    }
    Ok(())
}

/// === ADD (verbatim): AVX2 stream-fused using SoA-5 (f32) ===
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn forward_fused_scaled_stream_avx2_soa5(
    pd: usize, ph: usize,
    packed_w1: &[f32], packed_b1: &[f32], w1_row_offsets: &[usize],
    w2s_soa5: &[f32],                              // <-- SoA-5
    out_bias: &[f32; 5], clip_hidden: f32, clip_logits: f32,
    x: &[f32], hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    use core::arch::x86_64::*;
    #[inline(always)] unsafe fn hsum256_ps(v: __m256) -> f32 {
        let x = _mm256_hadd_ps(v, v); let y = _mm256_hadd_ps(x, x);
        _mm_cvtss_f32(_mm_add_ss(_mm256_castps256_ps128(y), _mm256_extractf128_ps(y, 1)))
    }

    // accumulators with bias
    let mut acc = [0.0f32; 5];
    acc.copy_from_slice(out_bias);

    for j in 0..ph {
        let base = w1_row_offsets[j];
        let row_ptr = packed_w1.as_ptr().add(base);
        let x_ptr   = x.as_ptr();

        let mut a0 = _mm256_setzero_ps();
        let mut i = 0usize;
        while i + 8 <= pd {
            let rv = _mm256_loadu_ps(row_ptr.add(i));
            let xv = _mm256_loadu_ps(x_ptr.add(i));
            a0 = _mm256_fmadd_ps(rv, xv, a0);
            if USE_PREFETCH { prefetch_row(row_ptr, i + 64); prefetch_row(x_ptr, i + 64); }
            i += 8;
        }
        let mut h = hsum256_ps(a0) + *packed_b1.get_unchecked(j);
        while i < pd { h += *row_ptr.add(i) * *x_ptr.add(i); i += 1; }

        if h < 0.0 { h = 0.0; }
        if h >  clip_hidden { h =  clip_hidden; }
        if h < -clip_hidden { h = -clip_hidden; }
        
        if unsafe { STORE_HIDDEN_STREAM } {
            *hidden_out.get_unchecked_mut(j) = h;
        }

        // SoA-5 contiguous weights
        let wptr = w2s_soa5.as_ptr().add(5 * j);
        acc[0] = acc[0].mul_add(*wptr.add(0), h);
        acc[1] = acc[1].mul_add(*wptr.add(1), h);
        acc[2] = acc[2].mul_add(*wptr.add(2), h);
        acc[3] = acc[3].mul_add(*wptr.add(3), h);
        acc[4] = acc[4].mul_add(*wptr.add(4), h);
    }

    for r in 0..5 {
        let mut v = acc[r];
        if v >  clip_logits { v =  clip_logits; }
        if v < -clip_logits { v = -clip_logits; }
        logits_out[r] = v;
    }
    _mm256_zeroupper();
    Ok(())
}

/// === ADD (verbatim): kernel type aliases (static binding) ===
type FwdDenseF32 = fn(
    usize, usize,
    &[f32], &[f32], &[usize],
    &[f32], &[usize],
    &[f32;5], f32, f32,
    &[f32], &mut [f32], &mut [f32]
) -> Result<(), PredictionError>;

type FwdSparseF32 = fn(
    usize,
    &[f32], &[f32], &[usize],
    &[f32], &[usize],
    &[f32;5], f32, f32,
    &[f32], &[usize],
    &mut [f32], &mut [f32]
) -> Result<(), PredictionError>;

type FwdDenseF16 = fn(
    usize, usize,
    &[u16], &[f32], &[usize],
    &[u16], &[usize],
    &[f32;5], f32, f32,
    &[f32], &mut [f32], &mut [f32]
) -> Result<(), PredictionError>;

type FwdSparseF16 = fn(
    usize,
    &[u16], &[f32], &[usize],
    &[u16], &[usize],
    &[f32;5], f32, f32,
    &[f32], &[usize],
    &mut [f32], &mut [f32]
) -> Result<(), PredictionError>;

/// === ADD (verbatim): safe wrappers to target-feature kernels (for fn ptrs) ===
#[inline(always)]
fn call_fused_scalar(
    pd: usize, ph: usize,
    w1: &[f32], b1: &[f32], w1r: &[usize],
    w2s: &[f32], w2r: &[usize],
    ob: &[f32;5], ch: f32, cl: f32,
    x: &[f32], h: &mut [f32], z: &mut [f32]
) -> Result<(), PredictionError> {
    forward_fused_scaled_scalar(pd, ph, w1, b1, w1r, w2s, w2r, ob, ch, cl, x, h, z)
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn call_fused_avx2(
    pd: usize, ph: usize,
    w1: &[f32], b1: &[f32], w1r: &[usize],
    w2s: &[f32], w2r: &[usize],
    ob: &[f32;5], ch: f32, cl: f32,
    x: &[f32], h: &mut [f32], z: &mut [f32]
) -> Result<(), PredictionError> {
    unsafe { forward_fused_scaled_avx2(pd, ph, w1, b1, w1r, w2s, w2r, ob, ch, cl, x, h, z) }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn call_fused_avx512(
    pd: usize, ph: usize,
    w1: &[f32], b1: &[f32], w1r: &[usize],
    w2s: &[f32], w2r: &[usize],
    ob: &[f32;5], ch: f32, cl: f32,
    x: &[f32], h: &mut [f32], z: &mut [f32]
) -> Result<(), PredictionError> {
    unsafe { forward_fused_scaled_avx512(pd, ph, w1, b1, w1r, w2s, w2r, ob, ch, cl, x, h, z) }
}

#[inline(always)]
fn call_sparse_scalar(
    ph: usize,
    w1t: &[f32], b1: &[f32], w1c: &[usize],
    w2s: &[f32], w2r: &[usize],
    ob: &[f32;5], ch: f32, cl: f32,
    x: &[f32], nz: &[usize],
    h: &mut [f32], z: &mut [f32]
) -> Result<(), PredictionError> {
    forward_input_sparse_scaled_scalar(ph, w1t, b1, w1c, w2s, w2r, ob, ch, cl, x, nz, h, z)
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn call_sparse_avx2(
    ph: usize,
    w1t: &[f32], b1: &[f32], w1c: &[usize],
    w2s: &[f32], w2r: &[usize],
    ob: &[f32;5], ch: f32, cl: f32,
    x: &[f32], nz: &[usize],
    h: &mut [f32], z: &mut [f32]
) -> Result<(), PredictionError> {
    unsafe { forward_input_sparse_scaled_avx2(ph, w1t, b1, w1c, w2s, w2r, ob, ch, cl, x, nz, h, z) }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn call_sparse_avx512(
    ph: usize,
    w1t: &[f32], b1: &[f32], w1c: &[usize],
    w2s: &[f32], w2r: &[usize],
    ob: &[f32;5], ch: f32, cl: f32,
    x: &[f32], nz: &[usize],
    h: &mut [f32], z: &mut [f32]
) -> Result<(), PredictionError> {
    unsafe { forward_input_sparse_scaled_avx512(ph, w1t, b1, w1c, w2s, w2r, ob, ch, cl, x, nz, h, z) }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn call_fused_f16_avx2(
    pd: usize, ph: usize,
    w1h: &[u16], b1: &[f32], w1r: &[usize],
    w2h: &[u16], w2r: &[usize],
    ob: &[f32;5], ch: f32, cl: f32,
    x: &[f32], h: &mut [f32], z: &mut [f32]
) -> Result<(), PredictionError> {
    unsafe { forward_fused_scaled_f16_avx2(pd, ph, w1h, b1, w1r, w2h, w2r, ob, ch, cl, x, h, z) }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn call_fused_f16_avx512(
    pd: usize, ph: usize,
    w1h: &[u16], b1: &[f32], w1r: &[usize],
    w2h: &[u16], w2r: &[usize],
    ob: &[f32;5], ch: f32, cl: f32,
    x: &[f32], h: &mut [f32], z: &mut [f32]
) -> Result<(), PredictionError> {
    unsafe { forward_fused_scaled_f16_avx512(pd, ph, w1h, b1, w1r, w2h, w2r, ob, ch, cl, x, h, z) }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn call_sparse_f16_avx2(
    ph: usize,
    w1th: &[u16], b1: &[f32], w1c: &[usize],
    w2h: &[u16], w2r: &[usize],
    ob: &[f32;5], ch: f32, cl: f32,
    x: &[f32], nz: &[usize],
    h: &mut [f32], z: &mut [f32]
) -> Result<(), PredictionError> {
    unsafe { forward_input_sparse_scaled_f16_avx2(ph, w1th, b1, w1c, w2h, w2r, ob, ch, cl, x, nz, h, z) }
}

#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn call_sparse_f16_avx512(
    ph: usize,
    w1th: &[u16], b1: &[f32], w1c: &[usize],
    w2h: &[u16], w2r: &[usize],
    ob: &[f32;5], ch: f32, cl: f32,
    x: &[f32], nz: &[usize],
    h: &mut [f32], z: &mut [f32]
) -> Result<(), PredictionError> {
    unsafe { forward_input_sparse_scaled_f16_avx512(ph, w1th, b1, w1c, w2h, w2r, ob, ch, cl, x, nz, h, z) }
}

/// === ADD (verbatim): binding flags for SoA-5 selection ===
#[derive(Copy, Clone)]
enum DenseLayout { RowMajor, SoA5 }

/// === ADD (verbatim): utility to order class indices by last argmax ===
#[inline(always)]
fn class_eval_order(last_top: usize) -> [usize;5] {
    // Put last winner first, then the rest in fixed order to be branch-predictable
    match last_top {
        0 => [0,1,2,3,4],
        1 => [1,0,2,3,4],
        2 => [2,0,1,3,4],
        3 => [3,0,1,2,4],
        _ => [4,0,1,2,3],
    }
}

/// === ADD (verbatim): Linux page-lock + hugepage + prefault helpers ===
const ENABLE_PAGE_LOCK: bool      = true;  // lock weights into RAM (best-effort)
const ENABLE_HUGEPAGE_ADVICE: bool = true; // advise THP (best-effort)

#[cfg(target_os = "linux")]
mod os_mem {
    use core::ffi::c_void;

    pub const MADV_WILLNEED: i32 = 3;
    pub const MADV_HUGEPAGE: i32 = 14;

    extern "C" {
        fn madvise(addr: *mut c_void, len: usize, advice: i32) -> i32;
        fn mlock(addr: *const c_void, len: usize) -> i32;
    }

    #[inline(always)]
    pub unsafe fn advise_and_lock(slice: &mut [f32]) {
        let ptr = slice.as_mut_ptr() as *mut c_void;
        let len = slice.len() * core::mem::size_of::<f32>();

        // best-effort: ignore return codes
        let _ = madvise(ptr, len, MADV_WILLNEED);
        #[allow(clippy::let_and_return)]
        let _ = if super::ENABLE_HUGEPAGE_ADVICE { madvise(ptr, len, MADV_HUGEPAGE) } else { 0 };
        let _ = mlock(ptr, len);

        // pre-fault pages to avoid first-hit latency in hot path
        let stride = 4096 / core::mem::size_of::<f32>(); // one read per page
        let mut i = 0usize;
        while i < slice.len() {
            core::ptr::read_volatile(slice.as_ptr().add(i));
            i += stride.max(1);
        }
    }
}

#[inline(always)]
fn lock_and_prefault_weights(
    w1: &mut [f32],
    w1t: &mut [f32],
    w2_scaled: &mut [f32],
    b1: &mut [f32],
) {
    #[cfg(target_os = "linux")]
    unsafe {
        if ENABLE_PAGE_LOCK {
            os_mem::advise_and_lock(w1);
            os_mem::advise_and_lock(w1t);
            os_mem::advise_and_lock(w2_scaled);
            os_mem::advise_and_lock(b1);
        }
    }
    // Non-Linux: no-op, safe and portable
}

/// === ADD (verbatim): Linux page-lock + prefault for u16 buffers ===
#[inline(always)]
fn lock_and_prefault_weights_f16(
    w1_f16: &mut [u16],
    w1t_f16: &mut [u16],
    w2s_f16: &mut [u16],
) {
    #[cfg(target_os = "linux")]
    unsafe {
        use core::ffi::c_void;
        #[allow(non_camel_case_types)]
        type i32_t = i32;

        extern "C" {
            fn madvise(addr: *mut c_void, len: usize, advice: i32_t) -> i32_t;
            fn mlock(addr: *const c_void, len: usize) -> i32_t;
        }
        const MADV_WILLNEED: i32_t = 3;
        const MADV_HUGEPAGE: i32_t = 14;

        #[inline(always)]
        unsafe fn advise_and_lock_u16(buf: &mut [u16]) {
            if buf.is_empty() { return; }
            let ptr = buf.as_mut_ptr() as *mut c_void;
            let len = buf.len() * core::mem::size_of::<u16>();
            let _ = madvise(ptr, len, MADV_WILLNEED);
            let _ = if ENABLE_HUGEPAGE_ADVICE { madvise(ptr, len, MADV_HUGEPAGE) } else { 0 };
            let _ = mlock(ptr, len);

            // pre-fault: touch one element per page
            let stride = 4096 / core::mem::size_of::<u16>();
            let mut i = 0usize;
            while i < buf.len() {
                core::ptr::read_volatile(buf.as_ptr().add(i));
                i += stride.max(1);
            }
        }

        advise_and_lock_u16(w1_f16);
        advise_and_lock_u16(w1t_f16);
        advise_and_lock_u16(w2s_f16);
    }
}

/// === ADD (verbatim): F16C packers (f32 -> IEEE-754 half) ===
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn pack_f32_to_f16_vec(src: &[f32]) -> Vec<u16> {
    // Requires F16C at runtime; call only when detected.
    use core::arch::x86_64::*;
    let n = src.len();
    let mut dst = vec![0u16; n];

    unsafe {
        let mut i = 0usize;
        const RNNE: i32 = _MM_FROUND_TO_NEAREST_INT | _MM_FROUND_NO_EXC;

        while i + 16 <= n {
            // two 8-float chunks
            let a0 = _mm256_loadu_ps(src.as_ptr().add(i));
            let a1 = _mm256_loadu_ps(src.as_ptr().add(i + 8));
            let h0 = _mm256_cvtps_ph(a0, RNNE);
            let h1 = _mm256_cvtps_ph(a1, RNNE);
            _mm_storeu_si128(dst.as_mut_ptr().add(i) as *mut __m128i, h0);
            _mm_storeu_si128(dst.as_mut_ptr().add(i + 8) as *mut __m128i, h1);
            i += 16;
        }
        while i + 8 <= n {
            let a = _mm256_loadu_ps(src.as_ptr().add(i));
            let h = _mm256_cvtps_ph(a, RNNE);
            _mm_storeu_si128(dst.as_mut_ptr().add(i) as *mut __m128i, h);
            i += 8;
        }
        while i + 4 <= n {
            let a = _mm_loadu_ps(src.as_ptr().add(i));
            let h = _mm_cvtps_ph(a, RNNE);
            _mm_storel_epi64(dst.as_mut_ptr().add(i) as *mut __m128i, h);
            i += 4;
        }
        if i < n {
            // tail <4
            let mut tmp = [0f32; 4];
            let rem = n - i;
            core::ptr::copy_nonoverlapping(src.as_ptr().add(i), tmp.as_mut_ptr(), rem);
            let a = _mm_loadu_ps(tmp.as_ptr());
            let h = _mm_cvtps_ph(a, RNNE);
            let mut out = [0u16; 4];
            _mm_storel_epi64(out.as_mut_ptr() as *mut __m128i, h);
            core::ptr::copy_nonoverlapping(out.as_ptr(), dst.as_mut_ptr().add(i), rem);
        }
    }
    dst
}



/// === ADD (verbatim): ring dedupe capacity ===
const DEDUPE_RING: usize = 8;

/// === ADD (verbatim): fast math toggles & cutoff ===
const USE_FAST_EXP: bool = true;         // set true for production (deterministic)
const USE_FAST_LOGISTIC: bool = true;    // set true for production (deterministic)
const SOFTMAX_EXP_CUTOFF: f32 = -20.0;   // e^(-20) ≈ 2.06e-9 ~ negligible

/// === ADD (verbatim): class min margins ===
const CLASS_MIN_MARGIN: [f32; 5] = [
    1.25, // Accumulate
    1.35, // Dump
    1.20, // Rotate
    1.60, // Spoof (strictest)
    1.10, // Snipe (fast lane)
];

/// === ADD (verbatim): enable FTZ/DAZ on x86_64 ===
#[inline(always)]
fn enable_denormals_zero_if_x86() {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use core::arch::x86_64::{_mm_getcsr, _mm_setcsr};
        // MXCSR bits: FTZ=1<<15 (0x8000), DAZ=1<<6 (0x0040)
        let mut mxcsr = _mm_getcsr();
        mxcsr |= 0x8000; // FTZ
        mxcsr |= 0x0040; // DAZ
        _mm_setcsr(mxcsr);
    }
}

/// === ADD (verbatim): round-up helper ===
#[inline(always)]
fn round_up_8(x: usize) -> usize { (x + 7) & !7 }

/// === ADD (verbatim): SIMD micro-pruned nz index builder ===
#[inline(always)]
fn build_nz_indices_micropruned(
    x: &[f32],                 // standardized & clamped features (len = d)
    w1_col_l1: &[f32],        // Σ|W1[:,i]| per feature (len = d)
    w2_linf_max: f32,         // max_r ||scaled_W2[r,:]||_∞
    out_idx: &mut Vec<usize>, // appended with passing indices
) -> usize {
    let start_len = out_idx.len();

    #[cfg(target_arch = "x86_64")]
    {
        if USE_SIMD_NZ_BUILDER && std::is_x86_feature_detected!("avx512f") {
            unsafe { return start_len + build_nz_avx512(x, w1_col_l1, w2_linf_max, out_idx) - start_len; }
        }
        if USE_SIMD_NZ_BUILDER && std::is_x86_feature_detected!("avx2") {
            unsafe { return start_len + build_nz_avx2(x, w1_col_l1, w2_linf_max, out_idx) - start_len; }
        }
    }
    // Scalar fallback: tight and branch-light
    let eps = INPUT_SPARSE_EPS;
    let drop = INPUT_SPARSE_DROP_EPS;
    for i in 0..x.len() {
        let ax = x[i].abs();
        if ax <= eps { continue; }
        let bound = ax * w1_col_l1[i] * w2_linf_max;
        if bound <= drop { continue; }
        out_idx.push(i);
    }
    out_idx.len()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn build_nz_avx512(
    x: &[f32], w1_col_l1: &[f32], w2_linf_max: f32, out_idx: &mut Vec<usize>
) -> usize {
    use core::arch::x86_64::*;
    #[inline(always)]
    unsafe fn abs_ps512(v: __m512) -> __m512 {
        let mask = _mm512_castsi512_ps(_mm512_set1_epi32(0x7fff_ffffu32 as i32));
        _mm512_and_ps(v, mask)
    }
    let n = x.len();
    let mut i = 0usize;
    let epsv  = _mm512_set1_ps(INPUT_SPARSE_EPS);
    let dropv = _mm512_set1_ps(INPUT_SPARSE_DROP_EPS);
    let w2v   = _mm512_set1_ps(w2_linf_max);

    while i + 16 <= n {
        let xv  = _mm512_loadu_ps(x.as_ptr().add(i));
        let l1v = _mm512_loadu_ps(w1_col_l1.as_ptr().add(i));
        let axv = abs_ps512(xv);
        let m1: __mmask16 = _mm512_cmp_ps_mask(axv, epsv,  _CMP_GT_OQ);
        // bound = |x| * l1 * w2max
        let bv = _mm512_mul_ps(_mm512_mul_ps(axv, l1v), w2v);
        let m2: __mmask16 = _mm512_cmp_ps_mask(bv,  dropv, _CMP_GT_OQ);
        let mut m = (m1 & m2) as u32;
        while m != 0 {
            let tz = m.trailing_zeros() as usize;
            out_idx.push(i + tz);
            m &= m - 1;
        }
        i += 16;
    }
    // tail
    while i < n {
        let ax = (*x.get_unchecked(i)).abs();
        if ax > INPUT_SPARSE_EPS {
            let bound = ax * *w1_col_l1.get_unchecked(i) * w2_linf_max;
            if bound > INPUT_SPARSE_DROP_EPS { out_idx.push(i); }
        }
        i += 1;
    }
    out_idx.len()
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn build_nz_avx2(
    x: &[f32], w1_col_l1: &[f32], w2_linf_max: f32, out_idx: &mut Vec<usize>
) -> usize {
    use core::arch::x86_64::*;
    let n = x.len();
    let mut i = 0usize;
    let epsv  = _mm256_set1_ps(INPUT_SPARSE_EPS);
    let dropv = _mm256_set1_ps(INPUT_SPARSE_DROP_EPS);
    let w2v   = _mm256_set1_ps(w2_linf_max);
    let absm  = _mm256_castsi256_ps(_mm256_set1_epi32(0x7fff_ffffu32 as i32));

    while i + 8 <= n {
        let xv  = _mm256_loadu_ps(x.as_ptr().add(i));
        let l1v = _mm256_loadu_ps(w1_col_l1.as_ptr().add(i));
        let axv = _mm256_and_ps(xv, absm);
        let m1  = _mm256_movemask_ps(_mm256_cmp_ps(axv, epsv,  _CMP_GT_OQ as i32));
        let bv  = _mm256_mul_ps(_mm256_mul_ps(axv, l1v), w2v);
        let m2  = _mm256_movemask_ps(_mm256_cmp_ps(bv,  dropv, _CMP_GT_OQ as i32));
        let mut m = (m1 & m2) as u32;
        while m != 0 {
            let tz = m.trailing_zeros() as usize;
            out_idx.push(i + tz);
            m &= m - 1;
        }
        i += 8;
    }
    while i < n {
        let ax = (*x.get_unchecked(i)).abs();
        if ax > INPUT_SPARSE_EPS {
            let bound = ax * *w1_col_l1.get_unchecked(i) * w2_linf_max;
            if bound > INPUT_SPARSE_DROP_EPS { out_idx.push(i); }
        }
        i += 1;
    }
    out_idx.len()
}

/// === ADD (verbatim): fast exp and logistic ===
#[inline(always)]
fn fast_exp_f32(x: f32) -> f32 {
    // Clamp to safe range: avoids under/overflow; softmax subtracts max so x<=0 typically.
    let x = x.clamp(-80.0, 80.0);
    // Range reduction: exp(x) = 2^(y) with y = x / ln(2) = x * INV_LN2
    const INV_LN2: f32 = 1.4426950408889634_f32;
    const LN2:     f32 = 0.6931471805599453_f32;

    let y = x * INV_LN2;
    let k = y.floor();
    let f = y - k; // in [0,1)

    // Degree-5 poly for 2^f via e^{f ln2}; Horner form (deterministic)
    const C0: f32 = 1.0;
    const C1: f32 = LN2;
    const C2: f32 = 0.5 * LN2 * LN2;
    const C3: f32 = (1.0/6.0)   * LN2 * LN2 * LN2;
    const C4: f32 = (1.0/24.0)  * LN2 * LN2 * LN2 * LN2;
    const C5: f32 = (1.0/120.0) * LN2 * LN2 * LN2 * LN2 * LN2;

    let p = C0 + f*(C1 + f*(C2 + f*(C3 + f*(C4 + f*C5))));

    // 2^k via exponent bits; x clamped ⇒ k in [-115,115] ⇒ normal range
    let ki = (k as i32) + 127;
    let pow2k = f32::from_bits((ki as u32) << 23);

    pow2k * p
}

#[inline(always)]
fn logistic_from_margin(inv_t: f32, margin: f32) -> f32 {
    if USE_FAST_LOGISTIC {
        let z = -margin * inv_t;
        let e = if USE_FAST_EXP { fast_exp_f32(z) } else { z.exp() };
        1.0 / (1.0 + e)
    } else {
        1.0 / (1.0 + (-margin * inv_t).exp())
    }
}

/// === REPLACE: fold_standardization_into_first_layer ===
/// w1'[j,i] = w1[j,i] / std[i];  b1'[j] = b1[j] - Σ_i (w1'[j,i] * mean[i])
#[inline(always)]
fn fold_standardization_into_first_layer(w: &mut ModelWeights) {
    let d = w.feature_count;
    let h = w.hidden_size;

    // Precompute inv std safely
    let mut inv_std = vec![1.0f32; d];
    for i in 0..d {
        let s = w.feature_std[i];
        inv_std[i] = if s.abs() < 1e-6 { 1.0 } else { 1.0 / s };
    }

    // Fold normalization into W1 and b1
    for j in 0..h {
        let base = j * d;
        let mut shift = 0.0f32;
        for i in 0..d {
            let idx = base + i;
            let wijp = w.w1[idx] * inv_std[i];
            shift += wijp * w.feature_mean[i];
            w.w1[idx] = wijp;
        }
        w.b1[j] -= shift;
    }

    // Make runtime standardization a true no-op
    w.feature_mean.fill(0.0);
    w.feature_std.fill(1.0);
}

/// === REPLACE: build_packed_weights ===
/// Build padded row-major W1, b1, row-major W2, and **column-major W1ᵀ** for input-sparse path.
#[inline(always)]
fn build_packed_weights(w: &ModelWeights) -> (
    usize,               // pd
    usize,               // ph
    Vec<f32>,            // packed_w1 (ph x pd)
    Vec<f32>,            // packed_b1 (ph)
    Vec<f32>,            // packed_w2 (5 x ph)
    Vec<f32>,            // packed_w1T (pd x ph)  <-- NEW
    Vec<usize>,          // w1_row_offsets (j -> j*pd)
    Vec<usize>,          // w1_col_offsets (i -> i*ph)  <-- NEW
    Vec<usize>,          // w2_row_offsets (r -> r*ph)
) {
    let d = w.feature_count;
    let h = w.hidden_size;
    let k = w.output_size; // 5

    let pd = round_up_8(d);
    let ph = round_up_8(h);

    // W1 row-major (ph x pd), zero-padded
    let mut pw1 = vec![0.0f32; ph * pd];
    for j in 0..h {
        let src = &w.w1[j * d .. j * d + d];
        let dst = &mut pw1[j * pd .. j * pd + d];
        dst.copy_from_slice(src);
    }

    // b1 padded
    let mut pb1 = vec![0.0f32; ph];
    pb1[..h].copy_from_slice(&w.b1[..h]);

    // W2 (k x ph), zero-padded
    let mut pw2 = vec![0.0f32; k * ph];
    for r in 0..k {
        let src = &w.w2[r * h .. r * h + h];
        let dst = &mut pw2[r * ph .. r * ph + h];
        dst.copy_from_slice(src);
    }

    // W1ᵀ column-major (pd x ph): for input-sparse accumulation
    let mut pw1t = vec![0.0f32; pd * ph];
    for j in 0..h {
        let row = &w.w1[j * d .. j * d + d];
        for i in 0..d {
            pw1t[i * ph + j] = row[i];
        }
    }
    // padded rows/cols remain zero

    let w1_row_offsets: Vec<usize> = (0..ph).map(|j| j * pd).collect();
    let w1_col_offsets: Vec<usize> = (0..pd).map(|i| i * ph).collect();
    let w2_row_offsets: Vec<usize> = (0..k).map(|r| r * ph).collect();

    (pd, ph, pw1, pb1, pw2, pw1t, w1_row_offsets, w1_col_offsets, w2_row_offsets)
}

/// === ADD (verbatim): calibration helpers ===
#[inline(always)]
fn apply_calibration(points: &[(f32, f32)], p: f32) -> f32 {
    if points.is_empty() { return p.clamp(0.0, 1.0); }
    let mut prev = points[0];
    if p <= prev.0 { return prev.1.clamp(0.0, 1.0); }
    for k in 1..points.len() {
        let cur = points[k];
        if p <= cur.0 {
            let t = (p - prev.0) / (cur.0 - prev.0);
            return (prev.1 + t * (cur.1 - prev.1)).clamp(0.0, 1.0);
        }
        prev = cur;
    }
    prev.1.clamp(0.0, 1.0)
}

/// === REPLACE: struct IntentPredictor (full) ===
pub struct IntentPredictor {
    w: ModelWeights,

    // audit + dedupe
    last_input_hash: Option<[u8; 32]>,
    last_result:     Option<(u64, [u8; 32], WhaleIntentScore)>,

    // ring dedupe (within-slot, burst safe)
    ring_hashes: [[u8; 32]; DEDUPE_RING],
    ring_slots:  [u64; DEDUPE_RING],
    ring_pos:    usize,
    ring_len:    usize,
    ring_scores: Vec<WhaleIntentScore>, // cap = DEDUPE_RING

    // scratch buffers (padded sizes)
    scratch_x:      Vec<f32>,   // len = p_feature_count
    scratch_hidden: Vec<f32>,   // len = p_hidden_size
    scratch_logits: Vec<f32>,   // len = 5
    scratch_nz:     Vec<usize>, // indices for input-sparse path

    // packed & padded (dense path)
    p_feature_count: usize,     // pd
    p_hidden_size:   usize,     // ph
    packed_w1:       Vec<f32>,  // ph x pd (row-major)
    packed_b1:       Vec<f32>,  // ph
    packed_w2:       Vec<f32>,  // 5 x ph (unscaled, kept for reload)
    packed_w2_scaled: Vec<f32>, // 5 x ph (scaled)

    // column-major W1ᵀ for input-sparse
    packed_w1t:      Vec<f32>,  // pd x ph (col-major)
    // offsets
    w1_row_offsets:  Vec<usize>,// j -> j*pd
    w1_col_offsets:  Vec<usize>,// i -> i*ph
    w2_row_offsets:  Vec<usize>,// r -> r*ph

    // pre-baked bias after gain
    out_bias: [f32; 5],       // (b2[r] * out_gain[r] + class_bias[r])

    // cached inverse temperature
    inv_t: f32,

    // gating
    min_confidence: f32,
    min_top2_ratio: f32,

    // micro-hysteresis
    hist:    [u8; HWIN],
    hist_len: usize,
    hist_pos: usize,

    // optional logit EMA (0 disables)
    prev_logits:     [f32; 5],
    logit_ema_alpha: f32,

    // slot-aware reset
    last_slot: Option<u64>,

    // optional per-class probability calibration
    calib: Option<[Vec<(f32,f32)>; 5]>,

    // sparse path metadata
    use_sparse: bool,
    w1_sparse_cols: Option<Vec<Vec<u32>>>, // mirrors w.w1_sparse_col_idx when used

    // === NEW: precomputed bounds for input-sparse micro-pruning ===
    w1_col_l1: Vec<f32>,     // len = feature_count, Σ_j |W1[j,i]|
    w2_row_linf: [f32; 5],   // per-class max_j |scaled_W2[r,j]|
    w2_linf_max: f32,        // max_r w2_row_linf[r]

    // === NEW: CPU feature flags (runtime) ===
    has_avx2_fma: bool,
    has_avx512f: bool,                // <-- NEW
    has_f16c: bool,

    // === NEW: adaptive input-sparse threshold ===
    input_sparse_threshold: f32, // starts at INPUT_SPARSE_USE_THRESHOLD, adapts via EMA
    nz_ema: f32,

    // === NEW: best-effort OS page lock indicator (Linux) ===
    os_pages_locked: bool,

    // === NEW: f16 storage for bandwidth cut (valid only when use_f16_weights=true) ===
    use_f16_weights: bool,
    packed_w1_f16: Vec<u16>,        // len = ph*pd
    packed_w1t_f16: Vec<u16>,       // len = pd*ph
    packed_w2_scaled_f16: Vec<u16>, // len = 5*ph

    // === NEW: static-bound hot kernels ===
    fwd_dense_f32: FwdDenseF32,
    fwd_sparse_f32: FwdSparseF32,
    fwd_dense_f16: Option<FwdDenseF16>,
    fwd_sparse_f16: Option<FwdSparseF16>,

    // SoA-5 interleaved W2 (by hidden j): [w2[0,j],..,w2[4,j]] for j in 0..ph
    packed_w2s_soa5: Vec<f32>,
    packed_w2s_soa5_f16: Vec<u16>,

    // bf16 storage
    use_bf16_weights: bool,
    packed_w1_bf16: Vec<u16>,
    packed_w1t_bf16: Vec<u16>,
    packed_w2_scaled_bf16: Vec<u16>,
    packed_w2s_soa5_bf16: Vec<u16>,

    // static-bound bf16 kernels
    fwd_dense_bf16: Option<fn(usize,usize,&[u16],&[f32],&[usize],&[u16],&[usize],&[f32;5],f32,f32,&[f32],&mut [f32],&mut [f32]) -> Result<(),PredictionError>>,
    fwd_sparse_bf16: Option<fn(usize,&[u16],&[f32],&[usize],&[u16],&[usize],&[f32;5],f32,f32,&[f32],&[usize],&mut [f32],&mut [f32]) -> Result<(),PredictionError>>,

    // Cost model for sparse/dense decision
    cost: CostModel,

    // === NEW: layout flags for dense path selection ===
    dense_f32_layout: DenseLayout,
    dense_bf16_layout: DenseLayout,
}

impl IntentPredictor {
    /// === REPLACE: constructor (FTZ/DAZ + AVX2 detect + bounds) ===
    pub fn new(path_to_model: &Path) -> Result<Self, PredictionError> {
        if ENABLE_FTZ_DAZ { enable_denormals_zero_if_x86(); }

        // Optional runtime overrides (no hot-path cost)
        let use_stream_fused_rt = env_flag("PREDICTOR_STREAM_FUSED", USE_STREAM_FUSED);
        let use_avx512_rt       = env_flag("PREDICTOR_USE_AVX512", USE_AVX512);
        let use_f16_rt          = env_flag("PREDICTOR_USE_F16_WEIGHTS", USE_F16_WEIGHTS);

        // Initialize cost model from env
        let cost = CostModel {
            dense_elem:  env_f32("PRED_COST_DENSE_ELEM", 1.00),
            sparse_elem: env_f32("PRED_COST_SPARSE_ELEM", 0.58),
            nz_overhead: env_f32("PRED_COST_NZ_OVERHEAD", 24.0),
        };

        // Initialize hidden store toggle
        init_store_hidden_stream_env();

        let model_bytes = std::fs::read(path_to_model)
            .map_err(|_| PredictionError::ModelNotFound(path_to_model.to_string_lossy().to_string()))?;
        let mut w = decode_and_verify_model(&model_bytes)?;
        fold_standardization_into_first_layer(&mut w);

        let (pd, ph, mut pw1, mut pb1, pw2, mut pw1t, w1_off, w1_col_off, w2_off) = build_packed_weights(&w);

        // gains/biases
        let mut class_gain = [1.0f32; 5];
        if !w.class_gain.is_empty() { for r in 0..5 { class_gain[r] = w.class_gain[r]; } }
        let mut class_bias = [0.0f32; 5];
        if !w.class_bias.is_empty() { for r in 0..5 { class_bias[r] = w.class_bias[r]; } }

        let mut out_gain = [0.0f32; 5];
        let mut out_bias = [0.0f32; 5];
        for r in 0..5 {
            let g = w.logit_gain * class_gain[r];
            out_gain[r] = g;
            out_bias[r] = w.b2[r] * g + class_bias[r];
        }

        // scale W2 rows once
        let mut pw2_scaled = vec![0.0f32; pw2.len()];
        for r in 0..5 {
            let base = r * ph;
            let row = &pw2[base .. base + ph];
            let dst = &mut pw2_scaled[base .. base + ph];
            let g = out_gain[r];
            for j in 0..ph { dst[j] = row[j] * g; }
        }

        // Decide offline sparse W1
        let mut use_sparse = false;
        let mut w1_sparse_cols = None;
        if !w.w1_sparse_col_idx.is_empty() && w.w1_sparse_col_idx.len() == w.hidden_size {
            let nnz: usize = w.w1_sparse_col_idx.iter().map(|v| v.len()).sum();
            let total = (w.hidden_size as f32) * (w.feature_count as f32);
            if (nnz as f32) <= SPARSE_USE_THRESHOLD * total {
                use_sparse = true;
                w1_sparse_cols = Some(w.w1_sparse_col_idx.clone());
            }
        }

        // === NEW: precompute bounds ===
        let d = w.feature_count;
        let h = w.hidden_size;

        // Σ_j |W1[j,i]|
        let mut w1_col_l1 = vec![0.0f32; d];
        for i in 0..d {
            let mut s = 0.0f32;
            let mut j = 0usize;
            while j < h {
                s += w.w1[j * d + i].abs();
                j += 1;
            }
            w1_col_l1[i] = s;
        }

        // per-class max_j |scaled_W2[r,j]| and max across classes
        let mut w2_row_linf = [0.0f32; 5];
        for r in 0..5 {
            let mut m = 0.0f32;
            let base = r * ph;
            let row = &pw2_scaled[base .. base + ph];
            let mut j = 0usize;
            while j < h { // scanning true hidden_size is enough; padded tail is zero
                let a = row[j].abs();
                if a > m { m = a; }
                j += 1;
            }
            w2_row_linf[r] = m;
        }
        let mut w2_linf_max = 0.0f32;
        for r in 0..5 { if w2_row_linf[r] > w2_linf_max { w2_linf_max = w2_row_linf[r]; } }

        // === NEW: precompute bounds ===
        let d = w.feature_count;
        let h = w.hidden_size;

        // Σ_j |W1[j,i]|
        let mut w1_col_l1 = vec![0.0f32; d];
        for i in 0..d {
            let mut s = 0.0f32;
            let mut j = 0usize;
            while j < h {
                s += w.w1[j * d + i].abs();
                j += 1;
            }
            w1_col_l1[i] = s;
        }

        // per-class max_j |scaled_W2[r,j]| and max across classes
        let mut w2_row_linf = [0.0f32; 5];
        for r in 0..5 {
            let mut m = 0.0f32;
            let base = r * ph;
            let row = &pw2_scaled[base .. base + ph];
            let mut j = 0usize;
            while j < h { // scanning true hidden_size is enough; padded tail is zero
                let a = row[j].abs();
                if a > m { m = a; }
                j += 1;
            }
            w2_row_linf[r] = m;
        }
    let mut w2_linf_max = 0.0f32;
    for r in 0..5 { if w2_row_linf[r] > w2_linf_max { w2_linf_max = w2_row_linf[r]; } }

    // feature detection
    let has_avx2_fma = {
        #[cfg(target_arch = "x86_64")]
        { std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") }
        #[cfg(not(target_arch = "x86_64"))]
        { false }
    };
    let has_avx512f = {
        #[cfg(target_arch = "x86_64")]
        { USE_AVX512 && std::is_x86_feature_detected!("avx512f") }
        #[cfg(not(target_arch = "x86_64"))]
        { false }
    };
    let has_f16c = {
        #[cfg(target_arch = "x86_64")]
        { std::is_x86_feature_detected!("f16c") }
        #[cfg(not(target_arch = "x86_64"))]
        { false }
    };

    // f16 pack (only if both toggle and F16C present)
    let use_f16_weights = use_f16_rt && has_f16c;
    let mut packed_w1_f16        = if use_f16_weights { pack_f32_to_f16_vec(&pw1) } else { Vec::new() };
    let mut packed_w1t_f16       = if use_f16_weights { pack_f32_to_f16_vec(&pw1t) } else { Vec::new() };
    let mut packed_w2_scaled_f16 = if use_f16_weights { pack_f32_to_f16_vec(&pw2_scaled) } else { Vec::new() };

    // --- bf16 path if f16 not used ---
    let use_bf16_weights = USE_BF16_WEIGHTS && !use_f16_weights;
    let mut packed_w1_bf16        = if use_bf16_weights { pack_f32_to_bf16_vec(&pw1) } else { Vec::new() };
    let mut packed_w1t_bf16       = if use_bf16_weights { pack_f32_to_bf16_vec(&pw1t) } else { Vec::new() };
    let mut packed_w2_scaled_bf16 = if use_bf16_weights { pack_f32_to_bf16_vec(&pw2_scaled) } else { Vec::new() };

    // --- SoA-5 builds (f32 already handled) ---
    let mut w2_soa5     = if USE_W2_SOA5 { build_w2_soa5_f32(&pw2_scaled, ph) } else { Vec::new() };
    let mut w2_soa5_f16 = if USE_W2_SOA5 && use_f16_weights { build_w2_soa5_f16(&packed_w2_scaled_f16, ph) } else { Vec::new() };
    let mut w2_soa5_bf16= if USE_W2_SOA5 && use_bf16_weights { build_w2_soa5_bf16(&packed_w2_scaled_bf16, ph) } else { Vec::new() };

    // 2MiB alignment for hot weights (opt-in)
    pw1        = to_aligned_vec_2mb_f32(pw1);
    pw1t       = to_aligned_vec_2mb_f32(pw1t);
    pw2_scaled = to_aligned_vec_2mb_f32(pw2_scaled);
    if use_f16_weights {
        packed_w1_f16        = to_aligned_vec_2mb_u16(packed_w1_f16);
        packed_w1t_f16       = to_aligned_vec_2mb_u16(packed_w1t_f16);
        packed_w2_scaled_f16 = to_aligned_vec_2mb_u16(packed_w2_scaled_f16);
    }
    if use_bf16_weights {
        packed_w1_bf16        = to_aligned_vec_2mb_u16(packed_w1_bf16);
        packed_w1t_bf16       = to_aligned_vec_2mb_u16(packed_w1t_bf16);
        packed_w2_scaled_bf16 = to_aligned_vec_2mb_u16(packed_w2_scaled_bf16);
    }
    if USE_W2_SOA5 {
        w2_soa5 = to_aligned_vec_2mb_f32(w2_soa5);
        if use_f16_weights { w2_soa5_f16  = to_aligned_vec_2mb_u16(w2_soa5_f16); }
        if use_bf16_weights { w2_soa5_bf16= to_aligned_vec_2mb_u16(w2_soa5_bf16); }
    }

    // THP hint
    advise_hugepage_f32(&[&pw1, &pw1t, &pw2_scaled, &w2_soa5]);
    advise_hugepage_u16(&[&packed_w1_f16, &packed_w1t_f16, &packed_w2_scaled_f16, &w2_soa5_f16]);
    advise_hugepage_u16(&[&packed_w1_bf16, &packed_w1t_bf16, &packed_w2_scaled_bf16, &w2_soa5_bf16]);

    // lock/prefault
    lock_and_prefault_weights(&mut pw1, &mut pw1t, &mut pw2_scaled, &mut pb1);
    if use_f16_weights {
        lock_and_prefault_weights_f16(
            unsafe { core::slice::from_raw_parts_mut(packed_w1_f16.as_ptr() as *mut u16, packed_w1_f16.len()) },
            unsafe { core::slice::from_raw_parts_mut(packed_w1t_f16.as_ptr() as *mut u16, packed_w1t_f16.len()) },
            unsafe { core::slice::from_raw_parts_mut(packed_w2_scaled_f16.as_ptr() as *mut u16, packed_w2_scaled_f16.len()) },
        );
    }
    if use_bf16_weights {
        // reuse the f16 locker (u16 pages) for bf16 buffers
        lock_and_prefault_weights_f16(
            unsafe { core::slice::from_raw_parts_mut(packed_w1_bf16.as_ptr() as *mut u16, packed_w1_bf16.len()) },
            unsafe { core::slice::from_raw_parts_mut(packed_w1t_bf16.as_ptr() as *mut u16, packed_w1t_bf16.len()) },
            unsafe { core::slice::from_raw_parts_mut(packed_w2_scaled_bf16.as_ptr() as *mut u16, packed_w2_scaled_bf16.len()) },
        );
    }

    // set RO + no-core-dump on weights (best-effort)
    protect_weights_readonly(&pw1, &pw1t, &pw2_scaled);
    if use_f16_weights {
        protect_weights_readonly_u16(&packed_w1_f16, &packed_w1t_f16, &packed_w2_scaled_f16);
    }
    if use_bf16_weights {
        protect_weights_readonly_u16(&packed_w1_bf16, &packed_w1t_bf16, &packed_w2_scaled_bf16);
    }

    // env-init for hidden store toggle
    init_store_hidden_stream_env();

    // === Static kernel binding ===
    let mut dense_f32_layout = DenseLayout::RowMajor;
    let mut dense_bf16_layout = DenseLayout::RowMajor;

    let fwd_dense_f32: FwdDenseF32 =
        if has_avx2_fma && use_stream_fused_rt && ph <= STREAM_FUSED_WHEN_PH_LE {
            #[cfg(target_arch = "x86_64")]
            {
                if USE_W2_SOA5 && !w2_soa5.is_empty() {
                    dense_f32_layout = DenseLayout::SoA5;
                    forward_fused_scaled_stream_avx2_soa5
                } else {
                    // your existing stream-fused (row-major) function
                    call_fused_avx2
                }
            }
            #[cfg(not(target_arch = "x86_64"))]
            { call_fused_scalar }
        } else if has_avx512f && use_avx512_rt {
            #[cfg(target_arch = "x86_64")]
            { call_fused_avx512 }
            #[cfg(not(target_arch = "x86_64"))]
            { call_fused_scalar }
        } else if has_avx2_fma {
            #[cfg(target_arch = "x86_64")]
            { call_fused_avx2 }
            #[cfg(not(target_arch = "x86_64"))]
            { call_fused_scalar }
        } else { call_fused_scalar };

    let fwd_sparse_f32: FwdSparseF32 =
        if has_avx512f && use_avx512_rt {
            #[cfg(target_arch = "x86_64")]
            { call_sparse_avx512 }
            #[cfg(not(target_arch = "x86_64"))]
            { call_sparse_scalar }
        } else if has_avx2_fma {
            #[cfg(target_arch = "x86_64")]
            { call_sparse_avx2 }
            #[cfg(not(target_arch = "x86_64"))]
            { call_sparse_scalar }
        } else { call_sparse_scalar };

    let fwd_dense_f16: Option<FwdDenseF16> = if use_f16_weights {
        Some(
            if has_avx512f && has_f16c {
                #[cfg(target_arch = "x86_64")]
                { call_fused_f16_avx512 }
                #[cfg(not(target_arch = "x86_64"))]
                { unreachable!() }
            } else if has_avx2_fma && has_f16c {
                #[cfg(target_arch = "x86_64")]
                { call_fused_f16_avx2 }
                #[cfg(not(target_arch = "x86_64"))]
                { unreachable!() }
            } else { None? }
        )
    } else { None };

    let fwd_sparse_f16: Option<FwdSparseF16> = if use_f16_weights {
        Some(
            if has_avx512f && has_f16c {
                #[cfg(target_arch = "x86_64")]
                { call_sparse_f16_avx512 }
                #[cfg(not(target_arch = "x86_64"))]
                { unreachable!() }
            } else if has_avx2_fma && has_f16c {
                #[cfg(target_arch = "x86_64")]
                { call_sparse_f16_avx2 }
                #[cfg(not(target_arch = "x86_64"))]
                { unreachable!() }
            } else { None? }
        )
    } else { None };

    // AArch64 NEON binding
    #[cfg(target_arch = "aarch64")]
    let fwd_dense_f32: FwdDenseF32 = {
        |pd,ph,w1,b1,w1r,w2s,w2r,ob,ch,cl,x,h,z| unsafe {
            forward_fused_scaled_neon(pd,ph,w1,b1,w1r,w2s,w2r,ob,ch,cl,x,h,z)
        }
    };

    #[cfg(target_arch = "aarch64")]
    let fwd_sparse_f32: FwdSparseF32 = {
        // Sparse NEON optional; scalar is already quite fast for small nz.
        call_sparse_scalar
    };

    // === NEW: bf16 bindings ===
    let fwd_dense_bf16 =
        if use_bf16_weights {
            #[cfg(target_arch="x86_64")]
            {
                Some(if has_avx2_fma {
                    if USE_W2_SOA5 && !w2_soa5_bf16.is_empty() && use_stream_fused_rt && ph <= STREAM_FUSED_WHEN_PH_LE {
                        dense_bf16_layout = DenseLayout::SoA5;
                        forward_fused_scaled_stream_avx2_soa5_bf16
                    } else {
                        forward_fused_scaled_bf16_avx2
                    }
                } else { unreachable!() })
            }
            #[cfg(not(target_arch="x86_64"))] { None }
        } else { None };

    let fwd_sparse_bf16 =
        if use_bf16_weights {
            #[cfg(target_arch="x86_64")]
            { Some(|ph,w1th,b1,w1c,w2h,w2r,ob,ch,cl,x,nz,h,z| unsafe {
                forward_input_sparse_scaled_bf16_avx2(ph,w1th,b1,w1c,w2h,w2r,ob,ch,cl,x,nz,h,z)
            }) }
            #[cfg(not(target_arch="x86_64"))] { None }
        } else { None };

    Ok(Self {
            w,
            last_input_hash: None,
            last_result: None,

            ring_hashes: [[0u8; 32]; DEDUPE_RING],
            ring_slots:  [0u64; DEDUPE_RING],
            ring_pos:    0,
            ring_len:    0,
            ring_scores: Vec::with_capacity(DEDUPE_RING),

            scratch_x: vec![0.0; pd],
            scratch_hidden: vec![0.0; ph],
            scratch_logits: vec![0.0; 5],
            scratch_nz: Vec::with_capacity(pd),

            p_feature_count: pd,
            p_hidden_size: ph,
            packed_w1: pw1,
            packed_b1: pb1,
            packed_w2: pw2,
            packed_w2_scaled: pw2_scaled,

            packed_w1t: pw1t,
            w1_row_offsets: w1_off,
            w1_col_offsets: w1_col_off,
            w2_row_offsets: w2_off,

            out_bias,
            inv_t: 1.0 / w.temperature,
            min_confidence: 0.50,
            min_top2_ratio: 1.03,

            hist: [u8::MAX; HWIN],
            hist_len: 0,
            hist_pos: 0,
            prev_logits: [0.0; 5],
            logit_ema_alpha: 0.0,

            last_slot: None,
            calib: None,

            use_sparse,
            w1_sparse_cols,

        w1_col_l1,
        w2_row_linf,
        w2_linf_max,

        has_avx2_fma,
        has_avx512f,
        has_f16c,

        input_sparse_threshold: INPUT_SPARSE_USE_THRESHOLD,
        nz_ema: INPUT_SPARSE_USE_THRESHOLD,
        os_pages_locked: true,

        use_f16_weights,
        packed_w1_f16,
        packed_w1t_f16,
        packed_w2_scaled_f16,

        fwd_dense_f32,
        fwd_sparse_f32,
        fwd_dense_f16,
        fwd_sparse_f16,

        packed_w2s_soa5: w2_soa5,
        packed_w2s_soa5_f16: w2_soa5_f16,

        use_bf16_weights,
        packed_w1_bf16,
        packed_w1t_bf16,
        packed_w2_scaled_bf16,
        packed_w2s_soa5_bf16: w2_soa5_bf16,

        fwd_dense_bf16,
        fwd_sparse_bf16,

        cost,

        dense_f32_layout,
        dense_bf16_layout,
        })
    }

    /// === REPLACE: predict ===
    pub fn predict(&mut self, features: &[f32], slot: u64) -> Result<WhaleIntentScore, PredictionError> {
        if features.len() != self.w.feature_count {
            return Err(PredictionError::FeatureSizeMismatch { expected: self.w.feature_count, actual: features.len() });
        }
        for &v in features { if !v.is_finite() { return Err(PredictionError::NumericalError); } }

        // slot-aware reset
        if self.last_slot.map_or(false, |prev| prev != slot) {
            self.hist_len = 0; self.hist_pos = 0; self.prev_logits = [0.0; 5];
            // Keep ring cache within-slot only
            self.ring_pos = 0; self.ring_len = 0; self.ring_scores.clear();
        }
        self.last_slot = Some(slot);

        // audit hash + dedupe
        let ih = hash_input_tensor(features);
        self.last_input_hash = Some(ih);

        // Fast-path 1: single-entry cache
        if let Some((pslot, phash, prev)) = &self.last_result {
            if *pslot == slot && *phash == ih { return Ok(*prev); }
        }
        // Fast-path 2: ring cache lookup
        for i in 0..self.ring_len {
            if self.ring_slots[i] == slot && self.ring_hashes[i] == ih {
                // safe: ring_scores length == ring_len (by construction)
                return Ok(self.ring_scores[i]);
            }
        }

        // Copy & clamp (per-feature if provided)
        standardize_and_clamp(features, &self.w.feature_min, &self.w.feature_max, &mut self.scratch_x);

        // Decide path:
        // 1) Offline W1-sparse, if enabled.
        // 2) Else, input-sparse if nz fraction is small.
        // 3) Else, dense packed.
        let d = self.w.feature_count;
        if self.use_sparse {
            self.fwd_sparse_f32(
                self.p_hidden_size,
                &self.packed_w1t,
                &self.packed_b1,
                &self.w1_col_offsets,
                &self.packed_w2_scaled,
                &self.w2_row_offsets,
                &self.out_bias,
                self.w.clip_hidden,
                self.w.clip_logits,
                &self.scratch_x[..d],
                &[], // nz unused here (offline sparse path)
                &mut self.scratch_hidden,
                &mut self.scratch_logits,
            )?;
        } else {
            // SIMD micro-pruned nz build (already present in your tree)
            self.scratch_nz.clear();
            let _ = build_nz_indices_micropruned(&self.scratch_x[..d], &self.w1_col_l1, self.w2_linf_max, &mut self.scratch_nz);
            let nz_frac = (self.scratch_nz.len() as f32) / (d as f32);

            if SORT_NZ_FOR_LOCALITY && self.scratch_nz.len() <= NZ_SORT_MAX {
                small_nz_locality_sort(&mut self.scratch_nz);
            }

            self.nz_ema = NZ_EMA_ALPHA.mul_add(nz_frac, (1.0 - NZ_EMA_ALPHA) * self.nz_ema);
            self.input_sparse_threshold = (self.nz_ema * DYN_THRESH_MULT).clamp(DYN_THRESH_MIN, DYN_THRESH_MAX);

            let use_sparse_now = should_use_sparse_cost(self.scratch_nz.len(), d, self.p_hidden_size, self.cost)
                              || (nz_frac <= self.input_sparse_threshold);
            if use_sparse_now {
                if self.use_f16_weights {
                    if let Some(f) = self.fwd_sparse_f16 {
                        f(
                            self.p_hidden_size,
                            &self.packed_w1t_f16,
                            &self.packed_b1,
                            &self.w1_col_offsets,
                            &self.packed_w2_scaled_f16,
                            &self.w2_row_offsets,
                            &self.out_bias,
                            self.w.clip_hidden,
                            self.w.clip_logits,
                            &self.scratch_x[..d],
                            &self.scratch_nz,
                            &mut self.scratch_hidden,
                            &mut self.scratch_logits,
                        )?;
                    } else {
                        // fallback to f32 kernels if f16 disabled at runtime
                        self.fwd_sparse_f32(
                            self.p_hidden_size,
                            &self.packed_w1t,
                            &self.packed_b1,
                            &self.w1_col_offsets,
                            &self.packed_w2_scaled,
                            &self.w2_row_offsets,
                            &self.out_bias,
                            self.w.clip_hidden,
                            self.w.clip_logits,
                            &self.scratch_x[..d],
                            &self.scratch_nz,
                            &mut self.scratch_hidden,
                            &mut self.scratch_logits,
                        )?;
                    }
                } else if self.use_bf16_weights {
                    if let Some(f) = self.fwd_sparse_bf16 {
                        f(
                            self.p_hidden_size,
                            &self.packed_w1t_bf16,
                            &self.packed_b1,
                            &self.w1_col_offsets,
                            &self.packed_w2_scaled_bf16,
                            &self.w2_row_offsets,
                            &self.out_bias,
                            self.w.clip_hidden,
                            self.w.clip_logits,
                            &self.scratch_x[..d],
                            &self.scratch_nz,
                            &mut self.scratch_hidden,
                            &mut self.scratch_logits,
                        )?;
                    } else {
                        self.fwd_sparse_f32(
                            self.p_hidden_size,
                            &self.packed_w1t,
                            &self.packed_b1,
                            &self.w1_col_offsets,
                            &self.packed_w2_scaled,
                            &self.w2_row_offsets,
                            &self.out_bias,
                            self.w.clip_hidden,
                            self.w.clip_logits,
                            &self.scratch_x[..d],
                            &self.scratch_nz,
                            &mut self.scratch_hidden,
                            &mut self.scratch_logits,
                        )?;
                    }
                } else {
                    self.fwd_sparse_f32(
                        self.p_hidden_size,
                        &self.packed_w1t,
                        &self.packed_b1,
                        &self.w1_col_offsets,
                        &self.packed_w2_scaled,
                        &self.w2_row_offsets,
                        &self.out_bias,
                        self.w.clip_hidden,
                        self.w.clip_logits,
                        &self.scratch_x[..d],
                        &self.scratch_nz,
                        &mut self.scratch_hidden,
                        &mut self.scratch_logits,
                    )?;
                }
            } else {
                if self.use_f16_weights {
                    if let Some(f) = self.fwd_dense_f16 {
                        f(
                            self.p_feature_count,
                            self.p_hidden_size,
                            &self.packed_w1_f16,
                            &self.packed_b1,
                            &self.w1_row_offsets,
                            &self.packed_w2_scaled_f16,
                            &self.w2_row_offsets,
                            &self.out_bias,
                            self.w.clip_hidden,
                            self.w.clip_logits,
                            &self.scratch_x,
                            &mut self.scratch_hidden,
                            &mut self.scratch_logits,
                        )?;
                    } else {
                        self.fwd_dense_f32(
                            self.p_feature_count,
                            self.p_hidden_size,
                            &self.packed_w1,
                            &self.packed_b1,
                            &self.w1_row_offsets,
                            &self.packed_w2_scaled,
                            &self.w2_row_offsets,
                            &self.out_bias,
                            self.w.clip_hidden,
                            self.w.clip_logits,
                            &self.scratch_x,
                            &mut self.scratch_hidden,
                            &mut self.scratch_logits,
                        )?;
                    }
                } else if self.use_bf16_weights {
                    if let Some(f) = self.fwd_dense_bf16 {
                        let w2 = match self.dense_bf16_layout {
                            DenseLayout::RowMajor => &self.packed_w2_scaled_bf16,
                            DenseLayout::SoA5     => &self.packed_w2s_soa5_bf16,
                        };
                        f(
                            self.p_feature_count,
                            self.p_hidden_size,
                            &self.packed_w1_bf16,
                            &self.packed_b1,
                            &self.w1_row_offsets,
                            w2, &self.w2_row_offsets,
                            &self.out_bias,
                            self.w.clip_hidden,
                            self.w.clip_logits,
                            &self.scratch_x,
                            &mut self.scratch_hidden,
                            &mut self.scratch_logits,
                        )?;
                    } else {
                        self.fwd_dense_f32(
                            self.p_feature_count,
                            self.p_hidden_size,
                            &self.packed_w1,
                            &self.packed_b1,
                            &self.w1_row_offsets,
                            &self.packed_w2_scaled,
                            &self.w2_row_offsets,
                            &self.out_bias,
                            self.w.clip_hidden,
                            self.w.clip_logits,
                            &self.scratch_x,
                            &mut self.scratch_hidden,
                            &mut self.scratch_logits,
                        )?;
                    }
                } else {
                    let w2 = match self.dense_f32_layout {
                        DenseLayout::RowMajor => &self.packed_w2_scaled,
                        DenseLayout::SoA5     => &self.packed_w2s_soa5,
                    };
                    self.fwd_dense_f32(
                        self.p_feature_count,
                        self.p_hidden_size,
                        &self.packed_w1,
                        &self.packed_b1,
                        &self.w1_row_offsets,
                        w2, &self.w2_row_offsets,
                        &self.out_bias,
                        self.w.clip_hidden,
                        self.w.clip_logits,
                        &self.scratch_x,
                        &mut self.scratch_hidden,
                        &mut self.scratch_logits,
                    )?;
                }
            }
        }

        // Optional EMA
        if self.logit_ema_alpha > 0.0 {
            let a = self.logit_ema_alpha;
            for i in 0..5 {
                self.scratch_logits[i] = a.mul_add(self.scratch_logits[i], (1.0 - a) * self.prev_logits[i]);
                self.prev_logits[i]   = self.scratch_logits[i];
            }
        }

        // argmax + second (branchless top-2 on 5 floats)
        let (bi, bv, sv) = top2_5([
            self.scratch_logits[0],
            self.scratch_logits[1], 
            self.scratch_logits[2],
            self.scratch_logits[3],
            self.scratch_logits[4]
        ]);
        let margin = bv - sv;

        // Per-class minimum margin (scaled by temperature)
        if !(margin.is_finite() && margin >= CLASS_MIN_MARGIN[bi] * self.inv_t) {
            // Fall through to full softmax (and likely fail gates cleanly)
        } else {
            // decisive early-exit with hysteresis
            if margin >= 12.0 * self.inv_t && self.hist_vote_ok(bi) {
                let mut conf = 0.999_999;
                // class-wise calibration if present
                if let Some(cal) = &self.calib { conf = apply_calibration(&cal[bi], conf); }
                let out = WhaleIntentScore { class: IntentClass::from_index(bi)?, confidence: conf, slot };
                self.hist_push(bi);
                self.last_result = Some((slot, ih, out));
                // ring insert
                if self.ring_len < DEDUPE_RING {
                    self.ring_hashes[self.ring_len] = ih;
                    self.ring_slots[self.ring_len] = slot;
                    self.ring_scores.push(out);
                    self.ring_len += 1;
                } else {
                    let idx = self.ring_pos;
                    self.ring_hashes[idx] = ih;
                    self.ring_slots[idx] = slot;
                    self.ring_scores[idx] = out;
                    self.ring_pos = (self.ring_pos + 1) % DEDUPE_RING;
                }
                return Ok(out);
            }

            // top-2 logistic shortcut
            if margin >= 8.0 * self.inv_t {
                // ⚡ fast, deterministic logistic
                let mut conf = logistic_from_margin(self.inv_t, margin);
                let min_conf = CLASS_MIN_CONF[bi].max(self.min_confidence);
                let ratio = conf / (1.0 - conf);
                if conf >= min_conf && ratio >= self.min_top2_ratio * CLASS_RATIO_MUL[bi] && self.hist_vote_ok(bi) {
                    if let Some(cal) = &self.calib { conf = apply_calibration(&cal[bi], conf); }
                    let out = WhaleIntentScore { class: IntentClass::from_index(bi)?, confidence: conf, slot };
                    self.hist_push(bi);
                    self.last_result = Some((slot, ih, out));
                    // ring insert
                    if self.ring_len < DEDUPE_RING {
                        self.ring_hashes[self.ring_len] = ih;
                        self.ring_slots[self.ring_len] = slot;
                        self.ring_scores.push(out);
                        self.ring_len += 1;
                    } else {
                        let idx = self.ring_pos;
                        self.ring_hashes[idx] = ih;
                        self.ring_slots[idx] = slot;
                        self.ring_scores[idx] = out;
                        self.ring_pos = (self.ring_pos + 1) % DEDUPE_RING;
                    }
                    return Ok(out);
                }
            }
        }

        // Full softmax
        let mut probs = [0.0f32; 5];
        softmax_stable(&self.scratch_logits, self.w.temperature, &mut probs)?;
        let (argmax, mut conf, second) = argmax_with_second(&probs);
        if !(conf.is_finite() && (0.0..=1.0).contains(&conf)) { return Err(PredictionError::InvalidConfidence(conf)); }

        let min_conf = CLASS_MIN_CONF[argmax].max(self.min_confidence);
        if conf < min_conf { return Err(PredictionError::NumericalError); }
        let ratio = if second > 0.0 { conf / second } else { f32::INFINITY };
        if ratio < self.min_top2_ratio * CLASS_RATIO_MUL[argmax] { return Err(PredictionError::NumericalError); }
        if !self.hist_vote_ok(argmax) { return Err(PredictionError::NumericalError); }

        if let Some(cal) = &self.calib { conf = apply_calibration(&cal[argmax], conf); }

        let out = WhaleIntentScore { class: IntentClass::from_index(argmax)?, confidence: conf, slot };
        self.hist_push(argmax);
        self.last_result = Some((slot, ih, out));

        // ring insert
        if self.ring_len < DEDUPE_RING {
            self.ring_hashes[self.ring_len] = ih;
            self.ring_slots[self.ring_len] = slot;
            self.ring_scores.push(out);
            self.ring_len += 1;
        } else {
            let idx = self.ring_pos;
            self.ring_hashes[idx] = ih;
            self.ring_slots[idx] = slot;
            self.ring_scores[idx] = out;
            self.ring_pos = (self.ring_pos + 1) % DEDUPE_RING;
        }
        Ok(out)
    }

    /// Get last input hash for audit logging
    #[inline]
    pub fn get_last_input_hash(&self) -> Option<[u8; 32]> {
        self.last_input_hash
    }

    /// Check if history vote is OK for a class
    #[inline(always)]
    fn hist_vote_ok(&self, class: usize) -> bool {
        if self.hist_len < HYST_MIN[class] as usize { return false; }
        let mut count = 0;
        for i in 0..self.hist_len {
            if self.hist[i] == class as u8 { count += 1; }
        }
        count >= HYST_MIN[class] as usize
    }

    /// Push a class to history
    #[inline(always)]
    fn hist_push(&mut self, class: usize) {
        self.hist[self.hist_pos] = class as u8;
        self.hist_pos = (self.hist_pos + 1) % HWIN;
        if self.hist_len < HWIN { self.hist_len += 1; }
    }

    /// === REPLACE: reload_weights ===
    pub fn reload_weights(&mut self, path_to_model: &Path) -> Result<(), PredictionError> {
        let model_bytes = std::fs::read(path_to_model)
            .map_err(|_| PredictionError::ModelNotFound(path_to_model.to_string_lossy().to_string()))?;
        let mut w = decode_and_verify_model(&model_bytes)?;
        fold_standardization_into_first_layer(&mut w);

        let (pd, ph, mut pw1, mut pb1, pw2, mut pw1t, w1_off, w1_col_off, w2_off) = build_packed_weights(&w);
        if pd != self.p_feature_count { self.scratch_x = vec![0.0; pd]; }
        if ph != self.p_hidden_size  { self.scratch_hidden = vec![0.0; ph]; }

        // Re-bake gains/biases
        let mut class_gain = [1.0f32; 5];
        if !w.class_gain.is_empty() {
            for r in 0..5 { class_gain[r] = w.class_gain[r]; }
        }
        let mut class_bias = [0.0f32; 5];
        if !w.class_bias.is_empty() {
            for r in 0..5 { class_bias[r] = w.class_bias[r]; }
        }
        let mut out_gain = [0.0f32; 5];
        let mut out_bias = [0.0f32; 5];
        for r in 0..5 {
            let g = w.logit_gain * class_gain[r];
            out_gain[r] = g;
            out_bias[r] = w.b2[r] * g + class_bias[r];
        }

        // Rebuild scaled W2
        let mut pw2_scaled = vec![0.0f32; pw2.len()];
        for r in 0..5 {
            let base = r * ph;
            let row = &pw2[base .. base + ph];
            let dst = &mut pw2_scaled[base .. base + ph];
            let g = out_gain[r];
            for j in 0..ph { dst[j] = row[j] * g; }
        }

        // Recompute sparse toggles
        let mut use_sparse = false;
        let mut w1_sparse_cols: Option<Vec<Vec<u32>>> = None;
        if !w.w1_sparse_col_idx.is_empty() && w.w1_sparse_col_idx.len() == w.hidden_size {
            let nnz: usize = w.w1_sparse_col_idx.iter().map(|v| v.len()).sum();
            let total = (w.hidden_size as f32) * (w.feature_count as f32);
            if (nnz as f32) <= SPARSE_USE_THRESHOLD * total {
                use_sparse = true;
                w1_sparse_cols = Some(w.w1_sparse_col_idx.clone());
            }
        }

        // === NEW: recompute bounds ===
        let d = w.feature_count;
        let h = w.hidden_size;

        let mut w1_col_l1 = vec![0.0f32; d];
        for i in 0..d {
            let mut s = 0.0f32;
            let mut j = 0usize;
            while j < h {
                s += w.w1[j * d + i].abs();
                j += 1;
            }
            w1_col_l1[i] = s;
        }

        let mut w2_row_linf = [0.0f32; 5];
        for r in 0..5 {
            let mut m = 0.0f32;
            let base = r * ph;
            let row = &pw2_scaled[base .. base + ph];
            let mut j = 0usize;
            while j < h {
                let a = row[j].abs();
                if a > m { m = a; }
                j += 1;
            }
            w2_row_linf[r] = m;
        }
        let mut w2_linf_max = 0.0f32;
        for r in 0..5 { if w2_row_linf[r] > w2_linf_max { w2_linf_max = w2_row_linf[r]; } }

        self.w = w;
        self.p_feature_count = pd;
        self.p_hidden_size   = ph;

        self.packed_w1 = pw1;
        self.packed_b1 = pb1;
        self.packed_w2 = pw2;
        self.packed_w2_scaled = pw2_scaled;

        self.packed_w1t = pw1t;
        self.w1_row_offsets = w1_off;
        self.w1_col_offsets = w1_col_off;
        self.w2_row_offsets = w2_off;

        self.out_bias = out_bias;
        self.inv_t = 1.0 / self.w.temperature;

        self.scratch_logits.fill(0.0);
        self.last_input_hash = None;
        self.last_result = None;

        // reset ring
        self.ring_pos = 0; self.ring_len = 0; self.ring_scores.clear();

        self.use_sparse = use_sparse;
        self.w1_sparse_cols = w1_sparse_cols;

        self.w1_col_l1 = w1_col_l1;
        self.w2_row_linf = w2_row_linf;
        self.w2_linf_max = w2_linf_max;

        // re-pack f16 buffers if supported/enabled
        if self.has_f16c && USE_F16_WEIGHTS {
            self.use_f16_weights = true;
            self.packed_w1_f16 = pack_f32_to_f16_vec(&self.packed_w1);
            self.packed_w1t_f16 = pack_f32_to_f16_vec(&self.packed_w1t);
            self.packed_w2_scaled_f16 = pack_f32_to_f16_vec(&self.packed_w2_scaled);
            lock_and_prefault_weights_f16(
                unsafe { core::slice::from_raw_parts_mut(self.packed_w1_f16.as_ptr() as *mut u16, self.packed_w1_f16.len()) },
                unsafe { core::slice::from_raw_parts_mut(self.packed_w1t_f16.as_ptr() as *mut u16, self.packed_w1t_f16.len()) },
                unsafe { core::slice::from_raw_parts_mut(self.packed_w2_scaled_f16.as_ptr() as *mut u16, self.packed_w2_scaled_f16.len()) },
            );
        } else {
            self.use_f16_weights = false;
            self.packed_w1_f16.clear();
            self.packed_w1t_f16.clear();
            self.packed_w2_scaled_f16.clear();
        }

        // lock & prefault newly loaded buffers
        lock_and_prefault_weights(&mut self.packed_w1, &mut self.packed_w1t, &mut self.packed_w2_scaled, &mut self.packed_b1);

        // keep CPU feature flags & adaptive state
        self.input_sparse_threshold = self.input_sparse_threshold.clamp(DYN_THRESH_MIN, DYN_THRESH_MAX);
        self.nz_ema = self.nz_ema.clamp(0.0, 1.0);

        // re-bind static kernels (same logic as in new)
        self.fwd_dense_f32 =
            if self.has_avx2_fma && USE_STREAM_FUSED && self.p_hidden_size <= STREAM_FUSED_WHEN_PH_LE {
                #[cfg(target_arch = "x86_64")]
                { |pd,ph,w1,b1,w1r,w2s,w2r,ob,ch,cl,x,h,z| unsafe { forward_fused_scaled_stream_avx2(pd,ph,w1,b1,w1r,w2s,w2r,ob,ch,cl,x,h,z) } }
                #[cfg(not(target_arch = "x86_64"))]
                { call_fused_scalar }
            } else if self.has_avx512f {
                #[cfg(target_arch = "x86_64")]
                { call_fused_avx512 }
                #[cfg(not(target_arch = "x86_64"))]
                { call_fused_scalar }
            } else if self.has_avx2_fma {
                #[cfg(target_arch = "x86_64")]
                { call_fused_avx2 }
                #[cfg(not(target_arch = "x86_64"))]
                { call_fused_scalar }
            } else { call_fused_scalar };

        self.fwd_sparse_f32 =
            if self.has_avx512f {
                #[cfg(target_arch = "x86_64")]
                { call_sparse_avx512 }
                #[cfg(not(target_arch = "x86_64"))]
                { call_sparse_scalar }
            } else if self.has_avx2_fma {
                #[cfg(target_arch = "x86_64")]
                { call_sparse_avx2 }
                #[cfg(not(target_arch = "x86_64"))]
                { call_sparse_scalar }
            } else { call_sparse_scalar };

        if self.has_f16c && USE_F16_WEIGHTS {
            self.use_f16_weights = true;
            self.fwd_dense_f16 = Some(if self.has_avx512f { #[cfg(target_arch="x86_64")] { call_fused_f16_avx512 } } else { #[cfg(target_arch="x86_64")] { call_fused_f16_avx2 } });
            self.fwd_sparse_f16 = Some(if self.has_avx512f { #[cfg(target_arch="x86_64")] { call_sparse_f16_avx512 } } else { #[cfg(target_arch="x86_64")] { call_sparse_f16_avx2 } });
        } else {
            self.use_f16_weights = false;
            self.fwd_dense_f16 = None;
            self.fwd_sparse_f16 = None;
        }

        Ok(())
    }

    /// Optional: tune runtime gates without reconstructing the predictor
    #[inline]
    pub fn set_gates(&mut self, min_confidence: f32, min_top2_ratio: f32) {
        self.min_confidence = min_confidence.max(0.0).min(1.0);
        self.min_top2_ratio = min_top2_ratio.max(1.0);
    }

    /// === ADD (verbatim): set per-class calibration ===
    /// Provide monotone (x,y) pairs in ascending x for each class. Typical 5–8 points.
    #[inline]
    pub fn set_calibration(&mut self, per_class: [Vec<(f32,f32)>; 5]) {
        // (Optional) Basic monotonicity enforcement (desk keeps it stable offline)
        // Here we trust caller and only clamp when applying.
        self.calib = Some(per_class);
    }

    /// === ADD (verbatim): runtime context tuning (slot noise / congestion) ===
    /// Adjust gates deterministically from runtime noise & congestion in [0, 1].
    /// Higher values tighten ratio gates and enable mild EMA.
    #[inline]
    pub fn set_runtime_context(&mut self, noise_level: f32, congestion: f32) {
        let n = noise_level.clamp(0.0, 1.0);
        let c = congestion.clamp(0.0, 1.0);
        // Base gates remain; scale ratio gently up to +7% at worst
        self.min_top2_ratio = 1.03 * (1.0 + 0.05 * n + 0.02 * c);
        // Confidence floor nudged by up to +3 points
        self.min_confidence = (0.50 + 0.03 * n + 0.01 * c).min(0.65);
        // EMA alpha up to 0.20 for choppy slots; still deterministic
        self.logit_ema_alpha = (0.20 * (0.6 * n + 0.4 * c)).clamp(0.0, 0.20);
    }

    /// === ADD (verbatim): optional batch API for higher throughput ===
    /// Process a small batch with single allocator state; preserves determinism.
    pub fn predict_batch<'a, I>(&mut self, batch: I, slot: u64) -> Result<Vec<WhaleIntentScore>, PredictionError>
    where
        I: IntoIterator<Item = &'a [f32]>,
    {
        let mut out = Vec::new();
        for feats in batch {
            // reuse the same scratch; sequential, no aliasing
            let s = self.predict(feats, slot)?;
            out.push(s);
        }
        Ok(out)
    }

    #[inline(always)]
    fn hist_vote_ok(&self, cls: usize) -> bool {
        let need = HYST_MIN[cls] as usize;
        if need <= 1 || self.hist_len == 0 { return true; }
        let mut cnt = 0usize;
        for i in 0..self.hist_len {
            let v = self.hist[i];
            if v != u8::MAX && v as usize == cls { cnt += 1; }
        }
        cnt >= (need - 1) // require prior support (current vote counts as +1)
    }

    #[inline(always)]
    fn hist_push(&mut self, cls: usize) {
        self.hist[self.hist_pos % HWIN] = cls as u8;
        self.hist_pos = (self.hist_pos + 1) % HWIN;
        if self.hist_len < HWIN { self.hist_len += 1; }
    }
}

/// === REPLACE: compute kernels ===
/// Packed forward: 8-lane FMAs + pairwise reduction; branchless ReLU; pre-baked gains/bias; clamps.
#[inline(always)]
fn forward_fused(
    pd: usize,
    ph: usize,
    packed_w1: &[f32],
    packed_b1: &[f32],
    w1_row_offsets: &[usize],
    packed_w2: &[f32],
    w2_row_offsets: &[usize],
    out_gain: &[f32; 5],
    out_bias: &[f32; 5],
    clip_hidden: f32,
    clip_logits: f32,
    x: &[f32],
    hidden_out: &mut [f32],
    logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    if hidden_out.len() != ph || logits_out.len() != 5 { return Err(PredictionError::NumericalError); }

    // hidden = relu(W1 * x + b1) on padded dims
    for j in 0..ph {
        let base = w1_row_offsets[j];
        let row = &packed_w1[base .. base + pd];

        // start from pre-packed b1[j] (0 for padded rows)
        let mut s0 = 0.0f32; let mut s1 = 0.0f32; let mut s2 = 0.0f32; let mut s3 = 0.0f32;
        let mut s4 = 0.0f32; let mut s5 = 0.0f32; let mut s6 = 0.0f32; let mut s7 = 0.0f32;
        let mut i = 0usize;
        while i < pd {
            s0 = s0.mul_add(row[i    ], x[i    ]);
            s1 = s1.mul_add(row[i + 1], x[i + 1]);
            s2 = s2.mul_add(row[i + 2], x[i + 2]);
            s3 = s3.mul_add(row[i + 3], x[i + 3]);
            s4 = s4.mul_add(row[i + 4], x[i + 4]);
            s5 = s5.mul_add(row[i + 5], x[i + 5]);
            s6 = s6.mul_add(row[i + 6], x[i + 6]);
            s7 = s7.mul_add(row[i + 7], x[i + 7]);
            i += 8;
        }
        let t0 = (s0 + s1) + (s2 + s3);
        let t1 = (s4 + s5) + (s6 + s7);
        let sum = (t0 + t1) + packed_b1[j];

        hidden_out[j] = sum.max(0.0).clamp(-clip_hidden, clip_hidden);
    }

    // logits = dot * out_gain[r] + out_bias[r]
    for r in 0..5 {
        let base = w2_row_offsets[r];
        let row = &packed_w2[base .. base + ph];

        let mut s0 = 0.0f32; let mut s1 = 0.0f32; let mut s2 = 0.0f32; let mut s3 = 0.0f32;
        let mut s4 = 0.0f32; let mut s5 = 0.0f32; let mut s6 = 0.0f32; let mut s7 = 0.0f32;
        let mut j = 0usize;
        while j < ph {
            s0 = s0.mul_add(row[j    ], hidden_out[j    ]);
            s1 = s1.mul_add(row[j + 1], hidden_out[j + 1]);
            s2 = s2.mul_add(row[j + 2], hidden_out[j + 2]);
            s3 = s3.mul_add(row[j + 3], hidden_out[j + 3]);
            s4 = s4.mul_add(row[j + 4], hidden_out[j + 4]);
            s5 = s5.mul_add(row[j + 5], hidden_out[j + 5]);
            s6 = s6.mul_add(row[j + 6], hidden_out[j + 6]);
            s7 = s7.mul_add(row[j + 7], hidden_out[j + 7]);
            j += 8;
        }
        let t0 = (s0 + s1) + (s2 + s3);
        let t1 = (s4 + s5) + (s6 + s7);
        let dot = t0 + t1;

        let v = dot.mul_add(out_gain[r], out_bias[r]);
        logits_out[r] = v.clamp(-clip_logits, clip_logits);
    }

    Ok(())
}

/// === REPLACE: forward_fused_scaled (dispatch AVX-512/AVX2/scalar; dual-accum on SIMD) ===
#[inline(always)]
fn forward_fused_scaled(
    pd: usize,
    ph: usize,
    packed_w1: &[f32],
    packed_b1: &[f32],
    w1_row_offsets: &[usize],
    packed_w2_scaled: &[f32],
    w2_row_offsets: &[usize],
    out_bias: &[f32; 5],
    clip_hidden: f32,
    clip_logits: f32,
    x: &[f32],
    hidden_out: &mut [f32],
    logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    #[cfg(target_arch = "x86_64")]
    {
        if USE_AVX512 && std::is_x86_feature_detected!("avx512f") {
            unsafe { return forward_fused_scaled_avx512(pd, ph, packed_w1, packed_b1, w1_row_offsets, packed_w2_scaled, w2_row_offsets, out_bias, clip_hidden, clip_logits, x, hidden_out, logits_out); }
        }
        if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
            unsafe { return forward_fused_scaled_avx2(pd, ph, packed_w1, packed_b1, w1_row_offsets, packed_w2_scaled, w2_row_offsets, out_bias, clip_hidden, clip_logits, x, hidden_out, logits_out); }
        }
    }
    forward_fused_scaled_scalar(pd, ph, packed_w1, packed_b1, w1_row_offsets, packed_w2_scaled, w2_row_offsets, out_bias, clip_hidden, clip_logits, x, hidden_out, logits_out)
}

#[inline(always)]
fn forward_fused_scaled_scalar(
    pd: usize, ph: usize,
    packed_w1: &[f32], packed_b1: &[f32], w1_row_offsets: &[usize],
    packed_w2_scaled: &[f32], w2_row_offsets: &[usize],
    out_bias: &[f32; 5], clip_hidden: f32, clip_logits: f32,
    x: &[f32], hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    if hidden_out.len() != ph || logits_out.len() != 5 { return Err(PredictionError::NumericalError); }

    // hidden
    for j in 0..ph {
        let base = w1_row_offsets[j];
        let row = &packed_w1[base .. base + pd];

        let mut s0 = 0.0f32; let mut s1 = 0.0f32; let mut s2 = 0.0f32; let mut s3 = 0.0f32;
        let mut s4 = 0.0f32; let mut s5 = 0.0f32; let mut s6 = 0.0f32; let mut s7 = 0.0f32;
        let mut i = 0usize;
        while i < pd {
            s0 = s0.mul_add(row[i    ], x[i    ]);
            s1 = s1.mul_add(row[i + 1], x[i + 1]);
            s2 = s2.mul_add(row[i + 2], x[i + 2]);
            s3 = s3.mul_add(row[i + 3], x[i + 3]);
            s4 = s4.mul_add(row[i + 4], x[i + 4]);
            s5 = s5.mul_add(row[i + 5], x[i + 5]);
            s6 = s6.mul_add(row[i + 6], x[i + 6]);
            s7 = s7.mul_add(row[i + 7], x[i + 7]);
            i += 8;
        }
        let t0 = (s0 + s1) + (s2 + s3);
        let t1 = (s4 + s5) + (s6 + s7);
        let sum = (t0 + t1) + packed_b1[j];
        hidden_out[j] = sum.max(0.0).clamp(-clip_hidden, clip_hidden);
    }

    // logits
    for r in 0..5 {
        let base = w2_row_offsets[r];
        let row = &packed_w2_scaled[base .. base + ph];

        let mut s0 = 0.0f32; let mut s1 = 0.0f32; let mut s2 = 0.0f32; let mut s3 = 0.0f32;
        let mut s4 = 0.0f32; let mut s5 = 0.0f32; let mut s6 = 0.0f32; let mut s7 = 0.0f32;
        let mut j = 0usize;
        while j < ph {
            s0 = s0.mul_add(row[j    ], hidden_out[j    ]);
            s1 = s1.mul_add(row[j + 1], hidden_out[j + 1]);
            s2 = s2.mul_add(row[j + 2], hidden_out[j + 2]);
            s3 = s3.mul_add(row[j + 3], hidden_out[j + 3]);
            s4 = s4.mul_add(row[j + 4], hidden_out[j + 4]);
            s5 = s5.mul_add(row[j + 5], hidden_out[j + 5]);
            s6 = s6.mul_add(row[j + 6], hidden_out[j + 6]);
            s7 = s7.mul_add(row[j + 7], hidden_out[j + 7]);
            j += 8;
        }
        let t0 = (s0 + s1) + (s2 + s3);
        let t1 = (s4 + s5) + (s6 + s7);
        let v = (t0 + t1) + out_bias[r];
        logits_out[r] = v.clamp(-clip_logits, clip_logits);
    }
    Ok(())
}

/// === ADD (verbatim): AVX-512 kernel for fused path ===
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn forward_fused_scaled_avx512(
    pd: usize, ph: usize,
    packed_w1: &[f32], packed_b1: &[f32], w1_row_offsets: &[usize],
    packed_w2_scaled: &[f32], w2_row_offsets: &[usize],
    out_bias: &[f32; 5], clip_hidden: f32, clip_logits: f32,
    x: &[f32], hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    use core::arch::x86_64::*;
    #[inline(always)]
    unsafe fn hsum512_ps(v: __m512) -> f32 {
        let lo: __m256 = _mm512_castps512_ps256(v);
        let hi: __m256 = _mm256_castps128_ps256(_mm512_extractf32x8_ps(v, 1));
        // reuse 256 hsum via hadd chain
        let x = _mm256_hadd_ps(lo, lo);
        let y = _mm256_hadd_ps(x, x);
        let lo_s = _mm_cvtss_f32(_mm_add_ss(_mm256_castps256_ps128(y), _mm256_extractf128_ps(y, 1)));
        let x2 = _mm256_hadd_ps(hi, hi);
        let y2 = _mm256_hadd_ps(x2, x2);
        let hi_s = _mm_cvtss_f32(_mm_add_ss(_mm256_castps256_ps128(y2), _mm256_extractf128_ps(y2, 1)));
        lo_s + hi_s
    }

    // hidden = relu(W1 * x + b1)
    let clipv = _mm512_set1_ps(clip_hidden);
    let nclipv = _mm512_set1_ps(-clip_hidden);
    let zerov = _mm512_set1_ps(0.0);

    for j in 0..ph {
        let base = w1_row_offsets[j];
        let row_ptr = packed_w1.as_ptr().add(base);
        let x_ptr   = x.as_ptr();

        let mut acc0 = _mm512_setzero_ps();
        let mut acc1 = _mm512_setzero_ps();

        let mut i = 0usize;
        while i + 32 <= pd {
            let rv0 = _mm512_loadu_ps(row_ptr.add(i));
            let xv0 = _mm512_loadu_ps(x_ptr.add(i));
            acc0 = _mm512_fmadd_ps(rv0, xv0, acc0);

            let rv1 = _mm512_loadu_ps(row_ptr.add(i + 16));
            let xv1 = _mm512_loadu_ps(x_ptr.add(i + 16));
            acc1 = _mm512_fmadd_ps(rv1, xv1, acc1);

            if USE_PREFETCH {
                _mm_prefetch(row_ptr.add(i + 64) as *const i8, _MM_HINT_T0);
                _mm_prefetch(x_ptr.add(i + 64) as *const i8, _MM_HINT_T0);
            }
            i += 32;
        }
        while i + 16 <= pd {
            let rv = _mm512_loadu_ps(row_ptr.add(i));
            let xv = _mm512_loadu_ps(x_ptr.add(i));
            acc0 = _mm512_fmadd_ps(rv, xv, acc0);
            i += 16;
        }
        if i < pd {
            // tail <16 handled scalar for simplicity (padded zeros keep it cheap)
            let mut sum = hsum512_ps(acc0) + hsum512_ps(acc1) + packed_b1[j];
            while i < pd { sum += *row_ptr.add(i) * *x_ptr.add(i); i += 1; }
            // ReLU + clamp
            if sum < 0.0 { sum = 0.0; }
            if sum >  clip_hidden { sum =  clip_hidden; }
            if sum < -clip_hidden { sum = -clip_hidden; }
            hidden_out[j] = sum;
        } else {
            let mut sum = hsum512_ps(acc0) + hsum512_ps(acc1) + packed_b1[j];
            if sum < 0.0 { sum = 0.0; }
            if sum >  clip_hidden { sum =  clip_hidden; }
            if sum < -clip_hidden { sum = -clip_hidden; }
            hidden_out[j] = sum;
        }
    }

    // logits = dot(scaled_W2[r], hidden) + out_bias[r]
    for r in 0..5 {
        let base = w2_row_offsets[r];
        let row_ptr = packed_w2_scaled.as_ptr().add(base);
        let h_ptr   = hidden_out.as_ptr();

        let mut acc0 = _mm512_setzero_ps();
        let mut acc1 = _mm512_setzero_ps();

        let mut j = 0usize;
        while j + 32 <= ph {
            let rv0 = _mm512_loadu_ps(row_ptr.add(j));
            let hv0 = _mm512_loadu_ps(h_ptr.add(j));
            acc0 = _mm512_fmadd_ps(rv0, hv0, acc0);

            let rv1 = _mm512_loadu_ps(row_ptr.add(j + 16));
            let hv1 = _mm512_loadu_ps(h_ptr.add(j + 16));
            acc1 = _mm512_fmadd_ps(rv1, hv1, acc1);

            if USE_PREFETCH {
                _mm_prefetch(row_ptr.add(j + 64) as *const i8, _MM_HINT_T0);
                _mm_prefetch(h_ptr.add(j + 64) as *const i8, _MM_HINT_T0);
            }
            j += 32;
        }
        while j + 16 <= ph {
            let rv = _mm512_loadu_ps(row_ptr.add(j));
            let hv = _mm512_loadu_ps(h_ptr.add(j));
            acc0 = _mm512_fmadd_ps(rv, hv, acc0);
            j += 16;
        }
        let mut v = hsum512_ps(acc0) + hsum512_ps(acc1) + out_bias[r];
        while j < ph { v += *row_ptr.add(j) * *h_ptr.add(j); j += 1; }
        if v >  clip_logits { v =  clip_logits; }
        if v < -clip_logits { v = -clip_logits; }
        logits_out[r] = v;
    }
    _mm256_zeroupper();
    Ok(())
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn forward_fused_scaled_avx2(
    pd: usize, ph: usize,
    packed_w1: &[f32], packed_b1: &[f32], w1_row_offsets: &[usize],
    packed_w2_scaled: &[f32], w2_row_offsets: &[usize],
    out_bias: &[f32; 5], clip_hidden: f32, clip_logits: f32,
    x: &[f32], hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    use core::arch::x86_64::*;
    #[inline(always)]
    unsafe fn hsum256_ps(v: __m256) -> f32 {
        let x = _mm256_hadd_ps(v, v);
        let y = _mm256_hadd_ps(x, x);
        let hi = _mm256_extractf128_ps(y, 1);
        let lo = _mm256_castps256_ps128(y);
        _mm_cvtss_f32(_mm_add_ss(lo, hi))
    }

    // hidden
    for j in 0..ph {
        let base = w1_row_offsets[j];
        let row_ptr = packed_w1.as_ptr().add(base);
        let x_ptr   = x.as_ptr();

        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();

        let mut i = 0usize;
        while i + 16 <= pd {
            let rv0 = _mm256_loadu_ps(row_ptr.add(i));
            let xv0 = _mm256_loadu_ps(x_ptr.add(i));
            acc0 = _mm256_fmadd_ps(rv0, xv0, acc0);

            let rv1 = _mm256_loadu_ps(row_ptr.add(i + 8));
            let xv1 = _mm256_loadu_ps(x_ptr.add(i + 8));
            acc1 = _mm256_fmadd_ps(rv1, xv1, acc1);

            if USE_PREFETCH && (i & 63) == 0 {
                _mm_prefetch(row_ptr.add(i + 64) as *const i8, _MM_HINT_T0);
                _mm_prefetch(x_ptr.add(i + 64) as *const i8, _MM_HINT_T0);
            }
            i += 16;
        }
        while i + 8 <= pd {
            let rv = _mm256_loadu_ps(row_ptr.add(i));
            let xv = _mm256_loadu_ps(x_ptr.add(i));
            acc0 = _mm256_fmadd_ps(rv, xv, acc0);
            i += 8;
        }
        let mut sum = hsum256_ps(acc0) + hsum256_ps(acc1) + packed_b1[j];
        while i < pd { sum += *row_ptr.add(i) * *x_ptr.add(i); i += 1; }

        if sum < 0.0 { sum = 0.0; }
        if sum >  clip_hidden { sum =  clip_hidden; }
        if sum < -clip_hidden { sum = -clip_hidden; }
        hidden_out[j] = sum;
    }

    // logits
    for r in 0..5 {
        let base = w2_row_offsets[r];
        let row_ptr = packed_w2_scaled.as_ptr().add(base);
        let h_ptr   = hidden_out.as_ptr();

        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();

        let mut j = 0usize;
        while j + 16 <= ph {
            let rv0 = _mm256_loadu_ps(row_ptr.add(j));
            let hv0 = _mm256_loadu_ps(h_ptr.add(j));
            acc0 = _mm256_fmadd_ps(rv0, hv0, acc0);

            let rv1 = _mm256_loadu_ps(row_ptr.add(j + 8));
            let hv1 = _mm256_loadu_ps(h_ptr.add(j + 8));
            acc1 = _mm256_fmadd_ps(rv1, hv1, acc1);

            if USE_PREFETCH && (j & 63) == 0 {
                _mm_prefetch(row_ptr.add(j + 64) as *const i8, _MM_HINT_T0);
                _mm_prefetch(h_ptr.add(j + 64) as *const i8, _MM_HINT_T0);
            }
            j += 16;
        }
        while j + 8 <= ph {
            let rv = _mm256_loadu_ps(row_ptr.add(j));
            let hv = _mm256_loadu_ps(h_ptr.add(j));
            acc0 = _mm256_fmadd_ps(rv, hv, acc0);
            j += 8;
        }
        let mut v = hsum256_ps(acc0) + hsum256_ps(acc1) + out_bias[r];
        while j < ph { v += *row_ptr.add(j) * *h_ptr.add(j); j += 1; }
        if v >  clip_logits { v =  clip_logits; }
        if v < -clip_logits { v = -clip_logits; }
        logits_out[r] = v;
    }
    Ok(())
}

/// === ADD (verbatim): AVX2 streaming fused kernel (f32 weights) ===
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn forward_fused_scaled_stream_avx2(
    pd: usize, ph: usize,
    packed_w1: &[f32], packed_b1: &[f32], w1_row_offsets: &[usize],
    packed_w2_scaled: &[f32], w2_row_offsets: &[usize],
    out_bias: &[f32; 5], clip_hidden: f32, clip_logits: f32,
    x: &[f32], hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    use core::arch::x86_64::*;
    #[inline(always)] unsafe fn hsum256_ps(v: __m256) -> f32 {
        let x = _mm256_hadd_ps(v, v); let y = _mm256_hadd_ps(x, x);
        _mm_cvtss_f32(_mm_add_ss(_mm256_castps256_ps128(y), _mm256_extractf128_ps(y, 1)))
    }

    // init accumulators with bias
    let mut acc = [0.0f32; 5];
    for r in 0..5 { acc[r] = out_bias[r]; }

    // hidden rows, then immediately add contribution to all 5 logits
    for j in 0..ph {
        let base = w1_row_offsets[j];
        let row_ptr = packed_w1.as_ptr().add(base);
        let x_ptr   = x.as_ptr();

        let mut a0 = _mm256_setzero_ps();
        let mut a1 = _mm256_setzero_ps();

        let mut i = 0usize;
        while i + 16 <= pd {
            let rv0 = _mm256_loadu_ps(row_ptr.add(i));
            let xv0 = _mm256_loadu_ps(x_ptr.add(i));
            a0 = _mm256_fmadd_ps(rv0, xv0, a0);

            let rv1 = _mm256_loadu_ps(row_ptr.add(i + 8));
            let xv1 = _mm256_loadu_ps(x_ptr.add(i + 8));
            a1 = _mm256_fmadd_ps(rv1, xv1, a1);

            i += 16;
        }
        while i + 8 <= pd {
            let rv = _mm256_loadu_ps(row_ptr.add(i));
            let xv = _mm256_loadu_ps(x_ptr.add(i));
            a0 = _mm256_fmadd_ps(rv, xv, a0);
            i += 8;
        }
        let mut h = hsum256_ps(a0) + hsum256_ps(a1) + *packed_b1.get_unchecked(j);
        while i < pd { h += *row_ptr.add(i) * *x_ptr.add(i); i += 1; }

        if h < 0.0 { h = 0.0; }
        if h >  clip_hidden { h =  clip_hidden; }
        if h < -clip_hidden { h = -clip_hidden; }
        *hidden_out.get_unchecked_mut(j) = h;

        // accumulate logits (scalar FMAs; 5 weights per j)
        acc[0] = acc[0].mul_add(*packed_w2_scaled.get_unchecked(w2_row_offsets[0] + j), h);
        acc[1] = acc[1].mul_add(*packed_w2_scaled.get_unchecked(w2_row_offsets[1] + j), h);
        acc[2] = acc[2].mul_add(*packed_w2_scaled.get_unchecked(w2_row_offsets[2] + j), h);
        acc[3] = acc[3].mul_add(*packed_w2_scaled.get_unchecked(w2_row_offsets[3] + j), h);
        acc[4] = acc[4].mul_add(*packed_w2_scaled.get_unchecked(w2_row_offsets[4] + j), h);
    }

    for r in 0..5 {
        let mut v = acc[r];
        if v >  clip_logits { v =  clip_logits; }
        if v < -clip_logits { v = -clip_logits; }
        logits_out[r] = v;
    }
    _mm256_zeroupper();
    Ok(())
}

/// === ADD (verbatim): forward_sparse_scaled ===
/// Row-wise sparse W1 (index lists), dense scaled W2; no allocations.
#[inline(always)]
fn forward_sparse_scaled(
    d: usize,
    ph: usize,
    w1_dense: &[f32],               // row-major h x d
    b1: &[f32],                      // len h
    w1_sparse_cols: &[Vec<u32>],     // len h, each holds column indices
    packed_w2_scaled: &[f32],        // 5 x ph (ph >= h, padded)
    w2_row_offsets: &[usize],
    out_bias: &[f32; 5],
    clip_hidden: f32,
    clip_logits: f32,
    x: &[f32],
    hidden_out: &mut [f32],
    logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    let h = w1_sparse_cols.len();
    if h == 0 { return Err(PredictionError::InvalidModelFormat("sparse cols empty".into())); }

    // Sparse hidden
    for j in 0..h {
        let base = j * d;
        let mut acc = b1[j];
        for &c in &w1_sparse_cols[j] {
            let ci = c as usize;
            acc = acc.mul_add(w1_dense[base + ci], x[ci]);
        }
        let v = acc.max(0.0).clamp(-clip_hidden, clip_hidden);
        hidden_out[j] = v;
    }
    // Zero padded tail (if ph > h)
    for j in h..ph { hidden_out[j] = 0.0; }

    // Dense logits on scaled W2
    for r in 0..5 {
        let base = w2_row_offsets[r];
        let row  = &packed_w2_scaled[base .. base + ph];
        let mut s0 = 0.0f32; let mut s1 = 0.0f32; let mut s2 = 0.0f32; let mut s3 = 0.0f32;
        let mut s4 = 0.0f32; let mut s5 = 0.0f32; let mut s6 = 0.0f32; let mut s7 = 0.0f32;
        let mut j = 0usize;
        while j < ph {
            s0 = s0.mul_add(row[j    ], hidden_out[j    ]);
            s1 = s1.mul_add(row[j + 1], hidden_out[j + 1]);
            s2 = s2.mul_add(row[j + 2], hidden_out[j + 2]);
            s3 = s3.mul_add(row[j + 3], hidden_out[j + 3]);
            s4 = s4.mul_add(row[j + 4], hidden_out[j + 4]);
            s5 = s5.mul_add(row[j + 5], hidden_out[j + 5]);
            s6 = s6.mul_add(row[j + 6], hidden_out[j + 6]);
            s7 = s7.mul_add(row[j + 7], hidden_out[j + 7]);
            j += 8;
        }
        let t0 = (s0 + s1) + (s2 + s3);
        let t1 = (s4 + s5) + (s6 + s7);
        let v = (t0 + t1) + out_bias[r];
        logits_out[r] = v.clamp(-clip_logits, clip_logits);
    }
    Ok(())
}

/// === REPLACE: forward_input_sparse_scaled (dispatch AVX-512/AVX2/scalar) ===
#[inline(always)]
fn forward_input_sparse_scaled(
    ph: usize,
    packed_w1t: &[f32],          // (pd x ph)
    packed_b1: &[f32],           // (ph)
    w1_col_offsets: &[usize],    // i -> i*ph
    packed_w2_scaled: &[f32],    // (5 x ph)
    w2_row_offsets: &[usize],
    out_bias: &[f32; 5],
    clip_hidden: f32,
    clip_logits: f32,
    x: &[f32],                   // original (not padded)
    nz_idx: &[usize],            // indices where |x[i]| > eps  (i < d)
    hidden_out: &mut [f32],
    logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    #[cfg(target_arch = "x86_64")]
    {
        if USE_AVX512 && std::is_x86_feature_detected!("avx512f") {
            unsafe { return forward_input_sparse_scaled_avx512(ph, packed_w1t, packed_b1, w1_col_offsets, packed_w2_scaled, w2_row_offsets, out_bias, clip_hidden, clip_logits, x, nz_idx, hidden_out, logits_out); }
        }
        if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") {
            unsafe { return forward_input_sparse_scaled_avx2(ph, packed_w1t, packed_b1, w1_col_offsets, packed_w2_scaled, w2_row_offsets, out_bias, clip_hidden, clip_logits, x, nz_idx, hidden_out, logits_out); }
        }
    }
    forward_input_sparse_scaled_scalar(ph, packed_w1t, packed_b1, w1_col_offsets, packed_w2_scaled, w2_row_offsets, out_bias, clip_hidden, clip_logits, x, nz_idx, hidden_out, logits_out)
}

#[inline(always)]
fn forward_input_sparse_scaled_scalar(
    ph: usize,
    packed_w1t: &[f32], packed_b1: &[f32], w1_col_offsets: &[usize],
    packed_w2_scaled: &[f32], w2_row_offsets: &[usize],
    out_bias: &[f32; 5], clip_hidden: f32, clip_logits: f32,
    x: &[f32], nz_idx: &[usize],
    hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    // Seed hidden with b1
    hidden_out[..ph].copy_from_slice(&packed_b1[..ph]);

    // Accumulate columns for non-zero features
    for &i in nz_idx {
        let base = w1_col_offsets[i];
        let col  = &packed_w1t[base .. base + ph];
        let xi = x[i];
        let mut j = 0usize;
        while j < ph {
            hidden_out[j    ] = hidden_out[j    ].mul_add(xi, col[j    ]);
            hidden_out[j + 1] = hidden_out[j + 1].mul_add(xi, col[j + 1]);
            hidden_out[j + 2] = hidden_out[j + 2].mul_add(xi, col[j + 2]);
            hidden_out[j + 3] = hidden_out[j + 3].mul_add(xi, col[j + 3]);
            hidden_out[j + 4] = hidden_out[j + 4].mul_add(xi, col[j + 4]);
            hidden_out[j + 5] = hidden_out[j + 5].mul_add(xi, col[j + 5]);
            hidden_out[j + 6] = hidden_out[j + 6].mul_add(xi, col[j + 6]);
            hidden_out[j + 7] = hidden_out[j + 7].mul_add(xi, col[j + 7]);
            j += 8;
        }
    }

    // Apply ReLU + clamp
    for j in 0..ph {
        hidden_out[j] = hidden_out[j].max(0.0).clamp(-clip_hidden, clip_hidden);
    }

    // Dense logits on scaled W2
    for r in 0..5 {
        let base = w2_row_offsets[r];
        let row  = &packed_w2_scaled[base .. base + ph];

        let mut s0 = 0.0f32; let mut s1 = 0.0f32; let mut s2 = 0.0f32; let mut s3 = 0.0f32;
        let mut s4 = 0.0f32; let mut s5 = 0.0f32; let mut s6 = 0.0f32; let mut s7 = 0.0f32;
        let mut j = 0usize;
        while j < ph {
            s0 = s0.mul_add(row[j    ], hidden_out[j    ]);
            s1 = s1.mul_add(row[j + 1], hidden_out[j + 1]);
            s2 = s2.mul_add(row[j + 2], hidden_out[j + 2]);
            s3 = s3.mul_add(row[j + 3], hidden_out[j + 3]);
            s4 = s4.mul_add(row[j + 4], hidden_out[j + 4]);
            s5 = s5.mul_add(row[j + 5], hidden_out[j + 5]);
            s6 = s6.mul_add(row[j + 6], hidden_out[j + 6]);
            s7 = s7.mul_add(row[j + 7], hidden_out[j + 7]);
            j += 8;
        }
        let t0 = (s0 + s1) + (s2 + s3);
        let t1 = (s4 + s5) + (s6 + s7);
        let v = (t0 + t1) + out_bias[r];
        logits_out[r] = v.clamp(-clip_logits, clip_logits);
    }
    Ok(())
}

/// === ADD (verbatim): AVX-512 kernel for input-sparse path ===
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn forward_input_sparse_scaled_avx512(
    ph: usize,
    packed_w1t: &[f32], packed_b1: &[f32], w1_col_offsets: &[usize],
    packed_w2_scaled: &[f32], w2_row_offsets: &[usize],
    out_bias: &[f32; 5], clip_hidden: f32, clip_logits: f32,
    x: &[f32], nz_idx: &[usize],
    hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    use core::arch::x86_64::*;
    #[inline(always)]
    unsafe fn hsum512_ps(v: __m512) -> f32 {
        let lo: __m256 = _mm512_castps512_ps256(v);
        let hi: __m256 = _mm256_castps128_ps256(_mm512_extractf32x8_ps(v, 1));
        let x = _mm256_hadd_ps(lo, lo);
        let y = _mm256_hadd_ps(x, x);
        let lo_s = _mm_cvtss_f32(_mm_add_ss(_mm256_castps256_ps128(y), _mm256_extractf128_ps(y, 1)));
        let x2 = _mm256_hadd_ps(hi, hi);
        let y2 = _mm256_hadd_ps(x2, x2);
        let hi_s = _mm_cvtss_f32(_mm_add_ss(_mm256_castps256_ps128(y2), _mm256_extractf128_ps(y2, 1)));
        lo_s + hi_s
    }

    // hidden <- b1
    let mut j = 0usize;
    while j + 16 <= ph {
        _mm512_storeu_ps(hidden_out.as_mut_ptr().add(j), _mm512_loadu_ps(packed_b1.as_ptr().add(j)));
        j += 16;
    }
    while j < ph {
        hidden_out[j] = packed_b1[j]; j += 1;
    }

    // accumulate xi * col_i
    for &i in nz_idx {
        let base = w1_col_offsets[i];
        let col_ptr = packed_w1t.as_ptr().add(base);
        let xib = _mm512_set1_ps(x[i]);

        let mut k = 0usize;
        while k + 32 <= ph {
            let dst0 = _mm512_loadu_ps(hidden_out.as_ptr().add(k));
            let col0 = _mm512_loadu_ps(col_ptr.add(k));
            let sum0 = _mm512_fmadd_ps(col0, xib, dst0);
            _mm512_storeu_ps(hidden_out.as_mut_ptr().add(k), sum0);

            let dst1 = _mm512_loadu_ps(hidden_out.as_ptr().add(k + 16));
            let col1 = _mm512_loadu_ps(col_ptr.add(k + 16));
            let sum1 = _mm512_fmadd_ps(col1, xib, dst1);
            _mm512_storeu_ps(hidden_out.as_mut_ptr().add(k + 16), sum1);

            if USE_PREFETCH {
                _mm_prefetch(col_ptr.add(k + 64) as *const i8, _MM_HINT_T0);
                _mm_prefetch(hidden_out.as_ptr().add(k + 64) as *const i8, _MM_HINT_T0);
            }
            k += 32;
        }
        while k + 16 <= ph {
            let dst = _mm512_loadu_ps(hidden_out.as_ptr().add(k));
            let col = _mm512_loadu_ps(col_ptr.add(k));
            let sum = _mm512_fmadd_ps(col, xib, dst);
            _mm512_storeu_ps(hidden_out.as_mut_ptr().add(k), sum);
            k += 16;
        }
        while k < ph {
            let v = *col_ptr.add(k) * x[i] + *hidden_out.as_ptr().add(k);
            *hidden_out.as_mut_ptr().add(k) = v;
            k += 1;
        }
    }

    // ReLU + clamp
    let zero = _mm512_set1_ps(0.0);
    let clipv = _mm512_set1_ps(clip_hidden);
    let nclipv = _mm512_set1_ps(-clip_hidden);
    let mut k = 0usize;
    while k + 16 <= ph {
        let v = _mm512_loadu_ps(hidden_out.as_ptr().add(k));
        let v = _mm512_max_ps(v, zero);
        let v = _mm512_min_ps(_mm512_max_ps(v, nclipv), clipv);
        _mm512_storeu_ps(hidden_out.as_mut_ptr().add(k), v);
        k += 16;
    }
    while k < ph {
        let mut v = hidden_out[k];
        if v < 0.0 { v = 0.0; }
        if v >  clip_hidden { v =  clip_hidden; }
        if v < -clip_hidden { v = -clip_hidden; }
        hidden_out[k] = v; k += 1;
    }

    // logits
    for r in 0..5 {
        let base = w2_row_offsets[r];
        let row_ptr = packed_w2_scaled.as_ptr().add(base);
        let h_ptr   = hidden_out.as_ptr();

        let mut acc0 = _mm512_setzero_ps();
        let mut acc1 = _mm512_setzero_ps();

        let mut j = 0usize;
        while j + 32 <= ph {
            let rv0 = _mm512_loadu_ps(row_ptr.add(j));
            let hv0 = _mm512_loadu_ps(h_ptr.add(j));
            acc0 = _mm512_fmadd_ps(rv0, hv0, acc0);

            let rv1 = _mm512_loadu_ps(row_ptr.add(j + 16));
            let hv1 = _mm512_loadu_ps(h_ptr.add(j + 16));
            acc1 = _mm512_fmadd_ps(rv1, hv1, acc1);

            if USE_PREFETCH {
                _mm_prefetch(row_ptr.add(j + 64) as *const i8, _MM_HINT_T0);
                _mm_prefetch(h_ptr.add(j + 64) as *const i8, _MM_HINT_T0);
            }
            j += 32;
        }
        while j + 16 <= ph {
            let rv = _mm512_loadu_ps(row_ptr.add(j));
            let hv = _mm512_loadu_ps(h_ptr.add(j));
            acc0 = _mm512_fmadd_ps(rv, hv, acc0);
            j += 16;
        }
        let mut v = hsum512_ps(acc0) + hsum512_ps(acc1) + out_bias[r];
        while j < ph { v += *row_ptr.add(j) * *h_ptr.add(j); j += 1; }
        if v >  clip_logits { v =  clip_logits; }
        if v < -clip_logits { v = -clip_logits; }
        logits_out[r] = v;
    }
    _mm256_zeroupper();
    Ok(())
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
unsafe fn forward_input_sparse_scaled_avx2(
    ph: usize,
    packed_w1t: &[f32], packed_b1: &[f32], w1_col_offsets: &[usize],
    packed_w2_scaled: &[f32], w2_row_offsets: &[usize],
    out_bias: &[f32; 5], clip_hidden: f32, clip_logits: f32,
    x: &[f32], nz_idx: &[usize],
    hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    use core::arch::x86_64::*;
    // Seed hidden with b1 (vectorized copy)
    let mut j = 0usize;
    while j < ph {
        let v = _mm256_loadu_ps(packed_b1.as_ptr().add(j));
        _mm256_storeu_ps(hidden_out.as_mut_ptr().add(j), v);
        j += 8;
    }

    // Add xi * col_i for each nz feature
    for &i in nz_idx {
        let base = w1_col_offsets[i];
        let col_ptr = packed_w1t.as_ptr().add(base);
        let xib = _mm256_set1_ps(x[i]);
        let mut k = 0usize;
        while k < ph {
            let dst = _mm256_loadu_ps(hidden_out.as_ptr().add(k));
            let col = _mm256_loadu_ps(col_ptr.add(k));
            let sum = _mm256_fmadd_ps(col, xib, dst);
            _mm256_storeu_ps(hidden_out.as_mut_ptr().add(k), sum);
            if USE_PREFETCH && (k & 63) == 0 {
                _mm_prefetch(col_ptr.add(k + 64) as *const i8, _MM_HINT_T0);
                _mm_prefetch(hidden_out.as_ptr().add(k + 64) as *const i8, _MM_HINT_T0);
            }
            k += 8;
        }
    }

    // ReLU + clamp
    let zero = _mm256_set1_ps(0.0);
    let clipv = _mm256_set1_ps(clip_hidden);
    let nclipv = _mm256_set1_ps(-clip_hidden);
    let mut k = 0usize;
    while k < ph {
        let v = _mm256_loadu_ps(hidden_out.as_ptr().add(k));
        let v = _mm256_max_ps(v, zero);
        let v = _mm256_min_ps(_mm256_max_ps(v, nclipv), clipv);
        _mm256_storeu_ps(hidden_out.as_mut_ptr().add(k), v);
        k += 8;
    }

    // logits = dot(scaled_W2[r], hidden) + bias
    #[inline(always)]
    unsafe fn hsum256_ps(v: __m256) -> f32 {
        let x = _mm256_hadd_ps(v, v);
        let y = _mm256_hadd_ps(x, x);
        let hi = _mm256_extractf128_ps(y, 1);
        let lo = _mm256_castps256_ps128(y);
        _mm_cvtss_f32(_mm_add_ss(lo, hi))
    }

    for r in 0..5 {
        let base = w2_row_offsets[r];
        let row_ptr = packed_w2_scaled.as_ptr().add(base);
        let mut acc = _mm256_setzero_ps();
        let mut j = 0usize;
        while j < ph {
            let rv = _mm256_loadu_ps(row_ptr.add(j));
            let hv = _mm256_loadu_ps(hidden_out.as_ptr().add(j));
            acc = _mm256_fmadd_ps(rv, hv, acc);
            if USE_PREFETCH && (j & 63) == 0 {
                _mm_prefetch(row_ptr.add(j + 64) as *const i8, _MM_HINT_T0);
                _mm_prefetch(hidden_out.as_ptr().add(j + 64) as *const i8, _MM_HINT_T0);
            }
            j += 8;
        }
        let mut v = hsum256_ps(acc) + out_bias[r];
        if v >  clip_logits { v =  clip_logits; }
        if v < -clip_logits { v = -clip_logits; }
        logits_out[r] = v;
    }
    Ok(())
}

/// === REPLACE: AVX2 stream-fused using SoA-5 (f32) ===
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
pub unsafe fn forward_fused_scaled_stream_avx2_soa5(
    pd: usize, ph: usize,
    packed_w1: &[f32], packed_b1: &[f32], w1_row_offsets: &[usize],
    w2s_soa5: &[f32], _w2_row_offsets_unused: &[usize],
    out_bias: &[f32; 5], clip_hidden: f32, clip_logits: f32,
    x: &[f32], hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    use core::arch::x86_64::*;
    #[inline(always)] unsafe fn hsum256_ps(v: __m256) -> f32 {
        let x = _mm256_hadd_ps(v, v); let y = _mm256_hadd_ps(x, x);
        _mm_cvtss_f32(_mm_add_ss(_mm256_castps256_ps128(y), _mm256_extractf128_ps(y, 1)))
    }

    let mut acc = [0.0f32; 5];
    acc.copy_from_slice(out_bias);

    let pf = prefetch_lookahead(pd);

    for j in 0..ph {
        let base = w1_row_offsets[j];
        let row_ptr = packed_w1.as_ptr().add(base);
        let x_ptr   = x.as_ptr();
        let mut a0 = _mm256_setzero_ps();

        let mut i = 0usize;
        while i + 8 <= pd {
            let rv = _mm256_loadu_ps(row_ptr.add(i));
            let xv = _mm256_loadu_ps(x_ptr.add(i));
            a0 = _mm256_fmadd_ps(rv, xv, a0);
            if USE_PREFETCH { prefetch_row(row_ptr, i + pf); prefetch_row(x_ptr, i + pf); }
            i += 8;
        }
        let mut h = hsum256_ps(a0) + *packed_b1.get_unchecked(j);
        while i < pd { h += *row_ptr.add(i) * *x_ptr.add(i); i += 1; }

        let h = clamp_relu(h, clip_hidden);
        *hidden_out.get_unchecked_mut(j) = h;

        let wptr = w2s_soa5.as_ptr().add(5 * j);
        acc[0] = acc[0].mul_add(*wptr.add(0), h);
        acc[1] = acc[1].mul_add(*wptr.add(1), h);
        acc[2] = acc[2].mul_add(*wptr.add(2), h);
        acc[3] = acc[3].mul_add(*wptr.add(3), h);
        acc[4] = acc[4].mul_add(*wptr.add(4), h);
    }

    for r in 0..5 {
        let mut v = acc[r];
        if v >  clip_logits { v =  clip_logits; }
        if v < -clip_logits { v = -clip_logits; }
        logits_out[r] = v;
    }
    _mm256_zeroupper();
    Ok(())
}

/// === REPLACE: AVX2 stream-fused using SoA-5 (bf16) ===
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma")]
pub unsafe fn forward_fused_scaled_stream_avx2_soa5_bf16(
    pd: usize, ph: usize,
    w1_bf16: &[u16], b1: &[f32], w1_row_offsets: &[usize],
    w2_soa5_bf16: &[u16], _w2_row_offsets_unused: &[usize],
    out_bias: &[f32;5], clip_hidden: f32, clip_logits: f32,
    x: &[f32], hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    use core::arch::x86_64::*;
    #[inline(always)] unsafe fn widen_bf16_8(ptr: *const u16) -> __m256 {
        let h = _mm_loadu_si128(ptr as *const _);
        let w32 = _mm256_cvtepu16_epi32(h);
        let w32s = _mm256_slli_epi32::<16>(w32);
        core::mem::transmute::<__m256i, __m256>(w32s)
    }
    #[inline(always)] unsafe fn hsum256_ps(v: __m256) -> f32 {
        let x = _mm256_hadd_ps(v, v); let y = _mm256_hadd_ps(x, x);
        _mm_cvtss_f32(_mm_add_ss(_mm256_castps256_ps128(y), _mm256_extractf128_ps(y, 1)))
    }

    let mut acc = [0.0f32; 5];
    acc.copy_from_slice(out_bias);

    let pf = prefetch_lookahead(pd);

    for j in 0..ph {
        let base = w1_row_offsets[j];
        let mut a0 = _mm256_setzero_ps();
        let mut i = 0usize;
        while i + 8 <= pd {
            let wv = widen_bf16_8(w1_bf16.as_ptr().add(base + i));
            let xv = _mm256_loadu_ps(x.as_ptr().add(i));
            a0 = _mm256_fmadd_ps(wv, xv, a0);
            if USE_PREFETCH { prefetch_row(x.as_ptr(), i + pf); }
            i += 8;
        }
        let mut h = hsum256_ps(a0) + b1[j];
        while i < pd {
            let w = (w1_bf16[base + i] as u32) << 16;
            h += f32::from_bits(w) * x[i]; i += 1;
        }

        let h = clamp_relu(h, clip_hidden);
        *hidden_out.get_unchecked_mut(j) = h;

        let wptr = w2_soa5_bf16.as_ptr().add(5 * j);
        acc[0] = acc[0].mul_add(f32::from_bits(((*wptr.add(0)) as u32) << 16), h);
        acc[1] = acc[1].mul_add(f32::from_bits(((*wptr.add(1)) as u32) << 16), h);
        acc[2] = acc[2].mul_add(f32::from_bits(((*wptr.add(2)) as u32) << 16), h);
        acc[3] = acc[3].mul_add(f32::from_bits(((*wptr.add(3)) as u32) << 16), h);
        acc[4] = acc[4].mul_add(f32::from_bits(((*wptr.add(4)) as u32) << 16), h);
    }

    for r in 0..5 {
        let mut v = acc[r];
        if v >  clip_logits { v =  clip_logits; }
        if v < -clip_logits { v = -clip_logits; }
        logits_out[r] = v;
    }
    _mm256_zeroupper();
    Ok(())
}

/// === ADD (verbatim): forward_fused_scaled_f16 (dispatch wrapper) ===
#[inline(always)]
fn forward_fused_scaled_f16(
    pd: usize, ph: usize,
    packed_w1_f16: &[u16], packed_b1: &[f32], w1_row_offsets: &[usize],
    packed_w2s_f16: &[u16], w2_row_offsets: &[usize],
    out_bias: &[f32; 5], clip_hidden: f32, clip_logits: f32,
    x: &[f32], hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    #[cfg(target_arch = "x86_64")]
    {
        // Use AVX-512/AVX2 FMAs; F16C conversion at load sites.
        if USE_AVX512 && std::is_x86_feature_detected!("avx512f") && std::is_x86_feature_detected!("f16c") {
            unsafe { return forward_fused_scaled_f16_avx512(pd, ph, packed_w1_f16, packed_b1, w1_row_offsets, packed_w2s_f16, w2_row_offsets, out_bias, clip_hidden, clip_logits, x, hidden_out, logits_out); }
        }
        if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") && std::is_x86_feature_detected!("f16c") {
            unsafe { return forward_fused_scaled_f16_avx2(pd, ph, packed_w1_f16, packed_b1, w1_row_offsets, packed_w2s_f16, w2_row_offsets, out_bias, clip_hidden, clip_logits, x, hidden_out, logits_out); }
        }
    }
    // If we get here, CPU lacks F16C or wide SIMD; f16 path should be disabled at constructor.
    Err(PredictionError::NumericalError)
}

/// === ADD (verbatim): AVX2 f16 fused kernel ===
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma,f16c")]
unsafe fn forward_fused_scaled_f16_avx2(
    pd: usize, ph: usize,
    w1_f16: &[u16], b1: &[f32], w1_row_offsets: &[usize],
    w2s_f16: &[u16], w2_row_offsets: &[usize],
    out_bias: &[f32; 5], clip_hidden: f32, clip_logits: f32,
    x: &[f32], hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    use core::arch::x86_64::*;
    #[inline(always)] unsafe fn hsum256_ps(v: __m256) -> f32 {
        let x = _mm256_hadd_ps(v, v);
        let y = _mm256_hadd_ps(x, x);
        _mm_cvtss_f32(_mm_add_ss(_mm256_castps256_ps128(y), _mm256_extractf128_ps(y, 1)))
    }
    const RNNE: i32 = _MM_FROUND_TO_NEAREST_INT | _MM_FROUND_NO_EXC;

    // hidden = relu(W1 * x + b1) with W1 in f16
    for j in 0..ph {
        let base = w1_row_offsets[j];
        let row_ptr = w1_f16.as_ptr().add(base) as *const i16; // 8 halfs per 128b
        let x_ptr   = x.as_ptr();

        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();

        let mut i = 0usize;
        while i + 16 <= pd {
            let h0 = _mm_loadu_si128(row_ptr.add(i) as *const _);
            let h1 = _mm_loadu_si128(row_ptr.add(i + 8) as *const _);
            let w0 = _mm256_cvtph_ps(h0);
            let w1 = _mm256_cvtph_ps(h1);
            let x0 = _mm256_loadu_ps(x_ptr.add(i));
            let x1 = _mm256_loadu_ps(x_ptr.add(i + 8));
            acc0 = _mm256_fmadd_ps(w0, x0, acc0);
            acc1 = _mm256_fmadd_ps(w1, x1, acc1);
            i += 16;
        }
        while i + 8 <= pd {
            let h = _mm_loadu_si128(row_ptr.add(i) as *const _);
            let w = _mm256_cvtph_ps(h);
            let x0 = _mm256_loadu_ps(x_ptr.add(i));
            acc0 = _mm256_fmadd_ps(w, x0, acc0);
            i += 8;
        }
        let mut sum = hsum256_ps(acc0) + hsum256_ps(acc1) + b1[j];
        while i < pd {
            // scalar tail: convert 4-or-less with SSE path
            let mut tmp_h = [0u16; 4];
            let mut tmp_x = [0f32; 4];
            let rem = (pd - i).min(4);
            core::ptr::copy_nonoverlapping(w1_f16.as_ptr().add(base + i), tmp_h.as_mut_ptr(), rem);
            core::ptr::copy_nonoverlapping(x_ptr.add(i), tmp_x.as_mut_ptr(), rem);
            let h128 = _mm_loadl_epi64(tmp_h.as_ptr() as *const _);
            let w128 = _mm_cvtph_ps(h128);
            let x128 = _mm_loadu_ps(tmp_x.as_ptr());
            let acc  = _mm_mul_ps(w128, x128);
            sum += _mm_cvtss_f32(acc)
                + _mm_cvtss_f32(_mm_shuffle_ps(acc, acc, 0b01_00_00_01))
                + _mm_cvtss_f32(_mm_shuffle_ps(acc, acc, 0b10_00_00_10))
                + _mm_cvtss_f32(_mm_shuffle_ps(acc, acc, 0b11_00_00_11));
            i += rem;
        }
        if sum < 0.0 { sum = 0.0; }
        if sum >  clip_hidden { sum =  clip_hidden; }
        if sum < -clip_hidden { sum = -clip_hidden; }
        hidden_out[j] = sum;
    }

    // logits = dot(W2s_f16[r], hidden) + out_bias[r]
    for r in 0..5 {
        let base = w2_row_offsets[r];
        let row_ptr = w2s_f16.as_ptr().add(base) as *const i16;
        let h_ptr   = hidden_out.as_ptr();

        let mut acc0 = _mm256_setzero_ps();
        let mut acc1 = _mm256_setzero_ps();

        let mut j = 0usize;
        while j + 16 <= ph {
            let h0 = _mm_loadu_si128(row_ptr.add(j) as *const _);
            let h1 = _mm_loadu_si128(row_ptr.add(j + 8) as *const _);
            let w0 = _mm256_cvtph_ps(h0);
            let w1 = _mm256_cvtph_ps(h1);
            let v0 = _mm256_loadu_ps(h_ptr.add(j));
            let v1 = _mm256_loadu_ps(h_ptr.add(j + 8));
            acc0 = _mm256_fmadd_ps(w0, v0, acc0);
            acc1 = _mm256_fmadd_ps(w1, v1, acc1);
            j += 16;
        }
        while j + 8 <= ph {
            let h = _mm_loadu_si128(row_ptr.add(j) as *const _);
            let w = _mm256_cvtph_ps(h);
            let v = _mm256_loadu_ps(h_ptr.add(j));
            acc0 = _mm256_fmadd_ps(w, v, acc0);
            j += 8;
        }
        let mut v = hsum256_ps(acc0) + hsum256_ps(acc1) + out_bias[r];
        while j < ph { v += (*w2s_f16.get_unchecked(base + j) as f32) * *h_ptr.add(j) * 0.0; /* should not happen */ j += 1; }
        if v >  clip_logits { v =  clip_logits; }
        if v < -clip_logits { v = -clip_logits; }
        logits_out[r] = v;
    }
    Ok(())
}

/// === ADD (verbatim): AVX-512 f16 fused kernel (converts 2x8 halves -> 512) ===
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx2,fma,f16c")]
unsafe fn forward_fused_scaled_f16_avx512(
    pd: usize, ph: usize,
    w1_f16: &[u16], b1: &[f32], w1_row_offsets: &[usize],
    w2s_f16: &[u16], w2_row_offsets: &[usize],
    out_bias: &[f32; 5], clip_hidden: f32, clip_logits: f32,
    x: &[f32], hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    use core::arch::x86_64::*;
    #[inline(always)] unsafe fn hsum512_ps(v: __m512) -> f32 {
        let lo: __m256 = _mm512_castps512_ps256(v);
        let hi: __m256 = _mm256_castps128_ps256(_mm512_extractf32x8_ps(v, 1));
        let x = _mm256_hadd_ps(lo, lo); let y = _mm256_hadd_ps(x, x);
        let lo_s = _mm_cvtss_f32(_mm_add_ss(_mm256_castps256_ps128(y), _mm256_extractf128_ps(y, 1)));
        let x2 = _mm256_hadd_ps(hi, hi); let y2 = _mm256_hadd_ps(x2, x2);
        let hi_s = _mm_cvtss_f32(_mm_add_ss(_mm256_castps256_ps128(y2), _mm256_extractf128_ps(y2, 1)));
        lo_s + hi_s
    }

    // hidden
    for j in 0..ph {
        let base = w1_row_offsets[j];
        let row_ptr = w1_f16.as_ptr().add(base) as *const i16;
        let x_ptr   = x.as_ptr();

        let mut acc0 = _mm512_setzero_ps();
        let mut acc1 = _mm512_setzero_ps();

        let mut i = 0usize;
        while i + 16 <= pd {
            let h0 = _mm_loadu_si128(row_ptr.add(i) as *const _);
            let h1 = _mm_loadu_si128(row_ptr.add(i + 8) as *const _);
            let w0_256 = _mm256_cvtph_ps(h0);
            let w1_256 = _mm256_cvtph_ps(h1);
            let w0 = _mm512_zextps256_ps512(w0_256);
            let w1 = _mm512_zextps256_ps512(w1_256);
            let x0 = _mm512_loadu_ps(x_ptr.add(i));
            let x1 = _mm512_loadu_ps(x_ptr.add(i)); // reuse base; accumulate separately
            acc0 = _mm512_fmadd_ps(w0, x0, acc0);
            acc1 = _mm512_fmadd_ps(w1, _mm512_loadu_ps(x_ptr.add(i + 16 - 16)), acc1);
            i += 16;
        }
        let mut sum = hsum512_ps(acc0) + hsum512_ps(acc1) + b1[j];
        while i < pd {
            // scalar tail: convert 4 halves
            let mut tmp_h = [0u16; 4];
            let mut tmp_x = [0f32; 4];
            let rem = (pd - i).min(4);
            core::ptr::copy_nonoverlapping(w1_f16.as_ptr().add(base + i), tmp_h.as_mut_ptr(), rem);
            core::ptr::copy_nonoverlapping(x_ptr.add(i), tmp_x.as_mut_ptr(), rem);
            let h128 = _mm_loadl_epi64(tmp_h.as_ptr() as *const _);
            let w128 = _mm_cvtph_ps(h128);
            let x128 = _mm_loadu_ps(tmp_x.as_ptr());
            let acc  = _mm_mul_ps(w128, x128);
            sum += _mm_cvtss_f32(acc)
                + _mm_cvtss_f32(_mm_shuffle_ps(acc, acc, 0b01_00_00_01))
                + _mm_cvtss_f32(_mm_shuffle_ps(acc, acc, 0b10_00_00_10))
                + _mm_cvtss_f32(_mm_shuffle_ps(acc, acc, 0b11_00_00_11));
            i += rem;
        }
        if sum < 0.0 { sum = 0.0; }
        if sum >  clip_hidden { sum =  clip_hidden; }
        if sum < -clip_hidden { sum = -clip_hidden; }
        hidden_out[j] = sum;
    }

    // logits
    for r in 0..5 {
        let base = w2_row_offsets[r];
        let row_ptr = w2s_f16.as_ptr().add(base) as *const i16;
        let h_ptr   = hidden_out.as_ptr();

        let mut acc0 = _mm512_setzero_ps();
        let mut acc1 = _mm512_setzero_ps();

        let mut j = 0usize;
        while j + 16 <= ph {
            let h0 = _mm_loadu_si128(row_ptr.add(j) as *const _);
            let h1 = _mm_loadu_si128(row_ptr.add(j + 8) as *const _);
            let w0 = _mm512_zextps256_ps512(_mm256_cvtph_ps(h0));
            let w1 = _mm512_zextps256_ps512(_mm256_cvtph_ps(h1));
            let v0 = _mm512_loadu_ps(h_ptr.add(j));
            let v1 = _mm512_loadu_ps(h_ptr.add(j)); // symmetrical accumulation
            acc0 = _mm512_fmadd_ps(w0, v0, acc0);
            acc1 = _mm512_fmadd_ps(w1, _mm512_loadu_ps(h_ptr.add(j + 16 - 16)), acc1);
            j += 16;
        }
        let mut v = hsum512_ps(acc0) + hsum512_ps(acc1) + out_bias[r];
        while j < ph { v += *h_ptr.add(j) * 0.0; j += 1; } // not reached in padded design
        if v >  clip_logits { v =  clip_logits; }
        if v < -clip_logits { v = -clip_logits; }
        logits_out[r] = v;
    }
    Ok(())
}

/// === ADD (verbatim): forward_input_sparse_scaled_f16 (dispatch wrapper) ===
#[inline(always)]
fn forward_input_sparse_scaled_f16(
    ph: usize,
    w1t_f16: &[u16], packed_b1: &[f32], w1_col_offsets: &[usize],
    w2s_f16: &[u16], w2_row_offsets: &[usize],
    out_bias: &[f32; 5], clip_hidden: f32, clip_logits: f32,
    x: &[f32], nz_idx: &[usize],
    hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    #[cfg(target_arch = "x86_64")]
    {
        if USE_AVX512 && std::is_x86_feature_detected!("avx512f") && std::is_x86_feature_detected!("f16c") {
            unsafe { return forward_input_sparse_scaled_f16_avx512(ph, w1t_f16, packed_b1, w1_col_offsets, w2s_f16, w2_row_offsets, out_bias, clip_hidden, clip_logits, x, nz_idx, hidden_out, logits_out); }
        }
        if std::is_x86_feature_detected!("avx2") && std::is_x86_feature_detected!("fma") && std::is_x86_feature_detected!("f16c") {
            unsafe { return forward_input_sparse_scaled_f16_avx2(ph, w1t_f16, packed_b1, w1_col_offsets, w2s_f16, w2_row_offsets, out_bias, clip_hidden, clip_logits, x, nz_idx, hidden_out, logits_out); }
        }
    }
    Err(PredictionError::NumericalError)
}

/// === ADD (verbatim): AVX2 f16 input-sparse kernel ===
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2,fma,f16c")]
unsafe fn forward_input_sparse_scaled_f16_avx2(
    ph: usize,
    w1t_f16: &[u16], packed_b1: &[f32], w1_col_offsets: &[usize],
    w2s_f16: &[u16], w2_row_offsets: &[usize],
    out_bias: &[f32; 5], clip_hidden: f32, clip_logits: f32,
    x: &[f32], nz_idx: &[usize],
    hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    use core::arch::x86_64::*;
    // Seed hidden with b1
    let mut j = 0usize; while j + 8 <= ph {
        _mm256_storeu_ps(hidden_out.as_mut_ptr().add(j), _mm256_loadu_ps(packed_b1.as_ptr().add(j))); j += 8;
    }
    while j < ph { hidden_out[j] = packed_b1[j]; j += 1; }

    // Accumulate columns: hidden += xi * col_i (col_i held in f16)
    for &i in nz_idx {
        let base = w1_col_offsets[i];
        let col_ptr = w1t_f16.as_ptr().add(base) as *const i16;
        let xib = _mm256_set1_ps(x[i]);

        let mut k = 0usize;
        while k + 8 <= ph {
            let h = _mm_loadu_si128(col_ptr.add(k) as *const _);
            let w = _mm256_cvtph_ps(h);
            let dst = _mm256_loadu_ps(hidden_out.as_ptr().add(k));
            let sum = _mm256_fmadd_ps(w, xib, dst);
            _mm256_storeu_ps(hidden_out.as_mut_ptr().add(k), sum);
            k += 8;
        }
        while k < ph {
            let mut tmp_h = [0u16; 4];
            let rem = (ph - k).min(4);
            core::ptr::copy_nonoverlapping(w1t_f16.as_ptr().add(base + k), tmp_h.as_mut_ptr(), rem);
            let h128 = _mm_loadl_epi64(tmp_h.as_ptr() as *const _);
            let w128 = _mm_cvtph_ps(h128);
            let mut tmp = [0f32; 4]; _mm_storeu_ps(tmp.as_mut_ptr(), w128);
            for t in 0..rem { hidden_out[k + t] = hidden_out[k + t].mul_add(x[i], tmp[t]); }
            k += rem;
        }
    }

    // ReLU + clamp
    let zero = _mm256_set1_ps(0.0);
    let clipv = _mm256_set1_ps(clip_hidden);
    let nclipv = _mm256_set1_ps(-clip_hidden);
    let mut k2 = 0usize;
    while k2 + 8 <= ph {
        let v = _mm256_loadu_ps(hidden_out.as_ptr().add(k2));
        let v = _mm256_max_ps(v, zero);
        let v = _mm256_min_ps(_mm256_max_ps(v, nclipv), clipv);
        _mm256_storeu_ps(hidden_out.as_mut_ptr().add(k2), v);
        k2 += 8;
    }
    while k2 < ph {
        let mut v = hidden_out[k2];
        if v < 0.0 { v = 0.0; }
        if v >  clip_hidden { v =  clip_hidden; }
        if v < -clip_hidden { v = -clip_hidden; }
        hidden_out[k2] = v; k2 += 1;
    }

    // logits
    #[inline(always)]
    unsafe fn hsum256_ps(v: __m256) -> f32 {
        let x = _mm256_hadd_ps(v, v); let y = _mm256_hadd_ps(x, x);
        _mm_cvtss_f32(_mm_add_ss(_mm256_castps256_ps128(y), _mm256_extractf128_ps(y, 1)))
    }
    for r in 0..5 {
        let base = w2_row_offsets[r];
        let row_ptr = w2s_f16.as_ptr().add(base) as *const i16;
        let h_ptr   = hidden_out.as_ptr();

        let mut acc0 = _mm256_setzero_ps(); let mut acc1 = _mm256_setzero_ps();
        let mut j = 0usize;
        while j + 16 <= ph {
            let h0 = _mm_loadu_si128(row_ptr.add(j) as *const _);
            let h1 = _mm_loadu_si128(row_ptr.add(j + 8) as *const _);
            let w0 = _mm256_cvtph_ps(h0);
            let w1 = _mm256_cvtph_ps(h1);
            let v0 = _mm256_loadu_ps(h_ptr.add(j));
            let v1 = _mm256_loadu_ps(h_ptr.add(j + 8));
            acc0 = _mm256_fmadd_ps(w0, v0, acc0);
            acc1 = _mm256_fmadd_ps(w1, v1, acc1);
            j += 16;
        }
        while j + 8 <= ph {
            let h = _mm_loadu_si128(row_ptr.add(j) as *const _);
            let w = _mm256_cvtph_ps(h);
            let v = _mm256_loadu_ps(h_ptr.add(j));
            acc0 = _mm256_fmadd_ps(w, v, acc0);
            j += 8;
        }
        let mut v = hsum256_ps(acc0) + hsum256_ps(acc1) + out_bias[r];
        while j < ph { v += *h_ptr.add(j) * 0.0; j += 1; }
        if v >  clip_logits { v =  clip_logits; }
        if v < -clip_logits { v = -clip_logits; }
        logits_out[r] = v;
    }
    Ok(())
}

/// === ADD (verbatim): AVX-512 f16 input-sparse kernel ===
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f,avx2,fma,f16c")]
unsafe fn forward_input_sparse_scaled_f16_avx512(
    ph: usize,
    w1t_f16: &[u16], packed_b1: &[f32], w1_col_offsets: &[usize],
    w2s_f16: &[u16], w2_row_offsets: &[usize],
    out_bias: &[f32; 5], clip_hidden: f32, clip_logits: f32,
    x: &[f32], nz_idx: &[usize],
    hidden_out: &mut [f32], logits_out: &mut [f32],
) -> Result<(), PredictionError> {
    use core::arch::x86_64::*;
    // b1 copy
    let mut j = 0usize; while j + 16 <= ph {
        _mm512_storeu_ps(hidden_out.as_mut_ptr().add(j), _mm512_loadu_ps(packed_b1.as_ptr().add(j))); j += 16;
    }
    while j < ph { hidden_out[j] = packed_b1[j]; j += 1; }

    for &i in nz_idx {
        let base = w1_col_offsets[i];
        let col_ptr = w1t_f16.as_ptr().add(base) as *const i16;
        let xib256  = _mm256_set1_ps(x[i]);

        let mut k = 0usize;
        while k + 16 <= ph {
            let h0 = _mm_loadu_si128(col_ptr.add(k) as *const _);
            let h1 = _mm_loadu_si128(col_ptr.add(k + 8) as *const _);
            let w0 = _mm256_cvtph_ps(h0);
            let w1 = _mm256_cvtph_ps(h1);

            let dst0 = _mm256_loadu_ps(hidden_out.as_ptr().add(k));
            let dst1 = _mm256_loadu_ps(hidden_out.as_ptr().add(k + 8));
            let sum0 = _mm256_fmadd_ps(w0, xib256, dst0);
            let sum1 = _mm256_fmadd_ps(w1, xib256, dst1);
            _mm256_storeu_ps(hidden_out.as_mut_ptr().add(k), sum0);
            _mm256_storeu_ps(hidden_out.as_mut_ptr().add(k + 8), sum1);
            k += 16;
        }
        while k + 8 <= ph {
            let h = _mm_loadu_si128(col_ptr.add(k) as *const _);
            let w = _mm256_cvtph_ps(h);
            let dst = _mm256_loadu_ps(hidden_out.as_ptr().add(k));
            let sum = _mm256_fmadd_ps(w, xib256, dst);
            _mm256_storeu_ps(hidden_out.as_mut_ptr().add(k), sum);
            k += 8;
        }
        while k < ph {
            // scalar tail via SSE convert
            let mut tmp_h = [0u16; 4];
            let rem = (ph - k).min(4);
            core::ptr::copy_nonoverlapping(w1t_f16.as_ptr().add(base + k), tmp_h.as_mut_ptr(), rem);
            let h128 = _mm_loadl_epi64(tmp_h.as_ptr() as *const _);
            let w128 = _mm_cvtph_ps(h128);
            let mut tmp = [0f32; 4]; _mm_storeu_ps(tmp.as_mut_ptr(), w128);
            for t in 0..rem { hidden_out[k + t] = hidden_out[k + t].mul_add(x[i], tmp[t]); }
            k += rem;
        }
    }

    // ReLU + clamp (reuse AVX2 pass is fine)
    let zero = _mm256_set1_ps(0.0);
    let clipv = _mm256_set1_ps(clip_hidden);
    let nclipv = _mm256_set1_ps(-clip_hidden);
    let mut k2 = 0usize;
    while k2 + 8 <= ph {
        let v = _mm256_loadu_ps(hidden_out.as_ptr().add(k2));
        let v = _mm256_max_ps(v, zero);
        let v = _mm256_min_ps(_mm256_max_ps(v, nclipv), clipv);
        _mm256_storeu_ps(hidden_out.as_mut_ptr().add(k2), v);
        k2 += 8;
    }
    while k2 < ph {
        let mut v = hidden_out[k2];
        if v < 0.0 { v = 0.0; }
        if v >  clip_hidden { v =  clip_hidden; }
        if v < -clip_hidden { v = -clip_hidden; }
        hidden_out[k2] = v; k2 += 1;
    }

    // logits: use AVX2 FMA/F16C in 2x8 blocks inside AVX-512 function (portable; fast)
    #[inline(always)]
    unsafe fn hsum256_ps(v: __m256) -> f32 {
        let x = _mm256_hadd_ps(v, v); let y = _mm256_hadd_ps(x, x);
        _mm_cvtss_f32(_mm_add_ss(_mm256_castps256_ps128(y), _mm256_extractf128_ps(y, 1)))
    }
    for r in 0..5 {
        let base = w2_row_offsets[r];
        let row_ptr = w2s_f16.as_ptr().add(base) as *const i16;
        let h_ptr   = hidden_out.as_ptr();

        let mut acc0 = _mm256_setzero_ps(); let mut acc1 = _mm256_setzero_ps();
        let mut j = 0usize;
        while j + 16 <= ph {
            let h0 = _mm_loadu_si128(row_ptr.add(j) as *const _);
            let h1 = _mm_loadu_si128(row_ptr.add(j + 8) as *const _);
            let w0 = _mm256_cvtph_ps(h0);
            let w1 = _mm256_cvtph_ps(h1);
            let v0 = _mm256_loadu_ps(h_ptr.add(j));
            let v1 = _mm256_loadu_ps(h_ptr.add(j + 8));
            acc0 = _mm256_fmadd_ps(w0, v0, acc0);
            acc1 = _mm256_fmadd_ps(w1, v1, acc1);
            j += 16;
        }
        while j + 8 <= ph {
            let h = _mm_loadu_si128(row_ptr.add(j) as *const _);
            let w = _mm256_cvtph_ps(h);
            let v = _mm256_loadu_ps(h_ptr.add(j));
            acc0 = _mm256_fmadd_ps(w, v, acc0);
            j += 8;
        }
        let mut v = hsum256_ps(acc0) + hsum256_ps(acc1) + out_bias[r];
        while j < ph { v += *h_ptr.add(j) * 0.0; j += 1; }
        if v >  clip_logits { v =  clip_logits; }
        if v < -clip_logits { v = -clip_logits; }
        logits_out[r] = v;
    }
    Ok(())
}

/// === REPLACE: standardize_and_clamp (signature & body) ===
/// Identity copy with optional per-feature clamp; tail stays zero.
#[inline(always)]
fn standardize_and_clamp(x: &[f32], fmin: &[f32], fmax: &[f32], out: &mut [f32]) {
    let n = x.len();
    debug_assert!(out.len() >= n);
    let has_mm = !fmin.is_empty() && !fmax.is_empty() && fmin.len() == n && fmax.len() == n;

    if has_mm {
        for i in 0..n {
            let v = x[i];
            // Per-feature clamp first, then global RAW_FEATURE_CLAMP as final guard
            let vv = v.max(fmin[i]).min(fmax[i]).clamp(-RAW_FEATURE_CLAMP, RAW_FEATURE_CLAMP);
            out[i] = vv;
        }
    } else {
        for i in 0..n {
            out[i] = x[i].clamp(-RAW_FEATURE_CLAMP, RAW_FEATURE_CLAMP);
        }
    }
}

/// === REPLACE: softmax_stable ===
/// Stable softmax with temperature scaling for exactly 5 classes, unrolled.
/// Uses fast polynomial exp + cutoff when toggled.
#[inline(always)]
fn softmax_stable(logits: &[f32], temperature: f32, out: &mut [f32; 5]) -> Result<(), PredictionError> {
    if logits.len() != 5 { return Err(PredictionError::NumericalError); }
    let inv_t = 1.0 / temperature;

    let z0 = logits[0] * inv_t;
    let z1 = logits[1] * inv_t;
    let z2 = logits[2] * inv_t;
    let z3 = logits[3] * inv_t;
    let z4 = logits[4] * inv_t;

    let m01   = if z0 > z1 { z0 } else { z1 };
    let m23   = if z2 > z3 { z2 } else { z3 };
    let m0123 = if m01 > m23 { m01 } else { m23 };
    let maxv  = if m0123 > z4 { m0123 } else { z4 };

    // Compute exponentials with cutoff
    let t0 = z0 - maxv;
    let t1 = z1 - maxv;
    let t2 = z2 - maxv;
    let t3 = z3 - maxv;
    let t4 = z4 - maxv;

    let e0 = if t0 < SOFTMAX_EXP_CUTOFF { 0.0 } else { if USE_FAST_EXP { fast_exp_f32(t0) } else { t0.exp() } };
    let e1 = if t1 < SOFTMAX_EXP_CUTOFF { 0.0 } else { if USE_FAST_EXP { fast_exp_f32(t1) } else { t1.exp() } };
    let e2 = if t2 < SOFTMAX_EXP_CUTOFF { 0.0 } else { if USE_FAST_EXP { fast_exp_f32(t2) } else { t2.exp() } };
    let e3 = if t3 < SOFTMAX_EXP_CUTOFF { 0.0 } else { if USE_FAST_EXP { fast_exp_f32(t3) } else { t3.exp() } };
    let e4 = if t4 < SOFTMAX_EXP_CUTOFF { 0.0 } else { if USE_FAST_EXP { fast_exp_f32(t4) } else { t4.exp() } };

    let sum = (((e0 + e1) + (e2 + e3)) + e4);
    if !(sum.is_finite()) || sum <= 0.0 { return Err(PredictionError::NumericalError); }
    let inv_sum = 1.0 / sum;

    out[0] = e0 * inv_sum;
    out[1] = e1 * inv_sum;
    out[2] = e2 * inv_sum;
    out[3] = e3 * inv_sum;
    out[4] = e4 * inv_sum;
    Ok(())
}

/// Return (argmax_index, argmax_value, second_best_value)
fn argmax_with_second(probs: &[f32; 5]) -> (usize, f32, f32) {
    let mut best_i = 0usize;
    let mut best_v = probs[0];
    let mut second_v = f32::NEG_INFINITY;

    for i in 1..5 {
        let v = probs[i];
        if v > best_v {
            second_v = best_v;
            best_v = v;
            best_i = i;
        } else if v > second_v {
            second_v = v;
        }
    }
    (best_i, best_v, second_v)
}

/// Branchless top-2 selection for 5 elements
#[inline(always)]
fn top2_5(vals: [f32; 5]) -> (usize, f32, f32) {
    let mut best_i = 0usize;
    let mut best_v = vals[0];
    let mut second_v = f32::NEG_INFINITY;

    for i in 1..5 {
        let v = vals[i];
        if v > best_v {
            second_v = best_v;
            best_v = v;
            best_i = i;
        } else if v > second_v {
            second_v = v;
        }
    }
    (best_i, best_v, second_v)
}

/// === REPLACE: hash_input_tensor (parallel FNV-1a lanes; 256-bit) ===
#[inline(always)]
fn hash_input_tensor(x: &[f32]) -> [u8; 32] {
    const FNV_OFF: u64 = 0xcbf29ce484222325;
    const FNV_PRM: u64 = 0x100000001b3;
    let mut a0 = FNV_OFF ^ (x.len() as u64);
    let mut a1 = FNV_OFF.wrapping_mul(0x9e3779b185ebca87);
    let mut a2 = FNV_OFF.wrapping_mul(0xc2b2ae3d27d4eb4f);
    let mut a3 = FNV_OFF.wrapping_mul(0x165667b19e3779f9);

    let mut i = 0usize;
    while i + 4 <= x.len() {
        let b0 = x[i    ].to_bits() as u64;
        let b1 = x[i + 1].to_bits() as u64;
        let b2 = x[i + 2].to_bits() as u64;
        let b3 = x[i + 3].to_bits() as u64;

        a0 ^= b0; a0 = a0.wrapping_mul(FNV_PRM);
        a1 ^= b1; a1 = a1.wrapping_mul(FNV_PRM);
        a2 ^= b2; a2 = a2.wrapping_mul(FNV_PRM);
        a3 ^= b3; a3 = a3.wrapping_mul(FNV_PRM);
        i += 4;
    }
    while i < x.len() {
        let b = x[i].to_bits() as u64;
        a0 ^= b; a0 = a0.wrapping_mul(FNV_PRM);
        i += 1;
    }

    // final avalanche (xorshift-mix)
    #[inline(always)]
    fn fmix(mut z: u64) -> u64 {
        z ^= z >> 33; z = z.wrapping_mul(0xff51afd7ed558ccd);
        z ^= z >> 33; z = z.wrapping_mul(0xc4ceb9fe1a85ec53);
        z ^= z >> 33; z
    }
    let h0 = fmix(a0 ^ a2);
    let h1 = fmix(a1 ^ a3);
    let h2 = fmix(a0.wrapping_add(a3));
    let h3 = fmix(a1.wrapping_add(a2));

    let mut out = [0u8; 32];
    out[ 0.. 8].copy_from_slice(&h0.to_le_bytes());
    out[ 8..16].copy_from_slice(&h1.to_le_bytes());
    out[16..24].copy_from_slice(&h2.to_le_bytes());
    out[24..32].copy_from_slice(&h3.to_le_bytes());
    out
}



#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn build_model_file_bytes() -> Vec<u8> {
        // Small deterministic model: feature_count=5, hidden=4, output=5
        let w = ModelWeights {
            // w1: 4x5
            w1: vec![
                0.10, 0.20, 0.30, 0.40, 0.50,
                0.20, 0.30, 0.40, 0.50, 0.60,
                0.30, 0.40, 0.50, 0.60, 0.70,
                0.40, 0.50, 0.60, 0.70, 0.80,
            ],
            b1: vec![0.10, 0.20, 0.30, 0.40],
            // w2: 5x4
            w2: vec![
                0.10, 0.20, 0.30, 0.40,
                0.20, 0.30, 0.40, 0.50,
                0.30, 0.40, 0.50, 0.60,
                0.40, 0.50, 0.60, 0.70,
                0.50, 0.60, 0.70, 0.80,
            ],
            b2: vec![0.10, 0.20, 0.30, 0.40, 0.50],
            feature_count: 5,
            hidden_size: 4,
            output_size: 5,
            feature_mean: vec![0.0; 5],
            feature_std: vec![1.0; 5],
            feature_min: vec![],
            feature_max: vec![],
            temperature: 1.0,
            version: 1,
            class_bias: vec![],
            logit_gain: 1.0,
            class_gain: vec![],
            clip_hidden: 1.0e6,
            clip_logits: 1.0e6,
            w1_sparse_col_idx: vec![],
        };
        let checksum = compute_weights_checksum(&w);
        let mf = ModelFile {
            magic: *b"INTENTV1",
            version: 1,
            weights: w,
            checksum,
        };
        bincode::serialize(&mf).unwrap()
    }

    #[test]
    fn test_prediction_consistency() {
        let bytes = build_model_file_bytes();
        let temp_path = PathBuf::from("/tmp/test_intent_model.bin");
        std::fs::write(&temp_path, &bytes).unwrap();

        let mut predictor = IntentPredictor::new(&temp_path).unwrap();

        let features = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let slot = 12345;

        let r1 = predictor.predict(&features, slot).unwrap();
        let r2 = predictor.predict(&features, slot).unwrap();
        let r3 = predictor.predict(&features, slot).unwrap();

        assert_eq!(r1.class, r2.class);
        assert_eq!(r2.class, r3.class);
        assert!((r1.confidence - r2.confidence).abs() < 1e-7);
        assert!((r2.confidence - r3.confidence).abs() < 1e-7);
        assert_eq!(r1.slot, r2.slot);
        assert_eq!(r2.slot, r3.slot);

        std::fs::remove_file(&temp_path).unwrap();
    }

    #[test]
    fn test_hash_input_tensor() {
        let features = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let h1 = hash_input_tensor(&features);
        let h2 = hash_input_tensor(&features);
        assert_eq!(h1, h2);

        let h3 = hash_input_tensor(&[2.0, 3.0, 4.0, 5.0, 6.0]);
        assert_ne!(h1, h3);
    }

    #[test]
    fn test_invalid_feature_size() {
        let bytes = build_model_file_bytes();
        let temp_path = PathBuf::from("/tmp/test_intent_model_invalid.bin");
        std::fs::write(&temp_path, &bytes).unwrap();

        let mut predictor = IntentPredictor::new(&temp_path).unwrap();
        let bad_features = vec![1.0, 2.0, 3.0]; // should be 5
        let res = predictor.predict(&bad_features, 42);
        assert!(matches!(res, Err(PredictionError::FeatureSizeMismatch { .. })));

        std::fs::remove_file(&temp_path).unwrap();
    }
}
