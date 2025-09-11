#![forbid(unsafe_code)]
#![deny(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
    f64::consts::PI,
};

use parking_lot::RwLock;                  // faster than std::sync::RwLock
use smallvec::SmallVec;                   // stack-alloc hot paths
use once_cell::sync::Lazy;

use tokio::sync::mpsc;
use rayon::prelude::*;
use ahash::{AHashMap, AHashSet};

use nalgebra::{DMatrix, DVector, SymmetricEigen};

use solana_client::nonblocking::rpc_client::RpcClient; // nonblocking
use solana_client::rpc_config::RpcTransactionConfig;
use solana_sdk::{pubkey::Pubkey, signature::Signature, commitment_config::CommitmentConfig};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta,
    UiTransactionEncoding,
    option_serializer::OptionSerializer,
};

// === [IMPROVED: PIECE α2] constants + fast LUTs ===
const CORRELATION_WINDOW: usize = 512;
const MIN_CORRELATION_THRESHOLD: f64 = 0.73;
const ENTANGLEMENT_DECAY_FACTOR: f64 = 0.94;
const MAX_TRACKED_ENTITIES: usize = 4096;
const CORRELATION_UPDATE_INTERVAL: Duration = Duration::from_millis(250);
const QUANTUM_PHASE_BINS: usize = 64;
const MIN_TRANSACTION_VOLUME: f64 = 0.1;
const EIGENVALUE_THRESHOLD: f64 = 0.01;
const MAX_CORRELATION_DEPTH: usize = 5;

// Precompute bin offsets: (sin θ_i, cos θ_i) for θ_i = 2π i / B
static PHASE_OFFSETS: Lazy<[(f64, f64); QUANTUM_PHASE_BINS]> = Lazy::new(|| {
    let mut arr = [(0.0, 0.0); QUANTUM_PHASE_BINS];
    for i in 0..QUANTUM_PHASE_BINS {
        let theta = (i as f64) * 2.0 * PI / (QUANTUM_PHASE_BINS as f64);
        arr[i] = (theta.sin(), theta.cos());
    }
    arr
});

// === [IMPROVED+++++: PIECE ε0] Precomputed decay LUT ===
static DECAY_LUT: Lazy<[f64; QUANTUM_PHASE_BINS]> = Lazy::new(|| {
    let mut arr = [0.0_f64; QUANTUM_PHASE_BINS];
    let n = QUANTUM_PHASE_BINS as f64;
    for i in 0..QUANTUM_PHASE_BINS { arr[i] = (-ENTANGLEMENT_DECAY_FACTOR * (i as f64) / n).exp(); }
    arr
});

#[inline(always)]
fn ord_pair(a: &Pubkey, b: &Pubkey) -> (Pubkey, Pubkey) {
    if a < b { (*a, *b) } else { (*b, *a) }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntanglementMetrics {
    pub mutual_information: f64,
    pub phase_correlation: f64,
    pub amplitude_coupling: f64,
    pub temporal_coherence: f64,
    pub entanglement_entropy: f64,
    pub bell_inequality_violation: f64,
}

#[derive(Debug, Clone)]
pub struct EntityProfile {
    pub pubkey: Pubkey,
    pub transaction_history: VecDeque<TransactionFeatures>,
    pub correlation_matrix: AHashMap<Pubkey, f64>,
    pub entanglement_state: QuantumState,
    pub last_update: Instant,
    pub activity_score: f64,
}

#[derive(Debug, Clone)]
pub struct TransactionFeatures {
    pub signature: Signature,
    pub timestamp: i64,
    pub volume: f64,
    pub accounts: Vec<Pubkey>,
    pub instruction_count: usize,
    pub success: bool,
    pub gas_used: u64,
    pub phase_vector: Vec<f64>,
}

#[derive(Debug, Clone)]
pub struct QuantumState {
    pub amplitude: Vec<Complex>,
    pub phase: Vec<f64>,
    pub coherence_length: f64,
    pub entanglement_degree: f64,
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct Complex {
    pub real: f64,
    pub imag: f64,
}

impl Complex {
    #[inline(always)]
    fn mag2(self) -> f64 { self.real * self.real + self.imag * self.imag }
    #[inline(always)]
    fn magnitude(self) -> f64 { self.mag2().sqrt() }
}

#[derive(Debug, Clone)]
pub struct CorrelationCluster {
    pub core_entities: Vec<Pubkey>,
    pub peripheral_entities: Vec<Pubkey>,
    pub cluster_strength: f64,
    pub activity_pattern: ActivityPattern,
    pub profitability_score: f64,
}

#[derive(Debug, Clone)]
pub struct ActivityPattern {
    pub periodicity: f64,
    pub burst_intensity: f64,
    pub directional_flow: HashMap<Pubkey, f64>,
    pub temporal_signature: Vec<f64>,
}

pub struct EntanglementCorrelationFinder {
    rpc_client: Arc<RpcClient>,
    entity_profiles: Arc<RwLock<AHashMap<Pubkey, EntityProfile>>>,
    correlation_cache: Arc<RwLock<AHashMap<(Pubkey, Pubkey), EntanglementMetrics>>>,
    clusters: Arc<RwLock<Vec<CorrelationCluster>>>,
    phase_distribution: Arc<RwLock<DMatrix<f64>>>,
    eigenspace: Arc<RwLock<Option<(DMatrix<f64>, DVector<f64>)>>>,
    update_channel: mpsc::Sender<TransactionUpdate>,
}

#[derive(Debug)]
pub struct TransactionUpdate {
    pub signature: Signature,
    pub accounts: Vec<Pubkey>,
    pub volume: f64,
    pub timestamp: i64,
    pub success: bool,
}

impl EntanglementCorrelationFinder {
    pub fn new(rpc_endpoint: &str) -> Self {
        let (tx, mut rx) = mpsc::channel::<TransactionUpdate>(8192);
        let rpc_client = Arc::new(RpcClient::new(rpc_endpoint.to_string()));
        
        let finder = Self {
            rpc_client: rpc_client.clone(),
            entity_profiles: Arc::new(RwLock::new(AHashMap::with_capacity(MAX_TRACKED_ENTITIES))),
            correlation_cache: Arc::new(RwLock::new(AHashMap::with_capacity(MAX_TRACKED_ENTITIES * 8))),
            clusters: Arc::new(RwLock::new(Vec::with_capacity(256))),
            phase_distribution: Arc::new(RwLock::new(DMatrix::zeros(QUANTUM_PHASE_BINS, QUANTUM_PHASE_BINS))),
            eigenspace: Arc::new(RwLock::new(None)),
            update_channel: tx.clone(),
        };

        let profiles = finder.entity_profiles.clone();
        let cache = finder.correlation_cache.clone();
        let clusters = finder.clusters.clone();
        
        tokio::spawn(async move {
            while let Some(update) = rx.recv().await {
                if update.volume < MIN_TRANSACTION_VOLUME { continue; }
                Self::process_transaction_update(update, profiles.clone(), cache.clone(), clusters.clone()).await;
            }
        });

        finder
    }

    pub async fn analyze_transaction(&self, signature: &Signature) -> Result<Vec<CorrelationCluster>, Box<dyn std::error::Error>> {
        // δ0: explicit config and early short-circuits
        let cfg = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };
        let tx = self.rpc_client.get_transaction_with_config(signature, cfg).await?;

        let Some(tx_meta) = tx.transaction else { return Ok(vec![]) };
        if tx_meta.meta.as_ref().map(|m| m.err.is_some()).unwrap_or(false) {
            return Ok(vec![]);
        }

        let accounts = self.extract_accounts(&tx_meta);
        let volume = self.calculate_volume(&tx_meta);
        if volume < MIN_TRANSACTION_VOLUME { return Ok(vec![]); }
        let timestamp = tx.block_time.unwrap_or(0);

        let update = TransactionUpdate {
            signature: *signature,
            accounts: accounts.clone(),
            volume,
            timestamp,
            success: true,
        };

        if self.update_channel.try_send(update).is_err() {
            let _ = self.update_channel.send(update).await;
        }
        self.find_correlated_clusters(&accounts, volume).await
    }

    async fn process_transaction_update(
        update: TransactionUpdate,
        profiles: Arc<RwLock<AHashMap<Pubkey, EntityProfile>>>,
        cache: Arc<RwLock<AHashMap<(Pubkey, Pubkey), EntanglementMetrics>>>,
        clusters: Arc<RwLock<Vec<CorrelationCluster>>>,
    ) {
        // ε4: lock-slim – compute off-lock, then write fast
        let phase_vector = Self::compute_phase_vector(&update);
        let features = TransactionFeatures {
            signature: update.signature,
            timestamp: update.timestamp,
            volume: update.volume,
            accounts: update.accounts.clone(),
            instruction_count: update.accounts.len(),
            success: update.success,
            gas_used: 5000,
            phase_vector: phase_vector.to_vec(),
        };

        // Snapshot states off the write lock
        let snapshots: AHashMap<Pubkey, QuantumState> = {
            let pr = profiles.read();
            update.accounts.iter().filter_map(|k| pr.get(k).map(|p| (*k, p.entanglement_state.clone()))).collect()
        };

        // Compute new states in parallel off-lock
        let computed: AHashMap<Pubkey, QuantumState> = update.accounts
            .par_iter()
            .map(|k| {
                let prev = snapshots.get(k).cloned().unwrap_or_else(|| Self::initialize_quantum_state());
                let next = Self::update_quantum_state(&prev, &phase_vector, update.volume);
                (*k, next)
            })
            .collect();

        // Apply quickly under write lock
        {
            let mut pw = profiles.write();
            for account in &update.accounts {
                let entry = pw.entry(*account).or_insert_with(|| EntityProfile {
                    pubkey: *account,
                    transaction_history: VecDeque::with_capacity(CORRELATION_WINDOW),
                    correlation_matrix: AHashMap::new(),
                    entanglement_state: Self::initialize_quantum_state(),
                    last_update: Instant::now(),
                    activity_score: 0.0,
                });

                entry.transaction_history.push_back(features.clone());
                if entry.transaction_history.len() > CORRELATION_WINDOW { entry.transaction_history.pop_front(); }
                if let Some(new_state) = computed.get(account) { entry.entanglement_state = new_state.clone(); }
                entry.activity_score = Self::calculate_activity_score(&entry.transaction_history);
                entry.last_update = Instant::now();
            }
        }

        if update.accounts.len() >= 2 {
            Self::update_correlations(&update.accounts, profiles.clone(), cache.clone()).await;
            Self::detect_and_update_clusters(profiles.clone(), cache.clone(), clusters.clone()).await;
        }
    }

    fn compute_phase_vector(update: &TransactionUpdate) -> SmallVec<[f64; QUANTUM_PHASE_BINS]> {
        // δ3: branchless with mul_add and finite guard
        let mut out: SmallVec<[f64; QUANTUM_PHASE_BINS]> = SmallVec::with_capacity(QUANTUM_PHASE_BINS);
        let base = (update.timestamp as f64 * 2.0 * PI / 86_400.0) % (2.0 * PI);
        let (sb, cb) = base.sin_cos();
        let v = update.volume.max(0.0).sqrt();
        for i in 0..QUANTUM_PHASE_BINS {
            let (si, ci) = PHASE_OFFSETS[i];
            let x = (sb.mul_add(ci, cb * si) * v).tanh();
            out.push(if x.is_finite() { x } else { 0.0 });
        }
        out
    }

    fn initialize_quantum_state() -> QuantumState {
        let mut amplitude = Vec::with_capacity(QUANTUM_PHASE_BINS);
        let mut phase = Vec::with_capacity(QUANTUM_PHASE_BINS);
        
        for i in 0..QUANTUM_PHASE_BINS {
            let theta = i as f64 * 2.0 * PI / QUANTUM_PHASE_BINS as f64;
            amplitude.push(Complex {
                real: (1.0 / QUANTUM_PHASE_BINS as f64).sqrt() * theta.cos(),
                imag: (1.0 / QUANTUM_PHASE_BINS as f64).sqrt() * theta.sin(),
            });
            phase.push(theta);
        }

        QuantumState {
            amplitude,
            phase,
            coherence_length: 1.0,
            entanglement_degree: 0.0,
        }
    }

    #[inline(always)]
    fn update_quantum_state(
        state: &QuantumState,
        phase_vector: &[f64],
        volume: f64,
    ) -> QuantumState {
        // ε2: use precomputed decay LUT and mul_add; guard NaNs
        let mut out = state.clone();
        let vf = (1.0 + volume.max(0.0)).ln() * 0.1;
        for i in 0..QUANTUM_PHASE_BINS {
            let shift = phase_vector[i] * vf;
            let (sn, cs) = shift.sin_cos();
            let decay = DECAY_LUT[i];

            let r  = state.amplitude[i].real * decay;
            let im = state.amplitude[i].imag * decay;

            let nr = r.mul_add(cs, -im * sn);
            let ni = r.mul_add(sn,  im * cs);

            out.amplitude[i].real = if nr.is_finite() { nr } else { 0.0 };
            out.amplitude[i].imag = if ni.is_finite() { ni } else { 0.0 };
            let np = state.phase[i] + shift;
            out.phase[i] = if np.is_finite() { np % (2.0 * PI) } else { 0.0 };
        }
        out.coherence_length    = Self::calculate_coherence_length(&out.amplitude);
        out.entanglement_degree = Self::calculate_entanglement_degree(&out);
        out
    }

    #[inline(always)]
    fn calculate_coherence_length(amplitude: &[Complex]) -> f64 {
        // ε5: bounds-safe, branchless accumulation
        let len = amplitude.len();
        if len < 2 { return 0.0; }
        let mut acc = 0.0;
        for i in 0..(len - 1) {
            acc += (amplitude[i].real * amplitude[i + 1].real + amplitude[i].imag * amplitude[i + 1].imag).abs();
        }
        acc / (len as f64 - 1.0)
    }

    fn calculate_entanglement_degree(state: &QuantumState) -> f64 {
        let mut entropy = 0.0;
        let mut total_prob = 0.0;

        for amp in &state.amplitude {
            let prob = amp.real * amp.real + amp.imag * amp.imag;
            total_prob += prob;
            if prob > 1e-10 {
                entropy -= prob * prob.ln();
            }
        }

        if total_prob > 0.0 {
            entropy / total_prob.ln()
        } else {
            0.0
        }
    }

    #[inline(always)]
    fn calculate_activity_score(history: &VecDeque<TransactionFeatures>) -> f64 {
        // ε9: numerically safe, branch-pruned
        let n = history.len();
        if n == 0 { return 0.0; }

        let mut vol_w = 0.0;
        let mut suc_w = 0.0;
        for (i, tx) in history.iter().enumerate() {
            let w = ((i as f64 + 1.0) / n as f64).sqrt();
            vol_w += tx.volume.max(0.0) * w;
            if tx.success { suc_w += w; }
        }

        let density = if n > 1 {
            let first = history.front().map(|t| t.timestamp).unwrap_or(0);
            let last  = history.back().map(|t| t.timestamp).unwrap_or(first);
            let span = (last - first).max(1) as f64;
            (n as f64) / span * 86_400.0
        } else { 0.0 };

        let vol_term = vol_w.max(1e-6).ln().max(0.0) * 0.4;
        let suc_term = (suc_w / n as f64) * 0.3;
        let den_term = density.min(100.0) / 100.0 * 0.3;
        vol_term + suc_term + den_term
    }

    // δ5: adaptive EWMA smoother for metrics updates
    const CORR_SMOOTH_ALPHA_BASE: f64 = 0.14;
    const CORR_SMOOTH_ALPHA_MAX:  f64 = 0.32;

    #[inline(always)]
    fn adaptive_alpha(prev: &EntanglementMetrics, newm: &EntanglementMetrics) -> f64 {
        let s_prev = Self::calculate_correlation_strength(prev);
        let s_new  = Self::calculate_correlation_strength(newm);
        let delta  = (s_new - s_prev).abs();
        (CORR_SMOOTH_ALPHA_BASE + delta * 0.6).min(CORR_SMOOTH_ALPHA_MAX).max(0.05)
    }

    #[inline(always)]
    fn sanitize(m: &EntanglementMetrics) -> EntanglementMetrics {
        #[inline(always)]
        fn f(x: f64) -> f64 { if x.is_finite() { x } else { 0.0 } }
        EntanglementMetrics {
            mutual_information:        f(m.mutual_information),
            phase_correlation:         f(m.phase_correlation),
            amplitude_coupling:        f(m.amplitude_coupling),
            temporal_coherence:        f(m.temporal_coherence),
            entanglement_entropy:      f(m.entanglement_entropy),
            bell_inequality_violation: f(m.bell_inequality_violation),
        }
    }

    #[inline(always)]
    fn smooth_metrics(prev: &EntanglementMetrics, newm: &EntanglementMetrics, a: f64) -> EntanglementMetrics {
        let b = 1.0 - a;
        let n = Self::sanitize(newm);
        EntanglementMetrics {
            mutual_information:        b*prev.mutual_information        + a*n.mutual_information,
            phase_correlation:         b*prev.phase_correlation         + a*n.phase_correlation,
            amplitude_coupling:        b*prev.amplitude_coupling        + a*n.amplitude_coupling,
            temporal_coherence:        b*prev.temporal_coherence        + a*n.temporal_coherence,
            entanglement_entropy:      b*prev.entanglement_entropy      + a*n.entanglement_entropy,
            bell_inequality_violation: b*prev.bell_inequality_violation + a*n.bell_inequality_violation,
        }
    }

    async fn update_correlations(
        accounts: &[Pubkey],
        profiles: Arc<RwLock<AHashMap<Pubkey, EntityProfile>>>,
        cache: Arc<RwLock<AHashMap<(Pubkey, Pubkey), EntanglementMetrics>>>,
    ) {
        // α11 + δ5: compute in parallel, then apply with adaptive smoothing under a single write-lock
        if accounts.len() < 2 { return; }

        let snapshot: Vec<(Pubkey, EntityProfile)> = {
            let pr = profiles.read();
            accounts.iter()
                .filter_map(|k| pr.get(k).cloned().map(|p| (*k, p)))
                .collect()
        };
        if snapshot.len() < 2 { return; }

        let pairs: Vec<((Pubkey, Pubkey), EntanglementMetrics)> = snapshot
            .par_iter()
            .enumerate()
            .flat_map(|(i, (ki, pi))| {
                snapshot[i+1..].par_iter().map(move |(kj, pj)| {
                    (ord_pair(ki, kj), Self::calculate_entanglement_metrics(pi, pj))
                })
            })
            .collect();

        let mut cw = cache.write();
        for (k, m_new) in pairs {
            if let Some(prev) = cw.get_mut(&k) {
                let a = Self::adaptive_alpha(prev, &m_new);
                *prev = Self::smooth_metrics(prev, &m_new, a);
            } else {
                cw.insert(k, Self::sanitize(&m_new));
            }
        }
    }

    fn calculate_entanglement_metrics(
        profile1: &EntityProfile,
        profile2: &EntityProfile,
    ) -> EntanglementMetrics {
        let mutual_info = Self::compute_mutual_information(
            &profile1.transaction_history,
            &profile2.transaction_history,
        );

        let phase_corr = Self::compute_phase_correlation(
            &profile1.entanglement_state,
            &profile2.entanglement_state,
        );

        let amplitude_coupling = Self::compute_amplitude_coupling(
            &profile1.entanglement_state,
            &profile2.entanglement_state,
        );

        let temporal_coherence = Self::compute_temporal_coherence(
            &profile1.transaction_history,
            &profile2.transaction_history,
        );

        let entanglement_entropy = Self::compute_entanglement_entropy(
            &profile1.entanglement_state,
            &profile2.entanglement_state,
        );

        let bell_violation = Self::compute_bell_inequality_violation(
            &profile1.entanglement_state,
            &profile2.entanglement_state,
        );

        EntanglementMetrics {
            mutual_information: mutual_info,
            phase_correlation: phase_corr,
            amplitude_coupling,
            temporal_coherence,
            entanglement_entropy,
            bell_inequality_violation: bell_violation,
        }
    }

    // ε3: MI v5 with LUT binning
    const VOL_BINS: usize = 64;
    static VOL_BOUNDS: Lazy<[f64; VOL_BINS + 1]> = Lazy::new(|| {
        let mut e = [0.0_f64; VOL_BINS + 1];
        for i in 0..=VOL_BINS { e[i] = (i as f64) / VOL_BINS as f64; }
        e
    });

    #[inline(always)]
    fn vol_to_bin(v: f64) -> usize {
        let x = (v.max(0.0).ln_1p() * 10.0).floor();
        let b = if x.is_finite() { x as isize } else { 0 };
        b.clamp(0, (VOL_BINS - 1) as isize) as usize
    }

    #[inline(always)]
    fn compute_mutual_information(
        h1: &VecDeque<TransactionFeatures>,
        h2: &VecDeque<TransactionFeatures>,
    ) -> f64 {
        let n = h1.len().min(h2.len());
        if n == 0 { return 0.0; }

        let mut joint = [[0u32; VOL_BINS]; VOL_BINS];
        let mut m1 = [0u32; VOL_BINS];
        let mut m2 = [0u32; VOL_BINS];

        for i in 0..n {
            let b1 = Self::vol_to_bin(h1[i].volume);
            let b2 = Self::vol_to_bin(h2[i].volume);
            joint[b1][b2] = joint[b1][b2].saturating_add(1);
            m1[b1] = m1[b1].saturating_add(1);
            m2[b2] = m2[b2].saturating_add(1);
        }

        let tot = n as f64;
        let mut mi = 0.0;
        for x in 0..VOL_BINS {
            let cx = m1[x];
            if cx == 0 { continue; }
            let px = (cx as f64) / tot;
            for y in 0..VOL_BINS {
                let cxy = joint[x][y];
                if cxy == 0 { continue; }
                let py = (m2[y] as f64) / tot;
                let pxy = (cxy as f64) / tot;
                mi += pxy * (pxy / (px * py)).ln();
            }
        }
        mi.clamp(0.0, 10.0)
    }

    #[inline(always)]
    fn compute_phase_correlation(s1: &QuantumState, s2: &QuantumState) -> f64 {
        // Φ1: Kahan-summed, numerically tight, single-pass
        let mut dot = 0.0;  let mut c_dot = 0.0;
        let mut n1  = 0.0;  let mut c_n1  = 0.0;
        let mut n2  = 0.0;  let mut c_n2  = 0.0;

        for i in 0..QUANTUM_PHASE_BINS {
            let a1 = s1.amplitude[i].mag2();
            let a2 = s2.amplitude[i].mag2();
            let w  = (a1 * a2).sqrt();
            let d  = (s1.phase[i] - s2.phase[i]).cos();

            // Kahan for dot
            let y  = w * d - c_dot;
            let t  = dot + y;
            c_dot  = (t - dot) - y;
            dot    = t;

            // Kahan for n1, n2
            let y1 = a1 - c_n1; let t1 = n1 + y1; c_n1 = (t1 - n1) - y1; n1 = t1;
            let y2 = a2 - c_n2; let t2 = n2 + y2; c_n2 = (t2 - n2) - y2; n2 = t2;
        }

        if n1 > 0.0 && n2 > 0.0 {
            let denom = (n1.sqrt() * n2.sqrt()).max(1e-18);
            let v = (dot / denom).abs();
            if v.is_finite() { v.min(1.0) } else { 0.0 }
        } else { 0.0 }
    }

    #[inline(always)]
    fn compute_amplitude_coupling(state1: &QuantumState, state2: &QuantumState) -> f64 {
        // Φ2: Bhattacharyya-style with compensated averaging
        let mut sum = 0.0; let mut c = 0.0;
        for i in 0..QUANTUM_PHASE_BINS {
            let a1 = state1.amplitude[i].mag2();
            let a2 = state2.amplitude[i].mag2();
            let y  = (a1 * a2).sqrt() - c;
            let t  = sum + y;
            c      = (t - sum) - y;
            sum    = t;
        }
        (sum / QUANTUM_PHASE_BINS as f64).min(1.0)
    }

    #[inline(always)]
    fn compute_temporal_coherence(
        history1: &VecDeque<TransactionFeatures>,
        history2: &VecDeque<TransactionFeatures>,
    ) -> f64 {
        // Φ3: log-ratio kernel, scale-invariant, robust
        let n1 = history1.len();
        let n2 = history2.len();
        if n1 < 2 || n2 < 2 { return 0.0; }
        let w = n1.min(n2);
        if w < 2 { return 0.0; }

        let mut sum = 0.0; let mut c = 0.0; let mut k = 0usize;
        for i in 1..w {
            let dt1 = (history1[i].timestamp - history1[i-1].timestamp) as f64;
            let dt2 = (history2[i].timestamp - history2[i-1].timestamp) as f64;
            if dt1 > 0.0 && dt2 > 0.0 {
                let r = (dt1 / dt2).abs().max(1e-12);
                let s = (-r.ln().abs()).exp();
                let y = s - c; let t = sum + y; c = (t - sum) - y; sum = t; k += 1;
            }
        }
        if k == 0 { 0.0 } else { (sum / k as f64).clamp(0.0, 1.0) }
    }

    // ε6: entropy v5 with thread-local buffer and PSD clip
    thread_local! {
        static RHO_BUF: std::cell::RefCell<DMatrix<f64>> =
            std::cell::RefCell::new(DMatrix::zeros(QUANTUM_PHASE_BINS, QUANTUM_PHASE_BINS));
    }

    #[inline(always)]
    fn compute_entanglement_entropy(s1: &QuantumState, s2: &QuantumState) -> f64 {
        // Φ4: normalized PSD, early exits, tight eigen use
        RHO_BUF.with(|cell| {
            let mut rho_ref = cell.borrow_mut();
            let rho = &mut *rho_ref;

            // Build symmetric, non-negative correlation surrogate
            for i in 0..QUANTUM_PHASE_BINS {
                let a1r = s1.amplitude[i].real; let a1i = s1.amplitude[i].imag;
                for j in 0..QUANTUM_PHASE_BINS {
                    let a2r = s2.amplitude[j].real; let a2i = s2.amplitude[j].imag;
                    rho[(i, j)] = (a1r * a2r + a1i * a2i).abs();
                }
            }
            for i in 0..QUANTUM_PHASE_BINS { for j in (i+1)..QUANTUM_PHASE_BINS {
                let v = 0.5 * (rho[(i, j)] + rho[(j, i)]);
                rho[(i, j)] = v; rho[(j, i)] = v;
            }}

            // Early exits: zero mass or tiny Frobenius norm
            let mut fro2 = 0.0;
            for i in 0..QUANTUM_PHASE_BINS { for j in 0..QUANTUM_PHASE_BINS {
                let v = rho[(i, j)];
                fro2 = v.mul_add(v, fro2);
            }}
            if fro2 <= 1e-24 { return 0.0; }

            // Normalize to trace 1
            let mut tr = 0.0; for i in 0..QUANTUM_PHASE_BINS { tr += rho[(i, i)]; }
            if tr <= 1e-24 { return 0.0; }
            let inv_tr = 1.0 / tr;
            for i in 0..QUANTUM_PHASE_BINS { for j in 0..QUANTUM_PHASE_BINS { rho[(i, j)] *= inv_tr; }}

            // Eigen on a temporary owned copy (nalgebra takes by value)
            let ev = SymmetricEigen::new(rho.clone_owned()).eigenvalues;
            let mut s = 0.0;
            for mut lam in ev.iter().copied() {
                if lam < 0.0 { lam = 0.0; }
                if lam > EIGENVALUE_THRESHOLD { s -= lam * lam.ln(); }
            }
            s.clamp(0.0, (QUANTUM_PHASE_BINS as f64).ln())
        })
    }

    // Φ5: Bell inequality v5 – 1× sincos per bin, angle-addition FMAs
    static BELL_ANGLES: [f64; 4] = [
        0.0,
        std::f64::consts::FRAC_PI_4,
        std::f64::consts::FRAC_PI_2,
        3.0 * std::f64::consts::FRAC_PI_4,
    ];
    const SQRT1_2: f64 = 0.7071067811865476; // 1/sqrt(2)
    static BELL_COS: [f64; 4] = [1.0, SQRT1_2, 0.0, -SQRT1_2];
    static BELL_SIN: [f64; 4] = [0.0, SQRT1_2, 1.0,  SQRT1_2];

    #[inline(always)]
    fn compute_bell_inequality_violation(s1: &QuantumState, s2: &QuantumState) -> f64 {
        let mut e = [0.0f64; 4];
        for i in 0..QUANTUM_PHASE_BINS {
            let dphi = s1.phase[i] - s2.phase[i];
            let (sd, cd) = dphi.sin_cos();
            let w = s1.amplitude[i].magnitude() * s2.amplitude[i].magnitude();

            let c0 = cd;
            let c1 = cd.mul_add(BELL_COS[1], -sd * BELL_SIN[1]);
            let c2 = cd.mul_add(BELL_COS[2], -sd * BELL_SIN[2]);
            let c3 = cd.mul_add(BELL_COS[3], -sd * BELL_SIN[3]);

            e[0] = (w * c0).mul_add(1.0, e[0]);
            e[1] = (w * c1).mul_add(1.0, e[1]);
            e[2] = (w * c2).mul_add(1.0, e[2]);
            e[3] = (w * c3).mul_add(1.0, e[3]);
        }
        let norm = QUANTUM_PHASE_BINS as f64; for k in 0..4 { e[k] /= norm; }
        let s = (e[0] - e[1] + e[2] + e[3]).abs();
        (s - 2.0).max(0.0).min(2.0 * 2.0f64.sqrt() - 2.0)
    }

    async fn detect_and_update_clusters(
        profiles: Arc<RwLock<AHashMap<Pubkey, EntityProfile>>>,
        cache: Arc<RwLock<AHashMap<(Pubkey, Pubkey), EntanglementMetrics>>>,
        clusters: Arc<RwLock<Vec<CorrelationCluster>>>,
    ) {
        let profiles_read = profiles.read();
        let cache_read = cache.read();
        
        let mut adjacency_matrix = AHashMap::new();
        let mut all_entities: Vec<Pubkey> = profiles_read.keys().cloned().collect();
        
        for ((acc1, acc2), metrics) in cache_read.iter() {
            let correlation_strength = Self::calculate_correlation_strength(metrics);
            
            if correlation_strength >= MIN_CORRELATION_THRESHOLD {
                adjacency_matrix.entry(*acc1).or_insert_with(Vec::new).push((*acc2, correlation_strength));
                adjacency_matrix.entry(*acc2).or_insert_with(Vec::new).push((*acc1, correlation_strength));
            }
        }

        drop(cache_read);
        
        let new_clusters = Self::hierarchical_clustering(&adjacency_matrix, &all_entities, &profiles_read);
        
        let mut clusters_write = clusters.write();
        *clusters_write = new_clusters;
    }

    fn calculate_correlation_strength(m: &EntanglementMetrics) -> f64 {
        // δ2: clamped inputs, FMA-style accumulation
        let w = [0.25, 0.20, 0.15, 0.15, 0.15, 0.10];
        let v = [
            (m.mutual_information / 10.0).clamp(0.0, 1.0),
            m.phase_correlation.clamp(0.0, 1.0),
            m.amplitude_coupling.clamp(0.0, 1.0),
            m.temporal_coherence.clamp(0.0, 1.0),
            (m.entanglement_entropy / 5.0).clamp(0.0, 1.0),
            (m.bell_inequality_violation / (2.0 * 2.0f64.sqrt() - 2.0)).clamp(0.0, 1.0),
        ];
        let mut s = 0.0;
        for i in 0..w.len() { s = (w[i] * v[i]).mul_add(1.0, s); }
        s.min(1.0)
    }

    fn hierarchical_clustering(
        adjacency: &AHashMap<Pubkey, Vec<(Pubkey, f64)>>,
        entities: &[Pubkey],
        profiles: &AHashMap<Pubkey, EntityProfile>,
    ) -> Vec<CorrelationCluster> {
        // α12: depth-aware BFS clustering with stable sort
        let mut clusters = Vec::new();
        let mut seen: AHashSet<Pubkey> = AHashSet::with_capacity(entities.len());

        for &seed in entities {
            if seen.contains(&seed) { continue; }
            let mut frontier = vec![(seed, 0usize)];
            let mut members: Vec<Pubkey> = Vec::new();
            let mut strength_sum = 0.0;
            let mut edges = 0usize;

            while let Some((node, depth)) = frontier.pop() {
                if !seen.insert(node) { continue; }
                members.push(node);

                if let Some(neis) = adjacency.get(&node) {
                    for &(nei, w) in neis.iter() {
                        if depth + 1 <= MAX_CORRELATION_DEPTH { frontier.push((nei, depth + 1)); }
                        strength_sum += w; edges += 1;
                    }
                }
            }

            if members.len() >= 2 {
                let avg_strength = if edges > 0 { strength_sum / edges as f64 } else { 0.0 };
                let activity_pattern = Self::analyze_cluster_activity(&members, profiles);
                let profitability = Self::estimate_cluster_profitability(&members, profiles);
                let (core, peripheral) = Self::classify_cluster_members(&members, adjacency);

                clusters.push(CorrelationCluster {
                    core_entities: core,
                    peripheral_entities: peripheral,
                    cluster_strength: avg_strength,
                    activity_pattern,
                    profitability_score: profitability,
                });
            }
        }

        clusters.sort_unstable_by(|a, b| b.profitability_score.partial_cmp(&a.profitability_score).unwrap());
        clusters.truncate(256);
        clusters
    }

    fn analyze_cluster_activity(
        members: &[Pubkey],
        profiles: &AHashMap<Pubkey, EntityProfile>,
    ) -> ActivityPattern {
        let mut timestamps = Vec::new();
        let mut directional_flow = HashMap::new();

        for member in members {
            if let Some(profile) = profiles.get(member) {
                for tx in &profile.transaction_history {
                    timestamps.push(tx.timestamp);
                    
                                        for account in &tx.accounts {
                        if members.contains(account) && account != member {
                            *directional_flow.entry(*account).or_insert(0.0) += tx.volume;
                        }
                    }
                }
            }
        }

        timestamps.sort_unstable();
        
        let periodicity = if timestamps.len() > 2 {
            Self::detect_periodicity(&timestamps)
        } else {
            0.0
        };

        let burst_intensity = Self::calculate_burst_intensity(&timestamps);
        let temporal_signature = Self::generate_temporal_signature(&timestamps);

        ActivityPattern {
            periodicity,
            burst_intensity,
            directional_flow,
            temporal_signature,
        }
    }

    fn detect_periodicity(timestamps: &[i64]) -> f64 {
        if timestamps.len() < 4 {
            return 0.0;
        }

        let mut intervals = Vec::with_capacity(timestamps.len() - 1);
        for i in 1..timestamps.len() {
            intervals.push((timestamps[i] - timestamps[i-1]) as f64);
        }

        let mean_interval = intervals.iter().sum::<f64>() / intervals.len() as f64;
        let mut variance = 0.0;
        
        for interval in &intervals {
            variance += (interval - mean_interval).powi(2);
        }
        
        variance /= intervals.len() as f64;
        let cv = if mean_interval > 0.0 { (variance.sqrt() / mean_interval) } else { f64::INFINITY };
        
        1.0 / (1.0 + cv).min(10.0)
    }

    fn calculate_burst_intensity(timestamps: &[i64]) -> f64 {
        if timestamps.len() < 2 {
            return 0.0;
        }

        let mut max_burst = 0.0;
        let window_size = 300;
        
        for i in 0..timestamps.len() {
            let window_start = timestamps[i];
            let window_end = window_start + window_size;
            let count = timestamps.iter()
                .filter(|&&t| t >= window_start && t < window_end)
                .count();
            
            let burst_rate = count as f64 / window_size as f64;
            max_burst = max_burst.max(burst_rate);
        }

        max_burst * 1000.0
    }

    fn generate_temporal_signature(timestamps: &[i64]) -> Vec<f64> {
        let mut signature = vec![0.0; 24];
        
        for &ts in timestamps {
            let hour = ((ts % 86400) / 3600) as usize;
            if hour < 24 {
                signature[hour] += 1.0;
            }
        }

        let max_val = signature.iter().fold(0.0f64, |a, &b| a.max(b));
        if max_val > 0.0 {
            for val in &mut signature {
                *val /= max_val;
            }
        }

        signature
    }

    fn estimate_cluster_profitability(
        members: &[Pubkey],
        profiles: &AHashMap<Pubkey, EntityProfile>,
    ) -> f64 {
        let mut total_volume = 0.0;
        let mut success_count = 0;
        let mut total_txs = 0;
        let mut activity_scores = Vec::new();

        for member in members {
            if let Some(profile) = profiles.get(member) {
                activity_scores.push(profile.activity_score);
                
                for tx in &profile.transaction_history {
                    total_volume += tx.volume;
                    total_txs += 1;
                    if tx.success {
                        success_count += 1;
                    }
                }
            }
        }

        if total_txs == 0 {
            return 0.0;
        }

        let avg_volume = total_volume / total_txs as f64;
        let success_rate = success_count as f64 / total_txs as f64;
        let avg_activity = activity_scores.iter().sum::<f64>() / activity_scores.len().max(1) as f64;
        
        let volume_score = (1.0 + avg_volume).ln().min(10.0) / 10.0;
        let profitability = volume_score * 0.4 + success_rate * 0.35 + avg_activity * 0.25;
        
        profitability
    }

    fn classify_cluster_members(
        members: &[Pubkey],
        adjacency: &AHashMap<Pubkey, Vec<(Pubkey, f64)>>,
    ) -> (Vec<Pubkey>, Vec<Pubkey>) {
        let mut member_degrees = Vec::new();
        
        for member in members {
            let degree = adjacency.get(member).map(|n| n.len()).unwrap_or(0);
            member_degrees.push((*member, degree));
        }

        member_degrees.sort_by(|a, b| b.1.cmp(&a.1));
        
        let core_threshold = (members.len() as f64 * 0.3).ceil() as usize;
        let core_threshold = core_threshold.max(1).min(members.len());
        
        let core: Vec<Pubkey> = member_degrees.iter()
            .take(core_threshold)
            .map(|(pubkey, _)| *pubkey)
            .collect();
            
        let peripheral: Vec<Pubkey> = member_degrees.iter()
            .skip(core_threshold)
            .map(|(pubkey, _)| *pubkey)
            .collect();

        (core, peripheral)
    }

    pub async fn find_correlated_clusters(
        &self,
        accounts: &[Pubkey],
        volume: f64,
    ) -> Result<Vec<CorrelationCluster>, Box<dyn std::error::Error>> {
        if volume < MIN_TRANSACTION_VOLUME {
            return Ok(vec![]);
        }

        let clusters_read = self.clusters.read();
        let mut relevant_clusters = Vec::new();

        for cluster in clusters_read.iter() {
            let relevance_score = Self::calculate_cluster_relevance(cluster, accounts);
            
            if relevance_score > 0.5 {
                relevant_clusters.push(cluster.clone());
            }
        }

        relevant_clusters.sort_by(|a, b| {
            b.profitability_score.partial_cmp(&a.profitability_score).unwrap()
        });

        Ok(relevant_clusters)
    }

    fn calculate_cluster_relevance(cluster: &CorrelationCluster, accounts: &[Pubkey]) -> f64 {
        let account_set: AHashSet<_> = accounts.iter().cloned().collect();
        let mut core_overlap = 0;
        let mut peripheral_overlap = 0;

        for entity in &cluster.core_entities {
            if account_set.contains(entity) {
                core_overlap += 1;
            }
        }

        for entity in &cluster.peripheral_entities {
            if account_set.contains(entity) {
                peripheral_overlap += 1;
            }
        }

        let core_score = core_overlap as f64 / cluster.core_entities.len().max(1) as f64;
        let peripheral_score = peripheral_overlap as f64 / cluster.peripheral_entities.len().max(1) as f64;
        
        core_score * 0.7 + peripheral_score * 0.3
    }

    #[inline(always)]
    fn extract_accounts(&self, tx: &EncodedConfirmedTransactionWithStatusMeta) -> Vec<Pubkey> {
        // Φ8: de-duped via set, branch-pruned
        let mut set: AHashSet<Pubkey> = AHashSet::with_capacity(64);
        if let Some(meta) = &tx.meta {
            if let Some(OptionSerializer::Some(balances)) = &meta.pre_token_balances {
                for b in balances {
                    if let Some(owner) = &b.owner {
                        if let Ok(pk) = owner.parse::<Pubkey>() { set.insert(pk); }
                    }
                }
            }
            if let Some(OptionSerializer::Some(balances)) = &meta.post_token_balances {
                for b in balances {
                    if let Some(owner) = &b.owner {
                        if let Ok(pk) = owner.parse::<Pubkey>() { set.insert(pk); }
                    }
                }
            }
        }
        set.into_iter().collect()
    }

    fn calculate_volume(&self, tx: &EncodedConfirmedTransactionWithStatusMeta) -> f64 {
        // δ9: exact, efficient volume computation across SOL and SPL with minimal branching
        let mut vol = 0.0;
        if let Some(meta) = &tx.meta {
            if let (Some(pre), Some(post)) = (&meta.pre_balances, &meta.post_balances) {
                for i in 0..pre.len().min(post.len()) {
                    vol += post[i].abs_diff(pre[i]) as f64 / 1e9;
                }
            }
            use std::collections::hash_map::Entry;
            let mut pre_map: AHashMap<(u32, String), (u128, u32)> = AHashMap::with_capacity(64);
            if let Some(OptionSerializer::Some(pre_tb)) = &meta.pre_token_balances {
                for b in pre_tb {
                    if let (Some(ix), Some(a)) = (b.account_index, b.ui_token_amount.amount.as_ref()) {
                        if let Ok(v) = a.parse::<u128>() {
                            pre_map.insert((ix, b.mint.clone()), (v, b.ui_token_amount.decimals as u32));
                        }
                    }
                }
            }
            let mut seen_post: AHashSet<(u32, String)> = AHashSet::with_capacity(pre_map.len().max(16));
            if let Some(OptionSerializer::Some(post_tb)) = &meta.post_token_balances {
                for b in post_tb {
                    if let (Some(ix), Some(a)) = (b.account_index, b.ui_token_amount.amount.as_ref()) {
                        if let Ok(v1) = a.parse::<u128>() {
                            let d1 = b.ui_token_amount.decimals as u32;
                            let k = (ix, b.mint.clone());
                            seen_post.insert(k.clone());
                            match pre_map.entry(k) {
                                Entry::Occupied(mut e) => {
                                    let (v0, d0) = *e.get();
                                    let d = if d0 == d1 { d0 } else { d1 };
                                    let scale = 10_u128.saturating_pow(d);
                                    if scale > 0 { vol += v0.abs_diff(v1) as f64 / scale as f64; }
                                    e.insert((v1, d));
                                }
                                Entry::Vacant(vac) => {
                                    let scale = 10_u128.saturating_pow(d1);
                                    if scale > 0 { vol += v1 as f64 / scale as f64; }
                                    vac.insert((v1, d1));
                                }
                            }
                        }
                    }
                }
            }
            for (k, (v0, d)) in pre_map {
                if !seen_post.contains(&k) {
                    let scale = 10_u128.saturating_pow(d);
                    if scale > 0 { vol += v0 as f64 / scale as f64; }
                }
            }
        }
        vol
    }

    pub async fn get_top_correlations(&self, limit: usize) -> Vec<(Pubkey, Pubkey, EntanglementMetrics)> {
        let cache_read = self.correlation_cache.read().unwrap();
        let mut correlations: Vec<_> = cache_read.iter()
            .map(|((k1, k2), metrics)| (*k1, *k2, metrics.clone()))
            .collect();

        correlations.par_sort_by(|a, b| {
            let strength_a = Self::calculate_correlation_strength(&a.2);
            let strength_b = Self::calculate_correlation_strength(&b.2);
            strength_b.partial_cmp(&strength_a).unwrap()
        });

        correlations.truncate(limit);
        correlations
    }

    pub async fn prune_inactive_entities(&self, max_age: Duration) {
        let mut profiles_write = self.entity_profiles.write().unwrap();
        let now = Instant::now();
        
        profiles_write.retain(|_, profile| {
            now.duration_since(profile.last_update) < max_age
        });

        let active_entities: AHashSet<_> = profiles_write.keys().cloned().collect();
        drop(profiles_write);

        let mut cache_write = self.correlation_cache.write().unwrap();
        cache_write.retain(|(k1, k2), _| {
            active_entities.contains(k1) && active_entities.contains(k2)
        });
    }

    pub fn get_cluster_recommendation(&self, target_accounts: &[Pubkey]) -> Option<CorrelationCluster> {
        let clusters_read = self.clusters.read().unwrap();
        
        clusters_read.iter()
            .filter(|cluster| cluster.profitability_score > 0.7)
            .max_by(|a, b| {
                let rel_a = Self::calculate_cluster_relevance(a, target_accounts);
                let rel_b = Self::calculate_cluster_relevance(b, target_accounts);
                rel_a.partial_cmp(&rel_b).unwrap()
            })
            .cloned()
    }
}

impl Complex {
    fn magnitude(&self) -> f64 {
        (self.real.powi(2) + self.imag.powi(2)).sqrt()
    }
}
