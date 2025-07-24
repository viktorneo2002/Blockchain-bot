use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    commitment_config::CommitmentConfig,
};
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta,
    UiTransactionEncoding,
    option_serializer::OptionSerializer,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
    f64::consts::PI,
};
use tokio::sync::mpsc;
use rayon::prelude::*;
use ahash::{AHashMap, AHashSet};
use nalgebra::{DMatrix, DVector};
use statrs::distribution::{ContinuousCDF, Normal};

const CORRELATION_WINDOW: usize = 512;
const MIN_CORRELATION_THRESHOLD: f64 = 0.73;
const ENTANGLEMENT_DECAY_FACTOR: f64 = 0.94;
const MAX_TRACKED_ENTITIES: usize = 4096;
const CORRELATION_UPDATE_INTERVAL: Duration = Duration::from_millis(250);
const QUANTUM_PHASE_BINS: usize = 64;
const MIN_TRANSACTION_VOLUME: f64 = 0.1;
const EIGENVALUE_THRESHOLD: f64 = 0.01;
const MAX_CORRELATION_DEPTH: usize = 5;

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
    pub correlation_matrix: HashMap<Pubkey, f64>,
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

#[derive(Debug, Clone)]
pub struct Complex {
    pub real: f64,
    pub imag: f64,
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
        let rpc_client = Arc::new(RpcClient::new_with_timeout(
            rpc_endpoint.to_string(),
            Duration::from_secs(30),
        ));
        
        let finder = Self {
            rpc_client: rpc_client.clone(),
            entity_profiles: Arc::new(RwLock::new(AHashMap::with_capacity(MAX_TRACKED_ENTITIES))),
            correlation_cache: Arc::new(RwLock::new(AHashMap::with_capacity(MAX_TRACKED_ENTITIES * 10))),
            clusters: Arc::new(RwLock::new(Vec::with_capacity(256))),
            phase_distribution: Arc::new(RwLock::new(DMatrix::zeros(QUANTUM_PHASE_BINS, QUANTUM_PHASE_BINS))),
            eigenspace: Arc::new(RwLock::new(None)),
            update_channel: tx,
        };

        let profiles = finder.entity_profiles.clone();
        let cache = finder.correlation_cache.clone();
        let clusters = finder.clusters.clone();
        
        tokio::spawn(async move {
            while let Some(update) = rx.recv().await {
                Self::process_transaction_update(
                    update,
                    profiles.clone(),
                    cache.clone(),
                    clusters.clone(),
                ).await;
            }
        });

        finder
    }

    pub async fn analyze_transaction(&self, signature: &Signature) -> Result<Vec<CorrelationCluster>, Box<dyn std::error::Error>> {
        let tx = self.rpc_client.get_transaction(
            signature,
            UiTransactionEncoding::Base64,
        )?;

        if let Some(tx_meta) = tx.transaction {
            let accounts = self.extract_accounts(&tx_meta);
            let volume = self.calculate_volume(&tx_meta);
            let timestamp = tx.block_time.unwrap_or(0);
            
            let update = TransactionUpdate {
                signature: *signature,
                accounts: accounts.clone(),
                volume,
                timestamp,
                success: tx_meta.meta.as_ref().map(|m| m.err.is_none()).unwrap_or(false),
            };

            self.update_channel.send(update).await?;
            
            self.find_correlated_clusters(&accounts, volume).await
        } else {
            Ok(vec![])
        }
    }

    async fn process_transaction_update(
        update: TransactionUpdate,
        profiles: Arc<RwLock<AHashMap<Pubkey, EntityProfile>>>,
        cache: Arc<RwLock<AHashMap<(Pubkey, Pubkey), EntanglementMetrics>>>,
        clusters: Arc<RwLock<Vec<CorrelationCluster>>>,
    ) {
        let phase_vector = Self::compute_phase_vector(&update);
        let features = TransactionFeatures {
            signature: update.signature,
            timestamp: update.timestamp,
            volume: update.volume,
            accounts: update.accounts.clone(),
            instruction_count: update.accounts.len(),
            success: update.success,
            gas_used: 5000,
            phase_vector: phase_vector.clone(),
        };

        let mut profiles_write = profiles.write().unwrap();
        
        for account in &update.accounts {
            let profile = profiles_write.entry(*account).or_insert_with(|| {
                EntityProfile {
                    pubkey: *account,
                    transaction_history: VecDeque::with_capacity(CORRELATION_WINDOW),
                    correlation_matrix: HashMap::new(),
                    entanglement_state: Self::initialize_quantum_state(),
                    last_update: Instant::now(),
                    activity_score: 0.0,
                }
            });

            profile.transaction_history.push_back(features.clone());
            if profile.transaction_history.len() > CORRELATION_WINDOW {
                profile.transaction_history.pop_front();
            }

            profile.entanglement_state = Self::update_quantum_state(
                &profile.entanglement_state,
                &phase_vector,
                update.volume,
            );
            
            profile.activity_score = Self::calculate_activity_score(&profile.transaction_history);
            profile.last_update = Instant::now();
        }

        drop(profiles_write);

        if update.accounts.len() >= 2 {
            Self::update_correlations(
                &update.accounts,
                profiles.clone(),
                cache.clone(),
            ).await;

            Self::detect_and_update_clusters(
                profiles.clone(),
                cache.clone(),
                clusters.clone(),
            ).await;
        }
    }

    fn compute_phase_vector(update: &TransactionUpdate) -> Vec<f64> {
        let mut phase = vec![0.0; QUANTUM_PHASE_BINS];
        let base_phase = (update.timestamp as f64 * 2.0 * PI / 86400.0) % (2.0 * PI);
        
        for i in 0..QUANTUM_PHASE_BINS {
            let bin_phase = base_phase + (i as f64 * 2.0 * PI / QUANTUM_PHASE_BINS as f64);
            phase[i] = (bin_phase.sin() * update.volume.sqrt()).tanh();
        }

        phase
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

    fn update_quantum_state(
        state: &QuantumState,
        phase_vector: &[f64],
        volume: f64,
    ) -> QuantumState {
        let mut new_state = state.clone();
        let volume_factor = (1.0 + volume).ln() / 10.0;

        for i in 0..QUANTUM_PHASE_BINS {
            let phase_shift = phase_vector[i] * volume_factor;
            let decay = (-ENTANGLEMENT_DECAY_FACTOR * (i as f64 / QUANTUM_PHASE_BINS as f64)).exp();
            
            new_state.amplitude[i].real = state.amplitude[i].real * decay * phase_shift.cos()
                - state.amplitude[i].imag * decay * phase_shift.sin();
            new_state.amplitude[i].imag = state.amplitude[i].real * decay * phase_shift.sin()
                + state.amplitude[i].imag * decay * phase_shift.cos();
            
            new_state.phase[i] = (state.phase[i] + phase_shift) % (2.0 * PI);
        }

        new_state.coherence_length = Self::calculate_coherence_length(&new_state.amplitude);
        new_state.entanglement_degree = Self::calculate_entanglement_degree(&new_state);

        new_state
    }

    fn calculate_coherence_length(amplitude: &[Complex]) -> f64 {
        let mut sum = 0.0;
        for i in 0..amplitude.len() - 1 {
            let correlation = amplitude[i].real * amplitude[i + 1].real
                + amplitude[i].imag * amplitude[i + 1].imag;
            sum += correlation.abs();
        }
        sum / (amplitude.len() - 1) as f64
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

    fn calculate_activity_score(history: &VecDeque<TransactionFeatures>) -> f64 {
        if history.is_empty() {
            return 0.0;
        }

        let mut score = 0.0;
        let mut volume_sum = 0.0;
        let mut success_rate = 0.0;
        let mut temporal_density = 0.0;

        for (i, tx) in history.iter().enumerate() {
            let recency_weight = ((i + 1) as f64 / history.len() as f64).sqrt();
            volume_sum += tx.volume * recency_weight;
            if tx.success {
                success_rate += recency_weight;
            }
        }

        if history.len() > 1 {
            let time_span = history.back().unwrap().timestamp - history.front().unwrap().timestamp;
            if time_span > 0 {
                temporal_density = history.len() as f64 / time_span as f64 * 86400.0;
            }
        }

                score = volume_sum.ln().max(0.0) * 0.4
            + (success_rate / history.len() as f64) * 0.3
            + temporal_density.min(100.0) / 100.0 * 0.3;

        score
    }

    async fn update_correlations(
        accounts: &[Pubkey],
        profiles: Arc<RwLock<AHashMap<Pubkey, EntityProfile>>>,
        cache: Arc<RwLock<AHashMap<(Pubkey, Pubkey), EntanglementMetrics>>>,
    ) {
        let profiles_read = profiles.read().unwrap();
        let mut cache_write = cache.write().unwrap();

        for i in 0..accounts.len() {
            for j in i + 1..accounts.len() {
                let acc1 = &accounts[i];
                let acc2 = &accounts[j];

                if let (Some(profile1), Some(profile2)) = (profiles_read.get(acc1), profiles_read.get(acc2)) {
                    let metrics = Self::calculate_entanglement_metrics(profile1, profile2);
                    
                    let key = if acc1 < acc2 { (*acc1, *acc2) } else { (*acc2, *acc1) };
                    cache_write.insert(key, metrics);
                }
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

    fn compute_mutual_information(
        history1: &VecDeque<TransactionFeatures>,
        history2: &VecDeque<TransactionFeatures>,
    ) -> f64 {
        if history1.is_empty() || history2.is_empty() {
            return 0.0;
        }

        let mut joint_prob = AHashMap::new();
        let mut prob1 = AHashMap::new();
        let mut prob2 = AHashMap::new();
        let mut total = 0.0;

        let window = history1.len().min(history2.len());
        
        for i in 0..window {
            let vol1_bin = (history1[i].volume.ln().max(0.0) * 10.0) as i32;
            let vol2_bin = (history2[i].volume.ln().max(0.0) * 10.0) as i32;
            
            *joint_prob.entry((vol1_bin, vol2_bin)).or_insert(0.0) += 1.0;
            *prob1.entry(vol1_bin).or_insert(0.0) += 1.0;
            *prob2.entry(vol2_bin).or_insert(0.0) += 1.0;
            total += 1.0;
        }

        let mut mi = 0.0;
        for ((x, y), joint_count) in &joint_prob {
            let p_xy = joint_count / total;
            let p_x = prob1.get(x).unwrap() / total;
            let p_y = prob2.get(y).unwrap() / total;
            
            if p_xy > 0.0 && p_x > 0.0 && p_y > 0.0 {
                mi += p_xy * (p_xy / (p_x * p_y)).ln();
            }
        }

        mi.max(0.0).min(10.0)
    }

    fn compute_phase_correlation(
        state1: &QuantumState,
        state2: &QuantumState,
    ) -> f64 {
        let mut correlation = 0.0;
        let mut norm1 = 0.0;
        let mut norm2 = 0.0;

        for i in 0..QUANTUM_PHASE_BINS {
            let phase_diff = (state1.phase[i] - state2.phase[i]).cos();
            let amp1 = (state1.amplitude[i].real.powi(2) + state1.amplitude[i].imag.powi(2)).sqrt();
            let amp2 = (state2.amplitude[i].real.powi(2) + state2.amplitude[i].imag.powi(2)).sqrt();
            
            correlation += amp1 * amp2 * phase_diff;
            norm1 += amp1.powi(2);
            norm2 += amp2.powi(2);
        }

        if norm1 > 0.0 && norm2 > 0.0 {
            (correlation / (norm1.sqrt() * norm2.sqrt())).abs()
        } else {
            0.0
        }
    }

    fn compute_amplitude_coupling(
        state1: &QuantumState,
        state2: &QuantumState,
    ) -> f64 {
        let mut coupling = 0.0;

        for i in 0..QUANTUM_PHASE_BINS {
            let amp1 = state1.amplitude[i].real.powi(2) + state1.amplitude[i].imag.powi(2);
            let amp2 = state2.amplitude[i].real.powi(2) + state2.amplitude[i].imag.powi(2);
            
            coupling += (amp1 * amp2).sqrt();
        }

        coupling / QUANTUM_PHASE_BINS as f64
    }

    fn compute_temporal_coherence(
        history1: &VecDeque<TransactionFeatures>,
        history2: &VecDeque<TransactionFeatures>,
    ) -> f64 {
        if history1.len() < 2 || history2.len() < 2 {
            return 0.0;
        }

        let mut coherence = 0.0;
        let window = history1.len().min(history2.len());
        
        for i in 1..window {
            let dt1 = (history1[i].timestamp - history1[i-1].timestamp) as f64;
            let dt2 = (history2[i].timestamp - history2[i-1].timestamp) as f64;
            
            if dt1 > 0.0 && dt2 > 0.0 {
                let ratio = (dt1 / dt2).min(dt2 / dt1);
                coherence += ratio;
            }
        }

        coherence / (window - 1) as f64
    }

    fn compute_entanglement_entropy(
        state1: &QuantumState,
        state2: &QuantumState,
    ) -> f64 {
        let mut density_matrix = DMatrix::zeros(QUANTUM_PHASE_BINS, QUANTUM_PHASE_BINS);
        
        for i in 0..QUANTUM_PHASE_BINS {
            for j in 0..QUANTUM_PHASE_BINS {
                let psi1_i = Complex {
                    real: state1.amplitude[i].real,
                    imag: state1.amplitude[i].imag,
                };
                let psi2_j = Complex {
                    real: state2.amplitude[j].real,
                    imag: state2.amplitude[j].imag,
                };
                
                density_matrix[(i, j)] = (psi1_i.real * psi2_j.real + psi1_i.imag * psi2_j.imag).abs();
            }
        }

        let eigenvalues = density_matrix.symmetric_eigenvalues();
        let mut entropy = 0.0;
        
        for &lambda in eigenvalues.iter() {
            if lambda > EIGENVALUE_THRESHOLD {
                entropy -= lambda * lambda.ln();
            }
        }

        entropy.max(0.0)
    }

    fn compute_bell_inequality_violation(
        state1: &QuantumState,
        state2: &QuantumState,
    ) -> f64 {
        let mut e_values = vec![0.0; 4];
        let angles = [0.0, PI / 4.0, PI / 2.0, 3.0 * PI / 4.0];
        
        for (idx, &theta) in angles.iter().enumerate() {
            let mut correlation = 0.0;
            
            for i in 0..QUANTUM_PHASE_BINS {
                let phase1 = state1.phase[i];
                let phase2 = state2.phase[i];
                let amp1 = (state1.amplitude[i].real.powi(2) + state1.amplitude[i].imag.powi(2)).sqrt();
                let amp2 = (state2.amplitude[i].real.powi(2) + state2.amplitude[i].imag.powi(2)).sqrt();
                
                correlation += amp1 * amp2 * (phase1 - phase2 + theta).cos();
            }
            
            e_values[idx] = correlation / QUANTUM_PHASE_BINS as f64;
        }

        let s = (e_values[0] - e_values[1] + e_values[2] + e_values[3]).abs();
        (s - 2.0).max(0.0).min(2.0 * 2.0f64.sqrt() - 2.0)
    }

    async fn detect_and_update_clusters(
        profiles: Arc<RwLock<AHashMap<Pubkey, EntityProfile>>>,
        cache: Arc<RwLock<AHashMap<(Pubkey, Pubkey), EntanglementMetrics>>>,
        clusters: Arc<RwLock<Vec<CorrelationCluster>>>,
    ) {
        let profiles_read = profiles.read().unwrap();
        let cache_read = cache.read().unwrap();
        
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
        
        let mut clusters_write = clusters.write().unwrap();
        *clusters_write = new_clusters;
    }

    fn calculate_correlation_strength(metrics: &EntanglementMetrics) -> f64 {
        let weights = [0.25, 0.20, 0.15, 0.15, 0.15, 0.10];
        let values = [
            metrics.mutual_information / 10.0,
            metrics.phase_correlation,
            metrics.amplitude_coupling,
            metrics.temporal_coherence,
            metrics.entanglement_entropy / 5.0,
            metrics.bell_inequality_violation / (2.0 * 2.0f64.sqrt() - 2.0),
        ];

        weights.iter().zip(values.iter()).map(|(w, v)| w * v).sum::<f64>().min(1.0)
    }

    fn hierarchical_clustering(
        adjacency: &AHashMap<Pubkey, Vec<(Pubkey, f64)>>,
        entities: &[Pubkey],
        profiles: &AHashMap<Pubkey, EntityProfile>,
    ) -> Vec<CorrelationCluster> {
        let mut clusters = Vec::new();
        let mut visited = AHashSet::new();

        for entity in entities {
            if visited.contains(entity) {
                continue;
            }

            let mut cluster_members = Vec::new();
            let mut to_visit = vec![*entity];
            let mut cluster_strength = 0.0;
            let mut edge_count = 0;

            while let Some(current) = to_visit.pop() {
                if visited.contains(&current) {
                    continue;
                }

                visited.insert(current);
                cluster_members.push(current);

                if let Some(neighbors) = adjacency.get(&current) {
                    for (neighbor, strength) in neighbors {
                        if !visited.contains(neighbor) && cluster_members.len() < MAX_CORRELATION_DEPTH {
                            to_visit.push(*neighbor);
                            cluster_strength += strength;
                            edge_count += 1;
                        }
                    }
                }
            }

            if cluster_members.len() >= 2 {
                let avg_strength = if edge_count > 0 { cluster_strength / edge_count as f64 } else { 0.0 };
                
                let activity_pattern = Self::analyze_cluster_activity(&cluster_members, profiles);
                let profitability = Self::estimate_cluster_profitability(&cluster_members, profiles);

                let (core, peripheral) = Self::classify_cluster_members(&cluster_members, adjacency);

                clusters.push(CorrelationCluster {
                    core_entities: core,
                    peripheral_entities: peripheral,
                    cluster_strength: avg_strength,
                    activity_pattern,
                    profitability_score: profitability,
                });
            }
        }

        clusters.sort_by(|a, b| b.profitability_score.partial_cmp(&a.profitability_score).unwrap());
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

        let clusters_read = self.clusters.read().unwrap();
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

    fn extract_accounts(&self, tx: &EncodedConfirmedTransactionWithStatusMeta) -> Vec<Pubkey> {
        let mut accounts = Vec::new();
        
        if let Some(meta) = &tx.meta {
            if let Some(pre_token_balances) = &meta.pre_token_balances {
                if let OptionSerializer::Some(balances) = pre_token_balances {
                    for balance in balances {
                        if let Some(owner) = &balance.owner {
                            if let Ok(pubkey) = owner.parse::<Pubkey>() {
                                accounts.push(pubkey);
                            }
                        }
                    }
                }
            }

            if let Some(post_token_balances) = &meta.post_token_balances {
                if let OptionSerializer::Some(balances) = post_token_balances {
                    for balance in balances {
                        if let Some(owner) = &balance.owner {
                            if let Ok(pubkey) = owner.parse::<Pubkey>() {
                                if !accounts.contains(&pubkey) {
                                    accounts.push(pubkey);
                                }
                            }
                        }
                    }
                }
            }
        }

        accounts
    }

    fn calculate_volume(&self, tx: &EncodedConfirmedTransactionWithStatusMeta) -> f64 {
        let mut volume = 0.0;

        if let Some(meta) = &tx.meta {
            if let (Some(pre_balances), Some(post_balances)) = (&meta.pre_balances, &meta.post_balances) {
                for i in 0..pre_balances.len().min(post_balances.len()) {
                    let diff = (post_balances[i] as i64 - pre_balances[i] as i64).abs();
                    volume += diff as f64 / 1e9;
                }
            }

            if let Some(pre_token_balances) = &meta.pre_token_balances {
                if let OptionSerializer::Some(pre_balances) = pre_token_balances {
                    if let Some(post_token_balances) = &meta.post_token_balances {
                        if let OptionSerializer::Some(post_balances) = post_token_balances {
                            for i in 0..pre_balances.len().min(post_balances.len()) {
                                if let (Some(pre_amount), Some(post_amount)) = 
                                    (&pre_balances[i].ui_token_amount.amount, &post_balances[i].ui_token_amount.amount) {
                                    if let (Ok(pre_val), Ok(post_val)) = (pre_amount.parse::<f64>(), post_amount.parse::<f64>()) {
                                        let decimals = pre_balances[i].ui_token_amount.decimals as f64;
                                        let diff = (post_val - pre_val).abs() / 10f64.powf(decimals);
                                        volume += diff;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        volume
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

