use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use solana_sdk::{
    pubkey::Pubkey,
    signature::Signature,
    transaction::Transaction,
    instruction::CompiledInstruction,
    hash::{Hash, hashv},
    message::Message,
};
use dashmap::DashMap;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

const MAX_BUNDLE_SIZE: usize = 5;
const COLLISION_WINDOW_MS: u64 = 400;
const MAX_TRACKED_BUNDLES: usize = 10000;
const ACCOUNT_LOCK_DECAY_MS: u64 = 150;
const COLLISION_SCORE_THRESHOLD: f64 = 0.75;
const MAX_ACCOUNT_HISTORY: usize = 1000;
const PREDICTION_LOOKAHEAD_MS: u64 = 200;
const SLOT_DURATION_MS: u64 = 400;
const MAX_METRICS_CACHE: usize = 50000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BundleCollisionMetrics {
    pub collision_probability: f64,
    pub account_overlap_score: f64,
    pub program_conflict_score: f64,
    pub timing_conflict_score: f64,
    pub priority_fee_competition: f64,
    pub recommended_delay_ms: u64,
    pub slot_conflict_probability: f64,
}

#[derive(Debug, Clone)]
pub struct TrackedBundle {
    pub id: Hash,
    pub transactions: Vec<Transaction>,
    pub accounts: HashSet<Pubkey>,
    pub write_accounts: HashSet<Pubkey>,
    pub programs: HashSet<Pubkey>,
    pub priority_fee: u64,
    pub submission_time: Instant,
    pub predicted_execution: Instant,
    pub estimated_cu: u64,
    pub slot_target: u64,
}

#[derive(Debug, Clone)]
struct AccountLockInfo {
    pub writer: Option<Hash>,
    pub readers: HashSet<Hash>,
    pub last_access: Instant,
    pub access_frequency: f64,
    pub contention_score: f64,
    pub write_lock_count: u64,
    pub read_lock_count: u64,
}

#[derive(Debug, Clone)]
struct ProgramInteraction {
    pub program_id: Pubkey,
    pub interaction_count: u64,
    pub avg_execution_time_ms: f64,
    pub conflict_patterns: HashMap<Pubkey, f64>,
    pub cu_consumption: u64,
    pub success_rate: f64,
}

pub struct BundleCollisionPredictor {
    tracked_bundles: Arc<DashMap<Hash, TrackedBundle>>,
    account_locks: Arc<DashMap<Pubkey, AccountLockInfo>>,
    program_interactions: Arc<DashMap<Pubkey, ProgramInteraction>>,
    collision_history: Arc<RwLock<VecDeque<CollisionEvent>>>,
    metrics_cache: Arc<DashMap<(Hash, Hash), (Instant, BundleCollisionMetrics)>>,
    submission_patterns: Arc<RwLock<SubmissionPatternAnalyzer>>,
    slot_tracker: Arc<RwLock<SlotTracker>>,
    account_access_patterns: Arc<DashMap<Pubkey, AccessPattern>>,
}

#[derive(Debug, Clone)]
struct CollisionEvent {
    bundle_a: Hash,
    bundle_b: Hash,
    timestamp: Instant,
    collision_type: CollisionType,
    severity: f64,
    slot: u64,
}

#[derive(Debug, Clone, PartialEq)]
enum CollisionType {
    AccountWrite,
    AccountReadWrite,
    ProgramConflict,
    TimingOverlap,
    SlotConflict,
}

#[derive(Debug, Clone)]
struct SubmissionPatternAnalyzer {
    time_buckets: HashMap<u64, Vec<f64>>,
    congestion_scores: VecDeque<(Instant, f64)>,
    optimal_windows: Vec<(u64, u64)>,
    slot_success_rates: HashMap<u64, f64>,
}

#[derive(Debug, Clone)]
struct SlotTracker {
    current_slot: u64,
    slot_start_time: Instant,
    bundles_per_slot: HashMap<u64, Vec<Hash>>,
    slot_collision_rates: HashMap<u64, f64>,
}

#[derive(Debug, Clone)]
struct AccessPattern {
    access_times: VecDeque<Instant>,
    access_intervals: Vec<Duration>,
    peak_times: Vec<u64>,
    avg_interval_ms: f64,
}

impl BundleCollisionPredictor {
    pub fn new() -> Self {
        Self {
            tracked_bundles: Arc::new(DashMap::with_capacity(MAX_TRACKED_BUNDLES)),
            account_locks: Arc::new(DashMap::with_capacity(100000)),
            program_interactions: Arc::new(DashMap::new()),
            collision_history: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_ACCOUNT_HISTORY))),
            metrics_cache: Arc::new(DashMap::with_capacity(MAX_METRICS_CACHE)),
            submission_patterns: Arc::new(RwLock::new(SubmissionPatternAnalyzer::new())),
            slot_tracker: Arc::new(RwLock::new(SlotTracker::new())),
            account_access_patterns: Arc::new(DashMap::new()),
        }
    }

    pub async fn track_bundle(&self, bundle: Vec<Transaction>, slot: u64) -> Hash {
        let bundle_id = self.generate_bundle_id(&bundle);
        let (accounts, write_accounts, programs) = self.extract_bundle_resources(&bundle);
        let estimated_cu = self.estimate_compute_units(&bundle);
        
        let tracked = TrackedBundle {
            id: bundle_id,
            transactions: bundle.clone(),
            accounts: accounts.clone(),
            write_accounts: write_accounts.clone(),
            programs,
            priority_fee: self.calculate_bundle_priority_fee(&bundle),
            submission_time: Instant::now(),
            predicted_execution: Instant::now() + Duration::from_millis(PREDICTION_LOOKAHEAD_MS),
            estimated_cu,
            slot_target: slot,
        };

        self.tracked_bundles.insert(bundle_id, tracked);
        self.update_account_locks(&bundle_id, &accounts, &write_accounts);
        self.update_access_patterns(&accounts);
        self.update_slot_tracking(bundle_id, slot);
        self.cleanup_old_data();
        
        bundle_id
    }

    pub async fn predict_collision(&self, bundle_a: &Hash, bundle_b: &Hash) -> BundleCollisionMetrics {
        let cache_key = if bundle_a < bundle_b {
            (*bundle_a, *bundle_b)
        } else {
            (*bundle_b, *bundle_a)
        };

        if let Some(entry) = self.metrics_cache.get(&cache_key) {
            let (cached_time, metrics) = entry.value();
            if cached_time.elapsed() < Duration::from_millis(50) {
                return metrics.clone();
            }
        }

        let metrics = match (self.tracked_bundles.get(bundle_a), self.tracked_bundles.get(bundle_b)) {
            (Some(a), Some(b)) => self.calculate_collision_metrics(a.value(), b.value()),
            _ => BundleCollisionMetrics {
                collision_probability: 0.0,
                account_overlap_score: 0.0,
                program_conflict_score: 0.0,
                timing_conflict_score: 0.0,
                priority_fee_competition: 0.0,
                recommended_delay_ms: 0,
                slot_conflict_probability: 0.0,
            },
        };

        self.metrics_cache.insert(cache_key, (Instant::now(), metrics.clone()));
        
        if self.metrics_cache.len() > MAX_METRICS_CACHE {
            self.cleanup_metrics_cache();
        }

        metrics
    }

    pub async fn find_optimal_submission_window(&self, bundle_id: &Hash) -> (Instant, f64) {
        let bundle = match self.tracked_bundles.get(bundle_id) {
            Some(b) => b,
            None => return (Instant::now(), 0.0),
        };

        let mut best_window = Instant::now();
        let mut lowest_collision_score = f64::MAX;
        let mut best_confidence = 0.0;

        let patterns = self.submission_patterns.read().unwrap();
        let slot_info = self.slot_tracker.read().unwrap();
        let current_congestion = self.calculate_current_congestion(&patterns);
        
        for offset_ms in (0..=PREDICTION_LOOKAHEAD_MS).step_by(10) {
            let test_time = Instant::now() + Duration::from_millis(offset_ms);
            let predicted_slot = self.predict_slot_at_time(test_time, &slot_info);
            
            let collision_score = self.calculate_time_window_collision_score(
                &bundle,
                test_time,
                current_congestion,
                predicted_slot
            );

            if collision_score < lowest_collision_score {
                lowest_collision_score = collision_score;
                best_window = test_time;
                best_confidence = 1.0 - collision_score;
            }
        }

        (best_window, best_confidence)
    }

    fn calculate_collision_metrics(&self, bundle_a: &TrackedBundle, bundle_b: &TrackedBundle) -> BundleCollisionMetrics {
        let account_overlap = self.calculate_account_overlap_score(bundle_a, bundle_b);
        let program_conflict = self.calculate_program_conflict_score(bundle_a, bundle_b);
        let timing_conflict = self.calculate_timing_conflict_score(bundle_a, bundle_b);
        let priority_competition = self.calculate_priority_competition(bundle_a, bundle_b);
        let slot_conflict = self.calculate_slot_conflict_probability(bundle_a, bundle_b);

        let collision_probability = 
            account_overlap * 0.35 + 
            program_conflict * 0.20 + 
            timing_conflict * 0.15 + 
            priority_competition * 0.15 +
            slot_conflict * 0.15;

        let recommended_delay = if collision_probability > COLLISION_SCORE_THRESHOLD {
            let base_delay = ((collision_probability - COLLISION_SCORE_THRESHOLD) * 200.0) as u64;
            let slot_adjusted = if bundle_a.slot_target == bundle_b.slot_target {
                base_delay + 50
            } else {
                base_delay
            };
            slot_adjusted.min(COLLISION_WINDOW_MS)
        } else {
            0
        };

        BundleCollisionMetrics {
            collision_probability: collision_probability.min(1.0),
            account_overlap_score: account_overlap,
            program_conflict_score: program_conflict,
            timing_conflict_score: timing_conflict,
            priority_fee_competition: priority_competition,
            recommended_delay_ms: recommended_delay,
            slot_conflict_probability: slot_conflict,
        }
    }

    fn calculate_account_overlap_score(&self, bundle_a: &TrackedBundle, bundle_b: &TrackedBundle) -> f64 {
        let write_write_conflicts = bundle_a.write_accounts.intersection(&bundle_b.write_accounts).count();
        let write_read_conflicts = bundle_a.write_accounts.intersection(&bundle_b.accounts).count() +
                                  bundle_b.write_accounts.intersection(&bundle_a.accounts).count();

        let total_unique_accounts = bundle_a.accounts.union(&bundle_b.accounts).count();
        if total_unique_accounts == 0 {
            return 0.0;
        }

        let base_score = (write_write_conflicts as f64 * 3.0 + write_read_conflicts as f64) / 
                        (total_unique_accounts as f64 * 2.0);
        
        let mut contention_multiplier = 1.0;
        for account in bundle_a.write_accounts.intersection(&bundle_b.write_accounts) {
            if let Some(pattern) = self.account_access_patterns.get(account) {
                let pattern_score = pattern.avg_interval_ms / SLOT_DURATION_MS as f64;
                contention_multiplier += (1.0 - pattern_score.min(1.0)) * 0.5;
            }
        }

        (base_score * contention_multiplier).min(1.0)
    }

    fn calculate_program_conflict_score(&self, bundle_a: &TrackedBundle, bundle_b: &TrackedBundle) -> f64 {
        let mut conflict_score = 0.0;
        let mut weight_sum = 0.0;

        for prog_a in &bundle_a.programs {
            for prog_b in &bundle_b.programs {
                let weight = 1.0;
                weight_sum += weight;

                if prog_a == prog_b {
                    conflict_score += weight * 0.4;
                } else if let Some(interaction) = self.program_interactions.get(prog_a) {
                    if let Some(conflict_rate) = interaction.conflict_patterns.get(prog_b) {
                        conflict_score += weight * conflict_rate;
                    } else {
                        conflict_score += weight * 0.1;
                    }
                } else {
                    conflict_score += weight * 0.05;
                }
            }
        }

        if weight_sum > 0.0 {
            (conflict_score / weight_sum).min(1.0)
        } else {
            0.0
        }
    }

    fn calculate_timing_conflict_score(&self, bundle_a: &TrackedBundle, bundle_b: &TrackedBundle) -> f64 {
        let time_diff = if bundle_a.predicted_execution > bundle_b.predicted_execution {
            bundle_a.predicted_execution.duration_since(bundle_b.predicted_execution)
        } else {
            bundle_b.predicted_execution.duration_since(bundle_a.predicted_execution)
        }.as_millis() as f64;

        if time_diff > COLLISION_WINDOW_MS as f64 {
            return 0.0;
        }

                let base_score = 1.0 - (time_diff / COLLISION_WINDOW_MS as f64).powi(2);
        
        if bundle_a.slot_target == bundle_b.slot_target {
            base_score * 1.5
        } else {
            base_score
        }.min(1.0)
    }

    fn calculate_priority_competition(&self, bundle_a: &TrackedBundle, bundle_b: &TrackedBundle) -> f64 {
        let fee_diff = (bundle_a.priority_fee as i64 - bundle_b.priority_fee as i64).abs() as f64;
        let max_fee = bundle_a.priority_fee.max(bundle_b.priority_fee) as f64;
        
        if max_fee == 0.0 {
            return 0.0;
        }

        let fee_proximity = 1.0 - (fee_diff / max_fee).min(1.0);
        let cu_competition = {
            let total_cu = bundle_a.estimated_cu + bundle_b.estimated_cu;
            let max_cu_per_block = 48_000_000;
            (total_cu as f64 / max_cu_per_block as f64).min(1.0)
        };

        fee_proximity * 0.7 + cu_competition * 0.3
    }

    fn calculate_slot_conflict_probability(&self, bundle_a: &TrackedBundle, bundle_b: &TrackedBundle) -> f64 {
        if bundle_a.slot_target != bundle_b.slot_target {
            return 0.0;
        }

        let slot_tracker = self.slot_tracker.read().unwrap();
        let base_rate = slot_tracker.slot_collision_rates
            .get(&bundle_a.slot_target)
            .copied()
            .unwrap_or(0.3);

        let slot_fill_rate = slot_tracker.bundles_per_slot
            .get(&bundle_a.slot_target)
            .map(|bundles| bundles.len() as f64 / 100.0)
            .unwrap_or(0.1)
            .min(1.0);

        (base_rate + slot_fill_rate) / 2.0
    }

    fn calculate_time_window_collision_score(
        &self, 
        bundle: &TrackedBundle, 
        test_time: Instant, 
        congestion: f64,
        predicted_slot: u64
    ) -> f64 {
        let mut collision_score = congestion * 0.25;
        let mut checked_bundles = 0;
        
        for tracked in self.tracked_bundles.iter() {
            if tracked.key() == &bundle.id || checked_bundles > 100 {
                break;
            }

            let other_bundle = tracked.value();
            let time_overlap = self.calculate_time_overlap(test_time, other_bundle.predicted_execution);
            
            if time_overlap > 0.0 {
                let slot_penalty = if predicted_slot == other_bundle.slot_target { 0.2 } else { 0.0 };
                let account_conflict = self.quick_account_conflict_check(bundle, other_bundle);
                
                collision_score += (time_overlap * 0.3 + account_conflict * 0.5 + slot_penalty) * 0.8;
                checked_bundles += 1;
            }
        }

        collision_score.min(1.0)
    }

    fn quick_account_conflict_check(&self, bundle_a: &TrackedBundle, bundle_b: &TrackedBundle) -> f64 {
        let write_conflicts = bundle_a.write_accounts.intersection(&bundle_b.write_accounts).count();
        let total_writes = bundle_a.write_accounts.len() + bundle_b.write_accounts.len();
        
        if total_writes == 0 {
            0.0
        } else {
            (write_conflicts as f64 * 2.0 / total_writes as f64).min(1.0)
        }
    }

    fn calculate_time_overlap(&self, time_a: Instant, time_b: Instant) -> f64 {
        let diff = if time_a > time_b {
            time_a.duration_since(time_b)
        } else {
            time_b.duration_since(time_a)
        }.as_millis() as f64;

        if diff > COLLISION_WINDOW_MS as f64 {
            0.0
        } else {
            1.0 - (diff / COLLISION_WINDOW_MS as f64)
        }
    }

    fn calculate_current_congestion(&self, patterns: &SubmissionPatternAnalyzer) -> f64 {
        let now = Instant::now();
        let recent_scores: Vec<f64> = patterns.congestion_scores
            .iter()
            .filter(|(time, _)| now.duration_since(*time).as_secs() < 60)
            .map(|(_, score)| *score)
            .collect();

        if recent_scores.is_empty() {
            return 0.3;
        }

        let weighted_sum: f64 = recent_scores.iter()
            .rev()
            .enumerate()
            .map(|(i, score)| score * (1.0 - i as f64 * 0.01).max(0.5))
            .sum();

        (weighted_sum / recent_scores.len() as f64).min(1.0)
    }

    fn predict_slot_at_time(&self, time: Instant, slot_info: &SlotTracker) -> u64 {
        let elapsed = time.duration_since(slot_info.slot_start_time).as_millis() as u64;
        let slots_passed = elapsed / SLOT_DURATION_MS;
        slot_info.current_slot + slots_passed
    }

    fn extract_bundle_resources(&self, bundle: &[Transaction]) -> (HashSet<Pubkey>, HashSet<Pubkey>, HashSet<Pubkey>) {
        let mut accounts = HashSet::with_capacity(bundle.len() * 8);
        let mut write_accounts = HashSet::with_capacity(bundle.len() * 4);
        let mut programs = HashSet::with_capacity(bundle.len() * 2);

        for tx in bundle {
            let message = &tx.message;
            
            for (idx, account_key) in message.account_keys.iter().enumerate() {
                accounts.insert(*account_key);
                
                if message.is_writable(idx) {
                    write_accounts.insert(*account_key);
                }
            }

            for instruction in &message.instructions {
                if let Ok(program_id_index) = usize::try_from(instruction.program_id_index) {
                    if program_id_index < message.account_keys.len() {
                        programs.insert(message.account_keys[program_id_index]);
                    }
                }
            }
        }

        (accounts, write_accounts, programs)
    }

    fn estimate_compute_units(&self, bundle: &[Transaction]) -> u64 {
        let mut total_cu = 0u64;
        
        for tx in bundle {
            let base_cu = 1400u64;
            let instruction_cu = tx.message.instructions.len() as u64 * 200;
            let account_cu = tx.message.account_keys.len() as u64 * 50;
            
            total_cu += base_cu + instruction_cu + account_cu;
        }

        total_cu.min(1_400_000)
    }

    fn calculate_bundle_priority_fee(&self, bundle: &[Transaction]) -> u64 {
        if bundle.is_empty() {
            return 0;
        }

        let mut total_fee = 0u64;
        
        for tx in bundle {
            let sig_count = tx.signatures.len() as u64;
            let base_fee = 5000u64;
            let priority_component = sig_count.saturating_mul(1000);
            let account_fee = tx.message.account_keys.len() as u64 * 100;
            
            total_fee = total_fee.saturating_add(
                base_fee.saturating_add(priority_component).saturating_add(account_fee)
            );
        }

        total_fee / bundle.len() as u64
    }

    fn generate_bundle_id(&self, bundle: &[Transaction]) -> Hash {
        let mut hash_data = Vec::with_capacity(bundle.len() * 64);
        
        for (idx, tx) in bundle.iter().enumerate() {
            if let Some(sig) = tx.signatures.first() {
                hash_data.extend_from_slice(sig.as_ref());
            }
            hash_data.extend_from_slice(&idx.to_le_bytes());
        }
        
        hashv(&[&hash_data])
    }

    fn update_account_locks(&self, bundle_id: &Hash, accounts: &HashSet<Pubkey>, write_accounts: &HashSet<Pubkey>) {
        let now = Instant::now();
        
        for account in write_accounts {
            self.account_locks
                .entry(*account)
                .and_modify(|lock| {
                    if let Some(old_writer) = lock.writer.replace(*bundle_id) {
                        lock.readers.insert(old_writer);
                    }
                    lock.last_access = now;
                    lock.access_frequency = (lock.access_frequency * 0.9) + 0.1;
                    lock.contention_score = (lock.contention_score * 0.95) + 0.05;
                    lock.write_lock_count += 1;
                })
                .or_insert(AccountLockInfo {
                    writer: Some(*bundle_id),
                    readers: HashSet::new(),
                    last_access: now,
                    access_frequency: 0.1,
                    contention_score: 0.05,
                    write_lock_count: 1,
                    read_lock_count: 0,
                });
        }

        for account in accounts.difference(write_accounts) {
            self.account_locks
                .entry(*account)
                .and_modify(|lock| {
                    lock.readers.insert(*bundle_id);
                    lock.last_access = now;
                    lock.access_frequency = (lock.access_frequency * 0.95) + 0.05;
                    lock.read_lock_count += 1;
                })
                .or_insert(AccountLockInfo {
                    writer: None,
                    readers: [*bundle_id].into_iter().collect(),
                    last_access: now,
                    access_frequency: 0.05,
                    contention_score: 0.0,
                    write_lock_count: 0,
                    read_lock_count: 1,
                });
        }

        self.decay_old_locks();
    }

    fn update_access_patterns(&self, accounts: &HashSet<Pubkey>) {
        let now = Instant::now();
        
        for account in accounts {
            self.account_access_patterns
                .entry(*account)
                .and_modify(|pattern| {
                    pattern.access_times.push_back(now);
                    if pattern.access_times.len() > 100 {
                        pattern.access_times.pop_front();
                    }
                    
                    if pattern.access_times.len() > 1 {
                        let intervals: Vec<Duration> = pattern.access_times
                            .iter()
                            .zip(pattern.access_times.iter().skip(1))
                            .map(|(a, b)| b.duration_since(*a))
                            .collect();
                        
                        pattern.avg_interval_ms = intervals.iter()
                            .map(|d| d.as_millis() as f64)
                            .sum::<f64>() / intervals.len() as f64;
                        
                        pattern.access_intervals = intervals;
                    }
                })
                .or_insert(AccessPattern {
                    access_times: vec![now].into_iter().collect(),
                    access_intervals: Vec::new(),
                    peak_times: Vec::new(),
                    avg_interval_ms: SLOT_DURATION_MS as f64,
                });
        }
    }

    fn update_slot_tracking(&self, bundle_id: Hash, slot: u64) {
        let mut tracker = self.slot_tracker.write().unwrap();
        
        tracker.bundles_per_slot
            .entry(slot)
            .and_modify(|bundles| bundles.push(bundle_id))
            .or_insert(vec![bundle_id]);
        
        if tracker.bundles_per_slot.len() > 100 {
            let min_slot = *tracker.bundles_per_slot.keys().min().unwrap_or(&0);
            tracker.bundles_per_slot.remove(&min_slot);
        }
    }

    fn decay_old_locks(&self) {
        let now = Instant::now();
        let decay_threshold = Duration::from_millis(ACCOUNT_LOCK_DECAY_MS);
        
        self.account_locks.retain(|_, lock| {
            let age = now.duration_since(lock.last_access);
            if age > decay_threshold {
                false
            } else {
                let decay_factor = 1.0 - (age.as_millis() as f64 / decay_threshold.as_millis() as f64).powi(2);
                lock.contention_score *= decay_factor;
                lock.access_frequency *= decay_factor;
                true
            }
        });
    }

    fn cleanup_old_data(&self) {
        if self.tracked_bundles.len() > MAX_TRACKED_BUNDLES {
            let mut bundles: Vec<(Hash, Instant)> = self.tracked_bundles
                .iter()
                .map(|entry| (*entry.key(), entry.value().submission_time))
                .collect();
            
            bundles.sort_by_key(|(_, time)| *time);
            
            let remove_count = self.tracked_bundles.len().saturating_sub(MAX_TRACKED_BUNDLES * 9 / 10);
            for (hash, _) in bundles.into_iter().take(remove_count) {
                self.tracked_bundles.remove(&hash);
            }
        }

        let now = Instant::now();
        self.tracked_bundles.retain(|_, bundle| {
            now.duration_since(bundle.submission_time).as_millis() < (COLLISION_WINDOW_MS * 3) as u128
        });
    }

    fn cleanup_metrics_cache(&self) {
        let now = Instant::now();
        self.metrics_cache.retain(|_, (time, _)| {
            now.duration_since(*time).as_millis() < 100
        });
    }

        pub async fn get_active_collisions(&self) -> Vec<(Hash, Hash, BundleCollisionMetrics)> {
        let mut collisions = Vec::with_capacity(1000);
        let bundles: Vec<(Hash, TrackedBundle)> = self.tracked_bundles
            .iter()
            .map(|e| (*e.key(), e.value().clone()))
            .collect();
        
        let bundle_count = bundles.len().min(200);
        for i in 0..bundle_count {
            for j in (i + 1)..bundle_count {
                let metrics = self.predict_collision(&bundles[i].0, &bundles[j].0).await;
                if metrics.collision_probability > COLLISION_SCORE_THRESHOLD {
                    collisions.push((bundles[i].0, bundles[j].0, metrics));
                }
            }
        }

        collisions.sort_by(|a, b| b.2.collision_probability.partial_cmp(&a.2.collision_probability).unwrap());
        collisions.truncate(100);
        collisions
    }

    pub fn update_program_interaction(&self, program_a: Pubkey, program_b: Pubkey, conflict_occurred: bool) {
        let conflict_delta = if conflict_occurred { 0.1 } else { -0.05 };
        let now = Instant::now();
        
        self.program_interactions
            .entry(program_a)
            .and_modify(|interaction| {
                interaction.interaction_count += 1;
                let current = interaction.conflict_patterns.get(&program_b).copied().unwrap_or(0.0);
                let new_value = (current + conflict_delta).clamp(0.0, 1.0);
                interaction.conflict_patterns.insert(program_b, new_value);
                
                if conflict_occurred {
                    interaction.success_rate = (interaction.success_rate * 0.95) + 0.0;
                } else {
                    interaction.success_rate = (interaction.success_rate * 0.95) + 0.05;
                }
            })
            .or_insert(ProgramInteraction {
                program_id: program_a,
                interaction_count: 1,
                avg_execution_time_ms: 50.0,
                conflict_patterns: [(program_b, conflict_delta.max(0.0))].into_iter().collect(),
                cu_consumption: 200_000,
                success_rate: if conflict_occurred { 0.0 } else { 1.0 },
            });

        if program_a != program_b {
            self.program_interactions
                .entry(program_b)
                .and_modify(|interaction| {
                    let current = interaction.conflict_patterns.get(&program_a).copied().unwrap_or(0.0);
                    let new_value = (current + conflict_delta).clamp(0.0, 1.0);
                    interaction.conflict_patterns.insert(program_a, new_value);
                })
                .or_insert(ProgramInteraction {
                    program_id: program_b,
                    interaction_count: 1,
                    avg_execution_time_ms: 50.0,
                    conflict_patterns: [(program_a, conflict_delta.max(0.0))].into_iter().collect(),
                    cu_consumption: 200_000,
                    success_rate: if conflict_occurred { 0.0 } else { 1.0 },
                });
        }
    }

    pub fn record_collision_event(&self, bundle_a: Hash, bundle_b: Hash, collision_type: CollisionType, severity: f64) {
        let slot_tracker = self.slot_tracker.read().unwrap();
        let event = CollisionEvent {
            bundle_a,
            bundle_b,
            timestamp: Instant::now(),
            collision_type: collision_type.clone(),
            severity: severity.clamp(0.0, 1.0),
            slot: slot_tracker.current_slot,
        };

        let mut history = self.collision_history.write().unwrap();
        history.push_back(event);
        
        if history.len() > MAX_ACCOUNT_HISTORY {
            history.pop_front();
        }

        drop(history);
        drop(slot_tracker);

        self.update_collision_patterns(bundle_a, bundle_b, collision_type, severity);
    }

    fn update_collision_patterns(&self, bundle_a: Hash, bundle_b: Hash, collision_type: CollisionType, severity: f64) {
        if let (Some(a), Some(b)) = (self.tracked_bundles.get(&bundle_a), self.tracked_bundles.get(&bundle_b)) {
            for prog_a in &a.programs {
                for prog_b in &b.programs {
                    self.update_program_interaction(*prog_a, *prog_b, severity > 0.5);
                }
            }

            if collision_type == CollisionType::SlotConflict {
                let mut slot_tracker = self.slot_tracker.write().unwrap();
                let slot = a.slot_target;
                let current_rate = slot_tracker.slot_collision_rates.get(&slot).copied().unwrap_or(0.0);
                slot_tracker.slot_collision_rates.insert(slot, (current_rate * 0.9 + severity * 0.1).min(1.0));
            }
        }
    }

    pub fn get_collision_statistics(&self) -> CollisionStatistics {
        let history = self.collision_history.read().unwrap();
        let now = Instant::now();
        let recent_window = Duration::from_secs(300);
        
        let recent_events: Vec<&CollisionEvent> = history
            .iter()
            .filter(|event| now.duration_since(event.timestamp) < recent_window)
            .collect();

        let total_collisions = recent_events.len();
        let avg_severity = if total_collisions > 0 {
            recent_events.iter().map(|e| e.severity).sum::<f64>() / total_collisions as f64
        } else {
            0.0
        };

        let mut type_counts = HashMap::new();
        for event in &recent_events {
            *type_counts.entry(event.collision_type.clone()).or_insert(0) += 1;
        }

        let collisions_per_minute = (total_collisions as f64 / recent_window.as_secs() as f64) * 60.0;

        CollisionStatistics {
            total_collisions,
            avg_severity,
            collisions_per_minute,
            type_distribution: type_counts,
            high_severity_count: recent_events.iter().filter(|e| e.severity > 0.8).count(),
        }
    }

    pub fn update_slot(&self, new_slot: u64) {
        let mut tracker = self.slot_tracker.write().unwrap();
        tracker.current_slot = new_slot;
        tracker.slot_start_time = Instant::now();
        
        if tracker.bundles_per_slot.len() > 100 {
            let slots_to_remove: Vec<u64> = tracker.bundles_per_slot
                .keys()
                .filter(|&&slot| slot < new_slot.saturating_sub(100))
                .copied()
                .collect();
            
            for slot in slots_to_remove {
                tracker.bundles_per_slot.remove(&slot);
                tracker.slot_collision_rates.remove(&slot);
            }
        }
    }

    pub fn record_submission_result(&self, success: bool, latency_ms: f64) {
        let mut patterns = self.submission_patterns.write().unwrap();
        patterns.record_submission(Instant::now(), success, latency_ms);
    }

    pub fn get_account_contention_info(&self, account: &Pubkey) -> Option<(f64, u64, u64)> {
        self.account_locks.get(account).map(|lock| {
            (lock.contention_score, lock.write_lock_count, lock.read_lock_count)
        })
    }

    pub fn predict_bundle_success_rate(&self, bundle: &[Transaction]) -> f64 {
        let (_, _, programs) = self.extract_bundle_resources(bundle);
        let mut success_scores = Vec::new();

        for program in programs {
            if let Some(interaction) = self.program_interactions.get(&program) {
                success_scores.push(interaction.success_rate);
            } else {
                success_scores.push(0.7);
            }
        }

        if success_scores.is_empty() {
            return 0.7;
        }

        let avg_success = success_scores.iter().sum::<f64>() / success_scores.len() as f64;
        let priority_factor = self.calculate_bundle_priority_fee(bundle) as f64 / 100_000.0;
        
        (avg_success * 0.8 + priority_factor.min(0.2)).min(1.0)
    }
}

impl SubmissionPatternAnalyzer {
    fn new() -> Self {
        Self {
            time_buckets: HashMap::with_capacity(1440),
            congestion_scores: VecDeque::with_capacity(1000),
            optimal_windows: Vec::with_capacity(24),
            slot_success_rates: HashMap::with_capacity(1000),
        }
    }

    pub fn record_submission(&mut self, timestamp: Instant, success: bool, latency_ms: f64) {
        let bucket = (timestamp.elapsed().as_secs() / 60) * 60;
        let score = if success { 
            0.2 - (latency_ms / 1000.0).min(0.15) 
        } else { 
            0.8 + (latency_ms / 500.0).min(0.2) 
        };
        
        self.time_buckets
            .entry(bucket)
            .and_modify(|scores| {
                scores.push(score);
                if scores.len() > 100 {
                    scores.remove(0);
                }
            })
            .or_insert_with(|| vec![score]);

        self.congestion_scores.push_back((timestamp, score));
        if self.congestion_scores.len() > 1000 {
            self.congestion_scores.pop_front();
        }

        self.update_optimal_windows();
    }

    fn update_optimal_windows(&mut self) {
        self.optimal_windows.clear();
        
        let mut bucket_stats: Vec<(u64, f64, usize)> = self.time_buckets
            .iter()
            .filter(|(_, scores)| scores.len() >= 5)
            .map(|(time, scores)| {
                let avg_score = scores.iter().sum::<f64>() / scores.len() as f64;
                (*time, avg_score, scores.len())
            })
            .collect();
        
        bucket_stats.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap());
        
        for (time, score, count) in bucket_stats.iter().take(20) {
            if *score < 0.5 && *count > 10 {
                self.optimal_windows.push((*time, *time + 60));
            }
        }
    }
}

impl SlotTracker {
    fn new() -> Self {
        Self {
            current_slot: 0,
            slot_start_time: Instant::now(),
            bundles_per_slot: HashMap::with_capacity(100),
            slot_collision_rates: HashMap::with_capacity(100),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CollisionStatistics {
    pub total_collisions: usize,
    pub avg_severity: f64,
    pub collisions_per_minute: f64,
    pub type_distribution: HashMap<CollisionType, usize>,
    pub high_severity_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;
    use solana_sdk::system_transaction;

    #[tokio::test]
    async fn test_collision_prediction() {
        let predictor = BundleCollisionPredictor::new();
        let keypair = Keypair::new();
        let pubkey = keypair.pubkey();
        
        let tx1 = system_transaction::transfer(&keypair, &pubkey, 1000, Hash::default());
        let tx2 = system_transaction::transfer(&keypair, &pubkey, 2000, Hash::default());
        
        let bundle1 = vec![tx1];
        let bundle2 = vec![tx2];
        
        let id1 = predictor.track_bundle(bundle1, 100).await;
        let id2 = predictor.track_bundle(bundle2, 100).await;
        
        let metrics = predictor.predict_collision(&id1, &id2).await;
        assert!(metrics.collision_probability > 0.0);
        assert!(metrics.account_overlap_score > 0.0);
    }

    #[tokio::test]
    async fn test_optimal_window_finding() {
        let predictor = BundleCollisionPredictor::new();
        let keypair = Keypair::new();
        let tx = system_transaction::transfer(&keypair, &keypair.pubkey(), 1000, Hash::default());
        
        let bundle_id = predictor.track_bundle(vec![tx], 100).await;
        let (window, confidence) = predictor.find_optimal_submission_window(&bundle_id).await;
        
        assert!(confidence >= 0.0 && confidence <= 1.0);
        assert!(window >= Instant::now());
    }
}

