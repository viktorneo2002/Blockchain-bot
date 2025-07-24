use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};
use tokio::sync::{mpsc, RwLock as TokioRwLock, Notify};
use tokio::time::{interval, sleep};
use solana_sdk::clock::{Slot, DEFAULT_MS_PER_SLOT};
use solana_sdk::pubkey::Pubkey;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_ledger::leader_schedule_cache::LeaderScheduleCache;
use dashmap::DashMap;
use tracing::{info, warn, error, debug};
use serde::{Deserialize, Serialize};

const SLOT_DURATION_MS: u64 = 400;
const SLOT_PREDICTION_WINDOW: u64 = 5;
const LEADER_SCHEDULE_REFRESH_INTERVAL: Duration = Duration::from_secs(30);
const SLOT_TIMING_HISTORY_SIZE: usize = 100;
const SLOT_DRIFT_CORRECTION_FACTOR: f64 = 0.15;
const MAX_SLOT_DRIFT_MS: i64 = 50;
const BUNDLE_SUBMISSION_OFFSET_MS: u64 = 320;
const CRITICAL_TIMING_THRESHOLD_MS: u64 = 20;
const SLOT_TRANSITION_BUFFER_MS: u64 = 25;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlotTiming {
    pub slot: Slot,
    pub start_time: Instant,
    pub unix_ms: u64,
    pub leader: Pubkey,
    pub estimated_end: Instant,
    pub actual_duration: Option<Duration>,
    pub drift_ms: i64,
}

#[derive(Debug, Clone)]
pub struct SlotMetrics {
    pub avg_duration_ms: f64,
    pub drift_variance: f64,
    pub success_rate: f64,
    pub timing_accuracy: f64,
    pub leader_hit_rate: HashMap<Pubkey, f64>,
}

#[derive(Debug)]
pub struct SlotManipulator {
    current_slot: Arc<AtomicU64>,
    slot_start_time: Arc<RwLock<Instant>>,
    slot_unix_ms: Arc<AtomicU64>,
    leader_schedule: Arc<TokioRwLock<HashMap<Slot, Pubkey>>>,
    slot_timing_history: Arc<RwLock<VecDeque<SlotTiming>>>,
    drift_correction: Arc<RwLock<i64>>,
    metrics: Arc<RwLock<SlotMetrics>>,
    rpc_client: Arc<RpcClient>,
    slot_notify: Arc<Notify>,
    running: Arc<AtomicBool>,
    bundle_windows: Arc<DashMap<Slot, BundleWindow>>,
    leader_performance: Arc<DashMap<Pubkey, LeaderStats>>,
}

#[derive(Debug, Clone)]
struct BundleWindow {
    slot: Slot,
    optimal_send_time: Instant,
    deadline: Instant,
    leader: Pubkey,
    priority_factor: f64,
}

#[derive(Debug, Clone, Default)]
struct LeaderStats {
    total_slots: u64,
    successful_bundles: u64,
    avg_latency_ms: f64,
    reliability_score: f64,
    last_seen: Instant,
}

impl SlotManipulator {
    pub fn new(rpc_client: Arc<RpcClient>) -> Self {
        let now = Instant::now();
        Self {
            current_slot: Arc::new(AtomicU64::new(0)),
            slot_start_time: Arc::new(RwLock::new(now)),
            slot_unix_ms: Arc::new(AtomicU64::new(
                SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64
            )),
            leader_schedule: Arc::new(TokioRwLock::new(HashMap::new())),
            slot_timing_history: Arc::new(RwLock::new(VecDeque::with_capacity(SLOT_TIMING_HISTORY_SIZE))),
            drift_correction: Arc::new(RwLock::new(0)),
            metrics: Arc::new(RwLock::new(SlotMetrics {
                avg_duration_ms: SLOT_DURATION_MS as f64,
                drift_variance: 0.0,
                success_rate: 1.0,
                timing_accuracy: 1.0,
                leader_hit_rate: HashMap::new(),
            })),
            rpc_client,
            slot_notify: Arc::new(Notify::new()),
            running: Arc::new(AtomicBool::new(false)),
            bundle_windows: Arc::new(DashMap::new()),
            leader_performance: Arc::new(DashMap::new()),
        }
    }

    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        let slot_tracker = self.clone();
        tokio::spawn(async move {
            slot_tracker.slot_tracking_loop().await;
        });

        let leader_updater = self.clone();
        tokio::spawn(async move {
            leader_updater.leader_schedule_update_loop().await;
        });

        let window_calculator = self.clone();
        tokio::spawn(async move {
            window_calculator.bundle_window_calculation_loop().await;
        });

        self.initialize_slot_data().await?;
        Ok(())
    }

    async fn initialize_slot_data(&self) -> Result<(), Box<dyn std::error::Error>> {
        let slot = self.rpc_client.get_slot().await?;
        self.current_slot.store(slot, Ordering::SeqCst);
        
        let now = Instant::now();
        *self.slot_start_time.write().unwrap() = now;
        
        let unix_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
        self.slot_unix_ms.store(unix_ms, Ordering::SeqCst);
        
        self.update_leader_schedule().await?;
        Ok(())
    }

    async fn slot_tracking_loop(&self) {
        let mut interval = interval(Duration::from_millis(50));
        let mut last_slot = self.current_slot.load(Ordering::SeqCst);
        let mut consecutive_errors = 0;

        while self.running.load(Ordering::SeqCst) {
            interval.tick().await;

            match self.rpc_client.get_slot().await {
                Ok(new_slot) => {
                    consecutive_errors = 0;
                    
                    if new_slot > last_slot {
                        let now = Instant::now();
                        let unix_ms = SystemTime::now().duration_since(UNIX_EPOCH)
                            .unwrap().as_millis() as u64;
                        
                        let old_start = *self.slot_start_time.read().unwrap();
                        let actual_duration = now.duration_since(old_start);
                        
                        self.record_slot_transition(last_slot, new_slot, actual_duration).await;
                        
                        self.current_slot.store(new_slot, Ordering::SeqCst);
                        *self.slot_start_time.write().unwrap() = now;
                        self.slot_unix_ms.store(unix_ms, Ordering::SeqCst);
                        
                        self.slot_notify.notify_waiters();
                        last_slot = new_slot;
                        
                        self.calculate_optimal_bundle_windows(new_slot).await;
                    }
                }
                Err(e) => {
                    consecutive_errors += 1;
                    if consecutive_errors > 5 {
                        error!("Slot tracking error streak: {}", e);
                        self.apply_predictive_slot_advance().await;
                    }
                }
            }
        }
    }

    async fn record_slot_transition(&self, old_slot: Slot, new_slot: Slot, duration: Duration) {
        let leader = self.get_slot_leader(new_slot).await.unwrap_or_default();
        let drift = (duration.as_millis() as i64) - (SLOT_DURATION_MS as i64);
        
        let timing = SlotTiming {
            slot: old_slot,
            start_time: *self.slot_start_time.read().unwrap(),
            unix_ms: self.slot_unix_ms.load(Ordering::SeqCst),
            leader: leader.clone(),
            estimated_end: *self.slot_start_time.read().unwrap() + Duration::from_millis(SLOT_DURATION_MS),
            actual_duration: Some(duration),
            drift_ms: drift,
        };

        let mut history = self.slot_timing_history.write().unwrap();
        history.push_back(timing);
        if history.len() > SLOT_TIMING_HISTORY_SIZE {
            history.pop_front();
        }

        self.update_drift_correction(&history);
        self.update_metrics(&history);
        
        if let Some(mut stats) = self.leader_performance.get_mut(&leader) {
            stats.total_slots += 1;
            stats.last_seen = Instant::now();
        } else {
            self.leader_performance.insert(leader, LeaderStats {
                total_slots: 1,
                last_seen: Instant::now(),
                ..Default::default()
            });
        }
    }

    fn update_drift_correction(&self, history: &VecDeque<SlotTiming>) {
        if history.len() < 10 {
            return;
        }

        let recent_drifts: Vec<i64> = history.iter()
            .rev()
            .take(20)
            .filter_map(|t| t.actual_duration.map(|d| (d.as_millis() as i64) - (SLOT_DURATION_MS as i64)))
            .collect();

        if !recent_drifts.is_empty() {
            let avg_drift = recent_drifts.iter().sum::<i64>() / recent_drifts.len() as i64;
            let correction = (avg_drift as f64 * SLOT_DRIFT_CORRECTION_FACTOR) as i64;
            let clamped = correction.clamp(-MAX_SLOT_DRIFT_MS, MAX_SLOT_DRIFT_MS);
            *self.drift_correction.write().unwrap() = clamped;
        }
    }

    fn update_metrics(&self, history: &VecDeque<SlotTiming>) {
        let mut metrics = self.metrics.write().unwrap();
        
        if history.len() > 10 {
            let durations: Vec<f64> = history.iter()
                .filter_map(|t| t.actual_duration.map(|d| d.as_millis() as f64))
                .collect();
            
            if !durations.is_empty() {
                metrics.avg_duration_ms = durations.iter().sum::<f64>() / durations.len() as f64;
                
                let variance = durations.iter()
                    .map(|d| (d - metrics.avg_duration_ms).powi(2))
                    .sum::<f64>() / durations.len() as f64;
                metrics.drift_variance = variance.sqrt();
                
                let within_threshold = durations.iter()
                    .filter(|&&d| (d - SLOT_DURATION_MS as f64).abs() < CRITICAL_TIMING_THRESHOLD_MS as f64)
                    .count();
                metrics.timing_accuracy = within_threshold as f64 / durations.len() as f64;
            }
        }
    }

    async fn apply_predictive_slot_advance(&self) {
        let correction = *self.drift_correction.read().unwrap();
        let adjusted_duration = ((SLOT_DURATION_MS as i64) + correction).max(350) as u64;
        
        let last_start = *self.slot_start_time.read().unwrap();
        let elapsed = Instant::now().duration_since(last_start);
        
        if elapsed.as_millis() as u64 >= adjusted_duration {
            let current = self.current_slot.load(Ordering::SeqCst);
            let predicted_slot = current + 1;
            
            self.current_slot.store(predicted_slot, Ordering::SeqCst);
            *self.slot_start_time.write().unwrap() = Instant::now();
            let unix_ms = SystemTime::now().duration_since(UNIX_EPOCH)
                .unwrap().as_millis() as u64;
            self.slot_unix_ms.store(unix_ms, Ordering::SeqCst);
            
            self.slot_notify.notify_waiters();
            debug!("Predictive slot advance to {}", predicted_slot);
        }
    }

    async fn leader_schedule_update_loop(&self) {
        let mut interval = interval(LEADER_SCHEDULE_REFRESH_INTERVAL);
        
        while self.running.load(Ordering::SeqCst) {
            interval.tick().await;
            if let Err(e) = self.update_leader_schedule().await {
                error!("Leader schedule update failed: {}", e);
            }
        }
    }

    async fn update_leader_schedule(&self) -> Result<(), Box<dyn std::error::Error>> {
        let current_slot = self.current_slot.load(Ordering::SeqCst);
        let epoch = current_slot / 432000;
        
        let leader_schedule = self.rpc_client.get_leader_schedule(Some(current_slot)).await?;
        
        if let Some(schedule) = leader_schedule {
            let mut new_schedule = HashMap::new();
            
            for (leader_str, slots) in schedule {
                if let Ok(leader) = leader_str.parse::<Pubkey>() {
                    for &slot_offset in &slots {
                        let absolute_slot = epoch * 432000 + slot_offset as u64;
                        new_schedule.insert(absolute_slot, leader);
                    }
                }
            }
            
            *self.leader_schedule.write().await = new_schedule;
        }
        
        Ok(())
    }

    async fn bundle_window_calculation_loop(&self) {
        let mut interval = interval(Duration::from_millis(100));
        
        while self.running.load(Ordering::SeqCst) {
            interval.tick().await;
            
            let current_slot = self.current_slot.load(Ordering::SeqCst);
            for slot in current_slot..(current_slot + SLOT_PREDICTION_WINDOW) {
                self.calculate_optimal_bundle_windows(slot).await;
            }
            
            self.cleanup_old_bundle_windows(current_slot);
        }
    }

    async fn calculate_optimal_bundle_windows(&self, slot: Slot) {
        let leader = match self.get_slot_leader(slot).await {
            Some(l) => l,
            None => return,
        };

        let slot_start = self.predict_slot_start_time(slot).await;
        let leader_stats = self.get_leader_stats(&leader);
        let reliability = leader_stats.reliability_score;
        let latency_adjustment = (leader_stats.avg_latency_ms * 0.85) as u64;
        
        let base_offset = BUNDLE_SUBMISSION_OFFSET_MS;
        let dynamic_offset = self.calculate_dynamic_offset(slot, &leader).await;
        let final_offset = base_offset.saturating_sub(latency_adjustment).max(dynamic_offset);
        
        let optimal_send_time = slot_start + Duration::from_millis(final_offset);
        let deadline = slot_start + Duration::from_millis(SLOT_DURATION_MS - SLOT_TRANSITION_BUFFER_MS);
        
        let priority_factor = self.calculate_priority_factor(slot, &leader, reliability).await;
        
        let window = BundleWindow {
            slot,
            optimal_send_time,
            deadline,
            leader,
            priority_factor,
        };
        
        self.bundle_windows.insert(slot, window);
    }

    async fn calculate_dynamic_offset(&self, slot: Slot, leader: &Pubkey) -> u64 {
        let metrics = self.metrics.read().unwrap();
        let drift = *self.drift_correction.read().unwrap();
        let leader_stats = self.get_leader_stats(leader);
        
        let base = 280u64;
        let variance_adjustment = (metrics.drift_variance * 0.5).min(30.0) as u64;
        let leader_adjustment = if leader_stats.reliability_score > 0.95 {
            10u64
        } else if leader_stats.reliability_score > 0.85 {
            20u64
        } else {
            35u64
        };
        
        let drift_adjustment = if drift > 0 {
            (drift.abs() as f64 * 0.7) as u64
        } else {
            0
        };
        
        base + variance_adjustment + leader_adjustment - drift_adjustment
    }

    async fn calculate_priority_factor(&self, slot: Slot, leader: &Pubkey, reliability: f64) -> f64 {
        let current_slot = self.current_slot.load(Ordering::SeqCst);
        let slot_distance = slot.saturating_sub(current_slot) as f64;
        
        let distance_factor = 1.0 / (1.0 + slot_distance * 0.1);
        let reliability_factor = reliability.powf(2.0);
        let timing_accuracy = self.metrics.read().unwrap().timing_accuracy;
        
        let mut priority = distance_factor * reliability_factor * timing_accuracy;
        
        if slot_distance <= 1.0 {
            priority *= 1.5;
        }
        
        let recent_success_rate = self.calculate_recent_leader_success(leader).await;
        priority *= (0.7 + 0.3 * recent_success_rate);
        
        priority.min(1.0).max(0.1)
    }

    async fn calculate_recent_leader_success(&self, leader: &Pubkey) -> f64 {
        let history = self.slot_timing_history.read().unwrap();
        let recent_slots: Vec<&SlotTiming> = history.iter()
            .rev()
            .take(50)
            .filter(|t| &t.leader == leader)
            .collect();
        
        if recent_slots.is_empty() {
            return 0.5;
        }
        
        let successful = recent_slots.iter()
            .filter(|t| {
                t.actual_duration
                    .map(|d| (d.as_millis() as i64 - SLOT_DURATION_MS as i64).abs() < 50)
                    .unwrap_or(false)
            })
            .count();
        
        successful as f64 / recent_slots.len() as f64
    }

    fn cleanup_old_bundle_windows(&self, current_slot: Slot) {
        let cutoff = current_slot.saturating_sub(2);
        self.bundle_windows.retain(|&slot, _| slot >= cutoff);
    }

    async fn predict_slot_start_time(&self, target_slot: Slot) -> Instant {
        let current_slot = self.current_slot.load(Ordering::SeqCst);
        let current_start = *self.slot_start_time.read().unwrap();
        
        if target_slot <= current_slot {
            return current_start;
        }
        
        let slot_difference = target_slot - current_slot;
        let drift = *self.drift_correction.read().unwrap();
        let adjusted_slot_duration = ((SLOT_DURATION_MS as i64) + drift).max(350) as u64;
        
        let naive_prediction = current_start + Duration::from_millis(slot_difference * adjusted_slot_duration);
        
        let metrics = self.metrics.read().unwrap();
        if metrics.timing_accuracy > 0.9 && metrics.drift_variance < 15.0 {
            naive_prediction
        } else {
            let variance_buffer = Duration::from_millis((metrics.drift_variance * 0.3) as u64);
            naive_prediction + variance_buffer
        }
    }

    async fn get_slot_leader(&self, slot: Slot) -> Option<Pubkey> {
        let schedule = self.leader_schedule.read().await;
        schedule.get(&slot).copied()
    }

    fn get_leader_stats(&self, leader: &Pubkey) -> LeaderStats {
        self.leader_performance
            .get(leader)
            .map(|entry| entry.value().clone())
            .unwrap_or_default()
    }

    pub async fn get_optimal_bundle_window(&self, slot: Slot) -> Option<BundleWindow> {
        self.bundle_windows.get(&slot).map(|entry| entry.value().clone())
    }

    pub fn get_current_slot(&self) -> Slot {
        self.current_slot.load(Ordering::SeqCst)
    }

    pub fn get_slot_start_time(&self) -> Instant {
        *self.slot_start_time.read().unwrap()
    }

    pub fn get_slot_unix_ms(&self) -> u64 {
        self.slot_unix_ms.load(Ordering::SeqCst)
    }

    pub async fn wait_for_slot(&self, target_slot: Slot) -> Result<(), Box<dyn std::error::Error>> {
        let timeout = Duration::from_secs(10);
        let start = Instant::now();
        
        while self.get_current_slot() < target_slot {
            if start.elapsed() > timeout {
                return Err("Slot wait timeout".into());
            }
            
            tokio::select! {
                _ = self.slot_notify.notified() => {},
                _ = sleep(Duration::from_millis(50)) => {},
            }
        }
        
        Ok(())
    }

    pub async fn get_next_leader_slots(&self, count: usize) -> Vec<(Slot, Pubkey)> {
        let current_slot = self.get_current_slot();
        let schedule = self.leader_schedule.read().await;
        
        let mut result = Vec::with_capacity(count);
        for i in 0..count {
            let slot = current_slot + i as u64;
            if let Some(&leader) = schedule.get(&slot) {
                result.push((slot, leader));
            }
        }
        
        result
    }

    pub async fn predict_next_slot_timing(&self) -> (Instant, Duration) {
        let current_slot = self.get_current_slot();
        let next_slot = current_slot + 1;
        
        let predicted_start = self.predict_slot_start_time(next_slot).await;
        let drift = *self.drift_correction.read().unwrap();
        let predicted_duration = Duration::from_millis(
            ((SLOT_DURATION_MS as i64) + drift).max(350) as u64
        );
        
        (predicted_start, predicted_duration)
    }

    pub fn get_slot_metrics(&self) -> SlotMetrics {
        self.metrics.read().unwrap().clone()
    }

    pub async fn update_leader_performance(&self, leader: Pubkey, success: bool, latency_ms: f64) {
        let mut stats = self.leader_performance.entry(leader).or_default();
        
        if success {
            stats.successful_bundles += 1;
        }
        
        let alpha = 0.1;
        stats.avg_latency_ms = stats.avg_latency_ms * (1.0 - alpha) + latency_ms * alpha;
        
        let success_rate = if stats.total_slots > 0 {
            stats.successful_bundles as f64 / stats.total_slots as f64
        } else {
            0.5
        };
        
        stats.reliability_score = stats.reliability_score * 0.95 + success_rate * 0.05;
        stats.last_seen = Instant::now();
    }

    pub async fn is_slot_transition_imminent(&self) -> bool {
        let slot_start = self.get_slot_start_time();
        let elapsed = Instant::now().duration_since(slot_start);
        let drift = *self.drift_correction.read().unwrap();
        let adjusted_duration = ((SLOT_DURATION_MS as i64) + drift).max(350) as u64;
        
        elapsed.as_millis() as u64 > adjusted_duration - SLOT_TRANSITION_BUFFER_MS
    }

    pub async fn get_slot_time_remaining(&self) -> Duration {
        let slot_start = self.get_slot_start_time();
        let elapsed = Instant::now().duration_since(slot_start);
        let drift = *self.drift_correction.read().unwrap();
        let adjusted_duration = ((SLOT_DURATION_MS as i64) + drift).max(350) as u64;
        
        let remaining_ms = adjusted_duration.saturating_sub(elapsed.as_millis() as u64);
        Duration::from_millis(remaining_ms)
    }

    pub fn shutdown(&self) {
        self.running.store(false, Ordering::SeqCst);
        self.slot_notify.notify_waiters();
    }
}

impl Clone for SlotManipulator {
    fn clone(&self) -> Self {
        Self {
            current_slot: Arc::clone(&self.current_slot),
            slot_start_time: Arc::clone(&self.slot_start_time),
            slot_unix_ms: Arc::clone(&self.slot_unix_ms),
            leader_schedule: Arc::clone(&self.leader_schedule),
            slot_timing_history: Arc::clone(&self.slot_timing_history),
            drift_correction: Arc::clone(&self.drift_correction),
            metrics: Arc::clone(&self.metrics),
            rpc_client: Arc::clone(&self.rpc_client),
            slot_notify: Arc::clone(&self.slot_notify),
            running: Arc::clone(&self.running),
            bundle_windows: Arc::clone(&self.bundle_windows),
            leader_performance: Arc::clone(&self.leader_performance),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_client::rpc_client::RpcClient as SyncRpcClient;

    #[tokio::test]
    async fn test_slot_manipulator_initialization() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let manipulator = SlotManipulator::new(rpc_client);
        
        assert_eq!(manipulator.get_current_slot(), 0);
        assert!(manipulator.start().await.is_ok());
        assert!(manipulator.get_current_slot() > 0);
    }

    #[tokio::test]
    async fn test_slot_prediction_accuracy() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let manipulator = SlotManipulator::new(rpc_client);
        
        let (predicted_start, predicted_duration) = manipulator.predict_next_slot_timing().await;
        assert!(predicted_duration.as_millis() >= 350);
        assert!(predicted_duration.as_millis() <= 450);
    }
}
