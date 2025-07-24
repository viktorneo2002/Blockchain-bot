#![deny(unsafe_code)]
#![deny(warnings)]

//! Ghost Slot Detection Engine for Solana MEV Protection
//!
//! This module provides production-ready detection of "ghost slots" - slots with
//! anomalous validator behavior or missing block confirmations that indicate
//! censorship, spam, or spoofed commitment.

use blake3::Hasher;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use tracing::{info, warn};

/// Commitment levels for Solana slots
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommitmentLevel {
    Processed,
    Confirmed,
    Finalized,
}

/// Metadata for a Solana slot
#[derive(Debug, Clone)]
pub struct SlotMeta {
    pub slot: u64,
    pub leader: [u8; 32], // Pubkey as bytes
    pub timestamp: SystemTime,
    pub commitment_level: CommitmentLevel,
    pub confirmed_transactions: u64,
    pub vote_credits: u64,
    pub block_hash: Option<[u8; 32]>,
    pub parent_slot: Option<u64>,
}

/// Leader schedule mapping slot ranges to validators
#[derive(Debug, Clone)]
pub struct LeaderSchedule {
    pub epoch: u64,
    pub slot_leaders: HashMap<u64, [u8; 32]>,
}

/// Reasons why a slot might be considered "ghost"
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GhostReason {
    /// Slot was scheduled for a validator but no block was produced
    MissingBlock {
        expected_leader: [u8; 32],
        timeout_ms: u64,
    },
    /// Slot has zero confirmed transactions and no recent vote credits
    ZeroActivity {
        expected_txs: u64,
        vote_credits: u64,
    },
    /// Leader identity changed unexpectedly
    LeaderMismatch {
        expected: [u8; 32],
        actual: [u8; 32],
    },
    /// Timestamp drift exceeds 500ms from median
    TimestampDrift {
        slot_time: SystemTime,
        median_time: SystemTime,
        drift_ms: u64,
    },
    /// Blockhash commitment was reversed or missing
    CommitmentReversal {
        previous_level: CommitmentLevel,
        current_level: CommitmentLevel,
    },
    /// Multiple leaders appear to sign the same slot
    DuplicateLeader {
        primary_leader: [u8; 32],
        duplicate_leader: [u8; 32],
    },
}

/// Ghost slot detection event
#[derive(Debug, Clone)]
pub struct GhostSlotEvent {
    pub slot: u64,
    pub leader: [u8; 32],
    pub reason: GhostReason,
    pub timestamp: SystemTime,
    pub anomaly_score: f64,
    pub audit_hash: [u8; 32],
}

impl GhostSlotEvent {
    /// Convert ghost slot event to feature vector for ML training
    pub fn to_feature_vector(&self) -> [f64; 16] {
        let mut features = [0.0; 16];
        
        // Feature 0: Slot number (normalized)
        features[0] = (self.slot as f64) / 1_000_000.0;
        
        // Feature 1: Timestamp (seconds since epoch, normalized)
        features[1] = self.timestamp
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_secs() as f64 / 1_000_000.0;
        
        // Feature 2: Anomaly score
        features[2] = self.anomaly_score;
        
        // Features 3-8: One-hot encoding for ghost reason type
        match &self.reason {
            GhostReason::MissingBlock { .. } => features[3] = 1.0,
            GhostReason::ZeroActivity { .. } => features[4] = 1.0,
            GhostReason::LeaderMismatch { .. } => features[5] = 1.0,
            GhostReason::TimestampDrift { .. } => features[6] = 1.0,
            GhostReason::CommitmentReversal { .. } => features[7] = 1.0,
            GhostReason::DuplicateLeader { .. } => features[8] = 1.0,
        }
        
        // Features 9-15: Additional contextual features based on reason
        match &self.reason {
            GhostReason::MissingBlock { timeout_ms, .. } => {
                features[9] = (*timeout_ms as f64) / 1000.0;
            }
            GhostReason::ZeroActivity { expected_txs, vote_credits } => {
                features[10] = *expected_txs as f64;
                features[11] = *vote_credits as f64;
            }
            GhostReason::TimestampDrift { drift_ms, .. } => {
                features[12] = (*drift_ms as f64) / 1000.0;
            }
            _ => {}
        }
        
        // Features 13-15: Leader identity hash (first 3 bytes normalized)
        features[13] = (self.leader[0] as f64) / 255.0;
        features[14] = (self.leader[1] as f64) / 255.0;
        features[15] = (self.leader[2] as f64) / 255.0;
        
        features
    }
    
    /// Create tamperproof audit hash for the event
    fn create_audit_hash(&self) -> [u8; 32] {
        let mut hasher = Hasher::new();
        hasher.update(&self.slot.to_le_bytes());
        hasher.update(&self.leader);
        hasher.update(&format!("{:?}", self.reason).as_bytes());
        hasher.update(&self.timestamp.duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_nanos()
            .to_le_bytes());
        hasher.update(&self.anomaly_score.to_le_bytes());
        hasher.finalize().into()
    }
}

/// Configuration for ghost slot detection
#[derive(Debug, Clone)]
pub struct DetectionConfig {
    pub max_slot_history: usize,
    pub timestamp_drift_threshold_ms: u64,
    pub min_expected_transactions: u64,
    pub block_production_timeout_ms: u64,
    pub commitment_reversal_detection: bool,
}

impl Default for DetectionConfig {
    fn default() -> Self {
        Self {
            max_slot_history: 1000,
            timestamp_drift_threshold_ms: 500,
            min_expected_transactions: 1,
            block_production_timeout_ms: 1000,
            commitment_reversal_detection: true,
        }
    }
}

/// Historical slot tracking for anomaly detection
#[derive(Debug)]
struct SlotHistory {
    slots: VecDeque<SlotMeta>,
    leader_occurrences: HashMap<[u8; 32], VecDeque<u64>>,
    timestamp_history: VecDeque<SystemTime>,
}

impl SlotHistory {
    fn new(max_size: usize) -> Self {
        Self {
            slots: VecDeque::with_capacity(max_size),
            leader_occurrences: HashMap::new(),
            timestamp_history: VecDeque::with_capacity(max_size),
        }
    }
    
    fn add_slot(&mut self, slot: SlotMeta, max_size: usize) {
        if self.slots.len() >= max_size {
            if let Some(old_slot) = self.slots.pop_front() {
                // Clean up old leader occurrences
                if let Some(occurrences) = self.leader_occurrences.get_mut(&old_slot.leader) {
                    occurrences.retain(|&s| s != old_slot.slot);
                    if occurrences.is_empty() {
                        self.leader_occurrences.remove(&old_slot.leader);
                    }
                }
            }
            self.timestamp_history.pop_front();
        }
        
        // Add new slot
        self.leader_occurrences
            .entry(slot.leader)
            .or_insert_with(VecDeque::new)
            .push_back(slot.slot);
        
        self.timestamp_history.push_back(slot.timestamp);
        self.slots.push_back(slot);
    }
    
    fn get_median_timestamp(&self) -> Option<SystemTime> {
        if self.timestamp_history.is_empty() {
            return None;
        }
        
        let mut times: Vec<_> = self.timestamp_history.iter()
            .filter_map(|t| t.duration_since(UNIX_EPOCH).ok())
            .collect();
        times.sort();
        
        let mid = times.len() / 2;
        times.get(mid).map(|d| UNIX_EPOCH + *d)
    }
    
    fn has_leader_conflicts(&self, slot: u64, leader: &[u8; 32]) -> Option<[u8; 32]> {
        for (stored_leader, slots) in &self.leader_occurrences {
            if stored_leader != leader && slots.contains(&slot) {
                return Some(*stored_leader);
            }
        }
        None
    }
}

/// Ghost slot detection engine
pub struct GhostSlotDetector {
    config: DetectionConfig,
    slot_history: Arc<RwLock<SlotHistory>>,
    leader_schedule: Arc<RwLock<Option<LeaderSchedule>>>,
    event_sender: mpsc::UnboundedSender<GhostSlotEvent>,
}

impl GhostSlotDetector {
    /// Create new ghost slot detector with event stream
    pub fn new(config: DetectionConfig) -> (Self, mpsc::UnboundedReceiver<GhostSlotEvent>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        
        let detector = Self {
            slot_history: Arc::new(RwLock::new(SlotHistory::new(config.max_slot_history))),
            leader_schedule: Arc::new(RwLock::new(None)),
            event_sender: sender,
            config,
        };
        
        (detector, receiver)
    }
    
    /// Update leader schedule for epoch
    pub async fn update_leader_schedule(&self, schedule: LeaderSchedule) {
        let mut guard = self.leader_schedule.write().await;
        *guard = Some(schedule);
        info!("Updated leader schedule for epoch {}", guard.as_ref().unwrap().epoch);
    }
    
    /// Process new slot metadata and detect ghost slots
    pub async fn process_slot(&self, slot_meta: SlotMeta) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let mut ghost_events = Vec::new();
        
        // Check for missing blocks
        if let Some(event) = self.check_missing_block(&slot_meta).await? {
            ghost_events.push(event);
        }
        
        // Check for zero activity
        if let Some(event) = self.check_zero_activity(&slot_meta).await? {
            ghost_events.push(event);
        }
        
        // Check for leader mismatch
        if let Some(event) = self.check_leader_mismatch(&slot_meta).await? {
            ghost_events.push(event);
        }
        
        // Check for timestamp drift
        if let Some(event) = self.check_timestamp_drift(&slot_meta).await? {
            ghost_events.push(event);
        }
        
        // Check for commitment reversal
        if let Some(event) = self.check_commitment_reversal(&slot_meta).await? {
            ghost_events.push(event);
        }
        
        // Check for duplicate leaders
        if let Some(event) = self.check_duplicate_leader(&slot_meta).await? {
            ghost_events.push(event);
        }
        
        // Add slot to history
        {
            let mut history = self.slot_history.write().await;
            history.add_slot(slot_meta.clone(), self.config.max_slot_history);
        }
        
        // Emit ghost events
        for event in ghost_events {
            if let Err(e) = self.event_sender.send(event) {
                warn!("Failed to send ghost slot event: {}", e);
            }
        }
        
        Ok(())
    }
    
    async fn check_missing_block(&self, slot_meta: &SlotMeta) -> Result<Option<GhostSlotEvent>, Box<dyn std::error::Error + Send + Sync>> {
        let schedule = self.leader_schedule.read().await;
        
        if let Some(ref schedule) = *schedule {
            if let Some(&expected_leader) = schedule.slot_leaders.get(&slot_meta.slot) {
                if expected_leader != slot_meta.leader && slot_meta.confirmed_transactions == 0 {
                    let event = GhostSlotEvent {
                        slot: slot_meta.slot,
                        leader: slot_meta.leader,
                        reason: GhostReason::MissingBlock {
                            expected_leader,
                            timeout_ms: self.config.block_production_timeout_ms,
                        },
                        timestamp: slot_meta.timestamp,
                        anomaly_score: 0.8,
                        audit_hash: [0; 32],
                    };
                    
                    let mut event_with_hash = event;
                    event_with_hash.audit_hash = event_with_hash.create_audit_hash();
                    
                    warn!("Ghost slot detected: Missing block for slot {}", slot_meta.slot);
                    return Ok(Some(event_with_hash));
                }
            }
        }
        
        Ok(None)
    }
    
    async fn check_zero_activity(&self, slot_meta: &SlotMeta) -> Result<Option<GhostSlotEvent>, Box<dyn std::error::Error + Send + Sync>> {
        if slot_meta.confirmed_transactions == 0 && slot_meta.vote_credits == 0 {
            let event = GhostSlotEvent {
                slot: slot_meta.slot,
                leader: slot_meta.leader,
                reason: GhostReason::ZeroActivity {
                    expected_txs: self.config.min_expected_transactions,
                    vote_credits: slot_meta.vote_credits,
                },
                timestamp: slot_meta.timestamp,
                anomaly_score: 0.6,
                audit_hash: [0; 32],
            };
            
            let mut event_with_hash = event;
            event_with_hash.audit_hash = event_with_hash.create_audit_hash();
            
            warn!("Ghost slot detected: Zero activity for slot {}", slot_meta.slot);
            return Ok(Some(event_with_hash));
        }
        
        Ok(None)
    }
    
    async fn check_leader_mismatch(&self, slot_meta: &SlotMeta) -> Result<Option<GhostSlotEvent>, Box<dyn std::error::Error + Send + Sync>> {
        let schedule = self.leader_schedule.read().await;
        
        if let Some(ref schedule) = *schedule {
            if let Some(&expected_leader) = schedule.slot_leaders.get(&slot_meta.slot) {
                if expected_leader != slot_meta.leader {
                    let event = GhostSlotEvent {
                        slot: slot_meta.slot,
                        leader: slot_meta.leader,
                        reason: GhostReason::LeaderMismatch {
                            expected: expected_leader,
                            actual: slot_meta.leader,
                        },
                        timestamp: slot_meta.timestamp,
                        anomaly_score: 0.9,
                        audit_hash: [0; 32],
                    };
                    
                    let mut event_with_hash = event;
                    event_with_hash.audit_hash = event_with_hash.create_audit_hash();
                    
                    warn!("Ghost slot detected: Leader mismatch for slot {}", slot_meta.slot);
                    return Ok(Some(event_with_hash));
                }
            }
        }
        
        Ok(None)
    }
    
    async fn check_timestamp_drift(&self, slot_meta: &SlotMeta) -> Result<Option<GhostSlotEvent>, Box<dyn std::error::Error + Send + Sync>> {
        let history = self.slot_history.read().await;
        
        if let Some(median_time) = history.get_median_timestamp() {
            let drift = slot_meta.timestamp.duration_since(median_time)
                .or_else(|_| median_time.duration_since(slot_meta.timestamp))
                .unwrap_or(Duration::ZERO);
            
            if drift.as_millis() > self.config.timestamp_drift_threshold_ms as u128 {
                let event = GhostSlotEvent {
                    slot: slot_meta.slot,
                    leader: slot_meta.leader,
                    reason: GhostReason::TimestampDrift {
                        slot_time: slot_meta.timestamp,
                        median_time,
                        drift_ms: drift.as_millis() as u64,
                    },
                    timestamp: slot_meta.timestamp,
                    anomaly_score: 0.7,
                    audit_hash: [0; 32],
                };
                
                let mut event_with_hash = event;
                event_with_hash.audit_hash = event_with_hash.create_audit_hash();
                
                warn!("Ghost slot detected: Timestamp drift for slot {}", slot_meta.slot);
                return Ok(Some(event_with_hash));
            }
        }
        
        Ok(None)
    }
    
    async fn check_commitment_reversal(&self, slot_meta: &SlotMeta) -> Result<Option<GhostSlotEvent>, Box<dyn std::error::Error + Send + Sync>> {
        if !self.config.commitment_reversal_detection {
            return Ok(None);
        }
        
        let history = self.slot_history.read().await;
        
        // Look for the same slot with higher commitment level in history
        for historical_slot in &history.slots {
            if historical_slot.slot == slot_meta.slot {
                let current_level = commitment_level_value(slot_meta.commitment_level);
                let historical_level = commitment_level_value(historical_slot.commitment_level);
                
                if current_level < historical_level {
                    let event = GhostSlotEvent {
                        slot: slot_meta.slot,
                        leader: slot_meta.leader,
                        reason: GhostReason::CommitmentReversal {
                            previous_level: historical_slot.commitment_level,
                            current_level: slot_meta.commitment_level,
                        },
                        timestamp: slot_meta.timestamp,
                        anomaly_score: 0.95,
                        audit_hash: [0; 32],
                    };
                    
                    let mut event_with_hash = event;
                    event_with_hash.audit_hash = event_with_hash.create_audit_hash();
                    
                    warn!("Ghost slot detected: Commitment reversal for slot {}", slot_meta.slot);
                    return Ok(Some(event_with_hash));
                }
            }
        }
        
        Ok(None)
    }
    
    async fn check_duplicate_leader(&self, slot_meta: &SlotMeta) -> Result<Option<GhostSlotEvent>, Box<dyn std::error::Error + Send + Sync>> {
        let history = self.slot_history.read().await;
        
        if let Some(duplicate_leader) = history.has_leader_conflicts(slot_meta.slot, &slot_meta.leader) {
            let event = GhostSlotEvent {
                slot: slot_meta.slot,
                leader: slot_meta.leader,
                reason: GhostReason::DuplicateLeader {
                    primary_leader: slot_meta.leader,
                    duplicate_leader,
                },
                timestamp: slot_meta.timestamp,
                anomaly_score: 1.0,
                audit_hash: [0; 32],
            };
            
            let mut event_with_hash = event;
            event_with_hash.audit_hash = event_with_hash.create_audit_hash();
            
            warn!("Ghost slot detected: Duplicate leader for slot {}", slot_meta.slot);
            return Ok(Some(event_with_hash));
        }
        
        Ok(None)
    }
}

/// Convert commitment level to numeric value for comparison
fn commitment_level_value(level: CommitmentLevel) -> u8 {
    match level {
        CommitmentLevel::Processed => 1,
        CommitmentLevel::Confirmed => 2,
        CommitmentLevel::Finalized => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    
    fn create_test_slot_meta(slot: u64, leader: [u8; 32], timestamp: SystemTime) -> SlotMeta {
        SlotMeta {
            slot,
            leader,
            timestamp,
            commitment_level: CommitmentLevel::Confirmed,
            confirmed_transactions: 10,
            vote_credits: 5,
            block_hash: Some([0; 32]),
            parent_slot: Some(slot.saturating_sub(1)),
        }
    }
    
    fn create_test_leader_schedule() -> LeaderSchedule {
        let mut slot_leaders = HashMap::new();
        let leader1 = [1u8; 32];
        let leader2 = [2u8; 32];
        
        for i in 0..100 {
            slot_leaders.insert(i, if i % 2 == 0 { leader1 } else { leader2 });
        }
        
        LeaderSchedule {
            epoch: 1,
            slot_leaders,
        }
    }
    
    #[tokio::test]
    async fn test_missing_block_detection() {
        let config = DetectionConfig::default();
        let (detector, mut receiver) = GhostSlotDetector::new(config);
        
        // Set up leader schedule
        let schedule = create_test_leader_schedule();
        detector.update_leader_schedule(schedule).await;
        
        // Create slot with wrong leader and no transactions
        let wrong_leader = [99u8; 32];
        let slot_meta = SlotMeta {
            slot: 0,
            leader: wrong_leader,
            timestamp: SystemTime::now(),
            commitment_level: CommitmentLevel::Confirmed,
            confirmed_transactions: 0,
            vote_credits: 0,
            block_hash: None,
            parent_slot: None,
        };
        
        detector.process_slot(slot_meta).await.unwrap();
        
        // Should detect missing block
        let event = receiver.try_recv().unwrap();
        assert_eq!(event.slot, 0);
        assert!(matches!(event.reason, GhostReason::MissingBlock { .. }));
        assert_eq!(event.anomaly_score, 0.8);
    }
    
    #[tokio::test]
    async fn test_zero_activity_detection() {
        let config = DetectionConfig::default();
        let (detector, mut receiver) = GhostSlotDetector::new(config);
        
        let slot_meta = SlotMeta {
            slot: 1,
            leader: [1u8; 32],
            timestamp: SystemTime::now(),
            commitment_level: CommitmentLevel::Confirmed,
            confirmed_transactions: 0,
            vote_credits: 0,
            block_hash: Some([0; 32]),
            parent_slot: Some(0),
        };
        
        detector.process_slot(slot_meta).await.unwrap();
        
        // Should detect zero activity
        let event = receiver.try_recv().unwrap();
        assert_eq!(event.slot, 1);
        assert!(matches!(event.reason, GhostReason::ZeroActivity { .. }));
        assert_eq!(event.anomaly_score, 0.6);
    }
    
    #[tokio::test]
    async fn test_leader_mismatch_detection() {
        let config = DetectionConfig::default();
        let (detector, mut receiver) = GhostSlotDetector::new(config);
        
        // Set up leader schedule
        let schedule = create_test_leader_schedule();
        detector.update_leader_schedule(schedule).await;
        
        // Create slot with wrong leader
        let wrong_leader = [99u8; 32];
        let slot_meta = create_test_slot_meta(2, wrong_leader, SystemTime::now());
        
        detector.process_slot(slot_meta).await.unwrap();
        
        // Should detect leader mismatch
        let event = receiver.try_recv().unwrap();
        assert_eq!(event.slot, 2);
        assert!(matches!(event.reason, GhostReason::LeaderMismatch { .. }));
        assert_eq!(event.anomaly_score, 0.9);
    }
    
    #[tokio::test]
    async fn test_timestamp_drift_detection() {
        let config = DetectionConfig::default();
        let (detector, mut receiver) = GhostSlotDetector::new(config);
        
        let base_time = SystemTime::now();
        
        // Add some normal slots to establish baseline
        for i in 0..5 {
            let slot_meta = create_test_slot_meta([i as u8; 32], [i as u8; 32], base_time);
            detector.process_slot(slot_meta).await.unwrap();
        }
        
        // Add slot with significant timestamp drift
        let drifted_time = base_time + Duration::from_secs(10);
        let slot_meta = create_test_slot_meta(10, [10u8; 32], drifted_time);
        detector.process_slot(slot_meta).await.unwrap();
        
        // Should detect timestamp drift
        let mut found_drift = false;
        while let Ok(event) = receiver.try_recv() {
            if matches!(event.reason, GhostReason::TimestampDrift { .. }) {
                found_drift = true;
                assert_eq!(event.slot, 10);
                assert_eq!(event.anomaly_score, 0.7);
                break;
            }
        }
        assert!(found_drift);
    }
    
    #[tokio::test]
    async fn test_commitment_reversal_detection() {
        let config = DetectionConfig::default();
        let (detector, mut receiver) = GhostSlotDetector::new(config);
        
        // Add slot with finalized commitment
        let mut slot_meta = create_test_slot_meta(5, [5u8; 32], SystemTime::now());
        slot_meta.commitment_level = CommitmentLevel::Finalized;
        detector.process_slot(slot_meta).await.unwrap();
        
        // Add same slot with lower commitment level
        let mut slot_meta_reversed = create_test_slot_meta(5, [5u8; 32], SystemTime::now());
        slot_meta_reversed.commitment_level = CommitmentLevel::Processed;
        detector.process_slot(slot_meta_reversed).await.unwrap();
        
        // Should detect commitment reversal
        let mut found_reversal = false;
        while let Ok(event) = receiver.try_recv() {
            if matches!(event.reason, GhostReason::CommitmentReversal { .. }) {
                found_reversal = true;
                assert_eq!(event.slot, 5);
                assert_eq!(event.anomaly_score, 0.95);
                break;
            }
        }
        assert!(found_reversal);
    }
    
    #[tokio::test]
    async fn test_duplicate_leader_detection() {
        let config = DetectionConfig::default();
        let (detector, mut receiver) = GhostSlotDetector::new(config);
        
        // Add slot with first leader
        let slot_meta1 = create_test_slot_meta(7, [7u8; 32], SystemTime::now());
        detector.process_slot(slot_meta1).await.unwrap();
        
        // Add same slot with different leader
        let slot_meta2 = create_test_slot_meta(7, [77u8; 32], SystemTime::now());
        detector.process_slot(slot_meta2).await.unwrap();
        
        // Should detect duplicate leader
        let mut found_duplicate = false;
        while let Ok(event) = receiver.try_recv() {
            if matches!(event.reason, GhostReason::DuplicateLeader { .. }) {
                found_duplicate = true;
                assert_eq!(event.slot, 7);
                assert_eq!(event.anomaly_score, 1.0);
                break;
            }
        }
        assert!(found_duplicate);
    }
    
    #[tokio::test]
    async fn test_feature_vector_generation() {
        let event = GhostSlotEvent {
            slot: 12345,
            leader: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32],
            reason: GhostReason::MissingBlock {
                expected_leader: [0; 32],
                timeout_ms: 1500,
            },
            timestamp: SystemTime::now(),
            anomaly_score: 0.8,
            audit_hash: [0; 32],
        };
        
        let features = event.to_feature_vector();
        
        // Check that features are properly set
        assert_eq!(features.len(), 16);
        assert_eq!(features[0], 12345.0 / 1_000_000.0); // Normalized slot
        assert_eq!(features[2], 0.8); // Anomaly score
        assert_eq!(features[3], 1.0); // Missing block one-hot
        assert_eq!(features[9], 1.5); // Timeout in seconds
        assert_eq!(features[13], 1.0 / 255.0); // Leader identity first byte
    }
    
    #[tokio::test]
    async fn test_audit_hash_generation() {
        let event = GhostSlotEvent {
            slot: 12345,
            leader: [1; 32],
            reason: GhostReason::ZeroActivity {
                expected_txs: 10,
                vote_credits: 0,
            },
            timestamp: SystemTime::now(),
            anomaly_score: 0.6,
            audit_hash: [0; 32],
        };
        
        let hash1 = event.create_audit_hash();
        let hash2 = event.create_audit_hash();
        
        // Hash should be consistent
        assert_eq!(hash1, hash2);
        
        // Hash should be different for different events
        let mut event2 = event.clone();
        event2.slot = 54321;
        let hash3 = event2.create_audit_hash();
        assert_ne!(hash1, hash3);
    }
    
    #[tokio::test]
    async fn test_normal_slot_processing() {
        let config = DetectionConfig::default();
        let (detector, mut receiver) = GhostSlotDetector::new(config);
        
        // Set up leader schedule
        let schedule = create_test_leader_schedule();
        detector.update_leader_schedule(schedule).await;
        
        // Process normal slot
        let slot_meta = create_test_slot_meta(0, [1u8; 32], SystemTime::now());
        detector.process_slot(slot_meta).await.unwrap();
        
        // Should not generate any ghost events
        assert!(receiver.try_recv().is_err());
    }
}
