use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use solana_sdk::pubkey::Pubkey;

#[derive(Default)]
struct LeaderAcceptanceStats {
    accepted: f64,
    rejected: f64,
    min_tip_seen: u64,
}

pub struct InclusionModel {
    // Leader acceptance tracking
    leader_acceptance: HashMap<Pubkey, EWMA>,
    
    // Leader-specific history
    leader_history: HashMap<Pubkey, LeaderAcceptanceStats>,
    
    // Slot congestion state
    slot_congestion: AtomicU64,
    congestion_threshold: u64,
    
    // Current dynamic multiplier
    current_multiplier: f64,
}

impl InclusionModel {
    pub fn new(congestion_threshold: u64) -> Self {
        Self {
            leader_acceptance: HashMap::new(),
            leader_history: HashMap::new(),
            slot_congestion: AtomicU64::new(0),
            congestion_threshold,
            current_multiplier: 1.0,
        }
    }
    
    pub fn update_leader_acceptance(&mut self, leader: &Pubkey, accepted: bool) {
        let acceptance = self.leader_acceptance
            .entry(*leader)
            .or_insert(EWMA::new(0.9)); // 90% decay
            
        acceptance.update(if accepted { 1.0 } else { 0.0 });
    }
    
    pub fn update_leader_stats(&mut self, leader: &Pubkey, accepted: bool, tip: u64) {
        let stats = self.leader_history.entry(*leader).or_default();
        if accepted {
            stats.accepted += 1.0;
            stats.min_tip_seen = stats.min_tip_seen.min(tip);
        } else {
            stats.rejected += 1.0;
        }
    }
    
    pub fn update_slot_congestion(&self, mempool_density: u64) {
        self.slot_congestion.store(mempool_density, Ordering::Relaxed);
        
        // Adjust multiplier based on congestion
        if mempool_density > self.congestion_threshold {
            let overload_ratio = mempool_density as f64 / self.congestion_threshold as f64;
            self.current_multiplier = 1.0 + overload_ratio.sqrt();
        } else {
            self.current_multiplier = 1.0;
        }
    }
    
    pub fn get_bid_multiplier(&self, leader: &Pubkey) -> f64 {
        let acceptance = self.leader_acceptance
            .get(leader)
            .map(|ewma| ewma.value())
            .unwrap_or(1.0);
            
        acceptance * self.current_multiplier
    }
    
    pub fn get_leader_tip_adjustment(&self, leader: &Pubkey) -> u64 {
        self.leader_history.get(leader)
            .map(|stats| {
                let reject_rate = stats.rejected / (stats.accepted + stats.rejected);
                if reject_rate > 0.3 {
                    stats.min_tip_seen.saturating_add(1000) // Add small buffer
                } else {
                    0
                }
            })
            .unwrap_or(0)
    }
}
