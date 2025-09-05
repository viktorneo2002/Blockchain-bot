use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct InclusionModel {
    // Leader acceptance tracking
    leader_acceptance: HashMap<Pubkey, EWMA>,
    
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
}
