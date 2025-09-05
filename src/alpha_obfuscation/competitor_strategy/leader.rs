use std::{collections::HashMap, sync::Arc, time::{Duration, Instant}};
use parking_lot::RwLock;
use solana_sdk::pubkey::Pubkey;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SlotPhase {
    Early, // first ~25% tx index in block
    Mid,   // 25%..75%
    Late,  // last ~25%
}

#[derive(Clone, Debug)]
pub struct LeaderInfo {
    pub leader: Pubkey,
    pub slot: u64,
    pub computed_at: Instant,
}

#[derive(Default)]
pub struct LeaderScheduleCache {
    // Simple LRU-ish via bounded maps; swap to moka if needed.
    by_slot: RwLock<HashMap<u64, LeaderInfo>>,
    max_entries: usize,
}

impl LeaderScheduleCache {
    pub fn new(max_entries: usize) -> Self {
        Self { by_slot: RwLock::new(HashMap::with_capacity(max_entries)), max_entries }
    }

    pub fn insert(&self, slot: u64, leader: Pubkey) {
        let mut w = self.by_slot.write();
        if w.len() >= self.max_entries {
            // Evict oldest by computed_at (simple scan; small map)
            if let Some((&k, _)) = w.iter().min_by_key(|(_, v)| v.computed_at) {
                w.remove(&k);
            }
        }
        w.insert(slot, LeaderInfo { leader, slot, computed_at: Instant::now() });
    }

    pub fn get(&self, slot: u64) -> Option<LeaderInfo> {
        self.by_slot.read().get(&slot).cloned()
    }
}

/// Calculate intra-slot phase using tx index and total tx count as a proxy.
/// Call with meta.loaded_addresses and transaction index in block if available.
pub fn slot_phase_from_index(tx_index_in_block: usize, total_txs_in_block: usize) -> SlotPhase {
    if total_txs_in_block == 0 { return SlotPhase::Mid; }
    let q = tx_index_in_block as f64 / total_txs_in_block as f64;
    match q {
        x if x < 0.25 => SlotPhase::Early,
        x if x < 0.75 => SlotPhase::Mid,
        _ => SlotPhase::Late,
    }
}

#[derive(Default)]
pub struct InclusionStats {
    // success keyed by (leader, phase)
    pub counts: HashMap<(Pubkey, SlotPhase), (u64 /*attempts*/, u64 /*successes*/)>,

    // moving success over last N samples per (leader, phase)
    window: usize,
}

impl InclusionStats {
    pub fn new(window: usize) -> Self { Self { counts: HashMap::new(), window } }

    pub fn record(&mut self, leader: Pubkey, phase: SlotPhase, success: bool) {
        let k = (leader, phase);
        let entry = self.counts.entry(k).or_insert((0, 0));
        entry.0 = entry.0.saturating_add(1);
        if success { entry.1 = entry.1.saturating_add(1); }
        // Optional: implement bounded rolling window with decay.
    }

    pub fn success_rate(&self, leader: Pubkey, phase: SlotPhase) -> f64 {
        self.counts.get(&(leader, phase)).map(|(a, s)| {
            if *a == 0 { 0.0 } else { *s as f64 / *a as f64 }
        }).unwrap_or(0.0)
    }
}
