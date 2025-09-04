#[derive(Debug, Clone)]
pub struct DecayEntry {
    pub opportunity_type: String, // e.g., "arbitrage", "liquidation"
    pub start_slot: u64,
    pub end_slot: Option<u64>,
    pub start_time: u64,
    pub end_time: Option<u64>,
    pub profit_before: f64,
    pub profit_after: Option<f64>,
    pub tx_signature: String,
    pub resolved: bool,
}

pub struct DecayTable {
    entries: Vec<DecayEntry>,
}

impl DecayTable {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    pub fn add_event(&mut self, entry: DecayEntry) {
        self.entries.push(entry);
    }

    pub fn resolve_event(&mut self, tx_sig: &str, end_slot: u64, end_time: u64, profit_after: f64) {
        if let Some(entry) = self.entries.iter_mut().find(|e| e.tx_signature == tx_sig) {
            entry.end_slot = Some(end_slot);
            entry.end_time = Some(end_time);
            entry.profit_after = Some(profit_after);
            entry.resolved = true;
        }
    }

    pub fn export_metrics(&self) -> DecayMetrics {
        // Example: calculate histograms, mean/median decay for each opportunity_type
        // ... aggregate using iterator/filter/map
        DecayMetrics::compute(&self.entries)
    }
}
