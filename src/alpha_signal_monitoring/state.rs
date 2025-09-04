use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpportunityState {
    pub dex: String,
    pub token_pair: String,
    pub route: Vec<String>,
    pub expected_profit: f64,
    pub fee: f64,
    pub slippage: f64,
    pub timestamp: u64,
    pub decay_rate_estimate: f64,
    pub status: OpportunityStatus,
    pub history: Vec<OpportunitySnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpportunityStatus {
    Open,
    Decayed,
    Claimed,
    Rejected,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpportunitySnapshot {
    pub timestamp: u64,
    pub profit_rate: f64,
    pub decay_rate: f64,
}
