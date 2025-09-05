use solana_sdk::pubkey::Pubkey;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::RwLock;
use std::collections::HashMap;

const FEE_PCTILE_DEFAULT: f64 = 0.99;
const FEE_PCTILE_AGGRESSIVE: f64 = 0.995;

#[derive(Clone, Default)]
struct LeaderStats {
    included: u64,
    missed: u64,
    fee_samples: Vec<u64>,
}

#[derive(Clone)]
pub struct FeeModel {
    leaders: Arc<RwLock<HashMap<Pubkey, LeaderStats>>>,
}

impl FeeModel {
    pub fn new() -> Self {
        Self {
            leaders: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn record_inclusion(&self, leader: Pubkey, landed: bool, micro_per_cu: u64) {
        let mut map = self.leaders.write().await;
        let stats = map.entry(leader).or_default();
        
        if landed {
            stats.included += 1;
        } else {
            stats.missed += 1;
        }

        if stats.fee_samples.len() > 2048 {
            stats.fee_samples.drain(0..1024);
        }
        stats.fee_samples.push(micro_per_cu);
    }

    pub async fn choose_percentile(&self, leader: Pubkey) -> f64 {
        let map = self.leaders.read().await;
        if let Some(s) = map.get(&leader) {
            let total = s.included + s.missed;
            if total > 50 && (s.missed as f64 / total as f64) > 0.4 {
                return FEE_PCTILE_AGGRESSIVE;
            }
        }
        FEE_PCTILE_DEFAULT
    }
}

#[derive(Clone)]
pub struct PriorityFeeCalculator {
    recent_fees: Arc<RwLock<Vec<u64>>>,
    base_priority_fee: Arc<AtomicU64>,
    fee_model: Arc<FeeModel>,
}

impl PriorityFeeCalculator {
    pub fn new(base_priority_fee: u64) -> Self {
        Self {
            recent_fees: Arc::new(RwLock::new(Vec::with_capacity(1000))),
            base_priority_fee: Arc::new(AtomicU64::new(base_priority_fee)),
            fee_model: Arc::new(FeeModel::new()),
        }
    }

    pub async fn estimate_micro_per_cu_for_accounts(
        &self,
        rpc: &solana_client::rpc_client::RpcClient,
        accounts: &[Pubkey],
        leader_opt: Option<Pubkey>,
        approximate_cu: u64,
    ) -> u64 {
        // Fetch recent prioritization fees scoped to accounts
        let mut sample: Vec<u64> = Vec::with_capacity(256);
        
        if let Ok(fees) = rpc.get_recent_prioritization_fees(accounts).await {
            for f in fees {
                if f.prioritization_fee > 0 {
                    sample.push(f.prioritization_fee);
                }
            }
        }

        // Fallback to global base if empty
        if sample.is_empty() {
            let base_lamports = self.base_priority_fee.load(Ordering::Acquire);
            return (base_lamports.saturating_mul(1_000_000)).saturating_div(approximate_cu.max(1));
        }

        sample.sort_unstable();
        let leader = leader_opt.unwrap_or_default();
        let pct = self.fee_model.choose_percentile(leader).await;
        let idx = ((sample.len() as f64 - 1.0) * pct).round() as usize;
        let lamports = sample[idx.min(sample.len() - 1)];
        
        (lamports.saturating_mul(1_000_000)).saturating_div(approximate_cu.max(1))
    }

    pub fn get_fee_model(&self) -> Arc<FeeModel> {
        self.fee_model.clone()
    }
}
