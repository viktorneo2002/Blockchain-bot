use dashmap::DashMap;
use parking_lot::RwLock;
use rand::{rngs::SmallRng, SeedableRng};
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use std::{collections::VecDeque, time::Instant};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InclusionSnapshot {
    pub leader_fee_baseline: u64,
    pub p50: u64,
    pub p75: u64,
    pub p90: u64,
    pub p95: u64,
    pub last_update: Instant,
}

#[derive(Debug)]
struct OnlineQuantiles {
    // Rolling window + EWMAs for robustness
    window: VecDeque<u64>,
    cap: usize,
    ewma: f64,
    alpha: f64,
}

impl OnlineQuantiles {
    fn new(cap: usize, alpha: f64) -> Self {
        Self { window: VecDeque::with_capacity(cap), cap, ewma: 0.0, alpha }
    }
    fn insert(&mut self, v: u64) {
        if self.window.len() == self.cap { self.window.pop_front(); }
        self.window.push_back(v);
        self.ewma = self.alpha * v as f64 + (1.0 - self.alpha) * self.ewma;
    }
    fn pct(sorted: &[u64], p: f64) -> u64 {
        if sorted.is_empty() { return 0; }
        let i = ((sorted.len() as f64 - 1.0) * p).round() as usize;
        sorted[i]
    }
    fn snapshot(&self) -> (u64, u64, u64, u64, u64) {
        if self.window.is_empty() {
            return (0, 0, 0, 0, 0);
        }
        let mut v = self.window.clone().into_iter().collect::<Vec<_>>();
        v.sort_unstable();
        let p50 = Self::pct(&v, 0.50);
        let p75 = Self::pct(&v, 0.75);
        let p90 = Self::pct(&v, 0.90);
        let p95 = Self::pct(&v, 0.95);
        let baseline = (self.ewma as u64).max(p50);
        (baseline, p50, p75, p90, p95)
    }
}

pub struct LeaderStats {
    // leader -> quantiles of lamports per CU used for landed tx
    fee_quantiles: DashMap<Pubkey, RwLock<OnlineQuantiles>>,
}

impl LeaderStats {
    pub fn new() -> Self {
        Self { fee_quantiles: DashMap::new() }
    }

    pub fn record_landing(&self, leader: Pubkey, lamports_per_cu: u64) {
        let entry = self.fee_quantiles
            .entry(leader)
            .or_insert_with(|| RwLock::new(OnlineQuantiles::new(512, 0.08)));
        {
            let mut q = entry.write();
            q.insert(lamports_per_cu);
        }
    }

    pub fn snapshot(&self, leader: Pubkey) -> InclusionSnapshot {
        if let Some(q) = self.fee_quantiles.get(&leader) {
            let (baseline, p50, p75, p90, p95) = q.read().snapshot();
            InclusionSnapshot { leader_fee_baseline: baseline, p50, p75, p90, p95, last_update: Instant::now() }
        } else {
            // Conservative defaults when unknown leader
            InclusionSnapshot {
                leader_fee_baseline: 3_000,
                p50: 3_500, p75: 5_000, p90: 8_000, p95: 12_000,
                last_update: Instant::now(),
            }
        }
    }
}
