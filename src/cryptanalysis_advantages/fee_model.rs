use rand::{rngs::SmallRng, Rng, SeedableRng};

#[derive(Debug, Clone)]
pub struct InclusionStats {
    pub leader_fee_baseline: u64,
    pub p50: u64,
    pub p75: u64,
    pub p90: u64,
    pub p95: u64,
}

#[derive(Debug, Clone)]
pub struct FeePlan {
    pub cu_price_rungs: Vec<u64>,    // lamports per CU (ascending)
    pub jito_tips: Vec<u64>,         // lamports per rung (ascending)
    pub micro_offsets_ms: Vec<i64>,  // diversified micro-windows
}

pub struct LadderPlanner {
    rng: SmallRng,
    jitter_ms: i64,
}

impl LadderPlanner {
    pub fn new() -> Self { Self { rng: SmallRng::from_entropy(), jitter_ms: 2 } }

    pub fn plan(
        &mut self,
        stats: &InclusionStats,
        target_rank: Option<u8>,
        congestion: f64,
    ) -> FeePlan {
        let mut base = stats.leader_fee_baseline;
        base = ((base as f64) * (1.0 + 0.25 * congestion.clamp(0.0, 2.0))) as u64;
        let (lo, hi) = match target_rank {
            Some(r) if r <= 3 => (stats.p90, stats.p95),
            Some(r) if r <= 8 => (stats.p75, stats.p90),
            _ => (stats.p50, stats.p75),
        };
        let rungs = vec![
            base.max(lo),
            ((lo as f64) * 1.05) as u64,
            ((lo as f64) * 1.10) as u64,
            hi,
            ((hi as f64) * 1.08) as u64,
        ];
        let tips = vec![5_000, 12_000, 25_000, 40_000, 70_000];

        let offsets = [-7, -3, 0, 2, 5]
            .into_iter()
            .map(|o| o + self.rng.gen_range(-self.jitter_ms..=self.jitter_ms))
            .collect();

        FeePlan { cu_price_rungs: rungs, jito_tips: tips, micro_offsets_ms: offsets }
    }
}
