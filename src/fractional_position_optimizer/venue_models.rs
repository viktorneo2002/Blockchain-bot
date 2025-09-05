use std::sync::Arc;
use parking_lot::RwLock;
use serde::{Serialize, Deserialize};

/// Market venue type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum VenueModel {
    /// Constant product AMM (e.g., Uniswap v2)
    Cpmm,
    /// StableSwap AMM (e.g., Curve)
    StableSwap,
    /// Central Limit Order Book
    OrderBook,
}

/// Slot phase for time-based optimization
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SlotPhase {
    /// First ~200ms of the slot
    Early,
    /// Middle of the slot
    Mid,
    /// Last ~200ms of the slot
    Late,
}

/// Order book snapshot for impact modeling
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    /// Price impact per notional (basis points per ratio)
    pub slope_bps_per_ratio: f64,
    /// Convexity term for impact
    pub curvature: f64,
    /// Total notional depth near best levels
    pub depth_notional: f64,
}

/// CPMM (Constant Product Market Maker) pool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CpmmPool {
    pub base_reserve: f64,
    pub quote_reserve: f64,
    pub fee_bps: f64,
}

/// StableSwap pool
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StableSwapPool {
    pub reserve_a: f64,
    pub reserve_b: f64,
    pub amp: f64,
    pub fee_bps: f64,
}

/// Tracks empirical inclusion probability for different tip levels
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InclusionHistogram {
    tip_edges: Vec<u64>,
    cdf_early: Vec<f64>,
    cdf_mid: Vec<f64>,
    cdf_late: Vec<f64>,
    alpha: f64,
}

impl InclusionHistogram {
    pub fn new(tip_edges: Vec<u64>, alpha: f64) -> Self {
        let n = tip_edges.len();
        Self {
            tip_edges,
            cdf_early: vec![0.0; n],
            cdf_mid: vec![0.0; n],
            cdf_late: vec![0.0; n],
            alpha,
        }
    }

    pub fn update(&mut self, phase: SlotPhase, tip: u64, included: bool) {
        if self.tip_edges.is_empty() {
            return;
        }

        let idx = match self.tip_edges.binary_search(&tip) {
            Ok(i) => i,
            Err(i) => i.saturating_sub(1).min(self.tip_edges.len() - 1),
        };

        let obs = if included { 1.0 } else { 0.0 };
        let (cdf, alpha) = match phase {
            SlotPhase::Early => (&mut self.cdf_early, self.alpha),
            SlotPhase::Mid => (&mut self.cdf_mid, self.alpha),
            SlotPhase::Late => (&mut self.cdf_late, self.alpha),
        };

        // EWMA update
        cdf[idx] = cdf[idx] * (1.0 - alpha) + obs * alpha;
        self.enforce_monotonic(cdf);
    }

    pub fn p_inclusion(&self, phase: SlotPhase, tip: u64) -> f64 {
        if self.tip_edges.is_empty() {
            return 0.0;
        }

        let cdf = match phase {
            SlotPhase::Early => &self.cdf_early,
            SlotPhase::Mid => &self.cdf_mid,
            SlotPhase::Late => &self.cdf_late,
        };

        // Find the bucket for this tip
        let (mut lo_i, mut hi_i) = (0, self.tip_edges.len() - 1);
        match self.tip_edges.binary_search(&tip) {
            Ok(i) => {
                lo_i = i;
                hi_i = i;
            }
            Err(i) => {
                lo_i = i.saturating_sub(1).min(self.tip_edges.len() - 1);
                hi_i = i.min(self.tip_edges.len() - 1);
            }
        }

        if lo_i == hi_i {
            return cdf[lo_i];
        }

        // Linear interpolation
        let lo_tip = self.tip_edges[lo_i] as f64;
        let hi_tip = self.tip_edges[hi_i] as f64;
        let w = ((tip as f64 - lo_tip) / (hi_tip - lo_tip)).clamp(0.0, 1.0);
        let p = cdf[lo_i] * (1.0 - w) + cdf[hi_i] * w;
        p.clamp(0.0, 1.0)
    }

    fn enforce_monotonic(&self, cdf: &mut [f64]) {
        for i in 1..cdf.len() {
            if cdf[i] < cdf[i - 1] {
                cdf[i] = cdf[i - 1];
            }
        }
    }
}

/// Impact model configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImpactModel {
    pub incl_alpha: f64,
    pub impact_alpha: f64,
    pub incl_sigmoid_k: f64,
    pub incl_sigmoid_mid: f64,
    pub venue: VenueModel,
    pub ob_snapshot: Option<OrderBookSnapshot>,
    pub cpmm: Option<CpmmPool>,
    pub stableswap: Option<StableSwapPool>,
    pub incl_hist: InclusionHistogram,
}

impl Default for ImpactModel {
    fn default() -> Self {
        Self {
            incl_alpha: 0.15,
            impact_alpha: 0.20,
            incl_sigmoid_k: 2.1,
            incl_sigmoid_mid: 20_000.0,
            venue: VenueModel::OrderBook,
            ob_snapshot: None,
            cpmm: None,
            stableswap: None,
            incl_hist: InclusionHistogram::new(
                vec![5_000, 10_000, 15_000, 20_000, 30_000, 50_000],
                0.2,
            ),
        }
    }
}

impl ImpactModel {
    /// Calculate impact cost based on venue model
    pub fn calculate_impact_cost(&self, size: f64) -> f64 {
        match self.venue {
            VenueModel::Cpmm => self
                .cpmm
                .as_ref()
                .map(|p| self.cpmm_impact_cost(size, p))
                .unwrap_or_else(|| self.fallback_impact(size)),
            VenueModel::StableSwap => self
                .stableswap
                .as_ref()
                .map(|p| self.stableswap_impact_cost(size, p))
                .unwrap_or_else(|| self.fallback_impact(size)),
            VenueModel::OrderBook => self
                .ob_snapshot
                .as_ref()
                .map(|ob| self.orderbook_impact_cost(size, ob))
                .unwrap_or_else(|| self.fallback_impact(size)),
        }
    }

    fn fallback_impact(&self, _size: f64) -> f64 {
        // Default fallback impact
        0.001 // 0.1%
    }

    fn cpmm_impact_cost(&self, size: f64, pool: &CpmmPool) -> f64 {
        let x0 = pool.base_reserve;
        let y0 = pool.quote_reserve;
        if x0 <= 0.0 || y0 <= 0.0 {
            return 0.0;
        }

        let trade_ratio = (size / (y0 + 1.0)).clamp(0.0, 0.1);
        let fee = pool.fee_bps / 10_000.0;
        let effective = trade_ratio * (1.0 - fee);
        effective / (1.0 - effective + 1e-9).abs()
    }

    fn stableswap_impact_cost(&self, size: f64, pool: &StableSwapPool) -> f64 {
        let r = (pool.reserve_a + pool.reserve_b).max(1.0);
        let fee = pool.fee_bps / 10_000.0;
        let base = (size / (r * pool.amp.max(1.0))).clamp(0.0, 0.05);
        base.powf(1.1) * (1.0 + fee)
    }

    fn orderbook_impact_cost(&self, size: f64, ob: &OrderBookSnapshot) -> f64 {
        let depth = ob.depth_notional.max(1.0);
        let x = (size / depth).clamp(0.0, 0.2);
        let linear = ob.slope_bps_per_ratio / 10_000.0 * x;
        let convex = ob.curvature * x * x;
        (linear + convex).abs()
    }

    /// Calculate inclusion probability using histogram and fallback to sigmoid
    pub fn inclusion_probability(&self, phase: SlotPhase, tip: u64) -> f64 {
        let hist_p = self.incl_hist.p_inclusion(phase, tip);
        let sigmoid_p = self.sigmoid_inclusion(tip as f64);
        hist_p.max(sigmoid_p * 0.5) // Hybrid approach
    }

    fn sigmoid_inclusion(&self, tip: f64) -> f64 {
        1.0 / (1.0 + (-self.incl_sigmoid_k * (tip - self.incl_sigmoid_mid) / self.incl_sigmoid_mid).exp())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inclusion_histogram() {
        let mut hist = InclusionHistogram::new(vec![5_000, 10_000, 20_000], 0.5);
        
        // Update with some observations
        hist.update(SlotPhase::Mid, 5_000, true);
        hist.update(SlotPhase::Mid, 10_000, false);
        hist.update(SlotPhase::Mid, 15_000, true);
        
        // Check monotonicity
        let p1 = hist.p_inclusion(SlotPhase::Mid, 5_000);
        let p2 = hist.p_inclusion(SlotPhase::Mid, 10_000);
        let p3 = hist.p_inclusion(SlotPhase::Mid, 20_000);
        
        assert!(p1 <= p2 && p2 <= p3, "CDF should be non-decreasing");
    }

    #[test]
    fn test_impact_calculation() {
        let mut model = ImpactModel::default();
        
        // Test CPMM impact
        model.venue = VenueModel::Cpmm;
        model.cpmm = Some(CpmmPool {
            base_reserve: 1_000_000.0,
            quote_reserve: 1_000_000.0,
            fee_bps: 30.0,
        });
        
        let impact = model.calculate_impact_cost(10_000.0);
        assert!(impact > 0.0, "Impact should be positive");
        
        // Test OrderBook impact
        model.venue = VenueModel::OrderBook;
        model.ob_snapshot = Some(OrderBookSnapshot {
            slope_bps_per_ratio: 50.0,
            curvature: 0.3,
            depth_notional: 5_000_000.0,
        });
        
        let impact = model.calculate_impact_cost(100_000.0);
        assert!(impact > 0.0, "Impact should be positive");
    }
}
