// ============================== 
// CompetitiveEdgeScoringEngine.rs (HFT v4)
// ==============================

// [IMPROVED] Imports & CU instructions ready
use solana_sdk::{
    clock::Slot,
    pubkey::Pubkey,
    instruction::Instruction,
    compute_budget::ComputeBudgetInstruction,
};

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicU64, Ordering as Ato};

// [IMPROVED] Low-contention primitives
use parking_lot::Mutex;
use dashmap::DashMap;

// [NEW] Faster hasher for DashMap (desk-standard)
use ahash::RandomState;

// [NEW] Deterministic tie-break: XXH3
use xxhash_rust::xxh3::xxh3_64_with_seed;

// [NEW] Optional parallel scoring for large batches
#[cfg(feature = "parallel")]
use rayon::prelude::*;

// ------------------------------
// [FIX] Fixed-point micro (1e6) with saturation
// ------------------------------
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(transparent)]
struct Fp(i128);

impl Fp {
    const SCALE: i128 = 1_000_000;

    #[inline(always)] fn from_i64(x: i64) -> Self { Self((x as i128) * Self::SCALE) }
    #[inline(always)] fn from_u64(x: u64) -> Self { Self((x as i128) * Self::SCALE) }
    #[inline(always)] fn from_f64_clamped(x: f64) -> Self {
        if !x.is_finite() { return Self(0); }
        // clamp to i128 range conservatively
        let v = (x * (Self::SCALE as f64)).round()
            .clamp((i128::MIN + 1) as f64, (i128::MAX - 1) as f64);
        Self(v as i128)
    }
    #[inline(always)] fn to_f64(self) -> f64 { (self.0 as f64) / (Self::SCALE as f64) }

    #[inline(always)] fn saturating_add(self, other: Self) -> Self { Self(self.0.saturating_add(other.0)) }
    #[inline(always)] fn saturating_sub(self, other: Self) -> Self { Self(self.0.saturating_sub(other.0)) }

    // [FIX] (a*b)/SCALE with saturation
    #[inline(always)] fn saturating_mul(self, other: Self) -> Self {
        match self.0.checked_mul(other.0) {
            Some(p) => Self(p / Self::SCALE),
            None => {
                let neg = (self.0 ^ other.0) < 0;
                if neg { Self(i128::MIN + 1) } else { Self(i128::MAX) }
            }
        }
    }
    #[inline(always)] fn saturating_div(self, other: Self) -> Self {
        if other.0 == 0 { return Self(0); }
        match self.0.checked_mul(Self::SCALE) {
            Some(num) => Self(num / other.0),
            None => {
                let neg = (self.0 ^ other.0) < 0;
                if neg { Self(i128::MIN + 1) } else { Self(i128::MAX) }
            }
        }
    }
    #[inline(always)] fn clamp01(self) -> Self {
        if self.0 < 0 { return Self(0); }
        if self.0 > Self::SCALE { return Self(Self::SCALE); }
        self
    }
}
#[inline(always)] const fn fp_const(x: i64) -> Fp { Fp((x as i128) * Fp::SCALE) }
macro_rules! fp { ($x:expr) => { Fp::from_f64_clamped($x as f64) } }
const FP_0: Fp = Fp(0);
const FP_1: Fp = fp_const(1);
const FP_100: Fp = fp_const(100);

// ==============================
// Domain types (unchanged API)
// ==============================
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OpportunityType { Arbitrage, Liquidation, SandwichAttack, JitLiquidity, FlashLoan }

#[derive(Debug, Clone)]
pub struct OpportunityMetrics {
    pub opportunity_type: OpportunityType,
    pub gross_profit_lamports: u64,
    pub priority_fee_lamports: u64,
    pub compute_units: u32,
    pub accounts_touched: Vec<Pubkey>,
    pub competing_bots: u32,
    pub market_volatility: f64,
    pub liquidity_depth: u64,
    pub slippage_tolerance: f64,
    pub execution_complexity: u8,
    pub time_sensitivity_ms: u64,
    pub bundle_size: usize,
}

#[derive(Debug, Clone)]
pub struct NetworkConditions {
    pub slot: Slot,
    pub recent_blockhash_age: u64,   // slots since last blockhash
    pub network_congestion: f64,     // 0..1
    pub average_priority_fee: u64,   // lamports
    pub slot_success_rate: f64,      // 0..1
    pub leader_schedule: HashMap<Pubkey, Vec<Slot>>,
    pub rpc_latency_ms: u64,
}

#[derive(Debug, Clone)]
pub struct HistoricalPerformance {
    pub success_count: u64,
    pub failure_count: u64,
    pub total_profit: i64, // lamports net
    pub average_priority_fee: u64,
    pub average_execution_time_ms: u64,
    pub revert_reasons: HashMap<String, u32>,
}

#[derive(Debug, Clone)]
pub struct CompetitorAnalysis {
    pub bot_pubkey: Pubkey,
    pub success_rate: f64,        // 0..1
    pub average_priority_fee: u64,// lamports
    pub typical_strategies: Vec<OpportunityType>,
    pub reaction_time_ms: u64,
    pub market_share: f64,        // 0..1
}

// ==============================
// [IMPROVED] Weights & Risk Params — fixed-point
// ==============================
#[derive(Debug, Clone)]
struct ScoringWeights {
    profit_weight: Fp,
    success_probability_weight: Fp,
    competition_weight: Fp,
    timing_weight: Fp,
    complexity_weight: Fp,
    network_condition_weight: Fp,
    historical_performance_weight: Fp,
}
impl Default for ScoringWeights {
    #[inline(always)]
    fn default() -> Self {
        Self {
            profit_weight: fp!(0.35),
            success_probability_weight: fp!(0.25),
            competition_weight: fp!(0.15),
            timing_weight: fp!(0.10),
            complexity_weight: fp!(0.05),
            network_condition_weight: fp!(0.05),
            historical_performance_weight: fp!(0.05),
        }
    }
}

#[derive(Debug, Clone)]
struct RiskParameters {
    max_priority_fee_ratio: Fp,    // relative to gross profit
    min_profit_threshold: u64,     // lamports
    max_compute_units: u32,
    max_accounts_per_tx: usize,
    volatility_penalty_factor: Fp,
    congestion_penalty_factor: Fp,
    failure_penalty_multiplier: Fp,
}
impl Default for RiskParameters {
    #[inline(always)]
    fn default() -> Self {
        Self {
            max_priority_fee_ratio: fp!(0.30),
            min_profit_threshold: 100_000,     // 0.0001 SOL
            max_compute_units: 1_400_000,
            max_accounts_per_tx: 64,
            volatility_penalty_factor: fp!(0.02),
            congestion_penalty_factor: fp!(0.03),
            failure_penalty_multiplier: fp!(2.5),
        }
    }
}

// ==============================
// [IMPROVED] Low-latency EWMA for slot performance (lock-free)
// ==============================
#[derive(Default)]
struct SlotPerfEwma { ewma: AtomicI64 } // micro-scaled Fp stored in i64

impl SlotPerfEwma {
    #[inline(always)]
    fn update(&self, perf_0_to_1: f64, alpha: f64) {
        let new_val = Fp::from_f64_clamped(perf_0_to_1);
        let a = Fp::from_f64_clamped(alpha);
        let one_minus_a = FP_1.saturating_sub(a);
        loop {
            let cur_i = self.ewma.load(Ato::Relaxed);
            let cur = Fp(cur_i as i128);
            let next = cur.saturating_mul(one_minus_a).saturating_add(new_val.saturating_mul(a));
            if self.ewma.compare_exchange(cur_i, next.0 as i64, Ato::AcqRel, Ato::Relaxed).is_ok() { break; }
        }
    }
    #[inline(always)] fn get(&self) -> Fp { Fp(self.ewma.load(Ato::Acquire) as i128) }
}

// ==============================
// [NEW] Bayesian prior for success (desk-grade stabilization)
// ==============================
#[derive(Default)]
struct BetaPrior {
    alpha: AtomicU64,
    beta:  AtomicU64,
}
impl BetaPrior {
    #[inline(always)] fn mean(&self) -> Fp {
        let a = self.alpha.load(Ato::Acquire).max(1);
        let b = self.beta.load(Ato::Acquire).max(1);
        Fp::from_u64(a).saturating_div(Fp::from_u64(a + b)).clamp01()
    }
    #[inline(always)] fn update(&self, success: bool) {
        if success { self.alpha.fetch_add(1, Ato::Relaxed); }
        else { self.beta.fetch_add(1, Ato::Relaxed); }
    }
}

// ==============================
// [IMPROVED] Engine with O(1) competitor stats + AHash
// ==============================
pub struct CompetitiveEdgeScoringEngine {
    // [HFT] Sharded maps with ahash for throughput
    historical_data: DashMap<OpportunityType, HistoricalPerformance, RandomState>,
    competitor_data: DashMap<Pubkey, CompetitorAnalysis, RandomState>,

    slot_perf_ewma: SlotPerfEwma,
    scoring_weights: ScoringWeights,
    risk_parameters: RiskParameters,

    // [NEW] O(1) per-strategy hot stats
    active_competitors_by_strategy: DashMap<OpportunityType, AtomicU32, RandomState>,
    avg_fee_ewma_by_strategy:      DashMap<OpportunityType, AtomicU64, RandomState>,

    // [NEW] Beta priors per strategy
    prior_by_strategy: DashMap<OpportunityType, BetaPrior, RandomState>,

    // [IMPROVED] Debug ring (not hot path, feature-gated)
    #[cfg(feature = "debug_scores")]
    recent_scores_dbg: Mutex<VecDeque<(Slot, OpportunityType, i64)>>,
}

// ==============================
// [NEW] FeeShaper: CU price + total tip suggestion
// ==============================
pub struct FeeShaper;
impl FeeShaper {
    #[inline(always)]
    pub fn suggest(m: &OpportunityMetrics, n: &NetworkConditions, score_0_to_100: Fp, avg_comp_fee: u64) -> (u64, u64) {
        let base = if avg_comp_fee > 0 { avg_comp_fee } else { n.average_priority_fee };

        // [HFT] Quadratic congestion fit
        let cong = Fp::from_f64_clamped(n.network_congestion).clamp01().to_f64();
        let cong_mult = 1.0 + 1.5 * cong * cong;

        // [HFT] Score shading
        let s = (score_0_to_100.to_f64() / 100.0).clamp(0.0, 1.0);
        let shade = 0.8 + 0.7 * s;

        // [HFT] Leader proximity discount
        let leader_discount = if is_leader_within(n, 2) { 0.92 } else { 1.0 };

        let total_tip = ((base as f64) * cong_mult * shade * leader_discount)
            .round()
            .max(2_000.0)
            .min(5_000_000.0) as u64;

        // [HFT] Stable CU-price from total_tip and CU usage
        let cu = m.compute_units.max(50_000) as u64;
        let cu_price = ((total_tip as u128 * 1_000_000u128) / (cu as u128)) as u64;

        (cu_price, total_tip)
    }
}

// ==============================
// [NEW] RiskBudget: slot-scoped atomic budget (attempts + lamports)
// ==============================
pub struct RiskBudget {
    last_slot: AtomicU64,
    attempts_left: AtomicU32,
    lamports_left: AtomicU64,
    max_attempts_per_slot: u32,
    max_lamports_per_slot: u64,
}
impl RiskBudget {
    #[inline(always)]
    pub fn new(max_attempts_per_slot: u32, max_lamports_per_slot: u64) -> Self {
        Self {
            last_slot: AtomicU64::new(0),
            attempts_left: AtomicU32::new(max_attempts_per_slot),
            lamports_left: AtomicU64::new(max_lamports_per_slot),
            max_attempts_per_slot,
            max_lamports_per_slot,
        }
    }
    #[inline(always)]
    pub fn on_slot(&self, slot: Slot) {
        let prev = self.last_slot.swap(slot as u64, Ato::Relaxed);
        if prev != slot as u64 {
            self.attempts_left.store(self.max_attempts_per_slot, Ato::Release);
            self.lamports_left.store(self.max_lamports_per_slot, Ato::Release);
        }
    }
    #[inline(always)]
    pub fn try_consume(&self, attempts: u32, lamports: u64) -> bool {
        // attempts
        loop {
            let a = self.attempts_left.load(Ato::Acquire);
            if a < attempts { return false; }
            if self.attempts_left.compare_exchange(a, a - attempts, Ato::AcqRel, Ato::Relaxed).is_ok() { break; }
        }
        // lamports
        loop {
            let l = self.lamports_left.load(Ato::Acquire);
            if l < lamports {
                self.attempts_left.fetch_add(attempts, Ato::Release);
                return false;
            }
            if self.lamports_left.compare_exchange(l, l - lamports, Ato::AcqRel, Ato::Relaxed).is_ok() { break; }
        }
        true
    }
    // [NEW] refund on failed inclusion → prevents budget starvation
    #[inline(always)]
    pub fn refund(&self, attempts: u32, lamports: u64) {
        self.attempts_left.fetch_add(attempts, Ato::Release);
        self.lamports_left.fetch_add(lamports, Ato::Release);
    }
    #[inline(always)] pub fn lamports_left(&self) -> u64 { self.lamports_left.load(Ato::Acquire) }
}

// ==============================
// Engine impl
// ==============================
impl CompetitiveEdgeScoringEngine {
    #[inline(always)]
    pub fn new() -> Self {
        let hasher = RandomState::default();
        Self {
            historical_data: DashMap::with_hasher(hasher.clone()),
            competitor_data: DashMap::with_hasher(hasher.clone()),
            slot_perf_ewma: SlotPerfEwma::default(),
            scoring_weights: ScoringWeights::default(),
            risk_parameters: RiskParameters::default(),
            active_competitors_by_strategy: DashMap::with_hasher(hasher.clone()),
            avg_fee_ewma_by_strategy:      DashMap::with_hasher(hasher.clone()),
            prior_by_strategy:             DashMap::with_hasher(hasher),
            #[cfg(feature = "debug_scores")]
            recent_scores_dbg: Mutex::new(VecDeque::with_capacity(256)),
        }
    }

    // ==========================
    // Core API
    // ==========================
    #[inline(always)]
    pub fn score_opportunity(&self, metrics: &OpportunityMetrics, network: &NetworkConditions) -> Fp {
        if !self.validate_opportunity(metrics) { return FP_0; }

        // hot-path, branch-light scoring
        let profit_score         = self.calculate_profit_score(metrics);
        let success_probability  = self.calculate_success_probability(metrics, network);
        let competition_score    = self.calculate_competition_score(metrics);
        let timing_score         = self.calculate_timing_score(metrics, network);
        let complexity_score     = self.calculate_complexity_score(metrics);
        let network_score        = self.calculate_network_score(network);
        let historical_score     = self.calculate_historical_score(metrics);

        let weighted = profit_score.saturating_mul(self.scoring_weights.profit_weight)
            .saturating_add(success_probability.saturating_mul(self.scoring_weights.success_probability_weight))
            .saturating_add(competition_score.saturating_mul(self.scoring_weights.competition_weight))
            .saturating_add(timing_score.saturating_mul(self.scoring_weights.timing_weight))
            .saturating_add(complexity_score.saturating_mul(self.scoring_weights.complexity_weight))
            .saturating_add(network_score.saturating_mul(self.scoring_weights.network_condition_weight))
            .saturating_add(historical_score.saturating_mul(self.scoring_weights.historical_performance_weight));

        // weights sum to ~1.0 in Fp → result already in [0..100]
        let base = weighted;
        let risk = self.apply_risk_adjustments(base, metrics, network);

        // [FIX] correct clamp (previous had FP_100.0 float typo)
        let risk_clamped = if risk.0 < 0 { FP_0 } else if risk.0 > FP_100.0 { FP_100 } else { risk };

        #[cfg(feature = "debug_scores")]
        {
            let mut q = self.recent_scores_dbg.lock();
            if q.len() == 256 { q.pop_front(); }
            q.push_back((network.slot, metrics.opportunity_type, risk_clamped.0 as i64));
        }
        risk_clamped
    }

    // [NEW] one-shot fee advice (CU price + total tip)
    #[inline(always)]
    pub fn suggest_fees(&self, m: &OpportunityMetrics, n: &NetworkConditions, score: Fp) -> (u64, u64) {
        let avg_comp_fee = self.avg_fee_ewma_by_strategy
            .get(&m.opportunity_type)
            .map(|a| a.load(Ato::Acquire) as u64)
            .unwrap_or(0);
        FeeShaper::suggest(m, n, score, avg_comp_fee)
    }

    // [NEW] Build ComputeBudget ixs deterministically (stable ordering)
    #[inline(always)]
    pub fn build_compute_budget_ixs(&self, m: &OpportunityMetrics, n: &NetworkConditions, score: Fp) -> [Instruction; 2] {
        let (cu_price, _tip) = self.suggest_fees(m, n, score);
        // [HFT] Round CU limit up to nearest 10k for CU-cache friendliness
        let cu_limit = ((m.compute_units + 9_999) / 10_000) * 10_000;
        [
            ComputeBudgetInstruction::set_compute_unit_limit(cu_limit),
            ComputeBudgetInstruction::set_compute_unit_price(cu_price),
        ]
    }

    #[inline(always)]
    fn validate_opportunity(&self, metrics: &OpportunityMetrics) -> bool {
        if metrics.compute_units > self.risk_parameters.max_compute_units { return false; }
        if metrics.accounts_touched.len() > self.risk_parameters.max_accounts_per_tx { return false; }

        let net_profit = metrics.gross_profit_lamports.saturating_sub(metrics.priority_fee_lamports);
        if net_profit < self.risk_parameters.min_profit_threshold { return false; }

        let fee_ratio = Fp::from_u64(metrics.priority_fee_lamports)
            .saturating_div(Fp::from_u64(metrics.gross_profit_lamports.max(1)));
        if fee_ratio.0 > self.risk_parameters.max_priority_fee_ratio.0 { return false; }
        true
    }

    // --------------------------
    // Scoring components
    // --------------------------
    #[inline(always)]
    fn calculate_profit_score(&self, m: &OpportunityMetrics) -> Fp {
        let net_profit = m.gross_profit_lamports.saturating_sub(m.priority_fee_lamports);
        let mut base = Fp::from_u64(net_profit)
            .saturating_div(Fp::from_u64(1_000_000_000)) // 1 SOL anchor
            .saturating_mul(FP_100);
        if base.0 > FP_100.0 { base = FP_100; }

        match m.opportunity_type {
            OpportunityType::Arbitrage => {
                let vol = Fp::from_f64_clamped(m.market_volatility);
                base.saturating_mul(FP_1.saturating_sub(vol.saturating_mul(self.risk_parameters.volatility_penalty_factor)))
            }
            OpportunityType::Liquidation => {
                let ts = Fp::from_u64(m.time_sensitivity_ms.max(100));
                let urg = FP_1.saturating_add(Fp::from_u64(1000).saturating_div(ts));
                base.saturating_mul(if urg.0 > fp!(1.5).0 { fp!(1.5) } else { urg })
            }
            OpportunityType::SandwichAttack => {
                let slip = Fp::from_f64_clamped(m.slippage_tolerance).clamp01();
                base.saturating_mul(FP_1.saturating_sub(slip))
            }
            OpportunityType::JitLiquidity => {
                let liq = Fp::from_u64(m.liquidity_depth).saturating_div(Fp::from_u64(1_000_000_000));
                let bonus = if liq.0 > fp!(1.2).0 { fp!(1.2) } else { liq };
                base.saturating_mul(bonus)
            }
            OpportunityType::FlashLoan => {
                let cx = Fp::from_i64(m.execution_complexity as i64).saturating_div(fp!(100.0));
                base.saturating_mul(FP_1.saturating_sub(cx))
            }
        }
    }

    #[inline(always)]
    fn calculate_success_probability(&self, m: &OpportunityMetrics, n: &NetworkConditions) -> Fp {
        // [NEW] Bayesian prior blended with empirical SR
        let prior = self.prior_by_strategy.get(&m.opportunity_type).map(|p| p.mean()).unwrap_or(fp!(0.5));

        let hist_p = if let Some(perf) = self.historical_data.get(&m.opportunity_type) {
            let total = perf.success_count + perf.failure_count;
            if total > 0 { Fp::from_u64(perf.success_count).saturating_div(Fp::from_u64(total)) } else { prior }
        } else { prior };

        let compute = FP_1.saturating_sub(
            Fp::from_u64(m.compute_units as u64)
                .saturating_div(Fp::from_u64(self.risk_parameters.max_compute_units as u64))
                .saturating_mul(fp!(0.3))
        );

        let netw = Fp::from_f64_clamped(n.slot_success_rate).clamp01();
        let comp = FP_1.saturating_div(Fp::from_u64(1).saturating_add(Fp::from_u64(m.competing_bots as u64).saturating_mul(fp!(0.1))));
        let timing = if m.time_sensitivity_ms < 100 { fp!(0.7) }
                     else if m.time_sensitivity_ms < 500 { fp!(0.85) } else { fp!(0.95) };
        let leader_boost = if is_leader_within(n, 2) { fp!(1.10) } else { FP_1 };

        hist_p.saturating_mul(compute)
              .saturating_mul(netw)
              .saturating_mul(comp)
              .saturating_mul(timing)
              .saturating_mul(leader_boost)
              .clamp01()
              .saturating_mul(FP_100)
    }

    #[inline(always)]
    fn calculate_competition_score(&self, m: &OpportunityMetrics) -> Fp {
        let active = self.active_competitors_by_strategy
            .get(&m.opportunity_type)
            .map(|x| x.load(Ato::Acquire) as usize)
            .unwrap_or(0);

        let intensity = Fp::from_u64((active + m.competing_bots as usize) as u64);
        let base = FP_100.saturating_div(Fp::from_u64(1).saturating_add(intensity.saturating_mul(fp!(0.2))));

        let avg_comp_fee = self.avg_fee_ewma_by_strategy
            .get(&m.opportunity_type)
            .map(|a| a.load(Ato::Acquire) as u64)
            .unwrap_or(0);

        let fee_adj = if m.priority_fee_lamports == 0 {
            fp!(0.5)
        } else if avg_comp_fee > 0 && m.priority_fee_lamports >= avg_comp_fee {
            let delta = Fp::from_u64(m.priority_fee_lamports - avg_comp_fee)
                .saturating_div(Fp::from_u64(avg_comp_fee.max(1)))
                .saturating_mul(fp!(0.2));
            FP_1.saturating_add(if delta.0 > fp!(0.5).0 { fp!(0.5) } else { delta })
        } else if avg_comp_fee > 0 {
            let delta = Fp::from_u64(avg_comp_fee - m.priority_fee_lamports)
                .saturating_div(Fp::from_u64(avg_comp_fee.max(1)))
                .saturating_mul(fp!(0.3));
            let adj = FP_1.saturating_sub(delta);
            if adj.0 < fp!(0.5).0 { fp!(0.5) } else { adj }
        } else { FP_1 };

        base.saturating_mul(fee_adj)
    }

    #[inline(always)]
    fn calculate_timing_score(&self, m: &OpportunityMetrics, n: &NetworkConditions) -> Fp {
        let latency_pen = Fp::from_u64(n.rpc_latency_ms).saturating_div(fp!(1000.0));
        let time_press  = Fp::from_u64(m.time_sensitivity_ms).saturating_div(fp!(10000.0));
        let blockhash_fresh = FP_1.saturating_sub(Fp::from_u64(n.recent_blockhash_age).saturating_div(fp!(150.0)));

        let exec_window = match m.opportunity_type {
            OpportunityType::Arbitrage       => fp!(0.95),
            OpportunityType::SandwichAttack  => fp!(0.85),
            OpportunityType::Liquidation     => fp!(0.90),
            OpportunityType::JitLiquidity    => fp!(0.80),
            OpportunityType::FlashLoan       => fp!(0.88),
        };

        let mut score = exec_window.saturating_sub(latency_pen).saturating_sub(time_press);
        if score.0 < 0 { score = FP_0; }
        score.saturating_mul(blockhash_fresh).saturating_mul(FP_100)
    }

    #[inline(always)]
    fn calculate_complexity_score(&self, m: &OpportunityMetrics) -> Fp {
        let acc_cx = FP_1.saturating_sub(
            Fp::from_u64(m.accounts_touched.len() as u64)
                .saturating_div(Fp::from_u64(self.risk_parameters.max_accounts_per_tx as u64))
                .saturating_mul(fp!(0.4))
        );
        let cu_cx = FP_1.saturating_sub(
            Fp::from_u64(m.compute_units as u64)
                .saturating_div(Fp::from_u64(self.risk_parameters.max_compute_units as u64))
                .saturating_mul(fp!(0.3))
        );
        let ex_cx = FP_1.saturating_sub(
            Fp::from_u64(m.execution_complexity as u64)
                .saturating_div(fp!(10.0))
                .saturating_mul(fp!(0.3))
        );
        let bundle_cx = {
            let raw = FP_1.saturating_sub(
                Fp::from_u64(m.bundle_size as u64)
                    .saturating_div(fp!(5.0))
                    .saturating_mul(fp!(0.2))
            );
            if raw.0 < fp!(0.6).0 { fp!(0.6) } else { raw }
        };

        acc_cx.saturating_mul(fp!(0.3))
             .saturating_add(cu_cx.saturating_mul(fp!(0.3)))
             .saturating_add(ex_cx.saturating_mul(fp!(0.2)))
             .saturating_add(bundle_cx.saturating_mul(fp!(0.2)))
             .saturating_mul(FP_100)
    }

    #[inline(always)]
    fn calculate_network_score(&self, n: &NetworkConditions) -> Fp {
        let cong = Fp::from_f64_clamped(n.network_congestion);
        let cong_factor = FP_1.saturating_sub(cong.saturating_mul(self.risk_parameters.congestion_penalty_factor));
        let ewma = self.slot_perf_ewma.get();
        let recent = if ewma.0 == 0 { fp!(0.5) } else { ewma };
        let fee_env = if n.average_priority_fee > 0 {
            let ratio = Fp::from_u64(n.average_priority_fee).saturating_div(fp!(1_000_000.0));
            FP_1.saturating_sub(if ratio.0 > fp!(0.5).0 { fp!(0.5) } else { ratio })
        } else { FP_1 };

        cong_factor.saturating_mul(recent).saturating_mul(fee_env).saturating_mul(FP_100)
    }

    #[inline(always)]
    fn calculate_historical_score(&self, m: &OpportunityMetrics) -> Fp {
        if let Some(perf) = self.historical_data.get(&m.opportunity_type) {
            let total = perf.success_count + perf.failure_count;
            if total == 0 { return fp!(50.0); }

            let sr = Fp::from_u64(perf.success_count).saturating_div(Fp::from_u64(total));
            let avg_profit = {
                let ap = perf.total_profit / (total as i64);
                if ap > 0 {
                    let p = Fp::from_i64(ap).saturating_div(Fp::from_u64(1_000_000));
                    if p.0 > FP_1.0 { FP_1 } else { p }
                } else { FP_0 }
            };
            let fee_eff = if perf.average_priority_fee > 0 && perf.total_profit > 0 {
                let fee = Fp::from_u64(perf.average_priority_fee);
                let mut per_attempt = perf.total_profit / (total as i64);
                if per_attempt == 0 { per_attempt = 1; }
                let prof = Fp::from_i64(per_attempt);
                let r = fee.saturating_div(prof);
                let one_minus = FP_1.saturating_sub(r);
                if one_minus.0 < 0 { FP_0 } else { one_minus }
            } else { fp!(0.5) };

            sr.saturating_mul(fp!(0.5))
              .saturating_add(avg_profit.saturating_mul(fp!(0.3)))
              .saturating_add(fee_eff.saturating_mul(fp!(0.2)))
              .saturating_mul(FP_100)
        } else { fp!(50.0) }
    }

    #[inline(always)]
    fn apply_risk_adjustments(&self, base: Fp, m: &OpportunityMetrics, n: &NetworkConditions) -> Fp {
        let mut s = base;

        let vol = Fp::from_f64_clamped(m.market_volatility);
        let vol_pen = FP_1.saturating_sub(vol.saturating_mul(self.risk_parameters.volatility_penalty_factor));
        s = s.saturating_mul(if vol_pen.0 < fp!(0.7).0 { fp!(0.7) } else { vol_pen });

        let cong = Fp::from_f64_clamped(n.network_congestion);
        let cong_pen = FP_1.saturating_sub(cong.saturating_mul(self.risk_parameters.congestion_penalty_factor));
        s = s.saturating_mul(if cong_pen.0 < fp!(0.8).0 { fp!(0.8) } else { cong_pen });

        if m.competing_bots > 5 {
            let pen = Fp::from_u64((m.competing_bots - 5) as u64).saturating_mul(fp!(0.02));
            let factor = FP_1.saturating_sub(if pen.0 > fp!(0.3).0 { fp!(0.3) } else { pen });
            s = s.saturating_mul(factor);
        }

        if let Some(perf) = self.historical_data.get(&m.opportunity_type) {
            if perf.failure_count > perf.success_count.saturating_mul(2) {
                let fr = Fp::from_u64(perf.failure_count).saturating_div(Fp::from_u64(perf.success_count.max(1)));
                let pen = fr.saturating_sub(fp!(2.0)).saturating_mul(fp!(0.1));
                let factor = FP_1.saturating_sub(if pen.0 > fp!(0.5).0 { fp!(0.5) } else { pen });
                s = s.saturating_mul(factor);
            }
        }

        if m.execution_complexity > 7 {
            let pen = Fp::from_u64((m.execution_complexity as u64 - 7)).saturating_mul(fp!(0.05));
            let factor = FP_1.saturating_sub(if pen.0 > fp!(0.2).0 { fp!(0.2) } else { pen });
            s = s.saturating_mul(factor);
        }
        s
    }

    // --------------------------
    // Online updates (thread-safe)
    // --------------------------
    pub fn update_historical_performance(
        &self,
        opportunity_type: OpportunityType,
        success: bool,
        profit_lamports: i64,
        priority_fee: u64,
        execution_time_ms: u64,
        revert_reason: Option<String>,
    ) {
        let mut entry = self.historical_data.entry(opportunity_type)
            .or_insert_with(|| HistoricalPerformance {
                success_count: 0, failure_count: 0, total_profit: 0,
                average_priority_fee: 0, average_execution_time_ms: 0,
                revert_reasons: HashMap::new(),
            });

        if success { entry.success_count = entry.success_count.saturating_add(1); }
        else {
            entry.failure_count = entry.failure_count.saturating_add(1);
            if let Some(r) = revert_reason { *entry.revert_reasons.entry(r).or_insert(0) += 1; }
        }

        entry.total_profit = entry.total_profit.saturating_add(profit_lamports);
        let total_attempts = entry.success_count.saturating_add(entry.failure_count).max(1);
        entry.average_priority_fee = ((entry.average_priority_fee as u128 * (total_attempts as u128 - 1))
            .saturating_add(priority_fee as u128) / (total_attempts as u128)) as u64;
        entry.average_execution_time_ms = ((entry.average_execution_time_ms as u128 * (total_attempts as u128 - 1))
            .saturating_add(execution_time_ms as u128) / (total_attempts as u128)) as u64;

        // [NEW] update prior
        let prior = self.prior_by_strategy
            .entry(opportunity_type)
            .or_insert_with(BetaPrior::default);
        prior.update(success);
    }

    pub fn update_competitor_analysis(
        &self,
        bot_pubkey: Pubkey,
        success_rate: f64,
        priority_fee: u64,
        strategy: OpportunityType,
        reaction_time_ms: u64,
    ) {
        let mut comp = self.competitor_data.entry(bot_pubkey).or_insert_with(|| CompetitorAnalysis {
            bot_pubkey,
            success_rate: 0.0,
            average_priority_fee: 0,
            typical_strategies: Vec::new(),
            reaction_time_ms: 0,
            market_share: 0.0,
        });

        // [HFT] EWMA with conservative decay
        comp.success_rate = comp.success_rate * 0.9 + success_rate * 0.1;
        comp.average_priority_fee = (((comp.average_priority_fee as f64) * 0.9) + (priority_fee as f64) * 0.1) as u64;

        // [NEW] per-strategy counters & fee-EMAs
        if !comp.typical_strategies.contains(&strategy) {
            comp.typical_strategies.push(strategy);
            let ctr = self.active_competitors_by_strategy.entry(strategy).or_insert_with(|| AtomicU32::new(0));
            ctr.fetch_add(1, Ato::Relaxed);
        }
        let fee_atom = self.avg_fee_ewma_by_strategy.entry(strategy).or_insert_with(|| AtomicU64::new(priority_fee));
        loop {
            let old = fee_atom.load(Ato::Relaxed);
            let new = (old.saturating_mul(9) + priority_fee) / 10;
            if fee_atom.compare_exchange(old, new, Ato::AcqRel, Ato::Relaxed).is_ok() { break; }
        }

        comp.reaction_time_ms = (((comp.reaction_time_ms as f64) * 0.9) + (reaction_time_ms as f64) * 0.1) as u64;

        // normalize market share (cold path)
        let total_activity: f64 = self.competitor_data.iter().map(|c| c.success_rate).sum::<f64>().max(1.0);
        for mut c in self.competitor_data.iter_mut() {
            c.market_share = c.success_rate / total_activity;
        }
    }

    #[inline(always)]
    pub fn update_slot_performance(&self, _slot: Slot, performance_0_to_1: f64) {
        self.slot_perf_ewma.update(performance_0_to_1, 0.2);
    }

    // [IMPROVED] Partial sort top-K with deterministic tie-break (XXH3)
    #[inline(always)]
    pub fn get_opportunity_ranking(
        &self,
        opportunities: Vec<OpportunityMetrics>,
        network: &NetworkConditions,
    ) -> Vec<(usize, Fp)> {
        let mut scored: Vec<(usize, Fp, u64)> = opportunities
            .iter()
            .enumerate()
            .map(|(i, m)| {
                let s = self.score_opportunity(m, network);
                let seed = tie_break_seed(m, network.slot);
                (i, s, seed)
            })
            .filter(|(_, s, _)| s.0 > 0)
            .collect();

        const K: usize = 10;
        if scored.len() > K {
            scored.select_nth_unstable_by(K - 1, |a, b| match b.1 .0.cmp(&a.1 .0) {
                std::cmp::Ordering::Equal => a.2.cmp(&b.2),
                o => o,
            });
            scored.truncate(K);
        }
        scored.sort_unstable_by(|a, b| match b.1 .0.cmp(&a.1 .0) {
            std::cmp::Ordering::Equal => a.2.cmp(&b.2),
            o => o,
        });
        scored.into_iter().map(|(i, s, _)| (i, s)).collect()
    }

    // [NEW] Deterministic parallel version (feature "parallel")
    #[cfg(feature = "parallel")]
    pub fn get_opportunity_ranking_par(
        &self,
        opportunities: Vec<OpportunityMetrics>,
        network: &NetworkConditions,
    ) -> Vec<(usize, Fp)> {
        let mut scored: Vec<(usize, Fp, u64)> = opportunities
            .par_iter()
            .enumerate()
            .map(|(i, m)| {
                let s = self.score_opportunity(m, network);
                let seed = tie_break_seed(m, network.slot);
                (i, s, seed)
            })
            .filter(|(_, s, _)| s.0 > 0)
            .collect();

        const K: usize = 10;
        if scored.len() > K {
            scored.select_nth_unstable_by(K - 1, |a, b| match b.1 .0.cmp(&a.1 .0) {
                std::cmp::Ordering::Equal => a.2.cmp(&b.2),
                o => o,
            });
            scored.truncate(K);
        }
        scored.sort_unstable_by(|a, b| match b.1 .0.cmp(&a.1 .0) {
            std::cmp::Ordering::Equal => a.2.cmp(&b.2),
            o => o,
        });
        scored.into_iter().map(|(i, s, _)| (i, s)).collect()
    }

    // [HFT] Branch-light gating
    #[inline(always)]
    pub fn should_execute_opportunity(&self, score: Fp, m: &OpportunityMetrics, n: &NetworkConditions) -> bool {
        if score.0 < fp!(30.0).0 { return false; }
        let net_profit = m.gross_profit_lamports.saturating_sub(m.priority_fee_lamports);
        if net_profit < self.risk_parameters.min_profit_threshold { return false; }
        if n.network_congestion > 0.85 && score.0 < fp!(70.0).0 { return false; }
        if m.competing_bots > 10 && score.0 < fp!(60.0).0 { return false; }

        if let Some(perf) = self.historical_data.get(&m.opportunity_type) {
            let total = perf.success_count + perf.failure_count;
            if total > 100 {
                let sr = perf.success_count as f64 / total as f64;
                if sr < 0.3 && score.0 < fp!(80.0).0 { return false; }
            }
        }
        true
    }

    // [IMPROVED] Stable renorm of weights
    pub fn adjust_weights(&mut self, performance_feedback: HashMap<OpportunityType, f64>) {
        for (_opp, perf) in performance_feedback.into_iter() {
            let adj = Fp::from_f64_clamped(perf);
            if adj.0 > fp!(1.2).0 {
                self.scoring_weights.profit_weight = self.scoring_weights.profit_weight.saturating_mul(fp!(1.05));
                self.scoring_weights.success_probability_weight = self.scoring_weights.success_probability_weight.saturating_mul(fp!(0.95));
            } else if adj.0 < fp!(0.8).0 {
                self.scoring_weights.profit_weight = self.scoring_weights.profit_weight.saturating_mul(fp!(0.95));
                self.scoring_weights.success_probability_weight = self.scoring_weights.success_probability_weight.saturating_mul(fp!(1.05));
            }
        }
        let sum = self.scoring_weights.profit_weight
            .saturating_add(self.scoring_weights.success_probability_weight)
            .saturating_add(self.scoring_weights.competition_weight)
            .saturating_add(self.scoring_weights.timing_weight)
            .saturating_add(self.scoring_weights.complexity_weight)
            .saturating_add(self.scoring_weights.network_condition_weight)
            .saturating_add(self.scoring_weights.historical_performance_weight);

        if sum.0 > 0 {
            self.scoring_weights.profit_weight = self.scoring_weights.profit_weight.saturating_div(sum);
            self.scoring_weights.success_probability_weight = self.scoring_weights.success_probability_weight.saturating_div(sum);
            self.scoring_weights.competition_weight = self.scoring_weights.competition_weight.saturating_div(sum);
            self.scoring_weights.timing_weight = self.scoring_weights.timing_weight.saturating_div(sum);
            self.scoring_weights.complexity_weight = self.scoring_weights.complexity_weight.saturating_div(sum);
            self.scoring_weights.network_condition_weight = self.scoring_weights.network_condition_weight.saturating_div(sum);
            self.scoring_weights.historical_performance_weight = self.scoring_weights.historical_performance_weight.saturating_div(sum);
        }
    }
}

// ==============================
// [NEW] Helpers
// ==============================
#[inline(always)]
fn is_leader_within(n: &NetworkConditions, within_slots: u64) -> bool {
    let cur = n.slot;
    for slots in n.leader_schedule.values() {
        for &s in slots {
            if s >= cur && s <= cur + within_slots { return true; }
        }
    }
    false
}

#[inline(always)]
fn tie_break_seed(m: &OpportunityMetrics, slot: Slot) -> u64 {
    // stable 48-byte buffer
    let mut buf = [0u8; 48];
    buf[0] = match m.opportunity_type {
        OpportunityType::Arbitrage => 1,
        OpportunityType::Liquidation => 2,
        OpportunityType::SandwichAttack => 3,
        OpportunityType::JitLiquidity => 4,
        OpportunityType::FlashLoan => 5,
    };
    if let Some(pk) = m.accounts_touched.get(0) {
        buf[1..33].copy_from_slice(pk.as_ref());
    }
    buf[33..41].copy_from_slice(&m.gross_profit_lamports.to_le_bytes());
    let seed = (slot as u64).wrapping_mul(0x9E3779B185EBCA87);
    xxh3_64_with_seed(&buf, seed)
}

// ==============================
// Minimal tests
// ==============================
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_engine_init() {
        let eng = CompetitiveEdgeScoringEngine::new();
        assert!(eng.historical_data.is_empty());
        assert!(eng.competitor_data.is_empty());
    }

    #[test]
    fn test_score_and_fee_and_ixs() {
        let eng = CompetitiveEdgeScoringEngine::new();
        let m = OpportunityMetrics {
            opportunity_type: OpportunityType::Arbitrage,
            gross_profit_lamports: 2_000_000,
            priority_fee_lamports: 50_000,
            compute_units: 300_000,
            accounts_touched: vec![],
            competing_bots: 2,
            market_volatility: 0.2,
            liquidity_depth: 50_000_000,
            slippage_tolerance: 0.03,
            execution_complexity: 3,
            time_sensitivity_ms: 600,
            bundle_size: 2,
        };
        let n = NetworkConditions {
            slot: 100,
            recent_blockhash_age: 5,
            network_congestion: 0.3,
            average_priority_fee: 5_000,
            slot_success_rate: 0.9,
            leader_schedule: HashMap::new(),
            rpc_latency_ms: 20,
        };
        let s = eng.score_opportunity(&m, &n);
        assert!(s.0 > 0);

        let (cu_price, total_tip) = eng.suggest_fees(&m, &n, s);
        assert!(cu_price > 0 && total_tip >= 2_000);

        let ixs = eng.build_compute_budget_ixs(&m, &n, s);
        assert_eq!(ixs.len(), 2);
    }
}
