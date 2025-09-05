#![allow(clippy::too_many_arguments)]
#![allow(clippy::float_cmp)]
use anchor_lang::prelude::*;
use solana_sdk::{clock::Clock, pubkey::Pubkey, sysvar::clock};

use std::{
    cmp::Ordering,
    collections::VecDeque,
    sync::{Arc, RwLock},
    time::Duration,
};

use arrayref::{array_ref, array_refs};
use borsh::{BorshDeserialize, BorshSerialize};
use fixed::types::I80F48;

// ========================= Constants & defaults =========================

const KELLY_FRACTION: f64 = 0.25;
const MIN_POSITION_SIZE: u64 = 100_000;
const MAX_POSITION_RATIO: f64 = 0.15;
const VOLATILITY_WINDOW: usize = 120;
const CONFIDENCE_THRESHOLD: f64 = 0.68;
const SLIPPAGE_BUFFER: f64 = 0.003;
const GAS_SAFETY_MULTIPLIER: f64 = 1.5;
const DECAY_FACTOR: f64 = 0.995;
const MIN_LIQUIDITY_RATIO: f64 = 0.02;
const MAX_LEVERAGE: f64 = 3.0;
const RISK_FREE_RATE: f64 = 0.02;
const POSITION_SCALE_FACTOR: f64 = 0.8;

// Risk targets
const TARGET_PORTFOLIO_VOL: f64 = 0.10; // annualized
const ES95_BUDGET: f64 = 0.02; // 2% of capital (session)
const ES99_BUDGET: f64 = 0.05; // 5% of capital (session)

// ========================= Data types =========================

/// Represents different types of market venues
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VenueModel {
    /// Constant product AMM (e.g., Uniswap)
    Cpmm,
    /// Curve/stable invariant AMM
    StableSwap,
    /// Central limit order book or L2 order book
    OrderBook,
}

/// Represents the phase within a Solana slot
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotPhase {
    /// First ~200ms of the slot (highest priority)
    Early,
    /// Middle of the slot window (medium priority)
    Mid,
    /// Last ~200ms of the slot (lowest priority)
    Late,
}

/// Represents an order in the order book
#[derive(Debug, Clone)]
pub struct Order {
    /// Price of the order
    pub price: f64,
    /// Size/amount of the order
    pub size: f64,
    /// Order ID or identifier
    pub order_id: Option<String>,
    /// Timestamp of when the order was placed
    pub timestamp: i64,
    /// Whether this is a bid (buy) or ask (sell) order
    pub side: TradeSide,
}

impl Default for Order {
    fn default() -> Self {
        Self {
            price: 0.0,
            size: 0.0,
            order_id: None,
            timestamp: 0,
            side: TradeSide::Buy,
        }
    }
}

/// Market conditions and state at a point in time
#[derive(Debug, Clone)]
pub struct MarketConditions {
    /// Current market volatility (standard deviation of returns)
    pub volatility: f64,
    /// Available liquidity at the top of the order book
    pub liquidity_depth: u64,
    /// Current spread in basis points (1/100th of a percent)
    pub spread_bps: f64,
    /// Current priority fee in lamports
    pub priority_fee: u64,
    /// Network congestion level (0.0 to 1.0)
    pub network_congestion: f64,
    /// Confidence in oracle price (0.0 to 1.0)
    pub oracle_confidence: f64,
    /// Recent observed slippage in basis points
    pub recent_slippage: f64,
    /// Type of market venue
    pub venue: VenueModel,
    /// Current slot phase for transaction timing
    pub slot_phase: SlotPhase,
}

#[derive(Debug, Clone, Copy, BorshSerialize, BorshDeserialize)]
pub struct PositionMetrics {
    pub win_rate: f64,
    pub avg_profit: f64,
    pub avg_loss: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub current_streak: i32,
    pub total_trades: u64,
    pub last_update: i64,
}

#[derive(Debug, Clone)]
pub struct MarketConditions {
    pub volatility: f64,       // per-period realized vol (e.g., per minute)
    pub liquidity_depth: u64,  // notional depth at near-mid (venue-specific)
    pub spread_bps: f64,
    pub priority_fee: u64,     // microlamports or venue units
    pub network_congestion: f64,
    pub oracle_confidence: f64,
    pub recent_slippage: f64,
}

#[derive(Debug, Clone)]
pub struct OptimizedPosition {
    pub size: u64,
    pub leverage: f64,
    pub stop_loss: f64,
    pub take_profit: f64,
    pub confidence: f64,
    pub expected_value: f64,
    pub risk_adjusted_size: u64,
    pub max_gas_price: u64,
}

#[derive(Debug, Clone)]
struct Snapshot {
    capital: u64,
    metrics: PositionMetrics,
    recent_trades: Vec<TradeResult>,
    ewma_vol: f64,
    cfg: Config,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub max_position_pct: f64,
    pub vol_adjustment_factor: f64,
    pub streak_penalty: f64,
    pub congestion_discount: f64,
    pub confidence_weight: f64,
    pub regime_trending_boost: f64,
    pub regime_ranging_cut: f64,
    pub regime_volatile_cut: f64,
    pub regime_illiquid_cut: f64,
    pub min_trades_for_stats: usize,
    pub min_trades_for_kelly: usize,
    pub es_conf_levels: (f64, f64), // (0.95, 0.99)
}

impl Default for Config {
    fn default() -> Self {
        Self {
            // Position sizing
            max_position_pct: MAX_POSITION_RATIO,
            vol_adjustment_factor: 2.0,
            
            // Performance adjustments
            streak_penalty: 0.30,  // Increased from 0.05
            congestion_discount: 0.30,
            confidence_weight: 1.5,
            
            // Market regime adjustments
            regime_trending_boost: 1.20,
            regime_ranging_cut: 0.80,
            regime_volatile_cut: 0.60,
            regime_illiquid_cut: 0.40,
            
            // Statistical thresholds
            min_trades_for_stats: 30,
            min_trades_for_kelly: 20,
            
            // Risk management
            es_conf_levels: (0.95, 0.99),  // ES95 and ES99 confidence levels
        }
    }
}

/// Tracks empirical inclusion probabilities for different tip levels and slot phases
#[derive(Debug, Clone)]
struct InclusionHistogram {
    // Tip buckets in native units; monotonically increasing
    tip_edges: Vec<u64>,
    // For each phase, empirical CDF (P(inclusion | tip in bucket))
    cdf_early: Vec<f64>,
    cdf_mid: Vec<f64>,
    cdf_late: Vec<f64>,
    // EWMA smoothing alpha
    alpha: f64,
}

impl InclusionHistogram {
    /// Creates a new InclusionHistogram with the given tip edges and smoothing factor
    fn new(tip_edges: Vec<u64>, alpha: f64) -> Self {
        let n = tip_edges.len();
        Self {
            tip_edges,
            cdf_early: vec![0.0; n],
            cdf_mid: vec![0.0; n],
            cdf_late: vec![0.0; n],
            alpha,
        }
    }

    /// Updates the histogram with a new observation
    fn update(&mut self, phase: SlotPhase, tip: u64, included: bool) {
        if self.tip_edges.is_empty() {
            return;
        }
        
        // Find the appropriate bucket for this tip
        let idx = match self.tip_edges.binary_search(&tip) {
            Ok(i) => i,
            Err(i) => i.saturating_sub(1).min(self.tip_edges.len() - 1),
        };
        
        let obs = if included { 1.0 } else { 0.0 };
        
        // Update the appropriate CDF based on slot phase
        let (cdf, alpha) = match phase {
            SlotPhase::Early => (&mut self.cdf_early, self.alpha),
            SlotPhase::Mid => (&mut self.cdf_mid, self.alpha),
            SlotPhase::Late => (&mut self.cdf_late, self.alpha),
        };
        
        // EWMA update
        cdf[idx] = cdf[idx] * (1.0 - alpha) + obs * alpha;
        
        // Ensure monotonicity
        enforce_monotonic(cdf);
    }
    
    /// Gets the probability of inclusion for a given tip and phase
    fn p_inclusion(&self, phase: SlotPhase, tip: u64) -> f64 {
        if self.tip_edges.is_empty() {
            return 0.0;
        }
        
        let cdf = match phase {
            SlotPhase::Early => &self.cdf_early,
            SlotPhase::Mid => &self.cdf_mid,
            SlotPhase::Late => &self.cdf_late,
        };
        
        // Find the bucket containing this tip
        let (mut lo_i, mut hi_i) = (0usize, self.tip_edges.len() - 1);
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
        
        // If we're at the exact bucket, return its value
        if lo_i == hi_i {
            return cdf[lo_i];
        }
        
        // Linear interpolation between buckets
        let lo_tip = self.tip_edges[lo_i] as f64;
        let hi_tip = self.tip_edges[hi_i] as f64;
        let w = ((tip as f64 - lo_tip) / (hi_tip - lo_tip)).clamp(0.0, 1.0);
        let p = cdf[lo_i] * (1.0 - w) + cdf[hi_i] * w;
        p.clamp(0.0, 1.0)
    }
}

/// Ensures a sequence is non-decreasing
fn enforce_monotonic(xs: &mut [f64]) {
    for i in 1..xs.len() {
        if xs[i] < xs[i-1] {
            xs[i] = xs[i-1];
        }
    }
}

/// Snapshot of an order book state for impact modeling
#[derive(Debug, Clone)]
pub struct OrderBookSnapshot {
    pub bids: Vec<Order>,
    pub asks: Vec<Order>,
    pub timestamp: i64,
    pub mid_price: f64,
    pub spread: f64,
}

/// CPMM pool state snapshot
#[derive(Debug, Clone)]
pub struct CpmmPool {
    pub reserves_a: u64,
    pub reserves_b: u64,
    pub fee_bps: u16,
    pub last_updated: i64,
}

/// StableSwap pool state snapshot
#[derive(Debug, Clone)]
pub struct StableSwapPool {
    pub reserves: Vec<u64>,
    pub a: u64, // Amplification coefficient
    pub fee_bps: u16,
    pub admin_fee_bps: u16,
    pub last_updated: i64,
}

/// Models the market impact and inclusion probability of transactions
#[derive(Debug, Clone)]
pub struct ImpactModel {
    // Empirical inclusion probability histogram
    pub inclusion_hist: InclusionHistogram,
    
    // Fallback parameters for sigmoid inclusion probability
    pub incl_sigmoid_k: f64,
    pub incl_sigmoid_mid: f64,
    
    // Impact model parameters
    pub impact_alpha: f64,
    
    // Venue impact context
    pub venue: VenueModel,
    
    // Optional snapshots for different venue models
    pub ob_snapshot: Option<OrderBookSnapshot>,
    pub cpmm: Option<CpmmPool>,
    pub stableswap: Option<StableSwapPool>,
}

impl Default for ImpactModel {
    fn default() -> Self {
        // Define tip buckets from 1k to 100k lamports (10 buckets)
        let tip_edges: Vec<u64> = (1..=10).map(|i| i * 10_000).collect();
        
        Self {
            inclusion_hist: InclusionHistogram::new(tip_edges, 0.05),
            incl_sigmoid_k: 0.0001,
            incl_sigmoid_mid: 5000.0, // 5k lamports
            impact_alpha: 0.1,
            venue: VenueModel::OrderBook, // Default to order book
            ob_snapshot: None,
            cpmm: None,
            stableswap: None,
        }
    }
    
    /// Records an inclusion observation for the given tip and phase
    pub fn record_inclusion(&mut self, phase: SlotPhase, tip: u64, included: bool) {
        self.inclusion_hist.update(phase, tip, included);
    }
    
    /// Gets the probability of inclusion for a given tip and phase
    pub fn get_inclusion_probability(&self, phase: SlotPhase, tip: u64) -> f64 {
        self.inclusion_hist.p_inclusion(phase, tip)
    }
    
    /// Updates the venue model and associated snapshot
    pub fn update_venue(&mut self, venue: VenueModel, snapshot: Option<OrderBookSnapshot>) {
        self.venue = venue;
        self.ob_snapshot = snapshot;
        // Clear other snapshots when changing venue type
        self.cpmm = None;
        self.stableswap = None;
    }
    
    /// Updates the CPMM pool state
    pub fn update_cpmm_pool(&mut self, pool: CpmmPool) {
        self.venue = VenueModel::Cpmm;
        self.cpmm = Some(pool);
        // Clear other snapshots
        self.ob_snapshot = None;
        self.stableswap = None;
    }
    
    /// Updates the StableSwap pool state
    pub fn update_stableswap_pool(&mut self, pool: StableSwapPool) {
        self.venue = VenueModel::StableSwap;
        self.stableswap = Some(pool);
        // Clear other snapshots
        self.ob_snapshot = None;
        self.cpmm = None;
    }
    
    /// Estimates the price impact of a trade of the given size
    pub fn estimate_impact(&self, size: u64, is_buy: bool) -> f64 {
        match self.venue {
            VenueModel::OrderBook => {
                self.estimate_ob_impact(size, is_buy)
            }
            VenueModel::Cpmm => {
                self.estimate_cpmm_impact(size, is_buy)
            }
            VenueModel::StableSwap => {
                self.estimate_stableswap_impact(size, is_buy)
            }
        }
    }
    
    /// Estimates impact for order book venues
    fn estimate_ob_impact(&self, size: u64, is_buy: bool) -> f64 {
        let snapshot = match &self.ob_snapshot {
            Some(snap) => snap,
            None => return 0.0, // No snapshot available
        };
        
        let levels = if is_buy { &snapshot.asks } else { &snapshot.bids };
        let mut remaining = size as f64;
        let mut total_value = 0.0;
        let mut total_size = 0.0;
        
        for order in levels {
            let size = order.size.min(remaining);
            total_value += size * order.price;
            total_size += size;
            remaining -= size;
            
            if remaining <= 0.0 {
                break;
            }
        }
        
        if total_size == 0.0 {
            return 0.0;
        }
        
        let vwap = total_value / total_size;
        let mid = snapshot.mid_price;
        
        // Return impact as basis points from mid
        if is_buy {
            (vwap / mid - 1.0) * 10_000.0
        } else {
            (1.0 - vwap / mid) * 10_000.0
        }
    }
    
    /// Estimates impact for CPMM venues
    fn estimate_cpmm_impact(&self, size: u64, is_buy: bool) -> f64 {
        let pool = match &self.cpmm {
            Some(p) => p,
            None => return 0.0,
        };
        
        let (reserve_in, reserve_out) = if is_buy {
            (pool.reserves_b, pool.reserves_a)
        } else {
            (pool.reserves_a, pool.reserves_b)
        };
        
        let fee_factor = 1.0 - (pool.fee_bps as f64 / 10_000.0);
        let amount_in = size as f64 * fee_factor;
        
        // Constant product formula: x * y = k
        let amount_out = (reserve_out as f64 * amount_in) / (reserve_in as f64 + amount_in);
        let price = amount_in / amount_out;
        
        // Calculate price impact as basis points from 1:1
        if is_buy {
            (price - 1.0) * 10_000.0
        } else {
            (1.0 - 1.0 / price) * 10_000.0
        }
    }
    
    /// Estimates impact for StableSwap venues
    fn estimate_stableswap_impact(&self, _size: u64, _is_buy: bool) -> f64 {
        // Simplified implementation - in practice this would use the StableSwap invariant
        // and the amplification coefficient to compute the exact impact
        // For now, we'll return a small constant impact
        5.0 // 0.05% impact
    }
}

#[derive(Debug, Clone)]
struct RiskParameters {
    max_position_pct: f64,
    vol_adjustment_factor: f64,
    streak_penalty: f64,
    congestion_discount: f64,
    confidence_weight: f64,
}

#[derive(Debug, Clone)]
struct TradeResult {
    pnl: f64,
    size: u64,
    timestamp: i64,
    slippage: f64,
    gas_cost: u64,
}

#[derive(Debug, Clone)]
struct PerformanceTracker {
    rolling_sharpe: f64,
    rolling_sortino: f64,
    calmar_ratio: f64,
    omega_ratio: f64,
    tail_ratio: f64,
    es95: f64,
    es99: f64,
}

#[derive(Debug, Clone)]
pub struct PerformanceStats {
    pub sharpe: f64,
    pub sortino: f64,
    pub calmar: f64,
    pub omega: f64,
    pub tail: f64,
    pub total_trades: u64,
    pub win_rate: f64,
    pub avg_profit: f64,
    pub avg_loss: f64,
    pub max_drawdown: f64,
    pub es95: f64,
    pub es99: f64,
}

#[derive(Debug, Clone, Copy)]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy)]
pub enum MarketState {
    Trending,
    Ranging,
    Volatile,
    Illiquid,
}

// ========================= Observability hooks =========================

pub trait MetricsSink: Send + Sync {
    fn gauge(&self, key: &str, value: f64);
    fn counter(&self, key: &str, delta: f64);
    fn event(&self, key: &str, msg: &str);
}

pub trait Tracer: Send + Sync {
    fn span<F: FnOnce()>(&self, name: &str, f: F);
    fn log_kv(&self, key: &str, value: &str);
}

struct NoopMetrics;
impl MetricsSink for NoopMetrics {
    fn gauge(&self, _key: &str, _value: f64) {}
    fn counter(&self, _key: &str, _delta: f64) {}
    fn event(&self, _key: &str, _msg: &str) {}
}

struct NoopTracer;
impl Tracer for NoopTracer {
    fn span<F: FnOnce()>(&self, _name: &str, f: F) {
        f()
    }
    fn log_kv(&self, _key: &str, _value: &str) {}
}

// ========================= Optimizer =========================

pub struct FractionalPositionOptimizer {
    capital_base: Arc<RwLock<u64>>,
    position_metrics: Arc<RwLock<PositionMetrics>>,
    performance_tracker: Arc<RwLock<PerformanceTracker>>,
    volatility_buffer: Arc<RwLock<VecDeque<f64>>>,
    recent_trades: Arc<RwLock<VecDeque<TradeResult>>>,

    // Config / models
    cfg: Arc<RwLock<Config>>,
    impact_model: Arc<RwLock<ImpactModel>>,

    // Obs
    metrics: Arc<dyn MetricsSink>,
    tracer: Arc<dyn Tracer>,
}

impl Default for FractionalPositionOptimizer {
    fn default() -> Self {
        Self::new(1_000_000_000)
    }
}

impl FractionalPositionOptimizer {
    pub fn new(initial_capital: u64) -> Self {
        Self {
            capital_base: Arc::new(RwLock::new(initial_capital)),
            position_metrics: Arc::new(RwLock::new(PositionMetrics {
                win_rate: 0.5,
                avg_profit: 0.0,
                avg_loss: 0.0,
                sharpe_ratio: 0.0,
                max_drawdown: 0.0,
                current_streak: 0,
                total_trades: 0,
                last_update: 0,
            })),
            performance_tracker: Arc::new(RwLock::new(PerformanceTracker {
                rolling_sharpe: 0.0,
                rolling_sortino: 0.0,
                calmar_ratio: 0.0,
                omega_ratio: 1.0,
                tail_ratio: 1.0,
                es95: 0.0,
                es99: 0.0,
            })),
            volatility_buffer: Arc::new(RwLock::new(VecDeque::with_capacity(VOLATILITY_WINDOW))),
            recent_trades: Arc::new(RwLock::new(VecDeque::with_capacity(2048))),
            cfg: Arc::new(RwLock::new(Config::default())),
            impact_model: Arc::new(RwLock::new(ImpactModel::default())),
            metrics: Arc::new(NoopMetrics),
            tracer: Arc::new(NoopTracer),
        }
    }

    // --------------------- Public API ---------------------

    pub fn calculate_optimal_position(
        &self,
        opportunity_value: u64,
        market: &MarketConditions,
        timestamp: i64,
    ) -> OptimizedPosition {
        let snap = self.snapshot();

        // Kelly base size with uncertainty shrinkage
        let kelly_size = self.calculate_kelly_position_uncertain(&snap, opportunity_value);

        // Volatility adjustment with EWMA buffer
        let vol_adjusted = self.adjust_for_volatility(kelly_size, market, snap.ewma_vol);

        // Risk constraints (VaR/ES, drawdown, max position)
        let risk_adj = self.apply_risk_constraints(vol_adjusted, &snap);

        // Liquidity & impact constraint
        let liq_adj = self.constrain_by_liquidity_and_impact(risk_adj, market);

        // Network-aware adjustment
        let net_adj = self.adjust_for_network_conditions(liq_adj, market, &snap);

        // Dynamic scaling by performance and market regime
        let final_size = self.apply_dynamic_scaling(net_adj, &snap.metrics, market, timestamp);

        // Optimal leverage under vol/drawdown constraints
        let leverage = self.calculate_optimal_leverage(&snap.metrics, market);

        // ATR-like stops
        let (stop_loss, take_profit) =
            self.calculate_stop_levels(&snap.metrics, market, final_size);

        // Confidence and EV with inclusion/gas model
        let confidence = self.calculate_position_confidence(&snap.metrics, market);
        let expected_value = self.calculate_expected_value(final_size, &snap.metrics, market);

        // Tip/priority fee budgeting under inclusion curve
        let max_gas_price = self.calculate_max_gas_inclusion_aware(
            final_size,
            market,
            &self.impact_model.read().unwrap(),
            expected_value,
        );

        let out = OptimizedPosition {
            size: final_size,
            leverage,
            stop_loss,
            take_profit,
            confidence,
            expected_value,
            risk_adjusted_size: final_size,
            max_gas_price,
        };

        self.metrics.gauge("fpo.position.size", out.size as f64);
        self.metrics.gauge("fpo.position.leverage", out.leverage);
        self.metrics.gauge("fpo.position.confidence", out.confidence);
        self.metrics.gauge("fpo.position.ev", out.expected_value);
        out
    }

    pub fn update_trade_result(&self, result: TradeResult) {
        {
            let mut trades = self.recent_trades.write().unwrap();
            trades.push_front(result.clone());
            if trades.len() > 2048 {
                trades.pop_back();
            }
        }

        self.update_metrics_and_trackers(&result);
    }

    pub fn update_volatility(&self, abs_price_change: f64) {
        let mut vol_buffer = self.volatility_buffer.write().unwrap();
        vol_buffer.push_front(abs_price_change.abs());
        if vol_buffer.len() > VOLATILITY_WINDOW {
            vol_buffer.pop_back();
        }
    }

    pub fn update_capital(&self, new_capital: u64) {
        *self.capital_base.write().unwrap() = new_capital;
    }

    pub fn set_metrics_sink(&mut self, sink: Arc<dyn MetricsSink>) {
        self.metrics = sink;
    }

    pub fn set_tracer(&mut self, tracer: Arc<dyn Tracer>) {
        self.tracer = tracer;
    }

    pub fn get_current_metrics(&self) -> PositionMetrics {
        self.position_metrics.read().unwrap().clone()
    }

    pub fn get_performance_stats(&self) -> PerformanceStats {
        let m = self.position_metrics.read().unwrap().clone();
        let p = self.performance_tracker.read().unwrap().clone();
        PerformanceStats {
            sharpe: p.rolling_sharpe,
            sortino: p.rolling_sortino,
            calmar: p.calmar_ratio,
            omega: p.omega_ratio,
            tail: p.tail_ratio,
            total_trades: m.total_trades,
            win_rate: m.win_rate,
            avg_profit: m.avg_profit,
            avg_loss: m.avg_loss,
            max_drawdown: m.max_drawdown,
            es95: p.es95,
            es99: p.es99,
        }
    }

    pub fn should_trade(&self, confidence: f64, expected_value: f64) -> bool {
        if confidence < CONFIDENCE_THRESHOLD {
            return false;
        }
        if expected_value <= 0.0 {
            return false;
        }
        let capital = *self.capital_base.read().unwrap();
        if capital < MIN_POSITION_SIZE * 10 {
            return false;
        }

        let metrics = self.position_metrics.read().unwrap();
        if metrics.current_streak <= -5 {
            return false;
        }
        if metrics.max_drawdown > 0.2 && metrics.current_streak < 0 {
            return false;
        }

        let perf = self.performance_tracker.read().unwrap();
        if perf.es99 > ES99_BUDGET {
            // Tail risk exceeded — cool down
            return false;
        }

        true
    }

    pub fn emergency_stop_check(&self) -> bool {
        let metrics = self.position_metrics.read().unwrap();

        if metrics.max_drawdown > 0.30 {
            return true;
        }
        if metrics.current_streak <= -8 {
            return true;
        }
        if metrics.sharpe_ratio < -1.0 && metrics.total_trades > 50 {
            return true;
        }

        let recent_trades = self.recent_trades.read().unwrap();
        if recent_trades.len() >= 10 {
            let recent_losses = recent_trades
                .iter()
                .take(10)
                .filter(|t| t.pnl < 0.0)
                .count();
            if recent_losses >= 8 {
                return true;
            }
        }

        let perf = self.performance_tracker.read().unwrap();
        if perf.es99 > ES99_BUDGET {
            return true;
        }

        false
    }

    pub fn get_risk_adjusted_size(&self, base_size: u64, urgency: f64) -> u64 {
        let metrics = self.position_metrics.read().unwrap();
        let capital = *self.capital_base.read().unwrap();

        let urgency_factor = urgency.clamp(0.5, 2.0);
        let confidence_factor = if metrics.total_trades < 50 { 0.5 } else { 1.0 };

        let streak_factor = match metrics.current_streak {
            s if s >= 3 => 1.1,
            s if s <= -3 => 0.7,
            _ => 1.0,
        };

        let sharpe_factor = if metrics.sharpe_ratio > 1.5 {
            1.1
        } else if metrics.sharpe_ratio < 0.5 {
            0.8
        } else {
            1.0
        };

        let adjusted =
            (base_size as f64 * urgency_factor * confidence_factor * streak_factor * sharpe_factor)
                as u64;
        adjusted
            .min((capital as f64 * MAX_POSITION_RATIO) as u64)
            .max(MIN_POSITION_SIZE)
    }

    pub fn calculate_adaptive_stops(
        &self,
        entry_price: f64,
        _position_size: u64,
        side: TradeSide,
    ) -> (f64, f64) {
        let metrics = self.position_metrics.read().unwrap();
        let vol_buffer = self.volatility_buffer.read().unwrap();

        let current_vol = if vol_buffer.len() >= 10 {
            vol_buffer.iter().take(10).sum::<f64>() / 10.0
        } else {
            0.02
        };

        let base_stop_distance = current_vol * 2.0;
        let streak_adjustment = if metrics.current_streak < -2 {
            0.7
        } else if metrics.current_streak > 2 {
            1.3
        } else {
            1.0
        };

        let stop_distance = base_stop_distance * streak_adjustment;
        let profit_distance = stop_distance * 2.5;

        match side {
            TradeSide::Buy => (
                entry_price * (1.0 - stop_distance),
                entry_price * (1.0 + profit_distance),
            ),
            TradeSide::Sell => (
                entry_price * (1.0 + stop_distance),
                entry_price * (1.0 - profit_distance),
            ),
        }
    }

    pub fn optimize_for_market_conditions(
        &self,
        base_position: OptimizedPosition,
        market_state: MarketState,
    ) -> OptimizedPosition {
        let mut optimized = base_position;

        match market_state {
            MarketState::Trending => {
                optimized.size = (optimized.size as f64 * 1.2) as u64;
                optimized.leverage = (optimized.leverage * 1.1).min(MAX_LEVERAGE);
                optimized.take_profit *= 1.5;
            }
            MarketState::Ranging => {
                optimized.size = (optimized.size as f64 * 0.8) as u64;
                optimized.leverage = (optimized.leverage * 0.9).max(1.0);
                optimized.stop_loss *= 0.8;
                optimized.take_profit *= 0.7;
            }
            MarketState::Volatile => {
                optimized.size = (optimized.size as f64 * 0.6) as u64;
                optimized.leverage = 1.0;
                optimized.stop_loss *= 1.5;
                optimized.max_gas_price = (optimized.max_gas_price as f64 * 1.5) as u64;
            }
            MarketState::Illiquid => {
                optimized.size = (optimized.size as f64 * 0.4) as u64;
                optimized.leverage = 1.0;
                optimized.confidence *= 0.7;
            }
        }

        optimized
    }

    pub fn calculate_portfolio_heat(&self) -> f64 {
        let capital = *self.capital_base.read().unwrap();
        let metrics = self.position_metrics.read().unwrap();
        let recent_trades = self.recent_trades.read().unwrap();

        let open_risk = recent_trades
            .iter()
            .take(5)
            .map(|t| t.size as f64 / capital as f64)
            .sum::<f64>();

        let drawdown_heat = (metrics.max_drawdown * 2.0).clamp(0.0, 1.0);
        let streak_heat = if metrics.current_streak < 0 {
            (metrics.current_streak.abs() as f64 * 0.05).clamp(0.0, 1.0)
        } else {
            0.0
        };

        (open_risk + drawdown_heat + streak_heat).min(1.0)
    }

    pub fn adjust_for_correlation(&self, position: OptimizedPosition, correlation: f64) -> OptimizedPosition {
        let mut adjusted = position;
        let correlation_factor = 1.0 - correlation.abs() * 0.5;

        adjusted.size = (adjusted.size as f64 * correlation_factor) as u64;
        adjusted.confidence *= correlation_factor;

        if correlation.abs() > 0.7 {
            adjusted.leverage = (adjusted.leverage * 0.8).max(1.0);
        }

        adjusted
    }

    pub fn calculate_time_decay_factor(&self, opportunity_age_ms: u64) -> f64 {
        let age_seconds = opportunity_age_ms as f64 / 1000.0;

        if age_seconds < 0.1 {
            1.0
        } else if age_seconds < 0.5 {
            0.95
        } else if age_seconds < 1.0 {
            0.8
        } else if age_seconds < 2.0 {
            0.6
        } else {
            0.3
        }
    }

    pub fn finalize_position(&self, mut position: OptimizedPosition) -> OptimizedPosition {
        let capital = *self.capital_base.read().unwrap();
        let metrics = self.position_metrics.read().unwrap();

        position.size = position.size.min((capital as f64 * 0.95) as u64);
        position.size = ((position.size / 1000) * 1000).max(MIN_POSITION_SIZE);

        position.stop_loss = (position.stop_loss * 10000.0).round() / 10000.0;
        position.take_profit = (position.take_profit * 10000.0).round() / 10000.0;

        if metrics.total_trades < 20 {
            position.size = (position.size as f64 * 0.5) as u64;
            position.leverage = 1.0;
        }

        position
    }

    // --------------------- Observation Hooks ---------------------
    
    /// Records the outcome of a transaction (included or not) with the given tip and slot phase.
    /// This updates the internal inclusion probability model.
    ///
    /// # Example
    /// ```rust
    /// let phase = SlotPhase::Mid; // Your slot phase tracker
    /// optimizer.record_inclusion_outcome(phase, 5000, true);
    /// ```
    pub fn record_inclusion_outcome(&self, phase: SlotPhase, tip: u64, included: bool) {
        self.update_inclusion_observation(phase, tip, included);
        // Uncomment if you have metrics enabled:
        // self.metrics.counter("fpo.inclusion.obs", 1.0);
    }
    
    // --------------------- Impact Model Setters ---------------------
    
    /// Updates the venue model type
    pub fn set_venue_model(&self, venue: VenueModel) {
        self.impact_model.write().unwrap().venue = venue;
    }
    
    /// Updates the order book snapshot
    pub fn set_orderbook_snapshot(&self, ob: OrderBookSnapshot) {
        let mut model = self.impact_model.write().unwrap();
        model.venue = VenueModel::OrderBook;
        model.ob_snapshot = Some(ob);
        // Clear other snapshots when setting order book
        model.cpmm = None;
        model.stableswap = None;
    }
    
    /// Updates the CPMM pool state
    pub fn set_cpmm_pool(&self, pool: CpmmPool) {
        let mut model = self.impact_model.write().unwrap();
        model.venue = VenueModel::Cpmm;
        model.cpmm = Some(pool);
        // Clear other snapshots when setting CPMM
        model.ob_snapshot = None;
        model.stableswap = None;
    }
    
    /// Updates the StableSwap pool state
    pub fn set_stableswap_pool(&self, pool: StableSwapPool) {
        let mut model = self.impact_model.write().unwrap();
        model.venue = VenueModel::StableSwap;
        model.stableswap = Some(pool);
        // Clear other snapshots when setting StableSwap
        model.ob_snapshot = None;
        model.cpmm = None;
    }
    
    /// Records an inclusion observation for the given tip and phase
    pub fn update_inclusion_observation(&self, phase: SlotPhase, tip: u64, included: bool) {
        self.impact_model.write().unwrap().record_inclusion(phase, tip, included);
    }
    
    // --------------------- Core internals ---------------------

    fn snapshot(&self) -> Snapshot {
        let capital = *self.capital_base.read().unwrap();
        let metrics = self.position_metrics.read().unwrap().clone();
        let cfg = self.cfg.read().unwrap().clone();
        let ewma_vol = {
            let v = self.volatility_buffer.read().unwrap();
            if v.is_empty() {
                0.02
            } else {
                // Simple average over last N for robustness
                let n = v.len().min(20);
                v.iter().take(n).sum::<f64>() / n as f64
            }
        };
        let recent_trades = {
            let r = self.recent_trades.read().unwrap();
            // Materialize a bounded slice to stable vec
            let take = r.len().min(252);
            r.iter().cloned().take(take).collect::<Vec<_>>()
        };
        Snapshot {
            capital,
            metrics,
            recent_trades,
            ewma_vol,
            cfg,
        }
    }

    fn calculate_kelly_position_uncertain(&self, snap: &Snapshot, opportunity_value: u64) -> u64 {
        let n = snap.metrics.total_trades;
        if (n as usize) < snap.cfg.min_trades_for_kelly || snap.metrics.avg_loss == 0.0 {
            return (opportunity_value as f64 * 0.01) as u64;
        }

        // Wilson lower bound for p
        let wins = self
            .recent_trades
            .read()
            .unwrap()
            .iter()
            .take(200)
            .filter(|t| t.pnl > 0.0)
            .count() as u64;
        let total = (snap.metrics.total_trades).min(200) as u64;
        let p_lb = wilson_lower_bound(wins, total, 1.96); // 95%

        // Shrink avg win/loss by their uncertainty
        let (avg_win, win_se) = mean_and_se(
            &self
                .recent_trades
                .read()
                .unwrap()
                .iter()
                .filter(|t| t.pnl > 0.0)
                .map(|t| t.pnl / (t.size as f64).max(1.0))
                .collect::<Vec<_>>(),
        );
        let (avg_loss_abs, loss_se) = mean_and_se(
            &self
                .recent_trades
                .read()
                .unwrap()
                .iter()
                .filter(|t| t.pnl < 0.0)
                .map(|t| (-t.pnl) / (t.size as f64).max(1.0))
                .collect::<Vec<_>>(),
        );

        let avg_win = (avg_win - win_se.max(0.0)).max(0.0);
        let avg_loss = (avg_loss_abs - loss_se.max(0.0)).max(1e-6);

        let odds = if avg_loss > 0.0 { avg_win / avg_loss } else { 0.0 };
        let loss_prob = 1.0 - p_lb;
        let kelly_raw = if odds > 0.0 {
            ((p_lb * odds) - loss_prob) / odds
        } else {
            0.0
        }
        .max(0.0);

        // Information ratio moderation
        let ir = snap.metrics.sharpe_ratio.max(0.0);
        let ir_cap = (ir / 2.0).clamp(0.0, 1.0);
        let adjusted_kelly = kelly_raw * KELLY_FRACTION * ir_cap;

        let cap = (opportunity_value as f64 * adjusted_kelly.min(0.25)) as u64;
        cap.max(MIN_POSITION_SIZE)
    }

    fn adjust_for_volatility(&self, base_size: u64, market: &MarketConditions, ewma_vol: f64) -> u64 {
        let recent_vol = ewma_vol.max(1e-6).max(market.volatility);
        let vol_ratio = recent_vol / 0.02; // normalize to baseline vol
        let vol_multiplier = if vol_ratio > 2.0 {
            0.5
        } else if vol_ratio > 1.5 {
            0.7
        } else if vol_ratio < 0.5 {
            1.3
        } else {
            1.0
        };

        (base_size as f64 * vol_multiplier) as u64
    }

    fn apply_risk_constraints(&self, size: u64, snap: &Snapshot) -> u64 {
        let max_position_cap = (snap.capital as f64 * snap.cfg.max_position_pct) as u64;

        let dd_adjusted = if snap.metrics.max_drawdown > 0.10 {
            (max_position_cap as f64 * (1.0 - snap.metrics.max_drawdown * 2.0).max(0.3)) as u64
        } else {
            max_position_cap
        };

        let risk_capacity = self.calculate_risk_capacity(snap);
        size.min(risk_capacity).min(dd_adjusted)
    }

    fn calculate_risk_capacity(&self, snap: &Snapshot) -> u64 {
        let (es95, es99) = self.empirical_es_levels(&snap.recent_trades, snap.capital);
        {
            let mut perf = self.performance_tracker.write().unwrap();
            perf.es95 = es95;
            perf.es99 = es99;
        }
        let risk_budget = (snap.capital as f64 * ES95_BUDGET) as u64;
        let var_constrained = (snap.capital as f64 * (1.0 - es95)).max(0.0) as u64;

        risk_budget.min(var_constrained).max(MIN_POSITION_SIZE)
    }

    /// Fallback impact model when no venue-specific data is available
    fn fallback_impact(size: f64, market: &MarketConditions) -> f64 {
        let spread = market.spread_bps / 10_000.0;
        let depth = market.liquidity_depth.max(1) as f64;
        let alpha = 2.0 * spread.max(0.0001);
        alpha * (size / depth).min(0.02).powf(1.2)
    }

    /// Constant product AMM exact output price delta -> convert to cost proxy
    fn cpmm_impact_cost(size: f64, pool: &CpmmPool) -> f64 {
        // Assume buying base with quote; impact approximated by price move
        let x0 = pool.reserves_a as f64;
        let y0 = pool.reserves_b as f64;
        if x0 <= 0.0 || y0 <= 0.0 {
            return 0.0;
        }
        
        // Small-trade approximation for speed
        let trade_ratio = (size / (y0 + 1.0)).clamp(0.0, 0.1);
        let fee = pool.fee_bps as f64 / 10_000.0;
        let effective = trade_ratio * (1.0 - fee);
        
        // Price impact ~ effective / (1 - effective) on CPMM marginally
        let impact = effective / (1.0 - effective + 1e-9);
        impact.abs()
    }

    /// StableSwap impact approximation: amplified liquidity reduces impact for small trades
    fn stableswap_impact_cost(size: f64, pool: &StableSwapPool) -> f64 {
        if pool.reserves.len() < 2 {
            return 0.0;
        }
        
        let r = (pool.reserves[0] + pool.reserves[1]) as f64;
        let r = r.max(1.0);
        let fee = pool.fee_bps as f64 / 10_000.0;
        
        // Heuristic: impact ~ (size / (r * amp)) with mild convexity
        let amp = pool.a as f64;
        let base = (size / (r * amp.max(1.0))).clamp(0.0, 0.05);
        base.powf(1.1) * (1.0 + fee)
    }

    /// OrderBook slope model: impact = slope * (size/depth) + curvature * (size/depth)^2
    fn orderbook_impact_cost(size: f64, ob: &OrderBookSnapshot) -> f64 {
        let depth = ob.asks.iter().chain(ob.bids.iter())
            .map(|o| o.size * ob.mid_price as f64)
            .sum::<f64>()
            .max(1.0);
            
        let x = (size / depth).clamp(0.0, 0.2);
        
        // Default values if not provided by the order book
        let slope = 0.5;  // Default slope of 50 bps per 1% of depth
        let curvature = 0.1;  // Small convexity adjustment
        
        let linear = slope * x;
        let convex = curvature * x * x;
        (linear + convex).abs()
    }

    fn constrain_by_liquidity_and_impact(&self, size: u64, market: &MarketConditions) -> u64 {
        let model = self.impact_model.read().unwrap().clone();
        let s = size as f64;
        
        // Calculate impact cost based on venue type
        let impact_cost = match model.venue {
            VenueModel::Cpmm => {
                if let Some(pool) = &model.cpmm {
                    Self::cpmm_impact_cost(s, pool)
                } else {
                    Self::fallback_impact(s, market)
                }
            }
            VenueModel::StableSwap => {
                if let Some(pool) = &model.stableswap {
                    Self::stableswap_impact_cost(s, pool)
                } else {
                    Self::fallback_impact(s, market)
                }
            }
            VenueModel::OrderBook => {
                if let Some(ob) = &model.ob_snapshot {
                    Self::orderbook_impact_cost(s, ob)
                } else {
                    Self::fallback_impact(s, market)
                }
            }
        };

        // Budget slippage to SLIPPAGE_BUFFER of notional size
        let budget = s * SLIPPAGE_BUFFER;
        let size_scaled = if impact_cost > budget {
            let scale = (budget / impact_cost).clamp(0.2, 1.0);
            (s * scale) as u64
        } else {
            size
        };

        // Also cap by minimal liquidity footprint ratio (generic)
        let depth = market.liquidity_depth.max(1) as f64;
        let max_impact_size = (depth * MIN_LIQUIDITY_RATIO) as u64;
        size_scaled.min(max_impact_size)
    }

    fn adjust_for_network_conditions(&self, size: u64, market: &MarketConditions, snap: &Snapshot) -> u64 {
        let congestion_factor = 1.0 - (market.network_congestion * snap.cfg.congestion_discount);
        let priority_factor = if market.priority_fee > 50_000 {
            0.8
        } else if market.priority_fee > 25_000 {
            0.9
        } else {
            1.0
        };

        let confidence_factor = market.oracle_confidence.powf(snap.cfg.confidence_weight);
        let adjustment = (congestion_factor * priority_factor * confidence_factor).clamp(0.3, 1.0);
        (size as f64 * adjustment) as u64
    }

    fn apply_dynamic_scaling(
        &self,
        size: u64,
        metrics: &PositionMetrics,
        market: &MarketConditions,
        timestamp: i64,
    ) -> u64 {
        let time_decay = if metrics.last_update > 0 {
            let elapsed = ((timestamp - metrics.last_update) as f64 / 3600.0).max(0.0);
            DECAY_FACTOR.powf(elapsed)
        } else {
            1.0
        };

        let performance_scale = self.calculate_performance_scale(metrics);
        let market_scale = self.calculate_market_scale(market);

        let total_scale = time_decay * performance_scale * market_scale * POSITION_SCALE_FACTOR;
        (size as f64 * total_scale.clamp(0.1, 1.5)) as u64
    }

    fn calculate_performance_scale(&self, metrics: &PositionMetrics) -> f64 {
        let sharpe_scale = (metrics.sharpe_ratio / 2.0).clamp(0.0, 1.5);
        let win_rate_scale = (metrics.win_rate * 2.0).clamp(0.5, 1.2);
        let drawdown_scale = (1.0 - metrics.max_drawdown).max(0.3);
        (sharpe_scale * win_rate_scale * drawdown_scale).powf(1.0 / 3.0)
    }

    fn calculate_market_scale(&self, market: &MarketConditions) -> f64 {
        let spread_scale = (10.0 / (market.spread_bps + 1.0)).min(1.0);
        let vol_scale = (0.02 / market.volatility.max(0.001)).clamp(0.5, 1.2);
        let liquidity_scale = ((market.liquidity_depth as f64).ln() / 20.0).clamp(0.3, 1.0);
        let slippage_scale = (1.0 - market.recent_slippage * 10.0).max(0.5);
        (spread_scale * vol_scale * liquidity_scale * slippage_scale).powf(0.25)
    }

    fn calculate_optimal_leverage(&self, metrics: &PositionMetrics, market: &MarketConditions) -> f64 {
        let base = if metrics.sharpe_ratio > 1.5 {
            2.0
        } else if metrics.sharpe_ratio > 1.0 {
            1.5
        } else if metrics.sharpe_ratio > 0.5 {
            1.2
        } else {
            1.0
        };

        let vol_adj = (0.02 / market.volatility.max(0.01)).clamp(0.5, 1.5);
        let win_adj = (metrics.win_rate * 2.0).clamp(0.7, 1.3);
        let dd_pen = (1.0 - metrics.max_drawdown * 2.0).max(0.5);

        (base * vol_adj * win_adj * dd_pen).clamp(1.0, MAX_LEVERAGE)
    }

    fn calculate_stop_levels(
        &self,
        metrics: &PositionMetrics,
        market: &MarketConditions,
        position_size: u64,
    ) -> (f64, f64) {
        let atr_estimate = market.volatility * 2.0;
        let spread_cost = market.spread_bps / 10_000.0;

        let avg_loss_pct = if metrics.avg_loss != 0.0 {
            (metrics.avg_loss / position_size as f64).abs()
        } else {
            0.02
        };

        let stop_loss_distance = (atr_estimate * 1.5 + spread_cost)
            .max(avg_loss_pct * 0.7)
            .min(0.05);

        let risk_reward_ratio = if metrics.avg_profit > 0.0 && metrics.avg_loss < 0.0 {
            (metrics.avg_profit / metrics.avg_loss.abs()).max(1.5)
        } else {
            2.0
        };

        let take_profit_distance = stop_loss_distance * risk_reward_ratio;

        let slippage_adjusted_stop = stop_loss_distance * (1.0 + market.recent_slippage);
        let slippage_adjusted_tp = take_profit_distance * (1.0 - market.recent_slippage * 0.5);

        (slippage_adjusted_stop, slippage_adjusted_tp)
    }

    fn calculate_position_confidence(&self, metrics: &PositionMetrics, market: &MarketConditions) -> f64 {
        let win_rate_confidence = if metrics.total_trades > 100 {
            metrics.win_rate * 0.9 + 0.1
        } else if metrics.total_trades > 50 {
            metrics.win_rate * 0.7 + 0.15
        } else {
            0.5
        };

        let sharpe_confidence = (metrics.sharpe_ratio / 3.0).clamp(0.0, 1.0);
        let oracle_confidence = market.oracle_confidence;

        let recency_weight = if metrics.total_trades > 0 {
            let recent_count = self.recent_trades.read().unwrap().iter().take(20).count();
            (recent_count as f64 / 20.0).min(1.0)
        } else {
            0.0
        };

        let market_confidence = 1.0 - (market.network_congestion * 0.5 + market.volatility * 2.0).min(0.7);

        let weighted_confidence = win_rate_confidence * 0.30
            + sharpe_confidence * 0.25
            + oracle_confidence * 0.20
            + recency_weight * 0.15
            + market_confidence * 0.10;

        weighted_confidence.clamp(CONFIDENCE_THRESHOLD * 0.5, 0.95)
    }

    fn calculate_expected_value(
        &self,
        size: u64,
        metrics: &PositionMetrics,
        market: &MarketConditions,
    ) -> f64 {
        let win_prob = metrics.win_rate.clamp(0.0, 1.0);
        let loss_prob = 1.0 - win_prob;

        let expected_profit = if metrics.avg_profit > 0.0 {
            metrics.avg_profit * (1.0 - market.recent_slippage)
        } else {
            size as f64 * 0.01
        };

        let expected_loss = if metrics.avg_loss < 0.0 {
            metrics.avg_loss * (1.0 + market.recent_slippage)
        } else {
            -(size as f64 * 0.02)
        };

        let gas_cost = market.priority_fee as f64 * GAS_SAFETY_MULTIPLIER;
        let spread_cost = size as f64 * (market.spread_bps / 10_000.0);

        let gross_ev = win_prob * expected_profit + loss_prob * expected_loss;
        let net_ev = gross_ev - gas_cost - spread_cost;

        net_ev * (1.0 - market.network_congestion * 0.2)
    }

    /// Helper function to generate candidate tips around the base tip
    fn candidate_tips(base_tip: u64) -> Vec<u64> {
        let base = base_tip as f64;
        // Generate a geometric sequence of tips from 0.5x to 4x base tip
        (0..=10).map(|i| {
            let scale = 0.5 * 1.3f64.powi(i);
            (base * scale).round() as u64
        }).collect()
    }

    /// Sigmoid fallback for inclusion probability
    fn sigmoid_inclusion(tip: u64, k: f64, mid: f64) -> f64 {
        1.0 / (1.0 + (-k * (tip as f64 - mid)).exp())
    }

   /// Gets the current slot phase based on Solana's on-chain clock and slot progress.
/// This uses the actual slot number and estimated slot duration to determine
/// Early / Mid / Late phases, and can be extended to be leader-aware.
fn slot_phase_from_now() -> SlotPhase {
    use solana_sdk::sysvar::Sysvar;

    // Pull the on-chain clock sysvar (current slot, epoch, etc.)
    let clock = Clock::get().expect("failed to read Clock sysvar");

    // Estimated slot duration in milliseconds (tunable if cluster changes)
    const SLOT_MS: u64 = 400;

    // Optional: number of phases and their boundaries as fractions of slot
    const EARLY_FRAC: f64 = 1.0 / 3.0;
    const MID_FRAC:   f64 = 2.0 / 3.0;

    // Derive the "slot progress" in ms from PoH ticks
    // On mainnet-beta, default ticks_per_slot = 64, PoH tick ~ 6.25ms
    let ticks_per_slot = clock.ticks_per_slot.max(1);
    let tick_duration_ms = 400.0 / ticks_per_slot as f64; // ~6.25ms
    let ticks_into_slot = clock.unix_timestamp_nanos
        .saturating_sub(clock.slot_start_time_ns.unwrap_or(0)) // if you store this
        / 1_000_000; // ns -> ms
    let progress_ms = ticks_into_slot as f64;

    // Fallback: if we don't have slot_start_time_ns, approximate using wallclock
    let approx_progress_ms = {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        now_ms % SLOT_MS
    } as f64;

    let slot_progress_ms = if progress_ms > 0.0 { progress_ms } else { approx_progress_ms };

    // Normalize to fraction of slot
    let frac = (slot_progress_ms / SLOT_MS as f64).clamp(0.0, 1.0);

    match frac {
        f if f < EARLY_FRAC => SlotPhase::Early,
        f if f < MID_FRAC   => SlotPhase::Mid,
        _                   => SlotPhase::Late,
    }
}

    fn calculate_max_gas_inclusion_aware(
        &self,
        position_size: u64,
        market: &MarketConditions,
        model: &ImpactModel,
        ev: f64,
    ) -> u64 {
        let base_tip = market.priority_fee;
        let size_ratio = (position_size as f64 / 1_000_000.0).min(10.0);
        let vol_mult = (market.volatility * 50.0).clamp(1.0, 2.0);
        let phase = Self::slot_phase_from_now();
        
        // Generate candidate tips around the base tip
        let tips = Self::candidate_tips(base_tip);
        let mut best = (base_tip, f64::MIN);
        
        // Evaluate each candidate tip
        for tip in tips {
            // Use empirical CDF if available, otherwise fall back to sigmoid
            let p_empirical = model.inclusion_hist.p_inclusion(phase, tip);
            let p_sigmoid = Self::sigmoid_inclusion(tip, model.incl_sigmoid_k, model.incl_sigmoid_mid);
            
            // Hybrid approach: take max of empirical and sigmoid (with sigmoid downweighted)
            let p_incl = p_empirical.max(p_sigmoid * 0.5);
            
            // Calculate objective: expected value minus cost
            let obj = p_incl * ev - (tip as f64) * vol_mult * size_ratio.sqrt();
            
            if obj > best.1 {
                best = (tip, obj);
            }
        }

        // Apply congestion-based multiplier
        let congestion_multiplier = if market.network_congestion > 0.8 {
            2.5
        } else if market.network_congestion > 0.6 {
            2.0
        } else if market.network_congestion > 0.4 {
            1.5
        } else {
            1.2
        };

        // Final tip with caps
        let max_tip = (best.0 as f64 * congestion_multiplier) as u64;
        let position_pct = position_size as f64 * 0.001;
        
        // Cap tip at 1% of position size (in basis points) with a minimum of 5,000 lamports
        max_tip
            .min((position_pct * 0.01) as u64)
            .max(5_000)
    }

    // --------------------- Metrics maintenance ---------------------

    fn update_metrics_and_trackers(&self, result: &TradeResult) {
        // Update metrics
        {
            let mut metrics = self.position_metrics.write().unwrap();
            metrics.total_trades += 1;
            metrics.last_update = result.timestamp;

            let is_win = result.pnl > 0.0;
            let alpha = 2.0 / (metrics.total_trades as f64 + 1.0).min(100.0);

            if is_win {
                metrics.current_streak = metrics.current_streak.max(0) + 1;
                metrics.avg_profit = metrics.avg_profit * (1.0 - alpha) + result.pnl * alpha;
            } else {
                metrics.current_streak = metrics.current_streak.min(0) - 1;
                metrics.avg_loss = metrics.avg_loss * (1.0 - alpha) + result.pnl * alpha;
            }

            let win_count = self
                .recent_trades
                .read()
                .unwrap()
                .iter()
                .take(100)
                .filter(|t| t.pnl > 0.0)
                .count();
            metrics.win_rate = if metrics.total_trades < 100 {
                win_count as f64 / metrics.total_trades as f64
            } else {
                win_count as f64 / 100.0
            };

            self.update_sharpe_ratio(&mut metrics);
            self.update_max_drawdown(&mut metrics);
        }

        // Update performance tracker
        self.update_performance_tracker();
    }

    fn update_sharpe_ratio(&self, metrics: &mut PositionMetrics) {
        let trades = self.recent_trades.read().unwrap();
        if trades.len() < 30 {
            return;
        }

        let rets: Vec<f64> = trades
            .iter()
            .take(252)
            .map(|t| t.pnl / (t.size as f64).max(1.0))
            .collect();

        if rets.is_empty() {
            return;
        }

        let mean = mean(&rets);
        let std = std_sample(&rets);
        if std == 0.0 {
            metrics.sharpe_ratio = 0.0;
            return;
        }

        let rf_per_period = RISK_FREE_RATE / 252.0;
        let excess = mean - rf_per_period;
        metrics.sharpe_ratio = excess / std * 252.0_f64.sqrt();
    }

    fn update_max_drawdown(&self, metrics: &mut PositionMetrics) {
        let trades = self.recent_trades.read().unwrap();
        if trades.is_empty() {
            metrics.max_drawdown = 0.0;
            return;
        }

        // Equity curve on normalized per-trade returns
        let mut eq: f64 = 0.0;
        let mut peak: f64 = 0.0;
        let mut max_dd: f64 = 0.0;

        for t in trades.iter().rev().take(1000) {
            let r = t.pnl / (t.size as f64).max(1.0);
            eq += r;
            peak = peak.max(eq);
            let dd = if peak > 0.0 { (peak - eq) / peak } else { 0.0 };
            max_dd = max_dd.max(dd);
        }

        metrics.max_drawdown = max_dd.clamp(0.0, 1.0);
    }

    fn update_performance_tracker(&self) {
        let trades = self.recent_trades.read().unwrap();
        if trades.len() < 30 {
            return;
        }

        let mut rets: Vec<f64> = trades
            .iter()
            .take(252)
            .map(|t| t.pnl / (t.size as f64).max(1.0))
            .collect();

        if rets.is_empty() {
            return;
        }

        let mu = mean(&rets);
        let sd = std_sample(&rets);
        let rf = RISK_FREE_RATE / 252.0;
        let excess = mu - rf;

        // Sortino
        let downside: Vec<f64> = rets.iter().map(|r| (*r - rf).min(0.0)).collect();
        let dd = std_sample(&downside);
        let sortino = if dd > 0.0 {
            excess / dd * 252.0_f64.sqrt()
        } else {
            0.0
        };

        // Calmar
        let mut eq = 0.0;
        let mut peak = 0.0;
        let mut max_dd = 0.0;
        for r in &rets {
            eq += *r;
            peak = peak.max(eq);
            let dd_cur = if peak > 0.0 { (peak - eq) / peak } else { 0.0 };
            max_dd = max_dd.max(dd_cur);
        }
        let ann_return = mu * 252.0;
        let calmar = if max_dd > 0.0 { ann_return / max_dd } else { 0.0 };

        // Omega
        let (gains, losses) = rets.iter().fold((0.0, 0.0), |(g, l), r| {
            let ex = *r - rf;
            if ex >= 0.0 {
                (g + ex, l)
            } else {
                (g, l + (-ex))
            }
        });
        let omega = if losses > 0.0 { gains / losses } else { f64::INFINITY };

        // Tail ratio using percentiles
        rets.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p95 = percentile(&rets, 0.95);
        let p05 = percentile(&rets, 0.05);
        let tail_ratio = if p05 != 0.0 { p95 / p05.abs() } else { 0.0 };

        // ES levels on normalized returns (convert to capital space in calculate_risk_capacity)
        let (es95_r, es99_r) = empirical_es(&rets, (0.95, 0.99));

        let mut tracker = self.performance_tracker.write().unwrap();
        tracker.rolling_sharpe = if sd > 0.0 {
            (excess / sd) * 252.0_f64.sqrt()
        } else {
            0.0
        };
        tracker.rolling_sortino = sortino;
        tracker.calmar_ratio = calmar;
        tracker.omega_ratio = omega;
        tracker.tail_ratio = tail_ratio;
        // Keep return-space ES here; capital-space ES is in calculate_risk_capacity
        tracker.es95 = es95_r.abs();
        tracker.es99 = es99_r.abs();
    }

    // --------------------- Risk utilities ---------------------

    fn empirical_es_levels(&self, trades: &[TradeResult], capital: u64) -> (f64, f64) {
        if trades.len() < 30 {
            // Fallback conservative
            return (0.05, 0.10);
        }
        let mut rets: Vec<f64> = trades
            .iter()
            .map(|t| t.pnl / (t.size as f64).max(1.0))
            .collect();

        rets.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let (es95_r, es99_r) = empirical_es(&rets, (0.95, 0.99));
        // Convert to capital relative scale using a conservative mapping:
        // assume position is a fraction of capital bounded by MAX_POSITION_RATIO.
        let pos_frac = MAX_POSITION_RATIO;
        let es95_cap = (es95_r.abs() * pos_frac).clamp(0.0, 1.0);
        let es99_cap = (es99_r.abs() * pos_frac * 1.5).clamp(0.0, 1.0);

        (es95_cap, es99_cap)
    }
}

// ========================= Math helpers =========================

fn mean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    xs.iter().sum::<f64>() / xs.len() as f64
}

fn std_sample(xs: &[f64]) -> f64 {
    let n = xs.len();
    if n <= 1 {
        return 0.0;
    }
    let m = mean(xs);
    let var = xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (n as f64 - 1.0);
    var.sqrt()
}

fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let n = sorted.len() as f64;
    let idx = ((n - 1.0) * q).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

// Empirical Expected Shortfall at levels (c1, c2) on sorted returns
fn empirical_es(sorted_rets: &[f64], levels: (f64, f64)) -> (f64, f64) {
    let n = sorted_rets.len();
    if n == 0 {
        return (0.0, 0.0);
    }
    let idx95 = ((1.0 - levels.0) * n as f64).floor().clamp(0.0, (n - 1) as f64) as usize;
    let idx99 = ((1.0 - levels.1) * n as f64).floor().clamp(0.0, (n - 1) as f64) as usize;

    let es95 = if idx95 == 0 {
        sorted_rets[0]
    } else {
        sorted_rets[..=idx95].iter().sum::<f64>() / (idx95 + 1) as f64
    };
    let es99 = if idx99 == 0 {
        sorted_rets[0]
    } else {
        sorted_rets[..=idx99].iter().sum::<f64>() / (idx99 + 1) as f64
    };
    (es95, es99)
}

fn mean_and_se(xs: &[f64]) -> (f64, f64) {
    if xs.is_empty() {
        return (0.0, 0.0);
    }
    let m = mean(xs);
    let sd = std_sample(xs);
    let se = if xs.len() > 0 { sd / (xs.len() as f64).sqrt() } else { 0.0 };
    (m, se)
}

fn wilson_lower_bound(successes: u64, n: u64, z: f64) -> f64 {
    if n == 0 {
        return 0.0;
    }
    let p = successes as f64 / n as f64;
    let denom = 1.0 + (z * z) / n as f64;
    let centre = p + (z * z) / (2.0 * n as f64);
    let adj = z * ((p * (1.0 - p) + (z * z) / (4.0 * n as f64)) / n as f64).sqrt();
    ((centre - adj) / denom).clamp(0.0, 1.0)
}

fn sigmoid_inclusion(tip: f64, k: f64, mid: f64) -> f64 {
    let x = (tip - mid) / mid.max(1.0);
    1.0 / (1.0 + (-k * x).exp())
}

// ========================= Examples =========================

/// Example usage of the optimizer with venue models and observation recording
/// 
/// ```rust
/// use fractional_position_optimizer::{
///     FractionalPositionOptimizer, 
///     SlotPhase,
///     VenueModel,
///     OrderBookSnapshot,
///     CpmmPool
/// };
///
/// // Initialize the optimizer
/// let optimizer = FractionalPositionOptimizer::new(1_000_000_000);
///
/// // Set up venue model (Order Book example)
/// optimizer.set_venue_model(VenueModel::OrderBook);
/// optimizer.set_orderbook_snapshot(OrderBookSnapshot {
///     bids: vec![],
///     asks: vec![],
///     timestamp: 0,
///     mid_price: 100.0,
///     spread: 0.001,
/// });
///
/// // Or for AMM pools:
/// optimizer.set_venue_model(VenueModel::Cpmm);
/// optimizer.set_cpmm_pool(CpmmPool {
///     reserves_a: 1_000_000,
///     reserves_b: 1_000_000,
///     fee_bps: 30,
///     last_updated: 0,
/// });
///
/// // After submitting a transaction, record the outcome
/// let phase = SlotPhase::Mid; // Your slot phase tracker
/// optimizer.record_inclusion_outcome(phase, 5000, true);
/// ```

// ========================= Tests =========================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_position_optimizer_initialization() {
        let optimizer = FractionalPositionOptimizer::new(1_000_000_000);
        assert_eq!(*optimizer.capital_base.read().unwrap(), 1_000_000_000);
    }

    #[test]
    fn test_optimal_position_calculation() {
        let optimizer = FractionalPositionOptimizer::new(1_000_000_000);
        let market = MarketConditions {
            volatility: 0.02,
            liquidity_depth: 10_000_000,
            spread_bps: 5.0,
            priority_fee: 10_000,
            network_congestion: 0.3,
            oracle_confidence: 0.95,
            recent_slippage: 0.001,
        };

        let position = optimizer.calculate_optimal_position(1_000_000, &market, 1000);
        assert!(position.size >= MIN_POSITION_SIZE);
        assert!((1.0..=MAX_LEVERAGE).contains(&position.leverage));
    }

    #[test]
    fn test_wilson_lower_bound() {
        let w = wilson_lower_bound(60, 100, 1.96);
        assert!(w > 0.45 && w < 0.65);
    }

    #[test]
    fn test_empirical_es() {
        let mut rets = vec![-0.05, -0.02, -0.01, 0.0, 0.01, 0.02, 0.05];
        rets.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let (es95, es99) = empirical_es(&rets, (0.95, 0.99));
        assert!(es95 <= 0.0 && es99 <= es95);
    }
    
    #[test]
    fn test_hist_monotonicity() {
        let mut h = InclusionHistogram::new(vec![5_000, 10_000, 20_000, 50_000], 0.5);
        
        // Add some observations
        h.update(SlotPhase::Mid, 5_000, true);
        h.update(SlotPhase::Mid, 5_000, false);
        h.update(SlotPhase::Mid, 10_000, true);
        h.update(SlotPhase::Mid, 20_000, true);
        
        // enforce_monotonic ensures non-decreasing CDF
        assert!(h.cdf_mid[0] <= h.cdf_mid[1], "CDF should be non-decreasing");
        assert!(h.cdf_mid[1] <= h.cdf_mid[2], "CDF should be non-decreasing");
        assert!(h.cdf_mid[2] <= h.cdf_mid[3], "CDF should be non-decreasing");
    }
    
    #[test]
    fn test_orderbook_impact_increasing() {
        let ob = OrderBookSnapshot {
            bids: vec![],
            asks: vec![],
            timestamp: 0,
            mid_price: 100.0,
            spread: 0.001,
        };
        
        // Test that larger orders have higher impact
        let small_order_impact = FractionalPositionOptimizer::orderbook_impact_cost(10_000.0, &ob);
        let large_order_impact = FractionalPositionOptimizer::orderbook_impact_cost(100_000.0, &ob);
        
        assert!(
            large_order_impact > small_order_impact,
            "Larger orders should have higher impact"
        );
        
        // Test edge case: zero size has zero impact
        assert_eq!(
            FractionalPositionOptimizer::orderbook_impact_cost(0.0, &ob),
            0.0,
            "Zero size should have zero impact"
        );
    }
}
