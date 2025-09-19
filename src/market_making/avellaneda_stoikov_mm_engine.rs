use anchor_lang::prelude::*;
use pyth_sdk_solana::Price;
use solana_program::clock::Clock;
use std::collections::{HashMap, VecDeque};
use std::cmp::Ordering;
use std::sync::Arc;
use tokio::sync::RwLock;

const MAX_PRICE_HISTORY: usize = 1000;
const VOLATILITY_WINDOW: usize = 100;
const GAMMA_DEFAULT: f64 = 0.1;
const KAPPA_DEFAULT: f64 = 1.5;
const ETA_DEFAULT: f64 = 0.01;
const MIN_SPREAD_BPS: f64 = 5.0;
const MAX_SPREAD_BPS: f64 = 200.0;
const INVENTORY_TARGET: f64 = 0.0;
const MAX_INVENTORY_RATIO: f64 = 0.8;
const TICK_SIZE: f64 = 0.00001;
const LOT_SIZE: f64 = 0.01;
const VOLATILITY_DECAY: f64 = 0.94;
const PRICE_IMPACT_COEFFICIENT: f64 = 0.0001;
const MIN_ORDER_SIZE: f64 = 0.1;
const MAX_ORDER_SIZE: f64 = 1000.0;
const RISK_AVERSION_MULTIPLIER: f64 = 2.0;
const TIME_HORIZON: f64 = 86400.0;
const VOLATILITY_FLOOR: f64 = 0.0001;
const VOLATILITY_CEILING: f64 = 0.5;
const SPREAD_BUFFER: f64 = 1.0001;
const INVENTORY_SKEW_FACTOR: f64 = 0.5;
const MAKER_FEE_BPS: f64 = -2.0;
const TAKER_FEE_BPS: f64 = 5.0;
const MIN_PROFIT_BPS: f64 = 3.0;
const ORACLE_CONFIDENCE_THRESHOLD: f64 = 0.02;
const MAX_PRICE_DEVIATION: f64 = 0.05;
const EWMA_ALPHA: f64 = 0.1;
const MICROSTRUCTURE_NOISE: f64 = 0.0001;

// ===== Elite desk signal/control constants =====
const TOXICITY_EWMA_ALPHA: f64 = 0.20;           // speed of adverse selection score
const TOXICITY_HALT_THRESHOLD: f64 = 0.85;       // if reached -> sizes collapse this cycle
const TOXICITY_WIDEN_BPS: f64 = 30.0;            // max extra spread (bps) at toxicity=1

const SLOT_RISK_WIDEN_BPS: f64 = 40.0;           // extra spread (bps) at slot_risk=1
const SLOT_RISK_SIZE_CUT: f64 = 0.60;            // size reduction at slot_risk=1

const CONF_WIDEN_MULT: f64 = 2.5;                // spread multiplier vs confidence proximity
const QUOTE_MIN_CHANGE_TICKS: i64 = 2;           // suppress micro flicker below this step
const MIN_TOUCH_TICKS: i64 = 1;                  // never cross tighter than 1 tick when equal

const ALPHA_EWMA: f64 = 0.20;                    // speed of micro-alpha EWMA
const ALPHA_SKEW_BPS_MAX: f64 = 20.0;            // cap reservation-price skew (bps)
const MOMENTUM_WINDOW_SHORT: usize = 12;         // short window for sign-consistent drift

// ===== Advanced edges =====
const MARKOUT_HORIZON_SECS: i64 = 2;            // horizon for post-fill markout toxicity (seconds)
const ORACLE_BAND_MULT: f64 = 1.0;              // clamp quotes inside ±(mult * confidence)
const QUOTE_MIN_TIME_SECS: i64 = 0;             // min secs between re-quotes (engine-level; 0 = off)

const GAMMA_TOX_WEIGHT: f64 = 1.2;              // gamma boost per unit toxicity
const GAMMA_VOL_WEIGHT: f64 = 0.8;              // gamma boost per normalized vol
const GAMMA_DD_WEIGHT: f64 = 0.8;               // gamma boost per drawdown ratio
const KAPPA_TOX_WEIGHT: f64 = 0.7;              // kappa reduction per toxicity
const KAPPA_VOL_WEIGHT: f64 = 0.5;              // kappa reduction per normalized vol

// ===== Cross-pool / venue / aging edges =====
const NETTING_AGGR_WEIGHT: f64 = 0.70;           // weight on external pools in net inventory
const NETTING_MAX_ABS_RATIO: f64 = 1.50;         // clamp |net_inv| <= 1.5 * max_inventory

const VENUE_SKEW_BPS_MAX: f64 = 15.0;            // per-venue skew cap (bps) vs core quote
const VENUE_SIZE_SCALE_MIN: f64 = 0.20;          // minimum size scale per venue
const VENUE_SIZE_SCALE_MAX: f64 = 1.50;          // maximum size scale per venue

const AGING_WIDEN_BPS_PER_SEC: f64 = 6.0;        // widen per second of stale quote
const AGING_WIDEN_BPS_CAP: f64 = 60.0;           // max aging widen

// ===== Intent buckets / venue learning / cancel governor =====
const INTENT_BUCKETS: usize = 5;                 // number of EV buckets for signed markouts
const INTENT_ALPHA: f64 = 0.15;                  // EWMA speed for bucket counts
const INTENT_WIDEN_BPS_MAX: f64 = 25.0;          // extra widen at most hostile intent

const VENUE_TOX_ALPHA: f64 = 0.15;               // EWMA for venue-level toxicity
const VENUE_TOX_CAP: f64 = 0.90;                 // cap venue toxicity
const VENUE_SLIP_ALPHA: f64 = 0.20;              // EWMA for venue slippage (bps)
const VENUE_SLIP_CAP_BPS: f64 = 50.0;            // cap for slippage estimate

const FLOW_TOX_WEIGHT_GAMMA: f64 = 0.50;         // additional gamma amplification by hostile flow
const FLOW_TOX_WEIGHT_KAPPA: f64 = 0.35;         // additional kappa reduction by hostile flow

const CANCEL_RATE_MAX_PER_SEC: f64 = 30.0;       // tokens/second
const CANCEL_BURST_MAX: f64 = 60.0;              // token bucket capacity
const CANCELS_PER_QUOTE_CHANGE: f64 = 2.0;       // assume 2 cancels for bid+ask replace

// ===== Queue-position shading & Jito tip optimizer =====
const QUEUE_EWMA_ALPHA: f64 = 0.25;              // speed for L2 size/queue EWMA
const QUEUE_POS_SKEW_TICKS_MAX: i64 = 2;         // max +/- ticks from queue position
const QUEUE_IMB_SKEW_TICKS_MAX: i64 = 2;         // max +/- ticks from L2 imbalance
const QUEUE_POS_FRONT_THRESH: f64 = 0.30;        // "back of queue" if < 30% ahead
const QUEUE_POS_BACK_THRESH: f64  = 0.70;        // "front of queue" if > 70% ahead

const LAMPORTS_PER_SOL: u64 = 1_000_000_000;
const JITO_TIP_MIN_LAMPORTS: u64 = 10_000;       // 0.00001 SOL (floor)
const JITO_TIP_MAX_LAMPORTS: u64 = 5_000_000;    // 0.005 SOL (cap)
const JITO_TIP_POWER: f64 = 1.30;                // convexity of tip vs urgency*EV
const JITO_TIP_FRACTION_OF_EV: f64 = 0.08;       // tip ~ 8% of EV (tuned)

#[derive(Clone, Debug)]
pub struct ASMMEngine {
    pub symbol: String,
    pub base_decimals: u8,
    pub quote_decimals: u8,

    // Avellaneda–Stoikov core params
    pub gamma: f64,
    pub kappa: f64,
    pub eta: f64,

    // Baselines for dynamic tuning
    pub gamma_base: f64,
    pub kappa_base: f64,

    // Vol estimate (annualized, bounded)
    pub sigma: f64,

    // Inventory & cost basis
    pub inventory: f64,            // base units (+ long, - short)
    pub inventory_vwap: f64,       // VWAP of current open position
    pub target_inventory: f64,
    pub max_inventory: f64,
    pub min_inventory: f64,

    // Market & state
    pub price_history: Arc<RwLock<VecDeque<PricePoint>>>,
    pub volatility_estimator: Arc<RwLock<VolatilityEstimator>>,
    pub risk_manager: Arc<RwLock<RiskManager>>,

    pub last_update: i64,          // unix ts for market data
    pub time_to_close: f64,        // horizon (secs)
    pub reservation_price: f64,
    pub optimal_spread: f64,
    pub bid_price: f64,
    pub ask_price: f64,
    pub mid_price: f64,
    pub oracle_price: f64,
    pub confidence_interval: f64,

    // Quoted sizes
    pub order_size_bid: f64,
    pub order_size_ask: f64,

    // PnL & stats
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub total_volume: f64,
    pub trade_count: u64,

    pub active: bool,

    // Micro performance helpers
    pub last_quote_id: u64,        // monotonic quote tag (for routing/debug)

    // ===== Elite controls (previous round) =====
    pub slot_risk: f64,            // [0..1]
    pub toxicity_score: f64,       // [0..1]
    pub alpha_signal: f64,         // signed micro-alpha

    // Anti-flicker memory
    pub last_published_bid: f64,
    pub last_published_ask: f64,

    // ===== New fields =====
    pub last_quote_ts: i64,                       // when we last finalized quotes
    pub markouts: VecDeque<FillEvent>,            // pending fills awaiting markout horizon

    // Cross-pool inventory netting
    pub external_inventory: HashMap<String, f64>, // base qty per external pool/venue
    pub pool_weights: HashMap<String, f64>,       // [0..1] weight per pool in netting

    // Per-venue parameters (for shadow quotes)
    pub venues: Vec<VenueParam>,

    // Flow intent classifier stats (EWMA of signed markouts)
    pub tox_pos_ewma: f64,
    pub tox_neg_ewma: f64,
    pub flow_classifier: f64,                      // [-1..1], >0 = hostile/toxic

    // Alpha-bucketed intent model
    pub intent_pos_buckets: [f64; INTENT_BUCKETS], // EWMA count mass for positive PnL
    pub intent_neg_buckets: [f64; INTENT_BUCKETS], // EWMA count mass for negative PnL

    // Cancel governor (token bucket)
    pub cancel_tokens: f64,
    pub last_cancel_refill_ts: i64,

    // ===== Per-venue L2 state & submission economics =====
    pub venue_books: HashMap<String, BookState>,  // venue -> L2/queue state
    pub tip_ref_sol_usd: f64,                     // SOL/USD ref for tip conversion (fallback)
}

#[derive(Clone, Copy, Debug)]
struct FillEvent {
    side: OrderSide,
    price: f64,
    qty:   f64,
    ts:    i64,
}

#[derive(Clone, Debug)]
pub struct VenueParam {
    pub name: String,
    pub maker_fee_bps: f64,    // negative for rebate
    pub taker_fee_bps: f64,
    pub liquidity_score: f64,  // [0..1], higher is deeper/safer
    pub slippage_bps: f64,     // expected additional slippage (bps)
    pub toxicity: f64,         // [0..1] venue-specific adverse selection
    pub last_update: i64,
}

#[derive(Clone, Debug)]
pub struct VenueQuote {
    pub venue: String,
    pub bid_price: f64,
    pub bid_size: f64,
    pub ask_price: f64,
    pub ask_size: f64,
    pub timestamp: i64,
}

#[derive(Clone, Debug)]
pub struct BookState {
    pub best_bid_qty: f64,  // L2 size at best bid (base units)
    pub best_ask_qty: f64,  // L2 size at best ask
    pub our_bid_qty:  f64,  // our posted bid size at venue (if any)
    pub our_ask_qty:  f64,  // our posted ask size at venue (if any)
    pub qpos_bid_ahead_frac: f64, // EWMA fraction of queue ahead of us on bid [0..1]
    pub qpos_ask_ahead_frac: f64, // EWMA fraction on ask [0..1]
    pub last_update: i64,
}

#[derive(Clone, Debug)]
pub struct JitoTipPlan {
    pub use_bundle: bool,
    pub tip_lamports: u64,
}

#[derive(Clone, Debug)]
pub struct PricePoint {
    pub timestamp: i64,
    pub price: f64,
    pub volume: f64,
    pub spread: f64,
}

#[derive(Clone, Debug)]
pub struct VolatilityEstimator {
    // Annualized realized vol from rolling variance of log-returns
    pub realized_vol: f64,
    // GARCH(1,1) volatility proxy (annualized)
    pub garch_vol: f64,
    // EWMA volatility proxy (annualized)
    pub ewma_vol: f64,

    // State
    pub returns: VecDeque<f64>,          // log-returns (most recent at back)
    pub squared_returns: VecDeque<f64>,  // squared log-returns

    // GARCH params (kept stable <1.0 persistence)
    pub alpha: f64,
    pub beta:  f64,
    pub omega: f64,

    // Extra robust stats
    pub ewma_r2: f64,        // EWMA of r^2 (for quick sigma proxy)
    pub last_return: f64,    // last observed log-return
}

#[derive(Clone, Debug)]
pub struct RiskManager {
    pub max_position_size: f64,
    pub max_order_size: f64,
    pub daily_loss_limit: f64,
    pub current_daily_pnl: f64,
    pub position_limits_breached: bool,
    pub trading_halted: bool,
    pub last_risk_check: i64,
}

impl ASMMEngine {
    pub fn new(
        symbol: String,
        base_decimals: u8,
        quote_decimals: u8,
        max_inventory: f64,
    ) -> Self {
        let volatility_estimator = Arc::new(RwLock::new(VolatilityEstimator {
            realized_vol: 0.01,
            garch_vol: 0.01,
            ewma_vol: 0.01,
            returns: VecDeque::with_capacity(VOLATILITY_WINDOW),
            squared_returns: VecDeque::with_capacity(VOLATILITY_WINDOW),
            alpha: 0.05,
            beta: 0.94,
            omega: 0.000001,
            ewma_r2: 0.01f64.powi(2),
            last_return: 0.0,
        }));

        let risk_manager = Arc::new(RwLock::new(RiskManager {
            max_position_size: max_inventory,
            max_order_size: (max_inventory * 0.1).max(MIN_ORDER_SIZE),
            daily_loss_limit: (max_inventory * 0.02).max(10.0),
            current_daily_pnl: 0.0,
            position_limits_breached: false,
            trading_halted: false,
            last_risk_check: 0,
        }));

        Self {
            symbol,
            base_decimals,
            quote_decimals,
            gamma: GAMMA_DEFAULT,
            kappa: KAPPA_DEFAULT,
            eta: ETA_DEFAULT,

            gamma_base: GAMMA_DEFAULT,
            kappa_base: KAPPA_DEFAULT,

            sigma: 0.01,

            inventory: 0.0,
            inventory_vwap: 0.0,
            target_inventory: INVENTORY_TARGET,
            max_inventory,
            min_inventory: -max_inventory,

            price_history: Arc::new(RwLock::new(VecDeque::with_capacity(MAX_PRICE_HISTORY))),
            volatility_estimator,
            risk_manager,

            last_update: 0,
            time_to_close: TIME_HORIZON,
            reservation_price: 0.0,
            optimal_spread: 0.0,
            bid_price: 0.0,
            ask_price: 0.0,
            mid_price: 0.0,
            oracle_price: 0.0,
            confidence_interval: 0.0,
            order_size_bid: MIN_ORDER_SIZE,
            order_size_ask: MIN_ORDER_SIZE,
            realized_pnl: 0.0,
            unrealized_pnl: 0.0,
            total_volume: 0.0,
            trade_count: 0,
            active: true,
            last_quote_id: 0,

            slot_risk: 0.0,
            toxicity_score: 0.0,
            alpha_signal: 0.0,
            last_published_bid: 0.0,
            last_published_ask: 0.0,

        last_quote_ts: 0,
        markouts: VecDeque::with_capacity(2048),

        external_inventory: HashMap::new(),
        pool_weights: HashMap::new(),
        venues: Vec::new(),

        tox_pos_ewma: 0.0,
        tox_neg_ewma: 0.0,
        flow_classifier: 0.0,

        intent_pos_buckets: [0.0; INTENT_BUCKETS],
        intent_neg_buckets: [0.0; INTENT_BUCKETS],

        cancel_tokens: CANCEL_BURST_MAX,
        last_cancel_refill_ts: 0,

        // NEW
        venue_books: HashMap::new(),
        tip_ref_sol_usd: 100.0, // conservative default; set via set_tip_ref_sol_usd()
    }
    }

    /// === REPLACE: effective_params (adds conf-stress & book-thinness; clamps; zero-thrash) ===
    fn effective_params(&self) -> (f64, f64) {
        // Vol normalization versus ceiling
        let vol_norm = (self.sigma / VOLATILITY_CEILING).clamp(0.0, 1.0);

        // Drawdown snapshot (non-blocking)
        let dd_ratio = if let Ok(rm) = self.risk_manager.try_read() {
            if rm.daily_loss_limit > 0.0 && rm.current_daily_pnl < 0.0 {
                (-rm.current_daily_pnl / rm.daily_loss_limit).clamp(0.0, 1.5)
            } else { 0.0 }
        } else { 0.0 };

        // Hostile flow + oracle confidence stress
        let hostile = self.flow_classifier.clamp(0.0, 1.0);
        let conf_stress = (self.confidence_interval / ORACLE_CONFIDENCE_THRESHOLD).clamp(0.0, 1.25);

        // Approx "book thinness": if we have any venue snapshot, use min depth proxy
        let mut thin = 0.0;
        if let Some((_k, bs)) = self.venue_books.iter().next() {
            // scale: if both sides ~0 ⇒ thin=1; if each ≥ base_size ⇒ thin→0
            let base_size = (self.max_inventory * 0.05).max(MIN_ORDER_SIZE);
            let depth_unit = (bs.best_bid_qty.min(bs.best_ask_qty)) / (base_size + 1e-9);
            thin = (1.0 - depth_unit.clamp(0.0, 1.0)).clamp(0.0, 1.0);
        }

        // Compose smoothed multipliers
        let mut gamma_eff = self.gamma_base * (1.0
            + GAMMA_VOL_WEIGHT * vol_norm
            + GAMMA_TOX_WEIGHT * self.toxicity_score.clamp(0.0,1.0)
            + FLOW_TOX_WEIGHT_GAMMA * hostile
            + GAMMA_DD_WEIGHT * dd_ratio
            + 0.35 * thin
            + 0.25 * conf_stress);

        let mut kappa_eff = self.kappa_base * (1.0
            - KAPPA_TOX_WEIGHT * self.toxicity_score.clamp(0.0,1.0)
            - KAPPA_VOL_WEIGHT * vol_norm
            - FLOW_TOX_WEIGHT_KAPPA * hostile
            - 0.25 * thin
            - 0.15 * conf_stress);

        // Final clamps to sane ranges (no oscillation)
        gamma_eff = gamma_eff.clamp(1e-6, 1.0);
        kappa_eff = kappa_eff.clamp(1e-6, 10.0);
        (gamma_eff, kappa_eff)
    }

    fn net_inventory(&self) -> f64 {
        // own inventory + weighted external pools
        let mut ext_sum = 0.0;
        for (k, qty) in self.external_inventory.iter() {
            let w = *self.pool_weights.get(k).unwrap_or(&1.0);
            ext_sum += w * qty;
        }
        let net = self.inventory + NETTING_AGGR_WEIGHT * ext_sum;
        // Clamp to avoid over-reaction
        let max_abs = self.max_inventory * NETTING_MAX_ABS_RATIO;
        net.clamp(-max_abs, max_abs)
    }

    pub fn set_external_inventory(&mut self, pool: String, base_qty: f64) {
        self.external_inventory.insert(pool, base_qty);
    }

    pub fn set_pool_weight(&mut self, pool: String, weight: f64) {
        self.pool_weights.insert(pool, weight.clamp(0.0, 1.0));
    }

    pub fn set_venues(&mut self, venues: Vec<VenueParam>) {
        self.venues = venues;
    }

    fn update_flow_classifier(&mut self, pnl: f64) {
        // Positive pnl at horizon => friendly; negative => hostile
        let alpha = TOXICITY_EWMA_ALPHA;
        if pnl >= 0.0 {
            self.tox_pos_ewma = (1.0 - alpha) * self.tox_pos_ewma + alpha * pnl.min(self.mid_price);
        } else {
            let mag = (-pnl).min(self.mid_price);
            self.tox_neg_ewma = (1.0 - alpha) * self.tox_neg_ewma + alpha * mag;
        }
        let denom = (self.tox_pos_ewma + self.tox_neg_ewma).max(1e-9);
        self.flow_classifier = (self.tox_neg_ewma - self.tox_pos_ewma) / denom; // [-1..1]
    }

    fn intent_bucket_index(pnl: f64, mid: f64) -> usize {
        // Map pnl to symmetric buckets using pnl as fraction of mid; robust to scale
        let x = (pnl / mid.max(TICK_SIZE)).clamp(-0.01, 0.01); // ±1% band
        // uniform buckets across [-1,1]
        let s = if x >= 0.0 { x / 0.01 } else { -x / 0.01 };
        let idx = (s * (INTENT_BUCKETS as f64 - 1.0)).round() as isize;
        idx.clamp(0, INTENT_BUCKETS as isize - 1) as usize
    }

    fn update_intent_buckets(&mut self, pnl: f64) {
        let alpha = INTENT_ALPHA;
        let idx = ASMMEngine::intent_bucket_index(pnl, self.mid_price);
        if pnl >= 0.0 {
            for i in 0..INTENT_BUCKETS {
                if i == idx { self.intent_pos_buckets[i] = (1.0 - alpha) * self.intent_pos_buckets[i] + alpha * 1.0; }
                else        { self.intent_pos_buckets[i] = (1.0 - alpha) * self.intent_pos_buckets[i]; }
            }
        } else {
            for i in 0..INTENT_BUCKETS {
                if i == idx { self.intent_neg_buckets[i] = (1.0 - alpha) * self.intent_neg_buckets[i] + alpha * 1.0; }
                else        { self.intent_neg_buckets[i] = (1.0 - alpha) * self.intent_neg_buckets[i]; }
            }
        }
    }

    fn intent_hostility_widen_bps(&self) -> f64 {
        // Compare mass in extreme negative bucket vs extreme positive bucket
        let neg_tail = self.intent_neg_buckets[INTENT_BUCKETS - 1];
        let pos_tail = self.intent_pos_buckets[INTENT_BUCKETS - 1];
        let denom = (neg_tail + pos_tail).max(1e-6);
        let hostility = (neg_tail - pos_tail).max(0.0) / denom; // [0..1]
        hostility * INTENT_WIDEN_BPS_MAX
    }

    // Venue toxicity/slippage online updates (callable by router)
    pub fn note_venue_fill(&mut self, venue: &str, side: OrderSide, price: f64, qty: f64, ts: i64) {
        let mid = self.mid_price.max(TICK_SIZE);
        // signed slippage in bps vs mid: positive if we paid above mid for a buy (bad) or sold below mid (bad)
        let slip_bps = match side {
            OrderSide::Buy  => ((price - mid) / mid * 10_000.0).max(0.0),
            OrderSide::Sell => ((mid - price) / mid * 10_000.0).max(0.0),
        }.min(VENUE_SLIP_CAP_BPS);

        if let Some(v) = self.venues.iter_mut().find(|x| x.name == venue) {
            // Toxicity: adversarial if immediate drift against us (use last_return sign)
            let ve = futures::executor::block_on(self.volatility_estimator.read());
            let adverse = match side { OrderSide::Buy => ve.last_return < 0.0, OrderSide::Sell => ve.last_return > 0.0 };
            let tox_hit = if adverse { 1.0 } else { 0.0 };

            v.toxicity = ((1.0 - VENUE_TOX_ALPHA) * v.toxicity + VENUE_TOX_ALPHA * tox_hit).min(VENUE_TOX_CAP);
            v.slippage_bps = ((1.0 - VENUE_SLIP_ALPHA) * v.slippage_bps + VENUE_SLIP_ALPHA * slip_bps)
                .min(VENUE_SLIP_CAP_BPS);
            v.last_update = ts;
        }
    }

    fn process_markouts(&mut self, now_ts: i64) {
        // Longer horizon in high vol (caps at 3x)
        let h_secs = ((MARKOUT_HORIZON_SECS as f64) * (1.0 + 4.0 * self.sigma).clamp(1.0, 3.0)) as i64;

        while let Some(fe) = self.markouts.front().copied() {
            if now_ts - fe.ts < h_secs { break; }
            let pnl = match fe.side {
                OrderSide::Buy  => (self.mid_price - fe.price) * fe.qty,
                OrderSide::Sell => (fe.price - self.mid_price) * fe.qty,
            };
            let hit = if pnl < 0.0 { 1.0 } else { 0.0 };
            self.toxicity_score = (1.0 - TOXICITY_EWMA_ALPHA) * self.toxicity_score + TOXICITY_EWMA_ALPHA * hit;
            self.update_flow_classifier(pnl);
            self.update_intent_buckets(pnl);
            self.markouts.pop_front();
        }
    }

    /// === REPLACE: update_market_data (oracle guards + adaptive confidence + anti-desync) ===
    pub async fn update_market_data(
        &mut self,
        oracle_price: f64,
        confidence: f64,
        timestamp: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if !(oracle_price.is_finite()) || oracle_price <= 0.0 {
            return Err("Invalid oracle price".into());
        }

        // Regime-adaptive confidence: higher vol allows more band slack; low vol tightens
        let conf_mult = (0.75 + 2.5 * self.sigma).clamp(0.75, 3.0);
        let conf_limit = ORACLE_CONFIDENCE_THRESHOLD * conf_mult;
        if confidence >= conf_limit {
            return Err("Oracle confidence too HIGH for regime".into());
        }

        // Dynamic deviation guard vs previous oracle
        if self.oracle_price > 0.0 {
            let dyn_dev = MAX_PRICE_DEVIATION * (1.0 + 3.0 * self.sigma).clamp(1.0, 3.0);
            let price_deviation = (oracle_price - self.oracle_price).abs() / self.oracle_price.max(1.0);
            if price_deviation > dyn_dev {
                return Err("Oracle price deviation exceeds dynamic threshold".into());
            }
        }

        self.oracle_price = oracle_price;
        self.confidence_interval = confidence;
        self.mid_price = oracle_price;

        {
            let mut ph = self.price_history.write().await;
            ph.push_back(PricePoint {
                timestamp,
                price: oracle_price,
                volume: 0.0,
                spread: self.optimal_spread,
            });
            if ph.len() > MAX_PRICE_HISTORY { ph.pop_front(); }
        }

        self.update_volatility(oracle_price).await?;
        self.process_markouts(timestamp);

        self.calculate_reservation_price(timestamp).await?;
        self.calculate_optimal_quotes(timestamp).await?;

        self.check_risk_limits(timestamp).await?;
        self.last_update = timestamp;
        Ok(())
    }

    /// === REPLACE: update_volatility (Huber clamp, multi-proxy blend, micro-alpha EWMA) ===
    async fn update_volatility(&mut self, price: f64) -> Result<(), Box<dyn std::error::Error>> {
        let mut ve = self.volatility_estimator.write().await;
        let ph = self.price_history.read().await;
        if ph.len() < 2 { return Ok(()); }

        let prev_price = ph[ph.len() - 2].price.max(TICK_SIZE);
        let mut r = (price / prev_price).ln();
        if !r.is_finite() { r = 0.0; }

        // Huber clamp using EWMA scale proxy
        let scale = ve.ewma_r2.sqrt().max(1e-9);
        let k = 8.0; // ~8-sigma equivalent
        let z = (r / scale).abs();
        if z > k { r = r.signum() * k * scale; }

        ve.last_return = r;
        ve.returns.push_back(r);
        ve.squared_returns.push_back(r * r);
        if ve.returns.len() > VOLATILITY_WINDOW {
            ve.returns.pop_front();
            ve.squared_returns.pop_front();
        }

        // Update fast proxy
        ve.ewma_r2 = EWMA_ALPHA * (r * r) + (1.0 - EWMA_ALPHA) * ve.ewma_r2;

        if ve.returns.len() >= 10 {
            let n = ve.returns.len() as f64;
            let mean = ve.returns.iter().sum::<f64>() / n;
            let var  = (ve.returns.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0)).max(0.0);
            let realized = var.sqrt() * (86400.0f64).sqrt();

            // Bipower variation (robust to jumps/spoofs)
            let mut bp_sum = 0.0;
            for i in 1..ve.returns.len() {
                bp_sum += ve.returns[i - 1].abs() * ve.returns[i].abs();
            }
            let bp = (bp_sum.max(0.0) / (ve.returns.len().saturating_sub(1).max(1) as f64)).sqrt() * (86400.0f64).sqrt();

            // EWMA vol
            let ewma = ve.ewma_r2.sqrt() * (86400.0f64).sqrt();

            // GARCH(1,1) update (stationary)
            let sigma_prev = ve.garch_vol.max(1e-9);
            let var_prev = (sigma_prev / (86400.0f64).sqrt()).powi(2);
            let var_now = ve.omega + ve.alpha * (r * r) + ve.beta * var_prev;
            ve.garch_vol = var_now.max(0.0).sqrt() * (86400.0f64).sqrt();

            // Blend — emphasize robust pieces in noisy regimes
            self.sigma = (0.30 * realized + 0.30 * ve.garch_vol + 0.25 * ewma + 0.15 * bp)
                .max(VOLATILITY_FLOOR)
                .min(VOLATILITY_CEILING);
        }

        // Short-window micro-momentum → alpha_signal
        if ve.returns.len() >= MOMENTUM_WINDOW_SHORT {
            let m = MOMENTUM_WINDOW_SHORT;
            let mut s = 0.0;
            for i in (ve.returns.len() - m)..ve.returns.len() { s += ve.returns[i]; }
            let raw = s / (m as f64);
            self.alpha_signal = (1.0 - ALPHA_EWMA) * self.alpha_signal + ALPHA_EWMA * raw;
        }
        Ok(())
    }

    /// === REPLACE: calculate_reservation_price (adds L2-imbalance micro-skew; oracle-safe) ===
    async fn calculate_reservation_price(&mut self, _timestamp: i64) -> Result<(), Box<dyn std::error::Error>> {
        let (gamma_eff, _kappa_eff) = self.effective_params();

        let inv_net = self.net_inventory() - self.target_inventory;

        // Core Avellaneda skew + microstructure noise
        let skew_cap = 0.005 * self.mid_price; // 50 bps
        let skew_core = gamma_eff * self.sigma * self.sigma * inv_net;
        let micro = (MICROSTRUCTURE_NOISE * self.sigma).copysign(inv_net);

        // L2-imbalance micro-skew (no price levels available; use quantity imbalance)
        let mut imb_skew = 0.0;
        if let Some((_k, bs)) = self.venue_books.iter().next() {
            // Convert imbalance [-1,1] → bps shift of mid
            let imb = ASMMEngine::l2_imbalance(bs);
            // cap 6 bps shift from pure imbalance
            let imb_bps = (6.0 * imb).clamp(-6.0, 6.0);
            imb_skew = (imb_bps / 10_000.0) * self.mid_price;
        }

        // Micro alpha & hostile-flow damp
        let alpha_bps = (self.alpha_signal * 10_000.0).clamp(-ALPHA_SKEW_BPS_MAX, ALPHA_SKEW_BPS_MAX);
        let hostile   = self.flow_classifier.clamp(0.0, 1.0);
        let alpha_shift = (alpha_bps / 10_000.0) * self.mid_price * (0.25 * (1.0 - 0.5 * hostile));

        let skew = (skew_core + micro + imb_skew).clamp(-skew_cap, skew_cap);
        self.reservation_price = (self.mid_price - skew + alpha_shift).max(TICK_SIZE);
        Ok(())
    }

    /// === REPLACE: calculate_optimal_quotes (adds depth/slope penalty, adaptive min-step) ===
    async fn calculate_optimal_quotes(&mut self, timestamp: i64) -> Result<(), Box<dyn std::error::Error>> {
        let (gamma_eff, kappa_eff) = self.effective_params();

        // Core half-spread
        let term_risk = 0.5 * gamma_eff * self.sigma * self.sigma;
        let term_flow = (1.0 / gamma_eff) * (1.0 + (gamma_eff / kappa_eff)).ln().max(0.0);
        let mut half_spread_abs = (term_risk + term_flow).max(TICK_SIZE);

        // Depth/slope penalty (thin books ⇒ widen)
        let mut depth_pen_abs = 0.0;
        if let Some((_k, bs)) = self.venue_books.iter().next() {
            let base_size = (self.max_inventory * 0.05).max(MIN_ORDER_SIZE);
            let depth_unit = (bs.best_bid_qty.min(bs.best_ask_qty)) / (base_size + 1e-9); // ~[0..∞)
            let thin = (1.0 - depth_unit.clamp(0.0, 1.0)).clamp(0.0, 1.0);
            depth_pen_abs = thin * 0.0006 * self.mid_price; // up to 6 bps
        }
        half_spread_abs += 0.5 * depth_pen_abs;

        let inv_net_abs = (self.net_inventory() - self.target_inventory).abs();
        let inv_penalty = (gamma_eff * self.sigma * self.sigma * inv_net_abs).max(0.0);

        // Base spread with regime/slot/toxicity/intent/aging
        let mut spread_abs = (2.0 * half_spread_abs + inv_penalty)
            .max(MIN_SPREAD_BPS / 10_000.0 * self.mid_price)
            .min(MAX_SPREAD_BPS / 10_000.0 * self.mid_price);

        spread_abs += (SLOT_RISK_WIDEN_BPS / 10_000.0) * self.mid_price * self.slot_risk.clamp(0.0, 1.0);

        let conf_ratio = (self.confidence_interval / ORACLE_CONFIDENCE_THRESHOLD).clamp(0.0, 1.0);
        spread_abs += conf_ratio * CONF_WIDEN_MULT * (MIN_SPREAD_BPS / 10_000.0) * self.mid_price;

        spread_abs += (TOXICITY_WIDEN_BPS / 10_000.0) * self.mid_price * self.toxicity_score.clamp(0.0, 1.0);

        if self.last_quote_ts > 0 {
            let age = (timestamp - self.last_quote_ts).max(0) as f64;
            let aging_bps = (AGING_WIDEN_BPS_PER_SEC * age).min(AGING_WIDEN_BPS_CAP);
            spread_abs += (aging_bps / 10_000.0) * self.mid_price;
        }

        // Intent hostility widen
        let intent_widen_bps = self.intent_hostility_widen_bps();
        spread_abs += (intent_widen_bps / 10_000.0) * self.mid_price;

        // Inventory skew
        let inventory_skew = INVENTORY_SKEW_FACTOR * gamma_eff * self.sigma * self.sigma
            * (self.net_inventory() - self.target_inventory);

        // Profit guard: after fees
        let spread_with_fees = ((TAKER_FEE_BPS - MAKER_FEE_BPS) / 10_000.0).max(0.0) * self.mid_price;
        let min_profitable_spread = ((MIN_PROFIT_BPS / 10_000.0) * self.mid_price + spread_with_fees) * SPREAD_BUFFER;

        let half = 0.5 * spread_abs;
        let mut bid = self.reservation_price - half - inventory_skew;
        let mut ask = self.reservation_price + half - inventory_skew;

        if ask - bid < min_profitable_spread {
            let pad = 0.5 * (min_profitable_spread - (ask - bid));
            bid -= pad; ask += pad;
        }

        // Tick rounding & oracle band
        let round_tick = |x: f64| ((x / TICK_SIZE).round() * TICK_SIZE).max(TICK_SIZE);
        let mut new_bid = round_tick(bid);
        let mut new_ask = round_tick(ask);

        let lower = (self.oracle_price * (1.0 - ORACLE_BAND_MULT * self.confidence_interval)).max(TICK_SIZE);
        let upper = (self.oracle_price * (1.0 + ORACLE_BAND_MULT * self.confidence_interval)).max(TICK_SIZE);
        if new_bid < lower { new_bid = lower; }
        if new_ask > upper { new_ask = upper; }

        new_bid = ((new_bid / TICK_SIZE).floor() * TICK_SIZE).max(TICK_SIZE);
        new_ask = ((new_ask / TICK_SIZE).ceil()  * TICK_SIZE).max(TICK_SIZE);

        // Adaptive anti-flicker: tougher gate under high slot risk/toxicity
        let base_min_ticks = QUOTE_MIN_CHANGE_TICKS as f64;
        let gate_mult = (1.0 + 1.5 * self.slot_risk.clamp(0.0,1.0) + 0.8 * self.toxicity_score.clamp(0.0,1.0)).clamp(1.0, 3.5);
        let min_step = base_min_ticks * gate_mult * TICK_SIZE;

        if self.last_published_bid > 0.0 && (new_bid - self.last_published_bid).abs() < min_step { new_bid = self.last_published_bid; }
        if self.last_published_ask > 0.0 && (new_ask - self.last_published_ask).abs() < min_step { new_ask = self.last_published_ask; }

        // Cancel-token governor
        let bid_changed = self.last_published_bid <= 0.0 || (new_bid - self.last_published_bid).abs() >= (TICK_SIZE - 1e-12);
        let ask_changed = self.last_published_ask <= 0.0 || (new_ask - self.last_published_ask).abs() >= (TICK_SIZE - 1e-12);
        self.refill_cancel_tokens(timestamp);

        let mut allowed = true;
        if bid_changed || ask_changed {
            allowed = self.consume_cancel_tokens(CANCELS_PER_QUOTE_CHANGE);
            if !allowed {
                if self.last_published_bid > 0.0 { new_bid = self.last_published_bid; }
                if self.last_published_ask > 0.0 { new_ask = self.last_published_ask; }
            }
        }

        if new_bid >= new_ask {
            new_bid = (new_bid - TICK_SIZE).max(TICK_SIZE);
            new_ask = new_ask + TICK_SIZE;
        }

        self.bid_price = new_bid;
        self.ask_price = new_ask;

        if allowed || self.last_published_bid == 0.0 || self.last_published_ask == 0.0 {
            self.last_published_bid = new_bid;
            self.last_published_ask = new_ask;
            self.last_quote_ts = timestamp;
        }

        self.calculate_order_sizes().await?;
        Ok(())
    }

    /// === REPLACE: calculate_order_sizes (adds depth/liquidity & intent hostility damp) ===
    async fn calculate_order_sizes(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let rm = self.risk_manager.read().await;

        let base_size = (self.max_inventory * 0.05).max(MIN_ORDER_SIZE);
        let vol_scale = (1.0 / (1.0 + 10.0 * self.sigma)).clamp(0.15, 1.0);

        // Edge after fees
        let spread = (self.ask_price - self.bid_price).max(TICK_SIZE);
        let fees_abs = ((TAKER_FEE_BPS - MAKER_FEE_BPS) / 10_000.0).max(0.0) * self.mid_price;
        let edge_abs = (spread - fees_abs).max(0.0) * 0.5;

        // Variance proxy
        let var_abs = (self.sigma * self.mid_price).powi(2).max(1e-12);
        let kelly = (edge_abs / var_abs).clamp(0.0, 0.25);

        // Inventory tilt
        let inv_ratio = (self.inventory / self.max_inventory).clamp(-1.0, 1.0);
        let bid_adj = 1.0 + INVENTORY_SKEW_FACTOR * inv_ratio;
        let ask_adj = 1.0 - INVENTORY_SKEW_FACTOR * inv_ratio;

        // Drawdown damp
        let dd_ratio = if rm.daily_loss_limit > 0.0 && rm.current_daily_pnl < 0.0 {
            (-rm.current_daily_pnl / rm.daily_loss_limit).clamp(0.0, 1.5)
        } else { 0.0 };
        let dd_scale = (1.0 - 0.6 * dd_ratio).clamp(0.25, 1.0);

        // Slot & toxicity size cuts + intent hostility (more negative mass ⇒ smaller)
        let slot_cut = 1.0 - (SLOT_RISK_SIZE_CUT * self.slot_risk.clamp(0.0, 1.0));
        let tox_cut  = 1.0 - (0.50 * self.toxicity_score.clamp(0.0, 1.0));
        let intent_cut = 1.0 - (self.intent_hostility_widen_bps() / INTENT_WIDEN_BPS_MAX).clamp(0.0, 1.0) * 0.30;
        let mut safety_scale = (slot_cut * tox_cut * dd_scale * intent_cut).clamp(0.10, 1.0);

        // L2 imbalance boost (favor near-term pressure)
        let mut imb = 0.0;
        if let Some((_k, bs)) = self.venue_books.iter().next() {
            imb = ASMMEngine::l2_imbalance(bs);
        }
        let imb_bid_boost = (1.0 - 0.25 * imb).clamp(0.70, 1.30);
        let imb_ask_boost = (1.0 + 0.25 * imb).clamp(0.70, 1.30);

        // Depth/liquidity damp (thin book ⇒ smaller size)
        if let Some((_k, bs)) = self.venue_books.iter().next() {
            let base_ref = base_size + 1e-9;
            let depth_unit = (bs.best_bid_qty.min(bs.best_ask_qty)) / base_ref; // 0..∞
            let liq_damp = (0.60 + 0.40 * depth_unit.clamp(0.0, 1.0)).clamp(0.60, 1.0);
            safety_scale *= liq_damp;
        }

        let target_bid = (base_size * vol_scale * (0.5 + kelly) * bid_adj * safety_scale * imb_bid_boost).max(MIN_ORDER_SIZE);
        let target_ask = (base_size * vol_scale * (0.5 + kelly) * ask_adj * safety_scale * imb_ask_boost).max(MIN_ORDER_SIZE);

        // Slew limiter based on cancel budget & risk
        let budget_frac = (self.cancel_tokens / CANCEL_BURST_MAX).clamp(0.0, 1.0);
        let step_frac = (0.30 + 0.50 * budget_frac + 0.20 * self.slot_risk.clamp(0.0,1.0)).clamp(0.25, 1.0);

        let smooth = |prev: f64, tgt: f64| -> f64 {
            if prev <= 0.0 { return tgt; }
            let delta = tgt - prev;
            let cap = (prev * step_frac).max(MIN_ORDER_SIZE * 0.5);
            prev + delta.clamp(-cap, cap)
        };

        let size_bid_raw = smooth(self.order_size_bid, target_bid);
        let size_ask_raw = smooth(self.order_size_ask, target_ask);

        self.order_size_bid = size_bid_raw
            .min(rm.max_order_size)
            .min(self.max_inventory - self.inventory)
            .max(0.0);

        self.order_size_ask = size_ask_raw
            .min(rm.max_order_size)
            .min(self.inventory - self.min_inventory)
            .max(0.0);

        if self.inventory >= self.max_inventory * MAX_INVENTORY_RATIO { self.order_size_bid = 0.0; }
        if self.inventory <= self.min_inventory * MAX_INVENTORY_RATIO { self.order_size_ask = 0.0; }

        if self.toxicity_score >= TOXICITY_HALT_THRESHOLD {
            self.order_size_bid = 0.0;
            self.order_size_ask = 0.0;
        }
        Ok(())
    }

    async fn check_risk_limits(&mut self, now_ts: i64) -> Result<(), Box<dyn std::error::Error>> {
        let mut rm = self.risk_manager.write().await;

        rm.position_limits_breached = self.inventory.abs() >= self.max_inventory * MAX_INVENTORY_RATIO;

        if rm.current_daily_pnl <= -rm.daily_loss_limit {
            rm.trading_halted = true;
            self.active = false;
            rm.last_risk_check = now_ts;
            return Err("Daily loss limit reached".into());
        }
        
        if rm.position_limits_breached {
            self.order_size_bid = 0.0;
            self.order_size_ask = 0.0;
        }

        // Absolute base-unit cap
        if self.inventory.abs() > rm.max_position_size {
            self.order_size_bid = 0.0;
            self.order_size_ask = 0.0;
            rm.trading_halted = true;
            self.active = false;
            rm.last_risk_check = now_ts;
            return Err("Absolute position size exceeded".into());
        }

        rm.last_risk_check = now_ts;
        Ok(())
    }

    pub async fn execute_trade(
        &mut self,
        side: OrderSide,
        quantity: f64,
        price: f64,
        timestamp: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if price <= 0.0 || !price.is_finite() { return Err("Invalid trade price".into()); }

        let qty = ((quantity / LOT_SIZE).round() * LOT_SIZE).max(0.0);
        if qty < MIN_ORDER_SIZE { return Err("Order size below minimum".into()); }

        match side {
            OrderSide::Buy => {
                if self.inventory + qty > self.max_inventory { return Err("Buy would exceed max inventory".into()); }
                let fee_mult = 1.0 + MAKER_FEE_BPS / 10_000.0;

                if self.inventory.abs() < f64::EPSILON {
                    self.inventory_vwap = price;
                } else {
                    self.inventory_vwap = ((self.inventory_vwap * self.inventory) + (price * qty)) / (self.inventory + qty);
                }
                self.inventory += qty;

                // Realize maker fee immediately
                self.realized_pnl -= qty * price * (fee_mult - 1.0);
            }
            OrderSide::Sell => {
                if self.inventory - qty < self.min_inventory { return Err("Sell would exceed min inventory".into()); }
                let fee_mult = 1.0 - MAKER_FEE_BPS / 10_000.0;

                let closing_qty = qty.min(self.inventory.abs());
                let pnl_core = (price - self.inventory_vwap) * closing_qty;
                self.realized_pnl += pnl_core;

                self.realized_pnl -= qty * price * (1.0 - fee_mult);

                self.inventory -= qty;
                if self.inventory.abs() < f64::EPSILON { self.inventory_vwap = 0.0; }
            }
        }

        self.total_volume += qty;
        self.trade_count += 1;

        // Enqueue for markout-based toxicity update
        self.markouts.push_back(FillEvent { side, price, qty, ts: timestamp });

        self.update_unrealized_pnl();
        {
            let mut rm = self.risk_manager.write().await;
            rm.current_daily_pnl = self.realized_pnl + self.unrealized_pnl;
        }

        self.update_toxicity(side); // immediate 1-step toxicity
        self.calculate_reservation_price(timestamp).await?;
        self.calculate_optimal_quotes(timestamp).await?;
        Ok(())
    }

    fn update_unrealized_pnl(&mut self) {
        if self.inventory.abs() < f64::EPSILON {
            self.unrealized_pnl = 0.0;
            return;
        }
        self.unrealized_pnl = (self.mid_price - self.inventory_vwap) * self.inventory;
    }

    fn calculate_average_cost(&self) -> f64 {
        if self.inventory.abs() < f64::EPSILON { return self.mid_price; }
        self.inventory_vwap.max(TICK_SIZE)
    }

    fn update_toxicity(&mut self, side: OrderSide) {
        let last_r = if let Ok(ve) = self.volatility_estimator.try_read() {
            ve.last_return
        } else { 0.0 };

        let adverse = match side {
            OrderSide::Buy  => last_r < 0.0,
            OrderSide::Sell => last_r > 0.0,
        };
        let hit = if adverse { 1.0 } else { 0.0 };
        self.toxicity_score = (1.0 - TOXICITY_EWMA_ALPHA) * self.toxicity_score + TOXICITY_EWMA_ALPHA * hit;
    }

    /// Set current slot risk [0..1]. Higher -> wider spreads, smaller sizes.
    pub fn set_slot_risk(&mut self, risk: f64) {
        self.slot_risk = risk.clamp(0.0, 1.0);
    }

    /// Optional: force reset of toxicity (e.g., after regime change)
    pub fn reset_toxicity(&mut self) {
        self.toxicity_score = 0.0;
    }

    /// Optional: seed last published quotes (after warm start)
    pub fn seed_last_quotes(&mut self, bid: f64, ask: f64) {
        self.last_published_bid = bid.max(TICK_SIZE);
        self.last_published_ask = ask.max(TICK_SIZE);
    }

    pub fn get_venue_quotes(&self) -> Vec<VenueQuote> {
        if self.venues.is_empty() || self.bid_price <= 0.0 || self.ask_price <= 0.0 {
            return Vec::new();
        }
        let core_spread = (self.ask_price - self.bid_price).max(TICK_SIZE);
        let core_mid = 0.5 * (self.bid_price + self.ask_price);

        let hostile = self.flow_classifier.clamp(0.0, 1.0);

        let mut out = Vec::with_capacity(self.venues.len());
        for v in self.venues.iter() {
            let liq = v.liquidity_score.clamp(0.0, 1.0);
            let size_scale = (VENUE_SIZE_SCALE_MIN + liq * (VENUE_SIZE_SCALE_MAX - VENUE_SIZE_SCALE_MIN))
                .clamp(VENUE_SIZE_SCALE_MIN, VENUE_SIZE_SCALE_MAX);

            // Learned slippage/toxicity widen (bps)
            let widen_bps = (v.slippage_bps + 20.0 * v.toxicity).max(0.0);
            let widen_abs = (widen_bps / 10_000.0) * core_mid;

            // Fee-aware edge
            let fee_edge = ((TAKER_FEE_BPS - v.maker_fee_bps) / 10_000.0).max(0.0) * core_mid;

            // Per-venue skew (bps): punish hostile venues, reward deep ones
            let skew_bps = ((VENUE_SKEW_BPS_MAX) * (v.toxicity - 0.5 * hostile) - 5.0 * (1.0 - liq)).clamp(-VENUE_SKEW_BPS_MAX, VENUE_SKEW_BPS_MAX);
            let skew_abs = (skew_bps / 10_000.0) * core_mid;

            // Queue-position shading (ticks)
            let (bid_ticks, ask_ticks) = self.shading_ticks_for_venue(v);

            let half = 0.5 * (core_spread + widen_abs + fee_edge);
            let mut bid = (core_mid - half - skew_abs).max(TICK_SIZE) + (bid_ticks as f64) * TICK_SIZE;
            let mut ask = (core_mid + half - skew_abs).max(TICK_SIZE) + (ask_ticks as f64) * TICK_SIZE;

            bid = ((bid / TICK_SIZE).floor() * TICK_SIZE).max(TICK_SIZE);
            ask = ((ask / TICK_SIZE).ceil()  * TICK_SIZE).max(TICK_SIZE);
            if bid >= ask { bid = (bid - TICK_SIZE).max(TICK_SIZE); ask += TICK_SIZE; }

            out.push(VenueQuote {
                venue: v.name.clone(),
                bid_price: bid,
                bid_size: (self.order_size_bid * size_scale).max(0.0),
                ask_price: ask,
                ask_size: (self.order_size_ask * size_scale).max(0.0),
                timestamp: self.last_update,
            });
        }
        out
    }

    pub fn upsert_venue(&mut self, vp: VenueParam) {
        if let Some(ix) = self.venues.iter().position(|x| x.name == vp.name) {
            self.venues[ix] = vp;
        } else {
            self.venues.push(vp);
        }
    }

    /// Provide SOL/USD ref for tip conversion (e.g., from Pyth SOL/USD)
    pub fn set_tip_ref_sol_usd(&mut self, sol_usd: f64) {
        self.tip_ref_sol_usd = sol_usd.max(0.01);
    }

    /// === REPLACE: conservative_ev_per_unit (adds depth/imbalance & sigma→500ms realism) ===
    fn conservative_ev_per_unit(&self) -> f64 {
        // Base edge: half-spread after taker-maker differential
        let spread = (self.ask_price - self.bid_price).max(TICK_SIZE);
        let fees_abs = ((TAKER_FEE_BPS - MAKER_FEE_BPS) / 10_000.0).max(0.0) * self.mid_price;
        let base = (spread - fees_abs).max(0.0) * 0.5;

        // Regime penalties
        let conf_ratio = (self.confidence_interval / ORACLE_CONFIDENCE_THRESHOLD).clamp(0.0, 1.0);
        let tox      = self.toxicity_score.clamp(0.0, 1.0);
        let hostile  = self.flow_classifier.clamp(0.0, 1.0);

        // 500ms sigma proxy (Solana reality)
        let sigma_500ms = (self.sigma / (86400.0f64).sqrt()) * (0.5f64).sqrt();
        let sigma_pen_bps = (sigma_500ms * 10_000.0).min(30.0);

        // Depth/imbalance adjustment: thin/one-sided book lowers effective edge
        let mut depth_pen_bps = 0.0;
        let mut imb_pen_bps   = 0.0;
        if let Some((_k, bs)) = self.venue_books.iter().next() {
            let base_size = (self.max_inventory * 0.05).max(MIN_ORDER_SIZE);
            let depth_unit = (bs.best_bid_qty.min(bs.best_ask_qty)) / (base_size + 1e-9);
            depth_pen_bps = (6.0 * (1.0 - depth_unit.clamp(0.0, 1.0))).clamp(0.0, 6.0);
            let imb = ASMMEngine::l2_imbalance(bs).abs();
            imb_pen_bps = (8.0 * imb).clamp(0.0, 8.0);
        }

        let penalty_abs = self.mid_price * (
            (tox * 12.0 + hostile * 8.0 + conf_ratio * 4.0 + sigma_pen_bps + depth_pen_bps + imb_pen_bps) / 10_000.0
        );

        (base - penalty_abs).max(0.0)
    }

    /// === REPLACE: submission_urgency (adds depth & queue-pressure realism) ===
    fn submission_urgency(&self, venue: &str, side: OrderSide, now_ts: i64) -> f64 {
        let mut s = 0.0;

        // Slot risk & quote aging dominate
        let age = if self.last_quote_ts > 0 { (now_ts - self.last_quote_ts).max(0) as f64 } else { 0.0 };
        s += 0.35 * self.slot_risk.clamp(0.0, 1.0);
        s += 0.25 * (age / 1.5).min(1.0); // >1.5s stale ⇒ high urgency

        // Toxic flow
        s += 0.20 * self.toxicity_score.clamp(0.0, 1.0);

        // Queue pressure + depth: back of queue or thin book ⇒ higher urgency
        if let Some(bs) = self.venue_books.get(venue) {
            let qf = match side {
                OrderSide::Buy  => (1.0 - bs.qpos_bid_ahead_frac).clamp(0.0, 1.0),
                OrderSide::Sell => (1.0 - bs.qpos_ask_ahead_frac).clamp(0.0, 1.0),
            };
            s += 0.15 * qf;

            let base_size = (self.max_inventory * 0.05).max(MIN_ORDER_SIZE);
            let depth_unit = (bs.best_bid_qty.min(bs.best_ask_qty)) / (base_size + 1e-9);
            let thin = (1.0 - depth_unit.clamp(0.0, 1.0)).clamp(0.0, 1.0);
            s += 0.15 * thin;
        } else {
            s += 0.10;
        }
        s.clamp(0.0, 1.0)
    }

    /// === REPLACE: plan_jito_bundle_for_venue (adds 1-tick improvement test vs tip cost) ===
    pub fn plan_jito_bundle_for_venue(
        &mut self,
        venue: &str,
        side: OrderSide,
        qty: f64,
        now_ts: i64,
    ) -> JitoTipPlan {
        let ev_per_unit = self.conservative_ev_per_unit();
        let ev_quote = (ev_per_unit * qty).max(0.0);
        let ev_usd = ev_quote;

        let sol_usd = if self.symbol.starts_with("SOL/") { self.mid_price.max(0.01) } else { self.tip_ref_sol_usd.max(0.01) };
        let urgency = self.submission_urgency(venue, side, now_ts);

        // Venue hostility factor
        let mut venue_factor = 1.0;
        if let Some(vp) = self.venues.iter().find(|x| x.name == venue) {
            venue_factor *= (1.0 + 0.20 * vp.toxicity.clamp(0.0,1.0));
            venue_factor *= (1.0 + 0.15 * (1.0 - vp.liquidity_score.clamp(0.0,1.0)));
        }

        // Tip as fraction of EV in SOL, convex in urgency & slot risk
        let base_tip_sol = (JITO_TIP_FRACTION_OF_EV * ev_usd / sol_usd).max(0.0);
        let tip_sol = base_tip_sol
            * urgency.powf(JITO_TIP_POWER)
            * (1.0 + 0.5 * self.slot_risk.clamp(0.0,1.0))
            * venue_factor;

        let mut tip_lamports = (tip_sol * (LAMPORTS_PER_SOL as f64)).round() as u64;
        if tip_lamports < JITO_TIP_MIN_LAMPORTS { tip_lamports = JITO_TIP_MIN_LAMPORTS; }
        if tip_lamports > JITO_TIP_MAX_LAMPORTS { tip_lamports = JITO_TIP_MAX_LAMPORTS; }

        // One-tick improvement test: if stepping improves EV enough, prefer bundle
        let tick_value_quote = TICK_SIZE * qty;
        // EV gain from one tick better fill, conservative: penalize half the fees differential
        let one_tick_gain_usd = (tick_value_quote - 0.5 * ((TAKER_FEE_BPS - MAKER_FEE_BPS).max(0.0) / 10_000.0) * self.mid_price * qty).max(0.0);

        let expected_tip_usd = (tip_lamports as f64 / LAMPORTS_PER_SOL as f64) * sol_usd;
        let use_bundle = (urgency > 0.35 || self.slot_risk > 0.4)
            && ev_usd + one_tick_gain_usd > expected_tip_usd;

        JitoTipPlan { use_bundle, tip_lamports }
    }

    fn refill_cancel_tokens(&mut self, now_ts: i64) {
        if self.last_cancel_refill_ts == 0 {
            self.last_cancel_refill_ts = now_ts;
            return;
        }
        let dt = (now_ts - self.last_cancel_refill_ts).max(0) as f64;
        if dt <= 0.0 { return; }

        // More budget in high slot risk / toxicity; less when flow is friendly.
        let hostile = self.flow_classifier.clamp(0.0, 1.0);
        let rate_scale = (1.0 + 0.5 * self.slot_risk.clamp(0.0,1.0) + 0.25 * self.toxicity_score.clamp(0.0,1.0) - 0.25 * (1.0 - hostile)).clamp(0.5, 2.0);
        let refill_rate = CANCEL_RATE_MAX_PER_SEC * rate_scale;

        // Slightly larger bucket when slot risk is elevated.
        let cap = (CANCEL_BURST_MAX * (1.0 + 0.25 * self.slot_risk.clamp(0.0,1.0))).clamp(CANCEL_BURST_MAX, CANCEL_BURST_MAX * 1.25);

        self.cancel_tokens = (self.cancel_tokens + refill_rate * dt).min(cap);
        self.last_cancel_refill_ts = now_ts;
    }

    fn consume_cancel_tokens(&mut self, tokens: f64) -> bool {
        if self.cancel_tokens + 1e-9 >= tokens {
            self.cancel_tokens -= tokens;
            true
        } else {
            false
        }
    }

    fn quote_change_cost_ticks(old_bid: f64, old_ask: f64, new_bid: f64, new_ask: f64) -> i64 {
        let db = ((new_bid - old_bid).abs() / TICK_SIZE).round() as i64;
        let da = ((new_ask - old_ask).abs() / TICK_SIZE).round() as i64;
        db + da
    }

    /// === REPLACE: update_orderbook_snapshot (spoof-resilient smoothing & jump damp) ===
    pub fn update_orderbook_snapshot(
        &mut self,
        venue: &str,
        best_bid_qty: f64,
        best_ask_qty: f64,
        our_bid_qty: f64,
        our_ask_qty: f64,
        queue_ahead_bid_frac: f64,
        queue_ahead_ask_frac: f64,
        ts: i64,
    ) {
        let e = self.venue_books.entry(venue.to_string()).or_insert(BookState {
            best_bid_qty: 0.0,
            best_ask_qty: 0.0,
            our_bid_qty: 0.0,
            our_ask_qty: 0.0,
            qpos_bid_ahead_frac: 0.5,
            qpos_ask_ahead_frac: 0.5,
            last_update: ts,
        });

        // Base α from elapsed time (τ≈400ms)
        let dt = (ts - e.last_update).max(0) as f64;
        let tau = 0.4;
        let mut a = (1.0 - (-dt / tau).exp()).clamp(0.05, 0.6);

        // Spoof/jump damp: if incoming size is a >x jump vs EWMA, reduce α for that side this tick
        let jump_thresh = 5.0;
        let bb = best_bid_qty.max(0.0);
        let ba = best_ask_qty.max(0.0);

        let damp_b = if e.best_bid_qty > 0.0 {
            let r = (bb / e.best_bid_qty.max(1e-9)).max(e.best_bid_qty.max(1e-9) / bb.max(1e-9));
            if r > jump_thresh { 0.25 } else { 1.0 }
        } else { 1.0 };
        let damp_a = if e.best_ask_qty > 0.0 {
            let r = (ba / e.best_ask_qty.max(1e-9)).max(e.best_ask_qty.max(1e-9) / ba.max(1e-9));
            if r > jump_thresh { 0.25 } else { 1.0 }
        } else { 1.0 };

        // Apply smoothing with per-side damp
        e.best_bid_qty = (1.0 - a * damp_b) * e.best_bid_qty + (a * damp_b) * bb;
        e.best_ask_qty = (1.0 - a * damp_a) * e.best_ask_qty + (a * damp_a) * ba;
        e.our_bid_qty  = (1.0 - a) * e.our_bid_qty  + a * our_bid_qty.max(0.0);
        e.our_ask_qty  = (1.0 - a) * e.our_ask_qty  + a * our_ask_qty.max(0.0);

        // Clamp & smooth queue fractions
        let qbf = queue_ahead_bid_frac.clamp(0.0, 1.0);
        let qaf = queue_ahead_ask_frac.clamp(0.0, 1.0);
        e.qpos_bid_ahead_frac = (1.0 - a) * e.qpos_bid_ahead_frac + a * qbf;
        e.qpos_ask_ahead_frac = (1.0 - a) * e.qpos_ask_ahead_frac + a * qaf;

        e.last_update = ts;
    }

    fn l2_imbalance(bs: &BookState) -> f64 {
        let b = bs.best_bid_qty.max(0.0);
        let a = bs.best_ask_qty.max(0.0);
        let s = (b + a).max(1e-9);
        (b - a) / s // [-1,1]
    }

    /// === REPLACE: shading_ticks_for_venue (queue + L2 imbalance + micro alpha + staleness damp) ===
    fn shading_ticks_for_venue(&self, venue: &VenueParam) -> (i64, i64) {
        if let Some(bs) = self.venue_books.get(&venue.name) {
            let mut bid_ticks = 0i64;
            let mut ask_ticks = 0i64;

            // Queue position pressure
            if bs.qpos_bid_ahead_frac < QUEUE_POS_FRONT_THRESH { bid_ticks += 1; }
            else if bs.qpos_bid_ahead_frac > QUEUE_POS_BACK_THRESH { bid_ticks -= 1; }
            if bs.qpos_ask_ahead_frac < QUEUE_POS_FRONT_THRESH { ask_ticks -= 1; }
            else if bs.qpos_ask_ahead_frac > QUEUE_POS_BACK_THRESH { ask_ticks += 1; }

            // L2 imbalance tilt
            let imb = ASMMEngine::l2_imbalance(bs); // [-1,1]
            let imb_ticks = (imb.abs() * (QUEUE_IMB_SKEW_TICKS_MAX as f64)).floor() as i64;
            if imb > 0.0 {
                bid_ticks -= imb_ticks;
                ask_ticks += imb_ticks;
            } else if imb < 0.0 {
                bid_ticks += imb_ticks;
                ask_ticks -= imb_ticks;
            }

            // Micro-alpha tilt
            let alpha_tick = if self.alpha_signal > 0.0 { 1 } else if self.alpha_signal < 0.0 { -1 } else { 0 };
            bid_ticks += alpha_tick;
            ask_ticks -= alpha_tick;

            // Staleness damp
            let age = (self.last_update.saturating_sub(bs.last_update)) as f64;
            if age > 0.0 {
                let damp = (1.0 - (age / 2.0).min(1.0)).clamp(0.0, 1.0);
                bid_ticks = ((bid_ticks as f64) * damp).round() as i64;
                ask_ticks = ((ask_ticks as f64) * damp).round() as i64;
            }

            bid_ticks = bid_ticks.clamp(-QUEUE_POS_SKEW_TICKS_MAX, QUEUE_POS_SKEW_TICKS_MAX);
            ask_ticks = ask_ticks.clamp(-QUEUE_POS_SKEW_TICKS_MAX, QUEUE_POS_SKEW_TICKS_MAX);
            (bid_ticks, ask_ticks)
        } else {
            (0, 0)
        }
    }

    pub async fn get_quote(&self) -> Result<Quote, Box<dyn std::error::Error>> {
        if !self.active { return Err("Engine inactive".into()); }
        let rm = self.risk_manager.read().await;
        if rm.trading_halted { return Err("Trading halted due to risk limits".into()); }

        if self.bid_price <= 0.0 || self.ask_price <= 0.0 || self.bid_price >= self.ask_price {
            return Err("Invalid quote state".into());
        }
        
        Ok(Quote {
            bid_price: self.bid_price,
            bid_size: self.order_size_bid,
            ask_price: self.ask_price,
            ask_size: self.order_size_ask,
            timestamp: self.last_update,
            spread: (self.ask_price - self.bid_price).max(TICK_SIZE),
            mid_price: self.mid_price,
        })
    }

    pub async fn update_parameters(
        &mut self,
        gamma: Option<f64>,
        kappa: Option<f64>,
        eta: Option<f64>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(g) = gamma {
            if (0.0..=1.0).contains(&g) {
                self.gamma = g.max(1e-6);
                self.gamma_base = self.gamma;
            } else { return Err("Invalid gamma".into()); }
        }
        if let Some(k) = kappa {
            if (0.0..=10.0).contains(&k) {
                self.kappa = k.max(1e-6);
                self.kappa_base = self.kappa;
            } else { return Err("Invalid kappa".into()); }
        }
        if let Some(e) = eta {
            if (0.0..=1.0).contains(&e) { self.eta = e.max(1e-6); }
            else { return Err("Invalid eta".into()); }
        }

        let ts = if self.last_update > 0 { self.last_update } else { 0 };
        self.calculate_optimal_quotes(ts).await?;
        Ok(())
    }

    pub async fn reset_daily_metrics(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut risk_manager = self.risk_manager.write().await;
        risk_manager.current_daily_pnl = 0.0;
        risk_manager.trading_halted = false;
        
        self.realized_pnl = 0.0;
        self.trade_count = 0;
        self.total_volume = 0.0;
        
        Ok(())
    }

    pub fn get_inventory_ratio(&self) -> f64 {
        self.inventory / self.max_inventory
    }

    pub fn get_spread_bps(&self) -> f64 {
        if self.mid_price > 0.0 {
            (self.ask_price - self.bid_price) / self.mid_price * 10000.0
        } else {
            0.0
        }
    }

    pub async fn emergency_close_position(&mut self, market_price: f64) -> Result<f64, Box<dyn std::error::Error>> {
        if self.inventory.abs() < f64::EPSILON {
            return Ok(0.0);
        }
        
        let fee_multiplier = if self.inventory > 0.0 {
            1.0 - TAKER_FEE_BPS / 10000.0
        } else {
            1.0 + TAKER_FEE_BPS / 10000.0
        };
        
        let close_pnl = self.inventory.abs() * market_price * fee_multiplier;
        
        if self.inventory > 0.0 {
            self.realized_pnl += close_pnl;
        } else {
            self.realized_pnl -= close_pnl;
        }
        
        self.inventory = 0.0;
        self.unrealized_pnl = 0.0;
        self.active = false;
        
        Ok(close_pnl)
    }

    pub fn calculate_sharpe_ratio(&self) -> f64 {
        let ve = futures::executor::block_on(self.volatility_estimator.read());
        if ve.returns.len() < 2 { return 0.0; }

        let n = ve.returns.len() as f64;
        let mean = ve.returns.iter().sum::<f64>() / n;
        let var = ve.returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n.max(1.0);
        if var <= 0.0 { return 0.0; }
        // daily sharpe proxy (assuming returns are ~ per second)
        let daily_scale = (86400.0f64).sqrt();
        mean / var.sqrt() * daily_scale
    }

    pub fn get_risk_metrics(&self) -> RiskMetrics {
        RiskMetrics {
            inventory_ratio: self.get_inventory_ratio(),
            spread_bps: self.get_spread_bps(),
            volatility: self.sigma,
            sharpe_ratio: self.calculate_sharpe_ratio(),
            total_pnl: self.realized_pnl + self.unrealized_pnl,
            realized_pnl: self.realized_pnl,
            unrealized_pnl: self.unrealized_pnl,
            trade_count: self.trade_count,
            total_volume: self.total_volume,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum OrderSide {
    Buy,
    Sell,
}

#[derive(Clone, Debug)]
pub struct Quote {
    pub bid_price: f64,
    pub bid_size: f64,
    pub ask_price: f64,
    pub ask_size: f64,
    pub timestamp: i64,
    pub spread: f64,
    pub mid_price: f64,
}

#[derive(Clone, Debug)]
pub struct RiskMetrics {
    pub inventory_ratio: f64,
    pub spread_bps: f64,
    pub volatility: f64,
    pub sharpe_ratio: f64,
    pub total_pnl: f64,
    pub realized_pnl: f64,
    pub unrealized_pnl: f64,
    pub trade_count: u64,
    pub total_volume: f64,
}

impl VolatilityEstimator {
    pub fn update_garch_params(&mut self, returns: &[f64]) {
        if returns.len() < 20 { return; }

        // Target unconditional variance
        let n = returns.len() as f64;
        let mean = returns.iter().sum::<f64>() / n;
        let var = returns.iter().map(|r| (r - mean).powi(2)).sum::<f64>() / n.max(1.0);

        // Conservative re-fit: keep alpha small, beta high, persistence < 0.995
        self.alpha = 0.05;
        self.beta  = 0.94;
        let pers = self.alpha + self.beta;
        if pers >= 0.995 {
            self.beta = (0.995 - self.alpha).max(0.90);
        }

        // Back out omega for stationary variance: var = omega / (1 - alpha - beta)
        let denom = (1.0 - self.alpha - self.beta).max(1e-6);
        self.omega = (var * denom * 0.5).max(1e-12);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_engine_initialization() {
        let engine = ASMMEngine::new(
            "SOL/USDC".to_string(),
            9,
            6,
            1000.0,
        );
        
        assert_eq!(engine.inventory, 0.0);
        assert_eq!(engine.gamma, GAMMA_DEFAULT);
        assert!(engine.active);
    }

    #[tokio::test]
    async fn test_quote_generation() {
        let mut engine = ASMMEngine::new(
            "SOL/USDC".to_string(),
            9,
            6,
            1000.0,
        );
        
        engine.update_market_data(100.0, 0.01, 1000).await.unwrap();
        
        let quote = engine.get_quote().await.unwrap();
        assert!(quote.bid_price < quote.ask_price);
        assert!(quote.bid_size > 0.0);
        assert!(quote.ask_size > 0.0);
    }
}

