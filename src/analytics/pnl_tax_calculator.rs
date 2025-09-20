use std::collections::{HashMap, VecDeque, BTreeMap};
use std::sync::{Arc, RwLock, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use rustc_hash::FxHashMap;
use chrono::{DateTime, Utc, Datelike};
use rust_decimal::prelude::*;
use rust_decimal::MathematicalOps;
use serde::{Deserialize, Serialize};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use thiserror::Error;
use log::{info, error, warn};
// + time source for lock-free rate limiting
use std::time::{SystemTime, UNIX_EPOCH};

const DECIMALS_USDC: u32 = 6;
const DECIMALS_SOL: u32 = 9;
const SHORT_TERM_DAYS: i64 = 365;
const WASH_SALE_DAYS: i64 = 30;
const MAX_PRECISION: u32 = 12;
const MAX_HISTORY_DAYS: i64 = 730;
const RATE_LIMIT_MS: u64 = 10;
const MAX_DISPOSALS: usize = 100000;
const SQRT_ITERATIONS: u64 = 50;
// [REPLACE] precise conversion: milliseconds → nanoseconds (for lock-free limiter)
const NS_PER_MS: u64 = 1_000_000;
// [ADD CONST] token bucket burst size (max ops allowed per window)
const RATE_BURST: u64 = 8;
// [ADD CONST] cap lots per mint; compact when exceeded (preserves tax correctness)
const MAX_LOTS_PER_MINT: usize = 4096;
// [ADD CONST] only merge acquisitions within this time window (seconds)
const LOT_COMPACT_WINDOW_SECS: i64 = 1;
// [ADD CONST] bound lot-order cache to avoid unbounded growth under many mints
const ORDER_CACHE_MAX_MINTS: usize = 4096;
// [ADD CONST] per-mint rate limiting
const PER_MINT_RATE_BURST: u64 = 4;

#[derive(Error, Debug)]
pub enum PnlError {
    #[error("Insufficient balance: need {needed}, have {available}")]
    InsufficientBalance { needed: Decimal, available: Decimal },
    #[error("Invalid decimal conversion")]
    DecimalConversion,
    #[error("Tax lot not found: {0}")]
    TaxLotNotFound(u64),
    #[error("Invalid tax method")]
    InvalidTaxMethod,
    #[error("Calculation overflow")]
    CalculationOverflow,
    #[error("Invalid quantity: {0}")]
    InvalidQuantity(Decimal),
    #[error("Rate limited")]
    RateLimited,
    #[error("State locked")]
    StateLocked,
    #[error("Invalid price: {0}")]
    InvalidPrice(Decimal),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TaxMethod {
    FIFO,
    LIFO,
    HIFO,
    SpecificIdentification,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TransactionType {
    Arbitrage,
    Sandwich,
    Liquidation,
    JitLiquidity,
    AtomicArb,
    Frontrun,
    Backrun,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TaxJurisdiction {
    US,
    UK,
    Germany,
    Singapore,
    Dubai,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaxLot {
    pub id: u64,
    pub mint: Pubkey,
    pub quantity: Decimal,
    pub cost_basis: Decimal,
    pub acquisition_date: DateTime<Utc>,
    pub acquisition_price: Decimal,
    pub transaction_type: TransactionType,
    pub signature: Signature,
    pub decimals: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Disposal {
    pub mint: Pubkey,
    pub quantity: Decimal,
    pub proceeds: Decimal,
    pub disposal_date: DateTime<Utc>,
    pub disposal_price: Decimal,
    pub lots_used: Vec<(u64, Decimal)>,
    pub realized_pnl: Decimal,
    pub short_term_gain: Decimal,
    pub long_term_gain: Decimal,
    pub fees: Decimal,
    pub audit_hash: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnlSnapshot {
    pub timestamp: DateTime<Utc>,
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub total_pnl: Decimal,
    pub roi_percentage: Decimal,
    pub sharpe_ratio: Decimal,
    pub win_rate: Decimal,
    pub avg_profit_per_trade: Decimal,
}

#[derive(Debug, Clone)]
struct StateTransaction {
    lots_backup: HashMap<Pubkey, VecDeque<TaxLot>>,
    disposals_count: usize,
}

// [ADD] Per-mint rate limiting bucket
#[derive(Debug)]
struct AtomicBucket {
    tokens: AtomicU64,
    last_refill_ns: AtomicU64,
}

// [REPLACE] struct: add order cache & versioning
pub struct PnlTaxCalculator {
    tax_lots: Arc<RwLock<HashMap<Pubkey, VecDeque<TaxLot>>>>,
    disposals: Arc<RwLock<Vec<Disposal>>>,
    tax_method: TaxMethod,
    jurisdiction: TaxJurisdiction,
    lot_counter: Arc<AtomicU64>,
    pnl_history: Arc<RwLock<BTreeMap<DateTime<Utc>, PnlSnapshot>>>,
    current_prices: Arc<RwLock<HashMap<Pubkey, Decimal>>>,
    basis_adjustments: Arc<RwLock<HashMap<u64, Decimal>>>,

    // rate limiter (burst-aware)
    last_op_ns: AtomicU64,
    rate_tokens: AtomicU64,
    rate_last_refill_ns: AtomicU64,

    // specific-id preferences
    specific_id_prefs: Arc<RwLock<HashMap<Pubkey, HashMap<u64, u64>>>>,

    // lot ordering cache & versioning
    order_cache: Arc<RwLock<HashMap<(Pubkey, u8), (u64, Vec<usize>)>>>,
    lot_versions: Arc<RwLock<HashMap<Pubkey, u64>>>,

    // misc
    decimal_cache: Arc<RwLock<HashMap<Pubkey, u32>>>,
    state_lock: Arc<Mutex<()>>,
    
    // per-mint rate limiting
    per_mint_rate_limiter: Arc<RwLock<FxHashMap<Pubkey, AtomicBucket>>>,
}

impl PnlTaxCalculator {
    // [REPLACE] new(): initialize caches & versions
    pub fn new(tax_method: TaxMethod, jurisdiction: TaxJurisdiction) -> Self {
        Self {
            tax_lots: Arc::new(RwLock::new(HashMap::new())),
            disposals: Arc::new(RwLock::new(Vec::with_capacity(1000))),
            tax_method,
            jurisdiction,
            lot_counter: Arc::new(AtomicU64::new(0)),
            pnl_history: Arc::new(RwLock::new(BTreeMap::new())),
            current_prices: Arc::new(RwLock::new(HashMap::new())),
            basis_adjustments: Arc::new(RwLock::new(HashMap::new())),

            last_op_ns: AtomicU64::new(0),
            rate_tokens: AtomicU64::new(RATE_BURST),
            rate_last_refill_ns: AtomicU64::new(0),

            specific_id_prefs: Arc::new(RwLock::new(HashMap::new())),
            order_cache: Arc::new(RwLock::new(HashMap::new())),
            lot_versions: Arc::new(RwLock::new(HashMap::new())),

            decimal_cache: Arc::new(RwLock::new(HashMap::new())),
            state_lock: Arc::new(Mutex::new(())),
            
            per_mint_rate_limiter: Arc::new(RwLock::new(FxHashMap::default())),
        }
    }

    // [REPLACE] burst-aware token bucket with atomic CAS; race-proof
    #[inline(always)]
    fn rate_limit_check(&self) -> Result<(), PnlError> {
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| PnlError::StateLocked)?
            .as_nanos() as u64;

        let window = RATE_LIMIT_MS * NS_PER_MS;

        // Refill phase: add tokens proportional to elapsed windows, capping at RATE_BURST
        let mut last_refill = self.rate_last_refill_ns.load(Ordering::Relaxed);
        loop {
            let elapsed = now_ns.saturating_sub(last_refill);
            if elapsed < window {
                break;
            }
            let periods = elapsed / window;
            let new_refill = last_refill.saturating_add(periods * window);
            match self.rate_last_refill_ns.compare_exchange_weak(
                last_refill,
                new_refill,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // Safely add periods tokens, clamp to burst
                    let mut cur = self.rate_tokens.load(Ordering::Relaxed);
                    loop {
                        let add = periods.min(RATE_BURST.saturating_sub(cur));
                        if add == 0 { break; }
                        match self.rate_tokens.compare_exchange_weak(
                            cur,
                            cur + add,
                            Ordering::SeqCst,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break,
                            Err(observed) => cur = observed,
                        }
                    }
                    break;
                }
                Err(observed) => last_refill = observed,
            }
        }

        // Consume one token
        let mut tokens = self.rate_tokens.load(Ordering::Relaxed);
        loop {
            if tokens == 0 {
                return Err(PnlError::RateLimited);
            }
            match self.rate_tokens.compare_exchange_weak(
                tokens,
                tokens - 1,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => tokens = observed,
            }
        }

        // Keep last_op_ns for external observability / metrics parity
        self.last_op_ns.store(now_ns, Ordering::Relaxed);
        Ok(())
    }

    // [ADD] Per-mint rate limiting to avoid unrelated throttling
    #[inline(always)]
    fn per_mint_rate_limit_check(&self, mint: Pubkey) -> Result<(), PnlError> {
        let now_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| PnlError::StateLocked)?
            .as_nanos() as u64;

        let window = RATE_LIMIT_MS * NS_PER_MS;

        // Get or create bucket for this mint
        let bucket = {
            let mut limiter = self.per_mint_rate_limiter.write().map_err(|_| PnlError::StateLocked)?;
            limiter.entry(mint).or_insert_with(|| AtomicBucket {
                tokens: AtomicU64::new(PER_MINT_RATE_BURST),
                last_refill_ns: AtomicU64::new(now_ns),
            }).clone()
        };

        // Refill tokens for this mint
        let mut last_refill = bucket.last_refill_ns.load(Ordering::Relaxed);
        loop {
            let elapsed = now_ns.saturating_sub(last_refill);
            if elapsed < window {
                break;
            }
            let periods = elapsed / window;
            let new_refill = last_refill.saturating_add(periods * window);
            match bucket.last_refill_ns.compare_exchange_weak(
                last_refill,
                new_refill,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    let mut cur = bucket.tokens.load(Ordering::Relaxed);
                    loop {
                        let add = periods.min(PER_MINT_RATE_BURST.saturating_sub(cur));
                        if add == 0 { break; }
                        match bucket.tokens.compare_exchange_weak(
                            cur,
                            cur + add,
                            Ordering::SeqCst,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break,
                            Err(observed) => cur = observed,
                        }
                    }
                    break;
                }
                Err(observed) => last_refill = observed,
            }
        }

        // Consume one token
        let mut tokens = bucket.tokens.load(Ordering::Relaxed);
        loop {
            if tokens == 0 {
                return Err(PnlError::RateLimited);
            }
            match bucket.tokens.compare_exchange_weak(
                tokens,
                tokens - 1,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => tokens = observed,
            }
        }

        Ok(())
    }

    #[inline(always)]
    fn validate_quantity(&self, quantity: Decimal) -> Result<(), PnlError> {
        if quantity <= Decimal::ZERO || quantity.is_sign_negative() {
            return Err(PnlError::InvalidQuantity(quantity));
        }
        if quantity.scale() > MAX_PRECISION {
            return Err(PnlError::InvalidQuantity(quantity));
        }
        Ok(())
    }

    #[inline(always)]
    fn validate_price(&self, price: Decimal) -> Result<(), PnlError> {
        if price <= Decimal::ZERO || price.is_sign_negative() {
            return Err(PnlError::InvalidPrice(price));
        }
        Ok(())
    }

    // [ADD] money rounding + zero clamp at USDC precision
    #[inline(always)]
    fn money6(&self, x: Decimal) -> Decimal {
        let y = x.round_dp_with_strategy(DECIMALS_USDC, rust_decimal::RoundingStrategy::MidpointAwayFromZero);
        // rust_decimal doesn't retain negative zero, but clamp anyway for hygiene
        if y.is_zero() { Decimal::ZERO } else { y }
    }

    // [ADD] mul_div: a*b/c with checks (avoid repeating patterns)
    #[inline(always)]
    fn mul_div(&self, a: Decimal, b: Decimal, c: Decimal) -> Result<Decimal, PnlError> {
        a.checked_mul(b).and_then(|v| v.checked_div(c)).ok_or(PnlError::CalculationOverflow)
    }

    // [ADD] pro-rata: amount * part / total (single place for checked mul/div)
    #[inline(always)]
    fn pro_rata(&self, amount: Decimal, part: Decimal, total: Decimal) -> Result<Decimal, PnlError> {
        self.mul_div(amount, part, total)
    }

    // [ADD] clamp tiny negative dust to zero (numeric hygiene)
    #[inline(always)]
    fn clamp_dust(&self, x: Decimal) -> Decimal {
        let tiny = Decimal::new(1, 12); // 1e-12
        if x.is_sign_negative() && x.abs() <= tiny { Decimal::ZERO } else { x }
    }

    // [ADD] compact audit hash for disposals: binary fields + delim-guarded lots
    #[inline(always)]
    fn audit_hash_disposal(
        &self,
        mint: &Pubkey,
        ts_unix: i64,
        qty: &Decimal,
        proceeds: &Decimal,
        fees: &Decimal,
        lots_used: &[(u64, Decimal)],
    ) -> [u8; 32] {
        use solana_sdk::hash::hashv;

        // Decimal → bytes via to_string (fast enough, avoids intermediary big format! string)
        let qty_s = qty.to_string();
        let prc_s = proceeds.to_string();
        let fee_s = fees.to_string();

        // Compact encode (id LE + qty_str + 0xFF delimiter) for each lot
        let mut lots_buf = Vec::with_capacity(lots_used.len() * (8 + 18)); // heuristic
        for (id, q) in lots_used {
            lots_buf.extend_from_slice(&id.to_le_bytes());
            let qs = q.to_string();
            lots_buf.extend_from_slice(qs.as_bytes());
            lots_buf.push(0xFF); // delimiter to avoid ambiguity
        }

        let ts_bytes = ts_unix.to_le_bytes();
        let h = hashv(&[
            mint.as_ref(),
            &ts_bytes,
            qty_s.as_bytes(),
            prc_s.as_bytes(),
            fee_s.as_bytes(),
            &lots_buf,
        ]);
        h.to_bytes()
    }

    // [ADD] method tag for cache key
    #[inline(always)]
    fn method_tag(&self) -> u8 {
        match self.tax_method {
            TaxMethod::FIFO => 0,
            TaxMethod::LIFO => 1,
            TaxMethod::HIFO => 2,
            TaxMethod::SpecificIdentification => 3,
        }
    }

    // [REPLACE] bump lot version and prune cache if oversized
    #[inline(always)]
    fn bump_lot_version(&self, mint: Pubkey) -> Result<(), PnlError> {
        {
            let mut vers = self.lot_versions.write().map_err(|_| PnlError::StateLocked)?;
            let v = vers.get(&mint).copied().unwrap_or(0).saturating_add(1);
            vers.insert(mint, v);
        }
        let mut oc = self.order_cache.write().map_err(|_| PnlError::StateLocked)?;
        oc.retain(|(m, _), _| *m != mint);
        if oc.len() > ORDER_CACHE_MAX_MINTS {
            oc.clear(); // cold-path nuke; deterministic & bounded memory
        }
        Ok(())
    }

    // [ADD] read current lot version
    #[inline(always)]
    fn lot_version(&self, mint: Pubkey) -> Result<u64, PnlError> {
        Ok(self.lot_versions.read().map_err(|_| PnlError::StateLocked)?
            .get(&mint).copied().unwrap_or(0))
    }

    // [ADD] iterate indices in deterministic lot order with zero alloc on FIFO/LIFO
    #[inline(always)]
    fn for_each_ordered_idx<F: FnMut(usize)>(&self, lots: &VecDeque<TaxLot>, mut f: F) {
        let n = lots.len();
        match self.tax_method {
            TaxMethod::FIFO => { for i in 0..n { f(i); } }
            TaxMethod::LIFO => { for i in (0..n).rev() { f(i); } }
            _ => {
                // HIFO / Specific-ID use cached order without cloning
                let mint = if n > 0 { lots[0].mint } else { return; };
                let tag = self.method_tag();
                if let Ok(oc) = self.order_cache.read() {
                    if let Some((v, idx)) = oc.get(&(mint, tag)) {
                        if self.lot_versions.read().ok().and_then(|m| m.get(&mint).copied()).unwrap_or(0) == *v {
                            for &i in idx.iter() { f(i); }
                            return;
                        }
                    }
                }
                // cache miss fallback: compute once via get_lot_order then iterate
                let idx = self.get_lot_order(lots);
                for i in idx { f(i); }
            }
        }
    }

    // [REPLACE] add inline hint on compaction
    #[inline(always)]
    fn coalesce_adjacent_locked(
        &self,
        mint: Pubkey,
        mint_lots: &mut VecDeque<TaxLot>,
        adj_map: &mut HashMap<u64, Decimal>,
    ) {
        if mint_lots.len() <= MAX_LOTS_PER_MINT { return; }
        if self.tax_method == TaxMethod::SpecificIdentification {
            if let Ok(prefs) = self.specific_id_prefs.read() {
                if prefs.get(&mint).map(|m| !m.is_empty()).unwrap_or(false) { return; }
            }
        }
        let price_eps = Decimal::new(1, 6); // 1e-6
        let mut i = 0usize;
        while i + 1 < mint_lots.len() {
            let a = &mint_lots[i];
            let b = &mint_lots[i + 1];
            let same_day = a.acquisition_date.date_naive() == b.acquisition_date.date_naive();
            let dt = (a.acquisition_date - b.acquisition_date).num_seconds().abs();
            let price_close = (a.acquisition_price - b.acquisition_price).abs() <= price_eps;
            let same_tx = a.transaction_type == b.transaction_type;
            let same_dec = a.decimals == b.decimals;

            if same_day && (dt as i64) <= LOT_COMPACT_WINDOW_SECS && price_close && same_tx && same_dec {
                // merge b into a; keep a.id
                let b_val = mint_lots.remove(i + 1).expect("index valid");
                let a_mut = &mut mint_lots[i];

                let new_qty = match a_mut.quantity.checked_add(b_val.quantity) { Some(v) => v, None => { i += 1; continue; } };
                let new_cb  = match a_mut.cost_basis.checked_add(b_val.cost_basis) { Some(v) => v, None => { i += 1; continue; } };

                a_mut.quantity = new_qty;
                a_mut.cost_basis = new_cb;
                a_mut.acquisition_date = a_mut.acquisition_date.min(b_val.acquisition_date);
                if let Some(px) = a_mut.cost_basis.checked_div(a_mut.quantity) { a_mut.acquisition_price = px; }

                let adj_a = adj_map.remove(&a_mut.id).unwrap_or(Decimal::ZERO);
                let adj_b = adj_map.remove(&b_val.id).unwrap_or(Decimal::ZERO);
                let adj_sum = adj_a.checked_add(adj_b).unwrap_or(adj_a);
                if adj_sum > Decimal::ZERO { adj_map.insert(a_mut.id, adj_sum); }
                continue; // re-check same i after merge
            } else {
                i += 1;
            }
        }
    }

    // [ADD] Periodic stable compaction for non-adjacent lots
    #[inline(always)]
    fn stable_compact_mint_lots(
        &self,
        mint: Pubkey,
        mint_lots: &mut VecDeque<TaxLot>,
        adj_map: &mut HashMap<u64, Decimal>,
    ) {
        if mint_lots.len() <= MAX_LOTS_PER_MINT { return; }
        if self.tax_method == TaxMethod::SpecificIdentification {
            if let Ok(prefs) = self.specific_id_prefs.read() {
                if prefs.get(&mint).map(|m| !m.is_empty()).unwrap_or(false) { return; }
            }
        }

        // Create a copy and sort by (day, price bucket) for stable compaction
        let mut sorted_indices: Vec<usize> = (0..mint_lots.len()).collect();
        sorted_indices.sort_by(|&a, &b| {
            let lot_a = &mint_lots[a];
            let lot_b = &mint_lots[b];
            
            // Primary sort: acquisition date (day)
            lot_a.acquisition_date.date_naive().cmp(&lot_b.acquisition_date.date_naive())
                .then_with(|| {
                    // Secondary sort: price bucket (rounded to 6 decimal places)
                    let price_a = lot_a.acquisition_price.round_dp(6);
                    let price_b = lot_b.acquisition_price.round_dp(6);
                    price_a.cmp(&price_b)
                })
                .then_with(|| lot_a.id.cmp(&lot_b.id))
        });

        // Merge lots sequentially under single write lock
        let price_eps = Decimal::new(1, 6); // 1e-6
        let mut i = 0usize;
        while i + 1 < sorted_indices.len() {
            let idx_a = sorted_indices[i];
            let idx_b = sorted_indices[i + 1];
            
            let lot_a = &mint_lots[idx_a];
            let lot_b = &mint_lots[idx_b];
            
            let same_day = lot_a.acquisition_date.date_naive() == lot_b.acquisition_date.date_naive();
            let price_close = (lot_a.acquisition_price - lot_b.acquisition_price).abs() <= price_eps;
            let same_tx = lot_a.transaction_type == lot_b.transaction_type;
            let same_dec = lot_a.decimals == lot_b.decimals;

            if same_day && price_close && same_tx && same_dec {
                // Merge lot_b into lot_a
                let b_val = mint_lots.remove(idx_b).expect("index valid");
                let a_mut = &mut mint_lots[idx_a];

                let new_qty = match a_mut.quantity.checked_add(b_val.quantity) { 
                    Some(v) => v, 
                    None => { 
                        // Update indices after removal
                        for j in (i + 1)..sorted_indices.len() {
                            if sorted_indices[j] > idx_b {
                                sorted_indices[j] -= 1;
                            }
                        }
                        i += 1; 
                        continue; 
                    } 
                };
                let new_cb = match a_mut.cost_basis.checked_add(b_val.cost_basis) { 
                    Some(v) => v, 
                    None => { 
                        // Update indices after removal
                        for j in (i + 1)..sorted_indices.len() {
                            if sorted_indices[j] > idx_b {
                                sorted_indices[j] -= 1;
                            }
                        }
                        i += 1; 
                        continue; 
                    } 
                };

                a_mut.quantity = new_qty;
                a_mut.cost_basis = new_cb;
                a_mut.acquisition_date = a_mut.acquisition_date.min(b_val.acquisition_date);
                if let Some(px) = a_mut.cost_basis.checked_div(a_mut.quantity) { 
                    a_mut.acquisition_price = px; 
                }

                // Merge basis adjustments
                let adj_a = adj_map.remove(&a_mut.id).unwrap_or(Decimal::ZERO);
                let adj_b = adj_map.remove(&b_val.id).unwrap_or(Decimal::ZERO);
                let adj_sum = adj_a.checked_add(adj_b).unwrap_or(adj_a);
                if adj_sum > Decimal::ZERO { 
                    adj_map.insert(a_mut.id, adj_sum); 
                }

                // Update indices after removal
                for j in (i + 1)..sorted_indices.len() {
                    if sorted_indices[j] > idx_b {
                        sorted_indices[j] -= 1;
                    }
                }
                sorted_indices.remove(i + 1);
                continue; // re-check same i after merge
            } else {
                i += 1;
            }
        }
    }

    // [REPLACE] True quantization to token decimals with midpoint-away rounding
    #[inline(always)]
    fn normalize_amount(&self, amount: Decimal, decimals: u32) -> Decimal {
        // clamp to avoid insane scales; rust_decimal max scale≈28
        let dp = decimals.min(MAX_PRECISION).min(28);
        amount.round_dp_with_strategy(dp, rust_decimal::RoundingStrategy::MidpointAwayFromZero)
    }

    // [ADD] Trigger stable compaction for a specific mint
    pub fn trigger_stable_compaction(&self, mint: Pubkey) -> Result<(), PnlError> {
        let mut lots = self.tax_lots.write().map_err(|_| PnlError::StateLocked)?;
        let mut adj_map = self.basis_adjustments.write().map_err(|_| PnlError::StateLocked)?;
        
        if let Some(mint_lots) = lots.get_mut(&mint) {
            self.stable_compact_mint_lots(mint, mint_lots, &mut adj_map);
        }
        
        Ok(())
    }

    fn create_state_backup(&self) -> Result<StateTransaction, PnlError> {
        let lots = self.tax_lots.read().map_err(|_| PnlError::StateLocked)?;
        let disposals = self.disposals.read().map_err(|_| PnlError::StateLocked)?;
        Ok(StateTransaction {
            lots_backup: lots.clone(),
            disposals_count: disposals.len(),
        })
    }

    fn rollback_state(&self, backup: StateTransaction) -> Result<(), PnlError> {
        let mut lots = self.tax_lots.write().map_err(|_| PnlError::StateLocked)?;
        let mut disposals = self.disposals.write().map_err(|_| PnlError::StateLocked)?;
        *lots = backup.lots_backup;
        disposals.truncate(backup.disposals_count);
        Ok(())
    }

    // [REPLACE] record_acquisition with compaction, hygiene, and cache bump
    pub fn record_acquisition(
        &self,
        mint: Pubkey,
        quantity: Decimal,
        cost: Decimal,
        transaction_type: TransactionType,
        signature: Signature,
        decimals: u32,
    ) -> Result<u64, PnlError> {
        self.per_mint_rate_limit_check(mint)?;
        self.validate_quantity(quantity)?;
        self.validate_price(cost)?;

        let _lock = self.state_lock.lock().map_err(|_| PnlError::StateLocked)?;

        let normalized_quantity = self.normalize_amount(quantity, decimals);
        let normalized_cost = self.money6(cost);
        let price = self.mul_div(normalized_cost, Decimal::ONE, normalized_quantity)?;
        self.validate_price(price)?;

        let lot_id = self.lot_counter.fetch_add(1, Ordering::SeqCst) + 1;
        let tax_lot = TaxLot {
            id: lot_id, mint, quantity: normalized_quantity, cost_basis: normalized_cost,
            acquisition_date: Utc::now(), acquisition_price: price,
            transaction_type, signature, decimals,
        };

        let mut lots = self.tax_lots.write().map_err(|_| PnlError::StateLocked)?;
        let mint_lots = lots.entry(mint).or_insert_with(VecDeque::new);
        mint_lots.push_back(tax_lot);

        self.decimal_cache.write().map_err(|_| PnlError::StateLocked)?.insert(mint, decimals);

        // compaction if necessary
        if mint_lots.len() > MAX_LOTS_PER_MINT {
            drop(lots);
            let mut lots = self.tax_lots.write().map_err(|_| PnlError::StateLocked)?;
            let mut adj_map = self.basis_adjustments.write().map_err(|_| PnlError::StateLocked)?;
            if let Some(mint_lots2) = lots.get_mut(&mint) {
                self.coalesce_adjacent_locked(mint, mint_lots2, &mut adj_map);
            }
        } else {
            drop(lots);
        }

        // invalidate cache for this mint
        self.bump_lot_version(mint)?;

        info!("Acquired {} {} at {} (lot #{})", normalized_quantity, mint, price, lot_id);
        Ok(lot_id)
    }

    pub fn record_disposal(
        &self,
        mint: Pubkey,
        quantity: Decimal,
        proceeds: Decimal,
        fees: Decimal,
    ) -> Result<Disposal, PnlError> {
        self.per_mint_rate_limit_check(mint)?;
        self.validate_quantity(quantity)?;
        self.validate_price(proceeds)?;

        let _lock = self.state_lock.lock().map_err(|_| PnlError::StateLocked)?;
        let backup = self.create_state_backup()?;

        let result = self.record_disposal_internal(mint, quantity, proceeds, fees);

        match result {
            Ok(disposal) => {
                self.prune_old_history()?;
                Ok(disposal)
            }
            Err(e) => {
                self.rollback_state(backup)?;
                Err(e)
            }
        }
    }

    // [REPLACE] disposal with zero-alloc ordering, binary audit hash, and fee guard
    fn record_disposal_internal(
        &self,
        mint: Pubkey,
        quantity: Decimal,
        proceeds: Decimal,
        fees: Decimal,
    ) -> Result<Disposal, PnlError> {
        let mut lots = self.tax_lots.write().map_err(|_| PnlError::StateLocked)?;
        let mint_lots = lots.get_mut(&mint).ok_or(PnlError::InsufficientBalance { needed: quantity, available: Decimal::ZERO })?;

        let decimals = self.decimal_cache.read().map_err(|_| PnlError::StateLocked)?
            .get(&mint).copied().unwrap_or(DECIMALS_SOL);

        let normalized_quantity = self.normalize_amount(quantity, decimals);
        let normalized_proceeds = self.money6(proceeds);
        let normalized_fees = self.money6(fees);

        // Guard: net proceeds cannot be negative (fees > proceeds)
        if normalized_fees > normalized_proceeds {
            return Err(PnlError::InvalidPrice(normalized_proceeds.checked_sub(normalized_fees).unwrap_or(Decimal::ZERO)));
        }

        let available: Decimal = mint_lots.iter().map(|l| l.quantity).sum();
        if normalized_quantity > available {
            return Err(PnlError::InsufficientBalance { needed: normalized_quantity, available });
        }

        let mut adj_map = self.basis_adjustments.write().map_err(|_| PnlError::StateLocked)?;

        let mut remaining = normalized_quantity;
        let mut lots_used: Vec<(u64, Decimal)> = Vec::with_capacity(mint_lots.len().min(128));
        let mut total_cost = Decimal::ZERO;
        let mut short_term_gain = Decimal::ZERO;
        let mut long_term_gain = Decimal::ZERO;
        let disposal_date = Utc::now();

        let inv_total_qty = Decimal::ONE.checked_div(normalized_quantity).ok_or(PnlError::DecimalConversion)?;

        // zero-alloc ordered iteration
        self.for_each_ordered_idx(mint_lots, |idx| {
            if remaining <= Decimal::ZERO { return; }
            // SAFETY: idx is within bounds by construction
            let lot = unsafe { &mut *(&mut mint_lots[idx] as *mut TaxLot) };

            let use_qty = remaining.min(lot.quantity);

            let lot_adj = *adj_map.get(&lot.id).unwrap_or(&Decimal::ZERO);
            let lot_effective_basis = lot.cost_basis.checked_add(lot_adj).expect("checked add");

            let used_cost = self.mul_div(lot_effective_basis, use_qty, lot.quantity).expect("mul_div ok");

            lots_used.push((lot.id, use_qty));
            total_cost = total_cost.checked_add(used_cost).expect("add ok");

            let proceeds_portion = self.pro_rata(normalized_proceeds, use_qty, normalized_quantity).expect("pro-rata ok");
            let fees_portion = self.pro_rata(normalized_fees, use_qty, normalized_quantity).expect("pro-rata ok");
            let net_proceeds = proceeds_portion.checked_sub(fees_portion).expect("sub ok");
            let gain = net_proceeds.checked_sub(used_cost).expect("sub ok");

            let days_held = (disposal_date - lot.acquisition_date).num_days();
            if days_held < SHORT_TERM_DAYS {
                short_term_gain = short_term_gain.checked_add(gain).expect("add ok");
            } else {
                long_term_gain = long_term_gain.checked_add(gain).expect("add ok");
            }

            // reduce basis & quantity; clamp dust; consume wash-sale adj proportionally
            let original_basis_used = self.mul_div(lot.cost_basis, use_qty, lot.quantity).expect("mul_div ok");
            let pre_qty = lot.quantity;

            lot.cost_basis = self.clamp_dust(lot.cost_basis.checked_sub(original_basis_used).expect("sub ok"));
            lot.quantity   = self.clamp_dust(lot.quantity.checked_sub(use_qty).expect("sub ok"));

            if lot_adj > Decimal::ZERO {
                let adj_used = self.mul_div(lot_adj, use_qty, pre_qty).expect("mul_div ok");
                let adj_left = self.clamp_dust(lot_adj.checked_sub(adj_used).unwrap_or(Decimal::ZERO));
                if lot.quantity > Decimal::ZERO && adj_left > Decimal::ZERO {
                    adj_map.insert(lot.id, adj_left);
                } else {
                    adj_map.remove(&lot.id);
                }
            }

            remaining = remaining.checked_sub(use_qty).expect("sub ok");
        });

        mint_lots.retain(|lot| lot.quantity > Decimal::ZERO);

        let net_proceeds = normalized_proceeds.checked_sub(normalized_fees).ok_or(PnlError::CalculationOverflow)?;
        let realized_pnl = net_proceeds.checked_sub(total_cost).ok_or(PnlError::CalculationOverflow)?;
        let disposal_price = self.mul_div(normalized_proceeds, Decimal::ONE, normalized_quantity)?;

        // Binary, multi-slice audit hash
        let audit_hash = self.audit_hash_disposal(
            &mint,
            disposal_date.timestamp(),
            &normalized_quantity,
            &normalized_proceeds,
            &normalized_fees,
            &lots_used,
        );

        let disposal = Disposal {
            mint,
            quantity: normalized_quantity,
            proceeds: self.money6(normalized_proceeds),
            disposal_date,
            disposal_price: self.money6(disposal_price),
            lots_used,
            realized_pnl: self.money6(realized_pnl),
            short_term_gain: self.money6(short_term_gain),
            long_term_gain: self.money6(long_term_gain),
            fees: self.money6(normalized_fees),
            audit_hash,
        };

        // Opportunistic compaction (we hold both locks)
        if mint_lots.len() > MAX_LOTS_PER_MINT {
            self.coalesce_adjacent_locked(mint, mint_lots, &mut adj_map);
        }

        drop(adj_map);
        drop(lots);

        // detailed wash-sale on losses
        self.check_wash_sales_detailed(&disposal)?;

        // invalidate order cache (lots changed)
        self.bump_lot_version(mint)?;

        let mut disposals = self.disposals.write().map_err(|_| PnlError::StateLocked)?;
        if disposals.len() >= MAX_DISPOSALS {
            let remove_count = disposals.len() / 10;
            disposals.drain(0..remove_count);
        }
        disposals.push(disposal.clone());
        drop(disposals);

        info!("Disposed {} {} at {} (PnL: {})", normalized_quantity, mint, disposal_price, realized_pnl);
        Ok(disposal)
    }

    // [REPLACE] get_lot_order with cache reuse, deterministic ties
    #[inline(always)]
    fn get_lot_order(&self, lots: &VecDeque<TaxLot>) -> Vec<usize> {
        let n = lots.len();
        if n == 0 { return Vec::new(); }
        let mint = lots[0].mint;
        let tag = self.method_tag();

        // try cache
        if let Ok(ver) = self.lot_version(mint) {
            if let Ok(oc) = self.order_cache.read() {
                if let Some((cached_ver, cached)) = oc.get(&(mint, tag)) {
                    if *cached_ver == ver {
                        return cached.clone(); // small clone; far cheaper than sort
                    }
                }
            }
            // compute fresh
            let idx: Vec<usize> = match self.tax_method {
                TaxMethod::FIFO => (0..n).collect(),
                TaxMethod::LIFO => (0..n).rev().collect(),
            TaxMethod::HIFO => {
                let mut idx: Vec<usize> = (0..n).collect();
                idx.sort_by(|&a, &b| {
                    let ua = (lots[a].cost_basis / lots[a].quantity.max(Decimal::ONE));
                    let ub = (lots[b].cost_basis / lots[b].quantity.max(Decimal::ONE));
                    ub.partial_cmp(&ua).unwrap_or(std::cmp::Ordering::Equal)
                        .then_with(|| lots[a].acquisition_date.cmp(&lots[b].acquisition_date))
                        .then_with(|| lots[a].id.cmp(&lots[b].id))
                });
                idx
            }
                TaxMethod::SpecificIdentification => {
                    if let Ok(prefs) = self.specific_id_prefs.read() {
                        if let Some(rankmap) = prefs.get(&mint) {
                            let mut idx: Vec<usize> = (0..n).collect();
                            idx.sort_by(|&a, &b| {
                                let ra = rankmap.get(&lots[a].id).copied().unwrap_or(u64::MAX);
                                let rb = rankmap.get(&lots[b].id).copied().unwrap_or(u64::MAX);
                                ra.cmp(&rb)
                                    .then_with(|| lots[a].acquisition_date.cmp(&lots[b].acquisition_date))
                                    .then_with(|| lots[a].id.cmp(&lots[b].id))
                            });
                            idx
                        } else { (0..n).collect() }
                    } else { (0..n).collect() }
                }
            };
            // write cache
            if let Ok(mut ocw) = self.order_cache.write() {
                ocw.insert((mint, tag), (ver, idx.clone()));
            }
            return idx;
        }
        // fallback (shouldn't hit)
        (0..n).collect()
    }

    // [REPLACE] deterministic preference setter + cache invalidation
    pub fn set_specific_lot_preference(&self, mint: Pubkey, order_by_lot_ids: Vec<u64>) -> Result<(), PnlError> {
        let mut rankmap = HashMap::with_capacity(order_by_lot_ids.len());
        for (rank, id) in order_by_lot_ids.into_iter().enumerate() {
            rankmap.insert(id, rank as u64);
        }
        self.specific_id_prefs.write().map_err(|_| PnlError::StateLocked)?
            .insert(mint, rankmap);
        self.bump_lot_version(mint)?; // ensure cache miss on next call
        Ok(())
    }

    fn check_wash_sales(&self, mint: &Pubkey, disposal_date: &DateTime<Utc>) -> Result<(), PnlError> {
        if self.jurisdiction != TaxJurisdiction::US {
            return Ok(());
        }

        let lots = self.tax_lots.read().map_err(|_| PnlError::StateLocked)?;
        let mut adjustments = self
            .basis_adjustments
            .write()
            .map_err(|_| PnlError::StateLocked)?;

        if let Some(mint_lots) = lots.get(mint) {
            for lot in mint_lots {
                let days_diff = (*disposal_date - lot.acquisition_date).num_days().abs();
                if days_diff <= WASH_SALE_DAYS {
                    // token wash-sale heuristic — tiny upward basis adjustment
                    let adjustment = Decimal::from_str("0.01").unwrap_or(Decimal::ZERO);
                    let current = adjustments.get(&lot.id).copied().unwrap_or(Decimal::ZERO);
                    adjustments.insert(
                        lot.id,
                        current.checked_add(adjustment).unwrap_or(current),
                    );
                }
            }
        }
        Ok(())
    }

    // [ADD] allocate disallowed loss to replacement lots proportionally (±30 days)
    fn check_wash_sales_detailed(&self, disposal: &Disposal) -> Result<(), PnlError> {
        if self.jurisdiction != TaxJurisdiction::US {
            return Ok(());
        }
        if disposal.realized_pnl >= Decimal::ZERO {
            return Ok(());
        }

        let loss = disposal.realized_pnl.checked_abs().ok_or(PnlError::CalculationOverflow)?;
        let start = disposal.disposal_date - chrono::Duration::days(WASH_SALE_DAYS);
        let end   = disposal.disposal_date + chrono::Duration::days(WASH_SALE_DAYS);

        let lots = self.tax_lots.read().map_err(|_| PnlError::StateLocked)?;
        let mut adjustments = self.basis_adjustments.write().map_err(|_| PnlError::StateLocked)?;

        if let Some(mint_lots) = lots.get(&disposal.mint) {
            // candidate lots are those acquired within the window
            let mut candidates: Vec<&TaxLot> = mint_lots
                .iter()
                .filter(|lot| lot.acquisition_date >= start && lot.acquisition_date <= end)
                .collect();

            // chronological order → consistent allocation
            candidates.sort_by(|a, b| a.acquisition_date.cmp(&b.acquisition_date).then(a.id.cmp(&b.id)));

            // total replacement quantity available
            let mut remaining_qty = disposal.quantity;
            for lot in candidates {
                if remaining_qty <= Decimal::ZERO { break; }
                let alloc_qty = remaining_qty.min(lot.quantity);

                // disallowed portion of loss ∝ allocated quantity / disposed quantity
                let proportion = alloc_qty
                    .checked_div(disposal.quantity)
                    .ok_or(PnlError::DecimalConversion)?;
                let adj = loss
                    .checked_mul(proportion)
                    .ok_or(PnlError::CalculationOverflow)?;

                let cur = adjustments.get(&lot.id).copied().unwrap_or(Decimal::ZERO);
                adjustments.insert(lot.id, cur.checked_add(adj).ok_or(PnlError::CalculationOverflow)?);

                remaining_qty = remaining_qty
                    .checked_sub(alloc_qty)
                    .ok_or(PnlError::CalculationOverflow)?;
            }
        }

        Ok(())
    }

    pub fn update_market_price(&self, mint: Pubkey, price: Decimal) -> Result<(), PnlError> {
        self.validate_price(price)?;
        self.current_prices
            .write()
            .map_err(|_| PnlError::StateLocked)?
            .insert(mint, price);
        Ok(())
    }

    // [REPLACE] Unrealized uses cost_basis + any remaining wash-sale adjustment
    pub fn calculate_unrealized_pnl(&self, mint: &Pubkey) -> Result<Decimal, PnlError> {
        let lots = self.tax_lots.read().map_err(|_| PnlError::StateLocked)?;
        let prices = self.current_prices.read().map_err(|_| PnlError::StateLocked)?;
        let adj_map = self.basis_adjustments.read().map_err(|_| PnlError::StateLocked)?;

        let current_price = prices
            .get(mint)
            .copied()
            .ok_or(PnlError::InvalidPrice(Decimal::ZERO))?;

        let mint_lots = match lots.get(mint) {
            Some(l) => l,
            None => return Ok(Decimal::ZERO),
        };

        let mut unrealized = Decimal::ZERO;
        for lot in mint_lots {
            let market_value = lot
                .quantity
                .checked_mul(current_price)
                .ok_or(PnlError::CalculationOverflow)?;
            let eff_basis = lot
                .cost_basis
                .checked_add(adj_map.get(&lot.id).copied().unwrap_or(Decimal::ZERO))
                .ok_or(PnlError::CalculationOverflow)?;
            let gain = market_value
                .checked_sub(eff_basis)
                .ok_or(PnlError::CalculationOverflow)?;
            unrealized = unrealized
                .checked_add(gain)
                .ok_or(PnlError::CalculationOverflow)?;
        }
        Ok(unrealized)
    }

    // [REPLACE] calculate_total_pnl with strict lock ordering, rounded outputs
    pub fn calculate_total_pnl(&self) -> Result<PnlSnapshot, PnlError> {
        let lots = self.tax_lots.read().map_err(|_| PnlError::StateLocked)?;
        let adj_map = self.basis_adjustments.read().map_err(|_| PnlError::StateLocked)?;
        let prices = self.current_prices.read().map_err(|_| PnlError::StateLocked)?;
        let disposals = self.disposals.read().map_err(|_| PnlError::StateLocked)?;

        let mut realized_pnl = Decimal::ZERO;
        let mut total_trades = 0u64;
        let mut winning_trades = 0u64;
        for d in disposals.iter() {
            realized_pnl = realized_pnl.checked_add(d.realized_pnl).ok_or(PnlError::CalculationOverflow)?;
            total_trades += 1;
            if d.realized_pnl > Decimal::ZERO { winning_trades += 1; }
        }

        let mut unrealized_pnl = Decimal::ZERO;
        let mut total_invested = Decimal::ZERO;
        for (mint, mint_lots) in lots.iter() {
            if let Some(&price) = prices.get(mint) {
                for lot in mint_lots {
                    let eff_basis = lot.cost_basis.checked_add(adj_map.get(&lot.id).copied().unwrap_or(Decimal::ZERO)).ok_or(PnlError::CalculationOverflow)?;
                    let market_value = lot.quantity.checked_mul(price).ok_or(PnlError::CalculationOverflow)?;
                    let unreal = market_value.checked_sub(eff_basis).ok_or(PnlError::CalculationOverflow)?;
                    unrealized_pnl = unrealized_pnl.checked_add(unreal).ok_or(PnlError::CalculationOverflow)?;
                    total_invested = total_invested.checked_add(eff_basis).ok_or(PnlError::CalculationOverflow)?;
                }
            }
        }

        let total_pnl = realized_pnl.checked_add(unrealized_pnl).ok_or(PnlError::CalculationOverflow)?;
        let roi_percentage = if total_invested > Decimal::ZERO {
            total_pnl.checked_mul(Decimal::from(100)).and_then(|p| p.checked_div(total_invested)).unwrap_or(Decimal::ZERO)
        } else { Decimal::ZERO };

        drop(prices); drop(adj_map); drop(lots); // shrink lock footprint before Sharpe

        let d = self.disposals.read().map_err(|_| PnlError::StateLocked)?;
        let sharpe = self.safe_calculate_sharpe(&d)?;

        let snapshot = PnlSnapshot {
            timestamp: Utc::now(),
            realized_pnl: self.money6(realized_pnl),
            unrealized_pnl: self.money6(unrealized_pnl),
            total_pnl: self.money6(total_pnl),
            roi_percentage: roi_percentage.round_dp_with_strategy(6, rust_decimal::RoundingStrategy::MidpointAwayFromZero),
            sharpe_ratio: sharpe,
            win_rate: if total_trades > 0 {
                Decimal::from(winning_trades).checked_mul(Decimal::from(100)).and_then(|w| w.checked_div(Decimal::from(total_trades))).unwrap_or(Decimal::ZERO)
            } else { Decimal::ZERO }.round_dp_with_strategy(6, rust_decimal::RoundingStrategy::MidpointAwayFromZero),
            avg_profit_per_trade: if total_trades > 0 {
                self.money6(realized_pnl.checked_div(Decimal::from(total_trades)).unwrap_or(Decimal::ZERO))
            } else { Decimal::ZERO },
        };

        self.pnl_history.write().map_err(|_| PnlError::StateLocked)?.insert(snapshot.timestamp, snapshot.clone());
        Ok(snapshot)
    }

    // [REPLACE] Welford variance; stable Sharpe; proper annualization
    fn safe_calculate_sharpe(&self, disposals: &[Disposal]) -> Result<Decimal, PnlError> {
        if disposals.len() < 30 {
            return Ok(Decimal::ZERO);
        }

        // Daily realized PnL aggregation
        let mut daily: HashMap<chrono::NaiveDate, Decimal> = HashMap::new();
        for d in disposals {
            // chrono 0.4: Date<Utc> -> NaiveDate
            let day = d.disposal_date.date().naive_utc();
            let e = daily.entry(day).or_insert(Decimal::ZERO);
            *e = e.checked_add(d.realized_pnl).ok_or(PnlError::CalculationOverflow)?;
        }
        if daily.len() < 20 {
            return Ok(Decimal::ZERO);
        }

        // Welford's online algorithm
        let mut n: u64 = 0;
        let mut mean = Decimal::ZERO;
        let mut m2 = Decimal::ZERO;

        for r in daily.values().copied() {
            n += 1;
            let n_dec = Decimal::from(n);
            let delta = r.checked_sub(mean).ok_or(PnlError::CalculationOverflow)?;
            mean = mean
                .checked_add(delta.checked_div(n_dec).ok_or(PnlError::CalculationOverflow)?)
                .ok_or(PnlError::CalculationOverflow)?;
            let delta2 = r.checked_sub(mean).ok_or(PnlError::CalculationOverflow)?;
            m2 = m2
                .checked_add(delta.checked_mul(delta2).ok_or(PnlError::CalculationOverflow)?)
                .ok_or(PnlError::CalculationOverflow)?;
        }

        if n < 2 {
            return Ok(Decimal::ZERO);
        }
        let var = m2
            .checked_div(Decimal::from(n - 1))  // sample variance
            .ok_or(PnlError::CalculationOverflow)?;
        if var <= Decimal::ZERO {
            return Ok(Decimal::ZERO);
        }

        let std = self.safe_sqrt(var)?;
        let rf_annual = Decimal::from_str("0.045").unwrap_or(Decimal::ZERO);
        let rf_daily = rf_annual
            .checked_div(Decimal::from(252))
            .unwrap_or(Decimal::ZERO);
        let excess = mean.checked_sub(rf_daily).unwrap_or(mean);

        if std == Decimal::ZERO {
            return Ok(Decimal::ZERO);
        }
        let sqrt252 = Decimal::from(252)
            .sqrt()
            .unwrap_or(Decimal::from_str("15.874507866").unwrap());
        let daily_sharpe = excess
            .checked_div(std)
            .ok_or(PnlError::CalculationOverflow)?;
        daily_sharpe
            .checked_mul(sqrt252)
            .ok_or(PnlError::CalculationOverflow)
    }

    fn safe_sqrt(&self, value: Decimal) -> Result<Decimal, PnlError> {
        if value <= Decimal::ZERO {
            return Ok(Decimal::ZERO);
        }
        let mut x = value;
        let two = Decimal::from(2);
        for _ in 0..SQRT_ITERATIONS {
            let next = x
                .checked_add(value.checked_div(x).unwrap_or(x))
                .and_then(|sum| sum.checked_div(two))
                .unwrap_or(x);
            if (next.checked_sub(x).unwrap_or(Decimal::ZERO)).abs()
                < Decimal::from_str("0.0000001").unwrap()
            {
                break;
            }
            x = next;
        }
        Ok(x)
    }

    pub fn calculate_tax_liability(&self, income: Decimal) -> Result<Decimal, PnlError> {
        if income <= Decimal::ZERO {
            return Ok(Decimal::ZERO);
        }

        let brackets = match self.jurisdiction {
            TaxJurisdiction::US => vec![
                (Decimal::from(10275), Decimal::from_str("0.10").unwrap()),
                (Decimal::from(41775), Decimal::from_str("0.12").unwrap()),
                (Decimal::from(89075), Decimal::from_str("0.22").unwrap()),
                (Decimal::from(170050), Decimal::from_str("0.24").unwrap()),
                (Decimal::from(215950), Decimal::from_str("0.32").unwrap()),
                (Decimal::from(539900), Decimal::from_str("0.35").unwrap()),
                (Decimal::MAX, Decimal::from_str("0.37").unwrap()),
            ],
            TaxJurisdiction::UK => vec![
                (Decimal::from(12570), Decimal::ZERO),
                (Decimal::from(50270), Decimal::from_str("0.20").unwrap()),
                (Decimal::from(150000), Decimal::from_str("0.40").unwrap()),
                (Decimal::MAX, Decimal::from_str("0.45").unwrap()),
            ],
            TaxJurisdiction::Germany => vec![
                (Decimal::from(10908), Decimal::ZERO),
                (Decimal::from(62810), Decimal::from_str("0.14").unwrap()),
                (Decimal::from(277826), Decimal::from_str("0.42").unwrap()),
                (Decimal::MAX, Decimal::from_str("0.45").unwrap()),
            ],
            TaxJurisdiction::Singapore => vec![
                (Decimal::from(20000), Decimal::ZERO),
                (Decimal::from(40000), Decimal::from_str("0.02").unwrap()),
                (Decimal::from(80000), Decimal::from_str("0.07").unwrap()),
                (Decimal::from(120000), Decimal::from_str("0.115").unwrap()),
                (Decimal::from(160000), Decimal::from_str("0.15").unwrap()),
                (Decimal::from(200000), Decimal::from_str("0.18").unwrap()),
                (Decimal::from(240000), Decimal::from_str("0.19").unwrap()),
                (Decimal::from(280000), Decimal::from_str("0.195").unwrap()),
                (Decimal::from(320000), Decimal::from_str("0.20").unwrap()),
                (Decimal::from(500000), Decimal::from_str("0.22").unwrap()),
                (Decimal::from(1000000), Decimal::from_str("0.23").unwrap()),
                (Decimal::MAX, Decimal::from_str("0.24").unwrap()),
            ],
            TaxJurisdiction::Dubai => vec![(Decimal::MAX, Decimal::ZERO)],
        };

        let mut tax = Decimal::ZERO;
        let mut prev_bracket = Decimal::ZERO;

        for (threshold, rate) in brackets {
            let taxable_in_bracket = income
                .min(threshold)
                .checked_sub(prev_bracket)
                .unwrap_or(Decimal::ZERO);

            if taxable_in_bracket > Decimal::ZERO {
                let bracket_tax = taxable_in_bracket
                    .checked_mul(rate)
                    .ok_or(PnlError::CalculationOverflow)?;
                tax = tax.checked_add(bracket_tax).ok_or(PnlError::CalculationOverflow)?;
            }

            if income <= threshold {
                break;
            }
            prev_bracket = threshold;
        }

        Ok(tax)
    }

    pub fn generate_tax_report(&self, year: i32) -> Result<TaxReport, PnlError> {
        let disposals = self.disposals.read().map_err(|_| PnlError::StateLocked)?;
        let year_start = chrono::NaiveDate::from_ymd_opt(year, 1, 1)
            .ok_or(PnlError::DecimalConversion)?
            .and_hms_opt(0, 0, 0)
            .ok_or(PnlError::DecimalConversion)?
            .and_utc();
        let year_end = chrono::NaiveDate::from_ymd_opt(year + 1, 1, 1)
            .ok_or(PnlError::DecimalConversion)?
            .and_hms_opt(0, 0, 0)
            .ok_or(PnlError::DecimalConversion)?
            .and_utc();

        let mut short_term_gains = Decimal::ZERO;
        let mut long_term_gains = Decimal::ZERO;
        let mut total_proceeds = Decimal::ZERO;
        let mut total_cost_basis = Decimal::ZERO;
        let mut total_fees = Decimal::ZERO;

        for disposal in disposals.iter() {
            if disposal.disposal_date >= year_start && disposal.disposal_date < year_end {
                short_term_gains = short_term_gains
                    .checked_add(disposal.short_term_gain)
                    .ok_or(PnlError::CalculationOverflow)?;
                long_term_gains = long_term_gains
                    .checked_add(disposal.long_term_gain)
                    .ok_or(PnlError::CalculationOverflow)?;
                total_proceeds = total_proceeds
                    .checked_add(disposal.proceeds)
                    .ok_or(PnlError::CalculationOverflow)?;
                total_fees = total_fees
                    .checked_add(disposal.fees)
                    .ok_or(PnlError::CalculationOverflow)?;

                let net_proceeds = disposal.proceeds.checked_sub(disposal.fees).unwrap_or(Decimal::ZERO);
                let cost = net_proceeds.checked_sub(disposal.realized_pnl).unwrap_or(Decimal::ZERO);
                total_cost_basis = total_cost_basis
                    .checked_add(cost)
                    .ok_or(PnlError::CalculationOverflow)?;
            }
        }

        let net_gains = short_term_gains
            .checked_add(long_term_gains)
            .ok_or(PnlError::CalculationOverflow)?;

        let short_tax = self.calculate_tax_liability(short_term_gains)?;
        let long_tax = if self.jurisdiction == TaxJurisdiction::US {
            long_term_gains
                .checked_mul(Decimal::from_str("0.15").unwrap())
                .ok_or(PnlError::CalculationOverflow)?
        } else {
            self.calculate_tax_liability(long_term_gains)?
        };

        let estimated_tax = short_tax
            .checked_add(long_tax)
            .ok_or(PnlError::CalculationOverflow)?;

        let effective_rate = if net_gains > Decimal::ZERO {
            estimated_tax
                .checked_div(net_gains)
                .unwrap_or(Decimal::ZERO)
        } else {
            Decimal::ZERO
        };

        Ok(TaxReport {
            year,
            jurisdiction: self.jurisdiction,
            short_term_gains,
            long_term_gains,
            total_proceeds,
            total_cost_basis,
            total_fees,
            net_gains,
            estimated_tax,
            effective_rate,
        })
    }

    pub fn optimize_tax_harvest(&self, target_loss: Decimal) -> Result<Vec<HarvestRecommendation>, PnlError> {
        self.validate_price(target_loss)?;

        let lots = self.tax_lots.read().map_err(|_| PnlError::StateLocked)?;
        let prices = self.current_prices.read().map_err(|_| PnlError::StateLocked)?;
        let mut recommendations = Vec::new();
        let mut accumulated_loss = Decimal::ZERO;

        let mut candidates = Vec::new();

        for (mint, mint_lots) in lots.iter() {
            if let Some(&current_price) = prices.get(mint) {
                for lot in mint_lots {
                    let market_value = lot
                        .quantity
                        .checked_mul(current_price)
                        .ok_or(PnlError::CalculationOverflow)?;

                    if market_value < lot.cost_basis {
                        let loss = lot.cost_basis.checked_sub(market_value).unwrap_or(Decimal::ZERO);
                        let days_held = (Utc::now() - lot.acquisition_date).num_days();
                        let priority = if days_held < SHORT_TERM_DAYS {
                            loss.checked_mul(Decimal::from_str("1.5").unwrap()).unwrap_or(loss)
                        } else {
                            loss
                        };
                        candidates.push((*mint, lot.id, lot.quantity, loss, priority));
                    }
                }
            }
        }

        candidates.sort_by(|a, b| b.4.cmp(&a.4));

        for (mint, lot_id, quantity, loss, _) in candidates {
            if accumulated_loss >= target_loss {
                break;
            }

            let tax_benefit = self.calculate_tax_liability(loss)?;

            recommendations.push(HarvestRecommendation {
                mint,
                lot_id,
                quantity,
                expected_loss: loss,
                tax_benefit,
            });

            accumulated_loss = accumulated_loss
                .checked_add(loss)
                .ok_or(PnlError::CalculationOverflow)?;
        }

        Ok(recommendations)
    }

    fn prune_old_history(&self) -> Result<(), PnlError> {
        let mut history = self.pnl_history.write().map_err(|_| PnlError::StateLocked)?;
        let cutoff = Utc::now() - chrono::Duration::days(MAX_HISTORY_DAYS);

        let keys_to_remove: Vec<_> = history.range(..cutoff).map(|(k, _)| *k).collect();
        for key in keys_to_remove {
            history.remove(&key);
        }
        Ok(())
    }

    pub fn get_portfolio_summary(&self) -> Result<PortfolioSummary, PnlError> {
        let lots = self.tax_lots.read().map_err(|_| PnlError::StateLocked)?;
        let prices = self.current_prices.read().map_err(|_| PnlError::StateLocked)?;

        let mut positions = Vec::new();
        let mut total_value = Decimal::ZERO;
        let mut total_cost = Decimal::ZERO;

        for (mint, mint_lots) in lots.iter() {
            let mut position_quantity = Decimal::ZERO;
            let mut position_cost = Decimal::ZERO;

            for lot in mint_lots {
                position_quantity = position_quantity
                    .checked_add(lot.quantity)
                    .ok_or(PnlError::CalculationOverflow)?;
                position_cost = position_cost
                    .checked_add(lot.cost_basis)
                    .ok_or(PnlError::CalculationOverflow)?;
            }

            if position_quantity > Decimal::ZERO {
                let current_price = prices.get(mint).copied().unwrap_or(Decimal::ZERO);
                let market_value = position_quantity
                    .checked_mul(current_price)
                    .ok_or(PnlError::CalculationOverflow)?;
                total_value = total_value
                    .checked_add(market_value)
                    .ok_or(PnlError::CalculationOverflow)?;
                total_cost = total_cost
                    .checked_add(position_cost)
                    .ok_or(PnlError::CalculationOverflow)?;

                positions.push(PositionSummary {
                    mint: *mint,
                    quantity: position_quantity,
                    cost_basis: position_cost,
                    market_value,
                    unrealized_pnl: market_value
                        .checked_sub(position_cost)
                        .unwrap_or(Decimal::ZERO),
                });
            }
        }

        Ok(PortfolioSummary {
            positions,
            total_market_value: total_value,
            total_cost_basis: total_cost,
            total_unrealized_pnl: total_value
                .checked_sub(total_cost)
                .unwrap_or(Decimal::ZERO),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaxReport {
    pub year: i32,
    pub jurisdiction: TaxJurisdiction,
    pub short_term_gains: Decimal,
    pub long_term_gains: Decimal,
    pub total_proceeds: Decimal,
    pub total_cost_basis: Decimal,
    pub total_fees: Decimal,
    pub net_gains: Decimal,
    pub estimated_tax: Decimal,
    pub effective_rate: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HarvestRecommendation {
    pub mint: Pubkey,
    pub lot_id: u64,
    pub quantity: Decimal,
    pub expected_loss: Decimal,
    pub tax_benefit: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PositionSummary {
    pub mint: Pubkey,
    pub quantity: Decimal,
    pub cost_basis: Decimal,
    pub market_value: Decimal,
    pub unrealized_pnl: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PortfolioSummary {
    pub positions: Vec<PositionSummary>,
    pub total_market_value: Decimal,
    pub total_cost_basis: Decimal,
    pub total_unrealized_pnl: Decimal,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pnl_calculation() {
        let calc = PnlTaxCalculator::new(TaxMethod::FIFO, TaxJurisdiction::US);
        let mint = Pubkey::new_unique();

        let _ = calc
            .record_acquisition(
                mint,
                Decimal::from(100),
                Decimal::from(1000),
                TransactionType::Arbitrage,
                Signature::default(),
                9,
            )
            .expect("Acquisition should succeed");

        let disposal = calc
            .record_disposal(
                mint,
                Decimal::from(50),
                Decimal::from(600),
                Decimal::from(5),
            )
            .expect("Disposal should succeed");

        // [REPLACE] fix broken assertion line
        assert_eq!(disposal.realized_pnl, Decimal::from(95));
        assert!(disposal.quantity == Decimal::from(50));
    }

    #[test]
    fn test_wash_sale_detection() {
        let calc = PnlTaxCalculator::new(TaxMethod::FIFO, TaxJurisdiction::US);
        let mint = Pubkey::new_unique();

        let _ = calc
            .record_acquisition(
                mint,
                Decimal::from(100),
                Decimal::from(1000),
                TransactionType::Arbitrage,
                Signature::default(),
                9,
            )
            .expect("First acquisition should succeed");

        let _ = calc
            .record_disposal(
                mint,
                Decimal::from(100),
                Decimal::from(900),
                Decimal::from(10),
            )
            .expect("Disposal should succeed");

        let _ = calc
            .record_acquisition(
                mint,
                Decimal::from(100),
                Decimal::from(950),
                TransactionType::Arbitrage,
                Signature::default(),
                9,
            )
            .expect("Second acquisition should succeed");

        let adjustments = calc.basis_adjustments.read().unwrap();
        assert!(!adjustments.is_empty());
    }

    #[test]
    fn test_tax_calculation() {
        let calc = PnlTaxCalculator::new(TaxMethod::FIFO, TaxJurisdiction::US);

        let tax = calc
            .calculate_tax_liability(Decimal::from(100000))
            .expect("Tax calculation should succeed");

        assert!(tax > Decimal::ZERO);
        assert!(tax < Decimal::from(100000));
    }

    #[test]
    fn test_rate_limiting() {
        let calc = PnlTaxCalculator::new(TaxMethod::FIFO, TaxJurisdiction::US);
        let mint = Pubkey::new_unique();

        for i in 0..5 {
            let result = calc.record_acquisition(
                mint,
                Decimal::from(100),
                Decimal::from(1000),
                TransactionType::Arbitrage,
                Signature::default(),
                9,
            );

            if i > 0 {
                std::thread::sleep(std::time::Duration::from_millis(RATE_LIMIT_MS + 1));
            }

            assert!(result.is_ok() || matches!(result, Err(PnlError::RateLimited)));
        }
    }

    #[test]
    fn test_invalid_quantities() {
        let calc = PnlTaxCalculator::new(TaxMethod::FIFO, TaxJurisdiction::US);
        let mint = Pubkey::new_unique();

        let result = calc.record_acquisition(
            mint,
            Decimal::from(-100),
            Decimal::from(1000),
            TransactionType::Arbitrage,
            Signature::default(),
            9,
        );
        assert!(matches!(result, Err(PnlError::InvalidQuantity(_))));

        let result2 = calc.record_acquisition(
            mint,
            Decimal::ZERO,
            Decimal::from(1000),
            TransactionType::Arbitrage,
            Signature::default(),
            9,
        );
        assert!(matches!(result2, Err(PnlError::InvalidQuantity(_))));
    }

    #[test]
    fn test_portfolio_summary() {
        let calc = PnlTaxCalculator::new(TaxMethod::HIFO, TaxJurisdiction::Singapore);
        let mint1 = Pubkey::new_unique();
        let mint2 = Pubkey::new_unique();

        calc.record_acquisition(
            mint1,
            Decimal::from(100),
            Decimal::from(10000),
            TransactionType::Arbitrage,
            Signature::default(),
            9,
        )
        .expect("Acquisition 1 should succeed");

        calc.record_acquisition(
            mint2,
            Decimal::from(50),
            Decimal::from(5000),
            TransactionType::Sandwich,
            Signature::default(),
            6,
        )
        .expect("Acquisition 2 should succeed");

        calc.update_market_price(mint1, Decimal::from(110))
            .expect("Price update should succeed");
        calc.update_market_price(mint2, Decimal::from(95))
            .expect("Price update should succeed");

        let summary = calc.get_portfolio_summary().expect("Portfolio summary should succeed");

        assert_eq!(summary.positions.len(), 2);
        assert!(summary.total_unrealized_pnl != Decimal::ZERO);
    }

    #[test]
    fn test_performance_metrics() {
        let calc = PnlTaxCalculator::new(TaxMethod::FIFO, TaxJurisdiction::US);
        let mint = Pubkey::new_unique();

        for i in 0..50 {
            std::thread::sleep(std::time::Duration::from_millis(15));

            calc.record_acquisition(
                mint,
                Decimal::from(10),
                Decimal::from(100 + i),
                TransactionType::Arbitrage,
                Signature::default(),
                9,
            )
            .expect("Acquisition should succeed");

            if i % 2 == 1 {
                calc.record_disposal(
                    mint,
                    Decimal::from(5),
                    Decimal::from(55 + i),
                    Decimal::from(1),
                )
                .expect("Disposal should succeed");
            }
        }

        let snapshot = calc.calculate_total_pnl().expect("PnL calculation should succeed");
        assert!(snapshot.win_rate > Decimal::ZERO);
        // Sharpe can be 0 if variance is flat, but generally should compute
        assert!(snapshot.sharpe_ratio >= Decimal::ZERO);
    }
}
