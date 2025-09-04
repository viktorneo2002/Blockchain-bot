Clean imports (replace top of your file with this block)
// ----------------- CLEANED IMPORTS -----------------
use anchor_client::{Client, Cluster, Program};
use anchor_lang::prelude::*;
use drift::controller::position::PositionDirection;
use drift::instructions::{OrderParams, OrderType, MarketType, OrderTriggerCondition, PostOnlyParam};
use drift::math::constants::{
    PRICE_PRECISION, PRICE_PRECISION_I64, QUOTE_PRECISION, QUOTE_PRECISION_I64,
    BASE_PRECISION, BASE_PRECISION_I64, FUNDING_RATE_PRECISION, FUNDING_RATE_PRECISION_I128,
    PEG_PRECISION, AMM_RESERVE_PRECISION, SPOT_BALANCE_PRECISION, SPOT_BALANCE_PRECISION_U64,
};
// Use canonical drift funding function only
use drift::math::funding::calculate_funding_rate_long_short;
use drift::math::oracle::{oracle_price_data_to_price, OraclePriceData};
use drift::state::oracle::OracleSource;
use drift::state::perp_market::{PerpMarket, MarketStatus, AMM};
use drift::state::spot_market::{SpotMarket, SpotBalanceType};
use drift::state::state::State;
use drift::state::user::{User, UserStats, PerpPosition, SpotPosition, MarketPosition};
use pyth_sdk_solana::state::PriceAccount;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
    signer::keypair::read_keypair_file,
};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::{interval, sleep, timeout};
use thiserror::Error;

// Local utils (you'll add these files)
use crate::utils::precision;
use crate::utils::market_metrics::PerpMarketMetrics;
use crate::core::funding_adjust::compute_adjusted_funding_rates;
use crate::core::confidence::calculate_confidence_score;
// ----------------------------------------------------


✅ Notes: I removed use crate::math::funding::calculate_funding_rate_long_short (duplicate) — use Drift canonical version. If you actually own a local calculate_funding_payment_in_quote_precision() in crate::math::funding, keep it but alias its import to avoid name collisions (e.g. use crate::math::funding::calculate_funding_payment_in_quote_precision as local_calc_funding_payment;).

B — src/utils/precision.rs (drop-in)
// src/utils/precision.rs
//! Minimal, safe fixed-point helpers for production use.

pub const SAFE_MIN_I128: i128 = i128::MIN / 4;
pub const SAFE_MAX_I128: i128 = i128::MAX / 4;

#[inline]
pub fn f64_to_i128_clamped(v: f64, min: i128, max: i128) -> i128 {
    if !v.is_finite() {
        return 0;
    }
    if v >= 0.0 {
        let vi = v.trunc();
        if vi > (max as f64) { return max; }
        return vi as i128;
    } else {
        let vi = v.trunc();
        if vi < (min as f64) { return min; }
        return vi as i128;
    }
}

#[inline]
pub fn i128_mul_div(a: i128, b: i128, denom: i128) -> i128 {
    if denom == 0 { return 0; }
    a.saturating_mul(b) / denom
}

C — src/utils/market_metrics.rs (drop-in)
// src/utils/market_metrics.rs
use std::collections::VecDeque;
use std::time::Instant;
use drift::state::perp_market::PerpMarket;

pub struct PerpMarketMetrics {
    pub market: PerpMarket,
    pub recent_marks: VecDeque<u64>,
    pub last_update: Instant,
    pub max_history: usize,
}

impl PerpMarketMetrics {
    pub fn new(market: PerpMarket, history: usize) -> Self {
        Self {
            market,
            recent_marks: VecDeque::with_capacity(history),
            last_update: Instant::now(),
            max_history: history,
        }
    }

    pub fn push_mark(&mut self, mark_price: u64) {
        if self.recent_marks.len() == self.max_history {
            self.recent_marks.pop_front();
        }
        self.recent_marks.push_back(mark_price);
        self.last_update = Instant::now();
    }

    pub fn std_dev_log_return(&self) -> f64 {
        let n = self.recent_marks.len();
        if n < 2 { return 0.0; }
        let mut returns = Vec::with_capacity(n-1);
        for i in 1..n {
            let prev = self.recent_marks[i-1] as f64;
            let cur = self.recent_marks[i] as f64;
            if prev <= 0.0 { continue; }
            returns.push((cur/prev).ln());
        }
        if returns.is_empty() { return 0.0; }
        let mean = returns.iter().sum::<f64>() / (returns.len() as f64);
        let var = returns.iter().map(|r| (r-mean).powi(2)).sum::<f64>() / (returns.len() as f64);
        var.sqrt()
    }

    pub fn spoof_score(&self, mark_price: u64) -> u8 {
        // small deterministic heuristic
        let n = self.recent_marks.len();
        if n < 3 { return 0; }
        let last = *self.recent_marks.back().unwrap() as f64;
        let prev = *self.recent_marks.get(n-2).unwrap() as f64;
        if prev <= 0.0 { return 0; }
        let last_ret = (last / prev).ln();
        let mut returns = Vec::new();
        for i in 1..n {
            let a = self.recent_marks[i-1] as f64;
            let b = self.recent_marks[i] as f64;
            if a <= 0.0 { continue; }
            returns.push((b/a).ln());
        }
        if returns.len() < 2 { return 0; }
        let mean = returns.iter().sum::<f64>() / (returns.len() as f64);
        let std = (returns.iter().map(|r| (r-mean).powi(2)).sum::<f64>() / (returns.len() as f64)).sqrt().max(1e-12);
        let z = (last_ret - mean) / std;
        (z.abs() * 10.0).min(100.0) as u8
    }

    pub fn total_notional_quote(&self, mark_price: u64) -> u128 {
        let oi = (self.market.amm.base_asset_amount_long.abs().max(self.market.amm.base_asset_amount_short.abs())) as u128;
        oi.saturating_mul(mark_price as u128)
    }
}

D — src/core/funding_adjust.rs (drop-in)
// src/core/funding_adjust.rs
use crate::utils::market_metrics::PerpMarketMetrics;
use crate::utils::precision::f64_to_i128_clamped;
use drift::math::funding::calculate_funding_rate_long_short;
use std::i128;

const VOLATILITY_SKEW_MAX: f64 = 0.20;
const MULT_MIN: f64 = 0.6;
const MULT_MAX: f64 = 2.0;

pub fn compute_adjusted_funding_rates(
    metrics: &PerpMarketMetrics,
    oracle_price: u64,
    mark_price: u64,
    ts: i64,
) -> Result<(i128, i128), String> {
    // call canonical drift funding function
    // signature used here: (market: &PerpMarket, oracle_price: i128, mark_price: u64, ts: i64)
    let (base_long, base_short, _settle_ts) = calculate_funding_rate_long_short(
        &metrics.market,
        oracle_price as i128,
        mark_price,
        ts,
    ).map_err(|e| format!("drift funding err: {:?}", e))?;

    let std_dev = metrics.std_dev_log_return().clamp(0.0, 1.0);
    let volatility_skew = (std_dev * 2.0).min(VOLATILITY_SKEW_MAX);

    let long_oi = metrics.market.amm.base_asset_amount_long.abs().max(1) as f64;
    let short_oi = metrics.market.amm.base_asset_amount_short.abs().max(1) as f64;
    let oi_ratio_ln = (long_oi / short_oi).ln().clamp(-0.75, 0.75);

    let oracle_diff_ratio = ((mark_price as f64 - oracle_price as f64).abs()) / oracle_price.max(1) as f64;
    let oracle_penalty = if oracle_diff_ratio > 0.01 { (1.0 - (oracle_diff_ratio * 2.0)).clamp(0.6, 1.0) } else { 1.0 };

    let fee_pool = metrics.market.amm.total_fee_minus_distributions.abs() as f64;
    let total_notional = metrics.total_notional_quote(mark_price) as f64 + 1.0;
    let fee_ratio = fee_pool / total_notional;
    let fee_guard = if fee_ratio < 0.01 { 0.75 } else { 1.0 };

    let spoof_score = metrics.spoof_score(mark_price) as f64 / 100.0;
    let entropy_penalty = if spoof_score > 0.25 { (1.0 - spoof_score.min(0.5)).clamp(0.5, 1.0) } else { 1.0 };

    let mut long_mult = 1.0 + volatility_skew + oi_ratio_ln;
    let mut short_mult = 1.0 + volatility_skew - oi_ratio_ln;
    long_mult = (long_mult * oracle_penalty * entropy_penalty * fee_guard).clamp(MULT_MIN, MULT_MAX);
    short_mult = (short_mult * oracle_penalty * entropy_penalty * fee_guard).clamp(MULT_MIN, MULT_MAX);

    let long_adj = (base_long as f64) * long_mult;
    let short_adj = (base_short as f64) * short_mult;

    let long_i128 = f64_to_i128_clamped(long_adj, i128::MIN/4, i128::MAX/4);
    let short_i128 = f64_to_i128_clamped(short_adj, i128::MIN/4, i128::MAX/4);

    Ok((long_i128, short_i128))
}

E — src/core/confidence.rs (drop-in)
// src/core/confidence.rs
pub fn calculate_confidence_score(
    oracle_confidence: u64,
    oracle_price: u64,
    open_interest: u128,
    total_fee: i128,
) -> f64 {
    if oracle_price == 0 { return 0.0; }
    let oracle_conf_ratio = (oracle_confidence as f64) / (oracle_price as f64);
    let conf_score = 1.0 - oracle_conf_ratio.powf(0.7).clamp(0.0, 0.6);

    let oi_score = ((open_interest as f64) / 1e15).min(1.0);
    let fee_score = ((total_fee.abs() as f64) / 1e9).min(1.0);

    (conf_score * 0.5 + oi_score * 0.35 + fee_score * 0.15).clamp(0.0, 1.0)
}

F — Replace the ADVANCED FUNDING block in find_arbitrage_opportunities() with the safe call

Replace the whole giant phantom-field advanced block with this:

// inside loop, after you have market_index and market_data
// use PerpMarketMetrics wrapper (create or look up in cache)
let mut metrics = PerpMarketMetrics::new(market_data.market.clone(), 64);
metrics.push_mark(market_data.mark_price);

// compute adjusted funding rates (safe canonical + bounded modifiers)
let (funding_rate_long_adj, funding_rate_short_adj) = match compute_adjusted_funding_rates(
    &metrics,
    market_data.oracle_price,
    market_data.mark_price,
    self.get_current_timestamp()?,
) {
    Ok((l,s)) => (l, s),
    Err(e) => {
        log::warn!("Funding adjust failed for market {}: {}", market_index, e);
        continue;
    }
};

// pick direction based on adjusted long rate sign
let direction = if funding_rate_long_adj > 0 {
    PositionDirection::Short
} else {
    PositionDirection::Long
};

// choose funding_rate used for payment calc (signed)
let funding_rate_used = if direction == PositionDirection::Long { funding_rate_short_adj } else { funding_rate_long_adj };

// compute max size and cap with on-chain limits
let max_base_size = self.calculate_max_position_size(&market_data.market, market_data.mark_price)?;
let size = max_base_size.min((MAX_POSITION_SIZE_USDC as u128 * PRICE_PRECISION as u128 / market_data.mark_price as u128) as u64);

// expected funding payment in quote precision (use your crate function if available)
let raw_funding_payment = calculate_funding_payment_in_quote_precision(funding_rate_used, size as i128)
    .map_err(|e| ArbitrageError::MathError(format!("funding payment calc failed: {:?}", e)))?;

// apply volatility/liquidity/fee multipliers (bounded)
let volatility_mult = (metrics.std_dev_log_return().powi(2) * 1.0 + 1.0).clamp(1.0, 3.0);
let liquidity = metrics.liquidity_depth() as f64;
let liquidity_mult = if liquidity < 1_000_000.0 { 1.15 + (1_000_000.0 - liquidity) / 10_000_000.0 } else { 1.0 };
let fee_pool_ratio = metrics.market.amm.total_fee_minus_distributions.abs() as f64 / (metrics.total_notional_quote(market_data.mark_price) as f64 + 1.0);
let fee_mult = if fee_pool_ratio < 0.01 { 0.8 } else { 1.0 };
let funding_buffer_multiplier = 1.0 - (FUNDING_RATE_BUFFER as f64 / PRICE_PRECISION as f64);
let funding_modifier = (volatility_mult * liquidity_mult * fee_mult * funding_buffer_multiplier).clamp(0.5, 3.0);

let expected_funding_payment = ((raw_funding_payment as f64) * funding_modifier).round() as i128;

let confidence_score = calculate_confidence_score(
    market_data.oracle_confidence,
    market_data.oracle_price,
    market_data.open_interest,
    market_data.market.amm.total_fee_minus_distributions,
);

if expected_funding_payment.abs() as u128 > (MIN_PROFIT_THRESHOLD_USDC as u128 * QUOTE_PRECISION as u128) && confidence_score > 0.7 {
    opportunities.push(ArbitrageOpportunity {
        market_index,
        direction,
        size,
        expected_funding_payment,
        entry_price: market_data.mark_price,
        funding_rate: funding_rate_used,
        confidence_score,
    });
}
