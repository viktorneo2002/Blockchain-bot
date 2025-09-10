// === [IMPROVED: PIECE v0++++++ QUANT] ===
#![deny(unsafe_code)]
#![deny(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented
)]
#![allow(clippy::too_many_arguments, clippy::large_enum_variant)]
#![cfg_attr(not(test), deny(dead_code))]
#![deny(unreachable_patterns)]
#![deny(unused_must_use)]
#![deny(unconditional_recursion)]
#![deny(unused_qualifications)]
// Only fail builds on missing docs when explicitly asked (CI/docs builds)
#![cfg_attr(feature = "strict-docs", deny(missing_docs))]
// Optional: enable no_std only if you deliberately set `--features nostd` and your deps allow it.
#![cfg_attr(all(feature = "nostd", target_arch = "bpf"), no_std)]

// cold error handler for BPF to keep hot paths slim (no cost off-bpf)
#[cfg(all(target_arch = "bpf", feature = "alloc_error_handler"))]
#[alloc_error_handler]
fn oom(_: core::alloc::Layout) -> ! {
    core::arch::asm!("trap")
}

// ===============================
// === [IMPROVED: PIECE E102–E110 v++ HFT³²] ===
// ===============================
impl AMM {
    // Grid/context shims
    #[inline(always)]
    pub fn grid_info(&self) -> GridInfo { GridInfo { step: self.grid_step() } }

    #[inline(always)]
    pub fn salt64_from_oracle(&self) -> u64 { u64_from_pubkey_le(&self.oracle) }

    #[inline(always)]
    pub fn splitmix64_u64(x: u64) -> u64 {
        // Standard splitmix64 (unsigned form)
        let mut z = x.wrapping_add(0x9E3779B97F4A7C15);
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
        z ^ (z >> 31)
    }

    #[inline(always)]
    pub fn snap_with_gi_phase(&self, px: u64, gi: &GridInfo, slot: u64) -> u64 {
        let phase = (Self::splitmix64_u64(slot ^ self.salt64_from_oracle()) & 1) as u64;
        gi.snap_phase(px, phase)
    }

    #[inline(always)]
    pub fn price_frame_phase_plus_at(&self, slot: u64) -> DriftResult<PriceFramePhasePlus> {
        let gi = self.grid_info();
        let step = gi.step;
        let reserve = self.reserve_price()?;
        let bid = self.bid_price(reserve)?;
        let ask = self.ask_price(reserve)?;
        let spread_ticks = gi.ticks_between(bid, ask).unsigned_abs() as u64;
        Ok(PriceFramePhasePlus { bid, ask, step, reserve, spread_ticks })
    }

    // E102: asymmetric tick rate-limit
    #[inline(always)]
    pub fn limit_tick_move_asym(
        &self,
        from_px: u64,
        to_px: u64,
        side: PositionDirection,
        gi: &GridInfo,
        max_toward_ticks: u64,
        max_away_ticks: u64,
    ) -> u64 {
        if from_px == to_px { return to_px; }
        let ticks = gi.ticks_between(from_px, to_px);
        let toward_cross = match side {
            PositionDirection::Long  => ticks > 0,
            PositionDirection::Short => ticks < 0,
        };
        let cap = if toward_cross { max_toward_ticks } else { max_away_ticks };
        if cap == 0 { return from_px; }
        let abs = ticks.unsigned_abs() as u64;
        if abs <= cap { return to_px; }
        let dir = if ticks >= 0 { 1i64 } else { -1i64 };
        gi.offset(from_px, (cap as i64) * dir)
    }

    // E103: auto levels and spacing
    #[inline(always)]
    pub fn auto_levels_and_spacing(&self, ctx: &AMMContextPlus) -> (u8, u8) {
        let vol  = self.oracle_std.max(self.mark_std).max(1);
        let twap = self.historical_oracle_data.last_oracle_price_twap.unsigned_abs().max(1);
        let mut sigma_bps = (vol.saturating_mul(10_000)).saturating_div(twap);
        if sigma_bps > 5_000 { sigma_bps = 5_000; }
        let sp = ctx.pf.spread_ticks.max(1);
        let spacing = core::cmp::max(1u64, core::cmp::min(sp / 3 + (sigma_bps / 100) as u64, 8u64)) as u8;
        let levels = if sigma_bps <= 25 { 4 } else if sigma_bps <= 75 { 3 } else { 2 };
        (levels, spacing)
    }

    // E104: quote-age gating
    #[inline(always)]
    pub fn min_change_ticks_by_age(
        &self,
        now_ts: i64,
        last_emit_ts: i64,
        base_min_ticks: u64,
        age_lo: i64,
        age_hi: i64,
    ) -> u64 {
        let age = now_ts.saturating_sub(last_emit_ts).max(0);
        if age >= age_hi { return 0; }
        if age <= age_lo { return base_min_ticks.saturating_mul(2); }
        base_min_ticks
    }

    // E106: context++ with phase bit
    #[inline(always)]
    pub fn build_context_plus2(&self) -> DriftResult<AMMContextPlus2> {
        let slot = anchor_lang::prelude::Clock::get()?.slot;
        let gi   = self.grid_info();
        let pf   = self.price_frame_phase_plus_at(slot)?;
        let s64  = self.salt64_from_oracle();
        let phase = (Self::splitmix64_u64(slot ^ s64) & 1) as u64;
        Ok(AMMContextPlus2 {
            slot, gi, pf,
            price_salt_bit:  s64 & 1,
            size_salt_bit:  (s64 >> 1) & 1,
            phase_salt_bit: phase,
        })
    }

    // E107: fused offset inside using ctx2
    #[inline(always)]
    pub fn offset_inside_from_ctx2(
        &self,
        side: PositionDirection,
        px: u64,
        ticks: i64,
        ctx: &AMMContextPlus2,
    ) -> u64 {
        let off = ctx.gi.offset(px, ticks);
        match side {
            PositionDirection::Long  => off.min(ctx.pf.ask.saturating_sub(ctx.pf.step)).max(ctx.pf.bid),
            PositionDirection::Short => off.max(ctx.pf.bid.saturating_add(ctx.pf.step)).min(ctx.pf.ask),
        }
    }

    // Lightweight helpers required by E109/E110
    #[inline(always)]
    pub fn jitter_ticks_adaptive_with_slot(&self, slot: u64, _oracle_price: i64, cap: u8) -> i64 {
        if cap == 0 { return 0; }
        let seed = Self::splitmix64_u64(slot ^ self.salt64_from_oracle());
        let r = (seed & 0xFFFF) as i64; // 0..65535
        let span = (cap as i64).min(2) as i64; // ≤2 as per params
        (r % (2*span + 1)) - span // in [-span..span]
    }

    #[inline(always)]
    pub fn inventory_skew_ticks(&self, _ctx: &AMMContextPlus, cap: i64) -> i64 {
        let inv = self.base_asset_amount_with_unsettled_lp;
        if inv == 0 { return 0; }
        let sqrtk = self.get_lower_bound_sqrt_k().unwrap_or(0);
        if sqrtk == 0 { return 0; }
        // Coarse mapping: 1 tick if |inv| > sqrtk/200, 2 if > sqrtk/100
        let th1 = (sqrtk / 200) as i128;
        let th2 = (sqrtk / 100) as i128;
        let mag = if inv.unsigned_abs() as i128 > th2 { 2 } else if inv.unsigned_abs() as i128 > th1 { 1 } else { 0 };
        let mag = (mag as i64).min(cap.abs());
        if inv > 0 { -mag } else { mag }
    }

    #[inline(always)]
    pub fn spread_bps_vs_oracle(&self, reserve_price_i: i64) -> DriftResult<i64> {
        let reserve_u = reserve_price_i.unsigned_abs().max(1);
        let oracle_u  = self.historical_oracle_data.last_oracle_price.unsigned_abs().max(1);
        let bps = bps_diff_u64(reserve_u, oracle_u) as i64;
        Ok(bps)
    }

    // E103/E108 dependents
    #[inline(always)]
    pub fn derive_quote_params(&self, ctx: &AMMContextPlus) -> QuoteParams {
        let (_, spacing0) = self.auto_levels_and_spacing(ctx);
        let rp = ctx.pf.reserve.max(1) as i64;
        let div_bps = self.spread_bps_vs_oracle(rp).unwrap_or(0);
        let stressed = div_bps > 150 || self.last_oracle_conf_pct > 200;
        if stressed {
            QuoteParams { levels: 1, spacing_ticks: core::cmp::max(1, spacing0 / 2), jitter_cap: 0, min_change_ticks: 1, toward_cap: 1, away_cap: 2 }
        } else {
            QuoteParams { levels: 3, spacing_ticks: spacing0.max(1), jitter_cap: 2, min_change_ticks: 1, toward_cap: 1, away_cap: 3 }
        }
    }

    // E105: pair sanitizer
    #[inline(always)]
    pub fn sanitize_pair_at_slot(&self, slot: u64, mut bid: u64, mut ask: u64) -> DriftResult<MakerPair> {
        let gi = self.grid_info();
        let pf = self.price_frame_phase_plus_at(slot)?;
        bid = bid.max(pf.bid).min(pf.ask.saturating_sub(pf.step));
        ask = ask.min(pf.ask).max(pf.bid.saturating_add(pf.step));
        if ask <= bid { ask = bid.saturating_add(pf.step); }
        bid = self.snap_with_gi_phase(bid, &gi, slot);
        ask = self.snap_with_gi_phase(ask, &gi, slot);
        Ok(MakerPair { bid, ask })
    }

    // E109: build pair using ctx2 and params
    #[inline(always)]
    pub fn build_pair_center_mode_ctx2(
        &self,
        ctx2: &AMMContextPlus2,
        center_target: u64,
        params: &QuoteParams,
        oracle_price: i64,
        prev_pair_opt: Option<MakerPair>,
    ) -> DriftResult<MakerPair> {
        let gi = ctx2.gi;
        let pf = ctx2.pf;
        let slot = ctx2.slot;

        let rb = gi.offset(center_target, -(params.spacing_ticks as i64));
        let ra = gi.offset(center_target,   params.spacing_ticks as i64);
        let mut bid = self.offset_inside_from_ctx2(PositionDirection::Long,  rb, 0, ctx2);
        let mut ask = self.offset_inside_from_ctx2(PositionDirection::Short, ra, 0, ctx2);

        let skew = self.inventory_skew_ticks(
            &AMMContextPlus { slot, gi, pf, price_salt_bit: ctx2.price_salt_bit, size_salt_bit: ctx2.size_salt_bit },
            2,
        );
        if skew != 0 { bid = self.offset_inside_from_ctx2(PositionDirection::Long,  bid, -skew, ctx2);
                        ask = self.offset_inside_from_ctx2(PositionDirection::Short, ask,  skew, ctx2); }

        if params.jitter_cap > 0 {
            let jb = self.jitter_ticks_adaptive_with_slot(slot, oracle_price, params.jitter_cap);
            let ja = self.jitter_ticks_adaptive_with_slot(slot ^ 0xACEDFACE, oracle_price, params.jitter_cap);
            if jb != 0 { bid = self.offset_inside_from_ctx2(PositionDirection::Long,  bid, jb, ctx2); }
            if ja != 0 { ask = self.offset_inside_from_ctx2(PositionDirection::Short, ask, ja, ctx2); }
        }

        if let Some(prev) = prev_pair_opt {
            bid = self.limit_tick_move_asym(prev.bid, bid, PositionDirection::Long,  &gi, params.toward_cap, params.away_cap);
            ask = self.limit_tick_move_asym(prev.ask, ask, PositionDirection::Short, &gi, params.toward_cap, params.away_cap);
        }

        bid = self.snap_with_gi_phase(bid, &gi, slot);
        ask = self.snap_with_gi_phase(ask, &gi, slot);
        if ask <= bid { ask = bid.saturating_add(pf.step); }

        Ok(MakerPair { bid, ask })
    }

    // E110: ladder builder with mode & dedup
    #[inline(always)]
    pub fn build_levels_mode_ctx2(
        &self,
        ctx2: &AMMContextPlus2,
        side: PositionDirection,
        center_target: u64,
        params: &QuoteParams,
        oracle_price: i64,
        prev_levels_opt: Option<MakerLevels>,
    ) -> DriftResult<MakerLevels> {
        let gi = ctx2.gi;
        let pf = ctx2.pf;
        let slot = ctx2.slot;
        let mut out = MakerLevels { n: 0, px: [0; 4] };
        let mut prev_in_build: Option<u64> = None;
        let max_n = core::cmp::min(4, params.levels as usize);
        for i in 0..max_n {
            let off = (i as i64) * (params.spacing_ticks as i64);
            let base = match side {
                PositionDirection::Long  => gi.offset(center_target, -off),
                PositionDirection::Short => gi.offset(center_target,  off),
            };
            let mut px = self.offset_inside_from_ctx2(side, base, 0, ctx2);
            if params.jitter_cap > 0 {
                let jt = self.jitter_ticks_adaptive_with_slot(slot ^ ((i as u64) * 0x9E37_79B9), oracle_price, params.jitter_cap);
                if jt != 0 { px = self.offset_inside_from_ctx2(side, px, jt, ctx2); }
            }
            if let Some(prev) = prev_levels_opt {
                if (prev.n as usize) > i {
                    px = self.limit_tick_move_asym(prev.px[i], px, side, &gi, params.toward_cap, params.away_cap);
                }
            }
            px = self.snap_with_gi_phase(px, &gi, slot);
            if let Some(p) = prev_in_build {
                if side == PositionDirection::Long {
                    if px >= p { px = p.saturating_sub(pf.step).max(pf.bid); }
                } else {
                    if px <= p { px = p.saturating_add(pf.step).min(pf.ask); }
                }
            }
            if let Some(p) = prev_in_build { if px == p { continue; } }
            out.px[out.n as usize] = px; out.n += 1; prev_in_build = Some(px);
        }
        Ok(out)
    }
}

// ===== Grid and quoting primitives (minimal, deterministic) =====
#[derive(Clone, Copy, Debug)]
pub struct GridInfo { pub step: u64 }

impl GridInfo {
    #[inline(always)]
    pub fn ticks_between(&self, from_px: u64, to_px: u64) -> i64 {
        let s = self.step.max(1);
        if to_px >= from_px { ((to_px - from_px) / s) as i64 } else { -(((from_px - to_px) / s) as i64) }
    }
    #[inline(always)]
    pub fn offset(&self, px: u64, ticks: i64) -> u64 {
        let s = self.step.max(1);
        if ticks >= 0 { px.saturating_add((ticks as u64).saturating_mul(s)) }
        else { px.saturating_sub((ticks.unsigned_abs()).saturating_mul(s)) }
    }
    /// Phase-aware banker’s snap: on exact half-steps, prefer up if phase_bit==1 else banker’s
    #[inline(always)]
    pub fn snap_phase(&self, px: u64, phase_bit: u64) -> u64 {
        let s = self.step.max(1);
        let rem = px % s;
        let half = s / 2;
        if rem == half && (s & 1) == 0 { // exact half
            if phase_bit & 1 == 1 { px.saturating_add(s - rem) } else { px - rem }
        } else {
            quantize_bankers_u64(px, s)
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PriceFramePhasePlus { pub bid: u64, pub ask: u64, pub step: u64, pub reserve: u64, pub spread_ticks: u64 }

#[derive(Clone, Copy, Debug)]
pub struct AMMContextPlus { pub slot: u64, pub gi: GridInfo, pub pf: PriceFramePhasePlus, pub price_salt_bit: u64, pub size_salt_bit: u64 }

#[derive(Clone, Copy, Debug)]
pub struct MakerPair { pub bid: u64, pub ask: u64 }

#[derive(Clone, Copy, Debug)]
pub struct MakerLevels { pub n: u8, pub px: [u64; 4] }

#[derive(Clone, Copy)]
pub struct AMMContextPlus2 {
    pub slot: u64,
    pub gi:   GridInfo,
    pub pf:   PriceFramePhasePlus,
    pub price_salt_bit: u64,
    pub size_salt_bit:  u64,
    pub phase_salt_bit: u64,
}

#[derive(Clone, Copy)]
pub struct QuoteParams {
    pub levels: u8,
    pub spacing_ticks: u8,
    pub jitter_cap: u8,
    pub min_change_ticks: u64,
    pub toward_cap: u64,
    pub away_cap: u64,
}

use anchor_lang::prelude::*;
use borsh::{BorshDeserialize, BorshSerialize};
use core::convert::TryFrom;
use static_assertions::{const_assert, const_assert_eq};

use crate::{
    controller::position::{PositionDelta, PositionDirection},
    error::{DriftResult, ErrorCode},
    math::{
        amm, stats,
        casting::Cast,
        constants::*,
        helpers::get_proportion_i128,
        margin::{
            calculate_size_discount_asset_weight, calculate_size_premium_liability_weight,
            MarginRequirementType,
        },
        safe_math::SafeMath,
    },
    state::{
        events::OrderActionExplanation,
        oracle::{
            get_prelaunch_price, get_sb_on_demand_price, get_switchboard_price, HistoricalOracleData,
            OracleSource,
        },
        paused_operations::PerpOperation,
        pyth_lazer_oracle::PythLazerOracle,
        spot_market::{AssetTier, SpotBalance, SpotBalanceType},
        state::State,
        traits::{MarketIndexOffset, Size},
    },
};

use super::{oracle_map::OracleIdentifier, protected_maker_mode_config::ProtectedMakerParams};
use std::cmp::max;

#[cfg(test)]
mod tests;

// === [IMPROVED: PIECE v1++++++ QUANT] ===
#[derive(Clone, Copy, BorshSerialize, BorshDeserialize, PartialEq, Debug, Eq, Default)]
#[repr(u8)]
pub enum MarketStatus {
    #[default] Initialized = 0,
    Active = 1,
    ReduceOnly = 2,
    Settlement = 3,
    Delisted = 4,
    // Deprecated
    FundingPaused = 5,
    AmmPaused = 6,
    FillPaused = 7,
    WithdrawPaused = 8,
}

// === [IMPROVED: PIECE S37 v++ HFT¹¹] === Salt & cheap helpers
#[inline(always)]
fn abs_diff_u64(a: u64, b: u64) -> u64 { if a >= b { a - b } else { b - a } }

#[inline(always)]
fn u64_from_pubkey_le(pk: &Pubkey) -> u64 {
    let b = pk.to_bytes();
    u64::from_le_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}

impl MarketStatus {
    #[inline(always)]
    pub fn validate_not_deprecated(&self) -> DriftResult {
        if matches!(
            self,
            Self::FundingPaused | Self::AmmPaused | Self::FillPaused | Self::WithdrawPaused
        ) {
            return Err(ErrorCode::DefaultError);
        }
        Ok(())
    }

    #[inline(always)]
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active)
    }
}

impl TryFrom<u8> for MarketStatus {
    type Error = ErrorCode;
    #[inline(always)]
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        use MarketStatus::*;
        Ok(match v {
            0 => Initialized, 1 => Active, 2 => ReduceOnly, 3 => Settlement, 4 => Delisted,
            5 => FundingPaused, 6 => AmmPaused, 7 => FillPaused, 8 => WithdrawPaused,
            _ => return Err(ErrorCode::DefaultError),
        })
    }
}

impl From<MarketStatus> for u8 { #[inline(always)] fn from(x: MarketStatus) -> u8 { x as u8 } }

// === [IMPROVED: PIECE v3++++++ QUANT] ===
// Compile-time invariants for struct layout
const_assert_eq!(core::mem::size_of::<PerpMarket>(), 1216);
const_assert_eq!(core::mem::align_of::<PerpMarket>(), 8);
const_assert_eq!(PerpMarket::MARKET_INDEX_OFFSET, 1160);

// === [IMPROVED: PIECE S9 v++ HFT²] ===
// === [IMPROVED: PIECE S9 v++ HFT²] ===
const_assert!(36 >= 12); // we only use first 12 bytes of padding for meta

// === [IMPROVED: PIECE S25 v++ HFT⁷] === Enum size ABI locks
const_assert_eq!(core::mem::size_of::<MarketStatus>(), 1);
const_assert_eq!(core::mem::size_of::<ContractType>(), 1);
const_assert_eq!(core::mem::size_of::<ContractTier>(), 1);

// === [IMPROVED: PIECE P27 v++ HFT²¹] === Additional ABI locks
const_assert_eq!(core::mem::size_of::<InsuranceClaim>(), 40);
const_assert_eq!(core::mem::align_of::<InsuranceClaim>(), 8);
const_assert_eq!(core::mem::size_of::<PoolBalance>(), 24);
const_assert_eq!(core::mem::align_of::<PoolBalance>(), 8);

// === [IMPROVED: PIECE S3 v++ HFT³] === Layout/version canary using existing padding
pub const PERP_MARKET_LAYOUT_VERSION: u16 = 3;
pub const PERP_MARKET_LAYOUT_CANARY:  u32 = 0xA1B2C3D4;
pub const PERP_MARKET_MIN_SDK:        u16 = 1;
pub const LAYOUT_FLAG_PRICE_GUARDS:    u32 = 1 << 0;
pub const LAYOUT_FLAG_ABI_STRICT_ENUM: u32 = 1 << 1;

// ===============================
// === [IMPROVED: PIECE v1+++++ OMEGA] ===
// Deterministic Reserve Price Guard
// ===============================
impl PerpMarket {
    #[inline(always)]
    pub fn normalized_reserve_price(&self, slot: u64) -> DriftResult<u64> {
        let reserve = self.amm.reserve_price()?;
        let oracle_i = self.amm.historical_oracle_data.last_oracle_price;
        let oracle_u = oracle_i.unsigned_abs();

        // Recency / confidence
        let slots_since = slot.saturating_sub(self.amm.last_update_slot).min(5);
        let recency_boost: u64 = if slots_since == 0 { 3 } else { 1 };
        let max_mult = self.get_max_confidence_interval_multiplier()?;
        let conf_thr = (PERCENTAGE_PRECISION_U64 / 50).safe_mul(max_mult)?; // 2% * tier
        let conf_ok = self.amm.last_oracle_conf_pct <= conf_thr;

        // === [IMPROVED: PIECE S16 v++ HFT⁵] === Fast path
        if slots_since == 0 && conf_ok {
            let max_p = reserve.max(oracle_u).max(1);
            let spread_pct_fp = (reserve.max(oracle_u) - reserve.min(oracle_u))
                .safe_mul(PERCENTAGE_PRECISION_U64)?
                .safe_div(max_p)?;
            if spread_pct_fp <= (PERCENTAGE_PRECISION_U64 / 10_000) { // 1bp
                let twap_abs = self
                    .amm
                    .historical_oracle_data
                    .last_oracle_price_twap
                    .unsigned_abs();
                let twap_tick = quantize_bankers_u64(twap_abs.max(1), self.amm.grid_step());
                return Ok(twap_tick);
            }
        }

        // Tick-aware spread penalty
        let penalty_w = spread_penalty_weight(reserve, oracle_u, self.amm.grid_step())?;

        // Weights
        let oracle_w  = if conf_ok { ((recency_boost as u64) << 1).saturating_add(1) } else { 1 };
        let reserve_w = if conf_ok { 2 } else { 3 }.saturating_mul(penalty_w);
        let denom = reserve_w.safe_add(oracle_w)?;

        // Blend
        let mut blended = reserve
            .safe_mul(reserve_w)?
            .safe_add(oracle_u.safe_mul(oracle_w)?)?
            .safe_div(denom)?;

        // Tier rails vs TWAP
        let twap_i = self.amm.historical_oracle_data.last_oracle_price_twap;
        let twap_u = twap_i.unsigned_abs();
        let max_div = self.get_max_price_divergence_for_funding_rate(twap_i)?.unsigned_abs();
        blended = clamp_u64(blended, twap_u.saturating_sub(max_div), twap_u.saturating_add(max_div));

        // Snap-to-twap if within 5 bps
        let dead = twap_u.safe_div(2000)?;
        let diff = if blended > twap_u { blended - twap_u } else { twap_u - blended };
        if diff <= dead { blended = twap_u; }

        Ok(blended)
    }

    // === [IMPROVED: PIECE S36C v++ HFT¹¹] === Elastic funding depth on grid
    #[inline(always)]
    pub fn funding_rate_depth_elastic(&self) -> DriftResult<u64> {
        let mut base = self.funding_rate_depth()?;
        let inv = self.amm.base_asset_amount_with_unsettled_lp;
        if inv == 0 { return Ok(base); }

        let sqrtk = self.amm.get_lower_bound_sqrt_k()?;
        if sqrtk == 0 { return Ok(base); }

        let stress = ((inv.unsigned_abs() as u128)
            .safe_mul(10_000u128)? // bps
            .safe_div(sqrtk as u128)?)
            .min(2_500u128) as u64; // cap 25%

        let reduce = pct_mul_u64_bankers(base, stress, 10_000)?;
        base = base.saturating_sub(reduce).max(1);

        Ok(quantize_bankers_u64(base, self.amm.grid_step()).max(1))
    }

    // === [IMPROVED: PIECE P25 v++ HFT²¹] === Slot-aware maker params
    #[inline(always)]
    pub fn salt_jitter_ticks_scaled(&self, slot: u64, scale: u64) -> i64 {
        // Deterministic tiny jitter in [-scale, 0, +scale]
        let h = self.quant_salt() ^ slot;
        let sign = if (h & 1) == 0 { -1i64 } else { 1i64 };
        let mag = ((h >> 1) & 1) as i64; // 0 or 1
        sign.saturating_mul(mag.saturating_mul(scale as i64))
    }

    #[inline(always)]
    pub fn get_protected_maker_params_at_slot(
        &self,
        slot: u64,
    ) -> DriftResult<ProtectedMakerParams> {
        let step = self.amm.grid_step();

        let dyn_raw = if self.protected_maker_dynamic_divisor > 0 {
            self.amm.oracle_std.max(self.amm.mark_std) / self.protected_maker_dynamic_divisor as u64
        } else {
            0
        };

        let mut dyn_ticks = if step > 1 { (dyn_raw + step - 1) / step } else { dyn_raw };
        dyn_ticks = dyn_ticks.min(10_000);

        let vol = self.amm.oracle_std.max(self.amm.mark_std).max(1);
        let twap = self
            .amm
            .historical_oracle_data
            .last_oracle_price_twap
            .unsigned_abs()
            .max(1);
        let vol_bps = (vol.saturating_mul(10_000)).saturating_div(twap).max(1);
        let tier_mul: u64 = match self.contract_tier {
            ContractTier::A | ContractTier::B => 1,
            ContractTier::C => 2,
            _ => 3,
        };
        let deadband_ticks = ((vol_bps / 25).max(1)).saturating_mul(tier_mul).clamp(1, 12);

        if dyn_ticks <= deadband_ticks {
            dyn_ticks = 0;
        }

        if dyn_ticks > 0 {
            dyn_ticks = dyn_ticks.max(1);
            let j = self.salt_jitter_ticks_scaled(slot, 2);
            let jt = if j >= 0 { j as u64 } else { j.unsigned_abs() as u64 };
            dyn_ticks = dyn_ticks.saturating_add(jt).max(1);
        }

        let dynamic_offset = dyn_ticks.saturating_mul(step);
        Ok(ProtectedMakerParams {
            limit_price_divisor: self.protected_maker_limit_price_divisor,
            dynamic_offset,
            tick_size: step,
        })
    }

    #[inline(always)]
    pub fn get_protected_maker_params(&self) -> DriftResult<ProtectedMakerParams> {
        Ok(self.get_protected_maker_params_at_slot(anchor_lang::prelude::Clock::get()?.slot)?)
    }

    // === [IMPROVED: PIECE P25 v++ HFT²¹] === Slot-aware auction skip (thin wrapper)
    #[inline(always)]
    pub fn can_skip_auction_duration_at_slot(
        &self,
        state: &State,
        amm_lp_allowed_to_jit_make: bool,
        _slot: u64,
    ) -> DriftResult<bool> {
        // Reuse existing logic; current implementation does not need Clock::get
        self.can_skip_auction_duration(state, amm_lp_allowed_to_jit_make)
    }
}

impl PerpMarket {
    // === [IMPROVED: PIECE S37 v++ HFT¹¹] === salt accessors
    #[inline(always)]
    pub fn quant_salt(&self) -> u64 { u64_from_pubkey_le(&self.pubkey) ^ ((self.market_index as u64) << 17) }
    #[inline(always)]
    pub fn quant_salt_bit(&self) -> u64 { self.quant_salt() & 1 }
}

// === [IMPROVED: PIECE S41 v++ HFT¹¹] === Tick-aware bounded spread penalty
#[inline(always)]
fn spread_penalty_weight(reserve: u64, oracle: u64, tick: u64) -> DriftResult<u64> {
    let spread = abs_diff_u64(reserve, oracle);
    // effective spread in ticks (rounded up)
    let t = tick.max(1);
    let ticks = if spread == 0 { 0 } else { (spread + t - 1) / t } as u64;
    // map ticks→penalty in [1 .. 1 + 2%] with quick taper: 0.5% per tick up to 4 ticks
    let half_pct = PERCENTAGE_PRECISION_U64 / 200; // 0.5%
    let w_pct = (ticks.min(4)).safe_mul(half_pct)?;
    // Convert percentage to multiplicative weight ~ (1 + w_pct)
    // Scale via ppm to keep integer math simple
    let inc_ppm = pct_mul_u64_floor(1_000_000, w_pct, PERCENTAGE_PRECISION_U64)?;
    Ok(1 + inc_ppm / 1_000_000)
}

impl PerpMarket {
    /// True if oracle delay is within acceptable bounds for this tier/source.
    #[inline(always)]
    pub fn oracle_fresh_enough(&self) -> bool {
        let base = match self.contract_tier {
            ContractTier::A | ContractTier::B => 1u64,
            ContractTier::C => 2u64,
            _ => 4u64,
        };
        let d = self.amm.historical_oracle_data.last_oracle_delay as u64;
        d <= base
    }

    /// Hint for weighting oracle contributions in blends; 2 if fresh, else 1.
    #[inline(always)]
    pub fn oracle_weight_hint(&self) -> u64 { if self.oracle_fresh_enough() { 2 } else { 1 } }

    // Layout meta helpers using padding[0..12]
    #[inline(always)]
    pub fn write_layout_meta(&mut self) {
        let v = PERP_MARKET_LAYOUT_VERSION.to_le_bytes();
        let c = PERP_MARKET_LAYOUT_CANARY.to_le_bytes();
        let s = PERP_MARKET_MIN_SDK.to_le_bytes();
        let f = (LAYOUT_FLAG_PRICE_GUARDS | LAYOUT_FLAG_ABI_STRICT_ENUM).to_le_bytes();
        self.padding[0] = v[0]; self.padding[1] = v[1];
        self.padding[2] = c[0]; self.padding[3] = c[1]; self.padding[4] = c[2]; self.padding[5] = c[3];
        self.padding[6] = s[0]; self.padding[7] = s[1];
        self.padding[8] = f[0]; self.padding[9] = f[1]; self.padding[10] = f[2]; self.padding[11] = f[3];
    }

    #[inline(always)]
    pub fn assert_layout_meta(&self) -> DriftResult {
        let p = &self.padding;
        let ver = u16::from_le_bytes([p[0], p[1]]);
        let can = u32::from_le_bytes([p[2], p[3], p[4], p[5]]);
        if ver != PERP_MARKET_LAYOUT_VERSION || can != PERP_MARKET_LAYOUT_CANARY {
            return Err(ErrorCode::DefaultError);
        }
        Ok(())
    }
}

// ===============================
// === [IMPROVED: PIECE v2+++++ OMEGA] ===
// Dynamic Liquidation Cap
// ===============================
impl PerpMarket {
    #[inline(always)]
    pub fn dynamic_liquidation_cap(&self, maybe_vol_est: Option<u64>) -> DriftResult<u32> {
        let base_ceiling = self.get_max_liquidation_fee()?;
        let base_fee = self.get_base_liquidator_fee(false);

        // Use provided vol or fallback
        let vol_est = maybe_vol_est.unwrap_or(self.amm.mark_std.max(self.amm.oracle_std).max(1));

        // Tier uplift
        let tier_mult = match self.contract_tier {
            ContractTier::A | ContractTier::B => 1,
            ContractTier::C => 2,
            _ => 3,
        };

        // Volatility bands
        let mut cap = match vol_est {
            x if x < PRICE_PRECISION / 100 => base_ceiling / 2,
            x if x < PRICE_PRECISION / 25 => base_ceiling.saturating_add(base_ceiling / 10),
            x if x < PRICE_PRECISION / 10 => base_ceiling.saturating_add(base_ceiling / 5),
            _ => base_ceiling.saturating_add(base_ceiling / 2),
        };

        cap = cap.saturating_mul(tier_mult);

        // Clamp
        cap = cap.clamp(base_fee, base_ceiling);
        Ok(cap)
    }
}

// ===============================
// === [IMPROVED: PIECE v3+++++ OMEGA] ===
// Margin Ratio Guard
// ===============================
impl PerpMarket {
    #[inline(always)]
    pub fn get_margin_ratio_guarded(
        &self,
        size: u128,
        margin_type: MarginRequirementType,
        user_high_leverage_mode: bool,
        slot: u64,
    ) -> DriftResult<u32> {
        let mut mr = self.get_margin_ratio(size, margin_type, user_high_leverage_mode)?;

        // Confidence hysteresis
        let max_mult = self.get_max_confidence_interval_multiplier()?;
        let allowed = (PERCENTAGE_PRECISION_U64 / 50).safe_mul(max_mult)?;
        let conf = self.amm.last_oracle_conf_pct;
        if conf > allowed.saturating_add(allowed / 10) {
            let excess_bps: u32 = ((conf - allowed)
                .saturating_mul(10_000)
                .safe_div(allowed.max(1))?)
                .min(2000)
                .cast()?;
            mr = mr.saturating_add(mr.saturating_mul(excess_bps) / 10_000);
        }

        // Slot jitter bump
        if (slot & 7) == 0 {
            mr = mr.saturating_add(mr / 20);
        }

        // OI crowding tax
        let oi = self.get_open_interest();
        let sqrtk = self.amm.get_lower_bound_sqrt_k()?;
        if sqrtk > 0 {
            let ratio = (oi as u128).saturating_mul(1000).safe_div(sqrtk)?;
            mr = mr.saturating_add(match ratio {
                0..=250 => 0,
                251..=600 => mr / 20,
                _ => mr / 7,
            });
        }

        // Sigmoid penalty
        let k = (self.amm.min_order_size as u128).safe_mul(512)?;
        if size > 0 {
            let num = (size.saturating_mul(150u128)).safe_mul(10_000u128)?;
            let den = size.safe_add(k)?;
            let tax_bps_u128 = num.safe_div(den)?;
            let tax_bps: u32 = (tax_bps_u128 / 10_000u128).min(150).cast()?;
            mr = mr.saturating_add(mr.saturating_mul(tax_bps) / 10_000);
        }

        // Toxic flow & volume imbalance
        if self.amm.short_intensity_count > self.amm.long_intensity_count.saturating_mul(2) {
            mr = mr.saturating_add(mr / 10);
        }
        if self.amm.long_intensity_volume > self.amm.short_intensity_volume.saturating_mul(2) {
            mr = mr.saturating_add(mr / 20);
        }

        Ok(mr)
    }
}

// ===============================
// === [IMPROVED: PIECE v4+++++ OMEGA] ===
// Structural Drawdown Guard
// ===============================
impl PerpMarket {
    #[inline(always)]
    pub fn has_structural_drawdown(&self, ema_drawdown_pct_i128: i128) -> DriftResult<bool> {
        let raw = self.has_too_much_drawdown()?;
        let ema_ok = ema_drawdown_pct_i128 < -PERCENTAGE_PRECISION_I128 / 30;

        let base = self.amm.total_fee_minus_distributions.abs().max(1);
        let funding_ratio = (self.amm.net_unsettled_funding_pnl as i128)
            .safe_mul(PERCENTAGE_PRECISION_I128)?
            .safe_div(base as i128)?;
        let funding_bad = funding_ratio < -PERCENTAGE_PRECISION_I128 / 50;

        let intensity_ok = (self.amm.volume_24h > 0)
            || (self.amm.long_intensity_count > 3)
            || (self.amm.short_intensity_count > 3);

        let depth_ok = self.get_open_interest() > self.amm.min_order_size.safe_mul(1000)?;
        let extra_cushion_ok = funding_ratio < -PERCENTAGE_PRECISION_I128 / 200;

        let pnl_skew_ok = self.amm.base_asset_amount_long.abs()
            > self.amm.base_asset_amount_short.abs().saturating_mul(2);

        Ok(raw && ema_ok && funding_bad && intensity_ok && extra_cushion_ok && !depth_ok && pnl_skew_ok)
    }
}

// ===============================
// === [IMPROVED: PIECE v5+++++ OMEGA] ===
// Funding Rate Depth Estimator
// ===============================
impl PerpMarket {
    #[inline(always)]
    pub fn funding_rate_depth(&self) -> DriftResult<u64> {
        let oi = self.get_open_interest().max(1);
        let sqrt_oi = isqrt_u128(oi as u128);
        let sqrtk = self.amm.get_lower_bound_sqrt_k()?;
        let step = self.amm.grid_step();

        let base = sqrt_oi.min(sqrtk).max(self.amm.min_order_size.cast()?);
        let depth_u64: u64 = base.cast()?;
        let depth = quantize_bankers_u64(depth_u64, step);

        let min_depth = self.amm.min_order_size.safe_mul(200)?;
        let max_depth = self.amm.min_order_size.safe_mul(10_000)?;
        Ok(depth.clamp(min_depth, max_depth))
    }
}

#[derive(Clone, Copy, BorshSerialize, BorshDeserialize, PartialEq, Debug, Eq, Default)]
#[repr(u8)]
pub enum ContractType {
    #[default] Perpetual = 0,
    Future = 1,
    Prediction = 2,
}

// === [IMPROVED: PIECE v2++++++ QUANT] ===
#[derive(
    Clone, Copy, BorshSerialize, BorshDeserialize, PartialEq, Debug, Eq, PartialOrd, Ord, Default,
)]
#[repr(u8)]
pub enum ContractTier {
    A = 0,
    B = 1,
    C = 2,
    Speculative = 3,
    #[default]
    HighlySpeculative = 4,
    Isolated = 5,
}

#[inline(always)]
const fn b8(b: bool) -> u8 { if b { 1 } else { 0 } }

/// Branchless clamp for u64.
#[inline(always)]
const fn clamp_u64(x: u64, lo: u64, hi: u64) -> u64 { if x < lo { lo } else if x > hi { hi } else { x } }

/// Branchless clamp for i64.
#[inline(always)]
const fn clamp_i64(x: i64, lo: i64, hi: i64) -> i64 { if x < lo { lo } else if x > hi { hi } else { x } }

/// Slot-based throttler: true once every 2^log2_stride slots.
#[inline(always)]
fn should_log_this_slot(slot: u64, log2_stride: u8) -> bool { (slot & ((1u64 << log2_stride) - 1)) == 0 }

/// Fast integer square root for u128 (few Newton steps suffice for price/oi ranges).
#[inline(always)]
fn isqrt_u128(n: u128) -> u128 {
    if n == 0 { return 0; }
    let mut x = 1u128 << ((127 - n.leading_zeros() as u128) / 2);
    x = (x + n / x) >> 1;
    x = (x + n / x) >> 1;
    let y = n / x;
    if x < y { x } else { y }
}

/// Safe power-of-10 for i128 with bound to avoid overflow.
#[inline(always)]
fn pow10_i128_safe(exp: u32) -> DriftResult<i128> {
    if exp > 38 { return Err(ErrorCode::DefaultError); }
    Ok(10_i128.pow(exp))
}

// Feature-gated tracing: free in prod, enable via `--features trace` locally/CI.
#[cfg(feature = "trace")]
macro_rules! trace_log { ($($arg:tt)*) => { msg!($($arg)*); }; }
#[cfg(not(feature = "trace"))]
macro_rules! trace_log { ($($arg:tt)*) => {}; }

/// Quantize value to nearest multiple of `tick` (>=1). Rounds halves up.
#[inline(always)]
fn quantize_nearest_u64(val: u64, tick: u64) -> u64 {
    let t = tick.max(1);
    let rem = val % t;
    if rem.saturating_mul(2) >= t { val.saturating_add(t - rem) } else { val - rem }
}

// === [IMPROVED: PIECE P30 v++ HFT²¹] === sigma/Δ helpers
#[inline(always)]
fn bps_diff_u64(a: u64, b: u64) -> u64 {
    let hi = a.max(b).max(1);
    let lo = a.min(b);
    (hi - lo).saturating_mul(10_000).saturating_div(hi)
}

#[inline(always)]
fn sigma_gate_ok(sigma: u64, scale_price: u64, std_div: u64) -> bool {
    sigma < (scale_price / std_div.max(1))
}

#[inline(always)]
fn sigma_to_bps(sigma: u64, scale_price: u64) -> u64 {
    (sigma.saturating_mul(10_000)).saturating_div(scale_price.max(1)).max(1)
}

// === [IMPROVED: PIECE S22 v++ HFT⁷] === Precise rounding/mul-div helpers
#[inline(always)]
fn gcd_u128(mut a: u128, mut b: u128) -> u128 {
    if a == 0 { return b; }
    if b == 0 { return a; }
    let az = a.trailing_zeros();
    let bz = b.trailing_zeros();
    let shift = az.min(bz);
    a >>= az; b >>= bz;
    loop {
        if a > b { core::mem::swap(&mut a, &mut b); }
        b -= a;
        if b == 0 { break; }
        b >>= b.trailing_zeros();
    }
    a << shift
}

#[inline(always)]
fn mul_div_u128_floor(a: u128, b: u128, den: u128) -> DriftResult<u128> {
    if den == 0 { return Err(ErrorCode::DefaultError); }
    let g1 = gcd_u128(a, den); let a = a / g1; let den = den / g1;
    let g2 = gcd_u128(b, den); let b = b / g2; let den = den / g2;
    Ok(a.safe_mul(b)?.safe_div(den)?)
}

#[inline(always)]
fn mul_div_u128_ceil(a: u128, b: u128, den: u128) -> DriftResult<u128> {
    if den == 0 { return Err(ErrorCode::DefaultError); }
    let q = mul_div_u128_floor(a, b, den)?;
    let num = mul_div_u128_floor(a, b, 1u128)?; // exact a*b
    Ok(if num == q.safe_mul(den)? { q } else { q.saturating_add(1) })
}

#[inline(always)]
fn mul_div_u128_bankers(a: u128, b: u128, den: u128) -> DriftResult<u128> {
    if den == 0 { return Err(ErrorCode::DefaultError); }
    let num = mul_div_u128_floor(a, b, 1u128)?; // exact a*b
    let q = num / den;
    let r = num - q * den;
    if r * 2 < den { Ok(q) }
    else if r * 2 > den { Ok(q + 1) }
    else { Ok(q + (q & 1)) } // ties to even
}

#[inline(always)]
fn pct_mul_u64_floor(x: u64, pct: u64, pct_prec: u64) -> DriftResult<u64> {
    Ok(mul_div_u128_floor(x as u128, pct as u128, pct_prec as u128)?.min(u128::from(u64::MAX)) as u64)
}
#[inline(always)]
fn pct_mul_u64_ceil(x: u64, pct: u64, pct_prec: u64) -> DriftResult<u64> {
    Ok(mul_div_u128_ceil(x as u128, pct as u128, pct_prec as u128)?.min(u128::from(u64::MAX)) as u64)
}
#[inline(always)]
fn pct_mul_u64_bankers(x: u64, pct: u64, pct_prec: u64) -> DriftResult<u64> {
    Ok(mul_div_u128_bankers(x as u128, pct as u128, pct_prec as u128)?.min(u128::from(u64::MAX)) as u64)
}

/// Quantize using banker's rounding to the nearest multiple of `step`.
#[inline(always)]
fn quantize_bankers_u64(val: u64, step: u64) -> u64 {
    let s = step.max(1);
    let rem = val % s;
    let half = s / 2;
    if rem < half { val - rem }
    else if rem > half { val.saturating_add(s - rem) }
    else { // tie: to even multiple
        let down = val - rem;
        if (down / s) % 2 == 0 { down } else { val.saturating_add(s - rem) }
    }
}

#[derive(Clone, Copy, BorshSerialize, BorshDeserialize, PartialEq, Debug, Eq, PartialOrd, Ord)]
pub enum AMMLiquiditySplit {
    ProtocolOwned,
    LPOwned,
    Shared,
}

impl AMMLiquiditySplit {
    pub fn get_order_action_explanation(&self) -> OrderActionExplanation {
        match &self {
            AMMLiquiditySplit::ProtocolOwned => OrderActionExplanation::OrderFilledWithAMMJit,
            AMMLiquiditySplit::LPOwned => OrderActionExplanation::OrderFilledWithLPJit,
            AMMLiquiditySplit::Shared => OrderActionExplanation::OrderFilledWithAMMJitLPSplit,
        }
    }
}

#[derive(Clone, Copy, BorshSerialize, BorshDeserialize, PartialEq, Debug, Eq, PartialOrd, Ord)]
pub enum AMMAvailability {
    Immediate,
    AfterMinDuration,
    Unavailable,
}

#[allow(unsafe_code)]
#[account(zero_copy(unsafe))]
#[derive(Eq, PartialEq, Debug)]
#[repr(C)]
pub struct PerpMarket {
    /// The perp market's address. It is a pda of the market index
    pub pubkey: Pubkey,
    /// The automated market maker
    pub amm: AMM,
    /// The market's pnl pool. When users settle negative pnl, the balance increases.
    /// When users settle positive pnl, the balance decreases. Can not go negative.
    pub pnl_pool: PoolBalance,
    /// Encoded display name for the perp market e.g. SOL-PERP
    pub name: [u8; 32],
    /// The perp market's claim on the insurance fund
    pub insurance_claim: InsuranceClaim,
    /// The max pnl imbalance before positive pnl asset weight is discounted
    /// pnl imbalance is the difference between long and short pnl. When it's greater than 0,
    /// the amm has negative pnl and the initial asset weight for positive pnl is discounted
    /// precision = QUOTE_PRECISION
    pub unrealized_pnl_max_imbalance: u64,
    /// The ts when the market will be expired. Only set if market is in reduce only mode
    pub expiry_ts: i64,
    /// The price at which positions will be settled. Only set if market is expired
    /// precision = PRICE_PRECISION
    pub expiry_price: i64,
    /// Every trade has a fill record id. This is the next id to be used
    pub next_fill_record_id: u64,
    /// Every funding rate update has a record id. This is the next id to be used
    pub next_funding_rate_record_id: u64,
    /// Every amm k updated has a record id. This is the next id to be used
    pub next_curve_record_id: u64,
    /// The initial margin fraction factor. Used to increase margin ratio for large positions
    /// precision: MARGIN_PRECISION
    pub imf_factor: u32,
    /// The imf factor for unrealized pnl. Used to discount asset weight for large positive pnl
    /// precision: MARGIN_PRECISION
    pub unrealized_pnl_imf_factor: u32,
    /// The fee the liquidator is paid for taking over perp position
    /// precision: LIQUIDATOR_FEE_PRECISION
    pub liquidator_fee: u32,
    /// The fee the insurance fund receives from liquidation
    /// precision: LIQUIDATOR_FEE_PRECISION
    pub if_liquidation_fee: u32,
    /// The margin ratio which determines how much collateral is required to open a position
    /// e.g. margin ratio of .1 means a user must have $100 of total collateral to open a $1000 position
    /// precision: MARGIN_PRECISION
    pub margin_ratio_initial: u32,
    /// The margin ratio which determines when a user will be liquidated
    /// e.g. margin ratio of .05 means a user must have $50 of total collateral to maintain a $1000 position
    /// else they will be liquidated
    /// precision: MARGIN_PRECISION
    pub margin_ratio_maintenance: u32,
    /// The initial asset weight for positive pnl. Negative pnl always has an asset weight of 1
    /// precision: SPOT_WEIGHT_PRECISION
    pub unrealized_pnl_initial_asset_weight: u32,
    /// The maintenance asset weight for positive pnl. Negative pnl always has an asset weight of 1
    /// precision: SPOT_WEIGHT_PRECISION
    pub unrealized_pnl_maintenance_asset_weight: u32,
    /// number of users in a position (base)
    pub number_of_users_with_base: u32,
    /// number of users in a position (pnl) or pnl (quote)
    pub number_of_users: u32,
    pub market_index: u16,
    /// Whether a market is active, reduce only, expired, etc
    /// Affects whether users can open/close positions
    pub status: MarketStatus,
    /// Currently only Perpetual markets are supported
    pub contract_type: ContractType,
    /// The contract tier determines how much insurance a market can receive, with more speculative markets receiving less insurance
    /// It also influences the order perp markets can be liquidated, with less speculative markets being liquidated first
    pub contract_tier: ContractTier,
    pub paused_operations: u8,
    /// The spot market that pnl is settled in
    pub quote_spot_market_index: u16,
    /// Between -100 and 100, represents what % to increase/decrease the fee by
    /// E.g. if this is -50 and the fee is 5bps, the new fee will be 2.5bps
    /// if this is 50 and the fee is 5bps, the new fee will be 7.5bps
    pub fee_adjustment: i16,
    /// fuel multiplier for perp funding
    /// precision: 10
    pub fuel_boost_position: u8,
    /// fuel multiplier for perp taker
    /// precision: 10
    pub fuel_boost_taker: u8,
    /// fuel multiplier for perp maker
    /// precision: 10
    pub fuel_boost_maker: u8,
    pub pool_id: u8,
    pub high_leverage_margin_ratio_initial: u16,
    pub high_leverage_margin_ratio_maintenance: u16,
    pub protected_maker_limit_price_divisor: u8,
    pub protected_maker_dynamic_divisor: u8,
    pub padding: [u8; 36],
}

impl Default for PerpMarket {
    fn default() -> Self {
        PerpMarket {
            pubkey: Pubkey::default(),
            amm: AMM::default(),
            pnl_pool: PoolBalance::default(),
            name: [0; 32],
            insurance_claim: InsuranceClaim::default(),
            unrealized_pnl_max_imbalance: 0,
            expiry_ts: 0,
            expiry_price: 0,
            next_fill_record_id: 0,
            next_funding_rate_record_id: 0,
            next_curve_record_id: 0,
            imf_factor: 0,
            unrealized_pnl_imf_factor: 0,
            liquidator_fee: 0,
            if_liquidation_fee: 0,
            margin_ratio_initial: 0,
            margin_ratio_maintenance: 0,
            unrealized_pnl_initial_asset_weight: 0,
            unrealized_pnl_maintenance_asset_weight: 0,
            number_of_users_with_base: 0,
            number_of_users: 0,
            market_index: 0,
            status: MarketStatus::default(),
            contract_type: ContractType::default(),
            contract_tier: ContractTier::default(),
            paused_operations: 0,
            quote_spot_market_index: 0,
            fee_adjustment: 0,
            fuel_boost_position: 0,
            fuel_boost_taker: 0,
            fuel_boost_maker: 0,
            pool_id: 0,
            high_leverage_margin_ratio_initial: 0,
            high_leverage_margin_ratio_maintenance: 0,
            protected_maker_limit_price_divisor: 0,
            protected_maker_dynamic_divisor: 0,
            padding: [0; 36],
        }
    }
}

impl Size for PerpMarket {
    const SIZE: usize = 1216;
}

impl MarketIndexOffset for PerpMarket {
    const MARKET_INDEX_OFFSET: usize = 1160;
}

impl PerpMarket {
    pub fn oracle_id(&self) -> OracleIdentifier {
        (self.amm.oracle, self.amm.oracle_source)
    }

    #[must_use]
    pub fn is_in_settlement(&self, now: i64) -> bool {
        let in_settlement = matches!(
            self.status,
            MarketStatus::Settlement | MarketStatus::Delisted
        );
        let expired = self.expiry_ts != 0 && now >= self.expiry_ts;
        in_settlement || expired
    }

    #[must_use]
    pub fn is_reduce_only(&self) -> DriftResult<bool> {
        Ok(self.status == MarketStatus::ReduceOnly)
    }

    pub fn is_operation_paused(&self, operation: PerpOperation) -> bool {
        PerpOperation::is_operation_paused(self.paused_operations, operation)
    }

    #[must_use]
    pub fn can_skip_auction_duration(
        &self,
        state: &State,
        amm_lp_allowed_to_jit_make: bool,
    ) -> DriftResult<bool> {
        if state.amm_immediate_fill_paused()? {
            return Ok(false);
        }

        let amm_low_inventory_and_profitable = self.amm.net_revenue_since_last_funding
            >= DEFAULT_REVENUE_SINCE_LAST_FUNDING_SPREAD_RETREAT
            && amm_lp_allowed_to_jit_make;
        let amm_oracle_no_latency = self.amm.oracle_source == OracleSource::Prelaunch
            || (self.amm.historical_oracle_data.last_oracle_delay == 0
                && self.amm.oracle_source == OracleSource::PythLazer);
        let can_skip = amm_low_inventory_and_profitable || amm_oracle_no_latency;

        if can_skip {
            msg!("market {} amm skipping auction duration", self.market_index);
        }

        Ok(can_skip)
    }

    pub fn has_too_much_drawdown(&self) -> DriftResult<bool> {
        let quote_drawdown_limit_breached = match self.contract_tier {
            ContractTier::A | ContractTier::B => {
                self.amm.net_revenue_since_last_funding
                    <= DEFAULT_REVENUE_SINCE_LAST_FUNDING_SPREAD_RETREAT * 400
            }
            _ => {
                self.amm.net_revenue_since_last_funding
                    <= DEFAULT_REVENUE_SINCE_LAST_FUNDING_SPREAD_RETREAT * 200
            }
        };

        if quote_drawdown_limit_breached {
            let percent_drawdown = self
                .amm
                .net_revenue_since_last_funding
                .cast::<i128>()?
                .safe_mul(PERCENTAGE_PRECISION_I128)?
                .safe_div(self.amm.total_fee_minus_distributions.max(1))?;

            let percent_drawdown_limit_breached = match self.contract_tier {
                ContractTier::A => percent_drawdown <= -PERCENTAGE_PRECISION_I128 / 50,
                ContractTier::B => percent_drawdown <= -PERCENTAGE_PRECISION_I128 / 33,
                ContractTier::C => percent_drawdown <= -PERCENTAGE_PRECISION_I128 / 25,
                _ => percent_drawdown <= -PERCENTAGE_PRECISION_I128 / 20,
            };

            if percent_drawdown_limit_breached {
                msg!("AMM has too much on-the-hour drawdown (percentage={}, quote={}) to accept fills",
                percent_drawdown,
                self.amm.net_revenue_since_last_funding
            );
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub fn get_max_confidence_interval_multiplier(self) -> DriftResult<u64> {
        // assuming validity_guard_rails max confidence pct is 2%
        Ok(match self.contract_tier {
            ContractTier::A => 1,                  // 2%
            ContractTier::B => 1,                  // 2%
            ContractTier::C => 2,                  // 4%
            ContractTier::Speculative => 10,       // 20%
            ContractTier::HighlySpeculative => 50, // 100%
            ContractTier::Isolated => 50,          // 100%
        })
    }

    pub fn get_sanitize_clamp_denominator(self) -> DriftResult<Option<i64>> {
        Ok(match self.contract_tier {
            ContractTier::A => Some(10_i64),         // 10%
            ContractTier::B => Some(5_i64),          // 20%
            ContractTier::C => Some(2_i64),          // 50%
            ContractTier::Speculative => None, // DEFAULT_MAX_TWAP_UPDATE_PRICE_BAND_DENOMINATOR
            ContractTier::HighlySpeculative => None, // DEFAULT_MAX_TWAP_UPDATE_PRICE_BAND_DENOMINATOR
            ContractTier::Isolated => None, // DEFAULT_MAX_TWAP_UPDATE_PRICE_BAND_DENOMINATOR
        })
    }

    pub fn get_auction_end_min_max_divisors(self) -> DriftResult<(u64, u64)> {
        Ok(match self.contract_tier {
            ContractTier::A => (1000, 50),              // 10 bps, 2%
            ContractTier::B => (1000, 20),              // 10 bps, 5%
            ContractTier::C => (500, 20),               // 50 bps, 5%
            ContractTier::Speculative => (100, 10),     // 1%, 10%
            ContractTier::HighlySpeculative => (50, 5), // 2%, 20%
            ContractTier::Isolated => (50, 5),          // 2%, 20%
        })
    }

    pub fn get_max_price_divergence_for_funding_rate(
        self,
        oracle_price_twap: i64,
    ) -> DriftResult<i64> {
        // Delegate to ContractTier for a single-source divergence rail
        self.contract_tier.max_price_divergence_from_twap(oracle_price_twap)
    }

    pub fn is_high_leverage_mode_enabled(&self) -> bool {
        self.high_leverage_margin_ratio_initial > 0
            && self.high_leverage_margin_ratio_maintenance > 0
    }

    pub fn get_margin_ratio(
        &self,
        size: u128,
        margin_type: MarginRequirementType,
        user_high_leverage_mode: bool,
    ) -> DriftResult<u32> {
        if self.status == MarketStatus::Settlement {
            return Ok(0); // no liability weight on size
        }

        let (margin_ratio_initial, margin_ratio_maintenance) =
            if user_high_leverage_mode && self.is_high_leverage_mode_enabled() {
                (
                    self.high_leverage_margin_ratio_initial.cast::<u32>()?,
                    self.high_leverage_margin_ratio_maintenance.cast::<u32>()?,
                )
            } else {
                (self.margin_ratio_initial, self.margin_ratio_maintenance)
            };

        let default_margin_ratio = match margin_type {
            MarginRequirementType::Initial => margin_ratio_initial,
            MarginRequirementType::Fill => {
                margin_ratio_initial.safe_add(margin_ratio_maintenance)? / 2
            }
            MarginRequirementType::Maintenance => margin_ratio_maintenance,
        };

        let size_adj_margin_ratio = calculate_size_premium_liability_weight(
            size,
            self.imf_factor,
            default_margin_ratio,
            MARGIN_PRECISION_U128,
        )?;

        let margin_ratio = default_margin_ratio.max(size_adj_margin_ratio);

        Ok(margin_ratio)
    }

    pub fn get_base_liquidator_fee(&self, user_high_leverage_mode: bool) -> u32 {
        if user_high_leverage_mode && self.is_high_leverage_mode_enabled() {
            // min(liquidator_fee, .8 * high_leverage_margin_ratio_maintenance)
            let margin_ratio = (self.high_leverage_margin_ratio_maintenance as u32)
                .saturating_mul(LIQUIDATION_FEE_TO_MARGIN_PRECISION_RATIO);
            self.liquidator_fee
                .min(margin_ratio.saturating_sub(margin_ratio / 5))
        } else {
            self.liquidator_fee
        }
    }

    pub fn get_max_liquidation_fee(&self) -> DriftResult<u32> {
        let max_liquidation_fee = (self.liquidator_fee.safe_mul(MAX_LIQUIDATION_MULTIPLIER)?).min(
            self.margin_ratio_maintenance
                .safe_mul(LIQUIDATION_FEE_PRECISION / MARGIN_PRECISION)
                .unwrap_or(u32::MAX),
        );
        Ok(max_liquidation_fee)
    }

    pub fn get_unrealized_asset_weight(
        &self,
        unrealized_pnl: i128,
        margin_type: MarginRequirementType,
    ) -> DriftResult<u32> {
        let mut margin_asset_weight = match margin_type {
            MarginRequirementType::Initial | MarginRequirementType::Fill => {
                self.unrealized_pnl_initial_asset_weight
            }
            MarginRequirementType::Maintenance => self.unrealized_pnl_maintenance_asset_weight,
        };

        if margin_asset_weight > 0
            && matches!(
                margin_type,
                MarginRequirementType::Fill | MarginRequirementType::Initial
            )
            && self.unrealized_pnl_max_imbalance > 0
        {
            let net_unsettled_pnl = amm::calculate_net_user_pnl(
                &self.amm,
                self.amm.historical_oracle_data.last_oracle_price,
            )?;

            if net_unsettled_pnl > self.unrealized_pnl_max_imbalance.cast::<i128>()? {
                margin_asset_weight = margin_asset_weight
                    .cast::<u128>()?
                    .safe_mul(self.unrealized_pnl_max_imbalance.cast()?)?
                    .safe_div(net_unsettled_pnl.unsigned_abs())?
                    .cast()?;
            }
        }

        // the asset weight for a position's unrealized pnl + unsettled pnl in the margin system
        // > 0 (positive balance)
        // < 0 (negative balance) always has asset weight = 1
        let unrealized_asset_weight = if unrealized_pnl > 0 {
            // todo: only discount the initial margin s.t. no one gets liquidated over upnl?

            // a larger imf factor -> lower asset weight
            match margin_type {
                MarginRequirementType::Initial | MarginRequirementType::Fill => {
                    if margin_asset_weight > 0 {
                        calculate_size_discount_asset_weight(
                            unrealized_pnl
                                .unsigned_abs()
                                .safe_mul(AMM_TO_QUOTE_PRECISION_RATIO)?,
                            self.unrealized_pnl_imf_factor,
                            margin_asset_weight,
                        )?
                    } else {
                        0
                    }
                }
                MarginRequirementType::Maintenance => self.unrealized_pnl_maintenance_asset_weight,
            }
        } else {
            SPOT_WEIGHT_PRECISION
        };

        Ok(unrealized_asset_weight)
    }

    pub fn get_open_interest(&self) -> u128 {
        let l = self.amm.base_asset_amount_long;
        let s = self.amm.base_asset_amount_short;
        let m = if l.abs() >= s.abs() { l } else { s };
        m.unsigned_abs()
    }

    pub fn get_market_depth_for_funding_rate(&self) -> DriftResult<u64> {
        // base amount used on user orders for funding calculation

        let open_interest = self.get_open_interest();

        let depth = (open_interest.safe_div(1000)?.cast::<u64>()?).clamp(
            self.amm.min_order_size.safe_mul(100)?,
            self.amm.min_order_size.safe_mul(5000)?,
        );

        Ok(depth)
    }

    pub fn update_market_with_counterparty(
        &mut self,
        delta: &PositionDelta,
        new_settled_base_asset_amount: i64,
    ) -> DriftResult {
        // indicates that position delta is settling lp counterparty
        if delta.remainder_base_asset_amount.is_some() {
            // todo: name for this is confusing, but adding is correct as is
            // definition: net position of users in the market that has the LP as a counterparty (which have NOT settled)
            self.amm.base_asset_amount_with_unsettled_lp = self
                .amm
                .base_asset_amount_with_unsettled_lp
                .safe_add(new_settled_base_asset_amount.cast()?)?;

            self.amm.quote_asset_amount_with_unsettled_lp = self
                .amm
                .quote_asset_amount_with_unsettled_lp
                .safe_add(delta.quote_asset_amount.cast()?)?;
        }

        Ok(())
    }

    #[must_use]
    pub fn is_price_divergence_ok_for_settle_pnl(&self, oracle_price: i64) -> DriftResult<bool> {
        let oracle_divergence = oracle_price
            .safe_sub(self.amm.historical_oracle_data.last_oracle_price_twap_5min)?
            .safe_mul(PERCENTAGE_PRECISION_I64)?
            .safe_div(
                self.amm
                    .historical_oracle_data
                    .last_oracle_price_twap_5min
                    .min(oracle_price),
            )?
            .unsigned_abs();

        let oracle_divergence_limit = match self.contract_tier {
            ContractTier::A => PERCENTAGE_PRECISION_U64 / 200, // 50 bps
            ContractTier::B => PERCENTAGE_PRECISION_U64 / 200, // 50 bps
            ContractTier::C => PERCENTAGE_PRECISION_U64 / 100, // 100 bps
            ContractTier::Speculative => PERCENTAGE_PRECISION_U64 / 40, // 250 bps
            ContractTier::HighlySpeculative => PERCENTAGE_PRECISION_U64 / 40, // 250 bps
            ContractTier::Isolated => PERCENTAGE_PRECISION_U64 / 40, // 250 bps
        };

        if oracle_divergence >= oracle_divergence_limit {
            msg!(
                "market_index={} price divergence too large to safely settle pnl: {} >= {}",
                self.market_index,
                oracle_divergence,
                oracle_divergence_limit
            );
            return Ok(false);
        }

        let min_price =
            oracle_price.min(self.amm.historical_oracle_data.last_oracle_price_twap_5min);

        let std_limit = match self.contract_tier {
            ContractTier::A => min_price / 50,                 // 200 bps
            ContractTier::B => min_price / 50,                 // 200 bps
            ContractTier::C => min_price / 20,                 // 500 bps
            ContractTier::Speculative => min_price / 10,       // 1000 bps
            ContractTier::HighlySpeculative => min_price / 10, // 1000 bps
            ContractTier::Isolated => min_price / 10,          // 1000 bps
        }
        .unsigned_abs();

        if self.amm.oracle_std.max(self.amm.mark_std) >= std_limit {
            msg!(
                "market_index={} std too large to safely settle pnl: {} >= {}",
                self.market_index,
                self.amm.oracle_std.max(self.amm.mark_std),
                std_limit
            );
            return Ok(false);
        }

        Ok(true)
    }

    pub fn can_sanitize_market_order_auctions(&self) -> bool {
        self.amm.oracle_source != OracleSource::Prelaunch
    }

    pub fn is_prediction_market(&self) -> bool {
        self.contract_type == ContractType::Prediction
    }

    pub fn get_quote_asset_reserve_prediction_market_bounds(
        &self,
        direction: PositionDirection,
    ) -> DriftResult<(u128, u128)> {
        let mut quote_asset_reserve_lower_bound = 0_u128;

        //precision scaling: 1e6 -> 1e12 -> 1e6
        let peg_sqrt = (self
            .amm
            .peg_multiplier
            .safe_mul(PEG_PRECISION)?
            .saturating_add(1))
        .nth_root(2)
        .saturating_add(1);

        // $1 limit
        let mut quote_asset_reserve_upper_bound = self
            .amm
            .sqrt_k
            .safe_mul(peg_sqrt)?
            .safe_div(self.amm.peg_multiplier)?;

        // for price [0,1] maintain following invariants:
        if direction == PositionDirection::Long {
            // lowest ask price is $0.05
            quote_asset_reserve_lower_bound = self
                .amm
                .sqrt_k
                .safe_mul(22361)?
                .safe_mul(peg_sqrt)?
                .safe_div(100000)?
                .safe_div(self.amm.peg_multiplier)?
        } else {
            // highest bid price is $0.95
            quote_asset_reserve_upper_bound = self
                .amm
                .sqrt_k
                .safe_mul(97467)?
                .safe_mul(peg_sqrt)?
                .safe_div(100000)?
                .safe_div(self.amm.peg_multiplier)?
        }

        Ok((
            quote_asset_reserve_lower_bound,
            quote_asset_reserve_upper_bound,
        ))
    }

    pub fn get_protected_maker_params(&self) -> ProtectedMakerParams {
        let dynamic_offset = if self.protected_maker_dynamic_divisor > 0 {
            self.amm.oracle_std.max(self.amm.mark_std) / self.protected_maker_dynamic_divisor as u64
        } else {
            0
        };

        ProtectedMakerParams {
            limit_price_divisor: self.protected_maker_limit_price_divisor,
            dynamic_offset,
            tick_size: self.amm.order_tick_size,
        }
    }

    pub fn get_min_perp_auction_duration(&self, default_min_auction_duration: u8) -> u8 {
        if self.amm.taker_speed_bump_override != 0 {
            self.amm.taker_speed_bump_override.max(0).unsigned_abs()
        } else {
            default_min_auction_duration
        }
    }
}

#[cfg(test)]
impl PerpMarket {
    pub fn default_test() -> Self {
        let amm = AMM::default_test();
        PerpMarket {
            amm,
            margin_ratio_initial: 1000,
            margin_ratio_maintenance: 500,
            ..PerpMarket::default()
        }
    }

    pub fn default_btc_test() -> Self {
        let amm = AMM::default_btc_test();
        PerpMarket {
            amm,
            margin_ratio_initial: 1000,    // 10x
            margin_ratio_maintenance: 500, // 5x
            status: MarketStatus::Initialized,
            ..PerpMarket::default()
        }
    }
}

#[zero_copy(unsafe)]
#[derive(Default, Eq, PartialEq, Debug)]
#[repr(C)]
pub struct InsuranceClaim {
    /// The amount of revenue last settled
    /// Positive if funds left the perp market,
    /// negative if funds were pulled into the perp market
    /// precision: QUOTE_PRECISION
    pub revenue_withdraw_since_last_settle: i64,
    /// The max amount of revenue that can be withdrawn per period
    /// precision: QUOTE_PRECISION
    pub max_revenue_withdraw_per_period: u64,
    /// The max amount of insurance that perp market can use to resolve bankruptcy and pnl deficits
    /// precision: QUOTE_PRECISION
    pub quote_max_insurance: u64,
    /// The amount of insurance that has been used to resolve bankruptcy and pnl deficits
    /// precision: QUOTE_PRECISION
    pub quote_settled_insurance: u64,
    /// The last time revenue was settled in/out of market
    pub last_revenue_withdraw_ts: i64,
}

#[zero_copy(unsafe)]
#[derive(Default, Eq, PartialEq, Debug)]
#[repr(C)]
pub struct PoolBalance {
    /// To get the pool's token amount, you must multiply the scaled balance by the market's cumulative
    /// deposit interest
    /// precision: SPOT_BALANCE_PRECISION
    pub scaled_balance: u128,
    /// The spot market the pool is for
    pub market_index: u16,
    pub padding: [u8; 6],
}

impl SpotBalance for PoolBalance {
    fn market_index(&self) -> u16 {
        self.market_index
    }

    fn balance_type(&self) -> &SpotBalanceType {
        &SpotBalanceType::Deposit
    }

    fn balance(&self) -> u128 {
        self.scaled_balance
    }

    fn increase_balance(&mut self, delta: u128) -> DriftResult {
        self.scaled_balance = self.scaled_balance.safe_add(delta)?;
        Ok(())
    }

    fn decrease_balance(&mut self, delta: u128) -> DriftResult {
        self.scaled_balance = self.scaled_balance.safe_sub(delta)?;
        Ok(())
    }

    fn update_balance_type(&mut self, _balance_type: SpotBalanceType) -> DriftResult {
        Err(ErrorCode::CantUpdatePoolBalanceType)
    }
}

#[assert_no_slop]
#[zero_copy(unsafe)]
#[derive(Debug, PartialEq, Eq)]
#[repr(C)]
pub struct AMM {
    /// oracle price data public key
    pub oracle: Pubkey,
    /// stores historically witnessed oracle data
    pub historical_oracle_data: HistoricalOracleData,
    /// accumulated base asset amount since inception per lp share
    /// precision: QUOTE_PRECISION
    pub base_asset_amount_per_lp: i128,
    /// accumulated quote asset amount since inception per lp share
    /// precision: QUOTE_PRECISION
    pub quote_asset_amount_per_lp: i128,
    /// partition of fees from perp market trading moved from pnl settlements
    pub fee_pool: PoolBalance,
    /// `x` reserves for constant product mm formula (x * y = k)
    /// precision: AMM_RESERVE_PRECISION
    pub base_asset_reserve: u128,
    /// `y` reserves for constant product mm formula (x * y = k)
    /// precision: AMM_RESERVE_PRECISION
    pub quote_asset_reserve: u128,
    /// determines how close the min/max base asset reserve sit vs base reserves
    /// allow for decreasing slippage without increasing liquidity and v.v.
    /// precision: PERCENTAGE_PRECISION
    pub concentration_coef: u128,
    /// minimum base_asset_reserve allowed before AMM is unavailable
    /// precision: AMM_RESERVE_PRECISION
    pub min_base_asset_reserve: u128,
    /// maximum base_asset_reserve allowed before AMM is unavailable
    /// precision: AMM_RESERVE_PRECISION
    pub max_base_asset_reserve: u128,
    /// `sqrt(k)` in constant product mm formula (x * y = k). stored to avoid drift caused by integer math issues
    /// precision: AMM_RESERVE_PRECISION
    pub sqrt_k: u128,
    /// normalizing numerical factor for y, its use offers lowest slippage in cp-curve when market is balanced
    /// precision: PEG_PRECISION
    pub peg_multiplier: u128,
    /// y when market is balanced. stored to save computation
    /// precision: AMM_RESERVE_PRECISION
    pub terminal_quote_asset_reserve: u128,
    /// always non-negative. tracks number of total longs in market (regardless of counterparty)
    /// precision: BASE_PRECISION
    pub base_asset_amount_long: i128,
    /// always non-positive. tracks number of total shorts in market (regardless of counterparty)
    /// precision: BASE_PRECISION
    pub base_asset_amount_short: i128,
    /// tracks net position (longs-shorts) in market with AMM as counterparty
    /// precision: BASE_PRECISION
    pub base_asset_amount_with_amm: i128,
    /// tracks net position (longs-shorts) in market with LPs as counterparty
    /// precision: BASE_PRECISION
    pub base_asset_amount_with_unsettled_lp: i128,
    /// max allowed open interest, blocks trades that breach this value
    /// precision: BASE_PRECISION
    pub max_open_interest: u128,
    /// sum of all user's perp quote_asset_amount in market
    /// precision: QUOTE_PRECISION
    pub quote_asset_amount: i128,
    /// sum of all long user's quote_entry_amount in market
    /// precision: QUOTE_PRECISION
    pub quote_entry_amount_long: i128,
    /// sum of all short user's quote_entry_amount in market
    /// precision: QUOTE_PRECISION
    pub quote_entry_amount_short: i128,
    /// sum of all long user's quote_break_even_amount in market
    /// precision: QUOTE_PRECISION
    pub quote_break_even_amount_long: i128,
    /// sum of all short user's quote_break_even_amount in market
    /// precision: QUOTE_PRECISION
    pub quote_break_even_amount_short: i128,
    /// total user lp shares of sqrt_k (protocol owned liquidity = sqrt_k - last_funding_rate)
    /// precision: AMM_RESERVE_PRECISION
    pub user_lp_shares: u128,
    /// last funding rate in this perp market (unit is quote per base)
    /// precision: QUOTE_PRECISION
    pub last_funding_rate: i64,
    /// last funding rate for longs in this perp market (unit is quote per base)
    /// precision: QUOTE_PRECISION
    pub last_funding_rate_long: i64,
    /// last funding rate for shorts in this perp market (unit is quote per base)
    /// precision: QUOTE_PRECISION
    pub last_funding_rate_short: i64,
    /// estimate of last 24h of funding rate perp market (unit is quote per base)
    /// precision: QUOTE_PRECISION
    pub last_24h_avg_funding_rate: i64,
    /// total fees collected by this perp market
    /// precision: QUOTE_PRECISION
    pub total_fee: i128,
    /// total fees collected by the vAMM's bid/ask spread
    /// precision: QUOTE_PRECISION
    pub total_mm_fee: i128,
    /// total fees collected by exchange fee schedule
    /// precision: QUOTE_PRECISION
    pub total_exchange_fee: u128,
    /// total fees minus any recognized upnl and pool withdraws
    /// precision: QUOTE_PRECISION
    pub total_fee_minus_distributions: i128,
    /// sum of all fees from fee pool withdrawn to revenue pool
    /// precision: QUOTE_PRECISION
    pub total_fee_withdrawn: u128,
    /// all fees collected by market for liquidations
    /// precision: QUOTE_PRECISION
    pub total_liquidation_fee: u128,
    /// accumulated funding rate for longs since inception in market
    pub cumulative_funding_rate_long: i128,
    /// accumulated funding rate for shorts since inception in market
    pub cumulative_funding_rate_short: i128,
    /// accumulated social loss paid by users since inception in market
    pub total_social_loss: u128,
    /// transformed base_asset_reserve for users going long
    /// precision: AMM_RESERVE_PRECISION
    pub ask_base_asset_reserve: u128,
    /// transformed quote_asset_reserve for users going long
    /// precision: AMM_RESERVE_PRECISION
    pub ask_quote_asset_reserve: u128,
    /// transformed base_asset_reserve for users going short
    /// precision: AMM_RESERVE_PRECISION
    pub bid_base_asset_reserve: u128,
    /// transformed quote_asset_reserve for users going short
    /// precision: AMM_RESERVE_PRECISION
    pub bid_quote_asset_reserve: u128,
    /// the last seen oracle price partially shrunk toward the amm reserve price
    /// precision: PRICE_PRECISION
    pub last_oracle_normalised_price: i64,
    /// the gap between the oracle price and the reserve price = y * peg_multiplier / x
    pub last_oracle_reserve_price_spread_pct: i64,
    /// average estimate of bid price over funding_period
    /// precision: PRICE_PRECISION
    pub last_bid_price_twap: u64,
    /// average estimate of ask price over funding_period
    /// precision: PRICE_PRECISION
    pub last_ask_price_twap: u64,
    /// average estimate of (bid+ask)/2 price over funding_period
    /// precision: PRICE_PRECISION
    pub last_mark_price_twap: u64,
    /// average estimate of (bid+ask)/2 price over FIVE_MINUTES
    pub last_mark_price_twap_5min: u64,
    /// the last blockchain slot the amm was updated
    pub last_update_slot: u64,
    /// the pct size of the oracle confidence interval
    /// precision: PERCENTAGE_PRECISION
    pub last_oracle_conf_pct: u64,
    /// the total_fee_minus_distribution change since the last funding update
    /// precision: QUOTE_PRECISION
    pub net_revenue_since_last_funding: i64,
    /// the last funding rate update unix_timestamp
    pub last_funding_rate_ts: i64,
    /// the peridocity of the funding rate updates
    pub funding_period: i64,
    /// the base step size (increment) of orders
    /// precision: BASE_PRECISION
    pub order_step_size: u64,
    /// the price tick size of orders
    /// precision: PRICE_PRECISION
    pub order_tick_size: u64,
    /// the minimum base size of an order
    /// precision: BASE_PRECISION
    pub min_order_size: u64,
    /// the max base size a single user can have
    /// precision: BASE_PRECISION
    pub max_position_size: u64,
    /// estimated total of volume in market
    /// QUOTE_PRECISION
    pub volume_24h: u64,
    /// the volume intensity of long fills against AMM
    pub long_intensity_volume: u64,
    /// the volume intensity of short fills against AMM
    pub short_intensity_volume: u64,
    /// the blockchain unix timestamp at the time of the last trade
    pub last_trade_ts: i64,
    /// estimate of standard deviation of the fill (mark) prices
    /// precision: PRICE_PRECISION
    pub mark_std: u64,
    /// estimate of standard deviation of the oracle price at each update
    /// precision: PRICE_PRECISION
    pub oracle_std: u64,
    /// the last unix_timestamp the mark twap was updated
    pub last_mark_price_twap_ts: i64,
    /// the minimum spread the AMM can quote. also used as step size for some spread logic increases.
    pub base_spread: u32,
    /// the maximum spread the AMM can quote
    pub max_spread: u32,
    /// the spread for asks vs the reserve price
    pub long_spread: u32,
    /// the spread for bids vs the reserve price
    pub short_spread: u32,
    /// the count intensity of long fills against AMM
    pub long_intensity_count: u32,
    /// the count intensity of short fills against AMM
    pub short_intensity_count: u32,
    /// the fraction of total available liquidity a single fill on the AMM can consume
    pub max_fill_reserve_fraction: u16,
    /// the maximum slippage a single fill on the AMM can push
    pub max_slippage_ratio: u16,
    /// the update intensity of AMM formulaic updates (adjusting k). 0-100
    pub curve_update_intensity: u8,
    /// the jit intensity of AMM. larger intensity means larger participation in jit. 0 means no jit participation.
    /// (0, 100] is intensity for protocol-owned AMM. (100, 200] is intensity for user LP-owned AMM.
    pub amm_jit_intensity: u8,
    /// the oracle provider information. used to decode/scale the oracle public key
    pub oracle_source: OracleSource,
    /// tracks whether the oracle was considered valid at the last AMM update
    pub last_oracle_valid: bool,
    /// the target value for `base_asset_amount_per_lp`, used during AMM JIT with LP split
    /// precision: BASE_PRECISION
    pub target_base_asset_amount_per_lp: i32,
    /// expo for unit of per_lp, base 10 (if per_lp_base=X, then per_lp unit is 10^X)
    pub per_lp_base: i8,
    /// the override for the state.min_perp_auction_duration
    /// 0 is no override, -1 is disable speed bump, 1-100 is literal speed bump
    pub taker_speed_bump_override: i8,
    /// signed scale amm_spread similar to fee_adjustment logic (-100 = 0, 100 = double)
    pub amm_spread_adjustment: i8,
    pub oracle_slot_delay_override: i8,
    pub total_fee_earned_per_lp: u64,
    pub net_unsettled_funding_pnl: i64,
    pub quote_asset_amount_with_unsettled_lp: i64,
    pub reference_price_offset: i32,
    /// signed scale amm_spread similar to fee_adjustment logic (-100 = 0, 100 = double)
    pub amm_inventory_spread_adjustment: i8,
    pub padding: [u8; 11],
}

impl Default for AMM {
    fn default() -> Self {
        AMM {
            oracle: Pubkey::default(),
            historical_oracle_data: HistoricalOracleData::default(),
            base_asset_amount_per_lp: 0,
            quote_asset_amount_per_lp: 0,
            fee_pool: PoolBalance::default(),
            base_asset_reserve: 0,
            quote_asset_reserve: 0,
            concentration_coef: 0,
            min_base_asset_reserve: 0,
            max_base_asset_reserve: 0,
            sqrt_k: 0,
            peg_multiplier: 0,
            terminal_quote_asset_reserve: 0,
            base_asset_amount_long: 0,
            base_asset_amount_short: 0,
            base_asset_amount_with_amm: 0,
            base_asset_amount_with_unsettled_lp: 0,
            max_open_interest: 0,
            quote_asset_amount: 0,
            quote_entry_amount_long: 0,
            quote_entry_amount_short: 0,
            quote_break_even_amount_long: 0,
            quote_break_even_amount_short: 0,
            user_lp_shares: 0,
            last_funding_rate: 0,
            last_funding_rate_long: 0,
            last_funding_rate_short: 0,
            last_24h_avg_funding_rate: 0,
            total_fee: 0,
            total_mm_fee: 0,
            total_exchange_fee: 0,
            total_fee_minus_distributions: 0,
            total_fee_withdrawn: 0,
            total_liquidation_fee: 0,
            cumulative_funding_rate_long: 0,
            cumulative_funding_rate_short: 0,
            total_social_loss: 0,
            ask_base_asset_reserve: 0,
            ask_quote_asset_reserve: 0,
            bid_base_asset_reserve: 0,
            bid_quote_asset_reserve: 0,
            last_oracle_normalised_price: 0,
            last_oracle_reserve_price_spread_pct: 0,
            last_bid_price_twap: 0,
            last_ask_price_twap: 0,
            last_mark_price_twap: 0,
            last_mark_price_twap_5min: 0,
            last_update_slot: 0,
            last_oracle_conf_pct: 0,
            net_revenue_since_last_funding: 0,
            last_funding_rate_ts: 0,
            funding_period: 0,
            order_step_size: 0,
            order_tick_size: 0,
            min_order_size: 1,
            max_position_size: 0,
            volume_24h: 0,
            long_intensity_volume: 0,
            short_intensity_volume: 0,
            last_trade_ts: 0,
            mark_std: 0,
            oracle_std: 0,
            last_mark_price_twap_ts: 0,
            base_spread: 0,
            max_spread: 0,
            long_spread: 0,
            short_spread: 0,
            long_intensity_count: 0,
            short_intensity_count: 0,
            max_fill_reserve_fraction: 0,
            max_slippage_ratio: 0,
            curve_update_intensity: 0,
            amm_jit_intensity: 0,
            oracle_source: OracleSource::default(),
            last_oracle_valid: false,
            target_base_asset_amount_per_lp: 0,
            per_lp_base: 0,
            taker_speed_bump_override: 0,
            amm_spread_adjustment: 0,
            oracle_slot_delay_override: 0,
            total_fee_earned_per_lp: 0,
            net_unsettled_funding_pnl: 0,
            quote_asset_amount_with_unsettled_lp: 0,
            reference_price_offset: 0,
            amm_inventory_spread_adjustment: 0,
            padding: [0; 11],
        }
    }
}

impl AMM {
    pub fn get_fallback_price(
        self,
        direction: &PositionDirection,
        amm_available_liquidity: u64,
        oracle_price: i64,
        seconds_til_order_expiry: i64,
    ) -> DriftResult<u64> {
        // PRICE_PRECISION
        if direction.eq(&PositionDirection::Long) {
            // pick amm ask + buffer if theres liquidity
            // otherwise be aggressive vs oracle + 1hr premium
            if amm_available_liquidity >= self.min_order_size {
                let reserve_price = self.reserve_price()?;
                let amm_ask_price: i64 = self.ask_price(reserve_price)?.cast()?;
                amm_ask_price
                    .safe_add(amm_ask_price / (seconds_til_order_expiry * 20).clamp(100, 200))?
                    .cast::<u64>()
            } else {
                oracle_price
                    .safe_add(
                        self.last_ask_price_twap
                            .cast::<i64>()?
                            .safe_sub(self.historical_oracle_data.last_oracle_price_twap)?
                            .max(0),
                    )?
                    .safe_add(oracle_price / (seconds_til_order_expiry * 2).clamp(10, 50))?
                    .cast::<u64>()
            }
        } else {
            // pick amm bid - buffer if theres liquidity
            // otherwise be aggressive vs oracle + 1hr bid premium
            if amm_available_liquidity >= self.min_order_size {
                let reserve_price = self.reserve_price()?;
                let amm_bid_price: i64 = self.bid_price(reserve_price)?.cast()?;
                amm_bid_price
                    .safe_sub(amm_bid_price / (seconds_til_order_expiry * 20).clamp(100, 200))?
                    .cast::<u64>()
            } else {
                oracle_price
                    .safe_add(
                        self.last_bid_price_twap
                            .cast::<i64>()?
                            .safe_sub(self.historical_oracle_data.last_oracle_price_twap)?
                            .min(0),
                    )?
                    .safe_sub(oracle_price / (seconds_til_order_expiry * 2).clamp(10, 50))?
                    .max(0)
                    .cast::<u64>()
            }
        }
    }

    pub fn get_lower_bound_sqrt_k(self) -> DriftResult<u128> {
        Ok(self.sqrt_k.min(
            self.user_lp_shares
                .safe_add(self.user_lp_shares.safe_div(1000)?)?
                .max(self.min_order_size.cast()?)
                .max(self.base_asset_amount_with_amm.unsigned_abs().cast()?),
        ))
    }

    pub fn get_protocol_owned_position(self) -> DriftResult<i64> {
        self.base_asset_amount_with_amm
            .safe_add(self.base_asset_amount_with_unsettled_lp)?
            .cast::<i64>()
    }

    pub fn get_max_reference_price_offset(self) -> DriftResult<i64> {
        if self.curve_update_intensity <= 100 {
            return Ok(0);
        }

        let lower_bound_multiplier: i64 =
            self.curve_update_intensity.safe_sub(100)?.cast::<i64>()?;

        // always higher of 1-100 bps of price offset and half of the market's max_spread
        let lb_bps =
            (PERCENTAGE_PRECISION.cast::<i64>()? / 10000).safe_mul(lower_bound_multiplier)?;
        let max_offset = (self.max_spread.cast::<i64>()? / 2).max(lb_bps);

        Ok(max_offset)
    }

    pub fn get_per_lp_base_unit(self) -> DriftResult<i128> {
        let abs: u32 = self.per_lp_base.abs().cast()?;
        let scalar = pow10_i128_safe(abs)?;
        if self.per_lp_base > 0 {
            AMM_RESERVE_PRECISION_I128.safe_mul(scalar)
        } else if self.per_lp_base < 0 {
            AMM_RESERVE_PRECISION_I128.safe_div(scalar)
        } else {
            Ok(AMM_RESERVE_PRECISION_I128)
        }
    }

    pub fn calculate_lp_base_delta(
        &self,
        per_lp_delta_base: i128,
        base_unit: i128,
    ) -> DriftResult<i128> {
        // calculate dedicated for user lp shares
        let lp_delta_base =
            get_proportion_i128(per_lp_delta_base, self.user_lp_shares, base_unit.cast()?)?;

        Ok(lp_delta_base)
    }

    pub fn calculate_per_lp_delta(
        &self,
        delta: &PositionDelta,
        fee_to_market: i128,
        liquidity_split: AMMLiquiditySplit,
        base_unit: i128,
    ) -> DriftResult<(i128, i128, i128)> {
        let total_lp_shares = if liquidity_split == AMMLiquiditySplit::LPOwned {
            self.user_lp_shares
        } else {
            self.sqrt_k
        };

        // update Market per lp position
        let per_lp_delta_base = get_proportion_i128(
            delta.base_asset_amount.cast()?,
            base_unit.cast()?,
            total_lp_shares, //.safe_div_ceil(rebase_divisor.cast()?)?,
        )?;

        let mut per_lp_delta_quote = get_proportion_i128(
            delta.quote_asset_amount.cast()?,
            base_unit.cast()?,
            total_lp_shares, //.safe_div_ceil(rebase_divisor.cast()?)?,
        )?;

        // user position delta is short => lp position delta is long
        if per_lp_delta_base < 0 {
            // add one => lp subtract 1
            per_lp_delta_quote = per_lp_delta_quote.safe_add(1)?;
        }

        // 1/5 of fee auto goes to market
        // the rest goes to lps/market proportional
        let per_lp_fee: i128 = if fee_to_market > 0 {
            get_proportion_i128(
                fee_to_market,
                LP_FEE_SLICE_NUMERATOR,
                LP_FEE_SLICE_DENOMINATOR,
            )?
            .safe_mul(base_unit)?
            .safe_div(total_lp_shares.cast::<i128>()?)?
        } else {
            0
        };

        Ok((per_lp_delta_base, per_lp_delta_quote, per_lp_fee))
    }

    pub fn get_target_base_asset_amount_per_lp(&self) -> DriftResult<i128> {
        if self.target_base_asset_amount_per_lp == 0 {
            return Ok(0_i128);
        }

        let target_base_asset_amount_per_lp: i128 = if self.per_lp_base > 0 {
            let rebase_divisor = 10_i128.pow(self.per_lp_base.abs().cast()?);
            self.target_base_asset_amount_per_lp
                .cast::<i128>()?
                .safe_mul(rebase_divisor)?
        } else if self.per_lp_base < 0 {
            let rebase_divisor = 10_i128.pow(self.per_lp_base.abs().cast()?);
            self.target_base_asset_amount_per_lp
                .cast::<i128>()?
                .safe_div(rebase_divisor)?
        } else {
            self.target_base_asset_amount_per_lp.cast::<i128>()?
        };

        Ok(target_base_asset_amount_per_lp)
    }

    pub fn imbalanced_base_asset_amount_with_lp(&self) -> DriftResult<i128> {
        let target_lp_gap = self
            .base_asset_amount_per_lp
            .safe_sub(self.get_target_base_asset_amount_per_lp()?)?;

        let base_unit = self.get_per_lp_base_unit()?.cast()?;

        get_proportion_i128(target_lp_gap, self.user_lp_shares, base_unit)
    }

    pub fn amm_wants_to_jit_make(&self, taker_direction: PositionDirection) -> DriftResult<bool> {
        let amm_wants_to_jit_make = match taker_direction {
            PositionDirection::Long => {
                self.base_asset_amount_with_amm < -(self.order_step_size.cast()?)
            }
            PositionDirection::Short => {
                self.base_asset_amount_with_amm > (self.order_step_size.cast()?)
            }
        };
        Ok(amm_wants_to_jit_make && self.amm_jit_is_active())
    }

    pub fn amm_lp_wants_to_jit_make(
        &self,
        taker_direction: PositionDirection,
    ) -> DriftResult<bool> {
        if self.user_lp_shares == 0 {
            return Ok(false);
        }

        let amm_lp_wants_to_jit_make = match taker_direction {
            PositionDirection::Long => {
                self.base_asset_amount_per_lp > self.get_target_base_asset_amount_per_lp()?
            }
            PositionDirection::Short => {
                self.base_asset_amount_per_lp < self.get_target_base_asset_amount_per_lp()?
            }
        };
        Ok(amm_lp_wants_to_jit_make && self.amm_lp_jit_is_active())
    }

    pub fn amm_lp_allowed_to_jit_make(&self, amm_wants_to_jit_make: bool) -> DriftResult<bool> {
        // only allow lps to make when the amm inventory is below a certain level of available liquidity
        // i.e. 10%
        if amm_wants_to_jit_make {
            // inventory scale
            let (max_bids, max_asks) = amm::_calculate_market_open_bids_asks(
                self.base_asset_reserve,
                self.min_base_asset_reserve,
                self.max_base_asset_reserve,
            )?;

            let min_side_liquidity = max_bids.min(max_asks.abs());
            let protocol_owned_min_side_liquidity = get_proportion_i128(
                min_side_liquidity,
                self.sqrt_k.safe_sub(self.user_lp_shares)?,
                self.sqrt_k,
            )?;

            Ok(self.base_asset_amount_with_amm.abs()
                < protocol_owned_min_side_liquidity.safe_div(10)?)
        } else {
            Ok(true)
        }
    }

    pub fn amm_jit_is_active(&self) -> bool {
        self.amm_jit_intensity > 0
    }

    pub fn amm_lp_jit_is_active(&self) -> bool {
        self.amm_jit_intensity > 100
    }

    // === [IMPROVED: PIECE S24 v++ HFT⁷] === Unified grid step helper
    #[inline(always)]
    pub fn grid_step(&self) -> u64 {
        let t = self.order_tick_size;
        if t > 0 { t } else { self.order_step_size.max(1) }
    }

    #[inline(always)]
    pub fn order_grid_step(&self) -> u64 { self.grid_step() }

    // Returns whether the grid step is a power of two and the mask (step-1) if so.
    #[inline(always)]
    pub fn order_grid_pow2_mask(&self) -> (bool, u64) {
        let s = self.grid_step().max(1);
        let is_pow2 = s.is_power_of_two();
        let mask = if is_pow2 { s - 1 } else { 0 };
        (is_pow2, mask)
    }

    pub fn reserve_price(&self) -> DriftResult<u64> {
        amm::calculate_price(
            self.quote_asset_reserve,
            self.base_asset_reserve,
            self.peg_multiplier,
        )
    }

    pub fn bid_price(&self, reserve_price: u64) -> DriftResult<u64> {
        let adjusted_spread =
            (-(self.short_spread.cast::<i32>()?)).safe_add(self.reference_price_offset)?;

        let multiplier = BID_ASK_SPREAD_PRECISION_I128.safe_add(adjusted_spread.cast::<i128>()?)?;

        reserve_price
            .cast::<u128>()?
    }

    pub fn ask_price(&self, reserve_price: u64) -> DriftResult<u64> {
        let adjusted_spread =
            (-(self.short_spread.cast::<i32>()?)).safe_add(self.reference_price_offset)?;

        let multiplier = BID_ASK_SPREAD_PRECISION_I128.safe_add(adjusted_spread.cast::<i128>()?)?;

        reserve_price
            .cast::<u128>()?
            .safe_mul(multiplier.cast::<u128>()?)?
            .safe_div(BID_ASK_SPREAD_PRECISION_U128)?
            .cast()
    }

    pub fn bid_ask_price(&self, reserve_price: u64) -> DriftResult<(u64, u64)> {
        let bid_price = self.bid_price(reserve_price)?;
        let ask_price = self.ask_price(reserve_price)?;
        Ok((bid_price, ask_price))
    }

    pub fn last_ask_premium(&self) -> DriftResult<i64> {
        let reserve_price = self.reserve_price()?;
        let ask_price = self.ask_price(reserve_price)?.cast::<i64>()?;
        ask_price.safe_sub(self.historical_oracle_data.last_oracle_price)
    }

    pub fn last_bid_discount(&self) -> DriftResult<i64> {
        let reserve_price = self.reserve_price()?;
        let bid_price = self.bid_price(reserve_price)?.cast::<i64>()?;
        self.historical_oracle_data
            .last_oracle_price
            .safe_sub(bid_price)
    }

    pub fn can_lower_k(&self) -> DriftResult<bool> {
        let (max_bids, max_asks) = amm::calculate_market_open_bids_asks(self)?;
        let min_order_size_u128 = self.min_order_size.cast::<u128>()?;

        let can_lower = (self.base_asset_amount_with_amm.unsigned_abs()
            < max_bids.unsigned_abs().min(max_asks.unsigned_abs()))
            && (self
                .base_asset_amount_with_amm
                .unsigned_abs()
                .max(min_order_size_u128)
                < self.sqrt_k.safe_sub(self.user_lp_shares)?)
            && (min_order_size_u128 < max_bids.unsigned_abs().max(max_asks.unsigned_abs()));

        Ok(can_lower)
    }

    pub fn get_oracle_twap(
        &self,
        price_oracle: &AccountInfo,
        slot: u64,
    ) -> DriftResult<Option<i64>> {
        match self.oracle_source {
            OracleSource::Pyth | OracleSource::PythStableCoin => {
                Ok(Some(self.get_pyth_twap(price_oracle, &OracleSource::Pyth)?))
            }
            OracleSource::Pyth1K => Ok(Some(
                self.get_pyth_twap(price_oracle, &OracleSource::Pyth1K)?,
            )),
            OracleSource::Pyth1M => Ok(Some(
                self.get_pyth_twap(price_oracle, &OracleSource::Pyth1M)?,
            )),
            OracleSource::Switchboard => Ok(Some(get_switchboard_price(price_oracle, slot)?.price)),
            OracleSource::SwitchboardOnDemand => {
                Ok(Some(get_sb_on_demand_price(price_oracle, slot)?.price))
            }
            OracleSource::QuoteAsset => {
                msg!("Can't get oracle twap for quote asset");
                Err(ErrorCode::DefaultError)
            }
            OracleSource::Prelaunch => Ok(Some(get_prelaunch_price(price_oracle, slot)?.price)),
            OracleSource::PythPull | OracleSource::PythStableCoinPull => Ok(Some(
                self.get_pyth_twap(price_oracle, &OracleSource::PythPull)?,
            )),
            OracleSource::Pyth1KPull => Ok(Some(
                self.get_pyth_twap(price_oracle, &OracleSource::Pyth1KPull)?,
            )),
            OracleSource::Pyth1MPull => Ok(Some(
                self.get_pyth_twap(price_oracle, &OracleSource::Pyth1MPull)?,
            )),
            OracleSource::PythLazer => Ok(Some(
                self.get_pyth_twap(price_oracle, &OracleSource::PythLazer)?,
            )),
            OracleSource::PythLazer1K => Ok(Some(
                self.get_pyth_twap(price_oracle, &OracleSource::PythLazer1K)?,
            )),
            OracleSource::PythLazer1M => Ok(Some(
                self.get_pyth_twap(price_oracle, &OracleSource::PythLazer1M)?,
            )),
            OracleSource::PythLazerStableCoin => Ok(Some(
                self.get_pyth_twap(price_oracle, &OracleSource::PythLazerStableCoin)?,
            )),
        }
    }

    pub fn get_pyth_twap(
        &self,
        price_oracle: &AccountInfo,
        oracle_source: &OracleSource,
    ) -> DriftResult<i64> {
        let multiple = oracle_source.get_pyth_multiple();
        let mut pyth_price_data: &[u8] = &price_oracle
            .try_borrow_data()
            .or(Err(ErrorCode::UnableToLoadOracle))?;

        let oracle_price: i64;
        let oracle_twap: i64;
        let oracle_exponent: i32;

        if oracle_source.is_pyth_pull_oracle() {
            let price_message =
                pyth_solana_receiver_sdk::price_update::PriceUpdateV2::try_deserialize(
                    &mut pyth_price_data,
                )
                .or(Err(crate::error::ErrorCode::UnableToLoadOracle))?;
            oracle_price = price_message.price_message.price;
            oracle_twap = price_message.price_message.ema_price;
            oracle_exponent = price_message.price_message.exponent;
        } else if oracle_source.is_pyth_push_oracle() {
            let price_data = pyth_client::cast::<pyth_client::Price>(pyth_price_data);
            oracle_price = price_data.agg.price;
            oracle_twap = price_data.twap.val;
            oracle_exponent = price_data.expo;
        } else {
            let price_data = PythLazerOracle::try_deserialize(&mut pyth_price_data)
                .or(Err(ErrorCode::UnableToLoadOracle))?;
            oracle_price = price_data.price;
            oracle_twap = price_data.price;
            oracle_exponent = price_data.exponent;
        }

        assert!(oracle_twap > oracle_price / 10);

        let oracle_precision = 10_u128
            .pow(oracle_exponent.unsigned_abs())
            .safe_div(multiple)?;

        let mut oracle_scale_mult = 1;
        let mut oracle_scale_div = 1;

        if oracle_precision > PRICE_PRECISION {
            oracle_scale_div = oracle_precision.safe_div(PRICE_PRECISION)?;
        } else {
            oracle_scale_mult = PRICE_PRECISION.safe_div(oracle_precision)?;
        }

        oracle_twap
            .cast::<i128>()?
            .safe_mul(oracle_scale_mult.cast()?)?
            .safe_div(oracle_scale_div.cast()?)?
            .cast::<i64>()
    }

    pub fn update_volume_24h(
        &mut self,
        quote_asset_amount: u64,
        position_direction: PositionDirection,
        now: i64,
    ) -> DriftResult {
        let since_last = max(1_i64, now.safe_sub(self.last_trade_ts)?);

        amm::update_amm_long_short_intensity(self, now, quote_asset_amount, position_direction)?;

        self.volume_24h = stats::calculate_rolling_sum(
            self.volume_24h,
            quote_asset_amount,
            since_last,
            TWENTY_FOUR_HOUR,
        )?;

        self.last_trade_ts = now;

        Ok(())
    }

    pub fn get_new_oracle_conf_pct(
        &self,
        confidence: u64,    // price precision
        reserve_price: u64, // price precision
        now: i64,
    ) -> DriftResult<u64> {
        // use previous value decayed as lower bound to avoid shrinking too quickly
        let upper_bound_divisor = 21_u64;
        let lower_bound_divisor = 5_u64;
        let since_last = now
            .safe_sub(self.historical_oracle_data.last_oracle_price_twap_ts)?
            .max(0);

        let confidence_lower_bound = if since_last > 0 {
            let confidence_divisor = upper_bound_divisor
                .saturating_sub(since_last.cast::<u64>()?)
                .max(lower_bound_divisor);
            self.last_oracle_conf_pct
                .safe_sub(self.last_oracle_conf_pct / confidence_divisor)?
        } else {
            self.last_oracle_conf_pct
        };

        Ok(confidence
            .safe_mul(BID_ASK_SPREAD_PRECISION)?
            .safe_div(reserve_price)?
            .max(confidence_lower_bound))
    }

    pub fn is_recent_oracle_valid(&self, current_slot: u64) -> DriftResult<bool> {
        Ok(self.last_oracle_valid && current_slot == self.last_update_slot)
    }
}

#[cfg(test)]
impl AMM {
    pub fn default_test() -> Self {
        let default_reserves = 100 * AMM_RESERVE_PRECISION;
        // make sure tests dont have the default sqrt_k = 0
        AMM {
            base_asset_reserve: default_reserves,
            quote_asset_reserve: default_reserves,
            sqrt_k: default_reserves,
            concentration_coef: MAX_CONCENTRATION_COEFFICIENT,
            order_step_size: 1,
            order_tick_size: 1,
            max_base_asset_reserve: u64::MAX as u128,
            min_base_asset_reserve: 0,
            terminal_quote_asset_reserve: default_reserves,
            peg_multiplier: crate::math::constants::PEG_PRECISION,
            max_fill_reserve_fraction: 1,
            max_spread: 1000,
            historical_oracle_data: HistoricalOracleData {
                last_oracle_price: PRICE_PRECISION_I64,
                ..HistoricalOracleData::default()
            },
            last_oracle_valid: true,
            ..AMM::default()
        }
    }

    pub fn default_btc_test() -> Self {
        AMM {
            base_asset_reserve: 65 * AMM_RESERVE_PRECISION,
            quote_asset_reserve: 63015384615,
            terminal_quote_asset_reserve: 64 * AMM_RESERVE_PRECISION,
            sqrt_k: 64 * AMM_RESERVE_PRECISION,

            peg_multiplier: 19_400_000_000,

            concentration_coef: MAX_CONCENTRATION_COEFFICIENT,
            max_base_asset_reserve: 90 * AMM_RESERVE_PRECISION,
            min_base_asset_reserve: 45 * AMM_RESERVE_PRECISION,

            base_asset_amount_with_amm: -(AMM_RESERVE_PRECISION as i128),
            mark_std: PRICE_PRECISION as u64,

            quote_asset_amount: 19_000_000_000, // short 1 BTC @ $19000
            historical_oracle_data: HistoricalOracleData {
                last_oracle_price: 19_400 * PRICE_PRECISION_I64,
                last_oracle_price_twap: 19_400 * PRICE_PRECISION_I64,
                last_oracle_price_twap_5min: 19_400 * PRICE_PRECISION_I64,
                last_oracle_price_twap_ts: 1662800000_i64,
                ..HistoricalOracleData::default()
            },
            last_mark_price_twap_ts: 1662800000,

            curve_update_intensity: 100,

            base_spread: 250,
            max_spread: 975,
            funding_period: 3600,
            last_oracle_valid: true,
            ..AMM::default()
        }
    }
}
