use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH, Instant, Duration};
use solana_sdk::pubkey::Pubkey;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use thiserror::Error;

// Stable decimal square root using Newton-Raphson. Keeps ~28-30 digit precision.
fn sqrt_decimal(x: Decimal) -> Decimal {
    if x <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    let mut z = x;
    for _ in 0..12 {
        z = (z + x / z) / dec!(2);
    }
    z
}

#[derive(Debug, Clone, Default)]
pub struct Metrics {
    pub entries_attempted: u64,
    pub entries_executed: u64,
    pub exits_executed: u64,
    pub bundles_sent: u64,
    pub last_error: Option<&'static str>,
}

#[derive(Debug, Clone)]
pub struct InclusionHints {
    pub leader_slot_in: u64,
    pub recent_inclusion_rate: Decimal, // 0..1
    pub tip_suggestion_lamports: u64,
}

pub trait Executor {
    fn send_limit(&self, price: Decimal, size: Decimal, max_slippage: Decimal) -> std::result::Result<(), String>;
    fn send_bundle(&self, price: Decimal, size: Decimal, tip_lamports: u64) -> std::result::Result<(), String>;
    fn hints(&self) -> InclusionHints;
}

#[derive(Error, Debug)]
pub enum MartingaleError {
    #[error("No active position")]
    NoActivePosition,
    #[error("Position already active")]
    PositionAlreadyActive,
    #[error("Circuit breaker triggered")]
    CircuitBreakerActive,
    #[error("Insufficient liquidity: {0}")]
    InsufficientLiquidity(Decimal),
    #[error("Cooldown period active")]
    CooldownActive,
    #[error("Invalid position size: {0}")]
    InvalidPositionSize(Decimal),
    #[error("Time error: {0}")]
    TimeError(#[from] std::time::SystemTimeError),
}

pub type Result<T> = std::result::Result<T, MartingaleError>;

#[derive(Debug, Clone)]
pub struct MartingaleConfig {
    pub base_position_size: Decimal,
    pub max_position_size: Decimal,
    pub multiplier: Decimal,
    pub max_consecutive_losses: u32,
    pub profit_target_percent: Decimal,
    pub stop_loss_percent: Decimal,
    pub cooldown_period_ms: u64,
    pub volatility_adjustment: bool,
    pub kelly_fraction: Decimal,
    pub max_drawdown_percent: Decimal,
    pub risk_per_trade: Decimal,
    pub slippage_tolerance: Decimal,
    pub min_liquidity_threshold: Decimal,
    pub max_gas_price_lamports: u64,
    pub position_timeout_ms: u64,
    pub circuit_breaker_threshold: Decimal,
    pub ema_period: usize,
    pub volatility_window: usize,
    pub confidence_threshold: Decimal,
    pub trailing_stop_activation: Decimal,
    pub max_trade_history: usize,
    pub min_order_size: Decimal,
    pub min_notional: Decimal,
}

impl Default for MartingaleConfig {
    fn default() -> Self {
        Self {
            base_position_size: dec!(0.001),
            max_position_size: dec!(0.05),
            multiplier: dec!(2.0),
            max_consecutive_losses: 5,
            profit_target_percent: dec!(0.015),
            stop_loss_percent: dec!(0.005),
            cooldown_period_ms: 100,
            volatility_adjustment: true,
            kelly_fraction: dec!(0.25),
            max_drawdown_percent: dec!(0.10),
            risk_per_trade: dec!(0.02),
            slippage_tolerance: dec!(0.003),
            min_liquidity_threshold: dec!(50000),
            max_gas_price_lamports: 50000,
            position_timeout_ms: 30000,
            circuit_breaker_threshold: dec!(0.05),
            ema_period: 20,
            volatility_window: 50,
            confidence_threshold: dec!(0.65),
            trailing_stop_activation: dec!(0.01),
            max_trade_history: 1000,
            min_order_size: dec!(0.0001),
            min_notional: dec!(1),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TradeMetrics {
    pub timestamp: u64,
    pub position_size: Decimal,
    pub entry_price: Decimal,
    pub exit_price: Option<Decimal>,
    pub pnl: Decimal,
    pub is_win: bool,
    pub slippage: Decimal,
    pub gas_cost: Decimal,
    pub execution_time_ms: u64,
}

#[derive(Debug, Clone)]
pub struct StrategyState {
    pub consecutive_losses: u32,
    pub consecutive_wins: u32,
    pub current_position_size: Decimal,
    pub total_pnl: Decimal,
    pub peak_balance: Decimal,
    pub current_drawdown: Decimal,
    pub boot_instant: Instant,
    pub last_trade_instant: Instant,
    pub reentry_lock_until_ms: u64,
    pub equity: Decimal,
    pub notional_cap: Decimal,
    pub trade_history: VecDeque<TradeMetrics>,
    pub price_history: VecDeque<(u64, Decimal)>,
    pub volatility_cache: Option<(Instant, Decimal)>,
    pub ema_value: Decimal,
    pub confidence_score: Decimal,
    pub active_position: Option<ActivePosition>,
    pub circuit_breaker_triggered: bool,
    pub total_gas_spent: Decimal,
    pub highest_price_since_entry: Decimal,
}

#[derive(Debug, Clone)]
pub struct ActivePosition {
    pub entry_price: Decimal,
    pub size: Decimal,
    pub timestamp: u64,
    pub stop_loss: Decimal,
    pub take_profit: Decimal,
    pub trailing_stop: Option<Decimal>,
}

pub struct MartingaleOptimizer {
    config: Arc<RwLock<MartingaleConfig>>,
    state: Arc<RwLock<StrategyState>>,
    metrics: Arc<RwLock<Metrics>>,
}

#[derive(Debug, Clone)]
pub enum Decision {
    Enter { size: Decimal, max_slippage: Decimal },
    Skip,
}

impl MartingaleOptimizer {

    pub async fn decide_entry(
        &self,
        current_price: Decimal,
        liquidity: Decimal,
        network_congestion: f64,
    ) -> Result<Decision> {
        // metrics: attempted entry
        {
            let mut m = self.metrics.write().await;
            m.entries_attempted = m.entries_attempted.saturating_add(1);
        }
        let config = self.config.read().await;
        let mut state = self.state.write().await;

        if state.circuit_breaker_triggered {
            return Err(MartingaleError::CircuitBreakerActive);
        }

        // Monotonic cooldown and reentry lock (reentry_lock_until_ms compared against UNIX now)
        if state.last_trade_instant.elapsed().as_millis() as u64 < config.cooldown_period_ms {
            return Err(MartingaleError::CooldownActive);
        }

        let now_unix_ms = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
        if now_unix_ms < state.reentry_lock_until_ms {
            return Err(MartingaleError::CooldownActive);
        }

        if liquidity < config.min_liquidity_threshold {
            return Err(MartingaleError::InsufficientLiquidity(liquidity));
        }

        if state.active_position.is_some() {
            return Err(MartingaleError::PositionAlreadyActive);
        }

        // Update histories for analytics (UNIX timestamps only for audit)
        self.update_price_history(&mut state, now_unix_ms, current_price);
        self.invalidate_volatility_cache(&mut state);

        let vol = self.calculate_volatility(&mut state, &config);
        self.update_ema(&mut state, current_price, config.ema_period);

        // leader_damp not available here without external hints; default to 1.0
        let size = self.calculate_optimal_position_size(
            &config,
            &state,
            current_price,
            vol,
            network_congestion,
            Decimal::ONE,
        );

        if size <= Decimal::ZERO || size > config.max_position_size {
            return Err(MartingaleError::InvalidPositionSize(size));
        }

        let conf = self.calculate_confidence_score(&state, vol, current_price);
        state.confidence_score = conf;

        if conf < config.confidence_threshold {
            return Ok(Decision::Skip);
        }

        // Profit guard: expected edge after costs must be positive
        let stats = self.calculate_trade_statistics(&state);
        let notional = size * current_price;
        let edge = self.expected_edge_after_costs(
            &stats,
            config.slippage_tolerance,
            Decimal::ZERO,
            Decimal::ZERO,
            notional,
        );
        if edge <= Decimal::ZERO {
            return Ok(Decision::Skip);
        }

        // Risk checks (worst-case loss + notional cap) in equity terms
        let stop_frac = config.stop_loss_percent.max(dec!(0.001));
        let worst_loss = size * current_price * stop_frac;
        let max_allowed_loss = state.equity * config.risk_per_trade;
        let notional_guard = notional <= state.equity * state.notional_cap;
        if !(worst_loss <= max_allowed_loss && notional_guard) {
            return Ok(Decision::Skip);
        }

        Ok(Decision::Enter { size, max_slippage: config.slippage_tolerance })
    }
    pub fn new(config: MartingaleConfig) -> Self {
        let now_inst = Instant::now();
        let state = StrategyState {
            consecutive_losses: 0,
            consecutive_wins: 0,
            current_position_size: config.base_position_size,
            total_pnl: Decimal::ZERO,
            peak_balance: dec!(1.0),
            current_drawdown: Decimal::ZERO,
            boot_instant: now_inst,
            last_trade_instant: now_inst,
            reentry_lock_until_ms: 0,
            equity: dec!(1.0),
            notional_cap: dec!(0.25),
            trade_history: VecDeque::with_capacity(config.max_trade_history),
            price_history: VecDeque::with_capacity(config.volatility_window * 2),
            volatility_cache: None,
            ema_value: Decimal::ZERO,
            confidence_score: dec!(0.5),
            active_position: None,
            circuit_breaker_triggered: false,
            total_gas_spent: Decimal::ZERO,
            highest_price_since_entry: Decimal::ZERO,
        };

        Self {
            config: Arc::new(RwLock::new(config)),
            state: Arc::new(RwLock::new(state)),
            metrics: Arc::new(RwLock::new(Metrics::default())),
        }
    }

    pub async fn should_enter_position(
        &self,
        current_price: Decimal,
        liquidity: Decimal,
        network_congestion: f64,
    ) -> Result<(bool, Decimal)> {
        let config = self.config.read().await;
        let mut state = self.state.write().await;

        if state.circuit_breaker_triggered {
            return Err(MartingaleError::CircuitBreakerActive);
        }

        // Monotonic cooldown based on last trade instant
        if state.last_trade_instant.elapsed().as_millis() as u64 < config.cooldown_period_ms {
            return Err(MartingaleError::CooldownActive);
        }

        if liquidity < config.min_liquidity_threshold {
            return Err(MartingaleError::InsufficientLiquidity(liquidity));
        }

        if state.active_position.is_some() {
            return Err(MartingaleError::PositionAlreadyActive);
        }

        // Still record UNIX timestamp for price/volatility history; durations use Instant elsewhere
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
        self.update_price_history(&mut state, now, current_price);
        self.invalidate_volatility_cache(&mut state);
        
        let volatility = self.calculate_volatility(&mut state, &config);
        self.update_ema(&mut state, current_price, config.ema_period);

        let position_size = self.calculate_optimal_position_size(
            &config,
            &state,
            current_price,
            volatility,
            network_congestion,
            Decimal::ONE,
        );

        if position_size <= Decimal::ZERO || position_size > config.max_position_size {
            return Err(MartingaleError::InvalidPositionSize(position_size));
        }

        let confidence = self.calculate_confidence_score(
            &state,
            volatility,
            current_price,
        );

        state.confidence_score = confidence;

        if confidence < config.confidence_threshold {
            return Ok((false, Decimal::ZERO));
        }

        // Profit guard: block entries when expected edge after costs <= 0
        let stats = self.calculate_trade_statistics(&state);
        let notional = position_size * current_price;
        let edge = self.expected_edge_after_costs(
            &stats,
            config.slippage_tolerance,
            Decimal::ZERO, // gas in quote unknown here; estimate externally
            Decimal::ZERO, // tip in quote unknown here; estimate externally
            notional,
        );
        if edge <= Decimal::ZERO {
            return Ok((false, Decimal::ZERO));
        }

        // Worst-case loss risk checks and notional cap
        let stop_frac = config.stop_loss_percent.max(dec!(0.001));
        let worst_loss = position_size * current_price * stop_frac;
        let max_allowed_loss = state.equity * config.risk_per_trade;
        let notional = position_size * current_price;
        let notional_guard = notional <= state.equity * state.notional_cap;
        let risk_check = worst_loss <= max_allowed_loss && notional_guard;

        let drawdown_check = state.current_drawdown < config.max_drawdown_percent;

        Ok((drawdown_check && risk_check, position_size))
    }

    pub async fn enter_position(
        &self,
        entry_price: Decimal,
        position_size: Decimal,
        actual_slippage: Decimal,
        gas_cost: u64,
    ) -> Result<()> {
        let config = self.config.read().await;
        let mut state = self.state.write().await;

        if state.active_position.is_some() {
            return Err(MartingaleError::PositionAlreadyActive);
        }

        // Min order and notional clamps (dust avoidance)
        if position_size < config.min_order_size || position_size * entry_price < config.min_notional {
            return Err(MartingaleError::InvalidPositionSize(position_size));
        }

        let adjusted_entry = entry_price * (Decimal::ONE + actual_slippage);
        let stop_loss = adjusted_entry * (Decimal::ONE - config.stop_loss_percent);
        let take_profit = adjusted_entry * (Decimal::ONE + config.profit_target_percent);

        // Store position start timestamp as milliseconds since boot for monotonic timeout calculation
        state.active_position = Some(ActivePosition {
            entry_price: adjusted_entry,
            size: position_size,
            timestamp: state.boot_instant.elapsed().as_millis() as u64,
            stop_loss,
            take_profit,
            trailing_stop: None,
        });

        state.current_position_size = position_size;
        state.total_gas_spent += Decimal::from(gas_cost) / dec!(1_000_000_000);
        state.highest_price_since_entry = adjusted_entry;

        // metrics
        {
            let mut m = self.metrics.write().await;
            m.entries_executed = m.entries_executed.saturating_add(1);
        }

        Ok(())
    }

    pub async fn should_exit_position(
        &self,
        current_price: Decimal,
    ) -> Result<(bool, ExitReason)> {
        let config = self.config.read().await;
        let mut state = self.state.write().await;

        let position = state.active_position.as_ref()
            .ok_or(MartingaleError::NoActivePosition)?;

        if current_price > state.highest_price_since_entry {
            state.highest_price_since_entry = current_price;
        }

        if current_price <= position.stop_loss {
            return Ok((true, ExitReason::StopLoss));
        }

        if current_price >= position.take_profit {
            return Ok((true, ExitReason::TakeProfit));
        }

        // Monotonic timeout since position start
        let elapsed = state
            .active_position
            .as_ref()
            .map(|p| state.boot_instant + Duration::from_millis(p.timestamp))
            .and_then(|t0| Instant::now().checked_duration_since(t0))
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        if elapsed > config.position_timeout_ms {
            return Ok((true, ExitReason::Timeout));
        }

        let profit_percent = (current_price - position.entry_price) / position.entry_price;
        if profit_percent > config.trailing_stop_activation {
            // Use ATR-like dynamic trailing stop using latest realized volatility
            let vol = self.calculate_volatility(&mut state, &config);
            let trailing_stop = self.calculate_trailing_stop_dyn(
                state.highest_price_since_entry,
                vol,
            );
            
            if let Some(pos) = state.active_position.as_mut() {
                pos.trailing_stop = Some(trailing_stop);
            }
            
            if current_price <= trailing_stop {
                return Ok((true, ExitReason::TrailingStop));
            }
        }

        Ok((false, ExitReason::None))
    }

    // ATR-like dynamic trailing stop using realized volatility
    fn calculate_trailing_stop_dyn(&self, highest: Decimal, vol: Decimal) -> Decimal {
        let atrp = (vol * dec!(100)).min(dec!(5)); // cap 5%
        let trail_pct = (dec!(0.003) + atrp / dec!(100) * dec!(0.5)).min(dec!(0.02));
        highest * (Decimal::ONE - trail_pct)
    }

    // Profit guard: expected edge after costs, slippage, and tip
    fn expected_edge_after_costs(
        &self,
        stats: &TradeStatistics,
        slippage_frac: Decimal,
        gas_quote: Decimal,
        tip_quote: Decimal,
        notional: Decimal,
    ) -> Decimal {
        if notional <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        let p = stats.win_rate;
        let ew = p * (stats.avg_win / notional)
            - (Decimal::ONE - p) * (stats.avg_loss / notional);
        ew - ((gas_quote + tip_quote) / notional) - slippage_frac
    }

    pub async fn exit_position(
        &self,
        exit_price: Decimal,
        actual_slippage: Decimal,
        gas_cost: u64,
        execution_time_ms: u64,
    ) -> Result<Decimal> {
        let config = self.config.read().await;
        let mut state = self.state.write().await;

        let position = state.active_position.take()
            .ok_or(MartingaleError::NoActivePosition)?;

        let adjusted_exit = exit_price * (Decimal::ONE - actual_slippage);
        let gross_pnl = (adjusted_exit - position.entry_price) * position.size;
        let gas_decimal = Decimal::from(gas_cost) / dec!(1_000_000_000);
        let net_pnl = gross_pnl - gas_decimal;

        let is_win = net_pnl > Decimal::ZERO;

        // Equity-based accounting and drawdown update
        state.equity += net_pnl;
        self.update_drawdown(&mut state);

        let trade = TradeMetrics {
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64,
            position_size: position.size,
            entry_price: position.entry_price,
            exit_price: Some(adjusted_exit),
            pnl: net_pnl,
            is_win,
            slippage: actual_slippage,
            gas_cost: gas_decimal,
            execution_time_ms,
        };

        self.record_trade(&mut state, trade, &config);
        self.update_position_sizing(&mut state, is_win, &config);
        self.check_circuit_breaker(&mut state, &config);

        state.highest_price_since_entry = Decimal::ZERO;

        // metrics
        {
            let mut m = self.metrics.write().await;
            m.exits_executed = m.exits_executed.saturating_add(1);
        }

        Ok(net_pnl)
    }

    // Safe Kelly fraction with caps to avoid runaway sizing
    fn safe_kelly(&self, p: Decimal, avg_win: Decimal, avg_loss: Decimal) -> Decimal {
        if p <= Decimal::ZERO || avg_win <= Decimal::ZERO || avg_loss <= Decimal::ZERO {
            return Decimal::ZERO;
        }
        let b = (avg_win / avg_loss).max(dec!(0.01));
        let p = p.min(dec!(0.99)).max(dec!(0.01));
        let q = Decimal::ONE - p;
        let edge = p - q / b;
        if edge <= Decimal::ZERO { return Decimal::ZERO; }
        edge.min(dec!(0.25))
    }

    fn calculate_optimal_position_size(
        &self,
        config: &MartingaleConfig,
        state: &StrategyState,
        current_price: Decimal,
        volatility: Decimal,
        network_congestion: f64,
        leader_damp: Decimal,
    ) -> Decimal {
        let stats = self.calculate_trade_statistics(state);

        let k = self.safe_kelly(stats.win_rate, stats.avg_win, stats.avg_loss) * config.kelly_fraction;
        let stop_frac = config.stop_loss_percent.max(dec!(0.001));
        let desired_loss = state.equity * k * config.risk_per_trade; // conservative stacking
        let mut size_from_kelly = if desired_loss > Decimal::ZERO {
            (desired_loss / stop_frac) / current_price
        } else {
            Decimal::ZERO
        };

        if config.volatility_adjustment {
            size_from_kelly *= Decimal::ONE / (Decimal::ONE + volatility * dec!(10));
        }

        let congestion_factor = Decimal::from_f64_retain(1.0 - network_congestion * 0.5)
            .unwrap_or(Decimal::ONE);
        let sharpe_adjustment = (stats.sharpe_ratio / dec!(2)).min(dec!(1.5)).max(dec!(0.5));

        (size_from_kelly * congestion_factor * state.confidence_score * sharpe_adjustment * leader_damp)
            .min(config.max_position_size)
            .max(Decimal::ZERO)
    }

    fn calculate_volatility(&self, state: &mut StrategyState, config: &MartingaleConfig) -> Decimal {
        if let Some((cache_time, cached_vol)) = state.volatility_cache {
            if cache_time.elapsed().as_millis() as u64  < 5000 {
                return cached_vol;
            }
        }

        if state.price_history.len() < 2 {
            return dec!(0.01);
        }

        let prices: Vec<Decimal> = state.price_history
            .iter()
            .rev()
            .take(config.volatility_window)
            .map(|(_, p)| *p)
            .collect();

        if prices.len() < 2 {
            return dec!(0.01);
        }

        let mut returns = Vec::with_capacity(prices.len() - 1);
        for i in 1..prices.len() {
            // forward simple return
            let ret = (prices[i] - prices[i - 1]) / prices[i - 1];
            returns.push(ret);
        }

        let n = returns.len() as u64;
        let mean = returns.iter().copied().sum::<Decimal>() / Decimal::from(n);
        let variance = if n > 1 {
            returns.iter()
                .map(|r| {
                    let d = *r - mean;
                    d * d
                })
                .sum::<Decimal>() / Decimal::from(n - 1)
        } else {
            Decimal::ZERO
        };

        let vol = sqrt_decimal(variance);
        state.volatility_cache = Some((Instant::now(), vol));
        vol
    }

    fn update_ema(&self, state: &mut StrategyState, current_price: Decimal, period: usize) {
        if state.ema_value == Decimal::ZERO {
            state.ema_value = current_price;
            return;
        }
        let p = Decimal::from((period.max(1)) as u64);
        let mut alpha = dec!(2) / (p + dec!(1));
        if alpha <= Decimal::ZERO { alpha = dec!(0.01); }
        if alpha >= Decimal::ONE { alpha = dec!(0.99); }
        state.ema_value = current_price * alpha + state.ema_value * (Decimal::ONE - alpha);
    }

    fn calculate_confidence_score(
        &self,
        state: &StrategyState,
        volatility: Decimal,
        current_price: Decimal,
    ) -> Decimal {
        let mut score = Decimal::ZERO;
        
        if state.ema_value != Decimal::ZERO {
            let trend_alignment = if current_price > state.ema_value {
                let strength = ((current_price - state.ema_value) / state.ema_value).min(dec!(0.05));
                dec!(0.3) + strength * dec!(2)
            } else {
                let weakness = ((state.ema_value - current_price) / state.ema_value).min(dec!(0.05));
                dec!(0.1) - weakness
            };
            score += trend_alignment.max(Decimal::ZERO);
        }

        let vol_score = ((dec!(0.02) - volatility) / dec!(0.02)).max(Decimal::ZERO) * dec!(0.25);
        score += vol_score;

        let stats = self.calculate_trade_statistics(state);
        score += stats.win_rate * dec!(0.2);

        if state.consecutive_wins > 0 {
            let win_momentum = (Decimal::from(state.consecutive_wins) / dec!(5)).min(dec!(1));
            score += win_momentum * dec!(0.15);
        }

        let drawdown_penalty = (state.current_drawdown * dec!(2)).min(dec!(0.3));
        score -= drawdown_penalty;

        if stats.profit_factor > dec!(1.5) {
            score += dec!(0.1);
        }

        score.max(Decimal::ZERO).min(Decimal::ONE)
    }

    fn calculate_trailing_stop(&self, highest_price: &Decimal, profit_percent: Decimal) -> Decimal {
        let trailing_percent = if profit_percent > dec!(0.05) {
            dec!(0.015)
        } else if profit_percent > dec!(0.03) {
            dec!(0.010)
        } else if profit_percent > dec!(0.02) {
            dec!(0.007)
        } else {
            dec!(0.005)
        };
        
        highest_price * (dec!(1) - trailing_percent)
    }

    fn update_price_history(&self, state: &mut StrategyState, timestamp: u64, price: Decimal) {
        state.price_history.push_back((timestamp, price));
        
        let cutoff_time = timestamp.saturating_sub(300000);
        while let Some((ts, _)) = state.price_history.front() {
            if *ts < cutoff_time {
                state.price_history.pop_front();
            } else {
                break;
            }
        }
    }

    fn invalidate_volatility_cache(&self, state: &mut StrategyState) {
        if let Some((cache_time, _)) = state.volatility_cache {
            if cache_time.elapsed().as_millis() as u64 > 5000 {
                state.volatility_cache = None;
            }
        }
    }

    fn calculate_trade_statistics(&self, state: &StrategyState) -> TradeStatistics {
        let recent_trades: Vec<&TradeMetrics> = state.trade_history
            .iter()
            .rev()
            .take(50)
            .collect();

        if recent_trades.is_empty() {
            return TradeStatistics::default();
        }

        let wins: Vec<&TradeMetrics> = recent_trades.iter()
            .filter(|t| t.is_win)
            .copied()
            .collect();

        let losses: Vec<&TradeMetrics> = recent_trades.iter()
            .filter(|t| !t.is_win)
            .copied()
            .collect();

        let win_rate = Decimal::from(wins.len()) / Decimal::from(recent_trades.len());
        
        let avg_win = if !wins.is_empty() {
            wins.iter().map(|t| t.pnl).sum::<Decimal>() / Decimal::from(wins.len())
        } else {
            dec!(0.01)
        };

        let avg_loss = if !losses.is_empty() {
            losses.iter().map(|t| t.pnl.abs()).sum::<Decimal>() / Decimal::from(losses.len())
        } else {
            dec!(0.01)
        };

        let gross_profit = wins.iter().map(|t| t.pnl).sum::<Decimal>();
        let gross_loss = losses.iter().map(|t| t.pnl.abs()).sum::<Decimal>();
        
        let profit_factor = if gross_loss > Decimal::ZERO {
            gross_profit / gross_loss
        } else if gross_profit > Decimal::ZERO {
            dec!(999)
        } else {
            dec!(1)
        };

        let returns: Vec<Decimal> = recent_trades.iter().map(|t| t.pnl).collect();
        let sharpe_ratio = self.calculate_sharpe_ratio(&returns);

        TradeStatistics {
            win_rate,
            avg_win,
            avg_loss,
            profit_factor,
            sharpe_ratio,
        }
    }

    fn calculate_sharpe_ratio(&self, returns: &[Decimal]) -> Decimal {
        if returns.len() < 2 {
            return Decimal::ZERO;
        }

        let mean_return = returns.iter().sum::<Decimal>() / Decimal::from(returns.len());
        let variance = returns
            .iter()
            .map(|r| (*r - mean_return).powi(2))
            .sum::<Decimal>() / Decimal::from(returns.len());

        let std_dev = sqrt_decimal(variance);
        
        if std_dev > Decimal::ZERO {
            mean_return / std_dev * dec!(15.87)
        } else {
            Decimal::ZERO
        }
    }

    fn record_trade(&self, state: &mut StrategyState, trade: TradeMetrics, config: &MartingaleConfig) {
        state.total_pnl += trade.pnl;
        state.total_gas_spent += trade.gas_cost;
        // Bump the last trade instant for cooldowns; keep UNIX timestamp only inside TradeMetrics for audit
        state.last_trade_instant = Instant::now();
        
        state.trade_history.push_back(trade);
        if state.trade_history.len() > config.max_trade_history {
            state.trade_history.pop_front();
        }
    }

    fn update_position_sizing(&self, state: &mut StrategyState, is_win: bool, config: &MartingaleConfig) {
        if is_win {
            state.consecutive_wins += 1;
            state.consecutive_losses = 0;
            state.current_position_size = config.base_position_size;
        } else {
            state.consecutive_losses += 1;
            state.consecutive_wins = 0;
            
            if state.consecutive_losses < config.max_consecutive_losses {
                state.current_position_size = (state.current_position_size * config.multiplier)
                    .min(config.max_position_size);
            } else {
                state.current_position_size = config.base_position_size;
                state.consecutive_losses = 0;
            }
        }
    }

    fn update_drawdown(&self, state: &mut StrategyState) {
        // Equity and peak_balance are in consistent quote units
        if state.equity > state.peak_balance {
            state.peak_balance = state.equity;
            state.current_drawdown = Decimal::ZERO;
        } else {
            state.current_drawdown = (state.peak_balance - state.equity) / state.peak_balance;
        }
    }

    fn check_circuit_breaker(&self, state: &mut StrategyState, config: &MartingaleConfig) {
        if state.current_drawdown >= config.circuit_breaker_threshold {
            state.circuit_breaker_triggered = true;
        }

        let recent_losses = state.trade_history
            .iter()
            .rev()
            .take(10)
            .filter(|t| !t.is_win)
            .count();

        if recent_losses >= 8 {
            state.circuit_breaker_triggered = true;
        }

        let recent_pnl: Decimal = state.trade_history
            .iter()
            .rev()
            .take(20)
            .map(|t| t.pnl)
            .sum();

        // Equity-based thresholding for recent PnL shock
        if recent_pnl < -(state.equity * config.circuit_breaker_threshold) {
            state.circuit_breaker_triggered = true;
        }
    }

    pub async fn reset_circuit_breaker(&self) {
        let mut state = self.state.write().await;
        state.circuit_breaker_triggered = false;
        state.consecutive_losses = 0;
        state.current_position_size = self.config.read().await.base_position_size;
    }

    pub async fn get_performance_metrics(&self) -> PerformanceMetrics {
        let state = self.state.read().await;
        let stats = self.calculate_trade_statistics(&state);
        
        let total_trades = state.trade_history.len();
        let winning_trades = state.trade_history.iter().filter(|t| t.is_win).count();
        
        let (max_consecutive_wins, max_consecutive_losses) = self.calculate_max_streaks(&state);

        let avg_trade_duration = if !state.trade_history.is_empty() {
            state.trade_history.iter()
                .map(|t| t.execution_time_ms)
                .sum::<u64>() / state.trade_history.len() as u64
        } else {
            0
        };

        PerformanceMetrics {
            total_pnl: state.total_pnl,
            total_trades,
            winning_trades,
            win_rate: stats.win_rate,
            profit_factor: stats.profit_factor,
            sharpe_ratio: stats.sharpe_ratio,
            max_drawdown: state.peak_balance.checked_sub(state.equity)
                .unwrap_or(Decimal::ZERO) / state.peak_balance,
            current_drawdown: state.current_drawdown,
            avg_trade_duration_ms: avg_trade_duration,
            total_gas_spent: state.total_gas_spent,
            current_position_size: state.current_position_size,
            confidence_score: state.confidence_score,
            circuit_breaker_active: state.circuit_breaker_triggered,
            consecutive_losses: state.consecutive_losses,
            consecutive_wins: state.consecutive_wins,
            max_consecutive_wins,
            max_consecutive_losses,
            avg_win: stats.avg_win,
            avg_loss: stats.avg_loss,
        }
    }

    fn calculate_max_streaks(&self, state: &StrategyState) -> (u32, u32) {
        let mut max_wins = 0u32;
        let mut max_losses = 0u32;
        let mut current_wins = 0u32;
        let mut current_losses = 0u32;

        for trade in &state.trade_history {
            if trade.is_win {
                current_wins += 1;
                current_losses = 0;
                max_wins = max_wins.max(current_wins);
            } else {
                current_losses += 1;
                current_wins = 0;
                max_losses = max_losses.max(current_losses);
            }
        }

        (max_wins, max_losses)
    }

    pub async fn adjust_parameters_adaptive(&self) {
        let metrics = self.get_performance_metrics().await;
        let mut config = self.config.write().await;

        if metrics.total_trades < 20 {
            return;
        }

        if metrics.win_rate < dec!(0.4) {
            config.profit_target_percent = (config.profit_target_percent * dec!(0.95)).max(dec!(0.008));
            config.stop_loss_percent = (config.stop_loss_percent * dec!(1.05)).min(dec!(0.01));
        } else if metrics.win_rate > dec!(0.65) {
            config.profit_target_percent = (config.profit_target_percent * dec!(1.02)).min(dec!(0.025));
        }

        if metrics.sharpe_ratio < dec!(0.5) {
            config.kelly_fraction = (config.kelly_fraction * dec!(0.9)).max(dec!(0.1));
        } else if metrics.sharpe_ratio > dec!(2.0) {
            config.kelly_fraction = (config.kelly_fraction * dec!(1.1)).min(dec!(0.4));
        }

        if metrics.current_drawdown > dec!(0.08) {
            config.base_position_size = (config.base_position_size * dec!(0.8)).max(dec!(0.0005));
            config.max_position_size = (config.max_position_size * dec!(0.8)).max(dec!(0.01));
        }

        if metrics.consecutive_losses >= 4 {
            config.multiplier = (config.multiplier * dec!(0.95)).max(dec!(1.5));
        } else if metrics.consecutive_wins >= 5 && metrics.profit_factor > dec!(2) {
            config.multiplier = (config.multiplier * dec!(1.05)).min(dec!(2.5));
        }

        if metrics.avg_trade_duration_ms > 20000 {
            config.position_timeout_ms = (config.position_timeout_ms * 9 / 10).max(15000);
        }

        let volatility = {
            let mut state = self.state.write().await;
            self.calculate_volatility(&mut state, &config)
        };

        if volatility > dec!(0.03) {
            config.confidence_threshold = (config.confidence_threshold + dec!(0.05)).min(dec!(0.8));
        } else if volatility < dec!(0.01) {
            config.confidence_threshold = (config.confidence_threshold - dec!(0.05)).max(dec!(0.5));
        }
    }

    pub async fn validate_market_conditions(
        &self,
        bid_ask_spread: Decimal,
        volume_24h: Decimal,
        price_impact: Decimal,
    ) -> bool {
        let config = self.config.read().await;
        let mut state = self.state.write().await;

        if bid_ask_spread > config.slippage_tolerance * dec!(2) {
            return false;
        }

        if volume_24h < config.min_liquidity_threshold * dec!(10) {
            return false;
        }

        if price_impact > config.slippage_tolerance {
            return false;
        }

        let volatility = self.calculate_volatility(&mut state, &config);
        if volatility > dec!(0.05) {
            return false;
        }

        if state.circuit_breaker_triggered {
            return false;
        }

        true
    }

    pub async fn emergency_close_position(&self, current_price: Decimal) -> Result<bool> {
        let mut state = self.state.write().await;
        
        if let Some(position) = &state.active_position {
            let pnl_percent = (current_price - position.entry_price) / position.entry_price;
            
            if pnl_percent < dec!(-0.02) || state.circuit_breaker_triggered {
                state.active_position = None;
                state.current_position_size = self.config.read().await.base_position_size;
                state.consecutive_losses = 0;
                // Reentry lock to avoid immediate whipsaw
                let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_millis() as u64;
                state.reentry_lock_until_ms = now.saturating_add(30_000);
                return Ok(true);
            }
        }
        
        Ok(false)
    }

    pub async fn get_position_recommendation(&self, market_data: &MarketData) -> PositionRecommendation {
        let config = self.config.read().await;
        let mut state = self.state.write().await;
        
        let volatility = self.calculate_volatility(&mut state, &config);
        let confidence = state.confidence_score;
        
        let risk_score = self.calculate_risk_score(&state, volatility, market_data);
        let opportunity_score = self.calculate_opportunity_score(&state, market_data, confidence);
        
        let recommended_size = if risk_score < dec!(0.3) && opportunity_score > dec!(0.7) {
            state.current_position_size * dec!(1.2)
        } else if risk_score > dec!(0.7) || opportunity_score < dec!(0.3) {
            state.current_position_size * dec!(0.5)
        } else {
            state.current_position_size
        };
        
        PositionRecommendation {
            action: if opportunity_score > confidence && risk_score < dec!(0.5) {
                RecommendedAction::Enter
            } else if state.active_position.is_some() && risk_score > dec!(0.8) {
                RecommendedAction::Exit
            } else {
                RecommendedAction::Hold
            },
            size: recommended_size.min(config.max_position_size),
            confidence: opportunity_score * (dec!(1) - risk_score),
            risk_level: risk_score,
        }
    }

    fn calculate_risk_score(
        &self,
        state: &StrategyState,
        volatility: Decimal,
        market_data: &MarketData,
    ) -> Decimal {
        let mut risk = Decimal::ZERO;
        
        risk += volatility * dec!(10);
        risk += state.current_drawdown * dec!(5);
        risk += Decimal::from(state.consecutive_losses) * dec!(0.1);
        
        if market_data.volume_24h < market_data.avg_volume_7d * dec!(0.5) {
            risk += dec!(0.2);
        }
        
        if market_data.bid_ask_spread > dec!(0.002) {
            risk += dec!(0.15);
        }
        
        risk.min(Decimal::ONE)
    }

    fn calculate_opportunity_score(
        &self,
        state: &StrategyState,
        market_data: &MarketData,
        confidence: Decimal,
    ) -> Decimal {
        let mut opportunity = confidence;
        
        if market_data.price < state.ema_value * dec!(0.98) {
            opportunity += dec!(0.2);
        }
        
        if market_data.rsi < dec!(30) {
            opportunity += dec!(0.15);
        } else if market_data.rsi > dec!(70) {
            opportunity -= dec!(0.15);
        }
        
        let volume_ratio = market_data.volume_24h / market_data.avg_volume_7d;
        if volume_ratio > dec!(1.5) {
            opportunity += dec!(0.1);
        }
        
        opportunity.max(Decimal::ZERO).min(Decimal::ONE)
    }
}

#[derive(Debug, Clone)]
pub struct TradeStatistics {
    pub win_rate: Decimal,
    pub avg_win: Decimal,
    pub avg_loss: Decimal,
    pub profit_factor: Decimal,
    pub sharpe_ratio: Decimal,
}

impl Default for TradeStatistics {
    fn default() -> Self {
        Self {
            win_rate: dec!(0.5),
            avg_win: dec!(0.01),
            avg_loss: dec!(0.01),
            profit_factor: dec!(1),
            sharpe_ratio: Decimal::ZERO,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub total_pnl: Decimal,
    pub total_trades: usize,
    pub winning_trades: usize,
    pub win_rate: Decimal,
    pub profit_factor: Decimal,
    pub sharpe_ratio: Decimal,
    pub max_drawdown: Decimal,
    pub current_drawdown: Decimal,
    pub avg_trade_duration_ms: u64,
    pub total_gas_spent: Decimal,
    pub current_position_size: Decimal,
    pub confidence_score: Decimal,
    pub circuit_breaker_active: bool,
    pub consecutive_losses: u32,
    pub consecutive_wins: u32,
    pub max_consecutive_wins: u32,
    pub max_consecutive_losses: u32,
    pub avg_win: Decimal,
    pub avg_loss: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ExitReason {
    None,
    StopLoss,
    TakeProfit,
    TrailingStop,
    Timeout,
}

#[derive(Debug, Clone)]
pub struct MarketData {
    pub price: Decimal,
    pub volume_24h: Decimal,
    pub avg_volume_7d: Decimal,
    pub bid_ask_spread: Decimal,
    pub rsi: Decimal,
}

#[derive(Debug, Clone)]
pub struct PositionRecommendation {
    pub action: RecommendedAction,
    pub size: Decimal,
    pub confidence: Decimal,
    pub risk_level: Decimal,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RecommendedAction {
    Enter,
    Exit,
    Hold,
}

impl MartingaleOptimizer {
    pub async fn health_check(&self) -> HealthStatus {
        let state = self.state.read().await;
        let config = self.config.read().await;
        
        let is_healthy = !state.circuit_breaker_triggered 
            && state.current_drawdown < config.max_drawdown_percent
            && state.total_pnl > -config.circuit_breaker_threshold;
        
        HealthStatus {
            is_healthy,
            circuit_breaker_active: state.circuit_breaker_triggered,
            current_drawdown: state.current_drawdown,
            total_pnl: state.total_pnl,
            last_trade_age_ms: state.last_trade_instant.elapsed().as_millis() as u64,
            active_position: state.active_position.is_some(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub is_healthy: bool,
    pub circuit_breaker_active: bool,
    pub current_drawdown: Decimal,
    pub total_pnl: Decimal,
    pub last_trade_age_ms: u64,
    pub active_position: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_martingale_optimizer_initialization() {
        let config = MartingaleConfig::default();
        let optimizer = MartingaleOptimizer::new(config);
        
        let metrics = optimizer.get_performance_metrics().await;
        assert_eq!(metrics.total_pnl, Decimal::ZERO);
        assert_eq!(metrics.total_trades, 0);
        assert!(!metrics.circuit_breaker_active);
    }

    #[tokio::test]
    async fn test_position_entry_validation() {
        let config = MartingaleConfig::default();
        let optimizer = MartingaleOptimizer::new(config);
        
        let result = optimizer.should_enter_position(
            dec!(100),
            dec!(10000),
            0.1,
        ).await;
        
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_circuit_breaker() {
        let mut config = MartingaleConfig::default();
        config.circuit_breaker_threshold = dec!(0.05);
        let optimizer = MartingaleOptimizer::new(config);
        
        let health = optimizer.health_check().await;
        assert!(health.is_healthy);
        assert!(!health.circuit_breaker_active);
    }

    #[tokio::test]
    async fn size_dampens_with_volatility() {
        let opt = MartingaleOptimizer::new(MartingaleConfig::default());
        {
            let mut st = opt.state.write().await;
            for _ in 0..10 {
                st.trade_history.push_back(TradeMetrics {
                    timestamp: 0,
                    position_size: dec!(0.01),
                    entry_price: dec!(100),
                    exit_price: Some(dec!(101)),
                    pnl: dec!(0.01),
                    is_win: true,
                    slippage: dec!(0),
                    gas_cost: dec!(0),
                    execution_time_ms: 5,
                });
            }
        }
        let cfg = opt.config.read().await.clone();
        let st = opt.state.read().await.clone();
        let s_low = opt.calculate_optimal_position_size(&cfg, &st, dec!(100), dec!(0.005), 0.1, Decimal::ONE);
        let s_high = opt.calculate_optimal_position_size(&cfg, &st, dec!(100), dec!(0.05), 0.1, Decimal::ONE);
        assert!(s_high < s_low);
    }
}
