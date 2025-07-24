use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::collections::{HashMap, VecDeque};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_client::rpc_client::RpcClient;
use serde::{Deserialize, Serialize};
use anyhow::{Result, bail};
use borsh::{BorshDeserialize, BorshSerialize};
use anchor_lang::prelude::*;
use pyth_sdk_solana::state::*;
use switchboard_v2::{AggregatorAccountData, SwitchboardDecimal};

const PYTH_PROGRAM: &str = "FsJ3A3u2vn5cTVofAjvy6y5kwABJAqYWpe4975bi2epH";
const SWITCHBOARD_PROGRAM: &str = "SW1TCH7qEPTdLsDHRgPuMQjbQxKdH2aBStViMFnt64f";

// Token oracle mappings
const ORACLE_MAPPINGS: &[(&str, &str, &str)] = &[
    ("So11111111111111111111111111111111111111112", "H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG", "GvDMxPzN1sCj7L26YDK2HnMRXEQmQ2aemov8YBtPS7vR"),
    ("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Gnt27xtC473ZT2Mw5u8wZ68Z3gULkSTb5DuxJy7eJotD", "HBu55GqEGpFeoJnHHpjvgtRYLDyF9E8hVEr9aERaGCgZ"),
    ("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", "JBu1AL4obBcCMqKBBxhpWCNUt136ijcuMZLFvTP7iWdB", "CZx29wKMUxaJDq6aLVQTdViPL754tTR64NAgQBUGxxHb"),
    ("7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs", "GaBFPKUoAN4Lh3JBmw5XQNF5ceMMRPTnwXaL4xfXwHhq", "6W7E64Gp8NPqFkHqLtdnKUW6KxJN4NqQt5zoHpMmXrLD"),
];

const TOKEN_DECIMALS: &[(&str, u32)] = &[
    ("So11111111111111111111111111111111111111112", 9),
    ("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", 6),
    ("Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB", 6),
    ("7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs", 8),
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskThresholds {
    pub max_position_size: u64,
    pub max_daily_loss: u64,
    pub max_single_loss: u64,
    pub min_success_rate: f64,
    pub max_slippage_bps: u64,
    pub max_priority_fee: u64,
    pub max_consecutive_failures: u32,
    pub volume_spike_threshold: f64,
    pub max_exposure_ratio: f64,
    pub cooldown_ms: u64,
}

impl Default for RiskThresholds {
    fn default() -> Self {
        Self {
            max_position_size: 100_000_000_000,
            max_daily_loss: 10_000_000_000,
            max_single_loss: 1_000_000_000,
            min_success_rate: 0.65,
            max_slippage_bps: 50,
            max_priority_fee: 50_000_000,
            max_consecutive_failures: 5,
            volume_spike_threshold: 3.0,
            max_exposure_ratio: 0.15,
            cooldown_ms: 100,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionMetrics {
    pub timestamp: Instant,
    pub success: bool,
    pub profit_loss: i64,
    pub slippage_bps: u64,
    pub priority_fee: u64,
    pub volume: u64,
    pub execution_time_ms: u64,
    pub token_mint: Pubkey,
}

#[derive(Debug, Clone)]
pub struct TokenExposure {
    pub position: i64,
    pub usd_value: u64,
    pub last_update: Instant,
    pub avg_entry_price: u64,
}

#[derive(Debug)]
pub struct RiskState {
    pub daily_pnl: i64,
    pub success_count: u32,
    pub failure_count: u32,
    pub consecutive_failures: u32,
    pub last_execution: Instant,
    pub execution_history: VecDeque<ExecutionMetrics>,
    pub token_exposures: HashMap<Pubkey, TokenExposure>,
    pub circuit_breaker_active: bool,
    pub circuit_breaker_until: Option<Instant>,
    pub oracle_cache: HashMap<Pubkey, OracleCache>,
}

#[derive(Debug, Clone)]
pub struct OracleCache {
    pub pyth_price: Option<u64>,
    pub switchboard_price: Option<u64>,
    pub timestamp: Instant,
}

pub struct MultiThresholdRiskController {
    thresholds: Arc<RwLock<RiskThresholds>>,
    state: Arc<RwLock<RiskState>>,
    emergency_stop: AtomicBool,
    total_volume_24h: AtomicU64,
    last_reset: Arc<RwLock<Instant>>,
    rpc_client: Arc<RpcClient>,
}

impl MultiThresholdRiskController {
    pub fn new(rpc_client: Arc<RpcClient>, thresholds: Option<RiskThresholds>) -> Self {
        Self {
            thresholds: Arc::new(RwLock::new(thresholds.unwrap_or_default())),
            state: Arc::new(RwLock::new(RiskState {
                daily_pnl: 0,
                success_count: 0,
                failure_count: 0,
                consecutive_failures: 0,
                last_execution: Instant::now(),
                execution_history: VecDeque::with_capacity(1000),
                token_exposures: HashMap::new(),
                circuit_breaker_active: false,
                circuit_breaker_until: None,
                oracle_cache: HashMap::new(),
            })),
            emergency_stop: AtomicBool::new(false),
            total_volume_24h: AtomicU64::new(0),
            last_reset: Arc::new(RwLock::new(Instant::now())),
            rpc_client,
        }
    }

    pub fn can_execute_trade(
        &self,
        token_mint: &Pubkey,
        position_delta: i64,
        expected_slippage_bps: u64,
        priority_fee: u64,
    ) -> Result<bool> {
        if self.emergency_stop.load(Ordering::Acquire) {
            bail!("Emergency stop activated");
        }

        let mut state = self.state.write().unwrap();
        let thresholds = self.thresholds.read().unwrap();

        if let Some(until) = state.circuit_breaker_until {
            if Instant::now() < until {
                bail!("Circuit breaker active");
            }
            state.circuit_breaker_active = false;
            state.circuit_breaker_until = None;
        }

        if state.last_execution.elapsed().as_millis() < thresholds.cooldown_ms as u128 {
            return Ok(false);
        }

        if state.consecutive_failures >= thresholds.max_consecutive_failures {
            self.activate_circuit_breaker(&mut state, Duration::from_secs(60));
            bail!("Max consecutive failures");
        }

        if state.daily_pnl < -(thresholds.max_daily_loss as i64) {
            self.activate_circuit_breaker(&mut state, Duration::from_secs(300));
            bail!("Daily loss limit exceeded");
        }

        let success_rate = self.calculate_success_rate(&state);
        if success_rate < thresholds.min_success_rate && state.success_count + state.failure_count > 20 {
            bail!("Success rate below threshold");
        }

        if expected_slippage_bps > thresholds.max_slippage_bps {
            bail!("Expected slippage too high");
        }

        if priority_fee > thresholds.max_priority_fee {
            bail!("Priority fee too high");
        }

        drop(state);
        
        let position_usd = self.get_position_value_usd(token_mint, position_delta.abs() as u64)?;
        if position_usd > thresholds.max_position_size {
            bail!("Position size exceeds limit");
        }

        let state = self.state.read().unwrap();
        let total_exposure = self.calculate_total_exposure(&state)?;
        if total_exposure > thresholds.max_exposure_ratio {
            bail!("Total exposure exceeds limit");
        }

        if self.detect_volume_spike(&state, thresholds.volume_spike_threshold) {
            bail!("Volume spike detected");
        }

        Ok(true)
    }

    pub fn record_execution(
        &self,
        token_mint: &Pubkey,
        success: bool,
        profit_loss: i64,
        slippage_bps: u64,
        priority_fee: u64,
        volume: u64,
        execution_time_ms: u64,
        final_position: i64,
    ) -> Result<()> {
        let mut state = self.state.write().unwrap();
        let thresholds = self.thresholds.read().unwrap();

        state.execution_history.push_back(ExecutionMetrics {
            timestamp: Instant::now(),
            success,
            profit_loss,
            slippage_bps,
            priority_fee,
            volume,
            execution_time_ms,
            token_mint: *token_mint,
        });

        if state.execution_history.len() > 1000 {
            state.execution_history.pop_front();
        }

        if success {
            state.success_count += 1;
            state.consecutive_failures = 0;
        } else {
            state.failure_count += 1;
            state.consecutive_failures += 1;
        }

        state.daily_pnl += profit_loss;

        if profit_loss < -(thresholds.max_single_loss as i64) {
            self.activate_circuit_breaker(&mut state, Duration::from_secs(120));
        }

        drop(state);
        
        let position_usd = self.get_position_value_usd(token_mint, final_position.abs() as u64)?;
        let oracle_price = self.get_weighted_oracle_price(token_mint)?;
        
        let mut state = self.state.write().unwrap();
        state.token_exposures.insert(
            *token_mint,
            TokenExposure {
                position: final_position,
                usd_value: position_usd,
                last_update: Instant::now(),
                avg_entry_price: oracle_price,
            },
        );

        self.total_volume_24h.fetch_add(volume, Ordering::Relaxed);
        state.last_execution = Instant::now();
        self.clean_old_metrics(&mut state);

        Ok(())
    }

    fn get_position_value_usd(&self, token_mint: &Pubkey, position: u64) -> Result<u64> {
        let price = self.get_weighted_oracle_price(token_mint)?;
        let decimals = self.get_token_decimals(token_mint);
        let normalized = position / 10_u64.pow(decimals.saturating_sub(6));
        Ok(normalized.saturating_mul(price))
    }

    fn get_weighted_oracle_price(&self, token_mint: &Pubkey) -> Result<u64> {
        let mut state = self.state.write().unwrap();
        
        if let Some(cached) = state.oracle_cache.get(token_mint) {
            if cached.timestamp.elapsed() < Duration::from_secs(5) {
                if let Some(pyth) = cached.pyth_price {
                    return Ok(pyth);
                }
                if let Some(sb) = cached.switchboard_price {
                    return Ok(sb);
                }
            }
        }

        let (pyth_oracle, switchboard_oracle) = self.get_oracle_addresses(token_mint)?;
        
        let mut prices = Vec::new();
        let mut weights = Vec::new();

        if let Ok(pyth_data) = self.rpc_client.get_account_data(&pyth_oracle) {
            if let Ok(price_feed) = pyth_sdk_solana::state::load_price_feed_from_account_info(&pyth_oracle, &pyth_data) {
                if let Some(price) = price_feed.get_current_price() {
                    let age = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() as i64 - price_feed.publish_time;
                    if age < 30 {
                        let price_u64 = (price.price.abs() as u64) * 10_u64.pow(6) / 10_u64.pow((-price.expo) as u32);
                        prices.push(price_u64);
                        weights.push(if age < 10 { 3 } else { 2 });
                    }
                }
            }
        }

                if let Ok(sb_data) = self.rpc_client.get_account_data(&switchboard_oracle) {
            if let Ok(aggregator) = AggregatorAccountData::new(&sb_data) {
                if let Ok(result) = aggregator.get_result() {
                    let age = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() - aggregator.latest_confirmed_round.round_open_timestamp as u64;
                    if age < 30 {
                        let price_u64 = result.mantissa.abs() as u64 * 10_u64.pow(6) / 10_u64.pow(result.scale as u32);
                        prices.push(price_u64);
                        weights.push(if age < 10 { 3 } else { 2 });
                    }
                }
            }
        }

        if prices.is_empty() {
            bail!("No valid oracle prices available for {}", token_mint);
        }

        let weighted_sum: u64 = prices.iter().zip(&weights).map(|(p, w)| p * w).sum();
        let total_weight: u64 = weights.iter().sum();
        let final_price = weighted_sum / total_weight;

        state.oracle_cache.insert(
            *token_mint,
            OracleCache {
                pyth_price: prices.first().copied(),
                switchboard_price: prices.get(1).copied(),
                timestamp: Instant::now(),
            },
        );

        Ok(final_price)
    }

    fn get_oracle_addresses(&self, token_mint: &Pubkey) -> Result<(Pubkey, Pubkey)> {
        let mint_str = token_mint.to_string();
        
        for (mint, pyth, switchboard) in ORACLE_MAPPINGS {
            if mint_str == *mint {
                return Ok((
                    pyth.parse::<Pubkey>()?,
                    switchboard.parse::<Pubkey>()?,
                ));
            }
        }
        
        bail!("Unknown token mint: {}", token_mint);
    }

    fn get_token_decimals(&self, token_mint: &Pubkey) -> u32 {
        let mint_str = token_mint.to_string();
        
        for (mint, decimals) in TOKEN_DECIMALS {
            if mint_str == *mint {
                return *decimals;
            }
        }
        
        9 // Default to 9 decimals
    }

    fn detect_volume_spike(&self, state: &RiskState, threshold: f64) -> bool {
        let now = Instant::now();
        let window = Duration::from_secs(300);
        
        let recent_volume: u64 = state.execution_history
            .iter()
            .filter(|m| now.duration_since(m.timestamp) <= window)
            .map(|m| m.volume)
            .sum();
        
        if state.execution_history.len() < 10 {
            return false;
        }
        
        let historical_avg: u64 = state.execution_history
            .iter()
            .map(|m| m.volume)
            .sum::<u64>() / state.execution_history.len() as u64;
        
        recent_volume > (historical_avg as f64 * threshold) as u64
    }

    fn calculate_success_rate(&self, state: &RiskState) -> f64 {
        let total = state.success_count + state.failure_count;
        if total == 0 {
            return 1.0;
        }
        state.success_count as f64 / total as f64
    }

    fn calculate_total_exposure(&self, state: &RiskState) -> Result<f64> {
        let mut total_usd = 0u64;
        
        for (token_mint, exposure) in &state.token_exposures {
            if exposure.position != 0 && exposure.last_update.elapsed() < Duration::from_secs(300) {
                total_usd = total_usd.saturating_add(exposure.usd_value);
            }
        }
        
        let thresholds = self.thresholds.read().unwrap();
        Ok(total_usd as f64 / thresholds.max_position_size as f64)
    }

    fn activate_circuit_breaker(&self, state: &mut RiskState, duration: Duration) {
        state.circuit_breaker_active = true;
        state.circuit_breaker_until = Some(Instant::now() + duration);
    }

    fn clean_old_metrics(&self, state: &mut RiskState) {
        let cutoff = Instant::now() - Duration::from_secs(3600);
        state.execution_history.retain(|m| m.timestamp > cutoff);
        
        state.token_exposures.retain(|_, exposure| {
            exposure.last_update.elapsed() < Duration::from_secs(300) && exposure.position != 0
        });
        
        state.oracle_cache.retain(|_, cache| {
            cache.timestamp.elapsed() < Duration::from_secs(60)
        });
    }

    pub fn update_thresholds(&self, new_thresholds: RiskThresholds) -> Result<()> {
        if new_thresholds.max_position_size == 0 {
            bail!("Invalid max position size");
        }
        if new_thresholds.min_success_rate > 1.0 || new_thresholds.min_success_rate < 0.0 {
            bail!("Invalid success rate threshold");
        }
        if new_thresholds.max_consecutive_failures == 0 {
            bail!("Invalid max consecutive failures");
        }
        
        *self.thresholds.write().unwrap() = new_thresholds;
        Ok(())
    }

    pub fn emergency_stop(&self) {
        self.emergency_stop.store(true, Ordering::Release);
        let mut state = self.state.write().unwrap();
        self.activate_circuit_breaker(&mut state, Duration::from_secs(3600));
    }

    pub fn resume_trading(&self) {
        self.emergency_stop.store(false, Ordering::Release);
        let mut state = self.state.write().unwrap();
        state.circuit_breaker_active = false;
        state.circuit_breaker_until = None;
        state.consecutive_failures = 0;
    }

    pub fn reset_daily_stats(&self) {
        let mut state = self.state.write().unwrap();
        let mut last_reset = self.last_reset.write().unwrap();
        
        if last_reset.elapsed() >= Duration::from_secs(86400) {
            state.daily_pnl = 0;
            state.success_count = 0;
            state.failure_count = 0;
            state.consecutive_failures = 0;
            self.total_volume_24h.store(0, Ordering::Relaxed);
            *last_reset = Instant::now();
        }
    }

    pub fn validate_transaction(&self, compute_units: u32, priority_fee: u64, accounts: usize) -> Result<()> {
        if compute_units > 1_400_000 {
            bail!("Compute units exceed maximum");
        }
        
        let thresholds = self.thresholds.read().unwrap();
        if priority_fee > thresholds.max_priority_fee {
            bail!("Priority fee exceeds threshold");
        }
        
        if accounts > 64 {
            bail!("Too many accounts in transaction");
        }
        
        Ok(())
    }

    pub fn should_use_jito(&self, opportunity_value: u64) -> bool {
        let state = self.state.read().unwrap();
        
        opportunity_value > 1_000_000_000 || 
        state.consecutive_failures > 2 || 
        self.calculate_success_rate(&state) < 0.7
    }

    pub fn get_dynamic_slippage(&self, base_slippage_bps: u64) -> u64 {
        let state = self.state.read().unwrap();
        let thresholds = self.thresholds.read().unwrap();
        
        let recent_slippages: Vec<u64> = state.execution_history
            .iter()
            .rev()
            .take(10)
            .filter(|m| m.success)
            .map(|m| m.slippage_bps)
            .collect();
        
        if recent_slippages.is_empty() {
            return base_slippage_bps;
        }
        
        let avg_slippage = recent_slippages.iter().sum::<u64>() / recent_slippages.len() as u64;
        
        let adjusted = if avg_slippage > base_slippage_bps {
            base_slippage_bps.saturating_mul(120) / 100
        } else {
            base_slippage_bps
        };
        
        adjusted.min(thresholds.max_slippage_bps)
    }

    pub fn get_priority_fee_recommendation(&self) -> u64 {
        let state = self.state.read().unwrap();
        let thresholds = self.thresholds.read().unwrap();
        
        let base_fee = 10_000;
        let success_rate = self.calculate_success_rate(&state);
        let network_condition = self.check_network_conditions();
        
        let rate_multiplier = match success_rate {
            r if r < 0.5 => 300,
            r if r < 0.7 => 200,
            r if r > 0.9 => 50,
            _ => 100,
        };
        
        let network_multiplier = match network_condition {
            NetworkCondition::Congested => 250,
            NetworkCondition::Busy => 150,
            NetworkCondition::Normal => 100,
        };
        
        let fee = base_fee
            .saturating_mul(rate_multiplier)
            .saturating_mul(network_multiplier) / 10000;
            
        fee.min(thresholds.max_priority_fee)
    }

    pub fn check_network_conditions(&self) -> NetworkCondition {
        let state = self.state.read().unwrap();
        
        let recent: Vec<&ExecutionMetrics> = state.execution_history
            .iter()
            .rev()
            .take(20)
            .collect();
            
        if recent.is_empty() {
            return NetworkCondition::Normal;
        }
        
        let failures = recent.iter().filter(|m| !m.success).count();
        let avg_time = recent.iter().map(|m| m.execution_time_ms).sum::<u64>() / recent.len() as u64;
        
        if failures > 15 || avg_time > 5000 {
            NetworkCondition::Congested
        } else if failures > 10 || avg_time > 2000 {
            NetworkCondition::Busy
        } else {
            NetworkCondition::Normal
        }
    }

    pub fn get_execution_delay_ms(&self) -> u64 {
        let condition = self.check_network_conditions();
        let base_delay = self.thresholds.read().unwrap().cooldown_ms;
        
        match condition {
            NetworkCondition::Normal => base_delay,
            NetworkCondition::Busy => base_delay.saturating_mul(2),
            NetworkCondition::Congested => base_delay.saturating_mul(5),
        }
    }

    pub fn get_risk_status(&self) -> RiskStatus {
        let state = self.state.read().unwrap();
        
        RiskStatus {
            emergency_stop: self.emergency_stop.load(Ordering::Acquire),
            circuit_breaker_active: state.circuit_breaker_active,
            circuit_breaker_until: state.circuit_breaker_until,
            daily_pnl: state.daily_pnl,
            success_rate: self.calculate_success_rate(&state),
            consecutive_failures: state.consecutive_failures,
            total_exposure: self.calculate_total_exposure(&state).unwrap_or(0.0),
            total_volume_24h: self.total_volume_24h.load(Ordering::Relaxed),
            active_positions: state.token_exposures.len(),
            network_condition: self.check_network_conditions(),
        }
    }

    pub fn export_metrics(&self) -> Result<String> {
        let status = self.get_risk_status();
        
        Ok(serde_json::to_string_pretty(&serde_json::json!({
            "timestamp": SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            "daily_pnl": status.daily_pnl,
            "success_rate": status.success_rate,
            "consecutive_failures": status.consecutive_failures,
            "total_exposure": status.total_exposure,
            "total_volume_24h": status.total_volume_24h,
            "active_positions": status.active_positions,
            "circuit_breaker": status.circuit_breaker_active,
            "emergency_stop": status.emergency_stop,
            "network_condition": format!("{:?}", status.network_condition),
        }))?)
    }

    pub fn should_close_position(&self, token_mint: &Pubkey) -> Result<bool> {
        let state = self.state.read().unwrap();
        let thresholds = self.thresholds.read().unwrap();
        
        if let Some(exposure) = state.token_exposures.get(token_mint) {
            let current_price = self.get_weighted_oracle_price(token_mint)?;
            let pnl = if exposure.position > 0 {
                ((current_price as i64 - exposure.avg_entry_price as i64) * exposure.position) / 1_000_000
            } else {
                ((exposure.avg_entry_price as i64 - current_price as i64) * exposure.position.abs()) / 1_000_000
            };
            
            if pnl < -(exposure.usd_value as i64 / 50) {
                return Ok(true);
            }
            
            if exposure.last_update.elapsed() > Duration::from_secs(1800) {
                return Ok(true);
            }
            
            if state.daily_pnl + pnl < -(thresholds.max_daily_loss as i64 * 8 / 10) {
                return Ok(true);
            }
        }
        
        Ok(false)
    }

    pub fn get_max_position_for_token(&self, token_mint: &Pubkey) -> Result<u64> {
        let state = self.state.read().unwrap();
        let thresholds = self.thresholds.read().unwrap();
        
        let current_exposure = self.calculate_total_exposure(&state)?;
        let remaining_ratio = (thresholds.max_exposure_ratio - current_exposure).max(0.0);
        
                let mint_str = token_mint.to_string();
        let token_limit = match mint_str.as_str() {
            "So11111111111111111111111111111111111111112" => thresholds.max_position_size / 2,
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" => thresholds.max_position_size / 3,
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB" => thresholds.max_position_size / 4,
            _ => thresholds.max_position_size / 5,
        };
        
        let available = (thresholds.max_position_size as f64 * remaining_ratio) as u64;
        Ok(available.min(token_limit))
    }

    pub fn adjust_for_market_impact(&self, token_mint: &Pubkey, size: u64) -> u64 {
        let state = self.state.read().unwrap();
        
        let recent_volume: u64 = state.execution_history
            .iter()
            .rev()
            .take(50)
            .filter(|m| &m.token_mint == token_mint)
            .map(|m| m.volume)
            .sum();
        
        if recent_volume == 0 {
            return size / 2;
        }
        
        let avg_trade_size = recent_volume / 50.max(1);
        let impact_factor = (size as f64 / avg_trade_size as f64).min(3.0);
        
        if impact_factor > 2.0 {
            size * 2 / 3
        } else if impact_factor > 1.5 {
            size * 4 / 5
        } else {
            size
        }
    }

    pub fn get_position_pnl(&self, token_mint: &Pubkey) -> Result<i64> {
        let state = self.state.read().unwrap();
        
        if let Some(exposure) = state.token_exposures.get(token_mint) {
            let current_price = self.get_weighted_oracle_price(token_mint)?;
            let entry_value = (exposure.position.abs() as u64) * exposure.avg_entry_price / 1_000_000;
            let current_value = (exposure.position.abs() as u64) * current_price / 1_000_000;
            
            let pnl = if exposure.position > 0 {
                current_value as i64 - entry_value as i64
            } else {
                entry_value as i64 - current_value as i64
            };
            
            Ok(pnl)
        } else {
            Ok(0)
        }
    }

    pub fn is_safe_to_execute(&self, token_mint: &Pubkey) -> Result<bool> {
        let state = self.state.read().unwrap();
        
        // Check oracle freshness
        if let Some(cache) = state.oracle_cache.get(token_mint) {
            if cache.timestamp.elapsed() > Duration::from_secs(30) {
                return Ok(false);
            }
        }
        
        // Check recent failures for this token
        let recent_failures = state.execution_history
            .iter()
            .rev()
            .take(10)
            .filter(|m| &m.token_mint == token_mint && !m.success)
            .count();
            
        if recent_failures > 3 {
            return Ok(false);
        }
        
        // Check if we have conflicting positions
        if let Some(exposure) = state.token_exposures.get(token_mint) {
            if exposure.position != 0 && exposure.last_update.elapsed() < Duration::from_secs(5) {
                return Ok(false);
            }
        }
        
        Ok(true)
    }

    pub fn get_recent_performance(&self, window_secs: u64) -> PerformanceMetrics {
        let state = self.state.read().unwrap();
        let cutoff = Instant::now() - Duration::from_secs(window_secs);
        
        let recent: Vec<&ExecutionMetrics> = state.execution_history
            .iter()
            .filter(|m| m.timestamp > cutoff)
            .collect();
        
        let total_trades = recent.len() as u32;
        let successful_trades = recent.iter().filter(|m| m.success).count() as u32;
        let total_pnl: i64 = recent.iter().map(|m| m.profit_loss).sum();
        let total_volume: u64 = recent.iter().map(|m| m.volume).sum();
        let avg_slippage = if !recent.is_empty() {
            recent.iter().map(|m| m.slippage_bps).sum::<u64>() / recent.len() as u64
        } else {
            0
        };
        
        PerformanceMetrics {
            total_trades,
            successful_trades,
            success_rate: if total_trades > 0 {
                successful_trades as f64 / total_trades as f64
            } else {
                0.0
            },
            total_pnl,
            total_volume,
            avg_slippage_bps: avg_slippage,
        }
    }

    pub fn health_check(&self) -> HealthStatus {
        let state = self.state.read().unwrap();
        let thresholds = self.thresholds.read().unwrap();
        
        let mut issues = Vec::new();
        
        if self.emergency_stop.load(Ordering::Acquire) {
            issues.push("Emergency stop active".to_string());
        }
        
        if state.circuit_breaker_active {
            issues.push("Circuit breaker active".to_string());
        }
        
        if state.daily_pnl < -(thresholds.max_daily_loss as i64 / 2) {
            issues.push(format!("Daily PnL at risk: {}", state.daily_pnl));
        }
        
        if state.consecutive_failures >= thresholds.max_consecutive_failures - 1 {
            issues.push(format!("High consecutive failures: {}", state.consecutive_failures));
        }
        
        let success_rate = self.calculate_success_rate(&state);
        if success_rate < thresholds.min_success_rate {
            issues.push(format!("Low success rate: {:.2}%", success_rate * 100.0));
        }
        
        HealthStatus {
            is_healthy: issues.is_empty(),
            issues,
            last_execution: state.last_execution,
            uptime_seconds: state.last_execution.elapsed().as_secs(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RiskStatus {
    pub emergency_stop: bool,
    pub circuit_breaker_active: bool,
    pub circuit_breaker_until: Option<Instant>,
    pub daily_pnl: i64,
    pub success_rate: f64,
    pub consecutive_failures: u32,
    pub total_exposure: f64,
    pub total_volume_24h: u64,
    pub active_positions: usize,
    pub network_condition: NetworkCondition,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NetworkCondition {
    Normal,
    Busy,
    Congested,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub total_trades: u32,
    pub successful_trades: u32,
    pub success_rate: f64,
    pub total_pnl: i64,
    pub total_volume: u64,
    pub avg_slippage_bps: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthStatus {
    pub is_healthy: bool,
    pub issues: Vec<String>,
    pub last_execution: Instant,
    pub uptime_seconds: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_risk_controller_initialization() {
        let rpc = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com"));
        let controller = MultiThresholdRiskController::new(rpc, None);
        let status = controller.get_risk_status();
        
        assert!(!status.emergency_stop);
        assert!(!status.circuit_breaker_active);
        assert_eq!(status.daily_pnl, 0);
    }

    #[test]
    fn test_trade_validation() {
        let rpc = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com"));
        let controller = MultiThresholdRiskController::new(rpc, None);
        let token = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
        
        let result = controller.can_execute_trade(&token, 1000, 25, 10_000);
        assert!(result.is_ok());
    }

    #[test]
    fn test_circuit_breaker_activation() {
        let rpc = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com"));
        let controller = MultiThresholdRiskController::new(rpc, None);
        
        controller.emergency_stop();
        let status = controller.get_risk_status();
        assert!(status.emergency_stop);
    }

    #[test]
    fn test_slippage_adjustment() {
        let rpc = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com"));
        let controller = MultiThresholdRiskController::new(rpc, None);
        
        let adjusted = controller.get_dynamic_slippage(30);
        assert!(adjusted >= 30);
        assert!(adjusted <= 50);
    }

    #[test]
    fn test_position_limits() {
        let rpc = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com"));
        let controller = MultiThresholdRiskController::new(rpc, None);
        let token = Pubkey::from_str("So11111111111111111111111111111111111111112").unwrap();
        
        match controller.get_max_position_for_token(&token) {
            Ok(max_pos) => assert!(max_pos > 0),
            Err(_) => panic!("Failed to get max position"),
        }
    }
}

