use anchor_lang::prelude::*;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType},
};
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::Signature,
};
use std::{
    collections::{HashMap, VecDeque, BTreeMap},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
    str::FromStr,
};
use tokio::sync::{mpsc, RwLock as TokioRwLock};
use borsh::{BorshDeserialize, BorshSerialize};
use bytemuck::{Pod, Zeroable};

const GAMMA_THRESHOLD: f64 = 0.85;
const DELTA_HEDGE_THRESHOLD: f64 = 0.70;
const VOLUME_SPIKE_MULTIPLIER: f64 = 2.5;
const PRICE_MOMENTUM_THRESHOLD: f64 = 0.03;
const MAX_EXPOSURE_RATIO: f64 = 0.25;
const GAMMA_ACCELERATION_THRESHOLD: f64 = 1.2;
const VEGA_IMPACT_THRESHOLD: f64 = 0.15;
const CHARM_DECAY_THRESHOLD: f64 = 0.08;
const MIN_LIQUIDITY_DEPTH: u64 = 1_000_000;
const SQUEEZE_CONFIDENCE_THRESHOLD: f64 = 0.92;
const RISK_FREE_RATE: f64 = 0.0525;
const SECONDS_PER_YEAR: f64 = 31536000.0;
const OPTION_CONTRACT_SIZE: u64 = 100;
const MAX_POSITION_AGE_MS: u64 = 5000;
const FLOW_HISTORY_SIZE: usize = 500;
const PRICE_PRECISION: u64 = 1_000_000;

#[repr(C)]
#[derive(Clone, Copy, Debug, Pod, Zeroable)]
pub struct OptionAccountData {
    pub discriminator: [u8; 8],
    pub strike_price: u64,
    pub expiry_timestamp: i64,
    pub is_call: u8,
    pub underlying_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub open_interest: u64,
    pub volume_24h: u64,
    pub implied_volatility_bps: u16,
    pub last_trade_price: u64,
    pub bid_price: u64,
    pub ask_price: u64,
    pub delta_bps: i16,
    pub gamma_bps: u16,
    pub last_update_slot: u64,
    pub _padding: [u8; 32],
}

#[derive(Debug, Clone)]
pub struct OptionPosition {
    pub pubkey: Pubkey,
    pub strike_price: f64,
    pub expiry: i64,
    pub is_call: bool,
    pub open_interest: u64,
    pub volume: u64,
    pub implied_volatility: f64,
    pub delta: f64,
    pub gamma: f64,
    pub vega: f64,
    pub theta: f64,
    pub rho: f64,
    pub charm: f64,
    pub vanna: f64,
    pub vomma: f64,
    pub last_update: i64,
    pub bid_ask_spread: f64,
}

#[derive(Debug, Clone)]
pub struct MarketMetrics {
    pub spot_price: f64,
    pub volume_24h: u64,
    pub volatility_realized: f64,
    pub volatility_implied: f64,
    pub skew: f64,
    pub kurtosis: f64,
    pub bid_ask_spread: f64,
    pub order_book_imbalance: f64,
    pub net_gamma_exposure: f64,
    pub net_delta_exposure: f64,
    pub pin_risk: f64,
    pub timestamp: i64,
    pub slot: u64,
}

#[derive(Debug, Clone)]
pub struct GammaSqueezeSignal {
    pub confidence: f64,
    pub expected_move: f64,
    pub time_to_expiry: i64,
    pub trigger_price: f64,
    pub stop_loss: f64,
    pub take_profit: f64,
    pub position_size: f64,
    pub risk_reward_ratio: f64,
    pub market_impact: f64,
    pub execution_urgency: f64,
    pub strike: u64,
    pub direction: i8,
    pub max_slippage_bps: u16,
}

pub struct GammaSqueezeDetector {
    rpc_client: Arc<RpcClient>,
    options_program_id: Pubkey,
    spot_oracle_pubkey: Pubkey,
    option_positions: Arc<TokioRwLock<HashMap<Pubkey, OptionPosition>>>,
    market_metrics: Arc<TokioRwLock<VecDeque<MarketMetrics>>>,
    gamma_profile: Arc<TokioRwLock<BTreeMap<u64, f64>>>,
    signal_sender: mpsc::UnboundedSender<GammaSqueezeSignal>,
    volatility_surface: Arc<TokioRwLock<HashMap<(u64, i64), f64>>>,
    hedging_flows: Arc<TokioRwLock<VecDeque<(i64, f64)>>>,
    pin_strikes: Arc<TokioRwLock<Vec<u64>>>,
    last_update_slot: Arc<TokioRwLock<u64>>,
}

impl GammaSqueezeDetector {
    pub fn new(
        rpc_url: &str,
        options_program_id: &str,
        spot_oracle_pubkey: &str,
        signal_sender: mpsc::UnboundedSender<GammaSqueezeSignal>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            rpc_client: Arc::new(RpcClient::new_with_commitment(
                rpc_url.to_string(),
                CommitmentConfig::processed(),
            )),
            options_program_id: Pubkey::from_str(options_program_id)?,
            spot_oracle_pubkey: Pubkey::from_str(spot_oracle_pubkey)?,
            option_positions: Arc::new(TokioRwLock::new(HashMap::with_capacity(1000))),
            market_metrics: Arc::new(TokioRwLock::new(VecDeque::with_capacity(1000))),
            gamma_profile: Arc::new(TokioRwLock::new(BTreeMap::new())),
            signal_sender,
            volatility_surface: Arc::new(TokioRwLock::new(HashMap::with_capacity(2000))),
            hedging_flows: Arc::new(TokioRwLock::new(VecDeque::with_capacity(FLOW_HISTORY_SIZE))),
            pin_strikes: Arc::new(TokioRwLock::new(Vec::with_capacity(20))),
            last_update_slot: Arc::new(TokioRwLock::new(0)),
        })
    }

    pub async fn start_monitoring(&self) -> Result<(), Box<dyn std::error::Error>> {
        let detector = Arc::new(self.clone());
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                if let Err(e) = detector.monitoring_cycle().await {
                    eprintln!("Monitoring cycle error: {:?}", e);
                }
            }
        });
        
        Ok(())
    }

    async fn monitoring_cycle(&self) -> Result<(), Box<dyn std::error::Error>> {
        let current_slot = self.rpc_client.get_slot().await?;
        let mut last_slot = self.last_update_slot.write().await;
        
        if current_slot <= *last_slot {
            return Ok(());
        }
        
        *last_slot = current_slot;
        drop(last_slot);
        
        self.update_market_metrics(current_slot).await?;
        self.update_option_positions().await?;
        self.calculate_gamma_exposure().await?;
        self.analyze_hedging_flows().await?;
        self.detect_squeeze_conditions().await?;
        
        Ok(())
    }

    async fn update_market_metrics(&self, slot: u64) -> Result<(), Box<dyn std::error::Error>> {
        let oracle_account = self.rpc_client.get_account(&self.spot_oracle_pubkey).await?;
        let spot_price = self.parse_oracle_price(&oracle_account)?;
        
        let metrics = MarketMetrics {
            spot_price,
            volume_24h: self.calculate_24h_volume().await?,
            volatility_realized: self.calculate_realized_volatility().await?,
            volatility_implied: self.calculate_implied_volatility().await?,
            skew: self.calculate_volatility_skew().await?,
            kurtosis: self.calculate_kurtosis().await?,
            bid_ask_spread: self.calculate_spread().await?,
            order_book_imbalance: self.calculate_order_imbalance().await?,
            net_gamma_exposure: 0.0,
            net_delta_exposure: 0.0,
            pin_risk: 0.0,
            timestamp: chrono::Utc::now().timestamp_millis(),
            slot,
        };
        
        let mut market_metrics = self.market_metrics.write().await;
        market_metrics.push_back(metrics);
        
        if market_metrics.len() > 1000 {
            market_metrics.pop_front();
        }
        
        Ok(())
    }

    fn parse_oracle_price(&self, account: &Account) -> Result<f64, Box<dyn std::error::Error>> {
        if account.data.len() < 16 {
            return Err("Invalid oracle account data".into());
        }
        
        let price_raw = u64::from_le_bytes(account.data[0..8].try_into()?);
        let decimals = account.data[8] as u32;
        
        Ok(price_raw as f64 / 10_f64.powi(decimals as i32))
    }

    async fn update_option_positions(&self) -> Result<(), Box<dyn std::error::Error>> {
        let config = RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::DataSize(
                std::mem::size_of::<OptionAccountData>() as u64
            )]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
                commitment: Some(CommitmentConfig::processed()),
                ..Default::default()
            },
            ..Default::default()
        };
        
        let accounts = self.rpc_client
            .get_program_accounts_with_config(&self.options_program_id, config)
            .await?;
        
        let mut positions = self.option_positions.write().await;
        let now = chrono::Utc::now().timestamp_millis();
        
        for (pubkey, account) in accounts {
            if let Ok(option_data) = self.parse_option_account(&account) {
                let position = self.calculate_option_greeks(pubkey, option_data, now).await?;
                positions.insert(pubkey, position);
            }
        }
        
        positions.retain(|_, p| now - p.last_update < MAX_POSITION_AGE_MS as i64);
        
        Ok(())
    }

    fn parse_option_account(&self, account: &Account) -> Result<OptionAccountData, Box<dyn std::error::Error>> {
        if account.data.len() != std::mem::size_of::<OptionAccountData>() {
            return Err("Invalid option account size".into());
        }
        
        let data = bytemuck::try_from_bytes::<OptionAccountData>(&account.data)
            .map_err(|_| "Failed to parse option account")?;
        
        Ok(*data)
    }

    async fn calculate_option_greeks(
        &self,
        pubkey: Pubkey,
        data: OptionAccountData,
        now: i64,
    ) -> Result<OptionPosition, Box<dyn std::error::Error>> {
        let spot = self.get_current_spot_price().await?;
        let strike = data.strike_price as f64 / PRICE_PRECISION as f64;
        let t = (data.expiry_timestamp - now / 1000) as f64 / SECONDS_PER_YEAR;
        let iv = data.implied_volatility_bps as f64 / 10000.0;
        let r = RISK_FREE_RATE;
        
        if t <= 0.0 {
            return Ok(OptionPosition {
                pubkey,
                strike_price: strike,
                expiry: data.expiry_timestamp,
                is_call: data.is_call != 0,
                open_interest: data.open_interest,
                volume: data.volume_24h,
                implied_volatility: iv,
                delta: 0.0,
                gamma: 0.0,
                vega: 0.0,
                theta: 0.0,
                rho: 0.0,
                charm: 0.0,
                vanna: 0.0,
                vomma: 0.0,
                last_update: now,
                bid_ask_spread: (data.ask_price - data.bid_price) as f64 / PRICE_PRECISION as f64,
            });
        }
        
        let sqrt_t = t.sqrt();
        let d1 = ((spot / strike).ln() + (r + 0.5 * iv * iv) * t) / (iv * sqrt_t);
        let d2 = d1 - iv * sqrt_t;
        
        let n_d1 = self.normal_cdf(d1);
        let n_d2 = self.normal_cdf(d2);
        let n_prime_d1 = self.normal_pdf(d1);
                let n_prime_prime_d1 = -d1 * n_prime_d1;
        
        let exp_rt = (-r * t).exp();
        let exp_qt = (-r * t).exp();
        
        let (delta, gamma, vega, theta, rho) = if data.is_call != 0 {
            let delta = exp_qt * n_d1;
            let gamma = exp_qt * n_prime_d1 / (spot * iv * sqrt_t);
            let vega = spot * exp_qt * n_prime_d1 * sqrt_t / 100.0;
            let theta = -(spot * exp_qt * n_prime_d1 * iv / (2.0 * sqrt_t) 
                + r * strike * exp_rt * n_d2) / 365.0;
            let rho = strike * t * exp_rt * n_d2 / 100.0;
            (delta, gamma, vega, theta, rho)
        } else {
            let delta = exp_qt * (n_d1 - 1.0);
            let gamma = exp_qt * n_prime_d1 / (spot * iv * sqrt_t);
            let vega = spot * exp_qt * n_prime_d1 * sqrt_t / 100.0;
            let theta = -(spot * exp_qt * n_prime_d1 * iv / (2.0 * sqrt_t) 
                - r * strike * exp_rt * (1.0 - n_d2)) / 365.0;
            let rho = -strike * t * exp_rt * (1.0 - n_d2) / 100.0;
            (delta, gamma, vega, theta, rho)
        };
        
        let charm = exp_qt * n_prime_d1 * (2.0 * r * t - d2 * iv * sqrt_t) / (2.0 * t * iv * sqrt_t);
        let vanna = -exp_qt * n_prime_d1 * d2 / iv;
        let vomma = vega * d1 * d2 / iv;
        
        Ok(OptionPosition {
            pubkey,
            strike_price: strike,
            expiry: data.expiry_timestamp,
            is_call: data.is_call != 0,
            open_interest: data.open_interest,
            volume: data.volume_24h,
            implied_volatility: iv,
            delta,
            gamma,
            vega,
            theta,
            rho,
            charm,
            vanna,
            vomma,
            last_update: now,
            bid_ask_spread: (data.ask_price.saturating_sub(data.bid_price)) as f64 / PRICE_PRECISION as f64,
        })
    }

    async fn calculate_gamma_exposure(&self) -> Result<(), Box<dyn std::error::Error>> {
        let positions = self.option_positions.read().await;
        let mut gamma_by_strike: BTreeMap<u64, f64> = BTreeMap::new();
        let spot = self.get_current_spot_price().await?;
        
        let mut total_net_gamma = 0.0;
        let mut total_net_delta = 0.0;
        
        for position in positions.values() {
            let strike_key = (position.strike_price * PRICE_PRECISION as f64) as u64;
            let gamma_exposure = position.gamma * position.open_interest as f64 * OPTION_CONTRACT_SIZE as f64;
            let delta_exposure = position.delta * position.open_interest as f64 * OPTION_CONTRACT_SIZE as f64;
            
            *gamma_by_strike.entry(strike_key).or_insert(0.0) += gamma_exposure;
            total_net_gamma += gamma_exposure;
            total_net_delta += delta_exposure;
        }
        
        let mut gamma_profile = self.gamma_profile.write().await;
        *gamma_profile = gamma_by_strike.clone();
        
        let mut market_metrics = self.market_metrics.write().await;
        if let Some(latest) = market_metrics.back_mut() {
            latest.net_gamma_exposure = total_net_gamma;
            latest.net_delta_exposure = total_net_delta;
        }
        
        for (strike, gamma) in gamma_by_strike {
            let normalized_gamma = gamma / (spot * OPTION_CONTRACT_SIZE as f64);
            if normalized_gamma.abs() > GAMMA_THRESHOLD {
                self.identify_pin_strike(strike, spot).await?;
            }
        }
        
        Ok(())
    }

    async fn analyze_hedging_flows(&self) -> Result<(), Box<dyn std::error::Error>> {
        let positions = self.option_positions.read().await;
        let spot = self.get_current_spot_price().await?;
        
        let mut net_delta = 0.0;
        let mut net_gamma = 0.0;
        let mut net_vanna = 0.0;
        let mut net_charm = 0.0;
        
        for position in positions.values() {
            let multiplier = position.open_interest as f64 * OPTION_CONTRACT_SIZE as f64;
            net_delta += position.delta * multiplier;
            net_gamma += position.gamma * multiplier;
            net_vanna += position.vanna * multiplier;
            net_charm += position.charm * multiplier;
        }
        
        let hedge_requirement = net_delta * spot;
        let gamma_acceleration = net_gamma * spot * spot / 100.0;
        
        let mut hedging_flows = self.hedging_flows.write().await;
        hedging_flows.push_back((chrono::Utc::now().timestamp_millis(), hedge_requirement));
        
        while hedging_flows.len() > FLOW_HISTORY_SIZE {
            hedging_flows.pop_front();
        }
        
        if hedge_requirement.abs() > DELTA_HEDGE_THRESHOLD * spot * 10000.0 {
            self.analyze_squeeze_potential(
                hedge_requirement,
                gamma_acceleration,
                net_vanna,
                net_charm
            ).await?;
        }
        
        Ok(())
    }

    async fn detect_squeeze_conditions(&self) -> Result<(), Box<dyn std::error::Error>> {
        let gamma_profile = self.gamma_profile.read().await;
        let spot = self.get_current_spot_price().await?;
        let metrics = self.get_latest_market_metrics().await?;
        
        for (&strike_raw, &gamma) in gamma_profile.iter() {
            let strike = strike_raw as f64 / PRICE_PRECISION as f64;
            if gamma.abs() < GAMMA_THRESHOLD * spot * OPTION_CONTRACT_SIZE as f64 {
                continue;
            }
            
            let distance_to_strike = (spot - strike).abs() / spot;
            let time_weighted_gamma = self.calculate_time_weighted_gamma(strike_raw).await?;
            let vol_smile_skew = self.calculate_volatility_skew_at_strike(strike_raw).await?;
            
            if distance_to_strike < PRICE_MOMENTUM_THRESHOLD && 
               time_weighted_gamma > GAMMA_ACCELERATION_THRESHOLD {
                let signal = self.generate_squeeze_signal(
                    strike_raw,
                    gamma,
                    time_weighted_gamma,
                    vol_smile_skew,
                    &metrics,
                ).await?;
                
                if signal.confidence > SQUEEZE_CONFIDENCE_THRESHOLD {
                    self.signal_sender.send(signal).map_err(|_| "Failed to send signal")?;
                }
            }
        }
        
        Ok(())
    }

    async fn generate_squeeze_signal(
        &self,
        strike_raw: u64,
        gamma: f64,
        time_weighted_gamma: f64,
        vol_skew: f64,
        metrics: &MarketMetrics,
    ) -> Result<GammaSqueezeSignal, Box<dyn std::error::Error>> {
        let spot = metrics.spot_price;
        let strike = strike_raw as f64 / PRICE_PRECISION as f64;
        let direction = if gamma > 0.0 { 1 } else { -1 };
        
        let gamma_impact = gamma.abs() / (spot * OPTION_CONTRACT_SIZE as f64);
        let momentum_factor = 1.0 + vol_skew.abs() * 0.5;
        let expected_move_pct = gamma_impact * momentum_factor * 0.01;
        let expected_move = spot * expected_move_pct * direction as f64;
        
        let confidence = self.calculate_squeeze_confidence(
            gamma,
            time_weighted_gamma,
            vol_skew,
            metrics.order_book_imbalance,
            metrics.volatility_implied,
        );
        
        let position_size = self.calculate_optimal_position_size(
            confidence,
            expected_move_pct,
            metrics.volume_24h,
            metrics.volatility_realized,
        );
        
        let market_impact_bps = self.estimate_market_impact(position_size, metrics.volume_24h);
        let adjusted_expected_move = expected_move * (1.0 - market_impact_bps / 10000.0);
        
        let risk_reward = (adjusted_expected_move.abs() * 0.8) / 
                         (adjusted_expected_move.abs() * 0.3 + spot * market_impact_bps / 10000.0);
        
        Ok(GammaSqueezeSignal {
            confidence,
            expected_move: adjusted_expected_move,
            time_to_expiry: self.get_nearest_expiry(strike_raw).await?,
            trigger_price: spot + adjusted_expected_move * 0.25,
            stop_loss: spot - adjusted_expected_move.abs() * 0.3 * direction as f64,
            take_profit: spot + adjusted_expected_move * 0.8,
            position_size,
            risk_reward_ratio: risk_reward,
            market_impact: market_impact_bps as f64 / 10000.0,
            execution_urgency: self.calculate_urgency(gamma, time_weighted_gamma, vol_skew),
            strike: strike_raw,
            direction,
            max_slippage_bps: (market_impact_bps * 1.5).min(50) as u16,
        })
    }

    fn calculate_squeeze_confidence(
        &self,
        gamma: f64,
        time_weighted_gamma: f64,
        vol_skew: f64,
        order_imbalance: f64,
        implied_vol: f64,
    ) -> f64 {
        let gamma_score = 1.0 / (1.0 + (-gamma.abs() / (GAMMA_THRESHOLD * 2.0)).exp());
        let acceleration_score = (time_weighted_gamma / GAMMA_ACCELERATION_THRESHOLD).min(1.0);
        let skew_score = 1.0 / (1.0 + (-vol_skew.abs() * 10.0).exp());
        let flow_score = 1.0 / (1.0 + (-order_imbalance.abs() * 5.0).exp());
        let vol_score = (implied_vol * 100.0).min(50.0) / 50.0;
        
        let weights = [0.30, 0.25, 0.20, 0.15, 0.10];
        let scores = [gamma_score, acceleration_score, skew_score, flow_score, vol_score];
        
        scores.iter().zip(weights.iter()).map(|(s, w)| s * w).sum::<f64>().min(0.99)
    }

    fn calculate_optimal_position_size(
        &self,
        confidence: f64,
        expected_move_pct: f64,
        daily_volume: u64,
        realized_vol: f64,
    ) -> f64 {
        let base_size = (daily_volume as f64 * MAX_EXPOSURE_RATIO) / OPTION_CONTRACT_SIZE as f64;
        let confidence_multiplier = confidence.powf(1.5);
        let vol_adjustment = (0.3 / realized_vol).min(2.0).max(0.5);
        let move_adjustment = (expected_move_pct.abs() / 0.02).min(2.0).max(0.5);
        
        (base_size * confidence_multiplier * vol_adjustment * move_adjustment)
            .min(daily_volume as f64 * 0.1)
    }

    fn estimate_market_impact(&self, size: f64, daily_volume: u64) -> f64 {
        let size_ratio = size / daily_volume.max(1) as f64;
        let linear_impact = size_ratio * 1000.0;
        let quadratic_impact = size_ratio * size_ratio * 5000.0;
        
        (linear_impact + quadratic_impact).min(100.0)
    }

    fn calculate_urgency(&self, gamma: f64, time_weighted: f64, vol_skew: f64) -> f64 {
        let gamma_urgency = (gamma.abs() / (GAMMA_THRESHOLD * 2.0)).min(1.0);
        let acceleration_urgency = (time_weighted / GAMMA_ACCELERATION_THRESHOLD).min(1.0);
        let skew_urgency = (vol_skew.abs() * 5.0).min(1.0);
        
        (gamma_urgency * 0.4 + acceleration_urgency * 0.4 + skew_urgency * 0.2).min(1.0)
    }

    async fn get_current_spot_price(&self) -> Result<f64, Box<dyn std::error::Error>> {
        let metrics = self.market_metrics.read().await;
        metrics.back()
            .map(|m| m.spot_price)
            .ok_or_else(|| "No market metrics available".into())
    }

    async fn get_latest_market_metrics(&self) -> Result<MarketMetrics, Box<dyn std::error::Error>> {
        let metrics = self.market_metrics.read().await;
        metrics.back()
            .cloned()
            .ok_or_else(|| "No market metrics available".into())
    }

    async fn get_nearest_expiry(&self, strike_raw: u64) -> Result<i64, Box<dyn std::error::Error>> {
        let positions = self.option_positions.read().await;
        let now = chrono::Utc::now().timestamp();
        
        Ok(positions.values()
                        .filter(|p| (p.strike_price * PRICE_PRECISION as f64) as u64 == strike_raw)
            .map(|p| p.expiry)
            .filter(|&expiry| expiry > now)
            .min()
            .unwrap_or(now + 86400))
    }

    async fn calculate_time_weighted_gamma(&self, strike_raw: u64) -> Result<f64, Box<dyn std::error::Error>> {
        let positions = self.option_positions.read().await;
        let now = chrono::Utc::now().timestamp();
        
        let mut weighted_gamma = 0.0;
        let mut total_weight = 0.0;
        
        for position in positions.values() {
            let position_strike = (position.strike_price * PRICE_PRECISION as f64) as u64;
            if position_strike == strike_raw {
                let time_to_expiry = (position.expiry - now) as f64 / 86400.0;
                if time_to_expiry > 0.0 && time_to_expiry <= 30.0 {
                    let weight = 1.0 / time_to_expiry.sqrt();
                    let gamma_contribution = position.gamma * position.open_interest as f64 * OPTION_CONTRACT_SIZE as f64;
                    weighted_gamma += gamma_contribution * weight;
                    total_weight += weight;
                }
            }
        }
        
        Ok(if total_weight > 0.0 {
            weighted_gamma / total_weight
        } else {
            0.0
        })
    }

    async fn calculate_volatility_skew_at_strike(&self, strike_raw: u64) -> Result<f64, Box<dyn std::error::Error>> {
        let vol_surface = self.volatility_surface.read().await;
        let spot = self.get_current_spot_price().await?;
        let atm_strike = (spot * PRICE_PRECISION as f64) as u64;
        
        let strike_vol = vol_surface.iter()
            .filter(|((s, _), _)| *s == strike_raw)
            .map(|(_, v)| *v)
            .next()
            .unwrap_or(0.30);
        
        let atm_vol = vol_surface.iter()
            .filter(|((s, _), _)| *s == atm_strike)
            .map(|(_, v)| *v)
            .next()
            .unwrap_or(0.30);
        
        Ok((strike_vol - atm_vol) / atm_vol.max(0.01))
    }

    async fn identify_pin_strike(&self, strike: u64, spot: f64) -> Result<(), Box<dyn std::error::Error>> {
        let mut pin_strikes = self.pin_strikes.write().await;
        let strike_price = strike as f64 / PRICE_PRECISION as f64;
        let distance_ratio = (spot - strike_price).abs() / spot;
        
        if distance_ratio < 0.05 && !pin_strikes.contains(&strike) {
            pin_strikes.push(strike);
            pin_strikes.sort_unstable();
            
            if pin_strikes.len() > 20 {
                let mid = pin_strikes.len() / 2;
                let spot_strike = (spot * PRICE_PRECISION as f64) as u64;
                pin_strikes.retain(|&s| (s as i64 - spot_strike as i64).abs() < 10_000_000);
            }
        }
        
        Ok(())
    }

    async fn analyze_squeeze_potential(
        &self,
        hedge_req: f64,
        gamma_accel: f64,
        net_vanna: f64,
        net_charm: f64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let hedging_flows = self.hedging_flows.read().await;
        
        if hedging_flows.len() < 10 {
            return Ok(());
        }
        
        let recent_flows: Vec<f64> = hedging_flows.iter()
            .rev()
            .take(20)
            .map(|(_, flow)| *flow)
            .collect();
        
        let flow_acceleration = self.calculate_flow_acceleration(&recent_flows);
        let vanna_impact = net_vanna.abs() / (self.get_current_spot_price().await? * 1000.0);
        let charm_decay = net_charm.abs() / (self.get_current_spot_price().await? * 100.0);
        
        if flow_acceleration > VOLUME_SPIKE_MULTIPLIER && 
           gamma_accel.abs() > GAMMA_ACCELERATION_THRESHOLD &&
           (vanna_impact > VEGA_IMPACT_THRESHOLD || charm_decay > CHARM_DECAY_THRESHOLD) {
            self.trigger_advanced_analysis(
                hedge_req,
                gamma_accel,
                flow_acceleration,
                vanna_impact,
                charm_decay
            ).await?;
        }
        
        Ok(())
    }

    fn calculate_flow_acceleration(&self, flows: &[f64]) -> f64 {
        if flows.len() < 3 {
            return 0.0;
        }
        
        let mut deltas = Vec::with_capacity(flows.len() - 1);
        for i in 1..flows.len() {
            deltas.push(flows[i] - flows[i-1]);
        }
        
        let avg_delta = deltas.iter().sum::<f64>() / deltas.len() as f64;
        let recent_deltas: Vec<f64> = deltas.iter().rev().take(3).copied().collect();
        let recent_avg = recent_deltas.iter().sum::<f64>() / recent_deltas.len() as f64;
        
        if avg_delta.abs() > 0.0 {
            (recent_avg / avg_delta).abs()
        } else {
            0.0
        }
    }

    async fn trigger_advanced_analysis(
        &self,
        hedge_req: f64,
        gamma_accel: f64,
        flow_accel: f64,
        vanna_impact: f64,
        charm_decay: f64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let spot = self.get_current_spot_price().await?;
        let pin_strikes = self.pin_strikes.read().await;
        let metrics = self.get_latest_market_metrics().await?;
        
        for &strike_raw in pin_strikes.iter() {
            let strike = strike_raw as f64 / PRICE_PRECISION as f64;
            let distance = (spot - strike).abs() / spot;
            
            if distance < PRICE_MOMENTUM_THRESHOLD * 2.0 {
                let enhanced_signal = self.create_enhanced_signal(
                    strike_raw,
                    hedge_req,
                    gamma_accel,
                    flow_accel,
                    vanna_impact,
                    charm_decay,
                    &metrics,
                ).await?;
                
                if enhanced_signal.confidence > SQUEEZE_CONFIDENCE_THRESHOLD * 0.95 {
                    self.signal_sender.send(enhanced_signal).map_err(|_| "Failed to send enhanced signal")?;
                }
            }
        }
        
        Ok(())
    }

    async fn create_enhanced_signal(
        &self,
        strike_raw: u64,
        hedge_req: f64,
        gamma_accel: f64,
        flow_accel: f64,
        vanna_impact: f64,
        charm_decay: f64,
        metrics: &MarketMetrics,
    ) -> Result<GammaSqueezeSignal, Box<dyn std::error::Error>> {
        let spot = metrics.spot_price;
        let strike = strike_raw as f64 / PRICE_PRECISION as f64;
        let direction = if hedge_req > 0.0 { 1 } else { -1 };
        
        let base_magnitude = gamma_accel.abs() * 0.01;
        let flow_multiplier = (flow_accel / VOLUME_SPIKE_MULTIPLIER).min(2.0);
        let greek_multiplier = 1.0 + vanna_impact + charm_decay;
        
        let expected_move_pct = base_magnitude * flow_multiplier * greek_multiplier;
        let expected_move = spot * expected_move_pct * direction as f64;
        
        let confidence = self.calculate_enhanced_confidence(
            gamma_accel,
            flow_accel,
            vanna_impact,
            charm_decay,
            metrics.order_book_imbalance,
        );
        
        let position_size = self.calculate_optimal_position_size(
            confidence,
            expected_move_pct,
            metrics.volume_24h,
            metrics.volatility_realized,
        );
        
        let market_impact_bps = self.estimate_market_impact(position_size, metrics.volume_24h);
        let urgency = (flow_accel / VOLUME_SPIKE_MULTIPLIER).min(1.0);
        
        Ok(GammaSqueezeSignal {
            confidence,
            expected_move,
            time_to_expiry: 86400,
            trigger_price: spot + expected_move * 0.2,
            stop_loss: spot - expected_move.abs() * 0.35 * direction as f64,
            take_profit: spot + expected_move * 0.85,
            position_size,
            risk_reward_ratio: 2.43,
            market_impact: market_impact_bps as f64 / 10000.0,
            execution_urgency: urgency,
            strike: strike_raw,
            direction,
            max_slippage_bps: (market_impact_bps * 1.3).min(40) as u16,
        })
    }

    fn calculate_enhanced_confidence(
        &self,
        gamma_accel: f64,
        flow_accel: f64,
        vanna_impact: f64,
        charm_decay: f64,
        order_imbalance: f64,
    ) -> f64 {
        let gamma_score = (gamma_accel.abs() / (GAMMA_ACCELERATION_THRESHOLD * 2.0)).min(1.0);
        let flow_score = (flow_accel / (VOLUME_SPIKE_MULTIPLIER * 2.0)).min(1.0);
        let vanna_score = (vanna_impact / VEGA_IMPACT_THRESHOLD).min(1.0);
        let charm_score = (charm_decay / CHARM_DECAY_THRESHOLD).min(1.0);
        let imbalance_score = order_imbalance.abs().min(1.0);
        
        let weights = [0.25, 0.30, 0.15, 0.15, 0.15];
        let scores = [gamma_score, flow_score, vanna_score, charm_score, imbalance_score];
        
        scores.iter().zip(weights.iter()).map(|(s, w)| s * w).sum::<f64>().min(0.99)
    }

    async fn calculate_24h_volume(&self) -> Result<u64, Box<dyn std::error::Error>> {
        let positions = self.option_positions.read().await;
        Ok(positions.values().map(|p| p.volume).sum())
    }

    async fn calculate_realized_volatility(&self) -> Result<f64, Box<dyn std::error::Error>> {
        let metrics = self.market_metrics.read().await;
        if metrics.len() < 24 {
            return Ok(0.30);
        }
        
        let prices: Vec<f64> = metrics.iter().rev().take(288).map(|m| m.spot_price).collect();
        let returns: Vec<f64> = prices.windows(2)
            .map(|w| (w[1] / w[0]).ln())
            .collect();
        
        if returns.is_empty() {
            return Ok(0.30);
        }
        
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;
        
        Ok((variance * 365.0).sqrt())
    }

    async fn calculate_implied_volatility(&self) -> Result<f64, Box<dyn std::error::Error>> {
        let positions = self.option_positions.read().await;
        let spot = self.get_current_spot_price().await?;
        
        let atm_vols: Vec<f64> = positions.values()
            .filter(|p| {
                let moneyness = spot / p.strike_price;
                moneyness > 0.95 && moneyness < 1.05
            })
            .map(|p| p.implied_volatility)
            .collect();
        
        Ok(if !atm_vols.is_empty() {
            atm_vols.iter().sum::<f64>() / atm_vols.len() as f64
        } else {
            0.35
        })
    }

    async fn calculate_volatility_skew(&self) -> Result<f64, Box<dyn std::error::Error>> {
        let positions = self.option_positions.read().await;
        let spot = self.get_current_spot_price().await?;
        
        let put_vols: Vec<f64> = positions.values()
            .filter(|p| !p.is_call && p.strike_price < spot * 0.97)
            .map(|p| p.implied_volatility)
            .collect();
        
        let call_vols: Vec<f64> = positions.values()
            .filter(|p| p.is_call && p.strike_price > spot * 1.03)
            .map(|p| p.implied_volatility)
            .collect();
        
        let avg_put_vol = if !put_vols.is_empty() {
            put_vols.iter().sum::<f64>() / put_vols.len() as f64
        } else {
            0.35
        };
        
        let avg_call_vol = if !call_vols.is_empty() {
            call_vols.iter().sum::<f64>() / call_vols.len() as f64
        } else {
            0.35
        };
        
        Ok(avg_put_vol - avg_call_vol)
    }

    async fn calculate_kurtosis(&self) -> Result<f64, Box<dyn std::error::Error>> {
        let metrics = self.market_metrics.read().await;
        if metrics.len() < 100 {
            return Ok(3.0);
        }
        
        let returns: Vec<f64> = metrics.windows(2)
            .map(|w| (w[1].spot_price / w[0].spot_price).ln())
            .collect();
        
                let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let std_dev = (returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64)
            .sqrt();
        
        if std_dev == 0.0 {
            return Ok(3.0);
        }
        
        let kurtosis = returns.iter()
            .map(|r| ((r - mean) / std_dev).powi(4))
            .sum::<f64>() / returns.len() as f64;
        
        Ok(kurtosis)
    }

    async fn calculate_spread(&self) -> Result<f64, Box<dyn std::error::Error>> {
        let positions = self.option_positions.read().await;
        let spot = self.get_current_spot_price().await?;
        
        let atm_spreads: Vec<f64> = positions.values()
            .filter(|p| {
                let moneyness = spot / p.strike_price;
                moneyness > 0.98 && moneyness < 1.02
            })
            .map(|p| p.bid_ask_spread / spot)
            .collect();
        
        Ok(if !atm_spreads.is_empty() {
            atm_spreads.iter().sum::<f64>() / atm_spreads.len() as f64
        } else {
            0.001
        })
    }

    async fn calculate_order_imbalance(&self) -> Result<f64, Box<dyn std::error::Error>> {
        let positions = self.option_positions.read().await;
        let spot = self.get_current_spot_price().await?;
        
        let mut call_oi = 0u64;
        let mut put_oi = 0u64;
        
        for position in positions.values() {
            let moneyness = spot / position.strike_price;
            if moneyness > 0.90 && moneyness < 1.10 {
                if position.is_call {
                    call_oi += position.open_interest;
                } else {
                    put_oi += position.open_interest;
                }
            }
        }
        
        let total_oi = call_oi + put_oi;
        if total_oi > 0 {
            Ok((call_oi as f64 - put_oi as f64) / total_oi as f64)
        } else {
            Ok(0.0)
        }
    }

    fn normal_cdf(&self, x: f64) -> f64 {
        0.5 * (1.0 + self.erf(x / std::f64::consts::SQRT_2))
    }

    fn normal_pdf(&self, x: f64) -> f64 {
        (1.0 / (std::f64::consts::TAU.sqrt())) * (-0.5 * x * x).exp()
    }

    fn erf(&self, x: f64) -> f64 {
        const A1: f64 = 0.254829592;
        const A2: f64 = -0.284496736;
        const A3: f64 = 1.421413741;
        const A4: f64 = -1.453152027;
        const A5: f64 = 1.061405429;
        const P: f64 = 0.3275911;

        let sign = x.signum();
        let x = x.abs();

        let t = 1.0 / (1.0 + P * x);
        let y = 1.0 - (((((A5 * t + A4) * t) + A3) * t + A2) * t + A1) * t * (-x * x).exp();

        sign * y
    }
}

impl Clone for GammaSqueezeDetector {
    fn clone(&self) -> Self {
        Self {
            rpc_client: self.rpc_client.clone(),
            options_program_id: self.options_program_id,
            spot_oracle_pubkey: self.spot_oracle_pubkey,
            option_positions: self.option_positions.clone(),
            market_metrics: self.market_metrics.clone(),
            gamma_profile: self.gamma_profile.clone(),
            signal_sender: self.signal_sender.clone(),
            volatility_surface: self.volatility_surface.clone(),
            hedging_flows: self.hedging_flows.clone(),
            pin_strikes: self.pin_strikes.clone(),
            last_update_slot: self.last_update_slot.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normal_distribution() {
        let detector = GammaSqueezeDetector::new(
            "https://api.mainnet-beta.solana.com",
            "11111111111111111111111111111111",
            "11111111111111111111111111111111",
            mpsc::unbounded_channel().0,
        ).unwrap();
        
        assert!((detector.normal_cdf(0.0) - 0.5).abs() < 0.0001);
        assert!((detector.normal_cdf(1.96) - 0.975).abs() < 0.001);
        assert!((detector.normal_pdf(0.0) - 0.3989).abs() < 0.001);
    }

    #[test]
    fn test_market_impact() {
        let detector = GammaSqueezeDetector::new(
            "https://api.mainnet-beta.solana.com",
            "11111111111111111111111111111111",
            "11111111111111111111111111111111",
            mpsc::unbounded_channel().0,
        ).unwrap();
        
        let impact = detector.estimate_market_impact(1000.0, 1_000_000);
        assert!(impact > 0.0 && impact <= 100.0);
        
        let large_impact = detector.estimate_market_impact(100_000.0, 1_000_000);
        assert_eq!(large_impact, 100.0);
    }

    #[test]
    fn test_flow_acceleration() {
        let detector = GammaSqueezeDetector::new(
            "https://api.mainnet-beta.solana.com",
            "11111111111111111111111111111111",
            "11111111111111111111111111111111",
            mpsc::unbounded_channel().0,
        ).unwrap();
        
        let flows = vec![100.0, 150.0, 250.0, 450.0, 850.0];
        let acceleration = detector.calculate_flow_acceleration(&flows);
        assert!(acceleration > 1.0);
        
        let stable_flows = vec![100.0, 100.0, 100.0, 100.0];
        let stable_accel = detector.calculate_flow_acceleration(&stable_flows);
        assert!(stable_accel.abs() < 0.1);
    }

    #[test]
    fn test_confidence_calculation() {
        let detector = GammaSqueezeDetector::new(
            "https://api.mainnet-beta.solana.com",
            "11111111111111111111111111111111",
            "11111111111111111111111111111111",
            mpsc::unbounded_channel().0,
        ).unwrap();
        
        let confidence = detector.calculate_squeeze_confidence(
            2.0,
            1.5,
            0.1,
            0.2,
            0.35,
        );
        assert!(confidence > 0.5 && confidence < 1.0);
        
        let low_confidence = detector.calculate_squeeze_confidence(
            0.1,
            0.1,
            0.01,
            0.01,
            0.20,
        );
        assert!(low_confidence < 0.5);
    }

    #[test]
    fn test_position_sizing() {
        let detector = GammaSqueezeDetector::new(
            "https://api.mainnet-beta.solana.com",
            "11111111111111111111111111111111",
            "11111111111111111111111111111111",
            mpsc::unbounded_channel().0,
        ).unwrap();
        
        let size = detector.calculate_optimal_position_size(
            0.95,
            0.05,
            10_000_000,
            0.30,
        );
        assert!(size > 0.0);
        assert!(size <= 10_000_000.0 * 0.1);
        
        let conservative_size = detector.calculate_optimal_position_size(
            0.60,
            0.01,
            10_000_000,
            0.50,
        );
        assert!(conservative_size < size);
    }
}

