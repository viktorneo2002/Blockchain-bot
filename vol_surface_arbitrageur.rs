use anchor_client::{solana_sdk::commitment_config::CommitmentConfig, Client, Cluster};
use anchor_lang::prelude::*;
use ordered_float::OrderedFloat;
use rayon::prelude::*;
use serum_dex::{
    instruction::SelfTradeBehavior,
    matching::{OrderType, Side},
    state::{Market, OpenOrders},
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    transaction::Transaction,
};
use spl_associated_token_account::get_associated_token_address;
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, RwLock};

const SPREAD_THRESHOLD_BPS: u64 = 20;
const MIN_PROFIT_BPS: u64 = 25;
const MAX_POSITION_USD: u64 = 500_000;
const VOL_CONVERGENCE_EPSILON: f64 = 0.0001;
const SABR_MAX_ITERATIONS: usize = 100;
const SURFACE_UPDATE_INTERVAL_MS: u64 = 250;
const RISK_FREE_RATE: f64 = 0.0525;
const MAX_DELTA_EXPOSURE: f64 = 0.1;
const MAX_GAMMA_EXPOSURE: f64 = 0.05;
const MAX_VEGA_EXPOSURE: f64 = 10000.0;
const PRIORITY_FEE_LAMPORTS: u64 = 50000;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Clone, Debug)]
pub struct OptionData {
    pub strike: f64,
    pub expiry: i64,
    pub is_call: bool,
    pub bid: f64,
    pub ask: f64,
    pub mid: f64,
    pub volume: u64,
    pub open_interest: u64,
    pub implied_vol: f64,
    pub delta: f64,
    pub gamma: f64,
    pub vega: f64,
    pub theta: f64,
    pub underlying_price: f64,
    pub market_pubkey: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
}

#[derive(Clone)]
pub struct VolatilitySurface {
    pub spot_price: f64,
    pub risk_free_rate: f64,
    pub dividend_yield: f64,
    pub strikes: Vec<f64>,
    pub expiries: Vec<f64>,
    pub vols: BTreeMap<(OrderedFloat<f64>, OrderedFloat<f64>), f64>,
    pub sabr_params: HashMap<OrderedFloat<f64>, SabrParams>,
    pub last_update: Instant,
}

#[derive(Clone, Debug)]
pub struct SabrParams {
    pub alpha: f64,
    pub beta: f64,
    pub rho: f64,
    pub nu: f64,
    pub forward: f64,
    pub expiry: f64,
}

#[derive(Clone)]
pub struct ArbitragePosition {
    pub id: [u8; 32],
    pub legs: Vec<OptionLeg>,
    pub entry_time: Instant,
    pub target_profit: f64,
    pub stop_loss: f64,
    pub net_delta: f64,
    pub net_gamma: f64,
    pub net_vega: f64,
    pub capital_required: u64,
}

#[derive(Clone)]
pub struct OptionLeg {
    pub option: OptionData,
    pub quantity: f64,
    pub side: TradeSide,
    pub executed_price: f64,
}

#[derive(Clone, Copy, PartialEq)]
pub enum TradeSide {
    Buy,
    Sell,
}

pub struct VolSurfaceArbitrageur {
    rpc: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    options_cache: Arc<RwLock<HashMap<Pubkey, OptionData>>>,
    vol_surface: Arc<RwLock<VolatilitySurface>>,
    positions: Arc<Mutex<HashMap<[u8; 32], ArbitragePosition>>>,
    oracle_pubkey: Pubkey,
}

impl VolSurfaceArbitrageur {
    pub fn new(rpc_url: &str, wallet: Keypair, oracle_pubkey: Pubkey) -> Self {
        Self {
            rpc: Arc::new(RpcClient::new_with_commitment(
                rpc_url.to_string(),
                CommitmentConfig::confirmed(),
            )),
            wallet: Arc::new(wallet),
            options_cache: Arc::new(RwLock::new(HashMap::new())),
            vol_surface: Arc::new(RwLock::new(VolatilitySurface {
                spot_price: 0.0,
                risk_free_rate: RISK_FREE_RATE,
                dividend_yield: 0.0,
                strikes: Vec::new(),
                expiries: Vec::new(),
                vols: BTreeMap::new(),
                sabr_params: HashMap::new(),
                last_update: Instant::now(),
            })),
            positions: Arc::new(Mutex::new(HashMap::new())),
            oracle_pubkey,
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut surface_interval = tokio::time::interval(Duration::from_millis(SURFACE_UPDATE_INTERVAL_MS));
        let mut arb_interval = tokio::time::interval(Duration::from_millis(50));

        loop {
            tokio::select! {
                _ = surface_interval.tick() => {
                    if let Err(e) = self.update_volatility_surface().await {
                        eprintln!("Surface update error: {}", e);
                    }
                }
                _ = arb_interval.tick() => {
                    if let Err(e) = self.scan_and_execute().await {
                        eprintln!("Arbitrage scan error: {}", e);
                    }
                }
            }
        }
    }

    async fn update_volatility_surface(&self) -> Result<()> {
        let spot_price = self.fetch_spot_price().await?;
        let options = self.fetch_all_options().await?;
        
        let mut surface = self.vol_surface.write().await;
        surface.spot_price = spot_price;
        surface.strikes.clear();
        surface.expiries.clear();
        surface.vols.clear();

        let mut unique_strikes = std::collections::HashSet::new();
        let mut unique_expiries = std::collections::HashSet::new();

        for option in &options {
            unique_strikes.insert(OrderedFloat(option.strike));
            unique_expiries.insert(OrderedFloat(option.expiry as f64));
            
            let key = (OrderedFloat(option.strike), OrderedFloat(option.expiry as f64));
            surface.vols.insert(key, option.implied_vol);
        }

        surface.strikes = unique_strikes.into_iter().map(|s| s.0).collect();
        surface.strikes.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        surface.expiries = unique_expiries.into_iter().map(|e| e.0).collect();
        surface.expiries.sort_by(|a, b| a.partial_cmp(b).unwrap());

        self.calibrate_sabr_surface(&mut surface).await?;
        surface.last_update = Instant::now();

        let mut cache = self.options_cache.write().await;
        cache.clear();
        for option in options {
            cache.insert(option.market_pubkey, option);
        }

        Ok(())
    }

    async fn fetch_spot_price(&self) -> Result<f64> {
        let account = self.rpc.get_account(&self.oracle_pubkey).await?;
        let price_data = &account.data[16..24];
        let raw_price = i64::from_le_bytes(price_data.try_into()?);
        Ok(raw_price as f64 / 1e6)
    }

    async fn fetch_all_options(&self) -> Result<Vec<OptionData>> {
        let option_programs = vec![
            "ZETAxsqBRek56DhiGXrn75yj2NHU3aYUnxvHXpkf3aD".parse()?,
            "PSYoPtmvtxDvVR5W4QSv5sHgYXhHvKwJkw2zUjdKtVU".parse()?,
        ];

        let mut all_options = Vec::new();

        for program_id in option_programs {
            let accounts = self.rpc.get_program_accounts(&program_id).await?;
            
            for (pubkey, account) in accounts {
                if let Ok(option) = self.parse_option_account(&account.data, pubkey).await {
                    all_options.push(option);
                }
            }
        }

        Ok(all_options)
    }

    async fn parse_option_account(&self, data: &[u8], pubkey: Pubkey) -> Result<OptionData> {
        if data.len() < 128 {
            return Err("Invalid option account data".into());
        }

        let discriminator = &data[0..8];
        let strike = f64::from_le_bytes(data[8..16].try_into()?);
        let expiry = i64::from_le_bytes(data[16..24].try_into()?);
        let is_call = data[24] == 1;
        let base_mint = Pubkey::new(&data[25..57]);
        let quote_mint = Pubkey::new(&data[57..89]);

        let market_data = self.fetch_market_data(&pubkey).await?;
        let spot_price = self.fetch_spot_price().await?;
        
        let time_to_expiry = ((expiry - chrono::Utc::now().timestamp()) as f64).max(0.0) / 31536000.0;
        
        let implied_vol = self.newton_raphson_iv(
            market_data.2,
            spot_price,
            strike,
            time_to_expiry,
            RISK_FREE_RATE,
            is_call,
        )?;

        let greeks = self.calculate_greeks(spot_price, strike, time_to_expiry, RISK_FREE_RATE, implied_vol, is_call);

        Ok(OptionData {
            strike,
            expiry,
            is_call,
            bid: market_data.0,
            ask: market_data.1,
            mid: market_data.2,
            volume: market_data.3,
            open_interest: market_data.4,
            implied_vol,
            delta: greeks.0,
            gamma: greeks.1,
            vega: greeks.2,
            theta: greeks.3,
            underlying_price: spot_price,
            market_pubkey: pubkey,
            base_mint,
            quote_mint,
        })
    }

    async fn fetch_market_data(&self, market_pubkey: &Pubkey) -> Result<(f64, f64, f64, u64, u64)> {
        let market_account = self.rpc.get_account(market_pubkey).await?;
        let market = Market::load(&market_account.data, market_pubkey, true)?;

        let bids_account = self.rpc.get_account(&market.bids).await?;
        let asks_account = self.rpc.get_account(&market.asks).await?;

        let bids = market.load_bids_mut(&bids_account.data)?;
        let asks = market.load_asks_mut(&asks_account.data)?;

        let best_bid = bids.iter().next().map(|o| o.price() as f64 / 1e6).unwrap_or(0.0);
        let best_ask = asks.iter().next().map(|o| o.price() as f64 / 1e6).unwrap_or(f64::MAX);
        let mid = if best_bid > 0.0 && best_ask < f64::MAX {
            (best_bid + best_ask) / 2.0
        } else {
            0.0
        };

        let volume = market.native_pc_total as u64;
        let open_interest = 0u64;

        Ok((best_bid, best_ask, mid, volume, open_interest))
    }

    fn newton_raphson_iv(&self, price: f64, spot: f64, strike: f64, time: f64, rate: f64, is_call: bool) -> Result<f64> {
        if time <= 0.0 || price <= 0.0 {
            return Ok(0.2);
        }

        let mut vol = 0.3;
        let mut iterations = 0;

        while iterations < 100 {
            let bs_price = self.black_scholes(spot, strike, time, rate, vol, is_call);
            let vega = self.calculate_vega(spot, strike, time, rate, vol);

            let diff = bs_price - price;
            if diff.abs() < VOL_CONVERGENCE_EPSILON || vega.abs() < 1e-10 {
                break;
            }

            vol -= diff / vega;
            vol = vol.max(0.01).min(5.0);
            iterations += 1;
        }

        Ok(vol)
    }

    fn black_scholes(&self, s: f64, k: f64, t: f64, r: f64, v: f64, is_call: bool) -> f64 {
        if t <= 0.0 {
            return (s - k).max(0.0);
        }

        let d1 = ((s / k).ln() + (r + 0.5 * v * v) * t) / (v * t.sqrt());
        let d2 = d1 - v * t.sqrt();

        if is_call {
            s * self.norm_cdf(d1) - k * (-r * t).exp() * self.norm_cdf(d2)
        } else {
                        k * (-r * t).exp() * self.norm_cdf(-d2) - s * self.norm_cdf(-d1)
        }
    }

    fn norm_cdf(&self, x: f64) -> f64 {
        0.5 * (1.0 + libm::erf(x / std::f64::consts::SQRT_2))
    }

    fn norm_pdf(&self, x: f64) -> f64 {
        (-0.5 * x * x).exp() / (2.0 * std::f64::consts::PI).sqrt()
    }

    fn calculate_greeks(&self, s: f64, k: f64, t: f64, r: f64, v: f64, is_call: bool) -> (f64, f64, f64, f64) {
        if t <= 0.0 {
            return (0.0, 0.0, 0.0, 0.0);
        }

        let sqrt_t = t.sqrt();
        let d1 = ((s / k).ln() + (r + 0.5 * v * v) * t) / (v * sqrt_t);
        let d2 = d1 - v * sqrt_t;
        
        let delta = if is_call {
            self.norm_cdf(d1)
        } else {
            self.norm_cdf(d1) - 1.0
        };
        
        let gamma = self.norm_pdf(d1) / (s * v * sqrt_t);
        let vega = s * self.norm_pdf(d1) * sqrt_t / 100.0;
        
        let theta = if is_call {
            (-s * self.norm_pdf(d1) * v / (2.0 * sqrt_t) - r * k * (-r * t).exp() * self.norm_cdf(d2)) / 365.0
        } else {
            (-s * self.norm_pdf(d1) * v / (2.0 * sqrt_t) + r * k * (-r * t).exp() * self.norm_cdf(-d2)) / 365.0
        };

        (delta, gamma, vega, theta)
    }

    fn calculate_vega(&self, s: f64, k: f64, t: f64, r: f64, v: f64) -> f64 {
        if t <= 0.0 {
            return 0.0;
        }
        let d1 = ((s / k).ln() + (r + 0.5 * v * v) * t) / (v * t.sqrt());
        s * self.norm_pdf(d1) * t.sqrt()
    }

    async fn calibrate_sabr_surface(&self, surface: &mut VolatilitySurface) -> Result<()> {
        let expiry_groups: HashMap<OrderedFloat<f64>, Vec<(f64, f64)>> = surface.vols.iter()
            .map(|((strike, expiry), vol)| (*expiry, (strike.0, *vol)))
            .fold(HashMap::new(), |mut map, (exp, data)| {
                map.entry(exp).or_insert_with(Vec::new).push(data);
                map
            });

        surface.sabr_params.clear();

        for (expiry, strikes_vols) in expiry_groups {
            if strikes_vols.len() >= 3 {
                let params = self.fit_sabr_params(surface.spot_price, expiry.0, &strikes_vols)?;
                surface.sabr_params.insert(expiry, params);
            }
        }

        Ok(())
    }

    fn fit_sabr_params(&self, spot: f64, expiry: f64, strikes_vols: &[(f64, f64)]) -> Result<SabrParams> {
        let time_to_expiry = expiry / 365.0;
        let forward = spot * (RISK_FREE_RATE * time_to_expiry).exp();
        
        let mut alpha = strikes_vols.iter().map(|(_, v)| *v).sum::<f64>() / strikes_vols.len() as f64;
        let beta = 0.5;
        let mut rho = 0.0;
        let mut nu = 0.4;

        for iteration in 0..SABR_MAX_ITERATIONS {
            let mut gradient = [0.0; 3];
            let mut hessian = [[0.0; 3]; 3];

            for (strike, market_vol) in strikes_vols {
                let model_vol = self.sabr_vol(forward, *strike, time_to_expiry, alpha, beta, rho, nu);
                let residual = model_vol - market_vol;
                
                let h = 1e-6;
                let dalpha = (self.sabr_vol(forward, *strike, time_to_expiry, alpha + h, beta, rho, nu) 
                    - self.sabr_vol(forward, *strike, time_to_expiry, alpha - h, beta, rho, nu)) / (2.0 * h);
                let drho = (self.sabr_vol(forward, *strike, time_to_expiry, alpha, beta, rho + h, nu) 
                    - self.sabr_vol(forward, *strike, time_to_expiry, alpha, beta, rho - h, nu)) / (2.0 * h);
                let dnu = (self.sabr_vol(forward, *strike, time_to_expiry, alpha, beta, rho, nu + h) 
                    - self.sabr_vol(forward, *strike, time_to_expiry, alpha, beta, rho, nu - h)) / (2.0 * h);

                gradient[0] += residual * dalpha;
                gradient[1] += residual * drho;
                gradient[2] += residual * dnu;

                hessian[0][0] += dalpha * dalpha;
                hessian[0][1] += dalpha * drho;
                hessian[0][2] += dalpha * dnu;
                hessian[1][0] += drho * dalpha;
                hessian[1][1] += drho * drho;
                hessian[1][2] += drho * dnu;
                hessian[2][0] += dnu * dalpha;
                hessian[2][1] += dnu * drho;
                hessian[2][2] += dnu * dnu;
            }

            let det = hessian[0][0] * (hessian[1][1] * hessian[2][2] - hessian[1][2] * hessian[2][1])
                - hessian[0][1] * (hessian[1][0] * hessian[2][2] - hessian[1][2] * hessian[2][0])
                + hessian[0][2] * (hessian[1][0] * hessian[2][1] - hessian[1][1] * hessian[2][0]);

            if det.abs() < 1e-10 {
                break;
            }

            let inv_hessian = [
                [
                    (hessian[1][1] * hessian[2][2] - hessian[1][2] * hessian[2][1]) / det,
                    (hessian[0][2] * hessian[2][1] - hessian[0][1] * hessian[2][2]) / det,
                    (hessian[0][1] * hessian[1][2] - hessian[0][2] * hessian[1][1]) / det,
                ],
                [
                    (hessian[1][2] * hessian[2][0] - hessian[1][0] * hessian[2][2]) / det,
                    (hessian[0][0] * hessian[2][2] - hessian[0][2] * hessian[2][0]) / det,
                    (hessian[0][2] * hessian[1][0] - hessian[0][0] * hessian[1][2]) / det,
                ],
                [
                    (hessian[1][0] * hessian[2][1] - hessian[1][1] * hessian[2][0]) / det,
                    (hessian[0][1] * hessian[2][0] - hessian[0][0] * hessian[2][1]) / det,
                    (hessian[0][0] * hessian[1][1] - hessian[0][1] * hessian[1][0]) / det,
                ],
            ];

            let delta_alpha = -(inv_hessian[0][0] * gradient[0] + inv_hessian[0][1] * gradient[1] + inv_hessian[0][2] * gradient[2]);
            let delta_rho = -(inv_hessian[1][0] * gradient[0] + inv_hessian[1][1] * gradient[1] + inv_hessian[1][2] * gradient[2]);
            let delta_nu = -(inv_hessian[2][0] * gradient[0] + inv_hessian[2][1] * gradient[1] + inv_hessian[2][2] * gradient[2]);

            alpha = (alpha + 0.5 * delta_alpha).max(0.001).min(2.0);
            rho = (rho + 0.5 * delta_rho).max(-0.999).min(0.999);
            nu = (nu + 0.5 * delta_nu).max(0.001).min(3.0);

            if gradient.iter().map(|g| g.abs()).sum::<f64>() < 1e-6 {
                break;
            }
        }

        Ok(SabrParams {
            alpha,
            beta,
            rho,
            nu,
            forward,
            expiry: time_to_expiry,
        })
    }

    fn sabr_vol(&self, f: f64, k: f64, t: f64, alpha: f64, beta: f64, rho: f64, nu: f64) -> f64 {
        if (f - k).abs() < 1e-10 {
            let v0 = alpha / f.powf(1.0 - beta);
            let term1 = 1.0 + t * ((1.0 - beta).powi(2) * alpha.powi(2) / (24.0 * f.powf(2.0 - 2.0 * beta))
                + 0.25 * rho * beta * nu * alpha / f.powf(1.0 - beta)
                + (2.0 - 3.0 * rho.powi(2)) * nu.powi(2) / 24.0);
            return v0 * term1;
        }

        let fk_mid = (f * k).powf(0.5);
        let log_fk = (f / k).ln();
        let z = nu / alpha * fk_mid.powf(1.0 - beta) * log_fk;
        let x = ((1.0 - 2.0 * rho * z + z.powi(2)).sqrt() + z - rho) / (1.0 - rho);

        let term1 = alpha / (fk_mid.powf(1.0 - beta) * (1.0 + (1.0 - beta).powi(2) * log_fk.powi(2) / 24.0
            + (1.0 - beta).powi(4) * log_fk.powi(4) / 1920.0));
        let term2 = z / x.ln();
        let term3 = 1.0 + t * ((1.0 - beta).powi(2) * alpha.powi(2) / (24.0 * fk_mid.powf(2.0 - 2.0 * beta))
            + 0.25 * rho * beta * nu * alpha / fk_mid.powf(1.0 - beta)
            + (2.0 - 3.0 * rho.powi(2)) * nu.powi(2) / 24.0);

        term1 * term2 * term3
    }

    async fn scan_and_execute(&self) -> Result<()> {
        let surface = self.vol_surface.read().await;
        let options = self.options_cache.read().await;

        if surface.last_update.elapsed() > Duration::from_secs(5) {
            return Ok(());
        }

        let opportunities = self.find_arbitrage_opportunities(&surface, &options)?;

        for opp in opportunities.iter().take(3) {
            if self.validate_opportunity(opp).await? {
                self.execute_arbitrage(opp).await?;
            }
        }

        self.manage_positions().await?;

        Ok(())
    }

    fn find_arbitrage_opportunities(&self, surface: &VolatilitySurface, options: &HashMap<Pubkey, OptionData>) -> Result<Vec<ArbitragePosition>> {
        let mut opportunities = Vec::new();

        let options_vec: Vec<_> = options.values().collect();
        
        for i in 0..options_vec.len() {
            for j in i + 1..options_vec.len() {
                let opt1 = options_vec[i];
                let opt2 = options_vec[j];

                if let Some(arb) = self.check_calendar_spread(opt1, opt2, surface)? {
                    opportunities.push(arb);
                }

                if let Some(arb) = self.check_volatility_smile_arb(opt1, opt2, surface)? {
                    opportunities.push(arb);
                }

                if opt1.expiry == opt2.expiry {
                    if let Some(arb) = self.check_butterfly_spread(opt1, opt2, &options_vec, surface)? {
                        opportunities.push(arb);
                    }
                }
            }
        }

        opportunities.sort_by(|a, b| {
            let a_score = a.target_profit / a.capital_required as f64;
            let b_score = b.target_profit / b.capital_required as f64;
            b_score.partial_cmp(&a_score).unwrap_or(Ordering::Equal)
        });

        Ok(opportunities)
    }

    fn check_calendar_spread(&self, near: &OptionData, far: &OptionData, surface: &VolatilitySurface) -> Result<Option<ArbitragePosition>> {
        if near.strike != far.strike || near.is_call != far.is_call || near.expiry >= far.expiry {
            return Ok(None);
        }

                let near_fair_vol = self.get_surface_vol(surface, near.strike, near.expiry)?;
        let far_fair_vol = self.get_surface_vol(surface, far.strike, far.expiry)?;

        let near_mispricing = near.implied_vol - near_fair_vol;
        let far_mispricing = far.implied_vol - far_fair_vol;

        if (near_mispricing * far_mispricing) < 0.0 && (near_mispricing - far_mispricing).abs() > 0.03 {
            let (buy_leg, sell_leg) = if near_mispricing < far_mispricing {
                (near, far)
            } else {
                (far, near)
            };

            let position_size = 1.0;
            let capital_required = ((buy_leg.ask * position_size + sell_leg.underlying_price * 0.15 * position_size) * 1e9) as u64;
            let target_profit = (near_mispricing - far_mispricing).abs() * buy_leg.vega * position_size * 100.0;

            if target_profit > capital_required as f64 * MIN_PROFIT_BPS as f64 / 10000.0 {
                let mut id = [0u8; 32];
                id[..8].copy_from_slice(&chrono::Utc::now().timestamp_nanos().to_le_bytes());
                id[8..16].copy_from_slice(&buy_leg.market_pubkey.to_bytes()[..8]);
                id[16..24].copy_from_slice(&sell_leg.market_pubkey.to_bytes()[..8]);

                return Ok(Some(ArbitragePosition {
                    id,
                    legs: vec![
                        OptionLeg {
                            option: buy_leg.clone(),
                            quantity: position_size,
                            side: TradeSide::Buy,
                            executed_price: buy_leg.ask,
                        },
                        OptionLeg {
                            option: sell_leg.clone(),
                            quantity: position_size,
                            side: TradeSide::Sell,
                            executed_price: sell_leg.bid,
                        },
                    ],
                    entry_time: Instant::now(),
                    target_profit,
                    stop_loss: -target_profit * 0.5,
                    net_delta: (buy_leg.delta - sell_leg.delta) * position_size,
                    net_gamma: (buy_leg.gamma - sell_leg.gamma) * position_size,
                    net_vega: (buy_leg.vega - sell_leg.vega) * position_size,
                    capital_required,
                }));
            }
        }

        Ok(None)
    }

    fn check_volatility_smile_arb(&self, opt1: &OptionData, opt2: &OptionData, surface: &VolatilitySurface) -> Result<Option<ArbitragePosition>> {
        if opt1.expiry != opt2.expiry || opt1.is_call != opt2.is_call {
            return Ok(None);
        }

        let moneyness1 = opt1.strike / opt1.underlying_price;
        let moneyness2 = opt2.strike / opt2.underlying_price;
        
        if (moneyness1 - 1.0).abs() > 0.15 || (moneyness2 - 1.0).abs() > 0.15 {
            return Ok(None);
        }

        let fair_vol1 = self.get_surface_vol(surface, opt1.strike, opt1.expiry)?;
        let fair_vol2 = self.get_surface_vol(surface, opt2.strike, opt2.expiry)?;

        let vol_spread = opt1.implied_vol - opt2.implied_vol;
        let fair_spread = fair_vol1 - fair_vol2;
        let mispricing = vol_spread - fair_spread;

        if mispricing.abs() > 0.04 {
            let (buy_opt, sell_opt) = if mispricing > 0.0 {
                (opt2, opt1)
            } else {
                (opt1, opt2)
            };

            let hedge_ratio = buy_opt.vega / sell_opt.vega;
            let buy_qty = 1.0;
            let sell_qty = hedge_ratio;

            let capital_required = ((buy_opt.ask * buy_qty + sell_opt.underlying_price * 0.15 * sell_qty) * 1e9) as u64;
            let target_profit = mispricing.abs() * buy_opt.vega * buy_qty * 100.0;

            if target_profit > capital_required as f64 * MIN_PROFIT_BPS as f64 / 10000.0 {
                let mut id = [0u8; 32];
                id[..8].copy_from_slice(&chrono::Utc::now().timestamp_nanos().to_le_bytes());
                id[8..16].copy_from_slice(&buy_opt.market_pubkey.to_bytes()[..8]);
                id[16..24].copy_from_slice(&sell_opt.market_pubkey.to_bytes()[..8]);

                return Ok(Some(ArbitragePosition {
                    id,
                    legs: vec![
                        OptionLeg {
                            option: buy_opt.clone(),
                            quantity: buy_qty,
                            side: TradeSide::Buy,
                            executed_price: buy_opt.ask,
                        },
                        OptionLeg {
                            option: sell_opt.clone(),
                            quantity: sell_qty,
                            side: TradeSide::Sell,
                            executed_price: sell_opt.bid,
                        },
                    ],
                    entry_time: Instant::now(),
                    target_profit,
                    stop_loss: -target_profit * 0.5,
                    net_delta: buy_opt.delta * buy_qty - sell_opt.delta * sell_qty,
                    net_gamma: buy_opt.gamma * buy_qty - sell_opt.gamma * sell_qty,
                    net_vega: 0.0,
                    capital_required,
                }));
            }
        }

        Ok(None)
    }

    fn check_butterfly_spread(&self, opt1: &OptionData, opt2: &OptionData, all_opts: &[&OptionData], surface: &VolatilitySurface) -> Result<Option<ArbitragePosition>> {
        if opt1.is_call != opt2.is_call || opt1.expiry != opt2.expiry {
            return Ok(None);
        }

        let k1 = opt1.strike.min(opt2.strike);
        let k3 = opt1.strike.max(opt2.strike);
        let k2 = (k1 + k3) / 2.0;

        let middle_opt = all_opts.iter()
            .find(|o| o.is_call == opt1.is_call && o.expiry == opt1.expiry && (o.strike - k2).abs() < 0.5);

        if let Some(&middle) = middle_opt {
            let wing1 = if opt1.strike < opt2.strike { opt1 } else { opt2 };
            let wing2 = if opt1.strike < opt2.strike { opt2 } else { opt1 };

            let butterfly_value = wing1.mid + wing2.mid - 2.0 * middle.mid;
            let max_payoff = (k3 - k2).min(k2 - k1);
            
            if butterfly_value < 0.0 {
                let position_size = 1.0;
                let capital_required = ((wing1.ask + wing2.ask + 2.0 * middle.ask) * position_size * 1e9) as u64;
                let target_profit = butterfly_value.abs() * position_size * 1e9;

                if target_profit > capital_required as f64 * MIN_PROFIT_BPS as f64 / 10000.0 && butterfly_value < -0.02 {
                    let mut id = [0u8; 32];
                    id[..8].copy_from_slice(&chrono::Utc::now().timestamp_nanos().to_le_bytes());

                    return Ok(Some(ArbitragePosition {
                        id,
                        legs: vec![
                            OptionLeg {
                                option: wing1.clone(),
                                quantity: position_size,
                                side: TradeSide::Buy,
                                executed_price: wing1.ask,
                            },
                            OptionLeg {
                                option: middle.clone(),
                                quantity: 2.0 * position_size,
                                side: TradeSide::Sell,
                                executed_price: middle.bid,
                            },
                            OptionLeg {
                                option: wing2.clone(),
                                quantity: position_size,
                                side: TradeSide::Buy,
                                executed_price: wing2.ask,
                            },
                        ],
                        entry_time: Instant::now(),
                        target_profit,
                        stop_loss: -capital_required as f64 * 0.1,
                        net_delta: 0.0,
                        net_gamma: 0.0,
                        net_vega: 0.0,
                        capital_required,
                    }));
                }
            }
        }

        Ok(None)
    }

    fn get_surface_vol(&self, surface: &VolatilitySurface, strike: f64, expiry: i64) -> Result<f64> {
        let exp_key = OrderedFloat(expiry as f64);
        
        if let Some(sabr) = surface.sabr_params.get(&exp_key) {
            return Ok(self.sabr_vol(sabr.forward, strike, sabr.expiry, sabr.alpha, sabr.beta, sabr.rho, sabr.nu));
        }

        let vol_key = (OrderedFloat(strike), OrderedFloat(expiry as f64));
        if let Some(&vol) = surface.vols.get(&vol_key) {
            return Ok(vol);
        }

        let lower_strike = surface.strikes.iter().rev().find(|&&s| s <= strike).copied();
        let upper_strike = surface.strikes.iter().find(|&&s| s >= strike).copied();
        let lower_expiry = surface.expiries.iter().rev().find(|&&e| e <= expiry as f64).copied();
        let upper_expiry = surface.expiries.iter().find(|&&e| e >= expiry as f64).copied();

        match (lower_strike, upper_strike, lower_expiry, upper_expiry) {
            (Some(k1), Some(k2), Some(t1), Some(t2)) => {
                let v11 = surface.vols.get(&(OrderedFloat(k1), OrderedFloat(t1))).copied().unwrap_or(0.3);
                let v12 = surface.vols.get(&(OrderedFloat(k1), OrderedFloat(t2))).copied().unwrap_or(0.3);
                let v21 = surface.vols.get(&(OrderedFloat(k2), OrderedFloat(t1))).copied().unwrap_or(0.3);
                let v22 = surface.vols.get(&(OrderedFloat(k2), OrderedFloat(t2))).copied().unwrap_or(0.3);

                let w_k = if k2 != k1 { (strike - k1) / (k2 - k1) } else { 0.5 };
                let w_t = if t2 != t1 { (expiry as f64 - t1) / (t2 - t1) } else { 0.5 };

                Ok(v11 * (1.0 - w_k) * (1.0 - w_t) + v12 * (1.0 - w_k) * w_t + v21 * w_k * (1.0 - w_t) + v22 * w_k * w_t)
            }
            _ => Ok(0.3)
        }
    }

    async fn validate_opportunity(&self, opp: &ArbitragePosition) -> Result<bool> {
        if opp.net_delta.abs() > MAX_DELTA_EXPOSURE {
            return Ok(false);
        }
        if opp.net_gamma.abs() > MAX_GAMMA_EXPOSURE {
            return Ok(false);
        }
        if opp.net_vega.abs() > MAX_VEGA_EXPOSURE {
            return Ok(false);
        }
        if opp.capital_required > MAX_POSITION_USD * 1_000_000 {
            return Ok(false);
        }

        for leg in &opp.legs {
            let spread_pct = (leg.option.ask - leg.option.bid) / leg.option.mid;
            if spread_pct > SPREAD_THRESHOLD_BPS as f64 / 10000.0 {
                return Ok(false);
            }
        }

        Ok(true)
    }

    async fn execute_arbitrage(&self, opp: &ArbitragePosition) -> Result<()> {
        let mut positions = self.positions.lock().await;
        
        if positions.len() >= 10 {
            return Ok(());
        }

        let mut instructions = Vec::new();
        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(1_400_000));
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS));

        for leg in &opp.legs {
            let ix = self.create_option_order_ix(&leg.option, leg.quantity, leg.side, leg.executed_price).await?;
            instructions.push(ix);
        }

        let recent_blockhash = self.rpc.get_latest_blockhash().await?;
        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.wallet.pubkey()),
            &[&*self.wallet],
            recent_blockhash,
        );

        match self.rpc.send_and_confirm_transaction(&tx).await {
            Ok(_) => {
                positions.insert(opp.id, opp.clone());
                Ok(())
            }
            Err(e) => {
                eprintln!("Execution failed: {}", e);
                Ok(())
            }
        }
    }

    async fn create_option_order_ix(&self, option: &OptionData, quantity: f64, side: TradeSide, price: f64) -> Result<Instruction> {
        let market_account = self.rpc.get_account(&option.market_pubkey).await?;
        let market = Market::load(&market_account.data, &option.market_pubkey, true)?;

                let user_base_ata = get_associated_token_address(&self.wallet.pubkey(), &option.base_mint);
        let user_quote_ata = get_associated_token_address(&self.wallet.pubkey(), &option.quote_mint);

        let order_payer_token_account = match side {
            TradeSide::Buy => user_quote_ata,
            TradeSide::Sell => user_base_ata,
        };

        let open_orders_accounts = self.rpc.get_program_accounts_with_config(
            &serum_dex::id(),
            solana_client::rpc_client::RpcProgramAccountsConfig {
                filters: Some(vec![
                    solana_client::rpc_filter::RpcFilterType::Memcmp(
                        solana_client::rpc_filter::Memcmp::new_base58_encoded(
                            45,
                            &self.wallet.pubkey().to_bytes(),
                        ),
                    ),
                    solana_client::rpc_filter::RpcFilterType::Memcmp(
                        solana_client::rpc_filter::Memcmp::new_base58_encoded(
                            13,
                            &option.market_pubkey.to_bytes(),
                        ),
                    ),
                ]),
                ..Default::default()
            },
        ).await?;

        let open_orders_pubkey = if open_orders_accounts.is_empty() {
            let (pda, _) = Pubkey::find_program_address(
                &[
                    b"open_orders",
                    option.market_pubkey.as_ref(),
                    self.wallet.pubkey().as_ref(),
                ],
                &serum_dex::id(),
            );
            pda
        } else {
            open_orders_accounts[0].0
        };

        let side_enum = match side {
            TradeSide::Buy => Side::Buy,
            TradeSide::Sell => Side::Sell,
        };

        let limit_price_lots = (price * 1e6 / market.pc_lot_size as f64) as u64;
        let max_base_qty_lots = (quantity * 1e6 / market.coin_lot_size as f64) as u64;
        let max_quote_qty_lots = if side == TradeSide::Buy {
            (quantity * price * 1.02 * 1e6 / market.pc_lot_size as f64) as u64
        } else {
            u64::MAX
        };

        Ok(serum_dex::instruction::new_order_v3(
            &option.market_pubkey,
            &open_orders_pubkey,
            &market.req_q,
            &market.event_q,
            &market.bids,
            &market.asks,
            &order_payer_token_account,
            &self.wallet.pubkey(),
            &market.coin_vault,
            &market.pc_vault,
            &spl_token::id(),
            &solana_sdk::sysvar::rent::id(),
            None,
            &serum_dex::id(),
            OrderType::ImmediateOrCancel,
            side_enum,
            limit_price_lots,
            max_base_qty_lots,
            max_quote_qty_lots,
            SelfTradeBehavior::DecrementTake,
            u16::MAX,
            u32::MAX,
            false,
        )?)
    }

    async fn manage_positions(&self) -> Result<()> {
        let mut positions = self.positions.lock().await;
        let mut positions_to_close = Vec::new();

        for (id, position) in positions.iter() {
            let elapsed = position.entry_time.elapsed();
            
            let current_pnl = self.calculate_position_pnl(position).await?;
            
            let mut should_close = false;
            let mut reason = "";

            if current_pnl >= position.target_profit * 0.85 {
                should_close = true;
                reason = "Target reached";
            } else if current_pnl <= position.stop_loss {
                should_close = true;
                reason = "Stop loss";
            } else if elapsed > Duration::from_secs(3600) {
                should_close = true;
                reason = "Time limit";
            } else {
                for leg in &position.legs {
                    let time_to_expiry = (leg.option.expiry - chrono::Utc::now().timestamp()) as f64 / 86400.0;
                    if time_to_expiry < 2.0 {
                        should_close = true;
                        reason = "Near expiry";
                        break;
                    }
                }
            }

            if !should_close {
                let surface = self.vol_surface.read().await;
                let mut total_edge = 0.0;
                
                for leg in &position.legs {
                    let current_vol = self.get_current_implied_vol(&leg.option).await?;
                    let fair_vol = self.get_surface_vol(&surface, leg.option.strike, leg.option.expiry)?;
                    let vol_edge = match leg.side {
                        TradeSide::Buy => fair_vol - current_vol,
                        TradeSide::Sell => current_vol - fair_vol,
                    };
                    total_edge += vol_edge * leg.option.vega * leg.quantity;
                }

                if total_edge < 0.0 {
                    should_close = true;
                    reason = "Edge lost";
                }
            }

            if should_close {
                positions_to_close.push((*id, reason.to_string()));
            }
        }

        for (id, reason) in positions_to_close {
            if let Some(position) = positions.get(&id) {
                match self.close_position(position).await {
                    Ok(_) => {
                        positions.remove(&id);
                    }
                    Err(e) => {
                        eprintln!("Failed to close position {}: {}", hex::encode(id), e);
                    }
                }
            }
        }

        Ok(())
    }

    async fn calculate_position_pnl(&self, position: &ArbitragePosition) -> Result<f64> {
        let mut total_pnl = 0.0;

        for leg in &position.legs {
            let current_price = self.get_current_option_price(&leg.option).await?;
            let price_diff = match leg.side {
                TradeSide::Buy => current_price - leg.executed_price,
                TradeSide::Sell => leg.executed_price - current_price,
            };
            total_pnl += price_diff * leg.quantity * 1e9;
        }

        Ok(total_pnl)
    }

    async fn get_current_option_price(&self, option: &OptionData) -> Result<f64> {
        let market_data = self.fetch_market_data(&option.market_pubkey).await?;
        Ok(market_data.2)
    }

    async fn get_current_implied_vol(&self, option: &OptionData) -> Result<f64> {
        let market_data = self.fetch_market_data(&option.market_pubkey).await?;
        let spot_price = self.fetch_spot_price().await?;
        let time_to_expiry = ((option.expiry - chrono::Utc::now().timestamp()) as f64).max(0.0) / 31536000.0;
        
        self.newton_raphson_iv(
            market_data.2,
            spot_price,
            option.strike,
            time_to_expiry,
            RISK_FREE_RATE,
            option.is_call,
        )
    }

    async fn close_position(&self, position: &ArbitragePosition) -> Result<()> {
        let mut instructions = Vec::new();
        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(1_400_000));
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS * 2));

        for leg in &position.legs {
            let close_side = match leg.side {
                TradeSide::Buy => TradeSide::Sell,
                TradeSide::Sell => TradeSide::Buy,
            };

            let current_price = self.get_current_option_price(&leg.option).await?;
            let slippage_price = match close_side {
                TradeSide::Buy => current_price * 1.01,
                TradeSide::Sell => current_price * 0.99,
            };

            let ix = self.create_option_order_ix(&leg.option, leg.quantity, close_side, slippage_price).await?;
            instructions.push(ix);
        }

        let recent_blockhash = self.rpc.get_latest_blockhash().await?;
        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.wallet.pubkey()),
            &[&*self.wallet],
            recent_blockhash,
        );

        let send_config = solana_client::rpc_config::RpcSendTransactionConfig {
            skip_preflight: false,
            preflight_commitment: Some(CommitmentConfig::confirmed()),
            max_retries: Some(3),
            ..Default::default()
        };

        match self.rpc.send_transaction_with_config(&tx, send_config).await {
            Ok(sig) => {
                let confirm_config = solana_client::rpc_config::RpcConfirmTransactionConfig {
                    commitment: Some(CommitmentConfig::confirmed()),
                };
                
                match self.rpc.confirm_transaction_with_config(&sig, confirm_config).await {
                    Ok(_) => Ok(()),
                    Err(e) => Err(format!("Failed to confirm close transaction: {}", e).into()),
                }
            }
            Err(e) => Err(format!("Failed to send close transaction: {}", e).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_black_scholes() {
        let arb = VolSurfaceArbitrageur::new(
            "https://api.mainnet-beta.solana.com",
            Keypair::new(),
            Pubkey::default(),
        );

        let price = arb.black_scholes(100.0, 100.0, 0.25, 0.05, 0.2, true);
        assert!((price - 5.876).abs() < 0.01);
    }

    #[test]
    fn test_sabr_vol() {
        let arb = VolSurfaceArbitrageur::new(
            "https://api.mainnet-beta.solana.com",
            Keypair::new(),
            Pubkey::default(),
        );

        let vol = arb.sabr_vol(100.0, 100.0, 0.25, 0.3, 0.5, -0.1, 0.4);
        assert!(vol > 0.0 && vol < 1.0);
    }
}

