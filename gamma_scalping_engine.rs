use anchor_lang::prelude::*;
use pyth_sdk_solana::{Price, PriceFeed};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use rust_decimal::prelude::*;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use statrs::distribution::{ContinuousCDF, Normal};

const MAX_POSITION_SIZE: Decimal = dec!(1000000);
const MIN_TRADE_SIZE: Decimal = dec!(0.1);
const GAMMA_THRESHOLD: Decimal = dec!(0.0001);
const DELTA_NEUTRAL_THRESHOLD: Decimal = dec!(0.01);
const MAX_SLIPPAGE_BPS: u64 = 50;
const VOLATILITY_WINDOW: usize = 120;
const BLACK_SCHOLES_ITERATIONS: u32 = 100;
const RISK_FREE_RATE: Decimal = dec!(0.05);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionPosition {
    pub mint: Pubkey,
    pub strike: Decimal,
    pub expiry: i64,
    pub is_call: bool,
    pub position_size: Decimal,
    pub entry_price: Decimal,
    pub underlying_price: Decimal,
    pub implied_volatility: Decimal,
    pub delta: Decimal,
    pub gamma: Decimal,
    pub vega: Decimal,
    pub theta: Decimal,
    pub last_hedge_price: Decimal,
    pub last_update: i64,
}

#[derive(Debug, Clone)]
pub struct MarketData {
    pub spot_price: Decimal,
    pub bid: Decimal,
    pub ask: Decimal,
    pub volume_24h: Decimal,
    pub historical_volatility: Decimal,
    pub funding_rate: Decimal,
    pub open_interest: Decimal,
    pub timestamp: i64,
}

#[derive(Debug, Clone)]
pub struct HedgeParameters {
    pub rebalance_threshold: Decimal,
    pub max_hedge_size: Decimal,
    pub min_profit_threshold: Decimal,
    pub max_gas_price: u64,
    pub emergency_exit_threshold: Decimal,
}

pub struct GammaScalpingEngine {
    rpc_client: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    positions: Arc<RwLock<HashMap<Pubkey, OptionPosition>>>,
    market_data: Arc<RwLock<HashMap<Pubkey, MarketData>>>,
    hedge_params: HedgeParameters,
    pyth_program_id: Pubkey,
    options_program_id: Pubkey,
    spot_market_program_id: Pubkey,
    price_history: Arc<RwLock<Vec<(i64, Decimal)>>>,
    total_pnl: Arc<RwLock<Decimal>>,
}

impl GammaScalpingEngine {
    pub async fn new(
        rpc_url: String,
        wallet: Keypair,
        pyth_program_id: Pubkey,
        options_program_id: Pubkey,
        spot_market_program_id: Pubkey,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url,
            CommitmentConfig::confirmed(),
        ));

        Ok(Self {
            rpc_client,
            wallet: Arc::new(wallet),
            positions: Arc::new(RwLock::new(HashMap::new())),
            market_data: Arc::new(RwLock::new(HashMap::new())),
            hedge_params: HedgeParameters {
                rebalance_threshold: dec!(0.02),
                max_hedge_size: dec!(100000),
                min_profit_threshold: dec!(0.001),
                max_gas_price: 1_000_000,
                emergency_exit_threshold: dec!(0.1),
            },
            pyth_program_id,
            options_program_id,
            spot_market_program_id,
            price_history: Arc::new(RwLock::new(Vec::with_capacity(VOLATILITY_WINDOW))),
            total_pnl: Arc::new(RwLock::new(Decimal::ZERO)),
        })
    }

    pub async fn calculate_black_scholes_greeks(
        &self,
        spot: Decimal,
        strike: Decimal,
        time_to_expiry: Decimal,
        volatility: Decimal,
        is_call: bool,
    ) -> Result<(Decimal, Decimal, Decimal, Decimal), Box<dyn std::error::Error>> {
        if time_to_expiry <= Decimal::ZERO {
            return Ok((Decimal::ZERO, Decimal::ZERO, Decimal::ZERO, Decimal::ZERO));
        }

        let sqrt_time = time_to_expiry.sqrt().ok_or("sqrt calculation failed")?;
        let vol_sqrt_time = volatility * sqrt_time;
        
        let d1 = ((spot / strike).ln() + (RISK_FREE_RATE + volatility * volatility / dec!(2)) * time_to_expiry) / vol_sqrt_time;
        let d2 = d1 - vol_sqrt_time;

        let normal = Normal::new(0.0, 1.0).map_err(|e| format!("Normal distribution error: {}", e))?;
        
        let n_d1 = Decimal::from_f64(normal.cdf(d1.to_f64().unwrap_or(0.0))).unwrap_or(Decimal::ZERO);
        let n_d2 = Decimal::from_f64(normal.cdf(d2.to_f64().unwrap_or(0.0))).unwrap_or(Decimal::ZERO);
        let n_prime_d1 = Decimal::from_f64(
            (-d1.to_f64().unwrap_or(0.0).powi(2) / 2.0).exp() / (2.0 * std::f64::consts::PI).sqrt()
        ).unwrap_or(Decimal::ZERO);

        let delta = if is_call { n_d1 } else { n_d1 - dec!(1) };
        let gamma = n_prime_d1 / (spot * vol_sqrt_time);
        let vega = spot * n_prime_d1 * sqrt_time / dec!(100);
        let theta = if is_call {
            (-spot * n_prime_d1 * volatility / (dec!(2) * sqrt_time)
                - RISK_FREE_RATE * strike * (-RISK_FREE_RATE * time_to_expiry).exp() * n_d2) / dec!(365)
        } else {
            (-spot * n_prime_d1 * volatility / (dec!(2) * sqrt_time)
                + RISK_FREE_RATE * strike * (-RISK_FREE_RATE * time_to_expiry).exp() * (dec!(1) - n_d2)) / dec!(365)
        };

        Ok((delta, gamma, vega, theta))
    }

    pub async fn update_market_data(&self, underlying: &Pubkey) -> Result<(), Box<dyn std::error::Error>> {
        let price_account = self.rpc_client.get_account(underlying).await?;
        let price_feed = PriceFeed::try_deserialize(&mut price_account.data.as_slice())
            .map_err(|e| format!("Failed to deserialize price feed: {}", e))?;

        let current_price = price_feed.get_current_price()
            .ok_or("No current price available")?;
        
        let spot_price = Decimal::from_i64(current_price.price).unwrap_or(Decimal::ZERO) 
            / Decimal::from_i32(10_i32.pow(current_price.expo.abs() as u32)).unwrap_or(dec!(1));

        let mut market_data = self.market_data.write().await;
        let mut price_history = self.price_history.write().await;

        price_history.push((clock::Clock::get()?.unix_timestamp, spot_price));
        if price_history.len() > VOLATILITY_WINDOW {
            price_history.remove(0);
        }

        let historical_volatility = self.calculate_historical_volatility(&price_history).await?;

        market_data.insert(*underlying, MarketData {
            spot_price,
            bid: spot_price * dec!(0.9995),
            ask: spot_price * dec!(1.0005),
            volume_24h: dec!(1000000),
            historical_volatility,
            funding_rate: dec!(0.0001),
            open_interest: dec!(50000000),
            timestamp: clock::Clock::get()?.unix_timestamp,
        });

        Ok(())
    }

    async fn calculate_historical_volatility(
        &self,
        price_history: &[(i64, Decimal)],
    ) -> Result<Decimal, Box<dyn std::error::Error>> {
        if price_history.len() < 2 {
            return Ok(dec!(0.5));
        }

        let mut returns = Vec::new();
        for i in 1..price_history.len() {
            let return_val = (price_history[i].1 / price_history[i - 1].1).ln();
            returns.push(return_val);
        }

        let mean = returns.iter().sum::<Decimal>() / Decimal::from_usize(returns.len()).unwrap_or(dec!(1));
        let variance = returns.iter()
            .map(|r| (*r - mean).powi(2))
            .sum::<Decimal>() / Decimal::from_usize(returns.len() - 1).unwrap_or(dec!(1));

        Ok(variance.sqrt().unwrap_or(dec!(0.5)) * dec!(15.811388))
    }

    pub async fn calculate_portfolio_greeks(&self) -> Result<(Decimal, Decimal), Box<dyn std::error::Error>> {
        let positions = self.positions.read().await;
        let mut total_delta = Decimal::ZERO;
        let mut total_gamma = Decimal::ZERO;

        for position in positions.values() {
            total_delta += position.delta * position.position_size;
            total_gamma += position.gamma * position.position_size;
        }

        Ok((total_delta, total_gamma))
    }

    pub async fn scan_arbitrage_opportunities(&self) -> Result<Vec<Instruction>, Box<dyn std::error::Error>> {
        let mut instructions = Vec::new();
        let positions = self.positions.read().await;
        let market_data = self.market_data.read().await;

        for (mint, position) in positions.iter() {
            if let Some(market) = market_data.get(mint) {
                let time_to_expiry = Decimal::from_i64(position.expiry - clock::Clock::get()?.unix_timestamp)
                    .unwrap_or(Decimal::ZERO) / dec!(31536000);

                if time_to_expiry <= Decimal::ZERO {
                    continue;
                }

                let (new_delta, new_gamma, new_vega, new_theta) = self.calculate_black_scholes_greeks(
                    market.spot_price,
                    position.strike,
                    time_to_expiry,
                    position.implied_volatility,
                    position.is_call,
                ).await?;

                let gamma_pnl = position.gamma * position.position_size * 
                    (market.spot_price - position.last_hedge_price).powi(2) / dec!(2);

                if gamma_pnl > self.hedge_params.min_profit_threshold {
                    let hedge_size = -(new_delta * position.position_size);
                    
                    if hedge_size.abs() > MIN_TRADE_SIZE && 
                       hedge_size.abs() < self.hedge_params.max_hedge_size {
                        
                        let hedge_instruction = self.build_hedge_instruction(
                            mint,
                            hedge_size,
                            market.spot_price,
                        ).await?;
                        
                        instructions.push(hedge_instruction);
                    }
                }
            }
        }

        Ok(instructions)
    }

    async fn build_hedge_instruction(
        &self,
        underlying: &Pubkey,
        size: Decimal,
        price: Decimal,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let accounts = vec![
            AccountMeta::new(self.wallet.pubkey(), true),
            AccountMeta::new(*underlying, false),
            AccountMeta::new_readonly(self.spot_market_program_id, false),
            AccountMeta::new_readonly(solana_sdk::system_program::id(), false),
        ];

        let size_u64 = (size.abs() * dec!(1000000)).to_u64()
            .ok_or("Failed to convert size to u64")?;
        let price_u64 = (price * dec!(1000000)).to_u64()
            .ok_or("Failed to convert price to u64")?;
        let is_buy = size > Decimal::ZERO;

        let instruction_data = [
            &[if is_buy { 0 } else { 1 }][..],
            &size_u64.to_le_bytes(),
            &price_u64.to_le_bytes(),
            &MAX_SLIPPAGE_BPS.to_le_bytes(),
        ].concat();

        Ok(Instruction {
            program_id: self.spot_market_program_id,
            accounts,
            data: instruction_data,
        })
    }

    pub async fn execute_gamma_scalp(&self) -> Result<(), Box<dyn std::error::Error>> {
        let (total_delta, total_gamma) = self.calculate_portfolio_greeks().await?;

        if total_gamma.abs() < GAMMA_THRESHOLD {
            return Ok(());
        }

                let instructions = self.scan_arbitrage_opportunities().await?;
        
        if instructions.is_empty() {
            return Ok(());
        }

        let mut transaction = Transaction::new_with_payer(
            &[
                ComputeBudgetInstruction::set_compute_unit_limit(1_400_000),
                ComputeBudgetInstruction::set_compute_unit_price(self.hedge_params.max_gas_price),
            ],
            Some(&self.wallet.pubkey()),
        );

        for instruction in instructions.iter().take(4) {
            transaction.message.instructions.push(instruction.clone());
        }

        let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;
        transaction.message.recent_blockhash = recent_blockhash;

        let simulation = self.rpc_client
            .simulate_transaction(&transaction)
            .await?;

        if simulation.value.err.is_some() {
            return Err(format!("Simulation failed: {:?}", simulation.value.err).into());
        }

        transaction.sign(&[self.wallet.as_ref()], recent_blockhash);

        let signature = self.rpc_client
            .send_and_confirm_transaction_with_spinner(&transaction)
            .await?;

        self.update_positions_post_hedge().await?;
        self.record_pnl().await?;

        Ok(())
    }

    async fn update_positions_post_hedge(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut positions = self.positions.write().await;
        let market_data = self.market_data.read().await;
        let current_time = clock::Clock::get()?.unix_timestamp;

        for (mint, position) in positions.iter_mut() {
            if let Some(market) = market_data.get(mint) {
                position.last_hedge_price = market.spot_price;
                position.last_update = current_time;
                position.underlying_price = market.spot_price;

                let time_to_expiry = Decimal::from_i64(position.expiry - current_time)
                    .unwrap_or(Decimal::ZERO) / dec!(31536000);

                if time_to_expiry > Decimal::ZERO {
                    let (delta, gamma, vega, theta) = self.calculate_black_scholes_greeks(
                        market.spot_price,
                        position.strike,
                        time_to_expiry,
                        position.implied_volatility,
                        position.is_call,
                    ).await?;

                    position.delta = delta;
                    position.gamma = gamma;
                    position.vega = vega;
                    position.theta = theta;
                }
            }
        }

        Ok(())
    }

    async fn record_pnl(&self) -> Result<(), Box<dyn std::error::Error>> {
        let positions = self.positions.read().await;
        let market_data = self.market_data.read().await;
        let mut total_pnl = self.total_pnl.write().await;

        for (mint, position) in positions.iter() {
            if let Some(market) = market_data.get(mint) {
                let spot_pnl = position.delta * position.position_size * 
                    (market.spot_price - position.last_hedge_price);
                let gamma_pnl = position.gamma * position.position_size * 
                    (market.spot_price - position.last_hedge_price).powi(2) / dec!(2);
                let theta_pnl = position.theta * position.position_size / dec!(365);

                *total_pnl += spot_pnl + gamma_pnl + theta_pnl;
            }
        }

        Ok(())
    }

    pub async fn add_option_position(
        &self,
        mint: Pubkey,
        strike: Decimal,
        expiry: i64,
        is_call: bool,
        size: Decimal,
        entry_price: Decimal,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if size.abs() > MAX_POSITION_SIZE {
            return Err("Position size exceeds maximum allowed".into());
        }

        let market_data = self.market_data.read().await;
        let underlying_price = market_data.get(&mint)
            .map(|m| m.spot_price)
            .unwrap_or(entry_price);

        let time_to_expiry = Decimal::from_i64(expiry - clock::Clock::get()?.unix_timestamp)
            .unwrap_or(Decimal::ZERO) / dec!(31536000);

        let implied_vol = self.calculate_implied_volatility(
            entry_price,
            underlying_price,
            strike,
            time_to_expiry,
            is_call,
        ).await?;

        let (delta, gamma, vega, theta) = self.calculate_black_scholes_greeks(
            underlying_price,
            strike,
            time_to_expiry,
            implied_vol,
            is_call,
        ).await?;

        let position = OptionPosition {
            mint,
            strike,
            expiry,
            is_call,
            position_size: size,
            entry_price,
            underlying_price,
            implied_volatility: implied_vol,
            delta,
            gamma,
            vega,
            theta,
            last_hedge_price: underlying_price,
            last_update: clock::Clock::get()?.unix_timestamp,
        };

        let mut positions = self.positions.write().await;
        positions.insert(mint, position);

        Ok(())
    }

    async fn calculate_implied_volatility(
        &self,
        option_price: Decimal,
        spot: Decimal,
        strike: Decimal,
        time_to_expiry: Decimal,
        is_call: bool,
    ) -> Result<Decimal, Box<dyn std::error::Error>> {
        if time_to_expiry <= Decimal::ZERO {
            return Ok(dec!(0.5));
        }

        let mut vol_low = dec!(0.01);
        let mut vol_high = dec!(5.0);
        let tolerance = dec!(0.0001);

        for _ in 0..BLACK_SCHOLES_ITERATIONS {
            let vol_mid = (vol_low + vol_high) / dec!(2);
            
            let theoretical_price = self.calculate_option_price(
                spot,
                strike,
                time_to_expiry,
                vol_mid,
                is_call,
            ).await?;

            if (theoretical_price - option_price).abs() < tolerance {
                return Ok(vol_mid);
            }

            if theoretical_price < option_price {
                vol_low = vol_mid;
            } else {
                vol_high = vol_mid;
            }
        }

        Ok((vol_low + vol_high) / dec!(2))
    }

    async fn calculate_option_price(
        &self,
        spot: Decimal,
        strike: Decimal,
        time_to_expiry: Decimal,
        volatility: Decimal,
        is_call: bool,
    ) -> Result<Decimal, Box<dyn std::error::Error>> {
        if time_to_expiry <= Decimal::ZERO {
            let intrinsic = if is_call {
                (spot - strike).max(Decimal::ZERO)
            } else {
                (strike - spot).max(Decimal::ZERO)
            };
            return Ok(intrinsic);
        }

        let sqrt_time = time_to_expiry.sqrt().ok_or("sqrt calculation failed")?;
        let vol_sqrt_time = volatility * sqrt_time;
        
        let d1 = ((spot / strike).ln() + (RISK_FREE_RATE + volatility * volatility / dec!(2)) * time_to_expiry) / vol_sqrt_time;
        let d2 = d1 - vol_sqrt_time;

        let normal = Normal::new(0.0, 1.0).map_err(|e| format!("Normal distribution error: {}", e))?;
        
        let n_d1 = Decimal::from_f64(normal.cdf(d1.to_f64().unwrap_or(0.0))).unwrap_or(Decimal::ZERO);
        let n_d2 = Decimal::from_f64(normal.cdf(d2.to_f64().unwrap_or(0.0))).unwrap_or(Decimal::ZERO);
        let n_neg_d1 = Decimal::from_f64(normal.cdf(-d1.to_f64().unwrap_or(0.0))).unwrap_or(Decimal::ZERO);
        let n_neg_d2 = Decimal::from_f64(normal.cdf(-d2.to_f64().unwrap_or(0.0))).unwrap_or(Decimal::ZERO);

        let discount = (-RISK_FREE_RATE * time_to_expiry).exp();

        let price = if is_call {
            spot * n_d1 - strike * discount * n_d2
        } else {
            strike * discount * n_neg_d2 - spot * n_neg_d1
        };

        Ok(price.max(Decimal::ZERO))
    }

    pub async fn monitor_risk_limits(&self) -> Result<bool, Box<dyn std::error::Error>> {
        let positions = self.positions.read().await;
        let market_data = self.market_data.read().await;
        
        let mut total_exposure = Decimal::ZERO;
        let mut total_vega_exposure = Decimal::ZERO;
        let mut max_loss = Decimal::ZERO;

        for (mint, position) in positions.iter() {
            if let Some(market) = market_data.get(mint) {
                let position_value = position.position_size.abs() * market.spot_price;
                total_exposure += position_value;
                
                total_vega_exposure += position.vega * position.position_size.abs();
                
                let worst_case_move = market.spot_price * self.hedge_params.emergency_exit_threshold;
                let position_loss = position.gamma * position.position_size.abs() * worst_case_move.powi(2) / dec!(2);
                max_loss += position_loss;
            }
        }

        if total_exposure > MAX_POSITION_SIZE || max_loss > MAX_POSITION_SIZE * dec!(0.1) {
            return Ok(false);
        }

        Ok(true)
    }

    pub async fn emergency_close_positions(&self) -> Result<(), Box<dyn std::error::Error>> {
        let positions = self.positions.read().await;
        let mut instructions = Vec::new();

        for (mint, position) in positions.iter() {
            let close_instruction = self.build_close_position_instruction(
                mint,
                position.position_size,
            ).await?;
            
            instructions.push(close_instruction);
        }

        if instructions.is_empty() {
            return Ok(());
        }

        for chunk in instructions.chunks(4) {
            let mut transaction = Transaction::new_with_payer(
                &[
                    ComputeBudgetInstruction::set_compute_unit_limit(1_400_000),
                    ComputeBudgetInstruction::set_compute_unit_price(self.hedge_params.max_gas_price * 2),
                ],
                Some(&self.wallet.pubkey()),
            );

            for instruction in chunk {
                transaction.message.instructions.push(instruction.clone());
            }

            let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;
            transaction.message.recent_blockhash = recent_blockhash;
            transaction.sign(&[self.wallet.as_ref()], recent_blockhash);

            match self.rpc_client.send_and_confirm_transaction_with_spinner(&transaction).await {
                Ok(_) => {},
                Err(e) => eprintln!("Failed to close position: {}", e),
            }
        }

        self.positions.write().await.clear();
        Ok(())
    }

    async fn build_close_position_instruction(
        &self,
        option_mint: &Pubkey,
        size: Decimal,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let accounts = vec![
            AccountMeta::new(self.wallet.pubkey(), true),
            AccountMeta::new(*option_mint, false),
            AccountMeta::new_readonly(self.options_program_id, false),
            AccountMeta::new_readonly(solana_sdk::system_program::id(), false),
        ];

        let size_u64 = (size.abs() * dec!(1000000)).to_u64()
            .ok_or("Failed to convert size to u64")?;

        let instruction_data = [
            &[2u8][..],
            &size_u64.to_le_bytes(),
        ].concat();

        Ok(Instruction {
            program_id: self.options_program_id,
            accounts,
            data: instruction_data,
        })
    }

    pub async fn calculate_optimal_hedge_ratio(&self) -> Result<Decimal, Box<dyn std::error::Error>> {
        let (total_delta, total_gamma) = self.calculate_portfolio_greeks().await?;
        
        if total_gamma.abs() < GAMMA_THRESHOLD {
            return Ok(Decimal::ZERO);
        }

        let market_data = self.market_data.read().await;
        let avg_volatility = market_data.values()
            .map(|m| m.historical_volatility)
            .sum::<Decimal>() / Decimal::from_usize(market_data.len().max(1)).unwrap_or(dec!(1));

        let optimal_ratio = -total_delta / (dec!(1) + total_gamma * avg_volatility.powi(2) * dec!(0.5));
        
        Ok(optimal_ratio.max(-self.hedge_params.max_hedge_size).min(self.hedge_params.max_hedge_size))
    }

    pub async fn get_portfolio_metrics(&self) -> Result<HashMap<String, Decimal>, Box<dyn std::error::Error>> {
        let mut metrics = HashMap::new();
        
        let (total_delta, total_gamma) = self.calculate_portfolio_greeks().await?;
        let total_pnl = *self.total_pnl.read().await;
        let positions = self.positions.read().await;
        
                let total_vega: Decimal = positions.values()
            .map(|p| p.vega * p.position_size)
            .sum();
        
        let total_theta: Decimal = positions.values()
            .map(|p| p.theta * p.position_size)
            .sum();

        let market_data = self.market_data.read().await;
        let total_notional: Decimal = positions.values()
            .filter_map(|p| market_data.get(&p.mint).map(|m| p.position_size.abs() * m.spot_price))
            .sum();

        metrics.insert("total_delta".to_string(), total_delta);
        metrics.insert("total_gamma".to_string(), total_gamma);
        metrics.insert("total_vega".to_string(), total_vega);
        metrics.insert("total_theta".to_string(), total_theta);
        metrics.insert("total_pnl".to_string(), total_pnl);
        metrics.insert("total_notional".to_string(), total_notional);
        metrics.insert("position_count".to_string(), Decimal::from(positions.len()));

        Ok(metrics)
    }

    pub async fn adjust_for_volatility_smile(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut positions = self.positions.write().await;
        let market_data = self.market_data.read().await;

        for (mint, position) in positions.iter_mut() {
            if let Some(market) = market_data.get(mint) {
                let moneyness = market.spot_price / position.strike;
                let skew_adjustment = self.calculate_volatility_skew(moneyness, position.implied_volatility)?;
                
                position.implied_volatility = position.implied_volatility * (dec!(1) + skew_adjustment);
                
                let time_to_expiry = Decimal::from_i64(position.expiry - clock::Clock::get()?.unix_timestamp)
                    .unwrap_or(Decimal::ZERO) / dec!(31536000);

                if time_to_expiry > Decimal::ZERO {
                    let (delta, gamma, vega, theta) = self.calculate_black_scholes_greeks(
                        market.spot_price,
                        position.strike,
                        time_to_expiry,
                        position.implied_volatility,
                        position.is_call,
                    ).await?;

                    position.delta = delta;
                    position.gamma = gamma;
                    position.vega = vega;
                    position.theta = theta;
                }
            }
        }

        Ok(())
    }

    fn calculate_volatility_skew(&self, moneyness: Decimal, base_vol: Decimal) -> Result<Decimal, Box<dyn std::error::Error>> {
        let ln_moneyness = moneyness.ln();
        let skew_factor = dec!(0.1);
        let convexity_factor = dec!(0.05);
        
        let skew = skew_factor * ln_moneyness + convexity_factor * ln_moneyness.powi(2);
        Ok(skew.max(dec!(-0.5)).min(dec!(0.5)))
    }

    pub async fn rebalance_portfolio(&self) -> Result<(), Box<dyn std::error::Error>> {
        let risk_ok = self.monitor_risk_limits().await?;
        if !risk_ok {
            self.emergency_close_positions().await?;
            return Ok(());
        }

        self.adjust_for_volatility_smile().await?;
        
        let (total_delta, total_gamma) = self.calculate_portfolio_greeks().await?;
        
        if total_delta.abs() > DELTA_NEUTRAL_THRESHOLD || total_gamma.abs() > GAMMA_THRESHOLD {
            self.execute_gamma_scalp().await?;
        }

        self.clean_expired_positions().await?;
        
        Ok(())
    }

    async fn clean_expired_positions(&self) -> Result<(), Box<dyn std::error::Error>> {
        let current_time = clock::Clock::get()?.unix_timestamp;
        let mut positions = self.positions.write().await;
        
        let expired_positions: Vec<Pubkey> = positions
            .iter()
            .filter(|(_, p)| p.expiry <= current_time)
            .map(|(k, _)| *k)
            .collect();

        for mint in expired_positions {
            positions.remove(&mint);
        }

        Ok(())
    }

    pub async fn calculate_portfolio_var(&self, confidence_level: Decimal) -> Result<Decimal, Box<dyn std::error::Error>> {
        let positions = self.positions.read().await;
        let market_data = self.market_data.read().await;
        
        let mut portfolio_values = Vec::new();
        let simulations = 10000;
        
        for _ in 0..simulations {
            let mut simulated_value = Decimal::ZERO;
            
            for (mint, position) in positions.iter() {
                if let Some(market) = market_data.get(mint) {
                    let random_return = self.generate_random_return(market.historical_volatility)?;
                    let simulated_price = market.spot_price * (dec!(1) + random_return);
                    
                    let time_to_expiry = Decimal::from_i64(position.expiry - clock::Clock::get()?.unix_timestamp)
                        .unwrap_or(Decimal::ZERO) / dec!(31536000);
                    
                    if time_to_expiry > Decimal::ZERO {
                        let option_value = self.calculate_option_price(
                            simulated_price,
                            position.strike,
                            time_to_expiry,
                            position.implied_volatility,
                            position.is_call,
                        ).await?;
                        
                        simulated_value += option_value * position.position_size;
                    }
                }
            }
            
            portfolio_values.push(simulated_value);
        }
        
        portfolio_values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let var_index = ((dec!(1) - confidence_level) * Decimal::from(simulations)).to_usize().unwrap_or(0);
        
        Ok(portfolio_values.get(var_index).copied().unwrap_or(Decimal::ZERO))
    }

    fn generate_random_return(&self, volatility: Decimal) -> Result<Decimal, Box<dyn std::error::Error>> {
        use rand::prelude::*;
        let mut rng = thread_rng();
        let normal_sample: f64 = rng.gen::<f64>() * 2.0 - 1.0;
        
        let daily_vol = volatility / dec!(15.811388);
        let return_val = Decimal::from_f64(normal_sample).unwrap_or(Decimal::ZERO) * daily_vol;
        
        Ok(return_val)
    }

    pub async fn optimize_hedge_timing(&self) -> Result<bool, Box<dyn std::error::Error>> {
        let market_data = self.market_data.read().await;
        let price_history = self.price_history.read().await;
        
        if price_history.len() < 10 {
            return Ok(false);
        }
        
        let recent_prices: Vec<Decimal> = price_history.iter()
            .rev()
            .take(10)
            .map(|(_, p)| *p)
            .collect();
        
        let mut momentum = Decimal::ZERO;
        for i in 1..recent_prices.len() {
            momentum += recent_prices[i - 1] - recent_prices[i];
        }
        momentum /= Decimal::from(recent_prices.len() - 1);
        
        let avg_volatility = market_data.values()
            .map(|m| m.historical_volatility)
            .sum::<Decimal>() / Decimal::from_usize(market_data.len().max(1)).unwrap_or(dec!(1));
        
        let volatility_threshold = avg_volatility * dec!(0.5);
        let momentum_threshold = volatility_threshold * dec!(0.1);
        
        Ok(momentum.abs() < momentum_threshold)
    }

    pub async fn calculate_hedge_cost(&self, hedge_size: Decimal) -> Result<Decimal, Box<dyn std::error::Error>> {
        let market_data = self.market_data.read().await;
        
        let avg_spread = market_data.values()
            .map(|m| (m.ask - m.bid) / m.spot_price)
            .sum::<Decimal>() / Decimal::from_usize(market_data.len().max(1)).unwrap_or(dec!(1));
        
        let slippage = hedge_size.abs() / dec!(1000000) * dec!(0.0001);
        let gas_cost = Decimal::from(self.hedge_params.max_gas_price) / dec!(1000000000);
        
        let total_cost = hedge_size.abs() * (avg_spread + slippage) + gas_cost;
        
        Ok(total_cost)
    }

    pub async fn should_execute_hedge(&self, hedge_size: Decimal, expected_profit: Decimal) -> Result<bool, Box<dyn std::error::Error>> {
        if hedge_size.abs() < MIN_TRADE_SIZE {
            return Ok(false);
        }
        
        let hedge_cost = self.calculate_hedge_cost(hedge_size).await?;
        let net_profit = expected_profit - hedge_cost;
        
        if net_profit < self.hedge_params.min_profit_threshold {
            return Ok(false);
        }
        
        let optimal_timing = self.optimize_hedge_timing().await?;
        if !optimal_timing {
            return Ok(false);
        }
        
        let risk_ok = self.monitor_risk_limits().await?;
        if !risk_ok {
            return Ok(false);
        }
        
        Ok(true)
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        loop {
            match self.rebalance_portfolio().await {
                Ok(_) => {
                    let metrics = self.get_portfolio_metrics().await?;
                    if let Some(pnl) = metrics.get("total_pnl") {
                        if *pnl < -MAX_POSITION_SIZE * dec!(0.05) {
                            self.emergency_close_positions().await?;
                            break;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error in rebalance: {}", e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
            }
            
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        }
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_black_scholes_calculation() {
        let engine = GammaScalpingEngine::new(
            "https://api.mainnet-beta.solana.com".to_string(),
            Keypair::new(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ).await.unwrap();

        let (delta, gamma, vega, theta) = engine.calculate_black_scholes_greeks(
            dec!(100),
            dec!(100),
            dec!(0.25),
            dec!(0.3),
            true,
        ).await.unwrap();

        assert!(delta > Decimal::ZERO && delta < dec!(1));
        assert!(gamma > Decimal::ZERO);
        assert!(vega > Decimal::ZERO);
        assert!(theta < Decimal::ZERO);
    }
}

