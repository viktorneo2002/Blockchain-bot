use borsh::{BorshDeserialize, BorshSerialize};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    clock::Clock,
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

const PRICE_HISTORY_SIZE: usize = 256;
const MIN_OBSERVATIONS: usize = 64;
const MAX_POSITION_SIZE: f64 = 0.25;
const MIN_PROFIT_BPS: f64 = 5.0;
const MAX_DRAWDOWN: f64 = 0.02;
const CONFIDENCE_THRESHOLD: f64 = 0.95;
const MAX_CORRELATION_LAG: usize = 10;
const VOLATILITY_WINDOW: usize = 32;
const HALF_LIFE_PERIODS: f64 = 14.0;
const MIN_HALF_LIFE: f64 = 2.0;
const MAX_HALF_LIFE: f64 = 100.0;
const OUTLIER_THRESHOLD: f64 = 4.0;
const MIN_LIQUIDITY_USD: f64 = 50000.0;
const MAX_SLIPPAGE_BPS: f64 = 10.0;
const EMERGENCY_EXIT_THRESHOLD: f64 = 0.05;
const GAS_BUFFER_LAMPORTS: u64 = 5_000_000;
const MAX_RETRY_ATTEMPTS: u8 = 3;
const RETRY_DELAY_MS: u64 = 50;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SignalStrength {
    VeryStrong,
    Strong,
    Medium,
    Weak,
    None,
}

#[derive(Debug, Clone, Copy)]
pub struct OUParameters {
    pub theta: f64,
    pub mu: f64,
    pub sigma: f64,
    pub half_life: f64,
    pub mean_reversion_time: f64,
    pub confidence: f64,
}

#[derive(Debug, Clone)]
pub struct PricePoint {
    pub price: f64,
    pub volume: f64,
    pub timestamp: u64,
    pub slot: u64,
}

#[derive(Debug, Clone)]
pub struct MarketState {
    pub bid: f64,
    pub ask: f64,
    pub mid: f64,
    pub spread_bps: f64,
    pub liquidity: f64,
    pub volatility: f64,
    pub skew: f64,
    pub kurtosis: f64,
}

#[derive(Debug, Clone)]
pub struct Signal {
    pub strength: SignalStrength,
    pub z_score: f64,
    pub expected_return: f64,
    pub confidence: f64,
    pub time_to_reversion: f64,
    pub position_size: f64,
    pub stop_loss: f64,
    pub take_profit: f64,
    pub max_hold_time: u64,
}

pub struct OrnsteinUhlenbeckMeanReverter {
    price_history: Arc<RwLock<VecDeque<PricePoint>>>,
    parameters: Arc<RwLock<OUParameters>>,
    current_position: Arc<RwLock<f64>>,
    entry_price: Arc<RwLock<Option<f64>>>,
    last_update: Arc<RwLock<Instant>>,
    total_pnl: Arc<RwLock<f64>>,
    win_rate: Arc<RwLock<f64>>,
    sharpe_ratio: Arc<RwLock<f64>>,
    max_drawdown_seen: Arc<RwLock<f64>>,
    trades_count: Arc<RwLock<u64>>,
}

impl OrnsteinUhlenbeckMeanReverter {
    pub fn new() -> Self {
        Self {
            price_history: Arc::new(RwLock::new(VecDeque::with_capacity(PRICE_HISTORY_SIZE))),
            parameters: Arc::new(RwLock::new(OUParameters {
                theta: 0.0,
                mu: 0.0,
                sigma: 0.0,
                half_life: HALF_LIFE_PERIODS,
                mean_reversion_time: 0.0,
                confidence: 0.0,
            })),
            current_position: Arc::new(RwLock::new(0.0)),
            entry_price: Arc::new(RwLock::new(None)),
            last_update: Arc::new(RwLock::new(Instant::now())),
            total_pnl: Arc::new(RwLock::new(0.0)),
            win_rate: Arc::new(RwLock::new(0.0)),
            sharpe_ratio: Arc::new(RwLock::new(0.0)),
            max_drawdown_seen: Arc::new(RwLock::new(0.0)),
            trades_count: Arc::new(RwLock::new(0)),
        }
    }

    pub fn update_price(&self, price: f64, volume: f64, timestamp: u64, slot: u64) -> Result<(), Box<dyn std::error::Error>> {
        let mut history = self.price_history.write().unwrap();
        
        if let Some(last) = history.back() {
            if (price - last.price).abs() / last.price > 0.5 {
                return Ok(());
            }
        }

        history.push_back(PricePoint { price, volume, timestamp, slot });
        
        if history.len() > PRICE_HISTORY_SIZE {
            history.pop_front();
        }

        drop(history);

        if self.price_history.read().unwrap().len() >= MIN_OBSERVATIONS {
            self.calibrate_parameters()?;
        }

        *self.last_update.write().unwrap() = Instant::now();
        Ok(())
    }

    fn calibrate_parameters(&self) -> Result<(), Box<dyn std::error::Error>> {
        let history = self.price_history.read().unwrap();
        let prices: Vec<f64> = history.iter().map(|p| p.price).collect();
        
        if prices.len() < MIN_OBSERVATIONS {
            return Ok(());
        }

        let log_prices: Vec<f64> = prices.iter().map(|p| p.ln()).collect();
        let returns: Vec<f64> = log_prices.windows(2)
            .map(|w| w[1] - w[0])
            .collect();

        let mu = self.calculate_robust_mean(&log_prices);
        let sigma = self.calculate_robust_std(&returns);
        
        let autocorr = self.calculate_autocorrelation(&returns, 1);
        let theta = -autocorr.ln().max(0.001);
        
        let half_life = (2.0_f64.ln() / theta).max(MIN_HALF_LIFE).min(MAX_HALF_LIFE);
        let mean_reversion_time = 1.0 / theta;
        
        let adf_stat = self.augmented_dickey_fuller(&log_prices);
        let confidence = self.calculate_confidence(adf_stat, returns.len());

        let mut params = self.parameters.write().unwrap();
        params.theta = theta;
        params.mu = mu;
        params.sigma = sigma;
        params.half_life = half_life;
        params.mean_reversion_time = mean_reversion_time;
        params.confidence = confidence;

        Ok(())
    }

    fn calculate_robust_mean(&self, data: &[f64]) -> f64 {
        let mut sorted = data.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let trim_pct = 0.1;
        let trim_count = (data.len() as f64 * trim_pct) as usize;
        
        let trimmed = &sorted[trim_count..sorted.len() - trim_count];
        trimmed.iter().sum::<f64>() / trimmed.len() as f64
    }

    fn calculate_robust_std(&self, data: &[f64]) -> f64 {
        let mean = self.calculate_robust_mean(data);
        let variance = data.iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>() / (data.len() - 1) as f64;
        variance.sqrt()
    }

    fn calculate_autocorrelation(&self, returns: &[f64], lag: usize) -> f64 {
        if returns.len() <= lag {
            return 0.0;
        }

        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;

        if variance < 1e-10 {
            return 0.0;
        }

        let covariance = returns[..returns.len() - lag].iter()
            .zip(returns[lag..].iter())
            .map(|(r1, r2)| (r1 - mean) * (r2 - mean))
            .sum::<f64>() / (returns.len() - lag) as f64;

        covariance / variance
    }

    fn augmented_dickey_fuller(&self, prices: &[f64]) -> f64 {
        let diffs: Vec<f64> = prices.windows(2)
            .map(|w| w[1] - w[0])
            .collect();
        
        let lagged: Vec<f64> = prices[..prices.len() - 1].to_vec();
        
        let mean_diff = diffs.iter().sum::<f64>() / diffs.len() as f64;
        let mean_lag = lagged.iter().sum::<f64>() / lagged.len() as f64;
        
        let numerator: f64 = diffs.iter().zip(lagged.iter())
            .map(|(d, l)| (d - mean_diff) * (l - mean_lag))
            .sum();
        
        let denominator: f64 = lagged.iter()
            .map(|l| (l - mean_lag).powi(2))
            .sum();
        
        if denominator < 1e-10 {
            return 0.0;
        }
        
        let beta = numerator / denominator;
        let residuals: Vec<f64> = diffs.iter().zip(lagged.iter())
            .map(|(d, l)| d - beta * l)
            .collect();
        
        let se_beta = (residuals.iter().map(|r| r.powi(2)).sum::<f64>() 
            / (residuals.len() as f64 - 1.0) / denominator).sqrt();
        
        if se_beta < 1e-10 {
            return 0.0;
        }
        
        beta / se_beta
    }

    fn calculate_confidence(&self, adf_stat: f64, n: usize) -> f64 {
        let critical_values = match n {
            n if n < 50 => [-3.75, -3.33, -3.00],
            n if n < 100 => [-3.58, -3.22, -2.93],
            n if n < 250 => [-3.51, -3.17, -2.89],
            _ => [-3.43, -3.12, -2.86],
        };
        
        if adf_stat < critical_values[0] {
            0.99
        } else if adf_stat < critical_values[1] {
            0.95
        } else if adf_stat < critical_values[2] {
            0.90
        } else {
            0.5 + (critical_values[2] - adf_stat).max(0.0).min(0.4)
        }
    }

    pub fn generate_signal(&self, market: &MarketState) -> Signal {
        let params = self.parameters.read().unwrap();
        let history = self.price_history.read().unwrap();
        
        if history.len() < MIN_OBSERVATIONS || params.confidence < CONFIDENCE_THRESHOLD {
            return Signal {
                strength: SignalStrength::None,
                z_score: 0.0,
                expected_return: 0.0,
                confidence: 0.0,
                time_to_reversion: 0.0,
                position_size: 0.0,
                stop_loss: 0.0,
                take_profit: 0.0,
                max_hold_time: 0,
            };
        }

        let current_log_price = market.mid.ln();
        let z_score = (current_log_price - params.mu) / params.sigma;
        
        let strength = match z_score.abs() {
            z if z > 3.0 => SignalStrength::VeryStrong,
            z if z > 2.5 => SignalStrength::Strong,
            z if z > 2.0 => SignalStrength::Medium,
            z if z > 1.5 => SignalStrength::Weak,
            _ => SignalStrength::None,
        };

        if matches!(strength, SignalStrength::None) || market.liquidity < MIN_LIQUIDITY_USD {
            return Signal {
                strength: SignalStrength::None,
                z_score: 0.0,
                expected_return: 0.0,
                confidence: 0.0,
                time_to_reversion: 0.0,
                position_size: 0.0,
                stop_loss: 0.0,
                take_profit: 0.0,
                max_hold_time: 0,
            };
        }

        let expected_return = -z_score * params.sigma * (1.0 - (-params.theta).exp());
        let time_to_reversion = params.half_life * z_score.abs().ln() / 2.0_f64.ln();
        
                let kelly_fraction = (expected_return * params.confidence) / (params.sigma.powi(2));
        let position_size = kelly_fraction.abs().min(MAX_POSITION_SIZE) * (1.0 - market.spread_bps / 10000.0);
        
        let direction = if z_score < 0.0 { 1.0 } else { -1.0 };
        let entry_price = if direction > 0.0 { market.ask } else { market.bid };
        
        let volatility_multiplier = (market.volatility / params.sigma).max(1.0);
        let stop_loss = entry_price * (1.0 - direction * OUTLIER_THRESHOLD * params.sigma * volatility_multiplier);
        let take_profit = entry_price * (1.0 + direction * expected_return.abs() * 0.8);
        
        let max_hold_time = (time_to_reversion * 3.0 * 1_000_000.0) as u64;

        Signal {
            strength,
            z_score,
            expected_return: expected_return * direction,
            confidence: params.confidence,
            time_to_reversion,
            position_size: position_size * direction,
            stop_loss,
            take_profit,
            max_hold_time,
        }
    }

    pub fn should_execute(&self, signal: &Signal, balance: f64) -> bool {
        let current_pos = *self.current_position.read().unwrap();
        let pnl = *self.total_pnl.read().unwrap();
        let max_dd = *self.max_drawdown_seen.read().unwrap();
        
        if max_dd > MAX_DRAWDOWN || pnl < -balance * EMERGENCY_EXIT_THRESHOLD {
            return false;
        }

        if signal.position_size.abs() < 0.01 {
            return false;
        }

        if current_pos.abs() > 0.01 && signal.position_size.signum() != current_pos.signum() {
            return true;
        }

        if current_pos.abs() < 0.01 && !matches!(signal.strength, SignalStrength::None) {
            return true;
        }

        let position_delta = (signal.position_size - current_pos).abs();
        position_delta > 0.05
    }

    pub fn calculate_execution_params(&self, signal: &Signal, balance: f64) -> ExecutionParams {
        let current_pos = *self.current_position.read().unwrap();
        let position_delta = signal.position_size - current_pos;
        let amount_usd = position_delta.abs() * balance;
        
        let slippage_tolerance = MAX_SLIPPAGE_BPS / 10000.0;
        let urgency = match signal.strength {
            SignalStrength::VeryStrong => 1.0,
            SignalStrength::Strong => 0.8,
            SignalStrength::Medium => 0.6,
            SignalStrength::Weak => 0.4,
            SignalStrength::None => 0.0,
        };

        ExecutionParams {
            amount_usd,
            direction: position_delta.signum(),
            max_slippage: slippage_tolerance,
            urgency,
            time_limit: Duration::from_millis(500),
            min_fill_ratio: 0.95,
        }
    }

    pub fn update_position(&self, new_position: f64, execution_price: f64) {
        let mut current_pos = self.current_position.write().unwrap();
        let old_position = *current_pos;
        *current_pos = new_position;
        
        let mut entry_price = self.entry_price.write().unwrap();
        
        if old_position.abs() < 0.01 && new_position.abs() > 0.01 {
            *entry_price = Some(execution_price);
            *self.trades_count.write().unwrap() += 1;
        } else if new_position.abs() < 0.01 && old_position.abs() > 0.01 {
            if let Some(entry) = *entry_price {
                let pnl = old_position * (execution_price - entry) / entry;
                *self.total_pnl.write().unwrap() += pnl;
                self.update_statistics(pnl);
            }
            *entry_price = None;
        } else if old_position.signum() != new_position.signum() && new_position.abs() > 0.01 {
            if let Some(entry) = *entry_price {
                let pnl = old_position * (execution_price - entry) / entry;
                *self.total_pnl.write().unwrap() += pnl;
                self.update_statistics(pnl);
            }
            *entry_price = Some(execution_price);
            *self.trades_count.write().unwrap() += 1;
        }
    }

    fn update_statistics(&self, trade_pnl: f64) {
        let trades = *self.trades_count.read().unwrap();
        if trades == 0 {
            return;
        }

        let mut win_rate = self.win_rate.write().unwrap();
        let current_wins = *win_rate * (trades - 1) as f64;
        let new_wins = current_wins + if trade_pnl > 0.0 { 1.0 } else { 0.0 };
        *win_rate = new_wins / trades as f64;

        let pnl = *self.total_pnl.read().unwrap();
        let mut max_dd = self.max_drawdown_seen.write().unwrap();
        if pnl < 0.0 && pnl.abs() > *max_dd {
            *max_dd = pnl.abs();
        }
    }

    pub fn check_stop_conditions(&self, current_price: f64, entry_time: u64, current_time: u64) -> bool {
        let position = *self.current_position.read().unwrap();
        if position.abs() < 0.01 {
            return false;
        }

        let entry_price = match *self.entry_price.read().unwrap() {
            Some(p) => p,
            None => return false,
        };

        let pnl = position * (current_price - entry_price) / entry_price;
        
        if pnl <= -MAX_DRAWDOWN {
            return true;
        }

        if current_time > entry_time + 86400 * 1_000_000 {
            return true;
        }

        let params = self.parameters.read().unwrap();
        let current_z = (current_price.ln() - params.mu) / params.sigma;
        
        if current_z.abs() < 0.5 && pnl > MIN_PROFIT_BPS / 10000.0 {
            return true;
        }

        false
    }

    pub fn get_portfolio_metrics(&self) -> PortfolioMetrics {
        PortfolioMetrics {
            total_pnl: *self.total_pnl.read().unwrap(),
            win_rate: *self.win_rate.read().unwrap(),
            sharpe_ratio: *self.sharpe_ratio.read().unwrap(),
            max_drawdown: *self.max_drawdown_seen.read().unwrap(),
            trades_count: *self.trades_count.read().unwrap(),
            current_position: *self.current_position.read().unwrap(),
        }
    }

    pub async fn execute_rebalance(
        &self,
        client: &RpcClient,
        program_id: &Pubkey,
        market_account: &Pubkey,
        user_account: &Pubkey,
        signer: &Keypair,
        signal: &Signal,
        execution_params: &ExecutionParams,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let current_slot = client.get_slot()?;
        let blockhash = client.get_latest_blockhash()?;
        
        let accounts = vec![
            solana_sdk::instruction::AccountMeta::new(*market_account, false),
            solana_sdk::instruction::AccountMeta::new(*user_account, false),
            solana_sdk::instruction::AccountMeta::new(signer.pubkey(), true),
            solana_sdk::instruction::AccountMeta::new_readonly(solana_sdk::sysvar::clock::id(), false),
        ];

        let instruction_data = RebalanceInstruction {
            amount: (execution_params.amount_usd * 1e9) as u64,
            direction: execution_params.direction as i8,
            max_slippage_bps: (execution_params.max_slippage * 10000.0) as u16,
            min_fill_ratio: (execution_params.min_fill_ratio * 100.0) as u8,
        };

        let data = instruction_data.try_to_vec()?;
        
        let instruction = solana_sdk::instruction::Instruction {
            program_id: *program_id,
            accounts,
            data,
        };

        let mut transaction = solana_sdk::transaction::Transaction::new_with_payer(
            &[instruction],
            Some(&signer.pubkey()),
        );

        transaction.sign(&[signer], blockhash);

        for attempt in 0..MAX_RETRY_ATTEMPTS {
            match client.send_and_confirm_transaction_with_spinner_and_commitment(
                &transaction,
                CommitmentConfig::processed(),
            ) {
                Ok(signature) => {
                    self.update_position(signal.position_size, signal.expected_return);
                    return Ok(());
                }
                Err(e) => {
                    if attempt < MAX_RETRY_ATTEMPTS - 1 {
                        tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS)).await;
                        continue;
                    }
                    return Err(e.into());
                }
            }
        }

        Ok(())
    }

    pub fn validate_market_conditions(&self, market: &MarketState) -> bool {
        if market.spread_bps > 50.0 {
            return false;
        }

        if market.liquidity < MIN_LIQUIDITY_USD {
            return false;
        }

        if market.volatility > self.parameters.read().unwrap().sigma * 3.0 {
            return false;
        }

        let history = self.price_history.read().unwrap();
        if history.len() < MIN_OBSERVATIONS {
            return false;
        }

        let recent_prices: Vec<f64> = history.iter()
            .rev()
            .take(10)
            .map(|p| p.price)
            .collect();

        let price_changes: Vec<f64> = recent_prices.windows(2)
            .map(|w| (w[1] - w[0]).abs() / w[0])
            .collect();

        let max_change = price_changes.iter().max_by(|a, b| a.partial_cmp(b).unwrap()).unwrap_or(&0.0);
        
        if *max_change > 0.1 {
            return false;
        }

        true
    }

    pub fn emergency_close_position(
        &self,
        client: &RpcClient,
        program_id: &Pubkey,
        market_account: &Pubkey,
        user_account: &Pubkey,
        signer: &Keypair,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let current_position = *self.current_position.read().unwrap();
        if current_position.abs() < 0.01 {
            return Ok(());
        }

        let blockhash = client.get_latest_blockhash()?;
        
        let accounts = vec![
            solana_sdk::instruction::AccountMeta::new(*market_account, false),
            solana_sdk::instruction::AccountMeta::new(*user_account, false),
            solana_sdk::instruction::AccountMeta::new(signer.pubkey(), true),
        ];

        let instruction_data = ClosePositionInstruction {
            emergency: true,
        };

        let data = instruction_data.try_to_vec()?;
        
        let instruction = solana_sdk::instruction::Instruction {
            program_id: *program_id,
            accounts,
            data,
        };

        let mut transaction = solana_sdk::transaction::Transaction::new_with_payer(
            &[instruction],
            Some(&signer.pubkey()),
        );

        transaction.sign(&[signer], blockhash);
        client.send_and_confirm_transaction(&transaction)?;

        *self.current_position.write().unwrap() = 0.0;
        *self.entry_price.write().unwrap() = None;

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct ExecutionParams {
    pub amount_usd: f64,
    pub direction: f64,
    pub max_slippage: f64,
    pub urgency: f64,
    pub time_limit: Duration,
    pub min_fill_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct PortfolioMetrics {
    pub total_pnl: f64,
    pub win_rate: f64,
    pub sharpe_ratio: f64,
    pub max_drawdown: f64,
    pub trades_count: u64,
    pub current_position: f64,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct RebalanceInstruction {
    amount: u64,
    direction: i8,
    max_slippage_bps: u16,
    min_fill_ratio: u8,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct ClosePositionInstruction {
    emergency: bool,
}

impl Default for OrnsteinUhlenbeckMeanReverter {
    fn default() -> Self {
        Self::new()
    }
}

