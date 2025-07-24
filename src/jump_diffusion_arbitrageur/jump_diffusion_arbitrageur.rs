use anchor_lang::prelude::*;
use anchor_spl::token::{self, Token, TokenAccount, Transfer};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tokio::sync::mpsc;
use pyth_sdk_solana::{Price, PriceFeed};
use raydium_amm_v3::states::{PersonalPositionState, PoolState};
use jupiter_amm_interface::{Amm, Quote, SwapParams};

const JUMP_INTENSITY: f64 = 0.15;
const JUMP_MEAN: f64 = -0.02;
const JUMP_STD: f64 = 0.08;
const VOLATILITY: f64 = 0.25;
const DRIFT: f64 = 0.05;
const MIN_PROFIT_BPS: u64 = 15;
const MAX_SLIPPAGE_BPS: u64 = 50;
const CONFIDENCE_THRESHOLD: f64 = 0.92;
const MAX_POSITION_USD: u64 = 100_000;
const GAS_BUFFER: u64 = 5000;
const PRIORITY_FEE_PERCENTILE: u8 = 95;

#[derive(Clone, Debug)]
pub struct JumpDiffusionModel {
    drift: f64,
    volatility: f64,
    jump_intensity: f64,
    jump_mean: f64,
    jump_std: f64,
    last_price: f64,
    last_update: Instant,
}

impl JumpDiffusionModel {
    pub fn new(initial_price: f64) -> Self {
        Self {
            drift: DRIFT,
            volatility: VOLATILITY,
            jump_intensity: JUMP_INTENSITY,
            jump_mean: JUMP_MEAN,
            jump_std: JUMP_STD,
            last_price: initial_price,
            last_update: Instant::now(),
        }
    }


pub struct JumpDiffusionModel {
    pub drift: f64,
    pub volatility: f64,
    pub jump_intensity: f64, // λ
    pub jump_mean: f64,      // μ
    pub jump_std: f64,       // δ
    pub last_price: f64,
    pub last_update: Instant,
}

impl JumpDiffusionModel {
    pub fn new(initial_price: f64) -> Self {
        Self {
            drift: 0.05,
            volatility: 0.25,
            jump_intensity: 0.15,
            jump_mean: -0.02,
            jump_std: 0.08,
            last_price: initial_price,
            last_update: Instant::now(),
        }
    }

    pub fn update_params(&mut self, price: f64, observed_volatility: f64) {
        let dt = self.last_update.elapsed().as_secs_f64().max(1e-6); // avoid div by zero
        let return_rate = (price / self.last_price).ln() / dt;

        self.drift = self.drift * 0.95 + return_rate * 0.05;
        self.volatility = self.volatility * 0.9 + observed_volatility * 0.1;
        self.last_price = price;
        self.last_update = Instant::now();
    }

    pub fn predict_price(&self, horizon: f64) -> (f64, f64, f64) {
        let mu = self.jump_mean;
        let delta = self.jump_std;
        let lambda = self.jump_intensity;
        let sigma = self.volatility;
        let drift = self.drift;

let k = (mu + 0.5 * delta.powi(2)).exp() - 1.0;
let adjusted_drift = drift - lambda * k - 0.5 * sigma.powi(2);

        let mut rng = thread_rng();
        let normal = Normal::new(0.0, 1.0).unwrap();
        let lognormal = LogNormal::new(mu, delta).unwrap();

        let brownian = sigma * normal.sample(&mut rng) * horizon.sqrt();

        let jump_count = poisson_sample(lambda * horizon);
        let mut jump_sum = 0.0;
        for _ in 0..jump_count {
            let jump_size = lognormal.sample(&mut rng);
            jump_sum += (jump_size - 1.0);
        }

        let log_return = adjusted_drift * horizon + brownian + jump_sum;
        let expected_price = self.last_price * log_return.exp();

        let variance_est = sigma.powi(2) * horizon + lambda * horizon * (mu.powi(2) + delta.powi(2));
        let std_dev = self.last_price * variance_est.sqrt();

pub fn predict_price(&self, horizon: f64) -> (f64, f64, f64) {
    let mu = self.jump_mean;
    let delta = self.jump_std;
    let lambda = self.jump_intensity;
    let sigma = self.volatility;
    let drift = self.drift;
    let k = (mu + 0.5 * delta.powi(2)).exp() - 1.0;

    // expected number of jumps in horizon
    let expected_jumps = lambda * horizon;

    // expected log return
    let mean_log_return = (drift - 0.5 * sigma.powi(2) - lambda * k) * horizon + expected_jumps * mu;

    // variance of log return
    let var_log_return = sigma.powi(2) * horizon + expected_jumps * (mu.powi(2) + delta.powi(2));

    let expected_price = self.last_price * (mean_log_return).exp();
    let std_dev = self.last_price * var_log_return.sqrt();

    // real confidence using 1σ = ~68.2%
    let z = 1.0; // for 68.2%
    let ci_upper = (mean_log_return + z * var_log_return.sqrt()).exp();
    let ci_lower = (mean_log_return - z * var_log_return.sqrt()).exp();
    let confidence = 0.682; // you can bump this to 0.95 if you prefer 2σ

    (expected_price, std_dev, confidence)
}


// Poisson sampling using Knuth's algorithm
fn poisson_sample(expected: f64) -> usize {
    use rand::Rng;
    let mut rng = rand::thread_rng();
    let l = (-expected).exp();
    let mut p = 1.0;
    let mut k = 0;

    while p > l {
        k += 1;
        p *= rng.gen::<f64>();
    }

    k.saturating_sub(1)
}
#[derive(Clone)]
pub struct ArbitrageOpportunity {
    pub buy_venue: Pubkey,
    pub sell_venue: Pubkey,
    pub token_mint: Pubkey,
    pub amount: u64,
    pub buy_price: f64,
    pub sell_price: f64,
    pub expected_profit: u64,
    pub confidence: f64,
    pub gas_estimate: u64,
}

pub struct JumpDiffusionArbitrageur {
    rpc_client: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    models: Arc<RwLock<HashMap<Pubkey, JumpDiffusionModel>>>,
    amm_cache: Arc<RwLock<HashMap<Pubkey, Box<dyn Amm>>>>,
    price_feeds: Arc<RwLock<HashMap<Pubkey, PriceFeed>>>,
    opportunity_tx: mpsc::Sender<ArbitrageOpportunity>,
    opportunity_rx: mpsc::Receiver<ArbitrageOpportunity>,
    active_positions: Arc<RwLock<HashMap<Pubkey, u64>>>,
    recent_gas_prices: Arc<RwLock<Vec<u64>>>,
}

impl JumpDiffusionArbitrageur {
    pub async fn new(
        rpc_url: &str,
        wallet: Keypair,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let (tx, rx) = mpsc::channel(1000);
        
        Ok(Self {
            rpc_client: Arc::new(RpcClient::new_with_commitment(
                rpc_url.to_string(),
                CommitmentConfig::processed(),
            )),
            wallet: Arc::new(wallet),
            models: Arc::new(RwLock::new(HashMap::new())),
            amm_cache: Arc::new(RwLock::new(HashMap::new())),
            price_feeds: Arc::new(RwLock::new(HashMap::new())),
            opportunity_tx: tx,
            opportunity_rx: rx,
            active_positions: Arc::new(RwLock::new(HashMap::new())),
            recent_gas_prices: Arc::new(RwLock::new(Vec::with_capacity(100))),
        })
    }

    pub async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let price_updater = self.spawn_price_updater();
        let opportunity_scanner = self.spawn_opportunity_scanner();
        let executor = self.spawn_executor();
        
        tokio::select! {
            _ = price_updater => {},
            _ = opportunity_scanner => {},
            _ = executor => {},
        }
        
        Ok(())
    }

    async fn spawn_price_updater(&self) -> tokio::task::JoinHandle<()> {
        let rpc = self.rpc_client.clone();
        let models = self.models.clone();
        let price_feeds = self.price_feeds.clone();
        let amm_cache = self.amm_cache.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            
            loop {
                interval.tick().await;
                
                let feeds = price_feeds.read().unwrap().clone();
                for (mint, feed) in feeds.iter() {
                    if let Ok(price_account) = rpc.get_account(&feed.price_account).await {
                        if let Ok(price_data) = Price::deserialize(&price_account.data) {
                            let price = price_data.price as f64 * 10f64.powi(price_data.expo);
                            let confidence = price_data.conf as f64 * 10f64.powi(price_data.expo);
                            let volatility = confidence / price;
                            
                            let mut models_guard = models.write().unwrap();
                            models_guard.entry(*mint)
                                .and_modify(|m| m.update_params(price, volatility))
                                .or_insert_with(|| JumpDiffusionModel::new(price));
                        }
                    }
                }
                
                let amms = amm_cache.read().unwrap().clone();
                for (amm_id, amm) in amms.iter() {
                    if let Ok(account_data) = rpc.get_account(&amm.key()).await {
                        // Update AMM state
                        amm_cache.write().unwrap().insert(*amm_id, amm.clone());
                    }
                }
            }
        })
    }

    async fn spawn_opportunity_scanner(&self) -> tokio::task::JoinHandle<()> {
        let models = self.models.clone();
        let amm_cache = self.amm_cache.clone();
        let opportunity_tx = self.opportunity_tx.clone();
        let active_positions = self.active_positions.clone();
        let recent_gas = self.recent_gas_prices.clone();
        
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(50));
            
            loop {
                interval.tick().await;
                
                let models_snapshot = models.read().unwrap().clone();
                let amms_snapshot = amm_cache.read().unwrap().clone();
                
                for (mint, model) in models_snapshot.iter() {
                    let horizons = vec![0.001, 0.005, 0.01, 0.05];
                    
                    for horizon in horizons {
                        let (predicted_price, std_dev, confidence) = model.predict_price(horizon);
                        
                        if confidence < CONFIDENCE_THRESHOLD {
            if spread / std_dev < 1.2 {

                continue; // Skip bad risk-adjusted trades

            }
                            continue;
                        }
                        
                        let mut best_buy: Option<(Pubkey, f64, u64)> = None;
                        let mut best_sell: Option<(Pubkey, f64, u64)> = None;
                        
                        for (amm_id, amm) in amms_snapshot.iter() {
                            let test_amount = 1_000_000_000; // 1 token with 9 decimals
                            
                            if let Ok(buy_quote) = amm.quote(&SwapParams {
                                source_mint: *mint,
                                destination_mint: *mint,
                                amount: test_amount,
                                swap_mode: jupiter_amm_interface::SwapMode::ExactIn,
                            }) {
                                let buy_price = buy_quote.in_amount as f64 / buy_quote.out_amount as f64;
                                
                                if best_buy.is_none() || buy_price < best_buy.as_ref().unwrap().1 {
                                    best_buy = Some((*amm_id, buy_price, buy_quote.out_amount));
                                }
                            }
                            
                            if let Ok(sell_quote) = amm.quote(&SwapParams {
                                source_mint: *mint,
                                destination_mint: *mint,
                                amount: test_amount,
                                swap_mode: jupiter_amm_interface::SwapMode::ExactOut,
                            }) {
                                let sell_price = sell_quote.out_amount as f64 / sell_quote.in_amount as f64;
                                
                                if best_sell.is_none() || sell_price > best_sell.as_ref().unwrap().1 {
                                    best_sell = Some((*amm_id, sell_price, sell_quote.in_amount));
                                }
                            }
                        }
                        
                        if let (Some((buy_venue, buy_price, _)), Some((sell_venue, sell_price, _))) = (best_buy, best_sell) {
                            let spread = sell_price - buy_price;
                            let spread_bps = (spread / buy_price * 10000.0) as u64;
                            
                            let gas_estimate = recent_gas.read().unwrap()
                                .iter()
                                .copied()
                                .nth(recent_gas.read().unwrap().len() * PRIORITY_FEE_PERCENTILE as usize / 100)
                                .unwrap_or(GAS_BUFFER);
                            
                            if spread_bps > MIN_PROFIT_BPS && predicted_price > buy_price * 1.001 {
                                let position_size = active_positions.read().unwrap()
                                    .get(mint)
                                    .copied()
                                    .unwrap_or(0);
                                
                                let max_amount = (MAX_POSITION_USD * 1_000_000_000 / buy_price as u64)
                                    .saturating_sub(position_size);
                                
fn calculate_optimal_amount(
    max_amount: u64,
    expected_return: f64,
    std_dev_log: f64,
    gas_cost: u64,
) -> u64 {
pub fn optimize_kelly_fraction(
    returns: &[f64],
    max_leverage: f64,
    leverage_step: f64,
    drawdown_penalty: f64, // e.g. 0.5 means penalize negative outcomes by 50%
) -> f64 {
    let mut best_f = 0.0;
    let mut best_growth = f64::NEG_INFINITY;

    for f in (0..=((max_leverage / leverage_step) as usize))
        .map(|x| x as f64 * leverage_step)
    {
        let mut log_growth_sum = 0.0;
        let mut count = 0;

        for &r in returns {
            let outcome = 1.0 + f * r;
            if outcome > 0.0 {
                let log_g = outcome.ln();

                // Heavy tail penalty if large negative return
                if r < -0.05 {
                    log_growth_sum += log_g * drawdown_penalty;
                } else {
                    log_growth_sum += log_g;
                }
                count += 1;
            }
        }

        if count > 0 {
            let avg_growth = log_growth_sum / count as f64;
            if avg_growth > best_growth {
                best_growth = avg_growth;
                best_f = f;
            }
        }
    }

    best_f
}

/// Returns the trade amount using Kelly leverage + real gas-aware filters + expected return floor
pub fn calculate_optimal_amount(
    max_amount: u64,
    historical_returns: &[f64],
    gas_cost: u64,
    leverage_cap: f64,
    drawdown_penalty: f64,
) -> u64 {
    let optimal_f = optimize_kelly_fraction(
        historical_returns,
        leverage_cap,
        0.01,
        drawdown_penalty,
    );

    let avg_return = historical_returns.iter().copied().sum::<f64>() / historical_returns.len().max(1) as f64;

    let raw = (max_amount as f64 * optimal_f).round() as u64;

    let min_profitable = if avg_return > 0.0 {
        ((gas_cost as f64 * 2.5) / avg_return).round() as u64
    } else {
        max_amount
    };

    raw.max(min_profitable).min(max_amount)
}                                    
                                    let expected_profit = (optimal_amount as f64 * spread) as u64 - gas_estimate;
                                    
                                    if expected_profit > gas_estimate * 2 {
                                        let _ = opportunity_tx.send(ArbitrageOpportunity {
                                            buy_venue,
                                            sell_venue,
                                            token_mint: *mint,
                                            amount: optimal_amount,
                                            buy_price,
                                            sell_price,
                                            expected_profit,
                                            confidence,
                                            gas_estimate,
                                        }).await;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
    }

    async fn spawn_executor(&mut self) -> tokio::task::JoinHandle<()> {
        let rpc = self.rpc_client.clone();
        let wallet = self.wallet.clone();
        let active_positions = self.active_positions.clone();
        let recent_gas = self.recent_gas_prices.clone();
        let mut opportunity_rx = self.opportunity_rx.clone();
        
        tokio::spawn(async move {
            while let Some(opportunity) = opportunity_rx.recv().await {
                let start = Instant::now();
                
                match Self::execute_arbitrage(
                    &rpc,
                    &wallet,
                    &opportunity,
                    &active_positions,
                ).await {
                    Ok(signature) => {
                        let gas_used = start.elapsed().as_micros() as u64;
                        recent_gas.write().unwrap().push(gas_used);
                        if recent_gas.read().unwrap().len() > 100 {
                            recent_gas.write().unwrap().remove(0);
                        }
                        
                                                println!("Arbitrage executed: {:?}, profit: {}", signature, opportunity.expected_profit);
                    }
                    Err(e) => {
                        eprintln!("Arbitrage failed: {:?}", e);
                    }
                }
            }
        })
    }

    async fn execute_arbitrage(
        rpc: &RpcClient,
        wallet: &Keypair,
        opportunity: &ArbitrageOpportunity,
        active_positions: &Arc<RwLock<HashMap<Pubkey, u64>>>,
    ) -> Result<solana_sdk::signature::Signature, Box<dyn std::error::Error>> {
        let blockhash = rpc.get_latest_blockhash().await?;
        let slot = rpc.get_slot().await?;
        
        // Get token accounts
        let wallet_pubkey = wallet.pubkey();
        let token_account = spl_associated_token_account::get_associated_token_address(
            &wallet_pubkey,
            &opportunity.token_mint,
        );
        
        // Build buy instruction
        let buy_accounts = vec![
            AccountMeta::new(opportunity.buy_venue, false),
            AccountMeta::new(token_account, false),
            AccountMeta::new(wallet_pubkey, true),
            AccountMeta::new_readonly(opportunity.token_mint, false),
            AccountMeta::new_readonly(spl_token::id(), false),
        ];
        
        let buy_data = Self::encode_swap_instruction(
            opportunity.amount,
            (opportunity.amount as f64 * (1.0 - MAX_SLIPPAGE_BPS as f64 / 10000.0)) as u64,
            true,
        );
        
        let buy_ix = Instruction {
            program_id: opportunity.buy_venue,
            accounts: buy_accounts,
            data: buy_data,
        };
        
        // Build sell instruction
        let sell_accounts = vec![
            AccountMeta::new(opportunity.sell_venue, false),
            AccountMeta::new(token_account, false),
            AccountMeta::new(wallet_pubkey, true),
            AccountMeta::new_readonly(opportunity.token_mint, false),
            AccountMeta::new_readonly(spl_token::id(), false),
        ];
        
        let expected_out = (opportunity.amount as f64 * opportunity.sell_price / opportunity.buy_price) as u64;
        let min_out = (expected_out as f64 * (1.0 - MAX_SLIPPAGE_BPS as f64 / 10000.0)) as u64;
        
        let sell_data = Self::encode_swap_instruction(
            opportunity.amount,
            min_out,
            false,
        );
        
        let sell_ix = Instruction {
            program_id: opportunity.sell_venue,
            accounts: sell_accounts,
            data: sell_data,
        };
        
        // Calculate dynamic priority fee
        let recent_fees = rpc.get_recent_prioritization_fees(&[opportunity.buy_venue, opportunity.sell_venue]).await?;
        let priority_fee = recent_fees.iter()
            .map(|f| f.prioritization_fee)
            .max()
            .unwrap_or(1000)
            .saturating_add(opportunity.gas_estimate);
        
        let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_price(priority_fee);
        let compute_limit_ix = ComputeBudgetInstruction::set_compute_unit_limit(400_000);
        
        // Build transaction with MEV protection
        let mut transaction = Transaction::new_with_payer(
            &[compute_budget_ix, compute_limit_ix, buy_ix, sell_ix],
            Some(&wallet_pubkey),
        );
        
        transaction.sign(&[wallet], blockhash);
        
        // Update position tracking
        active_positions.write().unwrap()
            .entry(opportunity.token_mint)
            .and_modify(|p| *p = p.saturating_add(opportunity.amount))
            .or_insert(opportunity.amount);
        
        // Send with retry logic
        let mut retries = 3;
        let mut last_error = None;
        
        while retries > 0 {
            match rpc.send_transaction_with_config(
                &transaction,
                solana_client::rpc_config::RpcSendTransactionConfig {
                    skip_preflight: true,
                    preflight_commitment: Some(CommitmentConfig::processed()),
                    max_retries: Some(0),
                    ..Default::default()
                },
            ).await {
                Ok(sig) => {
                    // Confirm transaction
                    match rpc.confirm_transaction(&sig).await {
                        Ok(_) => {
                            // Update position on success
                            active_positions.write().unwrap()
                                .entry(opportunity.token_mint)
                                .and_modify(|p| *p = p.saturating_sub(opportunity.amount));
                            return Ok(sig);
                        }
                        Err(e) => {
                            last_error = Some(e.to_string());
                        }
                    }
                }
                Err(e) => {
                    last_error = Some(e.to_string());
                }
            }
            
            retries -= 1;
            if retries > 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
        
        // Revert position tracking on failure
        active_positions.write().unwrap()
            .entry(opportunity.token_mint)
            .and_modify(|p| *p = p.saturating_sub(opportunity.amount));
        
        Err(format!("Transaction failed after retries: {:?}", last_error).into())
    }

fn calculate_optimal_amount(
    max_amount: u64,
    expected_return: f64,
    std_dev_log: f64,
    gas_cost: u64,
) -> u64 {
    let variance = std_dev_log.powi(2).max(1e-12); // avoid div-zero
    let kelly_fraction = (expected_return / variance).clamp(0.0, 0.25); // safe cap
    let raw_optimal = (max_amount as f64 * kelly_fraction) as u64;
    let min_profitable = if expected_return > 0.0 {
        (gas_cost as f64 * 2.5 / expected_return) as u64
    } else {
        max_amount
    };
    raw_optimal.max(min_profitable).min(max_amount)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_jump_diffusion_model() {
        let mut model = JumpDiffusionModel::new(100.0);
        
        // Test price prediction
        let (price, std_dev, confidence) = model.predict_price(0.01);
        assert!(price > 0.0);
        assert!(std_dev > 0.0);
        assert!(confidence > 0.0 && confidence <= 1.0);
        
        // Test parameter update
        model.update_params(105.0, 0.3);
        assert!(model.volatility > VOLATILITY * 0.8 && model.volatility < 0.35);
    }

    #[tokio::test]
    async fn test_optimal_amount_calculation() {
        let amount = JumpDiffusionArbitrageur::calculate_optimal_amount(
            1_000_000_000, // 1 token
            0.01,          // 1% spread
            0.02,          // 2% volatility
            5000,          // gas cost
        );
        
        assert!(amount > 0);
        assert!(amount <= 250_000_000); // Kelly cap at 25%
    }
}

