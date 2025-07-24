use anchor_client::solana_sdk::{
    commitment_config::CommitmentConfig,
    pubkey::Pubkey,
    signature::{Keypair, Signer},
};
use anchor_lang::prelude::*;
use arrayref::array_ref;
use rayon::prelude::*;
use rust_decimal::prelude::*;
use serum_dex::state::{Market, MarketState};
use solana_client::rpc_client::RpcClient;
use solana_program::{
    clock::Clock,
    instruction::{AccountMeta, Instruction},
    program_pack::Pack,
    system_program,
};
use spl_token::state::Account as TokenAccount;
use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};

const LEVY_ALPHA: f64 = 1.7;
const LEVY_BETA: f64 = 0.5;
const LEVY_GAMMA: f64 = 0.01;
const LEVY_DELTA: f64 = 0.0;
const JUMP_THRESHOLD: f64 = 0.015;
const MIN_PROFIT_BPS: u64 = 50;
const MAX_SLIPPAGE_BPS: u64 = 30;
const CONFIDENCE_THRESHOLD: f64 = 0.85;
const MAX_POSITION_SIZE: u64 = 100_000_000_000;
const COOLDOWN_MS: u64 = 100;

#[derive(Clone, Debug)]
pub struct LevyParameters {
    alpha: f64,
    beta: f64,
    gamma: f64,
    delta: f64,
    jump_intensity: f64,
    drift: f64,
    volatility: f64,
}

#[derive(Clone, Debug)]
pub struct MarketSnapshot {
    timestamp: u64,
    bid: f64,
    ask: f64,
    bid_size: f64,
    ask_size: f64,
    last_trade: f64,
    volume: f64,
    spread: f64,
    imbalance: f64,
}

#[derive(Clone, Debug)]
pub struct FlashLoanOpportunity {
    market_a: Pubkey,
    market_b: Pubkey,
    token_mint: Pubkey,
    entry_price: f64,
    exit_price: f64,
    size: u64,
    expected_profit: f64,
    confidence: f64,
    predicted_jump: f64,
    execution_window: u64,
}

pub struct LevyProcessPredictor {
    rpc_client: Arc<RpcClient>,
    keypair: Arc<Keypair>,
    market_cache: Arc<RwLock<HashMap<Pubkey, MarketState>>>,
    price_history: Arc<RwLock<HashMap<Pubkey, VecDeque<MarketSnapshot>>>>,
    levy_params: Arc<RwLock<HashMap<Pubkey, LevyParameters>>>,
    pending_opportunities: Arc<Mutex<Vec<FlashLoanOpportunity>>>,
    last_execution: Arc<RwLock<HashMap<Pubkey, Instant>>>,
}

impl LevyProcessPredictor {
    pub fn new(rpc_url: &str, keypair: Keypair) -> Self {
        Self {
            rpc_client: Arc::new(RpcClient::new_with_commitment(
                rpc_url.to_string(),
                CommitmentConfig::processed(),
            )),
            keypair: Arc::new(keypair),
            market_cache: Arc::new(RwLock::new(HashMap::new())),
            price_history: Arc::new(RwLock::new(HashMap::new())),
            levy_params: Arc::new(RwLock::new(HashMap::new())),
            pending_opportunities: Arc::new(Mutex::new(Vec::new())),
            last_execution: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn run(&self) {
        let update_handle = self.spawn_market_updater();
        let predictor_handle = self.spawn_levy_predictor();
        let executor_handle = self.spawn_flashloan_executor();

        tokio::select! {
            _ = update_handle => {},
            _ = predictor_handle => {},
            _ = executor_handle => {},
        }
    }

    fn spawn_market_updater(&self) -> tokio::task::JoinHandle<()> {
        let rpc_client = self.rpc_client.clone();
        let market_cache = self.market_cache.clone();
        let price_history = self.price_history.clone();

        tokio::spawn(async move {
            let markets = vec![
                "7dLVkUfBVfCGkFhSXDCq1ukM9usathSgS716t643iFGF",
                "8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYwn1XTh6",
                "DZjbn4XC8qoHKikZqzmhemykVzmossoayV9ffbsUqxVj",
                "HWHvQhFmJB3NUcu1aihKmrKegfVxBEHzwVX6yZCKEsi1",
            ];

            loop {
                for market_str in &markets {
                    if let Ok(market_pubkey) = market_str.parse::<Pubkey>() {
                        if let Ok(account) = rpc_client.get_account(&market_pubkey) {
                            if let Ok(market_state) = Market::unpack(&account.data[5..]) {
                                let mut cache = market_cache.write().unwrap();
                                cache.insert(market_pubkey, market_state.inner);

                                let snapshot = Self::extract_market_snapshot(&market_state.inner);
                                let mut history = price_history.write().unwrap();
                                let market_history = history.entry(market_pubkey).or_insert_with(|| {
                                    VecDeque::with_capacity(1000)
                                });
                                
                                market_history.push_back(snapshot);
                                if market_history.len() > 1000 {
                                    market_history.pop_front();
                                }
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
    }

    fn spawn_levy_predictor(&self) -> tokio::task::JoinHandle<()> {
        let price_history = self.price_history.clone();
        let levy_params = self.levy_params.clone();
        let pending_opportunities = self.pending_opportunities.clone();
        let market_cache = self.market_cache.clone();

        tokio::spawn(async move {
            loop {
                let history = price_history.read().unwrap();
                let markets: Vec<_> = history.keys().cloned().collect();
                drop(history);

                for i in 0..markets.len() {
                    for j in i+1..markets.len() {
                        let market_a = markets[i];
                        let market_b = markets[j];

                        if let Some(opportunity) = Self::detect_levy_opportunity(
                            &price_history,
                            &levy_params,
                            &market_cache,
                            market_a,
                            market_b,
                        ) {
                            let mut opportunities = pending_opportunities.lock().unwrap();
                            if opportunities.len() < 50 {
                                opportunities.push(opportunity);
                            }
                        }
                    }
                }

                Self::update_levy_parameters(&price_history, &levy_params);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
    }

    fn spawn_flashloan_executor(&self) -> tokio::task::JoinHandle<()> {
        let rpc_client = self.rpc_client.clone();
        let keypair = self.keypair.clone();
        let pending_opportunities = self.pending_opportunities.clone();
        let last_execution = self.last_execution.clone();

        tokio::spawn(async move {
            loop {
                let opportunity = {
                    let mut opportunities = pending_opportunities.lock().unwrap();
                    opportunities.sort_by(|a, b| {
                        b.expected_profit.partial_cmp(&a.expected_profit).unwrap()
                    });
                    opportunities.pop()
                };

                if let Some(opp) = opportunity {
                    let should_execute = {
                        let executions = last_execution.read().unwrap();
                        if let Some(last_time) = executions.get(&opp.token_mint) {
                            last_time.elapsed().as_millis() > COOLDOWN_MS as u128
                        } else {
                            true
                        }
                    };

                    if should_execute && opp.confidence > CONFIDENCE_THRESHOLD {
                        match Self::execute_flashloan(
                            &rpc_client,
                            &keypair,
                            &opp,
                        ).await {
                            Ok(_) => {
                                let mut executions = last_execution.write().unwrap();
                                executions.insert(opp.token_mint, Instant::now());
                            },
                            Err(_) => {}
                        }
                    }
                }

                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
    }

    fn extract_market_snapshot(market: &MarketState) -> MarketSnapshot {
        let clock = Clock::get().unwrap_or_default();
        let timestamp = clock.unix_timestamp as u64;

        let bid = market.native_pc_bid_quote() as f64;
        let ask = market.native_pc_ask_quote() as f64;
        let spread = (ask - bid) / bid;
        let mid = (bid + ask) / 2.0;

        MarketSnapshot {
            timestamp,
            bid,
            ask,
            bid_size: market.native_coin_bid() as f64,
            ask_size: market.native_coin_ask() as f64,
            last_trade: mid,
            volume: 0.0,
            spread,
            imbalance: (market.native_coin_bid() as f64 - market.native_coin_ask() as f64).abs(),
        }
    }

    fn detect_levy_opportunity(
        price_history: &Arc<RwLock<HashMap<Pubkey, VecDeque<MarketSnapshot>>>>,
        levy_params: &Arc<RwLock<HashMap<Pubkey, LevyParameters>>>,
        market_cache: &Arc<RwLock<HashMap<Pubkey, MarketState>>>,
        market_a: Pubkey,
        market_b: Pubkey,
    ) -> Option<FlashLoanOpportunity> {
        let history = price_history.read().unwrap();
        let params = levy_params.read().unwrap();
        let cache = market_cache.read().unwrap();

        let history_a = history.get(&market_a)?;
        let history_b = history.get(&market_b)?;

        if history_a.len() < 100 || history_b.len() < 100 {
            return None;
        }

        let levy_a = params.get(&market_a).cloned().unwrap_or_else(|| {
            Self::estimate_levy_parameters(history_a)
        });
        let levy_b = params.get(&market_b).cloned().unwrap_or_else(|| {
            Self::estimate_levy_parameters(history_b)
        });

        let jump_prob_a = Self::calculate_jump_probability(&levy_a, history_a);
        let jump_prob_b = Self::calculate_jump_probability(&levy_b, history_b);

        let price_a = history_a.back()?.last_trade;
        let price_b = history_b.back()?.last_trade;

        let predicted_jump_a = Self::predict_levy_jump(&levy_a, price_a);
        let predicted_jump_b = Self::predict_levy_jump(&levy_b, price_b);

        let cross_correlation = Self::calculate_cross_correlation(history_a, history_b);

        if (jump_prob_a > 0.7 || jump_prob_b > 0.7) && cross_correlation.abs() < 0.3 {
            let size = Self::calculate_optimal_size(
                history_a.back()?,
                history_b.back()?,
                &levy_a,
                &levy_b,
            );

            let expected_profit = (predicted_jump_a.abs() + predicted_jump_b.abs()) * size as f64;
            let confidence = (jump_prob_a.max(jump_prob_b) + (1.0 - cross_correlation.abs())) / 2.0;

            if expected_profit > (size as f64 * MIN_PROFIT_BPS as f64 / 10000.0) {
                return Some(FlashLoanOpportunity {
                    market_a,
                    market_b,
                    token_mint: Pubkey::default(),
                    entry_price: price_a,
                    exit_price: price_a + predicted_jump_a,
                    size,
                    expected_profit,
                    confidence,
                    predicted_jump: predicted_jump_a,
                    execution_window: 50,
                });
            }
        }

        None
    }

    fn estimate_levy_parameters(history: &VecDeque<MarketSnapshot>) -> LevyParameters {
        let returns: Vec<f64> = history.windows(2)
            .map(|w| (w[1].last_trade / w[0].last_trade).ln())
            .collect();

        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;

        let kurtosis = returns.iter()
            .map(|r| (r - mean).powi(4))
            .sum::<f64>() / (returns.len() as f64 * variance.powi(2));

        let skewness = returns.iter()
            .map(|r| (r - mean).powi(3))
            .sum::<f64>() / (returns.len() as f64 * variance.powf(1.5));

        let alpha = 2.0 / (1.0 + (-kurtosis / 3.0).exp()).min(2.0).max(0.1);
        let beta = skewness.signum() * (skewness.abs().min(1.0));
        let gamma = variance.sqrt() * (alpha / 2.0).sqrt();
        let delta = mean - beta * gamma * (1.0 - alpha).tan() * std::f64::consts::PI / alpha;

        let jump_intensity = returns.iter()
            .filter(|r| r.abs() > 2.0 * variance.sqrt())
            .count() as f64 / returns.len() as f64;

        LevyParameters {
            alpha,
            beta,
            gamma,
            delta,
            jump_intensity,
            drift: mean,
            volatility: variance.sqrt(),
        }
    }

    fn calculate_jump_probability(params: &LevyParameters, history: &VecDeque<MarketSnapshot>) -> f64 {
        if history.len() < 20 {
            return 0.0;
        }

        let recent = &history.as_slices().1[history.len().saturating_sub(20)..];
        let mut features = vec![0.0; 8];

        for i in 1..recent.len() {
            let ret = (recent[i].last_trade / recent[i-1].last_trade).ln();
            features[0] += ret;
            features[1] += ret.powi(2);
            features[2] += recent[i].spread;
            features[3] += recent[i].imbalance;
            features[4] = features[4].max(recent[i].volume);
        }

        features[0] /= recent.len() as f64;
        features[1] = (features[1] / recent.len() as f64 - features[0].powi(2)).sqrt();
        features[2] /= recent.len() as f64;
        features[3] /= recent.len() as f64;
        
        let spread_widening = recent.last().unwrap().spread / recent.first().unwrap().spread;
        features[5] = spread_widening;
        
        let order_flow_imbalance = recent.last().unwrap().imbalance;
        features[6] = order_flow_imbalance;
        
        let momentum = (recent.last().unwrap().last_trade / recent[recent.len()/2].last_trade).ln();
        features[7] = momentum;

        let base_prob = params.jump_intensity;
        let spread_factor = 1.0 + (spread_widening - 1.0).max(0.0).min(2.0);
        let imbalance_factor = 1.0 + (order_flow_imbalance / 1000.0).min(1.0);
        let momentum_factor = 1.0 + momentum.abs().min(0.1) * 10.0;
        
        (base_prob * spread_factor * imbalance_factor * momentum_factor).min(0.95)
    }

    fn predict_levy_jump(params: &LevyParameters, current_price: f64) -> f64 {
        let u = fastrand::f64();
        let v = fastrand::f64() * 2.0 - 1.0;
        
        let phi = v * std::f64::consts::PI / 2.0;
        let w = -u.ln();
        
        let x = if params.alpha == 1.0 {
            2.0 / std::f64::consts::PI * ((std::f64::consts::PI / 2.0 + params.beta * phi) * phi.tan()
                - params.beta * (std::f64::consts::PI / 2.0 + params.beta * phi).ln())
        } else {
            let zeta = -params.beta * (std::f64::consts::PI * params.alpha / 2.0).tan();
            let xi = (1.0 + zeta.powi(2)).powf(1.0 / (2.0 * params.alpha));
            
            ((1.0 + params.beta * phi.tan()).sin() * xi) / phi.cos().powf(1.0 / params.alpha)
                * ((phi.cos() - params.beta * phi.sin()) / w).powf((1.0 - params.alpha) / params.alpha)
        };
        
        let jump_size = params.gamma * x + params.delta;
        current_price * jump_size.max(-0.5).min(0.5)
    }

    fn calculate_cross_correlation(
        history_a: &VecDeque<MarketSnapshot>,
        history_b: &VecDeque<MarketSnapshot>,
    ) -> f64 {
        let n = history_a.len().min(history_b.len()).min(100);
        if n < 20 {
            return 0.0;
        }

        let returns_a: Vec<f64> = history_a.iter()
            .skip(history_a.len() - n)
            .collect::<Vec<_>>()
            .windows(2)
            .map(|w| (w[1].last_trade / w[0].last_trade).ln())
            .collect();

        let returns_b: Vec<f64> = history_b.iter()
            .skip(history_b.len() - n)
            .collect::<Vec<_>>()
            .windows(2)
            .map(|w| (w[1].last_trade / w[0].last_trade).ln())
            .collect();

        let mean_a = returns_a.iter().sum::<f64>() / returns_a.len() as f64;
        let mean_b = returns_b.iter().sum::<f64>() / returns_b.len() as f64;

        let cov = returns_a.iter().zip(returns_b.iter())
            .map(|(a, b)| (a - mean_a) * (b - mean_b))
            .sum::<f64>() / returns_a.len() as f64;

        let std_a = (returns_a.iter()
            .map(|r| (r - mean_a).powi(2))
            .sum::<f64>() / returns_a.len() as f64).sqrt();

        let std_b = (returns_b.iter()
            .map(|r| (r - mean_b).powi(2))
            .sum::<f64>() / returns_b.len() as f64).sqrt();

        if std_a > 0.0 && std_b > 0.0 {
            cov / (std_a * std_b)
        } else {
            0.0
        }
    }

    fn calculate_optimal_size(
        snapshot_a: &MarketSnapshot,
        snapshot_b: &MarketSnapshot,
        params_a: &LevyParameters,
        params_b: &LevyParameters,
    ) -> u64 {
        let available_liquidity = snapshot_a.bid_size.min(snapshot_b.ask_size);
        let max_impact = 0.001;
        
        let price_impact_a = available_liquidity / (snapshot_a.bid_size + snapshot_a.ask_size);
        let price_impact_b = available_liquidity / (snapshot_b.bid_size + snapshot_b.ask_size);
        
        let vol_adjusted_size_a = (max_impact / params_a.volatility) * available_liquidity;
        let vol_adjusted_size_b = (max_impact / params_b.volatility) * available_liquidity;
        
        let optimal = vol_adjusted_size_a.min(vol_adjusted_size_b)
            .min(available_liquidity * 0.1)
            .min(MAX_POSITION_SIZE as f64);
        
        (optimal * 0.8) as u64
    }

    fn update_levy_parameters(
        price_history: &Arc<RwLock<HashMap<Pubkey, VecDeque<MarketSnapshot>>>>,
        levy_params: &Arc<RwLock<HashMap<Pubkey, LevyParameters>>>,
    ) {
        let history = price_history.read().unwrap();
        let mut params = levy_params.write().unwrap();

        for (market, snapshots) in history.iter() {
            if snapshots.len() >= 100 {
                let new_params = Self::estimate_levy_parameters(snapshots);
                
                if let Some(existing) = params.get_mut(market) {
                    existing.alpha = 0.9 * existing.alpha + 0.1 * new_params.alpha;
                    existing.beta = 0.9 * existing.beta + 0.1 * new_params.beta;
                    existing.gamma = 0.9 * existing.gamma + 0.1 * new_params.gamma;
                    existing.delta = 0.9 * existing.delta + 0.1 * new_params.delta;
                    existing.jump_intensity = 0.8 * existing.jump_intensity + 0.2 * new_params.jump_intensity;
                    existing.drift = 0.7 * existing.drift + 0.3 * new_params.drift;
                    existing.volatility = 0.8 * existing.volatility + 0.2 * new_params.volatility;
                } else {
                    params.insert(*market, new_params);
                }
            }
        }
    }

    async fn execute_flashloan(
        rpc_client: &RpcClient,
        keypair: &Keypair,
        opportunity: &FlashLoanOpportunity,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let blockhash = rpc_client.get_latest_blockhash()?;
        
        let flash_loan_program = Pubkey::from_str("FLASHkGVm8yCvNxZrPGqXTcczn7zUfpnSzz7BwdeHvgp")?;
        let token_program = spl_token::id();
        
        let (authority_pda, _) = Pubkey::find_program_address(
            &[b"authority", keypair.pubkey().as_ref()],
            &flash_loan_program,
        );

        let user_token_account = spl_associated_token_account::get_associated_token_address(
            &keypair.pubkey(),
            &opportunity.token_mint,
        );

        let (pool_account, _) = Pubkey::find_program_address(
            &[b"pool", opportunity.token_mint.as_ref()],
            &flash_loan_program,
        );

        let mut instructions = vec![];

        let borrow_data = BorrowInstruction {
            amount: opportunity.size,
            expected_fee: (opportunity.size * 3) / 1000,
        };

        instructions.push(Instruction {
            program_id: flash_loan_program,
            accounts: vec![
                AccountMeta::new(keypair.pubkey(), true),
                AccountMeta::new(pool_account, false),
                AccountMeta::new(user_token_account, false),
                AccountMeta::new_readonly(authority_pda, false),
                AccountMeta::new_readonly(token_program, false),
                AccountMeta::new_readonly(system_program::id(), false),
            ],
            data: borrow_data.try_to_vec()?,
        });

        let arb_instruction = self.create_arbitrage_instruction(
            opportunity,
            &keypair.pubkey(),
            &user_token_account,
        )?;
        instructions.push(arb_instruction);

        let repay_data = RepayInstruction {
            amount: opportunity.size,
            fee: (opportunity.size * 3) / 1000,
        };

        instructions.push(Instruction {
            program_id: flash_loan_program,
            accounts: vec![
                AccountMeta::new(keypair.pubkey(), true),
                AccountMeta::new(pool_account, false),
                AccountMeta::new(user_token_account, false),
                AccountMeta::new_readonly(authority_pda, false),
                AccountMeta::new_readonly(token_program, false),
            ],
            data: repay_data.try_to_vec()?,
        });

        let mut transaction = solana_sdk::transaction::Transaction::new_with_payer(
            &instructions,
            Some(&keypair.pubkey()),
        );

        transaction.sign(&[keypair], blockhash);
        
        let config = solana_client::rpc_config::RpcSendTransactionConfig {
            skip_preflight: true,
            preflight_commitment: Some(CommitmentConfig::processed()),
            max_retries: Some(0),
            ..Default::default()
        };

        rpc_client.send_transaction_with_config(&transaction, config)?;
        Ok(())
    }

    fn create_arbitrage_instruction(
        &self,
        opportunity: &FlashLoanOpportunity,
        payer: &Pubkey,
        token_account: &Pubkey,
    ) -> Result<Instruction, Box<dyn std::error::Error>> {
        let arb_program = Pubkey::from_str("ARB11111111111111111111111111111111111111111")?;
        
        let data = ArbitrageData {
            market_a: opportunity.market_a,
            market_b: opportunity.market_b,
            amount: opportunity.size,
            min_profit: (opportunity.expected_profit * 0.8) as u64,
            max_slippage: MAX_SLIPPAGE_BPS,
        };

        Ok(Instruction {
            program_id: arb_program,
            accounts: vec![
                AccountMeta::new(*payer, true),
                AccountMeta::new(*token_account, false),
                AccountMeta::new(opportunity.market_a, false),
                AccountMeta::new(opportunity.market_b, false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data: data.try_to_vec()?,
        })
    }
}

#[derive(BorshSerialize, BorshDeserialize)]
struct BorrowInstruction {
    amount: u64,
    expected_fee: u64,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct RepayInstruction {
    amount: u64,
    fee: u64,
}

#[derive(BorshSerialize, BorshDeserialize)]
struct ArbitrageData {
    market_a: Pubkey,
    market_b: Pubkey,
    amount: u64,
    min_profit: u64,
    max_slippage: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_levy_parameters() {
        let mut history = VecDeque::new();
        for i in 0..100 {
            history.push_back(MarketSnapshot {
                timestamp: i as u64,
                bid: 100.0 + (i as f64).sin(),
                ask: 100.1 + (i as f64).sin(),
                bid_size: 1000.0,
                ask_size: 1000.0,
                last_trade: 100.05 + (i as f64).sin(),
                volume: 10000.0,
                spread: 0.001,
                imbalance: 0.0,
            });
        }
        let params = LevyProcessPredictor::estimate_levy_parameters(&history);
        assert!(params.alpha > 0.0 && params.alpha <= 2.0);
    }
}

impl LevyProcessPredictor {
    pub async fn initialize_and_run(rpc_url: &str, keypair_path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let keypair_bytes = std::fs::read(keypair_path)?;
        let keypair = Keypair::from_bytes(&keypair_bytes)?;
        
        let predictor = Arc::new(Self::new(rpc_url, keypair));
        
        // Pre-warm caches
        predictor.prewarm_caches().await?;
        
        // Start main execution loop
        predictor.run().await;
        
        Ok(())
    }

    async fn prewarm_caches(&self) -> Result<(), Box<dyn std::error::Error>> {
        let markets = vec![
            "7dLVkUfBVfCGkFhSXDCq1ukM9usathSgS716t643iFGF",
            "8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYwn1XTh6",
            "DZjbn4XC8qoHKikZqzmhemykVzmossoayV9ffbsUqxVj",
            "HWHvQhFmJB3NUcu1aihKmrKegfVxBEHzwVX6yZCKEsi1",
        ];

        for market_str in markets {
            if let Ok(market_pubkey) = market_str.parse::<Pubkey>() {
                if let Ok(account) = self.rpc_client.get_account(&market_pubkey) {
                    if let Ok(market_state) = Market::unpack(&account.data[5..]) {
                        let mut cache = self.market_cache.write().unwrap();
                        cache.insert(market_pubkey, market_state.inner);
                    }
                }
            }
        }

        Ok(())
    }

    pub fn calculate_portfolio_risk(&self) -> f64 {
        let opportunities = self.pending_opportunities.lock().unwrap();
        if opportunities.is_empty() {
            return 0.0;
        }

        let total_exposure: f64 = opportunities.iter()
            .map(|o| o.size as f64 * o.entry_price)
            .sum();

        let risk_scores: Vec<f64> = opportunities.iter()
            .map(|o| {
                let position_risk = (o.size as f64) / MAX_POSITION_SIZE as f64;
                let confidence_risk = 1.0 - o.confidence;
                let jump_risk = o.predicted_jump.abs() / o.entry_price;
                (position_risk + confidence_risk + jump_risk) / 3.0
            })
            .collect();

        let avg_risk = risk_scores.iter().sum::<f64>() / risk_scores.len() as f64;
        let max_risk = risk_scores.iter().fold(0.0, |a, &b| a.max(b));

        (avg_risk * 0.7 + max_risk * 0.3).min(1.0)
    }

    fn validate_opportunity(&self, opportunity: &FlashLoanOpportunity) -> bool {
        // Check basic constraints
        if opportunity.size == 0 || opportunity.size > MAX_POSITION_SIZE {
            return false;
        }

        if opportunity.confidence < CONFIDENCE_THRESHOLD {
            return false;
        }

        if opportunity.expected_profit < (opportunity.size as f64 * MIN_PROFIT_BPS as f64 / 10000.0) {
            return false;
        }

        // Check market conditions
        let markets = self.market_cache.read().unwrap();
        if let (Some(market_a), Some(market_b)) = (markets.get(&opportunity.market_a), markets.get(&opportunity.market_b)) {
            let liquidity_a = market_a.native_coin_bid() + market_a.native_coin_ask();
            let liquidity_b = market_b.native_coin_bid() + market_b.native_coin_ask();
            
            if opportunity.size > liquidity_a.min(liquidity_b) / 10 {
                return false;
            }
        } else {
            return false;
        }

        true
    }

    pub fn get_performance_metrics(&self) -> PerformanceMetrics {
        let executions = self.last_execution.read().unwrap();
        let opportunities = self.pending_opportunities.lock().unwrap();
        let history = self.price_history.read().unwrap();

        let total_markets = history.len();
        let total_snapshots: usize = history.values().map(|v| v.len()).sum();
        let pending_opportunities = opportunities.len();
        let recent_executions = executions.values()
            .filter(|t| t.elapsed().as_secs() < 300)
            .count();

        PerformanceMetrics {
            total_markets,
            total_snapshots,
            pending_opportunities,
            recent_executions,
            portfolio_risk: self.calculate_portfolio_risk(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub total_markets: usize,
    pub total_snapshots: usize,
    pub pending_opportunities: usize,
    pub recent_executions: usize,
    pub portfolio_risk: f64,
}

// Optimized fast random for Lévy process
mod fastrand {
    use std::cell::Cell;
    use std::num::Wrapping;

    thread_local! {
        static RNG: Cell<Wrapping<u64>> = Cell::new(Wrapping(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        ));
    }

    pub fn f64() -> f64 {
        RNG.with(|rng| {
            let mut x = rng.get();
            x ^= x >> 12;
            x ^= x << 25;
            x ^= x >> 27;
            rng.set(x);
            (x.0.wrapping_mul(0x2545F4914F6CDD1D) >> 32) as f64 / (1u64 << 32) as f64
        })
    }
}

// Entry point for production deployment
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let rpc_url = std::env::var("SOLANA_RPC_URL")
        .unwrap_or_else(|_| "https://api.mainnet-beta.solana.com".to_string());
    
    let keypair_path = std::env::var("KEYPAIR_PATH")
        .unwrap_or_else(|_| "./keypair.json".to_string());

    log::info!("Starting Lévy Process Flash Loan Predictor");
    log::info!("RPC URL: {}", rpc_url);

    LevyProcessPredictor::initialize_and_run(&rpc_url, &keypair_path).await?;

    Ok(())
}
