use anyhow::{Context, Result};
use dashmap::DashMap;
use futures::stream::StreamExt;
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
    rpc_filter::{Memcmp, RpcFilterType},
};
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use spl_associated_token_account::get_associated_token_address;
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    sync::{Mutex, RwLock},
    time::{interval, sleep},
};

const RAYDIUM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const OPENBOOK_V3: &str = "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX";
const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";

const COMPUTE_UNIT_LIMIT: u32 = 1_400_000;
const PRIORITY_FEE_LAMPORTS: u64 = 50_000;
const MIN_PROFIT_BPS: u64 = 15;
const MAX_POSITION_SIZE_SOL: f64 = 100.0;
const VOLATILITY_WINDOW: usize = 120;
const CORRELATION_THRESHOLD: f64 = 0.85;
const MAX_SLIPPAGE_BPS: u64 = 50;
const REBALANCE_THRESHOLD_BPS: u64 = 100;

#[derive(Clone, Debug)]
struct TokenPair {
    token_a: Pubkey,
    token_b: Pubkey,
    pool_address: Pubkey,
    program_id: Pubkey,
    last_price: f64,
    volatility: f64,
    volume_24h: f64,
    correlation: f64,
}

#[derive(Clone, Debug)]
struct DispersionOpportunity {
    pair: TokenPair,
    index_volatility: f64,
    pair_volatility: f64,
    dispersion_ratio: f64,
    expected_profit: f64,
    size_sol: f64,
    timestamp: u64,
}

#[derive(Clone)]
struct PriceData {
    price: f64,
    timestamp: u64,
    volume: f64,
}

pub struct DispersionTrader {
    rpc_client: Arc<RpcClient>,
    keypair: Arc<Keypair>,
    token_pairs: Arc<DashMap<Pubkey, TokenPair>>,
    price_history: Arc<DashMap<Pubkey, Vec<PriceData>>>,
    active_positions: Arc<RwLock<HashMap<Pubkey, Position>>>,
    is_running: Arc<AtomicBool>,
    last_rebalance: Arc<AtomicU64>,
}

#[derive(Clone, Debug)]
struct Position {
    pair: TokenPair,
    entry_price: f64,
    size_sol: f64,
    timestamp: u64,
    target_exit: f64,
    stop_loss: f64,
}

impl DispersionTrader {
    pub fn new(rpc_url: &str, keypair: Keypair) -> Result<Self> {
        let rpc_client = RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        );

        Ok(Self {
            rpc_client: Arc::new(rpc_client),
            keypair: Arc::new(keypair),
            token_pairs: Arc::new(DashMap::new()),
            price_history: Arc::new(DashMap::new()),
            active_positions: Arc::new(RwLock::new(HashMap::new())),
            is_running: Arc::new(AtomicBool::new(false)),
            last_rebalance: Arc::new(AtomicU64::new(0)),
        })
    }

    pub async fn start(&self) -> Result<()> {
        self.is_running.store(true, Ordering::Relaxed);
        
        let tasks = vec![
            tokio::spawn(self.clone().monitor_pools()),
            tokio::spawn(self.clone().calculate_volatilities()),
            tokio::spawn(self.clone().find_opportunities()),
            tokio::spawn(self.clone().manage_positions()),
        ];

        futures::future::join_all(tasks).await;
        Ok(())
    }

    async fn monitor_pools(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(Duration::from_millis(100));
        
        while self.is_running.load(Ordering::Relaxed) {
            interval.tick().await;
            
            let programs = vec![
                Pubkey::from_str(RAYDIUM_V4)?,
                Pubkey::from_str(ORCA_WHIRLPOOL)?,
                Pubkey::from_str(OPENBOOK_V3)?,
            ];

            for program in programs {
                if let Err(e) = self.fetch_pool_data(&program).await {
                    log::error!("Pool monitoring error: {}", e);
                }
            }
        }
        Ok(())
    }

    async fn fetch_pool_data(&self, program: &Pubkey) -> Result<()> {
        let config = RpcProgramAccountsConfig {
            filters: Some(vec![RpcFilterType::DataSize(
                if program == &Pubkey::from_str(RAYDIUM_V4)? { 752 } 
                else if program == &Pubkey::from_str(ORCA_WHIRLPOOL)? { 653 }
                else { 3228 }
            )]),
            account_config: RpcAccountInfoConfig {
                encoding: Some(solana_account_decoder::UiAccountEncoding::Base64),
                commitment: Some(self.rpc_client.commitment()),
                ..Default::default()
            },
            ..Default::default()
        };

        let accounts = self.rpc_client
            .get_program_accounts_with_config(program, config)
            .await?;

        for (pubkey, account) in accounts.iter().take(50) {
            if let Ok(pool_data) = self.decode_pool_data(program, &account.data).await {
                self.update_price_data(&pubkey, pool_data).await?;
            }
        }
        Ok(())
    }

    async fn decode_pool_data(&self, program: &Pubkey, data: &[u8]) -> Result<(Pubkey, Pubkey, f64, f64)> {
        if program == &Pubkey::from_str(RAYDIUM_V4)? {
            if data.len() < 752 { return Err(anyhow::anyhow!("Invalid data")); }
            
            let token_a = Pubkey::new(&data[65..97]);
            let token_b = Pubkey::new(&data[97..129]);
            let reserve_a = u64::from_le_bytes(data[129..137].try_into()?);
            let reserve_b = u64::from_le_bytes(data[137..145].try_into()?);
            
            let price = if reserve_a > 0 && reserve_b > 0 {
                reserve_b as f64 / reserve_a as f64
            } else { 0.0 };
            
            let volume = (reserve_a + reserve_b) as f64 * 0.003;
            Ok((token_a, token_b, price, volume))
        } else if program == &Pubkey::from_str(ORCA_WHIRLPOOL)? {
            if data.len() < 653 { return Err(anyhow::anyhow!("Invalid data")); }
            
            let token_a = Pubkey::new(&data[8..40]);
            let token_b = Pubkey::new(&data[40..72]);
            let sqrt_price = u128::from_le_bytes(data[72..88].try_into()?);
            let liquidity = u128::from_le_bytes(data[88..104].try_into()?);
            
            let price = (sqrt_price as f64 / (1u64 << 64) as f64).powi(2);
            let volume = liquidity as f64 * 0.002;
            Ok((token_a, token_b, price, volume))
        } else {
            Err(anyhow::anyhow!("Unsupported program"))
        }
    }

    async fn update_price_data(&self, pool: &Pubkey, data: (Pubkey, Pubkey, f64, f64)) -> Result<()> {
        let (token_a, token_b, price, volume) = data;
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        
        let price_data = PriceData { price, timestamp, volume };
        
        self.price_history
            .entry(*pool)
            .or_insert_with(Vec::new)
            .push(price_data.clone());
        
        if let Some(mut history) = self.price_history.get_mut(pool) {
            if history.len() > VOLATILITY_WINDOW {
                history.drain(0..history.len() - VOLATILITY_WINDOW);
            }
        }
        
        let volatility = self.calculate_volatility(&self.price_history.get(pool).unwrap());
        let correlation = self.calculate_correlation_to_sol(pool).await;
        
        self.token_pairs.insert(
            *pool,
            TokenPair {
                token_a,
                token_b,
                pool_address: *pool,
                program_id: *pool,
                last_price: price,
                volatility,
                volume_24h: volume,
                correlation,
            },
        );
        
        Ok(())
    }

    fn calculate_volatility(&self, history: &[PriceData]) -> f64 {
        if history.len() < 2 { return 0.0; }
        
        let returns: Vec<f64> = history.windows(2)
            .map(|w| (w[1].price / w[0].price).ln())
            .collect();
        
        let mean = returns.iter().sum::<f64>() / returns.len() as f64;
        let variance = returns.iter()
            .map(|r| (r - mean).powi(2))
            .sum::<f64>() / returns.len() as f64;
        
        variance.sqrt() * (365.25 * 24.0 * 60.0 * 60.0_f64).sqrt()
    }

    async fn calculate_correlation_to_sol(&self, pool: &Pubkey) -> f64 {
        let sol_usdc = Pubkey::from_str("58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2")
            .unwrap_or_default();
        
        let pool_history = self.price_history.get(pool);
        let sol_history = self.price_history.get(&sol_usdc);
        
        match (pool_history, sol_history) {
            (Some(ph), Some(sh)) => {
                if ph.len() < 20 || sh.len() < 20 { return 0.0; }
                
                let pool_returns: Vec<f64> = ph.windows(2)
                    .map(|w| (w[1].price / w[0].price).ln())
                    .collect();
                    
                let sol_returns: Vec<f64> = sh.windows(2)
                    .map(|w| (w[1].price / w[0].price).ln())
                    .collect();
                
                let min_len = pool_returns.len().min(sol_returns.len());
                let pr = &pool_returns[..min_len];
                let sr = &sol_returns[..min_len];
                
                let pr_mean = pr.iter().sum::<f64>() / pr.len() as f64;
                let sr_mean = sr.iter().sum::<f64>() / sr.len() as f64;
                
                let covariance: f64 = pr.iter().zip(sr.iter())
                    .map(|(p, s)| (p - pr_mean) * (s - sr_mean))
                    .sum::<f64>() / pr.len() as f64;
                
                let pr_std = (pr.iter().map(|p| (p - pr_mean).powi(2)).sum::<f64>() / pr.len() as f64).sqrt();
                let sr_std = (sr.iter().map(|s| (s - sr_mean).powi(2)).sum::<f64>() / sr.len() as f64).sqrt();
                
                if pr_std > 0.0 && sr_std > 0.0 {
                    covariance / (pr_std * sr_std)
                } else { 0.0 }
            }
            _ => 0.0,
        }
    }

    async fn calculate_volatilities(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(Duration::from_secs(1));
        
        while self.is_running.load(Ordering::Relaxed) {
            interval.tick().await;
            
            for entry in self.token_pairs.iter() {
                let pool = entry.key();
                                if let Some(history) = self.price_history.get(pool) {
                    let volatility = self.calculate_volatility(&history);
                    let correlation = self.calculate_correlation_to_sol(pool).await;
                    
                    if let Some(mut pair) = self.token_pairs.get_mut(pool) {
                        pair.volatility = volatility;
                        pair.correlation = correlation;
                    }
                }
            }
        }
        Ok(())
    }

    async fn find_opportunities(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(Duration::from_millis(250));
        
        while self.is_running.load(Ordering::Relaxed) {
            interval.tick().await;
            
            let sol_volatility = self.get_index_volatility().await;
            let mut opportunities = Vec::new();
            
            for entry in self.token_pairs.iter() {
                let pair = entry.value();
                
                if pair.volume_24h < 10000.0 || pair.correlation < CORRELATION_THRESHOLD {
                    continue;
                }
                
                let dispersion_ratio = if sol_volatility > 0.0 {
                    pair.volatility / sol_volatility
                } else { 0.0 };
                
                if dispersion_ratio > 1.15 && dispersion_ratio < 3.0 {
                    let expected_profit = self.calculate_expected_profit(&pair, sol_volatility, dispersion_ratio);
                    
                    if expected_profit > MIN_PROFIT_BPS as f64 / 10000.0 {
                        let size_sol = self.calculate_position_size(&pair, expected_profit).await;
                        
                        opportunities.push(DispersionOpportunity {
                            pair: pair.clone(),
                            index_volatility: sol_volatility,
                            pair_volatility: pair.volatility,
                            dispersion_ratio,
                            expected_profit,
                            size_sol,
                            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
                        });
                    }
                }
            }
            
            opportunities.sort_by(|a, b| b.expected_profit.partial_cmp(&a.expected_profit).unwrap());
            
            for opp in opportunities.iter().take(3) {
                if let Err(e) = self.execute_dispersion_trade(opp).await {
                    log::error!("Trade execution error: {}", e);
                }
            }
        }
        Ok(())
    }

    async fn get_index_volatility(&self) -> f64 {
        let sol_usdc = Pubkey::from_str("58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2")
            .unwrap_or_default();
            
        if let Some(history) = self.price_history.get(&sol_usdc) {
            self.calculate_volatility(&history)
        } else { 0.2 }
    }

    fn calculate_expected_profit(&self, pair: &TokenPair, index_vol: f64, dispersion_ratio: f64) -> f64 {
        let vol_spread = (pair.volatility - index_vol).abs();
        let mean_reversion_prob = 1.0 / (1.0 + (-4.0 * (dispersion_ratio - 1.5).abs()).exp());
        let base_profit = vol_spread * mean_reversion_prob * 0.1;
        
        let volume_factor = (pair.volume_24h / 100000.0).min(2.0);
        let correlation_penalty = (1.0 - pair.correlation).max(0.0);
        
        base_profit * volume_factor * (1.0 - correlation_penalty * 0.5)
    }

    async fn calculate_position_size(&self, pair: &TokenPair, expected_profit: f64) -> f64 {
        let balance = self.get_sol_balance().await.unwrap_or(0.0);
        let positions = self.active_positions.read().await;
        let total_exposure: f64 = positions.values().map(|p| p.size_sol).sum();
        
        let available_capital = (balance * 0.8 - total_exposure).max(0.0);
        let kelly_fraction = expected_profit / pair.volatility.powi(2);
        let position_size = available_capital * kelly_fraction.min(0.25);
        
        position_size.min(MAX_POSITION_SIZE_SOL)
    }

    async fn execute_dispersion_trade(&self, opp: &DispersionOpportunity) -> Result<()> {
        let positions = self.active_positions.read().await;
        if positions.contains_key(&opp.pair.pool_address) {
            return Ok(());
        }
        drop(positions);
        
        let (token_a_balance, token_b_balance) = self.get_token_balances(&opp.pair).await?;
        let required_sol = opp.size_sol;
        
        if self.get_sol_balance().await? < required_sol * 1.1 {
            return Ok(());
        }
        
        let instructions = self.build_dispersion_instructions(&opp.pair, required_sol).await?;
        let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;
        
        let mut transaction = Transaction::new_with_payer(&instructions, Some(&self.keypair.pubkey()));
        transaction.sign(&[self.keypair.as_ref()], recent_blockhash);
        
        match self.rpc_client.send_and_confirm_transaction(&transaction).await {
            Ok(sig) => {
                log::info!("Dispersion trade executed: {}", sig);
                
                let position = Position {
                    pair: opp.pair.clone(),
                    entry_price: opp.pair.last_price,
                    size_sol: opp.size_sol,
                    timestamp: opp.timestamp,
                    target_exit: opp.pair.last_price * (1.0 + opp.expected_profit),
                    stop_loss: opp.pair.last_price * 0.97,
                };
                
                self.active_positions.write().await.insert(opp.pair.pool_address, position);
            }
            Err(e) => {
                log::error!("Transaction failed: {}", e);
            }
        }
        
        Ok(())
    }

    async fn build_dispersion_instructions(&self, pair: &TokenPair, sol_amount: f64) -> Result<Vec<Instruction>> {
        let mut instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNIT_LIMIT),
            ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS),
        ];
        
        let token_a_account = get_associated_token_address(&self.keypair.pubkey(), &pair.token_a);
        let token_b_account = get_associated_token_address(&self.keypair.pubkey(), &pair.token_b);
        
        let amount_in = (sol_amount * 1e9) as u64;
        let min_amount_out = self.calculate_min_amount_out(pair, amount_in).await?;
        
        if pair.program_id == Pubkey::from_str(RAYDIUM_V4)? {
            instructions.push(self.build_raydium_swap(pair, amount_in, min_amount_out).await?);
        } else if pair.program_id == Pubkey::from_str(ORCA_WHIRLPOOL)? {
            instructions.push(self.build_orca_swap(pair, amount_in, min_amount_out).await?);
        }
        
        Ok(instructions)
    }

    async fn build_raydium_swap(&self, pair: &TokenPair, amount_in: u64, min_out: u64) -> Result<Instruction> {
        let pool_data = self.rpc_client.get_account(&pair.pool_address).await?;
        let discriminator = &pool_data.data[0..8];
        
        let mut data = vec![];
        data.extend_from_slice(&[9]); // Swap instruction
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_out.to_le_bytes());
        
        Ok(Instruction {
            program_id: Pubkey::from_str(RAYDIUM_V4)?,
            accounts: vec![
                AccountMeta::new(pair.pool_address, false),
                AccountMeta::new_readonly(Pubkey::from_str("5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1")?, false),
                AccountMeta::new_readonly(self.keypair.pubkey(), true),
                AccountMeta::new(get_associated_token_address(&self.keypair.pubkey(), &pair.token_a), false),
                AccountMeta::new(get_associated_token_address(&self.keypair.pubkey(), &pair.token_b), false),
                AccountMeta::new(pair.token_a, false),
                AccountMeta::new(pair.token_b, false),
                AccountMeta::new_readonly(spl_token::id(), false),
            ],
            data,
        })
    }

    async fn build_orca_swap(&self, pair: &TokenPair, amount_in: u64, min_out: u64) -> Result<Instruction> {
        let sqrt_price_limit = 0u128;
        let amount_specified = amount_in as i64;
        let other_amount_threshold = min_out;
        let exact_input = true;
        
        let mut data = vec![0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0x48]; // Swap discriminator
        data.extend_from_slice(&amount_specified.to_le_bytes());
        data.extend_from_slice(&other_amount_threshold.to_le_bytes());
        data.extend_from_slice(&sqrt_price_limit.to_le_bytes());
        data.push(exact_input as u8);
        
        Ok(Instruction {
            program_id: Pubkey::from_str(ORCA_WHIRLPOOL)?,
            accounts: vec![
                AccountMeta::new_readonly(spl_token::id(), false),
                AccountMeta::new_readonly(self.keypair.pubkey(), true),
                AccountMeta::new(pair.pool_address, false),
                AccountMeta::new(get_associated_token_address(&self.keypair.pubkey(), &pair.token_a), false),
                AccountMeta::new(get_associated_token_address(&self.keypair.pubkey(), &pair.token_b), false),
                AccountMeta::new(pair.token_a, false),
                AccountMeta::new(pair.token_b, false),
            ],
            data,
        })
    }

    async fn calculate_min_amount_out(&self, pair: &TokenPair, amount_in: u64) -> Result<u64> {
        let expected_out = (amount_in as f64 * pair.last_price) as u64;
        let slippage = expected_out * MAX_SLIPPAGE_BPS / 10000;
        Ok(expected_out.saturating_sub(slippage))
    }

    async fn manage_positions(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(Duration::from_millis(500));
        
        while self.is_running.load(Ordering::Relaxed) {
            interval.tick().await;
            
            let positions = self.active_positions.read().await.clone();
            let current_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            
            for (pool, position) in positions {
                if let Some(pair) = self.token_pairs.get(&pool) {
                    let current_price = pair.last_price;
                    let pnl_pct = (current_price / position.entry_price - 1.0) * 100.0;
                    
                    let should_exit = current_price >= position.target_exit ||
                                     current_price <= position.stop_loss ||
                                     (current_time - position.timestamp) > 3600 ||
                                     self.check_rebalance_needed(&position, &pair).await;
                    
                    if should_exit {
                        if let Err(e) = self.close_position(&pool, &position).await {
                            log::error!("Failed to close position: {}", e);
                        }
                    }
                }
            }
            
            let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
            if now - self.last_rebalance.load(Ordering::Relaxed) > 300 {
                self.rebalance_portfolio().await?;
                self.last_rebalance.store(now, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    async fn check_rebalance_needed(&self, position: &Position, current_pair: &TokenPair) -> bool {
        let price_change = ((current_pair.last_price / position.entry_price - 1.0) * 10000.0).abs() as u64;
        let vol_change = ((current_pair.volatility / position.pair.volatility - 1.0) * 10000.0).abs() as u64;
        
        price_change > REBALANCE_THRESHOLD_BPS || vol_change > REBALANCE_THRESHOLD_BPS * 2
    }

    async fn close_position(&self, pool: &Pubkey, position: &Position) -> Result<()> {
        let instructions = self.build_close_instructions(position).await?;
        let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;
        
        let mut transaction = Transaction::new_with_payer(&instructions, Some(&self.keypair.pubkey()));
        transaction.sign(&[self.keypair.as_ref()], recent_blockhash);
        
        match self.rpc_client.send_and_confirm_transaction(&transaction).await {
            Ok(sig) => {
                log::info!("Position closed: {}", sig);
                self.active_positions.write().await.remove(pool);
            }
                        Err(e) => {
                log::error!("Failed to close position: {}", e);
                return Err(anyhow::anyhow!("Transaction failed: {}", e));
            }
        }
        Ok(())
    }

    async fn build_close_instructions(&self, position: &Position) -> Result<Vec<Instruction>> {
        let mut instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNIT_LIMIT),
            ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS * 2),
        ];
        
        let token_a_balance = self.get_token_balance(&position.pair.token_a).await?;
        let token_b_balance = self.get_token_balance(&position.pair.token_b).await?;
        
        let (amount_in, min_out) = if token_a_balance > 0 {
            let out = (token_a_balance as f64 * position.pair.last_price * 0.995) as u64;
            (token_a_balance, out)
        } else {
            let out = (token_b_balance as f64 / position.pair.last_price * 0.995) as u64;
            (token_b_balance, out)
        };
        
        if position.pair.program_id == Pubkey::from_str(RAYDIUM_V4)? {
            instructions.push(self.build_raydium_swap(&position.pair, amount_in, min_out).await?);
        } else if position.pair.program_id == Pubkey::from_str(ORCA_WHIRLPOOL)? {
            instructions.push(self.build_orca_swap(&position.pair, amount_in, min_out).await?);
        }
        
        Ok(instructions)
    }

    async fn rebalance_portfolio(&self) -> Result<()> {
        let positions = self.active_positions.read().await;
        let mut total_value = 0.0;
        let mut position_values: Vec<(Pubkey, f64)> = Vec::new();
        
        for (pool, position) in positions.iter() {
            if let Some(pair) = self.token_pairs.get(pool) {
                let value = position.size_sol * (pair.last_price / position.entry_price);
                total_value += value;
                position_values.push((*pool, value));
            }
        }
        drop(positions);
        
        if total_value == 0.0 { return Ok(()); }
        
        let target_allocation = 1.0 / position_values.len().max(1) as f64;
        
        for (pool, value) in position_values {
            let current_allocation = value / total_value;
            let deviation = (current_allocation - target_allocation).abs();
            
            if deviation > 0.1 {
                let adjustment = (target_allocation - current_allocation) * total_value;
                
                if adjustment > 0.0 {
                    if let Some(pair) = self.token_pairs.get(&pool) {
                        let opp = DispersionOpportunity {
                            pair: pair.clone(),
                            index_volatility: self.get_index_volatility().await,
                            pair_volatility: pair.volatility,
                            dispersion_ratio: pair.volatility / self.get_index_volatility().await,
                            expected_profit: 0.002,
                            size_sol: adjustment.abs(),
                            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
                        };
                        let _ = self.execute_dispersion_trade(&opp).await;
                    }
                } else {
                    if let Some(position) = self.active_positions.read().await.get(&pool) {
                        let _ = self.close_position(&pool, position).await;
                    }
                }
            }
        }
        
        Ok(())
    }

    async fn get_sol_balance(&self) -> Result<f64> {
        let balance = self.rpc_client.get_balance(&self.keypair.pubkey()).await?;
        Ok(balance as f64 / 1e9)
    }

    async fn get_token_balance(&self, mint: &Pubkey) -> Result<u64> {
        let token_account = get_associated_token_address(&self.keypair.pubkey(), mint);
        
        match self.rpc_client.get_token_account_balance(&token_account).await {
            Ok(balance) => Ok(balance.amount.parse::<u64>().unwrap_or(0)),
            Err(_) => Ok(0),
        }
    }

    async fn get_token_balances(&self, pair: &TokenPair) -> Result<(u64, u64)> {
        let balance_a = self.get_token_balance(&pair.token_a).await?;
        let balance_b = self.get_token_balance(&pair.token_b).await?;
        Ok((balance_a, balance_b))
    }

    pub async fn stop(&self) {
        self.is_running.store(false, Ordering::Relaxed);
        
        let positions = self.active_positions.read().await.clone();
        for (pool, position) in positions {
            if let Err(e) = self.close_position(&pool, &position).await {
                log::error!("Failed to close position on shutdown: {}", e);
            }
        }
    }

    pub async fn get_performance_metrics(&self) -> Result<PerformanceMetrics> {
        let positions = self.active_positions.read().await;
        let mut total_pnl = 0.0;
        let mut win_count = 0;
        let mut loss_count = 0;
        
        for (pool, position) in positions.iter() {
            if let Some(pair) = self.token_pairs.get(pool) {
                let pnl = (pair.last_price / position.entry_price - 1.0) * position.size_sol;
                total_pnl += pnl;
                
                if pnl > 0.0 {
                    win_count += 1;
                } else {
                    loss_count += 1;
                }
            }
        }
        
        Ok(PerformanceMetrics {
            total_pnl,
            win_rate: win_count as f64 / (win_count + loss_count).max(1) as f64,
            active_positions: positions.len(),
            total_volume: positions.values().map(|p| p.size_sol).sum(),
        })
    }
}

#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub total_pnl: f64,
    pub win_rate: f64,
    pub active_positions: usize,
    pub total_volume: f64,
}

impl Clone for DispersionTrader {
    fn clone(&self) -> Self {
        Self {
            rpc_client: Arc::clone(&self.rpc_client),
            keypair: Arc::clone(&self.keypair),
            token_pairs: Arc::clone(&self.token_pairs),
            price_history: Arc::clone(&self.price_history),
            active_positions: Arc::clone(&self.active_positions),
            is_running: Arc::clone(&self.is_running),
            last_rebalance: Arc::clone(&self.last_rebalance),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;

    #[tokio::test]
    async fn test_volatility_calculation() {
        let trader = DispersionTrader::new("https://api.mainnet-beta.solana.com", Keypair::new()).unwrap();
        
        let price_data = vec![
            PriceData { price: 100.0, timestamp: 1000, volume: 1000.0 },
            PriceData { price: 102.0, timestamp: 1001, volume: 1100.0 },
            PriceData { price: 101.0, timestamp: 1002, volume: 1050.0 },
            PriceData { price: 103.0, timestamp: 1003, volume: 1200.0 },
        ];
        
        let volatility = trader.calculate_volatility(&price_data);
        assert!(volatility > 0.0);
    }

    #[tokio::test]
    async fn test_position_sizing() {
        let trader = DispersionTrader::new("https://api.mainnet-beta.solana.com", Keypair::new()).unwrap();
        
        let pair = TokenPair {
            token_a: Pubkey::new_unique(),
            token_b: Pubkey::new_unique(),
            pool_address: Pubkey::new_unique(),
            program_id: Pubkey::from_str(RAYDIUM_V4).unwrap(),
            last_price: 1.0,
            volatility: 0.5,
            volume_24h: 50000.0,
            correlation: 0.9,
        };
        
        let size = trader.calculate_position_size(&pair, 0.01).await;
        assert!(size >= 0.0 && size <= MAX_POSITION_SIZE_SOL);
    }
}

