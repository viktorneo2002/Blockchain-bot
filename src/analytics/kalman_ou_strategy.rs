use anchor_lang::prelude::*;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    system_instruction,
    transaction::Transaction,
};
use std::str::FromStr;
use std::{
    sync::Arc,
    time::Duration,
};
use tokio::{
    sync::{Mutex, RwLock},
    time::interval,
};
use anyhow::Result;
use crate::analytics::{KalmanFilterSpreadPredictor, OrnsteinUhlenbeckStrategy, Signal};
use crate::dex_connectors::{orca_connector::OrcaConnector, raydium_connector::RaydiumConnector};
use crate::config::dex_config::DexPoolsConfig;
use crate::main::Strategy;
use crate::flashloan_mathematical_supremacy::strategy_integrator::StrategyIntegrator;

// Constants for transaction building
const ORCA_WHIRLPOOL_PROGRAM: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const RAYDIUM_V4_PROGRAM: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const SERUM_PROGRAM_ID: &str = "srmqPvymJeFKQ4zGQed1GFppgkRHL9kaELCbyksJtPX";
const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const ASSOCIATED_TOKEN_PROGRAM_ID: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";

const COMPUTE_UNITS: u32 = 1_400_000;
const PRIORITY_FEE_MICROLAMPORTS: u64 = 25_000;
const MAX_SLIPPAGE_BPS: u64 = 30;
const JITO_TIP_LAMPORTS: u64 = 10_000;
const MAX_RETRIES: u8 = 2;

const TICK_SPACING_ORCA: i32 = 64;

/// Strategy implementation using Kalman Filter and Ornstein-Uhlenbeck models
/// for spread prediction and trading signal generation
pub struct KalmanOUStrategy {
    /// Kalman filter for spread prediction
    predictor: Arc<KalmanFilterSpreadPredictor>,
    /// Ornstein-Uhlenbeck strategy for signal generation
    strategy: Arc<OrnsteinUhlenbeckStrategy>,
    /// Orca connector for price data
    orca_connector: Arc<OrcaConnector>,
    /// Raydium connector for price data
    raydium_connector: Arc<RaydiumConnector>,
    /// Pool configurations
    pool_config: DexPoolsConfig,
    /// RPC client for on-chain data
    rpc_client: Arc<RpcClient>,
    /// Wallet for signing transactions
    wallet: Arc<Keypair>,
    /// Shutdown signal
    shutdown: Arc<Mutex<bool>>,
    /// Strategy integrator
    strategy_integrator: Arc<StrategyIntegrator>,
}

impl KalmanOUStrategy {
    /// Create a new Kalman-O-U strategy
    pub fn new(
        rpc_client: Arc<RpcClient>,
        wallet: Arc<Keypair>,
    ) -> Result<Self> {
        let predictor = Arc::new(KalmanFilterSpreadPredictor::new());
        let strategy = Arc::new(OrnsteinUhlenbeckStrategy::new());
        let strategy_integrator = Arc::new(StrategyIntegrator::new(predictor.clone(), strategy.clone()));
        
        // Initialize DEX connectors
        let rpc_url = rpc_client.url();
        let orca_connector = Arc::new(OrcaConnector::new(&rpc_url));
        let raydium_connector = Arc::new(RaydiumConnector::new(&rpc_url));
        
        // Load pool configurations
        let config_path = std::env::var("CONFIG_PATH").unwrap_or_else(|_| "config/dex_pools.json".to_string());
        let pool_config = DexPoolsConfig::load_from_file(&config_path)?;
        
        Ok(Self {
            predictor,
            strategy,
            orca_connector,
            raydium_connector,
            pool_config,
            rpc_client,
            wallet,
            shutdown: Arc::new(Mutex::new(false)),
            strategy_integrator,
        })
    }

    /// Update price data with a new observation
    async fn update_prices(&self, observation: crate::analytics::SpreadObservation) -> Result<()> {
        // Use actual pool addresses as the pair identifier
        // For this example, we'll use the first SOL/USDC pool from each DEX
        let sol_usdc_orca = self.pool_config.get_orca_pool("SOL/USDC").ok_or_else(|| anyhow::anyhow!("SOL/USDC Orca pool not found in config"))?;
        let sol_usdc_raydium = self.pool_config.get_raydium_pool("SOL/USDC").ok_or_else(|| anyhow::anyhow!("SOL/USDC Raydium pool not found in config"))?;
        
        let pair = (sol_usdc_orca.get_pubkey()?, sol_usdc_raydium.get_pubkey()?);
        self.predictor.update(pair, observation)?;
        Ok(())
    }

    /// Generate trading signal based on current spread prediction
    async fn generate_signal(&self) -> Result<Signal> {
        // Use actual pool addresses as the pair identifier
        let sol_usdc_orca = self.pool_config.get_orca_pool("SOL/USDC").ok_or_else(|| anyhow::anyhow!("SOL/USDC Orca pool not found in config"))?;
        let sol_usdc_raydium = self.pool_config.get_raydium_pool("SOL/USDC").ok_or_else(|| anyhow::anyhow!("SOL/USDC Raydium pool not found in config"))?;
        
        let pair = (sol_usdc_orca.get_pubkey()?, sol_usdc_raydium.get_pubkey()?);
        
        // Try to get a prediction
        match self.predictor.predict_spread(pair, 1000) { // 1 second horizon
            Ok(prediction) => {
                // Calculate actual price, volume, and spread from observations
                let price_a = prediction.predicted_spread;
                let price_b = prediction.volatility_estimate;
                let mid_price = (price_a + price_b) / 2.0;
                
                // Calculate volume from liquidity and trend strength
                let volume = prediction.trend_strength * prediction.volatility_estimate * 1000.0;
                
                // Calculate spread in basis points
                let spread_bps = ((price_a - price_b).abs() / mid_price * 10000.0) as u16;
                
                // Update the OU strategy with the actual data
                self.strategy.update_price(mid_price, volume, spread_bps)?;
                
                // Get the signal
                self.strategy.get_signal().map_err(|e| anyhow::anyhow!(e))
            },
            Err(e) => {
                tracing::warn!("Failed to predict spread: {:?}", e);
                Ok(Signal::Hold)
            }
        }
    }

    /// Execute trade based on signal
    async fn execute_trade(&self, signal: Signal) -> Result<()> {
        match signal {
            Signal::Buy { confidence, size_ratio } => {
                tracing::info!("Executing BUY signal: confidence={:.4}, size_ratio={:.4}", confidence, size_ratio);
                
                // Calculate position size based on Kelly criterion and risk management
                let position_size = self.calculate_position_size(confidence, size_ratio).await?;
                
                // Build and execute arbitrage transactions
                self.execute_arbitrage(position_size, true).await?;
            },
            Signal::Sell { confidence, size_ratio } => {
                tracing::info!("Executing SELL signal: confidence={:.4}, size_ratio={:.4}", confidence, size_ratio);
                
                // Calculate position size based on Kelly criterion and risk management
                let position_size = self.calculate_position_size(confidence, size_ratio).await?;
                
                // Build and execute arbitrage transactions
                self.execute_arbitrage(position_size, false).await?;
            },
            Signal::Hold => {
                tracing::info!("Holding position");
            }
        }
        
        Ok(())
    }

    /// Calculate optimal position size based on confidence and risk management
    async fn calculate_position_size(&self, confidence: f64, size_ratio: f64) -> Result<f64> {
        // Get current account balance
        let balance = self.rpc_client.get_balance(&self.wallet.pubkey()).await?;
        
        // Apply Kelly criterion with risk management
        let kelly_fraction = confidence * 0.5; // Conservative Kelly fraction
        let max_position = (balance as f64) * 0.01; // Max 1% of portfolio per trade
        
        // Calculate position size based on confidence and size ratio from OU strategy
        let base_position = max_position * size_ratio;
        let kelly_position = base_position * kelly_fraction;
        
        // Apply additional risk management
        let final_position = kelly_position.min(max_position);
        
        Ok(final_position)
    }
    
    /// Fetch data with retry logic
    async fn fetch_with_retry<F, Fut, T>(&self, mut fetch_fn: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut attempts = 0;
        loop {
            match fetch_fn().await {
                Ok(result) => return Ok(result),
                Err(e) if attempts < MAX_RETRIES - 1 => {
                    attempts += 1;
                    tracing::warn!("Fetch attempt {} failed: {:?}. Retrying...", attempts, e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(100 * attempts as u64)).await;
                }
                Err(e) => {
                    tracing::error!("Fetch failed after {} attempts: {:?}", MAX_RETRIES, e);
                    return Err(e);
                }
            }
        }
    }
    
    /// Combine observations from multiple DEXes into a unified view
    fn combine_observations(&self, orca_obs: crate::analytics::SpreadObservation, raydium_obs: crate::analytics::SpreadObservation) -> Result<crate::analytics::SpreadObservation> {
        // Calculate weighted average based on liquidity
        let total_liquidity = orca_obs.liquidity_a + raydium_obs.liquidity_b;
        
        if total_liquidity == 0.0 {
            return Err(anyhow::anyhow!("No liquidity available for combining observations"));
        }
        
        let orca_weight = orca_obs.liquidity_a / total_liquidity;
        let raydium_weight = raydium_obs.liquidity_b / total_liquidity;
        
        let combined_price_a = orca_obs.price_a * orca_weight + raydium_obs.price_a * raydium_weight;
        let combined_price_b = orca_obs.price_b * orca_weight + raydium_obs.price_b * raydium_weight;
        let combined_liquidity_a = orca_obs.liquidity_a + raydium_obs.liquidity_a;
        let combined_liquidity_b = orca_obs.liquidity_b + raydium_obs.liquidity_b;
        
        Ok(crate::analytics::SpreadObservation {
            timestamp: std::time::SystemTime::now(),
            price_a: combined_price_a,
            price_b: combined_price_b,
            liquidity_a: combined_liquidity_a,
            liquidity_b: combined_liquidity_b,
        })
    }

    /// Execute arbitrage transactions between DEXes
    async fn execute_arbitrage(&self, position_size: f64, is_buy: bool) -> Result<()> {
        tracing::info!(
            "Executing arbitrage: position_size={:.2}, direction={} (buy={})",
            position_size,
            if is_buy { "BUY" } else { "SELL" },
            is_buy
        );
        
        // Get the latest pool states
        let sol_usdc_orca = self.pool_config.get_orca_pool("SOL/USDC").ok_or_else(|| anyhow::anyhow!("SOL/USDC Orca pool not found in config"))?;
        let sol_usdc_raydium = self.pool_config.get_raydium_pool("SOL/USDC").ok_or_else(|| anyhow::anyhow!("SOL/USDC Raydium pool not found in config"))?;
        
        let orca_pool_pubkey = sol_usdc_orca.get_pubkey()?;
        let raydium_pool_pubkey = sol_usdc_raydium.get_pubkey()?;
        
        let orca_pool = self.orca_connector.get_pool_state(&orca_pool_pubkey).await?;
        let raydium_pool = self.raydium_connector.get_amm_info(&raydium_pool_pubkey).await?;
        
        // Calculate swap amounts based on position size
        let amount_in = (position_size * 1_000_000.0) as u64; // Assuming 6 decimals for now
        let min_amount_out = amount_in * (10000 - MAX_SLIPPAGE_BPS) / 10000;
        
        // Build swap instructions
        let mut instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNITS),
            ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_MICROLAMPORTS),
        ];
        
        // First leg of arbitrage - Orca swap
        let (orca_accounts, orca_data) = self.build_orca_swap(&orca_pool, amount_in, min_amount_out, is_buy)?;
        instructions.push(Instruction {
            program_id: Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM)?,
            accounts: orca_accounts,
            data: orca_data,
        });
        
        // Second leg of arbitrage - Raydium swap
        let (raydium_accounts, raydium_data) = self.build_raydium_swap(&raydium_pool, amount_in, min_amount_out, !is_buy)?;
        instructions.push(Instruction {
            program_id: Pubkey::from_str(RAYDIUM_V4_PROGRAM)?,
            accounts: raydium_accounts,
            data: raydium_data,
        });
        
        // Add Jito tip for faster transaction processing
        let jito_tips = [
            "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
            "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe",
            "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
            "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49",
            "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
            "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt",
            "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL",
            "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT",
        ];
        
        let tip_index = (std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_nanos() % jito_tips.len() as u128) as usize;
        let tip_account = Pubkey::from_str(jito_tips[tip_index])?;
        
        instructions.push(system_instruction::transfer(
            &self.wallet.pubkey(),
            &tip_account,
            JITO_TIP_LAMPORTS,
        ));
        
        // Get recent blockhash and build transaction
        let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;
        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.wallet.pubkey()),
            &[&self.wallet],
            recent_blockhash,
        );
        
        // Send transaction with retries
        for retry in 0..MAX_RETRIES {
            match self.rpc_client.send_and_confirm_transaction(&transaction).await {
                Ok(signature) => {
                    tracing::info!("Arbitrage transaction confirmed: {}", signature);
                    return Ok(());
                },
                Err(e) if retry < MAX_RETRIES - 1 => {
                    tracing::warn!("Transaction failed (attempt {}): {}. Retrying...", retry + 1, e);
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    continue;
                },
                Err(e) => {
                    tracing::error!("Arbitrage transaction failed after {} retries: {}", MAX_RETRIES, e);
                    return Err(e.into());
                },
            }
        }
        
        Ok(())
    }

    /// Main execution loop
    async fn execute_loop(self: Arc<Self>) -> Result<()> {
        let mut interval = interval(Duration::from_millis(100)); // 10 Hz update rate
        
        while !*self.shutdown.lock().await {
            interval.tick().await;
            
            // Fetch real market data from DEX connectors using configured pool addresses
            let sol_usdc_orca = self.pool_config.get_orca_pool("SOL/USDC").ok_or_else(|| anyhow::anyhow!("SOL/USDC Orca pool not found in config"))?;
            let sol_usdc_raydium = self.pool_config.get_raydium_pool("SOL/USDC").ok_or_else(|| anyhow::anyhow!("SOL/USDC Raydium pool not found in config"))?;
            
            let orca_pool_address = sol_usdc_orca.get_pubkey()?;
            let raydium_pool_address = sol_usdc_raydium.get_pubkey()?;
            
            // Fetch price observations from both DEXes with proper error handling and retries
            let orca_observation = self.fetch_with_retry(|| self.orca_connector.get_whirlpool_price(&orca_pool_address)).await?;
            let raydium_observation = self.fetch_with_retry(|| self.raydium_connector.get_amm_price(&raydium_pool_address)).await?;
            
            // Combine observations to create a unified market view
            let combined_observation = self.combine_observations(orca_observation, raydium_observation)?;
            
            // Update our models with the new combined data
            self.update_prices(combined_observation).await?;
            
            // Generate trading signals based on updated models
            let signal = self.generate_signal().await?;
            
            // Execute trades based on generated signals
            self.execute_trade(signal).await?;
            
            // Skip the old implementation that only used Orca observation
            continue;
            
            self.update_prices(observation).await?;
            
            let signal = self.generate_signal().await?;
            if !matches!(signal, Signal::Hold) {
                self.execute_trade(signal).await?;
            }
        }
        
        Ok(())
    }
}

#[async_trait::async_trait]
impl Strategy for KalmanOUStrategy {
    fn name(&self) -> &str {
        "Kalman Filter Ornstein-Uhlenbeck Strategy"
    }

    async fn run(&self) -> Result<()> {
        let strategy = Arc::new(self.clone());
        let handle = tokio::spawn(async move {
            if let Err(e) = strategy.execute_loop().await {
                tracing::error!("Strategy execution failed: {:?}", e);
            }
        });
        
        // Wait for the task to complete (it won't unless shutdown is called)
        handle.await?;
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        let mut shutdown = self.shutdown.lock().await;
        *shutdown = true;
        Ok(())
    }
    
    /// Get associated token address for a wallet and mint
    fn get_associated_token_address(wallet: &Pubkey, mint: &Pubkey) -> Result<Pubkey> {
        let token_program = Pubkey::from_str(TOKEN_PROGRAM_ID)
            .map_err(|e| anyhow::anyhow!("Failed to parse token program ID: {}", e))?;
        let associated_token_program = Pubkey::from_str(ASSOCIATED_TOKEN_PROGRAM_ID)
            .map_err(|e| anyhow::anyhow!("Failed to parse associated token program ID: {}", e))?;
        
        let (ata, _) = Pubkey::find_program_address(
            &[
                wallet.as_ref(),
                token_program.as_ref(),
                mint.as_ref(),
            ],
            &associated_token_program,
        );
        Ok(ata)
    }
    
    /// Build Orca Whirlpool swap accounts
    fn build_orca_swap_accounts(&self, pool: &Pubkey, user: &Pubkey, is_a_to_b: bool, token_a: &Pubkey, token_b: &Pubkey, pool_state: &crate::dex_connectors::orca_connector::WhirlpoolState) -> Result<Vec<AccountMeta>> {
        let token_program = Pubkey::from_str(TOKEN_PROGRAM_ID)
            .map_err(|e| anyhow::anyhow!("Failed to parse token program ID: {}", e))?;
        let user_ata_a = Self::get_associated_token_address(user, token_a)?;
        let user_ata_b = Self::get_associated_token_address(user, token_b)?;
        
        let (source_ata, dest_ata) = if is_a_to_b {
            (user_ata_a, user_ata_b)
        } else {
            (user_ata_b, user_ata_a)
        };

        // Derive the correct tick arrays based on the current tick and direction
        let tick_array_indices = self.calculate_tick_array_indices(
            pool_state.tick_current_index, 
            pool_state.tick_spacing, 
            is_a_to_b
        );
        
        let tick_arrays: Vec<Pubkey> = tick_array_indices
            .iter()
            .map(|&index| Self::derive_tick_array(pool, index))
            .collect::<Result<Vec<_>, _>>()??;
        
        // Ensure we have at least 3 tick arrays (pad with null if needed)
        let tick_array_0 = tick_arrays.get(0).cloned().unwrap_or_default();
        let tick_array_1 = tick_arrays.get(1).cloned().unwrap_or_default();
        let tick_array_2 = tick_arrays.get(2).cloned().unwrap_or_default();

        let oracle = Self::derive_oracle(pool)?;

        vec![
            AccountMeta::new_readonly(token_program, false),
            AccountMeta::new(*user, true),
            AccountMeta::new(*pool, false),
            AccountMeta::new(source_ata, false),
            AccountMeta::new_readonly(token_a, false),
            AccountMeta::new_readonly(token_b, false),
            AccountMeta::new(dest_ata, false),
            AccountMeta::new(tick_array_0, false),
            AccountMeta::new(tick_array_1, false),
            AccountMeta::new(tick_array_2, false),
            AccountMeta::new_readonly(oracle, false),
        ]
    }
    
    /// Build Raydium AMM swap accounts
    fn build_raydium_swap_accounts(&self, pool: &Pubkey, user: &Pubkey, is_a_to_b: bool, coin_mint: &Pubkey, pc_mint: &Pubkey, market: &Pubkey) -> Result<Vec<AccountMeta>> {
        let (open_orders, market_authority) = self.derive_raydium_pdas(pool, market)?;?;
        
        let source_mint = if is_a_to_b { coin_mint } else { pc_mint };
        let dest_mint = if is_a_to_b { pc_mint } else { coin_mint };
        
        let source_ata = self.get_associated_token_address(user, source_mint)?;
        let dest_ata = self.get_associated_token_address(user, dest_mint)?;
        
        Ok(vec![
            AccountMeta::new_readonly(*pool, false),
            AccountMeta::new_readonly(market_authority, false),
            AccountMeta::new(open_orders, false),
            AccountMeta::new(*user, true),
            AccountMeta::new(source_ata, false),
            AccountMeta::new(dest_ata, false),
            AccountMeta::new(*coin_mint, false),
            AccountMeta::new(*pc_mint, false),
            AccountMeta::new_readonly(Pubkey::from_str(TOKEN_PROGRAM_ID).unwrap(), false),
        ]
    }
    
    /// Build Orca Whirlpool swap instruction data
    fn build_orca_swap_data(&self, amount_in: u64, min_amount_out: u64, is_a_to_b: bool, current_sqrt_price: u128, slippage_bps: u16) -> Vec<u8> {
        let mut data = vec![0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0x48]; // Swap instruction discriminator
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_amount_out.to_le_bytes());
        
        // Calculate sqrt_price_limit based on current price and slippage tolerance
        let sqrt_price_limit = self.calculate_sqrt_price_limit(current_sqrt_price, slippage_bps, is_a_to_b);
        data.extend_from_slice(&sqrt_price_limit.to_le_bytes());
        
        data.push(is_a_to_b as u8);
        data.push(1); // Remaining accounts are tick arrays
        data
    }
    
    /// Calculate sqrt_price_limit based on current price and slippage tolerance
    fn calculate_sqrt_price_limit(&self, current_sqrt_price: u128, slippage_bps: u16, is_a_to_b: bool) -> u128 {
        // Convert sqrt_price from Q64.64 format
        let current_sqrt_price_f64 = current_sqrt_price as f64 / (2f64.powi(64));
        
        // Calculate price limit based on slippage
        let slippage_factor = slippage_bps as f64 / 10000.0;
        let price_limit = if is_a_to_b {
            // For A to B, price should not go below (1 - slippage) * current_price
            current_sqrt_price_f64 * current_sqrt_price_f64 * (1.0 - slippage_factor)
        } else {
            // For B to A, price should not go above (1 + slippage) * current_price
            current_sqrt_price_f64 * current_sqrt_price_f64 * (1.0 + slippage_factor)
        };
        
        // Convert back to sqrt_price Q64.64 format
        let sqrt_price_limit_f64 = price_limit.sqrt();
        (sqrt_price_limit_f64 * 2f64.powi(64)) as u128
    }
    
    /// Build Raydium AMM swap instruction data
    fn build_raydium_swap_data(&self, amount_in: u64, min_amount_out: u64) -> Vec<u8> {
        let mut data = vec![0x09]; // Swap instruction discriminator
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_amount_out.to_le_bytes());
        data
    }
    
    /// Build Orca swap instruction
    fn build_orca_swap(&self, pool_state: &crate::dex_connectors::orca_connector::WhirlpoolState, amount_in: u64, min_amount_out: u64, is_a_to_b: bool) -> Result<(Vec<AccountMeta>, Vec<u8>)> {
        let sol_usdc_orca = self.pool_config.get_orca_pool("SOL/USDC").ok_or_else(|| anyhow::anyhow!("SOL/USDC Orca pool not found in config"))?;
        let pool_pubkey = sol_usdc_orca.get_pubkey()?;
        let accounts = self.build_orca_swap_accounts(&pool_pubkey, &self.wallet.pubkey(), is_a_to_b, &pool_state.token_mint_a, &pool_state.token_mint_b, pool_state)?;
        let data = self.build_orca_swap_data(amount_in, min_amount_out, is_a_to_b, pool_state.sqrt_price, MAX_SLIPPAGE_BPS);
        Ok((accounts, data))
    }
    
    /// Build Raydium swap instruction
    fn build_raydium_swap(&self, pool_state: &crate::dex_connectors::raydium_connector::AmmInfo, amount_in: u64, min_amount_out: u64, is_a_to_b: bool) -> Result<(Vec<AccountMeta>, Vec<u8>)> {
        let sol_usdc_raydium = self.pool_config.get_raydium_pool("SOL/USDC").ok_or_else(|| anyhow::anyhow!("SOL/USDC Raydium pool not found in config"))?;
        let pool_pubkey = sol_usdc_raydium.get_pubkey()?;
        let accounts = self.build_raydium_swap_accounts(&pool_pubkey, &self.wallet.pubkey(), is_a_to_b, &pool_state.coin_vault_mint, &pool_state.pc_vault_mint, &pool_state.market)?;
        let data = self.build_raydium_swap_data(amount_in, min_amount_out);
        Ok((accounts, data))
    }
    
    /// Derive tick array address for Orca Whirlpool
    fn derive_tick_array(pool: &Pubkey, start_tick_index: i32) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM)
            .map_err(|e| anyhow::anyhow!("Failed to parse Orca Whirlpool program ID: {}", e))?;
        let (tick_array, _) = Pubkey::find_program_address(
            &[
                b"tick_array",
                pool.as_ref(),
                &start_tick_index.to_le_bytes(),
            ],
            &program_id,
        );
        Ok(tick_array)
    }
    
    /// Calculate appropriate tick array indices based on current tick and direction
    fn calculate_tick_array_indices(current_tick: i32, tick_spacing: u16, is_a_to_b: bool) -> Vec<i32> {
        let tick_spacing = tick_spacing as i32;
        let mut indices = Vec::new();
        
        // Calculate the tick array index that contains the current tick
        let current_array_start = (current_tick / (tick_spacing * TICK_ARRAY_SIZE)) * (tick_spacing * TICK_ARRAY_SIZE);
        
        if is_a_to_b {
            // For A to B swaps, we might need the current array and the next one
            indices.push(current_array_start);
            indices.push(current_array_start + (tick_spacing * TICK_ARRAY_SIZE));
        } else {
            // For B to A swaps, we might need the current array and the previous one
            indices.push(current_array_start);
            if current_array_start >= tick_spacing * TICK_ARRAY_SIZE {
                indices.push(current_array_start - (tick_spacing * TICK_ARRAY_SIZE));
            }
        }
        
        indices
    }
    
    /// Derive oracle address for Orca Whirlpool
    fn derive_oracle(pool: &Pubkey) -> Result<Pubkey> {
        let program_id = Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM)
            .map_err(|e| anyhow::anyhow!("Failed to parse Orca Whirlpool program ID: {}", e))?;
        let (oracle, _) = Pubkey::find_program_address(
            &[
                b"oracle",
                pool.as_ref(),
            ],
            &program_id,
        );
        Ok(oracle)
    }
    
    /// Derive PDA addresses for Raydium AMM
    fn derive_raydium_pdas(pool: &Pubkey, market: &Pubkey) -> Result<(Pubkey, Pubkey)> {
        let raydium_program = Pubkey::from_str(RAYDIUM_V4_PROGRAM)
            .map_err(|e| anyhow::anyhow!("Failed to parse Raydium V4 program ID: {}", e))?;
        let serum_program = Pubkey::from_str(SERUM_PROGRAM_ID)
            .map_err(|e| anyhow::anyhow!("Failed to parse Serum program ID: {}", e))?;
        
        let (open_orders, _) = Pubkey::find_program_address(
            &[
                b"open_order",
                pool.as_ref(),
                market.as_ref(),
            ],
            &raydium_program,
        );
        
        let (market_authority, _) = Pubkey::find_program_address(
            &[
                b"market",
                market.as_ref(),
            ],
            &serum_program,
        );
        
        Ok((open_orders, market_authority))
    }
}
