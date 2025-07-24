use anchor_lang::prelude::*;
use anchor_spl::token::{self, Token, TokenAccount};
use fixed::types::I80F48;
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
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;

const MAX_SLIPPAGE_BPS: u64 = 50; // 0.5%
const REBALANCE_THRESHOLD_BPS: u64 = 100; // 1%
const MIN_PROFIT_BPS: u64 = 10; // 0.1%
const MAX_LEVERAGE: u64 = 300; // 3x
const COLLATERAL_RATIO_TARGET: u64 = 150; // 150%
const LIQUIDATION_THRESHOLD: u64 = 120; // 120%
const MAX_POSITION_SIZE_USD: u64 = 1_000_000;
const ORACLE_CONFIDENCE_THRESHOLD: u64 = 100; // 1%
const MAX_RETRIES: u32 = 3;
const RETRY_DELAY_MS: u64 = 100;

#[derive(Clone, Debug)]
pub struct PortfolioPosition {
    pub token_mint: Pubkey,
    pub amount: I80F48,
    pub value_usd: I80F48,
    pub delta: I80F48,
    pub lending_pool: Pubkey,
    pub collateral_ratio: I80F48,
    pub unrealized_pnl: I80F48,
    pub entry_price: I80F48,
    pub last_update: u64,
}

#[derive(Clone, Debug)]
pub struct MarketData {
    pub price: I80F48,
    pub volume_24h: I80F48,
    pub volatility: I80F48,
    pub bid_ask_spread: I80F48,
    pub liquidity_depth: I80F48,
    pub funding_rate: I80F48,
    pub oracle_confidence: I80F48,
    pub last_update: u64,
}

#[derive(Clone)]
pub struct DeltaNeutralConfig {
    pub max_positions: usize,
    pub rebalance_interval_ms: u64,
    pub min_liquidity_usd: I80F48,
    pub max_drawdown_pct: I80F48,
    pub target_delta: I80F48,
    pub delta_tolerance: I80F48,
    pub compute_unit_limit: u32,
    pub priority_fee_lamports: u64,
}

pub struct DeltaNeutralPortfolioManager {
    rpc_client: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    positions: Arc<RwLock<HashMap<Pubkey, PortfolioPosition>>>,
    market_data: Arc<RwLock<HashMap<Pubkey, MarketData>>>,
    config: DeltaNeutralConfig,
    total_value_usd: Arc<RwLock<I80F48>>,
    portfolio_delta: Arc<RwLock<I80F48>>,
    last_rebalance: Arc<RwLock<u64>>,
}

impl DeltaNeutralPortfolioManager {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        wallet: Arc<Keypair>,
        config: DeltaNeutralConfig,
    ) -> Self {
        Self {
            rpc_client,
            wallet,
            positions: Arc::new(RwLock::new(HashMap::new())),
            market_data: Arc::new(RwLock::new(HashMap::new())),
            config,
            total_value_usd: Arc::new(RwLock::new(I80F48::ZERO)),
            portfolio_delta: Arc::new(RwLock::new(I80F48::ZERO)),
            last_rebalance: Arc::new(RwLock::new(0)),
        }
    }

    pub async fn update_market_data(&self, token_mint: &Pubkey) -> Result<()> {
        let oracle_data = self.fetch_oracle_price(token_mint).await?;
        let market_depth = self.fetch_market_depth(token_mint).await?;
        
        let mut market_data_map = self.market_data.write().await;
        market_data_map.insert(
            *token_mint,
            MarketData {
                price: oracle_data.0,
                volume_24h: market_depth.2,
                volatility: self.calculate_volatility(token_mint).await?,
                bid_ask_spread: market_depth.3,
                liquidity_depth: market_depth.0 + market_depth.1,
                funding_rate: self.fetch_funding_rate(token_mint).await?,
                oracle_confidence: oracle_data.1,
                last_update: self.get_timestamp(),
            },
        );
        
        Ok(())
    }

    async fn fetch_oracle_price(&self, token_mint: &Pubkey) -> Result<(I80F48, I80F48)> {
        let pyth_price_key = self.get_pyth_price_account(token_mint);
        let account_data = self.rpc_client.get_account(&pyth_price_key).await?;
        
        let price_data = &account_data.data[48..];
        let price = i64::from_le_bytes(price_data[0..8].try_into().unwrap());
        let confidence = u64::from_le_bytes(price_data[8..16].try_into().unwrap());
        let expo = i32::from_le_bytes(price_data[20..24].try_into().unwrap());
        
        let price_i80f48 = I80F48::from_num(price) / I80F48::from_num(10_i64.pow((-expo) as u32));
        let confidence_i80f48 = I80F48::from_num(confidence) / I80F48::from_num(price);
        
        Ok((price_i80f48, confidence_i80f48))
    }

    async fn fetch_market_depth(&self, token_mint: &Pubkey) -> Result<(I80F48, I80F48, I80F48, I80F48)> {
        let serum_market = self.get_serum_market(token_mint);
        let account_data = self.rpc_client.get_account(&serum_market).await?;
        
        let bids_key = Pubkey::new(&account_data.data[45..77]);
        let asks_key = Pubkey::new(&account_data.data[77..109]);
        
        let bids_data = self.rpc_client.get_account(&bids_key).await?;
        let asks_data = self.rpc_client.get_account(&asks_key).await?;
        
        let (bid_liquidity, best_bid) = self.parse_orderbook(&bids_data.data, true);
        let (ask_liquidity, best_ask) = self.parse_orderbook(&asks_data.data, false);
        
        let volume_24h = self.fetch_24h_volume(token_mint).await?;
        let spread = (best_ask - best_bid) / best_bid * I80F48::from_num(10000);
        
        Ok((bid_liquidity, ask_liquidity, volume_24h, spread))
    }

    fn parse_orderbook(&self, data: &[u8], is_bids: bool) -> (I80F48, I80F48) {
        let mut total_liquidity = I80F48::ZERO;
        let mut best_price = I80F48::ZERO;
        
        let header_len = 45;
        let node_size = 68;
        let max_nodes = (data.len() - header_len) / node_size;
        
        for i in 0..max_nodes.min(100) {
            let offset = header_len + i * node_size;
            if offset + node_size > data.len() { break; }
            
            let price_raw = u64::from_le_bytes(data[offset..offset+8].try_into().unwrap());
            let quantity_raw = u64::from_le_bytes(data[offset+8..offset+16].try_into().unwrap());
            
            if price_raw == 0 || quantity_raw == 0 { continue; }
            
            let price = I80F48::from_num(price_raw) / I80F48::from_num(1e6);
            let quantity = I80F48::from_num(quantity_raw) / I80F48::from_num(1e6);
            
            total_liquidity += price * quantity;
            
            if best_price == I80F48::ZERO {
                best_price = price;
            }
        }
        
        (total_liquidity, best_price)
    }

    async fn calculate_volatility(&self, token_mint: &Pubkey) -> Result<I80F48> {
        let market_data = self.market_data.read().await;
        if let Some(data) = market_data.get(token_mint) {
            let funding_rate = data.funding_rate.abs();
            let spread_factor = data.bid_ask_spread / I80F48::from_num(10000);
            let base_vol = I80F48::from_num(0.5);
            
            Ok(base_vol + funding_rate * I80F48::from_num(10) + spread_factor * I80F48::from_num(5))
        } else {
            Ok(I80F48::from_num(0.5))
        }
    }

    async fn fetch_funding_rate(&self, token_mint: &Pubkey) -> Result<I80F48> {
        let perp_market = self.get_perp_market(token_mint);
        let account_data = self.rpc_client.get_account(&perp_market).await?;
        
        if account_data.data.len() >= 256 {
            let funding_rate_raw = i64::from_le_bytes(account_data.data[176..184].try_into().unwrap());
            Ok(I80F48::from_num(funding_rate_raw) / I80F48::from_num(1e9))
        } else {
            Ok(I80F48::ZERO)
        }
    }

    async fn fetch_24h_volume(&self, token_mint: &Pubkey) -> Result<I80F48> {
        let serum_market = self.get_serum_market(token_mint);
        let account_data = self.rpc_client.get_account(&serum_market).await?;
        
        if account_data.data.len() >= 388 {
            let volume_raw = u64::from_le_bytes(account_data.data[380..388].try_into().unwrap());
            Ok(I80F48::from_num(volume_raw) / I80F48::from_num(1e6))
        } else {
            Ok(I80F48::from_num(1000000))
        }
    }

    pub async fn calculate_portfolio_delta(&self) -> Result<I80F48> {
        let positions = self.positions.read().await;
        let market_data = self.market_data.read().await;
        
        let mut total_delta = I80F48::ZERO;
        
        for (mint, position) in positions.iter() {
            if let Some(market) = market_data.get(mint) {
                let position_delta = position.amount * market.price / I80F48::from_num(1e6);
                let volatility_adj = I80F48::ONE + market.volatility / I80F48::from_num(100);
                total_delta += position_delta * volatility_adj;
            }
        }
        
        Ok(total_delta)
    }

    pub async fn rebalance_portfolio(&self) -> Result<Vec<Instruction>> {
        let current_time = self.get_timestamp();
        let last_rebalance = *self.last_rebalance.read().await;
        
        if current_time - last_rebalance < self.config.rebalance_interval_ms {
            return Ok(Vec::new());
        }
        
        let current_delta = self.calculate_portfolio_delta().await?;
        let delta_diff = (current_delta - self.config.target_delta).abs();
        
        if delta_diff < self.config.delta_tolerance {
            return Ok(Vec::new());
        }
        
        let mut instructions = Vec::new();
        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(self.config.compute_unit_limit));
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(self.config.priority_fee_lamports));
        
        let positions = self.positions.read().await;
        let market_data = self.market_data.read().await;
        
        let mut position_deltas: Vec<(Pubkey, I80F48, I80F48)> = Vec::new();
        
        for (mint, position) in positions.iter() {
            if let Some(market) = market_data.get(mint) {
                let pos_delta = position.amount * market.price / I80F48::from_num(1e6);
                position_deltas.push((*mint, pos_delta, market.liquidity_depth));
            }
        }
        
        position_deltas.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        
        let delta_to_adjust = current_delta - self.config.target_delta;
        let mut remaining_adjustment = delta_to_adjust;
        
        for (mint, pos_delta, liquidity) in position_deltas {
            if remaining_adjustment.abs() < I80F48::from_num(100) {
                break;
            }
            
                        let max_adjustment = liquidity * I80F48::from_num(0.1);
            let adjustment = if remaining_adjustment > I80F48::ZERO {
                remaining_adjustment.min(max_adjustment).min(pos_delta * I80F48::from_num(0.5))
            } else {
                remaining_adjustment.max(-max_adjustment).max(-pos_delta * I80F48::from_num(0.5))
            };
            
            if adjustment.abs() > I80F48::from_num(50) {
                let market = market_data.get(&mint).unwrap();
                let amount_to_trade = adjustment / market.price * I80F48::from_num(1e6);
                
                let instruction = if adjustment < I80F48::ZERO {
                    self.create_reduce_position_ix(&mint, amount_to_trade.abs()).await?
                } else {
                    self.create_increase_position_ix(&mint, amount_to_trade.abs()).await?
                };
                
                instructions.push(instruction);
                remaining_adjustment -= adjustment;
            }
        }
        
        *self.last_rebalance.write().await = current_time;
        Ok(instructions)
    }

    async fn create_reduce_position_ix(&self, token_mint: &Pubkey, amount: I80F48) -> Result<Instruction> {
        let position = self.positions.read().await.get(token_mint).cloned()
            .ok_or(ErrorCode::PositionNotFound)?;
        
        let amount_native = (amount * I80F48::from_num(1e6)).to_num::<u64>();
        let lending_pool = position.lending_pool;
        
        let (pool_authority, _) = Pubkey::find_program_address(
            &[lending_pool.as_ref(), b"authority"],
            &self.get_lending_program(),
        );
        
        let (user_collateral_account, _) = Pubkey::find_program_address(
            &[self.wallet.pubkey().as_ref(), token_mint.as_ref(), b"collateral"],
            &self.get_lending_program(),
        );
        
        let accounts = vec![
            AccountMeta::new(lending_pool, false),
            AccountMeta::new(pool_authority, false),
            AccountMeta::new(user_collateral_account, false),
            AccountMeta::new(self.wallet.pubkey(), true),
            AccountMeta::new_readonly(*token_mint, false),
            AccountMeta::new_readonly(spl_token::id(), false),
            AccountMeta::new_readonly(self.get_oracle_account(token_mint), false),
        ];
        
        let data = self.encode_withdraw_instruction(amount_native);
        
        Ok(Instruction {
            program_id: self.get_lending_program(),
            accounts,
            data,
        })
    }

    async fn create_increase_position_ix(&self, token_mint: &Pubkey, amount: I80F48) -> Result<Instruction> {
        let amount_native = (amount * I80F48::from_num(1e6)).to_num::<u64>();
        let lending_pool = self.get_lending_pool(token_mint);
        
        let (pool_authority, _) = Pubkey::find_program_address(
            &[lending_pool.as_ref(), b"authority"],
            &self.get_lending_program(),
        );
        
        let (user_collateral_account, _) = Pubkey::find_program_address(
            &[self.wallet.pubkey().as_ref(), token_mint.as_ref(), b"collateral"],
            &self.get_lending_program(),
        );
        
        let (pool_token_account, _) = Pubkey::find_program_address(
            &[lending_pool.as_ref(), token_mint.as_ref()],
            &self.get_lending_program(),
        );
        
        let accounts = vec![
            AccountMeta::new(lending_pool, false),
            AccountMeta::new(pool_authority, false),
            AccountMeta::new(user_collateral_account, false),
            AccountMeta::new(pool_token_account, false),
            AccountMeta::new(self.wallet.pubkey(), true),
            AccountMeta::new_readonly(*token_mint, false),
            AccountMeta::new_readonly(spl_token::id(), false),
            AccountMeta::new_readonly(self.get_oracle_account(token_mint), false),
        ];
        
        let data = self.encode_deposit_instruction(amount_native);
        
        Ok(Instruction {
            program_id: self.get_lending_program(),
            accounts,
            data,
        })
    }

    pub async fn open_delta_neutral_position(
        &self,
        long_token: &Pubkey,
        short_token: &Pubkey,
        size_usd: I80F48,
    ) -> Result<Vec<Instruction>> {
        let mut instructions = Vec::new();
        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(400_000));
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(self.config.priority_fee_lamports));
        
        let market_data = self.market_data.read().await;
        let long_market = market_data.get(long_token).ok_or(ErrorCode::MarketDataNotFound)?;
        let short_market = market_data.get(short_token).ok_or(ErrorCode::MarketDataNotFound)?;
        
        if long_market.oracle_confidence > I80F48::from_num(ORACLE_CONFIDENCE_THRESHOLD) / I80F48::from_num(10000) ||
           short_market.oracle_confidence > I80F48::from_num(ORACLE_CONFIDENCE_THRESHOLD) / I80F48::from_num(10000) {
            return Err(ErrorCode::OraclePriceStale.into());
        }
        
        let long_amount = size_usd / long_market.price;
        let short_amount = size_usd / short_market.price;
        
        let long_ix = self.create_long_position_ix(long_token, long_amount).await?;
        let short_ix = self.create_short_position_ix(short_token, short_amount).await?;
        
        instructions.push(long_ix);
        instructions.push(short_ix);
        
        let mut positions = self.positions.write().await;
        positions.insert(
            *long_token,
            PortfolioPosition {
                token_mint: *long_token,
                amount: long_amount,
                value_usd: size_usd,
                delta: long_amount,
                lending_pool: self.get_lending_pool(long_token),
                collateral_ratio: I80F48::from_num(COLLATERAL_RATIO_TARGET) / I80F48::from_num(100),
                unrealized_pnl: I80F48::ZERO,
                entry_price: long_market.price,
                last_update: self.get_timestamp(),
            },
        );
        
        positions.insert(
            *short_token,
            PortfolioPosition {
                token_mint: *short_token,
                amount: -short_amount,
                value_usd: size_usd,
                delta: -short_amount,
                lending_pool: self.get_lending_pool(short_token),
                collateral_ratio: I80F48::from_num(COLLATERAL_RATIO_TARGET) / I80F48::from_num(100),
                unrealized_pnl: I80F48::ZERO,
                entry_price: short_market.price,
                last_update: self.get_timestamp(),
            },
        );
        
        Ok(instructions)
    }

    async fn create_long_position_ix(&self, token_mint: &Pubkey, amount: I80F48) -> Result<Instruction> {
        let amount_native = (amount * I80F48::from_num(1e6)).to_num::<u64>();
        let lending_pool = self.get_lending_pool(token_mint);
        
        let accounts = self.build_lending_accounts(lending_pool, token_mint, true);
        let data = self.encode_borrow_instruction(amount_native, false);
        
        Ok(Instruction {
            program_id: self.get_lending_program(),
            accounts,
            data,
        })
    }

    async fn create_short_position_ix(&self, token_mint: &Pubkey, amount: I80F48) -> Result<Instruction> {
        let amount_native = (amount * I80F48::from_num(1e6)).to_num::<u64>();
        let lending_pool = self.get_lending_pool(token_mint);
        
        let accounts = self.build_lending_accounts(lending_pool, token_mint, false);
        let data = self.encode_borrow_instruction(amount_native, true);
        
        Ok(Instruction {
            program_id: self.get_lending_program(),
            accounts,
            data,
        })
    }

    fn build_lending_accounts(&self, pool: Pubkey, mint: &Pubkey, is_long: bool) -> Vec<AccountMeta> {
        let (pool_authority, _) = Pubkey::find_program_address(
            &[pool.as_ref(), b"authority"],
            &self.get_lending_program(),
        );
        
        let (user_account, _) = Pubkey::find_program_address(
            &[self.wallet.pubkey().as_ref(), pool.as_ref()],
            &self.get_lending_program(),
        );
        
        let (reserve_account, _) = Pubkey::find_program_address(
            &[pool.as_ref(), mint.as_ref(), b"reserve"],
            &self.get_lending_program(),
        );
        
        vec![
            AccountMeta::new(pool, false),
            AccountMeta::new(pool_authority, false),
            AccountMeta::new(user_account, false),
            AccountMeta::new(reserve_account, false),
            AccountMeta::new(self.wallet.pubkey(), true),
            AccountMeta::new_readonly(*mint, false),
            AccountMeta::new_readonly(self.get_oracle_account(mint), false),
            AccountMeta::new_readonly(spl_token::id(), false),
            AccountMeta::new_readonly(anchor_lang::system_program::ID, false),
        ]
    }

    pub async fn monitor_and_liquidate(&self) -> Result<Vec<Pubkey>> {
        let mut positions_to_liquidate = Vec::new();
        let positions = self.positions.read().await;
        let market_data = self.market_data.read().await;
        
        for (mint, position) in positions.iter() {
            if let Some(market) = market_data.get(mint) {
                let current_value = position.amount.abs() * market.price;
                let collateral_value = current_value * position.collateral_ratio;
                let borrowed_value = current_value - collateral_value;
                
                let current_ratio = if borrowed_value > I80F48::ZERO {
                    collateral_value / borrowed_value * I80F48::from_num(100)
                } else {
                    I80F48::from_num(999)
                };
                
                if current_ratio < I80F48::from_num(LIQUIDATION_THRESHOLD) {
                    positions_to_liquidate.push(*mint);
                }
            }
        }
        
        Ok(positions_to_liquidate)
    }

    pub async fn execute_emergency_exit(&self) -> Result<Vec<Transaction>> {
        let mut transactions = Vec::new();
        let positions = self.positions.read().await;
        
        for (mint, position) in positions.iter() {
            let mut instructions = Vec::new();
            instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(400_000));
            instructions.push(ComputeBudgetInstruction::set_compute_unit_price(50_000));
            
            let close_ix = if position.amount > I80F48::ZERO {
                self.create_reduce_position_ix(mint, position.amount.abs()).await?
            } else {
                self.create_cover_short_ix(mint, position.amount.abs()).await?
            };
            
            instructions.push(close_ix);
            
            let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;
            let tx = Transaction::new_signed_with_payer(
                &instructions,
                Some(&self.wallet.pubkey()),
                &[&*self.wallet],
                recent_blockhash,
            );
            
            transactions.push(tx);
        }
        
        Ok(transactions)
    }

    async fn create_cover_short_ix(&self, token_mint: &Pubkey, amount: I80F48) -> Result<Instruction> {
        let amount_native = (amount * I80F48::from_num(1e6)).to_num::<u64>();
        let lending_pool = self.get_lending_pool(token_mint);
        
        let accounts = self.build_lending_accounts(lending_pool, token_mint, false);
        let data = self.encode_repay_instruction(amount_native);
        
        Ok(Instruction {
            program_id: self.get_lending_program(),
            accounts,
            data,
        })
    }

    pub async fn calculate_portfolio_metrics(&self) -> Result<PortfolioMetrics> {
        let positions = self.positions.read().await;
        let market_data = self.market_data.read().await;
        
        let mut total_value = I80F48::ZERO;
        let mut total_pnl = I80F48::ZERO;
        let mut total_collateral = I80F48::ZERO;
        let mut total_borrowed = I80F48::ZERO;
        
        for (mint, position) in positions.iter() {
            if let Some(market) = market_data.get(mint) {
                let current_value = position.amount.abs() * market.price;
                total_value += current_value;
                
                let pnl = if position.amount > I80F48::ZERO {
                    (market.price - position.entry_price) * position.amount
                } else {
                    (position.entry_price - market.price) * position.amount.abs()
                };
                
                total_pnl += pnl;
                
                let collateral = current_value * position.collateral_ratio;
                total_collateral += collateral;
                total_borrowed += current_value - collateral;
            }
        }
        
        let leverage = if total_collateral > I80F48::ZERO {
            total_value / total_collateral
        } else {
            I80F48::ONE
        };
        
        Ok(PortfolioMetrics {
            total_value,
            total_pnl,
            portfolio_delta: self.calculate_portfolio_delta().await?,
            leverage,
                        collateral_ratio: if total_borrowed > I80F48::ZERO {
                total_collateral / total_borrowed
            } else {
                I80F48::from_num(999)
            },
            position_count: positions.len(),
        })
    }

    fn encode_deposit_instruction(&self, amount: u64) -> Vec<u8> {
        let mut data = vec![1u8]; // Deposit instruction discriminator
        data.extend_from_slice(&amount.to_le_bytes());
        data
    }

    fn encode_withdraw_instruction(&self, amount: u64) -> Vec<u8> {
        let mut data = vec![2u8]; // Withdraw instruction discriminator
        data.extend_from_slice(&amount.to_le_bytes());
        data
    }

    fn encode_borrow_instruction(&self, amount: u64, is_short: bool) -> Vec<u8> {
        let mut data = vec![3u8]; // Borrow instruction discriminator
        data.extend_from_slice(&amount.to_le_bytes());
        data.push(if is_short { 1u8 } else { 0u8 });
        data
    }

    fn encode_repay_instruction(&self, amount: u64) -> Vec<u8> {
        let mut data = vec![4u8]; // Repay instruction discriminator
        data.extend_from_slice(&amount.to_le_bytes());
        data
    }

    fn get_lending_program(&self) -> Pubkey {
        // Solend program ID
        Pubkey::from_str("So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo").unwrap()
    }

    fn get_lending_pool(&self, token_mint: &Pubkey) -> Pubkey {
        // Main pool mappings for major tokens
        match token_mint.to_string().as_str() {
            "So11111111111111111111111111111111111111112" => {
                Pubkey::from_str("8PbodeaosQP19SjYFx855UMqWxH2HynZLdBXmsrbac36").unwrap()
            }
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" => {
                Pubkey::from_str("7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj").unwrap()
            }
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB" => {
                Pubkey::from_str("B7Lg4cJZHPLFaGdqfaAWG35KFFaEtBMmRAGf98kNaogt").unwrap()
            }
            _ => {
                Pubkey::from_str("DdZR6zRFiUt4S5mg7AV1uKB2z1f1WzcNYCaTEEWPAuby").unwrap()
            }
        }
    }

    fn get_oracle_account(&self, token_mint: &Pubkey) -> Pubkey {
        // Pyth oracle mappings
        match token_mint.to_string().as_str() {
            "So11111111111111111111111111111111111111112" => {
                Pubkey::from_str("H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG").unwrap()
            }
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" => {
                Pubkey::from_str("Gnt27xtC473ZT2Mw5u8wZ68Z3gULkSTb5DuxJy7eJotD").unwrap()
            }
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB" => {
                Pubkey::from_str("3Ftc81diCdnRzCDLX1TtoUbnxMAoqzT7wqJ7vfap8YsF").unwrap()
            }
            _ => {
                Pubkey::from_str("GVXRSBjFk6e6J3NbVPXohDJetcTjaeeuykUpbQF8UoMU").unwrap()
            }
        }
    }

    fn get_pyth_price_account(&self, token_mint: &Pubkey) -> Pubkey {
        self.get_oracle_account(token_mint)
    }

    fn get_serum_market(&self, token_mint: &Pubkey) -> Pubkey {
        // Serum DEX market addresses
        match token_mint.to_string().as_str() {
            "So11111111111111111111111111111111111111112" => {
                Pubkey::from_str("9wFFyRfZBsuAha4YcuxcXLKwMxJR43S7fPfQLusDBzvT").unwrap()
            }
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" => {
                Pubkey::from_str("9wFFyRfZBsuAha4YcuxcXLKwMxJR43S7fPfQLusDBzvT").unwrap()
            }
            _ => {
                Pubkey::from_str("8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYwn1XTh6").unwrap()
            }
        }
    }

    fn get_perp_market(&self, token_mint: &Pubkey) -> Pubkey {
        // Mango perp market addresses
        match token_mint.to_string().as_str() {
            "So11111111111111111111111111111111111111112" => {
                Pubkey::from_str("ESdnpnNLgTkBCZRuTJkZLi5wKEZ2z47SG3PJrhundSQ2").unwrap()
            }
            _ => {
                Pubkey::from_str("FmKAfMMnxjVECj1wLCqzHAcWnNDUgYS98LKNw88KgF9Y").unwrap()
            }
        }
    }

    fn get_timestamp(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    pub async fn send_transaction_with_retry(&self, transaction: Transaction) -> Result<()> {
        let mut retries = 0;
        let commitment = CommitmentConfig::confirmed();
        
        loop {
            match self.rpc_client.send_and_confirm_transaction_with_spinner_and_commitment(
                &transaction,
                commitment,
            ).await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    retries += 1;
                    if retries >= MAX_RETRIES {
                        return Err(e.into());
                    }
                    tokio::time::sleep(Duration::from_millis(RETRY_DELAY_MS * retries as u64)).await;
                }
            }
        }
    }

    pub async fn optimize_gas_usage(&self, instructions: &mut Vec<Instruction>) -> Result<()> {
        let simulation_result = self.rpc_client.simulate_transaction(
            &Transaction::new_with_payer(
                instructions,
                Some(&self.wallet.pubkey()),
            ),
        ).await?;
        
        if let Some(units_consumed) = simulation_result.value.units_consumed {
            let optimal_units = (units_consumed as f64 * 1.1) as u32;
            instructions[0] = ComputeBudgetInstruction::set_compute_unit_limit(optimal_units);
        }
        
        Ok(())
    }

    pub async fn get_optimal_entry_timing(&self, token_mint: &Pubkey) -> Result<bool> {
        let market_data = self.market_data.read().await;
        let market = market_data.get(token_mint).ok_or(ErrorCode::MarketDataNotFound)?;
        
        let spread_bps = market.bid_ask_spread;
        let volume_threshold = I80F48::from_num(100_000);
        let volatility_threshold = I80F48::from_num(0.02);
        
        let is_liquid = market.liquidity_depth > volume_threshold;
        let is_stable = market.volatility < volatility_threshold;
        let is_tight_spread = spread_bps < I80F48::from_num(MAX_SLIPPAGE_BPS);
        
        Ok(is_liquid && is_stable && is_tight_spread)
    }

    pub async fn calculate_optimal_position_size(
        &self,
        available_capital: I80F48,
        token_mint: &Pubkey,
    ) -> Result<I80F48> {
        let market_data = self.market_data.read().await;
        let market = market_data.get(token_mint).ok_or(ErrorCode::MarketDataNotFound)?;
        
        let volatility_factor = I80F48::ONE - market.volatility / I80F48::from_num(10);
        let liquidity_factor = (market.liquidity_depth / I80F48::from_num(1_000_000)).min(I80F48::ONE);
        let confidence_factor = I80F48::ONE - market.oracle_confidence;
        
        let risk_adjusted_size = available_capital * volatility_factor * liquidity_factor * confidence_factor;
        let max_position = I80F48::from_num(MAX_POSITION_SIZE_USD);
        
        Ok(risk_adjusted_size.min(max_position))
    }
}

#[derive(Clone, Debug)]
pub struct PortfolioMetrics {
    pub total_value: I80F48,
    pub total_pnl: I80F48,
    pub portfolio_delta: I80F48,
    pub leverage: I80F48,
    pub collateral_ratio: I80F48,
    pub position_count: usize,
}

#[derive(Debug)]
pub enum ErrorCode {
    PositionNotFound,
    MarketDataNotFound,
    OraclePriceStale,
    InsufficientLiquidity,
    MaxLeverageExceeded,
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ErrorCode::PositionNotFound => write!(f, "Position not found"),
            ErrorCode::MarketDataNotFound => write!(f, "Market data not found"),
            ErrorCode::OraclePriceStale => write!(f, "Oracle price is stale"),
            ErrorCode::InsufficientLiquidity => write!(f, "Insufficient liquidity"),
            ErrorCode::MaxLeverageExceeded => write!(f, "Maximum leverage exceeded"),
        }
    }
}

impl std::error::Error for ErrorCode {}

impl From<ErrorCode> for Error {
    fn from(e: ErrorCode) -> Self {
        Error::msg(e.to_string())
    }
}

use std::str::FromStr;
type Result<T> = std::result::Result<T, Error>;
use anchor_lang::error::Error;

