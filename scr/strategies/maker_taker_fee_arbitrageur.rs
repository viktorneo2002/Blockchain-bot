use anchor_lang::prelude::*;
use anchor_spl::token::{Token, TokenAccount};
use borsh::{BorshDeserialize, BorshSerialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
    sysvar,
};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tokio::time::{sleep, Duration};

const SERUM_PROGRAM_ID: &str = "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin";
const MIN_PROFIT_THRESHOLD_BPS: u64 = 5;
const MAX_SLIPPAGE_BPS: u64 = 10;
const COMPUTE_UNITS: u32 = 400_000;
const PRIORITY_FEE_LAMPORTS: u64 = 50_000;
const MAKER_FEE_BPS: i64 = -3;
const TAKER_FEE_BPS: i64 = 5;
const MAX_RETRIES: u8 = 3;
const SLOT_BUFFER: u64 = 2;

#[derive(Clone, Debug)]
pub struct MarketInfo {
    market_address: Pubkey,
    base_mint: Pubkey,
    quote_mint: Pubkey,
    base_vault: Pubkey,
    quote_vault: Pubkey,
    request_queue: Pubkey,
    event_queue: Pubkey,
    bids: Pubkey,
    asks: Pubkey,
    base_lot_size: u64,
    quote_lot_size: u64,
    tick_size: u64,
    fee_rate_bps: u64,
    vault_signer: Pubkey,
}

#[derive(Clone, Debug)]
pub struct ArbitrageOpportunity {
    market: MarketInfo,
    side: OrderSide,
    price: u64,
    size: u64,
    profit_bps: u64,
    maker_rebate: i64,
    taker_fee: i64,
    timestamp: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OrderSide {
    Buy = 0,
    Sell = 1,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct MarketState {
    pub account_flags: u64,
    pub own_address: [u8; 32],
    pub vault_signer_nonce: u64,
    pub coin_mint: [u8; 32],
    pub pc_mint: [u8; 32],
    pub coin_vault: [u8; 32],
    pub coin_deposits_total: u64,
    pub coin_fees_accrued: u64,
    pub pc_vault: [u8; 32],
    pub pc_deposits_total: u64,
    pub pc_fees_accrued: u64,
    pub pc_dust_threshold: u64,
    pub req_q: [u8; 32],
    pub event_q: [u8; 32],
    pub bids: [u8; 32],
    pub asks: [u8; 32],
    pub coin_lot_size: u64,
    pub pc_lot_size: u64,
    pub fee_rate_bps: u64,
    pub referrer_rebates_accrued: u64,
}

pub struct MakerTakerArbitrageur {
    rpc: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    markets: Arc<RwLock<HashMap<Pubkey, MarketInfo>>>,
    token_accounts: Arc<RwLock<HashMap<Pubkey, Pubkey>>>,
    open_orders: Arc<RwLock<HashMap<Pubkey, Pubkey>>>,
    last_slot: Arc<RwLock<u64>>,
}

impl MakerTakerArbitrageur {
    pub fn new(rpc_url: &str, wallet: Keypair) -> Self {
        Self {
            rpc: Arc::new(RpcClient::new_with_commitment(
                rpc_url.to_string(),
                CommitmentConfig {
                    commitment: CommitmentLevel::Processed,
                },
            )),
            wallet: Arc::new(wallet),
            markets: Arc::new(RwLock::new(HashMap::new())),
            token_accounts: Arc::new(RwLock::new(HashMap::new())),
            open_orders: Arc::new(RwLock::new(HashMap::new())),
            last_slot: Arc::new(RwLock::new(0)),
        }
    }

    pub async fn initialize_markets(&self, market_addresses: Vec<&str>) -> Result<()> {
        let mut markets = self.markets.write().await;
        let serum_program = Pubkey::from_str(SERUM_PROGRAM_ID)
            .map_err(|_| Error::InvalidProgramId)?;
        
        for addr_str in market_addresses {
            let market_addr = Pubkey::from_str(addr_str)
                .map_err(|_| Error::InvalidMarketAddress)?;
            
            let account_data = self.retry_rpc(|| async {
                self.rpc.get_account_data(&market_addr).await
            }).await?;
            
            if account_data.len() < 381 {
                continue;
            }
            
            let market_state = Self::unpack_market(&account_data[5..])?;
            
            let (vault_signer, _) = Pubkey::find_program_address(
                &[&market_addr.to_bytes()[..], &[market_state.vault_signer_nonce as u8][..]],
                &serum_program,
            );
            
            let market_info = MarketInfo {
                market_address: market_addr,
                base_mint: Pubkey::new(&market_state.coin_mint),
                quote_mint: Pubkey::new(&market_state.pc_mint),
                base_vault: Pubkey::new(&market_state.coin_vault),
                quote_vault: Pubkey::new(&market_state.pc_vault),
                request_queue: Pubkey::new(&market_state.req_q),
                event_queue: Pubkey::new(&market_state.event_q),
                bids: Pubkey::new(&market_state.bids),
                asks: Pubkey::new(&market_state.asks),
                base_lot_size: market_state.coin_lot_size,
                quote_lot_size: market_state.pc_lot_size,
                tick_size: market_state.pc_lot_size,
                fee_rate_bps: market_state.fee_rate_bps,
                vault_signer,
            };
            
            markets.insert(market_addr, market_info);
        }
        
        Ok(())
    }

    fn unpack_market(data: &[u8]) -> Result<MarketState> {
        if data.len() < 376 {
            return Err(Error::MarketDeserializationError);
        }
        
        Ok(MarketState {
            account_flags: u64::from_le_bytes(data[0..8].try_into().unwrap()),
            own_address: data[8..40].try_into().unwrap(),
            vault_signer_nonce: u64::from_le_bytes(data[40..48].try_into().unwrap()),
            coin_mint: data[48..80].try_into().unwrap(),
            pc_mint: data[80..112].try_into().unwrap(),
            coin_vault: data[112..144].try_into().unwrap(),
            coin_deposits_total: u64::from_le_bytes(data[144..152].try_into().unwrap()),
            coin_fees_accrued: u64::from_le_bytes(data[152..160].try_into().unwrap()),
            pc_vault: data[160..192].try_into().unwrap(),
            pc_deposits_total: u64::from_le_bytes(data[192..200].try_into().unwrap()),
            pc_fees_accrued: u64::from_le_bytes(data[200..208].try_into().unwrap()),
            pc_dust_threshold: u64::from_le_bytes(data[208..216].try_into().unwrap()),
            req_q: data[216..248].try_into().unwrap(),
            event_q: data[248..280].try_into().unwrap(),
            bids: data[280..312].try_into().unwrap(),
            asks: data[312..344].try_into().unwrap(),
            coin_lot_size: u64::from_le_bytes(data[344..352].try_into().unwrap()),
            pc_lot_size: u64::from_le_bytes(data[352..360].try_into().unwrap()),
            fee_rate_bps: u64::from_le_bytes(data[360..368].try_into().unwrap()),
            referrer_rebates_accrued: u64::from_le_bytes(data[368..376].try_into().unwrap()),
        })
    }

    pub async fn find_opportunities(&self) -> Result<Vec<ArbitrageOpportunity>> {
        let mut opportunities = Vec::new();
        let markets = self.markets.read().await;
        let current_slot = self.get_current_slot().await?;
        
        for (_, market) in markets.iter() {
            let (bids_data, asks_data) = self.fetch_orderbook_data(market).await?;
            
            let bid_levels = self.parse_orderbook(&bids_data, true)?;
            let ask_levels = self.parse_orderbook(&asks_data, false)?;
            
            if let (Some(best_bid), Some(best_ask)) = (bid_levels.first(), ask_levels.first()) {
                let spread_bps = self.calculate_spread_bps(best_bid.0, best_ask.0);
                let fee_diff = (TAKER_FEE_BPS - MAKER_FEE_BPS) as u64;
                
                if spread_bps > fee_diff + MIN_PROFIT_THRESHOLD_BPS {
                    let profit_bps = spread_bps.saturating_sub(fee_diff);
                    let size = best_bid.1.min(best_ask.1).min(self.get_max_size(market).await?);
                    
                    if size > 0 {
                        opportunities.push(ArbitrageOpportunity {
                            market: market.clone(),
                            side: OrderSide::Sell,
                            price: best_bid.0,
                            size,
                            profit_bps,
                            maker_rebate: MAKER_FEE_BPS,
                            taker_fee: TAKER_FEE_BPS,
                            timestamp: current_slot,
                        });
                    }
                }
            }
        }
        
        opportunities.sort_by(|a, b| b.profit_bps.cmp(&a.profit_bps));
        Ok(opportunities)
    }

    async fn fetch_orderbook_data(&self, market: &MarketInfo) -> Result<(Vec<u8>, Vec<u8>)> {
        let (bids_data, asks_data) = tokio::try_join!(
            self.retry_rpc(|| async { self.rpc.get_account_data(&market.bids).await }),
            self.retry_rpc(|| async { self.rpc.get_account_data(&market.asks).await })
        )?;
        
        Ok((bids_data, asks_data))
    }

    fn parse_orderbook(&self, data: &[u8], is_bids: bool) -> Result<Vec<(u64, u64)>> {
        const HEADER_SIZE: usize = 45;
        const NODE_SIZE: usize = 72;
        
        if data.len() < HEADER_SIZE {
            return Ok(Vec::new());
        }
        
        let mut levels = Vec::with_capacity(20);
        let mut offset = HEADER_SIZE;
        
        while offset + NODE_SIZE <= data.len() && levels.len() < 20 {
            let tag = u32::from_le_bytes(data[offset..offset + 4].try_into().unwrap());
            
            if tag == 1 || tag == 2 {
                let price = u64::from_le_bytes(data[offset + 24..offset + 32].try_into().unwrap());
                let quantity = u64::from_le_bytes(data[offset + 32..offset + 40].try_into().unwrap());
                
                if price > 0 && quantity > 0 {
                    levels.push((price, quantity));
                }
            }
            
            offset += NODE_SIZE;
        }
        
        if is_bids {
            levels.sort_unstable_by(|a, b| b.0.cmp(&a.0));
        } else {
            levels.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        }
        
        Ok(levels)
    }

    pub async fn execute_arbitrage(&self, opportunity: &ArbitrageOpportunity) -> Result<()> {
        if !self.validate_opportunity(opportunity).await? {
            return Err(Error::InsufficientProfit);
        }
        
        let instructions = self.build_arbitrage_instructions(opportunity).await?;
        
        let recent_blockhash = self.retry_rpc(|| async {
            self.rpc.get_latest_blockhash().await
        }).await?;
        
        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.wallet.pubkey()),
            &[&*self.wallet],
            recent_blockhash,
        );
        
        let signature = self.retry_rpc(|| async {
            self.rpc.send_and_confirm_transaction(&transaction).await
        }).await?;
        
        Ok(())
    }

        async fn build_arbitrage_instructions(&self, opportunity: &ArbitrageOpportunity) -> Result<Vec<Instruction>> {
        let mut instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNITS),
            ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS),
        ];
        
        let base_token_account = self.get_or_create_token_account(&opportunity.market.base_mint).await?;
        let quote_token_account = self.get_or_create_token_account(&opportunity.market.quote_mint).await?;
        let open_orders = self.get_or_create_open_orders(&opportunity.market.market_address).await?;
        
        let client_order_id = self.generate_client_order_id();
        
        // Maker order
        instructions.push(self.build_new_order_instruction(
            &opportunity.market,
            open_orders,
            opportunity.side,
            opportunity.price,
            opportunity.size,
            OrderType::PostOnly,
            client_order_id,
            base_token_account,
            quote_token_account,
        )?);
        
        // Taker order to capture spread
        let taker_side = match opportunity.side {
            OrderSide::Buy => OrderSide::Sell,
            OrderSide::Sell => OrderSide::Buy,
        };
        
        let taker_price = match taker_side {
            OrderSide::Buy => opportunity.price.saturating_mul(10000 + MAX_SLIPPAGE_BPS) / 10000,
            OrderSide::Sell => opportunity.price.saturating_mul(10000 - MAX_SLIPPAGE_BPS) / 10000,
        };
        
        instructions.push(self.build_new_order_instruction(
            &opportunity.market,
            open_orders,
            taker_side,
            taker_price,
            opportunity.size,
            OrderType::ImmediateOrCancel,
            self.generate_client_order_id(),
            base_token_account,
            quote_token_account,
        )?);
        
        // Cancel any remaining maker order
        instructions.push(self.build_cancel_order_instruction(
            &opportunity.market,
            open_orders,
            client_order_id,
        )?);
        
        // Settle funds
        instructions.push(self.build_settle_funds_instruction(
            &opportunity.market,
            open_orders,
            base_token_account,
            quote_token_account,
        )?);
        
        Ok(instructions)
    }

    fn build_new_order_instruction(
        &self,
        market: &MarketInfo,
        open_orders: Pubkey,
        side: OrderSide,
        price: u64,
        size: u64,
        order_type: OrderType,
        client_order_id: u64,
        base_token_account: Pubkey,
        quote_token_account: Pubkey,
    ) -> Result<Instruction> {
        let serum_program = Pubkey::from_str(SERUM_PROGRAM_ID)
            .map_err(|_| Error::InvalidProgramId)?;
        
        let max_coin_qty = size;
        let max_native_pc_qty = match side {
            OrderSide::Buy => u64::MAX,
            OrderSide::Sell => size.saturating_mul(price).saturating_mul(market.quote_lot_size) / market.base_lot_size,
        };
        
        let mut data = Vec::with_capacity(48);
        data.extend_from_slice(&10u32.to_le_bytes()); // NewOrderV3
        data.extend_from_slice(&(side as u32).to_le_bytes());
        data.extend_from_slice(&price.to_le_bytes());
        data.extend_from_slice(&max_coin_qty.to_le_bytes());
        data.extend_from_slice(&max_native_pc_qty.to_le_bytes());
        data.extend_from_slice(&1u32.to_le_bytes()); // SelfTradeBehavior::DecrementTake
        data.extend_from_slice(&(order_type as u32).to_le_bytes());
        data.extend_from_slice(&client_order_id.to_le_bytes());
        data.extend_from_slice(&u16::MAX.to_le_bytes()); // limit
        
        let payer = match side {
            OrderSide::Buy => quote_token_account,
            OrderSide::Sell => base_token_account,
        };
        
        let accounts = vec![
            AccountMeta::new(market.market_address, false),
            AccountMeta::new(open_orders, false),
            AccountMeta::new(market.request_queue, false),
            AccountMeta::new(market.event_queue, false),
            AccountMeta::new(market.bids, false),
            AccountMeta::new(market.asks, false),
            AccountMeta::new(payer, false),
            AccountMeta::new_readonly(self.wallet.pubkey(), true),
            AccountMeta::new(market.base_vault, false),
            AccountMeta::new(market.quote_vault, false),
            AccountMeta::new_readonly(spl_token::id(), false),
            AccountMeta::new_readonly(sysvar::rent::id(), false),
        ];
        
        Ok(Instruction {
            program_id: serum_program,
            accounts,
            data,
        })
    }

    fn build_cancel_order_instruction(
        &self,
        market: &MarketInfo,
        open_orders: Pubkey,
        client_order_id: u64,
    ) -> Result<Instruction> {
        let serum_program = Pubkey::from_str(SERUM_PROGRAM_ID)
            .map_err(|_| Error::InvalidProgramId)?;
        
        let mut data = Vec::with_capacity(20);
        data.extend_from_slice(&11u32.to_le_bytes()); // CancelOrderByClientIdV2
        data.extend_from_slice(&client_order_id.to_le_bytes());
        
        let accounts = vec![
            AccountMeta::new(market.market_address, false),
            AccountMeta::new(market.bids, false),
            AccountMeta::new(market.asks, false),
            AccountMeta::new(open_orders, false),
            AccountMeta::new_readonly(self.wallet.pubkey(), true),
            AccountMeta::new(market.event_queue, false),
        ];
        
        Ok(Instruction {
            program_id: serum_program,
            accounts,
            data,
        })
    }

    fn build_settle_funds_instruction(
        &self,
        market: &MarketInfo,
        open_orders: Pubkey,
        base_token_account: Pubkey,
        quote_token_account: Pubkey,
    ) -> Result<Instruction> {
        let serum_program = Pubkey::from_str(SERUM_PROGRAM_ID)
            .map_err(|_| Error::InvalidProgramId)?;
        
        let mut data = Vec::with_capacity(4);
        data.extend_from_slice(&5u32.to_le_bytes()); // SettleFunds
        
        let accounts = vec![
            AccountMeta::new(market.market_address, false),
            AccountMeta::new(open_orders, false),
            AccountMeta::new_readonly(self.wallet.pubkey(), true),
            AccountMeta::new(market.base_vault, false),
            AccountMeta::new(market.quote_vault, false),
            AccountMeta::new(base_token_account, false),
            AccountMeta::new(quote_token_account, false),
            AccountMeta::new_readonly(market.vault_signer, false),
            AccountMeta::new_readonly(spl_token::id(), false),
        ];
        
        Ok(Instruction {
            program_id: serum_program,
            accounts,
            data,
        })
    }

    async fn get_or_create_token_account(&self, mint: &Pubkey) -> Result<Pubkey> {
        let token_accounts = self.token_accounts.read().await;
        
        if let Some(&account) = token_accounts.get(mint) {
            return Ok(account);
        }
        
        drop(token_accounts);
        
        let (ata, _) = Pubkey::find_program_address(
            &[
                self.wallet.pubkey().as_ref(),
                spl_token::id().as_ref(),
                mint.as_ref(),
            ],
            &spl_associated_token_account::id(),
        );
        
        let mut token_accounts = self.token_accounts.write().await;
        token_accounts.insert(*mint, ata);
        
        Ok(ata)
    }

    async fn get_or_create_open_orders(&self, market: &Pubkey) -> Result<Pubkey> {
        let open_orders_map = self.open_orders.read().await;
        
        if let Some(&account) = open_orders_map.get(market) {
            return Ok(account);
        }
        
        drop(open_orders_map);
        
        let serum_program = Pubkey::from_str(SERUM_PROGRAM_ID)
            .map_err(|_| Error::InvalidProgramId)?;
        
        let mut seeds = Vec::with_capacity(64);
        seeds.extend_from_slice(b"open-orders");
        seeds.extend_from_slice(market.as_ref());
        seeds.extend_from_slice(self.wallet.pubkey().as_ref());
        
        let (open_orders, _) = Pubkey::find_program_address(
            &[seeds.as_slice()],
            &serum_program,
        );
        
        let mut open_orders_map = self.open_orders.write().await;
        open_orders_map.insert(*market, open_orders);
        
        Ok(open_orders)
    }

    async fn validate_opportunity(&self, opportunity: &ArbitrageOpportunity) -> Result<bool> {
        let current_slot = self.get_current_slot().await?;
        
        if current_slot > opportunity.timestamp + SLOT_BUFFER {
            return Ok(false);
        }
        
        let expected_profit = self.calculate_expected_profit(opportunity)?;
        let gas_cost = PRIORITY_FEE_LAMPORTS + 5000;
        
        if expected_profit <= gas_cost {
            return Ok(false);
        }
        
        let balance_check = self.check_token_balances(opportunity).await?;
        
        Ok(balance_check)
    }

    fn calculate_expected_profit(&self, opportunity: &ArbitrageOpportunity) -> Result<u64> {
        let notional_value = opportunity.size.saturating_mul(opportunity.price);
        
        let maker_rebate = if opportunity.maker_rebate < 0 {
            (notional_value as i128 * opportunity.maker_rebate.abs() as i128 / 10000) as u64
        } else {
            0
        };
        
        let taker_fee = (notional_value as i128 * opportunity.taker_fee as i128 / 10000) as u64;
        let spread_profit = (notional_value * opportunity.profit_bps) / 10000;
        
        Ok(spread_profit + maker_rebate - taker_fee)
    }

    async fn check_token_balances(&self, opportunity: &ArbitrageOpportunity) -> Result<bool> {
        let base_balance = self.get_token_balance(&opportunity.market.base_mint).await?;
        let quote_balance = self.get_token_balance(&opportunity.market.quote_mint).await?;
        
        let required_base = opportunity.size.saturating_mul(opportunity.market.base_lot_size);
        let required_quote = opportunity.size
            .saturating_mul(opportunity.price)
            .saturating_mul(opportunity.market.quote_lot_size)
            / opportunity.market.base_lot_size;
        
        Ok(base_balance >= required_base && quote_balance >= required_quote)
    }

    async fn get_token_balance(&self, mint: &Pubkey) -> Result<u64> {
        let token_account = self.get_or_create_token_account(mint).await?;
        
        let account_data = self.retry_rpc(|| async {
            self.rpc.get_account_data(&token_account).await
        }).await?;
        
        if account_data.len() < 165 {
            return Ok(0);
        }
        
        let amount = u64::from_le_bytes(account_data[64..72].try_into().unwrap());
        Ok(amount)
    }

    async fn get_max_size(&self, market: &MarketInfo) -> Result<u64> {
        let base_balance = self.get_token_balance(&market.base_mint).await?;
        let quote_balance = self.get_token_balance(&market.quote_mint).await?;
        
        let max_base_lots = base_balance / market.base_lot_size;
        let max_quote_value = quote_balance * market.base_lot_size / market.quote_lot_size;
        
        Ok(max_base_lots.min(max_quote_value / 2))
    }

    fn calculate_spread_bps(&self, bid: u64, ask: u64) -> u64 {
        if bid == 0 || ask <= bid {
            return 0;
        }
        
        ((ask - bid) * 10000) / bid
    }

    fn generate_client_order_id(&self) -> u64 {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        
        let random = (self.wallet.pubkey().to_bytes()[0] as u64) << 8;
        (timestamp & 0xFFFFFFFFFFFF0000) | (random & 0xFFFF)
    }

    async fn get_current_slot(&self) -> Result<u64> {
        let slot = self.retry_rpc(|| async {
            self.rpc.get_slot().await
        }).await?;
        
        let mut last_slot = self.last_slot.write().await;
        *last_slot = slot;
        
        Ok(slot)
    }

        async fn retry_rpc<F, Fut, T>(&self, f: F) -> Result<T>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = std::result::Result<T, solana_client::client_error::ClientError>>,
    {
        let mut retries = 0;
        loop {
            match f().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    retries += 1;
                    if retries >= MAX_RETRIES {
                        return Err(Error::RpcError);
                    }
                    sleep(Duration::from_millis(50 * retries as u64)).await;
                }
            }
        }
    }

    pub async fn monitor_and_execute(&self) -> Result<()> {
        let mut consecutive_errors = 0u8;
        
        loop {
            let start = std::time::Instant::now();
            
            match self.find_opportunities().await {
                Ok(opportunities) => {
                    consecutive_errors = 0;
                    
                    for opportunity in opportunities.into_iter().take(3) {
                        if opportunity.profit_bps < MIN_PROFIT_THRESHOLD_BPS {
                            continue;
                        }
                        
                        let exec_start = std::time::Instant::now();
                        
                        match self.execute_arbitrage(&opportunity).await {
                            Ok(_) => {
                                let elapsed = exec_start.elapsed();
                                if elapsed.as_millis() > 100 {
                                    // Log slow execution
                                }
                            }
                            Err(e) => {
                                match e {
                                    Error::InsufficientProfit | Error::InsufficientBalance => continue,
                                    Error::TransactionFailed => {
                                        sleep(Duration::from_millis(50)).await;
                                    }
                                    _ => {
                                        sleep(Duration::from_millis(100)).await;
                                    }
                                }
                            }
                        }
                    }
                }
                Err(_) => {
                    consecutive_errors += 1;
                    if consecutive_errors > 10 {
                        return Err(Error::TooManyErrors);
                    }
                    sleep(Duration::from_millis(100 * consecutive_errors as u64)).await;
                }
            }
            
            let elapsed = start.elapsed();
            if elapsed.as_millis() < 10 {
                sleep(Duration::from_millis(10 - elapsed.as_millis() as u64)).await;
            }
        }
    }

    pub async fn run(&self) -> Result<()> {
        let markets = vec![
            "8BnEgHoWFysVcuFFX7QztDmzuH8r5ZFvyP3sYWZzXLZs", // SOL/USDC
            "9wFFyRfZBsuAha4YYwH3KvxKGKq4qthPFhBPGAPTAvfi", // ETH/USDC
            "FZxi3yWkE5mMjYKnKRedE3cEpFq1Zhn2fxtmWnFnNVVG", // BTC/USDC
            "H6sSJmfq98M8HDPRWekUHi8Jh7RTj87bCPzSXkHN3xQE", // SRM/USDC
            "2TQztME8qsWQcMknLB5tGPHHvWjZNWjCyjJQaBm5Forf", // RAY/USDC
        ];
        
        self.initialize_markets(markets).await?;
        
        // Pre-create token accounts for known mints
        let known_mints = vec![
            "So11111111111111111111111111111111111111112", // SOL
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", // USDC
            "7vfCXTUXx5WJV5JADk17DUJ4ksgau7utNKj4b963voxs", // ETH
            "9n4nbM75f5Ui33ZbPYXn59EwSgE8CGsHtAeTH5YFeJ9E", // BTC
        ];
        
        for mint_str in known_mints {
            if let Ok(mint) = Pubkey::from_str(mint_str) {
                let _ = self.get_or_create_token_account(&mint).await;
            }
        }
        
        self.monitor_and_execute().await
    }

    pub async fn health_check(&self) -> bool {
        match self.rpc.get_health().await {
            Ok(_) => {
                let slot = match self.get_current_slot().await {
                    Ok(s) => s,
                    Err(_) => return false,
                };
                
                let last_slot = *self.last_slot.read().await;
                slot > last_slot || slot == 0
            }
            Err(_) => false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderType {
    Limit = 0,
    ImmediateOrCancel = 1,
    PostOnly = 2,
}

#[derive(Debug, Clone, Copy)]
pub enum Error {
    InvalidMarketAddress,
    InvalidProgramId,
    RpcError,
    MarketDeserializationError,
    TokenAccountDeserializationError,
    TransactionFailed,
    InsufficientProfit,
    InsufficientBalance,
    TooManyErrors,
    InvalidOrderbook,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InvalidMarketAddress => write!(f, "Invalid market address"),
            Error::InvalidProgramId => write!(f, "Invalid program ID"),
            Error::RpcError => write!(f, "RPC error"),
            Error::MarketDeserializationError => write!(f, "Market deserialization error"),
            Error::TokenAccountDeserializationError => write!(f, "Token account deserialization error"),
            Error::TransactionFailed => write!(f, "Transaction failed"),
            Error::InsufficientProfit => write!(f, "Insufficient profit"),
            Error::InsufficientBalance => write!(f, "Insufficient balance"),
            Error::TooManyErrors => write!(f, "Too many consecutive errors"),
            Error::InvalidOrderbook => write!(f, "Invalid orderbook data"),
        }
    }
}

impl std::error::Error for Error {}

impl From<Error> for ProgramError {
    fn from(e: Error) -> Self {
        match e {
            Error::InvalidMarketAddress => ProgramError::InvalidArgument,
            Error::InvalidProgramId => ProgramError::IncorrectProgramId,
            Error::RpcError => ProgramError::Custom(1),
            Error::MarketDeserializationError => ProgramError::InvalidAccountData,
            Error::TokenAccountDeserializationError => ProgramError::InvalidAccountData,
            Error::TransactionFailed => ProgramError::Custom(2),
            Error::InsufficientProfit => ProgramError::Custom(3),
            Error::InsufficientBalance => ProgramError::InsufficientFunds,
            Error::TooManyErrors => ProgramError::Custom(4),
            Error::InvalidOrderbook => ProgramError::InvalidAccountData,
        }
    }
}

type Result<T> = std::result::Result<T, Error>;

pub async fn create_arbitrageur(
    rpc_url: &str,
    wallet_path: &str,
) -> Result<MakerTakerArbitrageur> {
    let wallet_bytes = std::fs::read(wallet_path)
        .map_err(|_| Error::InvalidMarketAddress)?;
    
    let wallet = Keypair::from_bytes(&wallet_bytes)
        .map_err(|_| Error::InvalidMarketAddress)?;
    
    Ok(MakerTakerArbitrageur::new(rpc_url, wallet))
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[tokio::test]
    async fn test_spread_calculation() {
        let arbitrageur = MakerTakerArbitrageur::new(
            "https://api.mainnet-beta.solana.com",
            Keypair::new(),
        );
        
        assert_eq!(arbitrageur.calculate_spread_bps(10000, 10010), 10);
        assert_eq!(arbitrageur.calculate_spread_bps(10000, 10000), 0);
        assert_eq!(arbitrageur.calculate_spread_bps(0, 10000), 0);
    }
    
    #[test]
    fn test_client_order_id_generation() {
        let arbitrageur = MakerTakerArbitrageur::new(
            "https://api.mainnet-beta.solana.com",
            Keypair::new(),
        );
        
        let id1 = arbitrageur.generate_client_order_id();
        let id2 = arbitrageur.generate_client_order_id();
        
        assert_ne!(id1, id2);
        assert!(id1 > 0);
        assert!(id2 > 0);
    }
    
    #[tokio::test]
    async fn test_profit_calculation() {
        let arbitrageur = MakerTakerArbitrageur::new(
            "https://api.mainnet-beta.solana.com",
            Keypair::new(),
        );
        
        let opportunity = ArbitrageOpportunity {
            market: MarketInfo {
                market_address: Pubkey::new_unique(),
                base_mint: Pubkey::new_unique(),
                quote_mint: Pubkey::new_unique(),
                base_vault: Pubkey::new_unique(),
                quote_vault: Pubkey::new_unique(),
                request_queue: Pubkey::new_unique(),
                event_queue: Pubkey::new_unique(),
                bids: Pubkey::new_unique(),
                asks: Pubkey::new_unique(),
                base_lot_size: 1000,
                quote_lot_size: 1,
                tick_size: 1,
                fee_rate_bps: 5,
                vault_signer: Pubkey::new_unique(),
            },
            side: OrderSide::Sell,
            price: 10000,
            size: 100,
            profit_bps: 10,
            maker_rebate: -3,
            taker_fee: 5,
            timestamp: 0,
        };
        
        let profit = arbitrageur.calculate_expected_profit(&opportunity).unwrap();
        assert!(profit > 0);
    }
}

