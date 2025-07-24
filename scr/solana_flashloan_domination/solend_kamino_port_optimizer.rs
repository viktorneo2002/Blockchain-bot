use spl_token::solana_program::program_pack::Pack;
use std::str::FromStr;
use solana_sdk::message::Message;
use std::collections::HashSet;
use anchor_client::{
    solana_sdk::{
        commitment_config::CommitmentConfig,
        compute_budget::ComputeBudgetInstruction,
        instruction::Instruction,
        pubkey::Pubkey,
        signature::{Keypair, Signer},
        transaction::Transaction,
    },
    
};
use anyhow::{anyhow, Result};
use solana_client::rpc_client::RpcClient;
use solana_program::{
    instruction::AccountMeta,
    program_pack::Pack,
    system_instruction,
};
use solana_sdk::hash::Hash;
use spl_token::state::Account as TokenAccount;
use std::{
    collections::HashMap,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::RwLock;

const SOLEND_PROGRAM_ID: &str = "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo";
const KAMINO_PROGRAM_ID: &str = "KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD";
const PRIORITY_FEE_LAMPORTS: u64 = 50000;
const COMPUTE_UNITS: u32 = 400000;
const MIN_PROFIT_BPS: u64 = 10;
const REBALANCE_THRESHOLD_BPS: u64 = 25;
const MAX_SLIPPAGE_BPS: u64 = 50;

#[derive(Clone, Debug)]
pub struct LendingMarket {
    pub market_pubkey: Pubkey,
    pub protocol: Protocol,
    pub supply_apy: f64,
    pub borrow_apy: f64,
    pub utilization: f64,
    pub liquidity_available: u64,
    pub last_update: Instant,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Protocol {
    Solend,
    Kamino,
}

pub struct PortOptimizer {
    rpc_client: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    markets: Arc<RwLock<HashMap<String, Vec<LendingMarket>>>>,
    active_positions: Arc<RwLock<HashMap<String, Position>>>,
}

#[derive(Clone, Debug)]
pub struct Position {
    pub token_mint: Pubkey,
    pub protocol: Protocol,
    pub amount: u64,
    pub entry_apy: f64,
    pub last_rebalance: Instant,
}

impl PortOptimizer {
    pub fn new(rpc_url: &str, wallet: Keypair) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ));

        Self {
            rpc_client,
            wallet: Arc::new(wallet),
            markets: Arc::new(RwLock::new(HashMap::new())),
            active_positions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn run(&self) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        
        loop {
            interval.tick().await;
            
            if let Err(e) = self.optimize_cycle().await {
                eprintln!("Optimization cycle error: {e:?}");
            }
        }
    }

    async fn optimize_cycle(&self) -> Result<()> {
        self.update_market_data().await?;
        
        let positions = self.active_positions.read().await.clone();
        let markets = self.markets.read().await.clone();
        
        for (token_mint, position) in positions.iter() {
            if let Some(token_markets) = markets.get(token_mint) {
                if let Some(opportunity) = self.find_arbitrage_opportunity(position, token_markets).await? {
                    self.execute_rebalance(position, opportunity).await?;
                }
            }
        }
        
        Ok(())
    }

    async fn update_market_data(&self) -> Result<()> {
        let mut markets = self.markets.write().await;
        
        let solend_markets = self.fetch_solend_markets().await?;
        let kamino_markets = self.fetch_kamino_markets().await?;
        
        for market in solend_markets {
            markets.entry(market.market_pubkey.to_string())
                .or_insert_with(Vec::new)
                .push(market);
        }
        
        for market in kamino_markets {
            markets.entry(market.market_pubkey.to_string())
                .or_insert_with(Vec::new)
                .push(market);
        }
        
        Ok(())
    }

    async fn fetch_solend_markets(&self) -> Result<Vec<LendingMarket>> {
        let program_id = Pubkey::from_str(SOLEND_PROGRAM_ID)?;
        let accounts = self.rpc_client.get_program_accounts(&program_id)?;
        
        let mut markets = Vec::new();
        
        for (pubkey, account) in accounts {
            if account.data.len() >= 600 {
                let market = self.parse_solend_reserve(&pubkey, &account.data)?;
                markets.push(market);
            }
        }
        
        Ok(markets)
    }

    async fn fetch_kamino_markets(&self) -> Result<Vec<LendingMarket>> {
        let program_id = Pubkey::from_str(KAMINO_PROGRAM_ID)?;
        let accounts = self.rpc_client.get_program_accounts(&program_id)?;
        
        let mut markets = Vec::new();
        
        for (pubkey, account) in accounts {
            if account.data.len() >= 800 {
                let market = self.parse_kamino_market(&pubkey, &account.data)?;
                markets.push(market);
            }
        }
        
        Ok(markets)
    }

    fn parse_solend_reserve(&self, pubkey: &Pubkey, data: &[u8]) -> Result<LendingMarket> {
        let supply_rate = u64::from_le_bytes(data[232..240].try_into()?);
        let borrow_rate = u64::from_le_bytes(data[240..248].try_into()?);
        let available_liquidity = u64::from_le_bytes(data[64..72].try_into()?);
        let borrowed_amount = u64::from_le_bytes(data[72..80].try_into()?);
        
        let total_liquidity = available_liquidity + borrowed_amount;
        let utilization = if total_liquidity > 0 {
            borrowed_amount as f64 / total_liquidity as f64
        } else {
            0.0
        };
        
        Ok(LendingMarket {
            market_pubkey: *pubkey,
            protocol: Protocol::Solend,
            supply_apy: self.calculate_apy_from_rate(supply_rate),
            borrow_apy: self.calculate_apy_from_rate(borrow_rate),
            utilization,
            liquidity_available: available_liquidity,
            last_update: Instant::now(),
        })
    }

    fn parse_kamino_market(&self, pubkey: &Pubkey, data: &[u8]) -> Result<LendingMarket> {
        let supply_rate = u64::from_le_bytes(data[296..304].try_into()?);
        let borrow_rate = u64::from_le_bytes(data[304..312].try_into()?);
        let available_liquidity = u64::from_le_bytes(data[80..88].try_into()?);
        let borrowed_amount = u64::from_le_bytes(data[88..96].try_into()?);
        
        let total_liquidity = available_liquidity + borrowed_amount;
        let utilization = if total_liquidity > 0 {
            borrowed_amount as f64 / total_liquidity as f64
        } else {
            0.0
        };
        
        Ok(LendingMarket {
            market_pubkey: *pubkey,
            protocol: Protocol::Kamino,
            supply_apy: self.calculate_apy_from_rate(supply_rate),
            borrow_apy: self.calculate_apy_from_rate(borrow_rate),
            utilization,
            liquidity_available: available_liquidity,
            last_update: Instant::now(),
        })
    }

    fn calculate_apy_from_rate(&self, rate: u64) -> f64 {
        let rate_per_slot = rate as f64 / 1e18;
        let slots_per_year = 63072000.0;
        ((1.0 + rate_per_slot).powf(slots_per_year) - 1.0) * 100.0
    }

    async fn find_arbitrage_opportunity(
        &self,
        position: &Position,
        markets: &[LendingMarket],
    ) -> Result<Option<LendingMarket>> {
        let current_market = markets.iter()
            .find(|m| m.protocol == position.protocol)
            .ok_or_else(|| anyhow!("Current market not found"))?;
        
        let best_alternative = markets.iter()
            .filter(|m| m.protocol != position.protocol)
            .max_by(|a, b| a.supply_apy.partial_cmp(&b.supply_apy).unwrap());
        
        if let Some(alt_market) = best_alternative {
            let apy_diff_bps = ((alt_market.supply_apy - current_market.supply_apy) * 100.0) as u64;
            
            if apy_diff_bps > REBALANCE_THRESHOLD_BPS {
                let gas_cost = self.estimate_rebalance_cost(position.amount).await?;
                let expected_profit = self.calculate_expected_profit(
                    position.amount,
                    current_market.supply_apy,
                    alt_market.supply_apy,
                    gas_cost,
                );
                
                if expected_profit > MIN_PROFIT_BPS as f64 {
                    return Ok(Some(alt_market.clone()));
                }
            }
        }
        
        Ok(None)
    }

    async fn estimate_rebalance_cost(&self, amount: u64) -> Result<u64> {
        let base_fee = PRIORITY_FEE_LAMPORTS * 2;
        let size_multiplier = (amount as f64 / 1e9).max(1.0);
        Ok((base_fee as f64 * size_multiplier) as u64)
    }

    fn calculate_expected_profit(
        &self,
        amount: u64,
        current_apy: f64,
        target_apy: f64,
        gas_cost: u64,
    ) -> f64 {
        let daily_rate_diff = (target_apy - current_apy) / 365.0 / 100.0;
        let daily_profit = (amount as f64) * daily_rate_diff;
        let gas_cost_tokens = gas_cost as f64 / 1e9;
        (daily_profit - gas_cost_tokens) / (amount as f64) * 10000.0
    }

    async fn execute_rebalance(
        &self,
        position: &Position,
        target_market: LendingMarket,
    ) -> Result<()> {
        let withdraw_ix = self.build_withdraw_instruction(position).await?;
        let deposit_ix = self.build_deposit_instruction(position, &target_market).await?;
        
        let recent_blockhash = self.get_recent_blockhash_with_retry().await?;
        
        let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNITS);
        let priority_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS);
        
        let mut transaction = Transaction::new_with_payer(
            &[compute_budget_ix, priority_fee_ix, withdraw_ix, deposit_ix],
            Some(&self.wallet.pubkey()),
        );
        
        transaction.sign(&[self.wallet.as_ref()], recent_blockhash);
        
        match self.send_transaction_with_retry(&transaction).await {
            Ok(signature) => {
                println!("Rebalance executed: {signature}");
                self.update_position(position, target_market).await?;
            }
            Err(e) => {
                eprintln!("Rebalance failed: {e:?}");
                return Err(e);
            }
        }
        
        Ok(())
    }

    async fn build_withdraw_instruction(&self, position: &Position) -> Result<Instruction> {
        let program_id = match position.protocol {
            Protocol::Solend => Pubkey::from_str(SOLEND_PROGRAM_ID)?,
            Protocol::Kamino => Pubkey::from_str(KAMINO_PROGRAM_ID)?,
        };
        
        let instruction_data = match position.protocol {
            Protocol::Solend => self.encode_solend_withdraw(position.amount),
            Protocol::Kamino => self.encode_kamino_withdraw(position.amount),
        };
        
        let accounts = self.get_withdraw_accounts(position).await?;
        
        Ok(Instruction {
            program_id,
            accounts,
            data: instruction_data,
        })
    }

    async fn build_deposit_instruction(
        &self,
        position: &Position,
        target_market: &LendingMarket,
    ) -> Result<Instruction> {
        let program_id = match target_market.protocol {
            Protocol::Solend => Pubkey::from_str(SOLEND_PROGRAM_ID)?,
            Protocol::Kamino => Pubkey::from_str(KAMINO_PROGRAM_ID)?,
        };
        
        let instruction_data = match target_market.protocol {
            Protocol::Solend => self.encode_solend_deposit(position.amount),
            Protocol::Kamino => self.encode_kamino_deposit(position.amount),
        };
        
        let accounts = self.get_deposit_accounts(position, target_market).await?;
        
        Ok(Instruction {
            program_id,
            accounts,
            data: instruction_data,
        })
    }

    fn encode_solend_withdraw(&self, amount: u64) -> Vec<u8> {
        let mut data = vec![5u8]; // Solend withdraw instruction discriminator
        data.extend_from_slice(&amount.to_le_bytes());
        data
    }

    fn encode_kamino_withdraw(&self, amount: u64) -> Vec<u8> {
        let mut data = vec![183, 18, 70, 156, 148, 109, 161, 34]; // Kamino withdraw hash
        data.extend_from_slice(&amount.to_le_bytes());
        data
    }

    fn encode_solend_deposit(&self, amount: u64) -> Vec<u8> {
        let mut data = vec![4u8]; // Solend deposit instruction discriminator
        data.extend_from_slice(&amount.to_le_bytes());
        data
    }

    fn encode_kamino_deposit(&self, amount: u64) -> Vec<u8> {
        let mut data = vec![242, 35, 198, 137, 82, 225, 242, 182]; // Kamino deposit hash
        data.extend_from_slice(&amount.to_le_bytes());
        data
    }

    async fn get_withdraw_accounts(&self, position: &Position) -> Result<Vec<AccountMeta>> {
        let user_pubkey = self.wallet.pubkey();
        
        match position.protocol {
            Protocol::Solend => {
                let (reserve_liquidity_supply, _) = Pubkey::find_program_address(
                    &[b"liquidity_supply", position.token_mint.as_ref()],
                    &Pubkey::from_str(SOLEND_PROGRAM_ID)?,
                );
                
                let (user_collateral, _) = Pubkey::find_program_address(
                    &[b"user_collateral", user_pubkey.as_ref(), position.token_mint.as_ref()],
                    &Pubkey::from_str(SOLEND_PROGRAM_ID)?,
                );
                
                Ok(vec![
                    AccountMeta::new(position.token_mint, false),
                    AccountMeta::new(reserve_liquidity_supply, false),
                    AccountMeta::new(user_collateral, false),
                    AccountMeta::new(user_pubkey, true),
                    AccountMeta::new_readonly(solana_sdk::pubkey::Pubkey::from_str(&spl_token::id().to_string()).unwrap(), false),
                ])
            }
            Protocol::Kamino => {
                let (market_authority, _) = Pubkey::find_program_address(
                    &[b"market_authority", position.token_mint.as_ref()],
                    &Pubkey::from_str(KAMINO_PROGRAM_ID)?,
                );
                
                let (reserve_vault, _) = Pubkey::find_program_address(
                    &[b"reserve_vault", position.token_mint.as_ref()],
                    &Pubkey::from_str(KAMINO_PROGRAM_ID)?,
                );
                
                Ok(vec![
                    AccountMeta::new(position.token_mint, false),
                    AccountMeta::new(market_authority, false),
                    AccountMeta::new(reserve_vault, false),
                    AccountMeta::new(user_pubkey, true),
                    AccountMeta::new_readonly(solana_sdk::pubkey::Pubkey::from_str(&spl_token::id().to_string()).unwrap(), false),
                ])
            }
        }
    }

    async fn get_deposit_accounts(
        &self,
        position: &Position,
        target_market: &LendingMarket,
    ) -> Result<Vec<AccountMeta>> {
        let user_pubkey = self.wallet.pubkey();
        
        match target_market.protocol {
            Protocol::Solend => {
                let (reserve_liquidity_supply, _) = Pubkey::find_program_address(
                    &[b"liquidity_supply", position.token_mint.as_ref()],
                    &Pubkey::from_str(SOLEND_PROGRAM_ID)?,
                );
                
                let (user_collateral, _) = Pubkey::find_program_address(
                    &[b"user_collateral", user_pubkey.as_ref(), position.token_mint.as_ref()],
                    &Pubkey::from_str(SOLEND_PROGRAM_ID)?,
                );
                
                Ok(vec![
                    AccountMeta::new(position.token_mint, false),
                    AccountMeta::new(reserve_liquidity_supply, false),
                    AccountMeta::new(user_collateral, false),
                    AccountMeta::new(user_pubkey, true),
                    AccountMeta::new_readonly(solana_sdk::pubkey::Pubkey::from_str(&spl_token::id().to_string()).unwrap(), false),
                ])
            }
            Protocol::Kamino => {
                let (market_authority, _) = Pubkey::find_program_address(
                    &[b"market_authority", position.token_mint.as_ref()],
                    &Pubkey::from_str(KAMINO_PROGRAM_ID)?,
                );
                
                let (reserve_vault, _) = Pubkey::find_program_address(
                    &[b"reserve_vault", position.token_mint.as_ref()],
                    &Pubkey::from_str(KAMINO_PROGRAM_ID)?,
                );
                
                Ok(vec![
                    AccountMeta::new(position.token_mint, false),
                    AccountMeta::new(market_authority, false),
                    AccountMeta::new(reserve_vault, false),
                    AccountMeta::new(user_pubkey, true),
                    AccountMeta::new_readonly(solana_sdk::pubkey::Pubkey::from_str(&spl_token::id().to_string()).unwrap(), false),
                ])
            }
        }
    }

    async fn get_recent_blockhash_with_retry(&self) -> Result<Hash> {
        let mut retries = 3;
        let mut last_error = None;
        
        while retries > 0 {
            match self.rpc_client.get_latest_blockhash() {
                Ok(blockhash) => return Ok(blockhash),
                Err(e) => {
                    last_error = Some(e);
                    retries -= 1;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
        
        Err(anyhow!("Failed to get blockhash: {:?}", last_error))
    }

    async fn send_transaction_with_retry(&self, transaction: &Transaction) -> Result<String> {
        let mut retries = 5;
        let mut last_error = None;
        
        while retries > 0 {
            match self.rpc_client.send_transaction(transaction) {
                Ok(signature) => {
                    if self.confirm_transaction(&signature).await? {
                        return Ok(signature.to_string());
                    }
                }
                Err(e) => {
                    if e.to_string().contains("AlreadyProcessed") {
                        return Ok(transaction.signatures[0].to_string());
                    }
                    last_error = Some(e);
                }
            }
            
            retries -= 1;
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        
        Err(anyhow!("Failed to send transaction: {:?}", last_error))
    }

    async fn confirm_transaction(&self, signature: &solana_sdk::signature::Signature) -> Result<bool> {
        let start = Instant::now();
        let timeout = Duration::from_secs(30);
        
        while start.elapsed() < timeout {
            match self.rpc_client.get_signature_status(signature)? {
                Some(status) => {
                    if status.is_ok() {
                        return Ok(true);
                    } else if status.is_err() {
                        return Ok(false);
                    }
                }
                None => {
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            }
        }
        
        Ok(false)
    }

    async fn update_position(
        &self,
        old_position: &Position,
        new_market: LendingMarket,
    ) -> Result<()> {
        let mut positions = self.active_positions.write().await;
        
        positions.insert(
            old_position.token_mint.to_string(),
            Position {
                token_mint: old_position.token_mint,
                protocol: new_market.protocol,
                amount: old_position.amount,
                entry_apy: new_market.supply_apy,
                last_rebalance: Instant::now(),
            },
        );
        
        Ok(())
    }

    pub async fn initialize_positions(&self, token_mints: Vec<Pubkey>) -> Result<()> {
        let mut positions = self.active_positions.write().await;
        
        for mint in token_mints {
            let balance = self.get_token_balance(&mint).await?;
            
            if balance > 0 {
                let best_market = self.find_best_market(&mint).await?;
                
                positions.insert(
                    mint.to_string(),
                    Position {
                        token_mint: mint,
                        protocol: best_market.protocol.clone(),
                        amount: balance,
                        entry_apy: best_market.supply_apy,
                        last_rebalance: Instant::now(),
                    },
                );
                
                self.execute_initial_deposit(&mint, &best_market, balance).await?;
            }
        }
        
        Ok(())
    }

    async fn get_token_balance(&self, mint: &Pubkey) -> Result<u64> {
        let token_account = spl_associated_token_account::get_associated_token_address(
            &self.wallet.pubkey(),
            mint,
        );
        
        match self.rpc_client.get_account(&token_account) {
            Ok(account) => {
                let token_account_data = TokenAccount::unpack(&account.data)?;
                Ok(token_account_data.amount)
            }
            Err(_) => Ok(0),
        }
    }

    async fn find_best_market(&self, _mint: &Pubkey) -> Result<LendingMarket> {
        self.update_market_data().await?;
        
        let markets = self.markets.read().await;
        let token_markets = markets.values()
            .flatten()
            .filter(|m| m.liquidity_available > 0)
            .max_by(|a, b| a.supply_apy.partial_cmp(&b.supply_apy).unwrap())
            .ok_or_else(|| anyhow!("No available markets for token"))?;
        
        Ok(token_markets.clone())
    }

    async fn execute_initial_deposit(
        &self,
        mint: &Pubkey,
        market: &LendingMarket,
        amount: u64,
    ) -> Result<()> {
        let deposit_ix = self.build_deposit_instruction(
            &Position {
                token_mint: *mint,
                protocol: market.protocol.clone(),
                amount,
                entry_apy: market.supply_apy,
                last_rebalance: Instant::now(),
            },
            market,
        ).await?;
        
        let recent_blockhash = self.get_recent_blockhash_with_retry().await?;
        
        let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNITS);
        let priority_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS);
        
        let mut transaction = Transaction::new_with_payer(
            &[compute_budget_ix, priority_fee_ix, deposit_ix],
            Some(&self.wallet.pubkey()),
        );
        
        transaction.sign(&[self.wallet.as_ref()], recent_blockhash);
        
        self.send_transaction_with_retry(&transaction).await?;
        
        Ok(())
    }

    pub async fn emergency_withdraw_all(&self) -> Result<()> {
        let positions = self.active_positions.read().await.clone();
        
        for (_, position) in positions {
            match self.emergency_withdraw(&position).await {
                Ok(_) => println!("Emergency withdraw successful for {}", position.token_mint),
                Err(e) => eprintln!("Emergency withdraw failed for {}: {:?}", position.token_mint, e),
            }
        }
        
        Ok(())
    }

    async fn emergency_withdraw(&self, position: &Position) -> Result<()> {
        let withdraw_ix = self.build_withdraw_instruction(position).await?;
        let recent_blockhash = self.get_recent_blockhash_with_retry().await?;
        
        let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNITS * 2);
        let priority_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(PRIORITY_FEE_LAMPORTS * 5);
        
        let mut transaction = Transaction::new_with_payer(
            &[compute_budget_ix, priority_fee_ix, withdraw_ix],
            Some(&self.wallet.pubkey()),
        );
        
        transaction.sign(&[self.wallet.as_ref()], recent_blockhash);
        
        self.send_transaction_with_retry(&transaction).await?;
        
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct OptimizationMetrics {
    pub total_rebalances: u64,
    pub successful_rebalances: u64,
    pub failed_rebalances: u64,
    pub total_profit_bps: f64,
    pub gas_spent: u64,
}

impl PortOptimizer {
    pub fn with_metrics(self) -> Self {
        self
    }

    pub async fn get_optimization_metrics(&self) -> OptimizationMetrics {
        OptimizationMetrics {
            total_rebalances: 0,
            successful_rebalances: 0,
            failed_rebalances: 0,
            total_profit_bps: 0.0,
            gas_spent: 0,
        }
    }

    async fn preflight_simulation(&self, transaction: &Transaction) -> Result<bool> {
        match self.rpc_client.simulate_transaction(transaction) {
            Ok(result) => {
                if result.value.err.is_none() {
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Err(_) => Ok(false),
        }
    }

    pub async fn monitor_health(&self) -> Result<()> {
        let positions = self.active_positions.read().await;
        
        for (token, position) in positions.iter() {
            let elapsed = position.last_rebalance.elapsed();
            if elapsed > Duration::from_secs(24 * 60 * 60) {
                eprintln!("Warning: Position {token} hasn't been rebalanced in 24h");
            }
        }
        
        Ok(())
    }

    async fn calculate_network_congestion(&self) -> Result<f64> {
        let recent_fees = self.rpc_client.get_recent_prioritization_fees(&[])?;
        let avg_fee = recent_fees.iter().map(|f| f.prioritization_fee).sum::<u64>() as f64 
            / recent_fees.len().max(1) as f64;
        
        Ok(avg_fee / 1000.0)
    }

    async fn dynamic_priority_fee(&self) -> Result<u64> {
        let congestion = self.calculate_network_congestion().await?;
        let base_fee = PRIORITY_FEE_LAMPORTS;
        
        let multiplier = if congestion > 100.0 {
            3.0
        } else if congestion > 50.0 {
            2.0
        } else {
            1.0
        };
        
        Ok((base_fee as f64 * multiplier) as u64)
    }

    pub async fn validate_market_data(&self, market: &LendingMarket) -> bool {
        if market.supply_apy > 1000.0 || market.supply_apy < 0.0 {
            return false;
        }
        
        if market.utilization > 1.0 || market.utilization < 0.0 {
            return false;
        }
        
        if market.last_update.elapsed() > Duration::from_secs(300) {
            return false;
        }
        
        true
    }

    async fn get_jito_tip(&self) -> u64 {
        let base_tip = 10000;
        let congestion = self.calculate_network_congestion().await.unwrap_or(1.0);
        (base_tip as f64 * (1.0 + congestion / 100.0)) as u64
    }

    pub async fn build_jito_bundle(
        &self,
        transactions: Vec<Transaction>,
    ) -> Result<Vec<Transaction>> {
        let tip_amount = self.get_jito_tip().await;
        let tip_accounts = vec![
            Pubkey::from_str("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5")?,
            Pubkey::from_str("HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe")?,
            Pubkey::from_str("Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY")?,
            Pubkey::from_str("ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49")?,
            Pubkey::from_str("DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh")?,
            Pubkey::from_str("ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt")?,
            Pubkey::from_str("DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL")?,
            Pubkey::from_str("3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT")?,
        ];
        
        let random_index = (self.rpc_client.get_slot()? % 8) as usize;
        let tip_account = tip_accounts[random_index];
        
        let tip_ix = system_instruction::transfer(
            &self.wallet.pubkey(),
            &tip_account,
            tip_amount,
        );
        
        let mut bundle = Vec::new();
        for tx in transactions {
            let mut new_instructions = vec![tip_ix.clone()];
            new_instructions.extend(tx.message.instructions.clone().clone().into_iter().map(|ix| {
                Instruction {
                    program_id: tx.message.account_keys[ix.program_id_index as usize],
                    accounts: ix.accounts.into_iter().map(|acc_idx| {
                        AccountMeta {
                            pubkey: tx.message.account_keys[acc_idx as usize],
                            is_signer: tx.message.is_signer(acc_idx as usize),
                            is_writable: tx.message.is_writable(acc_idx as usize),
                        }
                    }).collect(),
                    data: ix.data,
                }
            }));

            // Collect all unique signers
            let mut signers = HashSet::new();
            signers.insert(self.wallet.pubkey());
            
            for account in tx.message.account_keys.iter() {
                if tx.message.is_signer(tx.message.account_keys.iter().position(|a| a == account).unwrap()) {
                    signers.insert(*account);
                }
            }

            // Build new transaction with tip instruction prepended
            let message = Message::new(&new_instructions, Some(&self.wallet.pubkey()));
            let new_tx = Transaction::new_unsigned(message);
            bundle.push(new_tx);
        }

        Ok(bundle)
    }


    pub async fn optimize_compute_units(&self, transaction: &Transaction) -> u32 {
        match self.rpc_client.simulate_transaction(transaction) {
            Ok(result) => {
                if let Some(units) = result.value.units_consumed {
                    (units as f64 * 1.2) as u32
                } else {
                    COMPUTE_UNITS
                }
            }
            Err(_) => COMPUTE_UNITS,
        }
    }

    async fn check_slippage(&self, position: &Position, market: &LendingMarket) -> Result<bool> {
        let expected_amount = position.amount;
        let max_slippage = (expected_amount as f64 * MAX_SLIPPAGE_BPS as f64) / 10000.0;
        
        let available = market.liquidity_available as f64;
        if available < expected_amount as f64 - max_slippage {
            return Ok(false);
        }
        
        Ok(true)
    }

    pub async fn start_with_config(
        rpc_url: &str,
        _ws_url: &str,
        keypair_path: &str,
        token_mints: Vec<String>,
    ) -> Result<()> {
        let wallet = Keypair::from_bytes(&std::fs::read(keypair_path)?)?;
        let optimizer = Self::new(rpc_url, wallet);
        
        let mints: Vec<Pubkey> = token_mints
            .iter()
            .map(|s| Pubkey::from_str(s))
            .collect::<Result<Vec<_>, _>>()?;
        
        optimizer.initialize_positions(mints).await?;
        
        let _monitor_handle = {
            let opt = optimizer.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(60));
                loop {
                    interval.tick().await;
                    if let Err(e) = opt.monitor_health().await {
                        eprintln!("Health monitor error: {e:?}");
                    }
                }
            })
        };
        
        tokio::select! {
            result = optimizer.run() => {
                if let Err(e) = result {
                    eprintln!("Optimizer error: {e:?}");
                }
            }
            _ = tokio::signal::ctrl_c() => {
                println!("Shutting down...");
                optimizer.emergency_withdraw_all().await?;
            }
        }
        
        Ok(())
    }
}

impl Clone for PortOptimizer {
    fn clone(&self) -> Self {
        Self {
            rpc_client: self.rpc_client.clone(),
            wallet: self.wallet.clone(),
            markets: self.markets.clone(),
            active_positions: self.active_positions.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_apy_calculation() {
        let optimizer = PortOptimizer::new("https://api.mainnet-beta.solana.com", Keypair::new());
        let rate = 1000000000000000u64;
        let apy = optimizer.calculate_apy_from_rate(rate);
        assert!(apy > 0.0 && apy < 100.0);
    }

    #[tokio::test]
    async fn test_profit_calculation() {
        let optimizer = PortOptimizer::new("https://api.mainnet-beta.solana.com", Keypair::new());
        let profit = optimizer.calculate_expected_profit(
            1000000000,
            5.0,
            7.0,
            50000,
        );
        assert!(profit > 0.0);
    }
}
