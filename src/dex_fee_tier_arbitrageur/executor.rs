use anyhow::{anyhow, Result};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    instruction::{AccountMeta, Instruction},
    message::Message,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    system_instruction,
    transaction::Transaction,
};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};

use crate::config::{Config, ORCA_WHIRLPOOL_PROGRAM, RAYDIUM_V4_PROGRAM, TOKEN_PROGRAM_ID, JITO_TIP_PROGRAM};
use crate::pool_cache::PoolCache;
use crate::rpc_pool::RpcPool;
use crate::scanner::ArbitrageOpportunity;
use crate::simulator::calculate_min_amount_out;

/// Transaction executor with simulate-before-send
pub struct Executor {
    config: Arc<Config>,
    rpc_pool: Arc<RpcPool>,
    pool_cache: Arc<PoolCache>,
    wallet: Arc<Keypair>,
    opportunity_rx: mpsc::Receiver<ArbitrageOpportunity>,
    execution_stats: Arc<RwLock<ExecutionStats>>,
}

#[derive(Debug, Default)]
pub struct ExecutionStats {
    pub opportunities_received: u64,
    pub simulations_attempted: u64,
    pub simulations_succeeded: u64,
    pub transactions_sent: u64,
    pub transactions_confirmed: u64,
    pub total_profit_captured: i128,
    pub total_gas_spent: u128,
}

impl Executor {
    pub fn new(
        config: Arc<Config>,
        rpc_pool: Arc<RpcPool>,
        pool_cache: Arc<PoolCache>,
        wallet: Arc<Keypair>,
        opportunity_rx: mpsc::Receiver<ArbitrageOpportunity>,
    ) -> Self {
        Self {
            config,
            rpc_pool,
            pool_cache,
            wallet,
            opportunity_rx,
            execution_stats: Arc::new(RwLock::new(ExecutionStats::default())),
        }
    }
    
    /// Main execution loop
    pub async fn run(mut self) -> Result<()> {
        info!("Starting transaction executor");
        
        while let Some(opportunity) = self.opportunity_rx.recv().await {
            tokio::spawn({
                let executor = self.clone_for_spawn();
                async move {
                    if let Err(e) = executor.execute_opportunity(opportunity).await {
                        warn!("Failed to execute opportunity: {}", e);
                    }
                }
            });
        }
        
        Ok(())
    }
    
    /// Clone executor for spawning (without receiver)
    fn clone_for_spawn(&self) -> ExecutorHandle {
        ExecutorHandle {
            config: self.config.clone(),
            rpc_pool: self.rpc_pool.clone(),
            pool_cache: self.pool_cache.clone(),
            wallet: self.wallet.clone(),
            execution_stats: self.execution_stats.clone(),
        }
    }
    
    /// Get execution statistics
    pub async fn get_stats(&self) -> ExecutionStats {
        self.execution_stats.read().await.clone()
    }
}

/// Handle for executing opportunities
struct ExecutorHandle {
    config: Arc<Config>,
    rpc_pool: Arc<RpcPool>,
    pool_cache: Arc<PoolCache>,
    wallet: Arc<Keypair>,
    execution_stats: Arc<RwLock<ExecutionStats>>,
}

impl ExecutorHandle {
    /// Execute a single arbitrage opportunity
    async fn execute_opportunity(&self, opportunity: ArbitrageOpportunity) -> Result<()> {
        info!("Executing opportunity: {} expected_profit={}", opportunity.id, opportunity.estimated_profit);
        
        // Update stats
        {
            let mut stats = self.execution_stats.write().await;
            stats.opportunities_received += 1;
        }
        
        // 1. Last-state check: verify pools haven't changed significantly
        self.verify_pool_states(&opportunity).await?;
        
        // 2. Build atomic bundle transaction
        let transaction = self.build_arbitrage_transaction(&opportunity).await?;
        
        // 3. Simulate transaction
        let simulation_result = self.simulate_transaction(&transaction).await?;
        
        // Update stats
        {
            let mut stats = self.execution_stats.write().await;
            stats.simulations_attempted += 1;
            if simulation_result.is_ok() {
                stats.simulations_succeeded += 1;
            }
        }
        
        // 4. Check simulation results
        if let Err(e) = simulation_result {
            warn!("Simulation failed for {}: {}", opportunity.id, e);
            return Err(e);
        }
        
        // 5. Send transaction
        let signature = self.send_transaction(transaction).await?;
        
        info!("Sent transaction for opportunity {}: {}", opportunity.id, signature);
        
        // Update stats
        {
            let mut stats = self.execution_stats.write().await;
            stats.transactions_sent += 1;
        }
        
        // 6. Monitor confirmation
        tokio::spawn({
            let rpc_pool = self.rpc_pool.clone();
            let stats = self.execution_stats.clone();
            let profit = opportunity.estimated_profit;
            async move {
                if let Ok(confirmed) = Self::wait_for_confirmation(&rpc_pool, &signature).await {
                    if confirmed {
                        let mut stats = stats.write().await;
                        stats.transactions_confirmed += 1;
                        if profit > 0 {
                            stats.total_profit_captured += profit;
                        }
                        info!("Transaction confirmed: {}", signature);
                    }
                }
            }
        });
        
        Ok(())
    }
    
    /// Verify pool states haven't changed significantly
    async fn verify_pool_states(&self, opportunity: &ArbitrageOpportunity) -> Result<()> {
        // Get fresh pool states
        let fresh_pool_a = self.pool_cache.get(&opportunity.pool_a.address);
        let fresh_pool_b = self.pool_cache.get(&opportunity.pool_b.address);
        
        if fresh_pool_a.is_none() || fresh_pool_b.is_none() {
            return Err(anyhow!("Pools no longer in cache"));
        }
        
        let pool_a = fresh_pool_a.unwrap();
        let pool_b = fresh_pool_b.unwrap();
        
        // Check if pools are stale
        if pool_a.is_stale(self.config.max_pool_age_ms) || 
           pool_b.is_stale(self.config.max_pool_age_ms) {
            return Err(anyhow!("Pool states are stale"));
        }
        
        // Check if liquidity changed significantly (>5%)
        let liquidity_change_a = ((pool_a.calculate_value() as f64 - opportunity.pool_a.calculate_value() as f64).abs() 
            / opportunity.pool_a.calculate_value() as f64) * 100.0;
        let liquidity_change_b = ((pool_b.calculate_value() as f64 - opportunity.pool_b.calculate_value() as f64).abs() 
            / opportunity.pool_b.calculate_value() as f64) * 100.0;
        
        if liquidity_change_a > 5.0 || liquidity_change_b > 5.0 {
            return Err(anyhow!("Pool liquidity changed significantly"));
        }
        
        Ok(())
    }
    
    /// Build atomic arbitrage transaction
    async fn build_arbitrage_transaction(&self, opportunity: &ArbitrageOpportunity) -> Result<Transaction> {
        let mut instructions = Vec::new();
        
        // 1. Set compute budget
        instructions.push(ComputeBudgetInstruction::set_compute_unit_limit(
            self.config.estimated_compute_units
        ));
        
        instructions.push(ComputeBudgetInstruction::set_compute_unit_price(
            self.config.priority_fee_lamports
        ));
        
        // 2. Build swap instructions
        let swap1_ix = self.build_swap_instruction(
            &opportunity.pool_a,
            opportunity.token_in,
            opportunity.token_intermediate,
            opportunity.amount_in,
            opportunity.expected_intermediate,
        )?;
        instructions.push(swap1_ix);
        
        let swap2_ix = self.build_swap_instruction(
            &opportunity.pool_b,
            opportunity.token_intermediate,
            opportunity.token_out,
            opportunity.expected_intermediate,
            opportunity.expected_out,
        )?;
        instructions.push(swap2_ix);
        
        // 3. Add Jito tip if configured
        if self.config.jito_tip_lamports > 0 {
            let tip_accounts = self.get_jito_tip_accounts();
            let tip_ix = system_instruction::transfer(
                &self.wallet.pubkey(),
                &tip_accounts[0], // Use first tip account
                self.config.jito_tip_lamports,
            );
            instructions.push(tip_ix);
        }
        
        // Build message and transaction
        let recent_blockhash = self.rpc_pool.get_latest_blockhash().await?;
        let message = Message::new(&instructions, Some(&self.wallet.pubkey()));
        let transaction = Transaction::new(&[self.wallet.as_ref()], message, recent_blockhash);
        
        Ok(transaction)
    }
    
    /// Build swap instruction (simplified - use SDK in production)
    fn build_swap_instruction(
        &self,
        pool: &Arc<crate::pool_cache::PoolState>,
        token_in: Pubkey,
        token_out: Pubkey,
        amount_in: u128,
        min_amount_out: u128,
    ) -> Result<Instruction> {
        // Calculate minimum with slippage
        let min_out_with_slippage = calculate_min_amount_out(
            min_amount_out,
            self.config.max_slippage_bps,
        );
        
        match pool.protocol {
            crate::pool_cache::Protocol::OrcaWhirlpool => {
                // Build Orca swap instruction
                // In production, use orca-so/whirlpools SDK
                Ok(Instruction {
                    program_id: ORCA_WHIRLPOOL_PROGRAM,
                    accounts: vec![
                        AccountMeta::new_readonly(TOKEN_PROGRAM_ID, false),
                        AccountMeta::new_readonly(self.wallet.pubkey(), true),
                        AccountMeta::new(pool.address, false),
                        AccountMeta::new(pool.token_vault_a, false),
                        AccountMeta::new(pool.token_vault_b, false),
                        // Add tick arrays, oracle, etc.
                    ],
                    data: self.encode_orca_swap_data(amount_in as u64, min_out_with_slippage as u64, token_in == pool.token_a),
                })
            }
            crate::pool_cache::Protocol::RaydiumV4 => {
                // Build Raydium swap instruction
                // In production, use raydium-io/raydium-sdk
                Ok(Instruction {
                    program_id: RAYDIUM_V4_PROGRAM,
                    accounts: vec![
                        AccountMeta::new_readonly(TOKEN_PROGRAM_ID, false),
                        AccountMeta::new(pool.address, false),
                        AccountMeta::new_readonly(self.wallet.pubkey(), true),
                        AccountMeta::new(pool.open_orders.unwrap_or_default(), false),
                        AccountMeta::new(pool.coin_vault, false),
                        AccountMeta::new(pool.pc_vault, false),
                        AccountMeta::new(pool.market.unwrap_or_default(), false),
                        // Add user token accounts, etc.
                    ],
                    data: self.encode_raydium_swap_data(amount_in as u64, min_out_with_slippage as u64),
                })
            }
        }
    }
    
    /// Encode Orca swap instruction data (simplified)
    fn encode_orca_swap_data(&self, amount: u64, min_amount_out: u64, a_to_b: bool) -> Vec<u8> {
        let mut data = Vec::new();
        // Instruction discriminator for swap
        data.extend_from_slice(&[0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8]); // swap
        data.extend_from_slice(&amount.to_le_bytes());
        data.extend_from_slice(&min_amount_out.to_le_bytes());
        data.push(if a_to_b { 1 } else { 0 });
        data
    }
    
    /// Encode Raydium swap instruction data (simplified)
    fn encode_raydium_swap_data(&self, amount_in: u64, min_amount_out: u64) -> Vec<u8> {
        let mut data = Vec::new();
        // Instruction discriminator for swap
        data.push(9); // SwapBaseIn instruction
        data.extend_from_slice(&amount_in.to_le_bytes());
        data.extend_from_slice(&min_amount_out.to_le_bytes());
        data
    }
    
    /// Get Jito tip accounts
    fn get_jito_tip_accounts(&self) -> Vec<Pubkey> {
        // Mainnet Jito tip accounts (rotate through them)
        vec![
            "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5".parse().unwrap(),
            "HFqU5x63VTqvQss8hp11i4wVV8bD44PvwucfZ2bU7gRe".parse().unwrap(),
            "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY".parse().unwrap(),
            "ADaUMid9yfUytqMBgopwjb2DTLSokTSzL1zt6iGPaS49".parse().unwrap(),
            "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh".parse().unwrap(),
            "ADuUkR4vqLUMWXxW9gh6D6L8pMSawimctcNZ5pGwDcEt".parse().unwrap(),
            "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAumKUiL2KRL".parse().unwrap(),
            "3AVi9Tg9Uo68tJfuvoKvqKNWKkC5wPdSSdeBnizKZ6jT".parse().unwrap(),
        ]
    }
    
    /// Simulate transaction using RPC
    async fn simulate_transaction(&self, transaction: &Transaction) -> Result<()> {
        let rpc = self.rpc_pool.get_client().await;
        
        let result = rpc.simulate_transaction(transaction).await?;
        
        if let Some(err) = result.value.err {
            return Err(anyhow!("Simulation failed: {:?}", err));
        }
        
        if let Some(logs) = result.value.logs {
            for log in logs {
                if log.contains("failed") || log.contains("error") {
                    return Err(anyhow!("Simulation logs indicate failure: {}", log));
                }
            }
        }
        
        Ok(())
    }
    
    /// Send transaction
    async fn send_transaction(&self, transaction: Transaction) -> Result<Signature> {
        let rpc = self.rpc_pool.get_client().await;
        
        let signature = rpc.send_transaction(&transaction).await?;
        
        Ok(signature)
    }
    
    /// Wait for transaction confirmation
    async fn wait_for_confirmation(rpc_pool: &RpcPool, signature: &Signature) -> Result<bool> {
        let rpc = rpc_pool.get_client().await;
        
        let timeout = std::time::Duration::from_secs(30);
        let start = std::time::Instant::now();
        
        loop {
            if start.elapsed() > timeout {
                return Ok(false);
            }
            
            match rpc.get_signature_statuses(&[*signature]).await {
                Ok(response) => {
                    if let Some(status) = response.value[0].as_ref() {
                        if status.err.is_some() {
                            return Ok(false);
                        }
                        if status.confirmation_status.is_some() {
                            return Ok(true);
                        }
                    }
                }
                Err(e) => {
                    warn!("Error checking confirmation: {}", e);
                }
            }
            
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_stats_tracking() {
        let stats = ExecutionStats::default();
        assert_eq!(stats.opportunities_received, 0);
        assert_eq!(stats.transactions_sent, 0);
        assert_eq!(stats.total_profit_captured, 0);
    }
}
