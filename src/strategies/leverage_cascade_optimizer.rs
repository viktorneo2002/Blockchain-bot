use anchor_lang::prelude::*;
use anchor_lang::solana_program::{
    instruction::Instruction,
    pubkey::Pubkey,
    system_program,
    sysvar,
};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcSimulateTransactionConfig, RpcTransactionConfig},
};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::Transaction,
};
use std::{
    collections::{BinaryHeap, HashMap, HashSet},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, Mutex, Semaphore};
use borsh::{BorshDeserialize, BorshSerialize};
use rust_decimal::prelude::*;
use serde::{Deserialize, Serialize};

const MAX_LIQUIDATION_SIZE: u64 = 10_000_000_000;
const MIN_PROFIT_THRESHOLD: u64 = 1_000_000;
const COMPUTE_UNIT_LIMIT: u32 = 1_400_000;
const COMPUTE_UNIT_PRICE: u64 = 50_000;
const MAX_CASCADE_DEPTH: usize = 8;
const LIQUIDATION_DISCOUNT: f64 = 0.05;
const FLASH_LOAN_FEE: f64 = 0.0009;
const MAX_SLIPPAGE: f64 = 0.02;
const PRIORITY_FEE_PERCENTILE: f64 = 0.95;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LiquidationOpportunity {
    pub account: Pubkey,
    pub protocol: ProtocolType,
    pub collateral_value: u64,
    pub debt_value: u64,
    pub liquidation_bonus: u64,
    pub health_factor: f64,
    pub timestamp: u64,
    pub priority_score: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtocolType {
    Mango,
    Solend,
    MarginFi,
    Kamino,
    DriftV2,
}

#[derive(Debug, Clone)]
pub struct CascadePath {
    pub opportunities: Vec<LiquidationOpportunity>,
    pub total_profit: u64,
    pub required_capital: u64,
    pub execution_cost: u64,
    pub risk_score: f64,
}

#[derive(Debug)]
pub struct LeverageCascadeOptimizer {
    rpc_client: Arc<RpcClient>,
    keypair: Arc<Keypair>,
    opportunities: Arc<RwLock<HashMap<Pubkey, LiquidationOpportunity>>>,
    cascade_paths: Arc<RwLock<BinaryHeap<CascadePath>>>,
    execution_semaphore: Arc<Semaphore>,
    oracle_cache: Arc<RwLock<HashMap<Pubkey, OraclePrice>>>,
}

#[derive(Debug, Clone)]
struct OraclePrice {
    price: u64,
    confidence: u64,
    timestamp: u64,
}

impl Ord for CascadePath {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let self_score = (self.total_profit as f64) / (self.risk_score + 1.0);
        let other_score = (other.total_profit as f64) / (other.risk_score + 1.0);
        self_score.partial_cmp(&other_score).unwrap_or(std::cmp::Ordering::Equal)
    }
}

impl PartialOrd for CascadePath {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for CascadePath {
    fn eq(&self, other: &Self) -> bool {
        self.total_profit == other.total_profit && self.risk_score == other.risk_score
    }
}

impl Eq for CascadePath {}

impl LeverageCascadeOptimizer {
    pub fn new(rpc_url: String, keypair: Keypair) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url,
            CommitmentConfig::confirmed(),
        ));

        Self {
            rpc_client,
            keypair: Arc::new(keypair),
            opportunities: Arc::new(RwLock::new(HashMap::new())),
            cascade_paths: Arc::new(RwLock::new(BinaryHeap::new())),
            execution_semaphore: Arc::new(Semaphore::new(3)),
            oracle_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let (tx, mut rx) = mpsc::channel::<LiquidationOpportunity>(1000);
        
        let scanner_handle = tokio::spawn({
            let optimizer = self.clone();
            async move {
                optimizer.scan_liquidation_opportunities(tx).await
            }
        });

        let optimizer_handle = tokio::spawn({
            let optimizer = self.clone();
            async move {
                optimizer.optimize_cascade_paths().await
            }
        });

        let executor_handle = tokio::spawn({
            let optimizer = self.clone();
            async move {
                optimizer.execute_cascades().await
            }
        });

        tokio::select! {
            _ = scanner_handle => {},
            _ = optimizer_handle => {},
            _ = executor_handle => {},
        }

        Ok(())
    }

    async fn scan_liquidation_opportunities(
        &self,
        tx: mpsc::Sender<LiquidationOpportunity>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let protocols = vec![
            ProtocolType::Mango,
            ProtocolType::Solend,
            ProtocolType::MarginFi,
            ProtocolType::Kamino,
            ProtocolType::DriftV2,
        ];

        loop {
            for protocol in &protocols {
                match self.scan_protocol_accounts(*protocol).await {
                    Ok(opportunities) => {
                        for opp in opportunities {
                            if opp.health_factor < 1.0 && opp.liquidation_bonus > MIN_PROFIT_THRESHOLD {
                                let _ = tx.send(opp.clone()).await;
                                self.opportunities.write().unwrap().insert(opp.account, opp);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error scanning protocol {:?}: {}", protocol, e);
                    }
                }
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn scan_protocol_accounts(
        &self,
        protocol: ProtocolType,
    ) -> Result<Vec<LiquidationOpportunity>, Box<dyn std::error::Error + Send + Sync>> {
        let program_id = self.get_protocol_program_id(protocol);
        let accounts = self.rpc_client.get_program_accounts(&program_id).await?;
        
        let mut opportunities = Vec::new();
        
        for (pubkey, account) in accounts {
            if let Some(opportunity) = self.parse_liquidation_opportunity(pubkey, account, protocol).await {
                opportunities.push(opportunity);
            }
        }
        
        Ok(opportunities)
    }

    async fn parse_liquidation_opportunity(
        &self,
        account: Pubkey,
        data: solana_sdk::account::Account,
        protocol: ProtocolType,
    ) -> Option<LiquidationOpportunity> {
        let health = self.calculate_account_health(&data.data, protocol).await?;
        
        if health.health_factor >= 1.0 {
            return None;
        }

        let liquidation_bonus = self.calculate_liquidation_bonus(
            health.collateral_value,
            health.debt_value,
            protocol,
        );

        Some(LiquidationOpportunity {
            account,
            protocol,
            collateral_value: health.collateral_value,
            debt_value: health.debt_value,
            liquidation_bonus,
            health_factor: health.health_factor,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            priority_score: self.calculate_priority_score(&health, liquidation_bonus),
        })
    }

    async fn calculate_account_health(
        &self,
        data: &[u8],
        protocol: ProtocolType,
    ) -> Option<AccountHealth> {
        match protocol {
            ProtocolType::Mango => self.parse_mango_health(data).await,
            ProtocolType::Solend => self.parse_solend_health(data).await,
            ProtocolType::MarginFi => self.parse_marginfi_health(data).await,
            ProtocolType::Kamino => self.parse_kamino_health(data).await,
            ProtocolType::DriftV2 => self.parse_drift_health(data).await,
        }
    }

    async fn parse_mango_health(&self, data: &[u8]) -> Option<AccountHealth> {
        if data.len() < 200 {
            return None;
        }
        
        let collateral_value = u64::from_le_bytes(data[32..40].try_into().ok()?);
        let debt_value = u64::from_le_bytes(data[40..48].try_into().ok()?);
        let health_factor = if debt_value > 0 {
            (collateral_value as f64 * 0.85) / debt_value as f64
        } else {
            f64::MAX
        };

        Some(AccountHealth {
            collateral_value,
            debt_value,
            health_factor,
        })
    }

    async fn parse_solend_health(&self, data: &[u8]) -> Option<AccountHealth> {
        if data.len() < 180 {
            return None;
        }
        
        let collateral_value = u64::from_le_bytes(data[48..56].try_into().ok()?);
        let debt_value = u64::from_le_bytes(data[56..64].try_into().ok()?);
        let health_factor = if debt_value > 0 {
            (collateral_value as f64 * 0.8) / debt_value as f64
        } else {
            f64::MAX
        };

        Some(AccountHealth {
            collateral_value,
            debt_value,
            health_factor,
        })
    }

    async fn parse_marginfi_health(&self, data: &[u8]) -> Option<AccountHealth> {
        if data.len() < 256 {
            return None;
        }
        
        let collateral_value = u64::from_le_bytes(data[64..72].try_into().ok()?);
        let debt_value = u64::from_le_bytes(data[72..80].try_into().ok()?);
        let health_factor = if debt_value > 0 {
            (collateral_value as f64 * 0.82) / debt_value as f64
        } else {
            f64::MAX
        };

        Some(AccountHealth {
            collateral_value,
            debt_value,
            health_factor,
        })
    }

    async fn parse_kamino_health(&self, data: &[u8]) -> Option<AccountHealth> {
        if data.len() < 240 {
            return None;
        }
        
        let collateral_value = u64::from_le_bytes(data[80..88].try_into().ok()?);
        let debt_value = u64::from_le_bytes(data[88..96].try_into().ok()?);
        let health_factor = if debt_value > 0 {
            (collateral_value as f64 * 0.83) / debt_value as f64
        } else {
            f64::MAX
        };

        Some(AccountHealth {
            collateral_value,
            debt_value,
            health_factor,
        })
    }

    async fn parse_drift_health(&self, data: &[u8]) -> Option<AccountHealth> {
        if data.len() < 300 {
            return None;
        }
        
        let collateral_value = u64::from_le_bytes(data[96..104].try_into().ok()?);
        let debt_value = u64::from_le_bytes(data[104..112].try_into().ok()?);
        let health_factor = if debt_value > 0 {
            (collateral_value as f64 * 0.875) / debt_value as f64
        } else {
            f64::MAX
        };

        Some(AccountHealth {
            collateral_value,
            debt_value,
            health_factor,
        })
    }

    fn calculate_liquidation_bonus(
        &self,
        collateral_value: u64,
        debt_value: u64,
        protocol: ProtocolType,
    ) -> u64 {
        let base_bonus = match protocol {
            ProtocolType::Mango => 0.05,
            ProtocolType::Solend => 0.05,
            ProtocolType::MarginFi => 0.025,
            ProtocolType::Kamino => 0.04,
            ProtocolType::DriftV2 => 0.025,
        };
        
        let liquidatable_amount = collateral_value.min(debt_value);
        ((liquidatable_amount as f64) * base_bonus) as u64
    }

    fn calculate_priority_score(&self, health: &AccountHealth, bonus: u64) -> f64 {
        let urgency = 1.0 / (health.health_factor + 0.01);
        let profitability = bonus as f64 / 1_000_000.0;
        let size_factor = (health.debt_value as f64).ln() / 10.0;
        
                urgency * profitability * size_factor
    }

    async fn optimize_cascade_paths(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            let opportunities = self.opportunities.read().unwrap().clone();
            if opportunities.len() < 2 {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            }

            let mut cascade_paths = Vec::new();
            let mut visited = HashSet::new();

            for (_, start_opp) in opportunities.iter() {
                if visited.contains(&start_opp.account) {
                    continue;
                }

                let path = self.build_cascade_path(start_opp, &opportunities, &mut visited).await;
                if path.total_profit > MIN_PROFIT_THRESHOLD {
                    cascade_paths.push(path);
                }
            }

            cascade_paths.sort_by(|a, b| b.cmp(a));
            let top_paths: BinaryHeap<_> = cascade_paths.into_iter().take(10).collect();
            
            *self.cascade_paths.write().unwrap() = top_paths;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn build_cascade_path(
        &self,
        start: &LiquidationOpportunity,
        opportunities: &HashMap<Pubkey, LiquidationOpportunity>,
        visited: &mut HashSet<Pubkey>,
    ) -> CascadePath {
        let mut path = vec![start.clone()];
        let mut total_profit = start.liquidation_bonus;
        let mut required_capital = start.debt_value;
        let mut current = start.clone();
        
        visited.insert(start.account);

        for _ in 1..MAX_CASCADE_DEPTH {
            if let Some(next) = self.find_next_cascade(&current, opportunities, visited).await {
                let cascade_profit = self.calculate_cascade_profit(&current, &next);
                
                if cascade_profit > 0 {
                    total_profit += cascade_profit;
                    required_capital = required_capital.max(next.debt_value);
                    visited.insert(next.account);
                    path.push(next.clone());
                    current = next;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        let execution_cost = self.calculate_execution_cost(&path);
        let risk_score = self.calculate_risk_score(&path, required_capital);

        CascadePath {
            opportunities: path,
            total_profit: total_profit.saturating_sub(execution_cost),
            required_capital,
            execution_cost,
            risk_score,
        }
    }

    async fn find_next_cascade(
        &self,
        current: &LiquidationOpportunity,
        opportunities: &HashMap<Pubkey, LiquidationOpportunity>,
        visited: &HashSet<Pubkey>,
    ) -> Option<LiquidationOpportunity> {
        opportunities
            .values()
            .filter(|opp| {
                !visited.contains(&opp.account) &&
                opp.protocol == current.protocol &&
                opp.health_factor < 1.0 &&
                self.can_cascade(current, opp)
            })
            .max_by(|a, b| {
                a.priority_score.partial_cmp(&b.priority_score).unwrap_or(std::cmp::Ordering::Equal)
            })
            .cloned()
    }

    fn can_cascade(&self, from: &LiquidationOpportunity, to: &LiquidationOpportunity) -> bool {
        let time_diff = to.timestamp.saturating_sub(from.timestamp);
        time_diff < 60 && 
        to.debt_value <= from.collateral_value * 2 &&
        to.health_factor < from.health_factor + 0.1
    }

    fn calculate_cascade_profit(
        &self,
        from: &LiquidationOpportunity,
        to: &LiquidationOpportunity,
    ) -> u64 {
        let cascade_bonus = ((to.debt_value as f64) * LIQUIDATION_DISCOUNT * 0.8) as u64;
        let flash_loan_cost = ((to.debt_value as f64) * FLASH_LOAN_FEE) as u64;
        cascade_bonus.saturating_sub(flash_loan_cost)
    }

    fn calculate_execution_cost(&self, path: &[LiquidationOpportunity]) -> u64 {
        let base_tx_cost = 5000 * path.len() as u64;
        let compute_cost = COMPUTE_UNIT_PRICE * path.len() as u64;
        let priority_fee = 100_000 * path.len() as u64;
        base_tx_cost + compute_cost + priority_fee
    }

    fn calculate_risk_score(&self, path: &[LiquidationOpportunity], capital: u64) -> f64 {
        let depth_risk = (path.len() as f64) * 0.1;
        let capital_risk = (capital as f64 / MAX_LIQUIDATION_SIZE as f64).min(1.0);
        let timing_risk = path.iter()
            .map(|opp| 1.0 - opp.health_factor)
            .sum::<f64>() / path.len() as f64;
        
        depth_risk + capital_risk + timing_risk
    }

    async fn execute_cascades(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            let path = {
                let mut paths = self.cascade_paths.write().unwrap();
                paths.pop()
            };

            if let Some(cascade_path) = path {
                if cascade_path.total_profit > MIN_PROFIT_THRESHOLD {
                    let permit = self.execution_semaphore.acquire().await.unwrap();
                    tokio::spawn({
                        let optimizer = self.clone();
                        let path = cascade_path.clone();
                        async move {
                            let result = optimizer.execute_cascade_path(path).await;
                            drop(permit);
                            if let Err(e) = result {
                                eprintln!("Cascade execution error: {}", e);
                            }
                        }
                    });
                }
            }
            
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn execute_cascade_path(
        &self,
        path: CascadePath,
    ) -> Result<Signature, Box<dyn std::error::Error + Send + Sync>> {
        let flash_loan_ix = self.build_flash_loan_instruction(path.required_capital).await?;
        let mut instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNIT_LIMIT),
            ComputeBudgetInstruction::set_compute_unit_price(COMPUTE_UNIT_PRICE),
            flash_loan_ix,
        ];

        for opportunity in &path.opportunities {
            let liquidation_ix = self.build_liquidation_instruction(opportunity).await?;
            instructions.push(liquidation_ix);
        }

        let repay_ix = self.build_flash_loan_repay_instruction(path.required_capital).await?;
        instructions.push(repay_ix);

        let recent_blockhash = self.rpc_client.get_latest_blockhash().await?;
        let transaction = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.keypair.pubkey()),
            &[&*self.keypair],
            recent_blockhash,
        );

        let config = RpcSimulateTransactionConfig {
            sig_verify: true,
            replace_recent_blockhash: true,
            commitment: Some(CommitmentConfig::confirmed()),
            ..Default::default()
        };

        let simulation = self.rpc_client.simulate_transaction_with_config(&transaction, config).await?;
        
        if simulation.value.err.is_some() {
            return Err("Simulation failed".into());
        }

        let signature = self.rpc_client
            .send_and_confirm_transaction_with_spinner_and_config(
                &transaction,
                CommitmentConfig::confirmed(),
                RpcTransactionConfig {
                    skip_preflight: true,
                    ..Default::default()
                },
            )
            .await?;

        Ok(signature)
    }

    async fn build_flash_loan_instruction(&self, amount: u64) -> Result<Instruction, Box<dyn std::error::Error + Send + Sync>> {
        let flash_loan_program = Pubkey::from_str("So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo")?;
        let (pool_pda, _) = Pubkey::find_program_address(&[b"pool"], &flash_loan_program);
        
        let accounts = vec![
            solana_sdk::instruction::AccountMeta::new(self.keypair.pubkey(), true),
            solana_sdk::instruction::AccountMeta::new(pool_pda, false),
            solana_sdk::instruction::AccountMeta::new_readonly(sysvar::clock::id(), false),
        ];

        let data = FlashLoanInstruction::Borrow { amount }.try_to_vec()?;
        
        Ok(Instruction::new_with_bytes(flash_loan_program, &data, accounts))
    }

    async fn build_liquidation_instruction(
        &self,
        opportunity: &LiquidationOpportunity,
    ) -> Result<Instruction, Box<dyn std::error::Error + Send + Sync>> {
        let program_id = self.get_protocol_program_id(opportunity.protocol);
        
        let accounts = match opportunity.protocol {
            ProtocolType::Mango => self.build_mango_liquidation_accounts(opportunity),
            ProtocolType::Solend => self.build_solend_liquidation_accounts(opportunity),
            ProtocolType::MarginFi => self.build_marginfi_liquidation_accounts(opportunity),
            ProtocolType::Kamino => self.build_kamino_liquidation_accounts(opportunity),
            ProtocolType::DriftV2 => self.build_drift_liquidation_accounts(opportunity),
        };

        let data = LiquidateInstruction {
            amount: opportunity.debt_value,
            min_collateral: (opportunity.collateral_value as f64 * (1.0 - MAX_SLIPPAGE)) as u64,
        }.try_to_vec()?;

        Ok(Instruction::new_with_bytes(program_id, &data, accounts))
    }

    async fn build_flash_loan_repay_instruction(&self, amount: u64) -> Result<Instruction, Box<dyn std::error::Error + Send + Sync>> {
        let flash_loan_program = Pubkey::from_str("So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo")?;
        let (pool_pda, _) = Pubkey::find_program_address(&[b"pool"], &flash_loan_program);
        
        let repay_amount = ((amount as f64) * (1.0 + FLASH_LOAN_FEE)) as u64;
        
        let accounts = vec![
            solana_sdk::instruction::AccountMeta::new(self.keypair.pubkey(), true),
            solana_sdk::instruction::AccountMeta::new(pool_pda, false),
            solana_sdk::instruction::AccountMeta::new_readonly(sysvar::clock::id(), false),
        ];

        let data = FlashLoanInstruction::Repay { amount: repay_amount }.try_to_vec()?;
        
        Ok(Instruction::new_with_bytes(flash_loan_program, &data, accounts))
    }

    fn get_protocol_program_id(&self, protocol: ProtocolType) -> Pubkey {
        match protocol {
            ProtocolType::Mango => Pubkey::from_str("4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg").unwrap(),
            ProtocolType::Solend => Pubkey::from_str("So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo").unwrap(),
            ProtocolType::MarginFi => Pubkey::from_str("MFv2hWf31Z9kbCa1snEPYctwafyhdvnV7FZnsebVacA").unwrap(),
            ProtocolType::Kamino => Pubkey::from_str("KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD").unwrap(),
            ProtocolType::DriftV2 => Pubkey::from_str("dRiftyHA39MWEi3m9aunc5MzRF1JYuBsbn6VPcn33UH").unwrap(),
        }
    }

    fn build_mango_liquidation_accounts(&self, opp: &LiquidationOpportunity) -> Vec<solana_sdk::instruction::AccountMeta> {
        vec![
            solana_sdk::instruction::AccountMeta::new(self.keypair.pubkey(), true),
            solana_sdk::instruction::AccountMeta::new(opp.account, false),
            solana_sdk::instruction::AccountMeta::new_readonly(system_program::id(), false),
        ]
    }

    fn build_solend_liquidation_accounts(&self, opp: &LiquidationOpportunity) -> Vec<solana_sdk::instruction::AccountMeta> {
        vec![
            solana_sdk::instruction::AccountMeta::new(self.keypair.pubkey(), true),
            solana_sdk::instruction::AccountMeta::new(opp.account, false),
            solana_sdk::instruction::AccountMeta::new_readonly(sysvar::clock::id(), false),
        ]
    }

    fn build_marginfi_liquidation_accounts(&self, opp: &LiquidationOpportunity) -> Vec<solana_sdk::instruction::AccountMeta> {
        vec![
            solana_sdk::instruction::AccountMeta::new(self.keypair.pubkey(), true),
            solana_sdk::instruction::AccountMeta::new(opp.account, false),
            solana_sdk::instruction::AccountMeta::new_readonly(system_program::id(), false),
        ]
    }

        fn build_kamino_liquidation_accounts(&self, opp: &LiquidationOpportunity) -> Vec<solana_sdk::instruction::AccountMeta> {
        vec![
            solana_sdk::instruction::AccountMeta::new(self.keypair.pubkey(), true),
            solana_sdk::instruction::AccountMeta::new(opp.account, false),
            solana_sdk::instruction::AccountMeta::new_readonly(system_program::id(), false),
            solana_sdk::instruction::AccountMeta::new_readonly(sysvar::clock::id(), false),
        ]
    }

    fn build_drift_liquidation_accounts(&self, opp: &LiquidationOpportunity) -> Vec<solana_sdk::instruction::AccountMeta> {
        vec![
            solana_sdk::instruction::AccountMeta::new(self.keypair.pubkey(), true),
            solana_sdk::instruction::AccountMeta::new(opp.account, false),
            solana_sdk::instruction::AccountMeta::new_readonly(system_program::id(), false),
            solana_sdk::instruction::AccountMeta::new_readonly(sysvar::rent::id(), false),
        ]
    }
}

impl Clone for LeverageCascadeOptimizer {
    fn clone(&self) -> Self {
        Self {
            rpc_client: self.rpc_client.clone(),
            keypair: self.keypair.clone(),
            opportunities: self.opportunities.clone(),
            cascade_paths: self.cascade_paths.clone(),
            execution_semaphore: self.execution_semaphore.clone(),
            oracle_cache: self.oracle_cache.clone(),
        }
    }
}

#[derive(Debug, Clone)]
struct AccountHealth {
    collateral_value: u64,
    debt_value: u64,
    health_factor: f64,
}

#[derive(BorshSerialize, BorshDeserialize)]
enum FlashLoanInstruction {
    Borrow { amount: u64 },
    Repay { amount: u64 },
}

#[derive(BorshSerialize, BorshDeserialize)]
struct LiquidateInstruction {
    amount: u64,
    min_collateral: u64,
}

use std::str::FromStr;

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;

    #[tokio::test]
    async fn test_priority_score_calculation() {
        let optimizer = LeverageCascadeOptimizer::new(
            "https://api.mainnet-beta.solana.com".to_string(),
            Keypair::new(),
        );

        let health = AccountHealth {
            collateral_value: 1_000_000_000,
            debt_value: 950_000_000,
            health_factor: 0.95,
        };

        let score = optimizer.calculate_priority_score(&health, 50_000_000);
        assert!(score > 0.0);
    }

    #[tokio::test]
    async fn test_cascade_profit_calculation() {
        let optimizer = LeverageCascadeOptimizer::new(
            "https://api.mainnet-beta.solana.com".to_string(),
            Keypair::new(),
        );

        let from = LiquidationOpportunity {
            account: Pubkey::new_unique(),
            protocol: ProtocolType::Mango,
            collateral_value: 2_000_000_000,
            debt_value: 1_800_000_000,
            liquidation_bonus: 90_000_000,
            health_factor: 0.9,
            timestamp: 1000,
            priority_score: 10.0,
        };

        let to = LiquidationOpportunity {
            account: Pubkey::new_unique(),
            protocol: ProtocolType::Mango,
            collateral_value: 1_500_000_000,
            debt_value: 1_400_000_000,
            liquidation_bonus: 70_000_000,
            health_factor: 0.92,
            timestamp: 1010,
            priority_score: 8.0,
        };

        let profit = optimizer.calculate_cascade_profit(&from, &to);
        assert!(profit > 0);
    }
}

