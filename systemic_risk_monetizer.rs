use anchor_lang::prelude::*;
use anchor_spl::token::{self, Token, TokenAccount};
use pyth_sdk_solana::{load_price_feed_from_account_info, Price, PriceFeed};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    account::Account,
    commitment_config::CommitmentConfig,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use std::{
    collections::{HashMap, BinaryHeap},
    sync::{Arc, RwLock},
    time::{Duration, Instant},
    cmp::Ordering,
};
use tokio::sync::mpsc;
use borsh::{BorshDeserialize, BorshSerialize};

const LIQUIDATION_DISCOUNT: f64 = 0.95;
const MIN_PROFIT_THRESHOLD: u64 = 10_000_000; // 0.01 SOL
const MAX_SLIPPAGE_BPS: u16 = 50; // 0.5%
const ORACLE_CONFIDENCE_THRESHOLD: f64 = 0.02;
const MAX_EXPOSURE_PER_PROTOCOL: u64 = 1_000_000_000_000; // 1000 SOL
const PRIORITY_FEE_MICROLAMPORTS: u64 = 50_000;
const COMPUTE_UNITS: u32 = 1_400_000;

#[derive(Debug, Clone)]
pub struct SystemicRiskOpportunity {
    pub protocol: ProtocolType,
    pub risk_type: RiskType,
    pub affected_accounts: Vec<Pubkey>,
    pub estimated_profit: u64,
    pub execution_priority: f64,
    pub expiry: Instant,
    pub collateral_info: CollateralInfo,
    pub oracle_prices: HashMap<Pubkey, OracleData>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProtocolType {
    Mango,
    Solend,
    Kamino,
    MarginFi,
    Drift,
}

#[derive(Debug, Clone)]
pub enum RiskType {
    Liquidation { health_ratio: f64 },
    OracleDeviation { deviation_pct: f64 },
    ProtocolInsolvency { deficit: u64 },
    CascadingLiquidation { chain_length: u32 },
}

#[derive(Debug, Clone)]
pub struct CollateralInfo {
    pub mint: Pubkey,
    pub amount: u64,
    pub value_usd: f64,
    pub liquidation_threshold: f64,
    pub liquidation_bonus: f64,
}

#[derive(Debug, Clone)]
pub struct OracleData {
    pub price: f64,
    pub confidence: f64,
    pub last_update: i64,
    pub expo: i32,
}

impl Eq for SystemicRiskOpportunity {}

impl PartialEq for SystemicRiskOpportunity {
    fn eq(&self, other: &Self) -> bool {
        self.execution_priority.to_bits() == other.execution_priority.to_bits()
    }
}

impl Ord for SystemicRiskOpportunity {
    fn cmp(&self, other: &Self) -> Ordering {
        self.execution_priority.partial_cmp(&other.execution_priority)
            .unwrap_or(Ordering::Equal)
    }
}

impl PartialOrd for SystemicRiskOpportunity {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub struct SystemicRiskMonetizer {
    rpc_client: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    protocol_configs: HashMap<ProtocolType, ProtocolConfig>,
    opportunity_queue: Arc<RwLock<BinaryHeap<SystemicRiskOpportunity>>>,
    oracle_cache: Arc<RwLock<HashMap<Pubkey, OracleData>>>,
    exposure_tracker: Arc<RwLock<HashMap<ProtocolType, u64>>>,
}

#[derive(Clone)]
struct ProtocolConfig {
    program_id: Pubkey,
    oracle_mapping: HashMap<Pubkey, Pubkey>,
    liquidation_fee: f64,
    health_threshold: f64,
}

impl SystemicRiskMonetizer {
    pub fn new(rpc_url: &str, wallet: Keypair) -> Self {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            CommitmentConfig::processed(),
        ));

        let mut protocol_configs = HashMap::new();
        
        protocol_configs.insert(ProtocolType::Mango, ProtocolConfig {
            program_id: Pubkey::from_str_const("4MangoMjqJ2firMokCjjGgoK8d4MXcrgL7XJaL3w6fVg"),
            oracle_mapping: HashMap::new(),
            liquidation_fee: 0.05,
            health_threshold: 1.1,
        });

        protocol_configs.insert(ProtocolType::Solend, ProtocolConfig {
            program_id: Pubkey::from_str_const("So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo"),
            oracle_mapping: HashMap::new(),
            liquidation_fee: 0.05,
            health_threshold: 1.1,
        });

        protocol_configs.insert(ProtocolType::Kamino, ProtocolConfig {
            program_id: Pubkey::from_str_const("KLend2g3cP87fffoy8q1mQqGKjrxjC8boSyAYavgmjD"),
            oracle_mapping: HashMap::new(),
            liquidation_fee: 0.04,
            health_threshold: 1.05,
        });

        Self {
            rpc_client,
            wallet: Arc::new(wallet),
            protocol_configs,
            opportunity_queue: Arc::new(RwLock::new(BinaryHeap::new())),
            oracle_cache: Arc::new(RwLock::new(HashMap::new())),
            exposure_tracker: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub async fn scan_for_opportunities(&self) -> Result<Vec<SystemicRiskOpportunity>> {
        let mut opportunities = Vec::new();

        for (protocol_type, config) in &self.protocol_configs {
            match protocol_type {
                ProtocolType::Mango => {
                    let mango_opps = self.scan_mango_liquidations(config).await?;
                    opportunities.extend(mango_opps);
                }
                ProtocolType::Solend => {
                    let solend_opps = self.scan_solend_positions(config).await?;
                    opportunities.extend(solend_opps);
                }
                ProtocolType::Kamino => {
                    let kamino_opps = self.scan_kamino_risks(config).await?;
                    opportunities.extend(kamino_opps);
                }
                _ => {}
            }
        }

        self.identify_cascading_risks(&mut opportunities)?;
        Ok(opportunities)
    }

    async fn scan_mango_liquidations(&self, config: &ProtocolConfig) -> Result<Vec<SystemicRiskOpportunity>> {
        let accounts = self.rpc_client.get_program_accounts(&config.program_id)?;
        let mut opportunities = Vec::new();

        for (pubkey, account) in accounts {
            if account.data.len() < 3424 { continue; }
            
            let health_ratio = self.calculate_mango_health(&account.data)?;
            
            if health_ratio < config.health_threshold && health_ratio > 0.0 {
                let collateral = self.extract_mango_collateral(&account.data)?;
                let profit = self.calculate_liquidation_profit(&collateral, config.liquidation_fee);
                
                if profit > MIN_PROFIT_THRESHOLD {
                    opportunities.push(SystemicRiskOpportunity {
                        protocol: ProtocolType::Mango,
                        risk_type: RiskType::Liquidation { health_ratio },
                        affected_accounts: vec![pubkey],
                        estimated_profit: profit,
                        execution_priority: (profit as f64) / (1.0 + health_ratio),
                        expiry: Instant::now() + Duration::from_secs(30),
                        collateral_info: collateral,
                        oracle_prices: self.oracle_cache.read().unwrap().clone(),
                    });
                }
            }
        }

        Ok(opportunities)
    }

    async fn scan_solend_positions(&self, config: &ProtocolConfig) -> Result<Vec<SystemicRiskOpportunity>> {
        let accounts = self.rpc_client.get_program_accounts(&config.program_id)?;
        let mut opportunities = Vec::new();

        for (pubkey, account) in accounts {
            if account.data.len() < 1024 { continue; }
            
            let position = self.parse_solend_obligation(&account.data)?;
            
            if let Some(position) = position {
                let health = self.calculate_solend_health(&position)?;
                
                if health < 1.0 && health > 0.0 {
                    let profit = self.estimate_solend_liquidation_profit(&position);
                    
                    if profit > MIN_PROFIT_THRESHOLD {
                        opportunities.push(SystemicRiskOpportunity {
                            protocol: ProtocolType::Solend,
                            risk_type: RiskType::Liquidation { health_ratio: health },
                            affected_accounts: vec![pubkey],
                            estimated_profit: profit,
                            execution_priority: profit as f64 * (1.0 - health),
                            expiry: Instant::now() + Duration::from_secs(20),
                            collateral_info: position.collateral,
                            oracle_prices: self.oracle_cache.read().unwrap().clone(),
                        });
                    }
                }
            }
        }

        Ok(opportunities)
    }

    async fn scan_kamino_risks(&self, config: &ProtocolConfig) -> Result<Vec<SystemicRiskOpportunity>> {
        let accounts = self.rpc_client.get_program_accounts(&config.program_id)?;
        let mut opportunities = Vec::new();

        for (pubkey, account) in accounts {
            if account.data.len() < 512 { continue; }
            
            let risk_metrics = self.analyze_kamino_position(&account.data)?;
            
            if let Some(metrics) = risk_metrics {
                if metrics.is_liquidatable {
                    opportunities.push(SystemicRiskOpportunity {
                        protocol: ProtocolType::Kamino,
                        risk_type: RiskType::Liquidation { health_ratio: metrics.health },
                        affected_accounts: vec![pubkey],
                        estimated_profit: metrics.liquidation_value,
                        execution_priority: metrics.priority_score,
                        expiry: Instant::now() + Duration::from_secs(15),
                        collateral_info: metrics.collateral,
                        oracle_prices: self.oracle_cache.read().unwrap().clone(),
                    });
                }
            }
        }

        Ok(opportunities)
    }

    fn calculate_mango_health(&self, data: &[u8]) -> Result<f64> {
        if data.len() < 3424 { return Ok(2.0); }
        
        let assets_value = f64::from_le_bytes(data[256..264].try_into()?);
        let liabs_value = f64::from_le_bytes(data[264..272].try_into()?);
        let maint_weights = f64::from_le_bytes(data[272..280].try_into()?);
        
        if liabs_value == 0.0 { return Ok(100.0); }
        
        Ok((assets_value * maint_weights) / liabs_value)
    }

    fn extract_mango_collateral(&self, data: &[u8]) -> Result<CollateralInfo> {
        let mint = Pubkey::new(&data[32..64]);
        let amount = u64::from_le_bytes(data[64..72].try_into()?);
        let value_usd = f64::from_le_bytes(data[72..80].try_into()?);
        
        Ok(CollateralInfo {
            mint,
            amount,
            value_usd,
            liquidation_threshold: 0.85,
            liquidation_bonus: 0.05,
        })
    }

    fn calculate_liquidation_profit(&self, collateral: &CollateralInfo, liq_fee: f64) -> u64 {
        let liquidation_value = collateral.value_usd * LIQUIDATION_DISCOUNT;
        let bonus = collateral.value_usd * liq_fee;
        ((liquidation_value + bonus) * 1e9) as u64
    }

    fn parse_solend_obligation(&self, data: &[u8]) -> Result<Option<SolendPosition>> {
        if data.len() < 256 { return Ok(None); }
        
        let discriminator = &data[0..8];
        if discriminator != b"obligatn" { return Ok(None); }
        
        let collateral_value = u64::from_le_bytes(data[16..24].try_into()?);
        let borrowed_value = u64::from_le_bytes(data[24..32].try_into()?);
        let collateral_mint = Pubkey::new(&data[32..64]);
        
        Ok(Some(SolendPosition {
            collateral: CollateralInfo {
                mint: collateral_mint,
                amount: collateral_value,
                value_usd: collateral_value as f64 / 1e9,
                liquidation_threshold: 0.85,
                liquidation_bonus: 0.05,
            },
            borrowed_value,
        }))
    }

    fn calculate_solend_health(&self, position: &SolendPosition) -> Result<f64> {
        if position.borrowed_value == 0 { return Ok(100.0); }
        
                let risk_adjusted_collateral = position.collateral.value_usd * position.collateral.liquidation_threshold;
        Ok(risk_adjusted_collateral / (position.borrowed_value as f64 / 1e9))
    }

    fn estimate_solend_liquidation_profit(&self, position: &SolendPosition) -> u64 {
        let max_liquidation = (position.borrowed_value as f64 * 0.5) as u64;
        let collateral_seized = (max_liquidation as f64 * (1.0 + position.collateral.liquidation_bonus)) as u64;
        collateral_seized.saturating_sub(max_liquidation)
    }

    fn analyze_kamino_position(&self, data: &[u8]) -> Result<Option<KaminoRiskMetrics>> {
        if data.len() < 512 { return Ok(None); }
        
        let position_value = u64::from_le_bytes(data[32..40].try_into()?);
        let debt_value = u64::from_le_bytes(data[40..48].try_into()?);
        let collateral_mint = Pubkey::new(&data[48..80]);
        
        if debt_value == 0 { return Ok(None); }
        
        let health = (position_value as f64) / (debt_value as f64);
        let is_liquidatable = health < 1.1;
        
        Ok(Some(KaminoRiskMetrics {
            health,
            is_liquidatable,
            liquidation_value: debt_value / 20,
            priority_score: if is_liquidatable { (1.1 - health) * (debt_value as f64) } else { 0.0 },
            collateral: CollateralInfo {
                mint: collateral_mint,
                amount: position_value,
                value_usd: position_value as f64 / 1e9,
                liquidation_threshold: 0.9,
                liquidation_bonus: 0.04,
            },
        }))
    }

    fn identify_cascading_risks(&self, opportunities: &mut Vec<SystemicRiskOpportunity>) -> Result<()> {
        let mut risk_graph: HashMap<Pubkey, Vec<usize>> = HashMap::new();
        
        for (i, opp) in opportunities.iter().enumerate() {
            for (j, other) in opportunities.iter().enumerate() {
                if i != j && self.are_positions_linked(&opp.collateral_info, &other.collateral_info) {
                    risk_graph.entry(opp.affected_accounts[0]).or_insert_with(Vec::new).push(j);
                }
            }
        }
        
        for (_, linked_positions) in risk_graph {
            if linked_positions.len() > 2 {
                let total_value: u64 = linked_positions.iter()
                    .map(|&idx| opportunities[idx].estimated_profit)
                    .sum();
                
                if total_value > MIN_PROFIT_THRESHOLD * 10 {
                    opportunities.push(SystemicRiskOpportunity {
                        protocol: ProtocolType::Mango,
                        risk_type: RiskType::CascadingLiquidation { chain_length: linked_positions.len() as u32 },
                        affected_accounts: linked_positions.iter()
                            .map(|&idx| opportunities[idx].affected_accounts[0])
                            .collect(),
                        estimated_profit: total_value,
                        execution_priority: total_value as f64 * 1.5,
                        expiry: Instant::now() + Duration::from_secs(10),
                        collateral_info: opportunities[linked_positions[0]].collateral_info.clone(),
                        oracle_prices: self.oracle_cache.read().unwrap().clone(),
                    });
                }
            }
        }
        
        Ok(())
    }

    fn are_positions_linked(&self, collateral1: &CollateralInfo, collateral2: &CollateralInfo) -> bool {
        collateral1.mint == collateral2.mint || 
        (collateral1.value_usd - collateral2.value_usd).abs() / collateral1.value_usd < 0.1
    }

    pub async fn update_oracle_cache(&self) -> Result<()> {
        let oracle_accounts = vec![
            "H6ARHf6YXhGYeQfUzQNGk6rDNnLBQKrenN712K4AQJEG", // SOL/USD
            "Gnt27xtC473ZT2Mw5u8wZ68Z3gULkSTqQki",          // BTC/USD
            "JBu1AL4obBcCMqKBBxhpWCNUt136ijcuMZL",          // ETH/USD
        ];
        
        let mut cache = self.oracle_cache.write().unwrap();
        
        for oracle_str in oracle_accounts {
            let oracle_pubkey = Pubkey::from_str_const(oracle_str);
            let account = self.rpc_client.get_account(&oracle_pubkey)?;
            
            if let Ok(price_feed) = load_price_feed_from_account_info(&oracle_pubkey, &account.into()) {
                let price = price_feed.get_current_price().unwrap();
                cache.insert(oracle_pubkey, OracleData {
                    price: price.price as f64 * 10f64.powi(price.expo),
                    confidence: price.conf as f64 * 10f64.powi(price.expo),
                    last_update: price.publish_time,
                    expo: price.expo,
                });
            }
        }
        
        Ok(())
    }

    pub async fn execute_opportunity(&self, opportunity: &SystemicRiskOpportunity) -> Result<String> {
        let current_exposure = {
            let tracker = self.exposure_tracker.read().unwrap();
            *tracker.get(&opportunity.protocol).unwrap_or(&0)
        };
        
        if current_exposure + opportunity.estimated_profit > MAX_EXPOSURE_PER_PROTOCOL {
            return Err(anyhow::anyhow!("Max exposure exceeded"));
        }
        
        match &opportunity.risk_type {
            RiskType::Liquidation { .. } => {
                self.execute_liquidation(opportunity).await
            }
            RiskType::CascadingLiquidation { .. } => {
                self.execute_cascading_liquidation(opportunity).await
            }
            _ => {
                self.execute_generic_opportunity(opportunity).await
            }
        }
    }

    async fn execute_liquidation(&self, opportunity: &SystemicRiskOpportunity) -> Result<String> {
        let ix = match opportunity.protocol {
            ProtocolType::Mango => self.build_mango_liquidation_ix(opportunity)?,
            ProtocolType::Solend => self.build_solend_liquidation_ix(opportunity)?,
            ProtocolType::Kamino => self.build_kamino_liquidation_ix(opportunity)?,
            _ => return Err(anyhow::anyhow!("Unsupported protocol")),
        };
        
        let mut instructions = vec![
            self.build_compute_budget_ix(),
            self.build_priority_fee_ix(),
            ix,
        ];
        
        let recent_blockhash = self.rpc_client.get_latest_blockhash()?;
        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.wallet.pubkey()),
            &[&*self.wallet],
            recent_blockhash,
        );
        
        let signature = self.rpc_client.send_and_confirm_transaction(&tx)?;
        
        self.exposure_tracker.write().unwrap()
            .entry(opportunity.protocol.clone())
            .and_modify(|e| *e += opportunity.estimated_profit)
            .or_insert(opportunity.estimated_profit);
        
        Ok(signature.to_string())
    }

    async fn execute_cascading_liquidation(&self, opportunity: &SystemicRiskOpportunity) -> Result<String> {
        let mut instructions = vec![
            self.build_compute_budget_ix(),
            self.build_priority_fee_ix(),
        ];
        
        for account in &opportunity.affected_accounts[..3.min(opportunity.affected_accounts.len())] {
            let ix = self.build_liquidation_ix_for_account(&opportunity.protocol, account)?;
            instructions.push(ix);
        }
        
        let recent_blockhash = self.rpc_client.get_latest_blockhash()?;
        let tx = Transaction::new_signed_with_payer(
            &instructions,
            Some(&self.wallet.pubkey()),
            &[&*self.wallet],
            recent_blockhash,
        );
        
        let signature = self.rpc_client.send_and_confirm_transaction(&tx)?;
        Ok(signature.to_string())
    }

    async fn execute_generic_opportunity(&self, opportunity: &SystemicRiskOpportunity) -> Result<String> {
        let ix = self.build_generic_execution_ix(opportunity)?;
        
        let recent_blockhash = self.rpc_client.get_latest_blockhash()?;
        let tx = Transaction::new_signed_with_payer(
            &[
                self.build_compute_budget_ix(),
                self.build_priority_fee_ix(),
                ix,
            ],
            Some(&self.wallet.pubkey()),
            &[&*self.wallet],
            recent_blockhash,
        );
        
        let signature = self.rpc_client.send_and_confirm_transaction(&tx)?;
        Ok(signature.to_string())
    }

    fn build_mango_liquidation_ix(&self, opportunity: &SystemicRiskOpportunity) -> Result<Instruction> {
        let program_id = self.protocol_configs[&ProtocolType::Mango].program_id;
        let liquidatee = opportunity.affected_accounts[0];
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(liquidatee, false),
                AccountMeta::new(self.wallet.pubkey(), true),
                AccountMeta::new_readonly(opportunity.collateral_info.mint, false),
                AccountMeta::new_readonly(spl_token::ID, false),
            ],
            data: vec![4, 0, 0, 0, 0, 0, 0, 0],
        })
    }

    fn build_solend_liquidation_ix(&self, opportunity: &SystemicRiskOpportunity) -> Result<Instruction> {
        let program_id = self.protocol_configs[&ProtocolType::Solend].program_id;
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(opportunity.affected_accounts[0], false),
                AccountMeta::new(self.wallet.pubkey(), true),
                AccountMeta::new_readonly(opportunity.collateral_info.mint, false),
            ],
            data: vec![12, 0, 0, 0],
        })
    }

    fn build_kamino_liquidation_ix(&self, opportunity: &SystemicRiskOpportunity) -> Result<Instruction> {
        let program_id = self.protocol_configs[&ProtocolType::Kamino].program_id;
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(opportunity.affected_accounts[0], false),
                AccountMeta::new(self.wallet.pubkey(), true),
            ],
            data: vec![8, 0, 0, 0],
        })
    }

    fn build_liquidation_ix_for_account(&self, protocol: &ProtocolType, account: &Pubkey) -> Result<Instruction> {
        let program_id = self.protocol_configs[protocol].program_id;
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(*account, false),
                AccountMeta::new(self.wallet.pubkey(), true),
            ],
            data: vec![4, 0, 0, 0],
        })
    }

    fn build_generic_execution_ix(&self, opportunity: &SystemicRiskOpportunity) -> Result<Instruction> {
        let program_id = self.protocol_configs[&opportunity.protocol].program_id;
        
        Ok(Instruction {
            program_id,
            accounts: vec![
                AccountMeta::new(self.wallet.pubkey(), true),
                AccountMeta::new(opportunity.affected_accounts[0], false),
            ],
            data: vec![1, 0, 0, 0],
        })
    }

    fn build_compute_budget_ix(&self) -> Instruction {
        Instruction {
            program_id: Pubkey::from_str_const("ComputeBudget111111111111111111111111111111"),
            accounts: vec![],
            data: vec![2, COMPUTE_UNITS.to_le_bytes().to_vec()].concat(),
        }
    }

    fn build_priority_fee_ix(&self) -> Instruction {
        Instruction {
            program_id: Pubkey::from_str_const("ComputeBudget111111111111111111111111111111"),
            accounts: vec![],
            data: vec![3, PRIORITY_FEE_MICROLAMPORTS.to_le_bytes().to_vec()].concat(),
        }
    }

    pub async fn run_monitoring_loop(&self, tx: mpsc::Sender<SystemicRiskOpportunity>) -> Result<()> {
        loop {
            self.update_oracle_cache().await?;
            
            let opportunities = self.scan_for_opportunities().await?;
            
            {
                let mut queue = self.opportunity_queue.write().unwrap();
                for opp in opportunities {
                    if opp.estimated_profit > MIN_PROFIT_THRESHOLD {
                        queue.push(opp.clone());
                        let _ = tx.send(opp).await;
                    }
                }
            }
            
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

#[derive(Clone)]
struct SolendPosition {
    collateral: CollateralInfo,
    borrowed_value: u64,
}

struct KaminoRiskMetrics {
    health: f64,
    is_liquidatable: bool,
    liquidation_value: u64,
    priority_score: f64,
    collateral: CollateralInfo,
}

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

impl SystemicRiskMonetizer {
    pub fn get_pending_opportunities(&self) -> Vec<SystemicRiskOpportunity> {
        let queue = self.opportunity_queue.read().unwrap();
        let mut opportunities = Vec::new();
        let now = Instant::now();
        
        for opp in queue.iter() {
            if opp.expiry > now {
                opportunities.push(opp.clone());
            }
        }
        
        opportunities.sort_by(|a, b| b.execution_priority.partial_cmp(&a.execution_priority).unwrap());
        opportunities.truncate(10);
        opportunities
    }

    pub fn clear_expired_opportunities(&self) {
        let mut queue = self.opportunity_queue.write().unwrap();
        let now = Instant::now();
        let valid_opps: Vec<_> = queue.drain().filter(|opp| opp.expiry > now).collect();
        
        for opp in valid_opps {
            queue.push(opp);
        }
    }

    pub async fn calculate_systemic_risk_score(&self) -> f64 {
        let opportunities = self.get_pending_opportunities();
        let mut total_risk = 0.0;
        
        for opp in opportunities {
            match opp.risk_type {
                RiskType::CascadingLiquidation { chain_length } => {
                    total_risk += (chain_length as f64) * (opp.estimated_profit as f64) / 1e9;
                }
                RiskType::Liquidation { health_ratio } => {
                    total_risk += (1.0 - health_ratio) * (opp.estimated_profit as f64) / 1e9;
                }
                RiskType::OracleDeviation { deviation_pct } => {
                    total_risk += deviation_pct * (opp.estimated_profit as f64) / 1e9;
                }
                RiskType::ProtocolInsolvency { deficit } => {
                    total_risk += (deficit as f64) / 1e12;
                }
            }
        }
        
        total_risk.min(100.0)
    }

    pub fn validate_opportunity(&self, opportunity: &SystemicRiskOpportunity) -> bool {
        if opportunity.expiry < Instant::now() {
            return false;
        }
        
        if opportunity.estimated_profit < MIN_PROFIT_THRESHOLD {
            return false;
        }
        
        let oracle_cache = self.oracle_cache.read().unwrap();
        for (mint, oracle_data) in &opportunity.oracle_prices {
            if let Some(cached_data) = oracle_cache.get(mint) {
                let price_deviation = (oracle_data.price - cached_data.price).abs() / cached_data.price;
                if price_deviation > 0.05 {
                    return false;
                }
            }
        }
        
        true
    }

    pub async fn emergency_shutdown(&self) -> Result<()> {
        self.opportunity_queue.write().unwrap().clear();
        self.exposure_tracker.write().unwrap().clear();
        Ok(())
    }

    pub fn get_protocol_exposure(&self, protocol: &ProtocolType) -> u64 {
        *self.exposure_tracker.read().unwrap().get(protocol).unwrap_or(&0)
    }

    pub fn get_total_exposure(&self) -> u64 {
        self.exposure_tracker.read().unwrap().values().sum()
    }

    pub async fn rebalance_exposure(&self) -> Result<()> {
        let mut tracker = self.exposure_tracker.write().unwrap();
        let total_exposure = tracker.values().sum::<u64>();
        
        if total_exposure > MAX_EXPOSURE_PER_PROTOCOL * 3 {
            for (_, exposure) in tracker.iter_mut() {
                *exposure = (*exposure * 7) / 10;
            }
        }
        
        Ok(())
    }

    fn calculate_slippage_adjusted_profit(&self, base_profit: u64, liquidity: f64) -> u64 {
        let slippage_factor = 1.0 - (MAX_SLIPPAGE_BPS as f64 / 10000.0);
        let liquidity_adjustment = (liquidity / (liquidity + base_profit as f64)).min(1.0);
        (base_profit as f64 * slippage_factor * liquidity_adjustment) as u64
    }

    pub fn prioritize_opportunities(&self, opportunities: &mut Vec<SystemicRiskOpportunity>) {
        opportunities.sort_by(|a, b| {
            let a_score = self.calculate_opportunity_score(a);
            let b_score = self.calculate_opportunity_score(b);
            b_score.partial_cmp(&a_score).unwrap()
        });
    }

    fn calculate_opportunity_score(&self, opp: &SystemicRiskOpportunity) -> f64 {
        let profit_score = (opp.estimated_profit as f64) / 1e9;
        let urgency_score = match &opp.risk_type {
            RiskType::Liquidation { health_ratio } => (1.1 - health_ratio) * 10.0,
            RiskType::CascadingLiquidation { chain_length } => *chain_length as f64 * 2.0,
            RiskType::OracleDeviation { deviation_pct } => deviation_pct * 5.0,
            RiskType::ProtocolInsolvency { .. } => 20.0,
        };
        
        let time_factor = (opp.expiry.duration_since(Instant::now()).as_secs() as f64 / 30.0).min(1.0);
        profit_score * urgency_score * time_factor
    }

    pub async fn monitor_oracle_deviations(&self) -> Result<Vec<(Pubkey, f64)>> {
        self.update_oracle_cache().await?;
        let cache = self.oracle_cache.read().unwrap();
        let mut deviations = Vec::new();
        
        for (mint, data) in cache.iter() {
            if data.confidence / data.price > ORACLE_CONFIDENCE_THRESHOLD {
                deviations.push((*mint, data.confidence / data.price));
            }
        }
        
        deviations.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap());
        Ok(deviations)
    }

    pub fn estimate_gas_cost(&self, opportunity: &SystemicRiskOpportunity) -> u64 {
        let base_cost = PRIORITY_FEE_MICROLAMPORTS * COMPUTE_UNITS as u64 / 1_000_000;
        let account_cost = opportunity.affected_accounts.len() as u64 * 5000;
        base_cost + account_cost
    }

    pub fn should_execute(&self, opportunity: &SystemicRiskOpportunity) -> bool {
        let gas_cost = self.estimate_gas_cost(opportunity);
        let net_profit = opportunity.estimated_profit.saturating_sub(gas_cost);
        
        if net_profit < MIN_PROFIT_THRESHOLD {
            return false;
        }
        
        let exposure = self.get_protocol_exposure(&opportunity.protocol);
        if exposure + opportunity.estimated_profit > MAX_EXPOSURE_PER_PROTOCOL {
            return false;
        }
        
        self.validate_opportunity(opportunity)
    }
}

pub struct RiskMonitor {
    monetizer: Arc<SystemicRiskMonetizer>,
    execution_channel: mpsc::Sender<SystemicRiskOpportunity>,
}

impl RiskMonitor {
    pub fn new(monetizer: Arc<SystemicRiskMonetizer>, tx: mpsc::Sender<SystemicRiskOpportunity>) -> Self {
        Self {
            monetizer,
            execution_channel: tx,
        }
    }

    pub async fn start(&self) -> Result<()> {
        let monetizer = self.monetizer.clone();
        let tx = self.execution_channel.clone();
        
        tokio::spawn(async move {
            loop {
                if let Err(e) = monetizer.run_monitoring_loop(tx.clone()).await {
                    eprintln!("Monitoring error: {:?}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        });

        tokio::spawn({
            let monetizer = self.monetizer.clone();
            async move {
                loop {
                    monetizer.clear_expired_opportunities();
                    if let Err(e) = monetizer.rebalance_exposure().await {
                        eprintln!("Rebalancing error: {:?}", e);
                    }
                    tokio::time::sleep(Duration::from_secs(60)).await;
                }
            }
        });

        Ok(())
    }
}

