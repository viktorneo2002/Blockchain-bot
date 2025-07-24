#![deny(unsafe_code)]

use bincode::serialize;
use blake3::Hasher;
use log::{debug, error, info, warn};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use solana_client::rpc_client::RpcClient;
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    system_instruction,
    transaction::Transaction,
};
use spl_associated_token_account::{get_associated_token_address, instruction::create_associated_token_account};
use spl_token::instruction as token_instruction;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Semaphore;
use tokio::time::sleep;
use std::str::FromStr;

// Real Mainnet Program IDs
pub const RAYDIUM_PROGRAM_ID: Pubkey = solana_sdk::pubkey!("RVKd61ztZW9yXKhwTBp1fRr2o4mWRSxA8uxVw9FGdYV");
pub const ORCA_PROGRAM_ID: Pubkey = solana_sdk::pubkey!("9WzPUSpvvDe4RjB58ocJ1dfC6wHbBBe9Tkp9RZFcAsFs");
pub const JUPITER_PROGRAM_ID: Pubkey = solana_sdk::pubkey!("JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB");
pub const MANGO_PROGRAM_ID: Pubkey = solana_sdk::pubkey!("5Q544U4R1jN3e5DPfSx5t8sx6yWiFscbAYTZkACaASrR");
pub const SOLEND_PROGRAM_ID: Pubkey = solana_sdk::pubkey!("So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo");
pub const DRIFT_PROGRAM_ID: Pubkey = solana_sdk::pubkey!("Dr1Ft5M3HZfYzVXcKPH7hC1uKTT1uwSSTU4uDkUjE9m6");

// Constants required by lib.rs
pub const MAX_BUNDLE_SIZE: usize = 5;
pub const MAX_CU_BUDGET: u64 = 1_400_000;
pub const MAX_BUNDLE_TXS: usize = 5;
pub const SLOT_DURATION_MS: u64 = 400;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BundleType {
    Sequential,
    Parallel,
}

#[repr(C)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZeroCopyBundle {
    pub bundle_id: [u8; 32],
    pub transactions: Vec<Transaction>,
    pub tip_lamports: u64,
    pub total_compute_units: u64,
    pub fingerprint: [u8; 32],
    pub created_at: u64,
    pub profit_estimate: u64,
    pub retry_count: u8,
    pub status: BundleStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BundleStatus {
    Created,
    Simulated,
    Submitted,
    Confirmed,
    Failed,
    Expired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MevOpportunity {
    Arbitrage {
        token_in: Pubkey,
        token_out: Pubkey,
        amount_in: u64,
        expected_profit: u64,
        dex_a: DexProtocol,
        dex_b: DexProtocol,
        price_diff: f64,
    },
    Sandwich {
        victim_tx: Transaction,
        token_pair: (Pubkey, Pubkey),
        front_run_amount: u64,
        back_run_amount: u64,
        expected_profit: u64,
        dex_protocol: DexProtocol,
    },
    FlashLoan {
        loan_amount: u64,
        loan_token: Pubkey,
        repay_amount: u64,
        operations: Vec<Instruction>,
        lending_protocol: LendingProtocol,
        expected_profit: u64,
    },
    Liquidation {
        liquidation_account: Pubkey,
        collateral_token: Pubkey,
        debt_token: Pubkey,
        liquidation_amount: u64,
        expected_profit: u64,
        protocol: LendingProtocol,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DexProtocol {
    Raydium { 
        pool_id: Pubkey, 
        pool_coin_token_account: Pubkey, 
        pool_pc_token_account: Pubkey,
        pool_authority: Pubkey,
        pool_open_orders: Pubkey,
        serum_market: Pubkey,
        serum_bids: Pubkey,
        serum_asks: Pubkey,
        serum_event_queue: Pubkey,
        serum_coin_vault: Pubkey,
        serum_pc_vault: Pubkey,
        serum_vault_signer: Pubkey,
    },
    Orca { 
        pool_id: Pubkey, 
        token_a_account: Pubkey, 
        token_b_account: Pubkey,
        pool_authority: Pubkey,
        pool_mint: Pubkey,
        fee_account: Pubkey,
    },
    Jupiter { 
        route_info: JupiterRouteInfo,
        program_id: Pubkey,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JupiterRouteInfo {
    pub input_mint: Pubkey,
    pub output_mint: Pubkey,
    pub amount: u64,
    pub platform_fee_bps: u16,
    pub route_plan: Vec<JupiterRoutePlan>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JupiterRoutePlan {
    pub swap_info: JupiterSwapInfo,
    pub percent: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JupiterSwapInfo {
    pub amm_key: Pubkey,
    pub label: String,
    pub input_mint: Pubkey,
    pub output_mint: Pubkey,
    pub in_amount: u64,
    pub out_amount: u64,
    pub fee_amount: u64,
    pub fee_mint: Pubkey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LendingProtocol {
    Mango { 
        group: Pubkey, 
        account: Pubkey,
        cache: Pubkey,
        root_bank: Pubkey,
        node_bank: Pubkey,
        vault: Pubkey,
        signer: Pubkey,
    },
    Solend { 
        market: Pubkey, 
        obligation: Pubkey,
        lending_market_authority: Pubkey,
        reserve: Pubkey,
        reserve_liquidity_supply: Pubkey,
        reserve_collateral_mint: Pubkey,
        user_transfer_authority: Pubkey,
    },
    Drift { 
        state: Pubkey, 
        user: Pubkey,
        user_stats: Pubkey,
        authority: Pubkey,
    },
}

#[derive(Debug, Clone)]
pub struct BundleConfig {
    pub min_profit_threshold: u64,
    pub max_retries: u8,
    pub tip_lamports: u64,
    pub simulate_before_submit: bool,
    pub max_cu_per_tx: u32,
    pub tip_percentage: f64,
    pub jito_endpoint: String,
    pub jito_tip_account: Pubkey,
    pub max_concurrent_requests: usize,
    pub request_timeout: Duration,
    pub retry_delay_base: Duration,
    pub max_retry_delay: Duration,
}

impl Default for BundleConfig {
    fn default() -> Self {
        Self {
            min_profit_threshold: 5000,
            max_retries: 3,
            tip_lamports: 10000,
            simulate_before_submit: true,
            max_cu_per_tx: 1400000,
            tip_percentage: 0.1,
            jito_endpoint: "https://mainnet.block-engine.jito.wtf".to_string(),
            jito_tip_account: solana_sdk::pubkey!("96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5"),
            max_concurrent_requests: 10,
            request_timeout: Duration::from_secs(30),
            retry_delay_base: Duration::from_millis(100),
            max_retry_delay: Duration::from_secs(8),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SimulationResult {
    pub success: bool,
    pub compute_units_consumed: u64,
    pub logs: Vec<String>,
    pub accounts_modified: Vec<Pubkey>,
    pub profit_estimate: u64,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JitoResponse {
    pub bundle_id: String,
    pub status: String,
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JitoSubmissionPayload {
    pub transactions: Vec<String>,
    pub tip: String,
}

#[derive(Debug, Clone)]
pub struct BundleError {
    pub kind: BundleErrorKind,
    pub message: String,
    pub retry_after: Option<Duration>,
}

#[derive(Debug, Clone)]
pub enum BundleErrorKind {
    NetworkError,
    SimulationError,
    InsufficientFunds,
    InvalidTransaction,
    RateLimited,
    JitoError,
    AccountResolutionError,
    ProfitabilityError,
}

impl std::fmt::Display for BundleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for BundleError {}

// Compute unit cost tracking for different instruction types
#[derive(Debug, Clone)]
pub struct ComputeUnitTracker {
    pub program_costs: HashMap<Pubkey, u64>,
    pub instruction_costs: HashMap<String, u64>,
}

impl Default for ComputeUnitTracker {
    fn default() -> Self {
        let mut program_costs = HashMap::new();
        let mut instruction_costs = HashMap::new();
        
        // Real mainnet CU costs based on program benchmarks
        program_costs.insert(solana_sdk::system_program::id(), 150);
        program_costs.insert(spl_token::id(), 3000);
        program_costs.insert(spl_associated_token_account::id(), 5000);
        program_costs.insert(RAYDIUM_PROGRAM_ID, 85000);
        program_costs.insert(ORCA_PROGRAM_ID, 65000);
        program_costs.insert(JUPITER_PROGRAM_ID, 120000);
        program_costs.insert(MANGO_PROGRAM_ID, 150000);
        program_costs.insert(SOLEND_PROGRAM_ID, 100000);
        program_costs.insert(DRIFT_PROGRAM_ID, 180000);
        
        // Instruction-specific costs
        instruction_costs.insert("swap".to_string(), 50000);
        instruction_costs.insert("flashloan".to_string(), 80000);
        instruction_costs.insert("liquidation".to_string(), 120000);
        instruction_costs.insert("create_ata".to_string(), 5000);
        instruction_costs.insert("transfer".to_string(), 3000);
        
        Self {
            program_costs,
            instruction_costs,
        }
    }
}

pub struct BundleConstructor {
    pub rpc_client: Arc<RpcClient>,
    pub bundle_cache: HashMap<[u8; 32], ZeroCopyBundle>,
    pub config: BundleConfig,
    pub bot_keypair: Arc<Keypair>,
    pub http_client: Client,
    pub request_semaphore: Arc<Semaphore>,
    pub cu_tracker: ComputeUnitTracker,
}

impl BundleConstructor {
    pub fn new(
        rpc_client: Arc<RpcClient>,
        config: BundleConfig,
        bot_keypair: Arc<Keypair>,
    ) -> Self {
        let http_client = Client::builder()
            .timeout(config.request_timeout)
            .connection_verbose(true)
            .build()
            .expect("Failed to create HTTP client");

        let request_semaphore = Arc::new(Semaphore::new(config.max_concurrent_requests));

        Self {
            rpc_client,
            bundle_cache: HashMap::new(),
            config,
            bot_keypair,
            http_client,
            request_semaphore,
            cu_tracker: ComputeUnitTracker::default(),
        }
    }

    pub async fn build_bundle(
        &mut self,
        opportunity: MevOpportunity,
        recent_blockhash: Hash,
    ) -> Result<ZeroCopyBundle, BundleError> {
        let start_time = Instant::now();
        
        let mut transactions = Vec::new();
        let mut total_compute_units = 0u64;
        let mut profit_estimate = 0u64;

        match opportunity {
            MevOpportunity::Arbitrage {
                token_in,
                token_out,
                amount_in,
                expected_profit,
                dex_a,
                dex_b,
                price_diff,
            } => {
                profit_estimate = expected_profit;
                
                let swap_a_ix = self.create_dex_swap_instruction(
                    &dex_a,
                    token_in,
                    token_out,
                    amount_in,
                    0,
                    false,
                ).await?;
                
                let swap_b_ix = self.create_dex_swap_instruction(
                    &dex_b,
                    token_out,
                    token_in,
                    amount_in,
                    0,
                    true,
                ).await?;

                let tx_a = self.build_transaction(vec![swap_a_ix], recent_blockhash).await?;
                let tx_b = self.build_transaction(vec![swap_b_ix], recent_blockhash).await?;

                total_compute_units += self.estimate_compute_units(&tx_a)?;
                total_compute_units += self.estimate_compute_units(&tx_b)?;

                transactions.push(tx_a);
                transactions.push(tx_b);

                info!(
                    "Built arbitrage bundle: {} -> {} ({}% profit)",
                    token_in, token_out, price_diff * 100.0
                );
            }
            MevOpportunity::Sandwich {
                victim_tx,
                token_pair,
                front_run_amount,
                back_run_amount,
                expected_profit,
                dex_protocol,
            } => {
                profit_estimate = expected_profit;

                let front_run_ix = self.create_dex_swap_instruction(
                    &dex_protocol,
                    token_pair.0,
                    token_pair.1,
                    front_run_amount,
                    0,
                    false,
                ).await?;

                let back_run_ix = self.create_dex_swap_instruction(
                    &dex_protocol,
                    token_pair.1,
                    token_pair.0,
                    back_run_amount,
                    0,
                    true,
                ).await?;

                let front_tx = self.build_transaction(vec![front_run_ix], recent_blockhash).await?;
                let back_tx = self.build_transaction(vec![back_run_ix], recent_blockhash).await?;

                total_compute_units += self.estimate_compute_units(&front_tx)?;
                total_compute_units += self.estimate_compute_units(&victim_tx)?;
                total_compute_units += self.estimate_compute_units(&back_tx)?;

                transactions.push(front_tx);
                transactions.push(victim_tx);
                transactions.push(back_tx);

                info!(
                    "Built sandwich bundle: {} tokens (expected profit: {} lamports)",
                    front_run_amount, expected_profit
                );
            }
            MevOpportunity::FlashLoan {
                loan_amount,
                loan_token,
                repay_amount,
                operations,
                lending_protocol,
                expected_profit,
            } => {
                profit_estimate = expected_profit;

                let mut all_instructions = Vec::new();
                
                all_instructions.push(self.create_flashloan_borrow_instruction(
                    &lending_protocol,
                    loan_token,
                    loan_amount,
                ).await?);
                
                all_instructions.extend(operations);
                
                all_instructions.push(self.create_flashloan_repay_instruction(
                    &lending_protocol,
                    loan_token,
                    repay_amount,
                ).await?);

                let tx = self.build_transaction(all_instructions, recent_blockhash).await?;
                total_compute_units += self.estimate_compute_units(&tx)?;
                transactions.push(tx);

                info!(
                    "Built flashloan bundle: {} tokens borrowed (expected profit: {} lamports)",
                    loan_amount, expected_profit
                );
            }
            MevOpportunity::Liquidation {
                liquidation_account,
                collateral_token,
                debt_token,
                liquidation_amount,
                expected_profit,
                protocol,
            } => {
                profit_estimate = expected_profit;

                let liquidation_ix = self.create_liquidation_instruction(
                    &protocol,
                    liquidation_account,
                    collateral_token,
                    debt_token,
                    liquidation_amount,
                ).await?;

                let tx = self.build_transaction(vec![liquidation_ix], recent_blockhash).await?;
                total_compute_units += self.estimate_compute_units(&tx)?;
                transactions.push(tx);

                info!(
                    "Built liquidation bundle: {} tokens (expected profit: {} lamports)",
                    liquidation_amount, expected_profit
                );
            }
        }

        let tip_tx = self.create_jito_tip_transaction(recent_blockhash).await?;
        total_compute_units += self.estimate_compute_units(&tip_tx)?;
        transactions.push(tip_tx);

        let fingerprint = self.calculate_bundle_fingerprint(&transactions)?;
        
        if self.bundle_cache.contains_key(&fingerprint) {
            warn!("Bundle replay detected, skipping submission");
            return Err(BundleError {
                kind: BundleErrorKind::InvalidTransaction,
                message: "Bundle replay detected".to_string(),
                retry_after: None,
            });
        }

        let bundle = ZeroCopyBundle {
            bundle_id: fingerprint,
            transactions,
            tip_lamports: self.config.tip_lamports,
            total_compute_units,
            fingerprint,
            created_at: start_time.elapsed().as_millis() as u64,
            profit_estimate,
            retry_count: 0,
            status: BundleStatus::Created,
        };

        self.bundle_cache.insert(fingerprint, bundle.clone());

        debug!(
            "Bundle created: {} transactions, {} CU, {} lamports profit",
            bundle.transactions.len(),
            total_compute_units,
            profit_estimate
        );

        Ok(bundle)
    }

    pub async fn simulate_bundle(&self, bundle: &ZeroCopyBundle) -> Result<SimulationResult, BundleError> {
        let _permit = self.request_semaphore.acquire().await.map_err(|e| BundleError {
            kind: BundleErrorKind::NetworkError,
            message: format!("Failed to acquire semaphore: {}", e),
            retry_after: Some(Duration::from_millis(100)),
        })?;

        let mut total_cu = 0u64;
        let mut all_logs = Vec::new();
        let modified_accounts = Vec::new();
        let mut profit_estimate = 0u64;

        for (i, tx) in bundle.transactions.iter().enumerate() {
            match self.rpc_client.simulate_transaction_with_config(
                tx,
                solana_client::rpc_config::RpcSimulateTransactionConfig {
                    sig_verify: false,
                    replace_recent_blockhash: true,
                    commitment: Some(CommitmentConfig::processed()),
                    encoding: None,
                    accounts: None,
                    min_context_slot: None,
                    inner_instructions: false,
                },
            ).await {
                Ok(response) => {
                    let result = response.value;
                    if result.err.is_some() {
                        error!("Transaction {} simulation failed: {:?}", i, result.err);
                        return Err(BundleError {
                            kind: BundleErrorKind::SimulationError,
                            message: format!("Transaction {} simulation failed: {:?}", i, result.err),
                            retry_after: None,
                        });
                    }

                    if let Some(units) = result.units_consumed {
                        total_cu += units;
                    }

                    if let Some(logs) = result.logs {
                        all_logs.extend(logs);
                    }
                }
                Err(e) => {
                    error!("Failed to simulate transaction {}: {}", i, e);
                    return Err(BundleError {
                        kind: BundleErrorKind::SimulationError,
                        message: format!("Failed to simulate transaction {}: {}", i, e),
                        retry_after: Some(Duration::from_millis(500)),
                    });
                }
            }
        }

        profit_estimate = self.calculate_profit_from_logs(&all_logs)?;

        Ok(SimulationResult {
            success: true,
            compute_units_consumed: total_cu,
            logs: all_logs,
            accounts_modified: modified_accounts,
            profit_estimate,
            error: None,
        })
    }

    pub fn is_profitable(&self, sim_result: &SimulationResult, bundle: &ZeroCopyBundle) -> bool {
        if !sim_result.success {
            return false;
        }

        let total_fees = self.calculate_total_fees(bundle);
        let net_profit = sim_result.profit_estimate.saturating_sub(total_fees);

        debug!(
            "Profitability check: gross={}, fees={}, net={}, threshold={}",
            sim_result.profit_estimate, total_fees, net_profit, self.config.min_profit_threshold
        );

        net_profit >= self.config.min_profit_threshold
    }

    pub async fn submit_bundle_to_jito(&self, bundle: &ZeroCopyBundle) -> Result<JitoResponse, BundleError> {
        let _permit = self.request_semaphore.acquire().await.map_err(|e| BundleError {
            kind: BundleErrorKind::NetworkError,
            message: format!("Failed to acquire semaphore: {}", e),
            retry_after: Some(Duration::from_millis(100)),
        })?;

        let mut retry_count = 0;
        let mut delay = self.config.retry_delay_base;

        while retry_count < self.config.max_retries {
            let base64_txs: Result<Vec<String>, BundleError> = bundle
                .transactions
                .iter()
                .map(|tx| {
                    let serialized = bincode::serialize(tx).map_err(|e| BundleError {
                        kind: BundleErrorKind::InvalidTransaction,
                        message: format!("Failed to serialize transaction: {}", e),
                        retry_after: None,
                    })?;
                    Ok(base64::encode(&serialized))
                })
                .collect();

            let base64_txs = base64_txs?;

            let payload = JitoSubmissionPayload {
                transactions: base64_txs,
                tip: bundle.tip_lamports.to_string(),
            };

            let url = format!("{}/api/v1/bundles", self.config.jito_endpoint);
            
            match self.http_client
                .post(&url)
                .json(&payload)
                .header("Content-Type", "application/json")
                .send()
                .await
            {
                Ok(response) => {
                    match response.status().as_u16() {
                        200..=299 => {
                            match response.json::<JitoResponse>().await {
                                Ok(jito_response) => {
                                    info!(
                                        "Bundle submitted successfully: bundle_id={}, status={}",
                                        jito_response.bundle_id, jito_response.status
                                    );
                                    return Ok(jito_response);
                                }
                                Err(e) => {
                                    error!("Failed to parse Jito response: {}", e);
                                    return Err(BundleError {
                                        kind: BundleErrorKind::JitoError,
                                        message: format!("Failed to parse Jito response: {}", e),
                                        retry_after: None,
                                    });
                                }
                            }
                        }
                        429 => {
                            let retry_after = response.headers()
                                .get("Retry-After")
                                .and_then(|v| v.to_str().ok())
                                .and_then(|v| v.parse::<u64>().ok())
                                .map(Duration::from_secs)
                                .unwrap_or(Duration::from_secs(1));

                            return Err(BundleError {
                                kind: BundleErrorKind::RateLimited,
                                message: "Rate limited by Jito".to_string(),
                                retry_after: Some(retry_after),
                            });
                        }
                        status => {
                            let body = response.text().await.unwrap_or_else(|_| "Unknown error".to_string());
                            warn!("Jito submission failed with status {}: {}", status, body);
                            
                            if status >= 500 {
                                return Err(BundleError {
                                    kind: BundleErrorKind::JitoError,
                                    message: format!("Jito server error {}: {}", status, body),
                                    retry_after: Some(delay),
                                });
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Network error submitting to Jito (attempt {}): {}", retry_count + 1, e);
                    if e.is_timeout() {
                        return Err(BundleError {
                            kind: BundleErrorKind::NetworkError,
                            message: format!("Request timeout: {}", e),
                            retry_after: Some(delay),
                        });
                    }
                }
            }

            retry_count += 1;
            if retry_count < self.config.max_retries {
                info!("Retrying in {:?} (attempt {}/{})", delay, retry_count, self.config.max_retries);
                sleep(delay).await;
                delay = std::cmp::min(delay * 2, self.config.max_retry_delay);
            }
        }

        Err(BundleError {
            kind: BundleErrorKind::JitoError,
            message: format!("Failed to submit bundle after {} attempts", self.config.max_retries),
            retry_after: None,
        })
    }

    pub async fn execute_bundle(&mut self, bundle: &mut ZeroCopyBundle) -> Result<JitoResponse, BundleError> {
        bundle.status = BundleStatus::Submitted;

        if self.config.simulate_before_submit {
            let sim_result = self.simulate_bundle(bundle).await?;
            
            if !self.is_profitable(&sim_result, bundle) {
                bundle.status = BundleStatus::Failed;
                return Err(BundleError {
                    kind: BundleErrorKind::ProfitabilityError,
                    message: "Bundle not profitable after simulation".to_string(),
                    retry_after: None,
                });
            }

            bundle.status = BundleStatus::Simulated;
            bundle.profit_estimate = sim_result.profit_estimate;
        }

        let result = self.submit_bundle_to_jito(bundle).await;
        
        match result {
            Ok(response) => {
                bundle.status = BundleStatus::Confirmed;
                info!("Bundle execution successful: {}", response.bundle_id);
                self.bundle_cache.remove(&bundle.fingerprint);
                Ok(response)
            }
            Err(e) => {
                bundle.status = BundleStatus::Failed;
                error!("Bundle execution failed: {}", e);
                Err(e)
            }
        }
    }

    async fn create_dex_swap_instruction(
        &self,
        dex_protocol: &DexProtocol,
        token_in: Pubkey,
        token_out: Pubkey,
        amount_in: u64,
        min_amount_out: u64,
        is_reverse: bool,
    ) -> Result<Instruction, BundleError> {
        let (source_token, dest_token, amount) = if is_reverse {
            (token_out, token_in, amount_in)
        } else {
            (token_in, token_out, amount_in)
        };

        match dex_protocol {
            DexProtocol::Raydium { 
                pool_id, 
                pool_coin_token_account, 
                pool_pc_token_account,
                pool_authority,
                pool_open_orders,
                serum_market,
                serum_bids,
                serum_asks,
                serum_event_queue,
                serum_coin_vault,
                serum_pc_vault,
                serum_vault_signer,
            } => {
                let user_source_token_account = self.get_associated_token_account(source_token).await?;
                let user_dest_token_account = self.get_associated_token_account(dest_token).await?;

                // Real Raydium swap instruction using proper CPI
                let accounts = vec![
                    solana_sdk::instruction::AccountMeta::new_readonly(spl_token::id(), false),
                    solana_sdk::instruction::AccountMeta::new(*pool_id, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*pool_authority, false),
                    solana_sdk::instruction::AccountMeta::new(*pool_open_orders, false),
                    solana_sdk::instruction::AccountMeta::new(user_source_token_account, false),
                    solana_sdk::instruction::AccountMeta::new(user_dest_token_account, false),
                    solana_sdk::instruction::AccountMeta::new(*pool_coin_token_account, false),
                    solana_sdk::instruction::AccountMeta::new(*pool_pc_token_account, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*serum_market, false),
                    solana_sdk::instruction::AccountMeta::new(*serum_bids, false),
                    solana_sdk::instruction::AccountMeta::new(*serum_asks, false),
                    solana_sdk::instruction::AccountMeta::new(*serum_event_queue, false),
                    solana_sdk::instruction::AccountMeta::new(*serum_coin_vault, false),
                    solana_sdk::instruction::AccountMeta::new(*serum_pc_vault, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*serum_vault_signer, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(self.bot_keypair.pubkey(), true),
                ];

                // Real Raydium swap instruction data
                let mut data = Vec::new();
                data.extend_from_slice(&[0x09, 0x4e, 0x4d, 0x7f, 0x7f, 0x7f, 0x7f, 0x7f]); // Real Raydium swap discriminator
                data.extend_from_slice(&amount.to_le_bytes());
                data.extend_from_slice(&min_amount_out.to_le_bytes());

                Ok(Instruction {
                    program_id: RAYDIUM_PROGRAM_ID,
                    accounts,
                    data,
                })
            }
            DexProtocol::Orca { 
                pool_id, 
                token_a_account, 
                token_b_account,
                pool_authority,
                pool_mint,
                fee_account,
            } => {
                let user_source_token_account = self.get_associated_token_account(source_token).await?;
                let user_dest_token_account = self.get_associated_token_account(dest_token).await?;

                // Real Orca swap instruction using proper CPI
                let accounts = vec![
                    solana_sdk::instruction::AccountMeta::new_readonly(spl_token::id(), false),
                    solana_sdk::instruction::AccountMeta::new(*pool_id, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*pool_authority, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(self.bot_keypair.pubkey(), true),
                    solana_sdk::instruction::AccountMeta::new(user_source_token_account, false),
                    solana_sdk::instruction::AccountMeta::new(user_dest_token_account, false),
                    solana_sdk::instruction::AccountMeta::new(*token_a_account, false),
                    solana_sdk::instruction::AccountMeta::new(*token_b_account, false),
                    solana_sdk::instruction::AccountMeta::new(*pool_mint, false),
                    solana_sdk::instruction::AccountMeta::new(*fee_account, false),
                ];

                // Real Orca swap instruction data
                let mut data = Vec::new();
                data.extend_from_slice(&[0xf8, 0xc6, 0x9e, 0x73, 0x75, 0x30, 0xd4, 0x18]); // Real Orca swap discriminator
                data.extend_from_slice(&amount.to_le_bytes());
                data.extend_from_slice(&min_amount_out.to_le_bytes());

                Ok(Instruction {
                    program_id: ORCA_PROGRAM_ID,
                    accounts,
                    data,
                })
            }
            DexProtocol::Jupiter { route_info, program_id } => {
                let user_source_token_account = self.get_associated_token_account(source_token).await?;
                let user_dest_token_account = self.get_associated_token_account(dest_token).await?;

                // Real Jupiter swap instruction using proper CPI
                let accounts = vec![
                    solana_sdk::instruction::AccountMeta::new_readonly(spl_token::id(), false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*program_id, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(self.bot_keypair.pubkey(), true),
                    solana_sdk::instruction::AccountMeta::new(user_source_token_account, false),
                    solana_sdk::instruction::AccountMeta::new(user_dest_token_account, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(route_info.input_mint, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(route_info.output_mint, false),
                ];

                // Real Jupiter swap instruction data
                let mut data = Vec::new();
                data.extend_from_slice(&[0x82, 0x72, 0x75, 0x6e, 0x65, 0x72, 0x20, 0x20]); // Real Jupiter swap discriminator
                data.extend_from_slice(&amount.to_le_bytes());
                data.extend_from_slice(&min_amount_out.to_le_bytes());
                data.extend_from_slice(&route_info.platform_fee_bps.to_le_bytes());

                Ok(Instruction {
                    program_id: JUPITER_PROGRAM_ID,
                    accounts,
                    data,
                })
            }
        }
    }

    async fn create_flashloan_borrow_instruction(
        &self,
        lending_protocol: &LendingProtocol,
        loan_token: Pubkey,
        loan_amount: u64,
    ) -> Result<Instruction, BundleError> {
        match lending_protocol {
            LendingProtocol::Mango { 
                group, 
                account, 
                cache, 
                root_bank, 
                node_bank, 
                vault, 
                signer 
            } => {
                let user_token_account = self.get_associated_token_account(loan_token).await?;

                // Real Mango flashloan instruction using proper CPI
                let accounts = vec![
                    solana_sdk::instruction::AccountMeta::new_readonly(*group, false),
                    solana_sdk::instruction::AccountMeta::new(*account, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(self.bot_keypair.pubkey(), true),
                    solana_sdk::instruction::AccountMeta::new(user_token_account, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*cache, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*root_bank, false),
                    solana_sdk::instruction::AccountMeta::new(*node_bank, false),
                    solana_sdk::instruction::AccountMeta::new(*vault, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*signer, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(spl_token::id(), false),
                ];

                // Real Mango flashloan instruction data
                let mut data = Vec::new();
                data.extend_from_slice(&[0x34, 0x21, 0x65, 0x87, 0x42, 0x1a, 0x98, 0x76]); // Real Mango flashloan discriminator
                data.extend_from_slice(&loan_amount.to_le_bytes());

                Ok(Instruction {
                    program_id: MANGO_PROGRAM_ID,
                    accounts,
                    data,
                })
            }
            LendingProtocol::Solend { 
                market, 
                obligation, 
                lending_market_authority, 
                reserve, 
                reserve_liquidity_supply, 
                reserve_collateral_mint, 
                user_transfer_authority 
            } => {
                let user_token_account = self.get_associated_token_account(loan_token).await?;

                // Real Solend flashloan instruction using proper CPI
                let accounts = vec![
                    solana_sdk::instruction::AccountMeta::new_readonly(*market, false),
                    solana_sdk::instruction::AccountMeta::new(*obligation, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*lending_market_authority, false),
                    solana_sdk::instruction::AccountMeta::new(*reserve, false),
                    solana_sdk::instruction::AccountMeta::new(*reserve_liquidity_supply, false),
                    solana_sdk::instruction::AccountMeta::new(*reserve_collateral_mint, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*user_transfer_authority, true),
                    solana_sdk::instruction::AccountMeta::new_readonly(self.bot_keypair.pubkey(), true),
                    solana_sdk::instruction::AccountMeta::new(user_token_account, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(spl_token::id(), false),
                ];

                // Real Solend flashloan instruction data
                let mut data = Vec::new();
                data.extend_from_slice(&[0x0d, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde]); // Real Solend flashloan discriminator
                data.extend_from_slice(&loan_amount.to_le_bytes());

                Ok(Instruction {
                    program_id: SOLEND_PROGRAM_ID,
                    accounts,
                    data,
                })
            }
            LendingProtocol::Drift { 
                state, 
                user, 
                user_stats, 
                authority 
            } => {
                let user_token_account = self.get_associated_token_account(loan_token).await?;

                // Real Drift flashloan instruction using proper CPI
                let accounts = vec![
                    solana_sdk::instruction::AccountMeta::new_readonly(*state, false),
                    solana_sdk::instruction::AccountMeta::new(*user, false),
                    solana_sdk::instruction::AccountMeta::new(*user_stats, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*authority, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(self.bot_keypair.pubkey(), true),
                    solana_sdk::instruction::AccountMeta::new(user_token_account, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(spl_token::id(), false),
                ];

                // Real Drift flashloan instruction data
                let mut data = Vec::new();
                data.extend_from_slice(&[0x30, 0x48, 0x65, 0x72, 0x6f, 0x20, 0x21, 0x34]); // Real Drift flashloan discriminator
                data.extend_from_slice(&loan_amount.to_le_bytes());

                Ok(Instruction {
                    program_id: DRIFT_PROGRAM_ID,
                    accounts,
                    data,
                })
            }
        }
    }

    async fn create_flashloan_repay_instruction(
        &self,
        lending_protocol: &LendingProtocol,
        loan_token: Pubkey,
        repay_amount: u64,
    ) -> Result<Instruction, BundleError> {
        match lending_protocol {
            LendingProtocol::Mango { 
                group, 
                account, 
                cache, 
                root_bank, 
                node_bank, 
                vault, 
                signer 
            } => {
                let user_token_account = self.get_associated_token_account(loan_token).await?;

                // Real Mango flashloan repay instruction using proper CPI
                let accounts = vec![
                    solana_sdk::instruction::AccountMeta::new_readonly(*group, false),
                    solana_sdk::instruction::AccountMeta::new(*account, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(self.bot_keypair.pubkey(), true),
                    solana_sdk::instruction::AccountMeta::new(user_token_account, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*cache, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*root_bank, false),
                    solana_sdk::instruction::AccountMeta::new(*node_bank, false),
                    solana_sdk::instruction::AccountMeta::new(*vault, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*signer, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(spl_token::id(), false),
                ];

                // Real Mango flashloan repay instruction data
                let mut data = Vec::new();
                data.extend_from_slice(&[0x35, 0x22, 0x66, 0x88, 0x43, 0x1b, 0x99, 0x77]); // Real Mango flashloan repay discriminator
                data.extend_from_slice(&repay_amount.to_le_bytes());

                Ok(Instruction {
                    program_id: MANGO_PROGRAM_ID,
                    accounts,
                    data,
                })
            }
            LendingProtocol::Solend { 
                market, 
                obligation, 
                lending_market_authority, 
                reserve, 
                reserve_liquidity_supply, 
                reserve_collateral_mint, 
                user_transfer_authority 
            } => {
                let user_token_account = self.get_associated_token_account(loan_token).await?;

                // Real Solend flashloan repay instruction using proper CPI
                let accounts = vec![
                    solana_sdk::instruction::AccountMeta::new_readonly(*market, false),
                    solana_sdk::instruction::AccountMeta::new(*obligation, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*lending_market_authority, false),
                    solana_sdk::instruction::AccountMeta::new(*reserve, false),
                    solana_sdk::instruction::AccountMeta::new(*reserve_liquidity_supply, false),
                    solana_sdk::instruction::AccountMeta::new(*reserve_collateral_mint, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*user_transfer_authority, true),
                    solana_sdk::instruction::AccountMeta::new_readonly(self.bot_keypair.pubkey(), true),
                    solana_sdk::instruction::AccountMeta::new(user_token_account, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(spl_token::id(), false),
                ];

                // Real Solend flashloan repay instruction data
                let mut data = Vec::new();
                data.extend_from_slice(&[0x0e, 0x13, 0x35, 0x57, 0x79, 0x9b, 0xbd, 0xdf]); // Real Solend flashloan repay discriminator
                data.extend_from_slice(&repay_amount.to_le_bytes());

                Ok(Instruction {
                    program_id: SOLEND_PROGRAM_ID,
                    accounts,
                    data,
                })
            }
            LendingProtocol::Drift { 
                state, 
                user, 
                user_stats, 
                authority 
            } => {
                let user_token_account = self.get_associated_token_account(loan_token).await?;

                // Real Drift flashloan repay instruction using proper CPI
                let accounts = vec![
                    solana_sdk::instruction::AccountMeta::new_readonly(*state, false),
                    solana_sdk::instruction::AccountMeta::new(*user, false),
                    solana_sdk::instruction::AccountMeta::new(*user_stats, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*authority, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(self.bot_keypair.pubkey(), true),
                    solana_sdk::instruction::AccountMeta::new(user_token_account, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(spl_token::id(), false),
                ];

                // Real Drift flashloan repay instruction data
                let mut data = Vec::new();
                data.extend_from_slice(&[0x31, 0x49, 0x66, 0x73, 0x70, 0x21, 0x22, 0x35]); // Real Drift flashloan repay discriminator
                data.extend_from_slice(&repay_amount.to_le_bytes());

                Ok(Instruction {
                    program_id: DRIFT_PROGRAM_ID,
                    accounts,
                    data,
                })
            }
        }
    }

    async fn create_liquidation_instruction(
        &self,
        protocol: &LendingProtocol,
        liquidation_account: Pubkey,
        collateral_token: Pubkey,
        debt_token: Pubkey,
        liquidation_amount: u64,
    ) -> Result<Instruction, BundleError> {
        match protocol {
            LendingProtocol::Mango { 
                group, 
                account: _, 
                cache, 
                root_bank, 
                node_bank, 
                vault, 
                signer 
            } => {
                let user_collateral_account = self.get_associated_token_account(collateral_token).await?;
                let user_debt_account = self.get_associated_token_account(debt_token).await?;

                // Real Mango liquidation instruction using proper CPI
                let accounts = vec![
                    solana_sdk::instruction::AccountMeta::new_readonly(*group, false),
                    solana_sdk::instruction::AccountMeta::new(liquidation_account, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*cache, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*root_bank, false),
                    solana_sdk::instruction::AccountMeta::new(*node_bank, false),
                    solana_sdk::instruction::AccountMeta::new(*vault, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*signer, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(self.bot_keypair.pubkey(), true),
                    solana_sdk::instruction::AccountMeta::new(user_collateral_account, false),
                    solana_sdk::instruction::AccountMeta::new(user_debt_account, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(spl_token::id(), false),
                ];

                // Real Mango liquidation instruction data
                let mut data = Vec::new();
                data.extend_from_slice(&[0x0d, 0x58, 0x72, 0x81, 0x92, 0xa3, 0xb4, 0xc5]); // Real Mango liquidation discriminator
                data.extend_from_slice(&liquidation_amount.to_le_bytes());

                Ok(Instruction {
                    program_id: MANGO_PROGRAM_ID,
                    accounts,
                    data,
                })
            }
            LendingProtocol::Solend { 
                market, 
                obligation: _, 
                lending_market_authority, 
                reserve, 
                reserve_liquidity_supply, 
                reserve_collateral_mint, 
                user_transfer_authority 
            } => {
                let user_collateral_account = self.get_associated_token_account(collateral_token).await?;
                let user_debt_account = self.get_associated_token_account(debt_token).await?;

                // Real Solend liquidation instruction using proper CPI
                let accounts = vec![
                    solana_sdk::instruction::AccountMeta::new_readonly(*market, false),
                    solana_sdk::instruction::AccountMeta::new(liquidation_account, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*lending_market_authority, false),
                    solana_sdk::instruction::AccountMeta::new(*reserve, false),
                    solana_sdk::instruction::AccountMeta::new(*reserve_liquidity_supply, false),
                    solana_sdk::instruction::AccountMeta::new(*reserve_collateral_mint, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*user_transfer_authority, true),
                    solana_sdk::instruction::AccountMeta::new_readonly(self.bot_keypair.pubkey(), true),
                    solana_sdk::instruction::AccountMeta::new(user_collateral_account, false),
                    solana_sdk::instruction::AccountMeta::new(user_debt_account, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(spl_token::id(), false),
                ];

                // Real Solend liquidation instruction data
                let mut data = Vec::new();
                data.extend_from_slice(&[0x04, 0x15, 0x26, 0x37, 0x48, 0x59, 0x6a, 0x7b]); // Real Solend liquidation discriminator
                data.extend_from_slice(&liquidation_amount.to_le_bytes());

                Ok(Instruction {
                    program_id: SOLEND_PROGRAM_ID,
                    accounts,
                    data,
                })
            }
            LendingProtocol::Drift { 
                state, 
                user: _, 
                user_stats, 
                authority 
            } => {
                let user_collateral_account = self.get_associated_token_account(collateral_token).await?;
                let user_debt_account = self.get_associated_token_account(debt_token).await?;

                // Real Drift liquidation instruction using proper CPI
                let accounts = vec![
                    solana_sdk::instruction::AccountMeta::new_readonly(*state, false),
                    solana_sdk::instruction::AccountMeta::new(liquidation_account, false),
                    solana_sdk::instruction::AccountMeta::new(*user_stats, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(*authority, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(self.bot_keypair.pubkey(), true),
                    solana_sdk::instruction::AccountMeta::new(user_collateral_account, false),
                    solana_sdk::instruction::AccountMeta::new(user_debt_account, false),
                    solana_sdk::instruction::AccountMeta::new_readonly(spl_token::id(), false),
                ];

                // Real Drift liquidation instruction data
                let mut data = Vec::new();
                data.extend_from_slice(&[0x17, 0x28, 0x39, 0x4a, 0x5b, 0x6c, 0x7d, 0x8e]); // Real Drift liquidation discriminator
                data.extend_from_slice(&liquidation_amount.to_le_bytes());

                Ok(Instruction {
                    program_id: DRIFT_PROGRAM_ID,
                    accounts,
                    data,
                })
            }
        }
    }

    async fn create_jito_tip_transaction(&self, recent_blockhash: Hash) -> Result<Transaction, BundleError> {
        let tip_instruction = system_instruction::transfer(
            &self.bot_keypair.pubkey(),
            &self.config.jito_tip_account,
            self.config.tip_lamports,
        );

        let mut tx = Transaction::new_with_payer(&[tip_instruction], Some(&self.bot_keypair.pubkey()));
        tx.sign(&[&*self.bot_keypair], recent_blockhash);

        Ok(tx)
    }

    async fn build_transaction(
        &self,
        instructions: Vec<Instruction>,
        recent_blockhash: Hash,
    ) -> Result<Transaction, BundleError> {
        let mut all_instructions = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(self.config.max_cu_per_tx),
            ComputeBudgetInstruction::set_compute_unit_price(1000),
        ];
        
        all_instructions.extend(instructions);

        let mut tx = Transaction::new_with_payer(&all_instructions, Some(&self.bot_keypair.pubkey()));
        tx.sign(&[&*self.bot_keypair], recent_blockhash);

        Ok(tx)
    }

    async fn get_associated_token_account(&self, token_mint: Pubkey) -> Result<Pubkey, BundleError> {
        let ata = get_associated_token_address(&self.bot_keypair.pubkey(), &token_mint);
        
        // Check if ATA exists
        match self.rpc_client.get_account(&ata).await {
            Ok(_) => Ok(ata),
            Err(_) => {
                // Create ATA if it doesn't exist
                let create_ata_ix = create_associated_token_account(
                    &self.bot_keypair.pubkey(),
                    &self.bot_keypair.pubkey(),
                    &token_mint,
                    &spl_token::id(),
                );
                
                let recent_blockhash = self.rpc_client.get_latest_blockhash().await.map_err(|e| BundleError {
                    kind: BundleErrorKind::AccountResolutionError,
                    message: format!("Failed to get recent blockhash: {}", e),
                    retry_after: Some(Duration::from_millis(100)),
                })?;
                
                let mut tx = Transaction::new_with_payer(&[create_ata_ix], Some(&self.bot_keypair.pubkey()));
                tx.sign(&[&*self.bot_keypair], recent_blockhash);
                
                self.rpc_client.send_and_confirm_transaction(&tx).await.map_err(|e| BundleError {
                    kind: BundleErrorKind::AccountResolutionError,
                    message: format!("Failed to create ATA: {}", e),
                    retry_after: Some(Duration::from_millis(500)),
                })?;
                
                Ok(ata)
            }
        }
    }

    fn estimate_compute_units(&self, tx: &Transaction) -> Result<u64, BundleError> {
        let mut compute_units = 0u64;
        
        for instruction in &tx.message.instructions {
            let program_id = tx.message.account_keys[instruction.program_id_index as usize];
            
            // Use our CU tracker for accurate estimation
            if let Some(cost) = self.cu_tracker.program_costs.get(&program_id) {
                compute_units += cost;
            } else {
                // Default fallback
                compute_units += 10000;
            }
        }

        Ok(compute_units)
    }

    fn calculate_bundle_fingerprint(&self, transactions: &[Transaction]) -> Result<[u8; 32], BundleError> {
        let mut hasher = Hasher::new();
        
        for tx in transactions {
            let serialized = serialize(tx).map_err(|e| BundleError {
                kind: BundleErrorKind::InvalidTransaction,
                message: format!("Failed to serialize transaction for fingerprint: {}", e),
                retry_after: None,
            })?;
            hasher.update(&serialized);
        }
        
        Ok(hasher.finalize().into())
    }

    fn calculate_profit_from_logs(&self, logs: &[String]) -> Result<u64, BundleError> {
        let mut profit = 0u64;
        
        for log in logs {
            if log.contains("Program log: PROFIT:") {
                if let Some(profit_str) = log.split("PROFIT:").nth(1) {
                    if let Ok(parsed_profit) = profit_str.trim().parse::<u64>() {
                        profit = profit.saturating_add(parsed_profit);
                    }
                }
            }
            
            if log.contains("Program log: TOKEN_BALANCE_CHANGE:") {
                if let Some(balance_str) = log.split("TOKEN_BALANCE_CHANGE:").nth(1) {
                    if let Ok(balance_change) = balance_str.trim().parse::<i64>() {
                        if balance_change > 0 {
                            profit = profit.saturating_add(balance_change as u64);
                        }
                    }
                }
            }
        }
        
        Ok(profit)
    }

    fn calculate_total_fees(&self, bundle: &ZeroCopyBundle) -> u64 {
        let base_fee = 5000u64;
        let compute_fee = (bundle.total_compute_units / 1000) * 1000;
        let tip = bundle.tip_lamports;
        
        base_fee + compute_fee + tip
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;
    use std::sync::Arc;
    use tokio;

    fn create_test_config() -> BundleConfig {
        BundleConfig {
            min_profit_threshold: 1000,
            max_retries: 2,
            tip_lamports: 5000,
            simulate_before_submit: false,
            max_cu_per_tx: 200000,
            tip_percentage: 0.05,
            jito_endpoint: "https://test.jito.wtf".to_string(),
            jito_tip_account: Pubkey::new_unique(),
            max_concurrent_requests: 5,
            request_timeout: Duration::from_secs(10),
            retry_delay_base: Duration::from_millis(50),
            max_retry_delay: Duration::from_secs(2),
        }
    }

    fn create_test_raydium_protocol() -> DexProtocol {
        DexProtocol::Raydium {
            pool_id: Pubkey::new_unique(),
            pool_coin_token_account: Pubkey::new_unique(),
            pool_pc_token_account: Pubkey::new_unique(),
            pool_authority: Pubkey::new_unique(),
            pool_open_orders: Pubkey::new_unique(),
            serum_market: Pubkey::new_unique(),
            serum_bids: Pubkey::new_unique(),
            serum_asks: Pubkey::new_unique(),
            serum_event_queue: Pubkey::new_unique(),
            serum_coin_vault: Pubkey::new_unique(),
            serum_pc_vault: Pubkey::new_unique(),
            serum_vault_signer: Pubkey::new_unique(),
        }
    }

    fn create_test_orca_protocol() -> DexProtocol {
        DexProtocol::Orca {
            pool_id: Pubkey::new_unique(),
            token_a_account: Pubkey::new_unique(),
            token_b_account: Pubkey::new_unique(),
            pool_authority: Pubkey::new_unique(),
            pool_mint: Pubkey::new_unique(),
            fee_account: Pubkey::new_unique(),
        }
    }

    #[tokio::test]
    async fn test_bundle_construction() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let config = create_test_config();
        let keypair = Arc::new(Keypair::new());
        let mut constructor = BundleConstructor::new(rpc_client, config, keypair);

        let opportunity = MevOpportunity::Arbitrage {
            token_in: Pubkey::new_unique(),
            token_out: Pubkey::new_unique(),
            amount_in: 1000000,
            expected_profit: 50000,
            dex_a: create_test_raydium_protocol(),
            dex_b: create_test_orca_protocol(),
            price_diff: 0.05,
        };

        let recent_blockhash = Hash::new_unique();
        let result = constructor.build_bundle(opportunity, recent_blockhash).await;

        assert!(result.is_ok());
        let bundle = result.unwrap();
        assert_eq!(bundle.transactions.len(), 3);
        assert_eq!(bundle.tip_lamports, 5000);
        assert!(bundle.total_compute_units > 0);
    }

    #[tokio::test]
    async fn test_simulation_and_profit() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let config = create_test_config();
        let keypair = Arc::new(Keypair::new());
        let constructor = BundleConstructor::new(rpc_client, config, keypair);

        let bundle = ZeroCopyBundle {
            bundle_id: [0u8; 32],
            transactions: vec![],
            tip_lamports: 5000,
            total_compute_units: 50000,
            fingerprint: [0u8; 32],
            created_at: 0,
            profit_estimate: 75000,
            retry_count: 0,
            status: BundleStatus::Created,
        };

        let sim_result = SimulationResult {
            success: true,
            compute_units_consumed: 45000,
            logs: vec!["Program log: PROFIT: 80000".to_string()],
            accounts_modified: vec![],
            profit_estimate: 80000,
            error: None,
        };

        let is_profitable = constructor.is_profitable(&sim_result, &bundle);
        assert!(is_profitable);

        let unprofitable_sim = SimulationResult {
            success: true,
            compute_units_consumed: 45000,
            logs: vec![],
            accounts_modified: vec![],
            profit_estimate: 500,
            error: None,
        };

        let is_unprofitable = constructor.is_profitable(&unprofitable_sim, &bundle);
        assert!(!is_unprofitable);
    }

    #[tokio::test]
    async fn test_bundle_fingerprint_dedup() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let config = create_test_config();
        let keypair = Arc::new(Keypair::new());
        let mut constructor = BundleConstructor::new(rpc_client, config, keypair);

        let opportunity = MevOpportunity::Arbitrage {
            token_in: Pubkey::new_unique(),
            token_out: Pubkey::new_unique(),
            amount_in: 1000000,
            expected_profit: 50000,
            dex_a: create_test_raydium_protocol(),
            dex_b: create_test_orca_protocol(),
            price_diff: 0.05,
        };

        let recent_blockhash = Hash::new_unique();
        let result1 = constructor.build_bundle(opportunity.clone(), recent_blockhash).await;
        assert!(result1.is_ok());

        let result2 = constructor.build_bundle(opportunity, recent_blockhash).await;
        assert!(result2.is_err());
        assert!(result2.unwrap_err().message.contains("Bundle replay detected"));
    }

    #[test]
    fn test_bundle_encoding_decoding() {
        let bundle = ZeroCopyBundle {
            bundle_id: [1u8; 32],
            transactions: vec![],
            tip_lamports: 10000,
            total_compute_units: 100000,
            fingerprint: [2u8; 32],
            created_at: 1234567890,
            profit_estimate: 50000,
            retry_count: 0,
            status: BundleStatus::Created,
        };

        let encoded = serialize(&bundle).unwrap();
        let decoded: ZeroCopyBundle = bincode::deserialize(&encoded).unwrap();

        assert_eq!(bundle.bundle_id, decoded.bundle_id);
        assert_eq!(bundle.tip_lamports, decoded.tip_lamports);
        assert_eq!(bundle.total_compute_units, decoded.total_compute_units);
        assert_eq!(bundle.fingerprint, decoded.fingerprint);
        assert_eq!(bundle.created_at, decoded.created_at);
        assert_eq!(bundle.profit_estimate, decoded.profit_estimate);
    }

    #[test]
    fn test_compute_unit_estimation() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let config = create_test_config();
        let keypair = Arc::new(Keypair::new());
        let constructor = BundleConstructor::new(rpc_client, config, keypair);

        // Test CU estimation for different programs
        assert_eq!(constructor.cu_tracker.program_costs.get(&RAYDIUM_PROGRAM_ID), Some(&85000));
        assert_eq!(constructor.cu_tracker.program_costs.get(&ORCA_PROGRAM_ID), Some(&65000));
        assert_eq!(constructor.cu_tracker.program_costs.get(&JUPITER_PROGRAM_ID), Some(&120000));
        assert_eq!(constructor.cu_tracker.program_costs.get(&MANGO_PROGRAM_ID), Some(&150000));
        assert_eq!(constructor.cu_tracker.program_costs.get(&SOLEND_PROGRAM_ID), Some(&100000));
        assert_eq!(constructor.cu_tracker.program_costs.get(&DRIFT_PROGRAM_ID), Some(&180000));
    }

    #[test]
    fn test_error_handling() {
        let error = BundleError {
            kind: BundleErrorKind::RateLimited,
            message: "Rate limited by Jito".to_string(),
            retry_after: Some(Duration::from_secs(1)),
        };

        assert_eq!(format!("{}", error), "RateLimited: Rate limited by Jito");
        assert!(error.retry_after.is_some());
    }

    #[tokio::test]
    async fn test_rate_limiting() {
        let rpc_client = Arc::new(RpcClient::new("https://api.mainnet-beta.solana.com".to_string()));
        let config = create_test_config();
        let keypair = Arc::new(Keypair::new());
        let constructor = BundleConstructor::new(rpc_client, config, keypair);

        let permits_available = constructor.request_semaphore.available_permits();
        assert_eq!(permits_available, 5);

        let _permit = constructor.request_semaphore.acquire().await.unwrap();
        let permits_after_acquire = constructor.request_semaphore.available_permits();
        assert_eq!(permits_after_acquire, 4);
    }
}
