use anchor_lang::prelude::*;
use anchor_lang::solana_program::{
    clock::Clock,
    instruction::{AccountMeta, Instruction},
    program::invoke_signed,
    pubkey::Pubkey,
    rent::Rent,
    system_instruction,
    sysvar::{clock, rent},
};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcSendTransactionConfig, RpcSimulateTransactionConfig},
};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::{Transaction, VersionedTransaction},
};
use solana_transaction_status::UiTransactionEncoding;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, RwLock, Semaphore},
    time::{interval, sleep},
};

const SLOT_DURATION_MS: u64 = 400;
const SLOT_BOUNDARY_OFFSET_MS: i64 = -5;
const MAX_RETRIES: u8 = 3;
const PRIORITY_FEE_PERCENTILE: f64 = 0.99;
const COMPUTE_UNIT_LIMIT: u32 = 1_400_000;
const MAX_CONCURRENT_SIMULATIONS: usize = 10;
const SLOT_LEADER_SCHEDULE_CACHE_SIZE: usize = 432000;

#[derive(Clone)]
pub struct SlotBoundaryFlashLoanExecutor {
    rpc_client: Arc<RpcClient>,
    wallet: Arc<Keypair>,
    flashloan_program_id: Pubkey,
    slot_tracker: Arc<SlotTracker>,
    priority_fee_calculator: Arc<PriorityFeeCalculator>,
    simulation_semaphore: Arc<Semaphore>,
}

#[derive(Clone)]
struct SlotTracker {
    current_slot: Arc<AtomicU64>,
    slot_start_time: Arc<RwLock<Instant>>,
    leader_schedule: Arc<RwLock<Vec<Pubkey>>>,
    slot_hashes: Arc<RwLock<VecDeque<(u64, Hash)>>>,
}

#[derive(Clone)]
struct PriorityFeeCalculator {
    recent_fees: Arc<RwLock<Vec<u64>>>,
    base_priority_fee: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
pub struct FlashLoanRequest {
    pub token_mint: Pubkey,
    pub amount: u64,
    pub target_accounts: Vec<AccountMeta>,
    pub target_instructions: Vec<u8>,
    pub expected_profit: u64,
}

#[derive(Debug)]
pub struct ExecutionResult {
    pub signature: Signature,
    pub slot: u64,
    pub timing_offset_ms: i64,
    pub priority_fee: u64,
    pub profit: u64,
}

impl SlotBoundaryFlashLoanExecutor {
    pub async fn new(
        rpc_url: String,
        wallet: Keypair,
        flashloan_program_id: Pubkey,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let rpc_client = Arc::new(RpcClient::new_with_commitment(
            rpc_url,
            CommitmentConfig::processed(),
        ));

        let slot_tracker = Arc::new(SlotTracker {
            current_slot: Arc::new(AtomicU64::new(0)),
            slot_start_time: Arc::new(RwLock::new(Instant::now())),
            leader_schedule: Arc::new(RwLock::new(Vec::with_capacity(SLOT_LEADER_SCHEDULE_CACHE_SIZE))),
            slot_hashes: Arc::new(RwLock::new(VecDeque::with_capacity(150))),
        });

        let priority_fee_calculator = Arc::new(PriorityFeeCalculator {
            recent_fees: Arc::new(RwLock::new(Vec::with_capacity(1000))),
            base_priority_fee: Arc::new(AtomicU64::new(100_000)),
        });

        let executor = Self {
            rpc_client,
            wallet: Arc::new(wallet),
            flashloan_program_id,
            slot_tracker,
            priority_fee_calculator,
            simulation_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_SIMULATIONS)),
        };

        executor.start_slot_monitoring().await;
        executor.start_priority_fee_monitoring().await;
        executor.start_leader_schedule_refresh().await;

        Ok(executor)
    }

    async fn start_slot_monitoring(&self) {
        let slot_tracker = self.slot_tracker.clone();
        let rpc_client = self.rpc_client.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_millis(50));
            let mut last_slot = 0u64;

            loop {
                interval.tick().await;

                if let Ok(slot_info) = rpc_client.get_slot().await {
                    let current_slot = slot_info;
                    
                    if current_slot > last_slot {
                        slot_tracker.current_slot.store(current_slot, Ordering::Release);
                        *slot_tracker.slot_start_time.write().await = Instant::now();
                        last_slot = current_slot;

                        if let Ok(recent_blockhash) = rpc_client.get_latest_blockhash().await {
                            let mut slot_hashes = slot_tracker.slot_hashes.write().await;
                            slot_hashes.push_back((current_slot, recent_blockhash));
                            if slot_hashes.len() > 150 {
                                slot_hashes.pop_front();
                            }
                        }
                    }
                }
            }
        });
    }

    async fn start_priority_fee_monitoring(&self) {
        let priority_fee_calculator = self.priority_fee_calculator.clone();
        let rpc_client = self.rpc_client.clone();

        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(2));

            loop {
                interval.tick().await;

                if let Ok(recent_fees) = rpc_client.get_recent_prioritization_fees(&[]).await {
                    let mut fees = priority_fee_calculator.recent_fees.write().await;
                    
                    for fee_info in recent_fees {
                        fees.push(fee_info.prioritization_fee);
                        if fees.len() > 1000 {
                            fees.remove(0);
                        }
                    }

                    if !fees.is_empty() {
                        let mut sorted_fees = fees.clone();
                        sorted_fees.sort_unstable();
                        let percentile_index = ((sorted_fees.len() as f64 * PRIORITY_FEE_PERCENTILE) as usize)
                            .min(sorted_fees.len() - 1);
                        let optimal_fee = sorted_fees[percentile_index];
                        priority_fee_calculator.base_priority_fee.store(optimal_fee, Ordering::Release);
                    }
                }
            }
        });
    }

    async fn start_leader_schedule_refresh(&self) {
        let executor = self.clone();
        
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(60)); // Refresh every minute
            
            loop {
                interval.tick().await;
                
                if let Err(e) = executor.refresh_leader_schedule().await {
                    eprintln!("Failed to refresh leader schedule: {}", e);
                }
            }
        });
    }

    pub async fn execute_at_slot_boundary(
        &self,
        request: FlashLoanRequest,
    ) -> Result<ExecutionResult, Box<dyn std::error::Error>> {
        let slot_boundary_time = self.calculate_next_slot_boundary().await;
        let current_time = Instant::now();
        
        if slot_boundary_time > current_time {
            let wait_duration = slot_boundary_time.duration_since(current_time);
            if wait_duration > Duration::from_millis(SLOT_DURATION_MS) {
                return Err("Invalid slot boundary calculation".into());
            }
            sleep(wait_duration).await;
        }

        let priority_fee = self.calculate_dynamic_priority_fee(&request).await;
        let transaction = self.build_flashloan_transaction(&request, priority_fee).await?;

        let simulation_result = self.simulate_transaction(&transaction).await?;
        if !simulation_result.0 {
            return Err("Transaction simulation failed".into());
        }

        let send_time = Instant::now();
        let signature = self.send_transaction_with_retry(transaction, MAX_RETRIES).await?;
        let timing_offset_ms = send_time.duration_since(slot_boundary_time).as_millis() as i64;

        let current_slot = self.slot_tracker.current_slot.load(Ordering::Acquire);

        Ok(ExecutionResult {
            signature,
            slot: current_slot,
            timing_offset_ms,
            priority_fee,
            profit: request.expected_profit.saturating_sub(priority_fee),
        })
    }

    async fn calculate_next_slot_boundary(&self) -> Instant {
        let slot_start = self.slot_tracker.slot_start_time.read().await.clone();
        let elapsed = slot_start.elapsed().as_millis() as u64;
        let slot_progress = elapsed % SLOT_DURATION_MS;
        let time_to_next_slot = SLOT_DURATION_MS - slot_progress;
        
        let next_boundary = Instant::now() + Duration::from_millis(time_to_next_slot);
        
        if SLOT_BOUNDARY_OFFSET_MS < 0 {
            next_boundary - Duration::from_millis(SLOT_BOUNDARY_OFFSET_MS.abs() as u64)
        } else {
            next_boundary + Duration::from_millis(SLOT_BOUNDARY_OFFSET_MS as u64)
        }
    }

    async fn calculate_dynamic_priority_fee(&self, request: &FlashLoanRequest) -> u64 {
        let base_fee = self.priority_fee_calculator.base_priority_fee.load(Ordering::Acquire);
        let profit_ratio = request.expected_profit as f64 / request.amount as f64;
        
        let competitive_multiplier = if profit_ratio > 0.01 {
            2.5
        } else if profit_ratio > 0.005 {
            1.8
        } else {
            1.2
        };

        let slot_progress = {
            let slot_start = self.slot_tracker.slot_start_time.read().await;
            slot_start.elapsed().as_millis() as f64 / SLOT_DURATION_MS as f64
        };

        let timing_multiplier = if slot_progress < 0.1 || slot_progress > 0.9 {
            1.5
        } else {
            1.0
        };

        let dynamic_fee = (base_fee as f64 * competitive_multiplier * timing_multiplier) as u64;
        dynamic_fee.min(request.expected_profit / 3)
    }

    async fn build_flashloan_transaction(
        &self,
        request: &FlashLoanRequest,
        priority_fee: u64,
    ) -> Result<Transaction, Box<dyn std::error::Error>> {
        let recent_blockhash = self.get_optimal_blockhash().await?;

        let compute_budget_ix = ComputeBudgetInstruction::set_compute_unit_limit(COMPUTE_UNIT_LIMIT);
        let priority_fee_ix = ComputeBudgetInstruction::set_compute_unit_price(
            (priority_fee * 1_000_000) / COMPUTE_UNIT_LIMIT as u64
        );

        let flashloan_accounts = self.build_flashloan_accounts(request)?;
        let flashloan_data = self.build_flashloan_instruction_data(request)?;

        let flashloan_ix = Instruction {
            program_id: self.flashloan_program_id,
            accounts: flashloan_accounts,
            data: flashloan_data,
        };

        let transaction = Transaction::new_signed_with_payer(
            &[compute_budget_ix, priority_fee_ix, flashloan_ix],
            Some(&self.wallet.pubkey()),
            &[&*self.wallet],
            recent_blockhash,
        );

        Ok(transaction)
    }

    async fn get_optimal_blockhash(&self) -> Result<Hash, Box<dyn std::error::Error>> {
        let slot_hashes = self.slot_tracker.slot_hashes.read().await;
        
        if let Some((_, hash)) = slot_hashes.back() {
            Ok(*hash)
        } else {
            Ok(self.rpc_client.get_latest_blockhash().await?)
        }
    }

    fn build_flashloan_accounts(&self, request: &FlashLoanRequest) -> Result<Vec<AccountMeta>, Box<dyn std::error::Error>> {
        let mut accounts = vec![
            AccountMeta::new(self.wallet.pubkey(), true),
            AccountMeta::new_readonly(request.token_mint, false),
            AccountMeta::new_readonly(clock::id(), false),
            AccountMeta::new_readonly(rent::id(), false),
        ];

        accounts.extend(request.target_accounts.clone());

        Ok(accounts)
    }

    fn build_flashloan_instruction_data(&self, request: &FlashLoanRequest) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let mut data = vec![0u8; 8];
        data[0] = 1;

        data.extend_from_slice(&request.amount.to_le_bytes());
        data.extend_from_slice(&(request.target_instructions.len() as u32).to_le_bytes());
        data.extend_from_slice(&request.target_instructions);

        Ok(data)
    }

    async fn simulate_transaction(&self, transaction: &Transaction) -> Result<(bool, u64), Box<dyn std::error::Error>> {
        let _permit = self.simulation_semaphore.acquire().await?;

        let config = RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: true,
            commitment: Some(CommitmentConfig::processed()),
            encoding: Some(UiTransactionEncoding::Base64),
            accounts: None,
            min_context_slot: None,
            inner_instructions: false,
        };

        let result = self.rpc_client.simulate_transaction_with_config(
            &transaction,
            config,
        ).await?;

        if let Some(err) = result.value.err {
            return Ok((false, 0));
        }

        let units_consumed = result.value.units_consumed.unwrap_or(0);
        Ok((true, units_consumed))
    }

    async fn send_transaction_with_retry(
        &self,
        transaction: Transaction,
        max_retries: u8,
    ) -> Result<Signature, Box<dyn std::error::Error>> {
        let config = RpcSendTransactionConfig {
            skip_preflight: false, // Fixed: Changed to false for proper validation
            preflight_commitment: Some(CommitmentConfig::processed()), // Added proper commitment
            encoding: None,
            max_retries: Some(max_retries as u64), // Fixed: Align with function parameter
            min_context_slot: None,
        };

        let mut last_error = None;
        let mut retry_count = 0;

        while retry_count < max_retries {
            let send_result = self.rpc_client.send_transaction_with_config(
                &transaction,
                config.clone(),
            ).await;

            match send_result {
                Ok(signature) => {
                    let confirmation_start = Instant::now();
                    let mut confirmation_attempts = 0;

                    while confirmation_attempts < 30 && confirmation_start.elapsed() < Duration::from_secs(15) {
                        if let Ok(status) = self.rpc_client.get_signature_status(&signature).await {
                            if let Some(confirmation) = status {
                                if confirmation.satisfies_commitment(CommitmentConfig::confirmed()) {
                                    return Ok(signature);
                                }
                            }
                        }
                        
                        sleep(Duration::from_millis(100 * (retry_count as u64 + 1))).await; // Exponential backoff
                        confirmation_attempts += 1;
                    }

                    return Ok(signature);
                }
                Err(e) => {
                    last_error = Some(e);
                    retry_count += 1;
                    
                    if retry_count < max_retries {
                        sleep(Duration::from_millis(100 * (retry_count as u64).pow(2))).await; // Exponential backoff
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| "Transaction failed after retries".into()).into())
    }

    pub async fn monitor_slot_boundaries(
        &self,
        mut request_receiver: mpsc::Receiver<FlashLoanRequest>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (result_sender, mut result_receiver) = mpsc::channel::<ExecutionResult>(100);
        let executor = self.clone();

        tokio::spawn(async move {
            while let Some(request) = request_receiver.recv().await {
                let executor_clone = executor.clone();
                let result_sender_clone = result_sender.clone();

                tokio::spawn(async move {
                    match executor_clone.execute_at_slot_boundary(request).await {
                        Ok(result) => {
                            let _ = result_sender_clone.send(result).await;
                        }
                        Err(e) => {
                            eprintln!("Execution error: {:?}", e);
                        }
                    }
                });
            }
        });

        while let Some(result) = result_receiver.recv().await {
            println!(
                "Executed at slot {} with timing offset {}ms, profit: {} lamports",
                result.slot, result.timing_offset_ms, result.profit
            );
        }

        Ok(())
    }

    pub async fn get_slot_leader(&self, slot: u64) -> Option<Pubkey> {
        let leader_schedule = self.slot_tracker.leader_schedule.read().await;
        
        // Fixed: Check for empty leader schedule
        if leader_schedule.is_empty() {
            return None;
        }
        
        let slot_index = slot as usize % leader_schedule.len();
        
        if slot_index < leader_schedule.len() {
            Some(leader_schedule[slot_index])
        } else {
            None
        }
    }

    async fn refresh_leader_schedule(&self) -> Result<(), Box<dyn std::error::Error>> {
        let current_slot = self.slot_tracker.current_slot.load(Ordering::Acquire);
        let epoch = current_slot / 432000;
        
        if let Ok(leader_schedule) = self.rpc_client.get_leader_schedule_with_commitment(
            Some(current_slot),
            CommitmentConfig::finalized(),
        ).await {
            let mut leaders = Vec::with_capacity(432000);
            
            for (leader_pubkey, slots) in leader_schedule {
                let leader = leader_pubkey.parse::<Pubkey>()?;
                for &slot in slots.iter() {
                    if slot as usize >= leaders.len() {
                        leaders.resize(slot as usize + 1, Pubkey::default());
                    }
                    leaders[slot as usize] = leader;
                }
            }
            
            *self.slot_tracker.leader_schedule.write().await = leaders;
        }
        
        Ok(())
    }

    pub async fn execute_batch_at_boundary(
        &self,
        requests: Vec<FlashLoanRequest>,
    ) -> Vec<Result<ExecutionResult, Box<dyn std::error::Error>>> {
        let slot_boundary_time = self.calculate_next_slot_boundary().await;
        let current_time = Instant::now();
        
        if slot_boundary_time > current_time {
            let wait_duration = slot_boundary_time.duration_since(current_time);
            sleep(wait_duration).await;
        }

        let mut handles = Vec::with_capacity(requests.len());
        
        for request in requests {
            let executor = self.clone();
            let handle = tokio::spawn(async move {
                executor.execute_single_request(request).await
            });
            handles.push(handle);
        }

        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => results.push(Err(Box::new(e) as Box<dyn std::error::Error>)),
            }
        }

        results
    }

    async fn execute_single_request(
        &self,
        request: FlashLoanRequest,
    ) -> Result<ExecutionResult, Box<dyn std::error::Error>> {
        let priority_fee = self.calculate_dynamic_priority_fee(&request).await;
        let transaction = self.build_flashloan_transaction(&request, priority_fee).await?;
        
        let send_time = Instant::now();
        let signature = self.send_transaction_with_retry(transaction, 1).await?;
        let current_slot = self.slot_tracker.current_slot.load(Ordering::Acquire);
        
        Ok(ExecutionResult {
            signature,
            slot: current_slot,
            timing_offset_ms: 0,
            priority_fee,
            profit: request.expected_profit.saturating_sub(priority_fee),
        })
    }

    pub fn get_current_slot(&self) -> u64 {
        self.slot_tracker.current_slot.load(Ordering::Acquire)
    }

    pub async fn get_slot_progress(&self) -> f64 {
        let slot_start = self.slot_tracker.slot_start_time.read().await;
        let elapsed = slot_start.elapsed().as_millis() as f64;
        (elapsed % SLOT_DURATION_MS as f64) / SLOT_DURATION_MS as f64
    }

    pub fn get_base_priority_fee(&self) -> u64 {
        self.priority_fee_calculator.base_priority_fee.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_sdk::signature::Keypair;

    #[tokio::test]
    async fn test_slot_boundary_calculation() {
        let executor = SlotBoundaryFlashLoanExecutor::new(
            "https://api.mainnet-beta.solana.com".to_string(),
            Keypair::new(),
            Pubkey::new_unique(),
        ).await.unwrap();

        let boundary = executor.calculate_next_slot_boundary().await;
        let now = Instant::now();
        
        assert!(boundary > now);
        assert!(boundary.duration_since(now) < Duration::from_millis(SLOT_DURATION_MS));
    }
}
