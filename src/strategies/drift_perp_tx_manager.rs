use solana_client::{
    rpc_client::RpcClient,
    rpc_config::RpcSendTransactionConfig,
    rpc_response::RpcSimulateTransactionResult,
};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    instruction::Instruction,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    transaction::Transaction,
};
use solana_transaction_status::UiTransactionEncoding;
use std::{sync::Arc, time::{Duration, Instant}};
use tokio::time::sleep;

use crate::error::ArbitrageError;

pub struct TxManager {
    rpc: Arc<RpcClient>,
    max_retries: usize,
    base_backoff_ms: u64,
}

impl TxManager {
    pub fn new(rpc: Arc<RpcClient>) -> Self {
        Self {
            rpc,
            max_retries: 6,
            base_backoff_ms: 120,
        }
    }

    pub async fn recent_fee_micro_lamports(&self) -> u64 {
        self.rpc
            .get_recent_prioritization_fees(&[])
            .await
            .ok()
            .and_then(|v| v.into_iter().map(|f| f.prioritization_fee).max())
            .unwrap_or(60_000) as u64
    }

    pub async fn submit_and_confirm(
        &self,
        mut ixs: Vec<Instruction>,
        payer: &Keypair,
        signers: &[&Keypair],
        compute_units: u32,
        dynamic_priority: bool,
        commitment: CommitmentConfig,
    ) -> Result<Signature, ArbitrageError> {
        let mut retries = 0;
        loop {
            let recent_blockhash = self.rpc.get_latest_blockhash().await?;
            
            let mut final_ixs = Vec::with_capacity(ixs.len() + 2);
            final_ixs.push(ComputeBudgetInstruction::set_compute_unit_limit(compute_units));
            
            let mut priority = 30_000u64;
            if dynamic_priority {
                priority = self.recent_fee_micro_lamports().await;
            }
            final_ixs.push(ComputeBudgetInstruction::set_compute_unit_price(priority));
            final_ixs.extend(ixs.clone());

            let tx = Transaction::new_signed_with_payer(
                &final_ixs,
                Some(&payer.pubkey()),
                signers,
                recent_blockhash,
            );

            let cfg = RpcSendTransactionConfig {
                skip_preflight: true,
                preflight_commitment: Some(commitment.commitment),
                max_retries: Some(0),
                ..Default::default()
            };

            match self.rpc.send_transaction_with_config(&tx, cfg.clone()).await {
                Ok(sig) => {
                    // Poll status with timeout
                    let start = Instant::now();
                    loop {
                        if start.elapsed() > Duration::from_secs(25) {
                            break; // re-broadcast with fresh blockhash
                        }
                        
                        if let Some(status) = self.rpc.get_signature_status(&sig).await? {
                            match status {
                                Ok(_) => return Ok(sig),
                                Err(e) => return Err(ArbitrageError::RpcError(
                                    solana_client::client_error::ClientError::from(e)
                                )),
                            }
                        }
                        sleep(Duration::from_millis(350)).await;
                    }
                }
                Err(_) => { /* fall through to retry */ }
            }

            retries += 1;
            if retries > self.max_retries {
                return Err(ArbitrageError::TransactionFailed(self.max_retries as u32));
            }
            sleep(Duration::from_millis(self.base_backoff_ms * (retries as u64))).await;
        }
    }
}
