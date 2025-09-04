use solana_client::{
    rpc_client::RpcClient,
    rpc_config::RpcSendTransactionConfig,
};
use solana_sdk::{
    clock::Clock,
    commitment_config::CommitmentConfig,
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
    signature::{Keypair, Signer, Signature},
    transaction::Transaction,
};
use drift::{
    instructions::{OrderParams, PostOnlyParam},
    state::{user::{Order, User}, order::OrderStatus},
};
use thiserror::Error;
use anchor_lang::error;
use crate::tx_manager::TxManager;

const STALE_ORDER_THRESHOLD_SECONDS: i64 = 30; // 30 seconds threshold for stale orders

#[derive(Debug, Error)]
pub enum ExecutionError {
    #[error("Transaction failed")]
    TxFailure,
    #[error("Order placement failed")]
    OrderPlacementFailed,
    #[error("Anchor error")]
    AnchorError(#[from] anchor_lang::error::Error),
}

pub struct ExecutionEngine {
    tx_mgr: TxManager,
    drift_program: Program,
}

impl ExecutionEngine {
    pub fn new(tx_mgr: TxManager, drift_program: Program) -> Self {
        Self { tx_mgr, drift_program }
    }

    pub async fn place_limit_with_fallbacks(
        &self,
        base_ix_accounts: Vec<AccountMeta>,
        mut params: OrderParams,
        payer: &Keypair,
        signers: &[&Keypair],
    ) -> Result<Signature, ExecutionError> {
        // Try post-only first to avoid taking bad fills
        params.post_only = PostOnlyParam::TryPostOnly;
        params.immediate_or_cancel = false;
        
        match self.try_place(&base_ix_accounts, params.clone(), payer, signers).await {
            Ok(sig) => return Ok(sig),
            Err(_) => log::debug!("Post-only order failed, falling back to IOC"),
        }

        // Fallback to IOC at a conservative limit
        params.post_only = PostOnlyParam::None;
        params.immediate_or_cancel = true;
        
        self.try_place(&base_ix_accounts, params, payer, signers).await
    }

    pub async fn try_place(
        &self,
        accounts: &[AccountMeta],
        params: OrderParams,
        payer: &Keypair,
        signers: &[&Keypair],
    ) -> Result<Signature, ExecutionError> {
        let data = drift::instruction::PlaceOrder { order_params: params }
            .try_to_vec()
            .map_err(|_| ExecutionError::OrderPlacementFailed)?;

        let ix = Instruction {
            program_id: self.drift_program.id(),
            accounts: accounts.to_vec(),
            data,
        };

        self.tx_mgr
            .submit_and_confirm(
                vec![ix],
                payer,
                signers,
                400_000,
                true,
                CommitmentConfig::confirmed(),
            )
            .await
            .map_err(|_| ExecutionError::TxFailure)
    }

    pub async fn cancel_all_stale(
        &self,
        user_accounts: Vec<AccountMeta>,
        payer: &Keypair,
        signers: &[&Keypair],
    ) -> Result<(), ExecutionError> {
        // Get user's open orders
        let user_account = self.drift_program.account::<User>(user_accounts[0].pubkey).await?;
        let now = Clock::get()?.unix_timestamp;
        
        // Filter orders older than STALE_ORDER_THRESHOLD_SECONDS
        let stale_orders: Vec<_> = user_account.orders
            .iter()
            .filter(|o| o.status == OrderStatus::Open && now - o.slot_time > STALE_ORDER_THRESHOLD_SECONDS)
            .collect();

        if stale_orders.is_empty() {
            return Ok(());
        }

        // Batch cancel stale orders
        let mut ixs = Vec::with_capacity(stale_orders.len());
        for order in stale_orders {
            let data = drift::instruction::CancelOrder {
                order_id: order.order_id,
            }.try_to_vec()?;

            ixs.push(Instruction {
                program_id: self.drift_program.id(),
                accounts: user_accounts.clone(),
                data,
            });
        }

        self.tx_mgr
            .submit_and_confirm(
                ixs,
                payer,
                signers,
                400_000,
                true,
                CommitmentConfig::confirmed(),
            )
            .await?;

        Ok(())
    }
}
