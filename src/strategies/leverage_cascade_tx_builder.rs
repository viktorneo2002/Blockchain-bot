use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    signature::{Keypair, Signature},
    transaction::Transaction,
    instruction::Instruction,
    commitment_config::CommitmentConfig,
};
use crate::config::ComputeConfig;
use crate::errors::OptimizerError;

pub struct TxBuilder {
    rpc: Arc<RpcClient>,
    fee_payer: Arc<Keypair>,
    compute_cfg: ComputeConfig,
}

impl TxBuilder {
    pub fn new(rpc: Arc<RpcClient>, fee_payer: Arc<Keypair>, compute_cfg: ComputeConfig) -> Self {
        Self { rpc, fee_payer, compute_cfg }
    }

    pub async fn execute(
        &self,
        mut ix: Vec<Instruction>,
        retries: usize,
    ) -> Result<Signature, OptimizerError> {
        // Dynamic compute unit pricing
        let price = self.fetch_optimal_compute_price().await;
        ix.insert(0, ComputeBudgetInstruction::set_compute_unit_price(price));
        ix.insert(0, ComputeBudgetInstruction::set_compute_unit_limit(self.compute_cfg.unit_limit));

        for attempt in 0..=retries {
            let (recent_hash, _) = self.rpc
                .get_latest_blockhash_with_commitment(CommitmentConfig::confirmed())
                .await?;

            let tx = Transaction::new_signed_with_payer(
                &ix,
                Some(&self.fee_payer.pubkey()),
                &[&self.fee_payer],
                recent_hash,
            );

            // Safety simulation
            let sim = self.rpc.simulate_transaction(&tx).await?;
            if let Some(err) = sim.value.err {
                if attempt == retries {
                    return Err(OptimizerError::SimulationFailed(err));
                }
                continue;
            }

            // Send with retries
            match self.rpc.send_and_confirm_transaction(&tx).await {
                Ok(sig) => return Ok(sig),
                Err(e) if attempt == retries => return Err(OptimizerError::SendError(e)),
                _ => tokio::time::sleep(Duration::from_millis(100 * (attempt as u64 + 1))).await,
            }
        }

        unreachable!()
    }

    async fn fetch_optimal_compute_price(&self) -> u64 {
        // Implement Jito/EigenPhi price estimation
        // Would query current mempool conditions
        self.compute_cfg.unit_price_max
    }
}
