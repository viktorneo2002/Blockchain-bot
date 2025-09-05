use anyhow::{Context, Result};
use dashmap::DashMap;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    address_lookup_table_account::AddressLookupTableAccount,
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::Instruction,
    message::{v0::Message as MessageV0, Message as LegacyMessage},
    pubkey::Pubkey,
    signature::Keypair,
    signer::Signer,
    transaction::{Transaction, VersionedTransaction},
};
use solana_transaction_status::UiTransactionEncoding;
use std::sync::Arc;

#[derive(Clone)]
pub struct TxBuilder {
    rpc: Arc<RpcClient>,
    payer: Arc<Keypair>,
    alts: Arc<DashMap<Pubkey, AddressLookupTableAccount>>,
}

impl TxBuilder {
    pub fn new(rpc: Arc<RpcClient>, payer: Arc<Keypair>) -> Self {
        Self { rpc, payer, alts: Arc::new(DashMap::new()) }
    }

    pub fn set_alt(&self, key: Pubkey, alt: AddressLookupTableAccount) {
        self.alts.insert(key, alt);
    }

    fn load_luts(&self, keys: &[Pubkey]) -> Vec<AddressLookupTableAccount> {
        keys.iter().filter_map(|k| self.alts.get(k).map(|e| e.value().clone())).collect()
    }

    pub async fn get_blockhash(&self) -> Result<Hash> {
        Ok(self.rpc.get_latest_blockhash().await.context("latest blockhash")?)
    }

    pub async fn simulate_units(&self, ixs: &[Instruction], bh: Hash) -> Result<u64> {
        let msg = LegacyMessage::new_with_blockhash(ixs, Some(&self.payer.pubkey()), &bh);
        let mut tx = Transaction::new_unsigned(msg);
        tx.sign(&[self.payer.as_ref()], bh);
        let cfg = solana_client::rpc_config::RpcSimulateTransactionConfig {
            sig_verify: false,
            replace_recent_blockhash: true,
            commitment: Some(solana_sdk::commitment_config::CommitmentConfig::processed()),
            encoding: Some(UiTransactionEncoding::Base64),
            accounts: None,
            min_context_slot: None,
            inner_instructions: false,
        };
        let sim = self.rpc.simulate_transaction_with_config(&tx, cfg).await?;
        if let Some(err) = sim.value.err {
            anyhow::bail!("pre-sim error: {:?}", err);
        }
        Ok(sim.value.units_consumed.unwrap_or(120_000) as u64)
    }

    pub async fn build_v0(
        &self,
        mut ixs: Vec<Instruction>,
        lamports_per_cu: u64,
        cu_limit: u64,
        luts: &[Pubkey],
        bh: Hash,
    ) -> Result<VersionedTransaction> {
        let mut all_ixs = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(cu_limit as u32),
            ComputeBudgetInstruction::set_compute_unit_price(lamports_per_cu),
        ];
        all_ixs.append(&mut ixs);

        let loaded = self.load_luts(luts);
        let msg = MessageV0::try_compile(
            &self.payer.pubkey(),
            &all_ixs,
            &loaded.iter().collect::<Vec<_>>(),
            bh,
        )?;
        Ok(VersionedTransaction::try_new(msg, &[self.payer.as_ref()])?)
    }
}
