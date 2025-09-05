use anyhow::{Context, Result};
use crossbeam::channel::{unbounded, Receiver};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction,
    hash::Hash,
    instruction::Instruction,
    message::v0::Message as MessageV0,
    pubkey::Pubkey,
    signature::{Keypair, Signature},
    signer::Signer,
    transaction::VersionedTransaction,
};
use solana_ws::{Client as WsClient, Commitment, RpcTransactionLogsFilter, RpcTransactionLogsConfig};
use std::{collections::HashMap, sync::Arc, time::Instant};
use tokio::task;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerSpec {
    pub program: Pubkey,
    pub filter_contains: Option<String>, // simple contains on log line
    pub target_rank: Option<u8>,
}

#[derive(Debug, Clone)]
pub struct TxTemplate {
    // Static instruction skeleton (without budget)
    base_ixs: Vec<Instruction>,
    // Optional memo/seed instruction to perturb uniqueness
    seed_ix: Option<Instruction>,
    // Cached LUTs
    luts: Vec<solana_sdk::pubkey::Pubkey>,
    // Compute profile
    cu_limit: u64,
}

impl TxTemplate {
    pub fn new(base_ixs: Vec<Instruction>, seed_ix: Option<Instruction>, luts: Vec<Pubkey>, cu_limit: u64) -> Self {
        Self { base_ixs, seed_ix, luts, cu_limit }
    }
}

pub struct PreSigner {
    rpc: Arc<RpcClient>,
    payer: Arc<Keypair>,
    templates: RwLock<HashMap<String, TxTemplate>>, // key -> template
}

impl PreSigner {
    pub fn new(rpc: Arc<RpcClient>, payer: Arc<Keypair>) -> Self {
        Self { rpc, payer, templates: RwLock::new(HashMap::new()) }
    }

    pub fn upsert_template(&self, key: String, t: TxTemplate) {
        self.templates.write().insert(key, t);
    }

    fn compile(
        &self,
        ixs: Vec<Instruction>,
        luts: &[Pubkey],
        cu_limit: u64,
        cu_price: u64,
        bh: Hash,
    ) -> anyhow::Result<VersionedTransaction> {
        let mut with_budget = vec![
            ComputeBudgetInstruction::set_compute_unit_limit(cu_limit as u32),
            ComputeBudgetInstruction::set_compute_unit_price(cu_price),
        ];
        with_budget.extend(ixs);

        let alts_accounts = vec![]; // supply via builder if you cache AddressLookupTableAccount
        let msg = MessageV0::try_compile(
            &self.payer.pubkey(),
            &with_budget,
            &alts_accounts.iter().collect::<Vec<_>>(),
            bh,
        )?;
        Ok(VersionedTransaction::try_new(msg, &[self.payer.as_ref()])?)
    }

    pub async fn build_and_sign(
        &self,
        key: &str,
        cu_price: u64,
        mutate_seed: Option<Vec<u8>>,
    ) -> Result<VersionedTransaction> {
        let tpl = {
            let g = self.templates.read();
            g.get(key).cloned().context("template not found")?
        };
        let bh = self.rpc.get_latest_blockhash().await?;
        // Optionally mutate a seed memo/noop to make the wire unique
        let mut ixs = tpl.base_ixs.clone();
        if let Some(mut bytes) = mutate_seed {
            if let Some(mut seed_ix) = tpl.seed_ix.clone() {
                // Replace seed data
                seed_ix.data = bytes.split_off(0);
                ixs.push(seed_ix);
            }
        }
        self.compile(ixs, &tpl.luts, tpl.cu_limit, cu_price, bh)
    }
}

pub struct TriggerService {
    ws: WsClient,
    pre: Arc<PreSigner>,
}

impl TriggerService {
    pub async fn new(ws_url: &str, pre: Arc<PreSigner>) -> Result<(Self, Receiver<(TriggerSpec, Instant)>)> {
        let ws = WsClient::connect(ws_url).await?;
        let (tx, rx) = unbounded();

        // The listener task subscribes to all registered specs dynamically
        // For a minimal, proven path, we wire one subscription per program.
        // For multiple specs per same program, we filter inside callback.
        // Expose a function to register more specs as needed.
        let service = Self { ws, pre };

        // Example: external code will add subscriptions via add_subscription(...)
        // For now, return rx; the user calls add_subscription per spec.

        Ok((service, rx))
    }

    pub async fn add_subscription(&self, spec: TriggerSpec, tx: crossbeam::channel::Sender<(TriggerSpec, Instant)>) -> Result<()> {
        let program = spec.program;
        let contains = spec.filter_contains.clone();
        let sub = self.ws.logs_subscribe(
            RpcTransactionLogsFilter::Mentions(program.to_string()),
            RpcTransactionLogsConfig { commitment: Some(Commitment::Processed) },
            move |msg| {
                if let Some(contains) = &contains {
                    let matched = msg.value.logs.iter().any(|l| l.contains(contains));
                    if !matched { return; }
                }
                let _ = tx.send((spec.clone(), Instant::now()));
            },
        ).await?;
        // Keep sub alive by holding WsClient; unsub handled by drop or explicit unsubscription if needed
        let _ = sub;
        Ok(())
    }

    // Fire within sub-10 ms: pre-signed path and direct broadcast call by caller
    pub async fn build_tx_for_spec(
        &self,
        template_key: &str,
        cu_price: u64,
        mutate_seed: bool,
    ) -> Result<VersionedTransaction> {
        let seed = if mutate_seed {
            Some(Instant::now().elapsed().as_nanos().to_le_bytes().to_vec())
        } else { None };
        self.pre.build_and_sign(template_key, cu_price, seed).await
    }
}
