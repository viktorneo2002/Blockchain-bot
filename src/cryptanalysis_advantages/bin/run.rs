use std::sync::Arc;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{instruction::Instruction, signature::Keypair, pubkey::Pubkey};
use prometheus::Registry;
use timing_executor::{
    execution::{
        timing_attack_executor::TimingAttackExecutor,
        fee_model::InclusionStats,
        types::{ExecutionPlan, OpportunityMeta},
    },
    obs::metrics::Metrics,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let payer = Keypair::new();
    let reg = Registry::new();
    let metrics = Metrics::new(&reg);

    let exec = TimingAttackExecutor::new(
        "https://api.mainnet-beta.solana.com",
        "wss://api.mainnet-beta.solana.com",
        "https://mainnet.block-engine.jito.wtf/api/v1",
        None, // or Some(UUID)
        payer,
        vec![
            "https://api.mainnet-beta.solana.com".into(),
            "https://rpc.ankr.com/solana".into(),
        ],
        metrics,
    ).await?;

    let opp = OpportunityMeta {
        target_slot: 0,
        leader: None,
        target_rank: Some(3),
        pnl_min_lamports: 50_000,
        strict_window_ms: Some(150),
    };

    let plan = ExecutionPlan { instructions: vec![
        // Fill with real instructions for your strategy
    ]};

    let leader_stats = InclusionStats {
        leader_fee_baseline: 2500,
        p50: 3000, p75: 4500, p90: 7000, p95: 10000,
    };

    let sig = exec.execute_opportunity(opp, plan, leader_stats, &[]).await?;
    println!("Confirmed: {}", sig);
    Ok(())
}
