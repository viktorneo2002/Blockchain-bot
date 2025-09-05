use anyhow::{Context, Result};
use dashmap::DashMap;
use std::{sync::{Arc, atomic::{AtomicBool, Ordering}}, time::{Duration, Instant}};
use tokio::{select, spawn, sync::Semaphore, time::timeout};

use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{instruction::Instruction, pubkey::Pubkey, signature::{Keypair, Signature}, signer::Signer, transaction::VersionedTransaction};

use crate::{
    execution::{
        fee_model::{InclusionStats, LadderPlanner},
        timing_attack_executor::ExecutionStatus,
        types::{ExecutionPlan, OpportunityMeta, RouteKind},
    },
    learn::leader_stats::LeaderStats,
    obs::metrics::Metrics,
    risk::profit_guard::{ProfitGuard, ProfitGuardConfig},
    timing::engine::{NetworkMonitor, TimingEngine},
    net::broadcast::{Broadcaster, PublicRpcRoute, DirectTpuRoute, JitoBundleRoute, Route},
    execution::confirm::Confirmer,
    trigger::ws_triggers::{PreSigner, TriggerService, TriggerSpec, TxTemplate},
    execution::tx_builder::TxBuilder,
};

#[derive(Clone)]
pub struct TopExecutor {
    rpc: Arc<RpcClient>,
    payer: Arc<Keypair>,
    broadcaster: Arc<Broadcaster>,
    confirmer: Arc<Confirmer>,
    timing: Arc<TimingEngine>,
    leader_learn: Arc<LeaderStats>,
    profit_guard: Arc<tokio::sync::Mutex<ProfitGuard>>,
    active: Arc<DashMap<Signature, ExecutionStatus>>,
    permits: Arc<Semaphore>,
    metrics: Metrics,
    presigner: Arc<PreSigner>,
    triggers: Arc<TriggerService>,
}

impl TopExecutor {
    pub async fn new(
        rpc_url: &str,
        ws_url: &str,
        block_engine_url: &str,
        payer: Keypair,
        fast_rpcs: Vec<String>,
        metrics: Metrics,
    ) -> Result<Self> {
        let rpc = Arc::new(RpcClient::new_with_commitment(
            rpc_url.to_string(),
            solana_sdk::commitment_config::CommitmentConfig::confirmed(),
        ));
        let payer = Arc::new(payer);

        // Routes: hedged RPC + Direct TPU + Jito
        let mut routes: Vec<Arc<dyn Route>> = vec![];
        for url in fast_rpcs.into_iter().take(2) {
            routes.push(Arc::new(PublicRpcRoute::new(Arc::new(RpcClient::new(url)))) as _);
        }
        routes.push(Arc::new(DirectTpuRoute::new(rpc.clone())?) as _);
        routes.push(Arc::new(JitoBundleRoute::new(block_engine_url, None)) as _);
        let broadcaster = Arc::new(Broadcaster::new(routes));

        let confirmer = Arc::new(Confirmer::new(rpc.clone(), ws_url).await?);

        let net = NetworkMonitor::new();
        let timing = Arc::new(TimingEngine::new(net));

        let leader_learn = Arc::new(LeaderStats::new());
        let profit_guard = Arc::new(tokio::sync::Mutex::new(ProfitGuard::new(ProfitGuardConfig {
            min_expected_pnl_lamports: 30_000,
            max_fee_per_cu: 20_000,
            max_total_fee_lamports: 3_000_000,
            max_per_opportunity_fee_lamports: 400_000,
            rolling_window: Duration::from_secs(60),
            drawdown_limit_lamports: 1_000_000,
        })));

        // Pre-signer + trigger service
        let presigner = Arc::new(PreSigner::new(rpc.clone(), payer.clone()));
        let (triggers, rx) = TriggerService::new(ws_url, presigner.clone()).await?;
        let triggers = Arc::new(triggers);

        // High-priority fire loop: build+send within sub-10 ms
        let exec_clone = {
            let exec = TopExecutor {
                rpc: rpc.clone(),
                payer: payer.clone(),
                broadcaster: broadcaster.clone(),
                confirmer: confirmer.clone(),
                timing: timing.clone(),
                leader_learn: leader_learn.clone(),
                profit_guard: profit_guard.clone(),
                active: Arc::new(DashMap::new()),
                permits: Arc::new(Semaphore::new(256)),
                metrics: metrics.clone(),
                presigner: presigner.clone(),
                triggers: triggers.clone(),
            };
            Arc::new(exec)
        };

        // Dedicated firing task for triggers
        {
            let exec = exec_clone.clone();
            spawn(async move {
                while let Ok((spec, ts)) = rx.recv() {
                    let start = Instant::now();
                    // Fetch leader snapshot (if available by your own leader tracker)
                    let incl = exec.leader_learn.snapshot(spec.program); // here program used as key; wire real leader Pubkey instead

                    // Choose a cu price near target rank using your ladder logic
                    let mut ladder = crate::execution::fee_model::LadderPlanner::new();
                    let fee_plan = ladder.plan(
                        &crate::execution::fee_model::InclusionStats {
                            leader_fee_baseline: incl.leader_fee_baseline,
                            p50: incl.p50, p75: incl.p75, p90: incl.p90, p95: incl.p95
                        },
                        spec.target_rank,
                        exec.timing.network().snapshot().await.congestion,
                    );
                    let cu_price = fee_plan.cu_price_rungs.get(0).cloned().unwrap_or(5_000);

                    // Build pre-signed tx (blockhash refresh + sign) and fire
                    match exec.presigner.build_tx_for_spec("default_template", cu_price, true).await {
                        Ok(vtx) => {
                            let fire_at = Instant::now();
                            let outcome = exec.broadcaster.race_send(vtx.clone()).await;
                            match outcome {
                                Ok(sent) => {
                                    let _ = exec.confirmer.confirm_fast(sent.signature, 150).await;
                                    // After confirmation, update leader stats
                                    exec.leader_learn.record_landing(spec.program, cu_price);
                                }
                                Err(e) => {
                                    eprintln!("Trigger send failed: {e}");
                                }
                            }
                            let end = Instant::now();
                            let total = end.duration_since(start).as_millis();
                            // Log latency budget
                            if total > 10 {
                                eprintln!("Trigger path exceeded 10ms: {total} ms");
                            }
                        }
                        Err(e) => eprintln!("Prebuild error: {e}"),
                    }
                }
            });
        }

        Ok(Arc::try_unwrap(exec_clone).ok().unwrap())
    }

    // Register a trigger and template
    pub async fn register_trigger_with_template(
        &self,
        spec: TriggerSpec,
        template_key: &str,
        base_ixs: Vec<Instruction>,
        seed_ix: Option<Instruction>,
        luts: Vec<Pubkey>,
        cu_limit: u64,
    ) -> Result<()> {
        self.presigner.upsert_template(
            template_key.to_string(),
            TxTemplate::new(base_ixs, seed_ix, luts, cu_limit),
        );
        self.triggers.add_subscription(spec, crossbeam::channel::unbounded().0) // we’ll wire channel externally—use the one returned from TriggerService::new in your runtime wiring
            .await
            .context("add_subscription")?;
        Ok(())
    }

    // Standard opportunity execution: includes profit guard + leader learning
    pub async fn execute_opportunity(
        &self,
        opp: OpportunityMeta,
        plan: ExecutionPlan,
        leader_hint: Pubkey,
        luts: &[Pubkey],
        cu_used_estimate: u64,
    ) -> Result<Signature> {
        if opp.pnl_min_lamports <= 0 {
            anyhow::bail!("negative EV rejected");
        }

        // Build fee ladder from online stats
        let incl = self.leader_learn.snapshot(leader_hint);
        let mut ladder = LadderPlanner::new();
        let fee_plan = ladder.plan(
            &InclusionStats {
                leader_fee_baseline: incl.leader_fee_baseline,
                p50: incl.p50, p75: incl.p75, p90: incl.p90, p95: incl.p95,
            },
            opp.target_rank,
            self.timing.network().snapshot().await.congestion,
        );

        let txb = crate::execution::tx_builder::TxBuilder::new(self.rpc.clone(), self.payer.clone());

        let pre_ms = self.timing.pre_exec_offset_ms(true).await;
        let slot_start = Instant::now();

        let _permit = self.permits.acquire().await.unwrap();
        let mut last_err: Option<anyhow::Error> = None;

        {
            let mut pg = self.profit_guard.lock().await;
            pg.reset_opportunity();
        }

        for (i, (&cu_price, &micro)) in fee_plan.cu_price_rungs.iter().zip(fee_plan.micro_offsets_ms.iter()).enumerate() {
            // Profit guard
            {
                let mut pg = self.profit_guard.lock().await;
                pg.approve_send(opp.pnl_min_lamports, cu_used_estimate, cu_price)?;
            }

            let bh = txb.get_blockhash().await?;
            let vtx = txb.build_v0(
                plan.instructions.clone(),
                cu_price,
                (cu_used_estimate as f64 * 1.10).ceil() as u64,
                luts,
                bh,
            ).await?;

            let target = self.timing.target_instant(slot_start, micro, pre_ms).await;
            self.timing.wait_until(target).await?;

            let sent = match self.broadcaster.race_send(vtx.clone()).await {
                Ok(o) => o,
                Err(e) => { last_err = Some(e.into()); continue; }
            };

            self.active.insert(sent.signature, ExecutionStatus::Sent(sent.route));

            let ok = self.confirmer.confirm_fast(sent.signature, opp.strict_window_ms.unwrap_or(150)).await?;
            if ok {
                // Learn from landed
                self.leader_learn.record_landing(leader_hint, cu_price);
                self.active.remove(&sent.signature);
                return Ok(sent.signature);
            } else {
                self.active.insert(sent.signature, ExecutionStatus::Failed("timeout".into()));
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("confirmation timeout")))
    }
}
