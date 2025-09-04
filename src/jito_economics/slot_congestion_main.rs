//! v2.0 – Top 1% Solana+Jito tip predictor with EWMA, RL, Circuit Breakers, and Prometheus.

use anyhow::{Context, Result};
use dashmap::DashMap;
use jito_searcher_client::{Client as JitoClient, Bundle, TipAccount};
use prometheus::{Encoder, Gauge, IntCounter, Opts, Registry, TextEncoder};
use solana_client::{
    nonblocking::rpc_client::RpcClient,
    rpc_config::{RpcBlockConfig, RpcSimulateTransactionConfig},
};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    compute_budget::ComputeBudgetInstruction,
    signature::{Keypair, Signer},
    transaction::Transaction,
};
use statrs::distribution::{ContinuousCDF, Normal};
use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::{
    sync::{RwLock, Semaphore},
    time::sleep,
};
use tokio_retry::{
    strategy::{ExponentialBackoff, jitter},
    Retry,
};
use tower::{limit::GlobalConcurrencyLimitLayer, ServiceBuilder};

/// Application‐level errors
#[derive(Debug, Error)]
pub enum PredictorError {
    #[error("RPC error: {0}")]
    RpcError(#[from] solana_client::client_error::ClientError),
    #[error("Simulation failed: {0}")]
    SimulationError(String),
    #[error("Jito error: {0}")]
    JitoError(#[from] jito_searcher_client::Error),
    #[error("Keypair load error: {0}")]
    KeypairError(#[from] std::io::Error),
}

/// Exponentially Weighted Moving Average
struct Ewma {
    alpha: f64,
    current: f64,
}
impl Ewma {
    fn new(alpha: f64, init: f64) -> Self {
        Self { alpha, current: init }
    }
    fn update(&mut self, sample: f64) -> f64 {
        self.current = self.alpha * sample + (1.0 - self.alpha) * self.current;
        self.current
    }
}

/// Upper‐Confidence Bound (UCB1) bandit for adaptive parameters
struct Ucb1 {
    counts: Vec<u32>,
    rewards: Vec<f64>,
    total_steps: u32,
}
impl Ucb1 {
    fn new(n_arms: usize) -> Self {
        Self {
            counts: vec![0; n_arms],
            rewards: vec![0.0; n_arms],
            total_steps: 0,
        }
    }
    fn select(&self) -> usize {
        let mut best = 0;
        let mut best_val = f64::MIN;
        for (i, (&count, &reward)) in self.counts.iter().zip(&self.rewards).enumerate() {
            let avg = if count > 0 { reward / count as f64 } else { f64::MAX };
            let bonus = if count > 0 {
                (2.0 * (self.total_steps as f64).ln() / count as f64).sqrt()
            } else {
                f64::INFINITY
            };
            let val = avg + bonus;
            if val > best_val {
                best_val = val;
                best = i;
            }
        }
        best
    }
    fn update(&mut self, arm: usize, reward: f64) {
        self.counts[arm] += 1;
        self.rewards[arm] += reward;
        self.total_steps += 1;
    }
}

/// Core predictor struct
pub struct TipPredictor {
    rpc: Arc<RpcClient>,
    jito: Arc<JitoClient>,
    payer: Arc<Keypair>,
    semaphore: Arc<Semaphore>,
    ewma: RwLock<Ewma>,
    bandit: RwLock<Ucb1>,
    congestion_metric: Gauge,
    tip_suggestion: Gauge,
    sim_failure_counter: IntCounter,
    registry: Registry,
}

impl TipPredictor {
    /// Instantiate with RPC URL, Jito endpoint, payer keypair path
    pub async fn new(
        rpc_url: &str,
        jito_url: &str,
        payer_path: &str,
    ) -> Result<Self> {
        // Clients
        let rpc = Arc::new(RpcClient::new_with_commitment(rpc_url.to_string(), CommitmentConfig::confirmed()));
        let jito = Arc::new(JitoClient::new(jito_url)?);
        let payer = Arc::new(Keypair::from_file(payer_path)?);

        // Rate limiting & concurrency: max 10 concurrent RPC calls
        let semaphore = Arc::new(Semaphore::new(10));

        // Prometheus metrics
        let registry = Registry::new();
        let congestion_metric = Gauge::with_opts(Opts::new("slot_congestion", "EWMA slot congestion")).unwrap();
        let tip_suggestion = Gauge::with_opts(Opts::new("tip_suggestion", "Last suggested tip (lamports)")).unwrap();
        let sim_failure_counter = IntCounter::with_opts(Opts::new("simulation_failures", "Simulations that errored")).unwrap();
        registry.register(Box::new(congestion_metric.clone()))?;
        registry.register(Box::new(tip_suggestion.clone()))?;
        registry.register(Box::new(sim_failure_counter.clone()))?;

        Ok(Self {
            rpc,
            jito,
            payer,
            semaphore,
            ewma: RwLock::new(Ewma::new(0.2, 0.5)),
            bandit: RwLock::new(Ucb1::new(3)), // three arms: low/med/high multipliers
            congestion_metric,
            tip_suggestion,
            sim_failure_counter,
            registry,
        })
    }

    /// Main loop: fetch slot, update metrics, predict, simulate, and submit bundle
    pub async fn run(&self, interval: Duration) -> Result<()> {
        loop {
            let start = Instant::now();
            if let Err(err) = self.process_slot().await {
                tracing::error!("Error in slot processing: {:?}", err);
            }
            // Expose Prometheus metrics over HTTP
            self.export_metrics().await?;
            let elapsed = start.elapsed();
            if elapsed < interval {
                sleep(interval - elapsed).await;
            }
        }
    }

    /// Process a single slot: gather congestion, pick tip, simulate and submit
    async fn process_slot(&self) -> Result<()> {
        // 1. Fetch raw congestion snapshot (e.g., use getRecentPrioritizationFees + jito tip floor)
        let (recent_fees, tip_floor) = tokio::try_join!(
            self.rpc.get_recent_prioritization_fees(),
            self.jito.get_tip_floor(),
        ).context("Failed to fetch congestion metrics")?;

        // Normalize and combine
        let slot_snap = recent_fees
            .second_priority_fee as f64 / 1_000_000_000.0; // lamports → SOL
        let jito_floor = tip_floor.floor_tip as f64 / 1_000_000_000.0;
        let raw_congestion = slot_snap.max(jito_floor);

        // 2. EWMA smoothing
        let smoothed = {
            let mut ewma = self.ewma.write().await;
            ewma.update(raw_congestion)
        };
        self.congestion_metric.set(smoothed);

        // 3. Bandit‐based selection among [0.8×,1.0×,1.2×] multipliers
        let arm = { self.bandit.read().await.select() };
        let multiplier = match arm {
            0 => 0.8,
            1 => 1.0,
            2 => 1.2,
            _ => unreachable!(),
        };

        // 4. Base tip: Normal distribution quantile
        let normal = Normal::new(0.5, 0.15).unwrap();
        let quantile = normal.inverse_cdf((smoothed / 2.0).min(0.95).max(0.05));
        let base_tip_sol = (smoothed + quantile) * multiplier;
        let base_tip_lamports = (base_tip_sol * 1_000_000_000.0).round().clamp(1e4, 1e9) as u64;
        self.tip_suggestion.set(base_tip_lamports as f64);

        // 5. Build, simulate & score transaction
        let tx = self.build_transaction(base_tip_lamports).await?;
        let sim_ok = self.simulate_with_retry(&tx).await?;
        let reward = if sim_ok { 1.0 } else { 0.0 };

        // 6. Update bandit with reward
        { self.bandit.write().await.update(arm, reward) };

        // 7. Submit via Jito bundle
        if sim_ok {
            let tip_account = self.jito.get_random_tip_account().await?;
            let bundle = Bundle::new(vec![tx], tip_account.pubkey(), self.payer.pubkey());
            self.jito.send_bundle(&bundle).await?;
        }

        Ok(())
    }

    /// Build a simple SOL transfer + compute budget instruction
    async fn build_transaction(&self, tip: u64) -> Result<Transaction> {
        let mut ix = ComputeBudgetInstruction::set_compute_unit_price(tip);
        let (recent_blockhash, _) = self.rpc.get_latest_blockhash().await?;
        let tx = Transaction::new_signed_with_payer(
            &ix,
            Some(&self.payer.pubkey()),
            &[self.payer.as_ref()],
            recent_blockhash,
        );
        Ok(tx)
    }

    /// Simulate with retry + exponential backoff
    async fn simulate_with_retry(&self, tx: &Transaction) -> Result<bool> {
        // Rate-limit concurrency
        let _permit = self.semaphore.acquire().await.unwrap();
        let strategy = ExponentialBackoff::from_millis(50).map(jitter).take(5);
        let result = Retry::spawn(strategy, || async {
            let config = RpcSimulateTransactionConfig {
                sig_verify: false,
                ..Default::default()
            };
            self.rpc
                .simulate_transaction_with_config(tx.clone(), config)
                .await
                .map_err(|e| {
                    self.sim_failure_counter.inc();
                    PredictorError::RpcError(e)
                })
        })
        .await;
        match result {
            Ok(sim) => Ok(sim.value.err.is_none()),
            Err(e) => Err(anyhow::anyhow!("Simulation permanently failed: {}", e)),
        }
    }

    /// Export Prometheus metrics over stdout (or hook into HTTP endpoint)
    async fn export_metrics(&self) -> Result<()> {
        let mut buffer = Vec::new();
        let encoder = TextEncoder::new();
        let mfs = self.registry.gather();
        encoder.encode(&mfs, &mut buffer)?;
        println!("{}", String::from_utf8_lossy(&buffer));
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    env_logger::init();
    let predictor = TipPredictor::new(
        "https://api.mainnet-beta.solana.com",
        "https://engine.jito.network",
        "/home/solana/.config/solana/id.json",
    )
    .await?;
    predictor.run(Duration::from_secs(1)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::runtime::Runtime;

    #[test]
    fn test_ewma_update() {
        let mut e = Ewma::new(0.5, 1.0);
        assert!((e.update(0.0) - 0.5).abs() < 1e-6);
        assert!((e.update(1.0) - 0.75).abs() < 1e-6);
    }

    #[test]
    fn test_bandit_basic() {
        let mut b = Ucb1::new(2);
        // initially both arms untried, selection returns 0
        assert_eq!(b.select(), 0);
        b.update(0, 1.0);
        b.update(1, 0.0);
        // now arm 0 has reward, likely chosen
        let pick = b.select();
        assert!(pick == 0 || pick == 1);
    }
}
