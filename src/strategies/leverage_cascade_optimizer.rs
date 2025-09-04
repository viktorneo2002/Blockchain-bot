use std::{collections::HashMap, sync::Arc};
use tokio::sync::{RwLock, Semaphore};
use async_trait::async_trait;

use crate::{
    adapters::adapter::ProtocolAdapter,
    config::EnvConfig,
    tx_builder::TxBuilder,
    oracle::Oracle,
    metrics::Metrics,
    errors::OptimizerError,
};

pub struct ExecutionResult {
    pub path_id: String,
    pub profit: f64,
    pub cu_used: u64,
    pub success: bool,
}

pub struct CascadeOptimizer {
    adapters: Vec<Arc<dyn ProtocolAdapter>>,
    opportunities: Arc<RwLock<HashMap<String, f64>>>,
    tx_builder: Arc<TxBuilder>,
    oracle: Arc<Oracle>,
    metrics: Arc<Metrics>,
    sem: Arc<Semaphore>,
    config: EnvConfig,
}

impl CascadeOptimizer {
    pub fn new(
        adapters: Vec<Arc<dyn ProtocolAdapter>>,
        tx_builder: Arc<TxBuilder>,
        oracle: Arc<Oracle>,
        metrics: Arc<Metrics>,
        config: EnvConfig,
    ) -> Self {
        Self {
            adapters,
            opportunities: Arc::new(RwLock::new(HashMap::new())),
            tx_builder,
            oracle,
            metrics,
            sem: Arc::new(Semaphore::new(config.max_parallel_simulations)),
            config,
        }
    }

    pub async fn run(&self) -> Result<(), OptimizerError> {
        tokio::try_join!(
            self.spawn_scanner(),
            self.spawn_pathfinder(),
            self.spawn_executor(),
            self.spawn_metrics(),
            self.spawn_watchdog(),
        )?;
        Ok(())
    }

    async fn find_best_path(&self) -> Option<Vec<String>> {
        let mut memo = HashMap::new();
        let paths = self.generate_initial_paths().await;
        
        // Cost-based dynamic programming with memoization
        let best_path = self.dp_find_path(&paths, &mut memo).await;
        
        // Parallel pre-simulation
        let simulated_paths = self.pre_simulate_paths(vec![best_path]).await;
        
        simulated_paths.first().cloned()
    }

    async fn pre_simulate_paths(&self, paths: Vec<Vec<String>>) -> Vec<Vec<String>> {
        let mut tasks = vec![];
        
        for path in paths {
            let permit = self.sem.clone().acquire_owned().await.unwrap();
            let optimizer = self.clone();
            
            tasks.push(tokio::spawn(async move {
                let result = optimizer.simulate_path(&path).await;
                drop(permit);
                (path, result)
            }));
        }
        
        let mut results = vec![];
        for task in tasks {
            if let Ok((path, Ok(success))) = task.await {
                if success {
                    results.push(path);
                }
            }
        }
        
        results
    }

    async fn dp_find_path(
        &self,
        paths: &[Vec<String>],
        memo: &mut HashMap<String, (f64, Vec<String>)>,
    ) -> Vec<String> {
        let mut best_path = vec![];
        let mut best_score = f64::MIN;

        for path in paths {
            let path_key = path.join("->");
            
            if let Some((score, _)) = memo.get(&path_key) {
                if *score > best_score {
                    best_score = *score;
                    best_path = path.clone();
                }
                continue;
            }

            let score = self.calculate_path_score(path).await;
            memo.insert(path_key, (score, path.clone()));

            if score > best_score {
                best_score = score;
                best_path = path.clone();
            }
        }

        best_path
    }

    async fn calculate_path_score(&self, path: &[String]) -> f64 {
        // Get oracle prices for all steps in path
        let prices = self.get_path_prices(path).await;
        
        // Simulate execution to estimate profit and CU
        let sim_result = self.simulate_path(path).await.unwrap_or_default();
        
        // Score based on profit, CU efficiency, and price consistency
        sim_result.profit * 
            (1.0 - (sim_result.cu_used as f64 / self.config.max_cu as f64)) * 
            self.price_consistency_score(&prices)
    }

    fn adjust_weights(&self, result: &ExecutionResult) {
        let mut opportunities = self.opportunities.blocking_write();
        let weight = if result.success {
            result.profit * (1.0 - (result.cu_used as f64 / self.config.max_cu as f64))
        } else {
            -1.0 * opportunities.get(&result.path_id).unwrap_or(&0.0)
        };
        
        opportunities.insert(result.path_id.clone(), weight);
    }
}
