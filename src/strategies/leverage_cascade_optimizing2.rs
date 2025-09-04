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
