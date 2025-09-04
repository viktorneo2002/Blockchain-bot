use serde::Deserialize;
use std::{fs, path::Path};

#[derive(Debug, Deserialize, Clone)]
pub struct EnvConfig {
    pub rpc_url: String,
    pub keypair_path: String,
    pub protocols: Vec<ProtocolConfig>,
    pub risk: RiskConfig,
    pub compute: ComputeConfig,
    pub flash_loan_program: String,
    pub metrics: MetricsConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ProtocolConfig {
    pub name: String,
    pub program_id: String,
    pub adapter: String,
    pub health_params: HealthParams,
}

#[derive(Debug, Deserialize, Clone)]
pub struct HealthParams {
    pub collateral_factor: f64,
    pub liquidation_bonus: f64,
    pub partial_step: Option<f64>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RiskConfig {
    pub min_profit_threshold: u64,
    pub max_liquidation_size: u64,
    pub max_slippage: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct ComputeConfig {
    pub unit_limit: u32,
    pub unit_price_max: u64,
    pub priority_fee_multiplier: f64,
}

#[derive(Debug, Deserialize, Clone)]
pub struct MetricsConfig {
    pub listen_addr: String,
    pub namespace: String,
}

impl EnvConfig {
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let data = fs::read_to_string(path)?;
        let cfg: EnvConfig = toml::from_str(&data)?;
        Ok(cfg)
    }
}
