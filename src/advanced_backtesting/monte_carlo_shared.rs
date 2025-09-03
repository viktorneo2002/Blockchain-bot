use borsh::{BorshSerialize, BorshDeserialize};
use rust_decimal::Decimal;

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct SimulationConfig {
    pub num_paths: u64,
    pub time_steps: u64,
    pub initial_price: u64,  // Fixed-point representation
    pub volatility: u64,    // Fixed-point representation
    pub drift: u64,         // Fixed-point representation
    pub dt: u64,            // Fixed-point representation
    pub strike_price: u64,  // Fixed-point representation
    pub is_call_option: bool,
}

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct SimulationResultsFixed {
    pub expected_value: u64,
    pub variance: u64,
    pub percentile_5: u64,
    pub percentile_95: u64,
    pub max_profit: u64,
    pub max_loss: u64,
    pub sharpe_ratio: u64,
    pub paths_completed: u64,
}

#[derive(Debug, Clone)]
pub struct SimulationResults {
    pub expected_value: Decimal,
    pub variance: Decimal,
    pub percentile_5: Decimal,
    pub percentile_95: Decimal,
    pub max_profit: Decimal,
    pub max_loss: Decimal,
    pub sharpe_ratio: Decimal,
    pub paths_completed: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_serialization() {
        let config = SimulationConfig {
            num_paths: 100_000,
            time_steps: 365,
            initial_price: 10000,
            volatility: 2000,
            drift: 500,
            dt: 1,
            strike_price: 10500,
            is_call_option: true,
        };

        let serialized = config.try_to_vec().unwrap();
        let deserialized = SimulationConfig::try_from_slice(&serialized).unwrap();
        assert_eq!(config.num_paths, deserialized.num_paths);
    }
}
