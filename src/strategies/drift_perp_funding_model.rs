use std::collections::HashMap;

const FUNDING_RATE_PRECISION_I128: i128 = 1_000_000;

pub struct Twap {
    alpha: f64,
    value: Option<f64>,
}

impl Twap {
    pub fn new(window: usize) -> Self {
        let alpha = 2.0 / (window as f64 + 1.0);
        Self { alpha, value: None }
    }

    pub fn update(&mut self, x: f64) -> f64 {
        self.value = Some(match self.value {
            None => x,
            Some(prev) => prev + self.alpha * (x - prev),
        });
        self.value.unwrap()
    }

    pub fn get(&self) -> Option<f64> {
        self.value
    }
}

pub struct FundingModel {
    mark_twap: HashMap<u16, Twap>,
    oracle_twap: HashMap<u16, Twap>,
    max_hourly_bps: u64,
    safety_buffer_bps: u64,
}

impl FundingModel {
    pub fn new() -> Self {
        Self {
            mark_twap: HashMap::new(),
            oracle_twap: HashMap::new(),
            max_hourly_bps: 100,
            safety_buffer_bps: 10,
        }
    }

    pub fn update_and_estimate(
        &mut self,
        market_index: u16,
        mark_price: u64,
        oracle_price: u64,
    ) -> i128 {
        let m = self.mark_twap
            .entry(market_index)
            .or_insert_with(|| Twap::new(60));
        let o = self.oracle_twap
            .entry(market_index)
            .or_insert_with(|| Twap::new(60));

        let mt = m.update(mark_price as f64) as f64;
        let ot = o.update(oracle_price as f64) as f64;

        if ot <= 0.0 {
            return 0;
        }

        // hourly funding estimate: (mark_twap - oracle_twap)/oracle_twap
        let raw = ((mt - ot) / ot) * 10_000.0; // in bps

        // cap + safety buffer reduction (conservative)
        let capped = raw
            .clamp(
                -(self.max_hourly_bps as f64),
                self.max_hourly_bps as f64,
            ) * (1.0 - self.safety_buffer_bps as f64 / 10_000.0);

        // Convert to protocol precision (per-hour)
        let per_hour = (capped / 10_000.0) * FUNDING_RATE_PRECISION_I128 as f64;
        per_hour as i128
    }
}
