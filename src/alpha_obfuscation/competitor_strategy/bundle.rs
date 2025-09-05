use std::collections::VecDeque;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct BundleObs {
    pub slot: u64,
    pub bundle_rank: u32,
    pub tip_lamports: u64,
    pub cu_price: u64,
    pub cu_used: u64,
    pub included: bool,
    pub pnl_usd: f64,
    pub congestion_idx: f64, // 0..1 (e.g., blockfill proxy)
}

#[derive(Default)]
pub struct BidCurveModel {
    // Keep lightweight: per congestion bucket logistic fit parameters.
    pub buckets: Vec<BucketModel>,
}

#[derive(Default, Clone)]
pub struct BucketModel {
    pub lo: f64,
    pub hi: f64,
    // logistic parameters: success ~ 1/(1+exp(-(a + b*price + c*tip)))
    pub a: f64,
    pub b: f64,
    pub c: f64,
    pub n: usize,
}

impl BidCurveModel {
    pub fn new_buckets() -> Self {
        let mut m = Self::default();
        m.buckets = vec![
            BucketModel { lo: 0.0, hi: 0.33, a: -2.0, b: 0.00002, c: 0.0000005, n: 0 },
            BucketModel { lo: 0.33, hi: 0.66, a: -3.0, b: 0.00003, c: 0.0000007, n: 0 },
            BucketModel { lo: 0.66, hi: 1.0, a: -4.0, b: 0.00005, c: 0.0000010, n: 0 },
        ];
        m
    }

    pub fn observe(&mut self, o: &BundleObs) {
        if let Some(b) = self.bucket_for(o.congestion_idx) {
            // Online logistic regression update (very rough SGD step)
            let price = o.cu_price as f64;
            let tip = o.tip_lamports as f64;
            let y = if o.included { 1.0 } else { 0.0 };
            let z = b.a + b.b * price + b.c * tip;
            let yhat = 1.0 / (1.0 + (-z).exp());
            let lr = 1e-6; // tiny learning rate
            let grad = yhat - y;
            b.a -= lr * grad;
            b.b -= lr * grad * price;
            b.c -= lr * grad * tip;
            b.n += 1;
        }
    }

    pub fn recommend(&self, congestion_idx: f64, target_prob: f64, price_hint: u64) -> (u64 /*cu_price*/, u64 /*tip*/) {
        if let Some(b) = self.bucket_for(congestion_idx) {
            // Solve for tip given cu_price near hint to reach target_prob
            let price = price_hint as f64;
            let z_needed = (target_prob / (1.0 - target_prob)).ln();
            let tip = ((z_needed - b.a - b.b * price) / b.c).max(0.0);
            (price_hint, tip as u64)
        } else {
            (price_hint, 0)
        }
    }

    fn bucket_for(&self, x: f64) -> Option<&BucketModel> {
        self.buckets.iter().find(|b| x >= b.lo && x < b.hi)
    }
    fn bucket_for_mut(&mut self, x: f64) -> Option<&mut BucketModel> {
        self.buckets.iter_mut().find(|b| x >= b.lo && x < b.hi)
    }
}
