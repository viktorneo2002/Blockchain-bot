use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BidObs {
    pub cu_price: f64,
    pub tip: f64,
    pub congestion_idx: f64,
    pub included: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ElasticityModel {
    weights: Vec<f64>,
    learning_rate: f64,
    l2_penalty: f64,
    observation_count: usize,
}

impl ElasticityModel {
    pub fn new(learning_rate: f64, l2_penalty: f64) -> Self {
        Self {
            weights: vec![0.0; 4], // bias + 3 features
            learning_rate,
            l2_penalty,
            observation_count: 0,
        }
    }

    fn sigmoid(x: f64) -> f64 {
        1.0 / (1.0 + (-x).exp())
    }

    fn features(obs: &BidObs) -> Vec<f64> {
        vec![
            1.0, // bias term
            (obs.cu_price.max(0.0) + 1.0).ln(),
            (obs.tip.max(0.0) + 1.0).ln(),
            obs.congestion_idx,
        ]
    }

    fn dot_product(a: &[f64], b: &[f64]) -> f64 {
        a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
    }

    pub fn observe(&mut self, obs: &BidObs) {
        let features = Self::features(obs);
        let prediction = Self::sigmoid(Self::dot_product(&self.weights, &features));
        let target = if obs.included { 1.0 } else { 0.0 };
        let error = target - prediction;

        // Update weights with SGD and L2 regularization
        for i in 0..self.weights.len() {
            self.weights[i] += self.learning_rate * 
                (error * features[i] - self.l2_penalty * self.weights[i]);
        }

        self.observation_count += 1;
    }

    pub fn predict_prob(&self, cu_price: f64, tip: f64, congestion_idx: f64) -> f64 {
        let features = vec![
            1.0,
            (cu_price.max(0.0) + 1.0).ln(),
            (tip.max(0.0) + 1.0).ln(),
            congestion_idx,
        ];
        Self::sigmoid(Self::dot_product(&self.weights, &features))
    }

    pub fn suggest_tip(
        &self,
        cu_price: f64,
        target_prob: f64,
        congestion_idx: f64,
        min_tip: f64,
        max_tip: f64,
    ) -> f64 {
        let mut low = min_tip;
        let mut high = max_tip;
        
        for _ in 0..30 {
            let mid = (low + high) / 2.0;
            let prob = self.predict_prob(cu_price, mid, congestion_idx);
            
            if prob >= target_prob {
                high = mid;
            } else {
                low = mid;
            }
        }
        
        high
    }
}
