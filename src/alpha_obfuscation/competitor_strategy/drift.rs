use std::collections::VecDeque;

#[derive(Clone, Debug, Default)]
pub struct Fingerprint {
    // Feature vector (normalized): [strategy_entropy, avg_cu_price, avg_tip, phase_score]
    pub v: [f64; 4],
}

#[derive(Clone, Debug)]
pub struct DriftMonitor {
    // Rolling window of fingerprints
    window: VecDeque<Fingerprint>,
    max_len: usize,
    // EWMA state
    mean: [f64; 4],
    var: [f64; 4],
    alpha: f64,
    // Alert threshold (z-score)
    z_thresh: f64,
}

impl DriftMonitor {
    pub fn new(max_len: usize, alpha: f64, z_thresh: f64) -> Self {
        Self {
            window: VecDeque::with_capacity(max_len),
            max_len,
            mean: [0.0; 4],
            var: [1.0; 4],
            alpha,
            z_thresh,
        }
    }

    pub fn push_and_check(&mut self, f: Fingerprint) -> bool {
        // EWMA update
        for i in 0..4 {
            let diff = f.v[i] - self.mean[i];
            self.mean[i] += self.alpha * diff;
            self.var[i] = (1.0 - self.alpha) * (self.var[i] + self.alpha * diff * diff);
        }
        if self.window.len() == self.max_len { self.window.pop_front(); }
        self.window.push_back(f);

        // Z-score max across dims
        let mut max_z = 0.0;
        if let Some(last) = self.window.back() {
            for i in 0..4 {
                let sd = self.var[i].max(1e-9).sqrt();
                let z = ((last.v[i] - self.mean[i]) / sd).abs();
                if z > max_z { max_z = z; }
            }
        }
        max_z >= self.z_thresh
    }
}

/// Build a fingerprint from recent competitor stats
pub fn fingerprint_from_metrics(
    strategy_counts: &[(String, usize)], // [(strategy_label, count)]
    avg_cu_price: f64,
    avg_tip: f64,
    phase_hist: &[(super::leader::SlotPhase, usize)],
) -> Fingerprint {
    // Strategy entropy
    let total: f64 = strategy_counts.iter().map(|(_, c)| *c as f64).sum();
    let entropy = if total > 0.0 {
        -strategy_counts.iter().map(|(_, c)| {
            let p = *c as f64 / total;
            if p > 0.0 { p * p.log2() } else { 0.0 }
        }).sum::<f64>()
    } else { 0.0 };

    // Phase score: Early=1, Mid=0.5, Late=0
    let phase_total: f64 = phase_hist.iter().map(|(_, c)| *c as f64).sum();
    let phase_score = if phase_total > 0.0 {
        phase_hist.iter().map(|(p, c)| {
            let w = match p {
                super::leader::SlotPhase::Early => 1.0,
                super::leader::SlotPhase::Mid => 0.5,
                super::leader::SlotPhase::Late => 0.0,
            };
            w * (*c as f64 / phase_total)
        }).sum()
    } else { 0.0 };

    Fingerprint { v: [entropy, avg_cu_price, avg_tip, phase_score] }
}
