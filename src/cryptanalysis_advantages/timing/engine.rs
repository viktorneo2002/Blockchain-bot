use std::{collections::VecDeque, time::{Duration, Instant}};
use tokio::{sync::{Mutex, RwLock}, time::sleep};

#[derive(Debug, Clone, Default)]
pub struct NetStats {
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub congestion: f64,
}

#[derive(Clone)]
pub struct NetworkMonitor {
    lat: Mutex<VecDeque<u64>>,
    snap: RwLock<NetStats>,
}

impl NetworkMonitor {
    pub fn new() -> Self {
        Self { lat: Mutex::new(VecDeque::with_capacity(256)), snap: RwLock::new(NetStats::default()) }
    }

    pub async fn record(&self, ms: u64) {
        let mut v = self.lat.lock().await;
        if v.len() >= 256 { v.pop_front(); }
        v.push_back(ms);
        if v.len() >= 32 {
            let mut d: Vec<f64> = v.iter().map(|&x| x as f64).collect();
            d.sort_by(|a,b| a.partial_cmp(b).unwrap());
            let pct = |p: f64| -> f64 { let i = ((d.len() - 1) as f64 * p).round() as usize; d[i] };
            let mut s = self.snap.write().await;
            s.p50_ms = pct(0.50);
            s.p90_ms = pct(0.90);
            s.p99_ms = pct(0.99);
            s.congestion = ((s.p90_ms / 40.0).max(1.0)).min(3.0);
        }
    }

    pub async fn snapshot(&self) -> NetStats { self.snap.read().await.clone() }
}

#[derive(Debug, Clone)]
pub struct SlotModel {
    pub median_ms: u64,
}

#[derive(Clone)]
pub struct TimingEngine {
    net: NetworkMonitor,
    slot: RwLock<SlotModel>,
}

impl TimingEngine {
    pub fn new(net: NetworkMonitor) -> Self {
        Self { net, slot: RwLock::new(SlotModel { median_ms: 400 }) }
    }

    pub async fn update_slot_median(&self, observed_ms: u64) {
        let mut s = self.slot.write().await;
        s.median_ms = ((s.median_ms as f64) * 0.9 + (observed_ms as f64) * 0.1) as u64;
    }

    pub async fn pre_exec_offset_ms(&self, aggressive: bool) -> u64 {
        let n = self.net.snapshot().await;
        let base = if aggressive { n.p90_ms } else { n.p50_ms };
        let jitter = (n.p99_ms - n.p90_ms).max(0.0) * 0.25;
        (base + jitter).clamp(3.0, 50.0) as u64
    }

    pub async fn target_instant(&self, slot_start: Instant, micro_offset_ms: i64, pre_ms: u64) -> Instant {
        let s = self.slot.read().await;
        let phase = (s.median_ms as i64 / 2) + micro_offset_ms - (pre_ms as i64);
        if phase <= 0 { slot_start } else { slot_start + Duration::from_millis(phase as u64) }
    }

    pub async fn wait_until(&self, when: Instant) -> anyhow::Result<()> {
        let now = Instant::now();
        if when > now {
            let to = when - now;
            if to > Duration::from_secs(10) { anyhow::bail!("wait too long: {:?}", to); }
            if to > Duration::from_millis(1) { sleep(to - Duration::from_millis(1)).await; }
            while Instant::now() < when { std::hint::spin_loop(); }
        }
        Ok(())
    }

    pub fn network(&self) -> &NetworkMonitor { &self.net }
}
