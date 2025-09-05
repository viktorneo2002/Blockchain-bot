use prometheus::{Encoder, Histogram, IntCounter, Registry, TextEncoder};

#[derive(Clone)]
pub struct Metrics {
    pub sends_total: IntCounter,
    pub confirms_total: IntCounter,
    pub failures_total: IntCounter,
    pub fee_hist: Histogram,
    pub rtt_hist: Histogram,
}

impl Metrics {
    pub fn new(reg: &Registry) -> Self {
        let sends_total = IntCounter::new("sends_total", "Total sends").unwrap();
        let confirms_total = IntCounter::new("confirms_total", "Total confirms").unwrap();
        let failures_total = IntCounter::new("failures_total", "Total failures").unwrap();
        let fee_hist = Histogram::with_opts(prometheus::opts!("fee_lamports", "fee dist")).unwrap();
        let rtt_hist = Histogram::with_opts(prometheus::opts!("rtt_ms", "network rtt")).unwrap();
        reg.register(Box::new(sends_total.clone())).ok();
        reg.register(Box::new(confirms_total.clone())).ok();
        reg.register(Box::new(failures_total.clone())).ok();
        reg.register(Box::new(fee_hist.clone())).ok();
        reg.register(Box::new(rtt_hist.clone())).ok();
        Self { sends_total, confirms_total, failures_total, fee_hist, rtt_hist }
    }

    pub fn scrape(reg: &Registry) -> String {
        let mut buf = vec![];
        TextEncoder::new().encode(&reg.gather(), &mut buf).ok();
        String::from_utf8(buf).unwrap_or_default()
    }
}
