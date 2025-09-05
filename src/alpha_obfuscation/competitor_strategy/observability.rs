use once_cell::sync::Lazy;
use prometheus::{Encoder, TextEncoder, Registry, HistogramOpts, Histogram, IntCounterVec, Opts, IntGauge};
use std::net::SocketAddr;
use hyper::{Body, Request, Response, Server, service::{make_service_fn, service_fn}};

pub static REGISTRY: Lazy<Registry> = Lazy::new(Registry::new);
pub static ANALYZER_LATENCY_MS: Lazy<Histogram> = Lazy::new(|| {
    let h = Histogram::with_opts(HistogramOpts::new("analyzer_latency_ms", "End-to-end analysis latency ms").buckets(vec![5.0,10.0,20.0,50.0,100.0,200.0,500.0,1000.0])).unwrap();
    REGISTRY.register(Box::new(h.clone())).ok();
    h
});
pub static DECODE_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    let c = IntCounterVec::new(Opts::new("decode_errors", "Decode errors"), &["kind"]).unwrap();
    REGISTRY.register(Box::new(c.clone())).ok();
    c
});
pub static CU_PRICE_HIST: Lazy<Histogram> = Lazy::new(|| {
    let h = Histogram::with_opts(HistogramOpts::new("cu_price", "Compute unit price")).unwrap();
    REGISTRY.register(Box::new(h.clone())).ok();
    h
});
pub static TIP_HIST: Lazy<Histogram> = Lazy::new(|| {
    let h = Histogram::with_opts(HistogramOpts::new("tips", "Tips in lamports")).unwrap();
    REGISTRY.register(Box::new(h.clone())).ok();
    h
});

pub async fn run_metrics_server(addr: SocketAddr) -> anyhow::Result<()> {
    let make_svc = make_service_fn(|_| async {
        Ok::<_, hyper::Error>(service_fn(|_req: Request<Body>| async {
            let encoder = TextEncoder::new();
            let metric_families = REGISTRY.gather();
            let mut buffer = Vec::new();
            encoder.encode(&metric_families, &mut buffer).unwrap();
            Ok::<_, hyper::Error>(Response::new(Body::from(buffer)))
        }))
    });
    Server::bind(&addr).serve(make_svc).await?;
    Ok(())
}
