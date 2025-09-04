pub async fn run_stream(
    &self,
    dex_registry: DexRegistry,
    decay_tracker: Arc<RwLock<DecayTable>>,
    config: Arc<Config>,
) -> Result<()> {
    let mut stream = self.create_stream().await?; // Yellowstone/Jito/WS/gRPC/RPC

    while let Some(message) = stream.next().await {
        if let Ok(tx_data) = message {
            for dex_parser in dex_registry.iter() {
                if let Some(event) = dex_parser.parse(&tx_data, tx_data.slot, tx_data.timestamp) {
                    let mut tracker = decay_tracker.write().await;
                    tracker.add_event(event);
                    // Logging/metrics output (JSONL, Prometheus, etc.)
                }
            }
        }
    }
    Ok(())
}
