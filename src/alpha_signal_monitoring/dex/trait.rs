pub trait DexParser: Send + Sync {
    fn parse(
        &self,
        tx: &ParsedTransaction,
        slot: u64,
        timestamp: u64,
    ) -> Option<DecayEvent>;
}
