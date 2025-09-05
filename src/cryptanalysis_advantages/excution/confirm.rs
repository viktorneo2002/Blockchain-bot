use anyhow::Result;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::signature::Signature;
use solana_ws::{Client as WsClient, Commitment};
use std::{sync::Arc, time::Duration};
use tokio::time::{timeout, interval};

pub struct Confirmer {
    rpc: Arc<RpcClient>,
    ws: WsClient,
}

impl Confirmer {
    pub async fn new(rpc: Arc<RpcClient>, ws_url: &str) -> Result<Self> {
        let ws = WsClient::connect(ws_url).await?;
        Ok(Self { rpc, ws })
    }

    pub async fn confirm_fast(&self, sig: Signature, window_ms: u64) -> Result<bool> {
        // logsSubscribe signature path
        let (sender, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let sub = self.ws.logs_subscribe(
            solana_ws::RpcTransactionLogsFilter::Mentions(sig.to_string()),
            solana_ws::RpcTransactionLogsConfig { commitment: Some(Commitment::Processed) },
            move |msg| { let _ = sender.send(msg.value.clone()); },
        ).await?;

        let poll = async {
            let mut tick = interval(Duration::from_millis(20));
            loop {
                // WS-driven
                if let Ok(Some(_val)) = rx.try_recv().map(Some).or(Ok(None)) {
                    self.ws.logs_unsubscribe(sub).await.ok();
                    return Ok(true);
                }
                // Fallback short polling
                if let Ok(resp) = self.rpc.get_signature_statuses(&[sig]).await {
                    if let Some(Some(st)) = resp.value.get(0) {
                        if st.err.is_some() {
                            self.ws.logs_unsubscribe(sub).await.ok();
                            return Ok(false);
                        }
                        if st.confirmations.is_some() {
                            self.ws.logs_unsubscribe(sub).await.ok();
                            return Ok(true);
                        }
                    }
                }
                tick.tick().await;
            }
        };
        Ok(timeout(Duration::from_millis(window_ms), poll).await.unwrap_or(Ok(false))?)
    }
}
