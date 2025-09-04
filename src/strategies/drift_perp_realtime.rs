use solana_client::{
    nonblocking::pubsub_client::PubsubClient,
    rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig},
};
use solana_sdk::pubkey::Pubkey;
use tokio_stream::StreamExt;

use crate::error::ArbitrageError;

pub struct Realtime {
    pubsub_url: String,
}

impl Realtime {
    pub fn new(pubsub_url: String) -> Self {
        Self { pubsub_url }
    }

    pub async fn subscribe_accounts(
        &self,
        program_id: Pubkey,
    ) -> Result<(), ArbitrageError> {
        let client = PubsubClient::new(&self.pubsub_url).await
            .map_err(|e| ArbitrageError::RpcError(e.into()))?;
            
        let (mut stream, _unsub) = client
            .program_subscribe(
                program_id,
                Some(RpcProgramAccountsConfig {
                    filters: None,
                    with_context: Some(true),
                    account_config: RpcAccountInfoConfig {
                        encoding: Some(UiAccountEncoding::Base64),
                        ..Default::default()
                    },
                })
            )
            .await
            .map_err(|e| ArbitrageError::RpcError(e.into()))?;

        tokio::spawn(async move {
            while let Some(update) = stream.next().await {
                // Process account updates
                // TODO: Implement selective cache updates
                
                // Handle reorgs by checking slot order
                if let Some(context) = update.context {
                    log::debug!("Account update at slot: {}", context.slot);
                }
            }
        });

        Ok(())
    }

    pub async fn subscribe_pyth(
        &self,
        pyth_price_accounts: Vec<Pubkey>,
    ) -> Result<(), ArbitrageError> {
        // TODO: Implement Pyth push subscriptions if available
        log::info!("Using polling fallback for Pyth prices");
        Ok(())
    }
}
