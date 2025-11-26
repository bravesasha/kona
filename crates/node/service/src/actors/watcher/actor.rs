//! [`NodeActor`] implementation for an L1 chain watcher that polls for L1 block updates over HTTP
//! RPC.

use crate::{
    NodeActor,
    actors::{
        CancellableContext,
        watcher::{blockstream::BlockStream, error::L1WatcherActorError},
    },
};
use alloy_primitives::{Address, B256};
use alloy_provider::{Provider, RootProvider};
use alloy_rpc_types_eth::Log;
use async_trait::async_trait;
use futures::StreamExt;
use kona_genesis::{RollupConfig, SystemConfigLog, SystemConfigUpdate, UnsafeBlockSignerUpdate};
use kona_protocol::BlockInfo;
use std::sync::Arc;
use tokio::{
    select,
    sync::{
        mpsc::{self},
        watch,
    },
};
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};

/// An L1 chain watcher that checks for L1 block updates over RPC.
#[derive(Debug)]
pub struct L1WatcherActor {
    /// The [`RollupConfig`] to tell if ecotone is active.
    /// This is used to determine if the L1 watcher should check for unsafe block signer updates.
    pub rollup_config: Arc<RollupConfig>,
    /// The L1 provider.
    pub l1_provider: RootProvider,
    /// The latest L1 head block.
    pub latest_head: watch::Sender<Option<BlockInfo>>,
    /// The latest L1 finalized block.
    pub latest_finalized: watch::Sender<Option<BlockInfo>>,
    /// The block signer sender.
    pub block_signer_sender: mpsc::Sender<Address>,
    /// The cancellation token, shared between all tasks.
    pub cancellation: CancellationToken,
    /// A stream over the latest head.
    pub head_stream: BlockStream,
    /// A stream over the finalized block accepted as canonical.
    pub finalized_stream: BlockStream,
}

impl L1WatcherActor {
    /// Fetches logs for the given block hash.
    async fn fetch_logs(
        &self,
        block_hash: B256,
    ) -> Result<Vec<Log>, L1WatcherActorError<BlockInfo>> {
        let logs = self
            .l1_provider
            .get_logs(&alloy_rpc_types_eth::Filter::new().select(block_hash))
            .await?;

        Ok(logs)
    }
}

#[async_trait]
impl NodeActor for L1WatcherActor {
    type Error = L1WatcherActorError<BlockInfo>;
    type StartData = ();

    /// Start the main processing loop.
    async fn start(mut self, _: Self::StartData) -> Result<(), Self::Error> {
        let mut head_stream = self.head_stream.clone().into_stream();
        let mut finalized_stream = self.finalized_stream.clone().into_stream();

        loop {
            select! {
                _ = self.cancelled() => {
                    // Exit the task on cancellation.
                    info!(
                        target: "l1_watcher",
                        "Received shutdown signal. Exiting L1 watcher task."
                    );

                    return Ok(());
                },
                new_head = head_stream.next() => match new_head {
                    None => {
                        return Err(L1WatcherActorError::StreamEnded);
                    }
                    Some(head_block_info) => {
                        // Send the head update event to all consumers.
                        self.latest_head.send_replace(Some(head_block_info));

                        // For each log, attempt to construct a [`SystemConfigLog`].
                        // Build the [`SystemConfigUpdate`] from the log.
                        // If the update is an Unsafe block signer update, send the address
                        // to the block signer sender.
                        let logs = self.fetch_logs(head_block_info.hash).await?;
                        let ecotone_active = self.rollup_config.is_ecotone_active(head_block_info.timestamp);
                        for log in logs {
                            if log.address() != self.rollup_config.l1_system_config_address {
                                continue; // Skip logs not related to the system config.
                            }

                            let sys_cfg_log = SystemConfigLog::new(log.into(), ecotone_active);
                            if let Ok(SystemConfigUpdate::UnsafeBlockSigner(UnsafeBlockSignerUpdate { unsafe_block_signer })) = sys_cfg_log.build() {
                                info!(
                                    target: "l1_watcher",
                                    "Unsafe block signer update: {unsafe_block_signer}"
                                );
                                if let Err(e) = self.block_signer_sender.send(unsafe_block_signer).await {
                                    error!(
                                        target: "l1_watcher",
                                        "Error sending unsafe block signer update: {e}"
                                    );
                                }
                            }
                        }
                    },
                },
                new_finalized = finalized_stream.next() => match new_finalized {
                    None => {
                        return Err(L1WatcherActorError::StreamEnded);
                    }
                    Some(finalized_block_info) => {
                        self.latest_finalized.send_replace(Some(finalized_block_info));
                    }
                }
            }
        }
    }
}

impl CancellableContext for L1WatcherActor {
    fn cancelled(&self) -> WaitForCancellationFuture<'_> {
        self.cancellation.cancelled()
    }
}
