//! [`NodeActor`] implementation for an L1 chain query actor.
//! The Query actor mostly relays answers from an L1 provider and `BlockStream`.

use std::sync::Arc;

use alloy_eips::BlockId;
use alloy_primitives::Address;
use alloy_provider::{Provider as _, RootProvider};
use async_trait::async_trait;
use kona_genesis::RollupConfig;
use kona_protocol::BlockInfo;
use kona_rpc::{L1State, L1WatcherQueries};
use tokio::{
    select,
    sync::{mpsc, watch},
};
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};

use crate::{CancellableContext, NodeActor, actors::query::error::L1QueryActorError};

/// Actor processing inbound queries
#[derive(Debug)]
pub struct L1QueryActor {
    /// The [`RollupConfig`] to tell if ecotone is active.
    /// This is used to determine if the L1 watcher should check for unsafe block signer updates.
    pub rollup_config: Arc<RollupConfig>,
    /// The L1 provider.
    pub l1_provider: RootProvider,
    /// The inbound queries to the L1 watcher.
    pub inbound_queries: mpsc::Receiver<L1WatcherQueries>,
    /// The latest L1 head block.
    pub latest_head: watch::Sender<Option<BlockInfo>>,
    /// The latest L1 finalized block.
    pub latest_finalized: watch::Sender<Option<BlockInfo>>,
    /// The block signer sender.
    pub block_signer_sender: mpsc::Sender<Address>,
    /// The cancellation token, shared between all tasks.
    pub cancellation: CancellationToken,
}

#[async_trait]
impl NodeActor for L1QueryActor {
    type Error = L1QueryActorError<BlockInfo>;
    type StartData = ();

    /// Start the inbound query actor
    async fn start(mut self, _: Self::StartData) -> Result<(), Self::Error> {
        let cancel = self.cancellation.clone();
        let latest_head = self.latest_head.subscribe();

        loop {
            select! {
                    _ = cancel.cancelled() => {
                        // Exit the task on cancellation.
                        info!(
                            target: "l1_watcher_incoming",
                            "Received shutdown signal. Exiting L1 watcher task."
                        );

                        return Ok(());
                    },
                    inbound_query = self.inbound_queries.recv() => match inbound_query {
                     Some(query) => {
                        match query {
                            L1WatcherQueries::Config(sender) => {
                                if let Err(e) = sender.send((*self.rollup_config).clone()) {
                                    warn!(target: "l1_watcher", error = ?e, "Failed to send L1 config to the query sender");
                                }
                            }
                            L1WatcherQueries::L1State(sender) => {
                                let current_l1 = *latest_head.borrow();

                                let head_l1 = match self.l1_provider.get_block(BlockId::latest()).await {
                                        Ok(block) => block,
                                        Err(e) => {
                                            warn!(target: "l1_watcher", error = ?e, "failed to query l1 provider for latest head block");
                                            None
                                        }}.map(|block| block.into_consensus().into());

                                let finalized_l1 = match self.l1_provider.get_block(BlockId::finalized()).await {
                                        Ok(block) => block,
                                        Err(e) => {
                                            warn!(target: "l1_watcher", error = ?e, "failed to query l1 provider for latest finalized block");
                                            None
                                        }}.map(|block| block.into_consensus().into());

                                let safe_l1 = match self.l1_provider.get_block(BlockId::safe()).await {
                                        Ok(block) => block,
                                        Err(e) => {
                                            warn!(target: "l1_watcher", error = ?e, "failed to query l1 provider for latest safe block");
                                            None
                                        }}.map(|block| block.into_consensus().into());

                                if let Err(e) = sender.send(L1State {
                                    current_l1,
                                    current_l1_finalized: finalized_l1,
                                    head_l1,
                                    safe_l1,
                                    finalized_l1,
                                }) {
                                    warn!(target: "l1_watcher", error = ?e, "Failed to send L1 state to the query sender");
                                }
                            }
                        }
                    },
                    None => {
                        error!(target: "l1_watcher", "L1 watcher query channel closed unexpectedly, exiting query processor task.");
                        return Err(L1QueryActorError::Error("Channel closed".to_string()))
                    }
                },
            }
        }
    }
}

impl CancellableContext for L1QueryActor {
    fn cancelled(&self) -> WaitForCancellationFuture<'_> {
        self.cancellation.cancelled()
    }
}
