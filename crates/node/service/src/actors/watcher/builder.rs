use alloy_primitives::Address;
use alloy_provider::RootProvider;
use kona_genesis::RollupConfig;
use kona_protocol::BlockInfo;
use kona_rpc::L1WatcherQueries;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::actors::watcher::{
    BlockStream, actor::L1WatcherActor, error::L1WatcherActorBuilderError,
};

/// Implements the builder pattern for building an [`L1WatcherActor`].
#[derive(Debug, Default)]
pub struct L1WatcherActorBuilder {
    /// The [`RollupConfig`] to tell if ecotone is active.
    /// This is used to determine if the L1 watcher should check for unsafe block signer updates.
    pub rollup_config: Option<Arc<RollupConfig>>,
    /// The L1 provider.
    pub l1_provider: Option<RootProvider>,
    /// The inbound queries to the L1 watcher.
    pub inbound_queries: Option<mpsc::Receiver<L1WatcherQueries>>,
    /// The latest L1 head block.
    pub latest_head: Option<watch::Sender<Option<BlockInfo>>>,
    /// The latest L1 finalized block.
    pub latest_finalized: Option<watch::Sender<Option<BlockInfo>>>,
    /// The block signer sender.
    pub block_signer_sender: Option<mpsc::Sender<Address>>,
    /// The cancellation token, shared between all tasks.
    pub cancellation: Option<CancellationToken>,
    /// A stream over the latest head.
    pub head_stream: Option<BlockStream>,
    /// A stream over the finalized block accepted as canonical.
    pub finalized_stream: Option<BlockStream>,
}

impl L1WatcherActorBuilder {
    /// Instantiate the builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add [`RollupConfig`].
    pub fn with_rollup_config(mut self, rollup_config: Arc<RollupConfig>) -> Self {
        self.rollup_config = Some(rollup_config);
        self
    }

    /// Add [`RootProvider`] for L1.
    pub fn with_l1_provider(mut self, l1_provider: RootProvider) -> Self {
        self.l1_provider = Some(l1_provider);
        self
    }

    /// Add [`watch::Sender`] for [`BlockInfo`].
    pub fn with_latest_head(mut self, latest_head: watch::Sender<Option<BlockInfo>>) -> Self {
        self.latest_head = Some(latest_head);
        self
    }

    /// Add [`watch::Sender`] for [`BlockInfo`].
    pub fn with_latest_finalized(
        mut self,
        latest_finalized: watch::Sender<Option<BlockInfo>>,
    ) -> Self {
        self.latest_finalized = Some(latest_finalized);
        self
    }

    /// Add [`mpsc::Sender`] for [`Address`].
    pub fn with_block_signer_sender(mut self, block_signer_sender: mpsc::Sender<Address>) -> Self {
        self.block_signer_sender = Some(block_signer_sender);
        self
    }

    /// Add [`CancellationToken`].
    pub fn with_cancellation(mut self, cancellation: CancellationToken) -> Self {
        self.cancellation = Some(cancellation);
        self
    }

    /// Add [`BlockStream`] for latest blocks.
    pub fn with_head_stream(mut self, head_stream: BlockStream) -> Self {
        self.head_stream = Some(head_stream);
        self
    }

    /// Add [`BlockStream`] for finalized blocks.
    pub fn with_finalized_stream(mut self, finalized_stream: BlockStream) -> Self {
        self.finalized_stream = Some(finalized_stream);
        self
    }

    /// Build the [`L1WatcherActor`].  Returns an error if any field is missing.
    pub fn build(self) -> Result<L1WatcherActor, L1WatcherActorBuilderError> {
        Ok(L1WatcherActor {
            rollup_config: self.rollup_config.ok_or(L1WatcherActorBuilderError::BuildError(
                "no rollup config given".to_string(),
            ))?,
            l1_provider: self.l1_provider.ok_or(L1WatcherActorBuilderError::BuildError(
                "no l1 provider given".to_string(),
            ))?,
            latest_head: self.latest_head.ok_or(L1WatcherActorBuilderError::BuildError(
                "no latest head given".to_string(),
            ))?,
            latest_finalized: self.latest_finalized.ok_or(
                L1WatcherActorBuilderError::BuildError("no latest finalized given".to_string()),
            )?,
            block_signer_sender: self.block_signer_sender.ok_or(
                L1WatcherActorBuilderError::BuildError("no block signer sender given".to_string()),
            )?,
            cancellation: self.cancellation.ok_or(L1WatcherActorBuilderError::BuildError(
                "no cancellation given".to_string(),
            ))?,
            head_stream: self.head_stream.ok_or(L1WatcherActorBuilderError::BuildError(
                "no head stream given".to_string(),
            ))?,
            finalized_stream: self.finalized_stream.ok_or(
                L1WatcherActorBuilderError::BuildError("no finalized stream given".to_string()),
            )?,
        })
    }
}
