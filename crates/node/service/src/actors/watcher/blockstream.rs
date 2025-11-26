use std::time::Duration;

use alloy_eips::BlockNumberOrTag;
use alloy_provider::{Provider as _, RootProvider};
use alloy_rpc_client::PollerBuilder;
use alloy_rpc_types_eth::Block;
use async_stream::stream;
use futures::{Stream, StreamExt};
use kona_protocol::BlockInfo;

/// A wrapper around a [`PollerBuilder`] that observes [`BlockInfo`] updates on a [`RootProvider`].
///
/// Note that this stream is not guaranteed to be contiguous. It may miss certain blocks, and
/// yielded items should only be considered to be the latest block matching the given
/// [`BlockNumberOrTag`].
#[derive(Debug, Clone)]
pub struct BlockStream {
    /// The inner [`RootProvider`].
    l1_provider: RootProvider,
    /// The block tag to poll for.
    tag: BlockNumberOrTag,
    /// The poll interval (in seconds).
    poll_interval: Duration,
}

impl BlockStream {
    /// Creates a new [`BlockStream`] instance.
    ///
    /// # Returns
    /// Returns error if the passed [`BlockNumberOrTag`] is of the [`BlockNumberOrTag::Number`]
    /// variant.
    pub fn new(
        l1_provider: RootProvider,
        tag: BlockNumberOrTag,
        poll_interval: Duration,
    ) -> Result<Self, String> {
        if matches!(tag, BlockNumberOrTag::Number(_)) {
            error!("Invalid BlockNumberOrTag variant - Must be a tag");
        }
        Ok(Self { l1_provider, tag, poll_interval })
    }

    /// Creates a [`Stream`] of [`BlockInfo`].
    pub fn into_stream(self) -> impl Stream<Item = BlockInfo> + Unpin {
        let mut poll_stream = PollerBuilder::<(BlockNumberOrTag, bool), Block>::new(
            self.l1_provider.weak_client(),
            "eth_getBlockByNumber",
            (self.tag, false),
        )
        .with_poll_interval(self.poll_interval)
        .into_stream();

        Box::pin(stream! {
            let mut last_block = None;
            while let Some(next) = poll_stream.next().await {
                let info: BlockInfo = next.into_consensus().into();

                if last_block.map(|b| b != info).unwrap_or(true) {
                    last_block = Some(info);
                    yield info;
                }
            }
        })
    }
}
