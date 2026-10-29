//! Native capability-trait implementations for the in-memory broker.
//!
//! These run on the broker's own in-process semantics: buffered transactions, greedy batching,
//! correlated request / reply over an in-process inbox. The conformance harness offers a suite
//! per capability (see
//! `conformance::capabilities`), and these implementations are the executable reference for
//! them.

use std::{
    num::NonZeroUsize,
    task::{Poll, ready},
};

use futures::{Stream, stream};

use super::{LogMode, MemoryMessage, MemorySubscriber};
use ruststream_core::{BatchSubscriber, IncomingMessage, Partitioned, Subscriber};

/// The well-known header the [`Partitioned`] implementation reads the partition key from.
pub const PARTITION_KEY_HEADER: &str = "partition-key";

mod context;
mod request;
mod seek;
mod transaction;

pub use context::{MemoryBatchContext, MemoryContext, Position, SeekHandle};
pub use request::{MemoryRequester, RequestError};
pub(crate) use seek::SeekControl;
pub use seek::{MemoryPosition, MemorySeeker};
pub use transaction::MemoryTransaction;

/// Greedy batching: a batch is the first awaited delivery plus everything already buffered, up to
/// the size the registration asked for. Partial batches ship immediately, so no deadline timer is
/// needed - the in-memory transport has nothing to wait for. Each batch is allocated at the
/// requested size up front, so a partial batch holds the room of a full one.
impl<Log: LogMode> BatchSubscriber for MemorySubscriber<Log> {
    type Batch = Vec<MemoryMessage<Log>>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl Stream<Item = Result<Self::Batch, <Self as Subscriber>::Error>> + Send + '_ {
        let limit = size.get();
        #[cfg(feature = "testing")]
        let coordinator = self.coordinator();
        // The drain happens inside a single poll, so no batch state is buffered between polls
        // and the stream stays cancel-safe, like `MemorySubscriber::stream`.
        stream::poll_fn(move |cx| {
            self.begin_poll(cx);
            let first = ready!(self.poll_fresh(
                cx,
                #[cfg(feature = "testing")]
                coordinator.as_ref(),
            ));
            let Some(first) = first else {
                return Poll::Ready(None);
            };
            // Sized for the batch the registration asked for: a batch grown one push at a time
            // reallocates six times on its way to 64.
            let mut batch = Vec::with_capacity(limit);
            batch.push(self.message(
                first,
                #[cfg(feature = "testing")]
                coordinator.as_ref(),
            ));
            while batch.len() < limit {
                match self.poll_fresh(
                    cx,
                    #[cfg(feature = "testing")]
                    coordinator.as_ref(),
                ) {
                    Poll::Ready(Some(delivery)) => batch.push(self.message(
                        delivery,
                        #[cfg(feature = "testing")]
                        coordinator.as_ref(),
                    )),
                    // Drained (pending) or closed: ship what we have; a closed channel ends
                    // the stream on the next poll.
                    Poll::Ready(None) | Poll::Pending => break,
                }
            }
            Poll::Ready(Some(Ok(batch)))
        })
    }
}

/// The partition key is carried in the [`PARTITION_KEY_HEADER`] header; producers that want
/// per-key ordering set it at publish time. Messages without the header return `None` (any
/// partition will do).
impl<Log: LogMode> Partitioned for MemoryMessage<Log> {
    fn partition_key(&self) -> Option<&[u8]> {
        self.headers().get(PARTITION_KEY_HEADER)
    }
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;
    use crate::{MemoryBroker, Retaining, Retention};
    use ruststream_core::{HeaderMap, OutgoingMessage, Publisher, nonzero};

    /// A broker whose subscriptions can be repositioned. The window is wider than anything these
    /// tests publish, so what they assert is the seek machinery, never an eviction.
    pub(super) fn replaying() -> MemoryBroker<Retaining> {
        MemoryBroker::retaining(Retention::Messages(nonzero!(64)))
    }

    #[tokio::test]
    async fn batches_drain_buffered_deliveries() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("batch");
        let publisher = broker.publisher();
        for i in 0..5u8 {
            publisher
                .publish(OutgoingMessage::new("batch", &[i]), None)
                .await
                .unwrap();
        }

        let mut stream = std::pin::pin!(sub.batches(nonzero!(64)));
        let batch = stream.next().await.unwrap().unwrap();
        let payloads: Vec<u8> = batch.iter().map(|m| m.payload()[0]).collect();
        assert_eq!(payloads, [0, 1, 2, 3, 4]);
        for msg in batch {
            msg.ack().await.unwrap();
        }
    }

    /// A batch is allocated at the size the stream was opened at, so a short one holds the room of a
    /// full one and a full one never grows on the way.
    #[tokio::test]
    async fn a_batch_is_allocated_at_the_requested_size() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("batch.sized");
        let publisher = broker.publisher();
        publisher
            .publish(OutgoingMessage::new("batch.sized", b"a"), None)
            .await
            .unwrap();

        let mut stream = std::pin::pin!(sub.batches(nonzero!(8)));
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.len(), 1);
        assert!(batch.capacity() >= 8, "capacity {}", batch.capacity());
        for msg in batch {
            msg.ack().await.unwrap();
        }
    }

    #[tokio::test]
    async fn partition_key_reads_well_known_header() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("keyed");
        let publisher = broker.publisher();

        let mut headers = HeaderMap::new();
        headers.insert(PARTITION_KEY_HEADER, b"user-42".as_slice());
        publisher
            .publish(
                OutgoingMessage::new("keyed", b"a").with_headers(headers),
                None,
            )
            .await
            .unwrap();
        publisher
            .publish(OutgoingMessage::new("keyed", b"b"), None)
            .await
            .unwrap();

        let mut stream = std::pin::pin!(sub.stream());
        let keyed = stream.next().await.unwrap().unwrap();
        assert_eq!(
            Partitioned::partition_key(&keyed),
            Some(b"user-42".as_slice())
        );
        // The IncomingMessage hook must agree with the capability trait.
        assert_eq!(
            IncomingMessage::partition_key(&keyed),
            Some(b"user-42".as_slice())
        );
        keyed.ack().await.unwrap();

        let unkeyed = stream.next().await.unwrap().unwrap();
        assert_eq!(Partitioned::partition_key(&unkeyed), None);
        unkeyed.ack().await.unwrap();
    }
}
