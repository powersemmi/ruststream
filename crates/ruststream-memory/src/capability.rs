//! Native capability-trait implementations for the in-memory broker.
//!
//! These run on the broker's own in-process semantics: buffered transactions, greedy batching,
//! correlated request / reply over an in-process inbox. The conformance harness offers a suite
//! per capability (see
//! `conformance::capabilities`), and these implementations are the executable reference for
//! them.

#[cfg(feature = "testing")]
use ruststream_runtime::testing::Coordinator;
use std::{
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll, ready},
};

use super::{LogMode, MemoryMessage, MemorySubscriber};
use ruststream_core::{
    BatchStream, BatchSubscriber, IncomingMessage, MaybeSend, Partitioned, Subscriber,
};

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
/// needed - the in-memory transport has nothing to wait for. The batch is collected into the slot
/// the runtime lends, which the runtime sized for the registration once, when the subscription
/// started.
impl<Log: LogMode> BatchSubscriber for MemorySubscriber<Log> {
    type Batch = Vec<MemoryMessage<Log>>;

    fn batches(
        &mut self,
        size: NonZeroUsize,
    ) -> impl BatchStream<Self::Batch, Error = <Self as Subscriber>::Error> + MaybeSend + '_ {
        Greedy {
            #[cfg(feature = "testing")]
            coordinator: self.coordinator(),
            limit: size.get(),
            subscriber: self,
        }
    }
}

/// The batch stream of a [`MemorySubscriber`].
struct Greedy<'sub, Log: LogMode> {
    #[cfg(feature = "testing")]
    coordinator: Option<Coordinator>,
    limit: usize,
    subscriber: &'sub mut MemorySubscriber<Log>,
}

impl<Log: LogMode> BatchStream<Vec<MemoryMessage<Log>>> for Greedy<'_, Log> {
    type Error = <MemorySubscriber<Log> as Subscriber>::Error;

    // The drain happens inside a single poll, so no batch state is buffered between polls and
    // the stream stays cancel-safe, like `MemorySubscriber::stream`.
    fn poll_fill(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        slot: &mut Vec<MemoryMessage<Log>>,
    ) -> Poll<Option<Result<(), Self::Error>>> {
        let this = self.get_mut();
        let subscriber = &mut *this.subscriber;
        #[cfg(feature = "testing")]
        let coordinator = this.coordinator.as_ref();
        subscriber.begin_poll(cx);
        let first = ready!(subscriber.poll_fresh(
            cx,
            #[cfg(feature = "testing")]
            coordinator,
        ));
        let Some(first) = first else {
            return Poll::Ready(None);
        };
        slot.push(subscriber.message(
            first,
            #[cfg(feature = "testing")]
            coordinator,
        ));
        while slot.len() < this.limit {
            match subscriber.poll_fresh(
                cx,
                #[cfg(feature = "testing")]
                coordinator,
            ) {
                Poll::Ready(Some(delivery)) => slot.push(subscriber.message(
                    delivery,
                    #[cfg(feature = "testing")]
                    coordinator,
                )),
                // Drained (pending) or closed: ship what we have; a closed channel ends the
                // stream on the next poll.
                Poll::Ready(None) | Poll::Pending => break,
            }
        }
        Poll::Ready(Some(Ok(())))
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
    use std::convert::Infallible;
    use std::future::poll_fn;

    use futures::StreamExt;
    use ruststream_core::{BatchSlot, Settle};

    use super::*;
    use crate::{MemoryBroker, Retaining, Retention};
    use ruststream_core::{HeaderMap, OutgoingMessage, Publisher, nonzero};

    /// A broker whose subscriptions can be repositioned. The window is wider than anything these
    /// tests publish, so what they assert is the seek machinery, never an eviction.
    pub(super) fn replaying() -> MemoryBroker<Retaining> {
        MemoryBroker::retaining(Retention::Messages(nonzero!(64)))
    }

    /// The next batch the stream fills a fresh slot with, or `None` where the subscription ended.
    pub(super) async fn filled<Log: LogMode>(
        stream: &mut Pin<&mut impl BatchStream<Vec<MemoryMessage<Log>>, Error = Infallible>>,
    ) -> Option<Vec<MemoryMessage<Log>>> {
        let mut slot = Vec::new();
        let step = poll_fn(|cx| stream.as_mut().poll_fill(cx, &mut slot)).await;
        step.map(|_| slot)
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
        let batch = filled(&mut stream).await.unwrap();
        let payloads: Vec<u8> = batch.iter().map(|m| m.payload()[0]).collect();
        assert_eq!(payloads, [0, 1, 2, 3, 4]);
        for msg in batch {
            msg.ack().await.unwrap();
        }
    }

    /// A batch is collected into the slot the runtime lent, which already has the room for the
    /// registration's size: a short batch and a full one both leave its storage where it was.
    #[tokio::test]
    async fn a_batch_is_collected_into_the_lent_slot_without_growing_it() {
        let broker = MemoryBroker::new();
        let mut sub = broker.subscribe("batch.sized");
        let publisher = broker.publisher();
        for _ in 0..9 {
            publisher
                .publish(OutgoingMessage::new("batch.sized", b"a"), None)
                .await
                .unwrap();
        }

        let mut stream = std::pin::pin!(sub.batches(nonzero!(8)));
        let mut slot = Vec::with_capacity(8);
        let storage = slot.as_ptr();
        poll_fn(|cx| stream.as_mut().poll_fill(cx, &mut slot))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(slot.len(), 8);
        assert_eq!(slot.capacity(), 8);
        assert_eq!(slot.as_ptr(), storage);
        for msg in BatchSlot::drain(&mut slot) {
            msg.ack().await.unwrap();
        }
        poll_fn(|cx| stream.as_mut().poll_fill(cx, &mut slot))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(slot.len(), 1);
        assert_eq!(slot.as_ptr(), storage);
        for msg in slot {
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
