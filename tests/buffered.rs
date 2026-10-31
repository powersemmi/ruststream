//! A subscriber wrapped in `Buffered` over the in-memory broker: batches close at their size or
//! their deadline, a plain stream passes through, and the seeker reaches the wrapped
//! subscriber.
#![cfg(feature = "memory")]

use std::num::NonZeroUsize;
use std::time::Duration;

use futures::StreamExt;

use ruststream::Settle;
use ruststream::memory::{LogMode, MemoryBroker, MemoryPosition, MemorySubscriber, Retention};
use ruststream::{
    BatchSubscriber, Broker, Buffered, BufferedSubscriber, IncomingMessage, Name, OutgoingMessage,
    Publisher, Seekable, Seeker, Subscriber, SubscriptionSource, nonzero,
};

/// The batch size the checks below open their stream at, spelled once.
fn batch(size: usize) -> NonZeroUsize {
    NonZeroUsize::new(size).expect("test sizes are nonzero")
}

// Generic over the broker's log mode: most checks here need no history, the seek one does.
async fn buffered<Log: LogMode>(
    broker: &MemoryBroker<Log>,
    max_wait: Duration,
) -> BufferedSubscriber<MemorySubscriber<Log>> {
    let connected = broker
        .clone()
        .connect()
        .await
        .expect("memory connect is infallible");
    Buffered::new(Name::new("buffered"))
        .max_wait(max_wait)
        .subscribe(&connected)
        .await
        .expect("a memory subscription opens")
}

/// The size the stream is opened at is what closes a full batch, and it is the
/// registration's own: the adapter has no size of its own to disagree with.
#[tokio::test]
async fn the_batch_size_closes_a_full_batch() {
    let broker = MemoryBroker::new();
    let mut sub = buffered(&broker, Duration::from_secs(60)).await;
    let publisher = broker.publisher();
    for i in 0..4u8 {
        publisher
            .publish(OutgoingMessage::new("buffered", &[i]), None)
            .await
            .unwrap();
    }

    // The wait bound is far away; only the size can close these batches.
    let mut stream = std::pin::pin!(sub.batches(batch(2)));
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.len(), 2);
    let second = stream.next().await.unwrap().unwrap();
    assert_eq!(second.len(), 2);
    for msg in first.into_iter().chain(second) {
        msg.ack().await.unwrap();
    }
}

// Paused time needs the current-thread runtime; the deadline auto-advances instead of
// sleeping for real. Nothing spawns.
#[tokio::test(start_paused = true)]
async fn deadline_flushes_a_partial_batch() {
    let broker = MemoryBroker::new();
    let mut sub = buffered(&broker, Duration::from_millis(10)).await;
    let publisher = broker.publisher();
    publisher
        .publish(OutgoingMessage::new("buffered", b"only"), None)
        .await
        .unwrap();

    let mut stream = std::pin::pin!(sub.batches(batch(64)));
    let batch = stream.next().await.unwrap().unwrap();
    assert_eq!(batch.len(), 1);
    assert_eq!(batch[0].payload(), b"only");
    for msg in batch {
        msg.ack().await.unwrap();
    }
}

/// The seeker is the wrapped subscriber's own, so a buffered subscription replays: the
/// batches that come out after the seek are assembled from the replayed deliveries.
#[tokio::test]
async fn the_seeker_reaches_through_the_buffer() {
    let broker = MemoryBroker::retaining(Retention::Messages(nonzero!(8)));
    let publisher = broker.publisher();
    for i in 0..2u8 {
        publisher
            .publish(OutgoingMessage::new("buffered", &[i]), None)
            .await
            .unwrap();
    }
    // Opened after the publishes, so only a reposition can reach them.
    let mut sub = buffered(&broker, Duration::from_millis(10)).await;
    sub.seeker()
        .seek(MemoryPosition::start())
        .await
        .expect("the in-memory log replays from the start");

    let mut stream = std::pin::pin!(sub.batches(batch(8)));
    let replayed = stream.next().await.unwrap().unwrap();
    let payloads: Vec<&[u8]> = replayed.iter().map(IncomingMessage::payload).collect();
    assert_eq!(payloads, [[0].as_slice(), [1].as_slice()]);
    for msg in replayed {
        msg.ack().await.unwrap();
    }
}

#[tokio::test]
async fn plain_stream_passes_through() {
    let broker = MemoryBroker::new();
    let mut sub = buffered(&broker, Duration::from_millis(10)).await;
    let publisher = broker.publisher();
    publisher
        .publish(OutgoingMessage::new("buffered", b"single"), None)
        .await
        .unwrap();

    let mut stream = std::pin::pin!(sub.stream());
    let msg = stream.next().await.unwrap().unwrap();
    assert_eq!(msg.payload(), b"single");
    msg.ack().await.unwrap();
}
