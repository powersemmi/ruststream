//! Integration tests for the owned transaction kind: every `OwnedTransactions::transaction`
//! call opens an independent client buffer that commits atomically or is discarded, without
//! touching its sibling transactions or the handle's direct publishes.
#![cfg(feature = "memory")]

use std::convert::Infallible;
use std::pin::pin;

use futures::{FutureExt, Stream, StreamExt};
use ruststream::memory::{MemoryBroker, MemoryError, MemoryMessage};
use ruststream::{
    Broker, ConnectedBroker, IncomingMessage, OutgoingMessage, OwnedTransactions, Publisher,
    Subscriber, Transaction,
};

/// Drains the next already-enqueued delivery (acking it), or `None` when the queue is empty.
/// Memory fanout is synchronous, so absence here proves nothing was published.
async fn drain_next<S>(stream: &mut S) -> Option<Vec<u8>>
where
    S: Stream<Item = Result<MemoryMessage, Infallible>> + Unpin,
{
    let msg = stream
        .next()
        .now_or_never()
        .flatten()?
        .expect("memory subscriber never errors");
    let payload = msg.payload().to_vec();
    msg.ack().await.expect("ack failed");
    Some(payload)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_transactions_commit_independently_and_atomically() {
    let broker = MemoryBroker::new();
    let mut subscriber = broker.subscribe("orders");
    let publisher = broker.publisher();

    let mut first = publisher.transaction().await.expect("first transaction");
    let mut second = publisher.transaction().await.expect("second transaction");

    first
        .publish(OutgoingMessage::new("orders", b"first-a".as_slice()))
        .await
        .expect("publish into first failed");
    second
        .publish(OutgoingMessage::new("orders", b"second-a".as_slice()))
        .await
        .expect("publish into second failed");
    first
        .publish(OutgoingMessage::new("orders", b"first-b".as_slice()))
        .await
        .expect("publish into first failed");

    // The handle stays direct while both transactions are open.
    publisher
        .publish(OutgoingMessage::new("orders", b"direct".as_slice()))
        .await
        .expect("direct publish failed");

    let mut stream = pin!(subscriber.stream());
    assert_eq!(
        drain_next(&mut stream).await.as_deref(),
        Some(b"direct".as_slice())
    );
    assert_eq!(
        drain_next(&mut stream).await,
        None,
        "buffered publishes became visible before commit"
    );

    // Settling one transaction publishes exactly its buffer, leaving the sibling untouched.
    second.commit().await.expect("second commit failed");
    assert_eq!(
        drain_next(&mut stream).await.as_deref(),
        Some(b"second-a".as_slice())
    );
    assert_eq!(
        drain_next(&mut stream).await,
        None,
        "the sibling transaction leaked into the commit"
    );

    first.commit().await.expect("first commit failed");
    for expected in [b"first-a".as_slice(), b"first-b".as_slice()] {
        assert_eq!(drain_next(&mut stream).await.as_deref(), Some(expected));
    }
    assert_eq!(drain_next(&mut stream).await, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn abort_discards_the_owned_buffer() {
    let broker = MemoryBroker::new();
    let mut subscriber = broker.subscribe("orders");
    let publisher = broker.publisher();

    let mut txn = publisher.transaction().await.expect("transaction failed");
    txn.publish(OutgoingMessage::new("orders", b"gone".as_slice()))
        .await
        .expect("publish into the transaction failed");
    txn.abort().await.expect("abort failed");

    let mut stream = pin!(subscriber.stream());
    assert_eq!(
        drain_next(&mut stream).await,
        None,
        "aborted publish became visible"
    );

    // The handle is unaffected by the settled transaction.
    publisher
        .publish(OutgoingMessage::new("orders", b"kept".as_slice()))
        .await
        .expect("direct publish failed");
    assert_eq!(
        drain_next(&mut stream).await.as_deref(),
        Some(b"kept".as_slice())
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn commit_against_a_shut_down_bus_errors() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    let mut txn = publisher.transaction().await.expect("transaction failed");
    txn.publish(OutgoingMessage::new("orders", b"buffered".as_slice()))
        .await
        .expect("publish into the transaction failed");

    let connected = broker.connect().await.expect("connect failed");
    connected.shutdown().await.expect("shutdown failed");

    // Buffering never touched the bus, so the shutdown surfaces at the visibility point.
    assert_eq!(txn.commit().await, Err(MemoryError::ShutDown));
}

/// The typed sugar (`TypedPublisher::transaction`) over the same owned kind; the default codec
/// needs a codec feature, hence the gate.
#[cfg(feature = "json")]
mod typed {
    use ruststream::Outgoing;
    use ruststream::codec::{Codec, DefaultCodec};
    use ruststream::runtime::TypedPublisher;
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Outgoing, PartialEq, Serialize, Deserialize)]
    struct Order {
        id: u32,
    }

    fn decode_order(payload: &[u8]) -> Order {
        DefaultCodec::default()
            .decode(payload)
            .expect("decoding a committed payload failed")
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn typed_publish_round_trips_through_the_default_codec() {
        let broker = MemoryBroker::new();
        let mut subscriber = broker.subscribe("orders");
        let publisher = TypedPublisher::new(broker.publisher());

        let mut txn = publisher.transaction().await.expect("transaction failed");
        txn.message(&Order { id: 7 })
            .to("orders")
            .publish()
            .await
            .expect("typed publish into the transaction failed");

        let mut stream = pin!(subscriber.stream());
        assert_eq!(
            drain_next(&mut stream).await,
            None,
            "buffered typed publish became visible before commit"
        );

        txn.commit().await.expect("commit failed");
        let payload = drain_next(&mut stream)
            .await
            .expect("committed publish missing");
        assert_eq!(decode_order(&payload), Order { id: 7 });
        assert_eq!(drain_next(&mut stream).await, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_typed_scopes_commit_independently() {
        let broker = MemoryBroker::new();
        let mut subscriber = broker.subscribe("orders");
        let publisher = TypedPublisher::new(broker.publisher());

        let mut first = publisher.transaction().await.expect("first transaction");
        let mut second = publisher.transaction().await.expect("second transaction");
        first
            .message(&Order { id: 1 })
            .to("orders")
            .publish()
            .await
            .expect("publish into first failed");
        second
            .message(&Order { id: 2 })
            .to("orders")
            .publish()
            .await
            .expect("publish into second failed");

        // Settling one scope publishes exactly its buffer, leaving the sibling untouched.
        second.commit().await.expect("second commit failed");
        let mut stream = pin!(subscriber.stream());
        let payload = drain_next(&mut stream)
            .await
            .expect("second scope's publish missing");
        assert_eq!(decode_order(&payload), Order { id: 2 });
        assert_eq!(
            drain_next(&mut stream).await,
            None,
            "the sibling scope leaked into the commit"
        );

        first.commit().await.expect("first commit failed");
        let payload = drain_next(&mut stream)
            .await
            .expect("first scope's publish missing");
        assert_eq!(decode_order(&payload), Order { id: 1 });
        assert_eq!(drain_next(&mut stream).await, None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn typed_commit_against_a_shut_down_bus_errors() {
        let broker = MemoryBroker::new();
        let publisher = TypedPublisher::new(broker.publisher());

        let mut txn = publisher.transaction().await.expect("transaction failed");
        txn.message(&Order { id: 9 })
            .to("orders")
            .publish()
            .await
            .expect("typed publish into the transaction failed");

        let connected = broker.connect().await.expect("connect failed");
        connected.shutdown().await.expect("shutdown failed");

        // Buffering never touched the bus, so the shutdown surfaces at the visibility point.
        assert_eq!(txn.commit().await, Err(MemoryError::ShutDown));
    }
}
