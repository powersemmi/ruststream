use futures::StreamExt;

use super::*;
use crate::runtime::PublishExt;

#[tokio::test]
async fn debug_formats_and_message_accessors() {
    let broker = MemoryBroker::new();
    assert!(format!("{broker:?}").contains("MemoryBroker"));

    let source = MemorySource::new("orders");
    assert_eq!(source.name(), "orders");

    let publisher = broker.publisher();
    assert!(format!("{publisher:?}").contains("MemoryPublisher"));

    let mut sub = broker.subscribe("dbg");
    assert!(format!("{sub:?}").contains("MemorySubscriber"));

    publisher.raw(b"payload").to("dbg").publish().await.unwrap();

    let mut stream = std::pin::pin!(sub.stream());
    let msg = stream.next().await.unwrap().unwrap();
    assert!(format!("{msg:?}").contains("MemoryMessage"));
    assert_eq!(msg.name(), "dbg");

    // into_raw consumes the delivery without acking, yielding a broker-agnostic message.
    let raw = msg.into_raw();
    assert_eq!(raw.name(), "dbg");
    assert_eq!(raw.payload(), b"payload");
}

#[tokio::test]
async fn a_reconnect_revives_a_bus_that_was_shut_down() {
    let broker = MemoryBroker::new();
    let connected = broker.clone().connect().await.unwrap();
    assert!(format!("{connected:?}").contains("ConnectedMemoryBroker"));
    connected.shutdown().await.unwrap();

    // The lazy-connect contract lets the same configuration open a fresh bus afterwards.
    let reconnected = broker.connect().await.unwrap();
    let mut subscriber = reconnected.subscribe("orders").await.unwrap();
    reconnected
        .publisher()
        .raw(b"after")
        .to("orders")
        .publish()
        .await
        .unwrap();

    let mut stream = std::pin::pin!(subscriber.stream());
    let delivered = stream.next().await.unwrap().unwrap();
    assert_eq!(delivered.payload(), b"after");
}

#[tokio::test]
async fn shutdown_reports_dropped_registrations() {
    let broker = MemoryBroker::new();
    let connected = broker
        .connect()
        .await
        .expect("memory connect is infallible");
    let _first = connected.subscribe("orders").await.unwrap();
    let _second = connected.subscribe("orders").await.unwrap();
    let _third = connected.subscribe("billing").await.unwrap();

    let closed = connected.shutdown().await.unwrap();
    assert_eq!(closed.subscribers_dropped(), 3);
}

#[tokio::test]
async fn shutdown_after_a_sibling_shutdown_reports_nothing_dropped() {
    let broker = MemoryBroker::new();
    let first = broker.clone().connect().await.unwrap();
    let second = broker.connect().await.unwrap();
    let _sub = first.subscribe("orders").await.unwrap();

    assert_eq!(first.shutdown().await.unwrap().subscribers_dropped(), 1);
    // The sibling shares the bus, which is already terminal: nothing left to drop.
    assert_eq!(second.shutdown().await.unwrap().subscribers_dropped(), 0);
}

// Paused time needs the current-thread runtime; the redelivery timer auto-advances instead
// of sleeping for real.
#[tokio::test(start_paused = true)]
async fn nack_after_redelivers_after_the_delay() {
    let broker = MemoryBroker::new();
    let mut sub = MemoryBroker::subscribe(&broker, "delayed");
    let publisher = broker.publisher();

    publisher
        .raw(b"later")
        .to("delayed")
        .publish()
        .await
        .unwrap();

    let mut stream = std::pin::pin!(sub.stream());
    let msg = stream.next().await.unwrap().unwrap();
    msg.nack_after(Duration::from_secs(5)).await.unwrap();

    // Nothing is redelivered while the delay has not elapsed.
    assert!(futures::poll!(stream.next()).is_pending());
    tokio::time::advance(Duration::from_secs(5)).await;
    // The timer task needs a tick to run before the redelivery is visible.
    tokio::task::yield_now().await;

    let redelivered = stream.next().await.unwrap().unwrap();
    assert_eq!(redelivered.payload(), b"later");
    redelivered.ack().await.unwrap();
}

#[tokio::test]
async fn stream_can_be_reentered() {
    let broker = MemoryBroker::new();
    let mut sub = MemoryBroker::subscribe(&broker, "test");
    let publisher = broker.publisher();

    publisher.raw(b"one").to("test").publish().await.unwrap();
    {
        let mut stream = std::pin::pin!(sub.stream());
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.payload(), b"one");
        msg.ack().await.unwrap();
    }

    // Helpers like `conformance::helpers::next_message` re-enter `stream` per call; the
    // subscriber must keep yielding after the first stream is dropped.
    publisher.raw(b"two").to("test").publish().await.unwrap();
    let mut stream = std::pin::pin!(sub.stream());
    let msg = stream.next().await.unwrap().unwrap();
    assert_eq!(msg.payload(), b"two");
    msg.ack().await.unwrap();
}
