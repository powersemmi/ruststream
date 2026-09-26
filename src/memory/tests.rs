use futures::{FutureExt, StreamExt};

use super::*;

#[tokio::test]
async fn debug_formats_and_message_accessors() {
    let broker = MemoryBroker::new();
    assert!(format!("{broker:?}").contains("MemoryBroker"));

    let source = MemorySource::new("orders");
    // The source serves either log mode, so the call names the broker it is read for.
    assert_eq!(
        SubscriptionSource::<ConnectedMemoryBroker>::name(&source),
        "orders"
    );

    let publisher = broker.publisher();
    assert!(format!("{publisher:?}").contains("MemoryPublisher"));

    let mut sub = broker.subscribe("dbg");
    assert!(format!("{sub:?}").contains("MemorySubscriber"));

    publisher
        .publish(OutgoingMessage::new("dbg", b"payload"), None)
        .await
        .unwrap();

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
        .publish(OutgoingMessage::new("orders", b"after"), None)
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
        .publish(OutgoingMessage::new("delayed", b"later"), None)
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

/// A connect future built outside any runtime connects on the runtime that polls it.
#[test]
fn connect_captures_the_runtime_that_polls_it() {
    let connecting = MemoryBroker::new().connect();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    assert!(runtime.block_on(connecting).is_ok());
}

/// A subscription opened without connecting keeps no runtime: one opened on a runtime that has
/// since stopped is redelivered on the runtime that settles it.
#[test]
fn an_unconnected_subscription_redelivers_on_the_settling_runtime() {
    let broker = MemoryBroker::new();
    let opening = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let mut sub = opening.block_on(async { broker.subscribe("delayed") });
    drop(opening);

    let settling = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap();
    settling.block_on(async {
        broker
            .publisher()
            .publish(OutgoingMessage::new("delayed", b"later"), None)
            .await
            .unwrap();
        let mut stream = std::pin::pin!(sub.stream());
        let msg = stream.next().await.unwrap().unwrap();
        msg.nack_after(Duration::from_secs(5)).await.unwrap();
        let redelivered = tokio::time::timeout(Duration::from_secs(60), stream.next()).await;
        assert!(
            matches!(redelivered, Ok(Some(Ok(_)))),
            "the redelivery was spawned on the stopped runtime"
        );
    });
}

/// A pending redelivery keeps the way back to its subscription, not the bus: once the broker and
/// the subscriber are gone, the bus and its publish log are freed while the timer still waits.
#[tokio::test(start_paused = true)]
async fn a_pending_redelivery_does_not_keep_the_bus_alive() {
    let broker = MemoryBroker::retaining(Retention::Messages(crate::nonzero!(8)));
    let bus = Arc::downgrade(&broker.state);
    let mut sub = broker.subscribe("delayed");
    let publisher = broker.publisher();
    publisher
        .publish(OutgoingMessage::new("delayed", b"later"), None)
        .await
        .unwrap();
    {
        let mut stream = std::pin::pin!(sub.stream());
        let msg = stream.next().await.unwrap().unwrap();
        msg.nack_after(Duration::from_secs(3600)).await.unwrap();
    }

    drop((publisher, sub, broker));
    assert!(
        bus.upgrade().is_none(),
        "a redelivery an hour away holds the bus and its publish log",
    );

    // The timer still fires, into a subscription that is gone.
    tokio::time::advance(Duration::from_secs(3600)).await;
    tokio::task::yield_now().await;
}

/// What the default broker keeps: nothing. A service publishing forever on it grows only by
/// what its subscribers have yet to read.
#[tokio::test]
async fn a_discarding_broker_records_nothing_it_publishes() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();
    for i in 0..100u8 {
        publisher
            .publish(OutgoingMessage::new("orders", &[i]), None)
            .await
            .unwrap();
    }

    assert!(!broker.state.recording.load(Ordering::Acquire));
    assert!(broker.state.log.lock().unwrap().name("orders").is_none());
}

/// The bound holds per name: the newest messages stay, the oldest are evicted, and the
/// positions of what is left do not shift.
#[tokio::test]
async fn a_retaining_broker_evicts_past_its_message_bound() {
    let broker = MemoryBroker::retaining(Retention::Messages(crate::nonzero!(2)));
    let publisher = broker.publisher();
    for i in 0..5u8 {
        publisher
            .publish(OutgoingMessage::new("orders", &[i]), None)
            .await
            .unwrap();
    }

    let log = broker.state.log.lock().unwrap();
    let retained = log.name("orders").expect("the name was published to");
    let first_seq = retained.first_seq();
    let next_seq = retained.next_seq();
    let payloads: Vec<Vec<u8>> = retained
        .messages("orders")
        .iter()
        .map(|msg| msg.payload().to_vec())
        .collect();
    drop(log);

    assert_eq!(first_seq, 3, "the first three are evicted");
    assert_eq!(next_seq, 5, "positions stay absolute");
    assert_eq!(payloads, [vec![3], vec![4]]);
}

/// A payload wider than the byte bound is kept alone rather than dropped on arrival: the
/// message a publish just produced must be readable back.
#[tokio::test]
async fn a_byte_bound_keeps_the_newest_message_whatever_its_size() {
    let broker = MemoryBroker::retaining(Retention::Bytes(crate::nonzero!(4)));
    let publisher = broker.publisher();
    publisher
        .publish(OutgoingMessage::new("frames", &[0u8; 64]), None)
        .await
        .unwrap();

    let retained = {
        let log = broker.state.log.lock().unwrap();
        log.name("frames")
            .expect("the name was published to")
            .messages("frames")
    };
    assert_eq!(retained.len(), 1);
}

#[tokio::test]
async fn stream_can_be_reentered() {
    let broker = MemoryBroker::new();
    let mut sub = MemoryBroker::subscribe(&broker, "test");
    let publisher = broker.publisher();

    publisher
        .publish(OutgoingMessage::new("test", b"one"), None)
        .await
        .unwrap();
    {
        let mut stream = std::pin::pin!(sub.stream());
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.payload(), b"one");
        msg.ack().await.unwrap();
    }

    // Helpers like `conformance::helpers::next_message` re-enter `stream` per call; the
    // subscriber must keep yielding after the first stream is dropped.
    publisher
        .publish(OutgoingMessage::new("test", b"two"), None)
        .await
        .unwrap();
    let mut stream = std::pin::pin!(sub.stream());
    let msg = stream.next().await.unwrap().unwrap();
    assert_eq!(msg.payload(), b"two");
    msg.ack().await.unwrap();
}

/// One name per subscription rather than one per delivery: the fanout stamps the delivery with
/// the very handle the registration holds, which is what keeps the name off the allocator.
#[tokio::test]
async fn a_delivery_carries_the_name_the_registry_holds() {
    let broker = MemoryBroker::new();
    let mut subscriber = broker.subscribe("orders");
    broker
        .publisher()
        .publish(OutgoingMessage::new("orders", b"body"), None)
        .await
        .unwrap();

    let mut stream = std::pin::pin!(subscriber.stream());
    let delivered = stream.next().await.unwrap().unwrap();
    let stamped = &delivered
        .delivery
        .as_ref()
        .expect("the delivery is still held")
        .shared
        .name;

    let bus = broker.state.subscribers.lock().unwrap();
    let Bus::Live(registry) = &*bus else {
        panic!("the bus is live");
    };
    let (registered, _) = registry
        .names
        .get_key_value("orders")
        .expect("the subscription is registered");
    let registered = Arc::clone(registered);
    drop(bus);

    assert!(Arc::ptr_eq(&registered, stamped));
}

/// A publish to a name nothing reads is accepted and left nowhere: a broker that keeps no log
/// has no subscription to hand the message to, and a subscription opened afterwards starts at
/// the tip.
#[tokio::test]
async fn a_publish_nothing_reads_is_accepted_and_kept_nowhere() {
    let broker = MemoryBroker::new();
    broker
        .publisher()
        .publish(OutgoingMessage::new("unread", b"body"), None)
        .await
        .unwrap();

    let mut subscriber = broker.subscribe("unread");
    let mut stream = std::pin::pin!(subscriber.stream());
    assert!(stream.next().now_or_never().is_none());
}

/// The infallible constructor does not open a pattern: the stream ends at once instead of waiting
/// for publishes that never reach a name it did not register.
#[tokio::test]
async fn a_wildcard_name_on_the_inherent_subscribe_ends_the_stream() {
    let broker = MemoryBroker::new();
    let mut subscriber = broker.subscribe("orders.*");
    broker
        .publisher()
        .publish(OutgoingMessage::new("orders.*", b"body"), None)
        .await
        .unwrap();
    let mut stream = std::pin::pin!(subscriber.stream());
    assert!(stream.next().await.is_none());
}

/// Under `MostSpecific` a subscription that has gone takes nothing: an exact name or a more
/// specific pattern whose subscribers were all dropped leaves the publish to the next match that
/// still reads.
#[tokio::test]
async fn most_specific_skips_subscriptions_that_have_gone() {
    let connected = MemoryBroker::new()
        .routing(Routing::MostSpecific)
        .connect()
        .await
        .unwrap();
    let exact = connected.subscribe("orders.eu.created").await.unwrap();
    let specific = MemoryPattern::new("orders.eu.*")
        .subscribe(&connected)
        .await
        .unwrap();
    let mut general = MemoryPattern::new("orders.>")
        .subscribe(&connected)
        .await
        .unwrap();
    drop(exact);
    drop(specific);

    connected
        .publisher()
        .publish(OutgoingMessage::new("orders.eu.created", b"body"), None)
        .await
        .unwrap();
    let mut stream = std::pin::pin!(general.stream());
    assert_eq!(
        stream
            .next()
            .now_or_never()
            .flatten()
            .unwrap()
            .unwrap()
            .name(),
        "orders.eu.created"
    );
}

/// A pattern subscription reads every name it matches, stamped with the name it was published
/// to, and a shut-down bus keeps the routing rule for its revival.
#[tokio::test]
async fn a_pattern_reads_every_match_and_the_rule_survives_shutdown() {
    let broker = MemoryBroker::new().routing(Routing::MostSpecific);
    let connected = broker.clone().connect().await.unwrap();
    let mut pattern = MemoryPattern::new("orders.>")
        .subscribe(&connected)
        .await
        .unwrap();
    let publisher = connected.publisher();
    for name in ["orders.eu", "orders.eu.created", "invoices.eu"] {
        publisher
            .publish(OutgoingMessage::new(name, b"body"), None)
            .await
            .unwrap();
    }
    {
        let mut stream = std::pin::pin!(pattern.stream());
        assert_eq!(stream.next().await.unwrap().unwrap().name(), "orders.eu");
        assert_eq!(
            stream.next().await.unwrap().unwrap().name(),
            "orders.eu.created"
        );
        assert!(stream.next().now_or_never().is_none());
    }

    let closed = connected.shutdown().await.unwrap();
    assert_eq!(closed.subscribers_dropped(), 1);
    let revived = broker.connect().await.unwrap();
    assert_eq!(revived.state.routing(), Routing::MostSpecific);
}
