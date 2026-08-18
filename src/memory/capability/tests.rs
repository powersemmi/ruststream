use futures::StreamExt;

use super::super::{MemoryBroker, MemorySource};
use super::*;
#[cfg(feature = "testing")]
use crate::Subscribe;
#[cfg(feature = "testing")]
use crate::testing::{TestableBroker, coordinator::Coordinator};
use crate::{Broker, ConnectedBroker, Headers, StartAt, SubscriptionSource};

#[tokio::test]
async fn batches_drain_buffered_deliveries() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("batch");
    let publisher = broker.publisher();
    for i in 0..5u8 {
        publisher
            .publish(OutgoingMessage::new("batch", &[i]))
            .await
            .unwrap();
    }

    let mut stream = std::pin::pin!(sub.batches());
    let batch = stream.next().await.unwrap().unwrap();
    let payloads: Vec<u8> = batch.iter().map(|m| m.payload()[0]).collect();
    assert_eq!(payloads, [0, 1, 2, 3, 4]);
    for msg in batch {
        msg.ack().await.unwrap();
    }
}

#[tokio::test]
async fn batch_limit_caps_each_batch() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("batch.capped");
    sub.set_batch_limit(2);
    let publisher = broker.publisher();
    for i in 0..3u8 {
        publisher
            .publish(OutgoingMessage::new("batch.capped", &[i]))
            .await
            .unwrap();
    }

    let mut stream = std::pin::pin!(sub.batches());
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.len(), 2);
    let second = stream.next().await.unwrap().unwrap();
    assert_eq!(second.len(), 1);
    for msg in first.into_iter().chain(second) {
        msg.ack().await.unwrap();
    }
}

#[tokio::test]
async fn transaction_buffers_until_commit() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("txn");
    let publisher = broker.publisher();

    publisher.begin_transaction().await.unwrap();
    publisher
        .publish(OutgoingMessage::new("txn", b"a".as_slice()))
        .await
        .unwrap();
    publisher
        .publish(OutgoingMessage::new("txn", b"b".as_slice()))
        .await
        .unwrap();

    // Fanout is synchronous, so an empty queue here proves nothing was published yet.
    let mut stream = std::pin::pin!(sub.stream());
    assert!(futures::poll!(stream.next()).is_pending());

    publisher.commit().await.unwrap();
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.payload(), b"a");
    first.ack().await.unwrap();
    let second = stream.next().await.unwrap().unwrap();
    assert_eq!(second.payload(), b"b");
    second.ack().await.unwrap();
}

#[tokio::test]
async fn abort_discards_buffered_publishes() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("txn.abort");
    let publisher = broker.publisher();

    publisher.begin_transaction().await.unwrap();
    publisher
        .publish(OutgoingMessage::new("txn.abort", b"gone".as_slice()))
        .await
        .unwrap();
    publisher.abort().await.unwrap();

    let mut stream = std::pin::pin!(sub.stream());
    assert!(futures::poll!(stream.next()).is_pending());

    publisher
        .publish(OutgoingMessage::new("txn.abort", b"kept".as_slice()))
        .await
        .unwrap();
    let msg = stream.next().await.unwrap().unwrap();
    assert_eq!(msg.payload(), b"kept");
    msg.ack().await.unwrap();
}

#[tokio::test]
async fn clone_does_not_join_transaction() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("txn.clone");
    let transactional = broker.publisher();

    transactional.begin_transaction().await.unwrap();
    transactional
        .publish(OutgoingMessage::new("txn.clone", b"buffered".as_slice()))
        .await
        .unwrap();

    let independent = transactional.clone();
    independent
        .publish(OutgoingMessage::new("txn.clone", b"direct".as_slice()))
        .await
        .unwrap();

    let mut stream = std::pin::pin!(sub.stream());
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.payload(), b"direct");
    first.ack().await.unwrap();

    transactional.commit().await.unwrap();
    let second = stream.next().await.unwrap().unwrap();
    assert_eq!(second.payload(), b"buffered");
    second.ack().await.unwrap();
}

#[tokio::test]
async fn transactional_misuse_errors() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    assert_eq!(publisher.commit().await, Err(MemoryError::NoTransaction));
    assert_eq!(publisher.abort().await, Err(MemoryError::NoTransaction));

    publisher.begin_transaction().await.unwrap();
    assert_eq!(
        publisher.begin_transaction().await,
        Err(MemoryError::TransactionBusy),
    );
    // The rejected second begin left the transaction open; an abort settles it.
    publisher.abort().await.unwrap();
    assert_eq!(publisher.abort().await, Err(MemoryError::NoTransaction));
}

#[tokio::test]
async fn commit_after_shutdown_errors() {
    let broker = MemoryBroker::new();
    let publisher = broker.publisher();

    publisher.begin_transaction().await.unwrap();
    publisher
        .publish(OutgoingMessage::new("txn.down", b"buffered".as_slice()))
        .await
        .unwrap();
    let connected = broker.connect().await.unwrap();
    connected.shutdown().await.unwrap();

    // Buffering never touched the bus, so the shutdown surfaces at the visibility point.
    assert_eq!(publisher.commit().await, Err(MemoryError::ShutDown));
}

#[tokio::test(start_paused = true)]
async fn a_request_without_a_responder_times_out_naming_the_subject() {
    let broker = MemoryBroker::new();
    // Subscribed but never answering: the request must expire instead of hanging.
    let _service = broker.subscribe("svc.silent");
    let requester = broker.requester();

    let outcome = requester
        .request(
            OutgoingMessage::new("svc.silent", b"ping".as_slice()),
            Duration::from_secs(5),
        )
        .await;

    match outcome {
        Err(RequestError::Timeout { subject, timeout }) => {
            assert_eq!(subject, "svc.silent");
            assert_eq!(timeout, Duration::from_secs(5));
        }
        other => panic!("expected a timeout naming the subject, got {other:?}"),
    }
}

#[tokio::test]
async fn an_owned_transaction_dropped_unsettled_discards_its_buffer() {
    let broker = MemoryBroker::new();
    let mut subscriber = broker.subscribe("orders");
    let publisher = broker.publisher();

    let mut transaction = publisher.transaction().await.unwrap();
    transaction
        .publish(OutgoingMessage::new("orders", b"buffered".as_slice()))
        .await
        .unwrap();
    assert!(format!("{transaction:?}").contains("buffered: 1"));

    // Dropping without commit or abort is an implicit abort: nothing becomes visible.
    drop(transaction);
    let mut stream = std::pin::pin!(subscriber.stream());
    assert!(futures::poll!(stream.next()).is_pending());
}

#[test]
fn the_capability_debug_forms_name_their_subject_without_leaking_state() {
    let broker = MemoryBroker::new();
    assert!(format!("{:?}", broker.requester()).contains("MemoryRequester"));

    let seeker = broker.subscribe("seek.debug").seeker();
    let rendered = format!("{seeker:?}");
    // A seeker is only meaningful against its name, so Debug has to carry it.
    assert!(rendered.contains("seek.debug"), "{rendered}");
}

#[tokio::test]
async fn requester_errors_after_shutdown() {
    let broker = MemoryBroker::new();
    let requester = broker.requester();
    let connected = broker.connect().await.unwrap();
    connected.shutdown().await.unwrap();

    let publish = Publisher::publish(
        &requester,
        OutgoingMessage::new("svc.echo", b"ping".as_slice()),
    )
    .await;
    assert!(
        matches!(publish, Err(RequestError::ShutDown)),
        "{publish:?}"
    );

    let request = requester
        .request(
            OutgoingMessage::new("svc.echo", b"ping".as_slice()),
            Duration::from_millis(50),
        )
        .await;
    assert!(
        matches!(request, Err(RequestError::ShutDown)),
        "a request against a dead bus must fail fast, not time out",
    );
}

#[tokio::test]
async fn request_resolves_on_reply() {
    let broker = MemoryBroker::new();
    let mut service = broker.subscribe("svc.echo");
    let publisher = broker.publisher();
    let requester = broker.requester();

    let respond = async {
        let mut stream = std::pin::pin!(service.stream());
        let msg = stream.next().await.unwrap().unwrap();
        assert_eq!(msg.payload(), b"ping");
        let reply_to = msg.headers().reply_to().unwrap().to_owned();
        publisher
            .publish(OutgoingMessage::new(&reply_to, b"pong".as_slice()))
            .await
            .unwrap();
        msg.ack().await.unwrap();
    };
    let request = requester.request(
        OutgoingMessage::new("svc.echo", b"ping".as_slice()),
        Duration::from_secs(1),
    );

    let (reply, ()) = futures::join!(request, respond);
    assert_eq!(reply.unwrap().payload(), b"pong");

    // The single-use inbox must be unregistered once the request resolves.
    let inbox_leaked = match &*broker.state.subscribers.lock().unwrap() {
        Bus::Live(subscribers) => subscribers.keys().any(|name| name.starts_with("_inbox.")),
        Bus::ShutDown => false,
    };
    assert!(!inbox_leaked);
}

// Paused time needs the current-thread runtime; the test spawns nothing, so the timeout
// auto-advances instead of sleeping for real.
#[tokio::test(start_paused = true)]
async fn request_times_out_without_responder() {
    let broker = MemoryBroker::new();
    let requester = broker.requester();

    let outcome = requester
        .request(
            OutgoingMessage::new("svc.void", b"ping".as_slice()),
            Duration::from_millis(5),
        )
        .await;
    assert!(matches!(outcome, Err(RequestError::Timeout { .. })));

    let inbox_leaked = match &*broker.state.subscribers.lock().unwrap() {
        Bus::Live(subscribers) => subscribers.keys().any(|name| name.starts_with("_inbox.")),
        Bus::ShutDown => false,
    };
    assert!(!inbox_leaked);
}

#[tokio::test]
async fn seek_back_redelivers_from_the_captured_position() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("seek.back");
    let seeker = sub.seeker();
    let publisher = broker.publisher();
    for payload in [b"a", b"b", b"c"] {
        publisher
            .publish(OutgoingMessage::new("seek.back", payload.as_slice()))
            .await
            .unwrap();
    }

    let mut stream = std::pin::pin!(sub.stream());
    let mut positions = Vec::new();
    for _ in 0..3 {
        let msg = stream.next().await.unwrap().unwrap();
        positions.push(msg.position());
        msg.ack().await.unwrap();
    }

    seeker.seek(positions[1]).await.unwrap();
    let redelivered = stream.next().await.unwrap().unwrap();
    assert_eq!(redelivered.payload(), b"b");
    // The replayed copy reports the same position as the original delivery.
    assert_eq!(redelivered.position(), positions[1]);
    redelivered.ack().await.unwrap();
    let tail = stream.next().await.unwrap().unwrap();
    assert_eq!(tail.payload(), b"c");
    tail.ack().await.unwrap();
    assert!(futures::poll!(stream.next()).is_pending());
}

#[tokio::test]
async fn constructed_position_seeks_forward_skipping_queued() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("seek.fwd");
    let seeker = sub.seeker();
    let publisher = broker.publisher();
    for payload in [b"a", b"b", b"c"] {
        publisher
            .publish(OutgoingMessage::new("seek.fwd", payload.as_slice()))
            .await
            .unwrap();
    }

    let mut stream = std::pin::pin!(sub.stream());
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.payload(), b"a");
    first.ack().await.unwrap();

    // "b" is still queued; jumping to the third message must skip it.
    seeker.seek(MemoryPosition::sequence(2)).await.unwrap();
    let skipped_to = stream.next().await.unwrap().unwrap();
    assert_eq!(skipped_to.payload(), b"c");
    skipped_to.ack().await.unwrap();
    assert!(futures::poll!(stream.next()).is_pending());
}

#[tokio::test]
async fn stale_requeue_racing_a_seek_is_dropped() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("seek.stale");
    let seeker = sub.seeker();
    let publisher = broker.publisher();
    for payload in [b"a", b"b", b"c"] {
        publisher
            .publish(OutgoingMessage::new("seek.stale", payload.as_slice()))
            .await
            .unwrap();
    }

    let mut stream = std::pin::pin!(sub.stream());
    let held = stream.next().await.unwrap().unwrap();
    assert_eq!(held.payload(), b"a");

    seeker.seek(MemoryPosition::sequence(2)).await.unwrap();
    let skipped_to = stream.next().await.unwrap().unwrap();
    assert_eq!(skipped_to.payload(), b"c");
    skipped_to.ack().await.unwrap();

    // The requeue lands after the seek; its copy is below the watermark and is dropped
    // instead of resurrecting a delivery the reposition already skipped.
    held.nack(true).await.unwrap();
    assert!(futures::poll!(stream.next()).is_pending());

    publisher
        .publish(OutgoingMessage::new("seek.stale", b"d".as_slice()))
        .await
        .unwrap();
    let live = stream.next().await.unwrap().unwrap();
    assert_eq!(live.payload(), b"d");
    live.ack().await.unwrap();
}

#[tokio::test]
async fn seek_past_the_end_skips_the_queue_and_resumes_with_the_next_publish() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("seek.end");
    let seeker = sub.seeker();
    let publisher = broker.publisher();
    for payload in [b"a", b"b"] {
        publisher
            .publish(OutgoingMessage::new("seek.end", payload.as_slice()))
            .await
            .unwrap();
    }

    // The target is clamped to the log end at seek time, so everything queued is skipped
    // and the next publish is not filtered away.
    seeker.seek(MemoryPosition::sequence(10)).await.unwrap();
    let mut stream = std::pin::pin!(sub.stream());
    assert!(futures::poll!(stream.next()).is_pending());

    publisher
        .publish(OutgoingMessage::new("seek.end", b"c".as_slice()))
        .await
        .unwrap();
    let live = stream.next().await.unwrap().unwrap();
    assert_eq!(live.payload(), b"c");
    live.ack().await.unwrap();
}

#[tokio::test]
async fn start_at_replays_the_log_into_a_fresh_subscription() {
    let broker = MemoryBroker::new();
    let connected = broker.connect().await.unwrap();
    let publisher = connected.publisher();
    for payload in [b"a", b"b"] {
        publisher
            .publish(OutgoingMessage::new("start.replay", payload.as_slice()))
            .await
            .unwrap();
    }

    // The subscription opens after both publishes; the start position replays them.
    let mut sub = StartAt::new(MemorySource::new("start.replay"), MemoryPosition::start())
        .subscribe(&connected)
        .await
        .unwrap();
    let mut stream = std::pin::pin!(sub.stream());
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.payload(), b"a");
    first.ack().await.unwrap();
    let second = stream.next().await.unwrap().unwrap();
    assert_eq!(second.payload(), b"b");
    second.ack().await.unwrap();
    assert!(futures::poll!(stream.next()).is_pending());
}

#[tokio::test]
async fn start_at_end_skips_history_and_sees_the_next_publish() {
    let broker = MemoryBroker::new();
    let connected = broker.connect().await.unwrap();
    let publisher = connected.publisher();
    publisher
        .publish(OutgoingMessage::new("start.end", b"old".as_slice()))
        .await
        .unwrap();

    let mut sub = StartAt::new(MemorySource::new("start.end"), MemoryPosition::end())
        .subscribe(&connected)
        .await
        .unwrap();
    let mut stream = std::pin::pin!(sub.stream());
    assert!(futures::poll!(stream.next()).is_pending());

    publisher
        .publish(OutgoingMessage::new("start.end", b"new".as_slice()))
        .await
        .unwrap();
    let live = stream.next().await.unwrap().unwrap();
    assert_eq!(live.payload(), b"new");
    live.ack().await.unwrap();
}

#[tokio::test]
async fn seeker_errors_after_shutdown() {
    let broker = MemoryBroker::new();
    let sub = broker.subscribe("seek.down");
    let seeker = sub.seeker();
    let connected = broker.connect().await.unwrap();
    connected.shutdown().await.unwrap();

    assert_eq!(
        seeker.seek(MemoryPosition::start()).await,
        Err(MemoryError::ShutDown),
    );
}

#[tokio::test]
async fn batches_replay_after_a_seek() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("seek.batch");
    let seeker = sub.seeker();
    let publisher = broker.publisher();
    for payload in [b"a", b"b", b"c"] {
        publisher
            .publish(OutgoingMessage::new("seek.batch", payload.as_slice()))
            .await
            .unwrap();
    }

    let mut stream = std::pin::pin!(sub.batches());
    let batch = stream.next().await.unwrap().unwrap();
    assert_eq!(batch.len(), 3);
    for msg in batch {
        msg.ack().await.unwrap();
    }

    seeker.seek(MemoryPosition::start()).await.unwrap();
    let replayed = stream.next().await.unwrap().unwrap();
    let payloads: Vec<&[u8]> = replayed.iter().map(IncomingMessage::payload).collect();
    assert_eq!(
        payloads,
        [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
    );
    for msg in replayed {
        msg.ack().await.unwrap();
    }
}

#[tokio::test]
async fn position_is_stable_across_a_requeue() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("seek.requeue");
    let publisher = broker.publisher();
    publisher
        .publish(OutgoingMessage::new("seek.requeue", b"a".as_slice()))
        .await
        .unwrap();

    let mut stream = std::pin::pin!(sub.stream());
    let msg = stream.next().await.unwrap().unwrap();
    let position = msg.position();
    msg.nack(true).await.unwrap();

    let redelivered = stream.next().await.unwrap().unwrap();
    assert_eq!(redelivered.position(), position);
    redelivered.ack().await.unwrap();
}

// The wake path is the point of the capability: a dispatch loop parked on an empty
// subscription must observe a seek without waiting for an unrelated publish, so this test
// parks a real task (the oneshot fires on its first Pending poll) before seeking.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seek_wakes_a_parked_stream() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("seek.wake");
    let seeker = sub.seeker();
    let publisher = broker.publisher();
    publisher
        .publish(OutgoingMessage::new("seek.wake", b"a".as_slice()))
        .await
        .unwrap();
    {
        let mut stream = std::pin::pin!(sub.stream());
        stream.next().await.unwrap().unwrap().ack().await.unwrap();
    }

    let (parked_tx, parked_rx) = tokio::sync::oneshot::channel();
    let handle = tokio::spawn(async move {
        let mut stream = std::pin::pin!(sub.stream());
        let mut parked_tx = Some(parked_tx);
        std::future::poll_fn(move |cx| {
            let polled = stream.as_mut().poll_next(cx);
            if polled.is_pending() {
                if let Some(tx) = parked_tx.take() {
                    let _ = tx.send(());
                }
            }
            polled
        })
        .await
    });

    parked_rx.await.unwrap();
    seeker.seek(MemoryPosition::start()).await.unwrap();
    let replayed = timeout(Duration::from_secs(5), handle)
        .await
        .expect("a seek must wake the parked stream, not wait for the next publish")
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(replayed.payload(), b"a");
    replayed.ack().await.unwrap();
}

#[tokio::test]
async fn batches_drop_stale_requeues_after_a_seek() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("seek.batch.stale");
    sub.set_batch_limit(2);
    let seeker = sub.seeker();
    let publisher = broker.publisher();
    for payload in [b"a", b"b", b"c"] {
        publisher
            .publish(OutgoingMessage::new("seek.batch.stale", payload.as_slice()))
            .await
            .unwrap();
    }

    let mut stream = std::pin::pin!(sub.batches());
    let mut batch = stream.next().await.unwrap().unwrap();
    assert_eq!(batch.len(), 2);
    let held_b = batch.pop().unwrap();
    let held_a = batch.pop().unwrap();
    assert_eq!(held_a.payload(), b"a");
    assert_eq!(held_b.payload(), b"b");

    // Clamped to the log end: queued "c" is drained and nothing is replayed.
    seeker.seek(MemoryPosition::sequence(10)).await.unwrap();
    assert!(futures::poll!(stream.next()).is_pending());

    // A stale copy at the head of the queue: the first-element loop filters it out.
    held_b.nack(true).await.unwrap();
    assert!(futures::poll!(stream.next()).is_pending());

    // A stale copy behind a live delivery: the batch-fill loop filters it out.
    publisher
        .publish(OutgoingMessage::new("seek.batch.stale", b"d".as_slice()))
        .await
        .unwrap();
    held_a.nack(true).await.unwrap();
    let live = stream.next().await.unwrap().unwrap();
    let payloads: Vec<&[u8]> = live.iter().map(IncomingMessage::payload).collect();
    assert_eq!(payloads, [b"d".as_slice()]);
    for msg in live {
        msg.ack().await.unwrap();
    }
    assert!(futures::poll!(stream.next()).is_pending());
}

#[cfg(feature = "testing")]
#[tokio::test]
async fn seek_keeps_the_coordinator_in_flight_count_balanced() {
    let broker = MemoryBroker::new();
    let connected = broker.connect().await.unwrap();
    let coordinator = Coordinator::new(64);
    connected.install_coordinator(coordinator.clone());

    // Subscribed after the install, so every delivery carries the coordinator.
    let mut sub = connected.subscribe("seek.balance").await.unwrap();
    let seeker = sub.seeker();
    let publisher = connected.publisher();
    for payload in [b"a", b"b"] {
        publisher
            .publish(OutgoingMessage::new("seek.balance", payload.as_slice()))
            .await
            .unwrap();
    }

    let mut stream = std::pin::pin!(sub.stream());
    // Held unsettled so its requeue can race the seek below.
    let held = stream.next().await.unwrap().unwrap();
    assert_eq!(held.payload(), b"a");

    // Drains queued "b" (one consumed) and replays the log suffix (one enqueued).
    seeker.seek(MemoryPosition::sequence(1)).await.unwrap();
    let replayed = stream.next().await.unwrap().unwrap();
    assert_eq!(replayed.payload(), b"b");
    replayed.ack().await.unwrap();

    // The stale requeue of "a" is dropped by the watermark filter (one consumed).
    held.nack(true).await.unwrap();
    assert!(futures::poll!(stream.next()).is_pending());

    // Every enqueue across publish, drain, replay, requeue, and filter is balanced, so
    // the harness's quiescence wait resolves instead of hanging.
    coordinator.drive().await.unwrap();
}

#[tokio::test]
async fn seek_scope_is_one_subscriber_instance() {
    let broker = MemoryBroker::new();
    let mut seeking = broker.subscribe("seek.scope");
    let mut bystander = broker.subscribe("seek.scope");
    let seeker = seeking.seeker();
    let publisher = broker.publisher();
    for payload in [b"a", b"b"] {
        publisher
            .publish(OutgoingMessage::new("seek.scope", payload.as_slice()))
            .await
            .unwrap();
    }

    let mut seeking_stream = std::pin::pin!(seeking.stream());
    let mut bystander_stream = std::pin::pin!(bystander.stream());
    for _ in 0..2 {
        let msg = seeking_stream.next().await.unwrap().unwrap();
        msg.ack().await.unwrap();
        let msg = bystander_stream.next().await.unwrap().unwrap();
        msg.ack().await.unwrap();
    }

    seeker.seek(MemoryPosition::start()).await.unwrap();
    let replayed = seeking_stream.next().await.unwrap().unwrap();
    assert_eq!(replayed.payload(), b"a");
    replayed.ack().await.unwrap();
    let tail = seeking_stream.next().await.unwrap().unwrap();
    assert_eq!(tail.payload(), b"b");
    tail.ack().await.unwrap();

    // The sibling subscription of the same name is unaffected by the replay.
    assert!(futures::poll!(bystander_stream.next()).is_pending());
}

#[tokio::test]
async fn partition_key_reads_well_known_header() {
    let broker = MemoryBroker::new();
    let mut sub = broker.subscribe("keyed");
    let publisher = broker.publisher();

    let mut headers = Headers::new();
    headers.insert(PARTITION_KEY_HEADER, b"user-42".as_slice());
    publisher
        .publish(OutgoingMessage::new("keyed", b"a".as_slice()).with_headers(headers))
        .await
        .unwrap();
    publisher
        .publish(OutgoingMessage::new("keyed", b"b".as_slice()))
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
