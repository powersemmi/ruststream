//! The lifecycle contract past the happy path: what a shutdown must finish, and what every handle
//! that outlives the connection must answer.
//!
//! [`harness::lifecycle`](super::harness::lifecycle) runs the ladder every broker gets with no
//! code of its own. The two checks here need something only the broker can say, so a broker crate
//! calls each of them itself:
//!
//! * [`shutdown_flushes`] takes the broker's [`Backlog`] answer: whether a message waits for a
//!   subscription that opens later. It acknowledges a delivery, publishes a message and shuts down
//!   at once, then looks from another connection: the acknowledgement must hold and the message
//!   must arrive.
//! * [`shared_handle_closes`] needs a connected form that is [`Clone`], the shareable handle some
//!   brokers hand out. A clone kept past the shutdown of the original must error, never reach a
//!   connection of its own.
//!
//! # Examples
//!
//! ```no_run
//! # #[cfg(feature = "memory")]
//! # async fn run() {
//! use ruststream::conformance::lifecycle;
//! use ruststream::memory::{MemoryBroker, MemorySource};
//! use ruststream::testing::Backlog;
//!
//! // Every broker the factory builds reaches the same bus, the way two connections reach one
//! // server.
//! let bus = MemoryBroker::new();
//! lifecycle::shutdown_flushes(
//!     move || bus.clone(),
//!     |name| MemorySource::new(name),
//!     |connected| connected.publisher(),
//!     Backlog::Missed,
//! )
//! .await;
//! lifecycle::shared_handle_closes(
//!     MemoryBroker::new,
//!     |name| MemorySource::new(name),
//!     |connected| connected.publisher(),
//! )
//! .await;
//! # }
//! ```

use std::{
    fmt,
    future::Future,
    pin::pin,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use futures::{Stream, StreamExt};
use tokio::time::{Instant as TokioInstant, timeout, timeout_at};

use super::harness::{expect_next, on_foreign_runtime};
use super::helpers::unique_subject;
use crate::{
    AckError, Broker, Connected, ConnectedBroker, IncomingMessage, OutgoingMessage, Publisher,
    Subscriber, SubscriptionSource, testing::Backlog,
};

/// How long a single publish, settlement or subscription may take before the check fails it.
const STEP_TIMEOUT: Duration = Duration::from_secs(2);
/// How long a broker constructor may run. Recording configuration takes microseconds; a
/// constructor that dials, resolves or waits on the network takes longer.
pub(crate) const CONSTRUCTION_BUDGET: Duration = Duration::from_secs(1);
/// How long `shutdown` may take. Generous, because a real shutdown flushes over the network.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
/// The delay [`ladder`] settles with. Long enough that a broker which ignores it or rounds it down
/// to whole seconds redelivers before it runs out, which the check reads as a failure.
pub(crate) const REDELIVERY_DELAY: Duration = Duration::from_millis(1500);
/// How long past a delay, or after a requeue, the check waits for the message to come back:
/// brokers whose delays have second granularity round up, and a live server redelivers over the
/// network.
const REDELIVERY_TIMEOUT: Duration = Duration::from_secs(10);
/// How long "nothing arrives" is watched for.
const NEGATIVE_WAIT: Duration = Duration::from_millis(100);

const FIRST: &[u8] = b"lifecycle";
const FROM_BROKER_RUNTIME: &[u8] = b"lifecycle.again.broker-runtime";
const FROM_NEW_RUNTIME: &[u8] = b"lifecycle.again.new-runtime";
const HELD: &[u8] = b"lifecycle.held";
const ACKED: &[u8] = b"flush.acked";
const HELD_ACK: &[u8] = b"flush.held";
const FLUSHED: &[u8] = b"flush.published";

/// The message type a subscriber yields.
type SubscriberMessage<S> = <S as Subscriber>::Message;

/// The body of [`harness::lifecycle`](super::harness::lifecycle); its rustdoc is the contract.
pub(crate) async fn ladder<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
) where
    B: Broker,
    MkBroker: Fn() -> B + Sync,
    Src: SubscriptionSource<Connected<B>> + Clone + Send + 'static,
    Src::Subscriber: Send + 'static,
    MkSrc: Fn(&str) -> Src,
    SubscriberMessage<Src::Subscriber>: 'static,
    Pub: Publisher + 'static,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    let subject = unique_subject("conformance.lifecycle");

    let broker = construct_off_runtime(&make_broker);
    // Shared with the runtimes the subscription and the settlements come from, and taken back
    // whole for the consuming shutdown once they have stopped.
    let connected = Arc::new(
        broker
            .connect()
            .await
            .expect("lifecycle: the broker must connect after synchronous construction"),
    );

    // Opened from a runtime that stops before the first delivery is read, the way a subscription
    // opened from a dedicated thread is: what it reads with has to live on the broker's runtime.
    let source = make_source(&subject);
    let opening = Arc::clone(&connected);
    let mut subscriber = on_foreign_runtime(async move || source.subscribe(&opening).await)
        .await
        .unwrap_or_else(|err| {
            panic!(
                "lifecycle: the subscription to `{subject}` must open from a runtime of its own, \
                 got: {err:?}"
            )
        });

    let foreign = make_publisher(&connected);
    let native = make_publisher(&connected);
    let untouched = make_publisher(&connected);

    // Published from a runtime that stops before the delivery is read, the way a handler on a
    // dedicated thread publishes: what the publish left to finish runs on the broker's runtime.
    let (foreign, outcome) = publish_on_foreign(foreign, &subject, FIRST).await;
    outcome.unwrap_or_else(|err| panic!("lifecycle: publish after connect failed: {err}"));

    let mut stream = pin!(subscriber.stream());
    let msg = expect_payload(
        &mut stream,
        FIRST,
        "lifecycle: a subscription opened from a runtime that stopped must keep receiving",
    )
    .await;
    let msg = redeliver_after_delay(&mut stream, msg).await;
    requeue_from_foreign(&mut stream, msg).await;

    let foreign = foreign_after_stop(foreign, &subject).await;
    receive_again(&mut stream).await;

    publish_within(&native, &subject, HELD)
        .await
        .unwrap_or_else(|err| panic!("lifecycle: publish from the broker's runtime failed: {err}"));
    let held = expect_payload(&mut stream, HELD, "lifecycle: the held delivery").await;

    let connected = Arc::try_unwrap(connected).unwrap_or_else(|_| {
        unreachable!("every runtime the connected form was shared with has stopped")
    });
    shutdown_within(connected, "lifecycle").await;

    // The ladder makes owner-side misuse unrepresentable; the handles created before the
    // shutdown are the surface that must stay honest at runtime. The untouched one goes first: a
    // publisher that attaches lazily attaches here, after the connection is gone.
    for (publisher, which) in [
        (
            &untouched,
            "a publisher paired before shutdown and never used",
        ),
        (&native, "a publisher used only on the broker's runtime"),
        (
            &foreign,
            "a publisher used from runtimes that have since stopped",
        ),
    ] {
        let answer = within(
            publisher.publish(
                OutgoingMessage::new(&subject, b"post-shutdown".as_slice()),
                None,
            ),
            &format!(
                "lifecycle: publish after shutdown through {which} (a handle aliasing the closed \
                 connection must error)"
            ),
        )
        .await;
        assert!(
            answer.is_err(),
            "lifecycle: publish after shutdown through {which} succeeded; a handle aliasing the \
             closed connection must error",
        );
    }

    held_across_shutdown(&mut stream, held, &subject, make_broker, make_source).await;
}

/// Builds the broker on a thread with no Tokio runtime, the way the synchronous app builder runs
/// before any runtime exists.
///
/// The thread is scoped so the factory need not be `'static`; the wait blocks the caller's worker
/// only for as long as the constructor runs, and a constructor that only records configuration
/// runs for microseconds.
fn construct_off_runtime<B, MkBroker>(make_broker: &MkBroker) -> B
where
    B: Broker,
    MkBroker: Fn() -> B + Sync,
{
    thread::scope(|scope| {
        let started = Instant::now();
        let built = thread::Builder::new()
            .name("conformance-no-runtime".to_owned())
            .spawn_scoped(scope, make_broker)
            .expect("the thread with no runtime must start")
            .join();
        let elapsed = started.elapsed();
        let broker = built.unwrap_or_else(|_| {
            panic!(
                "lifecycle: the broker constructor panicked on a thread with no Tokio runtime (its \
                 message is above); a constructor only records configuration: no spawn, no \
                 `Handle::current`, no I/O, all of which belong in `connect`"
            )
        });
        assert!(
            elapsed < CONSTRUCTION_BUDGET,
            "lifecycle: the broker constructor took {elapsed:?}; a constructor only records \
             configuration and returns at once, while waiting on the network belongs in `connect`",
        );
        broker
    })
}

/// Settles `msg` with a delay from a runtime that stops at once, then expects it back no sooner
/// than the delay and no later than the timeout after it. A delivery that does not offer the
/// delayed settlement comes back unchanged.
async fn redeliver_after_delay<S, M, E>(stream: &mut S, msg: M) -> M
where
    S: Stream<Item = Result<M, E>> + Unpin + Send,
    M: IncomingMessage + 'static,
    E: fmt::Debug,
{
    if !msg.supports_nack_after() {
        return msg;
    }
    let settled_at = TokioInstant::now();
    let answer = on_foreign_runtime(async move || {
        timeout(STEP_TIMEOUT, msg.nack_after(REDELIVERY_DELAY))
            .await
            .ok()
    })
    .await;
    match answer {
        Some(Ok(())) => {}
        Some(Err(err)) => panic!(
            "lifecycle: the delivery offers nack_after, so a delayed nack must be accepted, got: \
             {err:?}"
        ),
        None => panic!("lifecycle: nack_after from a runtime of its own hung for {STEP_TIMEOUT:?}"),
    }
    let again = match next_until(stream, settled_at + REDELIVERY_DELAY + REDELIVERY_TIMEOUT).await {
        Next::Delivered(again) => again,
        Next::TimedOut => panic!(
            "lifecycle: a delayed nack settled from another runtime never came back; the broker \
             must run its internal tasks on the runtime it connected on"
        ),
        other => panic!("lifecycle: waiting for the delayed redelivery, the stream {other}"),
    };
    let elapsed = settled_at.elapsed();
    assert!(
        elapsed >= REDELIVERY_DELAY,
        "lifecycle: nack_after({REDELIVERY_DELAY:?}) came back after {elapsed:?}; a broker that \
         ignores the delay or rounds it down redelivers before it runs out",
    );
    assert_eq!(
        again.payload(),
        FIRST,
        "lifecycle: a delayed nack must redeliver the same message",
    );
    again
}

/// `nack(requeue = true)` from a runtime that stops at once: where the delivery answers `Ok`, the
/// message must come back on the broker's runtime. The redelivery is acknowledged from another
/// stopped runtime.
async fn requeue_from_foreign<S, M, E>(stream: &mut S, msg: M)
where
    S: Stream<Item = Result<M, E>> + Unpin + Send,
    M: IncomingMessage + 'static,
    E: fmt::Debug,
{
    match settle_on_foreign(msg, Settle::Requeue).await {
        Ok(()) => {}
        // A transport with nothing to take back has no redelivery to observe.
        Err(AckError::Unsupported) => return,
        Err(other) => {
            panic!("lifecycle: nack(requeue = true) must succeed or be unsupported, got: {other:?}")
        }
    }
    let again = match next_until(stream, TokioInstant::now() + REDELIVERY_TIMEOUT).await {
        Next::Delivered(again) => again,
        Next::TimedOut => panic!(
            "lifecycle: nack(requeue = true) from a runtime that stopped answered Ok, and the \
             message never came back within {REDELIVERY_TIMEOUT:?}; Ok promises a redelivery, \
             and the requeue runs on the runtime the broker connected on"
        ),
        other => panic!("lifecycle: waiting for the requeued message, the stream {other}"),
    };
    assert_eq!(
        again.payload(),
        FIRST,
        "lifecycle: nack(requeue = true) must redeliver the same message",
    );
    match settle_on_foreign(again, Settle::Ack).await {
        Ok(()) | Err(AckError::Unsupported) => {}
        Err(other) => panic!("lifecycle: ack must succeed or be unsupported, got: {other:?}"),
    }
}

/// Publishes once more through a publisher whose first publish came from a runtime that has since
/// stopped: once from the broker's runtime, once from a new runtime. Returns the publisher.
async fn foreign_after_stop<Pub: Publisher + 'static>(publisher: Pub, subject: &str) -> Pub {
    publish_within(&publisher, subject, FROM_BROKER_RUNTIME)
        .await
        .unwrap_or_else(|err| {
            panic!(
                "lifecycle: publish from the broker's runtime, through a publisher first used on a \
                 runtime that has since stopped, failed: {err}"
            )
        });
    let (publisher, outcome) = publish_on_foreign(publisher, subject, FROM_NEW_RUNTIME).await;
    outcome.unwrap_or_else(|err| {
        panic!(
            "lifecycle: publish from a new runtime, through a publisher first used on a runtime \
             that has since stopped, failed: {err}"
        )
    });
    publisher
}

/// Receives the two publishes of [`foreign_after_stop`], settling each as it arrives: the one from
/// the broker's runtime is dropped with `nack(requeue = false)` and the other acknowledged, both
/// from runtimes that stop at once. The dropped one must not come back.
async fn receive_again<S, M, E>(stream: &mut S)
where
    S: Stream<Item = Result<M, E>> + Unpin + Send,
    M: IncomingMessage + 'static,
    E: fmt::Debug,
{
    let deadline = TokioInstant::now() + REDELIVERY_TIMEOUT;
    let (mut from_broker, mut from_new) = (false, false);
    while !(from_broker && from_new) {
        let msg = match next_until(stream, deadline).await {
            Next::Delivered(msg) => msg,
            Next::TimedOut => panic!(
                "lifecycle: {} never arrived; a publisher whose first publish came from a runtime \
                 that stopped must keep publishing from any runtime (a lazily attached \
                 connection must live on the broker's runtime)",
                if from_broker {
                    "the publish from a new runtime"
                } else {
                    "the publish from the broker's runtime"
                },
            ),
            other => panic!("lifecycle: waiting for the repeated publishes, the stream {other}"),
        };
        let payload = msg.payload().to_vec();
        if payload == FROM_BROKER_RUNTIME && !from_broker {
            from_broker = true;
            match settle_on_foreign(msg, Settle::Drop).await {
                Ok(()) | Err(AckError::Unsupported) => {}
                Err(other) => panic!(
                    "lifecycle: nack(requeue = false) must succeed or be unsupported, got: {other:?}"
                ),
            }
        } else if payload == FROM_NEW_RUNTIME && !from_new {
            from_new = true;
            match settle_on_foreign(msg, Settle::Ack).await {
                Ok(()) | Err(AckError::Unsupported) => {}
                Err(other) => {
                    panic!("lifecycle: ack must succeed or be unsupported, got: {other:?}")
                }
            }
        } else {
            panic!(
                "lifecycle: expected the repeated publishes, got {:?} (a settled delivery came \
                 back, or one arrived twice)",
                String::from_utf8_lossy(&payload),
            );
        }
    }
    expect_quiet(
        stream,
        "lifecycle: a delivery dropped with nack(requeue = false) from a runtime that stopped came \
         back",
    )
    .await;
}

/// `nack(requeue = true)` on a delivery received before the shutdown and settled after it. An
/// error is the honest answer; `Ok` is a promise the message comes back, on its own subscription
/// or, where that ended with the connection, on a new one.
async fn held_across_shutdown<S, M, E, B, MkBroker, Src, MkSrc>(
    stream: &mut S,
    held: M,
    subject: &str,
    make_broker: MkBroker,
    make_source: MkSrc,
) where
    S: Stream<Item = Result<M, E>> + Unpin + Send,
    M: IncomingMessage,
    E: fmt::Debug,
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>>,
    MkSrc: Fn(&str) -> Src,
{
    let answer = within(
        held.nack(true),
        "lifecycle: nack(requeue = true) on a delivery held across shutdown (it must error or take \
         effect)",
    )
    .await;
    if answer.is_err() {
        return;
    }
    let deadline = TokioInstant::now() + STEP_TIMEOUT;
    loop {
        match next_until(stream, deadline).await {
            Next::Delivered(msg) if msg.payload() == HELD => return,
            // Anything else the old subscription still yields is not this check's subject.
            Next::Delivered(_) => {}
            Next::Ended | Next::Failed(_) | Next::TimedOut => break,
        }
    }

    let fresh = make_broker()
        .connect()
        .await
        .expect("lifecycle: a new broker must connect after the first shut down");
    let mut subscriber = timeout(STEP_TIMEOUT, make_source(subject).subscribe(&fresh))
        .await
        .expect("lifecycle: subscribing on a new connection hung")
        .expect("lifecycle: the subscription source must open on a new connection");
    let found = {
        let mut stream = pin!(subscriber.stream());
        let deadline = TokioInstant::now() + REDELIVERY_TIMEOUT;
        loop {
            match next_until(&mut stream, deadline).await {
                Next::Delivered(msg) => {
                    let found = msg.payload() == HELD;
                    let _ = timeout(STEP_TIMEOUT, msg.ack()).await;
                    if found {
                        break true;
                    }
                }
                Next::Ended | Next::Failed(_) | Next::TimedOut => break false,
            }
        }
    };
    assert!(
        found,
        "lifecycle: a delivery held across shutdown answered Ok to nack(requeue = true) and never \
         came back, on its own subscription or on a new connection; a settlement after shutdown \
         must error or take effect",
    );
    drop(subscriber);
    shutdown_within(fresh, "lifecycle").await;
}

/// Verifies a shutdown finishes what was handed to the broker before it: an acknowledgement and a
/// publish made right before `shutdown`, with no wait in between.
///
/// A client that queues its acknowledgements and publishes for a background task loses both when
/// the shutdown stops that task first: the acknowledged message comes back to the next consumer,
/// and the published one never reaches anyone. Both look fine to the process that shut down.
///
/// The check acknowledges one delivery, holds a second one, publishes a third message and shuts
/// down at once; `shutdown` itself must return within a bound. Then it looks from another
/// connection, and how depends on `backlog`, the broker's answer to whether a message waits for a
/// subscription that opens later ([`TestableBroker::backlog`](crate::testing::TestableBroker)):
///
/// * [`Backlog::Delivered`] (a queue, a log): a new connection subscribes after the shutdown. The
///   published message must arrive, the acknowledged one must not come back before it, and
///   neither may the held one where acknowledging it after the shutdown answered `Ok`.
/// * [`Backlog::Missed`] (publish/subscribe): a subscription on another connection is open before
///   the publish, and the published message must reach it.
///
/// `make_broker` must reach the same broker every time it is called: a server, or in process one
/// world shared by every broker it builds (clones of one [`MemoryBroker`](crate::memory::MemoryBroker)
/// share their bus). An in-process transport that gives every broker a world of its own runs this
/// check live only. Where the transport leases a delivery to the consumer that took it (an SQS
/// visibility timeout, a `JetStream` ack wait), give the descriptor a lease of a few seconds: a
/// message the first connection prefetched and never settled reaches the second one only once
/// its lease runs out, and the check waits ten seconds.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use ruststream::conformance::lifecycle;
/// use ruststream::memory::{MemoryBroker, MemorySource};
/// use ruststream::testing::Backlog;
///
/// let bus = MemoryBroker::new();
/// lifecycle::shutdown_flushes(
///     move || bus.clone(),
///     |name| MemorySource::new(name),
///     |connected| connected.publisher(),
///     Backlog::Missed,
/// )
/// .await;
/// # }
/// ```
///
/// # Panics
///
/// Panics with a descriptive message when the shutdown hangs or fails, when the published message
/// is lost, or when an acknowledged message comes back.
pub async fn shutdown_flushes<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
    backlog: Backlog,
) where
    B: Broker,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>>,
    MkSrc: Fn(&str) -> Src,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    let subject = unique_subject("conformance.flush");

    // A publish/subscribe transport delivers only to a subscription that is already open, so the
    // observer opens before the publish; a queue would share the messages out between the two.
    let observer = match backlog {
        Backlog::Missed => Some(connect_and_subscribe(make_broker(), make_source(&subject)).await),
        Backlog::Delivered => None,
    };

    let (connected, mut subscriber) =
        connect_and_subscribe(make_broker(), make_source(&subject)).await;
    let publisher = make_publisher(&connected);
    let (acked_ok, held) = {
        let mut stream = pin!(subscriber.stream());
        publish_within(&publisher, &subject, ACKED)
            .await
            .unwrap_or_else(|err| panic!("shutdown_flushes: publish failed: {err}"));
        let acked = expect_payload(&mut stream, ACKED, "shutdown_flushes").await;
        let acked_ok = match within(acked.ack(), "shutdown_flushes: ack").await {
            Ok(()) => true,
            Err(AckError::Unsupported) => false,
            Err(other) => {
                panic!("shutdown_flushes: ack must succeed or be unsupported, got: {other:?}")
            }
        };
        publish_within(&publisher, &subject, HELD_ACK)
            .await
            .unwrap_or_else(|err| panic!("shutdown_flushes: publish failed: {err}"));
        let held = expect_payload(&mut stream, HELD_ACK, "shutdown_flushes").await;
        (acked_ok, held)
    };
    publish_within(&publisher, &subject, FLUSHED)
        .await
        .unwrap_or_else(|err| panic!("shutdown_flushes: publish failed: {err}"));
    shutdown_within(connected, "shutdown_flushes").await;
    let held_ok = within(
        held.ack(),
        "shutdown_flushes: ack of a delivery held across shutdown (it must error or take effect)",
    )
    .await
    .is_ok();
    drop(subscriber);

    let (reader, mut subscriber) = match observer {
        Some(observer) => observer,
        None => connect_and_subscribe(make_broker(), make_source(&subject)).await,
    };
    {
        let mut stream = pin!(subscriber.stream());
        let mut deadline = TokioInstant::now() + REDELIVERY_TIMEOUT;
        let mut flushed = false;
        loop {
            let msg = match next_until(&mut stream, deadline).await {
                Next::Delivered(msg) => msg,
                Next::TimedOut if flushed => break,
                Next::TimedOut => panic!(
                    "shutdown_flushes: a message published right before shutdown never reached \
                     {}; shutdown must flush the publishes handed to it",
                    match backlog {
                        Backlog::Delivered => "a new connection (the broker declares a backlog)",
                        Backlog::Missed => "a subscription open on another connection",
                    },
                ),
                other => {
                    panic!("shutdown_flushes: reading from another connection, the stream {other}")
                }
            };
            let payload = msg.payload().to_vec();
            if backlog == Backlog::Delivered {
                assert!(
                    !(payload == ACKED && acked_ok),
                    "shutdown_flushes: a message acknowledged right before shutdown came back on \
                     a new connection; shutdown must flush the acknowledgements handed to it",
                );
                assert!(
                    !(payload == HELD_ACK && held_ok),
                    "shutdown_flushes: a delivery acknowledged after shutdown answered Ok and \
                     came back on a new connection; a settlement after shutdown must error or \
                     take effect",
                );
            }
            assert!(
                [ACKED, HELD_ACK, FLUSHED].contains(&payload.as_slice()),
                "shutdown_flushes: unexpected message {:?}",
                String::from_utf8_lossy(&payload),
            );
            let _ = timeout(STEP_TIMEOUT, msg.ack()).await;
            if payload == FLUSHED {
                if backlog == Backlog::Missed {
                    break;
                }
                // An acknowledged message that comes back behind the published one still shows
                // within a short wait.
                flushed = true;
                deadline = deadline.min(TokioInstant::now() + NEGATIVE_WAIT);
            }
        }
    }
    drop(subscriber);
    shutdown_within(reader, "shutdown_flushes").await;
}

/// Verifies a clone of a shareable connected form closes with the original.
///
/// Where the connected form is a cheap handle over one connection (`Clone`, typically an `Arc`),
/// a service holds clones in places the owner does not see. Once the owner shuts down, every clone
/// must answer with an error: a publisher it pairs, a publisher paired from it earlier and a
/// subscription it opens. A clone that quietly opens a connection of its own, or a subscription
/// that opens and never delivers, passes for a live service while the real one is gone.
///
/// A subscription on the closed connection may be refused, or may open and end (or yield an error)
/// at once; one that stays silent fails the check.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "memory")]
/// # async fn run() {
/// use ruststream::conformance::lifecycle;
/// use ruststream::memory::{MemoryBroker, MemorySource};
///
/// lifecycle::shared_handle_closes(
///     MemoryBroker::new,
///     |name| MemorySource::new(name),
///     |connected| connected.publisher(),
/// )
/// .await;
/// # }
/// ```
///
/// # Panics
///
/// Panics with a descriptive message when any use of the clone succeeds after the shutdown of the
/// original.
pub async fn shared_handle_closes<B, MkBroker, Src, MkSrc, Pub, MkPub>(
    make_broker: MkBroker,
    make_source: MkSrc,
    make_publisher: MkPub,
) where
    B: Broker,
    Connected<B>: Clone,
    MkBroker: Fn() -> B,
    Src: SubscriptionSource<Connected<B>>,
    MkSrc: Fn(&str) -> Src,
    Pub: Publisher,
    MkPub: Fn(&Connected<B>) -> Pub,
{
    let subject = unique_subject("conformance.shared");
    let connected = make_broker()
        .connect()
        .await
        .expect("shared_handle_closes: the broker must connect");
    let alias = connected.clone();
    let early = make_publisher(&alias);
    shutdown_within(connected, "shared_handle_closes").await;

    let late = make_publisher(&alias);
    for (publisher, which) in [
        (&early, "a publisher paired from the clone before shutdown"),
        (&late, "a publisher paired from the clone after shutdown"),
    ] {
        match publish_within(publisher, &subject, b"post-shutdown").await {
            Err(PublishFailure::Refused(_)) => {}
            Ok(()) => panic!(
                "shared_handle_closes: publish through {which} succeeded after the original shut \
                 down; every clone of the connected form must error"
            ),
            Err(PublishFailure::Hung) => panic!(
                "shared_handle_closes: publish through {which} hung for {STEP_TIMEOUT:?} after the \
                 original shut down; every clone of the connected form must error"
            ),
        }
    }

    let opened = timeout(STEP_TIMEOUT, make_source(&subject).subscribe(&alias))
        .await
        .unwrap_or_else(|_| {
            panic!(
                "shared_handle_closes: subscribing through the clone hung for {STEP_TIMEOUT:?} \
                 after the original shut down; it must error"
            )
        });
    if let Ok(mut subscriber) = opened {
        let mut stream = pin!(subscriber.stream());
        match next_until(&mut stream, TokioInstant::now() + STEP_TIMEOUT).await {
            Next::Ended | Next::Failed(_) => {}
            Next::Delivered(_) => panic!(
                "shared_handle_closes: a subscription opened through the clone after the original \
                 shut down delivered a message"
            ),
            Next::TimedOut => panic!(
                "shared_handle_closes: a subscription opened through the clone after the original \
                 shut down stayed open and silent; it must be refused or end"
            ),
        }
    }
}

/// Connects a broker from `make_broker` and opens a subscription to `subject` on it.
async fn connect_and_subscribe<B, Src>(broker: B, source: Src) -> (Connected<B>, Src::Subscriber)
where
    B: Broker,
    Src: SubscriptionSource<Connected<B>>,
{
    let connected = broker
        .connect()
        .await
        .expect("shutdown_flushes: the broker must connect");
    let subject = source.name().to_owned();
    let subscriber = within(
        source.subscribe(&connected),
        "shutdown_flushes: subscribing",
    )
    .await
    .unwrap_or_else(|err| {
        panic!("shutdown_flushes: the subscription to `{subject}` must open, got: {err:?}")
    });
    (connected, subscriber)
}

/// Awaits `work` within [`STEP_TIMEOUT`], panicking with `what` when it hangs.
async fn within<Work: Future>(work: Work, what: &str) -> Work::Output {
    timeout(STEP_TIMEOUT, work)
        .await
        .unwrap_or_else(|_| panic!("{what} hung for {STEP_TIMEOUT:?}"))
}

/// Shuts `connected` down within [`SHUTDOWN_TIMEOUT`], panicking when it hangs or fails.
pub(crate) async fn shutdown_within<C: ConnectedBroker>(connected: C, scenario: &str) {
    let answer = timeout(SHUTDOWN_TIMEOUT, connected.shutdown())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{scenario}: shutdown hung for {SHUTDOWN_TIMEOUT:?}; it must finish its teardown \
                 and return"
            )
        });
    if let Err(err) = answer {
        panic!("{scenario}: the broker must shut down cleanly, got: {err:?}");
    }
}

/// Why a publish did not succeed.
#[derive(Debug)]
enum PublishFailure {
    /// The publisher answered with an error.
    Refused(String),
    /// The publish did not return within [`STEP_TIMEOUT`].
    Hung,
}

impl fmt::Display for PublishFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Refused(err) => write!(f, "{err}"),
            Self::Hung => write!(f, "the publish hung for {STEP_TIMEOUT:?}"),
        }
    }
}

/// Publishes `payload` to `subject` on the current runtime within [`STEP_TIMEOUT`].
async fn publish_within<Pub: Publisher>(
    publisher: &Pub,
    subject: &str,
    payload: &[u8],
) -> Result<(), PublishFailure> {
    match timeout(
        STEP_TIMEOUT,
        publisher.publish(OutgoingMessage::new(subject, payload), None),
    )
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(PublishFailure::Refused(format!("{err:?}"))),
        Err(_) => Err(PublishFailure::Hung),
    }
}

/// Publishes from a runtime of its own that stops right after, handing the publisher back.
async fn publish_on_foreign<Pub: Publisher + 'static>(
    publisher: Pub,
    subject: &str,
    payload: &'static [u8],
) -> (Pub, Result<(), PublishFailure>) {
    let subject = subject.to_owned();
    on_foreign_runtime(async move || {
        let outcome = publish_within(&publisher, &subject, payload).await;
        (publisher, outcome)
    })
    .await
}

/// A settlement the check makes.
#[derive(Debug, Clone, Copy)]
enum Settle {
    Ack,
    Requeue,
    Drop,
}

/// Settles `msg` from a runtime of its own that stops right after, returning the answer.
async fn settle_on_foreign<M: IncomingMessage + 'static>(
    msg: M,
    how: Settle,
) -> Result<(), AckError> {
    let answer = on_foreign_runtime(async move || {
        match how {
            Settle::Ack => timeout(STEP_TIMEOUT, msg.ack()).await,
            Settle::Requeue => timeout(STEP_TIMEOUT, msg.nack(true)).await,
            Settle::Drop => timeout(STEP_TIMEOUT, msg.nack(false)).await,
        }
        .ok()
    })
    .await;
    answer.unwrap_or_else(|| {
        panic!("lifecycle: {how:?} from a runtime of its own hung for {STEP_TIMEOUT:?}")
    })
}

/// What a stream produced before a deadline.
enum Next<M> {
    Delivered(M),
    Ended,
    Failed(String),
    TimedOut,
}

impl<M> fmt::Display for Next<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Delivered(_) => write!(f, "delivered a message"),
            Self::Ended => write!(f, "ended"),
            Self::Failed(err) => write!(f, "yielded an error: {err}"),
            Self::TimedOut => write!(f, "timed out"),
        }
    }
}

/// The next item of `stream`, or what stopped it, before `deadline`.
async fn next_until<S, M, E>(stream: &mut S, deadline: TokioInstant) -> Next<M>
where
    S: Stream<Item = Result<M, E>> + Unpin + Send,
    E: fmt::Debug,
{
    match timeout_at(deadline, stream.next()).await {
        Ok(Some(Ok(msg))) => Next::Delivered(msg),
        Ok(Some(Err(err))) => Next::Failed(format!("{err:?}")),
        Ok(None) => Next::Ended,
        Err(_) => Next::TimedOut,
    }
}

/// The next delivery, which must carry `payload`.
async fn expect_payload<S, M, E>(stream: &mut S, payload: &[u8], scenario: &str) -> M
where
    S: Stream<Item = Result<M, E>> + Unpin + Send,
    M: IncomingMessage,
    E: fmt::Debug,
{
    let msg = expect_next(stream, scenario).await;
    assert_eq!(
        msg.payload(),
        payload,
        "{scenario}: expected {:?}, got {:?}",
        String::from_utf8_lossy(payload),
        String::from_utf8_lossy(msg.payload()),
    );
    msg
}

/// Nothing arrives within [`NEGATIVE_WAIT`].
async fn expect_quiet<S, M, E>(stream: &mut S, scenario: &str)
where
    S: Stream<Item = Result<M, E>> + Unpin + Send,
    E: fmt::Debug,
{
    if let Next::Delivered(_) = next_until(stream, TokioInstant::now() + NEGATIVE_WAIT).await {
        panic!("{scenario}");
    }
}

#[cfg(all(test, feature = "memory"))]
mod tests;
